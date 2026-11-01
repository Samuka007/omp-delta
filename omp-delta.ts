/**
 * omp-delta — 把当前 omp 会话实时记录进一条 Delta 线程(经 Delta CLI,合约 v8)。
 *
 * 实现官方插件合约中属于插件一侧的四件事(delta.dev/docs/cli/build-a-plugin):
 *   1. 把 omp 活动翻译成 Delta 记录并推送(thread append,ACP v2 update)
 *   2. 记录先落本地 outbox,Delta 确认前不删;重发安全(返回 duplicate)
 *   3. thread watch 把队友评论回流进 omp 上下文(follow-up,不自动触发 turn)
 *   4. 线程被他人接管时(superseded / fenced)停止本地 turn
 *
 * MVP 范围:
 *   - 只记录主会话(ctx.agent.kind === "main");子代理 → Delta 子线程留待下一步
 *   - 首次连接从当下开始记录,不回导历史会话
 *   - 命令:/delta-share /delta-status /delta-detach /delta-retry
 *
 * 架构说明:omp 会为每个会话(含子代理)重新绑定扩展工厂,但模块变量跨会话共享。
 * 因此所有可变状态都放在工厂闭包创建的 Runtime 里(每会话一份,互不污染);
 * 模块层只保留无状态的 Delta CLI 进程封装。
 *
 * 安装:复制本文件到 ~/.omp/agent/extensions/omp-delta.ts,重启 omp。
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as readline from "node:readline";

const SOURCE = "omp"; // Delta 侧插件固定标识(--source)
const PRODUCER = "omp"; // 线程内显示的生产者名(--producer-name)
const CONTRACT_MIN = 8; // 本插件实现的合约版本
const CONTRACT_MAX = 8;
const SCHEMA_VERSION = 1;
const APPEND_TIMEOUT_MS = 30_000;
const MOUNT_READY_TIMEOUT_MS = 90_000;
const MAX_AUTO_RETRIES = 3;

type DeltaRecord = {
	seq: number;
	turnSequence?: number;
	update: Record<string, unknown>;
};

type State = {
	connected: boolean;
	threadId?: string;
	worktreeRef?: string;
	visibility?: "private" | "organization";
	url?: string; // 真实 join 链接,来自 thread status(share id ≠ thread id)
	seq: number; // 已分配的最大 sequence
	lastAcked: number; // Delta 已确认的最大连续 sequence
	titleSent: boolean;
};

/** 每个omp会话一份的运行时状态(工厂闭包持有,互不共享)。 */
type Runtime = {
	pi: any;
	cwd: string;
	sid: string;
	isSub: boolean;
	state: State;
	currentTurn: number | null; // running 记录的 seq;turn 内所有记录共用
	paused: boolean;
	retryAttempts: number;
	detaching: boolean;
	mountChild: ChildProcess | null;
	watchChild: ChildProcess | null;
};

// ---- 无状态 Delta CLI 进程封装(模块级) ----

let deltaBin: string | null = null;

function resolveDelta(): string {
	if (deltaBin) return deltaBin;
	const lc = process.env.LOCALAPPDATA ?? "";
	const candidates = [
		"delta",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta.exe") : "",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta") : "",
	].filter(Boolean);
	for (const c of candidates) {
		try {
			const r = spawnSync(c, ["--version"], { windowsHide: true, timeout: 10_000 });
			if (r.status === 0) {
				deltaBin = c;
				return c;
			}
		} catch {
			/* 尝试下一个候选路径 */
		}
	}
	throw new Error("找不到 delta CLI(请确认 Delta 已安装并在 PATH 中,或重启 Delta 让新 PATH 生效)");
}

function runDelta(
	args: string[],
	opts?: { input?: string; timeoutMs?: number; cwd?: string },
): Promise<{ code: number; out: string; err: string }> {
	return new Promise((resolve) => {
		let child: ChildProcess;
		try {
			child = spawn(resolveDelta(), ["--json", ...args], {
				windowsHide: true,
				stdio: ["pipe", "pipe", "pipe"],
				cwd: opts?.cwd,
			});
		} catch (e) {
			resolve({ code: -1, out: "", err: String(e) });
			return;
		}
		let out = "";
		let err = "";
		const timer = setTimeout(() => {
			try {
				child.kill("SIGTERM");
			} catch {
				/* 已退出 */
			}
		}, opts?.timeoutMs ?? APPEND_TIMEOUT_MS);
		child.stdout?.on("data", (d) => {
			out += String(d);
		});
		child.stderr?.on("data", (d) => {
			err += String(d);
		});
		child.on("error", (e) => {
			clearTimeout(timer);
			resolve({ code: -1, out, err: String(e) });
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			resolve({ code: code ?? -1, out, err });
		});
		if (opts?.input !== undefined) child.stdin?.write(opts.input);
		child.stdin?.end();
	});
}

function parseJsonLines(text: string): any[] {
	const result: any[] = [];
	for (const line of text.split(/\r?\n/)) {
		const t = line.trim();
		if (!t) continue;
		try {
			result.push(JSON.parse(t));
		} catch {
			/* 非 JSON 行,忽略 */
		}
	}
	return result;
}

async function runDeltaJson(args: string[], opts?: { input?: string; timeoutMs?: number; cwd?: string }): Promise<any> {
	const r = await runDelta(args, opts);
	const js = parseJsonLines(r.out);
	if (js.length === 0) {
		throw new Error("delta " + args.join(" ") + " 无 JSON 输出: " + (r.err || r.out).slice(0, 400));
	}
	return js[js.length - 1];
}

/** 当前选中的组织(创建线程时必须显式指定)。 */
async function resolveOrg(): Promise<{ id: string; name: string }> {
	const cur = await runDeltaJson(["org", "current"]);
	const id = String(cur?.organization?.id ?? "");
	if (!id) {
		throw new Error("未能解析当前组织:请用 delta org switch 选择组织后重试。");
	}
	return { id, name: String(cur?.organization?.name ?? id) };
}

// ---- 扩展工厂:每会话一份 Runtime ----

export default function ompDelta(pi: any) {
	const rt: Runtime = {
		pi,
		cwd: process.cwd(),
		sid: "",
		isSub: false,
		state: { connected: false, seq: 0, lastAcked: 0, titleSent: false },
		currentTurn: null,
		paused: false,
		retryAttempts: 0,
		detaching: false,
		mountChild: null,
		watchChild: null,
	};
	// 供无 ctx 回调(如子进程 onClose)兜底使用的最新命令/事件 ctx
	let lastCtx: any = null;

	function log(msg: string) {
		const line = "[omp-delta] " + msg;
		try {
			pi?.logger?.warn?.(line);
		} catch {
			console.error(line);
		}
	}

	function notify(msg: string, level = "info") {
		try {
			lastCtx?.ui?.notify?.(msg, level);
		} catch {
			log(msg);
		}
	}

	function stateDir(): string {
		return path.join(os.homedir(), ".omp", "agent", "omp-delta", rt.sid || "unknown");
	}

	function loadState() {
		try {
			const raw = fs.readFileSync(path.join(stateDir(), "state.json"), "utf8");
			rt.state = { connected: false, seq: 0, lastAcked: 0, titleSent: false, ...JSON.parse(raw) };
			// 进程重启后 mount/watch 子进程必然已死;线程身份保留,连接稍后重建
			rt.state.connected = false;
		} catch {
			/* 首次运行,无状态文件 */
		}
	}

	function saveState() {
		try {
			fs.mkdirSync(stateDir(), { recursive: true });
			fs.writeFileSync(path.join(stateDir(), "state.json"), JSON.stringify(rt.state, null, 2));
		} catch (e) {
			log("保存状态失败: " + String(e));
		}
	}

	function outboxPath(): string {
		return path.join(stateDir(), "outbox.jsonl");
	}

	function ready(): boolean {
		return !rt.isSub && rt.state.connected && !!rt.state.threadId && !rt.paused;
	}

	function nextSeq(): number {
		rt.state.seq += 1;
		saveState();
		return rt.state.seq;
	}

	/** 分配 sequence、落 outbox、尝试推送。未连接时返回 null。 */
	function emit(update: Record<string, unknown>, opts?: { turnSequence?: number }): number | null {
		if (!ready()) return null;
		const seq = nextSeq();
		const rec: DeltaRecord = { seq, update };
		if (opts?.turnSequence !== undefined) rec.turnSequence = opts.turnSequence;
		try {
			fs.mkdirSync(stateDir(), { recursive: true });
			fs.appendFileSync(outboxPath(), JSON.stringify(rec) + "\n");
		} catch (e) {
			log("outbox 写入失败: " + String(e));
			return seq;
		}
		void flush();
		return seq;
	}

	// 实测回执形状(docs/contract-notes.md):
	// {"operation":"appended","sequence":N,"next_sequence":N+1,"duplicate":false,
	//  "disposition":"recorded","turn_sequence":3,"version":"…"}
	// 确认结论在 disposition(recorded/duplicate/fenced);兼容旧式 status 字段。
	function collectReceipts(j: any, into: Map<number, string>) {
		const items: any[] = Array.isArray(j)
			? j
			: Array.isArray(j?.receipts)
				? j.receipts
				: j?.sequence !== undefined && (j?.disposition !== undefined || j?.status !== undefined)
					? [j]
					: [];
		for (const it of items) {
			const seq = Number(it?.sequence);
			const st = String(it?.disposition ?? it?.status ?? "");
			if (!Number.isFinite(seq) || !st) continue;
			into.set(seq, it?.duplicate === true && st !== "fenced" ? "duplicate" : st);
		}
	}

	/** 从 lastAcked 起找最大连续确认位(有空洞时不越过未确认记录)。 */
	function contiguousAck(all: DeltaRecord[], receipts: Map<number, string>): number {
		const acked = new Map<number, boolean>();
		for (const [seq, st] of receipts) acked.set(seq, st === "recorded" || st === "duplicate");
		const bySeq = new Map<number, DeltaRecord>();
		for (const r of all) bySeq.set(r.seq, r);
		let seq = rt.state.lastAcked;
		while (acked.get(seq + 1) === true && bySeq.has(seq + 1)) seq += 1;
		return seq;
	}

	/** 已确认的记录从 outbox 文件里清掉,避免无限增长。
	 *  必须重新读文件:flush 的 await 期间 emit 可能追加了新记录,用旧快照重写会抹掉它们。 */
	function compactOutbox() {
		try {
			const raw = fs.readFileSync(outboxPath(), "utf8");
			const keep: string[] = [];
			for (const line of raw.split(/\r?\n/)) {
				const t = line.trim();
				if (!t) continue;
				try {
					const rec = JSON.parse(t) as DeltaRecord;
					if (rec.seq > rt.state.lastAcked) keep.push(t);
				} catch {
					keep.push(t); // 坏行原样保留,不静默丢弃
				}
			}
			fs.writeFileSync(outboxPath(), keep.join("\n") + (keep.length ? "\n" : ""));
		} catch (e) {
			log("outbox 压缩失败: " + String(e));
		}
	}

	let flushing = false;

	/** 把 outbox 里所有未确认记录推给 Delta;可重试错误自动退避,重试耗尽则暂停。 */
	async function flush(): Promise<void> {
		if (!rt.state.connected || !rt.state.threadId || flushing || rt.paused) return;
		flushing = true;
		try {
			let raw = "";
			try {
				raw = fs.readFileSync(outboxPath(), "utf8");
			} catch {
				return; // 还没有任何记录
			}
			const all: DeltaRecord[] = [];
			for (const line of raw.split(/\r?\n/)) {
				const t = line.trim();
				if (!t) continue;
				try {
					all.push(JSON.parse(t));
				} catch {
					/* 跳过坏行 */
				}
			}
			const pending = all.filter((r) => r.seq > rt.state.lastAcked);
			if (pending.length === 0) {
				rt.retryAttempts = 0;
				return;
			}

			// 重发已确认记录安全(Delta 返回 duplicate),故每次推送全部 pending
			const body =
				pending
					.map((r) => {
						const env: any = { schemaVersion: SCHEMA_VERSION, sequence: r.seq, update: r.update };
						if (r.turnSequence !== undefined) env.turnSequence = r.turnSequence;
						return JSON.stringify(env);
					})
					.join("\n") + "\n";

			const r = await runDelta(
				[
					"thread",
					"append",
					String(rt.state.threadId),
					"--source",
					SOURCE,
					"--external-id",
					rt.sid,
					"--producer-name",
					PRODUCER,
				],
				{ input: body, cwd: rt.cwd },
			);

			const receipts = new Map<number, string>();
			for (const j of parseJsonLines(r.out)) collectReceipts(j, receipts);
			if (r.code !== 0) for (const j of parseJsonLines(r.err)) collectReceipts(j, receipts);

			let fenced = false;
			let retryable = false;
			let retryAfter = 5_000;
			let errMsg = "";
			for (const [, st] of receipts) if (st === "fenced") fenced = true;
			if (r.code !== 0) {
				for (const j of parseJsonLines(r.err).concat(parseJsonLines(r.out))) {
					if (j?.error) {
						errMsg = String(j.error.message ?? "").slice(0, 300) || (r.err || r.out).slice(0, 300);
						if (j.error.retryable === true) {
							retryable = true;
							retryAfter = Number(j.error.retry_after_ms) || 5_000;
						}
					}
				}
				if (!errMsg) errMsg = (r.err || r.out || "exit " + r.code).slice(0, 300);
			}
			if (r.code === -1) {
				// spawn 失败或超时被杀:没有 JSON 错误体,按可重试故障处理
				retryable = true;
				if (!errMsg) errMsg = "delta 进程无响应或被终止";
			}

			if (fenced) {
				// 该 turn 已被他人接管:合约要求停止本地 turn(已执行的工具效果无法撤销)
				notify("Delta:该 turn 已被他人接管,本地 turn 已停止。", "warning");
				rt.state.lastAcked = Math.max(rt.state.lastAcked, contiguousAck(all, receipts));
				saveState();
				rt.currentTurn = null;
				try {
					lastCtx?.abort?.();
				} catch {
					/* 无进行中 turn */
				}
				return;
			}

			const newAck = contiguousAck(all, receipts);
			if (newAck > rt.state.lastAcked) {
				rt.state.lastAcked = newAck;
				saveState();
				rt.retryAttempts = 0;
				compactOutbox();
			}

			if (r.code !== 0 && rt.state.lastAcked < rt.state.seq) {
				if (retryable && rt.retryAttempts < MAX_AUTO_RETRIES) {
					rt.retryAttempts += 1;
					notify(
						"Delta 推送失败,自动重试 " + rt.retryAttempts + "/" + MAX_AUTO_RETRIES + "(" + errMsg + ")",
						"warning",
					);
					lastCtx?.setTimeout?.(() => {
						void flush();
					}, retryAfter);
				} else {
					rt.paused = true;
					notify("Delta 推送失败,已暂停自动重试:" + errMsg + "(用 /delta-retry 恢复)", "warning");
				}
			}
			if (r.code === 0 && rt.state.lastAcked < rt.state.seq) {
				// 部分确认:安排跟进推送;连续无进展则暂停(计数由有进展时归零)
				if (rt.retryAttempts < MAX_AUTO_RETRIES) {
					rt.retryAttempts += 1;
					lastCtx?.setTimeout?.(() => {
						void flush();
					}, 1_000);
				} else {
					rt.paused = true;
				notify("Delta 仍有未同步记录,自动重试已达上限(用 /delta-retry 恢复)", "warning");
				}
			}
		} finally {
			flushing = false;
		}
	}

	function spawnLineChild(
		args: string[],
		onLine: (j: any, raw: string) => void,
		onClose: (code: number | null) => void,
	): ChildProcess {
		const child = spawn(resolveDelta(), ["--json", ...args], {
			cwd: rt.cwd,
			windowsHide: true,
			stdio: ["ignore", "pipe", "pipe"],
		});
		const rl = readline.createInterface({ input: child.stdout! });
		rl.on("line", (line) => {
			const t = line.trim();
			if (!t) return;
			try {
				onLine(JSON.parse(t), t);
			} catch (e) {
				log("子进程输出解析失败: " + String(e));
			}
		});
		child.stderr?.on("data", () => {
			/* 诊断输出 MVP 不消费 */
		});
		child.on("close", (code) => onClose(code));
		child.on("error", (e) => {
			log("子进程启动失败: " + String(e));
			onClose(-1);
		});
		return child;
	}

	function killChild(child: ChildProcess | null) {
		try {
			child?.kill("SIGTERM");
		} catch {
			/* 已退出 */
		}
	}

	function handleWatchEvent(j: any) {
		const type = String(j?.type ?? "");
		if (type === "comment") {
			const c = j?.comment ?? j;
			const text = String(c?.text ?? "").trim();
			if (!text) return;
			const who = String(c?.author ?? c?.creator ?? "队友").slice(0, 40);
			// 合约要求:远端活动进上下文,但不自动触发 turn → followUp(空闲时只入上下文)
			pi?.sendMessage?.("[Delta 评论 · " + who + "] " + text, {
				deliverAs: "followUp",
				attribution: "agent",
			});
			notify("收到 Delta 评论,已作为 follow-up 进入 omp 上下文。");
			return;
		}
		if (type === "producer.turn" && JSON.stringify(j).includes("supersede")) {
			notify("Delta:你的 turn 已被接管,正在停止本地 turn。", "warning");
			rt.currentTurn = null;
			try {
				lastCtx?.abort?.();
			} catch {
				/* 无进行中 turn */
			}
		}
		// item / turn / presence / worktrees / children 等 MVP 暂不消费
	}

	function startWatch() {
		killChild(rt.watchChild);
		rt.watchChild = spawnLineChild(
			[
				"thread",
				"watch",
				String(rt.state.threadId),
				"--source",
				SOURCE,
				"--external-id",
				rt.sid,
				"--participate",
				PRODUCER,
			],
			(j) => handleWatchEvent(j),
			(code) => {
				rt.watchChild = null;
				if (!rt.detaching && rt.state.connected) {
					notify("Delta watch 断开(exit " + code + "),评论回流暂停;/delta-retry 可重连。", "warning");
				}
			},
		);
	}

	/** 已有线程:挂载 worktree + 启动 watch + 推送积压记录。 */
	async function attachAndStream(): Promise<void> {
		rt.detaching = false;
		const ref = String(rt.state.worktreeRef ?? "");
		if (!ref) throw new Error("缺少 worktree reference,请重新 /delta-share。");
		const a = await runDelta(["worktree", "attach", ref, "--thread", String(rt.state.threadId)], { cwd: rt.cwd });
		// 重连场景:线程已挂过同一 checkout 时 attach 报错,但挂载关系仍在,直接复用
		if (a.code !== 0 && !String(a.err || a.out).includes("already has a worktree")) {
			throw new Error("worktree attach 失败: " + (a.err || a.out).slice(0, 300));
		}

		// mount 上传 checkout 并持续同步;ready 行出现即继续,进程保持运行
		await new Promise<void>((resolveMount) => {
			let settled = false;
			const settle = () => {
				if (!settled) {
					settled = true;
					resolveMount();
				}
			};
			rt.mountChild = spawnLineChild(
				["worktree", "mount", ref, rt.cwd, "--existing-checkout"],
				() => settle(),
				(code) => {
					rt.mountChild = null;
					if (!rt.detaching && rt.state.connected) {
						notify("Delta worktree 同步进程退出(exit " + code + ")。", "warning");
					}
				},
			);
			// mount 启动慢时不无限等;进程仍在后台,就绪后会自行上传
			lastCtx?.setTimeout?.(settle, MOUNT_READY_TIMEOUT_MS);
		});

		rt.state.connected = true;
		saveState();
		startWatch();
		void flush();

		// join 链接用的是 share id(thread_01…),不是 thread create 返回的 id,只能问 status
		try {
			const st = await runDeltaJson(
				["thread", "status", String(rt.state.threadId), "--source", SOURCE, "--external-id", rt.sid],
				{ cwd: rt.cwd },
			);
			const url = String(st?.url ?? "");
			if (url && url !== rt.state.url) {
				rt.state.url = url;
				saveState();
			}
		} catch {
			/* 拿不到 url 不影响同步 */
		}
	}

	/** 首次连接:合约协商 → 建线程(幂等)→ worktree → 开始记录。组织由调用方先解析好。 */
	async function connectShared(mode: "private" | "organization", org: { id: string; name: string }): Promise<void> {
		const contract = await runDeltaJson(["thread", "contract"], { cwd: rt.cwd });
		const min = Number(contract?.min ?? 0);
		const max = Number(contract?.max ?? 0);
		if (max < CONTRACT_MIN) {
			throw new Error("delta CLI 过旧(支持合约 " + min + "-" + max + ",需要 ≥ " + CONTRACT_MIN + "),请更新 Delta。");
		}
		if (min > CONTRACT_MAX) {
			throw new Error("delta CLI 合约更新(min " + min + "),请更新 omp-delta 插件。");
		}

		// 同 source + external-id 幂等:重试/崩溃恢复都安全
		// 预检 git 仓库：worktree 挂载需要 git checkout，提前报错更可读
		const g = spawnSync("git", ["rev-parse", "--show-toplevel"], { cwd: rt.cwd, windowsHide: true });
		if (g.status !== 0) {
			throw new Error("当前目录不是 git 仓库,worktree 无法挂载:" + rt.cwd);
		}
		const remotes = spawnSync("git", ["remote"], { cwd: rt.cwd, windowsHide: true, encoding: "utf8" });
		if (!String(remotes.stdout ?? "").trim()) {
			throw new Error("仓库没有 Git remote,Delta worktree 创建会失败:先 git remote add origin …");
		}

		// 组织已在命令处理器里确认对话框之前解析(resolveOrg),这里直接使用
		// 同 source + external-id 幂等:重试/崩溃恢复都安全
		const created = await runDeltaJson(
			[
				"thread",
				"create",
				"--source",
				SOURCE,
				"--external-id",
				rt.sid,
				"--visibility",
				mode,
				"--organization",
				org.id,
			],
			{ cwd: rt.cwd },
		);
		rt.state.threadId = String(created?.thread_id ?? "");
		rt.state.visibility = mode;
		saveState(); // threadId 先落盘:后续任一步失败都能凭它幂等重连
		if (!rt.state.threadId) {
			throw new Error("thread create 未返回 thread_id: " + JSON.stringify(created).slice(0, 300));
		}

		// worktree 身份(参考 delta-claude checkoutReference 语义):
		// list 优先复用同 checkout 的既有身份,不盲目 create;多个身份报错让人选;
		// 创建/复用后立即落盘,防崩溃孤儿引用。
		let ref = "";
		const list = await runDeltaJson(["worktree", "list"], { cwd: rt.cwd });
		const rows: any[] = Array.isArray(list) ? list : (list?.worktrees ?? list?.items ?? []);
		const norm = (s: string) => s.replace(/[\\/]+$/, "").toLowerCase();
		const matches = rows.filter((x) => {
			const p = String(x?.path ?? x?.directory ?? "");
			return p && norm(p) === norm(rt.cwd);
		});
		if (matches.length > 1) {
			throw new Error("该 checkout 存在多个 worktree 身份,请先在 Delta 里选择正确的一个后重试。");
		}
		if (matches.length === 1) {
			ref = String(matches[0]?.reference ?? "");
		}
		if (!ref) {
			const w = await runDeltaJson(["worktree", "create", rt.cwd], { cwd: rt.cwd });
			ref = String(w?.reference ?? "");
		}
		if (!ref) throw new Error("worktree create/list 均未取得 reference。");
		rt.state.worktreeRef = ref;
		saveState();

		await attachAndStream();

		notify(
			"Delta 线程已连接:" +
				rt.state.threadId +
				"\n浏览器打开: " +
				(rt.state.url ?? "https://delta.dev/join/" + rt.state.threadId) +
				"\n(对话与工作区文件,含未提交改动,正在同步到 Delta)",
		);
	}

	function detach(quiet = false) {
		rt.detaching = true;
		killChild(rt.mountChild);
		rt.mountChild = null;
		killChild(rt.watchChild);
		rt.watchChild = null;
		rt.state.connected = false;
		saveState();
		if (!quiet) notify("已断开 Delta 同步(线程与已上传内容保留)。");
	}

	// ---- 事件与命令注册 ----

	pi.registerCommand("delta-share", {
		description: "把当前 omp 会话连接到 Delta 线程(/delta-share private 为私有,默认共享给组织)",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd); // omp 的 /cd 会改会话 cwd,连接前以当前值为准
			if (rt.isSub) {
				notify("子代理会话暂不支持连接 Delta。", "warning");
				return;
			}
			if (rt.state.connected) {
				notify("已连接:" + rt.state.threadId);
				return;
			}
			const mode: "private" | "organization" = String(args ?? "").trim().toLowerCase().startsWith("priv")
				? "private"
				: "organization";
			// 合约要求：共享前必须征得用户同意，并告知可见范围（组织名+内容）
			let ok = true;
			try {
				const org = await resolveOrg();
				if (typeof ctx?.ui?.confirm === "function") {
					ok = await ctx.ui.confirm(
						mode === "private" ? "连接 Delta（私有线程）？" : "连接 Delta 并把会话共享给组织？",
						mode === "private"
							? "可见性：私有，仅你自己可见\n共享内容：对话记录与工作区文件，含未提交改动"
							: "组织：" + org.name + "（" + org.id + "）\n共享内容：对话记录与工作区文件，含未提交改动",
					);
				} else {
					notify(mode === "private" ? "将创建私有线程。" : "将共享到组织：" + org.name);
				}
				if (!ok) {
					notify("已取消，未创建 Delta 线程。");
					return;
				}
				await connectShared(mode, org);
			} catch (e) {
				notify("连接失败：" + String(e instanceof Error ? e.message : e), "warning");
			}
		},
	});

	pi.registerCommand("delta-status", {
		description: "查看 omp-delta 连接与同步状态",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			const s = rt.state;
			const lines = [
				"连接: " + (s.connected ? "已连接" : "未连接") + (rt.paused ? "(推送暂停)" : ""),
				"线程: " + (s.threadId ?? "无"),
				s.threadId ? "可见性: " + (s.visibility ?? "未知") : "",
				s.threadId ? "链接: " + (s.url ?? "(尚未获取,重连后自动更新)") : "",
				"记录: 已分配 " + s.seq + " 条,已确认 " + s.lastAcked + " 条",
				"worktree: " + (s.worktreeRef ?? "无"),
				"mount/watch: " + (rt.mountChild ? "运行中" : "停止") + " / " + (rt.watchChild ? "运行中" : "停止"),
				"会话: " + (rt.isSub ? "子代理(不记录)" : "主会话") + " · " + rt.sid,
			].filter(Boolean);
			notify(lines.join("\n"));
		},
	});

	pi.registerCommand("delta-detach", {
		description: "断开 Delta 同步(保留线程与已上传内容)",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			if (!rt.state.threadId) {
				notify("尚未连接 Delta。");
				return;
			}
			detach();
		},
	});

	pi.registerCommand("delta-retry", {
		description: "恢复/重试 Delta 推送(断线后也可重连)",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			rt.paused = false;
			rt.retryAttempts = 0;
			if (rt.state.threadId && !rt.state.connected) {
				notify("尝试重连 Delta 线程 " + rt.state.threadId + " …");
				try {
					await attachAndStream();
					notify("已重连。");
				} catch (e) {
					notify("重连失败:" + String(e instanceof Error ? e.message : e), "warning");
					return;
				}
			} else {
				void flush();
			}
		},
	});

	pi.on("session_start", async (_event: any, ctx: any) => {
		lastCtx = ctx;
		rt.isSub = (ctx?.agent?.kind ?? "main") !== "main";
		if (rt.isSub) return;
		rt.sid = String(ctx?.sessionManager?.getSessionId?.() ?? "sess-" + randomUUID());
		rt.cwd = String(ctx?.cwd ?? process.cwd());
		loadState();
		if (rt.state.threadId) {
			// 不阻塞 session_start(事件处理器有 30s 预算):后台重连
			notify("发现已有 Delta 线程 " + rt.state.threadId + ",后台重连中…");
			void attachAndStream().then(
				() => notify("Delta 已重连: " + (rt.state.url ?? rt.state.threadId)),
				(e: unknown) =>
					notify(
						"Delta 重连失败:" + String(e instanceof Error ? e.message : e) + "(/delta-retry 可再试)",
						"warning",
					),
			);
		}
	});

	pi.on("session_switch", async () => {
		if (rt.isSub || !rt.state.threadId) return;
		if (rt.state.connected || rt.mountChild || rt.watchChild) {
			detach(true);
			notify("会话已切换,Delta 同步已停止;新会话可用 /delta-share 重新连接。");
		}
	});

	pi.on("input", async (event: any) => {
		if (!ready()) return;
		const text = String(event?.text ?? event?.prompt ?? "").trim();
		if (!text) return;
		if (text.startsWith("/")) return; // slash 命令不是对话内容,不记录
		if (!rt.state.titleSent) {
			rt.state.titleSent = true;
			saveState();
			let title = "";
			try {
				title = String((await pi?.getSessionName?.()) ?? "");
			} catch {
				/* 无会话名 */
			}
			emit({ sessionUpdate: "session_info_update", title: (title || text).slice(0, 120) });
		}
		emit({
			sessionUpdate: "user_message",
			messageId: "u" + (rt.state.seq + 1),
			content: [{ type: "text", text }],
		});
	});

	pi.on("turn_start", () => {
		if (!ready()) return;
		const seq = emit({ sessionUpdate: "state_update", state: "running" });
		if (seq !== null) rt.currentTurn = seq;
	});

	pi.on("turn_end", () => {
		if (!ready()) return;
		if (rt.currentTurn !== null) {
			emit({ sessionUpdate: "state_update", state: "idle" }, { turnSequence: rt.currentTurn });
		}
		rt.currentTurn = null;
	});

	pi.on("assistant_message", (event: any) => {
		if (!ready()) return;
		const blocks: any[] = Array.isArray(event?.message?.content) ? event.message.content : [];
		const text = blocks
			.filter((b) => b?.type === "text" && typeof b.text === "string")
			.map((b) => b.text)
			.join("\n\n")
			.trim();
		if (!text) return;
		emit(
			{
				sessionUpdate: "agent_message",
				messageId: "a" + (rt.state.seq + 1),
				content: [{ type: "text", text }],
			},
			rt.currentTurn !== null ? { turnSequence: rt.currentTurn } : undefined,
		);
	});

	pi.on("tool_execution_start", (event: any) => {
		if (!ready()) return;
		const id = String(event?.toolCallId ?? event?.callId ?? "");
		if (!id) return;
		emit(
			{
				sessionUpdate: "tool_call_update",
				toolCallId: id,
				title: String(event?.toolName ?? "tool").slice(0, 120),
				status: "pending",
			},
			rt.currentTurn !== null ? { turnSequence: rt.currentTurn } : undefined,
		);
	});

	pi.on("tool_execution_end", (event: any) => {
		if (!ready()) return;
		const id = String(event?.toolCallId ?? event?.callId ?? "");
		if (!id) return;
		emit(
			{ sessionUpdate: "tool_call_update", toolCallId: id, status: "completed" },
			rt.currentTurn !== null ? { turnSequence: rt.currentTurn } : undefined,
		);
	});

	pi.on("session_shutdown", () => {
		// 处理器只有 2s 预算:只做同步清理,不等待 CLI 调用
		try {
			killChild(rt.mountChild);
			killChild(rt.watchChild);
		} catch {
			/* 尽力而为 */
		}
		rt.mountChild = null;
		rt.watchChild = null;
		saveState();
	});
}
