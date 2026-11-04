/**
 * omp-delta — 把当前 omp 会话实时记录进一条 Delta 线程(经 Delta CLI,合约 v8)。
 *
 * 实现官方插件合约中属于插件一侧的四件事(delta.dev/docs/cli/build-a-plugin):
 *   1. 把 omp 活动翻译成 Delta 记录并推送(thread append,ACP v2 update)
 *   2. 记录只进内存队列,由服务端 next_sequence 对齐游标;已确认即出队;重发安全(返回 duplicate)
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
 * 模块层只保留无状态的 Delta CLI 进程封装与可单测的纯函数接缝。
 * 零本地状态(#10):线程身份以 DeltaDB 为权威,绑定关系写在 omp 会话记录里
 * (appendEntry 自定义条目),送达游标以服务端 next_sequence 为权威。
 *
 * 安装:omp plugin install 'github:Samuka007/omp-delta'(或本地开发 omp plugin link ./),重启 omp;
 * Windows 上 installer 静默失败时,在 ~/.omp/plugins 里 npm install 'github:Samuka007/omp-delta' 等价(见 README)。
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
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
const MAX_HISTORY_WAIT_ATTEMPTS = 60; // not_visible 供给窗口期:30s × 60 ≈ 30 分钟(实测可超 10 分钟)
const MARKER_TYPE = "omp-delta.attachment"; // 写入 omp 会话的绑定标记(appendEntry,不进 LLM 上下文)
// 供给窗口期签名:append 错误分类与连接重试共用一套,保持同步
const NOT_VISIBLE_RE = /not_visible|history is unavailable/i;
const PROVISIONING_RE = /history is unavailable|not_visible|not in this account|invalid_reference|worktree_storage_failed/i;

type DeltaRecord = {
	seq: number;
	turnSequence?: number;
	update: Record<string, unknown>;
};

/** 每个omp会话一份的运行时状态(工厂闭包持有,互不共享;一切身份/游标均为内存态)。
 *  权威:线程身份与送达游标在 DeltaDB,绑定关系在 omp session JSONL(MARKER_TYPE)。 */
type Runtime = {
	pi: any;
	cwd: string;
	sid: string;
	isSub: boolean;
	connected: boolean;
	// 线程身份(内存态)
	threadId: string | null;
	worktreeRef: string | null;
	visibility: "private" | "organization" | null;
	url: string | null;
	titleSent: boolean;
	// 送达游标(内存态;next_sequence 对齐后 seqReady 才放行 emit)
	seq: number;
	lastAcked: number;
	pending: DeltaRecord[];
	seqReady: boolean;
	currentTurn: number | null; // running 记录的 seq;turn 内所有记录共用
	paused: boolean;
	expectingIdentity: boolean; // 重连在途:事件照常入临时队列,游标到达后重编号
	retryAttempts: number;
	historyWaitAttempts: number;
	connectAttempts: number;
	eventCtx: any; // session_start 的事件 ctx:持有托管 setTimeout
	detaching: boolean;
	mountChild: ChildProcess | null;
	watchChild: ChildProcess | null;
};

// ---- 无状态 Delta CLI 进程封装(模块级) ----

let deltaBin: string | null = null;
let envDumped = false;

/** #11 诊断:delta 在 omp 内 spawn 时报 NotFound、外部终端同命令秒通。
 *  首次失败时把解析到的二进制与关键 env 写进 omp 日志,定位环境差异。 */
function dumpSpawnEnvOnce() {
	if (envDumped) return;
	envDumped = true;
	const keys = ["PATH", "LOCALAPPDATA", "APPDATA", "USERPROFILE", "HOME", "OMP_SESSION_ID", "DELTA_DATA_DIR", "DELTA_CONFIG_DIR"];
	const snap = keys
		.map((k) => k + "=" + (process.env[k] === undefined ? "<unset>" : String(process.env[k]).slice(0, 200)))
		.join(" | ");
	try {
		console.error("[omp-delta] spawn env snapshot: binary=" + (deltaBin ?? "<unresolved>") + " ; " + snap);
	} catch {
		/* console 不可用则跳过 */
	}
}

function resolveDelta(): string {
	if (deltaBin) return deltaBin;
	const lc = process.env.LOCALAPPDATA ?? "";
	const candidates = [
		"delta",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta.exe") : "",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta") : "",
		"/Applications/Delta.app/Contents/MacOS/delta", // macOS: App Bundle 内置 CLI,未装 PATH 时
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
		let done = false;
		const finish = (code: number) => {
			if (done) return;
			done = true;
			clearTimeout(timer);
			clearTimeout(hardKill);
			dumpSpawnEnvOnce(); // 首次调用即记录:对比 omp 内/外 delta 的运行环境(#11)
			resolve({ code, out, err });
		};
		const timer = setTimeout(() => {
			try {
				child.kill();
			} catch {
				/* 已退出 */
			}
		}, opts?.timeoutMs ?? APPEND_TIMEOUT_MS);
		// Windows 上 SIGTERM 后孙进程可能仍握住 stdio 管道,close 事件永不触发:
		// 兜底二次强杀,并以 exit 事件(进程死亡即发)解析,不依赖 close。
		const hardKill = setTimeout(() => {
			try {
				child.kill("SIGKILL");
			} catch {
				/* 已退出 */
			}
		}, (opts?.timeoutMs ?? APPEND_TIMEOUT_MS) + 1_000);
		child.stdout?.on("data", (d) => {
			out += String(d);
		});
		child.stderr?.on("data", (d) => {
			err += String(d);
		});
		child.on("exit", (code) => finish(code ?? -1));
		child.on("error", (e) => {
			finish(-1);
			void e;
		});
		if (opts?.input !== undefined) child.stdin?.write(opts.input);
		child.stdin?.end();
	});
}

export function parseJsonLines(text: string): any[] {
	const t = text.trim();
	// 以 [ / { 开头的输出优先整体解析:pretty-printed JSON(如 thread list)的行里
	// 恰好整行都是字符串字面量时,逐行解析会得到一堆垃圾字符串,
	// 导致"看起来解析成功"而永远不走兜底 —— 必须先试整体。JSONL 多值流整体解析失败,自然落到逐行。
	if (t.startsWith("[") || t.startsWith("{")) {
		try {
			return [JSON.parse(t)];
		} catch {
			/* 多值 JSONL 流:落回逐行 */
		}
	}
	const result: any[] = [];
	for (const line of t.split(/\r?\n/)) {
		const line2 = line.trim();
		if (!line2) continue;
		try {
			result.push(JSON.parse(line2));
		} catch {
			/* 非 JSON 行,忽略 */
		}
	}
	return result;
}

async function runDeltaJson(args: string[], opts?: { input?: string; timeoutMs?: number; cwd?: string }): Promise<any> {
	const r = await runDelta(args, opts);
	try {
		return runDeltaJsonFromText(r.out, r.err);
	} catch {
		throw new Error("delta " + args.join(" ") + " 无 JSON 输出: " + (r.err || r.out).slice(0, 400));
	}
}

/** 已有原始输出时的 JSON 解析(JSONL 优先,整体 JSON 兜底)。 */
function runDeltaJsonFromText(out: string, err: string): any {
	const js = parseJsonLines(out).concat(parseJsonLines(err));
	if (js.length === 0) {
		throw new Error("no JSON in output: " + (err || out).slice(0, 400));
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

// ---- 纯函数接缝(可直接单测;合约语义见 docs/contract-notes.md) ----

/** 实测回执形状:
 *  {"operation":"appended","sequence":N,"next_sequence":N+1,"duplicate":false,
 *   "disposition":"recorded","turn_sequence":3,"version":"…"}
 *  确认结论在 disposition(recorded/duplicate/fenced);兼容旧式 status 字段。 */
export function collectReceipts(j: any, into: Map<number, string>) {
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

/** 从 lastAcked 起找最大连续确认前缀(有空洞时不越过未确认记录)。
 *  pending 只含未确认记录,bySeq 以它为准:超出 pending 的回执不能延长前缀。 */
export function contiguousAck(pending: DeltaRecord[], lastAcked: number, receipts: Map<number, string>): number {
	const acked = new Map<number, boolean>();
	for (const [seq, st] of receipts) acked.set(seq, st === "recorded" || st === "duplicate");
	const bySeq = new Map<number, DeltaRecord>();
	for (const r of pending) bySeq.set(r.seq, r);
	let seq = lastAcked;
	while (acked.get(seq + 1) === true && bySeq.has(seq + 1)) seq += 1;
	return seq;
}

export type AppendClassification = {
	fenced: boolean;
	retryable: boolean;
	notVisible: boolean;
	retryAfter: number;
	errMsg: string;
};

/** 把 append 的退出码/输出/回执归类:接管、可重试、供给窗口、退避时长、错误消息。 */
export function classifyAppendResult(
	code: number,
	err: string,
	out: string,
	receipts: Map<number, string>,
): AppendClassification {
	let fenced = false;
	let retryable = false;
	let notVisible = false;
	let retryAfter = 5_000;
	let errMsg = "";
	for (const [, st] of receipts) if (st === "fenced") fenced = true;
	if (code !== 0) {
		for (const j of parseJsonLines(err).concat(parseJsonLines(out))) {
			if (j?.error) {
				errMsg = String(j.error.message ?? "").slice(0, 300) || (err || out).slice(0, 300);
				if (j.error.retryable === true) {
					retryable = true;
					retryAfter = Number(j.error.retry_after_ms) || 5_000;
				}
				// 新线程的 history 供给窗口期(实测可达数十分钟):暂不可见但可自愈,
				// 统一用 30s 长退避,不采信服务端短值
				const reason = String(j.error.reason ?? "");
				if (reason === "not_visible" || NOT_VISIBLE_RE.test(errMsg)) {
					notVisible = true;
					retryable = true;
					retryAfter = 30_000;
				}
			}
		}
		// JSON 解析失败时的原始文本兑底:CLI 某些报错不带 JSON
		const rawText = String(err || "") + String(out || "");
		if (!retryable && !notVisible && NOT_VISIBLE_RE.test(rawText)) {
			notVisible = true;
			retryable = true;
			retryAfter = 30_000;
			if (!errMsg) errMsg = "history not provisioned yet";
		}
		if (!errMsg) errMsg = (err || out || "exit " + code).slice(0, 300);
	}
	if (code === -1) {
		// spawn 失败或超时被杀:没有 JSON 错误体,按可重试故障处理
		retryable = true;
		if (!errMsg) errMsg = "delta 进程无响应或被终止";
	}
	return { fenced, retryable, notVisible, retryAfter, errMsg };
}

/** append 请求体:每行一个 envelope,schemaVersion + sequence(+ turnSequence)。 */
export function buildAppendEnvelope(pending: DeltaRecord[]): string {
	return (
		pending
			.map((r) => {
				const env: any = { schemaVersion: SCHEMA_VERSION, sequence: r.seq, update: r.update };
				if (r.turnSequence !== undefined) env.turnSequence = r.turnSequence;
				return JSON.stringify(env);
			})
			.join("\n") + (pending.length ? "\n" : "")
	);
}

/** 供给窗口期错误:服务端尚未可见,可后台重试。 */
export function isTransientProvisioning(msg: string): boolean {
	return PROVISIONING_RE.test(msg);
}

/** 参照 delta-claude:实时活动带 _meta.delta 来源标记,否则外部 producer 的 agent 输出不会被渲染进 transcript。 */
export function liveActivity(update: Record<string, unknown>): Record<string, unknown> {
	const kind = update.sessionUpdate;
	const activityKind =
		kind === "user_message" || kind === "agent_message" || kind === "agent_thought"
			? String(kind)
			: kind === "tool_call_update"
				? "tool_call"
				: undefined;
	if (!activityKind) return update;
	return {
		...update,
		_meta: {
			delta: { work_origin: "external_agent", interaction_surface: "cli", activity_kind: activityKind },
		},
	};
}

/** 路径归一化比较(Windows/POSIX 分隔符与大小写)。 */
function normPath(p: string): string {
	return p.replace(/[\\/]+/g, "/").replace(/\/$/, "").toLowerCase();
}

// ---- 身份与游标:服务端为权威,本地不落盘 ----

export type DeltaIdentity = {
	threadId: string;
	url: string;
	visibility: "private" | "organization" | undefined;
	externalId: string;
	nextSequence: number;
	worktrees: any[];
};

/** 从 `thread status` 解析可续录的线程身份。
 *  注意:join url 是 share id(thread_01…),与 thread_id 无派生关系,只能取字段本身。 */
export function resolveIdentityFromStatus(status: any): DeltaIdentity | null {
	const threadId = String(status?.thread_id ?? "");
	if (!threadId) return null;
	return {
		threadId,
		url: String(status?.url ?? ""),
		visibility: status?.visibility === "organization" ? "organization" : status?.visibility === "private" ? "private" : undefined,
		externalId: String(status?.producer?.external_id ?? ""),
		nextSequence: Number(status?.producer?.next_sequence ?? 0) || 0,
		worktrees: Array.isArray(status?.worktrees?.items) ? status.worktrees.items : [],
	};
}

export type WorktreePick = { ref: string; ambiguous: boolean };

/** 一个 worktree 条目上报的所有 checkout 目录:顶层 path/directory + 嵌套 checkouts[].directory
 *  (实采形状:delta 会把 checkout 物化在 <cwd>/.delta/worktrees/<uuid>/… 下,state 可为 inactive)。 */
function worktreeDirs(item: any): string[] {
	const dirs: string[] = [];
	const direct = String(item?.path ?? item?.directory ?? "");
	if (direct) dirs.push(direct);
	for (const c of Array.isArray(item?.checkouts) ? item.checkouts : []) {
		const d = String(c?.directory ?? "");
		if (d) dirs.push(d);
	}
	return dirs;
}

/** 从 worktree 列表里挑出当前 checkout 的身份;多个匹配必须报歧义,不静默选择。
 *  匹配规则:目录相等,或是当前 cwd 下 .delta/ 受管子树(delta 物化的 checkout)。 */
export function pickWorktreeForCwd(items: any[], cwd: string): WorktreePick {
	const base = normPath(cwd);
	const matches = items.filter((x) =>
		worktreeDirs(x).some((d) => {
			const n = normPath(d);
			return n === base || n.startsWith(base + "/.delta/");
		}),
	);
	if (matches.length > 1) return { ref: "", ambiguous: true };
	return { ref: matches.length === 1 ? String(matches[0]?.reference ?? "") : "", ambiguous: false };
}

export type SequenceState = { pending: DeltaRecord[]; seq: number; lastAcked: number };

/** 用服务端游标(next_sequence = 已送达条数)对齐本地队列:
 *  丢掉服务端已有的记录,序列号从服务端游标继续 —— 重启/换机后零本地状态也能衔接。 */
export function reconcileSequenceState(pending: DeltaRecord[], serverNext: number): SequenceState {
	const cursor = Math.max(0, serverNext - 1); // 服务端已确认到的最大 seq
	const kept = pending.filter((r) => r.seq > cursor);
	let seq = cursor;
	for (const r of kept) if (r.seq > seq) seq = r.seq;
	return { pending: kept, seq, lastAcked: cursor };
}

export type RenumberResult = { pending: DeltaRecord[]; seq: number; lastAcked: number; seqMap: Map<number, number> };

/** 首次对齐用:临时队列里的记录从未上过服务端,可安全重编号衔接服务端游标;
 *  内部 turnSequence 引用(指向同队列的 running 记录)经同一映射修正。原地改写 pending。 */
export function renumberFrom(pending: DeltaRecord[], serverNext: number): RenumberResult {
	const seqMap = new Map<number, number>();
	let s = Math.max(1, serverNext); // 全新 producer 从 1 起;已有游标从 next_sequence 继续
	for (const r of pending) {
		seqMap.set(r.seq, s);
		if (r.turnSequence !== undefined && seqMap.has(r.turnSequence)) {
			r.turnSequence = seqMap.get(r.turnSequence);
		}
		r.seq = s;
		s += 1;
	}
	return { pending, seq: s - 1, lastAcked: Math.max(0, serverNext - 1), seqMap };
}

// ---- 扩展工厂:每会话一份 Runtime ----

export default function ompDelta(pi: any) {
	const rt: Runtime = {
		pi,
		cwd: process.cwd(),
		sid: "",
		isSub: false,
		connected: false,
		threadId: null,
		worktreeRef: null,
		visibility: null,
		url: null,
		titleSent: false,
		seq: 0,
		lastAcked: 0,
		pending: [],
		seqReady: false,
		currentTurn: null,
		paused: false,
		expectingIdentity: false,
		retryAttempts: 0,
		historyWaitAttempts: 0,
		connectAttempts: 0,
		eventCtx: null,
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

	function shouldRecord(): boolean {
		// 重连在途(expectingIdentity)时也记录:进临时队列,游标到达后安全重编号;
		// 没有任何绑定前景时才丢弃。
		return !rt.isSub && !rt.paused && (rt.seqReady || rt.expectingIdentity);
	}

	function nextSeq(): number {
		rt.seq += 1;
		return rt.seq;
	}

	/** 分配 sequence、入队、尝试推送。无绑定前景或已暂停时返回 null。 */
	function emit(update: Record<string, unknown>, opts?: { turnSequence?: number; sequence?: number }): number | null {
		// 记录只进内存队列:权威内容在 omp session JSONL,权威送达游标在服务端。
		// seqReady 前的记录走临时编号,游标到达时由 adoptServerCursor 重编号。
		if (!shouldRecord()) return null;
		const seq = opts?.sequence ?? nextSeq();
		const rec: DeltaRecord = { seq, update: liveActivity(update) };
		if (opts?.turnSequence !== undefined) rec.turnSequence = opts.turnSequence;
		rt.pending.push(rec);
		void flush();
		return seq;
	}

	let flushing = false;

	/** 定时调度:优先事件 ctx 的托管定时器(异常隔离),否则原生 setTimeout 兜底。 */
	function schedule(fn: () => void, ms: number) {
		const managed = (rt.eventCtx ?? lastCtx)?.setTimeout?.(fn, ms);
		if (!managed) {
			const g = globalThis as any;
			(g.setTimeout ?? setTimeout)(fn, ms);
		}
	}

	/** 把未确认记录推给 Delta;可重试错误自动退避,重试耗尽则暂停。 */
	async function flush(): Promise<void> {
		if (!rt.threadId || !rt.seqReady || flushing || rt.paused) return;
		flushing = true;
		try {
			const pending = rt.pending.filter((r) => r.seq > rt.lastAcked);
			if (pending.length === 0) {
				rt.retryAttempts = 0;
				return;
			}

			// 已确认记录已从队列移除;重发未确认记录安全(Delta 返回 duplicate)
			const body = buildAppendEnvelope(pending);

			const r = await runDelta(
				[
					"thread",
					"append",
					rt.threadId,
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

			const cls = classifyAppendResult(r.code, r.err, r.out, receipts);

			if (cls.fenced) {
				// 该 turn 已被他人接管:合约要求停止本地 turn(已执行的工具效果无法撤销)
				notify("Delta:该 turn 已被他人接管,本地 turn 已停止。", "warning");
				const ack = contiguousAck(pending, rt.lastAcked, receipts);
				rt.lastAcked = Math.max(rt.lastAcked, ack);
				rt.pending = rt.pending.filter((rec) => rec.seq > rt.lastAcked);
				rt.currentTurn = null;
				try {
					lastCtx?.abort?.();
				} catch {
					/* 无进行中 turn */
				}
				return;
			}

			const newAck = contiguousAck(pending, rt.lastAcked, receipts);
			if (newAck > rt.lastAcked) {
				rt.lastAcked = newAck;
				// 已确认记录出队(内存队列,无需落盘)
				rt.pending = rt.pending.filter((rec) => rec.seq > rt.lastAcked);
				rt.retryAttempts = 0;
				rt.historyWaitAttempts = 0;
			}

			if (r.code !== 0 && rt.lastAcked < rt.seq) {
				if (cls.retryable && cls.notVisible && rt.historyWaitAttempts < MAX_HISTORY_WAIT_ATTEMPTS) {
					rt.historyWaitAttempts += 1;
					notify(
						"Delta 线程供给中,等待可见("
						+ rt.historyWaitAttempts + "/" + MAX_HISTORY_WAIT_ATTEMPTS + "):" + cls.errMsg,
					);
					schedule(() => {
						void flush();
					}, cls.retryAfter);
				} else if (cls.retryable && rt.retryAttempts < MAX_AUTO_RETRIES) {
					rt.retryAttempts += 1;
					notify(
						"Delta 推送失败,自动重试 " + rt.retryAttempts + "/" + MAX_AUTO_RETRIES + "(" + cls.errMsg + ")",
						"warning",
					);
					schedule(() => {
						void flush();
					}, cls.retryAfter);
				} else {
					rt.paused = true;
					notify("Delta 推送失败,已暂停自动重试:" + cls.errMsg + "(用 /delta-retry 恢复)", "warning");
				}
			}
			if (r.code === 0 && rt.lastAcked < rt.seq) {
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
		} catch (e) {
			log("flush 异常: " + String(e));
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
		// exit 而非 close:Windows 上孙进程握住管道时 close 永不触发(watch 断开误报为未断开)
		child.on("exit", (code) => {
			try {
				rl.close();
			} catch {
				/* 已关闭 */
			}
			onClose(code ?? -1);
		});
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
				String(rt.threadId),
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
				if (!rt.detaching && rt.connected) {
					notify("Delta watch 断开(exit " + code + "),评论回流暂停;/delta-retry 可重连。", "warning");
				}
			},
		);
	}

	/** 已有线程:挂载 worktree + 启动 watch + 推送积压记录。 */
	/** 从 omp 会话条目里取最新的绑定标记(appendEntry 写入;无则 null)。 */
	function findSessionMarker(ctx: any): { threadId: string; worktreeRef?: string; cwd?: string } | null {
		try {
			const entries: any[] = ctx?.sessionManager?.getEntries?.() ?? [];
			let marker: { threadId: string; worktreeRef?: string } | null = null;
			for (const e of entries) {
				if (e?.type === "custom" && e?.customType === MARKER_TYPE && e?.data?.threadId) {
					marker = e.data; // 取最后一个 = 最新绑定
				}
			}
			return marker;
		} catch {
			return null; // 无会话读取能力时只能手动 /delta-share
		}
	}

	/** 把对齐结果落进运行时:服务端游标为准,seqReady 放行 flush/emit。 */
	function applySequenceState(rc: { pending: DeltaRecord[]; seq: number; lastAcked: number }) {
		rt.pending = rc.pending;
		rt.seq = rc.seq;
		rt.lastAcked = rc.lastAcked;
		rt.seqReady = true;
	}

	/** 游标到达时的统一入口:已发送过的队列用 reconcile(丢已送达、保未送达),
	 *  临时队列(重连在途期间排队的记录)用 renumberFrom(从未上服务端,安全重编号,
	 *  内部 turnSequence 引用与 currentTurn 一并映射)。 */
	function adoptServerCursor(serverNext: number) {
		if (rt.seqReady) {
			applySequenceState(reconcileSequenceState(rt.pending, serverNext));
			return;
		}
		const r = renumberFrom(rt.pending, serverNext);
		rt.pending = r.pending;
		rt.seq = r.seq;
		rt.lastAcked = r.lastAcked;
		rt.seqReady = true;
		if (rt.currentTurn !== null && r.seqMap.has(rt.currentTurn)) {
			rt.currentTurn = r.seqMap.get(rt.currentTurn)!;
		}
	}

	async function attachAndStream(): Promise<void> {
		rt.detaching = false;
		const ref = String(rt.worktreeRef ?? "");
		if (!ref) throw new Error("缺少 worktree reference,请重新 /delta-share。");
		const a0 = await runDelta(["worktree", "attach", ref, "--thread", String(rt.threadId)], { cwd: rt.cwd });
		let a = a0;
		// 新线程的 history/worktree 供给需要数秒到数分钟;这些签名都可退避重试:
		// not in this account / invalid_reference(服务端还没见到线程)、history is unavailable(供给窗口)
		for (let i = 0; a.code !== 0 && i < 20; i++) {
			const blob = String(a.err || a.out);
			if (!/not in this account|invalid_reference|history is unavailable|not_visible/i.test(blob)) break;
			await new Promise<void>((r) => {
				const t = (rt.eventCtx ?? lastCtx)?.setTimeout?.(r, 3_000);
				if (!t) setTimeout(r, 3_000);
			});
			a = await runDelta(["worktree", "attach", ref, "--thread", String(rt.threadId)], { cwd: rt.cwd });
		}
		// 重连场景:线程已挂过同一 checkout 时 attach 报错,但挂载关系仍在,直接复用
		if (a.code !== 0 && !String(a.err || a.out).includes("already has a worktree")) {
			throw new Error("worktree attach 失败: " + String(a.err || a.out).slice(0, 300));
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
					if (!rt.detaching && rt.connected) {
						notify("Delta worktree 同步进程退出(exit " + code + ")。", "warning");
					}
				},
			);
			// mount 启动慢时不无限等;进程仍在后台,就绪后会自行上传
			schedule(() => settle(), MOUNT_READY_TIMEOUT_MS);
		});

		rt.connected = true;
		rt.connectAttempts = 0;
		startWatch();
		void flush();
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
		// 身份与游标:thread create 幂等(source+external-id 相同即同一线程)。
		// status 在供给窗口期可能 not_visible:此时按全新线程处理(seq 从 0),
		// 让首条记录先上服务端撑开 history 容器 —— 等容器打开后,后续连接的
		// reconcile 会以服务端游标为准校正。不能让 status 失败阻断首条记录,
		// 否则空容器永远等不到第一条记录(实测死锁 >90 分钟)。
		rt.threadId = String(created?.thread_id ?? "");
		rt.visibility = mode;
		if (!rt.threadId) {
			throw new Error("thread create 未返回 thread_id: " + JSON.stringify(created).slice(0, 300));
		}
		let id: DeltaIdentity | null = null;
		try {
			const st = await runDeltaJson(
				["thread", "status", rt.threadId, "--source", SOURCE, "--external-id", rt.sid],
				{ cwd: rt.cwd },
			);
			id = resolveIdentityFromStatus(st);
		} catch {
			/* 供给窗口期不可见:按空线程继续 */
		}
		if (id) rt.url = id.url ?? null;
		adoptServerCursor(id?.nextSequence ?? 0);

		// 立即推首条记录:空线程不会上传服务端,attach/status 会 invalid_reference
		if (!rt.titleSent) {
			rt.titleSent = true;
			let title = "";
			try {
				title = String((await pi?.getSessionName?.()) ?? "");
			} catch {
				/* 无会话名 */
			}
			emit({ sessionUpdate: "session_info_update", title: (title || "omp session").slice(0, 120) });
		}

		// worktree 身份(参考 delta-claude checkoutReference 语义):
		// list 优先复用同 checkout 的既有身份,不盲目 create;多个身份报歧义让人选。
		const list = await runDeltaJson(["worktree", "list"], { cwd: rt.cwd });
		const rows: any[] = Array.isArray(list) ? list : (list?.worktrees ?? list?.items ?? []);
		const pick = pickWorktreeForCwd(rows, rt.cwd);
		if (pick.ambiguous) {
			throw new Error("该 checkout 存在多个 worktree 身份,请先在 Delta 里选择正确的一个后重试。");
		}
		let ref = pick.ref;
		if (!ref) {
			const w = await runDeltaJson(["worktree", "create", rt.cwd], { cwd: rt.cwd });
			ref = String(w?.reference ?? "");
		}
		if (!ref) throw new Error("worktree create/list 均未取得 reference。");
		rt.worktreeRef = ref;

		await attachAndStream();

		// 绑定标记写入 omp 会话记录(官方 appendEntry:持久化但不进 LLM 上下文);
		// 重启后凭它重连,fork 后凭 external_id 不匹配拒绝静默续录;
		// cwd 用于重连时检测跨目录恢复,防把旧线程挂到错误 checkout。
		try {
			pi?.appendEntry?.(MARKER_TYPE, {
				threadId: rt.threadId,
				worktreeRef: rt.worktreeRef,
				cwd: rt.cwd,
			});
		} catch {
			/* 标记失败不影响本次连接;下次需手动 /delta-share */
		}

		notify(
			"Delta 线程已连接:" +
				rt.threadId +
				"\n浏览器打开: " +
				(rt.url ?? "https://delta.dev/join/" + rt.threadId) +
				"\n(对话与工作区文件,含未提交改动,正在同步到 Delta)",
		);
	}

	/** 后台重试连接(供给窗口期实测可达 10+ 分钟,预算 40×30s)。 */
	async function retryConnect(mode: "private" | "organization", org: { id: string; name: string }): Promise<void> {
		try {
			await connectShared(mode, org);
		} catch (e) {
			const msg = String(e instanceof Error ? e.message : e);
			if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
				rt.connectAttempts += 1;
				if (rt.connectAttempts % 5 === 0)
					notify("Delta 供给窗口等待中(第 " + rt.connectAttempts + " 次重试):" + msg);
				schedule(() => {
					void retryConnect(mode, org);
				}, 30_000);
				return;
			}
			notify("Delta 重连失败:" + msg, "warning");
		}
	}

	function detach(quiet = false) {
		rt.detaching = true;
		killChild(rt.mountChild);
		rt.mountChild = null;
		killChild(rt.watchChild);
		rt.watchChild = null;
		rt.connected = false;
		if (!quiet) notify("已断开 Delta 同步(线程与已上传内容保留)。");
	}

	/** 慢路径发现:moveOff/fork 后本会话无 marker,改从服务端找"绑定过当前 checkout
	 *  的最近 omp 线程"(thread list → 逐个 status → worktrees 匹配 cwd)。
	 *  权威在 DeltaDB,不依赖会话 id 的连续性 —— omp 的 continuation 链对它透明。 */
	async function discoverBindingByCheckout(): Promise<{ threadId: string; worktreeRef?: string } | null> {
		const lr = await runDelta(["thread", "list"], { cwd: rt.cwd });
		log("discovery: thread list code=" + lr.code + " out[0:180]=" + lr.out.slice(0, 180).replace(/\s+/g, " ") + " err[0:120]=" + lr.err.slice(0, 120).replace(/\s+/g, " "));
		const list = (() => {
			try {
				return runDeltaJsonFromText(lr.out, lr.err);
			} catch {
				return null;
			}
		})();
		const rows: any[] = Array.isArray(list)
			? list
			: Array.isArray(list?.threads)
				? list.threads
				: Array.isArray(list?.items)
					? list.items
					: [];
		const sorted = rows
			.slice()
			.sort((a, b) => String(b?.created_at_utc ?? "").localeCompare(String(a?.created_at_utc ?? "")));
		log("discovery: rows=" + rows.length + " probing top " + Math.min(8, sorted.length) + " cwd=" + rt.cwd);
		for (const row of sorted.slice(0, 8)) {
			const tid = String(row?.id ?? "");
			if (!tid) continue;
			try {
				const st = await runDeltaJson(
					["thread", "status", tid, "--source", SOURCE, "--external-id", rt.sid],
					{ cwd: rt.cwd },
				);
				const id = resolveIdentityFromStatus(st);
				if (!id) {
					log("discovery: " + tid.slice(0, 12) + " no identity");
					continue;
				}
				const pick = pickWorktreeForCwd(id.worktrees, rt.cwd);
				log("discovery: " + tid.slice(0, 12) + " wtDirs=" + JSON.stringify(id.worktrees).slice(0, 260) + " pick=" + JSON.stringify(pick));
				if (pick.ref && !pick.ambiguous) return { threadId: tid, worktreeRef: pick.ref };
			} catch (e) {
				log("discovery: " + tid.slice(0, 12) + " status failed: " + String(e).slice(0, 160));
			}
		}
		log("discovery: no match");
		return null;
	}

	/** 凭会话标记重连:status 校验 external_id(防 fork 静默续录)→ 对齐服务端游标 → attach。
	 *  身份权威在 DeltaDB:status 失败/线程不属于本会话时给明确指引,不静默重建。
	 *  决定性失败(mismatch/歧义/跨目录)会关闭 expectingIdentity;暂时性错误保留它,记录继续排队。 */
	async function reconnectByIdentity(marker: {
		threadId: string;
		worktreeRef?: string;
		cwd?: string;
	}): Promise<void> {
		try {
			const st = await runDeltaJson(
				["thread", "status", String(marker.threadId), "--source", SOURCE, "--external-id", rt.sid],
				{ cwd: rt.cwd },
			);
			const id = resolveIdentityFromStatus(st);
			if (!id) throw new Error("thread status 未返回 thread_id");
			// omp -r 恢复会话会 fork 出新 sid:marker 在、external_id 不同 → 以新 producer
			// 身份重绑同一线程(per-producer 游标独立,不会重复);无需用户重新 /delta-share。
			if (id.externalId && id.externalId !== rt.sid) {
				notify("会话已分叉(" + id.externalId + " → " + rt.sid + "):以新 producer 续录同一线程。");
			}
			rt.threadId = id.threadId;
			rt.url = id.url || null;
			rt.visibility = id.visibility ?? null;
			const pick = pickWorktreeForCwd(id.worktrees, rt.cwd);
			if (pick.ambiguous) {
				notify("该线程挂了多个 worktree 身份,请 /delta-share 重新绑定。", "warning");
				rt.expectingIdentity = false;
				return;
			}
			let ref = pick.ref;
			if (!ref) {
				// status 没列出匹配当前目录的 worktree:标记目录也不同时拒绝,防把旧线程挂到错误 checkout
				if (marker.cwd && normPath(marker.cwd) !== normPath(rt.cwd)) {
					notify(
						"该 Delta 线程绑定的是 " + marker.cwd + " 的 checkout,当前目录是 " + rt.cwd
							+ ";请在原目录打开 omp,或重新 /delta-share(旧线程保留在 Delta)。",
							"warning",
						);
						rt.expectingIdentity = false;
						return;
					}
				ref = String(marker.worktreeRef ?? "");
			}
			rt.worktreeRef = ref;
			adoptServerCursor(id.nextSequence);
			await attachAndStream();
			notify("Delta 已重连: " + (rt.url ?? rt.threadId));
		} catch (e) {
			// 暂时性错误(含供给窗口):expectingIdentity 保留,记录继续排队;/delta-retry 可再试
			notify("Delta 重连失败:" + String(e instanceof Error ? e.message : e) + "(/delta-retry 可再试)", "warning");
		}
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
			if (rt.connected) {
				notify("已连接:" + rt.threadId);
				return;
			}
			const mode: "private" | "organization" = String(args ?? "").trim().toLowerCase().startsWith("priv")
				? "private"
				: "organization";
			// 合约要求：共享前必须征得用户同意，并告知可见范围（组织名+内容）
			let ok = true;
			let org: { id: string; name: string };
			try {
				org = await resolveOrg();
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
			} catch (e) {
				notify("准备连接失败：" + String(e instanceof Error ? e.message : e), "warning");
				return;
			}
			try {
				await connectShared(mode, org);
			} catch (e) {
				const msg = String(e instanceof Error ? e.message : e);
				if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
					rt.connectAttempts += 1;
					if (rt.connectAttempts === 1 || rt.connectAttempts % 5 === 0)
						notify("Delta 供给窗口等待中(第 " + rt.connectAttempts + " 次重试):" + msg);
					schedule(() => {
						void retryConnect(mode, org);
					}, 30_000);
					return;
				}
				notify("连接失败:" + msg, "warning");
			}
		},
	});

	pi.registerCommand("delta-status", {
		description: "查看 omp-delta 连接与同步状态",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			const lines = [
				"连接: " + (rt.connected ? "已连接" : "未连接") + (rt.paused ? "(推送暂停)" : ""),
				"线程: " + (rt.threadId ?? "无"),
				rt.threadId ? "可见性: " + (rt.visibility ?? "未知") : "",
				rt.threadId ? "链接: " + (rt.url ?? "(尚未获取,重连后自动更新)") : "",
				"记录: 已分配 " + rt.seq + " 条,已确认 " + rt.lastAcked + " 条,待发 " + rt.pending.length + " 条",
				"worktree: " + (rt.worktreeRef ?? "无"),
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
			if (!rt.threadId) {
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
			rt.historyWaitAttempts = 0; // 供给窗口可能超过旧预算:手动重试必须重新武装长预算
			rt.connectAttempts = 0;
			if (rt.threadId && !rt.connected) {
				notify("尝试重连 Delta 线程 " + rt.threadId + " …");
				try {
					await attachAndStream();
					notify("已重连。");
				} catch (e) {
					notify("重连失败:" + String(e instanceof Error ? e.message : e), "warning");
					return;
				}
			} else if (!rt.threadId) {
				// 进程重启后内存为空:凭会话标记重连(无标记则提示 /delta-share)
				const marker = findSessionMarker(ctx);
				if (marker) {
					notify("凭会话标记重连 Delta 线程 " + marker.threadId + " …");
					rt.expectingIdentity = true;
					await reconnectByIdentity(marker);
				} else {
					notify("尚未绑定 Delta 线程;用 /delta-share 连接。");
				}
			} else {
				void flush();
			}
		},
	});

	pi.on("session_start", async (_event: any, ctx: any) => {
		lastCtx = ctx;
		rt.eventCtx = ctx; // 托管定时器来源
		rt.isSub = (ctx?.agent?.kind ?? "main") !== "main";
		if (rt.isSub) return;
		rt.sid = String(ctx?.sessionManager?.getSessionId?.() ?? "sess-" + randomUUID());
		rt.cwd = String(ctx?.cwd ?? process.cwd());
		// 绑定标记在 omp 会话记录里(appendEntry 写入,生命周期归 omp 管):
		// 有标记 → 后台凭它重连;无标记 → 等用户显式 /delta-share。
		const marker = findSessionMarker(ctx);
		if (marker) {
			// 不阻塞 session_start(事件处理器有 30s 预算):后台重连;
			// 等待期间事件照常入临时队列(expectingIdentity),游标到达后重编号补发
			rt.expectingIdentity = true;
			notify("发现会话绑定 Delta 线程 " + marker.threadId + ",后台重连中…");
			void reconnectByIdentity(marker);
		} else {
			// 无 marker(moveOff/fork/首次):从服务端按 checkout 发现绑定;
			// 发现不了才是真正的首次,等用户显式 /delta-share。
			rt.expectingIdentity = true;
			void (async () => {
				try {
					const found = await discoverBindingByCheckout();
					if (found) {
						notify("发现绑定当前 checkout 的 Delta 线程 " + found.threadId + ",后台重连中…");
						await reconnectByIdentity(found);
					} else {
						rt.expectingIdentity = false;
					}
				} catch (e) {
					rt.expectingIdentity = false;
					notify("Delta 绑定发现失败:" + String(e instanceof Error ? e.message : e) + "(/delta-share 可手动连接)", "warning");
				}
			})();
		}
	});

	pi.on("session_switch", async () => {
		if (rt.isSub || !rt.threadId) return;
		if (rt.connected || rt.mountChild || rt.watchChild) {
			detach(true);
			notify("会话已切换,Delta 同步已停止;新会话可用 /delta-share 重新连接。");
		}
	});

	pi.on("input", async (event: any) => {
		if (!shouldRecord()) return;
		const text = String(event?.text ?? event?.prompt ?? "").trim();
		if (!text) return;
		if (text.startsWith("/")) return; // slash 命令不是对话内容,不记录
		if (!rt.titleSent) {
			rt.titleSent = true;
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
			messageId: "u" + (rt.seq + 1),
			content: [{ type: "text", text }],
		});
	});

	pi.on("turn_start", () => {
		if (!shouldRecord()) return;
		// 合约要求:running 的 turnSequence 必须等于它自己的 sequence,否则 turn 不会开启,
		// 后续 agent_message/tool_call 会被服务端静默丢弃(不生成 item)。
		const seq = nextSeq();
		emit({ sessionUpdate: "state_update", state: "running" }, { turnSequence: seq, sequence: seq });
		if (seq !== null) rt.currentTurn = seq;
	});

	pi.on("turn_end", () => {
		if (!shouldRecord()) return;
		if (rt.currentTurn !== null) {
			emit({ sessionUpdate: "state_update", state: "idle" }, { turnSequence: rt.currentTurn });
		}
		rt.currentTurn = null;
	});

	pi.on("assistant_message", (event: any) => {
		if (!shouldRecord()) return;
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
				messageId: "a" + (rt.seq + 1),
				content: [{ type: "text", text }],
			},
			rt.currentTurn !== null ? { turnSequence: rt.currentTurn } : undefined,
		);
	});

	pi.on("tool_execution_start", (event: any) => {
		if (!shouldRecord()) return;
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
		if (!shouldRecord()) return;
		const id = String(event?.toolCallId ?? event?.callId ?? "");
		if (!id) return;
		emit(
			{ sessionUpdate: "tool_call_update", toolCallId: id, status: "completed" },
			rt.currentTurn !== null ? { turnSequence: rt.currentTurn } : undefined,
		);
	});

	pi.on("session_shutdown", () => {
		// 处理器只有 2s 预算:只做同步清理,不等待 CLI 调用;无状态需要落盘(零本地状态)
		try {
			killChild(rt.mountChild);
			killChild(rt.watchChild);
		} catch {
			/* 尽力而为 */
		}
		rt.mountChild = null;
		rt.watchChild = null;
	});
}
