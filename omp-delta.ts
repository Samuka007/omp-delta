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
 *   - 命令:/delta(路由)与平铺别名 /delta-share /delta-status /delta-detach /delta-retry /delta-org(只读) /delta-update
 *     /delta-setup(#21):仓库级同步策略 .delta/settings.json(兼容 delta-claude),private/organization 自动连接新会话
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
import * as fs from "node:fs";
import * as path from "node:path";
import * as readline from "node:readline";

const SOURCE = "omp"; // Delta 侧插件固定标识(--source)
const PRODUCER = "omp"; // 线程内显示的生产者名(--producer-name)
const CONTRACT_MIN = 8; // 本插件实现的合约版本
const CONTRACT_MAX = 8;
const SCHEMA_VERSION = 1;
const APPEND_TIMEOUT_MS = 30_000;
const UPDATE_TIMEOUT_MS = 300_000; // delta update 下载+安装远慢于 append:5 分钟预算
const MOUNT_READY_TIMEOUT_MS = 90_000;
const MAX_AUTO_RETRIES = 3;
const MAX_HISTORY_WAIT_ATTEMPTS = 60; // not_visible 供给窗口期:30s × 60 ≈ 30 分钟(实测可超 10 分钟)
const MARKER_TYPE = "omp-delta.attachment"; // 写入 omp 会话的绑定标记(appendEntry,不进 LLM 上下文)
// /delta-update:omp 没有 marketplace 自动更新,插件过期只能手动重装(#20)。
// 无可靠"插件已过期"程序化信号时,这条命令是重装指引的唯一保证(见 renderUpdateReport)。
export const PLUGIN_REINSTALL_CMD = "omp plugin install 'github:Samuka007/omp-delta'";
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
 *  仅在 OMP_DELTA_DEBUG=1 时,首次 spawn 把解析到的二进制与关键 env 写进 omp 日志,定位环境差异。 */
function dumpSpawnEnvOnce() {
		if (envDumped) return;
		if (process.env.OMP_DELTA_DEBUG !== "1") return;
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

// Discovery 顺序对齐参考插件(zed-industries/delta-claude hooks/cli/cli.ts):
// PATH 优先,再按平台尝试安装器的 App Bundle 位置(Linux 默认 ~/.local/delta.app/bin/delta)。
// 不提供 DELTA_BIN 之类的环境变量覆盖,与参考插件保持一致。
function resolveDelta(): string {
	if (deltaBin) return deltaBin;
	const home = process.env.HOME ?? "";
	const lc = process.env.LOCALAPPDATA ?? "";
	const candidates = [
		"delta",
		home ? path.join(home, ".local", "delta.app", "bin", "delta") : "", // Linux: install.sh 默认位置
		home ? path.join(home, "Applications", "Delta.app", "Contents", "MacOS", "delta") : "",
		"/Applications/Delta.app/Contents/MacOS/delta", // macOS: App Bundle 内置 CLI,未装 PATH 时
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta.exe") : "",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta") : "",
	].filter(Boolean);
	for (const c of candidates) {
		if (!isDeltaCli(c)) continue;
		deltaBin = c;
		return c;
	}
	throw new Error(
		"找不到 delta CLI(PATH 中的 delta 若是同名 git pager 会被跳过)。" +
			"请安装 Delta(curl -fsSL https://delta.dev/install.sh | sh)后重启 omp",
	);
}

// `delta --version` 无法区分 Delta CLI 与同名的 git pager(两者都输出 "delta X.Y.Z"),
// 参考插件直接信任 command -v 的结果,但 NixOS 等发行版的 PATH 里 delta 是 git pager,
// 所以这里用 `--help` 检查子命令列表(Delta CLI 独有 org/thread/worktree 等)才算命中。
function isDeltaCli(bin: string): boolean {
	try {
		const r = spawnSync(bin, ["--help"], { windowsHide: true, timeout: 10_000 });
		const out = String(r.stdout) + String(r.stderr);
		return r.status === 0 && /Commands:/.test(out) && /\bthread\b/.test(out) && /\borg\b/.test(out);
	} catch {
		return false;
	}
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

/** delta CLI 未登录时的统一可操作提示(#19:干净错误,不带堆栈)。
 *  实测签名来自 delta.exe 内嵌错误串:not signed in / authentication required。 */
const ORG_UNAUTH_HINT = "Delta CLI 未登录:请先运行 delta auth login(或在 Delta 应用内登录)后重试。";

/** 当前选中的组织(创建线程时必须显式指定)。 */
async function resolveOrg(): Promise<{ id: string; name: string }> {
	const r = await runDelta(["org", "current"]);
	const combined = r.out + "\n" + r.err;
	if (isUnauthenticatedText(combined)) throw new Error(ORG_UNAUTH_HINT);
	const org = parseOrgCurrentFromText(combined);
	if (!org) {
		throw new Error("未能解析当前组织(code " + r.code + "):请用 delta org switch 选择组织后重试。");
	}
	return org;
}

// ---- /delta-share 可见性决策(纯接缝:ui 由调用方注入,测试可传 fake ctx.ui.select) ----

/** pickShareVisibility 的结果:mode null = 用户放弃选择器(取消连接);fallback 非空 = 非交互回落默认(文案交调用方 notify)。 */
export type VisibilityPick = {
	mode: "private" | "organization" | null;
	fallback: string | null;
};

/** /delta-share 可见性决策(#16):
 *  - 显式参数(priv* 前缀 = 私有,其余非空参数 = 组织)→ 直接采用,绝不触碰选择器(向后兼容);
 *  - 无参数 → ctx.ui.select 弹出 Private / Organization(带当前组织名)选择器,先于 consent confirm;
 *  - 非交互(select 缺失/抛错)→ 回落默认 organization 并带回 fallback 文案,绝不抛出;
 *  - 用户放弃选择器(Esc/undefined)→ mode null,调用方按取消处理。 */
export async function pickShareVisibility(
	args: string | null | undefined,
	ui: any,
	org: { id: string; name: string },
): Promise<VisibilityPick> {
	const arg = String(args ?? "").trim().toLowerCase();
	if (arg) {
		// 旧行为:priv 前缀 → 私有,其余显式参数 → 组织
		return { mode: arg.startsWith("priv") ? "private" : "organization", fallback: null };
	}
	const options = [
		{ label: "Private(私有线程,仅自己可见)", description: "不共享给组织" },
		{ label: "Organization(共享给 " + org.name + ")", description: "组织成员可见" },
	];
	const select = ui?.select; // ExtensionUIContext.select(title, options): Promise<string | undefined>,返回所选 label
	if (typeof select !== "function") {
		return { mode: "organization", fallback: "当前环境不支持交互选择,使用默认可见性:共享给组织 " + org.name };
	}
	let label: unknown;
	try {
		label = await select("选择 Delta 线程可见性", options);
	} catch (e) {
		return {
			mode: "organization",
			fallback: "可见性选择器不可用(" + String(e instanceof Error ? e.message : e) + "),使用默认:共享给组织 " + org.name,
		};
	}
	if (typeof label !== "string") {
		return { mode: null, fallback: null }; // 用户放弃(Esc)
	}
	if (label === options[0].label) return { mode: "private", fallback: null };
	// 第二项 / 无法识别的标签都归组织(与旧默认一致;confirm 会再次展示组织名供最终确认)
	return { mode: "organization", fallback: null };
}

// ---- 纯函数接缝(可直接单测;合约语义见 docs/contract-notes.md) ----

// ---- /delta 路由(#15):单一入口;平铺命令保持原样作为别名 ----

/** /delta 路由决策:menu = 无参数(用法菜单,不是错误);route = 已实现子命令(转发同一 handler);
 *  unimplemented = 已知但未实现(join,#17);unknown = 未知子命令。 */
export type DeltaRouteDecision =
	| { kind: "menu" }
	| { kind: "route"; command: "delta-share" | "delta-status" | "delta-detach" | "delta-retry"; args: string }
	| { kind: "unimplemented"; command: "join" }
	| { kind: "unknown"; command: string };

/** 解析 `/delta <子命令> [剩余参数]`(#15):大小写不敏感,空白折叠。
 *  子命令与平铺命令一一对应:share→delta-share、status→delta-status、detach→delta-detach、
 *  retry→delta-retry;join 尚未实现(#17)。/delta-org、/delta-update 不在路由内(有独立拼写,菜单里指引)。 */
export function parseDeltaRouter(args: string): DeltaRouteDecision {
	const parts = String(args ?? "").trim().split(/\s+/).filter(Boolean);
	if (parts.length === 0) return { kind: "menu" };
	const sub = parts[0].toLowerCase();
	const rest = parts.slice(1).join(" ");
	if (sub === "join") return { kind: "unimplemented", command: "join" };
	const route: Record<string, "delta-share" | "delta-status" | "delta-detach" | "delta-retry"> = {
		share: "delta-share",
		status: "delta-status",
		detach: "delta-detach",
		retry: "delta-retry",
	};
	const command = route[sub];
	if (!command) return { kind: "unknown", command: sub };
	return { kind: "route", command, args: rest };
}

/** /delta 无参数时的用法菜单(不是错误)。 */
export function renderDeltaMenu(): string {
	return [
		"delta — omp-delta 命令路由:",
		"  /delta share [private]   连接当前会话(= /delta-share)",
		"  /delta status            查看连接与同步状态(= /delta-status)",
		"  /delta retry             恢复/重试推送(= /delta-retry)",
		"  /delta detach            断开同步(= /delta-detach)",
		"  /delta join <link|id>    加入已有线程(尚未实现:#17)",
		"  其他命令:/delta-setup [private|organization|manual] · /delta-org list|current · /delta-update [check]",
		"  平铺别名继续可用:/delta-share /delta-status /delta-detach /delta-retry",
	].join("\n");
}

/** 未登录判定:对 delta 的 stdout+stderr 原文做签名匹配(不区分 JSON/纯文本)。 */
export function isUnauthenticatedText(text: string): boolean {
	return /not signed in|not logged in|unauthenticated|authentication required|please (?:log|sign) in|unauthorized/i.test(text);
}

export type OrgInfo = { id: string; name: string; current: boolean };

/** 解析 `delta org list --json` 输出:{"operation":"listed","organizations":[{id,name,current}]}
 *  (顺序即 CLI pretty 展示顺序,current:true 恰好一个)。parseJsonLines 统一入口,
 *  pretty-printed 整体 JSON 与逐行 JSONL 都能吃;顶层裸数组也接受。
 *  返回 null = 没有任何组织载荷(失败/未登录);[] = 已登录但未加入任何组织。 */
export function parseOrgListFromText(text: string): OrgInfo[] | null {
	for (const js of parseJsonLines(text)) {
		const arr = Array.isArray(js?.organizations) ? js.organizations : Array.isArray(js) ? js : null;
		if (!arr) continue;
		return arr
			.map((o: any) => ({ id: String(o?.id ?? ""), name: String(o?.name ?? ""), current: o?.current === true }))
			.filter((o) => o.id);
	}
	return null;
}

/** 解析 `delta org current --json` 输出:{"operation":"current","organization":{id,name,current}}。 */
export function parseOrgCurrentFromText(text: string): { id: string; name: string } | null {
	for (const js of parseJsonLines(text)) {
		const id = String(js?.organization?.id ?? "");
		if (id) return { id, name: String(js?.organization?.name ?? id) };
	}
	return null;
}

/** /delta-org list 渲染:沿用 CLI 自身 pretty 约定(行首 `*` 标记当前组织)。 */
export function formatOrgList(orgs: OrgInfo[]): string {
	if (orgs.length === 0) return "未加入任何组织。";
	return orgs
		.map((o) => (o.current ? "* " : "  ") + o.name + " (" + o.id + ")" + (o.current ? " ← 当前:共享将使用此组织" : ""))
		.join("\n");
}

// ---- /delta-setup 仓库同步策略(#21):.delta/settings.json,格式兼容 zed-industries/delta-claude ----
// 格式权威:delta-claude hooks/setup/project-policy.ts(细节记录在 docs/contract-notes.md「Repo policy」)。
// 形状:{"threads":{"mode":"manual|private|organization","organization":"<org id>"}};
// private/organization 要求非空 organization(delta-claude 存 team.id,这里存 delta org list 的 id);
// manual 可带 organization(delta-claude 语义:/delta share 的共享目标,omp 暂不消费);其余键原样保留。
// 注意:仓库里的 .delta/ 目录是 Delta 自己的 worktree 簿记,与这份策略文件无关,忽略规则只认 settings.json 这一个路径。

/** 仓库的同步模式(缺文件/未配置 = manual,现状行为:仅显式 /delta-share)。 */
export type ThreadMode = "manual" | "private" | "organization";

/** 校验后的有效策略。problem 非空 = 文件存在但用不了(manual 兜底,提示只出一次)。 */
export type ProjectPolicy = {
	mode: ThreadMode;
	organization?: string;
	configured: boolean; // 文件刻意配置了策略;缺文件/缺 threads/缺 mode 都算未配置
	problem?: string; // 文件无法使用的原因(session_start 一次性提示,/delta-status 常驻展示)
};

/** 策略文件读取结果:policy = 有效策略;document = 原始文档(重写时未知键不丢);malformed = 解析失败。 */
export type PolicyFile = {
	policy: ProjectPolicy;
	document?: Record<string, unknown>;
	malformed?: boolean;
};

const POLICY_MODES: readonly ThreadMode[] = ["manual", "private", "organization"];

function isJsonObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** 解析并校验策略文本(缺文件传 undefined)。语义与 delta-claude readProjectPolicy 对齐:
 *  缺文件/缺 threads/缺 mode → 未配置 manual(无 problem);malformed JSON/非对象/非法 mode/
 *  自动模式缺 organization → manual 兜底 + problem。 */
export function readProjectPolicy(text: string | undefined): PolicyFile {
	if (text === undefined) return { policy: { mode: "manual", configured: false } };
	let document: unknown;
	try {
		document = JSON.parse(text);
	} catch (e) {
		return { malformed: true, policy: { mode: "manual", configured: false, problem: "不是有效 JSON(" + String(e instanceof Error ? e.message : e) + ")" } };
	}
	if (!isJsonObject(document)) {
		return { malformed: true, policy: { mode: "manual", configured: false, problem: "内容不是 JSON 对象" } };
	}
	const threads = document.threads;
	if (threads === undefined) return { document, policy: { mode: "manual", configured: false } };
	if (!isJsonObject(threads)) {
		return { document, policy: { mode: "manual", configured: false, problem: "threads 的值不是对象" } };
	}
	const mode = threads.mode;
	if (mode === undefined) return { document, policy: { mode: "manual", configured: false } };
	if (typeof mode !== "string" || !POLICY_MODES.includes(mode as ThreadMode)) {
		return { document, policy: { mode: "manual", configured: false, problem: "mode 必须是 " + POLICY_MODES.join(" / ") + " 之一" } };
	}
	const organization = threads.organization;
	if (mode === "manual") {
		// manual 允许带 organization(delta-claude 语义:/delta share 的共享目标)
		const policy: ProjectPolicy = { mode: "manual", configured: true };
		if (typeof organization === "string" && organization.trim()) policy.organization = organization;
		return { document, policy };
	}
	if (typeof organization !== "string" || !organization.trim()) {
		return { document, policy: { mode: "manual", configured: false, problem: 'mode "' + mode + '" 需要非空 organization' } };
	}
	return { document, policy: { mode: mode as ThreadMode, organization, configured: true } };
}

/** 序列化策略:保留上一文档全部未知键(顶层与 threads 内),2 空格缩进 + 末尾换行(同 delta-claude)。
 *  organization === undefined 时删除 threads.organization(manual 不写组织)。 */
export function serializeProjectPolicy(
	previous: Record<string, unknown> | undefined,
	mode: ThreadMode,
	organization?: string,
): string {
	const document = isJsonObject(previous) ? { ...previous } : {};
	const prevThreads = document.threads;
	const threads = isJsonObject(prevThreads) ? { ...prevThreads } : {};
	threads.mode = mode;
	if (organization === undefined) delete threads.organization;
	else threads.organization = organization;
	document.threads = threads;
	return JSON.stringify(document, null, 2) + "\n";
}

/** 策略是否自动记录(无需显式 /delta-share):private / organization。 */
export function recordsAutomatically(policy: ProjectPolicy): boolean {
	return policy.mode === "private" || policy.mode === "organization";
}

/** session_start 的自动同步决策(#21,纯接缝,单测覆盖):
 *  - 既有绑定标记永远优先 → reconnect(auto-connect 不参与竞争,绝不绕过 marker/重连);
 *  - 无标记且策略为自动模式(带组织)→ auto-connect(调用方还要求 checkout 发现未命中);
 *  - 其余(manual/未配置/自动模式缺组织)→ manual,等显式 /delta-share。 */
export type AutoSyncAction =
	| { kind: "reconnect"; reason: "marker" }
	| { kind: "auto-connect"; mode: "private" | "organization"; organization: string }
	| { kind: "manual"; configured: boolean };

export function decideAutoSyncAction(policy: ProjectPolicy, hasMarker: boolean): AutoSyncAction {
	if (hasMarker) return { kind: "reconnect", reason: "marker" };
	if (recordsAutomatically(policy) && policy.organization) {
		return { kind: "auto-connect", mode: policy.mode as "private" | "organization", organization: policy.organization };
	}
	return { kind: "manual", configured: policy.configured };
}

/** /delta-status 的策略行(#21):让"为什么自动连接了/没连"可见。 */
export function renderProjectPolicy(p: ProjectPolicy): string {
	if (p.problem) return "manual(策略文件无法使用:" + p.problem + ")";
	if (!p.configured) return "manual(未配置:仅 /delta-share 手动同步;/delta-setup 可选自动)";
	if (p.mode === "manual") return "manual(仅 /delta-share 手动同步)";
	if (p.mode === "private") return "private(新会话自动同步,私有线程;组织 " + (p.organization ?? "?") + ")";
	return "organization(新会话自动同步并共享给组织 " + (p.organization ?? "?") + ")";
}

// ---- /delta-setup 参数与选择器(#21,复用 #16 的 ui.select 接缝模式) ----

export type SetupArgDecision =
	| { kind: "picker" } // 无参数:交互选择器
	| { kind: "set"; mode: ThreadMode } // 显式模式:直接写文件
	| { kind: "invalid"; arg: string }; // 未知参数:用法提示,绝不猜测写入

/** /delta-setup 显式参数解析:private/priv* → private(归一化与 #16 pickShareVisibility 一致),
 *  organization/org → organization,manual → manual;大小写不敏感。 */
export function parseSetupModeArg(args: string): SetupArgDecision {
	const arg = String(args ?? "").trim().toLowerCase();
	if (!arg) return { kind: "picker" };
	if (arg === "manual") return { kind: "set", mode: "manual" };
	if (arg === "organization" || arg === "org") return { kind: "set", mode: "organization" };
	if (arg.startsWith("priv")) return { kind: "set", mode: "private" };
	return { kind: "invalid", arg };
}

export type SetupModePick = { mode: ThreadMode | null; fallback: string | null };

/** /delta-setup 模式选择器(无参数时):
 *  - ctx.ui.select 三选一:Private sync / Team sharing(带组织名;英文模式名与 delta-claude MODE_NAMES 对齐)/ Manual;
 *  - 非交互(select 缺失/抛错)→ 回落 manual(= 现状,绝不静默开启自动同步)并带回 fallback 文案;
 *  - 用户放弃(Esc)或无法识别的标签 → mode null,调用方按取消处理(不写文件)。 */
export async function pickSetupMode(ui: any, org: { id: string; name: string } | null): Promise<SetupModePick> {
	const orgName = org?.name || "当前组织";
	const options = [
		{ label: "Private sync(私有:新会话自动同步,仅自己可见)", description: "私有线程,不共享" },
		{ label: "Team sharing(共享:新会话自动同步并共享给 " + orgName + ")", description: "组织成员可见" },
		{ label: "Manual(手动:仅 /delta-share 时同步)", description: "保持现状,不自动连接" },
	];
	const select = ui?.select; // ExtensionUIContext.select(title, options): Promise<string | undefined>,返回所选 label
	if (typeof select !== "function") {
		return {
			mode: "manual",
			fallback: "当前环境不支持交互选择,已按现状写入 manual;要启用自动同步请显式运行 /delta-setup private 或 /delta-setup organization",
		};
	}
	let label: unknown;
	try {
		label = await select("选择本仓库的 Delta 同步策略", options);
	} catch (e) {
		return {
			mode: "manual",
			fallback: "策略选择器不可用(" + String(e instanceof Error ? e.message : e) + "),已按现状写入 manual;要启用自动同步请显式运行 /delta-setup private 或 /delta-setup organization",
		};
	}
	if (typeof label !== "string") return { mode: null, fallback: null }; // 用户放弃(Esc)
	if (label === options[0].label) return { mode: "private", fallback: null };
	if (label === options[1].label) return { mode: "organization", fallback: null };
	if (label === options[2].label) return { mode: "manual", fallback: null };
	return { mode: null, fallback: null }; // 无法识别的标签:按取消处理,不猜测写入
}

// ---- 策略文件 IO(checkout 根;与 CLI 进程封装一样是无状态模块级函数) ----

/** git rev-parse --show-toplevel:checkout 根;失败/非 git 目录 → null。 */
function gitCheckoutRoot(cwd: string): string | null {
	try {
		const g = spawnSync("git", ["rev-parse", "--show-toplevel"], { cwd, windowsHide: true });
		if (g.status !== 0) return null;
		const root = String(g.stdout ?? "").trim();
		return root || null;
	} catch {
		return null;
	}
}

/** 读 checkout 根的 .delta/settings.json:缺文件/非 git 目录 = 未配置;读不了(权限等)= manual + problem。 */
function readRepoPolicyFile(root: string): PolicyFile {
	let text: string;
	try {
		text = fs.readFileSync(path.join(root, ".delta", "settings.json"), "utf8");
	} catch (e) {
		if ((e as any)?.code === "ENOENT") return { policy: { mode: "manual", configured: false } };
		return { policy: { mode: "manual", configured: false, problem: "读取失败:" + String(e instanceof Error ? e.message : e) } };
	}
	return readProjectPolicy(text);
}

function writeRepoPolicyFile(root: string, text: string): void {
	const p = path.join(root, ".delta", "settings.json");
	fs.mkdirSync(path.dirname(p), { recursive: true });
	fs.writeFileSync(p, text, "utf8");
}

/** delta-claude 约定(setup.ts excludeFromGit):策略是个人选择,写进 <git-common-dir>/info/exclude
 *  (仓库本地、不被跟踪、不碰被跟踪的 .gitignore);条目 "/.delta/settings.json" 锚定仓库根,
 *  不会波及 Delta 自己的 .delta/ worktree 簿记目录。失败返回 false,调用方提示手动加 .gitignore。 */
const POLICY_EXCLUDE_ENTRY = "/.delta/settings.json";

function excludeFromGit(root: string): boolean {
	try {
		const g = spawnSync("git", ["rev-parse", "--git-common-dir"], { cwd: root, windowsHide: true });
		if (g.status !== 0) return false;
		const common = String(g.stdout ?? "").trim().replace(/[\\/]+$/, "");
		if (!common) return false;
		const excludePath = path.isAbsolute(common) ? path.join(common, "info", "exclude") : path.join(root, common, "info", "exclude");
		const contents = fs.existsSync(excludePath) ? fs.readFileSync(excludePath, "utf8") : "";
		if (contents.split(/\r?\n/).some((l) => l.trim() === POLICY_EXCLUDE_ENTRY)) return true;
		fs.mkdirSync(path.dirname(excludePath), { recursive: true });
		fs.writeFileSync(excludePath, contents + (contents && !contents.endsWith("\n") ? "\n" : "") + POLICY_EXCLUDE_ENTRY + "\n", "utf8");
		return true;
	} catch {
		return false;
	}
}

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

// ---- /delta-update:delta update 包装(#20) ----

/** 实测 `delta --json update --check` 输出(JSONL 两行;up-to-date 时 `delta update`
 *  输出同形,见 docs/contract-notes.md):
 *  {"schema_version":1,"type":"progress","phase":"checking"}
 *  {"schema_version":1,"status":"up_to_date","current_version":"0.23.0",
 *   "latest_version":"0.23.0","channel":"stable","can_install":true,
 *   "message":"Delta 0.23.0 is up to date."}
 *  progress 帧没有 status;取最后一个带 status 的对象。up_to_date 之外的状态一律
 *  视为需要安装/重启 —— `delta update --help`:"A running Delta app restarts to
 *  install it"(仅 up_to_date 形状实采过,其余防御性解析)。 */
export type DeltaUpdateParsed = {
	status: string;
	currentVersion: string;
	latestVersion: string;
	channel: string;
	canInstall: boolean;
	message: string;
	restartRequired: boolean;
};

export function parseUpdateResult(text: string): DeltaUpdateParsed | null {
	let picked: any = null;
	for (const j of parseJsonLines(text)) {
		if (j && typeof j.status === "string" && j.status) picked = j;
	}
	if (!picked) return null;
	const current = String(picked.current_version ?? "");
	const upToDate = picked.status === "up_to_date";
	return {
		status: String(picked.status),
		currentVersion: current,
		latestVersion: String(picked.latest_version ?? "") || current,
		channel: String(picked.channel ?? ""),
		canInstall: picked.can_install === true,
		message: String(picked.message ?? ""),
		restartRequired: !upToDate,
	};
}

/** 插件过期的唯一可靠程序化信号:omp 加载的就是安装目录里的这份构建,其支持上限
 *  是编译期常量 CONTRACT_MAX;`thread contract` 的 min 超过它 ⇒ 本插件对当前 Delta
 *  过旧,必须重装。安装目录 package.json 只有本地版本、没有"最新版"参照(联网查
 *  GitHub 不做:网络依赖且非 marketplace 语义),除此之外没有离线信号 —— 此时
 *  只剩 renderUpdateReport 的常量页脚提示。 */
export function pluginOutdatedFromContract(contract: { min?: unknown } | null | undefined): boolean {
	const min = Number(contract?.min);
	return Number.isFinite(min) && min > CONTRACT_MAX;
}

const UPDATE_RESTART_NOTE =
	"重启说明:Delta 应用会自动重启完成安装;omp 侧建议 /delta-retry 重连(或重启 omp),让 mount/watch 子进程改用新版 CLI。";
const PLUGIN_HINT_FOOTER =
	"提示:omp 没有 marketplace 自动更新,omp-delta 插件过期时不会自动升级;如需更新请重装: " + PLUGIN_REINSTALL_CMD;

export type UpdateReportInput = {
	mode: "check" | "update";
	result: DeltaUpdateParsed | null;
	errorText?: string; // 非零退出/无 JSON 时的原始输出
	contractMin?: number | null; // thread contract 的 min(拿不到为 null:不下结论)
};

/** /delta-update 的汇报文本:版本状态 / 安装结果 / 重启要求 / 插件重装提示。
 *  重装提示是常量页脚,每次 check/update 后都出现 —— 插件过期没有可靠的离线信号,
 *  这条是唯一保证到位的指引(pluginOutdatedFromContract 只在合约 min 超上限时
 *  把提示升级为"必须重装")。 */
export function renderUpdateReport(input: UpdateReportInput): string {
	const lines: string[] = [];
	const check = input.mode === "check";
	if (pluginOutdatedFromContract({ min: input.contractMin })) {
		lines.push(
			"检测到 omp-delta 插件对当前 Delta 过旧(合约 min " + input.contractMin + " > 插件支持 " + CONTRACT_MAX + "),必须重装: " + PLUGIN_REINSTALL_CMD,
		);
	}
	const p = input.result;
	if (!p) {
		const raw = (input.errorText ?? "").trim() || "无输出";
		lines.push((check ? "Delta 更新检查失败" : "Delta 更新失败") + ": " + raw.slice(0, 300));
	} else if (p.status === "up_to_date") {
		lines.push(
			(check ? "Delta 更新检查:" : "Delta 更新:") + "已是最新(" + p.currentVersion + (p.channel ? "," + p.channel : "") + ")" + (check ? "。仅检查,未安装任何内容。" : "."),
		);
	} else if (check) {
		lines.push(
			"Delta 更新检查:发现新版本 " + p.latestVersion + "(当前 " + p.currentVersion + (p.channel ? "," + p.channel : "") + ")。仅检查,未安装;运行 /delta-update 安装。",
		);
		lines.push(UPDATE_RESTART_NOTE);
	} else {
		lines.push("Delta 更新(" + p.status + "):" + (p.message || ("current " + p.currentVersion + " → latest " + p.latestVersion)));
		lines.push(UPDATE_RESTART_NOTE);
	}
	lines.push(PLUGIN_HINT_FOOTER);
	return lines.join("\n");
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

	/** 后台重试连接(供给窗口期实测可达 10+ 分钟,预算 40×30s)。failLabel:/delta-share 用默认重连文案,
	 *  #21 自动连接传自己的失败前缀。 */
	async function retryConnect(
		mode: "private" | "organization",
		org: { id: string; name: string },
		failLabel = "Delta 重连失败",
	): Promise<void> {
		try {
			await connectShared(mode, org);
		} catch (e) {
			const msg = String(e instanceof Error ? e.message : e);
			if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
				rt.connectAttempts += 1;
				if (rt.connectAttempts % 5 === 0)
					notify("Delta 供给窗口等待中(第 " + rt.connectAttempts + " 次重试):" + msg);
				schedule(() => {
					void retryConnect(mode, org, failLabel);
				}, 30_000);
				return;
			}
			rt.expectingIdentity = false; // 确定性失败:不再让记录滞留临时队列(auto-connect 路径需要;share 路径本就为 false)
			notify(failLabel + ":" + msg, "warning");
		}
	}

	/** #21:仓库策略存的组织 id(delta-claude 存 team.id 同构)。名字仅展示用;
	 *  org list 可解析时校验 id 在账号名下,避免把确定性失败(组织不在账号里)送进供给窗口重试循环。 */
	async function resolveStoredOrg(organizationId: string): Promise<{ id: string; name: string }> {
		const r = await runDelta(["org", "list"]);
		const combined = r.out + "\n" + r.err;
		if (isUnauthenticatedText(combined)) throw new Error(ORG_UNAUTH_HINT);
		const orgs = parseOrgListFromText(combined);
		if (orgs === null) return { id: organizationId, name: organizationId }; // 无载荷:交给 connectShared 报具体错误
		const hit = orgs.find((o) => o.id === organizationId);
		if (!hit) throw new Error("仓库策略指向的组织 " + organizationId + " 不在当前账号的组织列表;请运行 /delta-setup 重新选择。");
		return { id: hit.id, name: hit.name || hit.id };
	}

	/** #21:仓库策略自动连接(仅后台调用:marker 重连与 checkout 发现都未命中才走到这里)。
	 *  失败语义与 /delta-share 相同:供给窗口走既有 retryConnect 退避,确定性失败只警告;
	 *  自身绝不抛出,不影响 session_start 的 30s 预算(整段在后台 IIFE 里)。 */
	async function autoConnect(mode: "private" | "organization", organizationId: string): Promise<void> {
		if (rt.connected || rt.threadId) return; // 手动 /delta-share 等路径已抢先:幂等 create 也会撞同一线程,直接让位
		notify("仓库策略 " + (mode === "private" ? "private" : "organization") + ":自动连接 Delta…");
		let org: { id: string; name: string };
		try {
			org = await resolveStoredOrg(organizationId);
		} catch (e) {
			rt.expectingIdentity = false;
			notify("Delta 自动连接失败:" + String(e instanceof Error ? e.message : e), "warning");
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
					void retryConnect(mode, org, "Delta 自动连接失败");
				}, 30_000);
				return; // 供给窗口:expectingIdentity 保持,记录继续入临时队列
			}
			rt.expectingIdentity = false;
			notify("Delta 自动连接失败:" + msg + "(可用 /delta-share 手动连接)", "warning");
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

	// #15:可路由平铺命令的 handler 引用,注册时回填;/delta 路由复用同一函数对象(别名语义由构造保证)
	const deltaHandlers: Record<string, (args: string, ctx: any) => Promise<void>> = {};
	const registerDeltaCommand = (
		name: string,
		def: { description: string; handler: (args: string, ctx: any) => Promise<void> },
	) => {
		deltaHandlers[name] = def.handler;
		pi.registerCommand(name, def);
	};

	registerDeltaCommand("delta-share", {
		description: "把当前 omp 会话连接到 Delta 线程(无参数:交互选择可见性;private 参数:直接私有;非交互环境默认共享给组织)",
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
			// 合约要求：共享前必须征得用户同意，并告知可见范围（组织名+内容）
			let ok = true;
			let org: { id: string; name: string };
			let mode: "private" | "organization" = "organization"; // 兜底默认;显式参数或选择器会覆盖
			try {
				org = await resolveOrg();
				// 可见性(#16):显式参数直达旧行为;无参数先弹选择器(私有 vs 组织),再走同一个确认+连接路径
				const pick = await pickShareVisibility(args, ctx?.ui, org);
				if (pick.mode === null) {
					notify("已取消,未创建 Delta 线程。");
					return;
				}
				if (pick.fallback) notify(pick.fallback, "warning");
				mode = pick.mode;
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

	registerDeltaCommand("delta-status", {
		description: "查看 omp-delta 连接与同步状态",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			// #21:仓库策略一行展示,让"为什么自动连接了/没连"可见
			const policyRoot = gitCheckoutRoot(rt.cwd);
			const pf = policyRoot ? readRepoPolicyFile(policyRoot) : null;
			const lines = [
				"连接: " + (rt.connected ? "已连接" : "未连接") + (rt.paused ? "(推送暂停)" : ""),
				"线程: " + (rt.threadId ?? "无"),
				rt.threadId ? "可见性: " + (rt.visibility ?? "未知") : "",
				rt.threadId ? "链接: " + (rt.url ?? "(尚未获取,重连后自动更新)") : "",
				"记录: 已分配 " + rt.seq + " 条,已确认 " + rt.lastAcked + " 条,待发 " + rt.pending.length + " 条",
				"worktree: " + (rt.worktreeRef ?? "无"),
				"mount/watch: " + (rt.mountChild ? "运行中" : "停止") + " / " + (rt.watchChild ? "运行中" : "停止"),
				"会话: " + (rt.isSub ? "子代理(不记录)" : "主会话") + " · " + rt.sid,
				"策略: " + (pf ? renderProjectPolicy(pf.policy) : "无 Git checkout(不适用)"),
			].filter(Boolean);
			notify(lines.join("\n"));
		},
	});

	registerDeltaCommand("delta-setup", {
		description: "设置本仓库的 Delta 同步策略(.delta/settings.json,兼容 delta-claude):private|organization|manual;无参数交互选择",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			if (rt.isSub) {
				notify("子代理会话暂不支持 /delta-setup。", "warning");
				return;
			}
			const decision = parseSetupModeArg(args);
			if (decision.kind === "invalid") {
				notify(
					"未知模式:" + decision.arg + "\n用法:/delta-setup private|organization|manual(无参数交互选择)。\n" +
						"private/organization = 新会话自动连接(private 仅自己可见);manual = 仅 /delta-share 手动同步(现状)。",
					"warning",
				);
				return;
			}
			const root = gitCheckoutRoot(rt.cwd);
			if (!root) {
				notify("/delta-setup 需要 Git checkout:" + rt.cwd + " 下未找到仓库根。", "warning");
				return;
			}
			// 读现有文件:未知键整体保留;malformed 不静默覆盖(delta-claude 同样先征求同意)
			const policyFile = readRepoPolicyFile(root);
			if (policyFile.malformed) {
				let replace = false;
				if (typeof ctx?.ui?.confirm === "function") {
					try {
						replace = await ctx.ui.confirm(
							"替换无法解析的 .delta/settings.json?",
							(policyFile.policy.problem ?? "文件不是有效 JSON 对象") + "\n替换后原内容丢失;选择取消可先手动修复。",
						);
					} catch {
						replace = false;
					}
				} else {
					notify(".delta/settings.json 无法解析(" + (policyFile.policy.problem ?? "?") + ");未自动覆盖。请手动修复或删除该文件后重试。", "warning");
					return;
				}
				if (!replace) {
					notify("已取消,.delta/settings.json 保持原样。");
					return;
				}
			}
			// 组织:private/organization 必须带(delta-claude 语义);显式 manual 不需要,离线也能钉住现状
			let org: { id: string; name: string } | null = null;
			if (!(decision.kind === "set" && decision.mode === "manual")) {
				try {
					org = await resolveOrg();
				} catch (e) {
					notify(
						"解析当前组织失败:" + String(e instanceof Error ? e.message : e) +
							"\nprivate/organization 策略必须带组织;若只想固定手动模式,可运行 /delta-setup manual。",
						"warning",
					);
					return;
				}
			}
			let mode: ThreadMode;
			if (decision.kind === "set") {
				mode = decision.mode;
			} else {
				const pick = await pickSetupMode(ctx?.ui, org);
				if (pick.mode === null) {
					notify("已取消,.delta/settings.json 保持原样。");
					return;
				}
				if (pick.fallback) notify(pick.fallback, "warning");
				mode = pick.mode;
			}
			const text = serializeProjectPolicy(policyFile.document, mode, mode === "manual" ? undefined : org!.id);
			try {
				writeRepoPolicyFile(root, text);
			} catch (e) {
				notify("写入 .delta/settings.json 失败:" + String(e instanceof Error ? e.message : e), "warning");
				return;
			}
			// delta-claude 约定:策略是个人选择,进 .git/info/exclude 而不是被跟踪的 .gitignore
			if (!excludeFromGit(root)) {
				notify("未能把 .delta/settings.json 排除出 Git;若不想提交它,请手动在 .gitignore 加一行:.delta/settings.json", "warning");
			}
			const label = mode === "private" ? "Private sync(私有)" : mode === "organization" ? "Team sharing(共享给 " + (org?.name ?? "?") + ")" : "Manual(手动)";
			notify(
				"已保存 " + label + " 到 .delta/settings.json。\n" +
					(mode === "manual"
						? "新会话仅在你运行 /delta-share 时同步。"
						: "新会话将自动连接 Delta(" + (mode === "private" ? "私有线程" : "共享给组织 " + org!.name) + ");本会话不受影响,现在连接请运行 /delta-share。"),
			);
		},
	});

	registerDeltaCommand("delta-detach", {
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

	pi.registerCommand("delta-org", {
		description: "查看 Delta 组织(只读):list 列出并标记当前组织;current 显示共享将使用的组织",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			const mode = (String(args ?? "").trim().split(/\s+/)[0] ?? "").toLowerCase() || "list";
			if (mode !== "list" && mode !== "current") {
				notify("用法:/delta-org list(列出组织并标记当前)/ /delta-org current(查看共享将使用的组织)。只读命令:切换组织请用 delta org switch。");
				return;
			}
			try {
				if (mode === "current") {
					const org = await resolveOrg(); // 与 /delta-share 同一来源:展示共享实际会使用的组织
					notify("共享将使用的组织:" + org.name + "(" + org.id + ")");
					return;
				}
				const r = await runDelta(["org", "list"]);
				const combined = r.out + "\n" + r.err;
				const orgs = parseOrgListFromText(combined);
				if (!orgs) {
					notify(
						isUnauthenticatedText(combined) ? ORG_UNAUTH_HINT : "delta org list 失败(code " + r.code + "):" + (r.err || r.out).slice(0, 200),
						"warning",
					);
					return;
				}
				notify(orgs.length > 0 ? "Delta 组织(" + orgs.length + "):\n" + formatOrgList(orgs) : formatOrgList(orgs));
			} catch (e) {
				notify("delta org 失败:" + String(e instanceof Error ? e.message : e), "warning");
			}
		},
	});

	registerDeltaCommand("delta-retry", {
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

	pi.registerCommand("delta-update", {
		description: "检查或安装 Delta 更新(/delta-update check 仅检查不安装);输出含 omp-delta 插件重装提示",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			const mode: "check" | "update" = String(args ?? "").trim().toLowerCase().startsWith("check") ? "check" : "update";
			notify(mode === "check" ? "正在检查 Delta 更新…" : "正在更新 Delta(下载与安装可能需要几分钟)…");
			// runDelta 统一加全局 --json:实测 `delta --json update --check` 与 `delta update --check --json` 输出一致
			const r = await runDelta(mode === "check" ? ["update", "--check"] : ["update"], {
				cwd: rt.cwd,
				timeoutMs: UPDATE_TIMEOUT_MS,
			});
			const ok = r.code === 0;
			const parsed = parseUpdateResult(r.out + (r.err ? "\n" + r.err : ""));
			// 插件过期检测:合约 min 超过本构建支持上限 ⇒ 必须重装;拿不到合约就不下结论,只留常量提示
			let contractMin: number | null = null;
			try {
				const c = await runDeltaJson(["thread", "contract"], { cwd: rt.cwd });
				const min = Number(c?.min);
				if (Number.isFinite(min)) contractMin = min;
			} catch {
				/* 无合约输出:仅保留常量提示 */
			}
			notify(
				renderUpdateReport({
					mode,
					result: ok ? parsed : null,
					errorText: ok && parsed ? undefined : (ok ? "" : "exit " + r.code + ": ") + String(r.err || r.out),
					contractMin,
				}),
				ok && parsed ? "info" : "warning",
			);
		},
	});

	// /delta 路由(#15):转发到平铺命令的同一 handler;无参数输出用法菜单而非报错
	pi.registerCommand("delta", {
		description: "omp-delta 命令路由:/delta share|status|retry|detach(无参数显示用法;平铺命令仍可用)",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			const d = parseDeltaRouter(args);
			if (d.kind === "menu") {
				notify(renderDeltaMenu());
				return;
			}
			if (d.kind === "unimplemented") {
				notify("/delta join 尚未实现(#17):weak join 落地前,请先在 Delta 侧复制 join 链接并等待该能力上线。", "warning");
				return;
			}
			if (d.kind === "unknown") {
				notify("未知子命令:" + d.command + "\n\n" + renderDeltaMenu(), "warning");
				return;
			}
			const handler = deltaHandlers[d.command];
			if (!handler) {
				// 注册顺序保证到这里必然已回填;防御性兑底
				notify("内部错误:子命令 " + d.command + " 未注册。", "warning");
				return;
			}
			await handler(d.args, ctx);
		},
	});

	pi.on("session_start", async (_event: any, ctx: any) => {
		lastCtx = ctx;
		rt.eventCtx = ctx; // 托管定时器来源
		rt.isSub = (ctx?.agent?.kind ?? "main") !== "main";
		if (rt.isSub) return;
		rt.sid = String(ctx?.sessionManager?.getSessionId?.() ?? "sess-" + randomUUID());
		rt.cwd = String(ctx?.cwd ?? process.cwd());
		// #21:仓库策略(.delta/settings.json)决定无绑定时的自动连接;文件问题在此一次性提示
		const policyRoot = gitCheckoutRoot(rt.cwd);
		const policyFile = policyRoot ? readRepoPolicyFile(policyRoot) : null;
		if (policyFile?.policy.problem) {
			notify(".delta/settings.json 无法使用(" + policyFile.policy.problem + ");修复前仅 /delta-share 手动同步生效。", "warning");
		}
		// 绑定标记在 omp 会话记录里(appendEntry 写入,生命周期归 omp 管):
		// 有标记 → 后台凭它重连;无标记 → 先按 checkout 发现,再按仓库策略自动连接(#21)。
		const marker = findSessionMarker(ctx);
		// 决策(纯接缝):marker 永远优先,auto-connect 只兜底真正的全新会话
		const syncAction = decideAutoSyncAction(policyFile?.policy ?? { mode: "manual", configured: false }, marker !== null);
		if (marker) {
			// 不阻塞 session_start(事件处理器有 30s 预算):后台重连;
			// 等待期间事件照常入临时队列(expectingIdentity),游标到达后重编号补发
			rt.expectingIdentity = true;
			notify("发现会话绑定 Delta 线程 " + marker.threadId + ",后台重连中…");
			void reconnectByIdentity(marker);
		} else {
			// 无 marker(moveOff/fork/首次):从服务端按 checkout 发现绑定;
			// 发现不了才是真正的首次:策略为自动模式时自动连接,否则等显式 /delta-share(#21)。
			rt.expectingIdentity = true;
			void (async () => {
				try {
					const found = await discoverBindingByCheckout();
					if (found) {
						notify("发现绑定当前 checkout 的 Delta 线程 " + found.threadId + ",后台重连中…");
						await reconnectByIdentity(found);
					} else if (syncAction.kind === "auto-connect") {
						// 重连路径(marker/checkout 发现)都未命中才自动连接,不会绕过或与重连竞争;
						// 连接窗口期 expectingIdentity 保持,记录入临时队列,游标到达后重编号
						await autoConnect(syncAction.mode, syncAction.organization);
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
