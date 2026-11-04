/**
 * omp-delta — Records the current omp session live into a Delta thread (via the Delta CLI, contract v8).
 *
 * Implements the plugin-side halves of the official plugin contract (delta.dev/docs/cli/build-a-plugin):
 *   1. Translates omp activity into Delta records and pushes them (thread append, ACP v2 update)
 *   2. Records go into an in-memory queue, aligned to the server's next_sequence cursor; acked records leave the queue; re-sending is safe (server returns duplicate)
 *   3. thread watch flows teammate comments back into omp context (follow-ups, never auto-triggering a turn)
 *   4. When the thread is taken over by someone else (superseded / fenced), stops the local turn
 *
 * MVP scope:
 *   - Records the main session only (ctx.agent.kind === "main"); subagents → Delta child threads is a later step
 *   - First connect records from now on; no history backfill
 *   - Commands: /delta (router) plus flat aliases /delta-share /delta-status /delta-detach /delta-retry /delta-org (read-only) /delta-update
 *     /delta-setup (#21): repo-level sync policy in .delta/settings.json (delta-claude compatible); private/organization auto-connect new sessions
 *
 * Architecture note: omp rebinds the extension factory for every session (subagents included), but module variables are shared across sessions.
 * All mutable state therefore lives in the Runtime created inside the factory closure (one per session, no cross-talk);
 * the module layer keeps only the stateless Delta CLI process wrapper and unit-testable pure-function seams.
 * Zero local state (#10): thread identity is authoritative in DeltaDB, the binding is written into the omp session record
 * (appendEntry custom entry), and the delivery cursor is authoritative from the server's next_sequence.
 *
 * Install: omp plugin install 'github:Samuka007/omp-delta' (or omp plugin link ./ for local development), then restart omp;
 * on Windows, if the installer fails silently, npm install 'github:Samuka007/omp-delta' inside ~/.omp/plugins is equivalent (see README).
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import * as readline from "node:readline";

const SOURCE = "omp"; // fixed plugin identifier on the Delta side (--source)
const PRODUCER = "omp"; // producer name shown inside the thread (--producer-name)
const CONTRACT_MIN = 8; // contract version implemented by this plugin
const CONTRACT_MAX = 8;
const SCHEMA_VERSION = 1;
const APPEND_TIMEOUT_MS = 30_000;
const UPDATE_TIMEOUT_MS = 300_000; // delta update download+install is far slower than append: 5-minute budget
const MOUNT_READY_TIMEOUT_MS = 90_000;
const MAX_AUTO_RETRIES = 3;
const MAX_HISTORY_WAIT_ATTEMPTS = 60; // not_visible provisioning window: 30s × 60 ≈ 30 minutes (observed to exceed 10 minutes)
const MARKER_TYPE = "omp-delta.attachment"; // binding marker written into the omp session (appendEntry, never enters LLM context)
// /delta-update: omp has no marketplace auto-update; an outdated plugin can only be reinstalled manually (#20).
// Without a reliable programmatic "plugin outdated" signal, this command is the only guaranteed reinstall hint (see renderUpdateReport).
export const PLUGIN_REINSTALL_CMD = "omp plugin install 'github:Samuka007/omp-delta'";
// Provisioning-window signatures: shared by append error classification and connect retries, kept in sync
const NOT_VISIBLE_RE = /not_visible|history is unavailable/i;
const PROVISIONING_RE = /history is unavailable|not_visible|not in this account|invalid_reference|worktree_storage_failed/i;

type DeltaRecord = {
	seq: number;
	turnSequence?: number;
	update: Record<string, unknown>;
};

/** Runtime state, one instance per omp session (held by the factory closure, never shared; all identity/cursor state is in-memory).
 *  Authority: thread identity and delivery cursor live in DeltaDB; the binding lives in the omp session JSONL (MARKER_TYPE). */
type Runtime = {
	pi: any;
	cwd: string;
	sid: string;
	isSub: boolean;
	connected: boolean;
	// Thread identity (in-memory)
	threadId: string | null;
	worktreeRef: string | null;
	visibility: "private" | "organization" | null;
	url: string | null;
	titleSent: boolean;
	// Delivery cursor (in-memory; emit is gated on seqReady once aligned to next_sequence)
	seq: number;
	lastAcked: number;
	pending: DeltaRecord[];
	seqReady: boolean;
	currentTurn: number | null; // seq of the running record; shared by every record inside the turn
	paused: boolean;
	expectingIdentity: boolean; // reconnect in flight: events keep queueing temporarily, renumbered once the cursor arrives
	retryAttempts: number;
	historyWaitAttempts: number;
	connectAttempts: number;
	eventCtx: any; // event ctx from session_start: holds the managed setTimeout
	detaching: boolean;
	detached: boolean; // explicit user detach (#18): indicator clears its slot; restored only after a successful reconnect (distinct from disconnect offline)
	reusedBinding: boolean; // #26: the last attach short-circuited on "already has a worktree" — the history reconcile was skipped; /delta-status shows it
	mountChild: ChildProcess | null;
	watchChild: ChildProcess | null;
};

// ---- Stateless Delta CLI process wrapper (module level) ----

let deltaBin: string | null = null;
let envDumped = false;
let statusSurfaceWarned = false; // #18: warn about a missing status slot only once (module variables are shared across sessions → once per process)

/** #11 diagnostics: delta reports NotFound when spawned inside omp, while the same command works instantly in an external terminal.
 *  Only when OMP_DELTA_DEBUG=1: on first spawn, dump the resolved binary and key env vars into the omp log to pinpoint the environment difference. */
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
		/* skip if console is unavailable */
	}
}

// Discovery order matches the reference plugin (zed-industries/delta-claude hooks/cli/cli.ts):
// PATH first, then installer App Bundle locations per platform (Linux default ~/.local/delta.app/bin/delta).
// No DELTA_BIN-style env override, staying consistent with the reference plugin.
function resolveDelta(): string {
	if (deltaBin) return deltaBin;
	const home = process.env.HOME ?? "";
	const lc = process.env.LOCALAPPDATA ?? "";
	const candidates = [
		"delta",
		home ? path.join(home, ".local", "delta.app", "bin", "delta") : "", // Linux: install.sh default location
		home ? path.join(home, "Applications", "Delta.app", "Contents", "MacOS", "delta") : "",
		"/Applications/Delta.app/Contents/MacOS/delta", // macOS: CLI bundled in the App Bundle, for when it's not on PATH
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta.exe") : "",
		lc ? path.join(lc, "Programs", "Delta", "bin", "delta") : "",
	].filter(Boolean);
	for (const c of candidates) {
		if (!isDeltaCli(c)) continue;
		deltaBin = c;
		return c;
	}
	throw new Error(
		"delta CLI not found (a delta on PATH that is the same-named git pager is skipped)." +
			"Install Delta (curl -fsSL https://delta.dev/install.sh | sh) and restart omp",
	);
}

// `delta --version` cannot tell the Delta CLI apart from the same-named git pager (both print "delta X.Y.Z").
// The reference plugin trusts command -v directly, but on distros like NixOS the delta on PATH is the git pager,
// so instead check the subcommand list via `--help` (org/thread/worktree etc. are unique to the Delta CLI).
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
			dumpSpawnEnvOnce(); // record on first call: compare delta's environment inside vs. outside omp (#11)
			resolve({ code, out, err });
		};
		const timer = setTimeout(() => {
			try {
				child.kill();
			} catch {
				/* already exited */
			}
		}, opts?.timeoutMs ?? APPEND_TIMEOUT_MS);
		// On Windows, after SIGTERM a grandchild process may still hold the stdio pipes and the close event never fires:
		// fall back to a second hard kill, and parse from the exit event (fires on process death) instead of close.
		const hardKill = setTimeout(() => {
			try {
				child.kill("SIGKILL");
			} catch {
				/* already exited */
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
	// Output starting with [ / { is parsed as a whole first: with pretty-printed JSON (e.g. thread list),
	// lines can happen to be entire string literals, so line-by-line parsing yields a pile of garbage strings
	// that "looks like a successful parse" and never reaches the fallback — the whole-body attempt must come first.
	// Multi-value JSONL streams fail the whole-body parse and naturally fall through to line-by-line.
	if (t.startsWith("[") || t.startsWith("{")) {
		try {
			return [JSON.parse(t)];
		} catch {
			/* multi-value JSONL stream: fall through to line-by-line */
		}
	}
	const result: any[] = [];
	for (const line of t.split(/\r?\n/)) {
		const line2 = line.trim();
		if (!line2) continue;
		try {
			result.push(JSON.parse(line2));
		} catch {
			/* non-JSON line, ignored */
		}
	}
	return result;
}

async function runDeltaJson(args: string[], opts?: { input?: string; timeoutMs?: number; cwd?: string }): Promise<any> {
	const r = await runDelta(args, opts);
	try {
		return runDeltaJsonFromText(r.out, r.err);
	} catch {
		throw new Error("delta " + args.join(" ") + " produced no JSON output: " + (r.err || r.out).slice(0, 400));
	}
}

/** JSON parsing when raw output is already available (JSONL first, whole-body JSON fallback). */
function runDeltaJsonFromText(out: string, err: string): any {
	const js = parseJsonLines(out).concat(parseJsonLines(err));
	if (js.length === 0) {
		throw new Error("no JSON in output: " + (err || out).slice(0, 400));
	}
	return js[js.length - 1];
}

/** Uniform actionable hint for an unauthenticated delta CLI (#19: clean error, no stack).
 *  Signatures observed from delta.exe's embedded error strings: not signed in / authentication required. */
const ORG_UNAUTH_HINT = "Delta CLI is not signed in: run delta auth login (or sign in from the Delta app), then retry.";

/** Uniform actionable hint for a /delta-join account mismatch (#17).
 *  Observed signatures: someone else's private/nonexistent share id → not_visible "shared thread is absent or not visible";
 *  a thread id not owned by this account / a wrong link → invalid_reference "…exact thread ID in this account".
 *  A thread still in its provisioning window right after creation shows the same signature (not_visible), so the hint mentions that. */
const JOIN_MISMATCH_HINT =
	"Cannot join this thread: it is not visible to the current account — usually because the thread belongs to another account (private, not shared with you), the link or ID is wrong, or the thread was just created and is still provisioning (retry shortly).";

/** Currently selected org (must be given explicitly when creating a thread). */
async function resolveOrg(): Promise<{ id: string; name: string }> {
	const r = await runDelta(["org", "current"]);
	const combined = r.out + "\n" + r.err;
	if (isUnauthenticatedText(combined)) throw new Error(ORG_UNAUTH_HINT);
	const org = parseOrgCurrentFromText(combined);
	if (!org) {
		throw new Error("Could not parse the current org (code " + r.code + "): pick one with delta org switch and retry.");
	}
	return org;
}

// ---- /delta-share visibility decision (pure seam: ui is injected by the caller, tests can pass a fake ctx.ui.select) ----

/** Result of pickShareVisibility: mode null = user dismissed the picker (cancel connect); fallback non-empty = non-interactive default with a message for the caller to notify. */
export type VisibilityPick = {
	mode: "private" | "organization" | null;
	fallback: string | null;
};

/** /delta-share visibility decision (#16):
 *  - explicit argument (priv* prefix = private, any other non-empty argument = organization) → taken as-is, never touches the picker (backward compatible);
 *  - no argument → ctx.ui.select offers Private / Organization (with the current org name) before the consent confirm;
 *  - non-interactive (select missing/throws) → fall back to organization with a fallback message, never throws;
 *  - user dismisses the picker (Esc/undefined) → mode null, caller treats it as a cancel. */
export async function pickShareVisibility(
	args: string | null | undefined,
	ui: any,
	org: { id: string; name: string },
): Promise<VisibilityPick> {
	const arg = String(args ?? "").trim().toLowerCase();
	if (arg) {
		// Old behavior: priv prefix → private, any other explicit argument → organization
		return { mode: arg.startsWith("priv") ? "private" : "organization", fallback: null };
	}
	const options = [
		{ label: "Private (private thread, visible only to you)", description: "Not shared with the org" },
		{ label: "Organization (shared with " + org.name + ")", description: "Visible to org members" },
	];
	const select = ui?.select; // ExtensionUIContext.select(title, options): Promise<string | undefined>, resolves with the chosen label
	if (typeof select !== "function") {
		return { mode: "organization", fallback: "Interactive selection is unavailable in this environment; using the default visibility: shared with org " + org.name };
	}
	let label: unknown;
	try {
		label = await select("Choose Delta thread visibility", options);
	} catch (e) {
		return {
			mode: "organization",
			fallback: "Visibility picker unavailable (" + String(e instanceof Error ? e.message : e) + "); using the default: shared with org " + org.name,
		};
	}
	if (typeof label !== "string") {
		return { mode: null, fallback: null }; // user dismissed (Esc)
	}
	if (label === options[0].label) return { mode: "private", fallback: null };
	// The second option / an unrecognized label both count as organization (same as the old default; confirm shows the org name again for final consent)
	return { mode: "organization", fallback: null };
}

// ---- Pure-function seams (directly unit-testable; contract semantics in docs/contract-notes.md) ----

// ---- /delta router (#15): single entry point; flat commands remain unchanged as aliases ----

/** /delta routing decision: menu = no arguments (usage menu, not an error); route = implemented subcommand (forwards to the same handler);
 *  unknown = unknown subcommand. */
export type DeltaRouteDecision =
	| { kind: "menu" }
	| { kind: "route"; command: "delta-share" | "delta-status" | "delta-detach" | "delta-retry" | "delta-join"; args: string }
	| { kind: "unknown"; command: string };

/** Parses `/delta <subcommand> [rest]` (#15): case-insensitive, whitespace collapsed.
 *  Subcommands map one-to-one onto the flat commands: share→delta-share, status→delta-status, detach→delta-detach,
 *  retry→delta-retry, join→delta-join (#17).
 *  /delta-org and /delta-update are not routed (they have their own spelling; the menu points at them). */
export function parseDeltaRouter(args: string): DeltaRouteDecision {
	const parts = String(args ?? "").trim().split(/\s+/).filter(Boolean);
	if (parts.length === 0) return { kind: "menu" };
	const sub = parts[0].toLowerCase();
	const rest = parts.slice(1).join(" ");
	const route: Record<string, "delta-share" | "delta-status" | "delta-detach" | "delta-retry" | "delta-join"> = {
		share: "delta-share",
		status: "delta-status",
		detach: "delta-detach",
		retry: "delta-retry",
		join: "delta-join",
	};
	const command = route[sub];
	if (!command) return { kind: "unknown", command: sub };
	return { kind: "route", command, args: rest };
}

/** Usage menu for /delta with no arguments (not an error). */
export function renderDeltaMenu(): string {
	return [
		"delta — omp-delta command router:",
		"  /delta share [private]   connect the current session (= /delta-share)",
		"  /delta join <link|id>    join an existing thread; --move materializes the checkout and guides /move (= /delta-join)",
		"  /delta status            show connection and sync status (= /delta-status)",
		"  /delta retry             resume/retry pushing (= /delta-retry)",
		"  /delta detach            disconnect sync (= /delta-detach)",
		"  Other commands: /delta-setup [private|organization|manual] · /delta-org list|current · /delta-update [check]",
		"  Flat aliases keep working: /delta-share /delta-join /delta-status /delta-detach /delta-retry",
	].join("\n");
}

// ---- /delta-join (#17,#22): attach the current session to an existing thread; weak by default (attach the current checkout);
// ---- --move/--into materializes the thread's checkout into a new directory and guides a manual /move (automation pending upstream oh-my-pi#17441).

export type JoinTarget =
	| { kind: "link"; shareId: string; raw: string }
	| { kind: "share-id"; shareId: string; raw: string }
	| { kind: "thread-id"; raw: string }
	| { kind: "invalid"; raw: string }
	| { kind: "empty" };

// Observed shapes (delta 0.23.0): join link https://delta.dev/join/<share-id> or delta://join/<share-id>;
// share ids look like thread_01m6jt5nyc4znwdfcg9p2mk49d (no derivation to/from the thread id, see contract-notes);
// thread ids are opaque long tokens (letters/digits/-/_). One trailing / on a link is allowed; query/hash is always invalid (paste noise should be cleaned up by the user).
const JOIN_LINK_RE = /^(?:https:\/\/delta\.dev\/join\/|delta:\/\/join\/)([A-Za-z0-9][A-Za-z0-9_-]*)\/?$/i;
const JOIN_SHARE_ID_RE = /^thread_[A-Za-z0-9_-]+$/;
const JOIN_PLAUSIBLE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

/** Parses /delta-join arguments (#17): a delta.dev join link (https or delta://), a thread_ share id,
 *  or a bare thread id. Link forms have their shareId extracted for display; the raw text of all three resolvable
 *  forms is passed to `delta thread status <raw>` — the CLI resolves them uniformly (all three reach the same thread, as observed).
 *  empty = no argument given; invalid = input that cannot be an id (whitespace/path separators/query etc.),
 *  rejected locally without bothering the CLI. */
export function parseJoinTarget(args: string | null | undefined): JoinTarget {
	const raw = String(args ?? "").trim();
	if (!raw) return { kind: "empty" };
	const link = JOIN_LINK_RE.exec(raw);
	if (link) return { kind: "link", shareId: link[1], raw };
	if (JOIN_SHARE_ID_RE.test(raw)) return { kind: "share-id", shareId: raw, raw };
	if (JOIN_PLAUSIBLE_ID_RE.test(raw)) return { kind: "thread-id", raw };
	return { kind: "invalid", raw };
}

export type JoinFailureKind = "unauthenticated" | "account-mismatch" | "unknown";

/** The three parseJoinTarget forms that can go straight to the CLI (link/share-id/thread-id);
 *  empty/invalid are intercepted locally and never reach joinShared. */
export type ResolvableJoinTarget = Extract<JoinTarget, { kind: "link" | "share-id" | "thread-id" }>;

/** /delta-join invocation arguments (#22): target + materialization options. --into implies --move;
 *  into === "" means --into was given without a value (usage error); unknown flags stay inside target and
 *  are judged invalid by parseJoinTarget (a leading "-" is not a legal id). */
export type JoinInvocation = { target: JoinTarget; move: boolean; into: string | null };

export function parseJoinInvocation(args: string | null | undefined): JoinInvocation {
	const tokens = String(args ?? "").trim().split(/\s+/).filter(Boolean);
	const rest: string[] = [];
	let move = false;
	let into: string | null = null;
	for (let i = 0; i < tokens.length; i++) {
		const tok = tokens[i];
		if (tok === "--move") {
			move = true;
		} else if (tok === "--into") {
			const value = tokens[i + 1];
			into = value !== undefined ? value : "";
			if (value !== undefined) i += 1;
		} else {
			rest.push(tok);
		}
	}
	return { target: parseJoinTarget(rest.join(" ")), move, into };
}

export type MaterializePick =
	| { kind: "local"; cwdMatch: boolean; dirs: string[]; ref: string }
	| { kind: "ref"; ref: string }
	| { kind: "ambiguous"; refs: string[] }
	| { kind: "empty" };

/** Whether the thread's checkout exists on this machine (#22): if any checkout directory of any worktree entry
 *  exists locally → local (cwdMatch = the current directory is one of them); if no entry exists locally →
 *  exactly one foreign entry yields a materializable ref, several are ambiguous (unknown which to materialize),
 *  a thread with no worktree entries → empty (nothing to materialize). local still carries ref: an explicit --move
 *  may materialize an already-local ref into a fresh directory. exists is injected for unit tests (default fs.existsSync). */
export function pickMaterializeCandidate(
	items: any[],
	cwd: string,
	exists: (p: string) => boolean = (p) => fs.existsSync(p),
): MaterializePick {
	const rows: any[] = Array.isArray(items) ? items : [];
	if (rows.length === 0) return { kind: "empty" };
	const cwdPick = pickWorktreeForCwd(rows, cwd);
	const localDirs: string[] = [];
	const foreignRefs: string[] = [];
	for (const row of rows) {
		const dirs = worktreeDirs(row);
		const present = dirs.filter((d) => exists(d));
		if (present.length > 0) {
			localDirs.push(...present);
		} else {
			const ref = String(row?.reference ?? "");
			if (ref) foreignRefs.push(ref);
		}
	}
	if (localDirs.length > 0) {
		return { kind: "local", cwdMatch: cwdPick.ref !== "", dirs: localDirs, ref: String(rows[0]?.reference ?? "") };
	}
	if (foreignRefs.length === 1) return { kind: "ref", ref: foreignRefs[0] };
	if (foreignRefs.length > 1) return { kind: "ambiguous", refs: foreignRefs };
	return { kind: "empty" };
}

/** Default derivation of the materialization target directory (#22): a sibling of the current directory, prefixed with a
 *  sanitized (thread title or share id) and "delta-"; used only when the caller gave no --into. Pure path math, no fs checks. */
export function deriveMaterializeDir(cwd: string, seed: string): string {
	const clean = String(seed ?? "")
		.replace(/[^A-Za-z0-9._-]+/g, "-")
		.replace(/^[-.]+/, "")
		.slice(0, 48)
		.replace(/[-]+$/, "");
	const name = "delta-" + (clean || "join");
	return path.join(path.dirname(String(cwd ?? "")), name);
}

/** join failure classification (#17): the clean account-mismatch error comes first (acceptance criterion).
 *  classifyJoinFailure runs only when thread status exits non-zero; anything still matching
 *  invalid_reference/not_visible here is an "existence/visibility" rejection — syntactically
 *  garbage input has already been rejected locally by parseJoinTarget's invalid. */
export function classifyJoinFailure(text: string): JoinFailureKind {
	if (isUnauthenticatedText(text)) return "unauthenticated";
	if (/not_visible|shared thread is absent|not in this account|invalid_reference/i.test(text)) return "account-mismatch";
	return "unknown";
}

/** Unauthenticated detection: signature matching against delta's raw stdout+stderr (JSON vs. plain text agnostic). */
export function isUnauthenticatedText(text: string): boolean {
	return /not signed in|not logged in|unauthenticated|authentication required|please (?:log|sign) in|unauthorized/i.test(text);
}

export type OrgInfo = { id: string; name: string; current: boolean };

/** Parses `delta org list --json` output: {"operation":"listed","organizations":[{id,name,current}]}
 *  (order equals the CLI's pretty display order; exactly one current:true). parseJsonLines is the single entry,
 *  accepting both pretty-printed whole-body JSON and line-by-line JSONL; a top-level bare array is accepted too.
 *  Returns null = no org payload at all (failure/unauthenticated); [] = signed in but joined no orgs. */
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

/** Parses `delta org current --json` output: {"operation":"current","organization":{id,name,current}}. */
export function parseOrgCurrentFromText(text: string): { id: string; name: string } | null {
	for (const js of parseJsonLines(text)) {
		const id = String(js?.organization?.id ?? "");
		if (id) return { id, name: String(js?.organization?.name ?? id) };
	}
	return null;
}

/** /delta-org list rendering: follows the CLI's own pretty convention (a leading `*` marks the current org). */
export function formatOrgList(orgs: OrgInfo[]): string {
	if (orgs.length === 0) return "Not a member of any org.";
	return orgs
		.map((o) => (o.current ? "* " : "  ") + o.name + " (" + o.id + ")" + (o.current ? " ← current: sharing will use this org" : ""))
		.join("\n");
}

// ---- /delta-setup repo sync policy (#21): .delta/settings.json, format-compatible with zed-industries/delta-claude ----
// Format authority: delta-claude hooks/setup/project-policy.ts (details recorded in docs/contract-notes.md "Repo policy").
// Shape: {"threads":{"mode":"manual|private|organization","organization":"<org id>"}};
// private/organization require a non-empty organization (delta-claude stores team.id; we store the id from delta org list);
// manual may carry organization (delta-claude semantics: the /delta share target, not consumed by omp yet); other keys are kept as-is.
// Note: the repo's .delta/ directory is Delta's own worktree bookkeeping, unrelated to this policy file; the exclude rule recognizes only the settings.json path.

/** The repo's sync mode (missing file/unconfigured = manual, current behavior: explicit /delta-share only). */
export type ThreadMode = "manual" | "private" | "organization";

/** Validated effective policy. Non-empty problem = the file exists but is unusable (manual fallback; the notice shows once). */
export type ProjectPolicy = {
	mode: ThreadMode;
	organization?: string;
	configured: boolean; // the file deliberately configured a policy; missing file/threads/mode all count as unconfigured
	problem?: string; // why the file is unusable (one-time notice at session_start; always shown by /delta-status)
};

/** Policy file read result: policy = effective policy; document = raw document (unknown keys survive rewrites); malformed = parse failed. */
export type PolicyFile = {
	policy: ProjectPolicy;
	document?: Record<string, unknown>;
	malformed?: boolean;
};

const POLICY_MODES: readonly ThreadMode[] = ["manual", "private", "organization"];

function isJsonObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** Parses and validates policy text (pass undefined when the file is missing). Semantics aligned with delta-claude readProjectPolicy:
 *  missing file/threads/mode → unconfigured manual (no problem); malformed JSON/non-object/illegal mode/
 *  auto mode missing organization → manual fallback + problem. */
export function readProjectPolicy(text: string | undefined): PolicyFile {
	if (text === undefined) return { policy: { mode: "manual", configured: false } };
	let document: unknown;
	try {
		document = JSON.parse(text);
	} catch (e) {
		return { malformed: true, policy: { mode: "manual", configured: false, problem: "Not valid JSON (" + String(e instanceof Error ? e.message : e) + ")" } };
	}
	if (!isJsonObject(document)) {
		return { malformed: true, policy: { mode: "manual", configured: false, problem: "Content is not a JSON object" } };
	}
	const threads = document.threads;
	if (threads === undefined) return { document, policy: { mode: "manual", configured: false } };
	if (!isJsonObject(threads)) {
		return { document, policy: { mode: "manual", configured: false, problem: "threads value is not an object" } };
	}
	const mode = threads.mode;
	if (mode === undefined) return { document, policy: { mode: "manual", configured: false } };
	if (typeof mode !== "string" || !POLICY_MODES.includes(mode as ThreadMode)) {
		return { document, policy: { mode: "manual", configured: false, problem: "mode must be one of " + POLICY_MODES.join(" / ") } };
	}
	const organization = threads.organization;
	if (mode === "manual") {
		// manual may carry organization (delta-claude semantics: the /delta share target)
		const policy: ProjectPolicy = { mode: "manual", configured: true };
		if (typeof organization === "string" && organization.trim()) policy.organization = organization;
		return { document, policy };
	}
	if (typeof organization !== "string" || !organization.trim()) {
		return { document, policy: { mode: "manual", configured: false, problem: 'mode "' + mode + '" requires a non-empty organization' } };
	}
	return { document, policy: { mode: mode as ThreadMode, organization, configured: true } };
}

/** Serializes the policy: keeps every unknown key from the previous document (top level and inside threads), 2-space indent + trailing newline (same as delta-claude).
 *  organization === undefined deletes threads.organization (manual writes no org). */
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

/** Whether the policy records automatically (no explicit /delta-share needed): private / organization. */
export function recordsAutomatically(policy: ProjectPolicy): boolean {
	return policy.mode === "private" || policy.mode === "organization";
}

/** session_start auto-sync decision (#21, pure seam, unit-tested):
 *  - an existing binding marker always wins → reconnect (auto-connect does not compete, never bypasses the marker/reconnect);
 *  - no marker and the policy is an auto mode (with an org) → auto-connect (the caller additionally requires checkout discovery to have missed);
 *  - everything else (manual/unconfigured/auto mode without an org) → manual, waiting for an explicit /delta-share. */
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

/** /delta-status policy line (#21): makes "why did/didn't it auto-connect" visible. */
export function renderProjectPolicy(p: ProjectPolicy): string {
	if (p.problem) return "manual (policy file unusable: " + p.problem + ")";
	if (!p.configured) return "manual (unconfigured: /delta-share manual sync only; /delta-setup can enable auto)";
	if (p.mode === "manual") return "manual (/delta-share manual sync only)";
	if (p.mode === "private") return "private (new sessions sync automatically as private threads; org " + (p.organization ?? "?") + ")";
	return "organization (new sessions sync automatically and share with org " + (p.organization ?? "?") + ")";
}

// ---- /delta-setup arguments and picker (#21, reusing #16's ui.select seam pattern) ----

export type SetupArgDecision =
	| { kind: "picker" } // no argument: interactive picker
	| { kind: "set"; mode: ThreadMode } // explicit mode: write the file directly
	| { kind: "invalid"; arg: string }; // unknown argument: usage notice, never guesses a write

/** /delta-setup explicit argument parsing: private/priv* → private (normalized the same as #16 pickShareVisibility),
 *  organization/org → organization, manual → manual; case-insensitive. */
export function parseSetupModeArg(args: string): SetupArgDecision {
	const arg = String(args ?? "").trim().toLowerCase();
	if (!arg) return { kind: "picker" };
	if (arg === "manual") return { kind: "set", mode: "manual" };
	if (arg === "organization" || arg === "org") return { kind: "set", mode: "organization" };
	if (arg.startsWith("priv")) return { kind: "set", mode: "private" };
	return { kind: "invalid", arg };
}

export type SetupModePick = { mode: ThreadMode | null; fallback: string | null };

/** /delta-setup mode picker (when run with no arguments):
 *  - ctx.ui.select offers three choices: Private sync / Team sharing (with the org name; English mode names aligned with delta-claude MODE_NAMES) / Manual;
 *  - non-interactive (select missing/throws) → fall back to manual (= current behavior, never silently enables auto-sync) with a fallback message;
 *  - user dismisses (Esc) or an unrecognized label → mode null, caller treats it as a cancel (no file write). */
export async function pickSetupMode(ui: any, org: { id: string; name: string } | null): Promise<SetupModePick> {
	const orgName = org?.name || "current org";
	const options = [
		{ label: "Private sync (private: new sessions sync automatically, visible only to you)", description: "Private thread, not shared" },
		{ label: "Team sharing (shared: new sessions sync automatically and share with " + orgName + ")", description: "Visible to org members" },
		{ label: "Manual (manual: sync only on /delta-share)", description: "Keep current behavior, no auto-connect" },
	];
	const select = ui?.select; // ExtensionUIContext.select(title, options): Promise<string | undefined>, resolves with the chosen label
	if (typeof select !== "function") {
		return {
			mode: "manual",
			fallback: "Interactive selection is unavailable in this environment; manual was kept as-is. To enable auto-sync, run /delta-setup private or /delta-setup organization explicitly",
		};
	}
	let label: unknown;
	try {
		label = await select("Choose the Delta sync policy for this repo", options);
	} catch (e) {
		return {
			mode: "manual",
			fallback: "Policy picker unavailable (" + String(e instanceof Error ? e.message : e) + "); manual was kept as-is. To enable auto-sync, run /delta-setup private or /delta-setup organization explicitly",
		};
	}
	if (typeof label !== "string") return { mode: null, fallback: null }; // user dismissed (Esc)
	if (label === options[0].label) return { mode: "private", fallback: null };
	if (label === options[1].label) return { mode: "organization", fallback: null };
	if (label === options[2].label) return { mode: "manual", fallback: null };
	return { mode: null, fallback: null }; // unrecognized label: treat as cancel, never guess a write
}

// ---- Policy file IO (checkout root; stateless module-level functions like the CLI process wrapper) ----

/** git rev-parse --show-toplevel: the checkout root; failure/non-git directory → null. */
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

/** Reads the checkout root's .delta/settings.json: missing file/non-git dir = unconfigured; unreadable (permissions etc.) = manual + problem. */
function readRepoPolicyFile(root: string): PolicyFile {
	let text: string;
	try {
		text = fs.readFileSync(path.join(root, ".delta", "settings.json"), "utf8");
	} catch (e) {
		if ((e as any)?.code === "ENOENT") return { policy: { mode: "manual", configured: false } };
		return { policy: { mode: "manual", configured: false, problem: "Read failed: " + String(e instanceof Error ? e.message : e) } };
	}
	return readProjectPolicy(text);
}

function writeRepoPolicyFile(root: string, text: string): void {
	const p = path.join(root, ".delta", "settings.json");
	fs.mkdirSync(path.dirname(p), { recursive: true });
	fs.writeFileSync(p, text, "utf8");
}

/** delta-claude convention (setup.ts excludeFromGit): the policy is a personal choice, so it goes into <git-common-dir>/info/exclude
 *  (repo-local, untracked, leaves the tracked .gitignore untouched); the entry "/.delta/settings.json" is anchored to the repo root,
 *  so it does not affect Delta's own .delta/ worktree bookkeeping directory. Returns false on failure; the caller suggests adding it to .gitignore manually. */
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

/** Observed receipt shape:
 *  {"operation":"appended","sequence":N,"next_sequence":N+1,"duplicate":false,
 *   "disposition":"recorded","turn_sequence":3,"version":"…"}
 *  The verdict lives in disposition (recorded/duplicate/fenced); the legacy status field is accepted for compatibility. */
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

/** Finds the largest contiguous acked prefix starting from lastAcked (never jumps over unacked records on a hole).
 *  pending holds only unacked records and bySeq is built from it: receipts beyond pending cannot extend the prefix. */
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

/** Classifies the append's exit code/output/receipts: takeover, retryable, provisioning window, backoff duration, error message. */
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
				// A new thread's history provisioning window (observed to last tens of minutes): temporarily invisible but self-healing,
				// so use a uniform 30s long backoff and don't trust the server's short value
				const reason = String(j.error.reason ?? "");
				if (reason === "not_visible" || NOT_VISIBLE_RE.test(errMsg)) {
					notVisible = true;
					retryable = true;
					retryAfter = 30_000;
				}
			}
		}
		// Raw-text fallback when JSON parsing fails: some CLI errors carry no JSON
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
		// spawn failure or a killed timeout: no JSON error body, treat as a retryable failure
		retryable = true;
		if (!errMsg) errMsg = "delta process unresponsive or terminated";
	}
	return { fenced, retryable, notVisible, retryAfter, errMsg };
}

// ---- Attach failure classification (#23): branch on the JSON error envelope (error.code / error.retryable /
// error.retry_after_ms) instead of regex-matching human-readable stderr. Raw-text signatures remain only as a
// fallback for non-JSON CLI output. #25/#26/#27/#28 all stand on this classification. ----

export type AttachFailureKind = "provisioning" | "retryable" | "reused-binding" | "permanent";

export type AttachFailureClass = {
	kind: AttachFailureKind;
	code: string; // envelope error.code / error.reason when present, else ""
	retryAfterMs: number; // 0 for permanent failures
	message: string; // human-readable error text (envelope message first, trimmed CLI output as fallback)
};

// Provisioning-window signatures for attach: a freshly created thread's history/worktree provisioning takes
// seconds to minutes; the same identities are retryable with backoff (kept in sync with PROVISIONING_RE).
const ATTACH_PROVISIONING_RE = /history is unavailable|not_visible|not in this account|invalid_reference|worktree_storage_failed/i;
const REUSED_BINDING_RE = /already has a worktree/i;

/** Classifies one failed `worktree attach` result. Envelope first (error.code / retryable / retry_after_ms),
 *  raw-text fallback when the CLI printed no JSON envelope:
 *  - provisioning: thread/worktree history still provisioning — retryable with the fixed short backoff;
 *  - retryable: the envelope says retryable — back off using retry_after_ms;
 *  - reused-binding: the thread already carries this checkout — not a failure, attach short-circuits (#26);
 *  - permanent: everything else (disconnected / worktree_remote_failed with retryable:false, unknown codes) —
 *    deterministic, retries cannot fix it, surface immediately (#25). */
export function classifyAttachFailure(code: number, err: string, out: string): AttachFailureClass {
	const rawText = String(err ?? "") + "\n" + String(out ?? "");
	const raw = rawText.trim();
	const cls = (kind: AttachFailureKind, message: string, retryAfterMs = 0, ecode = ""): AttachFailureClass => ({
		kind,
		code: ecode,
		retryAfterMs,
		message: message.slice(0, 300),
	});
	// Reused binding is recognizable from either the envelope code or the raw text, and wins over everything else:
	// it is not a failure at all (the mount relation persists).
	if (REUSED_BINDING_RE.test(rawText)) return cls("reused-binding", "thread already has this worktree");
	for (const j of parseJsonLines(err).concat(parseJsonLines(out))) {
		const e = j?.error;
		if (!e || typeof e !== "object") continue;
		const ecode = String(e.code ?? e.reason ?? "");
		const msg = String(e.message ?? "") || raw;
		if (ATTACH_PROVISIONING_RE.test(ecode) || ATTACH_PROVISIONING_RE.test(String(e.message ?? ""))) {
			return cls("provisioning", msg, 3_000, ecode);
		}
		if (e.retryable === true) return cls("retryable", msg, Number(e.retry_after_ms) || 5_000, ecode);
		return cls("permanent", msg, 0, ecode);
	}
	// Non-JSON fallback (human-readable CLI output only)
	if (code !== 0 && ATTACH_PROVISIONING_RE.test(rawText)) return cls("provisioning", raw, 3_000);
	if (code === -1) return cls("retryable", "delta process unresponsive or terminated", 5_000);
	return cls("permanent", raw || "exit " + code);
}

/** #25: deterministic attach failures translate to actionable guidance instead of raw stderr;
 *  retryable classes keep their plain message (the caller keeps backing off automatically). */
export function renderAttachFailureGuidance(c: AttachFailureClass): string {
	const head = "worktree attach failed" + (c.code ? " (" + c.code + ")" : "") + ": " + c.message;
	if (c.kind !== "permanent") return head;
	return (
		head +
		"\nThis failure looks deterministic — retrying will not fix it. Next steps:" +
		"\n  1. Check sign-in state: delta auth status (a signed-out or broken-credentials app fails every worktree operation)." +
		"\n  2. Inspect ~/.local/share/delta/delta.log for \"[share-sync] … failed\" entries around the last attempt." +
		"\n  3. If the log shows the same instant failure on every attempt, report upstream with that log excerpt."
	);
}

/** Error carrying the attach classification (#25): /delta-retry reads `permanent` to tell "needs a human"
 *  apart from "transient, will keep retrying". */
export class AttachError extends Error {
	cls: AttachFailureClass;
	constructor(cls: AttachFailureClass) {
		super(renderAttachFailureGuidance(cls));
		this.name = "AttachError";
		this.cls = cls;
	}
	get permanent(): boolean {
		return this.cls.kind === "permanent";
	}
}

/** #24: the connect notice prints only the server-provided url — never a link fabricated from the create thread id
 *  (the share/join id is unrelated by derivation to thread_id; a fabricated link opens an empty thread page). */
export function renderConnectNotice(threadId: string, url: string | null): string {
	return (
		"Delta thread connected: " +
		threadId +
		"\nOpen in browser: " +
		(url ?? "(link not ready yet; run /delta-status to fetch it)") +
		"\n(conversation and workspace files, including uncommitted changes, are syncing to Delta)"
	);
}

// ---- Auth/terms preflight (#28): parity with delta-claude's requireAuthentication. Gate every
// thread/worktree mutation on `delta auth status` so a signed-out/broken-credentials app produces one
// actionable hint instead of confusing downstream failures (observed: an unauthenticated attach half-created
// a thread membership that took a day to untangle). ----

export type AuthPreflightState = "signed_in" | "signed_out" | "terms" | "unknown";

export const DELTA_TERMS_URL = "https://zed.dev/delta/terms";

/** Parses `delta auth status` output (JSON envelope first via text matching, human-readable fallback).
 *  Order matters: terms rejection is checked before the generic signed-out signatures. */
export function parseAuthStatus(text: string): AuthPreflightState {
	if (/terms_not_accepted|access_denied[^\n]*terms/i.test(text)) return "terms";
	if (/signed_out|not signed in|not logged in|logged out|not authenticated|authentication required|could not be restored/i.test(text))
		return "signed_out";
	if (/signed_in|signed in as|logged in as|authenticated as/i.test(text)) return "signed_in";
	return "unknown";
}

/** append request body: one envelope per line, schemaVersion + sequence (+ turnSequence). */
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

/** Provisioning-window error: not yet visible server-side; safe to retry in the background. */
export function isTransientProvisioning(msg: string): boolean {
	return PROVISIONING_RE.test(msg);
}

/** Following delta-claude: live activity carries a _meta.delta source marker, otherwise an external producer's agent output is not rendered into the transcript. */
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

// ---- Sync status indicator (#18): pure status→label seam, unit-tested; interactive painting lives in the factory closure ----
// Surface selection and lifecycle research in docs/status-indicator-research.md (the ctx.ui.setStatus slot).

export type SyncIndicatorState = "synced" | "supplying" | "offline" | "detached";

/** Inputs to deriveSyncIndicatorState: a snapshot of the Runtime fields involved in the state decision. */
export type SyncIndicatorInput = {
	connected: boolean;
	paused: boolean;
	detached: boolean; // explicit user detach (#18): the indicator clears its slot until reconnect
	expectingIdentity: boolean; // reconnect in flight: records queue temporarily awaiting the cursor
	threadId: string | null;
	pendingCount: number; // unacked records (rt.pending.length)
	retryAttempts: number; // push auto-retry counter (>0 = retry window)
	historyWaitAttempts: number; // provisioning window wait counter (>0 = waiting for visibility)
	connectAttempts: number; // background reconnect counter (>0 = reconnect window)
};

/** Runtime fields → indicator state: detached (explicit detach, or never bound: clears the slot) / offline (bound but disconnected with no reconnect in flight, or paused) /
 *  supplying (connected with work in flight, or disconnected while reconnect/provisioning is in flight: records still queue) / synced (idle and connected).
 *  Priority: detached > paused > reconnect in flight (supplying: records keep queueing during the retry window) > disconnect offline > supplying > synced. */
export function deriveSyncIndicatorState(input: SyncIndicatorInput): SyncIndicatorState {
	if (input.detached || (!input.threadId && !input.connected)) return "detached";
	if (input.paused) return "offline";
	if (input.expectingIdentity || input.connectAttempts > 0) return "supplying";
	if (!input.connected) return "offline";
	if (input.pendingCount > 0 || input.retryAttempts > 0 || input.historyWaitAttempts > 0) {
		return "supplying";
	}
	return "synced";
}

/** Short thread id: the thread_ prefix is constant, so strip it before taking the first 8 characters (otherwise only the uninformative thread_0 remains). */
export function shortThreadId(threadId: string): string {
	const bare = threadId.startsWith("thread_") ? threadId.slice("thread_".length) : threadId;
	return bare.slice(0, 8);
}

const SYNC_STATE_LABELS: Record<Exclude<SyncIndicatorState, "detached">, string> = {
	synced: "synced",
	supplying: "supplying",
	offline: "offline",
};

/** State → indicator label `Δ <short id> · <state>`; detached → null (the caller clears the slot accordingly).
 *  A defensive path with a null threadId renders an empty short id (actually unreachable when connected: connecting always has a threadId first). */
export function renderStatusIndicator(state: SyncIndicatorState, threadId: string | null): string | null {
	if (state === "detached") return null;
	return "Δ " + shortThreadId(String(threadId ?? "")) + " · " + SYNC_STATE_LABELS[state];
}

/** Path-normalized comparison (Windows/POSIX separators and casing). */
function normPath(p: string): string {
	return p.replace(/[\\/]+/g, "/").replace(/\/$/, "").toLowerCase();
}

// ---- Identity and cursor: the server is authoritative; nothing persists locally ----

export type DeltaIdentity = {
	threadId: string;
	url: string;
	visibility: "private" | "organization" | undefined;
	externalId: string;
	nextSequence: number;
	worktrees: any[];
	title?: string;
};

/** Parses a resumable thread identity from `thread status`.
 *  Note: the join url is a share id (thread_01…), unrelated by derivation to thread_id; only the field itself can be used. */
export function resolveIdentityFromStatus(status: any): DeltaIdentity | null {
	const threadId = String(status?.thread_id ?? "");
	if (!threadId) return null;
	const title = String(status?.title ?? "").trim();
	return {
		threadId,
		url: String(status?.url ?? ""),
		visibility: status?.visibility === "organization" ? "organization" : status?.visibility === "private" ? "private" : undefined,
		externalId: String(status?.producer?.external_id ?? ""),
		nextSequence: Number(status?.producer?.next_sequence ?? 0) || 0,
		worktrees: Array.isArray(status?.worktrees?.items) ? status.worktrees.items : [],
		...(title ? { title } : {}),
	};
}

export type WorktreePick = { ref: string; ambiguous: boolean };

/** All checkout directories reported by one worktree entry: top-level path/directory + nested checkouts[].directory
 *  (observed shape: delta materializes checkouts under <cwd>/.delta/worktrees/<uuid>/…, state may be inactive). */
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

/** Picks the current checkout's identity from a worktree list; multiple matches must be reported as ambiguous, never silently chosen.
 *  Match rule: directory equality, or a delta-managed .delta/ subtree under the current cwd (a delta-materialized checkout). */
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

/** #26: /delta-status distinguishes "reconciled" from "reused existing binding" (the attach short-circuit
 *  skipped the history reconcile — reused bindings are fine but must be visible). */
export function renderBindingState(worktreeRef: string | null, reusedBinding: boolean): string {
	if (!worktreeRef) return "none";
	return reusedBinding ? "reused existing binding (reconcile skipped on the last connect)" : "reconciled";
}

export type WorktreeResolution = {
	ref: string;
	source: "list" | "marker" | "create" | "none";
	ambiguous: boolean;
};

/** #27: worktree picking with a pure seam — the list match wins; only then the session marker's stored
 *  reference (cwd-verified: a marker from another directory must not mount this checkout); `create` is the
 *  last resort. One checkout must not accumulate orphan identities on transient list misses. */
export function resolveWorktreeChoice(
	listRows: any[],
	cwd: string,
	markerRef?: string,
	markerCwd?: string,
): WorktreeResolution {
	const pick = pickWorktreeForCwd(listRows, cwd);
	if (pick.ambiguous) return { ref: "", source: "none", ambiguous: true };
	if (pick.ref) return { ref: pick.ref, source: "list", ambiguous: false };
	if (markerRef && (!markerCwd || normPath(markerCwd) === normPath(cwd))) {
		return { ref: String(markerRef), source: "marker", ambiguous: false };
	}
	return { ref: "", source: "create", ambiguous: false };
}

export type SequenceState = { pending: DeltaRecord[]; seq: number; lastAcked: number };

/** Aligns the local queue with the server cursor (next_sequence = number of delivered records):
 *  drops records the server already has and continues sequence numbers from the server cursor — after a restart/machine switch, zero local state still reconnects seamlessly. */
export function reconcileSequenceState(pending: DeltaRecord[], serverNext: number): SequenceState {
	const cursor = Math.max(0, serverNext - 1); // the largest seq the server has acked
	const kept = pending.filter((r) => r.seq > cursor);
	let seq = cursor;
	for (const r of kept) if (r.seq > seq) seq = r.seq;
	return { pending: kept, seq, lastAcked: cursor };
}

export type RenumberResult = { pending: DeltaRecord[]; seq: number; lastAcked: number; seqMap: Map<number, number> };

/** For the first alignment: records in the temporary queue never reached the server, so they can be safely renumbered onto the server cursor;
 *  internal turnSequence references (pointing at the running record in the same queue) are fixed through the same map. Rewrites pending in place. */
export function renumberFrom(pending: DeltaRecord[], serverNext: number): RenumberResult {
	const seqMap = new Map<number, number>();
	let s = Math.max(1, serverNext); // a brand-new producer starts at 1; an existing cursor continues from next_sequence
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

// ---- /delta-update: delta update wrapper (#20) ----

/** Observed `delta --json update --check` output (two JSONL lines; `delta update` prints the same shape when up to date,
 *  see docs/contract-notes.md):
 *  {"schema_version":1,"type":"progress","phase":"checking"}
 *  {"schema_version":1,"status":"up_to_date","current_version":"0.23.0",
 *   "latest_version":"0.23.0","channel":"stable","can_install":true,
 *   "message":"Delta 0.23.0 is up to date."}
 *  progress frames have no status; take the last object that has one. Any status other than up_to_date is
 *  treated as install/restart required — `delta update --help`: "A running Delta app restarts to
 *  install it" (only the up_to_date shape was observed live; the rest is parsed defensively). */
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

/** The only reliable programmatic outdated-plugin signal: omp loads exactly this build from the install directory, whose support
 *  ceiling is the compile-time constant CONTRACT_MAX; if `thread contract`'s min exceeds it, this plugin is outdated for the
 *  current Delta and must be reinstalled. The install directory's package.json only carries a local version, with no "latest"
 *  reference (no networked GitHub lookup: that adds a network dependency and isn't marketplace semantics); beyond this there is
 *  no offline signal — all that remains is renderUpdateReport's constant footer hint. */
export function pluginOutdatedFromContract(contract: { min?: unknown } | null | undefined): boolean {
	const min = Number(contract?.min);
	return Number.isFinite(min) && min > CONTRACT_MAX;
}

const UPDATE_RESTART_NOTE =
	"Restart note: the Delta app restarts automatically to finish installing; on the omp side, run /delta-retry to reconnect (or restart omp) so the mount/watch subprocesses pick up the new CLI.";
const PLUGIN_HINT_FOOTER =
	"Hint: omp has no marketplace auto-update, so an outdated omp-delta plugin never upgrades itself; to update, reinstall: " + PLUGIN_REINSTALL_CMD;

export type UpdateReportInput = {
	mode: "check" | "update";
	result: DeltaUpdateParsed | null;
	errorText?: string; // raw output when the exit code was non-zero / no JSON came back
	contractMin?: number | null; // min from thread contract (null when unavailable: no verdict)
};

/** Report text for /delta-update: version status / install result / restart requirement / plugin reinstall hint.
 *  The reinstall hint is a constant footer that appears after every check/update — there is no reliable offline signal
 *  for a plugin being outdated, so this is the only guidance guaranteed to land (pluginOutdatedFromContract only
 *  escalates the hint to "must reinstall" when the contract min exceeds the ceiling). */
export function renderUpdateReport(input: UpdateReportInput): string {
	const lines: string[] = [];
	const check = input.mode === "check";
	if (pluginOutdatedFromContract({ min: input.contractMin })) {
		lines.push(
			"Detected that the omp-delta plugin is outdated for the current Delta (contract min " + input.contractMin + " > plugin supports " + CONTRACT_MAX + "); you must reinstall: " + PLUGIN_REINSTALL_CMD,
		);
	}
	const p = input.result;
	if (!p) {
		const raw = (input.errorText ?? "").trim() || "no output";
		lines.push((check ? "Delta update check failed" : "Delta update failed") + ": " + raw.slice(0, 300));
	} else if (p.status === "up_to_date") {
		lines.push(
			(check ? "Delta update check: " : "Delta update: ") + "already up to date (" + p.currentVersion + (p.channel ? ", " + p.channel : "") + ")" + (check ? ". Check only, nothing was installed." : "."),
		);
	} else if (check) {
		lines.push(
			"Delta update check: new version " + p.latestVersion + " found (current " + p.currentVersion + (p.channel ? ", " + p.channel : "") + "). Check only, nothing was installed; run /delta-update to install.",
		);
		lines.push(UPDATE_RESTART_NOTE);
	} else {
		lines.push("Delta update (" + p.status + "): " + (p.message || ("current " + p.currentVersion + " → latest " + p.latestVersion)));
		lines.push(UPDATE_RESTART_NOTE);
	}
	lines.push(PLUGIN_HINT_FOOTER);
	return lines.join("\n");
}

// ---- Extension factory: one Runtime per session ----

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
		detached: false,
		reusedBinding: false,
		mountChild: null,
		watchChild: null,
	};
	// Most recent command/event ctx, for fallback in callbacks without one (e.g. subprocess onClose)
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

	// ---- Sync status indicator (#18): ctx.ui.setStatus key slot (footer status band; research in docs/status-indicator-research.md) ----
	// The key slot persists inside the status-line component (changes invalidate the render cache; identical labels get deduped upstream),
	// coexists with pi's own link-hint (omp:link-hints); does not compete with the Orca title-bar spinner for title.
	const STATUS_KEY = "omp-delta:sync";
	let paintedStatus: string | null | undefined = undefined; // undefined = never painted

	/** Repaints the indicator after a state transition. Every paint goes through the same guard as the spinner: ctx.ui may
	 *  throw after a session-replacing dialog invalidated the runner — swallow it and leave paintedStatus untouched;
	 *  the next transition naturally retries. If the surface is missing, skip painting and warn only once (module-level dedup). */
	function paintStatus(ctx?: any): void {
		const ui = (ctx ?? lastCtx ?? rt.eventCtx)?.ui;
		const setStatus = ui?.setStatus;
		if (typeof setStatus !== "function") {
			if (!statusSurfaceWarned) {
				statusSurfaceWarned = true;
				notify("This omp environment has no status-bar slot (ctx.ui.setStatus); the sync status indicator is disabled.", "warning");
			}
			return;
		}
		const text = renderStatusIndicator(
			deriveSyncIndicatorState({
				connected: rt.connected,
				paused: rt.paused,
				detached: rt.detached,
				expectingIdentity: rt.expectingIdentity,
				threadId: rt.threadId,
				pendingCount: rt.pending.length,
				retryAttempts: rt.retryAttempts,
				historyWaitAttempts: rt.historyWaitAttempts,
				connectAttempts: rt.connectAttempts,
			}),
			rt.threadId,
		);
		if (text === paintedStatus) return; // no change, don't cross the extension boundary
		try {
			setStatus.call(ui, STATUS_KEY, text ?? undefined);
			paintedStatus = text; // spinner discipline: record the state only after a successful paint; a failed paint is retried by the next transition
		} catch {
			/* runner already invalidated: swallow */
		}
	}

	/** Clears the slot directly (used by session_shutdown: one synchronous call within the 2s budget, no state derivation). */
	function clearStatus(): void {
		const ui = (lastCtx ?? rt.eventCtx)?.ui;
		const setStatus = ui?.setStatus;
		if (typeof setStatus !== "function") return;
		try {
			setStatus.call(ui, STATUS_KEY, undefined);
			paintedStatus = null;
		} catch {
			/* runner already invalidated */
		}
	}

	function shouldRecord(): boolean {
		// Also record while a reconnect is in flight (expectingIdentity): into the temporary queue, safely renumbered once the cursor arrives;
		// dropped only when there is no binding prospect at all.
		return !rt.isSub && !rt.paused && (rt.seqReady || rt.expectingIdentity);
	}

	function nextSeq(): number {
		rt.seq += 1;
		return rt.seq;
	}

	/** Assigns a sequence, enqueues, and attempts a push. Returns null when there is no binding prospect or recording is paused. */
	function emit(update: Record<string, unknown>, opts?: { turnSequence?: number; sequence?: number }): number | null {
		// Records go into the in-memory queue only: the authoritative content is the omp session JSONL, the authoritative delivery cursor is the server.
		// Records emitted before seqReady use temporary numbering, renumbered by adoptServerCursor when the cursor arrives.
		if (!shouldRecord()) return null;
		const seq = opts?.sequence ?? nextSeq();
		const rec: DeltaRecord = { seq, update: liveActivity(update) };
		if (opts?.turnSequence !== undefined) rec.turnSequence = opts.turnSequence;
		rt.pending.push(rec);
		paintStatus(); // enqueued: synced → supplying (idle windows are already covered by expectingIdentity)
		void flush();
		return seq;
	}

	let flushing = false;

	/** Timed scheduling: prefer the event ctx's managed timers (exception-isolated), else fall back to native setTimeout. */
	function schedule(fn: () => void, ms: number) {
		const managed = (rt.eventCtx ?? lastCtx)?.setTimeout?.(fn, ms);
		if (!managed) {
			const g = globalThis as any;
			(g.setTimeout ?? setTimeout)(fn, ms);
		}
	}

	/** Pushes unacked records to Delta; retryable errors back off automatically, and push pauses when retries run out. */
	async function flush(): Promise<void> {
		if (!rt.threadId || !rt.seqReady || flushing || rt.paused) return;
		flushing = true;
		try {
			const pending = rt.pending.filter((r) => r.seq > rt.lastAcked);
			if (pending.length === 0) {
				if (rt.retryAttempts !== 0) {
					rt.retryAttempts = 0;
					paintStatus(); // retry window over
				}
				return;
			}

			// Acked records have left the queue; re-sending unacked records is safe (Delta returns duplicate)
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
				// This turn has been taken over by someone else: the contract requires stopping the local turn (already-executed tool effects cannot be undone)
				notify("Delta: this turn was taken over by someone else; the local turn has been stopped.", "warning");
				const ack = contiguousAck(pending, rt.lastAcked, receipts);
				rt.lastAcked = Math.max(rt.lastAcked, ack);
				rt.pending = rt.pending.filter((rec) => rec.seq > rt.lastAcked);
				rt.currentTurn = null;
				try {
					lastCtx?.abort?.();
				} catch {
					/* no turn in progress */
				}
				paintStatus();
				return;
			}

			const newAck = contiguousAck(pending, rt.lastAcked, receipts);
			if (newAck > rt.lastAcked) {
				rt.lastAcked = newAck;
				// Acked records leave the queue (in-memory queue, no persistence needed)
				rt.pending = rt.pending.filter((rec) => rec.seq > rt.lastAcked);
				rt.retryAttempts = 0;
				rt.historyWaitAttempts = 0;
			}

			if (r.code !== 0 && rt.lastAcked < rt.seq) {
				if (cls.retryable && cls.notVisible && rt.historyWaitAttempts < MAX_HISTORY_WAIT_ATTEMPTS) {
					rt.historyWaitAttempts += 1;
					notify(
						"Delta thread provisioning, waiting for visibility ("
						+ rt.historyWaitAttempts + "/" + MAX_HISTORY_WAIT_ATTEMPTS + "): " + cls.errMsg,
					);
					paintStatus();
					schedule(() => {
						void flush();
					}, cls.retryAfter);
				} else if (cls.retryable && rt.retryAttempts < MAX_AUTO_RETRIES) {
					rt.retryAttempts += 1;
					notify(
						"Delta push failed, auto-retry " + rt.retryAttempts + "/" + MAX_AUTO_RETRIES + " (" + cls.errMsg + ")",
						"warning",
					);
					paintStatus();
					schedule(() => {
						void flush();
					}, cls.retryAfter);
				} else {
					rt.paused = true;
					notify("Delta push failed, auto-retry paused: " + cls.errMsg + " (use /delta-retry to resume)", "warning");
					paintStatus();
				}
			}
			if (r.code === 0 && rt.lastAcked < rt.seq) {
				// Partial ack: schedule a follow-up push; pause after consecutive no-progress attempts (the counter resets on progress)
				if (rt.retryAttempts < MAX_AUTO_RETRIES) {
					rt.retryAttempts += 1;
					lastCtx?.setTimeout?.(() => {
						void flush();
					}, 1_000);
				} else {
					rt.paused = true;
					notify("Delta still has unsynced records and auto-retry has hit its limit (use /delta-retry to resume)", "warning");
				notify("Delta still has unsynced records and auto-retry has hit its limit (use /delta-retry to resume)", "warning");
			}
		}
		paintStatus(); // covers all branches above: ack progress (queue drain) / retry window / provisioning wait / pause
		} catch (e) {
			log("flush exception: " + String(e));
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
				log("failed to parse subprocess output: " + String(e));
			}
		});
		child.stderr?.on("data", () => {
			/* diagnostic output not consumed in MVP */
		});
		// exit rather than close: on Windows, when a grandchild holds the pipes, close never fires (a watch disconnect would be misreported as still connected)
		child.on("exit", (code) => {
			try {
				rl.close();
			} catch {
				/* already closed */
			}
			onClose(code ?? -1);
		});
		child.on("error", (e) => {
			log("subprocess failed to start: " + String(e));
			onClose(-1);
		});
		return child;
	}

	function killChild(child: ChildProcess | null) {
		try {
			child?.kill("SIGTERM");
		} catch {
			/* already exited */
		}
	}

	function handleWatchEvent(j: any) {
		const type = String(j?.type ?? "");
		if (type === "comment") {
			const c = j?.comment ?? j;
			const text = String(c?.text ?? "").trim();
			if (!text) return;
			const who = String(c?.author ?? c?.creator ?? "teammate").slice(0, 40);
			// Contract requirement: remote activity enters context but never auto-triggers a turn → followUp (context-only while idle)
			pi?.sendMessage?.("[Delta comment · " + who + "] " + text, {
				deliverAs: "followUp",
				attribution: "agent",
			});
			notify("Received a Delta comment; it entered omp context as a follow-up.");
			return;
		}
		if (type === "producer.turn" && JSON.stringify(j).includes("supersede")) {
			notify("Delta: your turn has been taken over; stopping the local turn.", "warning");
			rt.currentTurn = null;
			try {
				lastCtx?.abort?.();
			} catch {
				/* no turn in progress */
			}
		}
		// item / turn / presence / worktrees / children etc. are not consumed in the MVP
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
					notify("Delta watch disconnected (exit " + code + "); comment flow-back is paused. /delta-retry can reconnect.", "warning");
				}
			},
		);
	}

	/** Existing thread: attach the worktree + start watch + push the backlog. */
	/** Reads the latest binding marker from omp session entries (written by appendEntry; null when absent). */
	function findSessionMarker(ctx: any): { threadId: string; worktreeRef?: string; cwd?: string } | null {
		try {
			const entries: any[] = ctx?.sessionManager?.getEntries?.() ?? [];
			let marker: { threadId: string; worktreeRef?: string } | null = null;
			for (const e of entries) {
				if (e?.type === "custom" && e?.customType === MARKER_TYPE && e?.data?.threadId) {
					marker = e.data; // take the last one = the latest binding
				}
			}
			return marker;
		} catch {
			return null; // without session read access, only a manual /delta-share works
		}
	}

	/** Landing the alignment result into the runtime: the server cursor is authoritative; seqReady gates flush/emit. */
	function applySequenceState(rc: { pending: DeltaRecord[]; seq: number; lastAcked: number }) {
		rt.pending = rc.pending;
		rt.seq = rc.seq;
		rt.lastAcked = rc.lastAcked;
		rt.seqReady = true;
	}

	/** Single entry when the cursor arrives: queues that already went to the server use reconcile (drop delivered, keep undelivered),
	 *  the temporary queue (records queued while a reconnect was in flight) uses renumberFrom (never reached the server, safe to renumber;
	 *  internal turnSequence references and currentTurn are mapped together). */
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
		rt.detached = false; // re-attaching: clear the explicit detach marker so the indicator shows again
		const ref = String(rt.worktreeRef ?? "");
		if (!ref) throw new Error("Missing worktree reference; run /delta-share again.");
		const a0 = await runDelta(["worktree", "attach", ref, "--thread", String(rt.threadId)], { cwd: rt.cwd });
		let a = a0;
		// (#23) Envelope-driven retry: provisioning backsoff on the fixed short backoff, retryable uses retry_after_ms;
		// permanent surfaces immediately (no blind regex loop), reused-binding short-circuits but stays visible (#26).
		let reused = false;
		const waitBackoff = (ms: number) =>
			new Promise<void>((r) => {
				const t = (rt.eventCtx ?? lastCtx)?.setTimeout?.(r, ms);
				if (!t) setTimeout(r, ms);
			});
		for (let i = 0; a.code !== 0 && i < 20; i++) {
			const cls = classifyAttachFailure(a.code, a.err, a.out);
			if (cls.kind === "reused-binding") {
				reused = true;
				break;
			}
			if (cls.kind === "permanent") break;
			await waitBackoff(cls.kind === "retryable" ? Math.min(cls.retryAfterMs, 30_000) : 3_000);
			a = await runDelta(["worktree", "attach", ref, "--thread", String(rt.threadId)], { cwd: rt.cwd });
		}
		if (a.code !== 0) {
			const cls = classifyAttachFailure(a.code, a.err, a.out);
			if (cls.kind === "reused-binding") {
				reused = true;
			} else {
				throw new AttachError(cls); // (#25) permanent failures carry actionable guidance; retryables are exhausted here
			}
		}
		rt.reusedBinding = reused;
		if (reused) {
			notify(
				"Attach short-circuited: the thread already carries this checkout, so the history reconcile was skipped. " +
					"If the thread looks broken on the Delta side, /delta-detach then /delta-share to force a fresh reconcile.",
				"warning",
			);
		}

		// mount uploads the checkout and keeps syncing; continue once the ready line appears, the process keeps running.
		// On reconnect/thread switch, kill the old mount first (as startWatch does for watch) to prevent orphan sync processes double-writing
		killChild(rt.mountChild);
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
						notify("Delta worktree sync process exited (exit " + code + ").", "warning");
					}
				},
			);
			// Don't wait forever for a slow mount start; the process stays in the background and uploads by itself once ready
			schedule(() => settle(), MOUNT_READY_TIMEOUT_MS);
		});

		rt.connected = true;
		rt.connectAttempts = 0;
		startWatch();
		void flush();
		paintStatus(); // connect: confirms/clears offline at the supplying/synced starting point
	}

	/** #28: auth/terms preflight (parity with delta-claude's requireAuthentication) — runs before any
	 *  thread/worktree mutation; signed_in and unknown proceed without extra prompts. */
	async function preflightAuth(): Promise<void> {
		const r = await runDelta(["auth", "status"], { cwd: rt.cwd });
		const state = parseAuthStatus(r.out + "\n" + r.err);
		if (state === "terms") {
			throw new Error("Delta terms of service are not accepted for this account. Open " + DELTA_TERMS_URL + " to accept them, then retry.");
		}
		if (state === "signed_out") {
			throw new Error(ORG_UNAUTH_HINT);
		}
	}

	/** Contract handshake (shared by share and join): the CLI's supported range must intersect what this plugin implements. */
	async function ensureContractCompatible(): Promise<void> {
		const contract = await runDeltaJson(["thread", "contract"], { cwd: rt.cwd });
		const min = Number(contract?.min ?? 0);
		const max = Number(contract?.max ?? 0);
		if (max < CONTRACT_MIN) {
			throw new Error("delta CLI is too old (supports contract " + min + "-" + max + ", need ≥ " + CONTRACT_MIN + "); please update Delta.");
		}
		if (min > CONTRACT_MAX) {
			throw new Error("delta CLI contract is newer (min " + min + "); please update the omp-delta plugin.");
		}
	}

	/** git checkout preflight (shared by share and join): worktree mounting needs a git checkout + remote; erroring early reads better. */
	function assertGitCheckout(): void {
		const g = spawnSync("git", ["rev-parse", "--show-toplevel"], { cwd: rt.cwd, windowsHide: true });
		if (g.status !== 0) {
			throw new Error("The current directory is not a git repository; worktree cannot mount: " + rt.cwd);
		}
		const remotes = spawnSync("git", ["remote"], { cwd: rt.cwd, windowsHide: true, encoding: "utf8" });
		if (!String(remotes.stdout ?? "").trim()) {
			throw new Error("The repository has no Git remote; Delta worktree creation would fail: run git remote add origin … first");
		}
	}

	/** Worktree identity of the current checkout (shared by share and join, following delta-claude's checkoutReference semantics):
	 *  list prefers reusing an existing identity for the same checkout over blindly creating; multiple identities are reported as ambiguous for a human choice.
	 *  Join case: when this machine has never joined as a worktree, create a new identity and attach it to the target thread —
	 *  the thread then carries worktrees from multiple participants side by side (Delta's multi-worktree model). */
	async function resolveLocalWorktreeRef(): Promise<string> {
		const list = await runDeltaJson(["worktree", "list"], { cwd: rt.cwd });
		const rows: any[] = Array.isArray(list) ? list : (list?.worktrees ?? list?.items ?? []);
		// (#27) Prefer the session marker's stored reference (cwd-verified) over creating: a transient list miss
		// used to mint a brand-new identity every time (4 orphans in one day on one checkout).
		const marker = findSessionMarker(lastCtx);
		const r = resolveWorktreeChoice(rows, rt.cwd, marker?.worktreeRef, marker?.cwd);
		if (r.ambiguous) {
			throw new Error("Multiple worktree identities exist for this checkout; pick the right one in Delta first, then retry.");
		}
		let ref = r.ref;
		if (r.source === "create") {
			log("worktree: neither list nor marker produced a reference; creating a new identity for " + rt.cwd);
			const w = await runDeltaJson(["worktree", "create", rt.cwd], { cwd: rt.cwd });
			ref = String(w?.reference ?? "");
			if (ref) notify("Created a new Delta worktree identity " + ref + " for this checkout (no existing identity matched).");
		}
		if (!ref) throw new Error("Neither worktree create nor list produced a reference.");
		return ref;
	}

	/** Writes the binding marker into the omp session record (official appendEntry: persisted but outside LLM context);
	 *  after a restart it drives the reconnect; after a fork, a mismatching external_id refuses to silently continue recording;
	 *  cwd is used at reconnect to detect cross-directory recovery, preventing an old thread from mounting the wrong checkout.
	 *  join and share write the same kind of marker: reconnectByIdentity stays generic, no second persistence scheme invented (#17).
	 *  cwdOverride (#22): the materialization path writes the materialized directory (not the current session directory) so a restart recovers in the right directory. */
	function persistBinding(cwdOverride?: string) {
		try {
			pi?.appendEntry?.(MARKER_TYPE, {
				threadId: rt.threadId,
				worktreeRef: rt.worktreeRef,
				cwd: cwdOverride ?? rt.cwd,
			});
		} catch {
			/* marker failure doesn't affect this connection; next time needs a manual /delta-share or /delta-join */
		}
	}

	/** First connect: contract negotiation → create thread (idempotent) → worktree → start recording. The org is resolved by the caller beforehand. */
	async function connectShared(mode: "private" | "organization", org: { id: string; name: string }): Promise<void> {
		await ensureContractCompatible();
		await preflightAuth(); // (#28) before any thread/worktree mutation: signed-out/broken credentials once half-created a membership
		assertGitCheckout();

		// The org was resolved before the consent dialog in the command handler (resolveOrg); use it directly here.
		// Same source + external-id is idempotent: safe for retries/crash recovery
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
		// Identity and cursor: thread create is idempotent (same source+external-id = same thread).
		// During the provisioning window, status may be not_visible: treat it as a brand-new thread (seq from 0),
		// letting the first record reach the server and open the history container — once the container opens,
		// a later connect's reconcile corrects against the server cursor. Don't let a status failure block the first record,
		// otherwise an empty container waits forever for its first record (observed deadlock >90 minutes).
		rt.threadId = String(created?.thread_id ?? "");
		rt.visibility = mode;
		if (!rt.threadId) {
			throw new Error("thread create returned no thread_id: " + JSON.stringify(created).slice(0, 300));
		}
		let id: DeltaIdentity | null = null;
		try {
			const st = await runDeltaJson(
				["thread", "status", rt.threadId, "--source", SOURCE, "--external-id", rt.sid],
				{ cwd: rt.cwd },
			);
			id = resolveIdentityFromStatus(st);
		} catch {
			/* not visible during the provisioning window: continue as an empty thread */
		}
		if (id) rt.url = id.url ?? null;
		adoptServerCursor(id?.nextSequence ?? 0);

		// Push the first record immediately: an empty thread never reaches the server, and attach/status would invalid_reference
		if (!rt.titleSent) {
			rt.titleSent = true;
			let title = "";
			try {
				title = String((await pi?.getSessionName?.()) ?? "");
			} catch {
				/* no session name */
			}
			emit({ sessionUpdate: "session_info_update", title: (title || "omp session").slice(0, 120) });
		}

		// worktree identity: reuse the existing identity for this checkout or create one (see resolveLocalWorktreeRef)
		rt.worktreeRef = await resolveLocalWorktreeRef();

		await attachAndStream();

		persistBinding();

		// (#24) One extra status re-query when the provisioning window swallowed the url: the notice must never
		// fabricate a join link from the create thread id — print the pending message instead.
		if (!rt.url) {
			try {
				const st = await runDeltaJson(
					["thread", "status", rt.threadId!, "--source", SOURCE, "--external-id", rt.sid],
					{ cwd: rt.cwd },
				);
				rt.url = resolveIdentityFromStatus(st)?.url || rt.url;
			} catch {
				/* still provisioning: the notice says the link is pending */
			}
		}
		notify(renderConnectNotice(rt.threadId!, rt.url));
	}

	/** Background connect retry (the provisioning window has been observed at 10+ minutes; budget 40×30s). failLabel: /delta-share uses the default reconnect wording;
	 *  #21 auto-connect passes its own failure prefix. */
	async function retryConnect(
		mode: "private" | "organization",
		org: { id: string; name: string },
		failLabel = "Delta reconnect failed",
	): Promise<void> {
	try {
		await connectShared(mode, org);
	} catch (e) {
		const msg = String(e instanceof Error ? e.message : e);
		if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
			rt.connectAttempts += 1;
			if (rt.connectAttempts % 5 === 0)
				notify("Delta provisioning window, waiting (retry " + rt.connectAttempts + "): " + msg);
				paintStatus();
				schedule(() => {
					void retryConnect(mode, org, failLabel);
				}, 30_000);
				return;
			}
			rt.expectingIdentity = false; // decisive failure: stop stranding records in the temporary queue (needed on the auto-connect path; share path is already false)
			notify(failLabel + ": " + msg, "warning");
			paintStatus(); // reconnect chain ends: decisive failure → offline/detached
		}
	}

	/** #21: the org id stored by the repo policy (delta-claude stores team.id, isomorphic). The name is display-only;
	 *  when org list is parseable, validate that the id belongs to this account, avoiding sending a decisive failure (org not in the account) into the provisioning-window retry loop. */
	async function resolveStoredOrg(organizationId: string): Promise<{ id: string; name: string }> {
		const r = await runDelta(["org", "list"]);
		const combined = r.out + "\n" + r.err;
		if (isUnauthenticatedText(combined)) throw new Error(ORG_UNAUTH_HINT);
		const orgs = parseOrgListFromText(combined);
		if (orgs === null) return { id: organizationId, name: organizationId }; // no payload: let connectShared report the specific error
		const hit = orgs.find((o) => o.id === organizationId);
		if (!hit) throw new Error("The org " + organizationId + " pointed to by the repo policy is not in this account's org list; run /delta-setup to choose again.");
		return { id: hit.id, name: hit.name || hit.id };
	}

	/** #21: repo-policy auto-connect (called from the background only: reached when both marker reconnect and checkout discovery missed).
	 *  Failure semantics identical to /delta-share: provisioning window goes through the existing retryConnect backoff, decisive failures only warn;
	 *  it never throws itself, and does not touch session_start's 30s budget (the whole thing runs in a background IIFE). */
	async function autoConnect(mode: "private" | "organization", organizationId: string): Promise<void> {
		if (rt.connected || rt.threadId) return; // a manual /delta-share path already won: the idempotent create would hit the same thread; defer to it
		notify("Repo policy " + (mode === "private" ? "private" : "organization") + ": auto-connecting Delta…");
		let org: { id: string; name: string };
		try {
			org = await resolveStoredOrg(organizationId);
		} catch (e) {
			rt.expectingIdentity = false;
			notify("Delta auto-connect failed: " + String(e instanceof Error ? e.message : e), "warning");
			return;
		}
		try {
			await connectShared(mode, org);
		} catch (e) {
			const msg = String(e instanceof Error ? e.message : e);
			if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
				rt.connectAttempts += 1;
				if (rt.connectAttempts === 1 || rt.connectAttempts % 5 === 0)
					notify("Delta provisioning window, waiting (retry " + rt.connectAttempts + "): " + msg);
				schedule(() => {
					void retryConnect(mode, org, "Delta auto-connect failed");
				}, 30_000);
				return; // provisioning window: keep expectingIdentity, records keep queueing in the temporary queue
			}
			rt.expectingIdentity = false;
			notify("Delta auto-connect failed: " + msg + " (/delta-share can connect manually)", "warning");
		}
	}

	/** Front door for join identity resolution (#17): the CLI's `thread status` uniformly accepts join links
	 *  (https/delta://), thread_ share ids, and thread ids (all three reach the same thread, as observed),
	 *  returning thread_id, the worktree list, and this producer's cursor (cursors are per-producer isolated;
	 *  adoptServerCursor makes this session follow from "now", not replay others' history).
	 *  Uses runDelta to keep the raw text: failure classification needs raw-text matching (isUnauthenticatedText / not_visible signatures);
	 *  runDeltaJson folds the raw text into an error message, which would break classification. */
	async function resolveJoinedIdentity(target: ResolvableJoinTarget): Promise<DeltaIdentity> {
		const r = await runDelta(
			["thread", "status", target.raw, "--source", SOURCE, "--external-id", rt.sid],
			{ cwd: rt.cwd },
		);
		if (r.code !== 0) {
			const blob = r.err + "\n" + r.out;
			const kind = classifyJoinFailure(blob);
			if (kind === "unauthenticated") throw new Error(ORG_UNAUTH_HINT);
			if (kind === "account-mismatch") throw new Error(JOIN_MISMATCH_HINT);
			throw new Error("thread status failed (code " + r.code + "): " + (r.err || r.out).slice(0, 300));
		}
		const id = resolveIdentityFromStatus(runDeltaJsonFromText(r.out, r.err));
		if (!id) {
			throw new Error("thread status returned no thread_id: " + (r.out || r.err).slice(0, 300));
		}
		return id;
	}

	/** Sets the thread identity fields after a join (shared by weak and materialize).
	 *  Joining never steals the title: a joined thread's title belongs to its creator; setting the flag suppresses the input handler's first title record. */
	function adoptJoinedIdentity(id: DeltaIdentity) {
		rt.threadId = id.threadId;
		rt.url = id.url || null;
		rt.visibility = id.visibility ?? null;
		rt.titleSent = true;
		// The server cursor is authoritative: records may already sit in the temporary queue during the join (expectingIdentity); align and continue uniformly;
		// afterwards flush appends straight to that thread without waiting for mount (same first-record timing as share)
		adoptServerCursor(id.nextSequence);
	}

	/** Weak join tail (#17): attach the current checkout — worktree resolution → attach → mount → watch.
	 *  Precondition: the caller has confirmed the current directory is the checkout to attach (weak join semantics). */
	async function attachJoined(id: DeltaIdentity): Promise<void> {
		await preflightAuth(); // (#28) before the worktree attach mutation
		adoptJoinedIdentity(id);
		rt.worktreeRef = await resolveLocalWorktreeRef();
		await attachAndStream();
		persistBinding();
		notify(
			"Joined Delta thread: " + rt.threadId +
				"\nOpen in browser: " + (rt.url ?? "(link not fetched yet; updates automatically after reconnect)") +
				"\n(the current checkout and future conversation are syncing to that thread; use /delta-detach to exit)",
		);
	}

	/** worktree mount materialization (#22): `worktree mount <ref> <dir>` (without --existing-checkout)
	 *  materializes the shared checkout into a new directory; the ready line is {"operation":"mounted","reference","directory"}
	 *  (same shape as --existing-checkout, as observed); the process keeps running and syncing — it is the new directory's mount subprocess.
	 *  What resolves is the directory echoed by the ready line (the server's registration is authoritative); on timeout with the directory on disk,
	 *  continue with attachAndStream's tolerance (the process stays in the background); if the directory never lands, error out (the mount subprocess has exited). */
	function materializeCheckout(ref: string, requestedDir: string): Promise<string> {
		killChild(rt.mountChild);
		return new Promise<string>((resolve, reject) => {
			let settled = false;
			const finish = (dir: string, err?: string) => {
				if (settled) return;
				settled = true;
				if (err) reject(new Error(err));
				else resolve(dir);
			};
			rt.mountChild = spawnLineChild(
				["worktree", "mount", ref, requestedDir],
				(j) => {
					const d = String(j?.directory ?? "");
					if (d) finish(d);
				},
				(code) => {
					rt.mountChild = null;
					if (!settled) {
						finish("", "worktree mount materialization failed (exit " + code + "): " + requestedDir);
					} else if (!rt.detaching && rt.connected) {
						notify("Delta worktree sync process exited (exit " + code + ").", "warning");
					}
				},
			);
			schedule(() => {
				if (settled) return;
				if (fs.existsSync(requestedDir)) finish(requestedDir);
				else finish("", "worktree mount materialization timed out (" + Math.round(MOUNT_READY_TIMEOUT_MS / 1000) + "s) and the directory never landed: " + requestedDir);
			}, MOUNT_READY_TIMEOUT_MS);
		});
	}

	/** Full join tail (#22): materialize the thread's checkout into a new directory and switch the runtime to it.
	 *  ref is an existing worktree from the thread's status (no further worktree attach needed); the mount subprocess
	 *  is the materialized directory's continuous sync. Afterwards rt.cwd points at the materialized directory (the binding marker is written there too):
	 *  the omp session itself only migrates when the user runs /move manually (automation pending upstream oh-my-pi#17441),
	 *  but sync/watch/cursor don't depend on the session cwd — everything continues seamlessly after /move, see contract-notes. */
	async function materializeJoined(id: DeltaIdentity, ref: string, dir: string): Promise<void> {
		await preflightAuth(); // (#28) before the mount materialization mutation
		adoptJoinedIdentity(id);
		const actual = await materializeCheckout(ref, dir);
		rt.cwd = actual;
		rt.worktreeRef = ref;
		rt.detaching = false;
		rt.connected = true;
		rt.connectAttempts = 0;
		startWatch();
		void flush();
		persistBinding(actual);
		notify(
			"Materialized the thread's checkout: " + actual +
				"\nThe sync process has started (continuously uploading/downloading that directory)." +
				"\nNext step (manual; automation needs upstream oh-my-pi#17441 to let agents call cd):" +
				"\n  /move " + actual +
				"\nAfter the move, the thread binding and the server cursor are unchanged, watch keeps running, and sync points at the new directory; reopen omp in the new directory after a restart." +
				"\nThread: " + rt.threadId +
				"\nOpen in browser: " + (rt.url ?? "(link not fetched yet; updates automatically after reconnect)"),
		);
	}

	function detach(quiet = false) {
		rt.detaching = true;
		rt.detached = true;
		killChild(rt.mountChild);
		rt.mountChild = null;
		killChild(rt.watchChild);
		rt.watchChild = null;
		rt.connected = false;
		paintStatus(); // detach → detached → clear the slot (setStatus key → undefined, releasing it)
		if (!quiet) notify("Delta sync disconnected (the thread and uploaded content are kept).");
	}

	/** Slow-path discovery: after moveOff/fork this session has no marker, so look the server up for "the most recent omp
	 *  thread that bound this checkout" (thread list → per-thread status → worktrees matched against cwd).
	 *  Authority is DeltaDB; it doesn't depend on session id continuity — omp's continuation chain is transparent to it. */
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

	/** Reconnect from the session marker: status validates external_id (prevents silent continuation after a fork) → align the server cursor → attach.
	 *  Identity authority is DeltaDB: on status failure / a thread not belonging to this session, give clear guidance instead of silently recreating.
	 *  Decisive failures (mismatch/ambiguity/cross-directory) clear expectingIdentity; transient errors keep it and records keep queueing. */
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
			if (!id) throw new Error("thread status returned no thread_id");
			// While waiting for status, the user may have manually /delta-share or /delta-join'd another thread:
			// an expired reconnect gives up and never overwrites the foreground identity (#17 join makes this interleaving more common)
			if (rt.threadId && rt.seqReady) {
				notify("Background reconnect cancelled: the session already manually bound thread " + rt.threadId + ".");
				rt.expectingIdentity = false;
				return;
			}
			// An omp -r session resume forks a new sid: marker present, external_id different → re-bind the same thread as a new
			// producer (per-producer cursors are independent, no duplicates); no need for the user to /delta-share again.
			if (id.externalId && id.externalId !== rt.sid) {
				notify("Session has forked (" + id.externalId + " → " + rt.sid + "): continuing to record the same thread as a new producer.");
			}
			rt.threadId = id.threadId;
			rt.url = id.url || null;
			rt.visibility = id.visibility ?? null;
			const pick = pickWorktreeForCwd(id.worktrees, rt.cwd);
			if (pick.ambiguous) {
				notify("This thread carries multiple worktree identities; please re-bind with /delta-share.", "warning");
				rt.expectingIdentity = false;
				paintStatus();
				return;
			}
			let ref = pick.ref;
			if (!ref) {
				// status listed no worktree matching the current directory: refuse when the marker directory differs too, preventing an old thread from mounting the wrong checkout
				if (marker.cwd && normPath(marker.cwd) !== normPath(rt.cwd)) {
					notify(
						"This Delta thread is bound to the checkout at " + marker.cwd + ", but the current directory is " + rt.cwd
							+ "; open omp in the original directory, or /delta-share again (the old thread stays in Delta).",
						"warning",
					);
					rt.expectingIdentity = false;
					paintStatus(); // decisive failure: supplying → offline (thread exists, connection broken)
					return;
				}
				ref = String(marker.worktreeRef ?? "");
			}
			rt.worktreeRef = ref;
			adoptServerCursor(id.nextSequence);
			await attachAndStream();
			notify("Delta reconnected: " + (rt.url ?? rt.threadId));
		} catch (e) {
			// Transient error (incl. the provisioning window): keep expectingIdentity, records keep queueing; /delta-retry can try again
			notify("Delta reconnect failed: " + String(e instanceof Error ? e.message : e) + " (/delta-retry can try again)", "warning");
			paintStatus(); // most likely still supplying (expectingIdentity kept); a decisive failure has already cleared and repainted it in the inner layer
		}
	}

	// ---- Event and command registration ----

	// #15: handler references for the routable flat commands, backfilled at registration; the /delta router reuses the same function objects (alias semantics guaranteed by construction)
	const deltaHandlers: Record<string, (args: string, ctx: any) => Promise<void>> = {};
	const registerDeltaCommand = (
		name: string,
		def: { description: string; handler: (args: string, ctx: any) => Promise<void> },
	) => {
		deltaHandlers[name] = def.handler;
		pi.registerCommand(name, def);
	};

	registerDeltaCommand("delta-share", {
		description: "Connect the current omp session to a Delta thread (no argument: interactive visibility picker; 'private' argument: private directly; non-interactive environments default to sharing with the org)",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd); // omp's /cd changes the session cwd; use the current value before connecting
			if (rt.isSub) {
				notify("Subagent sessions cannot connect to Delta yet.", "warning");
				return;
			}
			if (rt.connected) {
				notify("Already connected: " + rt.threadId);
				return;
			}
			// Contract requirement: obtain user consent before sharing, and disclose the visibility scope (org name + content)
			let ok = true;
			let org: { id: string; name: string };
			let mode: "private" | "organization" = "organization"; // fallback default; explicit arguments or the picker override it
			try {
				org = await resolveOrg();
				// Visibility (#16): explicit arguments go straight to the old behavior; with no argument, show the picker (private vs. organization) first, then the same confirm+connect path
				const pick = await pickShareVisibility(args, ctx?.ui, org);
				if (pick.mode === null) {
					notify("Cancelled; no Delta thread created.");
					return;
				}
				if (pick.fallback) notify(pick.fallback, "warning");
				mode = pick.mode;
				if (typeof ctx?.ui?.confirm === "function") {
					ok = await ctx.ui.confirm(
						mode === "private" ? "Connect Delta (private thread)?" : "Connect Delta and share this session with the org?",
						mode === "private"
							? "Visibility: private, visible only to you\nShared content: conversation and workspace files, including uncommitted changes"
							: "Org: " + org.name + " (" + org.id + ")\nShared content: conversation and workspace files, including uncommitted changes",
					);
				} else {
					notify(mode === "private" ? "A private thread will be created." : "Will share with org: " + org.name);
				}
				if (!ok) {
					notify("Cancelled; no Delta thread created.");
					return;
				}
			} catch (e) {
				notify("Preparing the connection failed: " + String(e instanceof Error ? e.message : e), "warning");
				return;
			}
			try {
				await connectShared(mode, org);
			} catch (e) {
				const msg = String(e instanceof Error ? e.message : e);
				if (isTransientProvisioning(msg) && rt.connectAttempts < 40) {
					rt.connectAttempts += 1;
					if (rt.connectAttempts === 1 || rt.connectAttempts % 5 === 0)
						notify("Delta provisioning window, waiting (retry " + rt.connectAttempts + "): " + msg);
					paintStatus(); // background reconnect window → supplying
					schedule(() => {
						void retryConnect(mode, org);
					}, 30_000);
					return;
				}
				notify("Connection failed: " + msg, "warning");
				paintStatus(); // decisive failure: back to detached/offline
			}
		},
	});

	registerDeltaCommand("delta-join", {
		description: "Join an existing Delta thread (accepts https://delta.dev/join/…, delta://join/…, a thread_ share id, or a thread id; attaches the current checkout by default; --move or --into <abs-dir> instead materializes the thread's checkout into a new directory and guides /move)",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			if (rt.isSub) {
				notify("Subagent sessions cannot connect to Delta yet.", "warning");
				return;
			}
			if (rt.connected) {
				notify("Already connected: " + rt.threadId + "; to switch threads, /delta-detach first, then /delta-join.");
				return;
			}
			const inv = parseJoinInvocation(args);
			if (inv.into !== null && !inv.into) {
				notify("--into needs an absolute path: /delta-join <link|id> --into <dir>.", "warning");
				return;
			}
			if (inv.into !== null && !path.isAbsolute(inv.into)) {
				notify("--into needs an absolute path (the materialized directory cannot depend on the session cwd): " + inv.into, "warning");
				return;
			}
			if (inv.target.kind === "empty") {
				notify("Usage: /delta-join <link|id> [--move] [--into <dir>]\nThe target accepts a delta.dev join link, a thread_ share id, or a thread id; --move (or --into) materializes the thread's checkout into a new directory and guides /move.", "warning");
				return;
			}
			if (inv.target.kind === "invalid") {
				notify("Unrecognized join target: " + inv.target.raw + "\nPaste a full join link (https://delta.dev/join/… or delta://join/…) or a thread id.", "warning");
				return;
			}
			try {
				await ensureContractCompatible();
				// The git preflight belongs to weak join only (the materialization path's current directory need not be a git checkout, #22)
				if (!inv.move && inv.into === null) assertGitCheckout();
			} catch (e) {
				notify(String(e instanceof Error ? e.message : e), "warning");
				return;
			}
			try {
				const id = await resolveJoinedIdentity(inv.target);
				const pick = pickMaterializeCandidate(id.worktrees, rt.cwd);
				const what = inv.target.kind === "link" || inv.target.kind === "share-id" ? "share id " + inv.target.shareId : "thread " + inv.target.raw;
				const explicit = inv.move || inv.into !== null; // --into implies --move; explicit arguments imply informed consent (same precedent as share's explicit arguments)
				if (explicit && pick.kind === "local" && pick.cwdMatch) {
					// --move while already inside the checkout: materialization is pointless, go weak join (still consented, same as share's constant confirm)
					notify("The current directory is already this thread's checkout; attaching directly (no materialization needed).");
					if (!(await confirmWeakJoin(ctx, what))) return;
					await attachJoined(id);
				} else if (explicit) {
					await joinWithMaterialize(id, pick, inv);
				} else if (pick.kind === "local" && pick.cwdMatch) {
					if (!(await confirmWeakJoin(ctx, what))) return;
					await attachJoined(id);
				} else {
					// Default weak join; when the thread's checkout isn't the current directory, offer materialization once first, never silently attach to the wrong directory (#22)
					let offered = false;
					if (pick.kind === "local" && typeof ctx?.ui?.confirm === "function") {
						offered = true;
						const ok = await ctx.ui.confirm(
							"The thread's checkout is not the current directory; materialize a copy into a new directory?",
							"Already on this machine: " + pick.dirs.slice(0, 3).join("; ") +
								"\nConfirm = materialize into a new directory and print /move guidance" +
								"\nCancel = attach the current directory as usual (weak join)",
						);
						if (ok) {
							await joinWithMaterialize(id, pick, inv);
							return;
						}
					} else if (pick.kind !== "local" && pick.kind !== "empty") {
						notify("The thread's checkout is not on this machine; /delta-join … --move can materialize it into a new directory and guide /move.");
					} else if (pick.kind === "local") {
						notify("The current directory is not the thread's checkout (already on this machine: " + pick.dirs.slice(0, 3).join("; ") + "); attaching the current directory as usual.");
					}
					// The materialization prompt's cancel text already says "attach as usual"; no second confirm
					if (!offered && !(await confirmWeakJoin(ctx, what))) return;
					await attachJoined(id);
				}
			} catch (e) {
				// join's provisioning window differs from share's: the thread was created by someone else, so no automatic background retry (share's
				// retryConnect semantics are bound to connectShared); the account-mismatch hint already carries retry guidance.
				// Undo the title suppression: after a failed join, a later /delta-share should still send the first title
				rt.titleSent = false;
				notify("Join failed: " + String(e instanceof Error ? e.message : e), "warning");
			}
		},
	});

	/** Contract consent for weak join (carried over from #17): get consent before shared content (incl. uncommitted changes) enters the thread;
	 *  non-interactive environments reuse share's fallback (inform, then continue). */
	async function confirmWeakJoin(ctx: any, what: string): Promise<boolean> {
		if (typeof ctx?.ui?.confirm === "function") {
			const ok = await ctx.ui.confirm(
				"Join this Delta thread and attach the current checkout?",
				"Target: " + what + "\nShared content: conversation and workspace files, including uncommitted changes",
			);
			if (!ok) {
				notify("Cancelled; thread not joined.");
				return false;
			}
			return true;
		}
		notify("Will join the target thread and attach the current checkout (including uncommitted changes).");
		return true;
	}

	/** join's materialization branch (#22): decide the materialization target and ref, then go through materializeJoined.
	 *  pick.local with cwdMatch → no materialization (already inside the checkout); local elsewhere → re-materialize a fresh copy using
	 *  the first entry's ref; ambiguous/empty errors come from pickMaterializeCandidate. Materialized directory:
	 *  --into when given explicitly, otherwise derived from the thread title/share id (a sibling of the current directory); if it exists, --into is required.
	 *  Consent has already been obtained by the caller (explicit --move/--into, or the materialization prompt's confirm). */
	async function joinWithMaterialize(
		id: DeltaIdentity,
		pick: MaterializePick,
		inv: JoinInvocation,
	): Promise<void> {
		let ref: string;
		if (pick.kind === "ref" || pick.kind === "local") {
			// local can only be "elsewhere on this machine" (cwdMatch was intercepted in the handler): under an explicit request, use
			// the first entry's ref to materialize a fresh copy into a new directory
			ref = pick.ref;
		} else if (pick.kind === "ambiguous") {
			throw new Error(
				"This thread has several worktrees not on this machine; cannot determine which to materialize:\n  " +
					pick.refs.map((r) => r.slice(0, 60)).join("\n  ") +
					"\nWeak join from the correct checkout directory, or ask the thread's creator to confirm.",
			);
		} else {
			throw new Error("This thread has no worktree to materialize; ask the creator to mount the checkout first, or weak join from your checkout directory.");
		}
		const seed = id.title || (inv.target.kind === "link" || inv.target.kind === "share-id" ? inv.target.shareId : "") || id.threadId;
		const dir = inv.into ?? deriveMaterializeDir(rt.cwd, seed);
		if (fs.existsSync(dir)) {
			throw new Error("The materialization directory already exists: " + dir + "; use --into <abs-path> to name a directory that doesn't exist.");
		}
		await materializeJoined(id, ref, dir);
	}

	registerDeltaCommand("delta-status", {
		description: "Show omp-delta connection and sync status",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			// #21: show the repo policy on one line, making "why did/didn't it auto-connect" visible
			const policyRoot = gitCheckoutRoot(rt.cwd);
			const pf = policyRoot ? readRepoPolicyFile(policyRoot) : null;
			const lines = [
				"Connection: " + (rt.connected ? "connected" : "not connected") + (rt.paused ? " (push paused)" : ""),
				"Thread: " + (rt.threadId ?? "none"),
				rt.threadId ? "Visibility: " + (rt.visibility ?? "unknown") : "",
				rt.threadId ? "Link: " + (rt.url ?? "(not fetched yet; updates automatically after reconnect)") : "",
				"Records: " + rt.seq + " assigned, " + rt.lastAcked + " acked, " + rt.pending.length + " queued",
				"Binding: " + renderBindingState(rt.worktreeRef, rt.reusedBinding), // #26: reconciled vs reused-binding visible
				"worktree: " + (rt.worktreeRef ?? "none"),
				"mount/watch: " + (rt.mountChild ? "running" : "stopped") + " / " + (rt.watchChild ? "running" : "stopped"),
				"Session: " + (rt.isSub ? "subagent (not recorded)" : "main") + " · " + rt.sid,
				"Policy: " + (pf ? renderProjectPolicy(pf.policy) : "no Git checkout (n/a)"),
			].filter(Boolean);
			notify(lines.join("\n"));
		},
	});

	registerDeltaCommand("delta-setup", {
		description: "Set this repo's Delta sync policy (.delta/settings.json, delta-claude compatible): private|organization|manual; no argument opens an interactive picker",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			if (rt.isSub) {
				notify("Subagent sessions cannot use /delta-setup yet.", "warning");
				return;
			}
			const decision = parseSetupModeArg(args);
			if (decision.kind === "invalid") {
				notify(
					"Unknown mode: " + decision.arg + "\nUsage: /delta-setup private|organization|manual (no argument opens an interactive picker).\n" +
						"private/organization = auto-connect new sessions (private is visible only to you); manual = /delta-share manual sync only (current behavior).",
					"warning",
				);
				return;
			}
			const root = gitCheckoutRoot(rt.cwd);
			if (!root) {
				notify("/delta-setup needs a Git checkout: no repository root found under " + rt.cwd + ".", "warning");
				return;
			}
			// Read the existing file: unknown keys survive as a whole; never silently overwrite malformed files (delta-claude also asks first)
			const policyFile = readRepoPolicyFile(root);
			if (policyFile.malformed) {
				let replace = false;
				if (typeof ctx?.ui?.confirm === "function") {
					try {
						replace = await ctx.ui.confirm(
							"Replace the unparseable .delta/settings.json?",
							(policyFile.policy.problem ?? "The file is not a valid JSON object") + "\nThe original content is lost after replacing; choose cancel to fix it manually first.",
						);
					} catch {
						replace = false;
					}
				} else {
					notify(".delta/settings.json is unparseable (" + (policyFile.policy.problem ?? "?") + "); not overwritten automatically. Fix or delete the file manually, then retry.", "warning");
					return;
				}
				if (!replace) {
						notify("Cancelled; .delta/settings.json was left untouched.");
						return;
					}
				}
				// Org: required for private/organization (delta-claude semantics); explicit manual doesn't need it, so the current state can be pinned offline
			let org: { id: string; name: string } | null = null;
			if (!(decision.kind === "set" && decision.mode === "manual")) {
				try {
					org = await resolveOrg();
				} catch (e) {
					notify(
						"Resolving the current org failed: " + String(e instanceof Error ? e.message : e) +
							"\nprivate/organization policies require an org; if you only want to pin manual mode, run /delta-setup manual.",
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
					notify("Cancelled; .delta/settings.json was left untouched.");
					return;
				}
				if (pick.fallback) notify(pick.fallback, "warning");
				mode = pick.mode;
			}
			const text = serializeProjectPolicy(policyFile.document, mode, mode === "manual" ? undefined : org!.id);
			try {
				writeRepoPolicyFile(root, text);
			} catch (e) {
				notify("Failed to write .delta/settings.json: " + String(e instanceof Error ? e.message : e), "warning");
				return;
			}
			// delta-claude convention: the policy is a personal choice, so it goes into .git/info/exclude rather than the tracked .gitignore
			if (!excludeFromGit(root)) {
				notify("Could not exclude .delta/settings.json from Git; if you don't want to commit it, add a line to .gitignore manually: .delta/settings.json", "warning");
			}
			const label = mode === "private" ? "Private sync (private)" : mode === "organization" ? "Team sharing (shared with " + (org?.name ?? "?") + ")" : "Manual (manual)";
			notify(
				"Saved " + label + " to .delta/settings.json.\n" +
					(mode === "manual"
						? "New sessions sync only when you run /delta-share."
						: "New sessions will connect to Delta automatically (" + (mode === "private" ? "private threads" : "shared with org " + org!.name) + "); this session is unaffected — to connect now, run /delta-share."),
			);
		},
	});

	registerDeltaCommand("delta-detach", {
		description: "Disconnect Delta sync (the thread and uploaded content are kept)",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			if (!rt.threadId) {
				notify("Not connected to Delta yet.");
				return;
			}
			detach();
		},
	});

	pi.registerCommand("delta-org", {
		description: "Inspect Delta orgs (read-only): list shows orgs and marks the current one; current shows the org sharing will use",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			const mode = (String(args ?? "").trim().split(/\s+/)[0] ?? "").toLowerCase() || "list";
			if (mode !== "list" && mode !== "current") {
				notify("Usage: /delta-org list (list orgs and mark the current one) or /delta-org current (show the org sharing will use). Read-only: use delta org switch to change orgs.");
				return;
			}
			try {
				if (mode === "current") {
					const org = await resolveOrg(); // same source as /delta-share: shows the org sharing would actually use
					notify("Org sharing will use: " + org.name + " (" + org.id + ")");
					return;
				}
				const r = await runDelta(["org", "list"]);
				const combined = r.out + "\n" + r.err;
				const orgs = parseOrgListFromText(combined);
				if (!orgs) {
					notify(
						isUnauthenticatedText(combined) ? ORG_UNAUTH_HINT : "delta org list failed (code " + r.code + "): " + (r.err || r.out).slice(0, 200),
						"warning",
					);
					return;
				}
				notify(orgs.length > 0 ? "Delta orgs (" + orgs.length + "):\n" + formatOrgList(orgs) : formatOrgList(orgs));
			} catch (e) {
				notify("delta org failed: " + String(e instanceof Error ? e.message : e), "warning");
			}
		},
	});

	registerDeltaCommand("delta-retry", {
		description: "Resume/retry Delta push (also reconnects after a disconnect)",
		handler: async (_args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			rt.paused = false;
			rt.retryAttempts = 0;
			rt.historyWaitAttempts = 0; // the provisioning window can exceed the old budget: a manual retry must re-arm the long budget
			rt.connectAttempts = 0;
			paintStatus(ctx); // a manual retry clears offline/paused
			if (rt.threadId && !rt.connected) {
				notify("Trying to reconnect Delta thread " + rt.threadId + " …");
				try {
					await attachAndStream();
					notify("Reconnected.");
				} catch (e) {
					// (#25) Make the failure class obvious: permanent → "needs a human" (guidance in the message);
					// everything else → transient, auto-retry keeps backing off.
					if (e instanceof AttachError && e.permanent) {
						notify("Reconnect failed — this needs a human; auto-retry will not fix it.\n" + e.message, "warning");
					} else {
						notify(
							"Reconnect failed (transient; auto-retry keeps trying, /delta-retry to try again): " +
								String(e instanceof Error ? e.message : e),
								"warning",
							);
					}
					paintStatus(); // reconnect failed: the thread still exists → offline
					return;
				}
			} else if (!rt.threadId) {
				// Memory is empty after a process restart: reconnect from the session marker (without one, suggest /delta-share)
				const marker = findSessionMarker(ctx);
				if (marker) {
					notify("Reconnecting Delta thread " + marker.threadId + " from the session marker …");
					rt.expectingIdentity = true;
					await reconnectByIdentity(marker);
				} else {
					notify("No Delta thread bound yet; connect with /delta-share.");
				}
			} else {
				void flush();
			}
		},
	});

	pi.registerCommand("delta-update", {
		description: "Check or install Delta updates (/delta-update check checks without installing); the output carries the omp-delta plugin reinstall hint",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			rt.cwd = String(ctx?.cwd ?? rt.cwd);
			const mode: "check" | "update" = String(args ?? "").trim().toLowerCase().startsWith("check") ? "check" : "update";
			notify(mode === "check" ? "Checking for Delta updates…" : "Updating Delta (download and install may take a few minutes)…");
			// runDelta adds the global --json uniformly: `delta --json update --check` and `delta update --check --json` print the same output, as observed
			const r = await runDelta(mode === "check" ? ["update", "--check"] : ["update"], {
				cwd: rt.cwd,
				timeoutMs: UPDATE_TIMEOUT_MS,
			});
			const ok = r.code === 0;
			const parsed = parseUpdateResult(r.out + (r.err ? "\n" + r.err : ""));
			// Plugin-outdated detection: a contract min above this build's ceiling ⇒ must reinstall; without the contract, no verdict — only the constant hint remains
			let contractMin: number | null = null;
			try {
				const c = await runDeltaJson(["thread", "contract"], { cwd: rt.cwd });
				const min = Number(c?.min);
				if (Number.isFinite(min)) contractMin = min;
			} catch {
				/* no contract output: only the constant hint remains */
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

	// /delta router (#15,#17): forwards to the same handler as the flat command; with no argument, prints the usage menu instead of erroring
	pi.registerCommand("delta", {
		description: "omp-delta command router: /delta share|join|status|retry|detach (no argument shows usage; flat commands still work)",
		handler: async (args: string, ctx: any) => {
			lastCtx = ctx;
			const d = parseDeltaRouter(args);
			if (d.kind === "menu") {
				notify(renderDeltaMenu());
				return;
			}
			if (d.kind === "unknown") {
				notify("Unknown subcommand: " + d.command + "\n\n" + renderDeltaMenu(), "warning");
				return;
			}
			const handler = deltaHandlers[d.command];
			if (!handler) {
				// Registration order guarantees this is filled in by now; defensive fallback
				notify("Internal error: subcommand " + d.command + " is not registered.", "warning");
				return;
			}
			await handler(d.args, ctx);
		},
	});

	pi.on("session_start", async (_event: any, ctx: any) => {
		lastCtx = ctx;
		rt.eventCtx = ctx; // source of managed timers
		rt.isSub = (ctx?.agent?.kind ?? "main") !== "main";
		if (rt.isSub) return;
		rt.detached = false; // new session: explicit detach semantics don't carry over (the binding marker is discovered separately)
		rt.sid = String(ctx?.sessionManager?.getSessionId?.() ?? "sess-" + randomUUID());
		rt.cwd = String(ctx?.cwd ?? process.cwd());
		// #21: the repo policy (.delta/settings.json) decides auto-connect when unbound; file problems get a one-time notice here
		const policyRoot = gitCheckoutRoot(rt.cwd);
		const policyFile = policyRoot ? readRepoPolicyFile(policyRoot) : null;
		if (policyFile?.policy.problem) {
			notify(".delta/settings.json is unusable (" + policyFile.policy.problem + "); until it is fixed, only manual /delta-share sync is active.", "warning");
		}
		// The binding marker lives in the omp session record (written by appendEntry, lifecycle owned by omp):
		// marker present → reconnect in the background from it; no marker → discover by checkout first, then auto-connect per the repo policy (#21).
		const marker = findSessionMarker(ctx);
		// Decision (pure seam): the marker always wins; auto-connect is only the fallback for a truly brand-new session
		const syncAction = decideAutoSyncAction(policyFile?.policy ?? { mode: "manual", configured: false }, marker !== null);
		if (marker) {
			// Don't block session_start (event handlers get a 30s budget): reconnect in the background;
			// while waiting, events keep entering the temporary queue (expectingIdentity) and are renumbered and flushed once the cursor arrives
			rt.expectingIdentity = true;
			notify("Found the session's bound Delta thread " + marker.threadId + "; reconnecting in the background…");
			paintStatus(ctx); // session_start: rebuild/recalibrate the slot (session-replacing boundary, same as the spinner)
			void reconnectByIdentity(marker);
		} else {
			// No marker (moveOff/fork/first run): discover the binding from the server by checkout;
			// only when nothing is found is this truly a first run: auto-connect when the policy is an auto mode, else wait for an explicit /delta-share (#21).
			rt.expectingIdentity = true;
			paintStatus(ctx);
			void (async () => {
				try {
					const found = await discoverBindingByCheckout();
					if (found) {
						notify("Found the Delta thread bound to this checkout: " + found.threadId + "; reconnecting in the background…");
						await reconnectByIdentity(found);
					} else if (syncAction.kind === "auto-connect") {
						// Auto-connect only runs when both reconnect paths (marker/checkout discovery) missed; it never bypasses or races the reconnect;
						// during the connect window expectingIdentity holds, records enter the temporary queue, and are renumbered once the cursor arrives
						await autoConnect(syncAction.mode, syncAction.organization);
					} else {
						rt.expectingIdentity = false;
						paintStatus(); // no binding found: back to detached, slot cleared
					}
				} catch (e) {
					rt.expectingIdentity = false;
					paintStatus();
					notify("Delta binding discovery failed: " + String(e instanceof Error ? e.message : e) + " (/delta-share can connect manually)", "warning");
				}
			})();
		}
	});

	pi.on("session_switch", async () => {
		if (rt.isSub || !rt.threadId) return;
		if (rt.connected || rt.mountChild || rt.watchChild) {
			detach(true);
			notify("The session has switched; Delta sync stopped. The new session can reconnect with /delta-share.");
		} else {
			paintStatus(); // switched while a reconnect was in flight: no subprocesses to tear down, but still recalibrate the indicator (upstream dedups no-ops)
		}
	});

	pi.on("input", async (event: any) => {
		if (!shouldRecord()) return;
		const text = String(event?.text ?? event?.prompt ?? "").trim();
		if (!text) return;
		if (text.startsWith("/")) return; // slash commands are not conversation content, don't record
		if (!rt.titleSent) {
			rt.titleSent = true;
			let title = "";
			try {
				title = String((await pi?.getSessionName?.()) ?? "");
			} catch {
				/* no session name */
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
		// Contract requirement: a running record's turnSequence must equal its own sequence, otherwise the turn never opens
		// and subsequent agent_message/tool_call records are silently dropped server-side (no item generated).
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
		// The handler only gets a 2s budget: synchronous cleanup only, no waiting on CLI calls; no state needs persisting (zero local state)
		try {
			killChild(rt.mountChild);
			killChild(rt.watchChild);
		} catch {
			/* best effort */
		}
		rt.mountChild = null;
		rt.watchChild = null;
		clearStatus(); // release the status slot (one synchronous setStatus; research §6: #18 session teardown constraint)
	});
}
