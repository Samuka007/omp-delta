import assert from "node:assert/strict";
import * as path from "node:path";
import { describe, it } from "node:test";
import {
	classifyJoinFailure,
	deriveMaterializeDir,
	parseJoinInvocation,
	parseJoinTarget,
	pickMaterializeCandidate,
} from "../omp-delta.ts";

// #17:/delta-join 纯接缝(目标解析 + 失败分类)。live join 链接/share id/thread id 与
// 错误签名均取自 delta 0.23.0 实采(见 docs/contract-notes.md Join links);
// 这里只测提取与判定,不触网、不要求本地登录态。

describe("parseJoinTarget", () => {
	it("extracts the share id from an https delta.dev join link", () => {
		const raw = "https://delta.dev/join/thread_01m6jt5nyc4znwdfcg9p2mk49d";
		assert.deepEqual(parseJoinTarget(raw), { kind: "link", shareId: "thread_01m6jt5nyc4znwdfcg9p2mk49d", raw });
	});

	it("accepts the delta:// scheme and tolerates a trailing slash and surrounding whitespace", () => {
		assert.deepEqual(parseJoinTarget("  delta://join/thread_01abc/\n"), {
			kind: "link",
			shareId: "thread_01abc",
			raw: "delta://join/thread_01abc/",
		});
		assert.equal(parseJoinTarget("delta://join/thread_01abc").kind, "link");
	});

	it("recognizes a bare share id without inventing a thread-id relationship", () => {
		// 合约陷阱:share id(thread_01…)与 thread id 无派生关系,只做形态识别
		const r = parseJoinTarget("thread_01m6g8xfhjq7cxycw35h5h64zq");
		assert.equal(r.kind, "share-id");
		assert.equal((r as any).shareId, "thread_01m6g8xfhjq7cxycw35h5h64zq");
	});

	it("treats an opaque thread id as thread-id and preserves it verbatim", () => {
		const raw = "ksQQkb1Xm4vqSuaJfhrMS1kFTpLOADNQGZIBxBBKMeC2HWlBK4Odm0QS-xgK";
		assert.deepEqual(parseJoinTarget(raw), { kind: "thread-id", raw });
	});

	it("missing argument is empty, not an error", () => {
		assert.deepEqual(parseJoinTarget(""), { kind: "empty" });
		assert.deepEqual(parseJoinTarget("   "), { kind: "empty" });
		assert.deepEqual(parseJoinTarget(null), { kind: "empty" });
		assert.deepEqual(parseJoinTarget(undefined), { kind: "empty" });
	});

	it("paste noise with whitespace or path separators is rejected locally", () => {
		for (const bad of [
			"thread_01abc thread_02def",
			"https://evil.dev/join/thread_01abc",
			"https://delta.dev/other/thread_01abc",
			"join thread_01abc",
			"E:/some/path with space",
			"delta://join/thread_01abc?utm=1",
		]) {
			const r = parseJoinTarget(bad);
			assert.equal(r.kind, "invalid", JSON.stringify(bad));
			assert.equal((r as any).raw, bad);
		}
	});

	it("a non-delta https URL is invalid, never silently treated as a thread id", () => {
		assert.equal(parseJoinTarget("https://github.com/Samuka007/omp-delta").kind, "invalid");
	});
});

describe("parseJoinInvocation", () => {
	it("splits --move and --into off the target (#22)", () => {
		const r = parseJoinInvocation("https://delta.dev/join/thread_01abc --move");
		assert.equal(r.move, true);
		assert.equal(r.into, null);
		assert.equal(r.target.kind, "link");
		assert.equal((r.target as any).shareId, "thread_01abc");
	});

	it("--into takes the next token as the directory and implies move", () => {
		const r = parseJoinInvocation("--into E:\\repos\\x thread_01abc");
		assert.equal(r.move, false); // --into 隐含 move 的判定在 handler(explicit = move || into!==null)
		assert.equal(r.into, "E:\\repos\\x");
		assert.equal(r.target.kind, "share-id");
	});

	it("--into without a value yields an empty string (usage error in handler)", () => {
		const r = parseJoinInvocation("thread_01abc --into");
		assert.equal(r.into, "");
		assert.equal(r.target.kind, "share-id");
	});

	it("no flags passes through untouched", () => {
		const r = parseJoinInvocation("  delta://join/thread_01x  ");
		assert.equal(r.move, false);
		assert.equal(r.into, null);
		assert.equal(r.target.kind, "link");
	});

	it("unknown flags stay in the target and are rejected as invalid there", () => {
		const r = parseJoinInvocation("thread_01abc --force");
		assert.equal(r.target.kind, "invalid");
	});

	it("empty args produce an empty target", () => {
		assert.equal(parseJoinInvocation("").target.kind, "empty");
		assert.equal(parseJoinInvocation(null).target.kind, "empty");
	});
});

describe("pickMaterializeCandidate", () => {
	const items = [
		{ reference: "ref-a", checkouts: [{ directory: "E:\\dev\\omp-delta" }] },
		{ reference: "ref-b", checkouts: [{ directory: "E:\\nope\\missing" }] },
	];
	const exists = (p: string) => p === "E:\\dev\\omp-delta";

	it("cwd match → local with cwdMatch, no materialization needed", () => {
		const r = pickMaterializeCandidate(items, "e:/dev/omp-delta/", exists);
		assert.equal(r.kind, "local");
		assert.equal((r as any).cwdMatch, true);
		assert.deepEqual((r as any).dirs, ["E:\\dev\\omp-delta"]);
	});

	it("local elsewhere → local with cwdMatch false and a ref for an extra copy", () => {
		const r = pickMaterializeCandidate(items, "E:\\scratch", exists);
		assert.equal(r.kind, "local");
		assert.equal((r as any).cwdMatch, false);
		assert.equal((r as any).ref, "ref-a");
	});

	it("all foreign entries with exactly one ref → materializable", () => {
		const r = pickMaterializeCandidate([{ reference: "only-ref", checkouts: [{ directory: "E:\\gone" }] }], "E:\\scratch", () => false);
		assert.deepEqual(r, { kind: "ref", ref: "only-ref" });
	});

	it("multiple foreign refs are ambiguous and must not silently pick one", () => {
		const r = pickMaterializeCandidate(
			[
				{ reference: "ref-1", checkouts: [{ directory: "E:\\gone-1" }] },
				{ reference: "ref-2", checkouts: [{ directory: "E:\\gone-2" }] },
			],
			"E:\\scratch",
			() => false,
		);
		assert.deepEqual(r, { kind: "ambiguous", refs: ["ref-1", "ref-2"] });
	});

	it("a thread without worktrees is empty (nothing to materialize)", () => {
		assert.deepEqual(pickMaterializeCandidate([], "E:\\x", () => false), { kind: "empty" });
	});

	it("directory-keyed entries (no checkouts[]) participate too", () => {
		const r = pickMaterializeCandidate([{ reference: "ref-d", directory: "E:\\dev\\omp-delta" }], "E:\\scratch", exists);
		assert.equal(r.kind, "local");
		assert.deepEqual((r as any).dirs, ["E:\\dev\\omp-delta"]);
	});
});

describe("deriveMaterializeDir", () => {
	// 平台中立的基目录:dirname(cwd) 的同级 + delta- 前缀
	const base = process.platform === "win32" ? "E:\\w" : "/w";

	it("derives a sibling directory with a delta- prefix", () => {
		const cwd = path.join(base, "proj");
		assert.equal(deriveMaterializeDir(cwd, "Fix login bug"), path.join(base, "delta-Fix-login-bug"));
	});

	it("sanitizes path separators, whitespace and punctuation; truncates long seeds", () => {
		const cwd = path.join(base, "x");
		assert.equal(deriveMaterializeDir(cwd, "a/b\\c:d?e"), path.join(base, "delta-a-b-c-d-e"));
		assert.equal(deriveMaterializeDir(cwd, "x".repeat(80)), path.join(base, "delta-" + "x".repeat(48)));
	});

	it("a degenerate seed falls back to delta-join", () => {
		assert.equal(deriveMaterializeDir(path.join(base, "x"), "---"), path.join(base, "delta-join"));
		assert.equal(deriveMaterializeDir(path.join(base, "x"), ""), path.join(base, "delta-join"));
	});
});

describe("classifyJoinFailure", () => {
	// 实测错误签名(delta 0.23.0):
	// - 他人私有/不存在的分享 id → not_visible "shared thread is absent or not visible"
	// - 不属于本账号的线程 id / 链接有误 → invalid_reference "…exact thread ID in this account"
	const NOT_VISIBLE = 'delta: shared thread is absent or not visible\n{"error":{"code":"not_visible","reason":"not_visible","message":"shared thread is absent or not visible","retryable":false}}';
	const INVALID_REF = 'delta: expected a supported Delta link, share ID, or exact thread ID in this account\n{"error":{"code":"invalid_reference","reason":"invalid_reference","message":"expected a supported Delta link, share ID, or exact thread ID in this account","retryable":false}}';

	it("maps the not_visible signature to account-mismatch", () => {
		assert.equal(classifyJoinFailure(NOT_VISIBLE), "account-mismatch");
	});

	it("maps invalid_reference / not-in-this-account to account-mismatch", () => {
		assert.equal(classifyJoinFailure(INVALID_REF), "account-mismatch");
		assert.equal(classifyJoinFailure("thread not in this account"), "account-mismatch");
	});

	it("unauthenticated wins over account-mismatch (actionable login hint first)", () => {
		assert.equal(classifyJoinFailure("error: authentication required\n" + NOT_VISIBLE), "unauthenticated");
		assert.equal(classifyJoinFailure("not signed in"), "unauthenticated");
	});

	it("unknown failures stay unknown (handler shows the raw CLI message)", () => {
		assert.equal(classifyJoinFailure(""), "unknown");
		assert.equal(classifyJoinFailure("delta: network unreachable"), "unknown");
		// 供给窗口的重试签名不是账号问题:join 失败路径不把它伪装成可见性拒绝
		assert.equal(classifyJoinFailure("the history is unavailable: NotFound"), "unknown");
	});
});