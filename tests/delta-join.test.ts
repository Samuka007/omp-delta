import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { classifyJoinFailure, parseJoinTarget } from "../omp-delta.ts";

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