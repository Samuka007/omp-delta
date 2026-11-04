import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	deriveSyncIndicatorState,
	renderStatusIndicator,
	shortThreadId,
	type SyncIndicatorInput,
} from "../omp-delta.ts";

// #18:同步状态指示的纯接缝。Runtime 字段 → 状态(derive)与状态 → 文案(render)分开测;
// 覆盖 synced/supplying/offline/paused→offline/detached 的转移对与短 id 截断。
// 交互绘制(paintStatus / ctx.ui.setStatus)无法单测:见 docs/status-indicator-research.md §5。

function input(overrides: Partial<SyncIndicatorInput> = {}): SyncIndicatorInput {
	return {
		connected: false,
		paused: false,
		detached: false,
		expectingIdentity: false,
		threadId: "thread_01s5e5he446jdgzyk4",
		pendingCount: 0,
		retryAttempts: 0,
		historyWaitAttempts: 0,
		connectAttempts: 0,
		...overrides,
	};
}

describe("deriveSyncIndicatorState", () => {
	it("never bound → detached", () => {
		assert.equal(deriveSyncIndicatorState(input({ threadId: null })), "detached");
	});

	it("connected and idle → synced", () => {
		assert.equal(deriveSyncIndicatorState(input({ connected: true })), "synced");
	});

	it("each in-flight window flips synced → supplying", () => {
		const connected = { connected: true } as const;
		assert.equal(deriveSyncIndicatorState(input({ ...connected, pendingCount: 2 })), "supplying");
		assert.equal(deriveSyncIndicatorState(input({ ...connected, expectingIdentity: true })), "supplying");
		assert.equal(deriveSyncIndicatorState(input({ ...connected, retryAttempts: 1 })), "supplying");
		assert.equal(deriveSyncIndicatorState(input({ ...connected, historyWaitAttempts: 1 })), "supplying");
		assert.equal(deriveSyncIndicatorState(input({ ...connected, connectAttempts: 1 })), "supplying");
	});

	it("transport down with a binding → offline (not detached)", () => {
		assert.equal(deriveSyncIndicatorState(input({ connected: false })), "offline");
	});

	it("paused outranks in-flight work → offline", () => {
		assert.equal(
			deriveSyncIndicatorState(input({ connected: true, paused: true, pendingCount: 3, retryAttempts: 1 })),
			"offline",
		);
	});

	it("explicit detach clears the slot even while records remain pending", () => {
		assert.equal(
			deriveSyncIndicatorState(input({ connected: true, detached: true, pendingCount: 5 })),
			"detached",
		);
	});

	it("reconnect windows during identity adoption stay supplying", () => {
		assert.equal(
			deriveSyncIndicatorState(input({ expectingIdentity: true, connectAttempts: 3 })),
			"supplying",
		);
	});

	it("transitions: synced ↔ supplying ↔ offline ↔ detached follow field flips", () => {
		// enqueue → drain(排队→同步)
		assert.equal(deriveSyncIndicatorState(input({ connected: true, pendingCount: 1 })), "supplying");
		assert.equal(deriveSyncIndicatorState(input({ connected: true, pendingCount: 0 })), "synced");
		// retry start/stop(重试起停)
		assert.equal(deriveSyncIndicatorState(input({ connected: true, retryAttempts: 0 })), "synced");
		assert.equal(deriveSyncIndicatorState(input({ connected: true, retryAttempts: 2 })), "supplying");
		// watch 掉线 → retry 恢复
		assert.equal(deriveSyncIndicatorState(input({ connected: false, retryAttempts: 2 })), "offline");
	});
});

describe("shortThreadId", () => {
	it("strips the constant thread_ prefix then truncates to 8 chars", () => {
		assert.equal(shortThreadId("thread_01s5e5he446jdgzyk4"), "01s5e5he");
	});

	it("truncates bare ids to 8 chars", () => {
		assert.equal(shortThreadId("abcdefghij"), "abcdefgh");
	});

	it("keeps short ids intact", () => {
		assert.equal(shortThreadId("t"), "t");
		assert.equal(shortThreadId("12345678"), "12345678");
	});
});

describe("renderStatusIndicator", () => {
	const id = "thread_01s5e5he446jdgzyk4";

	it("renders Δ <short> · <state> for the live states", () => {
		assert.equal(renderStatusIndicator("synced", id), "Δ 01s5e5he · synced");
		assert.equal(renderStatusIndicator("supplying", id), "Δ 01s5e5he · supplying");
		assert.equal(renderStatusIndicator("offline", id), "Δ 01s5e5he · offline");
	});

	it("detached → null (caller clears the slot)", () => {
		assert.equal(renderStatusIndicator("detached", id), null);
		assert.equal(renderStatusIndicator("detached", null), null);
	});

	it("handles null threadId defensively (empty short id)", () => {
		assert.equal(renderStatusIndicator("offline", null), "Δ  · offline");
	});

	it("composes with derive for the full state→text mapping", () => {
		const state = deriveSyncIndicatorState(input({ connected: true, paused: true }));
		assert.equal(renderStatusIndicator(state, id), "Δ 01s5e5he · offline");
	});
});