import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	buildAppendEnvelope,
	classifyAppendResult,
	collectReceipts,
	contiguousAck,
	isTransientProvisioning,
	liveActivity,
	parseJsonLines,
} from "../omp-delta.ts";

describe("collectReceipts", () => {
	it("parses the contract-notes receipt shape", () => {
		// docs/contract-notes.md 的实测回执
		const j = {
			operation: "appended",
			thread_id: "t",
			sequence: 3,
			next_sequence: 4,
			duplicate: false,
			disposition: "recorded",
			turn_sequence: 3,
		};
		const into = new Map<number, string>();
		collectReceipts(j, into);
		assert.deepEqual([...into], [[3, "recorded"]]);
	});

	it("accepts a receipts array and legacy status field", () => {
		const into = new Map<number, string>();
		collectReceipts({ receipts: [{ sequence: 1, status: "recorded" }, { sequence: 2, disposition: "fenced" }] }, into);
		assert.equal(into.get(1), "recorded");
		assert.equal(into.get(2), "fenced");
	});

	it("marks duplicate:true as duplicate unless fenced", () => {
		const into = new Map<number, string>();
		collectReceipts(
			[
				{ sequence: 1, disposition: "recorded", duplicate: true },
				{ sequence: 2, disposition: "recorded", duplicate: true },
				{ sequence: 3, disposition: "fenced", duplicate: true },
			],
			into,
		);
		assert.equal(into.get(1), "duplicate");
		assert.equal(into.get(2), "duplicate");
		assert.equal(into.get(3), "fenced"); // fenced 优先于 duplicate 标记
	});

	it("ignores lines without sequence or disposition", () => {
		const into = new Map<number, string>();
		collectReceipts({ operation: "status" }, into);
		assert.equal(into.size, 0);
	});
});

describe("contiguousAck", () => {
	const rec = (seq: number) => ({ seq, update: {} });

	it("stays put when nothing is acked", () => {
		assert.equal(contiguousAck([rec(4), rec(5)], 3, new Map()), 3);
	});

	it("advances over consecutive acked records", () => {
		const receipts = new Map<number, string>([
			[4, "recorded"],
			[5, "recorded"],
		]);
		assert.equal(contiguousAck([rec(4), rec(5)], 3, receipts), 5);
	});

	it("stops at the first unacked hole", () => {
		const receipts = new Map<number, string>([
			[4, "recorded"],
			[6, "recorded"],
		]);
		assert.equal(contiguousAck([rec(4), rec(5), rec(6)], 3, receipts), 4);
	});

	it("does not count fenced or errored dispositions as acked", () => {
		const receipts = new Map<number, string>([
			[4, "fenced"],
			[5, "recorded"],
		]);
		assert.equal(contiguousAck([rec(4), rec(5)], 3, receipts), 3);
	});

	it("cannot ack a sequence that is not in pending", () => {
		const receipts = new Map<number, string>([[4, "recorded"]]);
		assert.equal(contiguousAck([rec(5)], 3, receipts), 3);
	});
});

describe("classifyAppendResult", () => {
	it("clean success is not retryable and not fenced", () => {
		const r = classifyAppendResult(0, "", "{\"operation\":\"appended\",\"sequence\":1,\"disposition\":\"recorded\"}\n", new Map());
		assert.equal(r.fenced, false);
		assert.equal(r.retryable, false);
		assert.equal(r.notVisible, false);
	});

	it("any fenced receipt flags fenced", () => {
		const receipts = new Map<number, string>([[2, "fenced"]]);
		const r = classifyAppendResult(0, "", "", receipts);
		assert.equal(r.fenced, true);
	});

	it("not_visible provisioning error is retryable with the long backoff", () => {
		const err = JSON.stringify({ error: { reason: "not_visible", message: "the history is unavailable: NotFound", retryable: true, retry_after_ms: 12000 } });
		const r = classifyAppendResult(1, err + "\n", "", new Map());
		assert.equal(r.notVisible, true);
		assert.equal(r.retryable, true);
		assert.equal(r.retryAfter, 30_000); // 供给窗口统一 30s 长退避,不采信服务端短值
		assert.match(r.errMsg, /history is unavailable/);
	});

	it("history-unavailable message without JSON body still classifies via raw fallback", () => {
		const r = classifyAppendResult(1, "delta: the history is unavailable: NotFound\n", "", new Map());
		assert.equal(r.notVisible, true);
		assert.equal(r.retryable, true);
	});

	it("retryable server error honors retry_after_ms when not provisioning", () => {
		const err = JSON.stringify({ error: { message: "rate limited", retryable: true, retry_after_ms: 7000 } });
		const r = classifyAppendResult(1, err + "\n", "", new Map());
		assert.equal(r.retryable, true);
		assert.equal(r.notVisible, false);
		assert.equal(r.retryAfter, 7000);
	});

	it("non-retryable error keeps the message and stops", () => {
		const err = JSON.stringify({ error: { message: "bad envelope", retryable: false } });
		const r = classifyAppendResult(1, err + "\n", "", new Map());
		assert.equal(r.retryable, false);
		assert.equal(r.errMsg, "bad envelope");
	});

	it("spawn failure (code -1) is retryable with a generic message", () => {
		const r = classifyAppendResult(-1, "", "", new Map());
		assert.equal(r.retryable, true);
		assert.notEqual(r.errMsg, "");
	});
});

describe("isTransientProvisioning", () => {
	it("matches every known provisioning-window signature", () => {
		for (const msg of [
			"worktree attach 失败: delta: the history is unavailable: NotFound",
			"error: not_visible",
			"thread not in this account",
			"invalid_reference",
			"worktree_storage_failed",
		]) {
			assert.equal(isTransientProvisioning(msg), true, msg);
		}
	});

	it("does not match permanent failures", () => {
		assert.equal(isTransientProvisioning("当前目录不是 git 仓库"), false);
		assert.equal(isTransientProvisioning(""), false);
	});
});

describe("buildAppendEnvelope", () => {
	it("wraps each record with schemaVersion/sequence and optional turnSequence", () => {
		const body = buildAppendEnvelope([
			{ seq: 1, update: { sessionUpdate: "user_message" } },
			{ seq: 2, turnSequence: 1, update: { sessionUpdate: "agent_message" } },
		]);
		const lines = body.split("\n");
		assert.equal(lines.length, 3); // 两条记录 + 末尾换行产生的空串
		assert.deepEqual(JSON.parse(lines[0]), { schemaVersion: 1, sequence: 1, update: { sessionUpdate: "user_message" } });
		assert.deepEqual(JSON.parse(lines[1]), { schemaVersion: 1, sequence: 2, turnSequence: 1, update: { sessionUpdate: "agent_message" } });
	});

	it("returns empty string for no pending records", () => {
		assert.equal(buildAppendEnvelope([]), "");
	});
});

describe("liveActivity", () => {
	it("marks user/agent/thought messages with activity kind", () => {
		const out = liveActivity({ sessionUpdate: "agent_message", content: [] });
		assert.equal(out._meta.delta.activity_kind, "agent_message");
		assert.equal(out._meta.delta.work_origin, "external_agent");
		assert.equal(out._meta.delta.interaction_surface, "cli");
	});

	it("maps tool_call_update to tool_call", () => {
		const out = liveActivity({ sessionUpdate: "tool_call_update", toolCallId: "x" });
		assert.equal(out._meta.delta.activity_kind, "tool_call");
	});

	it("leaves unmarked updates untouched", () => {
		const update = { sessionUpdate: "state_update", state: "running" };
		assert.equal(liveActivity(update), update); // 同一引用:无 _meta 时原样返回
	});
});

describe("parseJsonLines", () => {
	it("parses JSONL lines", () => {
		assert.deepEqual(parseJsonLines('{"a":1}\n{"a":2}\n'), [{ a: 1 }, { a: 2 }]);
	});

	it("falls back to whole-text JSON.parse for pretty-printed output (thread list shape)", () => {
		const pretty = '[\n  {\n    "id": "ksQQaxtj",\n    "name": "manual probe"\n  },\n  {\n    "id": "ksQQg5RG",\n    "name": "probe"\n  }\n]';
		// 整体 parse 的结果包一层:调用方(runDeltaJson 取末元素 / 调用侧 Array.isArray)按数组解包
		assert.deepEqual(parseJsonLines(pretty), [
			[
				{ id: "ksQQaxtj", name: "manual probe" },
				{ id: "ksQQg5RG", name: "probe" },
			],
		]);
	});

	it("returns empty for non-JSON output", () => {
		assert.deepEqual(parseJsonLines("delta: some human text\nmore text"), []);
	});
});
