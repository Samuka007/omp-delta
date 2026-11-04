import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	buildInitialExportRecords,
	shouldExportHistory,
} from "../omp-delta.ts";

// Fixture helpers shaped after omp's AgentMessage (pi-ai Message) as stored in session JSONL.
const userMsg = (text: string, extra: Record<string, unknown> = {}) => ({
	role: "user",
	content: [{ type: "text", text }],
	timestamp: 1,
	...extra,
});
const assistantMsg = (blocks: Record<string, unknown>[]) => ({
	role: "assistant",
	content: blocks,
	api: "antml",
	provider: "anthropic",
	model: "m",
	usage: {},
	stopReason: "stop",
	timestamp: 2,
});
const textBlock = (text: string) => ({ type: "text", text });
const toolCallBlock = (id: string, name: string, args: Record<string, unknown>) => ({
	type: "toolCall",
	id,
	name,
	arguments: args,
});
const toolResultMsg = (toolCallId: string, text: string, extra: Record<string, unknown> = {}) => ({
	role: "toolResult",
	toolCallId,
	toolName: "bash",
	content: [{ type: "text", text }],
	isError: false,
	timestamp: 3,
	...extra,
});

describe("shouldExportHistory", () => {
	it("exports onto a fresh producer", () => {
		assert.equal(shouldExportHistory(1), true);
		assert.equal(shouldExportHistory(0), true);
	});
	it("never exports onto a producer that already has records", () => {
		assert.equal(shouldExportHistory(2), false);
		assert.equal(shouldExportHistory(9), false);
	});
	it("decides unknown status by prior binding, not blindly", () => {
		// first share (no marker): export even while the provisioning window hides status
		assert.equal(shouldExportHistory(null, false), true);
		assert.equal(shouldExportHistory(undefined, false), true);
		// re-share (marker present): unknown status skips the export instead of risking duplication
		assert.equal(shouldExportHistory(null, true), false);
		assert.equal(shouldExportHistory(undefined, true), false);
	});
});

describe("buildInitialExportRecords", () => {
	it("returns no records for an empty transcript", () => {
		assert.deepEqual(buildInitialExportRecords([], 1), []);
	});

	it("groups a text turn: running opens it, messages reference it, idle closes it", () => {
		const records = buildInitialExportRecords(
			[userMsg("hello"), assistantMsg([textBlock("hi")])],
			1,
		);
		// running, user_message, agent_message, idle
		assert.equal(records.length, 4);
		assert.deepEqual(records.map((r) => r.seq), [1, 2, 3, 4]);
		const [running, user, agent, idle] = records;
		assert.equal(running!.update.sessionUpdate, "state_update");
		assert.equal(running!.update.state, "running");
		// Contract gate: the running record's turnSequence equals its own sequence
		assert.equal(running!.turnSequence, running!.seq);
		assert.equal(user!.update.sessionUpdate, "user_message");
		assert.equal(user!.turnSequence, 1);
		assert.equal(agent!.update.sessionUpdate, "agent_message");
		assert.equal(agent!.turnSequence, 1);
		assert.equal(idle!.update.sessionUpdate, "state_update");
		assert.equal(idle!.update.state, "idle");
		assert.equal(idle!.turnSequence, 1);
		// Rendering gate: live-activity markers on message rows
		assert.equal((user!.update as any)._meta.delta.activity_kind, "user_message");
		assert.equal((agent!.update as any)._meta.delta.activity_kind, "agent_message");
	});

	it("starts a new turn at each non-synthetic user prompt", () => {
		const records = buildInitialExportRecords(
			[
				userMsg("first"),
				assistantMsg([textBlock("a")]),
				userMsg("synthetic continuation", { synthetic: true }),
				assistantMsg([textBlock("b")]),
				userMsg("second"),
				assistantMsg([textBlock("c")]),
			],
			1,
		);
		const turns = records.filter((r) => r.update.sessionUpdate === "state_update" && r.update.state === "running");
		assert.equal(turns.length, 2);
		// The synthetic user message must not open a turn, but the following assistant text stays in turn 1;
		// turn 2's records carry turn 2's running record's own sequence, not an ordinal.
		const agentTurns = records.filter((r) => r.update.sessionUpdate === "agent_message").map((r) => r.turnSequence);
		assert.deepEqual(agentTurns, [turns[0]!.seq, turns[0]!.seq, turns[1]!.seq]);
	});

	it("exports tool calls with input and paired result", () => {
		const records = buildInitialExportRecords(
			[
				userMsg("run it"),
				assistantMsg([toolCallBlock("t1", "bash", { command: "ls" })]),
				toolResultMsg("t1", "file.txt"),
			],
			1,
		);
		const call = records.find((r) => (r.update as any).toolCallId === "t1" && (r.update as any).status === "in_progress");
		const result = records.find((r) => (r.update as any).toolCallId === "t1" && (r.update as any).status === "completed");
		assert.ok(call, "call record missing");
		assert.ok(result, "result record missing");
		assert.deepEqual(call!.update.rawInput, { command: "ls" });
		assert.equal(call!.update.name, "bash");
		assert.equal(result!.update.rawOutput, "file.txt");
		// Both records belong to the turn that made the call
		assert.equal(call!.turnSequence, result!.turnSequence);
	});

	it("survives unpaired calls and pending results at snapshot time", () => {
		const records = buildInitialExportRecords(
			[
				userMsg("go"),
				assistantMsg([toolCallBlock("t1", "bash", {}), toolCallBlock("t2", "bash", {})]),
				toolResultMsg("t1", "done"),
				// t2 has no result yet (mid-turn share); t3's call was elided but its result remains
				toolResultMsg("t3", "orphan"),
			],
			1,
		);
		const statuses = records.filter((r) => (r.update as any).toolCallId).map((r) => (r.update as any).status);
		assert.deepEqual(statuses, ["in_progress", "in_progress", "completed", "completed"]);
	});

	it("exports an edit diff from tool result details", () => {
		const records = buildInitialExportRecords(
			[
				userMsg("edit"),
				assistantMsg([toolCallBlock("e1", "edit", { input: "x" })]),
				toolResultMsg("e1", "ok", {
					toolName: "edit",
					details: { diff: "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@\n-x\n+y\n", path: "/repo/f.ts" },
				}),
			],
			1,
		);
		const result = records.find((r) => (r.update as any).toolCallId === "e1" && (r.update as any).status === "completed");
		assert.ok(result);
		assert.equal(result!.update.rawOutput, null);
		const content = (result!.update as any).content as any[];
		assert.equal(content[0].type, "diff");
		assert.equal(content[0].changes[0].path, "/repo/f.ts");
		assert.match(content[0].patch.text, /diff --git/);
		// A structured edit renders through the edit activity kind
		assert.equal((result!.update as any)._meta.delta.activity_kind, "edit");
	});

	it("continues sequences from startSeq without reusing numbers", () => {
		const records = buildInitialExportRecords([userMsg("a"), assistantMsg([textBlock("b")]), userMsg("c")], 5);
		const seqs = records.map((r) => r.seq);
		assert.deepEqual(seqs, seqs.map((_, i) => 5 + i));
		assert.ok(Math.min(...seqs) >= 5);
	});

	it("drops messages that render to nothing instead of opening empty turns", () => {
		const records = buildInitialExportRecords(
			[userMsg("   "), assistantMsg([textBlock("orphaned output")]), userMsg("real")],
			1,
		);
		const running = records.filter((r) => r.update.state === "running");
		// The blank prompt opens nothing; the orphaned assistant output keeps its own implicit turn,
		// and the real prompt opens the second turn.
		assert.equal(running.length, 2);
		assert.equal(
			records.filter((r) => r.update.sessionUpdate === "user_message").length,
			1,
		);
	});
});
