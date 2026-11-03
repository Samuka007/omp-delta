import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	pickWorktreeForCwd,
	reconcileSequenceState,
	resolveIdentityFromStatus,
} from "../omp-delta.ts";

// 形状取自实采 `delta --json thread status` 输出(Windows,2026-11-03)
const LIVE_STATUS = {
	operation: "status",
	thread_id: "ksQQcjPMeKogRtSeoe9dwsAR5JLOAARbRJIBxBBKMeC2HWlBK4Odm0QS-xgK",
	title: "Recorded conversation",
	url: "https://delta.dev/join/thread_01m6bz37y66y06p2282pz21pwx",
	visibility: "private",
	upload: { state: "uploaded", error: null },
	worktrees: {
		status: "available",
		items: [
			{ reference: "9830fb46-6fb7-4c7c-88f6-805d73a41dda", path: "E:\\dev\\omp-delta" },
		],
	},
	producer: {
		source: "omp",
		external_id: "01a19a3e-72fa-76f7-a551-e81213eb5ebf",
		next_sequence: 14,
	},
};

describe("resolveIdentityFromStatus", () => {
	it("extracts identity, join url and delivery cursor from a live status", () => {
		const id = resolveIdentityFromStatus(LIVE_STATUS);
		assert.equal(id?.threadId, "ksQQcjPMeKogRtSeoe9dwsAR5JLOAARbRJIBxBBKMeC2HWlBK4Odm0QS-xgK");
		assert.equal(id?.url, "https://delta.dev/join/thread_01m6bz37y66y06p2282pz21pwx");
		assert.equal(id?.visibility, "private");
		assert.equal(id?.externalId, "01a19a3e-72fa-76f7-a551-e81213eb5ebf");
		assert.equal(id?.nextSequence, 14);
		assert.equal(id?.worktrees.length, 1);
	});

	it("join url is NOT derived from thread id (contract trap: share id ≠ thread id)", () => {
		const id = resolveIdentityFromStatus(LIVE_STATUS);
		assert.ok(!id!.url.includes(id!.threadId)); // join 链接用 thread_01… share id
	});

	it("tolerates a fresh thread without producer/worktrees", () => {
		const id = resolveIdentityFromStatus({ thread_id: "t1" });
		assert.equal(id?.threadId, "t1");
		assert.equal(id?.nextSequence, 0);
		assert.deepEqual(id?.worktrees, []);
		assert.equal(id?.url, "");
	});

	it("returns null when there is no thread to resume", () => {
		assert.equal(resolveIdentityFromStatus(null), null);
		assert.equal(resolveIdentityFromStatus({}), null);
		assert.equal(resolveIdentityFromStatus(undefined), null);
	});
});

describe("pickWorktreeForCwd", () => {
	const items = [
		{ reference: "ref-a", path: "E:\\dev\\omp-delta" },
		{ reference: "ref-b", directory: "/Users/samuka_007/omp-delta" },
	];

	it("matches the checkout by normalized path (windows separators, case, trailing slash)", () => {
		const r = pickWorktreeForCwd(items, "e:/dev/omp-delta/");
		assert.equal(r.ref, "ref-a");
		assert.equal(r.ambiguous, false);
	});

	it("matches the directory-keyed variant", () => {
		const r = pickWorktreeForCwd(items, "/Users/Samuka_007/omp-delta");
		assert.equal(r.ref, "ref-b");
	});

	it("no match yields empty ref", () => {
		const r = pickWorktreeForCwd(items, "/tmp/elsewhere");
		assert.equal(r.ref, "");
		assert.equal(r.ambiguous, false);
	});

	it("multiple matches are ambiguous and must not silently pick one", () => {
		const dup = [...items, { reference: "ref-c", path: "E:/dev/omp-delta" }];
		const r = pickWorktreeForCwd(dup, "E:\\dev\\omp-delta");
		assert.equal(r.ref, "");
		assert.equal(r.ambiguous, true);
	});
});

describe("reconcileSequenceState", () => {
	const rec = (seq: number) => ({ seq, update: {} });

	it("drops delivered records, keeps undelivered, continues from the cursor", () => {
		const r = reconcileSequenceState([rec(1), rec(2), rec(3)], 3); // 服务端已有 seq 1..2
		assert.deepEqual(r.pending, [rec(3)]); // seq 3 未送达,保留
		assert.equal(r.seq, 3);
		assert.equal(r.lastAcked, 2);
	});

	it("keeps undelivered records and keeps numbering above the server cursor", () => {
		const r = reconcileSequenceState([rec(1), rec(2), rec(3)], 2); // 服务端已有 seq 1
		assert.deepEqual(r.pending, [rec(2), rec(3)]);
		assert.equal(r.seq, 3);
		assert.equal(r.lastAcked, 1);
	});

	it("empty pending initializes from the server cursor", () => {
		const r = reconcileSequenceState([], 7);
		assert.deepEqual(r.pending, []);
		assert.equal(r.seq, 6);
		assert.equal(r.lastAcked, 6);
	});

	it("a fresh thread (next_sequence 0/absent) starts at zero", () => {
		const r = reconcileSequenceState([], 0);
		assert.equal(r.seq, 0);
		assert.equal(r.lastAcked, 0);
	});
});
