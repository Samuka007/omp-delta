import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	AttachError,
	classifyAttachFailure,
	DELTA_TERMS_URL,
	parseAuthStatus,
	renderAttachFailureGuidance,
	renderBindingState,
	renderConnectNotice,
	resolveWorktreeChoice,
} from "../omp-delta.ts";

// ---- classifyAttachFailure (#23): envelope first, raw-text fallback ----

describe("classifyAttachFailure", () => {
	it("envelope retryable:true → retryable with retry_after_ms", () => {
		const c = classifyAttachFailure(1, '{"error":{"code":"transient","retryable":true,"retry_after_ms":2500,"message":"busy"}}', "");
		assert.equal(c.kind, "retryable");
		assert.equal(c.retryAfterMs, 2500);
		assert.equal(c.code, "transient");
		assert.equal(c.message, "busy");
	});

	it("envelope retryable:false → permanent (disconnected class)", () => {
		const c = classifyAttachFailure(1, '{"error":{"code":"worktree_remote_failed","retryable":false,"message":"disconnected"}}', "");
		assert.equal(c.kind, "permanent");
		assert.equal(c.code, "worktree_remote_failed");
		assert.equal(c.message, "disconnected");
	});

	it("provisioning signatures are retryable regardless of the envelope flag", () => {
		for (const msg of ["not_visible", "history is unavailable", "not in this account", "invalid_reference", "worktree_storage_failed"]) {
			const c = classifyAttachFailure(1, '{"error":{"code":"' + msg + '","retryable":false,"message":"x"}}', "");
			assert.equal(c.kind, "provisioning", msg);
		}
	});

	it("already has a worktree → reused-binding, wins over the envelope", () => {
		const c = classifyAttachFailure(1, '{"error":{"code":"conflict","message":"thread already has a worktree"}}', "");
		assert.equal(c.kind, "reused-binding");
	});

	it("raw-text fallback without JSON: provisioning vs permanent", () => {
		assert.equal(classifyAttachFailure(1, "delta: history is unavailable", "").kind, "provisioning");
		assert.equal(classifyAttachFailure(1, "delta: starting delta-app\ndelta: disconnected", "").kind, "permanent");
	});

	it("spawn failure (code -1) → retryable", () => {
		const c = classifyAttachFailure(-1, "", "");
		assert.equal(c.kind, "retryable");
	});
});

// ---- AttachError + guidance (#25) ----

describe("AttachError", () => {
	it("permanent failures carry actionable guidance and permanent=true", () => {
		const e = new AttachError(classifyAttachFailure(1, '{"error":{"code":"worktree_remote_failed","retryable":false,"message":"disconnected"}}', ""));
		assert.equal(e.permanent, true);
		const msg = renderAttachFailureGuidance(e.cls);
		assert.match(msg, /worktree_remote_failed/);
		assert.match(msg, /delta auth status/);
		assert.match(msg, /delta\.log/);
	});

	it("retryable classes keep the plain message (no guidance block)", () => {
		const e = new AttachError(classifyAttachFailure(1, '{"error":{"code":"transient","retryable":true,"retry_after_ms":100,"message":"busy"}}', ""));
		assert.equal(e.permanent, false);
		assert.equal(renderAttachFailureGuidance(e.cls), "worktree attach failed (transient): busy");
	});
});

// ---- renderConnectNotice (#24): never fabricate a join link from the create id ----

describe("renderConnectNotice", () => {
	it("prints the server url when available", () => {
		const n = renderConnectNotice("abc123", "https://delta.dev/join/thread_01xyz");
		assert.match(n, /https:\/\/delta\.dev\/join\/thread_01xyz/);
	});

	it("never contains a link built from the thread id when the url is missing", () => {
		const n = renderConnectNotice("abc12345", null);
		assert.doesNotMatch(n, /delta\.dev\/join\/abc12345/);
		assert.match(n, /\/delta-status/);
	});
});

// ---- renderBindingState (#26) ----

describe("renderBindingState", () => {
	it("distinguishes reconciled from reused-binding", () => {
		assert.equal(renderBindingState("wt1", false), "reconciled");
		assert.match(renderBindingState("wt1", true), /reused existing binding/);
		assert.match(renderBindingState("wt1", true), /reconcile skipped/);
		assert.equal(renderBindingState(null, false), "none");
	});
});

// ---- resolveWorktreeChoice (#27): list → marker (cwd-verified) → create ----

describe("resolveWorktreeChoice", () => {
	const rows = [{ reference: "wt-list", directory: "E:/dev/repo" }];

	it("list match wins", () => {
		const r = resolveWorktreeChoice(rows, "e:/dev/repo", "wt-marker", "e:/dev/repo");
		assert.deepEqual(r, { ref: "wt-list", source: "list", ambiguous: false });
	});

	it("marker ref preferred over create when the list misses and cwd matches", () => {
		const r = resolveWorktreeChoice([], "e:/dev/repo", "wt-marker", "E:/DEV/repo/");
		assert.deepEqual(r, { ref: "wt-marker", source: "marker", ambiguous: false });
	});

	it("marker from another directory is ignored → create", () => {
		const r = resolveWorktreeChoice([], "e:/dev/repo", "wt-marker", "e:/dev/other");
		assert.deepEqual(r, { ref: "", source: "create", ambiguous: false });
	});

	it("create only when neither list nor marker yields a reference", () => {
		const r = resolveWorktreeChoice([], "e:/dev/repo");
		assert.deepEqual(r, { ref: "", source: "create", ambiguous: false });
	});

	it("ambiguous list stays ambiguous (human decides)", () => {
		const r = resolveWorktreeChoice(rows.concat([{ reference: "wt2", directory: "E:/dev/repo" }]), "e:/dev/repo", "wt-marker");
		assert.equal(r.ambiguous, true);
	});
});

// ---- parseAuthStatus (#28) ----

describe("parseAuthStatus", () => {
	it("terms rejection wins over everything", () => {
		assert.equal(parseAuthStatus('{"error":{"code":"access_denied","reason":"terms_not_accepted"}}'), "terms");
	});

	it("signed_out signatures", () => {
		for (const t of ["Not signed in", "authentication required", "Saved credentials could not be restored", '{"state":"signed_out"}']) {
			assert.equal(parseAuthStatus(t), "signed_out", t);
		}
	});

	it("signed_in and unknown", () => {
		assert.equal(parseAuthStatus("Signed in as sam@example.com"), "signed_in");
		assert.equal(parseAuthStatus(""), "unknown");
		assert.equal(parseAuthStatus("something unreadable"), "unknown");
	});

	it("terms URL constant is the documented one", () => {
		assert.equal(DELTA_TERMS_URL, "https://zed.dev/delta/terms");
	});
});
