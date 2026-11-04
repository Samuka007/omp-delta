import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	decideAutoSyncAction,
	parseSetupModeArg,
	pickSetupMode,
	readProjectPolicy,
	recordsAutomatically,
	renderProjectPolicy,
	serializeProjectPolicy,
	type ProjectPolicy,
} from "../omp-delta.ts";

// live-format fixtures: shapes taken verbatim from zed-industries/delta-claude hooks/setup/project-policy.ts
// (legal inputs to readProjectPolicy / write output of serializeProjectPolicy; the org stores team.id, i.e. the org id).
// Compatibility goal: files written by delta-claude are readable by omp, and files written by omp are readable by delta-claude.
const FIXTURE_PRIVATE = `{
  "threads": {
    "mode": "private",
    "organization": "org_01J8ZQ4V7M3X"
  }
}
`;
const FIXTURE_ORGANIZATION = `{
  "threads": {
    "mode": "organization",
    "organization": "org_01J8ZQ4V7M3X"
  }
}
`;
const FIXTURE_MANUAL_WITH_ORG = `{
  "threads": {
    "mode": "manual",
    "organization": "org_01J8ZQ4V7M3X"
  }
}
`;

const ORG = { id: "org_01J8ZQ4V7M3X", name: "Acme Corp" };

/** Fakes ctx.ui.select: records every call's arguments; reply returns the chosen label or undefined (Esc); throwing simulates a host error. */
function fakeSelect(reply: (title: string, options: Array<{ label: string; description?: string }>) => string | undefined) {
	const calls: Array<{ title: string; options: Array<{ label: string; description?: string }> }> = [];
	const select = async (title: string, options: Array<{ label: string; description?: string }>) => {
		calls.push({ title, options });
		return reply(title, options);
	};
	return { select, calls };
}

describe("readProjectPolicy (#21, delta-claude live format)", () => {
	it("private policy: mode + organization under threads, configured, records automatically", () => {
		const f = readProjectPolicy(FIXTURE_PRIVATE);
		assert.equal(f.malformed, undefined);
		assert.equal(f.policy.mode, "private");
		assert.equal(f.policy.organization, "org_01J8ZQ4V7M3X");
		assert.equal(f.policy.configured, true);
		assert.equal(f.policy.problem, undefined);
		assert.equal(recordsAutomatically(f.policy), true);
	});

	it("organization policy: same shape, records automatically", () => {
		const f = readProjectPolicy(FIXTURE_ORGANIZATION);
		assert.equal(f.policy.mode, "organization");
		assert.equal(f.policy.organization, "org_01J8ZQ4V7M3X");
		assert.equal(f.policy.problem, undefined);
		assert.equal(recordsAutomatically(f.policy), true);
	});

	it("manual with organization (delta-claude saves the share target): configured, not automatic", () => {
		const f = readProjectPolicy(FIXTURE_MANUAL_WITH_ORG);
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.organization, "org_01J8ZQ4V7M3X");
		assert.equal(f.policy.configured, true);
		assert.equal(recordsAutomatically(f.policy), false);
	});

	it("manual without organization is valid and configured", () => {
		const f = readProjectPolicy('{\n  "threads": { "mode": "manual" }\n}\n');
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.organization, undefined);
		assert.equal(f.policy.configured, true);
		assert.equal(f.policy.problem, undefined);
	});

	it("missing file (undefined text) = unconfigured manual without problem", () => {
		const f = readProjectPolicy(undefined);
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.configured, false);
		assert.equal(f.policy.problem, undefined);
		assert.equal(recordsAutomatically(f.policy), false);
		assert.equal(f.document, undefined);
	});

	it("missing threads key = unconfigured manual, document preserved for rewrite", () => {
		const f = readProjectPolicy('{\n  "custom": { "x": 1 }\n}\n');
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.configured, false);
		assert.equal(f.policy.problem, undefined);
		assert.deepEqual(f.document, { custom: { x: 1 } });
	});

	it("missing mode inside threads = unconfigured manual", () => {
		const f = readProjectPolicy('{"threads":{"organization":"org_01"}}');
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.configured, false);
		assert.equal(f.policy.problem, undefined);
	});

	it("threads non-object = manual fallback with problem, document kept", () => {
		const f = readProjectPolicy('{"threads":"nope"}');
		assert.equal(f.malformed, undefined);
		assert.equal(f.policy.mode, "manual");
		assert.match(f.policy.problem ?? "", /threads/);
		assert.ok(f.document);
	});

	it("invalid mode string = manual fallback with problem listing valid modes", () => {
		const f = readProjectPolicy('{"threads":{"mode":"public","organization":"org_01"}}');
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.configured, false);
		assert.match(f.policy.problem ?? "", /manual \/ private \/ organization/);
	});

	it("auto mode without organization = manual fallback with problem (delta-claude rule)", () => {
		for (const mode of ["private", "organization"]) {
			const f = readProjectPolicy(JSON.stringify({ threads: { mode } }));
			assert.equal(f.policy.mode, "manual");
			assert.equal(f.policy.configured, false);
			assert.match(f.policy.problem ?? "", new RegExp(mode));
		}
	});

	it("malformed JSON = malformed flag + manual fallback with problem", () => {
		const f = readProjectPolicy("{ not json");
		assert.equal(f.malformed, true);
		assert.equal(f.policy.mode, "manual");
		assert.equal(f.policy.configured, false);
		assert.ok(f.policy.problem);
		assert.equal(f.document, undefined);
	});

	it("non-object JSON document (array/string/number) = malformed manual fallback", () => {
		for (const text of ["[1,2]", "\"x\"", "42", "null"]) {
			const f = readProjectPolicy(text);
			assert.equal(f.malformed, true);
			assert.equal(f.policy.mode, "manual");
			assert.ok(f.policy.problem);
		}
	});
});

describe("serializeProjectPolicy (#21, delta-claude compatible)", () => {
	it("writes the live delta-claude shape: threads.mode + threads.organization, 2-space indent, trailing newline", () => {
		const text = serializeProjectPolicy(undefined, "private", "org_01J8ZQ4V7M3X");
		assert.equal(text, FIXTURE_PRIVATE);
	});

	it("preserves unknown top-level keys and unknown keys inside threads", () => {
		const previous = { custom: { x: 2 }, threads: { mode: "manual", futureKey: 1 } };
		const text = serializeProjectPolicy(previous, "organization", "org_02");
		const doc = JSON.parse(text);
		assert.deepEqual(doc.custom, { x: 2 });
		assert.equal(doc.threads.futureKey, 1);
		assert.equal(doc.threads.mode, "organization");
		assert.equal(doc.threads.organization, "org_02");
	});

	it("organization === undefined deletes threads.organization (manual without share target)", () => {
		const text = serializeProjectPolicy({ threads: { mode: "private", organization: "org_01" } }, "manual");
		const doc = JSON.parse(text);
		assert.equal(doc.threads.mode, "manual");
		assert.equal("organization" in doc.threads, false);
	});

	it("round-trip: serialized text reads back as the same policy", () => {
		const text = serializeProjectPolicy(undefined, "organization", "org_01J8ZQ4V7M3X");
		const f = readProjectPolicy(text);
		assert.equal(f.policy.mode, "organization");
		assert.equal(f.policy.organization, "org_01J8ZQ4V7M3X");
		assert.equal(f.policy.configured, true);
		assert.equal(f.policy.problem, undefined);
	});

	it("rewriting a delta-claude manual+org file keeps unknown keys and drops the unused org for manual", () => {
		const f = readProjectPolicy(FIXTURE_MANUAL_WITH_ORG);
		const text = serializeProjectPolicy(f.document, "manual");
		const doc = JSON.parse(text);
		assert.equal(doc.threads.mode, "manual");
		assert.equal("organization" in doc.threads, false);
	});
});

describe("decideAutoSyncAction (#21 session_start decision)", () => {
	const privatePolicy: ProjectPolicy = { mode: "private", organization: "org_01", configured: true };
	const orgPolicy: ProjectPolicy = { mode: "organization", organization: "org_01", configured: true };

	it("session marker takes precedence over auto-connect (reconnect never bypassed)", () => {
		assert.deepEqual(decideAutoSyncAction(privatePolicy, true), { kind: "reconnect", reason: "marker" });
		assert.deepEqual(decideAutoSyncAction(orgPolicy, true), { kind: "reconnect", reason: "marker" });
	});

	it("no marker + private/organization policy = auto-connect with mode and stored org", () => {
		assert.deepEqual(decideAutoSyncAction(privatePolicy, false), { kind: "auto-connect", mode: "private", organization: "org_01" });
		assert.deepEqual(decideAutoSyncAction(orgPolicy, false), { kind: "auto-connect", mode: "organization", organization: "org_01" });
	});

	it("manual policy (configured or not) never auto-connects", () => {
		assert.deepEqual(decideAutoSyncAction({ mode: "manual", configured: true }, false), { kind: "manual", configured: true });
		assert.deepEqual(decideAutoSyncAction({ mode: "manual", configured: false }, false), { kind: "manual", configured: false });
	});

	it("defensive: automatic mode without organization degrades to manual", () => {
		assert.deepEqual(decideAutoSyncAction({ mode: "private", configured: true }, false), { kind: "manual", configured: true });
	});
});

describe("parseSetupModeArg (#21)", () => {
	it("empty/blank args pick interactively", () => {
		for (const args of ["", "   "]) assert.deepEqual(parseSetupModeArg(args), { kind: "picker" });
	});

	it("private accepted case-insensitively with #16 priv* normalization", () => {
		for (const args of ["private", "PRIVATE", "  priv "]) {
			assert.deepEqual(parseSetupModeArg(args), { kind: "set", mode: "private" });
		}
	});

	it("organization/org accepted case-insensitively", () => {
		for (const args of ["organization", "ORG", " org "]) {
			assert.deepEqual(parseSetupModeArg(args), { kind: "set", mode: "organization" });
		}
	});

	it("manual accepted case-insensitively", () => {
		assert.deepEqual(parseSetupModeArg("Manual"), { kind: "set", mode: "manual" });
	});

	it("unknown args are invalid, never guessed into a write", () => {
		for (const args of ["bogus", "pub", "share"]) {
			const d = parseSetupModeArg(args);
			assert.equal(d.kind, "invalid");
			if (d.kind === "invalid") assert.equal(d.arg, args.trim().toLowerCase());
		}
	});
});

describe("pickSetupMode (#21, ui.select seam)", () => {
	it("offers three modes; organization option carries the resolved org name", async () => {
		const ui = fakeSelect((_t, options) => options[1].label);
		const r = await pickSetupMode(ui as any, ORG);
		assert.equal(ui.calls.length, 1);
		assert.equal(ui.calls[0].options.length, 3);
		assert.match(ui.calls[0].options[0].label, /Private sync/);
		assert.match(ui.calls[0].options[1].label, /Acme Corp/);
		assert.match(ui.calls[0].options[1].label, /Team sharing/);
		assert.match(ui.calls[0].options[2].label, /Manual/);
		assert.equal(r.mode, "organization");
		assert.equal(r.fallback, null);
	});

	it("each option label maps to its mode", async () => {
		for (const [idx, mode] of [
			[0, "private"],
			[1, "organization"],
			[2, "manual"],
		] as const) {
			const ui = fakeSelect((_t, options) => options[idx].label);
			const r = await pickSetupMode(ui as any, ORG);
			assert.equal(r.mode, mode, `option ${idx} should map to ${mode}`);
		}
	});

	it("dismissed selector (undefined) cancels: mode null, no fallback, no write", async () => {
		const ui = fakeSelect(() => undefined);
		const r = await pickSetupMode(ui as any, ORG);
		assert.equal(r.mode, null);
		assert.equal(r.fallback, null);
	});

	it("unrecognized label cancels instead of guessing a write", async () => {
		const ui = fakeSelect(() => "Some other label");
		const r = await pickSetupMode(ui as any, ORG);
		assert.equal(r.mode, null);
	});

	it("non-interactive (missing ui.select) falls back to manual with a notify reason", async () => {
		for (const ui of [undefined, {}, { ui: {} }, { select: "not a function" }]) {
			const r = await pickSetupMode(ui, ORG);
			                    assert.equal(r.mode, "manual"); // fall back to the current behavior; never silently enable auto-sync
			assert.match(r.fallback ?? "", /manual/);
			assert.match(r.fallback ?? "", /delta-setup private/);
		}
	});

	it("select() throwing falls back to manual and surfaces the error in fallback text", async () => {
		const ui = fakeSelect(() => {
			throw new Error("TUI gone");
		});
		const r = await pickSetupMode(ui as any, ORG);
		assert.equal(r.mode, "manual");
		assert.match(r.fallback ?? "", /TUI gone/);
	});

	it("null org renders a generic organization label", async () => {
		const ui = fakeSelect((_t, options) => options[1].label);
		const r = await pickSetupMode(ui as any, null);
		            assert.match(ui.calls[0].options[1].label, /current org/);
		assert.equal(r.mode, "organization");
	});
});

describe("renderProjectPolicy (#21 /delta-status line)", () => {
	it("unconfigured policy states the manual default and hints at /delta-setup", () => {
		const s = renderProjectPolicy({ mode: "manual", configured: false });
		assert.match(s, /manual/);
		            assert.match(s, /unconfigured/);
		assert.match(s, /delta-setup/);
	});

	it("manual policy reports manual-only sync", () => {
		assert.match(renderProjectPolicy({ mode: "manual", configured: true }), /manual/);
	});

	it("auto policies report auto-connect and the target org", () => {
		assert.match(renderProjectPolicy({ mode: "private", organization: "org_01", configured: true }), /private/);
		assert.match(renderProjectPolicy({ mode: "private", organization: "org_01", configured: true }), /org_01/);
		assert.match(renderProjectPolicy({ mode: "organization", organization: "org_01", configured: true }), /organization/);
		assert.match(renderProjectPolicy({ mode: "organization", organization: "org_01", configured: true }), /org_01/);
	});

	it("problem policy surfaces the problem text", () => {
		        const s = renderProjectPolicy({ mode: "manual", configured: false, problem: "Not valid JSON (x)" });
		        assert.match(s, /unusable/);
		        assert.match(s, /Not valid JSON/);
	});
});
