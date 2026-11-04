import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	formatOrgList,
	isUnauthenticatedText,
	parseOrgCurrentFromText,
	parseOrgListFromText,
} from "../omp-delta.ts";

// 形状实采自本机 `delta --json org list` / `delta --json org current`(Windows,delta 0.23.0)
const LIVE_LIST = {
	operation: "listed",
	organizations: [
		{ id: "org_01km17je98bvcpaca0d07axwdq", name: "Samuka007's Organization", current: false },
		{ id: "org_01m68bjf0g8a2snb0p47zj9xrs", name: "ALLE AI", current: true },
	],
};
const LIVE_CURRENT = {
	operation: "current",
	organization: { id: "org_01m68bjf0g8a2snb0p47zj9xrs", name: "ALLE AI", current: true },
};

describe("parseOrgListFromText", () => {
	it("parses the live org list shape and preserves CLI order", () => {
		const orgs = parseOrgListFromText(JSON.stringify(LIVE_LIST) + "\n");
		assert.deepEqual(orgs, [
			{ id: "org_01km17je98bvcpaca0d07axwdq", name: "Samuka007's Organization", current: false },
			{ id: "org_01m68bjf0g8a2snb0p47zj9xrs", name: "ALLE AI", current: true },
		]);
	});

	it("falls back to whole-text parse for pretty-printed output", () => {
		const orgs = parseOrgListFromText(JSON.stringify(LIVE_LIST, null, 2));
		assert.equal(orgs?.length, 2);
		assert.equal(orgs?.find((o) => o.current)?.id, "org_01m68bjf0g8a2snb0p47zj9xrs");
	});

	it("returns [] for a signed-in account with no orgs, null without any org payload", () => {
		assert.deepEqual(parseOrgListFromText('{"operation":"listed","organizations":[]}'), []);
		assert.equal(parseOrgListFromText("delta: not signed in"), null);
		assert.equal(parseOrgListFromText('{"operation":"appended","sequence":1,"disposition":"recorded"}'), null);
	});

	it("accepts a top-level bare array variant", () => {
		const orgs = parseOrgListFromText('[{"id":"org_x","name":"X","current":true}]');
		assert.deepEqual(orgs, [{ id: "org_x", name: "X", current: true }]);
	});
});

describe("parseOrgCurrentFromText", () => {
	it("parses the live org current shape", () => {
		const org = parseOrgCurrentFromText(JSON.stringify(LIVE_CURRENT) + "\n");
		assert.deepEqual(org, { id: "org_01m68bjf0g8a2snb0p47zj9xrs", name: "ALLE AI" });
	});

	it("falls back to the id when name is absent", () => {
		assert.deepEqual(parseOrgCurrentFromText('{"organization":{"id":"org_x"}}'), { id: "org_x", name: "org_x" });
	});

	it("returns null without a resolvable organization", () => {
		assert.equal(parseOrgCurrentFromText("delta: not signed in"), null);
		assert.equal(parseOrgCurrentFromText('{"operation":"current"}'), null);
	});
});

describe("isUnauthenticatedText", () => {
	it("matches the signatures observed in the delta binary (json or plain text, any case)", () => {
		for (const text of [
			"delta: not signed in",
			'{"error":{"message":"authentication required","retryable":false}}',
			"ERROR: Not Signed In — run delta auth login",
			"please sign in to continue",
		]) {
			assert.equal(isUnauthenticatedText(text), true, text);
		}
	});

	it("does not match ordinary failures", () => {
		for (const text of ["", "rate limited", "the history is unavailable: NotFound", "bad envelope"]) {
			assert.equal(isUnauthenticatedText(text), false, text || "<empty>");
		}
	});
});

describe("formatOrgList", () => {
	it("marks exactly the current org with * and the share hint, others plain (id+name)", () => {
		const text = formatOrgList(parseOrgListFromText(JSON.stringify(LIVE_LIST))!);
		const lines = text.split("\n");
		assert.equal(lines.length, 2);
		assert.equal(lines[0], "  Samuka007's Organization (org_01km17je98bvcpaca0d07axwdq)");
		assert.equal(lines[1], "* ALLE AI (org_01m68bjf0g8a2snb0p47zj9xrs) ← 当前:共享将使用此组织");
	});

	it("renders a friendly line for an empty org list", () => {
		assert.equal(formatOrgList([]), "未加入任何组织。");
	});

	it("marks nothing when no org carries current:true", () => {
		const text = formatOrgList([
			{ id: "org_a", name: "A", current: false },
			{ id: "org_b", name: "B", current: false },
		]);
		assert.ok(!text.includes("*"));
	});
});
