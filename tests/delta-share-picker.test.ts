import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { pickShareVisibility } from "../omp-delta.ts";

const ORG = { id: "org_01", name: "Acme Corp" };

/** Fakes ctx.ui.select: records every call's arguments; reply returns the chosen label (the real host returns option one's label verbatim) or undefined (Esc); throwing simulates a host error. */
function fakeSelect(reply: (title: string, options: Array<{ label: string; description?: string }>) => string | undefined) {
	const calls: Array<{ title: string; options: Array<{ label: string; description?: string }> }> = [];
	const select = async (title: string, options: Array<{ label: string; description?: string }>) => {
		calls.push({ title, options });
		return reply(title, options);
	};
	return { select, calls };
}

describe("pickShareVisibility", () => {
	it("no-arg share shows the selector: Private / org, and a Private pick flows through", async () => {
		            const ui = fakeSelect((_t, options) => options[0].label); // the user picks the first option, Private
		const r = await pickShareVisibility("", ui as any, ORG);
		            assert.equal(ui.calls.length, 1); // the picker opens only once
		            assert.match(ui.calls[0].title, /visibility/i);
		assert.equal(ui.calls[0].options.length, 2);
		assert.match(ui.calls[0].options[0].label, /Private/);
		assert.equal(r.mode, "private");
		assert.equal(r.fallback, null);
	});

	it("org option carries the current org name resolved by resolveOrg; picking it yields organization", async () => {
		const ui = fakeSelect((_t, options) => options[1].label);
		const r = await pickShareVisibility(undefined, ui as any, ORG);
		            assert.match(ui.calls[0].options[1].label, /Acme Corp/); // the org name appears in the option (#16 requires showing it by name)
		assert.equal(r.mode, "organization");
		assert.equal(r.fallback, null);
	});

	it("explicit 'private' arg skips the picker entirely (back-compat, old normalization)", async () => {
		const ui = fakeSelect(() => {
			                throw new Error("the picker must not open for explicit arguments");
		});
		for (const args of ["private", "  PRIVATE ", "priv"]) {
			const r = await pickShareVisibility(args, ui as any, ORG);
			assert.equal(r.mode, "private");
			assert.equal(r.fallback, null);
		}
		            assert.equal(ui.calls.length, 0); // none of the three calls touched the picker
	});

	it("explicit non-private arg keeps old default (organization) without the picker", async () => {
		const ui = fakeSelect(() => undefined);
		const r = await pickShareVisibility("org", ui as any, ORG);
		assert.equal(ui.calls.length, 0);
		assert.equal(r.mode, "organization");
	});

	it("missing ui.select (non-interactive) falls back to organization default with a notify reason", async () => {
		for (const ui of [undefined, {}, { ui: {} }, { select: "not a function" }]) {
			const r = await pickShareVisibility("", ui, ORG);
			assert.equal(r.mode, "organization");
			assert.ok(r.fallback && r.fallback.includes("Acme Corp"), String(r.fallback));
		}
	});

	it("select() throwing falls back to organization default instead of crashing", async () => {
		const ui = fakeSelect(() => {
			throw new Error("TUI gone");
		});
		const r = await pickShareVisibility("", ui as any, ORG);
		assert.equal(r.mode, "organization");
		assert.match(r.fallback ?? "", /TUI gone/);
	});

	it("dismissed selector (undefined) cancels the share: mode null, no fallback", async () => {
		const ui = fakeSelect(() => undefined);
		const r = await pickShareVisibility("", ui as any, ORG);
		assert.equal(r.mode, null);
		assert.equal(r.fallback, null);
	});
});