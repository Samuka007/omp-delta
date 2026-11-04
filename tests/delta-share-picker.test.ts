import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { pickShareVisibility } from "../omp-delta.ts";

const ORG = { id: "org_01", name: "Acme Corp" };

/** 模拟 ctx.ui.select:记录每次调用的参数,reply 用所选 label(真实宿主返回选项一原样 label)或 undefined(Esc),抛错则模拟宿主异常。 */
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
		const ui = fakeSelect((_t, options) => options[0].label); // 用户选第一项 Private
		const r = await pickShareVisibility("", ui as any, ORG);
		assert.equal(ui.calls.length, 1); // 只弹一次选择器
		assert.match(ui.calls[0].title, /可见性/);
		assert.equal(ui.calls[0].options.length, 2);
		assert.match(ui.calls[0].options[0].label, /Private/);
		assert.equal(r.mode, "private");
		assert.equal(r.fallback, null);
	});

	it("org option carries the current org name resolved by resolveOrg; picking it yields organization", async () => {
		const ui = fakeSelect((_t, options) => options[1].label);
		const r = await pickShareVisibility(undefined, ui as any, ORG);
		assert.match(ui.calls[0].options[1].label, /Acme Corp/); // 组织名进选项(#16 要求按名展示)
		assert.equal(r.mode, "organization");
		assert.equal(r.fallback, null);
	});

	it("explicit 'private' arg skips the picker entirely (back-compat, old normalization)", async () => {
		const ui = fakeSelect(() => {
			throw new Error("选择器不得在显式参数下打开");
		});
		for (const args of ["private", "  PRIVATE ", "priv"]) {
			const r = await pickShareVisibility(args, ui as any, ORG);
			assert.equal(r.mode, "private");
			assert.equal(r.fallback, null);
		}
		assert.equal(ui.calls.length, 0); // 三次调用都未触碰选择器
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