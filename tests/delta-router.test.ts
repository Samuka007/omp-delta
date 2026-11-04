import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { parseDeltaRouter, renderDeltaMenu } from "../omp-delta.ts";

// #15:/delta 路由决策(纯接缝)。路由与平铺命令共享同一 handler 对象,由注册助手保证;
// 这里覆盖决策矩阵:menu/route/unimplemented/unknown + 大小写/空白/剩余参数语义。

describe("parseDeltaRouter", () => {
	it("no args (empty or whitespace) → menu", () => {
		assert.deepEqual(parseDeltaRouter(""), { kind: "menu" });
		assert.deepEqual(parseDeltaRouter("   "), { kind: "menu" });
	});

	it("routes each implemented subcommand to its flat command", () => {
		assert.deepEqual(parseDeltaRouter("share"), { kind: "route", command: "delta-share", args: "" });
		assert.deepEqual(parseDeltaRouter("status"), { kind: "route", command: "delta-status", args: "" });
		assert.deepEqual(parseDeltaRouter("detach"), { kind: "route", command: "delta-detach", args: "" });
		assert.deepEqual(parseDeltaRouter("retry"), { kind: "route", command: "delta-retry", args: "" });
	});

	it("preserves remaining args verbatim for the routed handler", () => {
		assert.deepEqual(parseDeltaRouter("share private"), {
			kind: "route",
			command: "delta-share",
			args: "private",
		});
		assert.deepEqual(parseDeltaRouter("  share    private  "), {
			kind: "route",
			command: "delta-share",
			args: "private",
		});
		assert.deepEqual(parseDeltaRouter("retry now"), { kind: "route", command: "delta-retry", args: "now" });
	});

	it("is case-insensitive on the subcommand", () => {
		assert.deepEqual(parseDeltaRouter("SHARE"), { kind: "route", command: "delta-share", args: "" });
		assert.deepEqual(parseDeltaRouter("Retry fast"), { kind: "route", command: "delta-retry", args: "fast" });
	});

	it("join is known but unimplemented (#17)", () => {
		assert.deepEqual(parseDeltaRouter("join"), { kind: "unimplemented", command: "join" });
		assert.deepEqual(parseDeltaRouter("JOIN delta://thread/x"), { kind: "unimplemented", command: "join" });
	});

	it("unknown subcommand reports the lowered name", () => {
		assert.deepEqual(parseDeltaRouter("bogus"), { kind: "unknown", command: "bogus" });
		assert.deepEqual(parseDeltaRouter("Bogus x"), { kind: "unknown", command: "bogus" });
		assert.notDeepEqual(parseDeltaRouter("bogus"), parseDeltaRouter(""));
	});

	it("org/update are not routed (independent slash spellings)", () => {
		assert.deepEqual(parseDeltaRouter("org"), { kind: "unknown", command: "org" });
		assert.deepEqual(parseDeltaRouter("update"), { kind: "unknown", command: "update" });
	});
});

describe("renderDeltaMenu", () => {
	it("lists every routed subcommand, the unimplemented join, and the flat aliases", () => {
		const menu = renderDeltaMenu();
		for (const expected of ["share", "status", "retry", "detach", "join", "/delta-share", "/delta-status", "/delta-detach", "/delta-retry", "/delta-org", "/delta-update", "#17"]) {
			assert.ok(menu.includes(expected), "menu should mention " + expected);
		}
	});

	it("marks join as not yet implemented, not as an error path", () => {
		assert.ok(/尚未实现/.test(renderDeltaMenu()));
	});
});
