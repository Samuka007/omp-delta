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

	it("routes join with the link/id verbatim as remaining args (#17)", () => {
		assert.deepEqual(parseDeltaRouter("join"), { kind: "route", command: "delta-join", args: "" });
		assert.deepEqual(parseDeltaRouter("JOIN https://delta.dev/join/thread_01x"), {
			kind: "route",
			command: "delta-join",
			args: "https://delta.dev/join/thread_01x",
		});
		assert.deepEqual(parseDeltaRouter("join delta://join/thread_01x"), {
			kind: "route",
			command: "delta-join",
			args: "delta://join/thread_01x",
		});
		assert.deepEqual(parseDeltaRouter("join thread_01abc"), { kind: "route", command: "delta-join", args: "thread_01abc" });
		assert.deepEqual(parseDeltaRouter("  join    ksQQabcDEF-_x  "), {
			kind: "route",
			command: "delta-join",
			args: "ksQQabcDEF-_x",
		});
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
	it("lists every routed subcommand and the flat aliases", () => {
		const menu = renderDeltaMenu();
		for (const expected of ["share", "join", "status", "retry", "detach", "/delta-share", "/delta-join", "/delta-status", "/delta-detach", "/delta-retry", "/delta-org", "/delta-update"]) {
			assert.ok(menu.includes(expected), "menu should mention " + expected);
		}
	});

	it("no longer marks join as unimplemented (#17 shipped)", () => {
		const menu = renderDeltaMenu();
		assert.ok(!menu.includes("尚未实现"));
		assert.ok(!menu.includes("#17"));
	});
});
