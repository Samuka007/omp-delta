import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	PLUGIN_REINSTALL_CMD,
	parseUpdateResult,
	pluginOutdatedFromContract,
	renderUpdateReport,
} from "../omp-delta.ts";

// Shapes captured live from docs/contract-notes.md (delta 0.23.0, up-to-date; progress frames carry no status)
const CHECK_UP_TO_DATE_JSONL =
	'{"schema_version":1,"type":"progress","phase":"checking"}\n' +
	'{"schema_version":1,"status":"up_to_date","current_version":"0.23.0","latest_version":"0.23.0","channel":"stable","can_install":true,"message":"Delta 0.23.0 is up to date."}\n';

const updateAvailable = () => ({
	status: "update_available",
	currentVersion: "0.23.0",
	latestVersion: "0.24.0",
	channel: "stable",
	canInstall: true,
	message: "",
	restartRequired: true,
});

describe("PLUGIN_REINSTALL_CMD", () => {
	it("is the exact reinstall command from issue #20", () => {
		assert.equal(PLUGIN_REINSTALL_CMD, "omp plugin install 'github:Samuka007/omp-delta'");
	});
});

describe("parseUpdateResult", () => {
	it("parses the live delta update --check --json shape, ignoring the progress frame", () => {
		const p = parseUpdateResult(CHECK_UP_TO_DATE_JSONL);
		assert.ok(p, "must find the status-bearing frame");
		assert.equal(p!.status, "up_to_date");
		assert.equal(p!.currentVersion, "0.23.0");
		assert.equal(p!.latestVersion, "0.23.0");
		assert.equal(p!.channel, "stable");
		assert.equal(p!.canInstall, true);
		assert.equal(p!.restartRequired, false);
	});

	it("treats any non-up_to_date status as restart-required and keeps versions", () => {
		const p = parseUpdateResult(
			'{"type":"progress","phase":"checking"}\n' +
				'{"status":"installed","current_version":"0.24.0","latest_version":"0.24.0","channel":"stable","can_install":true,"message":"Updated."}\n',
		);
		assert.ok(p);
		assert.equal(p!.status, "installed");
		assert.equal(p!.restartRequired, true);
		assert.equal(p!.latestVersion, "0.24.0");
	});

	it("prefers the last status-bearing frame when several appear", () => {
		const p = parseUpdateResult(
			'{"status":"checking","current_version":"0.23.0"}\n' +
				'{"status":"up_to_date","current_version":"0.23.0","latest_version":"0.23.0","channel":"stable"}\n',
		);
		assert.equal(p!.status, "up_to_date");
	});

	it("returns null for human-only output or empty text", () => {
		assert.equal(parseUpdateResult("Checking for Delta updates...\nDelta 0.23.0 is up to date.\n"), null);
		assert.equal(parseUpdateResult(""), null);
	});
});

describe("pluginOutdatedFromContract", () => {
	it("flags the plugin as outdated when contract min exceeds the plugin's supported max", () => {
		        // the observed shape is {operation:"contract",min,max}; the function only reads min, so only the relevant fields are kept here
		assert.equal(pluginOutdatedFromContract({ min: 9 }), true);
	});

	it("passes when the plugin supports the negotiated range", () => {
		assert.equal(pluginOutdatedFromContract({ min: 8 }), false);
		assert.equal(pluginOutdatedFromContract({ min: 3 }), false);
	});

	it("gives no conclusion without a usable min", () => {
		assert.equal(pluginOutdatedFromContract(null), false);
		assert.equal(pluginOutdatedFromContract(undefined), false);
		assert.equal(pluginOutdatedFromContract({}), false);
	});
});

describe("renderUpdateReport", () => {
	it("check-only up-to-date: reports the version, states nothing was installed, and carries the reinstall hint", () => {
		const out = renderUpdateReport({ mode: "check", result: parseUpdateResult(CHECK_UP_TO_DATE_JSONL), contractMin: 8 });
		assert.match(out, /0\.23\.0/);
		        assert.match(out, /nothing was installed/);
		        assert.ok(out.includes(PLUGIN_REINSTALL_CMD), "must contain the exact reinstall command");
			});

			it("check with a newer version available: says it did not install and mentions the restart", () => {
				const out = renderUpdateReport({ mode: "check", result: updateAvailable(), contractMin: 8 });
				assert.match(out, /0\.24\.0/);
				assert.match(out, /nothing was installed/);
				assert.match(out, /restart/i);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});

	it("update flow: reports the result plus restart guidance and the reinstall hint", () => {
		const out = renderUpdateReport({
			mode: "update",
			result: { ...updateAvailable(), status: "installed", message: "Delta updated to 0.24.0." },
			contractMin: 8,
		});
		assert.match(out, /0\.24\.0/);
		assert.match(out, /restart/i);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});

	it("update when already current: no restart line, hint still shown", () => {
		const out = renderUpdateReport({ mode: "update", result: parseUpdateResult(CHECK_UP_TO_DATE_JSONL), contractMin: 8 });
		assert.match(out, /already up to date/);
		assert.doesNotMatch(out, /Restart note/);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});

	it("contract min above plugin max: prints the definitive outdated line with the command", () => {
		const out = renderUpdateReport({ mode: "check", result: parseUpdateResult(CHECK_UP_TO_DATE_JSONL), contractMin: 9 });
		assert.match(out, /outdated/);
		assert.match(out, /must reinstall/);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});

	it("CLI failure: surfaces the raw error and still shows the hint", () => {
		const out = renderUpdateReport({ mode: "update", result: null, errorText: "exit 1: boom", contractMin: 8 });
		assert.match(out, /boom/);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});

	it("unknown contract: no definitive claim, but the constant hint remains", () => {
		const out = renderUpdateReport({ mode: "check", result: parseUpdateResult(CHECK_UP_TO_DATE_JSONL), contractMin: null });
		assert.doesNotMatch(out, /must reinstall/);
		assert.ok(out.includes(PLUGIN_REINSTALL_CMD));
	});
});
