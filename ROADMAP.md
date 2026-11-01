# Roadmap

Iteration order = list order. **Done** items live in git history and `docs/contract-notes.md`;
new discoveries land here as new tickets instead of chat messages.

## OD-1 [P0] lastAcked convergence after partial acks
Server accepted every record (`next_sequence` advanced) but client `lastAcked` stalled
(observed 3 vs 6), freezing compaction. Suspects: receipt capture across the append's stdout,
stale `bySeq` in `contiguousAck`, follow-up timer arming.
**Done when:** after 3 consecutive driven turns, `lastAcked === seq` within ~5s and outbox is
empty; verified twice in a row.

## OD-2 [P0] state.json save robustness on Windows
Atomic rename fails under file locks (AV/indexer/concurrent reader) and leaves **no** state.json.
**Done when:** bounded retry + copy-fallback keeps state.json present and parseable under a
concurrent reader loop; failures logged once.

## OD-3 [P1] auto-resume reconnect, verified end-to-end
omp autoResume restores the last session (same external-id) on relaunch. The reconnect path
(idempotent thread create + attach-tolerance + mount) exists — verify it fully, and decide the
archived-thread edge (actionable error vs auto-fork).
**Done when:** relaunch in the same cwd reconnects to the same thread; sequences continue; no
duplicate thread.

## OD-4 [P1] subagents → Delta child threads
`ctx.agent` gives kind/id/name/parentId per session; child threads via `thread create --parent`
with `--shared-worktrees`/`--isolated-worktrees`; isolated changes merge via `delta thread merge`.
**Done when:** a `/tan` subagent task appears as a rendered child thread in Delta, and an
isolated child's changes merge back via the CLI.

## OD-5 [P2] history backfill on share (initial export)
Share-time export of prior conversation so joiners see pre-share context.
**Done when:** sharing a session with prior history renders the full transcript for joiners.

## OD-6 [P2] thinking blocks → `agent_thought`
Currently dropped. **Done when:** a thinking model's session shows thought blocks in Delta.

## OD-7 [P2] E2E harness hardening (scripts only — never the plugin)
Wait-for-idle before prompting (boot race), Windows-style paths for herdr workspace create,
deterministic test workspace, test-thread cleanup. The boot-race workaround from debugging
belongs HERE, not in plugin code.

## OD-8 [P3] tool card enrichment + streaming
Diff blocks, rawInput/rawOutput, status transitions; streaming text (full-prefix resend) as
stretch.
