# omp-delta

An **oh-my-pi (omp)** extension that records a live omp session into a **Delta** thread, through the Delta CLI (plugin contract v8).

A community implementation of the [officially documented plugin contract](https://delta.dev/docs/cli/build-a-plugin) — the official Pi plugin is still "In Progress" on Delta's roadmap.

## What it does

- `/delta-share` — connect the current omp session to a Delta thread: create thread (idempotent) → attach + mount the worktree (including uncommitted changes) → share with your organization (confirmation first)
- Records automatically from then on: user input, turn start/stop, assistant replies, tool calls — watchable live in the Delta app or browser
- Teammate comments in Delta flow back into omp context as follow-ups (never auto-trigger a turn)
- If someone takes over the thread, the local turn stops (`superseded` / `fenced`)
- Local outbox keeps every record until Delta confirms it; retryable failures back off automatically
- `/delta-status`, `/delta-detach`, `/delta-retry` for connection management

## Install

```sh
cp omp-delta.ts ~/.omp/agent/extensions/omp-delta.ts
# restart omp, then run /delta-share inside a git repository
```

Requires Delta desktop ≥ 0.23.0, signed in (`delta auth status`), and omp running inside a git repository **with a remote**.

## Known limits (MVP)

- Main session only; omp subagents → Delta child threads not yet implemented
- No history backfill: recording starts when you connect
- Thinking blocks and streaming deltas are not recorded; tool cards carry name/status only
- Sharing targets the currently selected organization (`delta org current`)
- Switching sessions (`/new`) stops sync — run `/delta-share` again in the new session

## Roadmap (next)

- [x] Parse receipt `disposition` field — real shape: `{"operation":"appended","sequence":n,"disposition":"recorded",...}`
- [x] Use `thread status` → `.url` for the join link (share id ≠ thread id)
- [x] Precheck git repo **and remote** before connecting (`worktree create` requires both)
- [x] Treat spawn failure/timeout as retryable; compact outbox after ack
- [ ] Subagents → Delta child threads (`thread create --parent`, shared/isolated worktrees, `thread merge`)
- [ ] History backfill; thinking blocks → `agent_thought`

## Verified so far

- delta CLI 0.23.0 reports contract `{min:8,max:8}` — matches this plugin
- Full CLI chain exercised end-to-end: contract → org current → thread create → worktree create/attach/mount → append (receipts) → thread status
