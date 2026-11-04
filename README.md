# omp-delta

An **oh-my-pi (omp)** extension that records a live omp session into a **Delta** thread, through the Delta CLI (plugin contract v8).

A community implementation of the [officially documented plugin contract](https://delta.dev/docs/cli/build-a-plugin) — the official Pi plugin is still "In Progress" on Delta's roadmap.

## What it does

- `/delta-share` — connect the current omp session to a Delta thread: create thread (idempotent) → attach + mount the worktree (including uncommitted changes) → share privately or with your organization (interactive visibility picker when run without arguments; `/delta-share private` to skip it — confirmation first either way)
- Records automatically from then on: user input, turn start/stop, assistant replies, tool calls — watchable live in the Delta app or browser
- Teammate comments in Delta flow back into omp context as follow-ups (never auto-trigger a turn)
- Someone takes over the thread → the local turn stops (`superseded` / `fenced`)
- Zero local state: thread identity lives in the omp session record + DeltaDB, the delivery cursor is the server's `next_sequence`; undelivered records queue in memory and retryable failures back off automatically
- `/delta-status`, `/delta-detach`, `/delta-retry` for connection management; `/delta-org list|current` for read-only org inspection (no switching)

## Install

```sh
omp plugin install 'github:Samuka007/omp-delta'
# restart omp, then run /delta-share inside a git repository
```

For local development (symlink, edits stay live):

```sh
omp plugin link ./omp-delta
```

Requires Delta desktop ≥ 0.23.0, signed in (`delta auth status`), and omp running inside a git repository **with a remote**.

> **Windows note:** `omp plugin install` (18.x) can fail silently on Windows ("package.json not found") while leaving nothing in `~/.omp/plugins/node_modules/`. Workaround — install via npm into the plugins root directly:
>
> ```sh
> cd ~/.omp/plugins && npm install 'github:Samuka007/omp-delta'
> ```
>
> The result is identical to a successful `omp plugin install` (`omp plugin list` shows `omp-delta@0.2.0`).

## Known limits (MVP)

- Main session only; omp subagents → Delta child threads not yet implemented
- No history backfill: recording starts when you connect; while a reconnect is in flight, events queue in memory and are renumbered onto the server cursor when it arrives (dropped only when no session binding exists)
- Thinking blocks and streaming deltas are not recorded; tool cards carry name/status only
- Sharing targets the currently selected organization (`delta org current`)
- Switching sessions (`/new`) stops sync — run `/delta-share` again in the new session
- After an omp restart, reconnect happens automatically from the session binding marker (via `/delta-retry` if the background attempt gives up)

## Roadmap

Tracked as GitHub issues — the single source of truth:
<https://github.com/Samuka007/omp-delta/issues> (OD-1…OD-8; OD-1/OD-2 are P0).
Done items live in git history and `docs/contract-notes.md`.

## Verified so far

- delta CLI 0.23.0 reports contract `{min:8,max:8}` — matches this plugin
- Full CLI chain exercised end-to-end: contract → org current → thread create → worktree create/attach/mount → append (receipts) → thread status
- Live E2E (omp TUI in a herdr pane): private thread connect, user + assistant messages both render, server acknowledges all records
