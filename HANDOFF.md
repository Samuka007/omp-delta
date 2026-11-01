# HANDOFF — omp-delta state & next steps

Continuation notes from the thread that built this repo. Read this before changing code.

## Status

MVP extension (`omp-delta.ts`) is written, deployed to `~/.omp/agent/extensions/omp-delta.ts`, and partially verified. Not yet proven end-to-end inside omp.

## Environment facts (Windows)

- delta CLI 0.23.0 at `%LOCALAPPDATA%\Programs\Delta\bin\delta.exe`, signed in as Samuka007
- Org: ALLE AI `org_01m68bjf0g8a2snb0p47zj9xrs` (`delta org current`)
- omp 18.14.1 at `%LOCALAPPDATA%\omp\omp.exe` (installed manually; not on PATH of stale shells)
- gh CLI authenticated as Samuka007; jq at `~/.local/bin/jq.exe`
- User runs omp inside Orca-hosted terminals; extensions live flat in `~/.omp/agent/extensions/*.ts`

## Verified contract-v8 shapes (from live CLI runs — docs were incomplete)

- `--json` is a GLOBAL flag: `delta --json thread create ...`
- `thread create --visibility <v>` REQUIRES `--organization <org id>`; idempotent per `--source`+`--external-id`
- Append receipts: `{"operation":"appended","thread_id":...,"sequence":N,"next_sequence":N+1,"duplicate":false,"disposition":"recorded","turn_sequence":3,"version":"..."}`
  → the status field is **`disposition`** (recorded/duplicate/…), NOT `status` — `collectReceipts` must read it
- `thread status` → `.url` is the real join link (`https://delta.dev/join/thread_01…`, a share id). It is **not** `delta.dev/join/<thread_create id>`; `.producer.next_sequence` and `.turn.status` also useful
- `worktree create` REQUIRES the repo to have a Git remote (content is backfilled from it)
- mount ready line (single JSON): `{"operation":"mounted","reference":"...","directory":"..."}`
- Private thread + `--organization` uploads without explicit `thread share`

## Already fixed (commits)

- `eb2551a` global `--json` on every CLI invocation
- `1f84692` `--organization` auto-resolve + git-repo precheck

## Next actions, in order

1. `collectReceipts`: read `disposition` (fallback `status`); treat `duplicate:true` as ack
2. Show join link from `thread status` `.url` (persist as `state.url`); fix `/delta-status` link
3. Extend precheck: git remote must exist (not just a git repo)
4. `flush()`: treat spawn-failure/timeout (code -1) as retryable
5. Compact outbox after `lastAcked` advances (rewrite file with pending only)
6. `input` handler: skip recording submissions starting with `/` (slash commands are not messages)
7. E2E test: `omp --mode print -p "/delta-share"` in a real repo → check `~/.omp/agent/omp-delta/<sid>/state.json`, `outbox.jsonl`, and thread content in browser
8. Subagents → Delta child threads: `thread create --parent … --shared-worktrees|--isolated-worktrees`, `thread merge --into`; omp gives `ctx.agent = {kind:"main"|"sub", id, name, depth, parentId}`

## Runtime state location

`~/.omp/agent/omp-delta/<sessionId>/` — `state.json` (threadId, worktreeRef, seq, lastAcked, url?) + `outbox.jsonl`
