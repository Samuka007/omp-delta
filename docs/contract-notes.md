# Delta CLI contract v8 — notes from live testing

Field-level notes recorded while building this plugin against `delta` 0.23.0.
The [plugin guide](https://delta.dev/docs/cli/build-a-plugin) gives the canonical
flows; this page records details it does not spell out.

## CLI conventions

- `--json` is a **global** flag and must precede the subcommand: `delta --json thread create …`. Without it, commands print human-readable text and JSON-line parsers silently starve.
- All commands this plugin uses accept it: `thread contract/create/append/watch/status/stop`, `worktree create/attach/mount/list`, `org current`, `org list`.

## Orgs (/delta-org, #19)

- `org list --json` → `{"operation":"listed","organizations":[{"id":"org_01…","name":"ALLE AI","current":true}, …]}` — array order matches the pretty display, exactly one entry carries `current:true`; empty array = signed in but no orgs.
- `org current --json` → `{"operation":"current","organization":{"id":"…","name":"…","current":true}}`. Pretty output marks the current org with a leading `*`.
- Unauthenticated signatures (observed in the delta.exe binary's error strings): `not signed in` / `authentication required` — the plugin matches these on stdout+stderr and shows an actionable `delta auth login` hint instead of a raw parse error. `/delta-org` is read-only: no switching, no invite management.

## Threads

- `thread create --visibility <private|organization>` **requires** `--organization <org id>` (resolve with `delta org current`).
- `thread create` is idempotent per `--source` + `--external-id` — retries and crash recovery are safe.
- Append receipts arrive as one JSON object per line on stdout. The ack status field is **`disposition`** (`recorded` / `duplicate` / `fenced`), plus a `duplicate` boolean — there is no `status` field:

  ```json
  {"operation":"appended","thread_id":"…","sequence":3,"next_sequence":4,
   "duplicate":false,"disposition":"recorded","turn_sequence":3,"version":"…"}
  ```

- A thread created `private` with `--organization` uploads without an explicit `thread share`.

## Join links

`thread status` → `.url` is the real join URL (`https://delta.dev/join/thread_01…`).
The share id (`thread_01…`) is **not** the id returned by `thread create` — never
construct join links from the create id. `status` also exposes
`.producer.next_sequence` and `.turn.status`, both handy for status displays.

#17 probing (delta 0.23.0, live, 2026-11-05) — what `/delta-join` builds on:

- `thread status <arg>` resolves the **same thread** for all of: bare thread id,
  bare share id (`thread_01…`), `https://delta.dev/join/<share-id>`, and
  `delta://join/<share-id>`. The CLI owns link→thread resolution; the plugin never
  parses links itself. All forms **require** `--source/--external-id` (clap rejects
  the command without them).
- Producer rows materialize on first `status`: a never-appended producer already
  shows `producer.next_sequence: 1`. Cursors are per-producer — joining an existing
  thread does NOT replay the other producer's history; you follow from now.
- `.access` is `{"state":"granted","is_owner":…}`; a thread not visible from this
  account fails instead:
  - foreign/absent share id → exit 1, `code:"not_visible"`, "shared thread is absent
    or not visible" (`retryable:false`). Unlike the share path's self-inflicted
    provisioning window, join surfaces this as a hard account/visibility error
    (hint text still mentions the just-created provisioning caveat).
  - foreign/malformed thread id → exit 1, `code:"invalid_reference"`, "expected a
    supported Delta link, share ID, or exact thread ID in this account".
- `/delta-join` deliberately sends no title record: the joined thread's title
  belongs to its creator (`titleSent` is preset to suppress the input handler's
  first-record title).

## /move (#22) — source-level research (pi-coding-agent 18.16.0, not live-verified)

Findings from reading the installed bundle (`node_modules/@oh-my-pi/pi-coding-agent/dist/cli.js`,
TUI `handleMoveCommand` → `session.moveSession` → `sessionManager.moveTo` → `applyCwdChange`):

- **What /move does**: refuses while streaming; interactive dir picker without an arg.
  Then: flush settings → `sessionManager.moveTo(newDir)` → `applyCwdChange(newDir)`.
- **What moves**: the session JSONL file and the session's artifacts directory are
  **renamed** into the new project's session storage (same basename; EXDEV falls back to
  copy; the header's `cwd` field is rewritten and persisted).
- **All-or-nothing + rollback**: artifacts rename rolls back if the session-file rename
  fails and vice versa ("Failed to move artifacts and rollback"). If the post-move
  `applyCwdChange` fails, the TUI rolls the move back (`rollbackMove`) and re-aligns the
  workspace to the restored cwd; if even that fails it escalates to a full `shutdown()`.
- **Lease guards**: moving onto a session another live omp process writes is refused
  ("another live omp process writes the session there. Nothing was moved.").
- **Extension lifecycle**: extensions are NOT re-instantiated and NO extension event fires
  (`session_start/switch/branch/tree/shutdown` is the whole lifecycle vocabulary).
  `applyCwdChange` = `chdir` + settings rescope for the new cwd (`reloadForCwd`) +
  title-system-prompt and skills/commands refresh. An extension's Runtime survives /move
  with the same session id; later command handlers see the NEW cwd via `ctx.cwd`.
- **Manual-step boundary**: there is no agent-callable cwd change (checked: the `omp
  session` CLI covers machine handoff pack/unpack/moved/lease only). The extension prints
  the exact `/move <dir>` command; automation lands with upstream can1357/oh-my-pi#17441.
- **Consequence for omp-delta (#22)**: mount/watch children spawned before /move keep
  running. For the materialize-join flow this is exactly right — the mount child IS the
  materialized directory's sync, and watch is thread-scoped — so sync continues across
  /move with no remount. `rt.cwd` stays stale (old dir) until the next command handler
  refreshes it from `ctx.cwd`; only cosmetic (spawn cwd for delta CLI calls). Known
  limitation (documented, not handled): if watch drops BEFORE the user runs /move,
  `/delta-retry` re-attaches using the stale old cwd — the materialized mount child keeps
  running regardless, and post-/move retries adopt the materialized checkout via
  `--existing-checkout`.

#22 probing (delta 0.23.0, live):

- `delta --json worktree mount <ref> <new-dir>` (no `--existing-checkout`) materializes
  a shared checkout into a fresh directory and prints the same compact ready line as the
  adopt variant: `{"operation":"mounted","reference":"…","directory":"<abs dir>"}`.
  The process keeps running and syncs that directory (it becomes the mount child).
- The materialized directory registers under the thread's `status.worktrees.items[]`
  as `checkouts[].directory` (observed `state:"detached"` after the mount process ends),
  so restart-at-new-dir discovery matches by cwd exactly (and `marker.worktreeRef` is
  the fallback).
- `omp session` CLI actions are pack/unpack/moved/lease (machine handoff), NOT an
  in-process move — no automation path for the relocation step.

## Worktrees

- `worktree create` requires the repository to have a **Git remote**: Delta backfills blob content from it, and a fake/unreachable remote only fails later, when a client tries to materialize the checkout (`remote returned no git blobs …`).
- `worktree mount … --existing-checkout` prints one compact JSON line when ready:

  ```json
  {"operation":"mounted","reference":"…","directory":"…"}
  ```

- Ignored files and local git state (branches) are not shared.

## Rendering gates for externally recorded messages (undocumented)

Sequence numbers being accepted (`next_sequence` advances) does NOT mean records render.
Two independent requirements, discovered by controlled experiment:

1. **Live-activity marker**: each live record needs
   `_meta.delta = {work_origin: 'external_agent', interaction_surface: 'cli', activity_kind: 'user_message'|'agent_message'|'agent_thought'|'tool_call'|'edit'}`.
   Without it the record is stored but never rendered (delta-claude's `liveActivity()` marks
   every live row this way).
2. **Real turns**: the `running` state_update must carry `turnSequence` equal to its OWN
   sequence — that is what opens the turn; agent/tool records then reference that
   `turnSequence`. A running record without `turnSequence` is recorded as history, the turn
   never opens, and every record naming it is silently dropped (no item, no error).

Authoritative read-back: `thread watch` **without** `--source/--external-id` (those hide your
own records!) — count `"type":"item"` frames per kind.

## Update

- `delta update --check` only inspects release metadata (never installs); plain `delta update`
  installs, and **a running Delta app restarts to install it** (per `--help`).
- Unlike the other commands, `update` also accepts `--json` after the subcommand, but the
  global position (`delta --json update …`) works identically — `runDelta` uses the global one.
- `--json` output is JSONL: a progress frame (`{"type":"progress","phase":"checking"}`, no
  `status`) followed by the result frame. Live-captured up-to-date shape (delta 0.23.0):

  ```json
  {"schema_version":1,"status":"up_to_date","current_version":"0.23.0",
   "latest_version":"0.23.0","channel":"stable","can_install":true,
   "message":"Delta 0.23.0 is up to date."}
  ```

  Only `up_to_date` has been observed live; anything else is parsed defensively (any
  non-`up_to_date` status counts as restart-required).
- Plugin (omp-delta) outdatedness has no reliable offline signal: the installed
  `~/.omp/plugins/node_modules/omp-delta/package.json` version has no upstream reference to
  compare against, and there is no marketplace. The one real signal is the contract
  handshake: `thread contract` `.min` above the plugin's compiled-in `CONTRACT_MAX` means
  this build is too old for the installed CLI. Otherwise `/delta-update` always shows a
  constant reinstall-hint footer (`omp plugin install 'github:Samuka007/omp-delta'`).

## Repo policy (`.delta/settings.json`, /delta-setup #21)

Per-repo sync policy, format-compatible with the official Claude Code plugin
([zed-industries/delta-claude](https://github.com/zed-industries/delta-claude),
`hooks/setup/project-policy.ts` — the read/write authority both plugins share):

```json
{
  "threads": {
    "mode": "private",
    "organization": "org_01…"
  }
}
```

- `threads.mode` ∈ `manual` / `private` / `organization` (delta-claude's `ThreadMode`). Note the
  nesting: a top-level `{"mode": "…"}` is *not* the format — `mode` lives under `threads` and a
  file with only that is treated as unconfigured.
- `threads.organization` holds the **org id**: required non-empty for `private`/`organization`
  (delta-claude saves `team.id`), optional for `manual` (where it names the org `/delta share`
  shares to). omp stores the id from `delta org list` and resolves the display name from the same list.
- Missing file / missing `threads` / missing `mode` → unconfigured `manual` (no `problem`):
  current behavior, explicit `/delta-share` only.
- Malformed JSON / non-object / invalid mode / auto mode without organization → `manual`
  fallback plus a `problem` string (one-time `session_start` notice, shown in `/delta-status`);
  rewriting asks before overwriting an unreadable file and preserves unknown keys (top-level and
  inside `threads`), 2-space indent + trailing newline — same as delta-claude's `serializeProjectPolicy`.
- Git exclusion follows delta-claude's convention (`hooks/setup/setup.ts`): on save, the entry
  `/.delta/settings.json` (root-anchored) is appended to `<git-common-dir>/info/exclude` —
  repo-local, untracked, personal; the tracked `.gitignore` is not touched. This plugin's own
  repo `.gitignore` additionally carries `.delta/settings.json` (the `.delta/` worktree
  bookkeeping directory is a different thing and stays tracked).
- `session_start` precedence (#21): session-marker reconnect → server-side checkout discovery →
  policy auto-connect (`private`/`organization` only; the stored org id is validated against
  `delta org list` so a stale policy fails fast instead of looping in the provisioning retry).
  `manual`/unconfigured never auto-connects. Auto-connect runs in the session_start background
  and reuses `/delta-share`'s transient retry path (40×30s provisioning window); the handler
  itself never blocks or crashes on it.

## Windows notes

- ~~`state.json` writes must be atomic (tmp + rename)~~ Obsolete since the zero-local-state redesign (#10): the extension no longer writes any state files — identity lives in the omp session record, the delivery cursor is the server's `next_sequence`.
- Git Bash mangles leading-`/` arguments (MSYS path conversion) — set `MSYS_NO_PATHCONV=1`
  when invoking the CLI from bash with thread ids or paths; the same applies to paths passed
  to `herdr workspace create` (or use `E:/style` paths).
