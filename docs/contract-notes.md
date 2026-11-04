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
