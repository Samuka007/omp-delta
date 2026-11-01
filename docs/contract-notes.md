# Delta CLI contract v8 — notes from live testing

Field-level notes recorded while building this plugin against `delta` 0.23.0.
The [plugin guide](https://delta.dev/docs/cli/build-a-plugin) gives the canonical
flows; this page records details it does not spell out.

## CLI conventions

- `--json` is a **global** flag and must precede the subcommand: `delta --json thread create …`. Without it, commands print human-readable text and JSON-line parsers silently starve.
- All commands this plugin uses accept it: `thread contract/create/append/watch/status/stop`, `worktree create/attach/mount/list`, `org current`.

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

## Windows notes

- `state.json` writes must be atomic (tmp + rename): the extension saves constantly, and
  readers otherwise observe missing or half-written files.
- Git Bash mangles leading-`/` arguments (MSYS path conversion) — set `MSYS_NO_PATHCONV=1`
  when invoking the CLI from bash with thread ids or paths; the same applies to paths passed
  to `herdr workspace create` (or use `E:/style` paths).
