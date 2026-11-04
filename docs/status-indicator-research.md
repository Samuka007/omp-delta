# Sync-status indicator research (#18)

Phase-1 note for issue #18: which omp extension UI surface should carry the persistent
sync-status band (`Δ <thread-short-id> · synced|supplying|offline`), how the
painter/render lifecycle behaves across session-replacing dialogs, and who owns
status surfaces today. Everything is grounded in the locally installed
dependency (`node_modules/@oh-my-pi/pi-coding-agent`, v18.16.x) and the two
installed omp extensions on this machine.

## 1. Surface inventory

From the extension API types
(`src/extensibility/extensions/types.ts` in the installed package):

| Surface | Signature | Persistent band? | Verdict |
| --- | --- | --- | --- |
| `ui.setStatus` | `setStatus(key: string, text: string \| undefined)` — "Set status text in the footer/status bar. Pass undefined to clear." (types.ts L376–377) | Yes — keyed slot, set until cleared | **chosen** |
| `ui.setWidget` | `setWidget(key, content: string[] \| component factory, { placement: "aboveEditor" \| "belowEditor" })` (types.ts L383–385; content type: `pi-tui/src/chat/extension-types.ts` L10) | Yes — keyed container above/below the editor | viable alternative |
| `ui.setTitle` | `setTitle(title: string)` (types.ts L391–392) | Only with a permanent re-assert loop (see §2) | rejected |
| `ui.setFooter` / `ui.setHeader` | declared in types.ts, but wired as **no-ops** in interactive mode (`src/modes/controllers/extension-ui-controller.ts` L206–207: `setFooter: () => {}`) | No | unusable |
| `ui.setWorkingMessage` | types.ts L379–380, shown only during streaming | No | transient |
| `ui.custom` | modal overlay with keyboard focus | No | not a status band |

## 2. Painter / render-loop lifecycle, and session-replacing dialogs

**The status slot needs no render loop.** `setStatus` is implemented as
`ExtensionUIController.setHookStatus` → `statusLine.setHookStatus(key, text)`
(`extension-ui-controller.ts` L194, L661–663). In pi-tui
(`src/status-line/component.ts` L1243–1255) that is a keyed `#hookStatuses`
map: identical text returns early (dedup), and only a real change calls
`#invalidateStatusLineRenderCache()`. Statuses render as one line each below
the status bar, sorted by key (`component.ts` L3858–3882, role
`omp.status.hook`), gated by the `statusLine.showHookStatus` setting whose
default is **true** (`src/modes/settings.ts` L266–275). Text is sanitized
(`pi-tui/src/chrome/shared.ts`, `sanitizeHookStatusText`). pi itself uses this
slot for link hints (`src/modes/interactive-mode.ts` L520
`LINK_HINT_STATUS_KEY = "omp:link-hints"`, set/cleared at L4410/L4417) — proof
that multiple writers coexist, one line per key, without starvation.

**The title needs a permanent re-assert loop.** The installed Orca titlebar
spinner (`~/.omp/agent/extensions/orca-titlebar-spinner.ts`) documents why:
pi repaints the title from its own writers ("session_info_changed, the win32
update-check restore", `resetExtensionUI`, `rebindCurrentSession`) with no
event an extension can observe, so it re-asserts its title every 1000 ms
(`MARKER_REASSERT_MS`). A setTitle-based Delta status would have to fight that
loop: whenever an Orca pane is present (`ORCA_PANE_KEY` set), the spinner
re-asserts its title every second and the last writer wins — a visible
flicker war with no protocol to arbitrate.

**Session-replacing dialogs: what the spinner guards.** Direct quotes from
`orca-titlebar-spinner.ts`:

- `resolvePainter`: *"the ctx.ui pi passes is a getter that calls
  assertActive() and throws once a session-replacing dialog invalidates the
  runner; optional chaining cannot screen that out. Read it behind a try and
  never mutate state before a paint has succeeded."* → every paint is
  `try { ctx.ui.setTitle(...) } catch { clearOwnedTimers(); }`.
- `buildTitle` also runs inside the try (`getSessionName()` calls
  `assertActive()`; `process.cwd()` throws ENOENT on a deleted worktree).
  Most call sites are timer callbacks where an escape is an uncaught exception
  and pi exits(1).
- Session boundaries: *"pi drops an open dialog through resetExtensionUI
  without resolving its promise, so a replaced or reloaded session never sends
  the matching close."* The spinner treats `session_start` and `agent_start`
  as the boundaries that prove no old dialog is on screen, and re-inits state
  there.

**What actually happens on session replacement (installed dependency):**
one `ExtensionRunner` per `AgentSession`
(`src/session/agent-session.ts` L2139 `#extensionRunner = config.extensionRunner`).
`/new` and `/resume` are `newSession()` / `switchSession()` **on the same
AgentSession** — same runner, same InteractiveMode, same `StatusLineComponent`
(created once at `src/modes/interactive-mode.ts` L2204). Extensions receive a
`session_switch` event, not re-instantiation; the keyed hook statuses in the
status line therefore persist across switches and the extension must clear or
repaint them itself on `session_switch` (that is exactly what omp-delta's
detach-on-switch handler does for its slot). Full teardown
(`session_shutdown`) disposes the runner: `emitSessionShutdownEvent` with a
dedicated **2 s** cap (`src/extensibility/extensions/runner.ts` L152–168,
`SESSION_SHUTDOWN_HANDLER_TIMEOUT_MS`), `clearManagedTimers`, and
`disposeFileFallbacks`. Generic event handlers have a 30 s budget.

**Timers:** `ctx.setTimeout` / `ctx.setInterval` (`runner.ts` L588–600,
L1663–1664) run callbacks with handler-grade isolation — a throw is logged and
routed to `onError` instead of escaping to `uncaughtException` (issue #5664),
handles are `unref`'d, and every outstanding timer is cleared at session
teardown. omp-delta already schedules its retries through this (`schedule()`).
**Caveat for future work:** a timer callback holding a *stale* ctx from a
previous session can still hit an invalidated `ctx.ui` and throw inside its own
guard — isolation only stops the process from dying, it does not make the
paint succeed.

## 3. Status-ownership protocols

Two installed protocols coordinate status writers; both are Orca-pane-specific
(gated on `process.env.ORCA_PANE_KEY`) and neither touches the footer slot:

- **Title ownership** (`orca-titlebar-spinner.ts`): a `globalThis` map at
  `Symbol.for('orca.pi.titlebar.owners')` keyed by pane. *"replacement
  factories share the process realm; retire the old owner before painting"* —
  a new owner calls the previous owner's `dispose()` first. One title writer
  per pane, enforced by convention.
- **Agent-status ownership** (`orca-agent-status.ts`): PID-based single writer
  (`ORCA_PI_STATUS_OWNED`; a dead owner PID is claimable only when
  `process.kill(pid, 0)` fails with ESRCH) plus a per-pane session-ownership
  map at `Symbol.for('orca.omp.status-session-owners')`: subagent/task
  transcripts (`ctx.agentKind === "sub"`, sessions with a `parentSession`
  header, nested task transcripts) never own pane status.

For comparison, Claude Code's AbovePrompt surface (what delta-claude uses for
its band) is an **exclusive hook chain**: delta-claude's `drawBand(host, e,
next)` (`hooks/band/band.tsx`) returns only its own tree while shown and never
calls `next(e)`, so any other plugin's AbovePrompt band never draws
(zed-industries/delta-claude#27, open). omp's keyed hook-status map has no
such starvation: every key renders, one line each, sorted.

## 4. Choice: `ctx.ui.setStatus("omp-delta:sync", …)`

The closest equivalent to delta-claude's AbovePrompt band on omp's surfaces,
considering lifecycle and ownership:

- **`setStatus` wins on lifecycle.** It is persistent by contract (set until
  cleared), needs no re-assert loop, and survives `session_switch` in the
  status-line component. `setTitle` requires a forever-interval plus painter
  guards, and actively fights the Orca spinner's 1 s re-assert loop whenever
  an Orca pane exists — an ownership conflict that cannot be arbitrated
  (there is no cross-extension protocol on the title beyond the spinner's own
  retire-before-paint, which only covers spinner instances).
- **`setStatus` wins on ownership.** The slot is keyed and multi-writer by
  design; pi's link-hint status coexists with ours. No Claude-Code-style
  AbovePrompt starvation exists on this surface. We pick a namespaced key
  (`omp-delta:sync`) so we never collide with `omp:link-hints`.
- **`setWidget({ placement: "aboveEditor" })` is the positional equivalent**
  of AbovePrompt, and remains the right surface if the indicator ever needs
  multiple rows, clickable links, or theme colors. For a single text line it
  costs component machinery (factory or `string[]`, container rebuild,
  disposal, `EditorTopGap`/`statusRowOccupied` layout interplay) for the same
  visible result as a keyed status line.
- **Degradation path:** if `ctx.ui.setStatus` is unavailable (older omp,
  headless/non-interactive modes), painting is skipped and a warning is
  notified once per process — never per event.

## 5. State → indicator mapping and testability

`deriveSyncIndicatorState(rt)` maps runtime fields to one of
`synced | supplying | offline | detached`; `renderStatusIndicator(state,
threadId)` renders `Δ <short> · <label>` and returns `null` for `detached`
(caller clears the slot). Both plus `shortThreadId` (strips the constant
`thread_` prefix, then takes ~8 chars — ids are `thread_<ULID>`, so cutting
the raw id would yield the uninformative `thread_0`) are exported pure seams
with unit tests in `tests/delta-status-indicator.test.ts`.

- **detached**: explicit `/delta-detach` (`rt.detached`, cleared again by a
  successful attach — `session_switch` detaches through the same path) or no
  binding at all → slot cleared (`setStatus(key, undefined)`). An incidental
  disconnect with the binding still present is **offline**, not detached — the
  issue requires the slot cleared on detach while a dropped watch/paused push
  still shows a status.
- **offline**: has a thread but push paused (`rt.paused` — paused outranks
  in-flight work), or transport down with **no** reconnect in flight
  (`!rt.connected` and no identity/connect window — mount/watch dead,
  decisive reconnect failure).
- **supplying**: work or windows in flight — connected with pending records,
  retry or history-wait counts; **or disconnected but actively reconnecting**
  (`expectingIdentity` / `connectAttempts > 0`): records keep queuing during
  those windows, which the issue defines as supplying, so the reconnect path
  outranks the bare disconnected check.
- **synced**: connected, not paused, nothing in flight.

The interactive painter (`paintStatus` in `omp-delta.ts`) is deliberately a
thin seam: it reads runtime fields, dedupes against the last painted text
(a no-op never crosses the extension boundary), and guards every
`ctx.ui.setStatus` call with try/catch following the spinner's discipline —
`paintedStatus` is updated only after the paint succeeds, so a failed paint is
retried by the next transition instead of being silently swallowed. Painter
behavior against a real terminal cannot be unit-tested; the pure seams above
are the tested contract.

## 6. Notes for #22 and future work

- If join (#17/#22) adds a second producer or a second thread binding per
  session, key the status per producer (`omp-delta:sync:<n>`) — the map
  renders one line per key, so two bindings can show two lines without extra
  work.
- The widget surface (`setWidget … aboveEditor`) is the escape hatch for a
  richer band (link, colors, multiple rows); it is keyed the same way, so
  migrating later does not fight other writers.
- Never paint the title from here: the Orca spinner's re-assert loop and pi's
  own title writers (`session_info_changed`, `resetExtensionUI`,
  `rebindCurrentSession`, win32 update-check restore) make the title
  unusable for persistent status without an ownership protocol that does not
  exist today.
- `session_shutdown` handlers have a 2 s cap: clearing the slot there must
  stay a single synchronous `setStatus(key, undefined)` — no CLI calls.
