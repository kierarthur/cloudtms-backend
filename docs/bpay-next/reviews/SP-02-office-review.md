# SP-02 Office source review

10 October 2026. Independent read-only review of the actual saved FE diff against HEAD, full SP-02 card/checklist and `design/SP-02-caller-design.md`. No product edits, tests, fixtures, SQL, browser, runtime or hosted execution. Verdict: **three finite findings; not yet source PASS**.

## Findings

**H01 — late open is not bound to the original actor/open.** `import-workspace.js::open(initialTab, options)` assigns the singleton session, awaits the Source workspace and newly awaits the owner read, then unconditionally sets `modalCtx` and calls `showModal`. It has no original actor/current-actor callback or open generation check. Closing the shared owner client on logout makes its `peek()` return null, but does not prevent the Source workspace result from reopening a late stale Imports frame. Two concurrent opens can also share/overwrite session data. This is especially exposed by the newly added owner-read await. Narrow correction: capture original actor and an open generation, pass a current-actor callback from `openImportsModal`, reject stale completion before assigning returned workspace / showing or switching the frame, and bind owner-change updates to that original live frame. Owner display failure must not abort or delay factual Source access unnecessarily. Preserve the existing one-argument open API and null client for staff without the admin F-15 capability.

**H02 — NEXT-only text can outlive positive F-15 status.** `friendlyWorkspaceError` correctly checks `peek()` at the moment it converts the refusal, but stores the resulting NEXT sentence in `session.error`. `renderWorkspace` later renders that stored string unconditionally. The informational notice checks `peek()` on render, but Imports has no listener for the existing `cloudtms:bpay-next-owner-changed` event. Another existing surface can reset/requalify the shared client, leaving the already-rendered notice (and stored error text) visible while positive NEXT status is absent. Narrow correction: retain the typed error identity and derive its display during current rendering, or suppress/replace the exact NEXT sentence whenever current `peek()` is not NEXT; add finite owner-change repaint for the same actor/live Imports frame with disposal. Do not close the shared owner client from Imports. Do not infer NEXT from the DB refusal alone. The existing global client event and strict actor-aware `peek()` are sufficient; no new status API, timer or financial action is needed.

**H03 — the changed main handoff lacks a cache marker.** `index.html` bumps `weekly-source/import-workspace.js` to `20261010-next-source-retirement-r1`, but `js/main.js` has its old unchanged URL. The new module can therefore be paired with cached main.js that still calls `open()` without the shared owner client. Its NEXT-only copy then never qualifies despite current F-15 availability. Add one bounded main.js query marker for this handoff; do not change unrelated asset versions.

## Verified correct reuse

- Main passes the existing `bankingNextOwnerClient()` rather than creating an owner transport or deciding the payment writer. That client already closes on logout/actor change and returns null when its captured actor is no longer current.
- F-15 `begin` performs the existing strict two-field bounded read. Missing, LEGACY, DISABLED and unknown `peek()` values do not qualify the new notice at render time.
- The exact sentence is escaped plain text, not a raw DB code. It is included in both ordinary/single-scope rendering and the combined review early-return branch; combined finalise uses the shared outer render. A normal workspace refresh clears `session.error` but retains the owner client, so the independent informational notice survives that load when current status is NEXT.
- `requestJson` preserves the existing `error_code` into `error.code`; no request body, contract, selection, recovery UUID, generation, retry byte or business payload was changed.
- The diff does not disable factual Source controls or alter authorisation, upload, finalise, single/bulk selection, original recovery identities, invoice behavior or combined child owners. It adds display qualification only.
- The changed module's version marker is present and the owner module is loaded before the workspace and main. The shared owner module itself is unchanged.

## Observed source hashes

These identify the reviewed snapshot before any subsequent root fix, not a final acceptance receipt.

| FE file | SHA-256 |
| --- | --- |
| `js/weekly-source/import-workspace.js` | `81be3f8ea327591808fb23fd273ab715fcab5b7f2c7967264ec6e349d7ef4ae6` |
| `js/main.js` | `f4ed5ba611fd40ea425dea6f976046711c1eccfc087c7cbffcf9304553b44986` |
| `index.html` | `fc7034311579273fac6e79bb037d99aa345be8ff8acc01f074e0950bbad9a118` |
| Unchanged `js/bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |

Actual in-app behavior and joined SP-02 acceptance remain NOT_RUN. Re-read the final saved source before closing the three findings.

## Fresh fix review

The captured main actor/current-actor callback and `openGeneration` now guard successful workspace completion and the final owner await before `showModal`. H03 is closed: both the workspace marker and `bpay-next-source-retirement=20261010-r1` main marker are present. Saved NEXT error copy is also suppressed during rendering whenever current positive NEXT status is absent.

Two exact remnants prevent final source PASS in this snapshot:

- H01's rejected-open path still writes `session.workspace`, `loadedTab` and `error` in its catch without checking `current()`. A late rejected older open can corrupt the newer singleton session even though it later refuses to show its own modal. Add the same current guard before those catch assignments.
- H02's stated scoped owner-change repaint is not present in the saved module. There is no `cloudtms:bpay-next-owner-changed` listener; render-time qualification alone does not remove already-rendered NEXT copy when the shared client resets/changes. Add only the exact current actor/frame listener and its disposal.

Fresh source hashes: workspace `78d30066735abc2ef4c49f4f4e247ba6f527ce25fc49d3d7ec5f9d03f154956e`; main `a686f08d916ac57230eb695cd2e56ccf2ca3bcc624c59d82489c6a813fec895c`; index `ebeb48daa145b7187f9de7cdae60fb47fc906493d4ebe4e661c252ece59f4e7b`. No reviewer execution; root's reported ordinary checks are not independently executed here.

## Second fresh fix review

Workspace `bfe96f24388ac8ef3799023a77c17f4ad7411a099f946c1fd886476cdcc91e94`: H01 is now closed by the current guard before rejected-open catch assignments. The new single module-lifetime owner-change listener is present, does not accumulate on open, and does not close the shared client. It correctly repaints topmost Imports.

H02 has one finite remaining parent-return case: the listener ignores owner reset/change while a child modal is above Imports. Returning to the retained Imports frame can expose its old NEXT markup; its existing on-render callback otherwise calls `wire()` without re-rendering. Reuse the existing `session.reloadOnReturn` flag for an owner-change event affecting a retained Imports parent, so the already existing return callback repaints once. No new listener framework, timer or action is needed. Main/index hashes remain as in the fresh fix receipt. Source-only review remains open solely for that exact return path.

## Final closure — source PASS

Fresh saved workspace hash `bbaeea7cdc62d110b27b940483a6d565ffdc90e9a51cc2e032940cc09cc4061e`. The listener now returns without a client, repaints topmost Imports, and otherwise sets the existing `reloadOnReturn` flag. The existing return callback clears that flag before repainting once, so covered Imports no longer keeps old NEXT markup on return. The single module listener does not accumulate per open or close the shared F-15 client. Current positive `peek()` remains the only display qualification, including stored error suppression. H01–H03 are closed; no actionable finding remains in the named Office scope.

Main `a686f08d916ac57230eb695cd2e56ccf2ca3bcc624c59d82489c6a813fec895c` and index `ebeb48daa145b7187f9de7cdae60fb47fc906493d4ebe4e661c252ece59f4e7b` are unchanged from their reviewed fixes. This final source verdict supersedes the earlier open snapshots. No reviewer tests or runtime/business actions were executed; joined/in-app SP-02 acceptance remains NOT_RUN.
