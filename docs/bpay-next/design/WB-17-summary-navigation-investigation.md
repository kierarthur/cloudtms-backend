# WB-17 Candidate amount → Timesheets summary

Read-only follow-up, 10 October 2026. No product edits, SQL, tests, runtime or network calls. This supplements `WB-17-open-timesheet-investigation.md`; it is not a completed WB-17 join.

## Existing API is usable for a real finite scope

BUILD-FE `main.js:66427` defines `navigateBankingPayBatchTimesheetsSummary(batchId, options)`. Its **row scope** accepts native `timesheet_ids`/`timesheetIds`, validates UUIDs, deduplicates them and requires a nonempty list (`66460–66505`). Call shape already used by the legacy row owner at `21466–21472`:

```js
await navigateBankingPayBatchTimesheetsSummary('', {
  sourceAction: 'banking:pay:viewRowTimesheets',
  row_scope: true,
  timesheet_ids: exactPhysicalTimesheetIds
});
```

No fabricated legacy pay-batch ID is needed. In row mode the actual summary filter is **only** `{ ids: rowTimesheetIds.slice() }` (`66533`); candidate options are descriptive shortcut metadata, not an additional filter in this branch. Thus the NEXT caller must itself provide only that authenticated Candidate's exact selected Ready physical IDs. Passing candidate_id and a broad list does not establish Candidate scope.

The helper resets only the separate ordinary Timesheets list state/selection/cache (`66549–66663`), changes the main `currentSection` and calls `renderAll` (`66665–66675`). It **does not close/pop the current modal stack**, call the NEXT store or start a business command. Existing tooltip text correctly describes showing the summary without closing the modal (`main.js:55080`). This preserves the live Banking/Candidate frame, scroll and store, but the destination is the **background native Timesheets summary**, not a newly stacked Timesheets-summary child. Do not describe it as a child modal or route it through `openCandidateRecord`, `discardAllModalsAndState`, or accepted journals. Native `openTimesheet` remains the real per-row stacked Timesheet modal. A purpose-built duplicate summary modal is unnecessary.

`listTimesheetsSummary` serializes the actual IDs into the existing `/api/timesheets/summary` request (`main.js:379726–379739`). BE `handleTimesheetsSummary` authenticates admin (`index.js:86150–86152`), reuses its actual native filter specification, and passes the explicit ID list through `baseFilters.ids` (`86431–86433`). This is ordinary current display, not a NEXT financial reader or payment action. There is no need for a new SQL kind, server feature, financial calculation or producer to display a genuinely finite complete list of physical IDs.

## Concrete native bound

The native IDs path uses `effLimit = min(200, idList.length)` and `effOffset = 0` regardless of requested page (`index.js:86518–86524`). Consequently **more than 200 IDs cannot be represented as a fully paged exact scope through this helper today**; repeatedly changing page would repeat the first bounded result. The helper itself imposes no ID-list count bound. A NEXT integration must not collect arbitrary Candidate/agency IDs, pass a giant URL, or claim that a truncated loaded list is the complete selected scope. Within a proven complete <=200-physical-ID scope the existing reader is sufficient. On overflow or incomplete membership, do not navigate with a partial list presented as all selected Timesheets. This is a real current API limitation, not grounds to invent a new server feature merely to fill the screen design.

## Required selection proof before wiring the icon

The current Ready list has the shared WB21 choice store; `createChoiceStore` uses namespace agency/epoch/actor/pay date/Candidate/Client/channel, and its raw intent is IDs/booleans only. `candidateSelected`, Candidate dominance and immutable `freeze` are at `bpay-next-workbench.js:741–748`. Store intent is **not** a full positive physical-Timesheet membership. CANDIDATE_GROUPS loads ten native group rows at a time; GROUP_LINES has an independent <=100-row keyset. Neither the current visible DOM nor `includeCandidatePayments` is a complete positive selected-ID manifest.

For the eventual current amount shortcut, retain one coherent validated Workbench context and one frozen local-intent revision. Walk only the actual Candidate's **READY** native groups/members through their real cursors. Retain the same agency/epoch/actor/pay date/client/channel and Workbench/adoption revisions for every page; abort on stale/context change instead of retagging cursors. Ignore Case/ROW presentations, Waiting/accepted-Draft rows and every nonselectable member. For a Work, include its real projected revision physical ID only if at least one genuinely eligible member remains selected after Candidate → Work → component/day overrides. Deduplicate **physical UUIDs**, never booking/root originals or display reference strings. No money comparison, sum, browser amount-derived family or mutable Timesheets filter may decide eligibility.

A Candidate untick dominates subordinate choices. A Work ALL choice cannot override that Candidate exclusion; a Work exclusion suppresses that physical record. A SUBSET with no selected eligible member supplies no record. Partial component/expense/day inclusion still supplies the parent physical record once if a real eligible member remains. Existing projection `is_selected` is persisted projection state, not a substitute for the current local WB21 intent. Freeze/check local store revision before and after all reads and before navigation. Unknown/stale membership gets the existing explicit refusal/Refresh path; no automatic money action or preview.

## Ordered joins remain genuine

WB23 (`02_WORKBENCH.md:896–928`) owns complete all-matching/exclusion membership and bounded replay-safe pages. WB24 (`931–949`) owns complete Candidate/group three-state semantics including unloaded membership. WB25 (`952–978`) owns the actual group/day/expense/Additional subset controls and exact native component/segment selection, under Rule B. Current checklist still reports WB23 MISSING, WB24 MISSING and WB25 PARTLY_DONE; WB21's store/transport is a reusable prerequisite, not proof those controls/membership joins exist.

Therefore a visible Candidate amount icon cannot truthfully mean **all currently selected Ready Timesheets** merely by passing the ten loaded groups or defaulting every current Work to selected. Keep this final icon integration open through those real ordered joins. The native summary helper can be bound now as the real destination and its finite transport can be reused; do not ship a guessed selection collector, fake complete manifest, early alternative selection protocol or a Case/Waiting-inclusive Candidate filter. Post-Create captured evidence remains through A3c's single Open Draft and its exact existing frozen views; this current-summary shortcut never replaces those views.

Source boundary SHA-256: BUILD-FE `main.js` `2b085d88fabc4de2094a54ade45e73ebe2876910f38eb49d5145ada91512b16f`; FE Workbench `309673caf26633683fe97079ad85c0e840e66db7945624500a89c79c592f6b7e`; BE `broker/src/index.js` `b9872851195f757a57575b90cfade390d47ffeccc1991cbd702c01dd1dc8742b`. Saved files are authoritative and being edited concurrently; reread at implementation.
