# WB16 detail UI integration review

**DRAFT FINDINGS — concrete fixes required before source closure.** Read-only review of `.codex-local/WB16-detail-ui.txt` against current FE Candidate window and Office caller. No product/draft edits, SQL/runtime/app/test execution. Existing native choice store and Rule B remain unchanged.

## Minimal actual join

Current `js/bpay-next-workbench.js:887–956` openCandidateWindow already owns the native Candidate frame, exact namespace/scope check, ten-row CANDIDATE_GROUPS reader, coherent four HEADER receipts, three-page cache/32KiB compact cursor history, original-key refresh anchor and preserved choiceStore. Its opener (909 in reviewed snapshot) first preserves exact accepted WAITING Draft identity, then calls `options.openGroup`, otherwise opens the actual Timesheet. Current Office `js/bpay-next-office.js:720,757–759` creates the existing preparation transport and passes the actual shell client/store into openCandidateWindow, but does not yet pass an openGroup or preparationTransport detail owner. The scratch mount is presently unjoined.

Smallest join is one actual Payment detail child owned from that existing openGroup seam, using the same shell client and existing `transport.page` as preparationTransport. Pass actual native `showReadWindow`, actor/owner predicates, parent Candidate context and inherited scope; retain choiceStore instance. Store the child frame/controller in the Candidate window so parent dismissal closes it and all grandchildren. Native frame stack handles parent inertness/focus; use alive for asynchronous read publication and active only for interaction. Label the action by actual child owner (Open payment), not Open Timesheet when a Payment detail child is invoked. Preserve existing exact Open Draft branch and separate Timesheet record action.

Opening detail is read-only: no choiceStore mutation/recreation, CREATE, estimate, financial callback or journal write. Parent refresh keeps the store namespace/intent. On a genuine stale detail receipt, stop old cursors and expose Refresh/reload through the existing Candidate refresh; do not silently retag address.context or bind latest revision. Returning from a child preserves highlighted group, parent scroll anchor and selections.

## Required evidence and cursor identities

| Reader | Native parent | Native seek/proof |
|---|---|---|
| GROUP_LINES | original Candidate+inherited client/channel+group/ROW+section context | item UUID cursor and full typed page receipt; retain native member/portion identities |
| COMPONENTS | exact work_id+expected_revision_id | line_no; exact approved_line_id/revision/component |
| SHIFTS | same Work/revision+approved_line_id | detail_no; genuine shift_detail_id+approved_segment_id+segment_key, NULL aggregate identity remains honest |
| BREAKS | exact shift_detail_id under the same line/Work/revision | break_no and exact cursor |
| RATES | exact approved_line_id/revision | fixed five bucket seek; used vs available identities remain separate |
| CASE_COMPONENTS | exact Case+Candidate | component_ordinal seek; exact component/rule/period evidence |

Existing preparation `createTransport().page` owns closed args/row/cursor validation and GET request/environment/actor behavior. Use that existing transport rather than construct a business RPC/frozen-detail request. Existing Workbench validation must also run on injected client results: `validateWorkbenchResponse(page,kind,normalizeWorkbenchInput(kind,args),actor)` before the smaller sameWorkbenchRevision check. The latter compares only agency/actor/epoch/official date/workbench/adoption revisions (`workbench.js:590`), not kind/scope/filter/page receipt, so it is not a substitute for full validation.

Before and after every awaited factual→point revalidation chain, require the same request ticket/generation and current actor/owner/frame. Validate the exact point member Candidate/group/work/revision/approved line/segment or Case component and native portion, not just item UUID plus revision. Refusal or changed revision leaves evidence unavailable; never turn cached incomplete evidence into0/Ready.

## Concrete draft defects and finite fixes

1. **Case cache loop (draft40–41,67–71).** `native` pruning uses evidenceKey(row), producing null Work-prefix/member-key strings, but Case data is cached under `CASE:<case_component_id>`. Every render removes the Case receipt, then renderCase schedules it again. Use a single native key function for all cache/queue/pruning paths, with separate explicit Work/Case/GROUP_EVIDENCE branches. Do not query GROUP_EVIDENCE as if it were a component.

2. **False Case numbers before/error detail (68).** Original falls back to a maintained portion's approved_source_ex_vat, which RESERVED split can set0; Outstanding falls back to deduction_balance even for payment Case where that value is0. Show NULL/loading/unavailable until exact Case factual receipt exists; use its native direction/kind capacity/balance fields. Do not claim an actual zero from an unrelated portion. Exact retained due stays NULL without period and recoverable estimate remains later WB27.

3. **Case first100 is not a point lookup (71).** Existing Case reader returns ordinal page; find() on first100 fails for an actual later component. Use actual Case component ordinal retained by an exact GROUP_LINES PK enrichment if needed, request after=ordinal-1/limit1 and verify the actual ID; otherwise implement the existing reader's genuine paged traversal rather than treat absence on page1 as missing evidence. No new RPC/whole-Case scan or guessed ordinal.

4. **Backward continuity missing (74,78,37–38,83,89).** Main histories are written but no top-edge reverse-scroll branch consumes them. Breaks and Pay lines only scroll forward; Pay lines drops old pages with no seek history. Reuse the existing Candidate three-page/32KiB compact original-key pattern for each pane, both directions, preserving measured removed/inserted-page height. Clear on true scope/revision change; do not reconstruct/retag server cursors. Trimmed history needs an honest first/restart affordance, not an inaccessible earlier range.

5. **Shift continuation replaces all detail (66).** moreShifts overwrites shifts rather than keeping a bounded forward/backward page window and its old native seek history. The tiny continuation div may have no scrollable overflow so its listener never triggers. Attach continuation to the actual detail scroll region (or meaningful bounded child list), preserving the component anchor. Load breaks for actual newly emitted shifts through the same bounded evidence protocol.

6. **Nested cache expansion exceeds intended three-page memory bound (8,19–26,40–41).** Four sections × three25-row pages can retain300 members; eager evidence for each can retain25 shifts ×10 breaks plus context receipts. Pruning to current member rows is finite but does not enforce the intended small detail cache/bytes. Prefer on-demand/visible native evidence with a small bounded LRU/page window; keep compact history capped by actual UTF8 bytes. Cap combined in-flight requests, not merely4 outer tasks that each fan out4 Break requests (up to16 Break requests plus point validations). Dispose pruned queued/late receipts. No durable evidence cache/local-storage journal.

7. **Missing full typed read validation (13,15,73).** sameWorkbenchRevision alone accepts mismatched scope/kind/rows from an injected client. Apply existing normalize/validate functions to every GROUP_LINES read and revalidation, then exact original context and member checks. Recheck current/ticket after the second awaited point read before returning evidence. Scoped stale means reload-required, not generic successful detail.

8. **Incomplete child async guards (37,66,82–88).** Several catch/finally paths do not test the read ticket; moreShifts has no ticket and mutates a cached object after awaits. Use per-child generation/current/alive checks through success/catch/finally, and do not render/restore focus into an inactive/inert parent while a deeper child owns interaction. Closing parent/actor or owner change suppresses every late publication.

9. **Multi-pane duplicate identity.** Four section pages can contain AVAILABLE/RESERVED of the same genuine segment. Keep one physical shift using exact approved_segment/shift identity and separate portion/status facts; same-date sibling segments keep independent status. Component totals repeat on member seeds and are displayed once, never summed by FE. Do not use a date/detail_no/key suffix as financial identity. Main map grouping by exact Work/revision/line/component is appropriate, provided fields are fully validated and NULL line/GROUP_EVIDENCE is handled separately.

10. **Pay-lines incomplete/precision presentation (82–86).** RATES limit5 can truthfully return incomplete when exactly5 actual rows exist under the conservative current contract; the draft ignores its cursor/complete flag. Either support actual bucket continuation or establish a bounded complete native five-bucket contract without claiming complete falsely. Available schedule rows remain separate from used rows. displayPounds rounds to pennies (current591–596), so using it for six-decimal unit/charge rates conceals exact rate evidence; render typed rate precision with an honest unit label and keep money2dp separately. No FE margin/pay calculation or guessed suggested rate.

## Existing behavior worth retaining

- Actual date-only strings are rendered as date-only using UTC-noon; stored clocks use Europe/London and supplied local clocks win. No fabricated timezone for Source local clocks.
- Component header values are server-owned named ready/waiting/reserved amounts. Keep unknown NULL and native reason copy. No browser money aggregation.
- Pre-Draft SHIFTS/BREAKS/RATES are actual approved evidence, not frozen Draft/history APIs. Actual accepted Draft action stays on its current immutable owner.
- Only actual Case RESOLVED/REVIEW fact may label that Case. Future Work bucket resolution, estimate/ticks/snooze owners remain explicit later joins. A static Show estimate label must not pretend an enabled estimate action exists.

## Static receipt

This review is integration/draft analysis, not whole-WB16 PASS. Root alone authors the finite product fixes. Source compilation/unit/application/375px screenshots/latency and actual correction/mixed-shift/Case journeys remain NOT_RUN by this reviewer.

- `.codex-local/WB16-detail-ui.txt` — `898458623723cab4563b23bfef221bdff945e3a6536eb68ad4290a72c53dfa8d`
- `js/bpay-next-workbench.js` — `da51596e69a645490470a7868620a585e7fbae6a19791de8043474bf7af7687c`
- `js/bpay-next-office.js` — `ff25580ba0f17a2e26b0a428c348ef88ee8a64d6ba58099b32efb5a5fbeb8e47`
