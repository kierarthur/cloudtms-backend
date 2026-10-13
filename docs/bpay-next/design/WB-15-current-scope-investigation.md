# WB-15 current scope investigation

Read-only source/design investigation, 10 October 2026. No product, SQL, runtime, tests, fixtures, browser or design-pack generation was performed. This is implementation input, not an application or A4 screenshot verdict.

## Exact ownership

Authority: r3 `02_WORKBENCH.md:616–654`, all WB-15 checklist rows (sequence 63), and `review/R2.md` WB-15/WB-16/WB-17. The actual deliverables are **WB-15-D02, D03, D07**. D01 and D04–D06/D08 are shared-owner context, not additional duplicate implementation tasks.

| Row | Exact object | Required treatment |
|---|---|---|
| D02 | `private.bpay_next_current_case_page_v1(uuid,uuid,integer)` | Retain the bounded factual Case identities/labels; it is not a Ready/eligibility calculator. Complete maintained Ready/group/Case-choice scope must be supplied by the Workbench reader. |
| D03 | `js/bpay-next-office.js` thin shell/actor journal | Wire the Candidate child to the existing qualified client, local intent namespace and standard frame. Preserve pending bytes, original accepted run, readback and frozen physical detail. |
| D07 | Candidate Banking complete Ready-group window in `js/bpay-next-workbench.js` | Ten complete eligible parent tuples per SQL page, bounded independent children, exact amounts, separate Waiting/issues, coherent refresh/page anchor and retained local choices. |

Current final metadata: current Case page/component page remain `04102026_0585_banking_pay_next_current_case_pages_v1.sql`; common Workbench kind reader and public wrapper are now **1806**, not 1743; `private.bpay_next_workbench_candidate_groups_page_v1(jsonb,jsonb,integer)` remains the 1634 thin wrapper calling that common authority. Any supporting reader successor must preserve WB-14's reason sort, group-wide issue detail and WB-12 counts.

## Supplied screen inventory and binding omissions

Read `SCREEN_DESIGN_POLICY_20261010/DESIGN_INDEX.md:21–25` and the actual T02/SCR-07 JSON; viewed T02 dark/device-light and SCR-07 PNGs. SCR-09's indexed packet also includes SCR-09b (Action required), SCR-09c (rate set Ready), SCR-37 (pay hold) and T02c (correction). These are supplied alternatives with their own later action owners; they do not authorize simulated rate/resolution, snooze, hold, estimate or tick behavior. The saved T02 device-light PNG is blank below its background; record this as supplied-reference limitation, not a product screenshot result.

The usable T02 layout is the standard separate **Candidate Banking** modal, factual name/reference subtitle, Refresh, segmented Ready/Waiting/Action required/Blocked, wrapped Client, six outer columns (Payment, Client, Pay method/channel, Deductions, Amount ex VAT, Controls). Shift expansion, day/expense ticks, More/Snooze, Export CSV and Show estimate in this composite illustration remain their named WB-16/17/18/24/25/27/38 owners. Omit unavailable later controls rather than show placeholders. Demo values/counts are not authorities. No pay date or estimate is shown before its real owner.

Owner A3 overrides the specimen's one sorting arrow/paging: every data header needs server sorting; use continuous bounded scrolling, single-click row highlight, double-click/Enter opening and a reachable phone Open button. Waiting exists only inside Candidate Banking. An actual In Draft row has one **Open Draft** action to the existing progress/review or confirmed Payment Batch destination; it must not open a second Banking evidence viewer. The SCR-07 copy explicitly says that captured shifts/rates/breaks are reached inside that Draft. Dark product presentation also applies on device-light. Actual product screenshots and all applicable 42 policy checks remain A4 work after implementation.

## Current reader facts and concrete gaps

`broker/src/bpay-next-workbench.js:343–381` already closes CANDIDATE_GROUPS over genuine group identity, current revision/physical Timesheet, source/target channel, week, completeness/dirty/progress, section counts, client label, bank warning, selectable, reason and four separate exact decimal amounts. `1806` retains the 1743 group branch: READY requires complete/current/non-dirty and an actual selectable READY member; it selects at most 10(+sentinel) parents before per-parent member enrichment. This is the correct complete-parent boundary, not a truncated child-derived parent list.

1. **Window wiring is missing.** Current Office `loadCapabilities` mounts Ready rows but `openCandidate` still falls back to `openCandidateRecord(..., close)`. That closes the Banking parent and opens the ordinary Candidate record. Replace that fallback for the actual Ready-row action with a genuine Candidate Banking child. Keep the separate bank-warning link to the ordinary Candidate owner. The current `openIssueWindow` provides a reusable standard `showReadWindow`, actor/owner/frame lifetime and parent-context pattern; retain the real client and parent filter/scroll/intent store.
2. **All-data sorting is not present.** Worker input line 434 permits only `NAME_ASC` for CANDIDATE_GROUPS. Current tuple/order is fixed `(null-week, week, group_kind, native Work-or-Case UUID)` and the reader does not accept Client/channel/deduction/amount sorts. Supporting changes belong in the existing closed reader/wrapper, Worker and FE mirrors. Retain native UUID as final tie-breaker and exact cursor membership/context. Numeric sort uses genuine exact numeric amount and explicit unknown/null discriminant; never a browser float or a manufactured zero. Do not client-sort just the loaded slice.
3. **Current group metadata is narrower than the complete screen.** CANDIDATE_GROUPS has no Timesheet reference, Case kind/subtype/display description, Candidate name/reference, day/expense count or Draft reference/status. Candidate name/reference may be retained from the actual opening Ready row with the same identity/context, not demo values. Genuine Case labels are already available through CURRENT_CASES/CASE_COMPONENTS. For common outer labels either carry exact existing native typed fields in this reader or use those factual owners under one coherent scope; do not infer Payment Advance/Overpayment from a technical key.
4. **ROW exception needs an explicit native treatment.** Current closed group enum/schema is WORK/CASE with native Work-or-Case key. Card/R2 separately names stand-alone ROW = one Case component and the only null-group exception. Do not invent a new economic parent or merge unrelated components. Establish the actual native component identity/shape for that exception before calling the whole group contract complete; current WORK/CASE-only transport does not express it.
5. **Waiting cannot use READY money as its amount.** Current `group_amount` sums READY GROSS_ADD members; READY NET additions and gross/net debt balances are separate fields. A Waiting group can therefore carry zero `group_amount` while its genuine Waiting gross/nominal/reserved amount is positive. Use actual typed maintained members/section totals for the Waiting presentation, not that zero. GROUP_LINES already transports `accepted_run_id`, `accepted_command_id`, snooze identity/version/date, eligibility date, physical revision and portion money. Keep actual IN_PREPARATION/RESERVED separate from snoozed/FUTURE_WEEK; no tick, auto-unsnooze or recalculation.
6. **A whole-Candidate strip/count must be a whole scope fact.** HEADER/scope summaries are maintained server totals. Do not sum the first ten loaded groups or replace unknown with zero. A Candidate's per-section group counts and selected member count cannot be filled from the demo strip or partial list; omit a fact until its exact retained/reader authority is supplied.

`0585` CURRENT_CASES is STABLE, owner-only SECURITY INVOKER, NEXT-gated, max100/120000 bytes, UUID keyset, returns only OPEN/PAUSED Case principal and hold counters. It cannot alone prove selectable Ready scope. CASE_COMPONENTS has genuine Case kind/subtype/direction/stage, ordinal/revision and approved/funded/recovered/written-off/payout/recovery amounts; use those as factual labels/provenance, not a new allocation calculation.

## Narrow WB-16/WB-17 joins

WB-16 owns the actual expanded day/shift/expense/Additional/Case presentation. Its D02 is current Case-component page, D03 is existing `public.bpay_next_preparation_page_v1` and D07 is GROUP_LINES expansion. Existing pre-Draft COMPONENTS/SHIFTS/BREAKS/RATES take `work_id`, `expected_revision_id`, `approved_line_id`; no run ID or invented position-components RPC. GROUP_LINES supplies maintained eligibility/control/segment identity. Preserve zero versus null, submitted versus approved and signed recovery provenance; no browser headroom/payable reconstruction.

WB-17 owns the real Timesheet opener and frozen read transport. Before Create, use retained current revision's `physical_timesheet_id` (WITHDRAWN uses its actual applied revision where current is absent). After Create, use captured exact physical identity/evidence. Do not navigate via historical `original_timesheet_id` or silently substitute a newer mutable Timesheet. Main's existing actual `issueOwners.timesheet` callback already delegates to `openTimesheet` with actor/NEXT checks. Reuse that genuine owner through a stacked child preserving Candidate/main frames, pages, scroll, store and unknown journal. Current main's generic Candidate fallback that closes Office is unsuitable. Newer facts are an explicit informational Refresh, not an automatic changed selection.

## Source read receipts

SHA-256 at investigation boundary:

| Source | SHA-256 |
|---|---|
| BE `broker/src/bpay-next-workbench.js` | `bb8062751b52dd956bcb8cae01cb55a0ede80d255512388f4d41ff6470672824` |
| BE common/wrapper `10102026_1806_banking_pay_next_issue_reason_reader_v1.sql` | `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329` |
| BE `04102026_0585_banking_pay_next_current_case_pages_v1.sql` | `ec0ab1ec0ce95b690f50c711023a6dda431e562ed075a9716361d1ede2aac436` |
| FE `js/bpay-next-workbench.js` | `b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a` |
| FE `js/bpay-next-office.js` | `181b6609f8ba0ca2a35ded6bcbfcd6c810ea1201a94a1bea3f020c26ef3ac872` |
| FE `js/main.js` | `2b085d88fabc4de2094a54ade45e73ebe2876910f38eb49d5145ada91512b16f` |

Only the requested design note was written. Rule B's current native selection/captured Candidate/query snapshot/PIN authority is retained; parked F-25 accepted resolvers/producers and financial owners are outside this card.
