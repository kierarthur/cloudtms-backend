# WB-16 current factual reader and expansion investigation

Read-only investigation, 10 October 2026; sequence64. Read full WB-16 card, all nine checklist rows, R2-F02/F03/W12–W14, `addendum/R2_EXACT_CONNECTIONS.md §1`, master owner/order table and indexed SCR10 specifications. No product, tests, SQL, fixtures, runtime, hosted, browser or design-pack generation was performed. Root alone implements.

## Exact scope and current owners

Actual deliverables are **D02/D03/D07**; D01/D04–D06/D08 are shared-owner context.

| Row | Exact owner | Current final source |
|---|---|---|
| D02 | `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` | `04102026_0585_banking_pay_next_current_case_pages_v1.sql` |
| D03 | `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3776–3951`, full successor of 0575 |
| D07 | GROUP_LINES Timesheet expansion in FE `js/bpay-next-workbench.js` | Existing common Workbench reader + strict Worker mirror; WB15 is currently extending its genuine Candidate child/presentation identities |

`private.bpay_next_workbench_group_lines_page_v1(jsonb,jsonb,integer)` is the 1634 thin wrapper. Common/public1806 and the current WB15 successor must be respected as actual later winners rather than replacing an earlier 1520/1743 body. Fresh final registry must be reread when WB15 closes. No need for a fabricated `work_position_components_page_v1`, new route, kind, producer, command or financial owner.

## Existing exact pre-Draft transport

All four factual kinds already exist at `GET /api/banking/pay/next/preparation-page`, through `broker/src/bpay-next-preparation.js` and the named public SQL owner:

| Kind | Exact request fields besides kind | Actual row/cursor |
|---|---|---|
| COMPONENTS | work_id, expected_revision_id, after_line_no, limit | approved_line_id, line_no, component_key/kind/date, source channel, approved/realised/held/residual SOURCE, position_ready; cursor line_no |
| SHIFTS | work_id, expected_revision_id, approved_line_id, after_detail_no, limit | actual shift_detail_id/detail_no/date, supplied UTC or local clocks/overnight, submitted/approved minutes/hours, label, native segment pay/excluded and five bucket hours; cursor detail_no |
| BREAKS | work_id, expected_revision_id, approved_line_id, shift_detail_id, after_break_no, limit | actual break_detail_id/break_no, supplied clocks or duration-only minutes; cursor break_no |
| RATES | work_id, expected_revision_id, approved_line_id, after_bucket, limit | actual rate_detail_id/bucket, approved_hours and nullable approved used source_pay_rate; cursor bucket |

There is **no run_id** in these scopes. SQL requires APPROVED current revision = applied revision, sealed/approved revision, line in that exact revision and break's shift in that exact line. It raises `BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT` / `BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION`, not a historical fallback. Every native page is relational limit<=100 and 120000 SQL row-envelope bytes. Worker applies 8s/128KiB, active-admin auth before RPC, exact field schemas and increasing native cursor. Do not strip these checks to let stale current evidence resemble frozen Draft detail.

Frozen Office `frozenPage(SHIFTS/BREAKS/RATE_DETAIL/RATE_SCHEDULE/WORK...)` uses actual run-line/run-work identities and is a **different** contract, already used by the existing Draft screen. Preserve it. Current Office `PAGES/ROWS` includes Case/current Work/frozen lines but **does not include** the factual COMPONENTS/SHIFTS/BREAKS/RATES schemas, despite the Worker having them. The narrow UI support is to add those four real schemas to the existing preparation `transport.page` or expose that same qualified transport to the Candidate component. Do not call the frozen route before Draft or route through dormant ACCEPTED_* F25 readers.

## Stable segment join and independently eligible siblings

WB36's installed1100 schema adds `shift_detail.segment_key`; `approved_segment` has **unique shift_detail_id FK**, native ID, revision/line/component/key, date/clocks, five hours and rates, approved pay/excluded, and exact parent FK. Ordinary key is upstream segment_id; Source key is real work_event_id; Daily/aggregate has no key. Segment identity must never be inferred from array ordinal, same date, equal clocks or a row label.

Current GROUP_LINES already carries approved_line_id, approved_segment_id, segment_key, member_kind/member_key, portion_kind, exact revision, Ready/Waiting/selectable/snooze identity/version/date and separate approved/realised/reserved/included/excluded/recovery values. The actual `private.bpay_next_workbench_work_members_v1(uuid,uuid)` final **1321** consumes approved segments plus current1236 native segment balance; parent and segment/target sums must reconcile. Work classifier current1438 separately checks member completeness/attribution and `snooze_at_cut_v1` for each exact key; same-date positive siblings remain independently selectable. No browser eligibility recalculation is needed.

**Small D03 factual gap:** SHIFTS currently omits `segment_key` and approved_segment_id from its closed reply. Return the genuine existing segment key (and, if needed, exact joined native approved_segment ID) from the same line-bound query. This is sufficient for GROUP_LINES↔SHIFTS matching because `approved_segment.shift_detail_id` is unique. Preserve null for aggregate/no stable segment; never invent one. Extend Worker/FE strict row mirrors together. No new segment schema or writer is needed.

Maintain one entitlement per `(revision,component_key,member_kind,member_key)`. AVAILABLE and RESERVED portions are distinct amounts of the same native member, not duplicate shifts. Use day rows as presentation grouping only; actual selected component parent stays WORK:DAY and snooze stays exact approved child. Show Waiting/snoozed portion only in its explicit section/read-only expansion, not as grey payable Ready context; an unsnoozed sibling remains in Ready. Do not suppress a whole day, recover negative wages or fabricate per-shift PAYE NET. Child continuation cannot claim a whole day/group total from only its first page.

## Required fields versus current facts

| UI requirement | Current genuine source / finite remaining work |
|---|---|
| Role / Band | `work_revision.job_title`, `band_label` already exist (core1324:161–162); current factual replies omit them. Carry those exact revision fields in the existing COMPONENTS/SHIFTS or GROUP_LINES response, nullable when absent. Do not parse detail_label into Role/Band. |
| Expense/Additional units, description, rate | approved_line has approved_quantity, unit_label, approved_unit_rate, evidence_ref; current COMPONENTS exposes only kind/date/amount. Add only required typed native fields through D03; do not divide amount by quantity to invent used rate. Arbitrary Additional/mileage identity remains distinct. |
| Exact shifts / break | SHIFTS/BREAKS already provide true supplied clocks/durations. Duration-only breaks show minutes, not invented endpoints. Null approved minutes/hours is not submitted value/zero. |
| Used rates | Existing RATES actual approved used source rate/hours by bucket; Source used rate may legitimately be null. No live Contract fill or effective/blended-rate arithmetic. |
| Available schedule / charge | Existing immutable `rate_schedule` separately stores revision STANDARD/ADDITIONAL/MILEAGE paye/umbrella/charge rates. Expose required exact snapshot values through named D03 factual reader support; keep them labelled available rather than falsely treating them as approved used rate or original charge. No new rate-family kind or live rate lookup is required. |
| Selected/suggested treatment and margin, Original/New figures | Require a real persisted exact bucket/resolution/version/quote. Current COMPONENTS/RATES/GROUP_LINES have no such complete decision packet; later WB29/WB45 own it. Show null/unavailable or omit unavailable controls now, never calculate margin or copy a full target quote over residual. Do not restore F07 comparison machinery. |
| Correction / original reference | Current core Work/GROUP_LINES does not transport a proven correction carrier/ref. Merely newer revision, changed physical ID, component ADJUSTMENT or signed amount is insufficient. Add a typed genuine existing relation if found, otherwise record its later producer/reader join; do not invent Correction or resolve a replacement pair in this screen. |
| Overpayment Original / Outstanding | Actual Case-component approved/capacity/recovered/written-off/reserved and maintained GROUP_LINES balances are available. Present one actual Overpayment Case parent, no duplicated negative Work line. Preserve gross/net distinction. |
| Due and stored cap | D02 page currently omits actual rule_id and stored rule fields. Exact native rule supplies weekly_due_source_ex_vat, minimum_earnings_threshold, take_home_floor_override and schedule dates/count. Add these typed facts through existing owner; do not label them as current affordability. |
| Recoverable this run / headroom cap reason | Null until a real WB27 estimate or captured Draft allocation exists. Opening must not call preparation/allocation/shared quote owner. Stored weekly cap/floor is factual; "insufficient pay" is not available on open. |

Native 0360 preparation is the authority that materializes period due (its actual scheduled rule, outstanding/Case capacity, period recovery and active holds). A displayed due figure must use actual retained period/rule authority with a clear distinction; do not use current total debt balance as Due or reproduce that allocation in browser. Current Case projection's `nominal_source_ex_vat` is native available capacity, not a WB27 recoverable estimate. A null period before first preparation remains honest; adding a reader cannot create one.

## UI and supplied SCR10

DESIGN_INDEX SCR10 maps T02, T02c correction and T02e Pay lines. Read the JSONs and inspected T02/T02c/T02e pixels. T02 shows date parent rows, actual shift Role/Band/Start/Finish/Break/Amount beneath, separate expenses, exact snoozed sibling and fixed bucket evidence. T02c's Correction/replaces/Original values are illustrative **only when genuine persisted authority exists**. T02e is a standard stacked **Pay lines** modal with Line, Units, Original rate, Suggested rate, Amount, Original charge, Suggested margin, Action needed. Its Day/Night/Sat/Sun/BH order is fixed business order, so these expanded/dialog headers are SDP34 exceptions, not new server sorts. Do not copy demo figures or build an independent viewer/framework.

Small actual integration: add expand/collapse to the WB15 native Timesheet group row, mount a bounded child renderer inside that row, retain parent frame/table/context/scroll and local choice store. Read GROUP_LINES with exact native presentation/parent and same Workbench context; then factual COMPONENTS and selected line SHIFTS/RATES/BREAKS using the retained Work/revision/line/shift IDs. Never call all Candidate facts, preview or financial command on opening. Keep child page/cursor/loading/error/Try-again independent of the parent. Validate all data before one child DOM adoption and reject a stale revision without resetting ticks/creating a replacement UUID. Collapsing or closing invalidates pending read generation; actor/owner/frame mismatch after await cannot repaint another person's window. Existing standard window adapter is reused for Pay lines.

The day tick belongs WB25; actual Snooze/Amend/Unsnooze actions belong WB38; estimate belongs WB27; future resolution controls WB29; exact physical Timesheet navigation WB17; frozen Draft detail WB20. Omit unavailable future controls, preserve their open acceptance joins, and consume real current facts now. No stub money/data, second Banking viewer or header pay date. Owner scrolling/dark/phone/focus policy applies; A4 real product screenshot/42-point verdict remains separate.

## Source receipts / limits of this note

| Source | SHA-256 |
|---|---|
| Worker factual transport `broker/src/bpay-next-preparation.js` | `c04cdd54164ac1a534f2ddb366485b3b842568fd88fdb8a187476ecd925404b0` |
| Public preparation current2018 | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| D02 current Case pages0585 | `ec0ab1ec0ce95b690f50c711023a6dda431e562ed075a9716361d1ede2aac436` |
| Current native segment final1236 | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |

Only this requested design note was written. Supabase/Postgres skill review principles were limited to source typing, exact scopes, indexes, ACL and bounded reads; no skill-suggested query/advisor/CLI/network run was performed. Rule B supersedes old addendum F25 accepted resolvers: existing native selection/captured Candidate/query snapshot/PIN remain the financial authority.
