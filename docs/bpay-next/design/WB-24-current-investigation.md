# WB-24 current investigation — sequence 72

2026-10-11. Read-only investigation after WB26. Product edits belong to the root builder. This note supersedes the earlier waiting-for-WB26 inventory in `WB-24-investigation.md`; it does not claim implementation, installation or app acceptance.

## Exact required deliverable and owner limits

Read the full WB24 card, `02_WORKBENCH.md:931–950`, every WB24 checklist row, `IMPLEMENTATION_CHECKLIST.md:2110–2129`, the master foundation/build rules, approved sequence and R2-F06 (`review/R2.md:120–124`) plus the WB24 finding (`review/R2.md:236–238`). The pack remains read-only. WB24 is sequence72, after WB26 sequence71 and before WB27 sequence73. Its dependencies are WB21/WB23/WB26/F16; design-first is NO.

The sole product deliverable is **WB-24-D04**, complete Candidate/list and Level2 include-all plus group NONE/SOME/ALL controls in the maintained `js/bpay-next-workbench.js`. D01 existing Case append receipts, D02 exclusion memory, D03 run exclusion and D05 reviewed card are shared-owner context, not duplicate implementation instructions. Keep their current intake, memory and accepted-run receipt owners.

The required state covers complete current eligible Work and repayment/Case membership, including unloaded pages. Candidate false dominates Work ALL/SUBSET, component and Case intent. A deliberate whole-Candidate true includes all eligible children, including Payment Advance repayments, without enumerating all Work/Case UUIDs. Group state aggregates WB25 Work components and WB26 native Case components. Card names `selectable_item_count` and `selectable_case_component_count` as the complete server counts on CANDIDATES/CANDIDATE_GROUPS. Selection is local until Create and must not compute money or use headroom. WB27 owns estimates/allocation-plan extraction.

The Done journey remains 12 groups, untick a group on page2 → main-list Candidate SOME; select Candidate → all12 and eligible Payment Advance repayment; exclude Candidate → no sealed Work for that Candidate. E6 deductions-only/payable0 Candidate stays disabled, contributes no selected Candidate/Create count and shows Nothing to pay this week. Actual joined app journeys are NOT_RUN.

## Re-read current dependency evidence

The current checklist records WB26 ITEM Built/Reviewed, BE `950a15a9083c2d3dd46493953473644b6a8e0a33`, FE `49ebb10c2452519b34f1d41ddcf9b1d546d03a2a`, source9/9 and sole PG17 compile/contract/coupling PASS (`IMPLEMENTATION_CHECKLIST.md:2108`). Installation is unchecked and authored joined app journeys are NOT_RUN. These are root-recorded checkpoints, not runs by this investigator.

WB26's scope/review explicitly assigns complete parent Case/group NONE/SOME/ALL to WB24 (`WB-26-build-scope.md:19–21`, `WB-26-design-review.md:39`). WB26 has genuine ROW and GROUP_LINES native Case-component controls, complete immutable accepted Case-page upload/recovery, current OPEN/PAUSED facts and Show estimate. Its upload/journal, rule/control capture, accepted-empty proof and live balances are completed named-owner behavior; WB24 needs no money, producer, prepare kernel, new command, manifest or protocol change.

## Current native source owners

| Signature / purpose | Current registered final owner |
| --- | --- |
| `private.bpay_next_selection_memory_leaves_v1(jsonb)` | `11102026_0121_banking_pay_next_case_choice_control_point_v1.sql` |
| `private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)` | `10102026_2306_banking_pay_next_actor_selection_memory_v1.sql` |
| `private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)` | same2306 |
| `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` | `11102026_0113_banking_pay_next_case_choice_readers_v1.sql` |
| `public.bpay_next_workbench_page_v1(jsonb)` | `11102026_0008_banking_pay_next_component_count_scope_v1.sql` |
| `private.bpay_next_workbench_refresh_group_display_v1(text)` / `private.bpay_next_workbench_refresh_case_v1(uuid)` | `10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` |
| five-argument descriptor Create | `11102026_0005_banking_pay_next_component_choice_validation_v1.sql` |
| Case append/seal | `10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql` |
| selection seal / exclusion memory | retained2306 / migration2306 |

Re-read the actual final owner before any successor edit; do not install the historical2340 or replace current0121 with2306 LEAF. Any truly necessary count support is a narrow successor of the existing read helper and matching closed transport schemas, registered normally. It is not another shared selection/finance framework.

## Complete server counters already exist; exact remaining count support

`selection_memory_row_v1`2306:766 narrows Candidate while preserving original Client/channel. Lines767–774 select Work, native Case or native component scope. Lines778–783 call the existing complete `choices_v1` helper once and count total/selected/changed membership over the relevant scope; GROUP_LINES restricts the actual approved_line_id. Lines832–835 return `choice_member_count`, `choice_selected_count`, `default_selected`, `selection_eligible`, `new_changed` and `selection_basis`. Candidate eligibility additionally requires payable_count>0. These counts are complete eligible scope, not visible-page totals and not reserved/native `selected_count` money facts.

The card's split `selectable_item_count` and `selectable_case_component_count` fields are absent. Add their fixed Work/Case split to that same native count query if implementing the literal card contract; combined choice_member_count must equal the split sum. Count native choice-helper units, never presentation groups, loaded rows, amounts or raw LEAF segments. In choices2306:88–95, multiple LEAF segments are collapsed by Candidate/Work/revision/approved_line/component_key/direction. Direction is retained in that grouping. Consume the returned GROUP_LINES member/selected counts as authoritative; do not assume one line means one count unit.

`workSelection` FE:981–983 currently changes count by +/-1 per line using an ephemeral boolean default. The required narrow correction is each line's **full member/selected-count delta**, preserving the existing Work ALL/SUBSET and Candidate false precedence. A line selected=true contributes its complete member count; false contributes0; compare against its prior complete selected count or inherited parent ALL/NONE/SUBSET count. This avoids incorrect group/Candidate SOME when a line's native unit count exceeds1.

Existing public0008 validates HEADER `count_selection` and supplies exact complete selected_candidate_count/selected_member_count for Create. This is not Candidate/group state and must not become per-click network selection writes. It passes `_selection_memory`, not local raw overrides, into row read shaping. Therefore returned row counts are the remembered/default baseline; local changed intent must be applied separately.

## Current local store and control gaps

Current `createChoiceStore` FE:941–993 uses the actor/agency/epoch/pay-date/original-scope namespace and version2 bounded state. Raw Candidate/Work/component/Case intent survives hidden pages. Aggregate state is limited to32KiB; each raw array is finite. Preserve the exact input shapes, memory reference, financial basis comparison, unresolved reconciliation, immutable freeze and receipt ownership.

- `observed` stores only native selection_basis for retained changed identities. `componentDefaults` is an ephemeral max1000 cache of Work/revision/eligible/default boolean (observe:973); it is not retained on reload. No complete Work count witness or per-Case default count survives when its group is unloaded.
- Work basis2306:789–801 contains native revision/position/settings/control facts but no membership counts. Case-component basis802–814 contains native revision/rule/balances/resolution/control but no case_id/default count. Component basis815–828 likewise lacks count/default evidence. Baseline financial basis remains separate from the proposed count witness.
- Candidate `candidateSelected`:976 is binary and ignores child overrides. Ready `selected`:1010, tick1027 and Space1036 use it; no per-Candidate indeterminate state exists. `choose`:1016 calls binary chooseCandidate, so a true action leaves old subordinate exclusions.
- `includeCandidatePayments`:978 already provides the correct whole-true action: removes that Candidate's bounded Work/component/Case overrides across hidden pages, records parent true and prunes retained witnesses/unresolved keys atomically. Reuse it for whole-Candidate true; false remains the existing parent intent. Never loop all eligible identities.
- Candidate Level2 include1343 is a button, not a three-state checkbox. It already invokes the correct whole-true reset. Its address passed from Ready1017 lacks complete Candidate count data; detail headers cannot be assumed to represent selected/native choice counts.
- WB25 Work group checkbox1375 already uses workSelection and chooseWholeWork. WB26 ROW Case tick1376 and genuine Case detail component ticks exist. PAUSED controls are grey/disabled1377. CASE aggregate group currently has no parent checkbox or aggregate basis; strict validator:573 explicitly expects selection_basis=null for group_kind CASE.
- Office child `onSelection`913–914 updates shell text only. Main Ready controller has no pure local refreshSelection method (returned API1078). Add a local rerender callback from Candidate child changes so main-list parent state updates immediately without a network refresh.

The matching Worker closed row contract is `broker/src/bpay-next-workbench.js:400–404/568–572/596`, with the same combined counters and CASE-parent null-basis expectation. Any new split count/group witness needs that exact reader schema and FE schema updated together, while request kinds and original descriptor intent stay unchanged. This is the indispensable transport support for the card's new fixed response fields, despite its Worker:none statement; it is not a new Worker route.

## Smallest complete local aggregation

Use an existing-store version3 with compact typed **count witnesses only for bounded changed native identities**, alongside existing observed basis. Do not retain all visited pages or every unmodified child's evidence. Keep the current transient window cache and persist only evidence needed for retained intent under the same32KiB total ceiling. Store upgrade preserves old raw intent; missing/stale witnesses remain unresolved until the original native row is observed again. It must not fabricate ALL/NONE or weaken the existing Create reconciliation fence.

For a Candidate row, start with complete server total T and baseline selected S. Parent false gives selected0 immediately and dominates all retained child intent. A deliberate parent true clears subordinate raw overrides then gives selected T for all eligible loaded/unloaded membership. Otherwise apply only bounded changed identities:

1. For each affected Work, replace its prior complete Work baseline selected count with its effective complete Work count **once**. Combine Work ALL/SUBSET and each changed component inside that Work calculation, using complete per-line member/selected counts, so overlapping parent/line edits are not double counted. A component-only edit needs its Work baseline count witness even when no explicit Work override exists.
2. Apply each changed native Case component using its exact baseline selected count versus effective0/member_count, once per actual native ID. Keep case_id as a count witness only when required to aggregate its presentation Case group.
3. Validate 0≤selected≤total. ALL means total>0 and selected=total; NONE means selected0; SOME means0<selected<total. A missing/unqualified witness has an explicit unavailable/unresolved presentation; do not derive a false ALL/NONE from the visible page.

Count witness identity includes Candidate, native Work/component/Case ID and expected Work revision where applicable; original selection_basis supplies existing reconciliation facts. Complete counters need coherent actor/context/memory qualification. On restored dirty state, retain old raw intent and observe original changed identities again exactly as current version2 does. Existing closed local/Worker row schemas must validate fixed fields and decimal integer strings; no arrays of all Candidate membership.

Mount native checkbox controls in Ready and Level2 using the same reducer and usual indeterminate DOM/accessible semantics. Whole true uses includeCandidatePayments; whole false uses chooseCandidate(false). SOME click/Space convention should produce whole true; ALL click/Space produces whole false. Carry a coherent current Candidate count witness into Level2 from the Ready row, or add the exact same fixed choice counters to its existing Candidate-scoped HEADER. Reuse current focus/navigation/window/context rules.

## CASE aggregate parent: actual native identities, no guessed UUID

Current group display1946:19–29 makes Work → WORK, genuine Overpayment or component-null Case → CASE, and other actual Case-component groups → ROW. CASE presentation_id is Case ID and case_component_id is deliberately null. Do not use the Case ID as a component UUID or drop the parent group checkbox as supposedly owned by WB26.

Current ordinary Case creator0330:43–47 accepts Loan/Advance/Manual Debt/Credit and explicitly forbids Overpayment. It creates a primary component:233–240; optional recovery component2:125/246–256 is only Loan/Advance. Current automatic Overpayment owner `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4388–4401` creates one finance Case and one ordinal1 RECOVERY component for each actual collection basis, then stores both native IDs. That current legal producer therefore yields a one-component Overpayment CASE group. Use the actual component lookup, never infer an ID from that producer's presently equal Case/component UUIDs. Schema `04102026_0300_banking_pay_next_case_instruction_core.sql:73/100–101` gives positive ordinal and unique case component key/ordinal, **not a hard component maximum**. Parent selection must prove complete current eligible native membership rather than assume one component for every stored Case forever.

Minimum supported path: use existing bounded GROUP_LINES/detail native component rows with a terminal cursor plus complete count proof; once complete bounded native membership is known, a group action atomically updates only those existing `intent.cases` entries, requiring each actual native basis. Final0113:957–968 returns a limit+1 complete/last_key proof; retained three-page cache by itself is not a complete group proof. Retain only changed intent/witnesses; no all-page inventory or new Case-ID intent kind/protocol. For prompt parent ticking while children remain unopened, a fixed bounded complete native group-choice witness in the existing row reader is justified: usual legal Overpayment group needs one actual ID/count/default/basis. A finite single-group envelope (at most100 and existing response/store budgets) can support larger current groups with exact cardinality proof and one atomic local update. It is read evidence for original component intents, not a new write protocol. An oversized/unproved group must remain honestly unavailable rather than accept a partial-page group selection. The parent checkbox itself is required by card938 and the explicit WB26 scope allocation; rendering only Work/ROW ticks does not finish WB24.

## Review boundary and remaining work

The proposed product scope is WB24-D04 plus only indispensable existing row-counter/closed-schema/store support and Office local rerender integration. Keep original WB25/WB26 native selection and journal flow. No money estimates, allocation kernel, money locks, balances, parked2153/F25 producer/resolver, public command or immutable accepted membership change is necessary.

Required builder checks belong to root's normal source/sole PostgreSQL compile/contract/coupling review and supplied SCR04/SCR09 screen evidence. No tests, fixtures, DB, browser, hosted service, pack tool, commit or product change was performed by this investigation. Only this findings Markdown is written.
