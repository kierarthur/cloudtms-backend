# WB-15 factual Candidate Banking label investigation

Read-only source design for sequence63. Full WB15 card02_WORKBENCH616–652 and its exact checklist rows were read. D02 current Case reader, D03 Office integration and D07 complete Candidate Banking window are actual deliverables; D01/D04/D05/D06/D08 are context, not separate producer/money/schema rebuilds. No product edits, SQL/DB/runtime, tests, fixtures, application, hosted or network operation was performed. Rule B is preserved.

## Current physical winners and factual scope

- `private.bpay_next_current_case_page_v1(uuid,uuid,integer)` and current_case_component_page remain physically0585. The Case reader22–28 returns actual Case ID/Candidate/kind/tax/status and legacy principal counters for OPEN/PAUSED; it explicitly promises factual selection, not eligibility/allocation. Component reader63–70 already returns real component ID/ordinal/revision, Case subtype, instruction kind, direction/payroll stage and current counters. Do not use Case principal counters as the payable group amount.
- `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` and public workbench_page are1806. CANDIDATE_GROUPS114–159 is an actual maintained WORK/CASE parent reader. READY already requires complete/not dirty/CURRENT and a current selectable member. It pages on nullable week, native group kind and actual Work/Case UUID. Worker limits the kind to10; SQL first chooses at most11 exact parent rows, then enriches members460–470.
- Current Case classifier remains1252 refresh_case(uuid),477–619. It attaches a real client/Contract through work_collection→Work→Contract482–483, but never writes client_display_name or timesheet_reference for its Case items. A standalone Case may truthfully have no client/Contract. Current Work classifier1438 copies real captured revision client name/Timesheet reference126–127.
- Contribution owner1332:444–494 already defines the correct separate READY gross, Waiting gross, READY/Waiting net-addition and gross/net deduction-balance measures. Display these existing factual measures; do not price or allocate again.

## Minimal labels and fields

| UI fact | Actual authority / finite support | Honest label and empty handling |
|---|---|---|
| Candidate title/name/reference | Captured actor-scoped Candidate summary/metadata; current items carry candidate_display_name/candidate_reference; exact Candidate scope | Candidate Banking; actual name and reference. Never UUID as visible reference or latest different Candidate. |
| Timesheet Payment | Work group work_id/physical_timesheet_id, item.timesheet_reference copied from its current captured revision | Timesheet <actual reference>; Source/Expenses can use actual revision source/component kind. No invented reference if missing. |
| Overpayment Payment | Actual Case case_kind OVERPAYMENT and immutable current Case rule/component subtype | Overpayment. Native case_id remains its outer identity. Never call every CASE an Overpayment. |
| Standalone Payment | Actual case_component_id plus case_subtype/instruction_kind/direction/payroll_stage | Loan payout/Loan repayment, Payment advance/Advance repayment, Underpayment, Manual credit or Manual debt as the exact native tuple permits. ROW is presentation of that component, not a new economic group. |
| Client | Work item captured client_display_name. Case exact group.client_id, from the real work_collection link; point lookup public.clients.name if not captured | Actual client; absent link stays unavailable/No client attached. Do not borrow another Case, Candidate's last Client, or today's unrelated Contract. |
| Pay method | Group.target_pay_channel, actual current Candidate channel represented by maintained projection | PAYE/Umbrella. Unknown remains unavailable; source_pay_channel is a separate origin fact. |
| Deductions | Actual scope members direction DEDUCTION / existing has_deduction; store is the WB21 local selection authority | Existing Yes/No marker; later WB24/25/26 tick ownership remains. Never infer deduction amount from PAYE net. |
| Ready amount | Existing group_amount=SUM included_source_ex_vat for READY/PAYMENT/GROSS_ADD; ready_net_add_amount separately; gross/net deduction balances separately | SOURCE gross ex-VAT; NET addition; gross/net repayment balance. No netting into promised take-home, no VAT/ratio calculator in FE. |
| Waiting amount | Reuse1332:482 gross_due_source_ex_vat on WAITING/PAYMENT/GROSS_ADD except RESERVED and484 nominal_source_ex_vat on WAITING/PAYMENT/NET_ADD except RESERVED | Waiting SOURCE gross / Waiting NET addition. Current1806 group_amount sums READY only, so using it on a Waiting-only group would show misleading zero. Add the exact existing two Waiting measures to the group projection if shown. |
| Unknown amount | Existing amount_known/selected_amount_known and unknown count from exact scope members | Unavailable, not GBP0. For accepted-but-not-materialised selected amount, preserve NULL and label pending. Native current outstanding remains separately labelled. |
| Open Draft | Existing item.accepted_run_id/accepted_command_id/accepted_module_epoch; exact run_command binding and Candidate run_worker if present | Open the exact accepted run; if actual run worker absent, preparation is pending. Never latest Candidate run or inferred draft from a reason string. |

0585's finite factual D02 change can add actual current_rule_id/current_rule_revision/case_revision and current rule.case_subtype through the exact Case/Candidate FK, and a real linked Client label. It does not become the maintained eligibility reader. Bounded candidate/case keyset and private ACL remain. The component subtype/identity is already present and can be reused for ROW labels. Supporting1806 projection changes should remain one narrow successor with explicit fields mirrored in the existing Worker and FE closed contracts, not a new API kind or transport.

## Complete parent and ROW contract

READY must retain exact complete/not dirty/CURRENT parent, same Candidate and genuine READY selectable current members. Member enrichment must match the parent's projection_revision and financial_view_revision; unknown/incomplete or currently Waiting/Blocked membership does not leak into READY. Ten complete outer groups means select ten parent/native presentation identities before bounded child paging; do not return the first100 child rows as a supposedly complete parent amount. Aggregate whole exact parent membership as current1806 does, with original statement/request bounds. Current Work expected revision remains distinct from any frozen Draft origin.

Persisted1252 group schema32–62 permits only WORK/CASE, unique case_id, canonical WORK:<workUUID>/CASE:<caseUUID>. It has no ROW or arbitrary new group identity. Card634 explicitly requires ROW=one standalone case component. For a standalone row, retain parent CASE group key/Case UUID and expose actual case_component_id as its presentation/native row identity. If more than one component of a Case becomes a standalone outer row, the reader/cursor must end in that component UUID, not duplicate case_id. A finite reader presentation union/discriminator can distinguish WORK, OVERPAYMENT Case and standalone native Case components without changing financial tables or inventing another entitlement. Do not split one existing Work economic unit or create synthetic Case records.

The source proves legal component tuples:0300:107–111 binds PAYOUT(Loan/Advance) to NET_ADD payment, CREDIT to gross/net addition according to actual tax treatment, and RECOVERY(Loan/Advance/Overpayment/ManualDebt) to gross/net deduction. Rule subtype40–46 supplies Payment advance/Underpayment/Manual credit distinctions. There is no generic ROW enum on finance_case. This exact tuple must drive a bounded label mapping; unsupported tuple remains unavailable. Existing preparation CASE_COMPONENTS exposes actual IDs if a bounded child needs them; do not claim principal Case labels identify each component.

No client/schedule/week may be fabricated for a standalone row. Case group week is genuinely NULL in current1252; keep the existing null discriminator in deterministic keyset. If a reader presents ROW identities, its closed Worker cursor/type checks currently expect WORK/CASE and coalesce(work_id,case_id) (broker413/420); they must be changed together to the exact native discriminator/ID. No wholesale schema change is necessary merely to label a Case component.

## Accepted Draft identity and separate Waiting

1438:152–157 copies exact accepted Work identity into each current member and marks IN_PREPARATION/WAITING.1252:535–557 does the analogous actual Case selection/instruction membership, with collision refusal. The accepted Work helper1252:167–175 qualifies SEALED selections/live run+worker states; therefore current source does not license pretending any arbitrary OPEN run is an accepted Draft. Use only retained positive evidence available from actual current projection. Opening a preparation still uses the exact original run UUID and Candidate.

A parent can have different accepted pairs among different actual members. Projecting a single min/latest run would lose authority. At parent level count exact distinct nonnull (accepted_run_id,accepted_command_id,accepted_module_epoch) tuples. Zero gives no Open Draft; exactly one may return that pair after its exact run_command/epoch validation; more than one requires the actual child-specific controls or an explicit multiple-drafts chooser backed by bounded native rows, not first/latest guessing. Do not aggregate an unbounded arbitrary JSON list into the parent response. Existing GROUP_LINES can add each row's own accepted identities as a finite supporting projection if the child control requires them. Preparation run_worker is a nullable exact join on (run_id,candidate_id), not a fake ready state.

Waiting/snoozed/held facts remain separately named WAITING/BLOCKED views. No grey/historical fillers enter READY. Existing1806 broad HEADER refuses global pending/dirty/adoption state before a page, preserves5s SQL/4s transport and actor/context cursor. Open does not preview, reprice, tick-save or mutate; WB21 store remains the selection authority. WB16 expand, WB24/25 tick controls and later actual progress/Draft child owners are not built by this label note.

## Necessary narrow work only

1. Complete the factual D02 Case label reader with exact subtype/current-version/client link where genuinely present; keep its factual-only meaning and no authority from legacy principal counters.
2. Add missing CANDIDATE_GROUPS display fields (native Payment reference/subtype/component identity and actual accepted tuple, exact Waiting measures when shown) with the same explicit Worker/FE shape. Preserve native parent eligibility, byte/time bounds and deterministic ten-group paging.
3. Implement the actual standard Candidate Banking component and Office opening/closing integration, using coherent context, captured actor, local store, one replacement/page anchor, separate Waiting/issues and truthful NULL amounts. ROW needs an actual component identity/cursor; a display-only label on one CASE parent must not falsely claim multiple standalone components were separately paged.
4. Record source completion separately from application journeys. The card's12 Timesheets→10+2,160GBP expanded conservation, wrapped Client and focus/selection journeys are NOT_RUN in this investigation. No new testing programme, fixtures, source/protected producers, query protocol or money-owner changes are proposed.

## Source snapshot hashes

| Source | Raw SHA256 |
|---|---|
| `supabase/repeatable/04102026_0585_banking_pay_next_current_case_pages_v1.sql` | `ec0ab1ec0ce95b690f50c711023a6dda431e562ed075a9716361d1ede2aac436` |
| `supabase/repeatable/10102026_1806_banking_pay_next_issue_reason_reader_v1.sql` | `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329` |
| `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` | `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b` |
| `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql` | `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391` |
| `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` | `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5` |
| `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql` | `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab` |
| `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| `broker/src/bpay-next-workbench.js` | `bb8062751b52dd956bcb8cae01cb55a0ede80d255512388f4d41ff6470672824` |
