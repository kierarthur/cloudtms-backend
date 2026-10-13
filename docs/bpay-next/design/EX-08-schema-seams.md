# EX-08 schema seams — current source, 11 October 2026

Read-only source census for root's EX08 SQL. No product/schema edits, database calls, business RPCs, tests or fixtures. All runtime/application checks NOT_RUN. Source authority: current saved migrations and `docs/bpay-next/final-definer-closures.json`; no parked 2153 accepted-input protocol is used.

## 1. Contract-move comparison exclusion: current relation is absent

No current native Contract-move comparison binding/exclusion relation or registered comparison owner was found in the current migrations/registry. Current history owner `10102026_0653_banking_pay_next_ps_c16_indexed_history_v1.sql:58–59` explicitly says the moved-root flat binding/vector/comparison receipts are not yet implemented and unavailable. Generic `bpay_next_simple_revision_comparison_page_v1` reports captured/current revision differences; it is not a Contract-move binding.

EX08's card `04_EXECUTION_AND_OUTCOMES.md:329,334–335` still requires move-bound work to be excluded when that native binding exists. Do not manufacture it from Contract inequality, historical Timesheet Contract, a source-family guess, the generic revision comparison, or a parked F25 table. This is a precise downstream dependency absence; add no early CM producer/schema solely to synthesize that relation.

## 2. Protected accepted Work, including before capture

Current owner `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)`: `11102026_1358_banking_pay_next_guarded_create_v1.sql:52+`. It inserts concrete `private.bpay_next_run_choice_member` Work/Case membership at 372–409; inserts frozen Rule B Candidate target settings at 551–559; writes accepted descriptor/command identity at 570–573. These commit atomically. Workers and captured run-work rows may not exist yet.

Use native:
- `bpay_next_run_choice_member`: `run_id,member_kind,member_id,candidate_id,work_id,expected_revision_id,case_component_id,selection_mode,selected,member_no,skip_code`. WORK has `member_id=work_id`. PK `(run_id,member_kind,member_id)`, unique `(run_id,member_kind,member_no)`; Candidate seek `bpay_next_run_choice_member_candidate_idx(run_id,candidate_id,member_kind,member_id)` (schema1832:21–47).
- `bpay_next_pay_run`: `id,actor_user_id,pay_date,status,selection_state,created_at_utc,confirmed_at_utc,accepted_choice_descriptor,accepted_choice_command_id`. Accepted descriptor is the immutable actual Create identity, not an enrolled-worker count. Current guarded Create guard protects its tuple (1358:6–45).
- `bpay_next_selection_candidate`: PK `(run_id,candidate_id)`, `member_no`; added frozen `target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,min_take_home_wtd` (core1324:462–467; snapshot0151:21–25). This is the correct pre-worker target. Join exact PREPARE command via `bpay_next_run_command(run_id,command_id)`/`bpay_next_command`, preserving original module epoch.
- `bpay_next_run_worker`: `id,run_id,candidate_id,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,status,preparation_revision,captured_preparation_revision`. Unique `(run_id,candidate_id)`; frozen Work target must never be replaced by current Candidate method.
- `bpay_next_run_work`: `id,run_worker_id,candidate_id,work_id,captured_revision_id,created_at_utc`; unique `(run_worker_id,work_id)`; exact Work/revision FK (core1324:505–519).

Existing helper `private.bpay_next_workbench_accepted_work_v1(uuid,text,text)` → current1252:150–193 returns `run_id,command_id,module_epoch,selected_source_ex_vat` with exact component/segment/snooze qualification and collision refusal. It narrows to SEALED run/work choice and specific active run/worker states (167–175). It is not a complete coarse early accepted/pending protection reader by itself. Do not weaken its existing projection meaning just to reuse its output.

Current `bpay_next_work_choice` is `run_id,work_id,expected_revision_id,selection_mode,selection_state,page_count,component_count`, PK `(run_id,work_id)` (schema1241:19–34). OPEN belongs to subset upload. Preparation-job PINNING is distinct from work-choice OPEN and pay-run selection OPEN/SEALED. DY06's index `bpay_next_work_choice_work_run_idx(work_id,run_id)` exists (1107:5).

Do not treat selected=false or skipped membership alone as an accepted protected money slice, and do not release protection merely from a cancelled status/deadline: require the original exact completed native Release/Discard/Expiry receipt where applicable. The current Case protection helper1509:35–83 demonstrates those completed-disposition qualifications.

## 3. Transfer and returned cash identities

Native `private.bpay_next_transfer` (core1324:651–695):
`id,run_worker_id,candidate_id,projection_id,build_command_id,transfer_no,beneficiary_kind,beneficiary_id,account_approval_ref,destination_rail,beneficiary_name_snapshot,sort_code_snapshot,account_number_snapshot,bank_details_hash_snapshot,currency,cash_amount,member_count,member_cash_sum,status,original_transfer_id,return_cash_id,created_at_utc`.

Exact native statuses include BUILDING, MEMBERS_READY, DRAFT, SCHEDULED, ISSUED_CSV, SUBMITTED, UNKNOWN, SETTLED, RETURNED, REFUSED, CANCELLED. Preview protected categories use these actual states; no current Candidate channel substitution. Display-safe creditor label/channel comes from retained transfer/frozen worker or exact destination leg, not freshly joined Candidate current pay method. Raw bank/account/hash fields remain server-only.

`private.bpay_next_return_cash` (700–713): `id,original_transfer_id,candidate_id,amount_owed,amount_held,amount_reissued_paid,created_at_utc`; unique original transfer, unique `(id,candidate_id)` and `(id,original_transfer_id,candidate_id)`. Original transfer FK is the immutable reissue creditor identity. A return is cash only, not a new payable/Case revaluation.

`bpay_next_transfer_member`: PK `(transfer_id,member_no)`, `run_worker_id,subject_kind,run_line_id,case_hold_id,signed_cash_contribution` (723–744). Subject kinds WORK/CASE/NET_ADJUSTMENT/CASH_REISSUE; exact native FK links frozen run-line/Case hold.
`bpay_next_transfer_outcome`: `id,transfer_id,receipt_id,outcome_kind,whole_transfer_amount,occurred_at_utc,received_at_utc`, unique receipt; immutable outcome kinds SUBMITTED/UNKNOWN/SETTLED/RETURNED/REFUSED/CSV_ISSUED (746–755).

Mixed destination authority is `bpay_next_destination_group` keyed by `anchor_transfer_id`, with unique `run_worker_id` and `projection_id`, Candidate, exact draft/net states and preparation/selection revisions (1827:80–113). `bpay_next_destination_group_leg` keyed by `transfer_id` holds `anchor_transfer_id,destination_id,leg_kind(OWN|ONEOFF)` (114–123). Current capture destination owner is1827; execution leg/complete-posting owner is0639; begin simple transfer owner2018; begin Case transfer owner0559. Keep all untouched.

## 4. Existing indexes and specific missing Candidate seek support

Existing:
- `bpay_next_run_work_work_idx(work_id,id)`, `bpay_next_run_work_page_idx(run_worker_id,work_id,id)`.
- `bpay_next_run_worker_page_idx(run_id,candidate_id,id)`; unique `(run_id,candidate_id)`.
- `bpay_next_worker_current_week_basis_idx(candidate_id,run_id,id)` **only where status READY/DRAFT/ISSUED/COMPLETE** (0370:68–69); cannot cover PREPARING/REVIEW/CANCELLING preview.
- `bpay_next_transfer_worker_idx(run_worker_id,transfer_no,id)`; `bpay_next_transfer_member_page_idx(transfer_id,member_no)`; `bpay_next_transfer_outcome_idx(transfer_id,occurred_at_utc,id)` (core1324:1023–1057).
- `bpay_next_destination_group_candidate_idx(candidate_id,anchor_transfer_id)`; `bpay_next_destination_group_leg_page_idx(anchor_transfer_id,transfer_id)` (1827:113,123).
- FC16 `bpay_next_run_choice_case_protection_idx(case_component_id,run_id,candidate_id) where member_kind='CASE_COMPONENT' and selected` (1509 migration:8–10).

No unconditional Candidate-first worker/transfer/returned-cash or Candidate-first accepted Work membership index was found in the inspected current migrations. Existing composite unique `(id,candidate_id)` is not a Candidate-first page index. If EX08 previews all native Candidate protected identities, add only the exact genuinely required indexes for its chosen predicates/keysets; do not assume the partial week-basis index covers early Drafts or issue an unbounded historical sort. A Candidate-first selected WORK membership index is a supporting choice, not a new business relation.

## 5. Workbench and version column names

`bpay_next_worker_control` (core1324:779–784): Candidate PK `candidate_id`; `financial_view_revision bigint,active_owner_epoch bigint,pending_outcome_count integer,updated_at_utc`.

`bpay_next_workbench_revision` singleton PK `id=1` (1252:6–13): `workbench_revision bigint,adoption_revision bigint,adoption_state(UNSEEDED|SEEDING|CURRENT|ERROR),dirty_candidate_count bigint,dirty_group_count bigint,updated_at_utc`; 1429:7–8 adds `pending_task_count bigint`.

`bpay_next_workbench_candidate_metadata` Candidate PK (1252:17–27): `candidate_sort_name,tms_ref,bank_warning_code,metadata_version bigint,continuation_pending boolean,adopted_metadata_version bigint,updated_at_utc`; 1429:28–35 adds `target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,target_umbrella_enabled`.

`bpay_next_workbench_group` (1252:32–65): `group_key,group_kind(WORK|CASE),work_id,case_id,candidate_id,client_id,contract_id,physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,complete,dirty,projection_revision,financial_view_revision,metadata_version,ready_count,action_count,blocked_count,waiting_count,stale_snooze_count,stale_snooze_code,progress_state,error_code,updated_at_utc`;1316 adds `certified_zero`. Group PK `group_key`; unique Work and Case IDs; Candidate index `(candidate_id,group_key)`.

`bpay_next_workbench_item` (1252:67–125) native identity/selection fields: `id,group_key,candidate_id,work_id,revision_id,approved_line_id,approved_segment_id,case_id,case_component_id,rule_id,member_kind,component_key,member_key,segment_key,portion_kind,component_kind,tax_treatment,source_pay_channel,target_pay_channel,direction,payroll_stage,approved_quantity,unit_label,section,reason_code,reason_priority,selectable,is_selected,accepted_run_id,accepted_command_id,accepted_module_epoch,amount_known,selected_amount_known,financial_view_revision,projection_revision,metadata_version,progress_state,error_code`. Available/reserved/recovery portions remain separate.

Money fields: `nominal_source_ex_vat,approved_source_ex_vat,realised_source_ex_vat,active_reserved_source_ex_vat,residual_source_ex_vat,gross_due_source_ex_vat,included_source_ex_vat,excluded_source_ex_vat,recovery_source_ex_vat,held_overhang_source_ex_vat,deduction_balance_source_ex_vat,selected_source_ex_vat`;1316 adds `written_off_source_ex_vat`. There is no `held_source_ex_vat` Workbench item column; that name belongs to native position/capture facts. None are browser authority or a net/headroom estimate.

The current core Work projector is **0426**, current wrapper/refresh-many1438, metadata/task owner1429, current Case projector0559. Existing metadata trigger covers actual Candidate channel/Umbrella writes; native task pages are bounded Candidate→Work→Case. These are real EX08 supports; no new projector/money protocol is implied.
