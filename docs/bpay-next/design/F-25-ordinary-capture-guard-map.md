# F-25 ordinary capture guard: accepted parent and real period owner

Read-only source design, 9 October 2026. Root alone edits product. No SQL/runtime/tests/fixtures/hosted calls. Saved in pieces: this first part resolves the concrete case_period stage owner and instruction parent. Final 1519 guard is the existing dependency; 2308 case PREPARE is being authored and is not yet a reviewed implementation.

## 1. Minimal boundary

`private.bpay_next_work_collection_capture_guard_v1() returns trigger` (1519:66–158) is shared by run_position, run_case_instruction and case_period. Change only the ordinary F25 terms binding in the instruction and new-period branches. Preserve the current real-money period UPDATE branch :72–94 verbatim. Preserve managed run_position branch for the separate accepted WORK capture closure; do not globally change `bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` or the original immutable collection guard.

The late terms reader is `work_collection_origin_v1`: it selects today's applied revision and nominal approval/classification :20–50 plus current rule :57–58. It does **not** select WORK.current_revision_id. Its original paid identity and actual SOURCE/TARGET/principal/capacity consistency are distinct money proofs and must survive. The accepted instruction already knows run/Candidate/exact selection/preparation/rule/policy; case_period has only case/component/Candidate/week/rule and needs the genuine current allocation stage, not latest accepted_input.

## 2. Period insertion has a genuine existing parent

1958 creates `case_allocation_state` before INSTRUCTION_CAPTURE (:190–203). Its row has exact job_id, command_id, module_epoch, owner_epoch, run/worker, selection_revision/preparation_revision and captured pay_week_start. PREPARE page locks run, Candidate control, job, worker and this state; after lease takeover it updates that same state's owner_epoch before entering instruction capture. The nonnull work_collection period insertion occurs only at 1958:351–355 inside that stage. CASE_CREATE's 0330 period insertion has no ordinary collection and stays on its existing null-origin branch.

Core worker_control has **no active_job_id**. It has candidate_id and active_owner_epoch. Existing claim owner 2236:92–99 increments the control epoch and puts that exact epoch onto the claimed job; all claimants serialize through the same Candidate control. Old jobs with stale epochs cannot satisfy the join below. Use the genuine BUILDING DRAFT/0 state + its LEASED PREPARE FINANCE_ALLOCATE job + matching active control/module epoch. There is no `ORDER BY latest`, no LIMIT1, no timestamp guess and no borrowed RPC nonce. `INTO STRICT` refuses absence or ambiguous scope.

Design lookup for the period INSERT branch, before terms validation:

```sql
select input.id, child.rule_id, child.rule_revision, child.valuation_policy_id,
       child.work_collection_id, state.id as allocation_state_id
into strict v_parent_scope
from private.bpay_next_case_allocation_state state
join private.bpay_next_job job
  on job.id=state.job_id and job.command_id=state.command_id
    and job.candidate_id=state.candidate_id and job.module_epoch=state.module_epoch
join private.bpay_next_worker_control owner
  on owner.candidate_id=job.candidate_id and owner.active_owner_epoch=job.owner_epoch
join private.bpay_next_module_control module
  on module.id=1 and module.active_owner='NEXT' and module.owner_epoch=job.module_epoch
join private.bpay_next_run_worker worker
  on worker.id=state.run_worker_id and worker.run_id=state.run_id
    and worker.candidate_id=state.candidate_id
join private.bpay_next_pay_run run on run.id=state.run_id
join private.bpay_next_run_command rc
  on rc.run_id=state.run_id and rc.command_id=job.command_id
join private.bpay_next_command command
  on command.id=job.command_id and command.command_kind='PREPARE'
    and command.agency_sequence=job.command_sequence and command.module_epoch=job.module_epoch
join private.bpay_next_accepted_input input
  on input.run_id=state.run_id and input.prepare_command_id=job.command_id
    and input.candidate_id=state.candidate_id and input.module_epoch=job.module_epoch
    and input.accepted_sequence=job.command_sequence
join private.bpay_next_accepted_case_input child
  on child.accepted_input_id=input.id and child.run_id=state.run_id
    and child.candidate_id=state.candidate_id and child.selection_revision=state.selection_revision
where state.candidate_id=new.candidate_id and state.pass_kind='DRAFT'
  and state.projection_no=0 and state.projection_id is null and state.status='BUILDING'
  and state.prepare_stage='INSTRUCTION_CAPTURE'
  and state.owner_epoch=job.owner_epoch and state.module_epoch=module.owner_epoch
  and state.preparation_revision=worker.preparation_revision
  and state.selection_revision=worker.case_selection_revision
  and state.pay_week_start=new.pay_week_start and state.pay_week_start=input.pay_week_start
  and worker.status='PREPARING' and worker.review_issue_code is null
  and run.status='PREPARING' and run.selection_state='SEALED'
  and job.job_kind='PREPARE' and job.status='LEASED' and job.phase='FINANCE_ALLOCATE'
  and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
  and input.case_scope='SEALED_SELECTION' and input.case_selection_revision=state.selection_revision
  and child.is_selected and child.case_id=new.case_id and child.case_component_id=new.case_component_id
  and child.economic_origin_kind='WORK_COLLECTION' and child.work_collection_id=new.work_collection_id
  and child.rule_id=new.rule_id;
```

This is a **stage scope resolver**, not a second owner. The caller already holds its business locks; the trigger adds no different lock order. A naked period insertion outside this exact live stage fails closed. BEFORE INSERT also fires for `ON CONFLICT DO NOTHING`: matching current stage/selected child is therefore required even if the period already exists. Do not treat an existing shared period as a new accepted origin or overwrite its consumed opening/counters.

## 3. Instruction parent is direct

For a nonnull ordinary `new.work_collection_id`, resolve its accepted child using `(new.run_id,new.candidate_id,new.selection_revision,new.case_id,new.case_component_id)` through accepted_input, original run_command and PREPARE job. Require child.is_selected, economic_origin_kind WORK_COLLECTION, exact work_collection_id, retained child.rule_id/rule_revision equal new.rule_id/rule_revision and child.valuation_policy_id equal new.valuation_policy_id. Tie the existing BUILDING DRAFT/0 state to new.run_worker_id/new.preparation_revision and the same current LEASED PREPARE/FINANCE_ALLOCATE job/control/module scope as above; do not infer the job from a different run.

Keep 0310 instruction insert guard unchanged: it verifies PREPARING run/worker, preparation revision and exact SEALED selection. Accepted child FK also ties the exact selected sealed item. Keep actual source/target PAYE/TAXABLE/OVERPAYMENT/RECOVERY/GROSS_DEDUCT identity, original immutable paid effect/run_line/transfer/policy and live principal/native/capacity invariants. The selected Source refusal remains `BPAY_NEXT_COMPARISON_NOT_INSTALLED`; do not use this ordinary branch to admit a Source collection.

The current null work_collection branch must still reject a case that actually has a work_collection. The accepted child's origin_kind is not an excuse to relabel a real ordinary collection as a CASE_CREATE instruction.

## 4. Money proof and final source receipts

Once either exact accepted parent is resolved, load `bpay_next_work_collection` by **that child's FK** and require collection `(id,case_id,case_component_id,candidate_id)` equal the expected ordinary identity. Ordinary creator binds `id=case_id=case_component_id`; this is not a new CASE_CREATE receipt. Keep the original immutable `work_id,component_key,original_effect_id,original_run_line_id,original_revision_id,original_approved_line_id,original_transfer_id,original_source_pay_channel,original_tax_treatment,original_valuation_policy_id` tuple (1519:171–179). Verify original effect/line/hold/member as 1519:190–225, or call a narrowly factored private helper containing those same immutable checks. Do not call the current origin reader merely to obtain this tuple.

| Retained original proof | Exact constraint |
|---|---|
| Original actual pay | financial_effect.effect_kind PAYROLL_SETTLED, positive source disposition; exact work/component/Candidate/original transfer. Original effect operation belongs to CSV_SETTLEMENT/INTERNAL_SETTLEMENT as established by the original guard. |
| Frozen original line | Same work/component/original captured revision/approved line/source channel/original valuation policy; target PAYE; effect SOURCE/TARGET tuple equals actual source_consumed and frozen ex/VAT/inc. |
| Original actual hold | Exact effect.operation_item_id hold, same run_line, status REALISED, source_reserved=effect/source_consumed. |
| Original component and transfer member | Immutable original approved line WORK/TAXABLE; original revision.source_kind ORDINARY and immutable WORK.work_kind ORDINARY; exact component key. Original transfer_member is WORK for the same frozen line, signed_cash_contribution=frozen_inc_vat. |
| Retained rule/policy | Accepted rule_id/rule_revision and exact case/Candidate FK; immutable rule OVERPAYMENT, weekly_due null, same original collection identity. Accepted valuation_policy_id equals the immutable original collection policy and new instruction policy. Never today's finance_case.current_rule_id/current_rule_revision. |

The `paid_basis_qualified` flag is monotonic downward (1519:182), and qualification_state/observed_revision are mutable projections. Their successful original acceptance is already represented by the exact immutable accepted child's genuine staged origin; do not re-run these current qualification/terms projections as delayed eligibility. The actual immutable original paid-effect proof remains mandatory. A selected Source work_kind is refused before using this branch, even if the collection's current flags happen to look ordinary-compatible.

Read actual position and case/component money under the caller's existing locks and keep these exact existing invariants, independent of current approval/current rule discovery:

```sql
-- Terms come from the retained accepted child; these are real money checks.
p.source_basis_channel='PAYE'
and p.realised_source_ex_vat>=0
and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
  is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
and c.principal_funded=0
and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
  is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,
                        x.written_off_source_ex_vat,x.active_recovery_source_ex_vat)
```

Do not demand today's p.applied_revision/current nominal approval/classification to equal the accepted case terms; do not restore accepted money into these live counters. The caller retains actual outstanding after recovered/write-off and disjoint active capacity, original case/component PAYE/TAXABLE/RECOVERY/GROSS_DEDUCT identity, one shared selected quote target valuer, and period/capacity checks. Unsupported actual signed/mixed/cross-basis state still refuses.

The period INSERT scope validates accepted terms/parent, **not a replacement period opening**. Keep schema CHECK opening_due<=opening_outstanding. The final 1637 migration makes realised_recovery+active_unrealised<=opening_due conditional on work_collection_id IS NULL: ordinary unscheduled collection opening is audit evidence, not a cap on later legitimate debt in the same week. Preserve that ordinary exception and actual component/period/capacity money invariants. Keep existing caller's exact shared-period lock and any consumed-rule incompatibility refusal. Do not compare an existing period's opening to freshly computed present outstanding and then overwrite it. A period can have been consumed by another legitimate earlier arrangement.

The UPDATE branch :72–94 is already a money-only branch and must remain byte-equivalent: origin id immutable; case/component/Candidate/week/rule/opening pair immutable; period_revision advances exactly once; realised recovery cannot decrease; active_payout remains zero; component recovered/active counters cover period counters; case principal/recovered/written-off/active pair equals the component. It does not call current origin and does not need an accepted receipt lookup for later settlement/cancel/expiry owners.

## 5. Exact bounded hunk and apply map

1. Add local accepted-input/case/state/collection/original witness records, or one private shared accepted-ordinary proof helper with explicit accepted_input_id+case/component. Preserve existing trigger signature, SECURITY INVOKER and search_path.
2. In nonnull case_period **INSERT** replace only the current origin call :102–106 with strict real INSTRUCTION_CAPTURE parent resolution and retained original ordinary proof above. Null-origin and UPDATE branches remain.
3. In nonnull run_case_instruction **INSERT** replace current origin call :115–119 with exact new.run/run_worker/selection/preparation parent and same retained original proof/current money. Keep original policy equality using accepted/original policy, not a new current lookup. Null-origin branch remains.
4. Leave managed run_position branch :123–157 untouched in this specific successor; its separate F25 accepted WORK closure is explicitly tracked in delayed-consumer note. Keep its later completion/lease transition semantics when that closure is implemented.
5. Revoke/owner declaration remains 1519 audited logical owner postgres and no public/anon/authenticated/service_role grant. No trigger recreation/schema migration is needed for a CREATE OR REPLACE successor.

Install the accepted ordinary guard/helper closure after accepted schema/stage/guards and before 2308 case PREPARE consumer installation/first use; include its actual signature in final closure inventory. The master lists the later work-collection/capture chain FC-15→SP-09→CM-13a→CM-13→CM-21; F25's exact missing accepted-input dependency must be recorded without claiming those later cards complete. Do not modify the shared current origin reader, case projection owner, money posting or invoice/Source publication owners for this bounded change.

There is no missing period run-key blocker in this source: the existing genuine current DRAFT state supplies an exact parent. The only refusal required beyond existing invariants is inability to resolve **one** exact live owner/accepted selected ordinary child; it must never select a latest receipt to recover from that failure. A private period write without this owner scope is refused.

## 6. Source checkpoint

SHA256 of the saved sources examined. Root's 2308 file was a 273-byte scaffold at the first read and is being edited; no review verdict is supplied for it. 2306 has its own independent source PASS note. These hashes prove read source only, not SQL compilation/runtime or installation.

| File under supabase | SHA256 |
|---|---|
| repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql | `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988` |
| repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe` |
| repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql | `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b` |
| migrations/04102026_0300_banking_pay_next_case_instruction_core.sql | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| migrations/03102026_1324_banking_pay_next_core.sql | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| repeatable/09102026_2236_banking_pay_next_accepted_prepare_consumers_v1.sql | `2881978637f38c72a6db81ed982f11a0e684bae6bac54a24bfbbb76979c3203c` |
| repeatable/09102026_2306_banking_pay_next_accepted_no_case_consumers_v1.sql | `f5d9bfb70e86ebf6cbd4ffae7fad377af27c972b0a54783bcd5d91d91e8813b7` |
