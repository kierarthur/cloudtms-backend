# F-25 finite weekly origin selector

Read-only source design, 9 October 2026. Root alone writes product. No SQL, tests, runtime, data or hosted reads were executed. This finite fragment is executable SQL source design, not a compilation or installed-owner claim. Current 2153 now permits returned/reissued EXCLUDED zero without inventing a return-floor ref; ordinary returned earnings remain UNBOUND and refuse. Case NET processes ALL instructions but writes its own NET results only for NET_DEDUCT; other stages reuse DRAFT results.

## Complete original PREPARE + latest NET fragment

Invoke once per ACTUAL prior original worker under the acceptance owner's preheld Candidate/header/business/FK scope. No new lock is acquired by this selector. Parent must already prove exact prior order `(pay_date,created_at,worker_id)` and cut; if Enqueue has no reserved actual current worker UUID, exact paydate/created ties refuse. Never enumerate only contributions or DONE jobs, as that would omit missing earlier workers. This helper returns a refusal code rather than dropping a bad row. `issue_code IS NULL` is mandatory before acceptance. Expiry-rewritten original PREPARE requires its exact bound old DONE/READY snapshot below; an EXPIRED current job alone never proves original completion.

It returns closed economic identity/proof and INITIAL ARRANGED operand. Mandatory later eligibility closure is separate. Retain its canonical origin with the typed child; a non-reconstructible hash alone is insufficient. Bound original workers to 100, each original WORK/instruction scope to 1000 and its NET requests to 100, refusing overflow rather than truncating. This query is per exact worker and uses existing indexed scopes, not all-agency history. It does not rerun a finance calculator or snapshot money counters.

```sql
create or replace function private.bpay_next_week_origin_select_design_v1(
 p_candidate_id uuid,p_week date,p_original_worker_id uuid,p_cut bigint
) returns jsonb language sql security invoker
set search_path=pg_catalog,private as $sql$
with s as (
 select w.*,r.pay_date,r.created_at_utc as original_created,
   x.id as contribution_id,x.original_gross_amount,x.pay_week_start,
   x.original_pay_date,x.original_created_at_utc,x.contribution_revision,
   x.primary_binding_revision,x.basis_kind as observed_basis,x.payment_state as observed_payment,
   x.eligibility_state as observed_eligibility,x.eligible_arranged_amount as observed_operand,
   x.accepted_projection_id as observed_projection,x.accepted_bank_cash as observed_bank_cash,
   x.original_payroll_net as observed_payroll_net,x.original_transfer_id,x.returned_cash_id,
   x.reissue_transfer_id,x.return_floor_binding_ref,
   c.id as prep_command,c.agency_sequence as prep_seq,c.module_epoch as prep_epoch,
   c.command_kind,c.status as prep_command_status,c.sealed_at_utc,
   c.expected_member_count,c.enrolled_member_count,
   j.id as prep_job,j.status as prep_status,j.phase as prep_phase,
   j.command_sequence as prep_job_seq,j.module_epoch as prep_job_epoch,
   m.candidate_id as prep_member
 from (select p_original_worker_id as id) input
 left join private.bpay_next_run_worker w on w.id=input.id
 left join private.bpay_next_pay_run r on r.id=w.run_id
 left join private.bpay_next_worker_week_contribution x on x.original_run_worker_id=w.id
 left join private.bpay_next_run_command rc on rc.run_id=r.id
 left join private.bpay_next_command c on c.id=rc.command_id
 left join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
 left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='PREPARE'
), a as (
 select s.*,d.id as draft_id,d.status as draft_status,d.prepare_stage,d.completed_at_utc as draft_completed,
   d.command_id as draft_command,d.job_id as draft_job,d.module_epoch as draft_epoch,
   d.expected_instruction_count as draft_expected,d.processed_instruction_count as draft_processed,
   d.captured_instruction_count,d.weekly_binding_state as draft_week_binding,
   d.pay_week_start as draft_week,d.captured_work_gross,d.allocated_gross_additions,
   d.allocated_gross_deductions,d.allocated_net_additions,
   cs.status as selection_status,cs.selected_count,
   (select count(*) from private.bpay_next_run_line l where l.run_worker_id=s.id) as actual_line_count,
   (select count(*) from private.bpay_next_run_case_instruction i where i.run_worker_id=s.id
     and i.preparation_revision=s.preparation_revision and i.selection_revision=s.case_selection_revision) as actual_instruction_count,
   (select count(*) from private.bpay_next_case_allocation_result z where z.state_id=d.id) as draft_result_count,
   exists(select 1 from private.bpay_next_run_case_instruction i
     left join private.bpay_next_case_allocation_result z on z.instruction_id=i.id and z.state_id=d.id
     where i.run_worker_id=s.id and i.preparation_revision=s.preparation_revision
       and i.selection_revision=s.case_selection_revision and (z.id is null or z.pass_kind<>'DRAFT')) as draft_missing_result,
   q.command_id as net_command,q.request_no,q.input_kind,q.entered_paye_net,q.frozen_gross_inc_vat,
   q.case_draft_state_id,q.preparation_revision as net_prep_revision,q.selection_revision as net_selection_revision,
   q.expected_projection_revision,
   nc.command_kind as net_kind,nc.agency_sequence as net_seq,nc.module_epoch as net_epoch,
   nc.status as net_command_status,nc.sealed_at_utc as net_sealed,
   nc.expected_member_count as net_expected_members,nc.enrolled_member_count as net_enrolled_members,
   nj.id as net_job,nj.status as net_status,nj.phase as net_phase,
   nj.command_sequence as net_job_seq,nj.module_epoch as net_job_epoch,
   nm.candidate_id as net_member,
   p.id as projection_id,p.run_worker_id as projection_worker,p.projection_no,p.input_kind as projection_kind,
   p.gross_inc_vat as projection_gross,p.entered_paye_net as projection_entered,
   p.accepted_recoveries,p.accepted_net_additions,p.accepted_gross_additions,p.accepted_gross_deductions,
   p.cash_amount,p.retired_at_utc,
   n.id as net_state_id,n.status as net_state_status,n.net_stage,n.net_issue_code,n.completed_at_utc as net_completed,
   n.expected_instruction_count as net_expected,n.processed_instruction_count as net_processed,
   n.weekly_binding_state as net_week_binding,n.projection_id as net_state_projection,n.projection_no as net_state_no,
   n.allocated_gross_additions as net_gross_additions,n.allocated_gross_deductions as net_gross_deductions,
   n.allocated_net_additions as net_additions,n.recovered_target_total,
   nd.state_id as destination_state_id,nd.run_worker_id as destination_worker,
   nd.own_amount,nd.external_amount,nd.cash_amount as destination_cash,
   nd.external_credit_count,nd.external_leg_count,nd.completed_at_utc as destination_completed,
   (select count(*) from private.bpay_next_case_allocation_result z where z.state_id=n.id) as net_result_count,
   (select count(*) from private.bpay_next_run_case_instruction i where i.run_worker_id=s.id
     and i.preparation_revision=s.preparation_revision and i.selection_revision=s.case_selection_revision
     and i.payroll_stage='NET_DEDUCT') as net_deduct_count,
   exists(select 1 from private.bpay_next_run_case_instruction i
     left join private.bpay_next_case_allocation_result z on z.instruction_id=i.id and z.state_id=n.id
     where i.run_worker_id=s.id and i.preparation_revision=s.preparation_revision
       and i.selection_revision=s.case_selection_revision and i.payroll_stage='NET_DEDUCT'
       and (z.id is null or z.pass_kind<>'NET')) as net_missing_result
 from s
 left join private.bpay_next_case_allocation_state d on d.run_worker_id=s.id
   and d.preparation_revision=s.preparation_revision and d.selection_revision=s.case_selection_revision
   and d.pass_kind='DRAFT' and d.projection_no=0
 left join private.bpay_next_case_selection cs on cs.run_id=s.run_id and cs.candidate_id=s.candidate_id
   and cs.selection_revision=s.case_selection_revision
 left join lateral (
   -- Latest accepted request, including pending: never a DONE-only winner.
   select q0.* from private.bpay_next_paye_net_request q0
   join private.bpay_next_command c0 on c0.id=q0.command_id
   where q0.run_worker_id=s.id and c0.agency_sequence<p_cut
   order by c0.agency_sequence desc limit 1
 ) q on true
 left join private.bpay_next_command nc on nc.id=q.command_id
 left join private.bpay_next_command_member nm on nm.command_id=nc.id and nm.candidate_id=s.candidate_id
 left join private.bpay_next_job nj on nj.command_id=nc.id and nj.candidate_id=s.candidate_id and nj.job_kind='PAYE_NET_ENTRY'
 left join private.bpay_next_net_projection p on p.request_command_id=q.command_id
 left join private.bpay_next_case_allocation_state n on n.job_id=nj.id and n.command_id=nc.id and n.pass_kind='NET'
   and n.run_worker_id=s.id and n.candidate_id=s.candidate_id and n.run_id=s.run_id
   and n.preparation_revision=q.preparation_revision and n.selection_revision=q.selection_revision and n.module_epoch=nc.module_epoch
 left join private.bpay_next_net_destination_state nd on nd.projection_id=p.id
), v as (
 select a.*,case
 when p_candidate_id is null or p_original_worker_id is null or p_cut is null or p_cut<1
   or not coalesce(isfinite(p_week),false) or extract(isodow from p_week)<>1 then 'ORIGIN_ARGUMENT_INVALID'
 when id is null or candidate_id is distinct from p_candidate_id or target_pay_channel<>'PAYE'
   or pay_date<p_week or pay_date>=p_week+7 then 'ORIGINAL_SCOPE_UNBOUND'
 when contribution_id is null then 'CASE_WEEK_BASIS_UNBOUND'
 when (pay_week_start,original_pay_date,original_created_at_utc) is distinct from (p_week,pay_date,original_created)
   then 'CONTRIBUTION_SCOPE_INVALID'
 when prep_command is null or command_kind<>'PREPARE' or prep_seq>=p_cut
   or prep_command_status not in ('SEALED','COMPLETE') or sealed_at_utc is null
   or expected_member_count<>enrolled_member_count or prep_member is null or prep_job is null
   or ((prep_status,prep_phase) is distinct from ('DONE'::text,'READY'::text) and not (
     (prep_status,prep_phase)=('DONE','EXPIRED') and exists(
       select 1 from private.bpay_next_preparation_expiry_worker ew
       join private.bpay_next_preparation_expiry_request er on er.command_id=ew.command_id
       join private.bpay_next_command ec on ec.id=ew.command_id
       join private.bpay_next_job ej on ej.id=ew.job_id
       where ew.run_worker_id=a.id and ew.candidate_id=a.candidate_id and ew.run_id=a.run_id
         and er.original_prepare_command_id=a.prep_command and ec.command_kind='PREPARATION_EXPIRY'
         and ec.agency_sequence<p_cut and (ej.status,ej.phase)=('DONE','EXPIRED')
         and (ej.command_id,ej.command_sequence,ej.module_epoch)=(ec.id,ec.agency_sequence,ec.module_epoch)
         and ew.original_job_id=a.prep_job
         and (ew.original_job_status,ew.original_job_phase,ew.original_worker_status)=('DONE','READY','READY')
         and (ew.preparation_revision,ew.selection_revision)=(a.preparation_revision,a.case_selection_revision)
         and (ew.status,ew.stage)=('EXPIRED','COMPLETE') and ew.finished_at_utc is not null
         and ew.released_work_count=ew.expected_work_count
         and ew.released_case_hold_count=ew.expected_case_hold_count)))
   or (prep_job_seq,prep_job_epoch) is distinct from (prep_seq,prep_epoch) then 'ORIGINAL_READY_WITNESS_UNBOUND'
 when captured_line_count>1000 or case_instruction_count>1000 then 'ORIGINAL_SCOPE_LIMIT'
 when actual_line_count<>captured_line_count or gross_vat<>0 or gross_ex_vat<>gross_inc_vat
   or original_gross_amount<>gross_inc_vat then 'ORIGINAL_GROSS_INVALID'
 when case_selection_revision=0 and (captured_position_count<1 or captured_position_count<>captured_line_count or draft_id is not null)
   then 'NO_CASE_OUTPUT_UNBOUND'
 when case_selection_revision>0 and (draft_id is null or (draft_status,prepare_stage) is distinct from ('COMPLETE'::text,'COMPLETE'::text)
   or (draft_command,draft_job,draft_epoch) is distinct from (prep_command,prep_job,prep_epoch)
   or draft_completed is null or draft_week_binding='UNBOUND' or draft_week<>p_week
   or draft_expected<>draft_processed or captured_instruction_count<>draft_expected
   or draft_expected<>case_instruction_count or draft_processed<>case_allocated_count
   or actual_instruction_count<>draft_expected or draft_result_count<>draft_expected or draft_missing_result
   or selection_status is distinct from 'SEALED' or selected_count is distinct from draft_expected
   or captured_work_gross+allocated_gross_additions-allocated_gross_deductions<>gross_inc_vat) then 'DRAFT_OUTPUT_UNBOUND'
 when net_command is not null and (net_kind<>'PAYE_NET_ENTRY' or net_seq<=prep_seq or net_member is null
   or net_command_status not in ('SEALED','COMPLETE') or net_sealed is null or net_expected_members<>net_enrolled_members
   or net_job is null or (net_status,net_phase) is distinct from ('DONE'::text,'PROJECTED'::text)
   or (net_job_seq,net_job_epoch) is distinct from (net_seq,net_epoch)
   or projection_id is null or (projection_worker,projection_no,projection_kind) is distinct from (id,request_no,input_kind)
   or frozen_gross_inc_vat<>gross_inc_vat or projection_gross<>gross_inc_vat
   or projection_entered is distinct from entered_paye_net
   or (retired_at_utc is not null and not exists(
     select 1 from private.bpay_next_cancel_request cr
     join private.bpay_next_command cc on cc.id=cr.command_id
     join private.bpay_next_job cj on cj.command_id=cc.id and cj.candidate_id=a.candidate_id and cj.job_kind='SIMPLE_CANCEL'
     left join private.bpay_next_case_cancel_binding cb on cb.command_id=cr.command_id
     where cr.run_worker_id=a.id and cr.candidate_id=a.candidate_id and cc.command_kind='SIMPLE_CANCEL'
       and cc.agency_sequence>a.net_seq and cc.agency_sequence<p_cut and (cj.status,cj.phase)=('DONE','CANCELLED')
       and (cj.command_sequence,cj.module_epoch)=(cc.agency_sequence,cc.module_epoch)
       and cr.status='CANCELLED' and cr.released_line_count=cr.expected_line_count
       and cr.finished_at_utc=a.retired_at_utc
       and (a.case_selection_revision=0 or (cb.projection_id=a.projection_id and cb.net_state_id=a.net_state_id
         and (cb.status,cb.stage)=('CANCELLED','COMPLETE') and cb.finished_at_utc=a.retired_at_utc
         and cb.released_work_count=cb.expected_work_count and cb.released_case_hold_count=cb.expected_case_hold_count)))))
   then 'LATEST_NET_ORIGIN_UNBOUND'
 when net_command is not null and case_draft_state_id is null and
   (case_selection_revision<>0 or input_kind<>'PAYE_MANUAL' or request_no<>1 or destination_state_id is not null
    or accepted_recoveries<>0 or accepted_net_additions<>0 or accepted_gross_additions<>0 or accepted_gross_deductions<>0
    or cash_amount is distinct from entered_paye_net) then 'SIMPLE_NET_OUTPUT_UNBOUND'
 when case_draft_state_id is not null and (case_draft_state_id is distinct from draft_id
   or (net_prep_revision,net_selection_revision) is distinct from (preparation_revision,case_selection_revision)
   or net_state_id is null or (net_state_status,net_stage) is distinct from ('COMPLETE'::text,'COMPLETE'::text)
   or net_state_projection is distinct from projection_id or net_state_no<>request_no or net_completed is null
   or net_issue_code is not null or net_week_binding='UNBOUND'
   or net_expected<>actual_instruction_count or net_processed<>actual_instruction_count
   or net_result_count<>net_deduct_count or net_missing_result
   or (net_gross_additions,net_gross_deductions,net_additions) is distinct from (allocated_gross_additions,allocated_gross_deductions,allocated_net_additions)
   or (accepted_gross_additions,accepted_gross_deductions,accepted_net_additions,accepted_recoveries)
     is distinct from (net_gross_additions,net_gross_deductions,net_additions,recovered_target_total)
   or cash_amount<>coalesce(entered_paye_net,0)+net_additions-recovered_target_total
   or destination_state_id is distinct from net_state_id or destination_worker is distinct from id or destination_completed is null
   or own_amount is null or destination_cash is distinct from cash_amount or own_amount+external_amount<>cash_amount
   or external_amount>accepted_net_additions
   or ((external_amount>0) is distinct from (external_credit_count>0 and external_leg_count>0))) then 'CASE_NET_OUTPUT_UNBOUND'
 when input_kind='CASE_PAYOUT' and (gross_inc_vat<>0 or entered_paye_net is not null) then 'NON_EARNINGS_BASIS_INVALID'
 else null end as issue_code
 from a
)
select jsonb_build_object('issue_code',v.issue_code,'candidate_id',v.candidate_id,'pay_week_start',p_week,
 'contribution_id',v.contribution_id,'contribution_revision',v.contribution_revision,
 'original_run_worker_id',v.id,'original_pay_date',v.pay_date,'original_created_at_utc',v.original_created,
 'economic_basis_command_id',coalesce(v.net_command,v.prep_command),
 'economic_basis_sequence',coalesce(v.net_seq,v.prep_seq),'accepted_projection_id',v.projection_id,
 'basis_kind',case when v.net_command is null or v.input_kind='CASE_PAYOUT' then 'GROSS_FALLBACK' else 'PAYROLL_NET' end,
 'initial_payment_state','ARRANGED',
 'initial_eligibility_state',case when v.input_kind='CASE_PAYOUT' then 'EXCLUDED'
   when v.net_command is null then case when v.gross_inc_vat=0 then 'EXCLUDED' else 'ELIGIBLE' end
   when v.case_draft_state_id is not null then 'ELIGIBLE'
   when v.cash_amount=0 then 'EXCLUDED' else 'ELIGIBLE' end,
 'initial_operand',case when v.input_kind='CASE_PAYOUT' then 0 when v.net_command is null then v.gross_inc_vat
   when v.destination_state_id is not null then v.own_amount else v.cash_amount end,
 'primary_binding_revision',v.primary_binding_revision,
 'observed',jsonb_build_object('basis_kind',v.observed_basis,'payment_state',v.observed_payment,
   'eligibility_state',v.observed_eligibility,'operand',v.observed_operand,'projection_id',v.observed_projection,
   'bank_cash',v.observed_bank_cash,'payroll_net',v.observed_payroll_net,
   'original_transfer_id',v.original_transfer_id,'returned_cash_id',v.returned_cash_id,
   'reissue_transfer_id',v.reissue_transfer_id,'return_floor_binding_ref',v.return_floor_binding_ref),
 'prepare_origin',jsonb_build_object('command_id',v.prep_command,'sequence',v.prep_seq,'module_epoch',v.prep_epoch,
   'job_id',v.prep_job,'draft_state_id',v.draft_id,'draft_completed_at_utc',v.draft_completed,
   'gross',v.gross_inc_vat,'captured_line_count',v.captured_line_count,'captured_position_count',v.captured_position_count,
   'instruction_count',v.actual_instruction_count,'draft_result_count',v.draft_result_count,
   'captured_work_gross',v.captured_work_gross,'gross_additions',v.allocated_gross_additions,'gross_deductions',v.allocated_gross_deductions),
 'net_origin',jsonb_build_object('command_id',v.net_command,'sequence',v.net_seq,'job_id',v.net_job,
   'request_no',v.request_no,'input_kind',v.input_kind,'projection_id',v.projection_id,'net_state_id',v.net_state_id,
   'net_completed_at_utc',v.net_completed,'processed_count',v.net_processed,'expected_count',v.net_expected,
   'net_deduct_count',v.net_deduct_count,'net_result_count',v.net_result_count,
   'cash_amount',v.cash_amount,'entered_paye_net',v.entered_paye_net,'accepted_recoveries',v.accepted_recoveries,
   'accepted_net_additions',v.accepted_net_additions,'destination_state_id',v.destination_state_id,
   'own_amount',v.own_amount,'external_amount',v.external_amount,'external_credit_count',v.external_credit_count,
   'external_leg_count',v.external_leg_count,'destination_completed_at_utc',v.destination_completed)) from v;
$sql$;
```

Mandatory prefix guard before accepting a NET winner: all actual same-worker request commands below cut must be retained/checked, including absent jobs/projections. Count ≤100, count=latest.request_no, each request_no=expected_projection_revision+1 in range 1..latest; exact unique `(run_worker_id,request_no)` closes the range. For every request require same Candidate/PAYE_NET_ENTRY/member, seq>original PREPARE, DONE/PROJECTED exact job/module/sequence, matching projection worker/no/input; each nonzero predecessor projection has a genuine earlier request command sequence and a retirement caused by the successor owner. Missing prefix refuses; latest alone is insufficient. Root can adapt the explicit `NOT EXISTS` guard from the prior week-origin-proof note. Do not filter retired_at for retained earlier replay; the query's unretired latest check applies only at fresh prelocked acceptance.

Canonical proof must retain exact DRAFT instruction/result IDs and economic-output digests, plus NET_DEDUCT NET result IDs and reused DRAFT result IDs. Counts validate completeness but are not retained economic authority alone. No current rule/nominal/held/funded/recovered/outstanding counter is copied. Latest economic basis command stays PREPARE/PAYE_NET_ENTRY; a later outcome command determines eligibility, not the basis fields.

## Mandatory eligibility closure

The receipt selector below retains missing/pending receipts and returns an issue per row. Before durable acceptance root must require every issue NULL, prove the exact transition chain and compare the resulting state/operand with the observed contribution. Unsupported scope refuses; no filtered DONE-only winner or baseline.


```sql
create or replace function private.bpay_next_week_eligibility_receipts_design_v1(
 p_candidate_id uuid,p_original_worker_id uuid,p_cut bigint,
 p_basis_sequence bigint,p_projection_id uuid,p_original_prepare_command_id uuid,
 p_draft_state_id uuid,p_net_state_id uuid
) returns table(command_id uuid,agency_sequence bigint,command_kind text,
 weekly_applicable boolean,payment_after text,issue_code text,canonical_receipt jsonb)
 language sql security invoker set search_path=pg_catalog,private as $sql$
with e as (
 -- Every actual ACCEPTED receipt remains, including one with no completed job.
 select a.command_id,a.candidate_id,'OUTCOME'::text as lane,a.transfer_id,a.outcome_id,
   a.internal_receipt_id,a.posting_complete as complete,a.posted_member_count,
   null::uuid as expiry_job_id
 from private.bpay_next_outcome_request a join private.bpay_next_command c on c.id=a.command_id
 where a.run_worker_id=p_original_worker_id and c.agency_sequence<p_cut
 union all
 select a.command_id,a.candidate_id,'RETURN',a.transfer_id,a.outcome_id,null::uuid,
   a.posting_complete,null::bigint,null::uuid
 from private.bpay_next_return_request a join private.bpay_next_command c on c.id=a.command_id
 where a.run_worker_id=p_original_worker_id and c.agency_sequence<p_cut
 union all
 select a.command_id,a.candidate_id,'CANCEL',null::uuid,null::uuid,null::uuid,
   a.status='CANCELLED' and a.released_line_count=a.expected_line_count,
   null::bigint,null::uuid
 from private.bpay_next_cancel_request a join private.bpay_next_command c on c.id=a.command_id
 where a.run_worker_id=p_original_worker_id and c.agency_sequence<p_cut
 union all
 select a.command_id,a.candidate_id,'EXPIRY',null::uuid,null::uuid,null::uuid,
   a.status='EXPIRED' and a.stage='COMPLETE'
     and a.released_work_count=a.expected_work_count and a.released_case_hold_count=a.expected_case_hold_count,
   null::bigint,a.job_id
 from private.bpay_next_preparation_expiry_worker a join private.bpay_next_command c on c.id=a.command_id
 where a.run_worker_id=p_original_worker_id and c.agency_sequence<p_cut
), a as (
 select e.*,c.agency_sequence,c.command_kind,c.module_epoch,
   j.id as job_id,j.status as job_status,j.phase,j.command_sequence,j.module_epoch as job_module_epoch,
   m.candidate_id as enrolled_candidate,
   t.run_worker_id as transfer_worker,t.candidate_id as transfer_candidate,t.original_transfer_id as reissue_original,
   t.return_cash_id as reissue_cash,t.cash_amount as transfer_amount,t.member_count,t.execution_kind,
   ot.id as original_transfer,ot.run_worker_id as original_worker,ot.candidate_id as original_candidate,
   ot.projection_id as original_projection,ot.original_transfer_id as original_parent,
   ot.cash_amount as original_cash,ot.beneficiary_kind,ot.beneficiary_id,
   leg.leg_kind,leg.anchor_transfer_id,pp.input_kind,pp.cash_amount as projection_cash,
   o.outcome_kind,o.transfer_id as outcome_transfer,o.whole_transfer_amount,
   ir.command_id as internal_command,ir.transfer_id as internal_transfer,ir.run_worker_id as internal_worker,
   ir.candidate_id as internal_candidate,ir.projection_id as internal_projection,
   ir.execution_kind as internal_kind,ir.cash_amount as internal_amount,
   cash.id as cash_id,cash.original_transfer_id as cash_original,cash.candidate_id as cash_candidate,
   nd.state_id as destination_state,nd.own_amount,nd.completed_at_utc as destination_completed,
   cr.status as cancel_status,cr.expected_line_count,cr.released_line_count,cr.finished_at_utc as cancel_finished,
   cb.command_id as cancel_binding,cb.status as binding_status,cb.stage as binding_stage,
   cb.draft_state_id as binding_draft,cb.preparation_revision as binding_prep,cb.selection_revision as binding_selection,
   cb.expected_work_count as cancel_expected_work,cb.released_work_count as cancel_released_work,
   cb.expected_case_hold_count as cancel_expected_holds,cb.released_case_hold_count as cancel_released_holds,
   cb.accepted_projection_revision,cb.projection_id as binding_projection,cb.net_state_id as binding_net,
   cb.transfer_id as binding_transfer,cb.finished_at_utc as binding_finished,
   g.anchor_transfer_id as group_anchor,g.projection_id as group_projection,g.net_state_id as group_net,
   g.stage as group_stage,g.expected_leg_count as group_expected,g.sealed_leg_count as group_sealed,
   dc.command_id as group_cancel,dc.anchor_transfer_id as cancel_anchor,
   dc.expected_leg_count,dc.checked_leg_count,dc.cancelled_leg_count,dc.check_cursor,dc.cancel_cursor,
   ew.run_id as expiry_run,ew.run_worker_id as expiry_worker,ew.candidate_id as expiry_candidate,
   ew.status as expiry_status,ew.stage as expiry_stage,ew.original_job_id,ew.original_job_status,
   ew.original_job_phase,ew.original_worker_status,ew.preparation_revision as expiry_prep,
   ew.selection_revision as expiry_selection,ew.draft_state_id as expiry_draft,
   ew.expected_work_count as expiry_expected_work,ew.released_work_count as expiry_released_work,
   ew.expected_case_hold_count as expiry_expected_holds,ew.released_case_hold_count as expiry_released_holds,
   ew.finished_at_utc as expiry_finished,er.original_prepare_command_id,
   w.run_id,w.status as worker_status,w.preparation_revision,w.case_selection_revision,
   pj.id as original_prepare_job,
   (select count(*) from private.bpay_next_transfer q where q.run_worker_id=p_original_worker_id) as transfer_count
 from e join private.bpay_next_command c on c.id=e.command_id
 left join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=p_candidate_id
 left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=p_candidate_id and j.job_kind=c.command_kind
 left join private.bpay_next_run_worker w on w.id=p_original_worker_id
 left join private.bpay_next_job pj on pj.command_id=p_original_prepare_command_id and pj.candidate_id=p_candidate_id and pj.job_kind='PREPARE'
 left join private.bpay_next_transfer t on t.id=e.transfer_id
 left join private.bpay_next_transfer ot on ot.id=coalesce(t.original_transfer_id,t.id)
 left join private.bpay_next_destination_group_leg leg on leg.transfer_id=ot.id
 left join private.bpay_next_transfer_outcome o on o.id=e.outcome_id
 left join private.bpay_next_internal_receipt ir on ir.id=e.internal_receipt_id
 left join private.bpay_next_return_cash cash on cash.original_transfer_id=ot.id and cash.candidate_id=p_candidate_id
 left join private.bpay_next_net_projection pp on pp.id=p_projection_id
 left join private.bpay_next_net_destination_state nd on nd.projection_id=pp.id
 left join private.bpay_next_cancel_request cr on cr.command_id=e.command_id
 left join private.bpay_next_case_cancel_binding cb on cb.command_id=e.command_id
 left join private.bpay_next_destination_group g on g.run_worker_id=p_original_worker_id
 left join private.bpay_next_destination_cancel dc on dc.command_id=e.command_id
 left join private.bpay_next_preparation_expiry_worker ew on ew.job_id=e.expiry_job_id
 left join private.bpay_next_preparation_expiry_request er on er.command_id=e.command_id and er.run_id=ew.run_id
), v as (
 select a.*,coalesce(leg_kind<>'ONEOFF',true) as applies,
 case
 when candidate_id is distinct from p_candidate_id or job_id is null or job_status<>'DONE'
   or (command_sequence,job_module_epoch) is distinct from (agency_sequence,module_epoch)
   or enrolled_candidate is null or complete is not true
   or (expiry_job_id is not null and expiry_job_id is distinct from job_id) then 'EARLIER_ELIGIBILITY_PENDING'
 when lane='OUTCOME' and (command_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
   or phase<>'COMPLETE' or posted_member_count is distinct from member_count) then 'SETTLEMENT_RECEIPT_UNBOUND'
 when lane='RETURN' and (command_kind<>'CSV_RETURN' or phase<>'COMPLETE') then 'RETURN_RECEIPT_UNBOUND'
 when lane in ('OUTCOME','RETURN') and (transfer_id is null or outcome_transfer is distinct from transfer_id
   or outcome_kind is distinct from case when lane='RETURN' then 'RETURNED' else 'SETTLED' end
   or whole_transfer_amount is distinct from transfer_amount) and command_kind<>'INTERNAL_SETTLEMENT'
   then 'EXTERNAL_OUTCOME_UNBOUND'
 when command_kind='INTERNAL_SETTLEMENT' and ((internal_command,internal_transfer,internal_worker,internal_candidate,internal_projection)
   is distinct from (command_id,transfer_id,p_original_worker_id,p_candidate_id,p_projection_id)
   or internal_kind is distinct from 'INTERNAL_ZERO' or internal_amount is distinct from 0::numeric
   or execution_kind is distinct from 'INTERNAL_ZERO' or transfer_amount is distinct from 0::numeric
   or projection_cash is distinct from 0::numeric or reissue_original is not null) then 'INTERNAL_ZERO_UNBOUND'
 -- Even ONEOFF must have a valid complete receipt before its non-applicability is retained.
 when lane in ('OUTCOME','RETURN') and leg_kind='ONEOFF' then null
 when lane in ('OUTCOME','RETURN') and (agency_sequence<=p_basis_sequence
   or (transfer_worker,transfer_candidate,original_worker,original_candidate)
     is distinct from (p_original_worker_id,p_candidate_id,p_original_worker_id,p_candidate_id)
   or original_parent is not null or original_projection is distinct from p_projection_id
   or beneficiary_kind is distinct from 'CANDIDATE' or beneficiary_id is distinct from p_candidate_id
   or original_cash is distinct from case when leg_kind='OWN' then own_amount else projection_cash end
   or (leg_kind='OWN' and (destination_state is null or destination_completed is null
     or group_anchor is distinct from anchor_transfer_id or group_projection is distinct from p_projection_id
     or group_net is distinct from destination_state))) then 'OUTCOME_ORIGINAL_SCOPE_UNBOUND'
 when lane='RETURN' and (cash_id is null or cash_original is distinct from original_transfer
   or cash_candidate is distinct from p_candidate_id or (reissue_original is not null and reissue_cash is distinct from cash_id))
   then 'RETURN_CASH_IDENTITY_UNBOUND'
 when lane in ('OUTCOME','RETURN') and (lane='RETURN' or reissue_original is not null)
   and input_kind is distinct from 'CASE_PAYOUT' then 'CASE_WEEK_RETURN_FLOOR_UNBOUND'
 when lane='CANCEL' and (command_kind<>'SIMPLE_CANCEL' or phase<>'CANCELLED' or agency_sequence<=p_basis_sequence
   or cancel_status is distinct from 'CANCELLED' or cancel_finished is null
   or expected_line_count is distinct from released_line_count or worker_status is distinct from 'CANCELLED')
   then 'CANCEL_RECEIPT_UNBOUND'
 when lane='CANCEL' and case_selection_revision>0 and (cancel_binding is null
   or (binding_status,binding_stage) is distinct from ('CANCELLED'::text,'COMPLETE'::text)
   or (binding_draft,binding_prep,binding_selection) is distinct from (p_draft_state_id,preparation_revision,case_selection_revision)
   or cancel_expected_holds is null or accepted_projection_revision is null or binding_finished is null
   or cancel_expected_work is distinct from cancel_released_work or cancel_expected_holds is distinct from cancel_released_holds
   or (binding_projection,binding_net) is distinct from (p_projection_id,p_net_state_id)) then 'CASE_CANCEL_CERTIFICATE_UNBOUND'
 when lane='CANCEL' and group_anchor is not null and (group_stage is distinct from 'COMPLETE'
   or group_sealed is distinct from group_expected or group_cancel is null or cancel_anchor is distinct from group_anchor
   or expected_leg_count is distinct from group_expected or checked_leg_count is distinct from expected_leg_count
   or cancelled_leg_count is distinct from expected_leg_count or check_cursor is null or check_cursor is distinct from cancel_cursor
   or (binding_transfer,binding_projection,binding_net) is distinct from (group_anchor,group_projection,group_net))
   then 'DESTINATION_CANCEL_CERTIFICATE_UNBOUND'
 when lane='CANCEL' and group_anchor is null and transfer_count>1 then 'NO_SPLIT_CANCEL_TRANSFER_SCOPE_UNBOUND'
 when lane='EXPIRY' and (command_kind<>'PREPARATION_EXPIRY' or phase<>'EXPIRED' or agency_sequence<=p_basis_sequence
   or original_prepare_command_id is distinct from p_original_prepare_command_id
   or (expiry_run,expiry_worker,expiry_candidate) is distinct from (run_id,p_original_worker_id,p_candidate_id)
   or (expiry_status,expiry_stage) is distinct from ('EXPIRED'::text,'COMPLETE'::text)
   or original_job_id is distinct from original_prepare_job
   or (original_job_status,original_job_phase,original_worker_status) is distinct from ('DONE'::text,'READY'::text,'READY'::text)
   or (expiry_prep,expiry_selection,expiry_draft) is distinct from (preparation_revision,case_selection_revision,p_draft_state_id)
   or expiry_expected_work is distinct from expiry_released_work or expiry_expected_holds is distinct from expiry_released_holds
   or expiry_finished is null or worker_status is distinct from 'CANCELLED') then 'EXPIRY_CERTIFICATE_UNBOUND'
 else null end as issue,
 case when lane in ('CANCEL','EXPIRY') then 'CANCELLED' when lane='RETURN' then 'RETURNED_OWED'
   when reissue_original is not null then 'REISSUED_PAID' else 'PAID' end as after_state
 from a
), ordered as (
 select v.*,lag(after_state,1,'ARRANGED') over(order by agency_sequence) as before_state,
   first_value(original_transfer) over(order by agency_sequence) as first_original
 from v where applies
)
select v.command_id,v.agency_sequence,v.command_kind,v.applies,
 case when v.applies then v.after_state else null end,
 coalesce(v.issue,case
   when v.applies and v.lane in ('CANCEL','EXPIRY') and z.before_state<>'ARRANGED' then 'ELIGIBILITY_TRANSITION_INVALID'
   when v.applies and v.lane='OUTCOME' and v.reissue_original is null and z.before_state<>'ARRANGED' then 'ORIGINAL_SETTLEMENT_ORDER_INVALID'
   when v.applies and v.lane='OUTCOME' and v.reissue_original is not null and z.before_state<>'RETURNED_OWED' then 'REISSUE_SETTLEMENT_ORDER_INVALID'
   when v.applies and v.lane='RETURN' and z.before_state<>case when v.reissue_original is null then 'PAID' else 'REISSUED_PAID' end then 'RETURN_ORDER_INVALID'
   when v.applies and v.lane in ('OUTCOME','RETURN') and v.original_transfer is distinct from z.first_original then 'ORIGINAL_TRANSFER_LINEAGE_INVALID'
   else null end),
 jsonb_build_object('command_id',v.command_id,'sequence',v.agency_sequence,'command_kind',v.command_kind,
   'job_id',v.job_id,'module_epoch',v.module_epoch,'lane',v.lane,'weekly_applicable',v.applies,
   'payment_after',case when v.applies then v.after_state else null end,
   'outcome_id',v.outcome_id,'internal_receipt_id',v.internal_receipt_id,'transfer_id',v.transfer_id,
   'original_transfer_id',v.original_transfer,'return_cash_id',v.cash_id,'reissue_original',v.reissue_original,
   'reissue_cash',v.reissue_cash,'whole_transfer_amount',v.transfer_amount,'posted_member_count',v.posted_member_count,
   'case_cancel_binding_id',v.cancel_binding,'cancel_expected_work',v.cancel_expected_work,
   'cancel_released_work',v.cancel_released_work,'cancel_expected_holds',v.cancel_expected_holds,
   'cancel_released_holds',v.cancel_released_holds,'cancel_finished_at_utc',v.cancel_finished,
   'destination_anchor_id',v.group_anchor,'expected_leg_count',v.expected_leg_count,
   'checked_leg_count',v.checked_leg_count,'cancelled_leg_count',v.cancelled_leg_count,
   'expiry_original_job_id',v.original_job_id,'expiry_original_job_status',v.original_job_status,
   'expiry_original_job_phase',v.original_job_phase,'expiry_original_worker_status',v.original_worker_status,
   'expiry_expected_work',v.expiry_expected_work,'expiry_released_work',v.expiry_released_work,
   'expiry_expected_holds',v.expiry_expected_holds,'expiry_released_holds',v.expiry_released_holds,
   'expiry_finished_at_utc',v.expiry_finished)
 from v left join ordered z on z.command_id=v.command_id order by v.agency_sequence;
$sql$;
```

The selector keeps all known receipt lanes. Parent counts **all** returned rows ≤100 and requires every issue NULL; it must not filter them down to completed/valid receipts before counting. It retains completed ONEOFF receipts as `weekly_applicable=false`; ONEOFF money is not Candidate earnings and does not alter the transition chain. Unknown command kinds on a known receipt lane refuse. All applicable receipt sequences must exceed economic basis sequence and remain `< cut`; PREPARE and NET are the only economic basis kinds.

The closure deliberately refuses ordinary earnings as soon as a genuine return/reissue receipt exists: final 2018 actually sets UNBOUND and operand NULL. CASE_PAYOUT stays EXCLUDED zero throughout return/reissue, supported by exact original transfer, return-cash identity and completed owner receipts. Reissue counter totals are never captured as earnings. The serial state chain validates ARRANGED→PAID→RETURNED_OWED→REISSUED_PAID→RETURNED_OWED, or ARRANGED→CANCELLED. Other transitions refuse. Root should additionally compare each applicable reissue's exact return_cash_id and transfer ancestry with the immediately previous retained return/reissue receipt, not merely with a current counter.

### Final typed child and hash

After every origin/prefix/receipt guard passes, start with initial ARRANGED state/operand from economic origin. Apply the last applicable receipt (all preceding receipts remain retained/validated): PAID keeps initial eligibility/operand; CANCELLED sets EXCLUDED/0; RETURNED_OWED or REISSUED_PAID is supported only CASE_PAYOUT EXCLUDED/0. No receipt means initial ARRANGED. Derive original transfer from the first original settlement receipt, return-cash ID from actual same-original return receipt, reissue transfer from latest actual reissue settlement/return lineage. Compare these exact values and basis/projection/entered NET/bank cash against the observed contribution once while acceptance business locks are held. Any mismatch refuses.

Retain the complete canonical economic origin, prefix request/projection IDs and sequence/module/completion proof, immutable instruction/results scope/output digests and all canonical receipts. Compute `pg_catalog.sha256(pg_catalog.convert_to(proof::text,'UTF8'))` on this closed JSONB object, with domain `BPAY_NEXT_ACCEPTED_WEEK_ORIGIN_V1`; PG17 built-in avoids an assumed pgcrypto schema. Persist the closed proof under an append-only schema contract; do not store only a hash of mutable observations. No bank snapshot/account fields or current financial balances are included.

Project these exact SQL types into 2153 child INSERT (parent ID supplied only by acceptance):

| Typed child | Exact SQL type/value |
| --- | --- |
| accepted_input_id | uuid, actual new parent |
| candidate_id, contribution_id, original_run_worker_id | uuid, source identity verified above |
| pay_week_start, original_pay_date | date, finite/exact source tuple |
| original_created_at_utc | timestamptz, exact original run creation |
| contribution_revision, primary_binding_revision | bigint, exact observed checkpoint >0 |
| economic_basis_command_id, economic_basis_sequence | uuid/bigint, actual PREPARE or latest NET, both below parent cut |
| economic_basis_sha256 | bytea, exactly 32 built-in SHA256 bytes of retained closed proof |
| accepted_projection_id | uuid nullable, exact latest NET projection |
| basis_kind | text GROSS_FALLBACK for original/no NET and CASE_PAYOUT; PAYROLL_NET otherwise |
| payment_state | text ARRANGED/PAID/CANCELLED; RETURNED_OWED/REISSUED_PAID only supported CASE_PAYOUT |
| eligibility_state | text ELIGIBLE/EXCLUDED, preserve legitimate case NET ELIGIBLE zero |
| eligible_arranged_amount | private.bpay_next_penny_amount, genuine gross/own bank cash or certified excluded 0 |
| return_floor_binding_ref | text nullable; no invented ref; current finite supported returned payout uses NULL |
| original_transfer_id, returned_cash_id, reissue_transfer_id | uuid nullable, actual immutable origin/receipt ancestry |

### Exact finite prefix guard

The parent validates every earlier same-worker request before using the latest basis; this query returns the invalid IDs, and any row refuses. Count and range guard remains independently mandatory: count≤100, count=latest.request_no and each number in 1..latest.request_no. Existing UNIQUE(worker,request_no) plus exact range/count establishes no missing predecessor. Do not hide a missing job through INNER JOIN.

```sql
-- Parameters below belong to the actual acceptance owner.
select q.command_id
from private.bpay_next_paye_net_request q
join private.bpay_next_command c on c.id=q.command_id
left join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=p_candidate_id
left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=p_candidate_id and j.job_kind='PAYE_NET_ENTRY'
left join private.bpay_next_net_projection p on p.request_command_id=c.id
where q.run_worker_id=p_original_worker_id and c.agency_sequence<p_cut and (
 q.candidate_id is distinct from p_candidate_id or c.command_kind<>'PAYE_NET_ENTRY'
 or c.status not in ('SEALED','COMPLETE') or c.sealed_at_utc is null or c.expected_member_count<>c.enrolled_member_count
 or c.agency_sequence<=p_original_prepare_sequence or m.candidate_id is null
 or j.id is null or (j.status,j.phase) is distinct from ('DONE'::text,'PROJECTED'::text)
 or (j.command_sequence,j.module_epoch) is distinct from (c.agency_sequence,c.module_epoch)
 or p.id is null or (p.run_worker_id,p.projection_no,p.input_kind)
   is distinct from (p_original_worker_id,q.request_no,q.input_kind)
 or q.request_no<>q.expected_projection_revision+1 or q.request_no<1 or q.request_no>p_latest_request_no
 or (q.expected_projection_revision>0 and not exists(
   select 1 from private.bpay_next_net_projection predecessor
   join private.bpay_next_command predecessor_command on predecessor_command.id=predecessor.request_command_id
   where predecessor.run_worker_id=p_original_worker_id and predecessor.projection_no=q.expected_projection_revision
     and predecessor_command.agency_sequence<c.agency_sequence and predecessor.retired_at_utc is not null))
);
```

## Complete per-week census and honest refusals

The expected original-worker census starts from run_worker/pay_run/run_command/command, not contributions. Enumerate exact Candidate PAYE originals in this pay week and actual prior original tuple order below cut. Bound complete identity set (e.g. 100) and compare exact emitted origin IDs/count with expected IDs/count. Do not filter missing contribution, absent job, pending projection, cancelled original or changed current status out of the census. All necessary current facts are prelocked before the final agency clock. Later replay uses retained children only.

Current CLAIM creates the new worker UUID at 2018:706, so at Enqueue exact paydate/created ties require a genuinely reserved actual future UUID or a finite refusal. Unknown earlier queued financial/CASE producers may truthfully refuse before acceptance under F25; their later completion cannot silently refresh accepted inputs.

Partial expiry/no contribution: existing 0510:214–221 permits genuine PREPARING/REVIEW original preparation to preserve absence, and genuine no-case READY with previously unbound floor to preserve missing fact. Do not turn either into a manufactured zero. This finite fragment refuses such affected week until a separately retained exact absence certificate closes the original command/worker/binding/job/count scope. Original READY case missing contribution always refuses. The helper supports completed-original expiry only through its bound old DONE/READY snapshot and exact release/job completion certificate.

Completed cancellation retires latest projection at original cancellation finish: 0260:451–465; case 0460:360–364. Latest retired NET therefore remains a valid economic origin only if the exact earlier DONE/CANCELLED receipt (and case binding if applicable) explains retirement at that same finish timestamp. Unknown retirement still refuses. Earlier retired NET predecessors require actual successor request ancestry; retirement timestamp alone never orders commands.

Other finite refusals: unsupported FAILED producer; ordinary returned/reissued UNBOUND floor; wrong/missing Candidate/module/member/job/projection/output scope; no complete instruction/result identity sets; any missing earlier request; too many receipts; ambiguous no-split cancellation extra transfers; contradictory destination seal/counts; arbitrary return-floor string; source rows changed after prelocking; same-tx staged-unflushed settings/query receipts under parent protocol. No late SET CONSTRAINTS flush or new business lock is added.

## Implementation review limits

This fragment is not a product PASS. Root must adapt owning ACL/successor release order and preserve all existing guards; compiler/source-gate verification belongs to root. Local source review has not executed SQL. The helper itself returns source design JSON and explicit issues; parent insert must enforce all issue/prefix/receipt/count checks and cross-row sequence inequalities. Only actual owning acceptance may invoke it, with direct grants revoked. No installed-body predecessor may be edited.

The canonical retained proof must add actual exact DRAFT and NET result identity arrays/digests, not rely on count fields alone. Reuse immutable instruction/rule/window/origin IDs; no current finance rule fallback. Original frozen gross and genuine NET output are existing economic facts; they do not authorise new financial snapshots or recalculation. Actual held/realised/funding/principal/cash conservation continues live under the existing monetary owners.

## Fresh raw source provenance

Source bytes observed for this design, not installed evidence. Root may edit concurrently; re-read before port/review.

| BUILD-BE source | Raw SHA-256 |
| --- | --- |
| `docs/bpay-next/design/F-25-week-origin-proof.md` | `c962012f66d71b6ea7a26ee555b4ec107e7870625452ae900155e4fc5c1c896f` |
| `supabase/migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| `supabase/migrations/04102026_0370_banking_pay_next_case_prepare_binding.sql` | `cae36029cf5b3a2ceef5831a2b41065b89c8160294b46dce81ef6f5da88ffe37` |
| `supabase/migrations/04102026_0380_banking_pay_next_case_net_binding.sql` | `9b750149eb86e7ddd6cff218783cbff2674c64abf3a117f20e66579d5f3d291e` |
| `supabase/migrations/04102026_0450_banking_pay_next_case_cancel_binding.sql` | `ab0db5929f3613e989080cfd1fb00acc04938e2cebdaf1e2f3c9fcc659317b9d` |
| `supabase/migrations/04102026_0480_banking_pay_next_internal_zero_execution.sql` | `032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b` |
| `supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql` | `c9758121675a070ae40f1be352a40247f357309ae94efca59baf6de002cac845` |
| `supabase/migrations/04102026_1827_banking_pay_next_candidate_destination_group.sql` | `af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d` |
| `supabase/migrations/04102026_2025_banking_pay_next_destination_lifecycle.sql` | `c36df5188713a13e5ee23796466eef4ce2227b9c0405f9958c415de8422e633d` |
| `supabase/repeatable/03102026_1630_banking_pay_next_no_case_completion_v1.sql` | `3816b38a9b0776e033abbc55fa010cf62523294070c09a9c8508e36ac2d63d9c` |
| `supabase/repeatable/04102026_0420_banking_pay_next_no_case_week_fact_v1.sql` | `335f80467b7eb434034c3aa67295355aa8af04cc9c0834e313a67b056d10766b` |
| `supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql` | `9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb` |
| `supabase/repeatable/04102026_0430_banking_pay_next_week_lifecycle_v1.sql` | `fa496325a5df42973d25eb7b467c3751847df49786442f7540106e48e2f82987` |
| `supabase/repeatable/04102026_0260_banking_pay_next_simple_cancel_v1.sql` | `d10f3468e3fa6f39bc20b9e792f1d228259e5d234d433397cf5f552b9588135f` |
| `supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql` | `4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5` |
| `supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql` | `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa` |
| `supabase/repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql` | `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
