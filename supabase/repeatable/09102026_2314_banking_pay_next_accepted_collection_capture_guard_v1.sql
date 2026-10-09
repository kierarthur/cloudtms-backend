-- F-25 exact accepted ordinary CASE terms in genuine instruction/period stage.
-- Period money UPDATE and managed WORK capture branches retain their owners.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_accepted_collection_origin_v1(
  p_accepted_input_id uuid,p_case_component_id uuid
) returns private.bpay_next_work_collection
language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  if exists(select 1 from private.bpay_next_accepted_case_input a
    join private.bpay_next_work_collection b on b.id=a.work_collection_id
    join private.bpay_next_work w on w.id=b.work_id and w.work_kind='SOURCE'
    where a.accepted_input_id=p_accepted_input_id and a.case_component_id=p_case_component_id and a.is_selected) then
    raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
  end if;
  select b.* into v_basis from private.bpay_next_accepted_case_input a
    join private.bpay_next_accepted_input input on input.id=a.accepted_input_id
      and input.case_scope='SEALED_SELECTION' and input.case_selection_revision=a.selection_revision
    join private.bpay_next_work_collection b on b.id=a.work_collection_id
      and b.case_id=a.case_id and b.case_component_id=a.case_component_id and b.candidate_id=a.candidate_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id and work.work_kind='ORDINARY'
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=b.original_revision_id and r.work_id=b.work_id and r.source_kind='ORDINARY'
    join private.bpay_next_approved_line approved on approved.id=b.original_approved_line_id
      and approved.revision_id=r.id and approved.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=a.case_id and c.candidate_id=a.candidate_id
    join private.bpay_next_case_component x on x.id=a.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=a.rule_id and rule.rule_revision=a.rule_revision
      and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    join private.bpay_next_financial_effect effect on effect.id=b.original_effect_id
    join private.bpay_next_run_line line on line.id=b.original_run_line_id
    join private.bpay_next_command command on command.id=effect.operation_id
      and command.command_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
    where a.accepted_input_id=p_accepted_input_id and a.case_component_id=p_case_component_id
      and a.is_selected and a.economic_origin_kind='WORK_COLLECTION'
      and b.id=b.case_id and b.id=b.case_component_id
      and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and a.valuation_policy_id=b.original_valuation_policy_id
      and approved.component_kind='WORK' and approved.tax_treatment='TAXABLE'
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and effect.effect_kind='PAYROLL_SETTLED' and effect.source_disposition_ex_vat>0
      and (effect.work_id,effect.component_key,effect.candidate_id,effect.original_transfer_id)
        is not distinct from (b.work_id,b.component_key,b.candidate_id,b.original_transfer_id)
      and (line.work_id,line.component_key,line.captured_revision_id,line.approved_line_id,line.source_pay_channel,line.valuation_policy_id)
        is not distinct from (b.work_id,b.component_key,b.original_revision_id,b.original_approved_line_id,b.original_source_pay_channel,b.original_valuation_policy_id)
      and line.target_pay_channel='PAYE'
      and (effect.source_disposition_ex_vat,effect.target_amount_ex_vat,effect.target_amount_vat,effect.target_amount_inc_vat)
        is not distinct from (line.source_consumed_ex_vat,line.frozen_ex_vat,line.frozen_vat,line.frozen_inc_vat)
      and exists(select 1 from private.bpay_next_hold h where h.id=effect.operation_item_id
        and h.run_line_id=line.id and h.status='REALISED' and h.source_reserved_ex_vat=effect.source_disposition_ex_vat
        and h.source_reserved_ex_vat=line.source_consumed_ex_vat)
      and exists(select 1 from private.bpay_next_transfer_member m where m.transfer_id=b.original_transfer_id
        and m.run_line_id=line.id and m.subject_kind='WORK' and m.signed_cash_contribution=line.frozen_inc_vat)
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and (x.case_kind,x.case_subtype,x.tax_treatment,x.source_pay_channel,x.instruction_kind,x.direction,x.payroll_stage,x.component_key,x.component_ordinal)
        is not distinct from ('OVERPAYMENT','OVERPAYMENT','TAXABLE','PAYE','RECOVERY','DEDUCTION','GROSS_DEDUCT','RECOVERY',1::bigint)
      and rule.case_kind='OVERPAYMENT' and rule.weekly_due_source_ex_vat is null
      and p.source_basis_channel='PAYE' and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;

create or replace function private.bpay_next_work_collection_capture_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;v_parent_scope record;
begin
  if tg_table_name='bpay_next_case_period' then
    if tg_op='UPDATE' then
      if new.work_collection_id is distinct from old.work_collection_id then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_IMMUTABLE';
      end if;
      if new.work_collection_id is not null and
         ((new.case_component_id,new.case_id,new.candidate_id,new.pay_week_start,new.rule_id,
           new.opening_outstanding_source_ex_vat,new.opening_due_source_ex_vat)
           is distinct from (old.case_component_id,old.case_id,old.candidate_id,old.pay_week_start,old.rule_id,
             old.opening_outstanding_source_ex_vat,old.opening_due_source_ex_vat)
          or new.period_revision<>old.period_revision+1
          or new.realised_recovery_source_ex_vat<old.realised_recovery_source_ex_vat
          or new.active_payout_source_ex_vat<>0
          or not exists(select 1 from private.bpay_next_case_component x
            join private.bpay_next_finance_case c on c.id=x.case_id and c.candidate_id=x.candidate_id
            where x.id=new.case_component_id and x.id=new.work_collection_id
              and x.case_id=new.case_id and x.candidate_id=new.candidate_id
              and x.recovered_source_ex_vat>=new.realised_recovery_source_ex_vat
              and x.active_recovery_source_ex_vat>=new.active_unrealised_recovery_source_ex_vat
              and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
                is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat))) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_COUNTER_INVALID';
      end if;
      return new;
    end if;
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
select input.id as accepted_input_id, state.run_id,state.run_worker_id,state.preparation_revision,state.selection_revision,
       child.rule_id, child.rule_revision, child.valuation_policy_id,
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
    v_basis:=private.bpay_next_accepted_collection_origin_v1(v_parent_scope.accepted_input_id,new.case_component_id);
    if v_basis.id is distinct from new.work_collection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_case_instruction' then
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
select input.id as accepted_input_id, state.run_id,state.run_worker_id,state.preparation_revision,state.selection_revision,
       child.rule_id, child.rule_revision, child.valuation_policy_id,
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
    if (v_parent_scope.run_id,v_parent_scope.run_worker_id,v_parent_scope.preparation_revision,v_parent_scope.selection_revision,
        v_parent_scope.rule_revision,v_parent_scope.valuation_policy_id) is distinct from
      (new.run_id,new.run_worker_id,new.preparation_revision,new.selection_revision,new.rule_revision,new.valuation_policy_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_ACCEPTED_INSTRUCTION_SCOPE_INVALID';
    end if;
    v_basis:=private.bpay_next_accepted_collection_origin_v1(v_parent_scope.accepted_input_id,new.case_component_id);
    if v_basis.id is distinct from new.work_collection_id
       or new.valuation_policy_id is distinct from v_basis.original_valuation_policy_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_op<>'INSERT' then
    if tg_op<>'UPDATE' or old.work_collection_id is null or old.managed_capture_completed
       or not new.managed_capture_completed
       or (pg_catalog.to_jsonb(new)-'managed_capture_completed')
         is distinct from (pg_catalog.to_jsonb(old)-'managed_capture_completed') then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_IMMUTABLE';
    end if;
  elsif new.work_collection_id is null then
    return new;
  else
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
    v_basis:=private.bpay_next_work_collection_origin_v1(v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id);
    if v_basis.id is distinct from new.work_collection_id or new.managed_capture_completed
       or new.work_collection_revision is distinct from v_basis.reconcile_revision
       or (new.captured_revision_id,new.approved_source_ex_vat,new.realised_source_ex_vat)
         is distinct from (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat)
       or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
       or (new.held_source_ex_vat,new.held_target_ex_vat,new.held_target_vat,new.held_target_inc_vat)
         is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ORIGIN_INVALID';
    end if;
  end if;
  if not exists(select 1 from private.bpay_next_run_work rw
    join private.bpay_next_run_worker w on w.id=rw.run_worker_id and w.status='PREPARING'
    join private.bpay_next_run_command rc on rc.run_id=w.run_id
    join private.bpay_next_job j on j.command_id=rc.command_id and j.candidate_id=w.candidate_id
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where rw.id=new.run_work_id and rw.run_worker_id=new.run_worker_id and rw.work_id=new.work_id
      and rw.captured_revision_id=new.captured_revision_id and j.job_kind='PREPARE' and j.status='LEASED'
      and j.phase=case when tg_op='INSERT' then 'RESERVE' else 'ALLOCATE' end
      and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_COLLECTION_CAPTURE_OWNER_STALE';
  end if;
  return new;
end
$function$;

alter function private.bpay_next_work_collection_capture_guard_v1() owner to postgres;
alter function private.bpay_next_accepted_collection_origin_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_accepted_collection_origin_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_work_collection_capture_guard_v1() from public,anon,authenticated,service_role;
commit;
