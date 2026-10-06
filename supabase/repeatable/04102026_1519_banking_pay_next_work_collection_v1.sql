-- Current ordinary / genuine head-bound Source PAYE WORK collection and exact original recovery/cancel
-- links. No history lookup, manual schedule, forgiveness or frozen repricing.

\set ON_ERROR_STOP on

begin;

-- This point reader validates the applied/captured economic observation, not
-- WORK.current_revision_id. A later accepted approval may queue behind the
-- frozen PREPARE owner without invalidating its immutable offer.
create or replace function private.bpay_next_work_collection_origin_v1(
  p_case_id uuid,p_component_id uuid,p_candidate_id uuid
) returns private.bpay_next_work_collection
language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into v_basis from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=p_case_id and b.case_component_id=p_component_id and b.candidate_id=p_candidate_id
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.qualification_state='READY' and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((original.source_kind='SOURCE' and original.source_event_id is not null
            and (original.source_head_id is null or original.source_event_id=original.source_head_id)) or (original.source_kind='PROTECTED'
            and original.source_head_id is not null and original.source_event_id=original.source_head_id))
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind='WORK'
          and a.tax_treatment='TAXABLE' and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind='ORDINARY' or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment='TAXABLE'
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage='GROSS_DEDUCT' and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;

-- Captured managed rows are real typed positions; they can only transition
-- once through ALLOCATE. The separate worker counters conserve both lanes.
create or replace function private.bpay_next_work_collection_capture_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
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
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
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
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
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

create or replace function private.bpay_next_work_collection_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_DELETE_FORBIDDEN';
  end if;
  if tg_op='UPDATE' then
    if (new.id,new.work_id,new.component_key,new.candidate_id,new.original_effect_id,
        new.original_run_line_id,new.original_revision_id,new.original_approved_line_id,
        new.original_transfer_id,new.original_source_pay_channel,new.original_tax_treatment,
        new.original_valuation_policy_id,new.created_at_utc)
       is distinct from
       (old.id,old.work_id,old.component_key,old.candidate_id,old.original_effect_id,
        old.original_run_line_id,old.original_revision_id,old.original_approved_line_id,
        old.original_transfer_id,old.original_source_pay_channel,old.original_tax_treatment,
        old.original_valuation_policy_id,old.created_at_utc)
       or (old.case_id is not null and (new.case_id,new.case_component_id)
           is distinct from (old.case_id,old.case_component_id))
       or (not old.paid_basis_qualified and new.paid_basis_qualified)
       or new.reconcile_revision<>old.reconcile_revision+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BASIS_IMMUTABLE';
    end if;
  elsif new.reconcile_revision<>1 or new.case_id is not null
      or new.last_effect_id is distinct from new.original_effect_id then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_STATE_INVALID';
  end if;
  select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.original_run_line_id;
  if tg_op='INSERT' and not exists(select 1 from private.bpay_next_job j
      where j.id=new.last_job_id and j.command_id=v_effect.operation_id
        and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_OWNER_INVALID';
  end if;
  if v_effect.effect_kind<>'PAYROLL_SETTLED' or v_effect.source_disposition_ex_vat<=0
     or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id,v_effect.original_transfer_id)
       is distinct from (new.work_id,new.component_key,new.candidate_id,new.original_transfer_id)
     or (v_line.work_id,v_line.component_key,v_line.captured_revision_id,v_line.approved_line_id,
         v_line.source_pay_channel,v_line.valuation_policy_id)
       is distinct from (new.work_id,new.component_key,new.original_revision_id,new.original_approved_line_id,
         new.original_source_pay_channel,new.original_valuation_policy_id)
     or v_line.target_pay_channel<>'PAYE'
     or (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
       is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
     or not exists(select 1 from private.bpay_next_hold h
       where h.id=v_effect.operation_item_id and h.run_line_id=v_line.id and h.status='REALISED'
         and h.source_reserved_ex_vat=v_effect.source_disposition_ex_vat
         and h.source_reserved_ex_vat=v_line.source_consumed_ex_vat)
     or not exists(select 1 from private.bpay_next_approved_line a
       join private.bpay_next_work_revision r on r.id=a.revision_id
       join private.bpay_next_work w on w.id=r.work_id
       where a.id=new.original_approved_line_id and a.component_kind='WORK'
         and a.tax_treatment is not distinct from new.original_tax_treatment
         and w.id=new.work_id and a.component_key=new.component_key
         and ((r.source_kind='ORDINARY' and w.work_kind='ORDINARY')
           or (w.work_kind='SOURCE' and a.source_component_id is not null
             and a.component_key='SOURCE:'||a.source_component_id::text
             and ((r.source_kind='SOURCE' and r.source_event_id is not null
               and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
               and r.source_head_id is not null and r.source_event_id=r.source_head_id)))))
     or not exists(select 1 from private.bpay_next_transfer_member m
       where m.transfer_id=new.original_transfer_id and m.run_line_id=v_line.id and m.subject_kind='WORK'
         and m.signed_cash_contribution=v_line.frozen_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_ORIGINAL_BASIS_INVALID';
  end if;
  if new.case_id is not null and
     (new.case_id<>new.id or new.case_component_id<>new.id or not exists(
       select 1 from private.bpay_next_finance_case c
       join private.bpay_next_case_component x on x.id=new.case_component_id and x.case_id=c.id
       join private.bpay_next_case_rule r on r.id=x.rule_id and r.case_id=c.id
       where c.id=new.case_id and c.candidate_id=new.candidate_id
         and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
         and c.principal_approved=x.approved_source_ex_vat
         and c.principal_recovered=x.recovered_source_ex_vat
         and c.principal_written_off=x.written_off_source_ex_vat
         and c.active_recovery_hold_amount=x.active_recovery_source_ex_vat
         and x.candidate_id=c.candidate_id and x.component_key='RECOVERY' and x.component_ordinal=1
         and x.case_kind='OVERPAYMENT' and x.tax_treatment='TAXABLE' and x.source_pay_channel='PAYE'
         and x.instruction_kind='RECOVERY' and x.payroll_stage='GROSS_DEDUCT'
         and c.current_rule_id=r.id and c.current_rule_revision=r.rule_revision
         and r.id=new.id and r.case_kind='OVERPAYMENT' and r.weekly_due_source_ex_vat is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_OWNER_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_job j
     join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
     join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
     where j.id=new.last_job_id and j.candidate_id=new.candidate_id
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY')
       and j.status='LEASED' and j.module_epoch=m.owner_epoch
       and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()
       and ((j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null
         and exists(select 1 from private.bpay_next_publication p where p.command_id=j.command_id
           and p.work_id=new.work_id and p.candidate_id=new.candidate_id
           and p.revision_id=new.observed_revision_id and p.status in ('QUEUED','APPLYING')))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_outcome_request r on r.command_id=j.command_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='PAYROLL_SETTLED'
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=r.transfer_id and e.source_disposition_ex_vat>0))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_case_event ce on ce.id=e.operation_item_id and ce.operation_id=j.command_id
             join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
             join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ce.original_transfer_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='WORK_COLLECTION_RECOVERED'
               and ce.event_kind='RECOVERED' and ce.case_id=new.case_id and ce.case_component_id=new.case_component_id
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=ce.original_transfer_id
               and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
                 is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)))
         or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_expired_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='SIMPLE_CANCEL' and j.phase='RELEASE' and new.last_effect_id is null
           and exists(select 1 from private.bpay_next_case_cancel_binding cb
             join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
             join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
             join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
             join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
             where cb.command_id=j.command_id and cb.candidate_id=new.candidate_id
               and cb.status='CANCELLING' and cb.stage='CASE'
               and i.case_id=new.case_id and i.case_component_id=new.case_component_id)))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  if not exists(select 1 from private.bpay_next_position p
      where p.work_id=new.work_id and p.component_key=new.component_key
        and (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
          is not distinct from (new.observed_revision_id,new.observed_approved_source_ex_vat,new.observed_realised_source_ex_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_OBSERVATION_INVALID';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_work_collection_case_identity_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
begin
  if tg_table_name='bpay_next_finance_case' then
    if exists(select 1 from private.bpay_next_work_collection where case_id=old.id) and
       (new.id,new.candidate_id,new.case_kind,new.tax_treatment,new.current_rule_id,new.current_rule_revision)
         is distinct from (old.id,old.candidate_id,old.case_kind,old.tax_treatment,old.current_rule_id,old.current_rule_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_IDENTITY_IMMUTABLE';
    end if;
  elsif exists(select 1 from private.bpay_next_work_collection where case_component_id=old.id) and
     (new.id,new.case_id,new.candidate_id,new.component_key,new.component_ordinal,new.rule_id,
       new.case_kind,new.case_subtype,new.tax_treatment,new.instruction_kind,new.direction,
       new.payroll_stage,new.source_pay_channel,new.currency)
       is distinct from
     (old.id,old.case_id,old.candidate_id,old.component_key,old.component_ordinal,old.rule_id,
       old.case_kind,old.case_subtype,old.tax_treatment,old.instruction_kind,old.direction,
       old.payroll_stage,old.source_pay_channel,old.currency) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_COMPONENT_IDENTITY_IMMUTABLE';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_principal numeric;v_delta numeric;v_event text;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind='WORK'
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and v_approved.tax_treatment is not distinct from 'TAXABLE';
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null or not v_capturable then return;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id)
      values(p_work_id,p_component_key,v_candidate,p_effect_id,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        v_approved.tax_treatment is not distinct from 'TAXABLE'
          and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not v_supported_paid then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is distinct from 'TAXABLE' then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind='WORK' and a.tax_treatment='TAXABLE'
            and (v_work.work_kind='ORDINARY'
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_state='READY' then
    if v_basis.case_id is null then
      v_principal:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0);
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT','TAXABLE','OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT','TAXABLE',pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT','TAXABLE','RECOVERY','DEDUCTION','GROSS_DEDUCT','PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0);
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
end
$function$;

-- 0410 has already posted exactly this frozen CASE result and changed its
-- hold/use to REALISED. Apply its single negative SOURCE disposition to the
-- linked original WORK; no bank outcome/return is itself a recovery.
create or replace function private.bpay_next_apply_work_collection_recovery_v1(p_job_id uuid,p_case_event_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_event private.bpay_next_case_event%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_effect_id uuid;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  select * into strict v_event from private.bpay_next_case_event where id=p_case_event_id;
  select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_event.instruction_id;
  select * into strict v_basis from private.bpay_next_work_collection where id=v_instruction.work_collection_id;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.phase<>'MEMBERS' or v_job.status<>'LEASED'
     or v_job.candidate_id<>v_basis.candidate_id or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_event.operation_id,v_event.case_id,v_event.case_component_id,v_event.candidate_id)
       is distinct from (v_job.command_id,v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id)
     or v_event.event_kind<>'RECOVERED' or v_event.source_amount_ex_vat<=0
     or v_instruction.valuation_policy_id<>v_basis.original_valuation_policy_id
     or (v_event.source_amount_ex_vat,v_event.target_amount_ex_vat,v_event.target_amount_vat,v_event.target_amount_inc_vat)
       is distinct from (v_event.recovered_delta,v_event.recovered_delta,0::numeric,v_event.recovered_delta)
     or not exists(select 1 from private.bpay_next_module_control m
       join private.bpay_next_worker_control c on c.candidate_id=v_basis.candidate_id
       where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_job.module_epoch and c.active_owner_epoch=v_job.owner_epoch)
     or not exists(select 1 from private.bpay_next_case_hold h
       join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=v_event.id
       join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=v_event.original_transfer_id
       join private.bpay_next_transfer_member member on member.transfer_id=r.transfer_id and member.case_hold_id=h.id
         and member.case_instruction_id=v_instruction.id and member.case_allocation_result_id=v_event.allocation_result_id
       where h.id=v_event.operation_item_id and h.status='REALISED' and h.instruction_id=v_instruction.id
         and h.source_reserved_ex_vat=v_event.source_amount_ex_vat and h.run_worker_id=r.run_worker_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
  end if;
  insert into private.bpay_next_financial_effect(operation_id,operation_item_id,effect_kind,candidate_id,
    work_id,component_key,case_id,original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc)
    select v_job.command_id,v_event.id,'WORK_COLLECTION_RECOVERED',v_basis.candidate_id,
      v_basis.work_id,v_basis.component_key,v_basis.case_id,w.original_timesheet_id,v_event.original_transfer_id,
      -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat,v_event.occurred_at_utc
      from private.bpay_next_work w where w.id=v_basis.work_id
    on conflict(operation_id,operation_item_id,effect_kind) do nothing returning id into v_effect_id;
  if v_effect_id is null then
    select * into strict v_effect from private.bpay_next_financial_effect where operation_id=v_job.command_id
      and operation_item_id=v_event.id and effect_kind='WORK_COLLECTION_RECOVERED';
    if (v_effect.candidate_id,v_effect.work_id,v_effect.component_key,v_effect.case_id,v_effect.original_transfer_id,
        v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
      is distinct from (v_basis.candidate_id,v_basis.work_id,v_basis.component_key,v_basis.case_id,v_event.original_transfer_id,
        -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_REPLAY_CONFLICT';
    end if;
    return;
  end if;
  update private.bpay_next_position set realised_source_ex_vat=realised_source_ex_vat-v_event.source_amount_ex_vat,
    realised_target_ex_vat=realised_target_ex_vat-v_event.target_amount_ex_vat,
    realised_target_vat=realised_target_vat-v_event.target_amount_vat,
    realised_target_inc_vat=realised_target_inc_vat-v_event.target_amount_inc_vat,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where work_id=v_basis.work_id and component_key=v_basis.component_key and source_basis_channel='PAYE'
      and realised_source_ex_vat>=v_event.source_amount_ex_vat
      and (realised_target_ex_vat,realised_target_vat,realised_target_inc_vat)
        is not distinct from (realised_source_ex_vat,0::numeric,realised_source_ex_vat);
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_POSITION_INVALID';end if;
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,v_effect_id);
end
$function$;

create or replace function private.bpay_next_reconcile_cancelled_work_collection_v1(p_job_id uuid,p_case_hold_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into strict v_basis from private.bpay_next_case_hold h
    join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
    join private.bpay_next_work_collection b on b.id=i.work_collection_id and b.case_id=h.case_id and b.case_component_id=h.case_component_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
    join private.bpay_next_case_cancel_binding cb on cb.run_worker_id=h.run_worker_id and cb.command_id=(select command_id from private.bpay_next_job where id=p_job_id)
    where h.id=p_case_hold_id and h.status='RELEASED' and cb.status='CANCELLING' and cb.stage='CASE';
  -- Cancellation is not a financial disposition. Reconcile only this exact
  -- released linked hold; R and all frozen CSV/advice values stay unchanged.
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,null);
end
$function$;

drop trigger if exists bpay_next_work_collection_guard_v1 on private.bpay_next_work_collection;
create trigger bpay_next_work_collection_guard_v1 before insert or update or delete
on private.bpay_next_work_collection for each row execute function private.bpay_next_work_collection_guard_v1();
drop trigger if exists bpay_next_work_collection_truncate_guard_v1 on private.bpay_next_work_collection;
create trigger bpay_next_work_collection_truncate_guard_v1 before truncate
on private.bpay_next_work_collection for each statement execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_work_collection_case_identity_guard_v1 on private.bpay_next_finance_case;
create trigger bpay_next_work_collection_case_identity_guard_v1 before update
on private.bpay_next_finance_case for each row execute function private.bpay_next_work_collection_case_identity_guard_v1();
drop trigger if exists bpay_next_work_collection_component_identity_guard_v1 on private.bpay_next_case_component;
create trigger bpay_next_work_collection_component_identity_guard_v1 before update
on private.bpay_next_case_component for each row execute function private.bpay_next_work_collection_case_identity_guard_v1();

drop trigger if exists bpay_next_work_collection_capture_guard_v1 on private.bpay_next_run_position;
create trigger bpay_next_work_collection_capture_guard_v1 before insert or update or delete
on private.bpay_next_run_position for each row execute function private.bpay_next_work_collection_capture_guard_v1();
drop trigger if exists bpay_next_work_collection_instruction_guard_v1 on private.bpay_next_run_case_instruction;
create trigger bpay_next_work_collection_instruction_guard_v1 before insert
on private.bpay_next_run_case_instruction for each row execute function private.bpay_next_work_collection_capture_guard_v1();
drop trigger if exists bpay_next_work_collection_period_guard_v1 on private.bpay_next_case_period;
create trigger bpay_next_work_collection_period_guard_v1 before insert or update
on private.bpay_next_case_period for each row execute function private.bpay_next_work_collection_capture_guard_v1();

alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
alter function private.bpay_next_work_collection_case_identity_guard_v1() owner to postgres;
alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
alter function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_work_collection_capture_guard_v1() owner to postgres;
alter function private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_reconcile_cancelled_work_collection_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1(),
  private.bpay_next_work_collection_case_identity_guard_v1(),
  private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid),
  private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid),private.bpay_next_work_collection_capture_guard_v1(),
  private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid),private.bpay_next_reconcile_cancelled_work_collection_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
