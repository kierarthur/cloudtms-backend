-- WB-29 / BD155: native Source collection approval identity only.
-- Actual paid/observed/Position/job identities and every money formula retained.
\set ON_ERROR_STOP on
begin;

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
    join private.bpay_next_work_revision source_original on source_original.work_id=b.work_id and source_original.id=(case when original.sealed_at_utc is not null and original.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(original.id) else original.id end)
    join private.bpay_next_work_revision source_current on source_current.work_id=b.work_id and source_current.id=(case when r.sealed_at_utc is not null and r.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(r.id) else r.id end)
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=p_case_id and b.case_component_id=p_component_id and b.candidate_id=p_candidate_id
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.collection_mode='AUTOMATIC' and b.qualification_state='READY' and b.original_source_pay_channel='PAYE'
      and b.original_tax_treatment is not null and b.original_tax_treatment in ('TAXABLE','NON_TAXABLE')
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((source_original.source_kind='SOURCE' and source_original.source_event_id is not null
            and (source_original.source_head_id is null or source_original.source_event_id=source_original.source_head_id)) or (source_original.source_kind='PROTECTED'
            and source_original.source_head_id is not null and source_original.source_event_id=source_original.source_head_id))
          and ((source_current.source_kind='SOURCE' and source_current.source_event_id is not null
            and (source_current.source_head_id is null or source_current.source_event_id=source_current.source_head_id)) or (source_current.source_kind='PROTECTED'
            and source_current.source_head_id is not null and source_current.source_event_id=source_current.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind=original_line.component_kind
          and a.tax_treatment=original_line.tax_treatment and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind in ('ORDINARY','EXPENSE') or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment=b.original_tax_treatment and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment=b.original_tax_treatment
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage=case when b.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null and rule.tax_treatment=b.original_tax_treatment
      and ((original.source_kind in ('ORDINARY','EXPENSE') and ((original_line.component_kind in ('WORK','ADDITIONAL') and original_line.tax_treatment='TAXABLE') or (original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')))
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK' and original_line.tax_treatment='TAXABLE'))
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;


alter function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_case_allocation_plan_v1(p_scope jsonb)
returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private,public
as $function$
declare
 v_mode text;v_candidate uuid;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
 v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_period_rule private.bpay_next_case_rule%rowtype;
 v_origin private.bpay_next_case_create_request%rowtype;v_collection private.bpay_next_work_collection%rowtype;
 v_state private.bpay_next_case_allocation_state%rowtype;v_worker private.bpay_next_run_worker%rowtype;
 v_period private.bpay_next_case_period%rowtype;v_value record;v_row record;
 v_restructure jsonb;v_period_restructure jsonb;
 v_amendment jsonb;v_old_amendment jsonb;v_period_before jsonb;v_approved_principal numeric;
 v_instruction private.bpay_next_run_case_instruction%rowtype;v_instruction_json jsonb;v_rows jsonb:='[]';
 v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_status text;v_case_due date;
 v_automatic boolean;v_explanation text;v_outstanding numeric;v_case_outstanding numeric;
 v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_purpose text;v_age timestamptz;v_age_key bigint;
 v_period_operation text;v_resolution_ref text;v_e numeric;v_h numeric;v_taken numeric;v_next_e numeric;v_next_h numeric;
 v_foreign_period numeric;v_foreign_component numeric;v_foreign_case numeric;v_without_foreign numeric;v_foreign_reduction numeric;v_foreign_reason text;
 v_cap_reason text;v_affordability_reason text;v_capacity_reason text;v_keys text[];
begin
 if jsonb_typeof(p_scope) is distinct from 'object' then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
 v_mode:=p_scope->>'mode';
 if v_mode='DERIVE' then
  v_keys:=array['mode','candidate_id','case_id','case_component_id','pay_date','pay_week_start',
   'target_pay_channel','captured_default_floor','captured_default_floor_revision','weekly_binding_state','accepted_run_id'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_candidate:=(p_scope->>'candidate_id')::uuid;v_run_id:=(p_scope->>'accepted_run_id')::uuid;
  v_run.pay_date:=(p_scope->>'pay_date')::date;
  v_state.pay_week_start:=(p_scope->>'pay_week_start')::date;
  v_worker.target_pay_channel:=p_scope->>'target_pay_channel';
  v_state.captured_default_floor:=(p_scope->>'captured_default_floor')::numeric;
  v_state.captured_default_floor_revision:=(p_scope->>'captured_default_floor_revision')::bigint;
  v_state.weekly_binding_state:=p_scope->>'weekly_binding_state';
  if v_candidate is null or v_run.pay_date is null or not isfinite(v_run.pay_date)
   or v_state.pay_week_start is distinct from v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1)
   or v_state.captured_default_floor is null or v_state.captured_default_floor<0
   or v_state.captured_default_floor::text in ('NaN','Infinity','-Infinity')
   or v_state.captured_default_floor<>trunc(v_state.captured_default_floor,2)
   or coalesce(v_state.captured_default_floor_revision,0)<1
   or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if v_run_id is not null then
   select r.accepted_choice_descriptor into strict v_run.accepted_choice_descriptor from private.bpay_next_pay_run r
    where r.id=v_run_id and r.pay_date=v_run.pay_date and r.selection_state='SEALED';
  end if;
  select c.* into strict v_case from private.bpay_next_finance_case c
   where c.id=(p_scope->>'case_id')::uuid and c.candidate_id=v_candidate;
  select c.* into strict v_component from private.bpay_next_case_component c
   where c.id=(p_scope->>'case_component_id')::uuid and c.case_id=v_case.id and c.candidate_id=v_candidate;
        -- WB26: immutable rule/control choice from this original accepted cut.
        -- Balances below are still live and protected by the original money locks.
        v_memory_cut:=v_run_id is not null and v_run.accepted_choice_descriptor is not null
          and v_run.accepted_choice_descriptor ? 'memory_baseline_run_id';
        v_case_cut:=null;v_case_status:=v_case.status;v_case_due:=v_case.due_date;
        if v_memory_cut then
          select retained.basis into v_case_cut from private.bpay_next_selection_mode_memory retained
            where retained.run_id=v_run_id and retained.row_kind='LEAF'
              and retained.leaf_key='CASE:'||v_component.id::text
              and retained.candidate_id=v_candidate and retained.case_component_id=v_component.id;
          if v_case_cut is null or not(v_case_cut ?& array['case_id','case_component_id','case_revision',
              'case_state','case_due_date','case_kind','case_subtype','case_tax_treatment','case_instruction_kind',
              'case_component_ordinal','case_control_sequence','case_component_revision','rule_id','case_rule_revision'])
            or (v_case_cut->>'case_id',v_case_cut->>'case_component_id',v_case_cut->>'case_kind',v_case_cut->>'case_subtype',
                v_case_cut->>'case_tax_treatment',v_case_cut->>'case_instruction_kind',v_case_cut->>'direction',
                v_case_cut->>'payroll_stage',v_case_cut->>'source_pay_channel',v_case_cut->>'case_component_ordinal')
              is distinct from (v_case.id::text,v_component.id::text,v_component.case_kind,v_component.case_subtype,
                v_component.tax_treatment,v_component.instruction_kind,v_component.direction,v_component.payroll_stage,
                v_component.source_pay_channel,v_component.component_ordinal::text)
            or coalesce((v_case_cut->>'case_revision')::bigint,0)<1 or coalesce((v_case_cut->>'case_component_revision')::bigint,0)<1
            or v_case_cut->>'case_state' is distinct from 'OPEN'
            or v_case_cut->>'rule_id' is null or coalesce((v_case_cut->>'case_rule_revision')::bigint,0)<1
            or coalesce((v_case_cut->>'case_control_sequence')::bigint,-1)<0 then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          v_case_status:=v_case_cut->>'case_state';v_case_due:=(v_case_cut->>'case_due_date')::date;
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=(v_case_cut->>'rule_id')::uuid
            and r.case_id=v_case.id and r.candidate_id=v_candidate
            and r.rule_revision=(v_case_cut->>'case_rule_revision')::bigint
            and (r.case_kind,r.case_subtype,r.tax_treatment)
              is not distinct from (v_component.case_kind,v_component.case_subtype,v_component.tax_treatment);
        else
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id
            and r.case_id=v_case.id and r.candidate_id=v_candidate and r.rule_revision=v_case.current_rule_revision;
        end if;
        -- FC13 / BD148: an accepted resolution belonging to this exact native
        -- cut must never fall through to the old automatic full-source quote.
        -- CM13a owns the actual decision consumer. Earlier accepted cuts remain
        -- on their earlier rule and price. No new cut/protocol or money formula.
        if exists(select 1 from private.bpay_next_case_channel_component d
          join private.bpay_next_case_channel_request q on q.command_id=d.command_id
          join private.bpay_next_case_channel_outcome o on o.command_id=q.command_id and o.outcome='APPLIED'
          join private.bpay_next_command cmd on cmd.id=q.command_id and cmd.command_kind='CASE_CHANNEL_RESOLVE'
            and cmd.status='COMPLETE' and cmd.module_epoch=q.module_epoch and cmd.agency_sequence=q.command_sequence
          join private.bpay_next_job j on j.id=o.job_id and j.command_id=q.command_id
            and j.candidate_id=v_candidate and j.job_kind='CASE_CHANNEL_RESOLVE' and j.status='DONE' and j.phase='DONE'
            and j.module_epoch=q.module_epoch and j.command_sequence=q.command_sequence
          where d.case_id=v_case.id and d.case_component_id=v_component.id and d.candidate_id=v_candidate
            and o.applied_case_revision<=case when v_memory_cut then(v_case_cut->>'case_revision')::bigint else v_case.case_revision end
            and d.applied_component_revision<=case when v_memory_cut then(v_case_cut->>'case_component_revision')::bigint else v_component.component_revision end) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        select r.* into v_origin from private.bpay_next_case_create_request r where r.case_id=v_case.id and r.candidate_id=v_candidate;
        -- Refuse the immutable selected identity before qualification: a stale
        -- Source collection must not disappear into an ordinary review path.
        if exists(select 1 from private.bpay_next_work_collection c
          join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=c.candidate_id
          where c.case_id=v_case.id and c.case_component_id=v_component.id
            and c.candidate_id=v_candidate and w.work_kind='SOURCE') then
          raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
        end if;
        -- The exact original read qualification, in this STABLE snapshot.
  select b.* into v_collection from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work_revision source_original on source_original.work_id=b.work_id and source_original.id=(case when original.sealed_at_utc is not null and original.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(original.id) else original.id end)
    join private.bpay_next_work_revision source_current on source_current.work_id=b.work_id and source_current.id=(case when r.sealed_at_utc is not null and r.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(r.id) else r.id end)
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=v_case.id and b.case_component_id=v_component.id and b.candidate_id=v_candidate
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.collection_mode='AUTOMATIC' and b.qualification_state='READY' and b.original_source_pay_channel='PAYE'
      and b.original_tax_treatment is not null and b.original_tax_treatment in ('TAXABLE','NON_TAXABLE')
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((source_original.source_kind='SOURCE' and source_original.source_event_id is not null
            and (source_original.source_head_id is null or source_original.source_event_id=source_original.source_head_id)) or (source_original.source_kind='PROTECTED'
            and source_original.source_head_id is not null and source_original.source_event_id=source_original.source_head_id))
          and ((source_current.source_kind='SOURCE' and source_current.source_event_id is not null
            and (source_current.source_head_id is null or source_current.source_event_id=source_current.source_head_id)) or (source_current.source_kind='PROTECTED'
            and source_current.source_head_id is not null and source_current.source_event_id=source_current.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind=original_line.component_kind
          and a.tax_treatment=original_line.tax_treatment and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind in ('ORDINARY','EXPENSE') or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment=b.original_tax_treatment and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment=b.original_tax_treatment
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage=case when b.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null and rule.tax_treatment=b.original_tax_treatment
      and ((original.source_kind in ('ORDINARY','EXPENSE') and ((original_line.component_kind in ('WORK','ADDITIONAL') and original_line.tax_treatment='TAXABLE') or (original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')))
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK' and original_line.tax_treatment='TAXABLE'))
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);

        v_automatic:=v_collection.id is not null;
        if v_state.weekly_binding_state='NOT_AFFECTED'
          and (v_case.case_kind,v_component.case_kind,v_component.instruction_kind,v_component.payroll_stage)
            is distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND';
        end if;
        if v_rule.id is null or v_case.case_revision<1 or (not v_memory_cut and v_component.rule_id<>v_rule.id) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if not v_automatic then
          v_restructure:=private.bpay_next_case_restructure_origin_v1(v_rule.id);
          v_amendment:=private.bpay_next_case_edit_origin_v1(v_rule.id);
          v_approved_principal:=case when v_amendment is null then v_origin.principal_source_ex_vat
            else (v_amendment->>'principal')::numeric end;
          if v_amendment is not null and ((v_amendment->>'original_command_id')::uuid is distinct from v_origin.command_id
              or (v_amendment->>'case_id')::uuid is distinct from v_case.id
              or (v_amendment->>'candidate_id')::uuid is distinct from v_candidate
              or v_amendment->'input'->>'tax_treatment' is distinct from v_component.tax_treatment) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if v_automatic then
          -- Genuine paid WORK origin, not a fabricated CASE_CREATE receipt.
          -- The immutable original paid policy and the exact applied position
          -- are authority; a newer queued WORK approval is not a reprice.
          if v_origin.command_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          v_policy_id:=v_collection.original_valuation_policy_id;
        elsif v_origin.command_id is null
           or (not v_memory_cut and v_component.rule_id<>v_rule.id) or v_origin.case_kind<>v_case.case_kind
           or v_origin.source_pay_channel<>v_component.source_pay_channel
           or v_approved_principal<>v_component.approved_source_ex_vat
           or (v_component.id<>v_origin.primary_component_id
             and (v_origin.recovery_component_id is null or v_component.id<>v_origin.recovery_component_id))
           or (v_component.id=v_origin.primary_component_id and v_component.instruction_kind<>
             case when v_case.case_kind in ('LOAN','ADVANCE') then 'PAYOUT'
               when v_case.case_kind='CREDIT' then 'CREDIT' else 'RECOVERY' end)
           or (v_component.id=v_origin.recovery_component_id and v_component.instruction_kind<>'RECOVERY') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        else
          v_policy_id:=v_origin.valuation_policy_id;
        end if;
        if v_component.source_pay_channel<>'PAYE' or v_worker.target_pay_channel<>'PAYE' then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_TARGET_UNSUPPORTED';
        end if;
        v_explanation:='Exact approved case; same-channel PAYE current basis captured';
        v_outstanding:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
        if v_component.instruction_kind='RECOVERY' then
          if v_automatic then
            -- Retained automatic OVERPAYMENT policy has no instalment. Its
            -- remaining principal already excludes realised recovery and W;
            -- capacity below subtracts each disjoint active hold only once.
            v_nominal:=least(v_outstanding,v_case_outstanding);
            v_explanation:='Exact original ordinary PAYE collection; remaining outstanding, no instalment';
          else
          if v_rule.weekly_due_source_ex_vat is null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded>0
             and (v_rule.original_funding_event_id is null or v_rule.original_funded_at_utc is null) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          v_nominal:=case when v_state.pay_week_start>=v_rule.next_due_monday
            then least(v_rule.weekly_due_source_ex_vat,v_outstanding,v_case_outstanding) else 0 end;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded=0 then
            v_nominal:=0;v_explanation:='Approved loan/advance is unfunded; repayment not eligible';
          elsif v_state.pay_week_start<v_rule.next_due_monday then
            v_explanation:='Repayment schedule is not yet due in the pay-date week';
          end if;
          end if;
          v_purpose:=case when v_component.payroll_stage='GROSS_DEDUCT' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end;
        else
          v_outstanding:=v_component.approved_source_ex_vat-v_component.funded_source_ex_vat
            -v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat;
          v_case_outstanding:=v_case.principal_approved-v_case.principal_funded
            -v_case.principal_paid_credit-v_case.principal_written_off;
          v_nominal:=least(v_outstanding,v_case_outstanding);v_purpose:='PAYOUT';
        end if;
        -- Weekly due exists independently of this particular arrangement's
        -- eligibility. An early/paused selection must not make the whole
        -- week's legitimate opening due permanently zero for later pay dates.
        v_opening_due:=case when v_component.instruction_kind='RECOVERY' then v_nominal else 0 end;
        if v_case_status<>'OPEN' or (v_case_due is not null and v_case_due>v_run.pay_date) then
          v_nominal:=0;v_explanation:='Selected case is not currently open/due; no reservation';
        end if;
        if v_outstanding<0 or v_case_outstanding<0 or v_nominal<0 then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        -- Opening due is independent of later principal. Never subtract the
        -- same realised recovery twice or use max(overlapping counters).
        select p.* into v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start;
        v_period_operation:='KEEP';
        if not found then
          -- Prospective opening operands only; the writable owner persists them.
          v_period_operation:='INSERT';
          v_period.case_component_id:=v_component.id;v_period.case_id:=v_case.id;
          v_period.candidate_id:=v_candidate;v_period.pay_week_start:=v_state.pay_week_start;
          v_period.rule_id:=v_rule.id;v_period.period_revision:=1;
          v_period.opening_outstanding_source_ex_vat:=v_outstanding;
          v_period.opening_due_source_ex_vat:=v_opening_due;
          v_period.realised_recovery_source_ex_vat:=0;
          v_period.active_unrealised_recovery_source_ex_vat:=0;v_period.active_payout_source_ex_vat:=0;
          v_period.work_collection_id:=case when v_automatic then v_collection.id else null end;
        end if;
        if v_period.work_collection_id is distinct from (case when v_automatic then v_collection.id else null end) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if v_period.rule_id<>v_rule.id then
          -- Genuine original funding advances an immutable loan rule solely
          -- to bind its age/evidence. It is NOT a schedule/policy reprice.
          select r.* into strict v_period_rule from private.bpay_next_case_rule r where r.id=v_period.rule_id;
          v_old_amendment:=case when v_period_rule.id=v_origin.rule_id then null
            else private.bpay_next_case_edit_origin_v1(v_period_rule.id) end;
          v_period_restructure:=private.bpay_next_case_restructure_origin_v1(v_period_rule.id);
          if v_restructure is not null or v_period_restructure is not null then
            if (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,v_period_rule.tax_treatment,v_period_rule.case_created_at_utc)
              is distinct from(v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,v_rule.tax_treatment,v_rule.case_created_at_utc)
              or (v_old_amendment is not null and v_old_amendment->>'original_command_id' is distinct from v_origin.command_id::text)
              or (v_period_rule.id<>v_origin.rule_id and v_old_amendment is null and v_period_rule.original_funding_event_id is null) then
              raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
            if v_restructure is not null and v_component.instruction_kind='RECOVERY'
              and v_period_rule.rule_revision<v_rule.rule_revision and v_state.pay_week_start>=v_rule.next_due_monday
              and (v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                 is not distinct from(0::numeric,0::numeric,0::numeric)
              and private.bpay_next_case_restructure_period_free_v1(v_component.id,v_state.pay_week_start,v_rule.id,v_run_id) then
              v_period_before:=to_jsonb(v_period);v_period_operation:='CASE_RESTRUCTURE_OPENING';
              v_period.rule_id:=v_rule.id;v_period.period_revision:=v_period.period_revision+1;
              v_period.opening_outstanding_source_ex_vat:=v_outstanding;v_period.opening_due_source_ex_vat:=v_opening_due;
            end if;
            -- KEEP earlier/used/protected opening and older immutable accepted cut.
          elsif v_amendment is not null and v_period_rule.original_funding_event_id is null
             and v_period_rule.rule_revision<v_rule.rule_revision
             and (v_period_rule.id=v_origin.rule_id or v_old_amendment is not null)
             and (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_created_at_utc)
                is not distinct from (v_case.id,v_candidate,v_origin.accepted_at_utc)
             and (v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric) then
            v_period_before:=to_jsonb(v_period);v_period_operation:='CASE_EDIT_OPENING';
            v_period.rule_id:=v_rule.id;v_period.period_revision:=v_period.period_revision+1;
            v_period.opening_outstanding_source_ex_vat:=v_outstanding;v_period.opening_due_source_ex_vat:=v_opening_due;
          else
          if v_case.case_kind not in ('LOAN','ADVANCE') or v_rule.original_funding_event_id is null
             or (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
               v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
               v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
               v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
               is distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
                 v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
                 v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
             or not exists(select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id
               and e.case_id=v_case.id and e.event_kind='FUNDED' and e.occurred_at_utc=v_rule.original_funded_at_utc)
             or (v_period_rule.original_funding_event_id is not null and
               (v_period_rule.original_funding_event_id,v_period_rule.original_funded_at_utc)
                 is distinct from (v_rule.original_funding_event_id,v_rule.original_funded_at_utc)) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          if v_component.instruction_kind='RECOVERY' and v_period_rule.original_funding_event_id is null then
            if (v_period.opening_outstanding_source_ex_vat,v_period.opening_due_source_ex_vat,
                v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
              raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
            end if;
            -- Only this exact zero/unconsumed pay-week is made ready. Its
            -- frozen old zero instructions/results remain immutable and zero.
            v_period_operation:='FUNDING_ZERO';
            v_period.rule_id:=v_rule.id;v_period.opening_outstanding_source_ex_vat:=v_outstanding;
            v_period.opening_due_source_ex_vat:=v_opening_due;v_period.period_revision:=v_period.period_revision+1;
          end if;
          end if; -- exact approved amendment or retained original funding-only branch
          -- A consumed funded period (or original payout period) retains its
          -- recorded opening rule and counters; only the new instruction's
          -- captured_rule_id/age advance to the genuine current funded rule.
        end if;
        v_capacity:=case when v_automatic then
          least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          when v_component.instruction_kind='RECOVERY' then
          least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat,0),
            greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_payout_hold_amount,0)) end;

        -- DR14 capture-only diagnostic. The authoritative v_capacity above,
        -- shared row allocation, nominal, principal and all money remain exact.
        -- Restore only another run's ACTIVE counters to identify the causal
        -- difference; realised amounts and this run's own holds stay deducted.
        v_foreign_reduction:=0;v_foreign_reason:=null;
        if v_run_id is not null and exists(select 1 from private.bpay_next_run_worker own
          where own.run_id=v_run_id and own.candidate_id=v_candidate and exists(select 1 from private.bpay_next_case_hold h
            where h.run_worker_id=own.id and h.case_id=v_case.id and h.status='ACTIVE')) then
          -- The actual fresh/retained INSTRUCTION_CAPTURE precedes all own
          -- ALLOCATE holds. An exceptional other caller with own holds has
          -- unknown attribution, never a guessed amount or business refusal.
          v_foreign_reduction:=null;
        elsif v_run_id is not null and v_capacity<v_nominal then
          -- With no own ACTIVE hold, these exact locked native counters are
          -- the foreign contribution. No foreign reservation/history scan.
          v_foreign_period:=case when v_component.instruction_kind='RECOVERY' then v_period.active_unrealised_recovery_source_ex_vat else 0 end;
          v_foreign_component:=case when v_component.instruction_kind='RECOVERY' then v_component.active_recovery_source_ex_vat else v_component.active_payout_source_ex_vat end;
          v_foreign_case:=case when v_component.instruction_kind='RECOVERY' then v_case.active_recovery_hold_amount else v_case.active_payout_hold_amount end;
          v_without_foreign:=case when v_automatic then
            least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_foreign_case,0))
            when v_component.instruction_kind='RECOVERY' then
            least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
              -v_period.active_unrealised_recovery_source_ex_vat+v_foreign_period,0),
              greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_foreign_case,0))
            else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_payout_hold_amount+v_foreign_case,0)) end;
          v_foreign_reduction:=greatest(v_without_foreign-v_capacity,0);
          if v_foreign_reduction>0 then
            v_foreign_reason:=case when not v_automatic and v_component.instruction_kind='RECOVERY' and v_foreign_period>0
              and greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat-v_period.active_unrealised_recovery_source_ex_vat,0)=v_capacity
              then 'SAME_WEEK_INSTALMENT' else 'CASE_BALANCE' end;
          end if;
        end if;
        begin
          select x.* into strict v_value from private.bpay_next_value_source_for_target_v1(
            v_component.approved_source_ex_vat,'PAYE','PAYE',null,v_policy_id,v_run.pay_date) x;
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING','BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS') then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          else raise;end if;
        end;
        if v_value.target_ex_vat<>v_component.approved_source_ex_vat or v_value.target_vat<>0
           or v_value.target_inc_vat<>v_value.target_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        v_resolution_ref:=case when v_automatic then 'WORK_COLLECTION:'||v_collection.id::text||':PAYE'
            when v_restructure is not null then 'CASE_RESTRUCTURE:'||(v_restructure->>'command_id')||':PAYE'
            when v_amendment is not null then 'CASE_EDIT:'||(v_amendment->>'command_id')||':PAYE'
            else 'CASE_CREATE:'||v_origin.command_id::text||':PAYE' end;
        -- WB41: current offer only. Native Create admitted Ready/selectable Case
        -- membership at its original cut; a later WAIT never reprices that offer.
        if v_run_id is null and v_automatic and private.bpay_next_work_wait_state_v1(v_collection.work_id) then
          v_nominal:=0;v_capacity:=0;v_explanation:='ORIGIN_UNAUTHORISED_WAIT';
        end if;
        v_age:=coalesce(v_rule.original_funded_at_utc,v_rule.case_created_at_utc);
        -- A genuinely bound stored credit keeps its original creator age. The
        -- exact point sidecar is immutable, not a live legacy money read.
        if not v_automatic then
          select coalesce(o.original_created_at_utc,v_age) into v_age
            from (select 1) s left join private.bpay_next_stored_credit_origin o
              on o.command_id=v_component.id and o.candidate_id=v_candidate;
        end if;
        v_age_key:=(extract(epoch from v_age)*1000000)::bigint;
  return jsonb_build_object('period_operation',v_period_operation,'period',to_jsonb(v_period),
   'period_amendment',case when v_period_operation='CASE_EDIT_OPENING' then
     jsonb_build_object('edit_command_id',v_amendment->'command_id','before',v_period_before)
     when v_period_operation='CASE_RESTRUCTURE_OPENING' then jsonb_build_object('restructure_command_id',v_restructure->'command_id','before',v_period_before) else null end,
   'resolution',jsonb_build_object('target_ex_vat',v_value.target_ex_vat,'target_vat',0,
    'target_inc_vat',v_value.target_inc_vat,'valuation_policy_id',v_policy_id,
    'valuation_window_id',v_value.policy_window_id,'resolution_ref',v_resolution_ref),
   'instruction',jsonb_build_object('case_component_id',v_component.id,
'case_id',v_case.id,
'candidate_id',v_candidate,
'captured_case_revision',v_case.case_revision,
'rule_id',v_rule.id,
'rule_revision',v_rule.rule_revision,
'case_kind',v_component.case_kind,
'case_subtype',v_component.case_subtype,
'tax_treatment',v_component.tax_treatment,
'instruction_kind',v_component.instruction_kind,
'direction',v_component.direction,
'payroll_stage',v_component.payroll_stage,
'hold_purpose',v_purpose,
'age_key',v_age_key,
'debt_age_at_utc',v_age,
'component_ordinal',v_component.component_ordinal,
'pay_week_start',v_state.pay_week_start,
'source_pay_channel','PAYE',
'target_pay_channel','PAYE',
'allocation_channel','PAYE')||
jsonb_build_object('currency','GBP',
'nominal_source_ex_vat',v_nominal,
'nominal_target_ex_vat',v_nominal,
'nominal_target_vat',0,
'nominal_target_inc_vat',v_nominal,
'capacity_source_ex_vat',v_capacity,
'capacity_target_ex_vat',v_capacity,
'capacity_target_vat',0,
'capacity_target_inc_vat',v_capacity,
'other_run_capacity_reduction_source_ex_vat',v_foreign_reduction,
'other_run_capacity_reduction_target_ex_vat',v_foreign_reduction,
'other_run_capacity_reason',v_foreign_reason,
'minimum_earnings_threshold',v_rule.minimum_earnings_threshold,
'take_home_floor_override',v_rule.take_home_floor_override,
'captured_default_floor',v_state.captured_default_floor,
'captured_default_floor_revision',v_state.captured_default_floor_revision,
'resolved_take_home_floor',coalesce(v_rule.take_home_floor_override,v_state.captured_default_floor),
'valuation_policy_id',v_policy_id,
'valuation_window_id',v_value.policy_window_id,
'resolution_ref',v_resolution_ref,
'explanation',v_explanation,
'work_collection_id',case when v_automatic then v_collection.id else null end));
 elsif v_mode='ALLOCATE' then
  v_keys:=array['mode','instructions','channel_headroom','worker_take_home'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb
   or jsonb_typeof(p_scope->'instructions') is distinct from 'array'
   or jsonb_array_length(p_scope->'instructions')>1000 then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_e:=(p_scope->>'channel_headroom')::numeric;v_h:=(p_scope->>'worker_take_home')::numeric;
  if v_e is null or v_h is null or v_e<0 or v_e::text in ('NaN','Infinity','-Infinity')
   or v_h::text in ('NaN','Infinity','-Infinity') or v_e<>trunc(v_e,2) or v_h<>trunc(v_h,2) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if exists(select 1 from jsonb_array_elements(p_scope->'instructions') x
    group by x->>'case_component_id' having count(*)>1) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_DUPLICATE';end if;
  for v_instruction in select i.* from jsonb_populate_recordset(null::private.bpay_next_run_case_instruction,p_scope->'instructions') i
   order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id loop
   if v_instruction.case_component_id is null or v_instruction.case_id is null or v_instruction.age_key is null
    or v_instruction.component_ordinal is null or v_instruction.allocation_channel is distinct from 'PAYE'
    or v_instruction.nominal_target_ex_vat is null or v_instruction.capacity_target_ex_vat is null
    or v_instruction.nominal_target_ex_vat<0 or v_instruction.capacity_target_ex_vat<0 then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_INVALID';end if;
   if v_instruction.hold_purpose='PAYOUT' then
    v_taken:=least(v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat);
    v_cap_reason:=null;v_affordability_reason:=null;
    v_capacity_reason:=case when v_instruction.capacity_target_ex_vat<v_instruction.nominal_target_ex_vat then 'CASE_CAPACITY' else null end;
    v_next_e:=v_e;v_next_h:=v_h;
   else
    select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
     v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat,v_e,v_h,
     v_instruction.minimum_earnings_threshold,v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
    v_taken:=v_row.taken_amount;v_cap_reason:=v_row.cap_reason;v_affordability_reason:=v_row.affordability_reason;
    v_capacity_reason:=v_row.capacity_reason;v_next_e:=v_row.next_channel_headroom;v_next_h:=v_row.next_worker_take_home;
   end if;
   -- Typed control annotation on the original kernel's zero allocation, never
   -- a second money formula or a persisted later live-WAIT capture.
   if v_instruction.explanation='ORIGIN_UNAUTHORISED_WAIT' then
    if v_instruction.work_collection_id is null
      or (v_instruction.case_kind,v_instruction.instruction_kind,v_instruction.direction,
          v_instruction.payroll_stage,v_instruction.hold_purpose)
        is distinct from ('OVERPAYMENT','RECOVERY','DEDUCTION',
          case when v_instruction.tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end,
          case when v_instruction.tax_treatment='TAXABLE' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end)
      or v_instruction.tax_treatment is null or v_instruction.tax_treatment not in ('TAXABLE','NON_TAXABLE')
      or v_instruction.nominal_source_ex_vat is distinct from 0::numeric
      or v_instruction.capacity_source_ex_vat is distinct from 0::numeric
      or v_instruction.nominal_target_ex_vat is distinct from 0::numeric
      or v_instruction.capacity_target_ex_vat is distinct from 0::numeric
      or v_taken is distinct from 0::numeric or v_next_e is distinct from v_e or v_next_h is distinct from v_h then
     raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_INVALID';end if;
    v_cap_reason:='ORIGIN_UNAUTHORISED_WAIT';v_affordability_reason:=null;v_capacity_reason:=null;
   end if;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('instruction',to_jsonb(v_instruction),
    'taken_amount',v_taken,'cap_reason',v_cap_reason,'affordability_reason',v_affordability_reason,'capacity_reason',v_capacity_reason,
    'channel_headroom_before',v_e,'channel_headroom_after',v_next_e,'worker_take_home_before',v_h,'worker_take_home_after',v_next_h));
   v_e:=v_next_e;v_h:=v_next_h;
  end loop;
  return jsonb_build_object('rows',v_rows,'channel_headroom_after',v_e,'worker_take_home_after',v_h);
 else raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
end
$function$;


alter function private.bpay_next_case_allocation_plan_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_case_allocation_plan_v1(jsonb) from public,anon,authenticated,service_role;

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
      or (new.last_effect_id is distinct from new.original_effect_id and not exists(select 1 from private.bpay_next_job j
        where j.id=new.last_job_id and j.job_kind='POSITION_APPLY' and new.last_effect_id is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_STATE_INVALID';
  end if;
  select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.original_run_line_id;
  if tg_op='INSERT' and not exists(select 1 from private.bpay_next_job j
      where j.id=new.last_job_id and j.command_id=v_effect.operation_id
        and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT'))
      and not exists(select 1 from private.bpay_next_job j where j.id=new.last_job_id
        and j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_OWNER_INVALID';
  end if;
  if v_effect.effect_kind<>'PAYROLL_SETTLED' or v_effect.source_disposition_ex_vat<=0
     or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id,v_effect.original_transfer_id)
       is distinct from (new.work_id,new.component_key,new.candidate_id,new.original_transfer_id)
     or (v_line.work_id,v_line.component_key,v_line.captured_revision_id,v_line.approved_line_id,
         v_line.source_pay_channel,v_line.valuation_policy_id)
       is distinct from (new.work_id,new.component_key,new.original_revision_id,new.original_approved_line_id,
         new.original_source_pay_channel,new.original_valuation_policy_id)
     or (new.collection_mode='AUTOMATIC' and (v_line.source_pay_channel<>'PAYE' or v_line.target_pay_channel<>'PAYE'))
     or (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
       is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
     or not exists(select 1 from private.bpay_next_hold h
       where h.id=v_effect.operation_item_id and h.run_line_id=v_line.id and h.status='REALISED'
         and h.source_reserved_ex_vat=v_effect.source_disposition_ex_vat
         and h.source_reserved_ex_vat=v_line.source_consumed_ex_vat)
     or not exists(select 1 from private.bpay_next_approved_line a
       join private.bpay_next_work_revision r on r.id=a.revision_id
       join private.bpay_next_work_revision source_original on source_original.work_id=r.work_id and source_original.id=(case when r.sealed_at_utc is not null and r.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(r.id) else r.id end)
       join private.bpay_next_work w on w.id=r.work_id
       where a.id=new.original_approved_line_id and a.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE','PROTECTED_WORK','SOURCE_FIXED_EXPENSE')
         and a.tax_treatment is not distinct from new.original_tax_treatment
         and w.id=new.work_id and a.component_key=new.component_key
         and ((r.source_kind='ORDINARY' and w.work_kind='ORDINARY')
           or (w.work_kind='EXPENSE' and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null
             and a.component_kind in ('EXPENSE','MILEAGE') and a.tax_treatment='NON_TAXABLE')
           or (new.collection_mode='MANUAL' and w.work_kind='SOURCE' and r.source_kind in ('SOURCE','PROTECTED'))
           or (w.work_kind='SOURCE' and a.source_component_id is not null
             and a.component_key='SOURCE:'||a.source_component_id::text
             and ((source_original.source_kind='SOURCE' and source_original.source_event_id is not null
               and (source_original.source_head_id is null or source_original.source_event_id=source_original.source_head_id)) or (source_original.source_kind='PROTECTED'
               and source_original.source_head_id is not null and source_original.source_event_id=source_original.source_head_id)))))
     or not exists(select 1 from private.bpay_next_outcome_request original_request
       join private.bpay_next_job original_job on original_job.command_id=original_request.command_id
       join private.bpay_next_transfer original_transfer on original_transfer.id=original_request.transfer_id
       where original_request.command_id=v_effect.operation_id and original_request.transfer_id=new.original_transfer_id
         and original_request.run_worker_id=v_line.run_worker_id and original_job.candidate_id=new.candidate_id
         and original_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
         and original_transfer.candidate_id=new.candidate_id and original_transfer.original_transfer_id is null
         and original_transfer.return_cash_id is null)
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
         and c.case_kind='OVERPAYMENT' and c.tax_treatment=new.original_tax_treatment and c.principal_funded=0
         and c.principal_approved=x.approved_source_ex_vat
         and c.principal_recovered=x.recovered_source_ex_vat
         and c.principal_written_off=x.written_off_source_ex_vat
         and c.active_recovery_hold_amount=x.active_recovery_source_ex_vat
         and x.candidate_id=c.candidate_id and x.component_key='RECOVERY' and x.component_ordinal=1
         and x.case_kind='OVERPAYMENT' and x.tax_treatment=new.original_tax_treatment and x.source_pay_channel='PAYE'
         and x.instruction_kind='RECOVERY' and x.payroll_stage=case when new.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end
         and c.current_rule_id=r.id and c.current_rule_revision=r.rule_revision
         and r.id=new.id and r.case_kind='OVERPAYMENT' and r.tax_treatment=new.original_tax_treatment and r.weekly_due_source_ex_vat is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_OWNER_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_job j
     join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
     join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
     where j.id=new.last_job_id and j.candidate_id=new.candidate_id
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE')
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
         or (j.job_kind='PREPARE' and new.last_effect_id is null
           and private.bpay_next_worker_released_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_expired_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_DISCARD' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_discarded_work_collection_scope_v1(j.id,new.id,null))
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
  if new.segment_attribution_complete is distinct from coalesce((private.bpay_next_segment_collection_basis_v1(
      new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
    or new.observed_segment_negative_ex_vat is distinct from (case when new.segment_attribution_complete then
      (private.bpay_next_segment_collection_basis_v1(new.work_id,new.component_key,new.observed_revision_id)->>'negative_source_ex_vat')::numeric else 0::numeric end) then
    -- The original INSERT records only its native paid anchor; the same-owner
    -- follow-up supplies the qualified observation, exactly as the predecessor.
    if tg_op<>'INSERT' or new.segment_attribution_complete or new.observed_segment_negative_ex_vat<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_SEGMENT_OBSERVATION_INVALID';
    end if;
  end if;
  if new.collection_mode='MANUAL' then
    if (new.manual_reason_code,new.manual_outstanding_source_ex_vat) is distinct from
      (case when coalesce((private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
        then 'AUTOMATIC_COLLECTION_UNAVAILABLE' else 'AMOUNT_UNBOUND' end,
       case when coalesce((private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
        then (private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'free_recovery_source_ex_vat')::numeric else null::numeric end)
      and tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_MANUAL_OBSERVATION_INVALID';end if;
  end if;
  return new;
end
$function$;


alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1() from public,anon,authenticated,service_role;

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
  v_source_current private.bpay_next_work_revision%rowtype;v_source_original private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_segment_basis jsonb;v_segment_complete boolean;
  v_principal numeric;v_delta numeric;v_event text;
  v_observation jsonb;v_mode text;v_manual_amount numeric;v_manual_reason text;v_anchor uuid;
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
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate for update;
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
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    v_capturable:=v_capturable or (v_work.work_kind='EXPENSE' and v_approved.component_kind in ('EXPENSE','MILEAGE')
      and v_approved.tax_treatment='NON_TAXABLE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
        and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        join private.bpay_next_work_revision source_captured on source_captured.work_id=r.work_id and source_captured.id=(case when r.sealed_at_utc is not null and r.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(r.id) else r.id end)
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((source_captured.source_kind='SOURCE' and source_captured.source_event_id is not null
            and (source_captured.source_head_id is null or source_captured.source_event_id=source_captured.source_head_id)) or (source_captured.source_kind='PROTECTED'
            and source_captured.source_head_id is not null and source_captured.source_event_id=source_captured.source_head_id))));
    v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
      or(v_work.work_kind in ('ORDINARY','EXPENSE') and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
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
  elsif v_job.job_kind='PREPARE' then
    if p_effect_id is not null or not exists(select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_worker_released_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_WORKER_RELEASE_SCOPE_INVALID';end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_DISCARD' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_discarded_work_collection_scope_v1(p_job_id,b.id,null)) then
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
    if p_effect_id is null then
      -- Current authorised change retains the original actual paid anchor.
      -- No business/history sweep, full-amount debt or invented payment.
      if v_position.realised_source_ex_vat<=0 then return;end if;
      select e.id into v_anchor from private.bpay_next_financial_effect e
        where e.work_id=p_work_id and e.component_key=p_component_key
          and e.effect_kind='PAYROLL_SETTLED' and e.source_disposition_ex_vat>0
        order by e.occurred_at_utc,e.id limit 1;
      if v_anchor is null then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PAID_ANCHOR_UNBOUND';end if;
      select eff.* into strict v_effect from private.bpay_next_financial_effect eff where eff.id=v_anchor;
      select l.* into strict v_line from private.bpay_next_hold h
        join private.bpay_next_run_line l on l.id=h.run_line_id
        where h.id=v_effect.operation_item_id and h.status='REALISED';
      select line.* into strict v_approved from private.bpay_next_approved_line line where line.id=v_line.approved_line_id;
      -- A lone exact original payment may qualify; mixed history never does.
      v_capturable:=v_work.work_kind='ORDINARY' and v_line.source_pay_channel='PAYE'
        and v_line.target_pay_channel='PAYE' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
      v_capturable:=v_capturable or(v_work.work_kind='EXPENSE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
        and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
          and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
      v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
        or (v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
    else v_anchor:=p_effect_id;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id,collection_mode,manual_reason_code)
      values(p_work_id,p_component_key,v_candidate,v_anchor,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        coalesce(v_supported_paid,false) and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id,
        case when v_capturable then 'AUTOMATIC' else 'MANUAL' end,
        case when not coalesce(v_capturable,false) then 'AMOUNT_UNBOUND' end) returning * into v_basis;
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
  if p_effect_id is not null and not coalesce(v_supported_paid,false) then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select source.* into strict v_source_current from private.bpay_next_work_revision source
    where source.work_id=p_work_id and source.id=(case when v_revision.sealed_at_utc is not null and v_revision.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(v_revision.id) else v_revision.id end);
  select source.* into strict v_source_original from private.bpay_next_work_revision source
    where source.work_id=p_work_id and source.id=(case when v_original_revision.sealed_at_utc is not null and v_original_revision.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(v_original_revision.id) else v_original_revision.id end);
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or(v_work.work_kind='EXPENSE' and v_original_revision.source_kind='EXPENSE' and v_revision.source_kind='EXPENSE'
      and v_original_revision.financial_snapshot_id is not null and v_revision.financial_snapshot_id is not null
      and v_original_approved.component_kind in ('EXPENSE','MILEAGE') and v_original_approved.tax_treatment='NON_TAXABLE')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_source_original.source_kind='SOURCE' and v_source_original.source_event_id is not null
        and (v_source_original.source_head_id is null
        or v_source_original.source_event_id=v_source_original.source_head_id)) or (v_source_original.source_kind='PROTECTED'
        and v_source_original.source_head_id is not null
        and v_source_original.source_event_id=v_source_original.source_head_id))
      and ((v_source_current.source_kind='SOURCE' and v_source_current.source_event_id is not null
        and (v_source_current.source_head_id is null
        or v_source_current.source_event_id=v_source_current.source_head_id)) or (v_source_current.source_kind='PROTECTED'
        and v_source_current.source_head_id is not null and v_source_current.source_event_id=v_source_current.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_segment_basis:=private.bpay_next_segment_collection_basis_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_segment_complete:=coalesce((v_segment_basis->>'complete')::boolean,false);
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is null or v_basis.original_tax_treatment not in ('TAXABLE','NON_TAXABLE') then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and not v_segment_complete and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind=v_original_approved.component_kind and a.tax_treatment=v_original_approved.tax_treatment
            and (v_work.work_kind in ('ORDINARY','EXPENSE')
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      join private.bpay_next_publication actual_publication on actual_publication.work_id=p_work_id
        and actual_publication.revision_id=v_position.applied_revision_id and actual_publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_source_current.id
        and v.head_id=v_revision.source_head_id and v_source_current.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v_position.applied_revision_id
        and (v_source_current.id=v_position.applied_revision_id or (publication.status='APPLIED' and publication.phase='DONE'))
        and ((v_job.job_kind='POSITION_APPLY' and actual_publication.command_id=v_job.command_id
              and actual_publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and actual_publication.status='APPLIED' and actual_publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  v_observation:=private.bpay_next_work_collection_observation_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_mode:=case when v_state='READY' and v_basis.original_source_pay_channel='PAYE'
    and not (v_work.approval_state='WITHDRAWN' and v_basis.collection_mode='MANUAL') then 'AUTOMATIC' else 'MANUAL' end;
  v_manual_amount:=null;v_manual_reason:=null;
  if v_mode='MANUAL' then
    v_manual_reason:=case when coalesce((v_observation->>'complete')::boolean,false)
      then 'AUTOMATIC_COLLECTION_UNAVAILABLE' else 'AMOUNT_UNBOUND' end;
    if v_manual_reason='AUTOMATIC_COLLECTION_UNAVAILABLE' then
      v_manual_amount:=(v_observation->>'free_recovery_source_ex_vat')::numeric;end if;
  end if;
  if v_state='READY' or (v_basis.case_id is not null and v_manual_amount is not null) then
    if v_basis.case_id is null then
      v_principal:=case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric
        else greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0) end;
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT',v_basis.original_tax_treatment,'OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,'RECOVERY','DEDUCTION',
            case when v_basis.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end,'PAYE','GBP',v_principal,'REVIEW');
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
        case when v_mode='MANUAL' and coalesce((v_observation->>'segmented')::boolean,false) then
          (v_observation->>'outstanding_source_ex_vat')::numeric
        when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric else
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0) end;
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
    collection_mode=v_mode,manual_outstanding_source_ex_vat=v_manual_amount,manual_reason_code=v_manual_reason,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    segment_attribution_complete=v_segment_complete,
    observed_segment_negative_ex_vat=case when v_segment_complete then (v_segment_basis->>'negative_source_ex_vat')::numeric else 0 end,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
  perform private.bpay_next_workbench_refresh_many_v1(array[p_work_id],
    case when v_basis.case_id is null then array[]::uuid[] else array[v_basis.case_id] end);
end
$function$;


alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_work_collection_observation_v1(
  p_work_id uuid,p_component_key text,p_revision_id uuid
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_position private.bpay_next_position%rowtype;v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_original private.bpay_next_work_revision%rowtype;
  v_source_current private.bpay_next_work_revision%rowtype;v_source_original private.bpay_next_work_revision%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_current private.bpay_next_approved_line%rowtype;v_segments record;v_segmented boolean;
  v_retained boolean:=false;v_removed boolean:=false;v_complete boolean:=false;v_outstanding numeric;v_free numeric;v_negative numeric;v_positive numeric;v_overhang numeric;
begin
  select pos.* into v_position from private.bpay_next_position pos where pos.work_id=p_work_id and pos.component_key=p_component_key;
  select wrk.* into v_work from private.bpay_next_work wrk where wrk.id=p_work_id;
  select rev.* into v_revision from private.bpay_next_work_revision rev where rev.id=p_revision_id and rev.work_id=p_work_id;
  select coll.* into v_basis from private.bpay_next_work_collection coll where coll.work_id=p_work_id and coll.component_key=p_component_key;
  -- Withdrawal retains a previously known sealed observation, never approval zero.
  v_retained:=v_work.approval_state='WITHDRAWN' and v_work.current_revision_id is null
    and v_work.applied_revision_id=p_revision_id and v_basis.observed_revision_id=p_revision_id
    and v_basis.observed_approved_source_ex_vat=v_position.approved_source_ex_vat
    and v_basis.collection_mode='MANUAL' and v_basis.manual_reason_code='AUTOMATIC_COLLECTION_UNAVAILABLE'
    and v_basis.manual_outstanding_source_ex_vat is not null;
  if v_basis.id is null or v_position.applied_revision_id is distinct from p_revision_id
    or not (coalesce(v_retained,false) or (v_work.current_revision_id=p_revision_id and v_work.approval_state='APPROVED'))
    or v_revision.sealed_at_utc is null or v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  select rev.* into strict v_original from private.bpay_next_work_revision rev where rev.id=v_basis.original_revision_id;
  select source.* into strict v_source_current from private.bpay_next_work_revision source
    where source.work_id=p_work_id and source.id=(case when v_revision.sealed_at_utc is not null and v_revision.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(v_revision.id) else v_revision.id end);
  select source.* into strict v_source_original from private.bpay_next_work_revision source
    where source.work_id=p_work_id and source.id=(case when v_original.sealed_at_utc is not null and v_original.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(v_original.id) else v_original.id end);
  select line.* into strict v_line from private.bpay_next_approved_line line where line.id=v_basis.original_approved_line_id;
  select line.* into v_current from private.bpay_next_approved_line line where line.revision_id=p_revision_id and line.component_key=p_component_key;
  if v_basis.case_id is not null then
    select fc.* into strict v_case from private.bpay_next_finance_case fc where fc.id=v_basis.case_id;
    select cc.* into strict v_component from private.bpay_next_case_component cc where cc.id=v_basis.case_component_id;
    if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
      is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
        v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
      return jsonb_build_object('complete',false,'segmented',false);end if;
  end if;
  if v_revision.source_pay_channel is distinct from v_basis.original_source_pay_channel
    or v_position.source_basis_channel is distinct from v_basis.original_source_pay_channel then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  -- The same typed authorised key, not an arbitrary absent DAY/aggregate.
  v_complete:=v_revision.detail_kind=v_original.detail_kind and v_current.id is not null
    and v_current.component_kind=v_line.component_kind
    and v_current.tax_treatment is not distinct from v_line.tax_treatment
    and v_current.source_component_id is not distinct from v_line.source_component_id
    and v_current.source_pay_ex_vat=v_position.approved_source_ex_vat;
  v_complete:=v_complete or (v_revision.certified_zero and v_revision.expected_line_count=0
    and v_revision.approved_source_ex_vat=0 and v_position.approved_source_ex_vat=0);
  -- Explicit accepted REMOVED membership certifies this typed component's zero.
  -- A surviving fixed sibling may make the current revision FIXED; the paid
  -- component and native segment counters retain their original identity.
  v_removed:=v_work.work_kind='SOURCE' and v_original.work_id=p_work_id
    and v_line.revision_id=v_original.id and v_line.component_key=p_component_key
    and v_line.source_component_id is not null
    and p_component_key='SOURCE:'||v_line.source_component_id::text
    and v_current.id is null and v_position.approved_source_ex_vat=0
    and v_revision.source_head_id is not null and v_source_current.source_event_id=v_revision.source_head_id
    and ((v_source_original.source_kind='SOURCE' and v_source_original.source_event_id is not null
        and (v_source_original.source_head_id is null or v_source_original.source_head_id=v_source_original.source_event_id))
      or (v_source_original.source_kind='PROTECTED' and v_source_original.source_head_id is not null
        and v_source_original.source_head_id=v_source_original.source_event_id))
    and ((v_revision.source_kind='SOURCE' and v_source_current.source_event_id is not null
        and (v_revision.source_head_id is null or v_revision.source_head_id=v_source_current.source_event_id))
      or (v_revision.source_kind='PROTECTED' and v_revision.source_head_id is not null
        and v_revision.source_head_id=v_source_current.source_event_id))
    and exists(
      select 1 from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          and bv.canonical_work_id=b.canonical_work_id and bv.canonical_component_key=b.canonical_component_key
          and bv.candidate_id=b.candidate_id and bv.source_component_id=b.source_component_id
        join private.bpay_next_approved_line classification on classification.id=bv.classification_line_id
        join private.bpay_next_work_revision classified on classified.id=classification.revision_id
          and classified.work_id=p_work_id and classified.sealed_at_utc is not null
        join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
        join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=bv.decision_bundle_id
          and bundle.bundle_revision=bv.bundle_revision and bundle.state='COMMITTED'
        join public.weekly_source_entitlement_heads head on head.id=bv.head_id and head.state='COMMITTED_CURRENT'
        join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
          and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
        join private.bpay_next_publication publication on publication.command_id=bv.publication_command_id_1
          and publication.work_id=bv.publication_work_id_1 and publication.revision_id=bv.publication_revision_id_1
          and publication.candidate_id=v_work.candidate_id
        join private.bpay_next_publication actual_publication on actual_publication.work_id=p_work_id
          and actual_publication.revision_id=p_revision_id and actual_publication.candidate_id=v_work.candidate_id
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and b.candidate_id=v_work.candidate_id and b.source_component_id=v_line.source_component_id
        and bv.component_state='REMOVED' and bv.approved_line_id is null
        and bv.approval_work_id=p_work_id and bv.approval_revision_id=v_source_current.id
        and bv.approval_component_key=p_component_key and bv.approval_source_channel=v_basis.original_source_pay_channel
        and bv.publication_work_id_1=p_work_id and bv.publication_revision_id_1=v_source_current.id
        and bv.publication_command_id_2 is null and bv.publication_work_id_2 is null and bv.publication_revision_id_2 is null
        and bv.head_id=v_revision.source_head_id
        and classification.component_key=p_component_key and classification.source_component_id=v_line.source_component_id
        and classification.component_kind=v_line.component_kind
        and classification.tax_treatment is not distinct from v_line.tax_treatment
        and bv.tax_treatment is not distinct from v_line.tax_treatment
        and classified.source_pay_channel=v_basis.original_source_pay_channel
        and ((classified.source_kind='SOURCE' and classified.source_event_id is not null
            and (classified.source_head_id is null or classified.source_head_id=classified.source_event_id))
          or (classified.source_kind='PROTECTED' and classified.source_head_id is not null
            and classified.source_head_id=classified.source_event_id))
        and receipt.candidate_id=v_work.candidate_id and receipt.decision_bundle_id=bv.decision_bundle_id
        and receipt.bundle_revision=bv.bundle_revision and receipt.request_digest=bv.request_digest
        and cardinality(receipt.head_ids)=1 and receipt.head_ids[1]=bv.head_id
        and publication.status in ('QUEUED','APPLYING','APPLIED')
        and actual_publication.status in ('QUEUED','APPLYING','APPLIED'));
  v_complete:=v_complete or coalesce(v_removed,false);
  -- A Source head must be the genuinely authorised committed book. An
  -- unrelated/unbound binding is retained as AMOUNT_UNBOUND, never zero.
  if not coalesce(v_retained,false) and v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists(
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=bv.decision_bundle_id
        and bundle.bundle_revision=bv.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=bv.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=bv.publication_command_id_1
        and publication.work_id=p_work_id and publication.revision_id=v_source_current.id and publication.candidate_id=v_work.candidate_id
      join private.bpay_next_publication actual_publication on actual_publication.work_id=p_work_id
        and actual_publication.revision_id=p_revision_id and actual_publication.candidate_id=v_work.candidate_id
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and bv.approval_work_id=p_work_id and bv.approval_component_key=p_component_key
        and bv.approval_revision_id=v_source_current.id and bv.candidate_id=v_work.candidate_id
        and bv.publication_command_id_2 is null and bv.head_id=v_revision.source_head_id
        and v_source_current.source_event_id=bv.head_id and bv.approval_source_channel=v_revision.source_pay_channel
        and receipt.decision_bundle_id=bv.decision_bundle_id and receipt.bundle_revision=bv.bundle_revision
        and receipt.request_digest=bv.request_digest and cardinality(receipt.head_ids)=1 and receipt.head_ids[1]=bv.head_id
        and publication.status in ('QUEUED','APPLYING','APPLIED')
        and actual_publication.status in ('QUEUED','APPLYING','APPLIED')) then v_complete:=false;end if;
  v_segmented:=exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=p_revision_id and a.component_key=p_component_key)
    or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=p_component_key);
  if v_segmented then
    -- Reuse native maintained segment balances. Original actual target totals
    -- must also reconcile; no gross-to-net ratio or history re-sum.
    select count(d.segment_key) n,bool_and(d.approved_source_ex_vat>=0 and d.realised_source_ex_vat>=0) nonnegative,
      sum(d.approved_source_ex_vat) approved,sum(d.realised_source_ex_vat) realised,sum(d.held_source_ex_vat) held,
      sum(d.written_off_source_ex_vat) written_off,sum(d.active_recovery_source_ex_vat) recovery_held,
      sum(credit.ex) realised_ex,sum(credit.vat) realised_vat,sum(credit.inc) realised_inc,
      sum(held.ex) held_ex,sum(held.vat) held_vat,sum(held.inc) held_inc,
      sum(d.outstanding_source_ex_vat) outstanding,sum(d.free_recovery_source_ex_vat) free,
      sum(d.negative_source_ex_vat) negative,
      sum(greatest(d.approved_source_ex_vat-d.realised_source_ex_vat-d.held_source_ex_vat,0)) positive,
      sum(greatest(d.held_source_ex_vat-greatest(d.approved_source_ex_vat-d.realised_source_ex_vat,0),0)) overhang
      into v_segments from private.bpay_next_work_segment_debt_basis_v1(p_work_id,p_component_key,p_revision_id) d
      cross join lateral (select coalesce(sum(e.target_ex_vat),0) ex,coalesce(sum(e.target_vat),0) vat,coalesce(sum(e.target_inc_vat),0) inc
        from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id and e.component_key=p_component_key and e.segment_key=d.segment_key) credit
      cross join lateral (select coalesce(sum(s.target_reserved_ex_vat),0) ex,coalesce(sum(s.target_reserved_vat),0) vat,coalesce(sum(s.target_reserved_inc_vat),0) inc
        from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
        where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=d.segment_key) held;
    v_complete:=v_complete and v_line.component_kind='WORK' and (v_revision.detail_kind='SHIFT' or v_revision.certified_zero
        or (coalesce(v_removed,false) and v_original.detail_kind='SHIFT'))
      and v_segments.n>0 and v_segments.nonnegative
      and (v_segments.approved,v_segments.realised,v_segments.held,v_segments.realised_ex,v_segments.realised_vat,v_segments.realised_inc,
        v_segments.held_ex,v_segments.held_vat,v_segments.held_inc,v_segments.written_off,v_segments.recovery_held)
        is not distinct from (v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,v_position.held_source_ex_vat,
          v_position.realised_target_ex_vat,v_position.realised_target_vat,v_position.realised_target_inc_vat,
          v_position.held_target_ex_vat,v_position.held_target_vat,v_position.held_target_inc_vat,
          coalesce(v_case.principal_written_off,0),coalesce(v_case.active_recovery_hold_amount,0));
    v_outstanding:=v_segments.outstanding;v_free:=v_segments.free;v_negative:=v_segments.negative;v_positive:=v_segments.positive;v_overhang:=v_segments.overhang;
  else
    v_outstanding:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-coalesce(v_case.principal_written_off,0),
      coalesce(v_case.active_recovery_hold_amount,0),0);
    v_free:=v_outstanding-coalesce(v_case.active_recovery_hold_amount,0);
    v_negative:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0);
    v_positive:=greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat-v_position.held_source_ex_vat,0);
    v_overhang:=greatest(v_position.held_source_ex_vat-greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat,0),0);
  end if;
  return jsonb_build_object('complete',coalesce(v_complete,false),'segmented',v_segmented,
    'outstanding_source_ex_vat',case when v_complete then v_outstanding end,
    'free_recovery_source_ex_vat',case when v_complete then v_free end,
    'negative_source_ex_vat',case when v_complete then v_negative end,
    'positive_source_ex_vat',case when v_complete then v_positive end,
    'held_overhang_source_ex_vat',case when v_complete then v_overhang end);
end
$function$;


alter function private.bpay_next_work_collection_observation_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_observation_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

commit;
