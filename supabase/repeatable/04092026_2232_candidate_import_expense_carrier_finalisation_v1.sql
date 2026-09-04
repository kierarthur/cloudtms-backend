-- Candidate import-authoritative expense-carrier finalisation v1.
--
-- A Candidate expense workflow reserves its own additional Contract week
-- before manager approval.  Finalisation must reuse that exact reservation,
-- and the final-state guard must recognise the resulting zero-hour
-- EXPENSES/MILEAGE row as a separate carrier rather than imported hours.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_weekly_final_state_guard_v1(
  p_contract_week_id uuid,
  p_timesheet_id uuid default null,
  p_timesheet_create_json jsonb default null,
  p_timesheet_patch_json jsonb default '{}'::jsonb,
  p_tsfin_snapshot_json jsonb default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_snapshot jsonb:=coalesce(p_tsfin_snapshot_json,'{}'::jsonb);
  v_hours numeric:=0;
  v_additional numeric:=0;
  v_mileage numeric:=0;
  v_non_mileage numeric:=0;
  v_expenses numeric:=0;
  v_import boolean;
  v_role text;
  v_expected_line_type text;
  v_requested_line_type text;
  v_import_authority jsonb;
  v_candidate_expense_carrier boolean:=false;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities')
     and coalesce(current_setting('cloudtms.candidate_electronic_finalise',true),'')='' then
    return '{}'::jsonb;
  end if;
  select * into v_week from public.contract_weeks where id=p_contract_week_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id;
  if p_tsfin_snapshot_json is null and p_timesheet_id is not null then
    select to_jsonb(tf) into v_snapshot
    from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id and tf.is_current=true
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc
    limit 1;
    v_snapshot:=coalesce(v_snapshot,'{}'::jsonb);
  end if;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,v_week.week_ending_date);
  v_hours:=coalesce(nullif(v_snapshot->>'total_hours','')::numeric,
    coalesce(nullif(v_snapshot->>'hours_day','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_night','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_sat','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_sun','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_bh','')::numeric,0),0);
  v_additional:=greatest(
    private._candidate_json_numeric_sum(coalesce(v_snapshot->'additional_units_json','{}'::jsonb)),
    private._candidate_json_numeric_sum(coalesce(p_timesheet_create_json->'additional_units_week',p_timesheet_patch_json->'additional_units_week','{}'::jsonb))
      +private._candidate_json_numeric_sum(coalesce(p_timesheet_create_json->'additional_units_per_day',p_timesheet_patch_json->'additional_units_per_day','{}'::jsonb))
  );
  v_mileage:=abs(coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0));
  v_non_mileage:=abs(coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0));
  v_expenses:=v_mileage+v_non_mileage;
  v_requested_line_type:=upper(nullif(btrim(coalesce(
    p_timesheet_patch_json->>'line_type',p_timesheet_create_json->>'line_type','')),''));
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,coalesce(p_timesheet_id,v_week.timesheet_id),v_snapshot,v_week.week_ending_date
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);

  -- The exception is deliberately narrower than "zero hours": it requires
  -- the exact Candidate finalisation receipt, the workflow's reserved
  -- additional Contract week, a distinct worked anchor, and an explicit
  -- final expense line type.  The imported hours row therefore stays locked.
  if v_week.additional_seq>0
     and v_hours=0 and v_additional=0 and v_expenses<>0
     and v_requested_line_type in ('EXPENSES','MILEAGE') then
    select exists(
      select 1
      from public.candidate_submission_workflows workflow
      where current_setting('cloudtms.candidate_finalize_workflow',true)
              =workflow.id::text||':'||workflow.generation::text
        and workflow.workflow_kind='CONTRACT_EXPENSE'
        and workflow.contract_week_id=p_contract_week_id
        and workflow.contract_id=v_contract.id
        and workflow.week_ending_date=v_week.week_ending_date
        and workflow.state in ('READY_TO_FINALISE','RECEIVED')
        and workflow.anchor_timesheet_id is not null
        and workflow.anchor_timesheet_id is distinct from p_timesheet_id
        and workflow.target_timesheet_id is not distinct from p_timesheet_id
    ) into v_candidate_expense_carrier;
  end if;

  if v_import and v_expenses<>0 and not v_candidate_expense_carrier then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  if coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false)
     and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  v_role:=case
    when v_candidate_expense_carrier then 'EXPENSE_ONLY'
    when v_import then 'IMPORT_HOURS'
    when v_expenses<>0 and v_hours=0 and v_additional=0 then 'EXPENSE_ONLY'
    when (v_hours<>0 or v_additional<>0) and v_expenses=0 then 'HOURS_ONLY'
    when not coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false) then 'COMBINED_ALLOWED'
    else 'FLEXIBLE' end;
  v_expected_line_type:=case
    when v_role='EXPENSE_ONLY' and v_mileage<>0 and v_non_mileage=0 then 'MILEAGE'
    when v_role='EXPENSE_ONLY' then 'EXPENSES'
    else 'HOURS' end;
  if v_requested_line_type is not null and v_requested_line_type<>v_expected_line_type then
    raise exception 'CANDIDATE_LINE_TYPE_FINAL_STATE_INVALID'
      using errcode='22023',detail=jsonb_build_object('record_role',v_role,
        'expected_line_type',v_expected_line_type,'requested_line_type',v_requested_line_type)::text;
  end if;
  return jsonb_build_object('record_role',v_role,'expected_line_type',v_expected_line_type,
    'hours_value',v_hours,'additional_units_value',v_additional,'expense_value',v_expenses,'policy',v_policy);
exception when invalid_text_representation then
  raise exception 'CANDIDATE_FINAL_STATE_INPUT_INVALID' using errcode='22023';
end;
$function$;

create or replace function public.expense_carrier_resolve_or_create_atomic_v1(
  p_candidate_id uuid,
  p_environment text,
  p_anchor_timesheet_id uuid,
  p_expected_row_signature text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_placement jsonb;
  v_signature jsonb;
  v_new_week public.contract_weeks%rowtype;
  v_next_seq integer;
  v_bound_count integer:=0;
  v_bound_week public.contract_weeks%rowtype;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_anchor_timesheet_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'EXPENSE_CARRIER_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_anchor_week from public.contract_weeks where timesheet_id=p_anchor_timesheet_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  if nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null then
    v_signature:=public.timesheet_lifecycle_guard_signature_v1(p_anchor_timesheet_id,v_anchor_week.id,false);
    if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
      raise exception 'ROW_SIGNATURE_MISMATCH'
        using errcode='40001',detail=jsonb_build_object('code','ROW_SIGNATURE_MISMATCH')::text;
    end if;
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||v_anchor_week.week_ending_date::text||'|EXPENSE_CARRIER'));
  perform 1 from public.contract_weeks cw
  where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
  order by cw.additional_seq,cw.id for update;

  -- By finalisation time a CONTRACT_EXPENSE workflow already owns an exact
  -- additional Contract week. Reuse that reservation rather than allocating
  -- an unreferenced successor. The active-claim uniqueness constraint normally
  -- makes this one row; fail closed if historical corruption makes it plural.
  select count(*)::integer into v_bound_count
  from public.candidate_submission_workflows workflow
  join public.contract_weeks carrier on carrier.id=workflow.contract_week_id
  where workflow.environment=v_environment
    and workflow.candidate_id=p_candidate_id
    and workflow.contract_id=v_contract.id
    and workflow.week_ending_date=v_anchor_week.week_ending_date
    and workflow.workflow_kind='CONTRACT_EXPENSE'
    and workflow.anchor_timesheet_id=p_anchor_timesheet_id
    and workflow.target_timesheet_id is null
    and ((workflow.route='PAPER' and workflow.state='RECEIVED')
      or (workflow.route<>'PAPER' and workflow.state='READY_TO_FINALISE'))
    and carrier.contract_id=v_contract.id
    and carrier.week_ending_date=v_anchor_week.week_ending_date
    and carrier.additional_seq>0
    and carrier.status='OPEN'
    and carrier.timesheet_id is null;
  if v_bound_count>1 then
    raise exception 'EXPENSE_WORKFLOW_CARRIER_AMBIGUOUS' using errcode='55000';
  elsif v_bound_count=1 then
    select carrier.* into v_bound_week
    from public.candidate_submission_workflows workflow
    join public.contract_weeks carrier on carrier.id=workflow.contract_week_id
    where workflow.environment=v_environment
      and workflow.candidate_id=p_candidate_id
      and workflow.contract_id=v_contract.id
      and workflow.week_ending_date=v_anchor_week.week_ending_date
      and workflow.workflow_kind='CONTRACT_EXPENSE'
      and workflow.anchor_timesheet_id=p_anchor_timesheet_id
      and workflow.target_timesheet_id is null
      and ((workflow.route='PAPER' and workflow.state='RECEIVED')
        or (workflow.route<>'PAPER' and workflow.state='READY_TO_FINALISE'))
      and carrier.contract_id=v_contract.id
      and carrier.week_ending_date=v_anchor_week.week_ending_date
      and carrier.additional_seq>0
      and carrier.status='OPEN'
      and carrier.timesheet_id is null
    limit 1;
    return jsonb_build_object(
      'ok',true,'placement','REUSE_CARRIER','reason_code','WORKFLOW_CARRIER_RESERVED',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'target_timesheet_id',null,'target_contract_week_id',v_bound_week.id,
      'target_record_role','FLEXIBLE','idempotent_replay',true,'idempotency_key',p_idempotency_key
    );
  end if;

  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='BLOCKED' then
    raise exception '%',v_placement->>'reason_code' using errcode='55000',detail=v_placement::text;
  elsif v_placement->>'placement' in ('SAME_RECORD','REUSE_CARRIER') then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  select coalesce(max(additional_seq),0)+1 into v_next_seq from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=v_anchor_week.week_ending_date;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,is_adjustment,
    enforce_day_partition,allowed_days_mask,split_boundary_date,split_group_key,
    created_at,updated_at
  ) values (
    v_contract.id,v_anchor_week.week_ending_date,v_next_seq,'OPEN','MANUAL',
    '[]'::jsonb,
    jsonb_build_object(
      'hours',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),
      'additional_units_week','{}'::jsonb,
      'additional_units_per_day','{}'::jsonb,
      'expenses_draft',jsonb_build_object(
        'mileage_units',0,'travel_pay',0,'travel_charge',0,
        'accommodation_pay',0,'accommodation_charge',0,
        'other_pay',0,'other_charge',0,'note',''
      )
    ),
    '[]'::jsonb,true,
    v_anchor_week.enforce_day_partition,v_anchor_week.allowed_days_mask,
    v_anchor_week.split_boundary_date,v_anchor_week.split_group_key,
    p_now_utc,p_now_utc
  ) returning * into v_new_week;
  perform private._candidate_audit_v1('contract_week',v_new_week.id::text,'CANDIDATE_EXPENSE_CARRIER_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',v_new_week.week_ending_date,'additional_seq',v_new_week.additional_seq),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object(
    'ok',true,'placement','CREATE_CARRIER','reason_code','CARRIER_CREATED',
    'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',null,'target_contract_week_id',v_new_week.id,
    'target_record_role','FLEXIBLE','idempotent_replay',false,'idempotency_key',p_idempotency_key
  );
exception when unique_violation then
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='REUSE_CARRIER' then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  raise;
end;
$function$;

alter function private._candidate_weekly_final_state_guard_v1(uuid,uuid,jsonb,jsonb,jsonb) owner to postgres;
alter function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) owner to postgres;

revoke all on function private._candidate_weekly_final_state_guard_v1(uuid,uuid,jsonb,jsonb,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz)
  from public,anon,authenticated;
grant execute on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz)
  to service_role;

commit;
