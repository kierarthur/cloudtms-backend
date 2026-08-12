-- Candidate finalisation composes canonical CloudTMS Save/Recalculate, Process and Authorise authorities.
-- No Candidate-named financial calculation implementation is retained here.

create or replace function private._candidate_daily_rate_contract_v1(p_rate jsonb)
returns jsonb
language sql
immutable
set search_path = pg_catalog, private
as $function$
  select jsonb_build_object(
    'source_kind',nullif(p_rate->>'source_kind',''),
    'override_id',nullif(p_rate->>'override_id','')::uuid,
    'default_id',nullif(p_rate->>'default_id','')::uuid,
    'rate_type',nullif(upper(p_rate->>'rate_type'),''),
    'pay_day',nullif(p_rate->>'pay_day','')::numeric,
    'pay_night',nullif(p_rate->>'pay_night','')::numeric,
    'pay_sat',nullif(p_rate->>'pay_sat','')::numeric,
    'pay_sun',nullif(p_rate->>'pay_sun','')::numeric,
    'pay_bh',nullif(p_rate->>'pay_bh','')::numeric,
    'charge_day',nullif(p_rate->>'charge_day','')::numeric,
    'charge_night',nullif(p_rate->>'charge_night','')::numeric,
    'charge_sat',nullif(p_rate->>'charge_sat','')::numeric,
    'charge_sun',nullif(p_rate->>'charge_sun','')::numeric,
    'charge_bh',nullif(p_rate->>'charge_bh','')::numeric
  )
$function$;

create or replace function private._candidate_daily_context_contract_v1(
  p_workflow_id uuid,
  p_generation integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_candidate public.candidates%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_financial_policy jsonb;
  v_effective_flags jsonb;
  v_route jsonb;
  v_signature jsonb;
  v_row_signature text;
  v_client_id uuid;
  v_work_date date;
  v_rate_type text;
begin
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_generation;
  if not found or v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
     or v_workflow.target_timesheet_id is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;

  select * into v_timesheet
  from public.timesheets
  where timesheet_id=v_workflow.target_timesheet_id and is_current=true
    and archived_at_utc is null and sheet_scope='DAILY'::public.timesheet_scope_enum;
  if not found then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
  end if;

  select * into v_fin
  from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc
  limit 1;
  if not found or v_fin.candidate_id is distinct from v_workflow.candidate_id then
    raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
  end if;

  select * into v_candidate
  from public.candidates
  where id=v_workflow.candidate_id and active=true;
  if not found then
    raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
  end if;

  if v_timesheet.contract_id is not null then
    select * into v_contract
    from public.contracts
    where id=v_timesheet.contract_id and candidate_id=v_workflow.candidate_id;
  end if;
  v_client_id:=coalesce(v_fin.client_id,v_contract.client_id);
  if v_client_id is null then
    raise exception 'CANDIDATE_DAILY_CLIENT_CONTEXT_REQUIRED' using errcode='55000';
  end if;

  v_work_date:=private._candidate_daily_work_date_v1(
    coalesce(nullif(v_workflow.immutable_submission_json->>'worked_start_iso','')::timestamptz,
      v_timesheet.worked_start_iso),
    v_timesheet.scheduled_start_iso,
    coalesce(v_workflow.work_date,v_timesheet.week_ending_date)
  );
  if v_work_date is distinct from v_workflow.work_date then
    raise exception 'CANDIDATE_DAILY_WORK_DATE_MISMATCH' using errcode='40001';
  end if;

  v_policy:=private._candidate_policy_resolve_v1(
    v_client_id,v_timesheet.contract_id,v_work_date
  );
  select context_row.out_policy,context_row.out_effective_flags
  into v_financial_policy,v_effective_flags
  from public.tsfin_load_context_batch(array[v_timesheet.timesheet_id]::uuid[]) context_row
  where context_row.effective_timesheet_id=v_timesheet.timesheet_id;
  if not found then
    raise exception 'CANDIDATE_DAILY_CANONICAL_CONTEXT_NOT_FOUND' using errcode='55000';
  end if;
  v_route:=private._candidate_route_family_v1(v_timesheet.timesheet_id,null);
  v_rate_type:=case when upper(coalesce(v_candidate.pay_method,'')) in ('PAYE','UMBRELLA')
    then upper(v_candidate.pay_method) else 'UMBRELLA' end;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,null,false);
  v_row_signature:=coalesce(
    nullif(btrim(v_signature->>'backend_row_signature'),''),
    nullif(btrim(v_signature->>'row_signature'),'')
  );
  if v_row_signature is null then
    raise exception 'CANDIDATE_DAILY_ROW_SIGNATURE_REQUIRED' using errcode='55000';
  end if;

  return jsonb_build_object(
    'contract_version','CANDIDATE_DAILY_LOCKED_CONTEXT_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,
    'candidate_id',v_workflow.candidate_id,
    'contract_id',v_timesheet.contract_id,
    'client_id',v_client_id,
    'canonical_work_date',v_work_date,
    -- The current canonical TSFIN context is the authority for the rate role
    -- and band. Candidate/app input cannot select either value.
    'role',nullif(btrim(coalesce(v_fin.role,'')),''),
    'band',nullif(btrim(coalesce(v_fin.band,'')),''),
    'rate_type',v_rate_type,
    'route_family',v_route->>'route_family',
    'candidate_policy_fingerprint',v_policy->>'policy_fingerprint',
    'financial_policy_sha256_hex',encode(
      private._candidate_sha256_jsonb_v1(coalesce(v_financial_policy,'{}'::jsonb)),'hex'
    ),
    'effective_flags_sha256_hex',encode(
      private._candidate_sha256_jsonb_v1(coalesce(v_effective_flags,'{}'::jsonb)),'hex'
    ),
    'rate_source_sha256_hex',encode(
      private._candidate_sha256_jsonb_v1(coalesce(v_fin.rate_source_refs_json,'{}'::jsonb)),'hex'
    ),
    'pre_save_row_signature',v_row_signature
  );
end;
$function$;

create or replace function private._candidate_financial_content_sha256_v1(
  p_financials_id uuid
)
returns bytea
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_fin public.timesheets_financials%rowtype;
  v_business_content jsonb;
begin
  select * into v_fin
  from public.timesheets_financials
  where id=p_financials_id;
  if not found then
    raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='P0002';
  end if;

  -- Keep every current/future business and economic field by default. Remove
  -- only row identity, version routing, timestamps and lifecycle state so that
  -- moving an otherwise identical snapshot between route generations cannot
  -- change the economic fingerprint.
  v_business_content:=to_jsonb(v_fin)-array[
    'id','created_at','updated_at','computed_at_utc','timesheet_id',
    'timesheet_version','is_current','is_stale','processing_status',
    'authorised_at_utc','authorised_by_user_id','paid_at_utc',
    'locked_by_invoice_id','locked_at_utc'
  ];
  return private._candidate_sha256_jsonb_v1(v_business_content);
end;
$function$;

create or replace function private._candidate_daily_save_recalculate_atomic_v1(
  p_workflow_id uuid,
  p_generation integer,
  p_materialisation jsonb,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_current_fin public.timesheets_financials%rowtype;
  v_candidate public.candidates%rowtype;
  v_contract public.contracts%rowtype;
  v_input jsonb;
  v_patch jsonb;
  v_snapshot jsonb;
  v_rate_request jsonb;
  v_context jsonb;
  v_context_hash bytea;
  v_supplied_rate jsonb;
  v_fresh_rate jsonb;
  v_signature jsonb;
  v_before_signature text;
  v_after_signature text;
  v_write_result jsonb;
  v_financials_id uuid;
  v_receipt jsonb;
  v_client_id uuid;
  v_rate_type text;
begin
  if jsonb_typeof(p_materialisation)<>'object'
     or p_materialisation->>'contract_version'<>'CANDIDATE_DAILY_ATOMIC_FINALISATION_V2' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_INVALID' using errcode='22023';
  end if;
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_generation
  for update;
  if not found or v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
     or v_workflow.target_timesheet_id is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  if nullif(p_materialisation->>'workflow_id','')::uuid is distinct from v_workflow.id
     or nullif(p_materialisation->>'workflow_generation','')::integer is distinct from v_workflow.generation
     or nullif(p_materialisation->>'timesheet_id','')::uuid is distinct from v_workflow.target_timesheet_id then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_IDENTITY_MISMATCH' using errcode='40001';
  end if;
  v_input:=private._candidate_daily_canonical_save_input_v1(v_workflow.id,v_workflow.generation);
  if coalesce(p_materialisation->>'canonical_save_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
     or decode(p_materialisation->>'canonical_save_input_sha256_hex','hex')
        is distinct from private._candidate_sha256_jsonb_v1(v_input) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_STALE' using errcode='40001';
  end if;
  v_patch:=p_materialisation->'timesheet_patch_json';
  if jsonb_typeof(v_patch)<>'object'
     or (v_patch - 'worked_minutes') is distinct from (v_input->'timesheet_patch_json') then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_MISMATCH' using errcode='40001';
  end if;
  v_snapshot:=p_materialisation->'canonical_snapshot';
  v_rate_request:=p_materialisation->'rate_request';
  v_supplied_rate:=p_materialisation->'resolved_rate_row';
  if jsonb_typeof(v_snapshot)<>'object' or jsonb_typeof(v_rate_request)<>'object'
     or jsonb_typeof(v_supplied_rate)<>'object' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_INVALID' using errcode='22023';
  end if;

  select * into v_timesheet
  from public.timesheets
  where timesheet_id=v_workflow.target_timesheet_id and is_current=true
    and archived_at_utc is null and sheet_scope='DAILY'::public.timesheet_scope_enum
  for update;
  if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_current_fin
  from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc
  limit 1 for update;
  if not found or v_current_fin.candidate_id is distinct from v_workflow.candidate_id then
    raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
  end if;
  if v_timesheet.authorised_at_server is not null or v_current_fin.authorised_at_utc is not null
     or v_current_fin.locked_by_invoice_id is not null or v_current_fin.paid_at_utc is not null then
    raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
  end if;
  if not private._candidate_daily_entitled_v1(v_workflow.candidate_id) then
    raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,null,false);
  v_before_signature:=coalesce(
    nullif(btrim(v_signature->>'backend_row_signature'),''),
    nullif(btrim(v_signature->>'row_signature'),'')
  );
  if v_before_signature is null
     or v_before_signature is distinct from nullif(btrim(p_materialisation->>'expected_pre_save_row_signature'),'') then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_STALE' using errcode='40001';
  end if;

  v_context:=private._candidate_daily_context_contract_v1(
    v_workflow.id,v_workflow.generation
  );
  v_context_hash:=private._candidate_sha256_jsonb_v1(v_context);
  if coalesce(p_materialisation->>'canonical_context_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
     or v_workflow.daily_context_sha256 is null
     or decode(p_materialisation->>'canonical_context_sha256_hex','hex')
        is distinct from v_workflow.daily_context_sha256
     or v_context_hash is distinct from v_workflow.daily_context_sha256
     or nullif(v_context->>'pre_save_row_signature','') is distinct from v_before_signature then
    raise exception 'CANDIDATE_DAILY_LOCKED_CONTEXT_STALE' using errcode='40001';
  end if;

  select * into v_candidate from public.candidates where id=v_workflow.candidate_id and active=true;
  if not found then raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000'; end if;
  if v_timesheet.contract_id is not null then
    select * into v_contract from public.contracts
    where id=v_timesheet.contract_id and candidate_id=v_workflow.candidate_id;
  end if;
  v_client_id:=coalesce(v_current_fin.client_id,v_contract.client_id);
  v_rate_type:=case when upper(coalesce(v_candidate.pay_method,'')) in ('PAYE','UMBRELLA')
    then upper(v_candidate.pay_method) else 'UMBRELLA' end;
  if nullif(v_rate_request->>'k','')::uuid is distinct from v_timesheet.timesheet_id
     or nullif(v_rate_request->>'candidate_id','')::uuid is distinct from v_workflow.candidate_id
     or nullif(v_rate_request->>'client_id','')::uuid is distinct from v_client_id
     or nullif(v_rate_request->>'date','')::date
        is distinct from nullif(v_context->>'canonical_work_date','')::date
     or upper(coalesce(v_rate_request->>'rate_type',''))
        is distinct from upper(coalesce(v_context->>'rate_type',''))
     or nullif(v_rate_request->>'role','') is distinct from nullif(v_context->>'role','')
     or nullif(v_rate_request->>'band','') is distinct from nullif(v_context->>'band','')
     or nullif(v_snapshot->>'role','') is distinct from nullif(v_context->>'role','')
     or nullif(v_snapshot->>'band','') is distinct from nullif(v_context->>'band','')
     or encode(private._candidate_sha256_jsonb_v1(
          coalesce(v_snapshot->'policy_snapshot_json','{}'::jsonb)
        ),'hex') is distinct from v_context->>'financial_policy_sha256_hex' then
    raise exception 'CANDIDATE_DAILY_RATE_CONTEXT_STALE' using errcode='40001';
  end if;
  select to_jsonb(r) into v_fresh_rate
  from public.tsfin_resolve_rates_batch(jsonb_build_array(v_rate_request)) r
  limit 1;
  if v_fresh_rate is null
     or private._candidate_daily_rate_contract_v1(v_fresh_rate)
        is distinct from private._candidate_daily_rate_contract_v1(v_supplied_rate)
     or private._candidate_daily_rate_contract_v1(v_fresh_rate)
        is distinct from private._candidate_daily_rate_contract_v1(
          jsonb_build_object(
            'source_kind',v_snapshot#>>'{rate_source_refs_json,kind}',
            'override_id',v_snapshot#>>'{rate_source_refs_json,override_id}',
            'default_id',v_snapshot#>>'{rate_source_refs_json,default_id}',
            'rate_type',v_snapshot#>>'{rate_source_refs_json,rate_type}',
            'pay_day',v_snapshot->'pay_day','pay_night',v_snapshot->'pay_night',
            'pay_sat',v_snapshot->'pay_sat','pay_sun',v_snapshot->'pay_sun','pay_bh',v_snapshot->'pay_bh',
            'charge_day',v_snapshot->'charge_day','charge_night',v_snapshot->'charge_night',
            'charge_sat',v_snapshot->'charge_sat','charge_sun',v_snapshot->'charge_sun','charge_bh',v_snapshot->'charge_bh'
          )
        ) then
    raise exception 'CANDIDATE_DAILY_RATE_CONTEXT_STALE' using errcode='40001';
  end if;
  if nullif(v_snapshot->>'timesheet_id','')::uuid is distinct from v_timesheet.timesheet_id
     or nullif(v_snapshot->>'candidate_id','')::uuid is distinct from v_workflow.candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_client_id
     or nullif(v_snapshot->>'worked_start_iso','')::timestamptz
        is distinct from nullif(v_patch->>'worked_start_iso','')::timestamptz
     or nullif(v_snapshot->>'worked_end_iso','')::timestamptz
        is distinct from nullif(v_patch->>'worked_end_iso','')::timestamptz
     or nullif(v_snapshot->>'break_start_iso','')::timestamptz
        is distinct from nullif(v_patch->>'break_start_iso','')::timestamptz
     or nullif(v_snapshot->>'break_end_iso','')::timestamptz
        is distinct from nullif(v_patch->>'break_end_iso','')::timestamptz
     or nullif(v_snapshot->>'break_minutes','')::integer
        is distinct from nullif(v_patch->>'break_minutes','')::integer
     or upper(coalesce(v_snapshot->>'processing_status',''))<>'UNPROCESSED' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_MISMATCH' using errcode='40001';
  end if;

  update public.timesheets set
    worked_start_iso=nullif(v_patch->>'worked_start_iso','')::timestamptz,
    worked_end_iso=nullif(v_patch->>'worked_end_iso','')::timestamptz,
    worked_minutes=nullif(v_patch->>'worked_minutes','')::integer,
    break_start_iso=nullif(v_patch->>'break_start_iso','')::timestamptz,
    break_end_iso=nullif(v_patch->>'break_end_iso','')::timestamptz,
    break_minutes=nullif(v_patch->>'break_minutes','')::integer,
    actual_schedule_json=v_patch->'actual_schedule_json',
    candidate_submission_route_intent=null,
    updated_at=p_now_utc
  where timesheet_id=v_timesheet.timesheet_id and is_current=true;

  v_write_result:=public.tsfin_write_current_snapshot_single_bounded(
    v_timesheet.timesheet_id,v_timesheet.version,v_snapshot,p_actor_user_id,p_now_utc
  );
  if not coalesce((v_write_result->>'ok')::boolean,false) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_FAILED'
      using errcode='55000',detail=v_write_result::text;
  end if;
  v_financials_id:=nullif(v_write_result->>'timesheet_financials_id','')::uuid;
  select * into v_current_fin from public.timesheets_financials
  where id=v_financials_id and timesheet_id=v_timesheet.timesheet_id and is_current=true
  for update;
  if not found or v_current_fin.processing_status<>'UNPROCESSED'
     or coalesce(v_current_fin.has_rate_issue,false)
     or coalesce(v_current_fin.has_pay_channel_issue,false) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,null,false);
  v_after_signature:=coalesce(
    nullif(btrim(v_signature->>'backend_row_signature'),''),
    nullif(btrim(v_signature->>'row_signature'),'')
  );
  if v_after_signature is null then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
  end if;
  v_receipt:=jsonb_build_object(
    'contract_version','CANDIDATE_DAILY_ATOMIC_FINALISATION_RECEIPT_V2',
    'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,'financials_id',v_financials_id,
    'canonical_save_input_sha256_hex',p_materialisation->>'canonical_save_input_sha256_hex',
    'canonical_context_sha256_hex',p_materialisation->>'canonical_context_sha256_hex',
    'pre_save_row_signature',v_before_signature,'post_save_row_signature',v_after_signature,
    'canonical_work_date',private._candidate_daily_work_date_v1(
      nullif(v_patch->>'worked_start_iso','')::timestamptz,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date
    )
  );
  update public.candidate_submission_workflows set
    canonical_save_input_sha256=decode(p_materialisation->>'canonical_save_input_sha256_hex','hex'),
    canonical_save_row_signature=v_after_signature,
    canonical_save_financials_id=v_financials_id,
    canonical_save_receipt_json=v_receipt,
    canonical_saved_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where id=v_workflow.id;
  return v_receipt;
end;
$function$;

create or replace function public.candidate_submission_finalize_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text default null,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default null
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hours_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_approved_request public.candidate_approval_requests%rowtype;
  v_paper_hours_return public.candidate_submission_components%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_current_policy jsonb;
  v_system_actor uuid;
  v_input jsonb;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_render_input jsonb;
  v_result jsonb;
  v_authorise_result jsonb;
  v_expense_authorise_result jsonb;
  v_response jsonb;
  v_target_timesheet_id uuid;
  v_hours_timesheet_id uuid;
  v_expense_timesheet_id uuid;
  v_evidence_component_ids uuid[];
  v_placement jsonb;
  v_hours_result jsonb;
  v_hours_input jsonb;
  v_expense_input jsonb;
  v_effective_separation boolean:=false;
  v_after_signature text;
  v_auto_requested boolean:=false;
  v_auto_blocked boolean:=false;
  v_auto_blockers jsonb:='[]'::jsonb;
  v_constraint_name text;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_daily_save_input jsonb;
  v_daily_patch jsonb;
  v_daily_signature jsonb;
  v_daily_save_receipt jsonb;
  v_canonical_financials_id uuid;
  v_canonical_financial_sha256 bytea;
  v_service_finalisation jsonb;
  v_is_office_service boolean:=false;
  v_replay_probe_only boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_request_hash text;
  v_mutation_receipt jsonb;
  v_prior_receipt_before jsonb;
  v_prior_receipt_after jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  v_service_finalisation:=coalesce(p_daily_materialisation_json->'service_finalisation','{}'::jsonb);
  v_is_office_service:=p_session_id is null
    and private._candidate_office_service_context_valid_v1(
      v_environment,nullif(v_service_finalisation->>'actor_user_id','')::uuid,'RETRY_FINALISATION'
    );
  v_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_probe_only')::boolean,false);
  if not v_is_office_service then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023'; end if;

  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null then
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  if v_workflow.candidate_id<>v_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
        'idempotency_key',btrim(p_idempotency_key),
        'reason','CREATION_KEY_REUSED_FOR_MUTATION'
      )::text;
  end if;
  v_mutation_channel:=case when v_is_office_service then 'OFFICE'
    when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end;
  v_mutation_actor_identity:=case when v_is_office_service
    then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end;
  if v_replay_probe_only then
    if p_session_id is null and (
      coalesce(v_service_finalisation->>'contract_version','')
        <>'CANDIDATE_MANAGER_FINALISATION_V1'
      or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)
        <>p_expected_generation
    ) then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID' using errcode='28000';
    end if;
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from p_expected_generation+1 then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object('ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation);
  end if;
  v_mutation_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V2',
    'workflow_id',v_workflow.id,
    'action','RETRY_FINALISATION',
    'expected_generation',p_expected_generation,
    'service_finalisation',v_service_finalisation-'replay_probe_only',
    'channel',v_mutation_channel,
    'actor_identity',v_mutation_actor_identity
  )::text,'UTF8'),'sha256'),'hex');
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    v_mutation_channel,v_mutation_actor_identity,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return coalesce(v_mutation_receipt->'response','{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_session_id is null then
    if coalesce(v_service_finalisation->>'contract_version','')<>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)<>v_workflow.generation
       or upper(coalesce(v_service_finalisation->>'approval_method',''))<>v_workflow.route then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID' using errcode='28000';
    end if;
    if v_workflow.route='PAPER' then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID' using errcode='28000';
      end if;
    else
      select * into v_approved_request
      from public.candidate_approval_requests a
      where a.id=nullif(v_service_finalisation->>'approval_request_id','')::uuid
        and a.workflow_id=v_workflow.id
        and a.workflow_generation=p_expected_generation
        and a.method=upper(coalesce(v_service_finalisation->>'approval_method',''))
        and a.state='APPROVED'
        and encode(a.review_manifest_sha256,'hex')=lower(coalesce(v_service_finalisation->>'review_manifest_sha256_hex',''))
      for update;
      if not found then raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID' using errcode='28000'; end if;
    end if;
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  if v_workflow.workflow_kind='DAILY' then
    if v_workflow.scope<>'DAILY' or v_workflow.route not in ('PHONE','EMAIL')
       or v_workflow.contract_week_id is not null or v_workflow.week_ending_date is not null
       or v_workflow.target_timesheet_id is null
       or v_workflow.anchor_timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    select * into v_daily_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id
      and is_current=true and archived_at_utc is null
      and sheet_scope='DAILY'::public.timesheet_scope_enum
      and nullif(btrim(coalesce(booking_id,'')),'') is not null
    for update;
    if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
    if not private._candidate_daily_entitled_v1(v_candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    select * into v_daily_fin
    from public.timesheets_financials
    where timesheet_id=v_daily_timesheet.timesheet_id
      and is_current=true and candidate_id=v_candidate_id
    order by computed_at_utc desc nulls last,updated_at desc,id desc
    limit 1
    for update;
    if not found
       or v_workflow.work_date is distinct from private._candidate_daily_work_date_v1(
         coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
         v_daily_timesheet.scheduled_start_iso,
         v_daily_timesheet.week_ending_date
       ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
    end if;
    if v_daily_fin.authorised_at_utc is not null
       or v_daily_fin.paid_at_utc is not null
       or v_daily_fin.locked_by_invoice_id is not null
       or v_daily_timesheet.archived_at_utc is not null then
      raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
    end if;
    if v_daily_timesheet.contract_id is not null then
      select * into v_contract
      from public.contracts
      where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    end if;
    if coalesce(v_daily_fin.client_id,v_contract.client_id) is null then
      raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002';
    end if;
    v_current_policy:=private._candidate_policy_resolve_v1(
      coalesce(v_daily_fin.client_id,v_contract.client_id),v_contract.id,v_workflow.work_date
    );
    if (v_workflow.route='PHONE' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
       or (v_workflow.route='EMAIL' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  else
    if v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
       or v_workflow.scope<>'WEEKLY' or v_workflow.contract_id is null
       or v_workflow.contract_week_id is null or v_workflow.week_ending_date is null then
      raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_INVALID' using errcode='22023';
    end if;
    select * into v_contract
    from public.contracts
    where id=v_workflow.contract_id and candidate_id=v_candidate_id
    for update;
    if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    select * into v_week
    from public.contract_weeks
    where id=v_workflow.contract_week_id
      and contract_id=v_contract.id
      and week_ending_date=v_workflow.week_ending_date
    for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_IDENTITY_MISMATCH' using errcode='40001'; end if;
    if v_workflow.anchor_timesheet_id is not null then
      select cw.* into v_anchor_week
      from public.contract_weeks cw
      join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      where cw.timesheet_id=v_workflow.anchor_timesheet_id
        and cw.contract_id=v_contract.id
        and cw.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      if v_workflow.anchor_timesheet_id is null
         or coalesce((private._candidate_record_capabilities_v1(
           v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
         )->>'hours_value')::numeric,0)<=0
            and coalesce((private._candidate_record_capabilities_v1(
              v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
            )->>'additional_units_value')::numeric,0)<=0 then
        raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000';
      end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_workflow.target_timesheet_id is not null then
      raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='40001';
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_workflow.target_timesheet_id is distinct from v_week.timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_week.timesheet_id is not null then
      v_target_capabilities:=private._candidate_record_capabilities_v1(
        v_week.timesheet_id,v_week.id,'{}'::jsonb
      );
      if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
         or coalesce((v_target_capabilities->>'protected')::boolean,false)
         or not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false) then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
    end if;
    v_route_authority:=private._candidate_route_family_v1(
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_workflow.anchor_timesheet_id
        else v_week.timesheet_id end,
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end
    );
    if v_route_authority->>'route_family'='MANUAL_NON_QR'
       or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow.workflow_kind<>'CONTRACT_EXPENSE')
       or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
       or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'='QR') then
      raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
    end if;
    v_current_policy:=private._candidate_policy_resolve_v1(
      v_contract.client_id,v_contract.id,v_workflow.week_ending_date
    );
    if v_workflow.route='PAPER'
       and not coalesce((v_current_policy->>'paper_submission_enabled')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;
  if v_workflow.route='PAPER' then
    if v_workflow.state<>'RECEIVED'
       or v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
         where (
           select count(*)
           from public.candidate_submission_components returned_page
           where returned_page.workflow_id=v_workflow.id
             and returned_page.workflow_generation=v_workflow.generation
             and returned_page.component_kind='SIGNED_RETURN'
             and returned_page.paper_return_page_key=expected_page->>'page_key'
             and returned_page.state='IMMUTABLE'
             and returned_page.source_content_sha256 is not null
         )<>1
       )
       or exists(
         select 1
         from public.candidate_submission_components returned_page
         where returned_page.workflow_id=v_workflow.id
           and returned_page.workflow_generation=v_workflow.generation
           and returned_page.component_kind='SIGNED_RETURN'
           and returned_page.state='IMMUTABLE'
           and not exists(
             select 1
             from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
             where expected_page->>'page_key'=returned_page.paper_return_page_key
           )
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    end if;
  else
    if v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_approved_request
    from public.candidate_approval_requests a
      where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
        and a.state='APPROVED' and a.review_manifest_sha256=v_workflow.review_manifest_sha256
    for update;
    if not found then raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000'; end if;

    if not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) or exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and (c.state<>'IMMUTABLE' or c.review_render_state<>'READY'
          or c.final_signed_render_state<>'READY'
          or c.review_render_input_sha256 is distinct from c.final_signed_render_input_sha256)
    ) then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_hours_component from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='HOURS_TIMESHEET' and c.required=true and c.state='IMMUTABLE'
    for update;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY') and not found then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' and found then
      raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
    end if;
    if v_hours_component.id is not null and v_hours_component.review_render_input_sha256
       is distinct from v_hours_component.final_signed_render_input_sha256 then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
      select * into v_candidate_signature from public.candidate_submission_components c
      where c.id=v_workflow.candidate_signature_component_id and c.workflow_id=v_workflow.id
        and c.document_role='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE' for update;
    elsif v_workflow.candidate_signature_component_id is not null
       or v_workflow.candidate_signature_sha256 is not null then
      raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
    end if;
    select * into v_manager_signature from public.candidate_submission_components c
    where c.id=v_workflow.manager_signature_component_id and c.workflow_id=v_workflow.id
      and c.document_role='MANAGER_SIGNATURE' and c.state='IMMUTABLE'
      and c.approval_request_id=v_approved_request.id for update;
    if (v_workflow.workflow_kind<>'CONTRACT_EXPENSE' and (
          v_candidate_signature.id is null
          or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256))
       or v_manager_signature.id is null
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
       or v_workflow.manager_approved_at_utc is null then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    v_render_input:=private._candidate_render_input_v1(v_workflow.id,v_workflow.generation);
    if v_hours_component.id is not null and decode(v_render_input->>'render_input_sha256','hex')
       is distinct from decode(
         private._candidate_component_render_input_v1(
           v_workflow.id,v_workflow.generation,v_hours_component.id
         )->>'workflow_render_input_sha256','hex'
       ) then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC',
      'auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,
      'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',case when v_candidate_signature.source_content_sha256 is null then null
        else encode(v_candidate_signature.source_content_sha256,'hex') end,
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc
    );
  end if;
  if v_workflow.route='PAPER' then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;

  if coalesce(v_current_policy->>'policy_fingerprint','')
     is distinct from coalesce(v_workflow.policy_snapshot_json->>'policy_fingerprint','') then
    update public.candidate_approval_requests set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    v_response:=jsonb_build_object('ok',false,'error_code','CANDIDATE_POLICY_CHANGED','workflow_id',v_workflow.id,
      'state','SUPERSEDED','generation',v_workflow.generation+1,'current_policy',v_current_policy);
    update public.candidate_submission_workflows set state='SUPERSEDED',generation=generation+1,
      policy_snapshot_json=v_current_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_current_policy),
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
      case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
      case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
      v_response,p_now_utc
    );
    return v_response;
  end if;

  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;
  v_input:=v_workflow.immutable_submission_json;
  if v_input is null or private._candidate_sha256_jsonb_v1(v_input)
     is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_workflow.issue_codes:=private._candidate_submission_issue_codes_v1(
    v_workflow.id,v_input,v_current_policy
  );
  v_effective_separation:=coalesce((v_current_policy->>'expenses_require_separate_timesheet')::boolean,false);
  if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and coalesce((v_current_policy->>'import_expense_separation_mandatory')::boolean,false)
     and not coalesce((v_current_policy->>'expense_invoice_email_ready')::boolean,false) then
    raise exception 'EXPENSE_INVOICE_EMAIL_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.scope='WEEKLY' then
    select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
    if v_workflow.workflow_kind='CONTRACT_COMBINED' then
      v_hours_input:=coalesce(v_input->'hours_submission',v_input);
      v_expense_input:=coalesce(v_input->'expense_submission',v_input);
      if jsonb_typeof(v_hours_input)<>'object' or jsonb_typeof(v_expense_input)<>'object' then
        raise exception 'CANDIDATE_COMBINED_SNAPSHOTS_REQUIRED' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      v_expense_input:=v_input;
    else
      v_hours_input:=v_input;
    end if;

    if v_hours_input is not null then
      if v_hours_input->'canonical_tsfin_snapshot' is null then
        raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
      end if;
      perform set_config('cloudtms.candidate_electronic_finalise',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_hours_result:=public.contract_week_manual_upsert_atomic(
        p_week_id=>v_week.id,
        p_expected_timesheet_id=>v_workflow.target_timesheet_id,
        p_timesheet_create_json=>case when v_workflow.target_timesheet_id is null
          then coalesce(v_hours_input->'timesheet_create_json','{}'::jsonb)||v_electronic_patch else null end,
        p_timesheet_patch_json=>coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb)||v_electronic_patch,
        p_contract_week_patch_json=>coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb),
        p_tsfin_snapshot_json=>v_hours_input->'canonical_tsfin_snapshot',
        p_rotation_json=>null,p_actor_user_id=>v_system_actor,p_materialise_staged_evidence=>false,
        p_now_utc=>p_now_utc,p_expected_row_signature=>coalesce(p_expected_row_signature,v_workflow.expected_row_signature),
        p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
      );
      if coalesce((v_hours_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED' using errcode='55000',detail=v_hours_result::text;
      end if;
      v_hours_timesheet_id:=coalesce(nullif(v_hours_result->>'timesheet_id','')::uuid,
        nullif(v_hours_result#>>'{timesheet,timesheet_id}','')::uuid);
      v_after_signature:=coalesce(v_hours_result->>'row_signature',v_hours_result->>'backend_row_signature',
        v_hours_result#>>'{timesheet,row_signature}');
    end if;

    if v_expense_input is not null then
      if v_workflow.workflow_kind='CONTRACT_EXPENSE' or v_effective_separation then
        v_placement:=public.expense_carrier_resolve_or_create_atomic_v1(
          v_candidate_id,v_environment,coalesce(v_hours_timesheet_id,v_workflow.anchor_timesheet_id),
          coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature),
          p_idempotency_key||':carrier',p_now_utc);
      else
        v_placement:=jsonb_build_object(
          'placement','SAME_RECORD','target_timesheet_id',coalesce(v_hours_timesheet_id,v_workflow.target_timesheet_id),
          'target_contract_week_id',v_week.id);
      end if;
      if v_workflow.route='PAPER' then
        select array_agg(c.id order by c.component_no,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.component_kind='SIGNED_RETURN' and c.state='IMMUTABLE'
          and c.paper_return_page_key<>'HOURS_TIMESHEET';
      else
        select array_agg(c.id order by c.review_ordinal,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.required=true and c.state<>'SUPERSEDED' and c.component_kind<>'HOURS_TIMESHEET';
      end if;
      update public.candidate_submission_workflows set
        contract_week_id=nullif(v_placement->>'target_contract_week_id','')::uuid,
        target_timesheet_id=nullif(v_placement->>'target_timesheet_id','')::uuid,
        updated_at_utc=p_now_utc
      where id=v_workflow.id;
      perform set_config('cloudtms.candidate_finalize_workflow',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_result:=public.timesheet_expense_apply_atomic_v1(
        v_candidate_id,v_environment,nullif(v_placement->>'target_timesheet_id','')::uuid,
        v_workflow.id,v_workflow.generation,
        case when v_placement->>'placement'='SAME_RECORD' then coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature)
          else null end,
        v_expense_input,v_evidence_component_ids,p_idempotency_key||':expense',p_now_utc);
      v_expense_timesheet_id:=nullif(v_result->>'target_timesheet_id','')::uuid;
      v_target_timesheet_id:=coalesce(v_expense_timesheet_id,v_hours_timesheet_id);
      if v_hours_result is not null then
        v_result:=jsonb_build_object('ok',true,'hours_result',v_hours_result,'expense_result',v_result,
          'hours_timesheet_id',v_hours_timesheet_id,'expense_timesheet_id',v_expense_timesheet_id);
      end if;
    else
      v_result:=v_hours_result;
      v_target_timesheet_id:=v_hours_timesheet_id;
    end if;
  else
    if not v_is_office_service then
      perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
    v_target_timesheet_id:=v_workflow.target_timesheet_id;
    if v_target_timesheet_id is null then
      raise exception 'CANDIDATE_DAILY_TIMESHEET_REQUIRED' using errcode='22023';
    end if;
    v_daily_save_input:=private._candidate_daily_canonical_save_input_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_patch:=v_daily_save_input->'timesheet_patch_json';
    if jsonb_typeof(p_daily_materialisation_json)<>'object' then
      raise exception 'CANDIDATE_DAILY_MATERIALISATION_REQUIRED' using errcode='22023';
    end if;
    -- This private composition performs the pre-write row-signature check,
    -- factual save and bounded TSFIN write in this same finalisation transaction.
    -- Any later Process/Authorise error rolls the factual and financial write back.
    v_daily_save_receipt:=private._candidate_daily_save_recalculate_atomic_v1(
      v_workflow.id,v_workflow.generation,p_daily_materialisation_json,
      v_system_actor,p_now_utc
    );
    select * into v_daily_timesheet from public.timesheets
    where timesheet_id=v_target_timesheet_id and is_current=true for update;
    select * into v_daily_fin from public.timesheets_financials
    where id=nullif(v_daily_save_receipt->>'financials_id','')::uuid
      and timesheet_id=v_target_timesheet_id and is_current=true
    for update;
    if not found or v_daily_fin.processing_status<>'UNPROCESSED' then
      raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
    end if;
    v_after_signature:=nullif(v_daily_save_receipt->>'post_save_row_signature','');
    if v_after_signature is null then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
    end if;
    perform set_config('cloudtms.candidate_electronic_finalise','on',true);
    v_result:=public.timesheet_daily_manual_process_atomic(
      v_target_timesheet_id,v_target_timesheet_id,v_system_actor,
      v_electronic_patch,'{}'::jsonb,p_now_utc,v_after_signature
    );
    v_result:=jsonb_build_object(
      'ok',coalesce((v_result->>'ok')::boolean,false),
      'canonical_save_receipt',v_daily_save_receipt,
      'process_result',v_result,'timesheet_id',v_target_timesheet_id,
      'row_signature',coalesce(v_result->>'row_signature',v_result->>'backend_row_signature')
    );
    v_hours_timesheet_id:=v_target_timesheet_id;
    v_after_signature:=coalesce(v_result->>'row_signature',v_result->>'backend_row_signature');
  end if;
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'
      using errcode='55000',detail=jsonb_build_object(
        'code',coalesce(v_result->>'error_code','CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'),
        'canonical_result',v_result)::text;
  end if;
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_FINALISE_TARGET_MISSING' using errcode='55000'; end if;

  if v_workflow.route<>'PAPER' and v_hours_component.id is not null then
    update public.candidate_submission_components set timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_hours_component.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Official electronically signed timesheet',
      v_hours_component.final_signed_storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_hours_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  elsif v_workflow.route='PAPER' and v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_paper_hours_return
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='SIGNED_RETURN' and c.paper_return_page_key='HOURS_TIMESHEET'
      and c.state='IMMUTABLE' and c.source_content_sha256 is not null
    for update;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000'; end if;
    update public.candidate_submission_components set
      timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_paper_hours_return.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Returned signed paper timesheet',
      v_paper_hours_return.storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_paper_hours_return.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end if;

  v_auto_requested:=coalesce((v_current_policy->>'candidate_electronic_auto_authorise')::boolean,false)
    and v_workflow.route<>'PAPER';
  if v_workflow.issue_codes ?| array[
    'UNEXPECTED_HOURS','DAILY_BREAK_UNEXPECTED','DUPLICATE_EXPENSE_REVIEW',
    'HEALTHROSTER_VALIDATION_REQUIRED','EVIDENCE_REVIEW_REQUIRED',
    'ADDITIONAL_UNITS_NEEDS_CHECKING','PLANNED_HOURS_UNRESOLVED'
  ] then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||v_workflow.issue_codes;
  end if;
  if v_workflow.route='PAPER' then
    v_auto_blocked:=true;v_auto_blockers:=v_auto_blockers||'"PAPER_NEVER_AUTO_AUTHORISES"'::jsonb;
  end if;
  if v_auto_requested and not v_auto_blocked then
    if v_hours_timesheet_id is not null then
      v_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_hours_timesheet_id,v_hours_timesheet_id,v_system_actor,p_now_utc,v_after_signature
      );
      if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_authorise_result::text;
      end if;
    end if;
    if v_expense_timesheet_id is not null and v_expense_timesheet_id is distinct from v_hours_timesheet_id then
      v_expense_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_expense_timesheet_id,v_expense_timesheet_id,v_system_actor,p_now_utc,null);
      if coalesce((v_expense_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_expense_authorise_result::text;
      end if;
      v_authorise_result:=jsonb_build_object(
        'ok',true,'hours',v_authorise_result,'expenses',v_expense_authorise_result);
    end if;
    if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
      v_auto_blocked:=true;
      v_auto_blockers:=v_auto_blockers||jsonb_build_array(coalesce(v_authorise_result->>'error_code','AUTHORISE_NOT_ADVANCED'));
    end if;
  end if;

  select financials.id into v_canonical_financials_id
  from public.timesheets_financials financials
  where financials.timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    and financials.is_current=true
  order by financials.computed_at_utc desc nulls last,financials.updated_at desc,financials.id desc
  limit 1;
  if v_canonical_financials_id is null then
    raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='55000';
  end if;
  v_canonical_financial_sha256:=private._candidate_financial_content_sha256_v1(
    v_canonical_financials_id
  );

  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'state','FINALISED','generation',v_workflow.generation+1,
    'timesheet_id',v_target_timesheet_id,'canonical_result',v_result,
    'candidate_auto_authorise_effective',v_auto_requested,
    'auto_authorised',v_auto_requested and not v_auto_blocked,
    'canonical_financial_sha256_hex',encode(v_canonical_financial_sha256,'hex'),
    'auto_authorise_blockers',v_auto_blockers,
    'authorise_result',v_authorise_result
  );
  update public.candidate_submission_workflows set state='FINALISED',generation=generation+1,
    target_timesheet_id=v_target_timesheet_id,policy_snapshot_json=v_current_policy,
    canonical_financial_sha256=v_canonical_financial_sha256,
    issue_codes=v_workflow.issue_codes,finalised_at_utc=p_now_utc,
    last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_notification_insert_v1(v_workflow.account_id,v_candidate_id,v_workflow.id,v_target_timesheet_id,
    case when v_auto_requested and not v_auto_blocked then 'AUTHORISED' else 'SUBMISSION_RECEIVED' end,
    'authorisation','candidate-submission-finalised-v1',jsonb_build_object('auto_authorised',v_auto_requested and not v_auto_blocked),
    jsonb_build_object('type','timesheet','timesheet_id',v_target_timesheet_id),
    'CANDIDATE_FINALISED_V1:'||v_workflow.id::text||':'||(v_workflow.generation+1)::text,p_now_utc);
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,'CANDIDATE_SUBMISSION_FINALISED',
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state','FINALISED','generation',v_workflow.generation+1,'timesheet_id',v_target_timesheet_id,
      'auto_authorised',v_auto_requested and not v_auto_blocked),null,v_system_actor,p_idempotency_key,p_now_utc);
  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
    case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
    v_response,p_now_utc
  );
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
      raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
    end if;
    raise;
end;
$function$;

create or replace function private._candidate_timesheet_reject_rotate_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_reason text,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_new_timesheet_id uuid;
  v_new_version integer;
begin
  select current_row.* into v_current
  from public.timesheets current_row
  join public.timesheets requested_row on requested_row.booking_id=current_row.booking_id
  where requested_row.timesheet_id=p_timesheet_id and current_row.is_current=true
  for update of current_row;
  if not found then raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002'; end if;
  if v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true for update;
  if v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_REJECT_PROTECTED_HISTORY' using errcode='55000';
  end if;
  v_new_version:=coalesce(v_current.version,1)+1;
  update public.timesheets set is_current=false,status='REVOKED',
    revoked_reason=btrim(p_reason),revoked_by=p_actor_user_id::text,updated_at=p_now_utc
  where timesheet_id=v_current.timesheet_id and is_current=true;
  insert into public.timesheets(
    booking_id,version,is_current,status,contract_id,submission_mode,line_type,sheet_scope,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,week_ending_date,
    worked_start_iso,worked_end_iso,break_start_iso,break_end_iso,break_minutes,actual_schedule_json,
    additional_units_week,additional_units_per_day,manual_pdf_r2_key,authorised_at_server,
    reference_number,day_references_json,qr_token,qr_status,qr_payload_json,qr_generated_at,
    qr_scanned_at,qr_scan_info_json,qr_r2_key,created_at,updated_at
  ) values (
    v_current.booking_id,v_new_version,true,'RECEIVED',v_current.contract_id,'MANUAL',
    v_current.line_type,v_current.sheet_scope,v_current.occupant_key_norm,v_current.hospital_norm,
    v_current.ward_norm,v_current.job_title_norm,v_current.shift_label_norm,v_current.week_ending_date,
    null,null,null,null,null,null,'{}'::jsonb,'{}'::jsonb,null,null,null,null,
    null,null,'{}'::jsonb,null,null,null,null,p_now_utc,p_now_utc
  ) returning timesheet_id into v_new_timesheet_id;
  update public.contract_weeks set timesheet_id=v_new_timesheet_id,status='OPEN',
    day_entries_json='[]'::jsonb,totals_json='{}'::jsonb,updated_at=p_now_utc
  where timesheet_id=v_current.timesheet_id;
  if v_fin.id is not null then
    update public.timesheets_financials set
      timesheet_id=v_new_timesheet_id,timesheet_version=v_new_version,processing_status='UNPROCESSED',
      worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,break_minutes=null,
      actual_schedule_json=null,actual_minutes_by_day_json=null,
      hours_day=0,hours_night=0,hours_sat=0,hours_sun=0,hours_bh=0,total_hours=0,
      total_pay_ex_vat=0,total_charge_ex_vat=0,margin_ex_vat=0,
      additional_pay_ex_vat=0,additional_charge_ex_vat=0,additional_margin_ex_vat=0,
      additional_units_json='{}'::jsonb,expenses_pay_ex_vat=0,expenses_charge_ex_vat=0,
      expenses_description=null,expenses_evidence_r2_key=null,expenses_evidence_manifest=null,
      mileage_units=0,mileage_pay_ex_vat=0,mileage_charge_ex_vat=0,
      mileage_evidence_r2_key=null,mileage_evidence_manifest=null,
      travel_pay_ex_vat=0,travel_charge_ex_vat=0,accommodation_pay_ex_vat=0,
      accommodation_charge_ex_vat=0,other_pay_ex_vat=0,other_charge_ex_vat=0,
      authorised_at_utc=null,authorised_by_user_id=null,updated_at=p_now_utc
    where id=v_fin.id;
    insert into public.ts_financials_outbox(timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
    values (v_new_timesheet_id,'REVOKED',0,p_now_utc,null,p_now_utc)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;
  perform private._candidate_audit_v1('timesheet',v_new_timesheet_id::text,
    'CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED',
    jsonb_build_object('old_timesheet_id',v_current.timesheet_id,'old_version',v_current.version),
    jsonb_build_object('new_timesheet_id',v_new_timesheet_id,'new_version',v_new_version),
    btrim(p_reason),p_actor_user_id,null,p_now_utc);
  return v_new_timesheet_id;
end;
$function$;

create or replace function public.candidate_submission_reject_atomic_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_reason text,
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
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_week public.contract_weeks%rowtype;
  v_signature jsonb;
  v_capabilities jsonb;
  v_qr_result record;
  v_new_timesheet_id uuid;
  v_qr_backed boolean:=false;
  v_reject_scope text;
  v_workflow record;
  v_rejected_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_generations integer[]:='{}'::integer[];
  v_paper_retirement_result jsonb;
  v_rejection_family_contract_id uuid;
  v_rejection_family_week_ending_date date;
  v_rejection_family_key text;
  v_rejection_request_hash text;
  v_rejection_receipt_before jsonb;
  v_response jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if not private._candidate_office_service_context_valid_v1(
    v_environment,p_actor_user_id,'REJECT_CONFIRM'
  ) then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if p_actor_user_id is null or p_timesheet_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or nullif(btrim(coalesce(p_reason,'')),'') is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_REJECT_PAYLOAD_INVALID' using errcode='22023';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_REJECTION_IDEMPOTENCY:'||p_actor_user_id::text||':'||p_idempotency_key,0
  ));
  -- Serialise every Candidate rejection for the same contract/week before
  -- locking either the hours row or a separate expense carrier. This prevents
  -- the inverse target/workflow/source lock order that can deadlock H1/E1.
  select target.contract_id,target.week_ending_date
  into v_rejection_family_contract_id,v_rejection_family_week_ending_date
  from public.timesheets target
  where target.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;
  v_rejection_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
    ||coalesce(v_rejection_family_contract_id::text,'-')||':'
    ||coalesce(v_rejection_family_week_ending_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_rejection_family_key,0));

  v_rejection_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_REJECTION_REQUEST_V2',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'timesheet_id',p_timesheet_id,
    'expected_timesheet_id',p_expected_timesheet_id,
    'expected_row_signature',btrim(p_expected_row_signature),
    'reason',btrim(p_reason)
  )::text,'UTF8'),'sha256'),'hex');
  select ae.before_json,ae.after_json into v_rejection_receipt_before,v_response
  from public.audit_events ae
  where ae.object_type='candidate_submission_rejection_receipt'
    and ae.actor_user_id=p_actor_user_id
    and ae.correlation_id=p_idempotency_key
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    if v_rejection_receipt_before->>'request_sha256' is distinct from v_rejection_request_hash then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT','idempotency_key',p_idempotency_key
        )::text;
    end if;
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;

  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id for update;
  if not found then raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.timesheet_id<>p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true for update;
  if v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_REJECT_REQUIRES_UNAUTHORISE' using errcode='55000';
  end if;
  if v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_REJECT_PROTECTED_HISTORY' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false);
  if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001';
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  if coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)=false then
    raise exception 'CANDIDATE_REJECT_NOT_ALLOWED' using errcode='55000',detail=v_capabilities::text;
  end if;
  v_reject_scope:=v_capabilities->>'reject_scope';
  v_qr_backed:=v_timesheet.qr_status is not null or v_timesheet.qr_token is not null or v_timesheet.qr_r2_key is not null
    or exists(select 1 from public.candidate_submission_workflows w where w.target_timesheet_id=v_timesheet.timesheet_id and w.route='PAPER');

  for v_workflow in
    select w.id,w.generation,w.route,w.state
    from public.candidate_submission_workflows w
    where w.environment=v_environment
      and (
        (
          w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
          and (
            w.target_timesheet_id=v_timesheet.timesheet_id
            or w.anchor_timesheet_id=v_timesheet.timesheet_id
          )
        )
        or (
          w.state='FINALISED'
          and w.target_timesheet_id=v_timesheet.timesheet_id
        )
      )
    order by w.id
    for update
  loop
    v_rejected_workflow_ids:=array_append(v_rejected_workflow_ids,v_workflow.id);
    if v_workflow.route='PAPER'
       and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
      v_paper_workflow_ids:=array_append(v_paper_workflow_ids,v_workflow.id);
      v_paper_workflow_generations:=array_append(
        v_paper_workflow_generations,v_workflow.generation
      );
    end if;
  end loop;

  if cardinality(v_paper_workflow_ids)>0 then
    v_paper_retirement_result:=private._candidate_paper_delivery_retire_set_v1(
      v_paper_workflow_ids,v_paper_workflow_generations,
      'OFFICE_REJECTED',p_now_utc
    );
    if not coalesce((v_paper_retirement_result->>'retired')::boolean,false)
       or not coalesce(
         (v_paper_retirement_result->>'qr_invalidation_proven')::boolean,false
       ) then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
          'workflow_ids',to_jsonb(v_paper_workflow_ids),
          'retirement_receipt',v_paper_retirement_result
        )::text;
    end if;
  end if;

  if v_qr_backed then
    select * into v_qr_result from public.timesheet_qr_refuse_and_reset(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id
    );
    v_new_timesheet_id:=v_qr_result.timesheet_id;
  else
    v_new_timesheet_id:=private._candidate_timesheet_reject_rotate_v1(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id,p_now_utc);
  end if;
  if v_new_timesheet_id is null then raise exception 'CANDIDATE_REJECT_ROTATION_FAILED' using errcode='55000'; end if;

  update public.timesheets set
    authorised_at_server=null,worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,actual_schedule_json=null,additional_units_week='{}'::jsonb,additional_units_per_day='{}'::jsonb,
    manual_pdf_r2_key=null,reference_number=null,day_references_json=null,
    qr_token=null,qr_status=case when v_qr_backed then 'PENDING'::public.timesheet_qr_status_enum else null end,
    qr_payload_json='{}'::jsonb,qr_generated_at=null,qr_scanned_at=null,qr_scan_info_json=null,qr_r2_key=null,
    qr_last_sent_hash=null,qr_last_sent_at_utc=null,qr_signed_hash=null,qr_signed_at_utc=null,
    updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id;

  update public.timesheets_financials set
    processing_status='UNPROCESSED',worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,hours_day=0,hours_night=0,hours_sat=0,hours_sun=0,hours_bh=0,total_hours=0,
    additional_units_json='{}'::jsonb,additional_pay_ex_vat=0,additional_charge_ex_vat=0,additional_margin_ex_vat=0,
    expenses_pay_ex_vat=0,expenses_charge_ex_vat=0,expenses_description=null,expenses_evidence_r2_key=null,
    expenses_evidence_manifest=null,mileage_units=0,mileage_pay_ex_vat=0,mileage_charge_ex_vat=0,
    mileage_evidence_r2_key=null,mileage_evidence_manifest=null,travel_pay_ex_vat=0,travel_charge_ex_vat=0,
    accommodation_pay_ex_vat=0,accommodation_charge_ex_vat=0,other_pay_ex_vat=0,other_charge_ex_vat=0,
    actual_schedule_json=null,actual_minutes_by_day_json=null,total_pay_ex_vat=0,total_charge_ex_vat=0,margin_ex_vat=0,
    authorised_at_utc=null,authorised_by_user_id=null,updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id and is_current=true;

  update public.contract_weeks set status='OPEN',day_entries_json='[]'::jsonb,
    totals_json='{}'::jsonb,updated_at=p_now_utc where id=v_week.id;
  update public.timesheet_evidence set processing_state='SUPERSEDED'
  where timesheet_id=v_timesheet.timesheet_id and processing_state<>'SUPERSEDED';
  for v_workflow in
    select w.id,w.account_id,w.candidate_id,w.generation,w.state as captured_state,
      case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end as artifact_generation
    from public.candidate_submission_workflows w
    where w.id=any(v_rejected_workflow_ids)
    order by w.id
    for update
  loop
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state in ('PENDING','APPROVED');
    update public.candidate_submission_components set
      state='REJECTED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state not in ('REJECTED','SUPERSEDED','ABANDONED');
    update public.candidate_submission_workflows set
      state='REJECTED',generation=v_workflow.generation+1,
      rejection_reason=btrim(p_reason),rejection_scope=v_reject_scope,updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation
      and state=v_workflow.captured_state
      and state not in ('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED');
    if not found then
      raise exception 'CANDIDATE_REJECT_WORKFLOW_CONFLICT' using errcode='40001';
    end if;
    perform private._candidate_notification_insert_v1(v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,v_new_timesheet_id,
      'OFFICE_REJECTED','office_rejection','candidate-office-rejected-v1',
      jsonb_build_object('reason',btrim(p_reason),'resubmission_scope',v_reject_scope),
      jsonb_build_object('type','timesheet','timesheet_id',v_new_timesheet_id),
      'CANDIDATE_OFFICE_REJECTED_V1:'||v_workflow.id::text||':'||(v_workflow.generation+1)::text,p_now_utc);
  end loop;
  v_response:=jsonb_build_object(
    'ok',true,'old_timesheet_id',v_timesheet.timesheet_id,'timesheet_id',v_new_timesheet_id,
    'contract_week_id',v_week.id,'contract_week_status','OPEN','processing_status','UNPROCESSED',
    'rejection_scope',v_reject_scope,'qr_reissue_required',v_qr_backed,
    'paper_retirement_receipt',v_paper_retirement_result,
    'idempotency_key',p_idempotency_key
  );
  perform private._candidate_audit_v1('timesheet',v_new_timesheet_id::text,'CANDIDATE_SUBMISSION_REJECTED',
    jsonb_build_object('old_timesheet_id',v_timesheet.timesheet_id),v_response,btrim(p_reason),
    p_actor_user_id,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'candidate_submission_rejection_receipt',p_timesheet_id::text,
    'CANDIDATE_SUBMISSION_REJECTION_RECEIPT',jsonb_build_object(
      'request_sha256',v_rejection_request_hash,'contract_version','CANDIDATE_REJECTION_REQUEST_V2'
    ),v_response,btrim(p_reason),p_idempotency_key,p_now_utc
  );
  return v_response;
end;
$function$;

create or replace function public.candidate_no_work_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_week_id uuid,
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
  v_context jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_system_actor uuid;
  v_preview jsonb;
  v_result jsonb;
  v_response jsonb;
  v_signature text;
  v_route_authority jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null or p_contract_week_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_NO_WORK_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select ae.after_json into v_response
  from public.audit_events ae
  where ae.object_type='contract_week'
    and ae.object_id_text=p_contract_week_id::text
    and ae.action='CANDIDATE_NO_WORK'
    and ae.correlation_id=p_idempotency_key
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  select * into v_week from public.contract_weeks where id=p_contract_week_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;
  v_route_authority:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id);
  if not coalesce((v_route_authority->>'candidate_no_work_allowed')::boolean,false) then
    raise exception 'CANDIDATE_NO_WORK_NOT_ALLOWED' using errcode='55000',detail=v_route_authority::text;
  end if;

  if v_week.timesheet_id is null then
    if v_week.status not in ('PLANNED','OPEN') then raise exception 'CANDIDATE_NO_WORK_NOT_ALLOWED' using errcode='55000'; end if;
    select to_jsonb(x) into v_result from public.contract_week_delete_planned(v_week.id,v_system_actor) x;
    v_response:=jsonb_build_object('ok',true,'action','DELETE_PLANNED','contract_week_id',v_week.id,
      'result',v_result,'candidate_hidden',true,'idempotency_key',p_idempotency_key,'idempotent_replay',false);
    perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_NO_WORK',
      jsonb_build_object('timesheet_id',null),v_response,null,v_system_actor,p_idempotency_key,p_now_utc);
    return v_response;
  end if;

  v_signature:=coalesce(nullif(btrim(p_expected_row_signature),''),
    public.timesheet_lifecycle_guard_signature_v1(v_week.timesheet_id,v_week.id,false)->>'row_signature');
  if v_signature is null then raise exception 'CANDIDATE_ROW_SIGNATURE_REQUIRED' using errcode='22023'; end if;
  v_preview:=public.timesheet_standard_delete_preview_v1(
    v_week.timesheet_id,v_system_actor,v_week.timesheet_id,v_signature
  );
  if v_preview->>'decision'='PERMANENT_DELETE' then
    v_result:=public.timesheet_standard_delete_apply_v1(
      v_week.timesheet_id,v_system_actor,v_week.timesheet_id,v_signature
    );
  elsif v_preview->>'decision'='ARCHIVE_REQUIRED' then
    v_result:=public.timesheet_archive_transition_v1(
      v_week.timesheet_id,'ARCHIVE','STANDARD_DELETE',v_system_actor,v_week.timesheet_id,v_signature,p_now_utc
    );
  else
    raise exception 'CANDIDATE_NO_WORK_BLOCKED' using errcode='55000',detail=v_preview::text;
  end if;
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_NO_WORK_BLOCKED' using errcode='55000',detail=v_result::text;
  end if;
  v_response:=jsonb_build_object('ok',true,'contract_week_id',v_week.id,'timesheet_id',v_week.timesheet_id,
    'decision',v_preview->>'decision','result',v_result,'candidate_hidden',true,
    'idempotency_key',p_idempotency_key,'idempotent_replay',false);
  perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_NO_WORK',
    jsonb_build_object('timesheet_id',v_week.timesheet_id),v_response,null,v_system_actor,p_idempotency_key,p_now_utc);
  return v_response;
end;
$function$;

revoke all on function private._candidate_daily_rate_contract_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_context_contract_v1(uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private._candidate_financial_content_sha256_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_save_recalculate_atomic_v1(uuid,integer,jsonb,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) from public,anon,authenticated;
revoke all on function public.candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_no_work_atomic_v1(uuid,text,uuid,text,text,timestamptz) from public,anon,authenticated;
revoke all on function private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) to service_role;
grant execute on function public.candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamptz) to service_role;
grant execute on function public.candidate_no_work_atomic_v1(uuid,text,uuid,text,text,timestamptz) to service_role;
