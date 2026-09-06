-- Daily unresolved receipt adapter. Existing financial save/process/authorise owners remain unchanged.
begin;

-- Finalisation recalculates policy-derived issue codes, but the Candidate has
-- already explicitly confirmed any duplicate-expense warning at submission.
-- Preserve only those server-owned duplicate markers so Office can require an
-- individual review after the claim is materialised onto its final Timesheet.
create or replace function private._candidate_finalisation_issue_codes_v1(
  p_existing_issue_codes jsonb,
  p_recomputed_issue_codes jsonb
)
returns jsonb
language sql
immutable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  with retained_codes as (
    select recomputed_code.value as issue_code
    from jsonb_array_elements_text(coalesce(p_recomputed_issue_codes,'[]'::jsonb))
      recomputed_code(value)
    union
    select existing_code.value as issue_code
    from jsonb_array_elements_text(coalesce(p_existing_issue_codes,'[]'::jsonb))
      existing_code(value)
    where existing_code.value in (
      'DUPLICATE_EXPENSE_REVIEW',
      'DUPLICATE_EXPENSE_MILEAGE',
      'DUPLICATE_EXPENSE_TRAVEL',
      'DUPLICATE_EXPENSE_ACCOMMODATION',
      'DUPLICATE_EXPENSE_OTHER'
    )
  )
  select coalesce(jsonb_agg(retained_codes.issue_code order by retained_codes.issue_code),'[]'::jsonb)
  from retained_codes;
$function$;

revoke all on function private._candidate_finalisation_issue_codes_v1(jsonb,jsonb)
  from public,anon,authenticated,service_role;

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
set lock_timeout = '5s'
set statement_timeout = '120s'
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
  v_daily_receipt_context jsonb;
  v_daily_receipt_only boolean:=false;
  v_completion_state text:='FINALISED';
  v_completion_generation integer;
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
  v_key_replay_probe_only boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_request_hash text;
  v_mutation_receipt jsonb;
  v_prior_receipt_before jsonb;
  v_prior_receipt_after jsonb;
  v_finalisation_identity jsonb;
  v_finalisation_identity_hash text;
  v_completion_before jsonb;
  v_completion_after jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  v_service_finalisation:=coalesce(p_daily_materialisation_json->'service_finalisation','{}'::jsonb);
  v_is_office_service:=p_session_id is null
    and private._candidate_office_service_context_valid_v1(
      v_environment,nullif(v_service_finalisation->>'actor_user_id','')::uuid,'RETRY_FINALISATION'
    );
  v_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_probe_only')::boolean,false);
  v_key_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_key_probe_only')::boolean,false);
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
  if v_key_replay_probe_only then
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
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object(
      'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation
    );
  end if;
  -- Candidate-session calls retain their established lifecycle errors before
  -- an immutable approval identity exists. Service replay/finalisation calls
  -- continue through the receipt path below before mutable lifecycle checks.
  if p_session_id is not null then
    if v_workflow.route='PAPER' and v_workflow.state<>'RECEIVED' then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    elsif v_workflow.route<>'PAPER' and v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
  end if;
  v_finalisation_identity:=v_service_finalisation->'finalisation_identity';
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object' then
    if v_workflow.route='PAPER' then
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method','PAPER','approval_request_id',null,
        'approval_request_generation',null,'review_manifest_sha256_hex',null,
        'paper_return_manifest_sha256_hex',case when v_workflow.paper_return_manifest_sha256 is null
          then null else encode(v_workflow.paper_return_manifest_sha256,'hex') end
      );
    else
      select approved.* into v_approved_request
      from public.candidate_approval_requests approved
      where approved.workflow_id=v_workflow.id
        and approved.workflow_generation=p_expected_generation
        and approved.state='APPROVED'
        and (nullif(v_service_finalisation->>'approval_request_id','') is null
          or approved.id=(v_service_finalisation->>'approval_request_id')::uuid)
      order by approved.approved_at_utc desc,approved.id desc
      limit 1;
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method',coalesce(v_approved_request.method,v_service_finalisation->>'approval_method'),
        'approval_request_id',coalesce(v_approved_request.id,
          nullif(v_service_finalisation->>'approval_request_id','')::uuid),
        'approval_request_generation',v_approved_request.request_generation,
        'review_manifest_sha256_hex',case when v_approved_request.review_manifest_sha256 is null
          then null else encode(v_approved_request.review_manifest_sha256,'hex') end,
        'paper_return_manifest_sha256_hex',null
      );
    end if;
    v_service_finalisation:=v_service_finalisation||jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',p_expected_generation,
      'approval_method',v_finalisation_identity->>'approval_method',
      'approval_request_id',v_finalisation_identity->'approval_request_id',
      'approval_request_generation',v_finalisation_identity->'approval_request_generation',
      'review_manifest_sha256_hex',coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''),
      'paper_return_manifest_sha256_hex',coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''),
      'finalisation_identity',v_finalisation_identity
    );
  end if;
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object'
     or coalesce(v_finalisation_identity->>'contract_version','')
          <>'CANDIDATE_FINALISATION_IDENTITY_V1'
     or nullif(v_finalisation_identity->>'workflow_id','')::uuid is distinct from v_workflow.id
     or coalesce((v_finalisation_identity->>'workflow_generation')::integer,0)
          <>p_expected_generation
     or upper(coalesce(v_finalisation_identity->>'approval_method','')) not in ('EMAIL','PHONE','PAPER') then
    raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
      using errcode='28000',detail=jsonb_build_object('stage','IDENTITY')::text;
  end if;
  v_finalisation_identity_hash:=encode(extensions.digest(convert_to(
    v_finalisation_identity::text,'UTF8'
  ),'sha256'),'hex');
  v_mutation_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V3',
    'workflow_id',v_workflow.id,
    'action','RETRY_FINALISATION',
    'expected_generation',p_expected_generation,
    'service_finalisation',v_service_finalisation-'replay_probe_only',
    'channel',v_mutation_channel,
    'actor_identity',v_mutation_actor_identity
  )::text,'UTF8'),'sha256'),'hex');
  if v_replay_probe_only then
    if p_session_id is null and (
      coalesce(v_service_finalisation->>'contract_version','')
        <>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)
         <>p_expected_generation
    ) then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','REPLAY_ENVELOPE')::text;
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
      if v_prior_receipt_before->>'request_sha256' is distinct from v_mutation_request_hash
         or upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    select ae.before_json,ae.after_json
    into v_completion_before,v_completion_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_finalisation_completion'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_completion_before->>'finalisation_identity_sha256'
           is distinct from v_finalisation_identity_hash
         or nullif(v_completion_after->>'generation','')::integer
           is distinct from (p_expected_generation+case
             when v_workflow.workflow_kind='DAILY'
               and v_completion_after->>'state'='RECEIVED'
               and v_completion_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
      end if;
      return coalesce(v_completion_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object('ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation);
  end if;
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    v_mutation_channel,v_mutation_actor_identity,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return coalesce(v_mutation_receipt->'response','{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  select ae.before_json,ae.after_json
  into v_completion_before,v_completion_after
  from public.audit_events ae
  where ae.object_type='candidate_workflow_finalisation_completion'
    and ae.object_id_text=v_workflow.id::text
    and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_completion_after,'{}'::jsonb)
      ||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_session_id is null then
    if coalesce(v_service_finalisation->>'contract_version','')<>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)<>v_workflow.generation
       or upper(coalesce(v_service_finalisation->>'approval_method',''))<>v_workflow.route then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','SERVICE_ENVELOPE')::text;
    end if;
    if v_workflow.route='PAPER' then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null
         or nullif(v_finalisation_identity->>'approval_request_id','') is not null
         or upper(v_finalisation_identity->>'approval_method')<>'PAPER'
         or lower(coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''))
              <>encode(v_workflow.paper_return_manifest_sha256,'hex') then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','PAPER_IDENTITY')::text;
      end if;
    else
      select * into v_approved_request
      from public.candidate_approval_requests a
      where a.id=nullif(v_service_finalisation->>'approval_request_id','')::uuid
        and a.workflow_id=v_workflow.id
        and a.workflow_generation=p_expected_generation
        and a.request_generation=coalesce(
          nullif(v_service_finalisation->>'approval_request_generation','')::integer,0
        )
        and a.method=upper(coalesce(v_service_finalisation->>'approval_method',''))
        and a.state='APPROVED'
        and encode(a.review_manifest_sha256,'hex')=lower(coalesce(v_service_finalisation->>'review_manifest_sha256_hex',''))
        and v_finalisation_identity->>'approval_request_id'=a.id::text
        and coalesce((v_finalisation_identity->>'approval_request_generation')::integer,0)=a.request_generation
        and upper(v_finalisation_identity->>'approval_method')=a.method
        and lower(coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''))
              =encode(a.review_manifest_sha256,'hex')
      for update;
      if not found then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','APPROVAL_IDENTITY')::text;
      end if;
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
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_daily_receipt_only:=coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
      and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false);
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
    if (not found and not v_daily_receipt_only)
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
    if v_daily_receipt_only then
      v_current_policy:=v_daily_receipt_context->'policy';
    else
      if coalesce(v_daily_fin.client_id,v_contract.client_id) is null then
        raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002';
      end if;
      v_current_policy:=private._candidate_policy_resolve_v1(
        coalesce(v_daily_fin.client_id,v_contract.client_id),v_contract.id,v_workflow.work_date
      );
    end if;
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
  v_workflow.issue_codes:=private._candidate_finalisation_issue_codes_v1(
    v_workflow.issue_codes,
    private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_input,v_current_policy
    )
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
    if v_daily_receipt_only then
      if p_daily_materialisation_json->>'contract_version' is distinct from 'CANDIDATE_DAILY_FACTUAL_RECEIPT_V1'
         or p_daily_materialisation_json->>'workflow_id' is distinct from v_workflow.id::text
         or (p_daily_materialisation_json->>'workflow_generation')::integer is distinct from v_workflow.generation
         or p_daily_materialisation_json->>'timesheet_id' is distinct from v_target_timesheet_id::text then
        raise exception 'CANDIDATE_DAILY_RECEIPT_CONTEXT_CHANGED' using errcode='40001';
      end if;
      v_result:=private._candidate_daily_factual_receipt_v1(
        v_workflow.id,v_workflow.generation,
        p_daily_materialisation_json->>'canonical_save_input_sha256_hex',v_electronic_patch,p_now_utc);
      v_completion_state:='RECEIVED';
    else
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
    end if;
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
  if v_daily_receipt_only then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||'"OFFICE_RESOLUTION_REQUIRED"'::jsonb;
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
  if not v_daily_receipt_only then
    if v_canonical_financials_id is null then
      raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='55000';
    end if;
    v_canonical_financial_sha256:=private._candidate_financial_content_sha256_v1(
      v_canonical_financials_id
    );
  end if;
  -- A received factual claim retains its artifact generation. It is not
  -- financial finalisation and never carries a fabricated financial hash.
  v_completion_generation:=v_workflow.generation+case when v_daily_receipt_only then 0 else 1 end;

  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'state',v_completion_state,'generation',v_completion_generation,
    'office_resolution_pending',v_daily_receipt_only,
    'timesheet_id',v_target_timesheet_id,'canonical_result',v_result,
    'candidate_auto_authorise_effective',v_auto_requested,
    'auto_authorised',v_auto_requested and not v_auto_blocked,
    'canonical_financial_sha256_hex',encode(v_canonical_financial_sha256,'hex'),
    'auto_authorise_blockers',v_auto_blockers,
    'authorise_result',v_authorise_result
  );
  update public.candidate_submission_workflows set state=v_completion_state,generation=v_completion_generation,
    target_timesheet_id=v_target_timesheet_id,policy_snapshot_json=v_current_policy,
    canonical_financial_sha256=v_canonical_financial_sha256,
    issue_codes=v_workflow.issue_codes,
    finalised_at_utc=case when v_daily_receipt_only then null else p_now_utc end,
    last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_notification_insert_v1(v_workflow.account_id,v_candidate_id,v_workflow.id,v_target_timesheet_id,
    case when v_auto_requested and not v_auto_blocked then 'AUTHORISED' else 'SUBMISSION_RECEIVED' end,
    'authorisation','candidate-submission-finalised-v1',jsonb_build_object('auto_authorised',v_auto_requested and not v_auto_blocked),
    jsonb_build_object('type','timesheet','timesheet_id',v_target_timesheet_id),
    'CANDIDATE_FINALISED_V1:'||v_workflow.id::text||':'||v_completion_generation::text,p_now_utc);
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    case when v_daily_receipt_only then 'CANDIDATE_SUBMISSION_RECEIVED' else 'CANDIDATE_SUBMISSION_FINALISED' end,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state',v_completion_state,'generation',v_completion_generation,'timesheet_id',v_target_timesheet_id,
      'auto_authorised',v_auto_requested and not v_auto_blocked),null,v_system_actor,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    case when v_is_office_service then nullif(v_service_finalisation->>'actor_user_id','')::uuid
      else null end,
    'candidate_workflow_finalisation_completion',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_FINALISATION_COMPLETED',jsonb_build_object(
      'contract_version','CANDIDATE_FINALISATION_COMPLETION_V1',
      'workflow_generation',p_expected_generation,
      'finalisation_identity_sha256',v_finalisation_identity_hash,
      'finalisation_identity',v_finalisation_identity
    ),v_response,'Canonical finalisation completion receipt',
    p_expected_generation::text||':'||v_finalisation_identity_hash,p_now_utc
  );
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

alter function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) owner to postgres;
revoke all on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) to service_role;
commit;
