-- SP-12: seven actual outer/scope-restoration entries of Source gate writers.
-- Existing signatures, authority, lifecycle and money statements retained.
-- Rule-B cut unchanged; no F-25 producer protocol.
\set ON_ERROR_STOP on
begin;

-- Current predecessor: supabase/repeatable/24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql
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

  -- SP-12: plain existing session authentication, then early admission.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    if p_session_id is not null then
      v_context:=private._candidate_session_context_v1(
        p_session_id,v_environment,null,p_now_utc,false
      );
      v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
      if v_candidate_id is null then
        raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
      end if;
    end if;
    perform private.weekly_source_pay_query_admit_v2();
  end if;

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
        -- Reuse the existing expense-admission authority for source weeks:
        -- immutable Candidate worked-hours evidence may precede source TSFIN.
        -- This neither writes nor unlocks the source-owned hours record.
        if v_workflow.anchor_timesheet_id is null
           or not coalesce((private._candidate_record_capabilities_v1(
             v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
           )->>'import_authoritative')::boolean,false) then
          raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000';
        end if;
        v_placement:=public.expense_placement_resolve_v1(
          v_candidate_id,v_environment,v_workflow.anchor_timesheet_id,
          v_anchor_week.id,'{}'::jsonb,p_now_utc);
        if not coalesce((v_placement->>'ok')::boolean,false)
           or coalesce(v_placement->>'placement','') not in ('REUSE_CARRIER','CREATE_CARRIER') then
          raise exception 'CANDIDATE_EXPENSE_FINALISATION_ADMISSION_BLOCKED'
            using errcode='55000',detail=coalesce(v_placement->>'reason_code','NO_POSITIVE_WORKED_TIME');
        end if;
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
        -- A first combined weekly submission has no anchor until its hours row
        -- is materialised above. Bind that worked row before expense apply so
        -- SAME_RECORD stays a combined HOURS Timesheet. A genuinely separate
        -- expense carrier still has a different target and remains EXPENSES.
        anchor_timesheet_id=case when v_workflow.workflow_kind='CONTRACT_COMBINED'
          then coalesce(anchor_timesheet_id,v_hours_timesheet_id)
          else anchor_timesheet_id end,
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

alter function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb) owner to postgres;

-- Current predecessor: supabase/repeatable/04092026_1420_candidate_finalisation_single_flight_v1.sql
create or replace function public.candidate_submission_finalize_single_flight_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text,
  p_idempotency_key text,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if p_workflow_id is null or p_expected_generation is null or p_expected_generation < 1 then
    raise exception 'CANDIDATE_FINALISATION_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  -- SP-12: reuse current child plain authentication before either business key.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    declare
      v_environment text;
      v_service_finalisation jsonb;
      v_is_office_service boolean;
      v_replay_probe_only boolean;
      v_key_replay_probe_only boolean;
      v_context jsonb;
      v_candidate_id uuid;
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
      if p_session_id is not null then
        v_context:=private._candidate_session_context_v1(
          p_session_id,v_environment,null,p_now_utc,false
        );
        v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
        if v_candidate_id is null then
          raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
        end if;
      end if;
      perform private.weekly_source_pay_query_admit_v2();
    end;
  end if;

  -- Finalisation is normally a short transaction. A single global non-waiting
  -- gate reserves the remaining PostgREST pool for login, reads and unrelated
  -- app actions even if several independent claims become stuck at once.
  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended('CANDIDATE_FINALISATION_GLOBAL', 0)
  ) then
    return jsonb_build_object(
      'ok',true,
      'workflow_id',p_workflow_id,
      'generation',p_expected_generation,
      'state','FINALISATION_PENDING',
      'finalisation_pending',true,
      'single_flight_deferred',true,
      'reason','FINALISATION_CAPACITY_BUSY'
    );
  end if;

  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      'CANDIDATE_FINALISATION|' || p_workflow_id::text || '|' || p_expected_generation::text,
      0
    )
  ) then
    return jsonb_build_object(
      'ok',true,
      'workflow_id',p_workflow_id,
      'generation',p_expected_generation,
      'state','FINALISATION_PENDING',
      'finalisation_pending',true,
      'single_flight_deferred',true,
      'reason','ALREADY_BEING_PROCESSED'
    );
  end if;

  return public.candidate_submission_finalize_atomic_v1(
    p_session_id,
    p_environment,
    p_workflow_id,
    p_expected_generation,
    p_expected_row_signature,
    p_idempotency_key,
    p_now_utc,
    p_daily_materialisation_json
  );
end;
$function$;

alter function public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb) owner to postgres;

-- Current predecessor: supabase/repeatable/30082026_0714_candidate_weekly_paper_target_ready_state_v1.sql
create or replace function public.candidate_weekly_paper_target_prepare_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
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
  v_office_context jsonb;
  v_office_actor_user_id uuid;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_input jsonb;
  v_hours_input jsonb;
  v_create jsonb;
  v_patch jsonb;
  v_week_patch jsonb;
  v_snapshot jsonb;
  v_route_authority jsonb;
  v_result jsonb;
  v_timesheet_id uuid;
  v_system_actor uuid;
  v_client_id uuid;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1 then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;

  -- SP-12: retain original locking auth below; duplicate only plain auth here.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    if p_session_id is null then
      begin
        v_office_context:=nullif(current_setting(
          'cloudtms.office_candidate_context',true
        ),'')::jsonb;
      exception when others then
        v_office_context:=null;
      end;
      v_office_actor_user_id:=nullif(v_office_context->>'actor_user_id','')::uuid;
      if not private._candidate_office_service_context_valid_v1(
        v_environment,v_office_actor_user_id,'PAPER_PREPARE'
      ) then
        raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
      end if;
      select workflow.candidate_id into v_candidate_id
      from public.candidate_submission_workflows workflow
      where workflow.id=p_workflow_id and workflow.environment=v_environment;
    else
      v_context:=private._candidate_session_context_v1(
        p_session_id,v_environment,null,p_now_utc,false
      );
      v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    end if;
    if v_candidate_id is null then
      raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
    end if;

    perform private.weekly_source_pay_query_admit_v2();
  end if;

  if p_session_id is null then
    begin
      v_office_context:=nullif(current_setting(
        'cloudtms.office_candidate_context',true
      ),'')::jsonb;
    exception when others then
      v_office_context:=null;
    end;
    v_office_actor_user_id:=nullif(v_office_context->>'actor_user_id','')::uuid;
    if not private._candidate_office_service_context_valid_v1(
      v_environment,v_office_actor_user_id,'PAPER_PREPARE'
    ) then
      raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
    end if;
    select workflow.candidate_id into v_candidate_id
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment;
  else
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  end if;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-weekly-paper-target|'||v_environment||'|'||p_workflow_id::text,
    0
  ));
  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=v_environment
    and workflow_row.candidate_id=v_candidate_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation is distinct from p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.scope<>'WEEKLY'
     or v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_COMBINED')
     or v_workflow.contract_week_id is null
     or v_workflow.contract_id is null
     or v_workflow.week_ending_date is null then
    raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
  end if;
  if v_workflow.state not in ('READY_FOR_MANAGER_APPROVAL','WORKER_SUBMITTED','AWAITING_PAPER_RETURN') then
    raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=v_workflow.contract_week_id
    and week_row.contract_id=v_workflow.contract_id
    and week_row.week_ending_date=v_workflow.week_ending_date
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select contract_row.client_id into v_client_id
  from public.contracts contract_row
  where contract_row.id=v_workflow.contract_id
    and contract_row.candidate_id=v_candidate_id;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;

  v_route_authority:=private._candidate_route_family_v1(
    coalesce(v_workflow.target_timesheet_id,v_week.timesheet_id),v_week.id
  );
  if not coalesce(
    (v_route_authority->>'candidate_paper_submission_allowed')::boolean,false
  ) then
    raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED'
      using errcode='55000',detail=v_route_authority::text;
  end if;

  if v_workflow.target_timesheet_id is not null then
    if v_week.timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_workflow.target_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
    return jsonb_build_object(
      'ok',true,'created',false,'idempotent_replay',true,
      'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'timesheet_id',v_timesheet.timesheet_id
    );
  end if;

  v_input:=v_workflow.immutable_submission_json;
  if jsonb_typeof(v_input) is distinct from 'object'
     or private._candidate_sha256_jsonb_v1(v_input)
          is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_hours_input:=case
    when v_workflow.workflow_kind='CONTRACT_COMBINED'
      then v_input->'hours_submission'
    else v_input
  end;
  if jsonb_typeof(v_hours_input) is distinct from 'object'
     or v_hours_input->>'contract_version'
          is distinct from 'CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1' then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;

  v_create:=v_hours_input->'timesheet_create_json';
  v_patch:=coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb);
  v_week_patch:=coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb);
  v_snapshot:=v_hours_input->'canonical_tsfin_snapshot';
  if jsonb_typeof(v_create) is distinct from 'object'
     or jsonb_typeof(v_patch) is distinct from 'object'
     or jsonb_typeof(v_week_patch) is distinct from 'object'
     or jsonb_typeof(v_snapshot) is distinct from 'object'
     or coalesce(v_create->>'timesheet_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;
  v_timesheet_id:=(v_create->>'timesheet_id')::uuid;
  if v_create ?| array[
       'authorised_at_server','auth_name','auth_job_title','r2_auth_key',
       'img_sha256_auth','candidate_manager_approved_at_utc'
     ] then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='22023';
  end if;
  if nullif(v_create->>'contract_id','')::uuid is distinct from v_workflow.contract_id
     or nullif(v_create->>'week_ending_date','')::date is distinct from v_workflow.week_ending_date
     or upper(coalesce(v_create->>'sheet_scope',''))<>'WEEKLY'
     or upper(coalesce(v_create->>'submission_mode',''))<>'MANUAL'
     or upper(coalesce(v_create->>'line_type','')) not in ('HOURS','EXPENSES')
     or nullif(v_snapshot->>'timesheet_id','')::uuid is distinct from v_timesheet_id
     or nullif(v_snapshot->>'candidate_id','')::uuid is distinct from v_candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_client_id then
    raise exception 'CANDIDATE_CANONICAL_SUBMISSION_REQUIRED' using errcode='22023';
  end if;

  if v_week.timesheet_id is not null then
    if v_week.timesheet_id is distinct from v_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
  else
    select settings_row.candidate_app_system_actor_user_id
    into v_system_actor
    from public.settings_defaults settings_row
    where settings_row.id=1;
    if v_system_actor is null
       or not exists(select 1 from public.tms_users actor where actor.id=v_system_actor) then
      raise exception 'CANDIDATE_SYSTEM_ACTOR_REQUIRED' using errcode='55000';
    end if;

    v_result:=public.contract_week_manual_upsert_atomic(
      p_week_id=>v_week.id,
      p_expected_timesheet_id=>null,
      p_timesheet_create_json=>v_create,
      p_timesheet_patch_json=>v_patch,
      p_contract_week_patch_json=>v_week_patch,
      p_tsfin_snapshot_json=>v_snapshot,
      p_rotation_json=>null,
      p_actor_user_id=>v_system_actor,
      p_materialise_staged_evidence=>false,
      p_now_utc=>p_now_utc,
      p_expected_row_signature=>null,
      p_queue_timesheet_materialisation_json=>jsonb_build_object(
        'suppress_timesheet_evidence_materialisation',true
      )
    );
    if not coalesce((v_result->>'ok')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY'
        using errcode='55000',detail=v_result::text;
    end if;
    if coalesce(
         nullif(v_result->>'timesheet_id','')::uuid,
         nullif(v_result#>>'{timesheet,timesheet_id}','')::uuid
       ) is distinct from v_timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
    end if;
    select timesheet_row.* into v_timesheet
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=v_timesheet_id
      and timesheet_row.is_current=true
      and timesheet_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
  end if;

  if v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='55000';
  end if;
  if v_timesheet.contract_id is distinct from v_workflow.contract_id
     or v_timesheet.week_ending_date is distinct from v_workflow.week_ending_date
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;
  select financial_row.* into v_fin
  from public.timesheets_financials financial_row
  where financial_row.timesheet_id=v_timesheet.timesheet_id
    and financial_row.is_current=true
  order by financial_row.updated_at desc,financial_row.id desc
  limit 1
  for update;
  if not found
     or v_fin.authorised_at_utc is not null
     or v_fin.paid_at_utc is not null
     or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_PAPER_TARGET_AUTHORITY_FORBIDDEN' using errcode='55000';
  end if;

  update public.candidate_submission_workflows set
    target_timesheet_id=v_timesheet.timesheet_id,
    updated_at_utc=p_now_utc
  where id=v_workflow.id
    and generation=v_workflow.generation
    and target_timesheet_id is null;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
  end if;
  update public.candidate_submission_components set
    timesheet_id=v_timesheet.timesheet_id
  where workflow_id=v_workflow.id
    and workflow_generation=v_workflow.generation
    and timesheet_id is null
    and state<>'SUPERSEDED';

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_WEEKLY_PAPER_TARGET_PREPARED',
    jsonb_build_object('target_timesheet_id',null,'contract_week_id',v_week.id),
    jsonb_build_object('target_timesheet_id',v_timesheet.timesheet_id,'contract_week_id',v_week.id),
    null,v_system_actor,null,p_now_utc
  );
  return jsonb_build_object(
    'ok',true,'created',v_result is not null,'idempotent_replay',false,
    'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id
  );
end;
$function$;

alter function public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone) owner to postgres;

-- Current predecessor: supabase/repeatable/06092026_2355_candidate_advanced_expense_completion_v1.sql
create or replace function public.candidate_office_expense_paper_prepare_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_operation_id uuid,
  p_update_id uuid,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_operation public.candidate_expense_operations%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_target jsonb;
  v_result jsonb;
begin
  if p_actor_user_id is null or p_operation_id is null or p_update_id is null
     or p_workflow_id is null or coalesce(p_expected_generation,0)<1
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY' using errcode='22023';
  end if;
  -- SP-12: before operation/update/workflow business locks.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.operation_id=p_operation_id
    and operation.environment=v_environment
    and operation.actor_kind='OFFICE'
    and operation.actor_id=p_actor_user_id
    and operation.action_code='REJECT_EXPENSE_CATEGORY'
    and operation.workflow_id=p_workflow_id
    and operation.state='RENDERING'
  for update;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id
    and update_row.operation_id=p_operation_id
    and update_row.workflow_id=p_workflow_id
    and update_row.update_mode='PAPER_REPLACEMENT'
    and update_row.state='RENDERING'
  for update;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment
    and workflow.generation=p_expected_generation
  for update;
  if v_operation.operation_id is null or v_update.update_id is null
     or v_workflow.id is null
     or v_update.current_workflow_generation<>p_expected_generation then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY' using errcode='40001';
  end if;
  perform private._candidate_office_service_context_open_v1(
    v_environment,p_actor_user_id,'reject_submission','REJECT_EXPENSE_CATEGORY',p_now_utc
  );
  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    v_target:=public.candidate_weekly_paper_target_prepare_v1(
      null,v_environment,v_workflow.id,v_workflow.generation,p_now_utc
    );
    if coalesce((v_target->>'ok')::boolean,false) is not true then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='40001';
    end if;
  end if;
  v_result:=public.candidate_weekly_paper_prepare_atomic_v1(
    null,v_environment,v_workflow.id,'PAPER_PREPARE',v_workflow.generation,
    jsonb_build_object(
      'service_office_action',true,'actor_user_id',p_actor_user_id,
      'expense_update_id',v_update.update_id,
      'expense_operation_id',v_operation.operation_id
    ),btrim(p_idempotency_key),p_now_utc
  );
  perform private._candidate_office_service_context_close_v1();
  return v_result;
exception when others then
  perform private._candidate_office_service_context_close_v1();
  raise;
end;
$function$;

alter function public.candidate_office_expense_paper_prepare_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamp with time zone) owner to postgres;

-- Current predecessor: supabase/repeatable/21072026_1235_34_contract_week_manual_upsert_bulk_process_atomic.sql
CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_next_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_response_context text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_operation text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise' ELSE 'contract_week_manual_upsert_bulk_process' END;
  v_response_context text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise' ELSE 'bulk_process' END;
  v_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_detail text := NULL;
  v_detail_json jsonb := NULL;
  v_result_error_code text := NULL;
  v_result_message text := NULL;
  v_attempt integer := 0;
  v_max_attempts constant integer := 6;
  v_retry_wait_ms integer := 0;
  v_total_retry_wait_ms integer := 0;
  v_transient_contention boolean := FALSE;
BEGIN

  -- SP-12: outside legacy retries and before a continuing correction prebranch.
  IF p_week_id IS NOT NULL AND (SELECT active_owner FROM private.bpay_next_module_control WHERE id=1)='NEXT' THEN
    PERFORM private.weekly_source_pay_query_admit_v2();
  END IF;

  if p_expected_timesheet_id is not null
     and coalesce((public._ctms_import_correction_classify_v1(p_expected_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    declare v_transition jsonb;
    begin
      v_transition := public.timesheet_correction_pair_transition_v1(
        p_expected_timesheet_id, 'PROCESS', p_actor_user_id,
        null::uuid, null::text, true, 100
      );
      if coalesce((v_transition ->> 'action_ready')::boolean, false) is not true
         or coalesce((v_transition ->> 'expected_member_count')::integer, 0) > 1 then
        return jsonb_build_object(
          'ok', false,
          'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_ATOMIC_PROCESS_ORCHESTRATION',
          'transition', v_transition
        );
      end if;
    end;
  end if;
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', v_operation,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  LOOP
    v_attempt := v_attempt + 1;
    v_result := '{}'::jsonb;
    v_error_state := NULL;
    v_error_message := NULL;
    v_error_detail := NULL;
    v_detail_json := NULL;
    v_result_error_code := NULL;
    v_result_message := NULL;
    v_transient_contention := FALSE;

    BEGIN
      v_result := public.contract_week_manual_upsert_atomic(
        p_week_id => p_week_id,
        p_expected_timesheet_id => p_expected_timesheet_id,
        p_timesheet_create_json => p_timesheet_create_json,
        p_timesheet_patch_json => p_timesheet_patch_json,
        p_contract_week_patch_json => p_contract_week_patch_json,
        p_tsfin_snapshot_json => COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json),
        p_rotation_json => p_rotation_json,
        p_actor_user_id => p_actor_user_id,
        p_materialise_staged_evidence => p_materialise_staged_evidence,
        p_now_utc => v_now,
        p_expected_row_signature => p_expected_row_signature,
        p_queue_timesheet_materialisation_json => p_queue_timesheet_materialisation_json
      );

      v_result_error_code := UPPER(BTRIM(COALESCE(v_result ->> 'error_code', v_result ->> 'error', '')));
      v_result_message := LOWER(BTRIM(COALESCE(v_result ->> 'message', '')));
      v_transient_contention := COALESCE((v_result ->> 'ok')::boolean, true) IS DISTINCT FROM true
        AND (
          v_result_error_code IN ('LOCK_TIMEOUT', '55P03', '40P01', '40001', '57014', 'TRANSIENT_PROCESSING_CONTENTION')
          OR LOWER(v_result_error_code) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(BTRIM(COALESCE(v_result ->> 'sqlstate', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR LOWER(BTRIM(COALESCE(v_result -> 'detail_json' ->> 'error_state', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR v_result_message ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(COALESCE(v_result ->> 'detail', '')) ~ '(55p03|40p01|40001|57014|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
        );

      IF v_transient_contention IS DISTINCT FROM true THEN
        EXIT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_error_state = RETURNED_SQLSTATE,
        v_error_message = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

      BEGIN
        v_detail_json := v_error_detail::jsonb;
      EXCEPTION WHEN OTHERS THEN
        v_detail_json := NULL;
      END;

      v_transient_contention := (
        v_error_state IN ('55P03', '40P01', '40001', '57014')
        OR (
          v_error_state = 'P0001'
          AND UPPER(BTRIM(COALESCE(v_error_message, ''))) IN ('LOCK_TIMEOUT', 'TRANSIENT_PROCESSING_CONTENTION')
        )
        OR LOWER(COALESCE(v_error_message, '')) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
        OR LOWER(COALESCE(v_error_detail, '')) ~ '(55p03|40p01|40001|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
      );

      IF v_transient_contention IS DISTINCT FROM true THEN
        RETURN jsonb_build_object(
          'ok', false,
          'success', false,
          'operation', v_operation,
          'response_context', v_response_context,
          'bulk_process', v_response_context = 'bulk_process',
          'bulk_authorise', v_response_context = 'bulk_authorise',
          'error_code', CASE WHEN v_error_state = 'P0001' AND NULLIF(BTRIM(v_error_message), '') IS NOT NULL THEN v_error_message ELSE v_error_state END,
          'sqlstate', v_error_state,
          'message', v_error_message,
          'detail', v_error_detail,
          'detail_json', v_detail_json,
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'process_retry_count', GREATEST(v_attempt - 1, 0),
          'process_retry_wait_ms', v_total_retry_wait_ms,
          'refresh_required', true,
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'contract_week_id', p_week_id,
            'timesheet_id', p_expected_timesheet_id
          )
        );
      END IF;
    END;

    IF v_attempt >= v_max_attempts THEN
      RETURN jsonb_build_object(
        'ok', false,
        'success', false,
        'operation', v_operation,
        'response_context', v_response_context,
        'bulk_process', v_response_context = 'bulk_process',
        'bulk_authorise', v_response_context = 'bulk_authorise',
        'error_code', 'TRANSIENT_PROCESSING_CONTENTION',
        'message', 'The timesheet could not be completed immediately. The row has been safely refreshed.',
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'process_retry_count', GREATEST(v_attempt - 1, 0),
        'process_retry_wait_ms', v_total_retry_wait_ms,
        'refresh_required', true,
        'cache_invalidation_hints', jsonb_build_object(
          'changed_domains', jsonb_build_array('timesheet_lifecycle'),
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )
      );
    END IF;

    v_retry_wait_ms := LEAST(2000, (150::numeric * POWER(2::numeric, GREATEST(v_attempt - 1, 0)::numeric))::integer) + FLOOR(RANDOM() * 125)::integer;
    v_total_retry_wait_ms := v_total_retry_wait_ms + v_retry_wait_ms;
    PERFORM pg_sleep(v_retry_wait_ms::numeric / 1000::numeric);
  END LOOP;

  RETURN COALESCE(v_result, '{}'::jsonb)
    || jsonb_build_object(
      'ok', COALESCE((v_result ->> 'ok')::boolean, true),
      'success', COALESCE((v_result ->> 'success')::boolean, true),
      'operation', v_operation,
      'response_context', v_response_context,
      'bulk_process', v_response_context = 'bulk_process',
      'bulk_authorise', v_response_context = 'bulk_authorise',
      'process_retry_count', GREATEST(v_attempt - 1, 0),
      'process_retry_wait_ms', v_total_retry_wait_ms,
      'transient_contention_recovered', v_attempt > 1,
      'requires_affected_row_refresh', true,
      'refresh_required', true
    );
END;
$function$;

alter function public.contract_week_manual_upsert_bulk_process_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb,jsonb,text,jsonb) owner to postgres;

-- Current predecessor: supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql
CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
  v_duplicate_expense_review jsonb := '{}'::jsonb;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'AUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  -- SP-12: revoked_at restoration changes the qualified root scope.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
    END IF;

    perform private.weekly_source_pay_query_admit_v2();
  end if;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_duplicate_expense_review:=private._timesheet_duplicate_expense_review_v1(
    v_current_ts.timesheet_id
  );
  IF COALESCE((v_duplicate_expense_review->>'required')::boolean,false)
     AND COALESCE(current_setting('cloudtms.duplicate_expense_reviewed',true),'')<>'true' THEN
    RAISE EXCEPTION 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
      USING ERRCODE='PT409',DETAIL=jsonb_build_object(
        'code','DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        'timesheet_id',v_current_ts.timesheet_id,
        'categories',COALESCE(v_duplicate_expense_review->'categories','[]'::jsonb)
      )::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
SELECT
  COALESCE((authority.settings_json#>>'{values,requires_hr}')::boolean,false) AS client_requires_hr,
  (ts.timesheet_id IS NOT NULL
    AND COALESCE((authority.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
    AND NOT COALESCE((authority.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
    AND COALESCE(tf.total_hours,0)>0) AS hr_validation_required_for_invoice,
  UPPER(vl.status::text) AS validation_status
INTO v_client_requires_hr, v_hr_validation_required_for_invoice, v_validation_status
FROM public.timesheets ts
LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
LEFT JOIN LATERAL (
  SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
  WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
  ORDER BY f.created_at DESC LIMIT 1
) tf ON true
CROSS JOIN LATERAL (
  SELECT private._contract_settings_effective_core_v1(
    COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
    COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
      (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
    CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
    ts.timesheet_id) AS settings_json
) authority
LEFT JOIN LATERAL (
  SELECT tv.status FROM public.timesheet_validations tv
  WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
) vl ON true
WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
LIMIT 1
;
  ELSE
  -- Exact current-root flags only. Do not rebuild the summary of every
  -- Timesheet to authorise one root; retain the same dated/frozen settings,
  -- current-financial selection and latest-validation semantics.
  WITH bpay_authorisation_point_flags AS MATERIALIZED (
    SELECT ts.timesheet_id, COALESCE(tf.total_hours,0) AS total_hours,
      public.contract_settings_effective_get_v1(
        COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
        COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
          (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
        CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
        ts.timesheet_id) AS settings_json,
      UPPER(vl.status::text) AS validation_status
    FROM public.timesheets ts
    LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
    LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
    LEFT JOIN LATERAL (
      SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
      WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
      ORDER BY f.created_at DESC LIMIT 1
    ) tf ON true
    LEFT JOIN LATERAL (
      SELECT tv.status FROM public.timesheet_validations tv
      WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
    ) vl ON true
    WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
    LIMIT 1
  )
  SELECT COALESCE((point.settings_json#>>'{values,requires_hr}')::boolean,false),
    (point.timesheet_id IS NOT NULL
      AND COALESCE((point.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
      AND NOT COALESCE((point.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
      AND point.total_hours>0),
    point.validation_status
  INTO v_client_requires_hr,v_hr_validation_required_for_invoice,v_validation_status
  FROM bpay_authorisation_point_flags point;
  END IF;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
    SELECT cw.* INTO v_contract_week FROM public.contract_weeks cw
    WHERE cw.timesheet_id=v_current_ts.timesheet_id FOR UPDATE;
  ELSE
  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;
  END IF;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  v_has_uncleared_advance_override := private.bpay_has_uncleared_advance_v1(v_current_ts.timesheet_id);

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  -- Exact busy must escape before diagnostic writes or legacy lock translation.
  IF v_error_state = '55P03' AND v_error_message = 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' THEN
    RAISE;
  END IF;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;

alter function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) owner to postgres;

-- Current predecessor: supabase/repeatable/17092026_0900_weekly_source_pinned_owner_rotation_guard_v1.sql
CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_nhsp_shift_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_result jsonb;
  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23).
  v_weekly_source_guard jsonb;
  v_weekly_source_root uuid;
  v_contract_id uuid;
  v_contract_start date;
  v_contract_end date;
  v_week_ending_weekday integer;
  v_first_remaining_week date;
  v_last_remaining_week date;
  v_start_week date;
  v_end_week date;
  v_first_remaining_planned_date date;
  v_last_remaining_planned_date date;
  v_new_start date;
  v_new_end date;
BEGIN
  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  v_contract_id := NULLIF(v_preview ->> 'contract_id', '')::uuid;

  -- SP-12: after existing plain preview/auth, before the original locking owner.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  -- Plan 6.2 G6-11 (proof/34 section 3, entry point E23).  The delete base
  -- owner carries the same refusal under its own locks; this call covers the
  -- upgrade path, where the base body is not recreated because the rename block
  -- above is already satisfied.  Checked before any write of this chain.  An
  -- unmanaged family is unaffected.
  FOR v_weekly_source_root IN
    SELECT DISTINCT target.timesheet_id
    FROM (
      SELECT p_timesheet_id AS timesheet_id
      UNION
      SELECT unnest(COALESCE(p_expected_timesheet_ids, ARRAY[]::uuid[]))
      UNION
      SELECT NULLIF(element.value, '')::uuid
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(v_preview -> 'timesheet_ids') = 'array'
          THEN v_preview -> 'timesheet_ids' ELSE '[]'::jsonb END
      ) AS element(value)
    ) AS target(timesheet_id)
    WHERE target.timesheet_id IS NOT NULL
    ORDER BY 1
  LOOP
    v_weekly_source_guard := private.weekly_source_managed_root_guard_v1(v_weekly_source_root);
    -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
    -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
    -- protected family whose identity cannot be resolved, to a family carrying
    -- protected pay evidence but no authorisation row
    -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
    -- unauthorised-Timesheet contradiction, which must never be allowed to
    -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
    -- family -- including a malformed one -- keeps exactly the behaviour it had
    -- before this feature was installed.  Absent, null and non-boolean take the
    -- unsafe value at every read (Part 1 addendum rule 4).
    IF (COALESCE((v_weekly_source_guard->>'managed')::boolean, true)
         and (COALESCE((v_weekly_source_guard->>'ok')::boolean, true)
              or COALESCE((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
       or COALESCE((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
       or (v_weekly_source_guard->>'protected_target_ownership_state') is not null THEN
      RAISE EXCEPTION 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
                'entry_point','E23:public.timesheet_weekly_chain_delete_apply',
                'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
                'refusal_basis', case
                  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
                  -- canonical booking-reference collision and must be named as one.
                  -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
                  -- release, so the order of this edit and WP-03's rename cannot open a gap in
                  -- which Office stops seeing the ruled name.
                  when v_weekly_source_guard->>'reason' in (
                         'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
                    then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
                    then 'WEEKLY_SOURCE_MANAGED_ROOT'
                  when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
                    then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
                  when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
                    then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
                  else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
                'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
                'timesheet_id', v_weekly_source_root,
                'reason', v_weekly_source_guard->>'reason'
              )::text;
    END IF;
  END LOOP;

  v_result := private.timesheet_weekly_chain_delete_apply_base_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_ids,
    p_expected_contract_week_ids,
    p_expected_nhsp_shift_ids,
    p_expected_row_signature
  );

  IF COALESCE((v_result ->> 'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_result ->> 'apply_performed')::boolean, false) IS NOT TRUE THEN
    RETURN v_result;
  END IF;

  IF v_contract_id IS NULL THEN
    RETURN v_result;
  END IF;

  SELECT c.start_date, c.end_date, COALESCE(c.week_ending_weekday_snapshot, 0)
  INTO v_contract_start, v_contract_end, v_week_ending_weekday
  FROM public.contracts c
  WHERE c.id = v_contract_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN v_result;
  END IF;

  SELECT min(cw.week_ending_date), max(cw.week_ending_date)
  INTO v_first_remaining_week, v_last_remaining_week
  FROM public.contract_weeks cw
  WHERE cw.contract_id = v_contract_id;

  v_new_start := v_contract_start;
  v_new_end := v_contract_end;

  IF v_first_remaining_week IS NOT NULL THEN
    v_start_week := v_contract_start
      + mod(v_week_ending_weekday - extract(dow from v_contract_start)::integer + 7, 7);
    v_end_week := v_contract_end
      + mod(v_week_ending_weekday - extract(dow from v_contract_end)::integer + 7, 7);

    IF v_first_remaining_week > v_start_week THEN
      SELECT min((entry.item ->> 'date')::date)
      INTO v_first_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_first_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_start := COALESCE(v_first_remaining_planned_date, v_first_remaining_week - 6);
    END IF;

    IF v_last_remaining_week < v_end_week THEN
      SELECT max((entry.item ->> 'date')::date)
      INTO v_last_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_last_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_end := COALESCE(v_last_remaining_planned_date, v_last_remaining_week);
    END IF;

    IF v_new_start IS DISTINCT FROM v_contract_start
       OR v_new_end IS DISTINCT FROM v_contract_end THEN
      UPDATE public.contracts
      SET start_date = v_new_start,
          end_date = v_new_end
      WHERE id = v_contract_id;

      INSERT INTO public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      VALUES (
        p_actor_user_id,
        'contract',
        v_contract_id::text,
        'CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE',
        jsonb_build_object('start_date', v_contract_start, 'end_date', v_contract_end),
        jsonb_build_object('start_date', v_new_start, 'end_date', v_new_end),
        'WEEKLY_TIMESHEET_CHAIN_DELETED'
      );
    END IF;
  END IF;

  RETURN v_result || jsonb_build_object(
    'contract_dates', jsonb_build_object(
      'start_date', v_new_start,
      'end_date', v_new_end,
      'changed', v_new_start IS DISTINCT FROM v_contract_start
        OR v_new_end IS DISTINCT FROM v_contract_end
    )
  );
END;
$function$;

alter function public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text) owner to postgres;

notify pgrst, 'reload schema';
commit;
