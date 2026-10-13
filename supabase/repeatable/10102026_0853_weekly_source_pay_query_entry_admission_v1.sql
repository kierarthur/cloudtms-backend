-- SP-12: five named existing Source admission entry seams.
-- Original signatures, bodies, ownership, security and service-only ACLs retained.
-- Rule-B cut and parked F-25 producer protocol remain untouched.
\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_candidate_app_submit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_request_id uuid,
  p_body jsonb,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_kind text;
  v_key uuid;
  v_hash bytea;
  v_replay jsonb;
  v_projection jsonb;
  v_scope jsonb;
  v_internal jsonb;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_workflow_result jsonb;
  v_response_result jsonb;
  v_result jsonb;
  v_next_generation integer;
begin
  perform private.weekly_source_query_require_service_v1();
  -- Plain auth/input; original locking context revalidates below.
  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  if p_body is null or pg_catalog.jsonb_typeof(p_body)<>'object'
     or pg_catalog.jsonb_typeof(p_body->'responses')<>'array'
     or coalesce(p_body->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(p_body->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_body->>'submission_kind','')));
  if v_kind='RESPONSES_ONLY' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','responses','idempotency_key'
              )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  elsif v_kind='WHOLE_WEEK_REVISION' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','workflow_id','generation',
                'candidate_signature_component_id','candidate_signed_at_utc',
                'immutable_submission','responses','idempotency_key'
              )) or pg_catalog.jsonb_typeof(p_body->'immutable_submission')<>'object' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  begin
    v_key:=(p_body->>'idempotency_key')::uuid;
    perform (p_body->>'request_version')::integer;
    perform (p_body->>'scope_id')::uuid;
    perform (p_body->>'scope_version')::integer;
    if v_kind='WHOLE_WEEK_REVISION' then
      perform (p_body->>'workflow_id')::uuid;
      perform (p_body->>'generation')::integer;
      perform (p_body->>'candidate_signature_component_id')::uuid;
      perform (p_body->>'candidate_signed_at_utc')::timestamptz;
    end if;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end;

  -- SP-12: existing admission only; no producer/version evidence.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  if p_body is null or pg_catalog.jsonb_typeof(p_body)<>'object'
     or pg_catalog.jsonb_typeof(p_body->'responses')<>'array'
     or coalesce(p_body->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(p_body->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_body->>'submission_kind','')));
  if v_kind='RESPONSES_ONLY' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','responses','idempotency_key'
              )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  elsif v_kind='WHOLE_WEEK_REVISION' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','workflow_id','generation',
                'candidate_signature_component_id','candidate_signed_at_utc',
                'immutable_submission','responses','idempotency_key'
              )) or pg_catalog.jsonb_typeof(p_body->'immutable_submission')<>'object' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  begin
    v_key:=(p_body->>'idempotency_key')::uuid;
    perform (p_body->>'request_version')::integer;
    perform (p_body->>'scope_id')::uuid;
    perform (p_body->>'scope_version')::integer;
    if v_kind='WHOLE_WEEK_REVISION' then
      perform (p_body->>'workflow_id')::uuid;
      perform (p_body->>'generation')::integer;
      perform (p_body->>'candidate_signature_component_id')::uuid;
      perform (p_body->>'candidate_signed_at_utc')::timestamptz;
    end if;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end;
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CANDIDATE_APP_SUBMIT_V1',pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'request_id',p_request_id,
      'body',p_body-'idempotency_key'
    )
  );
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_CANDIDATE_APP:'||v_key::text,0
  ));
  v_replay:=private.weekly_source_candidate_app_receipt_v1(
    v_candidate_id,p_request_id,'FINAL_SUBMIT',v_key,v_hash
  );
  if v_replay is not null then
    return pg_catalog.jsonb_set(v_replay,'{idempotent_replay}','true'::jsonb,false);
  end if;

  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=p_request_id and candidate_id=v_candidate_id for update;
  if v_generation.state<>'ACTIVE' or p_now_utc>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_generation.source_cycle_id;
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
  if (p_body->>'request_version')::integer<>(v_projection->>'request_version')::integer
     or p_body->>'request_fingerprint'<>v_projection->>'request_fingerprint' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  select value into v_scope from pg_catalog.jsonb_array_elements(v_projection->'scopes')
  where value->>'scope_id'=p_body->>'scope_id';
  if v_scope is null
     or (p_body->>'scope_version')::integer<>(v_scope->>'scope_version')::integer
     or p_body->>'scope_fingerprint'<>v_scope->>'scope_fingerprint'
     or v_scope->>'completion_state'='COMPLETE' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE' using errcode='40001';
  end if;
  v_internal:=private.weekly_source_candidate_app_responses_v1(
    v_scope,p_body->'responses',true
  );

  if v_kind='RESPONSES_ONLY' then
    if v_scope->>'request_kind'<>'CHECK_HOURS'
       or pg_catalog.jsonb_array_length(v_internal)=0
       or exists(
         select 1 from pg_catalog.jsonb_array_elements(v_internal) item
         where item->>'choice'<>'CANDIDATE_CORRECT'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSES_ONLY_INVALID' using errcode='22023';
    end if;
    v_response_result:=public.weekly_source_candidate_response_submit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate_id,'candidate_generation_id',p_request_id,
        'projection_publication_id',v_cycle.current_projection_publication_id,
        'request_idempotency_key',v_key,'responses',v_internal
      )
    );
    v_next_generation:=null;
  else
    if v_scope->>'request_kind'='CHECK_HOURS' and (
      pg_catalog.jsonb_array_length(v_internal)=0
      or not exists(
        select 1 from pg_catalog.jsonb_array_elements(v_internal) item
        where item->>'choice'<>'CANDIDATE_CORRECT'
      )
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_NOT_REQUIRED' using errcode='22023';
    end if;
    if v_scope->>'request_kind'='SUBMIT_TIMESHEET'
       and pg_catalog.jsonb_array_length(p_body->'responses')<>0 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_TIMESHEET_RESPONSES_INVALID' using errcode='22023';
    end if;
    if v_scope->>'request_kind'='CHECK_HOURS' then
      perform private.weekly_source_candidate_app_assert_week_revision_v1(
        v_scope,p_body->'responses',v_internal,p_body->'immutable_submission'
      );
    else
      perform private.weekly_source_candidate_app_assert_new_week_submission_v1(
        v_scope,p_body->'immutable_submission'
      );
    end if;
    select * into strict v_workflow from public.candidate_submission_workflows
    where id=(p_body->>'workflow_id')::uuid
      and candidate_id=v_candidate_id
      and environment=pg_catalog.upper(pg_catalog.btrim(p_environment))
      and contract_id=(v_scope->>'contract_id')::uuid
      and week_ending_date=(v_scope->>'week_ending_date')::date;
    if v_workflow.generation<>(p_body->>'generation')::integer
       or v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_workflow_result:=public.weekly_source_candidate_check_materialise_atomic_v1(
      pg_catalog.jsonb_build_object(
        'account_id',(v_context->>'account_id')::uuid,
        'candidate_id',v_candidate_id,
        'candidate_generation_id',p_request_id,
        'projection_publication_id',v_cycle.current_projection_publication_id,
        'request_version',(p_body->>'request_version')::integer,
        'request_fingerprint',p_body->>'request_fingerprint',
        'scope_id',(p_body->>'scope_id')::uuid,
        'scope_version',(p_body->>'scope_version')::integer,
        'scope_fingerprint',p_body->>'scope_fingerprint',
        'request_kind',v_scope->>'request_kind',
        'workflow_id',v_workflow.id,
        'expected_workflow_generation',v_workflow.generation,
        'candidate_signature_component_id',
          (p_body->>'candidate_signature_component_id')::uuid,
        'candidate_signed_at_utc',(p_body->>'candidate_signed_at_utc')::timestamptz,
        'immutable_submission',p_body->'immutable_submission',
        'request_idempotency_key',v_key
      ),p_now_utc
    );
    v_next_generation:=(v_workflow_result->>'generation')::integer;
    if v_scope->>'request_kind'='CHECK_HOURS' then
      select coalesce(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_set(
          value,'{expected_timesheet_hash}',
          pg_catalog.to_jsonb(v_workflow_result->>'timesheet_hash'),false
        ) order by value->>'incident_id'
      ),'[]'::jsonb) into v_internal
      from pg_catalog.jsonb_array_elements(v_internal);
      v_response_result:=public.weekly_source_candidate_response_submit_atomic_v1(
        pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate_id,'candidate_generation_id',p_request_id,
          'projection_publication_id',v_cycle.current_projection_publication_id,
          'request_idempotency_key',v_key,'responses',v_internal
        )
      );
    end if;
  end if;
  update public.weekly_candidate_response_drafts
  set state='SUPERSEDED',updated_at_utc=p_now_utc
  where candidate_generation_id=p_request_id and candidate_id=v_candidate_id
    and state='DRAFT';
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
  perform private.weekly_source_candidate_app_assert_request_shape_v1(v_projection);
  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'idempotent_replay',false,
    'processed_scope_id',(p_body->>'scope_id')::uuid,
    'next_outstanding_scope_id',v_projection->'earliest_outstanding_scope_id',
    'workflow_id',case when v_kind='WHOLE_WEEK_REVISION'
      then (p_body->>'workflow_id')::uuid else null end,
    'generation',v_next_generation,'request',v_projection
  );
  if exists(select 1 from pg_catalog.jsonb_object_keys(v_result) key
            where key not in (
              'ok','idempotent_replay','processed_scope_id','next_outstanding_scope_id',
              'workflow_id','generation','request'
            )) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
  end if;
  insert into public.weekly_candidate_app_mutation_receipts(
    candidate_generation_id,candidate_id,mutation_kind,idempotency_key,
    request_hash,response_json,created_at_utc
  ) values (
    p_request_id,v_candidate_id,'FINAL_SUBMIT',v_key,v_hash,v_result,p_now_utc
  );
  return v_result;
end;
$function$;

alter function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone) owner to postgres;

create or replace function public.weekly_source_candidate_self_submit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_body jsonb,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_result jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  -- Plain auth/input; original locking context revalidates below.
  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  if p_body is null or pg_catalog.jsonb_typeof(p_body)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
       where key not in (
         'workflow_id','expected_workflow_generation','candidate_signature_component_id',
         'candidate_signed_at_utc','immutable_submission','idempotency_key'
       )) or pg_catalog.jsonb_typeof(p_body->'immutable_submission')<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SELF_SUBMIT_INVALID' using errcode='22023';
  end if;
  begin
    perform (p_body->>'workflow_id')::uuid;
    perform (p_body->>'expected_workflow_generation')::integer;
    perform (p_body->>'candidate_signature_component_id')::uuid;
    perform (p_body->>'candidate_signed_at_utc')::timestamptz;
    perform (p_body->>'idempotency_key')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SELF_SUBMIT_INVALID' using errcode='22023';
  end;

  -- SP-12: existing admission only; no producer/version evidence.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  if p_body is null or pg_catalog.jsonb_typeof(p_body)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
       where key not in (
         'workflow_id','expected_workflow_generation','candidate_signature_component_id',
         'candidate_signed_at_utc','immutable_submission','idempotency_key'
       )) or pg_catalog.jsonb_typeof(p_body->'immutable_submission')<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SELF_SUBMIT_INVALID' using errcode='22023';
  end if;
  begin
    select * into strict v_workflow from public.candidate_submission_workflows
    where id=(p_body->>'workflow_id')::uuid
      and candidate_id=v_candidate_id
      and account_id=(v_context->>'account_id')::uuid;
    perform (p_body->>'expected_workflow_generation')::integer;
    perform (p_body->>'candidate_signature_component_id')::uuid;
    perform (p_body->>'candidate_signed_at_utc')::timestamptz;
    perform (p_body->>'idempotency_key')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SELF_SUBMIT_INVALID' using errcode='22023';
  end;
  v_result:=public.weekly_source_candidate_check_materialise_atomic_v1(
    pg_catalog.jsonb_build_object(
      'account_id',(v_context->>'account_id')::uuid,
      'candidate_id',v_candidate_id,
      'request_kind','SELF_SUBMIT',
      'workflow_id',v_workflow.id,
      'expected_workflow_generation',(p_body->>'expected_workflow_generation')::integer,
      'candidate_signature_component_id',(p_body->>'candidate_signature_component_id')::uuid,
      'candidate_signed_at_utc',(p_body->>'candidate_signed_at_utc')::timestamptz,
      'immutable_submission',p_body->'immutable_submission',
      'request_idempotency_key',(p_body->>'idempotency_key')::uuid
    ),p_now_utc
  );
  return v_result;
end;
$function$;

alter function public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone) owner to postgres;

create or replace function public.weekly_source_candidate_check_materialise_atomic_v1(
  p_request jsonb,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_account_id uuid;
  v_candidate_id uuid;
  v_candidate_generation_id uuid;
  v_projection_publication_id uuid;
  v_scope_id uuid;
  v_workflow_id uuid;
  v_signature_id uuid;
  v_key uuid;
  v_expected_generation integer;
  v_request_hash bytea;
  v_projection jsonb;
  v_scope jsonb;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_signature public.candidate_submission_components%rowtype;
  v_signature_copy public.candidate_submission_components%rowtype;
  v_contract public.contracts%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_candidate public.candidates%rowtype;
  v_client public.clients%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_membership public.weekly_timesheet_submission_request_memberships%rowtype;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_schedule_input jsonb;
  v_units_week jsonb;
  v_units_day jsonb;
  v_schedule jsonb;
  v_next_generation integer;
  v_component_no integer;
  v_booking_id text;
  v_previous_context text;
  v_previous_defer_summary_refresh text;
  v_worked_start timestamptz;
  v_worked_end timestamptz;
  v_break_minutes integer;
  v_worked_minutes integer;
  v_timesheet_hash bytea;
  v_compare jsonb;
  v_completion jsonb;
  v_result jsonb;
  v_kind text;
  v_day_off_dates jsonb;
  v_today date;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in (
         'account_id','candidate_id','candidate_generation_id',
         'projection_publication_id','request_version','request_fingerprint',
         'scope_id','scope_version','scope_fingerprint','request_kind',
         'workflow_id','expected_workflow_generation',
         'candidate_signature_component_id','candidate_signed_at_utc',
         'immutable_submission','request_idempotency_key'
       )
     )
     or pg_catalog.jsonb_typeof(p_request->'immutable_submission')<>'object'
     or (coalesce(p_request->>'request_kind','')<>'SELF_SUBMIT'
       and (coalesce(p_request->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
         or coalesce(p_request->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$')) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_account_id:=(p_request->>'account_id')::uuid;
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_candidate_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_projection_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_scope_id:=(p_request->>'scope_id')::uuid;
    v_workflow_id:=(p_request->>'workflow_id')::uuid;
    v_signature_id:=(p_request->>'candidate_signature_component_id')::uuid;
    v_key:=(p_request->>'request_idempotency_key')::uuid;
    v_expected_generation:=(p_request->>'expected_workflow_generation')::integer;
    perform (p_request->>'request_version')::integer;
    perform (p_request->>'scope_version')::integer;
    perform (p_request->>'candidate_signed_at_utc')::timestamptz;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_REQUEST_INVALID' using errcode='22023';
  end;
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'request_kind','')));
  if v_kind not in ('CHECK_HOURS','SUBMIT_TIMESHEET','SELF_SUBMIT') then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_candidate_app_assert_hours_only_v1(
    p_request->'immutable_submission'
  );
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CANDIDATE_CHECK_MATERIALISATION_V1',
    p_request-'request_idempotency_key'
  );
  -- SP-12: existing admission only; no producer/version evidence.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_CANDIDATE_MATERIALISE:'||v_workflow_id::text,0
  ));

  select * into v_workflow from public.candidate_submission_workflows
  where id=v_workflow_id for update;
  if not found or v_workflow.account_id is distinct from v_account_id
     or v_workflow.candidate_id is distinct from v_candidate_id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WORKFLOW_STALE' using errcode='40001';
  end if;
  if v_workflow.last_mutation_idempotency_key=v_key::text then
    if v_workflow.last_mutation_response_json->>'materialisation_request_hash'
         is distinct from pg_catalog.encode(v_request_hash,'hex') then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_REPLAY_CONFLICT'
        using errcode='23505';
    end if;
    return pg_catalog.jsonb_set(
      v_workflow.last_mutation_response_json,'{idempotent_replay}','true'::jsonb,false
    );
  end if;
  if v_workflow.generation<>v_expected_generation
     or v_workflow.state<>'WORKER_DRAFT'
     or v_workflow.workflow_kind<>'CONTRACT_HOURS'
     or v_workflow.scope<>'WEEKLY'
     or v_workflow.route<>'ELECTRONIC' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WORKFLOW_STALE' using errcode='40001';
  end if;
  select * into strict v_account from public.candidate_app_accounts
  where id=v_account_id and environment=v_workflow.environment and status='ACTIVE';
  if v_kind<>'SELF_SUBMIT' then
  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=v_candidate_generation_id and candidate_id=v_candidate_id
    and request_kind=v_kind and state='ACTIVE' for update;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_generation.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id and environment=v_workflow.environment;
  if v_cycle.current_projection_publication_id is distinct from v_projection_publication_id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_query_current_publication_v1(
    v_cycle.id,v_projection_publication_id
  );
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,v_candidate_generation_id,p_now_utc
  );
  if (p_request->>'request_version')::integer
       is distinct from (v_projection->>'request_version')::integer
     or p_request->>'request_fingerprint' is distinct from v_projection->>'request_fingerprint' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  select value into v_scope from pg_catalog.jsonb_array_elements(v_projection->'scopes')
  where value->>'scope_id'=v_scope_id::text;
  if v_scope is null
     or (p_request->>'scope_version')::integer
          is distinct from (v_scope->>'scope_version')::integer
     or p_request->>'scope_fingerprint' is distinct from v_scope->>'scope_fingerprint'
     or v_scope->>'request_kind' is distinct from v_kind
     or v_scope->>'completion_state'='COMPLETE' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE' using errcode='40001';
  end if;
  else
    v_scope:=pg_catalog.jsonb_build_object(
      'contract_id',v_workflow.contract_id,
      'contract_week_id',v_workflow.contract_week_id,
      'week_ending_date',v_workflow.week_ending_date
    );
    v_today:=(p_now_utc at time zone 'Europe/London')::date;
    if v_workflow.week_ending_date-6>v_today then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_NOT_STARTED' using errcode='55000';
    end if;
    if exists(
      select 1 from public.weekly_timesheet_submission_request_memberships membership
      join public.weekly_timesheet_submission_requests submission
        on submission.id=membership.submission_request_id
      where submission.candidate_id=v_candidate_id
        and submission.state in ('ACTIVE','PARTLY_SUBMITTED','OVERDUE')
        and membership.state='WAITING'
        and membership.contract_id=v_workflow.contract_id
        and membership.week_ending=v_workflow.week_ending_date
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_USE_ACTIVE_REQUEST' using errcode='55000';
    end if;
  end if;
  if v_workflow.contract_id is distinct from (v_scope->>'contract_id')::uuid
     or v_workflow.contract_week_id is distinct from (v_scope->>'contract_week_id')::uuid
     or v_workflow.week_ending_date is distinct from (v_scope->>'week_ending_date')::date then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WORKFLOW_STALE' using errcode='40001';
  end if;
  select * into strict v_contract_week from public.contract_weeks
  where id=v_workflow.contract_week_id
    and contract_id=v_workflow.contract_id
    and week_ending_date=v_workflow.week_ending_date
    and additional_seq=0 and not is_adjustment
  for update;
  if v_contract_week.status in (
       'AUTHORISED'::public.contract_week_status_enum,
       'INVOICED'::public.contract_week_status_enum,
       'CANCELLED'::public.contract_week_status_enum
     ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_LOCKED' using errcode='55000';
  end if;
  select * into strict v_contract from public.contracts
  where id=v_workflow.contract_id and candidate_id=v_candidate_id
    and (v_kind='SELF_SUBMIT' or client_id=v_generation.client_id)
    and v_workflow.week_ending_date>=start_date
    and v_workflow.week_ending_date-6<=end_date;
  if (private._weekly_source_effective_policy_v1(
        v_contract.client_id,v_contract.id,v_workflow.week_ending_date
      )->>'authority_mode')<>'SOURCE_AUTHORITY'
     or (private._weekly_source_effective_policy_v1(
        v_contract.client_id,v_contract.id,v_workflow.week_ending_date
      )->>'document_mode')<>'CHECK_ONLY' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_NOT_APPLICABLE' using errcode='55000';
  end if;
  select * into strict v_signature from public.candidate_submission_components
  where id=v_signature_id and workflow_id=v_workflow.id
    and workflow_generation=v_workflow.generation
    and component_kind='CANDIDATE_SIGNATURE'
    and document_role='CANDIDATE_SIGNATURE' and state='IMMUTABLE'
    and immutable_at_utc is not null and storage_key is not null
    and media_type in ('image/png','image/jpeg') and byte_size>0
    and source_content_sha256 is not null
    and pg_catalog.octet_length(source_content_sha256)=32
  for update;
  if (p_request->>'candidate_signed_at_utc')::timestamptz
       >p_now_utc+interval '5 minutes' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SIGNATURE_TIME_INVALID' using errcode='22023';
  end if;

  v_schedule_input:=coalesce(
    p_request->'immutable_submission'->'actual_schedule_json',
    p_request->'immutable_submission'->'schedule_json',
    p_request->'immutable_submission'->'timesheet_patch_json'->'actual_schedule_json',
    p_request->'immutable_submission'->'timesheet_patch_json'->'schedule_json',
    p_request->'immutable_submission'->'hours_submission'->'actual_schedule_json',
    p_request->'immutable_submission'->'hours_submission'->'schedule_json'
  );
  v_units_week:=coalesce(
    p_request->'immutable_submission'->'additional_units_week',
    p_request->'immutable_submission'->'timesheet_patch_json'->'additional_units_week',
    p_request->'immutable_submission'->'hours_submission'->'additional_units_week',
    '{}'::jsonb
  );
  v_units_day:=coalesce(
    p_request->'immutable_submission'->'additional_units_per_day',
    p_request->'immutable_submission'->'timesheet_patch_json'->'additional_units_per_day',
    p_request->'immutable_submission'->'hours_submission'->'additional_units_per_day',
    '{}'::jsonb
  );
  if pg_catalog.jsonb_typeof(v_units_week)<>'object'
     or pg_catalog.jsonb_typeof(v_units_day)<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
  v_schedule:=private.weekly_source_candidate_app_schedule_v1(
    v_schedule_input,v_units_day
  );
  v_today:=(p_now_utc at time zone 'Europe/London')::date;
  if v_kind in ('SELF_SUBMIT','SUBMIT_TIMESHEET') then
    v_day_off_dates:=p_request->'immutable_submission'->'day_off_dates';
    if pg_catalog.jsonb_typeof(v_day_off_dates)<>'array'
       or pg_catalog.jsonb_array_length(v_day_off_dates)>7
       or exists(
         select 1 from pg_catalog.jsonb_array_elements_text(v_day_off_dates) value
         where value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_INCOMPLETE' using errcode='22023';
    end if;
    if exists(
      select 1 from pg_catalog.generate_series(
        v_workflow.week_ending_date-6,v_workflow.week_ending_date,interval '1 day'
      ) day_value
      where (select case when pg_catalog.count(*)>0 then 1 else 0 end
             from pg_catalog.jsonb_array_elements(v_schedule) row_value
             where (row_value->>'date')::date=day_value::date)
          +(select pg_catalog.count(*) from pg_catalog.jsonb_array_elements_text(v_day_off_dates) off_date
            where off_date::date=day_value::date)<>1
    ) or exists(
      select 1 from pg_catalog.jsonb_array_elements_text(v_day_off_dates) off_date
      where off_date::date not between v_workflow.week_ending_date-6 and v_workflow.week_ending_date
    ) or exists(
      select 1 from pg_catalog.jsonb_array_elements(v_schedule) row_value
      where (case when (row_value->>'end')::time<=(row_value->>'start')::time
        then (row_value->>'date')::date+1+(row_value->>'end')::time
        else (row_value->>'date')::date+(row_value->>'end')::time end)
          at time zone 'Europe/London'>p_now_utc
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_INCOMPLETE' using errcode='22023';
    end if;
  end if;
  perform private.weekly_source_candidate_app_units_week_v1(v_units_week);
  perform private.weekly_source_candidate_app_units_day_v1(v_units_day,null);
  if exists(
    select 1 from pg_catalog.jsonb_array_elements(v_schedule) item
    where (item->>'date')::date not between
      v_workflow.week_ending_date-6 and v_workflow.week_ending_date
  ) or exists(
    select 1
    from pg_catalog.jsonb_array_elements(v_schedule) item
    where nullif(item->>'row_key','') is not null
    group by item->>'row_key' having pg_catalog.count(*)>1
  ) or exists(
    select 1
    from pg_catalog.jsonb_array_elements(v_schedule) item
    group by item->>'date',item->>'start',item->>'end'
    having pg_catalog.count(*)>1
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
  if exists(
    with rows as (
      select ordinality,
        (value->>'date')::date+(value->>'start')::time as starts,
        case when (value->>'end')::time<=(value->>'start')::time
          then (value->>'date')::date+1+(value->>'end')::time
          else (value->>'date')::date+(value->>'end')::time end as ends
      from pg_catalog.jsonb_array_elements(v_schedule) with ordinality
    )
    select 1 from rows a join rows b on a.ordinality<b.ordinality
    where a.starts<b.ends and b.starts<a.ends
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_OVERLAP' using errcode='22023';
  end if;

  select
    pg_catalog.min(((value->>'date')::date+(value->>'start')::time)
      at time zone 'Europe/London'),
    pg_catalog.max((case when (value->>'end')::time<=(value->>'start')::time
      then (value->>'date')::date+1+(value->>'end')::time
      else (value->>'date')::date+(value->>'end')::time end)
      at time zone 'Europe/London'),
    coalesce(pg_catalog.sum(coalesce(
      nullif(value#>>'{break_entry,break_minutes}','')::integer,
      nullif(value#>>'{break_entry,calculated_break_minutes}','')::integer,0
    )),0)::integer,
    coalesce(pg_catalog.sum(
      (pg_catalog.date_part('epoch',(
        case when (value->>'end')::time<=(value->>'start')::time
          then (value->>'date')::date+1+(value->>'end')::time
          else (value->>'date')::date+(value->>'end')::time end
        -((value->>'date')::date+(value->>'start')::time)
      ))/60)::integer
      -coalesce(nullif(value#>>'{break_entry,break_minutes}','')::integer,
        nullif(value#>>'{break_entry,calculated_break_minutes}','')::integer,0)
    ),0)::integer
  into v_worked_start,v_worked_end,v_break_minutes,v_worked_minutes
  from pg_catalog.jsonb_array_elements(v_schedule);
  if v_worked_minutes<0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;

  if v_contract_week.timesheet_id is not null then
    select * into strict v_timesheet from public.timesheets
    where timesheet_id=v_contract_week.timesheet_id for update;
    if v_timesheet.contract_id is distinct from v_contract.id
       or v_timesheet.week_ending_date is distinct from v_workflow.week_ending_date
       or v_timesheet.sheet_scope<>'WEEKLY' or v_timesheet.line_type<>'HOURS'
       or v_timesheet.is_adjustment or not v_timesheet.is_current
       or v_timesheet.revoked_at is not null or v_timesheet.archived_at_utc is not null
       or v_timesheet.authorised_at_server is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_LOCKED' using errcode='55000';
    end if;
    if v_kind='CHECK_HOURS'
       and v_timesheet.timesheet_id is distinct from
         (v_scope#>>'{detail_target,id}')::uuid then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
    end if;
  elsif v_kind='CHECK_HOURS' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
  end if;
  if v_timesheet.timesheet_id is not null and (
       exists(select 1 from public.timesheets_financials financial
              where financial.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.invoice_lines invoice_line
                 where invoice_line.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.timesheet_pay_state pay_state
                 where pay_state.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.timesheet_pay_state_history pay_history
                 where pay_history.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.pay_batch_items batch_item
                 where batch_item.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.pay_batch_timesheet_snapshots batch_snapshot
                 where batch_snapshot.timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.pay_advances finance_case
                 where finance_case.linked_timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.pay_finance_case_components finance_component
                 where finance_component.linked_timesheet_id=v_timesheet.timesheet_id)
       or exists(select 1 from public.timesheet_payment_overrides payment_override
                 where payment_override.timesheet_id=v_timesheet.timesheet_id)
     ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_FINANCIAL_STATE_EXISTS' using errcode='55000';
  end if;

  v_previous_context:=pg_catalog.current_setting(
    'cloudtms.lifecycle_mutation_context',true
  );
  v_previous_defer_summary_refresh:=pg_catalog.current_setting(
    'cloudtms.lifecycle_defer_summary_refresh',true
  );
  perform pg_catalog.set_config(
    'cloudtms.lifecycle_mutation_context','ordinary_timesheet_save',true
  );
  -- Candidate comparison evidence is deliberately outside Banking Pay.  An
  -- update must not refresh its pay-summary cache; the later, ordinary
  -- authorisation owner remains the sole point that creates payment facts.
  perform pg_catalog.set_config(
    'cloudtms.lifecycle_defer_summary_refresh','on',true
  );
  if v_timesheet.timesheet_id is null then
    select * into strict v_candidate from public.candidates where id=v_candidate_id;
    select * into strict v_client from public.clients where id=v_contract.client_id;
    v_booking_id:='bk_'||pg_catalog.substr(pg_catalog.encode(
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_BASE_TIMESHEET_V1',
        pg_catalog.jsonb_build_object(
          'contract_id',v_contract.id,'week_ending_date',v_workflow.week_ending_date
        )
      ),'hex'
    ),1,24);
    if exists(select 1 from public.timesheets existing
              where existing.booking_id=v_booking_id and existing.is_current) then
      raise exception 'WEEKLY_SOURCE_BOOKING_ID_COLLISION' using errcode='55000';
    end if;
    insert into public.timesheets(
      booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
      worked_start_iso,worked_end_iso,break_minutes,worked_minutes,
      week_ending_date,contract_id,actual_schedule_json,additional_units_week,
      additional_units_per_day,r2_nurse_key,img_sha256_nurse,is_adjustment,
      candidate_workflow_id,candidate_workflow_generation,created_at,updated_at
    ) values (
      v_booking_id,1,true,'RECEIVED','WEEKLY','MANUAL','HOURS',
      pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(v_candidate.tms_ref),''),
        nullif(pg_catalog.btrim(v_candidate.display_name),''),v_candidate.id::text)),
      pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(v_contract.display_site),''),
        nullif(pg_catalog.btrim(v_client.name),''),v_client.id::text)),
      pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(v_contract.ward_hint),''),'contract')),
      pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(v_contract.role),''),'weekly')),
      'weekly-0',v_worked_start,v_worked_end,v_break_minutes,v_worked_minutes,
      v_workflow.week_ending_date,v_contract.id,v_schedule,v_units_week,v_units_day,
      v_signature.storage_key,pg_catalog.encode(v_signature.source_content_sha256,'hex'),false,
      v_workflow.id,v_workflow.generation+1,p_now_utc,p_now_utc
    ) returning * into v_timesheet;
    update public.contract_weeks
    set timesheet_id=v_timesheet.timesheet_id,status='SUBMITTED',
      submission_mode_snapshot='ELECTRONIC',updated_at=p_now_utc
    where id=v_contract_week.id returning * into v_contract_week;
  else
    update public.timesheets set
      status='RECEIVED',submission_mode='MANUAL',actual_schedule_json=v_schedule,
      additional_units_week=v_units_week,additional_units_per_day=v_units_day,
      worked_start_iso=v_worked_start,worked_end_iso=v_worked_end,
      break_minutes=v_break_minutes,worked_minutes=v_worked_minutes,
      r2_nurse_key=v_signature.storage_key,
      img_sha256_nurse=pg_catalog.encode(v_signature.source_content_sha256,'hex'),
      candidate_workflow_id=v_workflow.id,
      candidate_workflow_generation=v_workflow.generation+1,
      candidate_manager_approved_at_utc=null,updated_at=p_now_utc
    where timesheet_id=v_timesheet.timesheet_id returning * into v_timesheet;
    update public.contract_weeks
    set status=case when status='OPEN' then 'SUBMITTED' else status end,
      submission_mode_snapshot='ELECTRONIC',
      updated_at=p_now_utc
    where id=v_contract_week.id returning * into v_contract_week;
  end if;
  perform pg_catalog.set_config(
    'cloudtms.lifecycle_mutation_context',coalesce(v_previous_context,''),true
  );
  perform pg_catalog.set_config(
    'cloudtms.lifecycle_defer_summary_refresh',
    coalesce(v_previous_defer_summary_refresh,''),true
  );

  v_next_generation:=v_workflow.generation+1;
  select coalesce(pg_catalog.max(component_no),0)+1 into v_component_no
  from public.candidate_submission_components
  where workflow_id=v_workflow.id and workflow_generation=v_next_generation;
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,timesheet_id,
    component_kind,document_role,state,source_component_id,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc,required,
    review_ordinal,review_render_state,final_signed_render_state,created_at_utc
  ) values (
    v_workflow.id,v_next_generation,v_component_no,v_timesheet.timesheet_id,
    'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
    coalesce(v_signature.source_component_id,v_signature.id),v_signature.storage_key,
    v_signature.media_type,v_signature.byte_size,v_signature.source_content_sha256,
    p_now_utc,false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
  ) returning * into v_signature_copy;
  update public.candidate_submission_workflows set
    state='WORKER_SUBMITTED',generation=v_next_generation,
    anchor_timesheet_id=coalesce(anchor_timesheet_id,v_timesheet.timesheet_id),
    target_timesheet_id=v_timesheet.timesheet_id,
    input_snapshot_json=p_request->'immutable_submission',
    immutable_submission_json=p_request->'immutable_submission',
    immutable_submission_sha256=private._candidate_sha256_jsonb_v1(
      p_request->'immutable_submission'
    ),
    candidate_signature_component_id=v_signature_copy.id,
    candidate_signature_sha256=v_signature_copy.source_content_sha256,
    candidate_signed_at_utc=(p_request->>'candidate_signed_at_utc')::timestamptz,
    manager_name=null,manager_position=null,manager_signature_component_id=null,
    manager_signature_sha256=null,manager_approved_at_utc=null,
    review_manifest_json=null,review_manifest_sha256=null,
    issue_codes='[]'::jsonb,worker_submitted_at_utc=p_now_utc,
    finalised_at_utc=null,daily_context_sha256=null,canonical_financial_sha256=null,
    canonical_save_input_sha256=null,canonical_save_row_signature=null,
    canonical_save_financials_id=null,canonical_save_receipt_json=null,
    canonical_saved_at_utc=null,last_mutation_idempotency_key=v_key::text,
    last_mutation_response_json=null,updated_at_utc=p_now_utc
  where id=v_workflow.id returning * into v_workflow;

  v_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(
    v_timesheet.timesheet_id
  );
  if v_timesheet_hash is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
  end if;
  if v_kind='SUBMIT_TIMESHEET' then
    select * into strict v_submission
    from public.weekly_timesheet_submission_requests
    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=v_candidate_id and source_cycle_id=v_cycle.id
      and current_projection_publication_id=v_projection_publication_id
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
    for update;
    select * into strict v_membership
    from public.weekly_timesheet_submission_request_memberships
    where id=v_scope_id and submission_request_id=v_submission.id and state='WAITING'
    for update;
    v_compare:=private.weekly_source_candidate_submission_compare_sync_v1(
      v_generation.id,v_projection_publication_id,v_membership.id,
      v_timesheet.timesheet_id,v_timesheet_hash,p_now_utc
    );
    v_completion:=public.weekly_source_timesheet_submission_complete_atomic_v1(
      pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate_id,'submission_request_id',v_submission.id,
        'projection_publication_id',v_projection_publication_id,
        'completions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'membership_id',v_membership.id,'timesheet_id',v_timesheet.timesheet_id,
          'timesheet_revision',v_timesheet.version,
          'timesheet_hash',pg_catalog.encode(v_timesheet_hash,'hex'),
          'outcome',v_compare->>'outcome'
        ))
      )
    );
  end if;
  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'generation',v_workflow.generation,'state',v_workflow.state,
    'timesheet_id',v_timesheet.timesheet_id,'timesheet_revision',v_timesheet.version,
    'timesheet_hash',pg_catalog.encode(v_timesheet_hash,'hex'),
    'candidate_signature_component_id',v_signature_copy.id,
    'request_kind',v_kind,'comparison',v_compare,'completion',v_completion,
    'materialisation_request_hash',pg_catalog.encode(v_request_hash,'hex')
  );
  update public.candidate_submission_workflows
  set last_mutation_response_json=v_result where id=v_workflow.id;
  return v_result;
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_STALE' using errcode='40001';
end;
$function$;

alter function public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone) owner to postgres;

create or replace function public.weekly_exceptional_pay_stage_c1_request_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_top_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','client_id','work_event_id','evidence_timesheet_id',
    'expected_family_bound_version','protected_schedule','rate_classification',
    'source_proposal','target_snapshot','current_comparison_revision_id',
    'current_final_revision_id','c1_request','c1_sources','c1_components',
    'reason','idempotency_key'
  ];
  v_c1_keys constant text[]:=array[
    'request_id','request_sequence','actor_user_id','candidate_id','contract_id',
    'root_timesheet_id','week_ending_date','source_mode',
    'expected_head_revision','expected_source_count','expected_component_count',
    'expected_payload_bytes','source_manifest_sha256','entitlement_sha256',
    'approval_sha256','is_zero_entitlement','financial_row_id','request_sha256'
  ];
  v_source_keys constant text[]:=array[
    'source_ordinal','source_id','authority_kind','source_system',
    'external_identity','external_revision','source_document_sha256',
    'payload_bytes','part_count','work_date','root_timesheet_id','candidate_id',
    'contract_id','source_row_sha256','record_type','parts'
  ];
  v_part_keys constant text[]:=array[
    'source_ordinal','part_ordinal','payload_utf8','fragment_sha256','record_type'
  ];
  v_component_keys constant text[]:=array[
    'component_ordinal','component_id','source_ordinal','source_key',
    'component_kind','economic_key_type','economic_key_value',
    'component_member_identity','segment_id','segment_key','segment_stable_key',
    'work_date','reference_number','hours_day','hours_night','hours_sat',
    'hours_sun','hours_bh','additional_code_raw','unit_count','unit_pay_rate',
    -- S7 (WB-007, WB-013, 24 section 5): `adjustment_id` is deliberately absent
    -- from this exact-key allowlist and from the C1 component relation.  An
    -- adjustment is never copied into a head, because an adjustment created
    -- later would make the snapshot stale and create a second owner.  A caller
    -- that still sends the key now fails closed here.
    'unit_charge_rate','expense_code','pay_ex_vat',
    'charge_ex_vat','exclude_from_pay','origin','component_sha256','record_type'
  ];
  v_schedule_keys constant text[]:=array[
    'work_date','start_at_local','end_at_local','break_minutes'
  ];
  v_actor_user_id uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_source_cycle_id uuid;
  v_client_id uuid;
  v_work_event_id uuid;
  v_evidence_timesheet_id uuid;
  v_expected_bound_version bigint;
  v_comparison_revision_id uuid;
  v_final_revision_id uuid;
  v_reason text;
  v_idempotency_key text;
  v_schedule jsonb;
  v_rate_classification jsonb;
  v_source_proposal jsonb;
  v_selected_source_witness jsonb;
  v_target_snapshot jsonb;
  v_component_identity_context jsonb;
  v_component_identity_basis jsonb;
  v_start jsonb;
  v_sources jsonb;
  v_components jsonb;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_root public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_evidence public.timesheets%rowtype;
  v_signed_evidence jsonb;
  v_event public.weekly_work_events%rowtype;
  v_prior_generation public.weekly_exceptional_pay_generations%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_policy jsonb;
  v_source_mode text;
  v_work_date date;
  v_start_at timestamp without time zone;
  v_end_at timestamp without time zone;
  v_break_minutes integer;
  v_expected_source_count integer;
  v_expected_component_count integer;
  v_expected_payload_bytes integer;
  v_request_sequence bigint;
  v_expected_head_revision bigint;
  v_financial_row_id uuid;
  v_request_id uuid;
  v_source_manifest_hash bytea;
  v_entitlement_hash bytea;
  v_outer_approval_hash bytea;
  v_c1_request_hash bytea;
  v_rate_policy_hash bytea;
  v_schedule_hash bytea;
  v_source_proposal_hash bytea;
  v_prior_vector jsonb;
  v_prior_vector_hash bytea;
  v_next_vector jsonb;
  v_next_vector_hash bytea;
  v_fixed_source_hash bytea;
  v_local_approval_hash bytea;
  v_family_event_hash bytea;
  v_target_event_hash bytea;
  v_prior_family_event_hash bytea;
  v_prior_target_event_hash bytea;
  v_issue_ids uuid[]:='{}'::uuid[];
  v_issue_ids_hash bytea;
  v_signed_revision integer;
  v_signed_hash bytea;
  v_signed_at timestamptz;
  v_generation_number integer;
  v_generation_reason text;
  v_family_event_state text;
  v_resulting_lifecycle_state text;
  v_pending_outcome text;
  v_payment_event_kind text;
  v_other_wait_count integer:=0;
  v_family_event_sequence bigint;
  v_family_event_id uuid;
  v_target_event_sequence bigint;
  v_total_pay numeric(12,2):=0;
  v_payload_total bigint:=0;
  v_previous_source_key text:=null;
  v_source_entry jsonb;
  v_part_entry jsonb;
  v_component_entry jsonb;
  v_source_id uuid;
  v_source_ordinal integer;
  v_source_counter integer:=0;
  v_part_ordinal integer;
  v_component_ordinal integer;
  v_part_payload bytea;
  v_part_concat bytea;
  v_source_part_count integer;
  v_source_payload_bytes integer;
  v_source_kind text;
  v_source_document jsonb;
  v_count_client integer:=0;
  v_count_candidate integer:=0;
  v_count_root integer:=0;
  v_count_provider integer:=0;
  v_count_office integer:=0;
  v_step_sequence integer;
  v_request_body_hash bytea;
  v_before_hash bytea;
  v_after_hash bytea;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_top_keys)
     or not (p_request ?& v_top_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_STAGE_REQUEST_V1'
     or pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'rate_classification')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'source_proposal')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'target_snapshot')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'c1_request')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'c1_sources')<>'array'
     or pg_catalog.jsonb_typeof(p_request->'c1_components')<>'array' then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end if;
  if pg_catalog.octet_length(pg_catalog.convert_to(p_request::text,'UTF8'))>4194304 then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_TOO_LARGE' using errcode='54000';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
    v_source_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_client_id:=(p_request->>'client_id')::uuid;
    v_work_event_id:=(p_request->>'work_event_id')::uuid;
    v_evidence_timesheet_id:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
    v_expected_bound_version:=(p_request->>'expected_family_bound_version')::bigint;
    v_comparison_revision_id:=nullif(p_request->>'current_comparison_revision_id','')::uuid;
    v_final_revision_id:=nullif(p_request->>'current_final_revision_id','')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end;
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end if;

  v_schedule:=p_request->'protected_schedule';
  if not private.weekly_exceptional_json_keys_exact_v1(v_schedule,v_schedule_keys)
     or not (v_schedule ?& v_schedule_keys) then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end if;
  begin
    v_work_date:=(v_schedule->>'work_date')::date;
    v_start_at:=(v_schedule->>'start_at_local')::timestamp without time zone;
    v_end_at:=(v_schedule->>'end_at_local')::timestamp without time zone;
    v_break_minutes:=(v_schedule->>'break_minutes')::integer;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end;
  if v_end_at<=v_start_at or v_start_at::date<>v_work_date
     or v_break_minutes<0
     or v_break_minutes>=extract(epoch from (v_end_at-v_start_at))/60 then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end if;
  v_rate_classification:=p_request->'rate_classification';
  v_source_proposal:=p_request->'source_proposal';
  v_target_snapshot:=p_request->'target_snapshot';
  if pg_catalog.octet_length(pg_catalog.convert_to(v_rate_classification::text,'UTF8'))>65536
     or pg_catalog.octet_length(pg_catalog.convert_to(v_source_proposal::text,'UTF8'))>65536
     or pg_catalog.octet_length(pg_catalog.convert_to(v_target_snapshot::text,'UTF8'))>1048576 then
    raise exception 'WEEKLY_PROTECTED_TARGET_TOO_LARGE' using errcode='54000';
  end if;

  v_start:=p_request->'c1_request';
  v_sources:=p_request->'c1_sources';
  v_components:=p_request->'c1_components';
  if not private.weekly_exceptional_json_keys_exact_v1(v_start,v_c1_keys)
     or not (v_start ?& v_c1_keys)
     or pg_catalog.jsonb_array_length(v_sources) not between 1 and 65536
     or pg_catalog.jsonb_array_length(v_components)>65536 then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_INVALID' using errcode='22023';
  end if;
  begin
    v_request_id:=(v_start->>'request_id')::uuid;
    v_request_sequence:=(v_start->>'request_sequence')::bigint;
    v_expected_head_revision:=(v_start->>'expected_head_revision')::bigint;
    v_expected_source_count:=(v_start->>'expected_source_count')::integer;
    v_expected_component_count:=(v_start->>'expected_component_count')::integer;
    v_expected_payload_bytes:=(v_start->>'expected_payload_bytes')::integer;
    v_financial_row_id:=(v_start->>'financial_row_id')::uuid;
    v_source_manifest_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'source_manifest_sha256');
    v_entitlement_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'entitlement_sha256');
    v_outer_approval_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'approval_sha256');
    v_c1_request_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'request_sha256');
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_INVALID' using errcode='22023';
  end;
  if v_request_sequence<1 or v_expected_head_revision<0
     or v_expected_source_count<>pg_catalog.jsonb_array_length(v_sources)
     or v_expected_component_count<>pg_catalog.jsonb_array_length(v_components)
     or v_expected_payload_bytes<1
     or ((v_start->>'is_zero_entitlement')::boolean is distinct from
         (v_expected_component_count=0)) then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_COUNT_INVALID' using errcode='22023';
  end if;

  -- An exact staged request is the idempotency authority even after the family
  -- version advances.  A UUID collision with different bytes always refuses.
  -- SP-12: existing admission only; no producer/version evidence.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  select request.* into v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id
  for update;
  if found then
    if v_publication.family_id is distinct from v_family_id
       or v_publication.orchestration_run_id is distinct from v_run_id
       or v_publication.request_sha256 is distinct from v_c1_request_hash then
      raise exception 'WEEKLY_PROTECTED_C1_REQUEST_ID_COLLISION' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','STAGED','idempotent_replay',true,
      'family_id',v_publication.family_id,'generation_id',v_publication.generation_id,
      'publication_request_id',v_publication.id,'request_sha256',
      pg_catalog.encode(v_publication.request_sha256,'hex'),
      'state',v_publication.state
    );
  end if;

  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id
  for update;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle
  where cycle.id=v_source_cycle_id
  for share;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id and source_group.active
  for share;
  perform private.weekly_source_office_authority_v1(
    v_actor_user_id,'APPROVE_PROTECTED_PAY',v_group.id,v_client_id,v_work_date
  );

  if v_run.requested_by_user_id is distinct from v_actor_user_id
     or v_run.state<>'RUNNING'
     or v_run.request_kind not in (
       'APPROVE','AMEND','WITHDRAW','RECONCILE','RECORD_NOT_WORKED'
     )
     or v_family.bound_version<>v_expected_bound_version
     or v_family.c1_publication_state in ('PENDING','PUBLISHING')
     or v_family.agency_id is distinct from v_group.agency_id
     or v_family.root_timesheet_id is null
     or v_work_date not between v_family.week_start_date and v_family.week_ending_date
     or not exists(
       select 1 from public.weekly_source_group_clients membership
       where membership.source_group_id=v_group.id
         and membership.client_id=v_client_id
         and v_work_date between membership.valid_from
           and coalesce(membership.valid_to,'infinity'::date)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_SCOPE_INVALID' using errcode='55000';
  end if;

  select timesheet.* into strict v_root
  from public.timesheets timesheet
  where timesheet.timesheet_id=v_family.root_timesheet_id
  for share;
  select financial.* into strict v_fin
  from public.timesheets_financials financial
  where financial.id=v_financial_row_id
    and financial.timesheet_id=v_root.timesheet_id
    and financial.is_current
  for share;
  select event.* into strict v_event
  from public.weekly_work_events event
  where event.id=v_work_event_id
    and event.candidate_id=v_family.candidate_id
    and event.client_id=v_client_id
    and event.work_date=v_work_date
  for share;
  if v_root.contract_id is distinct from v_family.contract_id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or v_root.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_root.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_root.is_adjustment or not v_root.is_current
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null then
    raise exception 'WEEKLY_PROTECTED_C1_ROOT_INVALID' using errcode='55000';
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_client_id,v_family.contract_id,v_work_date
  );
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_PROTECTED_C1_POLICY_INVALID' using errcode='55000';
  end if;

  -- Existing WITHDRAW/RECONCILE actions accept actual Source, including
  -- certified absence; this does not define any other removal action. A
  -- Final-only synthetic zero proposal is not proof that Source is absent.
  -- Keep replay above this fresh admission check; do not reinterpret a saved
  -- decision after subsequent imports or change the Banking publication owner.
  if v_run.request_kind in ('WITHDRAW','RECONCILE') then
    v_selected_source_witness:=private.weekly_source_selected_source_witness_v1(
      v_family.id,v_cycle.id,v_event.id
    );
    if v_selected_source_witness is null
       or v_selected_source_witness->'source_proposal' is distinct from v_source_proposal
       or (v_selected_source_witness->>'kind'='CURRENT_ROW' and (
         v_selected_source_witness#>>'{basis,final_revision_id}' is null
         or v_selected_source_witness#>>'{basis,final_revision_id}' is distinct from v_source_proposal->>'source_revision'
         or v_source_proposal->'source_present' is distinct from 'true'::jsonb
       ))
       or (v_selected_source_witness->>'kind'='CERTIFIED_ABSENCE' and (
         v_source_proposal->'source_present' is distinct from 'false'::jsonb
         or v_source_proposal->>'source_minutes' is distinct from '0'
       ))
       or coalesce(v_selected_source_witness->>'kind' not in ('CURRENT_ROW','CERTIFIED_ABSENCE'),true) then
      raise exception 'WEEKLY_PROTECTED_C1_SELECTED_SOURCE_UNAVAILABLE' using errcode='55000';
    end if;
  end if;

  if (v_start->>'actor_user_id')::uuid is distinct from v_actor_user_id
     or (v_start->>'candidate_id')::uuid is distinct from v_family.candidate_id
     or (v_start->>'contract_id')::uuid is distinct from v_family.contract_id
     or (v_start->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
     or (v_start->>'week_ending_date')::date is distinct from v_family.week_ending_date
     or v_start->>'source_mode'<>v_source_mode
     or v_financial_row_id is distinct from v_fin.id
     or v_request_sequence<>v_family.current_generation_number+1 then
    raise exception 'WEEKLY_PROTECTED_C1_START_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_family.current_generation_id is null then
    if v_expected_head_revision<>0 then
      raise exception 'WEEKLY_PROTECTED_C1_HEAD_INVALID' using errcode='55000';
    end if;
  else
    select generation.* into strict v_prior_generation
    from public.weekly_exceptional_pay_generations generation
    where generation.id=v_family.current_generation_id
      and generation.family_id=v_family.id
      and generation.lifecycle_state='PUBLISHED'
    for share;
    select request.* into strict v_publication
    from public.weekly_exceptional_c1_publication_requests request
    where request.generation_id=v_prior_generation.id
      and request.family_id=v_family.id
      and (request.state='PUBLISHED' or (request.state='RETIRED' and exists(
        select 1 from private.weekly_source_local_protected_decision_receipts receipt
        where receipt.publication_request_id=request.id and receipt.generation_id=v_prior_generation.id
          and receipt.state='COMPLETE')));
    if (case when v_publication.state='RETIRED' then v_prior_generation.generation_number::bigint
             else v_publication.c1_head_revision end) is distinct from v_expected_head_revision then
      raise exception 'WEEKLY_PROTECTED_C1_HEAD_INVALID' using errcode='55000';
    end if;
    v_publication.id:=null;
  end if;

  if v_evidence_timesheet_id is not null then
    select evidence.* into strict v_evidence
    from public.timesheets evidence
    where evidence.timesheet_id=v_evidence_timesheet_id
      and evidence.contract_id=v_family.contract_id
      and evidence.week_ending_date=v_family.week_ending_date
      and evidence.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and evidence.is_current and evidence.revoked_at is null
      and evidence.archived_at_utc is null;
    v_signed_evidence:=private.weekly_exceptional_candidate_signed_evidence_v1(
      v_evidence_timesheet_id
    );
    v_signed_at:=(v_signed_evidence->>'signed_at_utc')::timestamptz;
    v_signed_revision:=(v_signed_evidence->>'timesheet_version')::integer;
    v_signed_hash:=private.weekly_exceptional_hex_sha256_v1(
      v_signed_evidence->>'signature_sha256'
    );
  end if;

  select coalesce(pg_catalog.array_agg(incident.id order by incident.id),'{}'::uuid[])
    into v_issue_ids
  from public.weekly_discrepancy_incidents incident
  where incident.source_group_id=v_group.id
    and incident.work_event_id=v_work_event_id
    and incident.candidate_id=v_family.candidate_id
    and incident.client_id=v_client_id;
  v_issue_ids_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_ISSUE_SET_V1',pg_catalog.to_jsonb(v_issue_ids)
  );
  v_schedule_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SCHEDULE_V1',v_schedule
  );
  v_rate_policy_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_RATE_POLICY_V1',
    pg_catalog.jsonb_build_object(
      'contract_id',v_family.contract_id,
      'rates_json',(select contract.rates_json from public.contracts contract where contract.id=v_family.contract_id),
      'policy',v_policy,'classification',v_rate_classification
    )
  );
  v_source_proposal_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',v_source_proposal
  );

  if v_comparison_revision_id is not null and not exists(
    select 1
    from public.weekly_issue_comparison_revisions comparison
    join public.weekly_discrepancy_incidents incident on incident.id=comparison.incident_id
    where comparison.id=v_comparison_revision_id
      and comparison.contract_id=v_family.contract_id
      and incident.source_group_id=v_group.id
      and incident.work_event_id=v_work_event_id
      and incident.candidate_id=v_family.candidate_id
      and incident.client_id=v_client_id
  ) then
    raise exception 'WEEKLY_PROTECTED_COMPARISON_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_final_revision_id is not null and not exists(
    select 1 from public.weekly_source_final_revisions final_revision
    where final_revision.id=v_final_revision_id
      and final_revision.source_cycle_id=v_source_cycle_id
      and final_revision.state='CURRENT'
  ) then
    raise exception 'WEEKLY_PROTECTED_FINAL_SCOPE_INVALID' using errcode='55000';
  end if;

  -- Validate and persist the complete normalized C1 source stream.  The C1
  -- owner independently replays its TLV hashes before certification; these
  -- local checks prevent truncation, reordering or cross-root substitution.
  for v_source_entry in
    select value from pg_catalog.jsonb_array_elements(v_sources)
  loop
    v_source_counter:=v_source_counter+1;
    if not private.weekly_exceptional_json_keys_exact_v1(v_source_entry,v_source_keys)
       or not (v_source_entry ?& v_source_keys)
       or v_source_entry->>'record_type'<>'SOURCE'
       or pg_catalog.jsonb_typeof(v_source_entry->'parts')<>'array' then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_INVALID' using errcode='22023';
    end if;
    begin
      v_source_ordinal:=(v_source_entry->>'source_ordinal')::integer;
      v_source_id:=(v_source_entry->>'source_id')::uuid;
      v_source_payload_bytes:=(v_source_entry->>'payload_bytes')::integer;
      v_source_part_count:=(v_source_entry->>'part_count')::integer;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_INVALID' using errcode='22023';
    end;
    if v_source_ordinal<>v_source_counter
       or v_source_ordinal not between 1 and v_expected_source_count
       or (v_source_entry->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
       or (v_source_entry->>'candidate_id')::uuid is distinct from v_family.candidate_id
       or (v_source_entry->>'contract_id')::uuid is distinct from v_family.contract_id
       or v_source_payload_bytes not between 1 and 16384
       or v_source_part_count<>pg_catalog.jsonb_array_length(v_source_entry->'parts')
       or v_source_part_count not between 1 and 6 then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_SCOPE_INVALID' using errcode='55000';
    end if;
    v_source_kind:=v_source_entry->>'authority_kind';
    if v_source_kind not in (
      'CANDIDATE_SUBMISSION','CLIENT_SOURCE','OFFICE_APPROVAL','ROOT_FINANCIAL',
      'PROVIDER','SOURCE_EXPENSE','ORDINARY_EXPENSE','APPROVED_COMPONENT'
    ) then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_KIND_INVALID' using errcode='22023';
    end if;
    v_count_client:=v_count_client+(v_source_kind='CLIENT_SOURCE')::integer;
    v_count_candidate:=v_count_candidate+(v_source_kind='CANDIDATE_SUBMISSION')::integer;
    v_count_root:=v_count_root+(v_source_kind='ROOT_FINANCIAL')::integer;
    v_count_provider:=v_count_provider+(v_source_kind='PROVIDER')::integer;
    v_count_office:=v_count_office+(v_source_kind='OFFICE_APPROVAL')::integer;
    v_part_concat:=''::bytea;
    v_part_ordinal:=0;
    for v_part_entry in
      select value from pg_catalog.jsonb_array_elements(v_source_entry->'parts')
    loop
      v_part_ordinal:=v_part_ordinal+1;
      if not private.weekly_exceptional_json_keys_exact_v1(v_part_entry,v_part_keys)
         or not (v_part_entry ?& v_part_keys)
         or v_part_entry->>'record_type'<>'PART'
         or (v_part_entry->>'source_ordinal')::integer<>v_source_ordinal
         or (v_part_entry->>'part_ordinal')::integer<>v_part_ordinal
         or coalesce(v_part_entry->>'payload_utf8','')!~'^(?:[0-9a-f]{2})+$' then
        raise exception 'WEEKLY_PROTECTED_C1_PART_INVALID' using errcode='22023';
      end if;
      v_part_payload:=pg_catalog.decode(v_part_entry->>'payload_utf8','hex');
      if pg_catalog.octet_length(v_part_payload) not between 1 and 3072
         or extensions.digest(v_part_payload,'sha256') is distinct from
            private.weekly_exceptional_hex_sha256_v1(v_part_entry->>'fragment_sha256') then
        raise exception 'WEEKLY_PROTECTED_C1_PART_HASH_INVALID' using errcode='22023';
      end if;
      v_part_concat:=v_part_concat||v_part_payload;
    end loop;
    if v_part_ordinal<>v_source_part_count
       or pg_catalog.octet_length(v_part_concat)<>v_source_payload_bytes then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_PAYLOAD_INVALID' using errcode='22023';
    end if;
    begin
      v_source_document:=pg_catalog.convert_from(v_part_concat,'UTF8')::jsonb;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_DOCUMENT_INVALID' using errcode='22023';
    end;
    if pg_catalog.jsonb_typeof(v_source_document)<>'object'
       or (v_source_document->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
       or (v_source_document->>'candidate_id')::uuid is distinct from v_family.candidate_id
       or (v_source_document->>'contract_id')::uuid is distinct from v_family.contract_id
       or (v_source_document->>'week_ending_date')::date is distinct from v_family.week_ending_date
       or v_source_document->>'document_sha256' is distinct from
          v_source_entry->>'source_document_sha256' then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_DOCUMENT_SCOPE_INVALID' using errcode='55000';
    end if;
    if v_source_kind='CLIENT_SOURCE'
       and coalesce((v_source_document->>'source_complete')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_C1_CLIENT_SOURCE_INCOMPLETE' using errcode='55000';
    elsif v_source_kind='ROOT_FINANCIAL'
       and (v_source_document->>'financial_row_id')::uuid is distinct from v_fin.id then
      raise exception 'WEEKLY_PROTECTED_C1_ROOT_FINANCIAL_INVALID' using errcode='55000';
    elsif v_source_kind='OFFICE_APPROVAL'
       and (
         (v_source_document->>'actor_user_id')::uuid is distinct from v_actor_user_id
         or (v_source_document->>'request_id')::uuid is distinct from v_request_id
         or v_source_document->>'entitlement_sha256' is distinct from
            v_start->>'entitlement_sha256'
         or v_source_document->>'decision'<>'APPROVE_ENTITLEMENT'
       ) then
      raise exception 'WEEKLY_PROTECTED_C1_OFFICE_SOURCE_INVALID' using errcode='55000';
    end if;
    v_payload_total:=v_payload_total+v_source_payload_bytes;
  end loop;
  if v_source_counter<>v_expected_source_count
     or v_count_client<1 or v_count_root<>1 or v_count_provider<>1 or v_count_office<>1
     or (v_evidence_timesheet_id is null and v_count_candidate<>0)
     or (v_evidence_timesheet_id is not null and v_count_candidate<>1)
     or v_payload_total<>v_expected_payload_bytes then
    raise exception 'WEEKLY_PROTECTED_C1_SOURCE_CARDINALITY_INVALID' using errcode='55000';
  end if;

  v_component_ordinal:=0;
  for v_component_entry in
    select value from pg_catalog.jsonb_array_elements(v_components)
  loop
    v_component_ordinal:=v_component_ordinal+1;
    if not private.weekly_exceptional_json_keys_exact_v1(v_component_entry,v_component_keys)
       or not (v_component_entry ?& v_component_keys)
       or v_component_entry->>'record_type'<>'COMPONENT'
       or (v_component_entry->>'component_ordinal')::integer<>v_component_ordinal
       or (v_component_entry->>'source_ordinal')::integer not between 1 and v_expected_source_count
       or coalesce(v_component_entry->>'source_key','')=''
       or (v_previous_source_key is not null and v_component_entry->>'source_key'<=v_previous_source_key) then
      raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_INVALID' using errcode='22023';
    end if;
    if nullif(v_component_entry->>'work_date','') is not null
       and (v_component_entry->>'work_date')::date not between
           v_family.week_start_date and v_family.week_ending_date then
      raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_WEEK_INVALID' using errcode='55000';
    end if;
    v_total_pay:=v_total_pay+(v_component_entry->>'pay_ex_vat')::numeric;
    perform private.weekly_exceptional_hex_sha256_v1(v_component_entry->>'component_sha256');
    v_previous_source_key:=v_component_entry->>'source_key';
  end loop;
  if v_component_ordinal<>v_expected_component_count then
    raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_COUNT_INVALID' using errcode='22023';
  end if;
  v_total_pay:=pg_catalog.round(v_total_pay,2);

  if v_family.current_generation_id is null then
    -- Before the prepare owner distinguished empty from published families,
    -- a retry could be recorded as AMEND even though nothing was published.
    -- Preserve that immutable request and permit only its first generation;
    -- never treat an existing staged/published entitlement as an initial one.
    if v_run.request_kind not in ('APPROVE','AMEND')
       or v_family.current_generation_number<>0
       or v_family.current_lifecycle_state<>'PENDING_APPROVAL'
       or exists(select 1 from public.weekly_exceptional_pay_generations existing
         where existing.family_id=v_family.id) then
      raise exception 'WEEKLY_PROTECTED_C1_INITIAL_ACTION_INVALID' using errcode='55000';
    end if;
    v_prior_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_TARGET_VECTOR_V1',
      'components',pg_catalog.jsonb_build_array(),
      'component_count',0,'is_zero_entitlement',true
    );
    v_generation_number:=1;
    v_generation_reason:='INITIAL_APPROVAL';
  else
    v_prior_vector:=v_prior_generation.complete_next_vector_json;
    v_generation_number:=v_prior_generation.generation_number+1;
    v_generation_reason:=case v_run.request_kind
      when 'AMEND' then 'OFFICE_AMENDMENT'
      when 'WITHDRAW' then 'WITHDRAW'
      when 'RECONCILE' then 'RECONCILE'
      when 'RECORD_NOT_WORKED' then 'RECORD_NOT_WORKED'
      else null end;
    if v_generation_reason is null then
      raise exception 'WEEKLY_PROTECTED_C1_ACTION_INVALID' using errcode='55000';
    end if;
  end if;
  v_family_event_state:=case
    when v_run.request_kind in ('APPROVE','AMEND') then 'WAIT'
    when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE'
    else 'NOT_WORKED' end;
  if v_run.request_kind='RECORD_NOT_WORKED'
     and coalesce((v_source_proposal->>'source_present')::boolean,true) then
    raise exception 'WEEKLY_PROTECTED_NOT_WORKED_SOURCE_PRESENT' using errcode='55000';
  end if;
  with latest as (
    select family_event.durable_work_event_id,family_event.state,
      pg_catalog.row_number() over (
        partition by family_event.durable_work_event_id
        order by family_event.event_sequence desc
      ) as event_rank
    from public.weekly_exceptional_pay_family_events family_event
    where family_event.family_id=v_family.id
      and family_event.durable_work_event_id<>v_work_event_id
  )
  select pg_catalog.count(*)::integer into v_other_wait_count
  from latest where event_rank=1 and state='WAIT';
  v_resulting_lifecycle_state:=case
    when v_family_event_state='WAIT' or v_other_wait_count>0 then 'WAITING_SOURCE'
    when v_run.request_kind='RECORD_NOT_WORKED' then 'NOT_WORKED'
    else 'RECONCILED' end;
  v_pending_outcome:=case v_family_event_state
    when 'WAIT' then 'WAIT'
    when 'ACCEPTED_SOURCE' then 'ACCEPT_SOURCE'
    else 'RECORD_NOT_WORKED' end;
  v_payment_event_kind:=case v_run.request_kind
    when 'APPROVE' then 'APPROVED'
    when 'AMEND' then 'APPROVED'
    when 'RECORD_NOT_WORKED' then 'RECORDED_NOT_WORKED'
    else 'MATCH_ACCEPTED' end;
  v_prior_vector_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_prior_vector
  );
  v_next_vector:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_TARGET_VECTOR_V1',
    'target_snapshot',v_target_snapshot,
    'components',v_components,
    'component_count',v_expected_component_count,
    'is_zero_entitlement',(v_expected_component_count=0),
    'entitlement_sha256',v_start->>'entitlement_sha256',
    'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00')
  );
  -- Identity metadata is admitted only against this fresh, actual server
  -- context under the established stage locks, never the caller's manifest.
  -- Matching clocks are not a historical identity-derivation fallback.
  v_component_identity_context:=public.weekly_exceptional_pay_action_context_v1(
    jsonb_build_object('schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
      'actor_user_id',v_actor_user_id,'family_id',v_family.id,
      'orchestration_run_id',v_run.id,'source_cycle_id',v_cycle.id,
      'work_event_id',v_event.id,'protected_schedule',v_schedule,
      'evidence_timesheet_id',v_evidence_timesheet_id));
  if v_component_identity_context->>'root_timesheet_id' is distinct from v_root.timesheet_id::text
     or v_component_identity_context#>>'{current_financial,id}' is distinct from v_fin.id::text
     or v_component_identity_context->>'family_bound_version' is distinct from v_expected_bound_version::text
     or v_component_identity_context->'source_proposal' is distinct from v_source_proposal then
    raise exception 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_UNAVAILABLE' using errcode='55000'; end if;
  v_component_identity_basis:=private.weekly_source_protected_component_manifest_v1(
    v_component_identity_context,v_target_snapshot);
  v_next_vector:=v_next_vector||jsonb_build_object(
    'protected_component_identity_basis',v_component_identity_basis);
  -- Seal the common before-position at approval. A later authorised amendment
  -- must not silently replace an entitlement published after this calculation.
  if v_root.authorised_at_server is not null then
    v_next_vector:=v_next_vector||pg_catalog.jsonb_build_object(
      'prior_effective_inventory',private.weekly_source_effective_inventory_v1(v_root.timesheet_id));
  end if;
  v_next_vector_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_next_vector
  );
  v_fixed_source_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FIXED_SOURCE_STATE_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_source_cycle_id,
      'comparison_revision_id',v_comparison_revision_id,
      'final_revision_id',v_final_revision_id,
      'source_proposal_hash',pg_catalog.encode(v_source_proposal_hash,'hex'),
      'financial_row_id',v_fin.id,'financial_timesheet_version',v_fin.timesheet_version
    )
  );
  v_local_approval_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_APPROVAL_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'work_event_id',v_work_event_id,
      'candidate_id',v_family.candidate_id,'client_id',v_client_id,
      'contract_id',v_family.contract_id,'week_ending_date',v_family.week_ending_date,
      'schedule_hash',pg_catalog.encode(v_schedule_hash,'hex'),
      'evidence_timesheet_id',v_evidence_timesheet_id,
      'signed_hash',case when v_signed_hash is null then null else pg_catalog.encode(v_signed_hash,'hex') end,
      'issue_ids_hash',pg_catalog.encode(v_issue_ids_hash,'hex'),
      'rate_policy_hash',pg_catalog.encode(v_rate_policy_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'actor_user_id',v_actor_user_id,'reason',v_reason
    )
  );

  insert into public.weekly_exceptional_payment_approvals(
    pay_target_family_id,work_event_id,evidence_timesheet_id,candidate_id,
    client_id,contract_id,week_ending,protected_work_date,
    protected_start_at_local,protected_end_at_local,protected_break_minutes,
    signed_submission_timesheet_id,signed_submission_revision,
    signed_submission_hash,signed_at_utc,contributing_issue_episode_ids,
    contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
    contract_rate_policy_source_fingerprint,approved_by_user_id,
    approval_reason,source_cycle_id,comparison_revision_id,final_revision_id,
    approved_target_pay_components_json,approved_target_gross,
    creation_orchestration_run_id,approval_hash,creation_idempotency_key
  ) values (
    v_family.id,v_work_event_id,v_evidence_timesheet_id,v_family.candidate_id,
    v_client_id,v_family.contract_id,v_family.week_ending_date,v_work_date,
    v_start_at,v_end_at,v_break_minutes,
    v_evidence_timesheet_id,v_signed_revision,v_signed_hash,v_signed_at,v_issue_ids,
    v_issue_ids_hash,v_schedule_hash,v_rate_policy_hash,v_actor_user_id,
    v_reason,v_source_cycle_id,v_comparison_revision_id,v_final_revision_id,
    v_target_snapshot,v_total_pay,v_run.id,v_local_approval_hash,
    v_idempotency_key||':approval'
  ) returning * into v_approval;

  insert into public.weekly_exceptional_pay_generations(
    family_id,generation_number,prior_generation_id,prior_generation_hash,
    request_idempotency_key,reason,complete_prior_vector_json,
    complete_prior_vector_hash,complete_next_vector_json,complete_next_vector_hash,
    fixed_target_source_state_fingerprint,lifecycle_state
  ) values (
    v_family.id,v_generation_number,v_family.current_generation_id,
    case when v_family.current_generation_id is null then null else v_prior_vector_hash end,
    v_idempotency_key||':generation',v_generation_reason,v_prior_vector,
    v_prior_vector_hash,v_next_vector,v_next_vector_hash,v_fixed_source_hash,'PENDING_C1'
  ) returning * into v_generation;

  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_family_event_sequence,v_prior_family_event_hash
  from public.weekly_exceptional_pay_family_events
  where family_id=v_family.id;
  v_family_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FAMILY_EVENT_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_family_event_sequence,
      'work_event_id',v_work_event_id,'approval_id',v_approval.id,
      'schedule',v_schedule,'source_proposal_hash',pg_catalog.encode(v_source_proposal_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'prior_event_hash',case when v_prior_family_event_hash is null then null
        else pg_catalog.encode(v_prior_family_event_hash,'hex') end
    )
  );
  insert into public.weekly_exceptional_pay_family_events(
    family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
    source_proposal_snapshot_json,source_proposal_hash,
    fixed_office_target_snapshot_json,fixed_office_target_hash,state,
    current_comparison_revision_id,current_final_revision_id,office_actor_user_id,
    office_reason,prior_event_hash,event_hash
  ) values (
    v_family.id,v_family_event_sequence,v_work_event_id,v_approval.id,
    v_work_date,v_start_at,v_end_at,v_break_minutes,v_rate_classification,
    v_source_proposal,v_source_proposal_hash,v_schedule,v_schedule_hash,v_family_event_state,
    v_comparison_revision_id,v_final_revision_id,v_actor_user_id,v_reason,
    v_prior_family_event_hash,v_family_event_hash
  ) returning id into v_family_event_id;
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_protected_current_decision_capture_v1(v_family_event_id);
  end if;

  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_target_event_sequence,v_prior_target_event_hash
  from public.weekly_exceptional_pay_target_events
  where family_id=v_family.id;
  v_target_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_TARGET_EVENT_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_target_event_sequence,
      'approval_id',v_approval.id,'generation_id',v_generation.id,
      'prior_vector_hash',pg_catalog.encode(v_prior_vector_hash,'hex'),
      'next_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'reason',v_generation_reason,'actor_user_id',v_actor_user_id
    )
  );
  insert into public.weekly_exceptional_pay_target_events(
    family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,
    fixed_target_component_snapshot,current_source_proposal_snapshot,
    complete_prior_family_vector_fingerprint,
    complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key
  ) values (
    v_family.id,v_approval.id,v_target_event_sequence,v_comparison_revision_id,
    v_final_revision_id,v_prior_target_event_hash,v_target_snapshot,v_source_proposal,
    v_prior_vector_hash,v_next_vector_hash,v_generation_reason,v_resulting_lifecycle_state,
    v_generation.id,v_actor_user_id,v_target_event_hash,v_idempotency_key||':target-event'
  );
  update public.weekly_exceptional_pending_reconciliation_targets target
  set state=case when v_run.request_kind='WITHDRAW' then 'WITHDRAWN' else 'CONSUMED' end,
      completed_at_utc=pg_catalog.statement_timestamp()
  where target.family_id=v_family.id
    and target.durable_work_event_id=v_work_event_id
    and target.state='ACTIVE';
  if v_family_event_state='WAIT' then
    insert into public.weekly_exceptional_pending_reconciliation_targets(
      family_id,approval_id,durable_work_event_id,incident_id,current_final_revision_id,
      intended_outcome,source_action_policy_target_fingerprint,state
    ) values (
      v_family.id,v_approval.id,v_work_event_id,
      (select incident.id from public.weekly_discrepancy_incidents incident
       where incident.source_group_id=v_group.id and incident.work_event_id=v_work_event_id
       order by incident.episode_number desc limit 1),
      v_final_revision_id,v_pending_outcome,v_fixed_source_hash,'ACTIVE'
    );
  end if;
  insert into public.weekly_exceptional_payment_events(
    family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,
    idempotency_key
  ) values (
    v_family.id,v_approval.id,v_payment_event_kind,'PENDING_C1',
    pg_catalog.jsonb_build_object(
      'work_event_id',v_work_event_id,'generation_id',v_generation.id,
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00')
    ),v_idempotency_key||':payment-event'
  );

  insert into public.weekly_exceptional_c1_publication_requests(
    id,family_id,generation_id,orchestration_run_id,agency_id,candidate_id,
    contract_id,root_timesheet_id,week_ending_date,source_mode,request_sequence,
    expected_head_revision,expected_source_count,expected_component_count,
    expected_payload_bytes,source_manifest_sha256,entitlement_sha256,
    approval_sha256,is_zero_entitlement,financial_row_id,request_sha256,state
  ) values (
    v_request_id,v_family.id,v_generation.id,v_run.id,v_family.agency_id,
    v_family.candidate_id,v_family.contract_id,v_root.timesheet_id,
    v_family.week_ending_date,v_source_mode,v_request_sequence,
    v_expected_head_revision,v_expected_source_count,v_expected_component_count,
    v_expected_payload_bytes,v_source_manifest_hash,v_entitlement_hash,
    v_outer_approval_hash,(v_expected_component_count=0),v_fin.id,
    v_c1_request_hash,'READY'
  ) returning * into v_publication;

  for v_source_entry in select value from pg_catalog.jsonb_array_elements(v_sources)
  loop
    v_source_ordinal:=(v_source_entry->>'source_ordinal')::integer;
    v_source_id:=(v_source_entry->>'source_id')::uuid;
    insert into public.weekly_exceptional_c1_source_records(
      id,publication_request_id,source_ordinal,authority_kind,source_system,
      external_identity,external_revision,source_document_sha256,payload_bytes,
      part_count,work_date,root_timesheet_id,candidate_id,contract_id,
      source_row_sha256
    ) values (
      v_source_id,v_publication.id,v_source_ordinal,v_source_entry->>'authority_kind',
      v_source_entry->>'source_system',v_source_entry->>'external_identity',
      v_source_entry->>'external_revision',
      private.weekly_exceptional_hex_sha256_v1(v_source_entry->>'source_document_sha256'),
      (v_source_entry->>'payload_bytes')::integer,
      (v_source_entry->>'part_count')::integer,
      nullif(v_source_entry->>'work_date','')::date,v_root.timesheet_id,
      v_family.candidate_id,v_family.contract_id,
      private.weekly_exceptional_hex_sha256_v1(v_source_entry->>'source_row_sha256')
    );
    for v_part_entry in select value from pg_catalog.jsonb_array_elements(v_source_entry->'parts')
    loop
      insert into public.weekly_exceptional_c1_source_parts(
        publication_request_id,source_record_id,source_ordinal,part_ordinal,
        payload_utf8,fragment_sha256
      ) values (
        v_publication.id,v_source_id,v_source_ordinal,
        (v_part_entry->>'part_ordinal')::integer,
        pg_catalog.decode(v_part_entry->>'payload_utf8','hex'),
        private.weekly_exceptional_hex_sha256_v1(v_part_entry->>'fragment_sha256')
      );
    end loop;
  end loop;

  for v_component_entry in select value from pg_catalog.jsonb_array_elements(v_components)
  loop
    insert into public.weekly_exceptional_c1_component_records(
      publication_request_id,component_ordinal,component_id,source_ordinal,
      source_key,component_kind,economic_key_type,economic_key_value,
      component_member_identity,segment_id,segment_key,segment_stable_key,
      work_date,reference_number,hours_day,hours_night,hours_sat,hours_sun,
      hours_bh,additional_code_raw,unit_count,unit_pay_rate,unit_charge_rate,
      expense_code,pay_ex_vat,charge_ex_vat,exclude_from_pay,
      origin,component_sha256
    ) values (
      v_publication.id,(v_component_entry->>'component_ordinal')::integer,
      (v_component_entry->>'component_id')::uuid,
      (v_component_entry->>'source_ordinal')::integer,
      v_component_entry->>'source_key',v_component_entry->>'component_kind',
      v_component_entry->>'economic_key_type',v_component_entry->>'economic_key_value',
      v_component_entry->>'component_member_identity',nullif(v_component_entry->>'segment_id',''),
      nullif(v_component_entry->>'segment_key',''),nullif(v_component_entry->>'segment_stable_key',''),
      nullif(v_component_entry->>'work_date','')::date,nullif(v_component_entry->>'reference_number',''),
      nullif(v_component_entry->>'hours_day','')::numeric,
      nullif(v_component_entry->>'hours_night','')::numeric,
      nullif(v_component_entry->>'hours_sat','')::numeric,
      nullif(v_component_entry->>'hours_sun','')::numeric,
      nullif(v_component_entry->>'hours_bh','')::numeric,
      nullif(v_component_entry->>'additional_code_raw',''),
      nullif(v_component_entry->>'unit_count','')::numeric,
      nullif(v_component_entry->>'unit_pay_rate','')::numeric,
      nullif(v_component_entry->>'unit_charge_rate','')::numeric,
      nullif(v_component_entry->>'expense_code',''),
      (v_component_entry->>'pay_ex_vat')::numeric,
      nullif(v_component_entry->>'charge_ex_vat','')::numeric,
      (v_component_entry->>'exclude_from_pay')::boolean,
      v_component_entry->>'origin',
      private.weekly_exceptional_hex_sha256_v1(v_component_entry->>'component_sha256')
    );
  end loop;

  update public.weekly_exceptional_pay_target_families
  set c1_publication_state='PENDING',bound_version=bound_version+1
  where id=v_family.id;

  v_request_body_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_STAGE_REQUEST_BODY_V1',p_request
  );
  v_before_hash:=v_run.before_state_fingerprint;
  v_after_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_STAGED_STATE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'generation_id',v_generation.id,
      'publication_request_id',v_publication.id,'request_sha256',
      pg_catalog.encode(v_c1_request_hash,'hex'),'new_bound_version',v_family.bound_version+1
    )
  );
  select coalesce(max(sequence),0)+1 into v_step_sequence
  from public.weekly_exceptional_orchestration_steps where orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(
    orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,
    after_state_fingerprint,outcome,completed_at_utc
  ) values (
    v_run.id,v_step_sequence,'STAGE_C1_REQUEST',v_idempotency_key,
    'public.weekly_exceptional_pay_stage_c1_request_v1','jsonb->jsonb',
    v_request_body_hash,v_before_hash,
    pg_catalog.jsonb_build_object(
      'publication_request_id',v_publication.id,'generation_id',v_generation.id,
      'request_sha256',pg_catalog.encode(v_c1_request_hash,'hex')
    ),v_after_hash,v_after_hash,'COMPLETE',pg_catalog.statement_timestamp()
  );

  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','STAGED','idempotent_replay',false,
    'family_id',v_family.id,'generation_id',v_generation.id,
    'approval_id',v_approval.id,'publication_request_id',v_publication.id,
    'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
    'source_count',v_expected_source_count,
    'component_count',v_expected_component_count,
    'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00'),
    'new_family_bound_version',v_family.bound_version+1,'state','READY'
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) owner to postgres;

create or replace function public.weekly_exceptional_pay_complete_c1_publication_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256','idempotency_key'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_expected_request_hash bytea;
  v_idempotency_key text;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_checkpoint public.weekly_exceptional_c1_publication_checkpoints%rowtype;
  v_result jsonb;
  v_publication_id uuid;
  v_head_revision bigint;
  v_receipt_hash bytea;
  v_request_hash bytea;
  v_target_state text;
  v_source_hash bytea;
  v_step_sequence integer;
  v_before_hash bytea;
  v_after_hash bytea;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_COMPLETE_V1' then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_expected_request_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_request_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if pg_catalog.char_length(v_idempotency_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end if;

  -- SP-12: existing admission only; no producer/version evidence.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id
  for update;
  select generation.* into strict v_generation
  from public.weekly_exceptional_pay_generations generation
  where generation.id=v_publication.generation_id
    and generation.family_id=v_publication.family_id
  for update;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_publication.family_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id
  for update;

  if v_publication.request_sha256 is distinct from v_expected_request_hash
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_publication.state='PUBLISHED' then
    if v_generation.lifecycle_state<>'PUBLISHED'
       or v_family.current_generation_id is distinct from v_generation.id
       or v_publication.c1_publication_id is null
       or v_publication.c1_head_revision is null
       or v_publication.c1_receipt_sha256 is null then
      raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_STATE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','PUBLISHED','idempotent_replay',true,
      'publication_request_id',v_publication.id,
      'family_id',v_family.id,'generation_id',v_generation.id,
      'c1_publication_id',v_publication.c1_publication_id,
      'c1_head_revision',v_publication.c1_head_revision::text,
      'state','LIVE'
    );
  end if;
  if v_publication.state<>'SUBMITTED'
     or v_generation.lifecycle_state<>'PENDING_C1'
     or v_family.c1_publication_state<>'PUBLISHING'
     or exists(
       select 1 from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
       where unknown_outcome.publication_request_id=v_publication.id
         and unknown_outcome.state='RECOVERY_REQUIRED'
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_STATE_INVALID' using errcode='55000';
  end if;

  select checkpoint.* into strict v_checkpoint
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.publication_request_id=v_publication.id
  order by checkpoint.checkpoint_sequence desc
  limit 1
  for share;
  v_result:=v_checkpoint.result_json;
  if v_checkpoint.phase<>'PUBLISH'
     or pg_catalog.upper(coalesce(v_result->>'status',''))<>'PUBLISHED'
     or pg_catalog.jsonb_typeof(v_result->'has_more')<>'boolean'
     or (v_result->>'has_more')::boolean
     or nullif(v_result->>'publication_id','') is null
     or nullif(v_result->>'head_revision','') is null
     or nullif(v_result->>'receipt_sha256','') is null
     or nullif(v_result->>'request_sha256','') is null then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end if;
  begin
    v_publication_id:=(v_result->>'publication_id')::uuid;
    v_head_revision:=(v_result->>'head_revision')::bigint;
    v_receipt_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'receipt_sha256');
    v_request_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'request_sha256');
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end;
  if v_head_revision<1
     or v_request_hash is distinct from v_publication.request_sha256
     or (v_publication.c1_operation_id is not null
         and (v_result->>'operation_id')::uuid is distinct from v_publication.c1_operation_id) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end if;

  select target_event.resulting_lifecycle_state into strict v_target_state
  from public.weekly_exceptional_pay_target_events target_event
  where target_event.family_id=v_family.id
    and target_event.financial_generation_id=v_generation.id
  order by target_event.event_sequence desc limit 1;
  if v_target_state not in (
    'PENDING_APPROVAL','PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE',
    'RECONCILED','NOT_WORKED','ACTION_REQUIRED'
  ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_TARGET_INVALID' using errcode='55000';
  end if;
  select family_event.source_proposal_hash into strict v_source_hash
  from public.weekly_exceptional_pay_family_events family_event
  where family_event.family_id=v_family.id
    and family_event.evidence_approval_id in (
      select approval.id from public.weekly_exceptional_payment_approvals approval
      where approval.pay_target_family_id=v_family.id
    )
  order by family_event.event_sequence desc limit 1;

  v_before_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_COMPLETE_BEFORE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'family_bound_version',v_family.bound_version,
      'prior_generation_id',v_family.current_generation_id,
      'new_generation_id',v_generation.id,
      'publication_request_id',v_publication.id
    )
  );
  if v_family.current_generation_id is not null
     and v_family.current_generation_id is distinct from v_generation.id then
    update public.weekly_exceptional_pay_generations
    set lifecycle_state='SUPERSEDED',superseded_by_generation_id=v_generation.id,
        superseded_at_utc=pg_catalog.statement_timestamp()
    where id=v_family.current_generation_id
      and family_id=v_family.id
      and lifecycle_state='PUBLISHED';
    if not found then
      raise exception 'WEEKLY_PROTECTED_C1_PRIOR_GENERATION_INVALID' using errcode='55000';
    end if;
  end if;

  update public.weekly_exceptional_pay_generations
  set lifecycle_state='PUBLISHED',published_at_utc=pg_catalog.statement_timestamp(),
      result_hash=v_receipt_hash
  where id=v_generation.id and lifecycle_state='PENDING_C1';
  if not found then
    raise exception 'WEEKLY_PROTECTED_C1_GENERATION_INVALID' using errcode='55000';
  end if;
  update public.weekly_exceptional_c1_publication_requests
  set state='PUBLISHED',c1_publication_id=v_publication_id,
      c1_head_revision=v_head_revision,c1_receipt_sha256=v_receipt_hash,
      typed_result_json=v_result,completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_publication.id;
  update public.weekly_exceptional_pay_target_families
  set ownership_state='TARGET_MANAGED',current_generation_id=v_generation.id,
      current_generation_number=v_generation.generation_number,
      current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
      current_source_proposal_hash=v_source_hash,
      current_lifecycle_state=v_target_state,
      c1_publication_state='LIVE',
      current_component_count=v_publication.expected_component_count,
      bound_version=bound_version+1
  where id=v_family.id;
  update public.weekly_exceptional_orchestration_runs
  set state='COMPLETE',after_state_fingerprint=v_receipt_hash,
      completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_run.id;
  insert into public.weekly_exceptional_payment_events(
    family_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key
  ) values (
    v_family.id,'PROTECTED_STATE_OBSERVED','LIVE',
    pg_catalog.jsonb_build_object(
      'generation_id',v_generation.id,
      'publication_request_id',v_publication.id,
      'c1_head_revision',v_head_revision::text
    ),v_idempotency_key||':payment-event'
  );
  v_after_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_COMPLETE_AFTER_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'generation_id',v_generation.id,
      'target_vector_hash',pg_catalog.encode(v_generation.complete_next_vector_hash,'hex'),
      'c1_publication_id',v_publication_id,'c1_head_revision',v_head_revision::text,
      'c1_receipt_sha256',pg_catalog.encode(v_receipt_hash,'hex')
    )
  );
  select coalesce(max(step.sequence),0)+1 into v_step_sequence
  from public.weekly_exceptional_orchestration_steps step
  where step.orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(
    orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,
    after_state_fingerprint,outcome,completed_at_utc
  ) values (
    v_run.id,v_step_sequence,'COMPLETE_C1_PUBLICATION',v_idempotency_key,
    'C1.weekly_source_publish_c1','sealed C1 V1',v_expected_request_hash,
    v_before_hash,v_result,v_receipt_hash,v_after_hash,'COMPLETE',
    pg_catalog.statement_timestamp()
  );
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,
    object_type,object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor_user_id,actor.display_name,actor.role,
    'weekly_exceptional_pay_target_families',v_family.id::text,
    'WEEKLY_PROTECTED_C1_PUBLISHED',
    pg_catalog.jsonb_build_object('prior_generation_id',v_family.current_generation_id),
    pg_catalog.jsonb_build_object(
      'generation_id',v_generation.id,'publication_request_id',v_publication.id,
      'c1_publication_id',v_publication_id,'c1_head_revision',v_head_revision::text,
      'component_count',v_publication.expected_component_count
    ),'Approved hours published through the existing pay route.'
  from public.tms_users actor where actor.id=v_actor_user_id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','PUBLISHED','idempotent_replay',false,
    'publication_request_id',v_publication.id,
    'family_id',v_family.id,'generation_id',v_generation.id,
    'c1_publication_id',v_publication_id,
    'c1_head_revision',v_head_revision::text,
    'state','LIVE','family_bound_version',v_family.bound_version+1
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) owner to postgres;

notify pgrst, 'reload schema';
commit;
