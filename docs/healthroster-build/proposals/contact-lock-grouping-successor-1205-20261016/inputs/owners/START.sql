create or replace function private.weekly_source_submission_request_start_v1(
  p_request jsonb,
  p_automatic boolean
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_contact_pair record;
  v_contact_group public.weekly_source_groups%rowtype;
  v_contact_guard jsonb;
  v_contact_now timestamptz;
  v_contact_owners uuid[];
  v_cycle_id uuid;
  v_publication_id uuid;
  v_candidate_id uuid;
  v_guard jsonb;
  v_scope jsonb;
  v_scopes jsonb;
  v_hash bytea;
  v_settings public.weekly_source_global_settings%rowtype;
  v_first_contract_id uuid;
  v_first_client_id uuid;
  v_first_work_date date;
  v_context jsonb;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_activation public.weekly_route_activations%rowtype;
  v_old_request public.weekly_timesheet_submission_requests%rowtype;
  v_old_generation public.weekly_candidate_outreach_generations%rowtype;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_generation_number integer;
  v_ordinal integer:=0;
  v_intent_id uuid;
  v_contract public.contracts%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in (
         'actor_user_id','source_cycle_id','projection_publication_id','candidate_id','scopes'
       )
     ) or pg_catalog.jsonb_typeof(p_request->'scopes')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'scopes')=0 then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_REQUEST_INVALID' using errcode='22023';
  end;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  if not p_automatic then
    perform private.weekly_source_office_authority_v1(
      v_actor,'ASK_CANDIDATES',(v_guard->>'source_group_id')::uuid,null,
      (v_guard->>'finalisation_week_ending')::date
    );
  end if;
  v_scopes:=coalesce((
    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'week_ending',value->>'week_ending','client_id',value->>'client_id',
        'contract_id',value->>'contract_id',
        'expected_source_fingerprint',value->>'expected_source_fingerprint'
      ) order by (value->>'week_ending')::date,(value->>'client_id')::uuid,(value->>'contract_id')::uuid
    )
    from pg_catalog.jsonb_array_elements(p_request->'scopes')
  ),'[]'::jsonb);
  if pg_catalog.jsonb_array_length(v_scopes)<>pg_catalog.jsonb_array_length(p_request->'scopes') then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_SCOPE_INVALID' using errcode='22023';
  end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_TIMESHEET_SUBMISSION_SCOPE_V1',v_scopes);
  for v_scope in select value from pg_catalog.jsonb_array_elements(v_scopes)
  loop
    if exists(
      select 1 from pg_catalog.jsonb_object_keys(v_scope) key
      where key not in ('week_ending','client_id','contract_id','expected_source_fingerprint')
    ) then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_SCOPE_INVALID' using errcode='22023';
    end if;
    select * into strict v_contract from public.contracts
    where id=(v_scope->>'contract_id')::uuid
      and client_id=(v_scope->>'client_id')::uuid
      and candidate_id=v_candidate_id;
    perform private.weekly_source_query_hex32_v1(
      v_scope->>'expected_source_fingerprint','WEEKLY_SOURCE_SCOPE_FINGERPRINT_INVALID'
    );
    if exists(
      select 1 from public.timesheets t
      where t.contract_id=v_contract.id and t.week_ending_date=(v_scope->>'week_ending')::date
        and t.is_current and t.revoked_at is null and t.archived_at_utc is null
        and t.sheet_scope='WEEKLY' and t.line_type='HOURS'
        and t.r2_nurse_key is not null and t.img_sha256_nurse is not null
    ) then
      raise exception 'WEEKLY_SOURCE_SIGNED_TIMESHEET_ALREADY_EXISTS' using errcode='55000';
    end if;
    if v_first_contract_id is null then
      v_first_contract_id:=v_contract.id;
      v_first_client_id:=v_contract.client_id;
      v_first_work_date:=(v_scope->>'week_ending')::date;
    end if;
    if coalesce((private._weekly_source_effective_policy_v1(
      v_contract.client_id,v_contract.id,(v_scope->>'week_ending')::date
    )->>'candidate_queries_enabled')::boolean,false) is not true then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERIES_DISABLED' using errcode='55000';
    end if;
  end loop;
  -- Same current deployment and sorted scope protocol before the native
  -- cohort/request/generation locks. Scope operands are the validated real
  -- Candidate/Client/Contract request members above, never a fake client.
  perform private.weekly_contact_source_submission_lock_v1(v_cycle_id,v_candidate_id,v_scopes,v_publication_id);
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  select * into strict v_contact_group from public.weekly_source_groups where id=(v_guard->>'source_group_id')::uuid;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-import-outreach:'||v_cycle_id::text||':'||v_candidate_id::text,0));
  select array_agg(distinct request.id order by request.id) into v_contact_owners
  from public.weekly_timesheet_submission_requests request
  join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
  join jsonb_array_elements(v_scopes)scope on(member.client_id,member.contract_id,member.week_ending)=
    ((scope->>'client_id')::uuid,(scope->>'contract_id')::uuid,(scope->>'week_ending')::date)
  where request.source_cycle_id=v_cycle_id and request.candidate_id=v_candidate_id
    and request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED');
  if cardinality(v_contact_owners)>1 then
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  if cardinality(v_contact_owners)=1 then
    select * into strict v_old_request from public.weekly_timesheet_submission_requests where id=v_contact_owners[1] for update;
    perform private.weekly_contact_submission_request_assert_v1(v_old_request.id);
    if exists(select 1 from jsonb_array_elements(v_scopes)scope where not exists(
      select 1 from public.weekly_timesheet_submission_request_memberships member
      where member.submission_request_id=v_old_request.id and(member.client_id,member.contract_id,member.week_ending)=
        ((scope->>'client_id')::uuid,(scope->>'contract_id')::uuid,(scope->>'week_ending')::date)
        and(member.expected_source_fingerprint=private.weekly_source_query_hex32_v1(
          scope->>'expected_source_fingerprint','WEEKLY_SOURCE_SCOPE_FINGERPRINT_INVALID')
          or private.weekly_source_missing_scope_unchanged_v1(v_old_request.current_projection_publication_id,v_publication_id,
            v_candidate_id,member.client_id,member.contract_id,member.week_ending)))) then
      raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
    select * into strict v_old_generation from public.weekly_candidate_outreach_generations generation
      where generation.candidate_cohort_id=v_old_request.candidate_cohort_id and generation.request_kind='SUBMIT_TIMESHEET'
        and generation.generation_number=v_old_request.request_generation;
    -- A B-only lookup may resolve the genuine legacy A-owned mixed request.
    -- Return that actual original generation and clock, never B's pointer.
    return pg_catalog.jsonb_build_object('ok',true,'status','UNCHANGED','submission_request_id',v_old_request.id,
      'candidate_generation_id',v_old_generation.id,'started_at_utc',v_old_request.started_at_utc,
      'reminder_due_at_utc',v_old_request.reminder_due_at_utc,'deadline_at_utc',v_old_request.deadline_at_utc);
  end if;
  -- Public operation is atomic across its exact original scope set. The
  -- private partition caller handles independent Client exclusions instead.
  for v_contact_pair in select distinct (scope->>'client_id')::uuid client_id from jsonb_array_elements(v_scopes)scope
    order by (scope->>'client_id')::uuid loop
    v_contact_guard:=private.weekly_contact_scope_guard_v1(v_candidate_id,v_contact_pair.client_id,'WORKER',clock_timestamp());
    if v_contact_guard->'allow' is distinct from 'true'::jsonb then
      raise exception 'CONTACT_PAUSED' using errcode='55000';
    end if;
  end loop;
  v_context:=private.weekly_source_query_cohort_ensure_v1(v_cycle_id,v_candidate_id,v_first_client_id,v_first_contract_id,v_first_work_date);
  select * into strict v_cohort from public.weekly_candidate_cohorts where id=(v_context->>'cohort_id')::uuid for update;
  if exists(select 1 from public.weekly_timesheet_submission_requests where candidate_cohort_id=v_cohort.id
    and state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')) then
    -- Do not append a new week to an already sent link or restart its clock.
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  if exists(select 1 from public.weekly_timesheet_submission_requests request
    join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
    where request.source_cycle_id=v_cycle_id and request.candidate_id=v_candidate_id
      and request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
      and exists(select 1 from jsonb_array_elements(v_scopes)scope where member.client_id=(scope->>'client_id')::uuid)) then
    -- A legacy A-owned mixed question may already own B's older week. Its
    -- cohort identity cannot authorize a fresh B question for a new week.
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  v_contact_now:=clock_timestamp();
  insert into public.weekly_route_activations(
    source_cycle_id,candidate_id,client_id,audience_route,route_mode,
    activated_by_user_id,activated_at_utc,updated_at_utc
  ) values (
    v_cycle_id,v_candidate_id,v_first_client_id,'CANDIDATE','CANDIDATE_FIRST',
    v_actor,v_contact_now,v_contact_now
  ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
  do update set route_mode='CANDIDATE_FIRST',
    activated_by_user_id=coalesce(public.weekly_route_activations.activated_by_user_id,excluded.activated_by_user_id),
    activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
    updated_at_utc=excluded.updated_at_utc
  returning * into v_activation;
  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  select coalesce(pg_catalog.max(generation_number),0)+1 into v_generation_number
  from public.weekly_candidate_outreach_generations where candidate_cohort_id=v_cohort.id;
  insert into public.weekly_candidate_outreach_generations(
    source_cycle_id,candidate_cohort_id,candidate_id,client_id,generation_number,
    activation_id,trigger_kind,request_kind,route_mode,started_at_utc,
    reminder_due_at_utc,deadline_at_utc,manual_reminder_available_at_utc,state,membership_hash
  ) values (
    v_cycle_id,v_cohort.id,v_candidate_id,v_first_client_id,v_generation_number,
    v_activation.id,case when p_automatic then 'NEW_INCIDENT' else 'OFFICE_ASK' end,
    'SUBMIT_TIMESHEET','CANDIDATE_FIRST',
    v_contact_now,
    v_contact_now+v_settings.candidate_reminder_after,
    v_contact_now+v_settings.candidate_response_deadline_after,
    v_contact_now+v_settings.candidate_manual_reminder_cooldown,
    'ACTIVE',v_hash
  ) returning * into v_generation;
  update public.weekly_candidate_cohorts set current_submission_generation_id=v_generation.id where id=v_cohort.id;
  insert into public.weekly_timesheet_submission_requests(
    environment,agency_id,source_cycle_id,candidate_id,candidate_cohort_id,
    request_generation,current_upload_id,current_projection_publication_id,state,
    started_at_utc,reminder_due_at_utc,deadline_at_utc,membership_hash
  )
  select source_group.environment,source_group.agency_id,v_cycle_id,v_candidate_id,v_cohort.id,
    v_generation.generation_number,
    (v_guard->>'upload_id')::uuid,v_publication_id,'ACTIVE',v_generation.started_at_utc,
    v_generation.reminder_due_at_utc,v_generation.deadline_at_utc,v_hash
  from public.weekly_source_cycles cycle
  join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
  where cycle.id=v_cycle_id
  returning * into v_submission;
  for v_scope in
    select value from pg_catalog.jsonb_array_elements(v_scopes)
    order by (value->>'week_ending')::date,(value->>'client_id')::uuid,(value->>'contract_id')::uuid
  loop
    v_ordinal:=v_ordinal+1;
    insert into public.weekly_timesheet_submission_request_memberships(
      submission_request_id,ordinal,week_ending,client_id,contract_id,
      expected_source_fingerprint,state
    ) values (
      v_submission.id,v_ordinal,(v_scope->>'week_ending')::date,
      (v_scope->>'client_id')::uuid,(v_scope->>'contract_id')::uuid,
      private.weekly_source_query_hex32_v1(
        v_scope->>'expected_source_fingerprint','WEEKLY_SOURCE_SCOPE_FINGERPRINT_INVALID'
      ),'WAITING'
    );
  end loop;
  v_intent_id:=private.weekly_source_query_candidate_intent_v1(
    v_generation.id,'TIMESHEET_SUBMISSION_INITIAL',1,v_generation.started_at_utc
  );
  perform public._audit_insert(
    'weekly_timesheet_submission_request',v_submission.id::text,
    'WEEKLY_TIMESHEET_SUBMISSION_REQUESTED',null,
    pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'source_cycle_id',v_cycle_id,
      'scope_count',pg_catalog.jsonb_array_length(v_scopes),
      'started_at_utc',v_submission.started_at_utc,
      'reminder_due_at_utc',v_submission.reminder_due_at_utc,
      'deadline_at_utc',v_submission.deadline_at_utc
    ),case when p_automatic then 'Candidate automatically asked to submit a Weekly Timesheet after import'
      else 'Candidate asked to submit a Weekly Timesheet' end,v_actor
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','CREATED','submission_request_id',v_submission.id,
    'candidate_generation_id',v_generation.id,'message_intent_id',v_intent_id,
    'started_at_utc',v_submission.started_at_utc,
    'reminder_due_at_utc',v_submission.reminder_due_at_utc,
    'deadline_at_utc',v_submission.deadline_at_utc,
    'scope_count',pg_catalog.jsonb_array_length(v_scopes)
  );
end;
$function$;