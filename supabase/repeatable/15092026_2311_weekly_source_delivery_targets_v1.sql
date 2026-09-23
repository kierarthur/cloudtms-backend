-- Replaceable Weekly Source communications transport.
--
-- The rendered message remains immutable.  Dispatch state is owned by one
-- durable row per exact target so acceptance by one Candidate device can never
-- close another device.  Candidate in-app notification creation is in the
-- same transaction as every accepted request/reminder intent.

create or replace function private.weekly_source_delivery_target_immutable_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if new.dispatch_command_id is distinct from old.dispatch_command_id
     or new.target_ordinal is distinct from old.target_ordinal
     or new.channel is distinct from old.channel
     or new.target_kind is distinct from old.target_kind
     or new.provider is distinct from old.provider
     or new.external_target_id is distinct from old.external_target_id
     or new.keyed_target_fingerprint is distinct from old.keyed_target_fingerprint
     or new.target_snapshot_hash is distinct from old.target_snapshot_hash
     or new.target_version is distinct from old.target_version
     or new.safe_target_snapshot_json is distinct from old.safe_target_snapshot_json
     or new.rendered_content_hash is distinct from old.rendered_content_hash
     or new.provider_idempotency_key is distinct from old.provider_idempotency_key
     or new.maximum_attempts is distinct from old.maximum_attempts
     or new.created_at_utc is distinct from old.created_at_utc then
    raise exception 'WEEKLY_SOURCE_DELIVERY_TARGET_IMMUTABLE' using errcode='55000';
  end if;
  new.updated_at_utc:=pg_catalog.transaction_timestamp();
  return new;
end;
$function$;

create or replace function public.weekly_source_message_dispatch_target_result_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_attempt_id uuid;
  v_outcome text;
  v_provider_message_id text;
  v_receipt jsonb;
  v_error jsonb;
  v_attempt public.weekly_message_target_attempts%rowtype;
  v_target public.weekly_message_dispatch_targets%rowtype;
  v_result_hash bytea;
  v_delay_seconds integer;
  v_error_code text;
  v_retirement_recorded boolean;
  v_aggregate jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'provider_attempt_id','outcome','provider_message_id',
                 'bounded_provider_receipt','bounded_error'
               )) then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_attempt_id:=(p_request->>'provider_attempt_id')::uuid;
    v_outcome:=pg_catalog.upper(pg_catalog.btrim(p_request->>'outcome'));
    v_provider_message_id:=nullif(pg_catalog.btrim(p_request->>'provider_message_id'),'');
    v_receipt:=coalesce(p_request->'bounded_provider_receipt','{}'::jsonb);
    v_error:=coalesce(p_request->'bounded_error','{}'::jsonb);
  exception when others then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_outcome not in (
       'ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS'
     )
     or pg_catalog.jsonb_typeof(v_receipt)<>'object'
     or pg_catalog.jsonb_typeof(v_error)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(v_receipt) key
               where key not in ('provider_status','provider_request_id','retirement_recorded'))
     or exists(select 1 from pg_catalog.jsonb_object_keys(v_error) key
               where key not in ('error_code','provider_status','retry_after_seconds'))
     or pg_catalog.char_length(coalesce(v_provider_message_id,''))>500
     or pg_catalog.char_length(coalesce(v_receipt->>'provider_request_id',''))>500
     or pg_catalog.char_length(coalesce(v_error->>'error_code',''))>120 then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    if v_receipt ? 'provider_status' then
      perform (v_receipt->>'provider_status')::integer;
    end if;
    if v_error ? 'provider_status' then
      perform (v_error->>'provider_status')::integer;
    end if;
    if v_error ? 'retry_after_seconds' then
      perform (v_error->>'retry_after_seconds')::integer;
    end if;
    if v_receipt ? 'retirement_recorded' then
      v_retirement_recorded:=(v_receipt->>'retirement_recorded')::boolean;
    end if;
  exception when others then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REQUEST_INVALID' using errcode='22023';
  end;
  v_error_code:=nullif(pg_catalog.upper(v_error->>'error_code'),'');
  if v_error_code is not null and v_error_code!~'^[A-Z][A-Z0-9_]{1,119}$' then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REQUEST_INVALID' using errcode='22023';
  end if;
  v_result_hash:=extensions.digest(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_attempt_id,'outcome',v_outcome,
      'provider_message_id',v_provider_message_id,
      'bounded_provider_receipt',v_receipt,'bounded_error',v_error
    )::text,'UTF8'),'sha256');

  select * into strict v_attempt from public.weekly_message_target_attempts
  where id=v_attempt_id for update;
  select * into strict v_target from public.weekly_message_dispatch_targets
  where id=v_attempt.dispatch_target_id for update;
  if v_attempt.completed_at_utc is not null then
    if v_attempt.result_hash<>v_result_hash then
      raise exception 'WEEKLY_SOURCE_TARGET_RESULT_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'target_state',v_target.state,
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_target.dispatch_command_id
    );
  end if;
  if v_target.state<>'SUBMISSION_STARTED' or v_target.attempt_count<>v_attempt.attempt_number then
    raise exception 'WEEKLY_SOURCE_TARGET_RESULT_STALE' using errcode='40001';
  end if;

  update public.weekly_message_target_attempts
  set completed_at_utc=pg_catalog.transaction_timestamp(),outcome=v_outcome,
      provider_message_id=v_provider_message_id,
      bounded_provider_receipt_json=v_receipt,bounded_error_json=v_error,
      result_hash=v_result_hash
  where id=v_attempt.id;

  if v_outcome='ACCEPTED' then
    update public.weekly_message_dispatch_targets
    set state='ACCEPTED',provider_message_id=v_provider_message_id,
        provider_accepted_at_utc=pg_catalog.transaction_timestamp(),
        terminal_at_utc=pg_catalog.transaction_timestamp(),terminal_reason='PROVIDER_ACCEPTED',
        next_attempt_at_utc=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
  elsif v_outcome='AMBIGUOUS' then
    update public.weekly_message_dispatch_targets
    set state='AMBIGUOUS',provider_message_id=v_provider_message_id,
        terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason=coalesce(v_error_code,'PROVIDER_OUTCOME_UNKNOWN'),
        next_attempt_at_utc=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    insert into public.weekly_message_delivery_failures(
      dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
    ) values (
      v_target.dispatch_command_id,v_target.id,'PROVIDER_AMBIGUOUS',
      coalesce(v_error_code,'PROVIDER_OUTCOME_UNKNOWN'),
      pg_catalog.jsonb_build_object('target_id',v_target.id,'attempt_number',v_attempt.attempt_number)
    ) on conflict do nothing;
  elsif v_outcome='TRANSIENT_FAILURE' and v_attempt.attempt_number<v_target.maximum_attempts then
    v_delay_seconds:=case v_attempt.attempt_number
      when 1 then 30 when 2 then 120 when 3 then 600 else 1800 end;
    if v_error ? 'retry_after_seconds' then
      v_delay_seconds:=greatest(
        v_delay_seconds,least((v_error->>'retry_after_seconds')::integer,3600)
      );
    end if;
    update public.weekly_message_dispatch_targets
    set state='TRANSIENT_FAILURE',next_attempt_at_utc=pg_catalog.transaction_timestamp()
          +pg_catalog.make_interval(secs=>v_delay_seconds),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
  else
    update public.weekly_message_dispatch_targets
    set state='DEFINITELY_REJECTED',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason=case when v_outcome='TRANSIENT_FAILURE'
          then 'RETRIES_EXHAUSTED' else coalesce(v_error_code,'PROVIDER_REJECTED') end,
        next_attempt_at_utc=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    if v_outcome='TRANSIENT_FAILURE' then
      insert into public.weekly_message_delivery_failures(
        dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
      ) values (
        v_target.dispatch_command_id,v_target.id,'RETRIES_EXHAUSTED','RETRIES_EXHAUSTED',
        pg_catalog.jsonb_build_object('target_id',v_target.id,'attempt_count',v_attempt.attempt_number)
      ) on conflict do nothing;
    elsif v_error_code='INVALID_TARGET' then
      insert into public.weekly_message_delivery_failures(
        dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
      ) values (
        v_target.dispatch_command_id,v_target.id,'INVALID_TARGET','INVALID_TARGET',
        pg_catalog.jsonb_build_object('target_id',v_target.id,'retirement_recorded',coalesce(v_retirement_recorded,false))
      ) on conflict do nothing;
      if not coalesce(v_retirement_recorded,false) then
        insert into public.weekly_message_delivery_failures(
          dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
        ) values (
          v_target.dispatch_command_id,v_target.id,'TARGET_RETIREMENT_FAILED',
          'TARGET_RETIREMENT_NOT_RECORDED',pg_catalog.jsonb_build_object('target_id',v_target.id)
        ) on conflict do nothing;
      end if;
    elsif v_error_code='PROVIDER_NOT_READY' then
      insert into public.weekly_message_delivery_failures(
        dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
      ) values (
        v_target.dispatch_command_id,v_target.id,'PROVIDER_CONFIGURATION','PROVIDER_NOT_READY',
        pg_catalog.jsonb_build_object('target_id',v_target.id,'provider',v_target.provider)
      ) on conflict do nothing;
    end if;
  end if;

  v_aggregate:=private.weekly_source_delivery_aggregate_command_v1(v_target.dispatch_command_id);
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'dispatch_target_id',v_target.id,
    'dispatch_command_id',v_target.dispatch_command_id,'outcome',v_outcome,
    'aggregate',v_aggregate
  );
end;
$function$;

-- The original command-level result functions can close a multi-device command
-- after one provider response.  Keep their signatures for a bounded migration
-- failure, but make the unsafe route impossible to call.
create or replace function public.weekly_source_message_dispatch_submission_start_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  perform private.weekly_source_query_require_service_v1();
  raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';
end;
$function$;

create or replace function public.weekly_source_message_dispatch_result_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  perform private.weekly_source_query_require_service_v1();
  raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';
end;
$function$;

create or replace function public.weekly_source_message_dispatch_target_claim_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_worker text;
  v_limit integer;
  v_lease_seconds integer;
  v_token uuid:=pg_catalog.gen_random_uuid();
  v_targets jsonb;
  v_stale record;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('worker_id','limit','lease_seconds')) then
    raise exception 'WEEKLY_SOURCE_TARGET_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_worker:=pg_catalog.btrim(p_request->>'worker_id');
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,50);
    v_lease_seconds:=coalesce(nullif(p_request->>'lease_seconds','')::integer,60);
  exception when others then
    raise exception 'WEEKLY_SOURCE_TARGET_CLAIM_REQUEST_INVALID' using errcode='22023';
  end;
  if v_worker is null or v_worker!~'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
     or v_limit<1 or v_limit>200 or v_lease_seconds<15 or v_lease_seconds>300 then
    raise exception 'WEEKLY_SOURCE_TARGET_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Once submission started, an expired lease is an unknown provider outcome.
  -- It is never guessed and never resubmitted automatically.
  for v_stale in
    update public.weekly_message_dispatch_targets target
    set state='AMBIGUOUS',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason='SUBMISSION_LEASE_EXPIRED',
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where target.state='SUBMISSION_STARTED'
      and target.lease_expires_at_utc<=pg_catalog.transaction_timestamp()
    returning target.id,target.dispatch_command_id
  loop
    insert into public.weekly_message_delivery_failures(
      dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
    ) values (
      v_stale.dispatch_command_id,v_stale.id,'PROVIDER_AMBIGUOUS',
      'SUBMISSION_LEASE_EXPIRED',pg_catalog.jsonb_build_object('target_id',v_stale.id)
    ) on conflict do nothing;
    perform private.weekly_source_delivery_aggregate_command_v1(v_stale.dispatch_command_id);
  end loop;

  for v_stale in
    update public.weekly_message_dispatch_targets target
    set state=case when target.state='SUBMISSION_STARTED' then 'AMBIGUOUS' else 'RETIRED' end,
        terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason=case when target.state='SUBMISSION_STARTED'
          then 'REQUEST_RETIRED_AFTER_SUBMISSION' else 'REQUEST_RETIRED' end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from public.weekly_message_dispatch_commands command,
         public.weekly_message_intents intent,
         public.weekly_message_renders render
    where target.dispatch_command_id=command.id
      and intent.id=command.message_intent_id
      and render.id=command.message_render_id
      and target.state not in (
        'ACCEPTED','DEFINITELY_REJECTED','AMBIGUOUS','RETIRED','SKIPPED'
      )
      and (intent.state='RETIRED' or render.state='STALE')
    returning target.id,target.dispatch_command_id,target.state
  loop
    if v_stale.state='AMBIGUOUS' then
      insert into public.weekly_message_delivery_failures(
        dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
      ) values (
        v_stale.dispatch_command_id,v_stale.id,'PROVIDER_AMBIGUOUS',
        'REQUEST_RETIRED_AFTER_SUBMISSION',pg_catalog.jsonb_build_object('target_id',v_stale.id)
      ) on conflict do nothing;
    end if;
    perform private.weekly_source_delivery_aggregate_command_v1(v_stale.dispatch_command_id);
  end loop;

  with eligible as (
    select target.id
    from public.weekly_message_dispatch_targets target
    where (
      (target.state in ('READY','TRANSIENT_FAILURE')
        and coalesce(target.next_attempt_at_utc,'-infinity'::timestamptz)
          <=pg_catalog.transaction_timestamp())
      or
      (target.state='LEASED'
        and target.lease_expires_at_utc<=pg_catalog.transaction_timestamp())
    )
    order by target.next_attempt_at_utc nulls first,target.id
    limit v_limit
    for update skip locked
  ), claimed as (
    update public.weekly_message_dispatch_targets target
    set state='LEASED',lease_owner=v_worker,lease_token=v_token,
        lease_expires_at_utc=pg_catalog.transaction_timestamp()
          +pg_catalog.make_interval(secs=>v_lease_seconds)
    from eligible where target.id=eligible.id
    returning target.*
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'dispatch_target_id',claimed.id,'dispatch_command_id',command.id,
      'lease_token',claimed.lease_token,'lease_expires_at_utc',claimed.lease_expires_at_utc,
      'channel',claimed.channel,'target_kind',claimed.target_kind,'provider',claimed.provider,
      'external_target_id',claimed.external_target_id,
      'target_fingerprint',pg_catalog.encode(claimed.keyed_target_fingerprint,'hex'),
      'target_snapshot_hash',pg_catalog.encode(claimed.target_snapshot_hash,'hex'),
      'target_version',claimed.target_version,
      'safe_target_snapshot',claimed.safe_target_snapshot_json,
      'provider_idempotency_key',claimed.provider_idempotency_key,
      'rendered_content_hash',pg_catalog.encode(claimed.rendered_content_hash,'hex'),
      'subject_text',render.subject_text,'html_body',render.html_body,
      'plain_body',render.plain_body,
      'push_title',case
        when command.tranche_kind like 'TIMESHEET_SUBMISSION%' then 'Please submit your Timesheet'
        when command.tranche_kind like '%REMINDER%' then 'Reminder: check your Timesheet hours'
        else 'Check your Timesheet hours' end,
      'deep_link',notification.deep_link_json,
      'manager_recipient',case when claimed.channel='EMAIL'
        then route.protected_recipient_address else null end
    ) order by claimed.id
  ),'[]'::jsonb) into v_targets
  from claimed
  join public.weekly_message_dispatch_commands command on command.id=claimed.dispatch_command_id
  join public.weekly_message_renders render on render.id=command.message_render_id
  join public.weekly_message_intents intent on intent.id=command.message_intent_id
  left join public.weekly_manager_recipient_routes route on route.id=command.recipient_route_id
  left join public.weekly_candidate_message_notifications message_notification
    on message_notification.message_intent_id=intent.id
  left join public.candidate_notifications notification
    on notification.id=message_notification.notification_id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'worker_id',v_worker,'lease_token',v_token,
    'claimed_count',pg_catalog.jsonb_array_length(v_targets),'targets',v_targets
  );
end;
$function$;

create or replace function public.weekly_source_message_dispatch_target_start_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_target_id uuid;
  v_lease_token uuid;
  v_worker text;
  v_target public.weekly_message_dispatch_targets%rowtype;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_render public.weekly_message_renders%rowtype;
  v_attempt public.weekly_message_target_attempts%rowtype;
  v_attempt_number integer;
  v_reason text;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('dispatch_target_id','lease_token','worker_id')) then
    raise exception 'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_target_id:=(p_request->>'dispatch_target_id')::uuid;
    v_lease_token:=(p_request->>'lease_token')::uuid;
    v_worker:=p_request->>'worker_id';
  exception when others then
    raise exception 'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' using errcode='22023';
  end;
  select * into strict v_target from public.weekly_message_dispatch_targets
  where id=v_target_id for update;
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_target.dispatch_command_id for update;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_command.message_intent_id;
  select * into strict v_render from public.weekly_message_renders
  where id=v_command.message_render_id;
  select * into v_attempt from public.weekly_message_target_attempts
  where dispatch_target_id=v_target.id and completed_at_utc is null
  order by attempt_number desc limit 1;
  if found and v_target.state='SUBMISSION_STARTED' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'provider_attempt_id',v_attempt.id,
      'provider_idempotency_key',v_attempt.provider_idempotency_key
    );
  end if;
  if v_target.state<>'LEASED' or v_target.lease_token<>v_lease_token
     or v_target.lease_owner<>v_worker
     or v_target.lease_expires_at_utc<=pg_catalog.transaction_timestamp()
     or v_intent.state<>'RENDERED' or v_render.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_TARGET_LEASE_STALE' using errcode='40001';
  end if;
  begin
    perform private.weekly_source_query_current_publication_v1(
      v_command.source_cycle_id,v_command.projection_publication_id
    );
  exception when others then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason='CURRENT_HOURS_CHANGED',next_attempt_at_utc=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','CURRENT_HOURS_CHANGED',
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id
    );
  end;
  if v_intent.audience_kind='CANDIDATE' and not exists(
    select 1 from public.weekly_candidate_outreach_generations generation
    join public.weekly_candidate_cohorts cohort on cohort.id=generation.candidate_cohort_id
    where generation.id=v_command.candidate_generation_id and generation.state='ACTIVE'
      and cohort.current_generation_id=generation.id
  ) then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason='CANDIDATE_GENERATION_CHANGED',next_attempt_at_utc=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','CANDIDATE_GENERATION_CHANGED',
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id
    );
  elsif v_intent.audience_kind='CANDIDATE' and not exists(
    select 1
    from public.weekly_candidate_outreach_memberships membership
    join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
    where membership.candidate_generation_id=v_command.candidate_generation_id
      and membership.state='ACTIONABLE' and incident.state='OPEN'
  ) and not exists(
    select 1
    from public.weekly_candidate_outreach_generations generation
    join public.weekly_timesheet_submission_requests submission
      on submission.candidate_cohort_id=generation.candidate_cohort_id
     and submission.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
    join public.weekly_timesheet_submission_request_memberships membership
      on membership.submission_request_id=submission.id and membership.state='WAITING'
    where generation.id=v_command.candidate_generation_id
      and generation.request_kind='SUBMIT_TIMESHEET'
  ) then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason='REQUEST_RESOLVED',next_attempt_at_utc=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','REQUEST_RESOLVED',
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id
    );
  elsif v_intent.audience_kind='MANAGER' and not exists(
    select 1
    from public.weekly_manager_recipient_generations generation
    join public.weekly_manager_recipient_routes route on route.id=generation.recipient_route_id
    join public.weekly_manager_review_batches batch
      on batch.recipient_generation_id=generation.id
     and batch.message_render_id=v_render.id and batch.state='ACTIVE'
    where generation.id=v_intent.recipient_generation_id and generation.state='ACTIVE'
      and route.current_generation_id=generation.id
  ) then
    v_reason:='REVIEW_REPLACED';
  elsif v_intent.audience_kind='MANAGER' and (
    exists(
      select 1
      from public.weekly_manager_review_batches batch
      join public.weekly_manager_review_items item on item.review_batch_id=batch.id
      join public.weekly_discrepancy_incidents incident on incident.id=item.incident_id
      join public.weekly_issue_comparison_revisions comparison
        on comparison.id=incident.current_comparison_revision_id
      where batch.message_render_id=v_render.id
        and (
          item.response_state<>'UNANSWERED' or incident.state<>'OPEN'
          or item.incident_episode<>incident.episode_number
          or item.sent_comparison_revision_id<>incident.current_comparison_revision_id
          or item.sent_comparison_fingerprint<>comparison.material_comparison_fingerprint
        )
    ) or not exists(
      select 1
      from public.weekly_manager_review_batches batch
      join public.weekly_manager_review_items item on item.review_batch_id=batch.id
      join public.weekly_discrepancy_incidents incident on incident.id=item.incident_id
      where batch.message_render_id=v_render.id
        and item.response_state='UNANSWERED' and incident.state='OPEN'
    )
  ) then
    v_reason:='REVIEW_CHANGED';
  end if;
  if v_reason is not null then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason=v_reason,next_attempt_at_utc=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    update public.weekly_manager_review_batches
    set state='REVOKED'
    where message_render_id=v_render.id and state='ACTIVE';
    update public.weekly_manager_route_receipts receipt
    set state='REVOKED',revoked_at_utc=pg_catalog.transaction_timestamp()
    from public.weekly_manager_review_batches batch
    where receipt.review_batch_id=batch.id and batch.message_render_id=v_render.id
      and receipt.state='ACTIVE';
    update public.weekly_manager_review_items item
    set response_state='OBSOLETE'
    from public.weekly_manager_review_batches batch
    where item.review_batch_id=batch.id and batch.message_render_id=v_render.id
      and item.response_state='UNANSWERED';
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason',v_reason,
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id
    );
  end if;
  select coalesce(pg_catalog.max(attempt_number),0)+1 into v_attempt_number
  from public.weekly_message_target_attempts where dispatch_target_id=v_target.id;
  if v_attempt_number>v_target.maximum_attempts then
    raise exception 'WEEKLY_SOURCE_TARGET_RETRIES_EXHAUSTED' using errcode='55000';
  end if;
  insert into public.weekly_message_target_attempts(
    dispatch_target_id,attempt_number,provider_idempotency_key,
    lease_owner,lease_token,submission_started_at_utc
  ) values (
    v_target.id,v_attempt_number,
    v_target.provider_idempotency_key||'/attempt-'||v_attempt_number::text,
    v_worker,v_lease_token,pg_catalog.transaction_timestamp()
  ) returning * into v_attempt;
  update public.weekly_message_dispatch_targets
  set state='SUBMISSION_STARTED',attempt_count=v_attempt_number
  where id=v_target.id;
  update public.weekly_message_dispatch_commands set state='SUBMISSION_STARTED'
  where id=v_command.id;
  update public.weekly_message_renders set state='SUBMISSION_STARTED'
  where id=v_render.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'provider_attempt_id',v_attempt.id,
    'provider_idempotency_key',v_attempt.provider_idempotency_key,
    'submission_started_at_utc',v_attempt.submission_started_at_utc
  );
end;
$function$;

create or replace function public.weekly_source_message_dispatch_claim_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_worker text;
  v_limit integer;
  v_lease_seconds integer;
  v_token uuid:=pg_catalog.gen_random_uuid();
  v_commands jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('worker_id','limit','lease_seconds')) then
    raise exception 'WEEKLY_SOURCE_DISPATCH_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_worker:=pg_catalog.btrim(p_request->>'worker_id');
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,25);
    v_lease_seconds:=coalesce(nullif(p_request->>'lease_seconds','')::integer,60);
  exception when others then
    raise exception 'WEEKLY_SOURCE_DISPATCH_CLAIM_REQUEST_INVALID' using errcode='22023';
  end;
  if v_worker is null or v_worker!~'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
     or v_limit<1 or v_limit>100 or v_lease_seconds<15 or v_lease_seconds>300 then
    raise exception 'WEEKLY_SOURCE_DISPATCH_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;
  with eligible as (
    select command.id
    from public.weekly_message_dispatch_commands command
    where command.target_set_state='NOT_PREPARED'
      and (
        (command.state in ('READY','FAILED')
          and coalesce(command.next_attempt_at_utc,'-infinity'::timestamptz)
            <=pg_catalog.transaction_timestamp())
        or
        (command.state='LEASED'
          and command.lease_expires_at_utc<=pg_catalog.transaction_timestamp())
      )
    order by command.next_attempt_at_utc nulls first,command.id
    limit v_limit
    for update skip locked
  ), claimed as (
    update public.weekly_message_dispatch_commands command
    set state='LEASED',lease_owner=v_worker,lease_token=v_token,
        lease_expires_at_utc=pg_catalog.transaction_timestamp()
          +pg_catalog.make_interval(secs=>v_lease_seconds)
    from eligible where command.id=eligible.id
    returning command.*
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'dispatch_command_id',claimed.id,'lease_token',claimed.lease_token,
      'lease_expires_at_utc',claimed.lease_expires_at_utc,
      'environment',claimed.environment,'agency_id',claimed.agency_id,
      'audience_kind',intent.audience_kind,'tranche_kind',claimed.tranche_kind,
      'candidate_id',candidate_generation.candidate_id,
      'candidate_generation_id',claimed.candidate_generation_id,
      'manager_recipient_route_id',claimed.recipient_route_id,
      'manager_recipient',route.protected_recipient_address,
      'manager_recipient_fingerprint',case when route.id is null then null
        else pg_catalog.encode(route.normalised_recipient_hash,'hex') end,
      'message_intent_id',intent.id,'message_render_id',render.id,
      'subject_text',render.subject_text,'html_body',render.html_body,
      'plain_body',render.plain_body,
      'provider_idempotency_key',render.provider_idempotency_key,
      'rendered_content_hash',pg_catalog.encode(render.rendered_content_hash,'hex'),
      'review_batch_id',batch.id,'control_plane_ticket_id',batch.control_plane_ticket_id
    ) order by claimed.id
  ),'[]'::jsonb) into v_commands
  from claimed
  join public.weekly_message_intents intent on intent.id=claimed.message_intent_id
  join public.weekly_message_renders render on render.id=claimed.message_render_id
  left join public.weekly_candidate_outreach_generations candidate_generation
    on candidate_generation.id=claimed.candidate_generation_id
  left join public.weekly_manager_recipient_routes route on route.id=claimed.recipient_route_id
  left join public.weekly_manager_review_batches batch on batch.message_render_id=render.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'worker_id',v_worker,'lease_token',v_token,
    'claimed_count',pg_catalog.jsonb_array_length(v_commands),'commands',v_commands
  );
end;
$function$;

create or replace function public.weekly_source_message_targets_register_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_command_id uuid;
  v_lease_token uuid;
  v_worker text;
  v_control_snapshot uuid;
  v_suppression text;
  v_targets jsonb;
  v_normalised jsonb:='[]'::jsonb;
  v_item jsonb;
  v_safe jsonb;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_render public.weekly_message_renders%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_external_id uuid;
  v_fingerprint bytea;
  v_snapshot_hash bytea;
  v_provider text;
  v_version integer;
  v_ordinal integer:=0;
  v_set_hash bytea;
  v_notification_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'dispatch_command_id','lease_token','worker_id',
                 'control_plane_snapshot_id','suppression_reason','targets'
               ))
     or pg_catalog.jsonb_typeof(coalesce(p_request->'targets','[]'::jsonb))<>'array' then
    raise exception 'WEEKLY_SOURCE_TARGET_SET_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_command_id:=(p_request->>'dispatch_command_id')::uuid;
    v_lease_token:=(p_request->>'lease_token')::uuid;
    v_worker:=p_request->>'worker_id';
    v_control_snapshot:=nullif(p_request->>'control_plane_snapshot_id','')::uuid;
    v_suppression:=nullif(pg_catalog.upper(pg_catalog.btrim(p_request->>'suppression_reason')),'');
    v_targets:=coalesce(p_request->'targets','[]'::jsonb);
  exception when others then
    raise exception 'WEEKLY_SOURCE_TARGET_SET_REQUEST_INVALID' using errcode='22023';
  end;
  if v_suppression is not null and v_suppression not in (
    'PERSONAL_PREFERENCE','NO_ACTIVE_DEVICE','PUSH_DELIVERY_UNAVAILABLE'
  ) then
    raise exception 'WEEKLY_SOURCE_TARGET_SUPPRESSION_INVALID' using errcode='22023';
  end if;
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_command_id for update;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_command.message_intent_id for update;
  select * into strict v_render from public.weekly_message_renders
  where id=v_command.message_render_id for update;
  if v_command.state<>'LEASED' or v_command.lease_token<>v_lease_token
     or v_command.lease_owner<>v_worker
     or v_command.lease_expires_at_utc<=pg_catalog.transaction_timestamp()
     or v_intent.state<>'RENDERED' or v_render.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_DISPATCH_LEASE_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_query_current_publication_v1(
    v_command.source_cycle_id,v_command.projection_publication_id
  );

  if v_suppression is not null then
    if v_intent.audience_kind<>'CANDIDATE'
       or pg_catalog.jsonb_array_length(v_targets)<>0
       or v_control_snapshot is null then
      raise exception 'WEEKLY_SOURCE_TARGET_SUPPRESSION_INVALID' using errcode='22023';
    end if;
    v_set_hash:=private.weekly_source_delivery_target_set_hash_v1(
      v_command.id,v_control_snapshot,v_suppression,'[]'::jsonb
    );
    update public.weekly_message_dispatch_commands
    set target_set_state='SUPPRESSED',target_set_hash=v_set_hash,
        control_plane_snapshot_id=v_control_snapshot,state='ACCEPTED',
        transport_finalised_at_utc=pg_catalog.transaction_timestamp(),
        terminal_reason=v_suppression,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_command.id;
    update public.weekly_message_renders set state='ACCEPTED' where id=v_render.id;
    update public.weekly_message_intents set state='DISPATCHED' where id=v_intent.id;
    select link.notification_id into strict v_notification_id
    from public.weekly_candidate_message_notifications link
    where link.message_intent_id=v_intent.id;
    update public.candidate_notifications
    set push_state='SKIPPED',last_error=v_suppression
    where id=v_notification_id;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',false,'suppressed',true,
      'reason',v_suppression,'target_count',0,
      'target_set_hash',pg_catalog.encode(v_set_hash,'hex')
    );
  end if;
  if pg_catalog.jsonb_array_length(v_targets)<1
     or pg_catalog.jsonb_array_length(v_targets)>20 then
    raise exception 'WEEKLY_SOURCE_TARGET_SET_SIZE_INVALID' using errcode='22023';
  end if;

  if v_intent.audience_kind='MANAGER' then
    if v_control_snapshot is not null or pg_catalog.jsonb_array_length(v_targets)<>1 then
      raise exception 'WEEKLY_SOURCE_MANAGER_TARGET_SET_INVALID' using errcode='22023';
    end if;
    select * into strict v_route from public.weekly_manager_recipient_routes
    where id=v_command.recipient_route_id;
  elsif v_intent.audience_kind='CANDIDATE' then
    if v_control_snapshot is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TARGET_SNAPSHOT_REQUIRED' using errcode='22023';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_TARGET_AUDIENCE_INVALID' using errcode='22023';
  end if;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(v_targets)
    order by value->>'provider',value->>'target_fingerprint',value->>'external_target_id'
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_item) key
                 where key not in (
                   'external_target_id','target_fingerprint','target_snapshot_hash',
                   'target_version','provider','safe_target_snapshot'
                 )) then
      raise exception 'WEEKLY_SOURCE_TARGET_INVALID' using errcode='22023';
    end if;
    begin
      v_external_id:=(v_item->>'external_target_id')::uuid;
      v_fingerprint:=private.weekly_source_query_hex32_v1(
        v_item->>'target_fingerprint','WEEKLY_SOURCE_TARGET_FINGERPRINT_INVALID'
      );
      v_snapshot_hash:=private.weekly_source_query_hex32_v1(
        v_item->>'target_snapshot_hash','WEEKLY_SOURCE_TARGET_SNAPSHOT_HASH_INVALID'
      );
      v_version:=(v_item->>'target_version')::integer;
      v_provider:=pg_catalog.upper(v_item->>'provider');
      v_safe:=v_item->'safe_target_snapshot';
    exception when others then
      raise exception 'WEEKLY_SOURCE_TARGET_INVALID' using errcode='22023';
    end;
    if v_version<1 or pg_catalog.jsonb_typeof(v_safe)<>'object'
       or v_safe ?| array['token','token_ciphertext','email','address','authorization','secret','credential'] then
      raise exception 'WEEKLY_SOURCE_TARGET_INVALID' using errcode='22023';
    end if;
    if v_intent.audience_kind='CANDIDATE' then
      if v_provider not in ('APNS','FCM')
         or nullif(v_safe->>'control_plane_snapshot_id','')::uuid<>v_control_snapshot
         or nullif(v_safe->>'snapshot_device_id','')::uuid<>v_external_id
         or pg_catalog.upper(v_safe->>'provider')<>v_provider
         or private.weekly_source_query_hex32_v1(
              v_safe->>'target_revision_hash','WEEKLY_SOURCE_TARGET_REVISION_HASH_INVALID'
            )<>v_snapshot_hash then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_TARGET_INVALID' using errcode='22023';
      end if;
    else
      if v_provider<>'POWER_AUTOMATE' or v_external_id<>v_route.id
         or v_fingerprint<>v_route.normalised_recipient_hash
         or v_safe<>pg_catalog.jsonb_build_object('recipient_route_id',v_route.id) then
        raise exception 'WEEKLY_SOURCE_MANAGER_TARGET_INVALID' using errcode='22023';
      end if;
    end if;
    v_ordinal:=v_ordinal+1;
    v_normalised:=v_normalised||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'external_target_id',v_external_id,
        'target_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),
        'target_snapshot_hash',pg_catalog.encode(v_snapshot_hash,'hex'),
        'target_version',v_version,'provider',v_provider,
        'safe_target_snapshot',v_safe
      )
    );
    insert into public.weekly_message_dispatch_targets(
      dispatch_command_id,target_ordinal,channel,target_kind,provider,
      external_target_id,keyed_target_fingerprint,target_snapshot_hash,
      target_version,safe_target_snapshot_json,rendered_content_hash,
      provider_idempotency_key,state,next_attempt_at_utc
    ) values (
      v_command.id,v_ordinal,
      case when v_intent.audience_kind='CANDIDATE' then 'PUSH' else 'EMAIL' end,
      case when v_intent.audience_kind='CANDIDATE' then 'CANDIDATE_DEVICE' else 'MANAGER_ADDRESS' end,
      v_provider,v_external_id,v_fingerprint,v_snapshot_hash,v_version,v_safe,
      v_render.rendered_content_hash,
      v_render.provider_idempotency_key||'/'||pg_catalog.encode(v_fingerprint,'hex'),
      'READY',pg_catalog.transaction_timestamp()
    );
  end loop;
  v_set_hash:=private.weekly_source_delivery_target_set_hash_v1(
    v_command.id,v_control_snapshot,null,v_normalised
  );
  update public.weekly_message_dispatch_commands
  set target_set_state='PREPARED',target_set_hash=v_set_hash,
      control_plane_snapshot_id=v_control_snapshot,target_count=v_ordinal,
      state='READY',next_attempt_at_utc=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
  where id=v_command.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'suppressed',false,'target_count',v_ordinal,
    'target_set_hash',pg_catalog.encode(v_set_hash,'hex')
  );
end;
$function$;

-- WP-44 F2.  A TRANSIENT failure of the Candidate push SNAPSHOT step.
--
-- The snapshot step asks the control plane which devices this Candidate has.
-- An absent binding, an unreachable control plane, a reset connection or an
-- answer this transport does not understand are all failures of the QUESTION,
-- never answers about the Candidate.  They must therefore not finalise the
-- command as `SUPPRESSED` (02 section 7.16 "opt-out or no device never changes
-- the request"; NTF-009 "provider fails temporarily -> visible bounded retry";
-- 03 section 11 "an exhausted message creates an Office-visible delivery
-- failure"), and nothing may persist a control-plane snapshot identity the
-- control plane did not give.
--
-- This owner therefore leaves `target_set_state` at `NOT_PREPARED` and
-- `control_plane_snapshot_id` untouched, returns the command to the claimable
-- `FAILED` state behind the same bounded backoff ladder the per-target retry
-- uses, and writes one Office-visible `TARGET_SNAPSHOT_FAILED` row per distinct
-- failure code so the gap is never silent.
create or replace function public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_command_id uuid;
  v_lease_token uuid;
  v_worker text;
  v_failure_code text;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_attempt integer;
  v_delay_seconds integer;
  v_next timestamptz;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'dispatch_command_id','lease_token','worker_id','failure_code'
               )) then
    raise exception 'WEEKLY_SOURCE_SNAPSHOT_FAILURE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_command_id:=(p_request->>'dispatch_command_id')::uuid;
    v_lease_token:=(p_request->>'lease_token')::uuid;
    v_worker:=p_request->>'worker_id';
    v_failure_code:=pg_catalog.upper(pg_catalog.btrim(p_request->>'failure_code'));
  exception when others then
    raise exception 'WEEKLY_SOURCE_SNAPSHOT_FAILURE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_command_id is null or v_lease_token is null
     or v_worker is null or v_worker!~'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'
     or v_failure_code is null or v_failure_code!~'^[A-Z][A-Z0-9_]{1,119}$' then
    raise exception 'WEEKLY_SOURCE_SNAPSHOT_FAILURE_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Lock BEFORE reading any guarded state.
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_command_id for update;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_command.message_intent_id for update;

  if v_command.state<>'LEASED' or v_command.lease_token<>v_lease_token
     or v_command.lease_owner<>v_worker
     or v_command.lease_expires_at_utc<=pg_catalog.transaction_timestamp() then
    raise exception 'WEEKLY_SOURCE_DISPATCH_LEASE_STALE' using errcode='40001';
  end if;
  if v_intent.audience_kind<>'CANDIDATE' then
    raise exception 'WEEKLY_SOURCE_SNAPSHOT_FAILURE_AUDIENCE_INVALID' using errcode='22023';
  end if;
  -- Fail closed.  A command whose target set has already been decided is not a
  -- snapshot failure and must never be reopened by this owner.
  if v_command.target_set_state<>'NOT_PREPARED'
     or v_command.transport_finalised_at_utc is not null then
    raise exception 'WEEKLY_SOURCE_TARGET_SET_ALREADY_DECIDED' using errcode='55000';
  end if;

  v_attempt:=v_command.attempt_count+1;
  v_delay_seconds:=case v_attempt
    when 1 then 30 when 2 then 120 when 3 then 600 else 1800 end;
  v_next:=pg_catalog.transaction_timestamp()
    +pg_catalog.make_interval(secs=>v_delay_seconds);

  update public.weekly_message_dispatch_commands
  set state='FAILED',attempt_count=v_attempt,next_attempt_at_utc=v_next,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
  where id=v_command.id;

  -- One Office-visible row per distinct failure code for this command.  The
  -- table's uniqueness treats a null `dispatch_target_id` as distinct, so the
  -- existence check is explicit rather than left to `on conflict`.
  if not exists(
    select 1 from public.weekly_message_delivery_failures failure
    where failure.dispatch_command_id=v_command.id
      and failure.dispatch_target_id is null
      and failure.failure_class='TARGET_SNAPSHOT_FAILED'
      and failure.safe_failure_code=v_failure_code
  ) then
    insert into public.weekly_message_delivery_failures(
      dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code,safe_context_json
    ) values (
      v_command.id,null,'TARGET_SNAPSHOT_FAILED',v_failure_code,
      pg_catalog.jsonb_build_object(
        'dispatch_command_id',v_command.id,'attempt_count',v_attempt)
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'deferred',true,'dispatch_command_id',v_command.id,
    'failure_code',v_failure_code,'attempt_count',v_attempt,
    'next_attempt_at_utc',v_next,'re_claimable',true,
    'target_set_state',v_command.target_set_state
  );
end;
$function$;

drop trigger if exists weekly_source_delivery_target_immutable
  on public.weekly_message_dispatch_targets;
create trigger weekly_source_delivery_target_immutable
before update on public.weekly_message_dispatch_targets
for each row execute function private.weekly_source_delivery_target_immutable_v1();

create or replace function private.weekly_source_candidate_notification_intent_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_link public.candidate_app_global_membership_links%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_notification public.candidate_notifications%rowtype;
  v_link_count integer;
  v_request_kind text;
  v_event_type text;
  v_template_key text;
  v_dedupe_key text;
  v_client_name text;
  v_timesheet_count integer;
  v_week_ending date;
begin
  if new.audience_kind<>'CANDIDATE' then return new; end if;
  if new.tranche_kind not in (
    'CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
    'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H'
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_NOTIFICATION_TRANCHE_INVALID' using errcode='22023';
  end if;
  select * into strict v_generation
  from public.weekly_candidate_outreach_generations
  where id=new.candidate_generation_id;

  -- T0 is accepted only when exactly one active Candidate App account can own
  -- the durable in-app request.  Later reminders retain that same owner even
  -- if the central membership is subsequently disabled; this preserves the
  -- request record without re-routing it to another account.
  if new.tranche_kind in ('CANDIDATE_INITIAL','TIMESHEET_SUBMISSION_INITIAL') then
    select pg_catalog.count(*) into v_link_count
    from public.candidate_app_global_membership_links link
    join public.candidate_app_accounts account on account.id=link.account_id
    where link.candidate_id=v_generation.candidate_id
      and link.state='ACTIVE' and account.status='ACTIVE';
    if v_link_count<>1 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE' using errcode='55000';
    end if;
    select link.* into strict v_link
    from public.candidate_app_global_membership_links link
    join public.candidate_app_accounts account on account.id=link.account_id
    where link.candidate_id=v_generation.candidate_id
      and link.state='ACTIVE' and account.status='ACTIVE';
  else
    select link.* into v_link
    from public.weekly_candidate_message_notifications prior
    join public.candidate_app_global_membership_links link
      on link.membership_id=prior.membership_id and link.account_id=prior.account_id
    where prior.candidate_generation_id=v_generation.id
    order by prior.created_at_utc,prior.message_intent_id
    limit 1;
    if not found then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE' using errcode='55000';
    end if;
  end if;
  select * into strict v_account from public.candidate_app_accounts where id=v_link.account_id;

  v_request_kind:=v_generation.request_kind;
  v_event_type:=case
    when new.tranche_kind in ('CANDIDATE_INITIAL','TIMESHEET_SUBMISSION_INITIAL')
      then 'WEEKLY_SOURCE_REQUEST'
    else 'WEEKLY_SOURCE_REMINDER' end;
  v_template_key:=case when v_request_kind='SUBMIT_TIMESHEET'
    then 'weekly-source-submit-timesheet-v1'
    else 'weekly-source-check-hours-v1' end;
  v_dedupe_key:='weekly-source-message-intent:'||new.id::text;

  if v_request_kind='SUBMIT_TIMESHEET' then
    select client.name into strict v_client_name
    from public.clients client
    where client.id=v_generation.client_id;

    select pg_catalog.count(*)::integer,pg_catalog.min(membership.week_ending)
      into v_timesheet_count,v_week_ending
    from public.weekly_timesheet_submission_requests submission
    join public.weekly_timesheet_submission_request_memberships membership
      on membership.submission_request_id=submission.id
    where submission.candidate_cohort_id=v_generation.candidate_cohort_id
      and submission.source_cycle_id=v_generation.source_cycle_id
      and submission.candidate_id=v_generation.candidate_id
      and submission.request_generation=v_generation.generation_number
      and submission.state not in ('SUPERSEDED','CANCELLED')
      and membership.state='WAITING';
    if coalesce(v_timesheet_count,0)<1 or v_week_ending is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMISSION_SCOPE_NOT_FOUND'
        using errcode='55000';
    end if;
  end if;

  insert into public.candidate_notifications(
    account_id,candidate_id,workflow_id,timesheet_id,event_type,
    preference_category,template_key,template_params,deep_link_json,
    state,push_state,dedupe_key,created_at_utc
  ) values (
    v_account.id,v_generation.candidate_id,null,null,v_event_type,
    'timesheet_expense_attention',v_template_key,
    pg_catalog.jsonb_build_object(
      'request_id',v_generation.id,
      'request_kind',v_request_kind,
      'tranche_kind',new.tranche_kind,
      'due_at_utc',new.due_at_utc,
      'deadline_at_utc',v_generation.deadline_at_utc
    )||case when v_request_kind='SUBMIT_TIMESHEET' then
      pg_catalog.jsonb_build_object(
        'timesheet_count',v_timesheet_count,
        'week_ending_label',pg_catalog.to_char(v_week_ending,'FMDD FMMonth YYYY'),
        'client_name',v_client_name
      ) else '{}'::jsonb end,
    pg_catalog.jsonb_build_object(
      'destination','WEEKLY_SOURCE_REQUEST','request_id',v_generation.id
    ),
    'UNREAD','PENDING',v_dedupe_key,pg_catalog.transaction_timestamp()
  )
  on conflict (dedupe_key) do update set dedupe_key=excluded.dedupe_key
  returning * into v_notification;

  insert into public.weekly_candidate_message_notifications(
    message_intent_id,candidate_generation_id,candidate_id,account_id,
    membership_id,notification_id,tranche_kind,dedupe_key
  ) values (
    new.id,v_generation.id,v_generation.candidate_id,v_account.id,
    v_link.membership_id,v_notification.id,new.tranche_kind,v_dedupe_key
  );
  return new;
end;
$function$;

drop trigger if exists weekly_source_candidate_notification_intent
  on public.weekly_message_intents;
create trigger weekly_source_candidate_notification_intent
after insert on public.weekly_message_intents
for each row execute function private.weekly_source_candidate_notification_intent_v1();

create or replace function private.weekly_source_candidate_notification_retire_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if new.audience_kind='CANDIDATE' and new.state='RETIRED' and old.state<>'RETIRED' then
    update public.candidate_notifications notification
    set state=case when notification.state='UNREAD' then 'DISMISSED' else notification.state end,
        dismissed_at_utc=case when notification.state='UNREAD'
          then pg_catalog.transaction_timestamp() else notification.dismissed_at_utc end,
        push_state=case when notification.push_state in ('PENDING','CLAIMED','FAILED')
          then 'SKIPPED' else notification.push_state end,
        last_error=case when notification.push_state in ('PENDING','CLAIMED','FAILED')
          then 'REQUEST_RETIRED' else notification.last_error end,
        deep_link_json=notification.deep_link_json||pg_catalog.jsonb_build_object('obsolete',true)
    from public.weekly_candidate_message_notifications link
    where link.message_intent_id=new.id and notification.id=link.notification_id;
    update public.weekly_candidate_message_notifications
    set retired_at_utc=coalesce(retired_at_utc,pg_catalog.transaction_timestamp())
    where message_intent_id=new.id;
  end if;
  return new;
end;
$function$;

drop trigger if exists weekly_source_candidate_notification_retire
  on public.weekly_message_intents;
create trigger weekly_source_candidate_notification_retire
after update of state on public.weekly_message_intents
for each row execute function private.weekly_source_candidate_notification_retire_v1();

create or replace function private.weekly_source_delivery_target_set_hash_v1(
  p_command_id uuid,
  p_control_plane_snapshot_id uuid,
  p_suppression_reason text,
  p_targets jsonb
) returns bytea
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select extensions.digest(
    pg_catalog.convert_to(
      pg_catalog.jsonb_build_object(
        'dispatch_command_id',p_command_id,
        'control_plane_snapshot_id',p_control_plane_snapshot_id,
        'suppression_reason',p_suppression_reason,
        'targets',coalesce(p_targets,'[]'::jsonb)
      )::text,'UTF8'
    ),'sha256'
  )
$function$;

create or replace function private.weekly_source_delivery_aggregate_command_v1(
  p_dispatch_command_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_total integer;
  v_terminal integer;
  v_accepted integer;
  v_ambiguous integer;
  v_retired integer;
  v_notification_id uuid;
begin
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=p_dispatch_command_id for update;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_command.message_intent_id for update;
  select pg_catalog.count(*),
         pg_catalog.count(*) filter(where target.state in (
           'ACCEPTED','DEFINITELY_REJECTED','AMBIGUOUS','RETIRED','SKIPPED'
         )),
         pg_catalog.count(*) filter(where target.state='ACCEPTED'),
         pg_catalog.count(*) filter(where target.state='AMBIGUOUS'),
         pg_catalog.count(*) filter(where target.state='RETIRED')
  into v_total,v_terminal,v_accepted,v_ambiguous,v_retired
  from public.weekly_message_dispatch_targets target
  where target.dispatch_command_id=v_command.id;
  if v_total=0 then
    return pg_catalog.jsonb_build_object('ok',true,'terminal',false,'target_count',0);
  end if;

  update public.weekly_message_dispatch_commands
  set target_count=v_total,terminal_target_count=v_terminal,
      accepted_target_count=v_accepted,
      state=case
        when v_terminal<v_total then 'READY'
        when v_retired=v_total then 'RETIRED'
        when v_ambiguous>0 then 'AMBIGUOUS'
        when v_intent.audience_kind='MANAGER' and v_accepted=0 then 'FAILED'
        else 'ACCEPTED' end,
      target_set_state=case when v_terminal=v_total then 'TERMINAL' else 'PREPARED' end,
      transport_finalised_at_utc=case when v_terminal=v_total
        then coalesce(transport_finalised_at_utc,pg_catalog.transaction_timestamp())
        else null end,
      terminal_reason=case
        when v_terminal<v_total then null
        when v_retired=v_total then 'REQUEST_RETIRED'
        when v_ambiguous>0 then 'AMBIGUOUS_PROVIDER_OUTCOME'
        when v_intent.audience_kind='MANAGER' and v_accepted=0 then 'NO_EMAIL_TARGET_ACCEPTED'
        when v_accepted=0 then 'PUSH_NOT_ACCEPTED_IN_APP_AVAILABLE'
        else 'DELIVERY_COMPLETE' end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
  where id=v_command.id;

  if v_terminal=v_total then
    update public.weekly_message_renders
    set state=case
      when v_retired=v_total then 'STALE'
      when v_ambiguous>0 then 'AMBIGUOUS'
      when v_intent.audience_kind='MANAGER' and v_accepted=0 then 'DEFINITELY_REJECTED'
      else 'ACCEPTED' end
    where id=v_command.message_render_id;
    update public.weekly_message_intents
    set state=case
      when v_retired=v_total then 'RETIRED'
      when v_intent.audience_kind='CANDIDATE' or v_accepted>0 then 'DISPATCHED'
      else 'RETIRED' end
    where id=v_intent.id;

    if v_intent.audience_kind='CANDIDATE' then
      select link.notification_id into strict v_notification_id
      from public.weekly_candidate_message_notifications link
      where link.message_intent_id=v_intent.id;
      update public.candidate_notifications
      set push_state=case when v_accepted>0 then 'SENT' else 'FAILED' end,
          push_sent_at_utc=case when v_accepted>0
            then pg_catalog.transaction_timestamp() else push_sent_at_utc end,
          push_failed_at_utc=case when v_accepted=0
            then pg_catalog.transaction_timestamp() else null end,
          last_error=case when v_accepted=0 then
            case when v_ambiguous>0 then 'PUSH_OUTCOME_UNKNOWN' else 'NO_PUSH_TARGET_ACCEPTED' end
            else null end
      where id=v_notification_id;
    elsif v_intent.audience_kind='MANAGER' and v_accepted>0 then
      update public.weekly_discrepancy_incidents incident
      set manager_action_state='SENT'
      from public.weekly_manager_review_items item
      join public.weekly_manager_review_batches batch on batch.id=item.review_batch_id
      where batch.message_render_id=v_command.message_render_id
        and item.incident_id=incident.id and item.response_state='UNANSWERED'
        and incident.manager_action_state='DUE';
    end if;
  else
    -- A completed target must release the immutable render for the next target.
    -- No target is allowed to close, or strand, its siblings.
    update public.weekly_message_renders
    set state='CURRENT'
    where id=v_command.message_render_id and state='SUBMISSION_STARTED';
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'terminal',v_terminal=v_total,'target_count',v_total,
    'terminal_target_count',v_terminal,'accepted_target_count',v_accepted,
    'ambiguous_target_count',v_ambiguous
  );
end;
$function$;

revoke all on function private.weekly_source_candidate_notification_intent_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_notification_retire_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_delivery_aggregate_command_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_delivery_target_immutable_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_delivery_target_set_hash_v1(uuid,uuid,text,jsonb)
  from public,anon,authenticated,service_role;

revoke all on function public.weekly_source_message_dispatch_claim_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_targets_register_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_target_claim_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_result_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb)
  from public,anon,authenticated;

grant execute on function public.weekly_source_message_dispatch_claim_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_targets_register_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_target_claim_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb) to service_role;

comment on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) is
  'Persists SUBMISSION_STARTED for one immutable Weekly Source provider target before any provider call.';
comment on function public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb) is
  'Records the bounded outcome for one Weekly Source provider target. One accepted target never closes sibling targets.';
comment on function public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb) is
  'Records a TRANSIENT Candidate push snapshot failure. The command stays NOT_PREPARED and re-claimable behind a bounded backoff, an Office-visible TARGET_SNAPSHOT_FAILED row is written, and no control-plane snapshot identity is invented. Service-only.';

create or replace function public.weekly_source_message_render_due_list_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_limit integer;
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('limit')) then
    raise exception 'WEEKLY_SOURCE_RENDER_DUE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,50);
  exception when others then
    raise exception 'WEEKLY_SOURCE_RENDER_DUE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_limit<1 or v_limit>200 then
    raise exception 'WEEKLY_SOURCE_RENDER_DUE_REQUEST_INVALID' using errcode='22023';
  end if;
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'message_intent_id',due.id,
      'projection_publication_id',due.current_projection_publication_id,
      'audience_kind',due.audience_kind,
      'tranche_kind',due.tranche_kind,
      'due_at_utc',due.due_at_utc
    ) order by due.due_at_utc,due.id
  ),'[]'::jsonb) into v_rows
  from (
    select intent.id,cycle.current_projection_publication_id,
           intent.audience_kind,intent.tranche_kind,intent.due_at_utc
    from public.weekly_message_intents intent
    join public.weekly_source_cycles cycle on cycle.id=intent.source_cycle_id
    join public.weekly_source_projection_publications publication
      on publication.id=cycle.current_projection_publication_id
     and publication.source_cycle_id=cycle.id and publication.state='CURRENT'
    where intent.state='DUE'
      and intent.audience_kind in ('CANDIDATE','MANAGER')
      and intent.due_at_utc<=pg_catalog.transaction_timestamp()
    order by intent.due_at_utc,intent.id
    limit v_limit
  ) due;
  return pg_catalog.jsonb_build_object(
    'ok',true,'due_count',pg_catalog.jsonb_array_length(v_rows),'intents',v_rows
  );
end;
$function$;

revoke all on function public.weekly_source_message_render_due_list_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_message_render_due_list_v1(jsonb)
  to service_role;

comment on function public.weekly_source_message_render_due_list_v1(jsonb) is
  'Lists bounded due Weekly Source candidate and manager intents for the single-consumer delivery renderer.';

notify pgrst, 'reload schema';
