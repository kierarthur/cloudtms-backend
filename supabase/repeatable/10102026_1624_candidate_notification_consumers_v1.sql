-- Complete current definitions, mechanically derived from their reviewed owners.
create or replace function private._candidate_home_summary_v1(
  p_environment text,
  p_account_id uuid,
  p_candidate_id uuid,
  p_daily_capability jsonb,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_settings public.settings_defaults%rowtype;
  v_unread_count integer:=0;
  v_timesheet_attention_count integer:=0;
  v_draft_timesheet_count integer:=0;
  v_draft_expense_count integer:=0;
  v_draft_timesheet_record_ids jsonb:='[]'::jsonb;
  v_draft_expense_record_ids jsonb:='[]'::jsonb;
  v_next_shift jsonb:=null;
begin
  select * into strict v_settings from public.settings_defaults s where s.id=1;
  if p_account_id is not null then
    select pg_catalog.count(*)::integer into v_unread_count
    from public.candidate_notifications n
    where n.account_id=p_account_id and n.candidate_id=p_candidate_id and n.state='UNREAD'
      and private.candidate_notification_visible_v1(n,p_now_utc);
  end if;

  if p_candidate_id is not null then
    select
      pg_catalog.count(distinct workflow.id) filter (
        where workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
      )::integer,
      pg_catalog.count(distinct workflow.id) filter (
        where workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      )::integer,
      coalesce(
        pg_catalog.jsonb_agg(distinct draft_record.record_id_text order by draft_record.record_id_text)
          filter (
            where workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
              and draft_record.record_id_text is not null
          ),
        '[]'::jsonb
      ),
      coalesce(
        pg_catalog.jsonb_agg(distinct draft_record.record_id_text order by draft_record.record_id_text)
          filter (
            where workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
              and draft_record.record_id_text is not null
          ),
        '[]'::jsonb
      )
    into
      v_draft_timesheet_count,
      v_draft_expense_count,
      v_draft_timesheet_record_ids,
      v_draft_expense_record_ids
    from public.candidate_submission_workflows workflow
    left join lateral (
      values
        (workflow.contract_week_id::text),
        (workflow.anchor_timesheet_id::text),
        (workflow.target_timesheet_id::text)
    ) as draft_record(record_id_text) on true
    where workflow.candidate_id=p_candidate_id
      and workflow.state in ('CREATED','WORKER_DRAFT')
      and exists(
        select 1
        from public.candidate_submission_components component
        where component.workflow_id=workflow.id
          and component.workflow_generation=workflow.generation
          and component.superseded_at_utc is null
          and component.component_kind in (
            'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
          )
      );

    select pg_catalog.count(*)::integer into v_timesheet_attention_count
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id and c.candidate_id=p_candidate_id
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      and t.is_current=true and t.archived_at_utc is null
    left join lateral (
      select f.* from public.timesheets_financials f
      where f.timesheet_id=t.timesheet_id and f.is_current=true
      order by f.computed_at_utc desc nulls last,f.updated_at desc,f.id desc limit 1
    ) tf on true
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id=c.client_id
        and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (p_now_utc at time zone 'Europe/London')::date
        +pg_catalog.mod(
          coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -pg_catalog.date_part('dow',(p_now_utc at time zone 'Europe/London')::date)::integer+7,
          7
        )
      )::date as current_week_ending_date
    ) current_window
    cross join lateral (
      select private._candidate_record_capabilities_v1(t.timesheet_id,cw.id,'{}'::jsonb) as value
    ) capability
    cross join lateral (
      select
        exists (
          select 1
          from public.candidate_submission_workflows workflow
          where workflow.candidate_id=p_candidate_id
            and workflow.state='REJECTED'
            and private._candidate_workflow_maps_to_card_v1(workflow.id,t.timesheet_id,cw.id)
            and not private._candidate_rejection_replaced_v1(workflow.id)
        ) as has_actionable_rejection,
        exists (
          select 1
          from public.candidate_submission_workflows workflow
          where workflow.candidate_id=p_candidate_id
            and workflow.state in ('CREATED','WORKER_DRAFT','AWAITING_PAPER_RETURN','REFUSED')
            and private._candidate_workflow_maps_to_card_v1(workflow.id,t.timesheet_id,cw.id)
        ) as has_candidate_action,
        exists (
          select 1
          from public.candidate_submission_workflows workflow
          where workflow.candidate_id=p_candidate_id
            and workflow.state in (
              'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
              'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
              'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
              'READY_TO_FINALISE','RECEIVED','FINALISED'
            )
            and private._candidate_workflow_maps_to_card_v1(workflow.id,t.timesheet_id,cw.id)
        ) as has_submitted_or_processed_workflow
    ) workflow_state
    where cw.week_ending_date<=current_window.current_week_ending_date
      and tf.paid_at_utc is null
      and coalesce(capability.value->>'record_role','')<>'EXPENSE_ONLY'
      and (
        workflow_state.has_actionable_rejection
        or workflow_state.has_candidate_action
        or (
          not workflow_state.has_submitted_or_processed_workflow
          and (
            coalesce((capability.value->>'can_edit_hours')::boolean,false)
            or coalesce((capability.value->>'can_edit_expenses')::boolean,false)
          )
        )
      );

    if coalesce((p_daily_capability->>'enabled')::boolean,false) then
      select pg_catalog.jsonb_build_object(
        'date',d.rota_date,
        'starts_at',d.shift_starts_at,
        'ends_at',d.shift_ends_at,
        'hospital',d.hospital,
        'ward',d.ward,
        'job_title',d.job_title,
        'booking_ref',d.booking_ref
      ) into v_next_shift
      from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g
        on g.generation_id=s.active_generation_id and g.state='ACTIVE'
      join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
      where s.environment=pg_catalog.upper(p_environment)
        and s.candidate_id=p_candidate_id
        and d.booked and d.shift_starts_at is not null and d.shift_ends_at>p_now_utc
      order by d.shift_starts_at,d.rota_date,d.booking_id
      limit 1;
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'announcement',pg_catalog.jsonb_build_object(
      'text',v_settings.candidate_home_announcement_text,
      'version',v_settings.candidate_home_announcement_version,
      'updated_at_utc',v_settings.candidate_home_announcement_updated_at_utc
    ),
    'timesheets',pg_catalog.jsonb_build_object(
      'attention_count',v_timesheet_attention_count,
      'draft_timesheet_count',v_draft_timesheet_count,
      'draft_expense_count',v_draft_expense_count,
      'draft_timesheet_record_ids',v_draft_timesheet_record_ids,
      'draft_expense_record_ids',v_draft_expense_record_ids
    ),
    'notifications',pg_catalog.jsonb_build_object('unread_count',v_unread_count),
    'next_shift',v_next_shift
  );
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
    and (target.channel<>'PUSH' or exists(
      select 1 from public.weekly_message_dispatch_commands command
      join public.weekly_candidate_message_notifications message_notification on message_notification.message_intent_id=command.message_intent_id
      join public.candidate_notifications notification on notification.id=message_notification.notification_id
      where command.id=target.dispatch_command_id and notification.state='UNREAD'
        and private.candidate_notification_visible_v1(notification,pg_catalog.transaction_timestamp())
    ))
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
  if v_target.channel='PUSH' and not exists(
    select 1 from public.weekly_candidate_message_notifications message_notification
    join public.candidate_notifications notification on notification.id=message_notification.notification_id
    where message_notification.message_intent_id=v_intent.id and notification.state='UNREAD'
      and private.candidate_notification_visible_v1(notification,pg_catalog.transaction_timestamp())
  ) then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),terminal_reason='NOTIFICATION_NO_LONGER_ACTIONABLE',
      next_attempt_at_utc=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object('ok',false,'reason','NOTIFICATION_NO_LONGER_ACTIONABLE',
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id);
  end if;
  if v_intent.audience_kind='CANDIDATE' and not exists(
    select 1 from public.weekly_candidate_outreach_generations generation
    join public.weekly_candidate_cohorts cohort on cohort.id=generation.candidate_cohort_id
    where generation.id=v_command.candidate_generation_id and generation.state='ACTIVE'
      and private.weekly_source_candidate_generation_current_v1(generation.id)
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

alter function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz) owner to postgres;
revoke all on function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz) from public,anon,authenticated,service_role;
alter function public.weekly_source_message_dispatch_target_claim_v1(jsonb) owner to postgres;
alter function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_message_dispatch_target_claim_v1(jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_message_dispatch_target_claim_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) to service_role;
