-- Repeatable CloudTMS authority: weekly_source_query_delivery_v1
--
-- Owns only the non-financial Weekly source discrepancy, outreach, deterministic
-- message-outbox, manager-review and Office-notice lifecycles. It deliberately
-- contains no provider call and cannot write Timesheet finance, pay, Workbench,
-- Banking Pay, invoice or source-finalisation state.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_query_require_service_v1()
returns void
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     ) <> 'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
end;
$function$;

-- Does an ACCEPTED manager review batch on this recipient route already own
-- this exact incident episode?
--
-- 04A section 6.1: EARLY_ALL makes every currently manager-actionable,
-- "unanswered and unsent" row due, but "never resends a row already in an
-- accepted batch".  04A section 6.5: "one accepted batch owns each row and the
-- other action rechecks the remainder. No row is sent twice."  04A section 8:
-- "A later cohort merely becoming due is not a revocation event for an earlier
-- accepted batch."
--
-- Ownership therefore belongs to the RECIPIENT ROUTE, not to one generation of
-- it: a generation rotates whenever anything on the route changes, and a rule
-- scoped to a single generation stops holding the moment it does.
--
-- Only a batch that reached provider submission owns anything.  04A section 9
-- names that exact moment: the sender "commits SUBMISSION_STARTED for the exact
-- dispatch attempt before making the external call", and the render carries
-- that state.  A render that was retired before submission is unsent work whose
-- rows are still owed, so it must not block a later send.
--
-- A later incident EPISODE is different work under 04A section 6 and is not
-- excluded: the episode is part of the key.
create or replace function private.weekly_source_query_manager_row_owned_v1(
  p_recipient_route_id uuid,
  p_incident_id uuid,
  p_incident_episode integer
) returns boolean
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select case
    when p_recipient_route_id is null or p_incident_id is null
      or p_incident_episode is null
    then true
    else exists(
      select 1
      from public.weekly_manager_review_items owning_item
      join public.weekly_manager_review_batches owning_batch
        on owning_batch.id=owning_item.review_batch_id
      join public.weekly_manager_recipient_generations owning_generation
        on owning_generation.id=owning_batch.recipient_generation_id
      join public.weekly_message_renders owning_render
        on owning_render.id=owning_batch.message_render_id
      where owning_item.incident_id=p_incident_id
        and owning_item.incident_episode=p_incident_episode
        and owning_generation.recipient_route_id=p_recipient_route_id
        and owning_batch.state in ('ACTIVE','COMPLETE')
        and owning_render.state in ('SUBMISSION_STARTED','ACCEPTED','AMBIGUOUS')
        and owning_item.response_state in ('UNANSWERED','ANSWERED')
    )
  end;
$function$;

create or replace function public.weekly_source_candidate_query_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_candidate_id uuid;
  v_generation_id uuid;
  v_publication_id uuid;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_items jsonb;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('candidate_id','candidate_generation_id','projection_publication_id')) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERY_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERY_REQUEST_INVALID' using errcode='22023';
  end;
  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=v_generation_id and candidate_id=v_candidate_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  if v_generation.request_kind='SUBMIT_TIMESHEET' then
    select * into strict v_submission from public.weekly_timesheet_submission_requests
    where candidate_cohort_id=v_generation.candidate_cohort_id
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED');
    select coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'request_membership_id',membership.id,'week_ending',membership.week_ending,
        'client_name',client.name,'contract_id',membership.contract_id
      ) order by membership.week_ending,private.weekly_source_query_ascii_fold_v1(client.name) collate "C",membership.id
    ),'[]'::jsonb) into v_items
    from public.weekly_timesheet_submission_request_memberships membership
    join public.clients client on client.id=membership.client_id
    where membership.submission_request_id=v_submission.id and membership.state='WAITING';
    return pg_catalog.jsonb_build_object(
      'ok',true,'request_kind','SUBMIT_TIMESHEET','title','Submit your Timesheet',
      'submission_request_id',v_submission.id,'deadline_at_utc',v_submission.deadline_at_utc,
      'overdue',v_submission.state='OVERDUE','items',v_items
    );
  end if;
  if v_generation.state<>'ACTIVE' or pg_catalog.transaction_timestamp()>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERY_UNAVAILABLE' using errcode='42501';
  end if;
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'incident_id',incident.id,
      'current_fact_version',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'),
      'expected_timesheet_hash',pg_catalog.encode(
        private.weekly_source_query_candidate_timesheet_hash_v1(comparison.candidate_timesheet_id),'hex'
      ),
      'client_name',client.name,'work_date',work_event.work_date,
      'candidate_start',to_char(comparison.candidate_start_at_local,'HH24:MI'),
      'candidate_end',to_char(comparison.candidate_end_at_local,'HH24:MI'),
      'candidate_break_minutes',comparison.candidate_break_minutes,
      'system_start',case when comparison.source_presence='PRESENT' then to_char(comparison.system_start_at_local,'HH24:MI') end,
      'system_end',case when comparison.source_presence='PRESENT' then to_char(comparison.system_end_at_local,'HH24:MI') end,
      'system_break_minutes',case when comparison.source_presence='PRESENT' then comparison.system_break_minutes end,
      'system_hours_text',case
        when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
          then 'Missing or not yet authorised'
        when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' then 'Not shown in system'
        when comparison.issue_family='HEALTHROSTER_NOT_FINALISED' then 'Not finalised - no actual hours'
        else null end,
      'response_options',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('value','CANDIDATE_WRONG','label','My hours are wrong. The client is correct.'),
        pg_catalog.jsonb_build_object('value','CANDIDATE_CORRECT','label','My hours are correct.'),
        pg_catalog.jsonb_build_object('value','NEITHER_CORRECT','label','Both sets of hours are wrong.')
      ),
      'contact_manager_message','Please contact your manager urgently if the system hours need to be changed.'
    ) order by work_event.work_date,coalesce(comparison.candidate_start_at_local,comparison.system_start_at_local),incident.id
  ),'[]'::jsonb) into v_items
  from public.weekly_candidate_outreach_memberships membership
  join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
  join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  join public.clients client on client.id=incident.client_id
  where membership.candidate_generation_id=v_generation.id and membership.state='ACTIONABLE'
    and incident.state='OPEN';
  return pg_catalog.jsonb_build_object(
    'ok',true,'request_kind','CHECK_HOURS','title','Check your Timesheet hours',
    'deadline_at_utc',v_generation.deadline_at_utc,'items',v_items
  );
end;
$function$;

create or replace function public.weekly_source_message_render_input_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_intent_id uuid;
  v_publication_id uuid;
  v_intent public.weekly_message_intents%rowtype;
  v_generation public.weekly_manager_recipient_generations%rowtype;
  v_candidate_generation public.weekly_candidate_outreach_generations%rowtype;
  v_membership jsonb;
  v_membership_hash bytea;
  v_rows jsonb;
  v_shift_count integer;
  v_client_count integer;
  v_candidate_count integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('message_intent_id','projection_publication_id')) then
    raise exception 'WEEKLY_SOURCE_RENDER_INPUT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_intent_id:=(p_request->>'message_intent_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_RENDER_INPUT_REQUEST_INVALID' using errcode='22023';
  end;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_intent_id and state='DUE';
  perform private.weekly_source_query_current_publication_v1(v_intent.source_cycle_id,v_publication_id);
  if v_intent.audience_kind='CANDIDATE' then
    select * into strict v_candidate_generation from public.weekly_candidate_outreach_generations
    where id=v_intent.candidate_generation_id and state='ACTIVE';
    return pg_catalog.jsonb_build_object(
      'ok',true,'audience_kind','CANDIDATE','tranche_kind',v_intent.tranche_kind,
      'candidate_id',v_candidate_generation.candidate_id,
      'request_kind',v_candidate_generation.request_kind,
      'membership_hash',pg_catalog.encode(v_candidate_generation.membership_hash,'hex')
    );
  end if;
  if v_intent.audience_kind<>'MANAGER' then
    raise exception 'WEEKLY_SOURCE_RENDER_INPUT_AUDIENCE_UNSUPPORTED' using errcode='55000';
  end if;
  select * into strict v_generation from public.weekly_manager_recipient_generations
  where id=v_intent.recipient_generation_id and state='ACTIVE';
  v_membership:=coalesce((
    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'incident_id',ranked.incident_id,'incident_episode',ranked.episode_number,
        'comparison_revision_id',ranked.comparison_revision_id,
        'comparison_fingerprint',pg_catalog.encode(ranked.comparison_fingerprint,'hex'),
        'client_order',ranked.client_order,'candidate_order',ranked.candidate_order,
        'shift_order',ranked.shift_order
      ) order by ranked.client_order,ranked.candidate_order,ranked.shift_order
    )
    from (
      select incident.id as incident_id,incident.episode_number,
        comparison.id as comparison_revision_id,
        comparison.material_comparison_fingerprint as comparison_fingerprint,
        pg_catalog.dense_rank() over(order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)::integer as client_order,
        pg_catalog.dense_rank() over(partition by client.id order by private.weekly_source_query_ascii_fold_v1(coalesce(nullif(candidate.display_name,''),pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name))) collate "C",candidate.id)::integer as candidate_order,
        pg_catalog.row_number() over(partition by client.id,candidate.id order by work_event.work_date,coalesce(comparison.system_start_at_local,comparison.candidate_start_at_local),incident.id)::integer as shift_order
      from public.weekly_manager_recipient_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
      join public.weekly_work_events work_event on work_event.id=incident.work_event_id
      join public.clients client on client.id=incident.client_id
      join public.candidates candidate on candidate.id=incident.candidate_id
      where membership.recipient_generation_id=v_generation.id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
        and (
          coalesce(pg_catalog.array_length(v_intent.sorted_due_event_ids,1),0)=0
          or membership.candidate_cohort_id=any(
            select event.candidate_cohort_id from public.weekly_manager_cohort_due_events event
            where event.id=any(v_intent.sorted_due_event_ids)
              and event.recipient_generation_id=v_generation.id
          )
        )
        and (v_intent.tranche_kind<>'MANAGER_T6_RESPONDED' or incident.candidate_action_state='RESPONDED')
        and not private.weekly_source_query_manager_row_owned_v1(
          v_generation.recipient_route_id,incident.id,incident.episode_number
        )
    ) ranked
  ),'[]'::jsonb);
  v_shift_count:=pg_catalog.jsonb_array_length(v_membership);
  if v_shift_count=0 then raise exception 'WEEKLY_SOURCE_MANAGER_RENDER_EMPTY' using errcode='55000'; end if;
  v_membership_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_REVIEW_BATCH_MEMBERSHIP_V1',v_membership
  );
  select pg_catalog.count(distinct incident.client_id),pg_catalog.count(distinct incident.candidate_id)
  into v_client_count,v_candidate_count
  from pg_catalog.jsonb_array_elements(v_membership) member
  join public.weekly_discrepancy_incidents incident on incident.id=(member->>'incident_id')::uuid;
  if v_client_count>100 or v_candidate_count>100 or v_shift_count>500 then
    raise exception 'WEEKLY_SOURCE_MANAGER_DIGEST_CAPACITY_EXCEEDED' using errcode='54000';
  end if;
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'clientId',incident.client_id,'clientName',client.name,
      'candidateId',incident.candidate_id,
      'displayName',coalesce(nullif(candidate.display_name,''),pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)),
      'issueId',incident.id,'workDate',work_event.work_date,
      'sourceStartInstant',coalesce(comparison.system_start_at_local,comparison.candidate_start_at_local),
      'start',to_char(comparison.candidate_start_at_local,'HH24:MI'),
      'end',to_char(comparison.candidate_end_at_local,'HH24:MI'),
      'breakMinutes',comparison.candidate_break_minutes,
      'systemStart',case when comparison.source_presence='PRESENT' then to_char(comparison.system_start_at_local,'HH24:MI') end,
      'systemEnd',case when comparison.source_presence='PRESENT' then to_char(comparison.system_end_at_local,'HH24:MI') end,
      'systemBreakMinutes',case when comparison.source_presence='PRESENT' then comparison.system_break_minutes end,
      'systemAbsent',comparison.source_presence<>'PRESENT',
      'issueFamily',case
        when comparison.issue_family='SOURCE_HOURS_DIFFER' then 'HOURS_DIFFER'
        when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and source_group.source_family='NHSP' then 'NHSP_ABSENT'
        when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' then 'SOURCE_ABSENT'
        else 'HEALTHROSTER_NOT_FINALISED' end,
      'candidateRequested',incident.candidate_action_state='RESPONDED'
    ) order by (member->>'client_order')::integer,(member->>'candidate_order')::integer,(member->>'shift_order')::integer
  ),'[]'::jsonb) into v_rows
  from pg_catalog.jsonb_array_elements(v_membership) member
  join public.weekly_discrepancy_incidents incident on incident.id=(member->>'incident_id')::uuid
  join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  join public.clients client on client.id=incident.client_id
  join public.candidates candidate on candidate.id=incident.candidate_id
  join public.weekly_source_groups source_group on source_group.id=incident.source_group_id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'audience_kind','MANAGER','tranche_kind',v_intent.tranche_kind,
    'policy_version','1.7.0','renderer_version','1.3.0','structure_version','1.0.0',
    'membership_hash',pg_catalog.encode(v_membership_hash,'hex'),
    'shift_count',v_shift_count,'client_count',v_client_count,
    'candidate_count',v_candidate_count,'rows',v_rows
  );
end;
$function$;

create or replace function public.weekly_source_timesheet_submission_complete_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_candidate_id uuid;
  v_submission_id uuid;
  v_publication_id uuid;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_completion jsonb;
  v_membership public.weekly_timesheet_submission_request_memberships%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_timesheet_hash bytea;
  v_outcome text;
  v_context jsonb;
  v_route_id uuid;
  v_route_ids uuid[]:='{}'::uuid[];
  v_route_activation public.weekly_route_activations%rowtype;
  v_manager_generation jsonb;
  v_manager_generation_id uuid;
  v_due_ids uuid[];
  v_intent_id uuid;
  v_completed integer:=0;
  v_issue_count integer:=0;
  v_manager_intents jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('candidate_id','submission_request_id','projection_publication_id','completions'))
     or pg_catalog.jsonb_typeof(p_request->'completions')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'completions')=0 then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_submission_id:=(p_request->>'submission_request_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_REQUEST_INVALID' using errcode='22023';
  end;
  select * into strict v_submission from public.weekly_timesheet_submission_requests
  where id=v_submission_id for update;
  if v_submission.candidate_id<>v_candidate_id
     or v_submission.state not in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED') then
    if v_submission.candidate_id=v_candidate_id and v_submission.state='COMPLETE' then
      return pg_catalog.jsonb_build_object('ok',true,'replay',true,'submission_request_id',v_submission.id);
    end if;
    raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_query_current_publication_v1(v_submission.source_cycle_id,v_publication_id);
  select generation.* into strict v_generation
  from public.weekly_candidate_outreach_generations generation
  join public.weekly_candidate_cohorts cohort on cohort.id=generation.candidate_cohort_id
  where generation.candidate_cohort_id=v_submission.candidate_cohort_id
    and generation.request_kind='SUBMIT_TIMESHEET' and generation.state='ACTIVE'
    and cohort.current_generation_id=generation.id
  for update of generation;

  for v_completion in
    select value from pg_catalog.jsonb_array_elements(p_request->'completions')
    order by (value->>'membership_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_completion)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_completion) key
                 where key not in (
                   'membership_id','timesheet_id','timesheet_revision','timesheet_hash','outcome'
                 )) then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_membership
      from public.weekly_timesheet_submission_request_memberships
      where id=(v_completion->>'membership_id')::uuid
        and submission_request_id=v_submission.id and state='WAITING'
      for update;
      select * into strict v_timesheet from public.timesheets
      where timesheet_id=(v_completion->>'timesheet_id')::uuid
        and version=(v_completion->>'timesheet_revision')::integer
        and contract_id=v_membership.contract_id
        and week_ending_date=v_membership.week_ending
        and is_current and revoked_at is null and archived_at_utc is null
        and sheet_scope='WEEKLY' and line_type='HOURS'
        and r2_nurse_key is not null and img_sha256_nurse is not null;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_ITEM_STALE' using errcode='40001';
    end;
    v_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(v_timesheet.timesheet_id);
    if v_timesheet_hash<>private.weekly_source_query_hex32_v1(
      v_completion->>'timesheet_hash','WEEKLY_SOURCE_CANDIDATE_TIMESHEET_HASH_INVALID'
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
    end if;
    v_outcome:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_completion->>'outcome','')));
    if v_outcome not in ('MATCHED','ISSUES') then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_OUTCOME_INVALID' using errcode='22023';
    end if;
    if v_outcome='MATCHED' and exists(
      select 1 from public.weekly_discrepancy_incidents incident
      join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
      join public.weekly_work_events work_event on work_event.id=incident.work_event_id
      where incident.source_cycle_id=v_submission.source_cycle_id
        and incident.candidate_id=v_candidate_id and incident.client_id=v_membership.client_id
        and comparison.contract_id=v_membership.contract_id and incident.state='OPEN'
        and work_event.work_date>v_membership.week_ending-7
        and work_event.work_date<=v_membership.week_ending
    ) then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_HAS_ISSUES' using errcode='40001';
    elsif v_outcome='ISSUES' and not exists(
      select 1 from public.weekly_discrepancy_incidents incident
      join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
      join public.weekly_work_events work_event on work_event.id=incident.work_event_id
      where incident.source_cycle_id=v_submission.source_cycle_id
        and incident.candidate_id=v_candidate_id and incident.client_id=v_membership.client_id
        and comparison.contract_id=v_membership.contract_id and incident.state='OPEN'
        and work_event.work_date>v_membership.week_ending-7
        and work_event.work_date<=v_membership.week_ending
    ) then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_ISSUES_MISSING' using errcode='40001';
    end if;
    update public.weekly_timesheet_submission_request_memberships
    set state=case when v_outcome='MATCHED' then 'SUBMITTED_MATCHED' else 'SUBMITTED_WITH_ISSUES' end,
        submitted_timesheet_id=v_timesheet.timesheet_id,
        submitted_timesheet_revision=v_timesheet.version,
        submitted_timesheet_hash=v_timesheet_hash,completed_at_utc=pg_catalog.transaction_timestamp()
    where id=v_membership.id;
    if v_outcome='ISSUES' then
      v_issue_count:=v_issue_count+1;
      update public.weekly_discrepancy_incidents incident
      set candidate_action_state='NOT_REQUIRED'
      from public.weekly_issue_comparison_revisions comparison,public.weekly_work_events work_event
      where comparison.id=incident.current_comparison_revision_id
        and work_event.id=incident.work_event_id
        and incident.source_cycle_id=v_submission.source_cycle_id
        and incident.candidate_id=v_candidate_id and incident.client_id=v_membership.client_id
        and comparison.contract_id=v_membership.contract_id and incident.state='OPEN'
        and work_event.work_date>v_membership.week_ending-7
        and work_event.work_date<=v_membership.week_ending;
      v_context:=private.weekly_source_query_cohort_ensure_v1(
        v_submission.source_cycle_id,v_candidate_id,v_membership.client_id,
        v_membership.contract_id,v_membership.week_ending
      );
      if coalesce((v_context->>'manager_queries_enabled')::boolean,false)
         and nullif(v_context->>'manager_route_id','') is not null then
        v_route_id:=(v_context->>'manager_route_id')::uuid;
        -- 04A section 6: a signed Timesheet that mismatches queues "the exact
        -- new shift incidents" for an immediate EARLY_ALL.  Section 6.1 makes
        -- only an unanswered AND unsent row due, so a row an accepted batch on
        -- this route already owns keeps its SENT state and is not re-queued.
        update public.weekly_discrepancy_incidents incident
        set manager_action_state='DUE'
        from public.weekly_issue_comparison_revisions comparison,public.weekly_work_events work_event
        where comparison.id=incident.current_comparison_revision_id
          and work_event.id=incident.work_event_id
          and incident.source_cycle_id=v_submission.source_cycle_id
          and incident.candidate_id=v_candidate_id and incident.client_id=v_membership.client_id
          and comparison.contract_id=v_membership.contract_id and incident.state='OPEN'
          and work_event.work_date>v_membership.week_ending-7
          and work_event.work_date<=v_membership.week_ending
          and incident.manager_potential_state='AVAILABLE'
          and not private.weekly_source_query_manager_row_owned_v1(
            v_route_id,incident.id,incident.episode_number
          );
        insert into public.weekly_route_activations(
          source_cycle_id,candidate_id,client_id,audience_route,route_mode,
          activated_by_user_id,activated_at_utc,updated_at_utc
        ) values (
          v_submission.source_cycle_id,v_candidate_id,v_membership.client_id,
          'MANAGER','MANAGER_DIRECT',null,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
        ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
        do update set route_mode='MANAGER_DIRECT',
          activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
          updated_at_utc=excluded.updated_at_utc
        returning * into v_route_activation;
        if not (v_route_id=any(v_route_ids)) then
          v_route_ids:=pg_catalog.array_append(v_route_ids,v_route_id);
        end if;
      end if;
    end if;
    v_completed:=v_completed+1;
  end loop;
  if v_completed<>pg_catalog.jsonb_array_length(p_request->'completions') then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_COMPLETION_SELECTION_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from public.weekly_timesheet_submission_request_memberships
            where submission_request_id=v_submission.id and state='WAITING') then
    update public.weekly_timesheet_submission_requests
    set state='PARTLY_SUBMITTED',updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_submission.id;
  else
    update public.weekly_timesheet_submission_requests
    set state='COMPLETE',updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_submission.id;
    update public.weekly_candidate_outreach_generations set state='COMPLETE'
    where id=v_generation.id;
    update public.weekly_message_intents set state='RETIRED'
    where candidate_generation_id=v_generation.id and state in ('DUE','RENDERED');
  end if;
  for v_route_id in
    select distinct route_id from pg_catalog.unnest(v_route_ids) route_id order by route_id
  loop
    v_manager_generation:=private.weekly_source_query_manager_generation_v1(
      v_route_id,'OFFICE_DIRECT',pg_catalog.transaction_timestamp(),true
    );
    v_manager_generation_id:=(v_manager_generation->>'recipient_generation_id')::uuid;
    select pg_catalog.array_agg(id order by id) into v_due_ids
    from public.weekly_manager_cohort_due_events
    where recipient_generation_id=v_manager_generation_id and event_kind='EARLY_ALL' and state='PENDING';
    if coalesce(pg_catalog.array_length(v_due_ids,1),0)>0
       and not exists(
         select 1 from public.weekly_message_intents intent
         where intent.recipient_generation_id=v_manager_generation_id
           and intent.state in ('DUE','RENDERED')
       )
       and exists(
         -- 04A section 6.1: a row already owned by an accepted batch on this
         -- route is neither unanswered-and-unsent nor due, so it cannot be the
         -- whole reason for an EARLY_ALL intent whose render would be empty.
         select 1 from public.weekly_manager_recipient_memberships membership
         join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
         where membership.recipient_generation_id=v_manager_generation_id
           and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
           and not private.weekly_source_query_manager_row_owned_v1(
             v_route_id,incident.id,incident.episode_number
           )
       ) then
      v_intent_id:=private.weekly_source_query_manager_intent_v1(
        v_route_id,v_manager_generation_id,v_due_ids,'MANAGER_EARLY_ALL',1,
        pg_catalog.transaction_timestamp()
      );
      update public.weekly_manager_cohort_due_events set state='CONSUMED' where id=any(v_due_ids);
      v_manager_intents:=v_manager_intents||pg_catalog.jsonb_build_array(v_intent_id);
    end if;
  end loop;
  perform public._audit_insert(
    'weekly_timesheet_submission_request',v_submission.id::text,
    'WEEKLY_TIMESHEET_SUBMISSION_COMPLETED',null,
    pg_catalog.jsonb_build_object(
      'completed_scope_count',v_completed,'issue_scope_count',v_issue_count,
      'request_complete',not exists(select 1 from public.weekly_timesheet_submission_request_memberships
                                    where submission_request_id=v_submission.id and state='WAITING')
    ),'Candidate submitted requested Weekly Timesheet',null
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'completed_scope_count',v_completed,
    'issue_scope_count',v_issue_count,'manager_message_intent_ids',v_manager_intents
  );
end;
$function$;

create or replace function public.weekly_source_query_accept_system_hours_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_requested uuid[];
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_count integer:=0;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','source_cycle_id','projection_publication_id','incident_ids'))
     or pg_catalog.jsonb_typeof(p_request->'incident_ids')<>'array' then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    select pg_catalog.array_agg(value::uuid order by value::uuid) into v_requested
    from pg_catalog.jsonb_array_elements_text(p_request->'incident_ids');
  exception when others then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end;
  if coalesce(pg_catalog.array_length(v_requested,1),0)=0 then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_INVALID' using errcode='22023';
  end if;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date
  );
  for v_incident in
    select incident.* from public.weekly_discrepancy_incidents incident
    where incident.id=any(v_requested) order by incident.id for update
  loop
    if v_incident.source_cycle_id<>v_cycle_id or v_incident.state<>'OPEN' then
      raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_comparison from public.weekly_issue_comparison_revisions
    where id=v_incident.current_comparison_revision_id;
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
        candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
        manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
        resolved_at_utc=pg_catalog.transaction_timestamp(),
        resolution_kind='OFFICE_ACCEPTED_SYSTEM_HOURS'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications set operational_state='RESOLVED',
      resolved_at_utc=pg_catalog.transaction_timestamp()
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'OFFICE_ACCEPTED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'RESOLVED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED_RESOLVED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    v_count:=v_count+1;
  end loop;
  if v_count<>pg_catalog.array_length(v_requested,1) then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
  end if;
  perform public._audit_insert(
    'weekly_source_query_resolution',v_publication_id::text,
    'WEEKLY_SOURCE_SYSTEM_HOURS_ACCEPTED',null,
    pg_catalog.jsonb_build_object('incident_count',v_count),
    'Office accepted system hours',v_actor
  );
  return pg_catalog.jsonb_build_object('ok',true,'resolved_count',v_count);
end;
$function$;

create or replace function public.weekly_source_office_notifications_list_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_limit integer;
  v_open_only boolean;
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','limit','open_only')) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,100);
    v_open_only:=coalesce((p_request->>'open_only')::boolean,true);
  exception when others then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_limit<1 or v_limit>500 or not exists(
    select 1 from public.tms_users u where u.id=v_actor and u.is_active
      and pg_catalog.lower(pg_catalog.btrim(u.role)) in ('admin','user')
  ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_FORBIDDEN' using errcode='42501';
  end if;
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data) order by row_data.created_at_utc desc,row_data.id),'[]'::jsonb)
  into v_rows
  from (
    select notice.id,notice.event_kind,notice.issue_id,notice.issue_generation,
      notice.payload_json,notice.operational_state,notice.read_at_utc,notice.created_at_utc
    from public.office_action_notifications notice
    where notice.recipient_user_id=v_actor
      and (not v_open_only or notice.operational_state='OPEN')
    order by notice.created_at_utc desc,notice.id
    limit v_limit
  ) row_data;
  return pg_catalog.jsonb_build_object('ok',true,'notifications',v_rows);
end;
$function$;

create or replace function public.weekly_source_office_notification_ack_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_notice_id uuid;
  v_notice public.office_action_notifications%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','notification_id')) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_ACK_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_notice_id:=(p_request->>'notification_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_ACK_INVALID' using errcode='22023';
  end;
  select * into strict v_notice from public.office_action_notifications
  where id=v_notice_id and recipient_user_id=v_actor for update;
  update public.office_action_notifications
  set read_at_utc=coalesce(read_at_utc,pg_catalog.transaction_timestamp())
  where id=v_notice.id;
  return pg_catalog.jsonb_build_object('ok',true,'notification_id',v_notice.id,'acknowledged',true);
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_NOT_FOUND' using errcode='42501';
end;
$function$;

create or replace function public.weekly_source_manager_review_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_credential_hash bytea;
  v_control_ticket uuid;
  v_receipt public.weekly_manager_route_receipts%rowtype;
  v_batch public.weekly_manager_review_batches%rowtype;
  v_items jsonb;
  v_remaining integer;
  v_total integer;
  v_completed integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('credential_hash','control_plane_ticket_id')) then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_credential_hash:=private.weekly_source_query_hex32_v1(
      p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
    );
    v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_REQUEST_INVALID' using errcode='22023';
  end;
  select receipt.* into strict v_receipt
  from public.weekly_manager_route_receipts receipt
  join public.weekly_manager_review_batches batch on batch.id=receipt.review_batch_id
  where receipt.credential_hash=v_credential_hash
    and receipt.control_plane_ticket_id=v_control_ticket
  for update of receipt;
  select * into strict v_batch from public.weekly_manager_review_batches
  where id=v_receipt.review_batch_id for update;
  if v_receipt.state not in ('ACTIVE','COMPLETE') or v_batch.state not in ('ACTIVE','COMPLETE') then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
  end if;
  if pg_catalog.transaction_timestamp()>=v_receipt.expires_at_utc then
    update public.weekly_manager_route_receipts
    set state='EXPIRED' where id=v_receipt.id and state='ACTIVE';
    update public.weekly_manager_review_batches
    set state='EXPIRED' where id=v_batch.id and state='ACTIVE';
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_EXPIRED' using errcode='42501';
  end if;
  update public.weekly_manager_review_items item
  set response_state='FILTERED_RESOLVED'
  from public.weekly_discrepancy_incidents incident
  where item.review_batch_id=v_batch.id and item.incident_id=incident.id
    and item.response_state='UNANSWERED' and incident.state<>'OPEN';

  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'review_item_id',row_data.review_item_id,
      'incident_episode',row_data.incident_episode,
      'current_fact_version',pg_catalog.encode(row_data.current_fingerprint,'hex'),
      'client_id',row_data.client_id,'client_name',row_data.client_name,
      'candidate_id',row_data.candidate_id,'candidate_name',row_data.candidate_name,
      'work_date',row_data.work_date,
      'source_start_instant',row_data.source_start_instant,
      'candidate_start',to_char(row_data.candidate_start,'HH24:MI'),
      'candidate_end',to_char(row_data.candidate_end,'HH24:MI'),
      'candidate_break_minutes',row_data.candidate_break,
      'system_start',case when row_data.source_presence='PRESENT' then to_char(row_data.system_start,'HH24:MI') end,
      'system_end',case when row_data.source_presence='PRESENT' then to_char(row_data.system_end,'HH24:MI') end,
      'system_break_minutes',case when row_data.source_presence='PRESENT' then row_data.system_break end,
      'system_hours_text',case
        when row_data.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and row_data.source_family='NHSP'
          then 'Missing or not yet authorised'
        when row_data.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
          then 'Not shown in system'
        when row_data.issue_family='HEALTHROSTER_NOT_FINALISED'
          then 'Not finalised - no actual hours'
        else null end,
      'issue_family',case
        when row_data.issue_family='SOURCE_HOURS_DIFFER' then 'HOURS_DIFFER'
        when row_data.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and row_data.source_family='NHSP' then 'NHSP_ABSENT'
        when row_data.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' then 'SOURCE_ABSENT'
        else 'HEALTHROSTER_NOT_FINALISED' end,
      'candidate_response',case when row_data.candidate_action_state='RESPONDED'
        then 'Candidate requested your review' end,
      'response_options',case
        when row_data.issue_family='SOURCE_HOURS_DIFFER' then pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object('value','SYSTEM_CORRECT','label','The system hours are correct. The candidate timesheet hours are wrong.'),
          pg_catalog.jsonb_build_object('value','MANAGER_CORRECTED_SOURCE','label','I have corrected the hours','requires_hours',true)
        )
        when row_data.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and row_data.source_family='NHSP' then pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object('value','CANDIDATE_DID_NOT_WORK','label','The candidate did not work this shift'),
          pg_catalog.jsonb_build_object('value','MANAGER_CORRECTED_SOURCE','label','I have added and/or authorised the shift','requires_hours',true)
        )
        when row_data.issue_family='HEALTHROSTER_NOT_FINALISED' then pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object('value','CANDIDATE_DID_NOT_WORK','label','The candidate did not work this shift'),
          pg_catalog.jsonb_build_object('value','MANAGER_CORRECTED_SOURCE','label','I have added and/or finalised the hours','requires_hours',true)
        )
        else pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object('value','CANDIDATE_DID_NOT_WORK','label','The candidate did not work this shift'),
          pg_catalog.jsonb_build_object('value','MANAGER_CORRECTED_SOURCE','label','I have added the shift','requires_hours',true)
        ) end
    ) order by row_data.client_order,row_data.candidate_order,row_data.shift_order
  ),'[]'::jsonb),pg_catalog.count(*) into v_items,v_remaining
  from (
    select item.id as review_item_id,item.incident_episode,item.client_order,
      item.candidate_order,item.shift_order,comparison.material_comparison_fingerprint as current_fingerprint,
      incident.client_id,incident.candidate_id,
      client.name as client_name,
      coalesce(nullif(candidate.display_name,''),pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)) as candidate_name,
      work_event.work_date,comparison.candidate_start_at_local as candidate_start,
      comparison.candidate_end_at_local as candidate_end,
      comparison.candidate_break_minutes as candidate_break,
      comparison.system_start_at_local as system_start,comparison.system_end_at_local as system_end,
      comparison.system_break_minutes as system_break,comparison.source_presence,
      comparison.issue_family,source_group.source_family,incident.candidate_action_state,
      (coalesce(comparison.system_start_at_local,comparison.candidate_start_at_local)
        at time zone 'Europe/London') as source_start_instant
    from public.weekly_manager_review_items item
    join public.weekly_discrepancy_incidents incident on incident.id=item.incident_id
      and incident.episode_number=item.incident_episode and incident.state='OPEN'
    join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    join public.clients client on client.id=incident.client_id
    join public.candidates candidate on candidate.id=incident.candidate_id
    join public.weekly_source_groups source_group on source_group.id=incident.source_group_id
    where item.review_batch_id=v_batch.id and item.response_state='UNANSWERED'
  ) row_data;
  if v_remaining=0 and v_batch.state='ACTIVE' then
    update public.weekly_manager_review_batches
    set state='COMPLETE',completed_at_utc=pg_catalog.transaction_timestamp()
    where id=v_batch.id;
    update public.weekly_manager_route_receipts set state='COMPLETE' where id=v_receipt.id;
  end if;
  select pg_catalog.count(*),
         pg_catalog.count(*) filter(where response_state<>'UNANSWERED')
  into v_total,v_completed
  from public.weekly_manager_review_items where review_batch_id=v_batch.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'review_batch_id',v_batch.id,
    'review_batch_version',v_batch.credential_generation,
    'response_fingerprint',pg_catalog.encode(v_batch.sent_content_hash,'hex'),
    'recipient_generation_id',v_batch.recipient_generation_id,
    'original_membership_hash',pg_catalog.encode(v_batch.original_membership_hash,'hex'),
    'credential_generation',v_batch.credential_generation,
    'expires_at_utc',v_batch.expires_at_utc,'total_count',v_total,
    'completed_count',v_completed,'remaining_count',v_remaining,'items',v_items
  );
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
end;
$function$;

create or replace function public.weekly_source_manager_review_respond_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_credential_hash bytea;
  v_control_ticket uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_review_batch_version integer;
  v_response_fingerprint bytea;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_receipt public.weekly_manager_route_receipts%rowtype;
  v_batch public.weekly_manager_review_batches%rowtype;
  v_response jsonb;
  v_item public.weekly_manager_review_items%rowtype;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_kind text;
  v_expected bytea;
  v_response_hash bytea;
  v_event_id uuid;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_summary text;
  v_answered integer:=0;
  v_notices integer:=0;
  v_intended_start timestamp without time zone;
  v_intended_end timestamp without time zone;
  v_intended_break integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'credential_hash','control_plane_ticket_id',
                 'request_idempotency_key','review_batch_version','response_fingerprint','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_credential_hash:=private.weekly_source_query_hex32_v1(
      p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
    );
    v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_review_batch_version:=(p_request->>'review_batch_version')::integer;
    v_response_fingerprint:=private.weekly_source_query_hex32_v1(
      p_request->>'response_fingerprint','WEEKLY_SOURCE_MANAGER_RESPONSE_FINGERPRINT_INVALID'
    );
    v_responses:=(select pg_catalog.jsonb_agg(value order by (value->>'review_item_id')::uuid)
                 from pg_catalog.jsonb_array_elements(p_request->'responses'));
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RESPONSE_REQUEST_V1',pg_catalog.jsonb_build_object(
      'control_plane_ticket_id',v_control_ticket,
      'review_batch_version',v_review_batch_version,
      'response_fingerprint',pg_catalog.encode(v_response_fingerprint,'hex'),
      'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');
  -- Exact response replay wins before credential expiry or mutable fact checks.
  select pg_catalog.count(*),min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='MANAGER_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;
  select receipt.* into strict v_receipt
  from public.weekly_manager_route_receipts receipt
  join public.weekly_manager_review_batches batch on batch.id=receipt.review_batch_id
  where receipt.credential_hash=v_credential_hash
    and receipt.control_plane_ticket_id=v_control_ticket
  for update of receipt;
  select * into strict v_batch from public.weekly_manager_review_batches
  where id=v_receipt.review_batch_id for update;
  if v_receipt.state<>'ACTIVE' or v_batch.state<>'ACTIVE'
     or pg_catalog.transaction_timestamp()>=v_receipt.expires_at_utc then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
  end if;
  if v_review_batch_version<>v_batch.credential_generation
     or v_response_fingerprint<>v_batch.sent_content_hash then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_BATCH_STALE' using errcode='40001';
  end if;
  for v_response in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'review_item_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_response)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                 where key not in (
                   'review_item_id','incident_episode','current_fact_version','response_kind',
                   'intended_start','intended_end','intended_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_item from public.weekly_manager_review_items
      where id=(v_response->>'review_item_id')::uuid and review_batch_id=v_batch.id
        and response_state='UNANSWERED' for update;
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=v_item.incident_id for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_response->>'current_fact_version','WEEKLY_SOURCE_MANAGER_FACT_VERSION_INVALID'
    );
    if v_incident.state<>'OPEN' or v_incident.episode_number<>(v_response->>'incident_episode')::integer
       or v_item.incident_episode<>v_incident.episode_number
       or v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_response->>'response_kind','')));
    if v_kind not in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK','MANAGER_CORRECTED_SOURCE')
       or (v_kind='SYSTEM_CORRECT' and v_comparison.issue_family<>'SOURCE_HOURS_DIFFER')
       or (v_kind='CANDIDATE_DID_NOT_WORK' and v_comparison.issue_family='SOURCE_HOURS_DIFFER') then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_KIND_INVALID' using errcode='22023';
    end if;
    v_intended_start:=null;
    v_intended_end:=null;
    v_intended_break:=null;
    if v_kind='MANAGER_CORRECTED_SOURCE' then
      begin
        if coalesce(v_response->>'intended_start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_response->>'intended_end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception 'invalid time';
        end if;
        v_intended_break:=(v_response->>'intended_break_minutes')::integer;
        v_intended_start:=v_work.work_date+(v_response->>'intended_start')::time;
        v_intended_end:=v_work.work_date+(v_response->>'intended_end')::time;
        -- 14 section 5.3.4 for the source side: "If start equals end, the row
        -- blocks because neither accepted profile supplies an explicit 24-hour
        -- marker."  The overnight roll below is for a genuine end-before-start
        -- shift; start equal to end is not a 24-hour shift and must not be
        -- recorded as one, now that Office reads this value.
        if v_intended_end=v_intended_start then raise exception 'equal start and end'; end if;
        if v_intended_end<v_intended_start then v_intended_end:=v_intended_end+interval '1 day'; end if;
        if v_intended_break<0 or v_intended_break>720
           or v_intended_end<=v_intended_start then raise exception 'invalid hours'; end if;
      exception when others then
        raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
      end;
    elsif v_response ? 'intended_start' or v_response ? 'intended_end'
       or v_response ? 'intended_break_minutes' then
      raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_MANAGER_RESPONSE_ITEM_V1',pg_catalog.jsonb_build_object(
        'review_item_id',v_item.id,'incident_id',v_incident.id,
        'incident_episode',v_incident.episode_number,
        'current_fact_version',pg_catalog.encode(v_expected,'hex'),
        'response_kind',v_kind,
        'intended_start_at_local',v_intended_start,
        'intended_end_at_local',v_intended_end,
        'intended_break_minutes',v_intended_break
      )
    );
    update public.weekly_manager_review_items
    set response_state='ANSWERED',response_kind=v_kind,
        intended_start_at_local=v_intended_start,
        intended_end_at_local=v_intended_end,
        intended_break_minutes=v_intended_break,
        response_fingerprint=v_response_hash,answered_at_utc=pg_catalog.transaction_timestamp()
    where id=v_item.id;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
      v_expected,'MANAGER_RESPONDED','MANAGER',pg_catalog.jsonb_build_object(
        'response_kind',v_kind,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses),
        'review_item_id',v_item.id
      ),'MANAGER_RESPONSE:'||v_request_key::text||':'||v_item.id::text
    ) returning id into v_event_id;
    if v_kind in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK') then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state=case when candidate_action_state='NOT_ASKED' then 'NOT_REQUIRED' else candidate_action_state end,
          manager_potential_state='NOT_REQUIRED',manager_action_state='RESPONDED',
          waiting_source_state='NOT_WAITING',resolved_at_utc=pg_catalog.transaction_timestamp(),
          resolution_kind='MANAGER_CONFIRMED_SYSTEM_HOURS'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
        v_expected,'RESOLVED','MANAGER',
        pg_catalog.jsonb_build_object('resolution','MANAGER_CONFIRMED_SYSTEM_HOURS'),
        'MANAGER_CONFIRMED:'||v_event_id::text
      );
    else
      update public.weekly_discrepancy_incidents
      set reconciliation_state='WAITING_FOR_SOURCE',manager_action_state='RESPONDED',
          waiting_source_state='WAITING_REIMPORT'
      where id=v_incident.id;
    end if;
    select * into strict v_client from public.clients where id=v_incident.client_id;
    select * into strict v_candidate from public.candidates where id=v_incident.candidate_id;
    select * into strict v_group from public.weekly_source_groups where id=v_incident.source_group_id;
    v_summary:=case
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
        then 'Shift missing or not yet authorised'
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
        then 'Shift not shown in system'
      else 'System hours reviewed'
    end;
    -- 04A section 4: "A corrected/added/finalised response records the manager's
    -- intended start, end and break and waits for a later accepted source
    -- upload; it never creates source hours."  The record lives on
    -- weekly_manager_review_items and is deliberately left out of every
    -- calculation.  It is repeated here, as plain non-financial text, so the
    -- Office notice this response raises actually carries the figures the
    -- manager gave; without them Office cannot compare the manager's stated
    -- correction with the later source, which is what section 6 expects it to
    -- do.  These keys are hours only: the fan-out owner rejects any pay,
    -- charge, rate, margin, VAT, invoice or banking key.
    v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
      v_incident.id,v_event_id,
      case when v_kind='MANAGER_CORRECTED_SOURCE'
        then 'WEEKLY_MANAGER_SOURCE_CORRECTED' else 'WEEKLY_MANAGER_SYSTEM_CONFIRMED' end,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'candidate_name',coalesce(nullif(v_candidate.display_name,''),
          pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
        'client_name',v_client.name,'work_date',v_work.work_date,
        'issue_summary',v_summary,'manager_response',v_kind,
        'manager_intended_start',to_char(v_intended_start,'YYYY-MM-DD HH24:MI'),
        'manager_intended_end',to_char(v_intended_end,'YYYY-MM-DD HH24:MI'),
        'manager_intended_break_minutes',v_intended_break
      ))
    );
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.weekly_manager_review_items
                where review_batch_id=v_batch.id and response_state='UNANSWERED') then
    update public.weekly_manager_review_batches set state='COMPLETE',
      completed_at_utc=pg_catalog.transaction_timestamp() where id=v_batch.id;
    update public.weekly_manager_route_receipts set state='COMPLETE' where id=v_receipt.id;
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'office_notification_count',v_notices,
    'remaining_count',(select pg_catalog.count(*) from public.weekly_manager_review_items
                       where review_batch_id=v_batch.id and response_state='UNANSWERED')
  );
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
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
    where (
        (command.state in ('READY','FAILED')
         and coalesce(command.next_attempt_at_utc,'-infinity'::timestamptz)<=pg_catalog.transaction_timestamp())
        or
        (command.state in ('LEASED','SUBMISSION_STARTED')
         and command.lease_expires_at_utc<=pg_catalog.transaction_timestamp())
      )
    order by command.next_attempt_at_utc nulls first,command.id
    limit v_limit
    for update skip locked
  ), claimed as (
    update public.weekly_message_dispatch_commands command
    set state=case when command.state='SUBMISSION_STARTED' then 'SUBMISSION_STARTED' else 'LEASED' end,
        lease_owner=v_worker,lease_token=v_token,
        lease_expires_at_utc=pg_catalog.transaction_timestamp()+pg_catalog.make_interval(secs=>v_lease_seconds)
    from eligible where command.id=eligible.id
    returning command.*
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'dispatch_command_id',claimed.id,'lease_token',claimed.lease_token,
      'lease_expires_at_utc',claimed.lease_expires_at_utc,
      'audience_kind',intent.audience_kind,'tranche_kind',claimed.tranche_kind,
      'candidate_id',candidate_generation.candidate_id,
      'manager_recipient',route.protected_recipient_address,
      'subject_text',render.subject_text,'html_body',render.html_body,
      'plain_body',render.plain_body,'provider_idempotency_key',render.provider_idempotency_key,
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

create or replace function public.weekly_source_message_dispatch_submission_start_atomic_v1(
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
  v_channel text;
  v_target_kind text;
  v_target_hash bytea;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_render public.weekly_message_renders%rowtype;
  v_attempt public.weekly_message_provider_attempts%rowtype;
  v_attempt_number integer;
  v_provider_key text;
  v_policy jsonb;
  v_member record;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'dispatch_command_id','lease_token','worker_id','channel','target_kind',
                 'keyed_target_fingerprint'
               )) then
    raise exception 'WEEKLY_SOURCE_DISPATCH_START_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_command_id:=(p_request->>'dispatch_command_id')::uuid;
    v_lease_token:=(p_request->>'lease_token')::uuid;
    v_worker:=p_request->>'worker_id';
    v_channel:=pg_catalog.upper(p_request->>'channel');
    v_target_kind:=pg_catalog.upper(p_request->>'target_kind');
    v_target_hash:=private.weekly_source_query_hex32_v1(
      p_request->>'keyed_target_fingerprint','WEEKLY_SOURCE_DISPATCH_TARGET_INVALID'
    );
  exception when others then
    raise exception 'WEEKLY_SOURCE_DISPATCH_START_REQUEST_INVALID' using errcode='22023';
  end;
  if v_channel not in ('PUSH','EMAIL') or v_target_kind not in (
    'CANDIDATE_DEVICE','MANAGER_ADDRESS','PACK_COPY_ADDRESS'
  ) or (v_channel='PUSH')<>(v_target_kind='CANDIDATE_DEVICE') then
    raise exception 'WEEKLY_SOURCE_DISPATCH_CHANNEL_INVALID' using errcode='22023';
  end if;
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_command_id for update;
  select * into strict v_intent from public.weekly_message_intents where id=v_command.message_intent_id;
  select * into strict v_render from public.weekly_message_renders where id=v_command.message_render_id for update;
  select * into v_attempt from public.weekly_message_provider_attempts
  where dispatch_command_id=v_command.id and keyed_target_fingerprint=v_target_hash
  order by attempt_number desc limit 1;
  if found and v_attempt.completed_at_utc is null
     and v_command.state in ('SUBMISSION_STARTED','ACCEPTED','AMBIGUOUS') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'provider_attempt_id',v_attempt.id,
      'provider_idempotency_key',v_attempt.provider_idempotency_key,
      'state',v_command.state
    );
  end if;
  if v_command.state<>'LEASED' or v_command.lease_token<>v_lease_token
     or v_command.lease_owner<>v_worker
     or v_command.lease_expires_at_utc<=pg_catalog.transaction_timestamp()
     or v_intent.state<>'RENDERED' or v_render.state not in ('CURRENT','SUBMISSION_STARTED') then
    raise exception 'WEEKLY_SOURCE_DISPATCH_LEASE_STALE' using errcode='40001';
  end if;
  begin
    perform private.weekly_source_query_current_publication_v1(
      v_command.source_cycle_id,v_command.projection_publication_id
    );
  exception when others then
    update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
      lease_token=null,lease_expires_at_utc=null where id=v_command.id;
    update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
    update public.weekly_message_renders set state='STALE' where id=v_render.id;
    return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','CURRENT_HOURS_CHANGED');
  end;
  if v_intent.audience_kind='CANDIDATE' then
    if not exists(
      select 1 from public.weekly_candidate_outreach_generations generation
      join public.weekly_candidate_cohorts cohort on cohort.id=generation.candidate_cohort_id
      where generation.id=v_command.candidate_generation_id and generation.state='ACTIVE'
        and cohort.current_generation_id=generation.id
    ) then
      update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
        lease_token=null,lease_expires_at_utc=null where id=v_command.id;
      update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
      update public.weekly_message_renders set state='STALE' where id=v_render.id;
      return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','REQUEST_REPLACED');
    end if;
    if not exists(
      select 1
      from public.weekly_candidate_outreach_memberships membership
      join public.weekly_discrepancy_incidents incident
        on incident.id=membership.incident_id
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
      update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
        lease_token=null,lease_expires_at_utc=null where id=v_command.id;
      update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
      update public.weekly_message_renders set state='STALE' where id=v_render.id;
      return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','REQUEST_RESOLVED');
    end if;
    for v_member in
      select incident.id as incident_id,null::uuid as client_id,
             null::uuid as contract_id,null::date as work_date
      from public.weekly_candidate_outreach_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.candidate_generation_id=v_command.candidate_generation_id
        and membership.state='ACTIONABLE'
      union all
      select null::uuid,membership.client_id,membership.contract_id,membership.week_ending
      from public.weekly_candidate_outreach_generations generation
      join public.weekly_timesheet_submission_requests submission
        on submission.candidate_cohort_id=generation.candidate_cohort_id
       and submission.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
      join public.weekly_timesheet_submission_request_memberships membership
        on membership.submission_request_id=submission.id and membership.state='WAITING'
      where generation.id=v_command.candidate_generation_id
        and generation.request_kind='SUBMIT_TIMESHEET'
    loop
      v_policy:=case when v_member.incident_id is not null
        then private.weekly_source_query_incident_policy_v1(v_member.incident_id)
        else private._weekly_source_effective_policy_v1(
          v_member.client_id,v_member.contract_id,v_member.work_date
        ) end;
      if coalesce((v_policy->>'candidate_queries_enabled')::boolean,false) is not true then
        update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
          lease_token=null,lease_expires_at_utc=null where id=v_command.id;
        update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
        update public.weekly_message_renders set state='STALE' where id=v_render.id;
        return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','CANDIDATE_CONTACT_DISABLED');
      end if;
    end loop;
  elsif v_intent.audience_kind='MANAGER' then
    if not exists(
      select 1 from public.weekly_manager_recipient_generations generation
      join public.weekly_manager_recipient_routes route on route.id=generation.recipient_route_id
      join public.weekly_manager_review_batches batch on batch.recipient_generation_id=generation.id
        and batch.message_render_id=v_render.id and batch.state='ACTIVE'
      where generation.id=v_intent.recipient_generation_id and generation.state='ACTIVE'
        and route.current_generation_id=generation.id
    ) then
      update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
        lease_token=null,lease_expires_at_utc=null where id=v_command.id;
      update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
      update public.weekly_message_renders set state='STALE' where id=v_render.id;
      return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','REVIEW_REPLACED');
    end if;
    if exists(
      select 1
      from public.weekly_manager_review_batches batch
      join public.weekly_manager_review_items item on item.review_batch_id=batch.id
      join public.weekly_discrepancy_incidents incident on incident.id=item.incident_id
      join public.weekly_issue_comparison_revisions comparison
        on comparison.id=incident.current_comparison_revision_id
      where batch.message_render_id=v_render.id
        and (
          item.response_state<>'UNANSWERED'
          or incident.state<>'OPEN'
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
    ) then
      update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
        lease_token=null,lease_expires_at_utc=null where id=v_command.id;
      update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
      update public.weekly_message_renders set state='STALE' where id=v_render.id;
      update public.weekly_manager_review_batches
      set state='REVOKED' where message_render_id=v_render.id and state='ACTIVE';
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
      return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','REVIEW_CHANGED');
    end if;
    for v_member in
      select incident.id
      from public.weekly_manager_review_items item
      join public.weekly_discrepancy_incidents incident on incident.id=item.incident_id
      where item.review_batch_id=(select id from public.weekly_manager_review_batches where message_render_id=v_render.id)
        and item.response_state='UNANSWERED'
    loop
      v_policy:=private.weekly_source_query_incident_policy_v1(v_member.id);
      if coalesce((v_policy->>'manager_queries_enabled')::boolean,false) is not true then
        update public.weekly_message_dispatch_commands set state='RETIRED',lease_owner=null,
          lease_token=null,lease_expires_at_utc=null where id=v_command.id;
        update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
        update public.weekly_message_renders set state='STALE' where id=v_render.id;
        return pg_catalog.jsonb_build_object('ok',false,'eligible',false,'reason','MANAGER_CONTACT_DISABLED');
      end if;
    end loop;
  end if;
  select coalesce(pg_catalog.max(attempt_number),0)+1 into v_attempt_number
  from public.weekly_message_provider_attempts
  where dispatch_command_id=v_command.id and keyed_target_fingerprint=v_target_hash;
  v_provider_key:=v_render.provider_idempotency_key||'/'||pg_catalog.encode(v_target_hash,'hex')||'/'||v_attempt_number::text;
  insert into public.weekly_message_provider_attempts(
    dispatch_command_id,attempt_number,channel,target_kind,keyed_target_fingerprint,
    provider_idempotency_key,started_at_utc
  ) values (
    v_command.id,v_attempt_number,v_channel,v_target_kind,v_target_hash,
    v_provider_key,pg_catalog.transaction_timestamp()
  ) returning * into v_attempt;
  update public.weekly_message_renders set state='SUBMISSION_STARTED' where id=v_render.id;
  update public.weekly_message_dispatch_commands
  set state='SUBMISSION_STARTED',attempt_count=attempt_count+1
  where id=v_command.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'provider_attempt_id',v_attempt.id,
    'provider_idempotency_key',v_attempt.provider_idempotency_key,
    'submission_started_at_utc',v_attempt.started_at_utc
  );
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
declare
  v_attempt_id uuid;
  v_outcome text;
  v_provider_message_id text;
  v_receipt jsonb;
  v_error jsonb;
  v_attempt public.weekly_message_provider_attempts%rowtype;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_render public.weekly_message_renders%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_result_hash bytea;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('provider_attempt_id','outcome','provider_message_id','receipt','error')) then
    raise exception 'WEEKLY_SOURCE_DISPATCH_RESULT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_attempt_id:=(p_request->>'provider_attempt_id')::uuid;
    v_outcome:=pg_catalog.upper(p_request->>'outcome');
    v_provider_message_id:=nullif(p_request->>'provider_message_id','');
    v_receipt:=coalesce(p_request->'receipt','{}'::jsonb);
    v_error:=coalesce(p_request->'error','{}'::jsonb);
  exception when others then
    raise exception 'WEEKLY_SOURCE_DISPATCH_RESULT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_outcome not in ('ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS')
     or pg_catalog.jsonb_typeof(v_receipt)<>'object' or pg_catalog.jsonb_typeof(v_error)<>'object'
     or pg_catalog.length(v_receipt::text)>20000 or pg_catalog.length(v_error::text)>20000
     or v_receipt ?| array['token','credential','password','secret','authorization']
     or v_error ?| array['token','credential','password','secret','authorization'] then
    raise exception 'WEEKLY_SOURCE_DISPATCH_RESULT_INVALID' using errcode='22023';
  end if;
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_PROVIDER_RESULT_V1',pg_catalog.jsonb_build_object(
      'provider_attempt_id',v_attempt_id,'outcome',v_outcome,
      'provider_message_id',v_provider_message_id,'receipt',v_receipt,'error',v_error
    )
  );
  select * into strict v_attempt from public.weekly_message_provider_attempts
  where id=v_attempt_id for update;
  if v_attempt.completed_at_utc is not null then
    if v_attempt.outcome<>v_outcome
       or v_attempt.bounded_provider_receipt_json->>'result_hash'<>pg_catalog.encode(v_result_hash,'hex') then
      raise exception 'WEEKLY_SOURCE_DISPATCH_RESULT_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object('ok',true,'replay',true,'outcome',v_attempt.outcome);
  end if;
  select * into strict v_command from public.weekly_message_dispatch_commands
  where id=v_attempt.dispatch_command_id for update;
  select * into strict v_render from public.weekly_message_renders where id=v_command.message_render_id for update;
  select * into strict v_intent from public.weekly_message_intents where id=v_command.message_intent_id for update;
  if v_command.state<>'SUBMISSION_STARTED' then
    raise exception 'WEEKLY_SOURCE_DISPATCH_RESULT_STATE_INVALID' using errcode='40001';
  end if;
  update public.weekly_message_provider_attempts
  set completed_at_utc=pg_catalog.transaction_timestamp(),outcome=v_outcome,
      bounded_provider_receipt_json=v_receipt||pg_catalog.jsonb_build_object(
        'result_hash',pg_catalog.encode(v_result_hash,'hex'),'provider_message_id',v_provider_message_id
      ),bounded_error_json=v_error
  where id=v_attempt.id;
  if v_outcome='ACCEPTED' then
    update public.weekly_message_dispatch_commands
    set state='ACCEPTED',provider_message_id=v_provider_message_id,
        provider_accepted_at_utc=pg_catalog.transaction_timestamp(),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_command.id;
    update public.weekly_message_renders set state='ACCEPTED' where id=v_render.id;
    update public.weekly_message_intents set state='DISPATCHED' where id=v_intent.id;
    if v_intent.audience_kind='MANAGER' then
      update public.weekly_discrepancy_incidents incident
      set manager_action_state='SENT'
      from public.weekly_manager_review_items item
      join public.weekly_manager_review_batches batch on batch.id=item.review_batch_id
      where batch.message_render_id=v_render.id and item.incident_id=incident.id
        and item.response_state='UNANSWERED' and incident.manager_action_state='DUE';
    elsif v_intent.audience_kind='CANDIDATE' then
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key
      )
      select incident.id,incident.episode_number,v_command.projection_publication_id,
        comparison.material_comparison_fingerprint,'REMINDER_SENT','SYSTEM',
        pg_catalog.jsonb_build_object('tranche_kind',v_command.tranche_kind),
        'CANDIDATE_MESSAGE_ACCEPTED:'||v_command.id::text||':'||incident.id::text
      from public.weekly_candidate_outreach_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
      where membership.candidate_generation_id=v_command.candidate_generation_id
        and membership.state='ACTIONABLE'
      on conflict (event_kind,idempotency_key) do nothing;
    end if;
  elsif v_outcome='AMBIGUOUS' then
    update public.weekly_message_dispatch_commands set state='AMBIGUOUS',lease_owner=null,
      lease_token=null,lease_expires_at_utc=null where id=v_command.id;
    update public.weekly_message_renders set state='AMBIGUOUS' where id=v_render.id;
  else
    update public.weekly_message_dispatch_commands
    set state=case when v_outcome='TRANSIENT_FAILURE' then 'FAILED' else 'RETIRED' end,
      next_attempt_at_utc=case when v_outcome='TRANSIENT_FAILURE'
      then pg_catalog.transaction_timestamp()+pg_catalog.make_interval(secs=>least(3600,30*(2^least(attempt_count,7))::integer))
      else null end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_command.id;
    update public.weekly_message_renders
    set state=case when v_outcome='TRANSIENT_FAILURE' then 'CURRENT' else 'DEFINITELY_REJECTED' end
    where id=v_render.id;
    if v_outcome='DEFINITELY_REJECTED' then
      update public.weekly_message_intents set state='RETIRED' where id=v_intent.id;
    end if;
  end if;
  return pg_catalog.jsonb_build_object('ok',true,'replay',false,'outcome',v_outcome);
end;
$function$;

create or replace function public.weekly_source_manager_route_prepare_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_intent_id uuid;
  v_publication_id uuid;
  v_intent public.weekly_message_intents%rowtype;
  v_generation public.weekly_manager_recipient_generations%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_input jsonb;
  v_membership_hash bytea;
  v_preparation public.weekly_manager_route_preparations%rowtype;
  v_sequence integer;
  v_credential_generation integer;
  v_issued_at timestamptz;
  v_secure_link_lifetime interval;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('message_intent_id','projection_publication_id')) then
    raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_intent_id:=(p_request->>'message_intent_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_REQUEST_INVALID' using errcode='22023';
  end;

  select * into strict v_intent from public.weekly_message_intents
  where id=v_intent_id for update;
  if v_intent.audience_kind<>'MANAGER' then
    raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_AUDIENCE_INVALID' using errcode='22023';
  end if;
  if v_intent.state='RENDERED' then
    select * into strict v_preparation
    from public.weekly_manager_route_preparations
    where message_intent_id=v_intent.id and state='BOUND';
    if v_preparation.projection_publication_id<>v_publication_id then
      raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    select * into strict v_route from public.weekly_manager_recipient_routes
    where id=v_intent.recipient_route_id;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'state',v_preparation.state,
      'manager_route_preparation_id',v_preparation.id,
      'review_batch_id',v_preparation.review_batch_id,
      'recipient_generation_id',v_preparation.recipient_generation_id,
      'credential_generation',v_preparation.credential_generation,
      'membership_hash',pg_catalog.encode(v_preparation.original_membership_hash,'hex'),
      'issued_at_utc',v_preparation.issued_at_utc,'expires_at_utc',v_preparation.expires_at_utc,
      'environment',v_route.environment,'agency_id',v_route.agency_id,
      'manager_address',v_route.protected_recipient_address
    );
  end if;
  if v_intent.state<>'DUE' then
    raise exception 'WEEKLY_SOURCE_MESSAGE_INTENT_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_query_current_publication_v1(v_intent.source_cycle_id,v_publication_id);
  select * into strict v_generation from public.weekly_manager_recipient_generations
  where id=v_intent.recipient_generation_id for update;
  select * into strict v_route from public.weekly_manager_recipient_routes
  where id=v_intent.recipient_route_id for update;
  if v_generation.state<>'ACTIVE' or v_route.current_generation_id<>v_generation.id
     or v_generation.recipient_route_id<>v_route.id then
    raise exception 'WEEKLY_SOURCE_MANAGER_MESSAGE_STALE' using errcode='40001';
  end if;

  v_input:=public.weekly_source_message_render_input_v1(pg_catalog.jsonb_build_object(
    'message_intent_id',v_intent.id,'projection_publication_id',v_publication_id
  ));
  v_membership_hash:=private.weekly_source_query_hex32_v1(
    v_input->>'membership_hash','WEEKLY_SOURCE_RENDER_MEMBERSHIP_HASH_INVALID'
  );

  select * into v_preparation
  from public.weekly_manager_route_preparations
  where message_intent_id=v_intent.id and state in ('PREPARED','BOUND')
  for update;
  if found then
    if v_preparation.state<>'PREPARED'
       or v_preparation.projection_publication_id<>v_publication_id
       or v_preparation.recipient_generation_id<>v_generation.id
       or v_preparation.original_membership_hash<>v_membership_hash then
      raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'state',v_preparation.state,
      'manager_route_preparation_id',v_preparation.id,
      'review_batch_id',v_preparation.review_batch_id,
      'recipient_generation_id',v_preparation.recipient_generation_id,
      'credential_generation',v_preparation.credential_generation,
      'membership_hash',pg_catalog.encode(v_preparation.original_membership_hash,'hex'),
      'issued_at_utc',v_preparation.issued_at_utc,'expires_at_utc',v_preparation.expires_at_utc,
      'environment',v_route.environment,'agency_id',v_route.agency_id,
      'manager_address',v_route.protected_recipient_address
    );
  end if;

  select coalesce(pg_catalog.max(preparation_sequence),0)+1 into v_sequence
  from public.weekly_manager_route_preparations where message_intent_id=v_intent.id;
  select coalesce(pg_catalog.max(number),0)+1 into v_credential_generation
  from (
    select batch.credential_generation as number
    from public.weekly_manager_review_batches batch
    where batch.recipient_generation_id=v_generation.id
    union all
    select preparation.credential_generation
    from public.weekly_manager_route_preparations preparation
    where preparation.recipient_generation_id=v_generation.id
  ) generations;
  v_issued_at:=pg_catalog.transaction_timestamp();
  select settings.manager_secure_link_lifetime
  into strict v_secure_link_lifetime
  from public.weekly_source_global_settings settings
  where settings.singleton;
  insert into public.weekly_manager_route_preparations(
    message_intent_id,projection_publication_id,recipient_generation_id,
    preparation_sequence,review_batch_id,credential_generation,
    original_membership_hash,issued_at_utc,expires_at_utc,state
  ) values (
    v_intent.id,v_publication_id,v_generation.id,v_sequence,
    pg_catalog.gen_random_uuid(),v_credential_generation,v_membership_hash,
    v_issued_at,v_issued_at+v_secure_link_lifetime,'PREPARED'
  ) returning * into v_preparation;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'state',v_preparation.state,
    'manager_route_preparation_id',v_preparation.id,
    'review_batch_id',v_preparation.review_batch_id,
    'recipient_generation_id',v_preparation.recipient_generation_id,
    'credential_generation',v_preparation.credential_generation,
    'membership_hash',pg_catalog.encode(v_preparation.original_membership_hash,'hex'),
    'issued_at_utc',v_preparation.issued_at_utc,'expires_at_utc',v_preparation.expires_at_utc,
    'environment',v_route.environment,'agency_id',v_route.agency_id,
    'manager_address',v_route.protected_recipient_address
  );
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARE_STALE' using errcode='40001';
end;
$function$;

create or replace function public.weekly_source_message_render_stage_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_intent_id uuid;
  v_publication_id uuid;
  v_intent public.weekly_message_intents%rowtype;
  v_subject text;
  v_html text;
  v_plain text;
  v_policy_version text;
  v_renderer_version text;
  v_structure_version text;
  v_membership jsonb;
  v_membership_hash bytea;
  v_expected_hash bytea;
  v_content_hash bytea;
  v_trigger_hash bytea;
  v_render public.weekly_message_renders%rowtype;
  v_command public.weekly_message_dispatch_commands%rowtype;
  v_render_sequence integer;
  v_generation public.weekly_manager_recipient_generations%rowtype;
  v_candidate_generation public.weekly_candidate_outreach_generations%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_batch public.weekly_manager_review_batches%rowtype;
  v_receipt public.weekly_manager_route_receipts%rowtype;
  v_preparation public.weekly_manager_route_preparations%rowtype;
  v_item jsonb;
  v_credential_hash bytea;
  v_credential_generation integer;
  v_control_ticket uuid;
  v_agency_receipt uuid;
  v_data_plane_identity text;
  v_route_version text;
  v_credential_version text;
  v_client_count integer;
  v_candidate_count integer;
  v_shift_count integer;
  v_provider_key text;
  v_command_key bytea;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'message_intent_id','projection_publication_id','membership_hash',
                 'policy_version','renderer_version','structure_version',
                 'subject_text','html_body','plain_body','expected_rendered_content_hash',
                 'credential_hash','control_plane_ticket_id','agency_receipt_id',
                 'data_plane_identity','route_version','credential_version',
                 'manager_route_preparation_id'
               )) then
    raise exception 'WEEKLY_SOURCE_MESSAGE_RENDER_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_intent_id:=(p_request->>'message_intent_id')::uuid;
    v_publication_id:=nullif(p_request->>'projection_publication_id','')::uuid;
    v_subject:=nullif(p_request->>'subject_text','');
    v_html:=nullif(p_request->>'html_body','');
    v_plain:=p_request->>'plain_body';
    v_policy_version:=p_request->>'policy_version';
    v_renderer_version:=p_request->>'renderer_version';
    v_structure_version:=nullif(p_request->>'structure_version','');
    v_expected_hash:=case when nullif(p_request->>'expected_rendered_content_hash','') is null
      then null else private.weekly_source_query_hex32_v1(
        p_request->>'expected_rendered_content_hash','WEEKLY_SOURCE_RENDER_HASH_INVALID'
      ) end;
  exception when others then
    raise exception 'WEEKLY_SOURCE_MESSAGE_RENDER_REQUEST_INVALID' using errcode='22023';
  end;
  if v_plain is null or v_policy_version is null or v_renderer_version is null
     or pg_catalog.length(v_plain)>200000 or pg_catalog.length(coalesce(v_html,''))>1000000
     or pg_catalog.length(coalesce(v_subject,''))>300
     or (coalesce(v_subject,'')||' '||coalesce(v_html,'')||' '||v_plain)
        ~* '(^|[^[:alnum:]_])(pay|charge|rate|vat|invoice|banking|import|source|fingerprint|generation|incident|projection)([^[:alnum:]_]|$)' then
    raise exception 'WEEKLY_SOURCE_MESSAGE_CONTENT_INVALID' using errcode='22023';
  end if;
  select * into strict v_intent from public.weekly_message_intents
  where id=v_intent_id for update;
  if v_intent.state='RENDERED' then
    select render.* into strict v_render from public.weekly_message_renders render
    where render.message_intent_id=v_intent.id and render.state='CURRENT';
    select command.* into strict v_command from public.weekly_message_dispatch_commands command
    where command.message_render_id=v_render.id and command.state<>'RETIRED';
    if v_render.projection_publication_id is distinct from v_publication_id
       or v_render.membership_hash<>private.weekly_source_query_hex32_v1(
         p_request->>'membership_hash','WEEKLY_SOURCE_RENDER_MEMBERSHIP_HASH_INVALID'
       )
       or v_render.policy_version<>v_policy_version
       or v_render.renderer_version<>v_renderer_version
       or v_render.subject_text is distinct from v_subject
       or v_render.html_body is distinct from v_html
       or v_render.plain_body is distinct from v_plain
       or (v_expected_hash is not null and v_render.rendered_content_hash<>v_expected_hash) then
      raise exception 'WEEKLY_SOURCE_MESSAGE_RENDER_REPLAY_CONFLICT' using errcode='23505';
    end if;
    if v_intent.audience_kind='MANAGER' then
      begin
        select * into strict v_preparation
        from public.weekly_manager_route_preparations
        where id=(p_request->>'manager_route_preparation_id')::uuid
          and message_intent_id=v_intent.id and state='BOUND';
        v_credential_hash:=private.weekly_source_query_hex32_v1(
          p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
        );
        v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
        v_agency_receipt:=(p_request->>'agency_receipt_id')::uuid;
        v_data_plane_identity:=p_request->>'data_plane_identity';
        v_route_version:=p_request->>'route_version';
        v_credential_version:=p_request->>'credential_version';
      exception when others then
        raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_AUTHORITY_INVALID' using errcode='22023';
      end;
      select * into strict v_batch from public.weekly_manager_review_batches
      where message_render_id=v_render.id;
      select * into strict v_receipt from public.weekly_manager_route_receipts
      where review_batch_id=v_batch.id;
      if v_batch.policy_version<>v_policy_version
         or v_batch.renderer_version<>v_renderer_version
         or v_batch.structure_version is distinct from v_structure_version
         or v_batch.control_plane_ticket_id<>v_control_ticket
         or v_batch.agency_receipt_id<>v_agency_receipt
         or v_batch.opaque_credential_hash<>v_credential_hash
         or v_batch.id<>v_preparation.review_batch_id
         or v_batch.credential_generation<>v_preparation.credential_generation
         or v_batch.original_membership_hash<>v_preparation.original_membership_hash
         or v_receipt.data_plane_identity<>v_data_plane_identity
         or v_receipt.route_version<>v_route_version
         or v_receipt.credential_version<>v_credential_version then
        raise exception 'WEEKLY_SOURCE_MESSAGE_RENDER_REPLAY_CONFLICT' using errcode='23505';
      end if;
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'message_render_id',v_render.id,
      'dispatch_command_id',v_command.id,
      'review_batch_id',(select batch.id from public.weekly_manager_review_batches batch
                         where batch.message_render_id=v_render.id)
    );
  end if;
  if v_intent.state<>'DUE' then
    raise exception 'WEEKLY_SOURCE_MESSAGE_INTENT_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_query_current_publication_v1(v_intent.source_cycle_id,v_publication_id);

  if v_intent.audience_kind='CANDIDATE' then
    select * into strict v_candidate_generation
    from public.weekly_candidate_outreach_generations
    where id=v_intent.candidate_generation_id;
    if v_candidate_generation.state<>'ACTIVE'
       or not exists(select 1 from public.weekly_candidate_cohorts cohort
                     where cohort.id=v_candidate_generation.candidate_cohort_id
                       and cohort.current_generation_id=v_candidate_generation.id) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_MESSAGE_STALE' using errcode='40001';
    end if;
    v_membership_hash:=v_candidate_generation.membership_hash;
    if v_subject is not null or v_html is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_CONTENT_INVALID' using errcode='22023';
    end if;
  elsif v_intent.audience_kind='MANAGER' then
    select * into strict v_generation from public.weekly_manager_recipient_generations
    where id=v_intent.recipient_generation_id;
    select * into strict v_route from public.weekly_manager_recipient_routes
    where id=v_intent.recipient_route_id for update;
    if v_generation.state<>'ACTIVE' or v_route.current_generation_id<>v_generation.id then
      raise exception 'WEEKLY_SOURCE_MANAGER_MESSAGE_STALE' using errcode='40001';
    end if;
    if v_policy_version<>'1.7.0' or v_renderer_version<>'1.3.0'
       or v_structure_version<>'1.0.0' or v_subject is null or v_html is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_RENDER_POLICY_INVALID' using errcode='22023';
    end if;
    v_membership:=coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'incident_id',ranked.incident_id,
          'incident_episode',ranked.episode_number,
          'comparison_revision_id',ranked.comparison_revision_id,
          'comparison_fingerprint',pg_catalog.encode(ranked.comparison_fingerprint,'hex'),
          'client_order',ranked.client_order,
          'candidate_order',ranked.candidate_order,
          'shift_order',ranked.shift_order
        ) order by ranked.client_order,ranked.candidate_order,ranked.shift_order
      )
      from (
        select incident.id as incident_id,incident.episode_number,
          comparison.id as comparison_revision_id,
          comparison.material_comparison_fingerprint as comparison_fingerprint,
          pg_catalog.dense_rank() over(order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)::integer as client_order,
          pg_catalog.dense_rank() over(partition by client.id order by private.weekly_source_query_ascii_fold_v1(coalesce(nullif(candidate.display_name,''),pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name))) collate "C",candidate.id)::integer as candidate_order,
          pg_catalog.row_number() over(partition by client.id,candidate.id order by work_event.work_date,coalesce(comparison.system_start_at_local,comparison.candidate_start_at_local),incident.id)::integer as shift_order
        from public.weekly_manager_recipient_memberships membership
        join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        join public.clients client on client.id=incident.client_id
        join public.candidates candidate on candidate.id=incident.candidate_id
        where membership.recipient_generation_id=v_generation.id
          and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
          and (
            coalesce(pg_catalog.array_length(v_intent.sorted_due_event_ids,1),0)=0
            or membership.candidate_cohort_id=any(
              select event.candidate_cohort_id from public.weekly_manager_cohort_due_events event
              where event.id=any(v_intent.sorted_due_event_ids)
                and event.recipient_generation_id=v_generation.id
            )
          )
          and (v_intent.tranche_kind<>'MANAGER_T6_RESPONDED' or incident.candidate_action_state='RESPONDED')
          and not private.weekly_source_query_manager_row_owned_v1(
            v_generation.recipient_route_id,incident.id,incident.episode_number
          )
      ) ranked
    ),'[]'::jsonb);
    v_shift_count:=pg_catalog.jsonb_array_length(v_membership);
    if v_shift_count=0 then
      raise exception 'WEEKLY_SOURCE_MANAGER_RENDER_EMPTY' using errcode='55000';
    end if;
    select pg_catalog.count(distinct incident.client_id),
           pg_catalog.count(distinct incident.candidate_id)
    into v_client_count,v_candidate_count
    from pg_catalog.jsonb_array_elements(v_membership) member
    join public.weekly_discrepancy_incidents incident on incident.id=(member->>'incident_id')::uuid;
    if v_client_count>100 or v_candidate_count>100 or v_shift_count>500 then
      raise exception 'WEEKLY_SOURCE_MANAGER_DIGEST_CAPACITY_EXCEEDED' using errcode='54000';
    end if;
    if v_subject<>pg_catalog.format(
      'Timesheet queries requiring your review - %s %s',v_shift_count,
      case when v_shift_count=1 then 'shift' else 'shifts' end
    ) then
      raise exception 'WEEKLY_SOURCE_MANAGER_SUBJECT_INVALID' using errcode='22023';
    end if;
    v_membership_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_MANAGER_REVIEW_BATCH_MEMBERSHIP_V1',v_membership
    );
    begin
      select * into strict v_preparation
      from public.weekly_manager_route_preparations
      where id=(p_request->>'manager_route_preparation_id')::uuid
        and message_intent_id=v_intent.id and state='PREPARED'
      for update;
      v_credential_hash:=private.weekly_source_query_hex32_v1(
        p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
      );
      v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
      v_agency_receipt:=(p_request->>'agency_receipt_id')::uuid;
      v_data_plane_identity:=p_request->>'data_plane_identity';
      v_route_version:=p_request->>'route_version';
      v_credential_version:=p_request->>'credential_version';
    exception when others then
      raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_AUTHORITY_INVALID' using errcode='22023';
    end;
    if nullif(v_data_plane_identity,'') is null or nullif(v_route_version,'') is null
       or nullif(v_credential_version,'') is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_AUTHORITY_INVALID' using errcode='22023';
    end if;
    if v_preparation.projection_publication_id<>v_publication_id
       or v_preparation.recipient_generation_id<>v_generation.id
       or v_preparation.original_membership_hash<>v_membership_hash
       or v_preparation.expires_at_utc<=pg_catalog.transaction_timestamp() then
      raise exception 'WEEKLY_SOURCE_MANAGER_ROUTE_PREPARATION_STALE' using errcode='40001';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_MESSAGE_AUDIENCE_UNSUPPORTED' using errcode='55000';
  end if;
  if v_membership_hash<>private.weekly_source_query_hex32_v1(
    p_request->>'membership_hash','WEEKLY_SOURCE_RENDER_MEMBERSHIP_HASH_INVALID'
  ) then
    raise exception 'WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE' using errcode='40001';
  end if;
  v_content_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_RENDER_V1',pg_catalog.jsonb_build_object(
      'message_intent_id',v_intent.id,'projection_publication_id',v_publication_id,
      'membership_hash',pg_catalog.encode(v_membership_hash,'hex'),
      'policy_version',v_policy_version,'renderer_version',v_renderer_version,
      'structure_version',v_structure_version,'subject_text',v_subject,
      'html_body',v_html,'plain_body',v_plain
    )
  );
  if v_expected_hash is not null and v_expected_hash<>v_content_hash then
    raise exception 'WEEKLY_SOURCE_RENDER_HASH_MISMATCH' using errcode='40001';
  end if;
  select coalesce(pg_catalog.max(render_sequence),0)+1 into v_render_sequence
  from public.weekly_message_renders where message_intent_id=v_intent.id;
  v_provider_key:='weekly-source/'||pg_catalog.encode(v_content_hash,'hex');
  insert into public.weekly_message_renders(
    message_intent_id,render_sequence,projection_publication_id,membership_hash,
    policy_version,renderer_version,subject_text,html_body,plain_body,
    rendered_content_hash,provider_idempotency_key,state
  ) values (
    v_intent.id,v_render_sequence,v_publication_id,v_membership_hash,
    v_policy_version,v_renderer_version,v_subject,v_html,v_plain,
    v_content_hash,v_provider_key,'CURRENT'
  ) returning * into v_render;

  if v_intent.audience_kind='MANAGER' then
    v_credential_generation:=v_preparation.credential_generation;
    insert into public.weekly_manager_review_batches(
      id,recipient_generation_id,message_render_id,policy_version,renderer_version,
      structure_version,sent_content_hash,original_membership_hash,
      control_plane_ticket_id,agency_receipt_id,credential_generation,
      opaque_credential_hash,issued_at_utc,expires_at_utc,state,send_sequence
    ) values (
      v_preparation.review_batch_id,v_generation.id,v_render.id,v_policy_version,v_renderer_version,
      v_structure_version,v_content_hash,v_membership_hash,
      v_control_ticket,v_agency_receipt,v_credential_generation,v_credential_hash,
      v_preparation.issued_at_utc,v_preparation.expires_at_utc,
      'ACTIVE',v_intent.tranche_sequence
    ) returning * into v_batch;
    for v_item in select value from pg_catalog.jsonb_array_elements(v_membership)
    loop
      insert into public.weekly_manager_review_items(
        review_batch_id,incident_id,incident_episode,sent_comparison_revision_id,
        sent_comparison_fingerprint,client_order,candidate_order,shift_order,response_state
      ) values (
        v_batch.id,(v_item->>'incident_id')::uuid,(v_item->>'incident_episode')::integer,
        (v_item->>'comparison_revision_id')::uuid,
        pg_catalog.decode(v_item->>'comparison_fingerprint','hex'),
        (v_item->>'client_order')::integer,(v_item->>'candidate_order')::integer,
        (v_item->>'shift_order')::integer,'UNANSWERED'
      );
    end loop;
    insert into public.weekly_manager_route_receipts(
      review_batch_id,recipient_generation_id,control_plane_ticket_id,environment,
      agency_id,data_plane_identity,route_version,credential_version,
      original_membership_hash,credential_generation,credential_hash,
      issued_at_utc,expires_at_utc,semantic_hash,state
    ) values (
      v_batch.id,v_generation.id,v_control_ticket,v_route.environment,v_route.agency_id,
      v_data_plane_identity,v_route_version,v_credential_version,v_membership_hash,
      v_credential_generation,v_credential_hash,v_batch.issued_at_utc,v_batch.expires_at_utc,
      private.weekly_source_sha256_jsonb_v1('WEEKLY_MANAGER_ROUTE_RECEIPT_V1',
        pg_catalog.jsonb_build_object(
          'review_batch_id',v_batch.id,'recipient_generation_id',v_generation.id,
          'control_plane_ticket_id',v_control_ticket,'agency_receipt_id',v_agency_receipt,
          'membership_hash',pg_catalog.encode(v_membership_hash,'hex'),
          'credential_generation',v_credential_generation
        )),'ACTIVE'
    ) returning * into v_receipt;
    update public.weekly_manager_route_preparations
    set state='BOUND',bound_at_utc=pg_catalog.transaction_timestamp()
    where id=v_preparation.id;
  end if;
  v_trigger_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_TRIGGER_EVENTS_V1',pg_catalog.to_jsonb(v_intent.sorted_due_event_ids)
  );
  v_command_key:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_DISPATCH_COMMAND_V1',pg_catalog.jsonb_build_object(
      'message_intent_id',v_intent.id,'message_render_id',v_render.id,
      'rendered_content_hash',pg_catalog.encode(v_content_hash,'hex')
    )
  );
  insert into public.weekly_message_dispatch_commands(
    message_intent_id,message_render_id,environment,agency_id,source_cycle_id,
    recipient_route_id,candidate_generation_id,tranche_kind,tranche_sequence,
    sorted_trigger_event_hash,membership_hash,policy_version,logical_key,state,
    next_attempt_at_utc,projection_publication_id,rendered_content_hash
  ) values (
    v_intent.id,v_render.id,v_intent.environment,v_intent.agency_id,v_intent.source_cycle_id,
    v_intent.recipient_route_id,v_intent.candidate_generation_id,v_intent.tranche_kind,
    v_intent.tranche_sequence,v_trigger_hash,v_membership_hash,v_policy_version,
    v_command_key,'READY',pg_catalog.transaction_timestamp(),v_publication_id,v_content_hash
  ) returning * into v_command;
  update public.weekly_message_intents set state='RENDERED' where id=v_intent.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'message_render_id',v_render.id,
    'rendered_content_hash',pg_catalog.encode(v_content_hash,'hex'),
    'dispatch_command_id',v_command.id,'review_batch_id',v_batch.id,
    'manager_route_receipt_id',v_receipt.id,
    'manager_route_preparation_id',v_preparation.id
  );
end;
$function$;

create or replace function public.weekly_source_query_scheduler_tick_v1(
  p_request jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_now timestamptz;
  v_limit integer;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_tranche text;
  v_intent_id uuid;
  v_candidate_count integer:=0;
  v_overdue_count integer:=0;
  v_manager_count integer:=0;
  v_route record;
  v_event_ids uuid[];
  v_due_kind text;
  v_manager_tranche text;
  v_actionable integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('now_utc','limit')) then
    raise exception 'WEEKLY_SOURCE_SCHEDULER_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_now:=coalesce(nullif(p_request->>'now_utc','')::timestamptz,pg_catalog.transaction_timestamp());
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,250);
  exception when others then
    raise exception 'WEEKLY_SOURCE_SCHEDULER_REQUEST_INVALID' using errcode='22023';
  end;
  if v_limit<1 or v_limit>1000 then
    raise exception 'WEEKLY_SOURCE_SCHEDULER_LIMIT_INVALID' using errcode='22023';
  end if;

  for v_generation in
    select generation.*
    from public.weekly_candidate_outreach_generations generation
    where generation.state='ACTIVE' and generation.reminder_due_at_utc<=v_now
      and exists(
        select 1 from public.weekly_message_intents intent
        where intent.candidate_generation_id=generation.id
          and intent.tranche_kind in ('CANDIDATE_INITIAL','TIMESHEET_SUBMISSION_INITIAL')
      )
      and not exists(
        select 1 from public.weekly_message_intents intent
        where intent.candidate_generation_id=generation.id
          and intent.tranche_kind in ('CANDIDATE_REMINDER_6H','TIMESHEET_SUBMISSION_REMINDER_6H')
      )
      and (
        (generation.request_kind='CHECK_HOURS' and exists(
          select 1 from public.weekly_candidate_outreach_memberships membership
          where membership.candidate_generation_id=generation.id and membership.state='ACTIONABLE'
        ))
        or
        (generation.request_kind='SUBMIT_TIMESHEET' and exists(
          select 1 from public.weekly_timesheet_submission_requests submission
          where submission.candidate_cohort_id=generation.candidate_cohort_id
            and submission.state in ('ACTIVE','PARTLY_SUBMITTED')
        ))
      )
    order by generation.reminder_due_at_utc,generation.id
    limit v_limit
    for update skip locked
  loop
    v_tranche:=case when v_generation.request_kind='SUBMIT_TIMESHEET'
      then 'TIMESHEET_SUBMISSION_REMINDER_6H' else 'CANDIDATE_REMINDER_6H' end;
    v_intent_id:=private.weekly_source_query_candidate_intent_v1(
      v_generation.id,v_tranche,1,v_generation.reminder_due_at_utc
    );
    if v_generation.request_kind='CHECK_HOURS' then
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key,occurred_at_utc
      )
      select incident.id,incident.episode_number,comparison.projection_publication_id,
        comparison.material_comparison_fingerprint,'REMINDER_DUE','SYSTEM',
        pg_catalog.jsonb_build_object('candidate_generation_id',v_generation.id),
        'CANDIDATE_REMINDER_DUE:'||v_generation.id::text||':'||incident.id::text,
        v_generation.reminder_due_at_utc
      from public.weekly_candidate_outreach_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
      where membership.candidate_generation_id=v_generation.id and membership.state='ACTIONABLE'
      on conflict (event_kind,idempotency_key) do nothing;
    end if;
    v_candidate_count:=v_candidate_count+1;
  end loop;

  update public.weekly_timesheet_submission_requests submission
  set state='OVERDUE',updated_at_utc=v_now
  where submission.state in ('ACTIVE','PARTLY_SUBMITTED') and submission.deadline_at_utc<=v_now;
  get diagnostics v_overdue_count=row_count;

  for v_route in
    select route.id as route_id,generation.id as generation_id,event.event_kind,
           pg_catalog.min(event.due_at_utc) as first_due
    from public.weekly_manager_recipient_routes route
    join public.weekly_manager_recipient_generations generation
      on generation.id=route.current_generation_id and generation.state='ACTIVE'
    join public.weekly_manager_cohort_due_events event
      on event.recipient_generation_id=generation.id
     and event.state='PENDING' and event.due_at_utc<=v_now
    group by route.id,generation.id,event.event_kind
    order by pg_catalog.min(event.due_at_utc),route.id,event.event_kind
    limit v_limit
  loop
    if exists(
      select 1 from public.weekly_message_intents intent
      where intent.recipient_generation_id=v_route.generation_id
        and intent.state in ('DUE','RENDERED')
    ) then
      continue;
    end if;
    v_due_kind:=v_route.event_kind;
    select pg_catalog.array_agg(locked_event.id order by locked_event.due_at_utc,locked_event.id)
    into v_event_ids
    from (
      select event.id,event.due_at_utc
      from public.weekly_manager_cohort_due_events event
      where event.recipient_generation_id=v_route.generation_id
        and event.event_kind=v_due_kind and event.state='PENDING' and event.due_at_utc<=v_now
      order by event.due_at_utc,event.id
      for update
    ) locked_event;
    if coalesce(pg_catalog.array_length(v_event_ids,1),0)=0 then continue; end if;
    select pg_catalog.count(*) into v_actionable
    from public.weekly_manager_recipient_memberships membership
    join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
    where membership.recipient_generation_id=v_route.generation_id
      and membership.candidate_cohort_id=any(
        select event.candidate_cohort_id
        from public.weekly_manager_cohort_due_events event where event.id=any(v_event_ids)
      )
      and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
      and (
        v_due_kind<>'T6_RESPONDED'
        or incident.candidate_action_state='RESPONDED'
      )
      and not private.weekly_source_query_manager_row_owned_v1(
        v_route.route_id,incident.id,incident.episode_number
      );
    if v_actionable>0 then
      v_manager_tranche:=case v_due_kind
        when 'EARLY_ALL' then 'MANAGER_EARLY_ALL'
        when 'T6_RESPONDED' then 'MANAGER_T6_RESPONDED'
        else 'MANAGER_T12_REMAINDER' end;
      v_intent_id:=private.weekly_source_query_manager_intent_v1(
        v_route.route_id,v_route.generation_id,v_event_ids,v_manager_tranche,1,v_route.first_due
      );
      -- 04A section 6.1 makes only an "unanswered and unsent" row due.  A row an
      -- accepted batch still owns is neither, so it keeps its SENT state and the
      -- Office screen keeps saying `Manager informed`.
      update public.weekly_discrepancy_incidents incident set manager_action_state='DUE'
      from public.weekly_manager_recipient_memberships membership
      where membership.recipient_generation_id=v_route.generation_id
        and membership.incident_id=incident.id
        and membership.candidate_cohort_id=any(
          select event.candidate_cohort_id from public.weekly_manager_cohort_due_events event
          where event.id=any(v_event_ids)
        ) and incident.manager_action_state in ('NOT_SENT','SENT')
        and not private.weekly_source_query_manager_row_owned_v1(
          v_route.route_id,incident.id,incident.episode_number
        );
      v_manager_count:=v_manager_count+1;
    end if;
    update public.weekly_manager_cohort_due_events set state='CONSUMED'
    where id=any(v_event_ids);
  end loop;
  return pg_catalog.jsonb_build_object(
    'ok',true,'now_utc',v_now,'candidate_reminder_intents',v_candidate_count,
    'timesheet_requests_marked_overdue',v_overdue_count,
    'manager_digest_intents',v_manager_count
  );
end;
$function$;

create or replace function public.weekly_source_candidate_response_submit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_candidate_id uuid;
  v_generation_id uuid;
  v_publication_id uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_item jsonb;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_membership public.weekly_candidate_outreach_memberships%rowtype;
  v_choice text;
  v_expected bytea;
  v_current_timesheet_hash bytea;
  v_response_hash bytea;
  v_draft public.weekly_candidate_response_drafts%rowtype;
  v_draft_version integer;
  v_event_id uuid;
  v_answered integer:=0;
  v_resolved integer:=0;
  v_notices integer:=0;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_summary text;
  v_recipient_generation_id uuid;
  v_recipient_route_id uuid;
  v_due_ids uuid[];
  v_intent_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'candidate_id','candidate_generation_id','projection_publication_id',
                 'request_idempotency_key','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end if;
  begin
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_responses:=(
      select pg_catalog.jsonb_agg(value order by (value->>'incident_id')::uuid)
      from pg_catalog.jsonb_array_elements(p_request->'responses')
    );
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_CANDIDATE_RESPONSE_REQUEST_V1',
    pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'candidate_generation_id',v_generation_id,
      'projection_publication_id',v_publication_id,'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');

  -- Exact durable replay is deliberately checked before any mutable generation,
  -- publication, Timesheet or incident test.
  select pg_catalog.count(*),
         min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='CANDIDATE_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;

  select * into strict v_generation
  from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;
  if v_generation.state<>'ACTIVE' or v_generation.request_kind<>'CHECK_HOURS'
     or v_generation.candidate_id<>v_candidate_id
     or pg_catalog.transaction_timestamp()>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cohort from public.weekly_candidate_cohorts
  where id=v_generation.candidate_cohort_id for update;
  if v_cohort.current_generation_id<>v_generation.id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_candidate from public.candidates where id=v_candidate_id;
  select coalesce(pg_catalog.max(draft_version),0)+1 into v_draft_version
  from public.weekly_candidate_response_drafts where candidate_generation_id=v_generation.id;
  insert into public.weekly_candidate_response_drafts(
    candidate_generation_id,candidate_id,draft_version,current_projection_publication_id,
    state,draft_hash,submitted_at_utc
  ) values (
    v_generation.id,v_candidate_id,v_draft_version,v_publication_id,
    'SUBMITTED',v_semantic_hash,pg_catalog.transaction_timestamp()
  ) returning * into v_draft;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'incident_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_item) key
                 where key not in (
                   'incident_id','expected_comparison_fingerprint','expected_timesheet_hash',
                   'choice','corrected_start_at_local','corrected_end_at_local','corrected_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=(v_item->>'incident_id')::uuid for update;
      select * into strict v_membership from public.weekly_candidate_outreach_memberships
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id
        and state='ACTIONABLE' for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    if v_incident.state<>'OPEN' or v_incident.candidate_id<>v_candidate_id
       or v_incident.source_cycle_id<>v_cycle.id then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_item->>'expected_comparison_fingerprint',
      'WEEKLY_SOURCE_CANDIDATE_RESPONSE_FINGERPRINT_INVALID'
    );
    if v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_current_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(
      v_comparison.candidate_timesheet_id
    );
    if v_current_timesheet_hash is null
       or v_current_timesheet_hash<>private.weekly_source_query_hex32_v1(
         v_item->>'expected_timesheet_hash','WEEKLY_SOURCE_CANDIDATE_TIMESHEET_HASH_INVALID'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
    end if;
    v_choice:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_item->>'choice','')));
    if v_choice not in ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT') then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_CHOICE_INVALID' using errcode='22023';
    end if;
    if (
      nullif(v_item->>'corrected_start_at_local','') is null
      or nullif(v_item->>'corrected_end_at_local','') is null
      or nullif(v_item->>'corrected_break_minutes','') is null
    ) and not (
      nullif(v_item->>'corrected_start_at_local','') is null
      and nullif(v_item->>'corrected_end_at_local','') is null
      and nullif(v_item->>'corrected_break_minutes','') is null
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_NOT_ALLOWED' using errcode='22023';
    end if;
    if v_choice='NEITHER_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_WRONG'
       and nullif(v_item->>'corrected_start_at_local','') is null
       and v_comparison.source_presence<>'ABSENT' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if nullif(v_item->>'corrected_start_at_local','') is not null and (
      nullif(v_item->>'corrected_break_minutes','')::integer<0
      or nullif(v_item->>'corrected_end_at_local','')::timestamp
         <=nullif(v_item->>'corrected_start_at_local','')::timestamp
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_query_response_fingerprint_v1(
      v_incident.id,v_comparison.id,v_candidate_id,v_draft_version,v_request_key,
      private.weekly_source_sha256_jsonb_v1('WEEKLY_CANDIDATE_RESPONSE_ITEM_V1',v_item)
    );
    insert into public.weekly_candidate_response_draft_items(
      response_draft_id,incident_id,comparison_revision_id,choice,
      corrected_start_at_local,corrected_end_at_local,corrected_break_minutes,
      response_fingerprint
    ) values (
      v_draft.id,v_incident.id,v_comparison.id,v_choice,
      nullif(v_item->>'corrected_start_at_local','')::timestamp,
      nullif(v_item->>'corrected_end_at_local','')::timestamp,
      nullif(v_item->>'corrected_break_minutes','')::integer,v_response_hash
    );
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'CANDIDATE_RESPONDED','CANDIDATE',
      pg_catalog.jsonb_build_object(
        'choice',v_choice,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,
        'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses)
      ),'CANDIDATE_RESPONSE:'||v_request_key::text||':'||v_incident.id::text
    ) returning id into v_event_id;
    if v_choice='CANDIDATE_WRONG' then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state='RESPONDED',manager_potential_state='NOT_REQUIRED',
          manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
          resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind='CANDIDATE_CORRECTED'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
      where incident_id=v_incident.id and response_state='UNANSWERED';
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication_id,
        v_comparison.material_comparison_fingerprint,'RESOLVED','CANDIDATE',
        pg_catalog.jsonb_build_object('resolution','CANDIDATE_CORRECTED'),
        'CANDIDATE_CORRECTED:'||v_event_id::text
      );
      v_resolved:=v_resolved+1;
    else
      v_policy:=private.weekly_source_query_incident_policy_v1(v_incident.id);
      update public.weekly_discrepancy_incidents
      set candidate_action_state='RESPONDED',
          manager_potential_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'AVAILABLE'
            else 'NOT_AVAILABLE'
          end,
          manager_action_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'DUE'
            else 'NOT_REQUIRED'
          end,
          reconciliation_state='WAITING_FOR_SOURCE'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='ANSWERED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
      select * into strict v_client from public.clients where id=v_incident.client_id;
      v_summary:=case
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
          then 'Shift missing or not yet authorised'
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
          then 'Shift missing from system hours'
        else 'System hours need review'
      end;
      v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
        v_incident.id,v_event_id,'WEEKLY_CANDIDATE_SOURCE_DISPUTED',
        pg_catalog.jsonb_build_object(
          'candidate_name',coalesce(nullif(v_candidate.display_name,''),
            pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
          'client_name',v_client.name,'work_date',v_work.work_date,
          'issue_summary',v_summary,'candidate_choice',v_choice
        )
      );
    end if;
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(
    select 1 from public.weekly_candidate_outreach_memberships
    where candidate_generation_id=v_generation.id and state='ACTIONABLE'
  ) then
    update public.weekly_candidate_outreach_generations set state='COMPLETE'
    where id=v_generation.id;
    update public.weekly_message_intents set state='RETIRED'
    where candidate_generation_id=v_generation.id and state in ('DUE','RENDERED');

    select generation.id,generation.recipient_route_id
      into v_recipient_generation_id,v_recipient_route_id
    from public.weekly_manager_recipient_memberships membership
    join public.weekly_manager_recipient_generations generation
      on generation.id=membership.recipient_generation_id and generation.state='ACTIVE'
    join public.weekly_manager_recipient_routes route
      on route.id=generation.recipient_route_id
     and route.current_generation_id=generation.id
    where membership.candidate_cohort_id=v_cohort.id
    order by generation.generation_number desc
    limit 1
    for update of generation;
    if v_recipient_generation_id is not null
       and not exists(
         select 1
         from public.weekly_manager_recipient_memberships route_membership
         join public.weekly_route_activations activation
           on activation.id=route_membership.route_activation_id
          and activation.route_mode='CANDIDATE_FIRST'
         join public.weekly_candidate_cohorts candidate_cohort
           on candidate_cohort.id=route_membership.candidate_cohort_id
         join public.weekly_candidate_outreach_memberships candidate_membership
           on candidate_membership.candidate_generation_id=candidate_cohort.current_generation_id
          and candidate_membership.state='ACTIONABLE'
         where route_membership.recipient_generation_id=v_recipient_generation_id
       ) and exists(
      -- 04A section 6.1: only a row that is unanswered AND unsent becomes due.
      -- A row an accepted batch on this route already owns is not work, so it
      -- must not raise an EARLY_ALL event whose render would then be empty.
      select 1 from public.weekly_manager_recipient_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
        and not private.weekly_source_query_manager_row_owned_v1(
          v_recipient_route_id,incident.id,incident.episode_number
        )
    ) then
      insert into public.weekly_manager_cohort_due_events(
        recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
        due_at_utc,state,trigger_hash
      ) select distinct
        v_recipient_generation_id,membership.candidate_cohort_id,'EARLY_ALL',
        candidate_generation.started_at_utc,pg_catalog.transaction_timestamp(),'PENDING',
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_MANAGER_COHORT_DUE_V1',pg_catalog.jsonb_build_object(
            'recipient_generation_id',v_recipient_generation_id,
            'candidate_cohort_id',membership.candidate_cohort_id,
            'event_kind','EARLY_ALL',
            'candidate_generation_id',candidate_generation.id
          )
        )
      from public.weekly_manager_recipient_memberships membership
      join public.weekly_route_activations activation
        on activation.id=membership.route_activation_id
       and activation.route_mode='CANDIDATE_FIRST'
      join public.weekly_candidate_cohorts candidate_cohort
        on candidate_cohort.id=membership.candidate_cohort_id
      join public.weekly_candidate_outreach_generations candidate_generation
        on candidate_generation.id=candidate_cohort.current_generation_id
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
      on conflict (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash) do nothing;
      select pg_catalog.array_agg(id order by id) into v_due_ids
      from public.weekly_manager_cohort_due_events
      where recipient_generation_id=v_recipient_generation_id
        and event_kind='EARLY_ALL' and state='PENDING';
      if coalesce(pg_catalog.array_length(v_due_ids,1),0)>0
         and not exists(
           select 1 from public.weekly_message_intents intent
           where intent.recipient_generation_id=v_recipient_generation_id
             and intent.state in ('DUE','RENDERED')
         ) then
        v_intent_id:=private.weekly_source_query_manager_intent_v1(
          (select recipient_route_id from public.weekly_manager_recipient_generations where id=v_recipient_generation_id),
          v_recipient_generation_id,v_due_ids,'MANAGER_EARLY_ALL',1,
          pg_catalog.transaction_timestamp()
        );
        update public.weekly_manager_cohort_due_events set state='CONSUMED'
        where id=any(v_due_ids);
      end if;
    end if;
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'resolved_count',v_resolved,
    'office_notification_count',v_notices,'manager_message_intent_id',v_intent_id
  );
end;
$function$;

create or replace function public.weekly_source_query_send_manager_now_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_requested uuid[];
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_context jsonb;
  v_route_id uuid;
  v_current_route_id uuid;
  v_seen_keys text[]:='{}'::text[];
  v_selected_cohort_ids uuid[]:='{}'::uuid[];
  v_key text;
  v_selected record;
  v_candidate_generation public.weekly_candidate_outreach_generations%rowtype;
  v_generation jsonb;
  v_generation_id uuid;
  v_due_ids uuid[];
  v_intent_id uuid;
  v_settings public.weekly_source_global_settings%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in (
         'actor_user_id','source_cycle_id','projection_publication_id','incident_ids'
       )
     ) or pg_catalog.jsonb_typeof(p_request->'incident_ids')<>'array' then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    select pg_catalog.array_agg(value::uuid order by value::uuid) into v_requested
    from pg_catalog.jsonb_array_elements_text(p_request->'incident_ids');
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_REQUEST_INVALID' using errcode='22023';
  end;
  if coalesce(pg_catalog.array_length(v_requested,1),0)=0
     or pg_catalog.array_length(v_requested,1)<>(
       select pg_catalog.count(distinct value) from pg_catalog.unnest(v_requested) value
     ) then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_INVALID' using errcode='22023';
  end if;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'SEND_MANAGER',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date
  );

  for v_incident in
    select incident.*
    from public.weekly_discrepancy_incidents incident
    where incident.id=any(v_requested)
    order by incident.id
    for update
  loop
    if v_incident.source_cycle_id<>v_cycle_id or v_incident.state<>'OPEN'
       or v_incident.manager_potential_state<>'AVAILABLE'
       or v_incident.manager_action_state in ('RESPONDED','NOT_REQUIRED') then
      raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_comparison from public.weekly_issue_comparison_revisions
    where id=v_incident.current_comparison_revision_id;
    if v_comparison.candidate_timesheet_id is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_REQUIRES_TIMESHEET' using errcode='55000';
    end if;
    select * into strict v_event from public.weekly_work_events where id=v_incident.work_event_id;
    v_context:=private.weekly_source_query_cohort_ensure_v1(
      v_cycle_id,v_incident.candidate_id,v_incident.client_id,
      v_comparison.contract_id,v_event.work_date
    );
    if coalesce((v_context->>'manager_queries_enabled')::boolean,false) is not true
       or nullif(v_context->>'manager_route_id','') is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_QUERIES_DISABLED' using errcode='55000';
    end if;
    v_current_route_id:=(v_context->>'manager_route_id')::uuid;
    if v_route_id is null then v_route_id:=v_current_route_id;
    elsif v_route_id<>v_current_route_id then
      raise exception 'WEEKLY_SOURCE_MANAGER_SEND_ONE_RECIPIENT_REQUIRED' using errcode='22023';
    end if;
    if not ((v_context->>'cohort_id')::uuid=any(v_selected_cohort_ids)) then
      v_selected_cohort_ids:=pg_catalog.array_append(
        v_selected_cohort_ids,(v_context->>'cohort_id')::uuid
      );
    end if;
    v_key:=v_incident.candidate_id::text||':'||v_incident.client_id::text;
    if not (v_key=any(v_seen_keys)) then
      v_seen_keys:=pg_catalog.array_append(v_seen_keys,v_key);
      insert into public.weekly_route_activations(
        source_cycle_id,candidate_id,client_id,audience_route,route_mode,
        activated_by_user_id,activated_at_utc,updated_at_utc
      ) values (
        v_cycle_id,v_incident.candidate_id,v_incident.client_id,'MANAGER','MANAGER_DIRECT',
        v_actor,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
      ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
      do update set route_mode='MANAGER_DIRECT',
        activated_by_user_id=excluded.activated_by_user_id,
        activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
        updated_at_utc=excluded.updated_at_utc;
    end if;
  end loop;
  if (select pg_catalog.count(*) from public.weekly_discrepancy_incidents where id=any(v_requested))
     <>pg_catalog.array_length(v_requested,1) then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE' using errcode='40001';
  end if;
  if exists(
    select 1
    from public.weekly_candidate_cohorts cohort
    join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=cohort.source_cycle_id
     and incident.candidate_id=cohort.candidate_id
     and incident.client_id=cohort.client_id
     and incident.state='OPEN'
     and incident.manager_potential_state='AVAILABLE'
     and incident.manager_action_state not in ('RESPONDED','NOT_REQUIRED')
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.candidate_timesheet_id is not null
    where cohort.id=any(v_selected_cohort_ids)
      and not (incident.id=any(v_requested))
  ) then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE' using errcode='40001';
  end if;

  -- Manager-direct is the durable route choice for every selected complete
  -- candidate cohort. Stop only candidate work that has not reached a provider;
  -- accepted, submission-started and ambiguous delivery history is retained.
  for v_selected in
    select cohort.id,cohort.candidate_id,cohort.client_id
    from public.weekly_candidate_cohorts cohort
    where cohort.id=any(v_selected_cohort_ids)
    order by cohort.id
  loop
    insert into public.weekly_route_activations(
      source_cycle_id,candidate_id,client_id,audience_route,route_mode,
      activated_by_user_id,activated_at_utc,updated_at_utc
    ) values (
      v_cycle_id,v_selected.candidate_id,v_selected.client_id,'CANDIDATE','MANAGER_DIRECT',
      v_actor,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
    ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
    do update set route_mode='MANAGER_DIRECT',
      activated_by_user_id=excluded.activated_by_user_id,
      activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
      updated_at_utc=excluded.updated_at_utc;
    select * into v_candidate_generation
    from public.weekly_candidate_outreach_generations
    where candidate_cohort_id=v_selected.id and state='ACTIVE'
    for update;
    if found then
      update public.weekly_candidate_cohorts
      set current_generation_id=null where id=v_selected.id;
      update public.weekly_message_renders render
      set state='STALE'
      from public.weekly_message_dispatch_commands command
      where command.message_render_id=render.id
        and command.candidate_generation_id=v_candidate_generation.id
        and command.state in ('READY','LEASED') and render.state='CURRENT';
      update public.weekly_message_dispatch_commands
      set state='RETIRED',lease_owner=null,lease_token=null,lease_expires_at_utc=null
      where candidate_generation_id=v_candidate_generation.id
        and state in ('READY','LEASED');
      update public.weekly_message_intents
      set state='RETIRED'
      where candidate_generation_id=v_candidate_generation.id
        and state in ('DUE','RENDERED');
      update public.weekly_candidate_outreach_memberships
      set state='SUPERSEDED'
      where candidate_generation_id=v_candidate_generation.id and state='ACTIONABLE';
      update public.weekly_candidate_outreach_generations
      set state='CANCELLED' where id=v_candidate_generation.id;
    end if;
    update public.weekly_discrepancy_incidents
    set candidate_action_state='NOT_REQUIRED'
    where source_cycle_id=v_cycle_id
      and candidate_id=v_selected.candidate_id and client_id=v_selected.client_id
      and id=any(v_requested) and candidate_action_state in ('NOT_ASKED','ASKED');
  end loop;

  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  select * into strict v_route from public.weekly_manager_recipient_routes
  where id=v_route_id for update;
  if v_route.manager_send_available_at_utc is not null
     and pg_catalog.transaction_timestamp()<v_route.manager_send_available_at_utc then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_COOLDOWN' using errcode='55000',
      detail=pg_catalog.jsonb_build_object(
        'available_at_utc',v_route.manager_send_available_at_utc
      )::text;
  end if;
  -- 04A section 6.5: a manual send "atomically claims its exact eligible rows",
  -- and "one accepted batch owns each row ... No row is sent twice."  If every
  -- selected row is already owned by an accepted batch on this route there is
  -- no remainder to claim, so refuse now rather than staging a due intent the
  -- renderer would have to reject and which would then hold the whole route.
  if not exists(
    select 1 from public.weekly_discrepancy_incidents incident
    where incident.id=any(v_requested)
      and not private.weekly_source_query_manager_row_owned_v1(
        v_route_id,incident.id,incident.episode_number
      )
  ) then
    raise exception 'WEEKLY_SOURCE_MANAGER_SEND_ALREADY_SENT' using errcode='55000';
  end if;
  v_generation:=private.weekly_source_query_manager_generation_v1(
    v_route_id,'OFFICE_DIRECT',pg_catalog.transaction_timestamp(),true
  );
  v_generation_id:=(v_generation->>'recipient_generation_id')::uuid;
  select pg_catalog.array_agg(id order by id) into v_due_ids
  from public.weekly_manager_cohort_due_events
  where recipient_generation_id=v_generation_id and event_kind='EARLY_ALL' and state='PENDING'
    and candidate_cohort_id=any(v_selected_cohort_ids);
  v_intent_id:=private.weekly_source_query_manager_intent_v1(
    v_route_id,v_generation_id,v_due_ids,'MANAGER_MANUAL_SELECTED',1,
    pg_catalog.transaction_timestamp()
  );
  update public.weekly_manager_cohort_due_events set state='CONSUMED'
  where id=any(coalesce(v_due_ids,'{}'::uuid[]));
  update public.weekly_manager_recipient_routes
  set manager_send_available_at_utc=pg_catalog.transaction_timestamp()+v_settings.manager_manual_send_cooldown
  where id=v_route_id;
  update public.weekly_discrepancy_incidents incident
  set manager_action_state='DUE'
  from public.weekly_manager_recipient_memberships membership
  where membership.recipient_generation_id=v_generation_id
    and membership.candidate_cohort_id=any(v_selected_cohort_ids)
    and membership.incident_id=incident.id
    and incident.manager_action_state in ('NOT_SENT','SENT')
    and not private.weekly_source_query_manager_row_owned_v1(
      v_route_id,incident.id,incident.episode_number
    );
  perform public._audit_insert(
    'weekly_source_manager_send',v_intent_id::text,'WEEKLY_SOURCE_MANAGER_SEND_STAGED',
    null,pg_catalog.jsonb_build_object(
      'recipient_route_id',v_route_id,'recipient_generation_id',v_generation_id,
      'selected_incident_count',pg_catalog.array_length(v_requested,1),
      'send_available_at_utc',pg_catalog.transaction_timestamp()+v_settings.manager_manual_send_cooldown
    ),'Manager review email staged',v_actor
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'message_intent_id',v_intent_id,'recipient_route_id',v_route_id,
    'recipient_generation_id',v_generation_id,
    'send_available_at_utc',pg_catalog.transaction_timestamp()+v_settings.manager_manual_send_cooldown
  );
end;
$function$;

create or replace function public.weekly_source_candidate_reminder_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_generation_id uuid;
  v_publication_id uuid;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_settings public.weekly_source_global_settings%rowtype;
  v_intent_id uuid;
  v_tranche text;
  v_sequence integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','candidate_generation_id','projection_publication_id')) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REMINDER_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REMINDER_REQUEST_INVALID' using errcode='22023';
  end;
  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'ASK_CANDIDATES',v_cycle.source_group_id,v_generation.client_id,
    v_cycle.finalisation_week_ending
  );
  if v_generation.state<>'ACTIVE' or pg_catalog.transaction_timestamp()<v_generation.manual_reminder_available_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REMINDER_UNAVAILABLE' using errcode='55000';
  end if;
  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  select coalesce(pg_catalog.max(tranche_sequence),0)+1 into v_sequence
  from public.weekly_message_intents
  where candidate_generation_id=v_generation.id
    and tranche_kind in ('CANDIDATE_MANUAL_REMINDER','TIMESHEET_SUBMISSION_REMINDER_6H');
  v_tranche:=case when v_generation.request_kind='SUBMIT_TIMESHEET'
    then 'TIMESHEET_SUBMISSION_REMINDER_6H' else 'CANDIDATE_MANUAL_REMINDER' end;
  v_intent_id:=private.weekly_source_query_candidate_intent_v1(
    v_generation.id,v_tranche,v_sequence,pg_catalog.transaction_timestamp()
  );
  update public.weekly_candidate_outreach_generations
  set manual_reminder_available_at_utc=pg_catalog.transaction_timestamp()+v_settings.candidate_manual_reminder_cooldown
  where id=v_generation.id;
  perform public._audit_insert(
    'weekly_source_candidate_reminder',v_intent_id::text,'WEEKLY_SOURCE_CANDIDATE_REMINDER_STAGED',
    null,pg_catalog.jsonb_build_object('candidate_generation_id',v_generation.id),
    'Candidate reminder staged',v_actor
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'message_intent_id',v_intent_id,
    'next_manual_reminder_at_utc',pg_catalog.transaction_timestamp()+v_settings.candidate_manual_reminder_cooldown
  );
end;
$function$;

create or replace function public.weekly_source_query_ask_candidate_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_requested uuid[];
  v_current uuid[];
  v_guard jsonb;
  v_first_contract_id uuid;
  v_first_work_date date;
  v_context jsonb;
  v_result jsonb;
  v_manager_activation public.weekly_route_activations%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in (
         'actor_user_id','source_cycle_id','projection_publication_id',
         'candidate_id','client_id','incident_ids'
       )
     ) or pg_catalog.jsonb_typeof(p_request->'incident_ids')<>'array' then
    raise exception 'WEEKLY_SOURCE_ASK_CANDIDATE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_client_id:=(p_request->>'client_id')::uuid;
    select pg_catalog.array_agg(value::uuid order by value::uuid) into v_requested
    from pg_catalog.jsonb_array_elements_text(p_request->'incident_ids');
  exception when others then
    raise exception 'WEEKLY_SOURCE_ASK_CANDIDATE_REQUEST_INVALID' using errcode='22023';
  end;
  if coalesce(pg_catalog.array_length(v_requested,1),0)=0
     or pg_catalog.array_length(v_requested,1)<>(
       select pg_catalog.count(distinct id_value) from pg_catalog.unnest(v_requested) id_value
     ) then
    raise exception 'WEEKLY_SOURCE_ASK_CANDIDATE_SELECTION_INVALID' using errcode='22023';
  end if;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'ASK_CANDIDATES',(v_guard->>'source_group_id')::uuid,v_client_id,
    (v_guard->>'finalisation_week_ending')::date
  );
  select pg_catalog.array_agg(incident.id order by incident.id),
         min(comparison.contract_id::text)::uuid,min(work_event.work_date)
  into v_current,v_first_contract_id,v_first_work_date
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  where incident.source_cycle_id=v_cycle_id and incident.candidate_id=v_candidate_id
    and incident.client_id=v_client_id and incident.state='OPEN'
    and incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED');
  if v_current is null or v_current is distinct from v_requested then
    raise exception 'WEEKLY_SOURCE_ASK_CANDIDATE_SELECTION_STALE' using errcode='40001';
  end if;
  v_context:=private.weekly_source_query_cohort_ensure_v1(
    v_cycle_id,v_candidate_id,v_client_id,v_first_contract_id,v_first_work_date
  );
  if coalesce((v_context->>'candidate_queries_enabled')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERIES_DISABLED' using errcode='55000';
  end if;
  if coalesce((v_context->>'manager_queries_enabled')::boolean,false)
     and nullif(v_context->>'manager_route_id','') is not null then
    insert into public.weekly_route_activations(
      source_cycle_id,candidate_id,client_id,audience_route,route_mode,
      activated_by_user_id,activated_at_utc,updated_at_utc
    ) values (
      v_cycle_id,v_candidate_id,v_client_id,'MANAGER','CANDIDATE_FIRST',
      v_actor,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
    ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
    do update set route_mode='CANDIDATE_FIRST',
      activated_by_user_id=coalesce(public.weekly_route_activations.activated_by_user_id,excluded.activated_by_user_id),
      activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
      updated_at_utc=excluded.updated_at_utc
    returning * into v_manager_activation;
  end if;
  v_result:=private.weekly_source_query_candidate_generation_v1(
    v_cycle_id,v_candidate_id,v_client_id,v_publication_id,
    'OFFICE_ASK',v_actor,pg_catalog.transaction_timestamp()
  );
  if v_manager_activation.id is not null then
    perform private.weekly_source_query_manager_generation_v1(
      (v_context->>'manager_route_id')::uuid,'T6_RESPONDED',
      pg_catalog.transaction_timestamp(),false
    );
  end if;
  perform public._audit_insert(
    'weekly_source_candidate_query',coalesce(v_result->>'candidate_generation_id',v_candidate_id::text),
    'WEEKLY_SOURCE_CANDIDATE_ASKED',null,v_result,
    'Candidate asked to check Weekly hours',v_actor
  );
  return v_result;
end;
$function$;

create or replace function public.weekly_source_timesheet_submission_request_start_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
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
  perform private.weekly_source_office_authority_v1(
    v_actor,'ASK_CANDIDATES',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date
  );
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
  v_context:=private.weekly_source_query_cohort_ensure_v1(
    v_cycle_id,v_candidate_id,v_first_client_id,v_first_contract_id,v_first_work_date
  );
  select * into strict v_cohort from public.weekly_candidate_cohorts
  where id=(v_context->>'cohort_id')::uuid for update;
  select * into v_old_request
  from public.weekly_timesheet_submission_requests
  where source_cycle_id=v_cycle_id and candidate_id=v_candidate_id
    and state in ('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
  for update;
  if found and v_old_request.membership_hash=v_hash then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','UNCHANGED','submission_request_id',v_old_request.id,
      'candidate_generation_id',v_cohort.current_generation_id,
      'started_at_utc',v_old_request.started_at_utc,
      'reminder_due_at_utc',v_old_request.reminder_due_at_utc,
      'deadline_at_utc',v_old_request.deadline_at_utc
    );
  end if;
  if found then
    update public.weekly_timesheet_submission_requests
    set state='SUPERSEDED',updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_old_request.id;
  end if;
  insert into public.weekly_route_activations(
    source_cycle_id,candidate_id,client_id,audience_route,route_mode,
    activated_by_user_id,activated_at_utc,updated_at_utc
  ) values (
    v_cycle_id,v_candidate_id,v_first_client_id,'CANDIDATE','CANDIDATE_FIRST',
    v_actor,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
  ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
  do update set route_mode='CANDIDATE_FIRST',
    activated_by_user_id=coalesce(public.weekly_route_activations.activated_by_user_id,excluded.activated_by_user_id),
    activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
    updated_at_utc=excluded.updated_at_utc
  returning * into v_activation;
  select * into v_old_generation
  from public.weekly_candidate_outreach_generations
  where candidate_cohort_id=v_cohort.id and state='ACTIVE' for update;
  if found then
    update public.weekly_candidate_cohorts set current_generation_id=null where id=v_cohort.id;
    update public.weekly_candidate_outreach_generations
    set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp()
    where id=v_old_generation.id;
    update public.weekly_message_intents set state='RETIRED'
    where candidate_generation_id=v_old_generation.id and state in ('DUE','RENDERED');
    update public.weekly_message_dispatch_commands set state='RETIRED'
    where candidate_generation_id=v_old_generation.id and state in ('READY','LEASED');
  end if;
  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  select coalesce(pg_catalog.max(generation_number),0)+1 into v_generation_number
  from public.weekly_candidate_outreach_generations where candidate_cohort_id=v_cohort.id;
  insert into public.weekly_candidate_outreach_generations(
    source_cycle_id,candidate_cohort_id,candidate_id,client_id,generation_number,
    activation_id,trigger_kind,request_kind,route_mode,started_at_utc,
    reminder_due_at_utc,deadline_at_utc,manual_reminder_available_at_utc,state,membership_hash
  ) values (
    v_cycle_id,v_cohort.id,v_candidate_id,v_first_client_id,v_generation_number,
    v_activation.id,'OFFICE_ASK','SUBMIT_TIMESHEET','CANDIDATE_FIRST',
    pg_catalog.transaction_timestamp(),
    pg_catalog.transaction_timestamp()+v_settings.candidate_reminder_after,
    pg_catalog.transaction_timestamp()+v_settings.candidate_response_deadline_after,
    pg_catalog.transaction_timestamp()+v_settings.candidate_manual_reminder_cooldown,
    'ACTIVE',v_hash
  ) returning * into v_generation;
  update public.weekly_candidate_cohorts set current_generation_id=v_generation.id where id=v_cohort.id;
  insert into public.weekly_timesheet_submission_requests(
    environment,agency_id,source_cycle_id,candidate_id,candidate_cohort_id,
    request_generation,current_upload_id,current_projection_publication_id,state,
    started_at_utc,reminder_due_at_utc,deadline_at_utc,membership_hash
  )
  select source_group.environment,source_group.agency_id,v_cycle_id,v_candidate_id,v_cohort.id,
    coalesce((select pg_catalog.max(request_generation)+1 from public.weekly_timesheet_submission_requests
              where source_cycle_id=v_cycle_id and candidate_id=v_candidate_id),1),
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
    ),'Candidate asked to submit a Weekly Timesheet',v_actor
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


create or replace function private.weekly_source_query_manager_generation_v1(
  p_recipient_route_id uuid,
  p_trigger_kind text,
  p_started_at_utc timestamptz,
  p_force_new boolean default false
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_old public.weekly_manager_recipient_generations%rowtype;
  v_new public.weekly_manager_recipient_generations%rowtype;
  v_membership jsonb;
  v_hash bytea;
  v_generation integer;
  v_ordinal integer:=0;
  v_row record;
  v_candidate_generation public.weekly_candidate_outreach_generations%rowtype;
  v_settings public.weekly_source_global_settings%rowtype;
  v_event_kind text;
  v_due timestamptz;
  v_trigger_hash bytea;
begin
  if p_trigger_kind not in (
    'OFFICE_DIRECT','EARLY_ALL','T6_RESPONDED','T12_REMAINDER','NEW_INCIDENT','MANUAL_RESEND'
  ) or p_started_at_utc is null then
    raise exception 'WEEKLY_SOURCE_MANAGER_TRIGGER_INVALID' using errcode='22023';
  end if;
  select * into strict v_route
  from public.weekly_manager_recipient_routes
  where id=p_recipient_route_id for update;
  select * into v_old
  from public.weekly_manager_recipient_generations
  where id=v_route.current_generation_id and state='ACTIVE'
  for update;
  v_membership:=coalesce((
    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'activation_id',activation.id,
        'candidate_cohort_id',cohort.id,
        'candidate_generation_id',cohort.current_generation_id,
        'incident_id',incident.id,
        'comparison_revision_id',incident.current_comparison_revision_id
      ) order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",
                 client.id::text collate "C",
                 private.weekly_source_query_ascii_fold_v1(coalesce(
                   nullif(candidate.display_name,''),
                   pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)
                 )) collate "C",
                 candidate.id::text collate "C",work_event.work_date,
                 comparison.system_start_at_local,incident.id
    )
    from public.weekly_candidate_cohorts cohort
    join public.weekly_route_activations activation
      on activation.source_cycle_id=cohort.source_cycle_id
     and activation.candidate_id=cohort.candidate_id
     and activation.client_id=cohort.client_id
     and activation.audience_route='MANAGER'
     and activation.route_mode in ('CANDIDATE_FIRST','MANAGER_DIRECT')
    join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=cohort.source_cycle_id
     and incident.candidate_id=cohort.candidate_id
     and incident.client_id=cohort.client_id
     and incident.state='OPEN'
     and incident.manager_potential_state='AVAILABLE'
      -- 04A section 6: "Resolved and manager-answered/waiting-source items are
      -- not reintroduced", and section 1: an item "already answered by the
      -- manager but waiting for a later source remains visible to Office and is
      -- never included again".  A genuinely new EPISODE of the same incident is
      -- different work, and the recheck owner already resets manager_action_state
      -- when it raises the episode; nothing else may resurrect an answer.
      and incident.manager_action_state in ('NOT_SENT','DUE','SENT')
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.candidate_timesheet_id is not null
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    join public.clients client on client.id=incident.client_id
    join public.candidates candidate on candidate.id=incident.candidate_id
    where cohort.source_cycle_id=v_route.source_cycle_id
      and cohort.manager_recipient_route_key=v_route.normalised_recipient_hash
  ),'[]'::jsonb);
  if pg_catalog.jsonb_array_length(v_membership)=0 then
    return pg_catalog.jsonb_build_object('ok',true,'status','NO_MANAGER_ACTIONABLE_INCIDENTS');
  end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_MANAGER_MEMBERSHIP_V1',v_membership);
  if v_old.id is not null and v_old.membership_hash=v_hash and not p_force_new then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','UNCHANGED','recipient_generation_id',v_old.id,
      'recipient_route_id',v_route.id,'membership_hash',pg_catalog.encode(v_hash,'hex')
    );
  end if;
  if v_old.id is not null then
    update public.weekly_manager_recipient_routes set current_generation_id=null where id=v_route.id;
    update public.weekly_manager_recipient_generations
    set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp()
    where id=v_old.id;
    -- 04A section 8: "A later cohort merely becoming due is not a revocation
    -- event for an earlier accepted batch", and section 1: a later generation
    -- "cannot broaden, alter, supersede or revoke an already accepted earlier
    -- batch merely by becoming due".  The only express revocation the pack names
    -- is correcting a final source.  So an ACCEPTED batch and its live secure
    -- link survive this rotation untouched, and section 8's promise -- the
    -- manager "may answer some items, close the page and return later" -- keeps
    -- holding.
    --
    -- A batch whose render never reached provider submission (section 9) was
    -- never accepted: nothing was sent, its command and intent are retired just
    -- below, and the delivery start guard would refuse it anyway.  That batch is
    -- revoked here so its rows return to the unsent pool rather than being
    -- silently owned by an email nobody received.
    update public.weekly_manager_review_batches batch
    set state='REVOKED'
    from public.weekly_message_renders render
    where render.id=batch.message_render_id
      and batch.recipient_generation_id=v_old.id and batch.state='ACTIVE'
      and render.state not in ('SUBMISSION_STARTED','ACCEPTED','AMBIGUOUS');
    update public.weekly_manager_route_receipts receipt
    set state='REVOKED',revoked_at_utc=pg_catalog.transaction_timestamp()
    from public.weekly_manager_review_batches batch
    where batch.id=receipt.review_batch_id
      and receipt.recipient_generation_id=v_old.id and receipt.state='ACTIVE'
      and batch.state='REVOKED';
    update public.weekly_manager_review_items item
    set response_state='OBSOLETE'
    from public.weekly_manager_review_batches batch
    where batch.id=item.review_batch_id
      and batch.recipient_generation_id=v_old.id and batch.state='REVOKED'
      and item.response_state='UNANSWERED';
    update public.weekly_manager_cohort_due_events
    set state='RETIRED' where recipient_generation_id=v_old.id and state='PENDING';
    update public.weekly_message_intents
    set state='RETIRED'
    where recipient_generation_id=v_old.id and state in ('DUE','RENDERED');
    update public.weekly_message_dispatch_commands command
    set state='RETIRED'
    from public.weekly_message_intents intent
    where command.message_intent_id=intent.id
      and intent.recipient_generation_id=v_old.id
      and command.state in ('READY','LEASED');
    update public.weekly_message_renders render
    set state='STALE'
    from public.weekly_message_intents intent
    where render.message_intent_id=intent.id and intent.recipient_generation_id=v_old.id
      and render.state='CURRENT';
  end if;
  select coalesce(pg_catalog.max(generation_number),0)+1 into v_generation
  from public.weekly_manager_recipient_generations where recipient_route_id=v_route.id;
  insert into public.weekly_manager_recipient_generations(
    recipient_route_id,generation_number,trigger_kind,state,membership_hash
  ) values (v_route.id,v_generation,p_trigger_kind,'ACTIVE',v_hash)
  returning * into v_new;
  for v_row in
    select
      activation.id as activation_id,cohort.id as cohort_id,
      cohort.current_generation_id as candidate_generation_id,
      incident.id as incident_id,incident.current_comparison_revision_id
    from public.weekly_candidate_cohorts cohort
    join public.weekly_route_activations activation
      on activation.source_cycle_id=cohort.source_cycle_id
     and activation.candidate_id=cohort.candidate_id
     and activation.client_id=cohort.client_id
     and activation.audience_route='MANAGER'
     and activation.route_mode in ('CANDIDATE_FIRST','MANAGER_DIRECT')
    join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=cohort.source_cycle_id
     and incident.candidate_id=cohort.candidate_id
     and incident.client_id=cohort.client_id
      and incident.state='OPEN' and incident.manager_potential_state='AVAILABLE'
      -- 04A section 6: "Resolved and manager-answered/waiting-source items are
      -- not reintroduced", and section 1: an item "already answered by the
      -- manager but waiting for a later source remains visible to Office and is
      -- never included again".  A genuinely new EPISODE of the same incident is
      -- different work, and the recheck owner already resets manager_action_state
      -- when it raises the episode; nothing else may resurrect an answer.
      and incident.manager_action_state in ('NOT_SENT','DUE','SENT')
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.candidate_timesheet_id is not null
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    join public.clients client on client.id=incident.client_id
    join public.candidates candidate on candidate.id=incident.candidate_id
    where cohort.source_cycle_id=v_route.source_cycle_id
      and cohort.manager_recipient_route_key=v_route.normalised_recipient_hash
    order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",
             client.id::text collate "C",
             private.weekly_source_query_ascii_fold_v1(coalesce(
               nullif(candidate.display_name,''),
               pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)
             )) collate "C",
             candidate.id::text collate "C",work_event.work_date,
             comparison.system_start_at_local,incident.id
  loop
    -- A due-but-unsent row returns to the unsent pool, because the intent,
    -- command and render that made it due are retired above and this generation
    -- must be able to drive it again.  A SENT row stays SENT and a RESPONDED row
    -- keeps its answer: 04A section 1 forbids a later generation from altering
    -- an already accepted batch, and section 6 forbids reintroducing a
    -- manager-answered or waiting-source item.
    if p_trigger_kind='NEW_INCIDENT' then
      update public.weekly_discrepancy_incidents
      set manager_action_state='NOT_SENT',waiting_source_state='NOT_WAITING',
          reconciliation_state='UNRESOLVED'
      where id=v_row.incident_id
        and manager_action_state='DUE'
        and not private.weekly_source_query_manager_row_owned_v1(
          v_route.id,id,episode_number
        );
    end if;
    v_ordinal:=v_ordinal+1;
    insert into public.weekly_manager_recipient_memberships(
      recipient_generation_id,route_activation_id,candidate_outreach_generation_id,
      candidate_cohort_id,incident_id,comparison_revision_id,membership_role,ordinal
    ) values (
      v_new.id,v_row.activation_id,v_row.candidate_generation_id,v_row.cohort_id,
      v_row.incident_id,v_row.current_comparison_revision_id,'MANAGER_ACTIONABLE',v_ordinal
    );
  end loop;
  update public.weekly_manager_recipient_routes
  set current_generation_id=v_new.id where id=v_route.id;

  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  for v_row in
    select distinct membership.candidate_cohort_id,activation.route_mode
    from public.weekly_manager_recipient_memberships membership
    join public.weekly_route_activations activation on activation.id=membership.route_activation_id
    where membership.recipient_generation_id=v_new.id
    order by membership.candidate_cohort_id
  loop
    v_candidate_generation.id:=null;
    v_candidate_generation.started_at_utc:=null;
    v_candidate_generation.deadline_at_utc:=null;
    if v_row.route_mode='MANAGER_DIRECT' then
      v_event_kind:='EARLY_ALL';
      v_due:=p_started_at_utc;
    else
      select * into v_candidate_generation
      from public.weekly_candidate_outreach_generations
      where id=(select current_generation_id from public.weekly_candidate_cohorts where id=v_row.candidate_cohort_id);
      if not found then
        continue;
      end if;
      v_event_kind:='T6_RESPONDED';
      v_due:=v_candidate_generation.started_at_utc+v_settings.manager_partial_digest_after;
    end if;
    v_trigger_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_MANAGER_COHORT_DUE_V1',
      pg_catalog.jsonb_build_object(
        'recipient_generation_id',v_new.id,'candidate_cohort_id',v_row.candidate_cohort_id,
        'event_kind',v_event_kind,'due_at_utc',v_due
      )
    );
    insert into public.weekly_manager_cohort_due_events(
      recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
      due_at_utc,state,trigger_hash
    ) values (
      v_new.id,v_row.candidate_cohort_id,v_event_kind,
      coalesce(v_candidate_generation.started_at_utc,p_started_at_utc),v_due,'PENDING',v_trigger_hash
    ) on conflict (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash) do nothing;
    if v_row.route_mode='CANDIDATE_FIRST' then
      v_event_kind:='T12_REMAINDER';
      v_due:=v_candidate_generation.deadline_at_utc;
      v_trigger_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_MANAGER_COHORT_DUE_V1',
        pg_catalog.jsonb_build_object(
          'recipient_generation_id',v_new.id,'candidate_cohort_id',v_row.candidate_cohort_id,
          'event_kind',v_event_kind,'due_at_utc',v_due
        )
      );
      insert into public.weekly_manager_cohort_due_events(
        recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
        due_at_utc,state,trigger_hash
      ) values (
        v_new.id,v_row.candidate_cohort_id,v_event_kind,
        v_candidate_generation.started_at_utc,v_due,'PENDING',v_trigger_hash
      ) on conflict (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash) do nothing;
    end if;
  end loop;
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','CREATED','recipient_route_id',v_route.id,
    'recipient_generation_id',v_new.id,
    'membership_hash',pg_catalog.encode(v_new.membership_hash,'hex')
  );
end;
$function$;

create or replace function private.weekly_source_query_manager_intent_v1(
  p_recipient_route_id uuid,
  p_recipient_generation_id uuid,
  p_due_event_ids uuid[],
  p_tranche_kind text,
  p_tranche_sequence integer,
  p_due_at_utc timestamptz
) returns uuid
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_generation public.weekly_manager_recipient_generations%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_events uuid[];
  v_key bytea;
begin
  if p_tranche_kind not in (
    'MANAGER_EARLY_ALL','MANAGER_T6_RESPONDED','MANAGER_T12_REMAINDER',
    'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND'
  ) or p_tranche_sequence<1 or p_due_at_utc is null then
    raise exception 'WEEKLY_SOURCE_MANAGER_INTENT_INVALID' using errcode='22023';
  end if;
  select * into strict v_route
  from public.weekly_manager_recipient_routes where id=p_recipient_route_id;
  select * into strict v_generation
  from public.weekly_manager_recipient_generations
  where id=p_recipient_generation_id and recipient_route_id=v_route.id and state='ACTIVE';
  select coalesce(pg_catalog.array_agg(event_id order by event_id),'{}'::uuid[]) into v_events
  from pg_catalog.unnest(coalesce(p_due_event_ids,'{}'::uuid[])) event_id;
  v_key:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_INTENT_V1',
    pg_catalog.jsonb_build_object(
      'environment',v_route.environment,'agency_id',v_route.agency_id,
      'source_cycle_id',v_route.source_cycle_id,'recipient_route_id',v_route.id,
      'recipient_generation_id',v_generation.id,'due_event_ids',v_events,
      'tranche_kind',p_tranche_kind,'tranche_sequence',p_tranche_sequence,
      'membership_hash',pg_catalog.encode(v_generation.membership_hash,'hex')
    )
  );
  insert into public.weekly_message_intents(
    environment,agency_id,source_cycle_id,audience_kind,recipient_route_id,
    recipient_generation_id,sorted_due_event_ids,tranche_kind,tranche_sequence,
    logical_key,state,due_at_utc
  ) values (
    v_route.environment,v_route.agency_id,v_route.source_cycle_id,'MANAGER',v_route.id,
    v_generation.id,v_events,p_tranche_kind,p_tranche_sequence,v_key,'DUE',p_due_at_utc
  ) on conflict (logical_key) do nothing;
  select * into strict v_intent from public.weekly_message_intents where logical_key=v_key;
  return v_intent.id;
end;
$function$;

create or replace function private.weekly_source_query_restart_activated_cohort_v1(
  p_source_cycle_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_projection_publication_id uuid
) returns void
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_activation public.weekly_route_activations%rowtype;
  v_candidate_result jsonb;
  v_first record;
  v_cohort_context jsonb;
begin
  select * into v_activation
  from public.weekly_route_activations
  where source_cycle_id=p_source_cycle_id and candidate_id=p_candidate_id
    and client_id=p_client_id and audience_route='CANDIDATE';
  if found and v_activation.route_mode='CANDIDATE_FIRST'
     and not exists(
       select 1
       from public.weekly_timesheet_submission_requests submission
       where submission.source_cycle_id=p_source_cycle_id
         and submission.candidate_id=p_candidate_id
         and submission.state in ('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
     ) then
    v_candidate_result:=private.weekly_source_query_candidate_generation_v1(
      p_source_cycle_id,p_candidate_id,p_client_id,p_projection_publication_id,
      'NEW_INCIDENT',v_activation.activated_by_user_id,pg_catalog.transaction_timestamp()
    );
  end if;
  select comparison.contract_id,work_event.work_date into v_first
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  where incident.source_cycle_id=p_source_cycle_id and incident.candidate_id=p_candidate_id
    and incident.client_id=p_client_id and incident.state='OPEN'
  order by work_event.work_date,incident.id limit 1;
  if found then
    v_cohort_context:=private.weekly_source_query_cohort_ensure_v1(
      p_source_cycle_id,p_candidate_id,p_client_id,v_first.contract_id,v_first.work_date
    );
  end if;
  select * into v_activation
  from public.weekly_route_activations
  where source_cycle_id=p_source_cycle_id and candidate_id=p_candidate_id
    and client_id=p_client_id and audience_route='MANAGER';
  if found and v_activation.route_mode in ('CANDIDATE_FIRST','MANAGER_DIRECT')
     and nullif(v_cohort_context->>'manager_route_id','') is not null then
    perform private.weekly_source_query_manager_generation_v1(
      (v_cohort_context->>'manager_route_id')::uuid,'NEW_INCIDENT',
      pg_catalog.transaction_timestamp(),true
    );
  end if;
end;
$function$;


create or replace function private.weekly_source_query_cohort_ensure_v1(
  p_source_cycle_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_contract_id uuid,
  p_work_date date
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_recipient text;
  v_route_key bytea;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_route public.weekly_manager_recipient_routes%rowtype;
begin
  select * into strict v_cycle
  from public.weekly_source_cycles where id=p_source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  v_policy:=private._weekly_source_effective_policy_v1(p_client_id,p_contract_id,p_work_date);
  if v_policy->>'source_group_id'<>v_group.id::text
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_policy->>'document_mode'<>'CHECK_ONLY' then
    raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
  end if;
  v_recipient:=case
    when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
    then private.weekly_source_query_normalise_recipient_v1(v_policy->>'manager_query_recipient')
    else null
  end;
  v_route_key:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RECIPIENT_ROUTE_V1',
    pg_catalog.jsonb_build_object(
      'environment',v_group.environment,'agency_id',v_group.agency_id,
      'source_cycle_id',v_cycle.id,
      'recipient',coalesce(v_recipient,'NO_MANAGER_ROUTE')
    )
  );
  insert into public.weekly_candidate_cohorts(
    source_cycle_id,candidate_id,client_id,manager_recipient_route_key
  ) values (v_cycle.id,p_candidate_id,p_client_id,v_route_key)
  on conflict (source_cycle_id,candidate_id,client_id,manager_recipient_route_key)
  do nothing;
  select * into strict v_cohort
  from public.weekly_candidate_cohorts
  where source_cycle_id=v_cycle.id and candidate_id=p_candidate_id
    and client_id=p_client_id and manager_recipient_route_key=v_route_key;
  if v_recipient is not null then
    insert into public.weekly_manager_recipient_routes(
      environment,agency_id,source_cycle_id,normalised_recipient_hash,
      protected_recipient_address
    ) values (
      v_group.environment,v_group.agency_id,v_cycle.id,v_route_key,v_recipient
    ) on conflict (environment,agency_id,source_cycle_id,normalised_recipient_hash)
    do nothing;
    select * into strict v_route
    from public.weekly_manager_recipient_routes
    where environment=v_group.environment and agency_id=v_group.agency_id
      and source_cycle_id=v_cycle.id and normalised_recipient_hash=v_route_key;
    if private.weekly_source_query_normalise_recipient_v1(v_route.protected_recipient_address)
       is distinct from v_recipient then
      raise exception 'WEEKLY_SOURCE_MANAGER_RECIPIENT_ROUTE_CONFLICT' using errcode='55000';
    end if;
  end if;
  return pg_catalog.jsonb_build_object(
    'cohort_id',v_cohort.id,'manager_route_id',v_route.id,
    'manager_route_key',pg_catalog.encode(v_route_key,'hex'),
    'candidate_queries_enabled',coalesce((v_policy->>'candidate_queries_enabled')::boolean,false),
    'manager_queries_enabled',coalesce((v_policy->>'manager_queries_enabled')::boolean,false),
    'manager_recipient',v_recipient
  );
end;
$function$;

create or replace function private.weekly_source_query_candidate_intent_v1(
  p_generation_id uuid,
  p_tranche_kind text,
  p_tranche_sequence integer,
  p_due_at_utc timestamptz
) returns uuid
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_intent public.weekly_message_intents%rowtype;
  v_key bytea;
begin
  if p_tranche_kind not in (
    'CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
    'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H'
  ) or p_tranche_sequence<1 or p_due_at_utc is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_INTENT_INVALID' using errcode='22023';
  end if;
  select * into strict v_generation
  from public.weekly_candidate_outreach_generations where id=p_generation_id;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  v_key:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MESSAGE_INTENT_V1',
    pg_catalog.jsonb_build_object(
      'environment',v_group.environment,'agency_id',v_group.agency_id,
      'source_cycle_id',v_cycle.id,'candidate_cohort_id',v_generation.candidate_cohort_id,
      'candidate_generation_id',v_generation.id,'tranche_kind',p_tranche_kind,
      'tranche_sequence',p_tranche_sequence
    )
  );
  insert into public.weekly_message_intents(
    environment,agency_id,source_cycle_id,audience_kind,candidate_cohort_id,
    candidate_generation_id,sorted_due_event_ids,tranche_kind,tranche_sequence,
    logical_key,state,due_at_utc
  ) values (
    v_group.environment,v_group.agency_id,v_cycle.id,'CANDIDATE',
    v_generation.candidate_cohort_id,v_generation.id,'{}'::uuid[],
    p_tranche_kind,p_tranche_sequence,v_key,'DUE',p_due_at_utc
  ) on conflict (logical_key) do nothing;
  select * into strict v_intent from public.weekly_message_intents where logical_key=v_key;
  return v_intent.id;
end;
$function$;

create or replace function private.weekly_source_query_candidate_generation_v1(
  p_source_cycle_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_projection_publication_id uuid,
  p_trigger_kind text,
  p_actor_user_id uuid,
  p_started_at_utc timestamptz
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_first record;
  v_cohort_context jsonb;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_activation public.weekly_route_activations%rowtype;
  v_old public.weekly_candidate_outreach_generations%rowtype;
  v_new public.weekly_candidate_outreach_generations%rowtype;
  v_settings public.weekly_source_global_settings%rowtype;
  v_membership jsonb;
  v_hash bytea;
  v_generation_number integer;
  v_ordinal integer:=0;
  v_row record;
  v_intent_id uuid;
begin
  if p_trigger_kind not in ('OFFICE_ASK','NEW_INCIDENT','REOPENED_INCIDENT') then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TRIGGER_INVALID' using errcode='22023';
  end if;
  select incident.id,comparison.contract_id,work_event.work_date
  into v_first
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  where incident.source_cycle_id=p_source_cycle_id and incident.candidate_id=p_candidate_id
    and incident.client_id=p_client_id and incident.state='OPEN'
    and (
      (p_trigger_kind in ('NEW_INCIDENT','REOPENED_INCIDENT')
       and incident.candidate_action_state<>'NOT_REQUIRED')
      or incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED')
    )
  order by work_event.work_date,incident.id
  limit 1;
  if not found then
    return pg_catalog.jsonb_build_object('ok',true,'status','NO_ACTIONABLE_INCIDENTS');
  end if;
  v_cohort_context:=private.weekly_source_query_cohort_ensure_v1(
    p_source_cycle_id,p_candidate_id,p_client_id,v_first.contract_id,v_first.work_date
  );
  if coalesce((v_cohort_context->>'candidate_queries_enabled')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_QUERIES_DISABLED' using errcode='55000';
  end if;
  select * into strict v_cohort
  from public.weekly_candidate_cohorts where id=(v_cohort_context->>'cohort_id')::uuid
  for update;
  insert into public.weekly_route_activations(
    source_cycle_id,candidate_id,client_id,audience_route,route_mode,
    activated_by_user_id,activated_at_utc,updated_at_utc
  ) values (
    p_source_cycle_id,p_candidate_id,p_client_id,'CANDIDATE','CANDIDATE_FIRST',
    p_actor_user_id,p_started_at_utc,p_started_at_utc
  ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
  do update set
    route_mode='CANDIDATE_FIRST',
    activated_by_user_id=coalesce(public.weekly_route_activations.activated_by_user_id,excluded.activated_by_user_id),
    activated_at_utc=coalesce(public.weekly_route_activations.activated_at_utc,excluded.activated_at_utc),
    updated_at_utc=excluded.updated_at_utc
  returning * into v_activation;
  select * into v_old
  from public.weekly_candidate_outreach_generations
  where candidate_cohort_id=v_cohort.id and state='ACTIVE'
  for update;
  v_membership:=coalesce((
    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'incident_id',incident.id,
        'comparison_revision_id',incident.current_comparison_revision_id
      ) order by work_event.work_date,comparison.candidate_start_at_local,incident.id
    )
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.source_cycle_id=p_source_cycle_id and incident.candidate_id=p_candidate_id
      and incident.client_id=p_client_id and incident.state='OPEN'
      and (
        (p_trigger_kind in ('NEW_INCIDENT','REOPENED_INCIDENT')
         and incident.candidate_action_state<>'NOT_REQUIRED')
        or incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED')
      )
  ),'[]'::jsonb);
  if pg_catalog.jsonb_array_length(v_membership)=0 then
    return pg_catalog.jsonb_build_object('ok',true,'status','NO_ACTIONABLE_INCIDENTS');
  end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CANDIDATE_MEMBERSHIP_V1',v_membership);
  if v_old.id is not null and v_old.membership_hash=v_hash then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','UNCHANGED','candidate_generation_id',v_old.id,
      'candidate_cohort_id',v_cohort.id
    );
  end if;
  if v_old.id is not null then
    update public.weekly_candidate_cohorts set current_generation_id=null where id=v_cohort.id;
    update public.weekly_candidate_outreach_generations
    set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp()
    where id=v_old.id;
    update public.weekly_candidate_outreach_memberships
    set state='SUPERSEDED' where candidate_generation_id=v_old.id and state='ACTIONABLE';
    update public.weekly_message_intents
    set state='RETIRED'
    where candidate_generation_id=v_old.id and state in ('DUE','RENDERED');
    update public.weekly_message_dispatch_commands
    set state='RETIRED'
    where candidate_generation_id=v_old.id and state in ('READY','LEASED');
    update public.weekly_message_renders render
    set state='STALE'
    from public.weekly_message_intents intent
    where render.message_intent_id=intent.id and intent.candidate_generation_id=v_old.id
      and render.state='CURRENT';
  end if;
  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  select coalesce(pg_catalog.max(generation_number),0)+1 into v_generation_number
  from public.weekly_candidate_outreach_generations where candidate_cohort_id=v_cohort.id;
  insert into public.weekly_candidate_outreach_generations(
    source_cycle_id,candidate_cohort_id,candidate_id,client_id,generation_number,
    activation_id,trigger_kind,request_kind,route_mode,started_at_utc,
    reminder_due_at_utc,deadline_at_utc,manual_reminder_available_at_utc,
    state,membership_hash
  ) values (
    p_source_cycle_id,v_cohort.id,p_candidate_id,p_client_id,v_generation_number,
    v_activation.id,p_trigger_kind,'CHECK_HOURS','CANDIDATE_FIRST',p_started_at_utc,
    p_started_at_utc+v_settings.candidate_reminder_after,
    p_started_at_utc+v_settings.candidate_response_deadline_after,
    p_started_at_utc+v_settings.candidate_manual_reminder_cooldown,
    'ACTIVE',v_hash
  ) returning * into v_new;
  for v_row in
    select incident.id as incident_id,incident.current_comparison_revision_id
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.source_cycle_id=p_source_cycle_id and incident.candidate_id=p_candidate_id
      and incident.client_id=p_client_id and incident.state='OPEN'
      and (
        (p_trigger_kind in ('NEW_INCIDENT','REOPENED_INCIDENT')
         and incident.candidate_action_state<>'NOT_REQUIRED')
        or incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED')
      )
    order by work_event.work_date,comparison.candidate_start_at_local,incident.id
  loop
    v_ordinal:=v_ordinal+1;
    insert into public.weekly_candidate_outreach_memberships(
      candidate_generation_id,incident_id,comparison_revision_id,ordinal,state
    ) values (v_new.id,v_row.incident_id,v_row.current_comparison_revision_id,v_ordinal,'ACTIONABLE');
    update public.weekly_discrepancy_incidents
    set candidate_action_state='ASKED'
    where id=v_row.incident_id
      and candidate_action_state in ('NOT_ASKED','RESPONDED');
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key,occurred_at_utc
    )
    select incident.id,incident.episode_number,p_projection_publication_id,
      comparison.material_comparison_fingerprint,'REQUESTED','OFFICE',p_actor_user_id,
      pg_catalog.jsonb_build_object(
        'route','CANDIDATE_FIRST','candidate_generation_id',v_new.id
      ),
      'CANDIDATE_REQUESTED:'||v_new.id::text||':'||incident.id::text,p_started_at_utc
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.id=v_row.incident_id
    on conflict (event_kind,idempotency_key) do nothing;
  end loop;
  update public.weekly_candidate_cohorts set current_generation_id=v_new.id where id=v_cohort.id;
  v_intent_id:=private.weekly_source_query_candidate_intent_v1(
    v_new.id,'CANDIDATE_INITIAL',1,p_started_at_utc
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','CREATED','candidate_generation_id',v_new.id,
    'candidate_cohort_id',v_cohort.id,'message_intent_id',v_intent_id,
    'started_at_utc',v_new.started_at_utc,
    'reminder_due_at_utc',v_new.reminder_due_at_utc,
    'deadline_at_utc',v_new.deadline_at_utc,
    'membership_hash',pg_catalog.encode(v_new.membership_hash,'hex')
  );
end;
$function$;


create or replace function private.weekly_source_query_hex32_v1(
  p_value text,
  p_code text
) returns bytea
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_value is null or p_value !~ '^[0-9a-f]{64}$' then
    raise exception '%',coalesce(p_code,'WEEKLY_SOURCE_HASH_INVALID') using errcode='22023';
  end if;
  return pg_catalog.decode(p_value,'hex');
end;
$function$;

create or replace function private.weekly_source_query_ascii_fold_v1(p_value text)
returns text
language sql
immutable
strict
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.translate(
    p_value,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
    'abcdefghijklmnopqrstuvwxyz'
  );
$function$;

create or replace function private.weekly_source_query_normalise_recipient_v1(
  p_address text
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_address text;
begin
  v_address:=pg_catalog.lower(normalize(pg_catalog.btrim(coalesce(p_address,'')),NFKC));
  if pg_catalog.char_length(v_address) not between 3 and 254
     or v_address !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
     or v_address ~ '[[:cntrl:]]' then
    raise exception 'WEEKLY_SOURCE_MANAGER_RECIPIENT_INVALID' using errcode='22023';
  end if;
  return v_address;
end;
$function$;

create or replace function private.weekly_source_query_current_publication_v1(
  p_source_cycle_id uuid,
  p_projection_publication_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_cycle public.weekly_source_cycles%rowtype;
  v_publication_cycle public.weekly_source_cycles%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
begin
  if p_source_cycle_id is null or p_projection_publication_id is null then
    raise exception 'WEEKLY_SOURCE_QUERY_PUBLICATION_REQUIRED' using errcode='22023';
  end if;
  select * into v_cycle
  from public.weekly_source_cycles
  where id=p_source_cycle_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select * into v_publication
  from public.weekly_source_projection_publications
  where id=p_projection_publication_id;
  if not found or v_publication.state<>'CURRENT' then
    raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
  end if;
  select * into strict v_publication_cycle
  from public.weekly_source_cycles
  where id=v_publication.source_cycle_id;
  if v_publication_cycle.source_group_id<>v_cycle.source_group_id
     or v_publication_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending then
    raise exception 'WEEKLY_SOURCE_QUERY_PUBLICATION_SCOPE_INVALID' using errcode='55000';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_publication.upload_id and state='CURRENT';
  if v_publication.authority_scope_kind='CYCLE' then
    if v_publication.report_scope_id is not null
       or v_publication_cycle.current_complete_upload_id is distinct from v_publication.upload_id
       or v_publication_cycle.current_projection_publication_id is distinct from v_publication.id
       or v_publication_cycle.version is distinct from v_publication.authority_scope_version
       or v_publication_cycle.projection_state<>'CURRENT' then
      raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
    end if;
  elsif v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id
      and source_cycle_id=v_publication_cycle.id;
    if not found
       or v_scope.current_complete_upload_id is distinct from v_publication.upload_id
       or v_scope.current_projection_publication_id is distinct from v_publication.id
       or v_scope.version is distinct from v_publication.authority_scope_version
       or v_scope.projection_state<>'CURRENT' then
      raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_QUERY_PUBLICATION_SCOPE_INVALID' using errcode='55000';
  end if;
  return pg_catalog.jsonb_build_object(
    'source_cycle_id',v_publication_cycle.id,
    'origin_source_cycle_id',v_cycle.id,
    'source_group_id',v_cycle.source_group_id,
    'finalisation_week_ending',v_publication_cycle.finalisation_week_ending,
    'publication_id',v_publication.id,
    'upload_id',v_publication.upload_id,
    'authority_scope_kind',v_publication.authority_scope_kind,
    'report_scope_id',v_publication.report_scope_id,
    'authority_scope_version',v_publication.authority_scope_version
  );
end;
$function$;

create or replace function private.weekly_source_query_response_fingerprint_v1(
  p_review_batch_id uuid,
  p_review_item_id uuid,
  p_incident_id uuid,
  p_incident_episode integer,
  p_comparison_revision_id uuid,
  p_comparison_fingerprint bytea
) returns bytea
language sql
stable
security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RESPONSE_FACT_V1',
    pg_catalog.jsonb_build_object(
      'review_batch_id',p_review_batch_id,
      'review_item_id',p_review_item_id,
      'incident_id',p_incident_id,
      'incident_episode',p_incident_episode,
      'comparison_revision_id',p_comparison_revision_id,
      'comparison_fingerprint',pg_catalog.encode(p_comparison_fingerprint,'hex')
    )
  );
$function$;

create or replace function private.weekly_source_query_candidate_timesheet_hash_v1(
  p_timesheet_id uuid
) returns bytea
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_CANDIDATE_SIGNED_TIMESHEET_V1',
    pg_catalog.jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'version',t.version,
      'contract_id',t.contract_id,
      'week_ending_date',t.week_ending_date,
      'actual_schedule_json',coalesce(t.actual_schedule_json,'[]'::jsonb),
      'additional_units_week',coalesce(t.additional_units_week,'{}'::jsonb),
      'additional_units_per_day',coalesce(t.additional_units_per_day,'{}'::jsonb),
      'nurse_evidence_sha256',t.img_sha256_nurse,
      'nurse_evidence_key',t.r2_nurse_key,
      'candidate_manager_approved_at_utc',t.candidate_manager_approved_at_utc
    )
  )
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id
    and t.is_current
    and t.revoked_at is null
    and t.archived_at_utc is null
    and t.sheet_scope='WEEKLY'
    and t.line_type='HOURS'
    and t.r2_nurse_key is not null
    and t.img_sha256_nurse is not null;
$function$;

create or replace function private.weekly_source_query_incident_policy_v1(
  p_incident_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_policy jsonb;
begin
  select * into strict v_incident
  from public.weekly_discrepancy_incidents where id=p_incident_id;
  select * into strict v_comparison
  from public.weekly_issue_comparison_revisions
  where id=v_incident.current_comparison_revision_id;
  select * into strict v_event
  from public.weekly_work_events where id=v_incident.work_event_id;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_incident.client_id,v_comparison.contract_id,v_event.work_date
  );
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_policy->>'document_mode'<>'CHECK_ONLY' then
    raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
  end if;
  return v_policy;
end;
$function$;

create or replace function private.weekly_source_query_notice_fanout_v1(
  p_incident_id uuid,
  p_event_id uuid,
  p_event_kind text,
  p_payload jsonb
) returns integer
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_inserted integer;
begin
  select * into strict v_incident
  from public.weekly_discrepancy_incidents where id=p_incident_id;
  if p_event_kind not in (
    'WEEKLY_CANDIDATE_SOURCE_DISPUTED',
    'WEEKLY_MANAGER_SYSTEM_CONFIRMED',
    'WEEKLY_MANAGER_SOURCE_CORRECTED'
  ) or pg_catalog.jsonb_typeof(p_payload)<>'object'
     or p_payload ?| array['pay','pay_rate','charge','charge_rate','margin','vat','invoice','banking'] then
    raise exception 'WEEKLY_SOURCE_NOTICE_PAYLOAD_INVALID' using errcode='22023';
  end if;
  insert into public.office_action_notifications(
    recipient_user_id,event_kind,issue_id,issue_generation,response_event_id,
    payload_json,dedupe_key,operational_state,resolved_at_utc
  )
  select
    office_user.id,p_event_kind,v_incident.id,v_incident.episode_number,p_event_id,
    p_payload,
    'WEEKLY_SOURCE_NOTICE_V1:'||p_event_id::text,
    case when v_incident.state='RESOLVED' then 'RESOLVED' else 'OPEN' end,
    case when v_incident.state='RESOLVED' then pg_catalog.transaction_timestamp() end
  from public.tms_users office_user
  where office_user.is_active
    and pg_catalog.lower(pg_catalog.btrim(office_user.role)) in ('admin','user')
  on conflict (recipient_user_id,response_event_id) do nothing;
  get diagnostics v_inserted=row_count;
  return v_inserted;
end;
$function$;

create or replace function private.weekly_source_query_comparison_fingerprint_v1(
  p_incident_id uuid,
  p_episode integer,
  p_work_event_id uuid,
  p_issue jsonb
) returns bytea
language sql
stable
security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_MATERIAL_COMPARISON_V1',
    pg_catalog.jsonb_build_object(
      'incident_id',p_incident_id,
      'episode_number',p_episode,
      'work_event_id',p_work_event_id,
      'candidate_timesheet_id',nullif(p_issue->>'candidate_timesheet_id','')::uuid,
      'candidate_timesheet_revision',nullif(p_issue->>'candidate_timesheet_revision','')::integer,
      'candidate_shift_fingerprint',pg_catalog.lower(nullif(p_issue->>'candidate_shift_fingerprint','')),
      'contract_id',nullif(p_issue->>'contract_id','')::uuid,
      'issue_family',pg_catalog.upper(pg_catalog.btrim(coalesce(p_issue->>'issue_family',''))),
      'source_presence',pg_catalog.upper(pg_catalog.btrim(coalesce(p_issue->>'source_presence',''))),
      'candidate_start_at_local',nullif(p_issue->>'candidate_start_at_local','')::timestamp,
      'candidate_end_at_local',nullif(p_issue->>'candidate_end_at_local','')::timestamp,
      'candidate_break_minutes',nullif(p_issue->>'candidate_break_minutes','')::integer,
      'system_start_at_local',nullif(p_issue->>'system_start_at_local','')::timestamp,
      'system_end_at_local',nullif(p_issue->>'system_end_at_local','')::timestamp,
      'system_break_minutes',nullif(p_issue->>'system_break_minutes','')::integer
    )
  );
$function$;

create or replace function public.weekly_source_query_sync_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_group_id uuid;
  v_upload_id uuid;
  v_scope_kind text;
  v_report_scope_id uuid;
  v_current_report_client_id uuid;
  v_final_authority_kind text;
  v_omission_meaning text;
  v_issue jsonb;
  v_event public.weekly_work_events%rowtype;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_policy jsonb;
  v_issue_family text;
  v_presence text;
  v_episode integer;
  v_revision integer;
  v_fingerprint bytea;
  v_expected bytea;
  v_contract_id uuid;
  v_timesheet_id uuid;
  v_timesheet_revision integer;
  v_candidate_shift_hash bytea;
  v_source_row_id uuid;
  v_source_link_id uuid;
  v_new_count integer:=0;
  v_changed_count integer:=0;
  v_unchanged_count integer:=0;
  v_resolved_count integer:=0;
  v_seen_work_events uuid[]:='{}'::uuid[];
  v_new_cohort_keys text[]:='{}'::text[];
  v_cohort_key text;
  v_is_new boolean;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','source_cycle_id','projection_publication_id','issues')
     ) or pg_catalog.jsonb_typeof(p_request->'issues')<>'array' then
    raise exception 'WEEKLY_SOURCE_QUERY_SYNC_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_QUERY_SYNC_REQUEST_INVALID' using errcode='22023';
  end;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  v_group_id:=(v_guard->>'source_group_id')::uuid;
  v_upload_id:=(v_guard->>'upload_id')::uuid;
  v_scope_kind:=v_guard->>'authority_scope_kind';
  v_report_scope_id:=nullif(v_guard->>'report_scope_id','')::uuid;
  select profile.final_authority_kind,profile.omission_meaning
  into strict v_final_authority_kind,v_omission_meaning
  from public.weekly_source_uploads upload
  join public.weekly_source_format_profiles profile
    on profile.id=upload.source_format_profile_id
  where upload.id=v_upload_id;
  if v_scope_kind='NHSP_REPORT_SCOPE' then
    select client_id into strict v_current_report_client_id
    from public.weekly_source_report_scopes
    where id=v_report_scope_id and source_cycle_id=v_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group_id,null,(v_guard->>'finalisation_week_ending')::date
  );
  perform 1 from public.weekly_source_cycles where id=v_cycle_id for update;

  for v_issue in
    select value
    from pg_catalog.jsonb_array_elements(p_request->'issues')
    order by value->>'work_event_id'
  loop
    if pg_catalog.jsonb_typeof(v_issue)<>'object'
       or exists(
         select 1 from pg_catalog.jsonb_object_keys(v_issue) key
         where key not in (
           'work_event_id','candidate_timesheet_id','candidate_timesheet_revision',
           'candidate_shift_fingerprint','source_row_id','source_work_event_link_id',
           'contract_id','issue_family','source_presence','candidate_start_at_local',
           'candidate_end_at_local','candidate_break_minutes','system_start_at_local',
           'system_end_at_local','system_break_minutes','expected_material_comparison_fingerprint'
         )
       ) then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_event from public.weekly_work_events
      where id=(v_issue->>'work_event_id')::uuid
        and first_source_group_id=v_group_id;
      v_contract_id:=nullif(v_issue->>'contract_id','')::uuid;
      v_timesheet_id:=nullif(v_issue->>'candidate_timesheet_id','')::uuid;
      v_timesheet_revision:=nullif(v_issue->>'candidate_timesheet_revision','')::integer;
      v_candidate_shift_hash:=case when nullif(v_issue->>'candidate_shift_fingerprint','') is null
        then null else private.weekly_source_query_hex32_v1(
          v_issue->>'candidate_shift_fingerprint','WEEKLY_SOURCE_CANDIDATE_SHIFT_HASH_INVALID'
        ) end;
      v_source_row_id:=nullif(v_issue->>'source_row_id','')::uuid;
      v_source_link_id:=nullif(v_issue->>'source_work_event_link_id','')::uuid;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_IDENTITY_INVALID' using errcode='22023';
    end;
    if v_event.id=any(v_seen_work_events) then
      raise exception 'WEEKLY_SOURCE_QUERY_DUPLICATE_WORK_EVENT' using errcode='22023';
    end if;
    v_seen_work_events:=pg_catalog.array_append(v_seen_work_events,v_event.id);
    v_issue_family:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_issue->>'issue_family','')));
    v_presence:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_issue->>'source_presence','')));
    if v_issue_family='CANDIDATE_TIMESHEET_MISSING' then
      raise exception 'WEEKLY_SOURCE_USE_TIMESHEET_SUBMISSION_REQUEST' using errcode='22023';
    end if;
    if v_issue_family not in (
      'SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER',
      'REFERENCE_MISSING','HEALTHROSTER_NOT_FINALISED'
    ) or v_presence not in ('PRESENT','ABSENT','UNFINALISED') then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_KIND_INVALID' using errcode='22023';
    end if;
    if v_issue_family='SOURCE_HOURS_DIFFER' and v_presence<>'PRESENT' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_presence<>'ABSENT' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_issue_family='HEALTHROSTER_NOT_FINALISED' and v_presence<>'UNFINALISED' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_timesheet_id is null or v_timesheet_revision is null or v_candidate_shift_hash is null
       or not exists(
         select 1 from public.timesheets t
         where t.timesheet_id=v_timesheet_id
           and t.version=v_timesheet_revision
           and t.contract_id=v_contract_id
           and t.week_ending_date>=v_event.work_date
           and t.week_ending_date<v_event.work_date+7
           and t.is_current and t.revoked_at is null and t.archived_at_utc is null
           and t.sheet_scope='WEEKLY' and t.line_type='HOURS'
           and t.r2_nurse_key is not null and t.img_sha256_nurse is not null
       ) then
      raise exception 'WEEKLY_SOURCE_SIGNED_TIMESHEET_REQUIRED' using errcode='55000';
    end if;
    if v_source_row_id is not null and not exists(
      select 1 from public.weekly_source_upload_rows r
      where r.id=v_source_row_id and r.upload_id=v_upload_id
    ) then
      raise exception 'WEEKLY_SOURCE_QUERY_SOURCE_ROW_STALE' using errcode='55000';
    end if;
    if v_source_link_id is not null and not exists(
      select 1
      from public.weekly_work_event_source_links link
      join public.weekly_source_row_resolutions resolution
        on resolution.id=link.row_resolution_id
      where link.id=v_source_link_id and link.work_event_id=v_event.id
        and (v_source_row_id is null or link.upload_row_id=v_source_row_id)
        and resolution.mapping_state='RESOLVED'
        and resolution.work_event_id=v_event.id
        and resolution.candidate_id=v_event.candidate_id
        and resolution.client_id=v_event.client_id
        and resolution.contract_id=v_contract_id
    ) then
      raise exception 'WEEKLY_SOURCE_QUERY_SOURCE_LINK_INVALID' using errcode='55000';
    end if;
    v_policy:=private._weekly_source_effective_policy_v1(
      v_event.client_id,v_contract_id,v_event.work_date
    );
    if v_policy->>'source_group_id'<>v_group_id::text
       or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
       or v_policy->>'document_mode'<>'CHECK_ONLY' then
      raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
    end if;

    select * into v_incident
    from public.weekly_discrepancy_incidents
    where source_group_id=v_group_id and work_event_id=v_event.id and state='OPEN'
    for update;
    v_is_new:=not found;
    if v_is_new then
      select coalesce(pg_catalog.max(episode_number),0)+1 into v_episode
      from public.weekly_discrepancy_incidents
      where source_group_id=v_group_id and work_event_id=v_event.id;
      insert into public.weekly_discrepancy_incidents(
        source_group_id,work_event_id,episode_number,candidate_id,client_id,
        source_cycle_id,state,reconciliation_state,candidate_action_state,
        manager_potential_state,manager_action_state,waiting_source_state
      ) values (
        v_group_id,v_event.id,v_episode,v_event.candidate_id,v_event.client_id,
        v_cycle_id,'OPEN','UNRESOLVED',
        case when coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
          then 'NOT_ASKED' else 'NOT_REQUIRED' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'AVAILABLE' else 'NOT_AVAILABLE' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'NOT_SENT' else 'NOT_REQUIRED' end,
        'NOT_WAITING'
      ) returning * into v_incident;
      v_new_count:=v_new_count+1;
      v_new_cohort_keys:=pg_catalog.array_append(
        v_new_cohort_keys,
        v_cycle_id::text||':'||v_event.candidate_id::text||':'||v_event.client_id::text
      );
    else
      if v_incident.candidate_id is distinct from v_event.candidate_id
         or v_incident.client_id is distinct from v_event.client_id then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      if v_incident.current_comparison_revision_id is null then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      select * into strict v_comparison
      from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      if v_comparison.contract_id is distinct from v_contract_id
         or exists(
           select 1
           from public.weekly_source_cycles origin_cycle
           where origin_cycle.id=v_incident.source_cycle_id
             and origin_cycle.finalisation_week_ending>(v_guard->>'finalisation_week_ending')::date
         ) then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      v_episode:=v_incident.episode_number;
    end if;
    if v_is_new then
      v_comparison.id:=null;
    end if;
    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_event.id,v_issue
    );
    if not v_is_new and v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      v_episode:=v_incident.episode_number+1;
      v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
        v_incident.id,v_episode,v_event.id,v_issue
      );
      update public.weekly_discrepancy_incidents
      set episode_number=v_episode,reconciliation_state='UNRESOLVED',
          candidate_action_state=case
            when coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
              then 'NOT_ASKED'
            else 'NOT_REQUIRED'
          end,
          manager_potential_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'AVAILABLE'
            else 'NOT_AVAILABLE'
          end,
          manager_action_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'NOT_SENT'
            else 'NOT_REQUIRED'
          end,
          waiting_source_state='NOT_WAITING',resolved_at_utc=null,resolution_kind=null
      where id=v_incident.id;
      update public.office_action_notifications
      set operational_state='RESOLVED',resolved_at_utc=pg_catalog.transaction_timestamp()
      where issue_id=v_incident.id and operational_state='OPEN';
      v_incident.episode_number:=v_episode;
      v_incident.candidate_action_state:=case
        when coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
          then 'NOT_ASKED'
        else 'NOT_REQUIRED'
      end;
      v_incident.manager_action_state:=case
        when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
             and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'NOT_SENT'
        else 'NOT_REQUIRED'
      end;
      v_changed_count:=v_changed_count+1;
      v_new_cohort_keys:=pg_catalog.array_append(
        v_new_cohort_keys,
        v_incident.source_cycle_id::text||':'||v_event.candidate_id::text||':'||v_event.client_id::text
      );
    end if;
    if nullif(v_issue->>'expected_material_comparison_fingerprint','') is not null then
      v_expected:=private.weekly_source_query_hex32_v1(
        v_issue->>'expected_material_comparison_fingerprint',
        'WEEKLY_SOURCE_COMPARISON_HASH_INVALID'
      );
      if v_expected is distinct from v_fingerprint then
        raise exception 'WEEKLY_SOURCE_COMPARISON_HASH_MISMATCH' using errcode='40001';
      end if;
    end if;
    if v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint=v_fingerprint then
      v_unchanged_count:=v_unchanged_count+1;
    else
      select coalesce(pg_catalog.max(revision_number),0)+1 into v_revision
      from public.weekly_issue_comparison_revisions
      where incident_id=v_incident.id;
      insert into public.weekly_issue_comparison_revisions(
        incident_id,revision_number,projection_publication_id,comparison_upload_id,
        final_revision_id,candidate_timesheet_id,candidate_timesheet_revision,
        candidate_shift_fingerprint,source_row_id,source_work_event_link_id,
        contract_id,issue_family,source_presence,candidate_start_at_local,
        candidate_end_at_local,candidate_break_minutes,system_start_at_local,
        system_end_at_local,system_break_minutes,material_comparison_fingerprint
      ) values (
        v_incident.id,v_revision,v_publication_id,v_upload_id,null,
        v_timesheet_id,v_timesheet_revision,v_candidate_shift_hash,v_source_row_id,
        v_source_link_id,v_contract_id,v_issue_family,v_presence,
        nullif(v_issue->>'candidate_start_at_local','')::timestamp,
        nullif(v_issue->>'candidate_end_at_local','')::timestamp,
        nullif(v_issue->>'candidate_break_minutes','')::integer,
        nullif(v_issue->>'system_start_at_local','')::timestamp,
        nullif(v_issue->>'system_end_at_local','')::timestamp,
        nullif(v_issue->>'system_break_minutes','')::integer,
        v_fingerprint
      ) returning * into v_comparison;
      update public.weekly_discrepancy_incidents
      set current_comparison_revision_id=v_comparison.id
      where id=v_incident.id;
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication_id,v_fingerprint,
        'SOURCE_RECHECKED','SYSTEM',
        pg_catalog.jsonb_build_object(
          'issue_family',v_issue_family,'source_presence',v_presence,
          'comparison_revision_id',v_comparison.id
        ),
        'SOURCE_RECHECKED:'||v_incident.id::text||':'||pg_catalog.encode(v_fingerprint,'hex')
      ) on conflict (event_kind,idempotency_key) do nothing;
    end if;
  end loop;

  for v_incident in
    select incident.*
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_source_cycles origin_cycle
      on origin_cycle.id=incident.source_cycle_id
    where incident.source_group_id=v_group_id
      and incident.state='OPEN'
      and not (incident.work_event_id=any(v_seen_work_events))
      and origin_cycle.finalisation_week_ending<=(v_guard->>'finalisation_week_ending')::date
      and (
        v_scope_kind='CYCLE'
        or (v_scope_kind='NHSP_REPORT_SCOPE' and incident.client_id=v_current_report_client_id)
      )
      and (
        v_omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE'
        or (
          v_final_authority_kind='NHSP_TRUST_BACKING_REPORT'
          and exists(
            select 1
            from public.weekly_work_event_source_links current_link
            join public.weekly_source_upload_rows current_row
              on current_row.id=current_link.upload_row_id
            join public.weekly_source_row_resolutions current_resolution
              on current_resolution.id=current_link.row_resolution_id
            where current_row.upload_id=v_upload_id
              and current_link.work_event_id=incident.work_event_id
              and current_link.link_kind in ('PROVISIONAL_SOURCE','POSITIVE_SOURCE')
              and current_row.row_finalisation_state='SOURCE_WORKED'
              and current_resolution.mapping_state='RESOLVED'
              and current_resolution.work_event_id=incident.work_event_id
              and current_resolution.candidate_id=incident.candidate_id
              and current_resolution.client_id=incident.client_id
              and current_resolution.contract_id=comparison.contract_id
              and current_row.start_at_local=comparison.candidate_start_at_local
              and current_row.end_at_local=comparison.candidate_end_at_local
              and current_row.break_minutes=comparison.candidate_break_minutes
          )
        )
      )
      and not exists(
        select 1
        from public.weekly_source_upload_rows current_row
        join public.weekly_source_row_resolutions current_resolution
          on current_resolution.upload_row_id=current_row.id
        where current_row.upload_id=v_upload_id
          and current_resolution.mapping_state='RESOLVED'
          and current_resolution.work_event_id=incident.work_event_id
          and (
            current_resolution.candidate_id is distinct from incident.candidate_id
            or current_resolution.client_id is distinct from incident.client_id
            or current_resolution.contract_id is distinct from comparison.contract_id
          )
      )
    order by incident.id
    for update of incident
  loop
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
        candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
        manager_action_state='NOT_REQUIRED',waiting_source_state='SOURCE_MATCHED',
        resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind='SOURCE_MATCHED'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships
    set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items
    set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications
    set operational_state='RESOLVED',resolved_at_utc=pg_catalog.transaction_timestamp()
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,null,
      'RESOLVED','SYSTEM',pg_catalog.jsonb_build_object('resolution','SOURCE_MATCHED'),
      'SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication_id::text
    ) on conflict (event_kind,idempotency_key) do nothing;
    v_resolved_count:=v_resolved_count+1;
  end loop;

  for v_cohort_key in
    select distinct key_value collate "C" as key_value
    from pg_catalog.unnest(v_new_cohort_keys) as key_value
    order by key_value
  loop
    perform private.weekly_source_query_restart_activated_cohort_v1(
      pg_catalog.split_part(v_cohort_key,':',1)::uuid,
      pg_catalog.split_part(v_cohort_key,':',2)::uuid,
      pg_catalog.split_part(v_cohort_key,':',3)::uuid,
      v_publication_id
    );
  end loop;

  perform public._audit_insert(
    'weekly_source_query_sync',v_publication_id::text,'WEEKLY_SOURCE_QUERY_SYNCED',
    null,
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle_id,'new_incidents',v_new_count,
      'changed_comparisons',v_changed_count,'unchanged_incidents',v_unchanged_count,
      'resolved_incidents',v_resolved_count
    ),
    'Current Weekly source comparison issues synchronised',v_actor
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'source_cycle_id',v_cycle_id,'projection_publication_id',v_publication_id,
    'new_incidents',v_new_count,'changed_comparisons',v_changed_count,
    'unchanged_incidents',v_unchanged_count,'resolved_incidents',v_resolved_count
  );
end;
$function$;

-- The broker/service boundary is the only callable API surface. Candidate and
-- manager browsers never receive direct function or table privileges.
do $acl$
declare
  v_name text;
  v_public_names constant text[]:=array[
    'weekly_source_candidate_query_get_v1',
    'weekly_source_message_render_input_v1',
    'weekly_source_timesheet_submission_complete_atomic_v1',
    'weekly_source_query_accept_system_hours_atomic_v1',
    'weekly_source_office_notifications_list_v1',
    'weekly_source_office_notification_ack_atomic_v1',
    'weekly_source_manager_review_get_v1',
    'weekly_source_manager_review_respond_atomic_v1',
    'weekly_source_message_dispatch_claim_v1',
    'weekly_source_message_dispatch_submission_start_atomic_v1',
    'weekly_source_message_dispatch_result_atomic_v1',
    'weekly_source_manager_route_prepare_atomic_v1',
    'weekly_source_message_render_stage_atomic_v1',
    'weekly_source_query_scheduler_tick_v1',
    'weekly_source_candidate_response_submit_atomic_v1',
    'weekly_source_query_send_manager_now_atomic_v1',
    'weekly_source_candidate_reminder_atomic_v1',
    'weekly_source_query_ask_candidate_atomic_v1',
    'weekly_source_timesheet_submission_request_start_atomic_v1',
    'weekly_source_query_sync_atomic_v1'
  ];
  v_private_names constant text[]:=array[
    'weekly_source_query_require_service_v1',
    'weekly_source_query_manager_row_owned_v1',
    'weekly_source_query_manager_generation_v1',
    'weekly_source_query_manager_intent_v1',
    'weekly_source_query_restart_activated_cohort_v1',
    'weekly_source_query_cohort_ensure_v1',
    'weekly_source_query_candidate_intent_v1',
    'weekly_source_query_candidate_generation_v1',
    'weekly_source_query_hex32_v1',
    'weekly_source_query_ascii_fold_v1',
    'weekly_source_query_normalise_recipient_v1',
    'weekly_source_query_current_publication_v1',
    'weekly_source_query_response_fingerprint_v1',
    'weekly_source_query_candidate_timesheet_hash_v1',
    'weekly_source_query_incident_policy_v1',
    'weekly_source_query_notice_fanout_v1',
    'weekly_source_query_comparison_fingerprint_v1'
  ];
begin
  if (select pg_catalog.count(*)
      from pg_catalog.pg_proc procedure
      join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
      where namespace.nspname='public' and procedure.proname=any(v_public_names)
        and pg_catalog.pg_get_function_identity_arguments(procedure.oid)='p_request jsonb')
     <>pg_catalog.array_length(v_public_names,1) then
    raise exception 'WEEKLY_SOURCE_QUERY_PUBLIC_RPC_SURFACE_INCOMPLETE';
  end if;
  foreach v_name in array v_public_names loop
    execute pg_catalog.format('alter function public.%I(jsonb) owner to postgres',v_name);
    execute pg_catalog.format(
      'revoke all on function public.%I(jsonb) from public,anon,authenticated,service_role',v_name
    );
    execute pg_catalog.format('grant execute on function public.%I(jsonb) to service_role',v_name);
  end loop;
  foreach v_name in array v_private_names loop
    if not exists(
      select 1 from pg_catalog.pg_proc procedure
      join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
      where namespace.nspname='private' and procedure.proname=v_name
    ) then
      raise exception 'WEEKLY_SOURCE_QUERY_PRIVATE_ROUTINE_MISSING: %',v_name;
    end if;
  end loop;
  for v_name in
    select pg_catalog.format('%I.%I(%s)',namespace.nspname,procedure.proname,
             pg_catalog.pg_get_function_identity_arguments(procedure.oid))
    from pg_catalog.pg_proc procedure
    join pg_catalog.pg_namespace namespace on namespace.oid=procedure.pronamespace
    where namespace.nspname='private' and procedure.proname=any(v_private_names)
    order by procedure.proname,pg_catalog.pg_get_function_identity_arguments(procedure.oid)
  loop
    execute 'alter function '||v_name||' owner to postgres';
    execute 'revoke all on function '||v_name||' from public,anon,authenticated,service_role';
  end loop;
end;
$acl$;

comment on function public.weekly_source_query_sync_atomic_v1(jsonb) is
  'Service-only, non-financial synchronisation of current Weekly source discrepancy facts.';
comment on function public.weekly_source_manager_review_respond_atomic_v1(jsonb) is
  'Service-only broker boundary for receipt-first, partial manager review responses.';
comment on function public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb) is
  'Service-only durable pre-provider submission fence; this function performs no provider call.';
comment on function public.weekly_source_manager_route_prepare_atomic_v1(jsonb) is
  'Service-only reservation of the exact manager review batch and credential generation before remote registration and deterministic email rendering.';

notify pgrst,'reload schema';

commit;
