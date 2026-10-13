-- Read-only Office contact projection. The existing atomic query owners remain
-- the only authority to queue messages and recheck policy at execution time.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_office_contact_policy_v1(
  p_source_cycle_id uuid, p_query jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_candidate uuid:=(p_query->>'candidate_id')::uuid;
  v_client uuid:=(p_query->>'client_id')::uuid;
  v_incidents uuid[];
  v_missing jsonb:=coalesce(p_query->'missing_scopes','[]'::jsonb);
  v_scope jsonb;
  v_policy jsonb;
  v_fact record;
  v_candidate_enabled boolean:=true;
  v_manager_enabled boolean:=true;
  v_recipient text;
  v_address text;
  v_recipient_ambiguous boolean:=false;
  v_app boolean:=coalesce((p_query->>'candidate_app_available')::boolean,false);
  v_candidate_reason text;
  v_manager_reason text;
  v_candidate_state text:='READY';
  v_manager_state text:='READY';
  v_route public.weekly_manager_recipient_routes%rowtype;
  v_reminder_at timestamptz;
  v_all_asked boolean:=true;
  v_all_owned boolean:=true;
begin
  perform private.weekly_source_query_require_service_v1();
  select coalesce(array_agg(value::uuid),'{}'::uuid[]) into v_incidents
    from jsonb_array_elements_text(coalesce(p_query->'incident_ids','[]'::jsonb));
  for v_fact in
    select incident.*,comparison.contract_id,event.work_date
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events event on event.id=incident.work_event_id
    where incident.id=any(v_incidents) and incident.source_cycle_id=p_source_cycle_id
      and incident.candidate_id=v_candidate and incident.client_id=v_client
  loop
    v_policy:=private._weekly_source_effective_policy_v1(v_client,v_fact.contract_id,v_fact.work_date);
    v_candidate_enabled:=v_candidate_enabled and coalesce((v_policy->>'candidate_queries_enabled')::boolean,false);
    v_manager_enabled:=v_manager_enabled and coalesce((v_policy->>'manager_queries_enabled')::boolean,false);
    v_address:=private.weekly_source_query_normalise_recipient_v1(v_policy->>'manager_query_recipient');
    if v_address is null then v_recipient_ambiguous:=true;
    elsif v_recipient is null then v_recipient:=v_address;
    elsif v_address<>v_recipient then v_recipient_ambiguous:=true; end if;
    v_all_asked:=v_all_asked and v_fact.candidate_action_state<>'NOT_ASKED';
  end loop;
  for v_scope in select value from jsonb_array_elements(v_missing)
  loop
    v_policy:=private._weekly_source_effective_policy_v1(
      (v_scope->>'client_id')::uuid,(v_scope->>'contract_id')::uuid,(v_scope->>'week_ending')::date);
    v_candidate_enabled:=v_candidate_enabled and coalesce((v_policy->>'candidate_queries_enabled')::boolean,false);
  end loop;

  -- An existing identical request is not a new message. Reminders keep their
  -- established separate owner and cooldown; do not reset the request clock.
  if jsonb_array_length(v_missing)>0 then
    v_all_asked:=not exists(select 1 from jsonb_array_elements(v_missing) scope
      where not exists(select 1 from public.weekly_timesheet_submission_requests request
        join public.weekly_timesheet_submission_request_memberships membership on membership.submission_request_id=request.id
        where request.source_cycle_id=p_source_cycle_id and request.candidate_id=v_candidate
          and request.state in ('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
          and membership.state='WAITING'
          and membership.client_id=(scope->>'client_id')::uuid
          and membership.contract_id=(scope->>'contract_id')::uuid
          and membership.week_ending=(scope->>'week_ending')::date
          and private.weekly_source_missing_scope_unchanged_v1(request.current_projection_publication_id,
            (p_query->>'projection_publication_id')::uuid,v_candidate,membership.client_id,
            membership.contract_id,membership.week_ending)));
  end if;
  select max(generation.manual_reminder_available_at_utc) into v_reminder_at
  from public.weekly_candidate_cohorts cohort
  join public.weekly_candidate_outreach_generations generation on generation.candidate_cohort_id=cohort.id
    and generation.state='ACTIVE'
  where cohort.source_cycle_id=p_source_cycle_id and cohort.candidate_id=v_candidate
    and cohort.client_id=v_client
    and generation.request_kind=case when jsonb_array_length(v_missing)>0 then 'SUBMIT_TIMESHEET' else 'CHECK_HOURS' end;

  if not v_candidate_enabled then
    v_candidate_state:='DISABLED';v_candidate_reason:='Candidate queries are disabled by the effective settings.';
  elsif not v_app then
    v_candidate_state:='NO_ACTIVE_APP';v_candidate_reason:='No active MyTMS access for this agency.';
  elsif not coalesce((p_query->>'outreach_eligible')::boolean,false) then
    v_candidate_state:='NOT_APPLICABLE';v_candidate_reason:='No candidate question is eligible for this workflow.';
  elsif v_all_asked then
    v_candidate_state:=case when v_reminder_at>transaction_timestamp() then 'COOLDOWN' else 'ALREADY_REQUESTED' end;
    v_candidate_reason:=case when v_reminder_at>transaction_timestamp()
      then 'Already requested. A reminder is available after '||to_char(v_reminder_at at time zone 'Europe/London','DD Mon YYYY HH24:MI')||' UK.'
      else 'Already requested. Open the question to check replies or send an eligible reminder.' end;
  end if;

  select route.* into v_route from public.weekly_manager_recipient_routes route
    join public.weekly_source_cycles cycle on cycle.id=route.source_cycle_id
    join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
  where route.source_cycle_id=p_source_cycle_id and route.environment=source_group.environment
    and route.agency_id=source_group.agency_id
    and route.normalised_recipient_hash=decode(replace(p_query->>'manager_recipient_route_key','\x',''),'hex');
  select coalesce(bool_and(private.weekly_source_query_manager_row_owned_v1(v_route.id,incident.id,incident.episode_number)),false)
    into v_all_owned from public.weekly_discrepancy_incidents incident where incident.id=any(v_incidents);
  if jsonb_array_length(v_missing)>0 or cardinality(v_incidents)=0 then
    v_manager_state:='NOT_APPLICABLE';v_manager_reason:='Candidate timesheet required before this manager query.';
  elsif not v_manager_enabled then
    v_manager_state:='DISABLED';v_manager_reason:='Manager queries are disabled by the effective settings.';
  elsif v_recipient is null or v_recipient_ambiguous then
    v_manager_state:='NO_RECIPIENT';v_manager_reason:='No single manager email recipient is configured for these shifts.';
  elsif not coalesce((p_query->>'manager_eligible')::boolean,false) then
    v_manager_state:='NOT_APPLICABLE';v_manager_reason:='No unresolved manager question is eligible.';
  elsif v_route.manager_send_available_at_utc>transaction_timestamp() then
    v_manager_state:='COOLDOWN';v_manager_reason:='Available after '||to_char(v_route.manager_send_available_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI')||' UK.';
  elsif v_all_owned then
    v_manager_state:='ALREADY_SENT';v_manager_reason:='These questions already belong to an accepted manager message.';
  elsif exists(select 1 from public.weekly_message_intents intent
    join public.weekly_manager_recipient_memberships membership on membership.recipient_generation_id=intent.recipient_generation_id
    join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
    where intent.recipient_route_id=v_route.id and intent.state in ('DUE','RENDERED')
      and membership.incident_id=any(v_incidents)
      and membership.comparison_revision_id=incident.current_comparison_revision_id) then
    v_manager_state:='QUEUED';v_manager_reason:='A manager message for these questions is already queued. Check its delivery status.';
  end if;

  return jsonb_build_object('contract','WEEKLY_SOURCE_CONTACT_POLICY_V1','checked_at_utc',transaction_timestamp(),
    'candidate',jsonb_build_object('eligible',v_candidate_state='READY','state',v_candidate_state,
      'reason',v_candidate_reason,'recipient_key','candidate:'||v_candidate::text,
      'recipient',p_query->>'candidate_name','channel','MyTMS notification',
      'request_kind',case when jsonb_array_length(v_missing)>0 then 'SUBMIT_TIMESHEET' else 'CHECK_HOURS' end,
      'summary',case when jsonb_array_length(v_missing)>0 then 'Ask the candidate to submit the outstanding timesheet(s).'
        else 'Ask the candidate to review the imported hours and respond in MyTMS.' end,
      'available_at_utc',v_reminder_at),
    'manager',jsonb_build_object('eligible',v_manager_state='READY','state',v_manager_state,
      'reason',v_manager_reason,'recipient_key','manager:'||coalesce(v_recipient,'unconfigured'),
      'recipient',case when v_manager_state not in ('DISABLED','NO_RECIPIENT') then v_recipient end,
      'channel','Email','request_kind','REVIEW_HOURS',
      'summary','Ask the configured manager to review the selected hours questions using a secure link.',
      'available_at_utc',v_route.manager_send_available_at_utc));
end;
$function$;
alter function private.weekly_source_office_contact_policy_v1(uuid,jsonb) owner to postgres;
-- Called only by the owner-executed Office workspace RPC, never exposed directly.
revoke all on function private.weekly_source_office_contact_policy_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
