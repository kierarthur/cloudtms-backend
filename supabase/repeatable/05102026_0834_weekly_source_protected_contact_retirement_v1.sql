begin;

-- Contact lifecycle only. A successfully qualified protected decision moves
-- its Office work to Protected shifts, without resolving the retained hours
-- incident used by the pay gate, changing money, or retiring sibling questions.
-- Completion owners call this AFTER their positive accepted receipt exists.
create or replace function private.weekly_source_protected_contact_retire_v1(
  p_family_id uuid, p_work_event_id uuid
) returns integer
language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_count integer:=0;
  v_completed_generations uuid[];
begin
  if p_family_id is null or p_work_event_id is null or not exists (
    select 1 from public.weekly_exceptional_pay_target_families family
    join public.weekly_work_events work on work.id=p_work_event_id
      and work.candidate_id=family.candidate_id
      and work.work_date between family.week_start_date and family.week_ending_date
    join public.contracts contract on contract.id=family.contract_id
      and contract.client_id=work.client_id
    where family.id=p_family_id and exists (
      select 1 from public.weekly_exceptional_pay_family_events event
      where event.family_id=family.id and event.durable_work_event_id=work.id
    )
  ) then
    raise exception 'WEEKLY_SOURCE_PROTECTED_CONTACT_SCOPE_INVALID' using errcode='55000';
  end if;
  for v_incident in
    select incident.* from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_exceptional_pay_target_families family
      on family.id=p_family_id and family.candidate_id=incident.candidate_id
      and family.contract_id=comparison.contract_id
    where incident.work_event_id=p_work_event_id and incident.state='OPEN'
    order by incident.id for update of incident
  loop
    -- A WAIT label, an unconfirmed Save, or an incomplete transport request is
    -- insufficient. Use the same evidence that actually removes the pay hold.
    if not private.weekly_source_covered_hours_incident_v1(v_incident.id) then continue; end if;
    update public.weekly_candidate_outreach_memberships membership set state='RESOLVED'
      from public.weekly_candidate_outreach_generations generation
      where membership.incident_id=v_incident.id and membership.state in ('ACTIONABLE','ANSWERED')
        and generation.id=membership.candidate_generation_id
        and generation.request_kind='CHECK_HOURS';
    -- Reuse the existing answered-hours completion rule. Keep a mixed cohort
    -- active while another question needs an answer; never finish the separate
    -- missing-Timesheet request or rewrite accepted delivery history.
    with completed as (
      update public.weekly_candidate_outreach_generations generation set state='COMPLETE'
      where generation.state='ACTIVE' and generation.request_kind='CHECK_HOURS'
        and exists(select 1 from public.weekly_candidate_outreach_memberships membership
          where membership.candidate_generation_id=generation.id
            and membership.incident_id=v_incident.id and membership.state='RESOLVED')
        and not exists(select 1 from public.weekly_candidate_outreach_memberships membership
          where membership.candidate_generation_id=generation.id and membership.state='ACTIONABLE')
      returning generation.id
    ) select array_agg(id) into v_completed_generations from completed;
    update public.weekly_message_intents set state='RETIRED'
      where candidate_generation_id=any(v_completed_generations) and state in ('DUE','RENDERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
      where incident_id=v_incident.id and incident_episode=v_incident.episode_number
        and response_state='UNANSWERED';
    update public.office_action_notifications set operational_state='RESOLVED',
      resolved_at_utc=transaction_timestamp()
      where issue_id=v_incident.id and operational_state='OPEN';
    v_count:=v_count+1;
  end loop;
  return v_count;
end;
$function$;
alter function private.weekly_source_protected_contact_retire_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_protected_contact_retire_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
