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
    and generation.source_cycle_id=v_submission.source_cycle_id and generation.candidate_id=v_submission.candidate_id
    and generation.generation_number=v_submission.request_generation and generation.membership_hash=v_submission.membership_hash
    and generation.request_kind='SUBMIT_TIMESHEET' and generation.state='ACTIVE'
    and cohort.current_submission_generation_id=generation.id
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
          and not private.weekly_source_covered_hours_incident_v1(incident.id)
          and not private.weekly_source_query_manager_row_owned_v1(
            v_route_id,incident.id,incident.episode_number
          );
        insert into public.weekly_route_activations(
          source_cycle_id,candidate_id,client_id,audience_route,route_mode,
          activated_by_user_id,activated_at_utc,updated_at_utc
        ) values (
          v_submission.source_cycle_id,v_candidate_id,v_membership.client_id,
          'MANAGER','CANDIDATE_FIRST',null,pg_catalog.transaction_timestamp(),pg_catalog.transaction_timestamp()
        ) on conflict (source_cycle_id,candidate_id,client_id,audience_route)
        do nothing
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