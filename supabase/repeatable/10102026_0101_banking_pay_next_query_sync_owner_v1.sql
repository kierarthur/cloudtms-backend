-- F-25 actual public QuerySync root: prelock entry, genuine original branches,
-- exact original omission cursor and complete cohort/outreach/audit true tail.
-- Design: F-25-query-sync-completion-build.md. No nested or Source closure claim.
\set ON_ERROR_STOP on
begin;
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
  v_bpay_producer uuid;v_bpay_outer boolean;v_bpay_request_sha bytea;
  v_bpay_input_ids uuid[]:='{}'::uuid[];v_bpay_issue_hashes bytea[]:='{}'::bytea[];
  v_bpay_omitted_ids uuid[]:='{}'::uuid[];v_bpay_completion_sha bytea;v_bpay_original_event uuid;
  v_bpay_prior_comparison uuid;v_bpay_outcome text;
  v_bpay_input private.bpay_next_query_sync_input_completion%rowtype;
  v_bpay_omission private.bpay_next_query_sync_omission_completion%rowtype;
  v_bpay_inputs private.bpay_next_query_sync_input_completion[]:='{}'::private.bpay_next_query_sync_input_completion[];
  v_bpay_omissions private.bpay_next_query_sync_omission_completion[]:='{}'::private.bpay_next_query_sync_omission_completion[];
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
  -- Pure Office/header authority above wins before the stable NEXT entry.
  -- Public Sync has no outer business receipt under a different parent; its
  -- different private Candidate comparator has its own child protocol.
  if exists(select 1 from private.bpay_next_query_producer q where q.preparing_transaction_id=pg_catalog.pg_current_xact_id()) then
    raise exception using errcode='55000',message='BPAY_NEXT_PUBLIC_QUERY_SYNC_NESTED_OWNER_NOT_INSTALLED';end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform 1 from private.bpay_next_module_control m where m.id=1 for share;
  -- Keep this actual owner decision stable across NEXT-only pure hashing and
  -- the reentrant producer entry. Source admission precedes module ownership.
  if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
    if pg_catalog.jsonb_array_length(p_request->'issues')>1000 then
      raise exception using errcode='22023',message='BPAY_NEXT_QUERY_SYNC_INPUT_SET_TOO_LARGE';end if;
    select coalesce(array_agg((issue->>'work_event_id')::uuid order by (issue->>'work_event_id')::uuid),'{}'::uuid[]),
       coalesce(array_agg(private.bpay_next_query_sync_issue_digest_v1(issue) order by (issue->>'work_event_id')::uuid),'{}'::bytea[])
    into v_bpay_input_ids,v_bpay_issue_hashes from pg_catalog.jsonb_array_elements(p_request->'issues') issue;
    if pg_catalog.cardinality(v_bpay_input_ids)<>(select count(distinct id) from pg_catalog.unnest(v_bpay_input_ids) id) then
      raise exception 'WEEKLY_SOURCE_QUERY_DUPLICATE_WORK_EVENT' using errcode='22023';end if;
    v_bpay_request_sha:=private.bpay_next_query_sync_request_digest_v1(v_actor,v_cycle_id,v_publication_id,v_bpay_input_ids,v_bpay_issue_hashes);
  else
    -- Stable other-owner entry returns NULL; preserve all original validation,
    -- lock order and business behavior without adding protocol certificates.
    v_bpay_request_sha:=pg_catalog.sha256(pg_catalog.convert_to('BPAY_NEXT_QUERY_SYNC_INACTIVE_V1','UTF8'));
  end if;
  select producer_id,is_outer into v_bpay_producer,v_bpay_outer from private.bpay_next_query_producer_enter_v1(
    'QUERY_SYNC','QUERY_SYNC','QUERY_SYNC:'||pg_catalog.encode(v_bpay_request_sha,'hex'),v_bpay_request_sha,'STANDALONE_QUERY');
  if v_bpay_producer is not null and not v_bpay_outer then
    raise exception using errcode='55000',message='BPAY_NEXT_PUBLIC_QUERY_SYNC_OUTER_REQUIRED';end if;
  perform 1 from public.weekly_source_cycles where id=v_cycle_id for update;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group_id,null,(v_guard->>'finalisation_week_ending')::date);

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
        and first_source_group_id=v_group_id
        and (nullif(v_guard->>'scope_client_id','') is null
          or client_id=(v_guard->>'scope_client_id')::uuid);
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

    perform 1 from public.weekly_work_events where id=v_event.id for update;
    if private.weekly_source_query_resolved_decision_matches_v1(v_group_id,v_event.id,v_issue) then
      if v_bpay_producer is not null then
        -- Capture the actual latest original resolved episode, not today's
        -- upload/Timesheet/fingerprint. The original nine-fact predicate wins
        -- before the existing expected-material-fingerprint equality below.
        select i.* into strict v_incident from public.weekly_discrepancy_incidents i
        where i.source_group_id=v_group_id and i.work_event_id=v_event.id
        order by i.episode_number desc,i.id desc limit 1 for update;
        if not private.weekly_source_query_resolved_decision_matches_v1(v_group_id,v_event.id,v_issue) then
          raise exception using errcode='40001',message='BPAY_NEXT_QUERY_SYNC_ORIGINAL_DECISION_CHANGED';end if;
        select c.* into strict v_comparison from public.weekly_issue_comparison_revisions c where c.id=v_incident.current_comparison_revision_id;
        select e.id into strict v_bpay_original_event from public.weekly_discrepancy_events e
        where e.incident_id=v_incident.id and e.issue_episode=v_incident.episode_number and e.event_kind='RESOLVED'
          and e.expected_comparison_fingerprint=v_comparison.material_comparison_fingerprint
          and e.bounded_payload_json->>'resolution'=v_incident.resolution_kind;
        perform private.bpay_next_query_resolved_evidence_assert_v1(v_comparison.id,v_bpay_original_event);
        v_bpay_input:=null;v_bpay_input.completion_id:=v_bpay_producer;v_bpay_input.work_event_id:=v_event.id;
        v_bpay_input.outcome:='UNCHANGED_RESOLVED';v_bpay_input.incident_id:=v_incident.id;
        v_bpay_input.issue_episode:=v_incident.episode_number;v_bpay_input.comparison_revision_id:=v_comparison.id;
        v_bpay_input.resolved_decision_event_id:=v_bpay_original_event;
        v_bpay_input.input_issue_sha256:=private.bpay_next_query_sync_issue_digest_v1(v_issue);
        v_bpay_input.matched_facts_sha256:=private.bpay_next_query_matched_facts_digest_v1(
          v_contract_id,v_issue_family,v_presence,nullif(v_issue->>'candidate_start_at_local','')::timestamp,
          nullif(v_issue->>'candidate_end_at_local','')::timestamp,nullif(v_issue->>'candidate_break_minutes','')::integer,
          nullif(v_issue->>'system_start_at_local','')::timestamp,nullif(v_issue->>'system_end_at_local','')::timestamp,
          nullif(v_issue->>'system_break_minutes','')::integer);
        v_bpay_input.evidence_sha256:=private.bpay_next_query_sync_input_completion_digest_v1(v_bpay_input);
        v_bpay_inputs:=pg_catalog.array_append(v_bpay_inputs,v_bpay_input);
      end if;
      v_unchanged_count:=v_unchanged_count+1;
      continue;
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
    v_bpay_prior_comparison:=v_comparison.id;
    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_event.id,v_issue
    );
    v_bpay_outcome:=case when v_is_new then 'NEW'
      when v_comparison.material_comparison_fingerprint is distinct from v_fingerprint then 'CHANGED'
      else 'UNCHANGED_OPEN' end;
    if not v_is_new and v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      -- A changed source tuple is a new comparison revision of the same open
      -- episode. Only a resolved work event that reappears starts a new episode.
      -- Preserve route activation, due times and accepted manager memberships.
      v_changed_count:=v_changed_count+1;
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
    if v_bpay_producer is not null then
      v_bpay_input:=null;v_bpay_input.completion_id:=v_bpay_producer;v_bpay_input.work_event_id:=v_event.id;
      v_bpay_input.outcome:=v_bpay_outcome;v_bpay_input.incident_id:=v_incident.id;
      v_bpay_input.issue_episode:=v_incident.episode_number;v_bpay_input.comparison_revision_id:=v_comparison.id;
      v_bpay_input.input_issue_sha256:=private.bpay_next_query_sync_issue_digest_v1(v_issue);
      if v_bpay_outcome='CHANGED' then v_bpay_input.prior_comparison_revision_id:=v_bpay_prior_comparison;end if;
      if v_bpay_outcome in ('NEW','CHANGED') then
        select e.id into strict v_bpay_input.recheck_event_id from public.weekly_discrepancy_events e
        where e.event_kind='SOURCE_RECHECKED' and e.idempotency_key='SOURCE_RECHECKED:'||v_incident.id::text||':'||pg_catalog.encode(v_fingerprint,'hex');
      end if;
      v_bpay_input.evidence_sha256:=private.bpay_next_query_sync_input_completion_digest_v1(v_bpay_input);
      v_bpay_inputs:=pg_catalog.array_append(v_bpay_inputs,v_bpay_input);
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
      and (nullif(v_guard->>'scope_client_id','') is null
        or incident.client_id=(v_guard->>'scope_client_id')::uuid)
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
    if v_bpay_producer is not null then
      -- Record the complete exact original FOR UPDATE omission cursor set
      -- before changing OPEN. Never rediscover that predicate after mutation.
      v_bpay_omitted_ids:=pg_catalog.array_append(v_bpay_omitted_ids,v_incident.id);
      if pg_catalog.cardinality(v_bpay_omitted_ids)>1000 then
        raise exception using errcode='22023',message='BPAY_NEXT_QUERY_SYNC_OMISSION_SET_TOO_LARGE';end if;
    end if;
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
    if v_bpay_producer is not null then
      v_bpay_omission:=null;v_bpay_omission.completion_id:=v_bpay_producer;v_bpay_omission.incident_id:=v_incident.id;
      v_bpay_omission.work_event_id:=v_incident.work_event_id;v_bpay_omission.issue_episode:=v_incident.episode_number;
      v_bpay_omission.comparison_revision_id:=v_incident.current_comparison_revision_id;
      select e.id into strict v_bpay_omission.resolved_event_id from public.weekly_discrepancy_events e
      where e.event_kind='RESOLVED' and e.idempotency_key='SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication_id::text;
      v_bpay_omission.evidence_sha256:=private.bpay_next_query_sync_omission_completion_digest_v1(v_bpay_omission);
      v_bpay_omissions:=pg_catalog.array_append(v_bpay_omissions,v_bpay_omission);
    end if;
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

  perform private.weekly_source_import_outreach_v1(v_publication_id);
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
  if v_bpay_producer is not null then
    -- Whole original cohort/outreach/audit tail has actually completed.
    -- Zero-stage unchanged completion retains typed proof and takes no clock.
    select coalesce(array_agg(id order by id),'{}'::uuid[]) into v_bpay_omitted_ids from pg_catalog.unnest(v_bpay_omitted_ids) id;
    v_bpay_completion_sha:=private.bpay_next_query_sync_complete_v1(v_bpay_producer,v_actor,v_cycle_id,v_publication_id,
      v_bpay_input_ids,v_bpay_issue_hashes,v_seen_work_events,v_bpay_omitted_ids,
      v_new_count,v_changed_count,v_unchanged_count,v_resolved_count,v_bpay_inputs,v_bpay_omissions);
    perform private.bpay_next_query_producer_finish_standalone_v1(v_bpay_producer,'QUERY_SYNC',v_bpay_producer,v_bpay_completion_sha);
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'source_cycle_id',v_cycle_id,'projection_publication_id',v_publication_id,
    'new_incidents',v_new_count,'changed_comparisons',v_changed_count,
    'unchanged_incidents',v_unchanged_count,'resolved_incidents',v_resolved_count
  );
end;
$function$;

alter function public.weekly_source_query_sync_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_query_sync_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_query_sync_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
