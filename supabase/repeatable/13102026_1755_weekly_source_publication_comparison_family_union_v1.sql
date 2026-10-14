-- Complete current Report/Protection publication and comparison owner union.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_publication_signed_family_fence_v1(
  p_publication_id uuid,p_before jsonb default null
) returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype; v_cycle public.weekly_source_cycles%rowtype;
  v_roots uuid[]; v_lock jsonb; v_proof jsonb;
begin
  select * into strict v_publication from public.weekly_source_projection_publications where id=p_publication_id;
  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  if v_upload.source_cycle_id is distinct from v_cycle.id then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000'; end if;
  v_roots:=private.weekly_source_publication_signed_roots_v1(v_publication.id);
  v_proof:=jsonb_build_object('publication_id',v_publication.id,'source_cycle_id',v_cycle.id,
    'source_group_id',v_cycle.source_group_id,'upload_id',v_upload.id,'report_scope_id',v_publication.report_scope_id,
    'authority_scope_kind',v_publication.authority_scope_kind,'authority_scope_version',v_publication.authority_scope_version,
    'projection_generation',v_publication.projection_generation,'correction_session_id',v_publication.correction_session_id,
    'source_format_profile_id',v_upload.source_format_profile_id,'coverage_start',v_upload.confirmed_coverage_start_local_date,
    'coverage_end',v_upload.confirmed_coverage_end_local_date,
    'client_id',private.weekly_source_checking_client_scope_v1(v_publication.id),'signed_root_ids',to_jsonb(v_roots),
    'publication_before',to_jsonb(v_publication),'upload_before',to_jsonb(v_upload),'cycle_before',to_jsonb(v_cycle),
    'group_before',(select to_jsonb(g) from public.weekly_source_groups g where g.id=v_cycle.source_group_id));
  if p_before is not null then
    if p_before is distinct from v_proof then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED' using errcode='40001'; end if;
    return v_proof;
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  if cardinality(v_roots)>0 then
    v_lock:=private.weekly_source_lock_family_rows_v1(v_roots,null);
    if v_lock->>'ok' is distinct from 'true' then
      raise exception '%',coalesce(v_lock->>'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE') using errcode='55000'; end if;
  end if;
  return v_proof;
end;
$function$;

create or replace function private.weekly_source_candidate_submission_compare_sync_v1(
  p_candidate_generation_id uuid,
  p_projection_publication_id uuid,
  p_membership_id uuid,
  p_timesheet_id uuid,
  p_timesheet_hash bytea,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile_code text;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_membership public.weekly_timesheet_submission_request_memberships%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate_row jsonb;
  v_source_json jsonb;
  v_source record;
  v_candidate_start timestamp without time zone;
  v_candidate_end timestamp without time zone;
  v_candidate_break integer;
  v_candidate_shift_hash bytea;
  v_durable_identity_hash bytea;
  v_prior_work_event_id uuid;
  v_work_event public.weekly_work_events%rowtype;
  v_issue jsonb;
  v_issues jsonb:='[]'::jsonb;
  v_used_source_rows uuid[]:='{}'::uuid[];
  v_seen_work_events uuid[]:='{}'::uuid[];
  v_tie_count integer;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_policy jsonb;
  v_episode integer;
  v_revision integer;
  v_fingerprint bytea;
  v_is_new boolean;
  v_issue_family text;
  v_presence text;
  v_new_count integer:=0;
  v_changed_count integer:=0;
  v_unchanged_count integer:=0;
  v_resolved_count integer:=0;
  v_open_count integer:=0;
  v_import_authoritative boolean:=false;
  v_scope_client_id uuid;
  v_source_question_changed boolean:=false;
  v_discovered_cycle_id uuid; v_discovered_group_id uuid; v_discovered_cycle jsonb;
  v_discovered_timesheet public.timesheets%rowtype; v_locked_timesheet public.timesheets%rowtype;
  v_family_lock jsonb; v_publication_i1_context jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_projection_publication_id is null or p_timesheet_id is null
     or p_timesheet_hash is null or pg_catalog.octet_length(p_timesheet_hash)<>32 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Plain scope/physical discovery precedes this owner's first business lock.
  select * into strict v_publication from public.weekly_source_projection_publications
    where id=p_projection_publication_id and state='CURRENT';
  select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  v_discovered_cycle_id:=v_cycle.id; v_discovered_group_id:=v_cycle.source_group_id; v_discovered_cycle:=to_jsonb(v_cycle);
  select * into strict v_discovered_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if nullif(current_setting('cloudtms.weekly_source_publication_i1_context',true),'') is not null then
    v_publication_i1_context:=current_setting('cloudtms.weekly_source_publication_i1_context',true)::jsonb;
    if v_publication_i1_context->>'schema_version' is distinct from 'WEEKLY_SOURCE_PUBLICATION_I1_CONTEXT_V1'
      or v_publication_i1_context->>'publication_id' is distinct from p_projection_publication_id::text
      or jsonb_typeof(v_publication_i1_context->'root_ids') is distinct from 'array'
      or not(v_publication_i1_context->'root_ids' ? p_timesheet_id::text) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED' using errcode='40001'; end if;
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  v_family_lock:=private.weekly_source_lock_family_rows_v1(array[p_timesheet_id],null);
  if v_family_lock->>'ok' is distinct from 'true' then
    raise exception '%',coalesce(v_family_lock->>'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE') using errcode='55000'; end if;
  select * into strict v_locked_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if to_jsonb(v_locked_timesheet) is distinct from to_jsonb(v_discovered_timesheet) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001'; end if;
  perform 1 from public.weekly_source_groups where id=v_discovered_group_id for update;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_discovered_cycle_id for update;
  if to_jsonb(v_cycle) is distinct from v_discovered_cycle or v_cycle.source_group_id is distinct from v_discovered_group_id then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='40001'; end if;
  if p_candidate_generation_id is null then
    -- A newly published checking source must recompare already-signed weekly
    -- evidence even when no submission request was needed or remains active.
    if p_membership_id is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
    end if;
    select * into strict v_publication
    from public.weekly_source_projection_publications
    where id=p_projection_publication_id and state='CURRENT';
    select * into strict v_cycle from public.weekly_source_cycles
    where id=v_publication.source_cycle_id for update;
    select * into strict v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current
      and revoked_at is null and archived_at_utc is null
      and sheet_scope='WEEKLY' and line_type='HOURS'
      and authorised_at_server is null
      and r2_nurse_key is not null and img_sha256_nurse is not null
    for update;
    select * into strict v_contract from public.contracts
    where id=v_timesheet.contract_id;
    v_generation.candidate_id:=v_contract.candidate_id;
    v_membership.contract_id:=v_contract.id;
    v_membership.client_id:=v_contract.client_id;
    v_membership.week_ending:=v_timesheet.week_ending_date;
  else
    if p_membership_id is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
    end if;
    select * into strict v_generation
    from public.weekly_candidate_outreach_generations
    where id=p_candidate_generation_id and request_kind='SUBMIT_TIMESHEET'
      and state='ACTIVE'
    for update;
    if v_generation.source_cycle_id is distinct from v_discovered_cycle_id then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='40001'; end if;
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_generation.source_cycle_id
    for update;
  end if;
  perform private.weekly_source_query_current_publication_v1(
    v_cycle.id,p_projection_publication_id
  );
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=p_projection_publication_id
    and source_cycle_id=v_cycle.id and state='CURRENT';
  select * into strict v_upload from public.weekly_source_uploads
  where id=v_publication.upload_id and source_cycle_id=v_cycle.id;
  select profile.profile_code into strict v_profile_code
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id;
  if v_profile_code='NHSP_PREFINAL_RELEASED_V1'
     and (v_upload.confirmed_coverage_start_local_date is null
       or v_upload.confirmed_coverage_end_local_date is null) then
    raise exception 'WEEKLY_SOURCE_PREFINAL_COVERAGE_INVALID' using errcode='55000';
  end if;
  if p_candidate_generation_id is not null then
    select * into strict v_submission
    from public.weekly_timesheet_submission_requests
    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=v_generation.candidate_id
      and source_cycle_id=v_cycle.id
      and current_projection_publication_id=v_publication.id
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
    for update;
    select * into strict v_membership
    from public.weekly_timesheet_submission_request_memberships
    where id=p_membership_id and submission_request_id=v_submission.id
      and state='WAITING'
    for update;
    select * into strict v_contract from public.contracts
    where id=v_membership.contract_id
      and candidate_id=v_generation.candidate_id
      and client_id=v_membership.client_id;
    select * into strict v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and contract_id=v_membership.contract_id
      and week_ending_date=v_membership.week_ending
      and is_current and revoked_at is null and archived_at_utc is null
      and sheet_scope='WEEKLY' and line_type='HOURS'
      and authorised_at_server is null
      and r2_nurse_key is not null and img_sha256_nurse is not null
    for update;
  end if;
  v_scope_client_id:=private.weekly_source_checking_client_scope_v1(v_publication.id);
  if v_scope_client_id is not null and v_scope_client_id is distinct from v_contract.client_id then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_timesheet.version<1
     or private.weekly_source_query_candidate_timesheet_hash_v1(v_timesheet.timesheet_id)
          is distinct from p_timesheet_hash then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
  end if;
  v_import_authoritative:=private.weekly_source_is_import_authoritative_v1(
    v_contract.client_id,v_contract.id,v_membership.week_ending);
  if v_import_authoritative and (v_upload.confirmed_coverage_start_local_date is null
    or v_upload.confirmed_coverage_end_local_date is null) then
    raise exception 'WEEKLY_SOURCE_PREFINAL_COVERAGE_INVALID' using errcode='55000'; end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_membership.week_ending
  );
  if v_policy->>'source_group_id' is distinct from v_group.id::text
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_policy->>'document_mode'<>'CHECK_ONLY' then
    raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
  end if;
  if exists(
    select 1
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where source_row.row_finalisation_state in (
      'BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'
    )
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SOURCE_SCOPE_BLOCKED' using errcode='55000';
  end if;

  for v_candidate_row in
    select value
    from pg_catalog.jsonb_array_elements(
      private.weekly_source_candidate_app_schedule_v1(
        v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day
      )
    ) candidate_row
    where (v_profile_code<>'NHSP_PREFINAL_RELEASED_V1' and not v_import_authoritative)
      or (candidate_row.value->>'date')::date between
        v_upload.confirmed_coverage_start_local_date
        and v_upload.confirmed_coverage_end_local_date
    order by value->>'date',value->>'start',value->>'end',
      coalesce(value->>'row_key','') collate "C"
  loop
    v_candidate_start:=(v_candidate_row->>'date')::date+
      (v_candidate_row->>'start')::time;
    v_candidate_end:=(v_candidate_row->>'date')::date+
      (v_candidate_row->>'end')::time;
    if v_candidate_end<=v_candidate_start then
      v_candidate_end:=v_candidate_end+interval '1 day';
    end if;
    v_candidate_break:=coalesce(
      nullif(v_candidate_row#>>'{break_entry,break_minutes}','')::integer,
      nullif(v_candidate_row#>>'{break_entry,calculated_break_minutes}','')::integer,0
    );
    v_candidate_shift_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CANDIDATE_SHIFT_V1',
      pg_catalog.jsonb_build_object(
        'timesheet_id',v_timesheet.timesheet_id,'timesheet_revision',v_timesheet.version,
        'row_key',v_candidate_row->>'row_key','work_date',v_candidate_row->>'date',
        'start_at_local',v_candidate_start,'end_at_local',v_candidate_end,
        'break_minutes',v_candidate_break,
        'additional_units',coalesce(v_candidate_row->'additional_units','[]'::jsonb)
      )
    );

    select pg_catalog.jsonb_build_object(
      'source_row_id',source_row.source_row_id,'source_link_id',source_row.source_link_id,
      'source_link_kind',(select link.link_kind
        from public.weekly_work_event_source_links link
        where link.id=source_row.source_link_id),
      'work_event_id',source_row.work_event_id,'work_date',source_row.work_date,
      'start_at_local',source_row.start_at_local,'end_at_local',source_row.end_at_local,
      'break_minutes',source_row.break_minutes,
      'row_finalisation_state',source_row.row_finalisation_state
    ) into v_source_json
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where not (source_row.source_row_id=any(v_used_source_rows))
      and source_row.row_finalisation_state in (
        'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_UNFINALISED'
      )
      and (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id)
            not in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
      and source_row.work_date=(v_candidate_row->>'date')::date
      and source_row.start_at_local=v_candidate_start
      and source_row.end_at_local=v_candidate_end
      and coalesce(source_row.break_minutes,0)=v_candidate_break
    order by source_row.source_row_id limit 1;

    if v_source_json is null then
      with possible as (
        select source_row.*,
          (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id) as source_link_kind,
          case
            when source_row.start_at_local is null or source_row.end_at_local is null
              then 1000000000::numeric
            else
              pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.start_at_local-v_candidate_start)/60)
              +pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.end_at_local-v_candidate_end)/60)
              +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
          end as distance
        from private.weekly_source_candidate_current_rows_v1(
          v_publication.id,v_generation.candidate_id,v_membership.client_id,
          v_membership.contract_id,v_membership.week_ending
        ) source_row
        where not (source_row.source_row_id=any(v_used_source_rows))
          and source_row.work_date=(v_candidate_row->>'date')::date
          and source_row.row_finalisation_state in (
            'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED'
          )
      ),minimum as (select pg_catalog.min(distance) distance from possible)
      select pg_catalog.count(*)::integer into v_tie_count
      from possible,minimum where possible.distance=minimum.distance;
      if v_tie_count>1 then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_AMBIGUOUS' using errcode='55000';
      elsif v_tie_count=1 then
        with possible as (
          select source_row.*,
            (select link.link_kind from public.weekly_work_event_source_links link
             where link.id=source_row.source_link_id) as source_link_kind,
            case
              when source_row.start_at_local is null or source_row.end_at_local is null
                then 1000000000::numeric
              else
                pg_catalog.abs(pg_catalog.date_part('epoch',
                  source_row.start_at_local-v_candidate_start)/60)
                +pg_catalog.abs(pg_catalog.date_part('epoch',
                  source_row.end_at_local-v_candidate_end)/60)
                +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
            end as distance
          from private.weekly_source_candidate_current_rows_v1(
            v_publication.id,v_generation.candidate_id,v_membership.client_id,
            v_membership.contract_id,v_membership.week_ending
          ) source_row
          where not (source_row.source_row_id=any(v_used_source_rows))
            and source_row.work_date=(v_candidate_row->>'date')::date
            and source_row.row_finalisation_state in (
              'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED'
            )
        )
        select pg_catalog.jsonb_build_object(
          'source_row_id',source_row_id,'source_link_id',source_link_id,
          'source_link_kind',source_link_kind,
          'work_event_id',work_event_id,'work_date',work_date,
          'start_at_local',start_at_local,'end_at_local',end_at_local,
          'break_minutes',break_minutes,'row_finalisation_state',row_finalisation_state
        ) into v_source_json
        from possible order by distance,source_row_id limit 1;
      end if;
    end if;

    if v_source_json is null then
      -- A replacement NHSP file may omit a previously linked shift. Reuse
      -- that durable event instead of inventing a new issue/notification clock.
      v_prior_work_event_id:=null;
      if p_candidate_generation_id is null then
        with prior_events as (
          select link.work_event_id,
            pg_catalog.min(
              pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.start_at_local-v_candidate_start)/60)
              +pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.end_at_local-v_candidate_end)/60)
              +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
            ) as distance
          from public.weekly_source_uploads historical_upload
          join public.weekly_source_upload_rows source_row
            on source_row.upload_id=historical_upload.id
          join public.weekly_source_row_resolutions resolution
            on resolution.upload_row_id=source_row.id
           and resolution.mapping_state='RESOLVED'
          join public.weekly_work_event_source_links link
            on link.upload_row_id=source_row.id
           and link.row_resolution_id=resolution.id
           and link.work_event_id=resolution.work_event_id
          join public.weekly_work_events work_event
            on work_event.id=link.work_event_id
          where historical_upload.source_cycle_id=v_cycle.id
            and historical_upload.id<>v_upload.id
            -- A provisional check must not choose a final backing-report
            -- event as the identity of a missing provisional shift.
            and historical_upload.source_format_profile_id=v_upload.source_format_profile_id
            and source_row.work_date=(v_candidate_row->>'date')::date
            and source_row.start_at_local is not null
            and source_row.end_at_local is not null
            and resolution.candidate_id=v_generation.candidate_id
            and resolution.client_id=v_membership.client_id
            and resolution.contract_id=v_membership.contract_id
            and work_event.first_source_group_id=v_group.id
          group by link.work_event_id
        ), nearest as (
          select work_event_id,distance,
            pg_catalog.count(*) over (partition by distance) as tie_count
          from prior_events
        )
        select work_event_id,tie_count
        into v_prior_work_event_id,v_tie_count
        from nearest order by distance,work_event_id limit 1;
        if v_tie_count>1 then
          raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_AMBIGUOUS' using errcode='55000';
        end if;
      end if;
      if v_prior_work_event_id is null then
        v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_WORK_EVENT_SCHEDULE_TUPLE_V1',
        pg_catalog.jsonb_build_object(
          'source_group_id',v_group.id,
          'source_format_profile_id',v_upload.source_format_profile_id,
          'candidate_id',v_generation.candidate_id,
          'client_id',v_membership.client_id,
          'work_date',(v_candidate_row->>'date')::date,
          'start_at_local',v_candidate_start,'end_at_local',v_candidate_end
        )
      );
        insert into public.weekly_work_events(
        candidate_id,client_id,work_date,identity_kind,profile_external_key,
        durable_identity_hash,first_source_group_id,source_format_profile_id
      ) values (
        v_generation.candidate_id,v_membership.client_id,
        (v_candidate_row->>'date')::date,'SCHEDULE_TUPLE',null,
        v_durable_identity_hash,v_group.id,v_upload.source_format_profile_id
      ) on conflict (durable_identity_hash) do nothing;
        select * into strict v_work_event from public.weekly_work_events
        where durable_identity_hash=v_durable_identity_hash;
        if v_work_event.candidate_id is distinct from v_generation.candidate_id
           or v_work_event.client_id is distinct from v_membership.client_id
           or v_work_event.work_date is distinct from (v_candidate_row->>'date')::date
           or v_work_event.first_source_group_id is distinct from v_group.id then
          raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_COLLISION' using errcode='55000';
        end if;
      else
        select * into strict v_work_event from public.weekly_work_events
        where id=v_prior_work_event_id;
      end if;
      v_issue:=pg_catalog.jsonb_build_object(
        'work_event_id',v_work_event.id,
        'candidate_timesheet_id',v_timesheet.timesheet_id,
        'candidate_timesheet_revision',v_timesheet.version,
        'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
        'source_row_id',null,'source_work_event_link_id',null,
        'contract_id',v_membership.contract_id,
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local',v_candidate_start,
        'candidate_end_at_local',v_candidate_end,
        'candidate_break_minutes',v_candidate_break,
        'system_start_at_local',null,'system_end_at_local',null,
        'system_break_minutes',null
      );
      v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
    else
      v_used_source_rows:=pg_catalog.array_append(
        v_used_source_rows,(v_source_json->>'source_row_id')::uuid
      );
      if v_source_json->>'source_link_kind' in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
         or v_source_json->>'row_finalisation_state'='SOURCE_ABSENT_ZERO' then
        v_issue_family:='SOURCE_MISSING_OR_NOT_AUTHORISED';
        v_presence:='ABSENT';
      elsif v_source_json->>'row_finalisation_state'='SOURCE_UNFINALISED' then
        v_issue_family:='HEALTHROSTER_NOT_FINALISED';
        v_presence:='UNFINALISED';
      elsif nullif(v_source_json->>'start_at_local','')::timestamp=v_candidate_start
         and nullif(v_source_json->>'end_at_local','')::timestamp=v_candidate_end
         and coalesce(nullif(v_source_json->>'break_minutes','')::integer,0)=v_candidate_break then
        v_issue_family:=null;
        v_presence:='PRESENT';
      else
        v_issue_family:='SOURCE_HOURS_DIFFER';
        v_presence:='PRESENT';
      end if;
      if v_issue_family is not null then
        v_issue:=pg_catalog.jsonb_build_object(
          'work_event_id',(v_source_json->>'work_event_id')::uuid,
          'candidate_timesheet_id',v_timesheet.timesheet_id,
          'candidate_timesheet_revision',v_timesheet.version,
          'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
          'source_row_id',(v_source_json->>'source_row_id')::uuid,
          'source_work_event_link_id',(v_source_json->>'source_link_id')::uuid,
          'contract_id',v_membership.contract_id,
          'issue_family',v_issue_family,'source_presence',v_presence,
          'candidate_start_at_local',v_candidate_start,
          'candidate_end_at_local',v_candidate_end,
          'candidate_break_minutes',v_candidate_break,
          'system_start_at_local',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
            then nullif(v_source_json->>'start_at_local','')::timestamp end,
          'system_end_at_local',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
            then nullif(v_source_json->>'end_at_local','')::timestamp end,
          'system_break_minutes',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
            then nullif(v_source_json->>'break_minutes','')::integer end
        );
        v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
      end if;
    end if;
  end loop;

  for v_source in
    select *
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where not (source_row.source_row_id=any(v_used_source_rows))
      and source_row.row_finalisation_state<>'SOURCE_ABSENT_ZERO'
      and (not v_import_authoritative or source_row.work_date between
        v_upload.confirmed_coverage_start_local_date and v_upload.confirmed_coverage_end_local_date)
      and (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id)
            not in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
    order by source_row.work_date,source_row.start_at_local,
      source_row.end_at_local,source_row.source_row_id
  loop
    v_candidate_shift_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CANDIDATE_SHIFT_V1',
      pg_catalog.jsonb_build_object(
        'timesheet_id',v_timesheet.timesheet_id,
        'timesheet_revision',v_timesheet.version,
        'work_event_id',v_source.work_event_id,'worked',false
      )
    );
    if v_source.row_finalisation_state='SOURCE_UNFINALISED' then
      if v_import_authoritative and not exists(select 1 from public.contract_weeks expected
        where expected.contract_id=v_contract.id and expected.additional_seq=0
          and not coalesce(expected.is_adjustment,false) and expected.status<>'CANCELLED'
          and v_source.work_date between expected.week_ending_date-6 and expected.week_ending_date) then continue; end if;
      v_issue_family:='HEALTHROSTER_NOT_FINALISED';
      v_presence:='UNFINALISED';
    elsif v_source.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED') then
      v_issue_family:='SOURCE_HOURS_DIFFER';
      v_presence:='PRESENT';
    else
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SOURCE_SCOPE_BLOCKED' using errcode='55000';
    end if;
    v_issue:=pg_catalog.jsonb_build_object(
      'work_event_id',v_source.work_event_id,
      'candidate_timesheet_id',v_timesheet.timesheet_id,
      'candidate_timesheet_revision',v_timesheet.version,
      'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
      'source_row_id',v_source.source_row_id,
      'source_work_event_link_id',v_source.source_link_id,
      'contract_id',v_membership.contract_id,
      'issue_family',v_issue_family,'source_presence',v_presence,
      'candidate_start_at_local',null,'candidate_end_at_local',null,
      'candidate_break_minutes',null,
      'system_start_at_local',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
        then v_source.start_at_local end,
      'system_end_at_local',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
        then v_source.end_at_local end,
      'system_break_minutes',case when v_presence='PRESENT' or (v_import_authoritative and v_presence='UNFINALISED')
        then v_source.break_minutes end
    );
    v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
  end loop;

  for v_issue in
    select value from pg_catalog.jsonb_array_elements(v_issues)
    order by value->>'work_event_id'
  loop
    select * into strict v_work_event from public.weekly_work_events
    where id=(v_issue->>'work_event_id')::uuid
      and candidate_id=v_generation.candidate_id
      and client_id=v_membership.client_id
      and first_source_group_id=v_group.id;
    if v_work_event.id=any(v_seen_work_events) then
      raise exception 'WEEKLY_SOURCE_QUERY_DUPLICATE_WORK_EVENT' using errcode='22023';
    end if;
    v_seen_work_events:=pg_catalog.array_append(v_seen_work_events,v_work_event.id);
    v_issue_family:=v_issue->>'issue_family';
    v_presence:=v_issue->>'source_presence';
    v_policy:=private._weekly_source_effective_policy_v1(
      v_membership.client_id,v_membership.contract_id,v_work_event.work_date
    );
    if v_policy->>'source_group_id' is distinct from v_group.id::text
       or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
       or v_policy->>'document_mode'<>'CHECK_ONLY' then
      raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
    end if;

    perform 1 from public.weekly_work_events where id=v_work_event.id for update;
    if private.weekly_source_query_resolved_decision_matches_v1(v_group.id,v_work_event.id,v_issue) then
      v_unchanged_count:=v_unchanged_count+1;
      continue;
    end if;
    select * into v_incident
    from public.weekly_discrepancy_incidents
    where source_group_id=v_group.id and work_event_id=v_work_event.id
      and state='OPEN'
    for update;
    v_is_new:=not found;
    if v_is_new then
      select coalesce(pg_catalog.max(episode_number),0)+1 into v_episode
      from public.weekly_discrepancy_incidents
      where source_group_id=v_group.id and work_event_id=v_work_event.id;
      insert into public.weekly_discrepancy_incidents(
        source_group_id,work_event_id,episode_number,candidate_id,client_id,
        source_cycle_id,state,reconciliation_state,candidate_action_state,
        manager_potential_state,manager_action_state,waiting_source_state
      ) values (
        v_group.id,v_work_event.id,v_episode,v_generation.candidate_id,
        v_membership.client_id,v_cycle.id,'OPEN','UNRESOLVED',
        case when v_issue_family='HEALTHROSTER_NOT_FINALISED' and v_import_authoritative then 'NOT_REQUIRED'
          when p_candidate_generation_id is null
          and coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
          then 'NOT_ASKED' else 'NOT_REQUIRED' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'AVAILABLE' else 'NOT_AVAILABLE' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'NOT_SENT' else 'NOT_REQUIRED' end,
        'NOT_WAITING'
      ) returning * into v_incident;
      v_comparison.id:=null;
      v_new_count:=v_new_count+1;
    else
      if v_incident.candidate_id is distinct from v_generation.candidate_id
         or v_incident.client_id is distinct from v_membership.client_id
         or v_incident.source_cycle_id is distinct from v_cycle.id
         or v_incident.current_comparison_revision_id is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_INCIDENT_SCOPE_CONFLICT' using errcode='55000';
      end if;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      if v_comparison.contract_id is distinct from v_membership.contract_id then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_INCIDENT_SCOPE_CONFLICT' using errcode='55000';
      end if;
      v_episode:=v_incident.episode_number;
    end if;

    v_source_question_changed:=v_import_authoritative and v_comparison.id is not null
      and (v_comparison.issue_family,v_comparison.source_presence,v_comparison.system_start_at_local,
        v_comparison.system_end_at_local,v_comparison.system_break_minutes)
      is distinct from (v_issue_family,v_presence,nullif(v_issue->>'system_start_at_local','')::timestamp,
        nullif(v_issue->>'system_end_at_local','')::timestamp,nullif(v_issue->>'system_break_minutes','')::integer);
    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_work_event.id,v_issue
    );
    if not v_is_new and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      -- A re-signed candidate week changes the current comparison but not the
      -- identity or outreach clock of an already-open work-event incident.
      v_changed_count:=v_changed_count+1;
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
        v_incident.id,v_revision,v_publication.id,v_upload.id,null,
        v_timesheet.timesheet_id,v_timesheet.version,
        private.weekly_source_query_hex32_v1(
          v_issue->>'candidate_shift_fingerprint',
          'WEEKLY_SOURCE_CANDIDATE_SHIFT_HASH_INVALID'
        ),nullif(v_issue->>'source_row_id','')::uuid,
        nullif(v_issue->>'source_work_event_link_id','')::uuid,
        v_membership.contract_id,v_issue_family,v_presence,
        nullif(v_issue->>'candidate_start_at_local','')::timestamp,
        nullif(v_issue->>'candidate_end_at_local','')::timestamp,
        nullif(v_issue->>'candidate_break_minutes','')::integer,
        nullif(v_issue->>'system_start_at_local','')::timestamp,
        nullif(v_issue->>'system_end_at_local','')::timestamp,
        nullif(v_issue->>'system_break_minutes','')::integer,v_fingerprint
      ) returning * into v_comparison;
      update public.weekly_discrepancy_incidents
      set current_comparison_revision_id=v_comparison.id,
        candidate_action_state=case when v_issue_family='HEALTHROSTER_NOT_FINALISED' and v_import_authoritative
          then 'NOT_REQUIRED' else candidate_action_state end,
        manager_action_state=case when v_source_question_changed and manager_potential_state='AVAILABLE'
          then 'NOT_SENT' else manager_action_state end,
        waiting_source_state=case when v_source_question_changed then 'NOT_WAITING' else waiting_source_state end,
        reconciliation_state=case when v_source_question_changed then 'UNRESOLVED' else reconciliation_state end
      where id=v_incident.id;
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication.id,v_fingerprint,
        'SOURCE_RECHECKED','SYSTEM',
        pg_catalog.jsonb_build_object(
          'issue_family',v_issue_family,'source_presence',v_presence,
          'comparison_revision_id',v_comparison.id,
          'origin','CANDIDATE_TIMESHEET_SUBMISSION'
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
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.source_group_id=v_group.id
      and incident.source_cycle_id=v_cycle.id
      and incident.candidate_id=v_generation.candidate_id
      and incident.client_id=v_membership.client_id
      and comparison.contract_id=v_membership.contract_id
      and work_event.work_date between v_membership.week_ending-6 and v_membership.week_ending
      and ((v_profile_code<>'NHSP_PREFINAL_RELEASED_V1' and not v_import_authoritative)
        or work_event.work_date between v_upload.confirmed_coverage_start_local_date
          and v_upload.confirmed_coverage_end_local_date)
      and incident.state='OPEN'
      and not (incident.work_event_id=any(v_seen_work_events))
    order by incident.id for update of incident
  loop
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
      candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
      manager_action_state='NOT_REQUIRED',waiting_source_state='SOURCE_MATCHED',
      resolved_at_utc=p_now_utc,resolution_kind='SOURCE_MATCHED'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications
    set operational_state='RESOLVED',resolved_at_utc=p_now_utc
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication.id,null,
      'RESOLVED','SYSTEM',
      pg_catalog.jsonb_build_object(
        'resolution','SOURCE_MATCHED','origin','CANDIDATE_TIMESHEET_SUBMISSION'
      ),
      'SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication.id::text
    ) on conflict (event_kind,idempotency_key) do nothing;
    v_resolved_count:=v_resolved_count+1;
  end loop;

  select pg_catalog.count(*)::integer into v_open_count
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  where incident.source_cycle_id=v_cycle.id
    and incident.candidate_id=v_generation.candidate_id
    and incident.client_id=v_membership.client_id
    and comparison.contract_id=v_membership.contract_id
    and work_event.work_date between v_membership.week_ending-6 and v_membership.week_ending
    and incident.state='OPEN';

  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome',case when v_open_count=0 then 'MATCHED' else 'ISSUES' end,
    'open_issue_count',v_open_count,'new_incidents',v_new_count,
    'changed_comparisons',v_changed_count,'unchanged_incidents',v_unchanged_count,
    'resolved_incidents',v_resolved_count
  );
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_STALE' using errcode='40001';
end;
$function$;

create or replace function private.weekly_source_candidate_prefinal_publish_recheck_v1(
  p_actor_user_id uuid,
  p_projection_publication_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_publication public.weekly_source_projection_publications%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_profile_code text;
  v_profile_id uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_signed record;
  v_result jsonb;
  v_checked integer:=0;
  v_new integer:=0;
  v_changed integer:=0;
  v_resolved integer:=0;
  v_restart_keys text[]:='{}'::text[];
  v_restart_key text;
  v_not_compared integer:=0; v_refusal text; v_unsigned jsonb;
  v_publication_i1_context jsonb; v_locked_root_ids uuid[]; v_observed_root_ids uuid[];
begin
  perform private.weekly_source_query_require_service_v1();
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=p_projection_publication_id and state='CURRENT';
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_publication.source_cycle_id;
  select profile.profile_code,profile.id into strict v_profile_code,v_profile_id
  from public.weekly_source_uploads upload
  join public.weekly_source_format_profiles profile
    on profile.id=upload.source_format_profile_id
  where upload.id=v_publication.upload_id;
  if (v_profile_code<>'NHSP_PREFINAL_RELEASED_V1' and not exists(select 1 from public.weekly_source_groups g
       where g.id=v_cycle.source_group_id and g.source_family='ROSTER'))
     or v_publication.authority_scope_kind<>'CYCLE'
     or v_cycle.current_projection_publication_id is distinct from v_publication.id then
    raise exception 'WEEKLY_SOURCE_PREFINAL_RECHECK_SCOPE_INVALID' using errcode='55000';
  end if;
  select * into strict v_upload from public.weekly_source_uploads
  where id=v_publication.upload_id and source_cycle_id=v_cycle.id;
  if v_upload.confirmed_coverage_start_local_date is null
     or v_upload.confirmed_coverage_end_local_date is null then
    raise exception 'WEEKLY_SOURCE_PREFINAL_COVERAGE_INVALID' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'RECHECK_SOURCE',v_cycle.source_group_id,null,
    v_cycle.finalisation_week_ending
  );

  if nullif(current_setting('cloudtms.weekly_source_publication_i1_context',true),'') is not null then
    v_publication_i1_context:=current_setting('cloudtms.weekly_source_publication_i1_context',true)::jsonb;
    if v_publication_i1_context->>'schema_version' is distinct from 'WEEKLY_SOURCE_PUBLICATION_I1_CONTEXT_V1'
      or v_publication_i1_context->>'publication_id' is distinct from v_publication.id::text
      or jsonb_typeof(v_publication_i1_context->'root_ids') is distinct from 'array' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED' using errcode='40001'; end if;
    select coalesce(array_agg(value::uuid order by value::uuid),'{}'::uuid[]) into v_locked_root_ids
      from jsonb_array_elements_text(v_publication_i1_context->'root_ids') value;
    v_observed_root_ids:=private.weekly_source_publication_signed_roots_v1(v_publication.id);
    if v_observed_root_ids is distinct from v_locked_root_ids then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED' using errcode='40001'; end if;
  end if;
  -- Recheck signed weeks only for days in this upload's confirmed export
  -- period. A week may straddle that period; compare_sync applies the same
  -- day boundary to candidate rows and incident resolution.
  for v_signed in
    select sheet.timesheet_id,sheet.week_ending_date,sheet.actual_schedule_json,sheet.additional_units_per_day,contract.candidate_id,contract.client_id,contract.id as contract_id
    from public.timesheets sheet join public.contracts contract on contract.id=sheet.contract_id
    where sheet.timesheet_id=any(private.weekly_source_publication_signed_roots_v1(v_publication.id))
    order by sheet.timesheet_id
  loop
    -- Growth refusal is OUTSIDE per-sheet isolation: never log it as a harmless
    -- NOT_COMPARED and then acquire a second physical family out of order.
    if v_locked_root_ids is not null and not(v_signed.timesheet_id=any(v_locked_root_ids)) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED' using errcode='40001'; end if;
    -- Each sheet is one native subtransaction. A refusal rolls back only its
    -- comparison effects; the native append-only audit records NOT_COMPARED.
    begin
      if not (exists (
        select 1
        from public.weekly_source_uploads historical_upload
        join public.weekly_source_upload_rows source_row
          on source_row.upload_id=historical_upload.id
        join public.weekly_source_row_resolutions resolution
          on resolution.upload_row_id=source_row.id
         and resolution.mapping_state='RESOLVED'
        where historical_upload.source_cycle_id=v_cycle.id
          and historical_upload.source_format_profile_id=v_profile_id
          and source_row.work_date between v_signed.week_ending_date-6
            and v_signed.week_ending_date
          and source_row.work_date between
            v_upload.confirmed_coverage_start_local_date
            and v_upload.confirmed_coverage_end_local_date
          and resolution.candidate_id=v_signed.candidate_id
          and resolution.client_id=v_signed.client_id
          and resolution.contract_id=v_signed.contract_id
      ) or exists (
        select 1
        from pg_catalog.jsonb_array_elements(
          private.weekly_source_candidate_app_schedule_v1(
            v_signed.actual_schedule_json,v_signed.additional_units_per_day
          )
        ) candidate_shift
        where (candidate_shift.value->>'date')::date between
          v_upload.confirmed_coverage_start_local_date
          and v_upload.confirmed_coverage_end_local_date
          and private._weekly_source_effective_policy_v1(
            v_signed.client_id,v_signed.contract_id,(candidate_shift.value->>'date')::date
          )->>'source_group_id'=v_cycle.source_group_id::text
      )) then continue; end if;
      v_result:=private.weekly_source_candidate_submission_compare_sync_v1(
        null,v_publication.id,null,v_signed.timesheet_id,
        private.weekly_source_query_candidate_timesheet_hash_v1(v_signed.timesheet_id));
      perform private.weekly_source_publication_comparison_record_v1(v_publication.id,v_signed.timesheet_id,'COMPARED',null);
    exception when others then
      v_refusal:=coalesce(substring(sqlerrm from '^[A-Z][A-Z0-9_]{0,119}$'),'WEEKLY_SOURCE_COMPARISON_REFUSED');
      perform private.weekly_source_publication_comparison_record_v1(v_publication.id,v_signed.timesheet_id,'NOT_COMPARED',v_refusal);
      v_not_compared:=v_not_compared+1;
      continue;
    end;
    v_checked:=v_checked+1;
    v_new:=v_new+(v_result->>'new_incidents')::integer;
    v_changed:=v_changed+(v_result->>'changed_comparisons')::integer;
    v_resolved:=v_resolved+(v_result->>'resolved_incidents')::integer;
    if (v_result->>'new_incidents')::integer+(v_result->>'changed_comparisons')::integer>0 then
      v_restart_keys:=pg_catalog.array_append(
        v_restart_keys,v_signed.candidate_id::text||':'||v_signed.client_id::text
      );
    end if;
  end loop;
  for v_restart_key in
    select distinct key_value collate "C"
    from pg_catalog.unnest(v_restart_keys) key_value
    order by key_value
  loop
    perform private.weekly_source_query_restart_activated_cohort_v1(
      v_cycle.id,pg_catalog.split_part(v_restart_key,':',1)::uuid,
      pg_catalog.split_part(v_restart_key,':',2)::uuid,v_publication.id
    );
  end loop;
  if v_profile_code<>'NHSP_PREFINAL_RELEASED_V1' then
    v_unsigned:=private.weekly_source_expected_week_unsigned_materialise_v1(v_publication.id);
    v_new:=v_new+(v_unsigned->>'new_incidents')::integer;
    v_changed:=v_changed+(v_unsigned->>'changed_comparisons')::integer;
    v_resolved:=v_resolved+(v_unsigned->>'resolved_incidents')::integer;
    perform private.weekly_source_not_finalised_dispatch_v1(v_publication.id);
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'not_compared',v_not_compared,'signed_weeks_checked',v_checked,'new_incidents',v_new,
    'changed_comparisons',v_changed,'resolved_incidents',v_resolved
  );
end;
$function$;

create or replace function public.weekly_source_projection_publish_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_publication_id uuid;
  v_expected_comparison bytea;
  v_expected_issue bytea;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_scope_version bigint;
  v_scope_upload uuid;
  v_client_id uuid;
  v_comparison_hash bytea;
  v_issue_hash bytea;
  v_fingerprint bytea;
  v_resolution_count integer;
  v_generation integer;
  v_ready_session_version bigint;
  v_signed_fence jsonb; v_prior_publication_i1_context text:=current_setting('cloudtms.weekly_source_publication_i1_context',true);
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','publication_id',
         'expected_comparison_manifest_hash','expected_issue_set_hash')
     ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_publication_id:=(p_request->>'publication_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_publication_id is null then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end if;
  v_expected_comparison:=case
    when nullif(p_request->>'expected_comparison_manifest_hash','') is null then null
    else private.weekly_source_hex32_v1(
      p_request->>'expected_comparison_manifest_hash',
      'WEEKLY_SOURCE_COMPARISON_MANIFEST_HASH_INVALID'
    ) end;
  v_expected_issue:=case
    when nullif(p_request->>'expected_issue_set_hash','') is null then null
    else private.weekly_source_hex32_v1(
      p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_SET_HASH_INVALID'
    ) end;

  select * into v_publication
  from public.weekly_source_projection_publications
  where id=v_publication_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_publication.authority_scope_kind='CYCLE' then
    v_scope_version:=v_cycle.version;
    v_scope_upload:=v_cycle.current_complete_upload_id;
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id;
    v_scope_version:=v_scope.version;
    v_scope_upload:=v_scope.current_complete_upload_id;
    v_client_id:=v_scope.client_id;
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  if v_publication.authority_scope_kind='CYCLE' and v_publication.correction_session_id is null then
    -- Full signed set, including an empty set, captured before any business lock.
    v_signed_fence:=private.weekly_source_publication_signed_family_fence_v1(v_publication.id,null);
    perform set_config('cloudtms.weekly_source_publication_i1_context',jsonb_build_object('schema_version','WEEKLY_SOURCE_PUBLICATION_I1_CONTEXT_V1',
      'publication_id',v_publication.id,'root_ids',v_signed_fence->'signed_root_ids')::text,true);
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837)
  );
  -- Retain the native scope advisory before group/cycle to match upload owners.
  -- Signed physical I-1 was acquired above. Binding's group lock is pre-held.
  select * into strict v_group from public.weekly_source_groups where id=v_group.id for update;
  perform 1 from public.weekly_source_cycles where id=v_cycle.id for update;
  if v_publication.authority_scope_kind='SELF_BILL_REPORT_SCOPE' then
    perform private.weekly_source_self_bill_report_seal_assert_v1(v_upload.id);
  end if;
  if v_publication.authority_scope_kind='CYCLE' then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_publication.source_cycle_id for update;
    v_scope_version:=v_cycle.version;
    v_scope_upload:=v_cycle.current_complete_upload_id;
  else
    -- Each NHSP Trust publishes independently under its report-scope head.
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_publication.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
    end if;
    v_scope_version:=v_scope.version;
    v_scope_upload:=v_scope.current_complete_upload_id;
    v_client_id:=v_scope.client_id;
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=v_publication_id for update;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_publication.upload_id;
  if v_signed_fence is not null then
    perform private.weekly_source_publication_signed_family_fence_v1(v_publication.id,v_signed_fence);
  end if;
  if private.weekly_source_scope_fingerprint_v1(
       v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
       v_publication.report_scope_id,v_client_id
     ) is distinct from v_upload.declared_scope_fingerprint then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if v_publication.correction_session_id is not null then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_publication.correction_session_id for update;
    if not found
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_publication.authority_scope_kind
       or v_correction.report_scope_id is distinct from v_publication.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.actor_user_id is distinct from v_actor
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.state<>'CORRECTION_READY'
       or v_scope_version<>v_publication.authority_scope_version
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
    if v_publication.state='CORRECTION_READY' then
      if v_correction.replacement_projection_publication_id is distinct from v_publication.id
         or (v_publication.rebuild_idempotency_key is not null
             and v_publication.ready_session_version is null) then
        raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
      end if;
      if (v_expected_comparison is not null
            and v_expected_comparison<>v_publication.comparison_manifest_hash)
         or (v_expected_issue is not null
            and v_expected_issue<>v_publication.issue_set_hash) then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH' using errcode='40001';
      end if;
      v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
        pg_catalog.jsonb_build_object(
          'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
          'authority_scope_version',v_publication.authority_scope_version,
          'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
          'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
        )
      );
      perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','CORRECTION_READY','publication_id',v_publication.id,
        'upload_id',v_publication.upload_id,
        'authority_scope_version',v_publication.authority_scope_version,
        'projection_generation',v_publication.projection_generation,
        'ready_session_version',v_publication.ready_session_version,
        'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
        'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),'idempotent',true
      );
    end if;
    if (v_publication.rebuild_idempotency_key is null and (
         v_correction.state<>'READY'
         or v_correction.replacement_projection_publication_id is not null
       ))
       or (v_publication.rebuild_idempotency_key is not null and (
         v_publication.projection_generation is null
         or v_publication.ready_session_version is null
         or v_correction.state<>'REVIEWED'
         or v_correction.version<>v_publication.ready_session_version
         or v_correction.replacement_projection_publication_id is null
         or v_correction.replacement_projection_publication_id=v_publication.id
       )) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
  end if;
  if v_publication.state='CURRENT'
      and v_publication.correction_session_id is null
      and v_scope_upload=v_publication.upload_id
      and v_scope_version=v_publication.authority_scope_version then
    if (v_expected_comparison is not null
          and v_expected_comparison<>v_publication.comparison_manifest_hash)
       or (v_expected_issue is not null
          and v_expected_issue<>v_publication.issue_set_hash) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH' using errcode='40001';
    end if;
    v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
      pg_catalog.jsonb_build_object(
        'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
        'authority_scope_version',v_publication.authority_scope_version,
        'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
      )
    );
    perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_publication.state,'publication_id',v_publication.id,
      'upload_id',v_publication.upload_id,
      'authority_scope_version',v_publication.authority_scope_version,
      'projection_generation',v_publication.projection_generation,
      'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
      'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),'idempotent',true
    );
  end if;
  if v_publication.state not in ('BUILDING','FAILED')
     or v_scope_version<>v_publication.authority_scope_version
     or (v_publication.correction_session_id is null and (
       v_scope_upload is distinct from v_publication.upload_id
       or v_upload.state<>'CURRENT'
     )) then
    if v_publication.state in ('BUILDING','FAILED') then
      update public.weekly_source_projection_publications
      set state='STALE',failure_code='AUTHORITY_SCOPE_CHANGED'
      where id=v_publication.id;
    end if;
    perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','STALE','reason_code','WEEKLY_SOURCE_PUBLICATION_CAS_STALE',
      'publication_id',v_publication.id,'current_upload_id',v_scope_upload,
      'current_authority_scope_version',v_scope_version
    );
  end if;

  if v_publication.authority_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_publication.authority_scope_version::integer
  );
  select pg_catalog.count(*)::integer into v_resolution_count
  from public.weekly_source_row_resolutions resolution
  join public.weekly_source_upload_rows source_row
    on source_row.id=resolution.upload_row_id
  where source_row.upload_id=v_upload.id
    and resolution.generation=v_generation;
  if v_resolution_count<>(select count(*) from public.weekly_source_upload_rows r
       where r.upload_id=v_upload.id and r.row_finalisation_state<>'SOURCE_REPORT_LINE')
     or exists(
       select 1
       from public.weekly_source_upload_rows source_row
       where source_row.upload_id=v_upload.id
         and source_row.row_finalisation_state<>'SOURCE_REPORT_LINE'
         and not exists(
           select 1
            from public.weekly_source_row_resolutions resolution
            where resolution.upload_row_id=source_row.id
              and resolution.generation=v_generation
              and resolution.source_row_fingerprint=source_row.normalised_row_hash
         )
     )
     or (v_publication.projection_generation is null and exists(
        select 1
        from public.weekly_source_row_resolutions resolution
       join public.weekly_source_upload_rows source_row
         on source_row.id=resolution.upload_row_id
        where source_row.upload_id=v_upload.id
          and resolution.generation<>v_generation
     )) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE'
      using errcode='55000';
  end if;

  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    v_publication.id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(v_publication.id);
  if (v_expected_comparison is not null and v_expected_comparison<>v_comparison_hash)
     or (v_expected_issue is not null and v_expected_issue<>v_issue_hash) then
    update public.weekly_source_projection_publications
    set state='FAILED',failure_code='PUBLICATION_HASH_MISMATCH'
    where id=v_publication.id;
    if v_publication.correction_session_id is not null then
      update public.weekly_final_source_correction_sessions
      set updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id;
    elsif v_publication.authority_scope_kind='CYCLE' then
      update public.weekly_source_cycles
      set projection_state='FAILED',current_projection_publication_id=null
      where id=v_cycle.id and current_complete_upload_id=v_publication.upload_id
        and version=v_publication.authority_scope_version;
    else
      update public.weekly_source_report_scopes
      set projection_state='FAILED',current_projection_publication_id=null,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_scope.id and current_complete_upload_id=v_publication.upload_id
        and version=v_publication.authority_scope_version;
    end if;
    perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','FAILED','reason_code','WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH',
      'publication_id',v_publication.id
    );
  end if;

  v_ready_session_version:=case when v_publication.correction_session_id is null then null
    else v_correction.version+1 end;
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state=case when v_publication.correction_session_id is null
        then 'CURRENT' else 'CORRECTION_READY' end,
      ready_session_version=v_ready_session_version,
      failure_code=null,published_at_utc=pg_catalog.transaction_timestamp()
  where id=v_publication.id;
  if v_publication.correction_session_id is not null then
    if v_publication.rebuild_idempotency_key is null then
      update public.weekly_final_source_correction_sessions
      set replacement_projection_publication_id=v_publication.id,
          version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id and state='READY'
        and version=v_correction.version
        and replacement_correction_upload_id=v_publication.upload_id
        and replacement_projection_publication_id is null;
    else
      update public.weekly_final_source_correction_sessions
      set replacement_projection_publication_id=v_publication.id,
          state='READY',review_idempotency_key=null,review_request_hash=null,
          review_result_json=null,review_result_hash=null,
          version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id and state='REVIEWED'
        and version=v_publication.ready_session_version
        and replacement_correction_upload_id=v_publication.upload_id
        and replacement_projection_publication_id is not null
        and replacement_projection_publication_id<>v_publication.id;
    end if;
    if not found then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_CAS_LOST' using errcode='40001';
    end if;
  elsif v_publication.authority_scope_kind='CYCLE' then
    update public.weekly_source_cycles
    set projection_state='CURRENT',current_projection_publication_id=v_publication.id
    where id=v_cycle.id and current_complete_upload_id=v_publication.upload_id
      and version=v_publication.authority_scope_version;
  else
    update public.weekly_source_report_scopes
    set projection_state='CURRENT',current_projection_publication_id=v_publication.id,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_scope.id and current_complete_upload_id=v_publication.upload_id
      and version=v_publication.authority_scope_version;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_CAS_LOST' using errcode='40001';
  end if;
  if v_publication.correction_session_id is null and exists (
    select 1 from public.weekly_source_format_profiles profile
    where profile.id=v_upload.source_format_profile_id
      and (profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
        or (v_group.source_family='ROSTER' and profile.profile_code in (
          'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')))
  ) and v_publication.authority_scope_kind='CYCLE' then
    -- Recheck already-signed candidate evidence in the same publication
    -- transaction. This writes comparison/query evidence only: no source
    -- financial movement or Timesheet hours are created here.
    perform private.weekly_source_candidate_prefinal_publish_recheck_v1(
      v_actor,v_publication.id
    );
  end if;
  if v_publication.correction_session_id is null then
    perform private.weekly_source_self_bill_report_bind_v1(v_publication.id);
  end if;
  if v_publication.correction_session_id is null and v_publication.authority_scope_kind<>'SELF_BILL_REPORT_SCOPE' then
    perform private.weekly_source_import_outreach_v1(v_publication.id);
  end if;
  v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
    pg_catalog.jsonb_build_object(
      'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
      'authority_scope_version',v_publication.authority_scope_version,
      'comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_issue_hash,'hex')
    )
  );
  perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
  return pg_catalog.jsonb_build_object(
    'ok',true,'status',case when v_publication.correction_session_id is null
      then 'CURRENT' else 'CORRECTION_READY' end,
    'publication_id',v_publication.id,
    'upload_id',v_publication.upload_id,
    'authority_scope_version',v_publication.authority_scope_version,
    'projection_generation',v_publication.projection_generation,
    'ready_session_version',v_ready_session_version,
    'comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
    'issue_set_hash',pg_catalog.encode(v_issue_hash,'hex'),
    'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),
    'idempotent',false
  );
exception when others then
  perform set_config('cloudtms.weekly_source_publication_i1_context',coalesce(v_prior_publication_i1_context,''),true);
  raise;
end;
$function$;
alter function private.weekly_source_publication_signed_family_fence_v1(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_publication_signed_family_fence_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) owner to postgres;
revoke all on function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_projection_publish_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_projection_publish_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_projection_publish_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
