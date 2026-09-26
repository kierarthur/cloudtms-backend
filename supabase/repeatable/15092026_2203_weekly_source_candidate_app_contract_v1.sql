-- Repeatable CloudTMS authority: weekly_source_candidate_app_contract_v1
--
-- Exact service-only adapter for the MyTMS Weekly source request contract.
-- It projects only hours/breaks and orchestration identities.  It owns no
-- pay, charge, invoice, Workbench, Banking Pay, remittance or expense logic.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_candidate_app_units_week_v1(
  p_value jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_result jsonb;
begin
  if p_value is null or p_value='null'::jsonb then return '[]'::jsonb; end if;
  if pg_catalog.jsonb_typeof(p_value)<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_UNITS_INVALID' using errcode='22023';
  end if;
  if exists(
    select 1 from pg_catalog.jsonb_each(p_value) item
    where pg_catalog.jsonb_typeof(item.value)<>'number'
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_UNITS_INVALID' using errcode='22023';
  end if;
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('code',item.key,'value',(item.value#>>'{}')::numeric)
    order by item.key collate "C"
  ),'[]'::jsonb) into v_result
  from pg_catalog.jsonb_each(p_value) item;
  return v_result;
end;
$function$;

create or replace function private.weekly_source_candidate_app_units_day_v1(
  p_value jsonb,
  p_date date default null
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_result jsonb;
begin
  if p_value is null or p_value='null'::jsonb then return '[]'::jsonb; end if;
  if pg_catalog.jsonb_typeof(p_value)<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_UNITS_INVALID' using errcode='22023';
  end if;
  if exists(
    select 1
    from pg_catalog.jsonb_each(p_value) day_item
    where pg_catalog.jsonb_typeof(day_item.value)<>'object'
       or exists(
         select 1 from pg_catalog.jsonb_each(day_item.value) unit_item
         where pg_catalog.jsonb_typeof(unit_item.value)<>'number'
       )
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_UNITS_INVALID' using errcode='22023';
  end if;
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'date',day_item.key,'code',unit_item.key,
      'value',(unit_item.value#>>'{}')::numeric
    ) order by day_item.key collate "C",unit_item.key collate "C"
  ),'[]'::jsonb) into v_result
  from pg_catalog.jsonb_each(p_value) day_item
  cross join lateral pg_catalog.jsonb_each(day_item.value) unit_item
  where p_date is null or day_item.key=pg_catalog.to_char(p_date,'YYYY-MM-DD');
  return v_result;
end;
$function$;

create or replace function private.weekly_source_candidate_app_break_v1(
  p_break jsonb,
  p_fallback_minutes integer default 0
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_kind text;
  v_start time;
  v_end time;
  v_minutes integer;
  v_calculated integer;
begin
  if p_break is null or p_break='null'::jsonb then
    if coalesce(p_fallback_minutes,0)<0 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    if coalesce(p_fallback_minutes,0)=0 then
      return pg_catalog.jsonb_build_object(
        'kind','NO_BREAK','no_break',true,'break_minutes',0
      );
    end if;
    return pg_catalog.jsonb_build_object(
      'kind','DURATION_MINUTES','break_minutes',p_fallback_minutes
    );
  end if;
  if pg_catalog.jsonb_typeof(p_break)<>'object' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
  end if;
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_break->>'kind','')));
  if v_kind='NO_BREAK' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_break) key
              where key not in ('kind','no_break','break_minutes'))
       or coalesce((p_break->>'no_break')::boolean,false)<>true
       or coalesce((p_break->>'break_minutes')::integer,0)<>0 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    return pg_catalog.jsonb_build_object(
      'kind','NO_BREAK','no_break',true,'break_minutes',0
    );
  elsif v_kind='DURATION_MINUTES' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_break) key
              where key not in ('kind','break_minutes')) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    v_minutes:=(p_break->>'break_minutes')::integer;
    if v_minutes<1 or v_minutes>720 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    return pg_catalog.jsonb_build_object(
      'kind','DURATION_MINUTES','break_minutes',v_minutes
    );
  elsif v_kind='START_END_TIMES' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_break) key
              where key not in ('kind','break_start','break_end','calculated_break_minutes'))
       or coalesce(p_break->>'break_start','') !~ '^[0-9]{2}:[0-9]{2}$'
       or coalesce(p_break->>'break_end','') !~ '^[0-9]{2}:[0-9]{2}$' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    begin
      v_start:=(p_break->>'break_start')::time;
      v_end:=(p_break->>'break_end')::time;
      v_minutes:=(p_break->>'calculated_break_minutes')::integer;
    exception when others then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end;
    v_calculated:=(pg_catalog.date_part('epoch',v_end-v_start)/60)::integer;
    if v_calculated<=0 then v_calculated:=v_calculated+1440; end if;
    if v_minutes<>v_calculated or v_minutes<1 or v_minutes>720 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
    end if;
    return pg_catalog.jsonb_build_object(
      'kind','START_END_TIMES','break_start',pg_catalog.to_char(v_start,'HH24:MI'),
      'break_end',pg_catalog.to_char(v_end,'HH24:MI'),
      'calculated_break_minutes',v_minutes
    );
  end if;
  raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
exception when invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_CANDIDATE_BREAK_INVALID' using errcode='22023';
end;
$function$;

create or replace function private.weekly_source_candidate_app_schedule_v1(
  p_schedule jsonb,
  p_additional_units_per_day jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_item jsonb;
  v_date date;
  v_start_text text;
  v_end_text text;
  v_start time;
  v_end time;
  v_break jsonb;
  v_rows jsonb:='[]'::jsonb;
  v_result jsonb;
begin
  if p_schedule is null or p_schedule='null'::jsonb then return '[]'::jsonb; end if;
  if pg_catalog.jsonb_typeof(p_schedule)<>'array' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SCHEDULE_INVALID' using errcode='22023';
  end if;
  for v_item in select value from pg_catalog.jsonb_array_elements(p_schedule)
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SCHEDULE_INVALID' using errcode='22023';
    end if;
    begin
      v_date:=coalesce(
        nullif(v_item->>'date','')::date,
        nullif(v_item->>'work_date','')::date,
        substring(coalesce(v_item->>'worked_start_iso',v_item->>'start_utc','') from 1 for 10)::date
      );
      v_start_text:=coalesce(
        nullif(v_item->>'start_time',''),nullif(v_item->>'start',''),
        pg_catalog.to_char(coalesce(
          nullif(v_item->>'worked_start_iso','')::timestamptz,
          nullif(v_item->>'start_utc','')::timestamptz
        ) at time zone 'Europe/London','HH24:MI')
      );
      v_end_text:=coalesce(
        nullif(v_item->>'end_time',''),nullif(v_item->>'end',''),
        pg_catalog.to_char(coalesce(
          nullif(v_item->>'worked_end_iso','')::timestamptz,
          nullif(v_item->>'end_utc','')::timestamptz
        ) at time zone 'Europe/London','HH24:MI')
      );
      if v_date is null or v_start_text !~ '^[0-9]{2}:[0-9]{2}$'
         or v_end_text !~ '^[0-9]{2}:[0-9]{2}$' then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_SCHEDULE_INVALID' using errcode='22023';
      end if;
      v_start:=v_start_text::time;
      v_end:=v_end_text::time;
      if v_start=v_end then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_SCHEDULE_INVALID' using errcode='22023';
      end if;
      v_break:=private.weekly_source_candidate_app_break_v1(
        v_item->'break_entry',coalesce(nullif(v_item->>'break_minutes','')::integer,0)
      );
    exception when others then
      if sqlerrm like 'WEEKLY_SOURCE_CANDIDATE_%' then raise; end if;
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SCHEDULE_INVALID' using errcode='22023';
    end;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'row_key',nullif(v_item->>'row_key',''),
      'worked',true,
      'date',pg_catalog.to_char(v_date,'YYYY-MM-DD'),
      'start',pg_catalog.to_char(v_start,'HH24:MI'),
      'end',pg_catalog.to_char(v_end,'HH24:MI'),
      'break_entry',v_break,
      'additional_units',(
        select coalesce(pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object('code',unit->>'code','value',(unit->>'value')::numeric)
          order by unit->>'code' collate "C"
        ),'[]'::jsonb)
        from pg_catalog.jsonb_array_elements(
          private.weekly_source_candidate_app_units_day_v1(p_additional_units_per_day,v_date)
        ) unit
      )
    ));
  end loop;
  select coalesce(pg_catalog.jsonb_agg(value order by
    value->>'date',value->>'start',value->>'end',coalesce(value->>'row_key','') collate "C"
  ),'[]'::jsonb) into v_result
  from pg_catalog.jsonb_array_elements(v_rows);
  return v_result;
end;
$function$;

create or replace function private.weekly_source_candidate_app_issue_hours_v1(
  p_date date,
  p_start timestamp without time zone,
  p_end timestamp without time zone,
  p_break_minutes integer,
  p_row_key text,
  p_additional_units jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
begin
  if p_start is null or p_end is null then
    return pg_catalog.jsonb_build_object(
      'row_key',p_row_key,'worked',false,'date',pg_catalog.to_char(p_date,'YYYY-MM-DD'),
      'start',null,'end',null,
      'break_entry',private.weekly_source_candidate_app_break_v1(null,0),
      'additional_units',coalesce(p_additional_units,'[]'::jsonb)
    );
  end if;
  if p_end<=p_start or coalesce(p_break_minutes,0)<0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_INVALID' using errcode='55000';
  end if;
  return pg_catalog.jsonb_build_object(
    'row_key',p_row_key,'worked',true,'date',pg_catalog.to_char(p_date,'YYYY-MM-DD'),
    'start',pg_catalog.to_char(p_start,'HH24:MI'),
    'end',pg_catalog.to_char(p_end,'HH24:MI'),
    'break_entry',private.weekly_source_candidate_app_break_v1(null,coalesce(p_break_minutes,0)),
    'additional_units',coalesce(p_additional_units,'[]'::jsonb)
  );
end;
$function$;

create or replace function private.weekly_source_candidate_app_projection_v1(
  p_candidate_id uuid,
  p_request_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_scope record;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_contract_week_id uuid;
  v_scope_id uuid;
  v_scope_version integer;
  v_scope_state text;
  v_scope_fingerprint text;
  v_timesheet_hash bytea;
  v_schedule jsonb;
  v_units_week jsonb;
  v_units_day jsonb;
  v_issues jsonb;
  v_issue_facts jsonb;
  v_draft public.weekly_candidate_response_drafts%rowtype;
  v_draft_id uuid;
  v_scopes jsonb:='[]'::jsonb;
  v_sorted_scopes jsonb;
  v_request_state text;
  v_earliest uuid;
  v_request_fingerprint text;
begin
  if p_candidate_id is null or p_request_id is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_INVALID' using errcode='22023';
  end if;
  select * into v_generation
  from public.weekly_candidate_outreach_generations
  where id=p_request_id and candidate_id=p_candidate_id;
  if not found or v_generation.state in ('SUPERSEDED','CANCELLED') then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_NOT_FOUND' using errcode='P0002';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_generation.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  perform private.weekly_source_query_current_publication_v1(
    v_cycle.id,v_cycle.current_projection_publication_id
  );

  select * into v_draft
  from public.weekly_candidate_response_drafts
  where candidate_generation_id=v_generation.id and candidate_id=p_candidate_id
    and state='DRAFT'
  order by draft_version desc,id desc limit 1;
  v_draft_id:=case when found then v_draft.id else null end;

  if v_generation.request_kind='CHECK_HOURS' then
    for v_scope in
      select min(membership.id::text)::uuid as scope_id,
        comparison.contract_id,comparison.candidate_timesheet_id,
        timesheet.week_ending_date,
        pg_catalog.count(*)::integer as member_count,
        pg_catalog.count(*) filter (
          where membership.state='ACTIONABLE' and incident.state='OPEN'
        )::integer as open_count,
        pg_catalog.count(*) filter (
          where membership.state in ('ANSWERED','RESOLVED') or incident.state='RESOLVED'
        )::integer as answered_count
      from public.weekly_candidate_outreach_memberships membership
      join public.weekly_discrepancy_incidents incident
        on incident.id=membership.incident_id
      join public.weekly_issue_comparison_revisions comparison
        on comparison.id=incident.current_comparison_revision_id
      join public.timesheets timesheet
        on timesheet.timesheet_id=comparison.candidate_timesheet_id
      where membership.candidate_generation_id=v_generation.id
        and membership.state<>'SUPERSEDED'
        and membership.comparison_revision_id=incident.current_comparison_revision_id
      group by comparison.contract_id,comparison.candidate_timesheet_id,timesheet.week_ending_date
      order by timesheet.week_ending_date,comparison.contract_id,comparison.candidate_timesheet_id
    loop
      if v_scope.contract_id is null or v_scope.candidate_timesheet_id is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_SCOPE_INVALID' using errcode='55000';
      end if;
      select * into strict v_timesheet from public.timesheets
      where timesheet_id=v_scope.candidate_timesheet_id
        and contract_id=v_scope.contract_id and week_ending_date=v_scope.week_ending_date
        and is_current=true and archived_at_utc is null;
      select * into strict v_contract from public.contracts
      where id=v_scope.contract_id and candidate_id=p_candidate_id
        and client_id=v_generation.client_id;
      select * into strict v_client from public.clients where id=v_contract.client_id;
      select week_row.id into v_contract_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_timesheet.timesheet_id
        and week_row.contract_id=v_contract.id
        and week_row.week_ending_date=v_timesheet.week_ending_date
      order by week_row.updated_at desc,week_row.id desc limit 1;
      if v_contract_week_id is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
      end if;
      v_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(
        v_timesheet.timesheet_id
      );
      if v_timesheet_hash is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
      end if;
      v_schedule:=private.weekly_source_candidate_app_schedule_v1(
        v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day
      );
      v_units_week:=private.weekly_source_candidate_app_units_week_v1(
        v_timesheet.additional_units_week
      );
      v_units_day:=private.weekly_source_candidate_app_units_day_v1(
        v_timesheet.additional_units_per_day,null
      );

      select coalesce(pg_catalog.jsonb_agg(issue_row order by
        issue_row->>'date',issue_row->'submitted_hours'->>'start',issue_row->>'issue_id'
      ),'[]'::jsonb),
      coalesce(pg_catalog.jsonb_agg(fact_row order by fact_row->>'incident_id'),'[]'::jsonb)
      into v_issues,v_issue_facts
      from (
        select
          pg_catalog.jsonb_build_object(
            'issue_id',incident.id,
            'issue_fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'),
            'issue_family',case
              when comparison.issue_family='SOURCE_HOURS_DIFFER' then 'HOURS_DIFFER'
              when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
                   and v_group.source_family='NHSP' then 'NHSP_ABSENT'
              when comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' then 'SOURCE_ABSENT'
              when comparison.issue_family='HEALTHROSTER_NOT_FINALISED' then 'HEALTHROSTER_NOT_FINALISED'
              else 'UNSUPPORTED'
            end,
            'date',pg_catalog.to_char(work_event.work_date,'YYYY-MM-DD'),
            'submitted_hours',private.weekly_source_candidate_app_issue_hours_v1(
              work_event.work_date,comparison.candidate_start_at_local,
              comparison.candidate_end_at_local,comparison.candidate_break_minutes,
              matched.row_key,matched.additional_units
            ),
            'system_hours',private.weekly_source_candidate_app_issue_hours_v1(
              work_event.work_date,
              case when comparison.source_presence='PRESENT' then comparison.system_start_at_local end,
              case when comparison.source_presence='PRESENT' then comparison.system_end_at_local end,
              case when comparison.source_presence='PRESENT' then comparison.system_break_minutes else 0 end,
              matched.row_key,matched.additional_units
            ),
            'saved_draft_response',case
              when membership.state<>'ACTIONABLE' or incident.state<>'OPEN' then null
              when draft_item.choice='CANDIDATE_CORRECT' then pg_catalog.jsonb_build_object(
                'issue_id',incident.id,
                'issue_fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'),
                'answer_code','MY_HOURS_CORRECT'
              )
              when draft_item.choice='CANDIDATE_WRONG' then pg_catalog.jsonb_build_object(
                'issue_id',incident.id,
                'issue_fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'),
                'answer_code','MY_HOURS_WRONG_SYSTEM_CORRECT'
              )
              when draft_item.choice='NEITHER_CORRECT' then pg_catalog.jsonb_build_object(
                'issue_id',incident.id,
                'issue_fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'),
                'answer_code','NEITHER_CORRECT',
                'corrected_hours',private.weekly_source_candidate_app_issue_hours_v1(
                  work_event.work_date,draft_item.corrected_start_at_local,
                  draft_item.corrected_end_at_local,draft_item.corrected_break_minutes,
                  matched.row_key,matched.additional_units
                )
              )
              else null
            end,
            'state',case when membership.state='ACTIONABLE' and incident.state='OPEN'
              then 'OPEN' else 'ANSWERED' end
          ) as issue_row,
          pg_catalog.jsonb_build_object(
            'incident_id',incident.id,'membership_id',membership.id,
            'membership_state',membership.state,'incident_state',incident.state,
            'comparison_revision_id',comparison.id,
            'comparison_fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex')
          ) as fact_row
        from public.weekly_candidate_outreach_memberships membership
        join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
        join public.weekly_issue_comparison_revisions comparison
          on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        left join public.weekly_candidate_response_draft_items draft_item
          on draft_item.response_draft_id=v_draft_id and draft_item.incident_id=incident.id
        left join lateral (
          select hours->>'row_key' as row_key,
            coalesce(hours->'additional_units','[]'::jsonb) as additional_units
          from pg_catalog.jsonb_array_elements(v_schedule) hours
          where hours->>'date'=pg_catalog.to_char(work_event.work_date,'YYYY-MM-DD')
            and hours->>'start'=pg_catalog.to_char(comparison.candidate_start_at_local,'HH24:MI')
            and hours->>'end'=pg_catalog.to_char(comparison.candidate_end_at_local,'HH24:MI')
          order by coalesce(hours->>'row_key','') collate "C" limit 1
        ) matched on true
        where membership.candidate_generation_id=v_generation.id
          and membership.state<>'SUPERSEDED'
          and comparison.contract_id=v_scope.contract_id
          and comparison.candidate_timesheet_id=v_scope.candidate_timesheet_id
      ) issue_rows;
      if exists(
        select 1 from pg_catalog.jsonb_array_elements(v_issues) issue
        where issue->>'issue_family'='UNSUPPORTED'
      ) then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_ISSUE_UNSUPPORTED' using errcode='55000';
      end if;
      v_scope_id:=v_scope.scope_id;
      v_scope_version:=v_timesheet.version;
      v_scope_state:=case
        when v_scope.open_count=0 then 'COMPLETE'
        when v_scope.answered_count>0 then 'PARTLY_COMPLETE'
        else 'OUTSTANDING'
      end;
      v_scope_fingerprint:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CANDIDATE_APP_SCOPE_V1',pg_catalog.jsonb_build_object(
          'scope_id',v_scope_id,'scope_version',v_scope_version,
          'request_kind','CHECK_HOURS','publication_id',v_cycle.current_projection_publication_id,
          'timesheet_hash',pg_catalog.encode(v_timesheet_hash,'hex'),
          'issue_facts',v_issue_facts,
          'draft_hash',case when v_draft_id is not null then pg_catalog.encode(v_draft.draft_hash,'hex') end
        )
      ),'hex');
      v_scopes:=v_scopes||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'scope_id',v_scope_id,'scope_version',v_scope_version,
        'scope_fingerprint',v_scope_fingerprint,'request_kind','CHECK_HOURS',
        'completion_state',v_scope_state,
        'week_ending_date',pg_catalog.to_char(v_timesheet.week_ending_date,'YYYY-MM-DD'),
        'contract_id',v_contract.id,'contract_week_id',v_contract_week_id,
        'client_name',v_client.name,'job_title',nullif(pg_catalog.btrim(coalesce(v_contract.role,'')),''),
        'detail_target',pg_catalog.jsonb_build_object(
          'identity_kind','timesheet','id',v_timesheet.timesheet_id
        ),
        'workflow_binding',null,
        'submitted_timesheet',v_schedule,
        'additional_units_week',v_units_week,
        'additional_units_per_day',v_units_day,
        'issues',v_issues
      ));
    end loop;
  else
    select * into v_submission
    from public.weekly_timesheet_submission_requests
    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=p_candidate_id
      and state not in ('SUPERSEDED','CANCELLED')
    order by request_generation desc,id desc limit 1;
    if not found then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMISSION_REQUEST_NOT_FOUND' using errcode='P0002';
    end if;
    for v_scope in
      select membership.* from public.weekly_timesheet_submission_request_memberships membership
      where membership.submission_request_id=v_submission.id
      order by membership.week_ending,membership.client_id,membership.contract_id,membership.id
    loop
      select * into strict v_contract from public.contracts
      where id=v_scope.contract_id and candidate_id=p_candidate_id
        and client_id=v_scope.client_id;
      select * into strict v_client from public.clients where id=v_contract.client_id;
      select week_row.id into v_contract_week_id
      from public.contract_weeks week_row
      where week_row.contract_id=v_scope.contract_id
        and week_row.week_ending_date=v_scope.week_ending
        and week_row.additional_seq=0 and week_row.is_adjustment=false
      order by week_row.updated_at desc,week_row.id desc limit 1;
      if v_contract_week_id is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
      end if;
      if v_scope.submitted_timesheet_id is not null then
        select * into strict v_timesheet from public.timesheets
        where timesheet_id=v_scope.submitted_timesheet_id
          and contract_id=v_scope.contract_id and week_ending_date=v_scope.week_ending;
        v_schedule:=private.weekly_source_candidate_app_schedule_v1(
          v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day
        );
        v_units_week:=private.weekly_source_candidate_app_units_week_v1(
          v_timesheet.additional_units_week
        );
        v_units_day:=private.weekly_source_candidate_app_units_day_v1(
          v_timesheet.additional_units_per_day,null
        );
      else
        v_schedule:='[]'::jsonb;
        v_units_week:='[]'::jsonb;
        v_units_day:='[]'::jsonb;
      end if;
      v_scope_id:=v_scope.id;
      v_scope_version:=v_submission.request_generation;
      v_scope_state:=case when v_scope.state='WAITING' then 'OUTSTANDING' else 'COMPLETE' end;
      v_scope_fingerprint:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CANDIDATE_APP_SCOPE_V1',pg_catalog.jsonb_build_object(
          'scope_id',v_scope.id,'scope_version',v_scope_version,
          'request_kind','SUBMIT_TIMESHEET','publication_id',v_cycle.current_projection_publication_id,
          'membership_state',v_scope.state,
          'expected_source_fingerprint',pg_catalog.encode(v_scope.expected_source_fingerprint,'hex'),
          'submitted_timesheet_hash',case when v_scope.submitted_timesheet_hash is not null
            then pg_catalog.encode(v_scope.submitted_timesheet_hash,'hex') end
        )
      ),'hex');
      v_scopes:=v_scopes||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'scope_id',v_scope_id,'scope_version',v_scope_version,
        'scope_fingerprint',v_scope_fingerprint,'request_kind','SUBMIT_TIMESHEET',
        'completion_state',v_scope_state,
        'week_ending_date',pg_catalog.to_char(v_scope.week_ending,'YYYY-MM-DD'),
        'contract_id',v_contract.id,'contract_week_id',v_contract_week_id,
        'client_name',v_client.name,'job_title',nullif(pg_catalog.btrim(coalesce(v_contract.role,'')),''),
        'detail_target',pg_catalog.jsonb_build_object(
          'identity_kind','contract-week','id',v_contract_week_id
        ),
        'workflow_binding',null,
        'submitted_timesheet',v_schedule,
        'additional_units_week',v_units_week,
        'additional_units_per_day',v_units_day,
        'issues','[]'::jsonb
      ));
    end loop;
  end if;

  if pg_catalog.jsonb_array_length(v_scopes)=0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_EMPTY' using errcode='55000';
  end if;
  select pg_catalog.jsonb_agg(value order by
    value->>'week_ending_date',private.weekly_source_query_ascii_fold_v1(value->>'client_name') collate "C",
    value->>'contract_id',value->>'scope_id'
  ) into v_sorted_scopes from pg_catalog.jsonb_array_elements(v_scopes);
  select case
    when pg_catalog.count(*) filter (where value->>'completion_state'<>'COMPLETE')=0 then 'COMPLETE'
    when pg_catalog.count(*) filter (where value->>'completion_state'='COMPLETE')>0
      or pg_catalog.count(*) filter (where value->>'completion_state'='PARTLY_COMPLETE')>0
      then 'PARTLY_COMPLETE'
    else 'OPEN'
  end into v_request_state
  from pg_catalog.jsonb_array_elements(v_sorted_scopes);
  select (value->>'scope_id')::uuid into v_earliest
  from pg_catalog.jsonb_array_elements(v_sorted_scopes)
  where value->>'completion_state'<>'COMPLETE'
  order by value->>'week_ending_date',value->>'client_name',value->>'contract_id',value->>'scope_id'
  limit 1;
  v_request_fingerprint:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CANDIDATE_APP_REQUEST_V1',pg_catalog.jsonb_build_object(
      'request_id',v_generation.id,'request_version',v_generation.generation_number,
      'request_kind',v_generation.request_kind,'generation_state',v_generation.state,
      'membership_hash',pg_catalog.encode(v_generation.membership_hash,'hex'),
      'publication_id',v_cycle.current_projection_publication_id,
      'scopes',(
        select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'scope_id',value->>'scope_id','scope_version',value->'scope_version',
          'scope_fingerprint',value->>'scope_fingerprint',
          'completion_state',value->>'completion_state'
        ) order by value->>'scope_id')
        from pg_catalog.jsonb_array_elements(v_sorted_scopes)
      )
    )
  ),'hex');
  return pg_catalog.jsonb_build_object(
    'ok',true,'request_id',v_generation.id,
    'request_version',v_generation.generation_number,
    'request_fingerprint',v_request_fingerprint,
    'state',v_request_state,'earliest_outstanding_scope_id',v_earliest,
    'scopes',v_sorted_scopes
  );
end;
$function$;

create or replace function public.weekly_source_candidate_app_request_get_v1(
  p_session_id uuid,
  p_environment text,
  p_request_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  return private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
end;
$function$;

create or replace function private.weekly_source_candidate_app_assert_request_shape_v1(
  p_result jsonb
) returns void
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb;
  v_issue jsonb;
begin
  if p_result is null or pg_catalog.jsonb_typeof(p_result)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_result) key
               where key not in (
                 'ok','request_id','request_version','request_fingerprint','state',
                 'earliest_outstanding_scope_id','scopes'
               ))
     or p_result->>'ok'<>'true'
     or coalesce(p_result->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
     or p_result->>'state' not in ('OPEN','PARTLY_COMPLETE','COMPLETE')
     or pg_catalog.jsonb_typeof(p_result->'scopes')<>'array'
     or pg_catalog.jsonb_array_length(p_result->'scopes')<1 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
  end if;
  begin
    perform (p_result->>'request_id')::uuid;
    perform (p_result->>'request_version')::integer;
    if p_result->>'earliest_outstanding_scope_id' is not null then
      perform (p_result->>'earliest_outstanding_scope_id')::uuid;
    end if;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
  end;
  for v_scope in select value from pg_catalog.jsonb_array_elements(p_result->'scopes')
  loop
    if pg_catalog.jsonb_typeof(v_scope)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_scope) key
                 where key not in (
                   'scope_id','scope_version','scope_fingerprint','request_kind',
                   'completion_state','week_ending_date','contract_id','contract_week_id',
                   'client_name','job_title','detail_target','workflow_binding',
                   'submitted_timesheet','additional_units_week','additional_units_per_day','issues'
                 ))
       or coalesce(v_scope->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$'
       or v_scope->>'request_kind' not in ('CHECK_HOURS','SUBMIT_TIMESHEET')
       or v_scope->>'completion_state' not in ('OUTSTANDING','PARTLY_COMPLETE','COMPLETE')
       or pg_catalog.jsonb_typeof(v_scope->'submitted_timesheet')<>'array'
       or pg_catalog.jsonb_typeof(v_scope->'additional_units_week')<>'array'
       or pg_catalog.jsonb_typeof(v_scope->'additional_units_per_day')<>'array'
       or pg_catalog.jsonb_typeof(v_scope->'issues')<>'array' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
    end if;
    for v_issue in select value from pg_catalog.jsonb_array_elements(v_scope->'issues')
    loop
      if pg_catalog.jsonb_typeof(v_issue)<>'object'
         or exists(select 1 from pg_catalog.jsonb_object_keys(v_issue) key
                   where key not in (
                     'issue_id','issue_fingerprint','issue_family','date','submitted_hours',
                     'system_hours','saved_draft_response','state'
                   ))
         or coalesce(v_issue->>'issue_fingerprint','') !~ '^[0-9a-f]{64}$'
         or v_issue->>'issue_family' not in (
           'HOURS_DIFFER','NHSP_ABSENT','SOURCE_ABSENT','HEALTHROSTER_NOT_FINALISED'
         )
         or v_issue->>'state' not in ('OPEN','ANSWERED') then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
      end if;
    end loop;
  end loop;
end;
$function$;

create or replace function private.weekly_source_candidate_app_responses_v1(
  p_scope jsonb,
  p_responses jsonb,
  p_require_all boolean
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_response jsonb;
  v_issue jsonb;
  v_issue_id uuid;
  v_answer text;
  v_hours jsonb;
  v_break jsonb;
  v_break_minutes integer;
  v_start_time time;
  v_end_time time;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_internal_choice text;
  v_seen uuid[]:='{}'::uuid[];
  v_result jsonb:='[]'::jsonb;
  v_expected_count integer;
begin
  if pg_catalog.jsonb_typeof(p_scope)<>'object'
     or pg_catalog.jsonb_typeof(p_responses)<>'array' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSES_INVALID' using errcode='22023';
  end if;
  select pg_catalog.count(*)::integer into v_expected_count
  from pg_catalog.jsonb_array_elements(p_scope->'issues') issue
  where issue->>'state'='OPEN';
  if p_require_all and pg_catalog.jsonb_array_length(p_responses)<>v_expected_count then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_ALL_OPEN_ISSUES_REQUIRED' using errcode='22023';
  end if;
  for v_response in select value from pg_catalog.jsonb_array_elements(p_responses)
  loop
    if pg_catalog.jsonb_typeof(v_response)<>'object' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
    end if;
    begin
      v_issue_id:=(v_response->>'issue_id')::uuid;
    exception when others then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
    end;
    if v_issue_id=any(v_seen) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_DUPLICATE' using errcode='22023';
    end if;
    v_seen:=pg_catalog.array_append(v_seen,v_issue_id);
    select value into v_issue
    from pg_catalog.jsonb_array_elements(p_scope->'issues')
    where value->>'issue_id'=v_issue_id::text and value->>'state'='OPEN';
    if v_issue is null
       or coalesce(v_response->>'issue_fingerprint','')<>v_issue->>'issue_fingerprint' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_STALE' using errcode='40001';
    end if;
    select * into strict v_incident from public.weekly_discrepancy_incidents
    where id=v_issue_id and state='OPEN';
    select * into strict v_comparison from public.weekly_issue_comparison_revisions
    where id=v_incident.current_comparison_revision_id
      and pg_catalog.encode(material_comparison_fingerprint,'hex')=v_issue->>'issue_fingerprint';
    v_answer:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_response->>'answer_code','')));
    if v_answer='MY_HOURS_CORRECT' then
      if exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                where key not in ('issue_id','issue_fingerprint','answer_code')) then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
      end if;
      v_internal_choice:='CANDIDATE_CORRECT';
      v_start:=null; v_end:=null; v_break_minutes:=null;
    elsif v_answer='MY_HOURS_WRONG_SYSTEM_CORRECT' then
      if exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                where key not in ('issue_id','issue_fingerprint','answer_code')) then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
      end if;
      if v_comparison.source_presence='UNFINALISED' then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_SYSTEM_HOURS_UNAVAILABLE' using errcode='22023';
      end if;
      v_internal_choice:='CANDIDATE_WRONG';
      if v_comparison.source_presence='PRESENT' then
        v_start:=v_comparison.system_start_at_local;
        v_end:=v_comparison.system_end_at_local;
        v_break_minutes:=v_comparison.system_break_minutes;
      else
        v_start:=null; v_end:=null; v_break_minutes:=null;
      end if;
    elsif v_answer='NEITHER_CORRECT' then
      if exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                where key not in ('issue_id','issue_fingerprint','answer_code','corrected_hours')) then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
      end if;
      v_hours:=v_response->'corrected_hours';
      if pg_catalog.jsonb_typeof(v_hours)<>'object'
         or exists(select 1 from pg_catalog.jsonb_object_keys(v_hours) key
                   where key not in (
                     'row_key','worked','date','start','end','break_entry','additional_units'
                   ))
         or coalesce((v_hours->>'worked')::boolean,false)<>true
         or v_hours->>'date'<>v_issue->>'date'
         or (v_hours->>'row_key') is distinct from (v_issue->'submitted_hours'->>'row_key')
         or pg_catalog.jsonb_typeof(v_hours->'additional_units')<>'array'
         or v_hours->'additional_units'<>v_issue->'submitted_hours'->'additional_units'
         or coalesce(v_hours->>'start','') !~ '^[0-9]{2}:[0-9]{2}$'
         or coalesce(v_hours->>'end','') !~ '^[0-9]{2}:[0-9]{2}$' then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
      end if;
      begin
        v_start_time:=(v_hours->>'start')::time;
        v_end_time:=(v_hours->>'end')::time;
        v_break:=private.weekly_source_candidate_app_break_v1(v_hours->'break_entry',0);
      exception when others then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
      end;
      if v_start_time=v_end_time or v_break<>v_hours->'break_entry' then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
      end if;
      v_break_minutes:=case v_break->>'kind'
        when 'NO_BREAK' then 0
        when 'DURATION_MINUTES' then (v_break->>'break_minutes')::integer
        else (v_break->>'calculated_break_minutes')::integer
      end;
      v_start:=(v_issue->>'date')::date+v_start_time;
      v_end:=(v_issue->>'date')::date+v_end_time;
      if v_end<=v_start then v_end:=v_end+interval '1 day'; end if;
      if v_comparison.source_presence='PRESENT'
         and v_start=v_comparison.system_start_at_local
         and v_end=v_comparison.system_end_at_local
         and v_break_minutes=v_comparison.system_break_minutes then
        v_internal_choice:='CANDIDATE_WRONG';
      else
        v_internal_choice:='NEITHER_CORRECT';
      end if;
    else
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
    end if;
    v_result:=v_result||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'incident_id',v_issue_id,
      'expected_comparison_fingerprint',v_issue->>'issue_fingerprint',
      'expected_timesheet_hash',pg_catalog.encode(
        private.weekly_source_query_candidate_timesheet_hash_v1(
          v_comparison.candidate_timesheet_id
        ),'hex'
      ),
      'choice',v_internal_choice,
      'corrected_start_at_local',v_start,
      'corrected_end_at_local',v_end,
      'corrected_break_minutes',v_break_minutes
    ));
  end loop;
  if p_require_all and exists(
    select 1 from pg_catalog.jsonb_array_elements(p_scope->'issues') issue
    where issue->>'state'='OPEN'
      and not ((issue->>'issue_id')::uuid=any(v_seen))
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_ALL_OPEN_ISSUES_REQUIRED' using errcode='22023';
  end if;
  return (
    select coalesce(pg_catalog.jsonb_agg(value order by value->>'incident_id'),'[]'::jsonb)
    from pg_catalog.jsonb_array_elements(v_result)
  );
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_STALE' using errcode='40001';
end;
$function$;

create or replace function private.weekly_source_candidate_app_assert_week_revision_v1(
  p_scope jsonb,
  p_api_responses jsonb,
  p_internal_responses jsonb,
  p_immutable_submission jsonb
) returns void
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_schedule_input jsonb;
  v_units_week_input jsonb;
  v_units_day_input jsonb;
  v_actual jsonb;
  v_expected jsonb:=p_scope->'submitted_timesheet';
  v_internal jsonb;
  v_api jsonb;
  v_issue jsonb;
  v_position integer;
  v_match_count integer;
  v_replacement jsonb;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
begin
  if pg_catalog.jsonb_typeof(p_immutable_submission)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_immutable_submission) key
               where key not in (
                 'break_entry_context','actual_schedule_json','schedule_json','break_entry',
                 'worked_minutes','reference_number','day_off_dates','additional_units_week',
                 'additional_units_per_day','timesheet_patch_json','hours_submission'
               )) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_INVALID' using errcode='22023';
  end if;
  v_schedule_input:=coalesce(
    p_immutable_submission->'actual_schedule_json',
    p_immutable_submission->'schedule_json',
    p_immutable_submission->'timesheet_patch_json'->'actual_schedule_json',
    p_immutable_submission->'timesheet_patch_json'->'schedule_json',
    p_immutable_submission->'hours_submission'->'actual_schedule_json',
    p_immutable_submission->'hours_submission'->'schedule_json'
  );
  if v_schedule_input is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_INVALID' using errcode='22023';
  end if;
  v_units_week_input:=coalesce(
    p_immutable_submission->'additional_units_week',
    p_immutable_submission->'timesheet_patch_json'->'additional_units_week',
    p_immutable_submission->'hours_submission'->'additional_units_week',
    '{}'::jsonb
  );
  v_units_day_input:=coalesce(
    p_immutable_submission->'additional_units_per_day',
    p_immutable_submission->'timesheet_patch_json'->'additional_units_per_day',
    p_immutable_submission->'hours_submission'->'additional_units_per_day',
    '{}'::jsonb
  );
  v_actual:=private.weekly_source_candidate_app_schedule_v1(
    v_schedule_input,v_units_day_input
  );
  if private.weekly_source_candidate_app_units_week_v1(v_units_week_input)
       <>p_scope->'additional_units_week'
     or private.weekly_source_candidate_app_units_day_v1(v_units_day_input,null)
       <>p_scope->'additional_units_per_day' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_INVALID' using errcode='22023';
  end if;
  for v_internal in select value from pg_catalog.jsonb_array_elements(p_internal_responses)
  loop
    select value into strict v_api from pg_catalog.jsonb_array_elements(p_api_responses)
    where value->>'issue_id'=v_internal->>'incident_id';
    select value into strict v_issue from pg_catalog.jsonb_array_elements(p_scope->'issues')
    where value->>'issue_id'=v_internal->>'incident_id';
    if v_api->>'answer_code'='MY_HOURS_CORRECT' then continue; end if;
    select pg_catalog.count(*)::integer,min(ordinality)::integer
    into v_match_count,v_position
    from pg_catalog.jsonb_array_elements(v_expected) with ordinality item(value,ordinality)
    where (
      v_issue->'submitted_hours'->>'row_key' is not null
      and value->>'row_key'=v_issue->'submitted_hours'->>'row_key'
    ) or (
      v_issue->'submitted_hours'->>'row_key' is null
      and value->>'date'=v_issue->'submitted_hours'->>'date'
      and value->>'start'=v_issue->'submitted_hours'->>'start'
      and value->>'end'=v_issue->'submitted_hours'->>'end'
    );
    if v_match_count<>1 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_STALE' using errcode='40001';
    end if;
    v_start:=nullif(v_internal->>'corrected_start_at_local','')::timestamp;
    v_end:=nullif(v_internal->>'corrected_end_at_local','')::timestamp;
    v_break:=nullif(v_internal->>'corrected_break_minutes','')::integer;
    if v_start is null then
      v_replacement:=null;
    else
      v_replacement:=private.weekly_source_candidate_app_issue_hours_v1(
        (v_issue->>'date')::date,v_start,v_end,v_break,
        v_issue->'submitted_hours'->>'row_key',
        v_issue->'submitted_hours'->'additional_units'
      );
    end if;
    select coalesce(pg_catalog.jsonb_agg(value order by ordinality),'[]'::jsonb)
    into v_expected
    from pg_catalog.jsonb_array_elements(v_expected) with ordinality item(value,ordinality)
    where ordinality<>v_position;
    if v_replacement is not null then
      v_expected:=v_expected||pg_catalog.jsonb_build_array(v_replacement);
    end if;
    select coalesce(pg_catalog.jsonb_agg(value order by
      value->>'date',value->>'start',value->>'end',coalesce(value->>'row_key','') collate "C"
    ),'[]'::jsonb) into v_expected
    from pg_catalog.jsonb_array_elements(v_expected);
  end loop;
  if v_actual<>v_expected then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_MISMATCH' using errcode='22023';
  end if;
end;
$function$;

create or replace function private.weekly_source_candidate_app_assert_new_week_submission_v1(
  p_scope jsonb,
  p_immutable_submission jsonb
) returns void
language plpgsql
stable
security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
declare
  v_schedule_input jsonb;
  v_units_week_input jsonb;
  v_units_day_input jsonb;
  v_schedule jsonb;
  v_week_ending date;
begin
  if pg_catalog.jsonb_typeof(p_immutable_submission)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_immutable_submission) key
               where key not in (
                 'break_entry_context','actual_schedule_json','schedule_json','break_entry',
                 'worked_minutes','reference_number','day_off_dates','additional_units_week',
                 'additional_units_per_day','timesheet_patch_json','hours_submission'
               )) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
  v_schedule_input:=coalesce(
    p_immutable_submission->'actual_schedule_json',
    p_immutable_submission->'schedule_json',
    p_immutable_submission->'timesheet_patch_json'->'actual_schedule_json',
    p_immutable_submission->'timesheet_patch_json'->'schedule_json',
    p_immutable_submission->'hours_submission'->'actual_schedule_json',
    p_immutable_submission->'hours_submission'->'schedule_json'
  );
  if v_schedule_input is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
  v_units_week_input:=coalesce(
    p_immutable_submission->'additional_units_week',
    p_immutable_submission->'timesheet_patch_json'->'additional_units_week',
    p_immutable_submission->'hours_submission'->'additional_units_week',
    '{}'::jsonb
  );
  v_units_day_input:=coalesce(
    p_immutable_submission->'additional_units_per_day',
    p_immutable_submission->'timesheet_patch_json'->'additional_units_per_day',
    p_immutable_submission->'hours_submission'->'additional_units_per_day',
    '{}'::jsonb
  );
  perform private.weekly_source_candidate_app_units_week_v1(v_units_week_input);
  v_schedule:=private.weekly_source_candidate_app_schedule_v1(
    v_schedule_input,v_units_day_input
  );
  v_week_ending:=(p_scope->>'week_ending_date')::date;
  if exists(
    select 1 from pg_catalog.jsonb_array_elements(v_schedule) row_value
    where (row_value->>'date')::date not between v_week_ending-6 and v_week_ending
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
end;
$function$;

create or replace function private.weekly_source_candidate_app_assert_hours_only_v1(
  p_value jsonb
) returns void
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_key text;
  v_child jsonb;
begin
  if p_value is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_SUBMISSION_INVALID' using errcode='22023';
  end if;
  if pg_catalog.jsonb_typeof(p_value)='object' then
    for v_key,v_child in select key,value from pg_catalog.jsonb_each(p_value)
    loop
      if pg_catalog.lower(v_key) in (
        'canonical_tsfin_snapshot','canonical_financial_snapshot','financials',
        'pay','pay_rate','pay_amount','gross_pay','net_pay','charge','charge_rate',
        'charge_amount','invoice','invoice_id','invoice_line_id','expense','expenses',
        'amount','vat','vat_rate','banking','banking_pay','remittance','payment'
      ) then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_FINANCIAL_AUTHORITY_FORBIDDEN'
          using errcode='22023';
      end if;
      perform private.weekly_source_candidate_app_assert_hours_only_v1(v_child);
    end loop;
  elsif pg_catalog.jsonb_typeof(p_value)='array' then
    for v_child in select value from pg_catalog.jsonb_array_elements(p_value)
    loop
      perform private.weekly_source_candidate_app_assert_hours_only_v1(v_child);
    end loop;
  end if;
end;
$function$;

create or replace function private.weekly_source_candidate_current_rows_v1(
  p_projection_publication_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_contract_id uuid,
  p_week_ending date
) returns table(
  source_row_id uuid,
  source_link_id uuid,
  work_event_id uuid,
  work_date date,
  start_at_local timestamp without time zone,
  end_at_local timestamp without time zone,
  break_minutes integer,
  row_finalisation_state text
)
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select source_row.id,source_link.id,work_event.id,source_row.work_date,
    source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
    source_row.row_finalisation_state
  from public.weekly_source_projection_publications publication
  join public.weekly_source_upload_rows source_row
    on source_row.upload_id=publication.upload_id
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id
   and resolution.generation=publication.authority_scope_version
   and resolution.mapping_state='RESOLVED'
   and resolution.candidate_id=p_candidate_id
   and resolution.client_id=p_client_id
   and resolution.contract_id=p_contract_id
  join public.weekly_work_event_source_links source_link
    on source_link.upload_row_id=source_row.id
   and source_link.row_resolution_id=resolution.id
   and source_link.work_event_id=resolution.work_event_id
  join public.weekly_work_events work_event
    on work_event.id=resolution.work_event_id
   and work_event.candidate_id=p_candidate_id
   and work_event.client_id=p_client_id
  where publication.id=p_projection_publication_id
    and publication.state='CURRENT'
    and source_row.work_date between p_week_ending-6 and p_week_ending
  order by source_row.work_date,source_row.start_at_local,
    source_row.end_at_local,source_row.id;
$function$;

create or replace function private.weekly_source_candidate_app_receipt_v1(
  p_candidate_id uuid,
  p_generation_id uuid,
  p_mutation_kind text,
  p_idempotency_key uuid,
  p_request_hash bytea
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
declare
  v_receipt public.weekly_candidate_app_mutation_receipts%rowtype;
begin
  select * into v_receipt from public.weekly_candidate_app_mutation_receipts
  where idempotency_key=p_idempotency_key;
  if not found then return null; end if;
  if v_receipt.candidate_id<>p_candidate_id
     or v_receipt.candidate_generation_id<>p_generation_id
     or v_receipt.mutation_kind<>p_mutation_kind
     or v_receipt.request_hash<>p_request_hash then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REPLAY_CONFLICT' using errcode='23505';
  end if;
  return v_receipt.response_json;
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
begin
  perform private.weekly_source_query_require_service_v1();
  if p_candidate_generation_id is null or p_projection_publication_id is null
     or p_membership_id is null or p_timesheet_id is null
     or p_timesheet_hash is null or pg_catalog.octet_length(p_timesheet_hash)<>32 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
  end if;

  select * into strict v_generation
  from public.weekly_candidate_outreach_generations
  where id=p_candidate_generation_id and request_kind='SUBMIT_TIMESHEET'
    and state='ACTIVE'
  for update;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_generation.source_cycle_id
  for update;
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
  if v_timesheet.version<1
     or private.weekly_source_query_candidate_timesheet_hash_v1(v_timesheet.timesheet_id)
          is distinct from p_timesheet_hash then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
  end if;
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
    )
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
          'system_start_at_local',case when v_presence='PRESENT'
            then nullif(v_source_json->>'start_at_local','')::timestamp end,
          'system_end_at_local',case when v_presence='PRESENT'
            then nullif(v_source_json->>'end_at_local','')::timestamp end,
          'system_break_minutes',case when v_presence='PRESENT'
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
      'system_start_at_local',case when v_presence='PRESENT'
        then v_source.start_at_local end,
      'system_end_at_local',case when v_presence='PRESENT'
        then v_source.end_at_local end,
      'system_break_minutes',case when v_presence='PRESENT'
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
        v_membership.client_id,v_cycle.id,'OPEN','UNRESOLVED','NOT_REQUIRED',
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

    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_work_event.id,v_issue
    );
    if not v_is_new and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      v_episode:=v_incident.episode_number+1;
      v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
        v_incident.id,v_episode,v_work_event.id,v_issue
      );
      update public.weekly_discrepancy_incidents
      set episode_number=v_episode,reconciliation_state='UNRESOLVED',
        candidate_action_state='NOT_REQUIRED',
        manager_potential_state=case
          when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
               and nullif(v_policy->>'manager_query_recipient','') is not null
            then 'AVAILABLE' else 'NOT_AVAILABLE' end,
        manager_action_state=case
          when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
               and nullif(v_policy->>'manager_query_recipient','') is not null
            then 'NOT_SENT' else 'NOT_REQUIRED' end,
        waiting_source_state='NOT_WAITING',resolved_at_utc=null,resolution_kind=null
      where id=v_incident.id;
      update public.office_action_notifications
      set operational_state='RESOLVED',resolved_at_utc=p_now_utc
      where issue_id=v_incident.id and operational_state='OPEN';
      v_incident.episode_number:=v_episode;
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
      set current_comparison_revision_id=v_comparison.id
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

create or replace function public.weekly_source_candidate_app_draft_save_atomic_v1(
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
  v_key uuid;
  v_hash bytea;
  v_replay jsonb;
  v_projection jsonb;
  v_scope jsonb;
  v_internal jsonb;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_draft public.weekly_candidate_response_drafts%rowtype;
  v_item jsonb;
  v_draft_version integer;
  v_publication_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
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
                 'request_version','request_fingerprint','scope_id','scope_version',
                 'scope_fingerprint','responses','idempotency_key'
               ))
     or pg_catalog.jsonb_typeof(p_body->'responses')<>'array'
     or coalesce(p_body->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(p_body->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_DRAFT_INVALID' using errcode='22023';
  end if;
  begin
    v_key:=(p_body->>'idempotency_key')::uuid;
    perform (p_body->>'request_version')::integer;
    perform (p_body->>'scope_id')::uuid;
    perform (p_body->>'scope_version')::integer;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_DRAFT_INVALID' using errcode='22023';
  end;
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CANDIDATE_APP_DRAFT_V1',pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'request_id',p_request_id,
      'body',p_body-'idempotency_key'
    )
  );
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_CANDIDATE_APP:'||v_key::text,0
  ));
  v_replay:=private.weekly_source_candidate_app_receipt_v1(
    v_candidate_id,p_request_id,'DRAFT_SAVE',v_key,v_hash
  );
  if v_replay is not null then return v_replay; end if;

  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=p_request_id and candidate_id=v_candidate_id for update;
  if v_generation.state<>'ACTIVE' or v_generation.request_kind<>'CHECK_HOURS'
     or p_now_utc>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
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
     or v_scope->>'request_kind'<>'CHECK_HOURS'
     or v_scope->>'completion_state'='COMPLETE' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE' using errcode='40001';
  end if;
  v_internal:=private.weekly_source_candidate_app_responses_v1(
    v_scope,p_body->'responses',false
  );
  update public.weekly_candidate_response_drafts
  set state='SUPERSEDED',updated_at_utc=p_now_utc
  where candidate_generation_id=p_request_id and candidate_id=v_candidate_id
    and state='DRAFT';
  select coalesce(pg_catalog.max(draft_version),0)+1 into v_draft_version
  from public.weekly_candidate_response_drafts where candidate_generation_id=p_request_id;
  select current_projection_publication_id into strict v_publication_id
  from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  insert into public.weekly_candidate_response_drafts(
    candidate_generation_id,candidate_id,draft_version,current_projection_publication_id,
    state,draft_hash,created_at_utc,updated_at_utc
  ) values (
    p_request_id,v_candidate_id,v_draft_version,v_publication_id,
    'DRAFT',v_hash,p_now_utc,p_now_utc
  ) returning * into v_draft;
  for v_item in select value from pg_catalog.jsonb_array_elements(v_internal)
  loop
    insert into public.weekly_candidate_response_draft_items(
      response_draft_id,incident_id,comparison_revision_id,choice,
      corrected_start_at_local,corrected_end_at_local,corrected_break_minutes,
      response_fingerprint,created_at_utc
    ) select
      v_draft.id,(v_item->>'incident_id')::uuid,incident.current_comparison_revision_id,
      v_item->>'choice',nullif(v_item->>'corrected_start_at_local','')::timestamp,
      nullif(v_item->>'corrected_end_at_local','')::timestamp,
      nullif(v_item->>'corrected_break_minutes','')::integer,
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CANDIDATE_APP_DRAFT_ITEM_V1',v_item
      ),p_now_utc
    from public.weekly_discrepancy_incidents incident
    where incident.id=(v_item->>'incident_id')::uuid;
  end loop;
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
  perform private.weekly_source_candidate_app_assert_request_shape_v1(v_projection);
  insert into public.weekly_candidate_app_mutation_receipts(
    candidate_generation_id,candidate_id,mutation_kind,idempotency_key,
    request_hash,response_json,created_at_utc
  ) values (
    p_request_id,v_candidate_id,'DRAFT_SAVE',v_key,v_hash,v_projection,p_now_utc
  );
  return v_projection;
end;
$function$;

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

-- Candidate-initiated signed CHECK_ONLY evidence does not require an import
-- cycle or an Office outreach generation.  The materialisation owner performs
-- the same workflow, signature, root-week and financial-state checks.
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

alter function private.weekly_source_candidate_app_units_week_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_units_day_v1(jsonb,date) owner to postgres;
alter function private.weekly_source_candidate_app_break_v1(jsonb,integer) owner to postgres;
alter function private.weekly_source_candidate_app_schedule_v1(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_issue_hours_v1(date,timestamp without time zone,timestamp without time zone,integer,text,jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_projection_v1(uuid,uuid,timestamptz) owner to postgres;
alter function private.weekly_source_candidate_app_assert_request_shape_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_responses_v1(jsonb,jsonb,boolean) owner to postgres;
alter function private.weekly_source_candidate_app_assert_week_revision_v1(jsonb,jsonb,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_assert_new_week_submission_v1(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_candidate_app_assert_hours_only_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_current_rows_v1(uuid,uuid,uuid,uuid,date) owner to postgres;
alter function private.weekly_source_candidate_app_receipt_v1(uuid,uuid,text,uuid,bytea) owner to postgres;
alter function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz) owner to postgres;
alter function public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz) owner to postgres;
alter function public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz) owner to postgres;
alter function public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz) owner to postgres;
alter function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz) owner to postgres;
alter function public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz) owner to postgres;

revoke all on function private.weekly_source_candidate_app_units_week_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_units_day_v1(jsonb,date) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_break_v1(jsonb,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_schedule_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_issue_hours_v1(date,timestamp without time zone,timestamp without time zone,integer,text,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_projection_v1(uuid,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_assert_request_shape_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_responses_v1(jsonb,jsonb,boolean) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_assert_week_revision_v1(jsonb,jsonb,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_assert_new_week_submission_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_assert_hours_only_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_current_rows_v1(uuid,uuid,uuid,uuid,date) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_app_receipt_v1(uuid,uuid,text,uuid,bytea) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz) from public,anon,authenticated;
revoke all on function public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz) from public,anon,authenticated;
revoke all on function public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz) from public,anon,authenticated;
revoke all on function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz) from public,anon,authenticated;
revoke all on function public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz) to service_role;
grant execute on function public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz) to service_role;
grant execute on function public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz) to service_role;
grant execute on function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz) to service_role;
grant execute on function public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz) to service_role;

commit;
