-- Bounded, server-owned import-review scope discovery and action paging for the
-- durable review UI. This repeatable also advances the fail-closed Worker/UI
-- contract without changing the financial apply envelope.

create or replace function public.import_review_contract_version_get_v1()
returns jsonb
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  select jsonb_build_object(
    'ok',true,
    'schema_contract_version','IMPORT_REVIEW_DB_V1',
    'apply_envelope_version','IMPORT_REVIEW_APPLY_V1',
    'apply_operation_version','IMPORT_APPLY_OPERATION_V2',
    'correction_operation_version','IMPORT_CORRECTION_OPERATION_V2',
    'follow_up_component_version','IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1',
    'review_ui_contract_version','IMPORT_REVIEW_UI_V3',
    'email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1',
    'legacy_contracts_supported',false
  )
$function$;

create or replace function public.import_review_staged_scope_get_v1(
  p_import_id uuid,
  p_actor_user_id uuid default null,
  p_candidate_page integer default 1,
  p_candidate_page_size integer default 100
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_import public.hr_imports%rowtype;
  v_page integer:=coalesce(p_candidate_page,1);
  v_size integer:=coalesce(p_candidate_page_size,100);
  v_row_count integer;
  v_from date;
  v_to date;
  v_candidates jsonb;
  v_clients jsonb;
  v_candidate_total integer;
  v_overlap jsonb;
  v_authority_mode text:='UNRESOLVED';
  v_authority_summary jsonb:='{}'::jsonb;
  v_authoritative_contract_count integer:=0;
  v_validation_contract_count integer:=0;
  v_route_ineligible_count integer:=0;
  v_unresolved_row_count integer:=0;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_page<1 or v_page>20 or v_size not in (25,50,75,100,500) then
    raise exception 'IMPORT_REVIEW_STAGED_SCOPE_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_import from public.hr_imports where id=p_import_id;
  if not found then raise exception 'IMPORT_REVIEW_IMPORT_NOT_FOUND' using errcode='P0002'; end if;
  if v_import.pruned_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_PRUNED' using errcode='55000'; end if;
  if nullif(btrim(coalesce(v_import.source_file_sha256,'')),'') is null
     or nullif(btrim(coalesce(v_import.parser_version,'')),'') is null then
    raise exception 'IMPORT_REVIEW_STAGING_EVIDENCE_REQUIRED' using errcode='55000';
  end if;

  select count(*),min(r.date_local),max(r.date_local)
    into v_row_count,v_from,v_to
  from public.hr_rows r where r.import_id=p_import_id;
  if v_row_count=0 or v_from is null or v_to is null then
    raise exception 'IMPORT_REVIEW_STAGED_ROWS_REQUIRED' using errcode='55000';
  end if;
  if v_row_count>500 then
    raise exception 'IMPORT_REVIEW_STAGED_SCOPE_ROW_LIMIT_EXCEEDED' using errcode='54000';
  end if;

  with raw as (
    select r.id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,''),'Unlabelled candidate') staff_label,
      coalesce(
        nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),''),
        'unlabelled:'||r.id::text
      ) staff_key
    from public.hr_rows r where r.import_id=p_import_id
  ), distinct_source as (
    select staff_key,min(staff_label) staff_label
    from raw group by staff_key
  ), mapped as (
    select d.*,
      coalesce(alias_match.id,name_map.candidate_id,exact_match.candidate_id) candidate_id
    from distinct_source d
    left join lateral (
      select c.id from public.candidates c
      where c.active and c.nhsp_hr_name_aliases is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[d.staff_key]::text[])
      order by c.id limit 1
    ) alias_match on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(d.staff_label)),d.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) name_map on alias_match.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end candidate_id
      from public.candidates c where c.active and (
        regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=d.staff_key
        or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=d.staff_key)
    ) exact_match on alias_match.id is null and name_map.candidate_id is null
  ), numbered as (
    select m.*,c.first_name,c.last_name,
      row_number() over(order by lower(coalesce(nullif(c.last_name,''),
        case when position(',' in m.staff_label)>0 then split_part(m.staff_label,',',1)
             else regexp_replace(btrim(m.staff_label),'^.*\s+','','') end,'')),
        lower(coalesce(c.first_name,m.staff_label)),m.staff_key) rn,
      count(*) over() total_count
    from mapped m left join public.candidates c on c.id=m.candidate_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'source_candidate_key',staff_key,
      'source_display_label',staff_label,
      'candidate_id',candidate_id,
      'resolved_display_name',nullif(btrim(concat_ws(' ',first_name,last_name)),''),
      'resolved',candidate_id is not null
    ) order by rn),'[]'::jsonb),coalesce(max(total_count),0)
    into v_candidates,v_candidate_total
  from numbered
  where rn>((v_page-1)*v_size) and rn<=v_page*v_size;

  with raw as (
    select r.id,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),
        nullif(r.unit_raw,''),nullif(r.unit_hint,''),nullif(c.name,''),'Unlabelled client') client_label,
      coalesce(
        case when v_import.client_id is not null then 'client:'||v_import.client_id::text end,
        nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),
          nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),''),
        'unlabelled:'||r.id::text
      ) client_key,
      v_import.client_id import_client_id
    from public.hr_rows r left join public.clients c on c.id=v_import.client_id
    where r.import_id=p_import_id
  ), distinct_source as (
    select client_key,min(client_label) client_label,
      (array_agg(import_client_id order by id) filter(where import_client_id is not null))[1] import_client_id
    from raw group by client_key
  ), mapped as (
    select d.*,
      coalesce(d.import_client_id,hospital_match.client_id,exact_match.client_id) client_id
    from distinct_source d
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where d.import_client_id is null and ch.hospital_name_norm @> to_jsonb(array[d.client_key]::text[])
      order by ch.id limit 1
    ) hospital_match on true
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end client_id
      from public.clients c where d.import_client_id is null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=d.client_key
    ) exact_match on hospital_match.client_id is null
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'source_client_key',m.client_key,
      'source_display_label',m.client_label,
      'client_id',m.client_id,
      'resolved_display_name',c.name,
      'resolved',m.client_id is not null
    ) order by lower(coalesce(c.name,m.client_label)),m.client_key),'[]'::jsonb)
    into v_clients
  from mapped m left join public.clients c on c.id=m.client_id;

  if jsonb_array_length(v_clients)>100 then
    raise exception 'IMPORT_REVIEW_STAGED_CLIENT_LIMIT_EXCEEDED' using errcode='54000';
  end if;

  -- Coverage wording is server-owned and uses the same current-setting
  -- authority core as catalogue generation and final application.
  if upper(v_import.source_system::text)='HEALTHROSTER_DAILY'
     or upper(coalesce(v_import.import_scope,'')) like '%DAILY%' then
    select case when a.route_eligible then 'VALIDATION_ONLY' else 'UNRESOLVED' end
      into v_authority_mode
    from public._import_review_effective_authority_core_v1(
      'HR_DAILY',null,v_import.client_id,v_from) a;
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','HR_DAILY',
      'authoritative_contract_count',0,
      'validation_contract_count',case when v_authority_mode='VALIDATION_ONLY' then 1 else 0 end,
      'route_ineligible_count',case when v_authority_mode='UNRESOLVED' then 1 else 0 end,
      'unresolved_row_count',0,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_SETTINGS_DAILY_EXISTING_TIMESHEET_VALIDATION'
    );
  elsif upper(v_import.source_system::text)='HEALTHROSTER'
        and upper(coalesce(v_import.import_scope,'HR_WEEKLY')) not like '%DAILY%' then
    with phase as materialized (
      select * from public.weekly_import_phase2(p_import_id,'HR_WEEKLY')
    ), applicable as (
      select distinct w.contract_id,w.week_ending_date,a.authority_mode,a.authority_fingerprint
      from phase w
      join public.contracts c on c.id=w.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',c.id,c.client_id,w.week_ending_date) a
      where w.contract_id is not null and upper(coalesce(w.action::text,''))='OK'
    )
    select count(*) filter(where authority_mode='AUTHORITATIVE'),
      count(*) filter(where authority_mode='VALIDATION_ONLY'),
      count(*) filter(where authority_mode='OUT_OF_SCOPE'),
      (select count(*) from phase where contract_id is null or upper(coalesce(action::text,''))<>'OK')
    into v_authoritative_contract_count,v_validation_contract_count,v_route_ineligible_count,v_unresolved_row_count
    from applicable;

    if v_route_ineligible_count>0 or v_unresolved_row_count>0 then
      v_authority_mode:='UNRESOLVED';
    elsif v_authoritative_contract_count>0 and v_validation_contract_count>0 then
      v_authority_mode:='MIXED';
    elsif v_authoritative_contract_count>0 then
      v_authority_mode:='AUTHORITATIVE';
    elsif v_validation_contract_count>0 then
      v_authority_mode:='VALIDATION_ONLY';
    else
      v_authority_mode:='UNRESOLVED';
    end if;

    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','HR_WEEKLY',
      'authoritative_contract_count',v_authoritative_contract_count,
      'validation_contract_count',v_validation_contract_count,
      'route_ineligible_count',v_route_ineligible_count,
      'unresolved_row_count',v_unresolved_row_count,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_CLIENT_AND_CONTRACT_SETTINGS'
    );
  elsif upper(v_import.source_system::text)='NHSP' then
    with phase as materialized (
      select * from public.weekly_import_phase2(p_import_id,'NHSP')
    ), applicable as (
      select distinct w.contract_id,w.week_ending_date,a.authority_mode
      from phase w join public.contracts c on c.id=w.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'NHSP',c.id,c.client_id,w.week_ending_date) a
      where w.contract_id is not null and upper(coalesce(w.action::text,''))='OK'
    )
    select count(*) filter(where authority_mode='AUTHORITATIVE'),
      count(*) filter(where authority_mode='OUT_OF_SCOPE'),
      (select count(*) from phase where contract_id is null or upper(coalesce(action::text,''))<>'OK')
    into v_authoritative_contract_count,v_route_ineligible_count,v_unresolved_row_count
    from applicable;
    v_authority_mode:=case when v_route_ineligible_count>0 or v_unresolved_row_count>0
      or v_authoritative_contract_count=0 then 'UNRESOLVED' else 'AUTHORITATIVE' end;
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','NHSP',
      'authoritative_contract_count',v_authoritative_contract_count,
      'validation_contract_count',0,
      'route_ineligible_count',v_route_ineligible_count,
      'unresolved_row_count',v_unresolved_row_count,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_NHSP_SETTINGS'
    );
  else
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route',coalesce(v_import.import_scope,v_import.source_system::text),
      'authoritative_contract_count',0,
      'validation_contract_count',0,
      'basis','UNRESOLVED_SOURCE_ROUTE'
    );
  end if;

  v_overlap:=public._import_review_overlap_preflight_core_v2(
    p_import_id,v_import.source_system,coalesce(v_import.import_scope,v_import.source_system::text),
    v_from,v_to,v_clients);

  return jsonb_build_object(
    'ok',true,
    'import_id',v_import.id,
    'filename',v_import.filename,
    'source_system',v_import.source_system,
    'source_route',coalesce(v_import.import_scope,v_import.source_system::text),
    'source_file_sha256',v_import.source_file_sha256,
    'parser_version',v_import.parser_version,
    'parse_summary',coalesce(v_import.parse_summary_json,'{}'::jsonb),
    'coverage_start_date',v_from,
    'coverage_end_date',v_to,
    'staged_row_count',v_row_count,
    'scope_clients',v_clients,
    'candidate_options',v_candidates,
    'candidate_page',v_page,
    'candidate_page_size',v_size,
    'candidate_total',v_candidate_total,
    'candidate_total_pages',case when v_candidate_total=0 then 0 else ceiling(v_candidate_total::numeric/v_size)::integer end,
    'candidate_has_previous',v_page>1,
    'candidate_has_next',v_page*v_size<v_candidate_total,
    'authority_mode',v_authority_mode,
    'authority_summary',v_authority_summary,
    'review_already_created',v_import.coverage_locked_at is not null,
    'review_status',(select s.status from public.import_review_states s where s.import_id=p_import_id),
    'overlapping_unfinished_reviews',v_overlap
  );
end
$function$;

create or replace function public.import_review_actions_page_v1(
  p_import_id uuid,
  p_actor_user_id uuid default null,
  p_page_number integer default 1,
  p_page_size integer default 25,
  p_sort_by text default 'CANDIDATE',
  p_sort_direction text default 'ASC',
  p_view text default 'ALL'
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_page integer:=coalesce(p_page_number,1);
  v_size integer:=coalesce(p_page_size,25);
  v_sort text:=upper(btrim(coalesce(p_sort_by,'CANDIDATE')));
  v_direction text:=upper(btrim(coalesce(p_sort_direction,'ASC')));
  v_view text:=upper(btrim(coalesce(p_view,'ALL')));
  v_items jsonb;
  v_total integer;
  v_counts jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_page<1 or v_page>100 or v_size not in (25,50,75,100)
     or v_sort not in ('CANDIDATE','CLIENT','WEEK_ENDING','WORK_DATE','ACTION','STATUS')
     or v_direction not in ('ASC','DESC')
     or v_view not in ('ALL','PENDING','READY','EMAIL','NO_ACTION') then
    raise exception 'IMPORT_REVIEW_ACTION_PAGE_INPUT_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.import_review_states s where s.import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;

  with current_actions as (
    select d.*,
      coalesce(nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),nullif(d.summary_json->>'candidate_name',''),'Unknown candidate') candidate_name,
      lower(coalesce(nullif(c.last_name,''),
        case when position(',' in coalesce(d.summary_json->>'candidate_name',''))>0 then split_part(d.summary_json->>'candidate_name',',',1)
             else regexp_replace(btrim(coalesce(d.summary_json->>'candidate_name','')),'^.*\s+','','') end,'')) candidate_surname_sort,
      case when d.candidate_id is not null then 'candidate:'||d.candidate_id::text
        else 'source:'||public._import_review_hash_v1(concat_ws('|',
          regexp_replace(lower(coalesce(d.summary_json->>'candidate_name',d.source_identity,'')),'[^a-z0-9]+','','g'),
          coalesce(d.client_id::text,regexp_replace(lower(coalesce(d.summary_json->>'client_name','')),'[^a-z0-9]+','','g')))) end
        candidate_branch_key,
      coalesce(nullif(cl.name,''),nullif(d.summary_json->>'client_name',''),'Unknown client') client_name,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then lower(btrim(case
        when coalesce(ct.send_ts_queries_to_different_email,false) then ct.ts_queries_alt_email_address
        else cl.ts_queries_email end)) end recipient_email,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then
        case when nullif(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else cl.ts_queries_email end),'') is null
          then 'RECIPIENT_UNAVAILABLE:'||coalesce(d.client_id::text,'UNKNOWN')
          else 'RECIPIENT_EMAIL:'||public._import_review_hash_v1(lower(btrim(case
            when coalesce(ct.send_ts_queries_to_different_email,false) then ct.ts_queries_alt_email_address
            else cl.ts_queries_email end))) end end recipient_group_key,
      case when d.contract_id is null then 'Client default'
        else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract') end contract_label,
      coalesce(timesheet_choices.options,'[]'::jsonb) daily_timesheet_options,
      coalesce(d.summary_json->'imported_evidence',case when hr.id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',hr.date_local,'start',hr.start_time_local,'end',hr.end_time_local,
        'break_minutes',coalesce((hr.payload_json->>'actual_break_mins')::integer,(hr.payload_json->>'actual_break_minutes')::integer,
          (hr.payload_json->>'break_mins')::integer,(hr.payload_json->>'break_minutes')::integer),
        'worked_hours',hr.hours_worked,'worked_minutes',case when hr.hours_worked is null then null else round(hr.hours_worked*60) end,
        'reference',hr.hr_request_id,'role',hr.assignment_grade_norm)) end) imported_evidence,
      coalesce(d.summary_json->'current_evidence',case when ts_ev.timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',(ts_ev.worked_start_iso at time zone 'Europe/London')::date,'start',ts_ev.worked_start_iso,'end',ts_ev.worked_end_iso,
        'break_minutes',ts_ev.break_minutes,'worked_minutes',ts_ev.worked_minutes,'worked_hours',round(ts_ev.worked_minutes/60.0,2),
        'reference',ts_ev.reference_number,'role',ts_ev.tsfin_role,'band',ts_ev.tsfin_band,'timesheet_id',ts_ev.timesheet_id))
        when shift_ev.id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',shift_ev.work_date,'start',shift_ev.start_utc,'end',shift_ev.end_utc,'break_minutes',shift_ev.break_mins,
        'worked_minutes',shift_ev.pay_minutes,'role',shift_ev.assignment_code,'timesheet_id',shift_ev.timesheet_id,'shift_id',shift_ev.id)) end) current_evidence,
      coalesce(d.summary_json->'difference_codes',case when nullif(d.summary_json->>'reason_code','') is not null
        then jsonb_build_array(d.summary_json->>'reason_code') else '[]'::jsonb end) difference_codes,
      coalesce(d.summary_json->'evidence_rows','[]'::jsonb) evidence_rows,
      coalesce(nullif(d.summary_json->>'outcome_label',''),case d.action_kind
        when 'INCLUDE_SHIFT' then 'TMS will add shift' when 'APPLY_AMENDMENT' then 'TMS will amend shift'
        when 'APPLY_CANCELLATION' then 'TMS will cancel shift' when 'MARK_VALIDATION_ERROR' then 'TMS will record validation issue'
        when 'EMAIL_ISSUE' then case when d.summary_json->>'reason_code'='MISSING_FROM_IMPORT'
          then 'Request new shift' else 'Request amend shift' end
        when 'EMAIL_REMINDER' then case when d.summary_json->>'reason_code'='MISSING_FROM_IMPORT'
          then 'Request new shift reminder' else 'Request amend shift reminder' end
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference' when 'NO_ACTION' then 'No action required'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' else 'Resolve before continuing' end) outcome_label,
      d.summary_json->>'resolution_kind' resolution_kind,
      d.summary_json->>'authority_mode' authority_mode,
      coalesce(d.summary_json->'resolution_options','[]'::jsonb) resolution_options,
      coalesce(d.summary_json->'protection','{}'::jsonb) protection,
      d.summary_json->>'default_excluded_reason' default_excluded_reason,
      nullif(d.summary_json->>'week_ending_date','')::date week_ending_date,
      nullif(d.summary_json->>'work_date','')::date work_date
    from public.import_review_decisions d
    left join public.candidates c on c.id=d.candidate_id
    left join public.clients cl on cl.id=d.client_id
    left join public.contracts ct on ct.id=d.contract_id
    left join public.hr_rows hr on hr.id=d.hr_row_id and hr.import_id=d.import_id
    left join public.v_timesheets_daily_match ts_ev on ts_ev.timesheet_id=d.timesheet_id
    left join public.nhsp_shifts shift_ev on shift_ev.id=d.shift_id
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',t.timesheet_id,'worked_start_iso',t.worked_start_iso,'worked_end_iso',t.worked_end_iso,
        'break_minutes',t.break_minutes,'worked_minutes',t.worked_minutes,
        'reference_number',t.reference_number,'processing_status',t.processing_status,
        'role',t.tsfin_role,'band',t.tsfin_band,'site',t.hospital_norm,'contract_id',ts.contract_id,
        'display_label',concat_ws(' · ',to_char((t.worked_start_iso at time zone 'Europe/London')::date,'DD Mon YYYY'),
          to_char(t.worked_start_iso at time zone 'Europe/London','HH24:MI')||'–'||to_char(t.worked_end_iso at time zone 'Europe/London','HH24:MI'),
          round(t.worked_minutes/60.0,2)||' hours',nullif(t.tsfin_role,''),nullif(t.tsfin_band,''),nullif(t.hospital_norm,''),
          case when nullif(t.reference_number,'') is not null then 'ref '||t.reference_number end)
      ) order by t.worked_start_iso,t.timesheet_id),'[]'::jsonb) options
      from public.v_timesheets_daily_match t
      left join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
      where d.action_kind='DAILY_TIMESHEET_RESOLUTION'
        and t.timesheet_id in (
          select value::uuid from jsonb_array_elements_text(coalesce(d.summary_json->'timesheet_options','[]'::jsonb)) value
        )
    ) timesheet_choices on true
    where d.import_id=p_import_id and d.is_current
  ), branch_badge_rows as (
    select a.candidate_branch_key,b.badge_code,label.badge_label,count(*)::integer badge_count
    from current_actions a
    cross join lateral unnest(array_remove(array[
      case a.summary_json->>'reason_code'
        when 'CANDIDATE_UNRESOLVED' then 'CANDIDATE_NOT_LINKED'
        when 'CLIENT_UNRESOLVED' then 'CLIENT_NOT_LINKED'
        when 'GRADE_MAPPING_REQUIRED' then 'GRADE_NOT_MAPPED'
        when 'CONTRACT_MISSING' then 'NO_CONTRACT'
        when 'CONTRACT_AMBIGUOUS' then 'MULTIPLE_CONTRACTS'
        when 'CONTRACT_OUT_OF_SCOPE' then 'CONTRACT_NOT_ELIGIBLE'
        when 'CONTRACT_RATES_INCOMPLETE' then 'RATES_INCOMPLETE'
        when 'TIMESHEET_NOT_FOUND' then 'TIMESHEET_MISSING'
        when 'TIMESHEET_AMBIGUOUS' then 'CHOOSE_TIMESHEET'
        when 'BLOCKED_ACTIVE_PAY_DRAFT' then 'BANKING_PAY_PROTECTED'
        when 'MISSING_FROM_IMPORT' then 'MISSING_FROM_FILE'
        when 'MISSING_FROM_COMPLETE_IMPORT' then 'MISSING_FROM_FILE'
        when 'REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT' then 'REFERENCE_REVIEW'
        when 'REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT' then 'REFERENCE_REVIEW'
        when 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID' then 'EMAIL_NOT_CONFIGURED'
        when 'HEALTHROSTER_WEEKLY' then 'WEEKLY_MISMATCH' end,
      case when a.action_kind='EMAIL_ISSUE' and a.selected then 'EMAIL_REQUEST_SELECTED' end,
      case when a.action_kind='EMAIL_REMINDER' then 'REMINDER_AVAILABLE' end,
      case when a.action_kind='INVALIDATE_REFERENCE' then 'REFERENCE_REVIEW' end,
      case when a.blocking and a.summary_json->>'reason_code' is null then 'NEEDS_RECHECK' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['WORKED_HOURS','ACTUAL_HOURS_MISMATCH'] then 'HOURS_DIFFER' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['START_TIME','END_TIME','START_END_MISMATCH'] then 'TIMES_DIFFER' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['BREAK_MINUTES','BREAK_MINUTES_MISMATCH'] then 'BREAK_DIFFERS' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['NEW_SHIFT','HR_ONLY'] then 'NOT_IN_CLOUDTMS' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['AMBIGUOUS'] then 'MATCH_UNCLEAR' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['REFERENCE'] then 'REFERENCE_ISSUE' end
    ],null)) b(badge_code)
    cross join lateral (select case b.badge_code
      when 'CANDIDATE_NOT_LINKED' then 'Candidate not linked' when 'CLIENT_NOT_LINKED' then 'Client not linked'
      when 'GRADE_NOT_MAPPED' then 'Grade not mapped' when 'NO_CONTRACT' then 'No contract'
      when 'MULTIPLE_CONTRACTS' then 'Multiple contracts' when 'CONTRACT_NOT_ELIGIBLE' then 'Contract not eligible'
      when 'RATES_INCOMPLETE' then 'Rates incomplete' when 'TIMESHEET_MISSING' then 'Timesheet missing'
      when 'CHOOSE_TIMESHEET' then 'Choose timesheet' when 'BANKING_PAY_PROTECTED' then 'Banking Pay protected'
      when 'NEEDS_RECHECK' then 'Needs recheck' when 'HOURS_DIFFER' then 'Hours differ'
      when 'TIMES_DIFFER' then 'Times differ' when 'BREAK_DIFFERS' then 'Break differs'
      when 'MISSING_FROM_FILE' then 'Missing from file' when 'NOT_IN_CLOUDTMS' then 'Not in CloudTMS'
      when 'REFERENCE_ISSUE' then 'Reference issue' when 'MATCH_UNCLEAR' then 'Match unclear'
      when 'EMAIL_NOT_CONFIGURED' then 'Email not configured' when 'WEEKLY_MISMATCH' then 'Weekly mismatch'
      when 'EMAIL_REQUEST_SELECTED' then 'Email request selected' when 'REMINDER_AVAILABLE' then 'Reminder available'
      when 'REFERENCE_REVIEW' then 'Reference review' else b.badge_code end badge_label) label
    group by a.candidate_branch_key,b.badge_code,label.badge_label
  ), branch_badges as (
    select candidate_branch_key,jsonb_agg(jsonb_build_object(
      'code',badge_code,'label',badge_label,'count',badge_count) order by badge_label) badges
    from branch_badge_rows group by candidate_branch_key
  ), filtered as (
    select a.*,coalesce(bb.badges,'[]'::jsonb) branch_badges
    from current_actions a left join branch_badges bb using(candidate_branch_key) where case v_view
      when 'PENDING' then a.blocking or a.action_category in ('PENDING','BLOCKED')
      when 'READY' then a.action_category='READY'
      when 'EMAIL' then a.action_category='EMAIL'
      when 'NO_ACTION' then a.action_category='NO_ACTION'
      else true end
  ), ordered as (
    select f.*,
      row_number() over(order by
        case when v_sort='CANDIDATE' and v_direction='ASC' then candidate_surname_sort end asc nulls last,
        case when v_sort='CANDIDATE' and v_direction='DESC' then candidate_surname_sort end desc nulls last,
        case when v_sort='CANDIDATE' and v_direction='ASC' then lower(candidate_name) end asc nulls last,
        case when v_sort='CANDIDATE' and v_direction='DESC' then lower(candidate_name) end desc nulls last,
        case when v_sort='CLIENT' and v_direction='ASC' then lower(client_name) end asc nulls last,
        case when v_sort='CLIENT' and v_direction='DESC' then lower(client_name) end desc nulls last,
        case when v_sort='WEEK_ENDING' and v_direction='ASC' then week_ending_date end asc nulls last,
        case when v_sort='WEEK_ENDING' and v_direction='DESC' then week_ending_date end desc nulls last,
        case when v_sort='WORK_DATE' and v_direction='ASC' then work_date end asc nulls last,
        case when v_sort='WORK_DATE' and v_direction='DESC' then work_date end desc nulls last,
        case when v_sort='ACTION' and v_direction='ASC' then action_kind end asc,
        case when v_sort='ACTION' and v_direction='DESC' then action_kind end desc,
        case when v_sort='STATUS' and v_direction='ASC' then action_category end asc,
        case when v_sort='STATUS' and v_direction='DESC' then action_category end desc,
        action_id asc) rn,
      count(*) over() total_count
    from filtered f
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'action_id',action_id,'action_kind',action_kind,'action_category',action_category,
      'target_key',target_key,'source_identity',source_identity,
      'hr_row_id',hr_row_id,'timesheet_id',timesheet_id,'shift_id',shift_id,
      'client_id',client_id,'candidate_id',candidate_id,'contract_id',contract_id,'issue_id',issue_id,
      'preview_generation',preview_generation,'evidence_fingerprint',evidence_fingerprint,
      'selectable',selectable,'selected',selected,'blocking',blocking,
      'candidate_name',candidate_name,'candidate_surname_sort',candidate_surname_sort,
      'candidate_branch_key',candidate_branch_key,'branch_badges',branch_badges,
      'client_name',client_name,'week_ending_date',week_ending_date,'work_date',work_date,
      'recipient_email',recipient_email,'recipient_group_key',recipient_group_key,'contract_label',contract_label,
      'daily_timesheet_options',daily_timesheet_options,
      'imported_evidence',imported_evidence,'current_evidence',current_evidence,
      'difference_codes',difference_codes,'evidence_rows',evidence_rows,'outcome_label',outcome_label,
      'resolution_kind',resolution_kind,'authority_mode',authority_mode,'resolution_options',resolution_options,
      'protection',protection,'default_excluded_reason',default_excluded_reason,
      'summary',summary_json
    ) order by rn),'[]'::jsonb),coalesce(max(total_count),0)
    into v_items,v_total
  from ordered where rn>((v_page-1)*v_size) and rn<=v_page*v_size;

  select jsonb_build_object(
    'ALL',count(*),
    'PENDING',count(*) filter(where blocking or action_category in ('PENDING','BLOCKED')),
    'READY',count(*) filter(where action_category='READY'),
    'EMAIL',count(*) filter(where action_category='EMAIL'),
    'NO_ACTION',count(*) filter(where action_category='NO_ACTION')
  ) into v_counts
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;

  return jsonb_build_object(
    'ok',true,'import_id',p_import_id,'view',v_view,'view_counts',v_counts,
    'items',v_items,'total_items',v_total,'page_number',v_page,'page_size',v_size,
    'total_pages',case when v_total=0 then 0 else ceiling(v_total::numeric/v_size)::integer end,
    'has_previous',v_page>1,'has_next',v_page*v_size<v_total,
    'sort_by',v_sort,'sort_direction',v_direction
  );
end
$function$;

revoke all on function public.import_review_contract_version_get_v1() from public,anon,authenticated;
grant execute on function public.import_review_contract_version_get_v1() to service_role;
revoke all on function public.import_review_staged_scope_get_v1(uuid,uuid,integer,integer) from public,anon,authenticated;
grant execute on function public.import_review_staged_scope_get_v1(uuid,uuid,integer,integer) to service_role;
revoke all on function public.import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text) from public,anon,authenticated;
grant execute on function public.import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text) to service_role;
