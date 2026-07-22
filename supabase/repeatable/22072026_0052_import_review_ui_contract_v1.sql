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
    'review_ui_contract_version','IMPORT_REVIEW_UI_V1',
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
    'review_already_created',v_import.coverage_locked_at is not null,
    'review_status',(select s.status from public.import_review_states s where s.import_id=p_import_id)
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
      coalesce(nullif(cl.name,''),nullif(d.summary_json->>'client_name',''),'Unknown client') client_name,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then email_route.route->>'recipient_email' end recipient_email,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then
        'RECIPIENT_EMAIL:'||public._import_review_hash_v1(lower(email_route.route->>'recipient_email')) end recipient_group_key,
      case when d.contract_id is null then 'Client default'
        else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract') end contract_label,
      coalesce(timesheet_choices.options,'[]'::jsonb) daily_timesheet_options,
      nullif(d.summary_json->>'week_ending_date','')::date week_ending_date,
      nullif(d.summary_json->>'work_date','')::date work_date
    from public.import_review_decisions d
    left join public.candidates c on c.id=d.candidate_id
    left join public.clients cl on cl.id=d.client_id
    left join public.contracts ct on ct.id=d.contract_id
    left join lateral (
      select public._timesheet_query_recipient_resolve_core_v1(d.client_id,d.contract_id) route
      where d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    ) email_route on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',t.timesheet_id,'worked_start_iso',t.worked_start_iso,'worked_end_iso',t.worked_end_iso,
        'break_minutes',t.break_minutes,'worked_minutes',t.worked_minutes,
        'reference_number',t.reference_number,'processing_status',t.processing_status
      ) order by t.worked_start_iso,t.timesheet_id),'[]'::jsonb) options
      from public.v_timesheets_daily_match t
      where d.action_kind='DAILY_TIMESHEET_RESOLUTION'
        and t.timesheet_id in (
          select value::uuid from jsonb_array_elements_text(coalesce(d.summary_json->'timesheet_options','[]'::jsonb)) value
        )
    ) timesheet_choices on true
    where d.import_id=p_import_id and d.is_current
  ), filtered as (
    select * from current_actions a where case v_view
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
      'client_name',client_name,'week_ending_date',week_ending_date,'work_date',work_date,
      'recipient_email',recipient_email,'recipient_group_key',recipient_group_key,'contract_label',contract_label,
      'daily_timesheet_options',daily_timesheet_options,
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
