-- Focused replacement authorities for Daily Validation compatibility.
-- Extracted from the exact reviewed definitions so superseded historical
-- repeatable roots remain unchanged.

\set ON_ERROR_STOP on

begin;

create or replace function public._import_review_effective_authority_core_v1(
  p_source_route text,
  p_contract_id uuid,
  p_client_id uuid,
  p_evidence_date date default null
)
returns table (
  route_eligible boolean,
  validation_eligible boolean,
  import_authoritative boolean,
  authority_mode text,
  authority_basis text,
  effective_is_nhsp boolean,
  effective_autoprocess_hr boolean,
  effective_requires_hr boolean,
  effective_no_timesheet_required boolean,
  settings_as_of_date date,
  client_settings_id uuid,
  client_settings_effective_from date,
  client_settings_updated_at timestamptz,
  contract_updated_at timestamptz,
  authority_fingerprint text
)
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  with context as (
    select upper(btrim(coalesce(p_source_route,''))) route,
      (statement_timestamp() at time zone 'Europe/London')::date today_london
  ), contract_row as (
    select c.* from public.contracts c where c.id=p_contract_id
  ), current_setting as (
    select cs.*
    from public.client_settings cs,context x
    where cs.client_id=coalesce((select c.client_id from contract_row c),p_client_id)
      and (cs.effective_from is null or cs.effective_from<=x.today_london)
    order by cs.effective_from desc nulls last,cs.updated_at desc nulls last,cs.id desc
    limit 1
  ), active_daily_override as (
    select bool_or(coalesce(c.autoprocess_hr,false)) route_enabled,
      max(c.updated_at) contract_updated_at
    from public.contracts c,context x
    where p_contract_id is null
      and c.client_id=p_client_id
      and coalesce(c.overrideclientsettings,false)
      and c.start_date<=x.today_london
      and (c.end_date is null or c.end_date>=x.today_london)
  ), effective as (
    select x.route,x.today_london,c.id contract_id,
      coalesce(c.updated_at,ado.contract_updated_at) contract_updated_at,
      cs.id client_settings_id,cs.effective_from client_settings_effective_from,
      cs.updated_at client_settings_updated_at,
      case when coalesce(c.overrideclientsettings,false) and c.is_nhsp is not null then c.is_nhsp
        else coalesce(cs.is_nhsp,false) end is_nhsp,
      case when coalesce(c.overrideclientsettings,false) and c.autoprocess_hr is not null then c.autoprocess_hr
        when c.id is null and x.route in ('HR_DAILY','HEALTHROSTER_DAILY')
          and coalesce(ado.route_enabled,false) then true
        else coalesce(cs.autoprocess_hr,false) end autoprocess_hr,
      case when coalesce(c.overrideclientsettings,false) and c.requires_hr is not null then c.requires_hr
        else coalesce(cs.requires_hr,false) end requires_hr,
      case when coalesce(c.overrideclientsettings,false) and c.no_timesheet_required is not null then c.no_timesheet_required
        else coalesce(cs.no_timesheet_required,false) end no_timesheet_required,
      case when c.id is null and x.route in ('HR_DAILY','HEALTHROSTER_DAILY')
        and coalesce(ado.route_enabled,false) then 'ACTIVE_CONTRACT_OVERRIDE'
        when coalesce(c.overrideclientsettings,false) and (
        c.is_nhsp is not null or c.autoprocess_hr is not null or c.requires_hr is not null
        or c.no_timesheet_required is not null) then 'CONTRACT_OVERRIDE' else 'CLIENT_SETTINGS_CURRENT' end basis
    from context x
    left join contract_row c on true
    left join current_setting cs on true
    left join active_daily_override ado on true
  ), decision as (
    select e.*,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
          then e.autoprocess_hr else false end route_ok,
      case when e.route in ('HR_DAILY','HEALTHROSTER_DAILY') then e.autoprocess_hr
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and not e.no_timesheet_required
        else false end validation_ok,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and e.no_timesheet_required
        else false end authoritative
    from effective e
  )
  select d.route_ok,d.validation_ok,d.authoritative,
    case when not d.route_ok then 'OUT_OF_SCOPE'
      when d.authoritative then 'AUTHORITATIVE'
      when d.validation_ok then 'VALIDATION_ONLY' else 'OUT_OF_SCOPE' end,
    d.basis,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
    d.today_london,d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
    d.contract_updated_at,
    public._import_review_hash_v1(concat_ws('|','import-authority-v1',d.route,p_contract_id,
      coalesce((select c.client_id from contract_row c),p_client_id),p_evidence_date,d.today_london,
      d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
      d.contract_updated_at,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
      d.route_ok,d.validation_ok,d.authoritative,d.basis))
  from decision d
$function$;

revoke all on function public._import_review_effective_authority_core_v1(text,uuid,uuid,date)
  from public,anon,authenticated,service_role;

create or replace function public._import_review_action_catalog_core_v1(
  p_import_id uuid,
  p_preview_generation integer,
  p_max_actions integer default 5000
)
returns table (
  action_id text,
  action_kind text,
  action_category text,
  target_key text,
  source_identity text,
  hr_row_id uuid,
  timesheet_id uuid,
  shift_id uuid,
  client_id uuid,
  candidate_id uuid,
  contract_id uuid,
  issue_id uuid,
  evidence_fingerprint text,
  selectable boolean,
  default_selected boolean,
  blocking boolean,
  summary_json jsonb
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>5000 then
    raise exception 'IMPORT_REVIEW_ACTION_CATALOG_INPUT_INVALID' using errcode='22023';
  end if;

  create temporary table if not exists pg_temp.import_review_catalog_v1 (
    action_id text, action_kind text, action_category text, target_key text, source_identity text,
    hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid,
    contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean,
    default_selected boolean, blocking boolean, summary_json jsonb
  ) on commit drop;
  truncate pg_temp.import_review_catalog_v1;

  if exists (
    select 1 from public.hr_imports hi
    where hi.id=p_import_id
      and upper(coalesce(hi.import_scope,''))='HR_DAILY'
      and coalesce((hi.parse_summary_json->>'declared_zero_shifts')::boolean,false)
  ) then
    select count(distinct t.timesheet_id) into v_count
    from public.hr_imports i
    join public.v_timesheets_daily_match t
      on (t.worked_start_iso at time zone 'Europe/London')::date
        between i.coverage_start_date and i.coverage_end_date
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id
      and ts.is_current and ts.revoked_at is null
    where i.id=p_import_id
      and t.sheet_scope::text='DAILY'
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id));
    if coalesce(v_count,0)>500 then
      raise exception 'DAILY_ZERO_DECLARATION_SCOPE_TOO_LARGE' using errcode='54000',
        detail=jsonb_build_object('supported_maximum',500,'target_count',v_count)::text;
    end if;
  end if;

  insert into pg_temp.import_review_catalog_v1
  with import_row as (
    select hi.* from public.hr_imports hi where hi.id=p_import_id
  ), raw as (
    select r.*, i.source_system::text as source_system, upper(coalesce(i.import_scope,'')) as import_scope,
      i.client_id as import_client_id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,'')) as staff_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),'') as staff_key,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),nullif(r.unit_raw,''),nullif(r.unit_hint,'')) as client_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),'') as client_key,
      lower(btrim(coalesce(nullif(r.assignment_grade_norm,''),r.payload_json->>'grade_raw',r.payload_json->>'Request_Grade',''))) as grade_key,
      coalesce(nullif(r.external_row_key,''),'hr-row:'||r.id::text) as source_row_key
    from public.hr_rows r join import_row i on true where r.import_id=p_import_id
    order by r.id limit 501
  ), mapped as (
    select raw.*,
      coalesce(c_alias.id,c_map.candidate_id,c_exact.candidate_id) as resolved_candidate_id,
      coalesce(raw.import_client_id,ch.client_id,c_client.client_id) as resolved_client_id
    from raw
    left join lateral (
      select c.id from public.candidates c
      where c.nhsp_hr_name_aliases is not null and raw.staff_key is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[raw.staff_key]::text[])
      order by c.id limit 1
    ) c_alias on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(coalesce(raw.staff_label,''))),raw.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) c_map on c_alias.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as candidate_id
      from public.candidates c where c.active and raw.staff_key is not null
        and (regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=raw.staff_key
          or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=raw.staff_key)
    ) c_exact on c_alias.id is null and c_map.candidate_id is null
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where raw.client_key is not null and ch.hospital_name_norm @> to_jsonb(array[raw.client_key]::text[])
      order by ch.id limit 1
    ) ch on raw.import_client_id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as client_id
      from public.clients c where raw.client_key is not null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=raw.client_key
    ) c_client on raw.import_client_id is null and ch.client_id is null
  ), weekly_phase as materialized (
    -- weekly_import_phase2 remains the single authority for assignment-code
    -- mapping precedence and contract choice.  The review catalogue consumes
    -- its answer rather than maintaining a second resolver.
    select w.*
    from import_row i
    cross join lateral public.weekly_import_phase2(
      p_import_id,
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end
    ) w
    where not (upper(i.source_system::text)='HEALTHROSTER_DAILY'
      or upper(coalesce(i.import_scope,'')) like '%DAILY%')
  ), classified as (
    select m.*,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
        then case when rtsx.contract_id is not null then 1 else 0 end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wp.week_ending_date as resolved_week_ending_date,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
      dtsx.submitted_timesheet_count as daily_submitted_timesheet_count,
      dtsx.submitted_timesheet_evidence_hash as daily_submitted_timesheet_evidence_hash,
      tsx.timesheet_contract_ids,dcon.contract_ids as eligible_contract_ids,dcon.contract_evidence_hash,
      cr.route_eligible as contract_route_eligible,cr.rate_complete as contract_rate_complete,
      cr.import_authoritative,cr.authority_mode,cr.authority_fingerprint,
      cr.rate_evidence as contract_rate_evidence,
      wopts.options as weekly_contract_options,dopts.options as daily_role_options,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      nss.start_utc as existing_shift_start_utc,nss.end_utc as existing_shift_end_utc,
      nss.break_mins as existing_shift_break_minutes,nss.pay_minutes as existing_shift_paid_minutes,
      nss.assignment_code as existing_shift_role,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join weekly_phase wp on wp.hr_row_id=m.id
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
    ) con on true
    left join lateral (
      select count(*)::integer mapping_count,
        (array_agg(gm.id order by gm.updated_at desc,gm.id))[1] mapping_id,
        (array_agg(gm.role_code order by gm.updated_at desc,gm.id))[1] role_code,
        (array_agg(gm.band_norm order by gm.updated_at desc,gm.id))[1] band_norm,
        (array_agg(gm.updated_at order by gm.updated_at desc,gm.id))[1] updated_at
      from public.hr_daily_grade_role_mappings gm
      where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active
    ) dgm on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer contract_count,
        case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id,
        array_agg(c.id order by c.id) contract_ids,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',c.id,c.updated_at,c.role,c.band,a.authority_fingerprint),',' order by c.id),'')) contract_evidence_hash
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        and coalesce(dgm.mapping_count,0)=1 and a.route_eligible
        and lower(btrim(coalesce(c.role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(c.band,'')))=lower(btrim(dgm.band_norm)))
    ) dcon on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer submitted_timesheet_count,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,
          t.worked_end_iso,t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,
          t.tsfin_role,t.tsfin_band,ts.contract_id,ts.updated_at),',' order by t.timesheet_id),''))
          submitted_timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) dtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      with candidates as (
        select abm.*,
          case when abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id then 3
            when abm.candidate_id=m.resolved_candidate_id and abm.client_id is null then 2
            when abm.candidate_id is null and abm.client_id=m.resolved_client_id then 1 else 0 end specificity
        from public.assignment_band_mappings abm
        where abm.active and upper(btrim(abm.system_type))=
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end
          and lower(btrim(abm.incoming_code))=m.grade_key
          and ((abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id=m.resolved_candidate_id and abm.client_id is null)
            or (abm.candidate_id is null and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id is null and abm.client_id is null))
      ), chosen as (select * from candidates where specificity=(select max(specificity) from candidates))
      select exists(select 1 from chosen) has_weekly_mapping,
        public._import_review_hash_v1(coalesce((select string_agg(concat_ws('|',id,updated_at,target_contract_id,band_match_pattern),',' order by id)
          from chosen),'')) mapping_evidence
    ) wm on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             array_agg(ts.contract_id order by t.worked_start_iso,t.timesheet_id) timesheet_contract_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id,
             public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,t.worked_end_iso,
               t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,t.tsfin_role,t.tsfin_band,
               ts.contract_id,ts.updated_at),',' order by t.timesheet_id),'')) timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
        and coalesce(dgm.mapping_count,0)=1
        and lower(btrim(coalesce(t.tsfin_role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(t.tsfin_band,'')))=lower(btrim(dgm.band_norm)))
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join lateral (
      select ts.contract_id
      from public.timesheets ts
      where ts.timesheet_id=coalesce(
        case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id)
        and ts.is_current and ts.revoked_at is null
      order by ts.updated_at desc limit 1
    ) rtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select a.route_eligible,a.import_authoritative,a.authority_mode,a.authority_fingerprint,
        (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else
            (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete,
        public._import_review_hash_v1(concat_ws('|',c.id,c.updated_at,c.start_date,c.end_date,c.role,c.band,
          c.pay_method_snapshot,c.rates_json,c.overrideclientsettings,c.is_nhsp,c.autoprocess_hr,c.requires_hr,
          c.no_timesheet_required,a.client_settings_id,a.client_settings_updated_at,
          a.effective_is_nhsp,a.effective_autoprocess_hr,a.effective_requires_hr,
          a.effective_no_timesheet_required,a.authority_fingerprint)) rate_evidence
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1(
        case when upper(m.source_system)='NHSP' then 'NHSP'
          when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then 'HR_DAILY'
          else 'HR_WEEKLY' end,c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
      where c.id=case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        -- Choosing a contract records the server-approved assignment mapping;
        -- it does not apply the import or grant financial authority.  An
        -- authoritative contract with incomplete rates must therefore remain
        -- selectable here and will still be blocked by the refreshed action
        -- catalogue before final application.
        'selectable',coalesce(o.route_eligible,false),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE' end,
        'display_label',concat_ws(' · ',nullif(o.role,''),nullif(o.band,''),nullif(o.display_site,''),
          to_char(o.start_date,'DD Mon YYYY')||' to '||coalesce(to_char(o.end_date,'DD Mon YYYY'),'open ended'))
      ) order by lower(coalesce(o.role,'')),lower(coalesce(o.band,'')),o.start_date desc,o.id),'[]'::jsonb) options
      from (
        select c.*,a.route_eligible,a.import_authoritative,a.authority_mode,
          (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete
        from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1(
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end,
          c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        order by c.start_date desc,c.id limit 25
      ) o
    ) wopts on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','daily-role:'||public._import_review_hash_v1(lower(concat_ws('|',o.role,o.band))),
        'role_code',o.role,'band_norm',o.band,'selectable',true,
        'display_label',concat_ws(' · ',nullif(o.role,''),coalesce(nullif(o.band,''),'No band'))
      ) order by lower(o.role),lower(coalesce(o.band,''))),'[]'::jsonb) options
      from (
        select distinct t.tsfin_role role,t.tsfin_band band
        from public.v_timesheets_daily_match t
        where t.candidate_id=m.resolved_candidate_id
          and t.client_id=m.resolved_client_id
          and t.sheet_scope::text='DAILY'
          and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
          and nullif(btrim(t.tsfin_role),'') is not null
        order by t.tsfin_role,t.tsfin_band
        limit 25
      ) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id) as authoritative_target_timesheet_id,
      public._import_review_timesheet_has_calculated_expenses_core_v1(
        coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
      ) as authoritative_timesheet_has_calculated_expenses,
      mutable_replacement.timesheet_id as mutable_replacement_timesheet_id,
      mutable_replacement.protection as mutable_replacement_protection,
      source_timesheet.authorised_at_server as source_authorised_at_server,
      source_tf.authorised_at_utc as source_tsfin_authorised_at_utc,
      source_tf.policy_snapshot_json as source_policy_snapshot_json,
      source_tf.basis::text as source_tsfin_basis,
      authoritative_hours.hours_day as authoritative_hours_day,
      authoritative_hours.hours_night as authoritative_hours_night,
      authoritative_hours.hours_sat as authoritative_hours_sat,
      authoritative_hours.hours_sun as authoritative_hours_sun,
      authoritative_hours.hours_bh as authoritative_hours_bh,
      authoritative_hours.total_hours as authoritative_total_hours,
      coalesce((auto_authorise.value->>'effective_value')::boolean,false) as effective_auto_authorise,
      public._import_review_timesheet_protection_core_v1(coalesce(
        c.resolved_timesheet_id,c.existing_shift_timesheet_id,base_week.timesheet_id
      )) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
    left join lateral (
      select cw.timesheet_id
      from public.contract_weeks cw
      where not c.is_daily
        and coalesce(c.import_authoritative,false)
        and cw.contract_id=c.resolved_contract_id
        and cw.week_ending_date=coalesce(
          c.resolved_week_ending_date,
          c.date_local + ((7-extract(dow from c.date_local)::integer)%7)
        )
        and cw.is_adjustment=false
        and coalesce(cw.additional_seq,0)=0
      order by cw.id
      limit 1
    ) base_week on true
    left join public.timesheets source_timesheet
      on source_timesheet.timesheet_id=coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
    left join public.timesheets_financials source_tf
      on source_tf.timesheet_id=source_timesheet.timesheet_id and source_tf.is_current=true
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(source_tf.policy_snapshot_json,'{}'::jsonb),
      (c.payload_json->>'start_utc')::timestamptz,
      (c.payload_json->>'end_utc')::timestamptz,
      coalesce((c.payload_json->>'actual_break_mins')::integer,
        (c.payload_json->>'actual_break_minutes')::integer,
        (c.payload_json->>'break_mins')::integer,
        (c.payload_json->>'break_minutes')::integer,0)
    ) authoritative_hours on not c.is_daily and coalesce(c.import_authoritative,false)
      and c.existing_shift_id is not null
    left join lateral (
      select case
        when not c.is_daily
          and coalesce(c.import_authoritative,false)
          and c.resolved_client_id is not null
          and c.resolved_contract_id is not null
        then public.import_auto_authorise_policy_resolve_v2(
          case when upper(c.source_system)='NHSP' then 'NHSP'::public.hr_source_enum else 'HEALTHROSTER'::public.hr_source_enum end,
          c.resolved_client_id,c.resolved_contract_id,source_timesheet.timesheet_id,c.date_local,false
        )
        else null::jsonb
      end as value
    ) auto_authorise on true
    left join lateral (
      select replacement_candidate.timesheet_id,replacement_candidate.protection
      from (
        select
          replacement_timesheet.timesheet_id,
          replacement_timesheet.updated_at,
          replacement_timesheet.created_at,
          public._import_review_timesheet_protection_core_v1(
            replacement_timesheet.timesheet_id
          ) as protection
        from public.timesheets replacement_timesheet
        where not c.is_daily
          and c.existing_shift_id is not null
          and replacement_timesheet.is_adjustment is true
          and replacement_timesheet.is_current is true
          and replacement_timesheet.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and jsonb_typeof(replacement_timesheet.actual_schedule_json)='array'
          and replacement_timesheet.actual_schedule_json @> jsonb_build_array(
            jsonb_build_object(
              'shift_id',c.existing_shift_id::text,
              'external_row_key',c.source_row_key
            )
          )
      ) replacement_candidate
      where coalesce(
          (replacement_candidate.protection->>'paid')::boolean,
          false
        ) is false
        and coalesce(
          (replacement_candidate.protection->>'invoice_locked')::boolean,
          false
        ) is false
      order by
        replacement_candidate.updated_at desc nulls last,
        replacement_candidate.created_at desc nulls last
      limit 1
    ) mutable_replacement on true
  ), reconciliation_source_rows as (
    select
      f.*,
      ((row_number() over (order by f.source_row_key) - 1) / 100)::integer as reconciliation_batch
    from facts f
    where not f.is_daily and coalesce(f.import_authoritative,false) and f.existing_shift_id is not null
  ), reconciliation_inputs as (
    select coalesce(jsonb_agg(jsonb_build_object(
      'source_identity',f.source_row_key,
      'source_system',case when upper(f.source_system)='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      'source_shift_id',f.existing_shift_id,
      'external_row_key',f.source_row_key,
      'hr_row_id',f.id,
      'source_timesheet_id',coalesce(f.existing_shift_timesheet_id,f.authoritative_target_timesheet_id),
      'candidate_id',f.resolved_candidate_id,'client_id',f.resolved_client_id,'contract_id',f.resolved_contract_id,
      'week_ending_date',coalesce(f.resolved_week_ending_date,f.date_local+((7-extract(dow from f.date_local)::integer)%7)),
      'invoice_stream',case when upper(coalesce(f.source_tsfin_basis,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end,
      'authoritative_import_id',p_import_id,
      'authoritative_schedule_json',jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'date',f.date_local,'start_utc',f.payload_json->>'start_utc','end_utc',f.payload_json->>'end_utc',
        'break_mins',coalesce((f.payload_json->>'actual_break_mins')::integer,(f.payload_json->>'actual_break_minutes')::integer,
          (f.payload_json->>'break_mins')::integer,(f.payload_json->>'break_minutes')::integer,0),
        'shift_id',f.existing_shift_id,'external_row_key',f.source_row_key,'import_id',p_import_id,
        'ref_num',coalesce(f.hr_request_id,f.payload_json->>'ref_num',f.payload_json->>'reference_number')
      ))),
      'authoritative_hours',jsonb_build_object(
        'hours_day',coalesce(f.authoritative_hours_day,0),'hours_night',coalesce(f.authoritative_hours_night,0),
        'hours_sat',coalesce(f.authoritative_hours_sat,0),'hours_sun',coalesce(f.authoritative_hours_sun,0),
        'hours_bh',coalesce(f.authoritative_hours_bh,0),'total_hours',coalesce(f.authoritative_total_hours,f.hours_worked,0)
      )
    ) order by f.source_row_key),'[]'::jsonb) items
    from reconciliation_source_rows f
    group by f.reconciliation_batch
  ), reconciliation_balances as materialized (
    select b.source_identity,b.balance_json
    from reconciliation_inputs i
    cross join lateral public._import_review_effective_invoice_balance_core_v1(
      p_import_id,i.items,100,512,256,128
    ) b
  ), evidenced as (
    select c.*,
      rb.balance_json as reconciliation_balance,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,coalesce(rb.balance_json::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
    left join reconciliation_balances rb on rb.source_identity=c.source_row_key
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when not f.is_daily and coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when coalesce(f.authoritative_timesheet_has_calculated_expenses,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when coalesce((f.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
          then 'APPLY_AMENDMENT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or ((f.payload_json->>'break_mins') is not null
            and (f.payload_json->>'break_mins')::integer is distinct from
              coalesce((select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id),0))
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY'
             or nullif(m.reconciliation_balance->>'blocking_code','') is not null
             or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or nullif(m.reconciliation_balance->>'blocking_code','') is not null
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT' then 'CONTRACT_MISSING'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and coalesce(m.weekly_resolution_action,'')<>'OK' then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'DAILY_TIMESHEET_NOT_SUBMITTED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when nullif(m.reconciliation_balance->>'blocking_code','') is not null
            then m.reconciliation_balance->>'blocking_code'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'existing_shift_id',m.existing_shift_id,
        'invoice_stream',m.reconciliation_balance->>'invoice_stream',
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'effective_hours_net_is_positive')::boolean,false)
            and coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,false)
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.protection->>'paid')::boolean,false)
            then 'AMEND_PAID_UNINVOICED_SOURCE'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'reconciliation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'FROZEN_INVOICE_BALANCE'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_SOURCE'
          else null end,
        'mutable_replacement_timesheet_id',coalesce(
          (select x.value::uuid from jsonb_array_elements_text(coalesce(m.reconciliation_balance->'active_mutable_member_ids','[]'::jsonb)) x(value)
            join public.timesheets mutable_ts on mutable_ts.timesheet_id=x.value::uuid and mutable_ts.correction_kind='CHANGED_HOURS_REPLACEMENT' limit 1),
          m.mutable_replacement_timesheet_id),
        'correction_id',m.reconciliation_balance->>'active_mutable_correction_id',
        'reviewed_existing_correction_id',m.reconciliation_balance->>'reviewed_existing_correction_id',
        'repair_identity_mode',m.reconciliation_balance->>'repair_identity_mode',
        'physically_missing_mutable_roles',coalesce(m.reconciliation_balance->'physically_missing_mutable_roles','[]'::jsonb),
        'archived_ignored_roles',coalesce(m.reconciliation_balance->'archived_history_roles','[]'::jsonb),
        'reversal_repair_required',coalesce((m.reconciliation_balance->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((m.reconciliation_balance->>'replacement_repair_required')::boolean,false),
        'correction_generation_required',coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
          and not coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false),
        'standard_representable',coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,true),
        'B_hours',m.reconciliation_balance->'B_hours','B_financials',m.reconciliation_balance->'B_financials',
        'B_standard_schedule_json',m.reconciliation_balance->'B_standard_schedule_json',
        'B_policy_fingerprint',m.reconciliation_balance->>'B_policy_fingerprint',
        'review_policy_basis_kind','IMPORT_AUTHORITATIVE_WEEKLY_V1',
        'review_policy_basis_fingerprint',public._import_review_hash_v1(concat_ws('|','review-policy-basis-v1',
          m.reconciliation_balance->>'source_scope_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
          m.reconciliation_balance->>'role_evidence_fingerprint',m.authority_fingerprint,
          m.reconciliation_balance->>'B_policy_fingerprint',m.reconciliation_balance->>'invoice_stream')),
        'effective_invoice_ids',m.reconciliation_balance->'effective_invoice_ids',
        'effective_invoice_line_ids',m.reconciliation_balance->'effective_invoice_line_ids',
        'M_hours',m.reconciliation_balance->'M_hours','M_existing_financials',m.reconciliation_balance->'M_existing_financials',
        'A_hours',m.reconciliation_balance->'A_hours','A_schedule_json',m.reconciliation_balance->'A_schedule_json',
        'effective_invoice_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
        'mutable_generation_fingerprint',m.reconciliation_balance->>'active_mutable_fingerprint',
        'authoritative_evidence_fingerprint',m.reconciliation_balance->>'A_evidence_fingerprint',
        'reconciliation_fingerprint',m.reconciliation_balance->>'reconciliation_fingerprint',
        'source_scope_fingerprint',m.reconciliation_balance->>'source_scope_fingerprint'
      ) || jsonb_build_object(
        'archived_timesheet_ids',m.reconciliation_balance->'archived_timesheet_ids',
        'archived_history_timesheet_ids',m.reconciliation_balance->'archived_history_timesheet_ids',
        'archived_history_roles',m.reconciliation_balance->'archived_history_roles',
        'historical_missing_timesheet_ids',m.reconciliation_balance->'historical_missing_timesheet_ids',
        'active_mutable_member_ids',m.reconciliation_balance->'active_mutable_member_ids',
        'missing_mutable_roles',m.reconciliation_balance->'active_mutable_missing_roles',
        'active_mutable_parent_timesheet_id',m.reconciliation_balance->>'active_mutable_parent_timesheet_id',
        'pre_apply_authorised',m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null,
        'effective_auto_authorise',m.effective_auto_authorise,
        'intended_authorisation_action',case
          when m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null then 'REAUTHORISE'
          when m.effective_auto_authorise then 'AUTHORISE' else 'LEAVE_UNAUTHORISED' end,
        'financial_validation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_TSFIN_DEFINES_A' end,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
          (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.start_time_local,'end',m.end_time_local,
          'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
            (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
          'worked_hours',m.hours_worked,'worked_minutes',case when m.hours_worked is null then null else round(m.hours_worked*60) end,
          'reference',m.hr_request_id,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),'grade',m.grade_key)),
        'current_evidence',case when m.is_daily and m.resolved_timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'start',m.worked_start_iso,'end',m.worked_end_iso,
          'break_minutes',m.ts_break_minutes,'elapsed_minutes',m.worked_minutes,
          'worked_minutes',greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0),
          'worked_hours',round(greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0)/60.0,2),
          'reference',m.reference_number,'role',m.tsfin_role,'band',m.tsfin_band,'timesheet_id',m.resolved_timesheet_id))
          when not m.is_daily and m.existing_shift_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.existing_shift_start_utc,'end',m.existing_shift_end_utc,
          'break_minutes',m.existing_shift_break_minutes,'worked_minutes',m.existing_shift_paid_minutes,
          'role',m.existing_shift_role,'timesheet_id',m.existing_shift_timesheet_id,'shift_id',m.existing_shift_id)) end,
        'difference_codes',to_jsonb(array_remove(array[
          case when m.existing_shift_id is null and not m.is_daily then 'NEW_SHIFT'::text end,
          case when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_SELECTION_REQUIRED'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.start_time_local is distinct from
            (m.worked_start_iso at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.end_time_local is distinct from
            (m.worked_end_iso at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.ts_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0))>1
            then 'WORKED_HOURS'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.start_time_local is distinct from
            (m.existing_shift_start_utc at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.end_time_local is distinct from
            (m.existing_shift_end_utc at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.existing_shift_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end,
          case when not m.is_daily and coalesce((m.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
            then 'FINANCIAL_POSITION'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate candidate timesheet'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'Request timesheet from candidate'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'Candidate timesheet states they did not work this shift'
          when not m.is_daily and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'Timesheet occupied by expenses'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'TMS will repair current correction generation'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'TMS will create correction generation'
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.protection->>'paid')::boolean,false)
            then 'TMS will amend paid uninvoiced shift'
          when m.action_kind='APPLY_AMENDMENT' then 'TMS will amend shift'
          when m.action_kind='APPLY_CANCELLATION' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift' else 'TMS will cancel shift' end
          when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' when m.action_kind='NO_ACTION' then 'No action required'
          else 'Resolve before continuing' end,
        'resolution_kind',case
          when m.resolved_candidate_id is null then 'CANDIDATE_LINK'
          when m.resolved_client_id is null then 'CLIENT_LINK'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'DAILY_GRADE_ROLE'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'WEEKLY_ASSIGNMENT_CONTRACT'
          when m.is_daily and m.resolved_timesheet_id is null and coalesce(m.timesheet_count,0)>0 then 'DAILY_EXISTING_TIMESHEET' end,
        'resolution_options',case
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then m.daily_role_options
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH' then m.weekly_contract_options
          else '[]'::jsonb end,
        'mapping_evidence',case when m.is_daily then jsonb_strip_nulls(jsonb_build_object(
          'mapping_id',m.daily_mapping_id,'updated_at',m.daily_mapping_updated_at,'role',m.daily_mapped_role,'band',m.daily_mapped_band))
          else jsonb_strip_nulls(jsonb_build_object('mapping_fingerprint',m.weekly_mapping_evidence,
            'resolution_action',m.weekly_resolution_action,'resolution_reason',m.weekly_resolution_reason)) end,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
        'occupied_timesheet_id',case when coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
          then m.authoritative_target_timesheet_id end,
        'protection',m.protection
      )) summary_json
    from main_actions m
  )
  select * from rendered;

  -- Daily mismatch/query actions are independent of the evidence association.
  insert into pg_temp.import_review_catalog_v1
  with r as (
    select h.*,d.resolved_timesheet_id as timesheet_id,t.candidate_id,t.client_id,t.worked_start_iso,t.worked_end_iso,
      t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status::text,
      c.id contract_id,public._import_review_timesheet_protection_core_v1(d.resolved_timesheet_id) protection
    from public.hr_rows h
    join public.hr_imports i on i.id=h.import_id
    join public.import_review_daily_timesheet_resolutions d on d.import_id=h.import_id and d.hr_row_id=h.id and d.status in ('CURRENT','APPLIED')
    join public.v_timesheets_daily_match t on t.timesheet_id=d.resolved_timesheet_id
    left join public.contracts c on c.id=(select ts.contract_id from public.timesheets ts where ts.timesheet_id=t.timesheet_id)
    where h.import_id=p_import_id and (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
    order by h.id limit 501
  ), mismatch as (
    select r.*,
      case
        when r.hours_worked is not null and r.worked_minutes is not null
          and abs((r.hours_worked*60)-greatest(r.worked_minutes-coalesce(r.break_minutes,0),0))>1
          then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_evidence_supplied')::boolean,false)
          and (r.payload_json->>'break_mins')::integer is distinct from coalesce(r.break_minutes,0)
          then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
      jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'comparison_key','hr-row:'||m.id::text,
        'work_date',m.date_local,
        'match_status',m.reason_code,
        'timesheet_start',to_char(m.worked_start_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_end',to_char(m.worked_end_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_break_mins',m.break_minutes,
        'healthroster_start',to_char(m.start_time_local,'HH24:MI'),
        'healthroster_end',to_char(m.end_time_local,'HH24:MI'),
        'healthroster_break_mins',case
          when coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            then nullif(m.payload_json->>'break_mins','')::integer end,
        'healthroster_unit',coalesce(nullif(m.payload_json->>'Unit',''),nullif(m.payload_json->>'unit',''),
          nullif(m.unit_raw,''),nullif(m.unit_hint,'')),
        'healthroster_hospital',coalesce(nullif(m.payload_json->>'hospital_or_trust',''),
          nullif(m.payload_json->>'trust','')),
        'healthroster_request_grade',coalesce(nullif(m.payload_json->>'Request Grade',''),
          nullif(m.payload_json->>'Request_Grade',''),nullif(m.payload_json->>'grade_raw',''),
          nullif(m.assignment_grade_norm,'')),
        'ref_before',m.reference_number,
        'ref_after',m.hr_request_id
      ))) email_comparisons,
      lower(btrim(case when coalesce(m.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=m.contract_id),false)
        then (select c.ts_queries_alt_email_address from public.contracts c where c.id=m.contract_id)
        else (select c.ts_queries_email from public.clients c where c.id=m.client_id) end)) route_email
    from mismatch m where m.reason_code is not null
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,i.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||i.issue_fingerprint,i.issue_fingerprint,i.id,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v2',i.issue_fingerprint,i.email_comparisons::text,i.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select concat_ws('|',c.updated_at,c.ts_queries_alt_email_address) from public.contracts c where c.id=i.contract_id)
        else (select concat_ws('|',c.rev,c.updated_at,c.ts_queries_email) from public.clients c where c.id=i.client_id) end)),
    not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    e.id is null and not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    false,
    jsonb_build_object('reason_code',i.reason_code,'issue_fingerprint',i.issue_fingerprint,'work_date',i.date_local,
      'candidate_name',i.staff_raw,'timesheet_id',i.timesheet_id,'recipient_scope_key',
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||i.contract_id::text else 'CLIENT_DEFAULT:'||i.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(i.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_alt_email_address,''))),c.updated_at)) from public.contracts c where c.id=i.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_email,''))),c.rev,c.updated_at)) from public.clients c where c.id=i.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'comparisons',i.email_comparisons,
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(i.route_email,'')) not between 3 and 320 or position('@' in coalesce(i.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((i.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',i.protection)
  from issues i left join public.hr_issue_emails e on e.issue_fingerprint=i.issue_fingerprint;

  -- Weekly validation-only issues use the installed comparison engine, but
  -- normalise every user choice into the same server-owned decision catalogue.
  if exists(select 1 from public.hr_imports i where i.id=p_import_id
      and i.source_system='HEALTHROSTER'::public.hr_source_enum
      and upper(coalesce(i.import_scope,'HR_WEEKLY')) not like '%DAILY%') then
    v_weekly_preview:=public.hr_weekly_validation_preview(p_import_id);

    -- Validation-only Weekly evidence has two distinct, server-proven states.
    -- Neither state is an instruction to mutate CloudTMS financial records.
    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), eligible_validation_groups as (
      select d.candidate_id,d.summary_json->>'week_ending_date' week_ending_date
      from pg_temp.import_review_catalog_v1 d
      where d.candidate_id is not null
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      group by d.candidate_id,d.summary_json->>'week_ending_date'
      having bool_and(d.action_kind='NO_ACTION' and not d.blocking)
    ), missing_timesheets as (
      select p.row_json,p.timesheet_id,p.candidate_id,
        d.hr_row_id shift_hr_row_id,d.client_id shift_client_id,
        d.contract_id shift_contract_id,d.source_identity shift_source_identity,
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), omitted_shifts as (
      select p.*,cx.value comparison_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null and cx.value->>'match_status'='HR_ONLY'
    ), confirmed_exceptions as (
      select p.*,cx.value exception_json
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_TIMESHEET_NOT_SUBMITTED',m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-not-submitted',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-not-submitted-v2',
        m.shift_evidence_fingerprint,m.row_json::text)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code','WEEKLY_TIMESHEET_NOT_SUBMITTED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'difference_codes',jsonb_build_array('TIMESHEET_NOT_SUBMITTED'),
        'outcome_label','Request timesheet from candidate'))
    from missing_timesheets m
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',o.comparison_json->>'hr_row_id')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-candidate-did-not-work',o.comparison_json->>'hr_row_id'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      nullif(o.comparison_json->>'hr_row_id','')::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      o.comparison_json->>'exception_evidence_fingerprint',
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Confirm candidate did not work this shift')
    from omitted_shifts o
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',c.exception_json->>'hr_row_id')),
      'NO_ACTION','NO_ACTION',
      concat_ws(':','weekly-candidate-did-not-work',c.exception_json->>'hr_row_id'),
      concat_ws('|',c.timesheet_id,c.exception_json->>'work_date',
        c.exception_json->>'healthroster_start',c.exception_json->>'healthroster_end'),
      nullif(c.exception_json->>'hr_row_id','')::uuid,c.timesheet_id,null::uuid,c.client_id,c.candidate_id,c.contract_id,null::uuid,
      c.exception_json->>'evidence_fingerprint',
      false,false,false,
      jsonb_build_object(
        'reason_code','CANDIDATE_DID_NOT_WORK_CONFIRMED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',c.row_json->>'candidate_name','week_ending_date',c.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(c.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'confirmed_exception_count',coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'work_date',c.exception_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',c.exception_json->>'work_date','start',c.exception_json->>'healthroster_start',
          'end',c.exception_json->>'healthroster_end',
          'break_minutes',nullif(c.exception_json->>'healthroster_break_mins','')::integer,
          'reference',c.exception_json->>'reference')),
        'current_evidence',jsonb_build_object('timesheet_id',c.timesheet_id),
        'difference_codes',jsonb_build_array('CONFIRMED_EXCEPTION'),
        'outcome_label','Passed with confirmed exception')
    from confirmed_exceptions c;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id,
        nullif(r.value->>'issue_fingerprint','') issue_fingerprint
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), email_filtered as (
      select p.*,
        coalesce((select jsonb_agg(cx.value||jsonb_strip_nulls(jsonb_build_object(
            'healthroster_unit',coalesce(nullif(hr.payload_json->>'Unit',''),nullif(hr.payload_json->>'unit',''),
              nullif(hr.unit_raw,''),nullif(hr.unit_hint,''),nullif(cx.value->>'location_after','')),
            'healthroster_hospital',coalesce(nullif(hr.payload_json->>'hospital_or_trust',''),
              nullif(hr.payload_json->>'trust','')),
            'healthroster_request_grade',coalesce(nullif(hr.payload_json->>'Request Grade',''),
              nullif(hr.payload_json->>'Request_Grade',''),nullif(hr.payload_json->>'grade_raw',''),
              nullif(hr.assignment_grade_norm,''))
          )) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          left join public.hr_rows hr on hr.id=nullif(cx.value->>'hr_row_id','')::uuid
          where (
            coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)
          where fr.value<>'HealthRoster has a shift not present on the timesheet.'),'[]'::jsonb) email_failure_reasons
      from preview_rows p
    ), routed as (
      select p.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_WEEKLY','validation-email-v2',
          p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text)) email_issue_fingerprint,
        c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from email_filtered p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=public._import_review_hash_v1(concat_ws('|',
        'HEALTHROSTER_WEEKLY','validation-email-v2',p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text))
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
        and exists (
          select 1 from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
        )
    ), email_actions as (
      select r.*,
        public._import_review_hash_v1(concat_ws('|','query-route-v1',r.recipient_scope_key,r.recipient_email,
          case when r.recipient_scope='CONTRACT_OVERRIDE' then r.contract_updated_at::text
            else concat_ws('|',r.client_rev,r.client_updated_at) end)) route_fingerprint,
        length(coalesce(r.recipient_email,'')) between 3 and 320
          and r.recipient_email~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' valid_email
      from routed r
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.email_issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.email_issue_fingerprint,a.email_issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v2',a.timesheet_id,
        a.row_json->>'candidate_name',a.row_json->>'week_ending_date',
        a.email_comparisons::text,a.email_days::text,a.email_failure_reasons::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.email_issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(a.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(a.row_json->>'confirmed_exception_count','')::integer,0),
        'validation_difference_count',jsonb_array_length(a.email_comparisons),
      'failure_reasons',a.email_failure_reasons,
        'days',a.email_days,'comparisons',a.email_comparisons,
        'evidence_rows',coalesce((
          select jsonb_agg(jsonb_build_object(
            'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'healthroster_start',
              'end',cx.value->>'healthroster_end','break_minutes',nullif(cx.value->>'healthroster_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'hr_minutes','')::integer,'reference',cx.value->>'ref_after')),
            'current_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'timesheet_start',
              'end',cx.value->>'timesheet_end','break_minutes',nullif(cx.value->>'timesheet_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'ts_minutes','')::integer,'reference',cx.value->>'ref_before')),
            'difference_codes',to_jsonb(array_remove(array[
              case when coalesce(cx.value->>'match_status','MATCH')<>'MATCH' then cx.value->>'match_status' end,
              case when coalesce((cx.value->>'ref_changed')::boolean,false) then 'REFERENCE' end,
              case when coalesce(day_json.value->>'day_status','OK')<>'OK' then 'WORKED_HOURS' end
            ],null))
          ) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(a.email_comparisons) cx(value)
          left join lateral (select d.value from jsonb_array_elements(coalesce(a.row_json->'days','[]'::jsonb)) d(value)
            where d.value->>'date'=cx.value->>'work_date' limit 1) day_json on true
        ),'[]'::jsonb),
        'outcome_label',case when a.issue_id is null then 'Request amend shift' else 'Request amend shift reminder' end,
        'recipient_scope_key',a.recipient_scope_key,'recipient_route_fingerprint',a.route_fingerprint,
        'delivery_history_status',coalesce(a.delivery_history_status,'NEW'),'sent_count',coalesce(a.sent_count,0),
        'default_excluded_reason',case when a.issue_id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
          when not a.valid_email then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
          when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
        'protection',a.protection)
    from email_actions a;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
      where nullif(r.value->>'timesheet_id','') is not null
    ), invalidations as (
      select p.*,cx.value comparison_json,nullif(btrim(cx.value->>'comparison_key'),'') comparison_key,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where coalesce((cx.value->>'is_destructive_invalidation')::boolean,false)
        and exists(select 1 from public.hr_imports hi where hi.id=p_import_id
          and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'))
        and nullif(btrim(cx.value->>'comparison_key'),'') is not null
        and nullif(btrim(cx.value->>'ref_before'),'') is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',i.timesheet_id,i.comparison_key)),
      'INVALIDATE_REFERENCE','PENDING','timesheet:'||i.timesheet_id::text||':'||i.comparison_key,
      i.comparison_key,null::uuid,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-reference-invalidation-v1',i.timesheet_id,i.comparison_json::text,i.protection::text)),
      not coalesce((i.protection->>'protected')::boolean,false),false,false,
      jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT',
        'candidate_name',i.row_json->>'candidate_name','week_ending_date',i.row_json->>'week_ending_date',
        'timesheet_id',i.timesheet_id,'comparison_key',i.comparison_key,'comparison',i.comparison_json,
        'protection',i.protection,'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
    from invalidations i;
  end if;

  -- Complete Daily coverage also exposes existing timesheets that are absent
  -- from the file.  Missing rows are query-email candidates; reference
  -- invalidation is a separate, explicit, default-off decision.
  insert into pg_temp.import_review_catalog_v1
  with i as (
    select * from public.hr_imports where id=p_import_id
  ), missing as (
    select t.*,ts.contract_id,c.first_name,c.last_name,cl.name as client_name,
      public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection,
      lower(btrim(case when coalesce(ts.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=ts.contract_id),false)
        then (select ct.ts_queries_alt_email_address from public.contracts ct where ct.id=ts.contract_id)
        else cl.ts_queries_email end)) route_email,
      public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY','MISSING_FROM_IMPORT',
        t.timesheet_id,t.candidate_id,t.client_id,(t.worked_start_iso at time zone 'Europe/London')::date,
        coalesce(t.reference_number,''))) issue_fingerprint
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
    join public.candidates c on c.id=t.candidate_id
    join public.clients cl on cl.id=t.client_id
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (
        select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED')
      )
    order by t.timesheet_id limit 501
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,m.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||m.issue_fingerprint,m.issue_fingerprint,null::uuid,m.timesheet_id,null::uuid,
    m.client_id,m.candidate_id,m.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','missing-daily-email-v1',m.issue_fingerprint,m.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(m.contract_id is not null and (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select concat_ws('|',ct.updated_at,ct.ts_queries_alt_email_address) from public.contracts ct where ct.id=m.contract_id)
        else (select concat_ws('|',cl.rev,cl.updated_at,cl.ts_queries_email) from public.clients cl where cl.id=m.client_id) end)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,
    e.id is null and not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,false,
    jsonb_build_object('reason_code','MISSING_FROM_IMPORT','issue_fingerprint',m.issue_fingerprint,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,
      'week_ending_date',(m.worked_start_iso at time zone 'Europe/London')::date
        + ((7-extract(dow from (m.worked_start_iso at time zone 'Europe/London')::date)::integer)%7),
      'candidate_name',btrim(concat_ws(' ',m.first_name,m.last_name)),'client_name',m.client_name,
      'timesheet_id',m.timesheet_id,'reference_number',m.reference_number,
      'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,
      'break_minutes',m.break_minutes,'role',m.tsfin_role,
      'recipient_scope_key',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||m.contract_id::text else 'CLIENT_DEFAULT:'||m.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||ct.id::text,
          lower(btrim(coalesce(ct.ts_queries_alt_email_address,''))),ct.updated_at)) from public.contracts ct where ct.id=m.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||cl.id::text,
          lower(btrim(coalesce(cl.ts_queries_email,''))),cl.rev,cl.updated_at)) from public.clients cl where cl.id=m.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(m.route_email,'')) not between 3 and 320 or position('@' in coalesce(m.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',m.protection)
  from missing m left join public.hr_issue_emails e on e.issue_fingerprint=m.issue_fingerprint;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists(select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'MARK_VALIDATION_ERROR',m.timesheet_id)),
    'MARK_VALIDATION_ERROR','READY','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-validation-v1',m.timesheet_id,m.worked_start_iso,
      m.worked_end_iso,m.break_minutes,m.worked_minutes,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false)
      then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_IMPORT' end,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,'break_minutes',m.break_minutes,
      'hours_worked',m.worked_minutes/60.0,'role',m.tsfin_role,'protection',m.protection)
  from missing m;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and nullif(btrim(t.reference_number),'') is not null
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',m.timesheet_id)),
    'INVALIDATE_REFERENCE','PENDING','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-reference-v1',m.timesheet_id,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'protected')::boolean,false),false,false,
    jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT',
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'protection',m.protection,
      'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
  from missing m;

  -- Omitted existing shifts are proposed only inside immutable complete coverage.
  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select s.*,public._import_review_timesheet_protection_core_v1(s.timesheet_id) protection
    from public.nhsp_shifts s
    join i on true
    cross join lateral public._import_review_effective_authority_core_v1(
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
      s.contract_id,s.client_id,coalesce(s.week_ending_date,s.work_date)) authority
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and authority.import_authoritative
      and s.cancelled_at_utc is null
      and s.work_date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or s.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=s.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=s.candidate_id))
      and not exists (select 1 from public.hr_rows h where h.import_id=i.id and h.external_row_key=s.external_row_key)
    order by s.id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'APPLY_CANCELLATION',m.id)),
    'APPLY_CANCELLATION','READY','shift:'||m.id::text,m.external_row_key,null::uuid,m.timesheet_id,m.id,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-shift-v1',m.id,m.updated_at,m.timesheet_id,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_COMPLETE_IMPORT' end,
      'work_date',m.work_date,'week_ending_date',m.week_ending_date,'candidate_id',m.candidate_id,'client_id',m.client_id,
      'start_time',m.start_utc,'end_time',m.end_utc,'break_minutes',m.break_mins,'role',m.assignment_code,'protection',m.protection)
  from missing m;

  -- Query emails can be committed only with one exact, complete, current
  -- timesheet PDF.  Invoice-linked validation records are never eligible for
  -- validation.  The same evidence fingerprint is frozen into the decision so
  -- document or invoice lifecycle movement makes a reviewed action stale.
  with evidence as materialized (
    select c.action_id,public._import_review_query_evidence_core_v1(c.timesheet_id) evidence_json
    from pg_temp.import_review_catalog_v1 c
    where c.timesheet_id is not null
      and (
        c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
        or c.summary_json->>'authority_mode'='VALIDATION_ONLY'
        or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
      )
  )
  update pg_temp.import_review_catalog_v1 c
  set evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','query-evidence-decision-v1',
        c.evidence_fingerprint,e.evidence_json->>'evidence_fingerprint')),
      action_category=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then 'PENDING'
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then 'PENDING'
        else c.action_category end,
      selectable=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.selectable end,
      default_selected=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.default_selected end,
      blocking=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then true
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then true
        else c.blocking end,
      summary_json=c.summary_json||jsonb_build_object(
        'attachment_evidence',e.evidence_json,
        'attachment_fingerprint',e.evidence_json->>'evidence_fingerprint'
      )||case
        when (
          c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null
        ) or (
          (
            c.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
          ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
        ) then jsonb_build_object(
          'reason_code',e.evidence_json->>'reason_code',
          'default_excluded_reason',e.evidence_json->>'reason_code',
          'outcome_label',case e.evidence_json->>'reason_code'
            when 'TIMESHEET_PRESENT_BUT_INVOICED' then 'Timesheet present but invoiced'
            when 'TIMESHEET_EVIDENCE_PREPARING' then 'Preparing timesheet evidence'
            else 'Timesheet evidence incomplete' end
        ) else '{}'::jsonb end
  from evidence e
  where e.action_id=c.action_id;

  -- An invoice-linked validation shift owns this blocker.  Do not also expose
  -- the aggregate query-email action as a second visual copy of the same
  -- shift.  Preserve any delivery history on the owning shift before removing
  -- only that redundant email row; genuine query emails remain untouched.
  update pg_temp.import_review_catalog_v1 owner_row
  set summary_json=owner_row.summary_json||jsonb_strip_nulls(jsonb_build_object(
        'delivery_history_status',email_row.summary_json->>'delivery_history_status',
        'sent_count',nullif(email_row.summary_json->>'sent_count','')::integer,
        'previously_queried',coalesce(nullif(email_row.summary_json->>'sent_count','')::integer,0)>0
      ))
  from pg_temp.import_review_catalog_v1 email_row
  where owner_row.timesheet_id=email_row.timesheet_id
    and owner_row.action_id<>email_row.action_id
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  delete from pg_temp.import_review_catalog_v1 email_row
  using pg_temp.import_review_catalog_v1 owner_row
  where email_row.timesheet_id=owner_row.timesheet_id
    and email_row.action_id<>owner_row.action_id
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  -- Daily validation is atomic per Daily timesheet.  An email, document hold
  -- or invoice blocker for that record prevents only that record from entering
  -- validation and TSFIN work.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','daily-validation-held-v1',
        current_row.evidence_fingerprint,current_row.timesheet_id)),
      summary_json=current_row.summary_json||jsonb_build_object(
        'daily_validation_held',true,
        'validation_hold_label','Validation held: resolve this Daily timesheet first'
      )
  where current_row.action_kind='NO_ACTION'
    and coalesce(nullif(current_row.summary_json->>'is_daily','')::boolean,false)
    and current_row.timesheet_id is not null
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.timesheet_id=current_row.timesheet_id
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  -- Weekly validation is all-or-nothing per candidate/client/contract/week.
  -- One mismatch, unresolved exception, missing attachment or invoice blocker
  -- holds the whole Weekly timesheet while leaving the actual issue visible.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','weekly-validation-held-v2',
        current_row.evidence_fingerprint,current_row.candidate_id,current_row.client_id,
        current_row.contract_id,current_row.summary_json->>'week_ending_date')),
      summary_json=current_row.summary_json||jsonb_build_object(
        'weekly_validation_held',true,
        'weekly_validation_badge_code','WEEKLY_VALIDATION_HELD',
        'weekly_validation_badge_label','Validation held: resolve outstanding shift',
        'validation_hold_label','Validation held: one or more shifts still require action'
      )
  where current_row.action_kind='NO_ACTION'
    and current_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and current_row.summary_json->>'source_route' not like '%DAILY%'
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.candidate_id=current_row.candidate_id
        and hold.client_id=current_row.client_id
        and hold.contract_id is not distinct from current_row.contract_id
        and hold.summary_json->>'week_ending_date'=current_row.summary_json->>'week_ending_date'
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  select count(*) into v_count from pg_temp.import_review_catalog_v1;
  if v_count>p_max_actions then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count',v_count,'max',p_max_actions)::text;
  end if;

  return query select c.action_id,c.action_kind,c.action_category,c.target_key,c.source_identity,
    c.hr_row_id,c.timesheet_id,c.shift_id,c.client_id,c.candidate_id,c.contract_id,c.issue_id,
    c.evidence_fingerprint,c.selectable,c.default_selected,c.blocking,c.summary_json
  from pg_temp.import_review_catalog_v1 c order by c.action_id;
end
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
  v_declared_zero_shifts boolean:=false;
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
  v_declared_zero_shifts:=upper(coalesce(v_import.import_scope,''))='HR_DAILY'
    and coalesce((v_import.parse_summary_json->>'declared_zero_shifts')::boolean,false);

  select count(*),min(r.date_local),max(r.date_local)
    into v_row_count,v_from,v_to
  from public.hr_rows r where r.import_id=p_import_id;
  if v_declared_zero_shifts then
    if v_row_count<>0 or v_import.file_r2_key is not null
       or v_import.coverage_start_date is null or v_import.coverage_end_date is null
       or v_import.coverage_start_date>v_import.coverage_end_date then
      raise exception 'DAILY_ZERO_DECLARATION_INVALID' using errcode='55000';
    end if;
    v_from:=v_import.coverage_start_date;
    v_to:=v_import.coverage_end_date;
  elsif v_row_count=0 or v_from is null or v_to is null then
    raise exception 'IMPORT_REVIEW_STAGED_ROWS_REQUIRED' using errcode='55000';
  end if;
  if v_row_count>5000 then
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

  if v_declared_zero_shifts then
    select jsonb_build_array(jsonb_build_object(
      'source_client_key','client:'||c.id::text,
      'source_display_label',c.name,
      'client_id',c.id,
      'resolved_display_name',c.name,
      'resolved',true
    )) into v_clients
    from public.clients c where c.id=v_import.client_id;
    if v_clients is null then
      raise exception 'DAILY_ZERO_DECLARATION_CLIENT_NOT_FOUND' using errcode='P0002';
    end if;
  end if;

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

create or replace function public.contracts_clone_and_extend_atomic(
  p_contract_id uuid,
  p_new_start_date date,
  p_new_end_date date,
  p_end_existing_on date,
  p_assign_existing_candidate boolean,
  p_new_candidate_id uuid default null,
  p_split_worker_note text default null,
  p_successor_overrides jsonb default null,
  p_force_schedule_clashes boolean default false,
  p_force_already_split_week boolean default false,
  p_confirmed_split_week boolean default false,
  p_actor_user_id uuid default null
) returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now                 timestamptz := now();

  v_cur                 public.contracts%rowtype;
  v_succ                public.contracts%rowtype;

  v_ov                  jsonb := coalesce(p_successor_overrides, '{}'::jsonb);

  v_close_to            date := p_end_existing_on;
  v_new_start           date := p_new_start_date;
  v_new_end             date := p_new_end_date;

  v_wew_pred            int;
  v_wew_succ            int;

  v_end_we_old          date;
  v_boundary_week_end   date;
  v_boundary_week_start date;

  v_split_week          boolean := false;
  v_already_split       boolean := false;
  v_split_note          text := null;
  v_split_group_key     text := null;
  v_old_mask            text := null;
  v_new_mask            text := null;

  v_schedule_clashes    jsonb := null;
  v_overlap_warnings    jsonb := '[]'::jsonb;
  v_clash_count         int := 0;

  v_err                 jsonb;

  -- Successor computed fields (merged overrides)
  v_succ_candidate_id   uuid;
  v_succ_client_id      uuid;
  v_succ_role           text;
  v_succ_band           text;
  v_succ_display_site   text;
  v_succ_ward_hint      text;

  v_succ_pay_method_snapshot text;
  v_succ_rates_json          jsonb;
  v_succ_std_schedule_json   jsonb;
  v_succ_std_hours_json      jsonb;
  v_succ_bucket_labels_json  jsonb;
  v_succ_additional_rates_json jsonb;

  v_succ_weekly_timesheet_source public.weekly_timesheet_source_enum;
  v_succ_overrideclientsettings boolean;
  v_succ_no_timesheet_required boolean;
  v_succ_daily_calc_of_invoices boolean;
  v_succ_group_nightsat_sunbh boolean;
  v_succ_is_nhsp boolean;
  v_succ_autoprocess_hr boolean;
  v_succ_requires_hr boolean;
  v_succ_hr_attach_to_invoice boolean;
  v_succ_ts_attach_to_invoice boolean;
  v_succ_reference_number_required_to_issue_invoice boolean;
  v_succ_send_manual_invoices_to_different_email boolean;
  v_succ_manual_invoices_alt_email_address text;
  v_succ_send_ts_queries_to_different_email boolean;
  v_succ_ts_queries_alt_email_address text;
  v_succ_is_ad_hoc boolean;
  v_succ_default_submission_mode public.submission_mode_enum;
  v_succ_timesheet_break_entry_mode public.timesheet_break_entry_mode_enum;

  -- std_hours derivation scratch
  v_day_key text;
  v_day_cfg jsonb;
  v_start_str text;
  v_end_str text;
  v_break_minutes numeric;
  v_start_h int;
  v_start_m int;
  v_end_h int;
  v_end_m int;
  v_start_mins int;
  v_end_mins int;
  v_minutes int;
  v_expected_minutes int;
  v_hours numeric;

  -- Split boundary hard-block helper
  v_bad_contract_id uuid;
  v_bad_timesheet_id uuid;

  -- Audit helpers
  v_before_state jsonb;
  v_after_state jsonb;
  v_audit_reason text;

  -- sink for CTE
  v_dummy int;

  -- =====================================================
  -- DEBUGGING (gated by settings_defaults.invoice_debug)
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := clock_timestamp();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_detail text := null;
  v_dbg_hint text := null;
  v_dbg_context text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_rc int := 0;

begin
  -- ─────────────────────────────────────────────────────────────
  -- Load invoice_debug flag (safe if column/table not present)
  -- ─────────────────────────────────────────────────────────────
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
      from public.settings_defaults sd
     where sd.id = 1
     limit 1;
  exception
    when undefined_column then
      v_invoice_debug := false;
    when undefined_table then
      v_invoice_debug := false;
    when others then
      -- never allow debug flag read to break functional flow
      v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'inputs', jsonb_build_object(
          'p_contract_id', coalesce(p_contract_id::text,''),
          'p_new_start_date', coalesce(p_new_start_date::text,''),
          'p_new_end_date', coalesce(p_new_end_date::text,''),
          'p_end_existing_on', coalesce(p_end_existing_on::text,''),
          'p_assign_existing_candidate', coalesce(p_assign_existing_candidate, true),
          'p_new_candidate_id', coalesce(p_new_candidate_id::text,''),
          'p_split_worker_note', coalesce(p_split_worker_note,''),
          'p_successor_overrides_type', case when p_successor_overrides is null then 'null' else jsonb_typeof(p_successor_overrides) end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false),
          'p_force_already_split_week', coalesce(p_force_already_split_week,false),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'p_actor_user_id', coalesce(p_actor_user_id::text,'')
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Basic input validation
  -- ─────────────────────────────────────────────────────────────
  if p_contract_id is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_contract_id is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','p_contract_id is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start is null or v_new_end is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date and p_new_end_date are required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','new_start or new_end is null',
          'error', v_err,
          'computed', jsonb_build_object('v_new_start', coalesce(v_new_start::text,''), 'v_new_end', coalesce(v_new_end::text,''))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start > v_new_end then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date must be <= p_new_end_date');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_new_start > v_new_end',
          'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_end_existing_on is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_close_to is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','validation_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end, 'v_close_to', v_close_to)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Lock predecessor contract
  -- ─────────────────────────────────────────────────────────────
  select c.*
    into v_cur
    from public.contracts as c
   where c.id = p_contract_id
   for update;

  if not found then
    v_err := jsonb_build_object('error','CONTRACT_NOT_FOUND','contract_id',p_contract_id);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','contract_not_found',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_predecessor',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'predecessor', jsonb_build_object(
          'id', v_cur.id,
          'candidate_id', coalesce(v_cur.candidate_id::text,''),
          'client_id', coalesce(v_cur.client_id::text,''),
          'start_date', v_cur.start_date,
          'end_date', v_cur.end_date,
          'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- Close window rules (end-existing enforced)
  if v_close_to < v_cur.start_date then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be >= predecessor.start_date',
      'predecessor_start_date', v_cur.start_date,
      'end_existing_on', v_close_to
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to < predecessor.start_date',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to > (v_new_start - 1) then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be <= p_new_start_date - 1 day',
      'end_existing_on', v_close_to,
      'new_start_date', v_new_start
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to > new_start - 1',
          'error', v_err,
          'computed', jsonb_build_object('new_start_minus_1', (v_new_start - 1))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','close_window_rules_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'predecessor_start_date', v_cur.start_date,
          'close_to', v_close_to,
          'new_start', v_new_start,
          'close_to_max', (v_new_start - 1)
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Determine successor candidate assignment
  -- ─────────────────────────────────────────────────────────────
  if coalesce(p_assign_existing_candidate, true) then
    v_succ_candidate_id := v_cur.candidate_id;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','succ_candidate_assignment',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'path','assign_existing_candidate',
          'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
        )
      );
    end if;

  else
    if p_new_candidate_id is not null then
      v_succ_candidate_id := p_new_candidate_id;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','explicit_param_new_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    elsif (v_ov ? 'candidate_id') and nullif(btrim(v_ov->>'candidate_id'), '') is not null then
      v_succ_candidate_id := (v_ov->>'candidate_id')::uuid;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','override_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    else
      v_succ_candidate_id := null;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','no_candidate_assigned',
            'successor_candidate_id',''
          )
        );
      end if;

    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Predecessor week-ending snapshot validation + derived dates
  -- ─────────────────────────────────────────────────────────────
  v_wew_pred := coalesce(v_cur.week_ending_weekday_snapshot, 0);

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','predecessor_wew_loaded',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'week_ending_weekday_snapshot', v_wew_pred
      )
    );
  end if;

  if v_wew_pred < 0 or v_wew_pred > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_CONTRACT_STATE',
      'message','predecessor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_predecessor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- endWEOld = week ending date of the week containing close_to (using predecessor snapshot)
  v_end_we_old :=
    (v_close_to + (((v_wew_pred - extract(dow from v_close_to)::int + 7) % 7)) * interval '1 day')::date;

  -- boundary week for newStart (using predecessor snapshot)
  v_boundary_week_end :=
    (v_new_start + (((v_wew_pred - extract(dow from v_new_start)::int + 7) % 7)) * interval '1 day')::date;
  v_boundary_week_start := (v_boundary_week_end - interval '6 days')::date;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','derived_dates',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'v_wew_pred', v_wew_pred,
          'v_end_we_old', v_end_we_old,
          'v_boundary_week_start', v_boundary_week_start,
          'v_boundary_week_end', v_boundary_week_end,
          'v_new_start', v_new_start,
          'v_new_end', v_new_end,
          'v_close_to', v_close_to
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Hard block: submitted beyond close window
  -- ─────────────────────────────────────────────────────────────
  if exists (
    select 1
      from public.contract_weeks as cw
     where cw.contract_id = v_cur.id
       and cw.timesheet_id is not null
       and cw.week_ending_date > v_end_we_old
  ) then
    v_err := jsonb_build_object(
      'error','SUBMITTED_BEYOND_CLOSE',
      'contract_id', v_cur.id,
      'end_week_ending_date', v_end_we_old
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_submitted_beyond_close',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','submitted_beyond_close_check_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'contract_id', v_cur.id::text,
        'end_we_old', v_end_we_old
      )
    );
  end if;

  -- NEW HARD RULE: ending week submitted + midweek truncation
  if v_close_to < v_end_we_old then
    if exists (
      select 1
        from public.contract_weeks as cw
       where cw.contract_id = v_cur.id
         and cw.week_ending_date = v_end_we_old
         and cw.timesheet_id is not null
    ) then
      v_err := jsonb_build_object(
        'error','ENDING_WEEK_SUBMITTED_CANNOT_TRUNCATE',
        'contract_id', v_cur.id,
        'close_to', v_close_to,
        'week_ending_date', v_end_we_old
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_ending_week_submitted_midweek_truncation',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ending_week_truncation_rule_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'close_to', v_close_to,
        'end_we_old', v_end_we_old,
        'is_midweek_truncation', (v_close_to < v_end_we_old)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week eligibility (same candidate on both halves + newStart midweek + predecessor ends in that same week)
  -- ─────────────────────────────────────────────────────────────
  v_split_week := (
    v_cur.candidate_id is not null
    and v_succ_candidate_id is not null
    and v_cur.candidate_id = v_succ_candidate_id
    and v_new_start <> v_boundary_week_start
    and v_end_we_old = v_boundary_week_end
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','split_week_eligibility',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'components', jsonb_build_object(
          'pred_candidate_not_null', (v_cur.candidate_id is not null),
          'succ_candidate_not_null', (v_succ_candidate_id is not null),
          'same_candidate', (v_cur.candidate_id is not null and v_succ_candidate_id is not null and v_cur.candidate_id = v_succ_candidate_id),
          'new_start_is_midweek', (v_new_start <> v_boundary_week_start),
          'pred_end_week_equals_boundary_week', (v_end_we_old = v_boundary_week_end)
        ),
        'result', v_split_week
      )
    );
  end if;

  if v_split_week then
    -- Hard block: boundary week already submitted (contract-based definition)
    select cw2.contract_id, cw2.timesheet_id
      into v_bad_contract_id, v_bad_timesheet_id
      from public.contracts as c2
      join public.contract_weeks as cw2
        on cw2.contract_id = c2.id
     where c2.candidate_id = v_cur.candidate_id
       and c2.client_id = v_cur.client_id
       and c2.start_date <= v_boundary_week_end
       and c2.end_date >= v_boundary_week_start
       and cw2.week_ending_date = v_boundary_week_end
       and cw2.timesheet_id is not null
     limit 1;

    if found then
      v_err := jsonb_build_object(
        'error','BOUNDARY_WEEK_TIMESHEET_ALREADY_SUBMITTED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_week_end', v_boundary_week_end,
        'contract_id', v_bad_contract_id,
        'timesheet_id', v_bad_timesheet_id
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_boundary_week_already_submitted',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'found_contract_id', coalesce(v_bad_contract_id::text,''),
            'found_timesheet_id', coalesce(v_bad_timesheet_id::text,''),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','boundary_week_submitted_check_ok',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'boundary_week_end', v_boundary_week_end
        )
      );
    end if;

    -- Already split detection (another overlapping contract for same candidate+client in boundary week)
    select exists (
      select 1
        from public.contracts as c2
       where c2.id <> v_cur.id
         and c2.candidate_id = v_cur.candidate_id
         and c2.client_id = v_cur.client_id
         and c2.start_date <= v_boundary_week_end
         and c2.end_date >= v_boundary_week_start
    ) into v_already_split;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','already_split_detection',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'already_split', v_already_split,
          'p_force_already_split_week', coalesce(p_force_already_split_week,false)
        )
      );
    end if;

    if v_already_split and not coalesce(p_force_already_split_week, false) then
      v_err := jsonb_build_object(
        'error','ALREADY_SPLIT_WEEK',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'overlapping_contract_ids', (
          select coalesce(jsonb_agg(c2.id order by c2.start_date), '[]'::jsonb)
            from public.contracts as c2
           where c2.candidate_id = v_cur.candidate_id
             and c2.client_id = v_cur.client_id
             and c2.start_date <= v_boundary_week_end
             and c2.end_date >= v_boundary_week_start
        )
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_already_split_week_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    -- Allowed day masks (Mon..Sun)
    -- Old half: max(predecessor.start_date, week_start) .. closeTo
    if greatest(v_cur.start_date, v_boundary_week_start) > v_close_to then
      v_old_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_old_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   greatest(v_cur.start_date, v_boundary_week_start)::timestamp,
                   v_close_to::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    end if;

    -- New half: newStart .. min(newEnd, week_end)
    if v_new_start > least(v_new_end, v_boundary_week_end) then
      v_new_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_new_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   v_new_start::timestamp,
                   least(v_new_end, v_boundary_week_end)::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    end if;

    -- Confirm gating (note required)
    v_split_note := btrim(coalesce(p_split_worker_note, ''));

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirm_gating',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'split_note_empty', (v_split_note = ''),
          'split_note_len', length(v_split_note),
          'old_mask', v_old_mask,
          'new_mask', v_new_mask
        )
      );
    end if;

    if (not coalesce(p_confirmed_split_week, false)) or v_split_note = '' then
      v_err := jsonb_build_object(
        'error','SPLIT_WEEK_CONFIRM_REQUIRED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
        'old_allowed_to', v_close_to,
        'new_allowed_from', v_new_start,
        'new_allowed_to', least(v_new_end, v_boundary_week_end),
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'suggested_worker_note',
          ('Contract rates have changed this week and therefore you need to submit two timesheets. One timesheet for work completed for ' ||
           to_char(greatest(v_cur.start_date, v_boundary_week_start),'YYYY-MM-DD') || ' to ' ||
           to_char(v_close_to,'YYYY-MM-DD') || ' and another timesheet for ' ||
           to_char(v_new_start,'YYYY-MM-DD') || ' to ' ||
           to_char(least(v_new_end, v_boundary_week_end),'YYYY-MM-DD') || '.')
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_split_week_confirm_required',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    v_split_group_key := gen_random_uuid()::text;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirmed',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'split_group_key', v_split_group_key,
          'worker_note', v_split_note
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Build successor merged fields from overrides
  -- ─────────────────────────────────────────────────────────────
  v_succ_client_id := coalesce(nullif(btrim(v_ov->>'client_id'), '')::uuid, v_cur.client_id);
  v_succ_role := coalesce(v_ov->>'role', v_cur.role);
  v_succ_band := coalesce(v_ov->>'band', v_cur.band);
  v_succ_display_site := coalesce(v_ov->>'display_site', v_cur.display_site);
  v_succ_ward_hint := coalesce(v_ov->>'ward_hint', v_cur.ward_hint);

  v_succ_pay_method_snapshot := coalesce(nullif(btrim(v_ov->>'pay_method_snapshot'), ''), v_cur.pay_method_snapshot);

  if (v_ov ? 'rates_json') and jsonb_typeof(v_ov->'rates_json') <> 'null' then
    v_succ_rates_json := v_ov->'rates_json';
  else
    v_succ_rates_json := v_cur.rates_json;
  end if;
  v_succ_rates_json := coalesce(v_succ_rates_json, '{}'::jsonb);

  if (v_ov ? 'std_schedule_json') then
    if jsonb_typeof(v_ov->'std_schedule_json') = 'null' then
      v_succ_std_schedule_json := null;
    else
      v_succ_std_schedule_json := v_ov->'std_schedule_json';
    end if;
  else
    v_succ_std_schedule_json := v_cur.std_schedule_json;
  end if;

  if v_succ_std_schedule_json is not null and jsonb_typeof(v_succ_std_schedule_json) = 'object' then
    -- Derive std_hours_json from std_schedule_json
    v_succ_std_hours_json := '{}'::jsonb;

    foreach v_day_key in array array['mon','tue','wed','thu','fri','sat','sun'] loop
      v_day_cfg := v_succ_std_schedule_json->v_day_key;

      if v_day_cfg is not null and jsonb_typeof(v_day_cfg) = 'object' then
        v_start_str := v_day_cfg->>'start';
        v_end_str := v_day_cfg->>'end';

        if v_start_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_start_h := split_part(v_start_str,':',1)::int;
          v_start_m := split_part(v_start_str,':',2)::int;
          if v_start_h between 0 and 23 and v_start_m between 0 and 59 then
            v_start_mins := v_start_h * 60 + v_start_m;
          else
            v_start_mins := null;
          end if;
        else
          v_start_mins := null;
        end if;

        if v_end_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_end_h := split_part(v_end_str,':',1)::int;
          v_end_m := split_part(v_end_str,':',2)::int;
          if v_end_h between 0 and 23 and v_end_m between 0 and 59 then
            v_end_mins := v_end_h * 60 + v_end_m;
          else
            v_end_mins := null;
          end if;
        else
          v_end_mins := null;
        end if;

        v_break_minutes := 0;
        if (v_day_cfg ? 'break_minutes')
          and (v_day_cfg->>'break_minutes') is not null
          and (v_day_cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
        then
          v_break_minutes := (v_day_cfg->>'break_minutes')::numeric;
        end if;

        if v_start_mins is not null and v_end_mins is not null then
          if v_end_mins <= v_start_mins then
            v_minutes := 1440 - v_start_mins + v_end_mins;
          else
            v_minutes := v_end_mins - v_start_mins;
          end if;

          v_expected_minutes := greatest(0, (v_minutes - v_break_minutes)::int);
          v_hours := round((v_expected_minutes::numeric / 60.0), 2);
        else
          v_hours := 0;
        end if;
      else
        v_hours := 0;
      end if;

      v_succ_std_hours_json := v_succ_std_hours_json || jsonb_build_object(v_day_key, v_hours);
    end loop;
  else
    if (v_ov ? 'std_hours_json') then
      if jsonb_typeof(v_ov->'std_hours_json') = 'null' then
        v_succ_std_hours_json := null;
      else
        v_succ_std_hours_json := v_ov->'std_hours_json';
      end if;
    else
      v_succ_std_hours_json := v_cur.std_hours_json;
    end if;
  end if;

  if (v_ov ? 'bucket_labels_json') then
    if jsonb_typeof(v_ov->'bucket_labels_json') = 'null' then
      v_succ_bucket_labels_json := null;
    else
      v_succ_bucket_labels_json := v_ov->'bucket_labels_json';
    end if;
  else
    v_succ_bucket_labels_json := v_cur.bucket_labels_json;
  end if;

  if (v_ov ? 'additional_rates_json') then
    if jsonb_typeof(v_ov->'additional_rates_json') = 'null' then
      v_succ_additional_rates_json := null;
    else
      v_succ_additional_rates_json := v_ov->'additional_rates_json';
    end if;
  else
    v_succ_additional_rates_json := v_cur.additional_rates_json;
  end if;

  if (v_ov ? 'weekly_timesheet_source') then
    v_succ_weekly_timesheet_source := nullif(btrim(v_ov->>'weekly_timesheet_source'), '')::public.weekly_timesheet_source_enum;
  else
    v_succ_weekly_timesheet_source := v_cur.weekly_timesheet_source;
  end if;

  if (v_ov ? 'overrideclientsettings') and jsonb_typeof(v_ov->'overrideclientsettings') <> 'null' then
    v_succ_overrideclientsettings := (v_ov->>'overrideclientsettings')::boolean;
  else
    v_succ_overrideclientsettings := v_cur.overrideclientsettings;
  end if;
  v_succ_overrideclientsettings := coalesce(v_succ_overrideclientsettings, false);

  if (v_ov ? 'no_timesheet_required') then
    if jsonb_typeof(v_ov->'no_timesheet_required') = 'null' then
      v_succ_no_timesheet_required := null;
    else
      v_succ_no_timesheet_required := (v_ov->>'no_timesheet_required')::boolean;
    end if;
  else
    v_succ_no_timesheet_required := v_cur.no_timesheet_required;
  end if;

  if (v_ov ? 'daily_calc_of_invoices') then
    if jsonb_typeof(v_ov->'daily_calc_of_invoices') = 'null' then
      v_succ_daily_calc_of_invoices := null;
    else
      v_succ_daily_calc_of_invoices := (v_ov->>'daily_calc_of_invoices')::boolean;
    end if;
  else
    v_succ_daily_calc_of_invoices := v_cur.daily_calc_of_invoices;
  end if;

  if (v_ov ? 'group_nightsat_sunbh') then
    if jsonb_typeof(v_ov->'group_nightsat_sunbh') = 'null' then
      v_succ_group_nightsat_sunbh := null;
    else
      v_succ_group_nightsat_sunbh := (v_ov->>'group_nightsat_sunbh')::boolean;
    end if;
  else
    v_succ_group_nightsat_sunbh := v_cur.group_nightsat_sunbh;
  end if;

  if (v_ov ? 'is_nhsp') then
    if jsonb_typeof(v_ov->'is_nhsp') = 'null' then
      v_succ_is_nhsp := null;
    else
      v_succ_is_nhsp := (v_ov->>'is_nhsp')::boolean;
    end if;
  else
    v_succ_is_nhsp := v_cur.is_nhsp;
  end if;

  if (v_ov ? 'autoprocess_hr') then
    if jsonb_typeof(v_ov->'autoprocess_hr') = 'null' then
      v_succ_autoprocess_hr := null;
    else
      v_succ_autoprocess_hr := (v_ov->>'autoprocess_hr')::boolean;
    end if;
  else
    v_succ_autoprocess_hr := v_cur.autoprocess_hr;
  end if;

  if (v_ov ? 'requires_hr') then
    if jsonb_typeof(v_ov->'requires_hr') = 'null' then
      v_succ_requires_hr := null;
    else
      v_succ_requires_hr := (v_ov->>'requires_hr')::boolean;
    end if;
  else
    v_succ_requires_hr := v_cur.requires_hr;
  end if;

  if (v_ov ? 'hr_attach_to_invoice') then
    if jsonb_typeof(v_ov->'hr_attach_to_invoice') = 'null' then
      v_succ_hr_attach_to_invoice := null;
    else
      v_succ_hr_attach_to_invoice := (v_ov->>'hr_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_hr_attach_to_invoice := v_cur.hr_attach_to_invoice;
  end if;

  if (v_ov ? 'ts_attach_to_invoice') then
    if jsonb_typeof(v_ov->'ts_attach_to_invoice') = 'null' then
      v_succ_ts_attach_to_invoice := null;
    else
      v_succ_ts_attach_to_invoice := (v_ov->>'ts_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_ts_attach_to_invoice := v_cur.ts_attach_to_invoice;
  end if;

  if (v_ov ? 'reference_number_required_to_issue_invoice') then
    if jsonb_typeof(v_ov->'reference_number_required_to_issue_invoice') = 'null' then
      v_succ_reference_number_required_to_issue_invoice := null;
    else
      v_succ_reference_number_required_to_issue_invoice := (v_ov->>'reference_number_required_to_issue_invoice')::boolean;
    end if;
  else
    v_succ_reference_number_required_to_issue_invoice := v_cur.reference_number_required_to_issue_invoice;
  end if;

  if (v_ov ? 'send_manual_invoices_to_different_email') then
    if jsonb_typeof(v_ov->'send_manual_invoices_to_different_email') = 'null' then
      v_succ_send_manual_invoices_to_different_email := null;
    else
      v_succ_send_manual_invoices_to_different_email := (v_ov->>'send_manual_invoices_to_different_email')::boolean;
    end if;
  else
    v_succ_send_manual_invoices_to_different_email := v_cur.send_manual_invoices_to_different_email;
  end if;

  if (v_ov ? 'manual_invoices_alt_email_address') then
    v_succ_manual_invoices_alt_email_address := nullif(btrim(v_ov->>'manual_invoices_alt_email_address'), '');
  else
    v_succ_manual_invoices_alt_email_address := v_cur.manual_invoices_alt_email_address;
  end if;

  if (v_ov ? 'send_ts_queries_to_different_email') then
    if jsonb_typeof(v_ov->'send_ts_queries_to_different_email') = 'null' then
      v_succ_send_ts_queries_to_different_email := coalesce(v_cur.send_ts_queries_to_different_email,false);
    else
      v_succ_send_ts_queries_to_different_email := (v_ov->>'send_ts_queries_to_different_email')::boolean;
    end if;
  else
    v_succ_send_ts_queries_to_different_email := coalesce(v_cur.send_ts_queries_to_different_email,false);
  end if;

  if (v_ov ? 'ts_queries_alt_email_address') then
    v_succ_ts_queries_alt_email_address := nullif(btrim(v_ov->>'ts_queries_alt_email_address'), '');
  else
    v_succ_ts_queries_alt_email_address := v_cur.ts_queries_alt_email_address;
  end if;

  if (v_ov ? 'is_ad_hoc') and jsonb_typeof(v_ov->'is_ad_hoc') <> 'null' then
    v_succ_is_ad_hoc := (v_ov->>'is_ad_hoc')::boolean;
  else
    v_succ_is_ad_hoc := v_cur.is_ad_hoc;
  end if;
  v_succ_is_ad_hoc := coalesce(v_succ_is_ad_hoc, false);

  if (v_ov ? 'default_submission_mode') then
    v_succ_default_submission_mode := nullif(btrim(v_ov->>'default_submission_mode'), '')::public.submission_mode_enum;
  else
    v_succ_default_submission_mode := v_cur.default_submission_mode;
  end if;

  if (v_ov ? 'timesheet_break_entry_mode') then
    if jsonb_typeof(v_ov->'timesheet_break_entry_mode')='null' then
      v_succ_timesheet_break_entry_mode:=null;
    else
      v_succ_timesheet_break_entry_mode:=
        nullif(btrim(v_ov->>'timesheet_break_entry_mode'),'')::public.timesheet_break_entry_mode_enum;
    end if;
  else
    v_succ_timesheet_break_entry_mode:=v_cur.timesheet_break_entry_mode;
  end if;

  if (v_ov ? 'week_ending_weekday_snapshot') and nullif(btrim(v_ov->>'week_ending_weekday_snapshot'), '') is not null then
    v_wew_succ := (v_ov->>'week_ending_weekday_snapshot')::int;
  else
    v_wew_succ := coalesce(v_cur.week_ending_weekday_snapshot, 0);
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','successor_fields_merged',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'merged', jsonb_build_object(
          'succ_candidate_id', coalesce(v_succ_candidate_id::text,''),
          'succ_client_id', coalesce(v_succ_client_id::text,''),
          'succ_role', coalesce(v_succ_role,''),
          'succ_band', coalesce(v_succ_band,''),
          'succ_display_site', coalesce(v_succ_display_site,''),
          'succ_ward_hint', coalesce(v_succ_ward_hint,''),
          'succ_pay_method_snapshot', coalesce(v_succ_pay_method_snapshot,''),
          'succ_week_ending_weekday_snapshot', v_wew_succ,
          'succ_overrideclientsettings', coalesce(v_succ_overrideclientsettings,false),
          'succ_weekly_timesheet_source', coalesce(v_succ_weekly_timesheet_source::text,''),
          'succ_no_timesheet_required', case when v_succ_no_timesheet_required is null then null else v_succ_no_timesheet_required end,
          'succ_daily_calc_of_invoices', case when v_succ_daily_calc_of_invoices is null then null else v_succ_daily_calc_of_invoices end,
          'succ_group_nightsat_sunbh', case when v_succ_group_nightsat_sunbh is null then null else v_succ_group_nightsat_sunbh end,
          'succ_is_nhsp', case when v_succ_is_nhsp is null then null else v_succ_is_nhsp end,
          'succ_autoprocess_hr', case when v_succ_autoprocess_hr is null then null else v_succ_autoprocess_hr end,
          'succ_requires_hr', case when v_succ_requires_hr is null then null else v_succ_requires_hr end,
          'succ_hr_attach_to_invoice', case when v_succ_hr_attach_to_invoice is null then null else v_succ_hr_attach_to_invoice end,
          'succ_ts_attach_to_invoice', case when v_succ_ts_attach_to_invoice is null then null else v_succ_ts_attach_to_invoice end,
          'succ_reference_number_required_to_issue_invoice', case when v_succ_reference_number_required_to_issue_invoice is null then null else v_succ_reference_number_required_to_issue_invoice end,
          'succ_send_manual_invoices_to_different_email', case when v_succ_send_manual_invoices_to_different_email is null then null else v_succ_send_manual_invoices_to_different_email end,
          'succ_manual_invoices_alt_email_address', coalesce(v_succ_manual_invoices_alt_email_address,''),
          'succ_send_ts_queries_to_different_email', case when v_succ_send_ts_queries_to_different_email is null then null else v_succ_send_ts_queries_to_different_email end,
          'succ_ts_queries_alt_email_address', coalesce(v_succ_ts_queries_alt_email_address,''),
          'succ_is_ad_hoc', coalesce(v_succ_is_ad_hoc,false),
          'succ_default_submission_mode', coalesce(v_succ_default_submission_mode::text,''),
          'succ_timesheet_break_entry_mode',v_succ_timesheet_break_entry_mode
        ),
        'overrides_keys', (
          select coalesce(jsonb_agg(k), '[]'::jsonb)
          from jsonb_object_keys(v_ov) as k
        )
      )
    );
  end if;

  if v_wew_succ < 0 or v_wew_succ > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','successor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_wew_succ
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_successor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- Route flag validations
  if v_succ_is_nhsp is true and v_succ_autoprocess_hr is true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','is_nhsp and autoprocess_hr cannot both be true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','is_nhsp && autoprocess_hr',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_succ_no_timesheet_required is true and v_succ_autoprocess_hr is distinct from true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','no_timesheet_required=true requires autoprocess_hr=true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','no_timesheet_required && autoprocess_hr != true',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','route_flags_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
      )
    );
  end if;

  -- Snapshot predecessor state for audit before mutations
  v_before_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_start_date', v_cur.start_date,
    'predecessor_end_date', v_cur.end_date,
    'predecessor_candidate_id', v_cur.candidate_id,
    'predecessor_client_id', v_cur.client_id,
    'new_start_date', v_new_start,
    'new_end_date', v_new_end,
    'end_existing_on', v_close_to
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','before_state_snapshot',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'before_state', v_before_state
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Insert successor contract
  -- ─────────────────────────────────────────────────────────────
  insert into public.contracts as c (
    candidate_id,
    client_id,
    role,
    band,
    display_site,
    ward_hint,
    start_date,
    end_date,
    pay_method_snapshot,
    rates_json,
    std_hours_json,
    default_submission_mode,
    week_ending_weekday_snapshot,
    auto_invoice,
    require_reference_to_pay,
    require_reference_to_invoice,
    bucket_labels_json,
    std_schedule_json,
    mileage_pay_rate,
    mileage_charge_rate,
    additional_rates_json,
    created_at,
    updated_at,
    self_bill,
    weekly_timesheet_source,
    no_timesheet_required,
    daily_calc_of_invoices,
    group_nightsat_sunbh,
    is_nhsp,
    autoprocess_hr,
    requires_hr,
    hr_attach_to_invoice,
    ts_attach_to_invoice,
    overrideclientsettings,
    reference_number_required_to_issue_invoice,
    send_manual_invoices_to_different_email,
    manual_invoices_alt_email_address,
    send_ts_queries_to_different_email,
    ts_queries_alt_email_address,
    is_ad_hoc,
    timesheet_break_entry_mode,
    candidate_paper_submission_enabled_override,
    candidate_manager_approval_policy_json
  ) values (
    v_succ_candidate_id,
    v_succ_client_id,
    v_succ_role,
    v_succ_band,
    v_succ_display_site,
    v_succ_ward_hint,
    v_new_start,
    v_new_end,
    v_succ_pay_method_snapshot,
    v_succ_rates_json,
    v_succ_std_hours_json,
    v_succ_default_submission_mode,
    v_wew_succ::smallint,
    v_cur.auto_invoice,
    v_cur.require_reference_to_pay,
    v_cur.require_reference_to_invoice,
    v_succ_bucket_labels_json,
    v_succ_std_schedule_json,
    v_cur.mileage_pay_rate,
    v_cur.mileage_charge_rate,
    v_succ_additional_rates_json,
    v_now,
    v_now,
    v_cur.self_bill,
    v_succ_weekly_timesheet_source,
    v_succ_no_timesheet_required,
    v_succ_daily_calc_of_invoices,
    v_succ_group_nightsat_sunbh,
    v_succ_is_nhsp,
    v_succ_autoprocess_hr,
    v_succ_requires_hr,
    v_succ_hr_attach_to_invoice,
    v_succ_ts_attach_to_invoice,
    v_succ_overrideclientsettings,
    v_succ_reference_number_required_to_issue_invoice,
    v_succ_send_manual_invoices_to_different_email,
    v_succ_manual_invoices_alt_email_address,
    v_succ_send_ts_queries_to_different_email,
    v_succ_ts_queries_alt_email_address,
    v_succ_is_ad_hoc,
    v_succ_timesheet_break_entry_mode,
    v_cur.candidate_paper_submission_enabled_override,
    v_cur.candidate_manager_approval_policy_json
  )
  returning c.* into v_succ;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','inserted_successor_contract',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'successor', jsonb_build_object(
          'id', v_succ.id,
          'candidate_id', coalesce(v_succ.candidate_id::text,''),
          'client_id', coalesce(v_succ.client_id::text,''),
          'start_date', v_succ.start_date,
          'end_date', v_succ.end_date,
          'week_ending_weekday_snapshot', v_succ.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Update predecessor end_date (end-existing enforced)
  -- ─────────────────────────────────────────────────────────────
  update public.contracts as c
     set end_date = v_close_to,
         updated_at = v_now
   where c.id = v_cur.id;

  get diagnostics v_rc = row_count;

  v_cur.end_date := v_close_to;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','updated_predecessor_end_date',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'predecessor_id', v_cur.id::text,
        'new_end_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Generate/ensure base contract_weeks rows for predecessor + successor in their final windows
  -- (including planned_schedule_json derived from std_schedule_json when present)
  -- ─────────────────────────────────────────────────────────────
  with contract_targets as (
    select
      v_cur.id as contract_id,
      v_cur.client_id as client_id,
      v_cur.start_date as start_date,
      v_cur.end_date as end_date,
      v_wew_pred as wew,
      v_cur.std_schedule_json as std_schedule_json,
      v_cur.overrideclientsettings as overrideclientsettings,
      v_cur.default_submission_mode as default_submission_mode
    union all
    select
      v_succ.id as contract_id,
      v_succ.client_id as client_id,
      v_succ.start_date as start_date,
      v_succ.end_date as end_date,
      v_wew_succ as wew,
      v_succ.std_schedule_json as std_schedule_json,
      v_succ.overrideclientsettings as overrideclientsettings,
      v_succ.default_submission_mode as default_submission_mode
  ),
  targets as (
    select
      ct.contract_id,
      ct.client_id,
      ct.start_date,
      ct.end_date,
      ct.wew,
      ct.std_schedule_json,
      ct.overrideclientsettings,
      ct.default_submission_mode,
      (
        select cs.default_submission_mode
          from public.client_settings as cs
         where cs.client_id = ct.client_id
         order by cs.effective_from desc nulls last, cs.updated_at desc
         limit 1
      ) as client_default_submission_mode
    from contract_targets as ct
  ),
  targets2 as (
    select
      t.contract_id,
      t.start_date,
      t.end_date,
      t.wew,
      t.std_schedule_json,
      case
        when t.overrideclientsettings is true
          then coalesce(t.default_submission_mode, t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
        else coalesce(t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
      end as submission_mode_snapshot,
      (t.start_date + (((t.wew - extract(dow from t.start_date)::int + 7) % 7)) * interval '1 day')::date as start_we,
      (t.end_date   + (((t.wew - extract(dow from t.end_date)::int   + 7) % 7)) * interval '1 day')::date as end_we
    from targets as t
  ),
  weeks as (
    select
      t2.contract_id,
      t2.start_date,
      t2.end_date,
      t2.wew,
      t2.std_schedule_json,
      t2.submission_mode_snapshot,
      gs::date as week_end
    from targets2 as t2
    cross join lateral generate_series(t2.start_we::timestamp, t2.end_we::timestamp, interval '7 days') as gs
  ),
  ins as (
    insert into public.contract_weeks as cw (
      contract_id,
      week_ending_date,
      additional_seq,
      status,
      timesheet_id,
      planned_schedule_json,
      created_at,
      updated_at,
      submission_mode_snapshot,
      is_adjustment,
      enforce_day_partition,
      allowed_days_mask,
      split_boundary_date,
      worker_note,
      split_group_key
    )
    select
      w.contract_id,
      w.week_end,
      0 as additional_seq,
      case
        when w.week_end <= current_date then 'OPEN'::public.contract_week_status_enum
        else 'PLANNED'::public.contract_week_status_enum
      end as status,
      null::uuid as timesheet_id,
      case
        when pj.plan_json is null then null
        when jsonb_typeof(pj.plan_json) <> 'array' then pj.plan_json
        when jsonb_array_length(pj.plan_json) = 0 then null
        else pj.plan_json
      end as planned_schedule_json,
      v_now as created_at,
      v_now as updated_at,
      w.submission_mode_snapshot,
      false as is_adjustment,
      false as enforce_day_partition,
      null::text as allowed_days_mask,
      null::date as split_boundary_date,
      null::text as worker_note,
      null::text as split_group_key
    from weeks as w
    left join lateral (
      select jsonb_agg(ent.entry_json order by ent.entry_date) as plan_json
      from (
        select
          (dt)::date as entry_date,
          jsonb_build_object(
            'date', to_char((dt)::date, 'YYYY-MM-DD'),
            'start', sc.cfg->>'start',
            'end',   sc.cfg->>'end',
            'breaks', case when jsonb_typeof(sc.cfg->'breaks') = 'array' then sc.cfg->'breaks' else '[]'::jsonb end,
            'break_minutes', br.break_minutes,
            'overnight', ov.overnight_flag,
            'expected_minutes', ov.expected_minutes
          ) as entry_json
        from generate_series(
               greatest(((w.week_end - interval '6 days')::date), w.start_date)::timestamp,
               least(w.week_end, w.end_date)::timestamp,
               interval '1 day'
             ) as dt
        cross join lateral (
          select case extract(dow from dt)::int
            when 1 then 'mon'
            when 2 then 'tue'
            when 3 then 'wed'
            when 4 then 'thu'
            when 5 then 'fri'
            when 6 then 'sat'
            else 'sun'
          end as day_key
        ) as dk
        cross join lateral (
          select (w.std_schedule_json -> dk.day_key) as cfg
        ) as sc
        cross join lateral (
          select
            case
              when (sc.cfg ? 'break_minutes')
               and (sc.cfg->>'break_minutes') is not null
               and (sc.cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
              then (sc.cfg->>'break_minutes')::numeric
              else 0::numeric
            end as break_minutes
        ) as br
        cross join lateral (
          select
            (sc.cfg->>'start') as start_str,
            (sc.cfg->>'end')   as end_str
        ) as se
        cross join lateral (
          select
            case
              when se.start_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.start_str,':',1)::int between 0 and 23
               and split_part(se.start_str,':',2)::int between 0 and 59
              then (split_part(se.start_str,':',1)::int * 60 + split_part(se.start_str,':',2)::int)
              else null
            end as start_mins,
            case
              when se.end_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.end_str,':',1)::int between 0 and 23
               and split_part(se.end_str,':',2)::int between 0 and 59
              then (split_part(se.end_str,':',1)::int * 60 + split_part(se.end_str,':',2)::int)
              else null
            end as end_mins
        ) as tm
        cross join lateral (
          select
            case
              when tm.start_mins is null or tm.end_mins is null then false
              when tm.end_mins <= tm.start_mins then true
              else false
            end as overnight_flag,
            case
              when tm.start_mins is null or tm.end_mins is null then 0
              when tm.end_mins <= tm.start_mins then (1440 - tm.start_mins + tm.end_mins)
              else (tm.end_mins - tm.start_mins)
            end as minutes_diff
        ) as md
        cross join lateral (
          select
            md.overnight_flag,
            greatest(0, (md.minutes_diff - br.break_minutes)::int) as expected_minutes
        ) as ov
        where w.std_schedule_json is not null
          and jsonb_typeof(w.std_schedule_json) = 'object'
          and jsonb_typeof(sc.cfg) = 'object'
          and tm.start_mins is not null
          and tm.end_mins is not null
      ) as ent
    ) as pj on true
    on conflict on constraint uq_contract_week do nothing
    returning 1
  )
  select 1 into v_dummy from ins limit 1;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ensure_contract_weeks_done',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'note','weeks ensure CTE executed (v_dummy indicates at least one insert when non-null)',
        'v_dummy', v_dummy,
        'predecessor_id', v_cur.id::text,
        'successor_id', v_succ.id::text
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Delete predecessor draft weeks beyond truncated window (never touches submitted weeks)
  -- ─────────────────────────────────────────────────────────────
  delete from public.contract_weeks as cw
   where cw.contract_id = v_cur.id
     and cw.timesheet_id is null
     and cw.week_ending_date > v_end_we_old;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','deleted_predecessor_draft_weeks_beyond_trunc',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'v_end_we_old', v_end_we_old
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Clamp predecessor ending week planned schedule (draft rows only; never delete the week row)
  -- If none remain, set planned_schedule_json = []
  -- ─────────────────────────────────────────────────────────────
  with tgt as (
    select
      cw.id as contract_week_id,
      case
        when cw.planned_schedule_json is null then '[]'::jsonb
        when jsonb_typeof(cw.planned_schedule_json) <> 'array' then cw.planned_schedule_json
        else (
          select coalesce(jsonb_agg(e.elem order by e.ord), '[]'::jsonb)
          from jsonb_array_elements(cw.planned_schedule_json) with ordinality as e(elem, ord)
          where (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            and (e.elem->>'date')::date <= v_close_to
        )
      end as new_plan
    from public.contract_weeks as cw
    where cw.contract_id = v_cur.id
      and cw.week_ending_date = v_end_we_old
      and cw.timesheet_id is null
  )
  update public.contract_weeks as cw
     set planned_schedule_json = tgt.new_plan,
         updated_at = v_now
    from tgt
   where cw.id = tgt.contract_week_id;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','clamped_predecessor_ending_week_plan',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'week_ending_date', v_end_we_old,
        'clamp_to_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week enforcement patch (draft rows only; includes additional_seq variants)
  -- ─────────────────────────────────────────────────────────────
  if v_split_week then
    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_old_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_cur.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_predecessor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_cur.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_old_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_new_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_succ.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_successor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_succ.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_new_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Compute schedule clashes inside DB (block unless forced)
  -- NOTE: avoid using CTE name "overlaps" (keyword/operator in SQL); use ovl_rows instead.
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_start',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'candidate_id', v_succ_candidate_id::text,
          'new_start', v_new_start,
          'new_end', v_new_end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    with params as (
      select
        (v_new_start - 1) as scan_from,
        (v_new_end + 1) as scan_to,
        (v_new_start - 7) as check_we_from,
        (v_new_end + 7) as check_we_to
    ),
    a_entries as (
      select
        cw.id as a_contract_week_id,
        cw.contract_id as a_contract_id,
        cw.week_ending_date as a_week_end,
        cw.additional_seq as a_additional_seq,
        (e.elem->>'date')::date as a_day_date,
        (e.elem->>'start') as a_start,
        (e.elem->>'end') as a_end,
        coalesce((e.elem->>'overnight')::boolean, false) as a_overnight
      from public.contract_weeks as cw
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      where cw.contract_id = v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= v_new_start
        and (e.elem->>'date')::date <= v_new_end
    ),
    a_ts as (
      select
        a.*,
        (a.a_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (a.a_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((a.a_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (a.a_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from a_entries as a
      cross join lateral (
        select
          case
            when a.a_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_start,':',1)::int between 0 and 23
             and split_part(a.a_start,':',2)::int between 0 and 59
            then (split_part(a.a_start,':',1)::int * 60 + split_part(a.a_start,':',2)::int)
            else null
          end as start_mins,
          case
            when a.a_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_end,':',1)::int between 0 and 23
             and split_part(a.a_end,':',2)::int between 0 and 59
            then (split_part(a.a_end,':',1)::int * 60 + split_part(a.a_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    b_entries as (
      select
        cw.id as b_contract_week_id,
        cw.contract_id as b_contract_id,
        c.client_id as b_client_id,
        cw.week_ending_date as b_week_end,
        cw.additional_seq as b_additional_seq,
        (e.elem->>'date')::date as b_day_date,
        (e.elem->>'start') as b_start,
        (e.elem->>'end') as b_end,
        coalesce((e.elem->>'overnight')::boolean, false) as b_overnight
      from public.contract_weeks as cw
      join public.contracts as c
        on c.id = cw.contract_id
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      cross join params as p
      where c.candidate_id = v_succ_candidate_id
        and c.id <> v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and cw.week_ending_date >= p.check_we_from
        and cw.week_ending_date <= p.check_we_to
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= p.scan_from
        and (e.elem->>'date')::date <= p.scan_to
    ),
    b_ts as (
      select
        b.*,
        (b.b_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (b.b_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((b.b_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (b.b_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from b_entries as b
      cross join lateral (
        select
          case
            when b.b_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_start,':',1)::int between 0 and 23
             and split_part(b.b_start,':',2)::int between 0 and 59
            then (split_part(b.b_start,':',1)::int * 60 + split_part(b.b_start,':',2)::int)
            else null
          end as start_mins,
          case
            when b.b_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_end,':',1)::int between 0 and 23
             and split_part(b.b_end,':',2)::int between 0 and 59
            then (split_part(b.b_end,':',1)::int * 60 + split_part(b.b_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    ovl_rows as (
      select
        greatest(a.start_ts, b.start_ts) as overlap_start,
        least(a.end_ts, b.end_ts) as overlap_end,

        a.a_contract_week_id,
        a.a_contract_id,
        a.a_week_end,
        a.a_additional_seq,
        to_char(a.a_day_date, 'YYYY-MM-DD') as a_date,
        a.a_start as a_start,
        a.a_end as a_end,

        b.b_contract_week_id,
        b.b_contract_id,
        b.b_client_id,
        b.b_week_end,
        b.b_additional_seq,
        to_char(b.b_day_date, 'YYYY-MM-DD') as b_date,
        b.b_start as b_start,
        b.b_end as b_end,

        row_number() over (order by greatest(a.start_ts, b.start_ts)) as rn,
        count(*) over () as total_count
      from a_ts as a
      join b_ts as b
        on a.start_ts < b.end_ts
       and a.end_ts > b.start_ts
      where least(a.end_ts, b.end_ts) > greatest(a.start_ts, b.start_ts)
    )
    select
      coalesce(max(o.total_count), 0),
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'overlap_start_utc', to_char(o.overlap_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'overlap_end_utc', to_char(o.overlap_end, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),

            'a_source', 'proposed',
            'a_contract_week_id', o.a_contract_week_id,
            'a_contract_id', o.a_contract_id,
            'a_week_end', o.a_week_end,
            'a_additional_seq', o.a_additional_seq,
            'a_date', o.a_date,
            'a_start', o.a_start,
            'a_end', o.a_end,

            'b_source', 'existing',
            'b_contract_week_id', o.b_contract_week_id,
            'b_contract_id', o.b_contract_id,
            'b_client_id', o.b_client_id,
            'b_week_end', o.b_week_end,
            'b_additional_seq', o.b_additional_seq,
            'b_date', o.b_date,
            'b_start', o.b_start,
            'b_end', o.b_end
          )
          order by o.overlap_start
        ) filter (where o.rn <= 500),
        '[]'::jsonb
      )
      into v_clash_count, v_schedule_clashes
      from ovl_rows as o;

    v_schedule_clashes := jsonb_build_object(
      'candidate_id', v_succ_candidate_id,
      'scan_from', to_char(v_new_start - 1, 'YYYY-MM-DD'),
      'scan_to', to_char(v_new_end + 1, 'YYYY-MM-DD'),
      'check_we_from', to_char(v_new_start - 7, 'YYYY-MM-DD'),
      'check_we_to', to_char(v_new_end + 7, 'YYYY-MM-DD'),
      'clash_count', coalesce(v_clash_count, 0),
      'clashes', coalesce(v_schedule_clashes, '[]'::jsonb)
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_done',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'clash_count', coalesce(v_clash_count,0),
          'forced', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    if coalesce(v_clash_count, 0) > 0 and not coalesce(p_force_schedule_clashes, false) then
      v_err := v_schedule_clashes || jsonb_build_object('error','SCHEDULE_CLASH');

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_schedule_clash_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Overlap warnings (date-range overlap with other contracts for successor candidate)
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', c2.id,
          'client_id', c2.client_id,
          'overlap_from', greatest(c2.start_date, v_new_start),
          'overlap_to', least(c2.end_date, v_new_end)
        )
        order by greatest(c2.start_date, v_new_start)
      ),
      '[]'::jsonb
    )
    into v_overlap_warnings
    from (
      select c2.*
      from public.contracts as c2
      where c2.candidate_id = v_succ_candidate_id
        and c2.id <> v_cur.id
        and c2.id <> v_succ.id
        and c2.start_date <= v_new_end
        and c2.end_date >= v_new_start
      order by c2.start_date
      limit 50
    ) as c2;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','overlap_warnings_computed',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'overlap_warning_count', case when v_overlap_warnings is null or jsonb_typeof(v_overlap_warnings) <> 'array' then null else jsonb_array_length(v_overlap_warnings) end
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Audit event (non-debug canonical writer)
  -- ─────────────────────────────────────────────────────────────
  v_after_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_closed_at', v_close_to,
    'successor_id', v_succ.id,
    'successor_start_date', v_succ.start_date,
    'successor_end_date', v_succ.end_date,
    'successor_candidate_id', v_succ.candidate_id,
    'successor_client_id', v_succ.client_id,
    'split_week', v_split_week,
    'split_group_key', v_split_group_key,
    'forced_schedule_clashes', coalesce(p_force_schedule_clashes, false),
    'forced_already_split_week', coalesce(p_force_already_split_week, false)
  );

  v_audit_reason :=
    'Clone & Extend: predecessor ' || v_cur.id::text ||
    ' closed to ' || to_char(v_close_to,'YYYY-MM-DD') ||
    '; successor ' || v_succ.id::text ||
    ' ' || to_char(v_succ.start_date,'YYYY-MM-DD') || '→' || to_char(v_succ.end_date,'YYYY-MM-DD') ||
    case when v_split_week then '; split week boundary ' || to_char(v_new_start,'YYYY-MM-DD') else '' end ||
    case when coalesce(p_force_schedule_clashes,false) then '; schedule clashes forced' else '' end ||
    case when coalesce(p_force_already_split_week,false) then '; already-split-week forced' else '' end;

  perform public._audit_insert(
    'contracts',
    v_cur.id::text,
    'CLONE_EXTEND',
    v_before_state,
    v_after_state,
    v_audit_reason,
    p_actor_user_id
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','canonical_audit_written',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'event','CLONE_EXTEND',
        'actor_user_id', coalesce(p_actor_user_id::text,'')
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- DEBUG AUDIT (single row per call; best-effort, never breaks flow)
  -- ─────────────────────────────────────────────────────────────
  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'predecessor_id', v_cur.id,
        'successor_id', v_succ.id,
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', v_boundary_week_start,
        'boundary_week_end', v_boundary_week_end,
        'end_we_old', v_end_we_old,
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_DEBUG',
        jsonb_build_object(
          'predecessor_id', v_cur.id::text,
          'successor_id', v_succ.id::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'warnings', jsonb_build_object(
            'schedule_clashes', v_schedule_clashes,
            'overlap_warnings', v_overlap_warnings
          )
        ),
        'contracts',
        ('contract:' || v_cur.id::text),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Return payload
  -- ─────────────────────────────────────────────────────────────
  return jsonb_build_object(
    'successor', jsonb_build_object(
      'id', v_succ.id,
      'candidate_id', v_succ.candidate_id,
      'client_id', v_succ.client_id,
      'role', v_succ.role,
      'band', v_succ.band,
      'display_site', v_succ.display_site,
      'ward_hint', v_succ.ward_hint,
      'start_date', v_succ.start_date,
      'end_date', v_succ.end_date,
      'timesheet_break_entry_mode',v_succ.timesheet_break_entry_mode
    ),
    'closed_at', v_close_to,
    'split', case
      when v_split_week then jsonb_build_object(
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'split_boundary_date', v_new_start,
        'worker_note', v_split_note,
        'split_group_key', v_split_group_key,
        'already_split', v_already_split
      )
      else null
    end,
    'overlap_warnings', v_overlap_warnings,
    'warnings', jsonb_build_object(
      'schedule_clashes', v_schedule_clashes,
      'overlap_warnings', v_overlap_warnings
    )
  );

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  begin
    get stacked diagnostics
      v_dbg_detail = PG_EXCEPTION_DETAIL,
      v_dbg_hint = PG_EXCEPTION_HINT,
      v_dbg_context = PG_EXCEPTION_CONTEXT;
  exception when others then
    v_dbg_detail := null;
    v_dbg_hint := null;
    v_dbg_context := null;
  end;

  if v_invoice_debug then
    begin
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','exception',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint
        )
      );

      v_dbg_stats := jsonb_build_object(
        'predecessor_id', coalesce(v_cur.id::text,''),
        'successor_id', coalesce(v_succ.id::text,''),
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', coalesce(v_boundary_week_start::text,''),
        'boundary_week_end', coalesce(v_boundary_week_end::text,''),
        'end_we_old', coalesce(v_end_we_old::text,''),
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_ERROR',
        jsonb_build_object(
          'predecessor_id', coalesce(v_cur.id::text,''),
          'successor_id', coalesce(v_succ.id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint,
          'context', v_dbg_context,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'contracts',
        ('contract:' || coalesce(p_contract_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;

ALTER FUNCTION public.contracts_clone_and_extend_atomic(uuid,date,date,date,boolean,uuid,text,jsonb,boolean,boolean,boolean,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.contracts_clone_and_extend_atomic(uuid,date,date,date,boolean,uuid,text,jsonb,boolean,boolean,boolean,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.contracts_clone_and_extend_atomic(uuid,date,date,date,boolean,uuid,text,jsonb,boolean,boolean,boolean,uuid) TO postgres, service_role;

CREATE OR REPLACE FUNCTION public.client_create_with_settings_v1(
  p_client_id uuid,
  p_client_json jsonb,
  p_actor_user_id uuid,
  p_settings_json jsonb DEFAULT '{}'::jsonb,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_client_payload jsonb := COALESCE(p_client_json, '{}'::jsonb);
  v_settings_payload jsonb := COALESCE(p_settings_json, '{}'::jsonb);

  v_client_input public.clients%ROWTYPE;
  v_settings_input public.client_settings%ROWTYPE;
  v_global public.settings_defaults%ROWTYPE;
  v_client public.clients%ROWTYPE;
  v_settings public.client_settings%ROWTYPE;
  v_existing_client public.clients%ROWTYPE;
  v_existing_settings public.client_settings%ROWTYPE;

  v_unknown_client_keys text[] := ARRAY[]::text[];
  v_unknown_settings_keys text[] := ARRAY[]::text[];
  v_client_mismatch_keys text[] := ARRAY[]::text[];
  v_settings_mismatch_keys text[] := ARRAY[]::text[];
  v_lock_acquired boolean := false;
  v_replay boolean := false;
  v_candidate_settings_enabled boolean := private._candidate_feature_enabled_current_v1('candidate_settings');
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ID_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ACTOR_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_ACTOR_REQUIRED')::text;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id
    AND COALESCE(actor_row.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ACTOR_INVALID'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_ACTOR_INVALID',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF jsonb_typeof(v_client_payload) <> 'object' THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF jsonb_typeof(v_settings_payload) <> 'object' THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_client_payload::text) > 65536 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYLOAD_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_PAYLOAD_TOO_LARGE',
              'max_bytes', 65536
            )::text;
  END IF;

  IF octet_length(v_settings_payload::text) > 131072 THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_PAYLOAD_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_PAYLOAD_TOO_LARGE',
              'max_bytes', 131072
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_each(v_client_payload) AS supplied_field(field_name, field_value)
    WHERE supplied_field.field_name IN ('name', 'vat_chargeable', 'payment_terms_days')
      AND supplied_field.field_value = 'null'::jsonb
  ) THEN
    RAISE EXCEPTION 'CLIENT_CREATE_REQUIRED_FIELD_NULL'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_REQUIRED_FIELD_NULL'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_each(v_settings_payload) AS supplied_field(field_name, field_value)
    WHERE (
      supplied_field.field_name IN (
      'effective_from',
      'hr_validation_required',
      'ts_reference_required',
      'autoprocess_hr',
      'pay_reference_required',
      'invoice_reference_required',
      'default_submission_mode',
      'is_nhsp',
      'self_bill_no_invoices_sent',
      'daily_calc_of_invoices',
      'no_timesheet_required',
      'group_nightsat_sunbh',
      'requires_hr',
      'hr_attach_to_invoice',
      'ts_attach_to_invoice',
      'auto_invoice_default',
      'send_manual_invoices_to_different_email',
      'invoice_consolidation_mode',
      'reference_number_required_to_issue_invoice',
      'opt_in_email',
      'opt_in_sms',
      'opt_in_whatsapp',
      'healthroster_import_auto_authorise',
      'nhsp_import_auto_authorise',
      'timesheet_break_entry_mode'
      )
      OR (v_candidate_settings_enabled AND supplied_field.field_name IN (
      'candidate_expenses_require_separate_timesheet',
      'candidate_paper_submission_enabled',
      'candidate_manager_approval_policy_json',
      'allow_daily_manager_authorise_on_phone',
      'allow_daily_manager_authorise_by_email'
      ))
    )
      AND supplied_field.field_value = 'null'::jsonb
  ) THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_REQUIRED_FIELD_NULL'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_REQUIRED_FIELD_NULL'
            )::text;
  END IF;

  SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
  INTO v_unknown_client_keys
  FROM jsonb_object_keys(v_client_payload) AS supplied_key(key_name)
  WHERE supplied_key.key_name <> ALL (
    ARRAY[
      'name',
      'invoice_address',
      'primary_invoice_email',
      'ap_phone',
      'vat_chargeable',
      'payment_terms_days',
      'mileage_charge_rate',
      'ts_queries_email',
      'client_address',
      'contact_title',
      'contact_known_as',
      'contact_forename',
      'contact_surname',
      'contact_job_title',
      'contact_tel',
      'contact_mobile',
      'contact_email',
      'website',
      'notes'
    ]::text[]
  );

  IF cardinality(v_unknown_client_keys) > 0 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_UNKNOWN_FIELDS'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_UNKNOWN_FIELDS',
              'unknown_fields', to_jsonb(v_unknown_client_keys)
            )::text;
  END IF;

  SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
  INTO v_unknown_settings_keys
  FROM jsonb_object_keys(v_settings_payload) AS supplied_key(key_name)
  WHERE supplied_key.key_name <> ALL (
    ARRAY[
      'timezone_id',
      'day_start',
      'day_end',
      'night_start',
      'night_end',
      'bh_source',
      'bh_list',
      'bh_feed_url',
      'vat_rate_pct',
      'holiday_pay_pct',
      'erni_pct',
      'apply_holiday_to',
      'apply_erni_to',
      'margin_includes',
      'effective_from',
      'hr_validation_required',
      'ts_reference_required',
      'week_ending_weekday',
      'autoprocess_hr',
      'pay_reference_required',
      'invoice_reference_required',
      'default_submission_mode',
      'sat_start',
      'sat_end',
      'sun_start',
      'sun_end',
      'is_nhsp',
      'self_bill_no_invoices_sent',
      'daily_calc_of_invoices',
      'no_timesheet_required',
      'group_nightsat_sunbh',
      'requires_hr',
      'hr_attach_to_invoice',
      'ts_attach_to_invoice',
      'bh_start',
      'bh_end',
      'auto_invoice_default',
      'send_manual_invoices_to_different_email',
      'manual_invoices_alt_email_address',
      'invoice_consolidation_mode',
      'reference_number_required_to_issue_invoice',
      'opt_in_email',
      'opt_in_sms',
      'opt_in_whatsapp',
      'healthroster_import_auto_authorise',
      'nhsp_import_auto_authorise',
      'reversal_complete_financials_date',
      'reversal_replacement_financials_date',
      'timesheet_break_entry_mode'
    ]::text[] || CASE WHEN v_candidate_settings_enabled THEN ARRAY[
      'candidate_electronic_auto_authorise',
      'candidate_expenses_require_separate_timesheet',
      'candidate_paper_submission_enabled',
      'candidate_expense_invoice_email',
      'candidate_manager_approval_policy_json',
      'allow_daily_manager_authorise_on_phone',
      'allow_daily_manager_authorise_by_email'
    ]::text[] ELSE ARRAY[]::text[] END
  );

  IF cardinality(v_unknown_settings_keys) > 0 THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_UNKNOWN_FIELDS'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_UNKNOWN_FIELDS',
              'unknown_fields', to_jsonb(v_unknown_settings_keys)
            )::text;
  END IF;

  SELECT *
  INTO v_client_input
  FROM jsonb_populate_record(NULL::public.clients, v_client_payload);

  SELECT *
  INTO v_settings_input
  FROM jsonb_populate_record(NULL::public.client_settings, v_settings_payload);

  IF v_candidate_settings_enabled
     AND v_settings_payload ? 'candidate_manager_approval_policy_json' THEN
    v_settings_input.candidate_manager_approval_policy_json :=
      private._candidate_normalize_manager_policy_v1(
        v_settings_input.candidate_manager_approval_policy_json
      );
  END IF;

  PERFORM public._ctms_assert_import_correction_settings_write_v1(
    COALESCE(v_settings_input.is_nhsp, false),
    COALESCE(v_settings_input.requires_hr, false),
    COALESCE(v_settings_input.no_timesheet_required, false),
    CASE WHEN v_settings_payload ? 'reversal_complete_financials_date'
      THEN v_settings_input.reversal_complete_financials_date
      ELSE NULL::public.correction_financials_date_basis_enum
    END,
    CASE WHEN v_settings_payload ? 'reversal_replacement_financials_date'
      THEN v_settings_input.reversal_replacement_financials_date
      ELSE NULL::public.correction_financials_date_basis_enum
    END
  );

  IF NULLIF(BTRIM(COALESCE(v_client_input.name, '')), '') IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NAME_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_NAME_REQUIRED')::text;
  END IF;

  IF char_length(v_client_input.name) > 500 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NAME_TOO_LONG'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_NAME_TOO_LONG',
              'max_characters', 500
            )::text;
  END IF;

  IF COALESCE(v_client_input.payment_terms_days, 30) < 0
     OR COALESCE(v_client_input.payment_terms_days, 30) > 365 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYMENT_TERMS_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_PAYMENT_TERMS_OUT_OF_RANGE',
              'min', 0,
              'max', 365
            )::text;
  END IF;

  IF v_client_input.mileage_charge_rate IS NOT NULL
     AND v_client_input.mileage_charge_rate < 0 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_MILEAGE_RATE_NEGATIVE'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.primary_invoice_email IS NOT NULL
     AND char_length(v_client_input.primary_invoice_email) > 320 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PRIMARY_EMAIL_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.contact_email IS NOT NULL
     AND char_length(v_client_input.contact_email) > 320 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_CONTACT_EMAIL_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.notes IS NOT NULL
     AND char_length(v_client_input.notes) > 20000 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NOTES_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  IF v_candidate_settings_enabled AND jsonb_typeof(COALESCE(
       v_settings_input.candidate_manager_approval_policy_json,
       '{}'::jsonb
     )) <> 'object' THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_CANDIDATE_MANAGER_POLICY_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF v_candidate_settings_enabled
     AND COALESCE(v_settings_input.allow_daily_manager_authorise_on_phone, true) = false
     AND COALESCE(v_settings_input.allow_daily_manager_authorise_by_email, false) = false THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_DAILY_MANAGER_METHOD_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_candidate_settings_enabled
     AND (
       COALESCE(v_settings_input.is_nhsp, false)
       OR (
         COALESCE(v_settings_input.requires_hr, false)
         AND COALESCE(v_settings_input.no_timesheet_required, false)
       )
     ) THEN
    IF NOT COALESCE(v_settings_input.candidate_expenses_require_separate_timesheet, false) THEN
      RAISE EXCEPTION 'CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED'
        USING ERRCODE = '22023';
    END IF;
    IF NULLIF(BTRIM(v_settings_input.candidate_expense_invoice_email), '') IS NULL
       OR char_length(BTRIM(v_settings_input.candidate_expense_invoice_email)) > 320
       OR BTRIM(v_settings_input.candidate_expense_invoice_email)
          !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN
      RAISE EXCEPTION 'CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED'
        USING ERRCODE = '22023';
    END IF;
  END IF;

  SELECT settings_row.*
  INTO v_global
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  FOR SHARE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CLIENT_CREATE_GLOBAL_SETTINGS_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_candidate_settings_enabled
     AND COALESCE((COALESCE(
       v_settings_input.candidate_manager_approval_policy_json,
       '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":false}'::jsonb
     )->>'allow_free_business_email')::boolean, false)
     AND (
       jsonb_typeof(v_global.candidate_barred_manager_email_domains) <> 'array'
       OR jsonb_array_length(v_global.candidate_barred_manager_email_domains) = 0
     ) THEN
    RAISE EXCEPTION 'CANDIDATE_BARRED_MANAGER_DOMAIN_POLICY_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  v_lock_acquired := pg_try_advisory_xact_lock(
    hashtextextended(
      'CLIENT_CREATE_WITH_SETTINGS|' || p_client_id::text,
      24062026
    )
  );

  IF NOT v_lock_acquired THEN
    RAISE EXCEPTION 'CLIENT_CREATE_LOCK_BUSY'
      USING ERRCODE = '55P03',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_LOCK_BUSY',
              'client_id', p_client_id::text,
              'retryable', true
            )::text;
  END IF;

  SELECT client_row.*
  INTO v_existing_client
  FROM public.clients AS client_row
  WHERE client_row.id = p_client_id
  FOR UPDATE;

  IF FOUND THEN
    SELECT settings_row.*
    INTO v_existing_settings
    FROM public.client_settings AS settings_row
    WHERE settings_row.client_id = p_client_id
    ORDER BY
      settings_row.effective_from DESC NULLS LAST,
      settings_row.updated_at DESC,
      settings_row.id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CLIENT_CREATE_REPLAY_SETTINGS_MISSING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'CLIENT_CREATE_REPLAY_SETTINGS_MISSING',
                'client_id', p_client_id::text
              )::text;
    END IF;

    SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
    INTO v_client_mismatch_keys
    FROM jsonb_object_keys(v_client_payload) AS requested_key(key_name)
    WHERE (to_jsonb(v_existing_client) -> requested_key.key_name)
          IS DISTINCT FROM
          (to_jsonb(v_client_input) -> requested_key.key_name);

    SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
    INTO v_settings_mismatch_keys
    FROM jsonb_object_keys(v_settings_payload) AS requested_key(key_name)
    WHERE (to_jsonb(v_existing_settings) -> requested_key.key_name)
          IS DISTINCT FROM
          (to_jsonb(v_settings_input) -> requested_key.key_name);

    IF cardinality(v_client_mismatch_keys) > 0
       OR cardinality(v_settings_mismatch_keys) > 0 THEN
      RAISE EXCEPTION 'CLIENT_CREATE_IDEMPOTENCY_CONFLICT'
        USING ERRCODE = '23505',
              DETAIL = jsonb_build_object(
                'code', 'CLIENT_CREATE_IDEMPOTENCY_CONFLICT',
                'client_id', p_client_id::text,
                'client_mismatch_fields', to_jsonb(v_client_mismatch_keys),
                'settings_mismatch_fields', to_jsonb(v_settings_mismatch_keys)
              )::text;
    END IF;

    v_replay := true;
    v_client := v_existing_client;
    v_settings := v_existing_settings;
  ELSE
    INSERT INTO public.clients (
      id,
      name,
      invoice_address,
      primary_invoice_email,
      ap_phone,
      vat_chargeable,
      payment_terms_days,
      created_at,
      updated_at,
      mileage_charge_rate,
      ts_queries_email,
      client_address,
      contact_title,
      contact_known_as,
      contact_forename,
      contact_surname,
      contact_job_title,
      contact_tel,
      contact_mobile,
      contact_email,
      website,
      notes
    )
    VALUES (
      p_client_id,
      BTRIM(v_client_input.name),
      v_client_input.invoice_address,
      v_client_input.primary_invoice_email,
      v_client_input.ap_phone,
      COALESCE(v_client_input.vat_chargeable, true),
      COALESCE(v_client_input.payment_terms_days, 30),
      v_now,
      v_now,
      v_client_input.mileage_charge_rate,
      v_client_input.ts_queries_email,
      v_client_input.client_address,
      v_client_input.contact_title,
      v_client_input.contact_known_as,
      v_client_input.contact_forename,
      v_client_input.contact_surname,
      v_client_input.contact_job_title,
      v_client_input.contact_tel,
      v_client_input.contact_mobile,
      v_client_input.contact_email,
      v_client_input.website,
      v_client_input.notes
    )
    RETURNING *
    INTO v_client;

    INSERT INTO public.client_settings (
      client_id,
      timezone_id,
      day_start,
      day_end,
      night_start,
      night_end,
      bh_source,
      bh_list,
      bh_feed_url,
      vat_rate_pct,
      holiday_pay_pct,
      erni_pct,
      apply_holiday_to,
      apply_erni_to,
      margin_includes,
      effective_from,
      created_at,
      updated_at,
      hr_validation_required,
      ts_reference_required,
      week_ending_weekday,
      autoprocess_hr,
      pay_reference_required,
      invoice_reference_required,
      default_submission_mode,
      sat_start,
      sat_end,
      sun_start,
      sun_end,
      is_nhsp,
      self_bill_no_invoices_sent,
      daily_calc_of_invoices,
      no_timesheet_required,
      group_nightsat_sunbh,
      requires_hr,
      hr_attach_to_invoice,
      ts_attach_to_invoice,
      bh_start,
      bh_end,
      auto_invoice_default,
      send_manual_invoices_to_different_email,
      manual_invoices_alt_email_address,
      invoice_consolidation_mode,
      reference_number_required_to_issue_invoice,
      opt_in_email,
      opt_in_sms,
      opt_in_whatsapp,
      healthroster_import_auto_authorise,
      nhsp_import_auto_authorise,
      reversal_complete_financials_date,
      reversal_replacement_financials_date,
      timesheet_break_entry_mode,
      candidate_electronic_auto_authorise,
      candidate_expenses_require_separate_timesheet,
      candidate_paper_submission_enabled,
      candidate_expense_invoice_email,
      candidate_manager_approval_policy_json,
      allow_daily_manager_authorise_on_phone,
      allow_daily_manager_authorise_by_email
    )
    VALUES (
      p_client_id,
      CASE WHEN v_settings_payload ? 'timezone_id'
        THEN v_settings_input.timezone_id ELSE v_global.timezone_id END,
      CASE WHEN v_settings_payload ? 'day_start'
        THEN v_settings_input.day_start ELSE v_global.day_start END,
      CASE WHEN v_settings_payload ? 'day_end'
        THEN v_settings_input.day_end ELSE v_global.day_end END,
      CASE WHEN v_settings_payload ? 'night_start'
        THEN v_settings_input.night_start ELSE v_global.night_start END,
      CASE WHEN v_settings_payload ? 'night_end'
        THEN v_settings_input.night_end ELSE v_global.night_end END,
      CASE WHEN v_settings_payload ? 'bh_source'
        THEN v_settings_input.bh_source ELSE v_global.bh_source END,
      CASE WHEN v_settings_payload ? 'bh_list'
        THEN v_settings_input.bh_list ELSE v_global.bh_list END,
      CASE WHEN v_settings_payload ? 'bh_feed_url'
        THEN v_settings_input.bh_feed_url ELSE v_global.bh_feed_url END,
      v_settings_input.vat_rate_pct,
      v_settings_input.holiday_pay_pct,
      v_settings_input.erni_pct,
      v_settings_input.apply_holiday_to,
      v_settings_input.apply_erni_to,
      v_settings_input.margin_includes,
      COALESCE(
        v_settings_input.effective_from,
        (v_now AT TIME ZONE 'Europe/London')::date
      ),
      v_now,
      v_now,
      COALESCE(v_settings_input.hr_validation_required, false),
      CASE WHEN v_settings_payload ? 'ts_reference_required'
        THEN COALESCE(v_settings_input.ts_reference_required, false)
        ELSE COALESCE(v_global.ts_reference_required, false)
      END,
      v_settings_input.week_ending_weekday,
      COALESCE(v_settings_input.autoprocess_hr, false),
      COALESCE(v_settings_input.pay_reference_required, false),
      COALESCE(v_settings_input.invoice_reference_required, false),
      COALESCE(
        v_settings_input.default_submission_mode,
        'ELECTRONIC'::public.submission_mode_enum
      ),
      CASE WHEN v_settings_payload ? 'sat_start'
        THEN v_settings_input.sat_start ELSE v_global.sat_start END,
      CASE WHEN v_settings_payload ? 'sat_end'
        THEN v_settings_input.sat_end ELSE v_global.sat_end END,
      CASE WHEN v_settings_payload ? 'sun_start'
        THEN v_settings_input.sun_start ELSE v_global.sun_start END,
      CASE WHEN v_settings_payload ? 'sun_end'
        THEN v_settings_input.sun_end ELSE v_global.sun_end END,
      COALESCE(v_settings_input.is_nhsp, false),
      COALESCE(v_settings_input.self_bill_no_invoices_sent, false),
      COALESCE(v_settings_input.daily_calc_of_invoices, false),
      COALESCE(v_settings_input.no_timesheet_required, false),
      COALESCE(v_settings_input.group_nightsat_sunbh, false),
      COALESCE(v_settings_input.requires_hr, false),
      CASE WHEN v_settings_payload ? 'hr_attach_to_invoice'
        THEN COALESCE(v_settings_input.hr_attach_to_invoice, true)
        ELSE COALESCE(v_global.hr_attach_to_invoice, true)
      END,
      CASE WHEN v_settings_payload ? 'ts_attach_to_invoice'
        THEN COALESCE(v_settings_input.ts_attach_to_invoice, true)
        ELSE COALESCE(v_global.ts_attach_to_invoice, true)
      END,
      CASE WHEN v_settings_payload ? 'bh_start'
        THEN v_settings_input.bh_start ELSE v_global.bh_start END,
      CASE WHEN v_settings_payload ? 'bh_end'
        THEN v_settings_input.bh_end ELSE v_global.bh_end END,
      COALESCE(v_settings_input.auto_invoice_default, false),
      COALESCE(
        v_settings_input.send_manual_invoices_to_different_email,
        false
      ),
      v_settings_input.manual_invoices_alt_email_address,
      COALESCE(
        v_settings_input.invoice_consolidation_mode,
        'NONE'::public.invoice_consolidation_mode_enum
      ),
      COALESCE(
        v_settings_input.reference_number_required_to_issue_invoice,
        false
      ),
      COALESCE(v_settings_input.opt_in_email, true),
      COALESCE(v_settings_input.opt_in_sms, true),
      COALESCE(v_settings_input.opt_in_whatsapp, true),
      CASE WHEN v_settings_payload ? 'healthroster_import_auto_authorise'
        THEN COALESCE(
          v_settings_input.healthroster_import_auto_authorise,
          v_global.healthroster_import_auto_authorise_default
        )
        ELSE v_global.healthroster_import_auto_authorise_default
      END,
      CASE WHEN v_settings_payload ? 'nhsp_import_auto_authorise'
        THEN COALESCE(
          v_settings_input.nhsp_import_auto_authorise,
          v_global.nhsp_import_auto_authorise_default
        )
        ELSE v_global.nhsp_import_auto_authorise_default
      END,
      CASE WHEN (
        COALESCE(v_settings_input.is_nhsp, false) = true
        OR (
          COALESCE(v_settings_input.requires_hr, false) = true
          AND COALESCE(v_settings_input.no_timesheet_required, false) = true
        )
      ) AND v_settings_payload ? 'reversal_complete_financials_date'
        THEN v_settings_input.reversal_complete_financials_date
        ELSE NULL::public.correction_financials_date_basis_enum
      END,
      CASE WHEN (
        COALESCE(v_settings_input.is_nhsp, false) = true
        OR (
          COALESCE(v_settings_input.requires_hr, false) = true
          AND COALESCE(v_settings_input.no_timesheet_required, false) = true
        )
      ) AND v_settings_payload ? 'reversal_replacement_financials_date'
        THEN v_settings_input.reversal_replacement_financials_date
        ELSE NULL::public.correction_financials_date_basis_enum
      END,
      COALESCE(
        v_settings_input.timesheet_break_entry_mode,
        'START_END_TIMES'::public.timesheet_break_entry_mode_enum
      ),
      CASE WHEN v_candidate_settings_enabled AND v_settings_payload ? 'candidate_electronic_auto_authorise'
        THEN v_settings_input.candidate_electronic_auto_authorise
        ELSE NULL::boolean END,
      CASE WHEN v_candidate_settings_enabled
        THEN COALESCE(v_settings_input.candidate_expenses_require_separate_timesheet, false)
        ELSE false END,
      CASE WHEN v_candidate_settings_enabled
        THEN COALESCE(v_settings_input.candidate_paper_submission_enabled, false)
        ELSE false END,
      CASE WHEN v_candidate_settings_enabled
        THEN NULLIF(BTRIM(v_settings_input.candidate_expense_invoice_email), '')
        ELSE NULL::text END,
      CASE WHEN v_candidate_settings_enabled THEN COALESCE(
          v_settings_input.candidate_manager_approval_policy_json,
          '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":false}'::jsonb
        )
        ELSE '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":false}'::jsonb
      END,
      CASE WHEN v_candidate_settings_enabled
        THEN COALESCE(v_settings_input.allow_daily_manager_authorise_on_phone, true)
        ELSE true END,
      CASE WHEN v_candidate_settings_enabled
        THEN COALESCE(v_settings_input.allow_daily_manager_authorise_by_email, false)
        ELSE false END
    )
    RETURNING *
    INTO v_settings;

    PERFORM public._inv_write_audit(
      p_actor_user_id,
      'CLIENT_CREATED_WITH_SETTINGS',
      jsonb_build_object(
        'client_id', v_client.id::text,
        'client_ref', v_client.cli_ref,
        'client_name', v_client.name,
        'client_settings_id', v_settings.id::text,
        'healthroster_import_auto_authorise',
          v_settings.healthroster_import_auto_authorise,
        'nhsp_import_auto_authorise',
          v_settings.nhsp_import_auto_authorise,
        'correction_policy_eligible',
          (v_settings.is_nhsp OR (v_settings.requires_hr AND v_settings.no_timesheet_required)),
        'reversal_complete_financials_date_override',
          v_settings.reversal_complete_financials_date,
        'reversal_complete_financials_date_effective',
          COALESCE(v_settings.reversal_complete_financials_date, v_global.reversal_complete_financials_date),
        'reversal_replacement_financials_date_override',
          v_settings.reversal_replacement_financials_date,
        'reversal_replacement_financials_date_effective',
          COALESCE(v_settings.reversal_replacement_financials_date, v_global.reversal_replacement_financials_date),
        'correction_setting_source',
          CASE WHEN v_settings.reversal_complete_financials_date IS NULL
                 AND v_settings.reversal_replacement_financials_date IS NULL
            THEN 'GLOBAL' ELSE 'CLIENT_OR_MIXED' END,
        'copied_global_settings_updated_at', v_global.updated_at
      ),
      'client',
      v_client.id::text,
      NULL::jsonb,
      'Atomic client and initial settings creation',
      NULL::text,
      NULL::text,
      'client-create-with-settings:' || v_client.id::text
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'replay', v_replay,
    'client', to_jsonb(v_client),
    'client_settings', to_jsonb(v_settings),
    'policy', jsonb_build_object(
      'healthroster', public.import_auto_authorise_policy_resolve_v1(
        'HEALTHROSTER'::public.hr_source_enum,
        v_client.id,
        NULL::uuid,
        false
      ),
      'nhsp', public.import_auto_authorise_policy_resolve_v1(
        'NHSP'::public.hr_source_enum,
        v_client.id,
        NULL::uuid,
        false
      )
    )
  );
END;
$function$;


commit;
