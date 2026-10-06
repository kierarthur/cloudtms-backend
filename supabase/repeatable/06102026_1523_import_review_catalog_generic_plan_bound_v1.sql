-- Bound the Import Review classification plan without changing its decisions.
-- The preceding repository-derived PostgreSQL 17 definition is preserved in full.
-- Only classified AS MATERIALIZED changes: classify each bounded source row once,
-- then reuse those rows instead of expanding its lateral joins into downstream plans.
-- No timeout, plan-cache setting, source/pay/invoice policy or action limit changes.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public._import_review_action_catalog_core_v1(p_import_id uuid, p_preview_generation integer, p_max_actions integer DEFAULT 5000)
 RETURNS TABLE(action_id text, action_kind text, action_category text, target_key text, source_identity text, hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid, contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean, default_selected boolean, blocking boolean, summary_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
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
  ), classified as materialized (
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
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json,
        workflow.state workflow_state,
        workflow.worker_submitted_at_utc,
        workflow.manager_approved_at_utc,
        workflow.updated_at_utc workflow_updated_at_utc
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      left join lateral (
        select w.state,w.worker_submitted_at_utc,w.manager_approved_at_utc,w.updated_at_utc
        from public.candidate_submission_workflows w
        where w.candidate_id=p.candidate_id
          and w.contract_id=d.contract_id
          and w.week_ending_date=nullif(p.row_json->>'week_ending_date','')::date
          and w.scope='WEEKLY'
          and w.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
          and w.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REFUSED','REJECTED')
        order by w.generation desc,w.updated_at_utc desc,w.id desc
        limit 1
      ) workflow on true
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), missing_timesheet_status as (
      select m.*,
        case
          when m.workflow_state in ('READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL')
            and m.manager_approved_at_utc is null
            then 'WEEKLY_TIMESHEET_AWAITING_MANAGER_APPROVAL'
          when m.workflow_state is null or m.workflow_state in ('CREATED','WORKER_DRAFT')
            then 'WEEKLY_TIMESHEET_NOT_SUBMITTED'
          else 'WEEKLY_TIMESHEET_COMPLETING'
        end missing_reason_code,
        case
          when m.workflow_state in ('READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL')
            and m.manager_approved_at_utc is null
            then 'MANAGER_APPROVAL_PENDING'
          when m.workflow_state is null or m.workflow_state in ('CREATED','WORKER_DRAFT')
            then 'TIMESHEET_NOT_SUBMITTED'
          else 'TIMESHEET_COMPLETION_PENDING'
        end missing_difference_code,
        case
          when m.workflow_state in ('READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL')
            and m.manager_approved_at_utc is null
            then 'Waiting for manager approval'
          when m.workflow_state is null or m.workflow_state in ('CREATED','WORKER_DRAFT')
            then 'Waiting for candidate to submit'
          else 'Waiting for completed timesheet'
        end missing_outcome_label
      from missing_timesheets m
    ), confirmed_exceptions as (
      select p.*,cx.value exception_json
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        m.missing_reason_code,m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-incomplete',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-incomplete-v3',
        m.shift_evidence_fingerprint,m.row_json::text,m.workflow_state,m.workflow_updated_at_utc)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code',m.missing_reason_code,'source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'candidate_workflow_state',m.workflow_state,
        'difference_codes',jsonb_build_array(m.missing_difference_code),
        'outcome_label',m.missing_outcome_label))
    from missing_timesheet_status m
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
            coalesce(cx.value->>'match_status','MATCH') <> 'MATCH'
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') <> 'MATCH'
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)),'[]'::jsonb) email_failure_reasons
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
          where coalesce(cx.value->>'match_status','MATCH') <> 'MATCH'
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
$function$
;

alter function public._import_review_action_catalog_core_v1(uuid,integer,integer) owner to postgres;
revoke all on function public._import_review_action_catalog_core_v1(uuid,integer,integer)
  from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
