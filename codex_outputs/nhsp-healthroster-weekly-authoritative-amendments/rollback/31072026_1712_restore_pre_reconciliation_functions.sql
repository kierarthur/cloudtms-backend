-- TEST-only rollback for the NHSP/HealthRoster Weekly Authoritative Amendments implementation.
-- Generated from backend Git HEAD 7f951085980b7cdd42c767b43cdf788120b4e991.
-- This does not reverse already committed import operations or timesheet data.
BEGIN;

-- Restore _import_review_action_catalog_core_v1 from backend Git HEAD before this implementation.
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
  ), evidenced as (
    select c.*,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
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
      case when m.action_kind='ADVISORY' or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
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
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and m.mutable_replacement_timesheet_id is not null
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and (
              coalesce((m.protection->>'paid')::boolean,false)
              or coalesce((m.protection->>'invoice_locked')::boolean,false)
            )
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'mutable_replacement_timesheet_id',m.mutable_replacement_timesheet_id,
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
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end
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
            and m.mutable_replacement_timesheet_id is not null
            then 'TMS will amend replacement shift'
          when m.action_kind='APPLY_AMENDMENT' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift and create replacement shift' else 'TMS will amend shift' end
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
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v1',i.issue_fingerprint,i.protection::text,
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
        'WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-shift-absent',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      null::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-shift-absent-v1',o.timesheet_id,o.comparison_json::text)),
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Candidate timesheet states they did not work this shift')
    from omitted_shifts o;

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
        coalesce((select jsonb_agg(cx.value order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
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
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
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
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
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
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
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

-- Restore _import_review_apply_envelope_core_v1 from backend Git HEAD before this implementation.
create or replace function public._import_review_apply_envelope_core_v1(p_import_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_import public.hr_imports%rowtype;
  v_selected_ids text[];
  v_invalidation_ids text[];
  v_correction_units jsonb;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(r.action_id order by r.action_id),array[]::text[])
  into v_selected_ids from public._import_review_ready_action_ids_core_v1(p_import_id) r;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(jsonb_build_object(
    'action_id',d.action_id,'root_timesheet_id',d.timesheet_id,'source_row_key',d.source_identity,
    'correction_action',case when d.action_kind='APPLY_AMENDMENT' then 'CHANGED_HOURS' else 'CANCELLATION' end,
    'correction_shape',case when d.action_kind='APPLY_AMENDMENT' then 'REVERSAL_REPLACEMENT' else 'REVERSAL_ONLY' end
  ) order by d.action_id),'[]'::jsonb)
  into v_correction_units
  from public.import_review_decisions d
  cross join lateral (
    select public._import_review_timesheet_protection_core_v1(d.timesheet_id) as protection
  ) pr
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select a.import_authoritative
      from public._import_review_effective_authority_core_v1(
        case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
        d.contract_id,d.client_id,coalesce(d.summary_json->>'work_date',d.summary_json->>'week_ending_date')::date) a),false);
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units,
    'batch_scope_units',coalesce((select jsonb_agg(jsonb_build_object(
      'candidate_id',u.candidate_id,'client_id',u.client_id) order by u.candidate_id,u.client_id)
      from (select distinct d.candidate_id,d.client_id from public.import_review_decisions d
        where d.import_id=p_import_id and d.action_id=any(v_selected_ids)) u),'[]'::jsonb),
    'deferred_action_count',(select count(*) from public.import_review_decisions d
      where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected));
end
$function$;

-- Restore import_review_apply_guard_v1 from backend Git HEAD before this implementation.
create or replace function public.import_review_apply_guard_v1(
  p_import_id uuid,p_expected_state_version bigint,p_expected_coverage_fingerprint text,p_expected_preview_fingerprint text,
  p_operation_id uuid,p_request_hash text,p_selected_action_ids jsonb,p_reference_invalidation_action_ids jsonb,p_actor_user_id uuid
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare v_s public.import_review_states%rowtype; v_i public.hr_imports%rowtype; v_o public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_invalidation_ids text[]; v_db_invalidation_ids text[];
  v_op_result jsonb; v_envelope jsonb; v_server_hash text; v_fresh_fingerprint text;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
    or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or jsonb_typeof(coalesce(p_reference_invalidation_action_ids,'[]'))<>'array'
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>5000
    or jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]'))>5000
    or pg_column_size(coalesce(p_selected_action_ids,'[]'))+pg_column_size(coalesce(p_reference_invalidation_action_ids,'[]'))>2097152
  then raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(coalesce(p_selected_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
    or exists(select 1 from jsonb_array_elements(coalesce(p_reference_invalidation_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
  then raise exception 'IMPORT_REVIEW_ACTION_ID_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_ids from jsonb_array_elements_text(coalesce(p_selected_action_ids,'[]'))value;
  if cardinality(v_ids)<>jsonb_array_length(coalesce(p_selected_action_ids,'[]')) then raise exception 'IMPORT_REVIEW_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_invalidation_ids
  from jsonb_array_elements_text(coalesce(p_reference_invalidation_action_ids,'[]')) value;
  if cardinality(v_invalidation_ids)<>jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]')) then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select * into v_i from public.hr_imports where id=p_import_id for update; select * into v_s from public.import_review_states where import_id=p_import_id for update;
  if v_i.id is null or v_s.import_id is null then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id;
  if found and v_o.committed_at_utc is not null then
    if lower(btrim(p_request_hash))<>v_o.request_hash then raise exception 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'operation_id',p_operation_id,
      'state_version',v_s.state_version,'operation_state',v_o.state,'stored_response',v_o.response_json);
  end if;
  if v_s.status not in ('BLOCKED','READY') or v_s.follow_up_status not in ('NOT_REQUIRED','COMPLETE')
    or v_s.state_version<>p_expected_state_version or v_i.coverage_fingerprint is distinct from p_expected_coverage_fingerprint
    or v_s.preview_fingerprint is distinct from p_expected_preview_fingerprint then raise exception 'IMPORT_REVIEW_APPLY_STALE_OR_NOT_READY' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_ids
  from public._import_review_ready_action_ids_core_v1(p_import_id);
  if cardinality(v_db_ids)=0 then raise exception 'IMPORT_REVIEW_NO_READY_SELECTED_ACTIONS' using errcode='55000'; end if;
  if v_ids is distinct from v_db_ids then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_invalidation_ids
  from public.import_review_decisions where import_id=p_import_id and is_current and action_id=any(v_ids)
    and action_kind='INVALIDATE_REFERENCE';
  if v_invalidation_ids is distinct from v_db_invalidation_ids then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_SET_MISMATCH' using errcode='40001'; end if;

  -- Deterministically lock the selected timesheet scope before the final
  -- protection read. Banking Pay keeps its own central freshness checks; these
  -- locks only ensure that a concurrent draft/import race has one winner.
  perform 1 from public.timesheets t
  where t.timesheet_id in (select d.timesheet_id from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids) and d.timesheet_id is not null)
  order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf
  where tf.is_current and tf.timesheet_id in (select d.timesheet_id from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids) and d.timesheet_id is not null)
  order by tf.timesheet_id,tf.id for update;

  create temporary table if not exists pg_temp.review_apply_fresh_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500) with no data;
  truncate pg_temp.review_apply_fresh_actions;
  insert into pg_temp.review_apply_fresh_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500);
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fresh_fingerprint from pg_temp.review_apply_fresh_actions;
  if v_fresh_fingerprint is distinct from v_s.preview_fingerprint then
    raise exception 'IMPORT_REVIEW_APPLY_EVIDENCE_STALE' using errcode='40001'; end if;
  if exists(
    select 1 from pg_temp.review_apply_fresh_actions b
    where b.blocking and exists (
      select 1 from public.import_review_decisions d
      where d.import_id=p_import_id and d.action_id=any(v_ids)
        and b.candidate_id=d.candidate_id
        and b.client_id=d.client_id
    )
  ) then raise exception 'IMPORT_REVIEW_APPLY_REFRESH_REQUIRED' using errcode='40001'; end if;
  if exists(
    select 1 from public.import_review_decisions d
    left join pg_temp.review_apply_fresh_actions n on n.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
      and (n.action_id is null or n.evidence_fingerprint is distinct from d.evidence_fingerprint
        or not n.selectable or n.blocking)
  ) then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001'; end if;
  if exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR') and d.timesheet_id is not null
    and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'active_pay_draft')::boolean,false)) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if v_i.source_system='HEALTHROSTER_DAILY'::public.hr_source_enum and exists(
    select 1 from public.import_review_daily_timesheet_resolutions r
    where r.import_id=p_import_id and r.status='CURRENT' and r.resolved_timesheet_id is not null
      and exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id
        and d.hr_row_id=r.hr_row_id and d.action_id=any(v_ids))
      and coalesce((public._import_review_timesheet_protection_core_v1(r.resolved_timesheet_id)->>'active_pay_draft')::boolean,false)
  ) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if exists(select 1 from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids) and d.action_kind='INVALIDATE_REFERENCE'
      and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'protected')::boolean,false)) then
    raise exception 'IMPORT_REVIEW_REFERENCE_INVALIDATION_PROTECTED' using errcode='55000'; end if;
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  v_server_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(p_request_hash))<>v_server_hash then
    raise exception 'IMPORT_REVIEW_APPLY_REQUEST_HASH_MISMATCH' using errcode='22023',detail=jsonb_build_object('server_request_hash',v_server_hash)::text;
  end if;
  v_op_result:=public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,v_i.source_system,
    concat_ws(':',coalesce(v_i.revision_group_id,v_i.id),coalesce(v_i.revision_no,1)),v_server_hash,p_actor_user_id,v_envelope);
  update public.import_review_states set status='APPLYING',state_version=state_version+1,last_operation_id=p_operation_id,updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json) values(p_import_id,v_s.state_version,p_operation_id,'APPLY_STARTED',p_actor_user_id,jsonb_build_object('request_hash',btrim(p_request_hash),'selected_count',cardinality(v_ids),'batch_scope_units',v_envelope->'batch_scope_units'));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'state_version',v_s.state_version,'selected_action_ids',to_jsonb(v_ids),'operation_state',v_op_result->>'state'); end $function$;

-- Restore timesheet_unauthorise_bulk_atomic from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'UNAUTHORISE', p_actor_user_id, 100);
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_results;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.sheet_scope AS current_sheet_scope,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    cw.id AS contract_week_id,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock
  FROM pg_temp.timesheet_unauthorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (SELECT 1 FROM public.timesheets AS cw_ts WHERE cw_ts.timesheet_id = cw_sel.timesheet_id AND cw_ts.booking_id = cur_ts.booking_id)
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END, cw_sel.updated_at DESC NULLS LAST, cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON cur_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NULL AND state_rows.tsfin_authorised_at_utc IS NULL THEN 'ALREADY_UNAUTHORISED'
      ELSE NULL::text
    END AS failure_code,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum AS new_processing_status
  FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = NULL,
           authorised_at_utc = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET timesheet_id = work_rows.current_timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_unauthorise:' || v_now::text,
    'TIMESHEET_BULK_UNAUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_UNAUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_unauthorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'UNAUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'UNAUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', v_success_count, 'authorised_eligible', -v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

-- Restore timesheet_authorise_bulk_atomic from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'AUTHORISE', p_actor_user_id, 100);
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.qr_status AS current_qr_status,
    cur_ts.qr_token AS current_qr_token,
    cur_ts.qr_generated_at AS current_qr_generated_at,
    cur_ts.qr_scanned_at AS current_qr_scanned_at,
    cur_ts.sheet_scope AS current_sheet_scope,
    cur_ts.contract_id AS current_contract_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.basis AS tsfin_basis,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    COALESCE(summary_row.client_requires_hr, contract_row.requires_hr, false) AS client_requires_hr,
    COALESCE(summary_row.hr_validation_required_for_invoice, contract_row.requires_hr, false) AS hr_validation_required_for_invoice,
    COALESCE(summary_row.validation_status_text, NULL::text) AS summary_validation_status,
    cw.id AS contract_week_id,
    cw.status AS contract_week_status,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock,
    COALESCE(validation_state.validation_ok, false) AS validation_ok
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT c.requires_hr
    FROM public.contracts AS c
    WHERE c.id = cur_ts.contract_id
    LIMIT 1
  ) AS contract_row ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(vts.client_requires_hr, false) AS client_requires_hr,
      COALESCE(vts.hr_validation_required_for_invoice, false) AS hr_validation_required_for_invoice,
      CASE
        WHEN vts.validation_status IS NULL THEN NULL::text
        ELSE UPPER(vts.validation_status::text)
      END AS validation_status_text
    FROM public.v_timesheets_summary_base AS vts
    WHERE vts.timesheet_id = cur_ts.timesheet_id
    LIMIT 1
  ) AS summary_row ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw_sel.timesheet_id
           AND cw_ts.booking_id = cur_ts.booking_id
       )
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END,
             cw_sel.updated_at DESC NULLS LAST,
             cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON true
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(UPPER(COALESCE(summary_row.validation_status_text, tv.status::text)) IN ('VALIDATION_OK', 'OVERRIDDEN'), false) AS validation_ok
    FROM public.timesheet_validations AS tv
    WHERE tv.timesheet_id = cur_ts.timesheet_id
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1
  ) AS validation_state ON true;

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'INVOICED'::public.contract_week_status_enum THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'CANCELLED'::public.contract_week_status_enum THEN 'CONTRACT_WEEK_NOT_AUTHORISABLE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'AUTHORISED'::public.contract_week_status_enum THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NOT NULL OR state_rows.tsfin_authorised_at_utc IS NOT NULL THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.tsfin_processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum OR (state_rows.current_qr_status = 'PENDING'::public.timesheet_qr_status_enum AND NULLIF(BTRIM(COALESCE(state_rows.current_qr_token, '')), '') IS NOT NULL AND state_rows.current_qr_generated_at IS NOT NULL AND state_rows.current_qr_scanned_at IS NULL) THEN 'AWAITING_SIGNED_QR'
      WHEN state_rows.tsfin_processing_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN 'AUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.hr_validation_required_for_invoice, false) AND NOT COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = p_actor_user_id,
           authorised_at_utc = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_authorise:' || v_now::text,
    'TIMESHEET_BULK_AUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

-- Restore hr_weekly_phase3_apply_adjustment_truth from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(p_import_id uuid, p_selected_external_row_keys text[], p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;

  v_selected_keys text[] := '{}';
  v_key text;
  v_last_key text := null;

  v_row jsonb;

  v_is_invoiced boolean := false;
  v_invoice_id_detected uuid := null;

  v_contract_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_work_date date;

  -- Week ending date (contract-driven / base-timesheet driven; never assumed Sunday)
  v_week_ending_date date;
  v_base_timesheet_id uuid := null;
  v_base_week_ending_date date := null;

  -- ✅ NEW: inherit policy identity from parent/base timesheet
  v_effective_sheet_scope public.timesheet_scope_enum := 'WEEKLY'::public.timesheet_scope_enum;
  v_effective_submission_mode public.submission_mode_enum := 'MANUAL'::public.submission_mode_enum;

  v_contract_week_ending_weekday_snapshot int := 0;

  v_work_dow int := 0;
  v_we_delta int := 0;

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

  -- ✅ keep string forms for deterministic correction id (so we can re-base against prior POS)
  v_old_start_str text := null;
  v_old_end_str text := null;
  v_new_start_str text := null;
  v_new_end_str text := null;
  v_old_break_str text := null;
  v_new_break_str text := null;

  v_old_paid_minutes int := null;
  v_new_paid_minutes int := null;
  v_delta_paid_minutes int := null;

  v_seg_start_utc timestamptz;
  v_seg_end_utc timestamptz;
  v_seg_break_mins int;

  v_shift_date_ymd text;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_base_week_id uuid;

  v_existing_ts_id uuid;

  v_existing_cw_id uuid;
  v_existing_cw_seq int;
  v_existing_cw_is_adjustment boolean;

  v_next_additional_seq int;
  v_cw_id uuid;

  v_ts_id uuid;

  v_ins_count int := 0;
  v_upd_count int := 0;
  v_skipped_count int := 0;

  v_created_ts_ids uuid[] := '{}';
  v_updated_ts_ids uuid[] := '{}';

  -- fnv1a32 helper vars
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  v_candidate_norm text;
  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_shift_label text;
  v_shift_label_norm text;

  v_schedule jsonb;
  v_hint jsonb;

  v_try int;

  -- debug sample (invoice_debug gated inside _imp_debug_audit)
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_key_ts jsonb;
  v_kind_op text;

  v_sqlstate text;
  v_err text;

  -- ✅ Evidence + reference linkage
  v_shift_id uuid := null;
  v_shift_prev_import_id uuid := null;
  v_shift_hr_request_id text := null;
  v_ref_num text := null;
  v_schedule_import_id uuid := null;

  -- ✅ Per-key artefacts for user-facing audit
  v_rev_ts_id uuid := null;
  v_rep_ts_id uuid := null;
  v_rev_cw_id uuid := null;
  v_rep_cw_id uuid := null;

  -- ✅ POLICY: avoid stacking + delete redundant pair
  v_existing_pos_ts_id uuid := null;
  v_existing_pos_correction_id text := null;
  v_existing_pos_schedule jsonb := null;
  v_existing_pos_is_invoiced boolean := false;
  v_existing_pos_tf_locked_by_invoice_id uuid := null;
  v_existing_pos_tf_invoice_breakdown_json jsonb := null;
  v_existing_pos_seg_invoice_id uuid := null;
  v_existing_pos_seg jsonb := null;

  v_existing_pos_old_start_str text := null;
  v_existing_pos_old_end_str text := null;
  v_existing_pos_old_break_str text := null;
  v_existing_pos_import_id uuid := null;
  v_existing_pair_parent_timesheet_id uuid := null;

  v_existing_neg_ts_id uuid := null;
  v_existing_neg_schedule jsonb := null;
  v_existing_neg_is_invoiced boolean := false;
  v_existing_neg_tf_locked_by_invoice_id uuid := null;
  v_existing_neg_tf_invoice_breakdown_json jsonb := null;
  v_existing_neg_seg_invoice_id uuid := null;

  v_existing_neg_base_start_utc timestamptz := null;
  v_existing_neg_base_end_utc timestamptz := null;
  v_existing_neg_base_break_mins int := null;

  v_existing_pos_count int := 0;
  v_existing_neg_count int := 0;

  v_updated_existing_replacement boolean := false;
  v_deleted_redundant_pair boolean := false;
  -- Historical correction finance authority (Policy X pre-draft only)
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;

begin
  -- ---- Validate import exists and is HEALTHROSTER ----
  select hi.source_system
  into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_weekly_phase3_apply_adjustment_truth: import_not_found (import_id=%)', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER'::public.hr_source_enum then
    raise exception
      'hr_weekly_phase3_apply_adjustment_truth: source_system_mismatch (import_id=% actual=% expected=HEALTHROSTER)',
      p_import_id, v_src;
  end if;

  -- ---- Normalise selected keys ----
  select coalesce(array_agg(distinct btrim(k)), '{}')
  into v_selected_keys
  from unnest(coalesce(p_selected_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  if array_length(v_selected_keys, 1) is null then
    return jsonb_build_object(
      'import_id', p_import_id,
      'selected_count', 0,
      'skipped_count', 0,
      'inserted_count', 0,
      'updated_count', 0,
      'created_timesheet_ids', '[]'::jsonb,
      'updated_timesheet_ids', '[]'::jsonb
    );
  end if;

  -- ---- Load Phase 3 rows for selected keys into a lookup ----
  create temporary table tmp_phase3_by_key(
    external_row_key text primary key,
    row_json jsonb not null
  ) on commit drop;

  insert into tmp_phase3_by_key(external_row_key, row_json)
  select
    r.external_row_key,
    to_jsonb(r) as row_json
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'HEALTHROSTER'
  ) as r
  where r.external_row_key = any(v_selected_keys)
  on conflict (external_row_key) do nothing;

  -- ---- Process each selected key ----
  foreach v_key in array v_selected_keys loop
    v_last_key := v_key;

    -- reset per-key ids for user-facing audit
    v_rev_ts_id := null;
    v_rep_ts_id := null;
    v_rev_cw_id := null;
    v_rep_cw_id := null;

    -- reset policy flags
    v_existing_pos_ts_id := null;
    v_existing_pos_correction_id := null;
    v_existing_pos_schedule := null;
    v_existing_pos_is_invoiced := false;
    v_existing_pos_tf_locked_by_invoice_id := null;
    v_existing_pos_tf_invoice_breakdown_json := null;
    v_existing_pos_seg_invoice_id := null;
    v_existing_pos_seg := null;
    v_existing_pos_old_start_str := null;
    v_existing_pos_old_end_str := null;
    v_existing_pos_old_break_str := null;
    v_existing_pos_import_id := null;
    v_existing_pair_parent_timesheet_id := null;

    v_existing_neg_ts_id := null;
    v_existing_neg_schedule := null;
    v_existing_neg_is_invoiced := false;
    v_existing_neg_tf_locked_by_invoice_id := null;
    v_existing_neg_tf_invoice_breakdown_json := null;
    v_existing_neg_seg_invoice_id := null;
    v_existing_neg_base_start_utc := null;
    v_existing_neg_base_end_utc := null;
    v_existing_neg_base_break_mins := null;

    v_existing_pos_count := 0;
    v_existing_neg_count := 0;

    v_updated_existing_replacement := false;
    v_deleted_redundant_pair := false;

    select t.row_json
    into v_row
    from tmp_phase3_by_key t
    where t.external_row_key = v_key;

    if v_row is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Determine invoiced flag (from Phase3 row) for logging only
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        else false
      end;

    begin
      v_invoice_id_detected := nullif(btrim(coalesce(v_row->>'invoice_id_detected','')), '')::uuid;
    exception when others then
      v_invoice_id_detected := null;
    end;

    -- Extract required mapping fields
    begin
      v_contract_id := (v_row->>'contract_id')::uuid;
      v_candidate_id := (v_row->>'candidate_id')::uuid;
      v_client_id := (v_row->>'client_id')::uuid;
      v_work_date := (v_row->>'work_date')::date;
    exception when others then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row missing/invalid contract_id/candidate_id/client_id/work_date for external_row_key=%', v_key;
    end;

    -- ✅ Resolve shift_id + previous import id + request id (ref)
    begin
      v_shift_id := nullif(btrim(coalesce(v_row->>'shift_id','')), '')::uuid;
    exception when others then
      v_shift_id := null;
    end;

    if v_shift_id is null then
      select ns.id
      into v_shift_id
      from public.nhsp_shifts ns
      where ns.external_row_key = v_key
        and ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.cancelled_at_utc is null
      order by ns.updated_at desc nulls last, ns.created_at desc nulls last
      limit 1;
    end if;

    if v_shift_id is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Failed to resolve shift_id for external_row_key=% (required for evidence/audit).', v_key;
    end if;

    select
      ns2.latest_import_id,
      ns2.hr_request_id
    into
      v_shift_prev_import_id,
      v_shift_hr_request_id
    from public.nhsp_shifts ns2
    where ns2.id = v_shift_id
    limit 1;

    v_ref_num := nullif(btrim(coalesce(v_shift_hr_request_id, '')), '');

    -- ---- Resolve week_ending_date (DO NOT assume Sunday) ----
    v_week_ending_date := null;
    v_base_timesheet_id := null;
    v_base_week_ending_date := null;

    -- ✅ reset inherited policy identity defaults for this key
    v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
    v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;

    -- 1) Prefer base timesheet week_ending_date when timesheet_id exists (authoritative)
    begin
      v_base_timesheet_id := nullif(btrim(coalesce(v_row->>'timesheet_id','')), '')::uuid;
    exception when others then
      v_base_timesheet_id := null;
    end;

     if v_base_timesheet_id is not null then
      select
        ts.week_ending_date,
        ts.sheet_scope,
        ts.submission_mode
      into
        v_base_week_ending_date,
        v_effective_sheet_scope,
        v_effective_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if v_effective_sheet_scope is null then
        v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
      end if;

      if v_effective_submission_mode is null then
        v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_base_week_ending_date is not null then
        v_week_ending_date := v_base_week_ending_date;
      end if;
    end if;

    -- 2) Next: use week_ending_date present on Phase3 row if provided
    if v_week_ending_date is null then
      begin
        v_week_ending_date := nullif(btrim(coalesce(v_row->>'week_ending_date','')), '')::date;
      exception when others then
        v_week_ending_date := null;
      end;
    end if;

    -- 3) Final fallback: derive from contracts.week_ending_weekday_snapshot (0=Sun) and work_date
    if v_week_ending_date is null then
      select coalesce(ct.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts ct
      where ct.id = v_contract_id
      limit 1;

      v_work_dow := extract(dow from v_work_date)::int; -- 0=Sun..6=Sat
      v_we_delta := ((v_contract_week_ending_weekday_snapshot - v_work_dow + 7) % 7);
      v_week_ending_date := (v_work_date + v_we_delta)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Failed to resolve week_ending_date for external_row_key=% (contract_id=% work_date=%)', v_key, v_contract_id, v_work_date;
    end if;

    if v_base_timesheet_id is null then
      raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','external_row_key',v_key)::text;
    end if;

    begin
      v_new_paid_minutes := nullif(btrim(v_row ->> 'new_paid_minutes'), '')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_INVALID', errcode='22023',
        detail=jsonb_build_object('external_row_key',v_key,'new_paid_minutes',v_row ->> 'new_paid_minutes')::text;
    end;
    if v_new_paid_minutes is null then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('external_row_key',v_key)::text;
    end if;
    if v_new_paid_minutes = 0 then
      raise exception using message='ZERO_HOURS_MUST_USE_CANCELLATION', errcode='P0001',
        detail=jsonb_build_object(
          'external_row_key',v_key,
          'required_action','CANCELLATION',
          'required_shape','REVERSAL_ONLY',
          'replacement_timesheet_required',false
        )::text;
    end if;

    select public.timesheet_correction_chain_scope_v1(
      v_base_timesheet_id, true, 32, 100
    ) into v_chain_scope;

    if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
      raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
    end if;

    v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
    v_latest_positive_timesheet_id := coalesce(
      nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
      v_base_timesheet_id
    );
    v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
      p_import_id,
      v_root_timesheet_id,
      v_key,
      'CHANGED_HOURS',
      'REVERSAL_REPLACEMENT'
    );
    v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
      v_base_timesheet_id,
      v_correction_operation_id,
      v_key,
      'CHANGED_HOURS',
      null::text,
      true,
      32
    );
    v_correction_financials_policy_envelope_fingerprint :=
      v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

    select public.import_timesheet_financial_preflight_v1(
      p_timesheet_ids := array[v_base_timesheet_id]::uuid[],
      p_action := 'IMPORT_CHANGED_HOURS_CORRECTION',
      p_actor_user_id := p_actor_user_id,
      p_expected_state_json := jsonb_build_object(
        'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
      ),
      p_lock_rows := true,
      p_max_scope := 100
    ) into v_financial_preflight;

    if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
      raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
    end if;

    -- Extract old/new shift times and break mins
    begin
      v_old_start_utc := nullif(v_row->>'old_start_utc','')::timestamptz;
      v_old_end_utc   := nullif(v_row->>'old_end_utc','')::timestamptz;
      v_new_start_utc := nullif(v_row->>'new_start_utc','')::timestamptz;
      v_new_end_utc   := nullif(v_row->>'new_end_utc','')::timestamptz;
    exception when others then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row has invalid timestamp fields for external_row_key=%', v_key;
    end;

    if v_old_start_utc is null or v_old_end_utc is null or v_new_start_utc is null or v_new_end_utc is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row missing old/new start/end timestamps for external_row_key=%', v_key;
    end if;

    begin
      v_old_break_mins := coalesce(nullif(v_row->>'old_break_mins','')::int, 0);
    exception when others then
      v_old_break_mins := 0;
    end;

    begin
      v_new_break_mins := coalesce(nullif(v_row->>'new_break_mins','')::int, 0);
    exception when others then
      v_new_break_mins := 0;
    end;

    -- ✅ preserve string forms for correction-id (and potential POS rebase)
    v_old_start_str := coalesce(v_row->>'old_start_utc','');
    v_old_end_str   := coalesce(v_row->>'old_end_utc','');
    v_new_start_str := coalesce(v_row->>'new_start_utc','');
    v_new_end_str   := coalesce(v_row->>'new_end_utc','');
    v_old_break_str := coalesce(v_row->>'old_break_mins','');
    v_new_break_str := coalesce(v_row->>'new_break_mins','');

    -- Paid minutes (prefer Phase3 values, fallback to computed)
    begin
      v_old_paid_minutes := nullif(btrim(coalesce(v_row->>'old_paid_minutes','')), '')::int;
    exception when others then
      v_old_paid_minutes := null;
    end;

    begin
      v_new_paid_minutes := nullif(btrim(coalesce(v_row->>'new_paid_minutes','')), '')::int;
    exception when others then
      v_new_paid_minutes := null;
    end;

    if v_old_paid_minutes is null then
      v_old_paid_minutes :=
        greatest(
          0,
          (floor(extract(epoch from (v_old_end_utc - v_old_start_utc)) / 60.0))::int
          - greatest(0, coalesce(v_old_break_mins,0))
        );
    end if;

    if v_new_paid_minutes is null then
      v_new_paid_minutes :=
        greatest(
          0,
          (floor(extract(epoch from (v_new_end_utc - v_new_start_utc)) / 60.0))::int
          - greatest(0, coalesce(v_new_break_mins,0))
        );
    end if;

    v_delta_paid_minutes := coalesce(v_new_paid_minutes,0) - coalesce(v_old_paid_minutes,0);

    -- Compute correction_id (stable + deterministic)
    v_fnv_s :=
      coalesce(p_import_id::text,'') || '|' ||
      coalesce(v_key,'') || '|' ||
      coalesce(v_old_start_str,'') || '|' ||
      coalesce(v_new_start_str,'') || '|' ||
      coalesce(v_old_end_str,'') || '|' ||
      coalesce(v_new_end_str,'') || '|' ||
      coalesce(v_old_break_str,'') || '|' ||
      coalesce(v_new_break_str,'');

    v_fnv_h := 2166136261;
    for v_fnv_i in 1..char_length(v_fnv_s) loop
      v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
      v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
    end loop;

    v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
    v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;

    -- Load contract + optional client/candidate display context for norms
    select
      c.display_site,
      c.ward_hint,
      c.role
    into
      v_contract_display_site,
      v_contract_ward_hint,
      v_contract_role
    from public.contracts c
    where c.id = v_contract_id
    limit 1;

    select cl.name
    into v_client_name
    from public.clients cl
    where cl.id = v_client_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_candidate_id
    limit 1;

    v_candidate_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_hospital_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_display_site, v_client_name, v_client_id::text))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_ward_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_ward_hint,'contract'))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_role_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_role,'weekly'))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    -- Ensure base contract_week exists (seq=0, is_adjustment=false); never duplicate
    select cw0.id
    into v_base_week_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_contract_id
      and cw0.week_ending_date = v_week_ending_date
      and cw0.additional_seq = 0
      and cw0.is_adjustment = false
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        is_adjustment
      )
      values (
        v_contract_id,
        v_week_ending_date,
        0,
        false
      )
      returning id into v_base_week_id;
    end if;

    -- ✅ POLICY LOOKUPS: find latest POS + latest NEG for this shift linkage
    select count(*)::int
    into v_existing_pos_count
    from public.timesheets tpos_cnt
    where tpos_cnt.is_adjustment is true
      and tpos_cnt.is_current is true
      and tpos_cnt.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos_cnt.actual_schedule_json) = 'array'
      and tpos_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      );

    select
      tpos.timesheet_id,
      tpos.correction_id,
      tpos.actual_schedule_json
    into
      v_existing_pos_ts_id,
      v_existing_pos_correction_id,
      v_existing_pos_schedule
    from public.timesheets tpos
    where tpos.is_adjustment is true
      and tpos.is_current is true
      and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos.actual_schedule_json) = 'array'
      and tpos.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      )
    order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
    limit 1;

    if v_existing_pos_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_pos_tf_locked_by_invoice_id,
        v_existing_pos_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_pos_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_pos_seg_invoice_id := null;
      begin
        select
          nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_pos_seg_invoice_id
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_existing_pos_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_pos_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s2;
      exception when others then
        v_existing_pos_seg_invoice_id := null;
      end;

      v_existing_pos_is_invoiced :=
        (v_existing_pos_tf_locked_by_invoice_id is not null)
        or (v_existing_pos_seg_invoice_id is not null)
        or coalesce((
          public._import_review_timesheet_protection_core_v1(v_existing_pos_ts_id)
            ->>'paid'
        )::boolean,false);

      v_existing_pos_seg := null;
      if v_existing_pos_schedule is not null and jsonb_typeof(v_existing_pos_schedule) = 'array' then
        v_existing_pos_seg := v_existing_pos_schedule->0;
      end if;

      if v_existing_pos_seg is not null then
        v_existing_pos_old_start_str := nullif(btrim(coalesce(v_existing_pos_seg->>'start_utc','')), '');
        v_existing_pos_old_end_str   := nullif(btrim(coalesce(v_existing_pos_seg->>'end_utc','')), '');
        v_existing_pos_old_break_str := nullif(btrim(coalesce(v_existing_pos_seg->>'break_mins','')), '');

        begin
          if (v_existing_pos_seg ? 'import_id')
             and (v_existing_pos_seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            v_existing_pos_import_id := (v_existing_pos_seg->>'import_id')::uuid;
          else
            v_existing_pos_import_id := null;
          end if;
        exception when others then
          v_existing_pos_import_id := null;
        end;
      end if;
    end if;

    select count(*)::int
    into v_existing_neg_count
    from public.timesheets tneg_cnt
    where tneg_cnt.is_adjustment is true
      and tneg_cnt.is_current is true
      and tneg_cnt.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg_cnt.actual_schedule_json) = 'array'
      and tneg_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      );

    select
      tneg.timesheet_id,
      tneg.actual_schedule_json
    into
      v_existing_neg_ts_id,
      v_existing_neg_schedule
    from public.timesheets tneg
    where tneg.is_adjustment is true
      and tneg.is_current is true
      and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg.actual_schedule_json) = 'array'
      and tneg.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      )
    order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
    limit 1;

    if v_existing_neg_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_neg_tf_locked_by_invoice_id,
        v_existing_neg_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_neg_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_neg_seg_invoice_id := null;
      begin
        select
          nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_neg_seg_invoice_id
        from (
          select s3.seg
          from jsonb_array_elements(
            case
              when v_existing_neg_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_neg_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s3(seg)
          where nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s3;
      exception when others then
        v_existing_neg_seg_invoice_id := null;
      end;

      v_existing_neg_is_invoiced :=
        (v_existing_neg_tf_locked_by_invoice_id is not null)
        or (v_existing_neg_seg_invoice_id is not null)
        or coalesce((
          public._import_review_timesheet_protection_core_v1(v_existing_neg_ts_id)
            ->>'paid'
        )::boolean,false);
    end if;

    -- Policy X retained-history rule: never delete an existing correction pair.
    -- If truth returns to the original schedule, the retained pair is updated to a
    -- zero residual after canonical pair unauthorisation; prior TSFIN, invoice and
    -- payment history remains authoritative.
    v_deleted_redundant_pair := false;

    -- ✅ If latest POS is NOT invoiced: update POS in place (do NOT create new NEG/POS)
    v_updated_existing_replacement := false;
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is false then
      -- Use existing POS correction_id (for continuity)
      if nullif(btrim(coalesce(v_existing_pos_correction_id,'')), '') is not null then
        v_correction_id := v_existing_pos_correction_id;
      end if;

      v_shift_date_ymd := to_char((v_new_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
          'start_utc', v_new_start_utc::text,
          'end_utc', v_new_end_utc::text,
          'break_mins', greatest(0, v_new_break_mins),
          'ref_num', v_ref_num,
          'external_row_key', v_key,
          'shift_id', v_shift_id::text,
          'import_id', p_import_id::text
        )
      );

      v_hint := jsonb_build_object(
        'import_correction', jsonb_build_object(
          'import_id', p_import_id::text,
          'external_row_key', v_key,
          'correction_id', v_correction_id,
          'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
          'updated_from_import_id', p_import_id::text
        )
      );

      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id', v_root_timesheet_id::text,
        'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
      );


      perform 1
      from public.timesheets tlock
      where tlock.correction_id = v_existing_pos_correction_id
        and tlock.is_current = true
        and tlock.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      order by tlock.timesheet_id
      for update;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      if exists (
        select 1
        from public.timesheets pair_ts
        left join public.timesheets_financials pair_tf
          on pair_tf.timesheet_id=pair_ts.timesheet_id and pair_tf.is_current=true
        where pair_ts.correction_id=v_existing_pos_correction_id
          and pair_ts.is_current=true
          and pair_ts.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (
            pair_ts.authorised_at_server is not null
            or pair_tf.authorised_at_utc is not null
            or pair_tf.paid_at_utc is not null
            or coalesce((
              public._import_review_timesheet_protection_core_v1(pair_ts.timesheet_id)
                ->>'paid'
            )::boolean,false)
            or pair_tf.locked_by_invoice_id is not null
            or exists (select 1 from public.invoice_lines il where il.timesheet_id=pair_ts.timesheet_id)
          )
      ) then
        raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',
            'correction_id',v_existing_pos_correction_id,
            'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
          )::text;
      end if;

      select pair_reversal.parent_timesheet_id
      into v_existing_pair_parent_timesheet_id
      from public.timesheets pair_reversal
      where pair_reversal.correction_id=v_existing_pos_correction_id
        and pair_reversal.is_current=true
        and pair_reversal.correction_kind='CHANGED_HOURS_REVERSAL'
      limit 1;

      if v_existing_pair_parent_timesheet_id is null then
        raise exception using message='CORRECTION_PAIR_PARENT_MISSING',errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_PARENT_MISSING',
            'correction_id',v_existing_pos_correction_id
          )::text;
      end if;

      -- Repair only the known legacy replay split in a complete, mutable pair.
      -- Frozen, invoiced, paid or authorised pair members were rejected above.
      update public.timesheets pair_replacement
      set parent_timesheet_id=v_existing_pair_parent_timesheet_id,
          updated_at=v_now
      where pair_replacement.correction_id=v_existing_pos_correction_id
        and pair_replacement.is_current=true
        and pair_replacement.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and pair_replacement.parent_timesheet_id is distinct from v_existing_pair_parent_timesheet_id;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
          and count(distinct pair_check.parent_timesheet_id) = 1
          and count(pair_check.parent_timesheet_id) = 2
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,

        -- ✅ inherit policy identity from base timesheet
        sheet_scope = v_effective_sheet_scope,
        submission_mode = v_effective_submission_mode,

        updated_at = v_now
      where tup.timesheet_id = v_existing_pos_ts_id;


      -- Keep contract_week snapshot in sync with the effective submission mode
      update public.contract_weeks cw_sm
      set submission_mode_snapshot = v_effective_submission_mode,
          updated_at = v_now
      where cw_sm.timesheet_id = v_existing_pos_ts_id
        and cw_sm.contract_id = v_contract_id
        and cw_sm.week_ending_date = v_week_ending_date;

      v_rep_ts_id := v_existing_pos_ts_id;
      v_rep_cw_id := null;

      v_upd_count := v_upd_count + 1;
      v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_pos_ts_id);
      v_updated_existing_replacement := true;

      v_key_ts := '[]'::jsonb;
      v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
        'kind', 'CHANGED_HOURS_REPLACEMENT',
        'timesheet_id', v_existing_pos_ts_id::text,
        'op', 'UPDATED_IN_PLACE'
      ));
    end if;
    -- ✅ If latest POS IS invoiced, re-base old values to POS (so NEG reverses POS)
    if v_updated_existing_replacement is false and v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is true then

      -- ✅ Treat the invoiced POS as the effective parent for policy inheritance
      v_base_timesheet_id := v_existing_pos_ts_id;

      select
        coalesce(ts.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        coalesce(ts.submission_mode, 'MANUAL'::public.submission_mode_enum)
      into
        v_effective_sheet_scope,
        v_effective_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if not found then
        v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
        v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_existing_pos_old_start_str is not null then
        begin
          v_old_start_utc := v_existing_pos_old_start_str::timestamptz;
        exception when others then
          null;
        end;
      end if;


      if v_existing_pos_old_end_str is not null then
        begin
          v_old_end_utc := v_existing_pos_old_end_str::timestamptz;
        exception when others then
          null;
        end;
      end if;

      if v_existing_pos_old_break_str is not null and v_existing_pos_old_break_str ~ '^[0-9]+$' then
        begin
          v_old_break_mins := v_existing_pos_old_break_str::int;
        exception when others then
          null;
        end;
      end if;

      if v_existing_pos_import_id is not null then
        v_shift_prev_import_id := v_existing_pos_import_id;
      end if;

      v_old_start_str := coalesce(v_existing_pos_old_start_str, v_old_start_str);
      v_old_end_str   := coalesce(v_existing_pos_old_end_str, v_old_end_str);
      v_old_break_str := coalesce(v_existing_pos_old_break_str, v_old_break_str);

      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_key,'') || '|' ||
        coalesce(v_old_start_str,'') || '|' ||
        coalesce(v_new_start_str,'') || '|' ||
        coalesce(v_old_end_str,'') || '|' ||
        coalesce(v_new_end_str,'') || '|' ||
        coalesce(v_old_break_str,'') || '|' ||
        coalesce(v_new_break_str,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;

      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
      v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;
    end if;

    -- If we updated POS in place, skip creating new corrections
    if v_updated_existing_replacement is true then
      -- still include in sample; audits below already handle v_rep_ts_id
      -- but we must still run the audit block (it uses v_rep_ts_id)
      null;
    else
      -- Apply two artefacts: REVERSAL and REPLACEMENT
      v_key_ts := '[]'::jsonb;

      for v_kind in select unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) loop
        v_kind_op := null;

        if v_kind = 'CHANGED_HOURS_REVERSAL' then
          v_seg_start_utc := v_old_start_utc;
          v_seg_end_utc := v_old_end_utc;
          v_seg_break_mins := greatest(0, v_old_break_mins);
          v_schedule_import_id := v_shift_prev_import_id;
        else
          v_seg_start_utc := v_new_start_utc;
          v_seg_end_utc := v_new_end_utc;
          v_seg_break_mins := greatest(0, v_new_break_mins);
          v_schedule_import_id := p_import_id;
        end if;

        v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

        v_hint := jsonb_build_object(
          'import_correction', jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'correction_id', v_correction_id,
            'correction_kind', v_kind
          )
        );

        v_hint := v_hint || jsonb_build_object(
          'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
          'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'root_timesheet_id', v_root_timesheet_id::text,
          'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
        );


        v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

        v_shift_label_norm :=
          regexp_replace(
            regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
            '[^\w\s\-@&\/,.:]',
            '',
            'g'
          );

        -- ✅ Schedule carries ref_num + evidence linkage (external_row_key/shift_id/import_id)
        v_schedule := jsonb_build_array(
          jsonb_build_object(
            'date', v_shift_date_ymd,
            'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
            'start_utc', v_seg_start_utc::text,
            'end_utc', v_seg_end_utc::text,
            'break_mins', v_seg_break_mins,
            'ref_num', v_ref_num,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'import_id', case when v_schedule_import_id is null then null else v_schedule_import_id::text end
          )
        );

        -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
        v_existing_ts_id := null;

        select t.timesheet_id
        into v_existing_ts_id
        from public.timesheets t
        where t.correction_id = v_correction_id
          and t.correction_kind = v_kind
        order by t.is_current desc, t.version desc
        limit 1
        for update;

        if v_existing_ts_id is not null then
          -- Ensure there is an adjustment contract_week linked; reuse it if present.
          v_existing_cw_id := null;
          v_existing_cw_seq := null;
          v_existing_cw_is_adjustment := null;

          select
            cw.id,
            cw.additional_seq,
            cw.is_adjustment
          into
            v_existing_cw_id,
            v_existing_cw_seq,
            v_existing_cw_is_adjustment
          from public.contract_weeks cw
          where cw.timesheet_id = v_existing_ts_id
            and cw.contract_id = v_contract_id
            and cw.week_ending_date = v_week_ending_date
          limit 1
          for update;

          if v_existing_cw_id is not null then
            if v_existing_cw_is_adjustment is not true or coalesce(v_existing_cw_seq,0) <= 0 then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: existing correction timesheet is linked to a non-adjustment contract_week (timesheet_id=%).', v_existing_ts_id;
            end if;

            update public.contract_weeks cw2
            set
              is_adjustment = true,
              submission_mode_snapshot = v_effective_submission_mode,
              status = 'SUBMITTED'::public.contract_week_status_enum,
              updated_at = v_now
            where cw2.id = v_existing_cw_id;

          else
            -- Create a new adjustment contract_week safely and link it to the existing correction timesheet.
            perform 1
            from public.contract_weeks cwlock
            where cwlock.contract_id = v_contract_id
              and cwlock.week_ending_date = v_week_ending_date
            for update;

            v_try := 0;
            loop
              v_try := v_try + 1;
              if v_try > 10 then
                raise exception 'hr_weekly_phase3_apply_adjustment_truth: failed to allocate additional_seq after retries (contract_id=% week_ending=%).', v_contract_id, v_week_ending_date;
              end if;

              select coalesce(max(cwmax.additional_seq), 0) + 1
              into v_next_additional_seq
              from public.contract_weeks cwmax
              where cwmax.contract_id = v_contract_id
                and cwmax.week_ending_date = v_week_ending_date;

              begin
                insert into public.contract_weeks(
                  contract_id,
                  week_ending_date,
                  additional_seq,
                  is_adjustment,
                  submission_mode_snapshot,
                  status,
                  created_at,
                  updated_at,
                  timesheet_id
                )
                values (
                  v_contract_id,
                  v_week_ending_date,
                  v_next_additional_seq,
                  true,
                  v_effective_submission_mode,
                  'SUBMITTED'::public.contract_week_status_enum,
                  v_now,
                  v_now,
                  v_existing_ts_id
                )
                returning id into v_existing_cw_id;

                exit;
              exception when unique_violation then
                v_existing_cw_id := null;
              end;
            end loop;
          end if;

          -- Update existing correction timesheet to ensure columns match locked contract
             update public.timesheets t2
          set
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            sheet_scope = v_effective_sheet_scope,
            submission_mode = v_effective_submission_mode,
            line_type = 'HOURS',

            week_ending_date = v_week_ending_date,
            contract_id = v_contract_id,
            occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
            hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
            ward_norm = lower(coalesce(v_contract_ward_hint,'contract')),
            job_title_norm = lower(coalesce(v_contract_role,'weekly')),
            shift_label_norm = v_shift_label_norm,
            manual_pdf_r2_key = null,
            actual_schedule_json = v_schedule,
            additional_units_week = '{}'::jsonb,
            additional_units_per_day = '{}'::jsonb,
            day_references_json = null,
            candidate_hint_text = v_hint,
            is_adjustment = true,
            parent_timesheet_id = v_base_timesheet_id,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CORRECTION',
            updated_at = v_now
          where t2.timesheet_id = v_existing_ts_id;

          v_upd_count := v_upd_count + 1;
          v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
          v_kind_op := 'UPDATED';

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_existing_ts_id;
            v_rev_cw_id := v_existing_cw_id;
          else
            v_rep_ts_id := v_existing_ts_id;
            v_rep_cw_id := v_existing_cw_id;
          end if;

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_existing_ts_id::text,
            'op', v_kind_op
          ));

        else
          -- Create a new adjustment contract_week (safe additional_seq) + a new correction timesheet linked to it.
          perform 1
          from public.contract_weeks cwlock2
          where cwlock2.contract_id = v_contract_id
            and cwlock2.week_ending_date = v_week_ending_date
          for update;

          v_try := 0;
          loop
            v_try := v_try + 1;
            if v_try > 10 then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: failed to allocate additional_seq after retries (contract_id=% week_ending=%).', v_contract_id, v_week_ending_date;
            end if;

            select coalesce(max(cwmax2.additional_seq), 0) + 1
            into v_next_additional_seq
            from public.contract_weeks cwmax2
            where cwmax2.contract_id = v_contract_id
              and cwmax2.week_ending_date = v_week_ending_date;

            begin
              insert into public.contract_weeks(
                contract_id,
                week_ending_date,
                additional_seq,
                is_adjustment,
                submission_mode_snapshot,
                status,
                created_at,
                updated_at
              )
              values (
                v_contract_id,
                v_week_ending_date,
                v_next_additional_seq,
                true,
                v_effective_submission_mode,
                'SUBMITTED'::public.contract_week_status_enum,
                v_now,
                v_now
              )
              returning id into v_cw_id;

              exit;
            exception when unique_violation then
              v_cw_id := null;
            end;
          end loop;

          v_booking_base :=
            v_candidate_norm || '|' ||
            v_week_ending_date::text || '|' ||
            v_hospital_norm || '|' ||
            v_ward_norm || '|' ||
            v_role_norm || '|' ||
            regexp_replace(
              regexp_replace(lower(trim('WEEKLY-' || v_next_additional_seq::text || '-' || v_kind || '-' || v_correction_id)), '\s+', ' ', 'g'),
              '[^\w\s\-@&\/,.:]',
              '',
              'g'
            );

          v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
          v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

     begin
         insert into public.timesheets(
  booking_id,
  version,
  is_current,
  status,
  occupant_key_norm,
  hospital_norm,
  ward_norm,
  job_title_norm,
  shift_label_norm,
  week_ending_date,
  contract_id,
  sheet_scope,
  submission_mode,
  line_type,
  manual_pdf_r2_key,
  actual_schedule_json,
  additional_units_week,
  additional_units_per_day,
  day_references_json,
  qr_status,
  qr_token,
  qr_generated_at,
  qr_scanned_at,
  qr_scan_info_json,
  qr_r2_key,
  qr_payload_json,
  created_at,
  updated_at,
  is_adjustment,
  parent_timesheet_id,
  candidate_hint_text,
  correction_id,
  correction_kind,
  adjustment_origin
)
values (
  v_booking_id,
  1,
  true,
  'RECEIVED'::public.timesheet_status_enum,
  lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
  lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
  lower(coalesce(v_contract_ward_hint,'contract')),
  lower(coalesce(v_contract_role,'weekly')),
  v_shift_label_norm,
  v_week_ending_date,
  v_contract_id,
  v_effective_sheet_scope,
  v_effective_submission_mode,
  'HOURS',

  null,
  v_schedule,
  '{}'::jsonb,
  '{}'::jsonb,
  null,
  null,
  null,
  null,
  null,
  null,
  null,
  '{}'::jsonb,
  v_now,
  v_now,
  true,
  v_base_timesheet_id,
  v_hint,
  v_correction_id,
  v_kind,
  'IMPORT_CORRECTION'
)
returning timesheet_id into v_ts_id;



          exception when unique_violation then
            select t3.timesheet_id
            into v_ts_id
            from public.timesheets t3
            where t3.correction_id = v_correction_id
              and t3.correction_kind = v_kind
            order by t3.is_current desc, t3.version desc
            limit 1
            for update;

            if v_ts_id is null then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: unique_violation inserting correction timesheet but failed to find existing row (correction_id=% kind=%).', v_correction_id, v_kind;
            end if;

                     update public.timesheets t4
            set
              is_current = true,
              status = 'RECEIVED'::public.timesheet_status_enum,
              sheet_scope = v_effective_sheet_scope,
              submission_mode = v_effective_submission_mode,
              line_type = 'HOURS',

              week_ending_date = v_week_ending_date,
              contract_id = v_contract_id,
              occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
              hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
              ward_norm = lower(coalesce(v_contract_ward_hint,'contract')),
              job_title_norm = lower(coalesce(v_contract_role,'weekly')),
              shift_label_norm = v_shift_label_norm,
              manual_pdf_r2_key = null,
              actual_schedule_json = v_schedule,
              additional_units_week = '{}'::jsonb,
              additional_units_per_day = '{}'::jsonb,
              day_references_json = null,
              candidate_hint_text = v_hint,
              is_adjustment = true,
              parent_timesheet_id = v_base_timesheet_id,
              correction_id = v_correction_id,
              correction_kind = v_kind,
              adjustment_origin = 'IMPORT_CORRECTION',
              updated_at = v_now
            where t4.timesheet_id = v_ts_id;
          end;

          update public.contract_weeks cw3
          set
            timesheet_id = v_ts_id,
            status = 'SUBMITTED'::public.contract_week_status_enum,
            submission_mode_snapshot = v_effective_submission_mode,
            is_adjustment = true,
            updated_at = v_now
          where cw3.id = v_cw_id;

          v_ins_count := v_ins_count + 1;
          v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);
          v_kind_op := 'CREATED';

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_ts_id;
            v_rev_cw_id := v_cw_id;
          else
            v_rep_ts_id := v_ts_id;
            v_rep_cw_id := v_cw_id;
          end if;

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_ts_id::text,
            'op', v_kind_op
          ));
        end if;

      end loop; -- kind loop
    end if; -- updated_existing_replacement

    -- ─────────────────────────────────────────────
    -- ✅ User-facing audit entries (timesheet modal + invoice history)
    -- ─────────────────────────────────────────────
    begin
      -- Timesheet audit: reversal
      if v_rev_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rev_ts_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
            'evidence_import_id', case when v_shift_prev_import_id is null then null else v_shift_prev_import_id::text end,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'old_start_utc', v_old_start_utc::text,
            'old_end_utc', v_old_end_utc::text,
            'old_break_mins', v_old_break_mins,
            'new_start_utc', v_new_start_utc::text,
            'new_end_utc', v_new_end_utc::text,
            'new_break_mins', v_new_break_mins,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Timesheet audit: replacement
      if v_rep_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rep_ts_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
            'evidence_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'old_start_utc', v_old_start_utc::text,
            'old_end_utc', v_old_end_utc::text,
            'old_break_mins', v_old_break_mins,
            'new_start_utc', v_new_start_utc::text,
            'new_end_utc', v_new_end_utc::text,
            'new_break_mins', v_new_break_mins,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Optional: contract_week audit
      if v_rev_cw_id is not null then
        perform public._audit_insert(
          'contract_weeks',
          v_rev_cw_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      if v_rep_cw_id is not null then
        perform public._audit_insert(
          'contract_weeks',
          v_rep_cw_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Invoice history entry (ungated)
      if v_invoice_id_detected is not null then
        perform public._inv_write_audit(
          p_actor_user_id,
          'HR_IMPORT_CORRECTION_APPLIED',
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id', v_invoice_id_detected::text,
            'correction_id', v_correction_id,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'reversal_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'invoices',
          v_invoice_id_detected::text,
          null,
          'IMPORT_CORRECTION',
          null,
          null,
          null
        );
      end if;
    exception when others then
      null;
    end;

    if v_sample_n < 20 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'external_row_key', v_key,
        'is_invoiced', v_is_invoiced,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'week_ending_date', v_week_ending_date::text,
        'base_timesheet_id', case when v_base_timesheet_id is null then null else v_base_timesheet_id::text end,
        'correction_id', v_correction_id,
        'replacement_updated_in_place', v_updated_existing_replacement,
        'redundant_pair_deleted', v_deleted_redundant_pair,
        'timesheets', v_key_ts
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop; -- selected keys loop

  -- Debug audit (invoice_debug gated inside _imp_debug_audit)
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_CORRECTION_SERIES_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
      'inserted_count', v_ins_count,
      'updated_count', v_upd_count,
      'created_timesheet_ids_count', coalesce(array_length(v_created_ts_ids, 1), 0),
      'updated_timesheet_ids_count', coalesce(array_length(v_updated_ts_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id', p_import_id,
    'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
    'skipped_count', v_skipped_count,
    'inserted_count', v_ins_count,
    'updated_count', v_upd_count,
    'created_timesheet_ids', to_jsonb(coalesce(v_created_ts_ids, '{}'::uuid[])),
    'updated_timesheet_ids', to_jsonb(coalesce(v_updated_ts_ids, '{}'::uuid[]))
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_CORRECTION_SERIES_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'last_external_row_key', v_last_key,
        'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
        'inserted_count', v_ins_count,
        'updated_count', v_upd_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- Restore nhsp_weekly_phase3_apply_adjustment_truth from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(p_import_id uuid, p_selected_external_row_keys text[], p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;

  v_selected_keys text[] := '{}';
  v_key text;
  v_last_key text := null;

  v_row jsonb;

  v_is_invoiced boolean := false;
  v_invoice_id_detected uuid := null;

  v_contract_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_work_date date;

  -- Week ending date (MUST be contract-driven or base-timesheet driven; not assumed Sunday)
  v_week_ending_date date;
  v_base_timesheet_id uuid := null;
  v_base_week_ending_date date := null;

  -- ✅ NEW: inherit policy identity from the parent/base timesheet (so adjustments follow parent stream)
  v_parent_sheet_scope public.timesheet_scope_enum := 'WEEKLY'::public.timesheet_scope_enum;
  v_parent_submission_mode public.submission_mode_enum := 'MANUAL'::public.submission_mode_enum;

  v_contract_week_ending_weekday_snapshot int := 0;
  v_work_dow int := 0;
  v_we_delta int := 0;

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

  -- ✅ Keep original string forms for deterministic correction_id hashing
  v_old_start_str text := null;
  v_old_end_str text := null;
  v_new_start_str text := null;
  v_new_end_str text := null;
  v_old_break_str text := null;
  v_new_break_str text := null;

  v_seg_start_utc timestamptz;
  v_seg_end_utc timestamptz;
  v_seg_break_mins int;

  v_ref_num text := null;

  -- ✅ Evidence linkage (NHSP)
  v_shift_id uuid := null;
  v_shift_prev_import_id uuid := null;
  v_schedule_import_id uuid := null;

  -- ✅ Existing replacement (POS₀) handling to avoid stacking corrections
  v_existing_pos_ts_id uuid := null;
  v_existing_pos_correction_id text := null;
  v_existing_pos_schedule jsonb := null;
  v_existing_pos_hint jsonb := null;
  v_existing_pos_is_invoiced boolean := false;
  v_existing_pos_tf_locked_by_invoice_id uuid := null;
  v_existing_pos_tf_invoice_breakdown_json jsonb := null;
  v_existing_pos_seg_invoice_id uuid := null;
  v_existing_pos_seg jsonb := null;

  v_existing_pos_old_start_str text := null;
  v_existing_pos_old_end_str text := null;
  v_existing_pos_old_break_str text := null;
  v_existing_pos_import_id uuid := null;
  v_existing_pair_parent_timesheet_id uuid := null;

  -- ✅ NEW: Existing base reversal (NEG₀) for edge-case deletion
  v_existing_neg_ts_id uuid := null;
  v_existing_neg_schedule jsonb := null;
  v_existing_neg_is_invoiced boolean := false;
  v_existing_neg_tf_locked_by_invoice_id uuid := null;
  v_existing_neg_tf_invoice_breakdown_json jsonb := null;
  v_existing_neg_seg_invoice_id uuid := null;

  v_existing_neg_base_start_utc timestamptz := null;
  v_existing_neg_base_end_utc timestamptz := null;
  v_existing_neg_base_break_mins int := null;

  v_existing_pos_count int := 0;
  v_existing_neg_count int := 0;

  v_deleted_redundant_pair boolean := false;
  -- Historical correction finance authority (Policy X pre-draft only)
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;


  v_updated_existing_replacement boolean := false;

  -- Per-key audit helpers
  v_old_paid_minutes int := null;
  v_new_paid_minutes int := null;
  v_delta_paid_minutes int := null;

  v_invoice_number_text text := null;

  v_rev_ts_id uuid := null;
  v_rep_ts_id uuid := null;

  v_shift_date_ymd text;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_base_week_id uuid;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;
  v_existing_cw_seq int;
  v_existing_cw_is_adjustment boolean;

  v_next_additional_seq int;

  v_ts_id uuid;

  v_ins_count int := 0;
  v_upd_count int := 0;
  v_skipped_count int := 0;

  v_created_ts_ids uuid[] := '{}';
  v_updated_ts_ids uuid[] := '{}';

  -- fnv1a32 helper vars
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_shift_label text;
  v_shift_label_norm text;

  v_schedule jsonb;
  v_hint jsonb;

  v_try int;

  -- debug sample (invoice_debug gated inside _imp_debug_audit)
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_key_ts jsonb;
  v_kind_op text;

  v_sqlstate text;
  v_err text;
begin
  -- ---- Validate import exists and is NHSP ----
  select hi.source_system
  into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: import_not_found (import_id=%)', p_import_id;
  end if;

  if v_src <> 'NHSP'::public.hr_source_enum then
    raise exception
      'nhsp_weekly_phase3_apply_adjustment_truth: source_system_mismatch (import_id=% actual=% expected=NHSP)',
      p_import_id, v_src;
  end if;

  -- ---- Normalise selected keys ----
  select coalesce(array_agg(distinct btrim(k)), '{}')
  into v_selected_keys
  from unnest(coalesce(p_selected_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  if array_length(v_selected_keys, 1) is null then
    return jsonb_build_object(
      'import_id', p_import_id,
      'selected_count', 0,
      'skipped_count', 0,
      'inserted_count', 0,
      'updated_count', 0,
      'created_timesheet_ids', '[]'::jsonb,
      'updated_timesheet_ids', '[]'::jsonb
    );
  end if;

  -- ---- Load Phase 3 rows for selected keys into a lookup ----
  create temporary table tmp_phase3_by_key(
    external_row_key text primary key,
    row_json jsonb not null
  ) on commit drop;

  insert into tmp_phase3_by_key(external_row_key, row_json)
  select
    r.external_row_key,
    to_jsonb(r) as row_json
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'NHSP'
  ) as r
  where r.external_row_key = any(v_selected_keys)
  on conflict (external_row_key) do nothing;

  -- ---- Process each selected key ----
  foreach v_key in array v_selected_keys loop
    v_last_key := v_key;

    -- reset per-key flags
    v_updated_existing_replacement := false;
    v_deleted_redundant_pair := false;

    v_existing_pos_ts_id := null;
    v_existing_pos_correction_id := null;
    v_existing_pos_schedule := null;
    v_existing_pos_is_invoiced := false;
    v_existing_pos_tf_locked_by_invoice_id := null;
    v_existing_pos_tf_invoice_breakdown_json := null;
    v_existing_pos_seg_invoice_id := null;
    v_existing_pos_seg := null;
    v_existing_pos_old_start_str := null;
    v_existing_pos_old_end_str := null;
    v_existing_pos_old_break_str := null;
    v_existing_pos_import_id := null;
    v_existing_pair_parent_timesheet_id := null;

    v_existing_neg_ts_id := null;
    v_existing_neg_schedule := null;
    v_existing_neg_is_invoiced := false;
    v_existing_neg_tf_locked_by_invoice_id := null;
    v_existing_neg_tf_invoice_breakdown_json := null;
    v_existing_neg_seg_invoice_id := null;

    v_existing_neg_base_start_utc := null;
    v_existing_neg_base_end_utc := null;
    v_existing_neg_base_break_mins := null;

    v_existing_pos_count := 0;
    v_existing_neg_count := 0;

    select t.row_json
    into v_row
    from tmp_phase3_by_key t
    where t.external_row_key = v_key;

    if v_row is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Determine invoiced flag (from Phase3 row) for logging only
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        else false
      end;

    begin
      v_invoice_id_detected := nullif(btrim(coalesce(v_row->>'invoice_id_detected','')), '')::uuid;
    exception when others then
      v_invoice_id_detected := null;
    end;

    -- Extract required mapping fields
    begin
      v_contract_id := (v_row->>'contract_id')::uuid;
      v_candidate_id := (v_row->>'candidate_id')::uuid;
      v_client_id := (v_row->>'client_id')::uuid;
      v_work_date := (v_row->>'work_date')::date;
    exception when others then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row missing/invalid contract_id/candidate_id/client_id/work_date for external_row_key=%', v_key;
    end;

    -- ---- Resolve week_ending_date (DO NOT assume Sunday) ----
    v_week_ending_date := null;
    v_base_timesheet_id := null;
    v_base_week_ending_date := null;

    -- ✅ reset inherited policy identity defaults for this key (avoid leaking previous key’s parent settings)
    v_parent_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
    v_parent_submission_mode := 'MANUAL'::public.submission_mode_enum;

    -- 1) Prefer base timesheet week_ending_date when timesheet_id exists (authoritative)
    begin
      v_base_timesheet_id := nullif(btrim(coalesce(v_row->>'timesheet_id','')), '')::uuid;
    exception when others then
      v_base_timesheet_id := null;
    end;
    if v_base_timesheet_id is not null then
      select
        ts.week_ending_date,
        ts.sheet_scope,
        ts.submission_mode
      into
        v_base_week_ending_date,
        v_parent_sheet_scope,
        v_parent_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if v_base_week_ending_date is not null then
        v_week_ending_date := v_base_week_ending_date;
      end if;
    end if;

    -- 2) Next: use week_ending_date present on Phase3 row if provided
    if v_week_ending_date is null then
      begin
        v_week_ending_date := nullif(btrim(coalesce(v_row->>'week_ending_date','')), '')::date;
      exception when others then
        v_week_ending_date := null;
      end;
    end if;

    -- 3) Final fallback: derive from contracts.week_ending_weekday_snapshot (0=Sunday) and work_date
    if v_week_ending_date is null then
      select coalesce(ct.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts ct
      where ct.id = v_contract_id
      limit 1;

      v_work_dow := extract(dow from v_work_date)::int; -- 0=Sun..6=Sat
      v_we_delta := ((v_contract_week_ending_weekday_snapshot - v_work_dow + 7) % 7);
      v_week_ending_date := (v_work_date + v_we_delta)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Failed to resolve week_ending_date for external_row_key=% (contract_id=% work_date=%)', v_key, v_contract_id, v_work_date;
    end if;

    if v_base_timesheet_id is null then
      raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','external_row_key',v_key)::text;
    end if;

    begin
      v_new_paid_minutes := nullif(btrim(v_row ->> 'new_paid_minutes'), '')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_INVALID', errcode='22023',
        detail=jsonb_build_object('external_row_key',v_key,'new_paid_minutes',v_row ->> 'new_paid_minutes')::text;
    end;
    if v_new_paid_minutes is null then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('external_row_key',v_key)::text;
    end if;
    if v_new_paid_minutes = 0 then
      raise exception using message='ZERO_HOURS_MUST_USE_CANCELLATION', errcode='P0001',
        detail=jsonb_build_object(
          'external_row_key',v_key,
          'required_action','CANCELLATION',
          'required_shape','REVERSAL_ONLY',
          'replacement_timesheet_required',false
        )::text;
    end if;

    select public.timesheet_correction_chain_scope_v1(
      v_base_timesheet_id, true, 32, 100
    ) into v_chain_scope;

    if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
      raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
    end if;

    v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
    v_latest_positive_timesheet_id := coalesce(
      nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
      v_base_timesheet_id
    );
    v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
      p_import_id,
      v_root_timesheet_id,
      v_key,
      'CHANGED_HOURS',
      'REVERSAL_REPLACEMENT'
    );
    v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
      v_base_timesheet_id,
      v_correction_operation_id,
      v_key,
      'CHANGED_HOURS',
      null::text,
      true,
      32
    );
    v_correction_financials_policy_envelope_fingerprint :=
      v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

    select public.import_timesheet_financial_preflight_v1(
      p_timesheet_ids := array[v_base_timesheet_id]::uuid[],
      p_action := 'IMPORT_CHANGED_HOURS_CORRECTION',
      p_actor_user_id := p_actor_user_id,
      p_expected_state_json := jsonb_build_object(
        'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
      ),
      p_lock_rows := true,
      p_max_scope := 100
    ) into v_financial_preflight;

    if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
      raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
    end if;

    -- Extract old/new shift times and break mins
    begin
      v_old_start_utc := nullif(v_row->>'old_start_utc','')::timestamptz;
      v_old_end_utc   := nullif(v_row->>'old_end_utc','')::timestamptz;
      v_new_start_utc := nullif(v_row->>'new_start_utc','')::timestamptz;
      v_new_end_utc   := nullif(v_row->>'new_end_utc','')::timestamptz;
    exception when others then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row has invalid timestamp fields for external_row_key=%', v_key;
    end;

    if v_old_start_utc is null or v_old_end_utc is null or v_new_start_utc is null or v_new_end_utc is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row missing old/new start/end timestamps for external_row_key=%', v_key;
    end if;

    begin
      v_old_break_mins := coalesce(nullif(v_row->>'old_break_mins','')::int, 0);
    exception when others then
      v_old_break_mins := 0;
    end;

    begin
      v_new_break_mins := coalesce(nullif(v_row->>'new_break_mins','')::int, 0);
    exception when others then
      v_new_break_mins := 0;
    end;

    -- ✅ Preserve string forms for deterministic correction_id hashing
    v_old_start_str := coalesce(v_row->>'old_start_utc', '');
    v_old_end_str   := coalesce(v_row->>'old_end_utc', '');
    v_new_start_str := coalesce(v_row->>'new_start_utc', '');
    v_new_end_str   := coalesce(v_row->>'new_end_utc', '');
    v_old_break_str := coalesce(v_row->>'old_break_mins', '');
    v_new_break_str := coalesce(v_row->>'new_break_mins', '');

    -- Compute correction_id (stable + deterministic)
    v_fnv_s :=
      coalesce(p_import_id::text,'') || '|' ||
      coalesce(v_key,'') || '|' ||
      coalesce(v_old_start_str,'') || '|' ||
      coalesce(v_new_start_str,'') || '|' ||
      coalesce(v_old_end_str,'')   || '|' ||
      coalesce(v_new_end_str,'')   || '|' ||
      coalesce(v_old_break_str,'') || '|' ||
      coalesce(v_new_break_str,'');

    v_fnv_h := 2166136261;
    for v_fnv_i in 1..char_length(v_fnv_s) loop
      v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
      v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
    end loop;

    v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
    v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;

    -- Load contract + optional client/candidate display context for norms
    select
      c.display_site,
      c.ward_hint,
      c.role
    into
      v_contract_display_site,
      v_contract_ward_hint,
      v_contract_role
    from public.contracts c
    where c.id = v_contract_id
    limit 1;

    select cl.name
    into v_client_name
    from public.clients cl
    where cl.id = v_client_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_candidate_id
    limit 1;

    v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text));
    v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
    v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

    v_booking_base :=
      'scope=WEEKLY' || '|' ||
      'client_id=' || coalesce(v_client_id::text,'') || '|' ||
      'candidate_id=' || coalesce(v_candidate_id::text,'') || '|' ||
      'contract_id=' || coalesce(v_contract_id::text,'') || '|' ||
      'week_ending_date=' || coalesce(v_week_ending_date::text,'') || '|' ||
      'hospital=' || v_hospital_norm || '|' ||
      'ward=' || v_ward_norm || '|' ||
      'role=' || v_role_norm;

    -- Ensure base week exists (additional_seq=0, is_adjustment=false)
    v_base_week_id := null;

    select cw0.id
    into v_base_week_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_contract_id
      and cw0.week_ending_date = v_week_ending_date
      and cw0.is_adjustment is false
      and coalesce(cw0.additional_seq, 0) = 0
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        is_adjustment,
        status,
        created_at,
        updated_at
      )
      values (
        v_contract_id,
        v_week_ending_date,
        0,
        false,
        'SUBMITTED'::public.contract_week_status_enum,
        v_now,
        v_now
      )
      returning id into v_base_week_id;
    end if;

    -- Resolve reference number for this external_row_key (used on BOTH reversal + replacement schedules)
    v_ref_num := null;

    select ns_ref.ref_num
    into v_ref_num
    from public.nhsp_shifts ns_ref
    where ns_ref.source_system = 'NHSP'::public.hr_source_enum
      and ns_ref.external_row_key = v_key
    order by ns_ref.updated_at desc nulls last, ns_ref.created_at desc nulls last
    limit 1;

    if nullif(btrim(coalesce(v_ref_num,'')), '') is null then
      v_ref_num := nullif(btrim(coalesce(v_row->>'ref_num', v_row->>'reference', '')), '');
    end if;

    if nullif(btrim(coalesce(v_ref_num,'')), '') is null then
      v_ref_num := nullif(btrim(split_part(v_key, '|', 5)), '');
    end if;

    -- ✅ Resolve shift_id + previous import id (used for evidence on schedules)
    v_shift_id := null;
    v_shift_prev_import_id := null;

    select
      ns0.id,
      ns0.latest_import_id
    into
      v_shift_id,
      v_shift_prev_import_id
    from public.nhsp_shifts ns0
    where ns0.source_system = 'NHSP'::public.hr_source_enum
      and ns0.external_row_key = v_key
      and ns0.cancelled_at_utc is null
    order by ns0.updated_at desc nulls last, ns0.created_at desc nulls last
    limit 1;

    if v_shift_id is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: cannot resolve nhsp_shifts.id (shift_id) for external_row_key=% (required for evidence linkage).', v_key;
    end if;

    -- ✅ Find current POS (replacement) for this shift
    select count(*)::int
    into v_existing_pos_count
    from public.timesheets tpos_cnt
    where tpos_cnt.is_adjustment is true
      and tpos_cnt.is_current is true
      and tpos_cnt.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos_cnt.actual_schedule_json) = 'array'
      and tpos_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      );

    select
      tpos.timesheet_id,
      tpos.correction_id,
      tpos.actual_schedule_json,
      coalesce(tpos.candidate_hint_text,tpos.qr_payload_json,'{}'::jsonb)
    into
      v_existing_pos_ts_id,
      v_existing_pos_correction_id,
      v_existing_pos_schedule,
      v_existing_pos_hint
    from public.timesheets tpos
    where tpos.is_adjustment is true
      and tpos.is_current is true
      and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos.actual_schedule_json) = 'array'
      and tpos.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      )
    order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
    limit 1;

    if v_existing_pos_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_pos_tf_locked_by_invoice_id,
        v_existing_pos_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_pos_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_pos_seg_invoice_id := null;

      begin
        select
          nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_pos_seg_invoice_id
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_existing_pos_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_pos_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s2;
      exception when others then
        v_existing_pos_seg_invoice_id := null;
      end;

      v_existing_pos_is_invoiced :=
        (v_existing_pos_tf_locked_by_invoice_id is not null)
        or (v_existing_pos_seg_invoice_id is not null);
    end if;

    -- ✅ Find current NEG (base reversal) for this shift (needed for edge-case deletion)
    select count(*)::int
    into v_existing_neg_count
    from public.timesheets tneg_cnt
    where tneg_cnt.is_adjustment is true
      and tneg_cnt.is_current is true
      and tneg_cnt.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg_cnt.actual_schedule_json) = 'array'
      and tneg_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      );

    select
      tneg.timesheet_id,
      tneg.actual_schedule_json
    into
      v_existing_neg_ts_id,
      v_existing_neg_schedule
    from public.timesheets tneg
    where tneg.is_adjustment is true
      and tneg.is_current is true
      and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg.actual_schedule_json) = 'array'
      and tneg.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      )
    order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
    limit 1;

    if v_existing_neg_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_neg_tf_locked_by_invoice_id,
        v_existing_neg_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_neg_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_neg_seg_invoice_id := null;

      begin
        select
          nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_neg_seg_invoice_id
        from (
          select s3.seg
          from jsonb_array_elements(
            case
              when v_existing_neg_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_neg_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s3(seg)
          where nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s3;
      exception when others then
        v_existing_neg_seg_invoice_id := null;
      end;

      v_existing_neg_is_invoiced :=
        (v_existing_neg_tf_locked_by_invoice_id is not null)
        or (v_existing_neg_seg_invoice_id is not null);
    end if;

    -- Policy X retained-history rule: retain prior correction members even when
    -- the latest truth returns to the original schedule. The pair is amended to
    -- a zero residual only after its canonical pair lifecycle transition.
    v_deleted_redundant_pair := false;

        -- If the latest POS is invoiced, the new series must reverse POS (not the original base).
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is true then
      v_existing_pos_seg := null;
      v_existing_pos_old_start_str := null;
      v_existing_pos_old_end_str := null;
      v_existing_pos_old_break_str := null;
      v_existing_pos_import_id := null;

      -- ✅ Treat the invoiced POS as the effective parent for policy inheritance
      v_base_timesheet_id := v_existing_pos_ts_id;

      select
        coalesce(ts.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        coalesce(ts.submission_mode, 'MANUAL'::public.submission_mode_enum)
      into
        v_parent_sheet_scope,
        v_parent_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if not found then
        v_parent_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
        v_parent_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_existing_pos_schedule is not null and jsonb_typeof(v_existing_pos_schedule) = 'array' then
        v_existing_pos_seg := v_existing_pos_schedule->0;
      end if;

      if v_existing_pos_seg is not null then
        v_existing_pos_old_start_str := nullif(btrim(coalesce(v_existing_pos_seg->>'start_utc','')), '');
        v_existing_pos_old_end_str   := nullif(btrim(coalesce(v_existing_pos_seg->>'end_utc','')), '');
        v_existing_pos_old_break_str := nullif(btrim(coalesce(v_existing_pos_seg->>'break_mins','')), '');


        begin
          if (v_existing_pos_seg ? 'import_id')
             and (v_existing_pos_seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            v_existing_pos_import_id := (v_existing_pos_seg->>'import_id')::uuid;
          else
            v_existing_pos_import_id := null;
          end if;
        exception when others then
          v_existing_pos_import_id := null;
        end;

        begin
          if v_existing_pos_old_start_str is not null then
            v_old_start_utc := v_existing_pos_old_start_str::timestamptz;
          end if;
        exception when others then
          null;
        end;

        begin
          if v_existing_pos_old_end_str is not null then
            v_old_end_utc := v_existing_pos_old_end_str::timestamptz;
          end if;
        exception when others then
          null;
        end;

        begin
          if v_existing_pos_old_break_str is not null and v_existing_pos_old_break_str ~ '^[0-9]+$' then
            v_old_break_mins := v_existing_pos_old_break_str::int;
          end if;
        exception when others then
          null;
        end;

        -- Override evidence "previous import" to the POS import_id when present (so new NEG shows correct raw row)
        if v_existing_pos_import_id is not null then
          v_shift_prev_import_id := v_existing_pos_import_id;
        end if;

        -- Recompute correction_id deterministically using POS-as-old values (stable strings)
        v_old_start_str := coalesce(v_existing_pos_old_start_str, v_old_start_str);
        v_old_end_str   := coalesce(v_existing_pos_old_end_str, v_old_end_str);
        v_old_break_str := coalesce(v_existing_pos_old_break_str, v_old_break_str);

        v_fnv_s :=
          coalesce(p_import_id::text,'') || '|' ||
          coalesce(v_key,'') || '|' ||
          coalesce(v_old_start_str,'') || '|' ||
          coalesce(v_new_start_str,'') || '|' ||
          coalesce(v_old_end_str,'')   || '|' ||
          coalesce(v_new_end_str,'')   || '|' ||
          coalesce(v_old_break_str,'') || '|' ||
          coalesce(v_new_break_str,'');

        v_fnv_h := 2166136261;
        for v_fnv_i in 1..char_length(v_fnv_s) loop
          v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
          v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
        end loop;

        v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
        v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;
      end if;
    end if;

    -- Reset per-key outputs (so we can write a single meaningful audit entry)
    v_rev_ts_id := null;
    v_rep_ts_id := null;

    -- Best-effort invoice number lookup for UI using the current invoices.invoice_no column only.
    -- Do not reference legacy/stale invoice_number or number columns.
    v_invoice_number_text := null;

    if v_invoice_id_detected is not null then
      begin
        select nullif(btrim(coalesce(i.invoice_no::text, '')), '')
        into v_invoice_number_text
        from public.invoices as i
        where i.id = v_invoice_id_detected
        limit 1;
      exception when undefined_table then
        v_invoice_number_text := null;
      when others then
        v_invoice_number_text := null;
      end;
    end if;

    -- Paid minutes (prefer Phase3 row fields; fallback to timestamp diff - break mins)
    begin
      v_old_paid_minutes := nullif(btrim(coalesce(v_row->>'old_paid_minutes','')), '')::int;
    exception when others then
      v_old_paid_minutes := null;
    end;

    begin
      v_new_paid_minutes := nullif(btrim(coalesce(v_row->>'new_paid_minutes','')), '')::int;
    exception when others then
      v_new_paid_minutes := null;
    end;

    if v_old_paid_minutes is null then
      v_old_paid_minutes :=
        greatest(
          0,
          (extract(epoch from (v_old_end_utc - v_old_start_utc)) / 60)::int - coalesce(v_old_break_mins, 0)
        );
    end if;

    if v_new_paid_minutes is null then
      v_new_paid_minutes :=
        greatest(
          0,
          (extract(epoch from (v_new_end_utc - v_new_start_utc)) / 60)::int - coalesce(v_new_break_mins, 0)
        );
    end if;

    v_delta_paid_minutes := coalesce(v_new_paid_minutes, 0) - coalesce(v_old_paid_minutes, 0);

    v_key_ts := coalesce(v_key_ts, '[]'::jsonb);

    -- ✅ Case A: if latest POS exists and is NOT invoiced -> update POS in place and do NOT create new series.
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is false then
      -- Use existing POS correction_id for audit consistency
      if nullif(btrim(coalesce(v_existing_pos_correction_id,'')), '') is not null then
        v_correction_id := v_existing_pos_correction_id;
      end if;

      -- Build replacement schedule from NEW truth (the new import)
      v_shift_date_ymd := to_char((v_new_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
          'start_utc', v_new_start_utc::text,
          'end_utc', v_new_end_utc::text,
          'break_mins', greatest(0, v_new_break_mins),
          'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_key,
          'import_id', p_import_id::text
        )
      );

      v_hint := jsonb_build_object(
        'import_correction', jsonb_build_object(
          'import_id', p_import_id::text,
          'external_row_key', v_key,
          'correction_id', v_correction_id,
          'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
          'updated_from_import_id', p_import_id::text
        )
      );

      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id', v_root_timesheet_id::text,
        'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
      );


      -- This is an amendment within the existing correction unit, not a new
      -- correction unit. Preserve the pair's shared frozen policy envelope and
      -- operation identity; otherwise the untouched reversal and amended
      -- replacement become two invalid one-member units. Only append bounded
      -- provenance for the import that amended the mutable replacement.
      if jsonb_typeof(v_existing_pos_hint) <> 'object'
         or jsonb_typeof(v_existing_pos_hint->'correction_financials_policy_envelope') <> 'object'
         or nullif(v_existing_pos_hint#>>'{correction_financials_policy_envelope,operation,operation_id}','') is null then
        raise exception using message='EXISTING_CORRECTION_POLICY_ENVELOPE_INVALID',errcode='P0001',
          detail=jsonb_build_object(
            'code','EXISTING_CORRECTION_POLICY_ENVELOPE_INVALID',
            'timesheet_id',v_existing_pos_ts_id
          )::text;
      end if;
      v_hint := v_existing_pos_hint || jsonb_build_object(
        'import_correction',coalesce(v_existing_pos_hint->'import_correction','{}'::jsonb)
          || jsonb_build_object('updated_from_import_id',p_import_id::text)
      );
      -- Lock the complete existing correction unit before validating or
      -- repairing its shared parent identity.
      perform 1
       from public.timesheets tlock
       where tlock.correction_id = v_existing_pos_correction_id
         and tlock.is_current = true
         and tlock.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
       order by tlock.timesheet_id
       for update;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      if exists (
        select 1
        from public.timesheets pair_ts
        left join public.timesheets_financials pair_tf
          on pair_tf.timesheet_id=pair_ts.timesheet_id and pair_tf.is_current=true
        where pair_ts.correction_id=v_existing_pos_correction_id
          and pair_ts.is_current=true
          and pair_ts.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (
            pair_ts.authorised_at_server is not null
            or pair_tf.authorised_at_utc is not null
            or pair_tf.paid_at_utc is not null
            or pair_tf.locked_by_invoice_id is not null
            or exists (select 1 from public.invoice_lines il where il.timesheet_id=pair_ts.timesheet_id)
          )
      ) then
        raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',
            'correction_id',v_existing_pos_correction_id,
            'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
          )::text;
      end if;

      select pair_reversal.parent_timesheet_id
      into v_existing_pair_parent_timesheet_id
      from public.timesheets pair_reversal
      where pair_reversal.correction_id=v_existing_pos_correction_id
        and pair_reversal.is_current=true
        and pair_reversal.correction_kind='CHANGED_HOURS_REVERSAL'
      limit 1;

      if v_existing_pair_parent_timesheet_id is null then
        raise exception using message='CORRECTION_PAIR_PARENT_MISSING',errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_PARENT_MISSING',
            'correction_id',v_existing_pos_correction_id
          )::text;
      end if;

      -- A previous implementation could rewrite only the mutable replacement
      -- to the latest base timesheet during replay, splitting the pair's
      -- parent identity. Repair only that exact, complete, mutable pair before
      -- continuing; lifecycle/frozen evidence was rejected above.
      update public.timesheets pair_replacement
      set parent_timesheet_id=v_existing_pair_parent_timesheet_id,
          updated_at=v_now
      where pair_replacement.correction_id=v_existing_pos_correction_id
        and pair_replacement.is_current=true
        and pair_replacement.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and pair_replacement.parent_timesheet_id is distinct from v_existing_pair_parent_timesheet_id;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
          and count(distinct pair_check.parent_timesheet_id) = 1
          and count(pair_check.parent_timesheet_id) = 2
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,

        -- ✅ inherit policy identity from base timesheet
        sheet_scope = v_parent_sheet_scope,
        submission_mode = v_parent_submission_mode,

        updated_at = v_now
      where tup.timesheet_id = v_existing_pos_ts_id;

      v_rep_ts_id := v_existing_pos_ts_id;
      v_updated_existing_replacement := true;

      v_upd_count := v_upd_count + 1;
      v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_pos_ts_id);

      v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
        'kind', 'CHANGED_HOURS_REPLACEMENT',
        'timesheet_id', v_existing_pos_ts_id::text,
        'op', 'UPDATED_IN_PLACE'
      ));
    end if;

    -- Two correction kinds per selected key: reversal + replacement
    if v_updated_existing_replacement is false then
      foreach v_kind in array array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'] loop
        v_kind_op := null;

        if v_kind = 'CHANGED_HOURS_REVERSAL' then
          v_seg_start_utc := v_old_start_utc;
          v_seg_end_utc := v_old_end_utc;
          v_seg_break_mins := greatest(0, v_old_break_mins);
          v_schedule_import_id := v_shift_prev_import_id;
        else
          v_seg_start_utc := v_new_start_utc;
          v_seg_end_utc := v_new_end_utc;
          v_seg_break_mins := greatest(0, v_new_break_mins);
          v_schedule_import_id := p_import_id;
        end if;

        v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

        v_hint := jsonb_build_object(
          'import_correction', jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'correction_id', v_correction_id,
            'correction_kind', v_kind
          )
        );

        v_hint := v_hint || jsonb_build_object(
          'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
          'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'root_timesheet_id', v_root_timesheet_id::text,
          'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
        );


        v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

        v_shift_label_norm :=
          regexp_replace(
            regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
            '[^\w\s\-@&\/,:]',
            '',
            'g'
          );

        -- ✅ booking_id must be UNIQUE per correction kind (REVERSAL vs REPLACEMENT)
        v_hash_hex := substring(
          encode(
            extensions.digest(
              convert_to(
                (v_booking_base || '|shift_label_norm=' || coalesce(v_shift_label_norm, '')),
                'utf8'
              ),
              'sha256'::text
            ),
            'hex'
          )
          from 1 for 16
        );
        v_booking_id := 'bk_' || v_hash_hex;

        -- ✅ Schedule includes evidence linkage (shift_id, external_row_key, import_id)
        v_schedule := jsonb_build_array(
          jsonb_build_object(
            'date', v_shift_date_ymd,
            'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
            'start_utc', v_seg_start_utc::text,
            'end_utc', v_seg_end_utc::text,
            'break_mins', v_seg_break_mins,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'shift_id', v_shift_id::text,
            'external_row_key', v_key,
            'import_id', case when v_schedule_import_id is null then null else v_schedule_import_id::text end
          )
        );

        -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
        v_existing_ts_id := null;

        select t.timesheet_id
        into v_existing_ts_id
        from public.timesheets t
        where t.correction_id = v_correction_id
          and t.correction_kind = v_kind
        order by t.is_current desc, t.version desc
        limit 1
        for update;

        if v_existing_ts_id is not null then
          -- Ensure there is an adjustment contract_week linked; reuse it if present.
          v_existing_cw_id := null;
          v_existing_cw_seq := null;
          v_existing_cw_is_adjustment := null;

          select
            cw.id,
            cw.additional_seq,
            cw.is_adjustment
          into
            v_existing_cw_id,
            v_existing_cw_seq,
            v_existing_cw_is_adjustment
          from public.contract_weeks cw
          where cw.timesheet_id = v_existing_ts_id
            and cw.contract_id = v_contract_id
            and cw.week_ending_date = v_week_ending_date
          limit 1
          for update;

          if v_existing_cw_id is not null then
            if v_existing_cw_is_adjustment is not true or coalesce(v_existing_cw_seq,0) <= 0 then
              update public.contract_weeks cw2
              set
                is_adjustment = true,
                status = 'SUBMITTED'::public.contract_week_status_enum,
                updated_at = v_now
              where cw2.id = v_existing_cw_id;
            end if;

             update public.timesheets t2
            set
              actual_schedule_json = v_schedule,
              qr_payload_json = v_hint,

              -- ✅ inherit policy identity from base timesheet
              sheet_scope = v_parent_sheet_scope,
              submission_mode = v_parent_submission_mode,
              parent_timesheet_id = v_base_timesheet_id,

              updated_at = v_now
            where t2.timesheet_id = v_existing_ts_id;

            if v_kind = 'CHANGED_HOURS_REVERSAL' then
              v_rev_ts_id := v_existing_ts_id;
            else
              v_rep_ts_id := v_existing_ts_id;
            end if;

            v_upd_count := v_upd_count + 1;
            v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
            v_kind_op := 'UPDATED';

            v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
              'kind', v_kind,
              'timesheet_id', v_existing_ts_id::text,
              'op', v_kind_op
            ));

            continue;
          end if;

          -- If we have an existing correction timesheet but no linked contract_week, create one.
          select coalesce(max(cw3.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cw3
          where cw3.contract_id = v_contract_id
            and cw3.week_ending_date = v_week_ending_date
            and cw3.is_adjustment is true;

          insert into public.contract_weeks(
            contract_id,
            week_ending_date,
            additional_seq,
            is_adjustment,
            status,
            timesheet_id,
            created_at,
            updated_at
          )
          values (
            v_contract_id,
            v_week_ending_date,
            v_next_additional_seq,
            true,
            'SUBMITTED'::public.contract_week_status_enum,
            v_existing_ts_id,
            v_now,
            v_now
          )
          returning id into v_existing_cw_id;
          update public.timesheets t2b
          set
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,

            -- ✅ inherit policy identity from base timesheet
            sheet_scope = v_parent_sheet_scope,
            submission_mode = v_parent_submission_mode,
            parent_timesheet_id = v_base_timesheet_id,

            updated_at = v_now
          where t2b.timesheet_id = v_existing_ts_id;

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_existing_ts_id;
          else
            v_rep_ts_id := v_existing_ts_id;
          end if;

          v_upd_count := v_upd_count + 1;
          v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
          v_kind_op := 'UPDATED';

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_existing_ts_id::text,
            'op', v_kind_op
          ));

          continue;
        end if;

        -- No existing correction timesheet: create new adjustment contract_week + timesheet
        v_ts_id := null;

        for v_try in 1..5 loop
          select coalesce(max(cw4.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cw4
          where cw4.contract_id = v_contract_id
            and cw4.week_ending_date = v_week_ending_date
            and cw4.is_adjustment is true;

          begin
               insert into public.timesheets(
              booking_id,
              version,
              is_current,
              status,
              occupant_key_norm,
              hospital_norm,
              ward_norm,
              job_title_norm,
              shift_label_norm,
              week_ending_date,
              contract_id,
              sheet_scope,
              submission_mode,
              line_type,
              manual_pdf_r2_key,
              actual_schedule_json,
              additional_units_week,
              additional_units_per_day,
              day_references_json,
              qr_status,
              qr_token,
              qr_generated_at,
              qr_scanned_at,
              qr_scan_info_json,
              qr_r2_key,
              qr_payload_json,
              created_at,
              updated_at,
              is_adjustment,
              parent_timesheet_id,
              candidate_hint_text,
              correction_id,
              correction_kind,
              adjustment_origin
            )
            values (
              v_booking_id,
              1,
              true,
              'RECEIVED'::public.timesheet_status_enum,
              lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
              lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
              lower(coalesce(v_contract_ward_hint,'contract')),
              lower(coalesce(v_contract_role,'weekly')),
              v_shift_label_norm,
              v_week_ending_date,
              v_contract_id,

              -- ✅ inherit policy identity from base timesheet
              v_parent_sheet_scope,
              v_parent_submission_mode,

              'HOURS'::public.timesheet_line_type_enum,
              null,
              v_schedule,
              '{}'::jsonb,
              '{}'::jsonb,
              '{}'::jsonb,
              null,
              null,
              null,
              null,
              '{}'::jsonb,
              null,
              v_hint,
              v_now,
              v_now,
              true,

              -- ✅ link to parent/base timesheet (may be null if not provided)
              v_base_timesheet_id,

              v_hint,
              v_correction_id,
              v_kind,
              'IMPORT_CORRECTION'
            )
            returning timesheet_id into v_ts_id;


            if v_kind = 'CHANGED_HOURS_REVERSAL' then
              v_rev_ts_id := v_ts_id;
            else
              v_rep_ts_id := v_ts_id;
            end if;

            insert into public.contract_weeks(
              contract_id,
              week_ending_date,
              additional_seq,
              is_adjustment,
              status,
              timesheet_id,
              created_at,
              updated_at
            )
            values (
              v_contract_id,
              v_week_ending_date,
              v_next_additional_seq,
              true,
              'SUBMITTED'::public.contract_week_status_enum,
              v_ts_id,
              v_now,
              v_now
            );

            v_ins_count := v_ins_count + 1;
            v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);
            v_kind_op := 'CREATED';

            v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
              'kind', v_kind,
              'timesheet_id', v_ts_id::text,
              'op', v_kind_op
            ));

            exit;
          exception
            when unique_violation then
              v_ts_id := null;
          end;

          exit when v_ts_id is not null;
        end loop;

        if v_ts_id is null then
          raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Failed to allocate correction timesheet/contract_week after retries (external_row_key=% kind=%)', v_key, v_kind;
        end if;

      end loop; -- kind loop
    end if; -- updated_existing_replacement

    -- ─────────────────────────────────────────────
    -- ✅ User-facing audit entries (timesheet modal + invoice history)
    -- ─────────────────────────────────────────────
    begin
      -- Timesheet audit: reversal
      if v_rev_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rev_ts_id::text,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'evidence_import_id', case when v_shift_prev_import_id is null then null else v_shift_prev_import_id::text end,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'op', case
                    when v_rev_ts_id = any(coalesce(v_created_ts_ids, '{}'::uuid[])) then 'CREATED'
                    when v_rev_ts_id = any(coalesce(v_updated_ts_ids, '{}'::uuid[])) then 'UPDATED'
                    else 'UPSERT'
                  end
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Timesheet audit: replacement
      if v_rep_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rep_ts_id::text,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'evidence_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'op', case
                    when v_rep_ts_id = any(coalesce(v_created_ts_ids, '{}'::uuid[])) then 'CREATED'
                    when v_rep_ts_id = any(coalesce(v_updated_ts_ids, '{}'::uuid[])) then 'UPDATED'
                    else 'UPSERT'
                  end
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Invoice history entry
      if v_invoice_id_detected is not null then
        perform public._inv_write_audit(
          p_actor_user_id,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', v_invoice_id_detected::text,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'reversal_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'invoices',
          v_invoice_id_detected::text,
          null,
          'IMPORT_CORRECTION',
          null,
          null,
          null
        );
      end if;
    exception when others then
      null;
    end;

    if v_sample_n < 20 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'external_row_key', v_key,
        'is_invoiced', v_is_invoiced,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'week_ending_date', v_week_ending_date::text,
        'base_timesheet_id', case when v_base_timesheet_id is null then null else v_base_timesheet_id::text end,
        'correction_id', v_correction_id,
        'replacement_updated_in_place', v_updated_existing_replacement,
        'redundant_pair_deleted', v_deleted_redundant_pair,
        'timesheets', v_key_ts
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop; -- selected keys loop

  perform public._imp_debug_audit(
    p_actor_user_id,
    'NHSP_CORRECTION_SERIES_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
      'inserted_count', v_ins_count,
      'updated_count', v_upd_count,
      'created_timesheet_ids_count', coalesce(array_length(v_created_ts_ids, 1), 0),
      'updated_timesheet_ids_count', coalesce(array_length(v_updated_ts_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id', p_import_id,
    'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
    'skipped_count', v_skipped_count,
    'inserted_count', v_ins_count,
    'updated_count', v_upd_count,
    'created_timesheet_ids', to_jsonb(coalesce(v_created_ts_ids, '{}'::uuid[])),
    'updated_timesheet_ids', to_jsonb(coalesce(v_updated_ts_ids, '{}'::uuid[]))
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_CORRECTION_SERIES_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'last_external_row_key', v_last_key,
        'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
        'inserted_count', v_ins_count,
        'updated_count', v_upd_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- Restore hr_weekly_apply_transactional from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.hr_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET plpgsql_check.mode TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;
  v_import_client_id uuid;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := '[]'::jsonb;

  -- Expanded only from persisted, selected review decisions.
  v_invalidation_actions jsonb := '[]'::jsonb;
  v_invalidation_actions_count int := 0;

  -- normalized selections
  v_selected_action_ids text[] := array[]::text[];
  v_selected_truth_keys text[] := array[]::text[];
  v_selected_cancel_shift_ids uuid[] := array[]::uuid[];

  -- derived mode key sets
  v_mode_a_external_keys text[] := array[]::text[];
  v_mode_b_external_keys text[] := array[]::text[];

  -- selected truth keys constrained to MODE_B
  v_selected_truth_keys_mode_b text[] := array[]::text[];

  -- Mode B tick-only enforced lists
  v_force_keys_final text[] := array[]::text[];
  v_skip_keys_final text[] := array[]::text[];

  -- changed-hours partition (selected keys only, MODE_B)
  v_invoiced_changed_keys text[] := array[]::text[];
  v_not_invoiced_changed_keys text[] := array[]::text[];
  v_protected_source_timesheet_ids uuid[] := array[]::uuid[];
  v_force_keys_non_invoiced text[] := array[]::text[];

  v_phase3_result jsonb := null;
  v_changed_preflight jsonb := null;
  v_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_reauthorise_timesheet_ids uuid[] := array[]::uuid[];
  v_lifecycle_items jsonb := '[]'::jsonb;
  v_unauthorise_result jsonb := null;

  -- Phase 1 / 1.5 (MODE_B)
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- cancellations (MODE_B)
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- mirror (MODE_A)
  v_mirror_result jsonb := null;

  -- validation (MODE_A)
  v_weekly_val_payload jsonb := null;
  v_validations_upserted int := 0;
  v_mismatched_tsids uuid[] := array[]::uuid[];

  -- ✅ NEW: validation-changed timesheets (MODE_A) that must trigger TSFIN recompute
  v_validation_changed_timesheet_ids uuid[] := array[]::uuid[];

  -- ✅ NEW: count of ref clears due to missing shifts (MODE_A)
  v_mode_a_ref_cleared_count int := 0;

  -- ✅ NEW: count of ref sets due to matched shifts (MODE_A)
  v_mode_a_ref_set_count int := 0;

  -- ✅ NEW: timesheets whose reference truth changed (for post-apply QR reissue + regen)
  v_ref_updated_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_updated_timesheet_ids_count int := 0;

  -- ✅ NEW: consolidated email jobs + items
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain (MODE_B + MODE_A validation changes)
  v_affected_timesheet_ids uuid[] := array[]::uuid[];
  -- MODE_B targets are ordinary authoritative work.  MODE_A targets are kept
  -- in a separate array and enter auto-authorisation only after the stricter
  -- complete whole-timesheet validation gate below.
  v_authoritative_affected_timesheet_ids uuid[] := array[]::uuid[];
  v_validation_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];

  -- policy A replacement-day
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- debug counts
  v_steps jsonb := '[]'::jsonb;

  v_selected_action_ids_count int := 0;
  v_selected_row_keys_count int := 0;
  v_selected_cancel_shift_ids_count int := 0;

  v_mode_a_ok_keys_total int := 0;
  v_mode_b_ok_keys_total int := 0;

  v_force_keys_count int := 0;
  v_skip_keys_count int := 0;

  v_invoiced_changed_keys_count int := 0;
  v_not_invoiced_changed_keys_count int := 0;

  v_cancellations_count int := 0;

  v_phase3_created_count int := 0;
  v_phase3_updated_count int := 0;
  v_cancel_adjustment_count int := 0;
  v_correction_timesheets_created_count int := 0;

  v_val_rows_count int := 0;
  v_email_actions_count int := 0;
  v_email_jobs_count int := 0;

  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_cancel_shift_ids jsonb := '[]'::jsonb;
  v_selected_action_ids_sample jsonb := '[]'::jsonb;

  v_mode_b_phase1_called boolean := false;
  v_mode_b_phase15_called boolean := false;
  v_mode_b_cancellations_called boolean := false;
  v_mode_b_phase3_called boolean := false;

  v_mode_b_should_run_phase1 boolean := false;
  v_mode_b_should_run_phase15 boolean := false;
  v_mode_b_should_run_cancellations boolean := false;
  v_mode_b_should_run_phase3 boolean := false;

  -- Server-owned review contract. A review state is mandatory for every call.
  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;
  v_post_commit_email_action_ids jsonb := '[]'::jsonb;

  v_phase1_shifts_created int := null;
  v_phase1_shifts_updated int := null;

  v_last_shift_id uuid := null;

  -- ─────────────────────────────────────────────
  -- ✅ ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER SHIFTS (invariant)
  -- ─────────────────────────────────────────────
  v_ensure_pairs_count int := 0;
  v_ensure_pairs_skipped_no_active int := 0;

  v_ensure_base_week_created_count int := 0;
  v_ensure_base_week_existing_count int := 0;

  v_ensure_timesheet_created_count int := 0;
  v_ensure_timesheet_reused_count int := 0;
  v_ensure_timesheet_missing_reference_count int := 0;

  v_ensure_shifts_attached_count int := 0;
  v_ensure_shifts_relinked_invalid_ts_count int := 0;
  v_ensure_remaining_active_detached_count int := 0;

  -- ✅ NEW: MODE_A shift→timesheet linking (fix evidence + HR crosscheck + ref propagation)
  v_mode_a_shifts_attached_count int := 0;
  v_mode_a_ts_linked_count int := 0;

  v_ensure_sample_pairs jsonb := '[]'::jsonb;
  v_ensure_sample_created_ts_ids jsonb := '[]'::jsonb;

  -- loop vars for ensure
  v_pair_contract_id uuid;
  v_pair_candidate_id uuid;
  v_pair_client_id uuid;
  v_pair_week_ending_date date;

  v_active_count int := 0;

  v_base_week_id uuid := null;
  v_base_week_ts_id uuid := null;

  v_ts_exists boolean := false;

  v_candidate_display_name text := null;
  v_candidate_tms_ref text := null;
  v_client_name text := null;
  v_contract_display_site text := null;
  v_contract_ward_hint text := null;
  v_contract_role text := null;

  v_occupant_norm text := null;
  v_hospital_norm text := null;
  v_ward_norm text := null;
  v_role_norm text := null;

  v_booking_base text := null;
  v_hash_hex text := null;
  v_booking_id text := null;
  v_shift_label_norm text := null;

  v_new_ts_id uuid := null;

  v_attached_null_count int := 0;
  v_relinked_invalid_count int := 0;

  v_sqlstate text;
  v_err text;
begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));

  -- ─────────────────────────────────────────────
  -- 0) Validate import + header fields
  -- ─────────────────────────────────────────────
  select
    upper(coalesce(hi.source_system::text, '')),
    hi.client_id
  into
    v_import_source_system,
    v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'hr_weekly_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_apply_transactional: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'hr_weekly_apply_transactional: import % missing client_id.', p_import_id;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
    or not(v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;

  -- The server guard has reduced the request to complete, ready
  -- candidate/client units.  Keep that boundary available to every MODE_A
  -- validation/mirror step; otherwise a partial batch could validate or link
  -- rows belonging to a candidate that the operator deliberately left
  -- pending.
  drop table if exists pg_temp.tmp_review_batch_units;
  create temporary table tmp_review_batch_units(
    candidate_id uuid not null,
    client_id uuid not null,
    primary key(candidate_id,client_id)
  ) on commit drop;
  insert into tmp_review_batch_units(candidate_id,client_id)
  select distinct d.candidate_id,d.client_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.candidate_id is not null and d.client_id is not null
  on conflict do nothing;
  if not exists(select 1 from tmp_review_batch_units) then
    raise exception 'IMPORT_REVIEW_BATCH_SCOPE_EMPTY' using errcode='55000';
  end if;
  select coalesce(jsonb_agg(to_jsonb(case when d.action_kind='APPLY_CANCELLATION' then 'CANCEL:'||d.shift_id::text else 'ROW:'||d.source_identity end) order by d.action_id),'[]'::jsonb)
    into v_actions_json
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION');
  select coalesce(jsonb_agg(jsonb_build_object('timesheet_id',d.timesheet_id,'comparison_key',d.source_identity,'invalidate',true) order by d.action_id),'[]'::jsonb)
    into v_invalidation_actions
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(to_jsonb(d.action_id) order by d.action_id),'[]'::jsonb)
    into v_post_commit_email_action_ids
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER');
  v_email_actions_count:=jsonb_array_length(v_post_commit_email_action_ids);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK','client_id',v_import_client_id::text));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_invalidation_actions) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: invalidation_actions must be a JSON array.';
  end if;

  v_invalidation_actions_count := jsonb_array_length(v_invalidation_actions);

  create temporary table tmp_sel_ids(
    action_id text primary key
  ) on commit drop;

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x.value), '')
  from jsonb_array_elements_text(v_actions_json) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  if exists (
    select 1
    from tmp_sel_ids s
    where s.action_id !~ '^(ROW|CANCEL):'
  ) then
    raise exception 'hr_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
  end if;

  select coalesce(array_agg(s.action_id order by s.action_id), array[]::text[])
  into v_selected_action_ids
  from tmp_sel_ids s;

  select coalesce(array_agg(distinct substring(s.action_id from 5) order by substring(s.action_id from 5)), array[]::text[])
  into v_selected_truth_keys
  from tmp_sel_ids s
  where s.action_id like 'ROW:%';

  select coalesce(array_agg(distinct (substring(s.action_id from 8))::uuid order by (substring(s.action_id from 8))::uuid), array[]::uuid[])
  into v_selected_cancel_shift_ids
  from tmp_sel_ids s
  where s.action_id like 'CANCEL:%';

  v_selected_action_ids_count := coalesce(array_length(v_selected_action_ids, 1), 0);
  v_selected_row_keys_count := coalesce(array_length(v_selected_truth_keys, 1), 0);
  v_selected_cancel_shift_ids_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  select to_jsonb(coalesce(array_agg(x.a), array[]::text[]))
  into v_selected_action_ids_sample
  from (
    select a as a
    from unnest(coalesce(v_selected_action_ids, array[]::text[])) as a
    order by a
    limit 20
  ) as x;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','SELECTION_PARSED',
      'selected_action_ids_count', v_selected_action_ids_count,
      'selected_row_keys_count', v_selected_row_keys_count,
      'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
      'email_actions_count', v_email_actions_count,
      'invalidation_actions_count', v_invalidation_actions_count,
      'selected_action_ids_sample', v_selected_action_ids_sample
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ tmp_aff_ts must exist early (PK + ON CONFLICT supported)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_aff_ts;
  create temporary table tmp_aff_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ✅ NEW: reference-updated timesheets (used for QR reissue + regen decisions)
  drop table if exists pg_temp.tmp_ref_updated_ts;
  create temporary table tmp_ref_updated_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 + compute per-group authority through the
  --    shared current-setting core used by staging and catalogue generation.
  -- ─────────────────────────────────────────────
  create temporary table tmp_p2_all on commit drop as
  select *
  from public.weekly_import_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

  create temporary table tmp_p2_ok on commit drop as
  select
    p2.external_row_key,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    p2.week_ending_date,
    p2.work_date,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.contract_id is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.week_ending_date is not null;

  create temporary table tmp_group_mode on commit drop as
  select distinct
    t.contract_id,
    t.candidate_id,
    t.client_id,
    t.week_ending_date,
    ('grp:' || t.contract_id::text || ':' || t.week_ending_date::text || ':' || t.candidate_id::text) as group_id,
    case a.authority_mode when 'AUTHORITATIVE' then 'MODE_B'
      when 'VALIDATION_ONLY' then 'MODE_A' else 'OUT_OF_SCOPE' end as mode
  from (
    select distinct p2ok.contract_id, p2ok.candidate_id, p2ok.client_id, p2ok.week_ending_date
    from tmp_p2_ok p2ok
  ) as t
  join public.contracts c
    on c.id = t.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',c.id,c.client_id,t.week_ending_date) a;

  if exists(select 1 from tmp_group_mode gm join tmp_review_batch_units bu
      on bu.candidate_id=gm.candidate_id and bu.client_id=gm.client_id
      where gm.mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_IMPORT_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_p2_ok_mode on commit drop as
  select
    p2ok.external_row_key,
    p2ok.candidate_id,
    p2ok.client_id,
    p2ok.contract_id,
    p2ok.week_ending_date,
    p2ok.work_date,
    gm.group_id,
    gm.mode
  from tmp_p2_ok p2ok
  join tmp_group_mode gm
    on gm.contract_id = p2ok.contract_id
   and gm.candidate_id = p2ok.candidate_id
   and gm.week_ending_date = p2ok.week_ending_date;

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_a_external_keys
  from tmp_p2_ok_mode m
  join tmp_review_batch_units bu
    on bu.candidate_id=m.candidate_id and bu.client_id=m.client_id
  where m.mode = 'MODE_A';

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_b_external_keys
  from tmp_p2_ok_mode m
  where m.mode = 'MODE_B';

  v_mode_a_ok_keys_total := coalesce(array_length(v_mode_a_external_keys, 1), 0);
  v_mode_b_ok_keys_total := coalesce(array_length(v_mode_b_external_keys, 1), 0);

  if exists (
    select 1
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    left join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
    where mbok.external_row_key is null
  ) then
    raise exception 'hr_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not MODE_B (timesheet required).';
  end if;

  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_mode_b
  from (
    select distinct k.external_row_key
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
  ) as k;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','PHASE2_OK_LOADED',
      'mode_a_ok_keys_total', v_mode_a_ok_keys_total,
      'mode_b_ok_keys_total', v_mode_b_ok_keys_total
    )
  );

  -- ─────────────────────────────────────────────
  -- 3) MODE_B tick = PROCEED (no decisions)
  -- ─────────────────────────────────────────────
  v_force_keys_final := coalesce(v_selected_truth_keys_mode_b, array[]::text[]);

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_mode_b_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  v_force_keys_count := coalesce(array_length(v_force_keys_final, 1), 0);
  v_skip_keys_count := coalesce(array_length(v_skip_keys_final, 1), 0);
  v_cancellations_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  -- Only MODE_B is import-authoritative.  Ignore unrelated expense-only
  -- timesheets, but refuse to reuse a base contract-week timesheet occupied by
  -- calculated expenses or to amend/reverse an imported shift whose own linked
  -- timesheet contains them.  This runs before any source mutation.
  if exists (
    select 1
    from (
      select cw.timesheet_id
      from tmp_p2_ok_mode p2
      join public.contract_weeks cw
        on cw.contract_id=p2.contract_id
       and cw.week_ending_date=p2.week_ending_date
       and cw.is_adjustment=false
       and coalesce(cw.additional_seq,0)=0
      where p2.mode='MODE_B'
        and p2.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
        and cw.timesheet_id is not null
        and not exists (
          select 1
          from public.nhsp_shifts existing_import_shift
          where existing_import_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and existing_import_shift.client_id=v_import_client_id
            and existing_import_shift.external_row_key=p2.external_row_key
            and existing_import_shift.cancelled_at_utc is null
        )
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      where ns.source_system='HEALTHROSTER'::public.hr_source_enum
        and ns.client_id=v_import_client_id
        and ns.timesheet_id is not null
        and (
          ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
          or ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        )
    ) expense_target
    where public._import_review_timesheet_has_calculated_expenses_core_v1(expense_target.timesheet_id)
  ) then
    raise exception using
      message='IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
      errcode='P0001',
      detail=jsonb_build_object(
        'code','IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
        'message','Timesheet occupied by expenses. Remove the expenses from this timesheet, save or recalculate it, then choose Recheck. Expenses must be invoiced on a separate timesheet for import-authoritative work; no import mutation was applied.'
      )::text;
  end if;

  v_mode_b_should_run_phase1 := (v_force_keys_count > 0);
  v_mode_b_should_run_phase15 := (v_force_keys_count > 0);
  v_mode_b_should_run_cancellations := (v_cancellations_count > 0);

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','TICK_PROCEED_KEYS_READY',
      'mode_b_force_keys_count', v_force_keys_count,
      'mode_b_skip_keys_count', v_skip_keys_count,
      'mode_b_cancellations_count', v_cancellations_count
    )
  );

  -- ─────────────────────────────────────────────
  -- 4) MODE_B: do NOT run truth mutation work when there is nothing to apply
  -- ─────────────────────────────────────────────
  if (v_mode_b_should_run_phase1 is false) and (v_mode_b_should_run_cancellations is false) then
    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','MODE_B_NOOP_GUARD',
        'reason','NO_SELECTION_NO_CANCELLATION => SKIP_MODE_B_TRUTH_MUTATION',
        'should_run_phase1', false,
        'should_run_phase15', false,
        'should_run_phase3', false,
        'should_run_cancellations', false
      )
    );
  else
    -- MODE_B PHASE3 / PHASE1 / PHASE1.5 / CANCELLATIONS BLOCKS
    create temporary table tmp_changed_sel on commit drop as
    select
      ch.external_row_key,
      ch.timesheet_id,
      ch.is_paid,
      ch.is_invoiced
    from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER') as ch
    where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is true
       or cs.is_paid is true;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_not_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is false
      and cs.is_paid is false;

    v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
    v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

    select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id), array[]::uuid[])
      into v_changed_timesheet_ids
    from tmp_changed_sel cs
    where cs.timesheet_id is not null;

    select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id),array[]::uuid[])
      into v_protected_source_timesheet_ids
    from tmp_changed_sel cs
    where cs.timesheet_id is not null
      and (cs.is_invoiced is true or cs.is_paid is true);

    if coalesce(array_length(v_changed_timesheet_ids, 1), 0) > 0 then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := v_changed_timesheet_ids,
        p_action := 'IMPORT_CHANGED_HOURS',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_changed_preflight;

      if coalesce((v_changed_preflight->>'allowed')::boolean, false) is not true then
        raise exception using
          message = 'IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',
          errcode = 'P0001',
          detail = v_changed_preflight::text;
      end if;

      if exists (
        select 1
        from tmp_changed_sel cs
        where cs.is_invoiced is false
          and exists (
            select 1 from public.timesheets_financials paid_tf
            where paid_tf.timesheet_id = cs.timesheet_id
              and paid_tf.paid_at_utc is not null
          )
          and not exists (
            select 1 from public.timesheets_financials current_tf
            where current_tf.timesheet_id = cs.timesheet_id
              and current_tf.is_current = true
              and current_tf.stale_reason = 'IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
              and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false) = true
          )
      ) then
        raise exception using
          message = 'PAID_UNINVOICED_ROLLOVER_REQUIRED',
          errcode = 'P0001',
          detail = jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED',
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER',
              'AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW',
            'timesheet_ids',to_jsonb(v_changed_timesheet_ids)
          )::text;
      end if;
    end if;

    -- Mode B only: preserve the lifecycle state of authorised, mutable source
    -- timesheets.  Reauthorisation is deliberately deferred until the Worker
    -- has completed the bounded TSFIN refresh for this committed operation.
    select coalesce(array_agg(distinct lifecycle_scope.timesheet_id order by lifecycle_scope.timesheet_id),array[]::uuid[])
    into v_reauthorise_timesheet_ids
    from (
      select cs.timesheet_id
      from tmp_changed_sel cs
      join public.timesheets ts on ts.timesheet_id=cs.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where cs.timesheet_id is not null
        and cs.is_invoiced is false
        and cs.is_paid is false
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      join public.timesheets ts on ts.timesheet_id=ns.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        and ns.timesheet_id is not null
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'paid')::boolean,false)=false
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'invoice_locked')::boolean,false)=false
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
    ) lifecycle_scope;

    if cardinality(v_reauthorise_timesheet_ids)>100 then
      raise exception 'IMPORT_REVIEW_REAUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
    end if;
    if cardinality(v_reauthorise_timesheet_ids)>0 then
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',target_id::text,
        'expected_timesheet_id',target_id::text
      ) order by target_id),'[]'::jsonb)
      into v_lifecycle_items
      from unnest(v_reauthorise_timesheet_ids) as lifecycle_target(target_id);

      select public.timesheet_unauthorise_bulk_atomic(v_lifecycle_items,p_actor_user_id,v_now)
      into v_unauthorise_result;
      if coalesce((v_unauthorise_result->>'ok')::boolean,false) is not true
        or coalesce((v_unauthorise_result->>'all_success')::boolean,false) is not true then
        raise exception using message='IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',errcode='P0001',
          detail=jsonb_build_object(
            'code','IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',
            'timesheet_ids',to_jsonb(v_reauthorise_timesheet_ids),
            'failure_count',coalesce((v_unauthorise_result->>'failure_count')::int,cardinality(v_reauthorise_timesheet_ids))
          )::text;
      end if;
      v_steps:=v_steps||jsonb_build_array(jsonb_build_object(
        'step','CANONICAL_UNAUTHORISE_COMPLETE',
        'reauthorise_timesheet_count',cardinality(v_reauthorise_timesheet_ids)
      ));
    end if;

    v_mode_b_should_run_phase3 := (v_invoiced_changed_keys_count > 0);

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','CHANGED_HOURS_PARTITIONED',
        'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
        'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count
      )
    );

    create temporary table tmp_selected_replacement_keys(
      candidate_id uuid,
      client_id uuid,
      old_work_date date,
      replacement_day_key text
    ) on commit drop;

    if array_length(v_force_keys_final, 1) is not null then
      create temporary table tmp_sel_truth_p2 on commit drop as
      select
        m.external_row_key,
        m.candidate_id,
        m.client_id,
        m.work_date as import_work_date
      from tmp_p2_ok_mode m
      where m.mode = 'MODE_B'
        and m.external_row_key = any(v_force_keys_final);

      create temporary table tmp_existing_by_key on commit drop as
      select distinct on (ns.external_row_key)
        ns.external_row_key,
        ns.id as shift_id,
        ns.candidate_id as candidate_id,
        ns.client_id as client_id,
        ns.work_date as old_work_date
      from public.nhsp_shifts ns
      where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.cancelled_at_utc is null
        and ns.external_row_key = any(v_force_keys_final)
        and ns.work_date is not null
      order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

      insert into tmp_selected_replacement_keys(candidate_id, client_id, old_work_date, replacement_day_key)
      select distinct
        (coalesce(ex.candidate_id, st.candidate_id))::uuid as candidate_id,
        (coalesce(ex.client_id, st.client_id))::uuid as client_id,
        ex.old_work_date as old_work_date,
        ((coalesce(ex.candidate_id, st.candidate_id))::text || '|' ||
         (coalesce(ex.client_id, st.client_id))::text || '|' ||
         (ex.old_work_date)::text) as replacement_day_key
      from tmp_sel_truth_p2 st
      join tmp_existing_by_key ex
        on ex.external_row_key = st.external_row_key
      where ex.old_work_date is not null
        and st.import_work_date is not null
        and ex.old_work_date <> st.import_work_date;

      select coalesce(array_agg(x::text), array[]::text[])
      into v_selected_cancel_shift_id_set
      from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as x;

      if exists (select 1 from tmp_selected_replacement_keys) then
        create temporary table tmp_required_cancel_ids on commit drop as
        select distinct
          rk.replacement_day_key,
          ns2.id as shift_id
        from tmp_selected_replacement_keys rk
        join public.nhsp_shifts ns2
          on ns2.source_system = 'HEALTHROSTER'::public.hr_source_enum
         and ns2.client_id = v_import_client_id
         and ns2.cancelled_at_utc is null
         and ns2.candidate_id = rk.candidate_id
         and ns2.client_id = rk.client_id
         and ns2.work_date = rk.old_work_date;

        if exists (
          select 1
          from tmp_required_cancel_ids rc
          left join unnest(coalesce(v_selected_cancel_shift_id_set, array[]::text[])) as sel(shift_id_text)
            on sel.shift_id_text = rc.shift_id::text
          where sel.shift_id_text is null
        ) then
          raise exception 'hr_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
        end if;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','POLICY_A_OK'));

    if v_mode_b_should_run_phase3 then
      select public.hr_weekly_phase3_apply_adjustment_truth(
        p_import_id := p_import_id,
        p_selected_external_row_keys := v_invoiced_changed_keys,
        p_actor_user_id := p_actor_user_id
      )
      into v_phase3_result;

      v_mode_b_phase3_called := true;
    end if;

    v_phase3_created_count := jsonb_array_length(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb));
    v_phase3_updated_count := jsonb_array_length(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb));

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE3_CORRECTIONS_DONE',
        'phase3_called', v_mode_b_phase3_called,
        'phase3_created_count', v_phase3_created_count,
        'phase3_updated_count', v_phase3_updated_count
      )
    );

    if v_mode_b_should_run_phase1 then
      select public.hr_autoprocess_apply_phase1(
        import_id := p_import_id,
        selected_group_ids := array[]::text[],
        p_skip_external_row_keys := v_skip_keys_final,
        p_force_overwrite_external_row_keys := v_force_keys_final
      )
      into v_phase1_result;

      v_mode_b_phase1_called := true;

      v_phase1_shifts_created :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_created')
           and (v_phase1_result->>'shifts_created') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_created')::int
          else null
        end;

      v_phase1_shifts_updated :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_updated')
           and (v_phase1_result->>'shifts_updated') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_updated')::int
          else null
        end;

      if v_mode_b_should_run_phase15 then
        create temporary table tmp_phase15_rows on commit drop as
        select *
        from public.weekly_import_apply_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

        select count(*)::int
        into v_phase15_ok
        from tmp_phase15_rows r
        where upper(coalesce(r.action::text,'')) = 'OK';

        select count(*)::int
        into v_phase15_updated
        from tmp_phase15_rows r
        where coalesce(r.shift_updated,false) is true;

        v_mode_b_phase15_called := true;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE1_PHASE15_DONE',
        'phase1_called', v_mode_b_phase1_called,
        'phase15_called', v_mode_b_phase15_called,
        'phase1_shifts_created', v_phase1_shifts_created,
        'phase1_shifts_updated', v_phase1_shifts_updated,
        'phase15_ok_rows', v_phase15_ok,
        'phase15_shift_updated_rows', v_phase15_updated
      )
    );

    if v_mode_b_should_run_cancellations then
      create temporary table tmp_cancel_meta on commit drop as
      select
        ns.id as shift_id,
        ns.candidate_id,
        ns.client_id,
        ns.work_date
      from public.nhsp_shifts ns
      where ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]));

      create temporary table tmp_selected_rep_keys_text on commit drop as
      select distinct rk.replacement_day_key
      from tmp_selected_replacement_keys rk;

      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'shift_id', cm.shift_id::text,
            'reason',
              case
                when exists (
                  select 1
                  from tmp_selected_rep_keys_text sr
                  where sr.replacement_day_key = (cm.candidate_id::text || '|' || cm.client_id::text || '|' || cm.work_date::text)
                ) then 'REPLACEMENT_DAY'
                else 'MISSING_FROM_IMPORT'
              end
          )
        ),
        '[]'::jsonb
      )
      into v_cancel_actions
      from tmp_cancel_meta cm;

      select public.weekly_import_apply_cancellations(
        p_import_id := p_import_id,
        p_actions := v_cancel_actions,
        p_actor_user_id := p_actor_user_id
      )
      into v_cancellations_result;

      v_mode_b_cancellations_called := true;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','CANCELLATIONS_DONE'));

    -- ─────────────────────────────────────────────
    -- 8.5) ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER MODE_B SHIFTS
    --
    -- Policy X guardrail:
    --   * This block runs only for HEALTHROSTER MODE_B / import-authoritative groups.
    --   * It creates/reuses the canonical weekly timesheet container and links active
    --     imported shifts to that container.
    --   * It does not use imported shift identifiers as Banking Pay economic keys.
    --   * It refuses to silently relink protected detached rows.
    -- ─────────────────────────────────────────────
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','HR_MODE_B_ENSURE_BASE_WEEKLY_START'));

    drop table if exists pg_temp.tmp_hr_mode_b_groups;
    create temporary table tmp_hr_mode_b_groups(
      contract_id uuid not null,
      candidate_id uuid not null,
      client_id uuid not null,
      week_ending_date date not null,
      primary key (contract_id, candidate_id, client_id, week_ending_date)
    ) on commit drop;

    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      p2ok.contract_id,
      p2ok.candidate_id,
      p2ok.client_id,
      p2ok.week_ending_date
    from tmp_p2_ok_mode p2ok
    where p2ok.mode = 'MODE_B'
      and p2ok.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
      and p2ok.contract_id is not null
      and p2ok.candidate_id is not null
      and p2ok.client_id is not null
      and p2ok.week_ending_date is not null
    on conflict do nothing;

    if array_length(v_selected_cancel_shift_ids, 1) is not null then
      insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
      select distinct
        ns.contract_id,
        ns.candidate_id,
        ns.client_id,
        ns.week_ending_date
      from public.nhsp_shifts ns
      join public.contracts c_cancel on c_cancel.id = ns.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',c_cancel.id,c_cancel.client_id,ns.week_ending_date) a_cancel
      where ns.id = any(v_selected_cancel_shift_ids)
        and ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.contract_id is not null
        and ns.candidate_id is not null
        and ns.client_id is not null
        and ns.week_ending_date is not null
        and a_cancel.import_authoritative
      on conflict do nothing;
    end if;

    -- Include all active HealthRoster shifts in the affected MODE_B groups.  This
    -- makes the transaction repair the whole canonical week bucket, not just the
    -- single selected row, while staying inside the MODE_B classification.
    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      ns.contract_id,
      ns.candidate_id,
      ns.client_id,
      ns.week_ending_date
    from public.nhsp_shifts ns
    join tmp_group_mode gm2
      on gm2.contract_id = ns.contract_id
     and gm2.candidate_id = ns.candidate_id
     and gm2.client_id = ns.client_id
     and gm2.week_ending_date = ns.week_ending_date
     and gm2.mode = 'MODE_B'
    where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.contract_id is not null
      and ns.candidate_id is not null
      and ns.client_id is not null
      and ns.week_ending_date is not null
      and (
        ns.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
        or ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]))
        or exists (
          select 1
          from tmp_hr_mode_b_groups g0
          where g0.contract_id = ns.contract_id
            and g0.candidate_id = ns.candidate_id
            and g0.client_id = ns.client_id
            and g0.week_ending_date = ns.week_ending_date
        )
      )
    on conflict do nothing;

    select count(*)::int
    into v_ensure_pairs_count
    from tmp_hr_mode_b_groups g;

    select coalesce(jsonb_agg(jsonb_build_object(
      'contract_id', g.contract_id::text,
      'candidate_id', g.candidate_id::text,
      'client_id', g.client_id::text,
      'week_ending_date', g.week_ending_date::text
    )), '[]'::jsonb)
    into v_ensure_sample_pairs
    from (
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
      limit 20
    ) as g;

    drop table if exists pg_temp.tmp_hr_mode_b_created_ts_ids;
    create temporary table tmp_hr_mode_b_created_ts_ids(
      timesheet_id uuid primary key
    ) on commit drop;

    drop table if exists pg_temp.tmp_hr_mode_b_protected_shift_ids;
    create temporary table tmp_hr_mode_b_protected_shift_ids(
      shift_id uuid primary key,
      reason text not null
    ) on commit drop;

    for v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date in
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
    loop
      select count(*)::int
      into v_active_count
      from public.nhsp_shifts ns_active
      where ns_active.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_active.cancelled_at_utc is null
        and ns_active.contract_id = v_pair_contract_id
        and ns_active.candidate_id = v_pair_candidate_id
        and ns_active.client_id = v_pair_client_id
        and ns_active.week_ending_date = v_pair_week_ending_date;

      if coalesce(v_active_count, 0) <= 0 then
        v_ensure_pairs_skipped_no_active := v_ensure_pairs_skipped_no_active + 1;
        continue;
      end if;

      v_base_week_id := null;
      v_base_week_ts_id := null;

      select cw0.id, cw0.timesheet_id
      into v_base_week_id, v_base_week_ts_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_pair_contract_id
        and cw0.week_ending_date = v_pair_week_ending_date
        and cw0.is_adjustment is false
        and coalesce(cw0.additional_seq, 0) = 0
      limit 1
      for update;

      if v_base_week_id is null then
        insert into public.contract_weeks(
          contract_id,
          week_ending_date,
          additional_seq,
          status,
          submission_mode_snapshot,
          timesheet_id,
          planned_schedule_json,
          created_at,
          updated_at,
          is_adjustment
        )
        values (
          v_pair_contract_id,
          v_pair_week_ending_date,
          0,
          'SUBMITTED'::public.contract_week_status_enum,
          'MANUAL'::public.submission_mode_enum,
          null,
          null,
          v_now,
          v_now,
          false
        )
        returning id into v_base_week_id;

        v_ensure_base_week_created_count := v_ensure_base_week_created_count + 1;
        v_base_week_ts_id := null;
      else
        v_ensure_base_week_existing_count := v_ensure_base_week_existing_count + 1;
      end if;

      if v_base_week_ts_id is not null then
        select exists(
          select 1
          from public.timesheets tchk
          where tchk.timesheet_id = v_base_week_ts_id
            and tchk.is_current is true
            and tchk.revoked_at is null
          limit 1
        )
        into v_ts_exists;

        if v_ts_exists is not true then
          update public.contract_weeks cw0u
          set
            timesheet_id = null,
            updated_at = v_now
          where cw0u.id = v_base_week_id;

          v_ensure_timesheet_missing_reference_count := v_ensure_timesheet_missing_reference_count + 1;
          v_base_week_ts_id := null;
        end if;
      end if;

      select ct.display_site, ct.ward_hint, ct.role
      into v_contract_display_site, v_contract_ward_hint, v_contract_role
      from public.contracts ct
      where ct.id = v_pair_contract_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_pair_candidate_id
      limit 1;

      select cli.name
      into v_client_name
      from public.clients cli
      where cli.id = v_pair_client_id
      limit 1;

      v_occupant_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_pair_candidate_id::text));
      v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_pair_client_id::text));
      v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
      v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

      v_shift_label_norm := 'weekly-0';

      v_booking_base :=
        v_occupant_norm || '|' ||
        v_pair_week_ending_date::text || '|' ||
        v_hospital_norm || '|' ||
        v_ward_norm || '|' ||
        v_role_norm || '|' ||
        v_shift_label_norm;

      v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
      v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

      if v_base_week_ts_id is null then
        v_new_ts_id := null;

        insert into public.timesheets(
          booking_id,
          version,
          is_current,
          status,

          sheet_scope,
          submission_mode,
          line_type,
          authorised_at_server,

          occupant_key_norm,
          hospital_norm,
          ward_norm,
          job_title_norm,
          shift_label_norm,

          week_ending_date,
          contract_id,

          manual_pdf_r2_key,
          actual_schedule_json,

          qr_payload_json,
          candidate_hint_text,

          is_adjustment,
          parent_timesheet_id,
          correction_id,
          correction_kind,
          adjustment_origin,

          created_at,
          updated_at
        )
        values (
          v_booking_id,
          1,
          true,
          'RECEIVED'::public.timesheet_status_enum,

          'WEEKLY'::public.timesheet_scope_enum,
          'MANUAL'::public.submission_mode_enum,
          'HOURS'::public.timesheet_line_type_enum,
          null,

          v_occupant_norm,
          v_hospital_norm,
          v_ward_norm,
          v_role_norm,
          v_shift_label_norm,

          v_pair_week_ending_date,
          v_pair_contract_id,

          null,
          '[]'::jsonb,

          '{}'::jsonb,
          null,

          false,
          null,
          null,
          null,
          null,

          v_now,
          v_now
        )
        returning timesheet_id into v_new_ts_id;

        v_ensure_timesheet_created_count := v_ensure_timesheet_created_count + 1;
        v_base_week_ts_id := v_new_ts_id;

        insert into tmp_hr_mode_b_created_ts_ids(timesheet_id)
        values (v_new_ts_id)
        on conflict do nothing;

        update public.contract_weeks cw0link
        set
          timesheet_id = v_new_ts_id,
          status = case
            when cw0link.status = 'AUTHORISED'::public.contract_week_status_enum then cw0link.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0link.id = v_base_week_id;

        perform public._audit_insert(
          'timesheets',
          v_new_ts_id::text,
          'HEALTHROSTER_IMPORT_TIMESHEET_CREATED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'source_system', 'HEALTHROSTER',
            'mode', 'MODE_B',
            'kind', 'BASE_WEEKLY',
            'contract_id', v_pair_contract_id::text,
            'contract_week_id', v_base_week_id::text,
            'candidate_id', v_pair_candidate_id::text,
            'client_id', v_pair_client_id::text,
            'week_ending_date', v_pair_week_ending_date::text,
            'booking_id', v_booking_id,
            'active_shifts_count', v_active_count
          ),
          'IMPORT_BIRTH',
          p_actor_user_id
        );
      else
        v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

        update public.contract_weeks cw0keep
        set
          status = case
            when cw0keep.status = 'AUTHORISED'::public.contract_week_status_enum then cw0keep.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0keep.id = v_base_week_id;

        if exists (
          select 1
          from public.timesheets identity_target
          where identity_target.timesheet_id=v_base_week_ts_id
            and (
              identity_target.week_ending_date is distinct from v_pair_week_ending_date
              or identity_target.contract_id is distinct from v_pair_contract_id
              or identity_target.occupant_key_norm is distinct from v_occupant_norm
              or identity_target.hospital_norm is distinct from v_hospital_norm
              or identity_target.ward_norm is distinct from v_ward_norm
              or identity_target.job_title_norm is distinct from v_role_norm
            )
        ) then
          select public.import_timesheet_financial_preflight_v1(
            p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
            p_action := 'IMPORT_FINANCIAL_IDENTITY_CHANGE',
            p_actor_user_id := p_actor_user_id,
            p_expected_state_json := '{}'::jsonb,
            p_lock_rows := true,
            p_max_scope := 100
          ) into v_changed_preflight;

          if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
            raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
          end if;

          if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
            raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
              )::text;
          elsif v_changed_preflight->>'required_path'='UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE' then
            raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
                'paid_uninvoiced_rollover_required',false
              )::text;
          elsif v_changed_preflight->>'required_path'='PAID_UNINVOICED_ROLLOVER'
            and not exists (
              select 1 from public.timesheets_financials rollover_identity
              where rollover_identity.timesheet_id=v_base_week_ts_id
                and rollover_identity.is_current=true
                and rollover_identity.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
                and coalesce((rollover_identity.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
            ) then
            raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array(
                  'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
                ),
                'invoice_policy_without_history','NOW'
              )::text;
          end if;
        end if;

        update public.timesheets tnorm
        set
          is_current = true,
          status = 'RECEIVED'::public.timesheet_status_enum,
          sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
          submission_mode = 'MANUAL'::public.submission_mode_enum,
          line_type = 'HOURS'::public.timesheet_line_type_enum,
          week_ending_date = v_pair_week_ending_date,
          contract_id = v_pair_contract_id,
          occupant_key_norm = v_occupant_norm,
          hospital_norm = v_hospital_norm,
          ward_norm = v_ward_norm,
          job_title_norm = v_role_norm,
          shift_label_norm = v_shift_label_norm,
          updated_at = v_now
        where tnorm.timesheet_id = v_base_week_ts_id;
      end if;

      truncate table tmp_hr_mode_b_protected_shift_ids;

      insert into tmp_hr_mode_b_protected_shift_ids(shift_id, reason)
      select distinct
        ns_lock.id,
        case
          when ns_lock.invoice_id is not null then 'SHIFT_INVOICED'
          when tf_lock.timesheet_id is not null then 'TIMESHEET_FINANCIALS_LOCKED_OR_PAID'
          when pbi_lock.timesheet_id is not null then 'PAY_BATCH_ITEM_EXISTS'
          when t_lock.timesheet_id is not null then 'CORRECTION_OR_ADJUSTMENT_OWNED_TIMESHEET'
          else 'PROTECTED_DETACHED_OR_INVALID_LINK'
        end as reason
      from public.nhsp_shifts ns_lock
      left join public.timesheets_financials tf_lock
        on tf_lock.timesheet_id = ns_lock.timesheet_id
       and tf_lock.is_current is true
       and (
         tf_lock.locked_by_invoice_id is not null
         or tf_lock.paid_at_utc is not null
       )
      left join public.pay_batch_items pbi_lock
        on pbi_lock.timesheet_id = ns_lock.timesheet_id
       and coalesce(pbi_lock.is_voided, false) is false
      left join public.timesheets t_lock
        on t_lock.timesheet_id = ns_lock.timesheet_id
       and (
         coalesce(t_lock.is_adjustment, false) is true
         or t_lock.parent_timesheet_id is not null
         or t_lock.correction_id is not null
         or t_lock.correction_kind is not null
         or t_lock.adjustment_origin is not null
       )
      where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_lock.cancelled_at_utc is null
        and ns_lock.contract_id = v_pair_contract_id
        and ns_lock.candidate_id = v_pair_candidate_id
        and ns_lock.client_id = v_pair_client_id
        and ns_lock.week_ending_date = v_pair_week_ending_date
        and (
          ns_lock.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tvalid
            where tvalid.timesheet_id = ns_lock.timesheet_id
              and tvalid.is_current is true
              and tvalid.revoked_at is null
            limit 1
          )
        )
        and (
          ns_lock.invoice_id is not null
          or tf_lock.timesheet_id is not null
          or pbi_lock.timesheet_id is not null
          or t_lock.timesheet_id is not null
        )
      on conflict do nothing;

      select count(*)::int
      into v_active_count
      from tmp_hr_mode_b_protected_shift_ids p;

      if coalesce(v_active_count, 0) > 0 then
        raise exception
          'hr_weekly_apply_transactional: protected active HEALTHROSTER MODE_B shifts are detached or linked to an invalid timesheet; refusing silent relink. contract_id=% candidate_id=% client_id=% week_ending_date=% protected_count=% sample=%',
          v_pair_contract_id,
          v_pair_candidate_id,
          v_pair_client_id,
          v_pair_week_ending_date,
          v_active_count,
          (
            select coalesce(jsonb_agg(jsonb_build_object('shift_id', p2.shift_id::text, 'reason', p2.reason)), '[]'::jsonb)
            from (
              select p.shift_id, p.reason
              from tmp_hr_mode_b_protected_shift_ids p
              order by p.shift_id::text
              limit 10
            ) p2
          );
      end if;

      if exists (
        select 1 from public.nhsp_shifts ns_scope
        where ns_scope.source_system = 'HEALTHROSTER'::public.hr_source_enum
            and ns_scope.cancelled_at_utc is null
            and ns_scope.contract_id = v_pair_contract_id
            and ns_scope.candidate_id = v_pair_candidate_id
            and ns_scope.client_id = v_pair_client_id
            and ns_scope.week_ending_date = v_pair_week_ending_date
            and (
              ns_scope.timesheet_id is null
              or not exists (
                select 1 from public.timesheets existing_link
                where existing_link.timesheet_id=ns_scope.timesheet_id
                  and existing_link.is_current=true
                  and existing_link.revoked_at is null
              )
            )
            and not exists (
              select 1 from tmp_hr_mode_b_protected_shift_ids protected
              where protected.shift_id=ns_scope.id
            )
      ) then
        select public.import_timesheet_financial_preflight_v1(
          p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
          p_action := 'IMPORT_SOURCE_ASSIGNMENT',
          p_actor_user_id := p_actor_user_id,
          p_expected_state_json := '{}'::jsonb,
          p_lock_rows := true,
          p_max_scope := 100
        ) into v_changed_preflight;

        if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
          raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
        end if;

        if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
          raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets source_target
          left join public.timesheets_financials source_target_tf
            on source_target_tf.timesheet_id=source_target.timesheet_id and source_target_tf.is_current=true
          where source_target.timesheet_id=v_base_week_ts_id
            and (source_target.authorised_at_server is not null or source_target_tf.authorised_at_utc is not null)
            and not exists (
              select 1 from public.timesheets_financials paid_target
              where paid_target.timesheet_id=source_target.timesheet_id
                and paid_target.paid_at_utc is not null
            )
        ) then
          raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
              'paid_uninvoiced_rollover_required',false
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets_financials paid_source
          where paid_source.timesheet_id=v_base_week_ts_id and paid_source.paid_at_utc is not null
        ) and not exists (
          select 1 from public.timesheets_financials rollover_source
          where rollover_source.timesheet_id=v_base_week_ts_id and rollover_source.is_current=true
            and rollover_source.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
            and coalesce((rollover_source.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
        ) then
          raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array(
                'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
              ),
              'invoice_policy_without_history','NOW'
            )::text;
        end if;
      end if;

      update public.nhsp_shifts nsu0
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu0.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu0.cancelled_at_utc is null
        and nsu0.contract_id = v_pair_contract_id
        and nsu0.candidate_id = v_pair_candidate_id
        and nsu0.client_id = v_pair_client_id
        and nsu0.week_ending_date = v_pair_week_ending_date
        and nsu0.timesheet_id is null
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p
          where p.shift_id = nsu0.id
        );

      get diagnostics v_attached_null_count = row_count;
      v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

      update public.nhsp_shifts nsu1
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu1.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu1.cancelled_at_utc is null
        and nsu1.contract_id = v_pair_contract_id
        and nsu1.candidate_id = v_pair_candidate_id
        and nsu1.client_id = v_pair_client_id
        and nsu1.week_ending_date = v_pair_week_ending_date
        and nsu1.timesheet_id is not null
        and not exists (
          select 1
          from public.timesheets tvalid2
          where tvalid2.timesheet_id = nsu1.timesheet_id
            and tvalid2.is_current is true
            and tvalid2.revoked_at is null
          limit 1
        )
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p2
          where p2.shift_id = nsu1.id
        );

      get diagnostics v_relinked_invalid_count = row_count;
      v_ensure_shifts_relinked_invalid_ts_count := v_ensure_shifts_relinked_invalid_ts_count + coalesce(v_relinked_invalid_count, 0);

      select count(*)::int
      into v_active_count
      from public.nhsp_shifts nscheck
      where nscheck.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nscheck.cancelled_at_utc is null
        and nscheck.contract_id = v_pair_contract_id
        and nscheck.candidate_id = v_pair_candidate_id
        and nscheck.client_id = v_pair_client_id
        and nscheck.week_ending_date = v_pair_week_ending_date
        and (
          nscheck.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tchk2
            where tchk2.timesheet_id = nscheck.timesheet_id
              and tchk2.is_current is true
              and tchk2.revoked_at is null
            limit 1
          )
        );

      if coalesce(v_active_count, 0) > 0 then
        v_ensure_remaining_active_detached_count := v_ensure_remaining_active_detached_count + v_active_count;
        raise exception
          'hr_weekly_apply_transactional: ENSURE invariant failed (active HEALTHROSTER MODE_B shifts remain detached or linked to missing/non-current/revoked timesheets) contract_id=% candidate_id=% client_id=% week_ending_date=% remaining=%.',
          v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date, v_active_count;
      end if;

      insert into tmp_aff_ts(timesheet_id)
      values (v_base_week_ts_id)
      on conflict do nothing;
    end loop;

    select coalesce(jsonb_agg(x.ts_id), '[]'::jsonb)
    into v_ensure_sample_created_ts_ids
    from (
      select tct.timesheet_id::text as ts_id
      from tmp_hr_mode_b_created_ts_ids tct
      order by tct.timesheet_id::text
      limit 20
    ) as x;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','HR_MODE_B_ENSURE_BASE_WEEKLY_DONE',
      'ensure_pairs_count', v_ensure_pairs_count,
      'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
      'base_week_created_count', v_ensure_base_week_created_count,
      'base_week_existing_count', v_ensure_base_week_existing_count,
      'base_timesheet_created_count', v_ensure_timesheet_created_count,
      'base_timesheet_reused_count', v_ensure_timesheet_reused_count,
      'missing_timesheet_reference_count', v_ensure_timesheet_missing_reference_count,
      'shifts_attached_null_count', v_ensure_shifts_attached_count,
      'shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
      'remaining_active_detached_count', v_ensure_remaining_active_detached_count,
      'sample_pairs', v_ensure_sample_pairs,
      'sample_created_ts_ids', v_ensure_sample_created_ts_ids
    ));
  end if;

  -- ─────────────────────────────────────────────
  -- 9) MODE_A mirror ingestion
  -- ─────────────────────────────────────────────
  if array_length(v_mode_a_external_keys, 1) is not null then
    select public.hr_weekly_mirror_upsert_deterministic(
      p_import_id := p_import_id,
      p_external_row_keys := v_mode_a_external_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_mirror_result;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','MODE_A_MIRROR_DONE'));

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A shift→timesheet linking
  -- ─────────────────────────────────────────────
  create temporary table tmp_mode_a_ts_map(
    external_row_key text primary key,
    timesheet_id uuid not null
  ) on commit drop;

  insert into tmp_mode_a_ts_map(external_row_key, timesheet_id)
  select distinct
    p2m.external_row_key,
    cw0.timesheet_id
  from tmp_p2_ok_mode p2m
  join public.contract_weeks cw0
    on cw0.contract_id = p2m.contract_id
   and cw0.week_ending_date = p2m.week_ending_date
   and cw0.is_adjustment is false
   and coalesce(cw0.additional_seq, 0) = 0
  where p2m.mode = 'MODE_A'
    and p2m.external_row_key = any(coalesce(v_mode_a_external_keys,array[]::text[]))
    and p2m.external_row_key is not null
    and cw0.timesheet_id is not null
  on conflict do nothing;

  select count(*)::int
  into v_mode_a_ts_linked_count
  from tmp_mode_a_ts_map mt;

  create temporary table tmp_mode_a_locked_shift_ids(
    shift_id uuid primary key
  ) on commit drop;

  insert into tmp_mode_a_locked_shift_ids(shift_id)
  select distinct ns_lock.id as shift_id
  from public.nhsp_shifts ns_lock
  join tmp_mode_a_ts_map mt_lock
    on mt_lock.external_row_key = ns_lock.external_row_key
  where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
    and ns_lock.cancelled_at_utc is null
    and ns_lock.latest_import_id = p_import_id
    and (
      ns_lock.invoice_id is not null
      or exists (
        select 1
        from public.timesheets_financials tf_lock
        cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg_lock(value)
        where tf_lock.is_current = true
          and tf_lock.timesheet_id = ns_lock.timesheet_id
          and nullif(btrim(seg_lock.value->>'nhsp_shift_id'), '') = ns_lock.id::text
          and nullif(btrim(seg_lock.value->>'invoice_locked_invoice_id'), '') is not null
        limit 1
      )
    )
  on conflict do nothing;

  update public.nhsp_shifts nsu
     set timesheet_id = mt.timesheet_id,
         updated_at = v_now
    from tmp_mode_a_ts_map mt
   where nsu.source_system = 'HEALTHROSTER'::public.hr_source_enum
     and nsu.cancelled_at_utc is null
     and nsu.latest_import_id = p_import_id
     and nsu.external_row_key = mt.external_row_key
     and (nsu.timesheet_id is distinct from mt.timesheet_id)
     and not exists (
       select 1
       from tmp_mode_a_locked_shift_ids l
       where l.shift_id = nsu.id
     );

  get diagnostics v_mode_a_shifts_attached_count = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mt2.timesheet_id
  from tmp_mode_a_ts_map mt2
  where mt2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_SHIFTS_LINKED',
      'mode_a_ts_linked_count', v_mode_a_ts_linked_count,
      'mode_a_shifts_attached_count', v_mode_a_shifts_attached_count,
      'mode_a_locked_shift_count', (select count(*)::int from tmp_mode_a_locked_shift_ids)
    )
  );

  -- ─────────────────────────────────────────────
  -- 10) MODE_A weekly validation upserts + email state
  -- ─────────────────────────────────────────────
  select public.hr_weekly_validation_preview(p_import_id := p_import_id)
  into v_weekly_val_payload;

  if v_weekly_val_payload is null or jsonb_typeof(v_weekly_val_payload) <> 'object' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview returned non-object payload.';
  end if;

  if jsonb_typeof(v_weekly_val_payload->'rows') <> 'array' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview payload missing rows array.';
  end if;

  create temporary table tmp_val_rows on commit drop as
  select
    nullif(btrim(r.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(r.value->>'candidate_id'), '')::uuid as candidate_id,
    nullif(btrim(r.value->>'contract_id'), '')::uuid as contract_id,
    nullif(btrim(r.value->>'week_ending_date'), '')::date as week_ending_date,
    nullif(btrim(r.value->>'client_id'), '')::uuid as client_id,
    upper(coalesce(r.value->>'overall_status','')) as overall_status,
    (lower(coalesce(r.value->>'has_mismatch','false')) in ('true','1')) as has_mismatch,
    r.value as row_json
  from jsonb_array_elements(v_weekly_val_payload->'rows') as r(value)
  where nullif(btrim(r.value->>'timesheet_id'), '') is not null
    and nullif(btrim(r.value->>'candidate_id'), '') is not null
    and nullif(btrim(r.value->>'contract_id'), '') is not null
    and nullif(btrim(r.value->>'week_ending_date'), '') is not null
    and nullif(btrim(r.value->>'client_id'), '') is not null
    and exists (
      select 1 from tmp_review_batch_units bu
      where bu.candidate_id=nullif(btrim(r.value->>'candidate_id'), '')::uuid
        and bu.client_id=nullif(btrim(r.value->>'client_id'), '')::uuid
    );

  select count(*)::int
  into v_val_rows_count
  from tmp_val_rows;

  create temporary table tmp_val_mode on commit drop as
  select
    vr.timesheet_id,
    case aval.authority_mode when 'VALIDATION_ONLY' then 'MODE_A'
      when 'AUTHORITATIVE' then 'MODE_B' else 'OUT_OF_SCOPE' end as mode
  from tmp_val_rows vr
  join public.contracts cval
    on cval.id = vr.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',cval.id,cval.client_id,vr.week_ending_date) aval;

  if exists(select 1 from tmp_val_mode where mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_VALIDATION_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_invalidation_actions(
    timesheet_id uuid not null,
    comparison_key text not null,
    invalidate boolean not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  if v_invalidation_actions_count > 0 then
    insert into tmp_invalidation_actions(timesheet_id, comparison_key, invalidate)
    select
      nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
      nullif(btrim(a.value->>'comparison_key'), '') as comparison_key,
      (lower(coalesce(a.value->>'invalidate','true')) in ('true','1')) as invalidate
    from jsonb_array_elements(v_invalidation_actions) as a(value)
    where nullif(btrim(a.value->>'timesheet_id'), '') is not null
      and nullif(btrim(a.value->>'comparison_key'), '') is not null
    on conflict (timesheet_id, comparison_key) do update
      set invalidate = excluded.invalidate;
  end if;

  create temporary table tmp_mode_a_missing_ref_clear(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_before text null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_missing_ref_clear(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_before)
  select distinct
    vr.timesheet_id,
    nullif(btrim(coalesce(cx.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(cx.value->>'ref_before'), '') as ref_before
  from tmp_val_rows vr
  join tmp_val_mode vmc
    on vmc.timesheet_id = vr.timesheet_id
   and vmc.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vr.row_json->'comparisons', '[]'::jsonb)) as cx(value)
  left join tmp_invalidation_actions ia
    on ia.timesheet_id = vr.timesheet_id
   and ia.comparison_key = nullif(btrim(coalesce(cx.value->>'comparison_key','')), '')
  where upper(coalesce(cx.value->>'match_status','')) in ('UNMATCHED','MISMATCH')
    and (lower(coalesce(cx.value->>'invoice_locked','false')) in ('true','1')) is false
    and nullif(btrim(coalesce(cx.value->>'invoice_locked_invoice_id','')), '') is null
    and nullif(btrim(coalesce(cx.value->>'ref_before','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'work_date','')), '') is not null
    and ia.timesheet_id is not null
    and ia.invalidate is true
  on conflict (timesheet_id, comparison_key) do nothing;

  -- ✅ Capture exact timesheets whose refs were cleared (for post-apply QR reissue + regen)
  drop table if exists pg_temp.tmp_mode_a_ref_clear_upd;
  create temporary table tmp_mode_a_ref_clear_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsclr
       set ref_num = null,
           hr_request_id = null,
           updated_at = v_now
      from tmp_mode_a_missing_ref_clear mrc
      left join public.timesheets_financials tfc
        on tfc.timesheet_id = mrc.timesheet_id
       and tfc.is_current = true
     where nsclr.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsclr.cancelled_at_utc is null
       and nsclr.timesheet_id = mrc.timesheet_id
       and nsclr.work_date = mrc.work_date
       and nsclr.ref_num is not null
       and nsclr.invoice_id is null
       and (tfc.timesheet_id is null or (tfc.locked_by_invoice_id is null and tfc.paid_at_utc is null))
       and to_char((date_trunc('minute', nsclr.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_start_hhmm
       and to_char((date_trunc('minute', nsclr.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_end_hhmm
       and coalesce(nsclr.break_mins,0) = coalesce(mrc.ts_break_mins,0)
       and not exists (
         select 1
         from public.timesheets_financials tf_lock
         cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(value)
         where tf_lock.is_current = true
           and tf_lock.timesheet_id = nsclr.timesheet_id
           and nullif(btrim(seg.value->>'nhsp_shift_id'), '') = nsclr.id::text
           and nullif(btrim(seg.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsclr.timesheet_id
  )
  insert into tmp_mode_a_ref_clear_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_cleared_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_clear_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_clear_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mrc2.timesheet_id
  from tmp_mode_a_missing_ref_clear mrc2
  where mrc2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MISSING_SHIFT_REF_CLEARED',
      'ref_cleared_count', v_mode_a_ref_cleared_count,
      'invalidation_actions_count', v_invalidation_actions_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A matched ref propagation (Request Id / booking reference)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_mode_a_ref_set;
  create temporary table tmp_mode_a_ref_set(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_after text not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_ref_set(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_after)
  select distinct
    vrm.timesheet_id,
    nullif(btrim(coalesce(cx2.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx2.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx2.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx2.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx2.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') as ref_after
  from tmp_val_rows vrm
  join tmp_val_mode vmm
    on vmm.timesheet_id = vrm.timesheet_id
   and vmm.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vrm.row_json->'comparisons', '[]'::jsonb)) as cx2(value)
  where vrm.timesheet_id is not null
    and (
      upper(coalesce(cx2.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
      or (lower(coalesce(cx2.value->>'match','false')) in ('true','1'))
    )
    and nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'work_date','')), '') is not null
  on conflict (timesheet_id, comparison_key) do nothing;

  drop table if exists pg_temp.tmp_mode_a_ref_set_upd;
  create temporary table tmp_mode_a_ref_set_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsset
       set ref_num = mrs.ref_after,
           hr_request_id = mrs.ref_after,
           updated_at = v_now
      from tmp_mode_a_ref_set mrs
      left join public.timesheets_financials tfm
        on tfm.timesheet_id = mrs.timesheet_id
       and tfm.is_current = true
     where nsset.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsset.cancelled_at_utc is null
       and nsset.timesheet_id = mrs.timesheet_id
       and nsset.work_date = mrs.work_date
       and nsset.invoice_id is null
       and (tfm.timesheet_id is null or (tfm.locked_by_invoice_id is null and tfm.paid_at_utc is null))
       and to_char((date_trunc('minute', nsset.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_start_hhmm
       and to_char((date_trunc('minute', nsset.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_end_hhmm
       and coalesce(nsset.break_mins,0) = coalesce(mrs.ts_break_mins,0)
       and (
         nsset.ref_num is distinct from mrs.ref_after
         or nsset.hr_request_id is distinct from mrs.ref_after
       )
       and not exists (
         select 1
         from public.timesheets_financials tf_lock2
         cross join lateral jsonb_array_elements(coalesce(tf_lock2.invoice_breakdown_json->'segments','[]'::jsonb)) as seg2(value)
         where tf_lock2.is_current = true
           and tf_lock2.timesheet_id = nsset.timesheet_id
           and nullif(btrim(seg2.value->>'nhsp_shift_id'), '') = nsset.id::text
           and nullif(btrim(seg2.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsset.timesheet_id
  )
  insert into tmp_mode_a_ref_set_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_set_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_set_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_set_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MATCHED_REF_SET',
      'ref_set_count', v_mode_a_ref_set_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ UPDATED: tmp_val_upsert now computes new_pre_validated
  --   new_pre_validated = true when:
  --     - validation result is OK/OVERRIDDEN (=> new_status VALIDATION_OK)
  --     - AND timesheet is NOT authorised yet (timesheets.authorised_at_server IS NULL)
  -- ─────────────────────────────────────────────
  create temporary table tmp_val_upsert on commit drop as
  select
    vr.timesheet_id,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then 'VALIDATION_OK'::public.validation_status_enum
      else 'VALIDATION_ERROR'::public.validation_status_enum
    end as new_status,
    'HEALTHROSTER_WEEKLY'::text as new_reason_code,
    case when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then v_now else null end as new_validated_at_utc,
    p_import_id as new_last_source,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE')
       and tva.timesheet_id is not null
       and tva.authorised_at_server is null
      then true
      else false
    end as new_pre_validated
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  left join public.timesheets tva
    on tva.timesheet_id = vr.timesheet_id
   and tva.is_current = true
  where vm.mode = 'MODE_A'
    and vr.timesheet_id is not null;

  -- ✅ UPDATED: include pre_validated changes as "validation_changed"
  select coalesce(array_agg(distinct x.timesheet_id order by x.timesheet_id), array[]::uuid[])
  into v_validation_changed_timesheet_ids
  from (
    select u.timesheet_id
    from tmp_val_upsert u
    left join public.timesheet_validations tv
      on tv.timesheet_id = u.timesheet_id
    where tv.timesheet_id is null
       or tv.status is distinct from u.new_status
       or tv.validated_at_utc is distinct from u.new_validated_at_utc
       or tv.last_source is distinct from u.new_last_source
       or tv.reason_code is distinct from u.new_reason_code
       or tv.pre_validated is distinct from u.new_pre_validated
  ) as x;

  -- ✅ UPDATED: insert/upsert includes pre_validated
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    pre_validated,
    updated_at
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    u.new_pre_validated,
    v_now
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status = excluded.status,
        reason_code = excluded.reason_code,
        validated_at_utc = excluded.validated_at_utc,
        last_source = excluded.last_source,
        pre_validated = excluded.pre_validated,
        updated_at = excluded.updated_at;

  get diagnostics v_validations_upserted = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct t.tsid
  from unnest(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])) as t(tsid)
  where t.tsid is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct vr.timesheet_id), array[]::uuid[])
  into v_mismatched_tsids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and vr.has_mismatch is true
    and vr.timesheet_id is not null;

  -- A validation-only Weekly timesheet is eligible for configured
  -- auto-authorisation only when the immutable coverage says omissions are
  -- meaningful and every segment on the whole timesheet has one exact
  -- HealthRoster match whose reference has been durably written.  Processing
  -- one selected row, one day or one matching segment can never authorise the
  -- rest of the timesheet by implication.
  select coalesce(array_agg(distinct vr.timesheet_id order by vr.timesheet_id),array[]::uuid[])
  into v_validation_auto_authorise_timesheet_ids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id=vr.timesheet_id
   and vm.mode='MODE_A'
  join tmp_val_upsert vu
    on vu.timesheet_id=vr.timesheet_id
  join public.timesheets t
    on t.timesheet_id=vr.timesheet_id
   and t.is_current=true
   and t.revoked_at is null
  join public.hr_imports hi
    on hi.id=p_import_id
  left join public.timesheets_financials tf
    on tf.timesheet_id=t.timesheet_id
   and tf.is_current=true
  cross join lateral (
    select case
      when jsonb_typeof(t.actual_schedule_json)='array'
       and jsonb_array_length(t.actual_schedule_json)>0
        then jsonb_array_length(t.actual_schedule_json)
      when jsonb_typeof(tf.invoice_breakdown_json)='object'
       and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
        then jsonb_array_length(tf.invoice_breakdown_json->'segments')
      else 0
    end as segment_count
  ) whole_timesheet
  where vu.new_status='VALIDATION_OK'::public.validation_status_enum
    and vu.new_pre_validated=true
    and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
    and (
      hi.coverage_mode='COMPLETE_ALL'
      or exists (
        select 1
        from public.import_review_scope_candidates scoped_candidate
        where scoped_candidate.import_id=p_import_id
          and scoped_candidate.candidate_id=vr.candidate_id
      )
    )
    and jsonb_typeof(vr.row_json->'comparisons')='array'
    and jsonb_array_length(vr.row_json->'comparisons')>0
    and whole_timesheet.segment_count=jsonb_array_length(vr.row_json->'comparisons')
    and not exists (
      select 1
      from jsonb_array_elements(vr.row_json->'comparisons') comparison(value)
      where not (
        (
          upper(coalesce(comparison.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
          or lower(coalesce(comparison.value->>'match','false')) in ('true','1')
        )
        and lower(coalesce(comparison.value->>'time_match','false')) in ('true','1')
        and nullif(btrim(comparison.value->>'ref_after'),'') is not null
        and nullif(btrim(comparison.value->>'work_date'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_start'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_end'),'') is not null
        and exists (
          select 1
          from public.nhsp_shifts matched_shift
          where matched_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and matched_shift.cancelled_at_utc is null
            and matched_shift.timesheet_id=vr.timesheet_id
            and matched_shift.work_date=(comparison.value->>'work_date')::date
            and to_char((date_trunc('minute',matched_shift.start_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_start'
            and to_char((date_trunc('minute',matched_shift.end_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_end'
            and coalesce(matched_shift.break_mins,0)=coalesce(nullif(btrim(comparison.value->>'timesheet_break_mins'),'')::integer,0)
            and matched_shift.ref_num=comparison.value->>'ref_after'
            and matched_shift.hr_request_id=comparison.value->>'ref_after'
        )
      )
    );

  -- Query emails are intentionally outside the source transaction. The
  -- database returns selected action IDs; the Worker later calls the
  -- idempotent outbox-backed enqueue RPC after source commit.

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_VALIDATIONS_DONE',
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ Compute ref_updated_timesheet_ids (for post-apply QR reissue + tspdf regen decisions)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(distinct rts.timesheet_id order by rts.timesheet_id), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from tmp_ref_updated_ts rts
  where rts.timesheet_id is not null;

  v_ref_updated_timesheet_ids_count := coalesce(array_length(v_ref_updated_timesheet_ids, 1), 0);

  -- ─────────────────────────────────────────────
  -- 11) Compute affected_timesheet_ids (MODE_B work + MODE_A validation changes)
  -- ─────────────────────────────────────────────
  -- Build the ordinary authoritative MODE_B scope separately from the
  -- whole-timesheet MODE_A validation scope calculated above.
  -- active imported rows, protected amendment correction pairs and
  -- cancellation/reversal results.  MODE_A validation/reference work is
  -- deliberately excluded even though it remains part of the TSFIN refresh.
  select coalesce(array_agg(distinct target.timesheet_id order by target.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select ns.timesheet_id
    from public.nhsp_shifts ns
    where ns.source_system='HEALTHROSTER'::public.hr_source_enum
      and ns.client_id=v_import_client_id
      and ns.cancelled_at_utc is null
      and ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
      -- Protected changed-hours rows are represented financially by their
      -- immutable reversal/replacement pair.  The source shift remains linked
      -- to the root for import identity, but refreshing that settled root as
      -- well would count the same delta twice in the correction-chain
      -- residual (live root truth plus the signed pair).
      and not (
        ns.external_row_key=any(
          coalesce(v_invoiced_changed_keys,array[]::text[])
        )
      )
      and ns.timesheet_id is not null
    union all
    select phase3_created.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids','[]'::jsonb)) phase3_created(value)
    union all
    select phase3_updated.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids','[]'::jsonb)) phase3_updated(value)
    union all
    select cancelled.value::uuid
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids','[]'::jsonb)) cancelled(value)
  ) target
  where target.timesheet_id is not null;

  -- A protected correction must remain a complete TSFIN/lifecycle unit even
  -- when this batch changes only its mutable replacement member.
  select coalesce(array_agg(distinct expanded.timesheet_id order by expanded.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select requested.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    where requested.timesheet_id is not null
    union
    select partner.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    join public.timesheets seed
      on seed.timesheet_id=requested.timesheet_id
     and seed.is_current=true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id=seed.correction_id
     and partner.is_current=true
     and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
  ) expanded
  where expanded.timesheet_id is not null;
  insert into tmp_aff_ts(timesheet_id)
  select authoritative.timesheet_id
  from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) authoritative(timesheet_id)
  where authoritative.timesheet_id is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null
    -- A protected source is immutable financial history. It can enter the
    -- generic affected set through MODE_A validation/reference bookkeeping
    -- even though the authoritative MODE_B scope above correctly selected
    -- only the new correction pair. Never let that bookkeeping requeue the
    -- settled root for TSFIN recalculation.
    and not (
      a.timesheet_id=any(
        coalesce(v_protected_source_timesheet_ids,array[]::uuid[])
      )
    );

  -- Restore every previously-authorised mutable source and authorise every
  -- financial correction member regardless of the ordinary setting.  A
  -- changed-hours reversal/replacement pair therefore moves together, while
  -- a true cancellation contributes its reversal only.
  select coalesce(array_agg(distinct required.timesheet_id order by required.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select existing.timesheet_id
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) existing(timesheet_id)
    union all
    select correction.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) affected(timesheet_id)
    join public.timesheets correction on correction.timesheet_id=affected.timesheet_id
    where correction.is_current=true
      and correction.revoked_at is null
      and coalesce(correction.is_adjustment,false)
      and correction.correction_id is not null
  ) required
  where required.timesheet_id is not null;

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_authoritative_affected_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,false
  );

  select coalesce(array_agg(distinct eligible.timesheet_id order by eligible.timesheet_id),array[]::uuid[])
  into v_auto_authorise_timesheet_ids
  from (
    select unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])) as timesheet_id
    union all
    select unnest(public._import_review_auto_authorise_targets_core_v1(
      v_validation_auto_authorise_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,true
    )) as timesheet_id
  ) eligible
  where eligible.timesheet_id is not null;

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  -- ─────────────────────────────────────────────
  -- 12) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 after it has proved that no
  -- deferred/selectable work or blockers remain.  An incremental batch must
  -- never make the staged import look globally applied.
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set import_scope = 'HR_WEEKLY'
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED'));

  -- ─────────────────────────────────────────────
  -- 13) Logging (invoice_debug only, via _imp_debug_audit)
  -- ─────────────────────────────────────────────
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_VALIDATIONS_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids_count', v_ref_updated_timesheet_ids_count,
      'invalidation_actions_count', v_invalidation_actions_count
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'steps', v_steps
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'client_id', v_import_client_id,
    'mode_b', jsonb_build_object(
      'selected_truth_keys', to_jsonb(coalesce(v_selected_truth_keys_mode_b, array[]::text[])),
      'force_overwrite_external_row_keys', to_jsonb(coalesce(v_force_keys_final, array[]::text[])),
      'skip_external_row_keys', to_jsonb(coalesce(v_skip_keys_final, array[]::text[])),
      'phase1', v_phase1_result,
      'phase15', jsonb_build_object(
        'ok_rows', v_phase15_ok,
        'shift_updated_rows', v_phase15_updated
      ),
      'phase3', v_phase3_result,
      'cancellations', v_cancellations_result
    ),
    'mode_a', jsonb_build_object(
      'mirror', v_mirror_result,
      'validations_upserted', v_validations_upserted,
      'mismatched_timesheet_ids', to_jsonb(coalesce(v_mismatched_tsids, array[]::uuid[])),
      'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
      'whole_timesheet_auto_authorise_eligible_ids',to_jsonb(coalesce(v_validation_auto_authorise_timesheet_ids,array[]::uuid[]))
    ),
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_timesheet_ids',to_jsonb(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])),
    'post_commit_reauthorise_timesheet_ids',to_jsonb(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
    'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
    'post_commit_email_action_ids',v_post_commit_email_action_ids,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    jsonb_array_length(v_post_commit_email_action_ids)>0 or cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_WEEKLY_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'client_id', case when v_import_client_id is null then null else v_import_client_id::text end,
        'steps', v_steps,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- Restore nhsp_weekly_apply_transactional from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.nhsp_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET plpgsql_check.mode TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := '[]'::jsonb;

  -- normalized selections
  v_selected_action_ids text[] := array[]::text[];
  v_selected_truth_keys text[] := array[]::text[];
  v_selected_cancel_shift_ids uuid[] := array[]::uuid[];

  -- deterministic external keys in this import (NHSP, OK rows)
  v_all_ok_external_keys text[] := array[]::text[];

  -- selected truth keys constrained to OK rows
  v_selected_truth_keys_ok text[] := array[]::text[];

  -- tick-only enforced lists
  v_force_keys_final text[] := array[]::text[];
  v_skip_keys_final text[] := array[]::text[];

  -- changed-hours partition (selected keys only)
  v_invoiced_changed_keys text[] := array[]::text[];
  v_not_invoiced_changed_keys text[] := array[]::text[];

  -- phase3 / phase1 / phase1.5
  v_phase3_result jsonb := null;
  v_changed_preflight jsonb := null;
  v_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_reauthorise_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_lifecycle_items jsonb := '[]'::jsonb;
  v_unauthorise_result jsonb := null;
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- policy A replacement-day enforcement + cancellation reasoning
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- cancellations
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- affected timesheets
  v_affected_timesheet_ids uuid[] := array[]::uuid[];
  v_force_keys_non_invoiced text[] := array[]::text[];

  -- debug / audit
  v_sample_selected_action_ids jsonb := '[]'::jsonb;
  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_skip_keys jsonb := '[]'::jsonb;
  v_sample_cancel_shift_ids jsonb := '[]'::jsonb;
  v_steps jsonb := '[]'::jsonb;

  v_selected_action_ids_count int := 0;
  v_selected_row_keys_count int := 0;
  v_selected_cancel_shift_ids_count int := 0;

  v_ok_keys_total int := 0;
  v_force_keys_count int := 0;
  v_skip_keys_count int := 0;

  v_invoiced_changed_keys_count int := 0;
  v_not_invoiced_changed_keys_count int := 0;

  v_cancellations_count int := 0;

  v_phase1_shifts_created int := 0;
  v_phase1_shifts_updated int := 0;

  v_phase3_created_count int := 0;
  v_phase3_updated_count int := 0;
  v_cancel_adjustment_count int := 0;
  v_correction_timesheets_created_count int := 0;

  v_should_run_phase1 boolean := false;
  v_should_run_phase15 boolean := false;
  v_should_run_phase3 boolean := false;
  v_should_run_cancellations boolean := false;

  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;

  -- ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE SHIFTS (invariant)
  v_ensure_pairs_count int := 0;
  v_ensure_pairs_skipped_no_active int := 0;

  v_ensure_base_week_created_count int := 0;
  v_ensure_base_week_existing_count int := 0;

  v_ensure_timesheet_created_count int := 0;
  v_ensure_timesheet_reused_count int := 0;
  v_ensure_timesheet_missing_reference_count int := 0;

  v_ensure_shifts_attached_count int := 0;
  v_ensure_shifts_relinked_invalid_ts_count int := 0;
  v_ensure_remaining_active_detached_count int := 0;

  v_ensure_sample_pairs jsonb := '[]'::jsonb;
  v_ensure_sample_created_ts_ids jsonb := '[]'::jsonb;

  -- loop vars for ensure
  v_pair_contract_id uuid;
  v_pair_candidate_id uuid;
  v_pair_client_id uuid;
  v_pair_week_ending_date date;

  v_active_count int := 0;

  v_base_week_id uuid := null;
  v_base_week_ts_id uuid := null;

  v_ts_exists boolean := false;

  v_candidate_display_name text := null;
  v_candidate_tms_ref text := null;
  v_client_name text := null;
  v_contract_display_site text := null;
  v_contract_ward_hint text := null;
  v_contract_role text := null;

  v_occupant_norm text := null;
  v_hospital_norm text := null;
  v_ward_norm text := null;
  v_role_norm text := null;

  v_booking_base text := null;
  v_hash_hex text := null;
  v_booking_id text := null;
  v_shift_label_norm text := null;

  v_new_ts_id uuid := null;

  v_attached_null_count int := 0;
  v_relinked_invalid_count int := 0;

  -- shared error
  v_sqlstate text;
  v_err text;
begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));

  -- ─────────────────────────────────────────────
  -- 0) Validate import exists and is NHSP
  -- ─────────────────────────────────────────────
  select upper(coalesce(hi.source_system::text, ''))
  into v_import_source_system
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'nhsp_weekly_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'NHSP' then
    raise exception 'nhsp_weekly_apply_transactional: import % source_system=%; expected NHSP.', p_import_id, v_import_source_system;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
    or not(v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;
  select coalesce(jsonb_agg(to_jsonb(case when d.action_kind='APPLY_CANCELLATION' then 'CANCEL:'||d.shift_id::text else 'ROW:'||d.source_identity end) order by d.action_id),'[]'::jsonb)
    into v_actions_json
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION');

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK'));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  create temporary table tmp_sel_ids(
    action_id text primary key
  ) on commit drop;

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x.value), '')
  from jsonb_array_elements_text(v_actions_json) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  if exists (
    select 1
    from tmp_sel_ids tsi
    where tsi.action_id !~ '^(ROW|CANCEL):'
  ) then
    raise exception 'nhsp_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
  end if;

  select coalesce(array_agg(tsi.action_id order by tsi.action_id), array[]::text[])
  into v_selected_action_ids
  from tmp_sel_ids tsi;

  select coalesce(array_agg(distinct substring(tsi.action_id from 5) order by substring(tsi.action_id from 5)), array[]::text[])
  into v_selected_truth_keys
  from tmp_sel_ids tsi
  where tsi.action_id like 'ROW:%';

  select coalesce(array_agg(distinct (substring(tsi.action_id from 8))::uuid order by (substring(tsi.action_id from 8))::uuid), array[]::uuid[])
  into v_selected_cancel_shift_ids
  from tmp_sel_ids tsi
  where tsi.action_id like 'CANCEL:%';

  v_selected_action_ids_count := coalesce(array_length(v_selected_action_ids, 1), 0);
  v_selected_row_keys_count := coalesce(array_length(v_selected_truth_keys, 1), 0);
  v_selected_cancel_shift_ids_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','SELECTION_PARSED',
    'selected_action_ids_count', v_selected_action_ids_count,
    'selected_row_keys_count', v_selected_row_keys_count,
    'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count
  ));

  -- sample selected action ids (cap 40)
  select coalesce(jsonb_agg(x.action_id), '[]'::jsonb)
  into v_sample_selected_action_ids
  from (
    select s.action_id
    from unnest(coalesce(v_selected_action_ids, array[]::text[])) as s(action_id)
    order by s.action_id
    limit 40
  ) as x;

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 for NHSP and constrain selection to OK rows
  -- ─────────────────────────────────────────────
  create temporary table tmp_p2_all on commit drop as
  select *
  from public.weekly_import_phase2(p_import_id := p_import_id, p_system_type := 'NHSP');

  create temporary table tmp_p2_ok on commit drop as
  select
    p2.hr_row_id,
    p2.external_row_key,
    p2.work_date,
    p2.week_ending_date,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.contract_id is not null
    and p2.work_date is not null
    and p2.week_ending_date is not null;

  select coalesce(array_agg(distinct p2.external_row_key order by p2.external_row_key), array[]::text[])
  into v_all_ok_external_keys
  from tmp_p2_ok p2;

  v_ok_keys_total := coalesce(array_length(v_all_ok_external_keys, 1), 0);

  -- selected truth keys must be present in OK universe
  if exists (
    select 1
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    left join (select distinct p2.external_row_key from tmp_p2_ok p2) as okk
      on okk.external_row_key = k.external_row_key
    where okk.external_row_key is null
  ) then
    raise exception 'nhsp_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not an OK/resolved NHSP row (resolve mappings first).';
  end if;

  select coalesce(array_agg(distinct k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_ok
  from (
    select distinct k.external_row_key
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    join (select distinct p2.external_row_key from tmp_p2_ok p2) as okk
      on okk.external_row_key = k.external_row_key
  ) as k;

  -- Tick = PROCEED semantics
  v_force_keys_final := coalesce(v_selected_truth_keys_ok, array[]::text[]);

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_all_ok_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  v_force_keys_count := coalesce(array_length(v_force_keys_final, 1), 0);
  v_skip_keys_count := coalesce(array_length(v_skip_keys_final, 1), 0);
  v_cancellations_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  -- Import-authoritative NHSP shifts and calculated expenses must never share
  -- a timesheet.  Check both an existing imported shift and the base
  -- contract-week timesheet that a new row would reuse, before any source or
  -- financial mutation begins.
  if exists (
    select 1
    from (
      select cw.timesheet_id
      from tmp_p2_ok p2
      join public.contract_weeks cw
        on cw.contract_id=p2.contract_id
       and cw.week_ending_date=p2.week_ending_date
       and cw.is_adjustment=false
       and coalesce(cw.additional_seq,0)=0
      where p2.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
        and cw.timesheet_id is not null
        and not exists (
          select 1
          from public.nhsp_shifts existing_import_shift
          where existing_import_shift.source_system='NHSP'::public.hr_source_enum
            and existing_import_shift.external_row_key=p2.external_row_key
            and existing_import_shift.cancelled_at_utc is null
        )
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      where ns.source_system='NHSP'::public.hr_source_enum
        and ns.timesheet_id is not null
        and (
          ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
          or ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        )
    ) expense_target
    where public._import_review_timesheet_has_calculated_expenses_core_v1(expense_target.timesheet_id)
  ) then
    raise exception using
      message='IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
      errcode='P0001',
      detail=jsonb_build_object(
        'code','IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
        'message','Timesheet occupied by expenses. Remove the expenses from this timesheet, save or recalculate it, then choose Recheck. Expenses must be invoiced on a separate timesheet for import-authoritative work; no import mutation was applied.'
      )::text;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE2_OK_LOADED',
    'ok_keys_total', v_ok_keys_total,
    'force_keys_count', v_force_keys_count,
    'skip_keys_count', v_skip_keys_count,
    'cancellations_count', v_cancellations_count
  ));

  -- samples (cap 40 each)
  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_force_keys
  from (
    select k as k
    from unnest(coalesce(v_force_keys_final, array[]::text[])) as k
    order by k
    limit 40
  ) as x;

  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_skip_keys
  from (
    select k as k
    from unnest(coalesce(v_skip_keys_final, array[]::text[])) as k
    order by k
    limit 40
  ) as x;

  select coalesce(jsonb_agg(y.s), '[]'::jsonb)
  into v_sample_cancel_shift_ids
  from (
    select s::text as s
    from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as s
    order by s::text
    limit 40
  ) as y;

  -- ─────────────────────────────────────────────
  -- ✅ FIX: No-op apply guard
  -- ─────────────────────────────────────────────
  v_should_run_phase1 := (v_force_keys_count > 0);
  v_should_run_phase15 := v_should_run_phase1;
  v_should_run_cancellations := (v_cancellations_count > 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','NOOP_GUARD_EVAL',
    'should_run_phase1', v_should_run_phase1,
    'should_run_phase15', v_should_run_phase15,
    'should_run_cancellations', v_should_run_cancellations,
    'reason',
      case
        when (v_should_run_phase1 is false and v_should_run_cancellations is false)
          then 'NO_SELECTION_NO_AUTONEW_NO_CANCELLATION => SKIP_TRUTH_MUTATION'
        when (v_should_run_phase1 is true and v_should_run_cancellations is false)
          then 'HAS_SELECTED_ROWS'
        when (v_should_run_phase1 is false and v_should_run_cancellations is true)
          then 'HAS_SELECTED_CANCELLATIONS'
        else 'HAS_SELECTED_ROWS_AND_CANCELLATIONS'
      end
  ));

  if (v_should_run_phase1 is false and v_should_run_cancellations is false) then
    update public.hr_imports hi_noop
    set import_scope = 'NHSP'
    where hi_noop.id = p_import_id;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED_NOOP'));

    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_WEEKLY_APPLY_DEBUG',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'steps', v_steps,

        'selected_action_ids_count', v_selected_action_ids_count,
        'selected_row_keys_count', v_selected_row_keys_count,
        'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,

        'selected_action_ids_sample', v_sample_selected_action_ids,

        'ok_keys_total', v_ok_keys_total,
        'force_keys_count', v_force_keys_count,
        'skip_keys_count', v_skip_keys_count,

        'phase1_called', false,
        'phase1_force_keys_sample', v_sample_force_keys,
        'phase1_skip_keys_sample', v_sample_skip_keys,

        'cancellations_called', false,
        'sample_cancel_shift_ids', v_sample_cancel_shift_ids,

        'invoiced_changed_keys_count', 0,
        'not_invoiced_changed_keys_count', 0,
        'phase3_created_count', 0,
        'phase3_updated_count', 0,
        'cancel_adjustment_count', 0,
        'correction_timesheets_created_count', 0,
        'affected_timesheet_ids_count', 0
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );

    v_review_result:=jsonb_build_object(
      'import_id', p_import_id,
      'mode_b', jsonb_build_object(
        'selected_truth_keys', to_jsonb(array[]::text[]),
        'force_overwrite_external_row_keys', to_jsonb(array[]::text[]),
        'skip_external_row_keys', to_jsonb(coalesce(v_all_ok_external_keys, array[]::text[])),
        'phase3', null,
        'phase1', null,
        'phase15', jsonb_build_object('ok_rows', 0, 'shift_updated_rows', 0),
        'cancellations', null
      ),
      'affected_timesheet_ids', to_jsonb(array[]::uuid[]),
      'post_commit_email_action_ids','[]'::jsonb,
      'review_operation_id',v_review_operation_id
    );
    perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,false);
    return v_review_result;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','NOOP_GUARD_PASSED'));

  -- ─────────────────────────────────────────────
  -- 3) Snapshot changed-hours rows for selected keys BEFORE any truth mutation
  -- ─────────────────────────────────────────────
  create temporary table tmp_changed_sel on commit drop as
  select
    ch.external_row_key,
    ch.timesheet_id,
    ch.is_paid,
    ch.is_invoiced
  from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'NHSP') as ch
  where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

  select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
  into v_invoiced_changed_keys
  from tmp_changed_sel cs
  where cs.is_invoiced is true;

  select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
  into v_not_invoiced_changed_keys
  from tmp_changed_sel cs
  where cs.is_invoiced is false;

  v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
  v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

  select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id), array[]::uuid[])
    into v_changed_timesheet_ids
  from tmp_changed_sel cs
  where cs.timesheet_id is not null;

  if coalesce(array_length(v_changed_timesheet_ids, 1), 0) > 0 then
    select public.import_timesheet_financial_preflight_v1(
      p_timesheet_ids := v_changed_timesheet_ids,
      p_action := 'IMPORT_CHANGED_HOURS',
      p_actor_user_id := p_actor_user_id,
      p_expected_state_json := '{}'::jsonb,
      p_lock_rows := true,
      p_max_scope := 100
    ) into v_changed_preflight;

    if coalesce((v_changed_preflight->>'allowed')::boolean, false) is not true then
      raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_changed_preflight::text;
    end if;

    if exists (
      select 1 from tmp_changed_sel cs
      where cs.is_invoiced is false
        and exists (select 1 from public.timesheets_financials paid_tf where paid_tf.timesheet_id=cs.timesheet_id and paid_tf.paid_at_utc is not null)
        and not exists (
          select 1 from public.timesheets_financials current_tf
          where current_tf.timesheet_id=cs.timesheet_id and current_tf.is_current=true
            and current_tf.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
            and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
        )
    ) then
      raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED', errcode='P0001',
        detail=jsonb_build_object(
          'code','PAID_UNINVOICED_ROLLOVER_REQUIRED',
          'required_path',jsonb_build_array(
            'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
          ),
          'invoice_policy_without_history','NOW',
          'timesheet_ids',to_jsonb(v_changed_timesheet_ids)
        )::text;
    end if;
  end if;

  -- Preserve the lifecycle state of authorised, mutable source timesheets.
  -- The source transaction performs the canonical unauthorise step before
  -- changing truth.  The Worker reauthorises exactly this persisted set only
  -- after its bounded TSFIN follow-up has completed successfully.
  select coalesce(array_agg(distinct lifecycle_scope.timesheet_id order by lifecycle_scope.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select cs.timesheet_id
    from tmp_changed_sel cs
    join public.timesheets ts on ts.timesheet_id=cs.timesheet_id and ts.is_current=true
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
    where cs.timesheet_id is not null
      and cs.is_invoiced is false
      and cs.is_paid is false
      and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
        or cw.status='AUTHORISED'::public.contract_week_status_enum)
    union
    -- A protected source timesheet can already have a later, still-mutable
    -- correction pair. Phase 3 deliberately amends that pair's replacement
    -- in place. Put its currently authorised replacement through the same
    -- canonical lifecycle; the bulk RPC expands it to the complete pair.
    select existing_replacement.timesheet_id
    from tmp_changed_sel cs
    cross join lateral (
      select ns_existing.id
      from public.nhsp_shifts ns_existing
      where ns_existing.source_system='NHSP'::public.hr_source_enum
        and ns_existing.external_row_key=cs.external_row_key
        and ns_existing.cancelled_at_utc is null
      order by ns_existing.updated_at desc nulls last,ns_existing.created_at desc nulls last
      limit 1
    ) source_shift
    cross join lateral (
      select tpos.timesheet_id
      from public.timesheets tpos
      where tpos.is_adjustment is true
        and tpos.is_current is true
        and tpos.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and jsonb_typeof(tpos.actual_schedule_json)='array'
        and tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
          'shift_id',source_shift.id::text,
          'external_row_key',cs.external_row_key
        ))
      order by tpos.updated_at desc nulls last,tpos.created_at desc nulls last
      limit 1
    ) existing_replacement
    join public.timesheets pair_pos
      on pair_pos.timesheet_id=existing_replacement.timesheet_id and pair_pos.is_current=true
    left join public.timesheets_financials pair_tf
      on pair_tf.timesheet_id=pair_pos.timesheet_id and pair_tf.is_current=true
    left join public.contract_weeks pair_cw on pair_cw.timesheet_id=pair_pos.timesheet_id
    where cs.is_invoiced is true
      and coalesce((public._import_review_timesheet_protection_core_v1(existing_replacement.timesheet_id)->>'paid')::boolean,false)=false
      and coalesce((public._import_review_timesheet_protection_core_v1(existing_replacement.timesheet_id)->>'invoice_locked')::boolean,false)=false
      and (pair_pos.authorised_at_server is not null or pair_tf.authorised_at_utc is not null
        or pair_cw.status='AUTHORISED'::public.contract_week_status_enum)
    union
    select ns.timesheet_id
    from public.nhsp_shifts ns
    join public.timesheets ts on ts.timesheet_id=ns.timesheet_id and ts.is_current=true
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
    where ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
      and ns.timesheet_id is not null
      and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'paid')::boolean,false)=false
      and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'invoice_locked')::boolean,false)=false
      and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
        or cw.status='AUTHORISED'::public.contract_week_status_enum)
  ) lifecycle_scope;

  if cardinality(v_reauthorise_timesheet_ids)>100 then
    raise exception 'IMPORT_REVIEW_REAUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
  end if;
  if cardinality(v_reauthorise_timesheet_ids)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',target_id::text,
      'expected_timesheet_id',target_id::text
    ) order by target_id),'[]'::jsonb)
    into v_lifecycle_items
    from unnest(v_reauthorise_timesheet_ids) as lifecycle_target(target_id);

    select public.timesheet_unauthorise_bulk_atomic(v_lifecycle_items,p_actor_user_id,v_now)
    into v_unauthorise_result;
    if coalesce((v_unauthorise_result->>'ok')::boolean,false) is not true
      or coalesce((v_unauthorise_result->>'all_success')::boolean,false) is not true then
      raise exception using message='IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',errcode='P0001',
        detail=jsonb_build_object(
          'code','IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',
          'timesheet_ids',to_jsonb(v_reauthorise_timesheet_ids),
          'failure_count',coalesce((v_unauthorise_result->>'failure_count')::int,cardinality(v_reauthorise_timesheet_ids))
        )::text;
    end if;
    v_steps:=v_steps||jsonb_build_array(jsonb_build_object(
      'step','CANONICAL_UNAUTHORISE_COMPLETE',
      'reauthorise_timesheet_count',cardinality(v_reauthorise_timesheet_ids)
    ));
  end if;

  v_should_run_phase3 := (v_invoiced_changed_keys_count > 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','CHANGED_HOURS_PARTITIONED',
    'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
    'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count,
    'phase3_should_run', v_should_run_phase3
  ));

  -- ─────────────────────────────────────────────
  -- 4) Policy A replacement-day enforcement (NHSP)
  -- ─────────────────────────────────────────────
  create temporary table tmp_selected_replacement_keys(
    candidate_id uuid,
    client_id uuid,
    old_work_date date,
    replacement_day_key text
  ) on commit drop;

  if array_length(v_force_keys_final, 1) is not null then
    create temporary table tmp_sel_truth_p2 on commit drop as
    select
      p2.external_row_key,
      p2.candidate_id,
      p2.client_id,
      p2.work_date as import_work_date
    from tmp_p2_ok p2
    where p2.external_row_key = any(v_force_keys_final);

    create temporary table tmp_existing_by_key on commit drop as
    select distinct on (ns.external_row_key)
      ns.external_row_key,
      ns.id as shift_id,
      ns.candidate_id as candidate_id,
      ns.client_id as client_id,
      ns.work_date as old_work_date
    from public.nhsp_shifts ns
    where ns.source_system = 'NHSP'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.external_row_key = any(v_force_keys_final)
      and ns.work_date is not null
    order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

    insert into tmp_selected_replacement_keys(candidate_id, client_id, old_work_date, replacement_day_key)
    select distinct
      (coalesce(ex.candidate_id, st.candidate_id))::uuid as candidate_id,
      (coalesce(ex.client_id, st.client_id))::uuid as client_id,
      ex.old_work_date as old_work_date,
      ((coalesce(ex.candidate_id, st.candidate_id))::text || '|' ||
       (coalesce(ex.client_id, st.client_id))::text || '|' ||
       (ex.old_work_date)::text) as replacement_day_key
    from tmp_sel_truth_p2 st
    join tmp_existing_by_key ex
      on ex.external_row_key = st.external_row_key
    where ex.old_work_date is not null
      and st.import_work_date is not null
      and ex.old_work_date <> st.import_work_date;

    select coalesce(array_agg(x::text), array[]::text[])
    into v_selected_cancel_shift_id_set
    from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as x;

    if exists (select 1 from tmp_selected_replacement_keys) then
      create temporary table tmp_required_cancel_ids on commit drop as
      select distinct
        rk.replacement_day_key,
        ns2.id as shift_id
      from tmp_selected_replacement_keys rk
      join public.nhsp_shifts ns2
        on ns2.source_system = 'NHSP'::public.hr_source_enum
       and ns2.cancelled_at_utc is null
       and ns2.candidate_id = rk.candidate_id
       and ns2.client_id = rk.client_id
       and ns2.work_date = rk.old_work_date;

      if exists (
        select 1
        from tmp_required_cancel_ids rc
        left join unnest(v_selected_cancel_shift_id_set) as sel(shift_id_text)
          on sel.shift_id_text = rc.shift_id::text
        where sel.shift_id_text is null
      ) then
        raise exception 'nhsp_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
      end if;
    end if;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','POLICY_A_OK'));

  -- ─────────────────────────────────────────────
  -- 5) Changed-hours correction series for invoiced keys (BEFORE Phase 1)
  -- ─────────────────────────────────────────────
  if v_should_run_phase3 then
    select public.nhsp_weekly_phase3_apply_adjustment_truth(
      p_import_id := p_import_id,
      p_selected_external_row_keys := v_invoiced_changed_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_phase3_result;
  end if;

  v_phase3_created_count := jsonb_array_length(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb));
  v_phase3_updated_count := jsonb_array_length(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb));

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE3_CORRECTIONS_DONE',
    'phase3_called', v_should_run_phase3,
    'phase3_created_count', v_phase3_created_count,
    'phase3_updated_count', v_phase3_updated_count
  ));

  -- ─────────────────────────────────────────────
  -- 6) Phase 1 upsert (NHSP) with tick-only skip/force
  -- ─────────────────────────────────────────────
  if v_should_run_phase1 then
    select public.nhsp_apply_import_phase1(
      p_import_id := p_import_id,
      p_selected_group_ids := array[]::text[],
      p_skip_external_row_keys := v_skip_keys_final,
      p_force_overwrite_external_row_keys := v_force_keys_final
    )
    into v_phase1_result;
  else
    v_phase1_result := null;
  end if;

  v_phase1_shifts_created := coalesce(nullif((coalesce(v_phase1_result,'{}'::jsonb)->>'shifts_created')::int, null), 0);
  v_phase1_shifts_updated := coalesce(nullif((coalesce(v_phase1_result,'{}'::jsonb)->>'shifts_updated')::int, null), 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE1_DONE',
    'phase1_called', v_should_run_phase1,
    'phase1_shifts_created', v_phase1_shifts_created,
    'phase1_shifts_updated', v_phase1_shifts_updated
  ));

  -- ─────────────────────────────────────────────
  -- 7) Phase 1.5 repair (NHSP)
  -- ─────────────────────────────────────────────
  if v_should_run_phase15 then
    create temporary table tmp_phase15_rows on commit drop as
    select *
    from public.weekly_import_apply_phase2(p_import_id := p_import_id, p_system_type := 'NHSP');

    select count(*)::int
    into v_phase15_ok
    from tmp_phase15_rows r
    where upper(coalesce(r.action::text,'')) = 'OK';

    select count(*)::int
    into v_phase15_updated
    from tmp_phase15_rows r
    where coalesce(r.shift_updated,false) is true;
  else
    v_phase15_ok := 0;
    v_phase15_updated := 0;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE15_DONE',
    'phase15_called', v_should_run_phase15,
    'phase15_ok_rows', v_phase15_ok,
    'phase15_shift_updated_rows', v_phase15_updated
  ));

  -- ─────────────────────────────────────────────
  -- 8) Apply selected cancellations (explicit shift_id only; NHSP)
  -- ─────────────────────────────────────────────
  if v_should_run_cancellations then
    create temporary table tmp_cancel_meta on commit drop as
    select
      ns.id as shift_id,
      ns.candidate_id,
      ns.client_id,
      ns.work_date
    from public.nhsp_shifts ns
    where ns.id = any(v_selected_cancel_shift_ids);

    create temporary table tmp_selected_rep_keys_text on commit drop as
    select distinct
      rk.replacement_day_key
    from tmp_selected_replacement_keys rk;

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'shift_id', cm.shift_id::text,
          'reason',
            case
              when exists (
                select 1
                from tmp_selected_rep_keys_text sr
                where sr.replacement_day_key = (cm.candidate_id::text || '|' || cm.client_id::text || '|' || cm.work_date::text)
              ) then 'REPLACEMENT_DAY'
              else 'MISSING_FROM_IMPORT'
            end
        )
      ),
      '[]'::jsonb
    )
    into v_cancel_actions
    from tmp_cancel_meta cm;

    select public.nhsp_weekly_apply_cancellations(
      p_import_id := p_import_id,
      p_actions := v_cancel_actions,
      p_actor_user_id := p_actor_user_id
    )
    into v_cancellations_result;
  else
    v_cancellations_result := null;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','CANCELLATIONS_DONE',
    'cancellations_called', v_should_run_cancellations
  ));

  -- ─────────────────────────────────────────────
  -- ✅ 8.5) ENSURE BASE WEEKLY TIMESHEET EXISTS + ATTACH ACTIVE NHSP SHIFTS
  -- ─────────────────────────────────────────────
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','ENSURE_BASE_WEEKLY_START'));

  create temporary table tmp_ensure_pairs(
    contract_id uuid,
    candidate_id uuid,
    client_id uuid,
    week_ending_date date
  ) on commit drop;

  insert into tmp_ensure_pairs(contract_id, candidate_id, client_id, week_ending_date)
  select distinct
    p2ok.contract_id,
    p2ok.candidate_id,
    p2ok.client_id,
    p2ok.week_ending_date
  from tmp_p2_ok p2ok
  where p2ok.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

  if array_length(v_selected_cancel_shift_ids, 1) is not null then
    insert into tmp_ensure_pairs(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      ns.contract_id,
      ns.candidate_id,
      ns.client_id,
      ns.week_ending_date
    from public.nhsp_shifts ns
    where ns.id = any(v_selected_cancel_shift_ids)
      and ns.contract_id is not null
      and ns.client_id is not null
      and ns.candidate_id is not null
      and ns.week_ending_date is not null;
  end if;

  create temporary table tmp_ensure_pairs_u on commit drop as
  select distinct
    tep.contract_id,
    tep.candidate_id,
    tep.client_id,
    tep.week_ending_date
  from tmp_ensure_pairs tep
  where tep.contract_id is not null
    and tep.client_id is not null
    and tep.candidate_id is not null
    and tep.week_ending_date is not null;

  select count(*)::int
  into v_ensure_pairs_count
  from tmp_ensure_pairs_u teu;

  select coalesce(jsonb_agg(jsonb_build_object(
    'contract_id', teu.contract_id::text,
    'week_ending_date', teu.week_ending_date::text
  )), '[]'::jsonb)
  into v_ensure_sample_pairs
  from (
    select teu.contract_id, teu.week_ending_date
    from tmp_ensure_pairs_u teu
    order by teu.contract_id::text, teu.week_ending_date::text
    limit 20
  ) as teu;

  drop table if exists pg_temp.tmp_aff_ts;
  create temporary table tmp_aff_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  create temporary table tmp_ensure_created_ts_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  for v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date in
    select teu.contract_id, teu.candidate_id, teu.client_id, teu.week_ending_date
    from tmp_ensure_pairs_u teu
    order by teu.contract_id::text, teu.week_ending_date::text
  loop

    select count(*)::int
    into v_active_count
    from public.nhsp_shifts ns_active
    where ns_active.source_system = 'NHSP'::public.hr_source_enum
      and ns_active.cancelled_at_utc is null
      and ns_active.contract_id = v_pair_contract_id
      and ns_active.candidate_id = v_pair_candidate_id
      and ns_active.client_id = v_pair_client_id
      and ns_active.week_ending_date = v_pair_week_ending_date;

    if coalesce(v_active_count, 0) <= 0 then
      v_ensure_pairs_skipped_no_active := v_ensure_pairs_skipped_no_active + 1;
      continue;
    end if;

    v_base_week_id := null;
    v_base_week_ts_id := null;

    select cw0.id, cw0.timesheet_id
    into v_base_week_id, v_base_week_ts_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_pair_contract_id
      and cw0.week_ending_date = v_pair_week_ending_date
      and cw0.is_adjustment is false
      and coalesce(cw0.additional_seq, 0) = 0
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        status,
        submission_mode_snapshot,
        timesheet_id,
        planned_schedule_json,
        created_at,
        updated_at,
        is_adjustment
      )
      values (
        v_pair_contract_id,
        v_pair_week_ending_date,
        0,
        'SUBMITTED'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        null,
        v_now,
        v_now,
        false
      )
      returning id into v_base_week_id;

      v_ensure_base_week_created_count := v_ensure_base_week_created_count + 1;
      v_base_week_ts_id := null;
    else
      v_ensure_base_week_existing_count := v_ensure_base_week_existing_count + 1;
    end if;

    if v_base_week_ts_id is not null then
      select exists(
        select 1
        from public.timesheets tchk
        where tchk.timesheet_id = v_base_week_ts_id
        limit 1
      )
      into v_ts_exists;

      if v_ts_exists is not true then
        update public.contract_weeks cw0u
        set
          timesheet_id = null,
          updated_at = v_now
        where cw0u.id = v_base_week_id;

        v_ensure_timesheet_missing_reference_count := v_ensure_timesheet_missing_reference_count + 1;
        v_base_week_ts_id := null;
      end if;
    end if;

    select ct.display_site, ct.ward_hint, ct.role
    into v_contract_display_site, v_contract_ward_hint, v_contract_role
    from public.contracts ct
    where ct.id = v_pair_contract_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_pair_candidate_id
    limit 1;

    select cli.name
    into v_client_name
    from public.clients cli
    where cli.id = v_pair_client_id
    limit 1;

    v_occupant_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_pair_candidate_id::text));
    v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_pair_client_id::text));
    v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
    v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

    v_shift_label_norm := 'weekly-0';

    v_booking_base :=
      v_occupant_norm || '|' ||
      v_pair_week_ending_date::text || '|' ||
      v_hospital_norm || '|' ||
      v_ward_norm || '|' ||
      v_role_norm || '|' ||
      v_shift_label_norm;

    v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
    v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

    if v_base_week_ts_id is null then
      v_new_ts_id := null;

      insert into public.timesheets(
        booking_id,
        version,
        is_current,
        status,

        sheet_scope,
        submission_mode,
        line_type,
        authorised_at_server,

        occupant_key_norm,
        hospital_norm,
        ward_norm,
        job_title_norm,
        shift_label_norm,

        week_ending_date,
        contract_id,

        manual_pdf_r2_key,
        actual_schedule_json,

        qr_payload_json,
        candidate_hint_text,

        is_adjustment,
        parent_timesheet_id,
        correction_id,
        correction_kind,
        adjustment_origin,

        created_at,
        updated_at
      )
      values (
        v_booking_id,
        1,
        true,
        'RECEIVED'::public.timesheet_status_enum,

        'WEEKLY'::public.timesheet_scope_enum,
        'MANUAL'::public.submission_mode_enum,
        'HOURS'::public.timesheet_line_type_enum,
        null,

        v_occupant_norm,
        v_hospital_norm,
        v_ward_norm,
        v_role_norm,
        v_shift_label_norm,

        v_pair_week_ending_date,
        v_pair_contract_id,

        null,
        '[]'::jsonb,

        '{}'::jsonb,
        null,

        false,
        null,
        null,
        null,
        null,

        v_now,
        v_now
      )
      returning timesheet_id into v_new_ts_id;

      v_ensure_timesheet_created_count := v_ensure_timesheet_created_count + 1;
      v_base_week_ts_id := v_new_ts_id;

      insert into tmp_ensure_created_ts_ids(timesheet_id)
      values (v_new_ts_id)
      on conflict do nothing;

      update public.contract_weeks cw0link
      set
        timesheet_id = v_new_ts_id,
        status = case
          when cw0link.status = 'AUTHORISED'::public.contract_week_status_enum then cw0link.status
          else 'SUBMITTED'::public.contract_week_status_enum
        end,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      where cw0link.id = v_base_week_id;

      -- ✅ NEW: user-facing audit line for "birth of base weekly timesheet" (NHSP)
      perform public._audit_insert(
        'timesheets',
        v_new_ts_id::text,
        'NHSP_IMPORT_TIMESHEET_CREATED',
        null,
        jsonb_build_object(
          'import_id', p_import_id::text,
          'source_system', 'NHSP',
          'kind', 'BASE_WEEKLY',
          'contract_id', v_pair_contract_id::text,
          'contract_week_id', v_base_week_id::text,
          'candidate_id', v_pair_candidate_id::text,
          'client_id', v_pair_client_id::text,
          'week_ending_date', v_pair_week_ending_date::text,
          'booking_id', v_booking_id,
          'active_shifts_count', v_active_count
        ),
        'IMPORT_BIRTH',
        p_actor_user_id
      );

    else
      v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

      update public.contract_weeks cw0keep
      set
        status = case
          when cw0keep.status = 'AUTHORISED'::public.contract_week_status_enum then cw0keep.status
          else 'SUBMITTED'::public.contract_week_status_enum
        end,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      where cw0keep.id = v_base_week_id;

      if exists (
        select 1
        from public.timesheets identity_target
        where identity_target.timesheet_id=v_base_week_ts_id
          and (
            identity_target.week_ending_date is distinct from v_pair_week_ending_date
            or identity_target.contract_id is distinct from v_pair_contract_id
            or identity_target.occupant_key_norm is distinct from v_occupant_norm
            or identity_target.hospital_norm is distinct from v_hospital_norm
            or identity_target.ward_norm is distinct from v_ward_norm
            or identity_target.job_title_norm is distinct from v_role_norm
          )
      ) then
        select public.import_timesheet_financial_preflight_v1(
          p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
          p_action := 'IMPORT_FINANCIAL_IDENTITY_CHANGE',
          p_actor_user_id := p_actor_user_id,
          p_expected_state_json := '{}'::jsonb,
          p_lock_rows := true,
          p_max_scope := 100
        ) into v_changed_preflight;

        if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
          raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
        end if;

        if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
          raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        elsif v_changed_preflight->>'required_path'='UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE' then
          raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
              'paid_uninvoiced_rollover_required',false
            )::text;
        elsif v_changed_preflight->>'required_path'='PAID_UNINVOICED_ROLLOVER'
          and not exists (
            select 1 from public.timesheets_financials rollover_identity
            where rollover_identity.timesheet_id=v_base_week_ts_id
              and rollover_identity.is_current=true
              and rollover_identity.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
              and coalesce((rollover_identity.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
          ) then
          raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path',jsonb_build_array(
                'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
              ),
              'invoice_policy_without_history','NOW'
            )::text;
        end if;
      end if;

      update public.timesheets tnorm
      set
        is_current = true,
        status = 'RECEIVED'::public.timesheet_status_enum,
        sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
        submission_mode = 'MANUAL'::public.submission_mode_enum,
        line_type = 'HOURS'::public.timesheet_line_type_enum,
        week_ending_date = v_pair_week_ending_date,
        contract_id = v_pair_contract_id,
        occupant_key_norm = v_occupant_norm,
        hospital_norm = v_hospital_norm,
        ward_norm = v_ward_norm,
        job_title_norm = v_role_norm,
        shift_label_norm = v_shift_label_norm,
        updated_at = v_now
      where tnorm.timesheet_id = v_base_week_ts_id;
    end if;

    if exists (
      select 1 from public.nhsp_shifts ns_scope
      where ns_scope.source_system = 'NHSP'::public.hr_source_enum
        and ns_scope.cancelled_at_utc is null
        and ns_scope.contract_id = v_pair_contract_id
        and ns_scope.candidate_id = v_pair_candidate_id
        and ns_scope.client_id = v_pair_client_id
        and ns_scope.week_ending_date = v_pair_week_ending_date
        and (
          ns_scope.timesheet_id is null
          or not exists (
            select 1 from public.timesheets existing_link
            where existing_link.timesheet_id=ns_scope.timesheet_id
          )
        )
    ) then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
        p_action := 'IMPORT_SOURCE_ASSIGNMENT',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_changed_preflight;

      if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
      end if;

      if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
        raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
          )::text;
      end if;

      if exists (
        select 1 from public.timesheets source_target
        left join public.timesheets_financials source_target_tf
          on source_target_tf.timesheet_id=source_target.timesheet_id and source_target_tf.is_current=true
        where source_target.timesheet_id=v_base_week_ts_id
          and (source_target.authorised_at_server is not null or source_target_tf.authorised_at_utc is not null)
          and not exists (
            select 1 from public.timesheets_financials paid_target
            where paid_target.timesheet_id=source_target.timesheet_id
              and paid_target.paid_at_utc is not null
          )
      ) then
        raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
            'paid_uninvoiced_rollover_required',false
          )::text;
      end if;

      if exists (
        select 1 from public.timesheets_financials paid_source
        where paid_source.timesheet_id=v_base_week_ts_id and paid_source.paid_at_utc is not null
      ) and not exists (
        select 1 from public.timesheets_financials rollover_source
        where rollover_source.timesheet_id=v_base_week_ts_id and rollover_source.is_current=true
          and rollover_source.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
          and coalesce((rollover_source.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
      ) then
        raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW'
          )::text;
      end if;
    end if;

    update public.nhsp_shifts nsu0
    set
      timesheet_id = v_base_week_ts_id,
      updated_at = v_now
    where nsu0.source_system = 'NHSP'::public.hr_source_enum
      and nsu0.cancelled_at_utc is null
      and nsu0.contract_id = v_pair_contract_id
      and nsu0.candidate_id = v_pair_candidate_id
      and nsu0.client_id = v_pair_client_id
      and nsu0.week_ending_date = v_pair_week_ending_date
      and nsu0.timesheet_id is null;

    get diagnostics v_attached_null_count = row_count;
    v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

    update public.nhsp_shifts nsu1
    set
      timesheet_id = v_base_week_ts_id,
      updated_at = v_now
    where nsu1.source_system = 'NHSP'::public.hr_source_enum
      and nsu1.cancelled_at_utc is null
      and nsu1.contract_id = v_pair_contract_id
      and nsu1.candidate_id = v_pair_candidate_id
      and nsu1.client_id = v_pair_client_id
      and nsu1.week_ending_date = v_pair_week_ending_date
      and nsu1.timesheet_id is not null
      and not exists (
        select 1
        from public.timesheets tmiss
        where tmiss.timesheet_id = nsu1.timesheet_id
        limit 1
      );

    get diagnostics v_relinked_invalid_count = row_count;
    v_ensure_shifts_relinked_invalid_ts_count := v_ensure_shifts_relinked_invalid_ts_count + coalesce(v_relinked_invalid_count, 0);

    select count(*)::int
    into v_active_count
    from public.nhsp_shifts nscheck
    where nscheck.source_system = 'NHSP'::public.hr_source_enum
      and nscheck.cancelled_at_utc is null
      and nscheck.contract_id = v_pair_contract_id
      and nscheck.candidate_id = v_pair_candidate_id
      and nscheck.client_id = v_pair_client_id
      and nscheck.week_ending_date = v_pair_week_ending_date
      and (
        nscheck.timesheet_id is null
        or not exists (
          select 1
          from public.timesheets tchk2
          where tchk2.timesheet_id = nscheck.timesheet_id
          limit 1
        )
      );

    if coalesce(v_active_count, 0) > 0 then
      v_ensure_remaining_active_detached_count := v_ensure_remaining_active_detached_count + v_active_count;
      raise exception
        'nhsp_weekly_apply_transactional: ENSURE invariant failed (active NHSP shifts remain detached or linked to missing timesheets) contract_id=% week_ending_date=% remaining=%.',
        v_pair_contract_id, v_pair_week_ending_date, v_active_count;
    end if;

    insert into tmp_aff_ts(timesheet_id)
    values (v_base_week_ts_id)
    on conflict do nothing;

  end loop;

  select coalesce(jsonb_agg(x.ts_id), '[]'::jsonb)
  into v_ensure_sample_created_ts_ids
  from (
    select tct.timesheet_id::text as ts_id
    from tmp_ensure_created_ts_ids tct
    order by tct.timesheet_id::text
    limit 20
  ) as x;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','ENSURE_BASE_WEEKLY_DONE',
    'ensure_pairs_count', v_ensure_pairs_count,
    'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
    'base_week_created_count', v_ensure_base_week_created_count,
    'base_week_existing_count', v_ensure_base_week_existing_count,
    'base_timesheet_created_count', v_ensure_timesheet_created_count,
    'base_timesheet_reused_count', v_ensure_timesheet_reused_count,
    'missing_timesheet_reference_count', v_ensure_timesheet_missing_reference_count,
    'shifts_attached_null_count', v_ensure_shifts_attached_count,
    'shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
    'sample_pairs', v_ensure_sample_pairs,
    'sample_created_ts_ids', v_ensure_sample_created_ts_ids
  ));

  -- ─────────────────────────────────────────────
  -- 9) Compute affected_timesheet_ids (union of ensure + corrections + cancellations + non-invoiced updates)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_force_keys_non_invoiced
  from (
    select distinct fk.external_row_key
    from unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
    left join unnest(coalesce(v_invoiced_changed_keys, array[]::text[])) as ik(external_row_key)
      on ik.external_row_key = fk.external_row_key
    where ik.external_row_key is null
  ) as k;

  insert into tmp_aff_ts(timesheet_id)
  select (x.value)::uuid
  from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select (x2.value)::uuid
  from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb)) as x2(value)
  where nullif(btrim(x2.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select (x3.value)::uuid
  from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb)) as x3(value)
  where nullif(btrim(x3.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct ns.timesheet_id
  from public.nhsp_shifts ns
  where ns.source_system = 'NHSP'::public.hr_source_enum
    and ns.cancelled_at_utc is null
    and ns.external_row_key = any(coalesce(v_force_keys_non_invoiced, array[]::text[]))
    and ns.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct partner.timesheet_id
  from tmp_aff_ts seed
  join public.timesheets seed_ts
    on seed_ts.timesheet_id=seed.timesheet_id
   and seed_ts.is_current=true
   and seed_ts.correction_id is not null
   and upper(btrim(coalesce(seed_ts.adjustment_origin,''))) in (
     'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
     'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
   )
  join public.timesheets partner
    on partner.correction_id=seed_ts.correction_id
   and partner.is_current=true
   and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
     'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
     'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
   )
  on conflict do nothing;

  -- Persist and enqueue complete correction units, including unchanged legs.
  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null;

  -- Authorised-state restoration and imported financial correction members
  -- are mandatory regardless of the ordinary auto-authorise setting.  This
  -- keeps a reversal/replacement unit authorised together and also covers a
  -- reversal-only cancellation without fabricating a replacement.
  select coalesce(array_agg(distinct required.timesheet_id order by required.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select existing.timesheet_id
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) existing(timesheet_id)
    union all
    select correction.timesheet_id
    from unnest(coalesce(v_affected_timesheet_ids,array[]::uuid[])) affected(timesheet_id)
    join public.timesheets correction on correction.timesheet_id=affected.timesheet_id
    where correction.is_current=true
      and correction.revoked_at is null
      and coalesce(correction.is_adjustment,false)
      and correction.correction_id is not null
  ) required
  where required.timesheet_id is not null;

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_affected_timesheet_ids,'NHSP'::public.hr_source_enum,false
  );

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  if jsonb_array_length(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) > 0 then
    create temporary table tmp_cancel_aff_ts(ts_id uuid primary key) on commit drop;

    insert into tmp_cancel_aff_ts(ts_id)
    select distinct (x4.value)::uuid
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x4(value)
    where nullif(btrim(x4.value), '') is not null
    on conflict do nothing;

    select count(*)::int
    into v_cancel_adjustment_count
    from tmp_cancel_aff_ts cts
    join public.timesheets tts
      on tts.timesheet_id = cts.ts_id
    where tts.is_adjustment is true;
  else
    v_cancel_adjustment_count := 0;
  end if;

  v_correction_timesheets_created_count := (v_phase3_created_count + v_phase3_updated_count + coalesce(v_cancel_adjustment_count, 0));

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','AFFECTED_TS_DONE',
    'affected_timesheet_ids_count', coalesce(array_length(v_affected_timesheet_ids, 1), 0),
    'cancel_adjustment_count', v_cancel_adjustment_count,
    'correction_timesheets_created_count', v_correction_timesheets_created_count
  ));

  -- ─────────────────────────────────────────────
  -- 10) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 only after no work remains.
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set import_scope = 'NHSP'
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED'));

  -- ─────────────────────────────────────────────
  -- 11) Debug audit (invoice_debug gated inside _imp_debug_audit)
  -- ─────────────────────────────────────────────
  perform public._imp_debug_audit(
    p_actor_user_id,
    'NHSP_WEEKLY_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'steps', v_steps,

      'selected_action_ids_count', v_selected_action_ids_count,
      'selected_row_keys_count', v_selected_row_keys_count,
      'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
      'selected_action_ids_sample', v_sample_selected_action_ids,

      'ok_keys_total', v_ok_keys_total,

      'phase1_called', v_should_run_phase1,
      'phase1_force_keys_count', v_force_keys_count,
      'phase1_skip_keys_count', v_skip_keys_count,
      'phase1_force_keys_sample', v_sample_force_keys,
      'phase1_skip_keys_sample', v_sample_skip_keys,
      'phase1_shifts_created', v_phase1_shifts_created,
      'phase1_shifts_updated', v_phase1_shifts_updated,

      'phase15_called', v_should_run_phase15,
      'phase15_ok_rows', v_phase15_ok,
      'phase15_shift_updated_rows', v_phase15_updated,

      'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
      'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count,
      'phase3_called', v_should_run_phase3,
      'phase3_created_count', v_phase3_created_count,
      'phase3_updated_count', v_phase3_updated_count,

      'cancellations_called', v_should_run_cancellations,
      'cancellations_count', v_cancellations_count,
      'sample_cancel_shift_ids', v_sample_cancel_shift_ids,

      'ensure_pairs_count', v_ensure_pairs_count,
      'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
      'ensure_base_week_created_count', v_ensure_base_week_created_count,
      'ensure_base_week_existing_count', v_ensure_base_week_existing_count,
      'ensure_timesheet_created_count', v_ensure_timesheet_created_count,
      'ensure_timesheet_reused_count', v_ensure_timesheet_reused_count,
      'ensure_timesheet_missing_reference_count', v_ensure_timesheet_missing_reference_count,
      'ensure_shifts_attached_null_count', v_ensure_shifts_attached_count,
      'ensure_shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
      'ensure_sample_pairs', v_ensure_sample_pairs,
      'ensure_sample_created_ts_ids', v_ensure_sample_created_ts_ids,

      'cancel_adjustment_count', v_cancel_adjustment_count,
      'correction_timesheets_created_count', v_correction_timesheets_created_count,

      'affected_timesheet_ids_count', coalesce(array_length(v_affected_timesheet_ids, 1), 0)
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'mode_b', jsonb_build_object(
      'selected_truth_keys', to_jsonb(coalesce(v_selected_truth_keys_ok, array[]::text[])),
      'force_overwrite_external_row_keys', to_jsonb(coalesce(v_force_keys_final, array[]::text[])),
      'skip_external_row_keys', to_jsonb(coalesce(v_skip_keys_final, array[]::text[])),
      'phase3', v_phase3_result,
      'phase1', v_phase1_result,
      'phase15', jsonb_build_object(
        'ok_rows', v_phase15_ok,
        'shift_updated_rows', v_phase15_updated
      ),
      'cancellations', v_cancellations_result
    ),
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_timesheet_ids',to_jsonb(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])),
    'post_commit_reauthorise_timesheet_ids',to_jsonb(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])),
    'post_commit_email_action_ids','[]'::jsonb,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_WEEKLY_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'steps', v_steps,
        'sqlstate', v_sqlstate,
        'error', v_err,

        'selected_action_ids_count', v_selected_action_ids_count,
        'selected_row_keys_count', v_selected_row_keys_count,
        'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
        'selected_action_ids_sample', v_sample_selected_action_ids
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- Restore import_review_actions_page_v1 from backend Git HEAD before this implementation.
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
  v_confirmation_counts jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_page<1 or v_page>10000 or v_size not in (25,50,75,100)
     or v_sort not in ('CANDIDATE','CLIENT','WEEK_ENDING','WORK_DATE','ACTION','STATUS')
     or v_direction not in ('ASC','DESC')
     or v_view not in ('ALL','PENDING','READY','EMAIL','NO_ACTION','CONFIRM_STANDARD',
       'CONFIRM_NON_STANDARD','CONFIRM_VALIDATION','CONFIRM_EMAIL','CONFIRM_REFERENCE') then
    raise exception 'IMPORT_REVIEW_ACTION_PAGE_INPUT_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.import_review_states s where s.import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;

  with ready_ids as (
    select r.action_id from public._import_review_ready_action_ids_core_v1(p_import_id) r
  ), current_actions as (
    select d.*,(ready.action_id is not null) batch_eligible,
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
      case when d.summary_json->>'resolution_kind'='WEEKLY_ASSIGNMENT_CONTRACT'
        then coalesce(current_weekly_options.options,'[]'::jsonb)
        else coalesce(d.summary_json->'resolution_options','[]'::jsonb) end resolution_options,
      coalesce(d.summary_json->'protection','{}'::jsonb) protection,
      d.summary_json->>'default_excluded_reason' default_excluded_reason,
      nullif(d.summary_json->>'week_ending_date','')::date week_ending_date,
      nullif(d.summary_json->>'work_date','')::date work_date
    from public.import_review_decisions d
    left join ready_ids ready on ready.action_id=d.action_id
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
    left join lateral (
      -- Resolution options are revalidated when they are read so a durable
      -- review created before a settings/contract correction cannot keep
      -- showing stale disabled options.  This only authorises creation of the
      -- assignment mapping; refresh/finalisation still reclassifies the row
      -- and enforces rates, authority and all financial guards independently.
      select coalesce(jsonb_agg(
        option_row.option_json || jsonb_strip_nulls(jsonb_build_object(
          'option_id',case when option_contract.id is not null then 'contract:'||option_contract.id::text end,
          'contract_id',option_contract.id,
          'candidate_id',option_contract.candidate_id,
          'client_id',option_contract.client_id,
          'role',option_contract.role,
          'band',option_contract.band,
          'site',option_contract.display_site,
          'start_date',option_contract.start_date,
          'end_date',option_contract.end_date,
          'source_route_eligible',coalesce(option_authority.route_eligible,false),
          'authority_mode',option_authority.authority_mode,
          'selectable',option_contract.id is not null
            and option_contract.candidate_id=d.candidate_id
            and option_contract.client_id=d.client_id
            and option_contract.start_date<=coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
            and (option_contract.end_date is null
              or option_contract.end_date>=coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local))
            and coalesce(option_authority.route_eligible,false),
          'disabled_reason_code',case when option_contract.id is null
              or option_contract.candidate_id is distinct from d.candidate_id
              or option_contract.client_id is distinct from d.client_id
              or option_contract.start_date>coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
              or (option_contract.end_date is not null
                and option_contract.end_date<coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local))
              or not coalesce(option_authority.route_eligible,false)
            then 'CONTRACT_NOT_ELIGIBLE' end,
          'display_label',case when option_contract.id is not null then concat_ws(' · ',
            nullif(option_contract.role,''),nullif(option_contract.band,''),nullif(option_contract.display_site,''),
            to_char(option_contract.start_date,'DD Mon YYYY')||' to '||
              coalesce(to_char(option_contract.end_date,'DD Mon YYYY'),'open ended')) end
        )) order by lower(coalesce(option_contract.role,option_row.option_json->>'role','')),
          lower(coalesce(option_contract.band,option_row.option_json->>'band','')),
          option_contract.start_date desc nulls last,option_row.option_json->>'option_id'
      ),'[]'::jsonb) options
      from jsonb_array_elements(coalesce(d.summary_json->'resolution_options','[]'::jsonb)) option_row(option_json)
      left join public.contracts option_contract on option_contract.id=case
        when coalesce(option_row.option_json->>'contract_id','')
          ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
          then (option_row.option_json->>'contract_id')::uuid end
      left join lateral public._import_review_effective_authority_core_v1(
        case when upper(coalesce(d.summary_json->>'source_system',d.summary_json->>'source_route',''))='NHSP'
          then 'NHSP' else 'HR_WEEKLY' end,
        option_contract.id,option_contract.client_id,
        coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
      ) option_authority on option_contract.id is not null
      where d.summary_json->>'resolution_kind'='WEEKLY_ASSIGNMENT_CONTRACT'
    ) current_weekly_options on true
    where d.import_id=p_import_id and d.is_current
  ), branch_badge_rows as (
    select a.candidate_branch_key,b.badge_code,label.badge_label,count(*)::integer badge_count,'ISSUE'::text badge_tone
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
        when 'TIMESHEET_OCCUPIED_BY_EXPENSES' then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
        when 'TIMESHEET_NOT_FOUND' then 'TIMESHEET_MISSING'
        when 'WEEKLY_TIMESHEET_NOT_SUBMITTED' then 'TIMESHEET_NOT_SUBMITTED'
        when 'DAILY_TIMESHEET_NOT_SUBMITTED' then 'TIMESHEET_NOT_SUBMITTED'
        when 'WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET' then 'SHIFT_MISSING_FROM_TIMESHEET'
        when 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET' then 'SHIFT_MISSING_FROM_TIMESHEET'
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
      when 'RATES_INCOMPLETE' then 'Rates incomplete'
      when 'TIMESHEET_OCCUPIED_BY_EXPENSES' then 'Timesheet occupied by expenses'
      when 'TIMESHEET_MISSING' then 'Timesheet missing'
      when 'TIMESHEET_NOT_SUBMITTED' then 'Timesheet not submitted'
      when 'SHIFT_MISSING_FROM_TIMESHEET' then 'Shift missing from timesheet'
      when 'CHOOSE_TIMESHEET' then 'Choose timesheet' when 'BANKING_PAY_PROTECTED' then 'Banking Pay protected'
      when 'NEEDS_RECHECK' then 'Needs recheck' when 'HOURS_DIFFER' then 'Hours differ'
      when 'TIMES_DIFFER' then 'Times differ' when 'BREAK_DIFFERS' then 'Break differs'
      when 'MISSING_FROM_FILE' then 'Missing from file' when 'NOT_IN_CLOUDTMS' then 'Shift not in CloudTMS'
      when 'REFERENCE_ISSUE' then 'Reference issue' when 'MATCH_UNCLEAR' then 'Match unclear'
      when 'EMAIL_NOT_CONFIGURED' then 'Email not configured' when 'WEEKLY_MISMATCH' then 'Weekly mismatch'
      when 'EMAIL_REQUEST_SELECTED' then 'Email request selected' when 'REMINDER_AVAILABLE' then 'Reminder available'
      when 'REFERENCE_REVIEW' then 'Reference review' else b.badge_code end badge_label) label
    group by a.candidate_branch_key,b.badge_code,label.badge_label
    union all
    select a.candidate_branch_key,'READY_ACTION:'||a.action_kind,
      case a.action_kind
        when 'INCLUDE_SHIFT' then 'TMS to add shift'
        when 'APPLY_AMENDMENT' then case
          when a.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT'
          then 'TMS to amend replacement shift'
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse and replace shift' else 'TMS to amend shift' end
        when 'APPLY_CANCELLATION' then case
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse shift' else 'TMS to cancel shift' end
        when 'MARK_VALIDATION_ERROR' then 'Validate timesheet'
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Link existing timesheet'
        when 'EMAIL_ISSUE' then 'Request client correction'
        when 'EMAIL_REMINDER' then 'Request client correction reminder'
        else regexp_replace(a.outcome_label,'^TMS will ','TMS to ','i') end,
      count(*)::integer,'READY'::text
    from current_actions a
    where a.batch_eligible and a.selected and a.action_category in ('READY','EMAIL')
    group by a.candidate_branch_key,a.action_kind,
      case a.action_kind
        when 'INCLUDE_SHIFT' then 'TMS to add shift'
        when 'APPLY_AMENDMENT' then case
          when a.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT'
          then 'TMS to amend replacement shift'
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse and replace shift' else 'TMS to amend shift' end
        when 'APPLY_CANCELLATION' then case
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse shift' else 'TMS to cancel shift' end
        when 'MARK_VALIDATION_ERROR' then 'Validate timesheet'
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Link existing timesheet'
        when 'EMAIL_ISSUE' then 'Request client correction'
        when 'EMAIL_REMINDER' then 'Request client correction reminder'
        else regexp_replace(a.outcome_label,'^TMS will ','TMS to ','i') end
    union all
    select a.candidate_branch_key,'DEFERRED_ACTION','Deferred',count(*)::integer,'DEFERRED'::text
    from current_actions a
    where a.selectable and not a.selected and a.action_category in ('READY','EMAIL')
    group by a.candidate_branch_key
    union all
    select 'candidate:'||o.candidate_id::text,'COMPLETED_ACTION:'||o.action_kind,
      o.completed_label,count(*)::integer,'COMPLETED'::text
    from public.import_review_action_outcomes o
    where o.import_id=p_import_id
    group by o.candidate_id,o.action_kind,o.completed_label
  ), branch_badges as (
    select candidate_branch_key,jsonb_agg(jsonb_build_object(
      'code',badge_code,'label',badge_label,'count',badge_count,'tone',badge_tone)
      order by case badge_tone when 'ISSUE' then 1 when 'READY' then 2 when 'DEFERRED' then 3 else 4 end,badge_label) badges
    from branch_badge_rows badges
    where badges.badge_code<>'NOT_IN_CLOUDTMS'
      or not exists (
        select 1 from branch_badge_rows other
        where other.candidate_branch_key=badges.candidate_branch_key
          and other.badge_code<>'NOT_IN_CLOUDTMS' and other.badge_tone='ISSUE'
      )
    group by candidate_branch_key
  ), filtered as (
    select a.*,coalesce(bb.badges,'[]'::jsonb) branch_badges
    from current_actions a left join branch_badges bb using(candidate_branch_key) where case v_view
      when 'PENDING' then a.blocking or a.action_category in ('PENDING','BLOCKED')
      when 'READY' then a.action_category='READY'
      when 'EMAIL' then a.action_category='EMAIL'
      when 'NO_ACTION' then a.action_category='NO_ACTION'
      when 'CONFIRM_STANDARD' then a.selected and a.batch_eligible and a.action_kind='INCLUDE_SHIFT'
      when 'CONFIRM_NON_STANDARD' then a.selected and a.batch_eligible and a.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION')
      when 'CONFIRM_VALIDATION' then a.selected and a.batch_eligible and a.action_kind='MARK_VALIDATION_ERROR'
      when 'CONFIRM_EMAIL' then a.selected and a.batch_eligible and a.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
      when 'CONFIRM_REFERENCE' then a.selected and a.batch_eligible and a.action_kind='INVALIDATE_REFERENCE'
      else true end
  ), ordered as (
    select f.*,
      row_number() over(order by
        case when v_view like 'CONFIRM_%' then lower(client_name) end asc nulls last,
        case when v_view like 'CONFIRM_%' then candidate_surname_sort end asc nulls last,
        case when v_view like 'CONFIRM_%' then lower(candidate_name) end asc nulls last,
        case when v_view like 'CONFIRM_%' then work_date end asc nulls last,
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
      count(*) over() total_count,
      count(*) over(partition by candidate_branch_key) candidate_section_total_count,
      count(*) over(partition by client_id,client_name) client_section_total_count
    from filtered f
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'action_id',action_id,'action_kind',action_kind,'action_category',action_category,
      'target_key',target_key,'source_identity',source_identity,
      'hr_row_id',hr_row_id,'timesheet_id',timesheet_id,'shift_id',shift_id,
      'client_id',client_id,'candidate_id',candidate_id,'contract_id',contract_id,'issue_id',issue_id,
      'preview_generation',preview_generation,'evidence_fingerprint',evidence_fingerprint,
      'selectable',selectable,'selected',selected,'blocking',blocking,'batch_eligible',batch_eligible,
      'candidate_name',candidate_name,'candidate_surname_sort',candidate_surname_sort,
      'candidate_branch_key',candidate_branch_key,'branch_badges',branch_badges,
      'candidate_section_total_count',candidate_section_total_count,
      'client_section_total_count',client_section_total_count,
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

  with ready_ids as (
    select r.action_id from public._import_review_ready_action_ids_core_v1(p_import_id) r
  ), selected_actions as (
    select d.action_kind,coalesce(d.summary_json->'protection','{}'::jsonb) protection,
      d.summary_json->>'amendment_route' amendment_route
    from public.import_review_decisions d
    join ready_ids r on r.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.selected
  )
  select jsonb_build_object(
    'selected_total',count(*),
    'standard',count(*) filter(where action_kind='INCLUDE_SHIFT'),
    'non_standard',count(*) filter(where action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION')),
    'amendment',count(*) filter(where action_kind='APPLY_AMENDMENT'
      and (amendment_route='AMEND_EXISTING_REPLACEMENT'
        or not (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false)))),
    'reversal_replacement',count(*) filter(where action_kind='APPLY_AMENDMENT'
      and amendment_route is distinct from 'AMEND_EXISTING_REPLACEMENT'
      and (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false))),
    'cancellation',count(*) filter(where action_kind='APPLY_CANCELLATION'
      and not (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false))),
    'reversal_only',count(*) filter(where action_kind='APPLY_CANCELLATION'
      and (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false))),
    'validation',count(*) filter(where action_kind='MARK_VALIDATION_ERROR'),
    'email',count(*) filter(where action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')),
    'reference',count(*) filter(where action_kind='INVALIDATE_REFERENCE')
  ) into v_confirmation_counts from selected_actions;

  return jsonb_build_object(
    'ok',true,'import_id',p_import_id,'view',v_view,'view_counts',v_counts,
    'confirmation_counts',v_confirmation_counts,
    'items',v_items,'total_items',v_total,'page_number',v_page,'page_size',v_size,
    'total_pages',case when v_total=0 then 0 else ceiling(v_total::numeric/v_size)::integer end,
    'has_previous',v_page>1,'has_next',v_page*v_size<v_total,
    'sort_by',v_sort,'sort_direction',v_direction
  );
end
$function$;

-- Restore _import_review_apply_complete_core_v1 from backend Git HEAD before this implementation.
create or replace function public._import_review_apply_complete_core_v1(p_import_id uuid,p_operation_id uuid,p_actor_user_id uuid,p_response_json jsonb,p_follow_up_required boolean)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_tsfin_required boolean; v_email_required boolean; v_response jsonb; v_refresh jsonb;
  v_selected_ids text[]; v_remaining_blockers integer; v_remaining_selectable integer;
  v_terminal boolean; v_result_status text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status<>'APPLYING' or v.last_operation_id<>p_operation_id or o.id is null then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_MISMATCH' using errcode='40001';
  end if;
  select coalesce(array_agg(value order by value),array[]::text[]) into v_selected_ids
  from jsonb_array_elements_text(coalesce(o.response_json#>'{request_envelope,selected_action_ids}','[]'::jsonb)) value;
  if cardinality(v_selected_ids)=0 then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_ACTION_SET_MISSING' using errcode='55000';
  end if;
  v_tsfin_required:=jsonb_typeof(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))>0;
  v_email_required:=jsonb_typeof(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))>0;
  v_response:=coalesce(p_response_json,'{}'::jsonb)||jsonb_build_object(
    'review_tsfin_follow_up_status',case when v_tsfin_required then 'PENDING' else 'NOT_REQUIRED' end,
    'review_email_follow_up_status',case when v_email_required then 'PENDING' else 'NOT_REQUIRED' end,
    'applied_action_ids',to_jsonb(v_selected_ids),'applied_action_count',cardinality(v_selected_ids));

  insert into public.import_review_action_outcomes(
    action_id,import_id,operation_id,action_kind,source_identity,candidate_id,client_id,contract_id,
    hr_row_id,timesheet_id,shift_id,evidence_fingerprint,completed_label,summary_json,applied_by_user_id
  )
  select d.action_id,p_import_id,p_operation_id,d.action_kind,d.source_identity,d.candidate_id,d.client_id,d.contract_id,
    d.hr_row_id,d.timesheet_id,d.shift_id,d.evidence_fingerprint,
    case d.action_kind
      when 'INCLUDE_SHIFT' then 'TMS added shift'
      when 'APPLY_AMENDMENT' then case
        when coalesce((d.summary_json#>>'{protection,paid}')::boolean,false)
          or coalesce((d.summary_json#>>'{protection,invoice_locked}')::boolean,false)
        then 'TMS reversed and replaced shift' else 'TMS amended shift' end
      when 'APPLY_CANCELLATION' then case
        when coalesce((d.summary_json#>>'{protection,paid}')::boolean,false)
          or coalesce((d.summary_json#>>'{protection,invoice_locked}')::boolean,false)
        then 'TMS reversed shift' else 'TMS cancelled shift' end
      when 'MARK_VALIDATION_ERROR' then 'Timesheet validated'
      when 'INVALIDATE_REFERENCE' then 'Stored reference cleared'
      when 'DAILY_TIMESHEET_RESOLUTION' then 'Timesheet linked'
      when 'EMAIL_ISSUE' then 'Client query queued'
      when 'EMAIL_REMINDER' then 'Client query reminder queued'
      when 'NO_ACTION' then 'No action confirmed'
      else 'Review action completed' end,
    d.summary_json,p_actor_user_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.candidate_id is not null and d.client_id is not null
  on conflict(action_id) do nothing;
  if (select count(*) from public.import_review_action_outcomes x
      where x.import_id=p_import_id and x.operation_id=p_operation_id)<>cardinality(v_selected_ids) then
    raise exception 'IMPORT_REVIEW_APPLY_OUTCOME_SET_MISMATCH' using errcode='55000';
  end if;

  update public.import_review_daily_timesheet_resolutions r set status='APPLIED',applied_operation_id=p_operation_id,applied_at_utc=now(),updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and exists(
    select 1 from public.import_review_decisions d where d.import_id=r.import_id and d.hr_row_id=r.hr_row_id
      and d.is_current and d.action_id=any(v_selected_ids));

  update public.import_review_states set status='IN_REVIEW',state_version=state_version+1,
    follow_up_status=case when v_tsfin_required or v_email_required or p_follow_up_required then 'PENDING' else 'NOT_REQUIRED' end,
    follow_up_error_code=null,follow_up_error_message=null,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v;

  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v.state_version,p_actor_user_id,5000);
  select count(*) filter(where d.blocking),count(*) filter(where d.selectable and not (
      d.action_kind='NO_ACTION' and exists(select 1 from public.import_review_action_outcomes x
        where x.import_id=d.import_id and x.source_identity=d.source_identity)))
    into v_remaining_blockers,v_remaining_selectable
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;
  v_terminal:=v_remaining_blockers=0 and v_remaining_selectable=0;
  if v_terminal then
    update public.import_review_states set status='APPLIED',state_version=state_version+1,
      applied_at_utc=now(),applied_by_user_id=p_actor_user_id,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id
    where import_id=p_import_id returning * into v;
    update public.hr_imports set applied_at=coalesce(applied_at,now()) where id=p_import_id;
  else
    select * into v from public.import_review_states where import_id=p_import_id;
  end if;
  v_result_status:=v.status;
  v_response:=v_response||jsonb_build_object(
    'partial_application',not v_terminal,
    'review_status_after_commit',v_result_status,
    'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
  update public.import_apply_operations
  set state=case when v_tsfin_required or v_email_required or p_follow_up_required then 'SOURCE_COMMITTED_TSFIN_PENDING' else 'COMPLETE' end,
    committed_at_utc=coalesce(committed_at_utc,now()),
    finalised_at_utc=case when v_tsfin_required or v_email_required or p_follow_up_required then finalised_at_utc else coalesce(finalised_at_utc,now()) end,
    response_json=response_json||v_response,updated_at_utc=now()
  where id=p_operation_id;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v.state_version,p_operation_id,'APPLY_COMMITTED',p_actor_user_id,jsonb_build_object(
    'follow_up_status',v.follow_up_status,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable));
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,
    'state_version',v.state_version,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
end $function$;

-- Restore bulk_timesheet_row_patch_v1 from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_patch_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_dataset_mode text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  v_projection text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'projection', v_filters->>'profile', 'status_patch')), ''));

  v_row_keys text[] := NULL;
  v_previous_row_key text := NULL;
  v_actor_user_id uuid := NULL;

  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;
  v_has_row_key_filter boolean := FALSE;

  v_changed_domains text[] := ARRAY[]::text[];
  v_status_only_hint boolean := FALSE;
  v_manual_changed_hint boolean := FALSE;
  v_evidence_changed_hint boolean := FALSE;
  v_storage_changed_hint boolean := FALSE;
  v_identity_changed_hint boolean := FALSE;
BEGIN
  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'mode', '')), ''));
  END IF;

  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := NULL;
  END IF;

  IF v_projection NOT IN ('status_patch', 'dataset_row', 'summary_row', 'active_row_header') THEN
    v_projection := 'status_patch';
  END IF;

  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'previous_row_key', v_filters->>'previousRowKey', '')), '');

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId', '')), '') IS NOT NULL
       AND COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId') ~* v_uuid_re THEN
      v_actor_user_id := COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId')::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_actor_user_id := NULL;
  END;

  v_has_row_key_filter :=
    (v_filters ? 'row_key')
    OR (v_filters ? 'rowKey')
    OR (v_filters ? 'row_keys')
    OR (v_filters ? 'rowKeys');

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey')), '')];
  END IF;

  IF v_has_row_key_filter AND COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('row_keys', to_jsonb(v_row_keys));
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids));
  END IF;

  IF v_filters ? 'changed_domains' AND jsonb_typeof(v_filters->'changed_domains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changed_domains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF v_filters ? 'changedDomains' AND jsonb_typeof(v_filters->'changedDomains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changedDomains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains', '')), '') IS NOT NULL THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(split_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM unnest(regexp_split_to_array(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains'), '\s*,\s*')) AS split_values(value)
    WHERE NULLIF(BTRIM(split_values.value), '') IS NOT NULL;
  END IF;

  v_identity_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['identity','adoption','row_identity','contract_week_to_timesheet','contract-week-to-timesheet','created_timesheet']::text[], FALSE);

  v_evidence_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['evidence','attached_evidence','queue_evidence','document','documents']::text[], FALSE);

  v_storage_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['storage','storage_key','storage-key','preview','artifact','primary_artifact']::text[], FALSE);

  v_manual_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['manual','schedule','editor','save','process','unprocess','financials','tsfin','additional_units','expenses']::text[], FALSE);

  -- Process-mode mutation callers may request a lightweight status_patch without
  -- explicit changed_domains. Treat those as manual/editor-affecting so the
  -- frontend fetches one fresh active_row_visible context instead of preserving
  -- stale schedule/expense/editor state as if this were a pure status patch.
  IF NOT v_manual_changed_hint
     AND COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
     AND v_dataset_mode = 'process'
     AND v_projection = 'status_patch' THEN
    v_manual_changed_hint := TRUE;
  END IF;

  v_status_only_hint :=
    (
      COALESCE(v_changed_domains && ARRAY['status','authorise','authorize','unauthorise','unauthorize','processing_status','authorisation','authorization']::text[], FALSE)
      OR (
        COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
        AND v_dataset_mode = 'authorise'
        AND v_projection = 'status_patch'
      )
    )
    AND NOT v_identity_changed_hint
    AND NOT v_evidence_changed_hint
    AND NOT v_storage_changed_hint
    AND NOT v_manual_changed_hint;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      source_rows.timesheet_id AS member_timesheet_id
    FROM source_rows
    WHERE source_rows.timesheet_id IS NOT NULL

    UNION

    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM source_rows
    JOIN public.timesheets AS anchor_timesheet
      ON anchor_timesheet.timesheet_id = source_rows.timesheet_id
     AND anchor_timesheet.is_current = TRUE
     AND anchor_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
    JOIN public.timesheets AS unit_timesheet
      ON unit_timesheet.booking_id = anchor_timesheet.booking_id
  ),
  retention_by_row AS MATERIALIZED (
    SELECT
      retention_unit_members.row_timesheet_id,
      COALESCE(BOOL_OR(marker.timesheet_id IS NOT NULL), FALSE) AS has_retained_financial_history
    FROM retention_unit_members
    LEFT JOIN public.timesheet_financial_retention AS marker
      ON marker.timesheet_id = retention_unit_members.member_timesheet_id
    GROUP BY retention_unit_members.row_timesheet_id
  ),
  enriched_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      timesheet_row.version AS timesheet_version,
      timesheet_row.updated_at AS timesheet_updated_at,
      timesheet_row.is_current AS timesheet_is_current,
      timesheet_row.manual_pdf_r2_key,
      timesheet_row.qr_r2_key,
      timesheet_row.generated_pdf_at_utc,
      timesheet_row.manual_pdf_rotation_degrees,
      timesheet_row.qr_token AS timesheet_qr_token,
      timesheet_row.qr_generated_at AS timesheet_qr_generated_at,
      timesheet_row.qr_scanned_at AS timesheet_qr_scanned_at,
      COALESCE(retention_unit.has_retained_financial_history, FALSE) AS has_retained_financial_history,
      contract_week_row.updated_at AS contract_week_updated_at,
      contract_week_row.uploaded_pdf_r2_key,
      contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
      contract_week_row.totals_json AS contract_week_totals_json,
      contract_week_row.is_adjustment AS contract_week_is_adjustment,
      contract_week_row.additional_seq AS contract_week_additional_seq,
      timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
      timesheet_row.is_adjustment AS timesheet_is_adjustment,
      timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
      timesheet_row.correction_id AS timesheet_correction_id,
      timesheet_row.correction_kind AS timesheet_correction_kind,
      timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
      financial_row.updated_at AS tsfin_updated_at,
      financial_row.actual_schedule_json AS tsfin_actual_schedule_json,
      financial_row.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
      financial_row.total_hours AS tsfin_total_hours,
      financial_row.expenses_pay_ex_vat AS tsfin_expenses_pay_ex_vat,
      financial_row.expenses_charge_ex_vat AS tsfin_expenses_charge_ex_vat,
      financial_row.mileage_units AS tsfin_mileage_units,
      financial_row.mileage_pay_ex_vat AS tsfin_mileage_pay_ex_vat,
      financial_row.mileage_charge_ex_vat AS tsfin_mileage_charge_ex_vat,
      financial_row.travel_pay_ex_vat AS tsfin_travel_pay_ex_vat,
      financial_row.travel_charge_ex_vat AS tsfin_travel_charge_ex_vat,
      financial_row.accommodation_pay_ex_vat AS tsfin_accommodation_pay_ex_vat,
      financial_row.accommodation_charge_ex_vat AS tsfin_accommodation_charge_ex_vat,
      financial_row.other_pay_ex_vat AS tsfin_other_pay_ex_vat,
      financial_row.other_charge_ex_vat AS tsfin_other_charge_ex_vat,
      financial_row.authorised_at_utc AS tsfin_authorised_at_utc,
      financial_row.processed_at_utc AS tsfin_processed_at_utc,
      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS evidence_attached_count,
      evidence_summary.primary_storage_key AS evidence_primary_storage_key,
      evidence_summary.primary_display_name AS evidence_primary_display_name,
      CASE
        WHEN evidence_summary.evidence_updated_at IS NOT NULL
         AND staged_evidence_summary.staged_evidence_updated_at IS NOT NULL
          THEN GREATEST(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
        ELSE COALESCE(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
      END AS evidence_updated_at,
      COALESCE(evidence_summary.has_attached_timesheet_evidence, FALSE) AS evidence_has_attached_timesheet,
      COALESCE(evidence_summary.has_attached_mileage_evidence, FALSE) AS evidence_has_attached_mileage,
      COALESCE(evidence_summary.has_attached_travel_evidence, FALSE) AS evidence_has_attached_travel,
      COALESCE(evidence_summary.has_attached_accommodation_evidence, FALSE) AS evidence_has_attached_accommodation,
      COALESCE(evidence_summary.has_attached_other_evidence, FALSE) AS evidence_has_attached_other,
      COALESCE(staged_evidence_summary.staged_evidence_count, 0)::integer AS evidence_staged_count,
      staged_evidence_summary.primary_staged_storage_key AS evidence_primary_staged_storage_key,
      staged_evidence_summary.primary_staged_display_name AS evidence_primary_staged_display_name,
      staged_evidence_summary.primary_staged_kind AS evidence_primary_staged_kind,
      COALESCE(staged_evidence_summary.has_staged_timesheet_evidence, FALSE) AS evidence_has_staged_timesheet,
      COALESCE(staged_evidence_summary.has_staged_mileage_evidence, FALSE) AS evidence_has_staged_mileage,
      COALESCE(staged_evidence_summary.has_staged_travel_evidence, FALSE) AS evidence_has_staged_travel,
      COALESCE(staged_evidence_summary.has_staged_accommodation_evidence, FALSE) AS evidence_has_staged_accommodation,
      COALESCE(staged_evidence_summary.has_staged_other_evidence, FALSE) AS evidence_has_staged_other,
      UPPER(COALESCE(source_rows.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(source_rows.submission_mode::text, '')) AS submission_mode_upper,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope::text, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(source_rows.sheet_scope::text, ''))
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type,
      (
        COALESCE(source_rows.is_qr, FALSE)
        OR source_rows.qr_status IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(timesheet_row.qr_token, '')), '') IS NOT NULL
        OR timesheet_row.qr_generated_at IS NOT NULL
      ) AS is_qr_route,
      (
        UPPER(COALESCE(source_rows.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'MANUAL'
        AND (
          COALESCE(timesheet_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.additional_seq, source_rows.additional_seq, 0) > 0
          OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
          OR timesheet_row.parent_timesheet_id IS NOT NULL
        )
        AND NOT (
          UPPER(COALESCE(timesheet_row.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
          OR NULLIF(BTRIM(COALESCE(timesheet_row.correction_kind, '')), '') IS NOT NULL
          OR timesheet_row.correction_id IS NOT NULL
        )
      ) AS is_manual_additional_adjustment_calc
    FROM source_rows
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN retention_by_row AS retention_unit
      ON retention_unit.row_timesheet_id = source_rows.timesheet_id
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET'), FALSE) AS has_attached_timesheet_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'MILEAGE'), FALSE) AS has_attached_mileage_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TRAVEL'), FALSE) AS has_attached_travel_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_attached_accommodation_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'OTHER'), FALSE) AS has_attached_other_evidence,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name,
        MAX(timesheet_evidence_row.created_at) AS evidence_updated_at
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(staged_queue_row.id)::integer AS staged_evidence_count,
        MAX(staged_queue_row.uploaded_at_utc) AS staged_evidence_updated_at,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence,
        (ARRAY_AGG(
          staged_queue_row.r2_key
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_storage_key,
        (ARRAY_AGG(
          staged_queue_row.display_name
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_display_name,
        (ARRAY_AGG(
          staged_queue_row.staged_kind_upper
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_kind
      FROM (
        SELECT
          manual_queue_row.id,
          manual_queue_row.r2_key,
          manual_queue_row.uploaded_at_utc,
          COALESCE(
            NULLIF(BTRIM(COALESCE(manual_queue_row.original_filename, '')), ''),
            NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'display_name', manual_queue_row.meta_json->>'displayName', manual_queue_row.meta_json->>'filename', manual_queue_row.meta_json->>'original_filename', manual_queue_row.meta_json->>'originalFilename', '')), '')
          ) AS display_name,
          CASE
            WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
              manual_queue_row.meta_json->>'staged_kind',
              manual_queue_row.meta_json->>'stagedKind',
              manual_queue_row.meta_json->>'evidence_kind',
              manual_queue_row.meta_json->>'evidenceKind',
              manual_queue_row.meta_json->>'kind',
              manual_queue_row.meta_json->>'type',
              manual_queue_row.meta_json->>'evidence_type',
              manual_queue_row.meta_json->>'evidenceType',
              ''
            )), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
              THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
                manual_queue_row.meta_json->>'staged_kind',
                manual_queue_row.meta_json->>'stagedKind',
                manual_queue_row.meta_json->>'evidence_kind',
                manual_queue_row.meta_json->>'evidenceKind',
                manual_queue_row.meta_json->>'kind',
                manual_queue_row.meta_json->>'type',
                manual_queue_row.meta_json->>'evidence_type',
                manual_queue_row.meta_json->>'evidenceType',
                ''
              )), ''), 'TIMESHEET'))
            ELSE 'OTHER'
          END AS staged_kind_upper
        FROM public.manual_timesheet_queue AS manual_queue_row
        WHERE source_rows.timesheet_id IS NULL
          AND source_rows.contract_week_id IS NOT NULL
          AND UPPER(COALESCE(manual_queue_row.status, '')) = 'STAGED'
          AND NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'contract_week_id', '')), '') = source_rows.contract_week_id::text
      ) AS staged_queue_row
    ) AS staged_evidence_summary ON TRUE
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      enriched_rows.*,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
        ) THEN 'NHSP'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
         AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN NULL::text
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      (
        enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required_calc,
      (
        enriched_rows.timesheet_id IS NULL
        AND enriched_rows.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.contract_week_status::text, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed_calc,
      (
        enriched_rows.timesheet_id IS NOT NULL
        AND UPPER(COALESCE(enriched_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (
          UPPER(COALESCE(enriched_rows.processing_status::text, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed_calc,
      (
        UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'ARCHIVED'
        OR COALESCE(enriched_rows.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.invoice_segments_locked, 0) > 0
        OR COALESCE(enriched_rows.invoice_is_paid, FALSE) = TRUE
      ) AS locked_calc,
      (
        enriched_rows.authorised_at_server IS NOT NULL
        OR enriched_rows.tsfin_authorised_at_utc IS NOT NULL
      ) AS is_authorised_calc,
      (
        UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(enriched_rows.client_requires_hr, FALSE) = TRUE
          AND COALESCE(enriched_rows.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'PENDING'
        AND (
          NULLIF(BTRIM(COALESCE(enriched_rows.timesheet_qr_token, '')), '') IS NOT NULL
          OR enriched_rows.timesheet_qr_generated_at IS NOT NULL
        )
        AND enriched_rows.timesheet_qr_scanned_at IS NULL
      ) AS qr_pending_awaiting_signature_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'USED'
        AND enriched_rows.timesheet_qr_scanned_at IS NOT NULL
      ) AS qr_signed_returned_calc,
      COALESCE(
        enriched_rows.evidence_primary_storage_key,
        enriched_rows.evidence_primary_staged_storage_key,
        NULLIF(enriched_rows.manual_pdf_r2_key, ''),
        NULLIF(enriched_rows.qr_r2_key, ''),
        NULLIF(enriched_rows.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key_calc,
      COALESCE(
        enriched_rows.evidence_primary_display_name,
        enriched_rows.evidence_primary_staged_display_name,
        CASE WHEN NULLIF(enriched_rows.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(enriched_rows.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(enriched_rows.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name_calc
    FROM enriched_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      CASE
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
         AND classified_rows.route_subfamily_calc = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) AS is_unprocessed_calc,
      CASE
        WHEN (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(classified_rows.client_hr_validation_required, FALSE) = TRUE
        AND UPPER(COALESCE(classified_rows.validation_status::text, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_awaiting_calc,
      (
        classified_rows.qr_pending_awaiting_signature_calc = TRUE
        OR UPPER(COALESCE(classified_rows.processing_status::text, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(COALESCE(classified_rows.issue_codes, ARRAY[]::text[]))
      ) AS qr_unsigned_blocked_calc,
      CASE
        WHEN classified_rows.primary_artifact_storage_key_calc IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode_calc
    FROM classified_rows
  ),
  final_rows AS MATERIALIZED (
    SELECT
      decision_rows.*,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || decision_rows.contract_week_id::text
        ELSE ''::text
      END AS row_key_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN decision_rows.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.requires_authorisation_calc = TRUE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.qr_unsigned_blocked_calc = FALSE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_authorise_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = TRUE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_unauthorise_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
      ) AS unprocess_action_visible_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
        AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = FALSE
      ) AS can_unprocess_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL
         AND decision_rows.locked_calc = FALSE
         AND decision_rows.is_authorised_calc = FALSE
         AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
         AND decision_rows.is_unprocessed_calc = FALSE
         AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = TRUE
      ) AS can_process_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR (decision_rows.contract_week_id IS NOT NULL AND decision_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND UPPER(COALESCE(decision_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (decision_rows.locked_by_invoice_id IS NOT NULL OR COALESCE(decision_rows.invoice_segments_locked, 0) > 0) = FALSE
        AND decision_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        decision_rows.locked_calc = TRUE
        OR decision_rows.is_authorised_calc = TRUE
        OR decision_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc,
      (
        decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND COALESCE(decision_rows.is_adjusted, FALSE) = FALSE
        AND (
          (decision_rows.period_type = 'WEEKLY' AND decision_rows.contract_week_id IS NOT NULL)
          OR (decision_rows.period_type = 'DAILY' AND decision_rows.timesheet_id IS NOT NULL)
        )
        AND (
          decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
          OR (decision_rows.route_family_calc = 'MANUAL_NON_QR' AND decision_rows.submission_mode_upper = 'MANUAL' AND decision_rows.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual_calc
    FROM decision_rows
  ),
  signed_rows_base AS MATERIALIZED (
    SELECT
      final_rows.*,
      md5(concat_ws('|',
        COALESCE(final_rows.timesheet_id::text, ''),
        COALESCE(final_rows.contract_week_id::text, ''),
        COALESCE(final_rows.timesheet_version::text, final_rows.timesheet_version::text, ''),
        COALESCE(final_rows.timesheet_updated_at::text, ''),
        COALESCE(final_rows.contract_week_updated_at::text, ''),
        COALESCE(final_rows.tsfin_updated_at::text, ''),
        COALESCE(final_rows.evidence_updated_at::text, ''),
        COALESCE(final_rows.evidence_staged_count::text, ''),
        COALESCE(final_rows.evidence_has_staged_timesheet::text, ''),
        COALESCE(final_rows.evidence_has_staged_mileage::text, ''),
        COALESCE(final_rows.evidence_has_staged_travel::text, ''),
        COALESCE(final_rows.evidence_has_staged_accommodation::text, ''),
        COALESCE(final_rows.evidence_has_staged_other::text, ''),
        COALESCE(final_rows.processing_status::text, ''),
        COALESCE(final_rows.summary_stage, ''),
        COALESCE(final_rows.tools_stage, ''),
        COALESCE(final_rows.authorised_at_server::text, ''),
        COALESCE(final_rows.tsfin_authorised_at_utc::text, ''),
        COALESCE(final_rows.is_authorised_calc::text, ''),
        COALESCE(final_rows.locked_calc::text, ''),
        COALESCE(final_rows.has_retained_financial_history::text, ''),
        COALESCE(final_rows.unprocess_action_visible_calc::text, ''),
        COALESCE(final_rows.can_unprocess_calc::text, ''),
        COALESCE(final_rows.unprocess_block_reason, ''),
        COALESCE(final_rows.route_family_calc, ''),
        COALESCE(final_rows.route_subfamily_calc, ''),
        COALESCE(final_rows.bulk_process_bucket_calc, ''),
        COALESCE(final_rows.bulk_authorise_classification_calc, ''),
        COALESCE(final_rows.primary_artifact_storage_key_calc, ''),
        COALESCE(final_rows.total_hours::text, ''),
        COALESCE(final_rows.total_pay_ex_vat::text, ''),
        COALESCE(final_rows.total_charge_ex_vat::text, ''),
        COALESCE(final_rows.margin_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_units::text, ''),
        COALESCE(final_rows.tsfin_mileage_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_charge_ex_vat::text, ''),
        COALESCE(final_rows.issue_codes::text, '')
      )) AS row_signature_calc
    FROM final_rows
  ),
  signed_rows AS MATERIALIZED (
    SELECT
      signed_rows_base.*,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS backend_row_signature_calc,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS mutation_row_signature_calc
    FROM signed_rows_base
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(signed_rows_base.timesheet_id, signed_rows_base.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      signed_rows.*,
      CASE
        WHEN signed_rows.can_bulk_authorise_calc THEN 'processed_eligible'
        WHEN signed_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible'
        ELSE NULL::text
      END AS bulk_authorise_section_calc,
      (
        v_identity_changed_hint
        OR (
          v_previous_row_key IS NOT NULL
          AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
        )
      ) AS identity_changed_calc,
      (
        v_status_only_hint
        AND NOT (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        )
      ) AS status_only_calc,
      jsonb_build_object(
        'status_only', (
          v_status_only_hint
          AND NOT (
            v_identity_changed_hint
            OR (
              v_previous_row_key IS NOT NULL
              AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            )
          )
        ),
        'manual_changed', v_manual_changed_hint,
        'evidence_changed', v_evidence_changed_hint,
        'storage_changed', v_storage_changed_hint,
        'identity_changed', (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        ),
        'invalidate_context', false,
        'invalidate_row_context', false,
        'invalidate_preview', CASE WHEN v_storage_changed_hint OR v_evidence_changed_hint THEN true ELSE false END,
        'invalidate_evidence', CASE WHEN v_evidence_changed_hint OR v_storage_changed_hint THEN true ELSE false END,
        'invalidate_editor_context', CASE WHEN v_manual_changed_hint THEN true ELSE false END,
        'row_keys', CASE
          WHEN v_previous_row_key IS NOT NULL
           AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            THEN jsonb_build_array(v_previous_row_key, signed_rows.row_key_calc)
          ELSE jsonb_build_array(signed_rows.row_key_calc)
        END,
        'timesheet_ids', CASE WHEN signed_rows.timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.timesheet_id) END,
        'contract_week_ids', CASE WHEN signed_rows.contract_week_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.contract_week_id) END,
        'storage_keys', CASE WHEN signed_rows.primary_artifact_storage_key_calc IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.primary_artifact_storage_key_calc) END,
        'row_signature', signed_rows.row_signature_calc,
        'backend_row_signature', signed_rows.backend_row_signature_calc,
        'mutation_row_signature', signed_rows.mutation_row_signature_calc,
        'datasets', CASE
          WHEN v_dataset_mode = 'authorise' THEN jsonb_build_array('bulk_authorise')
          WHEN v_dataset_mode = 'process' THEN jsonb_build_array('bulk_process')
          ELSE jsonb_build_array('bulk_process', 'bulk_authorise')
        END
      ) AS cache_hints_json,
      jsonb_build_object(
        'processed_eligible', 0,
        'authorised_eligible', 0,
        'unprocessed', 0,
        'processed', 0,
        'total', 0
      ) AS count_deltas_json,
      (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) AS keep_additional_manual_adjustment_schedule_empty_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json, '[]'::jsonb)
      END AS actual_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.contract_week_planned_schedule_json, '[]'::jsonb)
      END AS planned_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN 0::numeric
        ELSE COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric)
      END AS total_hours_calc,
      CASE
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
           OR COALESCE(signed_rows.client_is_nhsp, FALSE) = TRUE
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
           OR COALESCE(signed_rows.client_autoprocess_hr, FALSE) = TRUE
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE signed_rows.route_type
      END AS effective_route_type_calc,
      (
        COALESCE(signed_rows.timesheet_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_additional_seq, signed_rows.additional_seq, 0) > 0
        OR COALESCE(signed_rows.is_adjustment, FALSE) = TRUE
        OR signed_rows.timesheet_parent_timesheet_id IS NOT NULL
        OR signed_rows.timesheet_correction_id IS NOT NULL
        OR signed_rows.timesheet_correction_kind IS NOT NULL
      ) AS effective_is_adjustment_calc
    FROM signed_rows
  )
  SELECT
    (
      jsonb_build_object(
        'id', COALESCE(payload_rows.timesheet_id::text, payload_rows.contract_week_id::text),
        'row_key', payload_rows.row_key_calc,
        'previous_row_key', v_previous_row_key,
        'new_row_key', payload_rows.row_key_calc,
        'stable_row_id', payload_rows.stable_row_id_calc,
        'timesheet_id', payload_rows.timesheet_id,
        'current_timesheet_id', payload_rows.timesheet_id,
        'requested_timesheet_id', payload_rows.timesheet_id,
        'expected_timesheet_id', payload_rows.timesheet_id,
        'contract_week_id', payload_rows.contract_week_id,
        'contract_id', COALESCE((SELECT contract_lookup.contract_id FROM public.timesheets AS contract_lookup WHERE contract_lookup.timesheet_id = payload_rows.timesheet_id AND contract_lookup.is_current = TRUE LIMIT 1), (SELECT contract_week_lookup.contract_id FROM public.contract_weeks AS contract_week_lookup WHERE contract_week_lookup.id = payload_rows.contract_week_id LIMIT 1)),
        'row_signature', payload_rows.row_signature_calc,
        'backend_row_signature', payload_rows.backend_row_signature_calc,
        'mutation_row_signature', payload_rows.mutation_row_signature_calc,
        'render_signature', payload_rows.row_signature_calc,
        'previous_row_signature', NULL::text,
        'timesheet_version', payload_rows.timesheet_version,
        'current_version', payload_rows.timesheet_version,
        'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
        'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'actor_user_id', v_actor_user_id,
        'projection', v_projection,
        'dataset_mode', v_dataset_mode
      )
      || jsonb_build_object(
        'candidate_id', payload_rows.candidate_id,
        'candidate_name', payload_rows.candidate_name,
        'candidate_display_name', payload_rows.candidate_name,
        'client_id', payload_rows.client_id,
        'client_name', payload_rows.client_name,
        'client_display_name', payload_rows.client_name,
        'booking_id', payload_rows.booking_id,
        'booking_ref', payload_rows.booking_id,
        'occupant_key_norm', payload_rows.occupant_key_norm,
        'hospital_name', payload_rows.hospital_norm,
        'hospital_norm', payload_rows.hospital_norm,
        'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
        'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
        'period_type', payload_rows.period_type,
        'sheet_scope', payload_rows.sheet_scope,
        'timesheet_scope', payload_rows.sheet_scope,
        'submission_mode', payload_rows.submission_mode,
        'submission_mode_snapshot', payload_rows.submission_mode,
        'basis', payload_rows.basis,
        'route_type', payload_rows.effective_route_type_calc,
        'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
        'route_family', payload_rows.route_family_calc,
        'route_subfamily', payload_rows.route_subfamily_calc,
        'underlying_channel_family', payload_rows.underlying_channel_family_calc,
        'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', payload_rows.compare_block_required_calc,
        'is_adjustment', payload_rows.effective_is_adjustment_calc,
        'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
        'actual_schedule_json', payload_rows.actual_schedule_json_calc,
        'planned_schedule_json', payload_rows.planned_schedule_json_calc,
        'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc
      )
      || jsonb_build_object(
        'processing_status', payload_rows.processing_status::text,
        'processing_status_display', payload_rows.processing_status_display,
        'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
        'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
        'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
        'is_authorised', payload_rows.is_authorised_calc,
        'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
        'authorised_at_server', payload_rows.authorised_at_server,
        'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
        'requires_authorisation', payload_rows.requires_authorisation_calc,
        'locked', payload_rows.locked_calc,
        'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
        'paid_at_utc', payload_rows.paid_at_utc,
        'review_only', payload_rows.review_only_calc,
        'can_save', payload_rows.can_save_calc,
        'can_process', payload_rows.can_process_calc,
        'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
        'can_unprocess', payload_rows.can_unprocess_calc,
        'unprocess_block_reason', payload_rows.unprocess_block_reason,
        'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
        'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
        'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence', payload_rows.can_manage_evidence_calc,
        'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
      )
      || jsonb_build_object(
        'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
        'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
        'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
        'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
        'expenses_pay_ex_vat', payload_rows.tsfin_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', payload_rows.tsfin_expenses_charge_ex_vat,
        'mileage_units', payload_rows.tsfin_mileage_units,
        'mileage_pay_ex_vat', payload_rows.tsfin_mileage_pay_ex_vat,
        'mileage_charge_ex_vat', payload_rows.tsfin_mileage_charge_ex_vat,
        'travel_pay_ex_vat', payload_rows.tsfin_travel_pay_ex_vat,
        'travel_charge_ex_vat', payload_rows.tsfin_travel_charge_ex_vat,
        'accommodation_pay_ex_vat', payload_rows.tsfin_accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', payload_rows.tsfin_accommodation_charge_ex_vat,
        'other_pay_ex_vat', payload_rows.tsfin_other_pay_ex_vat,
        'other_charge_ex_vat', payload_rows.tsfin_other_charge_ex_vat,
        'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
        'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
        'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
        'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
        'invoice_segment_stage', payload_rows.invoice_segment_stage,
        'pay_icon_code', payload_rows.pay_icon_code,
        'pay_status_code', payload_rows.pay_status_code,
        'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
        'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
        'validation_status', payload_rows.validation_status::text,
        'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
        'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
        'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
        'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
        'qr_signed_returned', payload_rows.qr_signed_returned_calc
      )
      || jsonb_build_object(
        'is_qr', payload_rows.is_qr_route,
        'qr_status', payload_rows.qr_status::text,
        'qr_generated_at', payload_rows.timesheet_qr_generated_at,
        'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
        'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
        'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
        'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
        'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE),
        'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
        'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
        'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
        'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
        'has_any_evidence', (
          COALESCE(payload_rows.evidence_attached_count, 0) > 0
          OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
          OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
        ),
        'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
        'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
        'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
        'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
        'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
        'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
        'evidence_badges', jsonb_build_array(
          jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
          jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
          jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
          jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
          jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
        )
      )
      || jsonb_build_object(
        'count_deltas', payload_rows.count_deltas_json,
        'cache_invalidation_hints', payload_rows.cache_hints_json,
        'action_flags', jsonb_build_object(
          'can_save', payload_rows.can_save_calc,
          'can_process', payload_rows.can_process_calc,
          'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
          'can_unprocess', payload_rows.can_unprocess_calc,
          'unprocess_block_reason', payload_rows.unprocess_block_reason,
          'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
          'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
          'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', payload_rows.can_manage_evidence_calc,
          'can_add_additional_manual', payload_rows.can_add_additional_manual_calc,
          'review_only', payload_rows.review_only_calc,
          'is_adjustment', payload_rows.effective_is_adjustment_calc,
          'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
          'supportsUnprocessedExpenseDraft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'supports_unprocessed_expense_draft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'expense_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_DRAFT'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END
        ),
        'artifact_hints', jsonb_build_object(
          'route_family', payload_rows.route_family_calc,
          'route_subfamily', payload_rows.route_subfamily_calc,
          'underlying_channel_family', payload_rows.underlying_channel_family_calc,
          'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
          'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
          'has_any_evidence', (COALESCE(payload_rows.evidence_attached_count, 0) > 0 OR COALESCE(payload_rows.evidence_staged_count, 0) > 0 OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL),
          'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
          'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0)
        ),
        'row_patch', (
          jsonb_build_object(
            'previous_row_key', v_previous_row_key,
            'row_key', payload_rows.row_key_calc,
            'new_row_key', payload_rows.row_key_calc,
            'stable_row_id', payload_rows.stable_row_id_calc,
            'timesheet_id', payload_rows.timesheet_id,
            'current_timesheet_id', payload_rows.timesheet_id,
            'requested_timesheet_id', payload_rows.timesheet_id,
            'expected_timesheet_id', payload_rows.timesheet_id,
            'contract_week_id', payload_rows.contract_week_id,
            'row_signature', payload_rows.row_signature_calc,
            'backend_row_signature', payload_rows.backend_row_signature_calc,
            'mutation_row_signature', payload_rows.mutation_row_signature_calc,
            'render_signature', payload_rows.row_signature_calc,
            'previous_row_signature', NULL::text,
            'timesheet_version', payload_rows.timesheet_version,
            'current_version', payload_rows.timesheet_version,
            'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
            'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
            'was_stale', FALSE,
            'candidate_id', payload_rows.candidate_id,
            'candidate_name', payload_rows.candidate_name,
            'candidate_display_name', payload_rows.candidate_name,
            'client_id', payload_rows.client_id,
            'client_name', payload_rows.client_name,
            'client_display_name', payload_rows.client_name,
            'booking_id', payload_rows.booking_id,
            'booking_ref', payload_rows.booking_id,
            'occupant_key_norm', payload_rows.occupant_key_norm,
            'hospital_name', payload_rows.hospital_norm,
            'hospital_norm', payload_rows.hospital_norm,
            'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
            'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
            'period_type', payload_rows.period_type,
            'sheet_scope', payload_rows.sheet_scope,
            'timesheet_scope', payload_rows.sheet_scope,
            'submission_mode', payload_rows.submission_mode,
            'submission_mode_snapshot', payload_rows.submission_mode,
            'basis', payload_rows.basis,
            'route_type', payload_rows.effective_route_type_calc,
            'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
            'route_family', payload_rows.route_family_calc,
            'route_subfamily', payload_rows.route_subfamily_calc
          )
          || jsonb_build_object(
            'underlying_channel_family', payload_rows.underlying_channel_family_calc,
            'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
            'compare_block_required', payload_rows.compare_block_required_calc,
            'is_adjustment', payload_rows.effective_is_adjustment_calc,
            'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
            'actual_schedule_json', payload_rows.actual_schedule_json_calc,
            'planned_schedule_json', payload_rows.planned_schedule_json_calc,
            'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
            'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'processing_status', payload_rows.processing_status::text,
            'processing_status_display', payload_rows.processing_status_display,
            'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
            'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
            'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
            'previous_bulk_process_bucket', NULL::text,
            'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
            'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
            'previous_bulk_authorise_section', NULL::text,
            'is_authorised', payload_rows.is_authorised_calc,
            'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
            'authorised_at_server', payload_rows.authorised_at_server,
            'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
            'requires_authorisation', payload_rows.requires_authorisation_calc,
            'locked', payload_rows.locked_calc,
            'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
            'paid_at_utc', payload_rows.paid_at_utc,
            'review_only', payload_rows.review_only_calc,
            'can_save', payload_rows.can_save_calc,
            'can_process', payload_rows.can_process_calc,
            'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
            'can_unprocess', payload_rows.can_unprocess_calc,
            'unprocess_block_reason', payload_rows.unprocess_block_reason,
            'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
            'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
            'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
            'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
            'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
            'can_manage_evidence', payload_rows.can_manage_evidence_calc,
            'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
          )
          || jsonb_build_object(
            'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
            'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
            'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
            'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
            'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
            'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
            'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
            'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
            'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
            'invoice_segment_stage', payload_rows.invoice_segment_stage,
            'pay_icon_code', payload_rows.pay_icon_code,
            'pay_status_code', payload_rows.pay_status_code,
            'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
            'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
            'validation_status', payload_rows.validation_status::text,
            'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
            'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
            'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
            'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
            'qr_signed_returned', payload_rows.qr_signed_returned_calc,
            'is_qr', payload_rows.is_qr_route,
            'qr_status', payload_rows.qr_status::text,
            'qr_generated_at', payload_rows.timesheet_qr_generated_at,
            'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
            'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
            'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
            'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
            'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE)
          )
          || jsonb_build_object(
            'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
            'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
            'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
            'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
            'has_any_evidence', (
              COALESCE(payload_rows.evidence_attached_count, 0) > 0
              OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
              OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
            ),
            'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
            'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
            'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
            'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
            'previous_primary_artifact_storage_key', NULL::text,
            'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
            'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
            'evidence_badges', jsonb_build_array(
              jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
              jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
              jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
              jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
              jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
            ),
            'count_deltas', payload_rows.count_deltas_json,
            'cache_invalidation_hints', payload_rows.cache_hints_json
          )
        )
      )
    ) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;

-- Restore bulk_authorise_dataset_v1 from backend Git HEAD before this implementation.
CREATE OR REPLACE FUNCTION public.bulk_authorise_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_classification text := NULL;
  v_show_daily boolean := TRUE;
  v_show_weekly boolean := TRUE;
  v_show_manual boolean := TRUE;
  v_show_qr boolean := TRUE;
  v_show_electronic boolean := TRUE;
  v_validation_already boolean := TRUE;
  v_validation_awaiting boolean := TRUE;
  v_show_authorised_invoiced_unissued boolean := FALSE;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_classification := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'classification', v_filters->>'classificationRaw', '')), ''));
  IF v_classification NOT IN ('TIMESHEETS', 'NHSP', 'HR') THEN
    v_classification := NULL;
  END IF;

  v_show_daily := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_weekly := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_manual', v_filters->>'showManual', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_qr := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_qr', v_filters->>'showQr', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_electronic := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_electronic', v_filters->>'showElectronic', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_already := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_already', v_filters->>'validationAlready', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_awaiting := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_awaiting', v_filters->>'validationAwaiting', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_authorised_invoiced_unissued := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_authorised_invoiced_unissued', v_filters->>'showAuthorisedInvoicedUnissued', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT summary_row.*
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.sheet_scope, lightweight_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN lightweight_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(lightweight_rows.processing_status, lightweight_rows.tools_stage, lightweight_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(lightweight_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
        OR EXISTS (
          SELECT 1
          FROM public.invoice_lines AS issued_invoice_line
          JOIN public.invoices AS issued_invoice
            ON issued_invoice.id = issued_invoice_line.invoice_id
          WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
            AND (
              issued_invoice.issued_at_utc IS NOT NULL
              OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
            )
        )
      ) AS locked_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS draft_invoice_line
        JOIN public.invoices AS draft_invoice
          ON draft_invoice.id = draft_invoice_line.invoice_id
        WHERE draft_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND draft_invoice.issued_at_utc IS NULL
          AND UPPER(COALESCE(draft_invoice.status::text, '')) = 'DRAFT'
      ) AS has_unissued_invoice_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS issued_invoice_line
        JOIN public.invoices AS issued_invoice
          ON issued_invoice.id = issued_invoice_line.invoice_id
        WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND (
            issued_invoice.issued_at_utc IS NOT NULL
            OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
          )
      ) AS has_issued_invoice_calc,
      COALESCE(lightweight_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(lightweight_rows.qr_status, '')) = 'PENDING'
        AND lightweight_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(lightweight_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(lightweight_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HR'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(lightweight_rows.timesheet_id::text, ''),
        COALESCE(lightweight_rows.contract_week_id::text, ''),
        COALESCE(lightweight_rows.processing_status, ''),
        COALESCE(lightweight_rows.summary_stage, ''),
        COALESCE(lightweight_rows.tools_stage, ''),
        COALESCE(lightweight_rows.authorised_at_utc::text, ''),
        COALESCE(lightweight_rows.authorised_at_server::text, ''),
        COALESCE(lightweight_rows.processed_at_utc::text, ''),
        COALESCE(lightweight_rows.paid_at_utc::text, ''),
        COALESCE(lightweight_rows.invoice_segments_locked::text, ''),
        COALESCE(lightweight_rows.route_family, ''),
        COALESCE(lightweight_rows.route_subfamily, ''),
        COALESCE(lightweight_rows.validation_status, ''),
        COALESCE(lightweight_rows.attached_evidence_count::text, ''),
        COALESCE(lightweight_rows.primary_artifact_storage_key, '')
      )) AS row_signature_calc
    FROM lightweight_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.qr_unsigned_blocked_calc = FALSE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_unauthorise_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR (classified_rows.contract_week_id IS NOT NULL AND classified_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND COALESCE(classified_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        classified_rows.locked_calc = TRUE
        OR classified_rows.authorised_calc = TRUE
        OR classified_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc
    FROM classified_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status,
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section',
        CASE
          WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible'
          WHEN decision_rows.timesheet_id IS NOT NULL
            AND decision_rows.authorised_calc = TRUE
            AND decision_rows.locked_calc = FALSE
            AND decision_rows.has_issued_invoice_calc = FALSE
            AND (decision_rows.has_unissued_invoice_calc = FALSE OR v_show_authorised_invoiced_unissued = TRUE)
            THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        CASE
          WHEN decision_rows.has_issued_invoice_calc THEN 'ISSUED'
          WHEN decision_rows.has_unissued_invoice_calc THEN 'DRAFT'
          ELSE NULL::text
        END,
        'has_unissued_invoice',
        decision_rows.has_unissued_invoice_calc,
        'has_issued_invoice',
        decision_rows.has_issued_invoice_calc,
        'is_invoiced',
        decision_rows.has_unissued_invoice_calc OR decision_rows.has_issued_invoice_calc,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        decision_rows.is_adjusted,
        'is_adjustment',
        decision_rows.is_adjusted,
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        decision_rows.has_any_evidence,
        'attached_evidence_count',
        decision_rows.attached_evidence_count,
        'evidence_count',
        decision_rows.attached_evidence_count,
        'primary_artifact_id',
        NULL::text,
        'primary_artifact_kind',
        CASE WHEN decision_rows.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET' ELSE NULL::text END,
        'primary_artifact_storage_key',
        decision_rows.primary_artifact_storage_key,
        'primary_artifact_display_name',
        decision_rows.primary_artifact_display_name,
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        decision_rows.row_signature_calc
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc)
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        JSONB_BUILD_ARRAY(
          JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(decision_rows.has_any_evidence, FALSE), 'has_evidence', COALESCE(decision_rows.has_any_evidence, FALSE)),
          JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', decision_rows.has_any_evidence,
          'attached_evidence_count', decision_rows.attached_evidence_count,
          'primary_artifact_storage_key', decision_rows.primary_artifact_storage_key,
          'primary_artifact_display_name', decision_rows.primary_artifact_display_name,
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  canonical_authorise_signature_rows AS MATERIALIZED (
    SELECT canonical_patch.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      JSONB_BUILD_OBJECT(
        'dataset_mode', 'authorise',
        'projection', 'dataset_row',
        'row_keys', COALESCE((
          SELECT JSONB_AGG(payload_rows.row_json->>'row_key' ORDER BY payload_rows.row_json->>'row_key')
          FROM payload_rows
          WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'row_key', '')), '') IS NOT NULL
        ), '[]'::jsonb)
      )
    ) AS canonical_patch(row_json)
  ),
  canonical_payload_rows AS MATERIALIZED (
    SELECT
      (
        payload_rows.row_json
        || jsonb_strip_nulls(jsonb_build_object(
          'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'mutation_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'mutation_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'summary_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'summary_stage', '')), ''),
          'tools_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', '')), ''),
          'processing_status', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'processing_status', '')), '')
        ))
        || jsonb_build_object(
          'has_retained_financial_history', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'can_unprocess', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_action_visible', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
          'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
          'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'can_archive', FALSE,
          'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
        )
        || jsonb_build_object(
          'action_flags',
            COALESCE(payload_rows.row_json->'action_flags', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'action_flags', '{}'::jsonb)
            || jsonb_build_object(
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
              'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'can_archive', FALSE,
              'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
            ),
          'row_patch',
            COALESCE(payload_rows.row_json->'row_patch', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'row_patch', '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), '')
            ))
        )
      ) AS row_json
    FROM payload_rows
    LEFT JOIN canonical_authorise_signature_rows
      ON canonical_authorise_signature_rows.row_json->>'row_key' = payload_rows.row_json->>'row_key'
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT canonical_payload_rows.row_json
    FROM canonical_payload_rows
    WHERE NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(canonical_payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS MATERIALIZED (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS MATERIALIZED (
    SELECT classification_filtered_rows.row_json
    FROM classification_filtered_rows
    WHERE (
        UPPER(COALESCE(classification_filtered_rows.row_json->>'bulk_authorise_classification', '')) <> 'TIMESHEETS'
        OR (
          CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'DAILY' THEN v_show_daily
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' THEN v_show_weekly
            ELSE TRUE
          END
          AND CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'MANUAL_NON_QR' THEN v_show_manual
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'QR' THEN v_show_qr
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'ELECTRONIC' THEN v_show_electronic
            ELSE v_show_manual AND v_show_qr AND v_show_electronic
          END
          AND CASE
            WHEN COALESCE((classification_filtered_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN v_validation_awaiting
            ELSE v_validation_already
          END
        )
      )
  ),
  paged_visible_rows AS MATERIALIZED (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  visible_counts AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible')::integer AS processed_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible')::integer AS authorised_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR')::integer AS manual_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR')::integer AS qr_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC')::integer AS electronic_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE)::integer AS already_validated_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE)::integer AS awaiting_validation_count
    FROM visible_rows
  ),
  eligible_counts AS (
    SELECT
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS')::integer AS timesheets_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP')::integer AS nhsp_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR')::integer AS hr_count
    FROM eligible_rows_before_classification
  ),
  rows_payload AS (
    SELECT COALESCE(
      JSONB_AGG(
        paged_visible_rows.row_json
        ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key'
      ),
      '[]'::jsonb
    ) AS rows_json
    FROM paged_visible_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', COALESCE(visible_counts.total_count, 0),
      'processed_eligible', COALESCE(visible_counts.processed_eligible_count, 0),
      'authorised_eligible', COALESCE(visible_counts.authorised_eligible_count, 0),
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', COALESCE(eligible_counts.timesheets_count, 0),
        'NHSP', COALESCE(eligible_counts.nhsp_count, 0),
        'HR', COALESCE(eligible_counts.hr_count, 0)
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', COALESCE(visible_counts.manual_count, 0),
        'qr', COALESCE(visible_counts.qr_count, 0),
        'electronic', COALESCE(visible_counts.electronic_count, 0)
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', COALESCE(visible_counts.already_validated_count, 0),
        'awaiting_validation', COALESCE(visible_counts.awaiting_validation_count, 0),
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE(rows_payload.rows_json, '[]'::jsonb)
  )
  INTO v_out
  FROM visible_counts
  CROSS JOIN eligible_counts
  CROSS JOIN rows_payload;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', 0,
      'processed_eligible', 0,
      'authorised_eligible', 0,
      'by_classification', JSONB_BUILD_OBJECT('TIMESHEETS', 0, 'NHSP', 0, 'HR', 0),
      'timesheets_by_type', JSONB_BUILD_OBJECT('manual', 0, 'qr', 0, 'electronic', 0),
      'validation', JSONB_BUILD_OBJECT('already_validated', 0, 'awaiting_validation', 0)
    ),
    'rows', '[]'::jsonb
  ));
END;
$function$;

-- Restore invoice_correction_pair_scope_v1 from backend Git HEAD before this implementation.
create or replace function public.invoice_correction_pair_scope_v1(
  p_timesheet_id uuid,
  p_target_invoice_id uuid default null,
  p_actor_user_id uuid default null,
  p_lock_rows boolean default true,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_ready_count integer:=0;
  v_line_member_count integer:=0;
  v_line_invoice_count integer:=0;
  v_client_count integer:=0;
  v_contract_count integer:=0;
  v_week_count integer:=0;
  v_stream_count integer:=0;
  v_target public.invoices%rowtype;
  v_rows jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  r record;
  v_leg jsonb;
  v_policy_ready boolean;
  v_expected_stream text;
  v_current_stream text;
  v_target_stream text;
  v_line_policy_mismatch_count integer:=0;
begin
  if p_timesheet_id is null then raise exception 'INVOICE_CORRECTION_TIMESHEET_ID_REQUIRED' using errcode='22023'; end if;
  if p_max_members<1 or p_max_members>100 then raise exception 'INVOICE_CORRECTION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_actor_user_id is not null then
    perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
    if not found then raise exception 'INVOICE_CORRECTION_ACTOR_INVALID' using errcode='42501'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
  end if;
  v_unit:=v_chain->'requested_correction_unit';
  if jsonb_typeof(v_unit)<>'object' or coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_UNIT_INVALID' using errcode='P0001';
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_stream:=upper(btrim(coalesce(v_envelope->>'invoice_stream','')));
  if v_expected_stream not in ('NORMAL','SELF_BILL') then
    raise exception 'INVOICE_CORRECTION_FROZEN_STREAM_INVALID' using errcode='P0001';
  end if;
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then raise exception 'INVOICE_CORRECTION_MEMBER_COUNT_MISMATCH' using errcode='P0001'; end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids) order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.timesheet_id=any(v_ids) and tf.is_current=true
      order by tf.timesheet_id,tf.id for update;
  end if;

  if p_target_invoice_id is not null then
    if p_lock_rows then select * into v_target from public.invoices where id=p_target_invoice_id for update;
    else select * into v_target from public.invoices where id=p_target_invoice_id; end if;
    if not found then raise exception 'INVOICE_CORRECTION_TARGET_NOT_FOUND' using errcode='P0002'; end if;
    if upper(coalesce(v_target.status::text,''))<>'DRAFT' or v_target.issued_at_utc is not null then
      raise exception 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' using errcode='P0001',
        detail=jsonb_build_object('invoice_id',p_target_invoice_id,'status',v_target.status,'issued_at_utc',v_target.issued_at_utc)::text;
    end if;
    v_target_stream:=case
      when lower(coalesce(v_target.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
        then 'SELF_BILL' else 'NORMAL' end;
    if v_target_stream is distinct from v_expected_stream then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
        'code','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
        'expected_stream',v_expected_stream,
        'target_stream',v_target_stream
      ));
    end if;
  end if;

  for r in
    select ts.timesheet_id,ts.correction_kind,ts.contract_id,ts.week_ending_date,
      tf.id tsfin_id,tf.client_id,tf.basis,tf.processing_status,tf.is_stale,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      c.self_bill
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_current_stream:=case
      when upper(coalesce(r.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end;
    v_policy_ready:=
      coalesce(r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
               r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
               r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
        is not distinct from v_envelope->>'envelope_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_leg_fingerprint',r.rate_source_refs_json->>'correction_leg_fingerprint')
        is not distinct from v_leg->>'leg_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',r.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint',r.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
      and r.policy_snapshot_json->'correction_invoice_policy'
        is not distinct from v_leg->'invoice_policy'
      and upper(btrim(coalesce(r.policy_snapshot_json->>'correction_invoice_stream','')))
        is not distinct from v_expected_stream
      and upper(btrim(coalesce(v_leg#>>'{invoice_policy,invoice_stream}','')))
        is not distinct from v_expected_stream
      and v_current_stream is not distinct from v_expected_stream;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
       and v_policy_ready then v_ready_count:=v_ready_count+1; end if;
    if not v_policy_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id)); end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,'tsfin_id',r.tsfin_id,
      'client_id',r.client_id,'contract_id',r.contract_id,'week_ending_date',r.week_ending_date,
       'invoice_stream',v_expected_stream,'current_contract_stream',v_current_stream,
       'processing_status',r.processing_status,'policy_ready',v_policy_ready,
       'invoice_vat_chargeable',v_leg#>'{invoice_policy,invoice_vat_chargeable}',
       'invoice_vat_rate_pct',v_leg#>'{invoice_policy,applied_vat_rate_pct}',
       'invoice_policy_fingerprint',v_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
       'leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  select count(distinct tf.client_id),count(distinct ts.contract_id),count(distinct ts.week_ending_date),
    count(distinct case
      when upper(coalesce(tf.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end)
  into v_client_count,v_contract_count,v_week_count,v_stream_count
  from public.timesheets ts
  join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_ids);

  select count(distinct il.timesheet_id),count(distinct il.invoice_id)
  into v_line_member_count,v_line_invoice_count
  from public.invoice_lines il where il.timesheet_id=any(v_ids);

  select count(*)::integer into v_line_policy_mismatch_count
  from public.invoice_lines il
  cross join lateral (
    select public._ctms_correction_policy_leg_read_v1(il.timesheet_id) leg
  ) expected
  where il.timesheet_id=any(v_ids)
    and (p_target_invoice_id is null or il.invoice_id=p_target_invoice_id)
    and il.vat_rate_pct is distinct from
      (expected.leg#>>'{invoice_policy,applied_vat_rate_pct}')::numeric;

  if v_ready_count<>v_expected_count then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TSFIN_NOT_READY','ready_count',v_ready_count)); end if;
  if v_client_count<>1 or v_contract_count<>1 or v_week_count<>1 or v_stream_count<>1 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_SCOPE_MIXED'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    join public.timesheets_financials tf
      on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
      and (case
        when upper(coalesce(tf.basis::text,'')) in (
          'NHSP','NHSP_ADJUSTMENT',
          'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
        ) then 'SELF_BILL'
        else 'NORMAL'
      end)
        is distinct from v_expected_stream
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_FROZEN_STREAM_DRIFT',
      'expected_stream',v_expected_stream
    ));
  end if;
  if v_line_member_count not in (0,v_expected_count) or (v_line_member_count=v_expected_count and v_line_invoice_count<>1) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_UNIT_SPLIT'));
  end if;
  if v_line_policy_mismatch_count<>0 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_LINE_VAT_POLICY_MISMATCH',
      'mismatching_line_count',v_line_policy_mismatch_count
    ));
  end if;
  if p_target_invoice_id is not null and v_target.client_id is distinct from (select tf.client_id from public.timesheets_financials tf where tf.timesheet_id=v_ids[1] and tf.is_current=true) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH'));
  end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'target_invoice_id',p_target_invoice_id,'target_appendable',p_target_invoice_id is null or jsonb_array_length(v_errors)=0,
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'invoice_stream',v_expected_stream,
    'pair_rows',v_rows,'ready_count',v_ready_count,'existing_line_member_count',v_line_member_count,
    'existing_line_invoice_count',v_line_invoice_count,
    'line_policy_mismatch_count',v_line_policy_mismatch_count,'errors',v_errors);
end;
$function$;

-- CREATE OR REPLACE preserves existing ownership and ACLs. Reassert the exact
-- pre-change owner and role grants so rollback verification is deterministic.
ALTER FUNCTION public._import_review_action_catalog_core_v1(uuid,integer,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._import_review_action_catalog_core_v1(uuid,integer,integer) FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public._import_review_apply_complete_core_v1(uuid,uuid,uuid,jsonb,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._import_review_apply_complete_core_v1(uuid,uuid,uuid,jsonb,boolean) FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public._import_review_apply_envelope_core_v1(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._import_review_apply_envelope_core_v1(uuid) FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public.import_review_apply_guard_v1(uuid,bigint,text,text,uuid,text,jsonb,jsonb,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.import_review_apply_guard_v1(uuid,bigint,text,text,uuid,text,jsonb,jsonb,uuid) FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb,uuid,timestamptz) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb,uuid,timestamptz) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.timesheet_unauthorise_bulk_atomic(jsonb,uuid,timestamptz) TO authenticated, service_role;

ALTER FUNCTION public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz) TO authenticated, service_role;

ALTER FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) TO authenticated, service_role;

ALTER FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) TO authenticated, service_role;

ALTER FUNCTION public.hr_weekly_apply_transactional(uuid,jsonb,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_apply_transactional(uuid,jsonb,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.hr_weekly_apply_transactional(uuid,jsonb,uuid) TO service_role;

ALTER FUNCTION public.nhsp_weekly_apply_transactional(uuid,jsonb,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.nhsp_weekly_apply_transactional(uuid,jsonb,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.nhsp_weekly_apply_transactional(uuid,jsonb,uuid) TO service_role;

ALTER FUNCTION public.import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text) TO service_role;

ALTER FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO authenticated, service_role;

ALTER FUNCTION public.bulk_authorise_dataset_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_authorise_dataset_v1(jsonb) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.bulk_authorise_dataset_v1(jsonb) TO authenticated, service_role;

ALTER FUNCTION public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) TO service_role;

DROP FUNCTION IF EXISTS public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz);
DROP FUNCTION IF EXISTS public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer);
DROP INDEX IF EXISTS public.idx_audit_import_correction_source_v1;

COMMIT;
