create or replace function public.weekly_import_phase2(
  p_import_id uuid,
  p_system_type text
)
returns table (
  hr_row_id         uuid,
  external_row_key  text,
  work_date         date,
  incoming_code     text,
  candidate_id      uuid,
  client_id         uuid,
  week_ending_date  date,
  contract_id       uuid,
  action            text,
  reason            text
)
language plpgsql
as $$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
begin
  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  return query
  with imp as (
    select
      hi.id,
      hi.source_system,
      hi.client_id as hr_client_id
    from public.hr_imports hi
    where hi.id = p_import_id
    limit 1
  ),
  raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      r.date_local as work_date,

      -- staff_name
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif((r.payload_json ->> 'worker_name'), ''),
        nullif((r.payload_json ->> 'name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      -- ward/unit (for display / debugging only)
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif((r.payload_json ->> 'unit'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      -- trust_raw (NHSP only)
      coalesce(
        nullif((r.payload_json ->> 'trust'), ''),
        nullif((r.payload_json ->> 'hospital_or_trust'), ''),
        nullif(r.unit_raw, '')
      ) as trust_raw,

      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,

      coalesce(
        (r.payload_json ->> 'break_mins')::int,
        (r.payload_json ->> 'break_minutes')::int,
        (r.payload_json ->> 'actual_break_minutes')::int,
        0
      ) as break_mins,

      -- incoming_code depends on system_type:
      case
        when v_sys = 'NHSP' then coalesce(
          nullif((r.payload_json ->> 'assignment_code'), ''),
          nullif((r.payload_json ->> 'assignment'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
        else coalesce(
          nullif((r.payload_json ->> 'grade_raw'), ''),
          nullif((r.payload_json ->> 'Grade'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
      end as incoming_code_raw

    from public.hr_rows r
    join imp
      on imp.id = r.import_id
    where r.import_id = p_import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  resolved_ids as (
    select
      src.*,

      -- candidate mapping: aliases → hr_name_mappings
      coalesce(
        cand_alias.id,
        cand_map.candidate_id
      ) as candidate_id,

      -- client mapping:
      -- NHSP: trust → client_hospitals alias → fallback clients.name
      -- HR_WEEKLY: client_id comes from hr_imports.client_id
      case
        when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
        else imp.hr_client_id
      end as client_id

    from raw src
    join imp on true

    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases @> to_jsonb(array[lower(src.staff_name)]::text[])
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.hr_name_norm = lower(src.staff_name)
        and hm.active = true
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    left join lateral (
      select ch.client_id
      from public.client_hospitals ch
      where ch.hospital_name_norm @> to_jsonb(array[lower(src.trust_raw)]::text[])
      limit 1
    ) cli_alias on (v_sys = 'NHSP')

    left join lateral (
      select cl.id as client_id
      from public.clients cl
      where cl.name = src.trust_raw
      limit 1
    ) cli_name on (v_sys = 'NHSP' and cli_alias.client_id is null)
  ),
  with_we as (
    select
      r.*,

      -- week ending weekday from latest client_settings; default 0 (Sunday)
      coalesce(cs.week_ending_weekday, 0)::int as we_dow,

      -- compute week_ending_date by moving forward to we_dow
      (r.work_date
        + (
            (coalesce(cs.week_ending_weekday, 0)::int - extract(dow from r.work_date)::int + 7) % 7
          )
      )::date as week_ending_date,

      -- normalised incoming_code for mapping joins
      lower(trim(coalesce(r.incoming_code_raw,''))) as code_norm

    from resolved_ids r
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id = r.client_id
      order by cs.effective_from desc, cs.created_at desc
      limit 1
    ) cs on true
  ),
  in_range_counts as (
    select
      w.*,
      coalesce(cr.in_range_count, 0) as in_range_count
    from with_we w
    left join lateral (
      select count(*)::int as in_range_count
      from public.contracts c
      where c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
    ) cr on true
  ),
  chosen_maps as (
    select
      w.*,
      m.spec as map_spec,
      m.patterns as band_patterns
    from in_range_counts w
    left join lateral (
      with maps as (
        select m.band_match_pattern, 2 as spec
        from public.assignment_band_mappings m
        where m.active = true
          and upper(trim(m.system_type)) = v_sys
          and lower(trim(m.incoming_code)) = w.code_norm
          and w.candidate_id is not null
          and m.candidate_id = w.candidate_id

        union all
        select m.band_match_pattern, 1 as spec
        from public.assignment_band_mappings m
        where m.active = true
          and upper(trim(m.system_type)) = v_sys
          and lower(trim(m.incoming_code)) = w.code_norm
          and w.client_id is not null
          and m.candidate_id is null
          and m.client_id = w.client_id

        union all
        select m.band_match_pattern, 0 as spec
        from public.assignment_band_mappings m
        where m.active = true
          and upper(trim(m.system_type)) = v_sys
          and lower(trim(m.incoming_code)) = w.code_norm
          and m.candidate_id is null
          and m.client_id is null
      ),
      mx as (select max(spec) as m from maps)
      select
        (select m from mx) as spec,
        (select array_agg(lower(trim(band_match_pattern)))
         from maps
         where spec = (select m from mx)
        ) as patterns
    ) m on true
  ),
  chosen_contract as (
    select
      w.*,
      cc.contract_id
    from chosen_maps w
    left join lateral (
      select c.id as contract_id
      from public.contracts c
      where c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
        and w.band_patterns is not null
        and exists (
          select 1
          from unnest(w.band_patterns) p
          where position(p in lower(coalesce(c.band,''))) > 0
        )
      order by c.start_date desc nulls last, c.id desc
      limit 1
    ) cc on true
  )
  select
    w.hr_row_id,
    w.external_row_key,
    w.work_date,
    nullif(trim(coalesce(w.incoming_code_raw,'')),'') as incoming_code,
    w.candidate_id,
    w.client_id,
    w.week_ending_date,
    w.contract_id,
    case
      when (select count(*) from imp) = 0 then 'REJECT_IMPORT_NOT_FOUND'
      when v_sys = 'NHSP' and (select source_system from imp) <> 'NHSP'::hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when v_sys = 'HR_WEEKLY' and (select source_system from imp) <> 'HEALTHROSTER'::hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when w.candidate_id is null then 'REJECT_NO_CANDIDATE'
      when w.client_id is null then 'REJECT_NO_CLIENT'
      when w.code_norm = '' then 'REJECT_BAD_ROW'
      when w.in_range_count = 0 then 'REJECT_NO_CONTRACT'
      when w.band_patterns is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      when w.contract_id is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      else 'OK'
    end as action,
    case
      when (select count(*) from imp) = 0 then 'Import not found'
      when v_sys = 'NHSP' and (select source_system from imp) <> 'NHSP'::hr_source_enum
        then 'Import source_system is not NHSP'
      when v_sys = 'HR_WEEKLY' and (select source_system from imp) <> 'HEALTHROSTER'::hr_source_enum
        then 'Import source_system is not HEALTHROSTER'
      when w.candidate_id is null then 'No candidate mapping found for staff name'
      when w.client_id is null then 'No client mapping found'
      when w.code_norm = '' then 'Missing incoming_code (assignment/grade)'
      when w.in_range_count = 0 then 'No active contract for candidate/client on this date'
      when w.band_patterns is null
        then 'No band mapping rows exist for this incoming_code at candidate/client/global scope'
      when w.contract_id is null
        then 'No contract band matches incoming_code according to mapping table'
      else ''
    end as reason
  from chosen_contract w;

end;
$$;
