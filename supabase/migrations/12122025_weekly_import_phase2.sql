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
      n.staff_lc,
      n.staff_key,
      n.trust_lc,
      n.trust_key,

      -- candidate mapping precedence:
      -- 1) candidates.nhsp_hr_name_aliases contains staff_lc OR staff_key
      -- 2) hr_name_mappings.hr_name_norm equals staff_lc OR staff_key
      -- 3) UNIQUE exact candidate match on (first+last) OR (last+first) using staff_key
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id,

      -- client mapping:
      -- NHSP: trust → client_hospitals alias (trust_lc OR trust_key) → UNIQUE fallback clients.name (key)
      -- HR_WEEKLY: client_id comes from hr_imports.client_id
      case
        when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
        else imp.hr_client_id
      end as client_id

    from raw src
    join imp on true

    -- shared normalisations (strip spaces/symbols; keep only [a-z0-9])
    cross join lateral (
      select
        nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_key,
        nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
        nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_key
    ) n

    -- 1) candidate alias match (support legacy + key)
    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_lc  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
          or
          (n.staff_key is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_key]::text[]))
        )
      limit 1
    ) cand_alias on true

    -- 2) hr_name_mappings match (support legacy + key)
    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (n.staff_lc  is not null and hm.hr_name_norm = n.staff_lc)
          or
          (n.staff_key is not null and hm.hr_name_norm = n.staff_key)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    -- 3) UNIQUE exact candidate fallback: key matches first+last OR last+first (NO ambiguous col names, NO max(uuid))
    left join lateral (
      with matches as (
        select c.id as cid
        from public.candidates c
        where c.active = true
          and n.staff_key is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(cid order by cid::text))[1]
        end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)

    -- NHSP client alias match (support legacy + key)
    left join lateral (
      select ch.client_id
      from public.client_hospitals ch
      where v_sys = 'NHSP'
        and ch.hospital_name_norm is not null
        and (
          (n.trust_lc  is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
          or
          (n.trust_key is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_key]::text[]))
        )
      limit 1
    ) cli_alias on (v_sys = 'NHSP')

    -- NHSP UNIQUE fallback: trust_key == clients.name key (NO max(uuid))
    left join lateral (
      with matches as (
        select cl.id as clid
        from public.clients cl
        where v_sys = 'NHSP'
          and n.trust_key is not null
          and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_key
      )
      select
        case
          when count(*) = 1
            then (array_agg(clid order by clid::text))[1]
        end as client_id
      from matches
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
          where position(lower(p) in lower(coalesce(c.band,''))) > 0
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
