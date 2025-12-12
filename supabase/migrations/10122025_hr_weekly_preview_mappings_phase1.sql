create or replace function public.hr_weekly_preview_mappings_phase1(
  p_import_id uuid
)
returns table(
  hr_row_id      uuid,
  staff_name     text,
  staff_norm     text,
  ward           text,
  ward_norm      text,
  work_date      date,
  client_id      uuid,
  client_name    text,
  candidate_id   uuid,
  candidate_name text
)
language plpgsql
as $$
begin
  return query
  with src as (
    select
      r.id          as hr_row_id,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      -- Staff name: payload.staff_name, else staff_raw / staff_norm
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      -- Ward: payload.ward, else hints from hr_rows
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward
    from public.hr_rows r
    join public.hr_imports hi
      on hi.id = r.import_id
    where r.import_id = p_import_id
      and hi.source_system = 'HEALTHROSTER'::hr_source_enum
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  normed as (
    select
      s.hr_row_id,
      s.client_id,
      s.work_date,
      s.staff_name,

      -- keep existing behaviour for staff_norm output (lower+trim)
      nullif(lower(trim(coalesce(s.staff_name,''))), '') as staff_norm,

      -- NEW: symbol/space stripped variant for matching
      nullif(regexp_replace(lower(coalesce(s.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,

      s.ward,
      nullif(lower(trim(coalesce(s.ward,''))), '')       as ward_norm
    from src s
  ),
  resolved as (
    select
      n.*,

      -- Candidate mapping precedence:
      --  1) candidates.nhsp_hr_name_aliases contains staff_norm OR staff_norm2
      --  2) fallback to hr_name_mappings.hr_name_norm = staff_norm OR staff_norm2
      --  3) UNIQUE exact candidate match on (first+last) OR (last+first) using staff_norm2
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id,

      coalesce(
        cand_alias.display_name,
        cand_map.display_name,
        cand_exact_unique.display_name
      ) as candidate_name

    from normed n

    -- 1) candidate aliases via nhsp_hr_name_aliases (support legacy + symbol/space stripped)
    left join lateral (
      select c.id, c.display_name
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_norm  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm]::text[]))
          or
          (n.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    -- 2) fallback via hr_name_mappings.hr_name_norm (support legacy + symbol/space stripped)
    left join lateral (
      select hm.candidate_id, c.display_name
      from public.hr_name_mappings hm
      join public.candidates c
        on c.id = hm.candidate_id
      where hm.active = true
        and (
          (n.staff_norm  is not null and hm.hr_name_norm = n.staff_norm)
          or
          (n.staff_norm2 is not null and hm.hr_name_norm = n.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on cand_alias.id is null

    -- 3) UNIQUE exact candidate fallback (first+last OR last+first), symbols/spaces removed
    left join lateral (
      with matches as (
        select
          c.id,
          c.display_name
        from public.candidates c
        where c.active = true
          and n.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(id order by id::text))[1]
        end as candidate_id,
        case
          when count(*) = 1
            then (array_agg(display_name order by id::text))[1]
        end as display_name
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  )
  select
    r.hr_row_id,
    r.staff_name,
    r.staff_norm,
    r.ward,
    r.ward_norm,
    r.work_date,
    r.client_id,
    cli.name      as client_name,
    r.candidate_id,
    r.candidate_name
  from resolved r
  left join public.clients cli
    on cli.id = r.client_id;

end;
$$;
