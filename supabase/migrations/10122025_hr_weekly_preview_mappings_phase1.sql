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
      lower(trim(s.staff_name)) as staff_norm,
      s.ward,
      lower(trim(s.ward))       as ward_norm
    from src s
  ),
  resolved as (
    select
      n.*,
      coalesce(
        cand_alias.id,
        cand_map.candidate_id
      )                  as candidate_id,
      coalesce(
        cand_alias.display_name,
        cand_map.display_name
      )                  as candidate_name
    from normed n
    -- 1) candidate aliases via nhsp_hr_name_aliases
    left join lateral (
      select c.id, c.display_name
      from public.candidates c
      where c.nhsp_hr_name_aliases @> to_jsonb(array[lower(n.staff_name)]::text[])
      limit 1
    ) cand_alias on true
    -- 2) fallback via hr_name_mappings.hr_name_norm
    left join lateral (
      select hm.candidate_id, c.display_name
      from public.hr_name_mappings hm
      join public.candidates c
        on c.id = hm.candidate_id
      where hm.hr_name_norm = lower(n.staff_name)
        and hm.active = true
      order by hm.created_at desc
      limit 1
    ) cand_map on cand_alias.id is null
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
