create or replace function public.nhsp_preview_mappings_phase1(
  p_import_id uuid
)
returns table (
  hr_row_id       uuid,
  staff_name      text,
  work_date       date,
  ward            text,
  trust_raw       text,
  start_utc       timestamptz,
  end_utc         timestamptz,
  break_mins      integer,
  ref_num         text,
  assignment_code text,
  candidate_id    uuid,
  candidate_name  text,
  client_id       uuid,
  client_name     text
)
language sql
as $$
  with raw as (
    select
      r.id                             as hr_row_id,
      r.external_row_key,
      r.date_local                     as work_date,
      r.staff_norm                     as staff_norm,
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif((r.payload_json ->> 'worker_name'), ''),
        nullif((r.payload_json ->> 'name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      )                                as staff_name,
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif((r.payload_json ->> 'unit'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      )                                as ward,
      coalesce(
        nullif((r.payload_json ->> 'trust'), ''),
        nullif((r.payload_json ->> 'hospital_or_trust'), ''),
        nullif(r.unit_raw, '')
      )                                as trust_raw,
      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,
      coalesce(
        (r.payload_json ->> 'break_mins')::int,
        (r.payload_json ->> 'break_minutes')::int,
        (r.payload_json ->> 'actual_break_minutes')::int,
        0
      )                                as break_mins,
      coalesce(
        nullif((r.payload_json ->> 'ref_num'), ''),
        nullif((r.payload_json ->> 'Reference'), ''),
        nullif(r.hr_request_id, '')
      )                                as ref_num,
      coalesce(
        nullif((r.payload_json ->> 'assignment_code'), ''),
        nullif((r.payload_json ->> 'assignment'), ''),          -- ✅ NEW: parser writes payload_json.assignment
        nullif((r.payload_json ->> 'Request_Grade'), ''),
        nullif(r.assignment_grade_norm, '')
      )                                as assignment_code
    from public.hr_rows r
    where r.import_id = p_import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  resolved as (
    select
      src.*,
      greatest(
        0,
        (extract(epoch from (src.end_utc - src.start_utc)) / 60)::int
        - src.break_mins
      ) as pay_minutes,

      -- Candidate mapping:
      --  1) candidates.nhsp_hr_name_aliases contains staff_name (lower)
      --  2) fallback to hr_name_mappings.hr_name_norm
      coalesce(
        cand_alias.id,
        cand_map.candidate_id
      ) as candidate_id,

      -- Client mapping:
      --  1) client_hospitals.hospital_name_norm contains trust_norm
      --  2) fallback to clients.name = trust_raw
      coalesce(
        cli_alias.client_id,
        cli_name.client_id
      ) as client_id
    from raw src
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
    ) cli_alias on true
    left join lateral (
      select cl.id as client_id
      from public.clients cl
      where cl.name = src.trust_raw
      limit 1
    ) cli_name on (cli_alias.client_id is null)
  )
  select
    r.hr_row_id,
    r.staff_name,
    r.work_date,
    r.ward,
    r.trust_raw,
    r.start_utc,
    r.end_utc,
    r.break_mins,
    r.ref_num,
    r.assignment_code,
    r.candidate_id,
    cand.display_name as candidate_name,
    r.client_id,
    cli.name          as client_name
  from resolved r
  left join public.candidates cand on cand.id = r.candidate_id
  left join public.clients    cli  on cli.id  = r.client_id;
$$;
