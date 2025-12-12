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
        nullif((r.payload_json ->> 'assignment'), ''),          -- ✅ parser writes payload_json.assignment
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
      n.staff_lc,
      n.staff_norm2,
      n.trust_lc,
      n.trust_norm,

      greatest(
        0,
        (extract(epoch from (src.end_utc - src.start_utc)) / 60)::int
        - src.break_mins
      ) as pay_minutes,

      -- Candidate mapping precedence:
      --  1) candidates.nhsp_hr_name_aliases contains staff_lc OR staff_norm2
      --  2) fallback to hr_name_mappings.hr_name_norm = staff_lc OR staff_norm2
      --  3) UNIQUE exact candidate match on (first+last) OR (last+first) using staff_norm2 (spaces/symbols removed)
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id,

      -- Client mapping:
      --  1) client_hospitals.hospital_name_norm contains trust_lc OR trust_norm
      --  2) UNIQUE fallback where norm(clients.name) = trust_norm (spaces/symbols removed)
      coalesce(
        cli_alias.client_id,
        cli_name.client_id
      ) as client_id

    from raw src

    -- shared normalisations:
    -- staff_lc: lower+trim
    -- staff_norm2: remove all non [a-z0-9]
    -- trust_norm: same
    cross join lateral (
      select
        nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,
        nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
        nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_norm
    ) n

    -- 1) candidate alias match (support legacy + normalised)
    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_lc    is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
          or
          (n.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    -- 2) hr_name_mappings match (support legacy + normalised)
    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (n.staff_lc    is not null and hm.hr_name_norm = n.staff_lc)
          or
          (n.staff_norm2 is not null and hm.hr_name_norm = n.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    -- 3) UNIQUE exact candidate fallback (first+last OR last+first), symbols/spaces removed
    left join lateral (
      with matches as (
        select c.id as candidate_id
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
        case when count(*) = 1 then max(candidate_id) end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)

    -- 1) client alias match (support legacy + normalised)
    left join lateral (
      select ch.client_id
      from public.client_hospitals ch
      where ch.hospital_name_norm is not null
        and (
          (n.trust_lc   is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
          or
          (n.trust_norm is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_norm]::text[]))
        )
      limit 1
    ) cli_alias on true

    -- 2) UNIQUE fallback: normalised clients.name == trust_norm
    left join lateral (
      with matches as (
        select cl.id as client_id
        from public.clients cl
        where n.trust_norm is not null
          and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_norm
      )
      select
        case when count(*) = 1 then max(client_id) end as client_id
      from matches
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
