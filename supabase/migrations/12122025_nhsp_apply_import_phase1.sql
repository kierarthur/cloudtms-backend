-- nhsp_apply_import_phase1: Phase 1 of NHSP apply in Postgres
-- - hr_rows (for given import_id, + optional selected_group_ids)
--   → nhsp_shifts (upsert)
--   → auto candidate_id / client_id mapping
-- Returns: jsonb summary with counts.

create or replace function public.nhsp_apply_import_phase1(
  p_import_id uuid,
  p_selected_group_ids text[] default null
)
returns jsonb
language plpgsql
as $$
declare
  v_created           int := 0;
  v_updated           int := 0;
  v_mapped_candidates int := 0;
  v_mapped_clients    int := 0;
begin
  ----------------------------------------------------------------
  -- 0) Sanity check the import exists and is NHSP
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = p_import_id
    and hi.source_system = 'NHSP'::hr_source_enum;

  if not found then
    raise exception 'nhsp_apply_import_phase1: import % not found or not NHSP', p_import_id;
  end if;

  ----------------------------------------------------------------
  -- 1) Prepare / resolve all relevant hr_rows
  ----------------------------------------------------------------
  with raw as (
    select
      r.id                           as hr_row_id,
      r.external_row_key,
      r.date_local                   as work_date,
      r.staff_norm                   as staff_norm,
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif((r.payload_json ->> 'worker_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      )                              as staff_name,
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif((r.payload_json ->> 'unit'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      )                              as ward,
      coalesce(
        nullif((r.payload_json ->> 'trust'), ''),
        nullif((r.payload_json ->> 'hospital_or_trust'), ''),
        nullif(r.unit_raw, '')
      )                              as trust_raw,
      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,
      coalesce((r.payload_json ->> 'break_mins')::int, 0) as break_mins,
      coalesce(
        nullif((r.payload_json ->> 'ref_num'), ''),
        nullif((r.payload_json ->> 'Reference'), ''),
        nullif(r.hr_request_id, '')
      )                              as ref_num,
      coalesce(
        nullif((r.payload_json ->> 'assignment_code'), ''),
        nullif((r.payload_json ->> 'assignment'), ''),          -- ✅ NEW: parser writes payload_json.assignment
        nullif((r.payload_json ->> 'Request_Grade'), ''),
        nullif(r.assignment_grade_norm, '')
      )                              as assignment_code,
      coalesce(
        nullif((r.payload_json ->> 'group_key'), ''),
        nullif((r.payload_json ->> 'group_id'), '')
      )                              as group_key
    from public.hr_rows r
    where r.import_id = p_import_id
      -- Only rows with usable dates/times
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  src as (
    select *
    from raw
    where
      p_selected_group_ids is null
      or array_length(p_selected_group_ids, 1) is null
      or group_key = any(p_selected_group_ids)
  ),
  resolved as (
    select
      src.*,
      -- Compute pay_minutes (for completeness; not heavily used)
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
    from src
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
  ),
  ins as (
    insert into public.nhsp_shifts (
      external_row_key,
      latest_import_id,
      source_system,
      staff_name,
      staff_norm,
      ward,
      ward_norm,
      work_date,
      assignment_code,
      ref_num,
      start_utc,
      end_utc,
      break_mins,
      pay_minutes,
      invoice_status,
      created_at,
      updated_at,
      candidate_id,
      client_id
    )
    select
      r.external_row_key,
      p_import_id,
      'NHSP'::hr_source_enum,
      nullif(r.staff_name, ''),
      nullif(lower(r.staff_name), ''),
      nullif(r.ward, ''),
      nullif(lower(r.ward), ''),
      r.work_date,
      nullif(r.assignment_code, ''),
      nullif(r.ref_num, ''),
      r.start_utc,
      r.end_utc,
      coalesce(r.break_mins, 0),
      greatest(0, r.pay_minutes),
      'PENDING',
      now(),
      now(),
      r.candidate_id,
      r.client_id
    from resolved r
    where r.external_row_key is not null
      and not exists (
        select 1
        from public.nhsp_shifts s
        where s.external_row_key = r.external_row_key
      )
    returning
      (candidate_id is not null) as mapped_candidate,
      (client_id    is not null) as mapped_client
  ),
  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = p_import_id,
      staff_name       = nullif(r.staff_name, ''),
      staff_norm       = nullif(lower(r.staff_name), ''),
      ward             = nullif(r.ward, ''),
      ward_norm        = nullif(lower(r.ward), ''),
      work_date        = r.work_date,
      assignment_code  = nullif(r.assignment_code, ''),
      ref_num          = nullif(r.ref_num, ''),
      start_utc        = r.start_utc,
      end_utc          = r.end_utc,
      break_mins       = coalesce(r.break_mins, 0),
      pay_minutes      = greatest(0, r.pay_minutes),
      source_system    = 'NHSP'::hr_source_enum,
      updated_at       = now(),
      candidate_id     = coalesce(s.candidate_id, r.candidate_id),
      client_id        = coalesce(s.client_id,    r.client_id)
    from resolved r
    where s.external_row_key = r.external_row_key
    returning
      (s.candidate_id is null and r.candidate_id is not null) as mapped_candidate,
      (s.client_id    is null and r.client_id    is not null) as mapped_client
  )
  select
    coalesce((select count(*) from ins), 0)                                   as created,
    coalesce((select count(*) from upd), 0)                                   as updated,
    coalesce((select count(*) from ins where mapped_candidate), 0)
      + coalesce((select count(*) from upd where mapped_candidate), 0)       as mapped_candidates,
    coalesce((select count(*) from ins where mapped_client), 0)
      + coalesce((select count(*) from upd where mapped_client), 0)          as mapped_clients
  into
    v_created,
    v_updated,
    v_mapped_candidates,
    v_mapped_clients;

  return jsonb_build_object(
    'import_id',          p_import_id,
    'shifts_created',     v_created,
    'shifts_updated',     v_updated,
    'mapped_candidates',  v_mapped_candidates,
    'mapped_clients',     v_mapped_clients
  );
end;
$$;
