create or replace function public.nhsp_apply_import_phase1(
  p_import_id uuid,
  p_selected_group_ids text[] default null,
  p_skip_external_row_keys text[] default null,
  p_force_overwrite_external_row_keys text[] default null
)
returns jsonb
language plpgsql
as $$
declare
  v_created           int := 0;
  v_updated           int := 0;
  v_mapped_candidates int := 0;
  v_mapped_clients    int := 0;

  -- selection normalisation + mode
  v_force_mode boolean := false;
  v_skip_keys text[] := array[]::text[];
  v_force_keys text[] := array[]::text[];

  -- pre-write diagnostics
  v_raw_rows_count int := 0;
  v_src_rows_count int := 0;
  v_resolved_rows_count int := 0;

  v_sample_skip_keys jsonb := '[]'::jsonb;
  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_src_keys jsonb := '[]'::jsonb;
  v_sample_written jsonb := '[]'::jsonb;
begin
  ----------------------------------------------------------------
  -- 0) Sanity check the import exists and is NHSP
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = p_import_id
    and hi.source_system = 'NHSP'::public.hr_source_enum;

  if not found then
    raise exception 'nhsp_apply_import_phase1: import % not found or not NHSP', p_import_id;
  end if;

  ----------------------------------------------------------------
  -- 0.5) Normalise key arrays (distinct, trimmed, non-empty)
  -- Selection rule:
  --  - If p_force_overwrite_external_row_keys IS PROVIDED (even empty) => "FORCE MODE":
  --      only keys in FORCE list are eligible; SKIP list is still enforced as an exclusion.
  --  - Else (force is NULL) => legacy "SKIP MODE":
  --      all rows except SKIP list are eligible.
  ----------------------------------------------------------------
  v_force_mode := (p_force_overwrite_external_row_keys is not null);

  select coalesce(array_agg(distinct btrim(k)),'{}'::text[])
  into v_skip_keys
  from unnest(coalesce(p_skip_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  select coalesce(array_agg(distinct btrim(k)),'{}'::text[])
  into v_force_keys
  from unnest(coalesce(p_force_overwrite_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_skip_keys
  from (
    select k as k
    from unnest(coalesce(v_skip_keys, array[]::text[])) as k
    order by k
    limit 20
  ) as x;

  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_force_keys
  from (
    select k as k
    from unnest(coalesce(v_force_keys, array[]::text[])) as k
    order by k
    limit 20
  ) as x;

  ----------------------------------------------------------------
  -- 1) Prepare / resolve all relevant hr_rows
  -- IMPORTANT: We only act on rows with a non-null external_row_key.
  -- This prevents accidental mass updates via "skip list" semantics.
  ----------------------------------------------------------------
  create temporary table tmp_phase1_raw on commit drop as
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
      nullif((r.payload_json ->> 'assignment'), ''),
      nullif((r.payload_json ->> 'Request_Grade'), ''),
      nullif(r.assignment_grade_norm, '')
    )                              as assignment_code
  from public.hr_rows r
  where r.import_id = p_import_id
    and r.external_row_key is not null
    and r.date_local is not null
    and (r.payload_json ->> 'start_utc') is not null
    and (r.payload_json ->> 'end_utc')   is not null;

  select count(*)::int
  into v_raw_rows_count
  from tmp_phase1_raw tr;

  create temporary table tmp_phase1_src on commit drop as
  select tr.*
  from tmp_phase1_raw tr
  where
    -- Always enforce SKIP as an exclusion (if present)
    (
      array_length(v_skip_keys, 1) is null
      or tr.external_row_key <> all(v_skip_keys)
    )
    and
    (
      -- FORCE MODE: only keys in FORCE list (empty list => select none)
      (v_force_mode is true and array_length(v_force_keys, 1) is not null and tr.external_row_key = any(v_force_keys))
      or
      -- LEGACY SKIP MODE: include everything not skipped
      (v_force_mode is false)
    );

  select count(*)::int
  into v_src_rows_count
  from tmp_phase1_src ts;

  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_src_keys
  from (
    select ts.external_row_key as k
    from tmp_phase1_src ts
    order by ts.external_row_key
    limit 20
  ) as x;

  create temporary table tmp_phase1_resolved on commit drop as
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

    coalesce(
      cand_alias.id,
      cand_map.candidate_id,
      cand_exact_unique.candidate_id
    ) as candidate_id,

    coalesce(
      cli_alias.client_id,
      cli_name.client_id
    ) as client_id
  from tmp_phase1_src src

  cross join lateral (
    select
      nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
      nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,
      nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
      nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_norm
  ) n

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
      case
        when count(*) = 1
          then (array_agg(candidate_id order by candidate_id::text))[1]
      end as candidate_id
    from matches
  ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)

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

  left join lateral (
    with matches as (
      select cl.id as client_id
      from public.clients cl
      where n.trust_norm is not null
        and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_norm
    )
    select
      case
        when count(*) = 1
          then (array_agg(client_id order by client_id::text))[1]
      end as client_id
    from matches
  ) cli_name on (cli_alias.client_id is null);

  select count(*)::int
  into v_resolved_rows_count
  from tmp_phase1_resolved rr;

  ----------------------------------------------------------------
  -- 2) Current TSFIN lock state (for safe overwrite logic)
  ----------------------------------------------------------------
  create temporary table tmp_phase1_fin_current on commit drop as
  select distinct on (tf.timesheet_id)
    tf.timesheet_id,
    tf.locked_by_invoice_id,
    tf.paid_at_utc
  from public.timesheets_financials tf
  where tf.is_current = true
  order by tf.timesheet_id, tf.created_at desc;

  ----------------------------------------------------------------
  -- 3) Apply inserts/updates and capture samples
  ----------------------------------------------------------------
  create temporary table tmp_phase1_written(
    op text not null,
    shift_id uuid not null,
    external_row_key text not null
  ) on commit drop;

  with ins as (
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
      rr.external_row_key,
      p_import_id,
      'NHSP'::public.hr_source_enum,
      nullif(rr.staff_name, ''),
      nullif(lower(rr.staff_name), ''),
      nullif(rr.ward, ''),
      nullif(lower(rr.ward), ''),
      rr.work_date,
      nullif(rr.assignment_code, ''),
      nullif(rr.ref_num, ''),
      rr.start_utc,
      rr.end_utc,
      coalesce(rr.break_mins, 0),
      greatest(0, rr.pay_minutes),
      'PENDING',
      now(),
      now(),
      rr.candidate_id,
      rr.client_id
    from tmp_phase1_resolved rr
    where rr.external_row_key is not null
      and not exists (
        select 1
        from public.nhsp_shifts s2
        where s2.external_row_key = rr.external_row_key
          and s2.source_system = 'NHSP'::public.hr_source_enum
      )
    returning
      public.nhsp_shifts.id as shift_id,
      public.nhsp_shifts.external_row_key as external_row_key,
      (public.nhsp_shifts.candidate_id is not null) as mapped_candidate,
      (public.nhsp_shifts.client_id    is not null) as mapped_client
  ),
  upd_src as (
    select
      s.id as shift_id,
      s.external_row_key,
      s.timesheet_id,
      s.candidate_id as old_candidate_id,
      s.client_id    as old_client_id,

      rr.work_date,
      rr.staff_name,
      rr.ward,
      rr.assignment_code,
      rr.ref_num,
      rr.start_utc,
      rr.end_utc,
      rr.break_mins,
      rr.pay_minutes,

      rr.candidate_id as new_candidate_id,
      rr.client_id    as new_client_id,

      fc.locked_by_invoice_id,
      fc.paid_at_utc,

      (
        s.timesheet_id is null
        or fc.timesheet_id is null
        or (fc.locked_by_invoice_id is null and fc.paid_at_utc is null)
      ) as safe_to_overwrite,

      (
        v_force_mode is true
        and array_length(v_force_keys, 1) is not null
        and s.external_row_key = any(v_force_keys)
      ) as force_overwrite,

      (
        (
          s.timesheet_id is null
          or fc.timesheet_id is null
          or (fc.locked_by_invoice_id is null and fc.paid_at_utc is null)
        )
        or (
          v_force_mode is true
          and array_length(v_force_keys, 1) is not null
          and s.external_row_key = any(v_force_keys)
        )
      ) as should_overwrite_time

    from public.nhsp_shifts s
    join tmp_phase1_resolved rr
      on rr.external_row_key = s.external_row_key
    left join tmp_phase1_fin_current fc
      on fc.timesheet_id = s.timesheet_id
    where s.source_system = 'NHSP'::public.hr_source_enum
  ),
  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = p_import_id,

      staff_name       = nullif(us.staff_name, ''),
      staff_norm       = nullif(lower(us.staff_name), ''),
      ward             = nullif(us.ward, ''),
      ward_norm        = nullif(lower(us.ward), ''),
      work_date        = us.work_date,
      assignment_code  = nullif(us.assignment_code, ''),
      ref_num          = nullif(us.ref_num, ''),
      source_system    = 'NHSP'::public.hr_source_enum,
      updated_at       = now(),

      cancelled_at_utc = null,
      cancelled_by_import_id = null,
      cancelled_reason = null,

      start_utc        = case when us.should_overwrite_time then us.start_utc else s.start_utc end,
      end_utc          = case when us.should_overwrite_time then us.end_utc   else s.end_utc   end,
      break_mins       = case when us.should_overwrite_time then coalesce(us.break_mins, 0) else s.break_mins end,
      pay_minutes      = case when us.should_overwrite_time then greatest(0, us.pay_minutes) else s.pay_minutes end,

      candidate_id     = case
                           when us.new_candidate_id is not null and us.safe_to_overwrite
                             then us.new_candidate_id
                           else s.candidate_id
                         end,
      client_id        = case
                           when us.new_client_id is not null and us.safe_to_overwrite
                             then us.new_client_id
                           else s.client_id
                         end
    from upd_src us
    where s.id = us.shift_id
    returning
      s.id as shift_id,
      s.external_row_key as external_row_key,
      (us.old_candidate_id is null and us.new_candidate_id is not null and us.safe_to_overwrite) as mapped_candidate,
      (us.old_client_id    is null and us.new_client_id    is not null and us.safe_to_overwrite) as mapped_client
  )
  insert into tmp_phase1_written(op, shift_id, external_row_key)
  select 'INSERT', i.shift_id, i.external_row_key from ins i
  union all
  select 'UPDATE', u.shift_id, u.external_row_key from upd u;

  select
    coalesce((select count(*) from tmp_phase1_written w where w.op = 'INSERT'), 0) as created,
    coalesce((select count(*) from tmp_phase1_written w where w.op = 'UPDATE'), 0) as updated,
    coalesce((select count(*) from (
      select 1
      from tmp_phase1_written w
      join public.nhsp_shifts ns on ns.id = w.shift_id
      where ns.candidate_id is not null
    ) as x), 0) as mapped_candidates,
    coalesce((select count(*) from (
      select 1
      from tmp_phase1_written w
      join public.nhsp_shifts ns on ns.id = w.shift_id
      where ns.client_id is not null
    ) as y), 0) as mapped_clients
  into
    v_created,
    v_updated,
    v_mapped_candidates,
    v_mapped_clients;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'op', w.op,
        'external_row_key', w.external_row_key,
        'shift_id', w.shift_id::text
      )
      order by w.op, w.external_row_key
    ),
    '[]'::jsonb
  )
  into v_sample_written
  from (
    select w.op, w.shift_id, w.external_row_key
    from tmp_phase1_written w
    order by w.op, w.external_row_key
    limit 20
  ) as w;

  ----------------------------------------------------------------
  -- 4) Debug audit (invoice_debug gated inside _imp_debug_audit)
  -- NOTE: nhsp_apply_import_phase1 has no actor param; use NULL actor.
  ----------------------------------------------------------------
  perform public._imp_debug_audit(
    null,
    'NHSP_PHASE1_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'force_mode', v_force_mode,

      'input_skip_keys_count', coalesce(array_length(v_skip_keys, 1), 0),
      'input_force_keys_count', coalesce(array_length(v_force_keys, 1), 0),
      'sample_skip_keys', v_sample_skip_keys,
      'sample_force_keys', v_sample_force_keys,

      'raw_rows_count', v_raw_rows_count,
      'src_rows_count', v_src_rows_count,
      'resolved_rows_count', v_resolved_rows_count,
      'sample_src_external_row_keys', v_sample_src_keys,

      'shifts_created', v_created,
      'shifts_updated', v_updated,
      'mapped_candidates', v_mapped_candidates,
      'mapped_clients', v_mapped_clients,
      'sample_written', v_sample_written
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id',          p_import_id,
    'shifts_created',     v_created,
    'shifts_updated',     v_updated,
    'mapped_candidates',  v_mapped_candidates,
    'mapped_clients',     v_mapped_clients
  );

exception when others then
  begin
    perform public._imp_debug_audit(
      null,
      'NHSP_PHASE1_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'force_mode', v_force_mode,
        'input_skip_keys_count', coalesce(array_length(v_skip_keys, 1), 0),
        'input_force_keys_count', coalesce(array_length(v_force_keys, 1), 0),
        'raw_rows_count', v_raw_rows_count,
        'src_rows_count', v_src_rows_count,
        'resolved_rows_count', v_resolved_rows_count,
        'error', sqlerrm
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
$$;
