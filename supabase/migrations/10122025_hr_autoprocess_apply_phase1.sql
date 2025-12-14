-- Phase 1 of HealthRoster autoprocess apply:
-- hr_rows (for given import_id) -> nhsp_shifts (source_system = 'HEALTHROSTER')
-- + candidate_id auto-mapping.
-- Returns counts so the Worker can log / display summary.

create or replace function public.hr_autoprocess_apply_phase1(
  import_id uuid,
  selected_group_ids text[] default null  -- currently unused, kept for symmetry/future
)
returns jsonb
language plpgsql
as $$
declare
  v_created           int := 0;
  v_updated           int := 0;
  v_mapped_candidates int := 0;
begin
  ----------------------------------------------------------------
  -- Sanity: import must exist and be HEALTHROSTER
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = hr_autoprocess_apply_phase1.import_id
    and hi.source_system = 'HEALTHROSTER'::hr_source_enum;

  if not found then
    raise exception 'hr_autoprocess_apply_phase1: import % not found or not HEALTHROSTER', import_id;
  end if;

  ----------------------------------------------------------------
  -- src: normalise hr_rows + payload, join to hr_imports for client_id
  ----------------------------------------------------------------
  with src as (
    select
      r.id          as hr_row_id,
      r.external_row_key,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      /* Staff name: payload.staff_name, else staff_raw / staff_norm */
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      /* Ward: payload.ward, else hints from hr_rows */
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,
      coalesce((r.payload_json ->> 'break_mins')::int, 0) as break_mins,

      /* Finalised flags & HR request id */
      coalesce(
        nullif((r.payload_json ->> 'finalized_date'), ''),
        nullif((r.payload_json ->> 'finalised_date'), '')
      ) as finalized_raw,
      nullif((r.payload_json ->> 'request_id'), '') as request_id
    from public.hr_rows r
    join public.hr_imports hi
      on hi.id = r.import_id
    where r.import_id = hr_autoprocess_apply_phase1.import_id
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
      lower(trim(s.staff_name)) as staff_norm,

      s.ward,
      lower(trim(s.ward))       as ward_norm,
      s.start_utc,
      s.end_utc,
      s.break_mins,
      s.request_id,

      case when coalesce(trim(s.finalized_raw), '') = '' then 'NO_FINALISED_DATE'
           else null
      end as held_back_reason,

      -- Compute or reuse external_row_key (same recipe as JS):
      coalesce(
        s.external_row_key,
        case
          when s.work_date is null or s.start_utc is null or s.end_utc is null then null
          else array_to_string(ARRAY[
            regexp_replace(trim(s.work_date::text),               '\|', ' ', 'g'),
            regexp_replace(coalesce(lower(trim(s.staff_name)),''), '\|',' ','g'),
            regexp_replace(coalesce(lower(trim(s.ward)),''),       '\|',' ','g'),
            regexp_replace(coalesce(s.client_id::text,''),         '\|',' ','g'),
            regexp_replace(coalesce(trim(s.request_id),''),        '\|',' ','g')
          ], '|')
        end
      ) as external_row_key
    from src s
  ),

  ----------------------------------------------------------------
  -- Candidate auto-mapping:
  --  1) aliases (legacy lower/trim OR symbol/space stripped)
  --  2) hr_name_mappings (legacy OR stripped)
  --  3) UNIQUE exact match on candidates (first+last OR last+first), stripped
  ----------------------------------------------------------------
  resolved as (
    select
      n.*,

      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id

    from normed n

    -- normalisations used for matching
    cross join lateral (
      select
        nullif(lower(trim(coalesce(n.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(n.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2
    ) nx

    -- 1) candidate aliases via nhsp_hr_name_aliases (support legacy + stripped)
    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (nx.staff_lc    is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_lc]::text[]))
          or
          (nx.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    -- 2) fallback via hr_name_mappings.hr_name_norm (support legacy + stripped)
    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (nx.staff_lc    is not null and hm.hr_name_norm = nx.staff_lc)
          or
          (nx.staff_norm2 is not null and hm.hr_name_norm = nx.staff_norm2)
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
          and nx.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(candidate_id order by candidate_id::text))[1]
        end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  ),

  ----------------------------------------------------------------
  -- Current TSFIN state per timesheet (keyed by timesheet_id)
  ----------------------------------------------------------------
  fin_current as (
    select distinct on (tf.timesheet_id)
      tf.timesheet_id,
      tf.locked_by_invoice_id,
      tf.paid_at_utc
    from public.timesheets_financials tf
    where tf.is_current = true
    order by tf.timesheet_id, tf.created_at desc
  ),

  ----------------------------------------------------------------
  -- Update hr_rows.external_row_key where it was previously null/different
  ----------------------------------------------------------------
  ext_update as (
    update public.hr_rows r
    set external_row_key = res.external_row_key
    from resolved res
    where r.id = res.hr_row_id
      and res.external_row_key is not null
      and r.external_row_key is distinct from res.external_row_key
    returning 1
  ),

  ----------------------------------------------------------------
  -- Insert new HEALTHROSTER shifts
  ----------------------------------------------------------------
  ins as (
    insert into public.nhsp_shifts (
      external_row_key,
      latest_import_id,
      source_system,
      work_date,
      ward,
      start_utc,
      end_utc,
      break_mins,
      pay_minutes,
      client_id,
      hr_request_id,
      held_back_reason,
      candidate_id,
      created_at,
      updated_at
    )
    select
      r.external_row_key,
      hr_autoprocess_apply_phase1.import_id,
      'HEALTHROSTER'::hr_source_enum,
      r.work_date,
      nullif(r.ward, ''),
      r.start_utc,
      r.end_utc,
      coalesce(r.break_mins, 0),
      greatest(
        0,
        (extract(epoch from (r.end_utc - r.start_utc)) / 60)::int
        - coalesce(r.break_mins, 0)
      ) as pay_minutes,
      r.client_id,
      r.request_id,
      r.held_back_reason,
      r.candidate_id,
      now(),
      now()
    from resolved r
    where r.external_row_key is not null
      and not exists (
        select 1
        from public.nhsp_shifts s
        where s.external_row_key = r.external_row_key
      )
    returning
      (candidate_id is not null) as mapped_candidate
  ),

  ----------------------------------------------------------------
  -- Build update source rows + SAFE overwrite decision (no illegal LATERAL ref)
  ----------------------------------------------------------------
  upd_src as (
    select
      s.external_row_key,
      s.timesheet_id,

      s.candidate_id as old_candidate_id,
      s.client_id    as old_client_id,

      r.work_date,
      r.ward,
      r.start_utc,
      r.end_utc,
      r.break_mins,
      r.request_id,
      r.held_back_reason,
      r.client_id     as new_client_id,
      r.candidate_id  as new_candidate_id,

      fc.locked_by_invoice_id,
      fc.paid_at_utc,
      (fc.timesheet_id is null) as tsfin_missing,

      (
        s.timesheet_id is null
        or fc.timesheet_id is null
        or (fc.locked_by_invoice_id is null and fc.paid_at_utc is null)
      ) as safe_to_overwrite
    from public.nhsp_shifts s
    join resolved r
      on r.external_row_key = s.external_row_key
    left join fin_current fc
      on fc.timesheet_id = s.timesheet_id
  ),

  ----------------------------------------------------------------
  -- Update existing shifts with latest HR data (+ SAFE overwrite of ids)
  ----------------------------------------------------------------
  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = hr_autoprocess_apply_phase1.import_id,
      source_system    = 'HEALTHROSTER'::hr_source_enum,
      work_date        = u.work_date,
      ward             = nullif(u.ward, ''),
      start_utc        = u.start_utc,
      end_utc          = u.end_utc,
      break_mins       = coalesce(u.break_mins, 0),
      pay_minutes      = greatest(
                           0,
                           (extract(epoch from (u.end_utc - u.start_utc)) / 60)::int
                           - coalesce(u.break_mins, 0)
                         ),
      client_id        = case
                           when u.new_client_id is not null and u.safe_to_overwrite
                             then u.new_client_id
                           else s.client_id
                         end,
      hr_request_id    = u.request_id,
      held_back_reason = u.held_back_reason,
      updated_at       = now(),

      -- ✅ UPDATED FIX (same as NHSP apply logic):
      -- Allow corrected candidate_id to overwrite when SAFE:
      --   - shift not linked to a timesheet yet, OR
      --   - linked timesheet has no current TSFIN row, OR
      --   - linked timesheet TSFIN exists and is not paid and not invoice-locked.
      candidate_id     = case
                           when u.new_candidate_id is not null and u.safe_to_overwrite
                             then u.new_candidate_id
                           else s.candidate_id
                         end
    from upd_src u
    where s.external_row_key = u.external_row_key
    returning
      (u.old_candidate_id is null and u.new_candidate_id is not null and u.safe_to_overwrite) as mapped_candidate
  )

  ----------------------------------------------------------------
  -- Aggregate counts
  ----------------------------------------------------------------
  select
    coalesce((select count(*) from ins), 0),
    coalesce((select count(*) from upd), 0),
    coalesce((select count(*) from ins where mapped_candidate), 0)
      + coalesce((select count(*) from upd where mapped_candidate), 0)
  into
    v_created,
    v_updated,
    v_mapped_candidates;

  return jsonb_build_object(
    'import_id',         import_id,
    'shifts_created',    v_created,
    'shifts_updated',    v_updated,
    'mapped_candidates', v_mapped_candidates
  );
end;
$$;
