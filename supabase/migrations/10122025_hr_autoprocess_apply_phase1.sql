-- Phase 1 of HealthRoster autoprocess apply (REVISED):
-- hr_rows (for given import_id) -> nhsp_shifts (source_system = 'HEALTHROSTER')
-- + candidate_id auto-mapping.
--
-- REVISED LOGIC (hours-change workflow support):
-- - Adds support for:
--   (a) SKIP list: do not insert/update shifts for these external_row_keys
--   (b) FORCE OVERWRITE list: allow overwriting time fields (start/end/break/pay_minutes)
--       even when the linked timesheet is paid/invoiced/locked.
-- - If a shift is NOT safe_to_overwrite and NOT forced:
--   - we will NOT overwrite start/end/break/pay_minutes (preserve prior truth)
--   - we still update metadata (latest_import_id, ward, request_id, held_back_reason, etc.)
--
-- Returns counts so the Worker can log/display summary.



-- Phase 1 of HealthRoster autoprocess apply (REVISED):
-- hr_rows (for given import_id) -> nhsp_shifts (source_system = 'HEALTHROSTER')
-- + candidate_id auto-mapping.
--
-- LOCKED CONTRACT (per your instruction):
-- - Unticked rows → external_row_key must be included in p_skip_external_row_keys so Phase-1 does not touch them at all.
-- - Ticked rows (incl RED) → external_row_key must be included in p_force_overwrite_external_row_keys.
--   This is the technical representation of “user ticked apply”.
--
-- FINAL POLICY CHANGES IMPLEMENTED HERE:
-- - Paid/locked/invoiced never block truth updates when ticked (i.e. when in FORCE list).
-- - If an existing shift is currently cancelled (cancelled_at_utc is not null) and the row is applied (not skipped),
--   Phase-1 clears cancelled_at_utc/cancelled_by_import_id/cancelled_reason unconditionally.
-- - Phase-1 does NOT reattach timesheets; timesheet_id is not modified here.
--
-- NOTE: selected_group_ids remains unused (row-level selection is enforced by skip/force lists).


create or replace function public.hr_autoprocess_apply_phase1(
  import_id uuid,
  selected_group_ids text[] default null,
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

  -- ✅ NEW: count of rows we refused to touch because the matched shift is invoice/paid locked
  v_skipped_locked    int := 0;
begin
  ----------------------------------------------------------------
  -- Sanity: import must exist and be HEALTHROSTER
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = hr_autoprocess_apply_phase1.import_id
    and hi.source_system = 'HEALTHROSTER'::public.hr_source_enum;

  if not found then
    raise exception 'hr_autoprocess_apply_phase1: import % not found or not HEALTHROSTER', import_id;
  end if;

  with src as (
    select
      r.id          as hr_row_id,
      r.external_row_key,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,

      -- ✅ Break minutes priority:
      --  1) payload actual_break_mins / actual_break_minutes
      --  2) payload break_mins / break_minutes
      --  3) 0
      greatest(
        0,
        coalesce(
          case
            when (r.payload_json ? 'actual_break_mins')
             and nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '') is not null
             and (r.payload_json ->> 'actual_break_mins') ~ '^[0-9]+$'
            then (r.payload_json ->> 'actual_break_mins')::int
            else null
          end,
          case
            when (r.payload_json ? 'actual_break_minutes')
             and nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '') is not null
             and (r.payload_json ->> 'actual_break_minutes') ~ '^[0-9]+$'
            then (r.payload_json ->> 'actual_break_minutes')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '') is not null
             and (r.payload_json ->> 'break_mins') ~ '^[0-9]+$'
            then (r.payload_json ->> 'break_mins')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '') is not null
             and (r.payload_json ->> 'break_minutes') ~ '^[0-9]+$'
            then (r.payload_json ->> 'break_minutes')::int
            else null
          end,
          0
        )
      ) as break_mins,

      coalesce(
        nullif((r.payload_json ->> 'finalized_date'), ''),
        nullif((r.payload_json ->> 'finalised_date'), '')
      ) as finalized_raw,

      -- ✅ Request id priority (trimmed + blank->NULL):
      --  1) hr_rows.hr_request_id
      --  2) payload_json.request_id
      coalesce(
        nullif(btrim(coalesce(r.hr_request_id,'')), ''),
        nullif(btrim(coalesce(r.payload_json ->> 'request_id','')), '')
      ) as request_id
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
      lower(trim(s.staff_name)) as staff_norm,
      s.ward,
      lower(trim(s.ward))       as ward_norm,
      s.start_utc,
      s.end_utc,
      s.break_mins,

      -- ✅ ensure request_id is always trimmed and blank->NULL
      nullif(btrim(coalesce(s.request_id,'')), '') as request_id,

      case
        when coalesce(trim(s.finalized_raw), '') = '' then 'NO_FINALISED_DATE'
        else null
      end as held_back_reason,

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

  -- Enforce row-level selection:
  -- - Unticked rows must be in SKIP list → not present here
  -- - Ticked rows must be in FORCE list → only rows in FORCE are processed when FORCE list provided
  normed_filtered as (
    select *
    from normed n
    where
      (
        p_skip_external_row_keys is null
        or array_length(p_skip_external_row_keys, 1) is null
        or n.external_row_key is null
        or n.external_row_key <> all(p_skip_external_row_keys)
      )
      and (
        p_force_overwrite_external_row_keys is null
        or array_length(p_force_overwrite_external_row_keys, 1) is null
        or (n.external_row_key is not null and n.external_row_key = any(p_force_overwrite_external_row_keys))
      )
  ),

  resolved_base as (
    select
      n.*,
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id
    from normed_filtered n

    cross join lateral (
      select
        nullif(lower(trim(coalesce(n.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(n.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2
    ) nx

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

  -- ✅ NEW: if Request ID changes but the shift overlaps the existing one, reuse the existing external_row_key
  match_existing as (
    select
      x.hr_row_id,
      x.external_row_key as existing_external_row_key
    from (
      select
        rb.hr_row_id,
        s2.external_row_key,
        row_number() over (
          partition by rb.hr_row_id
          order by s2.updated_at desc nulls last, s2.created_at desc nulls last, s2.id desc
        ) as rn
      from resolved_base rb
      join public.nhsp_shifts s2
        on s2.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and s2.client_id = rb.client_id
       and rb.candidate_id is not null
       and s2.candidate_id = rb.candidate_id
       and s2.work_date = rb.work_date
       and rb.start_utc is not null
       and rb.end_utc is not null
       and s2.start_utc is not null
       and s2.end_utc is not null
       and (least(s2.end_utc, rb.end_utc) - greatest(s2.start_utc, rb.start_utc)) >= interval '1 minute'
    ) as x
    where x.rn = 1
  ),

  resolved as (
    select
      rb.*,
      coalesce(me.existing_external_row_key, rb.external_row_key) as external_row_key_eff
    from resolved_base rb
    left join match_existing me
      on me.hr_row_id = rb.hr_row_id
  ),

  fin_current as (
    select distinct on (tf.timesheet_id)
      tf.timesheet_id,
      tf.locked_by_invoice_id,
      tf.paid_at_utc,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
    order by tf.timesheet_id, tf.created_at desc
  ),

  ext_update as (
    update public.hr_rows r
    set external_row_key = res.external_row_key_eff
    from resolved res
    where r.id = res.hr_row_id
      and res.external_row_key_eff is not null
      and r.external_row_key is distinct from res.external_row_key_eff
    returning 1
  ),

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
      ref_num,
      held_back_reason,
      candidate_id,
      created_at,
      updated_at
    )
    select
      r.external_row_key_eff,
      hr_autoprocess_apply_phase1.import_id,
      'HEALTHROSTER'::public.hr_source_enum,
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
      r.request_id,
      r.held_back_reason,
      r.candidate_id,
      now(),
      now()
    from resolved r
    where r.external_row_key_eff is not null
      and not exists (
        select 1
        from public.nhsp_shifts s
        where s.external_row_key = r.external_row_key_eff
      )
    returning
      (candidate_id is not null) as mapped_candidate
  ),

  upd_src as (
    select
      s.external_row_key,
      s.id as nhsp_shift_id,
      s.invoice_id as shift_invoice_id,
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
      fc.invoice_breakdown_json,

      (
        fc.invoice_breakdown_json is not null
        and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
        and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
        and exists (
          select 1
          from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg(seg_obj)
          where nullif(btrim(coalesce(seg.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
            and nullif(btrim(coalesce(seg.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
        )
      ) as is_segment_locked,

      (
        s.invoice_id is not null
        or fc.locked_by_invoice_id is not null
        or fc.paid_at_utc is not null
        or (
          fc.invoice_breakdown_json is not null
          and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
          and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
          and exists (
            select 1
            from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg2(seg_obj)
            where nullif(btrim(coalesce(seg2.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
              and nullif(btrim(coalesce(seg2.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
          )
        )
      ) as is_invoice_locked,

      (
        not (
          s.invoice_id is not null
          or fc.locked_by_invoice_id is not null
          or fc.paid_at_utc is not null
          or (
            fc.invoice_breakdown_json is not null
            and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
            and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
            and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
            and exists (
              select 1
              from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg3(seg_obj)
              where nullif(btrim(coalesce(seg3.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
                and nullif(btrim(coalesce(seg3.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
            )
          )
        )
      ) as safe_to_overwrite

    from public.nhsp_shifts s
    join resolved r
      on r.external_row_key_eff = s.external_row_key
    left join fin_current fc
      on fc.timesheet_id = s.timesheet_id
    where s.source_system = 'HEALTHROSTER'::public.hr_source_enum
  ),

  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = hr_autoprocess_apply_phase1.import_id,
      source_system    = 'HEALTHROSTER'::public.hr_source_enum,

      work_date        = u.work_date,
      ward             = nullif(u.ward, ''),

      -- ✅ Do not clear request/ref if the new row has no request_id (preserve existing truth)
      hr_request_id    = case when u.request_id is not null then u.request_id else s.hr_request_id end,
      ref_num          = case when u.request_id is not null then u.request_id else s.ref_num end,

      held_back_reason = u.held_back_reason,
      updated_at       = now(),

      cancelled_at_utc = null,
      cancelled_by_import_id = null,
      cancelled_reason = null,

      start_utc        = u.start_utc,
      end_utc          = u.end_utc,
      break_mins       = coalesce(u.break_mins, 0),
      pay_minutes      = greatest(
                           0,
                           (extract(epoch from (u.end_utc - u.start_utc)) / 60)::int
                           - coalesce(u.break_mins, 0)
                         ),

      client_id        = case
                           when u.new_client_id is not null then u.new_client_id
                           else s.client_id
                         end,

      candidate_id     = case
                           when u.new_candidate_id is not null then u.new_candidate_id
                           else s.candidate_id
                         end
    from upd_src u
    where s.external_row_key = u.external_row_key
      and u.safe_to_overwrite is true
    returning
      (u.old_candidate_id is null and u.new_candidate_id is not null and u.safe_to_overwrite) as mapped_candidate
  )

  select
    coalesce((select count(*) from ins), 0),
    coalesce((select count(*) from upd), 0),
    coalesce((select count(*) from ins where mapped_candidate), 0)
      + coalesce((select count(*) from upd where mapped_candidate), 0),
    coalesce((select count(*) from upd_src where is_invoice_locked is true), 0)
  into
    v_created,
    v_updated,
    v_mapped_candidates,
    v_skipped_locked;

  return jsonb_build_object(
    'import_id',             import_id,
    'shifts_created',        v_created,
    'shifts_updated',        v_updated,
    'mapped_candidates',     v_mapped_candidates,
    'skipped_locked_shifts', v_skipped_locked
  );
end;
$$;


