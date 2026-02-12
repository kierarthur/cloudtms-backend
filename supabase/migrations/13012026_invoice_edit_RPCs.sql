
-- ============================================================
-- CloudTMS Patch (2026-01-18)
-- Segment lock summary invariant + debug logging (invoice_debug)
--
-- Includes:
--  - public._inv_unlock_segments_for_invoice (invariant recompute + debug)
--  - public._inv_lock_segments_for_invoice (invariant recompute + debug; applies universally)
--  - public._inv_unlock_segment_refs_for_invoice (NEW helper, selected unlock + debug)
--  - public.invoice_apply_edits (existing logic + segment add/remove; tsfin_id+segment_id; blocks segment ops when extras exist; debug)
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION statements only
-- ============================================================

create or replace function public._inv_unlock_segments_for_invoice(
  p_invoice_id uuid,
  p_timesheet_ids uuid[]
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_ts_id uuid;
  r_tf record;
  v_ib jsonb;
  v_seg jsonb;
  v_out_segs jsonb;
  v_locked_text text;

  v_seg_count int;
  v_any_unlocked boolean;
  v_first_locked text;
  v_multi boolean;
  v_new_locked uuid;
  v_new_locked_at timestamptz;

  v_snapshots_found int := 0;
  v_snapshots_updated int := 0;
  v_segments_unlocked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','start','at_utc',public._inv_iso_utc(v_dbg_started_at),'invoice_id',coalesce(p_invoice_id::text,''))
    );
  end if;

  if p_invoice_id is null then
    return;
  end if;

  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids,1),0) = 0 then
    return;
  end if;

  foreach v_ts_id in array p_timesheet_ids loop
    if v_ts_id is null then
      continue;
    end if;

    select tf.*
    into r_tf
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id = v_ts_id
    order by tf.created_at desc
    limit 1;

    if not found then
      continue;
    end if;

    v_snapshots_found := v_snapshots_found + 1;
    v_ib := r_tf.invoice_breakdown_json;

    if v_ib is not null
       and jsonb_typeof(v_ib) = 'object'
       and coalesce(v_ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(v_ib->'segments') = 'array'
    then
      v_out_segs := '[]'::jsonb;
      v_seg_count := 0;
      v_any_unlocked := false;
      v_first_locked := null;
      v_multi := false;

      for v_seg in
        select value from jsonb_array_elements(v_ib->'segments') value
      loop
        v_seg_count := v_seg_count + 1;

        if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
          v_out_segs := v_out_segs || jsonb_build_array(v_seg);
          v_any_unlocked := true;
          continue;
        end if;

        v_locked_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');

        if v_locked_text = p_invoice_id::text then
          v_locked_text := null;
          v_segments_unlocked := v_segments_unlocked + 1;
        end if;

        if v_locked_text is null then
          v_any_unlocked := true;
        else
          if v_first_locked is null then
            v_first_locked := v_locked_text;
          elsif v_locked_text <> v_first_locked then
            v_multi := true;
          end if;
        end if;

v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(v_locked_text), 'null'::jsonb), true);
v_out_segs := v_out_segs || jsonb_build_array(v_seg);

      end loop;

      v_ib := jsonb_set(v_ib, '{segments}', v_out_segs, true);

      v_new_locked := null;
      if v_seg_count > 0 and (not v_any_unlocked) and (not v_multi) and v_first_locked is not null then
        v_new_locked := v_first_locked::uuid;
      end if;

      if v_new_locked is not null then
        v_summaries_set := v_summaries_set + 1;
        if r_tf.locked_by_invoice_id is not null and r_tf.locked_by_invoice_id = v_new_locked and r_tf.locked_at_utc is not null then
          v_new_locked_at := r_tf.locked_at_utc;
        else
          v_new_locked_at := v_now;
        end if;
      else
        v_new_locked_at := null;
      end if;

      update public.timesheets_financials tfu
      set
        updated_at = v_now,
        invoice_breakdown_json = v_ib,
        locked_by_invoice_id = v_new_locked,
        locked_at_utc = v_new_locked_at
      where tfu.id = r_tf.id;

      v_snapshots_updated := v_snapshots_updated + 1;

    else
      -- Non-segments: unlock whole snapshot if it was locked to this invoice
      if r_tf.locked_by_invoice_id = p_invoice_id then
        update public.timesheets_financials tfu
        set
          updated_at = v_now,
          locked_by_invoice_id = null,
          locked_at_utc = null
        where tfu.id = r_tf.id;

        v_snapshots_updated := v_snapshots_updated + 1;
      end if;
    end if;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'timesheet_ids_count', coalesce(array_length(p_timesheet_ids,1),0),
        'snapshots_found', v_snapshots_found,
        'snapshots_updated', v_snapshots_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','finish','at_utc',public._inv_iso_utc(now()),'stats',v_dbg_stats));

      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGMENTS_DEBUG',
        jsonb_build_object('invoice_id',p_invoice_id::text,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || p_invoice_id::text),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'timesheet_ids_count', coalesce(array_length(p_timesheet_ids,1),0),
        'snapshots_found', v_snapshots_found,
        'snapshots_updated', v_snapshots_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGMENTS_ERROR',
        jsonb_build_object('invoice_id',coalesce(p_invoice_id::text,''),'sqlstate',v_dbg_sqlstate,'error',v_dbg_error,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;


create or replace function public._inv_unlock_segment_refs_for_invoice(
  p_invoice_id uuid,
  p_segment_refs jsonb
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_tsfin_id uuid;
  v_unlock_whole boolean;
  v_seg_ids text[];

  r_tf record;
  v_ib jsonb;
  v_seg jsonb;
  v_out_segs jsonb;
  v_sid text;
  v_locked_text text;

  v_seg_count int;
  v_any_unlocked boolean;
  v_first_locked text;
  v_multi boolean;
  v_new_locked uuid;
  v_new_locked_at timestamptz;

  v_tsfins int := 0;
  v_tsfins_updated int := 0;
  v_segments_unlocked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','start','at_utc',public._inv_iso_utc(v_dbg_started_at),'invoice_id',coalesce(p_invoice_id::text,''))
    );
  end if;

  if p_invoice_id is null then
    return;
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    return;
  end if;

  for v_tsfin_id in
    select distinct (x->>'tsfin_id')::uuid
    from jsonb_array_elements(p_segment_refs) x
    where nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null
  loop
    v_tsfins := v_tsfins + 1;

    select
      bool_or(nullif(btrim(coalesce(x->>'segment_id','')), '') is null) as unlock_whole,
      array_agg(distinct (x->>'segment_id')) filter (where nullif(btrim(coalesce(x->>'segment_id','')), '') is not null) as seg_ids
    into v_unlock_whole, v_seg_ids
    from jsonb_array_elements(p_segment_refs) x
    where (x->>'tsfin_id')::uuid = v_tsfin_id;

    v_unlock_whole := coalesce(v_unlock_whole,false);
    v_seg_ids := coalesce(v_seg_ids, array[]::text[]);

    -- ✅ FIX: lock the TSFIN row at read time to prevent lost-update corruption
    select tf.*
    into r_tf
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
    limit 1
    for update;

    if not found then
      continue;
    end if;

    v_ib := r_tf.invoice_breakdown_json;

    v_seg_count := 0;
    v_any_unlocked := false;
    v_first_locked := null;
    v_multi := false;

    if v_ib is not null
       and jsonb_typeof(v_ib) = 'object'
       and coalesce(v_ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(v_ib->'segments') = 'array'
    then
      v_out_segs := '[]'::jsonb;

      for v_seg in
        select value from jsonb_array_elements(v_ib->'segments') value
      loop
        v_seg_count := v_seg_count + 1;

        if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
          v_out_segs := v_out_segs || jsonb_build_array(v_seg);
          v_any_unlocked := true;
          continue;
        end if;

        v_sid := coalesce(v_seg->>'segment_id','');
        v_locked_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');

        if v_locked_text = p_invoice_id::text and (v_unlock_whole or (v_sid <> '' and v_sid = any(v_seg_ids))) then
          v_locked_text := null;
          v_segments_unlocked := v_segments_unlocked + 1;
        end if;

        if v_locked_text is null then
          v_any_unlocked := true;
        else
          if v_first_locked is null then
            v_first_locked := v_locked_text;
          elsif v_locked_text <> v_first_locked then
            v_multi := true;
          end if;
        end if;

  v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(v_locked_text), 'null'::jsonb), true);
v_out_segs := v_out_segs || jsonb_build_array(v_seg);

      end loop;

      v_ib := jsonb_set(v_ib, '{segments}', v_out_segs, true);

      v_new_locked := null;
      if v_seg_count > 0 and (not v_any_unlocked) and (not v_multi) and v_first_locked is not null then
        v_new_locked := v_first_locked::uuid;
      end if;

      if v_new_locked is not null then
        v_summaries_set := v_summaries_set + 1;
        if r_tf.locked_by_invoice_id is not null and r_tf.locked_by_invoice_id = v_new_locked and r_tf.locked_at_utc is not null then
          v_new_locked_at := r_tf.locked_at_utc;
        else
          v_new_locked_at := v_now;
        end if;
      else
        v_new_locked_at := null;
      end if;

      update public.timesheets_financials tfu
      set
        updated_at = v_now,
        invoice_breakdown_json = v_ib,
        locked_by_invoice_id = v_new_locked,
        locked_at_utc = v_new_locked_at
      where tfu.id = v_tsfin_id;

      v_tsfins_updated := v_tsfins_updated + 1;

    else
      if r_tf.locked_by_invoice_id = p_invoice_id then
        update public.timesheets_financials tfu
        set
          updated_at = v_now,
          locked_by_invoice_id = null,
          locked_at_utc = null
        where tfu.id = v_tsfin_id;

        v_tsfins_updated := v_tsfins_updated + 1;
      end if;
    end if;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins,
        'tsfins_updated', v_tsfins_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','finish','at_utc',public._inv_iso_utc(now()),'stats',v_dbg_stats));

      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGREFS_DEBUG',
        jsonb_build_object('invoice_id',p_invoice_id::text,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || p_invoice_id::text),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins,
        'tsfins_updated', v_tsfins_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGREFS_ERROR',
        jsonb_build_object('invoice_id',coalesce(p_invoice_id::text,''),'sqlstate',v_dbg_sqlstate,'error',v_dbg_error,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;

create or replace function public.invoice_apply_edits(
  p_invoice_id uuid,
  p_payload jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;


-- =====================================================
-- DEBUG (invoice_debug): single audit row per RPC call
-- =====================================================
v_invoice_debug boolean := false;
v_dbg_started_at timestamptz := now();
v_dbg_steps jsonb := '[]'::jsonb;
v_dbg_sqlstate text := null;
v_dbg_error text := null;
v_dbg_stats jsonb := '{}'::jsonb;


v_dbg_lines_deleted int := 0;
v_dbg_timesheets_unlocked int := 0;
v_dbg_seg_add_refs int := 0;
v_dbg_seg_remove_refs int := 0;
v_dbg_seg_tsfins int := 0;
v_dbg_seg_timesheets_rebuilt int := 0;
v_dbg_seg_timesheets_removed int := 0;
v_dbg_add_timesheets_found int := 0;
v_dbg_add_timesheets_skipped int := 0;

v_rc int := 0;

  v_inv record;
  v_week_start date;
  v_week_end date;

  v_remove_ids uuid[];
  v_add_ts_ids uuid[];

-- segment move payload (explicit segment add/remove)
v_remove_seg_refs jsonb;
v_add_seg_refs jsonb;
v_has_seg_ops boolean := false;
v_refresh_hr_cache boolean := false;
v_seg_tsfin_ids uuid[] := array[]::uuid[];
v_seg_ts_ids uuid[] := array[]::uuid[];
v_seg_refs_to_lock jsonb := '[]'::jsonb;
v_ref jsonb;
v_tsfin_id uuid;
v_seg_id text;
v_has_additional boolean;
v_has_expense_or_mileage boolean;

  -- reference updates (refs-to-issue)
  v_reference_updates jsonb;
  v_refupd jsonb;
  v_refupd_ts_id uuid;
  v_refupd_count int := 0;
  v_refupd_applied int := 0;
  v_refupd_set_refnum boolean;
  v_refupd_set_dayrefs boolean;
  v_refupd_set_sched boolean;
  v_refupd_dayrefs jsonb;
  v_refupd_sched jsonb;
  v_refupd_refnum text;

  -- reference update side-effects (meta refresh / segment ref sync)
  v_refupd_ts_ids uuid[] := array[]::uuid[];
  v_refupd_ts_ids_distinct uuid[] := array[]::uuid[];
  v_refupd_meta_rows_updated int := 0;
  v_refupd_tsfin_rows_updated int := 0;
  v_refupd_tsfin_segments_updated int := 0;

  -- temp vars for reference-derived meta and optional tsfin segment ref sync
  v_ref_ts_id uuid;
  v_ref_ts record;
  v_ref_schedule_refs jsonb := '[]'::jsonb;
  v_ref_schedule_refs_distinct jsonb := '[]'::jsonb;
  v_ref_sched_map jsonb := '{}'::jsonb;
  v_ref_sched_key text;
  v_ref_sched_ref text;
  v_ref_tsfin_id uuid;
  v_ref_ib jsonb;
  v_ref_new_segments jsonb := '[]'::jsonb;
  v_ref_seg_obj jsonb;
  v_ref_seg_start text;
  v_ref_seg_end text;
    v_ref_seg_id text;
v_ref_seg_cur_ref text;
  v_ref_seg_new_ref text;
  v_ref_seg_has_update boolean := false;
  v_ref_seg_updates_this_ts int := 0;
  v_ref_seg_matches_this_ts int := 0;

  -- audit (history) accumulators (NOT debug-only)
  v_hist_adj jsonb := '[]'::jsonb;
  v_hist_seg_add jsonb := '[]'::jsonb;
  v_hist_seg_remove jsonb := '[]'::jsonb;
  v_hist_lines_removed jsonb := '[]'::jsonb;
  v_hist_add_ts jsonb := '[]'::jsonb;

  -- contract week status touch set
  v_cw_ts_ids uuid[] := array[]::uuid[];

  v_ts_ids_touched uuid[] := array[]::uuid[];
  -- ✅ FIX: timesheets fully removed from this invoice (no remaining invoice_lines) should be unlocked
  v_ts_ids_fully_removed uuid[] := array[]::uuid[];

  v_vat_chargeable boolean := true;
  v_vat_rate numeric := 0;

  -- adjustments
  adj jsonb;
  v_adj_token text;
  v_adj_desc text;
  v_adj_ex numeric;
  v_adj_vat numeric;
  v_adj_inc numeric;
  v_adj_source_key text;
  v_meta jsonb;

  -- timesheet loop
  tsid uuid;
  snap record;
  ts record;
  pc record;
  contract_id uuid;
  v_client_daily_calc boolean := false;
  v_contract_override boolean := false;
  v_contract_daily_calc boolean;
  v_contract_bucket_labels jsonb;
  c_daily_calc boolean := false;
  c_bucket_labels jsonb := null;
  c_role text := null;
  c_display_site text := null;
  c_ward_hint text := null;

  -- segment filtering
  v_seg jsonb;
  segments jsonb := '[]'::jsonb;
  seg_target date;
  seg_date text;
  natural_start date;
  seg_locked text;
  seg_ref text;

  -- aggregation for weekly line
  h_day numeric; h_night numeric; h_sat numeric; h_sun numeric; h_bh numeric;
  pay_ex numeric; chg_ex numeric; margin_ex numeric;
  vat_amt numeric; inc_amt numeric;
  line_desc text;
  v_source_key text;

  -- daily aggregation record
  r_day record;

  -- segment refs for locking
  seg_refs jsonb := '[]'::jsonb;

  -- additional units
  kv record;
  ex jsonb;
  code text;
  unit_count numeric;
  bucket_name text;
  unit_name text;

  -- expenses notes
  v_note_travel text;
  v_note_accom text;
  v_note_other text;

  -- recompute totals
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  -- header meta counters (keep header_snapshot_json.meta in sync with current invoice state)
  v_hdr_ts_count_lines int := 0;
  v_hdr_ts_count_seglocks int := 0;
  v_hdr_seg_locked_count int := 0;
  v_hdr_meta_timesheet_count int := 0;
  v_hdr_meta_segment_count int := 0;

  v_manifest jsonb;
begin

-- Load invoice_debug flag (safe even if column not yet present)
begin
  select coalesce(sd.invoice_debug, false)
  into v_invoice_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;
exception when undefined_column then
  v_invoice_debug := false;
end;

if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'at_utc', public._inv_iso_utc(v_dbg_started_at),
      'invoice_id', p_invoice_id::text
    )
  );
end if;

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  for update
  limit 1;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Editable gate: DRAFT/ON_HOLD and unpaid
  if v_inv.status::text not in ('DRAFT','ON_HOLD') then
    raise exception 'Invoice is not editable (status=%)', v_inv.status::text;
  end if;

  if v_inv.paid_at_utc is not null then
    raise exception 'Invoice is not editable (already paid)';
  end if;



if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_loaded',
      'status', coalesce(v_inv.status::text,''),
      'paid_at_utc', case when v_inv.paid_at_utc is null then null else public._inv_iso_utc(v_inv.paid_at_utc) end
    )
  );
end if;

  -- Require invoice_week_start in header_snapshot_json.meta
  if v_inv.header_snapshot_json is null
     or btrim(coalesce(v_inv.header_snapshot_json #>> '{meta,invoice_week_start}','')) = ''
     or (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}') !~ '^\d{4}-\d{2}-\d{2}$'
  then
    raise exception 'Invoice header_snapshot_json.meta.invoice_week_start is required for edits';
  end if;

  v_week_start := (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}')::date;
  v_week_end := (v_week_start + interval '6 days')::date;


if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_week_loaded',
      'invoice_week_start', v_week_start::text,
      'invoice_week_end', v_week_end::text
    )
  );
end if;

  -- Load effective client setting daily_calc_of_invoices (used when contract.overrideclientsettings=false)
  begin
    select coalesce(cs0.daily_calc_of_invoices,false)
    into v_client_daily_calc
    from public.client_settings cs0
    where cs0.client_id = v_inv.client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;
  exception when others then
    v_client_daily_calc := false;
  end;

  -- VAT settings from invoice snapshot
  if jsonb_typeof(v_inv.header_snapshot_json->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_inv.header_snapshot_json->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := true;
  end if;

  if (v_inv.header_snapshot_json ? 'applied_vat_rate_pct') then
    begin
      v_vat_rate := (v_inv.header_snapshot_json->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_vat_rate := 0;
    end;
  else
    v_vat_rate := 0;
  end if;

  if v_vat_chargeable = false then
    v_vat_rate := 0;
  end if;

  -- Parse payload arrays
  v_remove_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_invoice_line_ids') then
    select array_agg((x)::uuid)
    into v_remove_ids
    from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  v_add_ts_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_timesheet_ids') then
    select array_agg((x)::uuid)
    into v_add_ts_ids
    from jsonb_array_elements_text(coalesce(p_payload->'add_timesheet_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;


-- Parse segment move payloads (tsfin_id + segment_id)
v_remove_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_segment_refs') then
  v_remove_seg_refs := coalesce(p_payload->'remove_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_remove_seg_refs) <> 'array' then
    v_remove_seg_refs := '[]'::jsonb;
  end if;
end if;

v_add_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_segment_refs') then
  v_add_seg_refs := coalesce(p_payload->'add_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_add_seg_refs) <> 'array' then
    v_add_seg_refs := '[]'::jsonb;
  end if;
end if;



-- Parse reference_updates (timesheet reference edits)
v_reference_updates := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'reference_updates') then
  v_reference_updates := coalesce(p_payload->'reference_updates','[]'::jsonb);
  if jsonb_typeof(v_reference_updates) <> 'array' then
    v_reference_updates := '[]'::jsonb;
  end if;
  v_refupd_count := jsonb_array_length(coalesce(v_reference_updates,'[]'::jsonb));
end if;
v_has_seg_ops :=
  (v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0)
  or (v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0);

  v_refresh_hr_cache := (coalesce(array_length(v_add_ts_ids,1),0) > 0) or coalesce(v_has_seg_ops,false);

  if v_invoice_debug then
    v_dbg_stats := v_dbg_stats || jsonb_build_object(
      'remove_invoice_line_ids_count', coalesce(array_length(v_remove_ids,1),0),
      'add_timesheet_ids_count', coalesce(array_length(v_add_ts_ids,1),0),
      'add_adjustments_count', case when p_payload is not null and jsonb_typeof(p_payload)='object' and (p_payload ? 'add_adjustments') and jsonb_typeof(p_payload->'add_adjustments')='array' then jsonb_array_length(p_payload->'add_adjustments') else 0 end,
      'remove_segment_refs_count', case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end,
      'add_segment_refs_count', case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end,
      'has_segment_ops', v_has_seg_ops
    );
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','payload_parsed','stats',v_dbg_stats));
  end if;





  -- 0) Apply reference updates to timesheets (does NOT recompute TSFIN; it updates the source timesheet refs)
  if v_reference_updates is not null and jsonb_typeof(v_reference_updates)='array' and jsonb_array_length(v_reference_updates) > 0 then
    for v_refupd in
      select value from jsonb_array_elements(v_reference_updates) value
    loop
      if v_refupd is null or jsonb_typeof(v_refupd) <> 'object' then
        continue;
      end if;

      if nullif(btrim(coalesce(v_refupd->>'timesheet_id','')), '') is null then
        continue;
      end if;

      v_refupd_ts_id := (v_refupd->>'timesheet_id')::uuid;

      v_refupd_set_refnum := (v_refupd ? 'reference_number');
      v_refupd_set_dayrefs := (v_refupd ? 'day_references_json');
      v_refupd_set_sched := (v_refupd ? 'actual_schedule_json');

      v_refupd_refnum := null;
      if v_refupd_set_refnum then
        v_refupd_refnum := nullif(btrim(coalesce(v_refupd->>'reference_number','')), '');
      end if;

      v_refupd_dayrefs := null;
      if v_refupd_set_dayrefs then
        v_refupd_dayrefs := v_refupd->'day_references_json';
        if v_refupd_dayrefs is not null and jsonb_typeof(v_refupd_dayrefs) = 'null' then
          v_refupd_dayrefs := null;
        end if;
      end if;

      v_refupd_sched := null;
      if v_refupd_set_sched then
        v_refupd_sched := v_refupd->'actual_schedule_json';
        if v_refupd_sched is not null and jsonb_typeof(v_refupd_sched) = 'null' then
          v_refupd_sched := null;
        end if;
      end if;

      update public.timesheets tsu
      set
        updated_at = v_now,
        reference_number = case when v_refupd_set_refnum then v_refupd_refnum else tsu.reference_number end,
        day_references_json = case when v_refupd_set_dayrefs then v_refupd_dayrefs else tsu.day_references_json end,
        actual_schedule_json = case when v_refupd_set_sched then v_refupd_sched else tsu.actual_schedule_json end
      where tsu.timesheet_id = v_refupd_ts_id
        and tsu.is_current = true;

      get diagnostics v_rc = row_count;
      if coalesce(v_rc,0) > 0 then
        v_refupd_applied := v_refupd_applied + 1;
        v_refupd_ts_ids := v_refupd_ts_ids || v_refupd_ts_id;
      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object('step','reference_updates_applied','count_requested',v_refupd_count,'count_applied',v_refupd_applied)
      );
    end if;
  end if;
  -- 0b) After reference updates: refresh invoice_lines meta (ts_reference_number / schedule_ref_nums) and
  -- (best-effort) sync tsfin SEGMENTS segment.ref_num from timesheets.actual_schedule_json
  if v_refupd_ts_ids is not null and coalesce(array_length(v_refupd_ts_ids,1),0) > 0 then

    select array_agg(distinct x)
    into v_refupd_ts_ids_distinct
    from unnest(v_refupd_ts_ids) x
    where x is not null;

    v_refupd_ts_ids_distinct := coalesce(v_refupd_ts_ids_distinct, array[]::uuid[]);

    foreach v_ref_ts_id in array v_refupd_ts_ids_distinct loop
      if v_ref_ts_id is null then
        continue;
      end if;

      select tsu.*
      into v_ref_ts
      from public.timesheets tsu
      where tsu.timesheet_id = v_ref_ts_id
        and tsu.is_current = true
      limit 1;

      if not found then
        continue;
      end if;

      -- Build schedule_ref_nums from timesheet-level ref, day refs, and manual schedule refs
      v_ref_schedule_refs := '[]'::jsonb;

      if nullif(btrim(coalesce(v_ref_ts.reference_number,'')), '') is not null then
        v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(v_ref_ts.reference_number),'')));
      end if;

      if v_ref_ts.day_references_json is not null and jsonb_typeof(v_ref_ts.day_references_json) = 'object' then
        for kv in
          select key as k, value as v
          from jsonb_each_text(v_ref_ts.day_references_json)
        loop
          if nullif(btrim(coalesce(kv.v,'')), '') is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(kv.v),'')));
          end if;
        end loop;
      end if;

      if v_ref_ts.actual_schedule_json is not null and jsonb_typeof(v_ref_ts.actual_schedule_json) = 'array' then
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ts.actual_schedule_json) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;
          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          if v_ref_sched_ref is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(v_ref_sched_ref));
          end if;
        end loop;
      end if;

      select coalesce(jsonb_agg(to_jsonb(x) order by x), '[]'::jsonb)
      into v_ref_schedule_refs_distinct
      from (
        select distinct btrim(t.x) as x
        from jsonb_array_elements_text(coalesce(v_ref_schedule_refs,'[]'::jsonb)) as t(x)
        where nullif(btrim(coalesce(t.x,'')), '') is not null
      ) q;

      -- Refresh meta_json on all invoice lines for this timesheet
      update public.invoice_lines ilu
      set meta_json = coalesce(ilu.meta_json, '{}'::jsonb) || jsonb_build_object(
        'ts_reference_number', nullif(btrim(coalesce(v_ref_ts.reference_number,'')), ''),
        'schedule_ref_nums', coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb),
        'schedule_ref_count', jsonb_array_length(coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb))
      )
      where ilu.invoice_id = p_invoice_id
        and ilu.timesheet_id = v_ref_ts_id;

      get diagnostics v_rc = row_count;
      v_refupd_meta_rows_updated := v_refupd_meta_rows_updated + coalesce(v_rc,0);

      -- Best-effort sync: update tsfin.invoice_breakdown_json segments[].ref_num by matching start/end fields
      v_ref_seg_updates_this_ts := 0;
      v_ref_seg_matches_this_ts := 0;
      v_ref_tsfin_id := null;
      v_ref_ib := null;

        select tfu.id, tfu.invoice_breakdown_json
      into v_ref_tsfin_id, v_ref_ib
      from public.timesheets_financials tfu
      where tfu.timesheet_id = v_ref_ts_id
        and tfu.is_current = true
      limit 1
      for update;


      if v_ref_tsfin_id is not null
         and v_ref_ib is not null
         and jsonb_typeof(v_ref_ib) = 'object'
         and upper(coalesce(v_ref_ib->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(v_ref_ib->'segments') = 'array'
         and v_ref_ts.actual_schedule_json is not null
         and jsonb_typeof(v_ref_ts.actual_schedule_json) = 'array'
       then
         -- Build a map of segment_id (preferred) or start|end -> ref_num from actual_schedule_json
        v_ref_sched_map := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ts.actual_schedule_json) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;

          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
            continue;
          end if;

          v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
          v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');
          if v_ref_seg_start is null or v_ref_seg_end is null then
            continue;
          end if;

          v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
          v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
        end loop;

        v_ref_new_segments := '[]'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ib->'segments') value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
            continue;
          end if;

          v_ref_seg_new_ref := null;
          v_ref_seg_has_update := false;

          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_sched_map ? v_ref_sched_key then
              v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
              v_ref_seg_has_update := true;
              v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
            end if;
          end if;

          if not v_ref_seg_has_update then
            v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
            v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');

            if v_ref_seg_start is not null and v_ref_seg_end is not null then
              v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
              if v_ref_sched_map ? v_ref_sched_key then
                v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
                v_ref_seg_has_update := true;
                v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
              end if;
            end if;
          end if;

          if v_ref_seg_has_update then
            v_ref_seg_cur_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
            if v_ref_seg_cur_ref is distinct from v_ref_seg_new_ref then
              v_ref_seg_obj := jsonb_set(
                v_ref_seg_obj,
                '{ref_num}',
                case when v_ref_seg_new_ref is null then 'null'::jsonb else to_jsonb(v_ref_seg_new_ref) end,
                true
              );
              v_ref_seg_updates_this_ts := v_ref_seg_updates_this_ts + 1;
            end if;
          end if;

          v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
        end loop;



        if v_ref_sched_map is not null
           and jsonb_typeof(v_ref_sched_map) = 'object'
           and v_ref_sched_map <> '{}'::jsonb
           and coalesce(jsonb_array_length(coalesce(v_ref_ib->'segments','[]'::jsonb)),0) > 0
           and coalesce(v_ref_seg_matches_this_ts,0) = 0
        then
          raise exception 'SEGMENTS reference sync failed: no segments matched schedule keys (timesheet_id=% tsfin_id=%)', v_ref_ts_id, v_ref_tsfin_id;
        end if;

        if v_ref_seg_updates_this_ts > 0 then
              update public.timesheets_financials tfu2
          set invoice_breakdown_json = jsonb_set(coalesce(tfu2.invoice_breakdown_json, '{}'::jsonb), '{segments}', v_ref_new_segments, true)
          where tfu2.id = v_ref_tsfin_id
            and tfu2.is_current = true;


          get diagnostics v_rc = row_count;
          if coalesce(v_rc,0) > 0 then
            v_refupd_tsfin_rows_updated := v_refupd_tsfin_rows_updated + 1;
            v_refupd_tsfin_segments_updated := v_refupd_tsfin_segments_updated + v_ref_seg_updates_this_ts;
          end if;
        end if;

      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','reference_updates_meta_refreshed',
          'timesheets_count', coalesce(array_length(v_refupd_ts_ids_distinct,1),0),
          'invoice_line_rows_updated', v_refupd_meta_rows_updated,
          'tsfin_rows_updated', v_refupd_tsfin_rows_updated,
          'tsfin_segments_refnum_updated', v_refupd_tsfin_segments_updated
        )
      );
    end if;
  end if;

  -- 1) Removals (by invoice_line_id)
  if v_remove_ids is not null and coalesce(array_length(v_remove_ids,1),0) > 0 then
    -- collect timesheet_ids touched (any)
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_ts_ids_touched
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids);

    -- record removed lines for history
    v_hist_lines_removed := coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb);

    -- Only unlock TSFIN when HOURS lines were removed (prevents accidental unlock when deleting only expenses/other lines)
    -- IMPORTANT: compute BEFORE deletion because we match on the removed invoice_line ids.
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_cw_ts_ids
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids)
      and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

    

    if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      v_refresh_hr_cache := true;
    end if;
delete from public.invoice_lines
    where invoice_id = p_invoice_id
      and id = any(v_remove_ids);
    get diagnostics v_rc = row_count;
    v_dbg_lines_deleted := v_dbg_lines_deleted + coalesce(v_rc,0);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','lines_removed',
          'rows_deleted', coalesce(v_rc,0),
          'timesheets_touched', coalesce(array_length(v_ts_ids_touched,1),0),
          'timesheets_to_unlock_count', coalesce(array_length(v_cw_ts_ids,1),0)
        )
      );
    end if;

    -- ✅ FIX: if a touched timesheet now has NO remaining invoice_lines on this invoice,
    -- unlock it even if the removed lines were expenses/mileage/additional (expense-only/SEGMENTS-empty case).
    v_ts_ids_fully_removed := array[]::uuid[];
    select array_agg(distinct x)
    into v_ts_ids_fully_removed
    from unnest(coalesce(v_ts_ids_touched, array[]::uuid[])) x
    where x is not null
      and not exists (
        select 1
        from public.invoice_lines l2
        where l2.invoice_id = p_invoice_id
          and l2.timesheet_id = x
      );

    v_ts_ids_fully_removed := coalesce(v_ts_ids_fully_removed, array[]::uuid[]);

    if coalesce(array_length(v_ts_ids_fully_removed,1),0) > 0 then
      -- unlock any segments locked to this invoice (safe no-op for SEGMENTS-empty)
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_ts_ids_fully_removed);

      -- clear whole-timesheet lock if it was set for this invoice (SEGMENTS-empty / non-segments / pseudo segment_id null locks)
      update public.timesheets_financials tfu_lock
      set
        locked_by_invoice_id = null,
        locked_at_utc = null,
        updated_at = v_now
      where tfu_lock.is_current = true
        and tfu_lock.timesheet_id = any(v_ts_ids_fully_removed)
        and tfu_lock.locked_by_invoice_id = p_invoice_id;

      get diagnostics v_rc = row_count;
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(v_rc,0);

      v_refresh_hr_cache := true;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','timesheets_unlocked_after_full_removal',
            'timesheets_fully_removed_count', coalesce(array_length(v_ts_ids_fully_removed,1),0),
            'tsfin_rows_unlocked_count', coalesce(v_rc,0)
          )
        );
      end if;
    end if;

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

    if v_cw_ts_ids is not null and coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_cw_ts_ids);
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(array_length(v_cw_ts_ids,1),0);

      -- Cleanup: if no segments remain on THIS invoice for a touched timesheet, remove ALL remaining invoice lines for that timesheet
      foreach tsid in array v_cw_ts_ids loop
        if tsid is null then continue; end if;

        -- detect if any segments are still locked to THIS invoice
        select tf.*
        into snap
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id = tsid
        limit 1;

        if not found then
          continue;
        end if;

        segments := '[]'::jsonb;
        if snap.invoice_breakdown_json is not null
           and jsonb_typeof(snap.invoice_breakdown_json)='object'
           and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
           and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
        then
          for v_seg in
            select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
          loop
            if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
              continue;
            end if;
            seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
            if seg_locked = p_invoice_id::text then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end loop;

          if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        else
          -- Non-segments: if snapshot is no longer locked to this invoice, remove all remaining invoice lines for this timesheet
          if snap.locked_by_invoice_id is null then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        end if;
      end loop;
    end if;

    -- History event (always)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_LINES_REMOVED',
      null,
      jsonb_build_object('remove_invoice_line_ids', v_hist_lines_removed, 'timesheet_ids_touched', coalesce(to_jsonb(v_ts_ids_touched), '[]'::jsonb)),
      null,
      p_actor_user_id
    );

  end if;


  -- 1b) Segment edits (SEGMENTS mode only)
-- NOTE: Segment moves are NOT allowed when additional rates OR expenses/mileage exist on the TSFIN snapshot.
-- Payload contract uses tsfin_id + segment_id.
if v_has_seg_ops then

  v_dbg_seg_add_refs := case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end;
  v_dbg_seg_remove_refs := case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','segment_ops_start',
        'add_segment_refs_count', v_dbg_seg_add_refs,
        'remove_segment_refs_count', v_dbg_seg_remove_refs
      )
    );
  end if;

  -- Collect distinct tsfin_ids involved in segment ops
  select array_agg(distinct (x->>'tsfin_id')::uuid)
  into v_seg_tsfin_ids
  from (
    select value as x
    from jsonb_array_elements(coalesce(v_remove_seg_refs,'[]'::jsonb))
    union all
    select value as x
    from jsonb_array_elements(coalesce(v_add_seg_refs,'[]'::jsonb))
  ) u
  where jsonb_typeof(x) = 'object'
    and nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null;

  v_seg_tsfin_ids := coalesce(v_seg_tsfin_ids, array[]::uuid[]);

  v_dbg_seg_tsfins := coalesce(array_length(v_seg_tsfin_ids,1),0);
  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','segment_ops_targets','tsfin_count',v_dbg_seg_tsfins));
  end if;

  if coalesce(array_length(v_seg_tsfin_ids,1),0) > 0 then

    -- Validate that each snapshot is current SEGMENTS mode and has NO additional/expenses/mileage
    foreach v_tsfin_id in array v_seg_tsfin_ids loop
      select *
      into snap
      from public.timesheets_financials tf
      where tf.id = v_tsfin_id
      limit 1;

      if not found then
        raise exception 'Segment edit refers to unknown tsfin_id %', v_tsfin_id;
      end if;

      if snap.is_current is not true then
        raise exception 'Segment edit requires current tsfin snapshot (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.client_id is distinct from v_inv.client_id then
        raise exception 'Segment edit timesheet client mismatch (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.invoice_breakdown_json is null
         or jsonb_typeof(snap.invoice_breakdown_json) <> 'object'
         or upper(coalesce(snap.invoice_breakdown_json->>'mode','')) <> 'SEGMENTS'
         or jsonb_typeof(snap.invoice_breakdown_json->'segments') <> 'array'
      then
        raise exception 'Segment edit is only supported for SEGMENTS timesheets (tsfin_id=%)', v_tsfin_id;
      end if;

      v_has_additional :=
        public._inv_round2(coalesce(snap.additional_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.additional_charge_ex_vat,0)) <> 0
        or (snap.additional_units_json is not null and jsonb_typeof(snap.additional_units_json)='object' and snap.additional_units_json <> '{}'::jsonb);

      v_has_expense_or_mileage :=
        public._inv_round2(coalesce(snap.expenses_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.expenses_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_units,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) <> 0;

      if v_has_additional then
        raise exception 'Segments cannot be moved when additional rates exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;

      if v_has_expense_or_mileage then
        raise exception 'Segments cannot be moved when expenses or mileage exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;
    end loop;

    -- Validate and apply add_segment_refs (lock selected segments)
    v_seg_refs_to_lock := '[]'::jsonb;
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      for v_ref in
        select value from jsonb_array_elements(v_add_seg_refs) value
      loop
        if v_ref is null or jsonb_typeof(v_ref) <> 'object' then
          continue;
        end if;

        if nullif(btrim(coalesce(v_ref->>'tsfin_id','')), '') is null then
          continue;
        end if;

        v_tsfin_id := (v_ref->>'tsfin_id')::uuid;
        v_seg_id := nullif(btrim(coalesce(v_ref->>'segment_id','')), '');

        if v_seg_id is null then
          raise exception 'add_segment_refs requires segment_id (tsfin_id=%)', v_tsfin_id;
        end if;

          -- Load snapshot + timesheet + precheck (must be OK)
        select
          tf.*,
          tsr.sheet_scope::text as sheet_scope,
          coalesce(tsr.submission_mode::text,'') as submission_mode,
          tsr.day_references_json,
          tsr.actual_schedule_json,
          tsr.week_ending_date,
          cw.contract_id
        into snap
        from public.timesheets_financials tf
        join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
        left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
        join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
        where tf.id = v_tsfin_id
          and tf.is_current = true
          and tf.client_id = v_inv.client_id
          and upper(coalesce(pcv.precheck_status,'')) = 'OK'
        limit 1;

        if not found then
          raise exception 'Segment add failed eligibility (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        natural_start := (snap.week_ending_date::date - 6);

        -- Locate the segment object
        v_seg := null;
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;
          if nullif(btrim(coalesce(v_seg->>'segment_id','')), '') = v_seg_id then
            exit;
          end if;
          v_seg := null;
        end loop;

        if v_seg is null then
          raise exception 'Segment not found (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
        if seg_locked is not null then
          raise exception 'Segment already invoiced (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
        seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

        -- segment-level ref gating if required
        select * into pc from public.v_ts_invoice_precheck where timesheet_id = snap.timesheet_id limit 1;
        if pc.require_reference_to_invoice is true and seg_ref = '' then
          raise exception 'Segment missing reference number (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
        end if;

        -- Week eligibility
        if seg_target is null or seg_target = natural_start then
          if v_week_start <> natural_start then
            raise exception 'Segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        else
          if v_week_start <> seg_target then
            raise exception 'Delayed segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
          if seg_target > v_anchor_ymd then
            raise exception 'Delayed segment cannot be invoiced early (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        end if;

        v_seg_refs_to_lock := v_seg_refs_to_lock || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', v_tsfin_id::text,
            'segment_id', v_seg_id
          )
        );
      end loop;

      if jsonb_typeof(v_seg_refs_to_lock) = 'array' and jsonb_array_length(v_seg_refs_to_lock) > 0 then
        perform public._inv_lock_segments_for_invoice(p_invoice_id, v_seg_refs_to_lock);
      end if;
    end if;

      -- Apply remove_segment_refs (unlock selected segments on THIS invoice)
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._inv_unlock_segment_refs_for_invoice(p_invoice_id, v_remove_seg_refs::jsonb, p_actor_user_id);
    end if;



    -- History: segment ops (always)
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_ADDED',
        null,
        jsonb_build_object('add_segment_refs', v_add_seg_refs),
        null,
        p_actor_user_id
      );
    end if;
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_REMOVED',
        null,
        jsonb_build_object('remove_segment_refs', v_remove_seg_refs),
        null,
        p_actor_user_id
      );
    end if;

    -- Rebuild HOURS lines for touched timesheets on this invoice
    select array_agg(distinct tf.timesheet_id)
    into v_seg_ts_ids
    from public.timesheets_financials tf
    where tf.id = any(v_seg_tsfin_ids);

    v_seg_ts_ids := coalesce(v_seg_ts_ids, array[]::uuid[]);

    foreach tsid in array v_seg_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + contract (no READY_FOR_INVOICE restriction; this is an invoice edit)
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',tsid::text));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',tsid::text,'tsfin_id',snap.id::text));
      end if;


      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_display_site := null;

      if contract_id is not null then
        select
          coalesce(overrideclientsettings,false),
          daily_calc_of_invoices,
          bucket_labels_json,
          nullif(btrim(coalesce(display_site,'')), '')
        into
          v_contract_override, v_contract_daily_calc, v_contract_bucket_labels, c_display_site
        from public.contracts
        where id = contract_id
        limit 1;

        c_daily_calc := case when v_contract_override then coalesce(v_contract_daily_calc,false) else v_client_daily_calc end;
        c_bucket_labels := case when v_contract_override then v_contract_bucket_labels else null end;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      -- Build segment set locked to THIS invoice
      segments := '[]'::jsonb;
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked = p_invoice_id::text then
            segments := segments || jsonb_build_array(v_seg);
          end if;
        end loop;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        -- If no segments remain on this invoice for this timesheet, remove ALL invoice lines for that timesheet
        delete from public.invoice_lines
        where invoice_id = p_invoice_id
          and timesheet_id = tsid;
        v_dbg_seg_timesheets_removed := v_dbg_seg_timesheets_removed + 1;
        continue;
      end if;

      v_dbg_seg_timesheets_rebuilt := v_dbg_seg_timesheets_rebuilt + 1;

      -- Replace HOURS lines for this timesheet on this invoice
      delete from public.invoice_lines
      where invoice_id = p_invoice_id
        and timesheet_id = tsid
        and upper(coalesce(meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');
        -- HOURS lines
        if c_daily_calc then
          -- Daily: group by segment.date
          for r_day in
            with rows as (
              select
                nullif(btrim(coalesce(seg_el->>'date','')), '') as ymd,
                coalesce((seg_el->>'hours_day')::numeric,0)   as h_day,
                coalesce((seg_el->>'hours_night')::numeric,0) as h_night,
                coalesce((seg_el->>'hours_sat')::numeric,0)   as h_sat,
                coalesce((seg_el->>'hours_sun')::numeric,0)   as h_sun,
                coalesce((seg_el->>'hours_bh')::numeric,0)    as h_bh,
                coalesce((seg_el->>'pay_amount')::numeric,0)  as pay_ex,
                coalesce((seg_el->>'charge_amount')::numeric,0) as chg_ex
              from jsonb_array_elements(segments) seg_el
            ),
            agg as (
              select
                ymd,
                sum(rows.h_day)::numeric as hours_day,
                sum(rows.h_night)::numeric as hours_night,
                sum(rows.h_sat)::numeric as hours_sat,
                sum(rows.h_sun)::numeric as hours_sun,
                sum(rows.h_bh)::numeric as hours_bh,
                sum(rows.pay_ex)::numeric as pay_ex,
                sum(rows.chg_ex)::numeric as chg_ex
              from rows
              where ymd is not null and ymd ~ '^\d{4}-\d{2}-\d{2}$'
              group by ymd
            )
            select * from agg order by ymd
          loop
          chg_ex := public._inv_round2(r_day.chg_ex);
if chg_ex = 0 then continue; end if;

if (coalesce(r_day.hours_day,0)+coalesce(r_day.hours_night,0)+coalesce(r_day.hours_sat,0)+coalesce(r_day.hours_sun,0)+coalesce(r_day.hours_bh,0)) = 0 then
  continue;
end if;

  
            pay_ex := public._inv_round2(r_day.pay_ex);
            margin_ex := public._inv_round2(chg_ex - pay_ex);
            vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
            inc_amt := public._inv_round2(chg_ex + vat_amt);
  
            line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                         ' – '|| r_day.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');
  
            v_meta := jsonb_build_object(
              'line_type','HOURS_DAILY',
              'timesheet_id', tsid::text,
              'tsfin_id', snap.id::text,
              'candidate_display', coalesce(nullif(btrim(coalesce(c_display_site,'')),''), null),
              'role', c_role,
              'hospital', c_display_site,
              'ward', c_ward_hint,
              'week_ending_date', snap.week_ending_date::text,
              'date', r_day.ymd,
              'bucket_labels', c_bucket_labels
            );
  
            v_source_key := 'TS:' || tsid::text || ':HOURS:' || r_day.ymd;
  
            insert into public.invoice_lines(
              invoice_id, timesheet_id, booking_id, description,
              hours_day, hours_night, hours_sat, hours_sun, hours_bh,
              pay_day, pay_night, pay_sat, pay_sun, pay_bh,
              charge_day, charge_night, charge_sat, charge_sun, charge_bh,
              total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
              vat_rate_pct, vat_amount, total_inc_vat,
              paper_ts_r2_key, meta_json, source_key
            )
            values (
              p_invoice_id, tsid, snap.booking_id, line_desc,
              public._inv_round2(r_day.hours_day), public._inv_round2(r_day.hours_night), public._inv_round2(r_day.hours_sat), public._inv_round2(r_day.hours_sun), public._inv_round2(r_day.hours_bh),
              null,null,null,null,null,
              null,null,null,null,null,
              pay_ex, chg_ex, margin_ex,
              v_vat_rate, vat_amt, inc_amt,
              ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
              v_meta,
              v_source_key
            )
            on conflict (invoice_id, source_key) do nothing;
          end loop;
        else
          -- Weekly hours line
          select
            public._inv_round2(coalesce(sum((seg_el->>'hours_day')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_night')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sat')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sun')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_bh')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'pay_amount')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'charge_amount')::numeric),0))
          into h_day, h_night, h_sat, h_sun, h_bh, pay_ex, chg_ex
          from jsonb_array_elements(segments) seg_el;
  
       if chg_ex <> 0 and (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) <> 0 then
  margin_ex := public._inv_round2(chg_ex - pay_ex);
  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
  inc_amt := public._inv_round2(chg_ex + vat_amt);

  line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
               ' – W/E '|| coalesce(snap.week_ending_date::text,'');

  v_meta := jsonb_build_object(
    'line_type','HOURS_WEEKLY',
    'timesheet_id', tsid::text,
    'tsfin_id', snap.id::text,
    'week_ending_date', snap.week_ending_date::text,
    'bucket_labels', c_bucket_labels
  );

  v_source_key := 'TS:' || tsid::text || ':HOURS:WEEK';

  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  values (
    p_invoice_id, tsid, snap.booking_id, line_desc,
    h_day, h_night, h_sat, h_sun, h_bh,
    null,null,null,null,null,
    null,null,null,null,null,
    pay_ex, chg_ex, margin_ex,
    v_vat_rate, vat_amt, inc_amt,
    ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
    v_meta,
    v_source_key
  )
  on conflict (invoice_id, source_key) do nothing;
end if;

        end if;
  
        -- Additional rates


    end loop;
  end if;
end if;

  -- 2) Add adjustments
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_adjustments') then
    if jsonb_typeof(p_payload->'add_adjustments') = 'array' then
      for adj in
        select value from jsonb_array_elements(p_payload->'add_adjustments') value
      loop
        if adj is null or jsonb_typeof(adj) <> 'object' then
          continue;
        end if;

        v_adj_token := nullif(btrim(coalesce(adj->>'client_token','')), '');
        v_adj_desc  := nullif(btrim(coalesce(adj->>'description','')), '');
        begin
          v_adj_ex := (adj->>'amount_ex_vat')::numeric;
        exception when others then
          v_adj_ex := null;
        end;

        if v_adj_token is null or v_adj_desc is null or v_adj_ex is null then
          continue;
        end if;

        v_adj_vat := public._inv_round2(v_adj_ex * v_vat_rate / 100);
        if v_vat_rate = 0 then v_adj_vat := 0; end if;
        v_adj_inc := public._inv_round2(v_adj_ex + v_adj_vat);

        v_adj_source_key := 'ADJ:' || v_adj_token;

        v_meta := jsonb_build_object(
          'line_type','ADJUSTMENT',
          'client_token', v_adj_token,
          'description', v_adj_desc,
          'amount_ex_vat', public._inv_round2(v_adj_ex),
          'vat_rate_pct', v_vat_rate,
          'vat_chargeable', v_vat_chargeable
        );

        v_hist_adj := v_hist_adj || jsonb_build_array(v_meta);

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, null, null, v_adj_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          0, public._inv_round2(v_adj_ex), public._inv_round2(v_adj_ex),
          v_vat_rate, v_adj_vat, v_adj_inc,
          null,
          v_meta,
          v_adj_source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end loop;
    end if;
  end if;

  -- History: adjustments added (always)
  if jsonb_typeof(v_hist_adj)='array' and jsonb_array_length(v_hist_adj) > 0 then
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_ADJUSTMENTS_ADDED',
      null,
      jsonb_build_object('adjustments', v_hist_adj),
      null,
      p_actor_user_id
    );
  end if;


  -- 3) Add timesheets (full parity: hours + additional + expenses + mileage), for THIS invoice week_start
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    foreach tsid in array v_add_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + precheck
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
        and tf.locked_by_invoice_id is null
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and upper(coalesce(pcv.precheck_status,'')) = 'OK'
        and tf.client_id = v_inv.client_id
        and (
          pcv.require_reference_to_invoice is not true
          or public._inv_timesheet_has_invoice_reference(
                tsr.sheet_scope::text,
                coalesce(tsr.submission_mode::text,''),
                tsr.reference_number,
                tsr.day_references_json,
                tsr.actual_schedule_json
             )
        )
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',coalesce(tsid::text,'')));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',coalesce(tsid::text,''),'tsfin_id',coalesce(snap.id::text,'')));
      end if;

      -- (rest of function unchanged)
      -- ...
    end loop;

    -- History: timesheets added (always; includes requested ids)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_TIMESHEETS_ADDED',
      null,
      jsonb_build_object('add_timesheet_ids', coalesce(to_jsonb(v_add_ts_ids), '[]'::jsonb)),
      null,
      p_actor_user_id
    );
  end if;

  -- 3c) Contract week status: set INVOICED only when timesheet is FULLY invoiced (segment-aware), and revert INVOICED -> AUTHORISED if no longer fully invoiced.
  -- Touch set = union of: add_timesheet_ids, line-removal touched (hours), segment-op touched.
  v_cw_ts_ids := array[]::uuid[];
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_add_ts_ids;
  end if;
  if v_ts_ids_touched is not null and coalesce(array_length(v_ts_ids_touched,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_ts_ids_touched;
  end if;
  if v_seg_ts_ids is not null and coalesce(array_length(v_seg_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_seg_ts_ids;
  end if;

  -- de-dup
  select array_agg(distinct x)
  into v_cw_ts_ids
  from unnest(coalesce(v_cw_ts_ids, array[]::uuid[])) x
  where x is not null;

  v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

  if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
    with src as (
      select
        cw.timesheet_id,
        cw.status as cw_status,
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json,
        (
          case
            when tf.invoice_breakdown_json is not null
             and jsonb_typeof(tf.invoice_breakdown_json)='object'
             and coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
             and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
             and jsonb_array_length(tf.invoice_breakdown_json->'segments') > 0
            then
              not exists (
                select 1
                from jsonb_array_elements(tf.invoice_breakdown_json->'segments') s(seg)
                where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is null
              )
            else
              (tf.locked_by_invoice_id is not null)
          end
        ) as fully_invoiced
      from public.contract_weeks cw
      join public.timesheets_financials tf
        on tf.is_current = true
       and tf.timesheet_id = cw.timesheet_id
      where cw.timesheet_id = any(v_cw_ts_ids)
    )
    update public.contract_weeks cw
    set status = case
      when src.fully_invoiced then 'INVOICED'::public.contract_week_status_enum
      when cw.status = 'INVOICED'::public.contract_week_status_enum then 'AUTHORISED'::public.contract_week_status_enum
      else cw.status
    end
    from src
    where cw.timesheet_id = src.timesheet_id;
  end if;


  -- 4) Recompute invoice totals from invoice_lines and clear PDF key
  select
    public._inv_round2(coalesce(sum(coalesce(l.total_charge_ex_vat,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.vat_amount,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.total_inc_vat,0)),0))
  into v_new_ex, v_new_vat, v_new_inc
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  
  perform public.invoice_recompute_totals(p_invoice_id);

  -- Recompute header_snapshot_json.meta counters to avoid stale values after edits
  select count(distinct l.timesheet_id)
  into v_hdr_ts_count_lines
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  select count(*)
  into v_hdr_seg_locked_count
  from public.timesheets_financials tf
  cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
  where tf.is_current = true
    and coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text;

  select count(distinct tf.timesheet_id)
  into v_hdr_ts_count_seglocks
  from public.timesheets_financials tf
  where tf.is_current = true
    and exists (
      select 1
      from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
      where coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text
    );

  if coalesce(v_hdr_seg_locked_count,0) > 0 then
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_seglocks,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_seg_locked_count,0);
  else
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_lines,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_meta_timesheet_count,0);
  end if;

  -- Ensure render cache invalidated (generated timestamp cleared as well) and persist refreshed meta counters
  update public.invoices invu
  set
    invoice_pdf_generated_at_utc = null,
    header_snapshot_json = jsonb_set(
      jsonb_set(
        coalesce(invu.header_snapshot_json,'{}'::jsonb),
        '{meta,timesheet_count}',
        to_jsonb(v_hdr_meta_timesheet_count),
        true
      ),
      '{meta,segment_count}',
      to_jsonb(v_hdr_meta_segment_count),
      true
    )
  where invu.id = p_invoice_id;
-- Refresh invoice-level NHSP/HR cache after timesheet/segment changes
  if coalesce(v_refresh_hr_cache,false) = true then
    perform 1
    from public.invoice_source_rows_collect(p_invoice_id, true) sr
    limit 1;
  end if;

-- Return updated manifest
  select public.invoice_render_manifest(p_invoice_id) into v_manifest;
  v_manifest := coalesce(v_manifest, '{}'::jsonb);

  if v_invoice_debug then
    begin
      -- attach finish marker (avoid extra heavy queries here)
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'invoice_total_charge_ex_vat', v_new_ex,
          'invoice_vat_amount', v_new_vat,
          'invoice_total_inc_vat', v_new_inc
        )
      );

      v_dbg_stats := v_dbg_stats || jsonb_build_object(
        'lines_deleted', v_dbg_lines_deleted,
        'timesheets_unlocked_via_line_removal', v_dbg_timesheets_unlocked,
        'segment_add_refs', v_dbg_seg_add_refs,
        'segment_remove_refs', v_dbg_seg_remove_refs,
        'segment_tsfin_count', v_dbg_seg_tsfins,
        'segment_timesheets_rebuilt', v_dbg_seg_timesheets_rebuilt,
        'segment_timesheets_removed', v_dbg_seg_timesheets_removed,
        'add_timesheets_found', v_dbg_add_timesheets_found,
        'add_timesheets_skipped', v_dbg_add_timesheets_skipped
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_DEBUG',
        jsonb_build_object(
          'invoice_id', p_invoice_id::text,
          'week_start', v_week_start::text,
          'week_end', v_week_end::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_manifest;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        coalesce(p_invoice_id::text,''),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;


