
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


