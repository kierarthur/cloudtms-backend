-- ============================================================
-- CloudTMS Patch (Jan 2026)
--
-- Contents:
--   1) DB change (B1): client_settings.reference_number_required_to_issue_invoice
--   2) NEW helper: public._inv_unlock_segment_refs_for_invoice(...)
--   3) NEW helper: public._inv_segments_for_invoice(...)
--
-- Notes:
-- - SAFE TO RE-RUN (idempotent DO for schema, CREATE OR REPLACE for functions)
-- - Extensive debug logging: if public.settings_defaults.invoice_debug = true,
--   each function writes ONE audit_events row via public._inv_write_audit(...).
-- - No ambiguous identifiers: all PL/pgSQL vars are v_* and all columns are qualified.
-- ============================================================

-- ------------------------------------------------------------
-- 1) DB change (B1)
-- ------------------------------------------------------------
do $$
begin
  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name   = 'client_settings'
      and column_name  = 'reference_number_required_to_issue_invoice'
  ) then
    alter table public.client_settings
      add column reference_number_required_to_issue_invoice boolean not null default false;
  end if;
end $$;
-- ------------------------------------------------------------
-- 2) NEW helper: unlock selected segment refs for an invoice
-- ------------------------------------------------------------
create or replace function public._inv_unlock_segment_refs_for_invoice(
  p_invoice_id uuid,
  p_segment_refs jsonb,
  p_actor_user_id uuid default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_tsfin_id uuid;
  v_seg_ids text[];
  v_ib jsonb;
  v_out_segs jsonb;
  v_seg jsonb;
  v_unlocked int;
  v_total int;
  v_has_unlocked boolean;
  v_locked_ids text[];
  v_only_locked_text text;
  v_locked_by uuid;
  v_locked_at timestamptz;
  v_now timestamptz := now();
  v_reason text := 'INVOICE_DEBUG';
  v_action_ok text := 'INV_UNLOCK_SEGREFS_DEBUG';
  v_action_err text := 'INV_UNLOCK_SEGREFS_ERROR';
  v_sqlstate text;
  v_err text;
begin
  -- Load invoice_debug flag
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    limit 1;
  exception when others then
    v_invoice_debug := false;
  end;

  if p_invoice_id is null then
    raise exception 'p_invoice_id is required';
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    raise exception 'p_segment_refs must be a json array';
  end if;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'invoice_id', p_invoice_id::text,
      'ref_count', jsonb_array_length(p_segment_refs),
      'at_utc', public._inv_iso_utc(v_now)
    )
  );

  -- Group segment refs by tsfin_id
  for v_tsfin_id, v_seg_ids in
    select
      (nullif(btrim(coalesce(x->>'tsfin_id','')),''))::uuid as tsfin_id,
      array_agg(nullif(btrim(coalesce(x->>'segment_id','')),''))
        filter (where nullif(btrim(coalesce(x->>'segment_id','')), '') is not null) as segment_ids
    from jsonb_array_elements(p_segment_refs) x
    group by (nullif(btrim(coalesce(x->>'tsfin_id','')),''))::uuid
  loop
    if v_tsfin_id is null then
      raise exception 'segment_refs contains null/invalid tsfin_id';
    end if;
    if v_seg_ids is null or array_length(v_seg_ids,1) is null then
      raise exception 'segment_refs contains no segment_id for tsfin_id %', v_tsfin_id;
    end if;

    -- ✅ FIX: Lock the TSFIN row at read time (prevents lost-update delay corruption)
    select tf.invoice_breakdown_json
      into v_ib
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
      and tf.is_current = true
    for update;

    if not found then
      raise exception 'timesheets_financials row not found or not current for tsfin_id %', v_tsfin_id;
    end if;

    if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
      raise exception 'invoice_breakdown_json invalid for tsfin_id %', v_tsfin_id;
    end if;

    if upper(coalesce(v_ib->>'mode','')) <> 'SEGMENTS' then
      raise exception 'tsfin_id % is not SEGMENTS mode', v_tsfin_id;
    end if;

    if jsonb_typeof(v_ib->'segments') <> 'array' then
      raise exception 'tsfin_id % has no segments array', v_tsfin_id;
    end if;

    v_out_segs := '[]'::jsonb;
    v_unlocked := 0;

    for v_seg in
      select value
      from jsonb_array_elements(v_ib->'segments') value
    loop
      if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
        v_out_segs := v_out_segs || jsonb_build_array(v_seg);
      else
        if (coalesce(v_seg->>'segment_id','') = any(v_seg_ids))
           and (nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text) then
          v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', 'null'::jsonb, true);
          v_seg := jsonb_set(v_seg, '{invoice_locked_at_utc}', 'null'::jsonb, true);
          v_unlocked := v_unlocked + 1;
        end if;
        v_out_segs := v_out_segs || jsonb_build_array(v_seg);
      end if;
    end loop;

    -- Recompute lock summary invariant
    -- ✅ FIX: ignore invalid/non-object elements (and elements missing segment_id) so JSON nulls
    -- cannot create "phantom unlocked" or affect whole-timesheet lock inference.
    select
      count(*)::int,
      bool_or(nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') is null),
      array_agg(distinct nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), ''))
        filter (where nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') is not null)
      into v_total, v_has_unlocked, v_locked_ids
    from jsonb_array_elements(v_out_segs) e
    where jsonb_typeof(e) = 'object'
      and nullif(btrim(coalesce(e->>'segment_id','')), '') is not null;

    v_locked_by := null;
    v_locked_at := null;

    if coalesce(v_total,0) = 0 then
      v_locked_by := null;
      v_locked_at := null;
    else
      if (v_has_unlocked is false) and v_locked_ids is not null and array_length(v_locked_ids,1) = 1 then
        v_only_locked_text := v_locked_ids[1];

        if v_only_locked_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          v_locked_by := v_only_locked_text::uuid;
        else
          v_locked_by := null;
        end if;

        begin
          select min((nullif(btrim(coalesce(e->>'invoice_locked_at_utc','')), ''))::timestamptz)
            into v_locked_at
          from jsonb_array_elements(v_out_segs) e
          where jsonb_typeof(e) = 'object'
            and nullif(btrim(coalesce(e->>'segment_id','')), '') is not null
            and nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') = v_only_locked_text
            and nullif(btrim(coalesce(e->>'invoice_locked_at_utc','')), '') is not null;
        exception when others then
          v_locked_at := null;
        end;
      end if;
    end if;

    update public.timesheets_financials tf
    set
      invoice_breakdown_json = jsonb_set(tf.invoice_breakdown_json, '{segments}', v_out_segs, true),
      locked_by_invoice_id = v_locked_by,
      locked_at_utc = v_locked_at
    where tf.id = v_tsfin_id
      and tf.is_current = true;

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','tsfin_updated',
        'tsfin_id', v_tsfin_id::text,
        'segment_ids', to_jsonb(v_seg_ids),
        'segments_total', coalesce(v_total,0),
        'segments_unlocked_now', v_unlocked,
        'has_unlocked', coalesce(v_has_unlocked,false),
        'distinct_locked_invoice_ids', coalesce(array_length(v_locked_ids,1),0),
        'locked_by_invoice_id', case when v_locked_by is null then null else v_locked_by::text end,
        'locked_at_utc', case when v_locked_at is null then null else public._inv_iso_utc(v_locked_at) end
      )
    );

  end loop;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_ok,
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'segment_refs', p_segment_refs,
        'steps', v_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_err,
      jsonb_build_object(
        'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
        'segment_refs', p_segment_refs,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'steps', v_steps
      ),
      'invoices',
      case when p_invoice_id is null then null else p_invoice_id::text end,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  raise;
end;
$$;

-- ------------------------------------------------------------
-- 3) NEW helper: return segments on THIS invoice keyed by timesheet_id
-- ------------------------------------------------------------
create or replace function public._inv_segments_for_invoice(
  p_invoice_id uuid,
  p_actor_user_id uuid default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_now timestamptz := now();
  v_reason text := 'INVOICE_DEBUG';
  v_action_ok text := 'INV_SEGMENTS_FOR_INVOICE_DEBUG';
  v_action_err text := 'INV_SEGMENTS_FOR_INVOICE_ERROR';
  v_sqlstate text;
  v_err text;
  v_ts_count int := 0;
  v_return_keys int := 0;
begin
  -- Load invoice_debug flag
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    limit 1;
  exception when others then
    v_invoice_debug := false;
  end;

  if p_invoice_id is null then
    raise exception 'p_invoice_id is required';
  end if;

  select count(distinct l.timesheet_id)::int
    into v_ts_count
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'invoice_id', p_invoice_id::text,
      'timesheet_count_on_invoice', coalesce(v_ts_count,0),
      'at_utc', public._inv_iso_utc(v_now)
    )
  );

  select coalesce(
    jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object(
        'invoiced_segments', coalesce(t.invoiced_segments, '[]'::jsonb),
        'uninvoiced_segment_count', coalesce(t.uninvoiced_segment_count, 0),
        'locked_elsewhere_segment_count', coalesce(t.locked_elsewhere_segment_count, 0)
      )
    ),
    '{}'::jsonb
  )
  into v_result
  from (
    select
      ts.timesheet_id,
      coalesce(
        jsonb_agg(s.seg order by coalesce(s.seg->>'date',''), coalesce(s.seg->>'segment_id',''))
          filter (where s.locked_text = p_invoice_id::text),
        '[]'::jsonb
      ) as invoiced_segments,
      (count(*) filter (where s.locked_text is null))::int as uninvoiced_segment_count,
      (count(*) filter (where s.locked_text is not null and s.locked_text <> p_invoice_id::text))::int as locked_elsewhere_segment_count
    from (
      select distinct l.timesheet_id
      from public.invoice_lines l
      where l.invoice_id = p_invoice_id
        and l.timesheet_id is not null
    ) ts
    join public.timesheets_financials tf
      on tf.is_current = true
     and tf.timesheet_id = ts.timesheet_id
    left join lateral (
      select
        value as seg,
        nullif(btrim(coalesce(value->>'invoice_locked_invoice_id','')), '') as locked_text
      from jsonb_array_elements(
        case
          when jsonb_typeof(tf.invoice_breakdown_json) = 'object'
           and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
          then tf.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) value
    ) s on true
    where upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
    group by ts.timesheet_id
  ) t;

  select count(*)::int
    into v_return_keys
  from jsonb_object_keys(v_result) k;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','built',
      'return_timesheet_keys', coalesce(v_return_keys,0)
    )
  );

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_ok,
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'timesheet_count_on_invoice', coalesce(v_ts_count,0),
        'return_timesheet_keys', coalesce(v_return_keys,0),
        'steps', v_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  return v_result;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_err,
      jsonb_build_object(
        'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'steps', v_steps
      ),
      'invoices',
      case when p_invoice_id is null then null else p_invoice_id::text end,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  raise;
end;
$$;
-- ============================================================
-- CloudTMS Patch: public.invoice_reference_rows(p_invoice_id)
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================
create or replace function public.invoice_reference_rows(
  p_invoice_id uuid
)
returns table (
  timesheet_id uuid,
  sheet_scope text,
  submission_mode text,
  ref_target text,
  segment_id text,
  day_ymd text,
  start_utc text,
  end_utc text,
  current_reference text,
  is_required boolean,
  row_key text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (v_now at time zone 'Europe/London')::date;
  v_invoice_debug boolean := false;

  v_ts_ids uuid[] := array[]::uuid[];

  v_rows_out int := 0;
  v_ts_out int := 0;

  v_dbg_steps jsonb := '[]'::jsonb;

  r_ts record;
  r_seg record;
  r_day record;

  v_tf_mode text;
  v_segments_json jsonb;
  v_sched_json jsonb;
  v_dayrefs_json jsonb;

  v_seg_locked text;

  v_idx int;
  v_start_local text;
  v_end_local text;

  -- invoice-level worked content (HOURS + ADDITIONAL only) for this timesheet on THIS invoice
  v_inv_worked_charge_ex numeric := 0;
  v_inv_worked_hours_sum numeric := 0;

  -- required flag for UI display: true if refs required for either invoice or issue
  v_is_required boolean := false;

  -- per-segment positivity test
  v_seg_hours_sum numeric := 0;
  v_seg_charge_ex numeric := 0;

  -- SEGMENTS-mode: SEGMENT refs are sourced from TSFIN segments (invoice_breakdown_json)
  v_seg_id_local text;
  v_sched_ref text;
  v_sched_found boolean;

begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load invoice_debug flag safely
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  select array_agg(distinct il.timesheet_id)
    into v_ts_ids
  from public.invoice_lines il
  where il.invoice_id = p_invoice_id
    and il.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    return;
  end if;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step', 'load_timesheets',
    'timesheet_count', coalesce(array_length(v_ts_ids,1),0)
  ));

  for r_ts in
    select
      ts.timesheet_id as ts_id,
      ts.sheet_scope as ts_sheet_scope,
      ts.submission_mode as ts_submission_mode,
      ts.reference_number as ts_reference_number,
      ts.week_ending_date as ts_week_ending_date,
      ts.worked_start_iso as ts_worked_start_iso,
      ts.worked_end_iso as ts_worked_end_iso,
      ts.scheduled_start_iso as ts_scheduled_start_iso,
      ts.scheduled_end_iso as ts_scheduled_end_iso,
      ts.actual_schedule_json as ts_actual_schedule_json,
      ts.day_references_json as ts_day_references_json,
      tf.invoice_breakdown_json as tf_invoice_breakdown_json,
      pc.require_reference_to_invoice as pc_require_reference_to_invoice,
      pc.reference_number_required_to_issue_invoice as pc_ref_to_issue
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = ts.timesheet_id
    where ts.timesheet_id = any(v_ts_ids)
    order by ts.week_ending_date asc nulls last, ts.timesheet_id
  loop
    v_ts_out := v_ts_out + 1;

    -- Two independent policies: required-to-invoice and required-to-issue.
    -- For UI display we mark required if either is true.
    v_is_required := coalesce(r_ts.pc_require_reference_to_invoice,false)
                     or coalesce(r_ts.pc_ref_to_issue,false);

    -- Invoice-level worked content on THIS invoice for this timesheet:
    -- Only HOURS_* and ADDITIONAL_RATE* lines are considered "worked content".
    -- If net worked content is <= 0 (hours AND charge), no refs are required and no rows should be returned.
    select
      coalesce(sum(
        case
          when upper(coalesce(il.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY')
            then coalesce(il.total_charge_ex_vat,0)
          else 0
        end
      ),0),
      coalesce(sum(
        case
          when upper(coalesce(il.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY')
            then coalesce(il.hours_day,0)+coalesce(il.hours_night,0)+coalesce(il.hours_sat,0)+coalesce(il.hours_sun,0)+coalesce(il.hours_bh,0)
          else 0
        end
      ),0)
    into v_inv_worked_charge_ex, v_inv_worked_hours_sum
    from public.invoice_lines il
    where il.invoice_id = p_invoice_id
      and il.timesheet_id = r_ts.ts_id;

    if coalesce(v_inv_worked_charge_ex,0) <= 0
       and coalesce(v_inv_worked_hours_sum,0) <= 0 then
      continue;
    end if;

    v_tf_mode := upper(coalesce(r_ts.tf_invoice_breakdown_json->>'mode',''));
    v_segments_json := r_ts.tf_invoice_breakdown_json->'segments';
    v_sched_json := r_ts.ts_actual_schedule_json;
    v_dayrefs_json := r_ts.ts_day_references_json;

    -- A) SEGMENTS mode: per-shift rows, only locked to this invoice and net-positive.
    if v_tf_mode = 'SEGMENTS' and jsonb_typeof(v_segments_json) = 'array' then
      for r_seg in
        select value as seg
        from jsonb_array_elements(v_segments_json) value
        order by
          coalesce(value->>'start_utc',''),
          coalesce(value->>'segment_id','')
      loop
        v_seg_locked := nullif(btrim(coalesce(r_seg.seg->>'invoice_locked_invoice_id','')), '');
        if v_seg_locked is null or v_seg_locked <> p_invoice_id::text then
          continue;
        end if;

        begin
          v_seg_hours_sum :=
            coalesce(nullif(r_seg.seg->>'hours_day','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_night','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_sat','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_sun','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_bh','')::numeric,0);
        exception when others then
          v_seg_hours_sum := 0;
        end;

        begin
          v_seg_charge_ex := coalesce(nullif(r_seg.seg->>'charge_amount','')::numeric,0);
        exception when others then
          v_seg_charge_ex := 0;
        end;

        if coalesce(v_seg_hours_sum,0) <= 0 and coalesce(v_seg_charge_ex,0) <= 0 then
          continue;
        end if;

        v_seg_id_local := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');

        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'SEGMENT';

        day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
        start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
        end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

        -- Fallback: derive day_ymd from start_utc (Europe/London) if missing
        if day_ymd is null and start_utc is not null then
          begin
            day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
          exception when others then
            day_ymd := null;
          end;
        end if;

        -- Fallback: if segment_id missing, produce stable identifier from start/end (used for UI row identity).
        segment_id := v_seg_id_local;
        if segment_id is null and start_utc is not null and end_utc is not null then
          segment_id := 'SE:' || start_utc || '|' || end_utc;
        end if;

        -- SEGMENTS mode: current_reference is sourced from TSFIN segments (invoice_breakdown_json),
        -- which invoice_apply_edits keeps in sync when refs are edited.
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- B) WEEKLY MANUAL: schedule entries (start/end present)
    elsif r_ts.ts_sheet_scope::text = 'WEEKLY'
      and r_ts.ts_submission_mode::text = 'MANUAL'
      and jsonb_typeof(v_sched_json) = 'array'
    then
      for r_seg in
        select value as seg, ordinality as idx
        from jsonb_array_elements(v_sched_json) with ordinality
      loop
        v_start_local := nullif(btrim(coalesce(r_seg.seg->>'start','')), '');
        v_end_local   := nullif(btrim(coalesce(r_seg.seg->>'end','')), '');
        if v_start_local is null or v_end_local is null then
          continue;
        end if;

        v_idx := (r_seg.idx - 1);

        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'SEGMENT';
        segment_id := ('ts:' || r_ts.ts_id::text || ':' || v_idx::text);
        day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
        start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
        end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- C) WEEKLY non-MANUAL: per-key FREEFORM rows from day_references_json (excluding internal __ keys)
    elsif r_ts.ts_sheet_scope::text = 'WEEKLY'
      and r_ts.ts_submission_mode::text <> 'MANUAL'
      and jsonb_typeof(v_dayrefs_json) = 'object'
    then
      for r_day in
        select j.key as k, j.value as v
        from jsonb_each_text(v_dayrefs_json) as j(key, value)
        where left(coalesce(j.key,''), 2) <> '__'
        order by j.key
      loop
        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'FREEFORM';
        segment_id := null;
        day_ymd := nullif(btrim(coalesce(r_day.k,'')), '');
        start_utc := null;
        end_utc := null;
        current_reference := nullif(btrim(coalesce(r_day.v,'')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- D) DAILY or any other fallback: single timesheet-level ref
    else
      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'TIMESHEET';
      segment_id := null;
      day_ymd := null;

      if r_ts.ts_worked_start_iso is not null then
        day_ymd := ((r_ts.ts_worked_start_iso at time zone 'Europe/London')::date)::text;
      elsif r_ts.ts_scheduled_start_iso is not null then
        day_ymd := ((r_ts.ts_scheduled_start_iso at time zone 'Europe/London')::date)::text;
      elsif r_ts.ts_week_ending_date is not null then
        day_ymd := r_ts.ts_week_ending_date::text;
      end if;

      start_utc := coalesce(r_ts.ts_worked_start_iso::text, r_ts.ts_scheduled_start_iso::text);
      end_utc := coalesce(r_ts.ts_worked_end_iso::text, r_ts.ts_scheduled_end_iso::text);
      current_reference := nullif(btrim(coalesce(r_ts.ts_reference_number,'')), '');
      is_required := v_is_required;

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_rows_out := v_rows_out + 1;
      return next;
    end if;
  end loop;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_REFERENCE_ROWS_DEBUG',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'anchor_ymd', v_anchor_ymd::text,
        'timesheet_count', coalesce(array_length(v_ts_ids,1),0),
        'timesheets_scanned', v_ts_out,
        'rows_returned', v_rows_out,
        'steps', v_dbg_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

exception when others then
  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_REFERENCE_ROWS_ERROR',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'anchor_ymd', v_anchor_ymd::text,
        'sqlstate', sqlstate,
        'error', sqlerrm,
        'steps', v_dbg_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;
  raise;
end;
$$;

