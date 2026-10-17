create or replace function public.timesheet_pdf_reference_rows(
  p_timesheet_id uuid
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
  row_key text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r_ts record;
  r_seg record;

  v_tf_mode text;
  v_segments_json jsonb;
  v_sched_json jsonb;

  v_idx int;
  v_start_local text;
  v_end_local text;

  v_seg_id_local text;
  v_emitted int := 0;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  -- Load the timesheet + current TSFIN snapshot (for SEGMENTS-mode refs when applicable)
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
    tf.invoice_breakdown_json as tf_invoice_breakdown_json
  into r_ts
  from public.timesheets ts
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    return;
  end if;

  v_tf_mode := upper(coalesce(r_ts.tf_invoice_breakdown_json->>'mode',''));
  v_segments_json := r_ts.tf_invoice_breakdown_json->'segments';
  v_sched_json := r_ts.ts_actual_schedule_json;

  -- ------------------------------------------------------------
  -- A) SEGMENTS mode (NHSP/HR/etc): derive SEGMENT rows from TSFIN segments
  -- ------------------------------------------------------------
  if v_tf_mode = 'SEGMENTS'
     and jsonb_typeof(v_segments_json) = 'array'
  then
    for r_seg in
      select value as seg
      from jsonb_array_elements(v_segments_json) value
    loop
      if r_seg.seg is null or jsonb_typeof(r_seg.seg) <> 'object' then
        continue;
      end if;

      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'SEGMENT';

      day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
      start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
      end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      v_seg_id_local := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      segment_id := v_seg_id_local;

      if segment_id is null and start_utc is not null and end_utc is not null then
        segment_id := 'SE:' || start_utc || '|' || end_utc;
      end if;

      current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_emitted := v_emitted + 1;
      return next;
    end loop;

    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- B) WEEKLY schedule-based rows from timesheets.actual_schedule_json
  -- ------------------------------------------------------------
  if r_ts.ts_sheet_scope::text = 'WEEKLY'
     and jsonb_typeof(v_sched_json) = 'array'
  then
    for r_seg in
      select value as seg, ordinality as idx
      from jsonb_array_elements(v_sched_json) with ordinality
    loop
      if r_seg.seg is null or jsonb_typeof(r_seg.seg) <> 'object' then
        continue;
      end if;

      v_start_local := nullif(btrim(coalesce(r_seg.seg->>'start','')), '');
      v_end_local   := nullif(btrim(coalesce(r_seg.seg->>'end','')), '');

      start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
      end_utc   := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

      if (v_start_local is null or v_end_local is null)
         and (start_utc is null or end_utc is null)
      then
        continue;
      end if;

      v_idx := (r_seg.idx - 1);

      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'SEGMENT';

      segment_id := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      if segment_id is null then
        segment_id := ('ts:' || r_ts.ts_id::text || ':' || v_idx::text);
      end if;

      day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');

      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      current_reference :=
        nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
      if current_reference is null then
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'booking_ref','')), '');
      end if;

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_emitted := v_emitted + 1;
      return next;
    end loop;

    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- C) DAILY (or any fallback): single timesheet-level reference row
  -- ------------------------------------------------------------
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

  start_utc := coalesce(
    (to_jsonb(r_ts.ts_worked_start_iso)#>>'{}'),
    (to_jsonb(r_ts.ts_scheduled_start_iso)#>>'{}')
  );
  end_utc := coalesce(
    (to_jsonb(r_ts.ts_worked_end_iso)#>>'{}'),
    (to_jsonb(r_ts.ts_scheduled_end_iso)#>>'{}')
  );
  current_reference := nullif(btrim(coalesce(r_ts.ts_reference_number,'')), '');

  row_key := r_ts.ts_id::text
    || '|' || coalesce(ref_target,'')
    || '|' || coalesce(segment_id,'')
    || '|' || coalesce(day_ymd,'')
    || '|' || coalesce(start_utc,'')
    || '|' || coalesce(end_utc,'');

  return next;
end;
$$;