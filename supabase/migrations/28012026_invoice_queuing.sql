-- ============================================================
-- Option A: Targeted enqueue RPCs (enqueue one / enqueue many)
-- These make your backend handlers perfectly align to the new
-- invoice_pdf_outbox workflow without using PostgREST inserts.
-- ============================================================

create or replace function public.invpdf_enqueue_one(
  p_invoice_id uuid,
  p_force_regen boolean default false
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_reason public.invoice_pdf_reason_enum := case when p_force_regen then 'FORCE_REGEN' else 'READY_FOR_RENDER' end;
  v_updated int := 0;
  v_inserted int := 0;
begin
  if p_invoice_id is null then
    return 0;
  end if;

  -- If caller requests force regen, ensure we don't leave redundant rows behind.
  if p_force_regen then
    delete from public.invoice_pdf_outbox o
    where o.invoice_id = p_invoice_id;

    insert into public.invoice_pdf_outbox(
      invoice_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      force_regen,
      created_at
    )
    values (
      p_invoice_id,
      v_reason,
      0,
      v_now,
      null,
      true,
      v_now
    )
    on conflict (invoice_id, reason) do update
      set next_attempt_at = excluded.next_attempt_at,
          last_error      = null,
          force_regen     = true;

    return 1;
  end if;

  -- Non-force: if ANY job exists for this invoice (READY and/or FORCE),
  -- bump them to run asap (idempotent), without RETURNING-into-scalar issues.
  update public.invoice_pdf_outbox o
     set next_attempt_at = v_now,
         last_error      = null
   where o.invoice_id = p_invoice_id;

  get diagnostics v_updated = row_count;

  if coalesce(v_updated, 0) > 0 then
    return v_updated;
  end if;

  -- No job exists yet: create READY job.
  insert into public.invoice_pdf_outbox(
    invoice_id,
    reason,
    attempt_count,
    next_attempt_at,
    last_error,
    force_regen,
    created_at
  )
  values (
    p_invoice_id,
    v_reason,
    0,
    v_now,
    null,
    false,
    v_now
  )
  on conflict (invoice_id, reason) do update
    set next_attempt_at = excluded.next_attempt_at,
        last_error      = null,
        force_regen     = public.invoice_pdf_outbox.force_regen or excluded.force_regen;

  get diagnostics v_inserted = row_count;
  return coalesce(v_inserted, 0);
end;
$$;



create or replace function public.invpdf_enqueue_many(
  p_invoice_ids uuid[],
  p_force_regen boolean default false,
  p_limit int default 500
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));
  v_count int := 0;
  v_i int := 0;
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids, 1), 0) = 0 then
    return 0;
  end if;

  foreach v_id in array p_invoice_ids loop
    exit when v_i >= v_lim;
    v_i := v_i + 1;

    if v_id is null then
      continue;
    end if;

    v_count := v_count + public.invpdf_enqueue_one(v_id, p_force_regen);
  end loop;

  return v_count;
end;
$$;

-- ============================================================
-- INVOICE PDF Outbox — DB objects (mirror TS PDF outbox pattern)
-- ============================================================

-- Ensure gen_random_uuid() exists
create extension if not exists pgcrypto;

-- 1) Enum (idempotent)
do $$
begin
  if not exists (select 1 from pg_type where typname = 'invoice_pdf_reason_enum') then
    create type public.invoice_pdf_reason_enum as enum (
      'READY_FOR_RENDER',
      'FORCE_REGEN'
    );
  end if;
end $$;

-- 2) Table
create table if not exists public.invoice_pdf_outbox (
  id uuid primary key default gen_random_uuid(),

  invoice_id uuid not null references public.invoices(id) on delete cascade,
  reason public.invoice_pdf_reason_enum not null,

  attempt_count int not null default 0,
  next_attempt_at timestamptz null,
  last_error text null,

  force_regen boolean not null default false,

  created_at timestamptz not null default now()
);

-- 3) Indexes
create unique index if not exists uq_invoice_pdf_outbox_invoice_reason
  on public.invoice_pdf_outbox(invoice_id, reason);

create index if not exists idx_invoice_pdf_outbox_due
  on public.invoice_pdf_outbox(next_attempt_at, created_at);

-- ============================================================
-- INVOICE PDF Outbox RPCs (enqueue / dequeue / ack success / ack fail)
-- ============================================================

-- ------------------------------------------------------------
-- Enqueue invoices that are ISSUED and have no generated PDF key
-- (scheduler-safe + idempotent).
-- ------------------------------------------------------------
create or replace function public.invpdf_enqueue_ready_for_render(p_limit int default 500)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 2000));
begin
  with eligible as (
    select i.id as invoice_id
    from public.invoices i
    where i.status = 'ISSUED'::public.invoice_status_enum
      and (
        i.invoice_pdf_r2_key is null
        or btrim(i.invoice_pdf_r2_key) = ''
        or i.invoice_pdf_generated_at_utc is null
        or (i.updated_at is not null and i.invoice_pdf_generated_at_utc is not null and i.updated_at > i.invoice_pdf_generated_at_utc)
      )
      -- Don't enqueue if ANY outbox row already exists for this invoice (READY or FORCE)
      and not exists (
        select 1
        from public.invoice_pdf_outbox o
        where o.invoice_id = i.id
      )
    order by i.updated_at desc nulls last
    limit v_lim
  ),
  ins as (
    insert into public.invoice_pdf_outbox(
      invoice_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      force_regen,
      created_at
    )
    select
      e.invoice_id,
      'READY_FOR_RENDER'::public.invoice_pdf_reason_enum,
      0,
      v_now,
      null,
      false,
      v_now
    from eligible e
    on conflict (invoice_id, reason) do nothing
    returning 1
  )
  select count(*) into v_ins from ins;

  return v_ins;
end;
$$;

-- ------------------------------------------------------------
-- Dequeue batch (lease rows deterministically, SKIP LOCKED)
-- Returns leased rows to worker; increments attempt_count and
-- schedules next_attempt_at for retry window.
-- ------------------------------------------------------------
create or replace function public.invpdf_dequeue_batch_ids(p_limit int default 10)
returns table (
  outbox_id uuid,
  invoice_id uuid,
  reason public.invoice_pdf_reason_enum,
  attempt_count int,
  next_attempt_at timestamptz,
  created_at timestamptz,
  force_regen boolean
)
language plpgsql
as $$
declare
  v_now timestamptz := now();
  v_lim int := greatest(1, least(coalesce(p_limit, 10), 200));
begin
  return query
  with picked as (
    select o.id
    from public.invoice_pdf_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  )
  update public.invoice_pdf_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning
    o.id as outbox_id,
    o.invoice_id,
    o.reason,
    o.attempt_count,
    o.next_attempt_at,
    o.created_at,
    o.force_regen;
end;
$$;

-- ------------------------------------------------------------
-- Bulk success ack: delete outbox rows
-- ------------------------------------------------------------
create or replace function public.invpdf_work_success_bulk(p_ids uuid[])
returns int
language plpgsql
as $$
declare
  v_count int := 0;
begin
  if p_ids is null or coalesce(array_length(p_ids, 1), 0) = 0 then
    return 0;
  end if;

  with gone as (
    delete from public.invoice_pdf_outbox o
    where o.id = any(p_ids)
    returning 1
  )
  select count(*) into v_count
  from gone;

  return v_count;
end;
$$;

-- ------------------------------------------------------------
-- Bulk fail ack:
-- p_rows is JSONB array of objects: [{ "outbox_id": "...", "error": "..." }, ...]
-- ------------------------------------------------------------
create or replace function public.invpdf_work_fail_bulk(p_rows jsonb)
returns int
language plpgsql
as $$
declare
  v_now timestamptz := now();
  v_count int := 0;
  r record;
begin
  if p_rows is null then return 0; end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid as outbox_id,
      left(coalesce(elem->>'error',''), 4000) as err
    from jsonb_array_elements(p_rows) as elem
  loop
    update public.invoice_pdf_outbox o
    set last_error = r.err,
        next_attempt_at = v_now + interval '30 minutes'
    where o.id = r.outbox_id;

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;
-- ============================================================
-- CloudTMS: public.timesheet_pdf_reference_rows(p_timesheet_id)
-- Returns the per-row reference items that a timesheet PDF would reflect,
-- using the SAME row_key model as public.invoice_reference_rows.
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================
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
  -- (timesheet-level, not invoice-scoped)
  -- Mirrors invoice_reference_rows SEGMENTS extraction (but without invoice lock filtering).
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

      -- Fallback: derive day_ymd from start_utc (Europe/London) if missing
      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      v_seg_id_local := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      segment_id := v_seg_id_local;

      -- Fallback: if segment_id missing, produce stable identifier from start/end (as in invoice_reference_rows)
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

    -- If we emitted at least one segment row, we are done.
    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- B) WEEKLY schedule-based rows from timesheets.actual_schedule_json
  -- Mirrors the segment row identity pattern used in invoice_reference_rows:
  --   - segment_id: use seg.segment_id if present, else ts:<timesheet_id>:<idx>
  --   - uses stored start_utc/end_utc if present
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

      -- Require a usable time window (either local start/end OR utc start/end)
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

      -- Prefer explicit segment_id from the object; else stable index-based id (as in invoice_reference_rows)
      segment_id := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');
      if segment_id is null then
        segment_id := ('ts:' || r_ts.ts_id::text || ':' || v_idx::text);
      end if;

      day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');

      -- Fallback: derive day_ymd from start_utc (Europe/London) if missing
      if day_ymd is null and start_utc is not null then
        begin
          day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
        exception when others then
          day_ymd := null;
        end;
      end if;

      -- Booking Ref: prefer ref_num, else booking_ref
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

    -- If we emitted at least one schedule row, we are done.
    if v_emitted > 0 then
      return;
    end if;
  end if;

  -- ------------------------------------------------------------
  -- C) DAILY (or any fallback): single timesheet-level reference row
  -- Mirrors invoice_reference_rows fallback branch exactly.
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

  start_utc := coalesce(r_ts.ts_worked_start_iso::text, r_ts.ts_scheduled_start_iso::text);
  end_utc := coalesce(r_ts.ts_worked_end_iso::text, r_ts.ts_scheduled_end_iso::text);
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
-- ============================================================
-- CloudTMS: public.timesheet_pdf_reference_sig(p_timesheet_id)
-- Canonical sha256 signature for current reference rows.
--
-- Signature algorithm:
--   raw_sig = string_agg(row_key || '=' || coalesce(current_reference,''), '||' ORDER BY row_key)
--   sig     = encode(extensions.digest(raw_sig,'sha256'),'hex')
--
-- Depends on: public.timesheet_pdf_reference_rows(uuid)
--             (must expose columns: row_key, current_reference)
-- ============================================================
create or replace function public.timesheet_pdf_reference_sig(
  p_timesheet_id uuid
)
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_raw_sig text;
  v_sig text;
begin
  if p_timesheet_id is null then
    return null;
  end if;

  select
    coalesce(
      string_agg(
        (r.row_key || '=' || coalesce(r.current_reference, '')),
        '||'
        order by r.row_key
      ),
      ''
    )
  into v_raw_sig
  from public.timesheet_pdf_reference_rows(p_timesheet_id) r;

  select
    encode(
      extensions.digest(coalesce(v_raw_sig, ''), 'sha256'),
      'hex'
    )
  into v_sig;

  return v_sig;
end;
$$;
