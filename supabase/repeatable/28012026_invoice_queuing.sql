
CREATE OR REPLACE FUNCTION public.trg_timesheets_enqueue_pdf_regen_on_refs_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_contract_id uuid := null;
  v_client_id uuid := null;

  v_overrideclientsettings boolean := false;
  v_no_timesheet_required boolean := false;
  v_is_nhsp boolean := false;

  v_as_of_date date := null;
begin
  -- CURRENT only
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- Ignore revoked
  if new.revoked_at is not null then
    return new;
  end if;

  -- ✅ Signed QR evidence is immutable: never enqueue regen for signed markers
  if new.qr_scanned_at is not null
     or new.qr_signed_hash is not null
     or new.qr_signed_at_utc is not null
  then
    return new;
  end if;

  -- ELECTRONIC only (generated PDF path)
  if upper(coalesce(new.submission_mode::text, '')) <> 'ELECTRONIC' then
    return new;
  end if;

  -- Manual PDF overrides mean we don't own the canonical generated PDF
  if new.manual_pdf_r2_key is not null then
    return new;
  end if;

  -- If no generated PDF baseline exists, do nothing (per spec)
  if new.generated_pdf_at_utc is null then
    return new;
  end if;

  -- Determine an "as-of" date for client_settings effective_from resolution
  v_as_of_date := coalesce(
    new.week_ending_date,
    ((new.worked_start_iso at time zone 'Europe/London')::date),
    ((new.scheduled_start_iso at time zone 'Europe/London')::date),
    (now() at time zone 'Europe/London')::date
  );

  -- Resolve contract_id (timesheets.contract_id or from contract_weeks)
  v_contract_id := new.contract_id;

  if v_contract_id is null then
    select cw.contract_id
      into v_contract_id
    from public.contract_weeks cw
    where cw.timesheet_id = new.timesheet_id
    limit 1;
  end if;

  -- Contract override path (only when overrideclientsettings=true)
  if v_contract_id is not null then
    select
      coalesce(ct.overrideclientsettings, false),
      coalesce(ct.no_timesheet_required, false),
      coalesce(ct.is_nhsp, false),
      ct.client_id
    into
      v_overrideclientsettings,
      v_no_timesheet_required,
      v_is_nhsp,
      v_client_id
    from public.contracts ct
    where ct.id = v_contract_id
    limit 1;

    if coalesce(v_overrideclientsettings, false) = true then
      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- If overrideclientsettings is FALSE, use the effective client_settings row (NOT bool_or across history)
  if coalesce(v_overrideclientsettings, false) = false then
    -- If client_id still unknown, fall back to current TSFIN client_id
    if v_client_id is null then
      select tf.client_id
        into v_client_id
      from public.timesheets_financials tf
      where tf.timesheet_id = new.timesheet_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;
    end if;

    if v_client_id is not null then
      select
        coalesce(csx.no_timesheet_required, false),
        coalesce(csx.is_nhsp, false)
      into
        v_no_timesheet_required,
        v_is_nhsp
      from public.client_settings csx
      where csx.client_id = v_client_id
        and (csx.effective_from is null or csx.effective_from <= v_as_of_date)
      order by csx.effective_from desc nulls last,
               csx.updated_at desc nulls last,
               csx.created_at desc nulls last
      limit 1;

      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- ✅ Enqueue-only (regen-check): no signature computation or dirty comparison in-trigger
  perform public.tspdf_enqueue_one(
    p_timesheet_id := new.timesheet_id,
    p_force_regen := false,
    p_prefer_generated := true,
    p_reason := 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
  );

  return new;
end;
$function$;



CREATE OR REPLACE FUNCTION public.trg_tsfin_enqueue_tspdf_on_refs_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_timesheet_id uuid := null;

  v_ts record;

  v_contract_id uuid := null;
  v_client_id uuid := null;

  v_overrideclientsettings boolean := false;
  v_no_timesheet_required boolean := false;
  v_is_nhsp boolean := false;

  v_as_of_date date := null;
begin
  -- Only for current TSFIN rows
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- Avoid work if UPDATE didn't actually change the JSON (defence-in-depth)
  if tg_op = 'UPDATE' then
    if new.invoice_breakdown_json is not distinct from old.invoice_breakdown_json then
      return new;
    end if;
  end if;

  v_timesheet_id := new.timesheet_id;
  if v_timesheet_id is null then
    return new;
  end if;

  -- Load current timesheet row (needed for submission_mode + baseline + gating)
  select
    ts.timesheet_id,
    ts.is_current,
    ts.revoked_at,
    ts.submission_mode,
    ts.manual_pdf_r2_key,
    ts.generated_pdf_at_utc,
    ts.contract_id,
    ts.week_ending_date,
    ts.worked_start_iso,
    ts.scheduled_start_iso,
    ts.qr_scanned_at,
    ts.qr_signed_hash,
    ts.qr_signed_at_utc
  into v_ts
  from public.timesheets ts
  where ts.timesheet_id = v_timesheet_id
    and ts.is_current = true
  limit 1;

  if not found then
    return new;
  end if;

  -- Ignore revoked
  if v_ts.revoked_at is not null then
    return new;
  end if;

  -- ✅ Signed QR evidence is immutable: never enqueue regen for signed markers
  if v_ts.qr_scanned_at is not null
     or v_ts.qr_signed_hash is not null
     or v_ts.qr_signed_at_utc is not null
  then
    return new;
  end if;

  -- ELECTRONIC only
  if upper(coalesce(v_ts.submission_mode::text, '')) <> 'ELECTRONIC' then
    return new;
  end if;

  -- Manual override means we don't own the generated PDF
  if v_ts.manual_pdf_r2_key is not null then
    return new;
  end if;

  -- If no baseline exists, do nothing (match spec)
  if v_ts.generated_pdf_at_utc is null then
    return new;
  end if;

  -- Determine as-of date for effective client_settings lookup
  v_as_of_date := coalesce(
    v_ts.week_ending_date,
    ((v_ts.worked_start_iso at time zone 'Europe/London')::date),
    ((v_ts.scheduled_start_iso at time zone 'Europe/London')::date),
    (now() at time zone 'Europe/London')::date
  );

  -- ------------------------------------------------------------
  -- Exclusion: do not enqueue when timesheet PDF is not required
  -- (NHSP / no_timesheet_required), respecting contract overrides.
  -- Mirrors the timesheets trigger precedence.
  -- ------------------------------------------------------------
  v_contract_id := v_ts.contract_id;

  if v_contract_id is null then
    select cw.contract_id
      into v_contract_id
    from public.contract_weeks cw
    where cw.timesheet_id = v_timesheet_id
    limit 1;
  end if;

  if v_contract_id is not null then
    select
      coalesce(ct.overrideclientsettings, false),
      coalesce(ct.no_timesheet_required, false),
      coalesce(ct.is_nhsp, false),
      ct.client_id
    into
      v_overrideclientsettings,
      v_no_timesheet_required,
      v_is_nhsp,
      v_client_id
    from public.contracts ct
    where ct.id = v_contract_id
    limit 1;

    if coalesce(v_overrideclientsettings, false) = true then
      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- If overrideclientsettings is FALSE, use effective client_settings row
  if coalesce(v_overrideclientsettings, false) = false then
    -- Prefer contract client_id, else TSFIN client_id
    if v_client_id is null then
      v_client_id := new.client_id;
    end if;

    if v_client_id is not null then
      select
        coalesce(csx.no_timesheet_required, false),
        coalesce(csx.is_nhsp, false)
      into
        v_no_timesheet_required,
        v_is_nhsp
      from public.client_settings csx
      where csx.client_id = v_client_id
        and (csx.effective_from is null or csx.effective_from <= v_as_of_date)
      order by csx.effective_from desc nulls last,
               csx.updated_at desc nulls last,
               csx.created_at desc nulls last
      limit 1;

      if coalesce(v_no_timesheet_required, false) = true
         or coalesce(v_is_nhsp, false) = true
      then
        return new;
      end if;
    end if;
  end if;

  -- ✅ Enqueue-only (regen-check): no signature computation or dirty comparison in-trigger
  perform public.tspdf_enqueue_one(
    p_timesheet_id := v_timesheet_id,
    p_force_regen := false,
    p_prefer_generated := true,
    p_reason := 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
  );

  return new;
end;
$function$;


-- ============================================================
-- INVOICE PDF Outbox — DB objects (mirror TS PDF outbox pattern)
-- ============================================================

-- Ensure gen_random_uuid() exists
create extension if not exists pgcrypto;

-- 1) Enum (idempotent)  ✅ MUST exist before any function that references it
do $$
begin
  if not exists (select 1 from pg_type where typname = 'invoice_pdf_reason_enum') then
    create type public.invoice_pdf_reason_enum as enum (
      'READY_FOR_RENDER',
      'FORCE_REGEN'
    );
  end if;
end $$;

-- 2) Table  ✅ create before functions (avoids check_function_bodies issues)
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



-- ============================================================
-- CloudTMS: public.timesheet_pdf_reference_sig(p_timesheet_id)
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

-- ============================================================
-- Trigger: timesheets → enqueue regen when refs truth changes
-- ============================================================

DROP TRIGGER IF EXISTS trg_timesheets_enqueue_pdf_regen_on_refs_change ON public.timesheets;

CREATE TRIGGER trg_timesheets_enqueue_pdf_regen_on_refs_change
AFTER UPDATE OF
  reference_number,
  day_references_json,
  actual_schedule_json,
  worked_start_iso,
  worked_end_iso,
  scheduled_start_iso,
  scheduled_end_iso,
  week_ending_date
ON public.timesheets
FOR EACH ROW
WHEN (
  COALESCE(NEW.is_current,false) = true
  AND COALESCE(OLD.is_current,false) = true
  AND (
    NEW.reference_number        IS DISTINCT FROM OLD.reference_number
    OR NEW.day_references_json  IS DISTINCT FROM OLD.day_references_json
    OR NEW.actual_schedule_json IS DISTINCT FROM OLD.actual_schedule_json
    OR NEW.worked_start_iso     IS DISTINCT FROM OLD.worked_start_iso
    OR NEW.worked_end_iso       IS DISTINCT FROM OLD.worked_end_iso
    OR NEW.scheduled_start_iso  IS DISTINCT FROM OLD.scheduled_start_iso
    OR NEW.scheduled_end_iso    IS DISTINCT FROM OLD.scheduled_end_iso
    OR NEW.week_ending_date     IS DISTINCT FROM OLD.week_ending_date
  )
)
EXECUTE FUNCTION public.trg_timesheets_enqueue_pdf_regen_on_refs_change();

-- ============================================================
-- Triggers: TSFIN → enqueue regen when invoice_breakdown_json changes
-- (FIX: drop both AI/AU triggers too, otherwise reruns fail)
-- ============================================================

DROP TRIGGER IF EXISTS trg_tsfin_enqueue_tspdf_on_refs_change ON public.timesheets_financials;
DROP TRIGGER IF EXISTS trg_tsfin_enqueue_tspdf_on_refs_change_ai ON public.timesheets_financials;
DROP TRIGGER IF EXISTS trg_tsfin_enqueue_tspdf_on_refs_change_au ON public.timesheets_financials;

-- INSERT: new current TSFIN rows
CREATE TRIGGER trg_tsfin_enqueue_tspdf_on_refs_change_ai
AFTER INSERT ON public.timesheets_financials
FOR EACH ROW
WHEN (COALESCE(NEW.is_current,false) = true)
EXECUTE FUNCTION public.trg_tsfin_enqueue_tspdf_on_refs_change();

-- UPDATE: only when invoice_breakdown_json truly changes
CREATE TRIGGER trg_tsfin_enqueue_tspdf_on_refs_change_au
AFTER UPDATE OF invoice_breakdown_json ON public.timesheets_financials
FOR EACH ROW
WHEN (
  COALESCE(NEW.is_current,false) = true
  AND NEW.invoice_breakdown_json IS DISTINCT FROM OLD.invoice_breakdown_json
)
EXECUTE FUNCTION public.trg_tsfin_enqueue_tspdf_on_refs_change();

-- ============================================================
-- (26) NEW: public.timesheet_doc_flags_batch(p_timesheet_ids uuid[])
-- ============================================================

create or replace function public.timesheet_doc_flags_batch(
  p_timesheet_ids uuid[]
)
returns table (
  timesheet_id uuid,
  candidate_id uuid,
  candidate_name text,
  client_id uuid,
  client_name text,
  sheet_scope text,
  week_ending_date date,

  qr_status text,
  qr_token text,
  qr_generated_at timestamptz,
  qr_scanned_at timestamptz,
  qr_signed_hash text,

  current_refs_sig text,
  qr_sent_refs_sig text,
  generated_pdf_refs_sig text,

  qr_refs_changed boolean,
  electronic_refs_changed boolean
)
language sql
stable
security definer
set search_path = public
as $$
with wanted as (
  select distinct unnest(p_timesheet_ids) as timesheet_id
  where p_timesheet_ids is not null
),
t as (
  select
    ts.timesheet_id,
    ts.sheet_scope,
    ts.submission_mode,
    ts.week_ending_date,
    ts.contract_id,

    ts.qr_status,
    ts.qr_token,
    ts.qr_generated_at,
    ts.qr_scanned_at,
    ts.qr_signed_hash,

    ts.qr_sent_refs_sig,
    ts.generated_pdf_at_utc,
    ts.generated_pdf_refs_sig,

    ts.occupant_key_norm,
    ts.hospital_norm
  from wanted w
  join public.timesheets ts
    on ts.timesheet_id = w.timesheet_id
   and ts.is_current = true
),
tf as (
  select
    tf0.timesheet_id,
    tf0.candidate_id,
    tf0.client_id
  from public.timesheets_financials tf0
  join wanted w
    on w.timesheet_id = tf0.timesheet_id
  where tf0.is_current = true
),
ct as (
  select
    t0.timesheet_id,
    ct0.candidate_id as contract_candidate_id,
    ct0.client_id as contract_client_id
  from t t0
  left join public.contracts ct0
    on ct0.id = t0.contract_id
),
vs as (
  select
    v.timesheet_id,
    v.candidate_id as vs_candidate_id,
    v.client_id as vs_client_id,
    v.candidate_name as vs_candidate_name,
    v.client_name as vs_client_name
  from t
  left join public.v_timesheets_summary v
    on v.timesheet_id = t.timesheet_id
),
ids as (
  select
    t1.timesheet_id,
    coalesce(vs1.vs_candidate_id, tf1.candidate_id, ct1.contract_candidate_id) as eff_candidate_id,
    coalesce(vs1.vs_client_id,    tf1.client_id,    ct1.contract_client_id)    as eff_client_id,
    vs1.vs_candidate_name,
    vs1.vs_client_name
  from t t1
  left join vs vs1
    on vs1.timesheet_id = t1.timesheet_id
  left join tf tf1
    on tf1.timesheet_id = t1.timesheet_id
  left join ct ct1
    on ct1.timesheet_id = t1.timesheet_id
),
cand as (
  select c0.id, c0.display_name
  from public.candidates c0
),
cli as (
  select cl0.id, cl0.name
  from public.clients cl0
),
sig as (
  select
    t2.timesheet_id,
    public.timesheet_pdf_reference_sig(t2.timesheet_id) as current_refs_sig
  from t t2
)
select
  t3.timesheet_id,

  ids2.eff_candidate_id as candidate_id,
  coalesce(ids2.vs_candidate_name, c2.display_name, t3.occupant_key_norm) as candidate_name,

  ids2.eff_client_id as client_id,
  coalesce(ids2.vs_client_name, cl2.name, t3.hospital_norm) as client_name,

  t3.sheet_scope::text as sheet_scope,
  t3.week_ending_date as week_ending_date,

  t3.qr_status::text as qr_status,
  nullif(btrim(coalesce(t3.qr_token, '')), '') as qr_token,
  t3.qr_generated_at,
  t3.qr_scanned_at,
  nullif(btrim(coalesce(t3.qr_signed_hash, '')), '') as qr_signed_hash,

  s3.current_refs_sig as current_refs_sig,
  nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') as qr_sent_refs_sig,
  nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') as generated_pdf_refs_sig,

  (
    upper(coalesce(t3.qr_status::text, '')) = 'PENDING'
    and nullif(btrim(coalesce(t3.qr_token, '')), '') is not null
    and t3.qr_generated_at is not null
    and t3.qr_scanned_at is null
    and nullif(btrim(coalesce(t3.qr_signed_hash, '')), '') is null
    and (
      nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') is null
      or nullif(btrim(coalesce(t3.qr_sent_refs_sig, '')), '') is distinct from s3.current_refs_sig
    )
  ) as qr_refs_changed,

  (
    upper(coalesce(t3.submission_mode::text, '')) = 'ELECTRONIC'
    and t3.generated_pdf_at_utc is not null
    and (
      nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') is null
      or nullif(btrim(coalesce(t3.generated_pdf_refs_sig, '')), '') is distinct from s3.current_refs_sig
    )
  ) as electronic_refs_changed
from t t3
left join ids ids2
  on ids2.timesheet_id = t3.timesheet_id
left join cand c2
  on c2.id = ids2.eff_candidate_id
left join cli cl2
  on cl2.id = ids2.eff_client_id
join sig s3
  on s3.timesheet_id = t3.timesheet_id
order by t3.timesheet_id;
$$;

-- ============================================================
-- (27) NEW: public.tspdf_enqueue_one / public.tspdf_enqueue_many
-- ============================================================

create or replace function public.tspdf_enqueue_one(
  p_timesheet_id uuid,
  p_force_regen boolean default false,
  p_prefer_generated boolean default true,
  p_reason public.ts_pdf_reason_enum default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_reason public.ts_pdf_reason_enum;
  v_updated int := 0;
  v_inserted int := 0;
begin
  if p_timesheet_id is null then
    return 0;
  end if;

  if coalesce(p_force_regen, false) is true then
    v_reason := 'FORCE_REGEN'::public.ts_pdf_reason_enum;
  else
    v_reason := coalesce(p_reason, 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum);
  end if;

  if coalesce(p_force_regen, false) is true then
    delete from public.ts_pdfs_outbox o
    where o.timesheet_id = p_timesheet_id;

    insert into public.ts_pdfs_outbox(
      timesheet_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      prefer_generated,
      force_regen,
      created_at
    )
    values (
      p_timesheet_id,
      v_reason,
      0,
      null,
      null,
      coalesce(p_prefer_generated, true),
      true,
      v_now
    )
    on conflict (timesheet_id, reason)
    do update
      set attempt_count    = 0,
          next_attempt_at  = null,
          last_error       = null,
          prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
          force_regen      = true;

    return 1;
  end if;

  update public.ts_pdfs_outbox o
     set attempt_count    = 0,
         next_attempt_at  = null,
         last_error       = null,
         prefer_generated = o.prefer_generated or coalesce(p_prefer_generated, false),
         force_regen      = o.force_regen
   where o.timesheet_id = p_timesheet_id;

  get diagnostics v_updated = row_count;

  if coalesce(v_updated, 0) > 0 then
    return v_updated;
  end if;

  insert into public.ts_pdfs_outbox(
    timesheet_id,
    reason,
    attempt_count,
    next_attempt_at,
    last_error,
    prefer_generated,
    force_regen,
    created_at
  )
  values (
    p_timesheet_id,
    v_reason,
    0,
    null,
    null,
    coalesce(p_prefer_generated, false),
    false,
    v_now
  )
  on conflict (timesheet_id, reason)
  do update
    set attempt_count    = 0,
        next_attempt_at  = null,
        last_error       = null,
        prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
        force_regen      = public.ts_pdfs_outbox.force_regen or excluded.force_regen;

  get diagnostics v_inserted = row_count;
  return coalesce(v_inserted, 0);
end;
$$;



create or replace function public.tspdf_enqueue_many(
  p_timesheet_ids uuid[],
  p_force_regen boolean default false,
  p_prefer_generated boolean default true,
  p_reason public.ts_pdf_reason_enum default null,
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
  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids, 1), 0) = 0 then
    return 0;
  end if;

  foreach v_id in array p_timesheet_ids loop
    exit when v_i >= v_lim;
    v_i := v_i + 1;

    if v_id is null then
      continue;
    end if;

    v_count := v_count + public.tspdf_enqueue_one(
      p_timesheet_id := v_id,
      p_force_regen := coalesce(p_force_regen, false),
      p_prefer_generated := coalesce(p_prefer_generated, true),
      p_reason := p_reason
    );
  end loop;

  return v_count;
end;
$$;


DROP TRIGGER IF EXISTS trg_timesheets_invalidate_prevalidation_on_change ON public.timesheets;

CREATE TRIGGER trg_timesheets_invalidate_prevalidation_on_change
AFTER UPDATE OF
  worked_start_iso,
  worked_end_iso,
  break_start_iso,
  break_end_iso,
  break_minutes,
  reference_number,
  day_references_json,
  actual_schedule_json,
  additional_units_week,
  additional_units_per_day
ON public.timesheets
FOR EACH ROW
WHEN (
  COALESCE(NEW.is_current,false) = true
  AND COALESCE(OLD.is_current,false) = true
)
EXECUTE FUNCTION public.timesheets_invalidate_prevalidation_on_change();

CREATE OR REPLACE FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_source_method text := UPPER(BTRIM(COALESCE(p_source_method, '')));
  v_target_method text := UPPER(BTRIM(COALESCE(p_target_method, '')));
  v_candidate_current_method text;
  v_latest_source_change_seq bigint := 0;
  v_authoritative_sessions_json jsonb := '[]'::jsonb;
  v_replaced_source_session_ids_json jsonb := '[]'::jsonb;
  v_represented_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_authorised_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_active_advance_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_retained_finance_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_target_details_json jsonb := '[]'::jsonb;
  v_preview_row_count integer := 0;
  v_source_target_mismatch_count integer := 0;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_route_operation_id_text text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_operation_id', true), '')), '');
  v_route_actor_user_id_text text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_actor_user_id', true), '')), '');
  v_route_reason text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_reason', true), '')), '');
  v_route_operation_id uuid := NULL::uuid;
  v_route_actor_user_id uuid := NULL::uuid;
  v_route_actor_role text := NULL::text;
  v_route_job_id uuid := NULL::uuid;
  v_route_job_status text := NULL::text;
  v_route_job_payload jsonb := '{}'::jsonb;
  v_route_dedupe_key text := NULL::text;
  v_route_job_count integer := 0;
  v_route_existing_raw_count integer := 0;
  v_route_existing_invalid_count integer := 0;
  v_route_existing_duplicate_count integer := 0;
  v_route_existing_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_source_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_SOURCE_METHOD_INVALID'
      USING ERRCODE = '22023', DETAIL = COALESCE(p_source_method, '');
  END IF;

  IF v_target_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_METHOD_INVALID'
      USING ERRCODE = '22023', DETAIL = COALESCE(p_target_method, '');
  END IF;

  IF v_source_method = v_target_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_METHODS_MUST_DIFFER'
      USING ERRCODE = '22023';
  END IF;

  SELECT UPPER(BTRIM(COALESCE(candidate_row.pay_method, '')))
  INTO v_candidate_current_method
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_NOT_FOUND'
      USING ERRCODE = 'P0002', DETAIL = p_candidate_id::text;
  END IF;

  -- Preview/plan calls must be bound to the candidate's current committed
  -- source route.  The dedicated apply transaction is the only exception:
  -- its trigger calls this helper after the candidate row has already moved
  -- to the target route and supplies the operation/actor context through
  -- transaction-local settings.
  IF v_route_operation_id_text IS NULL
     AND v_route_actor_user_id_text IS NULL
     AND v_candidate_current_method IS DISTINCT FROM v_source_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_SOURCE_METHOD_STALE'
      USING ERRCODE = '40001',
            DETAIL = jsonb_build_object(
              'candidate_id', p_candidate_id::text,
              'expected_source_method', v_source_method,
              'current_candidate_method', v_candidate_current_method,
              'target_method', v_target_method,
              'refresh_required', true
            )::text;
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_latest_source_change_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_latest_source_change_seq := COALESCE(v_latest_source_change_seq, 0);

  WITH candidate_open_scope AS (
    SELECT
      workbench_session.id AS session_id,
      workbench_session.actor_user_id,
      workbench_session.pay_date,
      workbench_session.week_ending_cutoff,
      workbench_session.version AS session_version,
      workbench_session.progress_state,
      workbench_session.progress_counter_version,
      workbench_session.updated_at_utc,
      MAX(workbench_session.pay_date) OVER (
        PARTITION BY workbench_session.actor_user_id
      ) AS latest_actor_open_pay_date
    FROM public.banking_pay_workbench_session_scope AS session_scope
    JOIN public.banking_pay_workbench_sessions AS workbench_session
      ON workbench_session.id = session_scope.session_id
    WHERE UPPER(BTRIM(COALESCE(workbench_session.status, ''))) = 'OPEN'
      AND workbench_session.discarded_at_utc IS NULL
      AND workbench_session.replacement_session_id IS NULL
      AND session_scope.candidate_id = p_candidate_id
  ),
  authoritative_sessions AS (
    SELECT DISTINCT
      candidate_scope.session_id,
      candidate_scope.actor_user_id,
      candidate_scope.pay_date,
      candidate_scope.week_ending_cutoff,
      candidate_scope.session_version,
      candidate_scope.progress_state,
      candidate_scope.progress_counter_version,
      candidate_scope.updated_at_utc
    FROM candidate_open_scope AS candidate_scope
    WHERE candidate_scope.pay_date = candidate_scope.latest_actor_open_pay_date
  ),
  retained_case_scope AS (
    SELECT
      finance_case.id AS finance_case_id,
      finance_case.linked_timesheet_id,
      finance_case.status,
      finance_case.outstanding_amount,
      finance_case.updated_at
    FROM public.pay_advances AS finance_case
    WHERE finance_case.candidate_id = p_candidate_id
      AND finance_case.written_off_at_utc IS NULL
      AND (
        finance_case.advance_kind IN (
          'OVERPAYMENT'::public.pay_advance_kind_enum,
          'UNDERPAYMENT'::public.pay_advance_kind_enum
        )
        OR finance_case.case_type IN (
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'UNDERPAYMENT'::public.pay_finance_case_type_enum
        )
      )
  ),
  retained_finance_authority_rows AS (
    -- Current open finance authority.
    SELECT DISTINCT
      COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(finance_component.source_pay_method, ''))), '') AS source_pay_method,
      10::integer AS authority_priority,
      COALESCE(finance_component.updated_at_utc, retained_case.updated_at) AS authority_at_utc,
      'OPEN_FINANCE_CASE_OR_COMPONENT'::text AS authority_source
    FROM retained_case_scope AS retained_case
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE retained_case.status IN (
        'ACTIVE'::public.pay_advance_status_enum,
        'PAUSED'::public.pay_advance_status_enum
      )
      AND (
        ROUND(COALESCE(retained_case.outstanding_amount, 0), 2) > 0
        OR (
          finance_component.id IS NOT NULL
          AND finance_component.closed_at_utc IS NULL
          AND ROUND(COALESCE(finance_component.remaining_source_amount, 0), 2) > 0
        )
      )
      AND COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Frozen active Draft authority. SETTLED is deliberately excluded here;
    -- settled authority is proven from the frozen item and settlement evidence
    -- in the next branch, so it cannot be counted as both active and settled.
    SELECT DISTINCT
      COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(
        active_reservation.frozen_source_pay_method,
        finance_component.source_pay_method,
        ''
      ))), '') AS source_pay_method,
      20::integer AS authority_priority,
      COALESCE(active_reservation.committed_at_utc, active_reservation.created_at_utc) AS authority_at_utc,
      'ACTIVE_FINANCE_RESERVATION'::text AS authority_source
    FROM public.pay_advance_reservations AS active_reservation
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = active_reservation.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = active_reservation.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE UPPER(BTRIM(COALESCE(active_reservation.status, ''))) IN ('RESERVED', 'COMMITTED')
      AND active_reservation.released_at_utc IS NULL
      AND COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Immutable settled movements that still contribute to projected retained
    -- value. Voided/reversed no-money authority is excluded exactly once.
    SELECT DISTINCT
      COALESCE(batch_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(
        batch_item.frozen_source_pay_method,
        finance_component.source_pay_method,
        batch_item.pay_channel,
        ''
      ))), '') AS source_pay_method,
      30::integer AS authority_priority,
      COALESCE(bank_transfer.completed_at_utc, batch_candidate.settled_at_utc, batch_item.created_at) AS authority_at_utc,
      'ACTIVE_SETTLED_FINANCE_MOVEMENT'::text AS authority_source
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
     AND batch_candidate.candidate_id = p_candidate_id
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = batch_item.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = batch_item.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    LEFT JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = batch_item.pay_bank_transfer_id
    WHERE batch_item.is_voided IS NOT TRUE
      AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
      AND (
        UPPER(BTRIM(COALESCE(batch_candidate.settlement_status, ''))) = 'SETTLED'
        OR batch_candidate.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(bank_transfer.status, ''))) = 'COMPLETED'
        OR bank_transfer.completed_at_utc IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_reversal
        WHERE applied_reversal.pay_batch_item_id = batch_item.id
          AND applied_reversal.status = 'APPLIED'
          AND applied_reversal.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      )
      AND COALESCE(batch_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Applied corrections are retained only where they are one of the exact
    -- authority-changing correction kinds and remain linked to this finance
    -- lineage. Generic historic events do not broaden route-change scope.
    SELECT DISTINCT
      COALESCE(correction_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(finance_component.source_pay_method, ''))), '') AS source_pay_method,
      40::integer AS authority_priority,
      COALESCE(correction_item.applied_at_utc, correction_item.created_at_utc) AS authority_at_utc,
      'APPLIED_FINANCE_CORRECTION'::text AS authority_source
    FROM public.pay_payment_correction_items AS correction_item
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = correction_item.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = correction_item.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE correction_item.candidate_id = p_candidate_id
      AND correction_item.status = 'APPLIED'
      AND correction_item.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      AND COALESCE(correction_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL
  ),
  canonical_seed_ids AS (
    SELECT COALESCE(
      array_agg(DISTINCT seed.timesheet_id ORDER BY seed.timesheet_id),
      ARRAY[]::uuid[]
    ) AS timesheet_ids
    FROM (
      SELECT current_timesheet.timesheet_id
      FROM public.timesheets AS current_timesheet
      JOIN public.contracts AS current_contract
        ON current_contract.id = current_timesheet.contract_id
       AND current_contract.candidate_id = p_candidate_id
      WHERE current_timesheet.is_current IS TRUE

      UNION

      SELECT current_financial.timesheet_id
      FROM public.timesheets_financials AS current_financial
      WHERE current_financial.candidate_id = p_candidate_id
        AND current_financial.is_current IS TRUE
        AND current_financial.timesheet_id IS NOT NULL

      UNION

      SELECT payment_override.timesheet_id
      FROM public.timesheet_payment_overrides AS payment_override
      WHERE payment_override.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
        AND payment_override.cleared_at_utc IS NULL
        AND payment_override.consumed_at_utc IS NULL
        AND payment_override.consumed_by_pay_batch_id IS NULL
        AND payment_override.timesheet_id IS NOT NULL

      UNION

      SELECT retained_authority.timesheet_id
      FROM retained_finance_authority_rows AS retained_authority
      WHERE retained_authority.timesheet_id IS NOT NULL
    ) AS seed
  ),
  rotation_scope AS (
    SELECT
      rotation_row.requested_timesheet_id,
      rotation_row.booking_id,
      rotation_row.canonical_timesheet_id,
      rotation_row.family_timesheet_id,
      rotation_row.family_is_current,
      rotation_row.family_version,
      rotation_row.requested_is_canonical
    FROM public._pay_timesheet_rotation_scope(
      COALESCE((SELECT canonical_seed_ids.timesheet_ids FROM canonical_seed_ids), ARRAY[]::uuid[])
    ) AS rotation_row
  ),
  canonical_ids AS (
    SELECT DISTINCT
      COALESCE(rotation_row.canonical_timesheet_id, seed_id.timesheet_id) AS timesheet_id
    FROM unnest(
      COALESCE((SELECT canonical_seed_ids.timesheet_ids FROM canonical_seed_ids), ARRAY[]::uuid[])
    ) AS seed_id(timesheet_id)
    LEFT JOIN rotation_scope AS rotation_row
      ON rotation_row.requested_timesheet_id = seed_id.timesheet_id
    WHERE COALESCE(rotation_row.canonical_timesheet_id, seed_id.timesheet_id) IS NOT NULL
  ),
  canonical_qualification AS (
    SELECT
      canonical_id.timesheet_id,
      (
        current_timesheet.archived_at_utc IS NULL
        AND current_timesheet.revoked_at IS NULL
        AND current_timesheet.is_current IS TRUE
        AND current_timesheet.authorised_at_server IS NOT NULL
      ) AS is_authorised,
      EXISTS (
        SELECT 1
        FROM public.timesheet_payment_overrides AS payment_override
        WHERE payment_override.candidate_id = p_candidate_id
          AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
          AND payment_override.cleared_at_utc IS NULL
          AND payment_override.consumed_at_utc IS NULL
          AND payment_override.consumed_by_pay_batch_id IS NULL
          AND (
            payment_override.timesheet_id = canonical_id.timesheet_id
            OR payment_override.timesheet_id IN (
              SELECT family_row.family_timesheet_id
              FROM rotation_scope AS family_row
              WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
            )
          )
      ) AS has_active_advance,
      EXISTS (
        SELECT 1
        FROM retained_finance_authority_rows AS retained_authority
        WHERE retained_authority.timesheet_id = canonical_id.timesheet_id
           OR retained_authority.timesheet_id IN (
             SELECT family_row.family_timesheet_id
             FROM rotation_scope AS family_row
             WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
           )
      ) AS has_retained_finance_authority,
      UPPER(BTRIM(COALESCE(
        current_financial.pay_method,
        retained_finance_authority.source_pay_method,
        current_contract.pay_method_snapshot,
        ''
      ))) AS source_pay_method
    FROM canonical_ids AS canonical_id
    LEFT JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = canonical_id.timesheet_id
     AND current_timesheet.is_current IS TRUE
    LEFT JOIN LATERAL (
      SELECT financial_row.pay_method
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = canonical_id.timesheet_id
        AND financial_row.is_current IS TRUE
        AND financial_row.candidate_id = p_candidate_id
      ORDER BY financial_row.timesheet_version DESC, financial_row.id DESC
      LIMIT 1
    ) AS current_financial ON TRUE
    LEFT JOIN LATERAL (
      SELECT retained_authority.source_pay_method
      FROM retained_finance_authority_rows AS retained_authority
      WHERE (
          retained_authority.timesheet_id = canonical_id.timesheet_id
          OR retained_authority.timesheet_id IN (
            SELECT family_row.family_timesheet_id
            FROM rotation_scope AS family_row
            WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
          )
        )
        AND retained_authority.source_pay_method IN ('PAYE', 'UMBRELLA')
      ORDER BY
        retained_authority.authority_priority,
        retained_authority.authority_at_utc DESC NULLS LAST,
        retained_authority.source_pay_method
      LIMIT 1
    ) AS retained_finance_authority ON TRUE
    LEFT JOIN public.contracts AS current_contract
      ON current_contract.id = current_timesheet.contract_id
     AND current_contract.candidate_id = p_candidate_id
  ),
  qualifying_timesheets AS (
    SELECT
      qualification.timesheet_id,
      qualification.is_authorised,
      qualification.has_active_advance,
      qualification.has_retained_finance_authority,
      qualification.source_pay_method
    FROM canonical_qualification AS qualification
    WHERE qualification.is_authorised IS TRUE
       OR qualification.has_active_advance IS TRUE
       OR qualification.has_retained_finance_authority IS TRUE
  ),
  current_preview_rows AS (
    SELECT DISTINCT
      authoritative_session.session_id,
      authoritative_session.session_version,
      preview_row.id AS preview_row_id,
      preview_row.timesheet_id AS requested_timesheet_id,
      COALESCE(current_preview_timesheet.timesheet_id, preview_row.timesheet_id) AS canonical_timesheet_id
    FROM authoritative_sessions AS authoritative_session
    JOIN public.banking_pay_workbench_preview_rows AS preview_row
      ON preview_row.session_id = authoritative_session.session_id
     AND preview_row.candidate_id = p_candidate_id
     AND preview_row.session_version = authoritative_session.session_version
    LEFT JOIN public.timesheets AS requested_preview_timesheet
      ON requested_preview_timesheet.timesheet_id = preview_row.timesheet_id
    LEFT JOIN LATERAL (
      SELECT candidate_current_timesheet.timesheet_id
      FROM public.timesheets AS candidate_current_timesheet
      WHERE requested_preview_timesheet.booking_id IS NOT NULL
        AND candidate_current_timesheet.booking_id = requested_preview_timesheet.booking_id
        AND candidate_current_timesheet.is_current IS TRUE
      ORDER BY candidate_current_timesheet.version DESC NULLS LAST,
               candidate_current_timesheet.updated_at DESC NULLS LAST,
               candidate_current_timesheet.created_at DESC NULLS LAST,
               candidate_current_timesheet.timesheet_id DESC
      LIMIT 1
    ) AS current_preview_timesheet ON TRUE
    WHERE preview_row.timesheet_id IS NOT NULL
  ),
  represented_ids AS (
    SELECT COALESCE(
      array_agg(DISTINCT preview_row.canonical_timesheet_id ORDER BY preview_row.canonical_timesheet_id),
      ARRAY[]::uuid[]
    ) AS timesheet_ids
    FROM current_preview_rows AS preview_row
    WHERE preview_row.canonical_timesheet_id IS NOT NULL
  ),
  session_rollup AS (
    SELECT
      authoritative_session.session_id,
      authoritative_session.actor_user_id,
      authoritative_session.pay_date,
      authoritative_session.week_ending_cutoff,
      authoritative_session.session_version,
      authoritative_session.progress_state,
      authoritative_session.progress_counter_version,
      authoritative_session.updated_at_utc,
      COUNT(DISTINCT current_preview.preview_row_id)::integer AS preview_row_count,
      COALESCE(
        array_agg(DISTINCT current_preview.canonical_timesheet_id ORDER BY current_preview.canonical_timesheet_id)
          FILTER (WHERE current_preview.canonical_timesheet_id IS NOT NULL),
        ARRAY[]::uuid[]
      ) AS represented_timesheet_ids,
      COALESCE(
        (SELECT array_agg(qualifying.timesheet_id ORDER BY qualifying.timesheet_id) FROM qualifying_timesheets AS qualifying),
        ARRAY[]::uuid[]
      ) AS qualifying_timesheet_ids
    FROM authoritative_sessions AS authoritative_session
    LEFT JOIN current_preview_rows AS current_preview
      ON current_preview.session_id = authoritative_session.session_id
     AND current_preview.session_version = authoritative_session.session_version
    GROUP BY
      authoritative_session.session_id,
      authoritative_session.actor_user_id,
      authoritative_session.pay_date,
      authoritative_session.week_ending_cutoff,
      authoritative_session.session_version,
      authoritative_session.progress_state,
      authoritative_session.progress_counter_version,
      authoritative_session.updated_at_utc
  )
  SELECT
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'session_id', session_row.session_id::text,
          'actor_user_id', session_row.actor_user_id::text,
          'pay_date', session_row.pay_date::text,
          'week_ending_cutoff', session_row.week_ending_cutoff::text,
          'session_version', session_row.session_version,
          'progress_state', session_row.progress_state,
          'progress_counter_version', session_row.progress_counter_version,
          'preview_row_count', session_row.preview_row_count,
          'represented_timesheet_ids', to_jsonb(session_row.represented_timesheet_ids),
          'qualifying_timesheet_ids', to_jsonb(session_row.qualifying_timesheet_ids),
          'updated_at_utc', session_row.updated_at_utc::text
        )
        ORDER BY session_row.updated_at_utc DESC, session_row.session_id
      )
      FROM session_rollup AS session_row
    ), '[]'::jsonb),
    COALESCE((SELECT COUNT(*)::integer FROM current_preview_rows), 0),
    COALESCE((SELECT represented_ids.timesheet_ids FROM represented_ids), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.is_authorised IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.has_active_advance IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.has_retained_finance_authority IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualifying.timesheet_id ORDER BY qualifying.timesheet_id)
      FROM qualifying_timesheets AS qualifying
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'timesheet_id', qualifying.timesheet_id::text,
          'is_authorised', qualifying.is_authorised,
          'has_active_advance', qualifying.has_active_advance,
          'has_retained_finance_authority', qualifying.has_retained_finance_authority,
          'source_pay_method', NULLIF(qualifying.source_pay_method, ''),
          'target_pay_method', v_target_method,
          'source_target_mismatch', (
            qualifying.source_pay_method IN ('PAYE', 'UMBRELLA')
            AND qualifying.source_pay_method <> v_target_method
          )
        )
        ORDER BY qualifying.timesheet_id
      )
      FROM qualifying_timesheets AS qualifying
    ), '[]'::jsonb),
    COALESCE((
      SELECT COUNT(*)::integer
      FROM qualifying_timesheets AS qualifying
      WHERE qualifying.source_pay_method IN ('PAYE', 'UMBRELLA')
        AND qualifying.source_pay_method <> v_target_method
    ), 0)
  INTO
    v_authoritative_sessions_json,
    v_preview_row_count,
    v_represented_timesheet_ids,
    v_authorised_timesheet_ids,
    v_active_advance_timesheet_ids,
    v_retained_finance_timesheet_ids,
    v_targeted_timesheet_ids,
    v_target_details_json,
    v_source_target_mismatch_count;

  WITH authoritative_session_ids AS (
    SELECT (session_value->>'session_id')::uuid AS session_id
    FROM jsonb_array_elements(v_authoritative_sessions_json) AS session_values(session_value)
    WHERE COALESCE(session_value->>'session_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  SELECT COALESCE(
    jsonb_agg(source_session.id::text ORDER BY source_session.updated_at_utc DESC, source_session.id),
    '[]'::jsonb
  )
  INTO v_replaced_source_session_ids_json
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.replacement_session_id IN (
    SELECT authoritative_session.session_id
    FROM authoritative_session_ids AS authoritative_session
  )
    AND (
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS source_scope
        WHERE source_scope.session_id = source_session.id
          AND source_scope.candidate_id = p_candidate_id
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS source_preview
        WHERE source_preview.session_id = source_session.id
          AND source_preview.candidate_id = p_candidate_id
      )
    );

  IF v_route_operation_id_text IS NOT NULL OR v_route_actor_user_id_text IS NOT NULL THEN
    IF v_route_operation_id_text IS NULL OR v_route_operation_id_text !~* v_uuid_re THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_CONTEXT_REQUIRED'
        USING ERRCODE = '22023';
    END IF;

    IF v_route_actor_user_id_text IS NULL OR v_route_actor_user_id_text !~* v_uuid_re THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ACTOR_CONTEXT_REQUIRED'
        USING ERRCODE = '22023';
    END IF;

    v_route_operation_id := v_route_operation_id_text::uuid;
    v_route_actor_user_id := v_route_actor_user_id_text::uuid;

    SELECT LOWER(BTRIM(COALESCE(actor_row.role, '')))
    INTO v_route_actor_role
    FROM public.tms_users AS actor_row
    WHERE actor_row.id = v_route_actor_user_id
      AND actor_row.is_active IS TRUE;

    IF NOT FOUND OR v_route_actor_role <> 'admin' THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ADMIN_REQUIRED'
        USING ERRCODE = '42501', DETAIL = v_route_actor_user_id::text;
    END IF;

    IF v_candidate_current_method IS DISTINCT FROM v_target_method THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_NOT_COMMITTED'
        USING ERRCODE = '40001',
              DETAIL = jsonb_build_object(
                'candidate_id', p_candidate_id::text,
                'candidate_current_method', v_candidate_current_method,
                'expected_target_method', v_target_method,
                'operation_id', v_route_operation_id::text
              )::text;
    END IF;

    v_route_dedupe_key := 'CANDIDATE_PAY_METHOD_CHANGE:' || p_candidate_id::text || ':' || v_route_operation_id::text;
    PERFORM pg_advisory_xact_lock(hashtext('candidate_pay_method_change_job:' || v_route_operation_id::text));

    SELECT COUNT(*)::integer
    INTO v_route_job_count
    FROM public.banking_pay_workbench_jobs AS existing_job
    WHERE existing_job.dedupe_key = v_route_dedupe_key;

    IF v_route_job_count > 1 THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_DUPLICATED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'candidate_id', p_candidate_id::text,
                'operation_id', v_route_operation_id::text,
                'dedupe_key', v_route_dedupe_key,
                'job_count', v_route_job_count
              )::text;
    END IF;

    SELECT
      existing_job.id,
      existing_job.status,
      COALESCE(existing_job.payload_json, '{}'::jsonb)
    INTO
      v_route_job_id,
      v_route_job_status,
      v_route_job_payload
    FROM public.banking_pay_workbench_jobs AS existing_job
    WHERE existing_job.dedupe_key = v_route_dedupe_key
    ORDER BY existing_job.updated_at_utc DESC, existing_job.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_route_job_id IS NOT NULL THEN
      IF COALESCE(v_route_job_payload->>'candidate_id', '') <> p_candidate_id::text
         OR COALESCE(v_route_job_payload->>'route_change_operation_id', '') <> v_route_operation_id::text
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'route_change_source_method', ''))) <> v_source_method
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'route_change_target_method', ''))) <> v_target_method THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_AUTHORITY_MISMATCH'
          USING ERRCODE = 'P0001', DETAIL = v_route_job_id::text;
      END IF;

      SELECT
        COUNT(*)::integer,
        COUNT(*) FILTER (WHERE raw_target.value !~* v_uuid_re)::integer,
        COALESCE(
          array_agg(DISTINCT raw_target.value::uuid ORDER BY raw_target.value::uuid)
            FILTER (WHERE raw_target.value ~* v_uuid_re),
          ARRAY[]::uuid[]
        )
      INTO
        v_route_existing_raw_count,
        v_route_existing_invalid_count,
        v_route_existing_targeted_timesheet_ids
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_route_job_payload->'targeted_timesheet_ids') = 'array'
            THEN v_route_job_payload->'targeted_timesheet_ids'
          ELSE '[]'::jsonb
        END
      ) AS raw_target(value);

      v_route_existing_duplicate_count := GREATEST(
        COALESCE(v_route_existing_raw_count, 0)
          - COALESCE(v_route_existing_invalid_count, 0)
          - COALESCE(array_length(v_route_existing_targeted_timesheet_ids, 1), 0),
        0
      );

      IF COALESCE(v_route_existing_invalid_count, 0) <> 0
         OR COALESCE(v_route_existing_duplicate_count, 0) <> 0
         OR v_route_existing_targeted_timesheet_ids IS DISTINCT FROM v_targeted_timesheet_ids
         OR LOWER(BTRIM(COALESCE(v_route_job_payload->>'exact_target_scope', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'refresh_scope_kind', ''))) <> 'TARGETED_TIMESHEETS' THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_SET_MISMATCH'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'job_id', v_route_job_id::text,
                  'expected_targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
                  'persisted_targeted_timesheet_ids', to_jsonb(v_route_existing_targeted_timesheet_ids),
                  'invalid_target_count', v_route_existing_invalid_count,
                  'effective_duplicate_count', v_route_existing_duplicate_count
                )::text;
      END IF;

      v_latest_source_change_seq := COALESCE(
        CASE
          WHEN COALESCE(v_route_job_payload->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
            THEN (v_route_job_payload->>'source_change_seq')::bigint
          ELSE v_latest_source_change_seq
        END,
        v_latest_source_change_seq,
        0
      );
    ELSE
      PERFORM public._change_bump('pay_candidate:' || p_candidate_id::text);

      SELECT COALESCE(change_counter.seq, 0)
      INTO v_latest_source_change_seq
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

      v_latest_source_change_seq := COALESCE(v_latest_source_change_seq, 0);

      v_route_job_payload := (
        jsonb_build_object(
          'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
          'scope_kind', 'CANDIDATE',
          'scope_id', p_candidate_id::text,
          'candidate_id', p_candidate_id::text,
          'reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'dirty_reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'candidate_pay_method_change', true,
          'candidate_payment_status_changed', true,
          'pay_method_changed', true,
          'prospective_only', true,
          'old_pay_method', v_source_method,
          'new_pay_method', v_target_method,
          'route_change_operation_id', v_route_operation_id::text,
          'route_change_actor_user_id', v_route_actor_user_id::text,
          'actor_user_id', v_route_actor_user_id::text,
          'route_change_reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'route_change_source_method', v_source_method,
          'route_change_target_method', v_target_method,
          'latest_source_change_seq', v_latest_source_change_seq,
          'source_change_seq', v_latest_source_change_seq,
          'source_change_sequence', v_latest_source_change_seq
        )
        || jsonb_build_object(
          'refresh_scope_kind', 'TARGETED_TIMESHEETS',
          'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
          'targeted_scope_is_empty', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0,
          'exact_target_scope', true,
          'coverage_basis', 'CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY',
          'coverage_complete', true,
          'authorised_timesheet_ids', to_jsonb(v_authorised_timesheet_ids),
          'authorised_timesheet_count', COALESCE(array_length(v_authorised_timesheet_ids, 1), 0),
          'active_advance_timesheet_ids', to_jsonb(v_active_advance_timesheet_ids),
          'active_advance_timesheet_count', COALESCE(array_length(v_active_advance_timesheet_ids, 1), 0),
          'retained_finance_timesheet_ids', to_jsonb(v_retained_finance_timesheet_ids),
          'retained_finance_timesheet_count', COALESCE(array_length(v_retained_finance_timesheet_ids, 1), 0),
          'authoritative_sessions', v_authoritative_sessions_json,
          'replaced_source_session_ids', v_replaced_source_session_ids_json,
          'target_details', v_target_details_json,
          'source_target_mismatch_count', COALESCE(v_source_target_mismatch_count, 0),
          'source_build_required', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0,
          'line_work_required', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0,
          'line_work_only', false,
          'line_work_action', 'SOURCE_BUILD',
          'rerun_required', false
        )
        || jsonb_build_object(
          'contracts_changed', 0,
          'contract_weeks_changed', 0,
          'timesheets_changed', 0,
          'rates_changed', 0,
          'tsfin_repricing_rows', 0,
          'source_records_mutated', false,
          'economic_truth_mutation_allowed', false,
          'refresh_completion_requires_terminal_exact_scope', true,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'policy_x_dirtying_only', true
        )
      );

      INSERT INTO public.banking_pay_workbench_jobs (
        job_type,
        status,
        priority,
        run_at_utc,
        attempt_count,
        max_attempts,
        dedupe_key,
        snapshot_run_id,
        session_id,
        candidate_id,
        payload_json,
        created_at_utc,
        updated_at_utc,
        started_at_utc,
        completed_at_utc,
        failed_at_utc,
        last_error_json
      )
      VALUES (
        'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0 THEN 'SUCCEEDED' ELSE 'QUEUED' END,
        -1000,
        v_now,
        0,
        8,
        v_route_dedupe_key,
        NULL,
        NULL,
        p_candidate_id,
        v_route_job_payload,
        v_now,
        v_now,
        NULL,
        CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0 THEN v_now ELSE NULL END,
        NULL,
        NULL
      )
      RETURNING id, status, payload_json
      INTO v_route_job_id, v_route_job_status, v_route_job_payload;
    END IF;
  END IF;

  RETURN (
    jsonb_build_object(
      'ok', true,
      'candidate_id', p_candidate_id::text,
      'candidate_current_method', v_candidate_current_method,
      'source_method', v_source_method,
      'target_method', v_target_method,
      'latest_source_change_seq', v_latest_source_change_seq,
      'source_change_seq', v_latest_source_change_seq,
      'durable_job_id', CASE WHEN v_route_job_id IS NULL THEN NULL ELSE v_route_job_id::text END,
      'durable_job_status', v_route_job_status,
      'durable_job_dedupe_key', v_route_dedupe_key,
      'durable_scope_persisted', v_route_job_id IS NOT NULL,
      'coverage_basis', 'CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY',
      'coverage_complete', true,
      'exact_scope', true,
      'authoritative_sessions', v_authoritative_sessions_json,
      'replaced_source_session_ids', v_replaced_source_session_ids_json,
      'represented_timesheet_ids', to_jsonb(v_represented_timesheet_ids),
      'authorised_timesheet_ids', to_jsonb(v_authorised_timesheet_ids),
      'active_advance_timesheet_ids', to_jsonb(v_active_advance_timesheet_ids),
      'retained_finance_timesheet_ids', to_jsonb(v_retained_finance_timesheet_ids)
    )
    || jsonb_build_object(
      'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
      'targeted_scope_is_empty', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0,
      'authoritative_session_count', jsonb_array_length(v_authoritative_sessions_json),
      'preview_row_count', v_preview_row_count,
      'represented_timesheet_count', COALESCE(array_length(v_represented_timesheet_ids, 1), 0),
      'authorised_timesheet_count', COALESCE(array_length(v_authorised_timesheet_ids, 1), 0),
      'active_advance_timesheet_count', COALESCE(array_length(v_active_advance_timesheet_ids, 1), 0),
      'retained_finance_timesheet_count', COALESCE(array_length(v_retained_finance_timesheet_ids, 1), 0),
      'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
      'source_target_mismatch_count', COALESCE(v_source_target_mismatch_count, 0),
      'target_details', v_target_details_json,
      'contracts_changed', 0,
      'contract_weeks_changed', 0,
      'timesheets_changed', 0,
      'rates_changed', 0,
      'tsfin_repricing_rows', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'policy_x_dirtying_only', true
    )
  );
END;
$function$;


REVOKE ALL ON FUNCTION public.candidate_pay_method_change_refresh_scope_v1(uuid, text, text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.candidate_pay_method_change_refresh_scope_v1(uuid, text, text) TO service_role;

