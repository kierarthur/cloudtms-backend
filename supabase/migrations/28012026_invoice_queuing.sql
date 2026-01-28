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
