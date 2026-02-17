-- ============================================================
-- TS PDF Outbox (ELECTRONIC timesheets) — DB objects
-- ============================================================

-- Ensure gen_random_uuid() exists
create extension if not exists pgcrypto;

-- 1) Enum (idempotent)
do $$
begin
  if not exists (select 1 from pg_type where typname = 'ts_pdf_reason_enum') then
    create type public.ts_pdf_reason_enum as enum (
      'READY_FOR_INVOICE',
      'FORCE_REGEN'
    );
  end if;
end $$;


-- 2) Table
create table if not exists public.ts_pdfs_outbox (
  id uuid primary key default gen_random_uuid(),

  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete cascade,
  reason public.ts_pdf_reason_enum not null,

  attempt_count int not null default 0,
  next_attempt_at timestamptz null,
  last_error text null,

  prefer_generated boolean not null default false,
  force_regen boolean not null default false,

  created_at timestamptz not null default now()
);

-- 3) Indexes
create unique index if not exists uq_ts_pdfs_outbox_timesheet_reason
  on public.ts_pdfs_outbox(timesheet_id, reason);

create index if not exists idx_ts_pdfs_outbox_due
  on public.ts_pdfs_outbox(next_attempt_at, created_at);

-- ============================================================
-- TS PDF Outbox RPCs (enqueue / dequeue / ack success / ack fail)
-- ============================================================

-- ------------------------------------------------------------
-- Enqueue ELECTRONIC timesheets that are READY_FOR_INVOICE and
-- have no manual scanned PDF (manual/QR evidence path)
-- ------------------------------------------------------------

create or replace function public.tspdf_enqueue_ready_for_invoice(p_limit int default 500)
returns int
language plpgsql
as $$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 2000));
begin
  with eligible as (
    select
      t.timesheet_id,
      t.generated_pdf_at_utc,
      nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') as prev_refs_sig,
      sig.cur_refs_sig,
      o.id as existing_outbox_id,
      coalesce(o.force_regen, false) as existing_force_regen,
      (
        nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
        and sig.cur_refs_sig is not null
        and nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') <> sig.cur_refs_sig
      ) as refs_sig_mismatch
    from public.timesheets t
    join public.timesheets_financials tf
      on tf.timesheet_id = t.timesheet_id
     and tf.is_current = true
    left join public.ts_pdfs_outbox o
      on o.timesheet_id = t.timesheet_id
     and o.reason = 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
    left join lateral (
      select
        case
          when nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
            then public.timesheet_pdf_reference_sig(t.timesheet_id)
          else null::text
        end as cur_refs_sig
    ) sig on true
    where t.is_current = true
      and t.revoked_at is null
      and t.submission_mode::text = 'ELECTRONIC'
      and t.manual_pdf_r2_key is null
      and t.r2_nurse_key is not null
      and t.r2_auth_key  is not null
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and (
        t.generated_pdf_at_utc is null
        or t.generated_pdf_refs_sig is null
        or (
          nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
          and sig.cur_refs_sig is not null
          and nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') <> sig.cur_refs_sig
        )
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  to_enqueue as (
    select
      e.timesheet_id,
      (e.refs_sig_mismatch is true) as force_regen
    from eligible e
    where
      (
        -- No existing READY_FOR_INVOICE outbox row: enqueue when dirty/missing OR refs mismatch.
        e.existing_outbox_id is null
      )
      or
      (
        -- Existing outbox row present: only upgrade to force_regen on first detection of mismatch.
        e.existing_outbox_id is not null
        and e.refs_sig_mismatch is true
        and e.existing_force_regen is not true
      )
  ),
  ins as (
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
    select
      te.timesheet_id,
      'READY_FOR_INVOICE'::public.ts_pdf_reason_enum,
      0,
      null,
      null,
      false,
      te.force_regen,
      now()
    from to_enqueue te
    on conflict (timesheet_id, reason)
    do update
      set attempt_count   = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then 0
                              else public.ts_pdfs_outbox.attempt_count
                            end,
          next_attempt_at = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then null
                              else public.ts_pdfs_outbox.next_attempt_at
                            end,
          last_error      = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then null
                              else public.ts_pdfs_outbox.last_error
                            end,
          prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
          force_regen      = public.ts_pdfs_outbox.force_regen or excluded.force_regen
      where public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
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
create or replace function public.tspdf_dequeue_batch_ids(p_limit int default 10)
returns table (
  outbox_id uuid,
  timesheet_id uuid,
  reason public.ts_pdf_reason_enum,
  attempt_count int,
  next_attempt_at timestamptz,
  created_at timestamptz,
  prefer_generated boolean,
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
    from public.ts_pdfs_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  )
  update public.ts_pdfs_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning
    o.id as outbox_id,
    o.timesheet_id,
    o.reason,
    o.attempt_count,
    o.next_attempt_at,
    o.created_at,
    o.prefer_generated,
    o.force_regen;
end;
$$;


-- ------------------------------------------------------------
-- Bulk success ack: delete outbox rows
-- ------------------------------------------------------------
create or replace function public.tspdf_work_success_bulk(p_ids uuid[])
returns int
language plpgsql
as $$
declare
  v_count int := 0;
begin
  if p_ids is null or coalesce(array_length(p_ids, 1), 0) = 0 then
    return 0;
  end if;

  -- Atomically:
  -- 1) delete outbox rows (the ACK)
  -- 2) set generated_pdf_at_utc for the corresponding CURRENT timesheet rows
  with gone as (
    delete from public.ts_pdfs_outbox o
    where o.id = any(p_ids)
    returning o.timesheet_id
  ),
  upd as (
    update public.timesheets t
    set generated_pdf_at_utc = now()
    from (select distinct timesheet_id from gone) g
    where t.timesheet_id = g.timesheet_id
      and t.is_current = true
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
create or replace function public.tspdf_work_fail_bulk(p_rows jsonb)
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
    update public.ts_pdfs_outbox o
    set last_error = r.err,
        next_attempt_at = v_now + interval '30 minutes'
    where o.id = r.outbox_id;

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;





create or replace function public.timesheet_pdf_load_context_batch(p_timesheet_ids uuid[])
returns table (
  timesheet_id uuid,
  out_ts jsonb,
  out_summary jsonb,
  out_contract jsonb,
  out_client jsonb,
  out_candidate jsonb,
  out_fin jsonb,
  out_def jsonb
)
language sql
stable
as $$
with wanted as (
  select distinct unnest(p_timesheet_ids) as timesheet_id
  where p_timesheet_ids is not null
),
t as (
  select ts.*
  from wanted w
  join public.timesheets ts
    on ts.timesheet_id = w.timesheet_id
   and ts.is_current = true
),
s as (
  -- ONLY fields that actually exist on v_timesheets_summary and are needed for identity resolution
  select
    t.timesheet_id,
    vs.candidate_id,
    vs.client_id,
    vs.candidate_name,
    vs.client_name,
    vs.contract_id
  from t
  left join public.v_timesheets_summary vs
    on vs.timesheet_id = t.timesheet_id
),
c as (
  select t.timesheet_id, ct.*
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join public.contracts ct
    on ct.id = coalesce(t.contract_id, s.contract_id)
),
ids as (
  select
    t.timesheet_id,
    coalesce(c.candidate_id, s.candidate_id) as eff_candidate_id,
    coalesce(c.client_id,    s.client_id)    as eff_client_id
  from t
  left join s on s.timesheet_id = t.timesheet_id
  left join c on c.timesheet_id = t.timesheet_id
)
select
  t.timesheet_id,
  (to_jsonb(t) || jsonb_build_object(
    'generated_pdf_refs_sig', t.generated_pdf_refs_sig,
    'generated_pdf_refs_snapshot_json', t.generated_pdf_refs_snapshot_json,
    'generated_pdf_refs_captured_at_utc', t.generated_pdf_refs_captured_at_utc
  )) as out_ts,
  to_jsonb(s) as out_summary,
  to_jsonb(c) as out_contract,
  to_jsonb(cl) as out_client,
  to_jsonb(ca) as out_candidate,
  to_jsonb(tf) as out_fin,
  jsonb_build_object(
    'agency_name', sd.agency_name,
    'agency_logo', sd.agency_logo,
    'timesheet_header_json', sd.timesheet_header_json,
    'timesheet_footer_json', sd.timesheet_footer_json,

    -- TEXT declaration columns do NOT exist in settings_defaults (keep keys for renderer compatibility):
    'temporary_worker_declaration', null::text,
    'client_declaration', null::text,

    -- JSON declarations DO exist:
    'temporary_worker_declaration_json', sd.temporary_worker_declaration_json,
    'client_declaration_json', sd.client_declaration_json
  ) as out_def
from t
left join s on s.timesheet_id = t.timesheet_id
left join c on c.timesheet_id = t.timesheet_id
left join ids on ids.timesheet_id = t.timesheet_id
left join public.clients    cl on cl.id = ids.eff_client_id
left join public.candidates ca on ca.id = ids.eff_candidate_id
left join public.timesheets_financials tf
  on tf.timesheet_id = t.timesheet_id
 and tf.is_current = true
left join public.settings_defaults sd on sd.id = 1;
$$;

