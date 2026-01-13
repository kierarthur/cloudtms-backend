-- ============================================================
-- CloudTMS: Invoice SQL-first RPCs (safe to re-run)
-- Requires:
--   - public.invoice_jobs_outbox table already created
--   - v_ts_invoice_precheck exists (authoritative gate)
--   - invoices.status is invoice_status_enum (DRAFT/ISSUED/PAID/ON_HOLD)
--   - invoices.type is invoice_type_enum (INVOICE/CREDIT_NOTE)
-- ============================================================

-- ------------------------------------------------------------
-- 3.1 Enqueue: invoice_enqueue_ready_for_invoice(p_limit int)
-- Strategy (no guessing):
--   - Uses v_ts_invoice_precheck.precheck_status='OK' (authoritative)
--   - Enqueues BY_WEEK jobs grouped by (client_id, invoice_week_start)
--   - invoice_week_start computed from week_ending_date - 6 days
-- Safe re-run: does NOT insert duplicates (NOT EXISTS predicate).
-- ------------------------------------------------------------
-- ------------------------------------------------------------
-- CloudTMS: invoice_enqueue_ready_for_invoice(p_limit int)
-- Re-scoped: AUTO-ENQUEUE ONLY (cron-safe)
--
-- Enqueues BY_WEEK jobs ONLY when:
--   - TSFIN snapshot is READY_FOR_INVOICE and unlocked
--   - timesheet is current + not revoked
--   - v_ts_invoice_precheck.precheck_status = 'OK' (authoritative, includes PDF gating)
--   - client_settings.auto_invoice_default (effective on London "today") = TRUE
--
-- NOTE:
--   - For clients where auto_invoice_default = FALSE, the frontend should INSERT
--     invoice_jobs_outbox rows directly when the user approves invoicing.
-- ------------------------------------------------------------
create or replace function public.invoice_enqueue_ready_for_invoice(p_limit int default 500)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));
begin
  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id

    -- client_settings (effective row on London "today")
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true

    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

      and pc.week_ending_date < (select anchor_ymd from anchor)

      -- ✅ auto-enqueue gate (client-led)
      and coalesce(cs.auto_invoice_default, false) = true

    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  return v_ins;
end;
$$;


-- ============================================================
-- CloudTMS: invoice_enqueue_auto_invoice_ready(p_limit int)
-- NEW (cron-safe auto enqueue)
--
-- Enqueues BY_WEEK jobs only for timesheets that are:
--   - TSFIN is_current + READY_FOR_INVOICE + unlocked
--   - timesheets is_current + not revoked
--   - v_ts_invoice_precheck.precheck_status = 'OK' (authoritative, includes PDF gating)
--   - Auto-invoice eligible:
--       COALESCE(contracts.auto_invoice, client_settings.auto_invoice_default, false) = true
--
-- Idempotent: will not enqueue duplicates for same (client_id, invoice_week_start).
-- ============================================================
create or replace function public.invoice_enqueue_auto_invoice_ready(p_limit int default 500)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));
begin
  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    left join public.contract_weeks cw
      on cw.timesheet_id = tf.timesheet_id
    left join public.contracts c
      on c.id = coalesce(t.contract_id, cw.contract_id)

    -- client_settings (effective row on London "today")
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true

    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

      and pc.week_ending_date < (select anchor_ymd from anchor)

      -- ✅ auto-invoice eligibility
      and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true

    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  return v_ins;
end;
$$;


-- ============================================================
-- CloudTMS: invoice_outbox_enqueue_hours(p_timesheet_ids, p_actor_user_id, p_meta)
-- NEW (manual/front-end enqueue HOURS)
--
-- Inserts a single HOURS job with payload.timesheet_ids = [uuid...]
-- Optional p_meta (if JSON object) is merged into payload (top-level).
-- Stores actor_user_id into payload for downstream use if desired.
--
-- Idempotent: uses payload.timesheet_ids_sig to avoid duplicates.
-- Returns the outbox_id (existing or newly inserted).
-- ============================================================
create or replace function public.invoice_outbox_enqueue_hours(
  p_timesheet_ids uuid[],
  p_actor_user_id uuid,
  p_meta jsonb default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ids uuid[];
  v_sig text;
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
begin
  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids, 1), 0) = 0 then
    raise exception 'timesheet_ids[] required';
  end if;

  select array_agg(x order by x::text)
  into v_ids
  from (
    select distinct unnest(p_timesheet_ids) as x
  ) q
  where q.x is not null;

  if v_ids is null or coalesce(array_length(v_ids, 1), 0) = 0 then
    raise exception 'timesheet_ids[] required';
  end if;

  v_sig := md5(array_to_string(v_ids::text[], '|'));

  v_payload := jsonb_build_object(
    'timesheet_ids', to_jsonb(v_ids),
    'timesheet_ids_sig', v_sig
  );

  if p_actor_user_id is not null then
    v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
  end if;

  if p_meta is not null then
    if jsonb_typeof(p_meta) = 'object' then
      v_payload := v_payload || p_meta;
    else
      v_payload := v_payload || jsonb_build_object('meta', p_meta);
    end if;
  end if;

  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'HOURS'
    and (o.payload->>'timesheet_ids_sig') = v_sig
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('HOURS'::text, v_payload)
  returning id into v_new;

  return v_new;
end;
$$;


-- ============================================================
-- CloudTMS: invoice_outbox_enqueue_by_week(p_client_id, p_invoice_week_start, p_actor_user_id, p_meta)
-- NEW (manual/front-end enqueue BY_WEEK)
--
-- Inserts a single BY_WEEK job with payload {client_id, invoice_week_start}.
-- Optional p_meta (if JSON object) is merged into payload (top-level).
-- Stores actor_user_id into payload for downstream use if desired.
--
-- Idempotent: will not create duplicates for same (client_id, invoice_week_start).
-- Returns the outbox_id (existing or newly inserted).
-- ============================================================
create or replace function public.invoice_outbox_enqueue_by_week(
  p_client_id uuid,
  p_invoice_week_start date,
  p_actor_user_id uuid,
  p_meta jsonb default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
begin
  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text
  );

  if p_actor_user_id is not null then
    v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
  end if;

  if p_meta is not null then
    if jsonb_typeof(p_meta) = 'object' then
      v_payload := v_payload || p_meta;
    else
      v_payload := v_payload || jsonb_build_object('meta', p_meta);
    end if;
  end if;

  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  return v_new;
end;
$$;

-- ============================================================
-- CloudTMS: invoice_outbox_enqueue(kind, payload, p_actor_user_id, p_meta)
-- OPTIONAL generic enqueue wrapper
--
-- - Normalizes kind to UPPER
-- - Merges p_meta into payload (top-level if object)
-- - Stores actor_user_id into payload if provided
-- - Best-effort idempotency:
--     * BY_WEEK: (client_id, invoice_week_start)
--     * HOURS:   (timesheet_ids_sig) if present
--
-- Returns the outbox_id (existing or newly inserted).
-- ============================================================
create or replace function public.invoice_outbox_enqueue(
  p_kind text,
  p_payload jsonb,
  p_actor_user_id uuid default null,
  p_meta jsonb default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_kind,'')));
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_existing uuid;
  v_new uuid;
  v_client_id text;
  v_week_start text;
  v_sig text;
begin
  if v_kind = '' then
    raise exception 'kind is required';
  end if;

  if jsonb_typeof(v_payload) <> 'object' then
    raise exception 'payload must be a jsonb object';
  end if;

  if p_actor_user_id is not null then
    v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
  end if;

  if p_meta is not null then
    if jsonb_typeof(p_meta) = 'object' then
      v_payload := v_payload || p_meta;
    else
      v_payload := v_payload || jsonb_build_object('meta', p_meta);
    end if;
  end if;

  -- Best-effort idempotency
  if v_kind = 'BY_WEEK' then
    v_client_id := nullif(btrim(coalesce(v_payload->>'client_id','')), '');
    v_week_start := nullif(btrim(coalesce(v_payload->>'invoice_week_start','')), '');

    if v_client_id is not null and v_week_start is not null then
      select o.id
      into v_existing
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = v_client_id
        and (o.payload->>'invoice_week_start') = v_week_start
      order by o.created_at desc
      limit 1;

      if v_existing is not null then
        return v_existing;
      end if;
    end if;

  elsif v_kind = 'HOURS' then
    v_sig := nullif(btrim(coalesce(v_payload->>'timesheet_ids_sig','')), '');

    if v_sig is not null then
      select o.id
      into v_existing
      from public.invoice_jobs_outbox o
      where o.kind = 'HOURS'
        and (o.payload->>'timesheet_ids_sig') = v_sig
      order by o.created_at desc
      limit 1;

      if v_existing is not null then
        return v_existing;
      end if;
    end if;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values (v_kind, v_payload)
  returning id into v_new;

  return v_new;
end;
$$;

-- ------------------------------------------------------------
-- 3.2 Dequeue: invoice_dequeue_batch_ids(p_limit int)
-- Mirrors tspdf_dequeue_batch_ids / tsfin_dequeue_batch_ids pattern.
-- ------------------------------------------------------------
create or replace function public.invoice_dequeue_batch_ids(p_limit int default 10)
returns table (
  outbox_id uuid,
  kind text,
  payload jsonb,
  attempt_count int,
  next_attempt_at timestamptz,
  created_at timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_lim int := greatest(1, least(coalesce(p_limit, 10), 500));
begin
  return query
  with picked as (
    select o.id
    from public.invoice_jobs_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  ),
    leased as (
    update public.invoice_jobs_outbox o
    set attempt_count   = coalesce(o.attempt_count, 0) + 1,
        next_attempt_at = v_now + interval '5 minutes'
    where o.id in (select id from picked)
    returning o.*
  )

  select
    l.id as outbox_id,
    l.kind,
    l.payload,
    l.attempt_count,
    l.next_attempt_at,
    l.created_at
  from leased l;
end;
$$;


-- ------------------------------------------------------------
-- Helper: insert audit_events (server-side)
-- Matches audit_events schema (actor_display/role optional).
-- ------------------------------------------------------------
create or replace function public._audit_insert(
  p_object_type text,
  p_object_id_text text,
  p_action text,
  p_before_json jsonb,
  p_after_json jsonb,
  p_reason text,
  p_actor_user_id uuid
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor_display text := null;
  v_actor_role    text := null;
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name, u.email, '')), ''),
      nullif(btrim(coalesce(u.role, '')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;
  end if;

  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time
  )
  values (
    coalesce(nullif(btrim(p_object_type),''), 'generic'),
    nullif(btrim(p_object_id_text), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    nullif(btrim(p_reason), ''),
    p_actor_user_id,
    nullif(btrim(v_actor_display), ''),
    nullif(btrim(v_actor_role), '')
  );
end;
$$;


-- ------------------------------------------------------------
-- 3.4 Issue: invoice_issue_one(p_invoice_id, p_actor_user_id)
-- Mirrors handleInvoiceIssue:
--   - collect invoice_lines.timesheet_id
--   - consult v_ts_invoice_precheck.precheck_status
--   - if blockers => status ON_HOLD and on_hold_reason = joined reasons
--   - else => status ISSUED, issued_at_utc=now(), on_hold_reason null
--   - write audit INVOICE_ON_HOLD or INVOICE_ISSUED
-- ------------------------------------------------------------
-- ============================================================
-- CloudTMS: Batch wrappers for Issue/Hold/Unhold/Unissue
-- Uses existing: invoice_issue_one, invoice_hold_one,
--               invoice_unhold_one, invoice_unissue_one
-- ============================================================

create or replace function public.invoice_issue_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  issued_at_utc timestamptz,
  on_hold_reason text,
  reasons text[],
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.issued_at_utc,
        x.on_hold_reason,
        x.reasons,
        null::text
      into invoice_id, ok, status, issued_at_utc, on_hold_reason, reasons, error
      from public.invoice_issue_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      issued_at_utc := null;
      on_hold_reason := null;
      reasons := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;


create or replace function public.invoice_hold_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid,
  p_reason text default null
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  on_hold_reason text,
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.on_hold_reason,
        null::text
      into invoice_id, ok, status, on_hold_reason, error
      from public.invoice_hold_one(v_id, p_actor_user_id, p_reason) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      on_hold_reason := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;


create or replace function public.invoice_unhold_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        null::text
      into invoice_id, ok, status, error
      from public.invoice_unhold_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;


create or replace function public.invoice_unissue_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid,
  p_clear_pdf boolean default false
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  cleared_pdf boolean,
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.cleared_pdf,
        null::text
      into invoice_id, ok, status, cleared_pdf, error
      from public.invoice_unissue_one(v_id, p_actor_user_id, p_clear_pdf) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      cleared_pdf := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;

-- ============================================================
-- UPDATED: public.invoice_issue_one(p_invoice_id, p_actor_user_id)
-- ✅ SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
--
-- CHANGE: Adds HR validation hardening:
--   - For each invoice timesheet:
--       if v_timesheets_summary_base.hr_validation_required_for_invoice = true
--       AND validation_status is NULL OR not in (VALIDATION_OK, OVERRIDDEN)
--       => add blocker reason and place invoice ON_HOLD.
--
-- Note:
--   - Uses v_timesheets_summary_base.hr_validation_required_for_invoice
--     (your new appended boolean) as the policy switch.
-- ============================================================

create or replace function public.invoice_issue_one(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  status text,
  issued_at_utc timestamptz,
  on_hold_reason text,
  reasons text[]
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_ts_ids uuid[];

  v_reasons text[] := array[]::text[];
  v_precheck_reasons text[] := array[]::text[];
  v_hr_reasons text[] := array[]::text[];

  v_on_hold_reason text := null;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load invoice + basic guards
  declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot issue a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot issue a PAID invoice';
    end if;

    if v_inv.status::text = 'ISSUED' then
      status := 'ISSUED';
      issued_at_utc := v_inv.issued_at_utc;
      on_hold_reason := v_inv.on_hold_reason;
      reasons := array[]::text[];
      return next;
      return;
    end if;

    if v_inv.status::text not in ('DRAFT','ON_HOLD') then
      raise exception 'Only DRAFT/ON_HOLD invoices can be issued (current status=%)', v_inv.status::text;
    end if;
  end;

  -- Timesheets on invoice
  select array_agg(distinct l.timesheet_id)
  into v_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    raise exception 'Invoice has no timesheets to validate';
  end if;

  -- ------------------------------------------------------------
  -- 1) Precheck blockers (authoritative: PDF/reference/evidence)
  -- ------------------------------------------------------------
  select array_agg(
    case
      when pc.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': precheck missing'

      when upper(coalesce(pc.precheck_status,'')) = 'OK'
        then null

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_REFERENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing reference/PO'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_PDF'
        then 'TS ' || pc.timesheet_id::text || ': missing timesheet PDF'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_MILEAGE_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing mileage evidence'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_EXPENSES_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing expenses evidence'

      else
        'TS ' || pc.timesheet_id::text || ': precheck blocker ' || upper(coalesce(pc.precheck_status,''))
    end
  )
  into v_precheck_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_ts_invoice_precheck pc
    on pc.timesheet_id = x.timesheet_id;

  v_precheck_reasons := array_remove(v_precheck_reasons, null);

  -- ------------------------------------------------------------
  -- 2) OPTIONAL HARDENING: HR validation blockers
  -- ------------------------------------------------------------
  select array_agg(
    case
      when s.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': summary missing'

      when coalesce(s.hr_validation_required_for_invoice, false) = true
        and (
          s.validation_status is null
          or s.validation_status <> all (array[
            'VALIDATION_OK'::public.validation_status_enum,
            'OVERRIDDEN'::public.validation_status_enum
          ])
        )
        then 'TS ' || x.timesheet_id::text || ': HR validation not passed'

      else null
    end
  )
  into v_hr_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_timesheets_summary_base s
    on s.timesheet_id = x.timesheet_id;

  v_hr_reasons := array_remove(v_hr_reasons, null);

  -- Merge blockers
  v_reasons := array_cat(v_precheck_reasons, v_hr_reasons);
  v_reasons := array_remove(v_reasons, null);

  -- Any blockers => ON_HOLD
  if coalesce(array_length(v_reasons, 1), 0) > 0 then
    v_on_hold_reason := array_to_string(v_reasons, '; ');

    update public.invoices
    set status = 'ON_HOLD'::public.invoice_status_enum,
        status_date_utc = v_now,
        issued_at_utc = null,
        on_hold_reason = v_on_hold_reason
    where id = p_invoice_id;

    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_ON_HOLD',
      null,
      jsonb_build_object('reasons', v_reasons),
      null,
      p_actor_user_id
    );

    status := 'ON_HOLD';
    issued_at_utc := null;
    on_hold_reason := v_on_hold_reason;
    reasons := v_reasons;
    return next;
    return;
  end if;

  -- No blockers => issue
  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = v_now,
      on_hold_reason = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_ISSUED',
    null,
    '{}'::jsonb,
    null,
    p_actor_user_id
  );

  status := 'ISSUED';
  issued_at_utc := v_now;
  on_hold_reason := null;
  reasons := array[]::text[];
  return next;
end;
$$;


-- ------------------------------------------------------------
-- 3.4 Hold/Unhold/Unissue
-- Mirrors handleInvoiceHold / handleInvoiceUnhold / handleInvoiceUnissue
-- ------------------------------------------------------------
create or replace function public.invoice_hold_one(
  p_invoice_id uuid,
  p_actor_user_id uuid,
  p_reason text default null
)
returns table (
  status text,
  on_hold_reason text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  if v_reason is null then
    v_reason := 'Placed on hold';
  end if;

    declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot hold a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot hold a PAID invoice';
    end if;
  end;

  update public.invoices
  set status = 'ON_HOLD'::public.invoice_status_enum,
      status_date_utc = v_now,
      on_hold_reason = v_reason
  where id = p_invoice_id;


  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_HELD',
    null,
    jsonb_build_object('reason', v_reason),
    null,
    p_actor_user_id
  );

  status := 'ON_HOLD';
  on_hold_reason := v_reason;
  return next;
end;
$$;



create or replace function public.invoice_unhold_one(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  status text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

   declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot unhold a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot unhold a PAID invoice';
    end if;

    if v_inv.status::text = 'DRAFT' then
      status := 'DRAFT';
      return next;
      return;
    end if;

    if v_inv.status::text <> 'ON_HOLD' then
      raise exception 'Only ON_HOLD invoices can be unheld (current status=%)', v_inv.status::text;
    end if;
  end;

  update public.invoices
  set status = 'DRAFT'::public.invoice_status_enum,
      status_date_utc = v_now,
      on_hold_reason = null
  where id = p_invoice_id;


  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNHELD',
    null,
    '{}'::jsonb,
    null,
    p_actor_user_id
  );

  status := 'DRAFT';
  return next;
end;
$$;



create or replace function public.invoice_unissue_one(
  p_invoice_id uuid,
  p_actor_user_id uuid,
  p_clear_pdf boolean default false
)
returns table (
  status text,
  cleared_pdf boolean
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

    declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot unissue a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot unissue a PAID invoice';
    end if;

    if v_inv.status::text = 'DRAFT' then
      status := 'DRAFT';
      cleared_pdf := false;
      return next;
      return;
    end if;

    if v_inv.status::text <> 'ISSUED' then
      raise exception 'Only ISSUED invoices can be unissued (current status=%)', v_inv.status::text;
    end if;
  end;

  update public.invoices
  set status = 'DRAFT'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = null,
      on_hold_reason = null,
      invoice_pdf_r2_key = case when p_clear_pdf then null else invoice_pdf_r2_key end
  where id = p_invoice_id;


  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNISSUED',
    null,
    jsonb_build_object('clear_pdf', p_clear_pdf),
    null,
    p_actor_user_id
  );

  status := 'DRAFT';
  cleared_pdf := p_clear_pdf;
  return next;
end;
$$;

-- ============================================================
-- CloudTMS: Invoice Paid/Unpaid RPCs (safe to re-run)
-- Depends on: public._audit_insert(...)
-- ============================================================

-- ----------------------------
-- Mark one invoice as PAID
-- ----------------------------
create or replace function public.invoice_mark_paid_one(
  p_invoice_id uuid,
  p_actor_user_id uuid,
  p_paid_at timestamptz default null
)
returns table (
  invoice_id uuid,
  status text,
  paid_at_utc timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_paid_at timestamptz := coalesce(p_paid_at, v_now);
  v_inv record;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot mark a CREDIT_NOTE as PAID';
  end if;

  if v_inv.status::text = 'PAID' then
    -- Idempotent: ensure paid_at_utc is set
    update public.invoices
    set paid_at_utc = coalesce(paid_at_utc, v_paid_at),
        status_date_utc = coalesce(status_date_utc, v_now)
    where id = p_invoice_id;

    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_PAID',
      jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
      jsonb_build_object('status', 'PAID', 'paid_at_utc', coalesce(v_inv.paid_at_utc, v_paid_at)),
      null,
      p_actor_user_id
    );

    invoice_id := p_invoice_id;
    status := 'PAID';
    paid_at_utc := coalesce(v_inv.paid_at_utc, v_paid_at);
    return next;
    return;
  end if;

  if v_inv.status::text <> 'ISSUED' then
    raise exception 'Only ISSUED invoices can be marked as PAID (current status=%)', v_inv.status::text;
  end if;

  update public.invoices
  set status = 'PAID'::public.invoice_status_enum,
      status_date_utc = v_now,
      paid_at_utc = v_paid_at,
      on_hold_reason = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_PAID',
    jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
    jsonb_build_object('status', 'PAID', 'paid_at_utc', v_paid_at),
    null,
    p_actor_user_id
  );

  invoice_id := p_invoice_id;
  status := 'PAID';
  paid_at_utc := v_paid_at;
  return next;
end;
$$;


-- ----------------------------
-- Mark one invoice as UNPAID (revert PAID -> ISSUED)
-- ----------------------------
create or replace function public.invoice_mark_unpaid_one(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  status text,
  paid_at_utc timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_inv record;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot mark a CREDIT_NOTE as UNPAID';
  end if;

  if v_inv.status::text <> 'PAID' then
    raise exception 'Only PAID invoices can be marked as UNPAID (current status=%)', v_inv.status::text;
  end if;

  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      paid_at_utc = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNPAID',
    jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
    jsonb_build_object('status', 'ISSUED', 'paid_at_utc', null),
    null,
    p_actor_user_id
  );

  invoice_id := p_invoice_id;
  status := 'ISSUED';
  paid_at_utc := null;
  return next;
end;
$$;


-- ----------------------------
-- Batch: mark MANY invoices PAID
-- Returns per-invoice ok/error
-- ----------------------------
create or replace function public.invoice_mark_paid_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid,
  p_paid_at timestamptz default null
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  paid_at_utc timestamptz,
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select x.invoice_id, true, x.status, x.paid_at_utc, null::text
      into invoice_id, ok, status, paid_at_utc, error
      from public.invoice_mark_paid_one(v_id, p_actor_user_id, p_paid_at) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      paid_at_utc := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;


-- ----------------------------
-- Batch: mark MANY invoices UNPAID
-- Returns per-invoice ok/error
-- ----------------------------
create or replace function public.invoice_mark_unpaid_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  ok boolean,
  status text,
  paid_at_utc timestamptz,
  error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select x.invoice_id, true, x.status, x.paid_at_utc, null::text
      into invoice_id, ok, status, paid_at_utc, error
      from public.invoice_mark_unpaid_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      paid_at_utc := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$$;


-- ------------------------------------------------------------
-- 3.5 Remove NHSP shifts from invoice (SQL)
-- Mirrors handleInvoiceRemoveTimesheet:
--   - patch nhsp_shifts: set invoice_status='PENDING', invoice_id=null
--   - delete invoice_lines where meta_json.nhsp_shift_id in p_shift_ids
--   - recompute totals from remaining invoice_lines
--   - unlock TSFIN where invoice has no remaining lines for those timesheets
--   - audit NHSP_INVOICE_SHIFT_REMOVED
-- ------------------------------------------------------------
create or replace function public.invoice_remove_nhsp_shifts(
  p_invoice_id uuid,
  p_shift_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  subtotal_ex_vat numeric,
  vat_amount numeric,
  total_inc_vat numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_shift_ids uuid[];

  -- prev/new totals
  v_prev_ex numeric := 0;
  v_prev_vat numeric := 0;
  v_prev_inc numeric := 0;
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  v_delta_ex numeric := 0;
  v_delta_vat numeric := 0;
  v_delta_inc numeric := 0;

  v_invoice_no text := null;
  v_prev_status text := null;
  v_new_status text := null;

  -- removed-lines detail
  v_removed_ts_ids uuid[] := null;
  v_removed_source_keys text[] := null;
  v_removed_line_count int := 0;
  v_removed_ex numeric := 0;
  v_removed_vat numeric := 0;
  v_removed_inc numeric := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  if p_shift_ids is null or coalesce(array_length(p_shift_ids,1),0) = 0 then
    raise exception 'shift_ids[] required';
  end if;

  v_shift_ids := (select array_agg(distinct x) from unnest(p_shift_ids) x where x is not null);

  -- Capture invoice BEFORE
  select
    i.invoice_no,
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_invoice_no,
    v_prev_status,
    v_prev_ex,
    v_prev_vat,
    v_prev_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Identify lines that will be removed (for audit detail)
  with to_remove as (
    select
      l.timesheet_id,
      l.source_key,
      coalesce(l.total_charge_ex_vat,0)::numeric as ex,
      coalesce(l.vat_amount,0)::numeric as vat,
      coalesce(l.total_inc_vat,0)::numeric as inc
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and (l.meta_json ? 'nhsp_shift_id')
      and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids)
  )
  select
    array_agg(distinct timesheet_id),
    array_agg(distinct source_key),
    count(*)::int,
    coalesce(sum(ex),0)::numeric,
    coalesce(sum(vat),0)::numeric,
    coalesce(sum(inc),0)::numeric
  into
    v_removed_ts_ids,
    v_removed_source_keys,
    v_removed_line_count,
    v_removed_ex,
    v_removed_vat,
    v_removed_inc
  from to_remove;

  -- 1) Unlink shifts (only those currently linked to this invoice)
  update public.nhsp_shifts s
  set invoice_status = 'PENDING',
      invoice_id = null,
      updated_at = v_now
  where s.id = any(v_shift_ids)
    and s.invoice_id = p_invoice_id;

  -- 2) Delete invoice lines referencing these shifts
  delete from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and (l.meta_json ? 'nhsp_shift_id')
    and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids);

  -- 3) Recompute totals from remaining lines
  update public.invoices i
  set
    subtotal_ex_vat = x.ex,
    vat_amount      = x.vat,
    total_inc_vat   = x.inc,
    updated_at      = v_now
  from (
    select
      coalesce(sum(l.total_charge_ex_vat),0)::numeric as ex,
      coalesce(sum(l.vat_amount),0)::numeric as vat,
      coalesce(sum(l.total_inc_vat),0)::numeric as inc
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
  ) x
  where i.id = p_invoice_id;

  -- 4) Unlock TSFIN if a timesheet now has no remaining lines on this invoice
  update public.timesheets_financials tf
  set locked_by_invoice_id = null,
      updated_at = v_now
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null
    and not exists (
      select 1
      from public.invoice_lines l2
      where l2.invoice_id = p_invoice_id
        and l2.timesheet_id = tf.timesheet_id
    );

  -- Capture invoice AFTER + compute delta
  select
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_new_status,
    v_new_ex,
    v_new_vat,
    v_new_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  v_delta_ex  := public._inv_round2(v_new_ex  - v_prev_ex);
  v_delta_vat := public._inv_round2(v_new_vat - v_prev_vat);
  v_delta_inc := public._inv_round2(v_new_inc - v_prev_inc);

  -- Existing audit (kept), now with totals + timesheets + delta
  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'NHSP_INVOICE_SHIFT_REMOVED',
    jsonb_build_object(
      'invoice_no', v_invoice_no,
      'status', v_prev_status,
      'subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'vat_amount', public._inv_round2(v_prev_vat),
      'total_inc_vat', public._inv_round2(v_prev_inc)
    ),
    jsonb_build_object(
      'invoice_id', p_invoice_id::text,
      'invoice_no', v_invoice_no,
      'shift_ids', to_jsonb(v_shift_ids),

      'removed_line_count', coalesce(v_removed_line_count,0),
      'removed_timesheet_ids', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
      'removed_source_keys', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),

      'removed_subtotal_ex_vat', public._inv_round2(v_removed_ex),
      'removed_vat_amount', public._inv_round2(v_removed_vat),
      'removed_total_inc_vat', public._inv_round2(v_removed_inc),

      'invoice_status_before', v_prev_status,
      'invoice_status_after', v_new_status,

      'prev_subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'prev_vat_amount', public._inv_round2(v_prev_vat),
      'prev_total_inc_vat', public._inv_round2(v_prev_inc),

      'delta_subtotal_ex_vat', v_delta_ex,
      'delta_vat_amount', v_delta_vat,
      'delta_total_inc_vat', v_delta_inc,

      'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
      'new_vat_amount', public._inv_round2(v_new_vat),
      'new_total_inc_vat', public._inv_round2(v_new_inc),

      'run_at_utc', public._inv_iso_utc(v_now),
      'run_kind', 'REMOVE_NHSP_SHIFTS'
    ),
    null,
    p_actor_user_id
  );

  -- Generic “totals delta applied” audit (optional but recommended for unified reporting)
  if (coalesce(v_delta_ex,0) <> 0 or coalesce(v_delta_vat,0) <> 0 or coalesce(v_delta_inc,0) <> 0) then
    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_TOTALS_DELTA_APPLIED',
      jsonb_build_object(
        'invoice_no', v_invoice_no,
        'status', v_prev_status,
        'subtotal_ex_vat', public._inv_round2(v_prev_ex),
        'vat_amount', public._inv_round2(v_prev_vat),
        'total_inc_vat', public._inv_round2(v_prev_inc)
      ),
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'invoice_no', v_invoice_no,
        'run_at_utc', public._inv_iso_utc(v_now),
        'run_kind', 'REMOVE_NHSP_SHIFTS',

        'invoice_status_before', v_prev_status,
        'invoice_status_after', v_new_status,

        'delta_subtotal_ex_vat', v_delta_ex,
        'delta_vat_amount', v_delta_vat,
        'delta_total_inc_vat', v_delta_inc,

        'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
        'new_vat_amount', public._inv_round2(v_new_vat),
        'new_total_inc_vat', public._inv_round2(v_new_inc),

        'timesheet_ids_this_run', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
        'source_keys_this_run', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),
        'line_count_this_run', coalesce(v_removed_line_count,0)
      ),
      null,
      p_actor_user_id
    );
  end if;

  -- Return updated invoice totals
  select u.id, u.subtotal_ex_vat, u.vat_amount, u.total_inc_vat
  into invoice_id, subtotal_ex_vat, vat_amount, total_inc_vat
  from public.invoices u
  where u.id = p_invoice_id;

  return next;
end;
$$;


create or replace function public.invoice_render_manifest(p_invoice_id uuid)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with inv as (
  select
    i.*,
    (i.header_snapshot_json->'attach_policy') as attach_policy
  from public.invoices i
  where i.id = p_invoice_id
  limit 1
),
lines as (
  select
    l.*,
    coalesce(
      l.paper_ts_r2_key,
      case
        when l.timesheet_id is not null
          then ('docs-pdf/timesheets/ts_' || l.timesheet_id::text || '.pdf')
        else null
      end
    ) as effective_paper_ts_r2_key
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
  order by l.created_at asc
),
ts_ids as (
  select distinct timesheet_id
  from lines
  where timesheet_id is not null
),
ev as (
  select
    e.timesheet_id,
    e.kind,
    e.display_name,
    e.storage_key,
    e.created_at
  from public.timesheet_evidence e
  where e.timesheet_id in (select timesheet_id from ts_ids)
  order by e.created_at asc
),
hr_cache as (
  select
    r.invoice_id,
    r.source_system,
    r.import_id,
    r.header_columns,
    r.rows_json
  from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id
),
tsfin as (
  select
    tf.timesheet_id,
    tf.external_source_rows_json
  from public.timesheets_financials tf
  where tf.is_current = true
    and tf.timesheet_id in (select timesheet_id from ts_ids)
),
email_summary as (
  select
    count(*)::int as email_count,
    max(m.created_at_utc) as last_email_at_utc
  from public.mail_outbox m
  where upper(coalesce(m.type,'')) = 'INVOICE'
    and m.attachments is not null
    and jsonb_typeof(m.attachments) = 'array'
    and exists (
      select 1
      from jsonb_array_elements(m.attachments) a
      where btrim(coalesce(a->>'invoice_id','')) = p_invoice_id::text
    )
)
select jsonb_build_object(
  'invoice', to_jsonb(inv.*),
  'header_snapshot_json', coalesce((select inv.header_snapshot_json from inv), '{}'::jsonb),
  'attach_policy', coalesce((select inv.attach_policy from inv), null),

  'lines', coalesce((
    select jsonb_agg(
      to_jsonb(l.*)
      || jsonb_build_object('paper_ts_r2_key', l.effective_paper_ts_r2_key)
      || jsonb_build_object('is_adjustment', (l.timesheet_id is null or upper(coalesce(l.meta_json->>'line_type','')) = 'ADJUSTMENT'))
      || jsonb_build_object('line_type_norm', upper(coalesce(l.meta_json->>'line_type','')))
      order by l.created_at
    )
    from lines l
  ), '[]'::jsonb),

  'timesheet_ids', coalesce((select jsonb_agg(t.timesheet_id::text) from ts_ids t), '[]'::jsonb),
  'evidence', coalesce((select jsonb_agg(to_jsonb(ev.*)) from ev), '[]'::jsonb),
  'hr_source_rows_cache', coalesce((select jsonb_agg(to_jsonb(h.*)) from hr_cache h), '[]'::jsonb),
  'tsfin_external_source_rows', coalesce((select jsonb_agg(to_jsonb(t.*)) from tsfin t), '[]'::jsonb),

  -- ✅ email summary for UI label (Email vs Re-email)
  'email_summary', jsonb_build_object(
    'emailed_once', (coalesce((select email_count from email_summary),0) > 0),
    'email_count', coalesce((select email_count from email_summary),0),
    'last_email_at_utc', (select last_email_at_utc from email_summary)
  )
)
from inv;
$$;




-- ============================================================
-- STUBS (compile-safe) for the two RPCs that require your paste
-- ============================================================




-- 3.6 Credit note + unlock (needs unredacted JS parity source)
create or replace function public.invoice_create_credit_note_and_unlock(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  credit_note_id uuid,
  unlocked_snapshots int
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_inv record;
  v_base_hdr jsonb := '{}'::jsonb;

  v_original_issued_at timestamptz;
  v_anchor_ymd date;

  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_bank jsonb;
  v_vat_reg text;

  v_client_name text;
  v_client_addr text;
  v_client_email text;
  v_vat_chargeable boolean;
  v_terms_days int;

  v_applied_vat numeric;
  v_global_vat numeric := 20;
  v_client_vat_override numeric;

  v_due_at timestamptz;

  v_credit_id uuid;

  v_ts_ids uuid[];
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load original invoice
  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if jsonb_typeof(v_inv.header_snapshot_json) = 'object' then
    v_base_hdr := v_inv.header_snapshot_json;
  end if;

  -- Original issued time (prefer invoice.issued_at_utc, else snapshot.issued_at_utc, else now)
  v_original_issued_at := v_inv.issued_at_utc;
  if v_original_issued_at is null and (v_base_hdr ? 'issued_at_utc') then
    begin
      v_original_issued_at := (v_base_hdr->>'issued_at_utc')::timestamptz;
    exception when others then
      v_original_issued_at := null;
    end;
  end if;
  if v_original_issued_at is null then
    v_original_issued_at := v_now;
  end if;

  v_anchor_ymd := (v_original_issued_at at time zone 'Europe/London')::date;

  -- Stationery key: prefer snapshot.stationery_key; else fallback constant (matches your JS fallback)
  v_stationery_key := nullif(btrim(coalesce(v_base_hdr->>'stationery_key','')), '');
  if v_stationery_key is null then
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  end if;

  -- pdf → @300dpi.png
  if right(lower(v_stationery_key), 4) = '.pdf' then
    v_stationery_key := left(v_stationery_key, length(v_stationery_key) - 4) || '@300dpi.png';
  end if;

  -- strip leading slashes
  while left(v_stationery_key, 1) = '/' loop
    v_stationery_key := substr(v_stationery_key, 2);
  end loop;

  -- Margins: accept [t,r,b,l] array or {top,right,bottom,left} object; else default
  v_margins := v_base_hdr->'stationery_margins_mm';

  if jsonb_typeof(v_margins) = 'array' and jsonb_array_length(v_margins) = 4 then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>0)::numeric, 32),
      'right',  coalesce((v_margins->>1)::numeric, 12),
      'bottom', coalesce((v_margins->>2)::numeric, 20),
      'left',   coalesce((v_margins->>3)::numeric, 12)
    );
  elsif jsonb_typeof(v_margins) = 'object' then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>'top')::numeric, 32),
      'right',  coalesce((v_margins->>'right')::numeric, 12),
      'bottom', coalesce((v_margins->>'bottom')::numeric, 20),
      'left',   coalesce((v_margins->>'left')::numeric, 12)
    );
  else
    v_margins := jsonb_build_object('top',32,'right',12,'bottom',20,'left',12);
  end if;

  -- hide_bank_footer: snapshot boolean else default TRUE
  if jsonb_typeof(v_base_hdr->'hide_bank_footer') = 'boolean' then
    v_hide_bank_footer := (v_base_hdr->>'hide_bank_footer')::boolean;
  else
    v_hide_bank_footer := true;
  end if;

  -- Bank + VAT registration: prefer snapshot, else settings_defaults(id=1)
  if jsonb_typeof(v_base_hdr->'bank') = 'object' then
    v_bank := v_base_hdr->'bank';
  else
    v_bank := null;
  end if;

  v_vat_reg := nullif(btrim(coalesce(v_base_hdr->>'vat_registration_number','')), '');

  if v_bank is null or v_vat_reg is null then
    declare
      v_def record;
    begin
      select bank_name, bank_sort_code, bank_account_number, vat_registration_number
      into v_def
      from public.settings_defaults
      where id = 1
      limit 1;

      if v_bank is null then
        v_bank := jsonb_build_object(
          'name', v_def.bank_name,
          'sort_code', v_def.bank_sort_code,
          'account_number', v_def.bank_account_number
        );
      end if;

      if v_vat_reg is null then
        v_vat_reg := v_def.vat_registration_number;
      end if;
    end;
  end if;

  -- Client info: prefer snapshot; else clients table
  v_client_name  := nullif(btrim(coalesce(v_base_hdr->>'client_name','')), '');
  v_client_addr  := nullif(btrim(coalesce(v_base_hdr->>'client_invoice_address','')), '');
  v_client_email := nullif(btrim(coalesce(v_base_hdr->>'client_primary_invoice_email','')), '');

  if jsonb_typeof(v_base_hdr->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_base_hdr->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := null;
  end if;

  if (v_base_hdr ? 'payment_terms_days') then
    begin
      v_terms_days := (v_base_hdr->>'payment_terms_days')::int;
    exception when others then
      v_terms_days := null;
    end;
  else
    v_terms_days := null;
  end if;

  if v_client_name is null or v_client_addr is null or v_vat_chargeable is null or v_terms_days is null then
    declare
      v_cli record;
    begin
      select name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
      into v_cli
      from public.clients
      where id = v_inv.client_id
      limit 1;

      if v_client_name is null then v_client_name := v_cli.name; end if;
      if v_client_addr is null then v_client_addr := v_cli.invoice_address; end if;
      if v_client_email is null then v_client_email := v_cli.primary_invoice_email; end if;

      if v_vat_chargeable is null then
        v_vat_chargeable := coalesce(v_cli.vat_chargeable, true);
      end if;

      if v_terms_days is null then
        v_terms_days := coalesce(v_cli.payment_terms_days, 30);
      end if;
    end;
  end if;

  -- Applied VAT % for the credit note:
  -- Prefer original snapshot applied_vat_rate_pct, else compute anchored to original issued date.
  v_applied_vat := null;
  if (v_base_hdr ? 'applied_vat_rate_pct') then
    begin
      v_applied_vat := (v_base_hdr->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_applied_vat := null;
    end;
  end if;

  if v_applied_vat is null then
    select coalesce(sf.vat_rate_pct, 20)
    into v_global_vat
    from public.settings_finance_pick(v_anchor_ymd) sf
    limit 1;

    select cs.vat_rate_pct
    into v_client_vat_override
    from public.client_settings cs
    where cs.client_id = v_inv.client_id
      and cs.effective_from <= v_anchor_ymd
    order by cs.effective_from desc
    limit 1;

    v_applied_vat := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_global_vat, 20)
    end;
  else
    if v_vat_chargeable = false then
      v_applied_vat := 0;
    end if;
  end if;

  v_due_at := v_now + make_interval(days => coalesce(v_terms_days, 30));

  -- Create the credit note invoice row (no totals/lines here, matching your JS)
   insert into public.invoices (
    client_id,
    type,
    status,
    status_date_utc,
    issued_at_utc,
    due_at_utc,
    subtotal_ex_vat,
    vat_amount,
    total_inc_vat,
    original_invoice_id,
    header_snapshot_json
  )
  values (
    v_inv.client_id,
    'CREDIT_NOTE'::public.invoice_type_enum,
    'ISSUED'::public.invoice_status_enum,
    v_now,
    v_now,
    v_due_at,
    0,
    0,
    0,
    v_inv.id,
    jsonb_build_object(
      'client_id', v_inv.client_id::text,
      'client_name', v_client_name,
      'client_invoice_address', v_client_addr,
      'client_primary_invoice_email', v_client_email,
      'vat_chargeable', coalesce(v_vat_chargeable, true),
      'applied_vat_rate_pct', coalesce(v_applied_vat, 0),
      'payment_terms_days', coalesce(v_terms_days, 30),
      'issued_at_utc', to_jsonb(v_now),
      'due_at_utc', to_jsonb(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', v_bank,
      'vat_registration_number', v_vat_reg,
      'meta', jsonb_build_object(
        'source', 'CREDIT_NOTE',
        'original_invoice_id', v_inv.id::text,
        'vat_anchor_ymd', v_anchor_ymd::text,
        'original_invoice_issued_at_utc', to_jsonb(v_original_issued_at)
      )
    )
  )
  returning id into v_credit_id;


  -- Unlock snapshots locked by the original invoice
  select array_agg(distinct tf.timesheet_id)
  into v_ts_ids
  from public.timesheets_financials tf
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null;

  unlocked_snapshots := coalesce(array_length(v_ts_ids, 1), 0);

  if unlocked_snapshots > 0 then
    update public.timesheets_financials tf
    set locked_by_invoice_id = null,
        locked_at_utc = null,
        unlocked_by_credit_note_id = v_credit_id,
        is_stale = true,
        stale_reason = 'UNLOCKED_BY_CREDIT',
        updated_at = v_now
    where tf.is_current = true
      and tf.locked_by_invoice_id = p_invoice_id;

    -- Enqueue recompute (idempotent)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    select
      x.timesheet_id,
      'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,
      v_now,
      null,
      v_now
    from (select unnest(v_ts_ids) as timesheet_id) x
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;

  credit_note_id := v_credit_id;
  return next;
end;
$$;


-- ============================================================
-- Invoice / Credit Note numbering on creation (safe to rerun)
-- - invoice_no is TEXT
-- - starts at 1000001
-- - assigned only if invoice_no is NULL/blank on insert
-- ============================================================

-- 1) Sequence (shared for invoices + credit notes)
create sequence if not exists public.invoice_no_seq
  start with 1000001
  increment by 1
  minvalue 1
  cache 1;

-- 2) Next-number helper
create or replace function public.invoice_no_next()
returns text
language sql
volatile
as $$
  select nextval('public.invoice_no_seq')::text;
$$;

-- 3) Trigger function: set invoice_no on insert if missing
create or replace function public.trg_invoices_set_invoice_no()
returns trigger
language plpgsql
as $$
begin
  if new.invoice_no is null or btrim(new.invoice_no) = '' then
    new.invoice_no := public.invoice_no_next();
  end if;
  return new;
end;
$$;




-- 4) Trigger: BEFORE INSERT on invoices
drop trigger if exists trg_invoices_set_invoice_no_bi on public.invoices;

create trigger trg_invoices_set_invoice_no_bi
before insert on public.invoices
for each row
execute function public.trg_invoices_set_invoice_no();
