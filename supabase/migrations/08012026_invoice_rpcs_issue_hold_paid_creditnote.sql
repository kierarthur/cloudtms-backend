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


  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
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

  -- DEBUG: compute counts and write one audit row (no effect unless enabled)
  if v_invoice_debug then
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
        left join lateral (
          select cs0.auto_invoice_default
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
          and coalesce(cs.auto_invoice_default, false) = true
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select anchor_ymd from anchor),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select n from already)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      -- Do not impact enqueue behaviour if audit function or debug queries fail
      null;
    end;
  end if;

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


  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
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

  -- DEBUG: compute counts and write one audit row (no effect unless enabled)
  if v_invoice_debug then
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
        left join lateral (
          select cs0.auto_invoice_default
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
          and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select anchor_ymd from anchor),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select n from already)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_AUTO_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

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

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_details jsonb := '{}'::jsonb;

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

  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids, 1), 0) = 0 then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REJECTED',
          jsonb_build_object(
            'reason', 'timesheet_ids_empty',
            'input_ids', to_jsonb(p_timesheet_ids),
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('hours:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'timesheet_ids[] required';
  end if;

  select array_agg(x order by x::text)
  into v_ids
  from (
    select distinct unnest(p_timesheet_ids) as x
  ) q
  where q.x is not null;

  if v_ids is null or coalesce(array_length(v_ids, 1), 0) = 0 then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REJECTED',
          jsonb_build_object(
            'reason', 'timesheet_ids_empty',
            'input_ids', to_jsonb(p_timesheet_ids),
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('hours:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
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
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REUSED',
          jsonb_build_object(
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now()),
            'timesheet_ids_count', coalesce(array_length(v_ids,1),0),
            'timesheet_ids_sig', v_sig,
            'existing_outbox_id', v_existing::text,
            'payload', v_payload
          ),
          'invoice_jobs_outbox',
          v_existing::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('HOURS'::text, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_HOURS_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'timesheet_ids_count', coalesce(array_length(v_ids,1),0),
          'timesheet_ids_sig', v_sig,
          'new_outbox_id', v_new::text,
          'payload', v_payload
        ),
        'invoice_jobs_outbox',
        v_new::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_new;
end;
$$;
-- ============================================================
-- CloudTMS: invoice_outbox_enqueue_by_week(p_client_id, p_invoice_week_start, p_actor_user_id, p_allow_early, p_meta)
-- UPDATED: enqueue BY_WEEK with correct allow_early + delayed-segment eligibility
--
-- Core rules implemented:
--  1) allow_early is ONLY about week-ending not yet passed (applies to SEGMENT + NON-SEGMENT)
--  2) allow_early NEVER makes delayed segments eligible early
--  3) delayed segments are eligible only when their invoice_target_week_start has been reached (<= London today)
--
-- Safety:
--  - Refuses enqueue if there are no due/invoiceable items for (client, invoice_week_start)
--  - Preserves idempotency: reuses existing outbox row for same (client_id, invoice_week_start),
--    but merges allow_early/actor/meta into payload for repeat calls.
--
-- Additional safety (minimal / no intended behaviour change):
--  - Ignore invalid segment entries (json null/non-object/missing segment_id) in SEGMENTS checks and debug.
--  - Serialize enqueue per (client_id, invoice_week_start) using pg_advisory_xact_lock to prevent
--    duplicate outbox rows from concurrent check+insert races.
-- ============================================================


create or replace function public.invoice_outbox_enqueue_by_week(
  p_client_id uuid,
  p_invoice_week_start date,
  p_actor_user_id uuid,
  p_allow_early boolean default false,
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

  v_london_today date := (now() at time zone 'Europe/London')::date;
  v_week_end date := (p_invoice_week_start + interval '6 days')::date;

  v_has_due boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_nonseg_due_count int := null;
  v_dbg_seg_due_count int := null;
  v_dbg_nonseg_due_sample jsonb := '[]'::jsonb;
  v_dbg_seg_due_sample jsonb := '[]'::jsonb;
  v_dbg_existing_outbox_id uuid := null;
  v_dbg_new_outbox_id uuid := null;

  -- extra breakdown (why NOT due)
  v_dbg_nonseg_any_count int := null;
  v_dbg_nonseg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_any_count int := null;
  v_dbg_seg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_delayed_not_due_count int := null;

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

  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  -- ------------------------------------------------------------
  -- ✅ Due/invoiceable existence check (prevents preview/enqueue mismatch)
  -- Implements the confirmed rules:
  --   - allow_early applies to SEGMENTS + NON-SEGMENTS week-ending gate
  --   - allow_early does NOT override delayed segments
  --   - delayed segments eligible only once delay date reached (target week start <= today)
  -- ------------------------------------------------------------
  select exists (
    -- NON-SEGMENTS (or SEGMENTS without per-segment delays): invoice week is natural week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and (
        coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
        or (
          coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
          and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
          and coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
        )
      )
      and (ts.week_ending_date::date - 6) = p_invoice_week_start
      and (p_allow_early = true or ts.week_ending_date::date < v_london_today)

    union all

    -- SEGMENTS mode: segment-level eligibility for this invoice week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
      and jsonb_typeof(seg) = 'object'
      and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
      and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null

      -- segment belongs to this invoice_week_start (target week, else natural week)
      and coalesce(
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
            (ts.week_ending_date::date - 6)
          ) = p_invoice_week_start

      and (
        -- DELAYED segment:
        -- invoice_target_week_start differs from natural week start
        -- eligibility depends ONLY on delay reaching (<= today), NOT allow_early
        (
          nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is not null
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <= v_london_today
        )
        or
        -- NON-DELAYED segment:
        -- (target is null OR equals natural week start)
        -- eligibility uses timesheet week-ending gate with allow_early
        (
          (
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is null
            or nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
          )
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
        )
      )
    limit 1
  ) into v_has_due;

  -- DEBUG: capture due breakdown (no effect unless enabled)
  if v_invoice_debug then
    begin
      -- NON-SEGMENTS (or segments mode with empty segments array but non-zero total)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_due_count,
        v_dbg_nonseg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          tf.processing_status::text as processing_status,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          coalesce(tf.total_charge_ex_vat,0)::numeric as total_charge_ex_vat,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and (ts.week_ending_date::date - 6) = p_invoice_week_start
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
          and (
            coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
            or (
              coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
              and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
              and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
              and coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
            )
          )
      ) s;

      -- SEGMENTS mode (segment-level eligibility)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where rn <= 25), '[]'::jsonb)
      into
        v_dbg_seg_due_count,
        v_dbg_seg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
          and (
            (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <= v_london_today
            )
            or
            (
              (
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
                or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
              )
              and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
            )
          )
      ) s;

      -- Breakdown: candidates for this client/week that are NOT due (helps explain why v_has_due=false)
      -- NON-SEGMENTS candidates (natural week start = invoice_week_start)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_any_count,
        v_dbg_nonseg_fail_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          tf.processing_status::text as processing_status,
          (tf.locked_by_invoice_id is not null) as locked_by_invoice,
          (ts.revoked_at is not null) as revoked,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          jsonb_typeof(tf.invoice_breakdown_json->'segments') as segments_type,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when (p_allow_early is not true) and (ts.week_ending_date::date >= v_london_today) then 'WEEK_NOT_PASSED'
            when (coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
                  and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
                  and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
                  and coalesce(tf.total_charge_ex_vat,0)::numeric = 0) then 'SEGMENTS_EMPTY_AND_ZERO_TOTAL'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and (ts.week_ending_date::date - 6) = p_invoice_week_start
      ) s;

      -- SEGMENTS candidates for this invoice_week_start (segment-level), including delayed-not-due reasons
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where rn <= 25), '[]'::jsonb),
        count(*) filter (where fail_reason = 'DELAYED_NOT_DUE')::int
      into
        v_dbg_seg_any_count,
        v_dbg_seg_fail_sample,
        v_dbg_seg_delayed_not_due_count
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is not null then 'SEGMENT_LOCKED'
            when (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date > v_london_today
            ) then 'DELAYED_NOT_DUE'
            when (
              (nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
               or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6))
              and (p_allow_early is not true)
              and (ts.week_ending_date::date >= v_london_today)
            ) then 'WEEK_NOT_PASSED'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
      ) s;
    exception when others then
      null;
    end;
  end if;

  if not v_has_due then
    -- Mirror existing UX: if week hasn't passed and allow_early is false, show that message.
    if (p_allow_early is not true) and (v_week_end >= v_london_today) then
      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
            jsonb_build_object(
              'reason', 'week_not_passed_allow_early_false',
              'client_id', p_client_id::text,
              'invoice_week_start', p_invoice_week_start::text,
              'week_ending', v_week_end::text,
              'london_today', v_london_today::text,
              'allow_early', coalesce(p_allow_early,false),
              'has_due', v_has_due,
              'nonseg_due_count', v_dbg_nonseg_due_count,
              'seg_due_count', v_dbg_seg_due_count,
              'nonseg_due_sample', v_dbg_nonseg_due_sample,
              'seg_due_sample', v_dbg_seg_due_sample,
              'nonseg_any_count', v_dbg_nonseg_any_count,
              'seg_any_count', v_dbg_seg_any_count,
              'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
              'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
              'seg_fail_sample', v_dbg_seg_fail_sample,
              'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
              'run_finished_at_utc', public._inv_iso_utc(now())
            ),
            'invoice_jobs_outbox',
            ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
            null,
            'INVOICE_DEBUG',
            null, null, null
          );
        exception when others then
          null;
        end;
      end if;
      raise exception 'Week ending % has not passed (London today=%). Use allow_early to override.', v_week_end, v_london_today;
    end if;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
          jsonb_build_object(
            'reason', 'no_invoiceable_items',
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'nonseg_due_sample', v_dbg_nonseg_due_sample,
            'seg_due_sample', v_dbg_seg_due_sample,
              'nonseg_any_count', v_dbg_nonseg_any_count,
              'seg_any_count', v_dbg_seg_any_count,
              'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
              'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
              'seg_fail_sample', v_dbg_seg_fail_sample,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'No invoiceable timesheets/segments for client=% and invoice_week_start=%', p_client_id, p_invoice_week_start;
  end if;

  -- Build payload
  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text,
    'allow_early', coalesce(p_allow_early, false)
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

  -- ✅ Concurrency guard: serialize enqueue per (client_id, invoice_week_start)
  -- Prevents duplicate BY_WEEK outbox rows from concurrent check+insert races.
  perform pg_advisory_xact_lock(
    hashtext(p_client_id::text),
    (p_invoice_week_start - date '2000-01-01')::int
  );

  -- Idempotent: reuse existing outbox row if present
  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    -- Merge allow_early/actor/meta into existing payload so subsequent calls are consistent
    update public.invoice_jobs_outbox o
    set payload = coalesce(o.payload, '{}'::jsonb) || v_payload
    where o.id = v_existing;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REUSED',
          jsonb_build_object(
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now()),
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'existing_outbox_id', v_existing::text,
            'payload_merge', v_payload
          ),
          'invoice_jobs_outbox',
          v_existing::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'client_id', p_client_id::text,
          'invoice_week_start', p_invoice_week_start::text,
          'week_ending', v_week_end::text,
          'london_today', v_london_today::text,
          'allow_early', coalesce(p_allow_early,false),
          'has_due', v_has_due,
          'nonseg_due_count', v_dbg_nonseg_due_count,
          'seg_due_count', v_dbg_seg_due_count,
          'new_outbox_id', v_new::text,
          'payload', v_payload
        ),
        'invoice_jobs_outbox',
        v_new::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

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
--
-- ✅ Fix fully implemented (minimal / no intended behaviour change):
--   - Serialize BY_WEEK and HOURS idempotency check+insert using pg_advisory_xact_lock
--     to prevent duplicate outbox rows from concurrent callers.
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

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();

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

  if v_kind = '' then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_REJECTED',
          jsonb_build_object(
            'reason', 'kind_required',
            'kind_input', p_kind,
            'payload_input', p_payload,
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('enqueue:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'kind is required';
  end if;

  if jsonb_typeof(v_payload) <> 'object' then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_REJECTED',
          jsonb_build_object(
            'reason', 'payload_not_object',
            'kind', v_kind,
            'payload_type', jsonb_typeof(v_payload),
            'payload_input', p_payload,
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('enqueue:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
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
      -- ✅ Concurrency guard: serialize check+insert for (client_id, invoice_week_start)
      -- Uses hashtext on both text keys to avoid new casting failures / behaviour changes.
      perform pg_advisory_xact_lock(hashtext(v_client_id), hashtext(v_week_start));

      select o.id
      into v_existing
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = v_client_id
        and (o.payload->>'invoice_week_start') = v_week_start
      order by o.created_at desc
      limit 1;

      if v_existing is not null then
        if v_invoice_debug then
          begin
            perform public._inv_write_audit(
              p_actor_user_id,
              'INVOICE_OUTBOX_ENQUEUE_REUSED',
              jsonb_build_object(
                'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
                'run_finished_at_utc', public._inv_iso_utc(now()),
                'kind', v_kind,
                'client_id', v_client_id,
                'invoice_week_start', v_week_start,
                'existing_outbox_id', v_existing::text,
                'payload', v_payload
              ),
              'invoice_jobs_outbox',
              v_existing::text,
              null,
              'INVOICE_DEBUG',
              null, null, null
            );
          exception when others then
            null;
          end;
        end if;
        return v_existing;
      end if;
    end if;

  elsif v_kind = 'HOURS' then
    v_sig := nullif(btrim(coalesce(v_payload->>'timesheet_ids_sig','')), '');

    if v_sig is not null then
      -- ✅ Concurrency guard: serialize check+insert for (kind=HOURS, timesheet_ids_sig)
      perform pg_advisory_xact_lock(hashtext(v_kind), hashtext(v_sig));

      select o.id
      into v_existing
      from public.invoice_jobs_outbox o
      where o.kind = 'HOURS'
        and (o.payload->>'timesheet_ids_sig') = v_sig
      order by o.created_at desc
      limit 1;

      if v_existing is not null then
        if v_invoice_debug then
          begin
            perform public._inv_write_audit(
              p_actor_user_id,
              'INVOICE_OUTBOX_ENQUEUE_REUSED',
              jsonb_build_object(
                'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
                'run_finished_at_utc', public._inv_iso_utc(now()),
                'kind', v_kind,
                'client_id', v_client_id,
                'invoice_week_start', v_week_start,
                'existing_outbox_id', v_existing::text,
                'payload', v_payload
              ),
              'invoice_jobs_outbox',
              v_existing::text,
              null,
              'INVOICE_DEBUG',
              null, null, null
            );
          exception when others then
            null;
          end;
        end if;
        return v_existing;
      end if;
    end if;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values (v_kind, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'kind', v_kind,
          'new_outbox_id', v_new::text,
          'payload', v_payload
        ),
        'invoice_jobs_outbox',
        v_new::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

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

create or replace function public.invoice_batch_issue_candidates(
  p_allow_early boolean default false,
  p_limit integer default 2000
)
returns jsonb
language sql
stable
security definer
set search_path = public
as $function$
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
),
inv_base as (
  select
    i.id as invoice_id,
    i.client_id,
    i.invoice_no,
    i.status::text as status,
    i.on_hold_reason,
    coalesce(i.subtotal_ex_vat, 0)::numeric as subtotal_ex_vat,
    coalesce(i.vat_amount, 0)::numeric as vat_amount,
    coalesce(i.total_inc_vat, 0)::numeric as total_inc_vat,
    i.header_snapshot_json,
    coalesce(i.do_not_send, false) as do_not_send,

    -- detect self-bill from header meta (robust string test)
    (
      lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', i.header_snapshot_json->>'self_bill', '')) in ('true','t','1','yes')
    ) as is_self_bill,

    -- week_start from header meta (robust)
    nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}',
                          i.header_snapshot_json->>'invoice_week_start', '')), '') as hdr_week_start_txt

  from public.invoices i
  where i.type::text = 'INVOICE'
    and i.status::text in ('DRAFT','ON_HOLD')
  order by i.created_at desc nulls last
  limit greatest(1, least(coalesce(p_limit, 2000), 20000))
),
inv_week as (
  select
    b.invoice_id,
    b.client_id,
    b.invoice_no,
    b.status,
    b.on_hold_reason,
    b.subtotal_ex_vat,
    b.vat_amount,
    b.total_inc_vat,
    b.is_self_bill,
    b.do_not_send,
    b.header_snapshot_json,

    -- compute week_start from header meta (if parseable)
    case
      when b.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$' then b.hdr_week_start_txt::date
      else null::date
    end as invoice_week_start,

    -- compute week_end from invoice week (NOT from timesheets.week_ending_date; avoids delayed-segment mismatch)
    case
      when b.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$'
        then (b.hdr_week_start_txt::date + 6)
      else null::date
    end as week_ending_date

  from inv_base b
),
filtered as (
  select
    w.*,
    c.name as client_name
  from inv_week w
  join public.clients c on c.id = w.client_id
  cross join anchor a
  where
    (p_allow_early = true)
    or (w.week_ending_date is null)              -- if unknown week_end, don't block listing
    or (w.week_ending_date < a.anchor_ymd)
),
inv_worked_ts as (
  -- timesheet ids on the invoice that contribute WORKED content (hours/additional rates)
  select distinct
    il.invoice_id,
    il.timesheet_id
  from public.invoice_lines il
  where il.invoice_id in (select invoice_id from filtered)
    and il.timesheet_id is not null
    and (
      upper(coalesce(il.meta_json->>'line_type','')) like 'HOURS%'
      or upper(coalesce(il.meta_json->>'line_type','')) like 'ADDITIONAL_RATE%'
    )
),
inv_refs as (
  -- issue-time reference signals, ONLY for timesheets that have WORKED content on this invoice
  select
    w.invoice_id,
    bool_or(coalesce(pc.reference_number_required_to_issue_invoice, false)) as refs_required_to_issue,
    bool_or(coalesce(pc.issue_missing_reference, false)) as any_issue_missing_reference,
    coalesce(sum(coalesce(pc.issue_missing_reference_count, 0))::int, 0) as issue_missing_reference_count,
    coalesce(
      array_agg(pc.timesheet_id::text order by pc.timesheet_id::text) filter (where coalesce(pc.issue_missing_reference, false) = true),
      array[]::text[]
    ) as issue_missing_timesheet_ids
  from inv_worked_ts w
  join public.v_ts_invoice_precheck pc
    on pc.timesheet_id = w.timesheet_id
  group by w.invoice_id
),
filtered2 as (
  select
    f.*,
    coalesce(r.refs_required_to_issue, false) as refs_required_to_issue,
    (coalesce(r.refs_required_to_issue, false) and coalesce(r.any_issue_missing_reference, false)) as issue_blocked_missing_refs,
    coalesce(r.issue_missing_reference_count, 0) as issue_missing_reference_count,
    coalesce(r.issue_missing_timesheet_ids, array[]::text[]) as issue_missing_timesheet_ids
  from filtered f
  left join inv_refs r
    on r.invoice_id = f.invoice_id
),
weeks as (
  select
    f.client_id,
    max(f.client_name) as client_name,
    f.week_ending_date,
    f.invoice_week_start,

    round(coalesce(sum(f.subtotal_ex_vat),0),2) as subtotal_ex_vat_sum,
    round(coalesce(sum(f.total_inc_vat),0),2) as total_inc_vat_sum,

    jsonb_agg(
      jsonb_build_object(
        'invoice_id', f.invoice_id::text,
        'invoice_no', f.invoice_no,
        'status', f.status,
        'on_hold_reason', f.on_hold_reason,
        'subtotal_ex_vat', round(coalesce(f.subtotal_ex_vat,0),2),
        'vat_amount', round(coalesce(f.vat_amount,0),2),
        'total_inc_vat', round(coalesce(f.total_inc_vat,0),2),
        'is_self_bill', f.is_self_bill,
        'do_not_send', f.do_not_send,

        -- issue-time reference gating signals (matches new refs-to-issue logic)
        'reference_number_required_to_issue_invoice', f.refs_required_to_issue,
        'issue_blocked_missing_refs', f.issue_blocked_missing_refs,
        'issue_missing_reference_count', f.issue_missing_reference_count,
        'issue_missing_timesheet_ids', to_jsonb(f.issue_missing_timesheet_ids)
      )
      order by f.status desc, f.invoice_no nulls last, f.invoice_id::text
    ) as invoices
  from filtered2 f
  group by f.client_id, f.week_ending_date, f.invoice_week_start
),
clients as (
  select
    w.client_id,
    max(w.client_name) as client_name,
    jsonb_agg(
      jsonb_build_object(
        'invoice_week_start', case when w.invoice_week_start is null then null else w.invoice_week_start::text end,
        'week_ending_date', case when w.week_ending_date is null then null else w.week_ending_date::text end,
        'subtotal_ex_vat_sum', w.subtotal_ex_vat_sum,
        'total_inc_vat_sum', w.total_inc_vat_sum,
        'invoices', w.invoices
      )
      order by w.week_ending_date desc nulls last
    ) as weeks
  from weeks w
  group by w.client_id
)
select coalesce(
  jsonb_agg(
    jsonb_build_object(
      'client_id', c.client_id::text,
      'client_name', c.client_name,
      'weeks', c.weeks
    )
    order by c.client_name nulls last, c.client_id::text
  ),
  '[]'::jsonb
)
from clients c;
$function$;




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
-- PATCH: public.invoice_issue_one
-- Snapshot client_settings.group_nightsat_sunbh into invoices.header_snapshot_json
-- at issue time if missing (audit-stable render behaviour).
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
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
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_terms_days int := null;
  v_due_at timestamptz := null;
  v_hdr jsonb := null;
  v_client_id uuid := null;

  v_group_nightsat_sunbh boolean := null;

  v_ts_ids uuid[];
  v_worked_ts_ids uuid[];

  v_reasons text[] := array[]::text[];
  v_precheck_reasons text[] := array[]::text[];
  v_hr_reasons text[] := array[]::text[];
  v_issue_ref_reasons text[] := array[]::text[];

  v_on_hold_reason text := null;

  -- ref-to-issue flag (effective: contract override aware via v_ts_invoice_precheck)
  v_ref_required_to_issue boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_started timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_ts jsonb := '[]'::jsonb;

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

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','start',
    'now_utc', public._inv_iso_utc(v_now),
    'anchor_ymd', v_anchor_ymd::text,
    'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end
  ));

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

      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_ISSUE_DEBUG',
            jsonb_build_object(
              'result','ALREADY_ISSUED',
              'invoice_id', p_invoice_id::text,
              'status', v_inv.status::text,
              'issued_at_utc', to_jsonb(v_inv.issued_at_utc),
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

      return next;
      return;
    end if;

    -- ✅ HARD BLOCK:
    -- If invoice is currently ON_HOLD, do not re-evaluate blockers; require UNHOLD first.
    if v_inv.status::text = 'ON_HOLD' then
      status := 'ON_HOLD';
      issued_at_utc := null;
      on_hold_reason := 'Unhold first';
      reasons := array['Unhold first']::text[];
      return next;
      return;
    end if;

    if v_inv.status::text not in ('DRAFT','ON_HOLD') then
      raise exception 'Only DRAFT/ON_HOLD invoices can be issued (current status=%)', v_inv.status::text;
    end if;
  end;

  -- Invoice client_id (used for settings lookup and header snapshot)
  select i.client_id
  into v_client_id
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Timesheets on invoice
  select array_agg(distinct l.timesheet_id)
  into v_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    raise exception 'Invoice has no timesheets to validate';
  end if;

  -- Timesheets that have WORKED content on this invoice (hours/additional only)
  select array_agg(distinct l.timesheet_id)
  into v_worked_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null
    and upper(coalesce(l.meta_json->>'line_type','')) in (
      'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
    );

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','collected_timesheets',
    'timesheet_count', coalesce(array_length(v_ts_ids,1),0),
    'worked_timesheet_count', coalesce(array_length(v_worked_ts_ids,1),0)
  ));

  -- ✅ Determine whether ref-to-issue is enabled for THIS invoice (contract override aware)
  -- We treat this as: any worked timesheet on the invoice has reference_number_required_to_issue_invoice = true
  begin
    select coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false)
    into v_ref_required_to_issue
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id;
  exception when others then
    v_ref_required_to_issue := false;
  end;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_issue_ref_policy',
    'reference_number_required_to_issue_invoice', v_ref_required_to_issue
  ));

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

  -- ------------------------------------------------------------
  -- 3) ISSUE-TIME reference gating (contract override aware, per-timesheet)
  -- Uses v_ts_invoice_precheck.issue_missing_reference which is already:
  --   (reference_number_required_to_issue_invoice = true)
  --   AND (total_hours > 0)
  --   AND (missing refs for LOCKED positive segments, or normal weekly/daily rules)
  -- ------------------------------------------------------------
  
  -- 3) ISSUE-TIME reference gating (scoped to this invoice)
  -- Only segments locked to THIS invoice can block issuing (so other-invoice segments never block).
  select array_agg(
    case
      when t.timesheet_id is null then null
      when t.issue_missing_count > 0
        then 'TS '||t.timesheet_id::text||': missing reference/PO for '||t.issue_missing_count::text||' shift(s) (required to issue)'
      else null
    end
  )
  into v_issue_ref_reasons
  from (
    select
      x.timesheet_id,
      case
        when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

        when tf.invoice_breakdown_json is not null
          and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
          and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
        then (
          select count(*)::int
          from jsonb_array_elements(tf.invoice_breakdown_json->'segments') as s(seg)
          where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
            and (
              (
                (case when coalesce(s.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_day')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_night')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sat')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sun')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_bh')::numeric else 0 end)
              ) > 0
              or (case when coalesce(s.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'charge_amount')::numeric else 0 end) > 0
            )
            and nullif(btrim(coalesce(s.seg->>'ref_num','')), '') is null
        )

        else (
          case
            when public._inv_timesheet_has_issue_reference(
              ts.sheet_scope::text,
              coalesce(ts.submission_mode::text,''),
              ts.reference_number,
              ts.day_references_json,
              ts.actual_schedule_json
            )
            then 0 else 1 end
        )
      end as issue_missing_count
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id
    left join public.timesheets ts
      on ts.timesheet_id = x.timesheet_id
     and ts.is_current = true
    left join public.timesheets_financials tf
      on tf.timesheet_id = x.timesheet_id
     and tf.is_current = true
  ) t;


  v_issue_ref_reasons := array_remove(v_issue_ref_reasons, null);

  -- Merge blockers
  v_reasons := array_cat(v_precheck_reasons, v_hr_reasons);
  v_reasons := array_cat(v_reasons, v_issue_ref_reasons);
  v_reasons := array_remove(v_reasons, null);

  -- Debug per-timesheet snapshot (only if enabled)
  if v_invoice_debug then
    begin
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'timesheet_id', x.timesheet_id::text,
          'has_worked_lines', (v_worked_ts_ids is not null and x.timesheet_id = any(v_worked_ts_ids)),
          'precheck_status', coalesce(pc.precheck_status,'')::text,
          'ref_required_to_issue_effective', coalesce(pc.reference_number_required_to_issue_invoice,false),
          'issue_missing_reference_on_invoice', ((
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_issue_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ) > 0),
          'issue_missing_reference_on_invoice_count', (
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_issue_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ),
          'hr_required', coalesce(s.hr_validation_required_for_invoice,false),
          'validation_status', case when s.validation_status is null then null else s.validation_status::text end
        )
      ), '[]'::jsonb)
      into v_dbg_ts
      from unnest(v_ts_ids) as x(timesheet_id)
      left join public.v_ts_invoice_precheck pc on pc.timesheet_id = x.timesheet_id
      left join public.v_timesheets_summary_base s on s.timesheet_id = x.timesheet_id;
    exception when others then
      v_dbg_ts := '[]'::jsonb;
    end;
  end if;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_blockers',
    'precheck_reasons_count', coalesce(array_length(v_precheck_reasons,1),0),
    'hr_reasons_count', coalesce(array_length(v_hr_reasons,1),0),
    'issue_ref_reasons_count', coalesce(array_length(v_issue_ref_reasons,1),0),
    'total_reasons_count', coalesce(array_length(v_reasons,1),0)
  ));

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

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_ISSUE_DEBUG',
          jsonb_build_object(
            'result','ON_HOLD',
            'invoice_id', p_invoice_id::text,
            'client_id', case when v_client_id is null then null else v_client_id::text end,
            'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
            'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
            'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
            'reasons', to_jsonb(coalesce(v_reasons, array[]::text[])),
            'timesheets_debug', v_dbg_ts,
            'steps', v_dbg_steps,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
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

    return next;
    return;
  end if;

  -- ------------------------------------------------------------
  -- No blockers => issue
  -- ------------------------------------------------------------
  select i.client_id, i.header_snapshot_json
  into v_client_id, v_hdr
  from public.invoices i
  where i.id = p_invoice_id;

  if v_hdr is null or jsonb_typeof(v_hdr) <> 'object' then
    v_hdr := '{}'::jsonb;
  end if;

  if not (v_hdr ? 'group_nightsat_sunbh') then
    select cs0.group_nightsat_sunbh
    into v_group_nightsat_sunbh
    from public.client_settings cs0
    where cs0.client_id = v_client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;

    v_hdr := v_hdr || jsonb_build_object('group_nightsat_sunbh', coalesce(v_group_nightsat_sunbh, false));
  end if;

  begin
    if (v_hdr ? 'payment_terms_days') then
      v_terms_days := (v_hdr->>'payment_terms_days')::int;
    end if;
  exception when others then
    v_terms_days := null;
  end;

  if v_terms_days is null then
    begin
      select c.payment_terms_days
      into v_terms_days
      from public.clients c
      where c.id = v_client_id;
    exception when others then
      v_terms_days := null;
    end;
  end if;

  v_terms_days := coalesce(v_terms_days, 30);
  v_due_at := v_now + make_interval(days => v_terms_days);

  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = v_now,
      due_at_utc = v_due_at,
      on_hold_reason = null,
      header_snapshot_json = v_hdr
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

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ISSUED',
          'invoice_id', p_invoice_id::text,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
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

  return next;

exception when others then
  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ERROR',
          'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'sqlstate', sqlstate,
          'error', sqlerrm,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
        ),
        'invoices',
        case when p_invoice_id is null then null else p_invoice_id::text end,
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
  set status = case
        when paid_at_utc is not null then 'PAID'::public.invoice_status_enum
        when issued_at_utc is not null then 'ISSUED'::public.invoice_status_enum
        else 'DRAFT'::public.invoice_status_enum
      end,
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
      due_at_utc = null,
      on_hold_reason = null,
      invoice_pdf_r2_key = null,
      invoice_pdf_generated_at_utc = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNISSUED',
    null,
    jsonb_build_object(
      'clear_pdf_requested', p_clear_pdf,
      'clear_pdf_applied', true
    ),
    null,
    p_actor_user_id
  );

  status := 'DRAFT';
  cleared_pdf := true;
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
  perform public.invoice_recompute_totals(p_invoice_id);
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
-- ============================================================
-- CloudTMS Patch: invoice_render_manifest (UPDATED + FIXED v5)
-- ============================================================
-- Adds:
-- 10) timesheet_reference_sources_by_id: JSON object keyed by timesheet_id
--     containing { reference_number, day_references_json, actual_schedule_json }
--     built from:
--       - all invoice line timesheets, PLUS
--       - any additional timesheet_ids present in reference_rows
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================

create or replace function public.invoice_render_manifest(p_invoice_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_debug boolean := false;
  v_dbg_started timestamptz := now();
  v_manifest jsonb := null;

  v_lines_count int := 0;
  v_timesheet_ids_count int := 0;
  v_evidence_count int := 0;
  v_ts_evidence_count int := 0;
  v_ev_other_count int := 0;
  v_history_count int := 0;
  v_seg_keys_count int := 0;

  v_sqlstate text := null;
  v_err text := null;
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
      ts.manual_pdf_r2_key,
      coalesce(
        l.paper_ts_r2_key,
        ts.manual_pdf_r2_key,
        case
          when l.timesheet_id is not null
            then ('docs-pdf/timesheets/ts_' || l.timesheet_id::text || '.pdf')
          else null
        end
      ) as effective_paper_ts_r2_key
    from public.invoice_lines l
    left join public.timesheets ts
      on ts.timesheet_id = l.timesheet_id
    where l.invoice_id = p_invoice_id
    order by l.created_at asc
  ),
  ts_ids as (
    select distinct timesheet_id
    from lines
    where timesheet_id is not null
  ),

  -- ✅ UPDATED: reference rows joined to candidate display name (for UI display)
  -- Added TSFIN candidate fallback so SEGMENTS/NHSP rows still render candidate when contract linkage is missing.
  ref_rows_joined as (
    select
      r.*,
      con0.id as contract_id,
      coalesce(con0.candidate_id, tf0.candidate_id) as candidate_id,
      coalesce(cand_contract.display_name, cand_tf.display_name) as candidate_display
    from public.invoice_reference_rows(p_invoice_id) r
    left join public.timesheets ts0
      on ts0.timesheet_id = r.timesheet_id
    left join public.contracts con0
      on con0.id = ts0.contract_id
    left join public.timesheets_financials tf0
      on tf0.timesheet_id = r.timesheet_id
     and tf0.is_current = true
    left join public.candidates cand_contract
      on cand_contract.id = con0.candidate_id
    left join public.candidates cand_tf
      on cand_tf.id = tf0.candidate_id
  ),

  -- ✅ additional timesheet ids referenced by reference rows (may include ids not present in lines)
  ref_ts_ids as (
    select distinct r.timesheet_id
    from ref_rows_joined r
    where r.timesheet_id is not null
  ),

  -- ✅ union set for reference-source hydration
  ts_ids_for_ref_sources as (
    select timesheet_id from ts_ids
    union
    select timesheet_id from ref_ts_ids
  ),

  -- ✅ TSFIN sources for timesheets that may have SEGMENTS refs (NHSP/HR/etc)
  tsfin_ref_sources as (
    select
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids_for_ref_sources)
  ),

  -- ✅ sources needed by FE to rebuild reference update payloads without extra calls
  --   - If timesheets.actual_schedule_json is present and non-empty, use it.
  --   - Else if TSFIN is SEGMENTS mode, derive an editable schedule-like array from TSFIN segments
  --     so multi-shift/day (NHSP/HR/manual) works via segment_id/start/end matching.
  ts_reference_sources as (
    select
      t.timesheet_id,
      t.reference_number,
      t.day_references_json,
      case
        when tf.invoice_breakdown_json is not null
          and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
          and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
        then coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'segment_id', seg->>'segment_id',
              'date', seg->>'date',
              'start_utc', seg->>'start_utc',
              'end_utc', seg->>'end_utc',
              -- legacy-friendly aliases (FE/DB matching may use either)
              'start', seg->>'start_utc',
              'end', seg->>'end_utc',
              'ref_num', seg->>'ref_num',
              'source_system', seg->>'source_system'
            )
            order by
              coalesce(seg->>'date',''),
              coalesce(seg->>'start_utc',''),
              coalesce(seg->>'segment_id','')
          )
          from jsonb_array_elements(tf.invoice_breakdown_json->'segments') seg
        ), '[]'::jsonb)

        when t.actual_schedule_json is not null
          and jsonb_typeof(t.actual_schedule_json) = 'array'
          and jsonb_array_length(t.actual_schedule_json) > 0
        then t.actual_schedule_json

        else '[]'::jsonb
      end as actual_schedule_json
    from public.timesheets t
    left join tsfin_ref_sources tf
      on tf.timesheet_id = t.timesheet_id
    where t.timesheet_id in (select timesheet_id from ts_ids_for_ref_sources)
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
      tf.id as tsfin_id,
      tf.timesheet_id,
      tf.external_source_rows_json,

      -- ✅ Mileage fields for invoice itemisation
      tf.mileage_units,
      tf.mileage_pay_rate,
      tf.mileage_charge_rate
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids)
  ),

  tsfin_segments as (
    select
      tf.id as tsfin_id,
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids)
  ),
  seg_stats as (
    select
      t.timesheet_id,
      t.tsfin_id,

      -- ONLY segments on THIS invoice
      coalesce(
        jsonb_agg(
          s.seg
          order by coalesce(s.seg->>'date',''), coalesce(s.seg->>'segment_id','')
        ) filter (
          where s.seg is not null
            and s.locked_text = p_invoice_id::text
        ),
        '[]'::jsonb
      ) as invoiced_segments,

      -- counts (do not return the segments)
      (count(*) filter (where s.seg is not null and s.locked_text is null))::int
        as uninvoiced_segment_count,

      (count(*) filter (where s.seg is not null and s.locked_text is not null and s.locked_text <> p_invoice_id::text))::int
        as locked_elsewhere_segment_count

    from tsfin_segments t

    left join lateral (
      select
        value as seg,
        nullif(btrim(coalesce(value->>'invoice_locked_invoice_id','')), '') as locked_text
      from jsonb_array_elements(
        case
          when t.invoice_breakdown_json is not null
            and jsonb_typeof(t.invoice_breakdown_json) = 'object'
            and jsonb_typeof(t.invoice_breakdown_json->'segments') = 'array'
          then t.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) value
    ) s on true

    where upper(coalesce(t.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'

    group by t.timesheet_id, t.tsfin_id
  ),
  hist_audit as (
    select
      ae.id,
      ae.ts_utc,
      ae.actor_user_id,
      coalesce(ae.actor_display, tu.display_name, tu.email, 'CloudTMS server') as actor_display,
      coalesce(ae.actor_role_at_time, tu.role, 'system') as actor_role_at_time,
      ae.object_type,
      ae.object_id_text,
      ae.action,
      ae.before_json,
      ae.after_json,
      ae.reason,
      ae.ip,
      ae.user_agent,
      ae.correlation_id
    from public.audit_events ae
    left join public.tms_users tu
      on tu.id = ae.actor_user_id
    where ae.object_type in ('invoice','invoices')
      and ae.object_id_text = p_invoice_id::text
    order by ae.ts_utc desc, ae.id desc
    limit 500
  ),
  hist_mail as (
    select
      m.id as mail_outbox_id,
      m.created_at_utc as ts_utc,
      m.status,
      m."to" as to_email,
      m.subject,
      m.reference
    from public.mail_outbox m
    where upper(coalesce(m.type,'')) = 'INVOICE'
      and m.attachments is not null
      and jsonb_typeof(m.attachments) = 'array'
      and exists (
        select 1
        from jsonb_array_elements(m.attachments) a
        where btrim(coalesce(a->>'invoice_id','')) = p_invoice_id::text
      )
    order by m.created_at_utc desc, m.id desc
    limit 200
  ),
  history as (
    select
      jsonb_build_object(
        'kind','AUDIT',
        'id', ae.id::text,
        'ts_utc', to_jsonb(ae.ts_utc),
        'actor_user_id', case when ae.actor_user_id is null then null else ae.actor_user_id::text end,
        'actor_display', ae.actor_display,
        'actor_role_at_time', ae.actor_role_at_time,
        'action', ae.action,
        'reason', ae.reason,
        'object_type', ae.object_type,
        'object_id_text', ae.object_id_text,
        'before_json', ae.before_json,
        'after_json', ae.after_json,
        'ip', ae.ip,
        'user_agent', ae.user_agent,
        'correlation_id', ae.correlation_id
      ) as row_json,
      ae.ts_utc as ts_sort
    from hist_audit ae

    union all

    select
      jsonb_build_object(
        'kind','EMAIL',
        'mail_outbox_id', m.mail_outbox_id::text,
        'ts_utc', to_jsonb(m.ts_utc),
        'status', m.status::text,
        'to', m.to_email,
        'subject', m.subject,
        'reference', m.reference
      ) as row_json,
      m.ts_utc as ts_sort
    from hist_mail m
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

    -- ✅ mapping needed for segment edits (tsfin_id is invoice_apply_edits input)
    'tsfin_id_by_timesheet_id', coalesce((
      select jsonb_object_agg(
        t.timesheet_id::text,
        t.tsfin_id::text
      )
      from tsfin t
    ), '{}'::jsonb),

    -- ✅ mileage units/rates per timesheet (for PDF builder / UI without scanning lines)
    'mileage_by_timesheet_id', coalesce((
      select jsonb_object_agg(
        t.timesheet_id::text,
        jsonb_build_object(
          'mileage_units', t.mileage_units,
          'mileage_pay_rate', t.mileage_pay_rate,
          'mileage_charge_rate', t.mileage_charge_rate
        )
      )
      from tsfin t
    ), '{}'::jsonb),

    -- ✅ reference sources needed by FE to build reference update payloads with no extra calls
    'timesheet_reference_sources_by_id', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'reference_number', s.reference_number,
          'day_references_json', s.day_references_json,
          'actual_schedule_json', s.actual_schedule_json
        )
      )
      from ts_reference_sources s
    ), '{}'::jsonb),

    -- ✅ UPDATED: embed reference edit rows for zero-subrequest ref modal
    -- Adds candidate_display for UI rendering and deterministic row_key to stabilise FE staging identity.
    'reference_rows', coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'row_key', concat_ws(
            '::',
            r.timesheet_id::text,
            r.ref_target,
            coalesce(r.segment_id,''),
            coalesce(r.day_ymd::text,''),
            coalesce(r.start_utc::text,''),
            coalesce(r.end_utc::text,'')
          ),
          'timesheet_id', r.timesheet_id::text,
          'candidate_id', case when r.candidate_id is null then null else r.candidate_id::text end,
          'candidate_display', r.candidate_display,
          'sheet_scope', r.sheet_scope,
          'submission_mode', r.submission_mode,
          'ref_target', r.ref_target,
          'segment_id', r.segment_id,
          'day_ymd', r.day_ymd,
          'start_utc', r.start_utc,
          'end_utc', r.end_utc,
          'current_reference', r.current_reference,
          'is_required', r.is_required
        )
        order by
          r.timesheet_id::text,
          r.ref_target,
          r.day_ymd nulls last,
          r.start_utc nulls last,
          r.end_utc nulls last,
          r.segment_id nulls last
      )
      from ref_rows_joined r
    ), '[]'::jsonb),

    -- SEGMENTS: segment info for invoice modal expansion
    'segments_by_timesheet', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'tsfin_id', case when s.tsfin_id is null then null else s.tsfin_id::text end,
          'invoiced_segments', coalesce(s.invoiced_segments, '[]'::jsonb),
          'uninvoiced_segment_count', coalesce(s.uninvoiced_segment_count, 0),
          'locked_elsewhere_segment_count', coalesce(s.locked_elsewhere_segment_count, 0)
        )
      )
      from seg_stats s
    ), '{}'::jsonb),

    -- Alias required by brief
    'segments_on_invoice_by_timesheet', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'tsfin_id', case when s.tsfin_id is null then null else s.tsfin_id::text end,
          'invoiced_segments', coalesce(s.invoiced_segments, '[]'::jsonb),
          'uninvoiced_segment_count', coalesce(s.uninvoiced_segment_count, 0),
          'locked_elsewhere_segment_count', coalesce(s.locked_elsewhere_segment_count, 0)
        )
      )
      from seg_stats s
    ), '{}'::jsonb),

    -- Backward compatible aggregate (all evidence)
    'evidence', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
    ), '[]'::jsonb),

    -- New explicit splits
    'timesheet_evidence', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
      where upper(coalesce(ev.kind,'')) = 'TIMESHEET'
    ), '[]'::jsonb),

    'evidence_other', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
      where upper(coalesce(ev.kind,'')) <> 'TIMESHEET'
    ), '[]'::jsonb),

    'hr_source_rows_cache', coalesce((select jsonb_agg(to_jsonb(h.*)) from hr_cache h), '[]'::jsonb),
    'tsfin_external_source_rows', coalesce((select jsonb_agg(to_jsonb(t.*)) from tsfin t), '[]'::jsonb),

    -- Invoice history
    'history', coalesce((
      select jsonb_agg(h.row_json order by h.ts_sort desc)
      from history h
    ), '[]'::jsonb),

    -- email summary for UI label (Email vs Re-email)
    'email_summary', jsonb_build_object(
      'emailed_once', (coalesce((select email_count from email_summary),0) > 0),
      'email_count', coalesce((select email_count from email_summary),0),
      'last_email_at_utc', (select last_email_at_utc from email_summary)
    )
  )
  into v_manifest
  from inv;

  if v_manifest is null then
    v_manifest := '{}'::jsonb;
  end if;

  -- Extract simple counts for debug
  begin
    v_lines_count := coalesce(jsonb_array_length(coalesce(v_manifest->'lines','[]'::jsonb)), 0);
  exception when others then
    v_lines_count := 0;
  end;

  begin
    v_timesheet_ids_count := coalesce(jsonb_array_length(coalesce(v_manifest->'timesheet_ids','[]'::jsonb)), 0);
  exception when others then
    v_timesheet_ids_count := 0;
  end;

  begin
    v_evidence_count := coalesce(jsonb_array_length(coalesce(v_manifest->'evidence','[]'::jsonb)), 0);
  exception when others then
    v_evidence_count := 0;
  end;

  begin
    v_ts_evidence_count := coalesce(jsonb_array_length(coalesce(v_manifest->'timesheet_evidence','[]'::jsonb)), 0);
  exception when others then
    v_ts_evidence_count := 0;
  end;

  begin
    v_ev_other_count := coalesce(jsonb_array_length(coalesce(v_manifest->'evidence_other','[]'::jsonb)), 0);
  exception when others then
    v_ev_other_count := 0;
  end;

  begin
    v_history_count := coalesce(jsonb_array_length(coalesce(v_manifest->'history','[]'::jsonb)), 0);
  exception when others then
    v_history_count := 0;
  end;

  begin
    select coalesce(count(*),0)
    into v_seg_keys_count
    from jsonb_object_keys(coalesce(v_manifest->'segments_by_timesheet','{}'::jsonb)) k;
  exception when others then
    v_seg_keys_count := 0;
  end;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_RENDER_MANIFEST_DEBUG',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
        'run_finished_at_utc', public._inv_iso_utc(now()),
        'counts', jsonb_build_object(
          'lines', v_lines_count,
          'timesheet_ids', v_timesheet_ids_count,
          'evidence', v_evidence_count,
          'timesheet_evidence', v_ts_evidence_count,
          'evidence_other', v_ev_other_count,
          'history', v_history_count,
          'segments_by_timesheet_keys', v_seg_keys_count
        )
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

  return v_manifest;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_RENDER_MANIFEST_ERROR',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
        'run_failed_at_utc', public._inv_iso_utc(now()),
        'error', jsonb_build_object(
          'sqlstate', v_sqlstate,
          'message', v_err
        )
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
  -- ✅ keep original invoice totals/PDF consistent after unlock (idempotent)
  perform public.invoice_recompute_totals(p_invoice_id);

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
