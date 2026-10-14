

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

-- public.invoice_batch_issue_candidates moved to supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_batch_issue_candidates.sql.





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
    select distinct
      case
        when l.timesheet_id is not null then l.timesheet_id
        when l.meta_json is not null
          and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') is not null
          and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '')::uuid
        else null
      end as timesheet_id
    from lines l
    where
      l.timesheet_id is not null
      or (
        l.meta_json is not null
        and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') is not null
        and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      )
  ),

  -- ✅ UPDATED: reference rows joined to candidate display name (for UI display)
  -- Hardened candidate resolution via COALESCE(contract candidate vs TSFIN candidate).
  ref_rows_joined as (
    select
      r.*,
      con0.id as contract_id,
      coalesce(con0.candidate_id, tf0.candidate_id) as candidate_id,
      coalesce(
        nullif(btrim(coalesce(cand_contract.display_name,'')), ''),
        nullif(btrim(coalesce(cand_tf.display_name,'')), '')
      ) as candidate_display
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

  -- ✅ NEW: per-timesheet deterministic current refs signature (invoice-scoped)
  ref_sig_by_timesheet as (
    select
      r.timesheet_id,
      encode(
        extensions.digest(
          coalesce(
            string_agg(
              (
                concat_ws(
                  '|',
                  r.timesheet_id::text,
                  r.ref_target,
                  coalesce(r.segment_id,''),
                  coalesce(r.day_ymd::text,''),
                  coalesce(r.start_utc::text,''),
                  coalesce(r.end_utc::text,'')
                )
                || '=' || coalesce(r.current_reference,'')
              ),
              '||'
              ORDER BY
                concat_ws(
                  '|',
                  r.timesheet_id::text,
                  r.ref_target,
                  coalesce(r.segment_id,''),
                  coalesce(r.day_ymd::text,''),
                  coalesce(r.start_utc::text,''),
                  coalesce(r.end_utc::text,'')
                )
            ),
            ''
          ),
          'sha256'
        ),
        'hex'
      ) as current_refs_sig
    from ref_rows_joined r
    where r.timesheet_id is not null
    group by r.timesheet_id
  ),

  -- ✅ NEW: summary-derived exclusion flags (NHSP / no_timesheet_required) per timesheet
  ts_summary_flags as (
    select
      v.timesheet_id,
      coalesce(v.client_is_nhsp, false) as client_is_nhsp,
      coalesce(v.client_no_timesheet_required, false) as client_no_timesheet_required
    from public.v_timesheets_summary_base v
    where v.timesheet_id in (select timesheet_id from ts_ids)
  ),

  -- ✅ NEW: per-timesheet document flags (electronic regen + QR refs changed)
  -- ✅ FIX: exclude NHSP / no_timesheet_required from BOTH electronic and QR flags
  timesheet_doc_flags as (
    select
      ts.timesheet_id,
      jsonb_build_object(
        'electronic_refs_changed', flags.electronic_changed,
        'electronic_pdf_regen_required', flags.electronic_changed,
        'qr_refs_changed', flags.qr_changed,
        'reasons', reasons.reasons_json
      ) as flags_json
    from ts_ids tid
    join public.timesheets ts
      on ts.timesheet_id = tid.timesheet_id
    left join ref_sig_by_timesheet rs
      on rs.timesheet_id = ts.timesheet_id
    left join ts_summary_flags sf
      on sf.timesheet_id = ts.timesheet_id
    cross join lateral (
      select
        (
          (not x.is_excluded)
          and upper(coalesce(ts.submission_mode::text,'')) = 'ELECTRONIC'
          and ts.manual_pdf_r2_key is null
          and rs.current_refs_sig is not null
          and (ts.generated_pdf_refs_sig is null or ts.generated_pdf_refs_sig <> rs.current_refs_sig)
        ) as electronic_changed,

        (
          (not x.is_excluded)
          and (
            (
              ts.qr_status is not null
              or ts.qr_token is not null
              or ts.qr_last_sent_hash is not null
              or ts.qr_last_sent_at_utc is not null
            )
          )
          and rs.current_refs_sig is not null
          and (ts.qr_sent_refs_sig is null or ts.qr_sent_refs_sig <> rs.current_refs_sig)
        ) as qr_changed
      from (
        select
          (
            coalesce(sf.client_is_nhsp, false) = true
            or coalesce(sf.client_no_timesheet_required, false) = true
          ) as is_excluded
      ) x
    ) flags
    cross join lateral (
      select coalesce(jsonb_agg(z.reason) filter (where z.reason is not null), '[]'::jsonb) as reasons_json
      from (
        select
          case
            when flags.electronic_changed and ts.generated_pdf_refs_sig is null then 'ELECTRONIC_REFS_SIG_MISSING'
            when flags.electronic_changed then 'ELECTRONIC_REFS_SIG_DIFFERENT'
            else null
          end as reason
        union all
        select
          case
            when flags.qr_changed and ts.qr_sent_refs_sig is null then 'QR_REFS_SIG_MISSING'
            when flags.qr_changed then 'QR_REFS_SIG_DIFFERENT'
            else null
          end as reason
      ) z
    ) reasons
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
      r.header_rows,
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

    -- ✅ NEW: per-timesheet document flags for renderer (electronic regen + QR refs changed)
    'timesheet_doc_flags_by_id', coalesce((
      select jsonb_object_agg(
        f.timesheet_id::text,
        f.flags_json
      )
      from timesheet_doc_flags f
    ), '{}'::jsonb),

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
  v_existing_credit_id uuid;

  v_ts_ids uuid[];

  v_has_credit_note_created_at boolean := false;
  v_inserted_lines int := 0;

  v_cn_ex numeric := 0;
  v_cn_vat numeric := 0;
  v_cn_inc numeric := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- A4 marker column detection (safe even if migration not applied yet)
  begin
    select exists(
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'invoices'
        and c.column_name = 'credit_note_created_at_utc'
    )
    into v_has_credit_note_created_at;
  exception when others then
    v_has_credit_note_created_at := false;
  end;

  -- Lock original invoice (prevents concurrent double-credit)
  select i.*
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  for update;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot credit a CREDIT_NOTE';
  end if;

  if jsonb_typeof(v_inv.header_snapshot_json) = 'object' then
    v_base_hdr := v_inv.header_snapshot_json;
  end if;

  -- Idempotence: reuse an existing credit note for this original invoice (if any)
  select i2.id
  into v_existing_credit_id
  from public.invoices i2
  where i2.original_invoice_id = p_invoice_id
    and i2.type::text = 'CREDIT_NOTE'
  order by i2.issued_at_utc desc nulls last, i2.created_at desc, i2.id desc
  limit 1
  for update;

  if v_existing_credit_id is not null then
    v_credit_id := v_existing_credit_id;

    -- Ensure A4 marker is set on original invoice (idempotent)
    if v_has_credit_note_created_at then
      execute
        'update public.invoices set credit_note_created_at_utc = coalesce(credit_note_created_at_utc, $1), updated_at = $1 where id = $2'
      using v_now, p_invoice_id;
    end if;

  else
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

    -- Stationery key
    v_stationery_key := nullif(btrim(coalesce(v_base_hdr->>'stationery_key','')), '');
    if v_stationery_key is null then
      v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
    end if;

    if right(lower(v_stationery_key), 4) = '.pdf' then
      v_stationery_key := left(v_stationery_key, length(v_stationery_key) - 4) || '@300dpi.png';
    end if;

    while left(v_stationery_key, 1) = '/' loop
      v_stationery_key := substr(v_stationery_key, 2);
    end loop;

    -- Margins
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

    -- hide_bank_footer default TRUE
    if jsonb_typeof(v_base_hdr->'hide_bank_footer') = 'boolean' then
      v_hide_bank_footer := (v_base_hdr->>'hide_bank_footer')::boolean;
    else
      v_hide_bank_footer := true;
    end if;

    -- Bank + VAT registration
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
        select sd.bank_name, sd.bank_sort_code, sd.bank_account_number, sd.vat_registration_number
        into v_def
        from public.settings_defaults sd
        where sd.id = 1
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

    -- Client info
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
        select c.name, c.invoice_address, c.primary_invoice_email, c.vat_chargeable, c.payment_terms_days
        into v_cli
        from public.clients c
        where c.id = v_inv.client_id
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

    -- VAT % (prefer original snapshot applied_vat_rate_pct; else compute anchored)
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

    -- Create the credit note invoice row
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
        'issued_at_utc', public._inv_iso_utc(v_now),
        'due_at_utc', public._inv_iso_utc(v_due_at),
        'stationery_key', v_stationery_key,
        'stationery_margins_mm', v_margins,
        'hide_bank_footer', v_hide_bank_footer,
        'bank', v_bank,
        'vat_registration_number', v_vat_reg,
        'meta', jsonb_build_object(
          'source', 'CREDIT_NOTE',
          'original_invoice_id', v_inv.id::text,
          'vat_anchor_ymd', v_anchor_ymd::text,
          'original_invoice_issued_at_utc', public._inv_iso_utc(v_original_issued_at)
        )
      )
    )
    returning id into v_credit_id;

    -- Ensure A4 marker is set on original invoice (idempotent)
    if v_has_credit_note_created_at then
      execute
        'update public.invoices set credit_note_created_at_utc = coalesce(credit_note_created_at_utc, $1), updated_at = $1 where id = $2'
      using v_now, p_invoice_id;
    end if;
  end if;

  -- Insert negative mirror lines IF the credit note currently has no lines (idempotent safety)
  if not exists (
    select 1
    from public.invoice_lines ln
    where ln.invoice_id = v_credit_id
    limit 1
  ) then
    insert into public.invoice_lines(
      invoice_id, timesheet_id, booking_id, description,
      hours_day, hours_night, hours_sat, hours_sun, hours_bh,
      pay_day, pay_night, pay_sat, pay_sun, pay_bh,
      charge_day, charge_night, charge_sat, charge_sun, charge_bh,
      total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
      vat_rate_pct, vat_amount, total_inc_vat,
      paper_ts_r2_key, meta_json, source_key
    )
    select
      v_credit_id,
      l.timesheet_id,
      l.booking_id,
      ('CREDIT NOTE – ' || coalesce(l.description,'')),

      l.hours_day, l.hours_night, l.hours_sat, l.hours_sun, l.hours_bh,

      l.pay_day, l.pay_night, l.pay_sat, l.pay_sun, l.pay_bh,
      l.charge_day, l.charge_night, l.charge_sat, l.charge_sun, l.charge_bh,

      public._inv_round2(-1 * coalesce(l.total_pay_ex_vat,0)),
      public._inv_round2(-1 * coalesce(l.total_charge_ex_vat,0)),
      public._inv_round2(-1 * coalesce(l.margin_ex_vat,0)),

      l.vat_rate_pct,
      public._inv_round2(-1 * coalesce(l.vat_amount,0)),
      public._inv_round2(-1 * coalesce(l.total_inc_vat,0)),

      l.paper_ts_r2_key,

      (coalesce(l.meta_json,'{}'::jsonb) ||
        jsonb_build_object(
          'credit_note', true,
          'original_invoice_id', v_inv.id::text,
          'original_invoice_line_id', l.id::text
        )
      ),

      ('CN:' || v_credit_id::text || ':LINE:' || l.id::text)
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
    on conflict (invoice_id, source_key) do nothing;

    get diagnostics v_inserted_lines = row_count;
  end if;

  -- Canonical totals recompute (also clears invoice_pdf_r2_key and enforces signed CREDIT_NOTE totals)
  perform public.invoice_recompute_totals(v_credit_id);

  -- Load totals for audit (from invoice row after recompute)
  select
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into v_cn_ex, v_cn_vat, v_cn_inc
  from public.invoices i
  where i.id = v_credit_id
  limit 1;

  -- Audit credit note creation / completion (best-effort)
  begin
    perform public._audit_insert(
      'invoice',
      v_credit_id::text,
      'CREDIT_NOTE_CREATED',
      null,
      jsonb_build_object(
        'credit_note_id', v_credit_id::text,
        'original_invoice_id', v_inv.id::text,
        'subtotal_ex_vat', public._inv_round2(v_cn_ex),
        'vat_amount', public._inv_round2(v_cn_vat),
        'total_inc_vat', public._inv_round2(v_cn_inc),
        'mirror_lines_inserted', coalesce(v_inserted_lines,0)
      ),
      null,
      p_actor_user_id
    );
  exception when others then
    null;
  end;

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

    -- Enqueue recompute (batch, idempotent)
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

    begin
      perform public._audit_insert(
        'invoice',
        v_credit_id::text,
        'CREDIT_NOTE_UNLOCKED_SNAPSHOTS',
        null,
        jsonb_build_object(
          'credit_note_id', v_credit_id::text,
          'original_invoice_id', v_inv.id::text,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'unlocked_count', unlocked_snapshots
        ),
        null,
        p_actor_user_id
      );
    exception when others then
      null;
    end;
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
