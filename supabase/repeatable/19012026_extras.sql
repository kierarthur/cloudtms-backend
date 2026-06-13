DO $$
DECLARE
  v_modern_oid oid;
  v_legacy_oid oid;
BEGIN
  /*
    Retire only the legacy seven-argument overload:

      public.contract_week_manual_upsert_atomic(
        uuid,
        uuid,
        jsonb,
        jsonb,
        jsonb,
        jsonb,
        timestamp with time zone
      )

    This block is safe to rerun:
      - If the legacy overload is already gone, it only raises a NOTICE.
      - If the legacy overload exists but the modern overload is missing, it aborts.
      - It does not use CASCADE, so dependent objects cannot be silently dropped.
  */

  SELECT pg_proc_entry.oid
    INTO v_modern_oid
  FROM pg_proc AS pg_proc_entry
  JOIN pg_namespace AS pg_namespace_entry
    ON pg_namespace_entry.oid = pg_proc_entry.pronamespace
  WHERE pg_namespace_entry.nspname = 'public'
    AND pg_proc_entry.proname = 'contract_week_manual_upsert_atomic'
    AND pg_get_function_identity_arguments(pg_proc_entry.oid) =
      'p_week_id uuid, p_expected_timesheet_id uuid, p_timesheet_create_json jsonb, p_timesheet_patch_json jsonb, p_contract_week_patch_json jsonb, p_tsfin_snapshot_json jsonb, p_rotation_json jsonb, p_actor_user_id uuid, p_materialise_staged_evidence boolean, p_now_utc timestamp with time zone, p_expected_row_signature text'
  LIMIT 1;

  SELECT pg_proc_entry.oid
    INTO v_legacy_oid
  FROM pg_proc AS pg_proc_entry
  JOIN pg_namespace AS pg_namespace_entry
    ON pg_namespace_entry.oid = pg_proc_entry.pronamespace
  WHERE pg_namespace_entry.nspname = 'public'
    AND pg_proc_entry.proname = 'contract_week_manual_upsert_atomic'
    AND pg_get_function_identity_arguments(pg_proc_entry.oid) =
      'p_week_id uuid, p_expected_timesheet_id uuid, p_timesheet_create_json jsonb, p_timesheet_patch_json jsonb, p_contract_week_patch_json jsonb, p_tsfin_snapshot_json jsonb, p_now_utc timestamp with time zone'
  LIMIT 1;

  IF v_legacy_oid IS NULL THEN
    RAISE NOTICE 'Legacy seven-argument contract_week_manual_upsert_atomic overload is not present; nothing to drop.';
    RETURN;
  END IF;

  IF v_modern_oid IS NULL THEN
    RAISE EXCEPTION
      'Refusing to drop legacy seven-argument contract_week_manual_upsert_atomic overload because the modern eleven-argument overload was not found.';
  END IF;

  DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(
    uuid,
    uuid,
    jsonb,
    jsonb,
    jsonb,
    jsonb,
    timestamp with time zone
  );

  RAISE NOTICE 'Dropped legacy seven-argument contract_week_manual_upsert_atomic overload.';
END;
$$;




CREATE OR REPLACE FUNCTION public.enqueue_ts_financials(_timesheet_id uuid, _reason ts_fin_reason_enum)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  VALUES (_timesheet_id, _reason, 0, NULL, NULL, now())
  ON CONFLICT (timesheet_id, reason)
  DO UPDATE SET
      attempt_count   = 0,
      next_attempt_at = NULL,
      last_error      = NULL,
      created_at      = now();
END;
$function$;
CREATE OR REPLACE FUNCTION public.tsfin_prepare_write(p_timesheet_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  -- Guard: refuse writes if the current snapshot is paid OR whole-timesheet locked by an invoice.
  -- IMPORTANT: Do NOT block just because some SEGMENTS are invoice-locked; partial recompute is allowed.
  IF EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
      AND (
        tf.paid_at_utc IS NOT NULL
        OR tf.locked_by_invoice_id IS NOT NULL
      )
  ) THEN
    RAISE EXCEPTION 'TSFIN_LOCKED';
  END IF;

  -- Make any current snapshots non-current (we keep history)
  UPDATE public.timesheets_financials tfu
  SET is_current = false
  WHERE tfu.timesheet_id = p_timesheet_id
    AND tfu.is_current = true;
END;
$function$;


CREATE OR REPLACE FUNCTION public.tsfin_work_success(p_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  DELETE FROM public.ts_financials_outbox t
  WHERE t.id = p_id;
END;
$function$;


CREATE OR REPLACE FUNCTION public.tsfin_work_fail(p_id uuid, p_error text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  v_attempt int;
  v_delay_minutes int;
BEGIN
  UPDATE public.ts_financials_outbox t
  SET attempt_count = t.attempt_count + 1,
      last_error    = p_error
  WHERE t.id = p_id
  RETURNING t.attempt_count INTO v_attempt;

  v_delay_minutes := LEAST(60, GREATEST(1, (2 ^ GREATEST(0, v_attempt - 1))));
  UPDATE public.ts_financials_outbox t2
  SET next_attempt_at = now() + (v_delay_minutes || ' minutes')::interval
  WHERE t2.id = p_id;
END;
$function$;


CREATE OR REPLACE FUNCTION public.tsfin_mark_revoked(p_timesheet_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  -- Guard: do not revoke the current snapshot if it is invoice-locked.
  -- (Same reasoning as tsfin_prepare_write: SEGMENTS can be partially/fully invoiced while summary lock is NULL.)
  IF EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
      AND (
        tf.locked_by_invoice_id IS NOT NULL
        OR (
          upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(
              CASE
                WHEN tf.invoice_breakdown_json IS NOT NULL
                  AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
                  AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
                THEN tf.invoice_breakdown_json->'segments'
                ELSE '[]'::jsonb
              END
            ) AS seg(value)
            WHERE nullif(btrim(coalesce(seg.value->>'invoice_locked_invoice_id','')), '') IS NOT NULL
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'TSFIN_LOCKED';
  END IF;

  UPDATE public.timesheets_financials tfu
  SET is_current = false
  WHERE tfu.timesheet_id = p_timesheet_id
    AND tfu.is_current = true;
END;
$function$;

-- ============================================================
-- CloudTMS Patch: related_list_v2 (SEGMENTS-safe, single-call)
-- ============================================================
-- Purpose:
--   Provide a single SQL/RPC call that returns the related list payload
--   (items + total) for the Related UI, in a way that is safe for
--   SEGMENTS invoicing (multi-invoice timesheets and multi-timesheet invoices).
--
-- Key fix vs legacy backend logic:
--   Use invoice_lines as the authoritative invoice↔timesheet relationship
--   rather than timesheets_financials.locked_by_invoice_id, which may be NULL
--   in multi-invoice SEGMENTS scenarios.
--
-- Return shape:
--   { "items": [...], "total": <int> }
--
-- Notes:
--   - For invoice rows returned, we use the same column set the backend uses
--     for "full invoice row" in related lists (id, invoice_no, etc.).
--   - For timesheet rows returned, we use v_timesheets_summary full shape.
--   - For candidate/invoice relationships, we derive the candidate's timesheet_ids
--     from current TSFIN and/or contracts-linked timesheets, then map via invoice_lines.
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================
create or replace function public.related_list_v2(
  p_entity text,
  p_id uuid,
  p_type text,
  p_limit int default 20,
  p_offset int default 0
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_entity text := lower(coalesce(p_entity,''));
  v_type text := lower(coalesce(p_type,''));
  v_limit int := greatest(1, least(coalesce(p_limit, 20), 100));
  v_offset int := greatest(0, coalesce(p_offset, 0));
  v_total int := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if p_id is null then
    raise exception 'related_list_v2: p_id is required';
  end if;

  -- ------------------------------------------------------------
  -- TIMESHEET -> INVOICES (segment-safe + ref/issue summary)
  -- ------------------------------------------------------------
  if v_entity = 'timesheet' and (v_type = 'invoice' or v_type = 'invoices') then
    with inv_ids as (
      select distinct il.invoice_id
      from public.invoice_lines il
      where il.timesheet_id = p_id
        and il.invoice_id is not null
    ),
    inv_total as (
      select count(*)::int as total
      from inv_ids
    ),
    page as (
      select i.*
      from public.invoices i
      join inv_ids x on x.invoice_id = i.id
      order by i.issued_at_utc desc nulls last, i.id desc
      limit v_limit offset v_offset
    ),
    -- all timesheets on each invoice
    inv_ts_all as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
    ),
    -- worked timesheets on each invoice (hours/additional only)
    inv_ts_worked as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
        and upper(coalesce(il.meta_json->>'line_type','')) in (
          'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
        )
    ),
    inv_ref_stats as (
      select
        x.invoice_id,

        -- issue-time policy enabled for any worked TS (contract override aware)
        coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false) as refs_required_to_issue,

        -- issue-time missing refs (contract override aware; uses LOCKED segment logic for SEGMENTS mode)
        coalesce(bool_or(coalesce(pc.issue_missing_reference,false)), false) as issue_blocked_missing_refs,
        coalesce(sum(case when coalesce(pc.issue_missing_reference,false) then coalesce(pc.issue_missing_reference_count,0) else 0 end),0)::int
          as issue_missing_reference_count,
        coalesce(
          jsonb_agg(distinct pc.timesheet_id::text) filter (where coalesce(pc.issue_missing_reference,false)),
          '[]'::jsonb
        ) as issue_missing_timesheet_ids,

        -- invoicing-time missing refs (precheck blocker; uses UNLOCKED segment logic for SEGMENTS mode)
        coalesce(bool_or(upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'), false) as invoicing_blocked_missing_refs,
        coalesce(
          jsonb_agg(distinct pc_all.timesheet_id::text) filter (where upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'),
          '[]'::jsonb
        ) as invoicing_missing_timesheet_ids

      from (select distinct invoice_id from inv_ids) x
      left join inv_ts_worked w
        on w.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = w.timesheet_id

      left join inv_ts_all a
        on a.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc_all
        on pc_all.timesheet_id = a.timesheet_id

      group by x.invoice_id
    ),
    payload as (
      select
        (select total from inv_total) as total,
        coalesce(jsonb_agg(
          jsonb_build_object(
            'id', p.id,
            'invoice_no', p.invoice_no,
            'client_id', p.client_id,
            'issued_at_utc', p.issued_at_utc,
            'due_at_utc', p.due_at_utc,
            'status', p.status,
            'subtotal_ex_vat', p.subtotal_ex_vat,
            'vat_amount', p.vat_amount,
            'total_inc_vat', p.total_inc_vat,
            'invoice_pdf_r2_key', p.invoice_pdf_r2_key,
            'header_snapshot_json', p.header_snapshot_json,
            'on_hold_reason', p.on_hold_reason,
            'paid_at_utc', p.paid_at_utc,

            -- ✅ new summary flags for UI consistency (segment-safe)
            'refs_required_to_issue', coalesce(s.refs_required_to_issue,false),
            'issue_blocked_missing_refs', coalesce(s.issue_blocked_missing_refs,false),
            'issue_missing_reference_count', coalesce(s.issue_missing_reference_count,0),
            'issue_missing_timesheet_ids', coalesce(s.issue_missing_timesheet_ids,'[]'::jsonb),
            'invoicing_blocked_missing_refs', coalesce(s.invoicing_blocked_missing_refs,false),
            'invoicing_missing_timesheet_ids', coalesce(s.invoicing_missing_timesheet_ids,'[]'::jsonb)
          )
        ), '[]'::jsonb) as items
      from page p
      left join inv_ref_stats s
        on s.invoice_id = p.id
    )
    select payload.total, payload.items
    into v_total, v_items
    from payload;

    return jsonb_build_object('items', v_items, 'total', coalesce(v_total, 0));
  end if;

  -- ------------------------------------------------------------
  -- INVOICE -> TIMESHEETS (segment-safe)
  -- ------------------------------------------------------------
  if v_entity = 'invoice' and v_type = 'timesheets' then
    with ts_ids as (
      select distinct il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id = p_id
        and il.timesheet_id is not null
    ),
    ts_total as (
      select count(*)::int as total
      from ts_ids
    ),
    page as (
      select s.*
      from public.v_timesheets_summary s
      join ts_ids x on x.timesheet_id = s.timesheet_id
      order by s.week_ending_date desc nulls last, s.client_name asc nulls last, s.candidate_name asc nulls last, s.timesheet_id::text
      limit v_limit offset v_offset
    )
    select
      (select total from ts_total) as total,
      coalesce(jsonb_agg(to_jsonb(page.*)), '[]'::jsonb) as items
    into v_total, v_items
    from page;

    return jsonb_build_object('items', v_items, 'total', coalesce(v_total, 0));
  end if;

  -- ------------------------------------------------------------
  -- CANDIDATE -> INVOICES (segment-safe + ref/issue summary)
  -- ------------------------------------------------------------
  if v_entity = 'candidate' and v_type = 'invoices' then
    with ts_ids as (
      -- Current TSFIN timesheets for candidate
      select distinct tf.timesheet_id
      from public.timesheets_financials tf
      where tf.is_current = true
        and tf.candidate_id = p_id
        and tf.timesheet_id is not null

      union

      -- Any timesheets linked via contracts for candidate (covers stale/non-current TSFIN)
      select distinct ts.timesheet_id
      from public.timesheets ts
      join public.contracts c on c.id = ts.contract_id
      where c.candidate_id = p_id
        and ts.timesheet_id is not null
    ),
    inv_ids as (
      select distinct il.invoice_id
      from public.invoice_lines il
      where il.timesheet_id in (select timesheet_id from ts_ids)
        and il.invoice_id is not null
    ),
    inv_total as (
      select count(*)::int as total
      from inv_ids
    ),
    page as (
      select i.*
      from public.invoices i
      join inv_ids x on x.invoice_id = i.id
      order by i.issued_at_utc desc nulls last, i.id desc
      limit v_limit offset v_offset
    ),
    inv_ts_all as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
    ),
    inv_ts_worked as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
        and upper(coalesce(il.meta_json->>'line_type','')) in (
          'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
        )
    ),
    inv_ref_stats as (
      select
        x.invoice_id,

        coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false) as refs_required_to_issue,
        coalesce(bool_or(coalesce(pc.issue_missing_reference,false)), false) as issue_blocked_missing_refs,
        coalesce(sum(case when coalesce(pc.issue_missing_reference,false) then coalesce(pc.issue_missing_reference_count,0) else 0 end),0)::int
          as issue_missing_reference_count,
        coalesce(
          jsonb_agg(distinct pc.timesheet_id::text) filter (where coalesce(pc.issue_missing_reference,false)),
          '[]'::jsonb
        ) as issue_missing_timesheet_ids,

        coalesce(bool_or(upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'), false) as invoicing_blocked_missing_refs,
        coalesce(
          jsonb_agg(distinct pc_all.timesheet_id::text) filter (where upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'),
          '[]'::jsonb
        ) as invoicing_missing_timesheet_ids

      from (select distinct invoice_id from inv_ids) x
      left join inv_ts_worked w
        on w.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = w.timesheet_id

      left join inv_ts_all a
        on a.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc_all
        on pc_all.timesheet_id = a.timesheet_id

      group by x.invoice_id
    )
    select
      (select total from inv_total) as total,
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', p.id,
          'invoice_no', p.invoice_no,
          'client_id', p.client_id,
          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'status', p.status,
          'subtotal_ex_vat', p.subtotal_ex_vat,
          'vat_amount', p.vat_amount,
          'total_inc_vat', p.total_inc_vat,
          'invoice_pdf_r2_key', p.invoice_pdf_r2_key,
          'header_snapshot_json', p.header_snapshot_json,
          'on_hold_reason', p.on_hold_reason,
          'paid_at_utc', p.paid_at_utc,

          'refs_required_to_issue', coalesce(s.refs_required_to_issue,false),
          'issue_blocked_missing_refs', coalesce(s.issue_blocked_missing_refs,false),
          'issue_missing_reference_count', coalesce(s.issue_missing_reference_count,0),
          'issue_missing_timesheet_ids', coalesce(s.issue_missing_timesheet_ids,'[]'::jsonb),
          'invoicing_blocked_missing_refs', coalesce(s.invoicing_blocked_missing_refs,false),
          'invoicing_missing_timesheet_ids', coalesce(s.invoicing_missing_timesheet_ids,'[]'::jsonb)
        )
      ), '[]'::jsonb) as items
    into v_total, v_items
    from page p
    left join inv_ref_stats s
      on s.invoice_id = p.id;

    return jsonb_build_object('items', v_items, 'total', coalesce(v_total, 0));
  end if;

  -- ------------------------------------------------------------
  -- UMBRELLA -> INVOICES (segment-safe + ref/issue summary)
  -- ------------------------------------------------------------
  if v_entity = 'umbrella' and v_type = 'invoices' then
    with cand_ids as (
      select distinct c.id as candidate_id
      from public.candidates c
      where c.umbrella_id = p_id
        and upper(coalesce(c.pay_method::text, '')) = 'UMBRELLA'
    ),
    ts_ids as (
      select distinct tf.timesheet_id
      from public.timesheets_financials tf
      where tf.is_current = true
        and tf.candidate_id in (select candidate_id from cand_ids)
        and tf.timesheet_id is not null
    ),
    inv_ids as (
      select distinct il.invoice_id
      from public.invoice_lines il
      where il.timesheet_id in (select timesheet_id from ts_ids)
        and il.invoice_id is not null
    ),
    inv_total as (
      select count(*)::int as total
      from inv_ids
    ),
    page as (
      select i.*
      from public.invoices i
      join inv_ids x on x.invoice_id = i.id
      order by i.issued_at_utc desc nulls last, i.id desc
      limit v_limit offset v_offset
    ),
    inv_ts_all as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
    ),
    inv_ts_worked as (
      select distinct il.invoice_id, il.timesheet_id
      from public.invoice_lines il
      where il.invoice_id in (select invoice_id from inv_ids)
        and il.timesheet_id is not null
        and upper(coalesce(il.meta_json->>'line_type','')) in (
          'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
        )
    ),
    inv_ref_stats as (
      select
        x.invoice_id,

        coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false) as refs_required_to_issue,
        coalesce(bool_or(coalesce(pc.issue_missing_reference,false)), false) as issue_blocked_missing_refs,
        coalesce(sum(case when coalesce(pc.issue_missing_reference,false) then coalesce(pc.issue_missing_reference_count,0) else 0 end),0)::int
          as issue_missing_reference_count,
        coalesce(
          jsonb_agg(distinct pc.timesheet_id::text) filter (where coalesce(pc.issue_missing_reference,false)),
          '[]'::jsonb
        ) as issue_missing_timesheet_ids,

        coalesce(bool_or(upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'), false) as invoicing_blocked_missing_refs,
        coalesce(
          jsonb_agg(distinct pc_all.timesheet_id::text) filter (where upper(coalesce(pc_all.precheck_status,'')) = 'BLOCK_NO_REFERENCE'),
          '[]'::jsonb
        ) as invoicing_missing_timesheet_ids

      from (select distinct invoice_id from inv_ids) x
      left join inv_ts_worked w
        on w.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = w.timesheet_id

      left join inv_ts_all a
        on a.invoice_id = x.invoice_id
      left join public.v_ts_invoice_precheck pc_all
        on pc_all.timesheet_id = a.timesheet_id

      group by x.invoice_id
    )
    select
      (select total from inv_total) as total,
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', p.id,
          'invoice_no', p.invoice_no,
          'client_id', p.client_id,
          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'status', p.status,
          'subtotal_ex_vat', p.subtotal_ex_vat,
          'vat_amount', p.vat_amount,
          'total_inc_vat', p.total_inc_vat,
          'invoice_pdf_r2_key', p.invoice_pdf_r2_key,
          'header_snapshot_json', p.header_snapshot_json,
          'on_hold_reason', p.on_hold_reason,
          'paid_at_utc', p.paid_at_utc,

          'refs_required_to_issue', coalesce(s.refs_required_to_issue,false),
          'issue_blocked_missing_refs', coalesce(s.issue_blocked_missing_refs,false),
          'issue_missing_reference_count', coalesce(s.issue_missing_reference_count,0),
          'issue_missing_timesheet_ids', coalesce(s.issue_missing_timesheet_ids,'[]'::jsonb),
          'invoicing_blocked_missing_refs', coalesce(s.invoicing_blocked_missing_refs,false),
          'invoicing_missing_timesheet_ids', coalesce(s.invoicing_missing_timesheet_ids,'[]'::jsonb)
        )
      ), '[]'::jsonb) as items
    into v_total, v_items
    from page p
    left join inv_ref_stats s
      on s.invoice_id = p.id;

    return jsonb_build_object('items', v_items, 'total', coalesce(v_total, 0));
  end if;

  raise exception 'related_list_v2: unsupported (entity=%, type=%)', p_entity, p_type;
end;
$$;

-- ============================================================
-- CloudTMS Patch: related_counts_v2 (SEGMENTS-safe, single-call)
-- ============================================================
-- Purpose:
--   Provide a single SQL/RPC call that returns the related counts payload
--   for the Related UI, in a way that is safe for SEGMENTS invoicing.
--
-- Key fix vs legacy backend logic:
--   Use invoice_lines as the authoritative invoice↔timesheet relationship
--   rather than timesheets_financials.locked_by_invoice_id, which may be NULL
--   in multi-invoice SEGMENTS scenarios.
--
-- Return shape:
--   A jsonb object whose keys match the existing related-counts UI:
--     candidate:  { timesheets, invoices, clients, contracts, umbrella }
--     timesheet:  { candidate, client, invoice, umbrella, contract, series }
--     invoice:    { timesheets, candidates, client, umbrellas }
--     client:     { timesheets, invoices, candidates, contracts }
--     contract:   { candidate, client, timesheets, umbrella }
--     umbrella:   { candidates, timesheets, invoices }
--     remittance: { timesheets, candidate }
--
-- Notes:
--   - Where legacy code returned 0/1 for invoice on a timesheet, we now return
--     the true count of related invoices (segment-split support).
--   - "Timesheets" counts for candidate/client/umbrella preserve the legacy intent:
--     current TSFIN rows + planned contract_weeks not already represented by TSFIN.
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================

create or replace function public.related_counts_v2(
  p_entity text,
  p_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_entity text := lower(coalesce(p_entity,''));
begin
  if p_id is null then
    raise exception 'related_counts_v2: p_id is required';
  end if;

  -- ------------------------------------------------------------
  -- CANDIDATE
  -- ------------------------------------------------------------
   if v_entity = 'candidate' then
    return (
      with tsfin_rows as (
        select tf.timesheet_id, tf.client_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.candidate_id = p_id
      ),
      contract_rows as (
        select c.id as contract_id, c.client_id
        from public.contracts c
        where c.candidate_id = p_id
      ),
          -- ✅ Timesheets count MUST match the related list source (v_timesheets_summary),
      -- otherwise contract_weeks can point at revoked/non-current timesheet_ids and overcount.
      ts_view as (
        select v.timesheet_id, v.client_id
        from public.v_timesheets_summary v
        where v.candidate_id = p_id
      ),

      timesheets_total as (
        select count(*)::int as total
        from public.v_timesheets_summary v2
        where v2.candidate_id = p_id
      ),

      clients_distinct as (
        select distinct tr.client_id
        from tsfin_rows tr
        where tr.client_id is not null
        union
        select distinct cr.client_id
        from contract_rows cr
        where cr.client_id is not null
      ),

      contracts_total as (
        select count(*)::int as total
        from contract_rows
      ),

      umbrella_count as (
        select
          case
            when upper(coalesce(c.pay_method::text, '')) = 'UMBRELLA' and c.umbrella_id is not null then 1
            else 0
          end::int as total
        from public.candidates c
        where c.id = p_id
        limit 1
      ),

      invoice_timesheet_ids as (
        select distinct tv.timesheet_id
        from ts_view tv
        where tv.timesheet_id is not null
      ),

      invoice_ids as (
        select distinct il.invoice_id
        from public.invoice_lines il
        where il.timesheet_id in (select iti.timesheet_id from invoice_timesheet_ids iti)
          and il.invoice_id is not null
      )

      select jsonb_build_object(
        'timesheets', coalesce((select total from timesheets_total), 0),
        'invoices',   coalesce((select count(*)::int from invoice_ids), 0),
        'clients',    coalesce((select count(*)::int from clients_distinct), 0),
        'contracts',  coalesce((select total from contracts_total), 0),
        'umbrella',   coalesce((select total from umbrella_count), 0)
      )
    );
  end if;


  -- ------------------------------------------------------------
  -- TIMESHEET (segment-safe invoice count)
  -- ------------------------------------------------------------
  if v_entity = 'timesheet' then
    return (
      with cur_tf as (
        select tf.candidate_id, tf.client_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id = p_id
        limit 1
      ),
      inv_ids as (
        select distinct il.invoice_id
        from public.invoice_lines il
        where il.timesheet_id = p_id
          and il.invoice_id is not null
      ),
      inv_count as (
        select count(*)::int as total
        from inv_ids
      ),
      contract_from_ts as (
        select ts.contract_id
        from public.timesheets ts
        where ts.timesheet_id = p_id
        limit 1
      ),
      contract_from_cw as (
        select cw.contract_id
        from public.contract_weeks cw
        where cw.id = p_id
        limit 1
      ),
      contract_id as (
        select contract_id from contract_from_ts
        union
        select contract_id from contract_from_cw
        limit 1
      ),
      cand_from_contract as (
        select c.candidate_id, c.client_id
        from public.contracts c
        where c.id in (select contract_id from contract_id)
        limit 1
      ),
      resolved_candidate as (
        select
          coalesce((select candidate_id from cur_tf),
                   (select candidate_id from cand_from_contract)) as candidate_id
      ),
      resolved_client as (
        select
          coalesce((select client_id from cur_tf),
                   (select client_id from cand_from_contract)) as client_id
      ),
      umbrella_count as (
        select
          case
            when upper(coalesce(c.pay_method::text, '')) = 'UMBRELLA' and c.umbrella_id is not null then 1
            else 0
          end::int as total
        from public.candidates c
        where c.id = (select candidate_id from resolved_candidate)
        limit 1
      ),
      series_count as (
        -- "series" = number of OTHER contract_week timesheets in same contract/week (adjustments)
        select
          case
            when cw.contract_id is null or cw.week_ending_date is null then 0
            else greatest(0,
              (select count(*)::int
               from public.contract_weeks cw2
               where cw2.contract_id = cw.contract_id
                 and cw2.week_ending_date = cw.week_ending_date
                 and cw2.timesheet_id is not null
              ) - 1
            )
          end::int as total
        from public.contract_weeks cw
        where cw.timesheet_id = p_id
        limit 1
      )
      select jsonb_build_object(
        'candidate', case when (select candidate_id from resolved_candidate) is null then 0 else 1 end,
        'client',    case when (select client_id from resolved_client) is null then 0 else 1 end,
        'invoice',   coalesce((select total from inv_count), 0),
        'umbrella',  coalesce((select total from umbrella_count), 0),
        'contract',  case when (select contract_id from contract_id) is null then 0 else 1 end,
        'series',    coalesce((select total from series_count), 0)
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- INVOICE (segment-safe timesheets + candidates)
  -- ------------------------------------------------------------
  if v_entity = 'invoice' then
    return (
      with ts_ids as (
        select distinct il.timesheet_id
        from public.invoice_lines il
        where il.invoice_id = p_id
          and il.timesheet_id is not null
      ),
      ts_count as (
        select count(*)::int as total
        from ts_ids
      ),
      cand_ids as (
        select distinct tf.candidate_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id in (select timesheet_id from ts_ids)
          and tf.candidate_id is not null
      ),
      cand_count as (
        select count(*)::int as total
        from cand_ids
      ),
      umb_ids as (
        select distinct c.umbrella_id
        from public.candidates c
        where c.id in (select candidate_id from cand_ids)
          and upper(coalesce(c.pay_method::text,'')) = 'UMBRELLA'
          and c.umbrella_id is not null
      ),
      umb_count as (
        select count(*)::int as total
        from umb_ids
      )
      select jsonb_build_object(
        'timesheets', coalesce((select total from ts_count), 0),
        'candidates', coalesce((select total from cand_count), 0),
        'client',     1,
        'umbrellas',  coalesce((select total from umb_count), 0)
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- CLIENT
  -- ------------------------------------------------------------
  if v_entity = 'client' then
    return (
      with tsfin_rows as (
        select tf.timesheet_id, tf.candidate_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.client_id = p_id
      ),
      contract_rows as (
        select c.id as contract_id, c.candidate_id
        from public.contracts c
        where c.client_id = p_id
      ),
           -- ✅ Timesheets count MUST match the related list source (v_timesheets_summary),
      -- otherwise contract_weeks can point at revoked/non-current timesheet_ids and overcount.
      timesheets_total as (
        select count(*)::int as total
        from public.v_timesheets_summary v
        where v.client_id = p_id
      ),

      cand_distinct as (
        select distinct tr.candidate_id
        from tsfin_rows tr
        where tr.candidate_id is not null
        union
        select distinct cr.candidate_id
        from contract_rows cr
        where cr.candidate_id is not null
      ),

      contracts_total as (
        select count(*)::int as total
        from contract_rows
      ),
      invoices_total as (
        select count(*)::int as total
        from public.invoices i
        where i.client_id = p_id
      )
      select jsonb_build_object(
        'timesheets', coalesce((select total from timesheets_total), 0),
        'invoices',   coalesce((select total from invoices_total), 0),
        'candidates', coalesce((select count(*)::int from cand_distinct), 0),
        'contracts',  coalesce((select total from contracts_total), 0)
      )
    );
  end if;


  -- ------------------------------------------------------------
  -- CONTRACT
  -- ------------------------------------------------------------
  if v_entity = 'contract' then
    return (
      with con as (
        select c.candidate_id, c.client_id
        from public.contracts c
        where c.id = p_id
        limit 1
      ),
      timesheets_total as (
        select count(*)::int as total
        from public.v_timesheets_summary s
        where s.contract_id = p_id
      ),
      umbrella_count as (
        select
          case
            when upper(coalesce(c.pay_method::text, '')) = 'UMBRELLA' and c.umbrella_id is not null then 1
            else 0
          end::int as total
        from public.candidates c
        where c.id = (select candidate_id from con)
        limit 1
      )
      select jsonb_build_object(
        'candidate', case when (select candidate_id from con) is null then 0 else 1 end,
        'client',    case when (select client_id from con) is null then 0 else 1 end,
        'timesheets', coalesce((select total from timesheets_total), 0),
        'umbrella',  coalesce((select total from umbrella_count), 0)
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- UMBRELLA (segment-safe invoice count)
  -- ------------------------------------------------------------
   if v_entity = 'umbrella' then
    return (
      with cand_ids as (
        select distinct c.id as candidate_id
        from public.candidates c
        where c.umbrella_id = p_id
      ),
      cand_count as (
        select count(*)::int as total
        from cand_ids
      ),
      contract_rows as (
        select c.id as contract_id
        from public.contracts c
        where c.candidate_id in (select ci.candidate_id from cand_ids ci)
      ),
      contract_ids as (
        select cr.contract_id
        from contract_rows cr
      ),
      contract_weeks as (
        select cw.id as contract_week_id, cw.timesheet_id, cw.contract_id
        from public.contract_weeks cw
        where cw.contract_id in (select cd.contract_id from contract_ids cd)
      ),

        -- ✅ Timesheets count MUST match the related list source (v_timesheets_summary),
      -- otherwise contract_weeks can point at revoked/non-current timesheet_ids and overcount.
      timesheets_total as (
        select count(*)::int as total
        from public.v_timesheets_summary v
        where v.candidate_id in (select ci.candidate_id from cand_ids ci)
      ),

      invoice_timesheet_ids as (
        select distinct v2.timesheet_id
        from public.v_timesheets_summary v2
        where v2.candidate_id in (select ci2.candidate_id from cand_ids ci2)
          and v2.timesheet_id is not null
      ),

      inv_ids as (
        select distinct il.invoice_id
        from public.invoice_lines il
        where il.timesheet_id in (select iti.timesheet_id from invoice_timesheet_ids iti)
          and il.invoice_id is not null
      ),

      invoices_total as (
        select count(*)::int as total
        from inv_ids
      )
      select jsonb_build_object(
        'candidates', coalesce((select total from cand_count), 0),
        'timesheets', coalesce((select total from timesheets_total), 0),
        'invoices',   coalesce((select total from invoices_total), 0)
      )
    );
  end if;


  -- ------------------------------------------------------------
  -- REMITTANCE
  -- ------------------------------------------------------------
  if v_entity = 'remittance' then
    return (
      with ts_ids as (
        select distinct ae.object_id_text
        from public.audit_events ae
        where ae.correlation_id = p_id::text
          and ae.reason = 'REMITTANCE'
          and ae.object_type = 'timesheet'
          and ae.object_id_text is not null
      ),
      ts_count as (
        select count(*)::int as total
        from ts_ids
      ),
      cand_exists as (
        select exists(
          select 1
          from public.audit_events ae
          where ae.correlation_id = p_id::text
            and ae.reason = 'REMITTANCE'
            and ae.object_type = 'candidate'
        ) as ok
      )
      select jsonb_build_object(
        'timesheets', coalesce((select total from ts_count), 0),
        'candidate',  case when (select ok from cand_exists) then 1 else 0 end
      )
    );
  end if;

  raise exception 'related_counts_v2: unsupported entity=%', p_entity;
end;
$$;

-- ============================================================
-- CloudTMS Patch (Option A)
-- Segment-aware "invoiced" filtering for Timesheets search/report
--
-- Behaviour:
-- - A timesheet is considered "invoiced" if:
--     (A) timesheets_financials.locked_by_invoice_id IS NOT NULL
--  OR (B) timesheets_financials.invoice_breakdown_json.mode = 'SEGMENTS'
--         AND ANY segment has a non-empty invoice_locked_invoice_id
--
-- - "Partial invoiced" (some segments invoiced) counts as invoiced.
--
-- This patch adds two RPCs used by the Worker:
--   1) public.tsfin_report_timesheets_v2(...)
--   2) public.tsfin_search_timesheets_v2(...)
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================

create or replace function public.tsfin_report_timesheets_v2(
  p_week_ending_from date default null,
  p_week_ending_to date default null,
  p_pay_method text default null,
  p_client_ids uuid[] default null,
  p_candidate_ids uuid[] default null,
  p_include_on_hold boolean default false,
  p_paid boolean default null,
  p_invoiced boolean default null
)
returns setof jsonb
language sql
stable
security definer
set search_path = public
as $$
with base as (
  select
    tf.timesheet_id,
    tf.candidate_id,
    tf.client_id,
    tf.pay_method,
    tf.total_pay_ex_vat,
    tf.total_charge_ex_vat,
    tf.margin_ex_vat,
    tf.expenses_charge_ex_vat,
    tf.mileage_charge_ex_vat,
    tf.paid_at_utc,
    tf.pay_on_hold,
    tf.locked_by_invoice_id,
    ts.week_ending_date,
    c.name as client_name,
    (
      tf.locked_by_invoice_id is not null
      or (
        upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and exists (
          select 1
          from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
          where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is not null
        )
      )
    ) as invoiced_any
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  left join public.clients c
    on c.id = tf.client_id
  where tf.is_current = true
    and (p_week_ending_from is null or ts.week_ending_date::date >= p_week_ending_from)
    and (p_week_ending_to is null or ts.week_ending_date::date <= p_week_ending_to)
    and (p_pay_method is null or upper(coalesce(tf.pay_method,'')) = upper(p_pay_method))
    and (p_include_on_hold or coalesce(tf.pay_on_hold,false) = false)
    and (
      p_client_ids is null
      or array_length(p_client_ids,1) is null
      or tf.client_id = any(p_client_ids)
    )
    and (
      p_candidate_ids is null
      or array_length(p_candidate_ids,1) is null
      or tf.candidate_id = any(p_candidate_ids)
    )
    and (
      p_paid is null
      or (p_paid = true and tf.paid_at_utc is not null)
      or (p_paid = false and tf.paid_at_utc is null)
    )
)
select jsonb_build_object(
  'timesheet_id', base.timesheet_id,
  'candidate_id', base.candidate_id,
  'client_id', base.client_id,
  'pay_method', base.pay_method,
  'locked_by_invoice_id', base.locked_by_invoice_id,
  'total_pay_ex_vat', base.total_pay_ex_vat,
  'total_charge_ex_vat', base.total_charge_ex_vat,
  'margin_ex_vat', base.margin_ex_vat,
  'expenses_charge_ex_vat', base.expenses_charge_ex_vat,
  'mileage_charge_ex_vat', base.mileage_charge_ex_vat,
  'paid_at_utc', base.paid_at_utc,
  'pay_on_hold', base.pay_on_hold,
  'invoiced_any', base.invoiced_any,
  'timesheet', jsonb_build_object(
    'week_ending_date', base.week_ending_date
  ),
  'client', jsonb_build_object(
    'name', base.client_name
  )
)
from base
where (p_invoiced is null or base.invoiced_any = p_invoiced)
order by
  base.week_ending_date desc nulls last,
  base.client_name asc nulls last,
  base.timesheet_id::text asc;
$$;


create or replace function public.tsfin_search_timesheets_v2(
  p_week_ending_from date default null,
  p_week_ending_to date default null,
  p_pay_method text default null,
  p_client_id uuid default null,
  p_candidate_id uuid default null,
  p_include_on_hold boolean default false,
  p_paid boolean default null,
  p_invoiced boolean default null,
  p_sheet_scope text default null,
  p_qr_status text default null,
  p_booking_id text default null,
  p_occupant_key_norm text default null,
  p_hospital_norm text default null,
  p_worked_from timestamptz default null,
  p_worked_to timestamptz default null,
  p_created_from timestamptz default null,
  p_created_to timestamptz default null,
  p_statuses text[] default null,
  p_timesheet_ids uuid[] default null,
  p_order_by text default 'week_ending_date',
  p_order_dir text default 'desc',
  p_limit int default 50,
  p_offset int default 0
)
returns setof jsonb
language sql
stable
security definer
set search_path = public
as $$
with base as (
  select
    tf.timesheet_id,
    tf.candidate_id,
    tf.client_id,
    tf.pay_method,
    tf.processing_status,
    tf.basis,
    tf.total_charge_ex_vat,
    tf.total_pay_ex_vat,
    tf.margin_ex_vat,
    tf.paid_at_utc,
    tf.locked_by_invoice_id,
    tf.pay_on_hold,
    tf.created_at,
    tf.worked_start_iso,
    tf.worked_end_iso,

    ts.week_ending_date,
    ts.status as ts_status,
    ts.booking_id,
    ts.occupant_key_norm,
    ts.hospital_norm,
    ts.sheet_scope,
    ts.submission_mode,
    ts.qr_status,

    c.name as client_name,

    (
      tf.locked_by_invoice_id is not null
      or (
        upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and exists (
          select 1
          from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
          where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is not null
        )
      )
    ) as invoiced_any

  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  left join public.clients c
    on c.id = tf.client_id
  where tf.is_current = true

    and (p_week_ending_from is null or ts.week_ending_date::date >= p_week_ending_from)
    and (p_week_ending_to is null or ts.week_ending_date::date <= p_week_ending_to)

    and (p_pay_method is null or upper(coalesce(tf.pay_method,'')) = upper(p_pay_method))

    and (p_client_id is null or tf.client_id = p_client_id)
    and (p_candidate_id is null or tf.candidate_id = p_candidate_id)

    and (p_include_on_hold or coalesce(tf.pay_on_hold,false) = false)

    and (
      p_paid is null
      or (p_paid = true and tf.paid_at_utc is not null)
      or (p_paid = false and tf.paid_at_utc is null)
    )

    and (p_sheet_scope is null or upper(coalesce(ts.sheet_scope::text,'')) = upper(p_sheet_scope))
    and (p_qr_status is null or upper(coalesce(ts.qr_status::text,'')) = upper(p_qr_status))

    and (p_booking_id is null or coalesce(ts.booking_id::text,'') = p_booking_id)
    and (p_occupant_key_norm is null or coalesce(ts.occupant_key_norm,'') = p_occupant_key_norm)
    and (p_hospital_norm is null or coalesce(ts.hospital_norm,'') = p_hospital_norm)

    and (p_worked_from is null or tf.worked_start_iso >= p_worked_from)
    and (p_worked_to is null or tf.worked_end_iso <= p_worked_to)

    and (p_created_from is null or tf.created_at >= p_created_from)
    and (p_created_to is null or tf.created_at <= p_created_to)

    and (
      p_statuses is null
      or array_length(p_statuses,1) is null
      or upper(coalesce(ts.status::text,'')) = any (array(select upper(x) from unnest(p_statuses) x))
    )

    and (
      p_timesheet_ids is null
      or array_length(p_timesheet_ids,1) is null
      or tf.timesheet_id = any(p_timesheet_ids)
    )
)
select jsonb_build_object(
  'timesheet_id', base.timesheet_id,
  'candidate_id', base.candidate_id,
  'client_id', base.client_id,
  'pay_method', base.pay_method,
  'processing_status', base.processing_status,
  'basis', base.basis,
  'total_charge_ex_vat', base.total_charge_ex_vat,
  'total_pay_ex_vat', base.total_pay_ex_vat,
  'margin_ex_vat', base.margin_ex_vat,
  'paid_at_utc', base.paid_at_utc,
  'locked_by_invoice_id', base.locked_by_invoice_id,
  'pay_on_hold', base.pay_on_hold,
  'created_at', base.created_at,
  'invoiced_any', base.invoiced_any,
  'timesheet', jsonb_build_object(
    'week_ending_date', base.week_ending_date,
    'status', base.ts_status,
    'booking_id', base.booking_id,
    'occupant_key_norm', base.occupant_key_norm,
    'hospital_norm', base.hospital_norm,
    'sheet_scope', base.sheet_scope,
    'submission_mode', base.submission_mode,
    'qr_status', base.qr_status
  ),
  'client', jsonb_build_object(
    'name', base.client_name
  )
)
from base
where (p_invoiced is null or base.invoiced_any = p_invoiced)
order by
  -- week_ending_date
  case when lower(coalesce(p_order_by,'')) in ('week_ending_date') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.week_ending_date end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('week_ending_date') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.week_ending_date end desc nulls last,

  -- margin
  case when lower(coalesce(p_order_by,'')) in ('margin','margin_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.margin_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('margin','margin_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.margin_ex_vat end desc nulls last,

  -- charge
  case when lower(coalesce(p_order_by,'')) in ('charge','total_charge_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.total_charge_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('charge','total_charge_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.total_charge_ex_vat end desc nulls last,

  -- pay
  case when lower(coalesce(p_order_by,'')) in ('pay','total_pay_ex_vat') and lower(coalesce(p_order_dir,'')) = 'asc'
    then base.total_pay_ex_vat end asc nulls last,
  case when lower(coalesce(p_order_by,'')) in ('pay','total_pay_ex_vat') and lower(coalesce(p_order_dir,'')) <> 'asc'
    then base.total_pay_ex_vat end desc nulls last,

  -- stable fallback
  base.week_ending_date desc nulls last,
  base.timesheet_id::text asc
limit greatest(1, least(coalesce(p_limit, 50), 200))
offset greatest(coalesce(p_offset, 0), 0);
$$;

-- ------------------------------------------------------------
-- CloudTMS: TSFIN self-heal helper (UPDATED)
-- Merge-repair SEGMENTS arrays for a timesheet where current TSFIN is
-- "locked + invalid" (e.g. contains JSON null elements), without changing
-- any invoice-locked segment objects.
--
-- IMPORTANT UPDATE:
--   - Prefer matching by nhsp_shift_id when present.
--   - Else prefer matching by external_row_key when present.
--   - Else fall back to stable signature:
--       (date/work_date) + (start_utc normalized to UTC) + (ref_num)
--   - Still FAILS CLOSED if keys are ambiguous or missing.
-- ------------------------------------------------------------



create or replace function public.tsfin_repair_merge_segments_locked(
  p_timesheet_id uuid,
  p_new_segments jsonb,
  p_actor_user_id uuid default null,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = public
as $$
declare
  v_tsfin_id uuid;
  v_ib jsonb;
  v_new_ib jsonb;

  v_old_invalid int := 0;
  v_new_invalid int := 0;

  -- Locked maps (preserve these objects exactly)
  v_locked_nhsp_map jsonb := '{}'::jsonb;  -- nhsp_shift_id -> locked seg json
  v_locked_ext_map  jsonb := '{}'::jsonb;  -- external_row_key -> locked seg json
  v_locked_sig_map  jsonb := '{}'::jsonb;  -- sig_key -> locked seg json

  -- Locked key -> locked segment_id (for error reporting)
  v_locked_nhsp_idmap jsonb := '{}'::jsonb;
  v_locked_ext_idmap  jsonb := '{}'::jsonb;
  v_locked_sig_idmap  jsonb := '{}'::jsonb;

  -- Fresh maps (built from p_new_segments)
  v_fresh_nhsp_map jsonb := '{}'::jsonb;
  v_fresh_ext_map  jsonb := '{}'::jsonb;
  v_fresh_sig_map  jsonb := '{}'::jsonb;

  -- NEW: preserve delay overrides from the current TSFIN JSON (even for unlocked segments)
  v_delay_by_segment_id jsonb := '{}'::jsonb; -- segment_id -> invoice_target_week_start (jsonb string)
  v_delay_text text := null;
  v_delays_reapplied int := 0;

  v_need_sig boolean := false;

  v_missing_locked_ids text[] := array[]::text[];

  v_preserved_locked_count int := 0;
  v_merged_len int := 0;

  e jsonb;

  sid text;
  lock_invoice_id text;

  v_date text;
  v_ref  text;
  v_start_ts timestamptz;
  v_start_norm text;
  v_sig_key text;

  v_nhsp_id text;
  v_ext_key text;

  chosen jsonb;

  v_merged_segments jsonb := '[]'::jsonb;

  v_rows_updated int := 0;

  -- scratch
  k text;
  v_locked_count int := 0;

  -- uuid regex test
  is_uuid boolean;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  -- corruption / diagnostics
  v_old_segments_len int := 0;
  v_old_non_object_count int := 0;
  v_old_missing_segment_id_count int := 0;
  v_old_locked_count int := 0;
  v_old_unlocked_count int := 0;
  v_old_unlocked_delayed_count int := 0;

  v_new_segments_len int := 0;

  v_old_invalid_samples jsonb := '[]'::jsonb;
  v_old_invalid_samples_cap int := 10;

  r record;

  -- helper payload for early returns
  v_ret jsonb;

  -- ✅ preserve additional/expenses and keep totals consistent
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;

  -- ✅ fallback if v_ib.additional is missing/invalid (derive non-seg totals from stored totals - original segment sums)
  v_tf_total_pay numeric := 0;
  v_tf_total_charge numeric := 0;
  v_old_seg_pay_sum numeric := 0;
  v_old_seg_charge_sum numeric := 0;
  v_add_ok boolean := false;
  v_add_pay_text text := null;
  v_add_charge_text text := null;

  v_seg_pay_sum numeric := 0;
  v_seg_charge_sum numeric := 0;
  v_total_pay numeric := 0;
  v_total_charge numeric := 0;

  -- ✅ ERNI-aware margin (PAYE only; wage pay only; never expenses/mileage)
  v_policy jsonb := '{}'::jsonb;
  v_pay_method_text text := '';
  v_additional_pay_ex_vat numeric := 0;
  v_expenses_pay_ex_vat numeric := 0;
  v_mileage_pay_ex_vat numeric := 0;

  v_apply_to text := 'PAYE_ONLY';
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text := '';
  v_erni_applies boolean := false;

  v_wage_pay numeric := 0;
  v_reimb_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_pay_cost numeric := 0;

  v_nonseg_wage_pay numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_pay_cost numeric := 0;
  v_nonseg_margin numeric := 0;

  v_margin numeric := 0;
  v_exclude boolean := false;
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
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'timesheet_id', coalesce(p_timesheet_id::text,''),
        'has_new_segments', (p_new_segments is not null),
        'correlation_id', p_correlation_id
      )
    );
  end if;

  if p_timesheet_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'TIMESHEET_ID_REQUIRED');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','TIMESHEET_ID_REQUIRED')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || coalesce(p_timesheet_id::text,'')),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if p_new_segments is null or jsonb_typeof(p_new_segments) <> 'array' then
    v_ret := jsonb_build_object('ok', false, 'error', 'NEW_SEGMENTS_MUST_BE_ARRAY');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MUST_BE_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Lock the current TSFIN row for this timesheet (also load stored totals + policy/pay inputs for ERNI-aware margin)
  select
      tf.id,
      tf.invoice_breakdown_json,
      coalesce(tf.total_pay_ex_vat, 0)::numeric as total_pay_ex_vat,
      coalesce(tf.total_charge_ex_vat, 0)::numeric as total_charge_ex_vat,
      coalesce(tf.policy_snapshot_json, '{}'::jsonb) as policy_snapshot_json,
      coalesce(tf.pay_method::text, '') as pay_method_text,
      coalesce(tf.additional_pay_ex_vat, 0)::numeric as additional_pay_ex_vat,
      coalesce(tf.expenses_pay_ex_vat, 0)::numeric as expenses_pay_ex_vat,
      coalesce(tf.mileage_pay_ex_vat, 0)::numeric as mileage_pay_ex_vat
    into
      v_tsfin_id,
      v_ib,
      v_tf_total_pay,
      v_tf_total_charge,
      v_policy,
      v_pay_method_text,
      v_additional_pay_ex_vat,
      v_expenses_pay_ex_vat,
      v_mileage_pay_ex_vat
  from public.timesheets_financials tf
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  for update;

  if not found or v_tsfin_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'CURRENT_TSFIN_NOT_FOUND');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_TSFIN_NOT_FOUND')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','tsfin_locked','at_utc', public._inv_iso_utc(now()), 'tsfin_id', v_tsfin_id::text)
    );
  end if;

  if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_INVOICE_BREAKDOWN_INVALID',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_INVOICE_BREAKDOWN_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if upper(coalesce(v_ib->>'mode','')) <> 'SEGMENTS' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_NOT_SEGMENTS_MODE',
      'tsfin_id', v_tsfin_id::text,
      'mode', upper(coalesce(v_ib->>'mode',''))
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_NOT_SEGMENTS_MODE')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if jsonb_typeof(v_ib->'segments') <> 'array' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_SEGMENTS_NOT_ARRAY',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_SEGMENTS_NOT_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Record old invalid count (for reporting)
  v_old_invalid := public._tsfin_invalid_segment_count(v_ib);

  -- Diagnostics: count what is "corrupted" in current segments (non-object/null/missing ids etc.)
  begin
    v_old_segments_len := jsonb_array_length(v_ib->'segments');
  exception when others then
    v_old_segments_len := 0;
  end;

  for r in
    select value as seg, ordinality as ord
    from jsonb_array_elements(v_ib->'segments') with ordinality
  loop
    if r.seg is null or jsonb_typeof(r.seg) <> 'object' then
      v_old_non_object_count := v_old_non_object_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'NON_OBJECT',
                'type', coalesce(jsonb_typeof(r.seg), 'null')
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;

      continue;
    end if;

    sid := nullif(btrim(coalesce(r.seg->>'segment_id','')), '');
    if sid is null then
      v_old_missing_segment_id_count := v_old_missing_segment_id_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'MISSING_SEGMENT_ID'
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(r.seg->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is not null then
      v_old_locked_count := v_old_locked_count + 1;
    else
      v_old_unlocked_count := v_old_unlocked_count + 1;

      if nullif(btrim(coalesce(r.seg->>'invoice_target_week_start','')), '') is not null then
        v_old_unlocked_delayed_count := v_old_unlocked_delayed_count + 1;
      end if;
    end if;
  end loop;

  begin
    v_new_segments_len := jsonb_array_length(p_new_segments);
  exception when others then
    v_new_segments_len := 0;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','loaded_current',
        'at_utc', public._inv_iso_utc(now()),
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len
      )
    );
  end if;

  -- ============================================================
  -- Build delay map from current segments (preserve overrides)
  -- We preserve only non-blank invoice_target_week_start, keyed by segment_id.
  -- ============================================================
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      continue;
    end if;

    v_delay_text := nullif(btrim(coalesce(e->>'invoice_target_week_start','')), '');
    if v_delay_text is not null then
      v_delay_by_segment_id := jsonb_set(v_delay_by_segment_id, array[sid], to_jsonb(v_delay_text), true);
    end if;
  end loop;

  -- ------------------------------------------------------------
  -- Build locked maps from current segments (preserve these exactly)
  -- Key precedence for locked segments:
  --   1) nhsp_shift_id (if valid uuid)
  --   2) external_row_key (if present)
  --   3) signature (date + start_utc UTC + ref_num)
  -- Fail closed if chosen key is missing or duplicated among locked segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is null then
      continue;
    end if;

    v_locked_count := v_locked_count + 1;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'stats', jsonb_build_object('locked_count', v_locked_count), 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    -- optional identities
    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- validate nhsp_shift_id looks like uuid
    is_uuid := false;
    if v_nhsp_id is not null then
      if v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
        is_uuid := true;
      else
        v_nhsp_id := null;
      end if;
    end if;

    if v_nhsp_id is not null and is_uuid then
      if v_locked_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_nhsp_map := jsonb_set(v_locked_nhsp_map, array[v_nhsp_id], e, true);
      v_locked_nhsp_idmap := jsonb_set(v_locked_nhsp_idmap, array[v_nhsp_id], to_jsonb(sid), true);
      continue;
    end if;

    if v_ext_key is not null then
      if v_locked_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_ext_map := jsonb_set(v_locked_ext_map, array[v_ext_key], e, true);
      v_locked_ext_idmap := jsonb_set(v_locked_ext_idmap, array[v_ext_key], to_jsonb(sid), true);
      continue;
    end if;

    -- fallback signature key
    v_need_sig := true;

    v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
    v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

    begin
      v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
    exception when others then
      v_start_ts := null;
    end;

    if v_date is null or v_start_ts is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_KEY_MISSING',
        'tsfin_id', v_tsfin_id::text,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_KEY_MISSING', 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

    if v_locked_sig_map ? v_sig_key then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'DUPLICATE_LOCKED_SEGMENT_KEY',
        'tsfin_id', v_tsfin_id::text,
        'sig_key', v_sig_key,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_SEGMENT_KEY', 'sig_key', v_sig_key, 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_locked_sig_map := jsonb_set(v_locked_sig_map, array[v_sig_key], e, true);
    v_locked_sig_idmap := jsonb_set(v_locked_sig_idmap, array[v_sig_key], to_jsonb(sid), true);
  end loop;

  v_preserved_locked_count := v_locked_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_maps_built',
        'at_utc', public._inv_iso_utc(now()),
        'locked_count', v_locked_count,
        'need_sig', v_need_sig
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- Build fresh maps from p_new_segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_CONTAINS_NON_OBJECT',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_CONTAINS_NON_OBJECT')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- nhsp_shift_id map
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_fresh_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_nhsp_map := jsonb_set(v_fresh_nhsp_map, array[v_nhsp_id], e, true);
    end if;

    -- external_row_key map
    if v_ext_key is not null then
      if v_fresh_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_ext_map := jsonb_set(v_fresh_ext_map, array[v_ext_key], e, true);
    end if;

    -- signature map (only if required)
    if v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is null or v_start_ts is null then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'NEW_SEGMENT_KEY_MISSING',
          'tsfin_id', v_tsfin_id::text,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENT_KEY_MISSING', 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
      v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

      if v_fresh_sig_map ? v_sig_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_SEGMENT_KEY',
          'tsfin_id', v_tsfin_id::text,
          'sig_key', v_sig_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_SEGMENT_KEY', 'sig_key', v_sig_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_fresh_sig_map := jsonb_set(v_fresh_sig_map, array[v_sig_key], e, true);
    end if;
  end loop;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','fresh_maps_built','at_utc', public._inv_iso_utc(now()))
    );
  end if;

  -- ------------------------------------------------------------
  -- Safety check: every locked key must exist in new segments set.
  -- ------------------------------------------------------------

  -- locked by nhsp_shift_id
  if jsonb_typeof(v_locked_nhsp_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_nhsp_map)
    loop
      if not (v_fresh_nhsp_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_nhsp_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by external_row_key
  if jsonb_typeof(v_locked_ext_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_ext_map)
    loop
      if not (v_fresh_ext_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_ext_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by signature
  if v_need_sig and jsonb_typeof(v_locked_sig_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_sig_map)
    loop
      if not (v_fresh_sig_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_sig_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  if v_missing_locked_ids is not null and array_length(v_missing_locked_ids, 1) is not null then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MISSING_LOCKED_SEGMENTS_IN_NEW',
      'tsfin_id', v_tsfin_id::text,
      'missing_locked_segment_ids', to_jsonb(v_missing_locked_ids),
      'preserved_locked_count', v_preserved_locked_count,
      'old_invalid_count', v_old_invalid
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MISSING_LOCKED_SEGMENTS_IN_NEW')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- ------------------------------------------------------------
  -- Build merged segments in the same order as p_new_segments:
  -- Prefer matching by nhsp_shift_id, then external_row_key, then signature (if needed).
  -- If a match exists -> use locked JSON exactly, else use new JSON.
  -- Re-apply invoice_target_week_start from current TSFIN (by segment_id).
  -- ------------------------------------------------------------
  v_merged_segments := '[]'::jsonb;

  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    chosen := null;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_locked_nhsp_map ? v_nhsp_id then
        chosen := v_locked_nhsp_map->v_nhsp_id;
      end if;
    end if;

    if chosen is null then
      v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');
      if v_ext_key is not null then
        if v_locked_ext_map ? v_ext_key then
          chosen := v_locked_ext_map->v_ext_key;
        end if;
      end if;
    end if;

    if chosen is null and v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is not null and v_start_ts is not null then
        v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
        v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');
        if v_locked_sig_map ? v_sig_key then
          chosen := v_locked_sig_map->v_sig_key;
        end if;
      end if;
    end if;

    if chosen is null then
      chosen := e;
    end if;

    -- preserve delay override from current TSFIN JSON (segment_id match)
    if chosen is not null and jsonb_typeof(chosen) = 'object' then
      sid := nullif(btrim(coalesce(chosen->>'segment_id','')), '');
      if sid is not null and (v_delay_by_segment_id ? sid) then
        chosen := jsonb_set(chosen, '{invoice_target_week_start}', v_delay_by_segment_id->sid, true);
        v_delays_reapplied := v_delays_reapplied + 1;
      end if;
    end if;

    v_merged_segments := v_merged_segments || jsonb_build_array(chosen);
  end loop;

  -- Apply merged segments to the existing invoice_breakdown_json
  v_new_ib := jsonb_set(v_ib, '{segments}', v_merged_segments, true);

  v_new_invalid := public._tsfin_invalid_segment_count(v_new_ib);
  if v_new_invalid > 0 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MERGE_RESULT_STILL_INVALID',
      'tsfin_id', v_tsfin_id::text,
      'old_invalid_count', v_old_invalid,
      'new_invalid_count', v_new_invalid,
      'preserved_locked_count', v_preserved_locked_count
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MERGE_RESULT_STILL_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'new_invalid_count', v_new_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- preserve additional/expenses and keep totals consistent with merged segments
  v_add_ok := false;
  v_add_pay := 0;
  v_add_charge := 0;

  if (v_ib ? 'additional') and jsonb_typeof(v_ib->'additional') = 'object' then
    v_add_pay_text := nullif(btrim(coalesce(v_ib->'additional'->>'pay_ex_vat','')), '');
    v_add_charge_text := nullif(btrim(coalesce(v_ib->'additional'->>'charge_ex_vat','')), '');

    if v_add_pay_text is not null and v_add_charge_text is not null then
      begin
        v_add_pay := v_add_pay_text::numeric;
        v_add_charge := v_add_charge_text::numeric;
        v_add_ok := true;
      exception when others then
        v_add_ok := false;
        v_add_pay := 0;
        v_add_charge := 0;
      end;
    end if;
  end if;

  -- Fallback: derive non-segment totals from stored totals minus ORIGINAL segment sums (pre-merge)
  if not v_add_ok then
    v_old_seg_pay_sum := 0;
    v_old_seg_charge_sum := 0;

    for e in
      select value from jsonb_array_elements(v_ib->'segments') as t(value)
    loop
      if e is null or jsonb_typeof(e) <> 'object' then
        continue;
      end if;

      begin
        v_old_seg_charge_sum := v_old_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
      exception when others then
        null;
      end;

      v_exclude := false;
      begin
        v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
      exception when others then
        v_exclude := false;
      end;

      begin
        if not v_exclude then
          v_old_seg_pay_sum := v_old_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
        end if;
      exception when others then
        null;
      end;
    end loop;

    v_old_seg_pay_sum := round(coalesce(v_old_seg_pay_sum, 0), 2);
    v_old_seg_charge_sum := round(coalesce(v_old_seg_charge_sum, 0), 2);

    v_add_pay := round(coalesce(v_tf_total_pay, 0) - coalesce(v_old_seg_pay_sum, 0), 2);
    v_add_charge := round(coalesce(v_tf_total_charge, 0) - coalesce(v_old_seg_charge_sum, 0), 2);
  else
    v_add_pay := round(coalesce(v_add_pay, 0), 2);
    v_add_charge := round(coalesce(v_add_charge, 0), 2);
  end if;

  v_seg_pay_sum := 0;
  v_seg_charge_sum := 0;

  for e in
    select value from jsonb_array_elements(v_merged_segments) as t(value)
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    -- segment charge sum (include all segments; negative allowed)
    begin
      v_seg_charge_sum := v_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    -- pay sum respects exclude_from_pay when present/true
    v_exclude := false;
    begin
      v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
    exception when others then
      v_exclude := false;
    end;

    begin
      if not v_exclude then
        v_seg_pay_sum := v_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;
  end loop;

  v_seg_pay_sum := round(coalesce(v_seg_pay_sum, 0), 2);
  v_seg_charge_sum := round(coalesce(v_seg_charge_sum, 0), 2);

  -- Totals are pure ex-VAT totals (NO ERNI in totals)
  v_total_pay := round(v_seg_pay_sum + coalesce(v_add_pay, 0), 2);
  v_total_charge := round(v_seg_charge_sum + coalesce(v_add_charge, 0), 2);

  -- ERNI-aware margin:
  -- - PAYE only
  -- - wage pay only = merged segment pay + additional_pay_ex_vat
  -- - never apply ERNI to expenses or mileage
  if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
    v_policy := '{}'::jsonb;
  end if;

  v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

  v_erni_pct_raw := 0;
  begin
    v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
  exception when others then
    v_erni_pct_raw := 0;
  end;

  v_erni_mult := 1;
  if coalesce(v_erni_pct_raw,0) > 0 then
    if v_erni_pct_raw > 1 then
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    else
      v_erni_mult := 1 + v_erni_pct_raw;
    end if;
  end if;

  v_pay_method_u := upper(coalesce(nullif(btrim(coalesce(v_pay_method_text,'')), ''), ''));

  v_erni_applies :=
    (v_pay_method_u = 'PAYE')
    and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

  v_wage_pay := round(coalesce(v_seg_pay_sum, 0) + coalesce(v_additional_pay_ex_vat, 0), 2);
  v_reimb_pay := round(coalesce(v_expenses_pay_ex_vat, 0) + coalesce(v_mileage_pay_ex_vat, 0), 2);

  v_wage_pay_cost := v_wage_pay;
  if v_erni_applies then
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  end if;

  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
  v_margin := round(v_total_charge - v_pay_cost, 2);

  -- Keep invoice_breakdown_json.totals in sync (do not touch other keys)
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_pay_ex_vat}', to_jsonb(v_total_pay), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_charge_ex_vat}', to_jsonb(v_total_charge), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,margin_ex_vat}', to_jsonb(v_margin), true);

  -- Keep additional.margin_ex_vat ERNI-accurate if additional object exists
  if (v_new_ib ? 'additional') and jsonb_typeof(v_new_ib->'additional') = 'object' then
    v_nonseg_wage_pay := round(coalesce(v_additional_pay_ex_vat, 0), 2);
    v_nonseg_reimb_pay := v_reimb_pay;

    v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
    if v_erni_applies then
      v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
    end if;

    v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
    v_nonseg_margin := round(coalesce(v_add_charge, 0) - v_nonseg_pay_cost, 2);

    v_new_ib := jsonb_set(v_new_ib, '{additional,margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  end if;

  -- Update segments + totals + columns (do NOT change lock summary fields)
  update public.timesheets_financials tfu
  set
    invoice_breakdown_json = v_new_ib,
    total_pay_ex_vat = v_total_pay,
    total_charge_ex_vat = v_total_charge,
    margin_ex_vat = v_margin,
    updated_at = now()
  where tfu.id = v_tsfin_id
    and tfu.is_current = true;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated <> 1 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'UPDATE_DID_NOT_APPLY',
      'tsfin_id', v_tsfin_id::text,
      'rows_updated', v_rows_updated
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','UPDATE_DID_NOT_APPLY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- merged length (for reporting)
  begin
    v_merged_len := jsonb_array_length(v_merged_segments);
  exception when others then
    v_merged_len := null;
  end;

  v_ret := jsonb_build_object(
    'ok', true,
    'tsfin_id', v_tsfin_id::text,
    'timesheet_id', p_timesheet_id::text,
    'merged_len', v_merged_len,
    'preserved_locked_count', v_preserved_locked_count,
    'old_invalid_count', v_old_invalid,
    'new_invalid_count', v_new_invalid,
    'delays_reapplied', v_delays_reapplied,
    'actor_user_id', case when p_actor_user_id is null then null else p_actor_user_id::text end,
    'correlation_id', p_correlation_id
  );

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len,
        'locked_preserved', v_preserved_locked_count,
        'merged_len', v_merged_len,
        'rows_updated', v_rows_updated,
        'delays_reapplied', v_delays_reapplied
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
        jsonb_build_object(
          'result', v_ret,
          'corruption', jsonb_build_object(
            'old_invalid_count', v_old_invalid,
            'old_segments_len', v_old_segments_len,
            'old_non_object_count', v_old_non_object_count,
            'old_missing_segment_id_count', v_old_missing_segment_id_count,
            'old_invalid_samples', v_old_invalid_samples
          ),
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('tsfin:' || v_tsfin_id::text),
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  return v_ret;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', coalesce(v_tsfin_id::text,''),
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_invalid_samples', v_old_invalid_samples,
        'locked_count', v_locked_count,
        'preserved_locked_count', v_preserved_locked_count,
        'delays_reapplied', v_delays_reapplied
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_ERROR',
        jsonb_build_object(
          'timesheet_id', coalesce(p_timesheet_id::text,''),
          'tsfin_id', coalesce(v_tsfin_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'correlation_id', p_correlation_id
        ),
        'timesheets_financials',
        case
          when v_tsfin_id is null then ('timesheet:' || coalesce(p_timesheet_id::text,''))
          else ('tsfin:' || v_tsfin_id::text)
        end,
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;





create or replace function public.tsfin_repair_merge_segments_locked(
  p_timesheet_id uuid,
  p_new_segments jsonb,
  p_actor_user_id uuid default null,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = public
as $$
declare
  v_tsfin_id uuid;
  v_ib jsonb;
  v_new_ib jsonb;

  v_old_invalid int := 0;
  v_new_invalid int := 0;

  -- Locked maps (preserve these objects exactly)
  v_locked_nhsp_map jsonb := '{}'::jsonb;  -- nhsp_shift_id -> locked seg json
  v_locked_ext_map  jsonb := '{}'::jsonb;  -- external_row_key -> locked seg json
  v_locked_sig_map  jsonb := '{}'::jsonb;  -- sig_key -> locked seg json

  -- Locked key -> locked segment_id (for error reporting)
  v_locked_nhsp_idmap jsonb := '{}'::jsonb;
  v_locked_ext_idmap  jsonb := '{}'::jsonb;
  v_locked_sig_idmap  jsonb := '{}'::jsonb;

  -- Fresh maps (built from p_new_segments)
  v_fresh_nhsp_map jsonb := '{}'::jsonb;
  v_fresh_ext_map  jsonb := '{}'::jsonb;
  v_fresh_sig_map  jsonb := '{}'::jsonb;

  -- NEW: preserve delay overrides from the current TSFIN JSON (even for unlocked segments)
  v_delay_by_segment_id jsonb := '{}'::jsonb; -- segment_id -> invoice_target_week_start (jsonb string)
  v_delay_text text := null;
  v_delays_reapplied int := 0;

  v_need_sig boolean := false;

  v_missing_locked_ids text[] := array[]::text[];

  v_preserved_locked_count int := 0;
  v_merged_len int := 0;

  e jsonb;

  sid text;
  lock_invoice_id text;

  v_date text;
  v_ref  text;
  v_start_ts timestamptz;
  v_start_norm text;
  v_sig_key text;

  v_nhsp_id text;
  v_ext_key text;

  chosen jsonb;

  v_merged_segments jsonb := '[]'::jsonb;

  v_rows_updated int := 0;

  -- scratch
  k text;
  v_locked_count int := 0;

  -- uuid regex test
  is_uuid boolean;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  -- corruption / diagnostics
  v_old_segments_len int := 0;
  v_old_non_object_count int := 0;
  v_old_missing_segment_id_count int := 0;
  v_old_locked_count int := 0;
  v_old_unlocked_count int := 0;
  v_old_unlocked_delayed_count int := 0;

  v_new_segments_len int := 0;

  v_old_invalid_samples jsonb := '[]'::jsonb;
  v_old_invalid_samples_cap int := 10;

  r record;

  -- helper payload for early returns
  v_ret jsonb;

  -- ✅ preserve additional/expenses and keep totals consistent
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;

  -- ✅ fallback if v_ib.additional is missing/invalid (derive non-seg totals from stored totals - original segment sums)
  v_tf_total_pay numeric := 0;
  v_tf_total_charge numeric := 0;
  v_old_seg_pay_sum numeric := 0;
  v_old_seg_charge_sum numeric := 0;
  v_add_ok boolean := false;
  v_add_pay_text text := null;
  v_add_charge_text text := null;

  v_seg_pay_sum numeric := 0;
  v_seg_charge_sum numeric := 0;
  v_total_pay numeric := 0;
  v_total_charge numeric := 0;

  -- ✅ ERNI-aware margin (PAYE only; wage pay only; never expenses/mileage)
  v_policy jsonb := '{}'::jsonb;
  v_pay_method_text text := '';
  v_additional_pay_ex_vat numeric := 0;
  v_expenses_pay_ex_vat numeric := 0;
  v_mileage_pay_ex_vat numeric := 0;

  v_apply_to text := 'PAYE_ONLY';
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text := '';
  v_erni_applies boolean := false;

  v_wage_pay numeric := 0;
  v_reimb_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_pay_cost numeric := 0;

  v_nonseg_wage_pay numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_pay_cost numeric := 0;
  v_nonseg_margin numeric := 0;

  v_margin numeric := 0;
  v_exclude boolean := false;
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
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'timesheet_id', coalesce(p_timesheet_id::text,''),
        'has_new_segments', (p_new_segments is not null),
        'correlation_id', p_correlation_id
      )
    );
  end if;

  if p_timesheet_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'TIMESHEET_ID_REQUIRED');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','TIMESHEET_ID_REQUIRED')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || coalesce(p_timesheet_id::text,'')),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if p_new_segments is null or jsonb_typeof(p_new_segments) <> 'array' then
    v_ret := jsonb_build_object('ok', false, 'error', 'NEW_SEGMENTS_MUST_BE_ARRAY');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MUST_BE_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Lock the current TSFIN row for this timesheet (also load stored totals + policy/pay inputs for ERNI-aware margin)
  select
      tf.id,
      tf.invoice_breakdown_json,
      coalesce(tf.total_pay_ex_vat, 0)::numeric as total_pay_ex_vat,
      coalesce(tf.total_charge_ex_vat, 0)::numeric as total_charge_ex_vat,
      coalesce(tf.policy_snapshot_json, '{}'::jsonb) as policy_snapshot_json,
      coalesce(tf.pay_method::text, '') as pay_method_text,
      coalesce(tf.additional_pay_ex_vat, 0)::numeric as additional_pay_ex_vat,
      coalesce(tf.expenses_pay_ex_vat, 0)::numeric as expenses_pay_ex_vat,
      coalesce(tf.mileage_pay_ex_vat, 0)::numeric as mileage_pay_ex_vat
    into
      v_tsfin_id,
      v_ib,
      v_tf_total_pay,
      v_tf_total_charge,
      v_policy,
      v_pay_method_text,
      v_additional_pay_ex_vat,
      v_expenses_pay_ex_vat,
      v_mileage_pay_ex_vat
  from public.timesheets_financials tf
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  for update;

  if not found or v_tsfin_id is null then
    v_ret := jsonb_build_object('ok', false, 'error', 'CURRENT_TSFIN_NOT_FOUND');

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_TSFIN_NOT_FOUND')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('timesheet:' || p_timesheet_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','tsfin_locked','at_utc', public._inv_iso_utc(now()), 'tsfin_id', v_tsfin_id::text)
    );
  end if;

  if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_INVOICE_BREAKDOWN_INVALID',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_INVOICE_BREAKDOWN_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if upper(coalesce(v_ib->>'mode','')) <> 'SEGMENTS' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_NOT_SEGMENTS_MODE',
      'tsfin_id', v_tsfin_id::text,
      'mode', upper(coalesce(v_ib->>'mode',''))
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_NOT_SEGMENTS_MODE')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  if jsonb_typeof(v_ib->'segments') <> 'array' then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'CURRENT_SEGMENTS_NOT_ARRAY',
      'tsfin_id', v_tsfin_id::text
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','CURRENT_SEGMENTS_NOT_ARRAY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- Record old invalid count (for reporting)
  v_old_invalid := public._tsfin_invalid_segment_count(v_ib);

  -- Diagnostics: count what is "corrupted" in current segments (non-object/null/missing ids etc.)
  begin
    v_old_segments_len := jsonb_array_length(v_ib->'segments');
  exception when others then
    v_old_segments_len := 0;
  end;

  for r in
    select value as seg, ordinality as ord
    from jsonb_array_elements(v_ib->'segments') with ordinality
  loop
    if r.seg is null or jsonb_typeof(r.seg) <> 'object' then
      v_old_non_object_count := v_old_non_object_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'NON_OBJECT',
                'type', coalesce(jsonb_typeof(r.seg), 'null')
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;

      continue;
    end if;

    sid := nullif(btrim(coalesce(r.seg->>'segment_id','')), '');
    if sid is null then
      v_old_missing_segment_id_count := v_old_missing_segment_id_count + 1;

      if v_invoice_debug then
        begin
          if jsonb_array_length(v_old_invalid_samples) < v_old_invalid_samples_cap then
            v_old_invalid_samples := v_old_invalid_samples || jsonb_build_array(
              jsonb_build_object(
                'ord', r.ord,
                'kind', 'MISSING_SEGMENT_ID'
              )
            );
          end if;
        exception when others then
          null;
        end;
      end if;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(r.seg->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is not null then
      v_old_locked_count := v_old_locked_count + 1;
    else
      v_old_unlocked_count := v_old_unlocked_count + 1;

      if nullif(btrim(coalesce(r.seg->>'invoice_target_week_start','')), '') is not null then
        v_old_unlocked_delayed_count := v_old_unlocked_delayed_count + 1;
      end if;
    end if;
  end loop;

  begin
    v_new_segments_len := jsonb_array_length(p_new_segments);
  exception when others then
    v_new_segments_len := 0;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','loaded_current',
        'at_utc', public._inv_iso_utc(now()),
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len
      )
    );
  end if;

  -- ============================================================
  -- Build delay map from current segments (preserve overrides)
  -- We preserve only non-blank invoice_target_week_start, keyed by segment_id.
  -- ============================================================
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      continue;
    end if;

    v_delay_text := nullif(btrim(coalesce(e->>'invoice_target_week_start','')), '');
    if v_delay_text is not null then
      v_delay_by_segment_id := jsonb_set(v_delay_by_segment_id, array[sid], to_jsonb(v_delay_text), true);
    end if;
  end loop;

  -- ------------------------------------------------------------
  -- Build locked maps from current segments (preserve these exactly)
  -- Key precedence for locked segments:
  --   1) nhsp_shift_id (if valid uuid)
  --   2) external_row_key (if present)
  --   3) signature (date + start_utc UTC + ref_num)
  -- Fail closed if chosen key is missing or duplicated among locked segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(v_ib->'segments') value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    lock_invoice_id := nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '');
    if lock_invoice_id is null then
      continue;
    end if;

    v_locked_count := v_locked_count + 1;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'stats', jsonb_build_object('locked_count', v_locked_count), 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    -- optional identities
    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- validate nhsp_shift_id looks like uuid
    is_uuid := false;
    if v_nhsp_id is not null then
      if v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
        is_uuid := true;
      else
        v_nhsp_id := null;
      end if;
    end if;

    if v_nhsp_id is not null and is_uuid then
      if v_locked_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_nhsp_map := jsonb_set(v_locked_nhsp_map, array[v_nhsp_id], e, true);
      v_locked_nhsp_idmap := jsonb_set(v_locked_nhsp_idmap, array[v_nhsp_id], to_jsonb(sid), true);
      continue;
    end if;

    if v_ext_key is not null then
      if v_locked_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_LOCKED_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key, 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_locked_ext_map := jsonb_set(v_locked_ext_map, array[v_ext_key], e, true);
      v_locked_ext_idmap := jsonb_set(v_locked_ext_idmap, array[v_ext_key], to_jsonb(sid), true);
      continue;
    end if;

    -- fallback signature key
    v_need_sig := true;

    v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
    v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

    begin
      v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
    exception when others then
      v_start_ts := null;
    end;

    if v_date is null or v_start_ts is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'LOCKED_SEGMENT_KEY_MISSING',
        'tsfin_id', v_tsfin_id::text,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','LOCKED_SEGMENT_KEY_MISSING', 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

    if v_locked_sig_map ? v_sig_key then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'DUPLICATE_LOCKED_SEGMENT_KEY',
        'tsfin_id', v_tsfin_id::text,
        'sig_key', v_sig_key,
        'segment_id', sid
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_LOCKED_SEGMENT_KEY', 'sig_key', v_sig_key, 'segment_id', sid)
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_locked_sig_map := jsonb_set(v_locked_sig_map, array[v_sig_key], e, true);
    v_locked_sig_idmap := jsonb_set(v_locked_sig_idmap, array[v_sig_key], to_jsonb(sid), true);
  end loop;

  v_preserved_locked_count := v_locked_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_maps_built',
        'at_utc', public._inv_iso_utc(now()),
        'locked_count', v_locked_count,
        'need_sig', v_need_sig
      )
    );
  end if;

  -- ------------------------------------------------------------
  -- Build fresh maps from p_new_segments.
  -- ------------------------------------------------------------
  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_CONTAINS_NON_OBJECT',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_CONTAINS_NON_OBJECT')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    sid := nullif(btrim(coalesce(e->>'segment_id','')), '');
    if sid is null then
      v_ret := jsonb_build_object(
        'ok', false,
        'error', 'NEW_SEGMENTS_MISSING_SEGMENT_ID',
        'tsfin_id', v_tsfin_id::text
      );

      if v_invoice_debug then
        begin
          v_dbg_steps := v_dbg_steps || jsonb_build_array(
            jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENTS_MISSING_SEGMENT_ID')
          );

          perform public._inv_write_audit(
            p_actor_user_id,
            'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
            jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
            'timesheets_financials',
            ('tsfin:' || v_tsfin_id::text),
            null,
            'INVOICE_DEBUG',
            null, null, p_correlation_id
          );
        exception when others then
          null;
        end;
      end if;

      return v_ret;
    end if;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');

    -- nhsp_shift_id map
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_fresh_nhsp_map ? v_nhsp_id then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_NHSP_SHIFT_ID',
          'tsfin_id', v_tsfin_id::text,
          'nhsp_shift_id', v_nhsp_id
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_NHSP_SHIFT_ID', 'nhsp_shift_id', v_nhsp_id)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_nhsp_map := jsonb_set(v_fresh_nhsp_map, array[v_nhsp_id], e, true);
    end if;

    -- external_row_key map
    if v_ext_key is not null then
      if v_fresh_ext_map ? v_ext_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_EXTERNAL_ROW_KEY',
          'tsfin_id', v_tsfin_id::text,
          'external_row_key', v_ext_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_EXTERNAL_ROW_KEY', 'external_row_key', v_ext_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;
      v_fresh_ext_map := jsonb_set(v_fresh_ext_map, array[v_ext_key], e, true);
    end if;

    -- signature map (only if required)
    if v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is null or v_start_ts is null then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'NEW_SEGMENT_KEY_MISSING',
          'tsfin_id', v_tsfin_id::text,
          'segment_id', sid
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','NEW_SEGMENT_KEY_MISSING', 'segment_id', sid)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
      v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');

      if v_fresh_sig_map ? v_sig_key then
        v_ret := jsonb_build_object(
          'ok', false,
          'error', 'DUPLICATE_NEW_SEGMENT_KEY',
          'tsfin_id', v_tsfin_id::text,
          'sig_key', v_sig_key
        );

        if v_invoice_debug then
          begin
            v_dbg_steps := v_dbg_steps || jsonb_build_array(
              jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','DUPLICATE_NEW_SEGMENT_KEY', 'sig_key', v_sig_key)
            );

            perform public._inv_write_audit(
              p_actor_user_id,
              'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
              jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
              'timesheets_financials',
              ('tsfin:' || v_tsfin_id::text),
              null,
              'INVOICE_DEBUG',
              null, null, p_correlation_id
            );
          exception when others then
            null;
          end;
        end if;

        return v_ret;
      end if;

      v_fresh_sig_map := jsonb_set(v_fresh_sig_map, array[v_sig_key], e, true);
    end if;
  end loop;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','fresh_maps_built','at_utc', public._inv_iso_utc(now()))
    );
  end if;

  -- ------------------------------------------------------------
  -- Safety check: every locked key must exist in new segments set.
  -- ------------------------------------------------------------

  -- locked by nhsp_shift_id
  if jsonb_typeof(v_locked_nhsp_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_nhsp_map)
    loop
      if not (v_fresh_nhsp_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_nhsp_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by external_row_key
  if jsonb_typeof(v_locked_ext_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_ext_map)
    loop
      if not (v_fresh_ext_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_ext_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  -- locked by signature
  if v_need_sig and jsonb_typeof(v_locked_sig_map) = 'object' then
    for k in select jsonb_object_keys(v_locked_sig_map)
    loop
      if not (v_fresh_sig_map ? k) then
        v_missing_locked_ids := array_append(
          v_missing_locked_ids,
          coalesce(nullif(btrim(coalesce(v_locked_sig_idmap->>k,'')), ''), k)
        );
      end if;
    end loop;
  end if;

  if v_missing_locked_ids is not null and array_length(v_missing_locked_ids, 1) is not null then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MISSING_LOCKED_SEGMENTS_IN_NEW',
      'tsfin_id', v_tsfin_id::text,
      'missing_locked_segment_ids', to_jsonb(v_missing_locked_ids),
      'preserved_locked_count', v_preserved_locked_count,
      'old_invalid_count', v_old_invalid
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MISSING_LOCKED_SEGMENTS_IN_NEW')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- ------------------------------------------------------------
  -- Build merged segments in the same order as p_new_segments:
  -- Prefer matching by nhsp_shift_id, then external_row_key, then signature (if needed).
  -- If a match exists -> use locked JSON exactly, else use new JSON.
  -- Re-apply invoice_target_week_start from current TSFIN (by segment_id).
  -- ------------------------------------------------------------
  v_merged_segments := '[]'::jsonb;

  for e in
    select value from jsonb_array_elements(p_new_segments) value
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    chosen := null;

    v_nhsp_id := nullif(btrim(coalesce(e->>'nhsp_shift_id','')), '');
    if v_nhsp_id is not null and v_nhsp_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      if v_locked_nhsp_map ? v_nhsp_id then
        chosen := v_locked_nhsp_map->v_nhsp_id;
      end if;
    end if;

    if chosen is null then
      v_ext_key := nullif(btrim(coalesce(e->>'external_row_key','')), '');
      if v_ext_key is not null then
        if v_locked_ext_map ? v_ext_key then
          chosen := v_locked_ext_map->v_ext_key;
        end if;
      end if;
    end if;

    if chosen is null and v_need_sig then
      v_date := nullif(btrim(coalesce(e->>'date', e->>'work_date', '')), '');
      v_ref  := nullif(btrim(coalesce(e->>'ref_num', e->>'ref', e->>'reference', '')), '');

      begin
        v_start_ts := nullif(btrim(coalesce(e->>'start_utc','')), '')::timestamptz;
      exception when others then
        v_start_ts := null;
      end;

      if v_date is not null and v_start_ts is not null then
        v_start_norm := to_char(v_start_ts at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
        v_sig_key := v_date || '|' || v_start_norm || '|' || coalesce(v_ref, '');
        if v_locked_sig_map ? v_sig_key then
          chosen := v_locked_sig_map->v_sig_key;
        end if;
      end if;
    end if;

    if chosen is null then
      chosen := e;
    end if;

    -- preserve delay override from current TSFIN JSON (segment_id match)
    if chosen is not null and jsonb_typeof(chosen) = 'object' then
      sid := nullif(btrim(coalesce(chosen->>'segment_id','')), '');
      if sid is not null and (v_delay_by_segment_id ? sid) then
        chosen := jsonb_set(chosen, '{invoice_target_week_start}', v_delay_by_segment_id->sid, true);
        v_delays_reapplied := v_delays_reapplied + 1;
      end if;
    end if;

    v_merged_segments := v_merged_segments || jsonb_build_array(chosen);
  end loop;

  -- Apply merged segments to the existing invoice_breakdown_json
  v_new_ib := jsonb_set(v_ib, '{segments}', v_merged_segments, true);

  v_new_invalid := public._tsfin_invalid_segment_count(v_new_ib);
  if v_new_invalid > 0 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'MERGE_RESULT_STILL_INVALID',
      'tsfin_id', v_tsfin_id::text,
      'old_invalid_count', v_old_invalid,
      'new_invalid_count', v_new_invalid,
      'preserved_locked_count', v_preserved_locked_count
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','MERGE_RESULT_STILL_INVALID')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object(
            'result', v_ret,
            'corruption', jsonb_build_object(
              'old_invalid_count', v_old_invalid,
              'new_invalid_count', v_new_invalid,
              'old_segments_len', v_old_segments_len,
              'old_non_object_count', v_old_non_object_count,
              'old_missing_segment_id_count', v_old_missing_segment_id_count,
              'old_invalid_samples', v_old_invalid_samples
            ),
            'steps', v_dbg_steps
          ),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- preserve additional/expenses and keep totals consistent with merged segments
  v_add_ok := false;
  v_add_pay := 0;
  v_add_charge := 0;

  if (v_ib ? 'additional') and jsonb_typeof(v_ib->'additional') = 'object' then
    v_add_pay_text := nullif(btrim(coalesce(v_ib->'additional'->>'pay_ex_vat','')), '');
    v_add_charge_text := nullif(btrim(coalesce(v_ib->'additional'->>'charge_ex_vat','')), '');

    if v_add_pay_text is not null and v_add_charge_text is not null then
      begin
        v_add_pay := v_add_pay_text::numeric;
        v_add_charge := v_add_charge_text::numeric;
        v_add_ok := true;
      exception when others then
        v_add_ok := false;
        v_add_pay := 0;
        v_add_charge := 0;
      end;
    end if;
  end if;

  -- Fallback: derive non-segment totals from stored totals minus ORIGINAL segment sums (pre-merge)
  if not v_add_ok then
    v_old_seg_pay_sum := 0;
    v_old_seg_charge_sum := 0;

    for e in
      select value from jsonb_array_elements(v_ib->'segments') as t(value)
    loop
      if e is null or jsonb_typeof(e) <> 'object' then
        continue;
      end if;

      begin
        v_old_seg_charge_sum := v_old_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
      exception when others then
        null;
      end;

      v_exclude := false;
      begin
        v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
      exception when others then
        v_exclude := false;
      end;

      begin
        if not v_exclude then
          v_old_seg_pay_sum := v_old_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
        end if;
      exception when others then
        null;
      end;
    end loop;

    v_old_seg_pay_sum := round(coalesce(v_old_seg_pay_sum, 0), 2);
    v_old_seg_charge_sum := round(coalesce(v_old_seg_charge_sum, 0), 2);

    v_add_pay := round(coalesce(v_tf_total_pay, 0) - coalesce(v_old_seg_pay_sum, 0), 2);
    v_add_charge := round(coalesce(v_tf_total_charge, 0) - coalesce(v_old_seg_charge_sum, 0), 2);
  else
    v_add_pay := round(coalesce(v_add_pay, 0), 2);
    v_add_charge := round(coalesce(v_add_charge, 0), 2);
  end if;

  v_seg_pay_sum := 0;
  v_seg_charge_sum := 0;

  for e in
    select value from jsonb_array_elements(v_merged_segments) as t(value)
  loop
    if e is null or jsonb_typeof(e) <> 'object' then
      continue;
    end if;

    -- segment charge sum (include all segments; negative allowed)
    begin
      v_seg_charge_sum := v_seg_charge_sum + coalesce(nullif(btrim(coalesce(e->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    -- pay sum respects exclude_from_pay when present/true
    v_exclude := false;
    begin
      v_exclude := coalesce(nullif(btrim(coalesce(e->>'exclude_from_pay','')), '')::boolean, false);
    exception when others then
      v_exclude := false;
    end;

    begin
      if not v_exclude then
        v_seg_pay_sum := v_seg_pay_sum + coalesce(nullif(btrim(coalesce(e->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;
  end loop;

  v_seg_pay_sum := round(coalesce(v_seg_pay_sum, 0), 2);
  v_seg_charge_sum := round(coalesce(v_seg_charge_sum, 0), 2);

  -- Totals are pure ex-VAT totals (NO ERNI in totals)
  v_total_pay := round(v_seg_pay_sum + coalesce(v_add_pay, 0), 2);
  v_total_charge := round(v_seg_charge_sum + coalesce(v_add_charge, 0), 2);

  -- ERNI-aware margin:
  -- - PAYE only
  -- - wage pay only = merged segment pay + additional_pay_ex_vat
  -- - never apply ERNI to expenses or mileage
  if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
    v_policy := '{}'::jsonb;
  end if;

  v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

  v_erni_pct_raw := 0;
  begin
    v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
  exception when others then
    v_erni_pct_raw := 0;
  end;

  v_erni_mult := 1;
  if coalesce(v_erni_pct_raw,0) > 0 then
    if v_erni_pct_raw > 1 then
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    else
      v_erni_mult := 1 + v_erni_pct_raw;
    end if;
  end if;

  v_pay_method_u := upper(coalesce(nullif(btrim(coalesce(v_pay_method_text,'')), ''), ''));

  v_erni_applies :=
    (v_pay_method_u = 'PAYE')
    and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

  v_wage_pay := round(coalesce(v_seg_pay_sum, 0) + coalesce(v_additional_pay_ex_vat, 0), 2);
  v_reimb_pay := round(coalesce(v_expenses_pay_ex_vat, 0) + coalesce(v_mileage_pay_ex_vat, 0), 2);

  v_wage_pay_cost := v_wage_pay;
  if v_erni_applies then
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  end if;

  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
  v_margin := round(v_total_charge - v_pay_cost, 2);

  -- Keep invoice_breakdown_json.totals in sync (do not touch other keys)
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_pay_ex_vat}', to_jsonb(v_total_pay), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,total_charge_ex_vat}', to_jsonb(v_total_charge), true);
  v_new_ib := jsonb_set(v_new_ib, '{totals,margin_ex_vat}', to_jsonb(v_margin), true);

  -- Keep additional.margin_ex_vat ERNI-accurate if additional object exists
  if (v_new_ib ? 'additional') and jsonb_typeof(v_new_ib->'additional') = 'object' then
    v_nonseg_wage_pay := round(coalesce(v_additional_pay_ex_vat, 0), 2);
    v_nonseg_reimb_pay := v_reimb_pay;

    v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
    if v_erni_applies then
      v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
    end if;

    v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
    v_nonseg_margin := round(coalesce(v_add_charge, 0) - v_nonseg_pay_cost, 2);

    v_new_ib := jsonb_set(v_new_ib, '{additional,margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  end if;

  -- Update segments + totals + columns (do NOT change lock summary fields)
  update public.timesheets_financials tfu
  set
    invoice_breakdown_json = v_new_ib,
    total_pay_ex_vat = v_total_pay,
    total_charge_ex_vat = v_total_charge,
    margin_ex_vat = v_margin,
    updated_at = now()
  where tfu.id = v_tsfin_id
    and tfu.is_current = true;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated <> 1 then
    v_ret := jsonb_build_object(
      'ok', false,
      'error', 'UPDATE_DID_NOT_APPLY',
      'tsfin_id', v_tsfin_id::text,
      'rows_updated', v_rows_updated
    );

    if v_invoice_debug then
      begin
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object('step','return','at_utc', public._inv_iso_utc(now()), 'error','UPDATE_DID_NOT_APPLY')
        );

        perform public._inv_write_audit(
          p_actor_user_id,
          'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
          jsonb_build_object('result', v_ret, 'steps', v_dbg_steps),
          'timesheets_financials',
          ('tsfin:' || v_tsfin_id::text),
          null,
          'INVOICE_DEBUG',
          null, null, p_correlation_id
        );
      exception when others then
        null;
      end;
    end if;

    return v_ret;
  end if;

  -- merged length (for reporting)
  begin
    v_merged_len := jsonb_array_length(v_merged_segments);
  exception when others then
    v_merged_len := null;
  end;

  v_ret := jsonb_build_object(
    'ok', true,
    'tsfin_id', v_tsfin_id::text,
    'timesheet_id', p_timesheet_id::text,
    'merged_len', v_merged_len,
    'preserved_locked_count', v_preserved_locked_count,
    'old_invalid_count', v_old_invalid,
    'new_invalid_count', v_new_invalid,
    'delays_reapplied', v_delays_reapplied,
    'actor_user_id', case when p_actor_user_id is null then null else p_actor_user_id::text end,
    'correlation_id', p_correlation_id
  );

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', v_tsfin_id::text,
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_locked_count', v_old_locked_count,
        'old_unlocked_count', v_old_unlocked_count,
        'old_unlocked_delayed_count', v_old_unlocked_delayed_count,
        'new_segments_len', v_new_segments_len,
        'locked_preserved', v_preserved_locked_count,
        'merged_len', v_merged_len,
        'rows_updated', v_rows_updated,
        'delays_reapplied', v_delays_reapplied
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_DEBUG',
        jsonb_build_object(
          'result', v_ret,
          'corruption', jsonb_build_object(
            'old_invalid_count', v_old_invalid,
            'old_segments_len', v_old_segments_len,
            'old_non_object_count', v_old_non_object_count,
            'old_missing_segment_id_count', v_old_missing_segment_id_count,
            'old_invalid_samples', v_old_invalid_samples
          ),
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('tsfin:' || v_tsfin_id::text),
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  return v_ret;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfin_id', coalesce(v_tsfin_id::text,''),
        'old_invalid_count', v_old_invalid,
        'new_invalid_count', v_new_invalid,
        'old_segments_len', v_old_segments_len,
        'old_non_object_count', v_old_non_object_count,
        'old_missing_segment_id_count', v_old_missing_segment_id_count,
        'old_invalid_samples', v_old_invalid_samples,
        'locked_count', v_locked_count,
        'preserved_locked_count', v_preserved_locked_count,
        'delays_reapplied', v_delays_reapplied
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'TSFIN_REPAIR_MERGE_SEGMENTS_ERROR',
        jsonb_build_object(
          'timesheet_id', coalesce(p_timesheet_id::text,''),
          'tsfin_id', coalesce(v_tsfin_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'correlation_id', p_correlation_id
        ),
        'timesheets_financials',
        case
          when v_tsfin_id is null then ('timesheet:' || coalesce(p_timesheet_id::text,''))
          else ('tsfin:' || v_tsfin_id::text)
        end,
        null,
        'INVOICE_DEBUG',
        null, null, p_correlation_id
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;
















grant execute on function public.tsfin_repair_merge_segments_locked(uuid, jsonb, uuid, text) to service_role;

select pg_notify('pgrst', 'reload schema');


create or replace function public.tsfin_update_segments_locked(
  p_timesheet_id uuid,
  p_segment_updates jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  -- locked row
  v_tf_id uuid;
  v_basis text;
  v_breakdown jsonb;

  -- stored totals (used to preserve non-segment totals if breakdown.additional is missing/incomplete)
  v_total_pay_ex_vat numeric;
  v_total_charge_ex_vat numeric;

  -- ERNI / policy inputs (for margin accuracy)
  v_policy jsonb;
  v_pay_method_text text;
  v_additional_pay_ex_vat numeric;
  v_expenses_pay_ex_vat numeric;
  v_mileage_pay_ex_vat numeric;

  v_week_ending date;
  v_natural_week_start date;

  v_allow_invoice_target_change boolean := false;

  -- updates map: { "<segment_id>": {exclude_from_pay:bool?, invoice_target_week_start:text?}, ... }
  v_updates jsonb := '{}'::jsonb;
  v_elem jsonb;
  v_sid text;
  v_entry jsonb;
  v_req_target text;

  -- apply loop
  v_segments jsonb;
  v_seg jsonb;
  v_out_segments jsonb := '[]'::jsonb;

  v_exclude boolean;
  v_new_total_pay_segments numeric := 0;
  v_new_total_charge_segments numeric := 0;

  -- ✅ preserve "additional/expenses" totals from breakdown.additional (now includes expenses+mileage in TSFIN invariant)
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;

  -- ✅ if breakdown.additional is missing/incomplete, derive non-segment totals from stored totals minus ORIGINAL segment totals
  v_old_total_pay_segments numeric := 0;
  v_old_total_charge_segments numeric := 0;
  v_use_breakdown_additional boolean := false;
  v_add_text_pay text;
  v_add_text_charge text;
  v_add_units jsonb;

  v_new_total_pay numeric := 0;
  v_new_total_charge numeric := 0;
  v_new_margin numeric := 0;

  -- ✅ ERNI-aware margin (PAYE only; applies to wage pay only: segments pay + additional_pay_ex_vat; never to expenses/mileage)
  v_apply_to text;
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text;
  v_erni_applies boolean := false;

  v_wage_pay numeric := 0;
  v_reimb_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_pay_cost numeric := 0;

  v_nonseg_wage_pay numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_pay_cost numeric := 0;
  v_nonseg_margin numeric := 0;

  -- validation helpers
  v_locked_invoice_id_text text;
  v_cur_target text;
  v_cur_target_has boolean;
  v_clear_requested boolean;

  v_updated_row jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id required';
  end if;

  if p_segment_updates is null or jsonb_typeof(p_segment_updates) <> 'array' then
    raise exception 'segment_updates must be a json array';
  end if;

  -- 1) Lock the current TSFIN row (critical: prevents lost-update corruption)
  select
    tf.id,
    upper(coalesce(tf.basis::text, '')) as basis,
    tf.invoice_breakdown_json,
    coalesce(tf.total_pay_ex_vat, 0)::numeric as total_pay_ex_vat,
    coalesce(tf.total_charge_ex_vat, 0)::numeric as total_charge_ex_vat,
    ts.week_ending_date::date as week_ending_date,

    -- ERNI/policy inputs for accurate margin recompute
    coalesce(tf.policy_snapshot_json, '{}'::jsonb) as policy_snapshot_json,
    coalesce(tf.pay_method::text, '') as pay_method_text,
    coalesce(tf.additional_pay_ex_vat, 0)::numeric as additional_pay_ex_vat,
    coalesce(tf.expenses_pay_ex_vat, 0)::numeric as expenses_pay_ex_vat,
    coalesce(tf.mileage_pay_ex_vat, 0)::numeric as mileage_pay_ex_vat
  into
    v_tf_id,
    v_basis,
    v_breakdown,
    v_total_pay_ex_vat,
    v_total_charge_ex_vat,
    v_week_ending,

    v_policy,
    v_pay_method_text,
    v_additional_pay_ex_vat,
    v_expenses_pay_ex_vat,
    v_mileage_pay_ex_vat
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  order by tf.created_at desc
  limit 1
  for update;

  if not found then
    raise exception 'Current financial snapshot not found for timesheet_id %', p_timesheet_id;
  end if;

  if v_breakdown is null or jsonb_typeof(v_breakdown) <> 'object' then
    raise exception 'No invoice_breakdown_json present on snapshot';
  end if;

  if upper(coalesce(v_breakdown->>'mode','')) <> 'SEGMENTS'
     or jsonb_typeof(v_breakdown->'segments') <> 'array' then
    raise exception 'This snapshot is not SEGMENTS-based; cannot update per-line settings';
  end if;

  v_segments := v_breakdown->'segments';

  -- Allowed bases (match Worker)
  v_allow_invoice_target_change :=
    v_basis in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT');

  if v_week_ending is not null then
    v_natural_week_start := (v_week_ending - 6);
  else
    v_natural_week_start := null;
  end if;

  -- 2) Build update map (last write wins)
  for v_elem in
    select value
    from jsonb_array_elements(p_segment_updates) as t(value)
  loop
    if v_elem is null or jsonb_typeof(v_elem) <> 'object' then
      continue;
    end if;

    v_sid := nullif(btrim(coalesce(v_elem->>'segment_id','')), '');
    if v_sid is null then
      continue;
    end if;

    v_entry := '{}'::jsonb;

    if v_elem ? 'exclude_from_pay' then
      v_entry := v_entry || jsonb_build_object('exclude_from_pay', v_elem->'exclude_from_pay');
    end if;

    -- ✅ FIX: support explicit clear (key present with null/blank) by carrying a json null in the update map
    if (v_elem ? 'invoice_target_week_start') then
      if nullif(btrim(coalesce(v_elem->>'invoice_target_week_start','')), '') is not null then
        v_entry := v_entry || jsonb_build_object(
          'invoice_target_week_start',
          nullif(btrim(coalesce(v_elem->>'invoice_target_week_start','')), '')
        );
      else
        v_entry := v_entry || jsonb_build_object('invoice_target_week_start', null);
      end if;
    end if;

    -- merge into map
    v_updates := v_updates || jsonb_build_object(v_sid, v_entry);
  end loop;

  if jsonb_typeof(v_updates) <> 'object' or v_updates = '{}'::jsonb then
    raise exception 'No valid segment_id entries to update';
  end if;

  -- 3) Pre-validate invoice_target_week_start changes (match Worker semantics)
  if not v_allow_invoice_target_change then
    if exists (
      select 1
      from jsonb_each(v_updates) as e(key, value)
      where (e.value ? 'invoice_target_week_start')
    ) then
      raise exception 'invoice_target_week_start cannot be changed for this snapshot basis';
    end if;
  else
    for v_sid, v_entry in
      select key, value
      from jsonb_each(v_updates)
    loop
      if v_entry is null or jsonb_typeof(v_entry) <> 'object' then
        continue;
      end if;

      if not (v_entry ? 'invoice_target_week_start') then
        continue;
      end if;

      v_req_target := nullif(btrim(coalesce(v_entry->>'invoice_target_week_start','')), '');
      v_clear_requested := (v_req_target is null);

      -- Find the current segment (objects only; unknown ids are ignored like Worker)
      select
        nullif(btrim(coalesce(seg.value->>'invoice_locked_invoice_id','')), '') as locked_inv,
        nullif(btrim(coalesce(seg.value->>'invoice_target_week_start','')), '') as cur_target,
        (seg.value ? 'invoice_target_week_start') as has_target
      into
        v_locked_invoice_id_text,
        v_cur_target,
        v_cur_target_has
      from jsonb_array_elements(v_segments) as seg(value)
      where jsonb_typeof(seg.value) = 'object'
        and nullif(btrim(coalesce(seg.value->>'segment_id','')), '') = v_sid
      limit 1;

      if v_locked_invoice_id_text is not null then
        -- ✅ FIX: explicit clear must be treated as a change unless it is a no-op
        if v_clear_requested then
          if not (
            (v_cur_target_has is not true) or
            (v_cur_target is null) or
            (v_natural_week_start is not null and v_cur_target = v_natural_week_start::text)
          ) then
            raise exception
              'Segment % is attached to an invoice and cannot have invoice delay changed. Remove from invoice first.',
              v_sid;
          end if;
          continue;
        end if;

        -- No-op tolerant rule (match Worker)
        if not (
          v_req_target = coalesce(v_cur_target, '')
          or (v_cur_target is null and v_natural_week_start is not null and v_req_target = v_natural_week_start::text)
        ) then
          raise exception
            'Segment % is attached to an invoice and cannot have invoice delay changed. Remove from invoice first.',
            v_sid;
        end if;
        continue;
      end if;

      -- For unlocked segments: only validate when setting an actual date (clears skip validation)
      if v_clear_requested then
        continue;
      end if;

      if v_natural_week_start is not null then
        begin
          if (v_req_target::date < v_natural_week_start) then
            raise exception
              'invoice_target_week_start for segment % cannot be earlier than natural week start %',
              v_sid, v_natural_week_start::text;
          end if;
        exception when others then
          raise exception 'invoice_target_week_start for segment % is invalid: %', v_sid, v_req_target;
        end;
      end if;
    end loop;
  end if;

  -- ✅ Preserve "additional/expenses" totals.
  -- Prefer breakdown.additional if present & numeric; otherwise derive from stored totals minus ORIGINAL segment totals.
  v_use_breakdown_additional := false;
  v_add_pay := 0;
  v_add_charge := 0;

  if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
    v_add_text_pay := nullif(btrim(coalesce(v_breakdown->'additional'->>'pay_ex_vat','')), '');
    v_add_text_charge := nullif(btrim(coalesce(v_breakdown->'additional'->>'charge_ex_vat','')), '');

    if v_add_text_pay is not null and v_add_text_charge is not null then
      begin
        v_add_pay := v_add_text_pay::numeric;
        v_add_charge := v_add_text_charge::numeric;
        v_use_breakdown_additional := true;
      exception when others then
        v_use_breakdown_additional := false;
        v_add_pay := 0;
        v_add_charge := 0;
      end;
    end if;
  end if;

  -- 4) Apply updates + recompute totals (segments pay) WITHOUT losing additional/expenses
  v_old_total_pay_segments := 0;
  v_old_total_charge_segments := 0;

  v_new_total_pay_segments := 0;
  v_new_total_charge_segments := 0;
  v_out_segments := '[]'::jsonb;

  for v_seg in
    select value
    from jsonb_array_elements(v_segments) as t(value)
  loop
    if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
      v_out_segments := v_out_segments || jsonb_build_array(v_seg);
      continue;
    end if;

    v_sid := nullif(btrim(coalesce(v_seg->>'segment_id','')), '');
    if v_sid is null then
      v_out_segments := v_out_segments || jsonb_build_array(v_seg);
      continue;
    end if;

    v_entry := v_updates->v_sid;

    -- ORIGINAL exclude_from_pay (for old totals)
    begin
      v_exclude := coalesce(nullif(btrim(coalesce(v_seg->>'exclude_from_pay','')), '')::boolean, false);
    exception when others then
      v_exclude := false;
    end;

    -- ORIGINAL totals
    begin
      v_old_total_charge_segments :=
        v_old_total_charge_segments
        + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    begin
      if not v_exclude then
        v_old_total_pay_segments :=
          v_old_total_pay_segments
          + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;

    -- UPDATED exclude_from_pay
    if v_entry is not null and jsonb_typeof(v_entry) = 'object' and (v_entry ? 'exclude_from_pay') then
      begin
        v_exclude := coalesce(nullif(btrim(coalesce(v_entry->>'exclude_from_pay','')), '')::boolean, false);
      exception when others then
        v_exclude := false;
      end;
      v_seg := jsonb_set(v_seg, '{exclude_from_pay}', to_jsonb(v_exclude), true);
    end if;

    -- invoice_target_week_start (allowed bases only; unlocked segments only)
    if v_allow_invoice_target_change
       and v_entry is not null
       and jsonb_typeof(v_entry) = 'object'
       and (v_entry ? 'invoice_target_week_start') then
      v_locked_invoice_id_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
      if v_locked_invoice_id_text is null then
        v_req_target := nullif(btrim(coalesce(v_entry->>'invoice_target_week_start','')), '');
        if v_req_target is not null then
          v_seg := jsonb_set(v_seg, '{invoice_target_week_start}', to_jsonb(v_req_target), true);
        else
          -- ✅ FIX: explicit clear removes the key entirely
          v_seg := v_seg - 'invoice_target_week_start';
        end if;
      end if;
    end if;

    -- NEW totals
    begin
      v_new_total_charge_segments :=
        v_new_total_charge_segments
        + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
    exception when others then
      null;
    end;

    begin
      if not v_exclude then
        v_new_total_pay_segments :=
          v_new_total_pay_segments
          + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
      end if;
    exception when others then
      null;
    end;

    v_out_segments := v_out_segments || jsonb_build_array(v_seg);
  end loop;

  v_old_total_pay_segments := round(coalesce(v_old_total_pay_segments, 0), 2);
  v_old_total_charge_segments := round(coalesce(v_old_total_charge_segments, 0), 2);

  -- If breakdown.additional missing/incomplete, derive non-segment totals from stored totals minus ORIGINAL segment totals
  if v_use_breakdown_additional is not true then
    v_add_pay := round(coalesce(v_total_pay_ex_vat, 0) - coalesce(v_old_total_pay_segments, 0), 2);
    v_add_charge := round(coalesce(v_total_charge_ex_vat, 0) - coalesce(v_old_total_charge_segments, 0), 2);

    -- Ensure breakdown.additional exists so future edits don't lose non-segment totals
    if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
      v_add_units := case
        when (v_breakdown->'additional' ? 'units') and jsonb_typeof(v_breakdown->'additional'->'units') = 'object'
          then v_breakdown->'additional'->'units'
        else '{}'::jsonb
      end;
    else
      v_add_units := '{}'::jsonb;
    end if;

    v_breakdown := jsonb_set(
      v_breakdown,
      '{additional}',
      jsonb_build_object(
        'units', v_add_units,
        'pay_ex_vat', v_add_pay,
        'charge_ex_vat', v_add_charge,
        'margin_ex_vat', round(v_add_charge - v_add_pay, 2)
      ),
      true
    );
  else
    v_add_pay := round(coalesce(v_add_pay, 0), 2);
    v_add_charge := round(coalesce(v_add_charge, 0), 2);
  end if;

  -- ✅ totals (pure ex-VAT totals; NO ERNI in totals)
  v_new_total_pay_segments := round(coalesce(v_new_total_pay_segments, 0), 2);
  v_new_total_charge_segments := round(coalesce(v_new_total_charge_segments, 0), 2);

  v_new_total_pay := round(v_new_total_pay_segments + coalesce(v_add_pay, 0), 2);
  v_new_total_charge := round(v_new_total_charge_segments + coalesce(v_add_charge, 0), 2);

  -- ✅ ERNI-aware margin recompute (PAYE only; wage pay only)
  if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
    v_policy := '{}'::jsonb;
  end if;

  v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

  v_erni_pct_raw := 0;
  begin
    v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
  exception when others then
    v_erni_pct_raw := 0;
  end;

  v_erni_mult := 1;
  if coalesce(v_erni_pct_raw,0) > 0 then
    if v_erni_pct_raw > 1 then
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    else
      v_erni_mult := 1 + v_erni_pct_raw;
    end if;
  end if;

  v_pay_method_u := upper(coalesce(nullif(btrim(coalesce(v_pay_method_text,'')), ''), ''));

  -- PAYE only (apply_erni_to never makes it apply to non-PAYE)
  v_erni_applies :=
    (v_pay_method_u = 'PAYE')
    and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

  -- Wage-like pay: segments pay + additional pay (NOT expenses/mileage)
  v_wage_pay := round(coalesce(v_new_total_pay_segments, 0) + coalesce(v_additional_pay_ex_vat, 0), 2);

  -- Reimbursements: expenses + mileage (NEVER ERNI)
  v_reimb_pay := round(coalesce(v_expenses_pay_ex_vat, 0) + coalesce(v_mileage_pay_ex_vat, 0), 2);

  v_wage_pay_cost := v_wage_pay;
  if v_erni_applies then
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  end if;

  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);

  v_new_margin := round(v_new_total_charge - v_pay_cost, 2);

  -- Update breakdown segments
  v_breakdown := jsonb_set(v_breakdown, '{segments}', v_out_segments, true);

  -- ✅ Update breakdown.additional.margin_ex_vat to be ERNI-accurate:
  -- Non-segment wage pay is additional_pay_ex_vat; reimbursements are expenses+mileage; ERNI applies only to PAYE wage pay.
  v_nonseg_wage_pay := round(coalesce(v_additional_pay_ex_vat, 0), 2);
  v_nonseg_reimb_pay := v_reimb_pay;

  v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
  if v_erni_applies then
    v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
  end if;

  v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
  v_nonseg_margin := round(coalesce(v_add_charge, 0) - v_nonseg_pay_cost, 2);

  if (v_breakdown ? 'additional') and jsonb_typeof(v_breakdown->'additional') = 'object' then
    v_breakdown := jsonb_set(v_breakdown, '{additional,margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  else
    v_breakdown := jsonb_set(
      v_breakdown,
      '{additional}',
      jsonb_build_object(
        'units', '{}'::jsonb,
        'pay_ex_vat', coalesce(v_add_pay, 0),
        'charge_ex_vat', coalesce(v_add_charge, 0),
        'margin_ex_vat', v_nonseg_margin
      ),
      true
    );
  end if;

  -- ✅ keep breakdown.totals in sync
  v_breakdown := jsonb_set(v_breakdown, '{totals,total_pay_ex_vat}', to_jsonb(v_new_total_pay), true);
  v_breakdown := jsonb_set(v_breakdown, '{totals,total_charge_ex_vat}', to_jsonb(v_new_total_charge), true);
  v_breakdown := jsonb_set(v_breakdown, '{totals,margin_ex_vat}', to_jsonb(v_new_margin), true);

  update public.timesheets_financials tfu
  set
    invoice_breakdown_json = v_breakdown,
    total_pay_ex_vat       = v_new_total_pay,
    total_charge_ex_vat    = v_new_total_charge,
    margin_ex_vat          = v_new_margin,
    updated_at             = v_now
  where tfu.id = v_tf_id
  returning to_jsonb(tfu) into v_updated_row;

  return jsonb_build_object('updated', v_updated_row);

end;
$$;




begin;

-- ============================================================
-- Trigger function: enforce overrideclientsettings rules on contracts
--
-- Behaviour:
-- 1) If overrideclientsettings = FALSE:
--    - clear all governed override fields to NULL (including default_submission_mode)
--
-- 2) If overrideclientsettings = TRUE:
--    - ensure all governed *boolean* fields are TRUE/FALSE (never NULL)
--      by filling NULLs from the most recent client_settings row for that client,
--      falling back to sensible defaults if no client_settings row exists.
--    - if send_manual_invoices_to_different_email = TRUE:
--        require manual_invoices_alt_email_address (fill from client_settings if possible,
--        otherwise raise an exception).
--
-- Notes:
-- - Picks client_settings row by (effective_from desc nulls last, updated_at desc).
-- - Safe to rerun: CREATE OR REPLACE FUNCTION + DROP TRIGGER IF EXISTS + CREATE TRIGGER
-- ============================================================

create or replace function public.contracts_enforce_overrideclientsettings()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  cs record;
begin
  -- Normalise NULL -> false (shouldn't happen because column is NOT NULL, but safe)
  new.overrideclientsettings := coalesce(new.overrideclientsettings, false);

  -- ------------------------------------------------------------
  -- If override is OFF: clear all governed override fields
  -- ------------------------------------------------------------
  if new.overrideclientsettings = false then
    -- nullable policy flags in contracts (must be NULL when override is off)
    new.no_timesheet_required   := null;
    new.daily_calc_of_invoices  := null;
    new.group_nightsat_sunbh    := null;
    new.is_nhsp                 := null;
    new.autoprocess_hr          := null;
    new.requires_hr             := null;
    new.hr_attach_to_invoice    := null;
    new.ts_attach_to_invoice    := null;

    -- new governed fields
    new.reference_number_required_to_issue_invoice := null;
    new.send_manual_invoices_to_different_email   := null;
    new.manual_invoices_alt_email_address         := null;

    -- default_submission_mode is governed; when override is off we store NULL (inherit)
    new.default_submission_mode := null;

    return new;
  end if;

  -- ------------------------------------------------------------
  -- If override is ON: fill any NULL booleans from client_settings
  -- ------------------------------------------------------------
  select
    cs.no_timesheet_required,
    cs.daily_calc_of_invoices,
    cs.group_nightsat_sunbh,
    cs.is_nhsp,
    cs.autoprocess_hr,
    cs.requires_hr,
    cs.hr_attach_to_invoice,
    cs.ts_attach_to_invoice,

    cs.reference_number_required_to_issue_invoice,
    cs.send_manual_invoices_to_different_email,
    cs.manual_invoices_alt_email_address
  into cs
  from public.client_settings cs
  where cs.client_id = new.client_id
  order by cs.effective_from desc nulls last, cs.updated_at desc
  limit 1;

  -- For booleans: never leave NULL when override is ON.
  -- If no client_settings row exists, fall back to defaults that match client_settings defaults.
  new.no_timesheet_required  := coalesce(new.no_timesheet_required,  cs.no_timesheet_required,  false);
  new.daily_calc_of_invoices := coalesce(new.daily_calc_of_invoices, cs.daily_calc_of_invoices, false);
  new.group_nightsat_sunbh   := coalesce(new.group_nightsat_sunbh,   cs.group_nightsat_sunbh,   false);
  new.is_nhsp                := coalesce(new.is_nhsp,                cs.is_nhsp,                false);
  new.autoprocess_hr         := coalesce(new.autoprocess_hr,         cs.autoprocess_hr,         false);
  new.requires_hr            := coalesce(new.requires_hr,            cs.requires_hr,            false);

  -- These default TRUE in client_settings
  new.hr_attach_to_invoice   := coalesce(new.hr_attach_to_invoice,   cs.hr_attach_to_invoice,   true);
  new.ts_attach_to_invoice   := coalesce(new.ts_attach_to_invoice,   cs.ts_attach_to_invoice,   true);

  -- New governed issue/email flags (default FALSE in client_settings)
  new.reference_number_required_to_issue_invoice :=
    coalesce(new.reference_number_required_to_issue_invoice, cs.reference_number_required_to_issue_invoice, false);

  new.send_manual_invoices_to_different_email :=
    coalesce(new.send_manual_invoices_to_different_email, cs.send_manual_invoices_to_different_email, false);

  -- If send_manual is TRUE, alt email must be present (try fill from client_settings first)
  if new.send_manual_invoices_to_different_email = true then
    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      new.manual_invoices_alt_email_address := cs.manual_invoices_alt_email_address;
    end if;

    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      raise exception 'manual_invoices_alt_email_address is required when send_manual_invoices_to_different_email is true';
    end if;

    -- Store trimmed version
    new.manual_invoices_alt_email_address := btrim(new.manual_invoices_alt_email_address);
  end if;

  -- default_submission_mode:
  -- You explicitly want contracts.default_submission_mode to be allowed NULL to inherit client settings.
  -- Therefore we DO NOT auto-fill it here.

  return new;
end;
$$;

drop trigger if exists trg_contracts_enforce_overrideclientsettings on public.contracts;

create trigger trg_contracts_enforce_overrideclientsettings
before insert or update on public.contracts
for each row
execute function public.contracts_enforce_overrideclientsettings();

commit;


CREATE OR REPLACE FUNCTION public.audit_events_list(
  p_search text DEFAULT NULL,
  p_action text DEFAULT NULL,
  p_object_type text DEFAULT NULL,
  p_actor_display text DEFAULT NULL,
  p_limit integer DEFAULT 50,
  p_offset integer DEFAULT 0,
  p_sort_by text DEFAULT 'ts_utc',
  p_sort_dir text DEFAULT 'desc'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_search text := NULLIF(btrim(p_search), '');
  v_action text := NULLIF(btrim(p_action), '');
  v_object_type text := NULLIF(btrim(p_object_type), '');
  v_actor_display text := NULLIF(btrim(p_actor_display), '');
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 500);
  v_offset integer := GREATEST(COALESCE(p_offset, 0), 0);
  v_sort_by text := lower(COALESCE(NULLIF(btrim(p_sort_by), ''), 'ts_utc'));
  v_sort_dir text := lower(COALESCE(NULLIF(btrim(p_sort_dir), ''), 'desc'));
  v_items jsonb := '[]'::jsonb;
  v_total bigint := 0;
BEGIN
  IF v_sort_by NOT IN ('ts_utc', 'action', 'actor_display', 'object_type', 'object_id_text', 'correlation_id') THEN
    v_sort_by := 'ts_utc';
  END IF;

  IF v_sort_dir NOT IN ('asc', 'desc') THEN
    v_sort_dir := 'desc';
  END IF;

  WITH filtered AS (
    SELECT
      ae.id,
      ae.ts_utc,
      ae.actor_user_id,
      ae.actor_display,
      ae.actor_role_at_time,
      ae.object_type,
      ae.object_id_text,
      ae.action,
      ae.before_json,
      ae.after_json,
      ae.reason,
      ae.ip,
      ae.user_agent,
      ae.correlation_id
    FROM public.audit_events AS ae
    WHERE
      (
        v_search IS NULL
        OR concat_ws(
          ' ',
          COALESCE(ae.actor_display, ''),
          COALESCE(ae.actor_role_at_time, ''),
          COALESCE(ae.object_type, ''),
          COALESCE(ae.object_id_text, ''),
          COALESCE(ae.action, ''),
          COALESCE(ae.reason, ''),
          COALESCE(ae.correlation_id, '')
        ) ILIKE ('%' || v_search || '%')
      )
      AND (
        v_action IS NULL
        OR upper(COALESCE(ae.action, '')) = upper(v_action)
      )
      AND (
        v_object_type IS NULL
        OR upper(COALESCE(ae.object_type, '')) = upper(v_object_type)
      )
      AND (
        v_actor_display IS NULL
        OR COALESCE(ae.actor_display, '') ILIKE ('%' || v_actor_display || '%')
      )
  ),
  page_rows AS (
    SELECT
      f.id,
      f.ts_utc,
      f.actor_user_id,
      f.actor_display,
      f.actor_role_at_time,
      f.object_type,
      f.object_id_text,
      f.action,
      f.before_json,
      f.after_json,
      f.reason,
      f.ip,
      f.user_agent,
      f.correlation_id
    FROM filtered AS f
    ORDER BY
      CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'asc' THEN f.ts_utc END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'desc' THEN f.ts_utc END DESC NULLS LAST,
      CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'asc' THEN f.action END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'desc' THEN f.action END DESC NULLS LAST,
      CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'asc' THEN f.actor_display END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'desc' THEN f.actor_display END DESC NULLS LAST,
      CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'asc' THEN f.object_type END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'desc' THEN f.object_type END DESC NULLS LAST,
      CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'asc' THEN f.object_id_text END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'desc' THEN f.object_id_text END DESC NULLS LAST,
      CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'asc' THEN f.correlation_id END ASC NULLS LAST,
      CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'desc' THEN f.correlation_id END DESC NULLS LAST,
      f.ts_utc DESC,
      f.id DESC
    LIMIT v_limit
    OFFSET v_offset
  )
  SELECT count(*) INTO v_total
  FROM filtered;

  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'id', pr.id,
        'ts_utc', pr.ts_utc,
        'actor_user_id', pr.actor_user_id,
        'actor_display', pr.actor_display,
        'actor_role_at_time', pr.actor_role_at_time,
        'object_type', pr.object_type,
        'object_id_text', pr.object_id_text,
        'action', pr.action,
        'before_json', pr.before_json,
        'after_json', pr.after_json,
        'reason', pr.reason,
        'ip', pr.ip,
        'user_agent', pr.user_agent,
        'correlation_id', pr.correlation_id
      )
    ),
    '[]'::jsonb
  )
  INTO v_items
  FROM page_rows AS pr;

  RETURN jsonb_build_object(
    'ok', true,
    'items', v_items,
    'total_count', v_total,
    'limit', v_limit,
    'offset', v_offset
  );
END;
$$;




create or replace function public.summary_typeahead_lookup(
  p_section text,
  p_filters jsonb default '{}'::jsonb,
  p_sort_key text default null,
  p_sort_dir text default 'asc',
  p_prefix text default null,
  p_page_size integer default null
)
returns table(
  row_id text,
  ordinal_index bigint,
  target_page integer,
  matched_value text,
  dataset_key text
)
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_section text := lower(btrim(coalesce(p_section, '')));
  v_sort_key text := lower(btrim(coalesce(p_sort_key, '')));
  v_sort_dir text := case when lower(btrim(coalesce(p_sort_dir, ''))) = 'desc' then 'desc' else 'asc' end;
  v_prefix text := lower(btrim(coalesce(p_prefix, '')));
  v_prefix_like text := null;
  v_dataset_key text := md5(jsonb_build_object('section', lower(btrim(coalesce(p_section, ''))), 'filters', coalesce(p_filters, '{}'::jsonb))::text);
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  v_ids text[] := null;
  v_q text := null;
  v_active boolean := null;
  v_enabled boolean := null;
  v_client_id uuid := null;
  v_candidate_id uuid := null;
  v_route_type text := null;
  v_sheet_scope text := null;
  v_issues_filter text := null;
  v_contract_status text := null;
  v_role text := null;
  v_band text := null;
  v_status_list text[] := null;
  v_issued_from date := null;
  v_issued_to date := null;
  v_week_ending_from date := null;
  v_week_ending_to date := null;
  v_job_title_include_node_ids text[] := null;
  v_job_title_exclude_node_ids text[] := null;
  v_job_title_role_ids text[] := null;
  v_job_title_primary_only boolean := false;
  v_candidate_filters jsonb := '{}'::jsonb;
  v_candidate_dataset_key text := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  if v_section = '' or v_sort_key = '' or v_prefix = '' then
    return;
  end if;

  v_prefix_like :=
    replace(
      replace(
        replace(v_prefix, E'\\', E'\\\\'),
        '%',
        E'\\%'
      ),
      '_',
      E'\\_'
    ) || '%';

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(s.val order by s.val)
        into v_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'ids') as e(value)
        ) as s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(s.val order by s.val)
        into v_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
        ) as s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  v_q := nullif(
    btrim(
      coalesce(
        p_filters->>'q',
        p_filters->>'name',
        ''
      )
    ),
    ''
  );

  if lower(coalesce(p_filters->>'active', '')) in ('true', 'false') then
    v_active := (lower(p_filters->>'active') = 'true');
  end if;

  if lower(coalesce(p_filters->>'enabled', '')) in ('true', 'false') then
    v_enabled := (lower(p_filters->>'enabled') = 'true');
  end if;

  begin
    if nullif(btrim(coalesce(p_filters->>'client_id', '')), '') is not null then
      v_client_id := (p_filters->>'client_id')::uuid;
    end if;
  exception when others then
    v_client_id := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'candidate_id', '')), '') is not null then
      v_candidate_id := (p_filters->>'candidate_id')::uuid;
    end if;
  exception when others then
    v_candidate_id := null;
  end;

  v_route_type := upper(nullif(btrim(coalesce(p_filters->>'route_type', '')), ''));
  if v_route_type = 'ALL' then v_route_type := null; end if;

  v_sheet_scope := upper(nullif(btrim(coalesce(p_filters->>'sheet_scope', '')), ''));
  if v_sheet_scope = 'ALL' then v_sheet_scope := null; end if;

  v_issues_filter := upper(nullif(btrim(coalesce(p_filters->>'issues_filter', '')), ''));
  if v_issues_filter = 'ALL' then v_issues_filter := null; end if;

  v_contract_status := upper(nullif(btrim(coalesce(p_filters->>'status', '')), ''));
  if v_contract_status = 'ALL' then v_contract_status := null; end if;

  v_role := nullif(btrim(coalesce(p_filters->>'role', '')), '');
  v_band := nullif(btrim(coalesce(p_filters->>'band', '')), '');

  begin
    if p_filters ? 'status' then
      if jsonb_typeof(p_filters->'status') = 'array' then
        select array_agg(s.val order by s.val)
        into v_status_list
        from (
          select distinct upper(nullif(btrim(e.value), '')) as val
          from jsonb_array_elements_text(p_filters->'status') as e(value)
        ) as s
        where s.val is not null
          and s.val <> 'ALL';
      elsif nullif(btrim(coalesce(p_filters->>'status', '')), '') is not null then
        select array_agg(s.val order by s.val)
        into v_status_list
        from (
          select distinct upper(nullif(btrim(x), '')) as val
          from unnest(regexp_split_to_array(p_filters->>'status', '\s*,\s*')) as u(x)
        ) as s
        where s.val is not null
          and s.val <> 'ALL';
      end if;
    end if;
  exception when others then
    v_status_list := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'issued_from', '')), '') is not null then
      v_issued_from := (p_filters->>'issued_from')::date;
    end if;
  exception when others then
    v_issued_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'issued_to', '')), '') is not null then
      v_issued_to := (p_filters->>'issued_to')::date;
    end if;
  exception when others then
    v_issued_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_from', '')), '') is not null then
      v_week_ending_from := (p_filters->>'week_ending_from')::date;
    end if;
  exception when others then
    v_week_ending_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_to', '')), '') is not null then
      v_week_ending_to := (p_filters->>'week_ending_to')::date;
    end if;
  exception when others then
    v_week_ending_to := null;
  end;

  begin
    if p_filters ? 'job_title_include_node_ids' then
      if jsonb_typeof(p_filters->'job_title_include_node_ids') = 'array' then
        select array_agg(s.val order by s.val)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'job_title_include_node_ids') as e(value)
        ) as s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_include_node_ids', '')), '') is not null then
        select array_agg(s.val order by s.val)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'job_title_include_node_ids', '\s*,\s*')) as u(x)
        ) as s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_job_title_include_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_exclude_node_ids' then
      if jsonb_typeof(p_filters->'job_title_exclude_node_ids') = 'array' then
        select array_agg(s.val order by s.val)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'job_title_exclude_node_ids') as e(value)
        ) as s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_exclude_node_ids', '')), '') is not null then
        select array_agg(s.val order by s.val)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'job_title_exclude_node_ids', '\s*,\s*')) as u(x)
        ) as s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_job_title_exclude_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_role_ids' then
      if jsonb_typeof(p_filters->'job_title_role_ids') = 'array' then
        select array_agg(s.val order by s.val)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'job_title_role_ids') as e(value)
        ) as s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_role_ids', '')), '') is not null then
        select array_agg(s.val order by s.val)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'job_title_role_ids', '\s*,\s*')) as u(x)
        ) as s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_job_title_role_ids := null;
  end;

  if lower(coalesce(p_filters->>'job_title_primary_only', '')) in ('true', 'false') then
    v_job_title_primary_only := (lower(p_filters->>'job_title_primary_only') = 'true');
  else
    v_job_title_primary_only := false;
  end if;

  if v_section = 'candidates' then
    v_candidate_filters := coalesce(p_filters, '{}'::jsonb);

    v_candidate_dataset_key :=
      md5(
        jsonb_build_object(
          'section', v_section,
          'filters', v_candidate_filters
        )::text
      );

    return query
    with candidate_ids as (
      select candidate_list_ids_row.id as candidate_id
      from public.candidate_list_ids(v_candidate_filters) as candidate_list_ids_row(id)
    ),
    filtered_rows as (
      select
        csa.id::text as row_id_text,
        case
          when v_sort_key = 'first_name' then coalesce(btrim(csa.first_name), '')
          when v_sort_key = 'last_name' then coalesce(btrim(csa.last_name), '')
          when v_sort_key = 'display_name' then coalesce(btrim(csa.display_name), '')
          when v_sort_key = 'email' then coalesce(btrim(csa.email), '')
          when v_sort_key = 'phone' then coalesce(btrim(csa.phone), '')
          when v_sort_key = 'tms_ref' then coalesce(btrim(csa.tms_ref), '')
          when v_sort_key = '__tms_ref' then coalesce(btrim(csa.tms_ref), '')
          when v_sort_key = 'job_titles_display' then coalesce(btrim(secsort.secondary_job_titles_sort_text), '')
          when v_sort_key = 'primary_job_title' then coalesce(btrim(csa.primary_job_title), '')
          when v_sort_key = 'pay_method' then coalesce(btrim(csa.pay_method), '')
          when v_sort_key = 'postcode' then coalesce(btrim(csa.postcode), '')
          when v_sort_key = 'town_city' then coalesce(btrim(csa.town_city), '')
          when v_sort_key = 'umbrella_name' then coalesce(btrim(csa.umbrella_name), '')
          when v_sort_key = 'created_at' then coalesce(csa.created_at::text, '')
          when v_sort_key = 'updated_at' then coalesce(csa.updated_at::text, '')
          when v_sort_key = 'active' then case when csa.active then 'true' else 'false' end
          else coalesce(btrim(csa.last_name), coalesce(btrim(csa.display_name), ''))
        end as sort_value_text,
        case
          when v_sort_key = 'first_name' then coalesce(btrim(csa.first_name), '')
          when v_sort_key = 'last_name' then coalesce(btrim(csa.last_name), '')
          when v_sort_key = 'display_name' then coalesce(btrim(csa.display_name), '')
          when v_sort_key = 'email' then coalesce(btrim(csa.email), '')
          when v_sort_key = 'phone' then coalesce(btrim(csa.phone), '')
          when v_sort_key = 'tms_ref' then coalesce(btrim(csa.tms_ref), '')
          when v_sort_key = '__tms_ref' then coalesce(btrim(csa.tms_ref), '')
          when v_sort_key = 'job_titles_display' then coalesce(btrim(secsort.secondary_job_titles_sort_text), '')
          when v_sort_key = 'primary_job_title' then coalesce(btrim(csa.primary_job_title), '')
          when v_sort_key = 'pay_method' then coalesce(btrim(csa.pay_method), '')
          when v_sort_key = 'postcode' then coalesce(btrim(csa.postcode), '')
          when v_sort_key = 'town_city' then coalesce(btrim(csa.town_city), '')
          when v_sort_key = 'umbrella_name' then coalesce(btrim(csa.umbrella_name), '')
          when v_sort_key = 'created_at' then coalesce(csa.created_at::text, '')
          when v_sort_key = 'updated_at' then coalesce(csa.updated_at::text, '')
          when v_sort_key = 'active' then case when csa.active then 'true' else 'false' end
          else coalesce(btrim(csa.last_name), coalesce(btrim(csa.display_name), ''))
        end as matched_value_output_text,
        csa.first_name,
        csa.last_name,
        csa.display_name,
        csa.email,
        csa.phone,
        csa.tms_ref,
        csa.tms_ref_num,
        csa.job_titles_display,
        csa.primary_job_title,
        csa.primary_job_title_id,
        csa.job_title_ids,
        coalesce(secsort.secondary_job_titles_sort_text, '') as secondary_job_titles_sort_text,
        csa.pay_method,
        csa.postcode,
        csa.town_city,
        csa.umbrella_name,
        csa.created_at,
        csa.updated_at,
        csa.active
      from public.candidates_summary_activity as csa
      join candidate_ids as candidate_ids_row
        on candidate_ids_row.candidate_id = csa.id
      left join lateral (
        select string_agg(sec.part, '; ' order by lower(sec.part), sec.part) as secondary_job_titles_sort_text
        from (
          select distinct btrim(split_part_item.part) as part
          from regexp_split_to_table(coalesce(csa.job_titles_display, ''), '\s*;\s*') as split_part_item(part)
          where nullif(btrim(split_part_item.part), '') is not null
            and (
              nullif(btrim(coalesce(csa.primary_job_title, '')), '') is null
              or lower(btrim(split_part_item.part)) <> lower(btrim(coalesce(csa.primary_job_title, '')))
            )
        ) as sec
      ) as secsort
        on true
    ),
    ranked_rows as (
      select
        filtered_rows.row_id_text,
        filtered_rows.sort_value_text,
        filtered_rows.matched_value_output_text,
        row_number() over (
          order by
            case when v_sort_key = 'created_at' and v_sort_dir = 'asc' then filtered_rows.created_at end asc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'desc' then filtered_rows.created_at end desc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'asc' then filtered_rows.updated_at end asc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'desc' then filtered_rows.updated_at end desc nulls last,

            case when v_sort_key = 'active' and v_sort_dir = 'asc' then case when filtered_rows.active then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'active' and v_sort_dir = 'desc' then case when filtered_rows.active then 1 else 0 end end desc nulls last,

            case when v_sort_key = '__tms_ref' and v_sort_dir = 'asc' then filtered_rows.tms_ref_num end asc nulls last,
            case when v_sort_key = '__tms_ref' and v_sort_dir = 'desc' then filtered_rows.tms_ref_num end desc nulls last,
            case when v_sort_key = '__tms_ref' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.tms_ref), '')), '') end asc nulls last,
            case when v_sort_key = '__tms_ref' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.tms_ref), '')), '') end desc nulls last,

            case when v_sort_key = 'first_name' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.first_name), '')), '') end asc nulls last,
            case when v_sort_key = 'first_name' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.first_name), '')), '') end desc nulls last,
            case when v_sort_key = 'last_name' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.last_name), '')), '') end asc nulls last,
            case when v_sort_key = 'last_name' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.last_name), '')), '') end desc nulls last,
            case when v_sort_key = 'display_name' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.display_name), '')), '') end asc nulls last,
            case when v_sort_key = 'display_name' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.display_name), '')), '') end desc nulls last,
            case when v_sort_key = 'email' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.email), '')), '') end asc nulls last,
            case when v_sort_key = 'email' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.email), '')), '') end desc nulls last,
            case when v_sort_key = 'phone' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.phone), '')), '') end asc nulls last,
            case when v_sort_key = 'phone' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.phone), '')), '') end desc nulls last,
            case when v_sort_key = 'tms_ref' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.tms_ref), '')), '') end asc nulls last,
            case when v_sort_key = 'tms_ref' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.tms_ref), '')), '') end desc nulls last,
            case when v_sort_key = 'job_titles_display' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.secondary_job_titles_sort_text), '')), '') end asc nulls last,
            case when v_sort_key = 'job_titles_display' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.secondary_job_titles_sort_text), '')), '') end desc nulls last,
            case when v_sort_key = 'primary_job_title' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.primary_job_title), '')), '') end asc nulls last,
            case when v_sort_key = 'primary_job_title' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.primary_job_title), '')), '') end desc nulls last,
            case when v_sort_key = 'pay_method' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.pay_method), '')), '') end asc nulls last,
            case when v_sort_key = 'pay_method' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.pay_method), '')), '') end desc nulls last,
            case when v_sort_key = 'postcode' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.postcode), '')), '') end asc nulls last,
            case when v_sort_key = 'postcode' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.postcode), '')), '') end desc nulls last,
            case when v_sort_key = 'town_city' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.town_city), '')), '') end asc nulls last,
            case when v_sort_key = 'town_city' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.town_city), '')), '') end desc nulls last,
            case when v_sort_key = 'umbrella_name' and v_sort_dir = 'asc' then nullif(lower(coalesce(btrim(filtered_rows.umbrella_name), '')), '') end asc nulls last,
            case when v_sort_key = 'umbrella_name' and v_sort_dir = 'desc' then nullif(lower(coalesce(btrim(filtered_rows.umbrella_name), '')), '') end desc nulls last,

            case when v_sort_key in ('job_titles_display', 'primary_job_title') then nullif(lower(coalesce(btrim(filtered_rows.last_name), '')), '') end asc nulls last,
            case when v_sort_key in ('job_titles_display', 'primary_job_title') then nullif(lower(coalesce(btrim(filtered_rows.first_name), '')), '') end asc nulls last,
            case when v_sort_key in ('job_titles_display', 'primary_job_title') then nullif(lower(coalesce(btrim(filtered_rows.display_name), '')), '') end asc nulls last,

            filtered_rows.row_id_text asc
        ) - 1 as rn
      from filtered_rows
    ),
    prefixed_rows as (
      select
        ranked_rows.row_id_text,
        ranked_rows.matched_value_output_text,
        ranked_rows.rn
      from ranked_rows
      where lower(coalesce(ranked_rows.sort_value_text, '')) like v_prefix_like escape '\'
    )
    select
      prefixed_rows.row_id_text as row_id,
      prefixed_rows.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((prefixed_rows.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      prefixed_rows.matched_value_output_text as matched_value,
      coalesce(v_candidate_dataset_key, v_dataset_key) as dataset_key
    from prefixed_rows
    order by prefixed_rows.rn
    limit 1;

    return;
  end if;

  if v_section = 'clients' then
    return query
    with filtered_rows as (
      select
        cli.id::text as row_id_text,
        case
          when v_sort_key = 'cli_ref' then coalesce(cli.cli_ref, '')
          when v_sort_key = 'name' then coalesce(cli.name, '')
          when v_sort_key = 'invoice_address' then coalesce(cli.invoice_address, '')
          when v_sort_key = 'primary_invoice_email' then coalesce(cli.primary_invoice_email, '')
          when v_sort_key = 'ap_phone' then coalesce(cli.ap_phone, '')
          when v_sort_key = 'vat_chargeable' then case when cli.vat_chargeable then 'true' else 'false' end
          when v_sort_key = 'payment_terms_days' then coalesce(cli.payment_terms_days::text, '')
          when v_sort_key = 'mileage_charge_rate' then coalesce(cli.mileage_charge_rate::text, '')
          when v_sort_key = 'ts_queries_email' then coalesce(cli.ts_queries_email, '')
          when v_sort_key = 'created_at' then coalesce(cli.created_at::text, '')
          when v_sort_key = 'updated_at' then coalesce(cli.updated_at::text, '')
          when v_sort_key = 'contact_title' then coalesce(cli.contact_title, '')
          when v_sort_key = 'contact_known_as' then coalesce(cli.contact_known_as, '')
          when v_sort_key = 'contact_forename' then coalesce(cli.contact_forename, '')
          when v_sort_key = 'contact_surname' then coalesce(cli.contact_surname, '')
          when v_sort_key = 'contact_job_title' then coalesce(cli.contact_job_title, '')
          when v_sort_key = 'contact_tel' then coalesce(cli.contact_tel, '')
          when v_sort_key = 'contact_mobile' then coalesce(cli.contact_mobile, '')
          when v_sort_key = 'contact_email' then coalesce(cli.contact_email, '')
          when v_sort_key = 'website' then coalesce(cli.website, '')
          when v_sort_key = 'notes' then coalesce(cli.notes, '')
          when v_sort_key = 'rev' then coalesce(cli.rev::text, '')
          else coalesce(cli.name, '')
        end as matched_value_text,
        cli.cli_ref,
        cli.name,
        cli.invoice_address,
        cli.primary_invoice_email,
        cli.ap_phone,
        cli.vat_chargeable,
        cli.payment_terms_days,
        cli.mileage_charge_rate,
        cli.ts_queries_email,
        cli.created_at,
        cli.updated_at,
        cli.contact_title,
        cli.contact_known_as,
        cli.contact_forename,
        cli.contact_surname,
        cli.contact_job_title,
        cli.contact_tel,
        cli.contact_mobile,
        cli.contact_email,
        cli.website,
        cli.notes,
        cli.rev
      from public.clients as cli
      where (v_ids is null or cli.id::text = any(v_ids))
        and (v_q is null or (
          coalesce(cli.name, '') ilike ('%' || v_q || '%')
          or coalesce(cli.cli_ref, '') ilike ('%' || v_q || '%')
          or coalesce(cli.primary_invoice_email, '') ilike ('%' || v_q || '%')
          or coalesce(cli.invoice_address, '') ilike ('%' || v_q || '%')
          or coalesce(cli.ap_phone, '') ilike ('%' || v_q || '%')
          or coalesce(cli.contact_forename, '') ilike ('%' || v_q || '%')
          or coalesce(cli.contact_surname, '') ilike ('%' || v_q || '%')
          or coalesce(cli.contact_email, '') ilike ('%' || v_q || '%')
        ))
    ),
    ranked_rows as (
      select
        fr.row_id_text,
        fr.matched_value_text,
        row_number() over (
          order by
            case when v_sort_key = 'created_at' and v_sort_dir = 'asc' then fr.created_at end asc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'desc' then fr.created_at end desc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'asc' then fr.updated_at end asc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'desc' then fr.updated_at end desc nulls last,
            case when v_sort_key = 'payment_terms_days' and v_sort_dir = 'asc' then fr.payment_terms_days end asc nulls last,
            case when v_sort_key = 'payment_terms_days' and v_sort_dir = 'desc' then fr.payment_terms_days end desc nulls last,
            case when v_sort_key = 'mileage_charge_rate' and v_sort_dir = 'asc' then fr.mileage_charge_rate end asc nulls last,
            case when v_sort_key = 'mileage_charge_rate' and v_sort_dir = 'desc' then fr.mileage_charge_rate end desc nulls last,
            case when v_sort_key = 'rev' and v_sort_dir = 'asc' then fr.rev end asc nulls last,
            case when v_sort_key = 'rev' and v_sort_dir = 'desc' then fr.rev end desc nulls last,
            case when v_sort_key = 'vat_chargeable' and v_sort_dir = 'asc' then case when fr.vat_chargeable then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'vat_chargeable' and v_sort_dir = 'desc' then case when fr.vat_chargeable then 1 else 0 end end desc nulls last,

            case when v_sort_key = 'cli_ref' and v_sort_dir = 'asc' then lower(coalesce(fr.cli_ref, '')) end asc nulls last,
            case when v_sort_key = 'cli_ref' and v_sort_dir = 'desc' then lower(coalesce(fr.cli_ref, '')) end desc nulls last,
            case when v_sort_key = 'name' and v_sort_dir = 'asc' then lower(coalesce(fr.name, '')) end asc nulls last,
            case when v_sort_key = 'name' and v_sort_dir = 'desc' then lower(coalesce(fr.name, '')) end desc nulls last,
            case when v_sort_key = 'invoice_address' and v_sort_dir = 'asc' then lower(coalesce(fr.invoice_address, '')) end asc nulls last,
            case when v_sort_key = 'invoice_address' and v_sort_dir = 'desc' then lower(coalesce(fr.invoice_address, '')) end desc nulls last,
            case when v_sort_key = 'primary_invoice_email' and v_sort_dir = 'asc' then lower(coalesce(fr.primary_invoice_email, '')) end asc nulls last,
            case when v_sort_key = 'primary_invoice_email' and v_sort_dir = 'desc' then lower(coalesce(fr.primary_invoice_email, '')) end desc nulls last,
            case when v_sort_key = 'ap_phone' and v_sort_dir = 'asc' then lower(coalesce(fr.ap_phone, '')) end asc nulls last,
            case when v_sort_key = 'ap_phone' and v_sort_dir = 'desc' then lower(coalesce(fr.ap_phone, '')) end desc nulls last,
            case when v_sort_key = 'ts_queries_email' and v_sort_dir = 'asc' then lower(coalesce(fr.ts_queries_email, '')) end asc nulls last,
            case when v_sort_key = 'ts_queries_email' and v_sort_dir = 'desc' then lower(coalesce(fr.ts_queries_email, '')) end desc nulls last,
            case when v_sort_key = 'contact_title' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_title, '')) end asc nulls last,
            case when v_sort_key = 'contact_title' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_title, '')) end desc nulls last,
            case when v_sort_key = 'contact_known_as' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_known_as, '')) end asc nulls last,
            case when v_sort_key = 'contact_known_as' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_known_as, '')) end desc nulls last,
            case when v_sort_key = 'contact_forename' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_forename, '')) end asc nulls last,
            case when v_sort_key = 'contact_forename' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_forename, '')) end desc nulls last,
            case when v_sort_key = 'contact_surname' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_surname, '')) end asc nulls last,
            case when v_sort_key = 'contact_surname' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_surname, '')) end desc nulls last,
            case when v_sort_key = 'contact_job_title' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_job_title, '')) end asc nulls last,
            case when v_sort_key = 'contact_job_title' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_job_title, '')) end desc nulls last,
            case when v_sort_key = 'contact_tel' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_tel, '')) end asc nulls last,
            case when v_sort_key = 'contact_tel' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_tel, '')) end desc nulls last,
            case when v_sort_key = 'contact_mobile' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_mobile, '')) end asc nulls last,
            case when v_sort_key = 'contact_mobile' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_mobile, '')) end desc nulls last,
            case when v_sort_key = 'contact_email' and v_sort_dir = 'asc' then lower(coalesce(fr.contact_email, '')) end asc nulls last,
            case when v_sort_key = 'contact_email' and v_sort_dir = 'desc' then lower(coalesce(fr.contact_email, '')) end desc nulls last,
            case when v_sort_key = 'website' and v_sort_dir = 'asc' then lower(coalesce(fr.website, '')) end asc nulls last,
            case when v_sort_key = 'website' and v_sort_dir = 'desc' then lower(coalesce(fr.website, '')) end desc nulls last,
            case when v_sort_key = 'notes' and v_sort_dir = 'asc' then lower(coalesce(fr.notes, '')) end asc nulls last,
            case when v_sort_key = 'notes' and v_sort_dir = 'desc' then lower(coalesce(fr.notes, '')) end desc nulls last,

            case when v_sort_dir = 'asc' then lower(coalesce(fr.matched_value_text, '')) end asc nulls last,
            case when v_sort_dir = 'desc' then lower(coalesce(fr.matched_value_text, '')) end desc nulls last,
            fr.row_id_text asc
        ) - 1 as rn
      from filtered_rows as fr
    ),
    prefixed_rows as (
      select
        rr.row_id_text,
        rr.matched_value_text,
        rr.rn
      from ranked_rows as rr
      where lower(coalesce(rr.matched_value_text, '')) like v_prefix_like escape '\'
    )
    select
      pr.row_id_text as row_id,
      pr.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((pr.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      pr.matched_value_text as matched_value,
      v_dataset_key as dataset_key
    from prefixed_rows as pr
    order by pr.rn
    limit 1;

    return;
  end if;

  if v_section = 'umbrellas' then
    return query
    with filtered_rows as (
      select
        umb.id::text as row_id_text,
        case
          when v_sort_key = 'name' then coalesce(umb.name, '')
          when v_sort_key = 'email' then coalesce(umb.remittance_email, '')
          when v_sort_key = 'remittance_email' then coalesce(umb.remittance_email, '')
          when v_sort_key = 'bank_name' then coalesce(umb.bank_name, '')
          when v_sort_key = 'sort_code' then coalesce(umb.sort_code, '')
          when v_sort_key = 'account_number' then coalesce(umb.account_number, '')
          when v_sort_key = 'vat_chargeable' then case when umb.vat_chargeable then 'true' else 'false' end
          when v_sort_key = 'active' then case when umb.enabled then 'true' else 'false' end
          when v_sort_key = 'enabled' then case when umb.enabled then 'true' else 'false' end
          when v_sort_key = 'created_at' then coalesce(umb.created_at::text, '')
          when v_sort_key = 'updated_at' then coalesce(umb.updated_at::text, '')
          when v_sort_key = 'company_number' then coalesce(umb.company_number, '')
          when v_sort_key = 'address_line1' then coalesce(umb.address_line1, '')
          when v_sort_key = 'address_line2' then coalesce(umb.address_line2, '')
          when v_sort_key = 'address_line3' then coalesce(umb.address_line3, '')
          when v_sort_key = 'town_city' then coalesce(umb.town_city, '')
          when v_sort_key = 'county' then coalesce(umb.county, '')
          when v_sort_key = 'postcode' then coalesce(umb.postcode, '')
          when v_sort_key = 'country' then coalesce(umb.country, '')
          else coalesce(umb.name, '')
        end as matched_value_text,
        umb.name,
        umb.remittance_email,
        umb.bank_name,
        umb.sort_code,
        umb.account_number,
        umb.vat_chargeable,
        umb.enabled,
        umb.created_at,
        umb.updated_at,
        umb.company_number,
        umb.address_line1,
        umb.address_line2,
        umb.address_line3,
        umb.town_city,
        umb.county,
        umb.postcode,
        umb.country
      from public.umbrellas as umb
      where (v_ids is null or umb.id::text = any(v_ids))
        and (v_q is null or (
          coalesce(umb.name, '') ilike ('%' || v_q || '%')
          or coalesce(umb.remittance_email, '') ilike ('%' || v_q || '%')
          or coalesce(umb.bank_name, '') ilike ('%' || v_q || '%')
          or coalesce(umb.sort_code, '') ilike ('%' || v_q || '%')
          or coalesce(umb.account_number, '') ilike ('%' || v_q || '%')
          or coalesce(umb.company_number, '') ilike ('%' || v_q || '%')
        ))
        and (v_enabled is null or umb.enabled is not distinct from v_enabled)
    ),
    ranked_rows as (
      select
        fr.row_id_text,
        fr.matched_value_text,
        row_number() over (
          order by
            case when v_sort_key = 'created_at' and v_sort_dir = 'asc' then fr.created_at end asc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'desc' then fr.created_at end desc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'asc' then fr.updated_at end asc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'desc' then fr.updated_at end desc nulls last,
            case when v_sort_key = 'vat_chargeable' and v_sort_dir = 'asc' then case when fr.vat_chargeable then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'vat_chargeable' and v_sort_dir = 'desc' then case when fr.vat_chargeable then 1 else 0 end end desc nulls last,
            case when (v_sort_key = 'enabled' or v_sort_key = 'active') and v_sort_dir = 'asc' then case when fr.enabled then 1 else 0 end end asc nulls last,
            case when (v_sort_key = 'enabled' or v_sort_key = 'active') and v_sort_dir = 'desc' then case when fr.enabled then 1 else 0 end end desc nulls last,

            case when v_sort_key = 'name' and v_sort_dir = 'asc' then lower(coalesce(fr.name, '')) end asc nulls last,
            case when v_sort_key = 'name' and v_sort_dir = 'desc' then lower(coalesce(fr.name, '')) end desc nulls last,
            case when (v_sort_key = 'email' or v_sort_key = 'remittance_email') and v_sort_dir = 'asc' then lower(coalesce(fr.remittance_email, '')) end asc nulls last,
            case when (v_sort_key = 'email' or v_sort_key = 'remittance_email') and v_sort_dir = 'desc' then lower(coalesce(fr.remittance_email, '')) end desc nulls last,
            case when v_sort_key = 'bank_name' and v_sort_dir = 'asc' then lower(coalesce(fr.bank_name, '')) end asc nulls last,
            case when v_sort_key = 'bank_name' and v_sort_dir = 'desc' then lower(coalesce(fr.bank_name, '')) end desc nulls last,
            case when v_sort_key = 'sort_code' and v_sort_dir = 'asc' then lower(coalesce(fr.sort_code, '')) end asc nulls last,
            case when v_sort_key = 'sort_code' and v_sort_dir = 'desc' then lower(coalesce(fr.sort_code, '')) end desc nulls last,
            case when v_sort_key = 'account_number' and v_sort_dir = 'asc' then lower(coalesce(fr.account_number, '')) end asc nulls last,
            case when v_sort_key = 'account_number' and v_sort_dir = 'desc' then lower(coalesce(fr.account_number, '')) end desc nulls last,
            case when v_sort_key = 'company_number' and v_sort_dir = 'asc' then lower(coalesce(fr.company_number, '')) end asc nulls last,
            case when v_sort_key = 'company_number' and v_sort_dir = 'desc' then lower(coalesce(fr.company_number, '')) end desc nulls last,
            case when v_sort_key = 'address_line1' and v_sort_dir = 'asc' then lower(coalesce(fr.address_line1, '')) end asc nulls last,
            case when v_sort_key = 'address_line1' and v_sort_dir = 'desc' then lower(coalesce(fr.address_line1, '')) end desc nulls last,
            case when v_sort_key = 'address_line2' and v_sort_dir = 'asc' then lower(coalesce(fr.address_line2, '')) end asc nulls last,
            case when v_sort_key = 'address_line2' and v_sort_dir = 'desc' then lower(coalesce(fr.address_line2, '')) end desc nulls last,
            case when v_sort_key = 'address_line3' and v_sort_dir = 'asc' then lower(coalesce(fr.address_line3, '')) end asc nulls last,
            case when v_sort_key = 'address_line3' and v_sort_dir = 'desc' then lower(coalesce(fr.address_line3, '')) end desc nulls last,
            case when v_sort_key = 'town_city' and v_sort_dir = 'asc' then lower(coalesce(fr.town_city, '')) end asc nulls last,
            case when v_sort_key = 'town_city' and v_sort_dir = 'desc' then lower(coalesce(fr.town_city, '')) end desc nulls last,
            case when v_sort_key = 'county' and v_sort_dir = 'asc' then lower(coalesce(fr.county, '')) end asc nulls last,
            case when v_sort_key = 'county' and v_sort_dir = 'desc' then lower(coalesce(fr.county, '')) end desc nulls last,
            case when v_sort_key = 'postcode' and v_sort_dir = 'asc' then lower(coalesce(fr.postcode, '')) end asc nulls last,
            case when v_sort_key = 'postcode' and v_sort_dir = 'desc' then lower(coalesce(fr.postcode, '')) end desc nulls last,
            case when v_sort_key = 'country' and v_sort_dir = 'asc' then lower(coalesce(fr.country, '')) end asc nulls last,
            case when v_sort_key = 'country' and v_sort_dir = 'desc' then lower(coalesce(fr.country, '')) end desc nulls last,

            case when v_sort_dir = 'asc' then lower(coalesce(fr.matched_value_text, '')) end asc nulls last,
            case when v_sort_dir = 'desc' then lower(coalesce(fr.matched_value_text, '')) end desc nulls last,
            fr.row_id_text asc
        ) - 1 as rn
      from filtered_rows as fr
    ),
    prefixed_rows as (
      select
        rr.row_id_text,
        rr.matched_value_text,
        rr.rn
      from ranked_rows as rr
      where lower(coalesce(rr.matched_value_text, '')) like v_prefix_like escape '\'
    )
    select
      pr.row_id_text as row_id,
      pr.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((pr.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      pr.matched_value_text as matched_value,
      v_dataset_key as dataset_key
    from prefixed_rows as pr
    order by pr.rn
    limit 1;

    return;
  end if;

  if v_section = 'contracts' then
    return query
    with filtered_rows as (
      select
        ctr.id::text as row_id_text,
        case
          when v_sort_key = 'candidate_display' then coalesce(cand.display_name, '')
          when v_sort_key = 'client_name' then coalesce(cli.name, '')
          when v_sort_key = 'role' then coalesce(ctr.role, '')
          when v_sort_key = 'band' then coalesce(ctr.band, '')
          when v_sort_key = 'display_site' then coalesce(ctr.display_site, '')
          when v_sort_key = 'ward_hint' then coalesce(ctr.ward_hint, '')
          when v_sort_key = 'start_date' then coalesce(ctr.start_date::text, '')
          when v_sort_key = 'end_date' then coalesce(ctr.end_date::text, '')
          when v_sort_key = 'pay_method_snapshot' then coalesce(ctr.pay_method_snapshot, '')
          when v_sort_key = 'default_submission_mode' then coalesce(ctr.default_submission_mode, '')
          when v_sort_key = 'week_ending_weekday_snapshot' then coalesce(ctr.week_ending_weekday_snapshot::text, '')
          when v_sort_key = 'auto_invoice' then case when ctr.auto_invoice then 'true' else 'false' end
          when v_sort_key = 'require_reference_to_pay' then case when ctr.require_reference_to_pay then 'true' else 'false' end
          when v_sort_key = 'require_reference_to_invoice' then case when ctr.require_reference_to_invoice then 'true' else 'false' end
          when v_sort_key = 'created_at' then coalesce(ctr.created_at::text, '')
          when v_sort_key = 'updated_at' then coalesce(ctr.updated_at::text, '')
          else coalesce(cand.display_name, coalesce(cli.name, coalesce(ctr.role, '')))
        end as matched_value_text,
        cand.display_name as candidate_display,
        cli.name as client_name,
        ctr.role,
        ctr.band,
        ctr.display_site,
        ctr.ward_hint,
        ctr.start_date,
        ctr.end_date,
        ctr.pay_method_snapshot,
        ctr.default_submission_mode,
        ctr.week_ending_weekday_snapshot,
        ctr.auto_invoice,
        ctr.require_reference_to_pay,
        ctr.require_reference_to_invoice,
        ctr.created_at,
        ctr.updated_at,
        ctr.candidate_id
      from public.contracts as ctr
      left join public.candidates as cand
        on cand.id = ctr.candidate_id
      left join public.clients as cli
        on cli.id = ctr.client_id
      where (v_ids is null or ctr.id::text = any(v_ids))
        and (v_client_id is null or ctr.client_id = v_client_id)
        and (v_candidate_id is null or ctr.candidate_id = v_candidate_id)
        and (v_role is null or ctr.role = v_role)
        and (v_band is null or ctr.band = v_band)
        and (
          v_contract_status is null
          or (
            v_contract_status = 'ACTIVE'
            and ctr.candidate_id is not null
            and ctr.start_date <= v_today_uk
            and (ctr.end_date is null or ctr.end_date >= v_today_uk)
          )
          or (
            v_contract_status = 'UNASSIGNED'
            and ctr.candidate_id is null
            and ctr.start_date <= v_today_uk
            and (ctr.end_date is null or ctr.end_date >= v_today_uk)
          )
          or (
            v_contract_status = 'COMPLETED'
            and ctr.end_date is not null
            and ctr.end_date < v_today_uk
          )
        )
        and (v_q is null or (
          coalesce(cli.name, '') ilike ('%' || v_q || '%')
          or coalesce(cand.display_name, '') ilike ('%' || v_q || '%')
          or coalesce(cand.first_name, '') ilike ('%' || v_q || '%')
          or coalesce(cand.last_name, '') ilike ('%' || v_q || '%')
          or coalesce(ctr.role, '') ilike ('%' || v_q || '%')
          or coalesce(ctr.band, '') ilike ('%' || v_q || '%')
          or coalesce(ctr.display_site, '') ilike ('%' || v_q || '%')
          or coalesce(ctr.ward_hint, '') ilike ('%' || v_q || '%')
        ))
    ),
    ranked_rows as (
      select
        fr.row_id_text,
        fr.matched_value_text,
        row_number() over (
          order by
            case when v_sort_key = 'start_date' and v_sort_dir = 'asc' then fr.start_date end asc nulls last,
            case when v_sort_key = 'start_date' and v_sort_dir = 'desc' then fr.start_date end desc nulls last,
            case when v_sort_key = 'end_date' and v_sort_dir = 'asc' then fr.end_date end asc nulls last,
            case when v_sort_key = 'end_date' and v_sort_dir = 'desc' then fr.end_date end desc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'asc' then fr.created_at end asc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'desc' then fr.created_at end desc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'asc' then fr.updated_at end asc nulls last,
            case when v_sort_key = 'updated_at' and v_sort_dir = 'desc' then fr.updated_at end desc nulls last,
            case when v_sort_key = 'week_ending_weekday_snapshot' and v_sort_dir = 'asc' then fr.week_ending_weekday_snapshot end asc nulls last,
            case when v_sort_key = 'week_ending_weekday_snapshot' and v_sort_dir = 'desc' then fr.week_ending_weekday_snapshot end desc nulls last,
            case when v_sort_key = 'auto_invoice' and v_sort_dir = 'asc' then case when fr.auto_invoice then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'auto_invoice' and v_sort_dir = 'desc' then case when fr.auto_invoice then 1 else 0 end end desc nulls last,
            case when v_sort_key = 'require_reference_to_pay' and v_sort_dir = 'asc' then case when fr.require_reference_to_pay then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'require_reference_to_pay' and v_sort_dir = 'desc' then case when fr.require_reference_to_pay then 1 else 0 end end desc nulls last,
            case when v_sort_key = 'require_reference_to_invoice' and v_sort_dir = 'asc' then case when fr.require_reference_to_invoice then 1 else 0 end end asc nulls last,
            case when v_sort_key = 'require_reference_to_invoice' and v_sort_dir = 'desc' then case when fr.require_reference_to_invoice then 1 else 0 end end desc nulls last,

            case when v_sort_key = 'candidate_display' and v_sort_dir = 'asc' then lower(coalesce(fr.candidate_display, '')) end asc nulls last,
            case when v_sort_key = 'candidate_display' and v_sort_dir = 'desc' then lower(coalesce(fr.candidate_display, '')) end desc nulls last,
            case when v_sort_key = 'client_name' and v_sort_dir = 'asc' then lower(coalesce(fr.client_name, '')) end asc nulls last,
            case when v_sort_key = 'client_name' and v_sort_dir = 'desc' then lower(coalesce(fr.client_name, '')) end desc nulls last,
            case when v_sort_key = 'role' and v_sort_dir = 'asc' then lower(coalesce(fr.role, '')) end asc nulls last,
            case when v_sort_key = 'role' and v_sort_dir = 'desc' then lower(coalesce(fr.role, '')) end desc nulls last,
            case when v_sort_key = 'band' and v_sort_dir = 'asc' then lower(coalesce(fr.band, '')) end asc nulls last,
            case when v_sort_key = 'band' and v_sort_dir = 'desc' then lower(coalesce(fr.band, '')) end desc nulls last,
            case when v_sort_key = 'display_site' and v_sort_dir = 'asc' then lower(coalesce(fr.display_site, '')) end asc nulls last,
            case when v_sort_key = 'display_site' and v_sort_dir = 'desc' then lower(coalesce(fr.display_site, '')) end desc nulls last,
            case when v_sort_key = 'ward_hint' and v_sort_dir = 'asc' then lower(coalesce(fr.ward_hint, '')) end asc nulls last,
            case when v_sort_key = 'ward_hint' and v_sort_dir = 'desc' then lower(coalesce(fr.ward_hint, '')) end desc nulls last,
            case when v_sort_key = 'pay_method_snapshot' and v_sort_dir = 'asc' then lower(coalesce(fr.pay_method_snapshot, '')) end asc nulls last,
            case when v_sort_key = 'pay_method_snapshot' and v_sort_dir = 'desc' then lower(coalesce(fr.pay_method_snapshot, '')) end desc nulls last,
            case when v_sort_key = 'default_submission_mode' and v_sort_dir = 'asc' then lower(coalesce(fr.default_submission_mode, '')) end asc nulls last,
            case when v_sort_key = 'default_submission_mode' and v_sort_dir = 'desc' then lower(coalesce(fr.default_submission_mode, '')) end desc nulls last,

            case when v_sort_dir = 'asc' then lower(coalesce(fr.matched_value_text, '')) end asc nulls last,
            case when v_sort_dir = 'desc' then lower(coalesce(fr.matched_value_text, '')) end desc nulls last,
            fr.row_id_text asc
        ) - 1 as rn
      from filtered_rows as fr
    ),
    prefixed_rows as (
      select
        rr.row_id_text,
        rr.matched_value_text,
        rr.rn
      from ranked_rows as rr
      where lower(coalesce(rr.matched_value_text, '')) like v_prefix_like escape '\'
    )
    select
      pr.row_id_text as row_id,
      pr.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((pr.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      pr.matched_value_text as matched_value,
      v_dataset_key as dataset_key
    from prefixed_rows as pr
    order by pr.rn
    limit 1;

    return;
  end if;

  if v_section = 'timesheets' then
    return query
    with filtered_rows as (
      select
        coalesce(vts.timesheet_id::text, vts.contract_week_id::text) as row_id_text,
        case
          when v_sort_key = 'booking_id' then coalesce(vts.booking_id, '')
          when v_sort_key = 'candidate_name' then coalesce(vts.candidate_name, '')
          when v_sort_key = 'client_name' then coalesce(vts.client_name, '')
          when v_sort_key = 'hospital_norm' then coalesce(vts.hospital_norm, '')
          when v_sort_key = 'week_ending_date' then coalesce(vts.week_ending_date::text, '')
          when v_sort_key = 'route_display' then coalesce(vts.route_display, '')
          when v_sort_key = 'route_type' then coalesce(vts.route_type, '')
          when v_sort_key = 'tools_stage' then coalesce(vts.tools_stage, '')
          when v_sort_key = 'processing_status_display' then coalesce(vts.processing_status_display, '')
          when v_sort_key = 'total_hours' then coalesce(vts.total_hours::text, '')
          when v_sort_key = 'total_pay_ex_vat' then coalesce(vts.total_pay_ex_vat::text, '')
          when v_sort_key = 'total_charge_ex_vat' then coalesce(vts.total_charge_ex_vat::text, '')
          when v_sort_key = 'margin_ex_vat' then coalesce(vts.margin_ex_vat::text, '')
          when v_sort_key = 'pay_paid_at_utc' then coalesce(vts.pay_paid_at_utc::text, '')
          else coalesce(vts.candidate_name, coalesce(vts.client_name, coalesce(vts.booking_id, '')))
        end as matched_value_text,
        vts.booking_id,
        vts.candidate_name,
        vts.client_name,
        vts.hospital_norm,
        vts.week_ending_date,
        vts.route_display,
        vts.route_type,
        vts.route_family,
        vts.tools_stage,
        vts.processing_status,
        vts.processing_status_display,
        vts.total_hours,
        vts.total_pay_ex_vat,
        vts.total_charge_ex_vat,
        vts.margin_ex_vat,
        vts.pay_paid_at_utc,
        vts.issue_codes,
        vts.is_qr,
        vts.qr_status,
        vts.candidate_id,
        vts.client_id,
        vts.timesheet_id,
        vts.contract_week_id,
        vts.occupant_key_norm
      from public.timesheet_summary_lightweight_rows_v1(
        (
          coalesce(p_filters, '{}'::jsonb)
          - 'q'
          - 'query'
          - 'name'
          - 'route_type'
          - 'routeType'
          - 'issues_filter'
          - 'issuesFilter'
        )
        || jsonb_build_object(
             'disable_paging', true,
             'purpose', 'typeahead',
             'order_by', coalesce(nullif(v_sort_key, ''), 'candidate_name'),
             'order_dir', v_sort_dir
           )
      ) as vts
      left join public.timesheets as timesheet_row
        on timesheet_row.timesheet_id = vts.timesheet_id
       and timesheet_row.is_current = true
      where (
          v_ids is null
          or vts.timesheet_id::text = any(v_ids)
          or vts.contract_week_id::text = any(v_ids)
        )
        and (v_client_id is null or vts.client_id = v_client_id)
        and (v_candidate_id is null or vts.candidate_id = v_candidate_id)
        and (
          v_q is null
          or coalesce(vts.candidate_name, '') ilike ('%' || v_q || '%')
          or coalesce(vts.client_name, '') ilike ('%' || v_q || '%')
          or coalesce(vts.booking_id, '') ilike ('%' || v_q || '%')
          or coalesce(vts.occupant_key_norm, '') ilike ('%' || v_q || '%')
          or coalesce(vts.hospital_norm, '') ilike ('%' || v_q || '%')
        )
        and (
          v_route_type is null
          or (
            v_route_type = 'ELECTRONIC'
            and upper(coalesce(vts.route_type, '')) in ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC')
          )
          or (
            v_route_type = 'MANUAL'
            and (
              upper(coalesce(vts.route_type, '')) in ('DAILY_MANUAL', 'WEEKLY_MANUAL')
              or upper(coalesce(vts.route_family, '')) = 'MANUAL'
            )
          )
          or (
            v_route_type = 'NHSP'
            and (
              upper(coalesce(vts.route_type, '')) in ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
              or upper(coalesce(vts.route_family, '')) = 'NHSP'
            )
          )
          or (
            v_route_type = 'HEALTHROSTER'
            and (
              upper(coalesce(vts.route_type, '')) in ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
              or upper(coalesce(vts.route_family, '')) = 'HEALTHROSTER'
            )
          )
          or (
            v_route_type = 'QR'
            and coalesce(vts.is_qr, false) = true
          )
          or upper(coalesce(vts.route_type, '')) = v_route_type
          or upper(coalesce(vts.route_family, '')) = v_route_type
        )
        and (
          v_sheet_scope is null
          or upper(coalesce(vts.sheet_scope, '')) = v_sheet_scope
        )
        and (
          v_issues_filter is null
          or (
            v_issues_filter = 'NO_MATCH_ID'
            and (vts.candidate_id is null or vts.client_id is null)
          )
          or (
            v_issues_filter = 'RATE_MISSING'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) in ('RATE', 'RATE MISSING')
            )
          )
          or (
            v_issues_filter in ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) in ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
          or (
            v_issues_filter in ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
            and (
              upper(coalesce(vts.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
              or exists (
                select 1
                from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
                where upper(coalesce(ic.code, '')) in ('HR VALIDATION', 'AWAITING HR VALIDATION')
              )
            )
          )
          or (
            v_issues_filter in ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) in ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
            )
          )
          or (
            v_issues_filter = 'HR_HOURS_MISSING'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'HR HOURS MISSING'
            )
          )
          or (
            v_issues_filter = 'DUPLICATE_CONTRACTS'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'DUPLICATE CONTRACTS'
            )
          )
          or (
            v_issues_filter = 'TIMESHEET_EVIDENCE'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'TIMESHEET EVIDENCE MISSING'
            )
          )
          or (
            v_issues_filter = 'EXPENSES_EVIDENCE'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'EXPENSES EVIDENCE MISSING'
            )
          )
          or (
            v_issues_filter = 'MILEAGE_EVIDENCE'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'MILEAGE EVIDENCE MISSING'
            )
          )
          or (
            v_issues_filter = 'REFERENCE_MISSING'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) in ('REFERENCE', 'REFERENCE MISSING')
            )
          )
          or (
            v_issues_filter = 'REFS_PDF_INVALID'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'REFS - TIMESHEET PDF INVALID'
            )
          )
          or (
            v_issues_filter = 'VALIDATION'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'VALIDATION'
            )
          )
          or (
            v_issues_filter = 'ON_HOLD'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) = 'ON HOLD'
            )
          )
          or (
            v_issues_filter = 'AUTHORISATION'
            and exists (
              select 1
              from unnest(coalesce(vts.issue_codes, '{}'::text[])) as ic(code)
              where upper(coalesce(ic.code, '')) in ('AUTHORISATION', 'AWAITING AUTHORISATION')
            )
          )
          or (
            v_issues_filter = 'QR_NOT_ISSUED'
            and vts.timesheet_id is not null
            and upper(coalesce(vts.qr_status::text, '')) = 'PENDING'
            and coalesce(timesheet_row.qr_token, '') = ''
            and timesheet_row.qr_generated_at is null
          )
          or (
            v_issues_filter in ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
            and vts.timesheet_id is not null
            and upper(coalesce(vts.qr_status::text, '')) = 'PENDING'
            and coalesce(timesheet_row.qr_token, '') <> ''
            and timesheet_row.qr_generated_at is not null
            and timesheet_row.qr_scanned_at is null
          )
        )
    ),
    ranked_rows as (
      select
        fr.row_id_text,
        fr.matched_value_text,
        row_number() over (
          order by
            case when v_sort_key = 'week_ending_date' and v_sort_dir = 'asc' then fr.week_ending_date end asc nulls last,
            case when v_sort_key = 'week_ending_date' and v_sort_dir = 'desc' then fr.week_ending_date end desc nulls last,
            case when v_sort_key = 'total_hours' and v_sort_dir = 'asc' then fr.total_hours end asc nulls last,
            case when v_sort_key = 'total_hours' and v_sort_dir = 'desc' then fr.total_hours end desc nulls last,
            case when v_sort_key = 'total_pay_ex_vat' and v_sort_dir = 'asc' then fr.total_pay_ex_vat end asc nulls last,
            case when v_sort_key = 'total_pay_ex_vat' and v_sort_dir = 'desc' then fr.total_pay_ex_vat end desc nulls last,
            case when v_sort_key = 'total_charge_ex_vat' and v_sort_dir = 'asc' then fr.total_charge_ex_vat end asc nulls last,
            case when v_sort_key = 'total_charge_ex_vat' and v_sort_dir = 'desc' then fr.total_charge_ex_vat end desc nulls last,
            case when v_sort_key = 'margin_ex_vat' and v_sort_dir = 'asc' then fr.margin_ex_vat end asc nulls last,
            case when v_sort_key = 'margin_ex_vat' and v_sort_dir = 'desc' then fr.margin_ex_vat end desc nulls last,
            case when v_sort_key = 'pay_paid_at_utc' and v_sort_dir = 'asc' then fr.pay_paid_at_utc end asc nulls last,
            case when v_sort_key = 'pay_paid_at_utc' and v_sort_dir = 'desc' then fr.pay_paid_at_utc end desc nulls last,

            case when v_sort_key = 'booking_id' and v_sort_dir = 'asc' then lower(coalesce(fr.booking_id, '')) end asc nulls last,
            case when v_sort_key = 'booking_id' and v_sort_dir = 'desc' then lower(coalesce(fr.booking_id, '')) end desc nulls last,
            case when v_sort_key = 'candidate_name' and v_sort_dir = 'asc' then lower(coalesce(fr.candidate_name, '')) end asc nulls last,
            case when v_sort_key = 'candidate_name' and v_sort_dir = 'desc' then lower(coalesce(fr.candidate_name, '')) end desc nulls last,
            case when v_sort_key = 'client_name' and v_sort_dir = 'asc' then lower(coalesce(fr.client_name, '')) end asc nulls last,
            case when v_sort_key = 'client_name' and v_sort_dir = 'desc' then lower(coalesce(fr.client_name, '')) end desc nulls last,
            case when v_sort_key = 'hospital_norm' and v_sort_dir = 'asc' then lower(coalesce(fr.hospital_norm, '')) end asc nulls last,
            case when v_sort_key = 'hospital_norm' and v_sort_dir = 'desc' then lower(coalesce(fr.hospital_norm, '')) end desc nulls last,
            case when v_sort_key = 'route_display' and v_sort_dir = 'asc' then lower(coalesce(fr.route_display, '')) end asc nulls last,
            case when v_sort_key = 'route_display' and v_sort_dir = 'desc' then lower(coalesce(fr.route_display, '')) end desc nulls last,
            case when v_sort_key = 'route_type' and v_sort_dir = 'asc' then lower(coalesce(fr.route_type, '')) end asc nulls last,
            case when v_sort_key = 'route_type' and v_sort_dir = 'desc' then lower(coalesce(fr.route_type, '')) end desc nulls last,
            case when v_sort_key = 'tools_stage' and v_sort_dir = 'asc' then lower(coalesce(fr.tools_stage, '')) end asc nulls last,
            case when v_sort_key = 'tools_stage' and v_sort_dir = 'desc' then lower(coalesce(fr.tools_stage, '')) end desc nulls last,
            case when v_sort_key = 'processing_status_display' and v_sort_dir = 'asc' then lower(coalesce(fr.processing_status_display, '')) end asc nulls last,
            case when v_sort_key = 'processing_status_display' and v_sort_dir = 'desc' then lower(coalesce(fr.processing_status_display, '')) end desc nulls last,

            case when v_sort_dir = 'asc' then lower(coalesce(fr.matched_value_text, '')) end asc nulls last,
            case when v_sort_dir = 'desc' then lower(coalesce(fr.matched_value_text, '')) end desc nulls last,
            fr.row_id_text asc
        ) - 1 as rn
      from filtered_rows as fr
    ),
    prefixed_rows as (
      select
        rr.row_id_text,
        rr.matched_value_text,
        rr.rn
      from ranked_rows as rr
      where lower(coalesce(rr.matched_value_text, '')) like v_prefix_like escape '\'
    )
    select
      pr.row_id_text as row_id,
      pr.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((pr.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      pr.matched_value_text as matched_value,
      v_dataset_key as dataset_key
    from prefixed_rows as pr
    order by pr.rn
    limit 1;

    return;
  end if;

  if v_section = 'invoices' then
    return query
    with filtered_rows as (
      select
        inv.id::text as row_id_text,
        case
          when v_sort_key = 'invoice_no' then coalesce(inv.invoice_no, '')
          when v_sort_key = 'status' then coalesce(inv.status::text, '')
          when v_sort_key = 'status_date_utc' then coalesce(inv.status_date_utc::text, '')
          when v_sort_key = 'issued_at_utc' then coalesce(inv.issued_at_utc::text, '')
          when v_sort_key = 'due_at_utc' then coalesce(inv.due_at_utc::text, '')
          when v_sort_key = 'paid_at_utc' then coalesce(inv.paid_at_utc::text, '')
          when v_sort_key = 'subtotal_ex_vat' then coalesce(inv.subtotal_ex_vat::text, '')
          when v_sort_key = 'vat_amount' then coalesce(inv.vat_amount::text, '')
          when v_sort_key = 'total_inc_vat' then coalesce(inv.total_inc_vat::text, '')
          when v_sort_key = 'created_at' then coalesce(inv.created_at::text, '')
          else coalesce(inv.invoice_no, '')
        end as matched_value_text,
        inv.invoice_no,
        inv.status,
        inv.status_date_utc,
        inv.issued_at_utc,
        inv.due_at_utc,
        inv.paid_at_utc,
        inv.subtotal_ex_vat,
        inv.vat_amount,
        inv.total_inc_vat,
        inv.created_at
      from public.invoices as inv
      where (v_ids is null or inv.id::text = any(v_ids))
        and (
          v_status_list is null
          or upper(coalesce(inv.status::text, '')) = any(v_status_list)
        )
        and (
          v_q is null
          or coalesce(inv.invoice_no, '') ilike ('%' || v_q || '%')
        )
        and (
          v_issued_from is null
          or (inv.issued_at_utc at time zone 'Europe/London')::date >= v_issued_from
        )
        and (
          v_issued_to is null
          or (inv.issued_at_utc at time zone 'Europe/London')::date <= v_issued_to
        )
        and (
          (v_week_ending_from is null and v_week_ending_to is null)
          or exists (
            select 1
            from public.invoice_lines as il
            join public.timesheets as ts
              on ts.timesheet_id = il.timesheet_id
            where il.invoice_id = inv.id
              and (v_week_ending_from is null or ts.week_ending_date >= v_week_ending_from)
              and (v_week_ending_to is null or ts.week_ending_date <= v_week_ending_to)
          )
        )
    ),
    ranked_rows as (
      select
        fr.row_id_text,
        fr.matched_value_text,
        row_number() over (
          order by
            case when v_sort_key = 'issued_at_utc' and v_sort_dir = 'asc' then fr.issued_at_utc end asc nulls last,
            case when v_sort_key = 'issued_at_utc' and v_sort_dir = 'desc' then fr.issued_at_utc end desc nulls last,
            case when v_sort_key = 'due_at_utc' and v_sort_dir = 'asc' then fr.due_at_utc end asc nulls last,
            case when v_sort_key = 'due_at_utc' and v_sort_dir = 'desc' then fr.due_at_utc end desc nulls last,
            case when v_sort_key = 'paid_at_utc' and v_sort_dir = 'asc' then fr.paid_at_utc end asc nulls last,
            case when v_sort_key = 'paid_at_utc' and v_sort_dir = 'desc' then fr.paid_at_utc end desc nulls last,
            case when v_sort_key = 'status_date_utc' and v_sort_dir = 'asc' then fr.status_date_utc end asc nulls last,
            case when v_sort_key = 'status_date_utc' and v_sort_dir = 'desc' then fr.status_date_utc end desc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'asc' then fr.created_at end asc nulls last,
            case when v_sort_key = 'created_at' and v_sort_dir = 'desc' then fr.created_at end desc nulls last,
            case when v_sort_key = 'subtotal_ex_vat' and v_sort_dir = 'asc' then fr.subtotal_ex_vat end asc nulls last,
            case when v_sort_key = 'subtotal_ex_vat' and v_sort_dir = 'desc' then fr.subtotal_ex_vat end desc nulls last,
            case when v_sort_key = 'vat_amount' and v_sort_dir = 'asc' then fr.vat_amount end asc nulls last,
            case when v_sort_key = 'vat_amount' and v_sort_dir = 'desc' then fr.vat_amount end desc nulls last,
            case when v_sort_key = 'total_inc_vat' and v_sort_dir = 'asc' then fr.total_inc_vat end asc nulls last,
            case when v_sort_key = 'total_inc_vat' and v_sort_dir = 'desc' then fr.total_inc_vat end desc nulls last,

            case when v_sort_key = 'invoice_no' and v_sort_dir = 'asc' then lower(coalesce(fr.invoice_no, '')) end asc nulls last,
            case when v_sort_key = 'invoice_no' and v_sort_dir = 'desc' then lower(coalesce(fr.invoice_no, '')) end desc nulls last,
            case when v_sort_key = 'status' and v_sort_dir = 'asc' then lower(coalesce(fr.status::text, '')) end asc nulls last,
            case when v_sort_key = 'status' and v_sort_dir = 'desc' then lower(coalesce(fr.status::text, '')) end desc nulls last,

            case when v_sort_dir = 'asc' then lower(coalesce(fr.matched_value_text, '')) end asc nulls last,
            case when v_sort_dir = 'desc' then lower(coalesce(fr.matched_value_text, '')) end desc nulls last,
            fr.row_id_text asc
        ) - 1 as rn
      from filtered_rows as fr
    ),
    prefixed_rows as (
      select
        rr.row_id_text,
        rr.matched_value_text,
        rr.rn
      from ranked_rows as rr
      where lower(coalesce(rr.matched_value_text, '')) like v_prefix_like escape '\'
    )
    select
      pr.row_id_text as row_id,
      pr.rn as ordinal_index,
      case
        when p_page_size is null or p_page_size < 1 then 1
        else floor((pr.rn)::numeric / p_page_size)::int + 1
      end as target_page,
      pr.matched_value_text as matched_value,
      v_dataset_key as dataset_key
    from prefixed_rows as pr
    order by pr.rn
    limit 1;

    return;
  end if;

  return;
end
$function$;

BEGIN;



CREATE OR REPLACE FUNCTION public.timesheet_summary_lightweight_rows_v1(
  p_filters jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE (
  timesheet_id uuid,
  contract_week_id uuid,
  contract_id uuid,
  candidate_id uuid,
  candidate_name text,
  candidate_display_name text,
  client_id uuid,
  client_name text,
  booking_id text,
  occupant_key_norm text,
  hospital_norm text,
  candidate_hint_text jsonb,
  week_ending_date date,
  work_date date,
  sheet_scope text,
  submission_mode text,
  submission_mode_snapshot text,
  basis text,
  route_type text,
  route_display text,
  route_family text,
  route_subfamily text,
  underlying_channel_family text,
  summary_stage text,
  tools_stage text,
  processing_status text,
  processing_status_display text,
  authorised_at_utc timestamptz,
  authorised_at_server timestamptz,
  processed_at_utc timestamptz,
  is_authorised boolean,
  total_hours numeric,
  total_pay_ex_vat numeric,
  total_charge_ex_vat numeric,
  margin_ex_vat numeric,
  net_delta_ex_vat numeric,
  paid_at_utc timestamptz,
  pay_icon_code text,
  pay_status_code text,
  pay_paid_at_utc timestamptz,
  invoice_is_paid boolean,
  invoice_issue_stage text,
  invoice_segment_stage text,
  invoice_segments_total integer,
  invoice_segments_locked integer,
  invoice_segments_unlocked integer,
  issue_codes text[],
  validation_status text,
  validation_summary text,
  hr_crosscheck_status text,
  hr_crosscheck_issues text[],
  qr_status text,
  is_qr boolean,
  is_adjusted boolean,
  needs_attention boolean,
  has_rate_issue boolean,
  has_pay_channel_issue boolean,
  client_no_timesheet_required boolean,
  client_autoprocess_hr boolean,
  client_is_nhsp boolean,
  has_any_evidence boolean,
  attached_evidence_count integer,
  primary_artifact_storage_key text,
  primary_artifact_display_name text,
  primary_artifact_preview_mode text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_id_text text := NULL;
  v_lookup_ids uuid[] := NULL;
  v_has_lookup_filter boolean := FALSE;

  v_timesheet_ids_filter uuid[] := NULL;
  v_contract_week_ids_filter uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;

  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id_filter uuid := NULL;
  v_client_id_filter uuid := NULL;
  v_has_candidate_filter boolean := FALSE;
  v_has_client_filter boolean := FALSE;

  v_q text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;

  v_candidate_paid boolean := NULL;
  v_is_adjusted boolean := NULL;
  v_is_qr boolean := NULL;
  v_hr_issue boolean := NULL;
  v_hr_issue_token text := NULL;

  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;

  v_order_by text := 'candidate_name';
  v_order_dir text := 'asc';
  v_limit integer := 100;
  v_offset integer := 0;
  v_disable_paging boolean := FALSE;
BEGIN
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');
  v_has_lookup_filter := (v_id_text IS NOT NULL) OR (v_filters ? 'ids');

  IF v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    v_lookup_ids := ARRAY[v_id_text::uuid];
  ELSIF v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS id_value
      FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS id_values;
  ELSIF v_filters ? 'ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT split_values.value::uuid AS id_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS id_values;
  END IF;

  IF v_has_lookup_filter AND COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object(
           'timesheet_ids', to_jsonb(v_lookup_ids),
           'contract_week_ids', to_jsonb(v_lookup_ids)
         );
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheet_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheetIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL
        AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids_filter));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contract_week_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contractWeekIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL
        AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids_filter));
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  v_has_candidate_filter := v_candidate_id_text IS NOT NULL;
  BEGIN
    IF v_candidate_id_text IS NOT NULL AND v_candidate_id_text ~* v_uuid_re THEN
      v_candidate_id_filter := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id_filter := NULL;
  END;

  IF v_has_candidate_filter AND v_candidate_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  v_has_client_filter := v_client_id_text IS NOT NULL;
  BEGIN
    IF v_client_id_text IS NOT NULL AND v_client_id_text ~* v_uuid_re THEN
      v_client_id_filter := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id_filter := NULL;
  END;

  IF v_has_client_filter AND v_client_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_q := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), ''));
  v_tools_stage := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  v_route_type := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  v_sheet_scope := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  v_qr_status := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  v_status_code := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  v_issues_filter := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));

  IF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('true','t','yes','y','1') THEN
    v_candidate_paid := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('false','f','no','n','0') THEN
    v_candidate_paid := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('true','t','yes','y','1') THEN
    v_is_adjusted := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('false','f','no','n','0') THEN
    v_is_adjusted := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('true','t','yes','y','1') THEN
    v_is_qr := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('false','f','no','n','0') THEN
    v_is_qr := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('true','t','yes','y','1') THEN
    v_hr_issue := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('false','f','no','n','0') THEN
    v_hr_issue := FALSE;
  ELSE
    v_hr_issue_token := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
    IF v_hr_issue_token = 'ALL' THEN
      v_hr_issue_token := NULL;
    END IF;
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  v_order_by := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_by', v_filters->>'orderBy', 'candidate_name')), ''));
  v_order_dir := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_dir', v_filters->>'orderDir', 'asc')), ''));

  IF v_order_dir NOT IN ('asc', 'desc') THEN
    v_order_dir := 'asc';
  END IF;

  v_disable_paging :=
    LOWER(COALESCE(v_filters->>'disable_paging', v_filters->>'disablePaging', v_filters->>'no_paging', v_filters->>'noPaging', '')) IN ('true','t','yes','y','1')
    OR LOWER(COALESCE(v_filters->>'apply_paging', v_filters->>'applyPaging', '')) IN ('false','f','no','n','0')
    OR LOWER(COALESCE(v_filters->>'purpose', '')) IN ('membership','memberships','ids','totals','total','count','counts');

  IF v_disable_paging THEN
    v_limit := NULL;
    v_offset := 0;
  ELSE
    IF COALESCE(v_filters->>'limit', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST((v_filters->>'limit')::integer, 1), 5000);
    ELSIF COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;

    IF COALESCE(v_filters->>'offset', '') ~ '^[0-9]+$' THEN
      v_offset := GREATEST((v_filters->>'offset')::integer, 0);
    ELSIF COALESCE(v_filters->>'page', '') ~ '^[0-9]+$'
          AND COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_offset :=
        GREATEST(((v_filters->>'page')::integer - 1), 0)
        * LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  enriched AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id,
      source_rows.contract_week_id,
      COALESCE(timesheet_row.contract_id, contract_week_row.contract_id) AS contract_id,

      source_rows.candidate_id,
      source_rows.candidate_name,
      source_rows.candidate_name AS candidate_display_name,
      source_rows.client_id,
      source_rows.client_name,

      source_rows.booking_id,
      source_rows.occupant_key_norm,
      source_rows.hospital_norm,
      source_rows.candidate_hint_text,

      COALESCE(source_rows.contract_week_ending_date, source_rows.week_ending_date) AS week_ending_date,

      CASE
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN
          COALESCE(timesheet_row.worked_start_iso::date, timesheet_row.scheduled_start_iso::date, source_rows.week_ending_date)
        ELSE NULL::date
      END AS work_date,

      source_rows.sheet_scope::text AS sheet_scope,
      source_rows.submission_mode::text AS submission_mode,
      COALESCE(contract_week_row.submission_mode_snapshot::text, source_rows.submission_mode::text) AS submission_mode_snapshot,
      source_rows.basis::text AS basis,

      source_rows.route_type,
      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_is_nhsp, FALSE) = TRUE THEN 'NHSP Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_autoprocess_hr, FALSE) = TRUE THEN 'HealthRoster Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'Manual Adjustment'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NHSP' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER' THEN 'HealthRoster'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'QR' THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
        WHEN COALESCE(source_rows.route_type, '') <> '' THEN INITCAP(REPLACE(source_rows.route_type, '_', ' '))
        ELSE 'Manual'
      END AS route_display,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'HEALTHROSTER'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        WHEN COALESCE(source_rows.client_no_timesheet_required, FALSE) THEN 'NO_TIMESHEET_REQUIRED'
        ELSE 'MANUAL'
      END AS route_family,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL_ADJUSTMENT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'DAILY'
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS route_subfamily,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'IMPORT'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        ELSE 'MANUAL'
      END AS underlying_channel_family,

      source_rows.summary_stage,
      source_rows.tools_stage,
      source_rows.processing_status::text AS processing_status,
      source_rows.processing_status_display,

      COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) AS authorised_at_utc,
      source_rows.authorised_at_server,
      financial_row.processed_at_utc,

      (COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) IS NOT NULL) AS is_authorised,

      COALESCE(source_rows.total_hours, 0::numeric) AS total_hours,
      COALESCE(source_rows.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(source_rows.total_charge_ex_vat, 0::numeric) AS total_charge_ex_vat,
      COALESCE(source_rows.margin_ex_vat, 0::numeric) AS margin_ex_vat,
      COALESCE(source_rows.net_delta_ex_vat, COALESCE(source_rows.total_charge_ex_vat, 0::numeric) - COALESCE(source_rows.total_pay_ex_vat, 0::numeric)) AS net_delta_ex_vat,

      source_rows.paid_at_utc,
      source_rows.pay_icon_code,
      source_rows.pay_status_code,
      source_rows.pay_paid_at_utc,

      COALESCE(source_rows.invoice_is_paid, FALSE) AS invoice_is_paid,

      CASE
        WHEN COALESCE(source_rows.invoice_is_paid, FALSE) THEN 'PAID'
        WHEN COALESCE(source_rows.invoice_segments_locked, 0) > 0 THEN 'LOCKED'
        WHEN COALESCE(source_rows.invoice_segments_total, 0) > 0 THEN 'DRAFT'
        ELSE NULL::text
      END AS invoice_issue_stage,

      source_rows.invoice_segment_stage,
      COALESCE(source_rows.invoice_segments_total, 0)::integer AS invoice_segments_total,
      COALESCE(source_rows.invoice_segments_locked, 0)::integer AS invoice_segments_locked,
      COALESCE(source_rows.invoice_segments_unlocked, 0)::integer AS invoice_segments_unlocked,

      COALESCE(source_rows.issue_codes, ARRAY[]::text[]) AS issue_codes,
      source_rows.validation_status::text AS validation_status,

      CASE
        WHEN source_rows.validation_status IS NULL THEN NULL::text
        ELSE source_rows.validation_status::text
      END AS validation_summary,

      source_rows.hr_crosscheck_status,
      COALESCE(source_rows.hr_crosscheck_issues, ARRAY[]::text[]) AS hr_crosscheck_issues,

      source_rows.qr_status::text AS qr_status,
      timesheet_row.qr_token AS qr_token,
      timesheet_row.qr_generated_at AS qr_generated_at,
      timesheet_row.qr_scanned_at AS qr_scanned_at,
      COALESCE(source_rows.is_qr, FALSE) AS is_qr,
      COALESCE(source_rows.is_adjusted, FALSE) AS is_adjusted,
      COALESCE(source_rows.needs_attention, FALSE) AS needs_attention,
      COALESCE(source_rows.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(source_rows.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,

      COALESCE(source_rows.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(source_rows.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(source_rows.client_is_nhsp, FALSE) AS client_is_nhsp,

      (
        COALESCE(evidence_summary.attached_evidence_count, 0) > 0
        OR NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL
        OR NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL
        OR NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL
      ) AS has_any_evidence,

      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS attached_evidence_count,

      COALESCE(
        evidence_summary.primary_storage_key,
        NULLIF(timesheet_row.manual_pdf_r2_key, ''),
        NULLIF(timesheet_row.qr_r2_key, ''),
        NULLIF(contract_week_row.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key,

      COALESCE(
        evidence_summary.primary_display_name,
        CASE WHEN NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name,

      CASE
        WHEN COALESCE(evidence_summary.primary_storage_key, NULLIF(timesheet_row.manual_pdf_r2_key, ''), NULLIF(timesheet_row.qr_r2_key, ''), NULLIF(contract_week_row.uploaded_pdf_r2_key, '')) IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode

    FROM source_rows
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
  ),
  filtered AS MATERIALIZED (
    SELECT enriched_row.*
    FROM enriched AS enriched_row
    WHERE
      (
        NOT v_has_lookup_filter
        OR (
          v_lookup_ids IS NOT NULL
          AND (
            enriched_row.timesheet_id = ANY(v_lookup_ids)
            OR enriched_row.contract_week_id = ANY(v_lookup_ids)
          )
        )
      )
      AND (
        NOT v_has_timesheet_filter
        OR (
          v_timesheet_ids_filter IS NOT NULL
          AND enriched_row.timesheet_id = ANY(v_timesheet_ids_filter)
        )
      )
      AND (
        NOT v_has_contract_week_filter
        OR (
          v_contract_week_ids_filter IS NOT NULL
          AND enriched_row.contract_week_id = ANY(v_contract_week_ids_filter)
        )
      )
      AND (
        NOT v_has_candidate_filter
        OR enriched_row.candidate_id = v_candidate_id_filter
      )
      AND (
        NOT v_has_client_filter
        OR enriched_row.client_id = v_client_id_filter
      )
      AND (
        v_week_ending_from IS NULL
        OR enriched_row.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR enriched_row.week_ending_date <= v_week_ending_to
      )
      AND (
        v_q IS NULL
        OR LOWER(COALESCE(enriched_row.candidate_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.candidate_display_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.client_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.booking_id, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.occupant_key_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.hospital_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.route_display, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.processing_status_display, '')) LIKE '%' || v_q || '%'
      )
      AND (
        v_tools_stage IS NULL
        OR LOWER(COALESCE(enriched_row.tools_stage, '')) = v_tools_stage
      )
      AND (
        v_route_type IS NULL
        OR (
          v_route_type = 'electronic'
          AND UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC')
        )
        OR (
          v_route_type = 'manual'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'MANUAL'
          )
        )
        OR (
          v_route_type = 'nhsp'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_NHSP', 'NHSP')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NHSP'
          )
        )
        OR (
          v_route_type = 'healthroster'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'HEALTHROSTER'
          )
        )
        OR (
          v_route_type = 'qr'
          AND COALESCE(enriched_row.is_qr, FALSE) = TRUE
        )
        OR (
          v_route_type IN ('no_timesheet_required', 'no-timesheet-required')
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED'
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED'
            OR COALESCE(enriched_row.client_no_timesheet_required, FALSE) = TRUE
          )
        )
        OR LOWER(COALESCE(enriched_row.route_type, '')) = v_route_type
        OR LOWER(COALESCE(enriched_row.route_family, '')) = v_route_type
      )
      AND (
        v_sheet_scope IS NULL
        OR LOWER(COALESCE(enriched_row.sheet_scope, '')) = v_sheet_scope
      )
      AND (
        v_qr_status IS NULL
        OR LOWER(COALESCE(enriched_row.qr_status, '')) = v_qr_status
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'no_match_id'
          AND (enriched_row.candidate_id IS NULL OR enriched_row.client_id IS NULL)
        )
        OR (
          v_status_code = 'rate_missing'
          AND enriched_row.has_rate_issue
        )
        OR (
          v_status_code IN ('pay_chan_miss', 'pay_channel_missing')
          AND enriched_row.has_pay_channel_issue
        )
        OR (
          v_status_code = 'ready_for_hr'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_HR'
        )
        OR (
          v_status_code = 'ready_for_inv'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_INVOICE'
        )
        OR LOWER(COALESCE(enriched_row.processing_status, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.summary_stage, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.tools_stage, '')) = v_status_code
      )
      AND (
        v_candidate_paid IS NULL
        OR (
          (
            UPPER(COALESCE(enriched_row.pay_status_code, '')) IN ('PAID','PARTIALLY_PAID')
            OR enriched_row.pay_paid_at_utc IS NOT NULL
            OR enriched_row.paid_at_utc IS NOT NULL
          ) = v_candidate_paid
        )
      )
      AND (
        v_is_adjusted IS NULL
        OR enriched_row.is_adjusted = v_is_adjusted
      )
      AND (
        v_is_qr IS NULL
        OR enriched_row.is_qr = v_is_qr
      )
      AND (
        v_hr_issue IS NULL
        OR (
          (
            COALESCE(ARRAY_LENGTH(enriched_row.hr_crosscheck_issues, 1), 0) > 0
            OR (
              enriched_row.hr_crosscheck_status IS NOT NULL
              AND UPPER(enriched_row.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          ) = v_hr_issue
        )
      )
      AND (
        v_hr_issue_token IS NULL
        OR EXISTS (
          SELECT 1
          FROM UNNEST(COALESCE(enriched_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue_token
        )
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter IN ('all', 'any')
          AND (enriched_row.needs_attention OR COALESCE(ARRAY_LENGTH(enriched_row.issue_codes, 1), 0) > 0)
        )
        OR (
          v_issues_filter IN ('none', 'clear')
          AND (NOT enriched_row.needs_attention AND COALESCE(ARRAY_LENGTH(enriched_row.issue_codes, 1), 0) = 0)
        )
        OR (
          v_issues_filter IN ('rate', 'rates', 'rate_missing')
          AND (
            enriched_row.has_rate_issue
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('pay', 'pay_channel', 'pay-channel', 'pay_chan_miss', 'pay_channel_missing')
          AND (
            enriched_row.has_pay_channel_issue
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('hr', 'hr_issue', 'hr-issue', 'awaiting_hr_validation', 'awaiting_hr_validation_required')
          AND (
            COALESCE(ARRAY_LENGTH(enriched_row.hr_crosscheck_issues, 1), 0) > 0
            OR UPPER(COALESCE(enriched_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
            OR (
              enriched_row.hr_crosscheck_status IS NOT NULL
              AND UPPER(enriched_row.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          )
        )
        OR (
          v_issues_filter IN ('hr_hours_mismatch', 'hours_mismatch_hr')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'hr_hours_missing'
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'duplicate_contracts'
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter IN ('timesheet_evidence', 'expenses_evidence', 'mileage_evidence', 'reference_missing', 'validation', 'authorisation', 'on_hold', 'refs_pdf_invalid')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE
              (v_issues_filter = 'timesheet_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'TIMESHEET EVIDENCE MISSING')
              OR (v_issues_filter = 'expenses_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'EXPENSES EVIDENCE MISSING')
              OR (v_issues_filter = 'mileage_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'MILEAGE EVIDENCE MISSING')
              OR (v_issues_filter = 'reference_missing' AND UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING'))
              OR (v_issues_filter = 'validation' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION')
              OR (v_issues_filter = 'authorisation' AND UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION'))
              OR (v_issues_filter = 'on_hold' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD')
              OR (v_issues_filter = 'refs_pdf_invalid' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID')
          )
        )
        OR (
          v_issues_filter = 'qr_not_issued'
          AND enriched_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(enriched_row.qr_status, '')) = 'PENDING'
          AND COALESCE(enriched_row.qr_token, '') = ''
          AND enriched_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('qr_awaiting_signature', 'qr_issued_awaiting_signature')
          AND enriched_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(enriched_row.qr_status, '')) = 'PENDING'
          AND COALESCE(enriched_row.qr_token, '') <> ''
          AND enriched_row.qr_generated_at IS NOT NULL
          AND enriched_row.qr_scanned_at IS NULL
        )
        OR EXISTS (
          SELECT 1
          FROM UNNEST(COALESCE(enriched_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
          WHERE LOWER(issue_value.issue_code) = v_issues_filter
        )
      )
  )
  SELECT
    filtered_row.timesheet_id,
    filtered_row.contract_week_id,
    filtered_row.contract_id,
    filtered_row.candidate_id,
    filtered_row.candidate_name,
    filtered_row.candidate_display_name,
    filtered_row.client_id,
    filtered_row.client_name,
    filtered_row.booking_id,
    filtered_row.occupant_key_norm,
    filtered_row.hospital_norm,
    filtered_row.candidate_hint_text,
    filtered_row.week_ending_date,
    filtered_row.work_date,
    filtered_row.sheet_scope,
    filtered_row.submission_mode,
    filtered_row.submission_mode_snapshot,
    filtered_row.basis,
    filtered_row.route_type,
    filtered_row.route_display,
    filtered_row.route_family,
    filtered_row.route_subfamily,
    filtered_row.underlying_channel_family,
    filtered_row.summary_stage,
    filtered_row.tools_stage,
    filtered_row.processing_status,
    filtered_row.processing_status_display,
    filtered_row.authorised_at_utc,
    filtered_row.authorised_at_server,
    filtered_row.processed_at_utc,
    filtered_row.is_authorised,
    filtered_row.total_hours,
    filtered_row.total_pay_ex_vat,
    filtered_row.total_charge_ex_vat,
    filtered_row.margin_ex_vat,
    filtered_row.net_delta_ex_vat,
    filtered_row.paid_at_utc,
    filtered_row.pay_icon_code,
    filtered_row.pay_status_code,
    filtered_row.pay_paid_at_utc,
    filtered_row.invoice_is_paid,
    filtered_row.invoice_issue_stage,
    filtered_row.invoice_segment_stage,
    filtered_row.invoice_segments_total,
    filtered_row.invoice_segments_locked,
    filtered_row.invoice_segments_unlocked,
    filtered_row.issue_codes,
    filtered_row.validation_status,
    filtered_row.validation_summary,
    filtered_row.hr_crosscheck_status,
    filtered_row.hr_crosscheck_issues,
    filtered_row.qr_status,
    filtered_row.is_qr,
    filtered_row.is_adjusted,
    filtered_row.needs_attention,
    filtered_row.has_rate_issue,
    filtered_row.has_pay_channel_issue,
    filtered_row.client_no_timesheet_required,
    filtered_row.client_autoprocess_hr,
    filtered_row.client_is_nhsp,
    filtered_row.has_any_evidence,
    filtered_row.attached_evidence_count,
    filtered_row.primary_artifact_storage_key,
    filtered_row.primary_artifact_display_name,
    filtered_row.primary_artifact_preview_mode
  FROM filtered AS filtered_row
  ORDER BY
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'asc' THEN filtered_row.candidate_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'desc' THEN filtered_row.candidate_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'asc' THEN filtered_row.client_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'desc' THEN filtered_row.client_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'asc' THEN filtered_row.week_ending_date END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'desc' THEN filtered_row.week_ending_date END DESC NULLS LAST,

    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'asc' THEN filtered_row.work_date END ASC NULLS LAST,
    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'desc' THEN filtered_row.work_date END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'asc' THEN filtered_row.processing_status END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'desc' THEN filtered_row.processing_status END DESC NULLS LAST,

    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'asc' THEN filtered_row.tools_stage END ASC NULLS LAST,
    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'desc' THEN filtered_row.tools_stage END DESC NULLS LAST,

    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'asc' THEN filtered_row.route_type END ASC NULLS LAST,
    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'desc' THEN filtered_row.route_type END DESC NULLS LAST,

    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'asc' THEN filtered_row.sheet_scope END ASC NULLS LAST,
    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'desc' THEN filtered_row.sheet_scope END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'asc' THEN filtered_row.total_pay_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'desc' THEN filtered_row.total_pay_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'asc' THEN filtered_row.total_charge_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'desc' THEN filtered_row.total_charge_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'asc' THEN filtered_row.margin_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'desc' THEN filtered_row.margin_ex_vat END DESC NULLS LAST,

    filtered_row.candidate_name ASC NULLS LAST,
    filtered_row.week_ending_date DESC NULLS LAST,
    filtered_row.work_date DESC NULLS LAST,
    filtered_row.timesheet_id NULLS LAST,
    filtered_row.contract_week_id NULLS LAST
  LIMIT v_limit
  OFFSET v_offset;
END;
$function$;





REVOKE ALL ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) TO service_role;

COMMIT;




DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS TABLE(contract_week_id uuid, contract_id uuid, timesheet_id uuid, current_timesheet_id uuid, current_timesheet_version integer, was_stale boolean, created_now boolean, processing_status ts_fin_processing_status_enum, contract_week_json jsonb, timesheet_json jsonb, timesheet_financials_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());

  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;

  v_create_json jsonb := CASE
    WHEN p_timesheet_create_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_timesheet_create_json) = 'object' THEN p_timesheet_create_json
    ELSE NULL
  END;
  v_patch_json jsonb := CASE
    WHEN p_timesheet_patch_json IS NULL THEN '{}'::jsonb
    WHEN jsonb_typeof(p_timesheet_patch_json) = 'object' THEN p_timesheet_patch_json
    ELSE NULL
  END;
  v_week_patch_json jsonb := CASE
    WHEN p_contract_week_patch_json IS NULL THEN '{}'::jsonb
    WHEN jsonb_typeof(p_contract_week_patch_json) = 'object' THEN p_contract_week_patch_json
    ELSE NULL
  END;
  v_tsfin_snapshot_json jsonb := CASE
    WHEN p_tsfin_snapshot_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_tsfin_snapshot_json) = 'object' THEN p_tsfin_snapshot_json
    ELSE NULL
  END;
  v_rotation_json jsonb := CASE
    WHEN p_rotation_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_rotation_json) = 'object' THEN p_rotation_json
    ELSE NULL
  END;

  v_create_rec public.timesheets%ROWTYPE;
  v_patch_rec public.timesheets%ROWTYPE;
  v_week_patch_rec public.contract_weeks%ROWTYPE;

  v_created_now boolean := false;
  v_was_stale boolean := false;

  v_rotation_action text := NULL;
  v_rotation_revoke_reason text := NULL;
  v_rotation_new_timesheet_id uuid := NULL;
  v_rotation_pending_qr boolean := false;
  v_next_version integer := NULL;
  v_rotated_ts public.timesheets%ROWTYPE;

  v_original_booking_id text := NULL;
  v_candidate_booking_id text := NULL;
  v_booking_retry integer := 0;
  v_existing_booking_ts public.timesheets%ROWTYPE;
  v_existing_booking_cw_id uuid := NULL;
  v_existing_booking_cw_contract_id uuid := NULL;
  v_existing_booking_cw_week_ending_date date := NULL;
  v_existing_booking_same_context boolean := FALSE;

  v_queue_item public.manual_timesheet_queue%ROWTYPE;
  v_queue_kind text := NULL;
  v_queue_storage_key text := NULL;
  v_primary_timesheet_queue_id uuid := NULL;
  v_primary_timesheet_storage_key text := NULL;
  v_primary_timesheet_rotation_raw integer := 0;
  v_primary_timesheet_rotation_deg integer := 0;
  v_timesheet_kind_count integer := 0;

  v_missing jsonb := '[]'::jsonb;
  v_has_evidence boolean := false;
  v_pre_decision_row jsonb := NULL;
  v_post_decision_row jsonb := NULL;
  v_expected_row_signature text := NULL;
  v_current_row_signature text := NULL;
  v_previous_row_signature text := NULL;
  v_new_row_signature text := NULL;
  v_previous_processing_status public.ts_fin_processing_status_enum := NULL;
  v_segment_invoice_lock boolean := FALSE;
  v_compare_key text := NULL;
  -- p_tsfin_snapshot_json is the intended next-write TSFIN snapshot.
  -- It must not be compared to the old/current TSFIN for mutable financial values.

  v_outbox public.ts_financials_outbox%ROWTYPE;
  v_write_result record;

  v_numeric_re text := '^-?[0-9]+([.][0-9]+)?$';
  v_is_additional_manual_adjustment boolean := FALSE;
  v_force_empty_additional_schedule boolean := FALSE;
  v_effective_planned_schedule_json jsonb := NULL;
  v_effective_totals_json jsonb := NULL;
  v_effective_contract_week_hours_zero boolean := TRUE;
  v_expenses_pay_ex_vat numeric := 0;
  v_expenses_charge_ex_vat numeric := 0;
  v_expenses_margin_ex_vat numeric := 0;
  v_mileage_pay_ex_vat numeric := 0;
  v_mileage_charge_ex_vat numeric := 0;
  v_additional_pay_ex_vat numeric := 0;
  v_additional_charge_ex_vat numeric := 0;
  v_additional_margin_ex_vat numeric := 0;
  v_nonsegment_pay_ex_vat numeric := 0;
  v_nonsegment_charge_ex_vat numeric := 0;
  v_nonsegment_margin_ex_vat numeric := 0;
  v_total_pay_ex_vat numeric := 0;
  v_total_charge_ex_vat numeric := 0;
  v_total_margin_ex_vat numeric := 0;
  v_effective_invoice_breakdown_json jsonb := '{}'::jsonb;
  v_additional_units_json jsonb := '{}'::jsonb;
  v_has_additional_unit_rows boolean := FALSE;
  v_existing_planned_schedule_json jsonb := NULL;
  v_existing_totals_json jsonb := NULL;
  v_existing_contract_week_hours_zero boolean := TRUE;
  v_existing_contract_week_schedule_empty boolean := FALSE;
  v_payload_keep_empty_requested boolean := FALSE;
  v_payload_explicit_schedule_edit boolean := FALSE;
  v_paid_direct_expense_change boolean := FALSE;
BEGIN
  IF p_week_id IS NULL THEN
    RAISE EXCEPTION 'p_week_id is required';
  END IF;

  IF p_timesheet_patch_json IS NOT NULL AND v_patch_json IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_patch_json must be a JSON object';
  END IF;

  IF p_contract_week_patch_json IS NOT NULL AND v_week_patch_json IS NULL THEN
    RAISE EXCEPTION 'p_contract_week_patch_json must be a JSON object';
  END IF;

  IF p_timesheet_create_json IS NOT NULL AND v_create_json IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_create_json must be a JSON object';
  END IF;

  IF p_tsfin_snapshot_json IS NOT NULL AND v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION 'p_tsfin_snapshot_json must be a JSON object';
  END IF;

  IF p_rotation_json IS NOT NULL AND v_rotation_json IS NULL THEN
    RAISE EXCEPTION 'p_rotation_json must be a JSON object';
  END IF;

  SELECT *
  INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_WEEK_NOT_FOUND';
  END IF;

  -- Serialise weekly manual process/materialisation for the contract-week staged evidence keyspace.
  -- The partial unique index added by the staged TIMESHEET invariant is the hard backstop;
  -- this lock reduces avoidable retry/conflict churn inside this SQL transaction.
  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  SELECT *
  INTO v_contract
  FROM public.contracts AS ct
  WHERE ct.id = v_week.contract_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_NOT_FOUND';
  END IF;

  v_is_additional_manual_adjustment :=
    COALESCE(v_week.is_adjustment, FALSE) = TRUE
    OR COALESCE(v_week.additional_seq, 0) > 0;

  v_existing_planned_schedule_json := v_week.planned_schedule_json;
  v_existing_totals_json := v_week.totals_json;

  v_existing_contract_week_schedule_empty :=
    v_existing_planned_schedule_json IS NULL
    OR v_existing_planned_schedule_json = 'null'::jsonb
    OR CASE
      WHEN jsonb_typeof(v_existing_planned_schedule_json) = 'array' THEN jsonb_array_length(v_existing_planned_schedule_json) = 0
      ELSE FALSE
    END;

  SELECT COALESCE(SUM(
           CASE
             WHEN NULLIF(BTRIM(COALESCE(existing_hours_value.hour_value, '')), '') ~ v_numeric_re THEN existing_hours_value.hour_value::numeric
             ELSE 0::numeric
           END
         ), 0::numeric) = 0::numeric
    INTO v_existing_contract_week_hours_zero
  FROM jsonb_each_text(
    CASE
      WHEN jsonb_typeof(v_existing_totals_json->'hours') = 'object' THEN v_existing_totals_json->'hours'
      ELSE '{}'::jsonb
    END
  ) AS existing_hours_value(hour_key, hour_value);

  v_effective_planned_schedule_json := CASE
    WHEN v_week_patch_json ? 'planned_schedule_json' THEN v_week_patch_json->'planned_schedule_json'
    ELSE v_week.planned_schedule_json
  END;

  v_effective_totals_json := CASE
    WHEN v_week_patch_json ? 'totals_json' THEN v_week_patch_json->'totals_json'
    ELSE v_week.totals_json
  END;

  SELECT COALESCE(SUM(
           CASE
             WHEN NULLIF(BTRIM(COALESCE(hours_value.value, '')), '') ~ v_numeric_re THEN hours_value.value::numeric
             ELSE 0::numeric
           END
         ), 0::numeric) = 0::numeric
    INTO v_effective_contract_week_hours_zero
  FROM jsonb_each_text(
    CASE
      WHEN jsonb_typeof(v_effective_totals_json->'hours') = 'object' THEN v_effective_totals_json->'hours'
      ELSE '{}'::jsonb
    END
  ) AS hours_value(key, value);

  v_payload_keep_empty_requested :=
    COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE);

  v_payload_explicit_schedule_edit :=
    COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE);

  v_force_empty_additional_schedule :=
    COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
    AND COALESCE(v_payload_explicit_schedule_edit, FALSE) = FALSE
    AND (
      COALESCE(v_payload_keep_empty_requested, FALSE) = TRUE
      OR (
        COALESCE(v_existing_contract_week_schedule_empty, FALSE) = TRUE
        AND COALESCE(v_existing_contract_week_hours_zero, TRUE) = TRUE
      )
      OR (
        (
          v_effective_planned_schedule_json IS NULL
          OR v_effective_planned_schedule_json = 'null'::jsonb
          OR CASE
            WHEN jsonb_typeof(v_effective_planned_schedule_json) = 'array' THEN jsonb_array_length(v_effective_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND COALESCE(v_effective_contract_week_hours_zero, TRUE) = TRUE
      )
    );

  IF COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN
    IF v_create_json IS NOT NULL THEN
      v_create_json := v_create_json || jsonb_build_object('is_adjustment', TRUE);
    END IF;

    v_patch_json := COALESCE(v_patch_json, '{}'::jsonb) || jsonb_build_object('is_adjustment', TRUE);
    v_week_patch_json := COALESCE(v_week_patch_json, '{}'::jsonb) || jsonb_build_object('is_adjustment', TRUE);
  END IF;

  IF COALESCE(v_force_empty_additional_schedule, FALSE) = TRUE THEN
    v_effective_totals_json := COALESCE(v_effective_totals_json, '{}'::jsonb) || jsonb_build_object(
      'hours', jsonb_build_object(
        'day', 0,
        'night', 0,
        'sat', 0,
        'sun', 0,
        'bh', 0
      )
    );

    v_week_patch_json := COALESCE(v_week_patch_json, '{}'::jsonb) || jsonb_build_object(
      'planned_schedule_json', '[]'::jsonb,
      'totals_json', v_effective_totals_json
    );

    IF v_create_json IS NOT NULL THEN
      v_create_json := v_create_json || jsonb_build_object('actual_schedule_json', '[]'::jsonb);
    END IF;

    v_patch_json := COALESCE(v_patch_json, '{}'::jsonb) || jsonb_build_object('actual_schedule_json', '[]'::jsonb);

    IF v_tsfin_snapshot_json IS NOT NULL THEN
      -- Empty additional/manual adjustment weeks have no standard hours, but their
      -- expense, mileage, and genuine additional-unit amounts remain separate TSFIN
      -- buckets. Do not copy expense totals into additional_pay_ex_vat.
      v_expenses_pay_ex_vat :=
        COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'travel_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'travel_pay_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'accommodation_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'accommodation_pay_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'other_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'other_pay_ex_vat')::numeric ELSE NULL END, 0::numeric);

      IF v_expenses_pay_ex_vat = 0::numeric THEN
        v_expenses_pay_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'expenses_pay_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );
      END IF;

      v_expenses_charge_ex_vat :=
        COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'travel_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'travel_charge_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'accommodation_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'accommodation_charge_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'other_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'other_charge_ex_vat')::numeric ELSE NULL END, 0::numeric);

      IF v_expenses_charge_ex_vat = 0::numeric THEN
        v_expenses_charge_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'expenses_charge_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );
      END IF;

      v_mileage_pay_ex_vat := COALESCE(
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'mileage_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'mileage_pay_ex_vat')::numeric
          ELSE NULL
        END,
        0::numeric
      );

      v_mileage_charge_ex_vat := COALESCE(
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'mileage_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'mileage_charge_ex_vat')::numeric
          ELSE NULL
        END,
        0::numeric
      );

      v_additional_units_json := CASE
        WHEN jsonb_typeof(v_tsfin_snapshot_json->'additional_units_json') = 'object' THEN v_tsfin_snapshot_json->'additional_units_json'
        ELSE '{}'::jsonb
      END;

      WITH RECURSIVE additional_unit_walk(value_json, key_text) AS (
        SELECT root_json, root_key
        FROM (
          VALUES
            (v_additional_units_json, NULL::text),
            (v_patch_json->'additional_units_week', NULL::text),
            (v_patch_json->'additional_units_per_day', NULL::text),
            (v_create_json->'additional_units_week', NULL::text),
            (v_create_json->'additional_units_per_day', NULL::text),
            (v_effective_totals_json->'additional_units_week', NULL::text),
            (v_effective_totals_json->'additional_units_per_day', NULL::text)
        ) AS additional_unit_roots(root_json, root_key)
        WHERE root_json IS NOT NULL
          AND jsonb_typeof(root_json) IN ('object', 'array', 'number', 'string')

        UNION ALL

        SELECT child_json.value_json, child_json.key_text
        FROM additional_unit_walk AS additional_unit_parent
        CROSS JOIN LATERAL (
          SELECT object_child.value AS value_json,
                 object_child.key::text AS key_text
          FROM jsonb_each(
            CASE
              WHEN jsonb_typeof(additional_unit_parent.value_json) = 'object' THEN additional_unit_parent.value_json
              ELSE '{}'::jsonb
            END
          ) AS object_child(key, value)

          UNION ALL

          SELECT array_child.value AS value_json,
                 additional_unit_parent.key_text AS key_text
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(additional_unit_parent.value_json) = 'array' THEN additional_unit_parent.value_json
              ELSE '[]'::jsonb
            END
          ) AS array_child(value)
        ) AS child_json
      )
      SELECT EXISTS (
        SELECT 1
        FROM additional_unit_walk AS additional_unit_value
        WHERE (
          (
            jsonb_typeof(additional_unit_value.value_json) = 'number'
          AND NULLIF(BTRIM(additional_unit_value.value_json #>> '{}'), '') ~ v_numeric_re
          AND (additional_unit_value.value_json #>> '{}')::numeric <> 0::numeric
        )
        OR (
          jsonb_typeof(additional_unit_value.value_json) = 'string'
          AND NULLIF(BTRIM(additional_unit_value.value_json #>> '{}'), '') ~ v_numeric_re
          AND (additional_unit_value.value_json #>> '{}')::numeric <> 0::numeric
          )
        )
        AND LOWER(COALESCE(NULLIF(BTRIM(additional_unit_value.key_text), ''), '')) NOT IN (
          'pay_ex_vat',
          'charge_ex_vat',
          'margin_ex_vat',
          'pay_rate',
          'charge_rate',
          'rate',
          'amount',
          'pay',
          'charge',
          'margin',
          'price',
          'cost',
          'total',
          'total_pay',
          'total_charge',
          'total_margin',
          'total_pay_ex_vat',
          'total_charge_ex_vat',
          'total_margin_ex_vat',
          'vat',
          'vat_rate',
          'inc_vat',
          'code',
          'label',
          'name',
          'id',
          'sort_order',
          'order',
          'seq',
          'sequence',
          'version',
          'day_index',
          'additional_seq',
          'week',
          'date'
        )
      )
      INTO v_has_additional_unit_rows;

      IF COALESCE(v_has_additional_unit_rows, FALSE) THEN
        v_additional_pay_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_pay_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );

        v_additional_charge_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_charge_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );

        v_additional_margin_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_margin_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_margin_ex_vat')::numeric
            ELSE NULL
          END,
          v_additional_charge_ex_vat - v_additional_pay_ex_vat
        );
      ELSE
        v_additional_pay_ex_vat := 0::numeric;
        v_additional_charge_ex_vat := 0::numeric;
        v_additional_margin_ex_vat := 0::numeric;
      END IF;

      v_expenses_margin_ex_vat := v_expenses_charge_ex_vat - v_expenses_pay_ex_vat;
      v_nonsegment_pay_ex_vat := v_additional_pay_ex_vat + v_expenses_pay_ex_vat + v_mileage_pay_ex_vat;
      v_nonsegment_charge_ex_vat := v_additional_charge_ex_vat + v_expenses_charge_ex_vat + v_mileage_charge_ex_vat;
      v_nonsegment_margin_ex_vat := v_nonsegment_charge_ex_vat - v_nonsegment_pay_ex_vat;
      v_total_pay_ex_vat := v_nonsegment_pay_ex_vat;
      v_total_charge_ex_vat := v_nonsegment_charge_ex_vat;
      v_total_margin_ex_vat := v_total_charge_ex_vat - v_total_pay_ex_vat;

      v_effective_invoice_breakdown_json := COALESCE(
        CASE
          WHEN jsonb_typeof(v_tsfin_snapshot_json->'invoice_breakdown_json') = 'object' THEN v_tsfin_snapshot_json->'invoice_breakdown_json'
          ELSE '{}'::jsonb
        END,
        '{}'::jsonb
      ) || jsonb_build_object(
        'mode', 'SEGMENTS',
        'segments', '[]'::jsonb,
        'additional', jsonb_build_object(
          'units', v_additional_units_json,
          'pay_ex_vat', v_additional_pay_ex_vat,
          'charge_ex_vat', v_additional_charge_ex_vat,
          'margin_ex_vat', v_additional_margin_ex_vat
        ),
        'totals', jsonb_build_object(
          'total_hours', 0,
          'total_pay_ex_vat', v_total_pay_ex_vat,
          'total_charge_ex_vat', v_total_charge_ex_vat,
          'margin_ex_vat', v_total_margin_ex_vat
        )
      );

      v_tsfin_snapshot_json := v_tsfin_snapshot_json || jsonb_build_object(
        'hours_day', 0,
        'hours_night', 0,
        'hours_sat', 0,
        'hours_sun', 0,
        'hours_bh', 0,
        'pay_day', 0,
        'pay_night', 0,
        'pay_sat', 0,
        'pay_sun', 0,
        'pay_bh', 0,
        'charge_day', 0,
        'charge_night', 0,
        'charge_sat', 0,
        'charge_sun', 0,
        'charge_bh', 0,
        'total_hours', 0,
        'expenses_pay_ex_vat', v_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', v_expenses_charge_ex_vat,
        'additional_pay_ex_vat', v_additional_pay_ex_vat,
        'additional_charge_ex_vat', v_additional_charge_ex_vat,
        'additional_margin_ex_vat', v_additional_margin_ex_vat,
        'total_pay_ex_vat', v_total_pay_ex_vat,
        'total_charge_ex_vat', v_total_charge_ex_vat,
        'margin_ex_vat', v_total_margin_ex_vat,
        'actual_schedule_json', '[]'::jsonb,
        'actual_minutes_by_day_json', '{}'::jsonb,
        'invoice_breakdown_json', v_effective_invoice_breakdown_json
      );
    END IF;
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    SELECT *
    INTO v_pointer_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
    END IF;

    IF COALESCE(v_pointer_ts.is_current, false) THEN
      v_current_ts := v_pointer_ts;
    ELSE
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_pointer_ts.booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        SELECT *
        INTO v_current_ts
        FROM public.timesheets AS ts
        WHERE ts.booking_id = v_pointer_ts.booking_id
        ORDER BY ts.version DESC, ts.timesheet_id DESC
        LIMIT 1
        FOR UPDATE;

        IF NOT FOUND THEN
          v_current_ts := v_pointer_ts;
        END IF;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS DISTINCT FROM v_week.timesheet_id THEN
      v_was_stale := true;
    END IF;

    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'expected_timesheet_id is required';
    END IF;

    IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
      RAISE EXCEPTION USING
        MESSAGE = 'TIMESHEET_MOVED',
        DETAIL = jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id::text
        )::text;
    END IF;
  END IF;

  SELECT decision_result.row_json
    INTO v_pre_decision_row
  FROM public.bulk_timesheet_row_decision_v1(
    CASE
      WHEN v_current_ts.timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'dataset_mode', 'process',
        'timesheet_id', v_current_ts.timesheet_id::text,
        'contract_week_id', v_week.id::text
      )
      ELSE JSONB_BUILD_OBJECT(
        'dataset_mode', 'process',
        'contract_week_id', v_week.id::text
      )
    END
  ) AS decision_result(row_json)
  LIMIT 1;

  v_expected_row_signature := NULLIF(BTRIM(COALESCE(
    p_expected_row_signature,
    v_patch_json->>'row_signature',
    v_patch_json->>'rowSignature',
    v_week_patch_json->>'row_signature',
    v_week_patch_json->>'rowSignature',
    ''
  )), '');
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_pre_decision_row->>'row_signature', '')), '');
  v_previous_row_signature := v_current_row_signature;

  IF v_expected_row_signature IS NOT NULL
     AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature,
        'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END,
        'contract_week_id', v_week.id::text
      )::text;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION 'TIMESHEET_ALREADY_AUTHORISED';
    END IF;

    SELECT *
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tsfin_lock
    WHERE tsfin_lock.timesheet_id = v_current_ts.timesheet_id
      AND tsfin_lock.is_current = TRUE
    ORDER BY tsfin_lock.computed_at_utc DESC NULLS LAST, tsfin_lock.created_at DESC NULLS LAST, tsfin_lock.updated_at DESC NULLS LAST, tsfin_lock.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      v_paid_direct_expense_change := FALSE;

      IF v_current_tsfin.paid_at_utc IS NOT NULL
         AND v_tsfin_snapshot_json IS NOT NULL THEN
        SELECT (
          EXISTS (
            SELECT 1
            FROM (VALUES
              ('mileage_units', COALESCE(v_current_tsfin.mileage_units, 0)::numeric),
              ('mileage_pay_rate', COALESCE(v_current_tsfin.mileage_pay_rate, 0)::numeric),
              ('mileage_charge_rate', COALESCE(v_current_tsfin.mileage_charge_rate, 0)::numeric),
              ('mileage_pay_ex_vat', COALESCE(v_current_tsfin.mileage_pay_ex_vat, 0)::numeric),
              ('mileage_charge_ex_vat', COALESCE(v_current_tsfin.mileage_charge_ex_vat, 0)::numeric),
              ('travel_pay_ex_vat', COALESCE(v_current_tsfin.travel_pay_ex_vat, 0)::numeric),
              ('travel_charge_ex_vat', COALESCE(v_current_tsfin.travel_charge_ex_vat, 0)::numeric),
              ('accommodation_pay_ex_vat', COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0)::numeric),
              ('accommodation_charge_ex_vat', COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0)::numeric),
              ('other_pay_ex_vat', COALESCE(v_current_tsfin.other_pay_ex_vat, 0)::numeric),
              ('other_charge_ex_vat', COALESCE(v_current_tsfin.other_charge_ex_vat, 0)::numeric),
              ('expenses_pay_ex_vat', COALESCE(v_current_tsfin.expenses_pay_ex_vat, 0)::numeric),
              ('expenses_charge_ex_vat', COALESCE(v_current_tsfin.expenses_charge_ex_vat, 0)::numeric)
            ) AS paid_expense_guard(field_name, current_value)
            WHERE v_tsfin_snapshot_json ? paid_expense_guard.field_name
              AND (
                CASE
                  WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>paid_expense_guard.field_name, '')), '') ~ v_numeric_re
                    THEN (v_tsfin_snapshot_json->>paid_expense_guard.field_name)::numeric
                  ELSE 0::numeric
                END
              ) IS DISTINCT FROM paid_expense_guard.current_value
          )
          OR (
            v_tsfin_snapshot_json ? 'expenses_description'
            AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_description', '')), '')
                IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_current_tsfin.expenses_description, '')), '')
          )
        )
        INTO v_paid_direct_expense_change;
      END IF;

      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
         OR COALESCE(v_segment_invoice_lock, FALSE) = TRUE
         OR COALESCE(v_paid_direct_expense_change, FALSE) = TRUE THEN
        RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
      END IF;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    IF v_create_json IS NULL THEN
      RAISE EXCEPTION 'p_timesheet_create_json is required when no current timesheet exists';
    END IF;

    v_create_rec := jsonb_populate_record(NULL::public.timesheets, v_create_json);

    IF NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '') IS NULL THEN
      RAISE EXCEPTION 'booking_id is required for create';
    END IF;

    IF COALESCE(v_create_rec.week_ending_date, v_week.week_ending_date) IS DISTINCT FROM v_week.week_ending_date THEN
      RAISE EXCEPTION 'week_ending_date must match contract_weeks.week_ending_date';
    END IF;

    IF COALESCE(v_create_rec.contract_id, v_week.contract_id) IS DISTINCT FROM v_week.contract_id THEN
      RAISE EXCEPTION 'contract_id must match contract_weeks.contract_id';
    END IF;

    v_create_rec.booking_id := NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '');
    v_create_rec.version := COALESCE(v_create_rec.version, 1);
    v_original_booking_id := v_create_rec.booking_id;

    PERFORM pg_advisory_xact_lock(hashtext(v_original_booking_id));

    SELECT ts_existing.*
      INTO v_existing_booking_ts
    FROM public.timesheets AS ts_existing
    WHERE ts_existing.booking_id = v_original_booking_id
    ORDER BY
      CASE WHEN COALESCE(ts_existing.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
      ts_existing.version DESC NULLS LAST,
      ts_existing.updated_at DESC NULLS LAST,
      ts_existing.created_at DESC NULLS LAST,
      ts_existing.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      v_existing_booking_cw_id := NULL;
      v_existing_booking_cw_contract_id := NULL;
      v_existing_booking_cw_week_ending_date := NULL;

      SELECT cw_existing.id,
             cw_existing.contract_id,
             cw_existing.week_ending_date
        INTO v_existing_booking_cw_id,
             v_existing_booking_cw_contract_id,
             v_existing_booking_cw_week_ending_date
      FROM public.contract_weeks AS cw_existing
      WHERE cw_existing.timesheet_id = v_existing_booking_ts.timesheet_id
      ORDER BY
        CASE WHEN cw_existing.id = v_week.id THEN 0 ELSE 1 END ASC,
        cw_existing.updated_at DESC NULLS LAST,
        cw_existing.created_at DESC NULLS LAST,
        cw_existing.id DESC
      LIMIT 1
      FOR UPDATE;

      v_existing_booking_same_context :=
        v_existing_booking_ts.contract_id IS NOT DISTINCT FROM v_week.contract_id
        AND v_existing_booking_ts.week_ending_date IS NOT DISTINCT FROM v_week.week_ending_date
        AND v_existing_booking_cw_id IS NOT NULL
        AND v_existing_booking_cw_id IS NOT DISTINCT FROM v_week.id;

      IF v_existing_booking_same_context THEN
        IF COALESCE(v_existing_booking_ts.is_current, FALSE) = TRUE THEN
          v_current_ts := v_existing_booking_ts;
        ELSE
          SELECT ts_current_existing.*
            INTO v_current_ts
          FROM public.timesheets AS ts_current_existing
          WHERE ts_current_existing.booking_id = v_original_booking_id
            AND ts_current_existing.is_current = TRUE
          ORDER BY ts_current_existing.version DESC NULLS LAST,
                   ts_current_existing.updated_at DESC NULLS LAST,
                   ts_current_existing.created_at DESC NULLS LAST,
                   ts_current_existing.timesheet_id DESC
          LIMIT 1
          FOR UPDATE;

          IF NOT FOUND THEN
            v_current_ts := NULL;
            SELECT COALESCE(MAX(ts_version_existing.version), 0) + 1
              INTO v_create_rec.version
            FROM public.timesheets AS ts_version_existing
            WHERE ts_version_existing.booking_id = v_original_booking_id;
            v_create_rec.booking_id := v_original_booking_id;
          END IF;
        END IF;

        IF v_current_ts.timesheet_id IS NOT NULL THEN
          v_was_stale := TRUE;
          v_patch_json :=
            (
              v_create_json
              - 'timesheet_id'
              - 'booking_id'
              - 'version'
              - 'is_current'
              - 'contract_id'
              - 'week_ending_date'
              - 'created_at'
            ) || COALESCE(v_patch_json, '{}'::jsonb);
        END IF;
      ELSE
        v_booking_retry := 0;
        LOOP
          v_booking_retry := v_booking_retry + 1;

          IF v_booking_retry > 25 THEN
            RAISE EXCEPTION USING
              MESSAGE = 'BOOKING_ID_COLLISION',
              DETAIL = jsonb_build_object(
                'reason', 'Unable to derive a collision-free weekly manual booking_id for contract_week.',
                'supplied_booking_id', v_original_booking_id,
                'contract_week_id', v_week.id::text,
                'target_contract_id', v_week.contract_id::text,
                'target_week_ending_date', v_week.week_ending_date::text,
                'existing_timesheet_id', v_existing_booking_ts.timesheet_id::text,
                'existing_contract_id', CASE WHEN v_existing_booking_ts.contract_id IS NULL THEN NULL ELSE v_existing_booking_ts.contract_id::text END,
                'existing_week_ending_date', CASE WHEN v_existing_booking_ts.week_ending_date IS NULL THEN NULL ELSE v_existing_booking_ts.week_ending_date::text END,
                'existing_contract_week_id', CASE WHEN v_existing_booking_cw_id IS NULL THEN NULL ELSE v_existing_booking_cw_id::text END
              )::text;
          END IF;

          IF v_booking_retry = 1 THEN
            v_candidate_booking_id := 'bk_cw_' || REPLACE(v_week.id::text, '-', '');
          ELSE
            v_candidate_booking_id := 'bk_cw_' || REPLACE(v_week.id::text, '-', '') || '_' || SUBSTRING(MD5(
              v_original_booking_id ||
              '|contract_week_id=' || v_week.id::text ||
              '|contract_id=' || v_week.contract_id::text ||
              '|week_ending_date=' || v_week.week_ending_date::text ||
              '|try=' || v_booking_retry::text
            ) FROM 1 FOR 16);
          END IF;

          PERFORM pg_advisory_xact_lock(hashtext(v_candidate_booking_id));

          SELECT ts_candidate.*
            INTO v_existing_booking_ts
          FROM public.timesheets AS ts_candidate
          WHERE ts_candidate.booking_id = v_candidate_booking_id
          ORDER BY
            CASE WHEN COALESCE(ts_candidate.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
            ts_candidate.version DESC NULLS LAST,
            ts_candidate.updated_at DESC NULLS LAST,
            ts_candidate.created_at DESC NULLS LAST,
            ts_candidate.timesheet_id DESC
          LIMIT 1
          FOR UPDATE;

          IF NOT FOUND THEN
            v_create_rec.booking_id := v_candidate_booking_id;
            EXIT;
          END IF;

          v_existing_booking_cw_id := NULL;
          v_existing_booking_cw_contract_id := NULL;
          v_existing_booking_cw_week_ending_date := NULL;

          SELECT cw_candidate.id,
                 cw_candidate.contract_id,
                 cw_candidate.week_ending_date
            INTO v_existing_booking_cw_id,
                 v_existing_booking_cw_contract_id,
                 v_existing_booking_cw_week_ending_date
          FROM public.contract_weeks AS cw_candidate
          WHERE cw_candidate.timesheet_id = v_existing_booking_ts.timesheet_id
          ORDER BY
            CASE WHEN cw_candidate.id = v_week.id THEN 0 ELSE 1 END ASC,
            cw_candidate.updated_at DESC NULLS LAST,
            cw_candidate.created_at DESC NULLS LAST,
            cw_candidate.id DESC
          LIMIT 1
          FOR UPDATE;

          v_existing_booking_same_context :=
            v_existing_booking_ts.contract_id IS NOT DISTINCT FROM v_week.contract_id
            AND v_existing_booking_ts.week_ending_date IS NOT DISTINCT FROM v_week.week_ending_date
            AND v_existing_booking_cw_id IS NOT NULL
            AND v_existing_booking_cw_id IS NOT DISTINCT FROM v_week.id;

          IF v_existing_booking_same_context THEN
            IF COALESCE(v_existing_booking_ts.is_current, FALSE) = TRUE THEN
              v_current_ts := v_existing_booking_ts;
            ELSE
              SELECT ts_current_candidate.*
                INTO v_current_ts
              FROM public.timesheets AS ts_current_candidate
              WHERE ts_current_candidate.booking_id = v_candidate_booking_id
                AND ts_current_candidate.is_current = TRUE
              ORDER BY ts_current_candidate.version DESC NULLS LAST,
                       ts_current_candidate.updated_at DESC NULLS LAST,
                       ts_current_candidate.created_at DESC NULLS LAST,
                       ts_current_candidate.timesheet_id DESC
              LIMIT 1
              FOR UPDATE;

              IF NOT FOUND THEN
                v_current_ts := NULL;
                SELECT COALESCE(MAX(ts_version_candidate.version), 0) + 1
                  INTO v_create_rec.version
                FROM public.timesheets AS ts_version_candidate
                WHERE ts_version_candidate.booking_id = v_candidate_booking_id;
                v_create_rec.booking_id := v_candidate_booking_id;
              END IF;
            END IF;

            IF v_current_ts.timesheet_id IS NOT NULL THEN
              v_was_stale := TRUE;
              v_patch_json :=
                (
                  v_create_json
                  - 'timesheet_id'
                  - 'booking_id'
                  - 'version'
                  - 'is_current'
                  - 'contract_id'
                  - 'week_ending_date'
                  - 'created_at'
                ) || COALESCE(v_patch_json, '{}'::jsonb);
            END IF;
            EXIT;
          END IF;
        END LOOP;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS NULL THEN
      BEGIN
        INSERT INTO public.timesheets (
      timesheet_id,
      booking_id,
      occupant_key_norm,
      hospital_norm,
      ward_norm,
      job_title_norm,
      shift_label_norm,
      week_ending_date,
      authorised_at_server,
      status,
      created_at,
      updated_at,
      version,
      is_current,
      contract_id,
      submission_mode,
      manual_pdf_r2_key,
      line_type,
      sheet_scope,
      actual_schedule_json,
      additional_units_week,
      additional_units_per_day,
      qr_token,
      qr_status,
      qr_payload_json,
      qr_generated_at,
      qr_scanned_at,
      qr_scan_info_json,
      qr_r2_key,
      day_references_json,
      manual_pdf_rotation_degrees,
      qr_last_sent_hash,
      qr_last_sent_at_utc,
      qr_signed_hash,
      qr_signed_at_utc,
      is_adjustment
    )
    VALUES (
      COALESCE(v_create_rec.timesheet_id, gen_random_uuid()),
      v_create_rec.booking_id,
      v_create_rec.occupant_key_norm,
      v_create_rec.hospital_norm,
      v_create_rec.ward_norm,
      v_create_rec.job_title_norm,
      COALESCE(v_create_rec.shift_label_norm, 'weekly'),
      v_week.week_ending_date,
      v_create_rec.authorised_at_server,
      COALESCE(v_create_rec.status, 'RECEIVED'::public.timesheet_status_enum),
      COALESCE(v_create_rec.created_at, v_now),
      COALESCE(v_create_rec.updated_at, v_now),
      COALESCE(v_create_rec.version, 1),
      true,
      v_week.contract_id,
      COALESCE(v_create_rec.submission_mode, 'MANUAL'::public.submission_mode_enum),
      v_create_rec.manual_pdf_r2_key,
      COALESCE(v_create_rec.line_type, 'HOURS'::public.timesheet_line_type_enum),
      COALESCE(v_create_rec.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
      COALESCE(v_create_rec.actual_schedule_json, '[]'::jsonb),
      COALESCE(v_create_rec.additional_units_week, '{}'::jsonb),
      COALESCE(v_create_rec.additional_units_per_day, '{}'::jsonb),
      v_create_rec.qr_token,
      v_create_rec.qr_status,
      COALESCE(v_create_rec.qr_payload_json, '{}'::jsonb),
      v_create_rec.qr_generated_at,
      v_create_rec.qr_scanned_at,
      v_create_rec.qr_scan_info_json,
      v_create_rec.qr_r2_key,
      v_create_rec.day_references_json,
      COALESCE(v_create_rec.manual_pdf_rotation_degrees, 0),
      v_create_rec.qr_last_sent_hash,
      v_create_rec.qr_last_sent_at_utc,
      v_create_rec.qr_signed_hash,
      v_create_rec.qr_signed_at_utc,
      CASE
        WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
        ELSE COALESCE(v_create_rec.is_adjustment, FALSE)
      END
    )
    RETURNING * INTO v_current_ts;

        v_created_now := true;
      EXCEPTION WHEN unique_violation THEN
        RAISE EXCEPTION USING
          MESSAGE = 'BOOKING_ID_COLLISION',
          DETAIL = jsonb_build_object(
            'reason', 'Unique violation while creating weekly manual timesheet.',
            'constraint', 'timesheets_booking_id_version_uidx',
            'booking_id', v_create_rec.booking_id,
            'version', COALESCE(v_create_rec.version, 1),
            'supplied_booking_id', v_original_booking_id,
            'contract_week_id', v_week.id::text,
            'target_contract_id', v_week.contract_id::text,
            'target_week_ending_date', v_week.week_ending_date::text
          )::text;
      END;
    END IF;
  ELSE
    IF v_rotation_json IS NOT NULL THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION 'p_actor_user_id is required when p_rotation_json is supplied';
      END IF;

      v_rotation_action := upper(NULLIF(BTRIM(COALESCE(v_rotation_json->>'qr_action', '')), ''));
      IF v_rotation_action NOT IN ('INVALIDATE', 'REISSUE', 'REVOKE_TO_MANUAL') THEN
        RAISE EXCEPTION 'p_rotation_json.qr_action must be INVALIDATE, REISSUE, or REVOKE_TO_MANUAL';
      END IF;

      v_rotation_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_rotation_json->>'new_timesheet_id', '')), '')::uuid;
      IF v_rotation_new_timesheet_id IS NULL THEN
        RAISE EXCEPTION 'p_rotation_json.new_timesheet_id is required';
      END IF;

      IF v_current_ts.timesheet_id IS NULL THEN
        RAISE EXCEPTION 'p_rotation_json requires an existing current timesheet';
      END IF;

      IF NULLIF(BTRIM(COALESCE(v_current_ts.booking_id, '')), '') IS NULL THEN
        RAISE EXCEPTION 'Cannot rotate: booking_id missing on current timesheet';
      END IF;

      v_rotation_revoke_reason := NULLIF(BTRIM(COALESCE(v_rotation_json->>'revoke_reason', '')), '');
      v_rotation_pending_qr := v_rotation_action IN ('INVALIDATE', 'REISSUE');

      PERFORM pg_advisory_xact_lock(hashtext(v_current_ts.booking_id));

      SELECT ts.version
      INTO v_next_version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_current_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      v_next_version := COALESCE(v_next_version, COALESCE(v_current_ts.version, 1)) + 1;

      UPDATE public.timesheets AS ts
      SET is_current = false,
          status = 'REVOKED'::public.timesheet_status_enum,
          revoked_reason = v_rotation_revoke_reason,
          revoked_by = p_actor_user_id::text,
          updated_at = v_now
      WHERE ts.timesheet_id = v_current_ts.timesheet_id
        AND ts.is_current = true;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'TIMESHEET_ROTATE_REVOKE_FAILED';
      END IF;

      v_rotated_ts := v_current_ts;
      v_rotated_ts.timesheet_id := v_rotation_new_timesheet_id;
      v_rotated_ts.version := v_next_version;
      v_rotated_ts.is_current := true;
      v_rotated_ts.status := 'RECEIVED'::public.timesheet_status_enum;
      v_rotated_ts.revoked_reason := NULL;
      v_rotated_ts.revoked_by := NULL;
      v_rotated_ts.authorised_at_server := NULL;
      v_rotated_ts.qr_token := NULL;
      v_rotated_ts.qr_status := CASE
        WHEN v_rotation_pending_qr THEN 'PENDING'::public.timesheet_qr_status_enum
        ELSE NULL
      END;
      v_rotated_ts.qr_payload_json := '{}'::jsonb;
      v_rotated_ts.qr_generated_at := NULL;
      v_rotated_ts.qr_scanned_at := NULL;
      v_rotated_ts.qr_scan_info_json := NULL;
      v_rotated_ts.qr_r2_key := NULL;
      v_rotated_ts.qr_last_sent_hash := NULL;
      v_rotated_ts.qr_last_sent_at_utc := NULL;
      v_rotated_ts.qr_signed_hash := NULL;
      v_rotated_ts.qr_signed_at_utc := NULL;
      v_rotated_ts.manual_pdf_r2_key := NULL;
      v_rotated_ts.created_at := v_now;
      v_rotated_ts.updated_at := v_now;

      INSERT INTO public.timesheets
      SELECT (v_rotated_ts).*
      RETURNING * INTO v_current_ts;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL AND COALESCE(v_created_now, FALSE) = FALSE THEN
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION 'TIMESHEET_ALREADY_AUTHORISED';
    END IF;

    SELECT *
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tsfin_reuse_lock
    WHERE tsfin_reuse_lock.timesheet_id = v_current_ts.timesheet_id
      AND tsfin_reuse_lock.is_current = TRUE
    ORDER BY tsfin_reuse_lock.computed_at_utc DESC NULLS LAST,
             tsfin_reuse_lock.created_at DESC NULLS LAST,
             tsfin_reuse_lock.updated_at DESC NULLS LAST,
             tsfin_reuse_lock.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS reuse_invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(reuse_invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      v_paid_direct_expense_change := FALSE;

      IF v_current_tsfin.paid_at_utc IS NOT NULL
         AND v_tsfin_snapshot_json IS NOT NULL THEN
        SELECT (
          EXISTS (
            SELECT 1
            FROM (VALUES
              ('mileage_units', COALESCE(v_current_tsfin.mileage_units, 0)::numeric),
              ('mileage_pay_rate', COALESCE(v_current_tsfin.mileage_pay_rate, 0)::numeric),
              ('mileage_charge_rate', COALESCE(v_current_tsfin.mileage_charge_rate, 0)::numeric),
              ('mileage_pay_ex_vat', COALESCE(v_current_tsfin.mileage_pay_ex_vat, 0)::numeric),
              ('mileage_charge_ex_vat', COALESCE(v_current_tsfin.mileage_charge_ex_vat, 0)::numeric),
              ('travel_pay_ex_vat', COALESCE(v_current_tsfin.travel_pay_ex_vat, 0)::numeric),
              ('travel_charge_ex_vat', COALESCE(v_current_tsfin.travel_charge_ex_vat, 0)::numeric),
              ('accommodation_pay_ex_vat', COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0)::numeric),
              ('accommodation_charge_ex_vat', COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0)::numeric),
              ('other_pay_ex_vat', COALESCE(v_current_tsfin.other_pay_ex_vat, 0)::numeric),
              ('other_charge_ex_vat', COALESCE(v_current_tsfin.other_charge_ex_vat, 0)::numeric),
              ('expenses_pay_ex_vat', COALESCE(v_current_tsfin.expenses_pay_ex_vat, 0)::numeric),
              ('expenses_charge_ex_vat', COALESCE(v_current_tsfin.expenses_charge_ex_vat, 0)::numeric)
            ) AS paid_expense_guard(field_name, current_value)
            WHERE v_tsfin_snapshot_json ? paid_expense_guard.field_name
              AND (
                CASE
                  WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>paid_expense_guard.field_name, '')), '') ~ v_numeric_re
                    THEN (v_tsfin_snapshot_json->>paid_expense_guard.field_name)::numeric
                  ELSE 0::numeric
                END
              ) IS DISTINCT FROM paid_expense_guard.current_value
          )
          OR (
            v_tsfin_snapshot_json ? 'expenses_description'
            AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_description', '')), '')
                IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_current_tsfin.expenses_description, '')), '')
          )
        )
        INTO v_paid_direct_expense_change;
      END IF;

      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
         OR COALESCE(v_segment_invoice_lock, FALSE) = TRUE
         OR COALESCE(v_paid_direct_expense_change, FALSE) = TRUE THEN
        RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
      END IF;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_READY';
  END IF;

  IF v_patch_json <> '{}'::jsonb THEN
    v_patch_rec := jsonb_populate_record(v_current_ts, v_patch_json);

    UPDATE public.timesheets AS ts
    SET occupant_key_norm = v_patch_rec.occupant_key_norm,
        hospital_norm = v_patch_rec.hospital_norm,
        ward_norm = v_patch_rec.ward_norm,
        job_title_norm = v_patch_rec.job_title_norm,
        shift_label_norm = v_patch_rec.shift_label_norm,
        authorised_at_server = v_patch_rec.authorised_at_server,
        status = v_patch_rec.status,
        updated_at = CASE
          WHEN v_patch_json ? 'updated_at' THEN COALESCE(v_patch_rec.updated_at, v_now)
          ELSE v_now
        END,
        submission_mode = v_patch_rec.submission_mode,
        manual_pdf_r2_key = v_patch_rec.manual_pdf_r2_key,
        line_type = v_patch_rec.line_type,
        sheet_scope = v_patch_rec.sheet_scope,
        actual_schedule_json = v_patch_rec.actual_schedule_json,
        additional_units_week = COALESCE(v_patch_rec.additional_units_week, '{}'::jsonb),
        additional_units_per_day = COALESCE(v_patch_rec.additional_units_per_day, '{}'::jsonb),
        qr_token = v_patch_rec.qr_token,
        qr_status = v_patch_rec.qr_status,
        qr_payload_json = COALESCE(v_patch_rec.qr_payload_json, '{}'::jsonb),
        qr_generated_at = v_patch_rec.qr_generated_at,
        qr_scanned_at = v_patch_rec.qr_scanned_at,
        qr_scan_info_json = v_patch_rec.qr_scan_info_json,
        qr_r2_key = v_patch_rec.qr_r2_key,
        day_references_json = v_patch_rec.day_references_json,
        manual_pdf_rotation_degrees = COALESCE(v_patch_rec.manual_pdf_rotation_degrees, 0),
        qr_last_sent_hash = v_patch_rec.qr_last_sent_hash,
        qr_last_sent_at_utc = v_patch_rec.qr_last_sent_at_utc,
        qr_signed_hash = v_patch_rec.qr_signed_hash,
        qr_signed_at_utc = v_patch_rec.qr_signed_at_utc,
        is_adjustment = CASE
          WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
          ELSE COALESCE(v_patch_rec.is_adjustment, ts.is_adjustment)
        END
    WHERE ts.timesheet_id = v_current_ts.timesheet_id
      AND ts.is_current = true
    RETURNING * INTO v_current_ts;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
    END IF;
  END IF;

  v_week_patch_rec := jsonb_populate_record(v_week, v_week_patch_json);

  UPDATE public.contract_weeks AS cw
  SET status = COALESCE(v_week_patch_rec.status, cw.status),
      submission_mode_snapshot = COALESCE(v_week_patch_rec.submission_mode_snapshot, cw.submission_mode_snapshot),
      timesheet_id = v_current_ts.timesheet_id,
      uploaded_pdf_r2_key = COALESCE(v_week_patch_rec.uploaded_pdf_r2_key, cw.uploaded_pdf_r2_key),
      day_entries_json = COALESCE(v_week_patch_rec.day_entries_json, cw.day_entries_json),
      totals_json = COALESCE(v_week_patch_rec.totals_json, cw.totals_json),
      updated_at = CASE
        WHEN v_week_patch_json ? 'updated_at' THEN COALESCE(v_week_patch_rec.updated_at, v_now)
        ELSE v_now
      END,
      planned_schedule_json = COALESCE(v_week_patch_rec.planned_schedule_json, cw.planned_schedule_json),
      is_adjustment = CASE
        WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
        ELSE COALESCE(v_week_patch_rec.is_adjustment, cw.is_adjustment)
      END,
      enforce_day_partition = COALESCE(v_week_patch_rec.enforce_day_partition, cw.enforce_day_partition),
      allowed_days_mask = COALESCE(v_week_patch_rec.allowed_days_mask, cw.allowed_days_mask),
      split_boundary_date = COALESCE(v_week_patch_rec.split_boundary_date, cw.split_boundary_date),
      worker_note = COALESCE(v_week_patch_rec.worker_note, cw.worker_note),
      split_group_key = COALESCE(v_week_patch_rec.split_group_key, cw.split_group_key)
  WHERE cw.id = v_week.id
  RETURNING * INTO v_week;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_WEEK_UPDATE_FAILED';
  END IF;

  IF v_created_now AND COALESCE(p_materialise_staged_evidence, true) THEN
    FOR v_queue_item IN
      SELECT *
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND mq.meta_json->>'contract_week_id' = v_week.id::text
      ORDER BY mq.uploaded_at_utc ASC, mq.id ASC
      FOR UPDATE
    LOOP
      v_queue_kind := upper(
        COALESCE(
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'staged_kind', '')), ''),
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'kind', '')), ''),
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'attached_kind', '')), ''),
          'TIMESHEET'
        )
      );

      IF v_queue_kind NOT IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER') THEN
        v_queue_kind := 'OTHER';
      END IF;

      v_queue_storage_key := NULLIF(
        BTRIM(
          COALESCE(
            NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'canonical_key', '')), ''),
            ''
          )
        ),
        ''
      );
      IF v_queue_storage_key IS NOT NULL THEN
        v_queue_storage_key := NULLIF(regexp_replace(v_queue_storage_key, '^/+', ''), '');
      END IF;

      IF v_queue_kind = 'TIMESHEET' THEN
        IF v_queue_storage_key IS NULL THEN
          RAISE EXCEPTION USING
            MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
            DETAIL = jsonb_build_object(
              'reason', 'missing_storage_key',
              'queue_id', v_queue_item.id::text,
              'contract_week_id', v_week.id::text
            )::text;
        END IF;

        IF v_primary_timesheet_storage_key IS NULL THEN
          v_primary_timesheet_storage_key := v_queue_storage_key;
          v_primary_timesheet_queue_id := v_queue_item.id;
          v_primary_timesheet_rotation_raw := COALESCE(v_queue_item.last_rotation_deg, 0);
          v_primary_timesheet_rotation_raw := ((v_primary_timesheet_rotation_raw % 360) + 360) % 360;
          v_primary_timesheet_rotation_deg :=
            CASE
              WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0
              WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90
              WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180
              ELSE 270
            END;
        ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
          RAISE EXCEPTION USING
            MESSAGE = 'MULTIPLE_STAGED_TIMESHEET_FILES',
            DETAIL = jsonb_build_object(
              'reason', 'different_storage_key_conflict',
              'contract_week_id', v_week.id::text,
              'existing_storage_key', v_primary_timesheet_storage_key,
              'conflicting_storage_key', v_queue_storage_key,
              'queue_id', v_queue_item.id::text
            )::text;
        END IF;

        v_timesheet_kind_count := v_timesheet_kind_count + 1;

        IF v_timesheet_kind_count = 1 THEN
          IF NOT EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te
            WHERE te.timesheet_id = v_current_ts.timesheet_id
              AND te.kind = 'TIMESHEET'
              AND te.storage_key = v_queue_storage_key
            LIMIT 1
          ) THEN
            INSERT INTO public.timesheet_evidence (
              timesheet_id,
              kind,
              display_name,
              storage_key,
              created_at,
              created_by
            )
            VALUES (
              v_current_ts.timesheet_id,
              'TIMESHEET',
              v_queue_item.original_filename,
              v_queue_storage_key,
              COALESCE(v_queue_item.uploaded_at_utc, v_now),
              COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id)
            );
          END IF;
        END IF;

        UPDATE public.manual_timesheet_queue AS mq
        SET status = 'ATTACHED',
            timesheet_id = v_current_ts.timesheet_id,
            r2_key = v_queue_storage_key,
            meta_json = (
              COALESCE(v_queue_item.meta_json, '{}'::jsonb)
                - 'deferred_target_timesheet_id'
                - 'materialisation_deferred_at_utc'
                - 'deferred_rotation_degrees'
                - 'dematerialised_from_timesheet_id'
                - 'dematerialised_from_booking_id'
                - 'dematerialised_at_utc'
            ) || jsonb_build_object(
              'contract_week_id', v_week.id::text,
              'staged_kind', 'TIMESHEET',
              'materialisation_deferred_to_backend', false,
              'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
              'materialised_at_utc', to_jsonb(v_now),
              'materialised_storage_key', v_queue_storage_key,
              'duplicate_timesheet_evidence_identity', (v_timesheet_kind_count > 1),
              'duplicate_of_queue_item_id', CASE WHEN v_timesheet_kind_count > 1 THEN v_primary_timesheet_queue_id::text ELSE NULL END,
              'materialisation_noop_reason', CASE WHEN v_timesheet_kind_count > 1 THEN 'same_storage_key_duplicate' ELSE NULL END
            )
        WHERE mq.id = v_queue_item.id;
      ELSE
        INSERT INTO public.timesheet_evidence (
          timesheet_id,
          kind,
          display_name,
          storage_key,
          created_at,
          created_by
        )
        VALUES (
          v_current_ts.timesheet_id,
          v_queue_kind,
          v_queue_item.original_filename,
          COALESCE(v_queue_storage_key, v_queue_item.r2_key),
          COALESCE(v_queue_item.uploaded_at_utc, v_now),
          COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id)
        );

        UPDATE public.manual_timesheet_queue AS mq
        SET status = 'ATTACHED',
            timesheet_id = v_current_ts.timesheet_id,
            meta_json = (
              COALESCE(v_queue_item.meta_json, '{}'::jsonb)
                - 'deferred_target_timesheet_id'
                - 'materialisation_deferred_at_utc'
                - 'deferred_rotation_degrees'
            ) || jsonb_build_object(
              'contract_week_id', v_week.id::text,
              'staged_kind', v_queue_kind,
              'materialisation_deferred_to_backend', false,
              'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
              'materialised_at_utc', to_jsonb(v_now),
              'materialised_storage_key', COALESCE(v_queue_storage_key, v_queue_item.r2_key)
            )
        WHERE mq.id = v_queue_item.id;
      END IF;
    END LOOP;
  END IF;

  IF v_primary_timesheet_storage_key IS NOT NULL THEN
    UPDATE public.timesheets AS ts
    SET manual_pdf_r2_key = v_primary_timesheet_storage_key,
        manual_pdf_rotation_degrees = v_primary_timesheet_rotation_deg,
        updated_at = v_now
    WHERE ts.timesheet_id = v_current_ts.timesheet_id
      AND ts.is_current = true
    RETURNING * INTO v_current_ts;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_MANUAL_PDF_UPDATE_FAILED';
    END IF;
  END IF;

  IF v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION 'p_tsfin_snapshot_json is required';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'client_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'client_id', '')), '')::uuid IS DISTINCT FROM v_contract.client_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'client_id',
        'expected_value', v_contract.client_id::text,
        'supplied_value', v_tsfin_snapshot_json->>'client_id',
        'contract_week_id', v_week.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  IF v_contract.candidate_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'candidate_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'candidate_id', '')), '')::uuid IS DISTINCT FROM v_contract.candidate_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'candidate_id',
        'expected_value', v_contract.candidate_id::text,
        'supplied_value', v_tsfin_snapshot_json->>'candidate_id',
        'contract_week_id', v_week.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  -- Do not compare the intended next TSFIN snapshot to the old current TSFIN for
  -- mutable financial values such as hours, rates, totals, expenses, mileage, or margin.
  -- Stale protection for this write is enforced by expected_timesheet_id, backend row
  -- signature, authorisation/invoice/payment locks, and invariant candidate/client checks.

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_units', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'MILEAGE'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'mileage', 'required_kind', 'MILEAGE')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'travel_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'travel_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'TRAVEL'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'travel', 'required_kind', 'TRAVEL')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'accommodation_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'accommodation_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'ACCOMMODATION'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'accommodation', 'required_kind', 'ACCOMMODATION')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'other_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'other_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'OTHER'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'other', 'required_kind', 'OTHER')
      );
    END IF;
  END IF;

  IF jsonb_array_length(v_missing) > 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EVIDENCE_REQUIRED',
      DETAIL = jsonb_build_object('missing', v_missing)::text;
  END IF;

  v_tsfin_snapshot_json :=
    v_tsfin_snapshot_json
    || jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id::text,
      'timesheet_version', v_current_ts.version
    );

  PERFORM public.enqueue_ts_financials_priority(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum
  );

  SELECT *
  INTO v_outbox
  FROM public.tsfin_dequeue_specific(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    1
  )
  LIMIT 1;

  IF v_outbox.id IS NULL THEN
    RAISE EXCEPTION 'TSFIN_OUTBOX_DEQUEUE_FAILED';
  END IF;

  SELECT wr.ok_count, wr.fail_count, wr.errors
  INTO v_write_result
  FROM public.tsfin_write_snapshots_and_complete(
    jsonb_build_array(
      jsonb_build_object(
        'outbox_id', v_outbox.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text,
        'snapshot', v_tsfin_snapshot_json
      )
    )
  ) AS wr;

  IF COALESCE(v_write_result.fail_count, 0) > 0 OR COALESCE(v_write_result.ok_count, 0) <> 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_WRITE_FAILED',
      DETAIL = COALESCE(v_write_result.errors, '[]'::jsonb)::text;
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  SELECT decision_result.row_json
    INTO v_post_decision_row
  FROM public.bulk_timesheet_row_decision_v1(JSONB_BUILD_OBJECT(
    'dataset_mode', 'process',
    'timesheet_id', v_current_ts.timesheet_id::text,
    'contract_week_id', v_week.id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_new_row_signature := NULLIF(BTRIM(COALESCE(v_post_decision_row->>'row_signature', '')), '');

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    CASE
      WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED'
      ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED'
    END,
    JSONB_BUILD_OBJECT(
      'actor_user_id', p_actor_user_id,
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'current_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_previous_row_signature,
      'row', COALESCE(v_pre_decision_row, '{}'::jsonb)
    ),
    JSONB_BUILD_OBJECT(
      'actor_user_id', p_actor_user_id,
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'new_processing_status', v_current_tsfin.processing_status,
      'new_row_signature', v_new_row_signature,
      'operation', CASE WHEN v_created_now THEN 'CREATE_PROCESS' ELSE 'UPDATE_PROCESS' END,
      'row', COALESCE(v_post_decision_row, '{}'::jsonb)
    ),
    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );

  contract_week_id := v_week.id;
  contract_id := v_week.contract_id;
  timesheet_id := v_current_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_timesheet_version := v_current_ts.version;
  was_stale := v_was_stale;
  created_now := v_created_now;
  processing_status := v_current_tsfin.processing_status;
  contract_week_json := to_jsonb(v_week);
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS TABLE(
  booking_id text,
  requested_timesheet_id uuid,
  current_timesheet_id uuid,
  current_version integer,
  was_stale boolean,
  processing_status_before public.ts_fin_processing_status_enum,
  processing_status_after public.ts_fin_processing_status_enum,
  timesheet_json jsonb,
  timesheet_financials_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_segment_invoice_locked boolean := false;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  SELECT *
  INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
  END IF;

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT *
    INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_requested_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        v_current_ts := v_requested_ts;
      END IF;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NO_TSFIN';
  END IF;

  SELECT (
    COALESCE(upper(v_current_tsfin.invoice_breakdown_json ->> 'mode') = 'SEGMENTS', false)
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_current_tsfin.invoice_breakdown_json -> 'segments', '[]'::jsonb)) AS seg(segment_json)
      WHERE NULLIF(btrim(seg.segment_json ->> 'invoice_locked_invoice_id'), '') IS NOT NULL
      LIMIT 1
    )
  )
  INTO v_segment_invoice_locked;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
     OR v_current_tsfin.paid_at_utc IS NOT NULL
     OR COALESCE(v_segment_invoice_locked, false) THEN
    RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  UPDATE public.timesheets AS ts
  SET authorised_at_server = NULL,
      updated_at = v_now
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  RETURNING * INTO v_current_ts;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
  END IF;

  UPDATE public.timesheets_financials AS tf
  SET processing_status = v_new_status,
      authorised_by_user_id = NULL,
      authorised_at_utc = NULL,
      updated_at = v_now
  WHERE tf.id = v_current_tsfin.id
  RETURNING * INTO v_current_tsfin;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TSFIN_UPDATE_FAILED';
  END IF;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    SELECT *
    INTO v_contract_week
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = v_current_ts.timesheet_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT cw.*
      INTO v_contract_week
      FROM public.contract_weeks AS cw
      JOIN public.timesheets AS tw
        ON tw.timesheet_id = cw.timesheet_id
      WHERE tw.booking_id = v_current_ts.booking_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
      LIMIT 1
      FOR UPDATE OF cw;
    END IF;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET';
    END IF;

    UPDATE public.contract_weeks AS cw
    SET timesheet_id = v_current_ts.timesheet_id,
        status = 'SUBMITTED'::public.contract_week_status_enum,
        updated_at = v_now
    WHERE cw.id = v_contract_week.id
    RETURNING * INTO v_contract_week;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CONTRACT_WEEK_STATUS_UPDATE_FAILED';
    END IF;
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  booking_id := v_current_ts.booking_id;
  requested_timesheet_id := v_requested_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_version := v_current_ts.version;
  was_stale := v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id;
  processing_status_before := v_prev_status;
  processing_status_after := v_current_tsfin.processing_status;
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS TABLE(
  booking_id text,
  requested_timesheet_id uuid,
  current_timesheet_id uuid,
  current_version integer,
  was_stale boolean,
  processing_status_before public.ts_fin_processing_status_enum,
  processing_status_after public.ts_fin_processing_status_enum,
  validation_status text,
  validation_pre_validated boolean,
  hr_validation_required_for_invoice boolean,
  timesheet_authorised_before timestamptz,
  timesheet_json jsonb,
  timesheet_financials_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum;
  v_new_status public.ts_fin_processing_status_enum;
  v_force_ready_for_invoice boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_has_segment_invoice_lock boolean := false;
  v_timesheet_authorised_before timestamptz := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required';
  END IF;

  SELECT *
  INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
  END IF;

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT *
    INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_requested_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        v_current_ts := v_requested_ts;
      END IF;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NO_TSFIN';
  END IF;

  v_prev_status := v_current_tsfin.processing_status;
  v_timesheet_authorised_before := v_current_ts.authorised_at_server;

  v_has_segment_invoice_lock := EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array'
         AND upper(COALESCE(v_current_tsfin.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
        THEN v_current_tsfin.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS seg(value)
    WHERE NULLIF(BTRIM(COALESCE(seg.value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  );

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
     OR v_current_tsfin.paid_at_utc IS NOT NULL
     OR v_has_segment_invoice_lock THEN
    RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
  END IF;

  SELECT
    COALESCE(vts.client_requires_hr, false),
    COALESCE(vts.hr_validation_required_for_invoice, false),
    CASE
      WHEN vts.validation_status IS NULL THEN NULL
      ELSE upper(vts.validation_status::text)
    END
  INTO
    v_client_requires_hr,
    v_hr_validation_required_for_invoice,
    v_validation_status
  FROM public.v_timesheets_summary_base AS vts
  WHERE vts.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  SELECT COALESCE(tv.pre_validated, false)
  INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_force_ready_for_invoice :=
    v_current_tsfin.basis IN (
      'NHSP'::public.timesheet_fin_basis_enum,
      'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum,
      'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum,
      'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum
    );

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_must_hold_for_hr_validation := v_hr_validation_required_for_invoice AND NOT v_validation_ok;
  v_prevalidated_fast_track := v_validation_pre_validated AND v_validation_ok;

  v_new_status := v_current_tsfin.processing_status;

  IF v_prev_status IN (
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    'READY_FOR_HR'::public.ts_fin_processing_status_enum
  ) THEN
    IF v_must_hold_for_hr_validation THEN
      v_new_status := 'READY_FOR_HR'::public.ts_fin_processing_status_enum;
    ELSE
      v_new_status :=
        CASE
          WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          WHEN v_client_requires_hr THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
          ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        END;
    END IF;
  END IF;

  UPDATE public.timesheets AS ts
  SET authorised_at_server = v_now,
      updated_at = v_now
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  RETURNING * INTO v_current_ts;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
  END IF;

  UPDATE public.timesheets_financials AS tf
  SET processing_status = v_new_status,
      authorised_by_user_id = p_actor_user_id,
      authorised_at_utc = v_now,
      updated_at = v_now
  WHERE tf.id = v_current_tsfin.id
  RETURNING * INTO v_current_tsfin;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TSFIN_UPDATE_FAILED';
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  booking_id := v_current_ts.booking_id;
  requested_timesheet_id := v_requested_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_version := v_current_ts.version;
  was_stale := v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id;
  processing_status_before := v_prev_status;
  processing_status_after := v_current_tsfin.processing_status;
  validation_status := v_validation_status;
  validation_pre_validated := v_validation_pre_validated;
  hr_validation_required_for_invoice := v_hr_validation_required_for_invoice;
  timesheet_authorised_before := v_timesheet_authorised_before;
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;


DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);

DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);


CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(
  p_week_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_timesheet_create_json jsonb DEFAULT NULL,
  p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb,
  p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb,
  p_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_rotation_json jsonb DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_materialise_staged_evidence boolean DEFAULT true,
  p_now_utc timestamp with time zone DEFAULT now(),
  p_expected_row_signature text DEFAULT NULL,
  p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_next_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_response_context text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_contract_week_id uuid := NULL;
  v_contract_id uuid := NULL;
  v_timesheet_id uuid := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_timesheet_version integer := NULL;
  v_was_stale boolean := FALSE;
  v_created_now boolean := FALSE;
  v_processing_status public.ts_fin_processing_status_enum := NULL;
  v_contract_week_json jsonb := NULL;
  v_timesheet_json jsonb := NULL;
  v_timesheet_financials_json jsonb := NULL;
  v_pre_row jsonb := NULL;
  v_post_row jsonb := NULL;
  v_row_patch jsonb := '{}'::jsonb;
  v_cache_invalidation_hints jsonb := '{}'::jsonb;
  v_cache_invalidation jsonb := '{}'::jsonb;
  v_pre_bucket text := NULL;
  v_post_bucket text := NULL;
  v_pre_authorise_section text := NULL;
  v_post_authorise_section text := NULL;
  v_unprocessed_delta integer := 0;
  v_processed_delta integer := 0;
  v_total_delta integer := 0;
  v_previous_storage_key text := NULL;
  v_primary_storage_key text := NULL;
  v_primary_artifact_id text := NULL;
  v_primary_artifact_kind text := NULL;
  v_primary_artifact_display_name text := NULL;
  v_primary_artifact_preview_mode text := NULL;
  v_row_key text := NULL;
  v_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_current_row_signature text := NULL;
  v_previous_row_key text := NULL;
  v_preview_changed boolean := FALSE;
  v_error_sqlstate text := NULL;
  v_error_message text := NULL;
  v_error_code text := NULL;
  v_exception_detail text := NULL;
  v_exception_hint text := NULL;
  v_detail_json jsonb := NULL;
  v_missing jsonb := '[]'::jsonb;
  v_error_current_timesheet_id uuid := NULL;
  v_rotation_action text := NULL;
  v_rotation_applied boolean := FALSE;
  v_rotation_old_timesheet_id text := NULL;
  v_rotation_new_timesheet_id text := NULL;
  v_rotation_old_version integer := NULL;
  v_rotation_new_version integer := NULL;
  v_pre_current_timesheet_id_text text := NULL;
  v_pre_current_version_text text := NULL;
  v_patch_dataset_mode text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'authorise'
    ELSE 'process'
  END;
  v_pre_patch_filters jsonb := '{}'::jsonb;
  v_post_patch_filters jsonb := '{}'::jsonb;
  v_atomic_guard_row jsonb := NULL;
  v_atomic_expected_row_signature text := NULL;
  v_response_context text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise'
    ELSE 'bulk_process'
  END;
  v_operation text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise'
    ELSE 'contract_week_manual_upsert_bulk_process'
  END;
  v_next_tsfin_snapshot_json jsonb := COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json);
  v_is_additional_manual_adjustment boolean := FALSE;
  v_additional_seq integer := 0;
  v_post_route_type text := NULL;
  v_post_route_family text := NULL;
  v_post_route_subfamily text := NULL;
  v_effective_actual_schedule_json jsonb := NULL;
  v_effective_planned_schedule_json jsonb := NULL;
  v_effective_total_hours numeric := NULL;
  v_keep_empty_additional_schedule boolean := FALSE;
  v_defer_post_patch boolean := LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_process_defer_patch';
BEGIN
  IF p_week_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  IF p_rotation_json IS NOT NULL AND jsonb_typeof(p_rotation_json) = 'object' THEN
    v_rotation_action := UPPER(BTRIM(COALESCE(p_rotation_json->>'qr_action', '')));
    IF v_rotation_action = '' THEN
      v_rotation_action := NULL;
    END IF;
  END IF;

  v_pre_patch_filters := JSONB_BUILD_OBJECT(
    'dataset_mode', v_patch_dataset_mode,
    'contract_week_id', p_week_id::text
  );

  IF p_expected_timesheet_id IS NOT NULL THEN
    v_pre_patch_filters := v_pre_patch_filters || JSONB_BUILD_OBJECT(
      'timesheet_id', p_expected_timesheet_id::text
    );
  END IF;

  SELECT decision_result.row_json
    INTO v_pre_row
  FROM public.bulk_timesheet_row_patch_v1(v_pre_patch_filters) AS decision_result(row_json)
  LIMIT 1;

  v_pre_bucket := NULLIF(BTRIM(COALESCE(v_pre_row->>'bulk_process_bucket', '')), '');
  v_pre_authorise_section := NULLIF(BTRIM(COALESCE(v_pre_row->>'bulk_authorise_section', '')), '');
  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_pre_row->>'row_key', '')), '');
  v_previous_storage_key := NULLIF(BTRIM(COALESCE(v_pre_row->>'primary_artifact_storage_key', '')), '');
  v_pre_current_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_pre_row->>'current_timesheet_id', v_pre_row->>'timesheet_id', p_expected_timesheet_id::text, '')), '');
  v_pre_current_version_text := NULLIF(BTRIM(COALESCE(v_pre_row->>'current_version', v_pre_row->>'current_timesheet_version', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_pre_row->>'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND v_pre_row IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'ROW_SIGNATURE_LOOKUP_FAILED',
      'message', 'The current row could not be resolved for the supplied timesheet and contract week. Refresh the row and try again.',
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', NULL,
      'current_timesheet_id', p_expected_timesheet_id,
      'contract_week_id', p_week_id,
      'previous_row_key', CASE WHEN p_expected_timesheet_id IS NULL THEN NULL ELSE 'timesheet:' || p_expected_timesheet_id::text END,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY('timesheet:' || p_expected_timesheet_id::text) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(p_expected_timesheet_id) END,
        'storage_keys', '[]'::jsonb,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', 'timesheet:' || p_expected_timesheet_id::text,
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(p_expected_timesheet_id) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  IF v_expected_row_signature IS NOT NULL
     AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Contract week row changed after it was loaded. Refresh the row and try again.',
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_current_row_signature,
      'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_pre_row->>'current_timesheet_id', v_pre_row->>'timesheet_id', '')), '')::uuid,
      'contract_week_id', p_week_id,
      'previous_row_key', v_previous_row_key,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text) END,
        'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  IF v_pre_current_version_text ~ '^[0-9]+$' THEN
    v_rotation_old_version := v_pre_current_version_text::integer;
  END IF;

  SELECT upsert_result.contract_week_id,
         upsert_result.contract_id,
         upsert_result.timesheet_id,
         upsert_result.current_timesheet_id,
         upsert_result.current_timesheet_version,
         upsert_result.was_stale,
         upsert_result.created_now,
         upsert_result.processing_status,
         upsert_result.contract_week_json,
         upsert_result.timesheet_json,
         upsert_result.timesheet_financials_json
    INTO v_contract_week_id,
         v_contract_id,
         v_timesheet_id,
         v_current_timesheet_id,
         v_current_timesheet_version,
         v_was_stale,
         v_created_now,
         v_processing_status,
         v_contract_week_json,
         v_timesheet_json,
         v_timesheet_financials_json
  FROM public.contract_week_manual_upsert_atomic(
    p_week_id => p_week_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_timesheet_create_json => p_timesheet_create_json,
    p_timesheet_patch_json => p_timesheet_patch_json,
    p_contract_week_patch_json => p_contract_week_patch_json,
    p_tsfin_snapshot_json => v_next_tsfin_snapshot_json,
    p_rotation_json => p_rotation_json,
    p_actor_user_id => p_actor_user_id,
    p_materialise_staged_evidence => p_materialise_staged_evidence,
    p_now_utc => v_now,
    p_expected_row_signature => NULL
  ) AS upsert_result;

  IF v_contract_week_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'UPSERT_RETURNED_NO_ROW',
      'message', 'contract_week_manual_upsert_atomic did not return a row.',
      'contract_week_id', p_week_id
    );
  END IF;

  IF v_defer_post_patch THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', TRUE,
      'operation', v_operation,
      'success', TRUE,
      'contract_week_id', v_contract_week_id,
      'contract_id', v_contract_id,
      'timesheet_id', v_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'expected_timesheet_id', v_current_timesheet_id,
      'current_version', v_current_timesheet_version,
      'current_timesheet_version', v_current_timesheet_version,
      'was_stale', COALESCE(v_was_stale, FALSE),
      'created_now', COALESCE(v_created_now, FALSE),
      'processing_status', v_processing_status,
      'contract_week', COALESCE(v_contract_week_json, NULL::jsonb),
      'contract_week_json', COALESCE(v_contract_week_json, NULL::jsonb),
      'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
      'timesheet_json', COALESCE(v_timesheet_json, NULL::jsonb),
      'tsfin', COALESCE(v_timesheet_financials_json, NULL::jsonb),
      'timesheet_financials_json', COALESCE(v_timesheet_financials_json, NULL::jsonb),
      'previous_row_key', v_previous_row_key,
      'requires_final_patch_refresh', TRUE,
      'deferred_bulk_patch', TRUE,
      'response_context', v_response_context,
      'bulk_authorise', FALSE,
      'bulk_process', TRUE,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'identity_changed', TRUE,
        'manual_changed', TRUE,
        'evidence_changed', FALSE,
        'storage_changed', FALSE,
        'invalidate_context', FALSE,
        'invalidate_row_context', FALSE,
        'invalidate_editor_context', TRUE,
        'invalidate_preview', FALSE,
        'invalidate_evidence', FALSE,
        'refresh_required', TRUE,
        'requires_final_patch_refresh', TRUE,
        'deferred_bulk_patch', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', v_contract_week_id,
          'timesheet_id', v_current_timesheet_id
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE,
        'requires_final_patch_refresh', TRUE,
        'deferred_bulk_patch', TRUE
      )
    );
  END IF;

  v_post_patch_filters := JSONB_BUILD_OBJECT(
    'dataset_mode', v_patch_dataset_mode,
    'contract_week_id', v_contract_week_id::text
  );

  IF v_current_timesheet_id IS NOT NULL THEN
    v_post_patch_filters := v_post_patch_filters || JSONB_BUILD_OBJECT(
      'timesheet_id', v_current_timesheet_id::text
    );
  END IF;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_patch_v1(v_post_patch_filters) AS decision_result(row_json)
  LIMIT 1;

  IF v_post_row IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'POST_DECISION_ROW_NOT_FOUND',
      'message', 'bulk_timesheet_row_patch_v1 did not return a post-upsert row.',
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'previous_row_key', v_previous_row_key,
      'rotation_applied', FALSE,
      'old_timesheet_id', CASE WHEN p_rotation_json IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', JSONB_BUILD_ARRAY(v_previous_row_key),
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'storage_keys', JSONB_BUILD_ARRAY(v_previous_storage_key),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', v_contract_week_id,
          'timesheet_id', v_current_timesheet_id
        )),
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  v_is_additional_manual_adjustment :=
    COALESCE(NULLIF(BTRIM(COALESCE(v_timesheet_json->>'is_adjustment', '')), '')::boolean, FALSE) = TRUE
    OR COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'is_adjustment', '')), '')::boolean, FALSE) = TRUE
    OR COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'additional_seq', '')), '')::integer, 0) > 0;

  v_additional_seq := COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'additional_seq', '')), '')::integer, 0);

  v_effective_actual_schedule_json := CASE
    WHEN jsonb_typeof(v_timesheet_json->'actual_schedule_json') = 'array' THEN v_timesheet_json->'actual_schedule_json'
    WHEN jsonb_typeof(v_timesheet_financials_json->'actual_schedule_json') = 'array' THEN v_timesheet_financials_json->'actual_schedule_json'
    ELSE COALESCE(v_post_row->'actual_schedule_json', '[]'::jsonb)
  END;

  v_effective_planned_schedule_json := CASE
    WHEN jsonb_typeof(v_contract_week_json->'planned_schedule_json') = 'array' THEN v_contract_week_json->'planned_schedule_json'
    ELSE COALESCE(v_post_row->'planned_schedule_json', '[]'::jsonb)
  END;

  v_effective_total_hours := COALESCE(
    NULLIF(BTRIM(COALESCE(v_timesheet_financials_json->>'total_hours', '')), '')::numeric,
    NULLIF(BTRIM(COALESCE(v_post_row->>'total_hours', '')), '')::numeric,
    0::numeric
  );

  v_keep_empty_additional_schedule :=
    COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
    AND COALESCE(v_effective_total_hours, 0::numeric) = 0::numeric
    AND (
      v_effective_actual_schedule_json IS NULL
      OR CASE
        WHEN jsonb_typeof(v_effective_actual_schedule_json) = 'array' THEN jsonb_array_length(v_effective_actual_schedule_json) = 0
        ELSE FALSE
      END
    )
    AND (
      v_effective_planned_schedule_json IS NULL
      OR CASE
        WHEN jsonb_typeof(v_effective_planned_schedule_json) = 'array' THEN jsonb_array_length(v_effective_planned_schedule_json) = 0
        ELSE FALSE
      END
    );

  IF COALESCE(v_keep_empty_additional_schedule, FALSE) = TRUE THEN
    v_effective_actual_schedule_json := '[]'::jsonb;
    v_effective_planned_schedule_json := '[]'::jsonb;
    v_effective_total_hours := 0::numeric;
  END IF;

  v_post_route_type := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
      AND (
        UPPER(COALESCE(v_post_row->>'contract_weekly_mode', '')) = 'NHSP'
        OR UPPER(COALESCE(v_post_row->>'route_type', '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT')
        OR UPPER(COALESCE(v_timesheet_financials_json->>'basis', '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
        OR COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'client_is_nhsp', '')), '')::boolean, FALSE) = TRUE
      ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
      AND (
        UPPER(COALESCE(v_post_row->>'contract_weekly_mode', '')) = 'HEALTHROSTER'
        OR UPPER(COALESCE(v_post_row->>'route_type', '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT')
        OR UPPER(COALESCE(v_timesheet_financials_json->>'basis', '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
        OR COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'client_autoprocess_hr', '')), '')::boolean, FALSE) = TRUE
      ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
    ELSE v_post_row->>'route_type'
  END;

  v_post_route_family := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN 'MANUAL_NON_QR'
    ELSE v_post_row->>'route_family'
  END;

  v_post_route_subfamily := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN 'MANUAL_NON_QR'
    ELSE v_post_row->>'route_subfamily'
  END;

  v_post_row := v_post_row || JSONB_BUILD_OBJECT(
    'route_type', v_post_route_type,
    'route_family', v_post_route_family,
    'route_subfamily', v_post_route_subfamily,
    'underlying_channel_family', v_post_route_subfamily,
    'is_import_authoritative', CASE WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN FALSE ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'is_import_authoritative', '')), '')::boolean, FALSE) END,
    'is_adjustment', COALESCE(v_is_additional_manual_adjustment, FALSE),
    'additional_seq', v_additional_seq,
    'actual_schedule_json', COALESCE(v_effective_actual_schedule_json, '[]'::jsonb),
    'planned_schedule_json', COALESCE(v_effective_planned_schedule_json, '[]'::jsonb),
    'total_hours', COALESCE(v_effective_total_hours, 0::numeric),
    'suppress_standard_schedule_fallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
    'keep_additional_manual_adjustment_schedule_empty', COALESCE(v_keep_empty_additional_schedule, FALSE),
    '__suppressStandardScheduleFallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
    '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(v_keep_empty_additional_schedule, FALSE)
  );

  v_post_bucket := NULLIF(BTRIM(COALESCE(v_post_row->>'bulk_process_bucket', '')), '');
  v_post_authorise_section := NULLIF(BTRIM(COALESCE(v_post_row->>'bulk_authorise_section', '')), '');
  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');
  v_primary_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_primary_artifact_id := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_id', '')), '');
  v_primary_artifact_kind := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_kind', '')), '');
  v_primary_artifact_display_name := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_display_name', '')), '');
  v_primary_artifact_preview_mode := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_preview_mode', '')), '');
  v_preview_changed := COALESCE(v_previous_storage_key, '') IS DISTINCT FROM COALESCE(v_primary_storage_key, '');

  v_rotation_applied := p_rotation_json IS NOT NULL
    AND NULLIF(BTRIM(COALESCE(v_pre_current_timesheet_id_text, '')), '') IS NOT NULL
    AND COALESCE(v_pre_current_timesheet_id_text, '') IS DISTINCT FROM COALESCE(v_current_timesheet_id::text, '');

  IF v_rotation_applied THEN
    v_rotation_old_timesheet_id := v_pre_current_timesheet_id_text;
    v_rotation_new_timesheet_id := v_current_timesheet_id::text;
    v_rotation_new_version := v_current_timesheet_version;
  ELSE
    v_rotation_old_timesheet_id := NULL;
    v_rotation_new_timesheet_id := NULL;
    v_rotation_old_version := NULL;
    v_rotation_new_version := NULL;
  END IF;

  v_total_delta := CASE
    WHEN v_pre_row IS NULL AND v_post_row IS NOT NULL THEN 1
    WHEN v_pre_row IS NOT NULL AND v_post_row IS NULL THEN -1
    ELSE 0
  END;

  v_unprocessed_delta := CASE
    WHEN COALESCE(v_pre_bucket, '') = 'UNPROCESSED' AND COALESCE(v_post_bucket, '') <> 'UNPROCESSED' THEN -1
    WHEN COALESCE(v_pre_bucket, '') <> 'UNPROCESSED' AND COALESCE(v_post_bucket, '') = 'UNPROCESSED' THEN 1
    ELSE 0
  END;

  v_processed_delta := CASE
    WHEN COALESCE(v_pre_bucket, '') = 'PROCESSED' AND COALESCE(v_post_bucket, '') <> 'PROCESSED' THEN -1
    WHEN COALESCE(v_pre_bucket, '') <> 'PROCESSED' AND COALESCE(v_post_bucket, '') = 'PROCESSED' THEN 1
    ELSE 0
  END;

  v_row_patch := COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT())
    || JSONB_BUILD_OBJECT(
      'previous_row_key', v_previous_row_key,
      'row_key', v_row_key,
      'new_row_key', v_row_key,
      'stable_row_id', v_post_row->>'stable_row_id',
      'contract_week_id', v_contract_week_id,
      'contract_id', v_contract_id,
      'timesheet_id', v_current_timesheet_id,
      'is_adjustment', COALESCE(v_is_additional_manual_adjustment, FALSE),
      'additional_seq', v_additional_seq,
      'route_type', v_post_route_type,
      'route_family', v_post_route_family,
      'route_subfamily', v_post_route_subfamily,
      'underlying_channel_family', v_post_route_subfamily,
      'is_import_authoritative', CASE WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN FALSE ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'is_import_authoritative', '')), '')::boolean, FALSE) END,
      'actual_schedule_json', COALESCE(v_effective_actual_schedule_json, '[]'::jsonb),
      'planned_schedule_json', COALESCE(v_effective_planned_schedule_json, '[]'::jsonb),
      'total_hours', COALESCE(v_effective_total_hours, 0::numeric),
      'suppress_standard_schedule_fallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
      'keep_additional_manual_adjustment_schedule_empty', COALESCE(v_keep_empty_additional_schedule, FALSE),
      '__suppressStandardScheduleFallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
      '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(v_keep_empty_additional_schedule, FALSE),
      'current_timesheet_id', v_current_timesheet_id,
      'expected_timesheet_id', v_current_timesheet_id,
      'current_version', v_current_timesheet_version,
      'current_timesheet_version', v_current_timesheet_version,
      'row_signature', v_row_signature,
      'backend_row_signature', v_row_signature,
      'row_backend_signature', v_row_signature,
      'previous_bulk_process_bucket', v_pre_bucket,
      'bulk_process_bucket', v_post_bucket,
      'previous_bulk_authorise_section', v_pre_authorise_section,
      'bulk_authorise_section', v_post_authorise_section,
      'bucket_transition', JSONB_BUILD_OBJECT(
        'from', v_pre_bucket,
        'to', v_post_bucket,
        'changed', COALESCE(v_pre_bucket, '') IS DISTINCT FROM COALESCE(v_post_bucket, '')
      ),
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'rotation_applied', v_rotation_applied,
      'old_timesheet_id', v_rotation_old_timesheet_id,
      'new_timesheet_id', v_rotation_new_timesheet_id,
      'qr_action', v_rotation_action
    );

  v_cache_invalidation_hints := JSONB_BUILD_OBJECT(
    'row_keys', JSONB_BUILD_ARRAY(v_previous_row_key, v_row_key),
    'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
    'timesheet_ids', JSONB_BUILD_ARRAY(v_rotation_old_timesheet_id, v_timesheet_id, v_current_timesheet_id),
    'storage_keys', CASE WHEN v_preview_changed THEN JSONB_BUILD_ARRAY(v_previous_storage_key, v_primary_storage_key) ELSE '[]'::jsonb END,
    'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
    'row_signature', v_row_signature,
    'backend_row_signature', v_row_signature,
    'row_backend_signature', v_row_signature,
    'identity_changed', TRUE,
    'manual_changed', TRUE,
    'evidence_changed', v_preview_changed,
    'storage_changed', v_preview_changed,
    'invalidate_context', FALSE,
    'invalidate_row_context', FALSE,
    'invalidate_editor_context', TRUE,
    'invalidate_preview', v_preview_changed,
    'invalidate_evidence', v_preview_changed
  );

  v_cache_invalidation := JSONB_BUILD_OBJECT(
    'rows', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
      'previous_row_key', v_previous_row_key,
      'row_key', v_row_key,
      'new_row_key', v_row_key,
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_current_timesheet_id,
      'old_timesheet_id', v_rotation_old_timesheet_id,
      'new_row_signature', v_row_signature
    )),
    'artifacts', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_current_timesheet_id,
      'previous_storage_key', v_previous_storage_key,
      'storage_key', v_primary_storage_key,
      'changed', v_preview_changed
    )),
    'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise')
  );

  RETURN JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'operation', v_operation,
    'success', TRUE,
    'contract_week_id', v_contract_week_id,
    'contract_id', v_contract_id,
    'timesheet_id', v_timesheet_id,
    'current_timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_timesheet_version,
    'current_timesheet_version', v_current_timesheet_version,
    'was_stale', COALESCE(v_was_stale, FALSE),
    'created_now', COALESCE(v_created_now, FALSE),
    'rotation_applied', v_rotation_applied,
    'rotation_action', v_rotation_action,
    'old_timesheet_id', v_rotation_old_timesheet_id,
    'new_timesheet_id', v_rotation_new_timesheet_id,
    'old_version', v_rotation_old_version,
    'new_version', v_rotation_new_version,
    'processing_status', v_processing_status,
    'previous_bulk_process_bucket', v_pre_bucket,
    'bulk_process_bucket', v_post_bucket,
    'previous_bulk_authorise_section', v_pre_authorise_section,
    'bulk_authorise_section', v_post_authorise_section,
    'bucket_transition', JSONB_BUILD_OBJECT(
      'from', v_pre_bucket,
      'to', v_post_bucket,
      'changed', COALESCE(v_pre_bucket, '') IS DISTINCT FROM COALESCE(v_post_bucket, '')
    ),
    'contract_week', COALESCE(v_contract_week_json, NULL::jsonb),
    'contract_week_json', COALESCE(v_contract_week_json, NULL::jsonb),
    'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
    'timesheet_json', COALESCE(v_timesheet_json, NULL::jsonb),
    'tsfin', COALESCE(v_timesheet_financials_json, NULL::jsonb),
    'timesheet_financials_json', COALESCE(v_timesheet_financials_json, NULL::jsonb),
    'row_patch', v_row_patch,
    'row_patches', JSONB_BUILD_ARRAY(v_row_patch),
    'data_row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'row_key', v_row_key,
    'new_row_key', v_row_key,
    'previous_row_key', v_previous_row_key,
    'row_signature', v_row_signature,
    'backend_row_signature', v_row_signature,
    'row_backend_signature', v_row_signature,
    'response_context', v_response_context,
    'bulk_authorise', v_response_context = 'bulk_authorise',
    'bulk_process', v_response_context = 'bulk_process',
    'primary_artifact', CASE
      WHEN v_primary_artifact_id IS NOT NULL OR v_primary_storage_key IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'id', v_primary_artifact_id,
        'kind', v_primary_artifact_kind,
        'display_name', v_primary_artifact_display_name,
        'storage_key', v_primary_storage_key,
        'previous_storage_key', v_previous_storage_key,
        'preview_mode', v_primary_artifact_preview_mode,
        'changed', v_preview_changed
      )
      ELSE NULL::jsonb
    END,
    'evidence_hints', JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'attached_evidence_count', COALESCE(NULLIF(v_post_row->>'attached_evidence_count', '')::integer, 0),
      'queue_staged_count', COALESCE(NULLIF(v_post_row->>'queue_staged_count', '')::integer, 0),
      'evidence_document_locked', COALESCE(NULLIF(v_post_row->>'evidence_document_locked', '')::boolean, FALSE),
      'evidence_lock_reason', v_post_row->>'evidence_lock_reason'
    ),
    'preview_hints', JSONB_BUILD_OBJECT(
      'primary_artifact_id', v_primary_artifact_id,
      'primary_artifact_kind', v_primary_artifact_kind,
      'primary_artifact_display_name', v_primary_artifact_display_name,
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'preview_storage_key', v_primary_storage_key,
      'primary_left_pane_mode', v_post_row->>'primary_left_pane_mode',
      'changed', v_preview_changed
    ),
    'artifact_hints', COALESCE(v_post_row->'artifact_hints', JSONB_BUILD_OBJECT(
      'route_family', v_post_row->>'route_family',
      'route_subfamily', v_post_row->>'route_subfamily',
      'underlying_channel_family', v_post_row->>'underlying_channel_family',
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'changed', v_preview_changed
    )),
    'count_deltas', JSONB_BUILD_OBJECT(
      'unprocessed', v_unprocessed_delta,
      'processed', v_processed_delta,
      'total', v_total_delta
    ),
    'cache_invalidation_hints', v_cache_invalidation_hints,
    'cache_invalidation', v_cache_invalidation
  );
EXCEPTION WHEN OTHERS THEN
  v_error_sqlstate := SQLSTATE;
  v_error_message := SQLERRM;
  v_error_code := CASE
    WHEN SQLSTATE = 'P0001' AND NULLIF(BTRIM(SQLERRM), '') IS NOT NULL THEN SQLERRM
    ELSE SQLSTATE
  END;

  GET STACKED DIAGNOSTICS
    v_exception_detail = PG_EXCEPTION_DETAIL,
    v_exception_hint = PG_EXCEPTION_HINT;

  IF v_error_sqlstate = '23505'
     AND (
       v_error_message ILIKE '%timesheets_booking_id_version_uidx%'
       OR COALESCE(v_exception_detail, '') ILIKE '%timesheets_booking_id_version_uidx%'
     ) THEN
    v_error_code := 'BOOKING_ID_COLLISION';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_exception_detail, '')), '') IS NOT NULL THEN
    BEGIN
      v_detail_json := v_exception_detail::jsonb;
      IF jsonb_typeof(v_detail_json->'missing') = 'array' THEN
        v_missing := v_detail_json->'missing';
      END IF;
      IF NULLIF(BTRIM(COALESCE(v_detail_json->>'current_timesheet_id', '')), '') IS NOT NULL THEN
        v_error_current_timesheet_id := (v_detail_json->>'current_timesheet_id')::uuid;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_detail_json := NULL;
      v_missing := '[]'::jsonb;
      v_error_current_timesheet_id := NULL;
    END;
  END IF;

  RETURN JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'operation', v_operation,
    'success', FALSE,
    'error_code', v_error_code,
    'sqlstate', v_error_sqlstate,
    'message', v_error_message,
    'detail', v_exception_detail,
    'detail_json', v_detail_json,
    'hint', v_exception_hint,
    'missing', v_missing,
    'supplied_booking_id', v_detail_json->>'supplied_booking_id',
    'booking_id', v_detail_json->>'booking_id',
    'existing_timesheet_id', v_detail_json->>'existing_timesheet_id',
    'existing_contract_id', v_detail_json->>'existing_contract_id',
    'target_contract_id', v_detail_json->>'target_contract_id',
    'target_week_ending_date', v_detail_json->>'target_week_ending_date',
    'contract_week_id', p_week_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', COALESCE(v_error_current_timesheet_id, p_expected_timesheet_id),
    'rotation_action', v_rotation_action,
    'rotation_applied', FALSE,
    'old_timesheet_id', CASE WHEN p_rotation_json IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END,
    'previous_row_key', v_previous_row_key,
    'response_context', v_response_context,
    'bulk_authorise', v_response_context = 'bulk_authorise',
    'bulk_process', v_response_context = 'bulk_process',
    'refresh_required', TRUE,
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
      'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
      'timesheet_ids', JSONB_BUILD_ARRAY(p_expected_timesheet_id, v_pre_current_timesheet_id_text),
      'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'invalidate_context', TRUE,
      'invalidate_preview', FALSE,
      'refresh_required', TRUE
    ),
    'cache_invalidation', JSONB_BUILD_OBJECT(
      'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'previous_row_key', v_previous_row_key,
        'contract_week_id', p_week_id,
        'timesheet_id', COALESCE(v_error_current_timesheet_id, p_expected_timesheet_id)
      )) END,
      'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
      'timesheet_ids', JSONB_BUILD_ARRAY(p_expected_timesheet_id, v_pre_current_timesheet_id_text),
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'refresh_required', TRUE
    )
  );
END;
$function$;






CREATE OR REPLACE FUNCTION public.cloudtms_format_london_date(p_date date)
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  IF p_date IS NULL THEN
    RETURN 'Not recorded';
  END IF;

  RETURN TO_CHAR(p_date, 'FMDD FMMonth YYYY');
END;
$function$;

CREATE OR REPLACE FUNCTION public.cloudtms_format_gbp(p_amount numeric)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_amount numeric;
  v_abs_amount numeric;
  v_formatted text;
BEGIN
  IF p_amount IS NULL THEN
    RETURN '—';
  END IF;

  v_amount := round(p_amount, 2);
  v_abs_amount := abs(v_amount);
  v_formatted := to_char(v_abs_amount, 'FM999,999,999,999,999,999,999,990.00');

  IF v_amount < 0 THEN
    RETURN '-£' || v_formatted;
  END IF;

  RETURN '£' || v_formatted;
END;
$function$;


CREATE OR REPLACE FUNCTION public.cloudtms_format_london_datetime(p_ts timestamptz)
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_local_ts timestamp without time zone;
BEGIN
  IF p_ts IS NULL THEN
    RETURN 'Not recorded';
  END IF;

  v_local_ts := p_ts AT TIME ZONE 'Europe/London';

  RETURN TO_CHAR(v_local_ts, 'FMDD FMMonth YYYY "at" HH24:MI "hrs"') || ' (UK time)';
END;
$function$;


