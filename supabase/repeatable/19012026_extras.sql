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
