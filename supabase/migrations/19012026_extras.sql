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
  -- Guard: if current snapshot is locked by an invoice, refuse writes.
  -- IMPORTANT: In SEGMENTS mode, locked_by_invoice_id may be NULL even when some segments are invoiced
  -- (e.g. split across multiple invoices). So we must also block if ANY segment has invoice_locked_invoice_id.
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
  -- TIMESHEET -> INVOICES (segment-safe)
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
            'paid_at_utc', p.paid_at_utc
          )
        ), '[]'::jsonb) as items
      from page p
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
  -- CANDIDATE -> INVOICES (segment-safe)
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
          'paid_at_utc', p.paid_at_utc
        )
      ), '[]'::jsonb) as items
    into v_total, v_items
    from page p;

    return jsonb_build_object('items', v_items, 'total', coalesce(v_total, 0));
  end if;

  -- ------------------------------------------------------------
  -- UMBRELLA -> INVOICES (segment-safe)
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
          'paid_at_utc', p.paid_at_utc
        )
      ), '[]'::jsonb) as items
    into v_total, v_items
    from page p;

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
      tsfin_ts_ids as (
        select distinct timesheet_id
        from tsfin_rows
        where timesheet_id is not null
      ),
      contract_rows as (
        select c.id as contract_id, c.client_id
        from public.contracts c
        where c.candidate_id = p_id
      ),
      contract_ids as (
        select contract_id from contract_rows
      ),
      contract_weeks as (
        select cw.id as contract_week_id, cw.timesheet_id
        from public.contract_weeks cw
        where cw.contract_id in (select contract_id from contract_ids)
      ),
      extra_timesheets as (
        select count(*)::int as extra_count
        from contract_weeks cw
        where cw.timesheet_id is null
           or cw.timesheet_id not in (select timesheet_id from tsfin_ts_ids)
      ),
      base_tsfin_count as (
        select count(*)::int as base_count
        from tsfin_rows
      ),
      timesheets_total as (
        select (select base_count from base_tsfin_count) + (select extra_count from extra_timesheets) as total
      ),
      clients_distinct as (
        select distinct client_id
        from tsfin_rows
        where client_id is not null
        union
        select distinct client_id
        from contract_rows
        where client_id is not null
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
      cand_timesheet_ids as (
        -- union TS IDs from current TSFIN and any timesheets linked via contracts
        select distinct tf.timesheet_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.candidate_id = p_id
          and tf.timesheet_id is not null
        union
        select distinct ts.timesheet_id
        from public.timesheets ts
        join public.contracts c on c.id = ts.contract_id
        where c.candidate_id = p_id
          and ts.timesheet_id is not null
      ),
      invoice_ids as (
        select distinct il.invoice_id
        from public.invoice_lines il
        where il.timesheet_id in (select timesheet_id from cand_timesheet_ids)
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
      tsfin_ts_ids as (
        select distinct timesheet_id
        from tsfin_rows
        where timesheet_id is not null
      ),
      contract_rows as (
        select c.id as contract_id, c.candidate_id
        from public.contracts c
        where c.client_id = p_id
      ),
      contract_ids as (
        select contract_id from contract_rows
      ),
      contract_weeks as (
        select cw.id as contract_week_id, cw.timesheet_id
        from public.contract_weeks cw
        where cw.contract_id in (select contract_id from contract_ids)
      ),
      extra_timesheets as (
        select count(*)::int as extra_count
        from contract_weeks cw
        where cw.timesheet_id is null
           or cw.timesheet_id not in (select timesheet_id from tsfin_ts_ids)
      ),
      base_tsfin_count as (
        select count(*)::int as base_count
        from tsfin_rows
      ),
      timesheets_total as (
        select (select base_count from base_tsfin_count) + (select extra_count from extra_timesheets) as total
      ),
      cand_distinct as (
        select distinct candidate_id
        from tsfin_rows
        where candidate_id is not null
        union
        select distinct candidate_id
        from contract_rows
        where candidate_id is not null
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
      tsfin_rows as (
        select tf.timesheet_id
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.candidate_id in (select candidate_id from cand_ids)
          and tf.timesheet_id is not null
      ),
      tsfin_ts_ids as (
        select distinct timesheet_id
        from tsfin_rows
      ),
      contract_rows as (
        select c.id as contract_id
        from public.contracts c
        where c.candidate_id in (select candidate_id from cand_ids)
      ),
      contract_ids as (
        select contract_id from contract_rows
      ),
      contract_weeks as (
        select cw.id as contract_week_id, cw.timesheet_id
        from public.contract_weeks cw
        where cw.contract_id in (select contract_id from contract_ids)
      ),
      extra_timesheets as (
        select count(*)::int as extra_count
        from contract_weeks cw
        where cw.timesheet_id is null
           or cw.timesheet_id not in (select timesheet_id from tsfin_ts_ids)
      ),
      base_tsfin_count as (
        select count(*)::int as base_count
        from tsfin_rows
      ),
      timesheets_total as (
        select (select base_count from base_tsfin_count) + (select extra_count from extra_timesheets) as total
      ),
      inv_ids as (
        select distinct il.invoice_id
        from public.invoice_lines il
        where il.timesheet_id in (select timesheet_id from tsfin_ts_ids)
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

