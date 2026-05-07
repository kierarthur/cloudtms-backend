-- ============================================================
-- CloudTMS: Summary Totals RPCs (Invoices / Timesheets / Contracts)
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION (idempotent)
--
-- These RPCs are designed to return totals for ALL rows matching the
-- same filter semantics used by the list endpoints / summary grids,
-- independent of paging.
--
-- Functions:
--   1) public.invoice_list_totals(p_filters jsonb)
--   2) public.timesheet_list_totals(p_filters jsonb)
--   3) public.contract_list_count(p_filters jsonb)   (optional)
--
-- Notes:
-- - Filters are passed as JSONB (same keys as your UI/backend).
-- - We deliberately do NOT apply paging (limit/offset) here.
-- ============================================================


-- ============================================================
-- 1) invoice_list_totals(filters jsonb)
-- Mirrors handleListInvoices() filter semantics:
--   client_id, status (enum list or legacy paid/unpaid), q (invoice_no ilike),
--   issued_from/to, due_from/to, created_from/to
-- Margin is computed as SUM(invoice_lines.margin_ex_vat) for matching invoices.
-- ============================================================
create or replace function public.invoice_list_totals(p_filters jsonb)
returns table (
  count_all bigint,
  subtotal_ex_vat_sum numeric,
  total_inc_vat_sum numeric,
  margin_ex_vat_sum numeric
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_client_id uuid := null;
  v_q text := null;

  v_issued_from timestamptz := null;
  v_issued_to   timestamptz := null;
  v_due_from    timestamptz := null;
  v_due_to      timestamptz := null;
  v_created_from timestamptz := null;
  v_created_to   timestamptz := null;

  -- NEW: week-ending filters (date)
  v_week_ending_from date := null;
  v_week_ending_to   date := null;

  v_status_raw jsonb := null;
  v_status_list text[] := null;
  v_legacy_paid_filter text := null; -- 'paid' | 'unpaid' | null
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  -- client_id
  begin
    if (p_filters ? 'client_id') and nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then
      v_client_id := (p_filters->>'client_id')::uuid;
    end if;
  exception when others then
    v_client_id := null;
  end;

  -- q (partial invoice_no)
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  -- dates (accept ISO date or timestamptz strings)
  begin if nullif(btrim(coalesce(p_filters->>'issued_from','')), '') is not null then v_issued_from := (p_filters->>'issued_from')::timestamptz; end if; exception when others then v_issued_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'issued_to','')), '') is not null then v_issued_to := (p_filters->>'issued_to')::timestamptz; end if; exception when others then v_issued_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_from','')), '') is not null then v_due_from := (p_filters->>'due_from')::timestamptz; end if; exception when others then v_due_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_to','')), '') is not null then v_due_to := (p_filters->>'due_to')::timestamptz; end if; exception when others then v_due_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;

  -- NEW: week ending (accept ISO date strings; ignore invalid)
  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_from','')), '') is not null then
      v_week_ending_from := (p_filters->>'week_ending_from')::date;
    end if;
  exception when others then
    v_week_ending_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_to','')), '') is not null then
      v_week_ending_to := (p_filters->>'week_ending_to')::date;
    end if;
  exception when others then
    v_week_ending_to := null;
  end;

  -- status logic:
  -- - p_filters.status may be array OR comma string OR single enum string OR 'paid'/'unpaid'
  if p_filters ? 'status' then
    v_status_raw := p_filters->'status';
  end if;

  if v_status_raw is not null then
    if jsonb_typeof(v_status_raw) = 'array' then
      select array_agg(upper(btrim(x))) into v_status_list
      from (
        select jsonb_array_elements_text(v_status_raw) as x
      ) s
      where nullif(btrim(coalesce(x,'')), '') is not null;
    else
      -- string / scalar (trim JSON quotes safely)
      v_status_list := array_remove(
        string_to_array(
          upper(btrim(trim(both '"' from coalesce(v_status_raw::text,'')))),
          ','
        ),
        ''
      );
    end if;
  end if;

  -- legacy paid/unpaid filter support
  if v_status_list is not null and array_length(v_status_list,1) = 1 then
    if lower(v_status_list[1]) = 'paid' or v_status_list[1] = 'PAID' then
      v_legacy_paid_filter := 'paid';
      v_status_list := null;
    elsif lower(v_status_list[1]) = 'unpaid' or v_status_list[1] = 'UNPAID' then
      v_legacy_paid_filter := 'unpaid';
      v_status_list := null;
    end if;
  end if;

  -- Sanitize enum status list (DRAFT/ISSUED/ON_HOLD/PAID)
  if v_status_list is not null then
    select array_agg(s)
    into v_status_list
    from (
      select distinct
        replace(
          replace(
            replace(
              replace(upper(btrim(x)),'(',''),')',''
            ),',',''
          ),'"',''
        ) as s
      from unnest(v_status_list) x
    ) t
    where s in ('DRAFT','ISSUED','ON_HOLD','PAID');

    if v_status_list is null or coalesce(array_length(v_status_list,1),0) = 0 then
      v_status_list := null;
    end if;
  end if;

  return query
  with inv as (
    select
      i.id,
      i.subtotal_ex_vat,
      i.total_inc_vat
    from public.invoices i
    where (v_client_id is null or i.client_id = v_client_id)
      and (v_q is null or i.invoice_no ilike ('%'||v_q||'%'))
      and (v_issued_from is null or i.issued_at_utc >= v_issued_from)
      and (v_issued_to   is null or i.issued_at_utc <= v_issued_to)
      and (v_due_from    is null or i.due_at_utc >= v_due_from)
      and (v_due_to      is null or i.due_at_utc <= v_due_to)
      and (v_created_from is null or i.created_at >= v_created_from)
      and (v_created_to   is null or i.created_at <= v_created_to)
      and (
        v_legacy_paid_filter is null
        or (v_legacy_paid_filter = 'paid' and i.paid_at_utc is not null)
        or (v_legacy_paid_filter = 'unpaid' and i.paid_at_utc is null)
      )
      and (
        v_status_list is null
        or i.status::text = any(v_status_list)
      )
      -- NEW: Week ending range filter (matches list endpoint semantics: via invoice_lines → timesheets)
      and (
        (v_week_ending_from is null and v_week_ending_to is null)
        or exists (
          select 1
          from public.invoice_lines l
          join public.timesheets t
            on t.timesheet_id = l.timesheet_id
          where l.invoice_id = i.id
            and (v_week_ending_from is null or t.week_ending_date >= v_week_ending_from)
            and (v_week_ending_to   is null or t.week_ending_date <= v_week_ending_to)
        )
      )
  ),
  m as (
    select
      l.invoice_id,
      sum(coalesce(l.margin_ex_vat,0))::numeric as margin_sum
    from public.invoice_lines l
    join inv on inv.id = l.invoice_id
    group by l.invoice_id
  )
  select
    count(*)::bigint as count_all,
    coalesce(sum(inv.subtotal_ex_vat),0)::numeric as subtotal_ex_vat_sum,
    coalesce(sum(inv.total_inc_vat),0)::numeric as total_inc_vat_sum,
    coalesce(sum(coalesce(m.margin_sum,0)),0)::numeric as margin_ex_vat_sum
  from inv
  left join m on m.invoice_id = inv.id;

end;
$$;


-- ============================================================
-- 2) timesheet_list_totals(filters jsonb)
-- Mirrors handleTimesheetsSummary() filter semantics against v_timesheets_summary.
-- Supported filter keys (subset):
--   client_id, candidate_id, summary_stage,
--   route_type (ALL/ELECTRONIC/MANUAL/NHSP/HEALTHROSTER/QR),
--   sheet_scope (ALL/WEEKLY/DAILY),
--   qr_status,
--   week_ending_from/to,
--   is_adjusted, is_qr, needs_attention,
--   candidate_paid (true), client_invoiced (true),
--   hr_issue (maps to hr_crosscheck_issues contains),
--   processing_status (comma list; ALL means no filter)
--   status_code (UI-only helper: NO_MATCH_ID/RATE_MISSING/PAY_CHAN_MISS/READY_FOR_HR/READY_FOR_INV)
-- Totals computed from v_timesheets_summary fields: total_pay_ex_vat, margin_ex_vat.
-- ============================================================

CREATE OR REPLACE FUNCTION public.timesheet_list_totals(p_filters jsonb)
RETURNS TABLE (
  count_all bigint,
  total_pay_ex_vat_sum numeric,
  margin_ex_vat_sum numeric
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := '{}'::jsonb;

  v_client_id uuid := NULL;
  v_candidate_id uuid := NULL;
  v_summary_stage text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_we_from date := NULL;
  v_we_to date := NULL;
  v_is_adjusted text := NULL;
  v_is_qr text := NULL;
  v_needs_attention text := NULL;
  v_candidate_paid text := NULL;
  v_client_invoiced text := NULL;
  v_hr_issue text := NULL;
  v_proc_status_raw text := NULL;
  v_proc_list text[] := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;
  v_q text := NULL;
  v_q_like text := NULL;
  v_ids text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_has_client_filter boolean := FALSE;
  v_has_candidate_filter boolean := FALSE;
BEGIN
  v_source_filters :=
    v_filters
    - 'q'
    - 'query'
    - 'name'
    - 'client_id'
    - 'clientId'
    - 'candidate_id'
    - 'candidateId'
    - 'week_ending_from'
    - 'weekEndingFrom'
    - 'week_ending_to'
    - 'weekEndingTo'
    - 'route_type'
    - 'routeType'
    - 'issues_filter'
    - 'issuesFilter'
    - 'status_code'
    - 'statusCode'
    - 'summary_stage'
    - 'summaryStage'
    - 'client_invoiced'
    - 'clientInvoiced'
    - 'needs_attention'
    - 'needsAttention'
    || jsonb_build_object('disable_paging', TRUE, 'purpose', 'totals');

  v_has_client_filter := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '') IS NOT NULL;
  IF v_has_client_filter THEN
    IF COALESCE(v_filters->>'client_id', v_filters->>'clientId') ~* v_uuid_re THEN
      v_client_id := COALESCE(v_filters->>'client_id', v_filters->>'clientId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_has_candidate_filter := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '') IS NOT NULL;
  IF v_has_candidate_filter THEN
    IF COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId') ~* v_uuid_re THEN
      v_candidate_id := COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  BEGIN
    IF v_filters ? 'ids' THEN
      IF jsonb_typeof(v_filters->'ids') = 'array' THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS id_value
          FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(split_values.value), '') AS id_value
          FROM unnest(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_ids := NULL;
  END;

  v_summary_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'summary_stage', v_filters->>'summaryStage', '')), ''));
  IF v_summary_stage = 'ALL' THEN v_summary_stage := NULL; END IF;

  v_tools_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'ALL' THEN v_tools_stage := NULL; END IF;

  v_issues_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'ALL' THEN v_issues_filter := NULL; END IF;

  v_route_type := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  IF v_route_type = 'ALL' THEN v_route_type := NULL; END IF;

  v_sheet_scope := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_sheet_scope = 'ALL' THEN v_sheet_scope := NULL; END IF;

  v_qr_status := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  IF v_qr_status = 'ALL' THEN v_qr_status := NULL; END IF;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '') IS NOT NULL THEN
      v_we_from := COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_from := NULL;
  END;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '') IS NOT NULL THEN
      v_we_to := COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_to := NULL;
  END;

  v_is_adjusted := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')), ''));
  v_is_qr := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')), ''));
  v_needs_attention := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'needs_attention', v_filters->>'needsAttention', '')), ''));
  v_candidate_paid := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')), ''));
  v_client_invoiced := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'client_invoiced', v_filters->>'clientInvoiced', '')), ''));

  v_hr_issue := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
  IF v_hr_issue = 'ALL' THEN v_hr_issue := NULL; END IF;

  v_proc_status_raw := NULLIF(BTRIM(COALESCE(v_filters->>'processing_status', v_filters->>'processingStatus', '')), '');
  IF v_proc_status_raw IS NOT NULL AND UPPER(v_proc_status_raw) <> 'ALL' THEN
    SELECT ARRAY_AGG(status_values.status_value)
    INTO v_proc_list
    FROM (
      SELECT NULLIF(BTRIM(UPPER(split_values.value)), '') AS status_value
      FROM unnest(regexp_split_to_array(v_proc_status_raw, '\s*,\s*')) AS split_values(value)
    ) AS status_values
    WHERE status_values.status_value IS NOT NULL;
  END IF;

  v_status_code := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  IF v_status_code = 'ALL' THEN v_status_code := NULL; END IF;

  RETURN QUERY
  WITH filtered_rows AS (
    SELECT
      summary_row.timesheet_id,
      summary_row.contract_week_id,
      COALESCE(summary_row.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(summary_row.margin_ex_vat, 0::numeric) AS margin_ex_vat
    FROM public.timesheet_summary_lightweight_rows_v1(v_source_filters) AS summary_row
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = summary_row.timesheet_id
     AND timesheet_row.is_current = TRUE
    WHERE

      (
        v_ids IS NULL
        OR summary_row.timesheet_id::text = ANY(v_ids)
        OR summary_row.contract_week_id::text = ANY(v_ids)
      )
      AND (v_client_id IS NULL OR summary_row.client_id = v_client_id)
      AND (v_candidate_id IS NULL OR summary_row.candidate_id = v_candidate_id)
      AND (
        v_q IS NULL
        OR COALESCE(summary_row.candidate_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.client_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.booking_id, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.occupant_key_norm, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.hospital_norm, '') ILIKE v_q_like ESCAPE '\'
      )
      AND (v_summary_stage IS NULL OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_summary_stage)
      AND (v_tools_stage IS NULL OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_tools_stage)
      AND (v_we_from IS NULL OR summary_row.week_ending_date >= v_we_from)
      AND (v_we_to IS NULL OR summary_row.week_ending_date <= v_we_to)
      AND (
        v_route_type IS NULL
        OR (v_route_type = 'ELECTRONIC' AND UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC'))
        OR (v_route_type = 'MANUAL' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL') OR UPPER(COALESCE(summary_row.route_family, '')) = 'MANUAL'))
        OR (v_route_type = 'NHSP' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') OR UPPER(COALESCE(summary_row.route_family, '')) = 'NHSP'))
        OR (v_route_type = 'HEALTHROSTER' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') OR UPPER(COALESCE(summary_row.route_family, '')) = 'HEALTHROSTER'))
        OR (v_route_type = 'QR' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_route_type IN ('NO_TIMESHEET_REQUIRED', 'NO-TIMESHEET-REQUIRED') AND (UPPER(COALESCE(summary_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED' OR UPPER(COALESCE(summary_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED' OR COALESCE(summary_row.client_no_timesheet_required, FALSE) = TRUE))
        OR UPPER(COALESCE(summary_row.route_type, '')) = v_route_type
        OR UPPER(COALESCE(summary_row.route_family, '')) = v_route_type
      )
      AND (v_sheet_scope IS NULL OR UPPER(COALESCE(summary_row.sheet_scope, '')) = v_sheet_scope)
      AND (v_qr_status IS NULL OR UPPER(COALESCE(summary_row.qr_status, '')) = v_qr_status)
      AND (
        v_is_adjusted IS NULL
        OR (v_is_adjusted = 'true' AND COALESCE(summary_row.is_adjusted, FALSE) = TRUE)
        OR (v_is_adjusted = 'false' AND COALESCE(summary_row.is_adjusted, FALSE) = FALSE)
      )
      AND (
        v_is_qr IS NULL
        OR (v_is_qr = 'true' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_is_qr = 'false' AND COALESCE(summary_row.is_qr, FALSE) = FALSE)
      )
      AND (
        v_needs_attention IS NULL
        OR (v_needs_attention = 'true' AND COALESCE(summary_row.needs_attention, FALSE) = TRUE)
        OR (v_needs_attention = 'false' AND COALESCE(summary_row.needs_attention, FALSE) = FALSE)
      )
      AND (
        v_candidate_paid IS NULL
        OR (v_candidate_paid = 'true' AND summary_row.pay_paid_at_utc IS NOT NULL)
        OR (v_candidate_paid = 'false' AND summary_row.pay_paid_at_utc IS NULL)
      )
      AND (
        v_client_invoiced IS NULL
        OR (v_client_invoiced = 'true' AND COALESCE(summary_row.invoice_segments_locked, 0) > 0)
        OR (v_client_invoiced = 'false' AND COALESCE(summary_row.invoice_segments_locked, 0) = 0)
      )
      AND (
        v_hr_issue IS NULL
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(summary_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue
        )
      )
      AND (
        v_proc_list IS NULL
        OR UPPER(COALESCE(summary_row.processing_status, '')) = ANY(v_proc_list)
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (
          v_issues_filter = 'RATE_MISSING'
          AND (
            COALESCE(summary_row.has_rate_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
          AND (
            COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
          AND (
            UPPER(COALESCE(summary_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
          )
        )
        OR (
          v_issues_filter IN ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'HR_HOURS_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'DUPLICATE_CONTRACTS'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter = 'REFERENCE_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING')
          )
        )
        OR (
          v_issues_filter = 'VALIDATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION'
          )
        )
        OR (
          v_issues_filter = 'AUTHORISATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION')
          )
        )
        OR (
          v_issues_filter = 'ON_HOLD'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD'
          )
        )
        OR (
          v_issues_filter = 'REFS_PDF_INVALID'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID'
          )
        )
        OR (
          v_issues_filter = 'QR_NOT_ISSUED'
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') = ''
          AND timesheet_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') <> ''
          AND timesheet_row.qr_generated_at IS NOT NULL
          AND timesheet_row.qr_scanned_at IS NULL
        )
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (v_status_code = 'RATE_MISSING' AND COALESCE(summary_row.has_rate_issue, FALSE) = TRUE)
        OR (v_status_code = 'PAY_CHAN_MISS' AND COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE)
        OR (v_status_code = 'READY_FOR_HR' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_HR')
        OR (v_status_code = 'READY_FOR_INV' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_INVOICE')
        OR UPPER(COALESCE(summary_row.processing_status, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_status_code
      )

  )
  SELECT
    COUNT(*)::bigint AS count_all,
    COALESCE(SUM(filtered_rows.total_pay_ex_vat), 0::numeric) AS total_pay_ex_vat_sum,
    COALESCE(SUM(filtered_rows.margin_ex_vat), 0::numeric) AS margin_ex_vat_sum
  FROM filtered_rows;
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_list_totals(p_filters jsonb)
RETURNS TABLE (
  count_all bigint,
  total_pay_ex_vat_sum numeric,
  margin_ex_vat_sum numeric
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := '{}'::jsonb;

  v_client_id uuid := NULL;
  v_candidate_id uuid := NULL;
  v_summary_stage text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_we_from date := NULL;
  v_we_to date := NULL;
  v_is_adjusted text := NULL;
  v_is_qr text := NULL;
  v_needs_attention text := NULL;
  v_candidate_paid text := NULL;
  v_client_invoiced text := NULL;
  v_hr_issue text := NULL;
  v_proc_status_raw text := NULL;
  v_proc_list text[] := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;
  v_q text := NULL;
  v_q_like text := NULL;
  v_ids text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_has_client_filter boolean := FALSE;
  v_has_candidate_filter boolean := FALSE;
BEGIN
  v_source_filters :=
    v_filters
    - 'q'
    - 'query'
    - 'name'
    - 'client_id'
    - 'clientId'
    - 'candidate_id'
    - 'candidateId'
    - 'week_ending_from'
    - 'weekEndingFrom'
    - 'week_ending_to'
    - 'weekEndingTo'
    - 'route_type'
    - 'routeType'
    - 'issues_filter'
    - 'issuesFilter'
    - 'status_code'
    - 'statusCode'
    - 'summary_stage'
    - 'summaryStage'
    - 'client_invoiced'
    - 'clientInvoiced'
    - 'needs_attention'
    - 'needsAttention'
    || jsonb_build_object('disable_paging', TRUE, 'purpose', 'totals');

  v_has_client_filter := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '') IS NOT NULL;
  IF v_has_client_filter THEN
    IF COALESCE(v_filters->>'client_id', v_filters->>'clientId') ~* v_uuid_re THEN
      v_client_id := COALESCE(v_filters->>'client_id', v_filters->>'clientId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_has_candidate_filter := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '') IS NOT NULL;
  IF v_has_candidate_filter THEN
    IF COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId') ~* v_uuid_re THEN
      v_candidate_id := COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  BEGIN
    IF v_filters ? 'ids' THEN
      IF jsonb_typeof(v_filters->'ids') = 'array' THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS id_value
          FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(split_values.value), '') AS id_value
          FROM unnest(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_ids := NULL;
  END;

  v_summary_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'summary_stage', v_filters->>'summaryStage', '')), ''));
  IF v_summary_stage = 'ALL' THEN v_summary_stage := NULL; END IF;

  v_tools_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'ALL' THEN v_tools_stage := NULL; END IF;

  v_issues_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'ALL' THEN v_issues_filter := NULL; END IF;

  v_route_type := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  IF v_route_type = 'ALL' THEN v_route_type := NULL; END IF;

  v_sheet_scope := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_sheet_scope = 'ALL' THEN v_sheet_scope := NULL; END IF;

  v_qr_status := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  IF v_qr_status = 'ALL' THEN v_qr_status := NULL; END IF;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '') IS NOT NULL THEN
      v_we_from := COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_from := NULL;
  END;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '') IS NOT NULL THEN
      v_we_to := COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_to := NULL;
  END;

  v_is_adjusted := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')), ''));
  v_is_qr := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')), ''));
  v_needs_attention := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'needs_attention', v_filters->>'needsAttention', '')), ''));
  v_candidate_paid := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')), ''));
  v_client_invoiced := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'client_invoiced', v_filters->>'clientInvoiced', '')), ''));

  v_hr_issue := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
  IF v_hr_issue = 'ALL' THEN v_hr_issue := NULL; END IF;

  v_proc_status_raw := NULLIF(BTRIM(COALESCE(v_filters->>'processing_status', v_filters->>'processingStatus', '')), '');
  IF v_proc_status_raw IS NOT NULL AND UPPER(v_proc_status_raw) <> 'ALL' THEN
    SELECT ARRAY_AGG(status_values.status_value)
    INTO v_proc_list
    FROM (
      SELECT NULLIF(BTRIM(UPPER(split_values.value)), '') AS status_value
      FROM unnest(regexp_split_to_array(v_proc_status_raw, '\s*,\s*')) AS split_values(value)
    ) AS status_values
    WHERE status_values.status_value IS NOT NULL;
  END IF;

  v_status_code := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  IF v_status_code = 'ALL' THEN v_status_code := NULL; END IF;

  RETURN QUERY
  WITH filtered_rows AS (
    SELECT
      summary_row.timesheet_id,
      summary_row.contract_week_id,
      COALESCE(summary_row.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(summary_row.margin_ex_vat, 0::numeric) AS margin_ex_vat
    FROM public.timesheet_summary_lightweight_rows_v1(v_source_filters) AS summary_row
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = summary_row.timesheet_id
     AND timesheet_row.is_current = TRUE
    WHERE

      (
        v_ids IS NULL
        OR summary_row.timesheet_id::text = ANY(v_ids)
        OR summary_row.contract_week_id::text = ANY(v_ids)
      )
      AND (v_client_id IS NULL OR summary_row.client_id = v_client_id)
      AND (v_candidate_id IS NULL OR summary_row.candidate_id = v_candidate_id)
      AND (
        v_q IS NULL
        OR COALESCE(summary_row.candidate_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.client_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.booking_id, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.occupant_key_norm, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.hospital_norm, '') ILIKE v_q_like ESCAPE '\'
      )
      AND (v_summary_stage IS NULL OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_summary_stage)
      AND (v_tools_stage IS NULL OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_tools_stage)
      AND (v_we_from IS NULL OR summary_row.week_ending_date >= v_we_from)
      AND (v_we_to IS NULL OR summary_row.week_ending_date <= v_we_to)
      AND (
        v_route_type IS NULL
        OR (v_route_type = 'ELECTRONIC' AND UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC'))
        OR (v_route_type = 'MANUAL' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL') OR UPPER(COALESCE(summary_row.route_family, '')) = 'MANUAL'))
        OR (v_route_type = 'NHSP' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') OR UPPER(COALESCE(summary_row.route_family, '')) = 'NHSP'))
        OR (v_route_type = 'HEALTHROSTER' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') OR UPPER(COALESCE(summary_row.route_family, '')) = 'HEALTHROSTER'))
        OR (v_route_type = 'QR' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_route_type IN ('NO_TIMESHEET_REQUIRED', 'NO-TIMESHEET-REQUIRED') AND (UPPER(COALESCE(summary_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED' OR UPPER(COALESCE(summary_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED' OR COALESCE(summary_row.client_no_timesheet_required, FALSE) = TRUE))
        OR UPPER(COALESCE(summary_row.route_type, '')) = v_route_type
        OR UPPER(COALESCE(summary_row.route_family, '')) = v_route_type
      )
      AND (v_sheet_scope IS NULL OR UPPER(COALESCE(summary_row.sheet_scope, '')) = v_sheet_scope)
      AND (v_qr_status IS NULL OR UPPER(COALESCE(summary_row.qr_status, '')) = v_qr_status)
      AND (
        v_is_adjusted IS NULL
        OR (v_is_adjusted = 'true' AND COALESCE(summary_row.is_adjusted, FALSE) = TRUE)
        OR (v_is_adjusted = 'false' AND COALESCE(summary_row.is_adjusted, FALSE) = FALSE)
      )
      AND (
        v_is_qr IS NULL
        OR (v_is_qr = 'true' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_is_qr = 'false' AND COALESCE(summary_row.is_qr, FALSE) = FALSE)
      )
      AND (
        v_needs_attention IS NULL
        OR (v_needs_attention = 'true' AND COALESCE(summary_row.needs_attention, FALSE) = TRUE)
        OR (v_needs_attention = 'false' AND COALESCE(summary_row.needs_attention, FALSE) = FALSE)
      )
      AND (
        v_candidate_paid IS NULL
        OR (v_candidate_paid = 'true' AND summary_row.pay_paid_at_utc IS NOT NULL)
        OR (v_candidate_paid = 'false' AND summary_row.pay_paid_at_utc IS NULL)
      )
      AND (
        v_client_invoiced IS NULL
        OR (v_client_invoiced = 'true' AND COALESCE(summary_row.invoice_segments_locked, 0) > 0)
        OR (v_client_invoiced = 'false' AND COALESCE(summary_row.invoice_segments_locked, 0) = 0)
      )
      AND (
        v_hr_issue IS NULL
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(summary_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue
        )
      )
      AND (
        v_proc_list IS NULL
        OR UPPER(COALESCE(summary_row.processing_status, '')) = ANY(v_proc_list)
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (
          v_issues_filter = 'RATE_MISSING'
          AND (
            COALESCE(summary_row.has_rate_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
          AND (
            COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
          AND (
            UPPER(COALESCE(summary_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
          )
        )
        OR (
          v_issues_filter IN ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'HR_HOURS_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'DUPLICATE_CONTRACTS'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter = 'REFERENCE_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING')
          )
        )
        OR (
          v_issues_filter = 'VALIDATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION'
          )
        )
        OR (
          v_issues_filter = 'AUTHORISATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION')
          )
        )
        OR (
          v_issues_filter = 'ON_HOLD'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD'
          )
        )
        OR (
          v_issues_filter = 'REFS_PDF_INVALID'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID'
          )
        )
        OR (
          v_issues_filter = 'QR_NOT_ISSUED'
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') = ''
          AND timesheet_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') <> ''
          AND timesheet_row.qr_generated_at IS NOT NULL
          AND timesheet_row.qr_scanned_at IS NULL
        )
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (v_status_code = 'RATE_MISSING' AND COALESCE(summary_row.has_rate_issue, FALSE) = TRUE)
        OR (v_status_code = 'PAY_CHAN_MISS' AND COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE)
        OR (v_status_code = 'READY_FOR_HR' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_HR')
        OR (v_status_code = 'READY_FOR_INV' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_INVOICE')
        OR UPPER(COALESCE(summary_row.processing_status, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_status_code
      )

  )
  SELECT
    COUNT(*)::bigint AS count_all,
    COALESCE(SUM(filtered_rows.total_pay_ex_vat), 0::numeric) AS total_pay_ex_vat_sum,
    COALESCE(SUM(filtered_rows.margin_ex_vat), 0::numeric) AS margin_ex_vat_sum
  FROM filtered_rows;
END;
$function$;




-- ============================================================
-- 3) contract_list_count(filters jsonb)  (optional)
-- Mirrors handleContractsList() core filtering and status=active/completed/unassigned.
-- This RPC returns ONLY count for all matching contracts (no paging).
-- ============================================================
create or replace function public.contract_list_count(p_filters jsonb)
returns table (
  count_all bigint
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_candidate_id uuid := null;
  v_client_id uuid := null;

  v_pay_method text := null;
  v_role_like text := null;
  v_band text := null;

  v_candidate_name_like text := null;
  v_client_name_like text := null;

  v_active_on date := null;

  v_auto_invoice text := null;
  v_q text := null;

  v_default_submission_mode text := null;
  v_week_ending_weekday_snapshot int := null;

  v_require_ref_pay text := null;
  v_require_ref_inv text := null;

  v_has_custom_labels text := null;

  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;

  v_start_from date := null;
  v_start_to date := null;
  v_end_from date := null;
  v_end_to date := null;

  v_mileage_pay_rate numeric := null;
  v_mileage_charge_rate numeric := null;

  v_status text := null; -- all|active|completed|unassigned

  v_ids jsonb := null;
  v_ids_arr uuid[] := null;

  INCOMPLETE_STATUSES text[] := array['OPEN','PLANNED','SUBMITTED','AUTHORISED'];
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;

  v_pay_method := nullif(btrim(coalesce(p_filters->>'pay_method_snapshot','')), '');
  if v_pay_method is not null then v_pay_method := upper(v_pay_method); end if;

  if nullif(btrim(coalesce(p_filters->>'role','')), '') is not null then
    v_role_like := '%' || (p_filters->>'role') || '%';
  end if;

  v_band := nullif(btrim(coalesce(p_filters->>'band','')), '');

  if nullif(btrim(coalesce(p_filters->>'candidate_name','')), '') is not null then
    v_candidate_name_like := '%' || (p_filters->>'candidate_name') || '%';
  end if;
  if nullif(btrim(coalesce(p_filters->>'client_name','')), '') is not null then
    v_client_name_like := '%' || (p_filters->>'client_name') || '%';
  end if;

  begin if nullif(btrim(coalesce(p_filters->>'active_on','')), '') is not null then v_active_on := (p_filters->>'active_on')::date; end if; exception when others then v_active_on := null; end;

  v_auto_invoice := nullif(btrim(coalesce(p_filters->>'auto_invoice','')), '');

  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'default_submission_mode','')), '');
  if v_default_submission_mode is null then
    v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'submission_mode','')), '');
  end if;
  if v_default_submission_mode is not null then v_default_submission_mode := upper(v_default_submission_mode); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_weekday_snapshot','')), '') is not null then v_week_ending_weekday_snapshot := (p_filters->>'week_ending_weekday_snapshot')::int; end if; exception when others then v_week_ending_weekday_snapshot := null; end;

  v_require_ref_pay := nullif(btrim(coalesce(p_filters->>'require_reference_to_pay','')), '');
  v_require_ref_inv := nullif(btrim(coalesce(p_filters->>'require_reference_to_invoice','')), '');

  v_has_custom_labels := nullif(btrim(coalesce(p_filters->>'has_custom_labels','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_from','')), '') is not null then v_updated_from := (p_filters->>'updated_from')::timestamptz; end if; exception when others then v_updated_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_to','')), '') is not null then v_updated_to := (p_filters->>'updated_to')::timestamptz; end if; exception when others then v_updated_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'start_date_from','')), '') is not null then v_start_from := (p_filters->>'start_date_from')::date; end if; exception when others then v_start_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'start_date_to','')), '') is not null then v_start_to := (p_filters->>'start_date_to')::date; end if; exception when others then v_start_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'end_date_from','')), '') is not null then v_end_from := (p_filters->>'end_date_from')::date; end if; exception when others then v_end_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_to','')), '') is not null then v_end_to := (p_filters->>'end_date_to')::date; end if; exception when others then v_end_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'mileage_pay_rate','')), '') is not null then v_mileage_pay_rate := (p_filters->>'mileage_pay_rate')::numeric; end if; exception when others then v_mileage_pay_rate := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate','')), '') is not null then v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric; end if; exception when others then v_mileage_charge_rate := null; end;

  v_status := lower(nullif(btrim(coalesce(p_filters->>'status','')), ''));
  if v_status is null then v_status := 'all'; end if;

  -- ids filter (focus selections): p_filters.ids can be array of uuids
  if p_filters ? 'ids' then
    v_ids := p_filters->'ids';
    if v_ids is not null and jsonb_typeof(v_ids)='array' then
      select array_agg((x)::uuid) into v_ids_arr
      from jsonb_array_elements_text(v_ids) x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    end if;
  end if;

  return query
  with base as (
    select
      c.*,
      cand.display_name as cand_display,
      cand.first_name as cand_first,
      cand.last_name as cand_last,
      cli.name as cli_name
    from public.contracts c
    left join public.candidates cand on cand.id = c.candidate_id
    left join public.clients cli on cli.id = c.client_id
    where (v_ids_arr is null or c.id = any(v_ids_arr))
      and (v_candidate_id is null or c.candidate_id = v_candidate_id)
      and (v_client_id is null or c.client_id = v_client_id)
      and (v_pay_method is null or upper(coalesce(c.pay_method_snapshot::text,'')) = v_pay_method)
      and (v_band is null or c.band = v_band)
      and (v_role_like is null or c.role ilike v_role_like)
      and (v_active_on is null or (c.start_date <= v_active_on and c.end_date >= v_active_on))
      and (
        v_auto_invoice is null
        or (lower(v_auto_invoice) = 'true' and c.auto_invoice = true)
        or (lower(v_auto_invoice) = 'false' and c.auto_invoice = false)
      )
      and (v_default_submission_mode is null or upper(coalesce(c.default_submission_mode::text,'')) = v_default_submission_mode)
      and (v_week_ending_weekday_snapshot is null or c.week_ending_weekday_snapshot = v_week_ending_weekday_snapshot)
      and (
        v_require_ref_pay is null
        or (lower(v_require_ref_pay) = 'true' and c.require_reference_to_pay = true)
        or (lower(v_require_ref_pay) = 'false' and c.require_reference_to_pay = false)
      )
      and (
        v_require_ref_inv is null
        or (lower(v_require_ref_inv) = 'true' and c.require_reference_to_invoice = true)
        or (lower(v_require_ref_inv) = 'false' and c.require_reference_to_invoice = false)
      )
      and (
        v_has_custom_labels is null
        or (lower(v_has_custom_labels) = 'true' and c.bucket_labels_json is not null)
        or (lower(v_has_custom_labels) = 'false' and c.bucket_labels_json is null)
      )
      and (v_created_from is null or c.created_at >= v_created_from)
      and (v_created_to is null or c.created_at <= v_created_to)
      and (v_updated_from is null or c.updated_at >= v_updated_from)
      and (v_updated_to is null or c.updated_at <= v_updated_to)
      and (v_start_from is null or c.start_date >= v_start_from)
      and (v_start_to is null or c.start_date <= v_start_to)
      and (v_end_from is null or c.end_date >= v_end_from)
      and (v_end_to is null or c.end_date <= v_end_to)
      and (v_mileage_pay_rate is null or c.mileage_pay_rate = v_mileage_pay_rate)
      and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)

      and (
        v_candidate_name_like is null
        or (coalesce(cand.display_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.first_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.last_name,'') ilike v_candidate_name_like)
      )
      and (
        v_client_name_like is null
        or (coalesce(cli.name,'') ilike v_client_name_like)
      )
      and (
        v_q is null
        or (
          coalesce(cli.name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.display_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.first_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.last_name,'') ilike ('%'||v_q||'%')
          or coalesce(c.role,'') ilike ('%'||v_q||'%')
        )
      )
  ),
  filtered as (
    select b.*
    from base b
    where
      -- status semantics:
      (v_status = 'all')
      or (v_status = 'unassigned' and b.candidate_id is null)
      or (
        v_status = 'active'
        and exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
      or (
        v_status = 'completed'
        and b.candidate_id is not null
        and not exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
  )
  select count(*)::bigint as count_all
  from filtered;
end;
$$;
create or replace function public.candidate_list_ids(p_filters jsonb)
returns table (
  id uuid
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_timezone_id text := null;
  v_first_name text := null;
  v_last_name text := null;
  v_email text := null;
  v_phone text := null;
  v_notes text := null;
  v_pay_method text := null;
  v_active text := null;
  v_created_from date := null;
  v_created_to date := null;
  v_updated_from date := null;
  v_updated_to date := null;
  v_job_title_include_node_ids text[] := null;
  v_job_title_exclude_node_ids text[] := null;
  v_job_title_role_ids uuid[] := null;
  v_job_title_primary_only boolean := false;
  v_prof_reg_number text := null;
  v_prof_reg_type text := null;
  v_dob date := null;
  v_gender text := null;
  v_town_city text := null;
  v_postcode text := null;
  v_sort_code text := null;
  v_account_number text := null;
  v_umbrella_name text := null;
  v_tms_ref text := null;
  v_q text := null;
  v_work_status text := null;
  v_recent_months_raw text := null;
  v_recent_months int := 3;
  v_recent_all boolean := false;
  v_cutoff_ymd date := null;
  v_ids uuid[] := null;
  v_roles_any text[] := null;
  v_roles_all text[] := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  begin
    select sd.timezone_id
      into v_timezone_id
    from public.settings_defaults as sd
    limit 1;
  exception when others then
    v_timezone_id := null;
  end;

  if nullif(btrim(coalesce(v_timezone_id, '')), '') is null then
    v_timezone_id := 'UTC';
  end if;

  v_first_name := nullif(btrim(coalesce(p_filters->>'first_name', '')), '');
  v_last_name := nullif(btrim(coalesce(p_filters->>'last_name', '')), '');
  v_email := nullif(btrim(coalesce(p_filters->>'email', '')), '');
  v_phone := nullif(btrim(coalesce(p_filters->>'phone', '')), '');
  v_notes := nullif(btrim(coalesce(p_filters->>'notes', '')), '');
  v_pay_method := nullif(upper(btrim(coalesce(p_filters->>'pay_method', ''))), '');
  v_active := nullif(lower(btrim(coalesce(p_filters->>'active', ''))), '');
  v_prof_reg_number := nullif(btrim(coalesce(p_filters->>'prof_reg_number', '')), '');
  v_prof_reg_type := nullif(upper(btrim(coalesce(p_filters->>'prof_reg_type', ''))), '');
  v_gender := nullif(btrim(coalesce(p_filters->>'gender', '')), '');
  v_town_city := nullif(btrim(coalesce(p_filters->>'town_city', '')), '');
  v_postcode := nullif(btrim(coalesce(p_filters->>'postcode', '')), '');
  v_sort_code := nullif(btrim(coalesce(p_filters->>'sort_code', '')), '');
  v_account_number := nullif(btrim(coalesce(p_filters->>'account_number', '')), '');
  v_umbrella_name := nullif(btrim(coalesce(p_filters->>'umbrella_name', '')), '');
  v_tms_ref := nullif(btrim(coalesce(p_filters->>'tms_ref', '')), '');
  v_q := nullif(btrim(coalesce(p_filters->>'q', coalesce(p_filters->>'name', ''))), '');
  v_work_status := nullif(upper(btrim(coalesce(p_filters->>'work_status', ''))), '');
  v_recent_months_raw := nullif(upper(btrim(coalesce(p_filters->>'recent_months', ''))), '');

  begin
    if nullif(btrim(coalesce(p_filters->>'created_from', '')), '') is not null then
      v_created_from := (p_filters->>'created_from')::date;
    end if;
  exception when others then
    v_created_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_to', '')), '') is not null then
      v_created_to := (p_filters->>'created_to')::date;
    end if;
  exception when others then
    v_created_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_from', '')), '') is not null then
      v_updated_from := (p_filters->>'updated_from')::date;
    end if;
  exception when others then
    v_updated_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_to', '')), '') is not null then
      v_updated_to := (p_filters->>'updated_to')::date;
    end if;
  exception when others then
    v_updated_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'dob', '')), '') is not null then
      v_dob := (p_filters->>'dob')::date;
    end if;
  exception when others then
    v_dob := null;
  end;

  begin
    if p_filters ? 'job_title_include_node_ids' then
      if jsonb_typeof(p_filters->'job_title_include_node_ids') = 'array' then
        select array_agg(parsed.include_node_id_text order by parsed.include_node_id_text)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(include_nodes.value), '') as include_node_id_text
          from jsonb_array_elements_text(p_filters->'job_title_include_node_ids') as include_nodes(value)
          where nullif(btrim(include_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_include_node_ids', '')), '') is not null then
        select array_agg(parsed.include_node_id_text order by parsed.include_node_id_text)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(include_tokens.token), '') as include_node_id_text
          from unnest(regexp_split_to_array(p_filters->>'job_title_include_node_ids', '\s*,\s*')) as include_tokens(token)
          where nullif(btrim(include_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_include_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_exclude_node_ids' then
      if jsonb_typeof(p_filters->'job_title_exclude_node_ids') = 'array' then
        select array_agg(parsed.exclude_node_id_text order by parsed.exclude_node_id_text)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(exclude_nodes.value), '') as exclude_node_id_text
          from jsonb_array_elements_text(p_filters->'job_title_exclude_node_ids') as exclude_nodes(value)
          where nullif(btrim(exclude_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_exclude_node_ids', '')), '') is not null then
        select array_agg(parsed.exclude_node_id_text order by parsed.exclude_node_id_text)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(exclude_tokens.token), '') as exclude_node_id_text
          from unnest(regexp_split_to_array(p_filters->>'job_title_exclude_node_ids', '\s*,\s*')) as exclude_tokens(token)
          where nullif(btrim(exclude_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_exclude_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_role_ids' then
      if jsonb_typeof(p_filters->'job_title_role_ids') = 'array' then
        select array_agg(parsed.role_id order by parsed.role_id)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(role_nodes.value), '')::uuid as role_id
          from jsonb_array_elements_text(p_filters->'job_title_role_ids') as role_nodes(value)
          where nullif(btrim(role_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_role_ids', '')), '') is not null then
        select array_agg(parsed.role_id order by parsed.role_id)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(role_tokens.token), '')::uuid as role_id
          from unnest(regexp_split_to_array(p_filters->>'job_title_role_ids', '\s*,\s*')) as role_tokens(token)
          where nullif(btrim(role_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_role_ids := null;
  end;

  begin
    if p_filters ? 'job_title_primary_only' then
      v_job_title_primary_only := case lower(btrim(coalesce(p_filters->>'job_title_primary_only', '')))
        when 'true' then true
        when '1' then true
        when 'yes' then true
        when 'y' then true
        when 'on' then true
        else false
      end;
    else
      v_job_title_primary_only := false;
    end if;
  exception when others then
    v_job_title_primary_only := false;
  end;

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(parsed.selected_id order by parsed.selected_id)
        into v_ids
        from (
          select distinct nullif(btrim(id_nodes.value), '')::uuid as selected_id
          from jsonb_array_elements_text(p_filters->'ids') as id_nodes(value)
          where nullif(btrim(id_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(parsed.selected_id order by parsed.selected_id)
        into v_ids
        from (
          select distinct nullif(btrim(id_tokens.token), '')::uuid as selected_id
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as id_tokens(token)
          where nullif(btrim(id_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  begin
    if p_filters ? 'roles_any' then
      if jsonb_typeof(p_filters->'roles_any') = 'array' then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_any
        from (
          select distinct upper(nullif(btrim(role_any_nodes.value), '')) as role_code
          from jsonb_array_elements_text(p_filters->'roles_any') as role_any_nodes(value)
        ) as parsed
        where parsed.role_code is not null;
      elsif nullif(btrim(coalesce(p_filters->>'roles_any', '')), '') is not null then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_any
        from (
          select distinct upper(nullif(btrim(role_any_tokens.token), '')) as role_code
          from unnest(regexp_split_to_array(p_filters->>'roles_any', '\s*,\s*')) as role_any_tokens(token)
        ) as parsed
        where parsed.role_code is not null;
      end if;
    end if;
  exception when others then
    v_roles_any := null;
  end;

  begin
    if p_filters ? 'roles_all' then
      if jsonb_typeof(p_filters->'roles_all') = 'array' then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_all
        from (
          select distinct upper(nullif(btrim(role_all_nodes.value), '')) as role_code
          from jsonb_array_elements_text(p_filters->'roles_all') as role_all_nodes(value)
        ) as parsed
        where parsed.role_code is not null;
      elsif nullif(btrim(coalesce(p_filters->>'roles_all', '')), '') is not null then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_all
        from (
          select distinct upper(nullif(btrim(role_all_tokens.token), '')) as role_code
          from unnest(regexp_split_to_array(p_filters->>'roles_all', '\s*,\s*')) as role_all_tokens(token)
        ) as parsed
        where parsed.role_code is not null;
      end if;
    end if;
  exception when others then
    v_roles_all := null;
  end;

  if v_recent_months_raw = 'ALL' then
    v_recent_all := true;
    v_recent_months := 3;
  elsif v_recent_months_raw is not null then
    begin
      v_recent_months := greatest(1, least(120, (v_recent_months_raw)::int));
    exception when others then
      v_recent_months := 3;
    end;
  else
    v_recent_months := 3;
  end if;

  if v_work_status in ('RECENT', 'NOT') and not v_recent_all then
    v_cutoff_ymd := ((now() at time zone v_timezone_id)::date - make_interval(months => v_recent_months))::date;
  end if;

  return query
  with recursive job_title_tree as (
    select
      root_job_titles.id,
      root_job_titles.parent_id,
      root_job_titles.is_role,
      case
        when root_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then true
        when root_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then false
        else false
      end as checked,
      case
        when root_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then 'included'
        when root_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then 'excluded'
        else 'none'
      end as inherited_mode
    from public.default_job_titles as root_job_titles
    where root_job_titles.parent_id is null
       or not exists (
         select 1
         from public.default_job_titles as parent_job_titles
         where parent_job_titles.id = root_job_titles.parent_id
       )

    union all

    select
      child_job_titles.id,
      child_job_titles.parent_id,
      child_job_titles.is_role,
      case
        when child_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then true
        when child_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then false
        when job_title_tree.inherited_mode = 'included' then true
        when job_title_tree.inherited_mode = 'excluded' then false
        else false
      end as checked,
      case
        when child_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then 'included'
        when child_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then 'excluded'
        when job_title_tree.inherited_mode = 'included' then 'included'
        when job_title_tree.inherited_mode = 'excluded' then 'excluded'
        else 'none'
      end as inherited_mode
    from public.default_job_titles as child_job_titles
    join job_title_tree
      on job_title_tree.id = child_job_titles.parent_id
  ),
  tree_selected_roles as (
    select distinct
      job_title_tree.id as role_id
    from job_title_tree
    where job_title_tree.is_role = true
      and job_title_tree.checked = true
  ),
  effective_role_ids as (
    select distinct
      tree_selected_roles.role_id
    from tree_selected_roles
    where (
      coalesce(array_length(v_job_title_include_node_ids, 1), 0) > 0
      or coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) > 0
    )
      and (
        coalesce(array_length(v_job_title_role_ids, 1), 0) = 0
        or tree_selected_roles.role_id = any(coalesce(v_job_title_role_ids, '{}'::uuid[]))
      )

    union

    select distinct
      direct_role_ids.role_id
    from unnest(coalesce(v_job_title_role_ids, '{}'::uuid[])) as direct_role_ids(role_id)
    where coalesce(array_length(v_job_title_include_node_ids, 1), 0) = 0
      and coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) = 0
      and coalesce(array_length(v_job_title_role_ids, 1), 0) > 0
  ),
  candidate_filter_flags as (
    select
      (
        coalesce(array_length(v_job_title_include_node_ids, 1), 0) > 0
        or coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) > 0
        or coalesce(array_length(v_job_title_role_ids, 1), 0) > 0
      ) as has_job_title_filter,
      v_job_title_primary_only as primary_only
  ),
  filtered as (
    select csa.id
    from public.candidates_summary_activity as csa
    cross join candidate_filter_flags as candidate_flags
    where (v_ids is null or csa.id = any(v_ids))
      and (v_first_name is null or csa.first_name ilike ('%' || v_first_name || '%'))
      and (v_last_name is null or csa.last_name ilike ('%' || v_last_name || '%'))
      and (v_email is null or csa.email ilike ('%' || v_email || '%'))
      and (v_phone is null or csa.phone ilike ('%' || v_phone || '%'))
      and (
        v_notes is null
        or csa.notes ilike ('%' || replace(replace(replace(v_notes, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%') escape '\\'
      )
      and (
        v_pay_method is null
        or (v_pay_method = 'BLANK' and csa.pay_method is null)
        or (v_pay_method <> 'BLANK' and upper(coalesce(csa.pay_method::text, '')) = v_pay_method)
      )
      and (
        v_active is null
        or (v_active = 'true' and csa.active = true)
        or (v_active = 'false' and csa.active = false)
      )
      and (v_created_from is null or (csa.created_at AT TIME ZONE v_timezone_id)::date >= v_created_from)
      and (v_created_to is null or (csa.created_at AT TIME ZONE v_timezone_id)::date <= v_created_to)
      and (v_updated_from is null or (csa.updated_at AT TIME ZONE v_timezone_id)::date >= v_updated_from)
      and (v_updated_to is null or (csa.updated_at AT TIME ZONE v_timezone_id)::date <= v_updated_to)
      and (
        candidate_flags.has_job_title_filter = false
        or (
          candidate_flags.primary_only = true
          and exists (
            select 1
            from effective_role_ids as effective_roles
            where effective_roles.role_id = csa.primary_job_title_id
          )
        )
        or (
          candidate_flags.primary_only = false
          and (
            exists (
              select 1
              from effective_role_ids as effective_roles
              where effective_roles.role_id = csa.primary_job_title_id
            )
            or exists (
              select 1
              from unnest(coalesce(csa.job_title_ids, '{}'::uuid[])) as candidate_job_titles(job_title_id)
              join effective_role_ids as effective_roles
                on effective_roles.role_id = candidate_job_titles.job_title_id
            )
          )
        )
      )
      and (v_prof_reg_number is null or csa.prof_reg_number ilike ('%' || v_prof_reg_number || '%'))
      and (v_prof_reg_type is null or upper(coalesce(csa.prof_reg_type::text, '')) = v_prof_reg_type)
      and (v_dob is null or csa.date_of_birth = v_dob)
      and (v_gender is null or csa.gender = v_gender)
      and (v_town_city is null or csa.town_city ilike ('%' || v_town_city || '%'))
      and (v_postcode is null or csa.postcode ilike ('%' || v_postcode || '%'))
      and (v_sort_code is null or csa.sort_code ilike ('%' || v_sort_code || '%'))
      and (v_account_number is null or csa.account_number ilike ('%' || v_account_number || '%'))
      and (v_umbrella_name is null or csa.umbrella_name ilike ('%' || v_umbrella_name || '%'))
      and (v_tms_ref is null or csa.tms_ref ilike ('%' || v_tms_ref || '%'))
      and (
        v_q is null
        or (
          csa.first_name ilike ('%' || v_q || '%')
          or csa.last_name ilike ('%' || v_q || '%')
          or csa.display_name ilike ('%' || v_q || '%')
          or csa.email ilike ('%' || v_q || '%')
          or csa.phone ilike ('%' || v_q || '%')
          or csa.tms_ref ilike ('%' || v_q || '%')
          or csa.job_titles_display ilike ('%' || v_q || '%')
        )
      )
      and (
        v_roles_any is null
        or exists (
          select 1
          from jsonb_array_elements(coalesce(csa.roles, '[]'::jsonb)) as roles_any_json(role_elem)
          where upper(coalesce(roles_any_json.role_elem->>'code', '')) = any(v_roles_any)
        )
      )
      and (
        v_roles_all is null
        or not exists (
          select 1
          from unnest(v_roles_all) as required_roles(required_code)
          where not exists (
            select 1
            from jsonb_array_elements(coalesce(csa.roles, '[]'::jsonb)) as roles_all_json(role_elem)
            where upper(coalesce(roles_all_json.role_elem->>'code', '')) = required_roles.required_code
          )
        )
      )
      and (
        v_work_status is null
        or v_work_status = 'ALL'
        or (v_work_status = 'CURRENT' and csa.is_currently_working = true)
        or (
          v_work_status = 'RECENT'
          and (
            csa.is_currently_working = true
            or (
              csa.is_currently_working = false
              and (
                (v_recent_all and csa.last_timesheet_week_ending is not null)
                or (not v_recent_all and v_cutoff_ymd is not null and csa.last_timesheet_week_ending >= v_cutoff_ymd)
              )
            )
          )
        )
        or (
          v_work_status = 'NOT'
          and csa.is_currently_working = false
          and (
            csa.last_timesheet_week_ending is null
            or (v_cutoff_ymd is not null and csa.last_timesheet_week_ending < v_cutoff_ymd)
          )
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$$;

create or replace function public.client_list_ids(p_filters jsonb)
returns table (
  id uuid
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_q text := null;
  v_cli_ref text := null;
  v_primary_invoice_email text := null;
  v_invoice_address text := null;
  v_ap_phone text := null;
  v_vat_chargeable text := null;
  v_payment_terms_days integer := null;
  v_mileage_charge_rate numeric := null;
  v_ts_queries_email text := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;
  v_notes text := null;
  v_ids uuid[] := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  v_q := nullif(btrim(coalesce(p_filters->>'q', coalesce(p_filters->>'name', ''))), '');
  v_cli_ref := nullif(btrim(coalesce(p_filters->>'cli_ref', '')), '');
  v_primary_invoice_email := nullif(btrim(coalesce(p_filters->>'primary_invoice_email', '')), '');
  v_invoice_address := nullif(btrim(coalesce(p_filters->>'invoice_address', '')), '');
  v_ap_phone := nullif(btrim(coalesce(p_filters->>'ap_phone', '')), '');
  v_vat_chargeable := nullif(lower(btrim(coalesce(p_filters->>'vat_chargeable', ''))), '');
  v_notes := nullif(btrim(coalesce(p_filters->>'notes', '')), '');
  v_ts_queries_email := nullif(btrim(coalesce(p_filters->>'ts_queries_email', '')), '');

  begin
    if nullif(btrim(coalesce(p_filters->>'payment_terms_days', '')), '') is not null then
      v_payment_terms_days := (p_filters->>'payment_terms_days')::integer;
    end if;
  exception when others then
    v_payment_terms_days := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate', '')), '') is not null then
      v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric;
    end if;
  exception when others then
    v_mileage_charge_rate := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_from', '')), '') is not null then
      v_created_from := (p_filters->>'created_from')::timestamptz;
    end if;
  exception when others then
    v_created_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_to', '')), '') is not null then
      v_created_to := (p_filters->>'created_to')::timestamptz;
    end if;
  exception when others then
    v_created_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_from', '')), '') is not null then
      v_updated_from := (p_filters->>'updated_from')::timestamptz;
    end if;
  exception when others then
    v_updated_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_to', '')), '') is not null then
      v_updated_to := (p_filters->>'updated_to')::timestamptz;
    end if;
  exception when others then
    v_updated_to := null;
  end;

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'ids') as e(value)
        ) s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
        ) s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  return query
  select c.id
  from public.clients c
  where (v_ids is null or c.id = any(v_ids))
    and (v_q is null or c.name ilike ('%' || v_q || '%'))
    and (v_cli_ref is null or c.cli_ref ilike ('%' || v_cli_ref || '%'))
    and (v_primary_invoice_email is null or c.primary_invoice_email ilike ('%' || v_primary_invoice_email || '%'))
    and (v_invoice_address is null or c.invoice_address ilike ('%' || v_invoice_address || '%'))
    and (v_ap_phone is null or c.ap_phone ilike ('%' || v_ap_phone || '%'))
    and (
      v_notes is null
      or c.notes ilike ('%' || replace(replace(replace(v_notes, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%') escape '\\'
    )
    and (
      v_vat_chargeable is null
      or (v_vat_chargeable = 'true' and c.vat_chargeable = true)
      or (v_vat_chargeable = 'false' and c.vat_chargeable = false)
    )
    and (v_payment_terms_days is null or c.payment_terms_days = v_payment_terms_days)
    and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)
    and (v_ts_queries_email is null or c.ts_queries_email ilike ('%' || v_ts_queries_email || '%'))
    and (v_created_from is null or c.created_at >= v_created_from)
    and (v_created_to is null or c.created_at <= v_created_to)
    and (v_updated_from is null or c.updated_at >= v_updated_from)
    and (v_updated_to is null or c.updated_at <= v_updated_to)
  order by c.id;
end;
$$;


create or replace function public.umbrella_list_ids(p_filters jsonb)
returns table (
  id uuid
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_q text := null;
  v_bank_name text := null;
  v_sort_code text := null;
  v_account_number text := null;
  v_enabled text := null;
  v_vat_chargeable text := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_ids uuid[] := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  v_q := nullif(btrim(coalesce(p_filters->>'q', coalesce(p_filters->>'name', ''))), '');
  v_bank_name := nullif(btrim(coalesce(p_filters->>'bank_name', '')), '');
  v_sort_code := nullif(btrim(coalesce(p_filters->>'sort_code', '')), '');
  v_account_number := nullif(btrim(coalesce(p_filters->>'account_number', '')), '');
  v_enabled := nullif(lower(btrim(coalesce(p_filters->>'enabled', ''))), '');
  v_vat_chargeable := nullif(lower(btrim(coalesce(p_filters->>'vat_chargeable', ''))), '');

  begin
    if nullif(btrim(coalesce(p_filters->>'created_from', '')), '') is not null then
      v_created_from := (p_filters->>'created_from')::timestamptz;
    end if;
  exception when others then
    v_created_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_to', '')), '') is not null then
      v_created_to := (p_filters->>'created_to')::timestamptz;
    end if;
  exception when others then
    v_created_to := null;
  end;

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'ids') as e(value)
        ) s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
        ) s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  return query
  select u.id
  from public.umbrellas u
  where (v_ids is null or u.id = any(v_ids))
    and (
      v_q is null
      or u.name ilike ('%' || v_q || '%')
      or u.remittance_email ilike ('%' || v_q || '%')
    )
    and (v_bank_name is null or u.bank_name ilike ('%' || v_bank_name || '%'))
    and (v_sort_code is null or u.sort_code ilike ('%' || v_sort_code || '%'))
    and (v_account_number is null or u.account_number ilike ('%' || v_account_number || '%'))
    and (
      v_enabled is null
      or (v_enabled = 'true' and u.enabled = true)
      or (v_enabled = 'false' and u.enabled = false)
    )
    and (
      v_vat_chargeable is null
      or (v_vat_chargeable = 'true' and u.vat_chargeable = true)
      or (v_vat_chargeable = 'false' and u.vat_chargeable = false)
    )
    and (v_created_from is null or u.created_at >= v_created_from)
    and (v_created_to is null or u.created_at <= v_created_to)
  order by u.id;
end;
$$;
create or replace function public.contract_list_ids(p_filters jsonb)
returns table (
  id uuid
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_candidate_id uuid := null;
  v_client_id uuid := null;
  v_pay_method text := null;
  v_role_like text := null;
  v_band text := null;
  v_candidate_name_like text := null;
  v_client_name_like text := null;
  v_active_on date := null;
  v_auto_invoice text := null;
  v_q text := null;
  v_default_submission_mode text := null;
  v_week_ending_weekday_snapshot int := null;
  v_require_ref_pay text := null;
  v_require_ref_inv text := null;
  v_has_custom_labels text := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;
  v_start_from date := null;
  v_start_to date := null;
  v_end_from date := null;
  v_end_to date := null;
  v_mileage_pay_rate numeric := null;
  v_mileage_charge_rate numeric := null;
  v_status text := null;
  v_ids jsonb := null;
  v_ids_arr uuid[] := null;
  incomplete_statuses text[] := array['OPEN','PLANNED','SUBMITTED','AUTHORISED'];
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;

  v_pay_method := nullif(btrim(coalesce(p_filters->>'pay_method_snapshot','')), '');
  if v_pay_method is not null then v_pay_method := upper(v_pay_method); end if;

  if nullif(btrim(coalesce(p_filters->>'role','')), '') is not null then
    v_role_like := '%' || (p_filters->>'role') || '%';
  end if;

  v_band := nullif(btrim(coalesce(p_filters->>'band','')), '');

  if nullif(btrim(coalesce(p_filters->>'candidate_name','')), '') is not null then
    v_candidate_name_like := '%' || (p_filters->>'candidate_name') || '%';
  end if;
  if nullif(btrim(coalesce(p_filters->>'client_name','')), '') is not null then
    v_client_name_like := '%' || (p_filters->>'client_name') || '%';
  end if;

  begin if nullif(btrim(coalesce(p_filters->>'active_on','')), '') is not null then v_active_on := (p_filters->>'active_on')::date; end if; exception when others then v_active_on := null; end;

  v_auto_invoice := nullif(btrim(coalesce(p_filters->>'auto_invoice','')), '');
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'default_submission_mode','')), '');
  if v_default_submission_mode is null then
    v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'submission_mode','')), '');
  end if;
  if v_default_submission_mode is not null then v_default_submission_mode := upper(v_default_submission_mode); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_weekday_snapshot','')), '') is not null then v_week_ending_weekday_snapshot := (p_filters->>'week_ending_weekday_snapshot')::int; end if; exception when others then v_week_ending_weekday_snapshot := null; end;

  v_require_ref_pay := nullif(btrim(coalesce(p_filters->>'require_reference_to_pay','')), '');
  v_require_ref_inv := nullif(btrim(coalesce(p_filters->>'require_reference_to_invoice','')), '');
  v_has_custom_labels := nullif(btrim(coalesce(p_filters->>'has_custom_labels','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_from','')), '') is not null then v_updated_from := (p_filters->>'updated_from')::timestamptz; end if; exception when others then v_updated_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_to','')), '') is not null then v_updated_to := (p_filters->>'updated_to')::timestamptz; end if; exception when others then v_updated_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'start_date_from','')), '') is not null then v_start_from := (p_filters->>'start_date_from')::date; end if; exception when others then v_start_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'start_date_to','')), '') is not null then v_start_to := (p_filters->>'start_date_to')::date; end if; exception when others then v_start_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_from','')), '') is not null then v_end_from := (p_filters->>'end_date_from')::date; end if; exception when others then v_end_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_to','')), '') is not null then v_end_to := (p_filters->>'end_date_to')::date; end if; exception when others then v_end_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'mileage_pay_rate','')), '') is not null then v_mileage_pay_rate := (p_filters->>'mileage_pay_rate')::numeric; end if; exception when others then v_mileage_pay_rate := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate','')), '') is not null then v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric; end if; exception when others then v_mileage_charge_rate := null; end;

  v_status := lower(nullif(btrim(coalesce(p_filters->>'status','')), ''));
  if v_status is null then v_status := 'all'; end if;

  if p_filters ? 'ids' then
    v_ids := p_filters->'ids';
    if v_ids is not null and jsonb_typeof(v_ids) = 'array' then
      select array_agg((x)::uuid) into v_ids_arr
      from jsonb_array_elements_text(v_ids) x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    elsif v_ids is not null and jsonb_typeof(v_ids) <> 'array' and nullif(btrim(coalesce(p_filters->>'ids','')), '') is not null then
      select array_agg(val::uuid)
      into v_ids_arr
      from (
        select distinct nullif(btrim(x), '') as val
        from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
      ) s
      where s.val is not null;
    end if;
  end if;

  return query
  with base as (
    select
      c.id,
      c.candidate_id,
      c.client_id,
      c.role,
      c.band,
      c.start_date,
      c.end_date,
      c.auto_invoice,
      c.pay_method_snapshot,
      c.default_submission_mode,
      c.week_ending_weekday_snapshot,
      c.require_reference_to_pay,
      c.require_reference_to_invoice,
      c.created_at,
      c.updated_at,
      c.bucket_labels_json,
      c.mileage_pay_rate,
      c.mileage_charge_rate,
      cand.display_name as cand_display,
      cand.first_name as cand_first,
      cand.last_name as cand_last,
      cli.name as cli_name
    from public.contracts c
    left join public.candidates cand on cand.id = c.candidate_id
    left join public.clients cli on cli.id = c.client_id
    where (v_ids_arr is null or c.id = any(v_ids_arr))
      and (v_candidate_id is null or c.candidate_id = v_candidate_id)
      and (v_client_id is null or c.client_id = v_client_id)
      and (v_pay_method is null or upper(coalesce(c.pay_method_snapshot::text,'')) = v_pay_method)
      and (v_band is null or c.band = v_band)
      and (v_role_like is null or c.role ilike v_role_like)
      and (v_active_on is null or (c.start_date <= v_active_on and c.end_date >= v_active_on))
      and (
        v_auto_invoice is null
        or (lower(v_auto_invoice) = 'true' and c.auto_invoice = true)
        or (lower(v_auto_invoice) = 'false' and c.auto_invoice = false)
      )
      and (v_default_submission_mode is null or upper(coalesce(c.default_submission_mode::text,'')) = v_default_submission_mode)
      and (v_week_ending_weekday_snapshot is null or c.week_ending_weekday_snapshot = v_week_ending_weekday_snapshot)
      and (
        v_require_ref_pay is null
        or (lower(v_require_ref_pay) = 'true' and c.require_reference_to_pay = true)
        or (lower(v_require_ref_pay) = 'false' and c.require_reference_to_pay = false)
      )
      and (
        v_require_ref_inv is null
        or (lower(v_require_ref_inv) = 'true' and c.require_reference_to_invoice = true)
        or (lower(v_require_ref_inv) = 'false' and c.require_reference_to_invoice = false)
      )
      and (
        v_has_custom_labels is null
        or (lower(v_has_custom_labels) = 'true' and c.bucket_labels_json is not null)
        or (lower(v_has_custom_labels) = 'false' and c.bucket_labels_json is null)
      )
      and (v_created_from is null or c.created_at >= v_created_from)
      and (v_created_to is null or c.created_at <= v_created_to)
      and (v_updated_from is null or c.updated_at >= v_updated_from)
      and (v_updated_to is null or c.updated_at <= v_updated_to)
      and (v_start_from is null or c.start_date >= v_start_from)
      and (v_start_to is null or c.start_date <= v_start_to)
      and (v_end_from is null or c.end_date >= v_end_from)
      and (v_end_to is null or c.end_date <= v_end_to)
      and (v_mileage_pay_rate is null or c.mileage_pay_rate = v_mileage_pay_rate)
      and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)
      and (
        v_candidate_name_like is null
        or (coalesce(cand.display_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.first_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.last_name,'') ilike v_candidate_name_like)
      )
      and (
        v_client_name_like is null
        or (coalesce(cli.name,'') ilike v_client_name_like)
      )
      and (
        v_q is null
        or (
          coalesce(cli.name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.display_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.first_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.last_name,'') ilike ('%'||v_q||'%')
          or coalesce(c.role,'') ilike ('%'||v_q||'%')
        )
      )
  ),
  filtered as (
    select b.id
    from base b
    where
      (v_status = 'all')
      or (v_status = 'unassigned' and b.candidate_id is null)
      or (
        v_status = 'active'
        and exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(incomplete_statuses)
        )
      )
      or (
        v_status = 'completed'
        and b.candidate_id is not null
        and not exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(incomplete_statuses)
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$$;


CREATE OR REPLACE FUNCTION public.timesheet_list_ids(p_filters jsonb)
RETURNS TABLE (
  id uuid
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := '{}'::jsonb;

  v_client_id uuid := NULL;
  v_candidate_id uuid := NULL;
  v_summary_stage text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_we_from date := NULL;
  v_we_to date := NULL;
  v_is_adjusted text := NULL;
  v_is_qr text := NULL;
  v_needs_attention text := NULL;
  v_candidate_paid text := NULL;
  v_client_invoiced text := NULL;
  v_hr_issue text := NULL;
  v_proc_status_raw text := NULL;
  v_proc_list text[] := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;
  v_q text := NULL;
  v_q_like text := NULL;
  v_ids text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_has_client_filter boolean := FALSE;
  v_has_candidate_filter boolean := FALSE;
BEGIN
  v_source_filters :=
    v_filters
    - 'q'
    - 'query'
    - 'name'
    - 'client_id'
    - 'clientId'
    - 'candidate_id'
    - 'candidateId'
    - 'week_ending_from'
    - 'weekEndingFrom'
    - 'week_ending_to'
    - 'weekEndingTo'
    - 'route_type'
    - 'routeType'
    - 'issues_filter'
    - 'issuesFilter'
    - 'status_code'
    - 'statusCode'
    - 'summary_stage'
    - 'summaryStage'
    - 'client_invoiced'
    - 'clientInvoiced'
    - 'needs_attention'
    - 'needsAttention'
    || jsonb_build_object('disable_paging', TRUE, 'purpose', 'membership');

  v_has_client_filter := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '') IS NOT NULL;
  IF v_has_client_filter THEN
    IF COALESCE(v_filters->>'client_id', v_filters->>'clientId') ~* v_uuid_re THEN
      v_client_id := COALESCE(v_filters->>'client_id', v_filters->>'clientId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_has_candidate_filter := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '') IS NOT NULL;
  IF v_has_candidate_filter THEN
    IF COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId') ~* v_uuid_re THEN
      v_candidate_id := COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId')::uuid;
    ELSE
      RETURN;
    END IF;
  END IF;

  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  BEGIN
    IF v_filters ? 'ids' THEN
      IF jsonb_typeof(v_filters->'ids') = 'array' THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS id_value
          FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
        SELECT ARRAY_AGG(id_values.id_value)
        INTO v_ids
        FROM (
          SELECT DISTINCT NULLIF(BTRIM(split_values.value), '') AS id_value
          FROM unnest(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
        ) AS id_values
        WHERE id_values.id_value IS NOT NULL;
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_ids := NULL;
  END;

  v_summary_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'summary_stage', v_filters->>'summaryStage', '')), ''));
  IF v_summary_stage = 'ALL' THEN v_summary_stage := NULL; END IF;

  v_tools_stage := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  IF v_tools_stage = 'ALL' THEN v_tools_stage := NULL; END IF;

  v_issues_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));
  IF v_issues_filter = 'ALL' THEN v_issues_filter := NULL; END IF;

  v_route_type := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  IF v_route_type = 'ALL' THEN v_route_type := NULL; END IF;

  v_sheet_scope := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_sheet_scope = 'ALL' THEN v_sheet_scope := NULL; END IF;

  v_qr_status := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  IF v_qr_status = 'ALL' THEN v_qr_status := NULL; END IF;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '') IS NOT NULL THEN
      v_we_from := COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_from := NULL;
  END;

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '') IS NOT NULL THEN
      v_we_to := COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo')::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_we_to := NULL;
  END;

  v_is_adjusted := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')), ''));
  v_is_qr := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')), ''));
  v_needs_attention := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'needs_attention', v_filters->>'needsAttention', '')), ''));
  v_candidate_paid := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')), ''));
  v_client_invoiced := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'client_invoiced', v_filters->>'clientInvoiced', '')), ''));

  v_hr_issue := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
  IF v_hr_issue = 'ALL' THEN v_hr_issue := NULL; END IF;

  v_proc_status_raw := NULLIF(BTRIM(COALESCE(v_filters->>'processing_status', v_filters->>'processingStatus', '')), '');
  IF v_proc_status_raw IS NOT NULL AND UPPER(v_proc_status_raw) <> 'ALL' THEN
    SELECT ARRAY_AGG(status_values.status_value)
    INTO v_proc_list
    FROM (
      SELECT NULLIF(BTRIM(UPPER(split_values.value)), '') AS status_value
      FROM unnest(regexp_split_to_array(v_proc_status_raw, '\s*,\s*')) AS split_values(value)
    ) AS status_values
    WHERE status_values.status_value IS NOT NULL;
  END IF;

  v_status_code := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  IF v_status_code = 'ALL' THEN v_status_code := NULL; END IF;

  RETURN QUERY
  WITH filtered_rows AS (
    SELECT
      COALESCE(summary_row.timesheet_id, summary_row.contract_week_id) AS row_id
    FROM public.timesheet_summary_lightweight_rows_v1(v_source_filters) AS summary_row
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = summary_row.timesheet_id
     AND timesheet_row.is_current = TRUE
    WHERE

      (
        v_ids IS NULL
        OR summary_row.timesheet_id::text = ANY(v_ids)
        OR summary_row.contract_week_id::text = ANY(v_ids)
      )
      AND (v_client_id IS NULL OR summary_row.client_id = v_client_id)
      AND (v_candidate_id IS NULL OR summary_row.candidate_id = v_candidate_id)
      AND (
        v_q IS NULL
        OR COALESCE(summary_row.candidate_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.client_name, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.booking_id, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.occupant_key_norm, '') ILIKE v_q_like ESCAPE '\'
        OR COALESCE(summary_row.hospital_norm, '') ILIKE v_q_like ESCAPE '\'
      )
      AND (v_summary_stage IS NULL OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_summary_stage)
      AND (v_tools_stage IS NULL OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_tools_stage)
      AND (v_we_from IS NULL OR summary_row.week_ending_date >= v_we_from)
      AND (v_we_to IS NULL OR summary_row.week_ending_date <= v_we_to)
      AND (
        v_route_type IS NULL
        OR (v_route_type = 'ELECTRONIC' AND UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC'))
        OR (v_route_type = 'MANUAL' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL') OR UPPER(COALESCE(summary_row.route_family, '')) = 'MANUAL'))
        OR (v_route_type = 'NHSP' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') OR UPPER(COALESCE(summary_row.route_family, '')) = 'NHSP'))
        OR (v_route_type = 'HEALTHROSTER' AND (UPPER(COALESCE(summary_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') OR UPPER(COALESCE(summary_row.route_family, '')) = 'HEALTHROSTER'))
        OR (v_route_type = 'QR' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_route_type IN ('NO_TIMESHEET_REQUIRED', 'NO-TIMESHEET-REQUIRED') AND (UPPER(COALESCE(summary_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED' OR UPPER(COALESCE(summary_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED' OR COALESCE(summary_row.client_no_timesheet_required, FALSE) = TRUE))
        OR UPPER(COALESCE(summary_row.route_type, '')) = v_route_type
        OR UPPER(COALESCE(summary_row.route_family, '')) = v_route_type
      )
      AND (v_sheet_scope IS NULL OR UPPER(COALESCE(summary_row.sheet_scope, '')) = v_sheet_scope)
      AND (v_qr_status IS NULL OR UPPER(COALESCE(summary_row.qr_status, '')) = v_qr_status)
      AND (
        v_is_adjusted IS NULL
        OR (v_is_adjusted = 'true' AND COALESCE(summary_row.is_adjusted, FALSE) = TRUE)
        OR (v_is_adjusted = 'false' AND COALESCE(summary_row.is_adjusted, FALSE) = FALSE)
      )
      AND (
        v_is_qr IS NULL
        OR (v_is_qr = 'true' AND COALESCE(summary_row.is_qr, FALSE) = TRUE)
        OR (v_is_qr = 'false' AND COALESCE(summary_row.is_qr, FALSE) = FALSE)
      )
      AND (
        v_needs_attention IS NULL
        OR (v_needs_attention = 'true' AND COALESCE(summary_row.needs_attention, FALSE) = TRUE)
        OR (v_needs_attention = 'false' AND COALESCE(summary_row.needs_attention, FALSE) = FALSE)
      )
      AND (
        v_candidate_paid IS NULL
        OR (v_candidate_paid = 'true' AND summary_row.pay_paid_at_utc IS NOT NULL)
        OR (v_candidate_paid = 'false' AND summary_row.pay_paid_at_utc IS NULL)
      )
      AND (
        v_client_invoiced IS NULL
        OR (v_client_invoiced = 'true' AND COALESCE(summary_row.invoice_segments_locked, 0) > 0)
        OR (v_client_invoiced = 'false' AND COALESCE(summary_row.invoice_segments_locked, 0) = 0)
      )
      AND (
        v_hr_issue IS NULL
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(summary_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue
        )
      )
      AND (
        v_proc_list IS NULL
        OR UPPER(COALESCE(summary_row.processing_status, '')) = ANY(v_proc_list)
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (
          v_issues_filter = 'RATE_MISSING'
          AND (
            COALESCE(summary_row.has_rate_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('PAY_CHAN_MISS', 'PAY_CHANNEL_MISSING')
          AND (
            COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('AWAITING_HR_VALIDATION', 'AWAITING_HR_VALIDATION_REQUIRED')
          AND (
            UPPER(COALESCE(summary_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
          )
        )
        OR (
          v_issues_filter IN ('HR_HOURS_MISMATCH', 'HOURS_MISMATCH_HR')
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'HR_HOURS_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'DUPLICATE_CONTRACTS'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter = 'REFERENCE_MISSING'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING')
          )
        )
        OR (
          v_issues_filter = 'VALIDATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION'
          )
        )
        OR (
          v_issues_filter = 'AUTHORISATION'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION')
          )
        )
        OR (
          v_issues_filter = 'ON_HOLD'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD'
          )
        )
        OR (
          v_issues_filter = 'REFS_PDF_INVALID'
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(summary_row.issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID'
          )
        )
        OR (
          v_issues_filter = 'QR_NOT_ISSUED'
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') = ''
          AND timesheet_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('QR_AWAITING_SIGNATURE', 'QR_ISSUED_AWAITING_SIGNATURE')
          AND summary_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(summary_row.qr_status, '')) = 'PENDING'
          AND COALESCE(timesheet_row.qr_token, '') <> ''
          AND timesheet_row.qr_generated_at IS NOT NULL
          AND timesheet_row.qr_scanned_at IS NULL
        )
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'NO_MATCH_ID'
          AND (summary_row.candidate_id IS NULL OR summary_row.client_id IS NULL)
        )
        OR (v_status_code = 'RATE_MISSING' AND COALESCE(summary_row.has_rate_issue, FALSE) = TRUE)
        OR (v_status_code = 'PAY_CHAN_MISS' AND COALESCE(summary_row.has_pay_channel_issue, FALSE) = TRUE)
        OR (v_status_code = 'READY_FOR_HR' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_HR')
        OR (v_status_code = 'READY_FOR_INV' AND UPPER(COALESCE(summary_row.processing_status, '')) = 'READY_FOR_INVOICE')
        OR UPPER(COALESCE(summary_row.processing_status, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.summary_stage, '')) = v_status_code
        OR UPPER(COALESCE(summary_row.tools_stage, '')) = v_status_code
      )

  )
  SELECT DISTINCT
    filtered_rows.row_id AS id
  FROM filtered_rows
  WHERE filtered_rows.row_id IS NOT NULL
  ORDER BY filtered_rows.row_id;
END;
$function$;




create or replace function public.invoice_list_ids(p_filters jsonb)
returns table (
  id uuid
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_client_id uuid := null;
  v_q text := null;
  v_issued_from timestamptz := null;
  v_issued_to timestamptz := null;
  v_due_from timestamptz := null;
  v_due_to timestamptz := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_week_ending_from date := null;
  v_week_ending_to date := null;
  v_status_raw jsonb := null;
  v_status_list text[] := null;
  v_legacy_paid_filter text := null;
  v_ids_arr uuid[] := null;
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if (p_filters ? 'client_id') and nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'issued_from','')), '') is not null then v_issued_from := (p_filters->>'issued_from')::timestamptz; end if; exception when others then v_issued_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'issued_to','')), '') is not null then v_issued_to := (p_filters->>'issued_to')::timestamptz; end if; exception when others then v_issued_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_from','')), '') is not null then v_due_from := (p_filters->>'due_from')::timestamptz; end if; exception when others then v_due_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_to','')), '') is not null then v_due_to := (p_filters->>'due_to')::timestamptz; end if; exception when others then v_due_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'week_ending_from','')), '') is not null then v_week_ending_from := (p_filters->>'week_ending_from')::date; end if; exception when others then v_week_ending_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'week_ending_to','')), '') is not null then v_week_ending_to := (p_filters->>'week_ending_to')::date; end if; exception when others then v_week_ending_to := null; end;

  if p_filters ? 'status' then
    v_status_raw := p_filters->'status';
  end if;

  if v_status_raw is not null then
    if jsonb_typeof(v_status_raw) = 'array' then
      select array_agg(upper(btrim(x))) into v_status_list
      from (
        select jsonb_array_elements_text(v_status_raw) as x
      ) s
      where nullif(btrim(coalesce(x,'')), '') is not null;
    else
      v_status_list := array_remove(string_to_array(upper(btrim(trim(both '"' from coalesce(v_status_raw::text,'')))), ','), '');
    end if;
  end if;

  if v_status_list is not null and array_length(v_status_list,1) = 1 then
    if lower(v_status_list[1]) = 'paid' or v_status_list[1] = 'PAID' then
      v_legacy_paid_filter := 'paid';
      v_status_list := null;
    elsif lower(v_status_list[1]) = 'unpaid' or v_status_list[1] = 'UNPAID' then
      v_legacy_paid_filter := 'unpaid';
      v_status_list := null;
    end if;
  end if;

  if v_status_list is not null then
    select array_agg(s)
    into v_status_list
    from (
      select distinct replace(replace(replace(replace(upper(btrim(x)), '(', ''), ')', ''), ',', ''), '"', '') as s
      from unnest(v_status_list) x
    ) t
    where s in ('DRAFT','ISSUED','ON_HOLD','PAID');

    if v_status_list is null or coalesce(array_length(v_status_list,1),0) = 0 then
      v_status_list := null;
    end if;
  end if;

  if p_filters ? 'ids' then
    if jsonb_typeof(p_filters->'ids') = 'array' then
      select array_agg((x)::uuid)
      into v_ids_arr
      from jsonb_array_elements_text(p_filters->'ids') x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    elsif nullif(btrim(coalesce(p_filters->>'ids','')), '') is not null then
      select array_agg(val::uuid)
      into v_ids_arr
      from (
        select distinct nullif(btrim(x), '') as val
        from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
      ) s
      where s.val is not null;
    end if;
  end if;

  return query
  with quick_ids as (
    select qq.invoice_id as id
    from public.invoice_quicksearch_ids(v_q, 20000) qq
    where v_q is not null
  ),
  filtered as (
    select i.id
    from public.invoices i
    where (v_client_id is null or i.client_id = v_client_id)
      and (v_ids_arr is null or i.id = any(v_ids_arr))
      and (
        v_q is null
        or exists (
          select 1
          from quick_ids qi
          where qi.id = i.id
        )
      )
      and (v_issued_from is null or i.issued_at_utc >= v_issued_from)
      and (v_issued_to is null or i.issued_at_utc <= v_issued_to)
      and (v_due_from is null or i.due_at_utc >= v_due_from)
      and (v_due_to is null or i.due_at_utc <= v_due_to)
      and (v_created_from is null or i.created_at >= v_created_from)
      and (v_created_to is null or i.created_at <= v_created_to)
      and (
        v_legacy_paid_filter is null
        or (v_legacy_paid_filter = 'paid' and i.paid_at_utc is not null)
        or (v_legacy_paid_filter = 'unpaid' and i.paid_at_utc is null)
      )
      and (
        v_status_list is null
        or i.status::text = any(v_status_list)
      )
      and (
        (v_week_ending_from is null and v_week_ending_to is null)
        or exists (
          select 1
          from public.invoice_lines il
          join public.timesheets t on t.timesheet_id = il.timesheet_id
          where il.invoice_id = i.id
            and (v_week_ending_from is null or t.week_ending_date >= v_week_ending_from)
            and (v_week_ending_to is null or t.week_ending_date <= v_week_ending_to)
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$$;


