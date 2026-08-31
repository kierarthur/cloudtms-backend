-- CloudTMS Candidate Office processing-status and Authorise authority closure.
-- Replaces the current installed definitions without changing their signatures or grants.
-- Legacy QR/PAPER columns no longer own Processing Status or Authorise eligibility;
-- canonical Candidate finalisation feeds the ordinary PENDING_AUTH Office authority.

CREATE OR REPLACE FUNCTION public.bulk_timesheet_workbench_row_source_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(timesheet_id uuid, timesheet_status timesheet_status_enum, week_ending_date date, booking_id text, occupant_key_norm text, hospital_norm text, sheet_scope timesheet_scope_enum, submission_mode submission_mode_enum, authorised_at_server timestamp with time zone, candidate_id uuid, client_id uuid, pay_method text, processing_status ts_fin_processing_status_enum, basis timesheet_fin_basis_enum, total_hours numeric, total_pay_ex_vat numeric, total_charge_ex_vat numeric, margin_ex_vat numeric, paid_at_utc timestamp with time zone, pay_on_hold boolean, ready_to_pay boolean, locked_by_invoice_id uuid, candidate_name text, client_name text, nhsp_shift_count integer, nhsp_shift_included_count integer, nhsp_shift_deferred_count integer, validation_status validation_status_enum, summary_stage text, route_type text, contract_week_id uuid, contract_week_ending_date date, contract_week_status contract_week_status_enum, additional_seq integer, is_adjustment boolean, qr_status timesheet_qr_status_enum, pay_adjustment_count integer, has_pay_adjustments boolean, is_adjusted boolean, is_qr boolean, needs_attention boolean, client_autoprocess_hr boolean, has_rate_issue boolean, has_pay_channel_issue boolean, hr_crosscheck_status text, hr_crosscheck_issues text[], external_source_rows_json jsonb, issue_codes text[], client_requires_hr boolean, client_no_timesheet_required boolean, client_is_nhsp boolean, client_pay_reference_required boolean, client_invoice_reference_required boolean, client_hr_validation_required boolean, client_ts_reference_required boolean, require_reference_to_pay boolean, require_reference_to_invoice boolean, qr_token text, qr_generated_at timestamp with time zone, qr_scanned_at timestamp with time zone, candidate_hint_text jsonb, expenses_pay_ex_vat numeric, expenses_description text, mileage_units numeric, mileage_pay_rate numeric, mileage_charge_rate numeric, mileage_pay_ex_vat numeric, travel_pay_ex_vat numeric, travel_charge_ex_vat numeric, accommodation_pay_ex_vat numeric, accommodation_charge_ex_vat numeric, other_pay_ex_vat numeric, other_charge_ex_vat numeric, hr_validation_required_for_invoice boolean, invoice_segments_total integer, invoice_segments_locked integer, invoice_segments_unlocked integer, invoice_segment_stage text, tools_stage text, processing_status_display text, invoice_is_paid boolean, refs_block_invoicing boolean, refs_block_issuing_invoices boolean, refs_block_invoice_and_issuing boolean, pay_icon_code text, pay_status_code text, pay_paid_at_utc timestamp with time zone, net_delta_ex_vat numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_row_key_timesheet_ids uuid[] := NULL;
  v_row_key_contract_week_ids uuid[] := NULL;
  v_has_contract_week_row_key boolean := FALSE;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_numeric_re text := E'^[-+]?[0-9]+(\\.[0-9]+)?$';
BEGIN
  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId' AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  ELSIF v_filters ? 'rowKey' AND NULLIF(BTRIM(COALESCE(v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'rowKey'), '')];
  END IF;


  IF v_row_keys IS NOT NULL THEN
    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_timesheet_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 11)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 10)) = 'timesheet:'
        AND SUBSTRING(row_key_values.row_key_value FROM 11) ~* v_uuid_re
    ) AS parsed_values;

    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_contract_week_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 15)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 14)) = 'contract_week:'
        AND SUBSTRING(row_key_values.row_key_value FROM 15) ~* v_uuid_re
    ) AS parsed_values;

    v_has_contract_week_row_key := COALESCE(ARRAY_LENGTH(v_row_key_contract_week_ids, 1), 0) > 0;

    IF v_row_key_timesheet_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_timesheet_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_timesheet_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;

    IF v_row_key_contract_week_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_contract_week_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_contract_week_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_contract_week_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');
  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

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

  RETURN QUERY
  WITH client_hr AS MATERIALIZED (
    SELECT
      cs0.client_id,
      BOOL_OR(cs0.autoprocess_hr) AS autoprocess_hr,
      BOOL_OR(cs0.requires_hr) AS requires_hr,
      BOOL_OR(cs0.no_timesheet_required) AS no_timesheet_required,
      BOOL_OR(cs0.pay_reference_required) AS pay_reference_required,
      BOOL_OR(cs0.invoice_reference_required) AS invoice_reference_required,
      BOOL_OR(cs0.reference_number_required_to_issue_invoice) AS reference_number_required_to_issue_invoice,
      BOOL_OR(cs0.hr_validation_required) AS hr_validation_required,
      BOOL_OR(cs0.ts_reference_required) AS ts_reference_required,
      BOOL_OR(cs0.is_nhsp) AS is_nhsp
    FROM public.client_settings AS cs0
    GROUP BY cs0.client_id
  ),
  timesheet_scope_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id,
      cw0.id AS contract_week_id,
      COALESCE(cw0.week_ending_date, ts0.week_ending_date) AS effective_week_ending_date,
      ts0.sheet_scope AS effective_sheet_scope,
      COALESCE(ts0.contract_id, cw0.contract_id) AS effective_contract_id
    FROM public.timesheets AS ts0
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.timesheet_id = ts0.timesheet_id
    WHERE ts0.is_current = TRUE
      AND (v_timesheet_ids IS NULL OR ts0.timesheet_id = ANY(v_timesheet_ids))
      AND (
        v_contract_week_ids IS NULL
        OR cw0.id = ANY(v_contract_week_ids)
        OR v_timesheet_ids IS NOT NULL
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_row_keys IS NULL
        OR ('timesheet:' || ts0.timesheet_id::text) = ANY(v_row_keys)
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_week_ending_date IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR UPPER(ts0.sheet_scope::text) = v_period_filter
      )
  ),
  contract_week_scope_rows AS MATERIALIZED (
    SELECT
      cw0.id AS contract_week_id,
      cw0.contract_id,
      cw0.week_ending_date AS effective_week_ending_date
    FROM public.contract_weeks AS cw0
    JOIN public.contracts AS ct_scope
      ON ct_scope.id = cw0.contract_id
    WHERE cw0.timesheet_id IS NULL
      AND (
        v_timesheet_ids IS NULL
        OR v_contract_week_ids IS NOT NULL
        OR v_has_contract_week_row_key = TRUE
      )
      AND (
        COALESCE(v_dataset_mode, 'PROCESS') <> 'AUTHORISE'
        OR v_has_contract_week_row_key = TRUE
      )
      AND (v_candidate_id IS NULL OR ct_scope.candidate_id = v_candidate_id)
      AND (v_client_id IS NULL OR ct_scope.client_id = v_client_id)
      AND (v_contract_week_ids IS NULL OR cw0.id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR ('contract_week:' || cw0.id::text) = ANY(v_row_keys)
      )
      AND (
        v_week_ending_date IS NULL
        OR cw0.week_ending_date = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR cw0.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR cw0.week_ending_date <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR cw0.week_ending_date >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR cw0.week_ending_date <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR v_period_filter = 'WEEKLY'
      )
  ),
  tf_ranked AS MATERIALIZED (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.candidate_id,
      tf0.client_id,
      tf0.pay_method,
      tf0.processing_status,
      tf0.basis,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.locked_by_invoice_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.invoice_breakdown_json,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_description,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.created_at,
      tf0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tf
        WHERE ts_scope_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS MATERIALIZED (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.candidate_id,
      tf1.client_id,
      tf1.pay_method,
      tf1.processing_status,
      tf1.basis,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.locked_by_invoice_id,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.external_source_rows_json,
      tf1.invoice_breakdown_json,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_description,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS MATERIALIZED (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.created_at,
      tv0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tv
        WHERE ts_scope_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS MATERIALIZED (
    SELECT
      tv1.timesheet_id,
      tv1.status
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  evidence_agg AS MATERIALIZED (
    SELECT
      te0.timesheet_id,
      COUNT(te0.id) FILTER (
        WHERE NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      )::integer AS evidence_count,
      COALESCE(BOOL_OR(
        UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET'
        AND NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      ), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(
        UPPER(COALESCE(te0.kind, '')) = 'MILEAGE'
        AND NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      ), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(
        UPPER(COALESCE(te0.kind, '')) = 'TRAVEL'
        AND NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      ), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(
        UPPER(COALESCE(te0.kind, '')) = 'ACCOMMODATION'
        AND NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      ), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(
        UPPER(COALESCE(te0.kind, '')) = 'OTHER'
        AND NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
      ), FALSE) AS has_other_evidence
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_te
        WHERE ts_scope_te.timesheet_id = te0.timesheet_id
      )
    GROUP BY te0.timesheet_id
  ),
  nhsp_agg AS MATERIALIZED (
    SELECT
      ns0.timesheet_id,
      COUNT(ns0.id)::integer AS nhsp_shift_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'INCLUDED'))::integer AS nhsp_shift_included_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'DEFERRED'))::integer AS nhsp_shift_deferred_count
    FROM public.nhsp_shifts AS ns0
    WHERE ns0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_ns
        WHERE ts_scope_ns.timesheet_id = ns0.timesheet_id
      )
    GROUP BY ns0.timesheet_id
  ),
  pay_adjustments_agg AS MATERIALIZED (
    SELECT
      pa0.timesheet_id,
      COUNT(pa0.id)::integer AS pay_adjustment_count
    FROM public.ts_pay_adjustments AS pa0
    WHERE pa0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_pa
        WHERE ts_scope_pa.timesheet_id = pa0.timesheet_id
      )
    GROUP BY pa0.timesheet_id
  ),
  timesheet_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id AS timesheet_id,
      ts0.status AS timesheet_status,
      ts0.week_ending_date AS week_ending_date,
      ts0.booking_id AS booking_id,
      ts0.occupant_key_norm AS occupant_key_norm,
      ts0.hospital_norm AS hospital_norm,
      ts0.sheet_scope AS sheet_scope,
      ts0.submission_mode AS submission_mode,
      ts0.authorised_at_server AS authorised_at_server,
      COALESCE(tf2.candidate_id, ct0.candidate_id) AS candidate_id,
      COALESCE(tf2.client_id, ct0.client_id) AS client_id,
      tf2.pay_method AS pay_method,
      tf2.processing_status AS processing_status,
      tf2.basis AS basis,
      tf2.total_hours AS total_hours,
      tf2.total_pay_ex_vat AS total_pay_ex_vat,
      tf2.total_charge_ex_vat AS total_charge_ex_vat,
      tf2.margin_ex_vat AS margin_ex_vat,
      tf2.paid_at_utc AS paid_at_utc,
      tf2.pay_on_hold AS pay_on_hold,
      tf2.locked_by_invoice_id AS locked_by_invoice_id,
      CASE
        WHEN COALESCE(tf2.candidate_id, ct0.candidate_id) IS NULL
         AND ts0.candidate_hint_text IS NOT NULL
         AND jsonb_typeof(ts0.candidate_hint_text) = 'object'
         AND (
           NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL
         ) THEN
          'Unresolved Timesheet - '
          || COALESCE(
            NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), ''),
            NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), ''),
            'Candidate'
          )
          || CASE
               WHEN NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL THEN ', Email - ' || BTRIM(ts0.candidate_hint_text->>'email')
               ELSE ''
             END
        ELSE COALESCE(cand0.display_name, ts0.occupant_key_norm)
      END AS candidate_name,
      cli0.name AS client_name,
      COALESCE(ns1.nhsp_shift_count, 0) AS nhsp_shift_count,
      COALESCE(ns1.nhsp_shift_included_count, 0) AS nhsp_shift_included_count,
      COALESCE(ns1.nhsp_shift_deferred_count, 0) AS nhsp_shift_deferred_count,
      tv2.status AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      COALESCE(ts0.is_adjustment, cw0.is_adjustment, FALSE) AS is_adjustment,
      ts0.qr_status AS qr_status,
      ts0.qr_token AS qr_token,
      ts0.qr_generated_at AS qr_generated_at,
      ts0.qr_scanned_at AS qr_scanned_at,
      ts0.qr_last_sent_hash AS qr_last_sent_hash,
      COALESCE(pa1.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.hr_validation_required,
        FALSE
      ) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      COALESCE(tf2.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf2.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      tf2.hr_crosscheck_status AS hr_crosscheck_status,
      tf2.hr_crosscheck_issues AS hr_crosscheck_issues,
      tf2.external_source_rows_json AS external_source_rows_json,
      ts0.reference_number AS reference_number,
      ts0.day_references_json AS day_references_json,
      ts0.actual_schedule_json AS actual_schedule_json,
      ts0.r2_nurse_key AS r2_nurse_key,
      ts0.r2_auth_key AS r2_auth_key,
      ts0.manual_pdf_r2_key AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      COALESCE(ev1.evidence_count, 0) AS evidence_count,
      COALESCE(ev1.has_timesheet_evidence, FALSE) AS has_timesheet_evidence,
      COALESCE(ev1.has_mileage_evidence, FALSE) AS has_mileage_evidence,
      COALESCE(ev1.has_travel_evidence, FALSE) AS has_travel_evidence,
      COALESCE(ev1.has_accommodation_evidence, FALSE) AS has_accommodation_evidence,
      COALESCE(ev1.has_other_evidence, FALSE) AS has_other_evidence,
      ts0.candidate_hint_text AS candidate_hint_text,
      tf2.expenses_pay_ex_vat AS expenses_pay_ex_vat,
      tf2.expenses_description AS expenses_description,
      tf2.mileage_units AS mileage_units,
      tf2.mileage_pay_rate AS mileage_pay_rate,
      tf2.mileage_charge_rate AS mileage_charge_rate,
      tf2.mileage_pay_ex_vat AS mileage_pay_ex_vat,
      tf2.mileage_charge_ex_vat AS mileage_charge_ex_vat,
      tf2.travel_pay_ex_vat AS travel_pay_ex_vat,
      tf2.travel_charge_ex_vat AS travel_charge_ex_vat,
      tf2.accommodation_pay_ex_vat AS accommodation_pay_ex_vat,
      tf2.accommodation_charge_ex_vat AS accommodation_charge_ex_vat,
      tf2.other_pay_ex_vat AS other_pay_ex_vat,
      tf2.other_charge_ex_vat AS other_charge_ex_vat,
      tf2.invoice_breakdown_json AS invoice_breakdown_json
    FROM timesheet_scope_rows AS ts_scope
    JOIN public.timesheets AS ts0
      ON ts0.timesheet_id = ts_scope.timesheet_id
     AND ts0.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.id = ts_scope.contract_week_id
    LEFT JOIN public.contracts AS ct0
      ON ct0.id = COALESCE(ts0.contract_id, cw0.contract_id)
    LEFT JOIN tf_latest AS tf2
      ON tf2.timesheet_id = ts0.timesheet_id
    LEFT JOIN tv_latest AS tv2
      ON tv2.timesheet_id = ts0.timesheet_id
    LEFT JOIN evidence_agg AS ev1
      ON ev1.timesheet_id = ts0.timesheet_id
    LEFT JOIN nhsp_agg AS ns1
      ON ns1.timesheet_id = ts0.timesheet_id
    LEFT JOIN pay_adjustments_agg AS pa1
      ON pa1.timesheet_id = ts0.timesheet_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = COALESCE(tf2.candidate_id, ct0.candidate_id)
    LEFT JOIN public.clients AS cli0
      ON cli0.id = COALESCE(tf2.client_id, ct0.client_id)
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = COALESCE(tf2.client_id, ct0.client_id)
  ),
  planned_week_rows AS MATERIALIZED (
    SELECT
      NULL::uuid AS timesheet_id,
      NULL::public.timesheet_status_enum AS timesheet_status,
      cw0.week_ending_date AS week_ending_date,
      NULL::text AS booking_id,
      NULL::text AS occupant_key_norm,
      NULL::text AS hospital_norm,
      'WEEKLY'::public.timesheet_scope_enum AS sheet_scope,
      cw0.submission_mode_snapshot AS submission_mode,
      NULL::timestamp with time zone AS authorised_at_server,
      ct0.candidate_id AS candidate_id,
      ct0.client_id AS client_id,
      NULL::text AS pay_method,
      NULL::public.ts_fin_processing_status_enum AS processing_status,
      NULL::public.timesheet_fin_basis_enum AS basis,
      ROUND(
        (
          CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,day}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,day}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,night}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,night}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sat}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sat}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sun}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sun}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,bh}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,bh}')::numeric ELSE 0::numeric END
        ),
        2
      ) AS total_hours,
      NULL::numeric AS total_pay_ex_vat,
      NULL::numeric AS total_charge_ex_vat,
      NULL::numeric AS margin_ex_vat,
      NULL::timestamp with time zone AS paid_at_utc,
      FALSE AS pay_on_hold,
      NULL::uuid AS locked_by_invoice_id,
      cand0.display_name AS candidate_name,
      cli0.name AS client_name,
      0::integer AS nhsp_shift_count,
      0::integer AS nhsp_shift_included_count,
      0::integer AS nhsp_shift_deferred_count,
      NULL::public.validation_status_enum AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      cw0.is_adjustment AS is_adjustment,
      NULL::public.timesheet_qr_status_enum AS qr_status,
      NULL::text AS qr_token,
      NULL::timestamp with time zone AS qr_generated_at,
      NULL::timestamp with time zone AS qr_scanned_at,
      NULL::text AS qr_last_sent_hash,
      0::integer AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.hr_validation_required,
        FALSE
      ) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      FALSE AS has_rate_issue,
      FALSE AS has_pay_channel_issue,
      NULL::text AS hr_crosscheck_status,
      NULL::text[] AS hr_crosscheck_issues,
      NULL::jsonb AS external_source_rows_json,
      NULL::text AS reference_number,
      NULL::jsonb AS day_references_json,
      NULL::jsonb AS actual_schedule_json,
      NULL::text AS r2_nurse_key,
      NULL::text AS r2_auth_key,
      NULL::text AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      0::integer AS evidence_count,
      FALSE AS has_timesheet_evidence,
      FALSE AS has_mileage_evidence,
      FALSE AS has_travel_evidence,
      FALSE AS has_accommodation_evidence,
      FALSE AS has_other_evidence,
      NULL::jsonb AS candidate_hint_text,
      NULL::numeric AS expenses_pay_ex_vat,
      NULL::text AS expenses_description,
      NULL::numeric AS mileage_units,
      NULL::numeric AS mileage_pay_rate,
      NULL::numeric AS mileage_charge_rate,
      NULL::numeric AS mileage_pay_ex_vat,
      NULL::numeric AS mileage_charge_ex_vat,
      NULL::numeric AS travel_pay_ex_vat,
      NULL::numeric AS travel_charge_ex_vat,
      NULL::numeric AS accommodation_pay_ex_vat,
      NULL::numeric AS accommodation_charge_ex_vat,
      NULL::numeric AS other_pay_ex_vat,
      NULL::numeric AS other_charge_ex_vat,
      NULL::jsonb AS invoice_breakdown_json
    FROM contract_week_scope_rows AS cw_scope
    JOIN public.contract_weeks AS cw0
      ON cw0.id = cw_scope.contract_week_id
    JOIN public.contracts AS ct0
      ON ct0.id = cw0.contract_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = ct0.candidate_id
    LEFT JOIN public.clients AS cli0
      ON cli0.id = ct0.client_id
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = ct0.client_id
  ),
  base_union AS MATERIALIZED (
    SELECT
      tr0.timesheet_id,
      tr0.timesheet_status,
      tr0.week_ending_date,
      tr0.booking_id,
      tr0.occupant_key_norm,
      tr0.hospital_norm,
      tr0.sheet_scope,
      tr0.submission_mode,
      tr0.authorised_at_server,
      tr0.candidate_id,
      tr0.client_id,
      tr0.pay_method,
      tr0.processing_status,
      tr0.basis,
      tr0.total_hours,
      tr0.total_pay_ex_vat,
      tr0.total_charge_ex_vat,
      tr0.margin_ex_vat,
      tr0.paid_at_utc,
      tr0.pay_on_hold,
      tr0.locked_by_invoice_id,
      tr0.candidate_name,
      tr0.client_name,
      tr0.nhsp_shift_count,
      tr0.nhsp_shift_included_count,
      tr0.nhsp_shift_deferred_count,
      tr0.validation_status,
      tr0.contract_week_id,
      tr0.contract_week_ending_date,
      tr0.contract_week_status,
      tr0.additional_seq,
      tr0.is_adjustment,
      tr0.qr_status,
      tr0.qr_token,
      tr0.qr_generated_at,
      tr0.qr_scanned_at,
      tr0.qr_last_sent_hash,
      tr0.pay_adjustment_count,
      tr0.client_autoprocess_hr,
      tr0.client_requires_hr,
      tr0.client_no_timesheet_required,
      tr0.client_pay_reference_required,
      tr0.client_invoice_reference_required,
      tr0.client_hr_validation_required,
      tr0.client_ts_reference_required,
      tr0.client_is_nhsp,
      tr0.has_rate_issue,
      tr0.has_pay_channel_issue,
      tr0.hr_crosscheck_status,
      tr0.hr_crosscheck_issues,
      tr0.external_source_rows_json,
      tr0.reference_number,
      tr0.day_references_json,
      tr0.actual_schedule_json,
      tr0.r2_nurse_key,
      tr0.r2_auth_key,
      tr0.manual_pdf_r2_key,
      tr0.require_reference_to_pay,
      tr0.require_reference_to_invoice,
      tr0.evidence_count,
      tr0.has_timesheet_evidence,
      tr0.has_mileage_evidence,
      tr0.has_travel_evidence,
      tr0.has_accommodation_evidence,
      tr0.has_other_evidence,
      tr0.candidate_hint_text,
      tr0.expenses_pay_ex_vat,
      tr0.expenses_description,
      tr0.mileage_units,
      tr0.mileage_pay_rate,
      tr0.mileage_charge_rate,
      tr0.mileage_pay_ex_vat,
      tr0.mileage_charge_ex_vat,
      tr0.travel_pay_ex_vat,
      tr0.travel_charge_ex_vat,
      tr0.accommodation_pay_ex_vat,
      tr0.accommodation_charge_ex_vat,
      tr0.other_pay_ex_vat,
      tr0.other_charge_ex_vat,
      tr0.invoice_breakdown_json
    FROM timesheet_rows AS tr0
    UNION ALL
    SELECT
      pwr0.timesheet_id,
      pwr0.timesheet_status,
      pwr0.week_ending_date,
      pwr0.booking_id,
      pwr0.occupant_key_norm,
      pwr0.hospital_norm,
      pwr0.sheet_scope,
      pwr0.submission_mode,
      pwr0.authorised_at_server,
      pwr0.candidate_id,
      pwr0.client_id,
      pwr0.pay_method,
      pwr0.processing_status,
      pwr0.basis,
      pwr0.total_hours,
      pwr0.total_pay_ex_vat,
      pwr0.total_charge_ex_vat,
      pwr0.margin_ex_vat,
      pwr0.paid_at_utc,
      pwr0.pay_on_hold,
      pwr0.locked_by_invoice_id,
      pwr0.candidate_name,
      pwr0.client_name,
      pwr0.nhsp_shift_count,
      pwr0.nhsp_shift_included_count,
      pwr0.nhsp_shift_deferred_count,
      pwr0.validation_status,
      pwr0.contract_week_id,
      pwr0.contract_week_ending_date,
      pwr0.contract_week_status,
      pwr0.additional_seq,
      pwr0.is_adjustment,
      pwr0.qr_status,
      pwr0.qr_token,
      pwr0.qr_generated_at,
      pwr0.qr_scanned_at,
      pwr0.qr_last_sent_hash,
      pwr0.pay_adjustment_count,
      pwr0.client_autoprocess_hr,
      pwr0.client_requires_hr,
      pwr0.client_no_timesheet_required,
      pwr0.client_pay_reference_required,
      pwr0.client_invoice_reference_required,
      pwr0.client_hr_validation_required,
      pwr0.client_ts_reference_required,
      pwr0.client_is_nhsp,
      pwr0.has_rate_issue,
      pwr0.has_pay_channel_issue,
      pwr0.hr_crosscheck_status,
      pwr0.hr_crosscheck_issues,
      pwr0.external_source_rows_json,
      pwr0.reference_number,
      pwr0.day_references_json,
      pwr0.actual_schedule_json,
      pwr0.r2_nurse_key,
      pwr0.r2_auth_key,
      pwr0.manual_pdf_r2_key,
      pwr0.require_reference_to_pay,
      pwr0.require_reference_to_invoice,
      pwr0.evidence_count,
      pwr0.has_timesheet_evidence,
      pwr0.has_mileage_evidence,
      pwr0.has_travel_evidence,
      pwr0.has_accommodation_evidence,
      pwr0.has_other_evidence,
      pwr0.candidate_hint_text,
      pwr0.expenses_pay_ex_vat,
      pwr0.expenses_description,
      pwr0.mileage_units,
      pwr0.mileage_pay_rate,
      pwr0.mileage_charge_rate,
      pwr0.mileage_pay_ex_vat,
      pwr0.mileage_charge_ex_vat,
      pwr0.travel_pay_ex_vat,
      pwr0.travel_charge_ex_vat,
      pwr0.accommodation_pay_ex_vat,
      pwr0.accommodation_charge_ex_vat,
      pwr0.other_pay_ex_vat,
      pwr0.other_charge_ex_vat,
      pwr0.invoice_breakdown_json
    FROM planned_week_rows AS pwr0
  ),
  issue_rows AS MATERIALIZED (
    SELECT
      bu0.*,
      (
        ARRAY[]::text[]
        || CASE
             WHEN COALESCE(bu0.has_rate_issue, FALSE) = TRUE OR bu0.processing_status = 'RATE_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Rate'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.has_pay_channel_issue, FALSE) = TRUE OR bu0.processing_status = 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Pay channel'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.processing_status = 'UNASSIGNED'::public.ts_fin_processing_status_enum THEN ARRAY['Candidate ID'::text]
             WHEN bu0.processing_status = 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum THEN ARRAY['Client ID'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.pay_on_hold, FALSE) = TRUE THEN ARRAY['On hold'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND (
                    bu0.hr_crosscheck_status = 'HOURS_MISMATCH_HR'
                    OR COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HOURS_MISMATCH_HR'::text]
                  ) THEN ARRAY['Hours mismatch HR'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HR_HOURS_MISSING'::text] THEN ARRAY['HR hours missing'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text] THEN ARRAY['Duplicate contracts'::text]
             ELSE ARRAY[]::text[]
           END

        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND (
                    ((COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_travel_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_accommodation_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_other_evidence, FALSE) = FALSE)
                  ) THEN ARRAY['Expenses evidence'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.mileage_units, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.has_mileage_evidence, FALSE) = FALSE THEN ARRAY['Mileage evidence'::text]
             ELSE ARRAY[]::text[]
           END


        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND (bu0.validation_status IS NULL OR bu0.validation_status = 'PENDING'::public.validation_status_enum) THEN ARRAY['Awaiting validation'::text]
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND bu0.validation_status IS NOT NULL
              AND bu0.validation_status <> ALL (ARRAY['VALIDATION_OK'::public.validation_status_enum, 'OVERRIDDEN'::public.validation_status_enum, 'PENDING'::public.validation_status_enum]) THEN ARRAY['Validation failed'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_requires_hr, FALSE) = TRUE
              AND COALESCE(bu0.client_autoprocess_hr, FALSE) = FALSE
              AND bu0.authorised_at_server IS NULL THEN ARRAY['Authorisation'::text]
             ELSE ARRAY[]::text[]
           END
      ) AS lightweight_issue_codes
    FROM base_union AS bu0
  ),
  segment_rows AS MATERIALIZED (
    SELECT
      ir0.*,
      segment_stats.seg_total AS invoice_segments_total_calc,
      segment_stats.seg_locked AS invoice_segments_locked_calc,
      COALESCE(invoice_paid_stats.invoice_paid_any, FALSE) AS invoice_is_paid_calc
    FROM issue_rows AS ir0
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN jsonb_array_length(ir0.invoice_breakdown_json->'segments')::integer
          ELSE 1::integer
        END AS seg_total,
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN (
             SELECT COUNT(*)::integer
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS lock_segment(segment_value)
             WHERE NULLIF(BTRIM(COALESCE(lock_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
           )
          ELSE CASE WHEN ir0.locked_by_invoice_id IS NULL THEN 0::integer ELSE 1::integer END
        END AS seg_locked
    ) AS segment_stats
      ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN FALSE
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN EXISTS (
             SELECT 1
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS paid_segment(segment_value)
             JOIN public.invoices AS inv2
               ON inv2.id = CASE
                 WHEN NULLIF(BTRIM(COALESCE(paid_segment.segment_value->>'invoice_locked_invoice_id', '')), '') ~* v_uuid_re THEN (paid_segment.segment_value->>'invoice_locked_invoice_id')::uuid
                 ELSE NULL::uuid
               END
             WHERE inv2.status = 'PAID'::public.invoice_status_enum
                OR inv2.paid_at_utc IS NOT NULL
           )
          ELSE EXISTS (
             SELECT 1
             FROM public.invoices AS inv3
             WHERE inv3.id = ir0.locked_by_invoice_id
               AND (inv3.status = 'PAID'::public.invoice_status_enum OR inv3.paid_at_utc IS NOT NULL)
           )
        END AS invoice_paid_any
    ) AS invoice_paid_stats
      ON TRUE
  ),
  issue_normalised_rows AS MATERIALIZED (
    SELECT
      sr0.*,
      COALESCE(sr0.lightweight_issue_codes, ARRAY[]::text[]) AS workbench_issue_codes
    FROM segment_rows AS sr0
  ),
  result_rows AS MATERIALIZED (
    SELECT
      sr0.timesheet_id,
      sr0.timesheet_status,
      sr0.week_ending_date,
      sr0.booking_id,
      sr0.occupant_key_norm,
      sr0.hospital_norm,
      sr0.sheet_scope,
      sr0.submission_mode,
      sr0.authorised_at_server,
      sr0.candidate_id,
      sr0.client_id,
      sr0.pay_method,
      sr0.processing_status,
      sr0.basis,
      sr0.total_hours,
      sr0.total_pay_ex_vat,
      sr0.total_charge_ex_vat,
      sr0.margin_ex_vat,
      sr0.paid_at_utc,
      sr0.pay_on_hold,
      FALSE AS ready_to_pay,
      sr0.locked_by_invoice_id,
      sr0.candidate_name,
      sr0.client_name,
      sr0.nhsp_shift_count,
      sr0.nhsp_shift_included_count,
      sr0.nhsp_shift_deferred_count,
      sr0.validation_status,
      CASE
        WHEN sr0.timesheet_id IS NOT NULL
         AND EXISTS (
           SELECT 1
           FROM public.timesheets AS archived_timesheet
           WHERE archived_timesheet.timesheet_id = sr0.timesheet_id
             AND archived_timesheet.archived_at_utc IS NOT NULL
         ) THEN 'ARCHIVED'
        WHEN sr0.timesheet_id IS NULL THEN CASE sr0.contract_week_status
          WHEN 'PLANNED'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'OPEN'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'SUBMITTED'::public.contract_week_status_enum THEN 'PENDING_AUTH'
          WHEN 'AUTHORISED'::public.contract_week_status_enum THEN 'READY_FOR_INVOICE'
          WHEN 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED'
          WHEN 'CANCELLED'::public.contract_week_status_enum THEN 'NEEDS_ATTENTION'
          ELSE 'UNKNOWN'
        END
        WHEN sr0.paid_at_utc IS NOT NULL THEN 'PAID'
        WHEN sr0.locked_by_invoice_id IS NOT NULL
          OR (
            sr0.invoice_segments_total_calc IS NOT NULL
            AND sr0.invoice_segments_total_calc > 0
            AND COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc
          ) THEN 'INVOICED'
        WHEN sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'READY_FOR_INVOICE'
        WHEN sr0.processing_status = 'READY_FOR_HR'::public.ts_fin_processing_status_enum THEN 'READY_FOR_HR'
        WHEN sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum THEN 'PENDING_AUTH'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum]) THEN 'NEEDS_ATTENTION'
        ELSE 'UNKNOWN'
      END AS summary_stage,
      CASE
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'DAILY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'DAILY_MANUAL'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_is_nhsp, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE THEN 'WEEKLY_HEALTHROSTER'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_is_nhsp, FALSE) = TRUE THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'WEEKLY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'WEEKLY_MANUAL'
        ELSE 'UNKNOWN'
      END AS route_type,
      sr0.contract_week_id,
      sr0.contract_week_ending_date,
      sr0.contract_week_status,
      sr0.additional_seq,
      sr0.is_adjustment,
      sr0.qr_status,
      sr0.pay_adjustment_count,
      (COALESCE(sr0.pay_adjustment_count, 0) > 0) AS has_pay_adjustments,
      (COALESCE(sr0.is_adjustment, FALSE) OR COALESCE(sr0.pay_adjustment_count, 0) > 0) AS is_adjusted,
      (sr0.qr_status IS NOT NULL) AS is_qr,
      (
        sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum])
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND sr0.hr_crosscheck_status IS NOT NULL
          AND sr0.hr_crosscheck_status <> 'OK'
        )
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND COALESCE(sr0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text]
        )
        OR COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) > 0
      ) AS needs_attention,
      sr0.client_autoprocess_hr,
      sr0.has_rate_issue,
      sr0.has_pay_channel_issue,
      sr0.hr_crosscheck_status,
      sr0.hr_crosscheck_issues,
      sr0.external_source_rows_json,
      COALESCE(sr0.workbench_issue_codes, ARRAY[]::text[]) AS issue_codes,
      sr0.client_requires_hr,
      sr0.client_no_timesheet_required,
      sr0.client_is_nhsp,
      sr0.client_pay_reference_required,
      sr0.client_invoice_reference_required,
      sr0.client_hr_validation_required,
      sr0.client_ts_reference_required,
      sr0.require_reference_to_pay,
      sr0.require_reference_to_invoice,
      sr0.qr_token,
      sr0.qr_generated_at,
      sr0.qr_scanned_at,
      sr0.candidate_hint_text,
      sr0.expenses_pay_ex_vat,
      sr0.expenses_description,
      sr0.mileage_units,
      sr0.mileage_pay_rate,
      sr0.mileage_charge_rate,
      sr0.mileage_pay_ex_vat,
      sr0.travel_pay_ex_vat,
      sr0.travel_charge_ex_vat,
      sr0.accommodation_pay_ex_vat,
      sr0.accommodation_charge_ex_vat,
      sr0.other_pay_ex_vat,
      sr0.other_charge_ex_vat,
      (
        sr0.timesheet_id IS NOT NULL
        AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
        AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
        AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
      ) AS hr_validation_required_for_invoice,
      sr0.invoice_segments_total_calc AS invoice_segments_total,
      sr0.invoice_segments_locked_calc AS invoice_segments_locked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::integer
        ELSE GREATEST(sr0.invoice_segments_total_calc - COALESCE(sr0.invoice_segments_locked_calc, 0), 0)
      END AS invoice_segments_unlocked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::text
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) = 0 THEN 'NOT_INVOICED'
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc THEN 'FULLY_INVOICED'
        ELSE 'PARTIALLY_INVOICED'
      END AS invoice_segment_stage,
      CASE
        WHEN sr0.timesheet_id IS NOT NULL
         AND EXISTS (
           SELECT 1
           FROM public.timesheets AS archived_timesheet
           WHERE archived_timesheet.timesheet_id = sr0.timesheet_id
             AND archived_timesheet.archived_at_utc IS NOT NULL
         ) THEN 'ARCHIVED'
        WHEN sr0.timesheet_id IS NULL THEN 'UNPROCESSED'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN 'INVOICED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'PROCESSED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'AUTHORISED_FOR_INVOICING'
        ELSE 'PROCESSING_DELAYED'
      END AS tools_stage,
      CASE
        WHEN sr0.timesheet_id IS NOT NULL
         AND EXISTS (
           SELECT 1
           FROM public.timesheets AS archived_timesheet
           WHERE archived_timesheet.timesheet_id = sr0.timesheet_id
             AND archived_timesheet.archived_at_utc IS NOT NULL
         ) THEN 'Archived'
        WHEN sr0.timesheet_id IS NULL THEN 'Unprocessed'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'Unprocessed'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN
          CASE
            WHEN sr0.invoice_segments_total_calc IS NOT NULL
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) > 0
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) < sr0.invoice_segments_total_calc THEN 'Partially Invoiced'
            ELSE 'Invoiced'
          END
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'Processed'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'Authorised for Invoicing'
        ELSE 'Processing Delayed'
      END AS processing_status_display,
      COALESCE(sr0.invoice_is_paid_calc, FALSE) AS invoice_is_paid,
      FALSE AS refs_block_invoicing,
      FALSE AS refs_block_issuing_invoices,
      FALSE AS refs_block_invoice_and_issuing,
      CASE
        WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
          THEN summary_pay_cache.summary_pay_icon_code
        WHEN tps.summary_pay_icon_code IS NOT NULL THEN tps.summary_pay_icon_code
        WHEN tps.summary_pay_status_code = 'PAID' THEN 'COIN'
        WHEN tps.summary_pay_status_code = 'PARTIALLY_PAID' THEN 'HALF_COIN'
        WHEN tps.summary_pay_status_code IN ('PROCESSING','ADVANCED') THEN 'CLOCK'
        WHEN tps.summary_pay_status_code = 'UNPAID' THEN 'NONE'
        WHEN tps.last_settled_at_utc IS NOT NULL OR sr0.paid_at_utc IS NOT NULL THEN 'COIN'
        ELSE 'NONE'
      END::text AS pay_icon_code,
      COALESCE(
        CASE
          WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
            THEN summary_pay_cache.summary_pay_status_code
          ELSE NULL
        END,
        tps.summary_pay_status_code,
        CASE
          WHEN tps.last_settled_at_utc IS NOT NULL OR sr0.paid_at_utc IS NOT NULL THEN 'PAID'
          ELSE 'UNPAID'
        END
      )::text AS pay_status_code,
      CASE
        WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
          THEN summary_pay_cache.last_paid_at_utc
        WHEN tps.summary_pay_status_code IS NOT NULL OR tps.summary_pay_icon_code IS NOT NULL THEN tps.summary_pay_paid_at_utc
        ELSE COALESCE(tps.last_settled_at_utc, sr0.paid_at_utc)
      END AS pay_paid_at_utc,
      COALESCE(
        CASE
          WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
            THEN summary_pay_cache.net_delta_ex_vat
          ELSE NULL
        END,
        tps.summary_net_delta_ex_vat,
        0
      )::numeric AS net_delta_ex_vat
    FROM issue_normalised_rows AS sr0
    LEFT JOIN public.timesheet_summary_pay_state_cache AS summary_pay_cache
      ON summary_pay_cache.timesheet_id = sr0.timesheet_id
    LEFT JOIN public.timesheet_pay_state AS tps
      ON tps.timesheet_id = sr0.timesheet_id
  )
  SELECT
    rr0.timesheet_id,
    rr0.timesheet_status,
    rr0.week_ending_date,
    rr0.booking_id,
    rr0.occupant_key_norm,
    rr0.hospital_norm,
    rr0.sheet_scope,
    rr0.submission_mode,
    rr0.authorised_at_server,
    rr0.candidate_id,
    rr0.client_id,
    rr0.pay_method,
    rr0.processing_status,
    rr0.basis,
    rr0.total_hours,
    rr0.total_pay_ex_vat,
    rr0.total_charge_ex_vat,
    rr0.margin_ex_vat,
    rr0.paid_at_utc,
    rr0.pay_on_hold,
    rr0.ready_to_pay,
    rr0.locked_by_invoice_id,
    rr0.candidate_name,
    rr0.client_name,
    rr0.nhsp_shift_count,
    rr0.nhsp_shift_included_count,
    rr0.nhsp_shift_deferred_count,
    rr0.validation_status,
    rr0.summary_stage,
    rr0.route_type,
    rr0.contract_week_id,
    rr0.contract_week_ending_date,
    rr0.contract_week_status,
    rr0.additional_seq,
    rr0.is_adjustment,
    rr0.qr_status,
    rr0.pay_adjustment_count,
    rr0.has_pay_adjustments,
    rr0.is_adjusted,
    rr0.is_qr,
    rr0.needs_attention,
    rr0.client_autoprocess_hr,
    rr0.has_rate_issue,
    rr0.has_pay_channel_issue,
    rr0.hr_crosscheck_status,
    rr0.hr_crosscheck_issues,
    rr0.external_source_rows_json,
    rr0.issue_codes,
    rr0.client_requires_hr,
    rr0.client_no_timesheet_required,
    rr0.client_is_nhsp,
    rr0.client_pay_reference_required,
    rr0.client_invoice_reference_required,
    rr0.client_hr_validation_required,
    rr0.client_ts_reference_required,
    rr0.require_reference_to_pay,
    rr0.require_reference_to_invoice,
    rr0.qr_token,
    rr0.qr_generated_at,
    rr0.qr_scanned_at,
    rr0.candidate_hint_text,
    rr0.expenses_pay_ex_vat,
    rr0.expenses_description,
    rr0.mileage_units,
    rr0.mileage_pay_rate,
    rr0.mileage_charge_rate,
    rr0.mileage_pay_ex_vat,
    rr0.travel_pay_ex_vat,
    rr0.travel_charge_ex_vat,
    rr0.accommodation_pay_ex_vat,
    rr0.accommodation_charge_ex_vat,
    rr0.other_pay_ex_vat,
    rr0.other_charge_ex_vat,
    rr0.hr_validation_required_for_invoice,
    rr0.invoice_segments_total,
    rr0.invoice_segments_locked,
    rr0.invoice_segments_unlocked,
    rr0.invoice_segment_stage,
    rr0.tools_stage,
    rr0.processing_status_display,
    rr0.invoice_is_paid,
    rr0.refs_block_invoicing,
    rr0.refs_block_issuing_invoices,
    rr0.refs_block_invoice_and_issuing,
    rr0.pay_icon_code,
    rr0.pay_status_code,
    rr0.pay_paid_at_utc,
    rr0.net_delta_ex_vat
  FROM result_rows AS rr0
  WHERE (v_candidate_id IS NULL OR rr0.candidate_id = v_candidate_id)
    AND (v_client_id IS NULL OR rr0.client_id = v_client_id);
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
  v_duplicate_expense_review jsonb := '{}'::jsonb;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'AUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_duplicate_expense_review:=private._timesheet_duplicate_expense_review_v1(
    v_current_ts.timesheet_id
  );
  IF COALESCE((v_duplicate_expense_review->>'required')::boolean,false)
     AND COALESCE(current_setting('cloudtms.duplicate_expense_reviewed',true),'')<>'true' THEN
    RAISE EXCEPTION 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
      USING ERRCODE='PT409',DETAIL=jsonb_build_object(
        'code','DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        'timesheet_id',v_current_ts.timesheet_id,
        'categories',COALESCE(v_duplicate_expense_review->'categories','[]'::jsonb)
      )::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  SELECT
    COALESCE(vts.client_requires_hr, COALESCE(v_contract_requires_hr, false)),
    COALESCE(vts.hr_validation_required_for_invoice, COALESCE(v_contract_requires_hr, false)),
    CASE
      WHEN vts.validation_status IS NULL THEN NULL::text
      ELSE UPPER(vts.validation_status::text)
    END
    INTO v_client_requires_hr,
         v_hr_validation_required_for_invoice,
         v_validation_status
  FROM public.v_timesheets_summary_base AS vts
  WHERE vts.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_timesheet_rotation_scope(ARRAY[v_current_ts.timesheet_id]) AS rs
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = rs.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
  ) INTO v_has_uncleared_advance_override;

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;
-- CloudTMS deployment metadata: deterministic owner and API-role ACLs.
ALTER FUNCTION public.timesheet_authorise_generic_atomic(uuid, uuid, uuid, timestamp with time zone, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_authorise_generic_atomic(uuid, uuid, uuid, timestamp with time zone, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_authorise_generic_atomic(uuid, uuid, uuid, timestamp with time zone, text) TO authenticated, service_role;

CREATE OR REPLACE FUNCTION public.timesheet_authorise_reviewed_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamp with time zone DEFAULT now(),
  p_expected_row_signature text DEFAULT NULL::text,
  p_duplicate_expense_confirmed boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  IF p_duplicate_expense_confirmed IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'DUPLICATE_EXPENSE_REVIEW_CONFIRMATION_REQUIRED'
      USING ERRCODE='PT409',DETAIL=jsonb_build_object(
        'code','DUPLICATE_EXPENSE_REVIEW_CONFIRMATION_REQUIRED',
        'timesheet_id',p_timesheet_id
      )::text;
  END IF;
  PERFORM set_config('cloudtms.duplicate_expense_reviewed','true',true);
  RETURN public.timesheet_authorise_generic_atomic(
    p_timesheet_id,p_expected_timesheet_id,p_actor_user_id,p_now_utc,p_expected_row_signature
  );
END;
$function$;
ALTER FUNCTION public.timesheet_authorise_reviewed_atomic(uuid, uuid, uuid, timestamp with time zone, text, boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_authorise_reviewed_atomic(uuid, uuid, uuid, timestamp with time zone, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_authorise_reviewed_atomic(uuid, uuid, uuid, timestamp with time zone, text, boolean)
  TO service_role;

CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_capability_items jsonb := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  IF to_regclass('pg_temp.import_review_lifecycle_capability_v1') IS NOT NULL
     AND nullif(current_setting('cloudtms.import_reconciliation_capability_token',true),'') IS NOT NULL THEN
    IF coalesce(current_setting('request.jwt.claim.role',true),
         nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','') <> 'service_role' THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_INVALID' USING ERRCODE='42501';
    END IF;
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',c.timesheet_id,'expected_timesheet_id',c.expected_timesheet_id,
      'expected_row_signature',c.expected_row_signature) ORDER BY c.timesheet_id),'[]'::jsonb)
    INTO v_capability_items
    FROM pg_temp.import_review_lifecycle_capability_v1 c
    WHERE c.capability_token=current_setting('cloudtms.import_reconciliation_capability_token',true)
      AND c.txid=txid_current() AND c.actor_user_id=p_actor_user_id AND c.action='AUTHORISE';
    IF jsonb_array_length(v_capability_items)=0 OR
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_items_array) x)
       IS DISTINCT FROM
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_capability_items) x) THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_ITEM_SET_MISMATCH' USING ERRCODE='22023';
    END IF;
    v_items_array:=v_capability_items;
  ELSE
    v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'AUTHORISE', p_actor_user_id, 100);
  END IF;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.qr_status AS current_qr_status,
    cur_ts.qr_token AS current_qr_token,
    cur_ts.qr_generated_at AS current_qr_generated_at,
    cur_ts.qr_scanned_at AS current_qr_scanned_at,
    cur_ts.sheet_scope AS current_sheet_scope,
    cur_ts.contract_id AS current_contract_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.basis AS tsfin_basis,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    COALESCE(summary_row.client_requires_hr, contract_row.requires_hr, false) AS client_requires_hr,
    COALESCE(summary_row.hr_validation_required_for_invoice, contract_row.requires_hr, false) AS hr_validation_required_for_invoice,
    COALESCE(summary_row.validation_status_text, NULL::text) AS summary_validation_status,
    cw.id AS contract_week_id,
    cw.status AS contract_week_status,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock,
    COALESCE(validation_state.validation_ok, false) AS validation_ok
    ,private._timesheet_duplicate_expense_review_v1(cur_ts.timesheet_id) AS duplicate_expense_review_json
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT c.requires_hr
    FROM public.contracts AS c
    WHERE c.id = cur_ts.contract_id
    LIMIT 1
  ) AS contract_row ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(vts.client_requires_hr, false) AS client_requires_hr,
      COALESCE(vts.hr_validation_required_for_invoice, false) AS hr_validation_required_for_invoice,
      CASE
        WHEN vts.validation_status IS NULL THEN NULL::text
        ELSE UPPER(vts.validation_status::text)
      END AS validation_status_text
    FROM public.v_timesheets_summary_base AS vts
    WHERE vts.timesheet_id = cur_ts.timesheet_id
    LIMIT 1
  ) AS summary_row ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw_sel.timesheet_id
           AND cw_ts.booking_id = cur_ts.booking_id
       )
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END,
             cw_sel.updated_at DESC NULLS LAST,
             cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON true
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(UPPER(COALESCE(summary_row.validation_status_text, tv.status::text)) IN ('VALIDATION_OK', 'OVERRIDDEN'), false) AS validation_ok
    FROM public.timesheet_validations AS tv
    WHERE tv.timesheet_id = cur_ts.timesheet_id
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1
  ) AS validation_state ON true;

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'INVOICED'::public.contract_week_status_enum THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'CANCELLED'::public.contract_week_status_enum THEN 'CONTRACT_WEEK_NOT_AUTHORISABLE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'AUTHORISED'::public.contract_week_status_enum THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NOT NULL OR state_rows.tsfin_authorised_at_utc IS NOT NULL THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.tsfin_processing_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN 'AUTHORISE_NOT_ALLOWED'
      WHEN COALESCE((state_rows.duplicate_expense_review_json->>'required')::boolean,false) THEN 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.hr_validation_required_for_invoice, false) AND NOT COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows;

  -- A linked correction pair is one lifecycle unit.  An already-authorised
  -- sibling in a repairable legacy mixed state is idempotent, while any real
  -- blocker is propagated to both members before either member is mutated.
  UPDATE pg_temp.timesheet_authorise_bulk_work work_rows
     SET failure_code=NULL
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND work_rows.failure_code='ALREADY_AUTHORISED';

  UPDATE pg_temp.timesheet_authorise_bulk_work work_rows
     SET failure_code='CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
   WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
     AND EXISTS (
       SELECT 1 FROM pg_temp.timesheet_authorise_bulk_work blocked
       WHERE blocked.item_json->>'lifecycle_group_id'=work_rows.item_json->>'lifecycle_group_id'
         AND blocked.failure_code IS NOT NULL
     );

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = p_actor_user_id,
           authorised_at_utc = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.timesheet_authorise_bulk_work work_rows
    JOIN public.timesheets current_pair
      ON current_pair.timesheet_id=work_rows.current_timesheet_id
    JOIN public.timesheets_financials current_tf
      ON current_tf.timesheet_id=current_pair.timesheet_id AND current_tf.is_current=true
    WHERE NULLIF(work_rows.item_json->>'lifecycle_group_id','') IS NOT NULL
      AND work_rows.failure_code IS NULL
    GROUP BY work_rows.item_json->>'lifecycle_group_id',
             (work_rows.item_json->>'lifecycle_group_size')::integer
    HAVING count(*)<>(work_rows.item_json->>'lifecycle_group_size')::integer
       OR count(*) FILTER (WHERE current_pair.authorised_at_server IS NOT NULL
                            AND current_tf.authorised_at_utc IS NOT NULL)<>count(*)
  ) THEN
    RAISE EXCEPTION 'CORRECTION_PAIR_LIFECYCLE_POSTCONDITION_FAILED' USING ERRCODE='P0001';
  END IF;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_authorise:' || v_now::text,
    'TIMESHEET_BULK_AUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'lifecycle_group_id', NULLIF(work_rows.item_json ->> 'lifecycle_group_id', ''),
      'pair_fingerprint', NULLIF(work_rows.item_json ->> 'pair_fingerprint', ''),
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;
-- CloudTMS deployment metadata: deterministic owner and API-role ACLs.
ALTER FUNCTION public.timesheet_authorise_bulk_atomic(jsonb, uuid, timestamp with time zone) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_authorise_bulk_atomic(jsonb, uuid, timestamp with time zone) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_authorise_bulk_atomic(jsonb, uuid, timestamp with time zone) TO authenticated, service_role;

CREATE OR REPLACE FUNCTION public.bulk_authorise_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_classification text := NULL;
  v_show_daily boolean := TRUE;
  v_show_weekly boolean := TRUE;
  v_show_manual boolean := TRUE;
  v_show_qr boolean := TRUE;
  v_show_electronic boolean := TRUE;
  v_validation_already boolean := TRUE;
  v_validation_awaiting boolean := TRUE;
  v_show_authorised_invoiced_unissued boolean := FALSE;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_classification := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'classification', v_filters->>'classificationRaw', '')), ''));
  IF v_classification NOT IN ('TIMESHEETS', 'NHSP', 'HR') THEN
    v_classification := NULL;
  END IF;

  v_show_daily := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_weekly := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_manual', v_filters->>'showManual', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_qr := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_qr', v_filters->>'showQr', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_electronic := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_electronic', v_filters->>'showElectronic', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_already := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_already', v_filters->>'validationAlready', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_awaiting := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_awaiting', v_filters->>'validationAwaiting', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_authorised_invoiced_unissued := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_authorised_invoiced_unissued', v_filters->>'showAuthorisedInvoicedUnissued', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  IF EXISTS(
    SELECT 1
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters||jsonb_build_object('disable_paging',true,'apply_paging',false,'profile','list','include_evidence',false,'include_compare',false,'include_import_source_rows',false)
    ) s
    JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = s.timesheet_id
    WHERE correction_timesheet.is_current=true and correction_timesheet.archived_at_utc is null
      and coalesce(correction_timesheet.is_adjustment,false) and correction_timesheet.parent_timesheet_id is not null
      and correction_timesheet.correction_id is not null and upper(coalesce(correction_timesheet.adjustment_origin,''))='IMPORT_CORRECTION'
      and correction_timesheet.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and ((upper(coalesce(s.route_type,'')) like '%NHSP%' or upper(coalesce(s.basis,'')) like 'NHSP%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
        = (upper(coalesce(s.route_type,'')) like '%HEALTHROSTER%' or upper(coalesce(s.basis,'')) like 'HEALTHROSTER%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER'))
  ) THEN
    RAISE EXCEPTION 'BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT' USING ERRCODE='22023';
  END IF;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT
      summary_row.*,
      correction_timesheet.is_current AS correction_is_current,
      correction_timesheet.archived_at_utc AS correction_archived_at_utc,
      correction_timesheet.is_adjustment AS correction_is_adjustment,
      correction_timesheet.parent_timesheet_id AS correction_parent_timesheet_id,
      correction_timesheet.correction_id,
      correction_timesheet.adjustment_origin,
      correction_timesheet.correction_kind,
      correction_timesheet.candidate_hint_text AS correction_candidate_hint_text
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    LEFT JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = summary_row.timesheet_id
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and lightweight_rows.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER')
        THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
        THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.sheet_scope, lightweight_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN lightweight_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(lightweight_rows.processing_status, lightweight_rows.tools_stage, lightweight_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(lightweight_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
        OR EXISTS (
          SELECT 1
          FROM public.invoice_lines AS issued_invoice_line
          JOIN public.invoices AS issued_invoice
            ON issued_invoice.id = issued_invoice_line.invoice_id
          WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
            AND (
              issued_invoice.issued_at_utc IS NOT NULL
              OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
            )
        )
      ) AS locked_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS draft_invoice_line
        JOIN public.invoices AS draft_invoice
          ON draft_invoice.id = draft_invoice_line.invoice_id
        WHERE draft_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND draft_invoice.issued_at_utc IS NULL
          AND UPPER(COALESCE(draft_invoice.status::text, '')) = 'DRAFT'
      ) AS has_unissued_invoice_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS issued_invoice_line
        JOIN public.invoices AS issued_invoice
          ON issued_invoice.id = issued_invoice_line.invoice_id
        WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND (
            issued_invoice.issued_at_utc IS NOT NULL
            OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
          )
      ) AS has_issued_invoice_calc,
      COALESCE(lightweight_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(lightweight_rows.qr_status, '')) = 'PENDING'
        AND lightweight_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(lightweight_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(lightweight_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER') then 'HR'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION' then 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HR'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(lightweight_rows.timesheet_id::text, ''),
        COALESCE(lightweight_rows.contract_week_id::text, ''),
        COALESCE(lightweight_rows.processing_status, ''),
        COALESCE(lightweight_rows.summary_stage, ''),
        COALESCE(lightweight_rows.tools_stage, ''),
        COALESCE(lightweight_rows.authorised_at_utc::text, ''),
        COALESCE(lightweight_rows.authorised_at_server::text, ''),
        COALESCE(lightweight_rows.processed_at_utc::text, ''),
        COALESCE(lightweight_rows.paid_at_utc::text, ''),
        COALESCE(lightweight_rows.invoice_segments_locked::text, ''),
        COALESCE(lightweight_rows.route_family, ''),
        COALESCE(lightweight_rows.route_subfamily, ''),
        COALESCE(lightweight_rows.validation_status, ''),
        COALESCE(lightweight_rows.attached_evidence_count::text, ''),
        COALESCE(lightweight_rows.primary_artifact_storage_key, '')
      )) AS row_signature_calc
    FROM lightweight_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_unauthorise_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR (classified_rows.contract_week_id IS NOT NULL AND classified_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND COALESCE(classified_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        classified_rows.locked_calc = TRUE
        OR classified_rows.authorised_calc = TRUE
        OR classified_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc
    FROM classified_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status,
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'correction_id',decision_rows.correction_id,
        'correction_kind',decision_rows.correction_kind,
        'adjustment_origin',decision_rows.adjustment_origin,
        'correction_source_system',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case when decision_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
        'correction_display_label',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
        'bulk_authorise_section',
        CASE
          WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible'
          WHEN decision_rows.timesheet_id IS NOT NULL
            AND decision_rows.authorised_calc = TRUE
            AND decision_rows.locked_calc = FALSE
            AND decision_rows.has_issued_invoice_calc = FALSE
            AND (decision_rows.has_unissued_invoice_calc = FALSE OR v_show_authorised_invoiced_unissued = TRUE)
            THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        CASE
          WHEN decision_rows.has_issued_invoice_calc THEN 'ISSUED'
          WHEN decision_rows.has_unissued_invoice_calc THEN 'DRAFT'
          ELSE NULL::text
        END,
        'has_unissued_invoice',
        decision_rows.has_unissued_invoice_calc,
        'has_issued_invoice',
        decision_rows.has_issued_invoice_calc,
        'is_invoiced',
        decision_rows.has_unissued_invoice_calc OR decision_rows.has_issued_invoice_calc,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        decision_rows.is_adjusted,
        'is_adjustment',
        decision_rows.is_adjusted,
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        decision_rows.has_any_evidence,
        'attached_evidence_count',
        decision_rows.attached_evidence_count,
        'evidence_count',
        decision_rows.attached_evidence_count,
        'primary_artifact_id',
        NULL::text,
        'primary_artifact_kind',
        CASE WHEN decision_rows.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET' ELSE NULL::text END,
        'primary_artifact_storage_key',
        decision_rows.primary_artifact_storage_key,
        'primary_artifact_display_name',
        decision_rows.primary_artifact_display_name,
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        decision_rows.row_signature_calc
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc)
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        JSONB_BUILD_ARRAY(
          JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(decision_rows.has_any_evidence, FALSE), 'has_evidence', COALESCE(decision_rows.has_any_evidence, FALSE)),
          JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', decision_rows.has_any_evidence,
          'attached_evidence_count', decision_rows.attached_evidence_count,
          'primary_artifact_storage_key', decision_rows.primary_artifact_storage_key,
          'primary_artifact_display_name', decision_rows.primary_artifact_display_name,
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  canonical_authorise_signature_rows AS MATERIALIZED (
    SELECT canonical_patch.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      JSONB_BUILD_OBJECT(
        'dataset_mode', 'authorise',
        'projection', 'dataset_row',
        'row_keys', COALESCE((
          SELECT JSONB_AGG(payload_rows.row_json->>'row_key' ORDER BY payload_rows.row_json->>'row_key')
          FROM payload_rows
          WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'row_key', '')), '') IS NOT NULL
        ), '[]'::jsonb)
      )
    ) AS canonical_patch(row_json)
  ),
  canonical_payload_rows AS MATERIALIZED (
    SELECT
      (
        payload_rows.row_json
        || jsonb_strip_nulls(jsonb_build_object(
          'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'mutation_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'mutation_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'summary_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'summary_stage', '')), ''),
          'tools_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', '')), ''),
          'processing_status', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'processing_status', '')), '')
        ))
        || jsonb_build_object(
          'has_retained_financial_history', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'can_unprocess', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_action_visible', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
          'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
          'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'can_archive', FALSE,
          'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
        )
        || jsonb_build_object(
          'action_flags',
            COALESCE(payload_rows.row_json->'action_flags', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'action_flags', '{}'::jsonb)
            || jsonb_build_object(
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
              'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'can_archive', FALSE,
              'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
            ),
          'row_patch',
            COALESCE(payload_rows.row_json->'row_patch', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'row_patch', '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), '')
            ))
        )
      ) AS row_json
    FROM payload_rows
    LEFT JOIN canonical_authorise_signature_rows
      ON canonical_authorise_signature_rows.row_json->>'row_key' = payload_rows.row_json->>'row_key'
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT canonical_payload_rows.row_json
    FROM canonical_payload_rows
    WHERE NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(canonical_payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS MATERIALIZED (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS MATERIALIZED (
    SELECT classification_filtered_rows.row_json
    FROM classification_filtered_rows
    WHERE (
        UPPER(COALESCE(classification_filtered_rows.row_json->>'bulk_authorise_classification', '')) <> 'TIMESHEETS'
        OR (
          CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'DAILY' THEN v_show_daily
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' THEN v_show_weekly
            ELSE TRUE
          END
          AND CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'MANUAL_NON_QR' THEN v_show_manual
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'QR' THEN v_show_qr
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'ELECTRONIC' THEN v_show_electronic
            ELSE v_show_manual AND v_show_qr AND v_show_electronic
          END
          AND CASE
            WHEN COALESCE((classification_filtered_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN v_validation_awaiting
            ELSE v_validation_already
          END
        )
      )
  ),
  paged_visible_rows AS MATERIALIZED (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  visible_counts AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible')::integer AS processed_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible')::integer AS authorised_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR')::integer AS manual_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR')::integer AS qr_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC')::integer AS electronic_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE)::integer AS already_validated_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE)::integer AS awaiting_validation_count
    FROM visible_rows
  ),
  eligible_counts AS (
    SELECT
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS')::integer AS timesheets_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP')::integer AS nhsp_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR')::integer AS hr_count
    FROM eligible_rows_before_classification
  ),
  rows_payload AS (
    SELECT COALESCE(
      JSONB_AGG(
        paged_visible_rows.row_json
        ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key'
      ),
      '[]'::jsonb
    ) AS rows_json
    FROM paged_visible_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', COALESCE(visible_counts.total_count, 0),
      'processed_eligible', COALESCE(visible_counts.processed_eligible_count, 0),
      'authorised_eligible', COALESCE(visible_counts.authorised_eligible_count, 0),
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', COALESCE(eligible_counts.timesheets_count, 0),
        'NHSP', COALESCE(eligible_counts.nhsp_count, 0),
        'HR', COALESCE(eligible_counts.hr_count, 0)
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', COALESCE(visible_counts.manual_count, 0),
        'qr', COALESCE(visible_counts.qr_count, 0),
        'electronic', COALESCE(visible_counts.electronic_count, 0)
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', COALESCE(visible_counts.already_validated_count, 0),
        'awaiting_validation', COALESCE(visible_counts.awaiting_validation_count, 0),
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE(rows_payload.rows_json, '[]'::jsonb)
  )
  INTO v_out
  FROM visible_counts
  CROSS JOIN eligible_counts
  CROSS JOIN rows_payload;

  RETURN private._candidate_dataset_overlay_v1(COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', 0,
      'processed_eligible', 0,
      'authorised_eligible', 0,
      'by_classification', JSONB_BUILD_OBJECT('TIMESHEETS', 0, 'NHSP', 0, 'HR', 0),
      'timesheets_by_type', JSONB_BUILD_OBJECT('manual', 0, 'qr', 0, 'electronic', 0),
      'validation', JSONB_BUILD_OBJECT('already_validated', 0, 'awaiting_validation', 0)
    ),
    'rows', '[]'::jsonb
  )));
END;
$function$;
