CREATE OR REPLACE FUNCTION public.bulk_process_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_show_weekly_manual boolean := TRUE;
  v_show_daily_manual boolean := TRUE;
  v_out jsonb;
BEGIN
  v_show_weekly_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly_manual', v_filters->>'showWeeklyManual', v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_daily_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily_manual', v_filters->>'showDailyManual', v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  WITH decision_rows AS (
    SELECT decision_result.row_json
    FROM public.bulk_timesheet_row_decision_v1(v_filters || JSONB_BUILD_OBJECT('dataset_mode', 'process')) AS decision_result(row_json)
  ),
  manual_rows AS (
    SELECT decision_rows.row_json
    FROM decision_rows
    WHERE UPPER(COALESCE(decision_rows.row_json->>'route_family', '')) = 'MANUAL_NON_QR'
      AND (
        (UPPER(COALESCE(decision_rows.row_json->>'period_type', decision_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(decision_rows.row_json->>'period_type', decision_rows.row_json->>'sheet_scope', '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(decision_rows.row_json->>'period_type', decision_rows.row_json->>'sheet_scope', '')) NOT IN ('WEEKLY', 'DAILY')
      )
  ),
  unprocessed_rows AS (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'UNPROCESSED'
  ),
  processed_rows AS (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'PROCESSED'
      AND NULLIF(BTRIM(COALESCE(manual_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND COALESCE((manual_rows.row_json->>'locked')::boolean, FALSE) = FALSE
      AND COALESCE((manual_rows.row_json->>'is_authorised')::boolean, FALSE) = FALSE
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*)::integer FROM unprocessed_rows) AS unprocessed_count,
      (SELECT COUNT(*)::integer FROM processed_rows) AS processed_count
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual
    ),
    'counts', JSONB_BUILD_OBJECT(
      'unprocessed', counts.unprocessed_count,
      'processed', counts.processed_count,
      'total', counts.unprocessed_count + counts.processed_count
    ),
    'unprocessed_rows', COALESCE((
      SELECT JSONB_AGG(unprocessed_rows.row_json ORDER BY unprocessed_rows.row_json->>'week_ending_date', unprocessed_rows.row_json->>'client_name', unprocessed_rows.row_json->>'candidate_name', unprocessed_rows.row_json->>'row_key')
      FROM unprocessed_rows
    ), '[]'::jsonb),
    'processed_rows', COALESCE((
      SELECT JSONB_AGG(processed_rows.row_json ORDER BY processed_rows.row_json->>'week_ending_date', processed_rows.row_json->>'client_name', processed_rows.row_json->>'candidate_name', processed_rows.row_json->>'row_key')
      FROM processed_rows
    ), '[]'::jsonb)
  )
  INTO v_out
  FROM counts;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual
    ),
    'counts', JSONB_BUILD_OBJECT('unprocessed', 0, 'processed', 0, 'total', 0),
    'unprocessed_rows', '[]'::jsonb,
    'processed_rows', '[]'::jsonb
  ));
END;
$function$;

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

  WITH decision_rows AS (
    SELECT decision_result.row_json
    FROM public.bulk_timesheet_row_decision_v1(v_filters || JSONB_BUILD_OBJECT('dataset_mode', 'authorise')) AS decision_result(row_json)
  ),
  eligible_rows_before_classification AS (
    SELECT decision_rows.row_json
    FROM decision_rows
    WHERE NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(decision_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS (
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
  counts AS (
    SELECT
      (SELECT COUNT(*)::integer FROM visible_rows) AS total_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible') AS processed_eligible_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible') AS authorised_eligible_count,
      (SELECT COUNT(*)::integer FROM eligible_rows_before_classification WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS') AS timesheets_count,
      (SELECT COUNT(*)::integer FROM eligible_rows_before_classification WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP') AS nhsp_count,
      (SELECT COUNT(*)::integer FROM eligible_rows_before_classification WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR') AS hr_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR') AS manual_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR') AS qr_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC') AS electronic_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE) AS already_validated_count,
      (SELECT COUNT(*)::integer FROM visible_rows WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE) AS awaiting_validation_count
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
      'validation_awaiting', v_validation_awaiting
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', counts.total_count,
      'processed_eligible', counts.processed_eligible_count,
      'authorised_eligible', counts.authorised_eligible_count,
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', counts.timesheets_count,
        'NHSP', counts.nhsp_count,
        'HR', counts.hr_count
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', counts.manual_count,
        'qr', counts.qr_count,
        'electronic', counts.electronic_count
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', counts.already_validated_count,
        'awaiting_validation', counts.awaiting_validation_count
      )
    ),
    'rows', COALESCE((
      SELECT JSONB_AGG(visible_rows.row_json ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key')
      FROM visible_rows
    ), '[]'::jsonb)
  )
  INTO v_out
  FROM counts;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
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
      'validation_awaiting', v_validation_awaiting
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
  ));
END;
$function$;


CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_decision_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_q text := NULL;
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_q_like text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
BEGIN
  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

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
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  END IF;

  RETURN QUERY
  WITH tf_ranked AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.processing_status,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.invoice_breakdown_json,
      tf0.external_source_rows_json,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.actual_schedule_json,
      tf0.additional_units_json,
      tf0.updated_at,
      tf0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
  ),
  tf_latest AS (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.timesheet_version,
      tf1.basis,
      tf1.is_stale,
      tf1.stale_reason,
      tf1.worked_start_iso,
      tf1.worked_end_iso,
      tf1.break_start_iso,
      tf1.break_end_iso,
      tf1.break_minutes,
      tf1.candidate_id,
      tf1.client_id,
      tf1.role,
      tf1.band,
      tf1.pay_method,
      tf1.policy_snapshot_json,
      tf1.processing_status,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.computed_at_utc,
      tf1.locked_by_invoice_id,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.pay_on_hold_reason,
      tf1.processed_by_user_id,
      tf1.processed_at_utc,
      tf1.authorised_by_user_id,
      tf1.authorised_at_utc,
      tf1.invoice_breakdown_json,
      tf1.external_source_rows_json,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_charge_ex_vat,
      tf1.expenses_description,
      tf1.expenses_evidence_r2_key,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.mileage_evidence_r2_key,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat,
      tf1.actual_schedule_json,
      tf1.additional_units_json,
      tf1.updated_at,
      tf1.created_at
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.pre_validated,
      tv0.validated_at_utc,
      tv0.updated_at,
      tv0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
  ),
  tv_latest AS (
    SELECT
      tv1.id,
      tv1.timesheet_id,
      tv1.status,
      tv1.reason_code,
      tv1.pre_validated,
      tv1.validated_at_utc,
      tv1.updated_at,
      tv1.created_at
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  te_ranked AS (
    SELECT
      te0.id,
      te0.timesheet_id,
      te0.kind,
      te0.display_name,
      te0.storage_key,
      te0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY te0.timesheet_id
        ORDER BY te0.created_at ASC NULLS LAST, te0.id ASC
      ) AS rn
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
  ),
  te_primary AS (
    SELECT
      te1.id,
      te1.timesheet_id,
      te1.kind,
      te1.display_name,
      te1.storage_key
    FROM te_ranked AS te1
    WHERE te1.rn = 1
  ),
  te_agg AS (
    SELECT
      te2.timesheet_id,
      COUNT(te2.id)::integer AS attached_evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence
    FROM public.timesheet_evidence AS te2
    GROUP BY te2.timesheet_id
  ),
  mq_ranked AS (
    SELECT
      mq0.id,
      mq0.r2_key,
      mq0.original_filename,
      mq0.mime_type,
      mq0.uploaded_at_utc,
      mq0.last_rotation_deg,
      mq0.meta_json,
      NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') AS contract_week_id_text,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
        ORDER BY mq0.uploaded_at_utc ASC NULLS LAST, mq0.id ASC
      ) AS rn
    FROM public.manual_timesheet_queue AS mq0
    WHERE UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') IS NOT NULL
  ),
  mq_primary AS (
    SELECT
      mq1.id,
      mq1.r2_key,
      mq1.original_filename,
      mq1.mime_type,
      mq1.last_rotation_deg,
      mq1.contract_week_id_text
    FROM mq_ranked AS mq1
    WHERE mq1.rn = 1
  ),
  mq_agg AS (
    SELECT
      mq2.contract_week_id_text,
      COUNT(mq2.id)::integer AS queue_staged_count
    FROM mq_ranked AS mq2
    GROUP BY mq2.contract_week_id_text
  ),
  raw_rows AS (
    SELECT
      vb.timesheet_id AS timesheet_id,
      vb.timesheet_status::text AS timesheet_status,
      vb.week_ending_date AS week_ending_date,
      vb.booking_id AS booking_id,
      vb.occupant_key_norm AS occupant_key_norm,
      vb.hospital_norm AS hospital_norm,
      vb.sheet_scope::text AS sheet_scope,
      vb.submission_mode::text AS submission_mode,
      vb.authorised_at_server AS authorised_at_server,
      COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) AS candidate_id,
      COALESCE(vb.client_id, tf.client_id, ct.client_id) AS client_id,
      COALESCE(vb.pay_method, tf.pay_method, cand.pay_method) AS pay_method,
      COALESCE(tf.processing_status::text, vb.processing_status::text) AS processing_status,
      COALESCE(tf.basis::text, vb.basis::text) AS basis,
      COALESCE(tf.total_hours, vb.total_hours) AS total_hours,
      COALESCE(tf.total_pay_ex_vat, vb.total_pay_ex_vat) AS total_pay_ex_vat,
      COALESCE(tf.total_charge_ex_vat, vb.total_charge_ex_vat) AS total_charge_ex_vat,
      COALESCE(tf.margin_ex_vat, vb.margin_ex_vat) AS margin_ex_vat,
      COALESCE(tf.paid_at_utc, vb.paid_at_utc) AS paid_at_utc,
      COALESCE(tf.pay_on_hold, vb.pay_on_hold, FALSE) AS pay_on_hold,
      vb.ready_to_pay AS ready_to_pay,
      COALESCE(tf.locked_by_invoice_id, vb.locked_by_invoice_id) AS locked_by_invoice_id,
      COALESCE(NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_name,
      COALESCE(NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_display_name,
      cand.first_name AS candidate_first_name,
      cand.last_name AS candidate_surname,
      cand.email AS candidate_email,
      COALESCE(cand.opt_in_email, TRUE) AS candidate_opt_in_email,
      COALESCE(NULLIF(BTRIM(vb.client_name), ''), NULLIF(BTRIM(cli.name), ''), vb.hospital_norm) AS client_name,
      COALESCE(NULLIF(BTRIM(cli.name), ''), NULLIF(BTRIM(vb.client_name), ''), vb.hospital_norm) AS client_display_name,
      vb.nhsp_shift_count AS nhsp_shift_count,
      vb.nhsp_shift_included_count AS nhsp_shift_included_count,
      vb.nhsp_shift_deferred_count AS nhsp_shift_deferred_count,
      COALESCE(tv.status::text, vb.validation_status::text) AS validation_status,
      vb.summary_stage AS summary_stage,
      vb.route_type AS route_type,
      vb.contract_week_id AS contract_week_id,
      vb.contract_week_ending_date AS contract_week_ending_date,
      COALESCE(vb.contract_week_status::text, cw.status::text) AS contract_week_status,
      COALESCE(vb.additional_seq, cw.additional_seq, 0) AS additional_seq,
      COALESCE(ts.is_adjustment, vb.is_adjustment, cw.is_adjustment, FALSE) AS is_adjustment,
      vb.qr_status::text AS vb_qr_status,
      ts.qr_status::text AS ts_qr_status,
      ts.qr_token AS qr_token,
      ts.qr_generated_at AS qr_generated_at,
      ts.qr_scanned_at AS qr_scanned_at,
      ts.qr_last_sent_hash AS qr_last_sent_hash,
      ts.qr_signed_hash AS qr_signed_hash,
      ts.qr_signed_at_utc AS qr_signed_at_utc,
      vb.qr_token AS vb_qr_token,
      vb.qr_generated_at AS vb_qr_generated_at,
      vb.qr_scanned_at AS vb_qr_scanned_at,
      COALESCE(vb.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(vb.has_pay_adjustments, FALSE) AS has_pay_adjustments,
      COALESCE(vb.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(vb.client_requires_hr, FALSE) AS client_requires_hr,
      COALESCE(vb.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(vb.client_pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(vb.client_invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(vb.client_hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(vb.client_ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(vb.client_is_nhsp, FALSE) AS client_is_nhsp,
      COALESCE(vb.hr_validation_required_for_invoice, FALSE) AS hr_validation_required_for_invoice,
      COALESCE(tf.has_rate_issue, vb.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf.has_pay_channel_issue, vb.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      COALESCE(tf.hr_crosscheck_status, vb.hr_crosscheck_status) AS hr_crosscheck_status,
      COALESCE(tf.hr_crosscheck_issues, vb.hr_crosscheck_issues) AS hr_crosscheck_issues,
      COALESCE(tf.external_source_rows_json, vb.external_source_rows_json) AS external_source_rows_json,
      COALESCE(vb.issue_codes, ARRAY[]::text[]) AS issue_codes,
      tf.worked_start_iso AS worked_start_iso,
      COALESCE(ts.contract_id, cw.contract_id, ct.id) AS contract_id,
      ct.role AS contract_role,
      ct.band AS contract_band,
      ct.display_site AS contract_display_site,
      ct.ward_hint AS contract_ward_hint,
      ct.std_schedule_json AS contract_std_schedule_json,
      ct.additional_rates_json AS contract_additional_rates_json,
      ct.mileage_pay_rate AS contract_mileage_pay_rate,
      ct.mileage_charge_rate AS contract_mileage_charge_rate,
      ct.default_submission_mode::text AS contract_default_submission_mode,
      ct.weekly_timesheet_source AS contract_weekly_timesheet_source,
      COALESCE(ct.is_nhsp, FALSE) AS contract_is_nhsp,
      COALESCE(ct.autoprocess_hr, FALSE) AS contract_autoprocess_hr,
      COALESCE(ct.requires_hr, FALSE) AS contract_requires_hr,
      COALESCE(ct.no_timesheet_required, FALSE) AS contract_no_timesheet_required,
      COALESCE(ct.daily_calc_of_invoices, FALSE) AS contract_daily_calc_of_invoices,
      COALESCE(ct.group_nightsat_sunbh, FALSE) AS contract_group_nightsat_sunbh,
      COALESCE(ct.is_ad_hoc, FALSE) AS contract_is_ad_hoc,
      vb.candidate_hint_text AS candidate_hint_text,
      COALESCE(tf.expenses_pay_ex_vat, vb.expenses_pay_ex_vat) AS expenses_pay_ex_vat,
      COALESCE(tf.expenses_description, vb.expenses_description) AS expenses_description,
      COALESCE(tf.mileage_units, vb.mileage_units) AS mileage_units,
      COALESCE(tf.mileage_pay_rate, vb.mileage_pay_rate) AS mileage_pay_rate,
      COALESCE(tf.mileage_charge_rate, vb.mileage_charge_rate) AS mileage_charge_rate,
      COALESCE(tf.mileage_pay_ex_vat, vb.mileage_pay_ex_vat) AS mileage_pay_ex_vat,
      COALESCE(tf.travel_pay_ex_vat, vb.travel_pay_ex_vat) AS travel_pay_ex_vat,
      COALESCE(tf.travel_charge_ex_vat, vb.travel_charge_ex_vat) AS travel_charge_ex_vat,
      COALESCE(tf.accommodation_pay_ex_vat, vb.accommodation_pay_ex_vat) AS accommodation_pay_ex_vat,
      COALESCE(tf.accommodation_charge_ex_vat, vb.accommodation_charge_ex_vat) AS accommodation_charge_ex_vat,
      COALESCE(tf.other_pay_ex_vat, vb.other_pay_ex_vat) AS other_pay_ex_vat,
      COALESCE(tf.other_charge_ex_vat, vb.other_charge_ex_vat) AS other_charge_ex_vat,
      vb.invoice_segments_total AS invoice_segments_total,
      vb.invoice_segments_locked AS invoice_segments_locked,
      vb.invoice_segments_unlocked AS invoice_segments_unlocked,
      vb.invoice_segment_stage AS invoice_segment_stage,
      vb.tools_stage AS tools_stage,
      vb.processing_status_display AS processing_status_display,
      vb.invoice_is_paid AS invoice_is_paid,
      vb.refs_block_invoicing AS refs_block_invoicing,
      vb.refs_block_issuing_invoices AS refs_block_issuing_invoices,
      vb.refs_block_invoice_and_issuing AS refs_block_invoice_and_issuing,
      COALESCE(vb.pay_icon_code, 'NONE') AS pay_icon_code,
      vb.pay_status_code AS pay_status_code,
      vb.pay_paid_at_utc AS pay_paid_at_utc,
      COALESCE(vb.net_delta_ex_vat, 0::numeric) AS net_delta_ex_vat,
      ts.version AS timesheet_version,
      ts.is_current AS timesheet_is_current,
      ts.created_at AS timesheet_created_at,
      ts.updated_at AS timesheet_updated_at,
      ts.worked_start_iso AS timesheet_worked_start_iso,
      ts.worked_end_iso AS timesheet_worked_end_iso,
      ts.scheduled_start_iso AS timesheet_scheduled_start_iso,
      ts.scheduled_end_iso AS timesheet_scheduled_end_iso,
      ts.break_start_iso AS timesheet_break_start_iso,
      ts.break_end_iso AS timesheet_break_end_iso,
      ts.break_minutes AS timesheet_break_minutes,
      ts.worked_minutes AS timesheet_worked_minutes,
      ts.reference_number AS reference_number,
      ts.r2_nurse_key AS r2_nurse_key,
      ts.r2_auth_key AS r2_auth_key,
      ts.manual_pdf_r2_key AS manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
      ts.generated_pdf_at_utc AS generated_pdf_at_utc,
      ts.actual_schedule_json AS timesheet_actual_schedule_json,
      ts.additional_units_week AS additional_units_week,
      ts.additional_units_per_day AS additional_units_per_day,
      ts.day_references_json AS day_references_json,
      ts.parent_timesheet_id AS parent_timesheet_id,
      ts.adjustment_origin AS adjustment_origin,
      ts.correction_id AS correction_id,
      ts.correction_kind AS correction_kind,
      cw.id AS cw_id,
      cw.status::text AS cw_status,
      cw.submission_mode_snapshot::text AS cw_submission_mode_snapshot,
      cw.timesheet_id AS cw_timesheet_id,
      cw.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
      cw.day_entries_json AS contract_week_day_entries_json,
      cw.totals_json AS contract_week_totals_json,
      cw.planned_schedule_json AS contract_week_planned_schedule_json,
      cw.updated_at AS contract_week_updated_at,
      cw.created_at AS contract_week_created_at,
      tf.id AS tsfin_id,
      tf.timesheet_version AS tsfin_timesheet_version,
      tf.is_stale AS tsfin_is_stale,
      tf.stale_reason AS tsfin_stale_reason,
      tf.computed_at_utc AS tsfin_computed_at_utc,
      tf.processed_by_user_id AS processed_by_user_id,
      tf.processed_at_utc AS processed_at_utc,
      pu.display_name AS processed_by_display,
      tf.authorised_by_user_id AS authorised_by_user_id,
      tf.authorised_at_utc AS authorised_at_utc,
      au.display_name AS authorised_by_display,
      tf.invoice_breakdown_json AS invoice_breakdown_json,
      tf.policy_snapshot_json AS policy_snapshot_json,
      tf.updated_at AS tsfin_updated_at,
      tv.pre_validated AS validation_pre_validated,
      tv.reason_code AS validation_reason_code,
      tv.validated_at_utc AS validation_validated_at_utc,
      te.attached_evidence_count AS attached_evidence_count,
      COALESCE(te.has_timesheet_evidence, FALSE) AS ev_timesheet,
      COALESCE(te.has_mileage_evidence, FALSE) AS ev_mileage,
      COALESCE(te.has_travel_evidence, FALSE) AS ev_travel,
      COALESCE(te.has_accommodation_evidence, FALSE) AS ev_accommodation,
      COALESCE(te.has_other_evidence, FALSE) AS ev_other,
      tep.id AS primary_evidence_id,
      tep.kind AS primary_evidence_kind,
      tep.display_name AS primary_evidence_display_name,
      tep.storage_key AS primary_evidence_storage_key,
      COALESCE(mqa.queue_staged_count, 0) AS queue_staged_count,
      mqp.id AS primary_queue_id,
      mqp.r2_key AS primary_queue_r2_key,
      mqp.original_filename AS primary_queue_original_filename,
      mqp.mime_type AS primary_queue_mime_type,
      mqp.last_rotation_deg AS primary_queue_rotation_degrees,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_electronic
        WHERE hist_electronic.booking_id = vb.booking_id
          AND hist_electronic.is_current = FALSE
          AND UPPER(COALESCE(hist_electronic.submission_mode::text, '')) = 'ELECTRONIC'
      ) AS has_historical_electronic,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_pending
        WHERE hist_qr_pending.booking_id = vb.booking_id
          AND hist_qr_pending.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_pending.qr_status::text, '')) = 'PENDING'
          AND hist_qr_pending.qr_generated_at IS NOT NULL
          AND hist_qr_pending.qr_scanned_at IS NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_pending.qr_token, '')), '') IS NOT NULL
      ) AS has_historical_qr_pending,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_signed
        WHERE hist_qr_signed.booking_id = vb.booking_id
          AND hist_qr_signed.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_signed.qr_status::text, '')) = 'USED'
          AND hist_qr_signed.qr_scanned_at IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_signed.manual_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_historical_qr_signed,
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' THEN tf.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_value)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) AS has_segment_invoice_lock
    FROM public.v_timesheets_summary_base AS vb
    LEFT JOIN public.timesheets AS ts
      ON ts.timesheet_id = vb.timesheet_id
     AND ts.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw
      ON cw.id = vb.contract_week_id
    LEFT JOIN public.contracts AS ct
      ON ct.id = COALESCE(ts.contract_id, cw.contract_id)
    LEFT JOIN tf_latest AS tf
      ON tf.timesheet_id = vb.timesheet_id
    LEFT JOIN tv_latest AS tv
      ON tv.timesheet_id = vb.timesheet_id
    LEFT JOIN te_agg AS te
      ON te.timesheet_id = vb.timesheet_id
    LEFT JOIN te_primary AS tep
      ON tep.timesheet_id = vb.timesheet_id
    LEFT JOIN mq_agg AS mqa
      ON mqa.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN mq_primary AS mqp
      ON mqp.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN public.candidates AS cand
      ON cand.id = COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id)
    LEFT JOIN public.clients AS cli
      ON cli.id = COALESCE(vb.client_id, tf.client_id, ct.client_id)
    LEFT JOIN public.tms_users AS pu
      ON pu.id = tf.processed_by_user_id
    LEFT JOIN public.tms_users AS au
      ON au.id = tf.authorised_by_user_id
    WHERE (v_candidate_id IS NULL OR COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) = v_candidate_id)
      AND (v_client_id IS NULL OR COALESCE(vb.client_id, tf.client_id, ct.client_id) = v_client_id)
      AND (v_timesheet_ids IS NULL OR vb.timesheet_id = ANY(v_timesheet_ids))
      AND (v_contract_week_ids IS NULL OR vb.contract_week_id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR CASE
             WHEN vb.timesheet_id IS NOT NULL THEN 'timesheet:' || vb.timesheet_id::text
             WHEN vb.contract_week_id IS NOT NULL THEN 'contract_week:' || vb.contract_week_id::text
             ELSE ''
           END = ANY(v_row_keys)
      )
      AND (
        v_q_like IS NULL
        OR CONCAT_WS(
             ' ',
             vb.candidate_name,
             cand.display_name,
             cand.first_name,
             cand.last_name,
             cand.email,
             vb.client_name,
             cli.name,
             vb.booking_id,
             vb.occupant_key_norm,
             vb.hospital_norm,
             ts.ward_norm,
             ct.display_site,
             ct.ward_hint,
             ts.reference_number,
             vb.route_type,
             vb.summary_stage,
             vb.tools_stage,
             vb.processing_status_display,
             vb.validation_status::text,
             vb.timesheet_id::text,
             vb.contract_week_id::text,
             COALESCE(ts.contract_id, cw.contract_id, ct.id)::text,
             vb.issue_codes::text
           ) ILIKE v_q_like ESCAPE E'\\'
      )
  ),
  classified AS (
    SELECT
      rr.*,
      UPPER(COALESCE(rr.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(rr.sheet_scope, '')) AS sheet_scope_upper,
      UPPER(COALESCE(rr.submission_mode, rr.cw_submission_mode_snapshot, '')) AS submission_mode_upper,
      UPPER(COALESCE(rr.vb_qr_status, rr.ts_qr_status, '')) AS qr_status_upper,
      (
        COALESCE(rr.is_adjustment, FALSE)
        OR COALESCE(rr.additional_seq, 0) > 0
        OR rr.parent_timesheet_id IS NOT NULL
        OR rr.correction_id IS NOT NULL
        OR rr.correction_kind IS NOT NULL
      ) AS is_adjustment_or_additional,
      (
        COALESCE(rr.vb_qr_status, rr.ts_qr_status) IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_token, rr.vb_qr_token, '')), '') IS NOT NULL
        OR rr.qr_generated_at IS NOT NULL
        OR rr.vb_qr_generated_at IS NOT NULL
        OR rr.qr_scanned_at IS NOT NULL
        OR rr.vb_qr_scanned_at IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_last_sent_hash, '')), '') IS NOT NULL
        OR COALESCE(rr.vb_qr_status, rr.ts_qr_status) IN ('PENDING', 'USED', 'EXPIRED', 'CANCELLED')
      ) AS is_qr_route,
      CASE
        WHEN UPPER(COALESCE(rr.sheet_scope, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(rr.sheet_scope, ''))
        WHEN UPPER(COALESCE(rr.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type
    FROM raw_rows AS rr
  ),
  routed AS (
    SELECT
      cl.*,
      CASE
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family,
      CASE
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
        ) THEN 'NHSP'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily,
      CASE
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN NULL::text
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family,
      (
        cl.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(cl.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required
    FROM classified AS cl
  ),
  decisions AS (
    SELECT
      rt.*,
      CASE
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' AND rt.route_subfamily = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification,
      CASE
        WHEN rt.route_family = 'MANUAL_NON_QR' THEN 1
        WHEN rt.route_family = 'QR' THEN 2
        WHEN rt.route_family = 'ELECTRONIC' THEN 3
        ELSE 99
      END AS timesheet_type_sort_key,
      (
        rt.timesheet_id IS NULL
        AND rt.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.contract_week_status, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed,
      (
        rt.timesheet_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.processing_status, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed,
      (
        COALESCE(rt.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(rt.paid_at_utc, NULL) IS NOT NULL
        OR COALESCE(rt.has_segment_invoice_lock, FALSE) = TRUE
        OR COALESCE(rt.invoice_segments_locked, 0) > 0
        OR COALESCE(rt.invoice_is_paid, FALSE) = TRUE
      ) AS locked,
      (
        rt.authorised_at_server IS NOT NULL
        OR rt.authorised_at_utc IS NOT NULL
      ) AS is_authorised,
      (
        UPPER(COALESCE(rt.processing_status, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(rt.client_requires_hr, FALSE) = TRUE
          AND COALESCE(rt.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(rt.processing_status, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation,
      (
        UPPER(COALESCE(rt.validation_status, '')) IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_satisfied,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'PENDING'
        AND (
          (
            NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NOT NULL
            AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NOT NULL
          )
          OR NULLIF(BTRIM(COALESCE(rt.qr_last_sent_hash, '')), '') IS NOT NULL
        )
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS qr_pending_awaiting_signature,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'USED'
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NOT NULL
      ) AS qr_signed_returned,
      (
        UPPER(COALESCE(rt.submission_mode, rt.cw_submission_mode_snapshot, '')) = 'MANUAL'
        AND NULLIF(BTRIM(COALESCE(rt.qr_status_upper, '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NULL
        AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NULL
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS is_manual_only,
      (
        COALESCE(rt.contract_default_submission_mode, '') = 'ELECTRONIC'
        OR UPPER(COALESCE(rt.policy_snapshot_json->>'default_submission_mode', '')) = 'ELECTRONIC'
      ) AS supports_electronic_submission,
      CASE
        WHEN COALESCE(rt.is_adjustment_or_additional, FALSE) = TRUE THEN
          CASE
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) LIKE 'IMPORT\_%' ESCAPE '\' OR rt.correction_kind IS NOT NULL OR rt.correction_id IS NOT NULL THEN 'NHSP Adjustment'
            ELSE 'Manual Adjustment'
          END
        WHEN rt.route_type_upper = 'DAILY_ELECTRONIC' THEN 'Daily Electronic'
        WHEN rt.route_type_upper = 'DAILY_MANUAL' THEN 'Daily Manual'
        WHEN rt.route_type_upper = 'WEEKLY_ELECTRONIC' THEN 'Weekly Electronic'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL' THEN 'Weekly Manual'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP' THEN 'Weekly NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'Weekly NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'Weekly HealthRoster'
        ELSE 'Unknown'
      END AS route_display,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(rt.client_is_nhsp, FALSE) = TRUE OR COALESCE(rt.contract_is_nhsp, FALSE) = TRUE THEN 'NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER'
        WHEN rt.period_type = 'WEEKLY' THEN 'NONE'
        ELSE NULL::text
      END AS contract_weekly_mode,
      CASE
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(rt.client_no_timesheet_required, FALSE) = TRUE THEN 'CREATE'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'VERIFY'
        ELSE ''
      END AS contract_hr_weekly_behaviour
    FROM routed AS rt
  ),
  final_rows AS (
    SELECT
      dc.*,
      (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) AS is_unprocessed,
      CASE WHEN (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) THEN 'UNPROCESSED' ELSE 'PROCESSED' END AS bulk_process_bucket,
      (dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE) AS hr_validation_awaiting,
      (
        dc.qr_pending_awaiting_signature = TRUE
        OR UPPER(COALESCE(dc.processing_status, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(dc.issue_codes)
      ) AS qr_unsigned_blocked,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.requires_authorisation = TRUE
        AND dc.is_authorised = FALSE
        AND (
          dc.qr_pending_awaiting_signature = FALSE
          AND UPPER(COALESCE(dc.processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE'
          AND NOT ('Awaiting signed QR timesheet' = ANY(dc.issue_codes))
        )
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_authorise,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = TRUE
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_unauthorise,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_unprocess,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_process,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_save,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = TRUE
      ) AS evidence_document_locked,
      CASE
        WHEN NOT (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR')) THEN 'INVALID_TIMESHEET_CONTEXT'
        WHEN dc.locked_by_invoice_id IS NOT NULL THEN 'INVOICE_LOCKED'
        WHEN dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0 THEN 'INVOICE_SEGMENT_LOCKED'
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_AUTHORITATIVE_ROUTE'
        ELSE NULL::text
      END AS evidence_lock_reason,
      (
        dc.locked = TRUE
        OR dc.is_authorised = TRUE
        OR dc.route_family <> 'MANUAL_NON_QR'
      ) AS review_only,
      (
        dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND (
          (dc.period_type = 'WEEKLY' AND dc.contract_week_id IS NOT NULL)
          OR (dc.period_type = 'DAILY' AND dc.timesheet_id IS NOT NULL)
        )
        AND (
          dc.route_family = 'IMPORT_AUTHORITATIVE'
          OR (dc.route_family = 'MANUAL_NON_QR' AND dc.submission_mode_upper = 'MANUAL' AND dc.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND (
          (dc.period_type = 'WEEKLY' AND dc.submission_mode_upper = 'ELECTRONIC' AND (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL))
          OR (dc.period_type = 'DAILY' AND dc.submission_mode_upper = 'ELECTRONIC' AND dc.timesheet_id IS NOT NULL)
        )
      ) AS can_switch_to_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.period_type = 'WEEKLY'
        AND dc.timesheet_id IS NULL
        AND dc.contract_week_id IS NOT NULL
        AND dc.submission_mode_upper = 'MANUAL'
      ) AS can_switch_planned_week_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.has_historical_electronic = TRUE
      ) AS can_revert_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
      ) AS can_allow_qr_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
        AND dc.supports_electronic_submission = TRUE
      ) AS can_allow_electronic_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.is_qr_route = TRUE
        AND NULLIF(BTRIM(COALESCE(dc.qr_status_upper, '')), '') IS NOT NULL
      ) AS can_convert_qr_to_manual_only,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_pending = TRUE
      ) AS can_restore_qr_pending,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_signed = TRUE
      ) AS can_restore_qr_signed,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_SOURCE_ONLY'
        WHEN dc.route_family = 'ELECTRONIC' AND dc.generated_pdf_at_utc IS NULL AND (NULLIF(BTRIM(COALESCE(dc.r2_nurse_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(dc.r2_auth_key, '')), '') IS NOT NULL) THEN 'SIGNATURES_ONLY'
        WHEN dc.timesheet_id IS NOT NULL AND (dc.manual_pdf_r2_key IS NOT NULL OR dc.generated_pdf_at_utc IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL AND (dc.uploaded_pdf_r2_key IS NOT NULL OR dc.primary_queue_r2_key IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        ELSE 'NONE'
      END AS primary_left_pane_mode,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR' THEN COALESCE(NULLIF(BTRIM(dc.manual_pdf_r2_key), ''), CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END)
        WHEN dc.route_family = 'ELECTRONIC' THEN COALESCE(CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END, NULLIF(BTRIM(dc.manual_pdf_r2_key), ''))
        WHEN dc.route_family = 'MANUAL_NON_QR' AND dc.timesheet_id IS NOT NULL THEN NULLIF(BTRIM(dc.manual_pdf_r2_key), '')
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL THEN COALESCE(NULLIF(BTRIM(dc.uploaded_pdf_r2_key), ''), NULLIF(BTRIM(dc.primary_queue_r2_key), ''))
        ELSE NULL::text
      END AS primary_artifact_storage_key,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN dc.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_text,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN 'timesheet:' || dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN 'contract_week:' || dc.contract_week_id::text
        ELSE ''
      END AS row_key,
      MD5(CONCAT_WS('|',
        COALESCE(dc.timesheet_id::text, ''),
        COALESCE(dc.contract_week_id::text, ''),
        COALESCE(dc.timesheet_version::text, ''),
        COALESCE(dc.timesheet_updated_at::text, ''),
        COALESCE(dc.contract_week_updated_at::text, ''),
        COALESCE(dc.tsfin_updated_at::text, ''),
        COALESCE(dc.processing_status, ''),
        COALESCE(dc.summary_stage, ''),
        COALESCE(dc.tools_stage, ''),
        COALESCE(dc.authorised_at_server::text, ''),
        COALESCE(dc.authorised_at_utc::text, ''),
        COALESCE(dc.manual_pdf_r2_key, ''),
        COALESCE(dc.uploaded_pdf_r2_key, ''),
        COALESCE(dc.generated_pdf_at_utc::text, ''),
        COALESCE(dc.primary_queue_r2_key, '')
      )) AS row_signature
    FROM decisions AS dc
  ),
  artifact_rows AS (
    SELECT
      fr.*,
      CASE
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        ELSE NULL::text
      END AS primary_artifact_id,
      CASE
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET'
        WHEN fr.primary_evidence_kind IS NOT NULL THEN UPPER(fr.primary_evidence_kind)
        WHEN fr.primary_queue_id IS NOT NULL THEN 'TIMESHEET'
        ELSE NULL::text
      END AS primary_artifact_kind,
      CASE
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'Timesheet PDF'
        WHEN fr.primary_evidence_display_name IS NOT NULL THEN fr.primary_evidence_display_name
        WHEN fr.primary_queue_original_filename IS NOT NULL THEN fr.primary_queue_original_filename
        ELSE NULL::text
      END AS primary_artifact_display_name,
      CASE
        WHEN fr.primary_left_pane_mode = 'IMPORT_SOURCE_ONLY' THEN 'IMPORT_TABLE'
        WHEN fr.primary_left_pane_mode = 'SIGNATURES_ONLY' THEN 'SIGNATURES'
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'PDF'
        WHEN fr.primary_evidence_storage_key IS NOT NULL THEN 'FILE'
        ELSE NULL::text
      END AS primary_artifact_preview_mode,
      (
        COALESCE(fr.ev_timesheet, FALSE)
        OR COALESCE(fr.ev_mileage, FALSE)
        OR COALESCE(fr.ev_travel, FALSE)
        OR COALESCE(fr.ev_accommodation, FALSE)
        OR COALESCE(fr.ev_other, FALSE)
        OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(fr.primary_queue_r2_key, '')), '') IS NOT NULL
      ) AS has_any_evidence,
      JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', (COALESCE(fr.ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.primary_queue_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(fr.ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.primary_queue_r2_key, '')), '') IS NOT NULL)),
        JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(fr.ev_mileage, FALSE), 'has_evidence', COALESCE(fr.ev_mileage, FALSE)),
        JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(fr.ev_travel, FALSE), 'has_evidence', COALESCE(fr.ev_travel, FALSE)),
        JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(fr.ev_accommodation, FALSE), 'has_evidence', COALESCE(fr.ev_accommodation, FALSE)),
        JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(fr.ev_other, FALSE), 'has_evidence', COALESCE(fr.ev_other, FALSE))
      ) AS evidence_badges
    FROM final_rows AS fr
  )
  SELECT
    (
      JSONB_BUILD_OBJECT(
        'id', COALESCE(ar.timesheet_id::text, ar.contract_week_id::text),
        'row_key', ar.row_key,
        'stable_row_id', ar.stable_row_id_text,
        'timesheet_id', ar.timesheet_id,
        'current_timesheet_id', ar.timesheet_id,
        'requested_timesheet_id', ar.timesheet_id,
        'expected_timesheet_id', ar.timesheet_id,
        'contract_week_id', ar.contract_week_id,
        'contract_id', ar.contract_id,
        'booking_id', ar.booking_id,
        'timesheet_version', ar.timesheet_version,
        'row_signature', ar.row_signature,
        'updated_at', COALESCE(ar.timesheet_updated_at, ar.contract_week_updated_at, ar.tsfin_updated_at),
        'is_current', COALESCE(ar.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'candidate_id', ar.candidate_id,
        'candidate_name', ar.candidate_name,
        'candidate_display_name', ar.candidate_display_name,
        'candidate_first_name', ar.candidate_first_name,
        'candidate_surname', ar.candidate_surname,
        'occupant_key_norm', ar.occupant_key_norm,
        'client_id', ar.client_id,
        'client_name', ar.client_name,
        'client_display_name', ar.client_display_name,
        'hospital_name', ar.hospital_norm,
        'hospital_norm', ar.hospital_norm,
        'site_name', ar.contract_display_site,
        'ward_name', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'ward_norm', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'booking_ref', ar.booking_id,
        'external_ref', ar.reference_number
      )
      || JSONB_BUILD_OBJECT(
        'week_ending_date', ar.week_ending_date,
        'contract_week_ending_date', ar.contract_week_ending_date,
        'work_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE ar.week_ending_date END,
        'shift_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'period_type', ar.period_type,
        'timesheet_type_sort_key', ar.timesheet_type_sort_key,
        'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
        'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
        'processing_status', ar.processing_status,
        'processing_status_display', CASE WHEN ar.is_unprocessed THEN 'Unprocessed' ELSE ar.processing_status_display END,
        'processed_at_utc', ar.processed_at_utc,
        'processed_by_user_id', ar.processed_by_user_id,
        'processed_by_display', ar.processed_by_display,
        'authorised_at_server', ar.authorised_at_server,
        'authorised_at_utc', ar.authorised_at_utc,
        'authorised_by_user_id', ar.authorised_by_user_id,
        'authorised_by_display', ar.authorised_by_display,
        'locked_by_invoice_id', ar.locked_by_invoice_id,
        'paid_at_utc', ar.paid_at_utc,
        'invoice_segments_total', ar.invoice_segments_total,
        'invoice_segments_locked', ar.invoice_segments_locked,
        'invoice_segments_unlocked', ar.invoice_segments_unlocked,
        'invoice_segment_stage', ar.invoice_segment_stage,
        'invoice_is_paid', ar.invoice_is_paid,
        'ready_to_pay', ar.ready_to_pay,
        'pay_icon_code', ar.pay_icon_code,
        'pay_status_code', ar.pay_status_code,
        'pay_paid_at_utc', ar.pay_paid_at_utc,
        'pay_on_hold', ar.pay_on_hold
      )
      || JSONB_BUILD_OBJECT(
        'total_hours', ar.total_hours,
        'total_pay_ex_vat', ar.total_pay_ex_vat,
        'total_charge_ex_vat', ar.total_charge_ex_vat,
        'margin_ex_vat', ar.margin_ex_vat,
        'net_delta_ex_vat', ar.net_delta_ex_vat,
        'route_type', ar.route_type,
        'route_display', ar.route_display,
        'submission_mode', ar.submission_mode,
        'submission_mode_snapshot', ar.cw_submission_mode_snapshot,
        'sheet_scope', ar.sheet_scope,
        'timesheet_scope', ar.sheet_scope,
        'route_family', ar.route_family,
        'route_subfamily', ar.route_subfamily,
        'underlying_channel_family', ar.underlying_channel_family,
        'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', ar.compare_block_required,
        'bulk_process_bucket', ar.bulk_process_bucket,
        'bulk_authorise_classification', ar.bulk_authorise_classification,
        'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
        'has_timesheet', ar.timesheet_id IS NOT NULL,
        'is_contract_week_only', ar.timesheet_id IS NULL AND ar.contract_week_id IS NOT NULL,
        'is_real_timesheet_row', ar.timesheet_id IS NOT NULL,
        'requires_authorisation', ar.requires_authorisation,
        'is_authorised', ar.is_authorised,
        'locked', ar.locked,
        'review_only', ar.review_only,
        'can_save', ar.can_save,
        'can_process', ar.can_process,
        'can_unprocess', ar.can_unprocess,
        'can_edit_timesheet_data', ar.can_edit_timesheet_data,
        'can_manage_evidence', ar.can_manage_evidence,
        'can_bulk_authorise', ar.can_bulk_authorise,
        'can_bulk_unauthorise', ar.can_bulk_unauthorise,
        'can_add_additional_manual', ar.can_add_additional_manual
      )
      || JSONB_BUILD_OBJECT(
        'is_qr', ar.is_qr_route,
        'qr_status', NULLIF(ar.qr_status_upper, ''),
        'qr_generated_at', COALESCE(ar.qr_generated_at, ar.vb_qr_generated_at),
        'qr_scanned_at', COALESCE(ar.qr_scanned_at, ar.vb_qr_scanned_at),
        'qr_signed_at_utc', ar.qr_signed_at_utc,
        'qr_unsigned_blocked', ar.qr_unsigned_blocked,
        'qr_signed_returned', ar.qr_signed_returned,
        'can_allow_qr_again', ar.can_allow_qr_again,
        'can_allow_electronic_again', ar.can_allow_electronic_again,
        'can_switch_to_manual', ar.can_switch_to_manual,
        'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
        'can_revert_to_electronic', ar.can_revert_to_electronic,
        'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
        'can_restore_qr_pending', ar.can_restore_qr_pending,
        'can_restore_qr_signed', ar.can_restore_qr_signed,
        'qr_email_can_send_now', (ar.can_allow_qr_again OR NULLIF(ar.qr_status_upper, '') = 'PENDING') AND NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL AND COALESCE(ar.candidate_opt_in_email, TRUE) = TRUE,
        'qr_email_recipient_available', NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL,
        'client_requires_hr', ar.client_requires_hr,
        'client_autoprocess_hr', ar.client_autoprocess_hr,
        'client_no_timesheet_required', ar.client_no_timesheet_required,
        'client_is_nhsp', ar.client_is_nhsp,
        'hr_validation_required_for_invoice', ar.hr_validation_required_for_invoice,
        'validation_status', ar.validation_status,
        'validation_pre_validated', COALESCE(ar.validation_pre_validated, FALSE),
        'hr_validation_satisfied', ar.hr_validation_satisfied,
        'hr_validation_awaiting', ar.hr_validation_awaiting,
        'issue_codes', ar.issue_codes,
        'has_deviation_marker', FALSE,
        'deviation_marker_reason', NULL::text,
        'nhsp_highlight_red', FALSE,
        'nhsp_highlight_reason', NULL::text,
        'nhsp_deviation_pct', NULL::numeric,
        'nhsp_is_ad_hoc', ar.contract_is_ad_hoc
      )
      || JSONB_BUILD_OBJECT(
        'has_any_evidence', ar.has_any_evidence,
        'evidence_badges', ar.evidence_badges,
        'primary_artifact_id', ar.primary_artifact_id,
        'primary_artifact_kind', ar.primary_artifact_kind,
        'primary_artifact_display_name', ar.primary_artifact_display_name,
        'primary_artifact_storage_key', ar.primary_artifact_storage_key,
        'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
        'primary_left_pane_mode', ar.primary_left_pane_mode,
        'manual_pdf_r2_key', ar.manual_pdf_r2_key,
        'uploaded_pdf_r2_key', ar.uploaded_pdf_r2_key,
        'generated_pdf_at_utc', ar.generated_pdf_at_utc,
        'manual_pdf_rotation_degrees', ar.manual_pdf_rotation_degrees,
        'attached_evidence_count', COALESCE(ar.attached_evidence_count, 0),
        'queue_staged_count', COALESCE(ar.queue_staged_count, 0),
        'evidence_document_locked', ar.evidence_document_locked,
        'evidence_lock_reason', ar.evidence_lock_reason,
        'contract_additional_rates_json', ar.contract_additional_rates_json,
        'contract_std_schedule_json', ar.contract_std_schedule_json,
        'contract_mileage_pay_rate', ar.contract_mileage_pay_rate,
        'contract_mileage_charge_rate', ar.contract_mileage_charge_rate,
        'contract_weekly_mode', ar.contract_weekly_mode,
        'contract_hr_weekly_behaviour', ar.contract_hr_weekly_behaviour,
        'contract_role', ar.contract_role,
        'contract_band', ar.contract_band,
        'reference_number', ar.reference_number,
        'has_pay_adjustments', ar.has_pay_adjustments,
        'pay_adjustment_count', ar.pay_adjustment_count
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', JSONB_BUILD_OBJECT(
          'can_restore_qr_pending', ar.can_restore_qr_pending,
          'can_restore_qr_signed', ar.can_restore_qr_signed,
          'can_revert_to_electronic', ar.can_revert_to_electronic,
          'can_allow_qr_again', ar.can_allow_qr_again,
          'can_allow_electronic_again', ar.can_allow_electronic_again,
          'can_switch_to_manual', ar.can_switch_to_manual,
          'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
          'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
          'supports_electronic_submission', ar.supports_electronic_submission,
          'is_manual_only', ar.is_manual_only,
          'is_adjustment', ar.is_adjustment_or_additional,
          'locked_by_invoice', ar.locked_by_invoice_id IS NOT NULL,
          'paid', ar.paid_at_utc IS NOT NULL,
          'is_unprocessed', ar.is_unprocessed,
          'route_family', ar.route_family,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'compare_block_required', ar.compare_block_required,
          'has_primary_artifact', ar.primary_artifact_storage_key IS NOT NULL,
          'primary_left_pane_mode', ar.primary_left_pane_mode,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'electronic_artifact_exists', ar.route_family = 'ELECTRONIC' AND ar.primary_artifact_storage_key IS NOT NULL,
          'healthroster_compare_required', ar.compare_block_required
        ),
        'bulk_authorise', JSONB_BUILD_OBJECT(
          'classification', ar.bulk_authorise_classification,
          'section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'can_authorise', ar.can_bulk_authorise,
          'can_unauthorise', ar.can_bulk_unauthorise,
          'requires_authorisation', ar.requires_authorisation,
          'is_authorised', ar.is_authorised,
          'hr_validation_awaiting', ar.hr_validation_awaiting,
          'qr_unsigned_blocked', ar.qr_unsigned_blocked,
          'qr_signed_returned', ar.qr_signed_returned
        ),
        'artifact_hints', JSONB_BUILD_OBJECT(
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'has_any_evidence', ar.has_any_evidence,
          'evidence_badges', ar.evidence_badges
        ),
        'row_patch', JSONB_BUILD_OBJECT(
          'row_key', ar.row_key,
          'stable_row_id', ar.stable_row_id_text,
          'timesheet_id', ar.timesheet_id,
          'current_timesheet_id', ar.timesheet_id,
          'expected_timesheet_id', ar.timesheet_id,
          'contract_week_id', ar.contract_week_id,
          'row_signature', ar.row_signature,
          'bulk_process_bucket', ar.bulk_process_bucket,
          'bulk_authorise_classification', ar.bulk_authorise_classification,
          'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'processing_status', ar.processing_status,
          'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
          'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
          'is_authorised', ar.is_authorised,
          'locked', ar.locked,
          'can_process', ar.can_process,
          'can_unprocess', ar.can_unprocess,
          'can_bulk_authorise', ar.can_bulk_authorise,
          'can_bulk_unauthorise', ar.can_bulk_unauthorise,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'evidence_badges', ar.evidence_badges
        )
      )
    ) AS row_json
  FROM artifact_rows AS ar
  ORDER BY
    ar.week_ending_date ASC NULLS LAST,
    ar.client_name ASC NULLS LAST,
    ar.candidate_name ASC NULLS LAST,
    ar.row_key ASC;
END;
$function$;





