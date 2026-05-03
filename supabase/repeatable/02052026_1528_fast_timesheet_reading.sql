



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
  v_bucket text := NULL;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
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

  v_bucket := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'bucket', v_filters->>'bulk_process_bucket', v_filters->>'bulkProcessBucket', '')), ''));
  IF v_bucket NOT IN ('UNPROCESSED', 'PROCESSED') THEN
    v_bucket := NULL;
  END IF;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

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
      AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
      AND COALESCE((manual_rows.row_json->>'can_process')::boolean, FALSE) = TRUE
  ),
  processed_rows AS (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'PROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
      AND NULLIF(BTRIM(COALESCE(manual_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND COALESCE((manual_rows.row_json->>'can_unprocess')::boolean, FALSE) = TRUE
  ),
  paged_unprocessed_rows AS (
    SELECT unprocessed_rows.row_json
    FROM unprocessed_rows
    ORDER BY unprocessed_rows.row_json->>'week_ending_date', unprocessed_rows.row_json->>'client_name', unprocessed_rows.row_json->>'candidate_name', unprocessed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  paged_processed_rows AS (
    SELECT processed_rows.row_json
    FROM processed_rows
    ORDER BY processed_rows.row_json->>'week_ending_date', processed_rows.row_json->>'client_name', processed_rows.row_json->>'candidate_name', processed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
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
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset
    ),
    'counts', JSONB_BUILD_OBJECT(
      'unprocessed', counts.unprocessed_count,
      'processed', counts.processed_count,
      'total', counts.unprocessed_count + counts.processed_count
    ),
    'unprocessed_rows', COALESCE((
      SELECT JSONB_AGG(paged_unprocessed_rows.row_json ORDER BY paged_unprocessed_rows.row_json->>'week_ending_date', paged_unprocessed_rows.row_json->>'client_name', paged_unprocessed_rows.row_json->>'candidate_name', paged_unprocessed_rows.row_json->>'row_key')
      FROM paged_unprocessed_rows
    ), '[]'::jsonb),
    'processed_rows', COALESCE((
      SELECT JSONB_AGG(paged_processed_rows.row_json ORDER BY paged_processed_rows.row_json->>'week_ending_date', paged_processed_rows.row_json->>'client_name', paged_processed_rows.row_json->>'candidate_name', paged_processed_rows.row_json->>'row_key')
      FROM paged_processed_rows
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
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset
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

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

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
  paged_visible_rows AS (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
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
      'validation_awaiting', v_validation_awaiting,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset
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
        'awaiting_validation', counts.awaiting_validation_count,
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE((
      SELECT JSONB_AGG(paged_visible_rows.row_json ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key')
      FROM paged_visible_rows
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
      'validation_awaiting', v_validation_awaiting,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset
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
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
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

  RETURN QUERY
  WITH base_scope AS (
    SELECT vb0.*
    FROM public.v_timesheets_summary_base AS vb0
    WHERE (v_candidate_id IS NULL OR vb0.candidate_id = v_candidate_id)
      AND (v_client_id IS NULL OR vb0.client_id = v_client_id)
      AND (v_timesheet_ids IS NULL OR vb0.timesheet_id = ANY(v_timesheet_ids))
      AND (v_contract_week_ids IS NULL OR vb0.contract_week_id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR CASE
             WHEN vb0.timesheet_id IS NOT NULL THEN 'timesheet:' || vb0.timesheet_id::text
             WHEN vb0.contract_week_id IS NOT NULL THEN 'contract_week:' || vb0.contract_week_id::text
             ELSE ''
           END = ANY(v_row_keys)
      )
      AND (
        v_week_ending_date IS NULL
        OR COALESCE(vb0.contract_week_ending_date, vb0.week_ending_date) = v_week_ending_date
      )
      AND (
        v_date_from IS NULL
        OR COALESCE(vb0.contract_week_ending_date, vb0.week_ending_date) >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR COALESCE(vb0.contract_week_ending_date, vb0.week_ending_date) <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR UPPER(COALESCE(vb0.sheet_scope::text, CASE WHEN UPPER(COALESCE(vb0.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY' ELSE 'WEEKLY' END)) = v_period_filter
      )
  ),
  base_ids AS (
    SELECT DISTINCT
      base_scope.timesheet_id,
      base_scope.contract_week_id
    FROM base_scope
  ),
  tf_ranked AS (
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
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tf
        WHERE bs_tf.timesheet_id = tf0.timesheet_id
      )
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
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tv
        WHERE bs_tv.timesheet_id = tv0.timesheet_id
      )
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
        ORDER BY
          CASE
            WHEN UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET' THEN 0
            ELSE 1
          END ASC,
          te0.created_at ASC NULLS LAST,
          te0.id ASC
      ) AS rn
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te
        WHERE bs_te.timesheet_id = te0.timesheet_id
      )
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
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence,
      MAX(te2.created_at) AS evidence_updated_at
    FROM public.timesheet_evidence AS te2
    WHERE te2.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te2
        WHERE bs_te2.timesheet_id = te2.timesheet_id
      )
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
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_mq
        WHERE bs_mq.contract_week_id::text = NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
      )
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
      COUNT(mq2.id)::integer AS queue_staged_count,
      MAX(mq2.uploaded_at_utc) AS queue_updated_at
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
      tv.updated_at AS validation_updated_at,
      tv.pre_validated AS validation_pre_validated,
      tv.reason_code AS validation_reason_code,
      tv.validated_at_utc AS validation_validated_at_utc,
      te.evidence_updated_at AS evidence_updated_at,
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
      mqa.queue_updated_at AS queue_updated_at,
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
    FROM base_scope AS vb
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
        WHEN dc.route_family = 'QR'
          AND COALESCE(
            NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'ELECTRONIC'
          AND COALESCE(
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
            NULLIF(BTRIM(dc.manual_pdf_r2_key), '')
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'MANUAL_NON_QR'
          AND dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.manual_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_evidence_storage_key), '') IS NOT NULL THEN 'EVIDENCE'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.uploaded_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_queue_r2_key), '') IS NOT NULL THEN 'QUEUE'
        ELSE NULL::text
      END AS primary_artifact_source,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR' THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'ELECTRONIC' THEN COALESCE(
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'MANUAL_NON_QR' AND dc.timesheet_id IS NOT NULL THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.timesheet_id IS NOT NULL THEN NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
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
        COALESCE(dc.validation_updated_at::text, ''),
        COALESCE(dc.evidence_updated_at::text, ''),
        COALESCE(dc.queue_updated_at::text, ''),
        COALESCE(dc.processing_status, ''),
        COALESCE(dc.summary_stage, ''),
        COALESCE(dc.tools_stage, ''),
        COALESCE(dc.authorised_at_server::text, ''),
        COALESCE(dc.authorised_at_utc::text, ''),
        COALESCE(dc.is_authorised::text, ''),
        COALESCE(dc.locked::text, ''),
        COALESCE(dc.locked_by_invoice_id::text, ''),
        COALESCE(dc.has_segment_invoice_lock::text, ''),
        COALESCE(dc.invoice_segments_locked::text, ''),
        COALESCE(dc.invoice_is_paid::text, ''),
        COALESCE(dc.paid_at_utc::text, ''),
        COALESCE(dc.route_family, ''),
        COALESCE(dc.route_subfamily, ''),
        COALESCE(dc.submission_mode, ''),
        COALESCE(dc.cw_submission_mode_snapshot, ''),
        COALESCE(dc.qr_status_upper, ''),
        COALESCE(dc.qr_generated_at::text, ''),
        COALESCE(dc.qr_scanned_at::text, ''),
        COALESCE(dc.qr_signed_at_utc::text, ''),
        COALESCE(dc.validation_status, ''),
        COALESCE(dc.validation_pre_validated::text, ''),
        COALESCE(dc.validation_reason_code, ''),
        COALESCE(dc.validation_validated_at_utc::text, ''),
        COALESCE((dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE)::text, ''),
        COALESCE(dc.issue_codes::text, ''),
        COALESCE(dc.attached_evidence_count::text, ''),
        COALESCE(dc.ev_timesheet::text, ''),
        COALESCE(dc.ev_mileage::text, ''),
        COALESCE(dc.ev_travel::text, ''),
        COALESCE(dc.ev_accommodation::text, ''),
        COALESCE(dc.ev_other::text, ''),
        COALESCE(dc.primary_evidence_id::text, ''),
        COALESCE(dc.primary_evidence_kind, ''),
        COALESCE(dc.queue_staged_count::text, ''),
        COALESCE(dc.primary_evidence_storage_key, ''),
        COALESCE(dc.manual_pdf_r2_key, ''),
        COALESCE(dc.uploaded_pdf_r2_key, ''),
        COALESCE(dc.generated_pdf_at_utc::text, ''),
        COALESCE(dc.primary_queue_r2_key, ''),
        COALESCE(dc.client_requires_hr::text, ''),
        COALESCE(dc.client_no_timesheet_required::text, ''),
        COALESCE(dc.client_autoprocess_hr::text, ''),
        COALESCE(dc.client_is_nhsp::text, ''),
        COALESCE(dc.total_hours::text, ''),
        COALESCE(dc.total_pay_ex_vat::text, ''),
        COALESCE(dc.total_charge_ex_vat::text, ''),
        COALESCE(dc.margin_ex_vat::text, '')
      )) AS row_signature
    FROM decisions AS dc
  ),
  artifact_rows AS (
    SELECT
      fr.*,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_artifact_source = 'EVIDENCE' AND fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_artifact_source = 'QUEUE' AND fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        ELSE NULL::text
      END AS primary_artifact_id,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'TIMESHEET'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_evidence_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_source = 'QUEUE' THEN 'TIMESHEET'
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET'
        WHEN fr.primary_evidence_kind IS NOT NULL THEN UPPER(fr.primary_evidence_kind)
        WHEN fr.primary_queue_id IS NOT NULL THEN 'TIMESHEET'
        ELSE NULL::text
      END AS primary_artifact_kind,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'Timesheet PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN COALESCE(NULLIF(BTRIM(fr.primary_evidence_display_name), ''), 'Evidence')
        WHEN fr.primary_artifact_source = 'QUEUE' THEN COALESCE(NULLIF(BTRIM(fr.primary_queue_original_filename), ''), 'Staged timesheet evidence')
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'Timesheet PDF'
        WHEN fr.primary_evidence_display_name IS NOT NULL THEN fr.primary_evidence_display_name
        WHEN fr.primary_queue_original_filename IS NOT NULL THEN fr.primary_queue_original_filename
        ELSE NULL::text
      END AS primary_artifact_display_name,
      CASE
        WHEN fr.primary_left_pane_mode = 'IMPORT_SOURCE_ONLY' THEN 'IMPORT_TABLE'
        WHEN fr.primary_left_pane_mode = 'SIGNATURES_ONLY' THEN 'SIGNATURES'
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN 'FILE'
        WHEN fr.primary_artifact_source = 'QUEUE' THEN 'PDF'
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
        'has_deviation_marker', (
          COALESCE(ar.has_rate_issue, FALSE)
          OR COALESCE(ar.has_pay_channel_issue, FALSE)
          OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
          OR COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0
          OR (
            NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
            AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
          )
        ),
        'deviation_marker_reason', CASE
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          WHEN COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0 THEN 'HR_CROSSCHECK_ISSUE'
          ELSE NULL::text
        END,
        'nhsp_highlight_red', (
          (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE)
          AND (
            COALESCE(ar.has_rate_issue, FALSE)
            OR COALESCE(ar.has_pay_channel_issue, FALSE)
            OR COALESCE(ar.nhsp_shift_deferred_count, 0) > 0
            OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
              AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
            )
          )
        ),
        'nhsp_highlight_reason', CASE
          WHEN NOT (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE) THEN NULL::text
          WHEN COALESCE(ar.nhsp_shift_deferred_count, 0) > 0 THEN 'DEFERRED_NHSP_SHIFTS'
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          ELSE NULL::text
        END,
        'nhsp_deviation_pct', CASE
          WHEN COALESCE(ar.nhsp_shift_count, 0) > 0 THEN ROUND((COALESCE(ar.nhsp_shift_deferred_count, 0)::numeric / NULLIF(ar.nhsp_shift_count::numeric, 0)) * 100, 2)
          ELSE NULL::numeric
        END,
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
        'primary_artifact_source', ar.primary_artifact_source,
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
          'previous_row_key', NULL::text,
          'row_key', ar.row_key,
          'new_row_key', ar.row_key,
          'stable_row_id', ar.stable_row_id_text,
          'timesheet_id', ar.timesheet_id,
          'current_timesheet_id', ar.timesheet_id,
          'expected_timesheet_id', ar.timesheet_id,
          'contract_week_id', ar.contract_week_id,
          'row_signature', ar.row_signature,
          'previous_row_signature', NULL::text,
          'bulk_process_bucket', ar.bulk_process_bucket,
          'previous_bulk_process_bucket', NULL::text,
          'bulk_authorise_classification', ar.bulk_authorise_classification,
          'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'previous_bulk_authorise_section', NULL::text,
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
          'previous_primary_artifact_storage_key', NULL::text,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'evidence_badges', ar.evidence_badges,
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(
            'row_keys', JSONB_BUILD_ARRAY(ar.row_key),
            'timesheet_ids', CASE WHEN ar.timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.timesheet_id) END,
            'contract_week_ids', CASE WHEN ar.contract_week_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.contract_week_id) END,
            'storage_keys', CASE WHEN ar.primary_artifact_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.primary_artifact_storage_key) END,
            'row_signature', ar.row_signature,
            'invalidate_context', TRUE,
            'invalidate_preview', FALSE
          )
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






CREATE OR REPLACE FUNCTION public.bulk_process_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_process_row_context',
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_process_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_include_evidence := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  WITH decision_row AS (
    SELECT decision_result.row_json
    FROM public.bulk_timesheet_row_decision_v1(v_decision_filters || JSONB_BUILD_OBJECT('dataset_mode', 'process')) AS decision_result(row_json)
    ORDER BY decision_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
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
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'uploaded_at_utc', row_ids.row_json->>'updated_at',
        'rotation_degrees', COALESCE(NULLIF(row_ids.row_json->>'manual_pdf_rotation_degrees', '')::integer, 0),
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'PDF'),
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
    FROM row_ids
    WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'preview_mode', 'FILE',
        'source_label', NULL::text,
        'source_badge', NULL::text
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'mime_type', mq0.mime_type,
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', mq0.uploaded_by_user_id,
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'is_staged_context', TRUE,
        'preview_mode', 'PDF',
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', contract_week_row.uploaded_pdf_r2_key,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_process_row_context',
        'context_type', 'bulk_process',
        'slim_context', TRUE,
        'evidence_loaded', v_include_evidence,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', NULLIF(row_ids.row_json->>'timesheet_version', '')::integer,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
        'has_timesheet', COALESCE((row_ids.row_json->>'has_timesheet')::boolean, FALSE),
        'locked', COALESCE((row_ids.row_json->>'locked')::boolean, FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
        'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN NULLIF(row_ids.row_json->>'timesheet_type_sort_key', '') IS NULL THEN NULL::integer ELSE (row_ids.row_json->>'timesheet_type_sort_key')::integer END,
        'can_save', COALESCE((row_ids.row_json->>'can_save')::boolean, FALSE),
        'can_process', COALESCE((row_ids.row_json->>'can_process')::boolean, FALSE),
        'can_unprocess', COALESCE((row_ids.row_json->>'can_unprocess')::boolean, FALSE),
        'can_edit_timesheet_data', COALESCE((row_ids.row_json->>'can_edit_timesheet_data')::boolean, FALSE),
        'can_manage_evidence', COALESCE((row_ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_add_additional_manual', COALESCE((row_ids.row_json->>'can_add_additional_manual')::boolean, FALSE),
        'review_only', COALESCE((row_ids.row_json->>'review_only')::boolean, FALSE),
        'hr_validation_required_for_invoice', COALESCE((row_ids.row_json->>'hr_validation_required_for_invoice')::boolean, FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
        'has_deviation_marker', COALESCE((row_ids.row_json->>'has_deviation_marker')::boolean, FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'row', row_ids.row_json,
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', NULLIF(row_ids.row_json->>'timesheet_version', '')::integer,
            'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
              'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
              'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', '[]'::jsonb,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
                'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
                'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
                'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
              'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
              'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
              'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
          'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
          'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', '[]'::jsonb
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
          'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
          'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
          'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
            'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
            'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
            'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE((row_ids.row_json->>'has_any_evidence')::boolean, FALSE),
          'evidence_badges', COALESCE(row_ids.row_json->'evidence_badges', '[]'::jsonb),
          'attached_evidence_count', COALESCE(NULLIF(row_ids.row_json->>'attached_evidence_count', '')::integer, 0),
          'queue_staged_count', COALESCE(NULLIF(row_ids.row_json->>'queue_staged_count', '')::integer, 0),
          'evidence_document_locked', COALESCE((row_ids.row_json->>'evidence_document_locked')::boolean, FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
  ),
  final_payload AS (
    SELECT
      base_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'details', (base_payload.payload_json - 'details'),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', base_payload.payload_json->>'route_family',
          'route_subfamily', base_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', base_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((base_payload.payload_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((base_payload.payload_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', COALESCE(base_payload.payload_json->'primary_artifact', NULL::jsonb),
          'source_items', '[]'::jsonb,
          'primary_left_pane_mode', base_payload.payload_json->>'primary_left_pane_mode'
        ),
        'data_row', COALESCE(base_payload.payload_json->'row', JSONB_BUILD_OBJECT())
      ) AS payload_json
    FROM base_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_process_row_context',
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk process row context was found for the supplied identity',
    'filters', v_filters
  ));
END;
$function$;





CREATE OR REPLACE FUNCTION public.bulk_authorise_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_include_compare boolean := FALSE;
  v_include_import_source_rows boolean := FALSE;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_authorise_row_context',
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_authorise_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_include_evidence := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_compare := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_compare', v_filters->>'includeCompare', v_filters->>'load_compare', v_filters->>'loadCompare', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_import_source_rows := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_import_source_rows', v_filters->>'includeImportSourceRows', v_filters->>'load_import_source_rows', v_filters->>'loadImportSourceRows', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  WITH decision_row AS (
    SELECT decision_result.row_json
    FROM public.bulk_timesheet_row_decision_v1(v_decision_filters || JSONB_BUILD_OBJECT('dataset_mode', 'authorise')) AS decision_result(row_json)
    ORDER BY decision_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
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
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'uploaded_at_utc', row_ids.row_json->>'updated_at',
        'rotation_degrees', COALESCE(NULLIF(row_ids.row_json->>'manual_pdf_rotation_degrees', '')::integer, 0),
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'PDF'),
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
    FROM row_ids
    WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
      AND NOT (
        NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') LIKE 'evidence:%'
        OR (
          row_ids.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te_primary_artifact
            WHERE te_primary_artifact.timesheet_id = row_ids.timesheet_id
              AND te_primary_artifact.storage_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          )
        )
      )
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'mime_type', NULL::text,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'rotation', 0,
        'rotation_degrees', 0,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'preview_mode', 'FILE',
        'source_label', 'Attached',
        'source_badge', 'Attached'
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'mime_type', mq0.mime_type,
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', mq0.uploaded_by_user_id,
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'is_staged_context', TRUE,
        'preview_mode', 'PDF',
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', contract_week_row.uploaded_pdf_r2_key,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  shift_rows AS (
    SELECT
      ns0.id,
      ns0.external_row_key,
      ns0.latest_import_id,
      ns0.candidate_id,
      ns0.client_id,
      ns0.contract_id,
      ns0.timesheet_id,
      ns0.work_date,
      ns0.ward,
      ns0.start_utc,
      ns0.end_utc,
      ns0.break_mins,
      ns0.pay_minutes,
      ns0.pay_amount_snapshot,
      ns0.charge_amount_snapshot,
      ns0.invoice_status,
      ns0.defer_until_run_after,
      ns0.invoice_id,
      ns0.created_at,
      ns0.updated_at,
      ns0.source_system,
      ns0.hr_request_id,
      ns0.held_back_reason,
      ns0.staff_name,
      ns0.staff_norm,
      ns0.ward_norm,
      ns0.assignment_code,
      ns0.ref_num,
      ns0.week_ending_date,
      ns0.cancelled_at_utc,
      ns0.cancelled_by_import_id,
      ns0.cancelled_reason
    FROM public.nhsp_shifts AS ns0
    CROSS JOIN row_ids AS ids
    WHERE (v_include_compare = TRUE OR v_include_import_source_rows = TRUE)
      AND (
        (v_include_compare = TRUE AND COALESCE((ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE)
        OR (v_include_import_source_rows = TRUE AND UPPER(COALESCE(ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE')
      )
      AND (
        (ids.timesheet_id IS NOT NULL AND ns0.timesheet_id = ids.timesheet_id)
        OR (
          ids.timesheet_id IS NULL
          AND ids.contract_id IS NOT NULL
          AND ns0.contract_id = ids.contract_id
          AND ns0.week_ending_date = CASE
            WHEN COALESCE(ids.row_json->>'week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'week_ending_date')::date
            WHEN COALESCE(ids.row_json->>'contract_week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'contract_week_ending_date')::date
            ELSE NULL::date
          END
        )
      )
    ORDER BY ns0.work_date ASC NULLS LAST, ns0.start_utc ASC NULLS LAST, ns0.id ASC
  ),
  shifts_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', shift_rows.id,
          'external_row_key', shift_rows.external_row_key,
          'latest_import_id', shift_rows.latest_import_id,
          'candidate_id', shift_rows.candidate_id,
          'client_id', shift_rows.client_id,
          'contract_id', shift_rows.contract_id,
          'timesheet_id', shift_rows.timesheet_id,
          'work_date', shift_rows.work_date,
          'ward', shift_rows.ward,
          'start_utc', shift_rows.start_utc,
          'end_utc', shift_rows.end_utc,
          'break_mins', shift_rows.break_mins,
          'pay_minutes', shift_rows.pay_minutes,
          'pay_amount_snapshot', shift_rows.pay_amount_snapshot,
          'charge_amount_snapshot', shift_rows.charge_amount_snapshot,
          'invoice_status', shift_rows.invoice_status,
          'defer_until_run_after', shift_rows.defer_until_run_after,
          'invoice_id', shift_rows.invoice_id,
          'created_at', shift_rows.created_at,
          'updated_at', shift_rows.updated_at,
          'source_system', shift_rows.source_system,
          'hr_request_id', shift_rows.hr_request_id,
          'held_back_reason', shift_rows.held_back_reason,
          'staff_name', shift_rows.staff_name,
          'staff_norm', shift_rows.staff_norm,
          'ward_norm', shift_rows.ward_norm,
          'assignment_code', shift_rows.assignment_code,
          'ref_num', shift_rows.ref_num,
          'week_ending_date', shift_rows.week_ending_date,
          'cancelled_at_utc', shift_rows.cancelled_at_utc,
          'cancelled_by_import_id', shift_rows.cancelled_by_import_id,
          'cancelled_reason', shift_rows.cancelled_reason
        ) ORDER BY shift_rows.work_date ASC NULLS LAST, shift_rows.start_utc ASC NULLS LAST, shift_rows.id ASC
      ), '[]'::jsonb) AS shifts_json,
      JSONB_BUILD_OBJECT(
        'import_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr1.latest_import_id::text)) FROM shift_rows AS sr1 WHERE sr1.latest_import_id IS NOT NULL), '[]'::jsonb),
        'source_systems', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr2.source_system::text)) FROM shift_rows AS sr2 WHERE sr2.source_system IS NOT NULL), '[]'::jsonb),
        'shift_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr3.id::text)) FROM shift_rows AS sr3 WHERE sr3.id IS NOT NULL), '[]'::jsonb),
        'external_row_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr4.external_row_key)) FROM shift_rows AS sr4 WHERE NULLIF(BTRIM(COALESCE(sr4.external_row_key, '')), '') IS NOT NULL), '[]'::jsonb),
        'hr_request_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr5.hr_request_id)) FROM shift_rows AS sr5 WHERE NULLIF(BTRIM(COALESCE(sr5.hr_request_id, '')), '') IS NOT NULL), '[]'::jsonb),
        'work_dates', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr6.work_date::text)) FROM shift_rows AS sr6 WHERE sr6.work_date IS NOT NULL), '[]'::jsonb),
        'ref_nums', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr7.ref_num)) FROM shift_rows AS sr7 WHERE NULLIF(BTRIM(COALESCE(sr7.ref_num, '')), '') IS NOT NULL), '[]'::jsonb)
      ) AS imported_detail_refs_json
    FROM shift_rows
  ),
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_authorise_row_context',
        'context_type', 'bulk_authorise',
        'slim_context', TRUE,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', NULLIF(row_ids.row_json->>'timesheet_version', '')::integer,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
        'has_timesheet', COALESCE((row_ids.row_json->>'has_timesheet')::boolean, FALSE),
        'locked', COALESCE((row_ids.row_json->>'locked')::boolean, FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
        'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN NULLIF(row_ids.row_json->>'timesheet_type_sort_key', '') IS NULL THEN NULL::integer ELSE (row_ids.row_json->>'timesheet_type_sort_key')::integer END,
        'can_save', COALESCE((row_ids.row_json->>'can_save')::boolean, FALSE),
        'can_process', COALESCE((row_ids.row_json->>'can_process')::boolean, FALSE),
        'can_unprocess', COALESCE((row_ids.row_json->>'can_unprocess')::boolean, FALSE),
        'can_edit_timesheet_data', COALESCE((row_ids.row_json->>'can_edit_timesheet_data')::boolean, FALSE),
        'can_manage_evidence', COALESCE((row_ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_add_additional_manual', COALESCE((row_ids.row_json->>'can_add_additional_manual')::boolean, FALSE),
        'review_only', COALESCE((row_ids.row_json->>'review_only')::boolean, FALSE),
        'hr_validation_required_for_invoice', COALESCE((row_ids.row_json->>'hr_validation_required_for_invoice')::boolean, FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
        'has_deviation_marker', COALESCE((row_ids.row_json->>'has_deviation_marker')::boolean, FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'can_bulk_authorise', COALESCE((row_ids.row_json->>'can_bulk_authorise')::boolean, FALSE),
        'can_bulk_unauthorise', COALESCE((row_ids.row_json->>'can_bulk_unauthorise')::boolean, FALSE),
        'requires_authorisation', COALESCE((row_ids.row_json->>'requires_authorisation')::boolean, FALSE),
        'is_authorised', COALESCE((row_ids.row_json->>'is_authorised')::boolean, FALSE),
        'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
        'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
        'qr_unsigned_blocked', COALESCE((row_ids.row_json->>'qr_unsigned_blocked')::boolean, FALSE),
        'qr_signed_returned', COALESCE((row_ids.row_json->>'qr_signed_returned')::boolean, FALSE),
        'row', row_ids.row_json,
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', NULLIF(row_ids.row_json->>'timesheet_version', '')::integer,
            'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
              'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
              'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', CASE
              WHEN v_include_import_source_rows = TRUE
               AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
              ELSE '[]'::jsonb
            END,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
                'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
                'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
                'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
              'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
              'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
              'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'healthroster_compare', CASE
              WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
                'required', TRUE,
                'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
                'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
              )
              ELSE JSONB_BUILD_OBJECT(
                'required', FALSE,
                'rows', '[]'::jsonb,
                'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
              )
            END,
            'import_source_rows', CASE
              WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
              ELSE NULL::jsonb
            END
          )
          || JSONB_BUILD_OBJECT(
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
      )
      || JSONB_BUILD_OBJECT(
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
          'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
          'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', CASE
          WHEN v_include_import_source_rows = TRUE
           AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END,
        'healthroster_compare', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE JSONB_BUILD_OBJECT(
            'required', FALSE,
            'rows', '[]'::jsonb,
            'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
          )
        END,
        'compare_payload', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE NULL::jsonb
        END,
        'import_source_rows', CASE
          WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
          ELSE NULL::jsonb
        END
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
          'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
          'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
          'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
            'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
            'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
            'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE((row_ids.row_json->>'has_any_evidence')::boolean, FALSE),
          'evidence_badges', COALESCE(row_ids.row_json->'evidence_badges', '[]'::jsonb),
          'attached_evidence_count', COALESCE(NULLIF(row_ids.row_json->>'attached_evidence_count', '')::integer, 0),
          'queue_staged_count', COALESCE(NULLIF(row_ids.row_json->>'queue_staged_count', '')::integer, 0),
          'evidence_document_locked', COALESCE((row_ids.row_json->>'evidence_document_locked')::boolean, FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'compare_payload', CASE
            WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
              'required', TRUE,
              'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
              'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
            )
            ELSE NULL::jsonb
          END,
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
    LEFT JOIN shifts_payload ON TRUE
  ),
  final_payload AS (
    SELECT
      base_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'details', (base_payload.payload_json - 'details'),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', base_payload.payload_json->>'route_family',
          'route_subfamily', base_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', base_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((base_payload.payload_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((base_payload.payload_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', COALESCE(base_payload.payload_json->'primary_artifact', NULL::jsonb),
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE THEN COALESCE(NULLIF(base_payload.payload_json->'import_source_rows', 'null'::jsonb), base_payload.payload_json->'shifts', '[]'::jsonb)
            ELSE '[]'::jsonb
          END,
          'primary_left_pane_mode', base_payload.payload_json->>'primary_left_pane_mode'
        ),
        'compare_payload', CASE
          WHEN v_include_compare = TRUE THEN COALESCE(base_payload.payload_json->'healthroster_compare', JSONB_BUILD_OBJECT('required', FALSE, 'rows', '[]'::jsonb, 'imported_detail_refs', JSONB_BUILD_OBJECT()))
          ELSE NULL::jsonb
        END,
        'data_row', COALESCE(base_payload.payload_json->'row', JSONB_BUILD_OBJECT())
      ) AS payload_json
    FROM base_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_authorise_row_context',
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk authorise row context was found for the supplied identity',
    'filters', v_filters
  ));
END;
$function$;



CREATE OR REPLACE FUNCTION public.timesheet_qr_send_enqueue_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_idempotency_key text DEFAULT NULL,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_timesheet_id uuid := NULL;
  v_requested_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_contract_id uuid := NULL;
  v_week_ending_date date := NULL;
  v_worked_start_iso timestamp with time zone := NULL;
  v_worked_end_iso timestamp with time zone := NULL;
  v_actual_schedule_json jsonb := NULL;
  v_qr_status text := NULL;
  v_qr_token text := NULL;
  v_effective_qr_token text := NULL;
  v_qr_payload_json jsonb := '{}'::jsonb;
  v_payload_qr_token text := NULL;
  v_qr_generated_at timestamp with time zone := NULL;
  v_qr_scanned_at timestamp with time zone := NULL;
  v_qr_signed_hash text := NULL;
  v_qr_signed_at_utc timestamp with time zone := NULL;
  v_updated_at timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_tsfin_processing_status text := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_has_hours_for_send boolean := FALSE;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_candidate_email text := NULL;
  v_candidate_name text := NULL;
  v_candidate_opt_in_email boolean := TRUE;
  v_recipient_available boolean := FALSE;
  v_contract_candidate_id uuid := NULL;
  v_contract_client_id uuid := NULL;
  v_idempotency_key text := NULL;
  v_client_idempotency_key text := NULL;
  v_recipient_namespace text := NULL;
  v_mail_held_until_pdf_rendered boolean := TRUE;
  v_existing_mail_id uuid := NULL;
  v_existing_mail_status text := NULL;
  v_mail_job_id uuid := NULL;
  v_pdf_job_id uuid := NULL;
  v_existing_pdf_job_id uuid := NULL;
  v_job_id uuid := NULL;
  v_send_state text := NULL;
  v_pdf_key text := NULL;
  v_mail_subject text := NULL;
  v_mail_body_text text := NULL;
  v_mail_body_html text := NULL;
  v_mail_reference text := NULL;
  v_mail_scheduled_for_utc timestamp with time zone := NULL;
  v_created_by_user_id uuid := NULL;
  v_post_row jsonb := NULL;
  v_row_key text := NULL;
  v_storage_key text := NULL;
  v_row_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_ID_REQUIRED',
      'message', 'p_timesheet_id is required.',
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT requested_ts.timesheet_id,
         requested_ts.booking_id
    INTO v_requested_timesheet_id,
         v_requested_booking_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_NOT_FOUND',
      'message', 'Timesheet was not found.',
      'requested_timesheet_id', p_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST,
           current_ts.updated_at DESC NULLS LAST,
           current_ts.created_at DESC NULLS LAST,
           current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  IF p_expected_timesheet_id IS NOT NULL AND p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'timesheet_qr_send_enqueue_v1 requires the current timesheet row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', COALESCE(p_expected_timesheet_id, p_timesheet_id),
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT ts_current.sheet_scope::text,
         ts_current.submission_mode::text,
         ts_current.contract_id,
         ts_current.week_ending_date,
         ts_current.worked_start_iso,
         ts_current.worked_end_iso,
         ts_current.actual_schedule_json,
         ts_current.qr_status::text,
         ts_current.qr_token,
         ts_current.qr_payload_json,
         ts_current.qr_generated_at,
         ts_current.qr_scanned_at,
         ts_current.qr_signed_hash,
         ts_current.qr_signed_at_utc,
         ts_current.version,
         ts_current.updated_at
    INTO v_sheet_scope,
         v_submission_mode,
         v_contract_id,
         v_week_ending_date,
         v_worked_start_iso,
         v_worked_end_iso,
         v_actual_schedule_json,
         v_qr_status,
         v_qr_token,
         v_qr_payload_json,
         v_qr_generated_at,
         v_qr_scanned_at,
         v_qr_signed_hash,
         v_qr_signed_at_utc,
         v_current_version,
         v_updated_at
  FROM public.timesheets AS ts_current
  WHERE ts_current.timesheet_id = v_current_timesheet_id
    AND ts_current.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'message', 'Current timesheet was not found.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) NOT IN ('DAILY', 'WEEKLY') THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'UNSUPPORTED_SHEET_SCOPE',
      'message', 'QR send is only supported for DAILY or WEEKLY timesheets.',
      'current_timesheet_id', v_current_timesheet_id,
      'sheet_scope', v_sheet_scope,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NOT_MANUAL_QR_ROUTE',
      'message', 'QR send requires a MANUAL submission-mode timesheet with QR enabled.',
      'current_timesheet_id', v_current_timesheet_id,
      'submission_mode', v_submission_mode,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_qr_status, '')) <> 'PENDING' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_NOT_PENDING',
      'message', 'QR send requires qr_status=PENDING.',
      'current_timesheet_id', v_current_timesheet_id,
      'qr_status', v_qr_status,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_qr_scanned_at IS NOT NULL OR NULLIF(BTRIM(COALESCE(v_qr_signed_hash, '')), '') IS NOT NULL OR v_qr_signed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_ALREADY_SIGNED',
      'message', 'Cannot queue QR send: timesheet already has signed QR markers.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT tf_current.id,
         tf_current.processing_status::text,
         tf_current.locked_by_invoice_id,
         tf_current.paid_at_utc,
         tf_current.invoice_breakdown_json,
         tf_current.candidate_id,
         tf_current.client_id
    INTO v_tsfin_id,
         v_tsfin_processing_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_candidate_id,
         v_client_id
  FROM public.timesheets_financials AS tf_current
  WHERE tf_current.timesheet_id = v_current_timesheet_id
    AND tf_current.is_current = TRUE
  ORDER BY tf_current.computed_at_utc DESC NULLS LAST,
           tf_current.created_at DESC NULLS LAST,
           tf_current.updated_at DESC NULLS LAST,
           tf_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_TSFIN',
      'message', 'No current financial snapshot exists for this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_LOCKED_OR_PAID',
      'message', 'Cannot queue QR send: timesheet is locked, invoice-locked, or paid.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_actual_schedule_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_actual_schedule_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS schedule_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'start', schedule_segment.segment_json->>'start_utc', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'end', schedule_segment.segment_json->>'end_utc', '')), '') IS NOT NULL
    ) OR EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
    ) INTO v_has_hours_for_send;
  ELSE
    v_has_hours_for_send := v_worked_start_iso IS NOT NULL AND v_worked_end_iso IS NOT NULL;
  END IF;

  IF COALESCE(v_has_hours_for_send, FALSE) = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_HOURS_RECORDED',
      'message', 'Cannot queue QR send: no hours are recorded yet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_contract_id IS NOT NULL THEN
    SELECT contract_row.candidate_id,
           contract_row.client_id
      INTO v_contract_candidate_id,
           v_contract_client_id
    FROM public.contracts AS contract_row
    WHERE contract_row.id = v_contract_id
    LIMIT 1;
  END IF;

  v_candidate_id := COALESCE(v_contract_candidate_id, v_candidate_id);
  v_client_id := COALESCE(v_contract_client_id, v_client_id);

  IF v_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_NOT_FOUND',
      'message', 'Cannot queue QR send: candidate could not be resolved.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT NULLIF(BTRIM(candidate_row.email), ''),
         COALESCE(NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')),
         COALESCE(candidate_row.opt_in_email, TRUE)
    INTO v_candidate_email,
         v_candidate_name,
         v_candidate_opt_in_email
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = v_candidate_id
  LIMIT 1;

  v_recipient_available := NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NOT NULL AND COALESCE(v_candidate_opt_in_email, TRUE) = TRUE;

  IF v_recipient_available = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'CANDIDATE_EMAIL_MISSING' ELSE 'CANDIDATE_EMAIL_OPTED_OUT' END,
      'message', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'Cannot queue QR send: candidate email is missing.' ELSE 'Cannot queue QR send: candidate has opted out of email.' END,
      'current_timesheet_id', v_current_timesheet_id,
      'candidate_id', v_candidate_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  v_payload_qr_token := CASE
    WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN NULLIF(BTRIM(COALESCE(v_qr_payload_json->>'tok', '')), '')
    ELSE NULL
  END;
  v_effective_qr_token := COALESCE(NULLIF(BTRIM(COALESCE(v_qr_token, '')), ''), v_payload_qr_token, gen_random_uuid()::text);
  v_qr_payload_json := COALESCE(CASE WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN v_qr_payload_json ELSE '{}'::jsonb END, '{}'::jsonb)
    || jsonb_build_object('v', 1, 'tok', v_effective_qr_token);

  UPDATE public.timesheets AS ts_qr_update
     SET qr_token = v_effective_qr_token,
         qr_payload_json = v_qr_payload_json,
         qr_generated_at = COALESCE(ts_qr_update.qr_generated_at, v_now),
         qr_r2_key = NULL,
         qr_scanned_at = NULL,
         qr_scan_info_json = NULL,
         updated_at = v_now
   WHERE ts_qr_update.timesheet_id = v_current_timesheet_id
     AND ts_qr_update.is_current = TRUE
  RETURNING ts_qr_update.qr_generated_at,
            ts_qr_update.updated_at
       INTO v_qr_generated_at,
            v_updated_at;

  IF UPPER(COALESCE(v_tsfin_processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE' THEN
    UPDATE public.timesheets_financials AS tf_update
       SET processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum,
           updated_at = v_now
     WHERE tf_update.id = v_tsfin_id
       AND tf_update.is_current = TRUE;
  END IF;

  SELECT tms_user_check.id
    INTO v_created_by_user_id
  FROM public.tms_users AS tms_user_check
  WHERE tms_user_check.id = p_actor_user_id
  LIMIT 1;

  v_client_idempotency_key := NULLIF(
    REGEXP_REPLACE(
      LOWER(BTRIM(COALESCE(p_idempotency_key, ''))),
      '[^a-z0-9._:-]+',
      '-',
      'g'
    ),
    ''
  );

  IF v_client_idempotency_key IS NULL THEN
    v_client_idempotency_key := 'auto:' || md5(CONCAT_WS('|',
      v_current_timesheet_id::text,
      COALESCE(v_current_version::text, ''),
      LOWER(COALESCE(v_candidate_email, ''))
    ));
  END IF;

  v_recipient_namespace := md5(LOWER(COALESCE(v_candidate_email, '')));

  v_idempotency_key := 'timesheet_qr_send:'
    || v_current_timesheet_id::text
    || ':v' || COALESCE(v_current_version::text, '0')
    || ':recipient:' || v_recipient_namespace
    || ':key:' || md5(v_client_idempotency_key);

  PERFORM pg_advisory_xact_lock(hashtext('timesheet_qr_send:' || v_current_timesheet_id::text));
  PERFORM pg_advisory_xact_lock(hashtext(v_idempotency_key));

  v_pdf_key := 'docs-pdf/timesheets/ts_' || v_current_timesheet_id::text || '.pdf';
  v_mail_reference := v_idempotency_key;
  v_mail_scheduled_for_utc := TIMESTAMPTZ '9999-12-31 00:00:00+00';
  v_mail_subject := CASE
    WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Weekly QR timesheet – week ending ' || COALESCE(v_week_ending_date::text, '(unknown)')
    ELSE 'Daily QR timesheet – ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)')
  END;
  v_mail_body_text := CONCAT_WS(E'\n',
    'Please print the attached timesheet, ask the ward manager to sign it,',
    'and then upload the signed copy via the app.',
    '',
    CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END,
    'Timesheet ID: ' || v_current_timesheet_id::text
  );
  v_mail_body_html := '<p>Please print the attached timesheet, ask the ward manager to sign it, and then upload the signed copy via the app.<br/><br/>'
    || CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END
    || '<br/>Timesheet ID: ' || v_current_timesheet_id::text || '</p>';

  SELECT existing_pdf.id
    INTO v_existing_pdf_job_id
  FROM public.ts_pdfs_outbox AS existing_pdf
  WHERE existing_pdf.timesheet_id = v_current_timesheet_id
    AND existing_pdf.reason = 'FORCE_REGEN'::public.ts_pdf_reason_enum
  ORDER BY existing_pdf.created_at DESC,
           existing_pdf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_pdf_job_id IS NOT NULL THEN
    UPDATE public.ts_pdfs_outbox AS pdf_update
       SET attempt_count = 0,
           next_attempt_at = NULL,
           last_error = NULL,
           prefer_generated = TRUE,
           force_regen = TRUE
     WHERE pdf_update.id = v_existing_pdf_job_id
    RETURNING pdf_update.id INTO v_pdf_job_id;
  ELSE
    INSERT INTO public.ts_pdfs_outbox AS pdf_insert (
      timesheet_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      prefer_generated,
      force_regen,
      created_at
    ) VALUES (
      v_current_timesheet_id,
      'FORCE_REGEN'::public.ts_pdf_reason_enum,
      0,
      NULL,
      NULL,
      TRUE,
      TRUE,
      v_now
    )
    RETURNING pdf_insert.id INTO v_pdf_job_id;
  END IF;

  SELECT mail_existing.id,
         mail_existing.status::text
    INTO v_existing_mail_id,
         v_existing_mail_status
  FROM public.mail_outbox AS mail_existing
  WHERE mail_existing.type = 'TIMESHEET_QR'
    AND mail_existing.reference = v_mail_reference
    AND mail_existing.context_kind = 'timesheets'
    AND mail_existing.context_id = v_current_timesheet_id
    AND mail_existing."to" = v_candidate_email
  ORDER BY mail_existing.created_at_utc DESC,
           mail_existing.id DESC
  LIMIT 1;

  IF v_existing_mail_id IS NOT NULL THEN
    v_mail_job_id := v_existing_mail_id;

    IF UPPER(COALESCE(v_existing_mail_status, '')) = 'SENT' THEN
      v_send_state := 'ALREADY_SENT';
    ELSE
      UPDATE public.mail_outbox AS mail_update
         SET status = 'QUEUED'::public.mail_status_enum,
             subject = v_mail_subject,
             body_html = v_mail_body_html,
             body_text = v_mail_body_text,
             attachments = jsonb_build_array(jsonb_build_object('r2_key', v_pdf_key, 'filename', 'Timesheet_' || COALESCE(v_week_ending_date::text, v_current_timesheet_id::text) || '.pdf')),
             last_error = NULL,
             failed_at = NULL,
             scheduled_for_utc = v_mail_scheduled_for_utc,
             next_attempt_at_utc = v_mail_scheduled_for_utc,
             provider_status = NULL,
             provider_message_id = NULL,
             attempt_lease_token = NULL,
             attempt_leased_at_utc = NULL,
             attempt_lease_expires_at_utc = NULL,
             payment_scope_json = jsonb_build_object(
               'job_kind', 'TIMESHEET_QR_SEND',
               'pdf_job_id', v_pdf_job_id::text,
               'idempotency_key', v_idempotency_key,
               'client_idempotency_key', v_client_idempotency_key,
               'requires_pdf_render', TRUE,
               'release_mail_after_pdf_render', TRUE,
               'mail_delayed_for_pdf_render', TRUE,
        'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
        'mail_hold_reason', 'PDF_RENDER_PENDING',
               'pdf_render_delay_minutes', 10,
               'pdf_storage_key', v_pdf_key,
               'current_timesheet_id', v_current_timesheet_id::text,
               'current_version', v_current_version,
               'recipient_email', v_candidate_email
             )
       WHERE mail_update.id = v_existing_mail_id;

      v_send_state := CASE WHEN UPPER(COALESCE(v_existing_mail_status, '')) = 'FAILED' THEN 'PDF_REQUEUED_MAIL_HELD' ELSE 'PDF_ALREADY_QUEUED_MAIL_HELD' END;
    END IF;
  ELSE
    INSERT INTO public.mail_outbox AS mail_insert (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      importance,
      email_type,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      last_error,
      created_at_utc,
      sent_at,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      mailshot_run_id,
      document_template_id,
      provider_status,
      delivered_at,
      read_at,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json
    ) VALUES (
      'TIMESHEET_QR',
      v_candidate_email,
      NULL,
      NULL,
      NULL,
      'Normal',
      'html',
      v_mail_subject,
      v_mail_body_html,
      v_mail_body_text,
      jsonb_build_array(jsonb_build_object('r2_key', v_pdf_key, 'filename', 'Timesheet_' || COALESCE(v_week_ending_date::text, v_current_timesheet_id::text) || '.pdf')),
      'QUEUED'::public.mail_status_enum,
      NULL,
      v_now,
      NULL,
      v_created_by_user_id,
      v_mail_reference,
      'candidate',
      v_candidate_id,
      'timesheets',
      v_current_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_mail_scheduled_for_utc,
      v_mail_scheduled_for_utc,
      jsonb_build_object(
        'job_kind', 'TIMESHEET_QR_SEND',
        'pdf_job_id', v_pdf_job_id::text,
        'idempotency_key', v_idempotency_key,
        'client_idempotency_key', v_client_idempotency_key,
        'requires_pdf_render', TRUE,
        'release_mail_after_pdf_render', TRUE,
        'mail_delayed_for_pdf_render', TRUE,
        'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
        'mail_hold_reason', 'PDF_RENDER_PENDING',
        'pdf_render_delay_minutes', 10,
        'pdf_storage_key', v_pdf_key,
        'current_timesheet_id', v_current_timesheet_id::text,
        'current_version', v_current_version,
        'recipient_email', v_candidate_email
      )
    )
    RETURNING id INTO v_mail_job_id;

    v_send_state := 'PDF_QUEUED_MAIL_HELD';
  END IF;

  v_job_id := v_mail_job_id;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_decision_v1(jsonb_build_object(
    'dataset_mode', 'process',
    'timesheet_id', v_current_timesheet_id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');

  INSERT INTO public.audit_events AS audit_insert (
    ts_utc,
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    v_now,
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_QR_SEND_QUEUED',
    NULL,
    jsonb_build_object(
      'timesheet_id', v_current_timesheet_id,
      'job_id', v_job_id,
      'mail_outbox_id', v_mail_job_id,
      'pdf_job_id', v_pdf_job_id,
      'idempotency_key', v_idempotency_key,
      'send_state', v_send_state,
      'recipient_email', v_candidate_email,
      'scheduled_for_utc', v_mail_scheduled_for_utc,
      'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
      'mail_delayed_for_pdf_render', TRUE,
      'mail_hold_reason', 'PDF_RENDER_PENDING',
      'pdf_render_delay_minutes', 10,
      'pdf_storage_key', v_pdf_key,
      'client_idempotency_key', v_client_idempotency_key,
      'recipient_namespace', v_recipient_namespace
    ),
    'Bulk QR send enqueue'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'queued', TRUE,
    'operation', 'timesheet_qr_send_enqueue',
    'job_id', v_job_id,
    'mail_outbox_id', v_mail_job_id,
    'pdf_job_id', v_pdf_job_id,
    'idempotency_key', v_idempotency_key,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'recipient_available', TRUE,
    'recipient_email', v_candidate_email,
    'recipient_name', v_candidate_name,
    'send_state', v_send_state,
    'scheduled_for_utc', v_mail_scheduled_for_utc,
    'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
    'mail_delayed_for_pdf_render', TRUE,
    'mail_hold_reason', 'PDF_RENDER_PENDING',
    'pdf_render_delay_minutes', 10,
    'pdf_storage_key', v_pdf_key,
    'client_idempotency_key', v_client_idempotency_key,
    'recipient_namespace', v_recipient_namespace,
    'row_patch', COALESCE(v_post_row->'row_patch', jsonb_build_object()),
    'data_row', COALESCE(v_post_row, jsonb_build_object()),
    'row', COALESCE(v_post_row, jsonb_build_object()),
    'cache_invalidation_hints', jsonb_build_object(
      'row_keys', jsonb_build_array(COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', jsonb_build_array(v_current_timesheet_id),
      'storage_keys', jsonb_build_array(v_storage_key, v_pdf_key),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise'),
      'row_signature', v_row_signature,
      'invalidate_context', TRUE,
      'invalidate_preview', TRUE
    ),
    'cache_invalidation', jsonb_build_object(
      'rows', jsonb_build_array(jsonb_build_object(
        'row_key', COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text),
        'timesheet_id', v_current_timesheet_id,
        'new_row_signature', v_row_signature
      )),
      'artifacts', jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'storage_key', COALESCE(v_storage_key, v_pdf_key),
        'pdf_storage_key', v_pdf_key,
        'changed', TRUE
      )),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise')
    )
  );
END;
$function$;


DROP FUNCTION IF EXISTS public.bulk_authorise_import_evidence_page_v1(jsonb, text, text, text, integer, integer, uuid);

CREATE OR REPLACE FUNCTION public.bulk_authorise_import_evidence_page_v1(
  p_items jsonb DEFAULT '[]'::jsonb,
  p_classification text DEFAULT NULL,
  p_section text DEFAULT NULL,
  p_mode text DEFAULT 'view_all',
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 20,
  p_actor_user_id uuid DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb := COALESCE(p_items, '[]'::jsonb);
  v_classification text := UPPER(NULLIF(BTRIM(COALESCE(p_classification, '')), ''));
  v_section text := LOWER(NULLIF(BTRIM(COALESCE(p_section, '')), ''));
  v_mode text := LOWER(NULLIF(BTRIM(COALESCE(p_mode, 'view_all')), ''));
  v_page integer := GREATEST(COALESCE(p_page, 1), 1);
  v_page_size integer := CASE WHEN COALESCE(p_page_size, 20) <= 0 THEN NULL ELSE LEAST(GREATEST(COALESCE(p_page_size, 20), 1), 500) END;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{12}$';
  v_result jsonb := NULL;
BEGIN
  IF v_classification = 'HEALTHROSTER' THEN
    v_classification := 'HR';
  END IF;

  IF v_classification NOT IN ('NHSP', 'HR') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_CLASSIFICATION',
      'message', 'p_classification must be NHSP, HR, or HEALTHROSTER.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  IF v_section NOT IN ('processed_eligible', 'authorised_eligible') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_SECTION',
      'message', 'p_section must be processed_eligible or authorised_eligible.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  IF v_mode NOT IN ('single', 'view_all') THEN
    v_mode := 'view_all';
  END IF;

  IF jsonb_typeof(v_items) IS DISTINCT FROM 'array' THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_ITEMS',
      'message', 'p_items must be a JSON array.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  WITH raw_items AS (
    SELECT
      raw_item.ordinality::integer AS input_ordinal,
      raw_item.item AS item_json
    FROM jsonb_array_elements(v_items) WITH ORDINALITY AS raw_item(item, ordinality)
    WHERE jsonb_typeof(raw_item.item) = 'object'
  ),
  normalised_items AS (
    SELECT
      raw_items.input_ordinal,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'row_key', '')), '') AS supplied_row_key,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'row_signature', raw_items.item_json->>'expected_row_signature', '')), '') AS supplied_row_signature,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'timesheet_id', '')), '') AS supplied_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'current_timesheet_id', '')), '') AS supplied_current_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'expected_timesheet_id', '')), '') AS supplied_expected_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'contract_week_id', '')), '') AS supplied_contract_week_id_text,
      raw_items.item_json AS item_json
    FROM raw_items
  ),
  resolved_items AS (
    SELECT
      normalised_items.input_ordinal,
      normalised_items.supplied_row_key,
      normalised_items.supplied_row_signature,
      CASE
        WHEN COALESCE(normalised_items.supplied_current_timesheet_id_text, normalised_items.supplied_expected_timesheet_id_text, normalised_items.supplied_timesheet_id_text) ~* v_uuid_re
          THEN COALESCE(normalised_items.supplied_current_timesheet_id_text, normalised_items.supplied_expected_timesheet_id_text, normalised_items.supplied_timesheet_id_text)::uuid
        WHEN normalised_items.supplied_row_key LIKE 'timesheet:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('timesheet:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('timesheet:') + 1)::uuid
        ELSE NULL::uuid
      END AS requested_timesheet_id,
      CASE
        WHEN normalised_items.supplied_contract_week_id_text ~* v_uuid_re
          THEN normalised_items.supplied_contract_week_id_text::uuid
        WHEN normalised_items.supplied_row_key LIKE 'contract_week:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract_week:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract_week:') + 1)::uuid
        WHEN normalised_items.supplied_row_key LIKE 'contract-week:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract-week:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract-week:') + 1)::uuid
        ELSE NULL::uuid
      END AS requested_contract_week_id,
      normalised_items.item_json
    FROM normalised_items
  ),
  decision_rows AS (
    SELECT
      resolved_items.input_ordinal,
      resolved_items.supplied_row_key,
      resolved_items.supplied_row_signature,
      resolved_items.requested_timesheet_id,
      resolved_items.requested_contract_week_id,
      decision_result.row_json AS row_json
    FROM resolved_items
    LEFT JOIN LATERAL (
      SELECT decision_call.row_json
      FROM public.bulk_timesheet_row_decision_v1(
        CASE
          WHEN resolved_items.requested_timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'timesheet_id', resolved_items.requested_timesheet_id::text
          )
          WHEN resolved_items.requested_contract_week_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'contract_week_id', resolved_items.requested_contract_week_id::text
          )
          WHEN resolved_items.supplied_row_key IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'row_key', resolved_items.supplied_row_key
          )
          ELSE JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'row_key', '__NO_VALID_ROW_KEY__'
          )
        END
      ) AS decision_call(row_json)
      LIMIT 1
    ) AS decision_result ON TRUE
  ),
  evaluated_rows AS (
    SELECT
      decision_rows.input_ordinal,
      decision_rows.supplied_row_key,
      decision_rows.supplied_row_signature,
      decision_rows.requested_timesheet_id,
      decision_rows.requested_contract_week_id,
      decision_rows.row_json,
      NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_key', '')), '') AS decision_row_key,
      NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), '') AS decision_row_signature,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'current_timesheet_id', decision_rows.row_json->>'timesheet_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'current_timesheet_id', decision_rows.row_json->>'timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS decision_timesheet_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'contract_week_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'contract_week_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS decision_contract_week_id,
      UPPER(NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'bulk_authorise_classification', decision_rows.row_json->'bulk_authorise'->>'classification', '')), '')) AS decision_classification,
      LOWER(NULLIF(BTRIM(COALESCE(
        decision_rows.row_json->>'bulk_authorise_section',
        decision_rows.row_json->'bulk_authorise'->>'section',
        CASE
          WHEN LOWER(COALESCE(decision_rows.row_json->>'can_bulk_authorise', 'false')) IN ('true', 't', '1', 'yes') THEN 'processed_eligible'
          WHEN LOWER(COALESCE(decision_rows.row_json->>'can_bulk_unauthorise', 'false')) IN ('true', 't', '1', 'yes') THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        ''
      )), '')) AS decision_section,
      (
        LOWER(COALESCE(decision_rows.row_json->>'is_import_authoritative', 'false')) IN ('true', 't', '1', 'yes')
        OR LOWER(COALESCE(decision_rows.row_json->'action_flags'->>'is_import_authoritative', 'false')) IN ('true', 't', '1', 'yes')
        OR UPPER(COALESCE(decision_rows.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE'
      ) AS is_import_authoritative,
      (
        NULLIF(BTRIM(COALESCE(decision_rows.supplied_row_signature, '')), '') IS NOT NULL
        AND COALESCE(NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), ''), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(decision_rows.supplied_row_signature, '')), '')
      ) AS is_stale
    FROM decision_rows
  ),
  accepted_candidates AS (
    SELECT
      evaluated_rows.input_ordinal,
      evaluated_rows.decision_row_key,
      evaluated_rows.decision_row_signature,
      evaluated_rows.decision_timesheet_id,
      evaluated_rows.decision_contract_week_id,
      evaluated_rows.row_json,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'candidate_name', evaluated_rows.row_json->>'candidate_display_name', '')), '') AS candidate_name,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'client_name', evaluated_rows.row_json->>'client_display_name', '')), '') AS client_name,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'week_ending_date', evaluated_rows.row_json->>'contract_week_ending_date', '')), '') AS week_ending_date_text
    FROM evaluated_rows
    WHERE evaluated_rows.row_json IS NOT NULL
      AND evaluated_rows.is_stale IS NOT TRUE
      AND evaluated_rows.is_import_authoritative IS TRUE
      AND evaluated_rows.decision_timesheet_id IS NOT NULL
      AND evaluated_rows.decision_classification = v_classification
      AND evaluated_rows.decision_section = v_section
  ),
  accepted_scope AS (
    SELECT DISTINCT ON (accepted_candidates.decision_timesheet_id)
      accepted_candidates.input_ordinal,
      accepted_candidates.decision_row_key,
      accepted_candidates.decision_row_signature,
      accepted_candidates.decision_timesheet_id,
      accepted_candidates.decision_contract_week_id,
      accepted_candidates.row_json,
      accepted_candidates.candidate_name,
      accepted_candidates.client_name,
      accepted_candidates.week_ending_date_text
    FROM accepted_candidates
    ORDER BY accepted_candidates.decision_timesheet_id, accepted_candidates.input_ordinal
  ),
  stale_items AS (
    SELECT
      evaluated_rows.input_ordinal,
      JSONB_BUILD_OBJECT(
        'code', 'ROW_SIGNATURE_MISMATCH',
        'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
        'timesheet_id', COALESCE(evaluated_rows.decision_timesheet_id, evaluated_rows.requested_timesheet_id),
        'contract_week_id', COALESCE(evaluated_rows.decision_contract_week_id, evaluated_rows.requested_contract_week_id),
        'expected_row_signature', evaluated_rows.supplied_row_signature,
        'current_row_signature', evaluated_rows.decision_row_signature
      ) AS item_json
    FROM evaluated_rows
    WHERE evaluated_rows.is_stale IS TRUE
  ),
  warning_items AS (
    SELECT
      evaluated_rows.input_ordinal,
      CASE
        WHEN evaluated_rows.row_json IS NULL THEN JSONB_BUILD_OBJECT(
          'code', 'ROW_NOT_FOUND',
          'row_key', evaluated_rows.supplied_row_key,
          'timesheet_id', evaluated_rows.requested_timesheet_id,
          'contract_week_id', evaluated_rows.requested_contract_week_id
        )
        WHEN evaluated_rows.is_stale IS TRUE THEN JSONB_BUILD_OBJECT(
          'code', 'ROW_SIGNATURE_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', COALESCE(evaluated_rows.decision_timesheet_id, evaluated_rows.requested_timesheet_id),
          'contract_week_id', COALESCE(evaluated_rows.decision_contract_week_id, evaluated_rows.requested_contract_week_id),
          'expected_row_signature', evaluated_rows.supplied_row_signature,
          'current_row_signature', evaluated_rows.decision_row_signature
        )
        WHEN evaluated_rows.is_import_authoritative IS NOT TRUE THEN JSONB_BUILD_OBJECT(
          'code', 'NOT_IMPORT_AUTHORITATIVE',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id
        )
        WHEN evaluated_rows.decision_classification IS DISTINCT FROM v_classification THEN JSONB_BUILD_OBJECT(
          'code', 'CLASSIFICATION_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id,
          'expected_classification', v_classification,
          'actual_classification', evaluated_rows.decision_classification
        )
        WHEN evaluated_rows.decision_section IS DISTINCT FROM v_section THEN JSONB_BUILD_OBJECT(
          'code', 'SECTION_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id,
          'expected_section', v_section,
          'actual_section', evaluated_rows.decision_section
        )
        ELSE NULL::jsonb
      END AS warning_json
    FROM evaluated_rows
  ),
  source_imports AS (
    SELECT
      accepted_scope.input_ordinal,
      accepted_scope.decision_row_key,
      accepted_scope.decision_row_signature,
      accepted_scope.decision_timesheet_id,
      accepted_scope.decision_contract_week_id,
      accepted_scope.candidate_name,
      accepted_scope.client_name,
      accepted_scope.week_ending_date_text,
      import_rows.source_system,
      import_rows.import_id,
      import_rows.filename,
      import_rows.uploaded_at_utc,
      import_rows.file_r2_key,
      COALESCE(import_rows.header_rows, '[]'::jsonb) AS header_rows,
      COALESCE(import_rows.header_columns, '[]'::jsonb) AS header_columns,
      COALESCE(import_rows.rows, '[]'::jsonb) AS rows_json
    FROM accepted_scope
    CROSS JOIN LATERAL public.timesheet_import_rows_for_timesheet_current(
      accepted_scope.decision_timesheet_id,
      TRUE,
      NULL::uuid,
      NULL::uuid
    ) AS import_rows(
      requested_timesheet_id,
      current_timesheet_id,
      source_system,
      import_id,
      filename,
      uploaded_at_utc,
      file_r2_key,
      header_rows,
      header_columns,
      rows
    )
  ),
  import_row_elements AS (
    SELECT
      source_imports.input_ordinal,
      source_imports.decision_row_key,
      source_imports.decision_row_signature,
      source_imports.decision_timesheet_id,
      source_imports.decision_contract_week_id,
      source_imports.candidate_name,
      source_imports.client_name,
      source_imports.week_ending_date_text,
      source_imports.source_system,
      source_imports.import_id,
      source_imports.filename,
      source_imports.uploaded_at_utc,
      source_imports.file_r2_key,
      source_imports.header_rows,
      source_imports.header_columns,
      import_row_item.row_value AS row_value,
      (import_row_item.row_ordinality - 1)::integer AS raw_row_index
    FROM source_imports
    CROSS JOIN LATERAL jsonb_array_elements(source_imports.rows_json) WITH ORDINALITY AS import_row_item(row_value, row_ordinality)
  ),
  flattened_items AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          import_row_elements.input_ordinal,
          import_row_elements.uploaded_at_utc NULLS LAST,
          import_row_elements.import_id,
          import_row_elements.raw_row_index
      ) AS result_ordinal,
      JSONB_BUILD_OBJECT(
        'id',
          CONCAT_WS('|',
            import_row_elements.decision_row_key,
            COALESCE(import_row_elements.import_id::text, ''),
            COALESCE(import_row_elements.row_value->'payload'->>'external_row_key', import_row_elements.row_value->'payload'->>'row_key', import_row_elements.row_value->'payload'->>'external_key', ''),
            COALESCE(import_row_elements.row_value->'payload'->>'source_row_key', import_row_elements.row_value->'payload'->>'stable_source_key', import_row_elements.raw_row_index::text)
          ),
        'timesheet_id', import_row_elements.decision_timesheet_id,
        'row_key', import_row_elements.decision_row_key,
        'row_signature', import_row_elements.decision_row_signature,
        'contract_week_id', import_row_elements.decision_contract_week_id,
        'candidate_name', import_row_elements.candidate_name,
        'client_name', import_row_elements.client_name,
        'week_ending_date', import_row_elements.week_ending_date_text,
        'source_system', import_row_elements.source_system,
        'import_id', import_row_elements.import_id,
        'filename', import_row_elements.filename,
        'uploaded_at_utc', import_row_elements.uploaded_at_utc,
        'file_r2_key', import_row_elements.file_r2_key,
        'header_rows', import_row_elements.header_rows,
        'header_columns', import_row_elements.header_columns,
        'raw_row_index', import_row_elements.raw_row_index,
        'raw_columns', import_row_elements.row_value->'raw_columns',
        'raw_row', COALESCE(import_row_elements.row_value->'payload', '{}'::jsonb),
        'external_row_key', NULLIF(BTRIM(COALESCE(import_row_elements.row_value->'payload'->>'external_row_key', import_row_elements.row_value->'payload'->>'row_key', import_row_elements.row_value->'payload'->>'external_key', '')), ''),
        'source_row_key', NULLIF(BTRIM(COALESCE(import_row_elements.row_value->'payload'->>'source_row_key', import_row_elements.row_value->'payload'->>'stable_source_key', import_row_elements.row_value->'payload'->>'source_key', '')), ''),
        'nhsp_shift_id', COALESCE(import_row_elements.row_value->'payload'->'nhsp_shift_id', import_row_elements.row_value->'payload'->'shift_id'),
        'hr_request_id', COALESCE(import_row_elements.row_value->'payload'->'hr_request_id', import_row_elements.row_value->'payload'->'request_id'),
        'hr_shift_id', import_row_elements.row_value->'payload'->'hr_shift_id'
      ) AS item_json
    FROM import_row_elements
  ),
  totals AS (
    SELECT COUNT(*)::integer AS total_count
    FROM flattened_items
  ),
  page_meta AS (
    SELECT
      totals.total_count,
      CASE
        WHEN v_page_size IS NULL THEN 1
        WHEN totals.total_count = 0 THEN 1
        ELSE LEAST(v_page, GREATEST(CEIL(totals.total_count::numeric / v_page_size::numeric)::integer, 1))
      END AS effective_page,
      CASE
        WHEN v_page_size IS NULL THEN totals.total_count
        ELSE v_page_size
      END AS effective_page_size,
      CASE
        WHEN v_page_size IS NULL THEN CASE WHEN totals.total_count > 0 THEN 1 ELSE 0 END
        WHEN totals.total_count = 0 THEN 0
        ELSE CEIL(totals.total_count::numeric / v_page_size::numeric)::integer
      END AS total_pages
    FROM totals
  ),
  page_items AS (
    SELECT flattened_items.item_json
    FROM flattened_items
    CROSS JOIN page_meta
    WHERE v_page_size IS NULL
       OR (
         flattened_items.result_ordinal > ((page_meta.effective_page - 1) * page_meta.effective_page_size)
         AND flattened_items.result_ordinal <= (page_meta.effective_page * page_meta.effective_page_size)
       )
    ORDER BY flattened_items.result_ordinal
  ),
  accepted_json AS (
    SELECT
      COALESCE(JSONB_AGG(accepted_scope.decision_row_key ORDER BY accepted_scope.input_ordinal), '[]'::jsonb) AS accepted_row_keys,
      COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT(
        'row_key', accepted_scope.decision_row_key,
        'row_signature', accepted_scope.decision_row_signature,
        'timesheet_id', accepted_scope.decision_timesheet_id,
        'contract_week_id', accepted_scope.decision_contract_week_id,
        'candidate_name', accepted_scope.candidate_name,
        'client_name', accepted_scope.client_name,
        'week_ending_date', accepted_scope.week_ending_date_text
      ) ORDER BY accepted_scope.input_ordinal), '[]'::jsonb) AS accepted_scope_json
    FROM accepted_scope
  ),
  stale_json AS (
    SELECT COALESCE(JSONB_AGG(stale_items.item_json ORDER BY stale_items.input_ordinal), '[]'::jsonb) AS stale_items_json
    FROM stale_items
  ),
  warnings_json AS (
    SELECT COALESCE(JSONB_AGG(warning_items.warning_json ORDER BY warning_items.input_ordinal), '[]'::jsonb) AS warnings_json
    FROM warning_items
    WHERE warning_items.warning_json IS NOT NULL
  ),
  imports_json AS (
    SELECT COALESCE(JSONB_AGG(import_summary.import_json ORDER BY import_summary.source_system, import_summary.uploaded_at_utc NULLS LAST, import_summary.import_id), '[]'::jsonb) AS imports_array
    FROM (
      SELECT DISTINCT ON (source_imports.decision_timesheet_id, source_imports.import_id, source_imports.file_r2_key)
        source_imports.source_system,
        source_imports.uploaded_at_utc,
        source_imports.import_id,
        JSONB_BUILD_OBJECT(
          'timesheet_id', source_imports.decision_timesheet_id,
          'row_key', source_imports.decision_row_key,
          'contract_week_id', source_imports.decision_contract_week_id,
          'source_system', source_imports.source_system,
          'import_id', source_imports.import_id,
          'filename', source_imports.filename,
          'uploaded_at_utc', source_imports.uploaded_at_utc,
          'file_r2_key', source_imports.file_r2_key,
          'header_rows', source_imports.header_rows,
          'header_columns', source_imports.header_columns
        ) AS import_json
      FROM source_imports
      ORDER BY source_imports.decision_timesheet_id, source_imports.import_id, source_imports.file_r2_key, source_imports.uploaded_at_utc NULLS LAST
    ) AS import_summary
  ),
  headers_json AS (
    SELECT
      COALESCE((
        SELECT source_imports.header_rows
        FROM source_imports
        WHERE jsonb_typeof(source_imports.header_rows) = 'array'
          AND jsonb_array_length(source_imports.header_rows) > 0
        ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
        LIMIT 1
      ), '[]'::jsonb) AS header_rows,
      COALESCE((
        SELECT source_imports.header_columns
        FROM source_imports
        WHERE jsonb_typeof(source_imports.header_columns) = 'array'
          AND jsonb_array_length(source_imports.header_columns) > 0
        ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
        LIMIT 1
      ), '[]'::jsonb) AS header_columns
  ),
  fallback_columns AS (
    SELECT COALESCE(JSONB_AGG(DISTINCT payload_keys.payload_key), '[]'::jsonb) AS column_keys
    FROM import_row_elements
    CROSS JOIN LATERAL jsonb_object_keys(COALESCE(import_row_elements.row_value->'payload', '{}'::jsonb)) AS payload_keys(payload_key)
  ),
  mapping_json AS (
    SELECT COALESCE(JSONB_AGG(mapping_rows.mapping_json ORDER BY mapping_rows.result_ordinal), '[]'::jsonb) AS mapping_array
    FROM (
      SELECT
        flattened_items.result_ordinal,
        JSONB_BUILD_OBJECT(
          'row_id', flattened_items.item_json->>'id',
          'timesheet_id', flattened_items.item_json->>'timesheet_id',
          'row_key', flattened_items.item_json->>'row_key',
          'contract_week_id', flattened_items.item_json->>'contract_week_id'
        ) AS mapping_json
      FROM flattened_items
    ) AS mapping_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'success', TRUE,
    'source_system', (
      SELECT source_imports.source_system
      FROM source_imports
      WHERE NULLIF(BTRIM(COALESCE(source_imports.source_system, '')), '') IS NOT NULL
      ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
      LIMIT 1
    ),
    'classification', v_classification,
    'mode', v_mode,
    'selected_section', v_section,
    'page', page_meta.effective_page,
    'page_size', page_meta.effective_page_size,
    'total', page_meta.total_count,
    'total_rows', page_meta.total_count,
    'total_pages', page_meta.total_pages,
    'items', COALESCE((SELECT JSONB_AGG(page_items.item_json ORDER BY page_items.item_json->>'id') FROM page_items), '[]'::jsonb),
    'rows', COALESCE((SELECT JSONB_AGG(page_items.item_json ORDER BY page_items.item_json->>'id') FROM page_items), '[]'::jsonb),
    'header_rows', headers_json.header_rows,
    'header_columns', headers_json.header_columns,
    'display_columns', CASE
      WHEN jsonb_typeof(headers_json.header_columns) = 'array' AND jsonb_array_length(headers_json.header_columns) > 0 THEN headers_json.header_columns
      ELSE fallback_columns.column_keys
    END,
    'imports', imports_json.imports_array,
    'row_to_timesheet_mapping', mapping_json.mapping_array,
    'stale_items', stale_json.stale_items_json,
    'accepted_row_keys', accepted_json.accepted_row_keys,
    'accepted_scope', accepted_json.accepted_scope_json,
    'warnings', warnings_json.warnings_json,
    'actor_user_id', p_actor_user_id
  )
    INTO v_result
  FROM page_meta
  CROSS JOIN headers_json
  CROSS JOIN fallback_columns
  CROSS JOIN imports_json
  CROSS JOIN mapping_json
  CROSS JOIN stale_json
  CROSS JOIN accepted_json
  CROSS JOIN warnings_json;

  RETURN COALESCE(v_result, JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'success', TRUE,
    'classification', v_classification,
    'mode', v_mode,
    'selected_section', v_section,
    'page', 1,
    'page_size', COALESCE(v_page_size, 0),
    'total', 0,
    'total_rows', 0,
    'total_pages', 0,
    'items', '[]'::jsonb,
    'rows', '[]'::jsonb,
    'header_rows', '[]'::jsonb,
    'header_columns', '[]'::jsonb,
    'display_columns', '[]'::jsonb,
    'imports', '[]'::jsonb,
    'row_to_timesheet_mapping', '[]'::jsonb,
    'stale_items', '[]'::jsonb,
    'accepted_row_keys', '[]'::jsonb,
    'accepted_scope', '[]'::jsonb,
    'warnings', '[]'::jsonb,
    'actor_user_id', p_actor_user_id
  ));
END;
$function$;


