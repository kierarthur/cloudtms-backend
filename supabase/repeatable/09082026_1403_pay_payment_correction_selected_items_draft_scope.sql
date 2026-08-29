
CREATE OR REPLACE FUNCTION public._pay_payment_correction_selected_items(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_include_already_corrected boolean DEFAULT false
)
RETURNS TABLE(
  pay_batch_id uuid,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  candidate_display_name text,
  candidate_tms_ref text,
  pay_batch_item_id uuid,
  item_type text,
  timesheet_id uuid,
  pay_bank_transfer_id uuid,
  transfer_status text,
  transfer_amount numeric,
  transfer_group_key text,
  payee_entity_kind text,
  payee_entity_id uuid,
  umbrella_id uuid,
  umbrella_name text,
  finance_case_id uuid,
  finance_component_id uuid,
  reservation_id uuid,
  pay_channel text,
  frozen_source_pay_method text,
  frozen_target_pay_method text,
  current_candidate_pay_method text,
  economic_key_type text,
  economic_key_value text,
  source_amount_ex_vat numeric,
  target_amount_ex_vat numeric,
  key_resolution_source text,
  key_resolution_failure_reason text,
  amount_ex_vat numeric,
  amount_vat numeric,
  amount_inc_vat numeric,
  is_voided boolean,
  already_corrected boolean,
  applied_correction_kinds text[]
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_type text;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_item_types text[] := ARRAY[]::text[];

  v_umbrella_id uuid;
  v_umbrella_id_text text;
  v_transfer_group_key text;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_count_text text;
  v_work_unit text := NULL::text;
  v_source_correction_request_id_text text := NULL::text;
  v_explicit_item_ids_authoritative boolean := false;

  v_return_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_item_count integer := 0;
  v_selected_already_corrected_count integer := 0;
  v_return_item_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_returned_count integer := 0;
  v_sorted_return_item_ids uuid[] := ARRAY[]::uuid[];
  v_sorted_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
BEGIN
  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'selection_scope_type', CASE
        WHEN p_selection_json IS NULL THEN NULL
        ELSE p_selection_json->>'scope_type'
      END,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'selection_keys', CASE
        WHEN p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN '[]'::jsonb
        ELSE COALESCE((
          SELECT jsonb_agg(selection_keys.key_name ORDER BY selection_keys.key_name)
          FROM jsonb_object_keys(p_selection_json) AS selection_keys(key_name)
        ), '[]'::jsonb)
      END
    ),
    'pay_payment_correction',
    COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_check
    WHERE batch_check.id = p_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_scope_type := upper(nullif(btrim(coalesce(p_selection_json->>'scope_type', '')), ''));

  -- Compatibility is deliberately limited to the exact server-owned whole-Draft
  -- cancellation envelope produced by pay_batch_cancel before scope_type was
  -- frozen explicitly. Every other caller must still supply scope_type.
  IF v_scope_type IS NULL
     AND upper(btrim(coalesce(p_selection_json->>'requested_action', ''))) = 'DRAFT_CANCEL'
     AND upper(btrim(coalesce(p_selection_json->>'mode', ''))) = 'ALL_MATCHING'
     AND coalesce(p_selection_json->>'source_context', '') = 'pay_batch_cancel'
     AND coalesce(p_selection_json->'filter_json', '{}'::jsonb) = '{}'::jsonb
     AND coalesce(p_selection_json->'exclusions', '[]'::jsonb) = '[]'::jsonb THEN
    v_scope_type := 'BATCH';
  END IF;

  IF v_scope_type IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF v_scope_type = 'TIMESHEET'
     OR p_selection_json ? 'timesheet_id'
     OR p_selection_json ? 'timesheet_ids' THEN
    RAISE EXCEPTION 'TIMESHEET_SCOPE_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'TIMESHEET_SCOPE_NOT_ALLOWED',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF v_scope_type NOT IN ('BATCH', 'CANDIDATES', 'TRANSFER', 'UMBRELLA_PAYMENT_GROUP') THEN
    RAISE EXCEPTION 'UNSUPPORTED_PAYMENT_CORRECTION_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_PAYMENT_CORRECTION_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'supported_scope_types', jsonb_build_array(
                'BATCH',
                'CANDIDATES',
                'TRANSFER',
                'UMBRELLA_PAYMENT_GROUP'
              )
            )::text;
  END IF;

  IF p_selection_json ? 'pay_batch_candidate_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BATCH_CANDIDATE_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANDIDATE_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'pay_bank_transfer_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'pay_batch_item_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'expected_pay_batch_item_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'EXPECTED_PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'EXPECTED_PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'finance_case_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'FINANCE_CASE_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'FINANCE_CASE_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'finance_component_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'FINANCE_COMPONENT_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'FINANCE_COMPONENT_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'reservation_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'RESERVATION_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'RESERVATION_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'item_types'
     AND COALESCE(jsonb_typeof(p_selection_json->'item_types'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'ITEM_TYPES_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'ITEM_TYPES_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT candidate_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_batch_candidate_ids'
        ELSE '[]'::jsonb
      END
    ) AS candidate_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_candidate_id'
    WHERE p_selection_json ? 'pay_batch_candidate_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_candidate_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;


  IF EXISTS (
    WITH raw_values AS (
      SELECT candidate_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_batch_candidate_ids'
          ELSE '[]'::jsonb
        END
      ) AS candidate_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_batch_candidate_id'
      WHERE p_selection_json ? 'pay_batch_candidate_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BATCH_CANDIDATE_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_CANDIDATE_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT transfer_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_bank_transfer_ids'
        ELSE '[]'::jsonb
      END
    ) AS transfer_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_bank_transfer_id'
    WHERE p_selection_json ? 'pay_bank_transfer_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT transfer_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_bank_transfer_id'
      WHERE p_selection_json ? 'pay_bank_transfer_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BANK_TRANSFER_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BANK_TRANSFER_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS item_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_item_id'
    WHERE p_selection_json ? 'pay_batch_item_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT item_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_batch_item_id'
      WHERE p_selection_json ? 'pay_batch_item_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BATCH_ITEM_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_ITEM_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT expected_item_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
          THEN p_selection_json->'expected_pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS expected_item_array_values(raw_value)
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_expected_pay_batch_item_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT expected_item_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN p_selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS expected_item_array_values(raw_value)
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_EXPECTED_PAY_BATCH_ITEM_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_EXPECTED_PAY_BATCH_ITEM_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT finance_case_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') = 'array'
          THEN p_selection_json->'finance_case_ids'
        ELSE '[]'::jsonb
      END
    ) AS finance_case_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'finance_case_id'
    WHERE p_selection_json ? 'finance_case_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT finance_case_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') = 'array'
            THEN p_selection_json->'finance_case_ids'
          ELSE '[]'::jsonb
        END
      ) AS finance_case_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'finance_case_id'
      WHERE p_selection_json ? 'finance_case_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_FINANCE_CASE_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_FINANCE_CASE_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT finance_component_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') = 'array'
          THEN p_selection_json->'finance_component_ids'
        ELSE '[]'::jsonb
      END
    ) AS finance_component_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'finance_component_id'
    WHERE p_selection_json ? 'finance_component_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_finance_component_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT finance_component_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') = 'array'
            THEN p_selection_json->'finance_component_ids'
          ELSE '[]'::jsonb
        END
      ) AS finance_component_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'finance_component_id'
      WHERE p_selection_json ? 'finance_component_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_FINANCE_COMPONENT_ID'
      USING ERRCODE = 'P0001',

            DETAIL = jsonb_build_object('code', 'INVALID_FINANCE_COMPONENT_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT reservation_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') = 'array'
          THEN p_selection_json->'reservation_ids'
        ELSE '[]'::jsonb
      END
    ) AS reservation_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'reservation_id'
    WHERE p_selection_json ? 'reservation_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_reservation_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT reservation_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') = 'array'
            THEN p_selection_json->'reservation_ids'
          ELSE '[]'::jsonb
        END
      ) AS reservation_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'reservation_id'
      WHERE p_selection_json ? 'reservation_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_RESERVATION_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_RESERVATION_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_type_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'item_types'), 'null') = 'array'
          THEN p_selection_json->'item_types'
        ELSE '[]'::jsonb
      END
    ) AS item_type_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'item_type'
    WHERE p_selection_json ? 'item_type'
  ),
  cleaned_values AS (
    SELECT upper(nullif(btrim(raw_values.raw_value), '')) AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value), ARRAY[]::text[])
  INTO v_item_types
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL;

  v_umbrella_id_text := nullif(btrim(coalesce(p_selection_json->>'umbrella_id', '')), '');
  v_transfer_group_key := nullif(btrim(coalesce(p_selection_json->>'transfer_group_key', '')), '');

  IF v_umbrella_id_text IS NOT NULL THEN
    IF v_umbrella_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_UMBRELLA_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_UMBRELLA_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
    END IF;

    v_umbrella_id := v_umbrella_id_text::uuid;
  END IF;

  v_expected_item_count_text := nullif(btrim(coalesce(p_selection_json->>'expected_item_count', '')), '');
  IF v_expected_item_count_text IS NOT NULL THEN
    BEGIN
      v_expected_item_count := v_expected_item_count_text::integer;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'EXPECTED_ITEM_COUNT_MUST_BE_INTEGER'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'EXPECTED_ITEM_COUNT_MUST_BE_INTEGER',
                  'pay_batch_id', p_pay_batch_id,
                  'expected_item_count', v_expected_item_count_text
                )::text;
    END;

    IF v_expected_item_count < 0 THEN
      RAISE EXCEPTION 'EXPECTED_ITEM_COUNT_MUST_NOT_BE_NEGATIVE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'EXPECTED_ITEM_COUNT_MUST_NOT_BE_NEGATIVE',
                'pay_batch_id', p_pay_batch_id,
                'expected_item_count', v_expected_item_count
              )::text;
    END IF;
  END IF;

  v_work_unit := upper(NULLIF(btrim(COALESCE(p_selection_json->>'work_unit', '')), ''));
  v_source_correction_request_id_text := NULLIF(btrim(COALESCE(p_selection_json->>'source_correction_request_id', '')), '');

  v_explicit_item_ids_authoritative := (
    (
      COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0
      OR COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0
    )
    AND (
      v_source_correction_request_id_text IS NOT NULL
      OR v_work_unit IN ('BATCH', 'CANDIDATE', 'TRANSFER', 'CANDIDATE_TRANSFER', 'FINANCE_CASE')
    )
  );

  IF COALESCE(v_explicit_item_ids_authoritative, false)
     AND COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
     AND COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    v_pay_batch_item_ids := v_expected_pay_batch_item_ids;
  END IF;

  IF v_scope_type = 'CANDIDATES'
     AND COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF v_scope_type = 'TRANSFER'
     AND COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BANK_TRANSFER_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
     AND v_umbrella_id IS NULL THEN
    RAISE EXCEPTION 'UMBRELLA_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'UMBRELLA_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0 THEN
    IF EXISTS (
      SELECT 1
      FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
      LEFT JOIN public.pay_batch_items AS requested_pay_batch_items
        ON requested_pay_batch_items.id = requested_item_ids.requested_pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS requested_pay_batch_candidates
        ON requested_pay_batch_candidates.id = requested_pay_batch_items.pay_batch_candidate_id
      WHERE requested_pay_batch_items.id IS NULL
         OR requested_pay_batch_candidates.pay_batch_id IS DISTINCT FROM p_pay_batch_id
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'reason', 'One or more supplied pay_batch_item_ids do not exist in the selected batch.'
              )::text;
    END IF;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    IF EXISTS (
      SELECT 1
      FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id)
      LEFT JOIN public.pay_batch_items AS expected_pay_batch_items
        ON expected_pay_batch_items.id = expected_item_ids.expected_pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS expected_pay_batch_candidates
        ON expected_pay_batch_candidates.id = expected_pay_batch_items.pay_batch_candidate_id
      WHERE expected_pay_batch_items.id IS NULL
         OR expected_pay_batch_candidates.pay_batch_id IS DISTINCT FROM p_pay_batch_id
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'reason', 'One or more supplied expected_pay_batch_item_ids do not exist in the selected batch.'
              )::text;
    END IF;
  END IF;

  WITH applied_correction_rows AS (
    SELECT
      public.pay_payment_correction_items.pay_batch_item_id AS correction_pay_batch_item_id,
      public.pay_payment_correction_items.correction_item_kind AS correction_item_kind
    FROM public.pay_payment_correction_items
    WHERE public.pay_payment_correction_items.pay_batch_id = p_pay_batch_id
      AND public.pay_payment_correction_items.pay_batch_item_id IS NOT NULL
      AND public.pay_payment_correction_items.status = 'APPLIED'
  ),
  applied_corrections AS (
    SELECT
      applied_correction_rows.correction_pay_batch_item_id AS correction_pay_batch_item_id,
      array_agg(DISTINCT applied_correction_rows.correction_item_kind ORDER BY applied_correction_rows.correction_item_kind) AS applied_correction_kinds
    FROM applied_correction_rows
    GROUP BY applied_correction_rows.correction_pay_batch_item_id
  ),
  base_scope_items AS (
    SELECT DISTINCT
      public.pay_batch_items.id AS selected_pay_batch_item_id
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND (
        v_scope_type = 'BATCH'
        OR (
          v_scope_type = 'CANDIDATES'
          AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
        )
        OR (
          v_scope_type = 'TRANSFER'
          AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
        )
        OR (
          v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
          AND (
            public.pay_batch_items.umbrella_id = v_umbrella_id
            OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
            OR (
              upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
              AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
            )
          )
          AND (
            v_transfer_group_key IS NULL
            OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
          )
        )
      )
  ),
  raw_selected_items AS (
    SELECT DISTINCT

      public.pay_batch_items.id AS selected_pay_batch_item_id
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    JOIN base_scope_items
      ON base_scope_items.selected_pay_batch_item_id = public.pay_batch_items.id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
        OR public.pay_batch_items.id = ANY(v_pay_batch_item_ids)
      )
      AND (
        COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
        OR public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_finance_case_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_case_id = ANY(v_finance_case_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_finance_component_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_component_id = ANY(v_finance_component_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_reservation_ids, 1), 0) = 0
        OR public.pay_batch_items.reservation_id = ANY(v_reservation_ids)
      )
      AND (
        COALESCE(array_length(v_item_types, 1), 0) = 0
        OR upper(coalesce(public.pay_batch_items.item_type, '')) = ANY(v_item_types)
      )
  ),
  requested_item_scope_violations AS (
    SELECT requested_item_ids.requested_pay_batch_item_id
    FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
    LEFT JOIN base_scope_items
      ON base_scope_items.selected_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id
    WHERE base_scope_items.selected_pay_batch_item_id IS NULL
  ),
  decorated_selected_items AS (
    SELECT
      raw_selected_items.selected_pay_batch_item_id AS selected_pay_batch_item_id,
      COALESCE(array_length(applied_corrections.applied_correction_kinds, 1), 0) > 0 AS selected_item_already_corrected
    FROM raw_selected_items
    LEFT JOIN applied_corrections
      ON applied_corrections.correction_pay_batch_item_id = raw_selected_items.selected_pay_batch_item_id
  ),
  selection_aggregate AS (
    SELECT
      COALESCE(
        array_agg(decorated_selected_items.selected_pay_batch_item_id ORDER BY decorated_selected_items.selected_pay_batch_item_id)
          FILTER (
            WHERE COALESCE(p_include_already_corrected, false)
               OR NOT decorated_selected_items.selected_item_already_corrected
          ),
        ARRAY[]::uuid[]
      ) AS return_item_ids,
      count(*)::integer AS selected_item_count,
      (count(*) FILTER (WHERE decorated_selected_items.selected_item_already_corrected))::integer AS selected_already_corrected_count
    FROM decorated_selected_items
  )
  SELECT
    selection_aggregate.return_item_ids,
    selection_aggregate.selected_item_count,
    selection_aggregate.selected_already_corrected_count
  INTO
    v_return_item_ids,
    v_selected_item_count,
    v_selected_already_corrected_count
  FROM selection_aggregate;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0
     AND EXISTS (
       WITH base_scope_items AS (
         SELECT DISTINCT
           public.pay_batch_items.id AS selected_pay_batch_item_id
         FROM public.pay_batch_items
         JOIN public.pay_batch_candidates
           ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
         LEFT JOIN public.pay_bank_transfers
           ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
         WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
           AND (
             v_scope_type = 'BATCH'
             OR (
               v_scope_type = 'CANDIDATES'
               AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
             )
             OR (
               v_scope_type = 'TRANSFER'
               AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
             )
             OR (
               v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
               AND (
                 public.pay_batch_items.umbrella_id = v_umbrella_id
                 OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
                 OR (
                   upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
                   AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
                 )
               )
               AND (
                 v_transfer_group_key IS NULL
                 OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
               )
             )
           )
       )
       SELECT 1
       FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
       LEFT JOIN base_scope_items
         ON base_scope_items.selected_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id
       WHERE base_scope_items.selected_pay_batch_item_id IS NULL
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'pay_batch_item_ids', to_jsonb(v_pay_batch_item_ids)
            )::text;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0
     AND EXISTS (
       WITH base_scope_items AS (
         SELECT DISTINCT
           public.pay_batch_items.id AS selected_pay_batch_item_id
         FROM public.pay_batch_items
         JOIN public.pay_batch_candidates
           ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
         LEFT JOIN public.pay_bank_transfers
           ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
         WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
           AND (
             v_scope_type = 'BATCH'
             OR (
               v_scope_type = 'CANDIDATES'
               AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
             )
             OR (
               v_scope_type = 'TRANSFER'
               AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
             )
             OR (
               v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
               AND (
                 public.pay_batch_items.umbrella_id = v_umbrella_id
                 OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
                 OR (
                   upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
                   AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
                 )
               )
               AND (
                 v_transfer_group_key IS NULL
                 OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
               )
             )
           )
       )
       SELECT 1
       FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id)
       LEFT JOIN base_scope_items
         ON base_scope_items.selected_pay_batch_item_id = expected_item_ids.expected_pay_batch_item_id
       WHERE base_scope_items.selected_pay_batch_item_id IS NULL
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'expected_pay_batch_item_ids', to_jsonb(v_expected_pay_batch_item_ids),
              'reason', 'One or more expected_pay_batch_item_ids are outside the base correction scope.'
            )::text;
  END IF;

  v_return_item_count := COALESCE(array_length(v_return_item_ids, 1), 0);

  IF v_expected_item_count IS NOT NULL
     AND v_return_item_count IS DISTINCT FROM v_expected_item_count THEN
    RAISE EXCEPTION 'WORK_SELECTION_DRIFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORK_SELECTION_DRIFT',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'expected_item_count', v_expected_item_count,
              'resolved_item_count', v_return_item_count,
              'reason', 'Resolved correction item count no longer matches the expected work-item count.'
            )::text;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    SELECT
      COALESCE(array_agg(DISTINCT returned_item_ids.returned_pay_batch_item_id ORDER BY returned_item_ids.returned_pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_sorted_return_item_ids
    FROM unnest(v_return_item_ids) AS returned_item_ids(returned_pay_batch_item_id);

    SELECT
      COALESCE(array_agg(DISTINCT expected_item_ids.expected_pay_batch_item_id ORDER BY expected_item_ids.expected_pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_sorted_expected_pay_batch_item_ids
    FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id);

    IF v_sorted_return_item_ids IS DISTINCT FROM v_sorted_expected_pay_batch_item_ids THEN
      RAISE EXCEPTION 'WORK_SELECTION_DRIFT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORK_SELECTION_DRIFT',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'expected_pay_batch_item_ids', to_jsonb(v_sorted_expected_pay_batch_item_ids),
                'resolved_pay_batch_item_ids', to_jsonb(v_sorted_return_item_ids),
                'reason', 'Resolved correction item ids no longer exactly match the expected work-item ids.'
              )::text;
    END IF;
  END IF;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_RESOLVED_SCOPE',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_scope_type,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'pay_batch_candidate_id_count', COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0),
      'pay_bank_transfer_id_count', COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0),
      'pay_batch_item_id_count', COALESCE(array_length(v_pay_batch_item_ids, 1), 0),
      'expected_pay_batch_item_id_count', COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0),
      'finance_case_id_count', COALESCE(array_length(v_finance_case_ids, 1), 0),
      'finance_component_id_count', COALESCE(array_length(v_finance_component_ids, 1), 0),
      'reservation_id_count', COALESCE(array_length(v_reservation_ids, 1), 0),
      'item_type_count', COALESCE(array_length(v_item_types, 1), 0),
      'umbrella_id', v_umbrella_id,
      'transfer_group_key', v_transfer_group_key,
      'expected_item_count', v_expected_item_count,
      'work_unit', v_work_unit,
      'source_correction_request_id_present', v_source_correction_request_id_text IS NOT NULL,
      'explicit_item_ids_authoritative', COALESCE(v_explicit_item_ids_authoritative, false),
      'selected_item_count', v_selected_item_count,
      'selected_already_corrected_count', v_selected_already_corrected_count,
      'return_item_count', v_return_item_count

    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF v_return_item_count = 0 THEN
    RETURN;
  END IF;

  WITH requested_item_ids AS (
    SELECT unnest(v_return_item_ids) AS requested_pay_batch_item_id
  ),
  economic_components AS (
    SELECT
      economic_component_rows.pay_batch_item_id AS economic_pay_batch_item_id,
      economic_component_rows.key_resolution_failure_reason AS economic_key_resolution_failure_reason
    FROM public._pay_batch_item_economic_components(NULL::uuid, v_return_item_ids) AS economic_component_rows
  )
  SELECT
    (count(*) FILTER (
      WHERE economic_components.economic_pay_batch_item_id IS NULL
         OR economic_components.economic_key_resolution_failure_reason IS NOT NULL
    ))::integer
  INTO v_key_resolution_failure_count
  FROM requested_item_ids
  LEFT JOIN economic_components
    ON economic_components.economic_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id;

  RETURN QUERY
  WITH applied_correction_rows AS (
    SELECT
      public.pay_payment_correction_items.pay_batch_item_id AS correction_pay_batch_item_id,
      public.pay_payment_correction_items.correction_item_kind AS correction_item_kind
    FROM public.pay_payment_correction_items
    WHERE public.pay_payment_correction_items.pay_batch_id = p_pay_batch_id
      AND public.pay_payment_correction_items.pay_batch_item_id IS NOT NULL
      AND public.pay_payment_correction_items.status = 'APPLIED'
  ),
  applied_corrections AS (
    SELECT
      applied_correction_rows.correction_pay_batch_item_id AS correction_pay_batch_item_id,
      array_agg(DISTINCT applied_correction_rows.correction_item_kind ORDER BY applied_correction_rows.correction_item_kind) AS applied_correction_kinds
    FROM applied_correction_rows
    GROUP BY applied_correction_rows.correction_pay_batch_item_id
  ),
  economic_components AS (
    SELECT
      economic_component_rows.pay_batch_id AS economic_pay_batch_id,
      economic_component_rows.pay_batch_item_id AS economic_pay_batch_item_id,
      economic_component_rows.timesheet_id AS economic_timesheet_id,
      economic_component_rows.item_type AS economic_item_type,
      economic_component_rows.key_type AS economic_key_type,
      economic_component_rows.key_value AS economic_key_value,
      economic_component_rows.source_amount_ex_vat AS economic_source_amount_ex_vat,
      economic_component_rows.source_amount_inc_vat AS economic_source_amount_inc_vat,
      economic_component_rows.target_amount_ex_vat AS economic_target_amount_ex_vat,
      economic_component_rows.key_resolution_source AS economic_key_resolution_source,
      economic_component_rows.key_resolution_failure_reason AS economic_key_resolution_failure_reason
    FROM public._pay_batch_item_economic_components(NULL::uuid, v_return_item_ids) AS economic_component_rows
  ),
  selected_batch_rows AS (
    SELECT
      public.pay_batch_candidates.pay_batch_id AS selected_pay_batch_id,
      public.pay_batch_candidates.id AS selected_pay_batch_candidate_id,
      public.pay_batch_candidates.candidate_id AS selected_candidate_id,
      COALESCE(
        NULLIF(btrim(public.pay_batch_candidates.candidate_display_name), ''),
        NULLIF(btrim(public.candidates.display_name), ''),
        NULLIF(btrim(concat_ws(' ', public.candidates.first_name, public.candidates.last_name)), ''),
        NULLIF(btrim(public.candidates.tms_ref), ''),
        public.pay_batch_candidates.candidate_id::text
      ) AS selected_candidate_display_name,
      COALESCE(
        NULLIF(btrim(public.pay_batch_candidates.candidate_tms_ref), ''),
        NULLIF(btrim(public.candidates.tms_ref), '')
      ) AS selected_candidate_tms_ref,
      public.pay_batch_items.id AS selected_pay_batch_item_id,
      public.pay_batch_items.item_type AS selected_item_type,
      public.pay_batch_items.timesheet_id AS selected_timesheet_id,
      public.pay_batch_items.pay_bank_transfer_id AS selected_pay_bank_transfer_id,
      public.pay_bank_transfers.status AS selected_transfer_status,
      public.pay_bank_transfers.amount AS selected_transfer_amount,
      public.pay_bank_transfers.transfer_group_key AS selected_transfer_group_key,
      public.pay_bank_transfers.payee_entity_kind AS selected_payee_entity_kind,
      public.pay_bank_transfers.payee_entity_id AS selected_payee_entity_id,
      COALESCE(
        public.pay_batch_items.umbrella_id,
        public.pay_bank_transfers.umbrella_id,
        CASE
          WHEN upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
            THEN public.pay_bank_transfers.payee_entity_id
          ELSE NULL::uuid
        END
      ) AS selected_umbrella_id,
      public.pay_batch_items.finance_case_id AS selected_finance_case_id,
      public.pay_batch_items.finance_component_id AS selected_finance_component_id,
      public.pay_batch_items.reservation_id AS selected_reservation_id,
      public.pay_batch_items.pay_channel AS selected_pay_channel,
      public.pay_batch_items.frozen_source_pay_method AS selected_frozen_source_pay_method,
      public.pay_batch_items.frozen_target_pay_method AS selected_frozen_target_pay_method,
      public.candidates.pay_method AS selected_current_candidate_pay_method,
      public.pay_batch_items.amount_ex_vat AS selected_amount_ex_vat,
      public.pay_batch_items.amount_vat AS selected_amount_vat,
      public.pay_batch_items.amount_inc_vat AS selected_amount_inc_vat,
      public.pay_batch_items.is_voided AS selected_is_voided,
      COALESCE(applied_corrections.applied_correction_kinds, ARRAY[]::text[]) AS selected_applied_correction_kinds
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    JOIN public.candidates
      ON public.candidates.id = public.pay_batch_candidates.candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    LEFT JOIN applied_corrections
      ON applied_corrections.correction_pay_batch_item_id = public.pay_batch_items.id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND public.pay_batch_items.id = ANY(v_return_item_ids)
  )
  SELECT
    selected_batch_rows.selected_pay_batch_id AS pay_batch_id,
    selected_batch_rows.selected_pay_batch_candidate_id AS pay_batch_candidate_id,
    selected_batch_rows.selected_candidate_id AS candidate_id,
    selected_batch_rows.selected_candidate_display_name AS candidate_display_name,
    selected_batch_rows.selected_candidate_tms_ref AS candidate_tms_ref,
    selected_batch_rows.selected_pay_batch_item_id AS pay_batch_item_id,
    selected_batch_rows.selected_item_type AS item_type,
    selected_batch_rows.selected_timesheet_id AS timesheet_id,
    selected_batch_rows.selected_pay_bank_transfer_id AS pay_bank_transfer_id,
    selected_batch_rows.selected_transfer_status AS transfer_status,
    selected_batch_rows.selected_transfer_amount AS transfer_amount,
    selected_batch_rows.selected_transfer_group_key AS transfer_group_key,
    selected_batch_rows.selected_payee_entity_kind AS payee_entity_kind,
    selected_batch_rows.selected_payee_entity_id AS payee_entity_id,
    selected_batch_rows.selected_umbrella_id AS umbrella_id,
    public.umbrellas.name AS umbrella_name,
    selected_batch_rows.selected_finance_case_id AS finance_case_id,
    selected_batch_rows.selected_finance_component_id AS finance_component_id,
    selected_batch_rows.selected_reservation_id AS reservation_id,
    selected_batch_rows.selected_pay_channel AS pay_channel,
    selected_batch_rows.selected_frozen_source_pay_method AS frozen_source_pay_method,
    selected_batch_rows.selected_frozen_target_pay_method AS frozen_target_pay_method,
    selected_batch_rows.selected_current_candidate_pay_method AS current_candidate_pay_method,
    economic_components.economic_key_type AS economic_key_type,
    economic_components.economic_key_value AS economic_key_value,
    economic_components.economic_source_amount_ex_vat AS source_amount_ex_vat,
    economic_components.economic_target_amount_ex_vat AS target_amount_ex_vat,
    COALESCE(economic_components.economic_key_resolution_source, 'KEY_RESOLUTION_FAILED') AS key_resolution_source,
    CASE
      WHEN economic_components.economic_pay_batch_item_id IS NULL THEN 'ECONOMIC_COMPONENT_ROW_NOT_RETURNED'
      ELSE economic_components.economic_key_resolution_failure_reason
    END AS key_resolution_failure_reason,
    selected_batch_rows.selected_amount_ex_vat AS amount_ex_vat,
    selected_batch_rows.selected_amount_vat AS amount_vat,
    economic_components.economic_source_amount_inc_vat AS amount_inc_vat,
    selected_batch_rows.selected_is_voided AS is_voided,
    COALESCE(array_length(selected_batch_rows.selected_applied_correction_kinds, 1), 0) > 0 AS already_corrected,
    selected_batch_rows.selected_applied_correction_kinds AS applied_correction_kinds
  FROM selected_batch_rows
  LEFT JOIN economic_components
    ON economic_components.economic_pay_batch_item_id = selected_batch_rows.selected_pay_batch_item_id
  LEFT JOIN public.umbrellas
    ON public.umbrellas.id = selected_batch_rows.selected_umbrella_id
  ORDER BY
    selected_batch_rows.selected_candidate_display_name,
    selected_batch_rows.selected_transfer_group_key NULLS LAST,
    selected_batch_rows.selected_pay_bank_transfer_id NULLS LAST,
    selected_batch_rows.selected_pay_batch_item_id;

  GET DIAGNOSTICS v_returned_count = ROW_COUNT;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_RETURNED',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_scope_type,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'selected_item_count', v_selected_item_count,
      'selected_already_corrected_count', v_selected_already_corrected_count,
      'return_item_count', v_return_item_count,
      'returned_row_count', v_returned_count,
      'key_resolution_failure_count', v_key_resolution_failure_count,
      'expected_item_count', v_expected_item_count,
      'expected_pay_batch_item_id_count', COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0),
      'work_unit', v_work_unit,
      'source_correction_request_id_present', v_source_correction_request_id_text IS NOT NULL,
      'explicit_item_ids_authoritative', COALESCE(v_explicit_item_ids_authoritative, false)
    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_CORRECTION_SELECTED_ITEMS_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'scope_type', v_scope_type,
        'include_already_corrected', COALESCE(p_include_already_corrected, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM,
        'selection_json', p_selection_json
      ),
      'pay_payment_correction',
      COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

ALTER FUNCTION public._pay_payment_correction_selected_items(uuid,jsonb,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_correction_selected_items(uuid,jsonb,boolean) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public._pay_payment_correction_selected_items(uuid,jsonb,boolean) TO postgres;
