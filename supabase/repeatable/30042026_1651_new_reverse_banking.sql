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
        COALESCE(array_length(v_finance_case_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_case_id = ANY(v_finance_case_ids)
      )
      AND (
        COALESCE(array_length(v_finance_component_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_component_id = ANY(v_finance_component_ids)
      )
      AND (
        COALESCE(array_length(v_reservation_ids, 1), 0) = 0
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
      'expected_pay_batch_item_id_count', COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0)
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





CREATE OR REPLACE FUNCTION public._pay_payment_movement_classify(
  p_pay_batch_id uuid,
  p_selection_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch public.pay_batches%rowtype;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_reasons jsonb := '[]'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_evidence jsonb := '{}'::jsonb;
  v_counts jsonb := '{}'::jsonb;
  v_selected_amounts jsonb := '{}'::jsonb;
  v_safe_to_auto_apply boolean := false;

  v_selected_item_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_umbrella_ids uuid[] := ARRAY[]::uuid[];

  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_voided_item_count integer := 0;
  v_already_corrected_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_selected_amount_ex_vat numeric := 0;
  v_selected_amount_vat numeric := 0;
  v_selected_amount_inc_vat numeric := 0;

  v_transfer_completed_count integer := 0;
  v_transfer_completed_at_count integer := 0;
  v_transfer_rail_tx_count integer := 0;
  v_transfer_request_ref_count integer := 0;
  v_transfer_rail_meta_count integer := 0;
  v_transfer_terminal_count integer := 0;
  v_transfer_pending_count integer := 0;
  v_transfer_unknown_count integer := 0;
  v_transfer_timeout_count integer := 0;
  v_transfer_returned_count integer := 0;

  v_candidate_settled_count integer := 0;
  v_candidate_settled_at_count integer := 0;
  v_timesheet_history_count integer := 0;
  v_reservation_settled_count integer := 0;
  v_reservation_settled_at_count integer := 0;
  v_payout_paid_count integer := 0;

  v_bank_event_count integer := 0;
  v_event_submitted_count integer := 0;
  v_event_completed_count integer := 0;
  v_event_terminal_no_money_count integer := 0;
  v_event_returned_count integer := 0;
  v_event_pending_count integer := 0;
  v_event_unknown_count integer := 0;
  v_event_ambiguous_mapping_count integer := 0;
  v_event_weak_mapping_count integer := 0;
  v_event_strong_matched_count integer := 0;
  v_event_amount_mismatch_count integer := 0;
  v_conflicting_bank_event_count integer := 0;
  v_provider_reference_count integer := 0;

  v_has_submission_evidence boolean := false;
  v_has_terminal_no_money_evidence boolean := false;
  v_has_returned_evidence boolean := false;
  v_has_settlement_evidence boolean := false;
  v_has_ambiguous_provider_state boolean := false;
  v_missing_provider_reference_after_submission boolean := false;

  v_aggregate_subset_issue_count integer := 0;
  v_aggregate_subset_json jsonb := '[]'::jsonb;

  v_selection_scope_type text := NULL;
  v_requested_action text := NULL;
  v_source_context text := NULL;
  v_result jsonb;
BEGIN
  v_selection_scope_type := upper(nullif(btrim(coalesce(p_selection_json->>'scope_type', '')), ''));
  v_requested_action := upper(nullif(btrim(coalesce(p_selection_json->>'requested_action', '')), ''));
  v_source_context := upper(nullif(btrim(coalesce(p_selection_json->>'source_context', '')), ''));

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_MOVEMENT_CLASSIFY_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_selection_scope_type,
      'requested_action', v_requested_action,
      'source_context', v_source_context,
      'selection_json', p_selection_json
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

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  WITH selected_items AS (
    SELECT selected_rows.*
    FROM public._pay_payment_correction_selected_items(
      p_pay_batch_id,
      p_selection_json,
      true
    ) AS selected_rows
  )
  SELECT
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_item_id) FILTER (WHERE selected_items.pay_batch_item_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.candidate_id) FILTER (WHERE selected_items.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_candidate_id) FILTER (WHERE selected_items.pay_batch_candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_bank_transfer_id) FILTER (WHERE selected_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.timesheet_id) FILTER (WHERE selected_items.timesheet_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.finance_case_id) FILTER (WHERE selected_items.finance_case_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.finance_component_id) FILTER (WHERE selected_items.finance_component_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.reservation_id) FILTER (WHERE selected_items.reservation_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.umbrella_id) FILTER (WHERE selected_items.umbrella_id IS NOT NULL), ARRAY[]::uuid[]),
    count(*)::integer,
    count(DISTINCT selected_items.candidate_id)::integer,
    count(DISTINCT selected_items.pay_bank_transfer_id)::integer,
    (count(*) FILTER (WHERE COALESCE(selected_items.is_voided, false)))::integer,
    (count(*) FILTER (WHERE COALESCE(selected_items.already_corrected, false)))::integer,
    (count(*) FILTER (WHERE selected_items.key_resolution_failure_reason IS NOT NULL))::integer,
    COALESCE(sum(COALESCE(selected_items.amount_ex_vat, 0)), 0),
    COALESCE(sum(COALESCE(selected_items.amount_vat, 0)), 0),
    COALESCE(sum(COALESCE(selected_items.amount_inc_vat, 0)), 0)
  INTO
    v_selected_item_ids,
    v_candidate_ids,
    v_pay_batch_candidate_ids,
    v_transfer_ids,
    v_timesheet_ids,
    v_finance_case_ids,
    v_finance_component_ids,
    v_reservation_ids,
    v_umbrella_ids,
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_voided_item_count,
    v_already_corrected_count,
    v_key_resolution_failure_count,
    v_selected_amount_ex_vat,
    v_selected_amount_vat,
    v_selected_amount_inc_vat
  FROM selected_items;

  IF v_selected_item_count = 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'NO_SELECTED_PAYMENT_ITEMS',
      'message', 'No pay_batch_items were resolved for the selected correction scope.'
    ));
  END IF;

  IF v_key_resolution_failure_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'KEY_RESOLUTION_FAILURE',
      'message', 'One or more selected frozen batch items could not be resolved into the economic keyspace.',
      'count', v_key_resolution_failure_count
    ));
  END IF;

  IF v_already_corrected_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SELECTED_ITEMS_ALREADY_CORRECTED',
      'message', 'One or more selected batch items already have an applied correction ledger row.',
      'count', v_already_corrected_count
    ));
  END IF;

  IF v_voided_item_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SELECTED_ITEMS_VOIDED',
      'message', 'One or more selected batch items are already voided.',
      'count', v_voided_item_count
    ));
  END IF;

  SELECT
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) = 'COMPLETED'
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) = 'COMPLETED'
    ))::integer,
    (count(*) FILTER (WHERE public.pay_bank_transfers.completed_at_utc IS NOT NULL))::integer,
    (count(*) FILTER (WHERE nullif(btrim(coalesce(public.pay_bank_transfers.rail_tx_id, '')), '') IS NOT NULL))::integer,
    (count(*) FILTER (WHERE nullif(btrim(coalesce(public.pay_bank_transfers.request_id, '')), '') IS NOT NULL))::integer,
    (count(*) FILTER (WHERE public.pay_bank_transfers.rail_meta_json IS NOT NULL AND public.pay_bank_transfers.rail_meta_json <> '{}'::jsonb))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED')
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED')
         OR nullif(btrim(coalesce(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'WAITING_BANK_CONFIRM')
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'WAITING_BANK_CONFIRM')
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) = 'UNKNOWN'
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) = 'UNKNOWN'
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT')
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT')
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('RETURNED', 'REVERTED')
         OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('RETURNED', 'REVERTED')
    ))::integer
  INTO
    v_transfer_completed_count,
    v_transfer_completed_at_count,
    v_transfer_rail_tx_count,
    v_transfer_request_ref_count,
    v_transfer_rail_meta_count,
    v_transfer_terminal_count,
    v_transfer_pending_count,
    v_transfer_unknown_count,
    v_transfer_timeout_count,
    v_transfer_returned_count
  FROM public.pay_bank_transfers
  WHERE public.pay_bank_transfers.pay_batch_id = p_pay_batch_id
    AND public.pay_bank_transfers.id = ANY(v_transfer_ids);

  SELECT
    (count(*) FILTER (WHERE upper(coalesce(public.pay_batch_candidates.settlement_status, '')) = 'SETTLED'))::integer,
    (count(*) FILTER (WHERE public.pay_batch_candidates.settled_at_utc IS NOT NULL))::integer
  INTO
    v_candidate_settled_count,
    v_candidate_settled_at_count
  FROM public.pay_batch_candidates
  WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
    AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids);

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) > 0 THEN
    SELECT count(*)::integer
    INTO v_timesheet_history_count
    FROM public.timesheet_pay_state_history
    WHERE public.timesheet_pay_state_history.pay_batch_id = p_pay_batch_id
      AND public.timesheet_pay_state_history.timesheet_id = ANY(v_timesheet_ids);
  ELSE
    v_timesheet_history_count := 0;
  END IF;

  IF COALESCE(array_length(v_reservation_ids, 1), 0) > 0 THEN
    SELECT
      (count(*) FILTER (WHERE upper(coalesce(public.pay_advance_reservations.status, '')) = 'SETTLED'))::integer,
      (count(*) FILTER (WHERE public.pay_advance_reservations.settled_at_utc IS NOT NULL))::integer
    INTO
      v_reservation_settled_count,
      v_reservation_settled_at_count
    FROM public.pay_advance_reservations
    WHERE public.pay_advance_reservations.id = ANY(v_reservation_ids);
  ELSE
    v_reservation_settled_count := 0;
    v_reservation_settled_at_count := 0;
  END IF;

  IF COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    SELECT count(*)::integer
    INTO v_payout_paid_count
    FROM public.pay_advances
    WHERE public.pay_advances.id = ANY(v_finance_case_ids)
      AND upper(coalesce(public.pay_advances.payout_status::text, '')) = 'PAID'
      AND (
        public.pay_advances.payout_pay_batch_id = p_pay_batch_id
        OR (
          COALESCE(array_length(v_transfer_ids, 1), 0) > 0
          AND public.pay_advances.payout_transfer_id = ANY(v_transfer_ids)
        )
      );
  ELSE
    v_payout_paid_count := 0;
  END IF;

  SELECT
    count(*)::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) = 'SUBMITTED'))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) = 'COMPLETED'))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED')))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('RETURNED', 'REVERTED')))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('PENDING', 'PROCESSING')))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) = 'UNKNOWN' OR upper(coalesce(public.pay_bank_transfer_events.provider_state, '')) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT')))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfer_events.mapping_status, '')) IN ('AMBIGUOUS', 'UNMATCHED', 'LEGACY_NO_ARTIFACT')
         OR upper(coalesce(public.pay_bank_transfer_events.mapping_method, '')) IN ('AMBIGUOUS', 'UNMATCHED', 'LEGACY_NO_ARTIFACT')
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfer_events.mapping_method, '')) = 'AMOUNT_ONLY_UNIQUE'
    ))::integer,
    (count(*) FILTER (
      WHERE upper(coalesce(public.pay_bank_transfer_events.mapping_status, '')) = 'MATCHED'
        AND upper(coalesce(public.pay_bank_transfer_events.mapping_method, '')) IN (
          'TRANSFER_ID',
          'PROVIDER_EVENT_ID',
          'PROVIDER_REFERENCE',
          'REQUEST_ID',
          'RAIL_TX_ID',
          'PAYMENT_REFERENCE',
          'MANUAL_TRANSFER_SELECTION'
        )
    ))::integer,
    (count(*) FILTER (
      WHERE public.pay_bank_transfer_events.amount IS NOT NULL
        AND public.pay_bank_transfer_events.pay_bank_transfer_id IS NOT NULL
        AND public.pay_bank_transfers.id IS NOT NULL
        AND abs(COALESCE(public.pay_bank_transfer_events.amount, 0) - COALESCE(public.pay_bank_transfers.amount, 0)) > 0.01
    ))::integer,
    (count(*) FILTER (
      WHERE nullif(btrim(coalesce(public.pay_bank_transfer_events.provider_reference, '')), '') IS NOT NULL
         OR nullif(btrim(coalesce(public.pay_bank_transfer_events.provider_event_id, '')), '') IS NOT NULL
    ))::integer
  INTO
    v_bank_event_count,
    v_event_submitted_count,
    v_event_completed_count,
    v_event_terminal_no_money_count,
    v_event_returned_count,
    v_event_pending_count,
    v_event_unknown_count,
    v_event_ambiguous_mapping_count,
    v_event_weak_mapping_count,
    v_event_strong_matched_count,
    v_event_amount_mismatch_count,
    v_provider_reference_count
  FROM public.pay_bank_transfer_events
  LEFT JOIN public.pay_bank_transfers
    ON public.pay_bank_transfers.id = public.pay_bank_transfer_events.pay_bank_transfer_id
  WHERE public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id
    AND (
      (
        COALESCE(array_length(v_transfer_ids, 1), 0) > 0
        AND public.pay_bank_transfer_events.pay_bank_transfer_id = ANY(v_transfer_ids)
      )
      OR (
        COALESCE(array_length(v_candidate_ids, 1), 0) > 0
        AND public.pay_bank_transfer_events.candidate_id = ANY(v_candidate_ids)
      )
      OR (
        COALESCE(array_length(v_umbrella_ids, 1), 0) > 0
        AND public.pay_bank_transfer_events.umbrella_id = ANY(v_umbrella_ids)
      )
      OR (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        AND COALESCE(array_length(v_candidate_ids, 1), 0) = 0
        AND COALESCE(array_length(v_umbrella_ids, 1), 0) = 0
      )
    );

  WITH selected_transfer_candidates AS (
    SELECT
      public.pay_batch_items.pay_bank_transfer_id AS selected_transfer_id,
      count(DISTINCT public.pay_batch_candidates.candidate_id)::integer AS selected_candidate_count
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND public.pay_batch_items.id = ANY(v_selected_item_ids)
      AND public.pay_batch_items.pay_bank_transfer_id IS NOT NULL
    GROUP BY public.pay_batch_items.pay_bank_transfer_id
  ),
  all_transfer_candidates AS (
    SELECT
      public.pay_batch_items.pay_bank_transfer_id AS all_transfer_id,
      count(DISTINCT public.pay_batch_candidates.candidate_id)::integer AS all_candidate_count
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_transfer_ids)
    GROUP BY public.pay_batch_items.pay_bank_transfer_id
  ),
  affected_transfer_evidence AS (
    SELECT
      public.pay_bank_transfers.id AS transfer_id,
      public.pay_bank_transfers.amount AS transfer_amount,
      public.pay_bank_transfers.umbrella_id AS transfer_umbrella_id,
      public.pay_bank_transfers.payee_entity_kind AS payee_entity_kind,
      public.pay_bank_transfers.status AS transfer_status,
      bool_or(upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED')) AS has_failed_event,
      bool_or(upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('RETURNED', 'REVERTED')) AS has_return_event,
      max(public.pay_bank_transfer_events.amount) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('RETURNED', 'REVERTED')) AS return_event_amount
    FROM public.pay_bank_transfers
    LEFT JOIN public.pay_bank_transfer_events
      ON public.pay_bank_transfer_events.pay_bank_transfer_id = public.pay_bank_transfers.id
    WHERE public.pay_bank_transfers.pay_batch_id = p_pay_batch_id
      AND public.pay_bank_transfers.id = ANY(v_transfer_ids)
    GROUP BY
      public.pay_bank_transfers.id,
      public.pay_bank_transfers.amount,
      public.pay_bank_transfers.umbrella_id,
      public.pay_bank_transfers.payee_entity_kind,
      public.pay_bank_transfers.status
  ),
  subset_issues AS (
    SELECT
      affected_transfer_evidence.transfer_id AS transfer_id,
      affected_transfer_evidence.transfer_amount AS transfer_amount,
      selected_transfer_candidates.selected_candidate_count AS selected_candidate_count,
      all_transfer_candidates.all_candidate_count AS all_candidate_count,
      affected_transfer_evidence.has_failed_event AS has_failed_event,
      affected_transfer_evidence.has_return_event AS has_return_event,
      affected_transfer_evidence.return_event_amount AS return_event_amount
    FROM affected_transfer_evidence
    JOIN selected_transfer_candidates
      ON selected_transfer_candidates.selected_transfer_id = affected_transfer_evidence.transfer_id
    JOIN all_transfer_candidates
      ON all_transfer_candidates.all_transfer_id = affected_transfer_evidence.transfer_id
    WHERE all_transfer_candidates.all_candidate_count > 1
      AND selected_transfer_candidates.selected_candidate_count < all_transfer_candidates.all_candidate_count
      AND (
        affected_transfer_evidence.has_failed_event
        OR upper(coalesce(affected_transfer_evidence.transfer_status, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED')
        OR affected_transfer_evidence.has_return_event
        OR (
          affected_transfer_evidence.return_event_amount IS NOT NULL
          AND abs(COALESCE(affected_transfer_evidence.return_event_amount, 0) - COALESCE(affected_transfer_evidence.transfer_amount, 0)) <= 0.01
        )
      )
  )
  SELECT
    count(*)::integer,
    COALESCE(jsonb_agg(jsonb_build_object(
      'pay_bank_transfer_id', subset_issues.transfer_id,
      'transfer_amount', subset_issues.transfer_amount,
      'selected_candidate_count', subset_issues.selected_candidate_count,
      'all_candidate_count', subset_issues.all_candidate_count,
      'has_failed_event', subset_issues.has_failed_event,
      'has_return_event', subset_issues.has_return_event,
      'return_event_amount', subset_issues.return_event_amount
    ) ORDER BY subset_issues.transfer_id), '[]'::jsonb)
  INTO
    v_aggregate_subset_issue_count,
    v_aggregate_subset_json
  FROM subset_issues;

  v_has_submission_evidence :=
    COALESCE(upper(coalesce(v_batch.execution_commit_state, 'NOT_SUBMITTED')) <> 'NOT_SUBMITTED', false)
    OR v_batch.execution_commit_ref IS NOT NULL
    OR v_batch.execution_committed_at_utc IS NOT NULL
    OR v_transfer_rail_tx_count > 0
    OR v_transfer_request_ref_count > 0
    OR v_transfer_rail_meta_count > 0
    OR v_event_submitted_count > 0
    OR v_event_completed_count > 0
    OR v_event_terminal_no_money_count > 0
    OR v_event_returned_count > 0
    OR v_bank_event_count > 0;

  v_has_terminal_no_money_evidence :=
    v_transfer_terminal_count > 0
    OR v_event_terminal_no_money_count > 0;

  v_has_returned_evidence :=
    v_transfer_returned_count > 0
    OR v_event_returned_count > 0;

  v_has_settlement_evidence :=
    v_transfer_completed_count > 0
    OR v_transfer_completed_at_count > 0
    OR v_event_completed_count > 0
    OR v_candidate_settled_count > 0
    OR v_candidate_settled_at_count > 0
    OR v_timesheet_history_count > 0
    OR v_reservation_settled_count > 0
    OR v_reservation_settled_at_count > 0
    OR v_payout_paid_count > 0;

  v_has_ambiguous_provider_state :=
    v_has_submission_evidence
    AND (
      v_transfer_pending_count > 0
      OR v_transfer_unknown_count > 0
      OR v_transfer_timeout_count > 0
      OR v_event_pending_count > 0
      OR v_event_unknown_count > 0
    );

  v_missing_provider_reference_after_submission :=
    v_has_submission_evidence
    AND v_transfer_rail_tx_count = 0
    AND v_transfer_request_ref_count = 0
    AND v_transfer_rail_meta_count = 0
    AND v_provider_reference_count = 0
    AND v_batch.execution_commit_ref IS NULL;

  IF v_has_ambiguous_provider_state THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'AMBIGUOUS_PROVIDER_STATE',
      'message', 'Provider or transfer evidence is pending, processing, unknown, or timed out.'
    ));
  END IF;

  IF v_event_ambiguous_mapping_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'AMBIGUOUS_PROVIDER_MAPPING',
      'message', 'One or more bank events have ambiguous, unmatched, or legacy no-artifact mapping.',
      'count', v_event_ambiguous_mapping_count
    ));
  END IF;

  IF v_event_weak_mapping_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'WEAK_PROVIDER_MAPPING',
      'message', 'One or more bank events were matched only by an amount-only or otherwise weak mapping method and require manual review.',
      'count', v_event_weak_mapping_count
    ));
  END IF;

  IF v_event_amount_mismatch_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'BANK_EVENT_AMOUNT_MISMATCH',
      'message', 'One or more mapped bank events have an amount mismatch against the selected transfer.',
      'count', v_event_amount_mismatch_count
    ));
  END IF;

  IF v_missing_provider_reference_after_submission THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'MISSING_PROVIDER_REFERENCE_AFTER_SUBMISSION',
      'message', 'The batch appears submitted or committed, but no durable provider transfer reference was found for the selected scope.'
    ));
  END IF;

  IF v_aggregate_subset_issue_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'AGGREGATE_UMBRELLA_TRANSFER_SUBSET_SELECTED',
      'message', 'The selected scope is only part of an aggregate transfer with failed or returned bank evidence. Correction must normally be applied to the whole transfer unless separately authorised with manual evidence.',
      'transfers', v_aggregate_subset_json
    ));
  END IF;

  IF v_has_settlement_evidence AND v_has_terminal_no_money_evidence AND NOT v_has_returned_evidence THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CONFLICTING_SETTLED_AND_FAILED_EVIDENCE',
      'message', 'Selected scope has settlement evidence and failed no-money evidence without a clear return event.'
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_conflicting_bank_event_count
  FROM (
    SELECT public.pay_bank_transfer_events.pay_bank_transfer_id AS grouped_transfer_id
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id
      AND public.pay_bank_transfer_events.pay_bank_transfer_id = ANY(v_transfer_ids)
    GROUP BY public.pay_bank_transfer_events.pay_bank_transfer_id
    HAVING bool_or(upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) = 'COMPLETED')
       AND bool_or(upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED'))
       AND NOT bool_or(upper(coalesce(public.pay_bank_transfer_events.normalised_state, '')) IN ('RETURNED', 'REVERTED'))
  ) AS conflicting_bank_event_groups;

  IF v_conflicting_bank_event_count > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CONFLICTING_BANK_EVENTS',
      'message', 'One or more selected transfers have conflicting completed and failed bank events.',
      'count', v_conflicting_bank_event_count
    ));
  END IF;

  IF v_has_settlement_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('SETTLEMENT_EVIDENCE_PRESENT');
  END IF;

  IF v_has_returned_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('RETURNED_OR_REVERTED_EVIDENCE_PRESENT');
  END IF;

  IF v_has_terminal_no_money_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('TERMINAL_NO_MONEY_FAILURE_EVIDENCE_PRESENT');
  END IF;

  IF NOT v_has_submission_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('NO_BANK_SUBMISSION_EVIDENCE_FOUND');
  END IF;

  IF jsonb_array_length(v_blockers) > 0 THEN
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
  ELSIF NOT v_has_submission_evidence THEN
    v_classification := 'PRE_BANK_CANCEL';
  ELSIF v_has_settlement_evidence
        AND (
          v_has_returned_evidence
          OR v_requested_action = 'REVERSE_SETTLED_PAYMENT'
        ) THEN
    v_classification := 'TRUE_SETTLED_REVERSAL_REQUIRED';
  ELSIF v_has_terminal_no_money_evidence
        AND NOT v_has_settlement_evidence THEN
    v_classification := 'NO_MONEY_UNWIND';
  ELSE
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';

    IF v_has_settlement_evidence THEN
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'SETTLED_PAYMENT_NO_RETURN_OR_REVERSAL_ACTION',
        'message', 'The selected scope has settlement evidence, but no returned/reverted evidence and no explicit settled reversal action was requested.'
      ));
    ELSE
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'SUBMITTED_BUT_NOT_TERMINAL_OR_SETTLED',
        'message', 'Selected scope has submission evidence but no clear terminal failure or settlement/return evidence.'
      ));
    END IF;
  END IF;

  v_safe_to_auto_apply :=
    jsonb_array_length(v_blockers) = 0
    AND (
      v_classification = 'NO_MONEY_UNWIND'
      OR (
        v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED'
        AND v_has_returned_evidence
      )
    )
    AND v_selected_item_count > 0
    AND v_already_corrected_count = 0
    AND v_key_resolution_failure_count = 0
    AND v_voided_item_count = 0
    AND v_event_amount_mismatch_count = 0
    AND v_event_weak_mapping_count = 0
    AND v_event_ambiguous_mapping_count = 0
    AND v_event_strong_matched_count > 0
    AND NOT (
      v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED'
      AND v_requested_action = 'REVERSE_SETTLED_PAYMENT'
      AND COALESCE(v_source_context, '') <> 'BANK_EVENT_INGEST'
    );

  v_counts := jsonb_build_object(
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'already_corrected_count', v_already_corrected_count,
    'key_resolution_failure_count', v_key_resolution_failure_count,
    'transfer_completed_count', v_transfer_completed_count,
    'transfer_completed_at_count', v_transfer_completed_at_count,
    'transfer_rail_tx_count', v_transfer_rail_tx_count,
    'transfer_request_ref_count', v_transfer_request_ref_count,
    'transfer_rail_meta_count', v_transfer_rail_meta_count,
    'transfer_terminal_count', v_transfer_terminal_count,
    'transfer_pending_count', v_transfer_pending_count,
    'transfer_unknown_count', v_transfer_unknown_count,
    'transfer_timeout_count', v_transfer_timeout_count,
    'transfer_returned_count', v_transfer_returned_count,
    'candidate_settled_count', v_candidate_settled_count,
    'candidate_settled_at_count', v_candidate_settled_at_count,
    'timesheet_pay_state_history_count', v_timesheet_history_count,
    'reservation_settled_count', v_reservation_settled_count,
    'reservation_settled_at_count', v_reservation_settled_at_count,
    'payout_paid_count', v_payout_paid_count,
    'bank_event_count', v_bank_event_count,
    'bank_event_ambiguous_mapping_count', v_event_ambiguous_mapping_count,
    'bank_event_weak_mapping_count', v_event_weak_mapping_count,
    'bank_event_strong_matched_count', v_event_strong_matched_count,
    'bank_event_amount_mismatch_count', v_event_amount_mismatch_count,
    'aggregate_subset_issue_count', v_aggregate_subset_issue_count
  );

  v_selected_amounts := jsonb_build_object(
    'amount_ex_vat', v_selected_amount_ex_vat,
    'amount_vat', v_selected_amount_vat,
    'amount_inc_vat', v_selected_amount_inc_vat
  );

  v_evidence := jsonb_build_object(
    'requested_action', v_requested_action,
    'source_context', v_source_context,
    'batch', jsonb_build_object(
      'status', v_batch.status,
      'execution_commit_state', v_batch.execution_commit_state,
      'execution_commit_ref', v_batch.execution_commit_ref,
      'execution_committed_at_utc', v_batch.execution_committed_at_utc
    ),
    'submission', jsonb_build_object(
      'has_submission_evidence', v_has_submission_evidence,
      'missing_provider_reference_after_submission', v_missing_provider_reference_after_submission
    ),
    'settlement', jsonb_build_object(
      'has_settlement_evidence', v_has_settlement_evidence,
      'transfer_completed_count', v_transfer_completed_count,
      'transfer_completed_at_count', v_transfer_completed_at_count,
      'transfer_rail_meta_count', v_transfer_rail_meta_count,
      'candidate_settled_count', v_candidate_settled_count,
      'candidate_settled_at_count', v_candidate_settled_at_count,
      'timesheet_pay_state_history_count', v_timesheet_history_count,
      'reservation_settled_count', v_reservation_settled_count,
      'reservation_settled_at_count', v_reservation_settled_at_count,
      'payout_paid_count', v_payout_paid_count
    ),
    'failure', jsonb_build_object(
      'has_terminal_no_money_evidence', v_has_terminal_no_money_evidence,
      'has_returned_evidence', v_has_returned_evidence,
      'transfer_terminal_count', v_transfer_terminal_count,
      'bank_event_terminal_no_money_count', v_event_terminal_no_money_count,
      'bank_event_returned_count', v_event_returned_count
    ),
    'provider_events', jsonb_build_object(
      'bank_event_count', v_bank_event_count,
      'submitted_count', v_event_submitted_count,
      'completed_count', v_event_completed_count,
      'pending_count', v_event_pending_count,
      'transfer_timeout_count', v_transfer_timeout_count,
      'unknown_count', v_event_unknown_count,
      'ambiguous_mapping_count', v_event_ambiguous_mapping_count,
      'weak_mapping_count', v_event_weak_mapping_count,
      'strong_matched_count', v_event_strong_matched_count,
      'amount_mismatch_count', v_event_amount_mismatch_count,
      'provider_reference_count', v_provider_reference_count
    ),
    'aggregate_transfer_subset', v_aggregate_subset_json
  );

  v_result := jsonb_build_object(
    'classification', v_classification,
    'reasons', v_reasons,
    'evidence', v_evidence,
    'counts', v_counts,
    'blockers', v_blockers,
    'selected_amounts', v_selected_amounts,
    'safe_to_auto_apply', v_safe_to_auto_apply
  );

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_MOVEMENT_CLASSIFY_RESULT',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_selection_scope_type,
      'requested_action', v_requested_action,
      'source_context', v_source_context,
      'classification', v_classification,
      'safe_to_auto_apply', v_safe_to_auto_apply,
      'counts', v_counts,
      'blocker_count', jsonb_array_length(v_blockers)
    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_CORRECTION_MOVEMENT_CLASSIFY_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'scope_type', v_selection_scope_type,
        'requested_action', v_requested_action,
        'source_context', v_source_context,
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


CREATE OR REPLACE FUNCTION public.pay_payment_correction_plan(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_id uuid;
  v_batch_status text;
  v_batch_pay_date date;
  v_batch_authoritative_payment_date date;
  v_effective_pay_date date;
  v_batch_created_at_utc timestamptz;
  v_batch_execution_commit_state text;
  v_batch_execution_commit_ref text;
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_recommended_action text := 'REVIEW_BANK_EVIDENCE';
  v_hard_blockers jsonb := '[]'::jsonb;
  v_warnings jsonb := '[]'::jsonb;
  v_selection_summary jsonb := '{}'::jsonb;
  v_affected_candidates jsonb := '[]'::jsonb;
  v_affected_transfers jsonb := '[]'::jsonb;
  v_affected_umbrellas jsonb := '[]'::jsonb;
  v_affected_items jsonb := '[]'::jsonb;
  v_affected_finance_cases jsonb := '[]'::jsonb;
  v_communication_effects jsonb := '{}'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_suggested_resolution jsonb := NULL::jsonb;
  v_amounts jsonb := '{}'::jsonb;
  v_draft_interference jsonb := '[]'::jsonb;
  v_large_correction jsonb := '{}'::jsonb;
  v_work_expansion_plan jsonb := '{}'::jsonb;
  v_can_apply boolean := false;

  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_umbrella_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_already_corrected_count integer := 0;
  v_voided_count integer := 0;
  v_pay_channel_change_count integer := 0;
  v_umbrella_change_count integer := 0;
  v_timesheet_item_count integer := 0;
  v_net_fixed_finance_item_count integer := 0;
  v_gross_channel_sensitive_item_count integer := 0;
  v_work_item_count integer := 0;
  v_large_correction_threshold integer := 100;
  v_recommended_chunk_size integer := 50;
  v_total_amount_ex_vat numeric := 0;
  v_total_amount_vat numeric := 0;
  v_total_amount_inc_vat numeric := 0;
  v_queued_unsent_count integer := 0;
  v_sent_notice_count integer := 0;
  v_subject_id text;
  v_scope_type text;
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_selection_hash text;
  v_selection_filters_applied jsonb := '{}'::jsonb;
  v_suggested_resolution_finance_cases jsonb := '[]'::jsonb;
  v_finance_case_record record;
  v_case_component_ids jsonb := '[]'::jsonb;
  v_case_component_fingerprints jsonb := '{}'::jsonb;
  v_case_suggestion jsonb := NULL::jsonb;
  v_case_suggestion_hash text := NULL::text;
  v_case_generation_error jsonb := NULL::jsonb;
  v_selected_mail_scope_json jsonb := '{}'::jsonb;
  v_mail_legacy_review_count integer := 0;
  v_mail_legacy_queued_review_count integer := 0;
BEGIN
  v_subject_id := COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID');
  v_scope_type := upper(nullif(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PLAN_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'selection_scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
      'actor_user_id', p_actor_user_id,
      'selection_json', p_selection_json
    ),
    'pay_payment_correction',
    v_subject_id,
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

  SELECT
    public.pay_batches.id,
    public.pay_batches.status,
    public.pay_batches.pay_date,
    public.pay_batches.authoritative_payment_date,
    public.pay_batches.created_at_utc,
    public.pay_batches.execution_commit_state,
    public.pay_batches.execution_commit_ref
  INTO
    v_batch_id,
    v_batch_status,
    v_batch_pay_date,
    v_batch_authoritative_payment_date,
    v_batch_created_at_utc,
    v_batch_execution_commit_state,
    v_batch_execution_commit_ref
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF v_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_effective_pay_date := COALESCE(v_batch_authoritative_payment_date, v_batch_pay_date);

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_selected;
  CREATE TEMP TABLE _tmp_payment_correction_plan_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.candidate_display_name,
    selected_rows.candidate_tms_ref,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_status,
    selected_rows.transfer_amount,
    selected_rows.transfer_group_key,
    selected_rows.payee_entity_kind,
    selected_rows.payee_entity_id,
    selected_rows.umbrella_id,
    selected_rows.umbrella_name,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_rows.pay_channel,
    selected_rows.frozen_source_pay_method,
    selected_rows.frozen_target_pay_method,
    selected_rows.current_candidate_pay_method,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.target_amount_ex_vat,
    selected_rows.key_resolution_source,
    selected_rows.key_resolution_failure_reason,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.applied_correction_kinds
  FROM public._pay_payment_correction_selected_items(
    p_pay_batch_id,
    p_selection_json,
    true
  ) AS selected_rows;

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (finance_case_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (finance_component_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (timesheet_id, economic_key_type, economic_key_value);

  SELECT
    COALESCE(array_agg(plan_selected.pay_batch_item_id ORDER BY plan_selected.pay_batch_item_id), ARRAY[]::uuid[]),
    md5(COALESCE(string_agg(plan_selected.pay_batch_item_id::text, ',' ORDER BY plan_selected.pay_batch_item_id::text), 'NO_SELECTED_ITEMS'))
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_selection_hash
  FROM pg_temp._tmp_payment_correction_plan_selected AS plan_selected;

  v_selection_filters_applied := jsonb_build_object(
    'scope_type', v_scope_type,
    'pay_batch_item_id_supplied', p_selection_json ? 'pay_batch_item_id',
    'pay_batch_item_ids_supplied', p_selection_json ? 'pay_batch_item_ids',
    'pay_bank_transfer_id_supplied', p_selection_json ? 'pay_bank_transfer_id',
    'pay_bank_transfer_ids_supplied', p_selection_json ? 'pay_bank_transfer_ids',
    'finance_case_id_supplied', p_selection_json ? 'finance_case_id',
    'finance_case_ids_supplied', p_selection_json ? 'finance_case_ids',
    'finance_component_id_supplied', p_selection_json ? 'finance_component_id',
    'finance_component_ids_supplied', p_selection_json ? 'finance_component_ids',
    'reservation_id_supplied', p_selection_json ? 'reservation_id',
    'reservation_ids_supplied', p_selection_json ? 'reservation_ids',
    'item_type_supplied', p_selection_json ? 'item_type',
    'item_types_supplied', p_selection_json ? 'item_types',
    'expected_item_count_supplied', p_selection_json ? 'expected_item_count',
    'expected_pay_batch_item_ids_supplied', p_selection_json ? 'expected_pay_batch_item_ids'
  );

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_detail;
  CREATE TEMP TABLE _tmp_payment_correction_plan_detail ON COMMIT DROP AS
  SELECT
    plan_selected.pay_batch_id,
    plan_selected.pay_batch_candidate_id,
    plan_selected.candidate_id,
    plan_selected.candidate_display_name,
    plan_selected.candidate_tms_ref,
    plan_selected.pay_batch_item_id,
    plan_selected.item_type,
    plan_selected.timesheet_id,
    plan_selected.pay_bank_transfer_id,
    plan_selected.transfer_status,
    plan_selected.transfer_amount,
    plan_selected.transfer_group_key,
    plan_selected.payee_entity_kind,
    plan_selected.payee_entity_id,
    plan_selected.umbrella_id,
    plan_selected.umbrella_name,
    plan_selected.finance_case_id,
    plan_selected.finance_component_id,
    plan_selected.reservation_id,
    plan_selected.pay_channel,
    plan_selected.frozen_source_pay_method,
    plan_selected.frozen_target_pay_method,
    plan_selected.current_candidate_pay_method,
    plan_selected.economic_key_type,
    plan_selected.economic_key_value,
    plan_selected.source_amount_ex_vat,
    plan_selected.target_amount_ex_vat,
    plan_selected.key_resolution_source,
    plan_selected.key_resolution_failure_reason,
    plan_selected.amount_ex_vat,
    plan_selected.amount_vat,
    plan_selected.amount_inc_vat,
    plan_selected.is_voided,
    plan_selected.already_corrected,
    plan_selected.applied_correction_kinds,
    public.candidates.pay_method AS live_candidate_pay_method,
    public.candidates.umbrella_id AS live_candidate_umbrella_id,
    public.pay_batch_items.frozen_component_classification::text AS frozen_component_classification,
    public.pay_batch_items.frozen_resolution_mode::text AS frozen_resolution_mode,
    public.pay_batch_items.frozen_resolution_payload_json AS frozen_resolution_payload_json,
    public.pay_batch_items.frozen_resolution_result_json AS frozen_resolution_result_json,
    public.pay_batch_items.payout_instruction_snapshot_json AS payout_instruction_snapshot_json,
    public.pay_finance_case_components.classification::text AS finance_component_classification,
    public.pay_finance_case_components.source_pay_method AS finance_component_source_pay_method,
    public.pay_finance_case_components.saved_target_pay_method AS finance_component_saved_target_pay_method,
    public.pay_finance_case_components.saved_resolution_mode::text AS finance_component_saved_resolution_mode,
    public.pay_finance_case_components.is_resolution_stale AS finance_component_is_resolution_stale,
    public.pay_finance_case_components.stale_reason AS finance_component_stale_reason,
    public.pay_finance_case_components.closed_at_utc AS finance_component_closed_at_utc,
    public.pay_advances.case_type::text AS finance_case_type,
    public.pay_advances.advance_kind::text AS finance_advance_kind,
    public.pay_advances.status::text AS finance_case_status,
    public.pay_advances.payout_status::text AS finance_payout_status,
    public.pay_advances.taxability::text AS finance_taxability,
    public.pay_advances.routing_kind::text AS finance_routing_kind,
    public.pay_advances.written_off_at_utc AS finance_written_off_at_utc,
    public.pay_advances.cleared_at_utc AS finance_cleared_at_utc
  FROM pg_temp._tmp_payment_correction_plan_selected AS plan_selected
  JOIN public.pay_batch_items
    ON public.pay_batch_items.id = plan_selected.pay_batch_item_id
  JOIN public.candidates
    ON public.candidates.id = plan_selected.candidate_id
  LEFT JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = plan_selected.finance_component_id
  LEFT JOIN public.pay_advances
    ON public.pay_advances.id = plan_selected.finance_case_id;

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (finance_case_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (finance_component_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (reservation_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (timesheet_id, economic_key_type, economic_key_value);

  v_classification_result := public._pay_payment_movement_classify(
    p_pay_batch_id,
    p_selection_json
  );

  v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
  v_hard_blockers := COALESCE(v_classification_result->'blockers', '[]'::jsonb);

  v_recommended_action := CASE v_classification
    WHEN 'PRE_BANK_CANCEL' THEN 'CANCEL_PAYMENT_ATTEMPT'
    WHEN 'NO_MONEY_UNWIND' THEN 'UNWIND_FAILED_PAYMENT'
    WHEN 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'REVERSE_SETTLED_PAYMENT'
    ELSE 'REVIEW_BANK_EVIDENCE'
  END;

  IF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
     AND jsonb_array_length(v_hard_blockers) = 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array('CLASSIFICATION_AMBIGUOUS_REVIEW_REQUIRED');
  END IF;

  SELECT
    count(*)::integer,
    count(DISTINCT plan_detail.candidate_id)::integer,
    count(DISTINCT plan_detail.pay_bank_transfer_id) FILTER (WHERE plan_detail.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT plan_detail.umbrella_id) FILTER (WHERE plan_detail.umbrella_id IS NOT NULL)::integer,
    count(*) FILTER (WHERE plan_detail.key_resolution_failure_reason IS NOT NULL OR plan_detail.economic_key_type IS NULL OR plan_detail.economic_key_value IS NULL)::integer,
    count(*) FILTER (WHERE COALESCE(plan_detail.already_corrected, false))::integer,
    count(*) FILTER (WHERE COALESCE(plan_detail.is_voided, false))::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_detail.item_type, ''))) IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
    )::integer,
    count(*) FILTER (
      WHERE plan_detail.finance_case_id IS NOT NULL
        AND COALESCE(plan_detail.finance_component_classification, plan_detail.frozen_component_classification, '') IN ('REIMBURSEMENT_GROSS_FIXED', 'NET_PAY_FIXED_RECOVERY')
    )::integer,
    count(*) FILTER (
      WHERE (
        COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
        OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
        OR (
          COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
          AND plan_detail.finance_case_id IS NOT NULL
          AND (
            COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
            OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
            OR NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
            OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
          )
        )
      )
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method, ''))) IS DISTINCT FROM upper(btrim(COALESCE(plan_detail.frozen_target_pay_method, plan_detail.frozen_source_pay_method, plan_detail.pay_channel, '')))
        AND NULLIF(btrim(COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method, '')), '') IS NOT NULL
        AND NULLIF(btrim(COALESCE(plan_detail.frozen_target_pay_method, plan_detail.frozen_source_pay_method, plan_detail.pay_channel, '')), '') IS NOT NULL
    )::integer,
    count(*) FILTER (
      WHERE plan_detail.umbrella_id IS NOT NULL
        AND plan_detail.live_candidate_umbrella_id IS DISTINCT FROM plan_detail.umbrella_id
    )::integer,
    round(COALESCE(sum(COALESCE(plan_detail.amount_ex_vat, 0)), 0), 2)::numeric,
    round(COALESCE(sum(COALESCE(plan_detail.amount_vat, 0)), 0), 2)::numeric,
    round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)::numeric
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_umbrella_count,
    v_key_resolution_failure_count,
    v_already_corrected_count,
    v_voided_count,
    v_timesheet_item_count,
    v_net_fixed_finance_item_count,
    v_gross_channel_sensitive_item_count,
    v_pay_channel_change_count,
    v_umbrella_change_count,
    v_total_amount_ex_vat,
    v_total_amount_vat,
    v_total_amount_inc_vat
  FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail;

  IF v_gross_channel_sensitive_item_count > 0 THEN
    v_suggested_resolution_required := true;
  END IF;

  SELECT COALESCE(jsonb_agg(candidate_rows.candidate_json ORDER BY candidate_rows.candidate_display_name), '[]'::jsonb)
  INTO v_affected_candidates
  FROM (
    SELECT
      plan_detail.candidate_display_name,
      jsonb_build_object(
        'pay_batch_candidate_id', plan_detail.pay_batch_candidate_id,
        'candidate_id', plan_detail.candidate_id,
        'candidate_display_name', plan_detail.candidate_display_name,
        'candidate_tms_ref', plan_detail.candidate_tms_ref,
        'current_candidate_pay_method', COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method),
        'current_umbrella_id', plan_detail.live_candidate_umbrella_id,
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS candidate_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    GROUP BY
      plan_detail.pay_batch_candidate_id,
      plan_detail.candidate_id,
      plan_detail.candidate_display_name,
      plan_detail.candidate_tms_ref,
      COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method),
      plan_detail.live_candidate_umbrella_id
  ) AS candidate_rows;

  SELECT COALESCE(jsonb_agg(transfer_rows.transfer_json ORDER BY transfer_rows.transfer_group_key, transfer_rows.pay_bank_transfer_id), '[]'::jsonb)
  INTO v_affected_transfers
  FROM (
    SELECT
      plan_detail.pay_bank_transfer_id,
      plan_detail.transfer_group_key,
      jsonb_build_object(
        'pay_bank_transfer_id', plan_detail.pay_bank_transfer_id,
        'transfer_status', plan_detail.transfer_status,
        'transfer_amount', plan_detail.transfer_amount,
        'transfer_group_key', plan_detail.transfer_group_key,
        'payee_entity_kind', plan_detail.payee_entity_kind,
        'payee_entity_id', plan_detail.payee_entity_id,
        'candidate_count', count(DISTINCT plan_detail.candidate_id)::integer,
        'item_count', count(*)::integer,
        'selected_amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS transfer_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
    GROUP BY
      plan_detail.pay_bank_transfer_id,
      plan_detail.transfer_status,
      plan_detail.transfer_amount,
      plan_detail.transfer_group_key,
      plan_detail.payee_entity_kind,
      plan_detail.payee_entity_id
  ) AS transfer_rows;

  SELECT COALESCE(jsonb_agg(umbrella_rows.umbrella_json ORDER BY umbrella_rows.umbrella_name), '[]'::jsonb)
  INTO v_affected_umbrellas
  FROM (
    SELECT
      plan_detail.umbrella_name,
      jsonb_build_object(
        'umbrella_id', plan_detail.umbrella_id,
        'umbrella_name', plan_detail.umbrella_name,
        'candidate_count', count(DISTINCT plan_detail.candidate_id)::integer,
        'transfer_count', count(DISTINCT plan_detail.pay_bank_transfer_id) FILTER (WHERE plan_detail.pay_bank_transfer_id IS NOT NULL)::integer,
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS umbrella_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.umbrella_id IS NOT NULL
    GROUP BY plan_detail.umbrella_id, plan_detail.umbrella_name
  ) AS umbrella_rows;

  SELECT COALESCE(jsonb_agg(item_rows.item_json ORDER BY item_rows.candidate_display_name, item_rows.pay_batch_item_id), '[]'::jsonb)
  INTO v_affected_items
  FROM (
    SELECT
      plan_detail.candidate_display_name,
      plan_detail.pay_batch_item_id,
      jsonb_build_object(
        'pay_batch_item_id', plan_detail.pay_batch_item_id,
        'pay_batch_candidate_id', plan_detail.pay_batch_candidate_id,
        'candidate_id', plan_detail.candidate_id,
        'candidate_display_name', plan_detail.candidate_display_name,
        'item_type', plan_detail.item_type,
        'timesheet_id', plan_detail.timesheet_id,
        'pay_bank_transfer_id', plan_detail.pay_bank_transfer_id,
        'finance_case_id', plan_detail.finance_case_id,
        'finance_component_id', plan_detail.finance_component_id,
        'reservation_id', plan_detail.reservation_id,
        'economic_key_type', plan_detail.economic_key_type,
        'economic_key_value', plan_detail.economic_key_value,
        'key_resolution_source', plan_detail.key_resolution_source,
        'key_resolution_failure_reason', plan_detail.key_resolution_failure_reason,
        'amount_ex_vat', plan_detail.amount_ex_vat,
        'amount_vat', plan_detail.amount_vat,
        'amount_inc_vat', plan_detail.amount_inc_vat,
        'is_voided', plan_detail.is_voided,
        'already_corrected', plan_detail.already_corrected,
        'applied_correction_kinds', plan_detail.applied_correction_kinds
      ) AS item_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
  ) AS item_rows;

  SELECT COALESCE(jsonb_agg(finance_case_rows.finance_case_json ORDER BY finance_case_rows.finance_case_id), '[]'::jsonb)
  INTO v_affected_finance_cases
  FROM (
    SELECT
      plan_detail.finance_case_id,
      jsonb_build_object(
        'finance_case_id', plan_detail.finance_case_id,
        'candidate_id', min(plan_detail.candidate_id::text),
        'case_type', max(plan_detail.finance_case_type),
        'advance_kind', max(plan_detail.finance_advance_kind),
        'status', max(plan_detail.finance_case_status),
        'payout_status', max(plan_detail.finance_payout_status),
        'taxability', max(plan_detail.finance_taxability),
        'routing_kind', max(plan_detail.finance_routing_kind),
        'written_off_at_utc', max(plan_detail.finance_written_off_at_utc),
        'cleared_at_utc', max(plan_detail.finance_cleared_at_utc),
        'component_ids', COALESCE(jsonb_agg(DISTINCT plan_detail.finance_component_id) FILTER (WHERE plan_detail.finance_component_id IS NOT NULL), '[]'::jsonb),
        'reservation_ids', COALESCE(jsonb_agg(DISTINCT plan_detail.reservation_id) FILTER (WHERE plan_detail.reservation_id IS NOT NULL), '[]'::jsonb),
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2),
        'requires_suggested_resolution', bool_or(
          COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR (
            COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
            AND (
              NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
            )
          )
        )
      ) AS finance_case_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_case_id IS NOT NULL
    GROUP BY plan_detail.finance_case_id
  ) AS finance_case_rows;

  v_amounts := jsonb_build_object(
    'amount_ex_vat', v_total_amount_ex_vat,
    'amount_vat', v_total_amount_vat,
    'amount_inc_vat', v_total_amount_inc_vat,
    'by_pay_channel', COALESCE((
      SELECT jsonb_agg(channel_rows.channel_json ORDER BY channel_rows.pay_channel)
      FROM (
        SELECT
          COALESCE(plan_detail.pay_channel, 'UNKNOWN') AS pay_channel,
          jsonb_build_object(
            'pay_channel', COALESCE(plan_detail.pay_channel, 'UNKNOWN'),
            'item_count', count(*)::integer,
            'amount_ex_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_ex_vat, 0)), 0), 2),
            'amount_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_vat, 0)), 0), 2),
            'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
          ) AS channel_json
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        GROUP BY COALESCE(plan_detail.pay_channel, 'UNKNOWN')
      ) AS channel_rows
    ), '[]'::jsonb)
  );

  IF v_pay_channel_change_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'PAY_CHANNEL_CHANGED_AFTER_BATCH',
      'message', 'Correction will use the original frozen batch artifact. Any future replacement payment will use the candidate current live pay channel.',
      'affected_item_count', v_pay_channel_change_count
    ));
  END IF;

  IF v_umbrella_change_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'CANDIDATE_UMBRELLA_CHANGED_AFTER_BATCH',
      'message', 'One or more selected items were frozen against a different umbrella/payment group from the candidate current umbrella. Correction uses the frozen batch artifact; future replacement payment uses the current live umbrella routing.',
      'affected_item_count', v_umbrella_change_count
    ));
  END IF;

  IF v_timesheet_item_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'TIMESHEET_CORRECTION_USES_FROZEN_BATCH_METHOD',
      'message', 'Timesheet correction follows the old frozen batch artifact and does not recalculate the old payment from current live timesheet financials.',
      'affected_item_count', v_timesheet_item_count
    ));
  END IF;

  IF v_net_fixed_finance_item_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'NET_FIXED_FINANCE_ITEMS_NO_CHANNEL_CONVERSION',
      'message', 'Net/fixed finance items do not require PAYE/Umbrella amount conversion. Reservation and payout state still need correction handling.',
      'affected_item_count', v_net_fixed_finance_item_count
    ));
  END IF;

  IF v_suggested_resolution_required THEN
    v_suggested_resolution_finance_cases := '[]'::jsonb;

    FOR v_finance_case_record IN
      SELECT
        plan_detail.finance_case_id AS finance_case_id,
        (array_agg(DISTINCT plan_detail.candidate_id ORDER BY plan_detail.candidate_id))[1] AS candidate_id,
        COALESCE(jsonb_agg(DISTINCT plan_detail.finance_component_id) FILTER (WHERE plan_detail.finance_component_id IS NOT NULL), '[]'::jsonb) AS selected_component_ids
      FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
      WHERE plan_detail.finance_case_id IS NOT NULL
        AND (
          COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR (
            COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
            AND (
              NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
            )
          )
        )
      GROUP BY plan_detail.finance_case_id
      ORDER BY plan_detail.finance_case_id
    LOOP
      SELECT
        COALESCE(jsonb_agg(finance_components.id ORDER BY finance_components.id), '[]'::jsonb),
        COALESCE(jsonb_object_agg(
          finance_components.id::text,
          COALESCE(
            NULLIF(btrim(finance_components.resolution_fingerprint), ''),
            md5(jsonb_build_object(
              'finance_component_id', finance_components.id,
              'finance_case_id', finance_components.finance_case_id,
              'classification', finance_components.classification::text,
              'source_pay_method', finance_components.source_pay_method,
              'source_amount', finance_components.source_amount,
              'remaining_source_amount', finance_components.remaining_source_amount,
              'saved_target_pay_method', finance_components.saved_target_pay_method,
              'saved_resolution_mode', finance_components.saved_resolution_mode::text,
              'saved_resolution_payload_json', finance_components.saved_resolution_payload_json,
              'saved_resolution_result_json', finance_components.saved_resolution_result_json,
              'is_resolution_stale', finance_components.is_resolution_stale,
              'closed_at_utc', finance_components.closed_at_utc,
              'updated_at_utc', finance_components.updated_at_utc
            )::text)
          )
        ), '{}'::jsonb)
      INTO
        v_case_component_ids,
        v_case_component_fingerprints
      FROM public.pay_finance_case_components AS finance_components
      WHERE finance_components.finance_case_id = v_finance_case_record.finance_case_id
        AND finance_components.id IN (
          SELECT DISTINCT plan_detail.finance_component_id
          FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
          WHERE plan_detail.finance_case_id = v_finance_case_record.finance_case_id
            AND plan_detail.finance_component_id IS NOT NULL
        );

      v_case_suggestion := NULL::jsonb;
      v_case_suggestion_hash := NULL::text;
      v_case_generation_error := NULL::jsonb;

      IF p_actor_user_id IS NULL THEN
        v_case_generation_error := jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION',
          'message', 'A real taxable channel restructure suggestion requires an actor user id because the existing suggested-resolution function requires p_actor_user_id.'
        );

        v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION',
          'message', 'Gross/taxable/channel-sensitive finance items require a suggested resolution, but no actor user id was supplied to generate it.',
          'finance_case_id', v_finance_case_record.finance_case_id
        ));
      ELSE
        BEGIN
          v_case_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
            p_finance_case_id => v_finance_case_record.finance_case_id,
            p_actor_user_id => p_actor_user_id,
            p_effective_pay_date => v_effective_pay_date,
            p_resolution_path => 'SUGGESTED',
            p_schedule_input_mode => NULL::text,
            p_weeks_total => NULL::integer,
            p_weekly_due => NULL::numeric,
            p_manual_total_remaining => NULL::numeric,
            p_note => 'Generated for payment correction plan ' || p_pay_batch_id::text
          );

          v_case_suggestion_hash := md5(jsonb_build_object(
            'finance_case_id', v_finance_case_record.finance_case_id,
            'candidate_id', v_finance_case_record.candidate_id,
            'component_ids', v_case_component_ids,
            'selected_component_ids', v_finance_case_record.selected_component_ids,
            'component_fingerprints', v_case_component_fingerprints,
            'effective_pay_date', v_effective_pay_date,
            'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
            'resolution_path', COALESCE(v_case_suggestion->>'resolution_path', v_case_suggestion#>>'{request,resolution_path}', 'SUGGESTED'),
            'resolution_mode', COALESCE(v_case_suggestion->>'resolution_mode', v_case_suggestion#>>'{result,resolution_mode}', v_case_suggestion#>>'{suggestion,resolution_mode}'),
            'weeks_total', COALESCE(v_case_suggestion->>'weeks_total', v_case_suggestion#>>'{result,weeks_total}', v_case_suggestion#>>'{suggestion,weeks_total}'),
            'weekly_due', COALESCE(v_case_suggestion->>'weekly_due', v_case_suggestion#>>'{result,weekly_due}', v_case_suggestion#>>'{suggestion,weekly_due}'),
            'manual_total_remaining', COALESCE(v_case_suggestion->>'manual_total_remaining', v_case_suggestion#>>'{result,manual_total_remaining}', v_case_suggestion#>>'{suggestion,manual_total_remaining}'),
            'taxable_channel_result', COALESCE(
              v_case_suggestion->'taxable_channel_result',
              v_case_suggestion->'result',
              v_case_suggestion->'suggestion',
              v_case_suggestion
            ) - 'generated_at'
              - 'generated_at_utc'
              - 'created_at'
              - 'created_at_utc'
              - 'updated_at'
              - 'updated_at_utc'
              - 'audit'
              - 'debug'
          )::text);
        EXCEPTION
          WHEN OTHERS THEN
            v_case_generation_error := jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_GENERATION_FAILED',
              'sqlstate', SQLSTATE,
              'message', SQLERRM
            );

            v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_GENERATION_FAILED',
              'message', 'A gross/taxable/channel-sensitive finance suggested resolution could not be generated using the existing suggested-resolution process.',
              'finance_case_id', v_finance_case_record.finance_case_id,
              'sqlstate', SQLSTATE,
              'error_message', SQLERRM
            ));
        END;
      END IF;

      v_suggested_resolution_finance_cases := v_suggested_resolution_finance_cases || jsonb_build_array(jsonb_build_object(
        'finance_case_id', v_finance_case_record.finance_case_id,
        'candidate_id', v_finance_case_record.candidate_id,
        'component_ids', v_case_component_ids,
        'selected_component_ids', v_finance_case_record.selected_component_ids,
        'current_component_fingerprints', v_case_component_fingerprints,
        'suggestion', v_case_suggestion,
        'suggestion_hash', v_case_suggestion_hash,
        'suggestion_hash_basis', jsonb_build_object(
          'finance_case_id', v_finance_case_record.finance_case_id,
          'candidate_id', v_finance_case_record.candidate_id,
          'component_ids', v_case_component_ids,
          'selected_component_ids', v_finance_case_record.selected_component_ids,
          'component_fingerprints', v_case_component_fingerprints,
          'effective_pay_date', v_effective_pay_date,
          'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure'
        ),
        'suggestion_generation_error', v_case_generation_error,
        'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
        'effective_pay_date', v_effective_pay_date,
        'accepted_payload_required', true
      ));
    END LOOP;

    v_suggested_resolution := jsonb_build_object(
      'required', true,
      'reason', 'Gross/taxable/channel-sensitive finance items are present and must use the existing suggested-resolution process before correction apply.',
      'must_be_accepted_before_apply', true,
      'must_be_applied_atomically_with_correction', true,
      'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
      'finance_cases', v_suggested_resolution_finance_cases
    );

    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SUGGESTED_RESOLUTION_REQUIRED',
      'message', 'Gross/taxable/channel-sensitive finance items require an accepted suggested resolution before this correction request can be started.',
      'affected_item_count', v_gross_channel_sensitive_item_count,
      'suggested_resolution', v_suggested_resolution
    ));
  ELSE
    v_suggested_resolution := NULL::jsonb;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_overlap_items;
  CREATE TEMP TABLE _tmp_payment_correction_plan_overlap_items ON COMMIT DROP AS
  WITH selected_candidate_ids AS (
    SELECT DISTINCT plan_detail.candidate_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.candidate_id IS NOT NULL
  ),
  selected_economic_keys AS (
    SELECT DISTINCT
      plan_detail.candidate_id,
      plan_detail.timesheet_id,
      plan_detail.economic_key_type,
      plan_detail.economic_key_value
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.candidate_id IS NOT NULL
      AND plan_detail.timesheet_id IS NOT NULL
      AND plan_detail.economic_key_type IS NOT NULL
      AND plan_detail.economic_key_value IS NOT NULL
  ),
  selected_finance_scope AS (
    SELECT DISTINCT
      plan_detail.finance_case_id,
      plan_detail.finance_component_id,
      plan_detail.reservation_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_case_id IS NOT NULL
       OR plan_detail.finance_component_id IS NOT NULL
       OR plan_detail.reservation_id IS NOT NULL
  ),
  filtered_other_items AS (
    SELECT
      public.pay_batches.id AS other_pay_batch_id,
      public.pay_batches.status AS other_batch_status,
      public.pay_batches.created_at_utc AS other_batch_created_at_utc,
      public.pay_batches.execution_commit_state AS other_execution_commit_state,
      public.pay_batches.execution_commit_ref AS other_execution_commit_ref,
      public.pay_batch_candidates.candidate_id AS other_candidate_id,
      public.pay_batch_items.id AS other_pay_batch_item_id,
      public.pay_batch_items.pay_batch_candidate_id AS other_pay_batch_candidate_id,
      public.pay_batch_items.timesheet_id AS other_timesheet_id,
      public.pay_batch_items.finance_case_id AS other_finance_case_id,
      public.pay_batch_items.finance_component_id AS other_finance_component_id,
      public.pay_batch_items.reservation_id AS other_reservation_id,
      public.pay_batch_items.pay_bank_transfer_id AS other_pay_bank_transfer_id,
      other_economic_components.key_type AS other_economic_key_type,
      other_economic_components.key_value AS other_economic_key_value,
      public.pay_batch_candidates.settlement_status AS other_candidate_settlement_status,
      public.pay_batch_candidates.settled_at_utc AS other_candidate_settled_at_utc,
      public.pay_bank_transfers.status AS other_transfer_status,
      public.pay_bank_transfers.completed_at_utc AS other_transfer_completed_at_utc
    FROM public.pay_batches
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.pay_batch_id = public.pay_batches.id
    JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = public.pay_batch_candidates.id
    JOIN selected_candidate_ids
      ON selected_candidate_ids.candidate_id = public.pay_batch_candidates.candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    LEFT JOIN LATERAL public._pay_batch_item_economic_components(NULL::uuid, ARRAY[public.pay_batch_items.id]) AS other_economic_components
      ON true
    WHERE public.pay_batches.id <> p_pay_batch_id
      AND COALESCE(public.pay_batch_items.is_voided, false) = false
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS other_applied_corrections
        WHERE other_applied_corrections.pay_batch_item_id = public.pay_batch_items.id
          AND other_applied_corrections.status = 'APPLIED'
      )
  )
  SELECT
    filtered_other_items.other_pay_batch_id,
    filtered_other_items.other_batch_status,
    filtered_other_items.other_batch_created_at_utc,
    filtered_other_items.other_execution_commit_state,
    filtered_other_items.other_execution_commit_ref,
    filtered_other_items.other_candidate_id,
    filtered_other_items.other_pay_batch_item_id,
    filtered_other_items.other_pay_batch_candidate_id,
    filtered_other_items.other_timesheet_id,
    filtered_other_items.other_finance_case_id,
    filtered_other_items.other_finance_component_id,
    filtered_other_items.other_reservation_id,
    filtered_other_items.other_pay_bank_transfer_id,
    filtered_other_items.other_economic_key_type,
    filtered_other_items.other_economic_key_value,
    filtered_other_items.other_candidate_settlement_status,
    filtered_other_items.other_candidate_settled_at_utc,
    filtered_other_items.other_transfer_status,
    filtered_other_items.other_transfer_completed_at_utc,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM selected_economic_keys
        WHERE selected_economic_keys.candidate_id = filtered_other_items.other_candidate_id
          AND selected_economic_keys.timesheet_id = filtered_other_items.other_timesheet_id
          AND selected_economic_keys.economic_key_type = filtered_other_items.other_economic_key_type
          AND selected_economic_keys.economic_key_value = filtered_other_items.other_economic_key_value
      ) THEN 'ECONOMIC_KEY'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.finance_case_id IS NOT NULL
          AND selected_finance_scope.finance_case_id = filtered_other_items.other_finance_case_id
      ) THEN 'FINANCE_CASE'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.finance_component_id IS NOT NULL
          AND selected_finance_scope.finance_component_id = filtered_other_items.other_finance_component_id
      ) THEN 'FINANCE_COMPONENT'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.reservation_id IS NOT NULL
          AND selected_finance_scope.reservation_id = filtered_other_items.other_reservation_id
      ) THEN 'RESERVATION'
      ELSE NULL
    END AS overlap_kind
  FROM filtered_other_items
  WHERE EXISTS (
      SELECT 1
      FROM selected_economic_keys
      WHERE selected_economic_keys.candidate_id = filtered_other_items.other_candidate_id
        AND selected_economic_keys.timesheet_id = filtered_other_items.other_timesheet_id
        AND selected_economic_keys.economic_key_type = filtered_other_items.other_economic_key_type
        AND selected_economic_keys.economic_key_value = filtered_other_items.other_economic_key_value
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.finance_case_id IS NOT NULL
        AND selected_finance_scope.finance_case_id = filtered_other_items.other_finance_case_id
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.finance_component_id IS NOT NULL
        AND selected_finance_scope.finance_component_id = filtered_other_items.other_finance_component_id
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.reservation_id IS NOT NULL
        AND selected_finance_scope.reservation_id = filtered_other_items.other_reservation_id
    );

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (other_pay_batch_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (other_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (overlap_kind);

  SELECT COALESCE(jsonb_agg(overlap_batches.overlap_json ORDER BY overlap_batches.other_batch_created_at_utc, overlap_batches.other_pay_batch_id), '[]'::jsonb)
  INTO v_draft_interference
  FROM (
    SELECT
      overlap_items.other_pay_batch_id,
      min(overlap_items.other_batch_created_at_utc) AS other_batch_created_at_utc,
      jsonb_build_object(
        'pay_batch_id', overlap_items.other_pay_batch_id,
        'status', max(overlap_items.other_batch_status),
        'execution_commit_state', max(overlap_items.other_execution_commit_state),
        'execution_commit_ref', max(overlap_items.other_execution_commit_ref),
        'overlap_kinds', COALESCE(jsonb_agg(DISTINCT overlap_items.overlap_kind) FILTER (WHERE overlap_items.overlap_kind IS NOT NULL), '[]'::jsonb),
        'candidate_count', count(DISTINCT overlap_items.other_candidate_id)::integer,
        'item_count', count(DISTINCT overlap_items.other_pay_batch_item_id)::integer,
        'message', 'This correction cannot be applied because draft batch ' || overlap_items.other_pay_batch_id::text || ' already reserves affected items under the current pay channel. Delete/cancel draft batch ' || overlap_items.other_pay_batch_id::text || ' first, then retry.'
      ) AS overlap_json
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE public._pay_batch_status_is_active_reservation(overlap_items.other_batch_status)
    GROUP BY overlap_items.other_pay_batch_id
  ) AS overlap_batches;

  IF jsonb_array_length(v_draft_interference) > 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'OVERLAPPING_DRAFT_BATCH_RESERVES_AFFECTED_ITEMS',
      'message', 'This correction cannot be applied because another draft/reserved batch already reserves affected items. Delete/cancel the interfering draft batch first, then retry.',
      'draft_interference', v_draft_interference
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_batch_status, ''))) IN ('SCHEDULED', 'EXECUTING', 'WAITING_BANK_CONFIRM', 'AUTHORISED_FOR_PAYMENT', 'AWAITING_AUTHORISATION')
        OR upper(btrim(COALESCE(overlap_items.other_execution_commit_state, ''))) IN ('COMMITTED', 'SUBMITTED')
        OR NULLIF(btrim(COALESCE(overlap_items.other_execution_commit_ref, '')), '') IS NOT NULL
      )
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_AUTHORISED_OR_SUBMITTED_BATCH_OVERLAP',
      'message', 'A later authorised/submitted batch overlaps the selected correction scope. Manual review is required before correction can apply.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_batch_status, ''))) IN ('FAILED', 'PARTIAL')
      )
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_FAILED_OR_AMBIGUOUS_BATCH_OVERLAP',
      'message', 'A later failed or partial batch overlaps the selected correction scope. Manual review is required.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_candidate_settlement_status, ''))) = 'SETTLED'
        OR overlap_items.other_candidate_settled_at_utc IS NOT NULL
        OR upper(btrim(COALESCE(overlap_items.other_transfer_status, ''))) = 'COMPLETED'
        OR overlap_items.other_transfer_completed_at_utc IS NOT NULL
      )
  ) THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_SETTLED_BATCH_OVERLAP_WARNING',
      'message', 'A later settled batch overlaps the candidate/economic scope. It must remain untouched; this correction only affects the selected old frozen batch artifacts.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_written_off_at_utc IS NOT NULL
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'FINANCE_CASE_WRITTEN_OFF',
      'message', 'One or more selected finance cases have been written off and require manual finance review.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_component_is_resolution_stale IS TRUE
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'FINANCE_COMPONENT_RESOLUTION_STALE',
      'message', 'One or more selected finance component resolutions are stale and must be regenerated before correction apply.'
    ));
  END IF;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(btrim(COALESCE(p_selection_json->>'work_unit', '')), ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', p_pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(p_pay_batch_id::text),
    'is_whole_batch', (
      v_scope_type = 'BATCH'
      AND NOT (COALESCE(p_selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_batch_candidate_id',
        'pay_batch_candidate_ids',
        'selected_pay_batch_candidate_ids',
        'candidate_id',
        'candidate_ids',
        'selected_candidate_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'umbrella_id',
        'umbrella_ids',
        'selected_umbrella_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(p_selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.candidate_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.umbrella_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.finance_case_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.finance_component_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.reservation_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.transfer_group_key AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE NULLIF(btrim(COALESCE(plan_detail.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_selected_mail_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;
  CREATE TEMP TABLE _tmp_payment_correction_plan_mail ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.type,
      public.mail_outbox."to" AS mail_to,
      public.mail_outbox.subject,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.created_at_utc,
      public.mail_outbox.sent_at,
      public.mail_outbox.failed_at,
      public.mail_outbox.reference,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.email_type,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) IN ('QUEUED', 'SENT')
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id,
      candidate_mail.type,
      candidate_mail.mail_to,
      candidate_mail.subject,
      candidate_mail.status,
      candidate_mail.created_at_utc,
      candidate_mail.sent_at,
      candidate_mail.failed_at,
      candidate_mail.reference,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.email_type,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        p_pay_batch_id,
        p_selection_json,
        v_selected_mail_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.id,
    matched_mail.type,
    matched_mail.mail_to,
    matched_mail.subject,
    matched_mail.status,
    matched_mail.created_at_utc,
    matched_mail.sent_at,
    matched_mail.failed_at,
    matched_mail.reference,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.email_type,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS scope_match,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result AS match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  SELECT
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    )::integer,
    count(*) FILTER (
      WHERE COALESCE(plan_mail.requires_review, false)
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.requires_review, false)
    )::integer
  INTO
    v_queued_unsent_count,
    v_sent_notice_count,
    v_mail_legacy_review_count,
    v_mail_legacy_queued_review_count
  FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail;

  IF v_mail_legacy_queued_review_count > 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'MAIL_SCOPE_LEGACY_BROAD_MATCH_REQUIRES_REVIEW',
      'message', 'One or more queued payment notices only match the selected payment scope by broad legacy candidate, umbrella, recipient, or reference data. Manual review is required before applying this correction.',
      'queued_notice_count', v_mail_legacy_queued_review_count
    ));
  ELSIF v_mail_legacy_review_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'MAIL_SCOPE_LEGACY_BROAD_MATCH_REQUIRES_REVIEW',
      'message', 'One or more payment notices only match the selected payment scope by broad legacy candidate, umbrella, recipient, or reference data and require review.',
      'notice_count', v_mail_legacy_review_count
    ));
  END IF;

  v_communication_effects := jsonb_build_object(
    'queued_unsent_to_cancel', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb)
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    ), '[]'::jsonb),
    'legacy_broad_matches_requiring_review', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'status', plan_mail.status,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb)
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE COALESCE(plan_mail.requires_review, false)
        AND COALESCE(plan_mail.safe_to_cancel, false) = false
    ), '[]'::jsonb),
    'sent_to_leave_as_audit', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb),
        'sent_at', plan_mail.sent_at
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    ), '[]'::jsonb),
    'external_correction_notice', false,
    'admin_notice_required', true,
    'queued_unsent_count', v_queued_unsent_count,
    'sent_notice_count', v_sent_notice_count,
    'legacy_broad_review_count', v_mail_legacy_review_count,
    'legacy_broad_queued_review_count', v_mail_legacy_queued_review_count,
    'selected_scope_json', v_selected_mail_scope_json
  );

  v_work_item_count := CASE
    WHEN v_selected_transfer_count > 0
      AND v_classification IN ('NO_MONEY_UNWIND', 'TRUE_SETTLED_REVERSAL_REQUIRED')
      THEN v_selected_transfer_count
    WHEN v_selected_candidate_count > 0
      THEN v_selected_candidate_count
    ELSE v_selected_item_count
  END;

  v_large_correction := jsonb_build_object(
    'large_correction', COALESCE(v_work_item_count, 0) > v_large_correction_threshold,
    'threshold', v_large_correction_threshold,
    'work_item_count', COALESCE(v_work_item_count, 0),
    'recommended_chunk_size', v_recommended_chunk_size
  );

  v_work_expansion_plan := jsonb_build_object(
    'work_kind', CASE v_classification
      WHEN 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
      WHEN 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
      WHEN 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL'
      ELSE NULL
    END,
    'work_unit', CASE
      WHEN v_selected_transfer_count > 0
        AND v_classification IN ('NO_MONEY_UNWIND', 'TRUE_SETTLED_REVERSAL_REQUIRED') THEN 'TRANSFER'
      WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
      ELSE 'ITEM'
    END,
    'work_item_count', COALESCE(v_work_item_count, 0),
    'process_synchronously', COALESCE(v_work_item_count, 0) <= v_large_correction_threshold,
    'recommended_chunk_size', v_recommended_chunk_size,
    'requires_work_queue', COALESCE(v_work_item_count, 0) > v_large_correction_threshold,
    'selected_pay_batch_item_ids', to_jsonb(v_selected_pay_batch_item_ids),
    'selected_selection_hash', v_selected_selection_hash
  );

  v_selection_summary := jsonb_build_object(
    'scope_type', upper(nullif(btrim(COALESCE(p_selection_json->>'scope_type', '')), '')),
    'selected_pay_batch_item_ids', to_jsonb(v_selected_pay_batch_item_ids),
    'selected_selection_hash', v_selected_selection_hash,
    'selection_filters_applied', v_selection_filters_applied,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'selected_umbrella_count', v_selected_umbrella_count,
    'key_resolution_failure_count', v_key_resolution_failure_count,
    'already_corrected_count', v_already_corrected_count,
    'voided_count', v_voided_count,
    'pay_channel_change_count', v_pay_channel_change_count,
    'umbrella_change_count', v_umbrella_change_count,
    'gross_channel_sensitive_item_count', v_gross_channel_sensitive_item_count,
    'net_fixed_finance_item_count', v_net_fixed_finance_item_count
  );

  v_can_apply := (
    v_classification <> 'AMBIGUOUS_REVIEW_REQUIRED'
    AND jsonb_array_length(v_hard_blockers) = 0
    AND v_selected_item_count > 0
    AND NOT v_suggested_resolution_required
  );

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PLAN_RESULT',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'classification', v_classification,
      'recommended_action', v_recommended_action,
      'can_apply', v_can_apply,
      'selected_item_count', v_selected_item_count,
      'selected_candidate_count', v_selected_candidate_count,
      'selected_transfer_count', v_selected_transfer_count,
      'hard_blocker_count', jsonb_array_length(v_hard_blockers),
      'warning_count', jsonb_array_length(v_warnings),
      'suggested_resolution_required', v_suggested_resolution_required,
      'umbrella_change_count', v_umbrella_change_count,
      'work_item_count', v_work_item_count,
      'large_correction', COALESCE(v_work_item_count, 0) > v_large_correction_threshold
    ),
    'pay_payment_correction',
    v_subject_id,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id,
    'classification', v_classification,
    'recommended_action', v_recommended_action,
    'can_apply', v_can_apply,
    'hard_blockers', v_hard_blockers,
    'warnings', v_warnings,
    'selection', v_selection_summary,
    'affected_candidates', v_affected_candidates,
    'affected_transfers', v_affected_transfers,
    'affected_umbrellas', v_affected_umbrellas,
    'affected_items', v_affected_items,
    'affected_finance_cases', v_affected_finance_cases,
    'communication_effects', v_communication_effects,
    'suggested_resolution_required', v_suggested_resolution_required,
    'suggested_resolution', v_suggested_resolution,
    'amounts', v_amounts,
    'draft_interference', v_draft_interference,
    'large_correction', v_large_correction,
    'work_expansion_plan', v_work_expansion_plan,
    'movement_classification_detail', v_classification_result,
    'batch', jsonb_build_object(
      'status', v_batch_status,
      'pay_date', v_batch_pay_date,
      'authoritative_payment_date', v_batch_authoritative_payment_date,
      'effective_pay_date', v_effective_pay_date,
      'created_at_utc', v_batch_created_at_utc,
      'execution_commit_state', v_batch_execution_commit_state,
      'execution_commit_ref', v_batch_execution_commit_ref
    )
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PLAN_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'selection_json', p_selection_json,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
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




CREATE OR REPLACE FUNCTION public.pay_payment_correction_request_start(p_pay_batch_id uuid, p_selection_json jsonb, p_reason text, p_actor_user_id uuid, p_source_bank_event_id uuid DEFAULT NULL::uuid, p_auto_requested boolean DEFAULT false, p_accepted_resolution_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_exists boolean := false;
  v_actor_exists boolean := false;
  v_actor_active boolean := false;
  v_source_event_exists boolean := false;
  v_source_event_source text;
  v_source_event_mapping_status text;
  v_source_event_mapping_method text;
  v_plan jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_plan_can_apply boolean := false;
  v_hard_blockers jsonb := '[]'::jsonb;
  v_effective_hard_blockers jsonb := '[]'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_accepted_resolution_supplied boolean := false;
  v_accepted_resolution_is_stale boolean := false;
  v_safe_to_auto_apply boolean := false;
  v_ambiguous_mapping_count integer := 0;
  v_amount_mismatch_count integer := 0;
  v_aggregate_subset_issue_count integer := 0;
  v_correction_kind text;
  v_status text;
  v_required_quantity integer := 1;
  v_approved_count integer := 0;
  v_selection_hash text;
  v_plan_material_json jsonb;
  v_plan_hash text;
  v_accepted_resolution_hash text;
  v_finance_resolution_validation jsonb := NULL::jsonb;
  v_finance_resolution_blocker jsonb := NULL::jsonb;
  v_selected_scope_json jsonb := '{}'::jsonb;
  v_request public.pay_payment_correction_requests%rowtype;
  v_existing_request public.pay_payment_correction_requests%rowtype;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_REQUEST_START_BEGIN',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
      'actor_user_id', p_actor_user_id,
      'source_bank_event_id', p_source_bank_event_id,
      'auto_requested', COALESCE(p_auto_requested, false),
      'accepted_resolution_supplied', p_accepted_resolution_json IS NOT NULL
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

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_check
    WHERE batch_check.id = p_pay_batch_id
  )
  INTO v_batch_exists;

  IF NOT COALESCE(v_batch_exists, false) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF COALESCE(p_auto_requested, false) = false THEN
    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'ACTOR_USER_ID_REQUIRED',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    SELECT
      true,
      COALESCE(public.tms_users.is_active, false)
    INTO
      v_actor_exists,
      v_actor_active
    FROM public.tms_users
    WHERE public.tms_users.id = p_actor_user_id;

    IF NOT COALESCE(v_actor_exists, false) THEN
      RAISE EXCEPTION 'ACTOR_USER_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'ACTOR_USER_NOT_FOUND',
                'actor_user_id', p_actor_user_id
              )::text;
    END IF;

    IF NOT COALESCE(v_actor_active, false) THEN
      RAISE EXCEPTION 'ACTOR_USER_INACTIVE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'ACTOR_USER_INACTIVE',
                'actor_user_id', p_actor_user_id
              )::text;
    END IF;

    IF NULLIF(btrim(COALESCE(p_reason, '')), '') IS NULL THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REASON_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_REASON_REQUIRED',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;
  END IF;

  IF COALESCE(p_auto_requested, false) = true THEN
    IF p_source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;
  END IF;

  IF p_source_bank_event_id IS NOT NULL THEN
    SELECT
      true,
      public.pay_bank_transfer_events.event_source,
      public.pay_bank_transfer_events.mapping_status,
      public.pay_bank_transfer_events.mapping_method
    INTO
      v_source_event_exists,
      v_source_event_source,
      v_source_event_mapping_status,
      v_source_event_mapping_method
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.id = p_source_bank_event_id
      AND public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id;

    IF NOT COALESCE(v_source_event_exists, false) THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH',
                'pay_batch_id', p_pay_batch_id,
                'source_bank_event_id', p_source_bank_event_id
              )::text;
    END IF;
  END IF;

  IF p_accepted_resolution_json IS NOT NULL
     AND COALESCE(jsonb_typeof(p_accepted_resolution_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'ACCEPTED_RESOLUTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACCEPTED_RESOLUTION_JSON_MUST_BE_OBJECT',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_plan := public.pay_payment_correction_plan(
    p_pay_batch_id,
    p_selection_json,
    p_actor_user_id
  );

  v_classification := COALESCE(v_plan->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
  v_plan_can_apply := COALESCE((v_plan->>'can_apply')::boolean, false);
  v_hard_blockers := COALESCE(v_plan->'hard_blockers', '[]'::jsonb);
  v_suggested_resolution_required := COALESCE((v_plan->>'suggested_resolution_required')::boolean, false);
  v_accepted_resolution_supplied := p_accepted_resolution_json IS NOT NULL;
  v_safe_to_auto_apply := COALESCE((v_plan#>>'{movement_classification_detail,safe_to_auto_apply}')::boolean, false);
  v_ambiguous_mapping_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,bank_event_ambiguous_mapping_count}')::integer, 0);
  v_amount_mismatch_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,bank_event_amount_mismatch_count}')::integer, 0);
  v_aggregate_subset_issue_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,aggregate_subset_issue_count}')::integer, 0);

  v_accepted_resolution_is_stale := p_accepted_resolution_json IS NOT NULL
    AND (
      lower(COALESCE(p_accepted_resolution_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json->>'stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json->>'resolution_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json#>>'{validation,is_stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json#>>'{validation,stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
    );

  SELECT COALESCE(jsonb_agg(blocker_elements.blocker_value ORDER BY blocker_elements.blocker_ordinal), '[]'::jsonb)
  INTO v_effective_hard_blockers
  FROM jsonb_array_elements(v_hard_blockers) WITH ORDINALITY AS blocker_elements(blocker_value, blocker_ordinal)
  WHERE NOT (
    v_suggested_resolution_required
    AND v_accepted_resolution_supplied
    AND COALESCE(blocker_elements.blocker_value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
  );

  IF v_accepted_resolution_is_stale THEN
    v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_STALE',
      'message', 'The accepted suggested finance resolution is stale and must be regenerated before correction request start.'
    ));
  END IF;

  IF v_suggested_resolution_required AND v_accepted_resolution_supplied THEN
    v_finance_resolution_validation := public._pay_payment_correction_validate_accepted_finance_resolution(
      p_pay_batch_id => p_pay_batch_id,
      p_selection_json => p_selection_json,
      p_plan_json => v_plan,
      p_accepted_resolution_json => p_accepted_resolution_json,
      p_actor_user_id => p_actor_user_id
    );

    IF NOT COALESCE((v_finance_resolution_validation->>'ok')::boolean, false)
       OR NOT COALESCE((v_finance_resolution_validation->>'validated')::boolean, false) THEN
      v_finance_resolution_blocker := COALESCE(
        v_finance_resolution_validation->'blocker',
        jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_VALIDATION_FAILED',
          'message', 'Accepted finance resolution validation failed before correction request start.'
        )
      );

      v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(v_finance_resolution_blocker);
    END IF;
  END IF;

  IF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_CLASSIFICATION_AMBIGUOUS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_CLASSIFICATION_AMBIGUOUS',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF v_suggested_resolution_required AND NOT v_accepted_resolution_supplied THEN
    RAISE EXCEPTION 'SUGGESTED_RESOLUTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_REQUIRED',
              'pay_batch_id', p_pay_batch_id,
              'suggested_resolution', v_plan->'suggested_resolution'
            )::text;
  END IF;

  IF jsonb_array_length(v_effective_hard_blockers) > 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLAN_HAS_BLOCKERS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_PLAN_HAS_BLOCKERS',
              'pay_batch_id', p_pay_batch_id,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF NOT v_plan_can_apply
     AND NOT (v_suggested_resolution_required AND v_accepted_resolution_supplied AND jsonb_array_length(v_effective_hard_blockers) = 0) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLAN_CANNOT_APPLY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_PLAN_CANNOT_APPLY',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification,
              'can_apply', v_plan_can_apply,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF COALESCE(p_auto_requested, false) THEN
    IF NOT v_safe_to_auto_apply THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_NOT_SAFE_TO_APPLY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_NOT_SAFE_TO_APPLY',
                'pay_batch_id', p_pay_batch_id,
                'classification', v_classification,
                'safe_to_auto_apply', v_safe_to_auto_apply,
                'blockers', v_effective_hard_blockers
              )::text;
    END IF;

    IF v_suggested_resolution_required THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_BLOCKED_BY_SUGGESTED_RESOLUTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_BLOCKED_BY_SUGGESTED_RESOLUTION',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    IF COALESCE(upper(v_source_event_mapping_status), '') <> 'MATCHED'
       OR COALESCE(upper(v_source_event_mapping_method), '') NOT IN (
         'TRANSFER_ID',
         'PROVIDER_EVENT_ID',
         'PROVIDER_REFERENCE',
         'REQUEST_ID',
         'RAIL_TX_ID',
         'PAYMENT_REFERENCE',
         'MANUAL_TRANSFER_SELECTION'
       ) THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_REQUIRES_STRONG_BANK_EVENT_MAPPING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_REQUIRES_STRONG_BANK_EVENT_MAPPING',
                'pay_batch_id', p_pay_batch_id,
                'source_bank_event_id', p_source_bank_event_id,
                'mapping_status', v_source_event_mapping_status,
                'mapping_method', v_source_event_mapping_method
              )::text;
    END IF;

    IF v_ambiguous_mapping_count > 0 OR v_amount_mismatch_count > 0 OR v_aggregate_subset_issue_count > 0 THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_HAS_AMBIGUITY_OR_AMOUNT_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_HAS_AMBIGUITY_OR_AMOUNT_MISMATCH',
                'pay_batch_id', p_pay_batch_id,
                'ambiguous_mapping_count', v_ambiguous_mapping_count,
                'amount_mismatch_count', v_amount_mismatch_count,
                'aggregate_subset_issue_count', v_aggregate_subset_issue_count
              )::text;
    END IF;
  END IF;

  v_correction_kind := CASE
    WHEN v_classification = 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
    WHEN v_classification = 'NO_MONEY_UNWIND'
         AND COALESCE(upper(v_source_event_source), '') = 'MANUAL_EVIDENCE' THEN 'MANUAL_EVIDENCE_NO_MONEY'
    WHEN v_classification = 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
    WHEN v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED'
         AND COALESCE(upper(v_source_event_source), '') = 'MANUAL_EVIDENCE' THEN 'MANUAL_EVIDENCE_SETTLED_RETURN'
    WHEN v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL'
    ELSE NULL
  END;

  IF v_correction_kind IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_KIND_COULD_NOT_BE_DETERMINED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_KIND_COULD_NOT_BE_DETERMINED',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification
            )::text;
  END IF;

  SELECT COALESCE(public.settings_defaults.payment_authoriser_quantity, 1)
  INTO v_required_quantity
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_required_quantity := GREATEST(COALESCE(v_required_quantity, 1), 1);

  IF COALESCE(p_auto_requested, false) THEN
    v_status := 'AUTHORISED';
    v_approved_count := v_required_quantity;
  ELSE
    v_status := 'AWAITING_AUTHORISATION';
    v_approved_count := 0;
  END IF;

  WITH selected_scope_rows AS (
    SELECT selected_items.*
    FROM public._pay_payment_correction_selected_items(
      p_pay_batch_id,
      p_selection_json,
      true
    ) AS selected_items
  )
  SELECT jsonb_build_object(
    'pay_batch_id', p_pay_batch_id,
    'requested_action', COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), ''),
    'correction_kind', v_correction_kind,
    'scope_type', COALESCE(NULLIF(btrim(p_selection_json->>'scope_type'), ''), ''),
    'transfer_group_key', COALESCE(NULLIF(btrim(p_selection_json->>'transfer_group_key'), ''), ''),
    'pay_batch_item_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_batch_item_id ORDER BY selected_scope_rows.pay_batch_item_id) FILTER (WHERE selected_scope_rows.pay_batch_item_id IS NOT NULL), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_bank_transfer_id ORDER BY selected_scope_rows.pay_bank_transfer_id) FILTER (WHERE selected_scope_rows.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_batch_candidate_id ORDER BY selected_scope_rows.pay_batch_candidate_id) FILTER (WHERE selected_scope_rows.pay_batch_candidate_id IS NOT NULL), '[]'::jsonb),
    'candidate_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.candidate_id ORDER BY selected_scope_rows.candidate_id) FILTER (WHERE selected_scope_rows.candidate_id IS NOT NULL), '[]'::jsonb),
    'finance_case_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.finance_case_id ORDER BY selected_scope_rows.finance_case_id) FILTER (WHERE selected_scope_rows.finance_case_id IS NOT NULL), '[]'::jsonb),
    'finance_component_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.finance_component_id ORDER BY selected_scope_rows.finance_component_id) FILTER (WHERE selected_scope_rows.finance_component_id IS NOT NULL), '[]'::jsonb),
    'reservation_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.reservation_id ORDER BY selected_scope_rows.reservation_id) FILTER (WHERE selected_scope_rows.reservation_id IS NOT NULL), '[]'::jsonb)
  )
  INTO v_selected_scope_json
  FROM selected_scope_rows;

  v_selected_scope_json := COALESCE(v_selected_scope_json, jsonb_build_object(
    'pay_batch_id', p_pay_batch_id,
    'requested_action', COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), ''),
    'correction_kind', v_correction_kind,
    'scope_type', COALESCE(NULLIF(btrim(p_selection_json->>'scope_type'), ''), ''),
    'transfer_group_key', COALESCE(NULLIF(btrim(p_selection_json->>'transfer_group_key'), ''), ''),
    'pay_batch_item_ids', '[]'::jsonb,
    'pay_bank_transfer_ids', '[]'::jsonb,
    'pay_batch_candidate_ids', '[]'::jsonb,
    'candidate_ids', '[]'::jsonb,
    'finance_case_ids', '[]'::jsonb,
    'finance_component_ids', '[]'::jsonb,
    'reservation_ids', '[]'::jsonb
  ));

  v_selection_hash := md5(v_selected_scope_json::text);

  v_plan_material_json := jsonb_build_object(
    'classification', v_classification,
    'recommended_action', v_plan->>'recommended_action',
    'selection', COALESCE(v_plan->'selection', '{}'::jsonb),
    'amounts', COALESCE(v_plan->'amounts', '{}'::jsonb),
    'hard_blockers', v_hard_blockers,
    'effective_hard_blockers', v_effective_hard_blockers,
    'suggested_resolution_required', v_suggested_resolution_required,
    'work_expansion_plan', COALESCE(v_plan->'work_expansion_plan', '{}'::jsonb),
    'movement_classification_detail', jsonb_build_object(
      'classification', v_plan#>>'{movement_classification_detail,classification}',
      'counts', COALESCE(v_plan#>'{movement_classification_detail,counts}', '{}'::jsonb),
      'blockers', COALESCE(v_plan#>'{movement_classification_detail,blockers}', '[]'::jsonb),
      'safe_to_auto_apply', COALESCE(v_plan#>'{movement_classification_detail,safe_to_auto_apply}', 'false'::jsonb)
    )
  );

  v_plan_hash := md5(v_plan_material_json::text);
  v_accepted_resolution_hash := CASE
    WHEN p_accepted_resolution_json IS NULL THEN NULL
    ELSE md5(p_accepted_resolution_json::text)
  END;

  SELECT public.pay_payment_correction_requests.*
  INTO v_existing_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.pay_batch_id = p_pay_batch_id
    AND public.pay_payment_correction_requests.selection_hash = v_selection_hash
    AND public.pay_payment_correction_requests.correction_kind = v_correction_kind
    AND public.pay_payment_correction_requests.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING')
  ORDER BY public.pay_payment_correction_requests.created_at_utc
  LIMIT 1;

  IF v_existing_request.id IS NOT NULL THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_REQUEST_START_EXISTING_OPEN_REQUEST',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'existing_correction_request_id', v_existing_request.id,
        'correction_kind', v_existing_request.correction_kind,
        'status', v_existing_request.status
      ),
      'pay_payment_correction',
      p_pay_batch_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'existing_request', true,
      'correction_request_id', v_existing_request.id,
      'pay_batch_id', v_existing_request.pay_batch_id,
      'correction_kind', v_existing_request.correction_kind,
      'status', v_existing_request.status,
      'authorisation_required', NOT COALESCE(v_existing_request.auto_requested, false),
      'approved_count', v_existing_request.approved_count,
      'required_quantity', v_existing_request.required_quantity,
      'plan', v_existing_request.plan_json
    );
  END IF;

  INSERT INTO public.pay_payment_correction_requests (
    pay_batch_id,
    correction_kind,
    status,
    requested_by_user_id,
    requested_at_utc,
    required_quantity,
    approved_count,
    golden_key_used,
    golden_key_user_id,
    reason,
    selection_json,
    selection_hash,
    plan_json,
    plan_hash,
    accepted_resolution_json,
    accepted_resolution_hash,
    source_bank_event_id,
    auto_requested,
    created_at_utc,
    authorised_at_utc,
    applied_at_utc,
    cancelled_at_utc,
    updated_at_utc
  )
  VALUES (
    p_pay_batch_id,
    v_correction_kind,
    v_status,
    CASE WHEN COALESCE(p_auto_requested, false) THEN NULL::uuid ELSE p_actor_user_id END,
    now(),
    v_required_quantity,
    v_approved_count,
    false,
    NULL::uuid,
    NULLIF(btrim(COALESCE(p_reason, '')), ''),
    p_selection_json,
    v_selection_hash,
    v_plan,
    v_plan_hash,
    p_accepted_resolution_json,
    v_accepted_resolution_hash,
    p_source_bank_event_id,
    COALESCE(p_auto_requested, false),
    now(),
    CASE WHEN COALESCE(p_auto_requested, false) THEN now() ELSE NULL::timestamptz END,
    NULL::timestamptz,
    NULL::timestamptz,
    now()
  )
  RETURNING public.pay_payment_correction_requests.* INTO v_request;

  INSERT INTO public.pay_payment_correction_actions (
    correction_request_id,
    pay_batch_id,
    actor_kind,
    actor_user_id,
    action,
    action_at_utc,
    note,
    before_json,
    after_json,
    metadata_json
  )
  VALUES (
    v_request.id,
    p_pay_batch_id,
    CASE WHEN COALESCE(p_auto_requested, false) THEN 'SYSTEM' ELSE 'USER' END,
    CASE WHEN COALESCE(p_auto_requested, false) THEN NULL::uuid ELSE p_actor_user_id END,
    'REQUEST',
    now(),
    NULLIF(btrim(COALESCE(p_reason, '')), ''),
    NULL::jsonb,
    to_jsonb(v_request),
    jsonb_build_object(
      'classification', v_classification,
      'correction_kind', v_correction_kind,
      'auto_requested', COALESCE(p_auto_requested, false),
      'source_bank_event_id', p_source_bank_event_id,
      'plan_hash', v_plan_hash,
      'selection_hash', v_selection_hash,
      'accepted_resolution_hash', v_accepted_resolution_hash,
      'selected_scope', v_selected_scope_json,
      'finance_resolution_validation', v_finance_resolution_validation
    )
  );

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_REQUEST_START_CREATED',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'correction_request_id', v_request.id,
      'correction_kind', v_correction_kind,
      'status', v_status,
      'classification', v_classification,
      'auto_requested', COALESCE(p_auto_requested, false),
      'required_quantity', v_required_quantity,
      'approved_count', v_approved_count,
      'suggested_resolution_required', v_suggested_resolution_required,
      'accepted_resolution_supplied', v_accepted_resolution_supplied
    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'existing_request', false,
    'correction_request_id', v_request.id,
    'pay_batch_id', v_request.pay_batch_id,
    'correction_kind', v_request.correction_kind,
    'status', v_request.status,
    'authorisation_required', NOT COALESCE(v_request.auto_requested, false),
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'plan', v_plan,
    'selection_hash', v_selection_hash,
    'plan_hash', v_plan_hash,
    'accepted_resolution_hash', v_accepted_resolution_hash,
    'selected_scope', v_selected_scope_json,
    'finance_resolution_validation', v_finance_resolution_validation
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_REQUEST_START_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
        'source_bank_event_id', p_source_bank_event_id,
        'auto_requested', COALESCE(p_auto_requested, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
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

CREATE OR REPLACE FUNCTION public.pay_payment_correction_authorise(
  p_correction_request_id uuid,
  p_actor_user_id uuid,
  p_action text,
  p_note text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_before_request jsonb;
  v_after_request jsonb;
  v_action text;
  v_actor_exists boolean := false;
  v_actor_active boolean := false;
  v_actor_payment_authoriser boolean := false;
  v_actor_payment_golden_key boolean := false;
  v_duplicate_authorisation boolean := false;
  v_new_approved_count integer := 0;
  v_fresh_plan jsonb := '{}'::jsonb;
  v_fresh_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_fresh_plan_can_apply boolean := false;
  v_fresh_hard_blockers jsonb := '[]'::jsonb;
  v_effective_hard_blockers jsonb := '[]'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_accepted_resolution_supplied boolean := false;
  v_accepted_resolution_is_stale boolean := false;
  v_fresh_plan_material_json jsonb;
  v_fresh_plan_hash text;
  v_block_reason text;
  v_expand_result jsonb := NULL::jsonb;
  v_work_item_counts jsonb := '{}'::jsonb;
  v_plan_changed_fields jsonb := '[]'::jsonb;
  v_plan_stale_detail jsonb := '{}'::jsonb;
  v_stored_effective_blocker_codes text := '';
  v_fresh_effective_blocker_codes text := '';
  v_finance_resolution_validation jsonb := NULL::jsonb;
  v_finance_resolution_blocker jsonb := NULL::jsonb;
  v_immediate_process_result jsonb := NULL::jsonb;
  v_total_work_item_count integer := 0;
  v_pending_work_item_count integer := 0;
BEGIN
  v_action := upper(nullif(btrim(COALESCE(p_action, '')), ''));

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_AUTHORISE_BEGIN',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'actor_user_id', p_actor_user_id,
      'action', v_action
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_ID_REQUIRED',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_action NOT IN ('AUTHORISE', 'USE_GOLDEN_KEY', 'REJECT', 'CANCEL') THEN
    RAISE EXCEPTION 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION',
              'correction_request_id', p_correction_request_id,
              'action', p_action,
              'supported_actions', jsonb_build_array('AUTHORISE', 'USE_GOLDEN_KEY', 'REJECT', 'CANCEL')
            )::text;
  END IF;

  SELECT
    true,
    COALESCE(public.tms_users.is_active, false),
    COALESCE(public.tms_users.payment_authoriser, false),
    COALESCE(public.tms_users.payment_golden_key, false)
  INTO
    v_actor_exists,
    v_actor_active,
    v_actor_payment_authoriser,
    v_actor_payment_golden_key
  FROM public.tms_users
  WHERE public.tms_users.id = p_actor_user_id;

  IF NOT COALESCE(v_actor_exists, false) THEN
    RAISE EXCEPTION 'ACTOR_USER_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_NOT_FOUND',
              'actor_user_id', p_actor_user_id
            )::text;
  END IF;

  IF NOT COALESCE(v_actor_active, false) THEN
    RAISE EXCEPTION 'ACTOR_USER_INACTIVE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_INACTIVE',
              'actor_user_id', p_actor_user_id
            )::text;
  END IF;

  IF v_action = 'AUTHORISE' AND NOT COALESCE(v_actor_payment_authoriser, false) THEN
    RAISE EXCEPTION 'ACTOR_IS_NOT_PAYMENT_AUTHORISER'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_IS_NOT_PAYMENT_AUTHORISER',
              'actor_user_id', p_actor_user_id,
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_action = 'USE_GOLDEN_KEY' AND NOT COALESCE(v_actor_payment_golden_key, false) THEN
    RAISE EXCEPTION 'ACTOR_DOES_NOT_HAVE_PAYMENT_GOLDEN_KEY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_DOES_NOT_HAVE_PAYMENT_GOLDEN_KEY',
              'actor_user_id', p_actor_user_id,
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF v_request.id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ALREADY_TERMINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_ALREADY_TERMINAL',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  v_before_request := to_jsonb(v_request);

  IF v_action = 'REJECT' THEN
    UPDATE public.pay_payment_correction_requests
    SET
      status = 'REJECTED',
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id,
      pay_batch_id,
      actor_kind,
      actor_user_id,
      action,
      action_at_utc,
      note,
      before_json,
      after_json,
      metadata_json
    )
    VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'REJECT',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object('previous_status', v_before_request->>'status', 'new_status', v_request.status)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'progress', '{}'::jsonb
    );
  END IF;

  IF v_action = 'CANCEL' THEN
    UPDATE public.pay_payment_correction_work_items
    SET
      status = 'CANCELLED',
      locked_at_utc = NULL,
      locked_by = NULL,
      result_json = COALESCE(public.pay_payment_correction_work_items.result_json, '{}'::jsonb) || jsonb_build_object(
        'cancelled_by_user_id', p_actor_user_id,
        'cancelled_at_utc', now(),
        'cancel_note', NULLIF(btrim(COALESCE(p_note, '')), '')
      )
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id
      AND public.pay_payment_correction_work_items.status = 'PENDING';

    UPDATE public.pay_payment_correction_requests
    SET
      status = 'CANCELLED',
      cancelled_at_utc = COALESCE(public.pay_payment_correction_requests.cancelled_at_utc, now()),
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id,
      pay_batch_id,
      actor_kind,
      actor_user_id,
      action,
      action_at_utc,
      note,
      before_json,
      after_json,
      metadata_json
    )
    VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'CANCEL',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object('previous_status', v_before_request->>'status', 'new_status', v_request.status)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'progress', '{}'::jsonb
    );
  END IF;

  IF v_request.status NOT IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'BLOCKED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE_IN_CURRENT_STATUS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE_IN_CURRENT_STATUS',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  IF v_action = 'AUTHORISE' THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_actions AS existing_action
      WHERE existing_action.correction_request_id = p_correction_request_id
        AND existing_action.actor_user_id = p_actor_user_id
        AND existing_action.action = 'AUTHORISE'
    )
    INTO v_duplicate_authorisation;

    IF COALESCE(v_duplicate_authorisation, false) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_DUPLICATE_AUTHORISATION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_DUPLICATE_AUTHORISATION',
                'correction_request_id', p_correction_request_id,
                'actor_user_id', p_actor_user_id
              )::text;
    END IF;

    v_new_approved_count := LEAST(COALESCE(v_request.approved_count, 0) + 1, GREATEST(COALESCE(v_request.required_quantity, 1), 1));

    UPDATE public.pay_payment_correction_requests
    SET
      approved_count = v_new_approved_count,
      status = CASE
        WHEN v_new_approved_count >= GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1) THEN 'AUTHORISED'
        ELSE 'AWAITING_AUTHORISATION'
      END,
      authorised_at_utc = CASE
        WHEN v_new_approved_count >= GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1)
          THEN COALESCE(public.pay_payment_correction_requests.authorised_at_utc, now())
        ELSE public.pay_payment_correction_requests.authorised_at_utc
      END,
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id,
      pay_batch_id,
      actor_kind,
      actor_user_id,
      action,
      action_at_utc,
      note,
      before_json,
      after_json,
      metadata_json
    )
    VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'AUTHORISE',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'approved_count_before', COALESCE((v_before_request->>'approved_count')::integer, 0),
        'approved_count_after', v_request.approved_count,
        'required_quantity', v_request.required_quantity
      )
    );

    IF v_request.approved_count < GREATEST(COALESCE(v_request.required_quantity, 1), 1) THEN
      RETURN jsonb_build_object(
        'ok', true,
        'correction_request_id', v_request.id,
        'pay_batch_id', v_request.pay_batch_id,
        'action', v_action,
        'status', v_request.status,
        'approved_count', v_request.approved_count,
        'required_quantity', v_request.required_quantity,
        'authorised', false,
        'expanded', false,
        'progress', '{}'::jsonb
      );
    END IF;
  END IF;

  IF v_action = 'USE_GOLDEN_KEY' THEN
    UPDATE public.pay_payment_correction_requests
    SET
      golden_key_used = true,
      golden_key_user_id = p_actor_user_id,
      approved_count = GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1),
      status = 'AUTHORISED',
      authorised_at_utc = COALESCE(public.pay_payment_correction_requests.authorised_at_utc, now()),
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id,
      pay_batch_id,
      actor_kind,
      actor_user_id,
      action,
      action_at_utc,
      note,
      before_json,
      after_json,
      metadata_json
    )
    VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'USE_GOLDEN_KEY',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'required_quantity', v_request.required_quantity,
        'golden_key_user_id', p_actor_user_id
      )
    );
  END IF;

  v_fresh_plan := public.pay_payment_correction_plan(
    v_request.pay_batch_id,
    v_request.selection_json,
    p_actor_user_id
  );

  v_fresh_classification := COALESCE(v_fresh_plan->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
  v_fresh_plan_can_apply := COALESCE((v_fresh_plan->>'can_apply')::boolean, false);
  v_fresh_hard_blockers := COALESCE(v_fresh_plan->'hard_blockers', '[]'::jsonb);
  v_suggested_resolution_required := COALESCE((v_fresh_plan->>'suggested_resolution_required')::boolean, false);
  v_accepted_resolution_supplied := v_request.accepted_resolution_json IS NOT NULL;

  SELECT COALESCE(jsonb_agg(blocker_elements.blocker_value ORDER BY blocker_elements.blocker_ordinal), '[]'::jsonb)
  INTO v_effective_hard_blockers
  FROM jsonb_array_elements(v_fresh_hard_blockers) WITH ORDINALITY AS blocker_elements(blocker_value, blocker_ordinal)
  WHERE NOT (
    v_suggested_resolution_required
    AND v_accepted_resolution_supplied
    AND COALESCE(blocker_elements.blocker_value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
  );

  v_accepted_resolution_is_stale := v_request.accepted_resolution_json IS NOT NULL
    AND (
      lower(COALESCE(v_request.accepted_resolution_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'resolution_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,is_stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
    );

  IF v_accepted_resolution_is_stale THEN
    v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_STALE',
      'message', 'The accepted suggested finance resolution is stale and must be regenerated before correction apply.'
    ));
  END IF;

  IF v_suggested_resolution_required AND v_accepted_resolution_supplied THEN
    v_finance_resolution_validation := public._pay_payment_correction_validate_accepted_finance_resolution(
      p_pay_batch_id => v_request.pay_batch_id,
      p_selection_json => v_request.selection_json,
      p_plan_json => v_fresh_plan,
      p_accepted_resolution_json => v_request.accepted_resolution_json,
      p_actor_user_id => p_actor_user_id
    );

    IF NOT COALESCE((v_finance_resolution_validation->>'ok')::boolean, false)
       OR NOT COALESCE((v_finance_resolution_validation->>'validated')::boolean, false) THEN
      v_finance_resolution_blocker := COALESCE(
        v_finance_resolution_validation->'blocker',
        jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_VALIDATION_FAILED',
          'message', 'Accepted finance resolution validation failed before correction authorisation.'
        )
      );

      v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(v_finance_resolution_blocker);
    END IF;
  END IF;

  SELECT COALESCE(string_agg(stored_blocker_codes.blocker_code, '|' ORDER BY stored_blocker_codes.blocker_code), '')
  INTO v_stored_effective_blocker_codes
  FROM (
    SELECT DISTINCT COALESCE(NULLIF(btrim(stored_blocker.value->>'code'), ''), stored_blocker.value::text) AS blocker_code
    FROM jsonb_array_elements(COALESCE(v_request.plan_json->'hard_blockers', '[]'::jsonb)) AS stored_blocker(value)
    WHERE NOT (
      v_suggested_resolution_required
      AND v_accepted_resolution_supplied
      AND COALESCE(stored_blocker.value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
    )
  ) AS stored_blocker_codes;

  SELECT COALESCE(string_agg(fresh_blocker_codes.blocker_code, '|' ORDER BY fresh_blocker_codes.blocker_code), '')
  INTO v_fresh_effective_blocker_codes
  FROM (
    SELECT DISTINCT COALESCE(NULLIF(btrim(fresh_blocker.value->>'code'), ''), fresh_blocker.value::text) AS blocker_code
    FROM jsonb_array_elements(COALESCE(v_effective_hard_blockers, '[]'::jsonb)) AS fresh_blocker(value)
  ) AS fresh_blocker_codes;

  v_fresh_plan_material_json := jsonb_build_object(
    'classification', v_fresh_classification,
    'recommended_action', v_fresh_plan->>'recommended_action',
    'selection', COALESCE(v_fresh_plan->'selection', '{}'::jsonb),
    'amounts', COALESCE(v_fresh_plan->'amounts', '{}'::jsonb),
    'hard_blockers', v_fresh_hard_blockers,
    'effective_hard_blockers', v_effective_hard_blockers,
    'suggested_resolution_required', v_suggested_resolution_required,
    'work_expansion_plan', COALESCE(v_fresh_plan->'work_expansion_plan', '{}'::jsonb),
    'movement_classification_detail', jsonb_build_object(
      'classification', v_fresh_plan#>>'{movement_classification_detail,classification}',
      'counts', COALESCE(v_fresh_plan#>'{movement_classification_detail,counts}', '{}'::jsonb),
      'blockers', COALESCE(v_fresh_plan#>'{movement_classification_detail,blockers}', '[]'::jsonb),
      'safe_to_auto_apply', COALESCE(v_fresh_plan#>'{movement_classification_detail,safe_to_auto_apply}', 'false'::jsonb)
    )
  );

  v_fresh_plan_hash := md5(v_fresh_plan_material_json::text);

  SELECT COALESCE(jsonb_agg(plan_change.field_name ORDER BY plan_change.field_name), '[]'::jsonb)
  INTO v_plan_changed_fields
  FROM (
    VALUES
      ('amounts', COALESCE((v_request.plan_json->'amounts')::text, ''), COALESCE((v_fresh_plan->'amounts')::text, '')),
      ('classification', COALESCE(v_request.plan_json->>'classification', ''), COALESCE(v_fresh_classification, '')),
      ('effective_hard_blocker_codes', COALESCE(v_stored_effective_blocker_codes, ''), COALESCE(v_fresh_effective_blocker_codes, '')),
      ('recommended_action', COALESCE(v_request.plan_json->>'recommended_action', ''), COALESCE(v_fresh_plan->>'recommended_action', '')),
      ('selected_item_count', COALESCE(v_request.plan_json#>>'{selection,selected_item_count}', ''), COALESCE(v_fresh_plan#>>'{selection,selected_item_count}', '')),
      ('selected_selection_hash', COALESCE(v_request.plan_json#>>'{selection,selected_selection_hash}', ''), COALESCE(v_fresh_plan#>>'{selection,selected_selection_hash}', '')),
      ('suggested_resolution_required', COALESCE(v_request.plan_json->>'suggested_resolution_required', ''), COALESCE(v_suggested_resolution_required::text, '')),
      ('work_unit', COALESCE(v_request.plan_json#>>'{work_expansion_plan,work_unit}', ''), COALESCE(v_fresh_plan#>>'{work_expansion_plan,work_unit}', ''))
  ) AS plan_change(field_name, stored_value, fresh_value)
  WHERE plan_change.stored_value IS DISTINCT FROM plan_change.fresh_value;

  v_plan_stale_detail := jsonb_build_object(
    'stored_plan_hash', v_request.plan_hash,
    'fresh_plan_hash', v_fresh_plan_hash,
    'changed_fields', v_plan_changed_fields,
    'stored_summary', jsonb_build_object(
      'classification', v_request.plan_json->>'classification',
      'recommended_action', v_request.plan_json->>'recommended_action',
      'selected_item_count', v_request.plan_json#>>'{selection,selected_item_count}',
      'selected_selection_hash', v_request.plan_json#>>'{selection,selected_selection_hash}',
      'suggested_resolution_required', v_request.plan_json->>'suggested_resolution_required',
      'work_unit', v_request.plan_json#>>'{work_expansion_plan,work_unit}',
      'amounts', COALESCE(v_request.plan_json->'amounts', '{}'::jsonb),
      'hard_blockers', COALESCE(v_request.plan_json->'hard_blockers', '[]'::jsonb)
    ),
    'fresh_summary', jsonb_build_object(
      'classification', v_fresh_classification,
      'recommended_action', v_fresh_plan->>'recommended_action',
      'selected_item_count', v_fresh_plan#>>'{selection,selected_item_count}',
      'selected_selection_hash', v_fresh_plan#>>'{selection,selected_selection_hash}',
      'suggested_resolution_required', v_suggested_resolution_required,
      'work_unit', v_fresh_plan#>>'{work_expansion_plan,work_unit}',
      'amounts', COALESCE(v_fresh_plan->'amounts', '{}'::jsonb),
      'hard_blockers', v_fresh_hard_blockers,
      'effective_hard_blockers', v_effective_hard_blockers
    )
  );

  IF v_accepted_resolution_is_stale THEN
    v_block_reason := 'ACCEPTED_RESOLUTION_STALE';
  ELSIF v_finance_resolution_blocker IS NOT NULL THEN
    v_block_reason := COALESCE(NULLIF(btrim(v_finance_resolution_blocker->>'code'), ''), 'ACCEPTED_RESOLUTION_VALIDATION_FAILED');
  ELSIF jsonb_array_length(v_plan_changed_fields) > 0 THEN
    v_block_reason := 'PLAN_STALE';
  ELSIF v_fresh_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    v_block_reason := 'CLASSIFICATION_AMBIGUOUS_REVIEW_REQUIRED';
  ELSIF jsonb_array_length(v_effective_hard_blockers) > 0 THEN
    v_block_reason := 'PLAN_HAS_BLOCKERS';
  ELSIF NOT v_fresh_plan_can_apply
        AND NOT (v_suggested_resolution_required AND v_accepted_resolution_supplied AND jsonb_array_length(v_effective_hard_blockers) = 0) THEN
    v_block_reason := 'PLAN_CANNOT_APPLY';
  ELSE
    v_block_reason := NULL;
  END IF;

  IF v_block_reason IS NOT NULL THEN
    v_before_request := to_jsonb(v_request);

    UPDATE public.pay_payment_correction_requests
    SET
      status = 'BLOCKED',
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id,
      pay_batch_id,
      actor_kind,
      actor_user_id,
      action,
      action_at_utc,
      note,
      before_json,
      after_json,
      metadata_json
    )
    VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'BLOCK',
      now(),
      v_block_reason,
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'block_reason', v_block_reason,
        'stored_plan_hash', v_request.plan_hash,
        'fresh_plan_hash', v_fresh_plan_hash,
        'effective_hard_blockers', v_effective_hard_blockers,
        'fresh_classification', v_fresh_classification,
        'fresh_can_apply', v_fresh_plan_can_apply,
        'finance_resolution_validation', v_finance_resolution_validation,
        'plan_stale_detail', v_plan_stale_detail
      )
    );

    RETURN jsonb_build_object(
      'ok', false,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'block_reason', v_block_reason,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'fresh_plan', v_fresh_plan,
      'effective_hard_blockers', v_effective_hard_blockers,
      'stored_plan_hash', v_request.plan_hash,
      'fresh_plan_hash', v_fresh_plan_hash,
      'plan_changed_fields', v_plan_changed_fields,
      'plan_stale_detail', v_plan_stale_detail,
      'progress', '{}'::jsonb
    );
  END IF;

  v_expand_result := public.pay_payment_correction_expand_work(
    p_correction_request_id,
    p_actor_user_id
  );

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_AUTHORISE_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expand_result', v_expand_result,
      'immediate_process_result', v_immediate_process_result
    ),
    'pay_payment_correction',
    p_correction_request_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  SELECT jsonb_build_object(
    'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING'),
    'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING'),
    'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED'),
    'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED'),
    'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED'),
    'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE'),
    'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL'),
    'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED'),
    'total', count(*)
  )
  INTO v_work_item_counts
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  v_total_work_item_count := COALESCE((v_work_item_counts->>'total')::integer, 0);
  v_pending_work_item_count := COALESCE((v_work_item_counts->>'pending')::integer, 0);

  IF v_total_work_item_count > 0
     AND v_total_work_item_count <= 50
     AND v_pending_work_item_count > 0 THEN
    v_immediate_process_result := public.pay_payment_correction_process_chunk(
      p_correction_request_id => p_correction_request_id,
      p_limit => 50,
      p_worker_id => 'correction-authorise',
      p_actor_user_id => p_actor_user_id
    );

    SELECT jsonb_build_object(
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING'),
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING'),
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED'),
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED'),
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED'),
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE'),
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL'),
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED'),
      'total', count(*)
    )
    INTO v_work_item_counts
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id;

  RETURN jsonb_build_object(
    'ok', true,
    'correction_request_id', v_request.id,
    'pay_batch_id', v_request.pay_batch_id,
    'action', v_action,
    'status', v_request.status,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'authorised', true,
    'expanded', true,
    'expand_result', v_expand_result,
    'immediate_process_result', v_immediate_process_result,
    'progress', v_work_item_counts
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_AUTHORISE_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'actor_user_id', p_actor_user_id,
        'action', p_action,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;





CREATE OR REPLACE FUNCTION public.pay_payment_correction_expand_work(
  p_correction_request_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_now timestamptz := now();
  v_work_kind text;
  v_plan_work_unit text;
  v_effective_work_unit text;
  v_existing_work_item_count integer := 0;
  v_inserted_work_item_count integer := 0;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_finance_case_count integer := 0;
  v_progress jsonb := '{}'::jsonb;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_selected_pay_batch_item_ids jsonb := '[]'::jsonb;
  v_selected_pay_bank_transfer_ids jsonb := '[]'::jsonb;
  v_selected_finance_case_ids jsonb := '[]'::jsonb;
  v_selected_finance_component_ids jsonb := '[]'::jsonb;
  v_selected_reservation_ids jsonb := '[]'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_EXPAND_WORK_START',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  v_before_json := jsonb_build_object(
    'status', v_request.status,
    'correction_kind', v_request.correction_kind,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'plan_hash', v_request.plan_hash
  );

  IF v_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_TERMINAL_CANNOT_EXPAND_WORK'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_TERMINAL_CANNOT_EXPAND_WORK',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  IF v_request.status NOT IN ('AUTHORISED', 'EXPANDED', 'PROCESSING') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED_FOR_EXPANSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED_FOR_EXPANSION',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  SELECT count(*)::integer
  INTO v_existing_work_item_count
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  IF v_existing_work_item_count > 0 THEN
    IF v_request.status = 'AUTHORISED' THEN
      UPDATE public.pay_payment_correction_requests AS existing_work_request
      SET
        status = 'EXPANDED',
        updated_at_utc = v_now
      WHERE existing_work_request.id = p_correction_request_id
      RETURNING existing_work_request.*
      INTO v_request;
    END IF;

    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_progress
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_EXPAND_WORK_EXISTING',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'pay_batch_id', v_request.pay_batch_id,
        'status', v_request.status,
        'existing_work_item_count', v_existing_work_item_count,
        'progress', v_progress
      ),
      'pay_payment_correction',
      p_correction_request_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'already_expanded', true,
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'status', v_request.status,
      'work_kind', CASE v_request.correction_kind
        WHEN 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
        WHEN 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
        WHEN 'MANUAL_EVIDENCE_NO_MONEY' THEN 'NO_MONEY_UNWIND'
        WHEN 'SETTLED_REVERSAL' THEN 'SETTLED_REVERSAL'
        WHEN 'MANUAL_EVIDENCE_SETTLED_RETURN' THEN 'SETTLED_REVERSAL'
        ELSE NULL
      END,
      'inserted_work_item_count', 0,
      'existing_work_item_count', v_existing_work_item_count,
      'progress', v_progress
    );
  END IF;

  v_work_kind := CASE v_request.correction_kind
    WHEN 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
    WHEN 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
    WHEN 'MANUAL_EVIDENCE_NO_MONEY' THEN 'NO_MONEY_UNWIND'
    WHEN 'SETTLED_REVERSAL' THEN 'SETTLED_REVERSAL'
    WHEN 'MANUAL_EVIDENCE_SETTLED_RETURN' THEN 'SETTLED_REVERSAL'
    ELSE NULL
  END;

  IF v_work_kind IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_KIND_NOT_RESOLVED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_KIND_NOT_RESOLVED',
              'correction_request_id', p_correction_request_id,
              'correction_kind', v_request.correction_kind
            )::text;
  END IF;

  v_plan_work_unit := upper(nullif(btrim(COALESCE(v_request.plan_json->'work_expansion_plan'->>'work_unit', '')), ''));

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_expand_selected;
  CREATE TEMP TABLE _tmp_payment_correction_expand_selected ON COMMIT DROP AS
  SELECT
    expand_selected.pay_batch_id,
    expand_selected.pay_batch_candidate_id,
    expand_selected.candidate_id,
    expand_selected.candidate_display_name,
    expand_selected.pay_batch_item_id,
    expand_selected.item_type,
    expand_selected.timesheet_id,
    expand_selected.pay_bank_transfer_id,
    expand_selected.transfer_group_key,
    expand_selected.umbrella_id,
    expand_selected.finance_case_id,
    expand_selected.finance_component_id,
    expand_selected.reservation_id,
    expand_selected.amount_inc_vat
  FROM public._pay_payment_correction_selected_items(
    v_request.pay_batch_id,
    v_request.selection_json,
    false
  ) AS expand_selected;

  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (finance_case_id);

  SELECT
    count(*)::integer,
    count(DISTINCT expand_selected.pay_batch_candidate_id) FILTER (WHERE expand_selected.pay_batch_candidate_id IS NOT NULL)::integer,
    count(DISTINCT expand_selected.pay_bank_transfer_id) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT expand_selected.finance_case_id) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_finance_case_count
  FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected;

  SELECT
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb)
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_pay_bank_transfer_ids,
    v_selected_finance_case_ids,
    v_selected_finance_component_ids,
    v_selected_reservation_ids
  FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected;

  IF v_selected_item_count <= 0 THEN
    RAISE EXCEPTION 'NO_SELECTED_PAYMENT_ITEMS_FOR_WORK_EXPANSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_WORK_EXPANSION',
              'correction_request_id', p_correction_request_id,
              'pay_batch_id', v_request.pay_batch_id
            )::text;
  END IF;

  v_effective_work_unit := CASE
    WHEN v_plan_work_unit IN ('TRANSFER', 'CANDIDATE', 'CANDIDATE_TRANSFER', 'FINANCE_CASE', 'BATCH') THEN v_plan_work_unit
    WHEN v_work_kind IN ('NO_MONEY_UNWIND', 'SETTLED_REVERSAL') AND v_selected_transfer_count > 0 THEN 'TRANSFER'
    WHEN v_selected_finance_case_count > 0 THEN 'FINANCE_CASE'
    WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
    ELSE 'BATCH'
  END;

  IF v_effective_work_unit = 'TRANSFER' AND v_selected_transfer_count = 0 THEN
    v_effective_work_unit := CASE WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE' ELSE 'BATCH' END;
  END IF;

  IF v_effective_work_unit = 'FINANCE_CASE' AND v_selected_finance_case_count = 0 THEN
    v_effective_work_unit := CASE WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE' ELSE 'BATCH' END;
  END IF;

  IF v_effective_work_unit = 'CANDIDATE' AND v_selected_candidate_count = 0 THEN
    v_effective_work_unit := 'BATCH';
  END IF;

  IF v_effective_work_unit = 'CANDIDATE_TRANSFER'
     AND (v_selected_candidate_count = 0 OR v_selected_transfer_count = 0) THEN
    v_effective_work_unit := CASE
      WHEN v_selected_transfer_count > 0 THEN 'TRANSFER'
      WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
      ELSE 'BATCH'
    END;
  END IF;

  IF v_effective_work_unit = 'BATCH' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    VALUES (
      p_correction_request_id,
      v_request.pay_batch_id,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      v_work_kind,
      v_request.selection_json || jsonb_build_object(
        'work_unit', 'BATCH',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'expected_item_count', v_selected_item_count,
        'pay_bank_transfer_ids', v_selected_pay_bank_transfer_ids,
        'finance_case_ids', v_selected_finance_case_ids,
        'finance_component_ids', v_selected_finance_component_ids,
        'reservation_ids', v_selected_reservation_ids
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'work_unit', 'BATCH',
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'pay_bank_transfer_ids', v_selected_pay_bank_transfer_ids,
        'finance_case_ids', v_selected_finance_case_ids,
        'finance_component_ids', v_selected_finance_component_ids,
        'reservation_ids', v_selected_reservation_ids
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'BATCH',
        'selected_item_count', v_selected_item_count,
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids
      )
    )
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'TRANSFER' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      NULL::uuid,
      transfer_work.pay_bank_transfer_id,
      NULL::uuid,
      transfer_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'TRANSFER',
        'pay_bank_transfer_ids', jsonb_build_array(transfer_work.pay_bank_transfer_id::text),
        'pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'expected_item_count', transfer_work.item_count,
        'work_unit', 'TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'finance_case_ids', transfer_work.finance_case_ids,
        'finance_component_ids', transfer_work.finance_component_ids,
        'reservation_ids', transfer_work.reservation_ids,
        'item_count', transfer_work.item_count,
        'candidate_count', transfer_work.candidate_count,
        'amount_inc_vat', transfer_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'TRANSFER',
        'pay_bank_transfer_ids', jsonb_build_array(transfer_work.pay_bank_transfer_id::text),
        'expected_pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'finance_case_ids', transfer_work.finance_case_ids,
        'finance_component_ids', transfer_work.finance_component_ids,
        'reservation_ids', transfer_work.reservation_ids,
        'work_unit', 'TRANSFER'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'TRANSFER',
        'item_count', transfer_work.item_count,
        'candidate_count', transfer_work.candidate_count,
        'amount_inc_vat', transfer_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_bank_transfer_id,
        (array_agg(expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        count(DISTINCT expand_selected.candidate_id)::integer AS candidate_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_bank_transfer_id IS NOT NULL
      GROUP BY expand_selected.pay_bank_transfer_id
    ) AS transfer_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'FINANCE_CASE' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      finance_work.primary_pay_batch_candidate_id,
      finance_work.primary_pay_bank_transfer_id,
      finance_work.primary_candidate_id,
      finance_work.primary_umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', finance_work.pay_batch_candidate_ids,
        'finance_case_ids', jsonb_build_array(finance_work.finance_case_id::text),
        'finance_component_ids', finance_work.finance_component_ids,
        'reservation_ids', finance_work.reservation_ids,
        'pay_bank_transfer_ids', finance_work.pay_bank_transfer_ids,
        'pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'expected_item_count', finance_work.item_count,
        'work_unit', 'FINANCE_CASE',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', finance_work.item_count,
        'candidate_count', finance_work.candidate_count,
        'amount_inc_vat', finance_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'FINANCE_CASE',
        'finance_case_id', finance_work.finance_case_id::text,
        'expected_pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'finance_component_ids', finance_work.finance_component_ids,
        'reservation_ids', finance_work.reservation_ids,
        'pay_bank_transfer_ids', finance_work.pay_bank_transfer_ids,
        'work_unit', 'FINANCE_CASE'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'FINANCE_CASE',
        'finance_case_id', finance_work.finance_case_id,
        'item_count', finance_work.item_count,
        'candidate_count', finance_work.candidate_count,
        'amount_inc_vat', finance_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.finance_case_id,
        (array_agg(DISTINCT expand_selected.pay_batch_candidate_id ORDER BY expand_selected.pay_batch_candidate_id))[1] AS primary_pay_batch_candidate_id,
        (array_agg(DISTINCT expand_selected.pay_bank_transfer_id ORDER BY expand_selected.pay_bank_transfer_id NULLS LAST))[1] AS primary_pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS primary_candidate_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS primary_umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_candidate_id::text ORDER BY expand_selected.pay_batch_candidate_id::text) FILTER (WHERE expand_selected.pay_batch_candidate_id IS NOT NULL)), '[]'::jsonb) AS pay_batch_candidate_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb) AS pay_bank_transfer_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        count(DISTINCT expand_selected.candidate_id)::integer AS candidate_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.finance_case_id IS NOT NULL
      GROUP BY expand_selected.finance_case_id
    ) AS finance_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'CANDIDATE_TRANSFER' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      candidate_transfer_work.pay_batch_candidate_id,
      candidate_transfer_work.pay_bank_transfer_id,
      candidate_transfer_work.candidate_id,
      candidate_transfer_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_transfer_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', jsonb_build_array(candidate_transfer_work.pay_bank_transfer_id::text),
        'pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'expected_item_count', candidate_transfer_work.item_count,
        'finance_case_ids', candidate_transfer_work.finance_case_ids,
        'finance_component_ids', candidate_transfer_work.finance_component_ids,
        'reservation_ids', candidate_transfer_work.reservation_ids,
        'work_unit', 'CANDIDATE_TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', candidate_transfer_work.item_count,
        'amount_inc_vat', candidate_transfer_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'CANDIDATE_TRANSFER',
        'pay_batch_candidate_id', candidate_transfer_work.pay_batch_candidate_id::text,
        'pay_bank_transfer_id', candidate_transfer_work.pay_bank_transfer_id::text,
        'expected_pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'finance_case_ids', candidate_transfer_work.finance_case_ids,
        'finance_component_ids', candidate_transfer_work.finance_component_ids,
        'reservation_ids', candidate_transfer_work.reservation_ids,
        'work_unit', 'CANDIDATE_TRANSFER'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'CANDIDATE_TRANSFER',
        'item_count', candidate_transfer_work.item_count,
        'amount_inc_vat', candidate_transfer_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_batch_candidate_id,
        expand_selected.pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS candidate_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_batch_candidate_id IS NOT NULL
        AND expand_selected.pay_bank_transfer_id IS NOT NULL
      GROUP BY expand_selected.pay_batch_candidate_id, expand_selected.pay_bank_transfer_id
    ) AS candidate_transfer_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSE
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      candidate_work.pay_batch_candidate_id,
      candidate_work.primary_pay_bank_transfer_id,
      candidate_work.candidate_id,
      candidate_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', candidate_work.pay_bank_transfer_ids,
        'pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'expected_item_count', candidate_work.item_count,
        'finance_case_ids', candidate_work.finance_case_ids,
        'finance_component_ids', candidate_work.finance_component_ids,
        'reservation_ids', candidate_work.reservation_ids,
        'work_unit', 'CANDIDATE',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', candidate_work.item_count,
        'amount_inc_vat', candidate_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', candidate_work.pay_bank_transfer_ids,
        'expected_pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'finance_case_ids', candidate_work.finance_case_ids,
        'finance_component_ids', candidate_work.finance_component_ids,
        'reservation_ids', candidate_work.reservation_ids,
        'work_unit', 'CANDIDATE'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'CANDIDATE',
        'item_count', candidate_work.item_count,
        'amount_inc_vat', candidate_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_batch_candidate_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS candidate_id,
        (array_agg(DISTINCT expand_selected.pay_bank_transfer_id ORDER BY expand_selected.pay_bank_transfer_id NULLS LAST))[1] AS primary_pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb) AS pay_bank_transfer_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_batch_candidate_id IS NOT NULL
      GROUP BY expand_selected.pay_batch_candidate_id
    ) AS candidate_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  END IF;

  GET DIAGNOSTICS v_inserted_work_item_count = ROW_COUNT;

  UPDATE public.pay_payment_correction_requests AS expanded_request
  SET
    status = 'EXPANDED',
    updated_at_utc = v_now
  WHERE expanded_request.id = p_correction_request_id
  RETURNING expanded_request.*
  INTO v_request;

  v_after_json := jsonb_build_object(
    'status', v_request.status,
    'correction_kind', v_request.correction_kind,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'work_kind', v_work_kind,
    'work_unit', v_effective_work_unit,
    'inserted_work_item_count', v_inserted_work_item_count
  );

  INSERT INTO public.pay_payment_correction_actions(
    correction_request_id,
    pay_batch_id,
    actor_kind,
    actor_user_id,
    action,
    action_at_utc,
    note,
    before_json,
    after_json,
    metadata_json
  )
  VALUES (
    p_correction_request_id,
    v_request.pay_batch_id,
    CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
    p_actor_user_id,
    'EXPAND_WORK',
    v_now,
    NULL::text,
    v_before_json,
    v_after_json,
    jsonb_build_object(
      'work_kind', v_work_kind,
      'plan_work_unit', v_plan_work_unit,
      'effective_work_unit', v_effective_work_unit,
      'selected_item_count', v_selected_item_count,
      'selected_candidate_count', v_selected_candidate_count,
      'selected_transfer_count', v_selected_transfer_count,
      'selected_finance_case_count', v_selected_finance_case_count,
      'inserted_work_item_count', v_inserted_work_item_count
    )
  );

  SELECT jsonb_build_object(
    'total', count(*)::integer,
    'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
    'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
    'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
    'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
    'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
    'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
    'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
    'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
  )
  INTO v_progress
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_EXPAND_WORK_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'status', v_request.status,
      'work_kind', v_work_kind,
      'plan_work_unit', v_plan_work_unit,
      'effective_work_unit', v_effective_work_unit,
      'inserted_work_item_count', v_inserted_work_item_count,
      'progress', v_progress
    ),
    'pay_payment_correction',
    p_correction_request_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'already_expanded', false,
    'correction_request_id', p_correction_request_id,
    'pay_batch_id', v_request.pay_batch_id,
    'status', v_request.status,
    'work_kind', v_work_kind,
    'plan_work_unit', v_plan_work_unit,
    'effective_work_unit', v_effective_work_unit,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'selected_finance_case_count', v_selected_finance_case_count,
    'inserted_work_item_count', v_inserted_work_item_count,
    'progress', v_progress
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_EXPAND_WORK_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;






DROP FUNCTION IF EXISTS public.pay_payment_correction_process_chunk(uuid, integer, text);

CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk(
  p_correction_request_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 50,
  p_worker_id text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 100);
  v_worker_id text := COALESCE(NULLIF(btrim(COALESCE(p_worker_id, '')), ''), 'payment-correction-worker');
  v_now timestamptz := now();
  v_claimed_count integer := 0;
  v_processed_count integer := 0;
  v_applied_count integer := 0;
  v_blocked_count integer := 0;
  v_failed_retryable_count integer := 0;
  v_failed_final_count integer := 0;
  v_skipped_count integer := 0;
  v_parent_status text := NULL;
  v_parent_request_ids uuid[] := ARRAY[]::uuid[];
  v_totals jsonb := '{}'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_result_ok boolean := false;
  v_result_status text := NULL;
  v_failure_status text := NULL;
  v_work_row record;
  v_request_status public.pay_payment_correction_requests.status%TYPE;
  v_work_selection_json jsonb := '{}'::jsonb;
  v_expected_item_count integer := NULL;
  v_expected_item_ids jsonb := '[]'::jsonb;
  v_resolved_item_count integer := 0;
  v_resolved_item_ids jsonb := '[]'::jsonb;
  v_selection_drift boolean := false;
  v_selection_drift_result jsonb := '{}'::jsonb;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_processing_actor_kind text := 'SYSTEM';
  v_source_bank_event_id uuid := NULL::uuid;
  v_source_event_disposition text := NULL::text;
  v_source_event_update jsonb := NULL::jsonb;
  v_source_event_updates jsonb := '[]'::jsonb;
  v_requires_user_action boolean := false;
  v_processing_continues boolean := false;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PROCESS_CHUNK_START',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'requested_limit', p_limit,
      'effective_limit', v_limit,
      'worker_id', v_worker_id,
      'actor_user_id', p_actor_user_id,
      'actor_supplied', p_actor_user_id IS NOT NULL
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_limit IS NOT NULL AND p_limit < 1 THEN
    v_limit := 1;
  END IF;

  IF p_limit IS NOT NULL AND p_limit > 100 THEN
    v_limit := 100;
  END IF;

  IF p_correction_request_id IS NOT NULL THEN
    SELECT public.pay_payment_correction_requests.status
    INTO v_request_status
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
                'correction_request_id', p_correction_request_id
              )::text;
    END IF;

    IF v_request_status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
      SELECT jsonb_build_object(
        'total', count(*)::integer,
        'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
        'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
        'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
        'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
        'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
        'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
        'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
        'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
      )
      INTO v_totals
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;


      DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
      CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
      WITH request_totals AS (
        SELECT
          public.pay_payment_correction_requests.id AS correction_request_id,
          public.pay_payment_correction_requests.source_bank_event_id,
          public.pay_payment_correction_requests.status::text AS parent_status,
          COALESCE((v_totals->>'total')::integer, 0) AS total_count,
          COALESCE((v_totals->>'pending')::integer, 0) AS pending_count,
          COALESCE((v_totals->>'processing')::integer, 0) AS processing_count,
          COALESCE((v_totals->>'applied')::integer, 0) AS applied_count,
          COALESCE((v_totals->>'skipped')::integer, 0) AS skipped_count,
          COALESCE((v_totals->>'blocked')::integer, 0) AS blocked_count,
          COALESCE((v_totals->>'failed_retryable')::integer, 0) AS failed_retryable_count,
          COALESCE((v_totals->>'failed_final')::integer, 0) AS failed_final_count,
          COALESCE((v_totals->>'cancelled')::integer, 0) AS cancelled_count
        FROM public.pay_payment_correction_requests
        WHERE public.pay_payment_correction_requests.id = p_correction_request_id
          AND public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
          AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      ), derived_event_status AS (
        SELECT
          request_totals.correction_request_id,
          request_totals.source_bank_event_id,
          request_totals.parent_status,
          request_totals.total_count,
          request_totals.pending_count,
          request_totals.processing_count,
          request_totals.applied_count,
          request_totals.skipped_count,
          request_totals.blocked_count,
          request_totals.failed_retryable_count,
          request_totals.failed_final_count,
          request_totals.cancelled_count,
          CASE
            WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
            WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
            WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
            WHEN request_totals.parent_status = 'APPLIED'
                 AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
            WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
                 AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            ELSE 'FAILED'
          END AS correction_disposition,
          jsonb_build_object(
            'total', COALESCE(request_totals.total_count, 0),
            'pending', COALESCE(request_totals.pending_count, 0),
            'processing', COALESCE(request_totals.processing_count, 0),
            'applied', COALESCE(request_totals.applied_count, 0),
            'skipped', COALESCE(request_totals.skipped_count, 0),
            'blocked', COALESCE(request_totals.blocked_count, 0),
            'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
            'failed_final', COALESCE(request_totals.failed_final_count, 0),
            'cancelled', COALESCE(request_totals.cancelled_count, 0)
          ) AS totals_json
        FROM request_totals
      ), updated_events AS (
        UPDATE public.pay_bank_transfer_events AS bank_event_to_update
        SET
          correction_disposition = derived_event_status.correction_disposition
        FROM derived_event_status
        WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
        RETURNING
          derived_event_status.correction_request_id,
          bank_event_to_update.id AS source_bank_event_id,
          derived_event_status.correction_disposition,
          derived_event_status.parent_status,
          derived_event_status.totals_json,
          (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
          (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
      )
      SELECT
        updated_events.correction_request_id,
        updated_events.source_bank_event_id,
        updated_events.correction_disposition,
        updated_events.parent_status,
        updated_events.totals_json,
        updated_events.requires_user_action,
        updated_events.processing_continues
      FROM updated_events;

      SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
      INTO v_source_event_updates
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

      SELECT
        jsonb_build_object(
          'correction_request_id', source_event_updates.correction_request_id,
          'event_id', source_event_updates.source_bank_event_id,
          'correction_disposition', source_event_updates.correction_disposition,
          'parent_status', source_event_updates.parent_status,
          'totals', source_event_updates.totals_json,
          'requires_user_action', source_event_updates.requires_user_action,
          'processing_continues', source_event_updates.processing_continues
        ),
        source_event_updates.requires_user_action,
        source_event_updates.processing_continues
      INTO
        v_source_event_update,
        v_requires_user_action,
        v_processing_continues
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
      ORDER BY source_event_updates.correction_request_id
      LIMIT 1;

      RETURN jsonb_build_object(
        'ok', true,
        'processed', 0,
        'applied', 0,
        'blocked', 0,
        'failed_retryable', 0,
        'failed_final', 0,
        'parent_status', v_request_status,
        'totals', v_totals,
        'requires_user_action', COALESCE(v_requires_user_action, false),
        'processing_continues', COALESCE(v_processing_continues, false),
        'source_bank_event_update', v_source_event_update,
        'source_bank_event_updates', v_source_event_updates,
        'terminal_request', true,
        'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
        'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
      );
    END IF;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_claimed_work;
  CREATE TEMP TABLE _tmp_payment_correction_claimed_work ON COMMIT DROP AS
  WITH claimable_work AS (
    SELECT claimable_items.id
    FROM public.pay_payment_correction_work_items AS claimable_items
    JOIN public.pay_payment_correction_requests AS claimable_requests
      ON claimable_requests.id = claimable_items.correction_request_id
    WHERE claimable_items.status IN ('PENDING', 'FAILED_RETRYABLE')
      AND claimable_requests.status IN ('AUTHORISED', 'EXPANDED', 'PROCESSING')
      AND (p_correction_request_id IS NULL OR claimable_items.correction_request_id = p_correction_request_id)
    ORDER BY
      claimable_items.created_at_utc,
      claimable_items.id
    FOR UPDATE OF claimable_items SKIP LOCKED
    LIMIT v_limit
  ),
  updated_work AS (
    UPDATE public.pay_payment_correction_work_items AS work_to_claim
    SET
      status = 'PROCESSING',
      attempt_count = COALESCE(work_to_claim.attempt_count, 0) + 1,
      locked_at_utc = v_now,
      locked_by = v_worker_id,
      processed_by_user_id = COALESCE(p_actor_user_id, work_to_claim.processed_by_user_id),
      result_json = COALESCE(work_to_claim.result_json, '{}'::jsonb) || jsonb_build_object(
        'claimed_at_utc', v_now,
        'claimed_by', v_worker_id,
        'previous_status', work_to_claim.status,
        'claim_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
      )
    FROM claimable_work
    WHERE work_to_claim.id = claimable_work.id
    RETURNING
      work_to_claim.id,
      work_to_claim.correction_request_id,
      work_to_claim.pay_batch_id,
      work_to_claim.work_kind,
      work_to_claim.attempt_count,
      work_to_claim.selection_json
  )
  SELECT
    updated_work.id,
    updated_work.correction_request_id,
    updated_work.pay_batch_id,
    updated_work.work_kind,
    updated_work.attempt_count,
    updated_work.selection_json
  FROM updated_work;

  SELECT count(*)::integer
  INTO v_claimed_count
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_count;

  SELECT COALESCE(array_agg(DISTINCT claimed_work_request_ids.correction_request_id), ARRAY[]::uuid[])
  INTO v_parent_request_ids
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_request_ids;

  IF v_claimed_count = 0 THEN
    IF p_correction_request_id IS NOT NULL THEN
      SELECT jsonb_build_object(
        'total', count(*)::integer,
        'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
        'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
        'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
        'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
        'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
        'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
        'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
        'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
      )
      INTO v_totals
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

      SELECT public.pay_payment_correction_requests.status
      INTO v_parent_status
      FROM public.pay_payment_correction_requests
      WHERE public.pay_payment_correction_requests.id = p_correction_request_id;
    ELSE
      v_totals := '{}'::jsonb;
      v_parent_status := NULL;
    END IF;

    IF p_correction_request_id IS NOT NULL THEN

      DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
      CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
      WITH request_totals AS (
        SELECT
          public.pay_payment_correction_requests.id AS correction_request_id,
          public.pay_payment_correction_requests.source_bank_event_id,
          public.pay_payment_correction_requests.status::text AS parent_status,
          COALESCE((v_totals->>'total')::integer, 0) AS total_count,
          COALESCE((v_totals->>'pending')::integer, 0) AS pending_count,
          COALESCE((v_totals->>'processing')::integer, 0) AS processing_count,
          COALESCE((v_totals->>'applied')::integer, 0) AS applied_count,
          COALESCE((v_totals->>'skipped')::integer, 0) AS skipped_count,
          COALESCE((v_totals->>'blocked')::integer, 0) AS blocked_count,
          COALESCE((v_totals->>'failed_retryable')::integer, 0) AS failed_retryable_count,
          COALESCE((v_totals->>'failed_final')::integer, 0) AS failed_final_count,
          COALESCE((v_totals->>'cancelled')::integer, 0) AS cancelled_count
        FROM public.pay_payment_correction_requests
        WHERE public.pay_payment_correction_requests.id = p_correction_request_id
          AND public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
          AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      ), derived_event_status AS (
        SELECT
          request_totals.correction_request_id,
          request_totals.source_bank_event_id,
          request_totals.parent_status,
          request_totals.total_count,
          request_totals.pending_count,
          request_totals.processing_count,
          request_totals.applied_count,
          request_totals.skipped_count,
          request_totals.blocked_count,
          request_totals.failed_retryable_count,
          request_totals.failed_final_count,
          request_totals.cancelled_count,
          CASE
            WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
            WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
            WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
            WHEN request_totals.parent_status = 'APPLIED'
                 AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
            WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
                 AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            ELSE 'FAILED'
          END AS correction_disposition,
          jsonb_build_object(
            'total', COALESCE(request_totals.total_count, 0),
            'pending', COALESCE(request_totals.pending_count, 0),
            'processing', COALESCE(request_totals.processing_count, 0),
            'applied', COALESCE(request_totals.applied_count, 0),
            'skipped', COALESCE(request_totals.skipped_count, 0),
            'blocked', COALESCE(request_totals.blocked_count, 0),
            'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
            'failed_final', COALESCE(request_totals.failed_final_count, 0),
            'cancelled', COALESCE(request_totals.cancelled_count, 0)
          ) AS totals_json
        FROM request_totals
      ), updated_events AS (
        UPDATE public.pay_bank_transfer_events AS bank_event_to_update
        SET
          correction_disposition = derived_event_status.correction_disposition
        FROM derived_event_status
        WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
        RETURNING
          derived_event_status.correction_request_id,
          bank_event_to_update.id AS source_bank_event_id,
          derived_event_status.correction_disposition,
          derived_event_status.parent_status,
          derived_event_status.totals_json,
          (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
          (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
      )
      SELECT
        updated_events.correction_request_id,
        updated_events.source_bank_event_id,
        updated_events.correction_disposition,
        updated_events.parent_status,
        updated_events.totals_json,
        updated_events.requires_user_action,
        updated_events.processing_continues
      FROM updated_events;

      SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
      INTO v_source_event_updates
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

      SELECT
        jsonb_build_object(
          'correction_request_id', source_event_updates.correction_request_id,
          'event_id', source_event_updates.source_bank_event_id,
          'correction_disposition', source_event_updates.correction_disposition,
          'parent_status', source_event_updates.parent_status,
          'totals', source_event_updates.totals_json,
          'requires_user_action', source_event_updates.requires_user_action,
          'processing_continues', source_event_updates.processing_continues
        ),
        source_event_updates.requires_user_action,
        source_event_updates.processing_continues
      INTO
        v_source_event_update,
        v_requires_user_action,
        v_processing_continues
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
      ORDER BY source_event_updates.correction_request_id
      LIMIT 1;

    END IF;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PROCESS_CHUNK_NO_WORK',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'effective_limit', v_limit,
        'worker_id', v_worker_id,
        'parent_status', v_parent_status,
        'totals', v_totals
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'processed', 0,
      'applied', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'parent_status', v_parent_status,
      'totals', v_totals,
      'requires_user_action', COALESCE(v_requires_user_action, false),
      'processing_continues', COALESCE(v_processing_continues, false),
      'source_bank_event_update', v_source_event_update,
      'source_bank_event_updates', v_source_event_updates,
      'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
      'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    );
  END IF;

  FOR v_work_row IN
    SELECT
      claimed_work.id,
      claimed_work.correction_request_id,
      claimed_work.pay_batch_id,
      claimed_work.work_kind,
      claimed_work.attempt_count,
      claimed_work.selection_json
    FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work
    ORDER BY claimed_work.id
  LOOP
    v_processed_count := v_processed_count + 1;
    v_result := '{}'::jsonb;
    v_result_ok := false;
    v_result_status := NULL;
    v_work_selection_json := COALESCE(v_work_row.selection_json, '{}'::jsonb);
    v_expected_item_count := CASE
      WHEN COALESCE(v_work_selection_json->>'expected_item_count', '') ~ '^[0-9]+$'
        THEN (v_work_selection_json->>'expected_item_count')::integer
      ELSE NULL::integer
    END;
    v_expected_item_ids := CASE
      WHEN jsonb_typeof(v_work_selection_json->'expected_pay_batch_item_ids') = 'array'
        THEN COALESCE(v_work_selection_json->'expected_pay_batch_item_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_work_selection_json->'pay_batch_item_ids') = 'array'
        THEN COALESCE(v_work_selection_json->'pay_batch_item_ids', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    SELECT COALESCE(jsonb_agg(DISTINCT expected_item_values.expected_item_id ORDER BY expected_item_values.expected_item_id), '[]'::jsonb)
    INTO v_expected_item_ids
    FROM jsonb_array_elements_text(v_expected_item_ids) AS expected_item_values(expected_item_id);

    SELECT
      COALESCE(p_actor_user_id, latest_authorising_action.actor_user_id, CASE WHEN COALESCE(parent_request.auto_requested, false) THEN NULL::uuid ELSE parent_request.requested_by_user_id END),
      CASE
        WHEN COALESCE(p_actor_user_id, latest_authorising_action.actor_user_id, CASE WHEN COALESCE(parent_request.auto_requested, false) THEN NULL::uuid ELSE parent_request.requested_by_user_id END) IS NULL THEN 'SYSTEM'
        ELSE 'USER'
      END
    INTO
      v_effective_actor_user_id,
      v_processing_actor_kind
    FROM public.pay_payment_correction_requests AS parent_request
    LEFT JOIN LATERAL (
      SELECT authorising_actions.actor_user_id
      FROM public.pay_payment_correction_actions AS authorising_actions
      WHERE authorising_actions.correction_request_id = parent_request.id
        AND authorising_actions.action IN ('AUTHORISE', 'USE_GOLDEN_KEY')
        AND authorising_actions.actor_user_id IS NOT NULL
      ORDER BY authorising_actions.action_at_utc DESC, authorising_actions.id DESC
      LIMIT 1
    ) AS latest_authorising_action ON true
    WHERE parent_request.id = v_work_row.correction_request_id;

    SELECT
      count(*)::integer,
      COALESCE(jsonb_agg(DISTINCT resolved_items.pay_batch_item_id::text ORDER BY resolved_items.pay_batch_item_id::text), '[]'::jsonb)
    INTO
      v_resolved_item_count,
      v_resolved_item_ids
    FROM public._pay_payment_correction_selected_items(
      v_work_row.pay_batch_id,
      v_work_selection_json,
      false
    ) AS resolved_items;

    v_selection_drift := (
      (v_expected_item_count IS NOT NULL AND v_expected_item_count <> v_resolved_item_count)
      OR (jsonb_array_length(v_expected_item_ids) > 0 AND v_expected_item_ids <> v_resolved_item_ids)
    );

    IF v_selection_drift THEN
      v_selection_drift_result := jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', jsonb_build_object(
          'code', 'WORK_SELECTION_DRIFT',
          'message', 'The correction work item selection no longer resolves to the expected selected pay_batch_items.',
          'expected_item_count', v_expected_item_count,
          'resolved_item_count', v_resolved_item_count,
          'expected_pay_batch_item_ids', v_expected_item_ids,
          'resolved_pay_batch_item_ids', v_resolved_item_ids
        ),
        'processed_by_chunk', true,
        'processed_worker_id', v_worker_id,
        'processing_actor_kind', v_processing_actor_kind,
        'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
        'processed_at_utc', now()
      );

      UPDATE public.pay_payment_correction_work_items AS selection_drift_work
      SET
        status = 'BLOCKED',
        processed_at_utc = COALESCE(selection_drift_work.processed_at_utc, now()),
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_by_user_id = COALESCE(v_effective_actor_user_id, selection_drift_work.processed_by_user_id),
        last_error = 'WORK_SELECTION_DRIFT',
        result_json = COALESCE(selection_drift_work.result_json, '{}'::jsonb) || v_selection_drift_result
      WHERE selection_drift_work.id = v_work_row.id;

      v_blocked_count := v_blocked_count + 1;
      CONTINUE;
    END IF;

    BEGIN
      IF v_work_row.work_kind = 'PRE_BANK_CANCEL' THEN
        v_result := public.pay_pre_bank_cancel_apply_work_item(v_work_row.id, v_effective_actor_user_id);
      ELSIF v_work_row.work_kind = 'NO_MONEY_UNWIND' THEN
        v_result := public.pay_no_money_unwind_apply_work_item(v_work_row.id, v_effective_actor_user_id);
      ELSIF v_work_row.work_kind = 'SETTLED_REVERSAL' THEN
        v_result := public.pay_settled_payment_reversal_apply_work_item(v_work_row.id, v_effective_actor_user_id);
      ELSE
        v_result := jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', jsonb_build_object(
            'code', 'UNKNOWN_WORK_KIND',
            'message', 'Unknown payment correction work kind.',
            'work_kind', v_work_row.work_kind
          )
        );
      END IF;

      v_result_ok := COALESCE((v_result->>'ok')::boolean, false);
      v_result_status := upper(nullif(btrim(COALESCE(v_result->>'status', '')), ''));

      IF v_result_ok THEN
        UPDATE public.pay_payment_correction_work_items AS processed_work_success
        SET
          status = 'APPLIED',
          processed_at_utc = COALESCE(processed_work_success.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, processed_work_success.processed_by_user_id),
          result_json = COALESCE(processed_work_success.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'processed_at_utc', now()
          )
        WHERE processed_work_success.id = v_work_row.id;

        v_applied_count := v_applied_count + 1;
      ELSIF v_result_status = 'SKIPPED' THEN
        UPDATE public.pay_payment_correction_work_items AS processed_work_skipped
        SET
          status = 'SKIPPED',
          processed_at_utc = COALESCE(processed_work_skipped.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, processed_work_skipped.processed_by_user_id),
          result_json = COALESCE(processed_work_skipped.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'processed_at_utc', now()
          )
        WHERE processed_work_skipped.id = v_work_row.id;

        v_skipped_count := v_skipped_count + 1;
      ELSE
        UPDATE public.pay_payment_correction_work_items AS processed_work_blocked
        SET
          status = 'BLOCKED',
          processed_at_utc = COALESCE(processed_work_blocked.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          last_error = COALESCE(v_result->>'error_message', v_result->>'message', v_result#>>'{blocker,message}', 'Payment correction work item blocked.'),
          result_json = COALESCE(processed_work_blocked.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processed_at_utc', now()
          )
        WHERE processed_work_blocked.id = v_work_row.id;

        v_blocked_count := v_blocked_count + 1;
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        v_failure_status := CASE
          WHEN SQLSTATE IN ('40001', '40P01', '55P03', '57014') THEN 'FAILED_RETRYABLE'
          WHEN SQLSTATE = 'P0001'
            AND (
              upper(SQLERRM) LIKE '%BLOCK%'
              OR upper(SQLERRM) LIKE '%STALE%'
              OR upper(SQLERRM) LIKE '%AMBIGUOUS%'
              OR upper(SQLERRM) LIKE '%NOT_SAFE%'
              OR upper(SQLERRM) LIKE '%CLASSIFICATION%'
              OR upper(SQLERRM) LIKE '%SETTLED%'
              OR upper(SQLERRM) LIKE '%AUTHORIS%'
              OR upper(SQLERRM) LIKE '%SELECTION%'
              OR upper(SQLERRM) LIKE '%DRIFT%'
              OR upper(SQLERRM) LIKE '%SCOPE%'
            ) THEN 'BLOCKED'
          ELSE 'FAILED_FINAL'
        END;

        UPDATE public.pay_payment_correction_work_items AS failed_work_item
        SET
          status = v_failure_status,
          locked_at_utc = NULL,
          locked_by = NULL,
          last_error = SQLERRM,
          processed_at_utc = CASE WHEN v_failure_status = 'FAILED_RETRYABLE' THEN NULL ELSE now() END,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, failed_work_item.processed_by_user_id),
          result_json = COALESCE(failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', v_failure_status,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM,
            'failed_at_utc', now(),
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'attempt_count', v_work_row.attempt_count
          )
        WHERE failed_work_item.id = v_work_row.id;

        IF v_failure_status = 'BLOCKED' THEN
          v_blocked_count := v_blocked_count + 1;
        ELSIF v_failure_status = 'FAILED_RETRYABLE' THEN
          v_failed_retryable_count := v_failed_retryable_count + 1;
        ELSE
          v_failed_final_count := v_failed_final_count + 1;
        END IF;

        PERFORM public._imp_debug_audit(
          v_effective_actor_user_id,
          'PAYMENT_CORRECTION_PROCESS_CHUNK_WORK_ITEM_ERROR',
          jsonb_build_object(
            'work_item_id', v_work_row.id,
            'correction_request_id', v_work_row.correction_request_id,
            'pay_batch_id', v_work_row.pay_batch_id,
            'work_kind', v_work_row.work_kind,
            'failure_status', v_failure_status,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM,
            'attempt_count', v_work_row.attempt_count
          ),
          'pay_payment_correction',
          v_work_row.id::text,
          NULL::jsonb,
          NULL::text,
          NULL::text,
          NULL::text
        );
    END;
  END LOOP;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_request_statuses;
  CREATE TEMP TABLE _tmp_payment_correction_request_statuses ON COMMIT DROP AS
  SELECT DISTINCT claimed_request_ids.correction_request_id
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_request_ids;

  UPDATE public.pay_payment_correction_requests AS processing_request
  SET
    status = derived_request_status.derived_status,
    applied_at_utc = CASE
      WHEN derived_request_status.derived_status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS')
        THEN COALESCE(processing_request.applied_at_utc, now())
      ELSE processing_request.applied_at_utc
    END,
    updated_at_utc = now()
  FROM (
    SELECT
      request_statuses.correction_request_id,
      CASE
        WHEN work_totals.total_count = 0 THEN 'EXPANDED'
        WHEN work_totals.cancelled_count = work_totals.total_count THEN 'CANCELLED'
        WHEN work_totals.applied_count + work_totals.skipped_count = work_totals.total_count THEN 'APPLIED'
        WHEN work_totals.failed_final_count = work_totals.total_count THEN 'FAILED'
        WHEN work_totals.pending_count + work_totals.processing_count + work_totals.failed_retryable_count > 0 THEN 'PROCESSING'
        WHEN work_totals.blocked_count + work_totals.failed_final_count > 0 THEN 'APPLIED_WITH_BLOCKERS'
        ELSE 'PROCESSING'
      END AS derived_status
    FROM pg_temp._tmp_payment_correction_request_statuses AS request_statuses
    CROSS JOIN LATERAL (
      SELECT
        count(*)::integer AS total_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer AS pending_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer AS processing_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer AS applied_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer AS skipped_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer AS blocked_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer AS failed_retryable_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer AS failed_final_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer AS cancelled_count
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = request_statuses.correction_request_id
    ) AS work_totals
  ) AS derived_request_status
  WHERE processing_request.id = derived_request_status.correction_request_id
    AND processing_request.status NOT IN ('REJECTED', 'CANCELLED', 'APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED');

  IF p_correction_request_id IS NOT NULL THEN
    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_totals
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

    SELECT public.pay_payment_correction_requests.status
    INTO v_parent_status
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id;
  ELSE
    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_totals
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.id IN (
      SELECT claimed_work_ids.id
      FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_ids
    );

    v_parent_status := NULL;
  END IF;


  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
  CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
  WITH request_scope AS (
    SELECT public.pay_payment_correction_requests.id AS correction_request_id
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
      AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      AND (
        (p_correction_request_id IS NOT NULL AND public.pay_payment_correction_requests.id = p_correction_request_id)
        OR (
          p_correction_request_id IS NULL
          AND public.pay_payment_correction_requests.id = ANY(v_parent_request_ids)
        )
      )
  ), request_totals AS (
    SELECT
      public.pay_payment_correction_requests.id AS correction_request_id,
      public.pay_payment_correction_requests.source_bank_event_id,
      public.pay_payment_correction_requests.status::text AS parent_status,
      count(public.pay_payment_correction_work_items.id)::integer AS total_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer AS pending_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer AS processing_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer AS applied_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer AS skipped_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer AS blocked_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer AS failed_retryable_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer AS failed_final_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer AS cancelled_count
    FROM request_scope
    JOIN public.pay_payment_correction_requests
      ON public.pay_payment_correction_requests.id = request_scope.correction_request_id
    LEFT JOIN public.pay_payment_correction_work_items
      ON public.pay_payment_correction_work_items.correction_request_id = public.pay_payment_correction_requests.id
    GROUP BY
      public.pay_payment_correction_requests.id,
      public.pay_payment_correction_requests.source_bank_event_id,
      public.pay_payment_correction_requests.status
  ), derived_event_status AS (
    SELECT
      request_totals.correction_request_id,
      request_totals.source_bank_event_id,
      request_totals.parent_status,
      request_totals.total_count,
      request_totals.pending_count,
      request_totals.processing_count,
      request_totals.applied_count,
      request_totals.skipped_count,
      request_totals.blocked_count,
      request_totals.failed_retryable_count,
      request_totals.failed_final_count,
      request_totals.cancelled_count,
      CASE
        WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
        WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
        WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
        WHEN request_totals.parent_status = 'APPLIED'
             AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
        WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
             AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
             AND COALESCE(request_totals.blocked_count, 0) = 0
             AND COALESCE(request_totals.failed_retryable_count, 0) = 0
             AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
        WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
             AND COALESCE(request_totals.blocked_count, 0) = 0
             AND COALESCE(request_totals.failed_retryable_count, 0) = 0
             AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
        ELSE 'FAILED'
      END AS correction_disposition,
      jsonb_build_object(
        'total', COALESCE(request_totals.total_count, 0),
        'pending', COALESCE(request_totals.pending_count, 0),
        'processing', COALESCE(request_totals.processing_count, 0),
        'applied', COALESCE(request_totals.applied_count, 0),
        'skipped', COALESCE(request_totals.skipped_count, 0),
        'blocked', COALESCE(request_totals.blocked_count, 0),
        'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
        'failed_final', COALESCE(request_totals.failed_final_count, 0),
        'cancelled', COALESCE(request_totals.cancelled_count, 0)
      ) AS totals_json
    FROM request_totals
  ), updated_events AS (
    UPDATE public.pay_bank_transfer_events AS bank_event_to_update
    SET
      correction_disposition = derived_event_status.correction_disposition
    FROM derived_event_status
    WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
    RETURNING
      derived_event_status.correction_request_id,
      bank_event_to_update.id AS source_bank_event_id,
      derived_event_status.correction_disposition,
      derived_event_status.parent_status,
      derived_event_status.totals_json,
      (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
      (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
  )
  SELECT
    updated_events.correction_request_id,
    updated_events.source_bank_event_id,
    updated_events.correction_disposition,
    updated_events.parent_status,
    updated_events.totals_json,
    updated_events.requires_user_action,
    updated_events.processing_continues
  FROM updated_events;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'correction_request_id', source_event_updates.correction_request_id,
    'event_id', source_event_updates.source_bank_event_id,
    'correction_disposition', source_event_updates.correction_disposition,
    'parent_status', source_event_updates.parent_status,
    'totals', source_event_updates.totals_json,
    'requires_user_action', source_event_updates.requires_user_action,
    'processing_continues', source_event_updates.processing_continues
  ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
  INTO v_source_event_updates
  FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

  IF p_correction_request_id IS NOT NULL THEN
    SELECT
      jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ),
      source_event_updates.source_bank_event_id,
      source_event_updates.correction_disposition,
      source_event_updates.requires_user_action,
      source_event_updates.processing_continues
    INTO
      v_source_event_update,
      v_source_bank_event_id,
      v_source_event_disposition,
      v_requires_user_action,
      v_processing_continues
    FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
    WHERE source_event_updates.correction_request_id = p_correction_request_id
    ORDER BY source_event_updates.correction_request_id
    LIMIT 1;
  ELSE
    v_source_event_update := NULL::jsonb;
    SELECT
      COALESCE(bool_or(source_event_updates.requires_user_action), false),
      COALESCE(bool_or(source_event_updates.processing_continues), false)
    INTO
      v_requires_user_action,
      v_processing_continues
    FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;
  END IF;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PROCESS_CHUNK_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'processed', v_processed_count,
      'applied', v_applied_count,
      'skipped', v_skipped_count,
      'blocked', v_blocked_count,
      'failed_retryable', v_failed_retryable_count,
      'failed_final', v_failed_final_count,
      'parent_status', v_parent_status,
      'totals', v_totals
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'processed', v_processed_count,
    'applied', v_applied_count,
    'skipped', v_skipped_count,
    'blocked', v_blocked_count,
    'failed_retryable', v_failed_retryable_count,
    'failed_final', v_failed_final_count,
    'parent_status', v_parent_status,
    'totals', v_totals,
    'requires_user_action', COALESCE(v_requires_user_action, false),
    'processing_continues', COALESCE(v_processing_continues, false),
    'source_bank_event_update', v_source_event_update,
    'source_bank_event_updates', v_source_event_updates,
    'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
    'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PROCESS_CHUNK_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'limit', p_limit,
        'worker_id', p_worker_id,
        'actor_user_id', p_actor_user_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;



CREATE OR REPLACE FUNCTION public.pay_pre_bank_cancel_apply_work_item(
  p_work_item_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_request public.pay_payment_correction_requests%rowtype;
  v_batch public.pay_batches%rowtype;
  v_now timestamptz := now();
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := NULL;
  v_blocker jsonb := NULL::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_reservation_count integer := 0;
  v_selected_finance_component_count integer := 0;
  v_voided_item_count integer := 0;
  v_inserted_correction_item_count integer := 0;
  v_released_reservation_count integer := 0;
  v_restored_component_count integer := 0;
  v_reset_payout_count integer := 0;
  v_cancelled_mail_count integer := 0;
  v_recalculated_transfer_count integer := 0;
  v_dirty_candidate_count integer := 0;
  v_has_settlement_evidence boolean := false;
  v_has_bank_submission_evidence boolean := false;
  v_has_authorised_partial_transfer_change boolean := false;
  v_batch_execution_commit_state text := 'NOT_SUBMITTED';
  v_is_authorised_or_scheduled_batch boolean := false;
  v_candidate_id uuid;
  v_refresh_result jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_is_whole_batch_work_item boolean := false;
  v_total_active_batch_item_count integer := 0;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_id_count integer := 0;
  v_expected_item_mismatch_count integer := 0;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_finance_resolution_result jsonb := NULL::jsonb;
  v_batch_cancel_result jsonb := NULL::jsonb;
  v_mail_selected_scope_json jsonb := '{}'::jsonb;
  v_communications_review_required_count integer := 0;
  v_mail_scope_matching jsonb := '{}'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_START',
    jsonb_build_object(
      'work_item_id', p_work_item_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  IF v_work_item.work_kind <> 'PRE_BANK_CANCEL' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_ITEM_KIND_NOT_PRE_BANK_CANCEL',
      'message', 'This work item is not a pre-bank cancellation work item.',
      'work_kind', v_work_item.work_kind
    );

    UPDATE public.pay_payment_correction_work_items AS blocked_work_kind
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(blocked_work_kind.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE blocked_work_kind.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = v_work_item.correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_request.requested_by_user_id);

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_work_item.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_selected;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_group_key,
    selected_rows.umbrella_id,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_pay_advances.payout_transfer_id,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.key_resolution_failure_reason
  FROM public._pay_payment_correction_selected_items(
    v_work_item.pay_batch_id,
    v_work_item.selection_json,
    false
  ) AS selected_rows
  LEFT JOIN public.pay_advances AS selected_pay_advances
    ON selected_pay_advances.id = selected_rows.finance_case_id;

  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (umbrella_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (transfer_group_key);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (payout_transfer_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (finance_component_id);

  SELECT
    count(*)::integer,
    count(DISTINCT pre_bank_selected.candidate_id) FILTER (WHERE pre_bank_selected.candidate_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.pay_bank_transfer_id) FILTER (WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.reservation_id) FILTER (WHERE pre_bank_selected.reservation_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.finance_component_id) FILTER (WHERE pre_bank_selected.finance_component_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_reservation_count,
    v_selected_finance_component_count
  FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected;

  IF v_selected_item_count = 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_PRE_BANK_CANCEL',
      'message', 'No selectable pay_batch_items were resolved for the pre-bank cancellation work item.'
    );

    UPDATE public.pay_payment_correction_work_items AS no_items_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_items_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_items_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
v_scope_type := upper(btrim(COALESCE(v_work_item.selection_json->>'scope_type', '')));
v_work_unit := upper(btrim(COALESCE(v_work_item.selection_json->>'work_unit', '')));

SELECT count(*)::integer
INTO v_total_active_batch_item_count
FROM public.pay_batch_items AS total_batch_items
JOIN public.pay_batch_candidates AS total_batch_candidates
  ON total_batch_candidates.id = total_batch_items.pay_batch_candidate_id
WHERE total_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
  AND COALESCE(total_batch_items.is_voided, false) = false;

v_is_whole_batch_work_item := (
  v_scope_type = 'BATCH'
  AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
  AND v_selected_item_count = COALESCE(v_total_active_batch_item_count, 0)
);

IF v_work_item.selection_json ? 'expected_item_count' THEN
  IF COALESCE(v_work_item.selection_json->>'expected_item_count', '') !~ '^[0-9]+$' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item expected_item_count is not a valid non-negative integer.',
      'expected_item_count_raw', v_work_item.selection_json->>'expected_item_count'
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_invalid_expected_count_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_invalid_expected_count_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_invalid_expected_count_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_expected_item_count := (v_work_item.selection_json->>'expected_item_count')::integer;

  IF v_expected_item_count <> v_selected_item_count THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_item count.',
      'expected_item_count', v_expected_item_count,
      'resolved_item_count', v_selected_item_count
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_count_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_expected_count_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_expected_count_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;

IF v_work_item.selection_json ? 'expected_pay_batch_item_ids'
   AND COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids must be a JSON array.'
  );

  UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_ids_type_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(pre_bank_cancel_expected_ids_type_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE pre_bank_cancel_expected_ids_type_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_expected_items;
CREATE TEMP TABLE _tmp_pre_bank_cancel_expected_items ON COMMIT DROP AS
WITH raw_expected_item_ids AS (
  SELECT jsonb_array_elements_text(
    CASE
      WHEN COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
        THEN v_work_item.selection_json->'expected_pay_batch_item_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_pay_batch_item_id
)
SELECT DISTINCT
  raw_expected_item_ids.raw_pay_batch_item_id,
  CASE
    WHEN raw_expected_item_ids.raw_pay_batch_item_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN raw_expected_item_ids.raw_pay_batch_item_id::uuid
    ELSE NULL::uuid
  END AS pay_batch_item_id
FROM raw_expected_item_ids;

SELECT count(*)::integer
INTO v_expected_item_mismatch_count
FROM pg_temp._tmp_pre_bank_cancel_expected_items AS invalid_expected_items
WHERE invalid_expected_items.pay_batch_item_id IS NULL;

IF v_expected_item_mismatch_count > 0 THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids contains invalid UUID values.',
    'invalid_expected_item_count', v_expected_item_mismatch_count
  );

  UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_invalid_expected_ids_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(pre_bank_cancel_invalid_expected_ids_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE pre_bank_cancel_invalid_expected_ids_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

SELECT count(*)::integer
INTO v_expected_item_id_count
FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_item_count
WHERE expected_item_count.pay_batch_item_id IS NOT NULL;

IF v_expected_item_id_count > 0 THEN
  SELECT count(*)::integer
  INTO v_expected_item_mismatch_count
  FROM (
    (
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
      EXCEPT
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_items
    )
    UNION ALL
    (
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_items
      EXCEPT
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
    )
  ) AS expected_item_drift;

  IF v_expected_item_mismatch_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_items.',
      'expected_item_count', v_expected_item_id_count,
      'resolved_item_count', v_selected_item_count,
      'mismatch_count', v_expected_item_mismatch_count
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_ids_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_expected_ids_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_expected_ids_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;


  PERFORM 1
  FROM public.pay_batch_items AS locked_batch_items
  JOIN pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_items
    ON lock_selected_items.pay_batch_item_id = locked_batch_items.id
  FOR UPDATE OF locked_batch_items;

  PERFORM 1
  FROM public.pay_batch_candidates AS locked_batch_candidates
  WHERE locked_batch_candidates.id IN (
    SELECT DISTINCT lock_selected_candidates.pay_batch_candidate_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_candidates
    WHERE lock_selected_candidates.pay_batch_candidate_id IS NOT NULL
  )
  FOR UPDATE OF locked_batch_candidates;

  PERFORM 1
  FROM public.pay_bank_transfers AS locked_bank_transfers
  WHERE locked_bank_transfers.id IN (
    SELECT DISTINCT lock_selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_transfers
    WHERE lock_selected_transfers.pay_bank_transfer_id IS NOT NULL
  )
  FOR UPDATE OF locked_bank_transfers;

  PERFORM 1
  FROM public.pay_advance_reservations AS locked_advance_reservations
  WHERE locked_advance_reservations.id IN (
    SELECT DISTINCT lock_selected_reservations.reservation_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_reservations
    WHERE lock_selected_reservations.reservation_id IS NOT NULL
  )
  FOR UPDATE OF locked_advance_reservations;

  PERFORM 1
  FROM public.pay_finance_case_components AS locked_finance_case_components
  WHERE locked_finance_case_components.id IN (
    SELECT DISTINCT lock_selected_components.finance_component_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_components
    WHERE lock_selected_components.finance_component_id IS NOT NULL
  )
  FOR UPDATE OF locked_finance_case_components;

  PERFORM 1
  FROM public.pay_advances AS locked_pay_advances
  WHERE locked_pay_advances.id IN (
    SELECT DISTINCT lock_selected_cases.finance_case_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_cases
    WHERE lock_selected_cases.finance_case_id IS NOT NULL
  )
  FOR UPDATE OF locked_pay_advances;

  v_classification_result := public._pay_payment_movement_classify(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb) || jsonb_build_object(
      'requested_action', 'CANCEL_PAYMENT_ATTEMPT',
      'source_context', 'WORK_ITEM_APPLY'
    )
  );

  v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');

  IF v_classification <> 'PRE_BANK_CANCEL' THEN
    v_blocker := jsonb_build_object(
      'code', 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED',
      'message', 'Selected scope is no longer classified as pre-bank cancellation.',
      'classification', v_classification,
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS classification_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(classification_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE classification_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_batch_candidates
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_selected
      ON settled_selected.pay_batch_candidate_id = public.pay_batch_candidates.id
    WHERE upper(btrim(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
       OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_bank_transfers
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_transfer_selected
      ON settled_transfer_selected.pay_bank_transfer_id = public.pay_bank_transfers.id
    WHERE upper(btrim(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
       OR public.pay_bank_transfers.completed_at_utc IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.timesheet_pay_state_history
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_history_selected
      ON settled_history_selected.timesheet_id = public.timesheet_pay_state_history.timesheet_id
    WHERE public.timesheet_pay_state_history.pay_batch_id = v_work_item.pay_batch_id
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_reservation_selected
      ON settled_reservation_selected.reservation_id = public.pay_advance_reservations.id
    WHERE upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'SETTLED'
       OR public.pay_advance_reservations.settled_at_utc IS NOT NULL
  )
  INTO v_has_settlement_evidence;

  IF v_has_settlement_evidence THEN
    v_blocker := jsonb_build_object(
      'code', 'SELECTED_SCOPE_HAS_SETTLEMENT_EVIDENCE',
      'message', 'Pre-bank cancellation cannot apply because selected scope has settlement evidence.'
    );

    UPDATE public.pay_payment_correction_work_items AS settled_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settled_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE settled_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_batch_execution_commit_state := upper(btrim(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED')));

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_bank_transfers
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS bank_submission_selected
      ON bank_submission_selected.pay_bank_transfer_id = public.pay_bank_transfers.id
    WHERE NULLIF(btrim(COALESCE(public.pay_bank_transfers.rail_tx_id, '')), '') IS NOT NULL
       OR NULLIF(btrim(COALESCE(public.pay_bank_transfers.request_id, '')), '') IS NOT NULL
       OR (
         public.pay_bank_transfers.rail_meta_json IS NOT NULL
         AND public.pay_bank_transfers.rail_meta_json <> '{}'::jsonb
       )
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.pay_batch_id = v_work_item.pay_batch_id
      AND public.pay_bank_transfer_events.pay_bank_transfer_id IN (
        SELECT DISTINCT bank_event_selected.pay_bank_transfer_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS bank_event_selected
        WHERE bank_event_selected.pay_bank_transfer_id IS NOT NULL
      )
  ) OR v_batch_execution_commit_state <> 'NOT_SUBMITTED'
    OR NULLIF(btrim(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL
    OR v_batch.execution_committed_at_utc IS NOT NULL
  INTO v_has_bank_submission_evidence;

  IF v_has_bank_submission_evidence THEN
    v_blocker := jsonb_build_object(
      'code', 'BANK_SUBMISSION_EVIDENCE_PRESENT',
      'message', 'Pre-bank cancellation cannot apply because bank/provider submission evidence exists.'
    );

    UPDATE public.pay_payment_correction_work_items AS submission_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(submission_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE submission_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_is_authorised_or_scheduled_batch := upper(btrim(COALESCE(v_batch.status, ''))) IN (
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT',
    'SCHEDULED',
    'EXECUTING',
    'WAITING_BANK_CONFIRM'
  );

  SELECT EXISTS (
    WITH affected_transfers AS (
      SELECT DISTINCT partial_selected.pay_bank_transfer_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS partial_selected
      WHERE partial_selected.pay_bank_transfer_id IS NOT NULL
    ),
    selected_transfer_items AS (
      SELECT
        affected_transfers.pay_bank_transfer_id,
        count(public.pay_batch_items.id)::integer AS selected_count
      FROM affected_transfers
      JOIN public.pay_batch_items
        ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
      JOIN pg_temp._tmp_pre_bank_cancel_selected AS partial_selected_items
        ON partial_selected_items.pay_batch_item_id = public.pay_batch_items.id
      GROUP BY affected_transfers.pay_bank_transfer_id
    ),
    all_transfer_items AS (
      SELECT
        affected_transfers.pay_bank_transfer_id,
        count(public.pay_batch_items.id)::integer AS total_count
      FROM affected_transfers
      JOIN public.pay_batch_items
        ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
      JOIN public.pay_batch_candidates
        ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
      WHERE public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
        AND COALESCE(public.pay_batch_items.is_voided, false) = false
      GROUP BY affected_transfers.pay_bank_transfer_id
    )
    SELECT 1
    FROM selected_transfer_items
    JOIN all_transfer_items
      ON all_transfer_items.pay_bank_transfer_id = selected_transfer_items.pay_bank_transfer_id
    WHERE selected_transfer_items.selected_count < all_transfer_items.total_count
  )
  INTO v_has_authorised_partial_transfer_change;

  IF v_is_authorised_or_scheduled_batch AND v_has_authorised_partial_transfer_change THEN
    v_blocker := jsonb_build_object(
      'code', 'AUTHORISED_TRANSFER_PARTIAL_CANCEL_BLOCKED',
      'message', 'Selected items are part of an authorised/scheduled transfer that still has remaining items. Cancel the whole transfer/whole batch or recreate the batch.'
    );

    UPDATE public.pay_payment_correction_work_items AS partial_transfer_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(partial_transfer_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE partial_transfer_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;


IF v_is_whole_batch_work_item THEN
  IF v_effective_actor_user_id IS NULL THEN
    v_blocker := jsonb_build_object(
      'code', 'PAYMENT_CORRECTION_ACTOR_REQUIRED_FOR_WHOLE_BATCH_CANCEL',
      'message', 'Whole-batch pre-bank cancellation requires a user actor so pay_batch_cancel can write the required audit trail.'
    );

    UPDATE public.pay_payment_correction_work_items AS whole_batch_actor_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(whole_batch_actor_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE whole_batch_actor_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_batch_cancel_result := public.pay_batch_cancel(
    v_work_item.pay_batch_id,
    v_effective_actor_user_id,
    COALESCE(NULLIF(btrim(COALESCE(v_request.reason, '')), ''), 'Payment correction pre-bank cancellation'),
    v_work_item.correction_request_id,
    p_work_item_id
  );

  v_finance_resolution_result := public._pay_payment_correction_apply_accepted_finance_resolution(
    v_work_item.correction_request_id,
    p_work_item_id,
    v_effective_actor_user_id
  );

  IF NOT COALESCE(NULLIF(v_finance_resolution_result->>'ok', '')::boolean, false) THEN
    RAISE EXCEPTION 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED',
              'message', 'Accepted gross/channel-sensitive finance resolution blocked whole-batch pre-bank cancellation; no partial correction must be committed.',
              'work_item_id', p_work_item_id,
              'correction_request_id', v_work_item.correction_request_id,
              'finance_resolution_result', v_finance_resolution_result
            )::text;
  END IF;

  v_cancelled_mail_count := CASE
    WHEN COALESCE(v_batch_cancel_result->>'communications_cancelled', '') ~ '^[0-9]+$'
      THEN (v_batch_cancel_result->>'communications_cancelled')::integer
    ELSE 0
  END;
  v_communications_review_required_count := 0;
  v_mail_scope_matching := jsonb_build_object(
    'exact_cancelled', v_cancelled_mail_count,
    'legacy_review', 0,
    'selected_scope_json', jsonb_build_object(
      'scope_type', v_scope_type,
      'work_unit', COALESCE(NULLIF(v_work_unit, ''), 'BATCH'),
      'pay_batch_id', v_work_item.pay_batch_id::text,
      'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id::text),
      'is_whole_batch', true
    ),
    'matches', '[]'::jsonb,
    'whole_batch_delegated_to_pay_batch_cancel', true,
    'safe_rule', 'Whole-batch correction may cancel queued mail for the batch; selected-scope cancellation must use _pay_payment_correction_mail_scope_match.'
  );

  v_result := COALESCE(v_batch_cancel_result, '{}'::jsonb) || jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'work_item_id', p_work_item_id,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'PRE_BANK_CANCEL',
    'work_unit', 'BATCH',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'classification_result', v_classification_result,
    'pay_batch_cancel_result', v_batch_cancel_result,
    'accepted_finance_resolution', v_finance_resolution_result,
    'applied_at_utc', v_now
  );

  UPDATE public.pay_payment_correction_work_items AS whole_batch_applied_work_item
  SET
    status = 'APPLIED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = COALESCE(whole_batch_applied_work_item.processed_at_utc, v_now),
    last_error = NULL,
    result_json = COALESCE(whole_batch_applied_work_item.result_json, '{}'::jsonb) || v_result
  WHERE whole_batch_applied_work_item.id = p_work_item_id;

  PERFORM public._imp_debug_audit(
    v_effective_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_RESULT',
    v_result,
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;
END IF;

  INSERT INTO public.pay_payment_correction_items(
    correction_request_id,
    pay_batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    pay_bank_transfer_id,
    timesheet_id,
    finance_case_id,
    finance_component_id,
    reservation_id,
    item_type,
    correction_item_kind,
    source_amount,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    economic_key_type,
    economic_key_value,
    before_snapshot_json,
    after_snapshot_json,
    status,
    created_at_utc,
    applied_at_utc
  )
  SELECT
    v_work_item.correction_request_id,
    selected_for_ledger.pay_batch_id,
    selected_for_ledger.pay_batch_candidate_id,
    selected_for_ledger.candidate_id,
    selected_for_ledger.pay_batch_item_id,
    selected_for_ledger.pay_bank_transfer_id,
    selected_for_ledger.timesheet_id,
    selected_for_ledger.finance_case_id,
    selected_for_ledger.finance_component_id,
    selected_for_ledger.reservation_id,
    selected_for_ledger.item_type,
    'PRE_BANK_CANCEL',
    selected_for_ledger.source_amount_ex_vat,
    selected_for_ledger.amount_ex_vat,
    selected_for_ledger.amount_vat,
    selected_for_ledger.amount_inc_vat,
    selected_for_ledger.economic_key_type,
    selected_for_ledger.economic_key_value,
    to_jsonb(ledger_items),
    to_jsonb(ledger_items) || jsonb_build_object('is_voided', true, 'updated_at', v_now),
    'APPLIED',
    v_now,
    v_now
  FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_for_ledger
  JOIN public.pay_batch_items AS ledger_items
    ON ledger_items.id = selected_for_ledger.pay_batch_item_id
  ON CONFLICT (pay_batch_item_id, correction_item_kind) WHERE status = 'APPLIED' AND pay_batch_item_id IS NOT NULL DO NOTHING;

  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;

  UPDATE public.pay_batch_items AS items_to_void
  SET
    is_voided = true,
    updated_at = v_now
  WHERE items_to_void.id IN (
    SELECT selected_to_void.pay_batch_item_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_to_void
  )
    AND COALESCE(items_to_void.is_voided, false) = false;

  GET DIAGNOSTICS v_voided_item_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_released_reservations;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_released_reservations ON COMMIT DROP AS
  WITH release_candidates AS (
    SELECT DISTINCT
      public.pay_advance_reservations.id,
      public.pay_advance_reservations.finance_case_id,
      public.pay_advance_reservations.finance_component_id,
      public.pay_advance_reservations.pay_batch_item_id,
      public.pay_advance_reservations.reserved_amount,
      public.pay_advance_reservations.reserved_source_amount
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS selected_reservation_items
      ON selected_reservation_items.reservation_id = public.pay_advance_reservations.id
      OR selected_reservation_items.pay_batch_item_id = public.pay_advance_reservations.pay_batch_item_id
    WHERE public.pay_advance_reservations.pay_batch_id = v_work_item.pay_batch_id
      AND (
        upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'RESERVED'
        OR (
          upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'COMMITTED'
          AND public.pay_advance_reservations.settled_at_utc IS NULL
        )
      )
  ),
  released_rows AS (
    UPDATE public.pay_advance_reservations AS reservations_to_release
    SET
      status = 'RELEASED',
      released_at_utc = COALESCE(reservations_to_release.released_at_utc, v_now),
      released_reason = 'PRE_BANK_CANCEL',
      updated_by_user_id = p_actor_user_id
    FROM release_candidates
    WHERE reservations_to_release.id = release_candidates.id
    RETURNING
      reservations_to_release.id,
      reservations_to_release.finance_case_id,
      reservations_to_release.finance_component_id,
      reservations_to_release.pay_batch_item_id,
      reservations_to_release.reserved_amount,
      reservations_to_release.reserved_source_amount
  )
  SELECT
    released_rows.id AS reservation_id,
    released_rows.finance_case_id,
    released_rows.finance_component_id,
    released_rows.pay_batch_item_id,
    released_rows.reserved_amount,
    released_rows.reserved_source_amount
  FROM released_rows;

  SELECT count(*)::integer
  INTO v_released_reservation_count
  FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservation_count;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_component_restore;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_component_restore ON COMMIT DROP AS
  SELECT
    restore_source.finance_component_id,
    restore_source.finance_case_id,
    round(sum(restore_source.restore_source_amount), 2)::numeric AS restore_source_amount
  FROM (
    SELECT
      COALESCE(released_reservations.finance_component_id, public.pay_batch_items.finance_component_id) AS finance_component_id,
      COALESCE(released_reservations.finance_case_id, public.pay_batch_items.finance_case_id) AS finance_case_id,
      round(abs(COALESCE(
        released_reservations.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(public.pay_batch_items.id),
        public.pay_batch_items.frozen_source_amount,
        released_reservations.reserved_amount,
        public.pay_batch_items.amount_ex_vat,
        public.pay_batch_items.amount_inc_vat,
        0
      )), 2)::numeric AS restore_source_amount
    FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservations
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.id = released_reservations.pay_batch_item_id
  ) AS restore_source
  WHERE restore_source.finance_component_id IS NOT NULL
    AND restore_source.restore_source_amount > 0
  GROUP BY restore_source.finance_component_id, restore_source.finance_case_id;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_component_restore_apply;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_component_restore_apply ON COMMIT DROP AS
  SELECT
    public.pay_finance_case_components.id AS finance_component_id,
    public.pay_finance_case_components.finance_case_id,
    public.pay_finance_case_components.remaining_source_amount AS remaining_before,
    LEAST(
      COALESCE(public.pay_finance_case_components.source_amount, 0),
      COALESCE(public.pay_finance_case_components.remaining_source_amount, 0) + COALESCE(component_restore.restore_source_amount, 0)
    ) AS remaining_after,
    component_restore.restore_source_amount
  FROM pg_temp._tmp_pre_bank_cancel_component_restore AS component_restore
  JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = component_restore.finance_component_id;

  UPDATE public.pay_finance_case_components AS components_to_restore
  SET
    remaining_source_amount = component_restore_apply.remaining_after,
    resolved_at_utc = CASE WHEN component_restore_apply.remaining_after > 0 THEN NULL ELSE components_to_restore.resolved_at_utc END,
    closed_at_utc = NULL,
    updated_at_utc = v_now
  FROM pg_temp._tmp_pre_bank_cancel_component_restore_apply AS component_restore_apply
  WHERE components_to_restore.id = component_restore_apply.finance_component_id;

  GET DIAGNOSTICS v_restored_component_count = ROW_COUNT;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    component_restore_apply.finance_case_id,
    component_restore_apply.finance_component_id,
    'COMPONENT_RESTORED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    NULL::uuid,
    jsonb_build_object('remaining_source_amount', component_restore_apply.remaining_before),
    jsonb_build_object(
      'remaining_source_amount', component_restore_apply.remaining_after,
      'restored_source_amount', component_restore_apply.restore_source_amount,
      'correction_kind', 'PRE_BANK_CANCEL',
      'work_item_id', p_work_item_id
    ),
    'PRE_BANK_CANCEL',
    'Payment correction pre-bank cancellation restored reserved component amount.'
  FROM pg_temp._tmp_pre_bank_cancel_component_restore_apply AS component_restore_apply;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    released_reservations.finance_case_id,
    released_reservations.finance_component_id,
    'RESERVATION_RELEASED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    released_reservations.reservation_id,
    jsonb_build_object('reservation_status', 'RESERVED_OR_COMMITTED'),
    jsonb_build_object(
      'reservation_status', 'RELEASED',
      'released_reason', 'PRE_BANK_CANCEL',
      'work_item_id', p_work_item_id
    ),
    'PRE_BANK_CANCEL',
    'Payment correction pre-bank cancellation released reservation.'
  FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservations
  WHERE released_reservations.finance_case_id IS NOT NULL;

  UPDATE public.pay_advances AS payout_cases_to_reset
  SET
    payout_status = 'PENDING',
    payout_pay_batch_id = NULL,
    payout_transfer_id = NULL,
    updated_at = v_now
  WHERE payout_cases_to_reset.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND COALESCE(payout_cases_to_reset.payout_status::text, '') <> 'PAID'
    AND (
      payout_cases_to_reset.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases_to_reset.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  GET DIAGNOSTICS v_reset_payout_count = ROW_COUNT;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(v_work_unit, ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', v_work_item.pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id::text),
    'is_whole_batch', v_is_whole_batch_work_item,
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(v_work_item.selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.umbrella_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.finance_case_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.finance_component_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.reservation_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.payout_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.payout_transfer_id IS NOT NULL
        UNION
        SELECT DISTINCT pre_bank_selected.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.transfer_group_key AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE NULLIF(btrim(COALESCE(pre_bank_selected.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_mail_selected_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_mail_scope_matches;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_mail_scope_matches ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.type,
      public.mail_outbox.email_type,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.reference,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) = 'QUEUED'
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id AS mail_outbox_id,
      candidate_mail.status,
      candidate_mail.type,
      candidate_mail.email_type,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.reference,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        v_work_item.pay_batch_id,
        v_work_item.selection_json,
        v_mail_selected_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.mail_outbox_id,
    matched_mail.status,
    matched_mail.type,
    matched_mail.email_type,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.reference,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS match_kind,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
  WHERE queued_mail_to_cancel.id = mail_scope_match.mail_outbox_id
    AND upper(btrim(COALESCE(queued_mail_to_cancel.status::text, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.safe_to_cancel, false)
    AND (
      mail_scope_match.match_confidence = 'EXACT'
      OR (
        v_is_whole_batch_work_item
        AND mail_scope_match.match_kind = 'WHOLE_BATCH'
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_communications_review_required_count
  FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
  WHERE upper(btrim(COALESCE(mail_scope_match.status, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.requires_review, false);

  v_mail_scope_matching := jsonb_build_object(
    'exact_cancelled', v_cancelled_mail_count,
    'legacy_review', v_communications_review_required_count,
    'selected_scope_json', v_mail_selected_scope_json,
    'matches', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', mail_scope_match.mail_outbox_id,
        'match_kind', mail_scope_match.match_kind,
        'match_confidence', mail_scope_match.match_confidence,
        'safe_to_cancel', mail_scope_match.safe_to_cancel,
        'requires_review', mail_scope_match.requires_review,
        'reason', mail_scope_match.match_reason,
        'status', mail_scope_match.status,
        'type', mail_scope_match.type,
        'email_type', mail_scope_match.email_type,
        'context_kind', mail_scope_match.context_kind,
        'context_id', mail_scope_match.context_id,
        'recipient_kind', mail_scope_match.recipient_kind,
        'recipient_id', mail_scope_match.recipient_id,
        'reference', mail_scope_match.reference,
        'payment_scope_json', mail_scope_match.payment_scope_json
      ) ORDER BY mail_scope_match.mail_outbox_id)
      FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
    ), '[]'::jsonb)
  );

  WITH affected_transfers AS (
    SELECT DISTINCT selected_transfer_amounts.pay_bank_transfer_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_transfer_amounts
    WHERE selected_transfer_amounts.pay_bank_transfer_id IS NOT NULL
  ),
  recalculated_transfers AS (
    SELECT
      affected_transfers.pay_bank_transfer_id,
      round(COALESCE(sum(COALESCE(public.pay_batch_items.amount_inc_vat, 0)), 0), 2)::numeric AS remaining_amount
    FROM affected_transfers
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
     AND COALESCE(public.pay_batch_items.is_voided, false) = false
    GROUP BY affected_transfers.pay_bank_transfer_id
  )
  UPDATE public.pay_bank_transfers AS transfer_to_recalculate
  SET
    amount = recalculated_transfers.remaining_amount,
    status = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN 'VOIDED'
      ELSE transfer_to_recalculate.status
    END,
    failed_reason = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.failed_reason, 'PRE_BANK_CANCEL_VOIDED')
      ELSE transfer_to_recalculate.failed_reason
    END,
    rail_meta_json = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'pre_bank_cancel_applied', true,
          'pre_bank_cancel_work_item_id', p_work_item_id::text,
          'pre_bank_cancel_at_utc', v_now,
          'pre_bank_cancel_status_note', 'Transfer amount became zero after selected pre-bank cancellation; status set to VOIDED.'
        )
      ELSE transfer_to_recalculate.rail_meta_json
    END
  FROM recalculated_transfers
  WHERE transfer_to_recalculate.id = recalculated_transfers.pay_bank_transfer_id;

  GET DIAGNOSTICS v_recalculated_transfer_count = ROW_COUNT;

  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
  SELECT
    'pay_candidate:' || dirty_candidates.candidate_id::text,
    1,
    v_now
  FROM (
    SELECT DISTINCT selected_dirty_candidates.candidate_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_dirty_candidates
    WHERE selected_dirty_candidates.candidate_id IS NOT NULL
  ) AS dirty_candidates
  ON CONFLICT (entity_key)
  DO UPDATE
  SET
    seq = public.app_change_counters.seq + 1,
    updated_at = v_now;

  GET DIAGNOSTICS v_dirty_candidate_count = ROW_COUNT;

  IF v_batch.source_snapshot_run_id IS NOT NULL THEN
    FOR v_candidate_id IN
      SELECT DISTINCT selected_refresh_candidates.candidate_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_refresh_candidates
      WHERE selected_refresh_candidates.candidate_id IS NOT NULL
      ORDER BY selected_refresh_candidates.candidate_id
    LOOP
      BEGIN
        v_refresh_result := public.pay_workbench_enqueue_candidate_refresh(
          p_snapshot_run_id => v_batch.source_snapshot_run_id,
          p_candidate_id => v_candidate_id,
          p_reason => 'PAYMENT_CORRECTION_PRE_BANK_CANCEL',
          p_actor_user_id => p_actor_user_id,
          p_payload_json => jsonb_build_object(
            'pay_batch_id', v_work_item.pay_batch_id,
            'correction_request_id', v_work_item.correction_request_id,
            'work_item_id', p_work_item_id,
            'refresh_reason', 'PAYMENT_CORRECTION_PRE_BANK_CANCEL'
          )
        );
      EXCEPTION
        WHEN OTHERS THEN
          PERFORM public._imp_debug_audit(
            p_actor_user_id,
            'PAYMENT_CORRECTION_PRE_BANK_CANCEL_REFRESH_ENQUEUE_ERROR',
            jsonb_build_object(
              'work_item_id', p_work_item_id,
              'candidate_id', v_candidate_id,
              'snapshot_run_id', v_batch.source_snapshot_run_id,
              'sqlstate', SQLSTATE,
              'error_message', SQLERRM
            ),
            'pay_payment_correction',
            p_work_item_id::text,
            NULL::jsonb,
            NULL::text,
            NULL::text,
            NULL::text
          );
      END;
    END LOOP;
  END IF;


v_finance_resolution_result := public._pay_payment_correction_apply_accepted_finance_resolution(
  v_work_item.correction_request_id,
  p_work_item_id,
  v_effective_actor_user_id
);

IF NOT COALESCE(NULLIF(v_finance_resolution_result->>'ok', '')::boolean, false) THEN
  RAISE EXCEPTION 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED'
    USING ERRCODE = 'P0001',
          DETAIL = jsonb_build_object(
            'code', 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED',
            'message', 'Accepted gross/channel-sensitive finance resolution blocked pre-bank cancellation; no partial correction must be committed.',
            'work_item_id', p_work_item_id,
            'correction_request_id', v_work_item.correction_request_id,
            'finance_resolution_result', v_finance_resolution_result
          )::text;
END IF;


  v_result := jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'work_item_id', p_work_item_id,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'PRE_BANK_CANCEL',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'recalculated_transfer_count', v_recalculated_transfer_count,
    'dirty_candidate_count', v_dirty_candidate_count,
    'classification_result', v_classification_result,
    'applied_at_utc', v_now
  );

  UPDATE public.pay_payment_correction_work_items AS applied_work_item
  SET
    status = 'APPLIED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = COALESCE(applied_work_item.processed_at_utc, v_now),
    last_error = NULL,
    result_json = COALESCE(applied_work_item.result_json, '{}'::jsonb) || v_result
  WHERE applied_work_item.id = p_work_item_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_RESULT',
    v_result,
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_ERROR',
      jsonb_build_object(
        'work_item_id', p_work_item_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;



CREATE OR REPLACE FUNCTION public.pay_settled_payment_reversal_apply_work_item(
  p_work_item_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_request public.pay_payment_correction_requests%rowtype;
  v_batch public.pay_batches%rowtype;
  v_now timestamptz := now();
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := NULL;
  v_blocker jsonb := NULL::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_reservation_count integer := 0;
  v_selected_finance_component_count integer := 0;
  v_selected_finance_case_count integer := 0;
  v_inserted_correction_item_count integer := 0;
  v_released_reservation_count integer := 0;
  v_restored_component_count integer := 0;
  v_reset_payout_count integer := 0;
  v_cancelled_mail_count integer := 0;
  v_dirty_candidate_count integer := 0;
  v_timesheet_state_updated_count integer := 0;
  v_timesheet_state_deleted_count integer := 0;
  v_notice_group_id uuid := NULL::uuid;
  v_quiet_minutes integer := 10;
  v_max_wait_minutes integer := 60;
  v_gross_channel_sensitive_item_count integer := 0;
  v_accepted_resolution_required boolean := false;
  v_accepted_resolution_is_stale boolean := false;
  v_fingerprint_missing boolean := false;
  v_fingerprint_mismatch_count integer := 0;
  v_finance_written_off_count integer := 0;
  v_finance_closed_count integer := 0;
  v_finance_stale_count integer := 0;
  v_late_manual_event_count integer := 0;
  v_reissued_payout_count integer := 0;
  v_has_aggregate_subset_blocker boolean := false;
  v_has_no_money_or_unsettled_mix boolean := false;
  v_candidate_id uuid;
  v_refresh_result jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_is_whole_batch_work_item boolean := false;
  v_total_active_batch_item_count integer := 0;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_id_count integer := 0;
  v_expected_item_mismatch_count integer := 0;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_finance_resolution_result jsonb := NULL::jsonb;
  v_notice_queue_result jsonb := NULL::jsonb;
  v_mail_selected_scope_json jsonb := '{}'::jsonb;
  v_communications_review_required_count integer := 0;
  v_mail_scope_matching jsonb := '{}'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_SETTLED_REVERSAL_WORK_START',
    jsonb_build_object(
      'work_item_id', p_work_item_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  IF v_work_item.work_kind <> 'SETTLED_REVERSAL' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_ITEM_KIND_NOT_SETTLED_REVERSAL',
      'message', 'This work item is not a settled reversal work item.',
      'work_kind', v_work_item.work_kind
    );

    UPDATE public.pay_payment_correction_work_items AS blocked_work_kind
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(blocked_work_kind.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE blocked_work_kind.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = v_work_item.correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_request.requested_by_user_id);

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_work_item.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_selected;
  CREATE TEMP TABLE _tmp_settled_reversal_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_group_key,
    selected_rows.umbrella_id,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.key_resolution_failure_reason
  FROM public._pay_payment_correction_selected_items(
    v_work_item.pay_batch_id,
    v_work_item.selection_json,
    false
  ) AS selected_rows;

  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (umbrella_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (transfer_group_key);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (finance_component_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (finance_case_id);
  CREATE INDEX ON pg_temp._tmp_settled_reversal_selected (timesheet_id);

  SELECT
    count(*)::integer,
    count(DISTINCT reversal_selected.candidate_id) FILTER (WHERE reversal_selected.candidate_id IS NOT NULL)::integer,
    count(DISTINCT reversal_selected.pay_bank_transfer_id) FILTER (WHERE reversal_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT reversal_selected.reservation_id) FILTER (WHERE reversal_selected.reservation_id IS NOT NULL)::integer,
    count(DISTINCT reversal_selected.finance_component_id) FILTER (WHERE reversal_selected.finance_component_id IS NOT NULL)::integer,
    count(DISTINCT reversal_selected.finance_case_id) FILTER (WHERE reversal_selected.finance_case_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_reservation_count,
    v_selected_finance_component_count,
    v_selected_finance_case_count
  FROM pg_temp._tmp_settled_reversal_selected AS reversal_selected;

  IF v_selected_item_count = 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_SETTLED_REVERSAL',
      'message', 'No selectable pay_batch_items were resolved for the settled reversal work item.'
    );

    UPDATE public.pay_payment_correction_work_items AS no_items_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_items_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_items_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
v_scope_type := upper(btrim(COALESCE(v_work_item.selection_json->>'scope_type', '')));
v_work_unit := upper(btrim(COALESCE(v_work_item.selection_json->>'work_unit', '')));

SELECT count(*)::integer
INTO v_total_active_batch_item_count
FROM public.pay_batch_items AS total_batch_items
JOIN public.pay_batch_candidates AS total_batch_candidates
  ON total_batch_candidates.id = total_batch_items.pay_batch_candidate_id
WHERE total_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
  AND COALESCE(total_batch_items.is_voided, false) = false;

v_is_whole_batch_work_item := (
  v_scope_type = 'BATCH'
  AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
  AND v_selected_item_count = COALESCE(v_total_active_batch_item_count, 0)
);

IF v_work_item.selection_json ? 'expected_item_count' THEN
  IF COALESCE(v_work_item.selection_json->>'expected_item_count', '') !~ '^[0-9]+$' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item expected_item_count is not a valid non-negative integer.',
      'expected_item_count_raw', v_work_item.selection_json->>'expected_item_count'
    );

    UPDATE public.pay_payment_correction_work_items AS settled_reversal_invalid_expected_count_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settled_reversal_invalid_expected_count_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE settled_reversal_invalid_expected_count_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_expected_item_count := (v_work_item.selection_json->>'expected_item_count')::integer;

  IF v_expected_item_count <> v_selected_item_count THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_item count.',
      'expected_item_count', v_expected_item_count,
      'resolved_item_count', v_selected_item_count
    );

    UPDATE public.pay_payment_correction_work_items AS settled_reversal_expected_count_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settled_reversal_expected_count_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE settled_reversal_expected_count_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;

IF v_work_item.selection_json ? 'expected_pay_batch_item_ids'
   AND COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids must be a JSON array.'
  );

  UPDATE public.pay_payment_correction_work_items AS settled_reversal_expected_ids_type_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(settled_reversal_expected_ids_type_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE settled_reversal_expected_ids_type_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_expected_items;
CREATE TEMP TABLE _tmp_settled_reversal_expected_items ON COMMIT DROP AS
WITH raw_expected_item_ids AS (
  SELECT jsonb_array_elements_text(
    CASE
      WHEN COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
        THEN v_work_item.selection_json->'expected_pay_batch_item_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_pay_batch_item_id
)
SELECT DISTINCT
  raw_expected_item_ids.raw_pay_batch_item_id,
  CASE
    WHEN raw_expected_item_ids.raw_pay_batch_item_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN raw_expected_item_ids.raw_pay_batch_item_id::uuid
    ELSE NULL::uuid
  END AS pay_batch_item_id
FROM raw_expected_item_ids;

SELECT count(*)::integer
INTO v_expected_item_mismatch_count
FROM pg_temp._tmp_settled_reversal_expected_items AS invalid_expected_items
WHERE invalid_expected_items.pay_batch_item_id IS NULL;

IF v_expected_item_mismatch_count > 0 THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids contains invalid UUID values.',
    'invalid_expected_item_count', v_expected_item_mismatch_count
  );

  UPDATE public.pay_payment_correction_work_items AS settled_reversal_invalid_expected_ids_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(settled_reversal_invalid_expected_ids_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE settled_reversal_invalid_expected_ids_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

SELECT count(*)::integer
INTO v_expected_item_id_count
FROM pg_temp._tmp_settled_reversal_expected_items AS expected_item_count
WHERE expected_item_count.pay_batch_item_id IS NOT NULL;

IF v_expected_item_id_count > 0 THEN
  SELECT count(*)::integer
  INTO v_expected_item_mismatch_count
  FROM (
    (
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_settled_reversal_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
      EXCEPT
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_settled_reversal_selected AS selected_items
    )
    UNION ALL
    (
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_settled_reversal_selected AS selected_items
      EXCEPT
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_settled_reversal_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
    )
  ) AS expected_item_drift;

  IF v_expected_item_mismatch_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_items.',
      'expected_item_count', v_expected_item_id_count,
      'resolved_item_count', v_selected_item_count,
      'mismatch_count', v_expected_item_mismatch_count
    );

    UPDATE public.pay_payment_correction_work_items AS settled_reversal_expected_ids_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settled_reversal_expected_ids_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE settled_reversal_expected_ids_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;


  PERFORM 1
  FROM public.pay_batch_items AS locked_batch_items
  JOIN pg_temp._tmp_settled_reversal_selected AS lock_selected_items
    ON lock_selected_items.pay_batch_item_id = locked_batch_items.id
  FOR UPDATE OF locked_batch_items;

  PERFORM 1
  FROM public.pay_batch_candidates AS locked_batch_candidates
  WHERE locked_batch_candidates.id IN (
    SELECT DISTINCT lock_selected_candidates.pay_batch_candidate_id
    FROM pg_temp._tmp_settled_reversal_selected AS lock_selected_candidates
    WHERE lock_selected_candidates.pay_batch_candidate_id IS NOT NULL
  )
  FOR UPDATE OF locked_batch_candidates;

  PERFORM 1
  FROM public.pay_bank_transfers AS locked_bank_transfers
  WHERE locked_bank_transfers.id IN (
    SELECT DISTINCT lock_selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_settled_reversal_selected AS lock_selected_transfers
    WHERE lock_selected_transfers.pay_bank_transfer_id IS NOT NULL
  )
  FOR UPDATE OF locked_bank_transfers;

  PERFORM 1
  FROM public.pay_advance_reservations AS locked_advance_reservations
  WHERE locked_advance_reservations.id IN (
    SELECT DISTINCT lock_selected_reservations.reservation_id
    FROM pg_temp._tmp_settled_reversal_selected AS lock_selected_reservations
    WHERE lock_selected_reservations.reservation_id IS NOT NULL
  )
  FOR UPDATE OF locked_advance_reservations;

  PERFORM 1
  FROM public.pay_finance_case_components AS locked_finance_case_components
  WHERE locked_finance_case_components.id IN (
    SELECT DISTINCT lock_selected_components.finance_component_id
    FROM pg_temp._tmp_settled_reversal_selected AS lock_selected_components
    WHERE lock_selected_components.finance_component_id IS NOT NULL
  )
  FOR UPDATE OF locked_finance_case_components;

  PERFORM 1
  FROM public.pay_advances AS locked_pay_advances
  WHERE locked_pay_advances.id IN (
    SELECT DISTINCT lock_selected_cases.finance_case_id
    FROM pg_temp._tmp_settled_reversal_selected AS lock_selected_cases
    WHERE lock_selected_cases.finance_case_id IS NOT NULL
  )
  FOR UPDATE OF locked_pay_advances;

  v_classification_result := public._pay_payment_movement_classify(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb) || jsonb_build_object(
      'requested_action', 'REVERSE_SETTLED_PAYMENT',
      'source_context', 'WORK_ITEM_APPLY'
    )
  );

  v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');

  IF v_classification <> 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN
    v_blocker := jsonb_build_object(
      'code', 'SETTLED_REVERSAL_CLASSIFICATION_REQUIRED',
      'message', 'Selected scope is no longer classified as requiring true settled reversal.',
      'classification', v_classification,
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS classification_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(classification_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE classification_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(v_classification_result->'blockers', '[]'::jsonb)) AS blocker_elements(blocker_value)
    WHERE COALESCE(blocker_elements.blocker_value->>'code', '') = 'AGGREGATE_UMBRELLA_TRANSFER_SUBSET_SELECTED'
  )
  INTO v_has_aggregate_subset_blocker;

  IF COALESCE(v_has_aggregate_subset_blocker, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'SETTLED_REVERSAL_AGGREGATE_TRANSFER_SUBSET_BLOCKED',
      'message', 'Bank return maps to an aggregate transfer, but the selected scope is only a subset. Apply to the whole transfer or use manual evidence review.',
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS aggregate_subset_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(aggregate_subset_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE aggregate_subset_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(v_classification_result->'blockers', '[]'::jsonb)) AS blocker_elements(blocker_value)
    WHERE COALESCE(blocker_elements.blocker_value->>'code', '') IN (
      'CONFLICTING_SETTLED_AND_FAILED_EVIDENCE',
      'CONFLICTING_BANK_EVENTS',
      'SUBMITTED_BUT_NOT_TERMINAL_OR_SETTLED',
      'AMBIGUOUS_PROVIDER_STATE'
    )
  )
  INTO v_has_no_money_or_unsettled_mix;

  IF COALESCE(v_has_no_money_or_unsettled_mix, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'SETTLED_REVERSAL_MIXED_OR_AMBIGUOUS_SCOPE_BLOCKED',
      'message', 'Selected scope contains mixed unsettled/no-money or ambiguous evidence that cannot be separated safely.',
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS mixed_scope_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(mixed_scope_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE mixed_scope_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_finance_detail;
  CREATE TEMP TABLE _tmp_settled_reversal_finance_detail ON COMMIT DROP AS
  SELECT
    selected_finance.pay_batch_item_id,
    selected_finance.finance_case_id,
    selected_finance.finance_component_id,
    public.pay_batch_items.frozen_component_classification::text AS frozen_component_classification,
    public.pay_batch_items.frozen_resolution_mode::text AS frozen_resolution_mode,
    public.pay_finance_case_components.classification::text AS finance_component_classification,
    public.pay_finance_case_components.resolution_fingerprint AS current_resolution_fingerprint,
    public.pay_finance_case_components.is_resolution_stale AS finance_component_is_resolution_stale,
    public.pay_finance_case_components.closed_at_utc AS finance_component_closed_at_utc,
    public.pay_advances.taxability::text AS finance_taxability,
    public.pay_advances.status::text AS finance_case_status,
    public.pay_advances.written_off_at_utc AS finance_written_off_at_utc,
    public.pay_advances.cleared_at_utc AS finance_cleared_at_utc
  FROM pg_temp._tmp_settled_reversal_selected AS selected_finance
  JOIN public.pay_batch_items
    ON public.pay_batch_items.id = selected_finance.pay_batch_item_id
  LEFT JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = selected_finance.finance_component_id
  LEFT JOIN public.pay_advances
    ON public.pay_advances.id = selected_finance.finance_case_id;

  SELECT count(*)::integer
  INTO v_gross_channel_sensitive_item_count
  FROM pg_temp._tmp_settled_reversal_finance_detail AS finance_detail
  WHERE finance_detail.finance_case_id IS NOT NULL
    AND (
      COALESCE(finance_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR COALESCE(finance_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR (
        COALESCE(finance_detail.finance_taxability, '') = 'TAXABLE'
        AND NULLIF(btrim(COALESCE(finance_detail.frozen_resolution_mode, '')), '') IS NOT NULL
      )
    );

  v_accepted_resolution_required := v_gross_channel_sensitive_item_count > 0;

  v_accepted_resolution_is_stale := v_request.accepted_resolution_json IS NOT NULL
    AND (
      lower(COALESCE(v_request.accepted_resolution_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'resolution_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,is_stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
    );

  IF v_accepted_resolution_required AND v_request.accepted_resolution_json IS NULL THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_SUGGESTED_RESOLUTION_REQUIRED',
      'message', 'Gross/taxable/channel-sensitive finance items require an accepted suggested resolution before settled reversal can apply.',
      'affected_item_count', v_gross_channel_sensitive_item_count
    );

    UPDATE public.pay_payment_correction_work_items AS accepted_resolution_required_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(accepted_resolution_required_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE accepted_resolution_required_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  IF v_accepted_resolution_required AND v_accepted_resolution_is_stale THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_STALE',
      'message', 'The accepted suggested finance resolution is stale and must be regenerated before settled reversal can apply.'
    );

    UPDATE public.pay_payment_correction_work_items AS stale_resolution_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(stale_resolution_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE stale_resolution_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  SELECT
    count(*) FILTER (WHERE finance_detail.finance_written_off_at_utc IS NOT NULL)::integer,
    0::integer,
    count(*) FILTER (WHERE finance_detail.finance_component_is_resolution_stale IS TRUE)::integer
  INTO
    v_finance_written_off_count,
    v_finance_closed_count,
    v_finance_stale_count
  FROM pg_temp._tmp_settled_reversal_finance_detail AS finance_detail;

  IF v_finance_written_off_count > 0 OR v_finance_stale_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'FINANCE_CASE_MANUAL_REVIEW_REQUIRED',
      'message', 'One or more selected finance cases/components are written off or stale and require manual finance review before settled reversal can apply.',
      'written_off_count', v_finance_written_off_count,
      'closed_or_cleared_count', v_finance_closed_count,
      'stale_component_count', v_finance_stale_count
    );

    UPDATE public.pay_payment_correction_work_items AS finance_state_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(finance_state_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE finance_state_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  SELECT count(*)::integer
  INTO v_late_manual_event_count
  FROM public.pay_finance_case_events AS finance_events
  WHERE finance_events.finance_case_id IN (
    SELECT DISTINCT selected_finance_cases.finance_case_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_finance_cases
    WHERE selected_finance_cases.finance_case_id IS NOT NULL
  )
    AND finance_events.event_at_utc > v_request.created_at_utc
    AND (
      upper(btrim(COALESCE(finance_events.event_type, ''))) LIKE '%WRITE%OFF%'
      OR upper(btrim(COALESCE(finance_events.event_type, ''))) LIKE '%RESTRUCT%'
      OR upper(btrim(COALESCE(finance_events.event_type, ''))) LIKE '%RESOLUTION%'
      OR upper(btrim(COALESCE(finance_events.event_type, ''))) LIKE '%MANUAL%'
    );

  IF v_late_manual_event_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'FINANCE_CASE_CHANGED_AFTER_CORRECTION_REQUEST',
      'message', 'Finance case/component events occurred after the correction request was created. Regenerate the correction plan before apply.',
      'event_count', v_late_manual_event_count
    );

    UPDATE public.pay_payment_correction_work_items AS late_finance_event_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(late_finance_event_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE late_finance_event_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  IF v_accepted_resolution_required THEN
    v_fingerprint_missing := NOT (
      COALESCE(v_request.accepted_resolution_json, '{}'::jsonb) ? 'component_fingerprints'
      OR COALESCE(v_request.accepted_resolution_json, '{}'::jsonb) ? 'resolution_fingerprints'
      OR COALESCE(v_request.accepted_resolution_json#>'{validation,component_fingerprints}', '{}'::jsonb) <> '{}'::jsonb
    );

    IF v_fingerprint_missing THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_FINGERPRINT_MISSING',
        'message', 'Accepted suggested resolution does not include finance component fingerprints required for safe apply.'
      );

      UPDATE public.pay_payment_correction_work_items AS missing_fingerprint_work
      SET
        status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(missing_fingerprint_work.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'processed_at_utc', v_now
        )
      WHERE missing_fingerprint_work.id = p_work_item_id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    WITH accepted_fingerprints AS (
      SELECT fingerprint_entries.component_id_text,
             fingerprint_entries.fingerprint_value
      FROM jsonb_each_text(COALESCE(v_request.accepted_resolution_json->'component_fingerprints', '{}'::jsonb)) AS fingerprint_entries(component_id_text, fingerprint_value)
      UNION ALL
      SELECT fingerprint_entries.component_id_text,
             fingerprint_entries.fingerprint_value
      FROM jsonb_each_text(COALESCE(v_request.accepted_resolution_json->'resolution_fingerprints', '{}'::jsonb)) AS fingerprint_entries(component_id_text, fingerprint_value)
      UNION ALL
      SELECT fingerprint_entries.component_id_text,
             fingerprint_entries.fingerprint_value
      FROM jsonb_each_text(COALESCE(v_request.accepted_resolution_json#>'{validation,component_fingerprints}', '{}'::jsonb)) AS fingerprint_entries(component_id_text, fingerprint_value)
    )
    SELECT count(*)::integer
    INTO v_fingerprint_mismatch_count
    FROM pg_temp._tmp_settled_reversal_finance_detail AS finance_detail
    JOIN accepted_fingerprints
      ON accepted_fingerprints.component_id_text = finance_detail.finance_component_id::text
    WHERE finance_detail.finance_component_id IS NOT NULL
      AND COALESCE(finance_detail.current_resolution_fingerprint, '') IS DISTINCT FROM COALESCE(accepted_fingerprints.fingerprint_value, '');

    IF v_fingerprint_mismatch_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_FINGERPRINT_MISMATCH',
        'message', 'One or more accepted finance resolution fingerprints no longer match current finance component state.',
        'mismatch_count', v_fingerprint_mismatch_count
      );

      UPDATE public.pay_payment_correction_work_items AS fingerprint_mismatch_work
      SET
        status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(fingerprint_mismatch_work.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'processed_at_utc', v_now
        )
      WHERE fingerprint_mismatch_work.id = p_work_item_id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;
  END IF;

  SELECT count(*)::integer
  INTO v_reissued_payout_count
  FROM public.pay_advances AS payout_cases
  WHERE payout_cases.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND upper(btrim(COALESCE(payout_cases.payout_status::text, ''))) = 'PAID'
    AND NOT (
      payout_cases.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_settled_reversal_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  IF v_reissued_payout_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'PAYOUT_REISSUED_BY_ANOTHER_BATCH',
      'message', 'One or more selected payouts were paid/reissued by another batch and cannot be silently reset.',
      'count', v_reissued_payout_count
    );

    UPDATE public.pay_payment_correction_work_items AS reissued_payout_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(reissued_payout_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE reissued_payout_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;


  INSERT INTO public.pay_payment_correction_items(
    correction_request_id,
    pay_batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    pay_bank_transfer_id,
    timesheet_id,
    finance_case_id,
    finance_component_id,
    reservation_id,
    item_type,
    correction_item_kind,
    source_amount,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    economic_key_type,
    economic_key_value,
    before_snapshot_json,
    after_snapshot_json,
    status,
    created_at_utc,
    applied_at_utc
  )
  SELECT
    v_work_item.correction_request_id,
    selected_for_ledger.pay_batch_id,
    selected_for_ledger.pay_batch_candidate_id,
    selected_for_ledger.candidate_id,
    selected_for_ledger.pay_batch_item_id,
    selected_for_ledger.pay_bank_transfer_id,
    selected_for_ledger.timesheet_id,
    selected_for_ledger.finance_case_id,
    selected_for_ledger.finance_component_id,
    selected_for_ledger.reservation_id,
    selected_for_ledger.item_type,
    'SETTLED_REVERSAL',
    selected_for_ledger.source_amount_ex_vat,
    selected_for_ledger.amount_ex_vat,
    selected_for_ledger.amount_vat,
    selected_for_ledger.amount_inc_vat,
    selected_for_ledger.economic_key_type,
    selected_for_ledger.economic_key_value,
    to_jsonb(ledger_items),
    to_jsonb(ledger_items) || jsonb_build_object(
      'settled_reversal_applied', true,
      'settled_reversal_at_utc', v_now,
      'work_item_id', p_work_item_id
    ),
    'APPLIED',
    v_now,
    v_now
  FROM pg_temp._tmp_settled_reversal_selected AS selected_for_ledger
  JOIN public.pay_batch_items AS ledger_items
    ON ledger_items.id = selected_for_ledger.pay_batch_item_id
  ON CONFLICT (pay_batch_item_id, correction_item_kind) WHERE status = 'APPLIED' AND pay_batch_item_id IS NOT NULL DO NOTHING;

  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_released_reservations;
  CREATE TEMP TABLE _tmp_settled_reversal_released_reservations ON COMMIT DROP AS
  WITH release_candidates AS (
    SELECT DISTINCT
      public.pay_advance_reservations.id,
      public.pay_advance_reservations.finance_case_id,
      public.pay_advance_reservations.finance_component_id,
      public.pay_advance_reservations.pay_batch_item_id,
      public.pay_advance_reservations.reserved_amount,
      public.pay_advance_reservations.reserved_source_amount
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_settled_reversal_selected AS selected_reservation_items
      ON selected_reservation_items.reservation_id = public.pay_advance_reservations.id
      OR selected_reservation_items.pay_batch_item_id = public.pay_advance_reservations.pay_batch_item_id
    WHERE public.pay_advance_reservations.pay_batch_id = v_work_item.pay_batch_id
      AND upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'SETTLED'
  ),
  released_rows AS (
    UPDATE public.pay_advance_reservations AS reservations_to_release
    SET
      status = 'RELEASED',
      released_at_utc = COALESCE(reservations_to_release.released_at_utc, v_now),
      released_reason = 'SETTLED_REVERSAL',
      updated_by_user_id = p_actor_user_id
    FROM release_candidates
    WHERE reservations_to_release.id = release_candidates.id
    RETURNING
      reservations_to_release.id,
      reservations_to_release.finance_case_id,
      reservations_to_release.finance_component_id,
      reservations_to_release.pay_batch_item_id,
      reservations_to_release.reserved_amount,
      reservations_to_release.reserved_source_amount
  )
  SELECT
    released_rows.id AS reservation_id,
    released_rows.finance_case_id,
    released_rows.finance_component_id,
    released_rows.pay_batch_item_id,
    released_rows.reserved_amount,
    released_rows.reserved_source_amount
  FROM released_rows;

  SELECT count(*)::integer
  INTO v_released_reservation_count
  FROM pg_temp._tmp_settled_reversal_released_reservations AS released_reservation_count;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_component_restore;
  CREATE TEMP TABLE _tmp_settled_reversal_component_restore ON COMMIT DROP AS
  SELECT
    restore_source.finance_component_id,
    restore_source.finance_case_id,
    round(sum(restore_source.restore_source_amount), 2)::numeric AS restore_source_amount
  FROM (
    SELECT
      COALESCE(released_reservations.finance_component_id, public.pay_batch_items.finance_component_id) AS finance_component_id,
      COALESCE(released_reservations.finance_case_id, public.pay_batch_items.finance_case_id) AS finance_case_id,
      round(abs(COALESCE(
        released_reservations.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(public.pay_batch_items.id),
        public.pay_batch_items.frozen_source_amount,
        released_reservations.reserved_amount,
        public.pay_batch_items.amount_ex_vat,
        public.pay_batch_items.amount_inc_vat,
        0
      )), 2)::numeric AS restore_source_amount
    FROM pg_temp._tmp_settled_reversal_released_reservations AS released_reservations
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.id = released_reservations.pay_batch_item_id
  ) AS restore_source
  WHERE restore_source.finance_component_id IS NOT NULL
    AND restore_source.restore_source_amount > 0
  GROUP BY restore_source.finance_component_id, restore_source.finance_case_id;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_component_restore_apply;
  CREATE TEMP TABLE _tmp_settled_reversal_component_restore_apply ON COMMIT DROP AS
  SELECT
    public.pay_finance_case_components.id AS finance_component_id,
    public.pay_finance_case_components.finance_case_id,
    public.pay_finance_case_components.remaining_source_amount AS remaining_before,
    LEAST(
      COALESCE(public.pay_finance_case_components.source_amount, 0),
      COALESCE(public.pay_finance_case_components.remaining_source_amount, 0) + COALESCE(component_restore.restore_source_amount, 0)
    ) AS remaining_after,
    component_restore.restore_source_amount
  FROM pg_temp._tmp_settled_reversal_component_restore AS component_restore
  JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = component_restore.finance_component_id;

  UPDATE public.pay_finance_case_components AS components_to_restore
  SET
    remaining_source_amount = component_restore_apply.remaining_after,
    resolved_at_utc = CASE WHEN component_restore_apply.remaining_after > 0 THEN NULL ELSE components_to_restore.resolved_at_utc END,
    closed_at_utc = NULL,
    updated_at_utc = v_now
  FROM pg_temp._tmp_settled_reversal_component_restore_apply AS component_restore_apply
  WHERE components_to_restore.id = component_restore_apply.finance_component_id;

  GET DIAGNOSTICS v_restored_component_count = ROW_COUNT;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    component_restore_apply.finance_case_id,
    component_restore_apply.finance_component_id,
    'COMPONENT_RESTORED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    NULL::uuid,
    jsonb_build_object('remaining_source_amount', component_restore_apply.remaining_before),
    jsonb_build_object(
      'remaining_source_amount', component_restore_apply.remaining_after,
      'restored_source_amount', component_restore_apply.restore_source_amount,
      'correction_kind', 'SETTLED_REVERSAL',
      'work_item_id', p_work_item_id
    ),
    'SETTLED_REVERSAL',
    'Payment correction settled reversal restored settled component amount.'
  FROM pg_temp._tmp_settled_reversal_component_restore_apply AS component_restore_apply;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    released_reservations.finance_case_id,
    released_reservations.finance_component_id,
    'RESERVATION_RELEASED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    released_reservations.reservation_id,
    jsonb_build_object('reservation_status', 'SETTLED'),
    jsonb_build_object(
      'reservation_status', 'RELEASED',
      'released_reason', 'SETTLED_REVERSAL',
      'work_item_id', p_work_item_id
    ),
    'SETTLED_REVERSAL',
    'Payment correction settled reversal released settled reservation.'
  FROM pg_temp._tmp_settled_reversal_released_reservations AS released_reservations
  WHERE released_reservations.finance_case_id IS NOT NULL;

  UPDATE public.pay_advances AS payout_cases_to_reset
  SET
    payout_status = 'PENDING',
    payout_pay_batch_id = NULL,
    payout_transfer_id = NULL,
    updated_at = v_now
  WHERE payout_cases_to_reset.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND upper(btrim(COALESCE(payout_cases_to_reset.payout_status::text, ''))) = 'PAID'
    AND (
      payout_cases_to_reset.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases_to_reset.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_settled_reversal_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  GET DIAGNOSTICS v_reset_payout_count = ROW_COUNT;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(v_work_unit, ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', v_work_item.pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id::text),
    'is_whole_batch', v_is_whole_batch_work_item,
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(v_work_item.selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.candidate_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.umbrella_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_case_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_component_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.reservation_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.transfer_group_key AS value_text
        FROM pg_temp._tmp_settled_reversal_selected AS selected_scope
        WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_mail_selected_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_mail_scope_matches;
  CREATE TEMP TABLE _tmp_settled_reversal_mail_scope_matches ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.type,
      public.mail_outbox.email_type,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.reference,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) = 'QUEUED'
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id AS mail_outbox_id,
      candidate_mail.status,
      candidate_mail.type,
      candidate_mail.email_type,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.reference,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        v_work_item.pay_batch_id,
        v_work_item.selection_json,
        v_mail_selected_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.mail_outbox_id,
    matched_mail.status,
    matched_mail.type,
    matched_mail.email_type,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.reference,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS match_kind,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  FROM pg_temp._tmp_settled_reversal_mail_scope_matches AS mail_scope_match
  WHERE queued_mail_to_cancel.id = mail_scope_match.mail_outbox_id
    AND upper(btrim(COALESCE(queued_mail_to_cancel.status::text, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.safe_to_cancel, false)
    AND (
      mail_scope_match.match_confidence = 'EXACT'
      OR (
        v_is_whole_batch_work_item
        AND mail_scope_match.match_kind = 'WHOLE_BATCH'
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_communications_review_required_count
  FROM pg_temp._tmp_settled_reversal_mail_scope_matches AS mail_scope_match
  WHERE upper(btrim(COALESCE(mail_scope_match.status, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.requires_review, false);

  v_mail_scope_matching := jsonb_build_object(
    'exact_cancelled', v_cancelled_mail_count,
    'legacy_review', v_communications_review_required_count,
    'selected_scope_json', v_mail_selected_scope_json,
    'matches', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', mail_scope_match.mail_outbox_id,
        'match_kind', mail_scope_match.match_kind,
        'match_confidence', mail_scope_match.match_confidence,
        'safe_to_cancel', mail_scope_match.safe_to_cancel,
        'requires_review', mail_scope_match.requires_review,
        'reason', mail_scope_match.match_reason,
        'status', mail_scope_match.status,
        'type', mail_scope_match.type,
        'email_type', mail_scope_match.email_type,
        'context_kind', mail_scope_match.context_kind,
        'context_id', mail_scope_match.context_id,
        'recipient_kind', mail_scope_match.recipient_kind,
        'recipient_id', mail_scope_match.recipient_id,
        'reference', mail_scope_match.reference,
        'payment_scope_json', mail_scope_match.payment_scope_json
      ) ORDER BY mail_scope_match.mail_outbox_id)
      FROM pg_temp._tmp_settled_reversal_mail_scope_matches AS mail_scope_match
    ), '[]'::jsonb)
  );


  DROP TABLE IF EXISTS pg_temp._tmp_settled_reversal_latest_state;
  CREATE TEMP TABLE _tmp_settled_reversal_latest_state ON COMMIT DROP AS
  WITH selected_timesheets AS (
    SELECT DISTINCT selected_state_timesheets.timesheet_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_state_timesheets
    WHERE selected_state_timesheets.timesheet_id IS NOT NULL
  ),
  active_history_rows AS (
    SELECT
      history_rows.timesheet_id,
      history_rows.pay_batch_id,
      history_rows.snapshot_json,
      history_rows.signature,
      history_rows.settled_at_utc,
      row_number() OVER (
        PARTITION BY history_rows.timesheet_id
        ORDER BY history_rows.settled_at_utc DESC, history_rows.id DESC
      ) AS row_number_for_timesheet
    FROM public.timesheet_pay_state_history AS history_rows
    JOIN selected_timesheets
      ON selected_timesheets.timesheet_id = history_rows.timesheet_id
    WHERE EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS history_batch_items
      JOIN public.pay_batch_candidates AS history_batch_candidates
        ON history_batch_candidates.id = history_batch_items.pay_batch_candidate_id
      LEFT JOIN public.pay_bank_transfers AS history_transfers
        ON history_transfers.id = history_batch_items.pay_bank_transfer_id
      WHERE history_batch_candidates.pay_batch_id = history_rows.pay_batch_id
        AND history_batch_items.timesheet_id = history_rows.timesheet_id
        AND COALESCE(history_batch_items.is_voided, false) = false
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS applied_corrections
          WHERE applied_corrections.pay_batch_item_id = history_batch_items.id
            AND applied_corrections.status = 'APPLIED'
            AND applied_corrections.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
        )
        AND (
          upper(btrim(COALESCE(history_batch_candidates.settlement_status, ''))) = 'SETTLED'
          OR history_batch_candidates.settled_at_utc IS NOT NULL
          OR upper(btrim(COALESCE(history_transfers.status, ''))) = 'COMPLETED'
          OR history_transfers.completed_at_utc IS NOT NULL
        )
    )
  )
  SELECT
    active_history_rows.timesheet_id,
    active_history_rows.pay_batch_id,
    active_history_rows.snapshot_json,
    active_history_rows.signature,
    active_history_rows.settled_at_utc
  FROM active_history_rows
  WHERE active_history_rows.row_number_for_timesheet = 1;

  UPDATE public.timesheet_pay_state AS current_timesheet_state
  SET
    last_settled_snapshot_json = latest_state.snapshot_json,
    last_settled_signature = latest_state.signature,
    last_settled_pay_batch_id = latest_state.pay_batch_id,
    last_settled_at_utc = latest_state.settled_at_utc
  FROM pg_temp._tmp_settled_reversal_latest_state AS latest_state
  WHERE current_timesheet_state.timesheet_id = latest_state.timesheet_id;

  GET DIAGNOSTICS v_timesheet_state_updated_count = ROW_COUNT;

  DELETE FROM public.timesheet_pay_state AS current_timesheet_state
  WHERE current_timesheet_state.timesheet_id IN (
    SELECT DISTINCT selected_state_delete.timesheet_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_state_delete
    WHERE selected_state_delete.timesheet_id IS NOT NULL
  )
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._tmp_settled_reversal_latest_state AS latest_state
      WHERE latest_state.timesheet_id = current_timesheet_state.timesheet_id
    );

  GET DIAGNOSTICS v_timesheet_state_deleted_count = ROW_COUNT;

  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
  SELECT
    'pay_candidate:' || dirty_candidates.candidate_id::text,
    1,
    v_now
  FROM (
    SELECT DISTINCT selected_dirty_candidates.candidate_id
    FROM pg_temp._tmp_settled_reversal_selected AS selected_dirty_candidates
    WHERE selected_dirty_candidates.candidate_id IS NOT NULL
  ) AS dirty_candidates
  ON CONFLICT (entity_key)
  DO UPDATE
  SET
    seq = public.app_change_counters.seq + 1,
    updated_at = v_now;

  GET DIAGNOSTICS v_dirty_candidate_count = ROW_COUNT;

  IF v_batch.source_snapshot_run_id IS NOT NULL THEN
    FOR v_candidate_id IN
      SELECT DISTINCT selected_refresh_candidates.candidate_id
      FROM pg_temp._tmp_settled_reversal_selected AS selected_refresh_candidates
      WHERE selected_refresh_candidates.candidate_id IS NOT NULL
      ORDER BY selected_refresh_candidates.candidate_id
    LOOP
      BEGIN
        v_refresh_result := public.pay_workbench_enqueue_candidate_refresh(
          p_snapshot_run_id => v_batch.source_snapshot_run_id,
          p_candidate_id => v_candidate_id,
          p_reason => 'PAYMENT_CORRECTION_SETTLED_REVERSAL',
          p_actor_user_id => p_actor_user_id,
          p_payload_json => jsonb_build_object(
            'pay_batch_id', v_work_item.pay_batch_id,
            'correction_request_id', v_work_item.correction_request_id,
            'work_item_id', p_work_item_id,
            'refresh_reason', 'PAYMENT_CORRECTION_SETTLED_REVERSAL'
          )
        );
      EXCEPTION
        WHEN OTHERS THEN
          PERFORM public._imp_debug_audit(
            p_actor_user_id,
            'PAYMENT_CORRECTION_SETTLED_REVERSAL_REFRESH_ENQUEUE_ERROR',
            jsonb_build_object(
              'work_item_id', p_work_item_id,
              'candidate_id', v_candidate_id,
              'snapshot_run_id', v_batch.source_snapshot_run_id,
              'sqlstate', SQLSTATE,
              'error_message', SQLERRM
            ),
            'pay_payment_correction',
            p_work_item_id::text,
            NULL::jsonb,
            NULL::text,
            NULL::text,
            NULL::text
          );
      END;
    END LOOP;
  END IF;



v_finance_resolution_result := public._pay_payment_correction_apply_accepted_finance_resolution(
  v_work_item.correction_request_id,
  p_work_item_id,
  v_effective_actor_user_id
);

IF NOT COALESCE(NULLIF(v_finance_resolution_result->>'ok', '')::boolean, false) THEN
  RAISE EXCEPTION 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED'
    USING ERRCODE = 'P0001',
          DETAIL = jsonb_build_object(
            'code', 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED',
            'message', 'Accepted gross/channel-sensitive finance resolution blocked settled reversal; no partial correction must be committed.',
            'work_item_id', p_work_item_id,
            'correction_request_id', v_work_item.correction_request_id,
            'finance_resolution_result', v_finance_resolution_result
          )::text;
END IF;

v_notice_queue_result := public.pay_payment_return_admin_notice_queue(
  p_notice_kind => 'SETTLED_REVERSAL_APPLIED',
  p_pay_batch_id => v_work_item.pay_batch_id,
  p_provider_key => COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
  p_execution_commit_ref => v_batch.execution_commit_ref,
  p_summary_json => jsonb_build_object(
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_request_id', v_work_item.correction_request_id,
    'work_item_id', p_work_item_id,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'timesheet_state_updated_count', v_timesheet_state_updated_count,
    'timesheet_state_deleted_count', v_timesheet_state_deleted_count,
    'accepted_finance_resolution', v_finance_resolution_result,
    'applied_at_utc', v_now
  )
);

v_notice_group_id := NULLIF(v_notice_queue_result->>'notice_group_id', '')::uuid;


  v_result := jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'work_item_id', p_work_item_id,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'SETTLED_REVERSAL',
    'correction_applied', true,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'timesheet_state_updated_count', v_timesheet_state_updated_count,
    'timesheet_state_deleted_count', v_timesheet_state_deleted_count,
    'dirty_candidate_count', v_dirty_candidate_count,
    'notice_group_id', v_notice_group_id,
    'notice_queue_result', v_notice_queue_result,
    'accepted_finance_resolution', v_finance_resolution_result,
    'classification_result', v_classification_result,
    'applied_at_utc', v_now
  );

  UPDATE public.pay_payment_correction_work_items AS applied_work_item
  SET
    status = 'APPLIED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = COALESCE(applied_work_item.processed_at_utc, v_now),
    last_error = NULL,
    result_json = COALESCE(applied_work_item.result_json, '{}'::jsonb) || v_result
  WHERE applied_work_item.id = p_work_item_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_SETTLED_REVERSAL_WORK_RESULT',
    v_result,
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    v_blocker := jsonb_build_object(
      'code', 'SETTLED_REVERSAL_WORK_ITEM_BLOCKED_BY_EXCEPTION',
      'message', 'Settled reversal work item was blocked after an exception; no partial correction was committed.',
      'work_item_id', p_work_item_id,
      'correction_request_id', CASE WHEN v_work_item.correction_request_id IS NULL THEN NULL ELSE v_work_item.correction_request_id END,
      'pay_batch_id', CASE WHEN v_work_item.pay_batch_id IS NULL THEN NULL ELSE v_work_item.pay_batch_id END,
      'sqlstate', SQLSTATE,
      'error_message', SQLERRM,
      'blocked_at_utc', v_now
    );

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_SETTLED_REVERSAL_WORK_ERROR',
      v_blocker,
      'pay_payment_correction',
      COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    IF p_work_item_id IS NOT NULL THEN
      UPDATE public.pay_payment_correction_work_items AS exception_blocked_work_item
      SET
        status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = COALESCE(exception_blocked_work_item.processed_at_utc, v_now),
        last_error = SQLERRM,
        result_json = COALESCE(exception_blocked_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'processed_at_utc', v_now
        )
      WHERE exception_blocked_work_item.id = p_work_item_id;
    END IF;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker
    );
END;
$function$;













CREATE OR REPLACE FUNCTION public._pay_active_settled_components(
  p_timesheet_ids uuid[]
)
RETURNS TABLE(
  timesheet_id uuid,
  key_type text,
  key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
WITH input_timesheets AS (
  SELECT DISTINCT
    input_timesheet_values.timesheet_id_value AS timesheet_id
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_values(timesheet_id_value)
  WHERE input_timesheet_values.timesheet_id_value IS NOT NULL
),
active_item_ids AS (
  SELECT DISTINCT
    public.pay_batch_items.id AS pay_batch_item_id
  FROM input_timesheets
  JOIN public.pay_batch_items
    ON public.pay_batch_items.timesheet_id = input_timesheets.timesheet_id
  JOIN public.pay_batch_candidates
    ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers
    ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
  WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(public.pay_batch_items.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND (
      UPPER(BTRIM(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
      OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
      OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
      OR public.pay_bank_transfers.completed_at_utc IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.timesheet_pay_state_history AS active_history
        WHERE active_history.pay_batch_id = public.pay_batch_candidates.pay_batch_id
          AND active_history.timesheet_id = public.pay_batch_items.timesheet_id
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_corrections
      WHERE applied_corrections.pay_batch_item_id = public.pay_batch_items.id
        AND applied_corrections.status = 'APPLIED'
        AND applied_corrections.correction_item_kind IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'SETTLED_REVERSAL'
        )
    )
),
active_components AS (
  SELECT
    economic_components.timesheet_id AS component_timesheet_id,
    economic_components.key_type AS component_key_type,
    economic_components.key_value AS component_key_value,
    economic_components.source_amount_ex_vat AS component_amount_ex_vat,
    economic_components.source_amount_inc_vat AS component_amount_inc_vat
  FROM active_item_ids
  JOIN LATERAL public._pay_batch_item_economic_components(
    NULL::uuid,
    ARRAY[active_item_ids.pay_batch_item_id]::uuid[]
  ) AS economic_components
    ON economic_components.pay_batch_item_id = active_item_ids.pay_batch_item_id
  WHERE economic_components.timesheet_id IS NOT NULL
    AND economic_components.key_type IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_type, '')) <> ''
    AND economic_components.key_value IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_value, '')) <> ''
    AND economic_components.key_resolution_failure_reason IS NULL
    AND UPPER(BTRIM(COALESCE(economic_components.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE'
    )
    AND NOT (
      UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) = 'TS_DAY'
      AND economic_components.key_value !~ '^\d{4}-\d{2}-\d{2}$'
    )
),
active_component_totals AS (
  SELECT
    active_components.component_timesheet_id AS timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)) AS key_type,
    active_components.component_key_value AS key_value,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM active_components
  GROUP BY
    active_components.component_timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)),
    active_components.component_key_value
)
SELECT
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value,
  active_component_totals.amount_ex_vat,
  active_component_totals.amount_inc_vat
FROM active_component_totals
WHERE active_component_totals.timesheet_id IS NOT NULL
  AND active_component_totals.key_type IS NOT NULL
  AND active_component_totals.key_value IS NOT NULL
  AND (
    ROUND(COALESCE(active_component_totals.amount_ex_vat, 0), 2) <> 0
    OR ROUND(COALESCE(active_component_totals.amount_inc_vat, 0), 2) <> 0
  )
ORDER BY
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value;
$function$;





CREATE OR REPLACE FUNCTION public.pay_bank_event_ingest(
  p_event_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_event_json jsonb := COALESCE(p_event_json, '{}'::jsonb);
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_now timestamptz := now();

  v_pay_batch_id uuid := NULL::uuid;
  v_supplied_pay_batch_id uuid := NULL::uuid;
  v_pay_batch_id_text text;
  v_pay_bank_transfer_id uuid := NULL::uuid;
  v_pay_bank_transfer_id_text text;
  v_candidate_id uuid := NULL::uuid;
  v_candidate_id_text text;
  v_umbrella_id uuid := NULL::uuid;
  v_umbrella_id_text text;

  v_provider_key text;
  v_provider_event_id text;
  v_provider_reference text;
  v_provider_state text;
  v_normalised_state text;
  v_event_source text;
  v_event_time_utc timestamptz := NULL::timestamptz;
  v_event_time_text text;
  v_amount numeric := NULL::numeric;
  v_amount_text text;
  v_currency text := 'GBP';
  v_raw_payload jsonb := '{}'::jsonb;
  v_idempotency_key text;

  v_mapping_status text := 'UNMATCHED';
  v_mapping_method text := 'UNMATCHED';
  v_mapping_candidate_count integer := 0;

  v_transfer public.pay_bank_transfers%rowtype;
  v_batch public.pay_batches%rowtype;
  v_event_id uuid := NULL::uuid;
  v_inserted_event boolean := false;

  v_selection_json jsonb := '{}'::jsonb;
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_safe_to_auto_apply boolean := false;
  v_auto_setting boolean := false;
  v_correction_disposition text := 'AMBIGUOUS';

  v_correction_request_id uuid := NULL::uuid;
  v_request_start_result jsonb := NULL::jsonb;
  v_expand_result jsonb := NULL::jsonb;
  v_process_result jsonb := NULL::jsonb;

  v_admin_notice_group_id uuid := NULL::uuid;
  v_admin_notice_result jsonb := NULL::jsonb;
  v_notice_kind text := 'BANK_FAILURE_DETECTED';
  v_exact_mapping_required_blocker jsonb := NULL::jsonb;

  v_provider_state_upper text := NULL::text;
  v_final_work_item_totals jsonb := jsonb_build_object(
    'total', 0,
    'applied', 0,
    'skipped', 0,
    'blocked', 0,
    'failed_retryable', 0,
    'failed_final', 0,
    'pending', 0,
    'processing', 0
  );
  v_work_total_count integer := 0;
  v_work_applied_count integer := 0;
  v_work_skipped_count integer := 0;
  v_work_blocked_count integer := 0;
  v_work_failed_retryable_count integer := 0;
  v_work_failed_final_count integer := 0;
  v_work_pending_count integer := 0;
  v_work_processing_count integer := 0;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_BANK_EVENT_INGEST_START',
    jsonb_build_object(
      'actor_user_id', p_actor_user_id,
      'event_keys', CASE
        WHEN p_event_json IS NULL OR jsonb_typeof(p_event_json) <> 'object' THEN '[]'::jsonb
        ELSE COALESCE((
          SELECT jsonb_agg(event_keys.key_name ORDER BY event_keys.key_name)
          FROM jsonb_object_keys(p_event_json) AS event_keys(key_name)
        ), '[]'::jsonb)
      END
    ),
    'pay_payment_correction',
    'BANK_EVENT_INGEST',
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_event_json IS NULL OR COALESCE(jsonb_typeof(p_event_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANK_EVENT_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANK_EVENT_JSON_MUST_BE_OBJECT')::text;
  END IF;

  v_pay_batch_id_text := nullif(btrim(COALESCE(
    v_event_json->>'pay_batch_id',
    v_event_json->>'batch_id',
    ''
  )), '');

  v_pay_bank_transfer_id_text := nullif(btrim(COALESCE(
    v_event_json->>'pay_bank_transfer_id',
    v_event_json->>'transfer_id',
    v_event_json->>'bank_transfer_id',
    ''
  )), '');

  v_candidate_id_text := nullif(btrim(COALESCE(v_event_json->>'candidate_id', '')), '');
  v_umbrella_id_text := nullif(btrim(COALESCE(v_event_json->>'umbrella_id', '')), '');

  IF v_pay_batch_id_text IS NOT NULL THEN
    IF v_pay_batch_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_PAY_BATCH_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_ID_IN_BANK_EVENT', 'pay_batch_id', v_pay_batch_id_text)::text;
    END IF;
    v_pay_batch_id := v_pay_batch_id_text::uuid;
    v_supplied_pay_batch_id := v_pay_batch_id;
  END IF;

  IF v_pay_bank_transfer_id_text IS NOT NULL THEN
    IF v_pay_bank_transfer_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_PAY_BANK_TRANSFER_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_PAY_BANK_TRANSFER_ID_IN_BANK_EVENT', 'pay_bank_transfer_id', v_pay_bank_transfer_id_text)::text;
    END IF;
    v_pay_bank_transfer_id := v_pay_bank_transfer_id_text::uuid;
  END IF;

  IF v_candidate_id_text IS NOT NULL THEN
    IF v_candidate_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_CANDIDATE_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_CANDIDATE_ID_IN_BANK_EVENT', 'candidate_id', v_candidate_id_text)::text;
    END IF;
    v_candidate_id := v_candidate_id_text::uuid;
  END IF;

  IF v_umbrella_id_text IS NOT NULL THEN
    IF v_umbrella_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_UMBRELLA_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_UMBRELLA_ID_IN_BANK_EVENT', 'umbrella_id', v_umbrella_id_text)::text;
    END IF;
    v_umbrella_id := v_umbrella_id_text::uuid;
  END IF;

  v_provider_key := upper(nullif(btrim(COALESCE(v_event_json->>'provider_key', v_event_json->>'provider', v_event_json->>'rail_provider', '')), ''));
  v_provider_event_id := nullif(btrim(COALESCE(v_event_json->>'provider_event_id', v_event_json->>'event_id', v_event_json->>'id', '')), '');
  v_provider_reference := nullif(btrim(COALESCE(
    v_event_json->>'provider_reference',
    v_event_json->>'provider_ref',
    v_event_json->>'rail_tx_id',
    v_event_json->>'request_id',
    v_event_json->>'payment_reference',
    ''
  )), '');
  v_provider_state := nullif(btrim(COALESCE(v_event_json->>'provider_state', v_event_json->>'state', v_event_json->>'status', '')), '');
  v_normalised_state := upper(nullif(btrim(COALESCE(v_event_json->>'normalised_state', v_event_json->>'normalized_state', v_provider_state, 'UNKNOWN')), ''));
  v_event_source := upper(nullif(btrim(COALESCE(v_event_json->>'event_source', v_event_json->>'source', 'PROVIDER_POLL')), ''));
  v_event_time_text := nullif(btrim(COALESCE(v_event_json->>'event_time_utc', v_event_json->>'event_time', v_event_json->>'created_at', '')), '');
  v_amount_text := nullif(btrim(COALESCE(v_event_json->>'amount', v_event_json->>'amount_inc_vat', '')), '');
  v_currency := upper(nullif(btrim(COALESCE(v_event_json->>'currency', 'GBP')), ''));
  v_raw_payload := COALESCE(v_event_json->'raw_payload', v_event_json);

  IF v_event_time_text IS NOT NULL THEN
    BEGIN
      v_event_time_utc := v_event_time_text::timestamptz;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'INVALID_BANK_EVENT_TIME'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'INVALID_BANK_EVENT_TIME', 'event_time_utc', v_event_time_text)::text;
    END;
  END IF;

  IF v_amount_text IS NOT NULL THEN
    BEGIN
      v_amount := v_amount_text::numeric;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'INVALID_BANK_EVENT_AMOUNT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'INVALID_BANK_EVENT_AMOUNT', 'amount', v_amount_text)::text;
    END;
  END IF;

  v_normalised_state := CASE
    WHEN v_normalised_state IN ('SUCCESS', 'SUCCEEDED', 'COMPLETE', 'COMPLETED', 'SETTLED', 'PAID') THEN 'COMPLETED'
    WHEN v_normalised_state IN ('FAIL', 'FAILED', 'DECLINED', 'REJECTED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT') THEN 'FAILED'
    WHEN v_normalised_state IN ('CANCELED', 'CANCELLED') THEN 'CANCELLED'
    WHEN v_normalised_state IN ('REVERTED', 'REVERT', 'REVERSED') THEN 'REVERTED'
    WHEN v_normalised_state IN ('RETURNED', 'RETURN') THEN 'RETURNED'
    WHEN v_normalised_state IN ('SUBMITTED', 'SENT') THEN 'SUBMITTED'
    WHEN v_normalised_state IN ('PENDING') THEN 'PENDING'
    WHEN v_normalised_state IN ('PROCESSING') THEN 'PROCESSING'
    WHEN v_normalised_state IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT') THEN 'UNKNOWN'
    ELSE 'UNKNOWN'
  END;

  v_provider_state_upper := upper(btrim(coalesce(v_provider_state, '')));

  IF v_event_source NOT IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'MANUAL_CONFIRM', 'MANUAL_EVIDENCE', 'SYSTEM') THEN
    v_event_source := CASE
      WHEN v_event_source LIKE '%WEBHOOK%' THEN 'PROVIDER_WEBHOOK'
      WHEN v_event_source LIKE '%MANUAL%EVIDENCE%' THEN 'MANUAL_EVIDENCE'
      WHEN v_event_source LIKE '%MANUAL%' THEN 'MANUAL_CONFIRM'
      WHEN v_event_source LIKE '%SYSTEM%' THEN 'SYSTEM'
      ELSE 'PROVIDER_POLL'
    END;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    SELECT public.pay_bank_transfers.*
    INTO v_transfer
    FROM public.pay_bank_transfers
    WHERE public.pay_bank_transfers.id = v_pay_bank_transfer_id;

    IF v_transfer.id IS NULL THEN
      RAISE EXCEPTION 'BANK_TRANSFER_NOT_FOUND_FOR_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANK_TRANSFER_NOT_FOUND_FOR_EVENT', 'pay_bank_transfer_id', v_pay_bank_transfer_id)::text;
    END IF;

    IF v_supplied_pay_batch_id IS NOT NULL AND v_supplied_pay_batch_id <> v_transfer.pay_batch_id THEN
      RAISE EXCEPTION 'BANK_EVENT_TRANSFER_BATCH_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANK_EVENT_TRANSFER_BATCH_MISMATCH',
                'supplied_pay_batch_id', v_supplied_pay_batch_id,
                'transfer_pay_batch_id', v_transfer.pay_batch_id,
                'pay_bank_transfer_id', v_transfer.id
              )::text;
    END IF;

    v_pay_batch_id := v_transfer.pay_batch_id;
    v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
    v_umbrella_id := COALESCE(
      v_umbrella_id,
      v_transfer.umbrella_id,
      CASE
        WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
        ELSE NULL::uuid
      END
    );
    v_mapping_method := CASE
      WHEN v_event_source = 'MANUAL_EVIDENCE' THEN 'MANUAL_TRANSFER_SELECTION'
      ELSE 'TRANSFER_ID'
    END;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_reference IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE (
      public.pay_bank_transfers.request_id = v_provider_reference
      OR public.pay_bank_transfers.rail_tx_id = v_provider_reference
      OR public.pay_bank_transfers.payment_reference = v_provider_reference
    )
      AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE (
        public.pay_bank_transfers.request_id = v_provider_reference
        OR public.pay_bank_transfers.rail_tx_id = v_provider_reference
        OR public.pay_bank_transfers.payment_reference = v_provider_reference
      )
        AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id)
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := CASE
        WHEN v_transfer.request_id = v_provider_reference THEN 'REQUEST_ID'
        WHEN v_transfer.rail_tx_id = v_provider_reference THEN 'RAIL_TX_ID'
        WHEN v_transfer.payment_reference = v_provider_reference THEN 'PAYMENT_REFERENCE'
        ELSE 'PROVIDER_REFERENCE'
      END;
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_event_id IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE (
      public.pay_bank_transfers.request_id = v_provider_event_id
      OR public.pay_bank_transfers.rail_tx_id = v_provider_event_id
      OR public.pay_bank_transfers.payment_reference = v_provider_event_id
    )
      AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE (
        public.pay_bank_transfers.request_id = v_provider_event_id
        OR public.pay_bank_transfers.rail_tx_id = v_provider_event_id
        OR public.pay_bank_transfers.payment_reference = v_provider_event_id
      )
        AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id)
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := 'PROVIDER_EVENT_ID';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_pay_batch_id IS NOT NULL AND v_amount IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE public.pay_bank_transfers.pay_batch_id = v_pay_batch_id
      AND abs(COALESCE(public.pay_bank_transfers.amount, 0) - COALESCE(v_amount, 0)) <= 0.01;

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE public.pay_bank_transfers.pay_batch_id = v_pay_batch_id
        AND abs(COALESCE(public.pay_bank_transfers.amount, 0) - COALESCE(v_amount, 0)) <= 0.01
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := 'AMOUNT_ONLY_UNIQUE';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'BANK_EVENT_PAY_BATCH_ID_COULD_NOT_BE_RESOLVED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANK_EVENT_PAY_BATCH_ID_COULD_NOT_BE_RESOLVED',
              'pay_bank_transfer_id', v_pay_bank_transfer_id,
              'provider_reference', v_provider_reference
            )::text;
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANK_EVENT_PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANK_EVENT_PAY_BATCH_NOT_FOUND', 'pay_batch_id', v_pay_batch_id)::text;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    v_mapping_status := 'MATCHED';
    IF v_mapping_method IS NULL OR v_mapping_method = 'UNMATCHED' THEN
      v_mapping_method := 'TRANSFER_ID';
    END IF;
  ELSIF v_mapping_candidate_count > 1 THEN
    v_mapping_status := 'AMBIGUOUS';
    v_mapping_method := 'AMBIGUOUS';
  ELSE
    v_mapping_status := 'UNMATCHED';
    v_mapping_method := 'UNMATCHED';
  END IF;

  IF v_mapping_status = 'UNMATCHED'
     AND (
       COALESCE(v_event_json->>'legacy_no_artifact', 'false') IN ('true', 't', 'yes', 'y', '1')
       OR NOT EXISTS (
         SELECT 1
         FROM public.pay_bank_transfers AS legacy_artifact_check
         WHERE legacy_artifact_check.pay_batch_id = v_pay_batch_id
       )
     ) THEN
    v_mapping_status := 'LEGACY_NO_ARTIFACT';
    v_mapping_method := 'LEGACY_NO_ARTIFACT';
  END IF;

  v_idempotency_key := nullif(btrim(COALESCE(v_event_json->>'idempotency_key', '')), '');

  IF v_idempotency_key IS NULL THEN
    v_idempotency_key := CASE
      WHEN v_provider_key IS NOT NULL AND v_provider_event_id IS NOT NULL
        THEN v_provider_key || '|' || v_provider_event_id
      WHEN v_provider_key IS NOT NULL AND v_provider_reference IS NOT NULL
        THEN v_provider_key || '|' || v_provider_reference || '|' || v_normalised_state || '|' || COALESCE(v_amount::text, '') || '|' || COALESCE(v_event_time_utc::text, '')
      WHEN v_event_source IN ('MANUAL_CONFIRM', 'MANUAL_EVIDENCE')
        THEN 'MANUAL|' || v_pay_batch_id::text || '|' || COALESCE(v_pay_bank_transfer_id::text, 'NO_TRANSFER') || '|' || v_normalised_state || '|' || COALESCE(p_actor_user_id::text, 'SYSTEM') || '|' || COALESCE(v_event_time_utc::text, v_now::text)
      ELSE 'SYSTEM|' || v_pay_batch_id::text || '|' || COALESCE(v_pay_bank_transfer_id::text, 'NO_TRANSFER') || '|' || v_normalised_state || '|' || md5(v_event_json::text)
    END;
  END IF;

  INSERT INTO public.pay_bank_transfer_events(
    pay_batch_id,
    pay_bank_transfer_id,
    candidate_id,
    umbrella_id,
    provider_key,
    provider_event_id,
    provider_reference,
    provider_state,
    normalised_state,
    event_source,
    event_time_utc,
    received_at_utc,
    amount,
    currency,
    mapping_status,
    mapping_method,
    movement_classification,
    correction_disposition,
    raw_payload,
    idempotency_key,
    created_at_utc
  )
  VALUES (
    v_pay_batch_id,
    v_pay_bank_transfer_id,
    v_candidate_id,
    v_umbrella_id,
    v_provider_key,
    v_provider_event_id,
    v_provider_reference,
    v_provider_state,
    v_normalised_state,
    v_event_source,
    v_event_time_utc,
    v_now,
    v_amount,
    COALESCE(v_currency, 'GBP'),
    v_mapping_status,
    v_mapping_method,
    NULL::text,
    NULL::text,
    COALESCE(v_raw_payload, '{}'::jsonb),
    v_idempotency_key,
    v_now
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING public.pay_bank_transfer_events.id
  INTO v_event_id;

  v_inserted_event := v_event_id IS NOT NULL;

  IF v_event_id IS NULL THEN
    SELECT public.pay_bank_transfer_events.id,
           public.pay_bank_transfer_events.mapping_status,
           public.pay_bank_transfer_events.mapping_method,
           public.pay_bank_transfer_events.movement_classification,
           public.pay_bank_transfer_events.correction_disposition,
           public.pay_bank_transfer_events.pay_bank_transfer_id,
           public.pay_bank_transfer_events.pay_batch_id,
           public.pay_bank_transfer_events.candidate_id,
           public.pay_bank_transfer_events.umbrella_id
    INTO v_event_id,
         v_mapping_status,
         v_mapping_method,
         v_classification,
         v_correction_disposition,
         v_pay_bank_transfer_id,
         v_pay_batch_id,
         v_candidate_id,
         v_umbrella_id
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.idempotency_key = v_idempotency_key;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_IDEMPOTENT_EXISTING',
      jsonb_build_object(
        'event_id', v_event_id,
        'idempotency_key', v_idempotency_key,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', v_classification,
        'correction_disposition', v_correction_disposition
      ),
      'pay_payment_correction',
      COALESCE(v_event_id::text, v_pay_batch_id::text),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'event_id', v_event_id,
      'inserted', false,
      'mapping_status', v_mapping_status,
      'mapping_method', v_mapping_method,
      'classification', COALESCE(v_classification, 'UNKNOWN'),
      'correction_disposition', COALESCE(v_correction_disposition, 'ALREADY_RECORDED'),
      'correction_request_id', NULL::uuid,
      'admin_notice_group_id', NULL::uuid
    );
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    IF v_normalised_state = 'COMPLETED' THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = 'COMPLETED',
        completed_at_utc = COALESCE(transfer_to_update.completed_at_utc, v_event_time_utc, v_now),
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_tx_id = COALESCE(NULLIF(v_provider_reference, ''), transfer_to_update.rail_tx_id),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    ELSIF v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED') THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = v_normalised_state,
        failed_reason = CASE
          WHEN v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN COALESCE(NULLIF(v_provider_state, ''), transfer_to_update.failed_reason)
          ELSE transfer_to_update.failed_reason
        END,
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    ELSIF v_normalised_state IN ('SUBMITTED', 'PENDING', 'PROCESSING', 'UNKNOWN') THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = CASE
          WHEN v_normalised_state = 'SUBMITTED' THEN 'PENDING'
          WHEN v_normalised_state IN ('PENDING', 'PROCESSING', 'UNKNOWN') THEN v_normalised_state
          ELSE transfer_to_update.status
        END,
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_tx_id = COALESCE(NULLIF(v_provider_reference, ''), transfer_to_update.rail_tx_id),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    END IF;
  END IF;

  IF v_normalised_state = 'COMPLETED' THEN
    v_classification := NULL::text;
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
    v_classification_result := jsonb_build_object(
      'classification', NULL::text,
      'reasons', jsonb_build_array('COMPLETED_EVENT_NO_PAYMENT_CORRECTION_REQUIRED'),
      'evidence', jsonb_build_object(
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'normalised_state', v_normalised_state
      ),
      'counts', '{}'::jsonb,
      'blockers', '[]'::jsonb,
      'selected_amounts', '{}'::jsonb,
      'safe_to_auto_apply', false
    );

    UPDATE public.pay_bank_transfer_events AS bank_event_to_update
    SET
      movement_classification = NULL::text,
      correction_disposition = v_correction_disposition
    WHERE bank_event_to_update.id = v_event_id;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_COMPLETED_NO_CORRECTION',
      jsonb_build_object(
        'event_id', v_event_id,
        'pay_batch_id', v_pay_batch_id,
        'pay_bank_transfer_id', v_pay_bank_transfer_id,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'correction_disposition', v_correction_disposition
      ),
      'pay_payment_correction',
      v_event_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'event_id', v_event_id,
      'inserted', v_inserted_event,
      'mapping_status', v_mapping_status,
      'mapping_method', v_mapping_method,
      'classification', NULL::text,
      'correction_disposition', v_correction_disposition,
      'correction_request_id', NULL::uuid,
      'admin_notice_group_id', NULL::uuid,
      'selection_json', NULL::jsonb,
      'classification_result', v_classification_result,
      'auto_apply', jsonb_build_object(
        'auto_setting', false,
        'safe_to_auto_apply', false,
        'request_start_result', NULL::jsonb,
        'expand_result', NULL::jsonb,
        'process_result', NULL::jsonb,
        'blocker', NULL::jsonb
      )
    );
  END IF;

  IF v_normalised_state IN ('SUBMITTED', 'PENDING', 'PROCESSING', 'UNKNOWN') THEN
    IF v_mapping_status = 'MATCHED'
       AND v_mapping_method IN (
         'TRANSFER_ID',
         'PROVIDER_EVENT_ID',
         'PROVIDER_REFERENCE',
         'REQUEST_ID',
         'RAIL_TX_ID',
         'PAYMENT_REFERENCE',
         'MANUAL_TRANSFER_SELECTION'
       )
       AND v_provider_state_upper NOT IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT') THEN
      v_classification := NULL::text;
      v_correction_disposition := 'NO_CORRECTION_REQUIRED';
      v_classification_result := jsonb_build_object(
        'classification', NULL::text,
        'reasons', jsonb_build_array('NON_TERMINAL_PROVIDER_STATE_NO_PAYMENT_CORRECTION_REQUIRED'),
        'evidence', jsonb_build_object(
          'mapping_status', v_mapping_status,
          'mapping_method', v_mapping_method,
          'normalised_state', v_normalised_state,
          'provider_state', v_provider_state
        ),
        'counts', '{}'::jsonb,
        'blockers', '[]'::jsonb,
        'selected_amounts', '{}'::jsonb,
        'safe_to_auto_apply', false
      );

      UPDATE public.pay_bank_transfer_events AS bank_event_to_update
      SET
        movement_classification = NULL::text,
        correction_disposition = v_correction_disposition,
        mapping_method = v_mapping_method
      WHERE bank_event_to_update.id = v_event_id;

      PERFORM public._imp_debug_audit(
        p_actor_user_id,
        'PAYMENT_BANK_EVENT_INGEST_NON_TERMINAL_NO_CORRECTION',
        jsonb_build_object(
          'event_id', v_event_id,
          'pay_batch_id', v_pay_batch_id,
          'pay_bank_transfer_id', v_pay_bank_transfer_id,
          'normalised_state', v_normalised_state,
          'mapping_status', v_mapping_status,
          'mapping_method', v_mapping_method,
          'correction_disposition', v_correction_disposition
        ),
        'pay_payment_correction',
        v_event_id::text,
        NULL::jsonb,
        NULL::text,
        NULL::text,
        NULL::text
      );

      RETURN jsonb_build_object(
        'ok', true,
        'event_id', v_event_id,
        'inserted', v_inserted_event,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', NULL::text,
        'correction_disposition', v_correction_disposition,
        'correction_request_id', NULL::uuid,
        'admin_notice_group_id', NULL::uuid,
        'message', 'Provider state recorded; no correction action required yet.',
        'selection_json', NULL::jsonb,
        'classification_result', v_classification_result,
        'auto_apply', jsonb_build_object(
          'auto_setting', false,
          'safe_to_auto_apply', false,
          'request_start_result', NULL::jsonb,
          'expand_result', NULL::jsonb,
          'process_result', NULL::jsonb,
          'final_work_item_totals', v_final_work_item_totals,
          'blocker', NULL::jsonb
        )
      );
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'TRANSFER',
      'pay_bank_transfer_ids', jsonb_build_array(v_pay_bank_transfer_id::text),
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_normalised_state IN ('RETURNED', 'REVERTED') THEN 'REVERSE_SETTLED_PAYMENT'
        WHEN v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN 'UNWIND_FAILED_PAYMENT'
        ELSE 'REVIEW_BANK_EVIDENCE'
      END
    );
  ELSIF v_candidate_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'CANDIDATES',
      'pay_batch_candidate_ids', COALESCE((
        SELECT jsonb_agg(candidate_scope_rows.id::text ORDER BY candidate_scope_rows.id::text)
        FROM public.pay_batch_candidates AS candidate_scope_rows
        WHERE candidate_scope_rows.pay_batch_id = v_pay_batch_id
          AND candidate_scope_rows.candidate_id = v_candidate_id
      ), '[]'::jsonb),
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_normalised_state IN ('RETURNED', 'REVERTED') THEN 'REVERSE_SETTLED_PAYMENT'
        WHEN v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN 'UNWIND_FAILED_PAYMENT'
        ELSE 'REVIEW_BANK_EVIDENCE'
      END
    );
  ELSE
    v_selection_json := jsonb_build_object(
      'scope_type', 'BATCH',
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_normalised_state IN ('RETURNED', 'REVERTED') THEN 'REVERSE_SETTLED_PAYMENT'
        WHEN v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN 'UNWIND_FAILED_PAYMENT'
        ELSE 'REVIEW_BANK_EVIDENCE'
      END
    );
  END IF;

  IF v_mapping_status = 'MATCHED'
     AND v_mapping_method IN (
       'TRANSFER_ID',
       'PROVIDER_EVENT_ID',
       'PROVIDER_REFERENCE',
       'REQUEST_ID',
       'RAIL_TX_ID',
       'PAYMENT_REFERENCE',
       'MANUAL_TRANSFER_SELECTION'
     ) THEN
    v_classification_result := public._pay_payment_movement_classify(
      v_pay_batch_id,
      v_selection_json
    );
    v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
    v_safe_to_auto_apply := COALESCE((v_classification_result->>'safe_to_auto_apply')::boolean, false);
  ELSE
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
    v_classification_result := jsonb_build_object(
      'classification', v_classification,
      'reasons', jsonb_build_array('BANK_EVENT_MAPPING_NOT_STRONG_MATCH'),
      'evidence', jsonb_build_object(
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method
      ),
      'counts', '{}'::jsonb,
      'blockers', jsonb_build_array(jsonb_build_object(
        'code', 'BANK_EVENT_MAPPING_NOT_STRONG_MATCH',
        'message', 'Bank event could not be mapped to a transfer using a strong exact mapping method. Amount-only, unmatched, ambiguous, and legacy no-artifact mappings require manual review.'
      )),
      'selected_amounts', '{}'::jsonb,
      'safe_to_auto_apply', false
    );
    v_safe_to_auto_apply := false;
  END IF;

  SELECT COALESCE(public.settings_defaults.payment_return_auto_reverse_timesheets, false)
  INTO v_auto_setting
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_notice_kind := CASE
    WHEN v_classification = 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND_REQUIRED'
    WHEN v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL_REQUIRED'
    WHEN v_normalised_state IN ('RETURNED', 'REVERTED') THEN 'SETTLED_RETURN_DETECTED'
    ELSE 'BANK_FAILURE_DETECTED'
  END;

  IF v_normalised_state NOT IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED', 'SUBMITTED', 'UNKNOWN', 'PENDING', 'PROCESSING') THEN
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
  ELSIF v_classification IS NULL THEN
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
  ELSIF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    v_correction_disposition := 'AMBIGUOUS';
  ELSIF NOT COALESCE(v_auto_setting, false) THEN
    v_correction_disposition := 'ACTION_REQUIRED';
  ELSIF NOT COALESCE(v_safe_to_auto_apply, false) THEN
    v_correction_disposition := 'BLOCKED';
  ELSE
    v_correction_disposition := 'ACTION_REQUIRED';
  END IF;

  IF COALESCE(v_auto_setting, false)
     AND COALESCE(v_safe_to_auto_apply, false)
     AND v_mapping_status = 'MATCHED'
     AND v_mapping_method IN (
       'TRANSFER_ID',
       'PROVIDER_EVENT_ID',
       'PROVIDER_REFERENCE',
       'REQUEST_ID',
       'RAIL_TX_ID',
       'PAYMENT_REFERENCE',
       'MANUAL_TRANSFER_SELECTION'
     )
     AND v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED')
     AND v_classification IN ('NO_MONEY_UNWIND', 'TRUE_SETTLED_REVERSAL_REQUIRED') THEN
    BEGIN
      v_request_start_result := public.pay_payment_correction_request_start(
        p_pay_batch_id => v_pay_batch_id,
        p_selection_json => v_selection_json,
        p_reason => 'Automatic clean bank event correction',
        p_actor_user_id => NULL::uuid,
        p_source_bank_event_id => v_event_id,
        p_auto_requested => true,
        p_accepted_resolution_json => NULL::jsonb
      );

      v_correction_request_id := (v_request_start_result->>'correction_request_id')::uuid;

      IF v_correction_request_id IS NOT NULL THEN
        v_expand_result := public.pay_payment_correction_expand_work(v_correction_request_id, NULL::uuid);
        v_process_result := public.pay_payment_correction_process_chunk(v_correction_request_id, 50, 'bank-event-ingest');

        SELECT
          count(*)::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'APPLIED')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'SKIPPED')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'BLOCKED')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'FAILED_RETRYABLE')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'FAILED_FINAL')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'PENDING')::integer,
          count(*) FILTER (WHERE correction_work_item.status = 'PROCESSING')::integer
        INTO
          v_work_total_count,
          v_work_applied_count,
          v_work_skipped_count,
          v_work_blocked_count,
          v_work_failed_retryable_count,
          v_work_failed_final_count,
          v_work_pending_count,
          v_work_processing_count
        FROM public.pay_payment_correction_work_items AS correction_work_item
        WHERE correction_work_item.correction_request_id = v_correction_request_id;

        v_final_work_item_totals := jsonb_build_object(
          'total', COALESCE(v_work_total_count, 0),
          'applied', COALESCE(v_work_applied_count, 0),
          'skipped', COALESCE(v_work_skipped_count, 0),
          'blocked', COALESCE(v_work_blocked_count, 0),
          'failed_retryable', COALESCE(v_work_failed_retryable_count, 0),
          'failed_final', COALESCE(v_work_failed_final_count, 0),
          'pending', COALESCE(v_work_pending_count, 0),
          'processing', COALESCE(v_work_processing_count, 0)
        );

        IF COALESCE(v_work_total_count, 0) <= 0 THEN
          v_correction_disposition := 'FAILED';
          v_exact_mapping_required_blocker := jsonb_build_object(
            'code', 'AUTO_CORRECTION_NO_WORK_ITEMS_PROCESSED',
            'message', 'Automatic payment correction created a request but no work items were available to apply.'
          );
        ELSIF COALESCE(v_work_blocked_count, 0) > 0 THEN
          v_correction_disposition := 'BLOCKED';
          v_exact_mapping_required_blocker := jsonb_build_object(
            'code', 'AUTO_CORRECTION_WORK_ITEMS_BLOCKED',
            'message', 'Automatic payment correction could not fully apply because one or more work items are blocked.',
            'final_work_item_totals', v_final_work_item_totals
          );
        ELSIF COALESCE(v_work_failed_retryable_count, 0) > 0 OR COALESCE(v_work_failed_final_count, 0) > 0 THEN
          v_correction_disposition := 'FAILED';
          v_exact_mapping_required_blocker := jsonb_build_object(
            'code', 'AUTO_CORRECTION_WORK_ITEMS_FAILED',
            'message', 'Automatic payment correction could not fully apply because one or more work items failed.',
            'final_work_item_totals', v_final_work_item_totals
          );
        ELSIF COALESCE(v_work_pending_count, 0) > 0 OR COALESCE(v_work_processing_count, 0) > 0 THEN
          v_correction_disposition := 'AUTO_PROCESSING';
          v_exact_mapping_required_blocker := NULL::jsonb;
        ELSIF COALESCE(v_work_applied_count, 0) + COALESCE(v_work_skipped_count, 0) = COALESCE(v_work_total_count, 0) THEN
          v_correction_disposition := 'AUTO_APPLIED';
          v_exact_mapping_required_blocker := NULL::jsonb;
        ELSE
          v_correction_disposition := 'FAILED';
          v_exact_mapping_required_blocker := jsonb_build_object(
            'code', 'AUTO_CORRECTION_WORK_ITEM_STATUS_UNRESOLVED',
            'message', 'Automatic payment correction finished with unresolved work item status totals.',
            'final_work_item_totals', v_final_work_item_totals
          );
        END IF;
      ELSE
        v_correction_disposition := 'BLOCKED';
        v_exact_mapping_required_blocker := jsonb_build_object(
          'code', 'AUTO_CORRECTION_REQUEST_NOT_CREATED',
          'message', 'Automatic payment correction could not create a correction request.'
        );
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        v_correction_disposition := 'BLOCKED';
        v_exact_mapping_required_blocker := jsonb_build_object(
          'code', 'AUTO_CORRECTION_APPLY_FAILED',
          'sqlstate', SQLSTATE,
          'error_message', SQLERRM
        );

        PERFORM public._imp_debug_audit(
          p_actor_user_id,
          'PAYMENT_BANK_EVENT_INGEST_AUTO_APPLY_ERROR',
          jsonb_build_object(
            'event_id', v_event_id,
            'pay_batch_id', v_pay_batch_id,
            'classification', v_classification,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM
          ),
          'pay_payment_correction',
          v_event_id::text,
          NULL::jsonb,
          NULL::text,
          NULL::text,
          NULL::text
        );
    END;
  END IF;

  IF v_correction_disposition NOT IN ('NO_CORRECTION_REQUIRED', 'AUTO_PROCESSING') THEN
    v_admin_notice_result := public.pay_payment_return_admin_notice_queue(
      p_notice_kind => CASE
        WHEN v_correction_disposition = 'AMBIGUOUS' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'BLOCKED' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'FAILED' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'AUTO_APPLIED' AND v_classification = 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND_APPLIED'
        WHEN v_correction_disposition = 'AUTO_APPLIED' AND v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL_APPLIED'
        ELSE v_notice_kind
      END,
      p_pay_batch_id => v_pay_batch_id,
      p_provider_key => COALESCE(v_provider_key, v_batch.rail_provider_snapshot, 'UNKNOWN'),
      p_execution_commit_ref => v_batch.execution_commit_ref,
      p_summary_json => jsonb_build_object(
        'pay_batch_id', v_pay_batch_id,
        'pay_bank_transfer_id', v_pay_bank_transfer_id,
        'bank_event_id', v_event_id,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', v_classification,
        'correction_disposition', v_correction_disposition,
        'provider_key', v_provider_key,
        'provider_state', v_provider_state,
        'normalised_state', v_normalised_state,
        'amount', v_amount,
        'currency', v_currency,
        'correction_request_id', CASE WHEN v_correction_request_id IS NULL THEN NULL ELSE v_correction_request_id::text END,
        'request_start_result', COALESCE(v_request_start_result, '{}'::jsonb),
        'expand_result', COALESCE(v_expand_result, '{}'::jsonb),
        'process_result', COALESCE(v_process_result, '{}'::jsonb),
        'final_work_item_totals', COALESCE(v_final_work_item_totals, '{}'::jsonb),
        'blocker', v_exact_mapping_required_blocker
      )
    );

    v_admin_notice_group_id := NULLIF(v_admin_notice_result->>'notice_group_id', '')::uuid;
  END IF;

  UPDATE public.pay_bank_transfer_events AS bank_event_to_update
  SET
    movement_classification = v_classification,
    correction_disposition = v_correction_disposition,
    mapping_method = v_mapping_method
  WHERE bank_event_to_update.id = v_event_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_BANK_EVENT_INGEST_RESULT',
    jsonb_build_object(
      'event_id', v_event_id,
      'inserted', v_inserted_event,
      'pay_batch_id', v_pay_batch_id,
      'pay_bank_transfer_id', v_pay_bank_transfer_id,
      'mapping_status', v_mapping_status,
      'mapping_method', v_mapping_method,
      'classification', v_classification,
      'safe_to_auto_apply', v_safe_to_auto_apply,
      'auto_setting', v_auto_setting,
      'correction_disposition', v_correction_disposition,
      'correction_request_id', v_correction_request_id,
      'admin_notice_group_id', v_admin_notice_group_id
    ),
    'pay_payment_correction',
    v_event_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'event_id', v_event_id,
    'inserted', v_inserted_event,
    'mapping_status', v_mapping_status,
    'mapping_method', v_mapping_method,
    'classification', v_classification,
    'correction_disposition', v_correction_disposition,
    'correction_request_id', v_correction_request_id,
    'admin_notice_group_id', v_admin_notice_group_id,
    'selection_json', v_selection_json,
    'classification_result', v_classification_result,
    'auto_apply', jsonb_build_object(
      'auto_setting', v_auto_setting,
      'safe_to_auto_apply', v_safe_to_auto_apply,
      'request_start_result', v_request_start_result,
      'expand_result', v_expand_result,
      'process_result', v_process_result,
      'final_work_item_totals', v_final_work_item_totals,
      'status', v_correction_disposition,
      'requires_user_action', v_correction_disposition IN ('ACTION_REQUIRED', 'AMBIGUOUS', 'BLOCKED', 'FAILED'),
      'blocker', v_exact_mapping_required_blocker
    )
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_ERROR',
      jsonb_build_object(
        'event_json', p_event_json,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      'BANK_EVENT_INGEST',
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_payment_return_admin_notice_queue(
  p_notice_kind text,
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_provider_key text DEFAULT NULL::text,
  p_execution_commit_ref text DEFAULT NULL::text,
  p_summary_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_notice_kind text;
  v_provider_key text;
  v_execution_commit_ref text;
  v_summary_json jsonb := '{}'::jsonb;
  v_recipient_role text := 'ADMIN';
  v_quiet_minutes integer := 10;
  v_max_wait_minutes integer := 60;
  v_now timestamptz := now();
  v_existing_group public.pay_payment_return_notice_groups%rowtype;
  v_notice_group_id uuid := NULL::uuid;
  v_quiet_until_utc timestamptz;
  v_max_send_at_utc timestamptz;
  v_merge_count integer := 1;
  v_return_json jsonb := '{}'::jsonb;
BEGIN
  v_notice_kind := upper(nullif(btrim(COALESCE(p_notice_kind, '')), ''));
  v_provider_key := upper(nullif(btrim(COALESCE(p_provider_key, '')), ''));
  v_execution_commit_ref := nullif(btrim(COALESCE(p_execution_commit_ref, '')), '');

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_RETURN_ADMIN_NOTICE_QUEUE_START',
    jsonb_build_object(
      'notice_kind', v_notice_kind,
      'pay_batch_id', p_pay_batch_id,
      'provider_key', v_provider_key,
      'execution_commit_ref', v_execution_commit_ref,
      'summary_json_keys', CASE
        WHEN p_summary_json IS NULL OR jsonb_typeof(p_summary_json) <> 'object' THEN '[]'::jsonb
        ELSE COALESCE((
          SELECT jsonb_agg(summary_keys.key_name ORDER BY summary_keys.key_name)
          FROM jsonb_object_keys(p_summary_json) AS summary_keys(key_name)
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

  IF v_notice_kind IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_RETURN_ADMIN_NOTICE_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_RETURN_ADMIN_NOTICE_KIND_REQUIRED')::text;
  END IF;

  IF v_notice_kind NOT IN (
    'BANK_FAILURE_DETECTED',
    'NO_MONEY_UNWIND_REQUIRED',
    'NO_MONEY_UNWIND_APPLIED',
    'SETTLED_RETURN_DETECTED',
    'SETTLED_REVERSAL_REQUIRED',
    'SETTLED_REVERSAL_APPLIED',
    'AUTO_CORRECTION_BLOCKED',
    'MANUAL_CORRECTION_APPLIED'
  ) THEN
    RAISE EXCEPTION 'PAYMENT_RETURN_ADMIN_NOTICE_KIND_UNSUPPORTED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_RETURN_ADMIN_NOTICE_KIND_UNSUPPORTED',
              'notice_kind', v_notice_kind
            )::text;
  END IF;

  IF p_summary_json IS NOT NULL AND COALESCE(jsonb_typeof(p_summary_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_RETURN_ADMIN_NOTICE_SUMMARY_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_RETURN_ADMIN_NOTICE_SUMMARY_MUST_BE_OBJECT',
              'notice_kind', v_notice_kind
            )::text;
  END IF;

  v_summary_json := COALESCE(p_summary_json, '{}'::jsonb);

  SELECT
    COALESCE(NULLIF(btrim(public.settings_defaults.payment_return_admin_recipient_role), ''), 'ADMIN'),
    COALESCE(public.settings_defaults.payment_return_admin_notice_quiet_minutes, 10),
    COALESCE(public.settings_defaults.payment_return_admin_notice_max_wait_minutes, 60)
  INTO
    v_recipient_role,
    v_quiet_minutes,
    v_max_wait_minutes
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_recipient_role := COALESCE(NULLIF(btrim(v_recipient_role), ''), 'ADMIN');
  v_quiet_minutes := LEAST(GREATEST(COALESCE(v_quiet_minutes, 10), 0), 1440);
  v_max_wait_minutes := LEAST(GREATEST(COALESCE(v_max_wait_minutes, 60), v_quiet_minutes), 1440);

  SELECT public.pay_payment_return_notice_groups.*
  INTO v_existing_group
  FROM public.pay_payment_return_notice_groups
  WHERE public.pay_payment_return_notice_groups.status = 'OPEN'
    AND public.pay_payment_return_notice_groups.notice_kind = v_notice_kind
    AND public.pay_payment_return_notice_groups.pay_batch_id IS NOT DISTINCT FROM p_pay_batch_id
    AND public.pay_payment_return_notice_groups.provider_key IS NOT DISTINCT FROM v_provider_key
    AND public.pay_payment_return_notice_groups.execution_commit_ref IS NOT DISTINCT FROM v_execution_commit_ref
  ORDER BY public.pay_payment_return_notice_groups.created_at_utc ASC, public.pay_payment_return_notice_groups.id ASC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_group.id IS NOT NULL THEN
    v_merge_count := COALESCE((v_existing_group.summary_json#>>'{notice_queue_meta,merge_count}')::integer, 1) + 1;
    v_quiet_until_utc := LEAST(
      v_existing_group.max_send_at_utc,
      GREATEST(v_existing_group.quiet_until_utc, v_now + make_interval(mins => v_quiet_minutes))
    );

    UPDATE public.pay_payment_return_notice_groups AS existing_notice_group
    SET
      quiet_until_utc = v_quiet_until_utc,
      summary_json = COALESCE(existing_notice_group.summary_json, '{}'::jsonb)
        || v_summary_json
        || jsonb_build_object(
          'notice_queue_meta', jsonb_build_object(
            'merge_count', v_merge_count,
            'last_queued_at_utc', v_now,
            'recipient_role', v_recipient_role,
            'quiet_minutes', v_quiet_minutes,
            'max_wait_minutes', v_max_wait_minutes
          ),
          'latest_summary_json', v_summary_json
        ),
      updated_at_utc = v_now
    WHERE existing_notice_group.id = v_existing_group.id
    RETURNING existing_notice_group.id
    INTO v_notice_group_id;

    v_return_json := jsonb_build_object(
      'ok', true,
      'created', false,
      'notice_group_id', v_notice_group_id,
      'notice_kind', v_notice_kind,
      'pay_batch_id', p_pay_batch_id,
      'provider_key', v_provider_key,
      'execution_commit_ref', v_execution_commit_ref,
      'recipient_role', v_recipient_role,
      'quiet_until_utc', v_quiet_until_utc,
      'max_send_at_utc', v_existing_group.max_send_at_utc,
      'merge_count', v_merge_count
    );
  ELSE
    v_quiet_until_utc := v_now + make_interval(mins => v_quiet_minutes);
    v_max_send_at_utc := v_now + make_interval(mins => v_max_wait_minutes);

    INSERT INTO public.pay_payment_return_notice_groups(
      pay_batch_id,
      execution_commit_ref,
      provider_key,
      event_source,
      notice_kind,
      status,
      quiet_until_utc,
      max_send_at_utc,
      summary_json,
      mail_outbox_ids,
      created_at_utc,
      updated_at_utc,
      sent_at_utc
    )
    VALUES (
      p_pay_batch_id,
      v_execution_commit_ref,
      v_provider_key,
      'SYSTEM',
      v_notice_kind,
      'OPEN',
      v_quiet_until_utc,
      v_max_send_at_utc,
      v_summary_json || jsonb_build_object(
        'notice_queue_meta', jsonb_build_object(
          'merge_count', 1,
          'created_at_utc', v_now,
          'recipient_role', v_recipient_role,
          'quiet_minutes', v_quiet_minutes,
          'max_wait_minutes', v_max_wait_minutes
        )
      ),
      '[]'::jsonb,
      v_now,
      v_now,
      NULL::timestamptz
    )
    RETURNING public.pay_payment_return_notice_groups.id
    INTO v_notice_group_id;

    v_return_json := jsonb_build_object(
      'ok', true,
      'created', true,
      'notice_group_id', v_notice_group_id,
      'notice_kind', v_notice_kind,
      'pay_batch_id', p_pay_batch_id,
      'provider_key', v_provider_key,
      'execution_commit_ref', v_execution_commit_ref,
      'recipient_role', v_recipient_role,
      'quiet_until_utc', v_quiet_until_utc,
      'max_send_at_utc', v_max_send_at_utc,
      'merge_count', 1
    );
  END IF;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_RETURN_ADMIN_NOTICE_QUEUE_RESULT',
    v_return_json,
    'pay_payment_correction',
    COALESCE(v_notice_group_id::text, COALESCE(p_pay_batch_id::text, 'NO_NOTICE_GROUP_ID')),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_return_json;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_RETURN_ADMIN_NOTICE_QUEUE_ERROR',
      jsonb_build_object(
        'notice_kind', p_notice_kind,
        'pay_batch_id', p_pay_batch_id,
        'provider_key', p_provider_key,
        'execution_commit_ref', p_execution_commit_ref,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
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


CREATE OR REPLACE FUNCTION public.pay_payment_return_admin_notice_dispatch_due(
  p_limit integer DEFAULT 50
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 100);
  v_now timestamptz := now();
  v_recipient_role text := 'ADMIN';
  v_claimed_count integer := 0;
  v_sent_count integer := 0;
  v_failed_count integer := 0;
  v_no_recipient_count integer := 0;
  v_mail_outbox_id uuid;
  v_to_emails text;
  v_subject text;
  v_body_text text;
  v_body_html text;
  v_group_row record;
  v_notice_summary text;
  v_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_RETURN_ADMIN_NOTICE_DISPATCH_DUE_START',
    jsonb_build_object(
      'requested_limit', p_limit,
      'effective_limit', v_limit
    ),
    'pay_payment_correction',
    'admin_notice_dispatch_due',
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  SELECT COALESCE(NULLIF(btrim(public.settings_defaults.payment_return_admin_recipient_role), ''), 'ADMIN')
  INTO v_recipient_role
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_recipient_role := COALESCE(NULLIF(btrim(v_recipient_role), ''), 'ADMIN');

  DROP TABLE IF EXISTS pg_temp._tmp_payment_return_notice_due;
  CREATE TEMP TABLE _tmp_payment_return_notice_due ON COMMIT DROP AS
  WITH due_groups AS (
    SELECT public.pay_payment_return_notice_groups.id
    FROM public.pay_payment_return_notice_groups
    WHERE public.pay_payment_return_notice_groups.status IN ('OPEN', 'READY')
      AND (
        public.pay_payment_return_notice_groups.quiet_until_utc <= v_now
        OR public.pay_payment_return_notice_groups.max_send_at_utc <= v_now
      )
    ORDER BY
      public.pay_payment_return_notice_groups.max_send_at_utc ASC,
      public.pay_payment_return_notice_groups.quiet_until_utc ASC,
      public.pay_payment_return_notice_groups.created_at_utc ASC,
      public.pay_payment_return_notice_groups.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT v_limit
  ),
  claimed_groups AS (
    UPDATE public.pay_payment_return_notice_groups AS notice_group_to_claim
    SET
      status = 'READY',
      updated_at_utc = v_now
    FROM due_groups
    WHERE notice_group_to_claim.id = due_groups.id
    RETURNING notice_group_to_claim.*
  )
  SELECT
    claimed_groups.id,
    claimed_groups.pay_batch_id,
    claimed_groups.execution_commit_ref,
    claimed_groups.provider_key,
    claimed_groups.event_source,
    claimed_groups.notice_kind,
    claimed_groups.status,
    claimed_groups.quiet_until_utc,
    claimed_groups.max_send_at_utc,
    claimed_groups.summary_json,
    claimed_groups.mail_outbox_ids,
    claimed_groups.created_at_utc,
    claimed_groups.updated_at_utc,
    claimed_groups.sent_at_utc
  FROM claimed_groups;

  SELECT count(*)::integer
  INTO v_claimed_count
  FROM pg_temp._tmp_payment_return_notice_due AS due_count;

  IF v_claimed_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'claimed', 0,
      'sent', 0,
      'failed', 0,
      'no_recipient', 0,
      'recipient_role', v_recipient_role
    );
  END IF;

  FOR v_group_row IN
    SELECT due_groups.*
    FROM pg_temp._tmp_payment_return_notice_due AS due_groups
    ORDER BY due_groups.created_at_utc ASC, due_groups.id ASC
  LOOP
    v_mail_outbox_id := NULL::uuid;
    v_to_emails := NULL::text;

    SELECT string_agg(admin_recipients.email, ',' ORDER BY admin_recipients.email)
    INTO v_to_emails
    FROM (
      SELECT DISTINCT public.tms_users.email AS email
      FROM public.tms_users
      WHERE COALESCE(public.tms_users.is_active, false) = true
        AND NULLIF(btrim(COALESCE(public.tms_users.email, '')), '') IS NOT NULL
        AND upper(btrim(COALESCE(public.tms_users.role, ''))) = upper(btrim(v_recipient_role))
    ) AS admin_recipients;

    IF NULLIF(btrim(COALESCE(v_to_emails, '')), '') IS NULL THEN
      UPDATE public.pay_payment_return_notice_groups AS no_recipient_group
      SET
        status = 'FAILED',
        updated_at_utc = v_now,
        summary_json = COALESCE(no_recipient_group.summary_json, '{}'::jsonb) || jsonb_build_object(
          'dispatch_error', jsonb_build_object(
            'code', 'NO_ADMIN_NOTICE_RECIPIENTS_FOR_ROLE',
            'recipient_role', v_recipient_role,
            'failed_at_utc', v_now
          )
        )
      WHERE no_recipient_group.id = v_group_row.id;

      v_failed_count := v_failed_count + 1;
      v_no_recipient_count := v_no_recipient_count + 1;
      CONTINUE;
    END IF;

    v_notice_summary := left(COALESCE(v_group_row.summary_json::text, '{}'), 12000);
    v_subject := 'CloudTMS payment correction notice: ' || COALESCE(v_group_row.notice_kind, 'PAYMENT_RETURN_NOTICE');
    v_body_text := concat_ws(E'\n',
      'CloudTMS payment correction/admin notice',
      '',
      'Notice kind: ' || COALESCE(v_group_row.notice_kind, ''),
      'Pay batch ID: ' || COALESCE(v_group_row.pay_batch_id::text, ''),
      'Provider: ' || COALESCE(v_group_row.provider_key, ''),
      'Execution reference: ' || COALESCE(v_group_row.execution_commit_ref, ''),
      'Created at UTC: ' || COALESCE(v_group_row.created_at_utc::text, ''),
      '',
      'Summary JSON:',
      v_notice_summary
    );
    v_body_html := '<p>CloudTMS payment correction/admin notice</p>'
      || '<p><strong>Notice kind:</strong> ' || COALESCE(v_group_row.notice_kind, '') || '</p>'
      || '<p><strong>Pay batch ID:</strong> ' || COALESCE(v_group_row.pay_batch_id::text, '') || '</p>'
      || '<p><strong>Provider:</strong> ' || COALESCE(v_group_row.provider_key, '') || '</p>'
      || '<p><strong>Execution reference:</strong> ' || COALESCE(v_group_row.execution_commit_ref, '') || '</p>'
      || '<pre>' || replace(replace(replace(v_notice_summary, '&', '&amp;'), '<', '&lt;'), '>', '&gt;') || '</pre>';

    INSERT INTO public.mail_outbox(
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
      reference,
      created_at_utc,
      created_by,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      scheduled_for_utc,
      next_attempt_at_utc
    )
    SELECT
      'BROADCAST',
      v_to_emails,
      NULL::text,
      NULL::text,
      NULL::text,
      'High',
      'payment_return_admin_notice',
      v_subject,
      v_body_html,
      v_body_text,
      NULL::jsonb,
      'QUEUED',
      'payment_return_notice_group:' || v_group_row.id::text,
      v_now,
      NULL::uuid,
      'ROLE',
      NULL::uuid,
      'pay_payment_return_notice_groups',
      v_group_row.id,
      NULL::timestamptz,
      v_now
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.mail_outbox AS existing_notice_mail
      WHERE existing_notice_mail.reference = 'payment_return_notice_group:' || v_group_row.id::text
        AND existing_notice_mail.email_type = 'payment_return_admin_notice'
    )
    RETURNING public.mail_outbox.id
    INTO v_mail_outbox_id;

    IF v_mail_outbox_id IS NULL THEN
      SELECT public.mail_outbox.id
      INTO v_mail_outbox_id
      FROM public.mail_outbox
      WHERE public.mail_outbox.reference = 'payment_return_notice_group:' || v_group_row.id::text
        AND public.mail_outbox.email_type = 'payment_return_admin_notice'
      ORDER BY public.mail_outbox.created_at_utc ASC, public.mail_outbox.id ASC
      LIMIT 1;
    END IF;

    IF v_mail_outbox_id IS NULL THEN
      UPDATE public.pay_payment_return_notice_groups AS failed_insert_group
      SET
        status = 'FAILED',
        updated_at_utc = v_now,
        summary_json = COALESCE(failed_insert_group.summary_json, '{}'::jsonb) || jsonb_build_object(
          'dispatch_error', jsonb_build_object(
            'code', 'ADMIN_NOTICE_MAIL_OUTBOX_INSERT_FAILED',
            'failed_at_utc', v_now
          )
        )
      WHERE failed_insert_group.id = v_group_row.id;

      v_failed_count := v_failed_count + 1;
    ELSE
      UPDATE public.pay_payment_return_notice_groups AS sent_group
      SET
        status = 'SENT',
        sent_at_utc = COALESCE(sent_group.sent_at_utc, v_now),
        updated_at_utc = v_now,
        mail_outbox_ids = COALESCE(sent_group.mail_outbox_ids, '[]'::jsonb) || jsonb_build_array(v_mail_outbox_id::text),
        summary_json = COALESCE(sent_group.summary_json, '{}'::jsonb) || jsonb_build_object(
          'dispatch', jsonb_build_object(
            'mail_outbox_id', v_mail_outbox_id,
            'recipient_role', v_recipient_role,
            'to', v_to_emails,
            'dispatched_at_utc', v_now
          )
        )
      WHERE sent_group.id = v_group_row.id;

      v_sent_count := v_sent_count + 1;
    END IF;
  END LOOP;

  v_result := jsonb_build_object(
    'ok', true,
    'claimed', v_claimed_count,
    'sent', v_sent_count,
    'failed', v_failed_count,
    'no_recipient', v_no_recipient_count,
    'recipient_role', v_recipient_role
  );

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_RETURN_ADMIN_NOTICE_DISPATCH_DUE_RESULT',
    v_result,
    'pay_payment_correction',
    'admin_notice_dispatch_due',
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_RETURN_ADMIN_NOTICE_DISPATCH_DUE_ERROR',
      jsonb_build_object(
        'limit', p_limit,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      'admin_notice_dispatch_due',
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_remittance_maybe_queue_for_trigger(
  p_pay_batch_id uuid,
  p_trigger text,
  p_scope text DEFAULT 'ALL'::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_only_confirmed boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch public.pay_batches%rowtype;
  v_trigger text;
  v_scope text;
  v_timing_setting text := 'ON_EXECUTION';
  v_effective_actor_user_id uuid;
  v_is_loans_batch boolean := false;
  v_suppress_remittances boolean := false;
  v_suppress_reasons jsonb := '[]'::jsonb;
  v_auth_suppress_count integer := 0;
  v_queue_result jsonb := '{}'::jsonb;
  v_has_remittance_confirmed_overload boolean := false;
  v_has_finance_confirmed_overload boolean := false;
  v_has_remittance_legacy_function boolean := false;
  v_has_finance_legacy_function boolean := false;
  v_dispatch_required boolean := false;
  v_job_count integer := 0;
BEGIN
  v_trigger := upper(nullif(btrim(COALESCE(p_trigger, '')), ''));
  v_scope := upper(nullif(btrim(COALESCE(p_scope, 'ALL')), ''));

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'trigger', v_trigger,
      'scope', v_scope,
      'actor_user_id', p_actor_user_id,
      'only_confirmed', COALESCE(p_only_confirmed, false)
    ),
    'pay_remittance',
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

  IF v_trigger NOT IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED') THEN
    RAISE EXCEPTION 'UNSUPPORTED_REMITTANCE_QUEUE_TRIGGER'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_REMITTANCE_QUEUE_TRIGGER',
              'trigger', p_trigger,
              'supported_triggers', jsonb_build_array('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED')
            )::text;
  END IF;

  IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'UNSUPPORTED_REMITTANCE_QUEUE_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_REMITTANCE_QUEUE_SCOPE',
              'scope', p_scope,
              'supported_scopes', jsonb_build_array('ALL', 'PAYE', 'UMBRELLA')
            )::text;
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_batch.created_by_user_id);
  v_is_loans_batch := upper(btrim(COALESCE(v_batch.batch_kind_fixed, ''))) IN ('LOANS', 'LOAN', 'FINANCE_PAYOUTS', 'PAYOUTS');

  SELECT COALESCE(NULLIF(btrim(public.settings_defaults.payment_remittance_send_timing), ''), 'ON_EXECUTION')
  INTO v_timing_setting
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_timing_setting := upper(COALESCE(NULLIF(btrim(v_timing_setting), ''), 'ON_EXECUTION'));

  IF v_timing_setting NOT IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED') THEN
    v_timing_setting := 'ON_EXECUTION';
  END IF;

  IF v_timing_setting <> v_trigger THEN
    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'deferred', true,
      'suppressed', false,
      'dispatch_required', false,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'message', 'Remittance/payout notice queueing deferred because configured timing does not match this trigger.'
    );
  END IF;

  IF lower(btrim(COALESCE(v_batch.execution_intent_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.execution_intent_json.suppress_remittances');
  END IF;

  IF lower(btrim(COALESCE(v_batch.settlement_confirmation_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.settlement_confirmation_json.suppress_remittances');
  END IF;

  IF lower(btrim(COALESCE(v_batch.execution_intent_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.execution_intent_json.suppress_remittances_pending');
  END IF;

  IF lower(btrim(COALESCE(v_batch.settlement_confirmation_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.settlement_confirmation_json.suppress_remittances_pending');
  END IF;

  SELECT count(*)::integer
  INTO v_auth_suppress_count
  FROM public.pay_batch_auth_requests AS active_auth_requests
  WHERE active_auth_requests.pay_batch_id = p_pay_batch_id
    AND active_auth_requests.finalised_at_utc IS NULL
    AND (
      lower(btrim(COALESCE(active_auth_requests.execution_intent_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on')
      OR lower(btrim(COALESCE(active_auth_requests.execution_intent_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on')
    );

  IF v_auth_suppress_count > 0 THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('active_auth_request.execution_intent_json.suppress_remittances');
  END IF;

  IF v_suppress_remittances THEN
    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'deferred', false,
      'suppressed', true,
      'dispatch_required', false,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'suppress_reasons', v_suppress_reasons,
      'message', 'Remittance/payout notice queueing suppressed by execution or settlement intent.'
    );
  END IF;

  IF v_effective_actor_user_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'queued', false,
      'deferred', false,
      'suppressed', false,
      'dispatch_required', false,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'error', 'ACTOR_USER_ID_REQUIRED_FOR_QUEUE_STAGE',
      'message', 'Queue-stage RPC requires an actor user id and none was supplied or available from the batch creator.'
    );
  END IF;

  v_has_remittance_confirmed_overload := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid,boolean)') IS NOT NULL;
  v_has_finance_confirmed_overload := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid,boolean)') IS NOT NULL;
  v_has_remittance_legacy_function := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid)') IS NOT NULL;
  v_has_finance_legacy_function := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid)') IS NOT NULL;

  IF v_is_loans_batch THEN
    IF v_has_finance_confirmed_overload THEN
      EXECUTE 'SELECT public.pay_finance_payout_notice_queue_commit_stage($1, $2, $3)'
      INTO v_queue_result
      USING p_pay_batch_id, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF COALESCE(p_only_confirmed, false) THEN
      v_queue_result := jsonb_build_object(
        'ok', true,
        'trigger_status', 'CONFIRMED_ONLY_QUEUE_STAGE_OVERLOAD_NOT_INSTALLED',
        'message_kind', 'PAYOUT_NOTICE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'message', 'Confirmed-only finance payout notice queue-stage overload is not installed yet.'
      );
    ELSIF v_has_finance_legacy_function THEN
      EXECUTE 'SELECT public.pay_finance_payout_notice_queue_commit_stage($1, $2)'
      INTO v_queue_result
      USING p_pay_batch_id, v_effective_actor_user_id;
    ELSE
      v_queue_result := jsonb_build_object(
        'ok', false,
        'trigger_status', 'PAYOUT_NOTICE_QUEUE_STAGE_FUNCTION_MISSING',
        'message_kind', 'PAYOUT_NOTICE',
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'error', 'pay_finance_payout_notice_queue_commit_stage function not found'
      );
    END IF;
  ELSE
    IF v_has_remittance_confirmed_overload THEN
      EXECUTE 'SELECT public.pay_remittance_queue_commit_stage($1, $2, $3, $4)'
      INTO v_queue_result
      USING p_pay_batch_id, v_scope, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF COALESCE(p_only_confirmed, false) THEN
      v_queue_result := jsonb_build_object(
        'ok', true,
        'trigger_status', 'CONFIRMED_ONLY_QUEUE_STAGE_OVERLOAD_NOT_INSTALLED',
        'message_kind', 'REMITTANCE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'scope', v_scope,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'message', 'Confirmed-only remittance queue-stage overload is not installed yet.'
      );
    ELSIF v_has_remittance_legacy_function THEN
      EXECUTE 'SELECT public.pay_remittance_queue_commit_stage($1, $2, $3)'
      INTO v_queue_result
      USING p_pay_batch_id, v_scope, v_effective_actor_user_id;
    ELSE
      v_queue_result := jsonb_build_object(
        'ok', false,
        'trigger_status', 'REMITTANCE_QUEUE_STAGE_FUNCTION_MISSING',
        'message_kind', 'REMITTANCE',
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'scope', v_scope,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'error', 'pay_remittance_queue_commit_stage function not found'
      );
    END IF;
  END IF;

  v_dispatch_required := COALESCE((v_queue_result->>'dispatch_required')::boolean, false);
  v_job_count := COALESCE((v_queue_result->>'job_count')::integer, 0);

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_RESULT',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'is_loans_batch', v_is_loans_batch,
      'dispatch_required', v_dispatch_required,
      'job_count', v_job_count,
      'queue_result_status', v_queue_result->>'trigger_status'
    ),
    'pay_remittance',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', COALESCE((v_queue_result->>'ok')::boolean, true),
    'queued', v_dispatch_required,
    'deferred', false,
    'suppressed', false,
    'dispatch_required', v_dispatch_required,
    'trigger', v_trigger,
    'configured_timing', v_timing_setting,
    'pay_batch_id', p_pay_batch_id,
    'scope', v_scope,
    'only_confirmed', COALESCE(p_only_confirmed, false),
    'is_loans_batch', v_is_loans_batch,
    'effective_actor_user_id', v_effective_actor_user_id,
    'queue_result', COALESCE(v_queue_result, '{}'::jsonb)
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'trigger', p_trigger,
        'scope', p_scope,
        'only_confirmed', COALESCE(p_only_confirmed, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_remittance',
      COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;



CREATE OR REPLACE FUNCTION public._pay_payment_correction_apply_accepted_finance_resolution(
  p_correction_request_id uuid,
  p_work_item_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_batch public.pay_batches%rowtype;
  v_now timestamptz := now();
  v_effective_actor_user_id uuid := NULL::uuid;
  v_actor_kind text := 'SYSTEM';

  v_resolution_root jsonb := NULL::jsonb;
  v_resolution_body jsonb := NULL::jsonb;
  v_resolution_cases jsonb := '[]'::jsonb;

  v_sensitive_case_count integer := 0;
  v_case_record record;
  v_component_record record;
  v_accepted_case_json jsonb := NULL::jsonb;
  v_accepted_component_ids_json jsonb := '[]'::jsonb;
  v_accepted_fingerprints_json jsonb := '{}'::jsonb;
  v_accepted_surface text := NULL::text;
  v_accepted_effective_pay_date_text text := NULL::text;
  v_accepted_effective_pay_date date := NULL::date;
  v_resolution_path text := NULL::text;
  v_schedule_input_mode text := NULL::text;
  v_weeks_total integer := NULL::integer;
  v_weekly_due numeric := NULL::numeric;
  v_manual_total_remaining numeric := NULL::numeric;
  v_note text := NULL::text;
  v_component_resolutions jsonb := '[]'::jsonb;

  v_case_row public.pay_advances%rowtype;
  v_expected_fingerprint text := NULL::text;
  v_current_fingerprint text := NULL::text;
  v_regenerated_suggestion jsonb := NULL::jsonb;
  v_regenerated_suggestion_hash text := NULL::text;
  v_accepted_suggestion_hash text := NULL::text;
  v_plan_case_json jsonb := NULL::jsonb;
  v_plan_suggestion_hash text := NULL::text;
  v_plan_effective_pay_date_text text := NULL::text;
  v_apply_result jsonb := NULL::jsonb;
  v_apply_results jsonb := '[]'::jsonb;
  v_blocker jsonb := NULL::jsonb;
  v_open_overlap_count integer := 0;
  v_selected_component_count integer := 0;
  v_missing_component_count integer := 0;
  v_missing_fingerprint_count integer := 0;
  v_fingerprint_mismatch_count integer := 0;
  v_stale_component_count integer := 0;
  v_closed_unrecoverable_component_count integer := 0;
  v_total_selected_item_count integer := 0;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  IF v_work_item.correction_request_id IS DISTINCT FROM v_request.id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_REQUEST_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_REQUEST_MISMATCH',
              'correction_request_id', v_request.id,
              'work_item_id', v_work_item.id,
              'work_item_correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  IF v_work_item.pay_batch_id IS DISTINCT FROM v_request.pay_batch_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_BATCH_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_BATCH_MISMATCH',
              'correction_request_id', v_request.id,
              'work_item_id', v_work_item.id,
              'request_pay_batch_id', v_request.pay_batch_id,
              'work_item_pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_request.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', v_request.pay_batch_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_actions.actor_user_id
  INTO v_effective_actor_user_id
  FROM public.pay_payment_correction_actions
  WHERE public.pay_payment_correction_actions.correction_request_id = v_request.id
    AND public.pay_payment_correction_actions.action IN ('AUTHORISE', 'USE_GOLDEN_KEY')
    AND public.pay_payment_correction_actions.actor_user_id IS NOT NULL
  ORDER BY public.pay_payment_correction_actions.action_at_utc DESC, public.pay_payment_correction_actions.id DESC
  LIMIT 1;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_effective_actor_user_id, v_request.requested_by_user_id);
  v_actor_kind := CASE WHEN v_effective_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END;

  DROP TABLE IF EXISTS pg_temp._tmp_pcafr_selected_items;
  CREATE TEMP TABLE _tmp_pcafr_selected_items ON COMMIT DROP AS
  SELECT selected_items.*
  FROM public._pay_payment_correction_selected_items(
    v_request.pay_batch_id,
    v_work_item.selection_json,
    true
  ) AS selected_items;

  SELECT count(*)::integer
  INTO v_total_selected_item_count
  FROM pg_temp._tmp_pcafr_selected_items AS selected_count;

  DROP TABLE IF EXISTS pg_temp._tmp_pcafr_sensitive_cases;
  CREATE TEMP TABLE _tmp_pcafr_sensitive_cases ON COMMIT DROP AS
  SELECT
    selected_items.finance_case_id AS finance_case_id,
    COALESCE(pay_finance_case_components.candidate_id, selected_items.candidate_id) AS candidate_id,
    COALESCE(
      jsonb_agg(DISTINCT selected_items.finance_component_id ORDER BY selected_items.finance_component_id)
        FILTER (WHERE selected_items.finance_component_id IS NOT NULL),
      '[]'::jsonb
    ) AS selected_component_ids
  FROM pg_temp._tmp_pcafr_selected_items AS selected_items
  JOIN public.pay_batch_items AS pay_batch_items
    ON pay_batch_items.id = selected_items.pay_batch_item_id
  LEFT JOIN public.pay_finance_case_components AS pay_finance_case_components
    ON pay_finance_case_components.id = selected_items.finance_component_id
  LEFT JOIN public.pay_advances AS pay_advances
    ON pay_advances.id = selected_items.finance_case_id
  WHERE selected_items.finance_case_id IS NOT NULL
    AND selected_items.finance_component_id IS NOT NULL
    AND (
      COALESCE(pay_finance_case_components.classification::text, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR COALESCE(pay_batch_items.frozen_component_classification::text, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR (
        COALESCE(pay_advances.taxability::text, '') = 'TAXABLE'
        AND (
          pay_batch_items.frozen_resolution_mode IS NOT NULL
          OR pay_finance_case_components.saved_resolution_mode IS NOT NULL
        )
      )
    )
  GROUP BY selected_items.finance_case_id, COALESCE(pay_finance_case_components.candidate_id, selected_items.candidate_id);

  SELECT count(*)::integer
  INTO v_sensitive_case_count
  FROM pg_temp._tmp_pcafr_sensitive_cases AS sensitive_case_count;

  IF v_sensitive_case_count = 0 THEN
    v_result := jsonb_build_object(
      'ok', true,
      'applied', false,
      'reason', 'NO_CHANNEL_SENSITIVE_FINANCE',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text,
      'selected_item_count', v_total_selected_item_count,
      'processed_at_utc', v_now,
      'processing_actor_kind', v_actor_kind,
      'actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END
    );

    UPDATE public.pay_payment_correction_work_items AS no_sensitive_work_item
    SET result_json = COALESCE(no_sensitive_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'accepted_finance_resolution', v_result
        )
    WHERE no_sensitive_work_item.id = v_work_item.id;

    RETURN v_result;
  END IF;

  IF v_request.accepted_resolution_json IS NULL
     OR COALESCE(jsonb_typeof(v_request.accepted_resolution_json), 'null') <> 'object' THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_REQUIRED',
      'message', 'Selected gross/taxable/channel-sensitive finance items require accepted_resolution_json before correction apply.',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text,
      'sensitive_finance_case_count', v_sensitive_case_count
    );

    UPDATE public.pay_payment_correction_work_items AS accepted_missing_work_item
    SET status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(accepted_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
          'processed_at_utc', v_now
        )
    WHERE accepted_missing_work_item.id = v_work_item.id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_resolution_root := v_request.accepted_resolution_json;

  v_resolution_body := CASE
    WHEN v_resolution_root ? 'suggested_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'suggested_resolution'), 'null') = 'object'
      THEN v_resolution_root->'suggested_resolution'
    WHEN v_resolution_root ? 'accepted_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'accepted_resolution'), 'null') = 'object'
      THEN v_resolution_root->'accepted_resolution'
    ELSE v_resolution_root
  END;

  v_resolution_cases := COALESCE(v_resolution_body->'finance_cases', v_resolution_root->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_resolution_cases), 'null') <> 'array' THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_FINANCE_CASES_INVALID',
      'message', 'accepted_resolution_json.finance_cases must be an array.',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text
    );

    UPDATE public.pay_payment_correction_work_items AS accepted_cases_invalid_work_item
    SET status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(accepted_cases_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
          'processed_at_utc', v_now
        )
    WHERE accepted_cases_invalid_work_item.id = v_work_item.id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  FOR v_case_record IN
    SELECT sensitive_cases.*
    FROM pg_temp._tmp_pcafr_sensitive_cases AS sensitive_cases
    ORDER BY sensitive_cases.finance_case_id
  LOOP
    SELECT finance_case_rows.*
    INTO v_case_row
    FROM public.pay_advances AS finance_case_rows
    WHERE finance_case_rows.id = v_case_record.finance_case_id
    FOR UPDATE;

    IF NOT FOUND THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_NOT_FOUND',
        'message', 'Selected finance case no longer exists.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS finance_case_missing_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(finance_case_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE finance_case_missing_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT accepted_case.value
    INTO v_accepted_case_json
    FROM jsonb_array_elements(v_resolution_cases) AS accepted_case(value)
    WHERE NULLIF(btrim(COALESCE(accepted_case.value->>'finance_case_id', '')), '') = v_case_record.finance_case_id::text
    LIMIT 1;

    IF v_accepted_case_json IS NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_CASE_MISSING',
        'message', 'accepted_resolution_json does not include the selected gross/channel-sensitive finance case.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_case_missing_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_case_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_case_missing_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '') IS NOT NULL
       AND NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '') <> v_case_record.candidate_id::text THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_CANDIDATE_MISMATCH',
        'message', 'accepted_resolution_json candidate_id does not match the selected finance case candidate.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'accepted_candidate_id', v_accepted_case_json->>'candidate_id',
        'selected_candidate_id', v_case_record.candidate_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_candidate_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_candidate_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_candidate_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT plan_case.value
    INTO v_plan_case_json
    FROM jsonb_array_elements(
      CASE
        WHEN COALESCE(jsonb_typeof(v_request.plan_json#>'{suggested_resolution,finance_cases}'), 'null') = 'array'
          THEN v_request.plan_json#>'{suggested_resolution,finance_cases}'
        ELSE '[]'::jsonb
      END
    ) AS plan_case(value)
    WHERE NULLIF(btrim(COALESCE(plan_case.value->>'finance_case_id', '')), '') = v_case_record.finance_case_id::text
    LIMIT 1;

    v_plan_suggestion_hash := NULLIF(btrim(COALESCE(v_plan_case_json->>'suggestion_hash', '')), '');
    v_plan_effective_pay_date_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'effective_pay_date', '')), '');

    v_accepted_component_ids_json := COALESCE(
      v_accepted_case_json->'component_ids',
      v_accepted_case_json->'selected_component_ids',
      '[]'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_accepted_component_ids_json), 'null') <> 'array' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_IDS_INVALID',
        'message', 'accepted_resolution_json finance case component_ids must be an array.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_components_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_components_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_components_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT count(*)::integer
    INTO v_selected_component_count
    FROM public.pay_finance_case_components AS selected_component_count
    WHERE selected_component_count.finance_case_id = v_case_record.finance_case_id
      AND selected_component_count.id IN (
        SELECT (selected_component_ids.value)::uuid
        FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
        WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      );

    SELECT count(*)::integer
    INTO v_missing_component_count
    FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
    WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
        WHERE accepted_component_ids.value = selected_component_ids.value
      );

    IF v_selected_component_count = 0 OR v_missing_component_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
        'message', 'accepted_resolution_json does not cover all selected gross/channel-sensitive finance components.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'missing_component_count', v_missing_component_count,
        'selected_component_count', v_selected_component_count
      );

      UPDATE public.pay_payment_correction_work_items AS component_scope_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(component_scope_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE component_scope_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_fingerprints_json := COALESCE(
      v_accepted_case_json->'current_component_fingerprints',
      v_accepted_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_accepted_fingerprints_json), 'null') <> 'object' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_FINGERPRINTS_INVALID',
        'message', 'accepted_resolution_json current_component_fingerprints must be an object.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS fingerprints_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(fingerprints_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE fingerprints_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_missing_fingerprint_count := 0;
    v_fingerprint_mismatch_count := 0;
    v_stale_component_count := 0;
    v_closed_unrecoverable_component_count := 0;

    FOR v_component_record IN
      SELECT public.pay_finance_case_components.*
      FROM public.pay_finance_case_components
      WHERE public.pay_finance_case_components.finance_case_id = v_case_record.finance_case_id
        AND public.pay_finance_case_components.id IN (
          SELECT (selected_component_ids.value)::uuid
          FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
          WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
      ORDER BY public.pay_finance_case_components.id
      FOR UPDATE
    LOOP
      v_expected_fingerprint := NULLIF(btrim(COALESCE(v_accepted_fingerprints_json->>v_component_record.id::text, '')), '');
      v_current_fingerprint := COALESCE(
        NULLIF(btrim(v_component_record.resolution_fingerprint), ''),
        md5(jsonb_build_object(
          'finance_component_id', v_component_record.id,
          'finance_case_id', v_component_record.finance_case_id,
          'classification', v_component_record.classification::text,
          'source_pay_method', v_component_record.source_pay_method,
          'source_amount', v_component_record.source_amount,
          'remaining_source_amount', v_component_record.remaining_source_amount,
          'saved_target_pay_method', v_component_record.saved_target_pay_method,
          'saved_resolution_mode', v_component_record.saved_resolution_mode::text,
          'saved_resolution_payload_json', v_component_record.saved_resolution_payload_json,
          'saved_resolution_result_json', v_component_record.saved_resolution_result_json,
          'is_resolution_stale', v_component_record.is_resolution_stale,
          'closed_at_utc', v_component_record.closed_at_utc,
          'updated_at_utc', v_component_record.updated_at_utc
        )::text)
      );

      IF v_expected_fingerprint IS NULL THEN
        v_missing_fingerprint_count := v_missing_fingerprint_count + 1;
      ELSIF v_expected_fingerprint <> v_current_fingerprint THEN
        v_fingerprint_mismatch_count := v_fingerprint_mismatch_count + 1;
      END IF;

      IF COALESCE(v_component_record.is_resolution_stale, false) THEN
        v_stale_component_count := v_stale_component_count + 1;
      END IF;

      IF v_component_record.closed_at_utc IS NOT NULL
         AND round(COALESCE(v_component_record.remaining_source_amount, 0), 2) <= 0 THEN
        v_closed_unrecoverable_component_count := v_closed_unrecoverable_component_count + 1;
      END IF;
    END LOOP;

    IF v_missing_fingerprint_count > 0 OR v_fingerprint_mismatch_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_STALE',
        'message', 'Accepted finance resolution is stale because one or more component fingerprints no longer match.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'missing_fingerprint_count', v_missing_fingerprint_count,
        'fingerprint_mismatch_count', v_fingerprint_mismatch_count
      );

      UPDATE public.pay_payment_correction_work_items AS stale_fingerprint_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(stale_fingerprint_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE stale_fingerprint_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_stale_component_count > 0 OR v_closed_unrecoverable_component_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_MANUAL_REVIEW_REQUIRED',
        'message', 'Selected finance components are stale or closed in a way that requires manual review before correction apply.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'stale_component_count', v_stale_component_count,
        'closed_unrecoverable_component_count', v_closed_unrecoverable_component_count
      );

      UPDATE public.pay_payment_correction_work_items AS manual_review_component_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(manual_review_component_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE manual_review_component_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_case_row.status <> 'ACTIVE'::public.pay_advance_status_enum
       OR v_case_row.written_off_at_utc IS NOT NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_NOT_ACTIVE_OR_WRITTEN_OFF',
        'message', 'Selected finance case is no longer active or has been written off.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'status', v_case_row.status::text,
        'written_off_at_utc', v_case_row.written_off_at_utc
      );

      UPDATE public.pay_payment_correction_work_items AS inactive_case_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(inactive_case_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE inactive_case_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT count(*)::integer
    INTO v_open_overlap_count
    FROM public.pay_batch_items AS overlapping_items
    JOIN public.pay_batch_candidates AS overlapping_candidates
      ON overlapping_candidates.id = overlapping_items.pay_batch_candidate_id
    JOIN public.pay_batches AS overlapping_batches
      ON overlapping_batches.id = overlapping_candidates.pay_batch_id
    WHERE overlapping_candidates.pay_batch_id <> v_request.pay_batch_id
      AND COALESCE(overlapping_items.is_voided, false) = false
      AND overlapping_batches.cancelled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(overlapping_batches.status)
      AND (
        overlapping_items.finance_case_id = v_case_record.finance_case_id
        OR overlapping_items.finance_component_id IN (
          SELECT (selected_component_ids.value)::uuid
          FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
          WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        OR overlapping_items.reservation_id IN (
          SELECT selected_items.reservation_id
          FROM pg_temp._tmp_pcafr_selected_items AS selected_items
          WHERE selected_items.finance_case_id = v_case_record.finance_case_id
            AND selected_items.reservation_id IS NOT NULL
        )
      );

    IF v_open_overlap_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'DRAFT_BATCH_INTERFERENCE',
        'message', 'An open overlapping draft/reserved batch already references the selected finance case/component/reservation. Delete or cancel the overlapping draft first.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'overlap_count', v_open_overlap_count
      );

      UPDATE public.pay_payment_correction_work_items AS overlap_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(overlap_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE overlap_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_surface := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_resolution_body->>'apply_surface'), ''),
      'pay_finance_case_apply_taxable_channel_restructure'
    );

    IF v_accepted_surface NOT IN (
      'pay_finance_case_apply_taxable_channel_restructure',
      'pay_manual_debt_adjustment_resolve_taxable_channel_change',
      'pay_finance_component_resolutions_apply'
    ) THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_UNSUPPORTED',
        'message', 'accepted_resolution_json apply_surface is not supported by the payment correction finance helper.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'apply_surface', v_accepted_surface
      );

      UPDATE public.pay_payment_correction_work_items AS unsupported_surface_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(unsupported_surface_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE unsupported_surface_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_effective_pay_date_text := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'effective_pay_date'), ''),
      v_plan_effective_pay_date_text,
      NULLIF(btrim(v_resolution_body->>'effective_pay_date'), ''),
      COALESCE(v_batch.authoritative_payment_date, v_batch.pay_date)::text
    );

    IF v_accepted_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_INVALID',
        'message', 'accepted_resolution_json effective_pay_date must be YYYY-MM-DD.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'effective_pay_date', v_accepted_effective_pay_date_text
      );

      UPDATE public.pay_payment_correction_work_items AS effective_date_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(effective_date_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE effective_date_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_plan_effective_pay_date_text IS NOT NULL
       AND v_plan_effective_pay_date_text ~ '^\d{4}-\d{2}-\d{2}$'
       AND v_accepted_effective_pay_date_text <> v_plan_effective_pay_date_text THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_MISMATCH',
        'message', 'accepted_resolution_json effective_pay_date does not match the payment correction plan effective_pay_date.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'accepted_effective_pay_date', v_accepted_effective_pay_date_text,
        'planned_effective_pay_date', v_plan_effective_pay_date_text
      );

      UPDATE public.pay_payment_correction_work_items AS effective_date_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(effective_date_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE effective_date_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_effective_pay_date := v_accepted_effective_pay_date_text::date;
    v_resolution_path := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'resolution_path'), ''),
      NULLIF(btrim(v_resolution_body->>'resolution_path'), ''),
      NULLIF(btrim(v_accepted_case_json#>>'{suggestion,resolution_path}'), ''),
      'SUGGESTED'
    );
    v_schedule_input_mode := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_resolution_body->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_accepted_case_json#>>'{suggestion,schedule_input_mode}'), '')
    );

    v_weeks_total := CASE
      WHEN COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}') ~ '^-?\d+$'
        THEN COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}')::integer
      ELSE NULL::integer
    END;

    v_weekly_due := CASE
      WHEN COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}') ~ '^-?\d+(\.\d+)?$'
        THEN COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}')::numeric
      ELSE NULL::numeric
    END;

    v_manual_total_remaining := CASE
      WHEN COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining') ~ '^-?\d+(\.\d+)?$'
        THEN COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining')::numeric
      ELSE NULL::numeric
    END;

    v_note := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'note'), ''),
      NULLIF(btrim(v_resolution_body->>'note'), ''),
      'Applied accepted taxable channel finance resolution for payment correction request ' || v_request.id::text || ', work item ' || v_work_item.id::text
    );

    IF v_effective_actor_user_id IS NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'ACTOR_USER_ID_REQUIRED_FOR_ACCEPTED_FINANCE_RESOLUTION',
        'message', 'Applying accepted gross/channel-sensitive finance resolution requires a user actor.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS actor_required_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(actor_required_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE actor_required_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    BEGIN
      v_regenerated_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
        p_finance_case_id => v_case_record.finance_case_id,
        p_actor_user_id => v_effective_actor_user_id,
        p_effective_pay_date => v_accepted_effective_pay_date,
        p_resolution_path => v_resolution_path,
        p_schedule_input_mode => v_schedule_input_mode,
        p_weeks_total => v_weeks_total,
        p_weekly_due => v_weekly_due,
        p_manual_total_remaining => v_manual_total_remaining,
        p_note => 'Regenerated for accepted payment correction finance resolution ' || v_request.id::text || ', work item ' || v_work_item.id::text
      );

      v_regenerated_suggestion_hash := md5(v_regenerated_suggestion::text);
      v_accepted_suggestion_hash := NULLIF(btrim(COALESCE(v_accepted_case_json->>'suggestion_hash', '')), '');

      IF v_accepted_suggestion_hash IS NULL THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_SUGGESTION_HASH_REQUIRED',
          'message', 'accepted_resolution_json must include suggestion_hash for every selected gross/channel-sensitive finance case.',
          'finance_case_id', v_case_record.finance_case_id::text
        );

        UPDATE public.pay_payment_correction_work_items AS missing_suggestion_hash_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(missing_suggestion_hash_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE missing_suggestion_hash_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;

      IF v_plan_suggestion_hash IS NOT NULL
         AND v_accepted_suggestion_hash <> v_plan_suggestion_hash THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_PLAN_HASH_MISMATCH',
          'message', 'accepted_resolution_json suggestion_hash does not match the stored correction plan suggestion_hash.',
          'finance_case_id', v_case_record.finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'planned_suggestion_hash', v_plan_suggestion_hash
        );

        UPDATE public.pay_payment_correction_work_items AS plan_hash_mismatch_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(plan_hash_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE plan_hash_mismatch_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;

      IF v_accepted_suggestion_hash <> v_regenerated_suggestion_hash THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because the regenerated suggestion no longer matches the accepted suggestion hash.',
          'finance_case_id', v_case_record.finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'regenerated_suggestion_hash', v_regenerated_suggestion_hash
        );

        UPDATE public.pay_payment_correction_work_items AS stale_suggestion_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(stale_suggestion_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE stale_suggestion_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_REGENERATION_FAILED',
        'message', 'Accepted finance resolution could not be regenerated using the existing suggested-resolution function.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      );

      UPDATE public.pay_payment_correction_work_items AS regeneration_failed_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(regeneration_failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE regeneration_failed_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END;

    BEGIN
      IF v_accepted_surface = 'pay_finance_case_apply_taxable_channel_restructure' THEN
        v_apply_result := public.pay_finance_case_apply_taxable_channel_restructure(
          p_finance_case_id => v_case_record.finance_case_id,
          p_actor_user_id => v_effective_actor_user_id,
          p_resolution_path => v_resolution_path,
          p_schedule_input_mode => v_schedule_input_mode,
          p_weeks_total => v_weeks_total,
          p_weekly_due => v_weekly_due,
          p_manual_total_remaining => v_manual_total_remaining,
          p_effective_pay_date => v_accepted_effective_pay_date,
          p_note => v_note
        );
      ELSIF v_accepted_surface = 'pay_manual_debt_adjustment_resolve_taxable_channel_change' THEN
        v_apply_result := public.pay_manual_debt_adjustment_resolve_taxable_channel_change(
          p_finance_case_id => v_case_record.finance_case_id,
          p_actor_user_id => v_effective_actor_user_id,
          p_resolution_path => v_resolution_path,
          p_schedule_input_mode => v_schedule_input_mode,
          p_weeks_total => v_weeks_total,
          p_weekly_due => v_weekly_due,
          p_manual_total_remaining => v_manual_total_remaining,
          p_effective_pay_date => v_accepted_effective_pay_date,
          p_note => v_note
        );
      ELSE
        v_component_resolutions := COALESCE(
          v_accepted_case_json->'component_resolutions',
          v_accepted_case_json#>'{suggestion,component_resolutions}',
          v_accepted_case_json#>'{suggestion,resolutions}',
          v_accepted_case_json#>'{suggestion,components}',
          '[]'::jsonb
        );

        IF COALESCE(jsonb_typeof(v_component_resolutions), 'null') <> 'array'
           OR jsonb_array_length(v_component_resolutions) = 0 THEN
          RAISE EXCEPTION 'COMPONENT_RESOLUTIONS_REQUIRED'
            USING DETAIL = jsonb_build_object(
              'code', 'COMPONENT_RESOLUTIONS_REQUIRED',
              'finance_case_id', v_case_record.finance_case_id::text,
              'apply_surface', v_accepted_surface
            )::text;
        END IF;

        v_apply_result := public.pay_finance_component_resolutions_apply(
          p_candidate_id => v_case_record.candidate_id,
          p_component_resolutions => v_component_resolutions,
          p_actor_user_id => v_effective_actor_user_id,
          p_finance_case_id => v_case_record.finance_case_id,
          p_reason => v_note
        );
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_FINANCE_RESOLUTION_APPLY_FAILED',
        'message', 'Accepted finance resolution failed to apply. The helper marked this work item blocked; caller must not mark the correction work item applied.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'apply_surface', v_accepted_surface,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      );

      UPDATE public.pay_payment_correction_work_items AS apply_failed_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(apply_failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE apply_failed_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END;

    v_apply_results := v_apply_results || jsonb_build_array(jsonb_build_object(
      'finance_case_id', v_case_record.finance_case_id::text,
      'candidate_id', v_case_record.candidate_id::text,
      'selected_component_ids', v_case_record.selected_component_ids,
      'apply_surface', v_accepted_surface,
      'effective_pay_date', v_accepted_effective_pay_date::text,
      'regenerated_suggestion_hash', v_regenerated_suggestion_hash,
      'apply_result', v_apply_result
    ));
  END LOOP;

  v_result := jsonb_build_object(
    'ok', true,
    'applied', true,
    'correction_request_id', v_request.id::text,
    'work_item_id', v_work_item.id::text,
    'sensitive_finance_case_count', v_sensitive_case_count,
    'selected_item_count', v_total_selected_item_count,
    'apply_results', v_apply_results,
    'processed_at_utc', v_now,
    'processing_actor_kind', v_actor_kind,
    'actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END
  );

  UPDATE public.pay_payment_correction_work_items AS successful_work_item
  SET result_json = COALESCE(successful_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
        'accepted_finance_resolution', v_result
      )
  WHERE successful_work_item.id = v_work_item.id;

  RETURN v_result;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item(
  p_work_item_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_request public.pay_payment_correction_requests%rowtype;
  v_batch public.pay_batches%rowtype;
  v_now timestamptz := now();
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := NULL;
  v_blocker jsonb := NULL::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_reservation_count integer := 0;
  v_selected_finance_component_count integer := 0;
  v_voided_item_count integer := 0;
  v_inserted_correction_item_count integer := 0;
  v_released_reservation_count integer := 0;
  v_restored_component_count integer := 0;
  v_reset_payout_count integer := 0;
  v_cancelled_mail_count integer := 0;
  v_updated_transfer_count integer := 0;
  v_updated_candidate_count integer := 0;
  v_dirty_candidate_count integer := 0;
  v_notice_group_id uuid := NULL::uuid;
  v_quiet_minutes integer := 10;
  v_max_wait_minutes integer := 60;
  v_has_settlement_evidence boolean := false;
  v_has_aggregate_subset_blocker boolean := false;
  v_candidate_id uuid;
  v_refresh_result jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_is_whole_batch_work_item boolean := false;
  v_total_active_batch_item_count integer := 0;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_id_count integer := 0;
  v_expected_item_mismatch_count integer := 0;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_finance_resolution_result jsonb := NULL::jsonb;
  v_notice_queue_result jsonb := NULL::jsonb;
  v_mail_selected_scope_json jsonb := '{}'::jsonb;
  v_communications_review_required_count integer := 0;
  v_mail_scope_matching jsonb := '{}'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_START',
    jsonb_build_object(
      'work_item_id', p_work_item_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  IF v_work_item.work_kind <> 'NO_MONEY_UNWIND' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_ITEM_KIND_NOT_NO_MONEY_UNWIND',
      'message', 'This work item is not a no-money unwind work item.',
      'work_kind', v_work_item.work_kind
    );

    UPDATE public.pay_payment_correction_work_items AS blocked_work_kind
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(blocked_work_kind.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE blocked_work_kind.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = v_work_item.correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_request.requested_by_user_id);

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_work_item.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_selected;
  CREATE TEMP TABLE _tmp_no_money_unwind_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_group_key,
    selected_rows.umbrella_id,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.key_resolution_failure_reason
  FROM public._pay_payment_correction_selected_items(
    v_work_item.pay_batch_id,
    v_work_item.selection_json,
    false
  ) AS selected_rows;

  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (umbrella_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (transfer_group_key);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (finance_component_id);

  SELECT
    count(*)::integer,
    count(DISTINCT no_money_selected.candidate_id) FILTER (WHERE no_money_selected.candidate_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.pay_bank_transfer_id) FILTER (WHERE no_money_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.reservation_id) FILTER (WHERE no_money_selected.reservation_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.finance_component_id) FILTER (WHERE no_money_selected.finance_component_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_reservation_count,
    v_selected_finance_component_count
  FROM pg_temp._tmp_no_money_unwind_selected AS no_money_selected;

  IF v_selected_item_count = 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_NO_MONEY_UNWIND',
      'message', 'No selectable pay_batch_items were resolved for the no-money unwind work item.'
    );

    UPDATE public.pay_payment_correction_work_items AS no_items_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_items_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_items_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
v_scope_type := upper(btrim(COALESCE(v_work_item.selection_json->>'scope_type', '')));
v_work_unit := upper(btrim(COALESCE(v_work_item.selection_json->>'work_unit', '')));

SELECT count(*)::integer
INTO v_total_active_batch_item_count
FROM public.pay_batch_items AS total_batch_items
JOIN public.pay_batch_candidates AS total_batch_candidates
  ON total_batch_candidates.id = total_batch_items.pay_batch_candidate_id
WHERE total_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
  AND COALESCE(total_batch_items.is_voided, false) = false;

v_is_whole_batch_work_item := (
  v_scope_type = 'BATCH'
  AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
  AND v_selected_item_count = COALESCE(v_total_active_batch_item_count, 0)
);

IF v_work_item.selection_json ? 'expected_item_count' THEN
  IF COALESCE(v_work_item.selection_json->>'expected_item_count', '') !~ '^[0-9]+$' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item expected_item_count is not a valid non-negative integer.',
      'expected_item_count_raw', v_work_item.selection_json->>'expected_item_count'
    );

    UPDATE public.pay_payment_correction_work_items AS no_money_invalid_expected_count_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_invalid_expected_count_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_invalid_expected_count_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_expected_item_count := (v_work_item.selection_json->>'expected_item_count')::integer;

  IF v_expected_item_count <> v_selected_item_count THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_item count.',
      'expected_item_count', v_expected_item_count,
      'resolved_item_count', v_selected_item_count
    );

    UPDATE public.pay_payment_correction_work_items AS no_money_expected_count_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_expected_count_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_expected_count_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;

IF v_work_item.selection_json ? 'expected_pay_batch_item_ids'
   AND COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids must be a JSON array.'
  );

  UPDATE public.pay_payment_correction_work_items AS no_money_expected_ids_type_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(no_money_expected_ids_type_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE no_money_expected_ids_type_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_expected_items;
CREATE TEMP TABLE _tmp_no_money_unwind_expected_items ON COMMIT DROP AS
WITH raw_expected_item_ids AS (
  SELECT jsonb_array_elements_text(
    CASE
      WHEN COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
        THEN v_work_item.selection_json->'expected_pay_batch_item_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_pay_batch_item_id
)
SELECT DISTINCT
  raw_expected_item_ids.raw_pay_batch_item_id,
  CASE
    WHEN raw_expected_item_ids.raw_pay_batch_item_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN raw_expected_item_ids.raw_pay_batch_item_id::uuid
    ELSE NULL::uuid
  END AS pay_batch_item_id
FROM raw_expected_item_ids;

SELECT count(*)::integer
INTO v_expected_item_mismatch_count
FROM pg_temp._tmp_no_money_unwind_expected_items AS invalid_expected_items
WHERE invalid_expected_items.pay_batch_item_id IS NULL;

IF v_expected_item_mismatch_count > 0 THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids contains invalid UUID values.',
    'invalid_expected_item_count', v_expected_item_mismatch_count
  );

  UPDATE public.pay_payment_correction_work_items AS no_money_invalid_expected_ids_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(no_money_invalid_expected_ids_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE no_money_invalid_expected_ids_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

SELECT count(*)::integer
INTO v_expected_item_id_count
FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_item_count
WHERE expected_item_count.pay_batch_item_id IS NOT NULL;

IF v_expected_item_id_count > 0 THEN
  SELECT count(*)::integer
  INTO v_expected_item_mismatch_count
  FROM (
    (
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
      EXCEPT
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_items
    )
    UNION ALL
    (
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_items
      EXCEPT
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
    )
  ) AS expected_item_drift;

  IF v_expected_item_mismatch_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_items.',
      'expected_item_count', v_expected_item_id_count,
      'resolved_item_count', v_selected_item_count,
      'mismatch_count', v_expected_item_mismatch_count
    );

    UPDATE public.pay_payment_correction_work_items AS no_money_expected_ids_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_expected_ids_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_expected_ids_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;


  PERFORM 1
  FROM public.pay_batch_items AS locked_batch_items
  JOIN pg_temp._tmp_no_money_unwind_selected AS lock_selected_items
    ON lock_selected_items.pay_batch_item_id = locked_batch_items.id
  FOR UPDATE OF locked_batch_items;

  PERFORM 1
  FROM public.pay_batch_candidates AS locked_batch_candidates
  WHERE locked_batch_candidates.id IN (
    SELECT DISTINCT lock_selected_candidates.pay_batch_candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_candidates
    WHERE lock_selected_candidates.pay_batch_candidate_id IS NOT NULL
  )
  FOR UPDATE OF locked_batch_candidates;

  PERFORM 1
  FROM public.pay_bank_transfers AS locked_bank_transfers
  WHERE locked_bank_transfers.id IN (
    SELECT DISTINCT lock_selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_transfers
    WHERE lock_selected_transfers.pay_bank_transfer_id IS NOT NULL
  )
  FOR UPDATE OF locked_bank_transfers;

  PERFORM 1
  FROM public.pay_advance_reservations AS locked_advance_reservations
  WHERE locked_advance_reservations.id IN (
    SELECT DISTINCT lock_selected_reservations.reservation_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_reservations
    WHERE lock_selected_reservations.reservation_id IS NOT NULL
  )
  FOR UPDATE OF locked_advance_reservations;

  PERFORM 1
  FROM public.pay_finance_case_components AS locked_finance_case_components
  WHERE locked_finance_case_components.id IN (
    SELECT DISTINCT lock_selected_components.finance_component_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_components
    WHERE lock_selected_components.finance_component_id IS NOT NULL
  )
  FOR UPDATE OF locked_finance_case_components;

  PERFORM 1
  FROM public.pay_advances AS locked_pay_advances
  WHERE locked_pay_advances.id IN (
    SELECT DISTINCT lock_selected_cases.finance_case_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_cases
    WHERE lock_selected_cases.finance_case_id IS NOT NULL
  )
  FOR UPDATE OF locked_pay_advances;

  v_classification_result := public._pay_payment_movement_classify(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb) || jsonb_build_object(
      'requested_action', 'UNWIND_FAILED_PAYMENT',
      'source_context', 'WORK_ITEM_APPLY'
    )
  );

  v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');

  IF v_classification <> 'NO_MONEY_UNWIND' THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_CLASSIFICATION_REQUIRED',
      'message', 'Selected scope is no longer classified as a no-money unwind.',
      'classification', v_classification,
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS classification_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(classification_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE classification_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  IF COALESCE((v_classification_result#>>'{evidence,settlement,has_settlement_evidence}')::boolean, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_HAS_SETTLEMENT_EVIDENCE',
      'message', 'No-money unwind cannot apply because selected scope now has settlement evidence.'
    );

    UPDATE public.pay_payment_correction_work_items AS settlement_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settlement_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE settlement_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(v_classification_result->'blockers', '[]'::jsonb)) AS blocker_elements(blocker_value)
    WHERE COALESCE(blocker_elements.blocker_value->>'code', '') = 'AGGREGATE_UMBRELLA_TRANSFER_SUBSET_SELECTED'
  )
  INTO v_has_aggregate_subset_blocker;

  IF COALESCE(v_has_aggregate_subset_blocker, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_AGGREGATE_TRANSFER_SUBSET_BLOCKED',
      'message', 'Provider failure maps to an aggregate transfer, but the selected scope is only a subset. Apply to the whole transfer or use manual evidence review.',
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS aggregate_subset_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(aggregate_subset_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE aggregate_subset_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;


  INSERT INTO public.pay_payment_correction_items(
    correction_request_id,
    pay_batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    pay_bank_transfer_id,
    timesheet_id,
    finance_case_id,
    finance_component_id,
    reservation_id,
    item_type,
    correction_item_kind,
    source_amount,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    economic_key_type,
    economic_key_value,
    before_snapshot_json,
    after_snapshot_json,
    status,
    created_at_utc,
    applied_at_utc
  )
  SELECT
    v_work_item.correction_request_id,
    selected_for_ledger.pay_batch_id,
    selected_for_ledger.pay_batch_candidate_id,
    selected_for_ledger.candidate_id,
    selected_for_ledger.pay_batch_item_id,
    selected_for_ledger.pay_bank_transfer_id,
    selected_for_ledger.timesheet_id,
    selected_for_ledger.finance_case_id,
    selected_for_ledger.finance_component_id,
    selected_for_ledger.reservation_id,
    selected_for_ledger.item_type,
    'NO_MONEY_UNWIND',
    selected_for_ledger.source_amount_ex_vat,
    selected_for_ledger.amount_ex_vat,
    selected_for_ledger.amount_vat,
    selected_for_ledger.amount_inc_vat,
    selected_for_ledger.economic_key_type,
    selected_for_ledger.economic_key_value,
    to_jsonb(ledger_items),
    to_jsonb(ledger_items) || jsonb_build_object('is_voided', true, 'updated_at', v_now),
    'APPLIED',
    v_now,
    v_now
  FROM pg_temp._tmp_no_money_unwind_selected AS selected_for_ledger
  JOIN public.pay_batch_items AS ledger_items
    ON ledger_items.id = selected_for_ledger.pay_batch_item_id
  ON CONFLICT (pay_batch_item_id, correction_item_kind) WHERE status = 'APPLIED' AND pay_batch_item_id IS NOT NULL DO NOTHING;

  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;

  UPDATE public.pay_batch_items AS items_to_void
  SET
    is_voided = true,
    updated_at = v_now
  WHERE items_to_void.id IN (
    SELECT selected_to_void.pay_batch_item_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_to_void
  )
    AND COALESCE(items_to_void.is_voided, false) = false;

  GET DIAGNOSTICS v_voided_item_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_released_reservations;
  CREATE TEMP TABLE _tmp_no_money_unwind_released_reservations ON COMMIT DROP AS
  WITH release_candidates AS (
    SELECT DISTINCT
      public.pay_advance_reservations.id,
      public.pay_advance_reservations.finance_case_id,
      public.pay_advance_reservations.finance_component_id,
      public.pay_advance_reservations.pay_batch_item_id,
      public.pay_advance_reservations.reserved_amount,
      public.pay_advance_reservations.reserved_source_amount
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_no_money_unwind_selected AS selected_reservation_items
      ON selected_reservation_items.reservation_id = public.pay_advance_reservations.id
      OR selected_reservation_items.pay_batch_item_id = public.pay_advance_reservations.pay_batch_item_id
    WHERE public.pay_advance_reservations.pay_batch_id = v_work_item.pay_batch_id
      AND public.pay_advance_reservations.settled_at_utc IS NULL
      AND upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) <> 'SETTLED'
      AND upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) <> 'RELEASED'
  ),
  released_rows AS (
    UPDATE public.pay_advance_reservations AS reservations_to_release
    SET
      status = 'RELEASED',
      released_at_utc = COALESCE(reservations_to_release.released_at_utc, v_now),
      released_reason = 'NO_MONEY_UNWIND',
      updated_by_user_id = p_actor_user_id
    FROM release_candidates
    WHERE reservations_to_release.id = release_candidates.id
    RETURNING
      reservations_to_release.id,
      reservations_to_release.finance_case_id,
      reservations_to_release.finance_component_id,
      reservations_to_release.pay_batch_item_id,
      reservations_to_release.reserved_amount,
      reservations_to_release.reserved_source_amount
  )
  SELECT
    released_rows.id AS reservation_id,
    released_rows.finance_case_id,
    released_rows.finance_component_id,
    released_rows.pay_batch_item_id,
    released_rows.reserved_amount,
    released_rows.reserved_source_amount
  FROM released_rows;

  SELECT count(*)::integer
  INTO v_released_reservation_count
  FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservation_count;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_component_restore;
  CREATE TEMP TABLE _tmp_no_money_unwind_component_restore ON COMMIT DROP AS
  SELECT
    restore_source.finance_component_id,
    restore_source.finance_case_id,
    round(sum(restore_source.restore_source_amount), 2)::numeric AS restore_source_amount
  FROM (
    SELECT
      COALESCE(released_reservations.finance_component_id, public.pay_batch_items.finance_component_id) AS finance_component_id,
      COALESCE(released_reservations.finance_case_id, public.pay_batch_items.finance_case_id) AS finance_case_id,
      round(abs(COALESCE(
        released_reservations.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(public.pay_batch_items.id),
        public.pay_batch_items.frozen_source_amount,
        released_reservations.reserved_amount,
        public.pay_batch_items.amount_ex_vat,
        public.pay_batch_items.amount_inc_vat,
        0
      )), 2)::numeric AS restore_source_amount
    FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservations
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.id = released_reservations.pay_batch_item_id
  ) AS restore_source
  WHERE restore_source.finance_component_id IS NOT NULL
    AND restore_source.restore_source_amount > 0
  GROUP BY restore_source.finance_component_id, restore_source.finance_case_id;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_component_restore_apply;
  CREATE TEMP TABLE _tmp_no_money_unwind_component_restore_apply ON COMMIT DROP AS
  SELECT
    public.pay_finance_case_components.id AS finance_component_id,
    public.pay_finance_case_components.finance_case_id,
    public.pay_finance_case_components.remaining_source_amount AS remaining_before,
    LEAST(
      COALESCE(public.pay_finance_case_components.source_amount, 0),
      COALESCE(public.pay_finance_case_components.remaining_source_amount, 0) + COALESCE(component_restore.restore_source_amount, 0)
    ) AS remaining_after,
    component_restore.restore_source_amount
  FROM pg_temp._tmp_no_money_unwind_component_restore AS component_restore
  JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = component_restore.finance_component_id;

  UPDATE public.pay_finance_case_components AS components_to_restore
  SET
    remaining_source_amount = component_restore_apply.remaining_after,
    resolved_at_utc = CASE WHEN component_restore_apply.remaining_after > 0 THEN NULL ELSE components_to_restore.resolved_at_utc END,
    closed_at_utc = NULL,
    updated_at_utc = v_now
  FROM pg_temp._tmp_no_money_unwind_component_restore_apply AS component_restore_apply
  WHERE components_to_restore.id = component_restore_apply.finance_component_id;

  GET DIAGNOSTICS v_restored_component_count = ROW_COUNT;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    component_restore_apply.finance_case_id,
    component_restore_apply.finance_component_id,
    'COMPONENT_RESTORED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    NULL::uuid,
    jsonb_build_object('remaining_source_amount', component_restore_apply.remaining_before),
    jsonb_build_object(
      'remaining_source_amount', component_restore_apply.remaining_after,
      'restored_source_amount', component_restore_apply.restore_source_amount,
      'correction_kind', 'NO_MONEY_UNWIND',
      'work_item_id', p_work_item_id
    ),
    'NO_MONEY_UNWIND',
    'Payment correction no-money unwind restored reserved component amount.'
  FROM pg_temp._tmp_no_money_unwind_component_restore_apply AS component_restore_apply;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    released_reservations.finance_case_id,
    released_reservations.finance_component_id,
    'RESERVATION_RELEASED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    released_reservations.reservation_id,
    jsonb_build_object('reservation_status', 'RESERVED_OR_COMMITTED'),
    jsonb_build_object(
      'reservation_status', 'RELEASED',
      'released_reason', 'NO_MONEY_UNWIND',
      'work_item_id', p_work_item_id
    ),
    'NO_MONEY_UNWIND',
    'Payment correction no-money unwind released reservation.'
  FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservations
  WHERE released_reservations.finance_case_id IS NOT NULL;

  UPDATE public.pay_advances AS payout_cases_to_reset
  SET
    payout_status = 'PENDING',
    payout_pay_batch_id = NULL,
    payout_transfer_id = NULL,
    updated_at = v_now
  WHERE payout_cases_to_reset.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND COALESCE(payout_cases_to_reset.payout_status::text, '') <> 'PAID'
    AND (
      payout_cases_to_reset.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases_to_reset.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  GET DIAGNOSTICS v_reset_payout_count = ROW_COUNT;

  UPDATE public.pay_bank_transfers AS transfer_to_mark_failed
  SET
    status = CASE
      WHEN upper(btrim(COALESCE(transfer_to_mark_failed.status, ''))) = 'COMPLETED' THEN transfer_to_mark_failed.status
      ELSE 'FAILED'
    END,
    failed_reason = COALESCE(NULLIF(btrim(COALESCE(transfer_to_mark_failed.failed_reason, '')), ''), 'NO_MONEY_UNWIND'),
    rail_meta_json = COALESCE(transfer_to_mark_failed.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
      'no_money_unwind_applied', true,
      'no_money_unwind_work_item_id', p_work_item_id::text,
      'no_money_unwind_at_utc', v_now
    )
  WHERE transfer_to_mark_failed.id IN (
    SELECT DISTINCT selected_transfer_to_fail.pay_bank_transfer_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_transfer_to_fail
    WHERE selected_transfer_to_fail.pay_bank_transfer_id IS NOT NULL
  )
    AND upper(btrim(COALESCE(transfer_to_mark_failed.status, ''))) <> 'COMPLETED';

  GET DIAGNOSTICS v_updated_transfer_count = ROW_COUNT;

  UPDATE public.pay_batch_candidates AS candidates_to_clear
  SET
    settlement_status = CASE
      WHEN upper(btrim(COALESCE(candidates_to_clear.settlement_status, ''))) = 'SETTLED' THEN NULL
      ELSE candidates_to_clear.settlement_status
    END,
    settled_at_utc = NULL,
    settled_via = NULL,
    settled_note = COALESCE(NULLIF(btrim(COALESCE(candidates_to_clear.settled_note, '')), ''), 'NO_MONEY_UNWIND'),
    updated_at = v_now
  WHERE candidates_to_clear.id IN (
    SELECT DISTINCT selected_candidate_to_clear.pay_batch_candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_candidate_to_clear
    WHERE selected_candidate_to_clear.pay_batch_candidate_id IS NOT NULL
  )
    AND NOT EXISTS (
      SELECT 1
      FROM public.timesheet_pay_state_history
      JOIN pg_temp._tmp_no_money_unwind_selected AS selected_history_check
        ON selected_history_check.timesheet_id = public.timesheet_pay_state_history.timesheet_id
      WHERE public.timesheet_pay_state_history.pay_batch_id = v_work_item.pay_batch_id
        AND selected_history_check.pay_batch_candidate_id = candidates_to_clear.id
    );

  GET DIAGNOSTICS v_updated_candidate_count = ROW_COUNT;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(v_work_unit, ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', v_work_item.pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id::text),
    'is_whole_batch', v_is_whole_batch_work_item,
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(v_work_item.selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.candidate_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.umbrella_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_case_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_component_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.reservation_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.transfer_group_key AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_mail_selected_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_mail_scope_matches;
  CREATE TEMP TABLE _tmp_no_money_unwind_mail_scope_matches ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.type,
      public.mail_outbox.email_type,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.reference,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) = 'QUEUED'
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id AS mail_outbox_id,
      candidate_mail.status,
      candidate_mail.type,
      candidate_mail.email_type,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.reference,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        v_work_item.pay_batch_id,
        v_work_item.selection_json,
        v_mail_selected_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.mail_outbox_id,
    matched_mail.status,
    matched_mail.type,
    matched_mail.email_type,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.reference,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS match_kind,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
  WHERE queued_mail_to_cancel.id = mail_scope_match.mail_outbox_id
    AND upper(btrim(COALESCE(queued_mail_to_cancel.status::text, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.safe_to_cancel, false)
    AND (
      mail_scope_match.match_confidence = 'EXACT'
      OR (
        v_is_whole_batch_work_item
        AND mail_scope_match.match_kind = 'WHOLE_BATCH'
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_communications_review_required_count
  FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
  WHERE upper(btrim(COALESCE(mail_scope_match.status, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.requires_review, false);

  v_mail_scope_matching := jsonb_build_object(
    'exact_cancelled', v_cancelled_mail_count,
    'legacy_review', v_communications_review_required_count,
    'selected_scope_json', v_mail_selected_scope_json,
    'matches', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', mail_scope_match.mail_outbox_id,
        'match_kind', mail_scope_match.match_kind,
        'match_confidence', mail_scope_match.match_confidence,
        'safe_to_cancel', mail_scope_match.safe_to_cancel,
        'requires_review', mail_scope_match.requires_review,
        'reason', mail_scope_match.match_reason,
        'status', mail_scope_match.status,
        'type', mail_scope_match.type,
        'email_type', mail_scope_match.email_type,
        'context_kind', mail_scope_match.context_kind,
        'context_id', mail_scope_match.context_id,
        'recipient_kind', mail_scope_match.recipient_kind,
        'recipient_id', mail_scope_match.recipient_id,
        'reference', mail_scope_match.reference,
        'payment_scope_json', mail_scope_match.payment_scope_json
      ) ORDER BY mail_scope_match.mail_outbox_id)
      FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
    ), '[]'::jsonb)
  );


  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
  SELECT
    'pay_candidate:' || dirty_candidates.candidate_id::text,
    1,
    v_now
  FROM (
    SELECT DISTINCT selected_dirty_candidates.candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_dirty_candidates
    WHERE selected_dirty_candidates.candidate_id IS NOT NULL
  ) AS dirty_candidates
  ON CONFLICT (entity_key)
  DO UPDATE
  SET
    seq = public.app_change_counters.seq + 1,
    updated_at = v_now;

  GET DIAGNOSTICS v_dirty_candidate_count = ROW_COUNT;

  IF v_batch.source_snapshot_run_id IS NOT NULL THEN
    FOR v_candidate_id IN
      SELECT DISTINCT selected_refresh_candidates.candidate_id
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_refresh_candidates
      WHERE selected_refresh_candidates.candidate_id IS NOT NULL
      ORDER BY selected_refresh_candidates.candidate_id
    LOOP
      BEGIN
        v_refresh_result := public.pay_workbench_enqueue_candidate_refresh(
          p_snapshot_run_id => v_batch.source_snapshot_run_id,
          p_candidate_id => v_candidate_id,
          p_reason => 'PAYMENT_CORRECTION_NO_MONEY_UNWIND',
          p_actor_user_id => p_actor_user_id,
          p_payload_json => jsonb_build_object(
            'pay_batch_id', v_work_item.pay_batch_id,
            'correction_request_id', v_work_item.correction_request_id,
            'work_item_id', p_work_item_id,
            'refresh_reason', 'PAYMENT_CORRECTION_NO_MONEY_UNWIND'
          )
        );
      EXCEPTION
        WHEN OTHERS THEN
          PERFORM public._imp_debug_audit(
            p_actor_user_id,
            'PAYMENT_CORRECTION_NO_MONEY_UNWIND_REFRESH_ENQUEUE_ERROR',
            jsonb_build_object(
              'work_item_id', p_work_item_id,
              'candidate_id', v_candidate_id,
              'snapshot_run_id', v_batch.source_snapshot_run_id,
              'sqlstate', SQLSTATE,
              'error_message', SQLERRM
            ),
            'pay_payment_correction',
            p_work_item_id::text,
            NULL::jsonb,
            NULL::text,
            NULL::text,
            NULL::text
          );
      END;
    END LOOP;
  END IF;



v_finance_resolution_result := public._pay_payment_correction_apply_accepted_finance_resolution(
  v_work_item.correction_request_id,
  p_work_item_id,
  v_effective_actor_user_id
);

IF NOT COALESCE(NULLIF(v_finance_resolution_result->>'ok', '')::boolean, false) THEN
  RAISE EXCEPTION 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED'
    USING ERRCODE = 'P0001',
          DETAIL = jsonb_build_object(
            'code', 'ACCEPTED_FINANCE_RESOLUTION_BLOCKED',
            'message', 'Accepted gross/channel-sensitive finance resolution blocked no-money unwind; no partial correction must be committed.',
            'work_item_id', p_work_item_id,
            'correction_request_id', v_work_item.correction_request_id,
            'finance_resolution_result', v_finance_resolution_result
          )::text;
END IF;

v_notice_queue_result := public.pay_payment_return_admin_notice_queue(
  p_notice_kind => 'NO_MONEY_UNWIND_APPLIED',
  p_pay_batch_id => v_work_item.pay_batch_id,
  p_provider_key => COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
  p_execution_commit_ref => v_batch.execution_commit_ref,
  p_summary_json => jsonb_build_object(
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_request_id', v_work_item.correction_request_id,
    'work_item_id', p_work_item_id,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'accepted_finance_resolution', v_finance_resolution_result,
    'applied_at_utc', v_now
  )
);

v_notice_group_id := NULLIF(v_notice_queue_result->>'notice_group_id', '')::uuid;


  v_result := jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'work_item_id', p_work_item_id,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'NO_MONEY_UNWIND',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'updated_transfer_count', v_updated_transfer_count,
    'updated_candidate_count', v_updated_candidate_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'dirty_candidate_count', v_dirty_candidate_count,
    'notice_group_id', v_notice_group_id,
    'notice_queue_result', v_notice_queue_result,
    'accepted_finance_resolution', v_finance_resolution_result,
    'classification_result', v_classification_result,
    'applied_at_utc', v_now
  );

  UPDATE public.pay_payment_correction_work_items AS applied_work_item
  SET
    status = 'APPLIED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = COALESCE(applied_work_item.processed_at_utc, v_now),
    last_error = NULL,
    result_json = COALESCE(applied_work_item.result_json, '{}'::jsonb) || v_result
  WHERE applied_work_item.id = p_work_item_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT',
    v_result,
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_WORK_ITEM_BLOCKED_BY_EXCEPTION',
      'message', 'No-money unwind work item was blocked after an exception; no partial correction was committed.',
      'work_item_id', p_work_item_id,
      'correction_request_id', CASE WHEN v_work_item.correction_request_id IS NULL THEN NULL ELSE v_work_item.correction_request_id END,
      'pay_batch_id', CASE WHEN v_work_item.pay_batch_id IS NULL THEN NULL ELSE v_work_item.pay_batch_id END,
      'sqlstate', SQLSTATE,
      'error_message', SQLERRM,
      'blocked_at_utc', v_now
    );

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_ERROR',
      v_blocker,
      'pay_payment_correction',
      COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    IF p_work_item_id IS NOT NULL THEN
      UPDATE public.pay_payment_correction_work_items AS exception_blocked_work_item
      SET
        status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = COALESCE(exception_blocked_work_item.processed_at_utc, v_now),
        last_error = SQLERRM,
        result_json = COALESCE(exception_blocked_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'processed_at_utc', v_now
        )
      WHERE exception_blocked_work_item.id = p_work_item_id;
    END IF;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker
    );
END;
$function$;


CREATE OR REPLACE FUNCTION public._pay_payment_correction_validate_accepted_finance_resolution(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_plan_json jsonb,
  p_accepted_resolution_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch public.pay_batches%rowtype;
  v_suggested_required boolean := false;
  v_plan_resolution jsonb := NULL::jsonb;
  v_plan_cases jsonb := '[]'::jsonb;
  v_resolution_root jsonb := NULL::jsonb;
  v_resolution_body jsonb := NULL::jsonb;
  v_resolution_cases jsonb := '[]'::jsonb;
  v_plan_case_json jsonb := NULL::jsonb;
  v_accepted_case_json jsonb := NULL::jsonb;
  v_case_row public.pay_advances%rowtype;
  v_component_record record;

  v_finance_case_id uuid := NULL::uuid;
  v_plan_candidate_id uuid := NULL::uuid;
  v_plan_candidate_id_text text := NULL::text;
  v_accepted_candidate_id_text text := NULL::text;
  v_plan_apply_surface text := NULL::text;
  v_accepted_apply_surface text := NULL::text;
  v_plan_effective_pay_date_text text := NULL::text;
  v_accepted_effective_pay_date_text text := NULL::text;
  v_plan_effective_pay_date date := NULL::date;

  v_plan_selected_component_ids_json jsonb := '[]'::jsonb;
  v_accepted_component_ids_json jsonb := '[]'::jsonb;
  v_plan_component_ids uuid[] := ARRAY[]::uuid[];
  v_accepted_component_ids uuid[] := ARRAY[]::uuid[];
  v_current_component_ids uuid[] := ARRAY[]::uuid[];
  v_plan_component_count integer := 0;
  v_accepted_component_count integer := 0;
  v_current_component_count integer := 0;
  v_invalid_component_id_count integer := 0;

  v_plan_fingerprints_json jsonb := '{}'::jsonb;
  v_accepted_fingerprints_json jsonb := '{}'::jsonb;
  v_expected_fingerprint text := NULL::text;
  v_plan_fingerprint text := NULL::text;
  v_current_fingerprint text := NULL::text;
  v_missing_fingerprint_count integer := 0;
  v_fingerprint_mismatch_count integer := 0;
  v_plan_fingerprint_mismatch_count integer := 0;
  v_stale_component_count integer := 0;
  v_closed_unrecoverable_component_count integer := 0;

  v_plan_suggestion_hash text := NULL::text;
  v_accepted_suggestion_hash text := NULL::text;
  v_regenerated_suggestion jsonb := NULL::jsonb;
  v_regenerated_suggestion_hash text := NULL::text;
  v_resolution_path text := NULL::text;
  v_resolution_mode text := NULL::text;
  v_weeks_total text := NULL::text;
  v_weekly_due text := NULL::text;
  v_manual_total_remaining text := NULL::text;

  v_affected_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_open_overlap_count integer := 0;
  v_extra_accepted_case_count integer := 0;
  v_case_count integer := 0;
  v_blocker jsonb := NULL::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAY_BATCH_ID_REQUIRED',
        'message', 'p_pay_batch_id is required.'
      )
    );
  END IF;

  IF p_plan_json IS NULL OR COALESCE(jsonb_typeof(p_plan_json), 'null') <> 'object' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_PLAN_JSON_MUST_BE_OBJECT',
        'message', 'p_plan_json must be an object.'
      )
    );
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAY_BATCH_NOT_FOUND',
        'message', 'The selected pay batch does not exist.',
        'pay_batch_id', p_pay_batch_id::text
      )
    );
  END IF;

  v_suggested_required := COALESCE((p_plan_json->>'suggested_resolution_required')::boolean, false)
    OR COALESCE((p_plan_json#>>'{suggested_resolution,required}')::boolean, false);

  v_plan_resolution := COALESCE(p_plan_json->'suggested_resolution', '{}'::jsonb);
  v_plan_cases := COALESCE(v_plan_resolution->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_plan_cases), 'null') <> 'array' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', v_suggested_required,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_PLAN_FINANCE_CASES_INVALID',
        'message', 'p_plan_json.suggested_resolution.finance_cases must be an array when suggested resolution is required.'
      )
    );
  END IF;

  SELECT count(*)::integer
  INTO v_case_count
  FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value);

  IF NOT v_suggested_required OR v_case_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'required', false,
      'validated', false,
      'finance_case_count', 0,
      'message', 'No accepted gross/channel-sensitive finance resolution is required for this correction plan.'
    );
  END IF;

  IF p_accepted_resolution_json IS NULL
     OR COALESCE(jsonb_typeof(p_accepted_resolution_json), 'null') <> 'object' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_REQUIRED',
        'message', 'accepted_resolution_json is required for selected gross/taxable/channel-sensitive finance items.',
        'finance_case_count', v_case_count
      )
    );
  END IF;

  v_resolution_root := p_accepted_resolution_json;
  v_resolution_body := CASE
    WHEN v_resolution_root ? 'suggested_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'suggested_resolution'), 'null') = 'object'
      THEN v_resolution_root->'suggested_resolution'
    WHEN v_resolution_root ? 'accepted_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'accepted_resolution'), 'null') = 'object'
      THEN v_resolution_root->'accepted_resolution'
    ELSE v_resolution_root
  END;

  v_resolution_cases := COALESCE(v_resolution_body->'finance_cases', v_resolution_root->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_resolution_cases), 'null') <> 'array' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_FINANCE_CASES_INVALID',
        'message', 'accepted_resolution_json.finance_cases must be an array.'
      )
    );
  END IF;

  SELECT count(*)::integer
  INTO v_extra_accepted_case_count
  FROM jsonb_array_elements(v_resolution_cases) AS accepted_case_elements(value)
  WHERE NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '') IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value)
      WHERE NULLIF(btrim(COALESCE(plan_case_elements.value->>'finance_case_id', '')), '') = NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '')
    );

  IF v_extra_accepted_case_count > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EXTRA_FINANCE_CASE',
        'message', 'accepted_resolution_json includes finance cases that are not present in the current correction plan.',
        'extra_case_count', v_extra_accepted_case_count
      )
    );
  END IF;

  FOR v_plan_case_json IN
    SELECT plan_case_elements.value
    FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value)
    ORDER BY NULLIF(btrim(COALESCE(plan_case_elements.value->>'finance_case_id', '')), '')
  LOOP
    IF NULLIF(btrim(COALESCE(v_plan_case_json->>'finance_case_id', '')), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_FINANCE_CASE_ID_INVALID',
          'message', 'The correction plan contains an invalid finance_case_id.',
          'finance_case_id', v_plan_case_json->>'finance_case_id'
        )
      );
    END IF;

    v_finance_case_id := (v_plan_case_json->>'finance_case_id')::uuid;
    v_plan_candidate_id_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'candidate_id', '')), '');
    v_plan_candidate_id := CASE
      WHEN v_plan_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN v_plan_candidate_id_text::uuid
      ELSE NULL::uuid
    END;

    SELECT public.pay_advances.*
    INTO v_case_row
    FROM public.pay_advances
    WHERE public.pay_advances.id = v_finance_case_id;

    IF NOT FOUND THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_NOT_FOUND',
          'message', 'A selected finance case no longer exists.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_plan_candidate_id IS NOT NULL AND v_case_row.candidate_id IS DISTINCT FROM v_plan_candidate_id THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_FINANCE_CASE_CANDIDATE_MISMATCH',
          'message', 'The plan finance case candidate does not match the current finance case candidate.',
          'finance_case_id', v_finance_case_id::text,
          'planned_candidate_id', v_plan_candidate_id::text,
          'current_candidate_id', v_case_row.candidate_id::text
        )
      );
    END IF;

    SELECT accepted_case_elements.value
    INTO v_accepted_case_json
    FROM jsonb_array_elements(v_resolution_cases) AS accepted_case_elements(value)
    WHERE NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '') = v_finance_case_id::text
    LIMIT 1;

    IF v_accepted_case_json IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_CASE_MISSING',
          'message', 'accepted_resolution_json does not include a selected gross/channel-sensitive finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    v_accepted_candidate_id_text := NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '');

    IF v_accepted_candidate_id_text IS NOT NULL
       AND v_accepted_candidate_id_text <> v_case_row.candidate_id::text THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_CANDIDATE_MISMATCH',
          'message', 'accepted_resolution_json candidate_id does not match the selected finance case candidate.',
          'finance_case_id', v_finance_case_id::text,
          'accepted_candidate_id', v_accepted_candidate_id_text,
          'selected_candidate_id', v_case_row.candidate_id::text
        )
      );
    END IF;

    v_plan_selected_component_ids_json := COALESCE(
      v_plan_case_json->'selected_component_ids',
      v_plan_case_json->'component_ids',
      '[]'::jsonb
    );

    v_accepted_component_ids_json := COALESCE(
      v_accepted_case_json->'selected_component_ids',
      v_accepted_case_json->'component_ids',
      '[]'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_plan_selected_component_ids_json), 'null') <> 'array'
       OR COALESCE(jsonb_typeof(v_accepted_component_ids_json), 'null') <> 'array' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_IDS_INVALID',
          'message', 'Plan and accepted component ids must both be arrays.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    SELECT count(*)::integer
    INTO v_invalid_component_id_count
    FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
    WHERE accepted_component_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    IF v_invalid_component_id_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_ID_INVALID',
          'message', 'accepted_resolution_json contains invalid finance component ids.',
          'finance_case_id', v_finance_case_id::text,
          'invalid_component_id_count', v_invalid_component_id_count
        )
      );
    END IF;

    SELECT COALESCE(array_agg(DISTINCT plan_component_ids.value::uuid ORDER BY plan_component_ids.value::uuid), ARRAY[]::uuid[])
    INTO v_plan_component_ids
    FROM jsonb_array_elements_text(v_plan_selected_component_ids_json) AS plan_component_ids(value)
    WHERE plan_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT accepted_component_ids.value::uuid ORDER BY accepted_component_ids.value::uuid), ARRAY[]::uuid[])
    INTO v_accepted_component_ids
    FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
    WHERE accepted_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    v_plan_component_count := COALESCE(array_length(v_plan_component_ids, 1), 0);
    v_accepted_component_count := COALESCE(array_length(v_accepted_component_ids, 1), 0);

    IF v_plan_component_count = 0 OR v_plan_component_ids IS DISTINCT FROM v_accepted_component_ids THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
          'message', 'accepted_resolution_json selected component ids do not exactly match the current correction plan.',
          'finance_case_id', v_finance_case_id::text,
          'planned_component_count', v_plan_component_count,
          'accepted_component_count', v_accepted_component_count
        )
      );
    END IF;

    SELECT COALESCE(array_agg(public.pay_finance_case_components.id ORDER BY public.pay_finance_case_components.id), ARRAY[]::uuid[])
    INTO v_current_component_ids
    FROM public.pay_finance_case_components
    WHERE public.pay_finance_case_components.finance_case_id = v_finance_case_id
      AND public.pay_finance_case_components.id = ANY(v_plan_component_ids);

    v_current_component_count := COALESCE(array_length(v_current_component_ids, 1), 0);

    IF v_current_component_ids IS DISTINCT FROM v_plan_component_ids THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
          'message', 'One or more selected finance components no longer belongs to the planned finance case.',
          'finance_case_id', v_finance_case_id::text,
          'planned_component_count', v_plan_component_count,
          'current_component_count', v_current_component_count
        )
      );
    END IF;

    v_plan_fingerprints_json := COALESCE(
      v_plan_case_json->'current_component_fingerprints',
      v_plan_case_json#>'{suggestion_hash_basis,component_fingerprints}',
      v_plan_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    v_accepted_fingerprints_json := COALESCE(
      v_accepted_case_json->'current_component_fingerprints',
      v_accepted_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_plan_fingerprints_json), 'null') <> 'object'
       OR COALESCE(jsonb_typeof(v_accepted_fingerprints_json), 'null') <> 'object' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_FINGERPRINTS_INVALID',
          'message', 'Plan and accepted component fingerprints must both be objects.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    v_missing_fingerprint_count := 0;
    v_fingerprint_mismatch_count := 0;
    v_plan_fingerprint_mismatch_count := 0;
    v_stale_component_count := 0;
    v_closed_unrecoverable_component_count := 0;

    FOR v_component_record IN
      SELECT public.pay_finance_case_components.*
      FROM public.pay_finance_case_components
      WHERE public.pay_finance_case_components.finance_case_id = v_finance_case_id
        AND public.pay_finance_case_components.id = ANY(v_plan_component_ids)
      ORDER BY public.pay_finance_case_components.id
    LOOP
      v_current_fingerprint := COALESCE(
        NULLIF(btrim(v_component_record.resolution_fingerprint), ''),
        md5(jsonb_build_object(
          'finance_component_id', v_component_record.id,
          'finance_case_id', v_component_record.finance_case_id,
          'classification', v_component_record.classification::text,
          'source_pay_method', v_component_record.source_pay_method,
          'source_amount', v_component_record.source_amount,
          'remaining_source_amount', v_component_record.remaining_source_amount,
          'saved_target_pay_method', v_component_record.saved_target_pay_method,
          'saved_resolution_mode', v_component_record.saved_resolution_mode::text,
          'saved_resolution_payload_json', v_component_record.saved_resolution_payload_json,
          'saved_resolution_result_json', v_component_record.saved_resolution_result_json,
          'is_resolution_stale', v_component_record.is_resolution_stale,
          'closed_at_utc', v_component_record.closed_at_utc,
          'updated_at_utc', v_component_record.updated_at_utc
        )::text)
      );

      v_expected_fingerprint := NULLIF(btrim(COALESCE(v_accepted_fingerprints_json->>v_component_record.id::text, '')), '');
      v_plan_fingerprint := NULLIF(btrim(COALESCE(v_plan_fingerprints_json->>v_component_record.id::text, '')), '');

      IF v_expected_fingerprint IS NULL THEN
        v_missing_fingerprint_count := v_missing_fingerprint_count + 1;
      ELSIF v_expected_fingerprint <> v_current_fingerprint THEN
        v_fingerprint_mismatch_count := v_fingerprint_mismatch_count + 1;
      END IF;

      IF v_plan_fingerprint IS NOT NULL AND v_plan_fingerprint <> v_expected_fingerprint THEN
        v_plan_fingerprint_mismatch_count := v_plan_fingerprint_mismatch_count + 1;
      END IF;

      IF COALESCE(v_component_record.is_resolution_stale, false) THEN
        v_stale_component_count := v_stale_component_count + 1;
      END IF;

      IF v_component_record.closed_at_utc IS NOT NULL
         AND round(COALESCE(v_component_record.remaining_source_amount, 0), 2) <= 0 THEN
        v_closed_unrecoverable_component_count := v_closed_unrecoverable_component_count + 1;
      END IF;
    END LOOP;

    IF v_missing_fingerprint_count > 0
       OR v_fingerprint_mismatch_count > 0
       OR v_plan_fingerprint_mismatch_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because one or more component fingerprints no longer match.',
          'finance_case_id', v_finance_case_id::text,
          'missing_fingerprint_count', v_missing_fingerprint_count,
          'fingerprint_mismatch_count', v_fingerprint_mismatch_count,
          'plan_fingerprint_mismatch_count', v_plan_fingerprint_mismatch_count
        )
      );
    END IF;

    IF v_stale_component_count > 0 OR v_closed_unrecoverable_component_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_MANUAL_REVIEW_REQUIRED',
          'message', 'Selected finance components are stale or closed in a way that requires manual review before correction apply.',
          'finance_case_id', v_finance_case_id::text,
          'stale_component_count', v_stale_component_count,
          'closed_unrecoverable_component_count', v_closed_unrecoverable_component_count
        )
      );
    END IF;

    IF v_case_row.status <> 'ACTIVE'::public.pay_advance_status_enum
       OR v_case_row.written_off_at_utc IS NOT NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_NOT_ACTIVE_OR_WRITTEN_OFF',
          'message', 'Selected finance case is no longer active or has been written off.',
          'finance_case_id', v_finance_case_id::text,
          'status', v_case_row.status::text,
          'written_off_at_utc', v_case_row.written_off_at_utc
        )
      );
    END IF;

    SELECT COALESCE(array_agg(DISTINCT (affected_item_elements.value->>'reservation_id')::uuid ORDER BY (affected_item_elements.value->>'reservation_id')::uuid), ARRAY[]::uuid[])
    INTO v_affected_reservation_ids
    FROM jsonb_array_elements(COALESCE(p_plan_json->'affected_items', '[]'::jsonb)) AS affected_item_elements(value)
    WHERE NULLIF(btrim(COALESCE(affected_item_elements.value->>'finance_case_id', '')), '') = v_finance_case_id::text
      AND NULLIF(btrim(COALESCE(affected_item_elements.value->>'reservation_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT count(*)::integer
    INTO v_open_overlap_count
    FROM public.pay_batch_items AS overlapping_items
    JOIN public.pay_batch_candidates AS overlapping_candidates
      ON overlapping_candidates.id = overlapping_items.pay_batch_candidate_id
    JOIN public.pay_batches AS overlapping_batches
      ON overlapping_batches.id = overlapping_candidates.pay_batch_id
    WHERE overlapping_candidates.pay_batch_id <> p_pay_batch_id
      AND COALESCE(overlapping_items.is_voided, false) = false
      AND overlapping_batches.cancelled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(overlapping_batches.status)
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS overlap_corrections
        WHERE overlap_corrections.pay_batch_item_id = overlapping_items.id
          AND overlap_corrections.status = 'APPLIED'
      )
      AND (
        overlapping_items.finance_case_id = v_finance_case_id
        OR overlapping_items.finance_component_id = ANY(v_plan_component_ids)
        OR (
          COALESCE(array_length(v_affected_reservation_ids, 1), 0) > 0
          AND overlapping_items.reservation_id = ANY(v_affected_reservation_ids)
        )
      );

    IF v_open_overlap_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'DRAFT_BATCH_INTERFERENCE',
          'message', 'An open overlapping draft/reserved batch already references the selected finance case/component/reservation. Delete or cancel the overlapping draft first.',
          'finance_case_id', v_finance_case_id::text,
          'overlap_count', v_open_overlap_count
        )
      );
    END IF;

    v_plan_apply_surface := COALESCE(
      NULLIF(btrim(v_plan_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_plan_resolution->>'apply_surface'), ''),
      'pay_finance_case_apply_taxable_channel_restructure'
    );

    v_accepted_apply_surface := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_resolution_body->>'apply_surface'), ''),
      v_plan_apply_surface
    );

    IF v_accepted_apply_surface NOT IN (
      'pay_finance_case_apply_taxable_channel_restructure',
      'pay_manual_debt_adjustment_resolve_taxable_channel_change',
      'pay_finance_component_resolutions_apply'
    ) THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_UNSUPPORTED',
          'message', 'accepted_resolution_json apply_surface is not supported by the payment correction finance validation helper.',
          'finance_case_id', v_finance_case_id::text,
          'apply_surface', v_accepted_apply_surface
        )
      );
    END IF;

    IF v_plan_apply_surface IS NOT NULL AND v_accepted_apply_surface <> v_plan_apply_surface THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_MISMATCH',
          'message', 'accepted_resolution_json apply_surface does not match the correction plan apply_surface.',
          'finance_case_id', v_finance_case_id::text,
          'planned_apply_surface', v_plan_apply_surface,
          'accepted_apply_surface', v_accepted_apply_surface
        )
      );
    END IF;

    v_plan_effective_pay_date_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'effective_pay_date', p_plan_json#>>'{batch,effective_pay_date}', '')), '');
    v_accepted_effective_pay_date_text := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'effective_pay_date'), ''),
      NULLIF(btrim(v_resolution_body->>'effective_pay_date'), ''),
      v_plan_effective_pay_date_text
    );

    IF v_plan_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$'
       OR v_accepted_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_INVALID',
          'message', 'Plan and accepted effective_pay_date must both be YYYY-MM-DD.',
          'finance_case_id', v_finance_case_id::text,
          'planned_effective_pay_date', v_plan_effective_pay_date_text,
          'accepted_effective_pay_date', v_accepted_effective_pay_date_text
        )
      );
    END IF;

    IF v_accepted_effective_pay_date_text <> v_plan_effective_pay_date_text THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_MISMATCH',
          'message', 'accepted_resolution_json effective_pay_date does not match the correction plan effective_pay_date.',
          'finance_case_id', v_finance_case_id::text,
          'planned_effective_pay_date', v_plan_effective_pay_date_text,
          'accepted_effective_pay_date', v_accepted_effective_pay_date_text
        )
      );
    END IF;

    v_plan_effective_pay_date := v_plan_effective_pay_date_text::date;
    v_plan_suggestion_hash := NULLIF(btrim(COALESCE(v_plan_case_json->>'suggestion_hash', '')), '');
    v_accepted_suggestion_hash := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'suggestion_hash'), ''),
      NULLIF(btrim(v_resolution_body->>'suggestion_hash'), '')
    );

    IF v_plan_suggestion_hash IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_SUGGESTION_HASH_MISSING',
          'message', 'The correction plan does not include a suggestion_hash for the selected finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_accepted_suggestion_hash IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_SUGGESTION_HASH_MISSING',
          'message', 'accepted_resolution_json does not include suggestion_hash for the selected finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_accepted_suggestion_hash <> v_plan_suggestion_hash THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'accepted_resolution_json suggestion_hash does not match the correction plan suggestion_hash.',
          'finance_case_id', v_finance_case_id::text,
          'planned_suggestion_hash', v_plan_suggestion_hash,
          'accepted_suggestion_hash', v_accepted_suggestion_hash
        )
      );
    END IF;

    IF p_actor_user_id IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION_VALIDATION',
          'message', 'A user id is required to regenerate and validate a gross/channel-sensitive finance suggestion.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    BEGIN
      v_regenerated_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
        p_finance_case_id => v_finance_case_id,
        p_actor_user_id => p_actor_user_id,
        p_effective_pay_date => v_plan_effective_pay_date,
        p_resolution_path => 'SUGGESTED',
        p_schedule_input_mode => NULL::text,
        p_weeks_total => NULL::integer,
        p_weekly_due => NULL::numeric,
        p_manual_total_remaining => NULL::numeric,
        p_note => 'Regenerated for payment correction accepted-resolution validation ' || p_pay_batch_id::text
      );
    EXCEPTION WHEN OTHERS THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'SUGGESTED_RESOLUTION_REGENERATION_FAILED',
          'message', 'The gross/channel-sensitive finance suggestion could not be regenerated during accepted-resolution validation.',
          'finance_case_id', v_finance_case_id::text,
          'sqlstate', SQLSTATE,
          'error_message', SQLERRM
        )
      );
    END;

    v_resolution_path := COALESCE(v_regenerated_suggestion->>'resolution_path', v_regenerated_suggestion#>>'{request,resolution_path}', 'SUGGESTED');
    v_resolution_mode := COALESCE(v_regenerated_suggestion->>'resolution_mode', v_regenerated_suggestion#>>'{result,resolution_mode}', v_regenerated_suggestion#>>'{suggestion,resolution_mode}');
    v_weeks_total := COALESCE(v_regenerated_suggestion->>'weeks_total', v_regenerated_suggestion#>>'{result,weeks_total}', v_regenerated_suggestion#>>'{suggestion,weeks_total}');
    v_weekly_due := COALESCE(v_regenerated_suggestion->>'weekly_due', v_regenerated_suggestion#>>'{result,weekly_due}', v_regenerated_suggestion#>>'{suggestion,weekly_due}');
    v_manual_total_remaining := COALESCE(v_regenerated_suggestion->>'manual_total_remaining', v_regenerated_suggestion#>>'{result,manual_total_remaining}', v_regenerated_suggestion#>>'{suggestion,manual_total_remaining}');

    v_regenerated_suggestion_hash := md5(jsonb_build_object(
      'finance_case_id', v_finance_case_id,
      'candidate_id', v_case_row.candidate_id,
      'component_ids', COALESCE(v_plan_case_json->'component_ids', to_jsonb(v_plan_component_ids)),
      'selected_component_ids', COALESCE(v_plan_case_json->'selected_component_ids', to_jsonb(v_plan_component_ids)),
      'component_fingerprints', v_plan_fingerprints_json,
      'effective_pay_date', v_plan_effective_pay_date,
      'apply_surface', v_plan_apply_surface,
      'resolution_path', v_resolution_path,
      'resolution_mode', v_resolution_mode,
      'weeks_total', v_weeks_total,
      'weekly_due', v_weekly_due,
      'manual_total_remaining', v_manual_total_remaining,
      'taxable_channel_result', COALESCE(
        v_regenerated_suggestion->'taxable_channel_result',
        v_regenerated_suggestion->'result',
        v_regenerated_suggestion->'suggestion',
        v_regenerated_suggestion
      ) - 'generated_at'
        - 'generated_at_utc'
        - 'created_at'
        - 'created_at_utc'
        - 'updated_at'
        - 'updated_at_utc'
        - 'audit'
        - 'debug'
    )::text);

    IF v_regenerated_suggestion_hash <> v_accepted_suggestion_hash THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because the regenerated suggestion hash no longer matches the accepted suggestion hash.',
          'finance_case_id', v_finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'regenerated_suggestion_hash', v_regenerated_suggestion_hash
        )
      );
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'required', true,
    'validated', true,
    'finance_case_count', v_case_count,
    'pay_batch_id', p_pay_batch_id::text,
    'validated_at_utc', now()
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public._pay_payment_correction_mail_scope_match(
  p_mail_outbox_id uuid,
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_selected_scope_json jsonb,
  p_allow_legacy_broad_match boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
SET search_path TO 'public'
AS $function$
DECLARE
  v_mail_row public.mail_outbox%ROWTYPE;
  v_mail_scope jsonb := '{}'::jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_uuid_pattern text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

  v_selected_pay_batch_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_selected_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_selected_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_selected_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_selected_payout_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_group_keys text[] := ARRAY[]::text[];

  v_mail_pay_batch_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_mail_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_mail_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_mail_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_mail_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_mail_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_mail_payout_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_mail_transfer_group_keys text[] := ARRAY[]::text[];

  v_mail_batch_matches boolean := false;
  v_is_whole_batch_scope boolean := false;
  v_selected_has_narrow_payment_scope boolean := false;
  v_selected_candidate_scope_complete boolean := false;
  v_selected_has_candidate_filter boolean := false;
  v_selected_has_umbrella_filter boolean := false;
  v_candidate_filter_requires_payment_match boolean := false;
  v_umbrella_filter_requires_payment_match boolean := false;
  v_mail_candidate_scope_matches boolean := false;
  v_mail_umbrella_scope_matches boolean := false;
  v_transfer_scope_candidate_safe boolean := false;
  v_transfer_scope_umbrella_safe boolean := false;
  v_transfer_scope_safe boolean := false;

  v_item_exact boolean := false;
  v_item_partial boolean := false;
  v_transfer_exact boolean := false;
  v_transfer_partial boolean := false;
  v_payout_transfer_exact boolean := false;
  v_payout_transfer_partial boolean := false;
  v_transfer_group_exact boolean := false;
  v_transfer_group_partial boolean := false;
  v_finance_case_exact boolean := false;
  v_finance_case_partial boolean := false;
  v_finance_component_exact boolean := false;
  v_finance_component_partial boolean := false;
  v_reservation_exact boolean := false;
  v_reservation_partial boolean := false;
  v_pay_batch_candidate_exact boolean := false;
  v_pay_batch_candidate_partial boolean := false;
  v_umbrella_group_exact boolean := false;
  v_umbrella_partial boolean := false;
  v_context_exact boolean := false;
  v_legacy_broad_match boolean := false;
  v_reference_broad_match boolean := false;
  v_recipient_broad_match boolean := false;
  v_payment_scope_broad_match boolean := false;
  v_partial_overlap_requires_review boolean := false;

  v_match_kind text := 'NONE';
  v_match_confidence text := 'NONE';
  v_safe_to_cancel boolean := false;
  v_requires_review boolean := false;
  v_reason text := 'NO_SCOPE_MATCH';
BEGIN
  IF p_mail_outbox_id IS NULL THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_OUTBOX_ID_REQUIRED'
    );
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'PAY_BATCH_ID_REQUIRED',
      'mail_outbox_id', p_mail_outbox_id
    );
  END IF;

  SELECT public.mail_outbox.*
  INTO v_mail_row
  FROM public.mail_outbox
  WHERE public.mail_outbox.id = p_mail_outbox_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_OUTBOX_NOT_FOUND',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id
    );
  END IF;

  v_mail_scope := COALESCE(v_mail_row.payment_scope_json, '{}'::jsonb);
  v_scope_type := upper(btrim(COALESCE(p_selected_scope_json->>'scope_type', p_selection_json->>'scope_type', '')));
  v_work_unit := upper(btrim(COALESCE(p_selected_scope_json->>'work_unit', p_selection_json->>'work_unit', '')));

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_id'), ('pay_batch_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_item_id'), ('pay_batch_item_ids'), ('selected_pay_batch_item_ids'), ('expected_pay_batch_item_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_item_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_candidate_id'), ('pay_batch_candidate_ids'), ('selected_pay_batch_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('candidate_id'), ('candidate_ids'), ('selected_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_bank_transfer_id'), ('pay_bank_transfer_ids'), ('selected_pay_bank_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_bank_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('umbrella_id'), ('umbrella_ids'), ('selected_umbrella_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_umbrella_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_case_id'), ('finance_case_ids'), ('selected_finance_case_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_finance_case_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_component_id'), ('finance_component_ids'), ('selected_finance_component_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_finance_component_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('reservation_id'), ('reservation_ids'), ('selected_reservation_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_reservation_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('payout_transfer_id'), ('payout_transfer_ids'), ('selected_payout_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_payout_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('transfer_group_key'), ('transfer_group_keys'), ('selected_transfer_group_keys')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT text_values.text_value), ARRAY[]::text[])
  INTO v_selected_transfer_group_keys
  FROM (
    SELECT raw_values.raw_value AS text_value
    FROM raw_values
    WHERE NULLIF(raw_values.raw_value, '') IS NOT NULL
  ) AS text_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_id'), ('pay_batch_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_item_id'), ('pay_batch_item_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_item_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_candidate_id'), ('pay_batch_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('candidate_id'), ('candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_bank_transfer_id'), ('pay_bank_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_bank_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('umbrella_id'), ('umbrella_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_umbrella_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_case_id'), ('finance_case_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_finance_case_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_component_id'), ('finance_component_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_finance_component_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('reservation_id'), ('reservation_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_reservation_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('payout_transfer_id'), ('payout_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_payout_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('transfer_group_key'), ('transfer_group_keys')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT text_values.text_value), ARRAY[]::text[])
  INTO v_mail_transfer_group_keys
  FROM (
    SELECT raw_values.raw_value AS text_value
    FROM raw_values
    WHERE NULLIF(raw_values.raw_value, '') IS NOT NULL
  ) AS text_values;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batches', 'pay_batch')
     AND v_mail_row.context_id = p_pay_batch_id THEN
    v_mail_pay_batch_ids := ARRAY(
      SELECT DISTINCT context_batch_ids.batch_id
      FROM unnest(v_mail_pay_batch_ids || ARRAY[p_pay_batch_id]) AS context_batch_ids(batch_id)
      WHERE context_batch_ids.batch_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_batch_item_ids := ARRAY(
      SELECT DISTINCT context_item_ids.item_id
      FROM unnest(v_mail_pay_batch_item_ids || ARRAY[v_mail_row.context_id]) AS context_item_ids(item_id)
      WHERE context_item_ids.item_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_bank_transfer_ids := ARRAY(
      SELECT DISTINCT context_transfer_ids.transfer_id
      FROM unnest(v_mail_pay_bank_transfer_ids || ARRAY[v_mail_row.context_id]) AS context_transfer_ids(transfer_id)
      WHERE context_transfer_ids.transfer_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_batch_candidate_ids := ARRAY(
      SELECT DISTINCT context_candidate_ids.pay_batch_candidate_id
      FROM unnest(v_mail_pay_batch_candidate_ids || ARRAY[v_mail_row.context_id]) AS context_candidate_ids(pay_batch_candidate_id)
      WHERE context_candidate_ids.pay_batch_candidate_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advances', 'pay_advance', 'finance_case', 'finance_cases')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_finance_case_ids := ARRAY(
      SELECT DISTINCT context_finance_case_ids.finance_case_id
      FROM unnest(v_mail_finance_case_ids || ARRAY[v_mail_row.context_id]) AS context_finance_case_ids(finance_case_id)
      WHERE context_finance_case_ids.finance_case_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_reservation_ids := ARRAY(
      SELECT DISTINCT context_reservation_ids.reservation_id
      FROM unnest(v_mail_reservation_ids || ARRAY[v_mail_row.context_id]) AS context_reservation_ids(reservation_id)
      WHERE context_reservation_ids.reservation_id IS NOT NULL
    );
  END IF;

  v_mail_batch_matches := (
    p_pay_batch_id = ANY(v_mail_pay_batch_ids)
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batches', 'pay_batch')
      AND v_mail_row.context_id = p_pay_batch_id
    )
  );

  v_is_whole_batch_scope := (
    v_scope_type = 'BATCH'
    AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) = 0
  ) OR (lower(btrim(COALESCE(p_selected_scope_json->>'is_whole_batch', ''))) IN ('true', 't', '1', 'yes'));

  v_selected_has_narrow_payment_scope := (
    COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    OR COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
  );

  v_selected_candidate_scope_complete := (
    (lower(btrim(COALESCE(p_selected_scope_json->>'selected_candidate_scope_complete', ''))) IN ('true', 't', '1', 'yes'))
    OR (lower(btrim(COALESCE(p_selected_scope_json->>'candidate_scope_complete', ''))) IN ('true', 't', '1', 'yes'))
    OR (
      v_scope_type = 'CANDIDATES'
      AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND v_selected_has_narrow_payment_scope = false
    )
  );

  v_selected_has_candidate_filter := (
    v_scope_type = 'CANDIDATES'
    OR COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
  );

  v_selected_has_umbrella_filter := (
    v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
    OR COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
  );

  v_candidate_filter_requires_payment_match := (
    v_scope_type = 'CANDIDATES'
    AND v_selected_has_candidate_filter
    AND v_selected_has_narrow_payment_scope
  );

  v_umbrella_filter_requires_payment_match := (
    v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
    AND v_selected_has_umbrella_filter
    AND v_selected_has_narrow_payment_scope
  );

  v_mail_candidate_scope_matches := (
    (
      COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
      AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    )
    OR (
      COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_candidate_ids, 1), 0) > 0
      AND v_mail_candidate_ids && v_selected_candidate_ids
    )
    OR (
      COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'CANDIDATE'
      AND v_mail_row.recipient_id IS NOT NULL
      AND v_mail_row.recipient_id = ANY(v_selected_candidate_ids)
    )
  );

  v_mail_umbrella_scope_matches := (
    (
      COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
      AND v_mail_umbrella_ids && v_selected_umbrella_ids
    )
    OR (
      COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'UMBRELLA'
      AND v_mail_row.recipient_id IS NOT NULL
      AND v_mail_row.recipient_id = ANY(v_selected_umbrella_ids)
    )
  );

  v_transfer_scope_candidate_safe := (
    v_candidate_filter_requires_payment_match = false
    OR v_mail_candidate_scope_matches
  );

  v_transfer_scope_umbrella_safe := (
    v_umbrella_filter_requires_payment_match = false
    OR v_mail_umbrella_scope_matches
  );

  v_transfer_scope_safe := (
    v_transfer_scope_candidate_safe
    AND v_transfer_scope_umbrella_safe
  );

  IF v_mail_batch_matches = false THEN
    SELECT EXISTS(
      SELECT 1
      FROM unnest(v_selected_pay_batch_ids || ARRAY[p_pay_batch_id]) AS selected_batch_ids(batch_id)
      WHERE selected_batch_ids.batch_id IS NOT NULL
        AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_batch_ids.batch_id::text || '%'
    )
    INTO v_reference_broad_match;

    IF v_reference_broad_match THEN
      IF v_is_whole_batch_scope THEN
        RETURN jsonb_build_object(
          'matched', true,
          'match_kind', 'WHOLE_BATCH',
          'match_confidence', 'LEGACY_BROAD',
          'safe_to_cancel', true,
          'requires_review', false,
          'reason', 'WHOLE_BATCH_SCOPE_MATCH_BY_LEGACY_BATCH_REFERENCE',
          'mail_outbox_id', p_mail_outbox_id,
          'pay_batch_id', p_pay_batch_id,
          'mail_status', v_mail_row.status::text,
          'mail_type', v_mail_row.type,
          'recipient_kind', v_mail_row.recipient_kind,
          'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
          'context_kind', v_mail_row.context_kind,
          'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
          'reference', v_mail_row.reference,
          'payment_scope_json', v_mail_scope,
          'legacy_broad_match_allowed', true,
          'safe_rule', 'Whole-batch correction may cancel mail rows matched by pay_batch_id.'
        );
      END IF;

      RETURN jsonb_build_object(
        'matched', true,
        'match_kind', 'LEGACY_BROAD',
        'match_confidence', 'LEGACY_BROAD',
        'safe_to_cancel', false,
        'requires_review', true,
        'reason', 'MAIL_MATCHES_BATCH_ONLY_BY_LEGACY_REFERENCE',
        'mail_outbox_id', p_mail_outbox_id,
        'pay_batch_id', p_pay_batch_id,
        'mail_status', v_mail_row.status::text,
        'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false)
      );
    END IF;

    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_ROW_NOT_IN_PAY_BATCH_SCOPE',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_context_kind', v_mail_row.context_kind,
      'mail_context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END
    );
  END IF;

  IF v_is_whole_batch_scope THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'WHOLE_BATCH',
      'match_confidence', 'EXACT',
      'safe_to_cancel', true,
      'requires_review', false,
      'reason', 'WHOLE_BATCH_SCOPE_MATCH',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'payment_scope_json', v_mail_scope
    );
  END IF;

  v_item_exact := (
    COALESCE(array_length(v_mail_pay_batch_item_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    AND v_mail_pay_batch_item_ids <@ v_selected_pay_batch_item_ids
  );

  v_item_partial := (
    COALESCE(array_length(v_mail_pay_batch_item_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    AND v_mail_pay_batch_item_ids && v_selected_pay_batch_item_ids
    AND v_item_exact = false
  );

  v_transfer_exact := (
    COALESCE(array_length(v_mail_pay_bank_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    AND v_mail_pay_bank_transfer_ids <@ v_selected_pay_bank_transfer_ids
  );

  v_transfer_partial := (
    COALESCE(array_length(v_mail_pay_bank_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    AND v_mail_pay_bank_transfer_ids && v_selected_pay_bank_transfer_ids
    AND v_transfer_exact = false
  );

  v_payout_transfer_exact := (
    COALESCE(array_length(v_mail_payout_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
    AND v_mail_payout_transfer_ids <@ v_selected_payout_transfer_ids
  );

  v_payout_transfer_partial := (
    COALESCE(array_length(v_mail_payout_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
    AND v_mail_payout_transfer_ids && v_selected_payout_transfer_ids
    AND v_payout_transfer_exact = false
  );

  v_transfer_group_exact := (
    COALESCE(array_length(v_mail_transfer_group_keys, 1), 0) > 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    AND v_mail_transfer_group_keys <@ v_selected_transfer_group_keys
  );

  v_transfer_group_partial := (
    COALESCE(array_length(v_mail_transfer_group_keys, 1), 0) > 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    AND v_mail_transfer_group_keys && v_selected_transfer_group_keys
    AND v_transfer_group_exact = false
  );

  v_finance_case_exact := (
    COALESCE(array_length(v_mail_finance_case_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    AND v_mail_finance_case_ids <@ v_selected_finance_case_ids
  );

  v_finance_case_partial := (
    COALESCE(array_length(v_mail_finance_case_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    AND v_mail_finance_case_ids && v_selected_finance_case_ids
    AND v_finance_case_exact = false
  );

  v_finance_component_exact := (
    COALESCE(array_length(v_mail_finance_component_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    AND v_mail_finance_component_ids <@ v_selected_finance_component_ids
  );

  v_finance_component_partial := (
    COALESCE(array_length(v_mail_finance_component_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    AND v_mail_finance_component_ids && v_selected_finance_component_ids
    AND v_finance_component_exact = false
  );

  v_reservation_exact := (
    COALESCE(array_length(v_mail_reservation_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    AND v_mail_reservation_ids <@ v_selected_reservation_ids
  );

  v_reservation_partial := (
    COALESCE(array_length(v_mail_reservation_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    AND v_mail_reservation_ids && v_selected_reservation_ids
    AND v_reservation_exact = false
  );

  v_pay_batch_candidate_exact := (
    v_selected_candidate_scope_complete
    AND COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    AND v_mail_pay_batch_candidate_ids <@ v_selected_pay_batch_candidate_ids
  );

  v_pay_batch_candidate_partial := (
    COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    AND v_pay_batch_candidate_exact = false
  );

  v_umbrella_group_exact := (
    COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
    AND v_mail_umbrella_ids <@ v_selected_umbrella_ids
    AND (
      v_transfer_group_exact
      OR v_transfer_exact
      OR v_item_exact
      OR v_payout_transfer_exact
      OR v_finance_case_exact
      OR v_finance_component_exact
      OR v_reservation_exact
    )
  );

  v_umbrella_partial := (
    COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
    AND v_mail_umbrella_ids && v_selected_umbrella_ids
    AND v_umbrella_group_exact = false
  );

  v_context_exact := (
    (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item')
      AND v_mail_row.context_id = ANY(v_selected_pay_batch_item_ids)
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer')
      AND v_mail_row.context_id = ANY(v_selected_pay_bank_transfer_ids)
      AND v_transfer_scope_safe
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advances', 'pay_advance', 'finance_case', 'finance_cases')
      AND v_mail_row.context_id = ANY(v_selected_finance_case_ids)
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations')
      AND v_mail_row.context_id = ANY(v_selected_reservation_ids)
    )
    OR (
      v_selected_candidate_scope_complete
      AND lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate')
      AND v_mail_row.context_id = ANY(v_selected_pay_batch_candidate_ids)
    )
  );

  IF v_item_exact THEN
    v_match_kind := 'PAY_BATCH_ITEM';
    v_reason := 'PAY_BATCH_ITEM_SCOPE_MATCH';
  ELSIF v_transfer_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'TRANSFER_SCOPE_MATCH';
  ELSIF v_payout_transfer_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'PAYOUT_TRANSFER_SCOPE_MATCH';
  ELSIF v_umbrella_group_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'UMBRELLA_GROUP';
    v_reason := 'UMBRELLA_GROUP_SCOPE_MATCH';
  ELSIF v_finance_component_exact THEN
    v_match_kind := 'FINANCE_CASE';
    v_reason := 'FINANCE_COMPONENT_SCOPE_MATCH';
  ELSIF v_reservation_exact THEN
    v_match_kind := 'RESERVATION';
    v_reason := 'RESERVATION_SCOPE_MATCH';
  ELSIF v_finance_case_exact THEN
    v_match_kind := 'FINANCE_CASE';
    v_reason := 'FINANCE_CASE_SCOPE_MATCH';
  ELSIF v_transfer_group_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'TRANSFER_GROUP_SCOPE_MATCH';
  ELSIF v_pay_batch_candidate_exact THEN
    v_match_kind := 'PAY_BATCH_CANDIDATE';
    v_reason := 'COMPLETE_PAY_BATCH_CANDIDATE_SCOPE_MATCH';
  ELSIF v_context_exact THEN
    v_match_kind := CASE
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item') THEN 'PAY_BATCH_ITEM'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer') THEN 'TRANSFER'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate') THEN 'PAY_BATCH_CANDIDATE'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations') THEN 'RESERVATION'
      ELSE 'FINANCE_CASE'
    END;
    v_reason := 'CONTEXT_SCOPE_MATCH';
  END IF;

  v_partial_overlap_requires_review := (
    v_item_partial
    OR v_transfer_partial
    OR (v_transfer_exact AND v_transfer_scope_safe = false)
    OR v_payout_transfer_partial
    OR (v_payout_transfer_exact AND v_transfer_scope_safe = false)
    OR v_transfer_group_partial
    OR (v_transfer_group_exact AND v_transfer_scope_safe = false)
    OR (v_umbrella_group_exact AND v_transfer_scope_safe = false)
    OR v_finance_case_partial
    OR v_finance_component_partial
    OR v_reservation_partial
    OR (
      v_pay_batch_candidate_partial
      AND NOT (
        v_item_exact
        OR v_transfer_exact
        OR v_payout_transfer_exact
        OR v_transfer_group_exact
        OR v_finance_case_exact
        OR v_finance_component_exact
        OR v_reservation_exact
      )
    )
    OR (
      v_umbrella_partial
      AND NOT (
        v_item_exact
        OR v_transfer_exact
        OR v_payout_transfer_exact
        OR v_transfer_group_exact
        OR v_finance_case_exact
        OR v_finance_component_exact
        OR v_reservation_exact
      )
    )
  );

  IF v_match_kind <> 'NONE' AND v_partial_overlap_requires_review THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'LEGACY_BROAD',
      'match_confidence', 'LEGACY_BROAD',
      'safe_to_cancel', false,
      'requires_review', true,
      'reason', 'MAIL_SCOPE_PARTIAL_OVERLAP_REQUIRES_REVIEW',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false),
      'partial_overlap', true,
      'would_otherwise_match_kind', v_match_kind,
      'would_otherwise_match_reason', v_reason,
      'safe_rule', 'Do not cancel selected-scope mail rows when a populated scope dimension only partially overlaps the selected scope.'
    );
  END IF;

  IF v_match_kind <> 'NONE' THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', v_match_kind,
      'match_confidence', 'EXACT',
      'safe_to_cancel', true,
      'requires_review', false,
      'reason', v_reason,
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'selected_scope_summary', jsonb_build_object(
        'scope_type', v_scope_type,
        'pay_batch_item_count', COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0),
        'pay_bank_transfer_count', COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0),
        'pay_batch_candidate_count', COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0),
        'finance_case_count', COALESCE(array_length(v_selected_finance_case_ids, 1), 0),
        'finance_component_count', COALESCE(array_length(v_selected_finance_component_ids, 1), 0),
        'reservation_count', COALESCE(array_length(v_selected_reservation_ids, 1), 0),
        'payout_transfer_count', COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0),
        'transfer_group_count', COALESCE(array_length(v_selected_transfer_group_keys, 1), 0)
      )
    );
  END IF;

  v_partial_overlap_requires_review := (
    v_item_partial
    OR v_transfer_partial
    OR (v_transfer_exact AND v_transfer_scope_safe = false)
    OR v_payout_transfer_partial
    OR (v_payout_transfer_exact AND v_transfer_scope_safe = false)
    OR v_transfer_group_partial
    OR (v_transfer_group_exact AND v_transfer_scope_safe = false)
    OR (v_umbrella_group_exact AND v_transfer_scope_safe = false)
    OR v_finance_case_partial
    OR v_finance_component_partial
    OR v_reservation_partial
    OR v_pay_batch_candidate_partial
    OR v_umbrella_partial
  );

  SELECT EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_batch_item_ids) AS selected_reference_item_ids(item_id)
    WHERE selected_reference_item_ids.item_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_item_ids.item_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_bank_transfer_ids) AS selected_reference_transfer_ids(transfer_id)
    WHERE selected_reference_transfer_ids.transfer_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_transfer_ids.transfer_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_batch_candidate_ids) AS selected_reference_candidate_ids(pay_batch_candidate_id)
    WHERE selected_reference_candidate_ids.pay_batch_candidate_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_candidate_ids.pay_batch_candidate_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_candidate_ids) AS selected_reference_candidates(candidate_id)
    WHERE selected_reference_candidates.candidate_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_candidates.candidate_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_umbrella_ids) AS selected_reference_umbrellas(umbrella_id)
    WHERE selected_reference_umbrellas.umbrella_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_umbrellas.umbrella_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_finance_case_ids) AS selected_reference_finance_cases(finance_case_id)
    WHERE selected_reference_finance_cases.finance_case_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_finance_cases.finance_case_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_finance_component_ids) AS selected_reference_finance_components(finance_component_id)
    WHERE selected_reference_finance_components.finance_component_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_finance_components.finance_component_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_reservation_ids) AS selected_reference_reservations(reservation_id)
    WHERE selected_reference_reservations.reservation_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_reservations.reservation_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_transfer_group_keys) AS selected_reference_groups(transfer_group_key)
    WHERE NULLIF(selected_reference_groups.transfer_group_key, '') IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_groups.transfer_group_key || '%'
  )
  INTO v_reference_broad_match;

  v_recipient_broad_match := (
    (
      upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'CANDIDATE'
      AND v_mail_row.recipient_id IS NOT NULL
      AND (
        v_mail_row.recipient_id = ANY(v_selected_candidate_ids)
      )
    )
    OR (
      upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'UMBRELLA'
      AND v_mail_row.recipient_id IS NOT NULL
      AND (
        v_mail_row.recipient_id = ANY(v_selected_umbrella_ids)
      )
    )
  );

  v_payment_scope_broad_match := (
    (
      COALESCE(array_length(v_mail_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND v_mail_candidate_ids && v_selected_candidate_ids
    )
    OR (
      COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND v_mail_umbrella_ids && v_selected_umbrella_ids
    )
    OR (
      COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    )
  );

  v_legacy_broad_match := (
    v_partial_overlap_requires_review
    OR v_reference_broad_match
    OR v_recipient_broad_match
    OR v_payment_scope_broad_match
  );

  IF v_legacy_broad_match THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'LEGACY_BROAD',
      'match_confidence', 'LEGACY_BROAD',
      'safe_to_cancel', false,
      'requires_review', true,
      'reason', CASE
        WHEN v_partial_overlap_requires_review THEN 'MAIL_SCOPE_PARTIAL_OVERLAP_REQUIRES_REVIEW'
        WHEN v_reference_broad_match THEN 'MAIL_SCOPE_MATCHES_LEGACY_REFERENCE_ONLY'
        WHEN v_recipient_broad_match THEN 'MAIL_SCOPE_MATCHES_RECIPIENT_ONLY'
        ELSE 'MAIL_SCOPE_MATCHES_BROAD_PAYMENT_SCOPE_ONLY'
      END,
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false),
      'partial_overlap', v_partial_overlap_requires_review,
      'safe_rule', 'Do not cancel selected-scope mail rows unless safe_to_cancel is true.'
    );
  END IF;

  RETURN jsonb_build_object(
    'matched', false,
    'match_kind', 'NONE',
    'match_confidence', 'NONE',
    'safe_to_cancel', false,
    'requires_review', false,
    'reason', 'NO_EXACT_OR_LEGACY_SCOPE_MATCH',
    'mail_outbox_id', p_mail_outbox_id,
    'pay_batch_id', p_pay_batch_id,
    'mail_status', v_mail_row.status::text,
    'mail_type', v_mail_row.type,
    'recipient_kind', v_mail_row.recipient_kind,
    'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
    'context_kind', v_mail_row.context_kind,
    'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
    'reference', v_mail_row.reference,
    'payment_scope_json', v_mail_scope
  );
END;
$function$;

