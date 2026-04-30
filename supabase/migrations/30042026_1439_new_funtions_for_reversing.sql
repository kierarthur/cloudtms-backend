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
  v_umbrella_id uuid;
  v_umbrella_id_text text;
  v_transfer_group_key text;
  v_return_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_item_count integer := 0;
  v_selected_already_corrected_count integer := 0;
  v_return_item_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_returned_count integer := 0;
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
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_ID_REQUIRED'
            )::text;
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
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_CANDIDATE_IDS_MUST_BE_ARRAY',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF p_selection_json ? 'pay_bank_transfer_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF EXISTS (
    WITH raw_candidate_values AS (
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
    cleaned_candidate_values AS (
      SELECT nullif(btrim(raw_candidate_values.raw_value), '') AS clean_value
      FROM raw_candidate_values
    )
    SELECT 1
    FROM cleaned_candidate_values
    WHERE cleaned_candidate_values.clean_value IS NOT NULL
      AND cleaned_candidate_values.clean_value !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BATCH_CANDIDATE_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'INVALID_PAY_BATCH_CANDIDATE_ID',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  WITH raw_candidate_values AS (
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
  cleaned_candidate_values AS (
    SELECT nullif(btrim(raw_candidate_values.raw_value), '') AS clean_value
    FROM raw_candidate_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_candidate_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_candidate_ids
  FROM cleaned_candidate_values
  WHERE cleaned_candidate_values.clean_value IS NOT NULL;

  IF EXISTS (
    WITH raw_transfer_values AS (
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
    cleaned_transfer_values AS (
      SELECT nullif(btrim(raw_transfer_values.raw_value), '') AS clean_value
      FROM raw_transfer_values
    )
    SELECT 1
    FROM cleaned_transfer_values
    WHERE cleaned_transfer_values.clean_value IS NOT NULL
      AND cleaned_transfer_values.clean_value !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BANK_TRANSFER_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'INVALID_PAY_BANK_TRANSFER_ID',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  WITH raw_transfer_values AS (
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
  cleaned_transfer_values AS (
    SELECT nullif(btrim(raw_transfer_values.raw_value), '') AS clean_value
    FROM raw_transfer_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_transfer_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM cleaned_transfer_values
  WHERE cleaned_transfer_values.clean_value IS NOT NULL;

  v_umbrella_id_text := nullif(btrim(coalesce(p_selection_json->>'umbrella_id', '')), '');
  v_transfer_group_key := nullif(btrim(coalesce(p_selection_json->>'transfer_group_key', '')), '');

  IF v_umbrella_id_text IS NOT NULL THEN
    IF v_umbrella_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_UMBRELLA_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'INVALID_UMBRELLA_ID',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type
              )::text;
    END IF;

    v_umbrella_id := v_umbrella_id_text::uuid;
  END IF;

  IF v_scope_type = 'CANDIDATES'
     AND COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF v_scope_type = 'TRANSFER'
     AND COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BANK_TRANSFER_SELECTION_REQUIRED',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
     AND v_umbrella_id IS NULL THEN
    RAISE EXCEPTION 'UMBRELLA_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UMBRELLA_SELECTION_REQUIRED',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
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
  raw_selected_items AS (
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
            COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
            OR public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
          )
          AND (
            v_transfer_group_key IS NULL
            OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
          )
        )
      )
  ),
  decorated_selected_items AS (
    SELECT
      raw_selected_items.selected_pay_batch_item_id AS selected_pay_batch_item_id,
      COALESCE(array_length(applied_corrections.applied_correction_kinds, 1), 0) > 0 AS selected_item_already_corrected
    FROM raw_selected_items
    LEFT JOIN applied_corrections
      ON applied_corrections.correction_pay_batch_item_id = raw_selected_items.selected_pay_batch_item_id
  )
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
  INTO
    v_return_item_ids,
    v_selected_item_count,
    v_selected_already_corrected_count
  FROM decorated_selected_items;

  v_return_item_count := COALESCE(array_length(v_return_item_ids, 1), 0);

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_RESOLVED_SCOPE',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_scope_type,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'pay_batch_candidate_id_count', COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0),
      'pay_bank_transfer_id_count', COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0),
      'umbrella_id', v_umbrella_id,
      'transfer_group_key', v_transfer_group_key,
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
    selected_batch_rows.selected_amount_inc_vat AS amount_inc_vat,
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
      'key_resolution_failure_count', v_key_resolution_failure_count
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
  v_result jsonb;
BEGIN
  v_selection_scope_type := upper(nullif(btrim(coalesce(p_selection_json->>'scope_type', '')), ''));

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_MOVEMENT_CLASSIFY_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_selection_scope_type,
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
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) = 'COMPLETED' OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) = 'COMPLETED'))::integer,
    (count(*) FILTER (WHERE public.pay_bank_transfers.completed_at_utc IS NOT NULL))::integer,
    (count(*) FILTER (WHERE nullif(btrim(coalesce(public.pay_bank_transfers.rail_tx_id, '')), '') IS NOT NULL))::integer,
    (count(*) FILTER (WHERE nullif(btrim(coalesce(public.pay_bank_transfers.request_id, '')), '') IS NOT NULL))::integer,
    (count(*) FILTER (WHERE public.pay_bank_transfers.rail_meta_json IS NOT NULL AND public.pay_bank_transfers.rail_meta_json <> '{}'::jsonb))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED') OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'FAILED_BEFORE_COMMIT', 'SUBMISSION_FAILED') OR nullif(btrim(coalesce(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'WAITING_BANK_CONFIRM') OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'WAITING_BANK_CONFIRM')))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) = 'UNKNOWN' OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) = 'UNKNOWN'))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT') OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT')))::integer,
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfers.status, '')) IN ('RETURNED', 'REVERTED') OR upper(coalesce(public.pay_bank_transfers.rail_state, '')) IN ('RETURNED', 'REVERTED')))::integer
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
    (count(*) FILTER (WHERE upper(coalesce(public.pay_bank_transfer_events.mapping_status, '')) IN ('AMBIGUOUS', 'UNMATCHED', 'LEGACY_NO_ARTIFACT')))::integer,
    (count(*) FILTER (
      WHERE public.pay_bank_transfer_events.amount IS NOT NULL
        AND public.pay_bank_transfer_events.pay_bank_transfer_id IS NOT NULL
        AND public.pay_bank_transfers.id IS NOT NULL
        AND abs(COALESCE(public.pay_bank_transfer_events.amount, 0) - COALESCE(public.pay_bank_transfers.amount, 0)) > 0.01
    ))::integer,
    (count(*) FILTER (WHERE nullif(btrim(coalesce(public.pay_bank_transfer_events.provider_reference, '')), '') IS NOT NULL OR nullif(btrim(coalesce(public.pay_bank_transfer_events.provider_event_id, '')), '') IS NOT NULL))::integer
  INTO
    v_bank_event_count,
    v_event_submitted_count,
    v_event_completed_count,
    v_event_terminal_no_money_count,
    v_event_returned_count,
    v_event_pending_count,
    v_event_unknown_count,
    v_event_ambiguous_mapping_count,
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
    v_transfer_pending_count > 0
    OR v_transfer_unknown_count > 0
    OR v_transfer_timeout_count > 0
    OR v_event_pending_count > 0
    OR v_event_unknown_count > 0;

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

  IF v_has_terminal_no_money_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('TERMINAL_NO_MONEY_FAILURE_EVIDENCE_PRESENT');
  END IF;

  IF NOT v_has_submission_evidence THEN
    v_reasons := v_reasons || jsonb_build_array('NO_BANK_SUBMISSION_EVIDENCE_FOUND');
  END IF;

  IF jsonb_array_length(v_blockers) > 0 THEN
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
  ELSIF v_has_settlement_evidence THEN
    v_classification := 'TRUE_SETTLED_REVERSAL_REQUIRED';
  ELSIF v_has_terminal_no_money_evidence THEN
    v_classification := 'NO_MONEY_UNWIND';
  ELSIF NOT v_has_submission_evidence THEN
    v_classification := 'PRE_BANK_CANCEL';
  ELSE
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SUBMITTED_BUT_NOT_TERMINAL_OR_SETTLED',
      'message', 'Selected scope has submission evidence but no clear terminal failure or settlement evidence.'
    ));
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
    AND v_voided_item_count = 0;

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
    'candidate_settled_count', v_candidate_settled_count,
    'candidate_settled_at_count', v_candidate_settled_at_count,
    'timesheet_pay_state_history_count', v_timesheet_history_count,
    'reservation_settled_count', v_reservation_settled_count,
    'reservation_settled_at_count', v_reservation_settled_at_count,
    'payout_paid_count', v_payout_paid_count,
    'bank_event_count', v_bank_event_count,
    'bank_event_ambiguous_mapping_count', v_event_ambiguous_mapping_count,
    'bank_event_amount_mismatch_count', v_event_amount_mismatch_count,
    'aggregate_subset_issue_count', v_aggregate_subset_issue_count
  );

  v_selected_amounts := jsonb_build_object(
    'amount_ex_vat', v_selected_amount_ex_vat,
    'amount_vat', v_selected_amount_vat,
    'amount_inc_vat', v_selected_amount_inc_vat
  );

  v_evidence := jsonb_build_object(
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
BEGIN
  v_subject_id := COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID');

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
    public.pay_batches.created_at_utc,
    public.pay_batches.execution_commit_state,
    public.pay_batches.execution_commit_ref
  INTO
    v_batch_id,
    v_batch_status,
    v_batch_pay_date,
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
    v_suggested_resolution := jsonb_build_object(
      'required', true,
      'reason', 'Gross/taxable/channel-sensitive finance items are present and must use the existing suggested-resolution process before correction apply.',
      'must_be_accepted_before_apply', true,
      'must_be_applied_atomically_with_correction', true,
      'callable_surface', jsonb_build_array(
        'pay_finance_case_taxable_channel_restructure_suggestion',
        'pay_finance_case_apply_taxable_channel_restructure',
        'pay_manual_debt_adjustment_resolve_taxable_channel_change',
        'pay_finance_component_resolutions_apply'
      ),
      'finance_cases', v_affected_finance_cases
    );

    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SUGGESTED_RESOLUTION_REQUIRED',
      'message', 'Gross/taxable/channel-sensitive finance items require an accepted suggested resolution before this correction request can be started.',
      'affected_item_count', v_gross_channel_sensitive_item_count
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

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;
  CREATE TEMP TABLE _tmp_payment_correction_plan_mail ON COMMIT DROP AS
  WITH selected_references AS (
    SELECT p_pay_batch_id::text AS reference_value
    UNION
    SELECT DISTINCT plan_detail.pay_bank_transfer_id::text
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
    UNION
    SELECT DISTINCT plan_detail.pay_batch_candidate_id::text
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.pay_batch_candidate_id IS NOT NULL
  ),
  selected_recipient_ids AS (
    SELECT DISTINCT plan_detail.candidate_id AS recipient_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.candidate_id IS NOT NULL
    UNION
    SELECT DISTINCT plan_detail.umbrella_id AS recipient_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.umbrella_id IS NOT NULL
  )
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
    public.mail_outbox.email_type
  FROM public.mail_outbox
  WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) IN ('QUEUED', 'SENT')
    AND (
      public.mail_outbox.context_id = p_pay_batch_id
      OR public.mail_outbox.context_id IN (
        SELECT plan_detail.pay_bank_transfer_id
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
      )
      OR public.mail_outbox.recipient_id IN (
        SELECT selected_recipient_ids.recipient_id
        FROM selected_recipient_ids
      )
      OR EXISTS (
        SELECT 1
        FROM selected_references
        WHERE public.mail_outbox.reference ILIKE '%' || selected_references.reference_value || '%'
      )
    )
    AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference)) LIKE ANY (
      ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
    );

  SELECT
    count(*) FILTER (WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED')::integer,
    count(*) FILTER (WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT')::integer
  INTO v_queued_unsent_count, v_sent_notice_count
  FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail;

  v_communication_effects := jsonb_build_object(
    'queued_unsent_to_cancel', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
    ), '[]'::jsonb),
    'sent_to_leave_as_audit', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'sent_at', plan_mail.sent_at
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT'
    ), '[]'::jsonb),
    'external_correction_notice', false,
    'admin_notice_required', true,
    'queued_unsent_count', v_queued_unsent_count,
    'sent_notice_count', v_sent_notice_count
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
    'requires_work_queue', COALESCE(v_work_item_count, 0) > v_large_correction_threshold
  );

  v_selection_summary := jsonb_build_object(
    'scope_type', upper(nullif(btrim(COALESCE(p_selection_json->>'scope_type', '')), '')),
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

CREATE OR REPLACE FUNCTION public.pay_payment_correction_request_start(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_reason text,
  p_actor_user_id uuid,
  p_source_bank_event_id uuid DEFAULT NULL::uuid,
  p_auto_requested boolean DEFAULT false,
  p_accepted_resolution_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
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
      public.pay_bank_transfer_events.mapping_status
    INTO
      v_source_event_exists,
      v_source_event_source,
      v_source_event_mapping_status
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

    IF COALESCE(upper(v_source_event_mapping_status), '') <> 'MATCHED' THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_REQUIRES_EXACT_BANK_EVENT_MAPPING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_REQUIRES_EXACT_BANK_EVENT_MAPPING',
                'pay_batch_id', p_pay_batch_id,
                'source_bank_event_id', p_source_bank_event_id,
                'mapping_status', v_source_event_mapping_status
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

  v_selection_hash := md5(p_selection_json::text);

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
      'accepted_resolution_hash', v_accepted_resolution_hash
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
    'accepted_resolution_hash', v_accepted_resolution_hash
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

  IF v_fresh_plan_hash <> v_request.plan_hash THEN
    v_block_reason := 'PLAN_STALE';
  ELSIF v_accepted_resolution_is_stale THEN
    v_block_reason := 'ACCEPTED_RESOLUTION_STALE';
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
        'fresh_can_apply', v_fresh_plan_can_apply
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
      'expand_result', v_expand_result
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
      v_request.selection_json || jsonb_build_object('work_unit', 'BATCH'),
      md5((v_request.selection_json || jsonb_build_object('work_unit', 'BATCH'))::text),
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
        'selected_item_count', v_selected_item_count
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
        'work_unit', 'TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'item_count', transfer_work.item_count,
        'candidate_count', transfer_work.candidate_count,
        'amount_inc_vat', transfer_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'scope_type', 'TRANSFER',
        'pay_bank_transfer_ids', jsonb_build_array(transfer_work.pay_bank_transfer_id::text),
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
        'pay_bank_transfer_ids', finance_work.pay_bank_transfer_ids,
        'work_unit', 'FINANCE_CASE',
        'source_correction_request_id', p_correction_request_id::text,
        'item_count', finance_work.item_count,
        'candidate_count', finance_work.candidate_count,
        'amount_inc_vat', finance_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'scope_type', 'FINANCE_CASE',
        'finance_case_id', finance_work.finance_case_id::text,
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
        COALESCE(jsonb_agg(DISTINCT expand_selected.pay_batch_candidate_id::text) FILTER (WHERE expand_selected.pay_batch_candidate_id IS NOT NULL), '[]'::jsonb) AS pay_batch_candidate_ids,
        COALESCE(jsonb_agg(DISTINCT expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb) AS pay_bank_transfer_ids,
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
        'work_unit', 'CANDIDATE_TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'item_count', candidate_transfer_work.item_count,
        'amount_inc_vat', candidate_transfer_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'scope_type', 'CANDIDATE_TRANSFER',
        'pay_batch_candidate_id', candidate_transfer_work.pay_batch_candidate_id::text,
        'pay_bank_transfer_id', candidate_transfer_work.pay_bank_transfer_id::text,
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
        'work_unit', 'CANDIDATE',
        'source_correction_request_id', p_correction_request_id::text,
        'item_count', candidate_work.item_count,
        'amount_inc_vat', candidate_work.amount_inc_vat
      ),
      md5(jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_work.pay_batch_candidate_id::text),
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
        COALESCE(jsonb_agg(DISTINCT expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb) AS pay_bank_transfer_ids,
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


CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk(
  p_correction_request_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 50,
  p_worker_id text DEFAULT NULL::text
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
BEGIN
  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_PROCESS_CHUNK_START',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'requested_limit', p_limit,
      'effective_limit', v_limit,
      'worker_id', v_worker_id
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

      RETURN jsonb_build_object(
        'ok', true,
        'processed', 0,
        'applied', 0,
        'blocked', 0,
        'failed_retryable', 0,
        'failed_final', 0,
        'parent_status', v_request_status,
        'totals', v_totals,
        'terminal_request', true
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
      result_json = COALESCE(work_to_claim.result_json, '{}'::jsonb) || jsonb_build_object(
        'claimed_at_utc', v_now,
        'claimed_by', v_worker_id,
        'previous_status', work_to_claim.status
      )
    FROM claimable_work
    WHERE work_to_claim.id = claimable_work.id
    RETURNING
      work_to_claim.id,
      work_to_claim.correction_request_id,
      work_to_claim.pay_batch_id,
      work_to_claim.work_kind,
      work_to_claim.attempt_count
  )
  SELECT
    updated_work.id,
    updated_work.correction_request_id,
    updated_work.pay_batch_id,
    updated_work.work_kind,
    updated_work.attempt_count
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

    PERFORM public._imp_debug_audit(
      NULL::uuid,
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
      'totals', v_totals
    );
  END IF;

  FOR v_work_row IN
    SELECT
      claimed_work.id,
      claimed_work.correction_request_id,
      claimed_work.pay_batch_id,
      claimed_work.work_kind,
      claimed_work.attempt_count
    FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work
    ORDER BY claimed_work.id
  LOOP
    v_processed_count := v_processed_count + 1;
    v_result := '{}'::jsonb;
    v_result_ok := false;
    v_result_status := NULL;

    BEGIN
      IF v_work_row.work_kind = 'PRE_BANK_CANCEL' THEN
        v_result := public.pay_pre_bank_cancel_apply_work_item(v_work_row.id, NULL::uuid);
      ELSIF v_work_row.work_kind = 'NO_MONEY_UNWIND' THEN
        v_result := public.pay_no_money_unwind_apply_work_item(v_work_row.id, NULL::uuid);
      ELSIF v_work_row.work_kind = 'SETTLED_REVERSAL' THEN
        v_result := public.pay_settled_payment_reversal_apply_work_item(v_work_row.id, NULL::uuid);
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
          result_json = COALESCE(processed_work_success.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
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
          result_json = COALESCE(processed_work_skipped.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
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
          result_json = COALESCE(failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', v_failure_status,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM,
            'failed_at_utc', now(),
            'processed_worker_id', v_worker_id,
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
          NULL::uuid,
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

  PERFORM public._imp_debug_audit(
    NULL::uuid,
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
    'totals', v_totals
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_CORRECTION_PROCESS_CHUNK_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'limit', p_limit,
        'worker_id', p_worker_id,
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

  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (reservation_id);
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
    v_work_item.selection_json
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

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  WHERE queued_mail_to_cancel.status::text = 'QUEUED'
    AND (
      queued_mail_to_cancel.context_id = v_work_item.pay_batch_id
      OR queued_mail_to_cancel.context_id IN (
        SELECT DISTINCT selected_mail_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_mail_transfers
        WHERE selected_mail_transfers.pay_bank_transfer_id IS NOT NULL
      )
      OR queued_mail_to_cancel.recipient_id IN (
        SELECT DISTINCT selected_mail_candidates.candidate_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_mail_candidates
        WHERE selected_mail_candidates.candidate_id IS NOT NULL
        UNION
        SELECT DISTINCT selected_mail_umbrellas.umbrella_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_mail_umbrellas
        WHERE selected_mail_umbrellas.umbrella_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_mail_references
        WHERE queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_item_id::text || '%'
           OR queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_candidate_id::text || '%'
           OR (
             selected_mail_references.pay_bank_transfer_id IS NOT NULL
             AND queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_bank_transfer_id::text || '%'
           )
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

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
        THEN 'BLOCKED'
      ELSE transfer_to_recalculate.status
    END,
    failed_reason = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.failed_reason, 'PRE_BANK_CANCEL')
      ELSE transfer_to_recalculate.failed_reason
    END,
    rail_meta_json = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'pre_bank_cancel_applied', true,
          'pre_bank_cancel_work_item_id', p_work_item_id::text,
          'pre_bank_cancel_at_utc', v_now,
          'pre_bank_cancel_status_note', 'Transfer amount became zero after selected pre-bank cancellation; status set to BLOCKED because pay_bank_transfers.status does not support CANCELLED/VOIDED in the current DB contract.'
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
    v_work_item.selection_json
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

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  WHERE queued_mail_to_cancel.status::text = 'QUEUED'
    AND (
      queued_mail_to_cancel.context_id = v_work_item.pay_batch_id
      OR queued_mail_to_cancel.context_id IN (
        SELECT DISTINCT selected_mail_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_mail_transfers
        WHERE selected_mail_transfers.pay_bank_transfer_id IS NOT NULL
      )
      OR queued_mail_to_cancel.recipient_id IN (
        SELECT DISTINCT selected_mail_candidates.candidate_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_mail_candidates
        WHERE selected_mail_candidates.candidate_id IS NOT NULL
        UNION
        SELECT DISTINCT selected_mail_umbrellas.umbrella_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_mail_umbrellas
        WHERE selected_mail_umbrellas.umbrella_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_mail_references
        WHERE queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_item_id::text || '%'
           OR queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_candidate_id::text || '%'
           OR (
             selected_mail_references.pay_bank_transfer_id IS NOT NULL
             AND queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_bank_transfer_id::text || '%'
           )
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

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

  SELECT COALESCE(public.settings_defaults.payment_return_admin_notice_quiet_minutes, 10),
         COALESCE(public.settings_defaults.payment_return_admin_notice_max_wait_minutes, 60)
  INTO v_quiet_minutes, v_max_wait_minutes
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_quiet_minutes := LEAST(GREATEST(COALESCE(v_quiet_minutes, 10), 0), 1440);
  v_max_wait_minutes := LEAST(GREATEST(COALESCE(v_max_wait_minutes, 60), v_quiet_minutes), 1440);

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
    v_work_item.pay_batch_id,
    v_batch.execution_commit_ref,
    COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
    'SYSTEM',
    'NO_MONEY_UNWIND_APPLIED',
    'OPEN',
    v_now + make_interval(mins => v_quiet_minutes),
    v_now + make_interval(mins => v_max_wait_minutes),
    jsonb_build_object(
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
      'applied_at_utc', v_now
    ),
    '[]'::jsonb,
    v_now,
    v_now,
    NULL::timestamptz
  )
  RETURNING public.pay_payment_return_notice_groups.id
  INTO v_notice_group_id;

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
    'dirty_candidate_count', v_dirty_candidate_count,
    'notice_group_id', v_notice_group_id,
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
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_ERROR',
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
    v_work_item.selection_json
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
    count(*) FILTER (WHERE finance_detail.finance_component_closed_at_utc IS NOT NULL OR finance_detail.finance_cleared_at_utc IS NOT NULL)::integer,
    count(*) FILTER (WHERE finance_detail.finance_component_is_resolution_stale IS TRUE)::integer
  INTO
    v_finance_written_off_count,
    v_finance_closed_count,
    v_finance_stale_count
  FROM pg_temp._tmp_settled_reversal_finance_detail AS finance_detail;

  IF v_finance_written_off_count > 0 OR v_finance_closed_count > 0 OR v_finance_stale_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'FINANCE_STATE_BLOCKS_SETTLED_REVERSAL',
      'message', 'One or more selected finance cases/components are written off, closed, cleared, or stale and require manual finance review.',
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

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  WHERE queued_mail_to_cancel.status::text = 'QUEUED'
    AND (
      queued_mail_to_cancel.context_id = v_work_item.pay_batch_id
      OR queued_mail_to_cancel.context_id IN (
        SELECT DISTINCT selected_mail_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_settled_reversal_selected AS selected_mail_transfers
        WHERE selected_mail_transfers.pay_bank_transfer_id IS NOT NULL
      )
      OR queued_mail_to_cancel.recipient_id IN (
        SELECT DISTINCT selected_mail_candidates.candidate_id
        FROM pg_temp._tmp_settled_reversal_selected AS selected_mail_candidates
        WHERE selected_mail_candidates.candidate_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp._tmp_settled_reversal_selected AS selected_mail_references
        WHERE queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_item_id::text || '%'
           OR queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_batch_candidate_id::text || '%'
           OR (
             selected_mail_references.pay_bank_transfer_id IS NOT NULL
             AND queued_mail_to_cancel.reference ILIKE '%' || selected_mail_references.pay_bank_transfer_id::text || '%'
           )
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

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

  SELECT COALESCE(public.settings_defaults.payment_return_admin_notice_quiet_minutes, 10),
         COALESCE(public.settings_defaults.payment_return_admin_notice_max_wait_minutes, 60)
  INTO v_quiet_minutes, v_max_wait_minutes
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_quiet_minutes := LEAST(GREATEST(COALESCE(v_quiet_minutes, 10), 0), 1440);
  v_max_wait_minutes := LEAST(GREATEST(COALESCE(v_max_wait_minutes, 60), v_quiet_minutes), 1440);

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
    v_work_item.pay_batch_id,
    v_batch.execution_commit_ref,
    COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
    'SYSTEM',
    'SETTLED_REVERSAL_APPLIED',
    'OPEN',
    v_now + make_interval(mins => v_quiet_minutes),
    v_now + make_interval(mins => v_max_wait_minutes),
    jsonb_build_object(
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
      'timesheet_state_updated_count', v_timesheet_state_updated_count,
      'timesheet_state_deleted_count', v_timesheet_state_deleted_count,
      'applied_at_utc', v_now
    ),
    '[]'::jsonb,
    v_now,
    v_now,
    NULL::timestamptz
  )
  RETURNING public.pay_payment_return_notice_groups.id
  INTO v_notice_group_id;

  v_result := jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'work_item_id', p_work_item_id,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'SETTLED_REVERSAL',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'timesheet_state_updated_count', v_timesheet_state_updated_count,
    'timesheet_state_deleted_count', v_timesheet_state_deleted_count,
    'dirty_candidate_count', v_dirty_candidate_count,
    'notice_group_id', v_notice_group_id,
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
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_SETTLED_REVERSAL_WORK_ERROR',
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
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_input_timesheet_count integer := 0;
  v_candidate_item_count integer := 0;
  v_returned_row_count integer := 0;
BEGIN
  SELECT COALESCE(count(DISTINCT input_timesheet_ids.timesheet_id), 0)::integer
  INTO v_input_timesheet_count
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_ids(timesheet_id)
  WHERE input_timesheet_ids.timesheet_id IS NOT NULL;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAY_ACTIVE_SETTLED_COMPONENTS_START',
    jsonb_build_object(
      'input_timesheet_count', v_input_timesheet_count
    ),
    'pay_payment_correction',
    'active_settled_components',
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF v_input_timesheet_count = 0 THEN
    RETURN;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_active_settled_item_ids;
  CREATE TEMP TABLE _tmp_active_settled_item_ids ON COMMIT DROP AS
  WITH input_timesheets AS (
    SELECT DISTINCT input_timesheet_ids.timesheet_id
    FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_ids(timesheet_id)
    WHERE input_timesheet_ids.timesheet_id IS NOT NULL
  )
  SELECT DISTINCT
    public.pay_batch_items.id AS pay_batch_item_id
  FROM public.pay_batch_items
  JOIN public.pay_batch_candidates
    ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers
    ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
  JOIN input_timesheets
    ON input_timesheets.timesheet_id = public.pay_batch_items.timesheet_id
  WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
    AND upper(btrim(COALESCE(public.pay_batch_items.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND (
      upper(btrim(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
      OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
      OR upper(btrim(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
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
    );

  SELECT count(*)::integer
  INTO v_candidate_item_count
  FROM pg_temp._tmp_active_settled_item_ids AS active_item_ids;

  IF v_candidate_item_count = 0 THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAY_ACTIVE_SETTLED_COMPONENTS_RESULT',
      jsonb_build_object(
        'input_timesheet_count', v_input_timesheet_count,
        'candidate_item_count', 0,
        'returned_row_count', 0
      ),
      'pay_payment_correction',
      'active_settled_components',
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN;
  END IF;

  RETURN QUERY
  WITH active_components AS (
    SELECT
      economic_components.timesheet_id AS component_timesheet_id,
      economic_components.key_type AS component_key_type,
      economic_components.key_value AS component_key_value,
      economic_components.source_amount_ex_vat AS component_amount_ex_vat,
      public.pay_batch_items.amount_inc_vat AS component_amount_inc_vat
    FROM public._pay_batch_item_economic_components(
      NULL::uuid,
      (
        SELECT COALESCE(array_agg(active_item_ids.pay_batch_item_id ORDER BY active_item_ids.pay_batch_item_id), ARRAY[]::uuid[])
        FROM pg_temp._tmp_active_settled_item_ids AS active_item_ids
      )
    ) AS economic_components
    JOIN public.pay_batch_items
      ON public.pay_batch_items.id = economic_components.pay_batch_item_id
    WHERE economic_components.timesheet_id IS NOT NULL
      AND economic_components.key_type IS NOT NULL
      AND economic_components.key_value IS NOT NULL
      AND economic_components.key_resolution_failure_reason IS NULL
  )
  SELECT
    active_components.component_timesheet_id AS timesheet_id,
    active_components.component_key_type AS key_type,
    active_components.component_key_value AS key_value,
    round(COALESCE(sum(COALESCE(active_components.component_amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    round(COALESCE(sum(COALESCE(active_components.component_amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM active_components
  GROUP BY
    active_components.component_timesheet_id,
    active_components.component_key_type,
    active_components.component_key_value
  HAVING round(COALESCE(sum(COALESCE(active_components.component_amount_ex_vat, 0)), 0), 2) <> 0
      OR round(COALESCE(sum(COALESCE(active_components.component_amount_inc_vat, 0)), 0), 2) <> 0
  ORDER BY
    active_components.component_timesheet_id,
    active_components.component_key_type,
    active_components.component_key_value;

  GET DIAGNOSTICS v_returned_row_count = ROW_COUNT;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAY_ACTIVE_SETTLED_COMPONENTS_RESULT',
    jsonb_build_object(
      'input_timesheet_count', v_input_timesheet_count,
      'candidate_item_count', v_candidate_item_count,
      'returned_row_count', v_returned_row_count
    ),
    'pay_payment_correction',
    'active_settled_components',
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
      'PAY_ACTIVE_SETTLED_COMPONENTS_ERROR',
      jsonb_build_object(
        'input_timesheet_count', v_input_timesheet_count,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      'active_settled_components',
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
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
  v_notice_kind text := 'BANK_FAILURE_DETECTED';
  v_quiet_minutes integer := 10;
  v_max_wait_minutes integer := 60;
  v_exact_mapping_required_blocker jsonb := NULL::jsonb;
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
    WHEN v_normalised_state IN ('RETURNED', 'RETURN', 'REVERSED', 'REVERTED') THEN 'RETURNED'
    WHEN v_normalised_state IN ('SUBMITTED', 'SENT') THEN 'SUBMITTED'
    WHEN v_normalised_state IN ('PENDING') THEN 'PENDING'
    WHEN v_normalised_state IN ('PROCESSING') THEN 'PROCESSING'
    WHEN v_normalised_state IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT') THEN 'UNKNOWN'
    ELSE 'UNKNOWN'
  END;

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

    IF v_transfer.id IS NOT NULL THEN
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
      v_umbrella_id := COALESCE(v_umbrella_id, v_transfer.umbrella_id, CASE WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id ELSE NULL::uuid END);
    END IF;
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
      v_umbrella_id := COALESCE(v_umbrella_id, v_transfer.umbrella_id, CASE WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id ELSE NULL::uuid END);
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
      v_umbrella_id := COALESCE(v_umbrella_id, v_transfer.umbrella_id, CASE WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id ELSE NULL::uuid END);
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
  ELSIF v_mapping_candidate_count > 1 THEN
    v_mapping_status := 'AMBIGUOUS';
  ELSE
    v_mapping_status := 'UNMATCHED';
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
           public.pay_bank_transfer_events.movement_classification,
           public.pay_bank_transfer_events.correction_disposition,
           public.pay_bank_transfer_events.pay_bank_transfer_id,
           public.pay_bank_transfer_events.pay_batch_id,
           public.pay_bank_transfer_events.candidate_id,
           public.pay_bank_transfer_events.umbrella_id
    INTO v_event_id,
         v_mapping_status,
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
      'classification', COALESCE(v_classification, 'UNKNOWN'),
      'correction_disposition', COALESCE(v_correction_disposition, 'ALREADY_RECORDED'),
      'correction_request_id', NULL::uuid,
      'admin_notice_group_id', NULL::uuid
    );
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'TRANSFER',
      'pay_bank_transfer_ids', jsonb_build_array(v_pay_bank_transfer_id::text)
    );
  ELSIF v_candidate_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'CANDIDATES',
      'pay_batch_candidate_ids', COALESCE((
        SELECT jsonb_agg(candidate_scope_rows.id::text ORDER BY candidate_scope_rows.id::text)
        FROM public.pay_batch_candidates AS candidate_scope_rows
        WHERE candidate_scope_rows.pay_batch_id = v_pay_batch_id
          AND candidate_scope_rows.candidate_id = v_candidate_id
      ), '[]'::jsonb)
    );
  ELSE
    v_selection_json := jsonb_build_object('scope_type', 'BATCH');
  END IF;

  IF v_mapping_status = 'MATCHED' THEN
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
      'reasons', jsonb_build_array('BANK_EVENT_MAPPING_NOT_MATCHED'),
      'evidence', jsonb_build_object('mapping_status', v_mapping_status),
      'counts', '{}'::jsonb,
      'blockers', jsonb_build_array(jsonb_build_object(
        'code', 'BANK_EVENT_MAPPING_NOT_MATCHED',
        'message', 'Bank event could not be mapped exactly to a transfer.'
      )),
      'selected_amounts', '{}'::jsonb,
      'safe_to_auto_apply', false
    );
    v_safe_to_auto_apply := false;
  END IF;

  SELECT COALESCE(public.settings_defaults.payment_return_auto_reverse_timesheets, false),
         COALESCE(public.settings_defaults.payment_return_admin_notice_quiet_minutes, 10),
         COALESCE(public.settings_defaults.payment_return_admin_notice_max_wait_minutes, 60)
  INTO v_auto_setting, v_quiet_minutes, v_max_wait_minutes
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_quiet_minutes := LEAST(GREATEST(COALESCE(v_quiet_minutes, 10), 0), 1440);
  v_max_wait_minutes := LEAST(GREATEST(COALESCE(v_max_wait_minutes, 60), v_quiet_minutes), 1440);

  v_notice_kind := CASE
    WHEN v_classification = 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND_REQUIRED'
    WHEN v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL_REQUIRED'
    WHEN v_normalised_state IN ('RETURNED', 'REVERTED') THEN 'SETTLED_RETURN_DETECTED'
    ELSE 'BANK_FAILURE_DETECTED'
  END;

  IF v_normalised_state NOT IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED', 'UNKNOWN', 'PENDING', 'PROCESSING') THEN
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
        v_correction_disposition := 'AUTO_APPLIED';
      ELSE
        v_correction_disposition := 'BLOCKED';
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

  IF v_correction_disposition <> 'NO_CORRECTION_REQUIRED' THEN
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
    v_pay_batch_id,
    v_batch.execution_commit_ref,
    COALESCE(v_provider_key, v_batch.rail_provider_snapshot, 'UNKNOWN'),
    v_event_source,
    CASE
      WHEN v_correction_disposition = 'AMBIGUOUS' THEN 'AUTO_CORRECTION_BLOCKED'
      WHEN v_correction_disposition = 'BLOCKED' THEN 'AUTO_CORRECTION_BLOCKED'
      WHEN v_correction_disposition = 'AUTO_APPLIED' AND v_classification = 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND_APPLIED'
      WHEN v_correction_disposition = 'AUTO_APPLIED' AND v_classification = 'TRUE_SETTLED_REVERSAL_REQUIRED' THEN 'SETTLED_REVERSAL_APPLIED'
      ELSE v_notice_kind
    END,
    'OPEN',
    v_now + make_interval(mins => v_quiet_minutes),
    v_now + make_interval(mins => v_max_wait_minutes),
    jsonb_build_object(
      'pay_batch_id', v_pay_batch_id,
      'pay_bank_transfer_id', v_pay_bank_transfer_id,
      'bank_event_id', v_event_id,
      'mapping_status', v_mapping_status,
      'classification', v_classification,
      'correction_disposition', v_correction_disposition,
      'provider_key', v_provider_key,
      'provider_state', v_provider_state,
      'normalised_state', v_normalised_state,
      'amount', v_amount,
      'currency', v_currency
    ),
    '[]'::jsonb,
    v_now,
    v_now,
    NULL::timestamptz
  )
  RETURNING public.pay_payment_return_notice_groups.id
  INTO v_admin_notice_group_id;
  END IF;

  UPDATE public.pay_bank_transfer_events AS bank_event_to_update
  SET
    movement_classification = v_classification,
    correction_disposition = v_correction_disposition
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








