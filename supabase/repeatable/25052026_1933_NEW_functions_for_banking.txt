CREATE OR REPLACE FUNCTION public._pay_detect_manual_adjustments_for_carry_forward(
  p_pay_batch_id uuid,
  p_scope_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_json jsonb := COALESCE(p_scope_json, '{}'::jsonb);
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_manual_adjustments_to_carry_forward jsonb := '[]'::jsonb;
  v_carry_forward_blockers jsonb := '[]'::jsonb;
  v_manual_adjustment_support_details_json jsonb := '{}'::jsonb;
  v_finance_backed_count integer := 0;
  v_source_backed_count integer := 0;
  v_source_less_safe_count integer := 0;
  v_source_less_ambiguous_count integer := 0;
  v_manual_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF v_scope_json IS NULL OR jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_SCOPE_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'manual_adjustment_carry_forward_required', false,
      'manual_adjustments_to_carry_forward', '[]'::jsonb,
      'can_carry_forward_automatically', true,
      'carry_forward_blockers', '[]'::jsonb,
      'manual_adjustment_support_details_json', jsonb_build_object('manual_item_count', 0, 'reason', 'No pay_batch_item_ids were supplied in the resolved scope.'),
      'finance_backed_count', 0,
      'source_backed_count', 0,
      'source_less_safe_count', 0,
      'source_less_ambiguous_count', 0
    );
  END IF;

  WITH scoped_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      item_rows.item_type,
      item_rows.timesheet_id,
      item_rows.source_ref,
      item_rows.description,
      item_rows.amount_ex_vat,
      item_rows.amount_vat,
      item_rows.amount_inc_vat,
      item_rows.pay_channel,
      item_rows.umbrella_id,
      item_rows.finance_case_id,
      item_rows.finance_component_id,
      item_rows.reservation_id,
      item_rows.paye_treatment,
      item_rows.frozen_component_snapshot_json,
      item_rows.frozen_component_key_type,
      item_rows.frozen_component_key_value,
      item_rows.frozen_component_classification::text AS frozen_component_classification,
      item_rows.frozen_source_basis_json,
      item_rows.frozen_source_pay_method,
      item_rows.frozen_target_pay_method,
      item_rows.frozen_resolution_mode::text AS frozen_resolution_mode,
      item_rows.frozen_resolution_payload_json,
      item_rows.frozen_resolution_result_json,
      item_rows.operation_source_key,
      item_rows.pay_bank_transfer_id,
      batch_candidate_rows.pay_batch_id,
      batch_candidate_rows.id AS pay_batch_candidate_id,
      batch_candidate_rows.candidate_id,
      COALESCE(
        item_rows.umbrella_id,
        transfer_rows.umbrella_id,
        CASE
          WHEN upper(btrim(COALESCE(item_rows.pay_channel, ''))) = 'UMBRELLA' THEN candidate_record_rows.umbrella_id
          ELSE NULL::uuid
        END,
        CASE
          WHEN upper(COALESCE(transfer_rows.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN transfer_rows.payee_entity_id
          ELSE NULL::uuid
        END
      ) AS effective_umbrella_id,
      transfer_rows.transfer_group_key,
      transfer_rows.payee_entity_kind,
      transfer_rows.payee_entity_id,
      existing_target_carry_forward_rows.id AS existing_target_carry_forward_id,
      existing_source_carry_forward_rows.id AS existing_source_carry_forward_id,
      (
        upper(COALESCE(item_rows.item_type, '')) LIKE '%MANUAL%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%ADJUSTMENT%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%DEBT%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%CREDIT%'
        OR upper(COALESCE(item_rows.item_type, '')) IN ('ADJUSTMENT_DELTA', 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT', 'MANUAL_CREDIT_PAYOUT', 'MANUAL_DEBT_RECOVERY', 'FINANCE_ADJUSTMENT')
        OR upper(COALESCE(item_rows.source_ref, '')) LIKE 'MANUAL%'
        OR upper(COALESCE(item_rows.operation_source_key, '')) LIKE 'MANUAL%'
      ) AS is_manual_like
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    JOIN public.candidates AS candidate_record_rows
      ON candidate_record_rows.id = batch_candidate_rows.candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = item_rows.pay_bank_transfer_id
    LEFT JOIN public.pay_manual_adjustment_carry_forwards AS existing_target_carry_forward_rows
      ON existing_target_carry_forward_rows.target_pay_batch_item_id = item_rows.id
    LEFT JOIN public.pay_manual_adjustment_carry_forwards AS existing_source_carry_forward_rows
      ON existing_source_carry_forward_rows.source_pay_batch_item_id = item_rows.id
    WHERE batch_candidate_rows.pay_batch_id = p_pay_batch_id
      AND item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
  ), classified_items AS (
    SELECT
      scoped_items.*,
      CASE
        WHEN scoped_items.is_manual_like = false THEN 'NOT_MANUAL'
        WHEN scoped_items.existing_target_carry_forward_id IS NOT NULL
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'CARRY_FORWARD:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'CARRY_FORWARD:%'
          THEN 'SOURCE_BACKED'
        WHEN scoped_items.finance_case_id IS NOT NULL
          OR scoped_items.finance_component_id IS NOT NULL
          OR scoped_items.reservation_id IS NOT NULL
          THEN 'FINANCE_BACKED'
        WHEN scoped_items.timesheet_id IS NOT NULL
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'TIMESHEET:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'TS:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'SEG:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADJ:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'EXPENSE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'MILEAGE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADDITIONAL%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'FINANCE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'FINANCE_CASE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'RESERVATION:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADVANCE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'TS:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'SEG:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'ADJ:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'EXPENSE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'MILEAGE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'ADDITIONAL%'
          THEN 'SOURCE_BACKED'
        WHEN scoped_items.amount_inc_vat IS NOT NULL
          AND round(scoped_items.amount_inc_vat, 2) <> 0
          AND scoped_items.amount_ex_vat IS NOT NULL
          AND scoped_items.amount_vat IS NOT NULL
          AND NULLIF(btrim(COALESCE(scoped_items.description, '')), '') IS NOT NULL
          AND scoped_items.candidate_id IS NOT NULL
          AND upper(btrim(COALESCE(scoped_items.pay_channel, ''))) IN ('PAYE', 'UMBRELLA')
          AND (
            upper(btrim(COALESCE(scoped_items.pay_channel, ''))) = 'PAYE'
            OR scoped_items.effective_umbrella_id IS NOT NULL
          )
          THEN 'SOURCE_LESS_CARRY_FORWARD_SAFE'
        ELSE 'SOURCE_LESS_AMBIGUOUS'
      END AS classification,
      CASE
        WHEN scoped_items.amount_inc_vat IS NULL THEN 'MISSING_AMOUNT_INC_VAT'
        WHEN round(scoped_items.amount_inc_vat, 2) = 0 THEN 'ZERO_AMOUNT'
        WHEN scoped_items.amount_ex_vat IS NULL THEN 'MISSING_AMOUNT_EX_VAT'
        WHEN scoped_items.amount_vat IS NULL THEN 'MISSING_AMOUNT_VAT'
        WHEN NULLIF(btrim(COALESCE(scoped_items.description, '')), '') IS NULL THEN 'MISSING_DESCRIPTION'
        WHEN scoped_items.candidate_id IS NULL THEN 'MISSING_CANDIDATE_CONTEXT'
        WHEN upper(btrim(COALESCE(scoped_items.pay_channel, ''))) NOT IN ('PAYE', 'UMBRELLA') THEN 'UNSUPPORTED_OR_MISSING_PAY_CHANNEL'
        WHEN upper(btrim(COALESCE(scoped_items.pay_channel, ''))) = 'UMBRELLA' AND scoped_items.effective_umbrella_id IS NULL THEN 'MISSING_UMBRELLA_PAYEE_CONTEXT'
        ELSE NULL::text
      END AS ambiguity_reason
    FROM scoped_items
  )
  SELECT
    COALESCE((count(*) FILTER (WHERE classified_items.is_manual_like))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'FINANCE_BACKED'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_BACKED'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_CARRY_FORWARD_SAFE'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_AMBIGUOUS'))::integer, 0),
    COALESCE(jsonb_agg(jsonb_build_object(
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'pay_batch_id', classified_items.pay_batch_id::text,
      'pay_batch_candidate_id', classified_items.pay_batch_candidate_id::text,
      'candidate_id', classified_items.candidate_id::text,
      'umbrella_id', CASE WHEN classified_items.effective_umbrella_id IS NULL THEN NULL ELSE classified_items.effective_umbrella_id::text END,
      'pay_bank_transfer_id', CASE WHEN classified_items.pay_bank_transfer_id IS NULL THEN NULL ELSE classified_items.pay_bank_transfer_id::text END,
      'item_type', classified_items.item_type,
      'description', classified_items.description,
      'pay_channel', classified_items.pay_channel,
      'amount_ex_vat', classified_items.amount_ex_vat,
      'amount_vat', classified_items.amount_vat,
      'amount_inc_vat', classified_items.amount_inc_vat,
      'adjustment_direction', CASE WHEN classified_items.amount_inc_vat > 0 THEN 'CREDIT' ELSE 'DEBIT' END,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'paye_treatment', classified_items.paye_treatment,
      'source_ref', classified_items.source_ref,
      'operation_source_key', classified_items.operation_source_key,
      'classification', classified_items.classification
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_CARRY_FORWARD_SAFE'), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_build_object(
      'code', 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'item_type', classified_items.item_type,
      'description', classified_items.description,
      'amount_inc_vat', classified_items.amount_inc_vat,
      'pay_channel', classified_items.pay_channel,
      'reason', classified_items.ambiguity_reason
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_AMBIGUOUS'), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_build_object(
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'item_type', classified_items.item_type,
      'classification', classified_items.classification,
      'is_manual_like', classified_items.is_manual_like,
      'effective_umbrella_id', CASE WHEN classified_items.effective_umbrella_id IS NULL THEN NULL ELSE classified_items.effective_umbrella_id::text END,
      'payee_entity_kind', classified_items.payee_entity_kind,
      'payee_entity_id', CASE WHEN classified_items.payee_entity_id IS NULL THEN NULL ELSE classified_items.payee_entity_id::text END,
      'finance_case_id', CASE WHEN classified_items.finance_case_id IS NULL THEN NULL ELSE classified_items.finance_case_id::text END,
      'finance_component_id', CASE WHEN classified_items.finance_component_id IS NULL THEN NULL ELSE classified_items.finance_component_id::text END,
      'reservation_id', CASE WHEN classified_items.reservation_id IS NULL THEN NULL ELSE classified_items.reservation_id::text END,
      'timesheet_id', CASE WHEN classified_items.timesheet_id IS NULL THEN NULL ELSE classified_items.timesheet_id::text END,
      'source_ref', classified_items.source_ref,
      'operation_source_key', classified_items.operation_source_key,
      'existing_target_carry_forward_id', CASE WHEN classified_items.existing_target_carry_forward_id IS NULL THEN NULL ELSE classified_items.existing_target_carry_forward_id::text END,
      'existing_source_carry_forward_id', CASE WHEN classified_items.existing_source_carry_forward_id IS NULL THEN NULL ELSE classified_items.existing_source_carry_forward_id::text END,
      'ambiguity_reason', classified_items.ambiguity_reason
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.is_manual_like), '[]'::jsonb)
  INTO
    v_manual_count,
    v_finance_backed_count,
    v_source_backed_count,
    v_source_less_safe_count,
    v_source_less_ambiguous_count,
    v_manual_adjustments_to_carry_forward,
    v_carry_forward_blockers,
    v_manual_adjustment_support_details_json
  FROM classified_items;

  RETURN jsonb_build_object(
    'manual_adjustment_carry_forward_required', v_source_less_safe_count > 0,
    'manual_adjustments_to_carry_forward', COALESCE(v_manual_adjustments_to_carry_forward, '[]'::jsonb),
    'can_carry_forward_automatically', v_source_less_ambiguous_count = 0,
    'carry_forward_blockers', COALESCE(v_carry_forward_blockers, '[]'::jsonb),
    'manual_adjustment_support_details_json', jsonb_build_object(
      'manual_item_count', v_manual_count,
      'classified_items', COALESCE(v_manual_adjustment_support_details_json, '[]'::jsonb),
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true,
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    ),
    'finance_backed_count', v_finance_backed_count,
    'source_backed_count', v_source_backed_count,
    'source_less_safe_count', v_source_less_safe_count,
    'source_less_ambiguous_count', v_source_less_ambiguous_count
  );
END;
$function$;
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_create(
  p_source_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[],
  p_resolved_scope_json jsonb DEFAULT NULL::jsonb,
  p_source_correction_request_id uuid DEFAULT NULL::uuid,
  p_source_correction_work_item_id uuid DEFAULT NULL::uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_source_pay_batch_item_ids uuid[] := COALESCE(p_source_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_item_ids uuid[] := ARRAY[]::uuid[];
  v_source_pay_batch_id uuid := NULL::uuid;
  v_detection_result jsonb := '{}'::jsonb;
  v_safe_source_item_ids uuid[] := ARRAY[]::uuid[];
  v_existing_source_item_ids uuid[] := ARRAY[]::uuid[];
  v_created_count integer := 0;
  v_existing_count integer := 0;
  v_skipped_count integer := 0;
  v_total_safe_count integer := 0;
  v_records_json jsonb := '[]'::jsonb;
  v_skipped_json jsonb := '[]'::jsonb;
BEGIN
  IF p_resolved_scope_json IS NOT NULL AND jsonb_typeof(p_resolved_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF p_resolved_scope_json IS NOT NULL AND jsonb_typeof(p_resolved_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(p_resolved_scope_json->'pay_batch_item_ids') = 'array' THEN p_resolved_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  v_source_pay_batch_item_ids := COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[]) || COALESCE(v_scope_item_ids, ARRAY[]::uuid[]);

  SELECT COALESCE(array_agg(DISTINCT source_item_values.source_item_id) FILTER (WHERE source_item_values.source_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_source_pay_batch_item_ids
  FROM unnest(COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[])) AS source_item_values(source_item_id);

  IF COALESCE(array_length(v_source_pay_batch_item_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'created_count', 0,
      'existing_count', 0,
      'skipped_count', 0,
      'carry_forward_records', '[]'::jsonb,
      'skipped_items', '[]'::jsonb,
      'message', 'No source pay_batch_item_ids supplied.'
    );
  END IF;

  SELECT batch_candidate_rows.pay_batch_id
  INTO v_source_pay_batch_id
  FROM public.pay_batch_items AS source_item_rows
  JOIN public.pay_batch_candidates AS batch_candidate_rows
    ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
  WHERE source_item_rows.id = ANY(v_source_pay_batch_item_ids)
  ORDER BY batch_candidate_rows.pay_batch_id
  LIMIT 1;

  IF v_source_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'SOURCE_PAY_BATCH_ITEMS_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SOURCE_PAY_BATCH_ITEMS_NOT_FOUND')::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_batch_items AS source_item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
    WHERE source_item_rows.id = ANY(v_source_pay_batch_item_ids)
      AND batch_candidate_rows.pay_batch_id IS DISTINCT FROM v_source_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'SOURCE_PAY_BATCH_ITEMS_MUST_BELONG_TO_ONE_BATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SOURCE_PAY_BATCH_ITEMS_MUST_BELONG_TO_ONE_BATCH')::text;
  END IF;

  PERFORM 1
  FROM public.pay_batch_items AS lock_source_item_rows
  WHERE lock_source_item_rows.id = ANY(v_source_pay_batch_item_ids)
  ORDER BY lock_source_item_rows.id
  FOR UPDATE;

  v_detection_result := public._pay_detect_manual_adjustments_for_carry_forward(
    v_source_pay_batch_id,
    jsonb_build_object(
      'pay_batch_id', v_source_pay_batch_id::text,
      'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(item_id_values.source_item_id::text ORDER BY item_id_values.source_item_id::text) FROM unnest(v_source_pay_batch_item_ids) AS item_id_values(source_item_id)), '[]'::jsonb)
    ),
    p_actor_user_id
  );

  WITH safe_items AS (
    SELECT NULLIF(btrim(safe_item_values.safe_item_json->>'pay_batch_item_id'), '')::uuid AS pay_batch_item_id
    FROM jsonb_array_elements(COALESCE(v_detection_result->'manual_adjustments_to_carry_forward', '[]'::jsonb)) AS safe_item_values(safe_item_json)
    WHERE NULLIF(btrim(safe_item_values.safe_item_json->>'pay_batch_item_id'), '') ~ v_uuid_regex
  )
  SELECT COALESCE(array_agg(safe_items.pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_safe_source_item_ids
  FROM safe_items;

  v_total_safe_count := COALESCE(array_length(v_safe_source_item_ids, 1), 0);

  SELECT COALESCE(array_agg(existing_carry_forward_rows.source_pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_existing_source_item_ids
  FROM public.pay_manual_adjustment_carry_forwards AS existing_carry_forward_rows
  WHERE existing_carry_forward_rows.source_pay_batch_item_id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[]));

  WITH candidate_rows AS (
    SELECT
      source_item_rows.id AS source_pay_batch_item_id,
      batch_candidate_rows.pay_batch_id AS source_pay_batch_id,
      source_item_rows.pay_bank_transfer_id AS source_pay_bank_transfer_id,
      source_item_rows.pay_batch_candidate_id AS source_pay_batch_candidate_id,
      batch_candidate_rows.candidate_id AS candidate_id,
      COALESCE(source_item_rows.umbrella_id, transfer_rows.umbrella_id, CASE WHEN upper(COALESCE(source_item_rows.pay_channel, '')) = 'UMBRELLA' THEN candidate_record_rows.umbrella_id ELSE NULL::uuid END) AS umbrella_id,
      CASE
        WHEN NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'client_id', '')), '') ~ v_uuid_regex
             AND client_rows.id IS NOT NULL
          THEN client_rows.id
        ELSE NULL::uuid
      END AS client_id,
      source_item_rows.timesheet_id AS timesheet_id,
      upper(btrim(COALESCE(source_item_rows.pay_channel, ''))) AS pay_channel,
      CASE WHEN source_item_rows.amount_inc_vat > 0 THEN 'CREDIT' ELSE 'DEBIT' END AS adjustment_direction,
      source_item_rows.amount_ex_vat AS amount_ex_vat,
      source_item_rows.amount_vat AS amount_vat,
      source_item_rows.amount_inc_vat AS amount_inc_vat,
      COALESCE(NULLIF(btrim(source_item_rows.frozen_component_key_type), ''), source_item_rows.item_type) AS amount_basis,
      source_item_rows.paye_treatment AS paye_treatment,
      jsonb_build_object(
        'signed_amount_convention', 'SIGNED_AMOUNTS',
        'adjustment_direction_is_display_only', true,
        'pay_channel', source_item_rows.pay_channel,
        'paye_treatment', source_item_rows.paye_treatment,
        'frozen_component_classification', CASE WHEN source_item_rows.frozen_component_classification IS NULL THEN NULL ELSE source_item_rows.frozen_component_classification::text END,
        'frozen_source_pay_method', source_item_rows.frozen_source_pay_method,
        'frozen_target_pay_method', source_item_rows.frozen_target_pay_method,
        'amount_ex_vat', source_item_rows.amount_ex_vat,
        'amount_vat', source_item_rows.amount_vat,
        'amount_inc_vat', source_item_rows.amount_inc_vat
      ) AS tax_treatment_json,
      source_item_rows.description AS description,
      COALESCE(
        NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'reason', '')), ''),
        NULLIF(btrim(COALESCE(source_item_rows.frozen_resolution_payload_json->>'reason', '')), ''),
        NULLIF(btrim(COALESCE(source_item_rows.description, '')), '')
      ) AS reason,
      source_item_rows.source_ref AS source_ref,
      source_item_rows.operation_source_key AS source_operation_source_key,
      jsonb_build_object(
        'source_pay_batch_item', to_jsonb(source_item_rows),
        'source_pay_batch_candidate', to_jsonb(batch_candidate_rows),
        'source_pay_bank_transfer', CASE WHEN transfer_rows.id IS NULL THEN NULL ELSE to_jsonb(transfer_rows) END,
        'signed_amount_convention', 'SIGNED_AMOUNTS',
        'created_from_function', '_pay_manual_adjustment_carry_forward_create'
      ) AS source_snapshot_json
    FROM public.pay_batch_items AS source_item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
    JOIN public.candidates AS candidate_record_rows
      ON candidate_record_rows.id = batch_candidate_rows.candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = source_item_rows.pay_bank_transfer_id
    LEFT JOIN public.clients AS client_rows
      ON client_rows.id = CASE
        WHEN NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'client_id', '')), '') ~ v_uuid_regex
          THEN (source_item_rows.frozen_source_basis_json->>'client_id')::uuid
        ELSE NULL::uuid
      END
    WHERE source_item_rows.id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[]))
      AND source_item_rows.amount_inc_vat IS NOT NULL
      AND round(source_item_rows.amount_inc_vat, 2) <> 0
      AND NULLIF(btrim(COALESCE(source_item_rows.description, '')), '') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_manual_adjustment_carry_forwards AS target_carry_forward_rows
        WHERE target_carry_forward_rows.target_pay_batch_item_id = source_item_rows.id
      )
      AND upper(COALESCE(source_item_rows.source_ref, '')) NOT LIKE 'CARRY_FORWARD:%'
      AND upper(COALESCE(source_item_rows.operation_source_key, '')) NOT LIKE 'CARRY_FORWARD:%'
  ), upserted_rows AS (
    INSERT INTO public.pay_manual_adjustment_carry_forwards AS carry_forward_target_rows (
      source_pay_batch_id,
      source_pay_batch_item_id,
      source_pay_bank_transfer_id,
      source_pay_batch_candidate_id,
      source_correction_request_id,
      source_correction_work_item_id,
      candidate_id,
      umbrella_id,
      client_id,
      timesheet_id,
      pay_channel,
      adjustment_direction,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      amount_basis,
      paye_treatment,
      tax_treatment_json,
      description,
      reason,
      source_ref,
      source_operation_source_key,
      source_snapshot_json,
      status,
      created_by_user_id
    )
    SELECT
      candidate_rows.source_pay_batch_id,
      candidate_rows.source_pay_batch_item_id,
      candidate_rows.source_pay_bank_transfer_id,
      candidate_rows.source_pay_batch_candidate_id,
      p_source_correction_request_id,
      p_source_correction_work_item_id,
      candidate_rows.candidate_id,
      candidate_rows.umbrella_id,
      candidate_rows.client_id,
      candidate_rows.timesheet_id,
      candidate_rows.pay_channel,
      candidate_rows.adjustment_direction,
      candidate_rows.amount_ex_vat,
      candidate_rows.amount_vat,
      candidate_rows.amount_inc_vat,
      candidate_rows.amount_basis,
      candidate_rows.paye_treatment,
      candidate_rows.tax_treatment_json,
      candidate_rows.description,
      candidate_rows.reason,
      candidate_rows.source_ref,
      candidate_rows.source_operation_source_key,
      candidate_rows.source_snapshot_json,
      'PENDING_CARRY_FORWARD',
      p_actor_user_id
    FROM candidate_rows
    ON CONFLICT (source_pay_batch_item_id)
    DO UPDATE
    SET
      source_correction_request_id = COALESCE(carry_forward_target_rows.source_correction_request_id, EXCLUDED.source_correction_request_id),
      source_correction_work_item_id = COALESCE(carry_forward_target_rows.source_correction_work_item_id, EXCLUDED.source_correction_work_item_id),
      source_snapshot_json = COALESCE(carry_forward_target_rows.source_snapshot_json, '{}'::jsonb) || jsonb_build_object('last_seen_source_snapshot_json', EXCLUDED.source_snapshot_json),
      updated_at_utc = now()
    RETURNING *
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', upserted_rows.id::text,
    'source_pay_batch_id', upserted_rows.source_pay_batch_id::text,
    'source_pay_batch_item_id', upserted_rows.source_pay_batch_item_id::text,
    'source_pay_bank_transfer_id', CASE WHEN upserted_rows.source_pay_bank_transfer_id IS NULL THEN NULL ELSE upserted_rows.source_pay_bank_transfer_id::text END,
    'source_pay_batch_candidate_id', CASE WHEN upserted_rows.source_pay_batch_candidate_id IS NULL THEN NULL ELSE upserted_rows.source_pay_batch_candidate_id::text END,
    'candidate_id', upserted_rows.candidate_id::text,
    'umbrella_id', CASE WHEN upserted_rows.umbrella_id IS NULL THEN NULL ELSE upserted_rows.umbrella_id::text END,
    'pay_channel', upserted_rows.pay_channel,
    'adjustment_direction', upserted_rows.adjustment_direction,
    'amount_ex_vat', upserted_rows.amount_ex_vat,
    'amount_vat', upserted_rows.amount_vat,
    'amount_inc_vat', upserted_rows.amount_inc_vat,
    'description', upserted_rows.description,
    'status', upserted_rows.status,
    'was_existing', upserted_rows.source_pay_batch_item_id = ANY(COALESCE(v_existing_source_item_ids, ARRAY[]::uuid[])),
    'signed_amount_convention', 'SIGNED_AMOUNTS'
  ) ORDER BY upserted_rows.source_pay_batch_item_id::text), '[]'::jsonb)
  INTO v_records_json
  FROM upserted_rows;

  SELECT
    COALESCE((count(*) FILTER (WHERE COALESCE((record_values.record_json->>'was_existing')::boolean, false)))::integer, 0),
    COALESCE((count(*) FILTER (WHERE NOT COALESCE((record_values.record_json->>'was_existing')::boolean, false)))::integer, 0)
  INTO v_existing_count, v_created_count
  FROM jsonb_array_elements(COALESCE(v_records_json, '[]'::jsonb)) AS record_values(record_json);

  WITH recorded_items AS (
    SELECT NULLIF(btrim(record_values.record_json->>'source_pay_batch_item_id'), '')::uuid AS source_item_id
    FROM jsonb_array_elements(COALESCE(v_records_json, '[]'::jsonb)) AS record_values(record_json)
    WHERE NULLIF(btrim(record_values.record_json->>'source_pay_batch_item_id'), '') ~ v_uuid_regex
  ), skipped_items AS (
    SELECT source_item_values.source_item_id
    FROM unnest(COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[])) AS source_item_values(source_item_id)
    WHERE NOT EXISTS (
      SELECT 1
      FROM recorded_items AS recorded_item_rows
      WHERE recorded_item_rows.source_item_id = source_item_values.source_item_id
    )
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'pay_batch_item_id', skipped_items.source_item_id::text,
    'reason', CASE
      WHEN skipped_items.source_item_id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[])) THEN 'SAFE_ITEM_NOT_INSERTED_OR_REUSED_TARGET_CARRY_FORWARD_SOURCE'
      ELSE 'NOT_SOURCE_LESS_CARRY_FORWARD_SAFE'
    END
  ) ORDER BY skipped_items.source_item_id::text), '[]'::jsonb)
  INTO v_skipped_json
  FROM skipped_items;

  v_skipped_count := COALESCE(jsonb_array_length(COALESCE(v_skipped_json, '[]'::jsonb)), 0);

  RETURN jsonb_build_object(
    'ok', true,
    'source_pay_batch_id', v_source_pay_batch_id::text,
    'source_correction_request_id', CASE WHEN p_source_correction_request_id IS NULL THEN NULL ELSE p_source_correction_request_id::text END,
    'source_correction_work_item_id', CASE WHEN p_source_correction_work_item_id IS NULL THEN NULL ELSE p_source_correction_work_item_id::text END,
    'created_count', COALESCE(v_created_count, 0),
    'existing_count', COALESCE(v_existing_count, 0),
    'skipped_count', COALESCE(v_skipped_count, 0),
    'carry_forward_records', COALESCE(v_records_json, '[]'::jsonb),
    'skipped_items', COALESCE(v_skipped_json, '[]'::jsonb),
    'detection_result', COALESCE(v_detection_result, '{}'::jsonb),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true
  );
END;
$function$;
CREATE OR REPLACE FUNCTION public._pay_rail_state_money_movement_classify(
  p_transfer_status text DEFAULT NULL::text,
  p_rail_state text DEFAULT NULL::text,
  p_event_payload_json jsonb DEFAULT '{}'::jsonb,
  p_provider_meta_json jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(
  cash_state text,
  normalised_transfer_status text,
  is_final_money_moved boolean,
  is_terminal_no_money boolean,
  is_pending_non_final boolean,
  completed_at_allowed boolean,
  reason text,
  support_details_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_event_payload_json jsonb := '{}'::jsonb;
  v_provider_meta_json jsonb := '{}'::jsonb;
  v_transfer_status_upper text := NULL::text;
  v_rail_state_upper text := NULL::text;
  v_event_status_upper text := NULL::text;
  v_provider_status_upper text := NULL::text;
  v_event_outcome_upper text := NULL::text;
  v_provider_outcome_upper text := NULL::text;
  v_error_code_upper text := NULL::text;
  v_reason_code_upper text := NULL::text;
  v_primary_status_upper text := NULL::text;
  v_status_terms text[] := ARRAY[]::text[];
  v_final_status_present boolean := false;
  v_terminal_status_present boolean := false;
  v_pending_status_present boolean := false;
  v_explicit_final_paid_evidence boolean := false;
  v_explicit_terminal_no_money_evidence boolean := false;
  v_explicit_pending_evidence boolean := false;
  v_ambiguous_commit_or_execute boolean := false;
  v_bool_true_terms text[] := ARRAY['true','t','yes','y','1','final','settled','paid','completed','success','succeeded'];
  v_bool_false_terms text[] := ARRAY['false','f','no','n','0'];
BEGIN
  IF p_event_payload_json IS NOT NULL AND jsonb_typeof(p_event_payload_json) = 'object' THEN
    v_event_payload_json := p_event_payload_json;
  END IF;

  IF p_provider_meta_json IS NOT NULL AND jsonb_typeof(p_provider_meta_json) = 'object' THEN
    v_provider_meta_json := p_provider_meta_json;
  END IF;

  v_transfer_status_upper := upper(NULLIF(btrim(COALESCE(p_transfer_status, '')), ''));
  v_rail_state_upper := upper(NULLIF(btrim(COALESCE(p_rail_state, '')), ''));

  v_event_status_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'status',
    v_event_payload_json->>'state',
    v_event_payload_json->>'rail_state',
    v_event_payload_json->>'payment_status',
    v_event_payload_json->>'provider_status',
    v_event_payload_json #>> '{payment,status}',
    v_event_payload_json #>> '{provider,status}',
    v_event_payload_json #>> '{data,status}',
    ''
  )), ''));

  v_provider_status_upper := upper(NULLIF(btrim(COALESCE(
    v_provider_meta_json->>'status',
    v_provider_meta_json->>'state',
    v_provider_meta_json->>'rail_state',
    v_provider_meta_json->>'payment_status',
    v_provider_meta_json->>'provider_status',
    v_provider_meta_json #>> '{payment,status}',
    v_provider_meta_json #>> '{provider,status}',
    v_provider_meta_json #>> '{data,status}',
    ''
  )), ''));

  v_event_outcome_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'outcome',
    v_event_payload_json->>'cash_state',
    v_event_payload_json->>'money_movement_state',
    v_event_payload_json->>'mapping_status',
    v_event_payload_json #>> '{payment,outcome}',
    v_event_payload_json #>> '{provider,outcome}',
    v_event_payload_json #>> '{data,outcome}',
    ''
  )), ''));

  v_provider_outcome_upper := upper(NULLIF(btrim(COALESCE(
    v_provider_meta_json->>'outcome',
    v_provider_meta_json->>'cash_state',
    v_provider_meta_json->>'money_movement_state',
    v_provider_meta_json->>'mapping_status',
    v_provider_meta_json #>> '{payment,outcome}',
    v_provider_meta_json #>> '{provider,outcome}',
    v_provider_meta_json #>> '{data,outcome}',
    ''
  )), ''));

  v_error_code_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'error_code',
    v_event_payload_json->>'failure_code',
    v_event_payload_json->>'decline_code',
    v_event_payload_json #>> '{error,code}',
    v_event_payload_json #>> '{failure,code}',
    v_provider_meta_json->>'error_code',
    v_provider_meta_json->>'failure_code',
    v_provider_meta_json->>'decline_code',
    v_provider_meta_json #>> '{error,code}',
    v_provider_meta_json #>> '{failure,code}',
    ''
  )), ''));

  v_reason_code_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'reason_code',
    v_event_payload_json->>'reason',
    v_event_payload_json->>'failed_reason',
    v_event_payload_json #>> '{reason,code}',
    v_provider_meta_json->>'reason_code',
    v_provider_meta_json->>'reason',
    v_provider_meta_json->>'failed_reason',
    v_provider_meta_json #>> '{reason,code}',
    ''
  )), ''));

  v_status_terms := ARRAY[
    v_transfer_status_upper,
    v_rail_state_upper,
    v_event_status_upper,
    v_provider_status_upper,
    v_event_outcome_upper,
    v_provider_outcome_upper,
    v_error_code_upper,
    v_reason_code_upper
  ];

  SELECT COALESCE(status_terms.status_text, 'UNKNOWN')
  INTO v_primary_status_upper
  FROM unnest(v_status_terms) AS status_terms(status_text)
  WHERE status_terms.status_text IS NOT NULL
  LIMIT 1;

  v_final_status_present := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'PAID',
      'SETTLED',
      'COMPLETED',
      'SUCCESS',
      'SUCCEEDED',
      'PAYMENT_PAID',
      'PAYMENT_SETTLED',
      'PAYMENT_COMPLETED',
      'FINAL_PAID',
      'MONEY_MOVED',
      'CONFIRMED_PAID',
      'CONFIRMED_SETTLED'
    )
  );

  v_terminal_status_present := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'FAILED',
      'FAILURE',
      'REJECTED',
      'DECLINED',
      'CANCELLED',
      'CANCELED',
      'SUBMISSION_FAILED',
      'FAILED_BEFORE_COMMIT',
      'CANCELLED_BEFORE_RELEASE',
      'CANCELED_BEFORE_RELEASE',
      'WRONG_BANK',
      'WRONG_BANK_DETAILS',
      'NO_MONEY',
      'NO_PAYMENT_MADE',
      'NOT_PAID',
      'RETURNED',
      'REVERSED_BEFORE_RELEASE',
      'INSUFFICIENT_FUNDS',
      'ACCOUNT_CLOSED',
      'INVALID_ACCOUNT',
      'INVALID_SORT_CODE',
      'BANK_REJECTED'
    )
  );

  v_pending_status_present := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'ACCEPTED',
      'SCHEDULED',
      'SUBMITTED',
      'SENT',
      'PROCESSING',
      'IN_FLIGHT',
      'IN-FLIGHT',
      'INFLIGHT',
      'QUEUED',
      'PENDING',
      'PENDING_SETTLEMENT',
      'PENDING_CONFIRMATION',
      'PENDING_SUBMISSION',
      'AUTHORISED',
      'AUTHORIZED',
      'AWAITING_CONFIRMATION',
      'AWAITING_SETTLEMENT'
    )
  );

  v_ambiguous_commit_or_execute := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN ('COMMITTED', 'EXECUTED')
  );

  v_explicit_final_paid_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'final_paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'is_final_money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'settlement_confirmed', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'settled', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{money_movement,final_paid}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{settlement,confirmed}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'final_paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'is_final_money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'settlement_confirmed', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'settled', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{money_movement,final_paid}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{settlement,confirmed}', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_terminal_no_money_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'no_payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'terminal_no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'wrong_bank', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'cancelled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'canceled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'failed_before_commit', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{money_movement,no_payment_made}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'no_payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'terminal_no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'wrong_bank', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'cancelled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'canceled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'failed_before_commit', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{money_movement,no_payment_made}', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_pending_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'pending', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'in_flight', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'processing', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'pending', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'in_flight', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'processing', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_final_paid_evidence := COALESCE(v_explicit_final_paid_evidence, false);
  v_explicit_terminal_no_money_evidence := COALESCE(v_explicit_terminal_no_money_evidence, false);
  v_explicit_pending_evidence := COALESCE(v_explicit_pending_evidence, false);

  IF v_explicit_final_paid_evidence OR v_final_status_present THEN
    cash_state := 'FINAL_PAID';
    normalised_transfer_status := 'COMPLETED';
    is_final_money_moved := true;
    is_terminal_no_money := false;
    is_pending_non_final := false;
    completed_at_allowed := true;
    reason := 'Explicit final-paid/provider-settled evidence was found.';
  ELSIF v_explicit_terminal_no_money_evidence OR v_terminal_status_present THEN
    cash_state := 'TERMINAL_NO_MONEY';
    normalised_transfer_status := CASE
      WHEN v_primary_status_upper IN ('CANCELLED', 'CANCELED', 'CANCELLED_BEFORE_RELEASE', 'CANCELED_BEFORE_RELEASE') THEN 'CANCELLED'
      ELSE 'FAILED'
    END;
    is_final_money_moved := false;
    is_terminal_no_money := true;
    is_pending_non_final := false;
    completed_at_allowed := false;
    reason := 'Terminal no-money/failure/cancelled-before-release evidence was found.';
  ELSIF v_explicit_pending_evidence OR v_pending_status_present THEN
    cash_state := 'PENDING_NON_FINAL';
    normalised_transfer_status := CASE
      WHEN v_primary_status_upper IN ('SCHEDULED', 'SUBMITTED', 'SENT', 'PROCESSING', 'QUEUED', 'ACCEPTED', 'PENDING') THEN v_primary_status_upper
      ELSE 'PENDING'
    END;
    is_final_money_moved := false;
    is_terminal_no_money := false;
    is_pending_non_final := true;
    completed_at_allowed := false;
    reason := 'Pending/non-final provider or rail state was found.';
  ELSE
    cash_state := 'UNKNOWN';
    normalised_transfer_status := COALESCE(v_primary_status_upper, 'UNKNOWN');
    is_final_money_moved := false;
    is_terminal_no_money := false;
    is_pending_non_final := false;
    completed_at_allowed := false;
    reason := CASE
      WHEN v_ambiguous_commit_or_execute THEN 'Bare COMMITTED/EXECUTED evidence is ambiguous and is not final-paid evidence without explicit final settlement metadata.'
      ELSE 'No explicit final-paid, terminal no-money, or pending non-final evidence was found.'
    END;
  END IF;

  support_details_json := jsonb_build_object(
    'transfer_status', p_transfer_status,
    'rail_state', p_rail_state,
    'transfer_status_upper', v_transfer_status_upper,
    'rail_state_upper', v_rail_state_upper,
    'event_status_upper', v_event_status_upper,
    'provider_status_upper', v_provider_status_upper,
    'event_outcome_upper', v_event_outcome_upper,
    'provider_outcome_upper', v_provider_outcome_upper,
    'error_code_upper', v_error_code_upper,
    'reason_code_upper', v_reason_code_upper,
    'primary_status_upper', v_primary_status_upper,
    'final_status_present', v_final_status_present,
    'terminal_status_present', v_terminal_status_present,
    'pending_status_present', v_pending_status_present,
    'explicit_final_paid_evidence', v_explicit_final_paid_evidence,
    'explicit_terminal_no_money_evidence', v_explicit_terminal_no_money_evidence,
    'explicit_pending_evidence', v_explicit_pending_evidence,
    'ambiguous_commit_or_execute', v_ambiguous_commit_or_execute,
    'committed_or_executed_requires_explicit_final_paid_metadata', true,
    'event_payload_keys', CASE
      WHEN jsonb_typeof(v_event_payload_json) = 'object' THEN COALESCE((SELECT jsonb_agg(event_keys.key_name ORDER BY event_keys.key_name) FROM jsonb_object_keys(v_event_payload_json) AS event_keys(key_name)), '[]'::jsonb)
      ELSE '[]'::jsonb
    END,
    'provider_meta_keys', CASE
      WHEN jsonb_typeof(v_provider_meta_json) = 'object' THEN COALESCE((SELECT jsonb_agg(meta_keys.key_name ORDER BY meta_keys.key_name) FROM jsonb_object_keys(v_provider_meta_json) AS meta_keys(key_name)), '[]'::jsonb)
      ELSE '[]'::jsonb
    END
  );

  RETURN NEXT;
END;
$function$;
CREATE OR REPLACE FUNCTION public._pay_resolve_payment_scope_for_cancel_rewind(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_lock_mode boolean DEFAULT false,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_type text := NULL::text;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_group_keys text[] := ARRAY[]::text[];
  v_selected_pay_channels text[] := ARRAY[]::text[];
  v_explicit_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_authoritative_explicit_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_pay_channels text[] := ARRAY[]::text[];
  v_transfer_group_keys text[] := ARRAY[]::text[];
  v_candidate_payment_group_keys text[] := ARRAY[]::text[];
  v_missing_explicit_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_item_count integer := 0;
  v_expanded_item_count integer := 0;
  v_is_full_scope boolean := true;
  v_partial_scope_blockers jsonb := '[]'::jsonb;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_SCOPE_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_SCOPE_SELECTION_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM public.pay_batches AS batch_check WHERE batch_check.id = p_pay_batch_id) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_scope_type := upper(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));

  IF v_scope_type IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_SCOPE_TYPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_SCOPE_TYPE_REQUIRED', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  WITH selected_items AS (
    SELECT selected_item_rows.*
    FROM public._pay_payment_correction_selected_items(p_pay_batch_id, p_selection_json, true) AS selected_item_rows
  )
  SELECT
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_item_id) FILTER (WHERE selected_items.pay_batch_item_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_candidate_id) FILTER (WHERE selected_items.pay_batch_candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.candidate_id) FILTER (WHERE selected_items.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_bank_transfer_id) FILTER (WHERE selected_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.umbrella_id) FILTER (WHERE selected_items.umbrella_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.transfer_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(selected_items.transfer_group_key, '')), '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT upper(btrim(COALESCE(selected_items.pay_channel, '')))) FILTER (WHERE NULLIF(btrim(COALESCE(selected_items.pay_channel, '')), '') IS NOT NULL), ARRAY[]::text[]),
    count(*)::integer
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_pay_batch_candidate_ids,
    v_selected_candidate_ids,
    v_selected_pay_bank_transfer_ids,
    v_selected_umbrella_ids,
    v_selected_transfer_group_keys,
    v_selected_pay_channels,
    v_selected_item_count
  FROM selected_items;

  WITH raw_values AS (
    SELECT explicit_item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_selection_json->'pay_batch_item_ids') = 'array' THEN p_selection_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS explicit_item_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_item_id'
    WHERE p_selection_json ? 'pay_batch_item_id'
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_explicit_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  WITH raw_values AS (
    SELECT expected_item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids') = 'array' THEN p_selection_json->'expected_pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS expected_item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_expected_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  SELECT COALESCE(array_agg(DISTINCT explicit_items.explicit_item_id) FILTER (WHERE explicit_items.explicit_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_authoritative_explicit_item_ids
  FROM (
    SELECT unnest(COALESCE(v_explicit_pay_batch_item_ids, ARRAY[]::uuid[])) AS explicit_item_id
    UNION ALL
    SELECT unnest(COALESCE(v_expected_pay_batch_item_ids, ARRAY[]::uuid[])) AS explicit_item_id
  ) AS explicit_items;

  WITH selected_seed AS (
    SELECT selected_item_rows.*
    FROM public._pay_payment_correction_selected_items(p_pay_batch_id, p_selection_json, true) AS selected_item_rows
  ), expanded_items AS (
    SELECT DISTINCT candidate_item_rows.id AS pay_batch_item_id
    FROM public.pay_batch_items AS candidate_item_rows
    JOIN public.pay_batch_candidates AS candidate_batch_rows
      ON candidate_batch_rows.id = candidate_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS candidate_transfer_rows
      ON candidate_transfer_rows.id = candidate_item_rows.pay_bank_transfer_id
    WHERE candidate_batch_rows.pay_batch_id = p_pay_batch_id
      AND (
        v_scope_type = 'BATCH'
        OR candidate_item_rows.id = ANY(COALESCE(v_selected_pay_batch_item_ids, ARRAY[]::uuid[]))
        OR (
          COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
          AND candidate_item_rows.pay_bank_transfer_id = ANY(v_selected_pay_bank_transfer_ids)
        )
        OR (
          v_scope_type = 'CANDIDATES'
          AND candidate_item_rows.pay_batch_candidate_id = ANY(COALESCE(v_selected_pay_batch_candidate_ids, ARRAY[]::uuid[]))
        )
        OR EXISTS (
          SELECT 1
          FROM selected_seed AS selected_seed_rows
          WHERE selected_seed_rows.pay_batch_candidate_id = candidate_item_rows.pay_batch_candidate_id
            AND (
              (
                selected_seed_rows.pay_bank_transfer_id IS NOT NULL
                AND candidate_item_rows.pay_bank_transfer_id = selected_seed_rows.pay_bank_transfer_id
              )
              OR (
                NULLIF(btrim(COALESCE(selected_seed_rows.transfer_group_key, '')), '') IS NOT NULL
                AND NULLIF(btrim(COALESCE(candidate_transfer_rows.transfer_group_key, '')), '') IS NOT NULL
                AND candidate_transfer_rows.transfer_group_key = selected_seed_rows.transfer_group_key
                AND upper(btrim(COALESCE(candidate_item_rows.pay_channel, ''))) = upper(btrim(COALESCE(selected_seed_rows.pay_channel, '')))
              )
              OR (
                selected_seed_rows.pay_bank_transfer_id IS NULL
                AND candidate_item_rows.pay_bank_transfer_id IS NULL
                AND upper(btrim(COALESCE(candidate_item_rows.pay_channel, ''))) = upper(btrim(COALESCE(selected_seed_rows.pay_channel, '')))
                AND candidate_item_rows.umbrella_id IS NOT DISTINCT FROM selected_seed_rows.umbrella_id
              )
            )
        )
      )
  )
  SELECT COALESCE(array_agg(expanded_items.pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM expanded_items;

  v_expanded_item_count := COALESCE(array_length(v_pay_batch_item_ids, 1), 0);

  WITH expanded_batch_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      item_rows.pay_batch_candidate_id,
      batch_candidate_rows.candidate_id,
      item_rows.pay_bank_transfer_id,
      COALESCE(item_rows.umbrella_id, transfer_rows.umbrella_id) AS umbrella_id,
      item_rows.finance_case_id,
      item_rows.finance_component_id,
      item_rows.reservation_id,
      item_rows.timesheet_id,
      upper(btrim(COALESCE(item_rows.pay_channel, ''))) AS pay_channel,
      transfer_rows.transfer_group_key,
      (
        batch_candidate_rows.candidate_id::text
        || ':' || upper(btrim(COALESCE(item_rows.pay_channel, '')))
        || ':' || COALESCE(item_rows.umbrella_id::text, transfer_rows.umbrella_id::text, 'NO_UMBRELLA')
        || ':' || COALESCE(transfer_rows.transfer_group_key, 'NO_TRANSFER_GROUP')
      ) AS candidate_payment_group_key
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = item_rows.pay_bank_transfer_id
    WHERE item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
  ), reservation_rows AS (
    SELECT reservation_source_rows.*
    FROM public.pay_advance_reservations AS reservation_source_rows
    WHERE reservation_source_rows.pay_batch_id = p_pay_batch_id
      AND (
        reservation_source_rows.pay_batch_item_id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
        OR reservation_source_rows.pay_batch_candidate_id = ANY(COALESCE(v_selected_pay_batch_candidate_ids, ARRAY[]::uuid[]))
      )
  )
  SELECT
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_batch_candidate_id) FILTER (WHERE expanded_batch_items.pay_batch_candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.candidate_id) FILTER (WHERE expanded_batch_items.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.umbrella_id) FILTER (WHERE expanded_batch_items.umbrella_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_bank_transfer_id) FILTER (WHERE expanded_batch_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.finance_case_id, reservation_rows.finance_case_id)) FILTER (WHERE COALESCE(expanded_batch_items.finance_case_id, reservation_rows.finance_case_id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.finance_component_id, reservation_rows.finance_component_id)) FILTER (WHERE COALESCE(expanded_batch_items.finance_component_id, reservation_rows.finance_component_id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.reservation_id, reservation_rows.id)) FILTER (WHERE COALESCE(expanded_batch_items.reservation_id, reservation_rows.id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.timesheet_id) FILTER (WHERE expanded_batch_items.timesheet_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_channel) FILTER (WHERE NULLIF(expanded_batch_items.pay_channel, '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.transfer_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(expanded_batch_items.transfer_group_key, '')), '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.candidate_payment_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(expanded_batch_items.candidate_payment_group_key, '')), '') IS NOT NULL), ARRAY[]::text[])
  INTO
    v_pay_batch_candidate_ids,
    v_candidate_ids,
    v_umbrella_ids,
    v_pay_bank_transfer_ids,
    v_finance_case_ids,
    v_finance_component_ids,
    v_reservation_ids,
    v_timesheet_ids,
    v_pay_channels,
    v_transfer_group_keys,
    v_candidate_payment_group_keys
  FROM expanded_batch_items
  LEFT JOIN reservation_rows
    ON reservation_rows.pay_batch_item_id = expanded_batch_items.pay_batch_item_id
    OR reservation_rows.pay_batch_candidate_id = expanded_batch_items.pay_batch_candidate_id;

  IF COALESCE(array_length(v_authoritative_explicit_item_ids, 1), 0) > 0 THEN
    SELECT COALESCE(array_agg(missing_item_rows.pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_missing_explicit_item_ids
    FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS missing_item_rows(pay_batch_item_id)
    WHERE NOT (missing_item_rows.pay_batch_item_id = ANY(v_authoritative_explicit_item_ids));

    v_is_full_scope := COALESCE(array_length(v_missing_explicit_item_ids, 1), 0) = 0;
  ELSE
    v_is_full_scope := true;
  END IF;

  IF NOT v_is_full_scope THEN
    v_partial_scope_blockers := v_partial_scope_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SELECT_FULL_UNPAID_PAYMENT_SCOPE_REQUIRED',
      'message', 'The selected rows do not include the whole unpaid payment scope. Select the full payment scope before cancellation or rewind.',
      'missing_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(missing_values.pay_batch_item_id::text ORDER BY missing_values.pay_batch_item_id::text) FROM unnest(v_missing_explicit_item_ids) AS missing_values(pay_batch_item_id)), '[]'::jsonb)
    ));
  END IF;

  IF COALESCE(p_lock_mode, false) THEN
    PERFORM 1
    FROM public.pay_batches AS lock_batch_rows
    WHERE lock_batch_rows.id = p_pay_batch_id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_bank_transfers AS lock_transfer_rows
    WHERE lock_transfer_rows.id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    ORDER BY lock_transfer_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_batch_candidates AS lock_candidate_rows
    WHERE lock_candidate_rows.id = ANY(COALESCE(v_pay_batch_candidate_ids, ARRAY[]::uuid[]))
    ORDER BY lock_candidate_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_batch_items AS lock_item_rows
    WHERE lock_item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
    ORDER BY lock_item_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_advance_reservations AS lock_reservation_rows
    WHERE lock_reservation_rows.id = ANY(COALESCE(v_reservation_ids, ARRAY[]::uuid[]))
    ORDER BY lock_reservation_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_finance_case_components AS lock_component_rows
    WHERE lock_component_rows.id = ANY(COALESCE(v_finance_component_ids, ARRAY[]::uuid[]))
    ORDER BY lock_component_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_payment_correction_requests AS lock_request_rows
    WHERE lock_request_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_request_rows.status, '')) IN ('REQUESTED', 'PENDING', 'AUTHORISED', 'AUTHORIZED', 'IN_PROGRESS', 'PROCESSING', 'EXPANDED')
    ORDER BY lock_request_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_payment_correction_work_items AS lock_work_item_rows
    WHERE lock_work_item_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_work_item_rows.status, '')) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING')
    ORDER BY lock_work_item_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.banking_pay_operations AS lock_operation_rows
    WHERE lock_operation_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_operation_rows.status, '')) IN ('QUEUED', 'RUNNING', 'PROCESSING', 'IN_PROGRESS', 'REVIEW_REQUIRED')
    ORDER BY lock_operation_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.banking_pay_operation_chunks AS lock_chunk_rows
    JOIN public.banking_pay_operations AS lock_chunk_operation_rows
      ON lock_chunk_operation_rows.id = lock_chunk_rows.operation_id
    WHERE lock_chunk_operation_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_chunk_rows.status, '')) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING')
    ORDER BY lock_chunk_rows.id
    FOR UPDATE;
  END IF;

  v_result := jsonb_build_object(
    'scope_type', v_scope_type,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_batch_candidate_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_batch_candidate_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'candidate_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'umbrella_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_umbrella_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'finance_case_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_finance_case_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'finance_component_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_finance_component_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'reservation_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_reservation_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'timesheet_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_channels', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_pay_channels, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'transfer_group_keys', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_transfer_group_keys, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'candidate_payment_group_keys', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_candidate_payment_group_keys, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'is_full_scope', v_is_full_scope,
    'partial_scope_blockers', COALESCE(v_partial_scope_blockers, '[]'::jsonb),
    'support_details_json', jsonb_build_object(
      'selected_item_count', v_selected_item_count,
      'expanded_item_count', v_expanded_item_count,
      'explicit_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_explicit_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
      'expected_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_expected_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
      'lock_mode', COALESCE(p_lock_mode, false),
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    )
  );

  RETURN v_result;
END;
$function$;
CREATE OR REPLACE FUNCTION public._pay_bank_transfer_provider_evidence_classify(
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_pay_bank_transfer_id uuid DEFAULT NULL::uuid,
  p_selection_json jsonb DEFAULT NULL::jsonb,
  p_operation_id uuid DEFAULT NULL::uuid
)
RETURNS TABLE(
  evidence_class text,
  provider_submitted boolean,
  provider_request_sent boolean,
  provider_response_present boolean,
  provider_event_present boolean,
  provider_external_id_present boolean,
  local_prepared_only boolean,
  cash_state text,
  blocker_code text,
  reason text,
  support_details_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_type text := NULL::text;
  v_direct_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_count integer := 0;
  v_scope_row_count integer := 0;
  v_provider_external_id_count integer := 0;
  v_provider_response_count integer := 0;
  v_provider_event_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_outcome_unknown_count integer := 0;
  v_local_prepare_identity_count integer := 0;
  v_final_paid_count integer := 0;
  v_terminal_no_money_count integer := 0;
  v_pending_non_final_count integer := 0;
  v_unknown_cash_state_count integer := 0;
  v_operation_submit_attempt_count integer := 0;
  v_operation_submit_unknown_count integer := 0;
  v_operation_payload_evidence_count integer := 0;
  v_chunk_submit_attempt_count integer := 0;
BEGIN
  IF p_pay_bank_transfer_id IS NOT NULL AND v_effective_pay_batch_id IS NULL THEN
    SELECT transfer_row.pay_batch_id
    INTO v_effective_pay_batch_id
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.id = p_pay_bank_transfer_id;
  END IF;

  IF p_selection_json IS NOT NULL AND jsonb_typeof(p_selection_json) = 'object' THEN
    v_scope_type := upper(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));
  END IF;

  IF p_pay_bank_transfer_id IS NOT NULL THEN
    v_direct_transfer_ids := ARRAY[p_pay_bank_transfer_id];
  END IF;

  IF p_selection_json IS NOT NULL AND jsonb_typeof(p_selection_json) = 'object' THEN
    WITH raw_values AS (
      SELECT direct_transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(p_selection_json->'pay_bank_transfer_ids') = 'array' THEN p_selection_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS direct_transfer_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_bank_transfer_id'
      WHERE p_selection_json ? 'pay_bank_transfer_id'
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_selected_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  v_transfer_ids := COALESCE(v_direct_transfer_ids, ARRAY[]::uuid[]) || COALESCE(v_selected_transfer_ids, ARRAY[]::uuid[]);

  IF v_effective_pay_batch_id IS NOT NULL
     AND p_selection_json IS NOT NULL
     AND jsonb_typeof(p_selection_json) = 'object'
     AND v_scope_type IS NOT NULL THEN
    BEGIN
      SELECT COALESCE(array_agg(DISTINCT selected_items.pay_bank_transfer_id) FILTER (WHERE selected_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[])
      INTO v_selected_transfer_ids
      FROM public._pay_payment_correction_selected_items(v_effective_pay_batch_id, p_selection_json, true) AS selected_items;

      v_transfer_ids := COALESCE(v_transfer_ids, ARRAY[]::uuid[]) || COALESCE(v_selected_transfer_ids, ARRAY[]::uuid[]);
    EXCEPTION
      WHEN OTHERS THEN
        v_transfer_ids := COALESCE(v_transfer_ids, ARRAY[]::uuid[]);
    END;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT transfer_id_rows.transfer_id) FILTER (WHERE transfer_id_rows.transfer_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_transfer_ids
  FROM unnest(COALESCE(v_transfer_ids, ARRAY[]::uuid[])) AS transfer_id_rows(transfer_id);

  IF v_effective_pay_batch_id IS NOT NULL
     AND COALESCE(array_length(v_transfer_ids, 1), 0) = 0
     AND (p_selection_json IS NULL OR v_scope_type = 'BATCH') THEN
    SELECT COALESCE(array_agg(batch_transfer_rows.id), ARRAY[]::uuid[])
    INTO v_transfer_ids
    FROM public.pay_bank_transfers AS batch_transfer_rows
    WHERE batch_transfer_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  WITH target_transfers AS (
    SELECT transfer_row.*
    FROM public.pay_bank_transfers AS transfer_row
    WHERE (
        COALESCE(array_length(v_transfer_ids, 1), 0) > 0
        AND transfer_row.id = ANY(v_transfer_ids)
      )
      OR (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        AND v_effective_pay_batch_id IS NOT NULL
        AND transfer_row.pay_batch_id = v_effective_pay_batch_id
        AND (p_selection_json IS NULL OR v_scope_type = 'BATCH')
      )
  ), target_scopes AS (
    SELECT scope_row.*
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE (v_effective_pay_batch_id IS NULL OR scope_row.pay_batch_id = v_effective_pay_batch_id)
      AND (p_operation_id IS NULL OR scope_row.operation_id = p_operation_id)
      AND (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        OR scope_row.pay_bank_transfer_id = ANY(v_transfer_ids)
        OR EXISTS (
          SELECT 1
          FROM target_transfers AS transfer_for_scope
          WHERE transfer_for_scope.pay_batch_id = scope_row.pay_batch_id
            AND transfer_for_scope.pay_channel = scope_row.pay_channel
            AND NULLIF(btrim(COALESCE(transfer_for_scope.transfer_group_key, '')), '') IS NOT NULL
            AND transfer_for_scope.transfer_group_key = scope_row.transfer_group_key
        )
      )
  ), evidence_rows AS (
    SELECT
      'TRANSFER'::text AS evidence_source,
      transfer_row.id AS pay_bank_transfer_id,
      transfer_row.pay_batch_id AS pay_batch_id,
      NULL::uuid AS operation_id,
      transfer_row.status AS transfer_status,
      transfer_row.rail_state AS rail_state,
      transfer_row.request_id AS request_id,
      transfer_row.payment_reference AS payment_reference,
      transfer_row.rail_tx_id AS rail_tx_id,
      COALESCE(transfer_row.rail_meta_json, '{}'::jsonb) AS meta_json,
      transfer_row.completed_at_utc AS completed_at_utc
    FROM target_transfers AS transfer_row
    UNION ALL
    SELECT
      'TRANSFER_SCOPE'::text AS evidence_source,
      scope_row.pay_bank_transfer_id AS pay_bank_transfer_id,
      scope_row.pay_batch_id AS pay_batch_id,
      scope_row.operation_id AS operation_id,
      scope_row.status AS transfer_status,
      NULL::text AS rail_state,
      scope_row.request_id AS request_id,
      scope_row.payment_reference AS payment_reference,
      NULL::text AS rail_tx_id,
      jsonb_build_object(
        'scope_id', scope_row.id::text,
        'operation_id', scope_row.operation_id::text,
        'scope_status', scope_row.status,
        'scope_request_id_present', NULLIF(btrim(COALESCE(scope_row.request_id, '')), '') IS NOT NULL,
        'scope_payment_reference_present', NULLIF(btrim(COALESCE(scope_row.payment_reference, '')), '') IS NOT NULL
      ) AS meta_json,
      NULL::timestamptz AS completed_at_utc
    FROM target_scopes AS scope_row
  ), classified_evidence_rows AS (
    SELECT
      evidence_rows.*,
      classification_rows.cash_state AS classified_cash_state,
      classification_rows.is_final_money_moved AS classified_is_final_money_moved,
      classification_rows.is_terminal_no_money AS classified_is_terminal_no_money,
      classification_rows.is_pending_non_final AS classified_is_pending_non_final,
      classification_rows.support_details_json AS classification_support_details_json,
      (
        NULLIF(btrim(COALESCE(evidence_rows.request_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(COALESCE(evidence_rows.payment_reference, '')), '') IS NOT NULL
        OR evidence_rows.evidence_source = 'TRANSFER_SCOPE'
      ) AS has_local_prepare_identity,
      (
        (NULLIF(btrim(COALESCE(evidence_rows.rail_tx_id, '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.rail_tx_id, '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'rail_tx_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'rail_tx_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_transaction_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_transaction_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_payment_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_payment_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_reference', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_reference', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_external_id,
      (
        (evidence_rows.meta_json ? 'provider_response' AND evidence_rows.meta_json->'provider_response' IS NOT NULL AND evidence_rows.meta_json->'provider_response' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'object' AND evidence_rows.meta_json->'provider_response' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'array' AND evidence_rows.meta_json->'provider_response' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_response_json' AND evidence_rows.meta_json->'provider_response_json' IS NOT NULL AND evidence_rows.meta_json->'provider_response_json' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'object' AND evidence_rows.meta_json->'provider_response_json' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'array' AND evidence_rows.meta_json->'provider_response_json' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'number')))
        OR (evidence_rows.meta_json ? 'submit_response' AND evidence_rows.meta_json->'submit_response' IS NOT NULL AND evidence_rows.meta_json->'submit_response' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'object' AND evidence_rows.meta_json->'submit_response' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'array' AND evidence_rows.meta_json->'submit_response' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'submit_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'submit_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'number')))
        OR (evidence_rows.meta_json ? 'response_json' AND evidence_rows.meta_json->'response_json' IS NOT NULL AND evidence_rows.meta_json->'response_json' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'response_json') = 'object' AND evidence_rows.meta_json->'response_json' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'array' AND evidence_rows.meta_json->'response_json' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_result' AND evidence_rows.meta_json->'provider_result' IS NOT NULL AND evidence_rows.meta_json->'provider_result' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'object' AND evidence_rows.meta_json->'provider_result' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'array' AND evidence_rows.meta_json->'provider_result' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_payload' AND evidence_rows.meta_json->'provider_payload' IS NOT NULL AND evidence_rows.meta_json->'provider_payload' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'object' AND evidence_rows.meta_json->'provider_payload' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'array' AND evidence_rows.meta_json->'provider_payload' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_payload')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_payload')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'number')))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'http_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'http_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_http_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_http_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'response_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'response_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,response}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,response}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_response,
      (
        (evidence_rows.meta_json ? 'provider_event' AND evidence_rows.meta_json->'provider_event' IS NOT NULL AND evidence_rows.meta_json->'provider_event' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'object' AND evidence_rows.meta_json->'provider_event' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'array' AND evidence_rows.meta_json->'provider_event' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_event')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_event')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'number')))
        OR (evidence_rows.meta_json ? 'latest_provider_event' AND evidence_rows.meta_json->'latest_provider_event' IS NOT NULL AND evidence_rows.meta_json->'latest_provider_event' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'object' AND evidence_rows.meta_json->'latest_provider_event' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'array' AND evidence_rows.meta_json->'latest_provider_event' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'latest_provider_event')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'latest_provider_event')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'number')))
        OR (evidence_rows.meta_json ? 'webhook' AND evidence_rows.meta_json->'webhook' IS NOT NULL AND evidence_rows.meta_json->'webhook' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'webhook') = 'object' AND evidence_rows.meta_json->'webhook' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'array' AND evidence_rows.meta_json->'webhook' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'webhook')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'webhook')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'number')))
        OR (evidence_rows.meta_json ? 'events' AND evidence_rows.meta_json->'events' IS NOT NULL AND evidence_rows.meta_json->'events' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'events') = 'object' AND evidence_rows.meta_json->'events' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'array' AND evidence_rows.meta_json->'events' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'events')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'events')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'number')))
        OR (evidence_rows.meta_json ? 'poll_result' AND evidence_rows.meta_json->'poll_result' IS NOT NULL AND evidence_rows.meta_json->'poll_result' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'object' AND evidence_rows.meta_json->'poll_result' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'array' AND evidence_rows.meta_json->'poll_result' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'poll_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'poll_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'number')))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'poll_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'poll_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'event_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'event_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_event_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_event_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,event_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,event_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_event,
      (
        lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_submit_attempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_dispatched', '')), '')) IN ('true','t','yes','y','1')
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR upper(NULLIF(btrim(COALESCE(evidence_rows.transfer_status, '')), '')) IN ('REQUEST_SENT', 'PROVIDER_REQUEST_SENT', 'SUBMITTED', 'SENT', 'PROCESSING', 'ACCEPTED', 'PROVIDER_SUBMITTED', 'SUBMISSION_UNKNOWN', 'REQUEST_SENT_NO_RESPONSE')
      ) AS has_provider_request_sent,
      (
        lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_outcome_unknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_no_response', '')), '')) IN ('true','t','yes','y','1')
        OR upper(NULLIF(btrim(COALESCE(evidence_rows.transfer_status, '')), '')) IN ('UNKNOWN', 'SUBMISSION_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_NO_RESPONSE')
      ) AS has_provider_outcome_unknown
    FROM evidence_rows
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      evidence_rows.transfer_status,
      evidence_rows.rail_state,
      evidence_rows.meta_json,
      evidence_rows.meta_json
    ) AS classification_rows
  )
  SELECT
    (SELECT count(*)::integer FROM target_transfers),
    (SELECT count(*)::integer FROM target_scopes),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_external_id))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_response))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_event))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_request_sent))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_outcome_unknown))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_local_prepare_identity))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_final_money_moved))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_terminal_no_money))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_pending_non_final))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_cash_state = 'UNKNOWN'))::integer, 0)
  INTO
    v_transfer_count,
    v_scope_row_count,
    v_provider_external_id_count,
    v_provider_response_count,
    v_provider_event_count,
    v_provider_request_sent_count,
    v_provider_outcome_unknown_count,
    v_local_prepare_identity_count,
    v_final_paid_count,
    v_terminal_no_money_count,
    v_pending_non_final_count,
    v_unknown_cash_state_count
  FROM classified_evidence_rows;

  WITH operation_payloads AS (
    SELECT
      'OPERATION'::text AS evidence_source,
      operation_rows.id AS operation_id,
      NULL::uuid AS chunk_id,
      payload_rows.payload_name,
      COALESCE(payload_rows.payload_json, '{}'::jsonb) AS payload_json
    FROM public.banking_pay_operations AS operation_rows
    CROSS JOIN LATERAL (VALUES
      ('progress_json'::text, operation_rows.progress_json),
      ('result_json'::text, operation_rows.result_json),
      ('error_json'::text, operation_rows.error_json)
    ) AS payload_rows(payload_name, payload_json)
    WHERE (p_operation_id IS NOT NULL AND operation_rows.id = p_operation_id)
       OR (
         p_operation_id IS NULL
         AND v_effective_pay_batch_id IS NOT NULL
         AND operation_rows.pay_batch_id = v_effective_pay_batch_id
         AND upper(COALESCE(operation_rows.operation_type, '')) LIKE '%PAY%'
       )
    UNION ALL
    SELECT
      'CHUNK'::text AS evidence_source,
      operation_rows.id AS operation_id,
      chunk_rows.id AS chunk_id,
      payload_rows.payload_name,
      COALESCE(payload_rows.payload_json, '{}'::jsonb) AS payload_json
    FROM public.banking_pay_operation_chunks AS chunk_rows
    JOIN public.banking_pay_operations AS operation_rows
      ON operation_rows.id = chunk_rows.operation_id
    CROSS JOIN LATERAL (VALUES
      ('payload_json'::text, chunk_rows.payload_json),
      ('result_json'::text, chunk_rows.result_json),
      ('error_json'::text, chunk_rows.error_json)
    ) AS payload_rows(payload_name, payload_json)
    WHERE (p_operation_id IS NOT NULL AND chunk_rows.operation_id = p_operation_id)
       OR (
         p_operation_id IS NULL
         AND v_effective_pay_batch_id IS NOT NULL
         AND operation_rows.pay_batch_id = v_effective_pay_batch_id
       )
  ), operation_payload_flags AS (
    SELECT
      operation_payloads.evidence_source,
      operation_payloads.operation_id,
      operation_payloads.chunk_id,
      operation_payloads.payload_name,
      operation_payloads.payload_json,
      (
        lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_confirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentConfirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_dispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestDispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_submit_attempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerSubmitAttempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_evidence,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerEvidence,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{outcome,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{outcome,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'provider_request_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'providerRequestSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_call_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'provider_call_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerCallSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'providerCallSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}')::numeric > 0
        )
      ) AS has_provider_request_sent,
      (
        lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_outcome_unknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerOutcomeUnknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_no_response', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentNoResponse', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'error_code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'status', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'outcome', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{error,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
      ) AS has_provider_outcome_unknown,
      (
        (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'rail_tx_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'rail_tx_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'railTxId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'railTxId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_transaction_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_transaction_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerTransactionId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerTransactionId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_payment_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_payment_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerPaymentId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerPaymentId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,paymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,paymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (operation_payloads.payload_json ? 'provider_response' AND operation_payloads.payload_json->'provider_response' IS NOT NULL AND operation_payloads.payload_json->'provider_response' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'object' AND operation_payloads.payload_json->'provider_response' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'array' AND operation_payloads.payload_json->'provider_response' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_response_json' AND operation_payloads.payload_json->'provider_response_json' IS NOT NULL AND operation_payloads.payload_json->'provider_response_json' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'object' AND operation_payloads.payload_json->'provider_response_json' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'array' AND operation_payloads.payload_json->'provider_response_json' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'number')))
        OR (operation_payloads.payload_json ? 'submit_response' AND operation_payloads.payload_json->'submit_response' IS NOT NULL AND operation_payloads.payload_json->'submit_response' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'object' AND operation_payloads.payload_json->'submit_response' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'array' AND operation_payloads.payload_json->'submit_response' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'submit_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'submit_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'number')))
        OR (operation_payloads.payload_json ? 'response_json' AND operation_payloads.payload_json->'response_json' IS NOT NULL AND operation_payloads.payload_json->'response_json' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'response_json') = 'object' AND operation_payloads.payload_json->'response_json' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'array' AND operation_payloads.payload_json->'response_json' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_result' AND operation_payloads.payload_json->'provider_result' IS NOT NULL AND operation_payloads.payload_json->'provider_result' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'object' AND operation_payloads.payload_json->'provider_result' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'array' AND operation_payloads.payload_json->'provider_result' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_payload' AND operation_payloads.payload_json->'provider_payload' IS NOT NULL AND operation_payloads.payload_json->'provider_payload' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'object' AND operation_payloads.payload_json->'provider_payload' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'array' AND operation_payloads.payload_json->'provider_payload' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_payload')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_payload')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'number')))
      ) AS has_provider_payload_evidence
    FROM operation_payloads
  )
  SELECT
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.evidence_source = 'OPERATION' AND operation_payload_flags.has_provider_request_sent))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.has_provider_outcome_unknown))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.has_provider_payload_evidence))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.evidence_source = 'CHUNK' AND operation_payload_flags.has_provider_request_sent))::integer, 0)
  INTO
    v_operation_submit_attempt_count,
    v_operation_submit_unknown_count,
    v_operation_payload_evidence_count,
    v_chunk_submit_attempt_count
  FROM operation_payload_flags;

  v_operation_submit_attempt_count := COALESCE(v_operation_submit_attempt_count, 0) + COALESCE(v_chunk_submit_attempt_count, 0);

  provider_external_id_present := v_provider_external_id_count > 0;
  provider_response_present := v_provider_response_count > 0 OR v_operation_payload_evidence_count > 0;
  provider_event_present := v_provider_event_count > 0;
  provider_request_sent := provider_external_id_present
    OR provider_response_present
    OR provider_event_present
    OR v_provider_request_sent_count > 0
    OR v_operation_submit_attempt_count > 0;
  provider_submitted := provider_request_sent;
  local_prepared_only := (
    provider_submitted = false
    AND (
      v_local_prepare_identity_count > 0
      OR v_transfer_count > 0
      OR v_scope_row_count > 0
    )
  );

  IF v_final_paid_count > 0 THEN
    cash_state := 'FINAL_PAID';
  ELSIF v_terminal_no_money_count > 0 THEN
    cash_state := 'TERMINAL_NO_MONEY';
  ELSIF v_pending_non_final_count > 0 THEN
    cash_state := 'PENDING_NON_FINAL';
  ELSIF v_unknown_cash_state_count > 0 OR provider_request_sent THEN
    cash_state := 'UNKNOWN';
  ELSE
    cash_state := 'NO_TRANSFER_EVIDENCE';
  END IF;

  IF provider_request_sent AND (v_provider_outcome_unknown_count > 0 OR v_operation_submit_unknown_count > 0 OR cash_state = 'UNKNOWN') THEN
    evidence_class := 'PROVIDER_OUTCOME_UNKNOWN';
  ELSIF provider_event_present THEN
    evidence_class := 'PROVIDER_EVENT_PRESENT';
  ELSIF provider_response_present THEN
    evidence_class := 'PROVIDER_RESPONSE_PRESENT';
  ELSIF provider_external_id_present THEN
    evidence_class := 'PROVIDER_EXTERNAL_ID_PRESENT';
  ELSIF provider_request_sent THEN
    evidence_class := 'PROVIDER_REQUEST_SENT';
  ELSIF local_prepared_only THEN
    evidence_class := 'LOCAL_PREPARED_ONLY';
  ELSE
    evidence_class := 'NO_PROVIDER_EVIDENCE';
  END IF;

  blocker_code := CASE
    WHEN evidence_class = 'PROVIDER_OUTCOME_UNKNOWN' OR (provider_submitted AND cash_state = 'UNKNOWN') THEN 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN provider_submitted AND cash_state = 'PENDING_NON_FINAL' THEN 'PROVIDER_CANCELLATION_REQUIRED_BEFORE_UNWIND'
    ELSE NULL::text
  END;

  reason := CASE evidence_class
    WHEN 'PROVIDER_EVENT_PRESENT' THEN 'Provider event/webhook/poll evidence is present.'
    WHEN 'PROVIDER_RESPONSE_PRESENT' THEN 'Provider response evidence is present.'
    WHEN 'PROVIDER_EXTERNAL_ID_PRESENT' THEN 'Provider external transaction/payment ID evidence is present.'
    WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'Provider request may have been sent but final outcome is unknown; check/poll provider before retry or unwind.'
    WHEN 'PROVIDER_REQUEST_SENT' THEN 'Provider request-sent evidence is present.'
    WHEN 'LOCAL_PREPARED_ONLY' THEN 'Only local CloudTMS preparation artefacts are present; no provider submission evidence was found.'
    ELSE 'No provider submission evidence was found.'
  END;

  support_details_json := jsonb_build_object(
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'pay_bank_transfer_id', CASE WHEN p_pay_bank_transfer_id IS NULL THEN NULL ELSE p_pay_bank_transfer_id::text END,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'scope_type', v_scope_type,
    'transfer_ids', COALESCE((SELECT jsonb_agg(transfer_id_values.transfer_id::text ORDER BY transfer_id_values.transfer_id::text) FROM unnest(COALESCE(v_transfer_ids, ARRAY[]::uuid[])) AS transfer_id_values(transfer_id)), '[]'::jsonb),
    'transfer_count', v_transfer_count,
    'scope_row_count', v_scope_row_count,
    'provider_external_id_count', v_provider_external_id_count,
    'provider_response_count', v_provider_response_count,
    'provider_event_count', v_provider_event_count,
    'provider_request_sent_count', v_provider_request_sent_count,
    'provider_outcome_unknown_count', v_provider_outcome_unknown_count,
    'local_prepare_identity_count', v_local_prepare_identity_count,
    'local_prepared_only', local_prepared_only,
    'local_prepared_only_basis', CASE
      WHEN local_prepared_only THEN jsonb_build_array(
        CASE WHEN v_local_prepare_identity_count > 0 THEN 'local_request_or_scope_identity' ELSE NULL END,
        CASE WHEN v_transfer_count > 0 THEN 'local_transfer_row' ELSE NULL END,
        CASE WHEN v_scope_row_count > 0 THEN 'local_transfer_scope_row' ELSE NULL END
      )
      ELSE '[]'::jsonb
    END,
    'final_paid_count', v_final_paid_count,
    'terminal_no_money_count', v_terminal_no_money_count,
    'pending_non_final_count', v_pending_non_final_count,
    'unknown_cash_state_count', v_unknown_cash_state_count,
    'operation_submit_attempt_count', v_operation_submit_attempt_count,
    'operation_submit_unknown_count', v_operation_submit_unknown_count,
    'operation_payload_evidence_count', v_operation_payload_evidence_count,
    'local_artifacts_do_not_count_as_provider_submission', jsonb_build_array('request_id', 'payment_reference', 'bulk_reference', 'operation_id', 'scope_id', 'auth_request_id', 'local_idempotency_key', 'local_only_rail_meta_json', 'pending_local_transfer_rows')
  );

  RETURN NEXT;
END;
$function$;

CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_reserve_for_batch_item(
  p_carry_forward_id uuid,
  p_target_pay_batch_id uuid,
  p_target_pay_batch_item_id uuid,
  p_target_operation_source_key text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_carry_forward_row public.pay_manual_adjustment_carry_forwards%ROWTYPE;
  v_target_item_id uuid := p_target_pay_batch_item_id;
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_target_operation_source_key text := NULL::text;
  v_target_item_pay_batch_id uuid := NULL::uuid;
  v_target_item_candidate_id uuid := NULL::uuid;
  v_target_item_pay_channel text := NULL::text;
  v_target_item_amount_ex_vat numeric := NULL::numeric;
  v_target_item_amount_vat numeric := NULL::numeric;
  v_target_item_amount_inc_vat numeric := NULL::numeric;
  v_target_item_operation_source_key text := NULL::text;
  v_target_item_source_ref text := NULL::text;
  v_target_item_is_voided boolean := false;
  v_existing_target_carry_forward_id uuid := NULL::uuid;
  v_blockers jsonb := '[]'::jsonb;
  v_result_status text := NULL::text;
  v_was_idempotent boolean := false;
BEGIN
  IF p_carry_forward_id IS NULL THEN
    RAISE EXCEPTION 'CARRY_FORWARD_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'CARRY_FORWARD_ID_REQUIRED')::text;
  END IF;

  IF p_target_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ID_REQUIRED', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  IF p_target_pay_batch_item_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ITEM_ID_REQUIRED', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  SELECT carry_forward_rows.*
  INTO v_carry_forward_row
  FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
  WHERE carry_forward_rows.id = p_carry_forward_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CARRY_FORWARD_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'CARRY_FORWARD_NOT_FOUND', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  SELECT
    batch_candidate_rows.pay_batch_id,
    batch_candidate_rows.candidate_id,
    upper(btrim(COALESCE(target_item_rows.pay_channel, ''))),
    target_item_rows.amount_ex_vat,
    target_item_rows.amount_vat,
    target_item_rows.amount_inc_vat,
    target_item_rows.operation_source_key,
    target_item_rows.source_ref,
    COALESCE(target_item_rows.is_voided, false)
  INTO
    v_target_item_pay_batch_id,
    v_target_item_candidate_id,
    v_target_item_pay_channel,
    v_target_item_amount_ex_vat,
    v_target_item_amount_vat,
    v_target_item_amount_inc_vat,
    v_target_item_operation_source_key,
    v_target_item_source_ref,
    v_target_item_is_voided
  FROM public.pay_batch_items AS target_item_rows
  JOIN public.pay_batch_candidates AS batch_candidate_rows
    ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
  WHERE target_item_rows.id = p_target_pay_batch_item_id
  FOR UPDATE OF target_item_rows, batch_candidate_rows;

  IF v_target_item_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ITEM_NOT_FOUND', 'target_pay_batch_item_id', p_target_pay_batch_item_id)::text;
  END IF;

  SELECT existing_target_rows.id
  INTO v_existing_target_carry_forward_id
  FROM public.pay_manual_adjustment_carry_forwards AS existing_target_rows
  WHERE existing_target_rows.target_pay_batch_item_id = p_target_pay_batch_item_id
    AND existing_target_rows.id <> p_carry_forward_id
  FOR UPDATE;

  IF v_existing_target_carry_forward_id IS NOT NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_PAY_BATCH_ITEM_ALREADY_RESERVED_BY_ANOTHER_CARRY_FORWARD',
      'carry_forward_id', p_carry_forward_id::text,
      'existing_carry_forward_id', v_existing_target_carry_forward_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  v_target_operation_source_key := COALESCE(
    NULLIF(btrim(COALESCE(p_target_operation_source_key, '')), ''),
    NULLIF(btrim(COALESCE(v_target_item_operation_source_key, '')), ''),
    'carry_forward:' || p_carry_forward_id::text
  );

  IF v_target_item_pay_batch_id IS DISTINCT FROM p_target_pay_batch_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_ITEM_BATCH_MISMATCH',
      'target_pay_batch_id', p_target_pay_batch_id::text,
      'actual_pay_batch_id', v_target_item_pay_batch_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_target_item_is_voided THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_PAY_BATCH_ITEM_VOIDED',
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_carry_forward_row.status NOT IN ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT') THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_STATUS_NOT_RESERVABLE',
      'carry_forward_id', p_carry_forward_id::text,
      'status', v_carry_forward_row.status
    ));
  END IF;

  IF v_carry_forward_row.status = 'RESERVED_IN_DRAFT'
     AND (
       v_carry_forward_row.target_pay_batch_id IS DISTINCT FROM p_target_pay_batch_id
       OR v_carry_forward_row.target_pay_batch_item_id IS DISTINCT FROM p_target_pay_batch_item_id
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_ALREADY_RESERVED_FOR_ANOTHER_TARGET',
      'carry_forward_id', p_carry_forward_id::text,
      'existing_target_pay_batch_id', CASE WHEN v_carry_forward_row.target_pay_batch_id IS NULL THEN NULL ELSE v_carry_forward_row.target_pay_batch_id::text END,
      'existing_target_pay_batch_item_id', CASE WHEN v_carry_forward_row.target_pay_batch_item_id IS NULL THEN NULL ELSE v_carry_forward_row.target_pay_batch_item_id::text END,
      'requested_target_pay_batch_id', p_target_pay_batch_id::text,
      'requested_target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_carry_forward_row.status = 'RESERVED_IN_DRAFT'
     AND v_carry_forward_row.target_pay_batch_id IS NOT DISTINCT FROM p_target_pay_batch_id
     AND v_carry_forward_row.target_pay_batch_item_id IS NOT DISTINCT FROM p_target_pay_batch_item_id THEN
    v_was_idempotent := true;
  END IF;

  IF v_carry_forward_row.candidate_id IS DISTINCT FROM v_target_item_candidate_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_CANDIDATE_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'carry_forward_candidate_id', v_carry_forward_row.candidate_id::text,
      'target_candidate_id', CASE WHEN v_target_item_candidate_id IS NULL THEN NULL ELSE v_target_item_candidate_id::text END
    ));
  END IF;

  IF upper(btrim(COALESCE(v_carry_forward_row.pay_channel, ''))) IS DISTINCT FROM v_target_item_pay_channel THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_PAY_CHANNEL_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'carry_forward_pay_channel', v_carry_forward_row.pay_channel,
      'target_pay_channel', v_target_item_pay_channel
    ));
  END IF;

  IF v_target_item_amount_inc_vat IS NULL
     OR round(v_target_item_amount_inc_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_inc_vat, 2)
     OR (
       v_carry_forward_row.amount_ex_vat IS NULL
       AND v_target_item_amount_ex_vat IS NOT NULL
     )
     OR (
       v_carry_forward_row.amount_ex_vat IS NOT NULL
       AND (
         v_target_item_amount_ex_vat IS NULL
         OR round(v_target_item_amount_ex_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_ex_vat, 2)
       )
     )
     OR (
       v_carry_forward_row.amount_vat IS NULL
       AND v_target_item_amount_vat IS NOT NULL
     )
     OR (
       v_carry_forward_row.amount_vat IS NOT NULL
       AND (
         v_target_item_amount_vat IS NULL
         OR round(v_target_item_amount_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_vat, 2)
       )
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_AMOUNT_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
      'carry_forward_amount_ex_vat', v_carry_forward_row.amount_ex_vat,
      'carry_forward_amount_vat', v_carry_forward_row.amount_vat,
      'carry_forward_amount_inc_vat', v_carry_forward_row.amount_inc_vat,
      'target_amount_ex_vat', v_target_item_amount_ex_vat,
      'target_amount_vat', v_target_item_amount_vat,
      'target_amount_inc_vat', v_target_item_amount_inc_vat,
      'signed_amount_convention', 'SIGNED_AMOUNTS'
    ));
  END IF;

  IF COALESCE(jsonb_array_length(v_blockers), 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'reserved', false,
      'idempotent', false,
      'carry_forward_id', p_carry_forward_id::text,
      'target_pay_batch_id', p_target_pay_batch_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
      'blockers', v_blockers,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true
    );
  END IF;

  UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
  SET
    target_pay_batch_id = p_target_pay_batch_id,
    target_pay_batch_item_id = p_target_pay_batch_item_id,
    target_operation_source_key = v_target_operation_source_key,
    status = 'RESERVED_IN_DRAFT',
    reserved_at_utc = COALESCE(carry_forward_rows.reserved_at_utc, now()),
    released_at_utc = NULL,
    cancelled_at_utc = NULL,
    status_reason = CASE
      WHEN v_was_idempotent THEN COALESCE(carry_forward_rows.status_reason, 'Already reserved for this target item.')
      ELSE 'Reserved in draft target pay_batch_item.'
    END,
    updated_at_utc = now()
  WHERE carry_forward_rows.id = p_carry_forward_id
  RETURNING carry_forward_rows.status
  INTO v_result_status;

  RETURN jsonb_build_object(
    'ok', true,
    'reserved', true,
    'idempotent', v_was_idempotent,
    'carry_forward_id', p_carry_forward_id::text,
    'target_pay_batch_id', p_target_pay_batch_id::text,
    'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
    'target_operation_source_key', v_target_operation_source_key,
    'status', COALESCE(v_result_status, 'RESERVED_IN_DRAFT'),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_release_for_scope(
  p_target_pay_batch_id uuid DEFAULT NULL::uuid,
  p_resolved_scope_json jsonb DEFAULT NULL::jsonb,
  p_target_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[],
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_reason text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_scope_item_ids uuid[] := COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_released_ids jsonb := '[]'::jsonb;
  v_blocked_items jsonb := '[]'::jsonb;
  v_candidate_count integer := 0;
  v_released_count integer := 0;
  v_blocked_count integer := 0;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_target_pay_batch_id IS NULL
     AND v_scope_json IS NOT NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_target_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;

    v_scope_item_ids := COALESCE(v_scope_item_ids, ARRAY[]::uuid[]) || COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);

    SELECT COALESCE(array_agg(DISTINCT item_id_values.item_id) FILTER (WHERE item_id_values.item_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM unnest(COALESCE(v_scope_item_ids, ARRAY[]::uuid[])) AS item_id_values(item_id);

    WITH raw_values AS (
      SELECT transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_scope_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  IF v_target_pay_batch_id IS NULL AND COALESCE(array_length(v_scope_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED')::text;
  END IF;

  PERFORM 1
  FROM public.pay_manual_adjustment_carry_forwards AS lock_carry_forward_rows
  WHERE lock_carry_forward_rows.status = 'RESERVED_IN_DRAFT'
    AND (
      (v_target_pay_batch_id IS NOT NULL AND lock_carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
      OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND lock_carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
    )
  ORDER BY lock_carry_forward_rows.id
  FOR UPDATE;

  WITH candidate_rows AS (
    SELECT
      carry_forward_rows.id AS carry_forward_id,
      carry_forward_rows.target_pay_batch_id,
      carry_forward_rows.target_pay_batch_item_id,
      carry_forward_rows.status,
      carry_forward_rows.amount_ex_vat,
      carry_forward_rows.amount_vat,
      carry_forward_rows.amount_inc_vat,
      target_item_rows.pay_bank_transfer_id,
      target_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_item_rows.amount_vat AS target_amount_vat,
      target_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_item_rows.is_voided AS target_is_voided,
      batch_candidate_rows.settlement_status,
      batch_candidate_rows.settled_at_utc,
      target_transfer_rows.status AS transfer_status,
      target_transfer_rows.rail_state,
      target_transfer_rows.completed_at_utc,
      target_transfer_rows.rail_meta_json,
      classifier_rows.cash_state,
      classifier_rows.is_final_money_moved
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN public.pay_batch_items AS target_item_rows
      ON target_item_rows.id = carry_forward_rows.target_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS target_transfer_rows
      ON target_transfer_rows.id = target_item_rows.pay_bank_transfer_id
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      target_transfer_rows.status,
      target_transfer_rows.rail_state,
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb),
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb)
    ) AS classifier_rows
    WHERE carry_forward_rows.status = 'RESERVED_IN_DRAFT'
      AND (
        (v_target_pay_batch_id IS NOT NULL AND carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
        OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
        OR (COALESCE(array_length(v_scope_transfer_ids, 1), 0) > 0 AND target_item_rows.pay_bank_transfer_id = ANY(v_scope_transfer_ids))
      )
  ), classified_rows AS (
    SELECT
      candidate_rows.*,
      (
        COALESCE(candidate_rows.is_final_money_moved, false)
        OR upper(COALESCE(candidate_rows.cash_state, '')) = 'FINAL_PAID'
        OR upper(COALESCE(candidate_rows.settlement_status, '')) = 'SETTLED'
        OR candidate_rows.settled_at_utc IS NOT NULL
      ) AS has_final_paid_or_settled_evidence
    FROM candidate_rows
  ), blocked_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.has_final_paid_or_settled_evidence
  ), releasable_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.has_final_paid_or_settled_evidence = false
  ), updated_rows AS (
    UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_update_rows
    SET
      status = 'PENDING_CARRY_FORWARD',
      target_pay_batch_id = NULL,
      target_pay_batch_item_id = NULL,
      target_operation_source_key = NULL,
      reserved_at_utc = NULL,
      released_at_utc = now(),
      status_reason = COALESCE(NULLIF(btrim(p_reason), ''), 'Released because target payment scope was cancelled or unwound before money moved.'),
      updated_at_utc = now()
    FROM releasable_rows
    WHERE carry_forward_update_rows.id = releasable_rows.carry_forward_id
    RETURNING carry_forward_update_rows.id
  )
  SELECT
    (SELECT count(*)::integer FROM candidate_rows),
    (SELECT count(*)::integer FROM updated_rows),
    (SELECT count(*)::integer FROM blocked_rows),
    COALESCE((SELECT jsonb_agg(updated_rows.id::text ORDER BY updated_rows.id::text) FROM updated_rows), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'code', 'CARRY_FORWARD_TARGET_HAS_FINAL_PAID_OR_SETTLED_EVIDENCE',
        'carry_forward_id', blocked_rows.carry_forward_id::text,
        'target_pay_batch_id', CASE WHEN blocked_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_id::text END,
        'target_pay_batch_item_id', CASE WHEN blocked_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_item_id::text END,
        'pay_bank_transfer_id', CASE WHEN blocked_rows.pay_bank_transfer_id IS NULL THEN NULL ELSE blocked_rows.pay_bank_transfer_id::text END,
        'cash_state', blocked_rows.cash_state,
        'transfer_status', blocked_rows.transfer_status,
        'settlement_status', blocked_rows.settlement_status,
        'settled_at_utc', blocked_rows.settled_at_utc,
        'completed_at_utc', blocked_rows.completed_at_utc
      ) ORDER BY blocked_rows.carry_forward_id::text)
      FROM blocked_rows
    ), '[]'::jsonb)
  INTO
    v_candidate_count,
    v_released_count,
    v_blocked_count,
    v_released_ids,
    v_blocked_items;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_blocked_count, 0) = 0,
    'target_pay_batch_id', CASE WHEN v_target_pay_batch_id IS NULL THEN NULL ELSE v_target_pay_batch_id::text END,
    'candidate_count', COALESCE(v_candidate_count, 0),
    'released_count', COALESCE(v_released_count, 0),
    'released_carry_forward_ids', COALESCE(v_released_ids, '[]'::jsonb),
    'blocked_count', COALESCE(v_blocked_count, 0),
    'blockers', COALESCE(v_blocked_items, '[]'::jsonb),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_mark_consumed(
  p_target_pay_batch_id uuid DEFAULT NULL::uuid,
  p_target_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[],
  p_resolved_scope_json jsonb DEFAULT NULL::jsonb,
  p_final_paid_evidence_json jsonb DEFAULT NULL::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_scope_item_ids uuid[] := COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_explicit_final_paid boolean := false;
  v_candidate_count integer := 0;
  v_consumed_count integer := 0;
  v_existing_consumed_count integer := 0;
  v_blocked_count integer := 0;
  v_consumed_ids jsonb := '[]'::jsonb;
  v_existing_consumed_ids jsonb := '[]'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_target_pay_batch_id IS NULL
     AND v_scope_json IS NOT NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_target_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;

    v_scope_item_ids := COALESCE(v_scope_item_ids, ARRAY[]::uuid[]) || COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);

    SELECT COALESCE(array_agg(DISTINCT item_id_values.item_id) FILTER (WHERE item_id_values.item_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM unnest(COALESCE(v_scope_item_ids, ARRAY[]::uuid[])) AS item_id_values(item_id);

    WITH raw_values AS (
      SELECT transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_scope_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  IF v_target_pay_batch_id IS NULL AND COALESCE(array_length(v_scope_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED')::text;
  END IF;

  IF p_final_paid_evidence_json IS NOT NULL AND jsonb_typeof(p_final_paid_evidence_json) = 'object' THEN
    v_explicit_final_paid := (
      lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'final_paid', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'is_final_money_moved', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'money_moved', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'payment_made', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'settled', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR upper(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'cash_state', '')), '')) = 'FINAL_PAID'
    );
  END IF;

  PERFORM 1
  FROM public.pay_manual_adjustment_carry_forwards AS lock_carry_forward_rows
  WHERE lock_carry_forward_rows.status IN ('RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH')
    AND (
      (v_target_pay_batch_id IS NOT NULL AND lock_carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
      OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND lock_carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
    )
  ORDER BY lock_carry_forward_rows.id
  FOR UPDATE;

  WITH candidate_rows AS (
    SELECT
      carry_forward_rows.id AS carry_forward_id,
      carry_forward_rows.status AS carry_forward_status,
      carry_forward_rows.target_pay_batch_id,
      carry_forward_rows.target_pay_batch_item_id,
      carry_forward_rows.amount_ex_vat,
      carry_forward_rows.amount_vat,
      carry_forward_rows.amount_inc_vat,
      target_item_rows.pay_bank_transfer_id,
      target_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_item_rows.amount_vat AS target_amount_vat,
      target_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_item_rows.is_voided AS target_is_voided,
      batch_candidate_rows.settlement_status,
      batch_candidate_rows.settled_at_utc,
      target_transfer_rows.status AS transfer_status,
      target_transfer_rows.rail_state,
      target_transfer_rows.completed_at_utc,
      target_transfer_rows.rail_meta_json,
      classifier_rows.cash_state,
      classifier_rows.is_final_money_moved
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN public.pay_batch_items AS target_item_rows
      ON target_item_rows.id = carry_forward_rows.target_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS target_transfer_rows
      ON target_transfer_rows.id = target_item_rows.pay_bank_transfer_id
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      target_transfer_rows.status,
      target_transfer_rows.rail_state,
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb),
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb)
    ) AS classifier_rows
    WHERE carry_forward_rows.status IN ('RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH')
      AND (
        (v_target_pay_batch_id IS NOT NULL AND carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
        OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
        OR (COALESCE(array_length(v_scope_transfer_ids, 1), 0) > 0 AND target_item_rows.pay_bank_transfer_id = ANY(v_scope_transfer_ids))
      )
  ), classified_rows AS (
    SELECT
      candidate_rows.*,
      (
        COALESCE(candidate_rows.is_final_money_moved, false)
        OR upper(COALESCE(candidate_rows.cash_state, '')) = 'FINAL_PAID'
        OR upper(COALESCE(candidate_rows.settlement_status, '')) = 'SETTLED'
        OR candidate_rows.settled_at_utc IS NOT NULL
        OR COALESCE(v_explicit_final_paid, false)
      ) AS has_final_paid_or_settled_evidence,
      (
        candidate_rows.target_pay_batch_item_id IS NOT NULL
        AND candidate_rows.target_amount_inc_vat IS NOT NULL
        AND round(candidate_rows.target_amount_inc_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_inc_vat, 2)
        AND (
          (candidate_rows.amount_ex_vat IS NULL AND candidate_rows.target_amount_ex_vat IS NULL)
          OR (candidate_rows.amount_ex_vat IS NOT NULL AND candidate_rows.target_amount_ex_vat IS NOT NULL AND round(candidate_rows.target_amount_ex_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_ex_vat, 2))
        )
        AND (
          (candidate_rows.amount_vat IS NULL AND candidate_rows.target_amount_vat IS NULL)
          OR (candidate_rows.amount_vat IS NOT NULL AND candidate_rows.target_amount_vat IS NOT NULL AND round(candidate_rows.target_amount_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_vat, 2))
        )
      ) AS target_amount_matches
    FROM candidate_rows
  ), existing_consumed_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.carry_forward_status = 'CONSUMED_IN_BATCH'
  ), consumable_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.carry_forward_status = 'RESERVED_IN_DRAFT'
      AND classified_rows.has_final_paid_or_settled_evidence
      AND classified_rows.target_amount_matches
  ), blocked_rows AS (
    SELECT classified_rows.*,
      CASE
        WHEN classified_rows.carry_forward_status <> 'RESERVED_IN_DRAFT' THEN 'CARRY_FORWARD_STATUS_NOT_RESERVED'
        WHEN classified_rows.target_pay_batch_item_id IS NULL THEN 'CARRY_FORWARD_TARGET_ITEM_MISSING'
        WHEN classified_rows.target_amount_matches = false THEN 'CARRY_FORWARD_TARGET_AMOUNT_MISMATCH'
        WHEN classified_rows.has_final_paid_or_settled_evidence = false THEN 'FINAL_PAID_EVIDENCE_REQUIRED'
        ELSE 'CARRY_FORWARD_NOT_CONSUMABLE'
      END AS blocker_code
    FROM classified_rows
    WHERE classified_rows.carry_forward_status <> 'CONSUMED_IN_BATCH'
      AND NOT (
        classified_rows.carry_forward_status = 'RESERVED_IN_DRAFT'
        AND classified_rows.has_final_paid_or_settled_evidence
        AND classified_rows.target_amount_matches
      )
  ), updated_rows AS (
    UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_update_rows
    SET
      status = 'CONSUMED_IN_BATCH',
      consumed_at_utc = COALESCE(carry_forward_update_rows.consumed_at_utc, now()),
      status_reason = 'Consumed after final paid/settled evidence for target payment.',
      updated_at_utc = now()
    FROM consumable_rows
    WHERE carry_forward_update_rows.id = consumable_rows.carry_forward_id
    RETURNING carry_forward_update_rows.id
  )
  SELECT
    (SELECT count(*)::integer FROM candidate_rows),
    (SELECT count(*)::integer FROM updated_rows),
    (SELECT count(*)::integer FROM existing_consumed_rows),
    (SELECT count(*)::integer FROM blocked_rows),
    COALESCE((SELECT jsonb_agg(updated_rows.id::text ORDER BY updated_rows.id::text) FROM updated_rows), '[]'::jsonb),
    COALESCE((SELECT jsonb_agg(existing_consumed_rows.carry_forward_id::text ORDER BY existing_consumed_rows.carry_forward_id::text) FROM existing_consumed_rows), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'code', blocked_rows.blocker_code,
        'carry_forward_id', blocked_rows.carry_forward_id::text,
        'status', blocked_rows.carry_forward_status,
        'target_pay_batch_id', CASE WHEN blocked_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_id::text END,
        'target_pay_batch_item_id', CASE WHEN blocked_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_item_id::text END,
        'cash_state', blocked_rows.cash_state,
        'transfer_status', blocked_rows.transfer_status,
        'settlement_status', blocked_rows.settlement_status,
        'settled_at_utc', blocked_rows.settled_at_utc,
        'completed_at_utc', blocked_rows.completed_at_utc,
        'target_amount_matches', blocked_rows.target_amount_matches,
        'signed_amount_convention', 'SIGNED_AMOUNTS'
      ) ORDER BY blocked_rows.carry_forward_id::text)
      FROM blocked_rows
    ), '[]'::jsonb)
  INTO
    v_candidate_count,
    v_consumed_count,
    v_existing_consumed_count,
    v_blocked_count,
    v_consumed_ids,
    v_existing_consumed_ids,
    v_blockers;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_blocked_count, 0) = 0,
    'target_pay_batch_id', CASE WHEN v_target_pay_batch_id IS NULL THEN NULL ELSE v_target_pay_batch_id::text END,
    'candidate_count', COALESCE(v_candidate_count, 0),
    'consumed_count', COALESCE(v_consumed_count, 0),
    'existing_consumed_count', COALESCE(v_existing_consumed_count, 0),
    'consumed_carry_forward_ids', COALESCE(v_consumed_ids, '[]'::jsonb),
    'existing_consumed_carry_forward_ids', COALESCE(v_existing_consumed_ids, '[]'::jsonb),
    'blocked_count', COALESCE(v_blocked_count, 0),
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'explicit_final_paid_evidence', COALESCE(v_explicit_final_paid, false),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_freshness_check(
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_candidate_ids uuid[] DEFAULT NULL::uuid[],
  p_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[],
  p_resolved_scope_json jsonb DEFAULT NULL::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_candidate_ids uuid[] := COALESCE(p_candidate_ids, ARRAY[]::uuid[]);
  v_pay_batch_item_ids uuid[] := COALESCE(p_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_blockers jsonb := '[]'::jsonb;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_carry_forward_ids_json jsonb := '[]'::jsonb;
  v_blocker_count integer := 0;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_effective_pay_batch_id IS NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_effective_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_effective_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  WITH raw_candidate_values AS (
    SELECT candidate_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'candidate_ids') = 'array' THEN v_scope_json->'candidate_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,candidate_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,candidate_ids}'
        ELSE '[]'::jsonb
      END
    ) AS candidate_values(raw_value)
  ), raw_item_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), all_candidates AS (
    SELECT candidate_array_values.candidate_id AS candidate_id
    FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS candidate_array_values(candidate_id)
    WHERE candidate_array_values.candidate_id IS NOT NULL
    UNION ALL
    SELECT NULLIF(btrim(raw_candidate_values.raw_value), '')::uuid AS candidate_id
    FROM raw_candidate_values
    WHERE NULLIF(btrim(raw_candidate_values.raw_value), '') ~ v_uuid_regex
  ), all_items AS (
    SELECT item_array_values.item_id AS pay_batch_item_id
    FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS item_array_values(item_id)
    WHERE item_array_values.item_id IS NOT NULL
    UNION ALL
    SELECT NULLIF(btrim(raw_item_values.raw_value), '')::uuid AS pay_batch_item_id
    FROM raw_item_values
    WHERE NULLIF(btrim(raw_item_values.raw_value), '') ~ v_uuid_regex
  )
  SELECT
    COALESCE((SELECT array_agg(DISTINCT all_candidates.candidate_id) FROM all_candidates WHERE all_candidates.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE((SELECT array_agg(DISTINCT all_items.pay_batch_item_id) FROM all_items WHERE all_items.pay_batch_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_candidate_ids, v_pay_batch_item_ids;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0
     AND COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0 THEN
    SELECT COALESCE(array_agg(DISTINCT batch_candidate_rows.candidate_id) FILTER (WHERE batch_candidate_rows.candidate_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM public.pay_batch_candidates AS batch_candidate_rows
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
     AND COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    SELECT COALESCE(array_agg(DISTINCT item_rows.id), ARRAY[]::uuid[])
    INTO v_pay_batch_item_ids
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  WITH target_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      batch_candidate_rows.pay_batch_id,
      batch_candidate_rows.candidate_id,
      item_rows.pay_channel,
      item_rows.operation_source_key,
      item_rows.source_ref,
      item_rows.amount_ex_vat,
      item_rows.amount_vat,
      item_rows.amount_inc_vat,
      item_rows.is_voided
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
        OR item_rows.id = ANY(v_pay_batch_item_ids)
      )
      AND (
        COALESCE(array_length(v_candidate_ids, 1), 0) = 0
        OR batch_candidate_rows.candidate_id = ANY(v_candidate_ids)
      )
  ), relevant_carry_forwards AS (
    SELECT DISTINCT carry_forward_rows.*
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN target_items AS target_item_rows
      ON target_item_rows.pay_batch_item_id = carry_forward_rows.target_pay_batch_item_id
      OR lower(COALESCE(target_item_rows.operation_source_key, '')) = 'carry_forward:' || carry_forward_rows.id::text
      OR lower(COALESCE(target_item_rows.source_ref, '')) = 'carry_forward:' || carry_forward_rows.id::text
    WHERE carry_forward_rows.target_pay_batch_id = v_effective_pay_batch_id
       OR target_item_rows.pay_batch_item_id IS NOT NULL
       OR (
         COALESCE(array_length(v_candidate_ids, 1), 0) > 0
         AND carry_forward_rows.candidate_id = ANY(COALESCE(v_candidate_ids, ARRAY[]::uuid[]))
         AND carry_forward_rows.status IN ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH', 'CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW')
       )
  ), target_item_rows AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      target_item_rows.pay_batch_item_id AS target_pay_batch_item_id,
      target_item_rows.pay_batch_id AS target_pay_batch_id,
      target_item_rows.candidate_id AS target_candidate_id,
      target_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_item_rows.amount_vat AS target_amount_vat,
      target_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_item_rows.is_voided AS target_is_voided,
      target_item_rows.source_ref AS target_source_ref,
      target_item_rows.operation_source_key AS target_operation_source_key
    FROM relevant_carry_forwards
    LEFT JOIN target_items AS target_item_rows
      ON target_item_rows.pay_batch_item_id = relevant_carry_forwards.target_pay_batch_item_id
      OR lower(COALESCE(target_item_rows.operation_source_key, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
      OR lower(COALESCE(target_item_rows.source_ref, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
  ), source_item_rows AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      source_item_rows.id AS source_pay_batch_item_id,
      source_item_rows.amount_ex_vat AS source_amount_ex_vat,
      source_item_rows.amount_vat AS source_amount_vat,
      source_item_rows.amount_inc_vat AS source_amount_inc_vat,
      source_item_rows.pay_bank_transfer_id AS source_pay_bank_transfer_id,
      source_candidate_rows.settlement_status AS source_settlement_status,
      source_candidate_rows.settled_at_utc AS source_settled_at_utc,
      source_transfer_rows.status AS source_transfer_status,
      source_transfer_rows.rail_state AS source_transfer_rail_state,
      COALESCE(source_transfer_rows.rail_meta_json, '{}'::jsonb) AS source_transfer_meta_json
    FROM relevant_carry_forwards
    LEFT JOIN public.pay_batch_items AS source_item_rows
      ON source_item_rows.id = relevant_carry_forwards.source_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS source_candidate_rows
      ON source_candidate_rows.id = source_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS source_transfer_rows
      ON source_transfer_rows.id = source_item_rows.pay_bank_transfer_id
  ), source_classified_rows AS (
    SELECT
      source_item_rows.*,
      COALESCE(source_classification_rows.is_final_money_moved, false) AS source_is_final_money_moved
    FROM source_item_rows
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      source_item_rows.source_transfer_status,
      source_item_rows.source_transfer_rail_state,
      source_item_rows.source_transfer_meta_json,
      source_item_rows.source_transfer_meta_json
    ) AS source_classification_rows ON source_item_rows.source_pay_bank_transfer_id IS NOT NULL
  ), checked_carry_forwards AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      relevant_carry_forwards.status AS carry_forward_status,
      relevant_carry_forwards.candidate_id,
      relevant_carry_forwards.pay_channel,
      relevant_carry_forwards.amount_ex_vat AS carry_forward_amount_ex_vat,
      relevant_carry_forwards.amount_vat AS carry_forward_amount_vat,
      relevant_carry_forwards.amount_inc_vat AS carry_forward_amount_inc_vat,
      relevant_carry_forwards.source_pay_batch_id,
      relevant_carry_forwards.source_pay_batch_item_id,
      relevant_carry_forwards.target_pay_batch_id,
      relevant_carry_forwards.target_pay_batch_item_id,
      target_item_rows.target_pay_batch_item_id AS actual_target_pay_batch_item_id,
      target_item_rows.target_pay_batch_id AS actual_target_pay_batch_id,
      target_item_rows.target_amount_ex_vat,
      target_item_rows.target_amount_vat,
      target_item_rows.target_amount_inc_vat,
      target_item_rows.target_is_voided,
      source_classified_rows.source_pay_batch_item_id AS actual_source_pay_batch_item_id,
      source_classified_rows.source_amount_ex_vat,
      source_classified_rows.source_amount_vat,
      source_classified_rows.source_amount_inc_vat,
      source_classified_rows.source_settlement_status,
      source_classified_rows.source_settled_at_utc,
      source_classified_rows.source_is_final_money_moved,
      EXISTS (
        SELECT 1
        FROM target_items AS represented_target_items
        WHERE represented_target_items.candidate_id = relevant_carry_forwards.candidate_id
          AND upper(btrim(COALESCE(represented_target_items.pay_channel, ''))) = upper(btrim(COALESCE(relevant_carry_forwards.pay_channel, '')))
          AND COALESCE(represented_target_items.is_voided, false) = false
          AND (
            represented_target_items.pay_batch_item_id = relevant_carry_forwards.target_pay_batch_item_id
            OR lower(COALESCE(represented_target_items.operation_source_key, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
            OR lower(COALESCE(represented_target_items.source_ref, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
          )
      ) AS is_represented_in_target_batch
    FROM relevant_carry_forwards
    LEFT JOIN target_item_rows
      ON target_item_rows.carry_forward_id = relevant_carry_forwards.id
    LEFT JOIN source_classified_rows
      ON source_classified_rows.carry_forward_id = relevant_carry_forwards.id
  ), blocker_rows AS (
    SELECT
      checked_carry_forwards.carry_forward_id,
      CASE
        WHEN checked_carry_forwards.carry_forward_status IN ('CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW') THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CHANGED'
        WHEN checked_carry_forwards.carry_forward_status = 'CONSUMED_IN_BATCH'
          AND checked_carry_forwards.target_pay_batch_id IS DISTINCT FROM v_effective_pay_batch_id THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id IS NOT NULL
          AND checked_carry_forwards.target_pay_batch_id IS DISTINCT FROM v_effective_pay_batch_id THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_RESERVED_ELSEWHERE'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND checked_carry_forwards.actual_target_pay_batch_item_id IS NULL THEN 'RESERVED_CARRY_FORWARD_MISSING_FROM_TARGET_BATCH'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND COALESCE(checked_carry_forwards.target_is_voided, false) THEN 'RESERVED_CARRY_FORWARD_TARGET_ITEM_VOIDED'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND (
            round(COALESCE(checked_carry_forwards.carry_forward_amount_ex_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_ex_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_inc_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_inc_vat, 0), 2)
          ) THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_AMOUNT_CHANGED'
        WHEN checked_carry_forwards.actual_source_pay_batch_item_id IS NOT NULL
          AND (
            round(COALESCE(checked_carry_forwards.carry_forward_amount_ex_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_ex_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_inc_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_inc_vat, 0), 2)
          ) THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_SOURCE_AMOUNT_CHANGED'
        WHEN upper(COALESCE(checked_carry_forwards.source_settlement_status, '')) = 'SETTLED'
          OR checked_carry_forwards.source_settled_at_utc IS NOT NULL
          OR COALESCE(checked_carry_forwards.source_is_final_money_moved, false) THEN 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
        WHEN checked_carry_forwards.carry_forward_status = 'PENDING_CARRY_FORWARD'
          AND COALESCE(array_length(v_candidate_ids, 1), 0) > 0
          AND checked_carry_forwards.candidate_id = ANY(COALESCE(v_candidate_ids, ARRAY[]::uuid[]))
          AND checked_carry_forwards.is_represented_in_target_batch = false THEN 'PENDING_CARRY_FORWARD_NOT_INCLUDED_IN_TARGET_BATCH'
        ELSE NULL::text
      END AS blocker_code,
      checked_carry_forwards.*
    FROM checked_carry_forwards
  )
  SELECT
    COALESCE(jsonb_agg(jsonb_build_object(
      'code', blocker_rows.blocker_code,
      'carry_forward_id', blocker_rows.carry_forward_id::text,
      'status', blocker_rows.carry_forward_status,
      'candidate_id', CASE WHEN blocker_rows.candidate_id IS NULL THEN NULL ELSE blocker_rows.candidate_id::text END,
      'pay_channel', blocker_rows.pay_channel,
      'source_pay_batch_id', blocker_rows.source_pay_batch_id::text,
      'source_pay_batch_item_id', blocker_rows.source_pay_batch_item_id::text,
      'target_pay_batch_id', CASE WHEN blocker_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocker_rows.target_pay_batch_id::text END,
      'target_pay_batch_item_id', CASE WHEN blocker_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocker_rows.target_pay_batch_item_id::text END,
      'actual_target_pay_batch_item_id', CASE WHEN blocker_rows.actual_target_pay_batch_item_id IS NULL THEN NULL ELSE blocker_rows.actual_target_pay_batch_item_id::text END,
      'carry_forward_amount_inc_vat', blocker_rows.carry_forward_amount_inc_vat,
      'target_amount_inc_vat', blocker_rows.target_amount_inc_vat
    ) ORDER BY blocker_rows.carry_forward_id::text, blocker_rows.blocker_code) FILTER (WHERE blocker_rows.blocker_code IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(DISTINCT blocker_rows.blocker_code) FILTER (WHERE blocker_rows.blocker_code IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(DISTINCT blocker_rows.carry_forward_id::text) FILTER (WHERE blocker_rows.carry_forward_id IS NOT NULL), '[]'::jsonb)
  INTO v_blockers, v_stale_reasons, v_carry_forward_ids_json
  FROM blocker_rows;

  v_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_blockers, '[]'::jsonb)), 0);

  RETURN jsonb_build_object(
    'ok', v_blocker_count = 0,
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'carry_forward_ids', COALESCE(v_carry_forward_ids_json, '[]'::jsonb),
    'stale_reasons', COALESCE(v_stale_reasons, '[]'::jsonb),
    'support_details_json', jsonb_build_object(
      'pay_batch_id', v_effective_pay_batch_id::text,
      'candidate_ids', COALESCE((SELECT jsonb_agg(candidate_values.candidate_id::text ORDER BY candidate_values.candidate_id::text) FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS candidate_values(candidate_id)), '[]'::jsonb),
      'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(item_values.pay_batch_item_id::text ORDER BY item_values.pay_batch_item_id::text) FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS item_values(pay_batch_item_id)), '[]'::jsonb),
      'blocker_count', v_blocker_count,
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public._pay_batch_provider_submit_preflight_recheck(
  p_pay_batch_id uuid,
  p_scope_json jsonb DEFAULT '{}'::jsonb,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_json jsonb := COALESCE(p_scope_json, '{}'::jsonb);
  v_effective_scope_json jsonb := '{}'::jsonb;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_blockers jsonb := '[]'::jsonb;
  v_provider_evidence_json jsonb := '{}'::jsonb;
  v_provider_submitted boolean := false;
  v_provider_evidence_class text := NULL::text;
  v_provider_cash_state text := NULL::text;
  v_transfer_provider_evidence_count integer := 0;
  v_operation_submit_attempt_evidence_count integer := 0;
  v_batch_status text := NULL::text;
  v_batch_cancelled_at_utc timestamptz := NULL::timestamptz;
  v_batch_execution_commit_state text := NULL::text;
  v_voided_item_count integer := 0;
  v_applied_pre_bank_cancel_count integer := 0;
  v_cancelled_transfer_count integer := 0;
  v_open_pre_bank_cancel_count integer := 0;
  v_other_operation_count integer := 0;
  v_other_chunk_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SCOPE_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  IF v_scope_json IS NULL OR jsonb_typeof(v_scope_json) <> 'object' OR NULLIF(btrim(COALESCE(v_scope_json->>'scope_type', '')), '') IS NULL THEN
    v_effective_scope_json := COALESCE(v_scope_json, '{}'::jsonb) || jsonb_build_object('scope_type', 'BATCH');
  ELSE
    v_effective_scope_json := v_scope_json;
  END IF;

  SELECT
    batch_rows.status,
    batch_rows.cancelled_at_utc,
    batch_rows.execution_commit_state
  INTO
    v_batch_status,
    v_batch_cancelled_at_utc,
    v_batch_execution_commit_state
  FROM public.pay_batches AS batch_rows
  WHERE batch_rows.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_resolved_scope_json := public._pay_resolve_payment_scope_for_cancel_rewind(
    p_pay_batch_id,
    v_effective_scope_json,
    true,
    p_actor_user_id
  );

  WITH raw_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_resolved_scope_json->'pay_batch_item_ids') = 'array' THEN v_resolved_scope_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  WITH raw_values AS (
    SELECT transfer_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_resolved_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_resolved_scope_json->'pay_bank_transfer_ids'
        ELSE '[]'::jsonb
      END
    ) AS transfer_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  IF upper(btrim(COALESCE(v_batch_status, ''))) = 'CANCELLED'
     OR v_batch_cancelled_at_utc IS NOT NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TRANSFER_CANCELLED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'The pay batch is already cancelled before provider submission.',
      'pay_batch_id', p_pay_batch_id::text,
      'batch_status', v_batch_status,
      'cancelled_at_utc', v_batch_cancelled_at_utc
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_voided_item_count
  FROM public.pay_batch_items AS item_rows
  WHERE item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
    AND COALESCE(item_rows.is_voided, false) = true;

  IF COALESCE(v_voided_item_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_ITEMS_VOIDED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more payment items in the provider-submit scope have already been voided.',
      'voided_item_count', v_voided_item_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_applied_pre_bank_cancel_count
  FROM public.pay_payment_correction_items AS correction_item_rows
  WHERE correction_item_rows.pay_batch_id = p_pay_batch_id
    AND correction_item_rows.correction_item_kind = 'PRE_BANK_CANCEL'
    AND upper(btrim(COALESCE(correction_item_rows.status, ''))) = 'APPLIED'
    AND (
      correction_item_rows.pay_batch_item_id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
      OR correction_item_rows.pay_bank_transfer_id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    );

  IF COALESCE(v_applied_pre_bank_cancel_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_ITEMS_VOIDED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more payment items in the provider-submit scope already have an applied PRE_BANK_CANCEL correction.',
      'applied_pre_bank_cancel_count', v_applied_pre_bank_cancel_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_cancelled_transfer_count
  FROM public.pay_bank_transfers AS transfer_rows
  WHERE transfer_rows.id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    AND upper(btrim(COALESCE(transfer_rows.status, ''))) IN (
      'CANCELLED',
      'CANCELED',
      'VOIDED',
      'LOCALLY_CANCELLED',
      'LOCAL_CANCELLED',
      'CANCELLED_BEFORE_BANK_SUBMISSION',
      'CANCELED_BEFORE_BANK_SUBMISSION'
    );

  IF COALESCE(v_cancelled_transfer_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TRANSFER_CANCELLED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more bank transfer rows in the provider-submit scope have already been locally cancelled.',
      'cancelled_transfer_count', v_cancelled_transfer_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_open_pre_bank_cancel_count
  FROM public.pay_payment_correction_requests AS request_rows
  WHERE request_rows.pay_batch_id = p_pay_batch_id
    AND upper(btrim(COALESCE(request_rows.correction_kind, ''))) = 'PRE_BANK_CANCEL'
    AND upper(btrim(COALESCE(request_rows.status, ''))) IN (
      'REQUESTED',
      'PENDING',
      'AUTHORISED',
      'AUTHORIZED',
      'IN_PROGRESS',
      'PROCESSING',
      'EXPANDED'
    );

  IF COALESCE(v_open_pre_bank_cancel_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
      'reason', 'A pre-bank cancellation request exists for this batch/scope, so provider submission must not proceed.',
      'open_pre_bank_cancel_count', v_open_pre_bank_cancel_count
    ));
  END IF;

  SELECT jsonb_build_object(
      'evidence_class', evidence_rows.evidence_class,
      'provider_submitted', evidence_rows.provider_submitted,
      'provider_request_sent', evidence_rows.provider_request_sent,
      'provider_response_present', evidence_rows.provider_response_present,
      'provider_event_present', evidence_rows.provider_event_present,
      'provider_external_id_present', evidence_rows.provider_external_id_present,
      'local_prepared_only', evidence_rows.local_prepared_only,
      'cash_state', evidence_rows.cash_state,
      'blocker_code', evidence_rows.blocker_code,
      'reason', evidence_rows.reason,
      'support_details_json', evidence_rows.support_details_json
    ),
    evidence_rows.provider_submitted,
    evidence_rows.evidence_class,
    evidence_rows.cash_state
  INTO
    v_provider_evidence_json,
    v_provider_submitted,
    v_provider_evidence_class,
    v_provider_cash_state
  FROM public._pay_bank_transfer_provider_evidence_classify(
    p_pay_batch_id,
    NULL::uuid,
    v_effective_scope_json,
    p_operation_id
  ) AS evidence_rows
  LIMIT 1;

  v_transfer_provider_evidence_count :=
    COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_external_id_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_response_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_event_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_request_sent_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_outcome_unknown_count}', '')::integer, 0);

  v_operation_submit_attempt_evidence_count := COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,operation_submit_attempt_count}', '')::integer, 0);

  IF COALESCE(v_provider_submitted, false)
     AND (
       COALESCE(v_transfer_provider_evidence_count, 0) > 0
       OR (p_operation_id IS NULL AND COALESCE(v_operation_submit_attempt_evidence_count, 0) > 0)
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_ALREADY_CLAIMED',
      'reason', 'Provider submission evidence already exists for this payment scope.',
      'evidence_class', v_provider_evidence_class,
      'cash_state', v_provider_cash_state,
      'transfer_provider_evidence_count', v_transfer_provider_evidence_count,
      'operation_submit_attempt_evidence_count', v_operation_submit_attempt_evidence_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_other_operation_count
  FROM public.banking_pay_operations AS operation_rows
  WHERE operation_rows.pay_batch_id = p_pay_batch_id
    AND (p_operation_id IS NULL OR operation_rows.id <> p_operation_id)
    AND upper(btrim(COALESCE(operation_rows.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT')
    AND upper(btrim(COALESCE(operation_rows.status, ''))) IN ('QUEUED', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING', 'IN_PROGRESS', 'REVIEW_REQUIRED');

  SELECT count(*)::integer
  INTO v_other_chunk_count
  FROM public.banking_pay_operation_chunks AS chunk_rows
  JOIN public.banking_pay_operations AS operation_rows
    ON operation_rows.id = chunk_rows.operation_id
  WHERE operation_rows.pay_batch_id = p_pay_batch_id
    AND (p_operation_id IS NULL OR operation_rows.id <> p_operation_id)
    AND upper(btrim(COALESCE(operation_rows.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT')
    AND upper(btrim(COALESCE(chunk_rows.status, ''))) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING');

  IF COALESCE(v_other_operation_count, 0) + COALESCE(v_other_chunk_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_IN_PROGRESS',
      'reason', 'Another payment provider operation/chunk is already in progress for this batch.',
      'other_operation_count', v_other_operation_count,
      'other_chunk_count', v_other_chunk_count
    ));
  END IF;

  RETURN jsonb_build_object(
    'ok', COALESCE(jsonb_array_length(v_blockers), 0) = 0,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'resolved_scope_json', COALESCE(v_resolved_scope_json, '{}'::jsonb),
    'provider_evidence_json', COALESCE(v_provider_evidence_json, '{}'::jsonb),
    'support_details_json', jsonb_build_object(
      'batch_status', v_batch_status,
      'batch_cancelled_at_utc', v_batch_cancelled_at_utc,
      'batch_execution_commit_state', v_batch_execution_commit_state,
      'voided_item_count', COALESCE(v_voided_item_count, 0),
      'applied_pre_bank_cancel_count', COALESCE(v_applied_pre_bank_cancel_count, 0),
      'cancelled_transfer_count', COALESCE(v_cancelled_transfer_count, 0),
      'open_pre_bank_cancel_count', COALESCE(v_open_pre_bank_cancel_count, 0),
      'transfer_provider_evidence_count', COALESCE(v_transfer_provider_evidence_count, 0),
      'operation_submit_attempt_evidence_count', COALESCE(v_operation_submit_attempt_evidence_count, 0),
      'other_operation_count', COALESCE(v_other_operation_count, 0),
      'other_chunk_count', COALESCE(v_other_chunk_count, 0),
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    )
  );
END;
$function$;


