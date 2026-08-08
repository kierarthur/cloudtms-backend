-- Operation freshness is defined over the active frozen payment scope.
-- Items already voided before the operation starts are not payable units and
-- must not be seeded only to fail immediately as PAY_BATCH_ITEM_VOIDED.
-- A unit that is active when seeded and becomes voided later is still caught
-- by pay_batch_validate_freshness_chunk, preserving the concurrent-drift gate.

CREATE OR REPLACE FUNCTION public.pay_batch_freshness_scope_seed(
  p_operation_id uuid,
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_cursor_json jsonb DEFAULT NULL::jsonb,
  p_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_pay_batch_id uuid := p_pay_batch_id;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_cursor_candidate_id uuid := NULL::uuid;
  v_cursor_item_id uuid := NULL::uuid;
  v_seeded_count integer := 0;
  v_page_count integer := 0;
  v_has_more boolean := false;
  v_next_cursor jsonb := NULL::jsonb;
  v_last_candidate_id uuid := NULL::uuid;
  v_last_item_id uuid := NULL::uuid;
  v_existing_max_ordinal bigint := 0;
  v_new_unit_count integer := 0;
  v_touched_unit_count integer := 0;
  v_page_scope_hash text := NULL::text;
  v_prev_summary jsonb := '{}'::jsonb;
  v_new_summary jsonb := '{}'::jsonb;
  v_prev_total_units integer := 0;
  v_prev_pending_units integer := 0;
  v_prev_seeded_units integer := 0;
  v_new_total_units integer := 0;
  v_new_pending_units integer := 0;
  v_new_seeded_units integer := 0;
  v_prev_scope_hash text := NULL::text;
  v_new_scope_hash text := NULL::text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_freshness_scope_seed: p_operation_id is required';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batch_freshness_scope_seed operation not found: %', p_operation_id;
  END IF;

  v_pay_batch_id := COALESCE(v_pay_batch_id, v_operation.pay_batch_id);
  IF v_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_freshness_scope_seed: pay_batch_id is required';
  END IF;

  IF v_operation.pay_batch_id IS NOT NULL AND v_operation.pay_batch_id <> v_pay_batch_id THEN
    RAISE EXCEPTION 'pay_batch_freshness_scope_seed operation % belongs to pay batch %, not %', p_operation_id, v_operation.pay_batch_id, v_pay_batch_id;
  END IF;

  PERFORM 1
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batch_freshness_scope_seed pay batch not found: %', v_pay_batch_id;
  END IF;

  IF p_cursor_json IS NOT NULL AND jsonb_typeof(p_cursor_json) = 'object' THEN
    IF COALESCE(p_cursor_json->>'last_candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_cursor_candidate_id := (p_cursor_json->>'last_candidate_id')::uuid;
    END IF;
    IF COALESCE(p_cursor_json->>'last_pay_batch_item_id', p_cursor_json->>'last_item_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_cursor_item_id := COALESCE(p_cursor_json->>'last_pay_batch_item_id', p_cursor_json->>'last_item_id')::uuid;
    END IF;
  END IF;

  SELECT COALESCE(MAX(scope_unit.unit_ordinal), 0)
  INTO v_existing_max_ordinal
  FROM public.banking_pay_operation_scope_units AS scope_unit
  WHERE scope_unit.operation_id = p_operation_id
    AND scope_unit.phase = 'FRESHNESS'
    AND scope_unit.unit_type = 'PAY_BATCH_ITEM';

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_freshness_seed_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_freshness_seed_page ON COMMIT DROP AS
  WITH ordered_items AS (
    SELECT
      batch_candidate.candidate_id,
      batch_item.id AS pay_batch_item_id,
      batch_item.pay_batch_candidate_id,
      batch_item.item_type,
      batch_item.timesheet_id,
      batch_item.source_ref,
      batch_item.description,
      batch_item.amount_ex_vat,
      batch_item.amount_vat,
      batch_item.amount_inc_vat,
      batch_item.pay_channel,
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      batch_item.frozen_component_key_type,
      batch_item.frozen_component_key_value,
      batch_item.frozen_component_snapshot_json,
      batch_item.frozen_source_basis_json,
      batch_item.frozen_component_classification,
      batch_item.frozen_source_amount,
      batch_item.frozen_target_amount_ex_vat,
      batch_item.frozen_target_amount_vat,
      batch_item.frozen_target_amount_inc_vat,
      batch_item.payout_instruction_snapshot_json,
      batch_item.operation_source_key,
      COALESCE(batch_item.is_voided, false) AS is_voided
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = v_pay_batch_id
      AND COALESCE(batch_item.is_voided, false) = false
      AND (
        v_cursor_candidate_id IS NULL
        OR batch_candidate.candidate_id > v_cursor_candidate_id
        OR (batch_candidate.candidate_id = v_cursor_candidate_id AND batch_item.id > COALESCE(v_cursor_item_id, '00000000-0000-0000-0000-000000000000'::uuid))
      )
    ORDER BY batch_candidate.candidate_id, batch_item.id
    LIMIT (v_limit + 1)
  )
  SELECT
    ordered_items.*,
    row_number() OVER (ORDER BY ordered_items.candidate_id, ordered_items.pay_batch_item_id)::integer AS page_ordinal
  FROM ordered_items;

  SELECT COUNT(*)::integer
  INTO v_page_count
  FROM pg_temp.tmp_pay_batch_freshness_seed_page AS page_row;

  v_has_more := COALESCE(v_page_count, 0) > v_limit;

  WITH seed_rows AS (
    SELECT
      page_row.*,
      (v_existing_max_ordinal + page_row.page_ordinal)::bigint AS unit_ordinal,
      policy_key.timesheet_id AS resolved_timesheet_id,
      policy_key.key_type AS resolved_key_type,
      policy_key.key_value AS resolved_key_value,
      policy_key.key_resolution_source,
      policy_key.key_resolution_failure_reason
    FROM pg_temp.tmp_pay_batch_freshness_seed_page AS page_row
    LEFT JOIN LATERAL public._pay_policy_x_resolve_post_draft_economic_key(
      p_pay_batch_item_id => page_row.pay_batch_item_id,
      p_pay_batch_id => v_pay_batch_id,
      p_timesheet_id => page_row.timesheet_id,
      p_item_type => page_row.item_type,
      p_frozen_key_type => page_row.frozen_component_key_type,
      p_frozen_key_value => page_row.frozen_component_key_value,
      p_frozen_component_snapshot_json => COALESCE(page_row.frozen_component_snapshot_json, '{}'::jsonb),
      p_frozen_source_basis_json => COALESCE(page_row.frozen_source_basis_json, '{}'::jsonb),
      p_breakdown_meta_json => '{}'::jsonb,
      p_target_snapshot_json => NULL::jsonb
    ) AS policy_key ON true
    WHERE page_row.page_ordinal <= v_limit
  ), upserted_units AS (
    INSERT INTO public.banking_pay_operation_scope_units (
      operation_id,
      pay_batch_id,
      phase,
      unit_type,
      unit_key,
      unit_payload_json,
      unit_ordinal,
      status,
      chunk_id,
      result_hash,
      error_json,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      p_operation_id,
      v_pay_batch_id,
      'FRESHNESS',
      'PAY_BATCH_ITEM',
      'pay_batch_item:' || seed_rows.pay_batch_item_id::text,
      jsonb_build_object(
        'pay_batch_item_id', seed_rows.pay_batch_item_id::text,
        'pay_batch_candidate_id', seed_rows.pay_batch_candidate_id::text,
        'candidate_id', seed_rows.candidate_id::text,
        'timesheet_id', CASE WHEN seed_rows.timesheet_id IS NULL THEN NULL ELSE seed_rows.timesheet_id::text END,
        'item_type', seed_rows.item_type,
        'source_ref', seed_rows.source_ref,
        'description', seed_rows.description,
        'pay_channel', seed_rows.pay_channel,
        'finance_case_id', CASE WHEN seed_rows.finance_case_id IS NULL THEN NULL ELSE seed_rows.finance_case_id::text END,
        'finance_component_id', CASE WHEN seed_rows.finance_component_id IS NULL THEN NULL ELSE seed_rows.finance_component_id::text END,
        'amounts', jsonb_build_object(
          'amount_ex_vat', seed_rows.amount_ex_vat,
          'amount_vat', seed_rows.amount_vat,
          'amount_inc_vat', seed_rows.amount_inc_vat
        ),
        'economic_key', jsonb_strip_nulls(jsonb_build_object(
          'timesheet_id', CASE WHEN seed_rows.resolved_timesheet_id IS NULL THEN NULL ELSE seed_rows.resolved_timesheet_id::text END,
          'key_type', seed_rows.resolved_key_type,
          'key_value', seed_rows.resolved_key_value,
          'resolution_source', seed_rows.key_resolution_source,
          'failure_reason', seed_rows.key_resolution_failure_reason
        )),
        'frozen', jsonb_strip_nulls(jsonb_build_object(
          'frozen_component_key_type', seed_rows.frozen_component_key_type,
          'frozen_component_key_value', seed_rows.frozen_component_key_value,
          'frozen_component_snapshot_json', seed_rows.frozen_component_snapshot_json,
          'frozen_source_basis_json', seed_rows.frozen_source_basis_json,
          'frozen_component_classification', CASE WHEN seed_rows.frozen_component_classification IS NULL THEN NULL ELSE seed_rows.frozen_component_classification::text END,
          'frozen_source_amount', seed_rows.frozen_source_amount,
          'frozen_target_amount_ex_vat', seed_rows.frozen_target_amount_ex_vat,
          'frozen_target_amount_vat', seed_rows.frozen_target_amount_vat,
          'frozen_target_amount_inc_vat', seed_rows.frozen_target_amount_inc_vat
        )),
        'payout_instruction_snapshot_json', seed_rows.payout_instruction_snapshot_json,
        'operation_source_key', seed_rows.operation_source_key,
        'is_voided', seed_rows.is_voided
      ),
      seed_rows.unit_ordinal,
      'PENDING',
      NULL::uuid,
      NULL::text,
      NULL::jsonb,
      v_now,
      v_now
    FROM seed_rows
    ON CONFLICT (operation_id, phase, unit_type, unit_key)
    DO UPDATE
    SET unit_payload_json = EXCLUDED.unit_payload_json,
        unit_ordinal = LEAST(public.banking_pay_operation_scope_units.unit_ordinal, EXCLUDED.unit_ordinal),
        status = CASE
          WHEN public.banking_pay_operation_scope_units.status IN ('FRESH', 'STALE', 'ERROR') THEN public.banking_pay_operation_scope_units.status
          ELSE 'PENDING'
        END,
        error_json = CASE
          WHEN public.banking_pay_operation_scope_units.status IN ('FRESH', 'STALE') THEN public.banking_pay_operation_scope_units.error_json
          ELSE NULL::jsonb
        END,
        updated_at_utc = v_now
    RETURNING
      (xmax = '0'::xid) AS inserted_row,
      unit_key,
      unit_ordinal
  )
  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE upserted_units.inserted_row)::integer,
    md5(COALESCE(string_agg(upserted_units.unit_key, '|' ORDER BY upserted_units.unit_ordinal, upserted_units.unit_key) FILTER (WHERE upserted_units.inserted_row), 'NO_NEW_SCOPE_UNITS'))
  INTO
    v_touched_unit_count,
    v_new_unit_count,
    v_page_scope_hash
  FROM upserted_units;

  v_seeded_count := COALESCE(v_new_unit_count, 0);

  IF v_has_more THEN
    SELECT jsonb_build_object(
      'last_candidate_id', page_row.candidate_id::text,
      'last_pay_batch_item_id', page_row.pay_batch_item_id::text
    )
    INTO v_next_cursor
    FROM pg_temp.tmp_pay_batch_freshness_seed_page AS page_row
    WHERE page_row.page_ordinal = v_limit;
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  v_prev_summary := CASE
    WHEN jsonb_typeof(COALESCE(v_operation.progress_json, '{}'::jsonb)->'freshness_summary') = 'object'
      THEN COALESCE(v_operation.progress_json->'freshness_summary', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_prev_total_units := COALESCE(
    CASE WHEN COALESCE(v_prev_summary->>'total_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'total_units')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_operation.progress_json->>'freshness_total_units', '') ~ '^[0-9]+$' THEN (v_operation.progress_json->>'freshness_total_units')::integer ELSE NULL::integer END,
    0
  );
  v_prev_pending_units := COALESCE(
    CASE WHEN COALESCE(v_prev_summary->>'pending_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'pending_units')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_operation.progress_json->>'freshness_pending_units', '') ~ '^[0-9]+$' THEN (v_operation.progress_json->>'freshness_pending_units')::integer ELSE NULL::integer END,
    0
  );
  v_prev_seeded_units := COALESCE(
    CASE WHEN COALESCE(v_prev_summary->>'seeded_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'seeded_units')::integer ELSE NULL::integer END,
    v_prev_total_units,
    0
  );
  v_prev_scope_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_prev_summary->>'scope_hash', '')), ''),
    NULLIF(BTRIM(COALESCE(v_operation.progress_json->>'freshness_scope_hash', '')), '')
  );

  v_new_seeded_units := COALESCE(v_prev_seeded_units, 0) + COALESCE(v_new_unit_count, 0);
  v_new_total_units := GREATEST(COALESCE(v_prev_total_units, 0) + COALESCE(v_new_unit_count, 0), v_new_seeded_units);
  v_new_pending_units := COALESCE(v_prev_pending_units, 0) + COALESCE(v_new_unit_count, 0);
  v_new_scope_hash := CASE
    WHEN COALESCE(v_new_unit_count, 0) <= 0 THEN v_prev_scope_hash
    WHEN v_prev_scope_hash IS NULL THEN v_page_scope_hash
    ELSE md5(v_prev_scope_hash || '|seed|' || COALESCE(v_page_scope_hash, 'NO_NEW_SCOPE_UNITS'))
  END;

  v_new_summary := jsonb_strip_nulls(jsonb_build_object(
    'status', 'PENDING',
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_pay_batch_id::text,
    'phase', 'FRESHNESS',
    'unit_type', 'PAY_BATCH_ITEM',
    'seeded_units', COALESCE(v_new_seeded_units, 0),
    'total_units', COALESCE(v_new_total_units, 0),
    'fresh_units', COALESCE(CASE WHEN COALESCE(v_prev_summary->>'fresh_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'fresh_units')::integer ELSE NULL::integer END, 0),
    'stale_units', COALESCE(CASE WHEN COALESCE(v_prev_summary->>'stale_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'stale_units')::integer ELSE NULL::integer END, 0),
    'pending_units', COALESCE(v_new_pending_units, 0),
    'error_units', COALESCE(CASE WHEN COALESCE(v_prev_summary->>'error_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'error_units')::integer ELSE NULL::integer END, 0),
    'scope_hash', v_new_scope_hash,
    'result_hash', NULL::text,
    'stale_reasons', COALESCE(v_prev_summary->'stale_reasons', '[]'::jsonb),
    'diff_samples', COALESCE(v_prev_summary->'diff_samples', '[]'::jsonb),
    'last_seeded_at_utc', v_now::text,
    'last_seeded_count', COALESCE(v_seeded_count, 0),
    'last_touched_count', COALESCE(v_touched_unit_count, 0),
    'seed_has_more', COALESCE(v_has_more, false),
    'seed_next_cursor', v_next_cursor
  ));

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'freshness_summary', v_new_summary,
          'freshness_status', 'PENDING',
          'freshness_total_units', COALESCE(v_new_total_units, 0),
          'freshness_pending_units', COALESCE(v_new_pending_units, 0),
          'freshness_stale_units', COALESCE(CASE WHEN COALESCE(v_prev_summary->>'stale_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'stale_units')::integer ELSE NULL::integer END, 0),
          'freshness_error_units', COALESCE(CASE WHEN COALESCE(v_prev_summary->>'error_units', '') ~ '^[0-9]+$' THEN (v_prev_summary->>'error_units')::integer ELSE NULL::integer END, 0),
          'freshness_scope_hash', v_new_scope_hash,
          'freshness_result_hash', NULL::text
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  UPDATE public.pay_batches AS batch_update
  SET freshness_operation_id = p_operation_id,
      freshness_validation_status = 'PENDING',
      freshness_scope_hash = COALESCE(v_new_scope_hash, batch_update.freshness_scope_hash),
      freshness_result_hash = NULL::text,
      freshness_result_json = jsonb_build_object(
        'ok', true,
        'operation_id', p_operation_id::text,
        'pay_batch_id', v_pay_batch_id::text,
        'phase', 'FRESHNESS',
        'status', 'PENDING',
        'is_stale', false,
        'summary', v_new_summary
      )
  WHERE batch_update.id = v_pay_batch_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_pay_batch_id::text,
    'phase', 'FRESHNESS',
    'unit_type', 'PAY_BATCH_ITEM',
    'seeded_count', COALESCE(v_seeded_count, 0),
    'limit', v_limit,
    'has_more', COALESCE(v_has_more, false),
    'next_cursor', v_next_cursor
  );
END;
$function$;

ALTER FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_freshness_scope_seed(uuid, uuid, jsonb, integer) TO service_role;
