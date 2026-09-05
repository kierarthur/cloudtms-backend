-- DRAFT_CERTIFICATE_CONSUMER_V1 terminal compatibility context.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
--
-- This helper transports already-owned Draft facts into the existing terminal
-- response contract. It makes no payment, tax, VAT, channel, reservation or
-- category decision. Complete selected-constituent arrays are never rebuilt.

CREATE OR REPLACE FUNCTION private.banking_pay_draft_terminal_context_build_v8(
  p_operation_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
SET search_path = ''
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_created_batch_ids uuid[] := ARRAY[]::uuid[];
  v_skipped_batch_ids uuid[] := ARRAY[]::uuid[];
  v_cancelled_batch_ids uuid[] := ARRAY[]::uuid[];
  v_created_batches jsonb := '[]'::jsonb;
  v_patch_results jsonb := '[]'::jsonb;
  v_paye_batch_id uuid;
  v_umbrella_batch_id uuid;
  v_candidate_count integer := 0;
  v_skipped_preview_row_count integer := 0;
  v_clipped_preview_row_count integer := 0;
  v_skipped_item_rows integer := 0;
  v_clipped_item_rows integer := 0;
  v_preview_rows jsonb := '[]'::jsonb;
  v_reservation_availability jsonb := '{}'::jsonb;
  v_replacement_required boolean := false;
  v_replacement_idempotency_key text;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_TERMINAL_CONTEXT_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id;
  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'DRAFT_TERMINAL_CONTEXT_OPERATION_INVALID'
      USING ERRCODE = '55000';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_TERMINAL_CONTEXT_SCOPE_NOT_FROZEN'
      USING ERRCODE = '55000';
  END IF;

  SELECT certificate_row.*
  INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
  WHERE certificate_row.certificate_uuid = v_scope.certificate_uuid;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_TERMINAL_CONTEXT_CERTIFICATE_MISSING'
      USING ERRCODE = '55000';
  END IF;

  SELECT
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'PASS'), ARRAY[]::uuid[]),
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state IN ('SKIPPED_EMPTY_RESERVED','CANCELLED_EMPTY_RESERVED')), ARRAY[]::uuid[]),
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'CANCELLED_EMPTY_RESERVED'), ARRAY[]::uuid[]),
    COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'pay_batch_id', batch_row.pay_batch_id,
        'pay_channel', candidate_scope.resolved_pay_channel)
      ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'PASS'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object('pay_batch_id', batch_row.pay_batch_id)
        || COALESCE(batch_row.post_refresh_result_json, '{}'::jsonb)
      ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'PASS'), '[]'::jsonb),
    (pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'PASS'
        AND candidate_scope.resolved_pay_channel = 'PAYE'))[1],
    (pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state = 'PASS'
        AND candidate_scope.resolved_pay_channel = 'UMBRELLA'))[1],
    COALESCE(pg_catalog.bool_or(batch_row.post_refresh_state = 'REPLACEMENT_REQUIRED'), false)
  INTO v_created_batch_ids, v_skipped_batch_ids, v_cancelled_batch_ids,
       v_created_batches, v_patch_results, v_paye_batch_id, v_umbrella_batch_id,
       v_replacement_required
  FROM private.banking_pay_draft_operation_created_batches_v8 AS batch_row
  JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
    ON candidate_scope.operation_id = batch_row.operation_id
   AND candidate_scope.candidate_scope_ordinal = batch_row.candidate_scope_ordinal
  WHERE batch_row.operation_id = p_operation_id;

  IF pg_catalog.cardinality(v_created_batch_ids) = 0 THEN
    RAISE EXCEPTION 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
      USING ERRCODE = '55000';
  END IF;

  SELECT pg_catalog.count(DISTINCT candidate_scope.candidate_id)::integer
  INTO v_candidate_count
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
  WHERE candidate_scope.operation_id = p_operation_id;

  SELECT
    COALESCE(pg_catalog.sum(CASE
      WHEN COALESCE(public_scope.allocation_basis_json#>>'{reservation_availability,skipped_preview_row_count}', '')
             ~ '^[0-9]+(\.[0-9]+)?$'
      THEN pg_catalog.floor((public_scope.allocation_basis_json#>>'{reservation_availability,skipped_preview_row_count}')::numeric)::integer
      ELSE 0 END), 0)::integer,
    COALESCE(pg_catalog.sum(CASE
      WHEN COALESCE(public_scope.allocation_basis_json#>>'{reservation_availability,clipped_preview_row_count}', '')
             ~ '^[0-9]+(\.[0-9]+)?$'
      THEN pg_catalog.floor((public_scope.allocation_basis_json#>>'{reservation_availability,clipped_preview_row_count}')::numeric)::integer
      ELSE 0 END), 0)::integer,
    COALESCE(pg_catalog.sum(CASE
      WHEN COALESCE(public_scope.allocation_basis_json#>>'{reservation_availability,skipped_item_rows}', '')
             ~ '^[0-9]+(\.[0-9]+)?$'
      THEN pg_catalog.floor((public_scope.allocation_basis_json#>>'{reservation_availability,skipped_item_rows}')::numeric)::integer
      ELSE 0 END), 0)::integer,
    COALESCE(pg_catalog.sum(CASE
      WHEN COALESCE(public_scope.allocation_basis_json#>>'{reservation_availability,clipped_item_rows}', '')
             ~ '^[0-9]+(\.[0-9]+)?$'
      THEN pg_catalog.floor((public_scope.allocation_basis_json#>>'{reservation_availability,clipped_item_rows}')::numeric)::integer
      ELSE 0 END), 0)::integer
  INTO v_skipped_preview_row_count, v_clipped_preview_row_count,
       v_skipped_item_rows, v_clipped_item_rows
  FROM public.banking_pay_operation_candidate_scope AS public_scope
  WHERE public_scope.operation_id = p_operation_id;

  WITH exploded AS (
    SELECT
      public_scope.id AS candidate_scope_id,
      public_scope.pay_batch_id,
      public_scope.pay_channel,
      public_scope.chunk_sequence,
      preview_row.value AS row_json,
      preview_row.ordinality,
      pg_catalog.row_number() OVER (
        PARTITION BY public_scope.id,
          COALESCE(preview_row.value->>'preview_row_id', ''),
          COALESCE(preview_row.value->>'timesheet_id', ''),
          COALESCE(preview_row.value->>'candidate_id', '')
        ORDER BY public_scope.pay_channel, public_scope.chunk_sequence,
          public_scope.id, preview_row.ordinality
      ) AS duplicate_ordinal
    FROM public.banking_pay_operation_candidate_scope AS public_scope
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(CASE
      WHEN pg_catalog.jsonb_typeof(public_scope.allocation_basis_json#>'{reservation_availability,preview_rows}') = 'array'
      THEN public_scope.allocation_basis_json#>'{reservation_availability,preview_rows}'
      ELSE '[]'::jsonb END) WITH ORDINALITY AS preview_row(value, ordinality)
    WHERE public_scope.operation_id = p_operation_id
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(
    exploded.row_json || pg_catalog.jsonb_build_object(
      'candidate_scope_id', exploded.candidate_scope_id,
      'pay_batch_id', exploded.pay_batch_id,
      'pay_channel', COALESCE(exploded.row_json->>'pay_channel', exploded.pay_channel))
    ORDER BY exploded.pay_channel, exploded.chunk_sequence,
      exploded.candidate_scope_id, exploded.ordinality), '[]'::jsonb)
  INTO v_preview_rows
  FROM exploded
  WHERE exploded.duplicate_ordinal = 1;

  v_reservation_availability := pg_catalog.jsonb_build_object(
    'applied', v_skipped_preview_row_count > 0 OR v_clipped_preview_row_count > 0
      OR v_skipped_item_rows > 0 OR v_clipped_item_rows > 0
      OR pg_catalog.jsonb_array_length(v_preview_rows) > 0,
    'skipped_preview_row_count', v_skipped_preview_row_count,
    'clipped_preview_row_count', v_clipped_preview_row_count,
    'skipped_item_rows', v_skipped_item_rows,
    'clipped_item_rows', v_clipped_item_rows,
    'message', CASE WHEN v_skipped_preview_row_count > 0 OR v_clipped_preview_row_count > 0
      OR v_skipped_item_rows > 0 OR v_clipped_item_rows > 0
      OR pg_catalog.jsonb_array_length(v_preview_rows) > 0
      THEN 'Some payments were not added because they are already reserved in another draft batch.'
      ELSE NULL END,
    'preview_rows', v_preview_rows
  );

  SELECT 'DRAFT_CREATE:' || p_operation_id::text || ':BATCHES:' ||
    COALESCE(pg_catalog.string_agg(batch_id::text, ',' ORDER BY ordinal), 'NONE')
  INTO v_replacement_idempotency_key
  FROM pg_catalog.unnest(v_created_batch_ids) WITH ORDINALITY AS batch_rows(batch_id, ordinal);

  RETURN pg_catalog.jsonb_build_object(
    'contract', 'BANKING_PAY_DRAFT_TERMINAL_CONTEXT_V8',
    'ok', true,
    'operation_id', p_operation_id,
    'workbench_session_id', v_operation.workbench_session_id,
    'source_session_id', v_operation.workbench_session_id,
    'source_session_version', v_operation.frozen_source_session_version,
    'source_snapshot_run_id', v_operation.frozen_source_snapshot_run_id,
    'source_session_signature', v_header.session_signature,
    'pay_batch_ids', pg_catalog.to_jsonb(v_created_batch_ids),
    'created_pay_batch_ids', pg_catalog.to_jsonb(v_created_batch_ids),
    'skipped_empty_pay_batch_ids', pg_catalog.to_jsonb(v_skipped_batch_ids),
    'cancelled_empty_pay_batch_ids', pg_catalog.to_jsonb(v_cancelled_batch_ids),
    'primary_pay_batch_id', v_created_batch_ids[1],
    'pay_batch_id', v_created_batch_ids[1],
    'created_batch_count', pg_catalog.cardinality(v_created_batch_ids),
    'created_batches', v_created_batches,
    'paye_pay_batch_id', v_paye_batch_id,
    'umbrella_pay_batch_id', v_umbrella_batch_id,
    'candidate_count', v_candidate_count,
    'reservation_availability', v_reservation_availability,
    'patch_results', v_patch_results,
    'replacement_session_required', v_replacement_required,
    'replacement_idempotency_key', v_replacement_idempotency_key
  );
END;
$function$;

ALTER FUNCTION private.banking_pay_draft_terminal_context_build_v8(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.banking_pay_draft_terminal_context_build_v8(uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.banking_pay_draft_terminal_context_build_v8(uuid) TO postgres;
