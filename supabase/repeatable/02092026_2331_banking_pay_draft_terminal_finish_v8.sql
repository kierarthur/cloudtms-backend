-- DRAFT_CERTIFICATE_CONSUMER_V1 row-backed terminal adapter.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
--
-- This function preserves banking_pay_operation_finish's accepted DRAFT_CREATE
-- completion effects and response shape.  Its only orchestration difference is
-- that it validates the certified selected universe from normalized V8 rows;
-- it never reconstructs or expands a complete selected-ID JSON array.

CREATE OR REPLACE FUNCTION public.banking_pay_draft_operation_finish_v8(
  p_operation_id uuid,
  p_status text,
  p_result_json jsonb DEFAULT NULL::jsonb,
  p_error_json jsonb DEFAULT NULL::jsonb
)
RETURNS TABLE (
  finished boolean,
  not_finished_reason text,
  operation_id uuid,
  operation_type text,
  status text,
  phase text,
  actor_user_id uuid,
  workbench_session_id uuid,
  pay_batch_id uuid,
  root_operation_id uuid,
  idempotency_key text,
  input_json jsonb,
  config_json jsonb,
  progress_json jsonb,
  result_json jsonb,
  error_json jsonb,
  total_units integer,
  completed_units integer,
  failed_units integer,
  current_chunk_index integer,
  chunk_count integer,
  locked_by text,
  lock_expires_at_utc timestamptz,
  created_at_utc timestamptz,
  started_at_utc timestamptz,
  updated_at_utc timestamptz,
  completed_at_utc timestamptz,
  failed_at_utc timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_status text := pg_catalog.upper(NULLIF(pg_catalog.btrim(COALESCE(p_status, '')), ''));
  v_result_json jsonb := p_result_json;
  v_finish_scope_generation bigint := 0;
  v_finish_relevant_generation bigint := NULL::bigint;
  v_finish_unresolved_root_count integer := 0;
  v_finish_failed_root_count integer := 0;
  v_finish_scope_count integer := 0;
  v_finish_selected_count integer := 0;
  v_finish_scope_invalid_count integer := 0;
  v_finish_scope_hash text := NULL::text;
  v_finish_phase_invalid_count integer := 0;
  v_finish_parity_count integer := 0;
  v_finish_parity_invalid_count integer := 0;
  v_finish_refresh_invalid_count integer := 0;
  v_finish_blocker jsonb := '{}'::jsonb;
  v_finish_scope_status text := 'NONE';
  v_finish_freshness_status text := 'VALID_AT_SCOPE_FREEZE';
  v_post_draft_authority_count integer := 0;
  v_created_batch_ids uuid[] := ARRAY[]::uuid[];
  v_skipped_batch_ids uuid[] := ARRAY[]::uuid[];
  v_cancelled_batch_ids uuid[] := ARRAY[]::uuid[];
  v_created_batches jsonb := '[]'::jsonb;
  v_paye_batch_id uuid := NULL::uuid;
  v_umbrella_batch_id uuid := NULL::uuid;
  v_result_created_ids uuid[] := ARRAY[]::uuid[];
  v_result_pay_ids uuid[] := ARRAY[]::uuid[];
  v_result_created_batch_ids uuid[] := ARRAY[]::uuid[];
  v_result_skipped_ids uuid[] := ARRAY[]::uuid[];
  v_result_cancelled_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_count integer := 0;
  v_expected_replacement_idempotency_key text;
  v_terminal_digest text;
  v_created_ids_digest text;
  v_source_publication_identity_enforce_enabled boolean := false;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout', '15000', true);
  PERFORM pg_catalog.set_config('lock_timeout', '1500', true);

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_FINISH_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023', DETAIL = '{"code":"BANKING_PAY_DRAFT_FINISH_OPERATION_ID_REQUIRED"}';
  END IF;
  IF v_status <> 'COMPLETE' OR p_error_json IS NOT NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_FINISH_COMPLETE_ONLY'
      USING ERRCODE = '22023', DETAIL = '{"code":"BANKING_PAY_DRAFT_FINISH_COMPLETE_ONLY"}';
  END IF;
  IF pg_catalog.jsonb_typeof(v_result_json) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT'
      USING ERRCODE = '22023', DETAIL = '{"code":"BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT"}';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT false, 'NOT_FOUND'::text, p_operation_id,
      NULL::text, NULL::text, NULL::text, NULL::uuid, NULL::uuid, NULL::uuid,
      NULL::uuid, NULL::text, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb,
      NULL::jsonb, NULL::integer, NULL::integer, NULL::integer, NULL::integer,
      NULL::integer, NULL::text, NULL::timestamptz, NULL::timestamptz,
      NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz;
    RETURN;
  END IF;

  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status, '')))
       IN ('COMPLETE','FAILED','CANCELLED','CANCELED') THEN
    RETURN QUERY SELECT false, 'ALREADY_TERMINAL'::text,
      v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase,
      v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id,
      v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json,
      v_operation.config_json, v_operation.progress_json, v_operation.result_json,
      v_operation.error_json, v_operation.total_units, v_operation.completed_units,
      v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count,
      COALESCE(v_operation.lease_owner, v_operation.locked_by),
      COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
      v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc,
      v_operation.completed_at_utc, v_operation.failed_at_utc;
    RETURN;
  END IF;

  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_FINISH_OPERATION_TYPE_INVALID'
      USING ERRCODE = '55000', DETAIL = '{"code":"BANKING_PAY_DRAFT_FINISH_OPERATION_TYPE_INVALID"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;
  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.scope_freeze_status, ''))) <> 'FROZEN'
     OR NOT COALESCE(v_operation.source_scope_seed_complete, false)
     OR v_operation.frozen_scope_change_generation IS NULL
     OR v_operation.scope_frozen_at_utc IS NULL
     OR v_operation.frozen_source_session_version IS NULL
     OR v_operation.frozen_source_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN"}';
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id
  FOR UPDATE;
  IF NOT FOUND OR v_link.certificate_uuid IS DISTINCT FROM v_scope.certificate_uuid
     OR v_link.link_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FROZEN'
      USING ERRCODE = '55000', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FROZEN"}';
  END IF;

  SELECT
    pg_catalog.count(*)::integer,
    (SELECT pg_catalog.count(DISTINCT member.constituent_ordinal)::integer
       FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
      WHERE member.operation_id = p_operation_id),
    pg_catalog.count(*) FILTER (
      WHERE scope_row.pay_batch_id IS NULL
         OR scope_row.workbench_session_id IS DISTINCT FROM v_operation.workbench_session_id
         OR scope_row.source_session_version IS DISTINCT FROM v_operation.frozen_source_session_version
         OR scope_row.source_snapshot_run_id IS DISTINCT FROM v_operation.frozen_source_snapshot_run_id
         OR pg_catalog.upper(pg_catalog.btrim(COALESCE(scope_row.status, ''))) NOT IN ('ALLOCATED','DRAFTED')
         OR NOT EXISTS (
           SELECT 1 FROM public.pay_batches AS batch_row
           WHERE batch_row.id = scope_row.pay_batch_id
             AND batch_row.source_scope_change_generation IS NOT DISTINCT FROM v_operation.frozen_scope_change_generation
             AND batch_row.source_workbench_session_id IS NOT DISTINCT FROM v_operation.workbench_session_id
             AND batch_row.source_session_version IS NOT DISTINCT FROM v_operation.frozen_source_session_version
             AND batch_row.source_snapshot_run_id IS NOT DISTINCT FROM v_operation.frozen_source_snapshot_run_id
         ))::integer,
    pg_catalog.md5(COALESCE(pg_catalog.string_agg(
      scope_row.candidate_id::text || ':' || scope_row.pay_channel || ':' || scope_row.scope_hash,
      '|' ORDER BY scope_row.pay_channel, scope_row.candidate_id), ''))
  INTO v_finish_scope_count, v_finish_selected_count, v_finish_scope_invalid_count, v_finish_scope_hash
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id;

  IF v_finish_scope_count IS DISTINCT FROM v_scope.partition_count
     OR v_finish_scope_count IS DISTINCT FROM v_operation.frozen_candidate_scope_count
     OR v_finish_selected_count IS DISTINCT FROM v_scope.constituent_count
     OR v_finish_selected_count IS DISTINCT FROM v_operation.frozen_selected_row_count
     OR v_finish_scope_invalid_count <> 0
     OR v_finish_scope_hash IS DISTINCT FROM v_operation.frozen_operation_scope_hash THEN
    RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH'
      USING ERRCODE = '55000', DETAIL = pg_catalog.jsonb_build_object(
        'code','DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH',
        'expected_scope_count',v_scope.partition_count,'actual_scope_count',v_finish_scope_count,
        'expected_selected_count',v_scope.constituent_count,'actual_selected_count',v_finish_selected_count,
        'invalid_scope_count',v_finish_scope_invalid_count)::text;
  END IF;

  SELECT pg_catalog.count(*) FILTER (WHERE unit_row.unit_state <> 'COMPLETE')::integer
  INTO v_finish_phase_invalid_count
  FROM private.banking_pay_draft_phase_units_v1 AS unit_row
  WHERE unit_row.operation_id = p_operation_id;
  IF v_finish_phase_invalid_count <> 0
     OR NOT EXISTS (
       SELECT 1 FROM private.banking_pay_draft_phase_units_v1 AS unit_row
       WHERE unit_row.operation_id=p_operation_id AND unit_row.phase='FINALISE_RESERVATIONS'
     ) THEN
    RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_PHASE_UNITS_INCOMPLETE'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_OPERATION_PHASE_UNITS_INCOMPLETE"}';
  END IF;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (WHERE parity_row.comparison_status <> 'MATCH')::integer
  INTO v_finish_parity_count, v_finish_parity_invalid_count
  FROM private.banking_pay_draft_constituent_parity_results_v8 AS parity_row
  WHERE parity_row.operation_id = p_operation_id;
  IF v_finish_parity_count IS DISTINCT FROM v_scope.constituent_count
     OR v_finish_parity_invalid_count <> 0 THEN
    RAISE EXCEPTION 'DRAFT_CREATE_CONSTITUENT_PARITY_INCOMPLETE'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_CONSTITUENT_PARITY_INCOMPLETE"}';
  END IF;

  SELECT 'DRAFT_CREATE:' || p_operation_id::text || ':BATCHES:' || COALESCE(
    pg_catalog.string_agg(batch_row.pay_batch_id::text, ',' ORDER BY batch_row.batch_ordinal),
    'NONE')
  INTO v_expected_replacement_idempotency_key
  FROM private.banking_pay_draft_operation_created_batches_v8 AS batch_row
  WHERE batch_row.operation_id = p_operation_id
    AND batch_row.integrity_state = 'PASS';

  IF EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_operation_created_batches_v8 AS batch_row
    WHERE batch_row.operation_id = p_operation_id
      AND batch_row.integrity_state = 'PASS'
      AND batch_row.post_refresh_state = 'REPLACEMENT_REQUIRED'
  ) THEN
    IF pg_catalog.jsonb_typeof(v_result_json->'replacement_available') <> 'boolean'
       OR (v_result_json->>'replacement_available')::boolean IS NOT TRUE
       OR COALESCE(v_result_json->>'action', '') <> 'ADOPT_REPLACEMENT_SESSION'
       OR COALESCE(v_result_json->>'next_recommended_action', '') <> 'ADOPT_REPLACEMENT_SESSION'
       OR COALESCE(v_result_json->>'replacement_session_id', '')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_result_json->>'replacement_session_version', '') !~ '^[1-9][0-9]*$'
       OR COALESCE(v_result_json->>'replacement_idempotency_key', '')
            IS DISTINCT FROM v_expected_replacement_idempotency_key
       OR pg_catalog.jsonb_typeof(v_result_json->'replacement_adoption_contract') <> 'object'
       OR v_result_json#>>'{replacement_adoption_contract,replacement_session_id}'
            IS DISTINCT FROM v_result_json->>'replacement_session_id'
       OR v_result_json#>>'{replacement_adoption_contract,replacement_session_version}'
            IS DISTINCT FROM v_result_json->>'replacement_session_version'
       OR v_result_json#>>'{replacement_adoption_contract,replacement_idempotency_key}'
            IS DISTINCT FROM v_expected_replacement_idempotency_key
       OR COALESCE(v_result_json#>>'{replacement_adoption_contract,action}', '')
            <> 'ADOPT_REPLACEMENT_SESSION'
       OR pg_catalog.jsonb_typeof(v_result_json->'post_create_refresh') <> 'object'
       OR v_result_json#>>'{post_create_refresh,replacement_session_id}'
            IS DISTINCT FROM v_result_json->>'replacement_session_id' THEN
      RAISE EXCEPTION 'DRAFT_CREATE_REPLACEMENT_TERMINAL_RESULT_MISMATCH'
        USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_REPLACEMENT_TERMINAL_RESULT_MISMATCH"}';
    END IF;

    UPDATE private.banking_pay_draft_operation_created_batches_v8 AS batch_row
    SET post_refresh_state = 'APPLIED_REPLACEMENT'
    WHERE batch_row.operation_id = p_operation_id
      AND batch_row.integrity_state = 'PASS'
      AND batch_row.post_refresh_state = 'REPLACEMENT_REQUIRED';
  ELSIF COALESCE(v_result_json->>'replacement_available', 'false')
          IN ('true','t','1','yes','y','on')
        OR NULLIF(pg_catalog.btrim(COALESCE(v_result_json->>'replacement_session_id', '')), '') IS NOT NULL
        OR COALESCE(v_result_json->>'action', '') = 'ADOPT_REPLACEMENT_SESSION' THEN
    RAISE EXCEPTION 'DRAFT_CREATE_UNREQUESTED_REPLACEMENT_RESULT'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_UNREQUESTED_REPLACEMENT_RESULT"}';
  END IF;

  SELECT pg_catalog.count(*) FILTER (
    WHERE batch_row.integrity_state = 'PASS'
          AND batch_row.post_refresh_state NOT IN ('APPLIED','APPLIED_REPLACEMENT')
       OR batch_row.integrity_state = 'SKIPPED_EMPTY_RESERVED')::integer
  INTO v_finish_refresh_invalid_count
  FROM private.banking_pay_draft_operation_created_batches_v8 AS batch_row
  WHERE batch_row.operation_id = p_operation_id;
  IF v_finish_refresh_invalid_count <> 0 THEN
    RAISE EXCEPTION 'DRAFT_POST_CREATE_REFRESH_INCOMPLETE'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_POST_CREATE_REFRESH_INCOMPLETE"}';
  END IF;

  SELECT
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state='PASS'), ARRAY[]::uuid[]),
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state IN ('SKIPPED_EMPTY_RESERVED','CANCELLED_EMPTY_RESERVED')), ARRAY[]::uuid[]),
    COALESCE(pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state='CANCELLED_EMPTY_RESERVED'), ARRAY[]::uuid[]),
    (pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state='PASS' AND scope_row.resolved_pay_channel='PAYE'))[1],
    (pg_catalog.array_agg(batch_row.pay_batch_id ORDER BY batch_row.batch_ordinal)
      FILTER (WHERE batch_row.integrity_state='PASS' AND scope_row.resolved_pay_channel='UMBRELLA'))[1]
  INTO v_created_batch_ids, v_skipped_batch_ids, v_cancelled_batch_ids,
       v_paye_batch_id, v_umbrella_batch_id
  FROM private.banking_pay_draft_operation_created_batches_v8 AS batch_row
  JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS scope_row
    ON scope_row.operation_id=batch_row.operation_id
   AND scope_row.candidate_scope_ordinal=batch_row.candidate_scope_ordinal
  WHERE batch_row.operation_id=p_operation_id;

  IF pg_catalog.cardinality(v_created_batch_ids) = 0 THEN
    RAISE EXCEPTION 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
      USING ERRCODE = '55000', DETAIL = '{"code":"PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED"}';
  END IF;

  SELECT COALESCE(pg_catalog.array_agg(value::uuid ORDER BY ordinal), ARRAY[]::uuid[])
  INTO v_result_pay_ids
  FROM pg_catalog.jsonb_array_elements_text(COALESCE(v_result_json->'pay_batch_ids','[]'::jsonb))
       WITH ORDINALITY AS values_row(value,ordinal);
  SELECT COALESCE(pg_catalog.array_agg(value::uuid ORDER BY ordinal), ARRAY[]::uuid[])
  INTO v_result_created_ids
  FROM pg_catalog.jsonb_array_elements_text(COALESCE(v_result_json->'created_pay_batch_ids','[]'::jsonb))
       WITH ORDINALITY AS values_row(value,ordinal);
  SELECT COALESCE(pg_catalog.array_agg((value->>'pay_batch_id')::uuid ORDER BY ordinal), ARRAY[]::uuid[])
  INTO v_result_created_batch_ids
  FROM pg_catalog.jsonb_array_elements(COALESCE(v_result_json->'created_batches','[]'::jsonb))
       WITH ORDINALITY AS values_row(value,ordinal);
  SELECT COALESCE(pg_catalog.array_agg(value::uuid ORDER BY ordinal), ARRAY[]::uuid[])
  INTO v_result_skipped_ids
  FROM pg_catalog.jsonb_array_elements_text(COALESCE(v_result_json->'skipped_empty_pay_batch_ids','[]'::jsonb))
       WITH ORDINALITY AS values_row(value,ordinal);
  SELECT COALESCE(pg_catalog.array_agg(value::uuid ORDER BY ordinal), ARRAY[]::uuid[])
  INTO v_result_cancelled_ids
  FROM pg_catalog.jsonb_array_elements_text(COALESCE(v_result_json->'cancelled_empty_pay_batch_ids','[]'::jsonb))
       WITH ORDINALITY AS values_row(value,ordinal);
  SELECT pg_catalog.count(DISTINCT scope_row.candidate_id)::integer
  INTO v_candidate_count
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id=p_operation_id;

  IF COALESCE((v_result_json->>'ok')::boolean,false) IS NOT TRUE
     OR v_result_json->>'operation_id' IS DISTINCT FROM p_operation_id::text
     OR pg_catalog.upper(COALESCE(v_result_json->>'operation_type','')) <> 'DRAFT_CREATE'
     OR v_result_pay_ids IS DISTINCT FROM v_created_batch_ids
     OR v_result_created_ids IS DISTINCT FROM v_created_batch_ids
     OR v_result_created_batch_ids IS DISTINCT FROM v_created_batch_ids
     OR v_result_skipped_ids IS DISTINCT FROM v_skipped_batch_ids
     OR v_result_cancelled_ids IS DISTINCT FROM v_cancelled_batch_ids
     OR NULLIF(v_result_json->>'primary_pay_batch_id','')::uuid IS DISTINCT FROM v_created_batch_ids[1]
     OR NULLIF(v_result_json->>'pay_batch_id','')::uuid IS DISTINCT FROM v_created_batch_ids[1]
     OR COALESCE((v_result_json->>'created_batch_count')::integer,-1) <> pg_catalog.cardinality(v_created_batch_ids)
     OR COALESCE((v_result_json->>'candidate_count')::integer,-1) <> v_candidate_count
     OR NULLIF(v_result_json->>'paye_pay_batch_id','')::uuid IS DISTINCT FROM v_paye_batch_id
     OR NULLIF(v_result_json->>'umbrella_pay_batch_id','')::uuid IS DISTINCT FROM v_umbrella_batch_id
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_result_json->'created_batches') AS result_batch(value)
       LEFT JOIN private.banking_pay_draft_operation_created_batches_v8 AS batch_row
         ON batch_row.operation_id=p_operation_id
        AND batch_row.pay_batch_id=(result_batch.value->>'pay_batch_id')::uuid
        AND batch_row.integrity_state='PASS'
       LEFT JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS scope_row
         ON scope_row.operation_id=batch_row.operation_id
        AND scope_row.candidate_scope_ordinal=batch_row.candidate_scope_ordinal
       WHERE batch_row.pay_batch_id IS NULL
          OR pg_catalog.upper(COALESCE(result_batch.value->>'pay_channel',''))
               IS DISTINCT FROM scope_row.resolved_pay_channel
     ) THEN
    RAISE EXCEPTION 'DRAFT_CREATE_TERMINAL_RESULT_CONTRACT_MISMATCH'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_CREATE_TERMINAL_RESULT_CONTRACT_MISMATCH"}';
  END IF;

  v_created_batches := v_result_json->'created_batches';

  -- This is the current banking_pay_operation_finish DRAFT_CREATE post-Draft
  -- authority projection, unchanged apart from its row-backed admission proof.
  WITH authority_rows AS (
    SELECT
      draft_scope.id AS scope_id,
      draft_scope.candidate_id,
      draft_scope.pay_batch_id,
      draft_scope.workbench_session_id,
      draft_scope.source_session_version,
      draft_scope.source_snapshot_run_id,
      COALESCE(candidate_counter.seq,0) AS source_change_seq,
      COALESCE(candidate_counter.scope_change_generation,0) AS dirty_generation,
      draft_scope.allocation_basis_json->>'source_build_run_id' AS original_source_build_run_id,
      draft_scope.allocation_basis_json->>'source_publication_id' AS original_source_publication_id,
      draft_scope.allocation_basis_json->>'source_identity_digest' AS original_source_identity_digest,
      draft_scope.allocation_basis_json->>'semantic_proof_digest' AS original_semantic_proof_digest,
      draft_scope.allocation_basis_json->'source_publication_attestation' AS source_attestation
    FROM public.banking_pay_operation_candidate_scope AS draft_scope
    LEFT JOIN public.app_change_counters AS candidate_counter
      ON candidate_counter.entity_key='pay_candidate:'||draft_scope.candidate_id::text
    WHERE draft_scope.operation_id=p_operation_id
  ), eligible_authority AS (
    SELECT authority_rows.*,
      COALESCE(authority_rows.original_source_publication_id,'')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AS fast_reversion_eligible,
      pg_catalog.md5(
        p_operation_id::text||'|'||authority_rows.pay_batch_id::text||'|'||
        authority_rows.workbench_session_id::text||'|'||authority_rows.candidate_id::text||'|'||
        authority_rows.source_change_seq::text||'|'||authority_rows.dirty_generation::text||'|'||
        authority_rows.source_session_version::text||'|'||authority_rows.source_snapshot_run_id::text||'|'||
        authority_rows.original_source_build_run_id||'|'||
        COALESCE(authority_rows.original_source_publication_id,'')||'|'||
        authority_rows.original_source_identity_digest||'|'||authority_rows.original_semantic_proof_digest||'|'||
        (COALESCE(authority_rows.original_source_publication_id,'')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')::text||
        '|POST_DRAFT_LIVE_AUTHORITY_V2') AS authority_digest
    FROM authority_rows
    WHERE COALESCE(authority_rows.source_attestation->>'attestation_version','')='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
      AND COALESCE(authority_rows.source_attestation->>'semantic_contract_version','')='READY_TO_PAY_SEMANTIC_V2'
      AND COALESCE((authority_rows.source_attestation->>'semantic_ready')::boolean,false)
      AND COALESCE((authority_rows.source_attestation->>'parity_complete')::boolean,false)
      AND COALESCE(authority_rows.original_source_build_run_id,'')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND NULLIF(pg_catalog.btrim(COALESCE(authority_rows.original_source_identity_digest,'')),'') IS NOT NULL
      AND NULLIF(pg_catalog.btrim(COALESCE(authority_rows.original_semantic_proof_digest,'')),'') IS NOT NULL
  ), frozen_authority AS (
    UPDATE public.banking_pay_operation_candidate_scope AS draft_scope
    SET allocation_basis_json=COALESCE(draft_scope.allocation_basis_json,'{}'::jsonb)||
      pg_catalog.jsonb_build_object('post_draft_authority',pg_catalog.jsonb_build_object(
        'contract_version','POST_DRAFT_LIVE_AUTHORITY_V2','draft_operation_id',p_operation_id,
        'pay_batch_id',eligible_authority.pay_batch_id,'workbench_session_id',eligible_authority.workbench_session_id,
        'candidate_id',eligible_authority.candidate_id,'source_change_seq',eligible_authority.source_change_seq,
        'dirty_generation',eligible_authority.dirty_generation,'source_session_version',eligible_authority.source_session_version,
        'source_snapshot_run_id',eligible_authority.source_snapshot_run_id,
        'original_source_build_run_id',eligible_authority.original_source_build_run_id,
        'original_source_publication_id',eligible_authority.original_source_publication_id,
        'fast_reversion_eligible',eligible_authority.fast_reversion_eligible,
        'fast_reversion_ineligible_reason',CASE WHEN eligible_authority.fast_reversion_eligible THEN NULL ELSE 'LEGACY_PHYSICAL_PUBLICATION_MISSING' END,
        'original_source_identity_digest',eligible_authority.original_source_identity_digest,
        'original_semantic_proof_digest',eligible_authority.original_semantic_proof_digest,
        'authority_digest',eligible_authority.authority_digest,'captured_at_utc',v_now,
        'policy_x_authority','FROZEN_PRE_DRAFT_SOURCE_PLUS_POST_DRAFT_LIVE_FENCE')),
        updated_at_utc=v_now
    FROM eligible_authority
    WHERE draft_scope.id=eligible_authority.scope_id
    RETURNING draft_scope.id
  )
  SELECT pg_catalog.count(*)::integer INTO v_post_draft_authority_count FROM frozen_authority;

  v_result_json := v_result_json || pg_catalog.jsonb_build_object(
    'post_draft_authority_contract_version','POST_DRAFT_LIVE_AUTHORITY_V2',
    'post_draft_authority_count',v_post_draft_authority_count,
    'post_draft_authority_candidate_count',v_finish_scope_count,
    'post_draft_fast_reversion_eligible_count',(
      SELECT pg_catalog.count(*)::integer
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      WHERE scope_row.operation_id=p_operation_id
        AND COALESCE((scope_row.allocation_basis_json->'post_draft_authority'->>'fast_reversion_eligible')::boolean,false)));

  SELECT COALESCE(change_counter.seq,0)
  INTO v_finish_scope_generation
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key='pay_candidate_scope_generation';
  SELECT pg_catalog.max(candidate_counter.scope_change_generation)
  INTO v_finish_relevant_generation
  FROM public.banking_pay_operation_candidate_scope AS frozen_scope
  JOIN public.app_change_counters AS candidate_counter
    ON candidate_counter.entity_key='pay_candidate:'||frozen_scope.candidate_id::text
  WHERE frozen_scope.operation_id=p_operation_id
    AND candidate_counter.scope_change_generation>v_operation.frozen_scope_change_generation
    AND candidate_counter.scope_change_generation<=v_finish_scope_generation;

  v_finish_blocker := public.pay_workbench_scope_blocker_state_v1(
    v_operation.workbench_session_id,v_finish_scope_generation,p_operation_id);
  v_finish_unresolved_root_count := COALESCE((v_finish_blocker->>'upstream_active_count')::integer,0);
  v_finish_failed_root_count := COALESCE((v_finish_blocker->>'upstream_unresolved_failure_count')::integer,0);
  IF v_finish_scope_generation=v_operation.frozen_scope_change_generation THEN
    v_finish_scope_status:='NONE';v_finish_freshness_status:='VALID_AT_SCOPE_FREEZE';
  ELSIF v_finish_relevant_generation IS NOT NULL THEN
    v_finish_scope_status:='RELEVANT';v_finish_freshness_status:='STALE_POST_SCOPE_FREEZE';
  ELSIF v_finish_failed_root_count>0 THEN
    v_finish_scope_status:='PENDING_RELEVANCE';v_finish_freshness_status:='PENDING_SCOPE_CHANGE_RELEVANCE_FAILED';
  ELSIF v_finish_unresolved_root_count>0 THEN
    v_finish_scope_status:='PENDING_RELEVANCE';v_finish_freshness_status:='PENDING_SCOPE_CHANGE_RELEVANCE';
  ELSE
    v_finish_scope_status:='IRRELEVANT';v_finish_freshness_status:='VALID_AT_SCOPE_FREEZE';
  END IF;

  UPDATE public.pay_batches AS batch_row
  SET freshness_validation_status=v_finish_freshness_status,
      freshness_checked_at_utc=v_now,
      scope_generation_observed_at_shell=GREATEST(COALESCE(batch_row.scope_generation_observed_at_shell,0),v_finish_scope_generation),
      freshness_result_json=COALESCE(batch_row.freshness_result_json,'{}'::jsonb)||pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'post_freeze_scope_status',v_finish_scope_status,
        'scope_generation_observed_at_operation_finish',v_finish_scope_generation,
        'post_freeze_relevant_generation',v_finish_relevant_generation,
        'unresolved_broad_root_count',v_finish_unresolved_root_count,
        'failed_broad_root_count',v_finish_failed_root_count,
        'scope_blocker_failure_sample',COALESCE(v_finish_blocker->'failure_sample','[]'::jsonb),
        'checked_at_utc',v_now::text,'policy_x_authority','FROZEN_OPERATION_SCOPE'))
  WHERE batch_row.id IN (
    SELECT DISTINCT scope_row.pay_batch_id
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.operation_id=p_operation_id AND scope_row.pay_batch_id IS NOT NULL);

  UPDATE public.banking_pay_operations AS operation_row
  SET status='COMPLETE',runner_state='COMPLETE',requires_user_action=false,
      resume_reason='OPERATION_COMPLETE',result_json=v_result_json,error_json=NULL,
      lease_owner=NULL,lease_expires_at_utc=NULL,locked_by=NULL,lock_expires_at_utc=NULL,
      run_after_utc=NULL,heartbeat_at_utc=v_now,last_advanced_at_utc=v_now,
      completed_at_utc=COALESCE(operation_row.completed_at_utc,v_now),
      progress_json=pg_catalog.jsonb_strip_nulls(COALESCE(operation_row.progress_json,'{}'::jsonb)||pg_catalog.jsonb_build_object(
        'finished_at_utc',v_now::text,'finish_status','COMPLETE','runner_state','COMPLETE',
        'requires_user_action',false,'resume_reason','OPERATION_COMPLETE',
        'draft_v8_terminal_adapter',true)),
      post_freeze_scope_status=v_finish_scope_status,
      post_freeze_observed_generation=v_finish_scope_generation,
      post_freeze_relevant_generation=v_finish_relevant_generation,
      post_freeze_scope_checked_at_utc=v_now,updated_at_utc=v_now
  WHERE operation_row.id=p_operation_id
  RETURNING operation_row.* INTO v_operation;

  UPDATE private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope_row
  SET freeze_state='TERMINAL_COMPLETE'
  WHERE frozen_scope_row.operation_id=p_operation_id;
  UPDATE private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  SET link_state='TERMINAL_COMPLETE'
  WHERE link_row.operation_id=p_operation_id;
  UPDATE private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope_row
  SET scope_state='COMPLETE'
  WHERE candidate_scope_row.operation_id=p_operation_id AND candidate_scope_row.scope_state<>'FAILED';

  v_created_ids_digest := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to(pg_catalog.to_jsonb(v_created_batch_ids)::text,'UTF8'),'sha256'),'hex');
  v_terminal_digest := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to(v_result_json::text,'UTF8'),'sha256'),'hex');
  INSERT INTO private.banking_pay_draft_operation_terminal_results_v8(
    operation_id,terminal_status,created_pay_batch_count,created_pay_batch_ids_digest_sha256,
    result_digest_sha256,terminal_error_code,completed_at_utc,legacy_terminal_result_json,
    legacy_terminal_result_digest_sha256,pay_batch_ids,created_pay_batch_ids,
    primary_pay_batch_id,pay_batch_id,created_batches,additive_certificate_diagnostics_json
  ) VALUES (
    p_operation_id,'COMPLETE',pg_catalog.cardinality(v_created_batch_ids),v_created_ids_digest,
    v_terminal_digest,NULL,v_operation.completed_at_utc,v_result_json,v_terminal_digest,
    v_created_batch_ids,v_created_batch_ids,v_created_batch_ids[1],v_created_batch_ids[1],v_created_batches,
    pg_catalog.jsonb_build_object(
      'contract','DRAFT_CERTIFICATE_CONSUMER_V1','certificate_uuid',v_scope.certificate_uuid,
      'manifest_digest_sha256',v_scope.manifest_digest_sha256,
      'selected_constituent_count',v_scope.constituent_count,
      'selected_partition_count',v_scope.partition_count,
      'selected_constituents_digest_sha256',v_scope.selected_constituents_digest_sha256,
      'selected_partitions_digest_sha256',v_scope.selected_partitions_digest_sha256))
  ;

  RETURN QUERY SELECT true,NULL::text,
    v_operation.id,v_operation.operation_type,v_operation.status,v_operation.phase,
    v_operation.actor_user_id,v_operation.workbench_session_id,v_operation.pay_batch_id,
    v_operation.root_operation_id,v_operation.idempotency_key,v_operation.input_json,
    v_operation.config_json,v_operation.progress_json,v_operation.result_json,
    v_operation.error_json,v_operation.total_units,v_operation.completed_units,
    v_operation.failed_units,v_operation.current_chunk_index,v_operation.chunk_count,
    COALESCE(v_operation.lease_owner,v_operation.locked_by),
    COALESCE(v_operation.lease_expires_at_utc,v_operation.lock_expires_at_utc),
    v_operation.created_at_utc,v_operation.started_at_utc,v_operation.updated_at_utc,
    v_operation.completed_at_utc,v_operation.failed_at_utc;
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_operation_finish_v8(uuid,text,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_operation_finish_v8(uuid,text,jsonb,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_operation_finish_v8(uuid,text,jsonb,jsonb) TO service_role;
