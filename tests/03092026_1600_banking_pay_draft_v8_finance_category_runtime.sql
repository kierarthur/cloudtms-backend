-- H2 rollback-only bridge from the immutable six-category canonical producer
-- fixture into the real V8 certificate, normalized staging and Candidate-scope
-- route.  The historical 02092026_1042 fixture remains byte-identical and its
-- former provisional scope function is not installed.  This transaction-local
-- replacement exists only so that the immutable downstream fixture invokes the
-- new row-backed transport at the exact former scope boundary; ROLLBACK restores
-- the current installed owner and leaves no Draft, payment or provider state.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL jit = off;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30s';

CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_scope_seed(
  p_operation_id uuid,
  p_workbench_session_id uuid,
  p_actor_user_id uuid,
  p_selected_preview_row_ids jsonb DEFAULT NULL::jsonb,
  p_pay_channel_scope text DEFAULT NULL::text,
  p_same_week_paye_override_json jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(
  candidate_scope_count integer,
  selected_row_count integer,
  timesheet_count integer,
  finance_case_count integer,
  pay_channel_count integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_start jsonb;
  v_append jsonb;
  v_seal jsonb;
  v_envelope jsonb;
  v_reference jsonb;
  v_admission jsonb;
  v_stage jsonb;
  v_certificate_uuid uuid;
  v_after_ordinal integer;
  v_previous_receipt text;
  v_lease_owner text;
  v_override jsonb;
  v_iteration integer;
  v_scope text := pg_catalog.upper(pg_catalog.btrim(COALESCE(NULLIF(p_pay_channel_scope, ''), 'ALL')));
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL OR p_workbench_session_id IS NULL OR p_actor_user_id IS NULL
     OR v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA')
     OR pg_catalog.jsonb_typeof(p_selected_preview_row_ids) <> 'array' THEN
    RAISE EXCEPTION 'H2_V8_ROLLBACK_BRIDGE_INPUT_INVALID' USING ERRCODE = '22023';
  END IF;

  SELECT session_row.*
  INTO STRICT v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_workbench_session_id
    AND session_row.actor_user_id = p_actor_user_id;

  -- The immutable legacy fixture supplies the selected ids only as an assertion
  -- of what its already-executed prepare owner accepted.  V8 independently
  -- rebuilds authority from the settled server session; it never persists or
  -- consumes this array as its Draft selection input.
  IF pg_catalog.jsonb_array_length(p_selected_preview_row_ids) <> v_session.selected_row_count THEN
    RAISE EXCEPTION 'H2_V8_ROLLBACK_BRIDGE_SELECTION_COUNT_MISMATCH' USING ERRCODE = '55000';
  END IF;

  RAISE NOTICE 'H2_V8_ROLLBACK_BRIDGE_COUNTS=%', pg_catalog.jsonb_build_object(
    'session_scope_total', v_session.scope_total_count,
    'actual_scope_total', (SELECT pg_catalog.count(*) FROM public.banking_pay_workbench_session_scope AS scope_row WHERE scope_row.session_id = p_workbench_session_id),
    'session_preview_total', v_session.preview_row_count,
    'actual_preview_total', (SELECT pg_catalog.count(*) FROM public.banking_pay_workbench_preview_rows AS preview_row WHERE preview_row.session_id = p_workbench_session_id),
    'session_selected_total', v_session.selected_row_count,
    'actual_selected_total', (SELECT pg_catalog.count(*) FROM public.banking_pay_workbench_preview_rows AS preview_row WHERE preview_row.session_id = p_workbench_session_id AND preview_row.selected)
  );
  RAISE NOTICE 'H2_V8_ROLLBACK_BRIDGE_PUBLICATION=%', (
    SELECT pg_catalog.to_jsonb(observed)
    FROM (
      SELECT scope_row.status AS scope_status,
             state_row.status AS candidate_status,
             scope_row.certified_preview_publication_parity_ok,
             scope_row.certified_preview_publication_session_version,
             v_session.version AS expected_session_version,
             scope_row.certified_preview_publication_source_change_seq,
             state_row.source_change_seq AS expected_source_change_seq,
             scope_row.certified_preview_publication_source_build_run_id IS NOT NULL AS has_build_run,
             scope_row.certified_preview_publication_source_publication_id IS NOT NULL AS has_publication,
             pg_catalog.jsonb_typeof(scope_row.certified_preview_publication_attestation_json) AS attestation_type
      FROM public.banking_pay_workbench_session_scope AS scope_row
      LEFT JOIN public.banking_pay_workbench_session_candidate_state AS state_row
        ON state_row.session_id = scope_row.session_id
       AND state_row.candidate_id = scope_row.candidate_id
      WHERE scope_row.session_id = p_workbench_session_id
    ) AS observed
  );
  RAISE NOTICE 'H2_V8_ROLLBACK_BRIDGE_CONTRACTS=%', (
    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'preview_row_id', preview_row.id,
      'line_type', preview_row.row_json->>'line_type',
      'selected', preview_row.selected,
      'status', preview_row.status,
      'selection_state', preview_row.selection_state,
      'contract', private.pay_workbench_settled_certificate_preview_contract_v8(preview_row.id)
    ) ORDER BY preview_row.row_ordinal, preview_row.id)
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_workbench_session_id
      AND preview_row.selected
  );
  RAISE NOTICE 'H2_V8_ROLLBACK_BRIDGE_FINANCE_SOURCE=%', (
    SELECT preview_row.row_json
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_workbench_session_id
      AND preview_row.selected
      AND preview_row.row_json->>'line_type' <> 'TIMESHEET_PAYMENT'
    ORDER BY preview_row.row_ordinal, preview_row.id
    LIMIT 1
  );

  v_start := public.pay_workbench_settled_certificate_build_start_v8(
    p_workbench_session_id,
    p_actor_user_id,
    'WORKBENCH_SETTLED_CERTIFICATE_V8:' || p_workbench_session_id::text
  );
  IF v_start->'ok' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H2_V8_CERTIFICATE_BUILD_START_FAILED:%', v_start;
  END IF;
  v_certificate_uuid := (v_start->>'certificate_uuid')::uuid;
  v_lease_owner := COALESCE(NULLIF(v_start->>'lease_owner', ''), p_actor_user_id::text);
  v_after_ordinal := NULLIF(v_start->>'next_after_ordinal', '')::integer;
  v_previous_receipt := NULLIF(v_start->>'last_page_receipt_sha256', '');

  IF v_start->>'lifecycle' <> 'SEALED_CURRENT' THEN
    FOR v_iteration IN 1..200 LOOP
      v_append := public.pay_workbench_settled_certificate_build_append_page_v8(
        v_certificate_uuid,
        v_after_ordinal,
        256,
        v_previous_receipt,
        v_lease_owner
      );
      IF v_append->'ok' IS DISTINCT FROM 'true'::jsonb THEN
        RAISE EXCEPTION 'H2_V8_CERTIFICATE_BUILD_APPEND_FAILED:%', v_append;
      END IF;
      v_after_ordinal := NULLIF(v_append->>'next_after_ordinal', '')::integer;
      v_previous_receipt := NULLIF(v_append->>'page_receipt_sha256', '');
      EXIT WHEN v_append->'has_more' IS DISTINCT FROM 'true'::jsonb;
    END LOOP;
    IF v_append->'has_more' IS NOT DISTINCT FROM 'true'::jsonb THEN
      RAISE EXCEPTION 'H2_V8_CERTIFICATE_BUILD_APPEND_DID_NOT_TERMINATE';
    END IF;

    FOR v_iteration IN 1..400 LOOP
      v_seal := public.pay_workbench_settled_certificate_seal_v8(
        v_certificate_uuid,
        p_actor_user_id
      );
      EXIT WHEN v_seal->'sealed' IS NOT DISTINCT FROM 'true'::jsonb;
    END LOOP;
    IF v_seal->'sealed' IS DISTINCT FROM 'true'::jsonb THEN
      RAISE EXCEPTION 'H2_V8_CERTIFICATE_SEAL_DID_NOT_TERMINATE:%', v_seal;
    END IF;
  END IF;

  v_override := pg_catalog.jsonb_build_object(
    'continue', false,
    'verified', false,
    'used', false,
    'pay_date', v_session.pay_date::text,
    'pay_week_start', public._pay_week_start_monday(v_session.pay_date)::text,
    'pay_week_end', (public._pay_week_start_monday(v_session.pay_date) + 6)::text,
    'reason', NULL,
    'verified_by_user_id', NULL,
    'verified_at_utc', NULL,
    'reauth_purpose', NULL,
    'guardrail_code', NULL
  );

  v_envelope := public.pay_workbench_settled_certificate_current_reference_issue_v8(
    p_workbench_session_id,
    v_session.version,
    v_session.progress_counter_version,
    v_scope,
    (SELECT operation_row.idempotency_key
     FROM public.banking_pay_operations AS operation_row
     WHERE operation_row.id = p_operation_id),
    v_override
  );
  v_reference := v_envelope->'certificate_reference';
  IF pg_catalog.jsonb_typeof(v_reference) <> 'object' THEN
    RAISE EXCEPTION 'H2_V8_CERTIFICATE_REFERENCE_ISSUE_FAILED:%', v_envelope;
  END IF;

  UPDATE public.banking_pay_operations AS operation_row
  SET phase = 'INITIALISE',
      input_json = (COALESCE(operation_row.input_json, '{}'::jsonb)
        - 'expected_workbench_selected_preview_row_ids'
        - 'selected_preview_row_ids'
        - 'selected_constituents'
        - 'selected_constituent_contracts')
        || pg_catalog.jsonb_build_object(
          'workbench_settled_certificate_reference_v8', v_reference
        ),
      progress_json = '{}'::jsonb,
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE operation_row.id = p_operation_id
    AND operation_row.workbench_session_id = p_workbench_session_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'H2_V8_ROLLBACK_BRIDGE_OPERATION_MISSING';
  END IF;

  v_admission := private.pay_workbench_settled_certificate_operation_admit_v8(
    p_operation_id
  );
  IF v_admission->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_admission->>'freeze_state' <> 'STAGING' THEN
    RAISE EXCEPTION 'H2_V8_OPERATION_ADMISSION_FAILED:%', v_admission;
  END IF;

  FOR v_iteration IN 1..20 LOOP
    v_stage := public.banking_pay_draft_certificate_stage_advance_v8(
      p_operation_id,
      'H2_V8_ROLLBACK_BRIDGE'
    );
    EXIT WHEN (
      SELECT operation_row.phase = 'DRAIN_TSFIN'
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.id = p_operation_id
    );
  END LOOP;

  IF (SELECT operation_row.phase FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.id = p_operation_id) <> 'DRAIN_TSFIN' THEN
    RAISE EXCEPTION 'H2_V8_CERTIFICATE_STAGING_DID_NOT_TERMINATE:%', v_stage;
  END IF;

  RETURN QUERY
  SELECT
    (SELECT pg_catalog.count(*)::integer
     FROM public.banking_pay_operation_candidate_scope AS candidate_scope
     WHERE candidate_scope.operation_id = p_operation_id),
    (SELECT pg_catalog.count(*)::integer
     FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS constituent_ref
     WHERE constituent_ref.operation_id = p_operation_id),
    (SELECT pg_catalog.count(DISTINCT payload.timesheet_id)::integer
     FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
     WHERE payload.operation_id = p_operation_id AND payload.timesheet_id IS NOT NULL),
    (SELECT pg_catalog.count(DISTINCT payload.finance_case_id)::integer
     FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
     WHERE payload.operation_id = p_operation_id AND payload.finance_case_id IS NOT NULL),
    (SELECT pg_catalog.count(DISTINCT candidate_scope.pay_channel)::integer
     FROM public.banking_pay_operation_candidate_scope AS candidate_scope
     WHERE candidate_scope.operation_id = p_operation_id);
END;
$function$;

-- The V8 certificate requires the existing physical source-publication identity
-- that Miget TEST already uses.  The generic disposable baseline defaults these
-- rollout switches off, so enable them only inside this outer rollback before
-- the immutable fixture publishes its canonical rows.
UPDATE public.settings_defaults
SET banking_pay_source_publication_identity_write_v1_enabled = true,
    banking_pay_source_publication_identity_enforce_v1_enabled = true
WHERE id = 1;

-- The nested BEGIN emits only PostgreSQL's harmless already-in-transaction
-- warning.  The immutable fixture's final ROLLBACK removes this adapter and all
-- synthetic business rows together.
\ir 02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql
