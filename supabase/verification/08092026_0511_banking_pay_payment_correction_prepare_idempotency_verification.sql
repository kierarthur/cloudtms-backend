-- CloudTMS Banking Pay correction PREPARE/START_PREPARED replay verification.
-- Read-only metadata/source/ACL and cutover assertions; no business row is changed.

\set ON_ERROR_STOP on

BEGIN;

DO $verification$
DECLARE
  v_oid oid := pg_catalog.to_regprocedure(
    'public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb)'
  );
  v_definition text;
  v_active_legacy_count integer := 0;
BEGIN
  IF v_oid IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_START_MISSING';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS function_row
    WHERE function_row.oid = v_oid
      AND function_row.prosecdef
      AND function_row.provolatile = 'v'
      AND function_row.proparallel = 'u'
      AND pg_catalog.pg_get_userbyid(function_row.proowner) IN ('postgres', current_user)
      AND COALESCE(function_row.proconfig, ARRAY[]::text[]) @> ARRAY[
        'search_path=pg_catalog, private, extensions, pg_temp',
        'statement_timeout=6000ms',
        'lock_timeout=1000ms'
      ]::text[]
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_START_METADATA_MISMATCH';
  END IF;

  IF NOT pg_catalog.has_function_privilege('service_role', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('anon', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('public', v_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_START_ACL_MISMATCH';
  END IF;

  IF pg_catalog.to_regclass('public.idx_banking_pay_operations_idempotency_key') IS NULL
     OR pg_catalog.to_regclass('public.ux_banking_pay_operations_correction_request') IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_INDEX_MISSING';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(v_oid) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'OPERATION_IDEMPOTENCY_KEY_AMBIGUOUS') = 0
     OR pg_catalog.strpos(v_definition, 'IMMUTABLE_REQUEST_IDENTITY_MISMATCH') = 0
     OR pg_catalog.strpos(v_definition, 'START_PREPARED_REPLAY_CONFLICT') = 0
     OR pg_catalog.strpos(v_definition, 'EXACT_CONSUMED_PROOF_OR_REQUEST_MISMATCH') = 0
     OR pg_catalog.strpos(v_definition, 'REQUEST_OPERATION_LIFECYCLE_MISMATCH') = 0
     OR pg_catalog.strpos(v_definition, 'v_replay_expected_selection IS DISTINCT FROM v_replay_request.selection_json') = 0
     OR pg_catalog.strpos(v_definition, 'coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE') = 0
     OR pg_catalog.strpos(v_definition, 'banking_pay_operation_start:PAYMENT_CORRECTION:') = 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXACT_REPLAY_GUARD_MISSING';
  END IF;

  IF pg_catalog.strpos(v_definition, 'v_replay_request.plan_json->>''idempotency_key''') > 0
     OR pg_catalog.strpos(v_definition, 'SET statement_timeout TO ''6000ms''') = 0
     OR pg_catalog.strpos(v_definition, 'SET lock_timeout TO ''1000ms''') = 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_MUTABLE_IDENTITY_OR_BUDGET_DRIFT';
  END IF;

  -- The target release has no active old requests.  Any nonterminal row that
  -- cannot prove the immutable key and unique operation link blocks release;
  -- terminal audit history is deliberately untouched.
  SELECT pg_catalog.count(*)::integer
  INTO v_active_legacy_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IN (
      'PLANNING','PLANNED','REQUESTED','AWAITING_AUTHORISATION',
      'AUTHORISED','EXPANDED','PROCESSING'
    )
    AND (
      pg_catalog.jsonb_typeof(request_row.selection_json) <> 'object'
      OR NULLIF(pg_catalog.btrim(coalesce(request_row.selection_json->>'idempotency_key', '')), '') IS NULL
      OR (
        SELECT pg_catalog.count(*)
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
          AND operation_row.input_json->>'correction_request_id' = request_row.id::text
          AND operation_row.pay_batch_id = request_row.pay_batch_id
          AND operation_row.idempotency_key = NULLIF(
            pg_catalog.btrim(coalesce(request_row.selection_json->>'idempotency_key', '')),
            ''
          )
      ) <> 1
    );

  IF v_active_legacy_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_ACTIVE_REQUEST_REPLAY_IDENTITY_INVALID: %', v_active_legacy_count;
  END IF;
END;
$verification$;

ROLLBACK;
