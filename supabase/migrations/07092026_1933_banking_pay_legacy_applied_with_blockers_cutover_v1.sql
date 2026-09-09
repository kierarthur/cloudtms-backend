-- One-time TEST cutover for an August cancellation request whose financial
-- work is terminal but whose legacy request envelope cannot be resumed by the
-- current whole-batch contract because it has no scope_type or V2 scope proof.
--
-- The applied cancellation remains applied.  The blocked work and its exact
-- classification remain unchanged.  Only the obsolete resumable parent state
-- becomes the truthful terminal BLOCKED state before the cancellation-notice
-- cutover admits current-contract requests.

\set ON_ERROR_STOP on

begin;

set local statement_timeout = '6000ms';
set local lock_timeout = '1000ms';

DO $legacy_applied_with_blockers_cutover$
DECLARE
  v_environment text;
  v_active_request_count integer := 0;
  v_eligible_request_count integer := 0;
  v_remaining_active_request_count integer := 0;
  v_target record;
  v_before_json jsonb;
  v_after_json jsonb;
BEGIN
  IF pg_catalog.to_regclass('private.cloudtms_database_identity') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_requests') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_work_items') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_actions') IS NULL
     OR pg_catalog.to_regclass('public.banking_pay_operations') IS NULL
     OR pg_catalog.to_regclass('public.pay_batches') IS NULL
     OR pg_catalog.to_regclass('public.pay_bank_transfers') IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CANCELLATION_NOTICE_LEGACY_CUTOVER_AUTHORITY_MISSING';
  END IF;

  SELECT identity_row.environment
  INTO STRICT v_environment
  FROM private.cloudtms_database_identity AS identity_row
  WHERE identity_row.singleton;

  IF v_environment IS DISTINCT FROM 'TEST' THEN
    RETURN;
  END IF;

  LOCK TABLE ONLY public.pay_payment_correction_requests
    IN SHARE ROW EXCLUSIVE MODE;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_request_count
  FROM ONLY public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR request_row.status NOT IN (
       'APPLIED', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
     );

  IF v_active_request_count = 0 THEN
    RETURN;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_eligible_request_count
  FROM public.pay_payment_correction_requests AS request_row
  JOIN public.pay_batches AS batch_row
    ON batch_row.id = request_row.pay_batch_id
  WHERE request_row.correction_kind = 'PRE_BANK_CANCEL'
    AND request_row.status = 'APPLIED_WITH_BLOCKERS'
    AND request_row.requested_by_user_id IS NOT NULL
    AND request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz
    AND batch_row.status = 'DRAFT'
    AND batch_row.execution_commit_state = 'NOT_SUBMITTED'
    AND NULLIF(pg_catalog.btrim(COALESCE(request_row.selection_json->>'scope_type', '')), '') IS NULL
    AND NULLIF(pg_catalog.btrim(COALESCE(request_row.plan_json->>'candidate_scope_contract_version', '')), '') IS NULL
    AND NULLIF(pg_catalog.btrim(COALESCE(request_row.plan_json->>'communication_cleanup_contract_version', '')), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = request_row.pay_batch_id
    )
    AND EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = request_row.id
        AND work_row.work_kind = 'PRE_BANK_CANCEL'
        AND work_row.status = 'APPLIED'
    )
    AND EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = request_row.id
        AND work_row.work_kind = 'PRE_BANK_CANCEL'
        AND work_row.status = 'BLOCKED'
        AND work_row.result_json#>>'{blocker,code}' = 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED'
        AND work_row.result_json#>>'{blocker,classification}' = 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION'
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = request_row.id
        AND (
          work_row.work_kind IS DISTINCT FROM 'PRE_BANK_CANCEL'
          OR work_row.status NOT IN ('APPLIED', 'BLOCKED')
          OR (
            work_row.status = 'BLOCKED'
            AND (
              work_row.result_json#>>'{blocker,code}' IS DISTINCT FROM 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED'
              OR work_row.result_json#>>'{blocker,classification}' IS DISTINCT FROM 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION'
            )
          )
        )
    )
    AND 1 = (
      SELECT pg_catalog.count(*)
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id' = request_row.id::text
        AND operation_row.pay_batch_id = request_row.pay_batch_id
        AND operation_row.status = 'REVIEW_REQUIRED'
        AND operation_row.phase = 'REFRESH_WORKBENCH'
        AND operation_row.error_json->>'code' = 'LEGACY_WORKBENCH_REFRESH_REQUIRES_CURRENT_AUTHORITY'
    )
    AND 1 = (
      SELECT pg_catalog.count(*)
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id' = request_row.id::text
    );

  IF v_active_request_count IS DISTINCT FROM 1
     OR v_eligible_request_count IS DISTINCT FROM v_active_request_count THEN
    RAISE EXCEPTION 'PAYMENT_CANCELLATION_NOTICE_LEGACY_CUTOVER_SCOPE_MISMATCH: active=%, eligible=%',
      v_active_request_count,
      v_eligible_request_count;
  END IF;

  FOR v_target IN
    SELECT request_row.*
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.correction_kind = 'PRE_BANK_CANCEL'
      AND request_row.status = 'APPLIED_WITH_BLOCKERS'
      AND request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz
    ORDER BY request_row.id
    FOR UPDATE
  LOOP
    v_before_json := pg_catalog.to_jsonb(v_target);

    UPDATE public.pay_payment_correction_requests AS terminal_request
    SET status = 'BLOCKED',
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE terminal_request.id = v_target.id
      AND terminal_request.status = 'APPLIED_WITH_BLOCKERS'
    RETURNING pg_catalog.to_jsonb(terminal_request.*)
    INTO STRICT v_after_json;

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
    ) VALUES (
      v_target.id,
      v_target.pay_batch_id,
      'SYSTEM',
      NULL,
      'BLOCK',
      pg_catalog.clock_timestamp(),
      'Terminalised pre-contract TEST cancellation audit residue before cancellation-notice cutover.',
      v_before_json,
      v_after_json,
      pg_catalog.jsonb_build_object(
        'code', 'LEGACY_CANCELLATION_REQUEST_NOT_CURRENT_CONTRACT_RESUMABLE',
        'preserved_applied_work', true,
        'preserved_blocked_work', true,
        'payment_policy_changed', false,
        'financial_rows_changed', false
      )
    );
  END LOOP;

  SELECT pg_catalog.count(*)::integer
  INTO v_remaining_active_request_count
  FROM ONLY public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR request_row.status NOT IN (
       'APPLIED', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
     );

  IF v_remaining_active_request_count IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'PAYMENT_CANCELLATION_NOTICE_LEGACY_CUTOVER_NOT_CLEAN: requests=%',
      v_remaining_active_request_count;
  END IF;
END
$legacy_applied_with_blockers_cutover$;

commit;
