-- One-time TEST cutover repair for legacy cancellation requests whose bounded
-- financial work is already terminal but whose historical operation stopped
-- immediately before request finalisation.
--
-- This migration deliberately does not infer or rewrite payment policy.  It
-- invokes the installed correction finaliser for each exactly eligible row,
-- then leaves the historical post-financial Workbench publication step as an
-- explicit terminal review record.  Current source-authority rebuilds remain
-- owned by the Workbench queue after the release.

\set ON_ERROR_STOP on

begin;

set local statement_timeout = '6000ms';
set local lock_timeout = '1000ms';

DO $legacy_terminal_correction_cutover_repair$
DECLARE
  v_environment text;
  v_active_request_count integer := 0;
  v_eligible_request_count integer := 0;
  v_remaining_active_request_count integer := 0;
  v_remaining_active_operation_count integer := 0;
  v_worker_id constant text := 'release-legacy-terminal-correction-cutover-v1';
  v_target record;
  v_guard jsonb;
  v_result jsonb;
BEGIN
  IF pg_catalog.to_regclass('private.cloudtms_database_identity') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_requests') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_work_items') IS NULL
     OR pg_catalog.to_regclass('public.banking_pay_operations') IS NULL
     OR pg_catalog.to_regprocedure(
       'public.pay_payment_correction_process_chunk(uuid,integer,text,uuid)'
     ) IS NULL
     OR pg_catalog.to_regprocedure(
       'private.pay_payment_mutation_guard_v1(uuid,uuid,text)'
     ) IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_REQUIRED_AUTHORITY_MISSING';
  END IF;

  SELECT identity_row.environment
  INTO STRICT v_environment
  FROM private.cloudtms_database_identity AS identity_row
  WHERE identity_row.singleton;

  -- This repairs only the authorised TEST cutover evidence.  LIVE retains the
  -- unchanged fail-closed zero-active-request gate in the following migration.
  IF v_environment IS DISTINCT FROM 'TEST' THEN
    RETURN;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  IF v_active_request_count = 0 THEN
    RETURN;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_eligible_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.correction_kind = 'PRE_BANK_CANCEL'
    AND request_row.status = 'PROCESSING'
    AND request_row.requested_by_user_id IS NOT NULL
    AND request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz
    AND EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = request_row.id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = request_row.id
        AND work_row.status NOT IN (
          'APPLIED', 'SKIPPED', 'BLOCKED', 'FAILED_FINAL', 'CANCELLED'
        )
    )
    AND 1 = (
      SELECT pg_catalog.count(*)
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id' = request_row.id::text
        AND operation_row.pay_batch_id = request_row.pay_batch_id
        AND operation_row.status = 'REVIEW_REQUIRED'
        AND operation_row.phase = 'FINALISE'
    )
    AND 1 = (
      SELECT pg_catalog.count(*)
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id' = request_row.id::text
    );

  IF v_eligible_request_count IS DISTINCT FROM v_active_request_count
     OR v_eligible_request_count < 1
     OR v_eligible_request_count > 8 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_SCOPE_MISMATCH: active=%, eligible=%',
      v_active_request_count,
      v_eligible_request_count;
  END IF;

  FOR v_target IN
    SELECT
      request_row.id AS request_id,
      request_row.pay_batch_id,
      request_row.requested_by_user_id AS actor_user_id,
      operation_row.id AS operation_id
    FROM public.pay_payment_correction_requests AS request_row
    JOIN public.banking_pay_operations AS operation_row
      ON operation_row.operation_type = 'PAYMENT_CORRECTION'
     AND operation_row.input_json->>'correction_request_id' = request_row.id::text
     AND operation_row.pay_batch_id = request_row.pay_batch_id
     AND operation_row.status = 'REVIEW_REQUIRED'
     AND operation_row.phase = 'FINALISE'
    WHERE request_row.correction_kind = 'PRE_BANK_CANCEL'
      AND request_row.status = 'PROCESSING'
      AND request_row.requested_by_user_id IS NOT NULL
      AND request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz
      AND EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = request_row.id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = request_row.id
          AND work_row.status NOT IN (
            'APPLIED', 'SKIPPED', 'BLOCKED', 'FAILED_FINAL', 'CANCELLED'
          )
      )
    ORDER BY request_row.id
  LOOP
    -- Match the installed finaliser's canonical lock order before temporarily
    -- restoring its expired historical lease.
    v_guard := private.pay_payment_mutation_guard_v1(
      v_target.pay_batch_id,
      v_target.request_id,
      'CORRECTION_APPLY'
    );
    IF COALESCE((v_guard->>'ok')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_MUTATION_GUARD_REJECTED'
        USING DETAIL = v_guard::text;
    END IF;

    PERFORM 1
    FROM public.pay_payment_correction_requests AS locked_request
    WHERE locked_request.id = v_target.request_id
      AND locked_request.status = 'PROCESSING'
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_REQUEST_CHANGED';
    END IF;

    PERFORM 1
    FROM public.pay_batches AS locked_batch
    WHERE locked_batch.id = v_target.pay_batch_id
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_BATCH_MISSING';
    END IF;

    PERFORM 1
    FROM public.banking_pay_operations AS locked_operation
    WHERE locked_operation.id = v_target.operation_id
      AND locked_operation.operation_type = 'PAYMENT_CORRECTION'
      AND locked_operation.pay_batch_id = v_target.pay_batch_id
      AND locked_operation.input_json->>'correction_request_id' = v_target.request_id::text
      AND locked_operation.status = 'REVIEW_REQUIRED'
      AND locked_operation.phase = 'FINALISE'
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_OPERATION_CHANGED';
    END IF;

    UPDATE public.banking_pay_operations AS repair_operation
    SET status = 'RUNNING',
        runner_state = 'RUNNABLE',
        requires_user_action = false,
        run_after_utc = pg_catalog.clock_timestamp(),
        locked_by = v_worker_id,
        lock_expires_at_utc = pg_catalog.clock_timestamp() + interval '30 seconds',
        lease_owner = v_worker_id,
        lease_expires_at_utc = pg_catalog.clock_timestamp() + interval '30 seconds',
        heartbeat_at_utc = pg_catalog.clock_timestamp(),
        error_json = NULL,
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE repair_operation.id = v_target.operation_id;

    v_result := public.pay_payment_correction_process_chunk(
      v_target.request_id,
      100,
      v_worker_id,
      v_target.actor_user_id
    );

    IF v_result->>'code' IS DISTINCT FROM 'PAYMENT_CORRECTION_FINALISED'
       OR COALESCE((v_result->>'financial_complete')::boolean, false) IS NOT TRUE
       OR v_result->>'phase' IS DISTINCT FROM 'REFRESH_WORKBENCH'
       OR v_result->>'request_status' NOT IN (
         'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'CANCELLED'
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_FINALISE_REJECTED'
        USING DETAIL = v_result::text;
    END IF;

    -- The August operation predates the current post-commit Workbench evidence
    -- contract.  Do not invent that evidence and do not report publication as
    -- complete.  Financial cancellation is terminal; the current Workbench
    -- queue will rebuild affected Candidates after the release.
    UPDATE public.banking_pay_operations AS repaired_operation
    SET status = 'REVIEW_REQUIRED',
        phase = 'REFRESH_WORKBENCH',
        runner_state = 'WAITING_USER_REVIEW',
        requires_user_action = true,
        run_after_utc = NULL,
        locked_by = NULL,
        lock_expires_at_utc = NULL,
        lease_owner = NULL,
        lease_expires_at_utc = NULL,
        heartbeat_at_utc = NULL,
        error_json = pg_catalog.jsonb_build_object(
          'contract_version', 'PAYMENT_CORRECTION_LEGACY_CUTOVER_REVIEW_V1',
          'code', 'LEGACY_WORKBENCH_REFRESH_REQUIRES_CURRENT_AUTHORITY',
          'financial_complete', true,
          'release_cutover_terminal', true
        ),
        progress_json = COALESCE(repaired_operation.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'legacy_cutover_financial_finalised', true,
            'legacy_cutover_workbench_refresh_pending', true
          ),
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE repaired_operation.id = v_target.operation_id;
  END LOOP;

  SELECT pg_catalog.count(*)::integer
  INTO v_remaining_active_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  SELECT pg_catalog.count(*)::integer
  INTO v_remaining_active_operation_count
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND (
      operation_row.status IS NULL
      OR pg_catalog.upper(pg_catalog.btrim(operation_row.status)) NOT IN (
        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'
      )
    );

  IF v_remaining_active_request_count <> 0
     OR v_remaining_active_operation_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_CUTOVER_NOT_CLEAN: requests=%, operations=%',
      v_remaining_active_request_count,
      v_remaining_active_operation_count;
  END IF;
END
$legacy_terminal_correction_cutover_repair$;

commit;
