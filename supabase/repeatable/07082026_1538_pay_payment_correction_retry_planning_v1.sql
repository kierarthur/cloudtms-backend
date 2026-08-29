-- CloudTMS Banking Pay cancellation — narrowly scoped safe retry.
--
-- This does not create a second request and cannot advance financial work.  It
-- returns the same PAYMENT_CORRECTION operation either to PREPARE_SELECTION
-- after a pre-selection failure, or to EXPAND_WORK for a whole-Draft
-- cancellation that failed before any correction/provider/transfer work was
-- durably created.  The historical function identity is retained so existing
-- callers and ACLs do not change.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_retry_planning_v1(
  p_correction_request_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '5000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_enabled boolean := false;
  v_actor public.tms_users%ROWTYPE;
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_batch public.pay_batches%ROWTYPE;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_is_admin boolean := false;
  v_retry_count integer := 0;
  v_error_code text := NULL::text;
  v_retry_mode text := NULL::text;
  v_retry_reason text := NULL::text;
  v_retry_counter_key text := NULL::text;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_RETRY_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_RETRY_REQUEST_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'ACTOR_USER_ID_REQUIRED')::text;
  END IF;

  SELECT COALESCE(settings_row.banking_pay_candidate_cancellation_enabled, false)
  INTO v_enabled
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  IF v_enabled IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_FEATURE_DISABLED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_FEATURE_DISABLED')::text;
  END IF;

  SELECT actor_row.*
  INTO v_actor
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id
    AND COALESCE(actor_row.is_active, false);

  IF NOT FOUND THEN
    RAISE EXCEPTION 'ACTOR_USER_INACTIVE'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'PERMISSION_DENIED')::text;
  END IF;

  v_is_admin := pg_catalog.lower(COALESCE(v_actor.role, '')) = 'admin';

  -- Canonical correction lock order: request -> batch -> operation.
  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  IF p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id AND v_is_admin IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_RETRY_REQUEST_OWNER_OR_ADMIN_REQUIRED'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_OWNER_OR_ADMIN_REQUIRED')::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_request.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND')::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.pay_batch_id = v_request.pay_batch_id
    AND operation_row.input_json ->> 'correction_request_id' = p_correction_request_id::text
  ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_OPERATION_NOT_FOUND')::text;
  END IF;

  v_error_code := pg_catalog.upper(COALESCE(
    v_operation.error_json ->> 'code',
    v_operation.error_json ->> 'error_code',
    ''
  ));
  IF pg_catalog.upper(COALESCE(v_request.status, '')) = 'PLANNING'
     AND pg_catalog.upper(COALESCE(v_operation.phase, '')) = 'PREPARE_SELECTION' THEN
    v_retry_mode := 'PLANNING';
    v_retry_reason := 'EXPLICIT_SAFE_PLANNING_RETRY';
    v_retry_counter_key := 'planning_retry_count';
  ELSIF pg_catalog.upper(COALESCE(v_request.status, '')) IN ('AUTHORISED', 'AUTHORIZED')
     AND pg_catalog.upper(COALESCE(v_operation.phase, '')) = 'EXPAND_WORK'
     AND pg_catalog.upper(COALESCE(v_request.plan_json ->> 'requested_action', '')) = 'DRAFT_CANCEL'
     AND pg_catalog.upper(COALESCE(v_batch.status, '')) = 'DRAFT' THEN
    v_retry_mode := 'DRAFT_EXPAND';
    v_retry_reason := 'EXPLICIT_SAFE_DRAFT_EXPAND_RETRY';
    v_retry_counter_key := 'processing_retry_count';
  ELSIF pg_catalog.upper(COALESCE(v_request.status, '')) IN ('PROCESSING', 'APPLIED')
     AND pg_catalog.upper(COALESCE(v_operation.phase, '')) IN ('FINALISE', 'REFRESH_WORKBENCH')
     AND (
       pg_catalog.upper(COALESCE(v_request.status, '')) = 'PROCESSING'
       OR pg_catalog.upper(COALESCE(v_operation.phase, '')) = 'REFRESH_WORKBENCH'
     )
     AND pg_catalog.upper(COALESCE(v_request.plan_json ->> 'requested_action', '')) = 'PRE_BANK_CANCEL' THEN
    v_retry_mode := 'POST_FINANCIAL';
    v_retry_reason := 'EXPLICIT_SAFE_POST_FINANCIAL_RETRY';
    v_retry_counter_key := 'processing_retry_count';
  END IF;

  v_retry_count := CASE
    WHEN v_retry_counter_key IS NOT NULL
      AND COALESCE(v_operation.progress_json ->> v_retry_counter_key, '') ~ '^[0-9]+$'
      THEN GREATEST((v_operation.progress_json ->> v_retry_counter_key)::integer, 0)
    ELSE 0
  END;

  IF v_retry_mode IS NULL
     OR pg_catalog.upper(COALESCE(v_operation.status, '')) <> 'REVIEW_REQUIRED'
     OR pg_catalog.upper(COALESCE(v_operation.runner_state, '')) <> 'WAITING_USER_REVIEW'
     OR COALESCE(v_operation.requires_user_action, false) IS NOT TRUE
     OR v_error_code <> 'BANKING_PAY_OPERATION_ADVANCE_FAILED'
     OR v_retry_count >= 3 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SAFE_RETRY_NOT_ALLOWED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_SAFE_RETRY_NOT_ALLOWED',
        'request_status', v_request.status,
        'operation_status', v_operation.status,
        'operation_phase', v_operation.phase
      )::text;
  END IF;

  IF v_retry_mode = 'PLANNING' THEN
    IF EXISTS (
         SELECT 1 FROM public.banking_pay_operation_chunks AS chunk_row
         WHERE chunk_row.operation_id = v_operation.id
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_request_candidates AS membership_row
         WHERE membership_row.correction_request_id = p_correction_request_id
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_work_items AS work_row
         WHERE work_row.correction_request_id = p_correction_request_id
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_items AS correction_row
         WHERE correction_row.correction_request_id = p_correction_request_id
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_actions AS action_row
         WHERE action_row.correction_request_id = p_correction_request_id
           AND action_row.action IS DISTINCT FROM 'REQUEST'
       )
       OR EXISTS (
         SELECT 1 FROM public.banking_pay_operation_provider_attempts AS provider_attempt
         WHERE provider_attempt.operation_id = v_operation.id
            OR provider_attempt.pay_batch_id = v_request.pay_batch_id
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_PLANNING_RETRY_EVIDENCE_EXISTS'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_PLANNING_RETRY_EVIDENCE_EXISTS'
        )::text;
    END IF;
  ELSIF v_retry_mode = 'DRAFT_EXPAND' THEN
    -- EXPAND_WORK is one PostgreSQL transaction.  A failure before these
    -- durable owners exist therefore leaves no financial work to repeat.
    IF EXISTS (
         SELECT 1 FROM public.pay_payment_correction_work_items AS work_row
         WHERE work_row.correction_request_id = p_correction_request_id
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_items AS correction_row
         WHERE correction_row.correction_request_id = p_correction_request_id
       )
       OR EXISTS (
         SELECT 1 FROM public.banking_pay_operation_provider_attempts AS provider_attempt
         WHERE provider_attempt.operation_id = v_operation.id
            OR provider_attempt.pay_batch_id = v_request.pay_batch_id
       )
       OR EXISTS (
         SELECT 1 FROM public.banking_pay_operation_transfer_scope AS transfer_scope
         WHERE transfer_scope.operation_id = v_operation.id
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_DRAFT_EXPAND_RETRY_EVIDENCE_EXISTS'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_DRAFT_EXPAND_RETRY_EVIDENCE_EXISTS'
        )::text;
    END IF;
  ELSE
    -- FINALISE and REFRESH_WORKBENCH are post-financial, idempotent route
    -- phases.  Resume only the same pre-bank request after at least one exact
    -- financial work item was applied and every work item is already terminal.
    -- The retry never rewinds to PROCESS_CHUNKS and never repeats financial DML.
    IF NOT EXISTS (
         SELECT 1 FROM public.pay_payment_correction_work_items AS applied_work
         WHERE applied_work.correction_request_id = p_correction_request_id
           AND applied_work.status = 'APPLIED'
       )
       OR EXISTS (
         SELECT 1 FROM public.pay_payment_correction_work_items AS unfinished_work
         WHERE unfinished_work.correction_request_id = p_correction_request_id
           AND unfinished_work.status NOT IN ('APPLIED', 'BLOCKED', 'CANCELLED', 'FAILED')
       )
       OR EXISTS (
         SELECT 1 FROM public.banking_pay_operation_provider_attempts AS provider_attempt
         WHERE provider_attempt.operation_id = v_operation.id
            OR provider_attempt.pay_batch_id = v_request.pay_batch_id
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_POST_FINANCIAL_RETRY_EVIDENCE_INVALID'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_POST_FINANCIAL_RETRY_EVIDENCE_INVALID'
        )::text;
    END IF;
  END IF;

  UPDATE public.banking_pay_operations AS operation_update
  SET status = 'RUNNING',
      runner_state = 'RUNNABLE',
      requires_user_action = false,
      run_after_utc = v_now,
      lease_owner = NULL,
      lease_expires_at_utc = NULL,
      locked_by = NULL,
      lock_expires_at_utc = NULL,
      error_json = NULL,
      resume_reason = v_retry_reason,
      progress_json = pg_catalog.jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          v_retry_counter_key, v_retry_count + 1,
          CASE WHEN v_retry_mode = 'PLANNING' THEN 'planning_retry_at_utc' ELSE 'processing_retry_at_utc' END, v_now::text,
          CASE WHEN v_retry_mode = 'PLANNING' THEN 'planning_retry_previous_error_code' ELSE 'processing_retry_previous_error_code' END, v_error_code,
          'safe_retry_mode', v_retry_mode,
          'status', 'RUNNING',
          'phase', v_operation.phase,
          'runner_state', 'RUNNABLE',
          'requires_user_action', false,
          'review_required', false,
          'resume_reason', v_retry_reason
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = v_operation.id;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'code', CASE WHEN v_retry_mode = 'PLANNING'
      THEN 'PAYMENT_CORRECTION_PLANNING_RETRY_QUEUED'
      ELSE 'PAYMENT_CORRECTION_PROCESSING_RETRY_QUEUED' END,
    'retry_mode', v_retry_mode,
    'correction_request_id', p_correction_request_id,
    'pay_batch_id', v_request.pay_batch_id,
    'operation_id', v_operation.id,
    'continuation', pg_catalog.jsonb_build_object(
      'required', true,
      'operation_id', v_operation.id,
      'operation_type', 'PAYMENT_CORRECTION',
      'pay_batch_id', v_request.pay_batch_id,
      'root_operation_id', v_operation.root_operation_id,
      'phase', v_operation.phase,
      'run_after_utc', v_now,
      'reason', v_retry_reason,
      'successor_relation', 'SELF',
      'requires_user_action', false,
      'terminal', false
    )
  );
END
$function$;

ALTER FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_retry_planning_v1(uuid, uuid) TO service_role;
