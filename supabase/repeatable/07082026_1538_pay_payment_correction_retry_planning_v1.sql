-- CloudTMS Banking Pay cancellation — narrowly scoped planning retry.
--
-- This does not create a second request and cannot advance financial work.  It
-- only returns an existing PAYMENT_CORRECTION operation to PREPARE_SELECTION
-- after an explicit requester/admin retry where the earlier failure happened
-- before any durable selection, work, correction, provider, or chunk evidence.

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
  v_retry_count := CASE
    WHEN COALESCE(v_operation.progress_json ->> 'planning_retry_count', '') ~ '^[0-9]+$'
      THEN GREATEST((v_operation.progress_json ->> 'planning_retry_count')::integer, 0)
    ELSE 0
  END;

  IF pg_catalog.upper(COALESCE(v_request.status, '')) <> 'PLANNING'
     OR pg_catalog.upper(COALESCE(v_operation.status, '')) <> 'REVIEW_REQUIRED'
     OR pg_catalog.upper(COALESCE(v_operation.phase, '')) <> 'PREPARE_SELECTION'
     OR pg_catalog.upper(COALESCE(v_operation.runner_state, '')) <> 'WAITING_USER_REVIEW'
     OR COALESCE(v_operation.requires_user_action, false) IS NOT TRUE
     OR v_error_code <> 'BANKING_PAY_OPERATION_ADVANCE_FAILED'
     OR v_retry_count >= 3 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLANNING_RETRY_NOT_ALLOWED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_PLANNING_RETRY_NOT_ALLOWED',
        'request_status', v_request.status,
        'operation_status', v_operation.status,
        'operation_phase', v_operation.phase
      )::text;
  END IF;

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
      resume_reason = 'EXPLICIT_SAFE_PLANNING_RETRY',
      progress_json = pg_catalog.jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'planning_retry_count', v_retry_count + 1,
          'planning_retry_at_utc', v_now::text,
          'planning_retry_previous_error_code', v_error_code,
          'status', 'RUNNING',
          'phase', 'PREPARE_SELECTION',
          'runner_state', 'RUNNABLE',
          'requires_user_action', false,
          'resume_reason', 'EXPLICIT_SAFE_PLANNING_RETRY'
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = v_operation.id;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'code', 'PAYMENT_CORRECTION_PLANNING_RETRY_QUEUED',
    'correction_request_id', p_correction_request_id,
    'pay_batch_id', v_request.pay_batch_id,
    'operation_id', v_operation.id,
    'continuation', pg_catalog.jsonb_build_object(
      'required', true,
      'operation_id', v_operation.id,
      'operation_type', 'PAYMENT_CORRECTION',
      'pay_batch_id', v_request.pay_batch_id,
      'root_operation_id', v_operation.root_operation_id,
      'phase', 'PREPARE_SELECTION',
      'run_after_utc', v_now,
      'reason', 'EXPLICIT_SAFE_PLANNING_RETRY',
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
