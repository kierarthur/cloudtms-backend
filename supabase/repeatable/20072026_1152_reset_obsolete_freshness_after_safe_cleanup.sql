-- Replace the public cleanup entry point while retaining the original bounded
-- cleanup implementation as a private base helper. A retry-safe, pre-provider
-- cleanup must also retire freshness evidence produced by that same failed
-- operation; otherwise the draft remains permanently marked STALE in the UI.

DO $bootstrap_base$
BEGIN
  IF to_regprocedure('public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid,uuid,text,jsonb,boolean)') IS NULL THEN
    IF to_regprocedure('public.pay_execute_operation_cleanup_failed_local_artifacts(uuid,uuid,text,jsonb,boolean)') IS NULL THEN
      RAISE EXCEPTION 'PAY_EXECUTE_CLEANUP_BASE_FUNCTION_REQUIRED';
    END IF;

    ALTER FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(uuid, uuid, text, jsonb, boolean)
      RENAME TO _pay_execute_operation_cleanup_failed_local_artifacts_base;
  END IF;
END;
$bootstrap_base$;

REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM anon;
REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM authenticated;
GRANT EXECUTE ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(
  p_operation_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_failure_phase text DEFAULT NULL::text,
  p_failure_error_json jsonb DEFAULT '{}'::jsonb,
  p_dry_run boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_result jsonb := '{}'::jsonb;
  v_pay_batch_id uuid := NULL::uuid;
  v_freshness_state_cleared integer := 0;
BEGIN
  v_result := COALESCE(public._pay_execute_operation_cleanup_failed_local_artifacts_base(
    p_operation_id,
    p_actor_user_id,
    p_failure_phase,
    p_failure_error_json,
    p_dry_run
  ), '{}'::jsonb);

  IF COALESCE(p_dry_run, false) IS FALSE
     AND COALESCE((v_result->>'safe_to_retry')::boolean, false) IS TRUE
     AND COALESCE((v_result->>'retry_blocked')::boolean, true) IS FALSE
     AND COALESCE((v_result->>'review_required')::boolean, true) IS FALSE THEN
    SELECT operation_row.pay_batch_id
    INTO v_pay_batch_id
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    LIMIT 1;

    UPDATE public.pay_batches AS batch_update
    SET freshness_operation_id = NULL::uuid,
        freshness_validation_status = NULL::text,
        freshness_checked_at_utc = NULL::timestamptz,
        freshness_result_hash = NULL::text,
        freshness_scope_hash = NULL::text,
        freshness_result_json = '{}'::jsonb
    WHERE batch_update.id = v_pay_batch_id
      AND batch_update.freshness_operation_id = p_operation_id
      AND upper(BTRIM(COALESCE(batch_update.status, ''))) IN ('DRAFT', 'DRAFT_CREATED')
      AND upper(BTRIM(COALESCE(batch_update.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
      AND NULLIF(BTRIM(COALESCE(batch_update.execution_commit_ref, '')), '') IS NULL
      AND batch_update.execution_committed_at_utc IS NULL;
    GET DIAGNOSTICS v_freshness_state_cleared = ROW_COUNT;

    IF v_freshness_state_cleared > 0 THEN
      BEGIN
        PERFORM public.pay_batch_display_summary_touch(v_pay_batch_id);
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;

      BEGIN
        PERFORM public.banking_pay_batch_signal_touch(
          v_pay_batch_id,
          'PAYMENT_EXECUTION_OBSOLETE_FRESHNESS_CLEARED',
          'pay_execute_operation_cleanup_failed_local_artifacts',
          jsonb_build_object(
            'operation_id', p_operation_id::text,
            'freshness_state_cleared', true,
            'safe_to_retry', true
          ),
          true,
          false,
          false,
          true
        );
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END IF;
  END IF;

  RETURN v_result || jsonb_build_object(
    'freshness_state_cleared', COALESCE(v_freshness_state_cleared, 0)
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(uuid, uuid, text, jsonb, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(uuid, uuid, text, jsonb, boolean) FROM anon;
GRANT EXECUTE ON FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(uuid, uuid, text, jsonb, boolean) TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(uuid, uuid, text, jsonb, boolean) TO service_role;
