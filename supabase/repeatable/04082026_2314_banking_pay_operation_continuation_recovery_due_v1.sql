CREATE OR REPLACE FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(
  p_limit integer DEFAULT 25,
  p_stale_after_seconds integer DEFAULT 90,
  p_operation_types text[] DEFAULT ARRAY[
    'DRAFT_CREATE',
    'PAYMENT_EXECUTE',
    'PAYMENT_RETRY_BLOCKED_FUNDS',
    'PAYMENT_SETTLEMENT',
    'REMITTANCE_QUEUE',
    'PAYMENT_CORRECTION'
  ]::text[]
)
RETURNS jsonb
LANGUAGE sql
STABLE
PARALLEL RESTRICTED
SECURITY DEFINER
SET search_path = pg_catalog, private, extensions, pg_temp
SET statement_timeout = '5000ms'
AS $function$
WITH supplied_types AS (
  SELECT COALESCE(
    array_agg(DISTINCT upper(btrim(type_value)))
      FILTER (WHERE NULLIF(btrim(type_value), '') IS NOT NULL),
    ARRAY[]::text[]
  ) AS values
  FROM unnest(COALESCE(p_operation_types, ARRAY[]::text[])) AS supplied(type_value)
),
validated AS (
  SELECT
    LEAST(GREATEST(COALESCE(p_limit, 25), 1), 25) AS row_limit,
    LEAST(GREATEST(COALESCE(p_stale_after_seconds, 90), 90), 3600) AS caller_stale_seconds,
    supplied_types.values AS operation_types,
    clock_timestamp() AS checked_at_utc
  FROM supplied_types
),
eligible AS (
  SELECT
    operation_row.id AS operation_id,
    operation_row.operation_type,
    operation_row.pay_batch_id,
    operation_row.root_operation_id,
    operation_row.status,
    operation_row.phase,
    operation_row.run_after_utc,
    operation_row.requires_user_action,
    operation_row.runner_state,
    operation_row.resume_reason,
    operation_row.attempt_count,
    operation_row.max_attempts,
    operation_row.progress_json,
    validation.checked_at_utc,
    GREATEST(
      validation.caller_stale_seconds,
      COALESCE(operation_config.lock_seconds, NULLIF(operation_row.config_json->>'lock_seconds', '')::integer, 60)
        + CEIL(COALESCE(operation_config.max_advance_ms, NULLIF(operation_row.config_json->>'max_advance_ms', '')::integer, 7500) / 1000.0)::integer
        + 15
    ) AS effective_stale_seconds,
    COALESCE(
      NULLIF(operation_row.progress_json->>'continuation_witness_changed_at_utc', '')::timestamptz,
      operation_row.last_advanced_at_utc,
      operation_row.started_at_utc,
      operation_row.created_at_utc
    ) AS last_meaningful_activity_utc
  FROM public.banking_pay_operations AS operation_row
  CROSS JOIN validated AS validation
  LEFT JOIN LATERAL (
    SELECT config_row.lock_seconds, config_row.max_advance_ms
    FROM public.banking_pay_operation_config AS config_row
    WHERE upper(btrim(config_row.operation_type)) = upper(btrim(operation_row.operation_type))
      AND upper(btrim(config_row.phase)) IN (upper(btrim(operation_row.phase)), 'ALL')
    ORDER BY CASE WHEN upper(btrim(config_row.phase)) = upper(btrim(operation_row.phase)) THEN 0 ELSE 1 END,
             config_row.id
    LIMIT 1
  ) AS operation_config ON true
  WHERE COALESCE(array_length(validation.operation_types, 1), 0) > 0
    AND upper(btrim(operation_row.operation_type)) = ANY(validation.operation_types)
    AND (
      (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'RUNNING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) IN ('RUNNABLE', 'RUNNING', 'IDLE')
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) = 'RUNNABLE'
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) = 'WAITING_CHILD'
        AND COALESCE(operation_row.progress_json->>'child_operation_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        AND EXISTS (
          SELECT 1
          FROM public.banking_pay_operations AS child_operation
          WHERE child_operation.id = (operation_row.progress_json->>'child_operation_id')::uuid
            AND child_operation.root_operation_id = operation_row.id
            AND upper(btrim(COALESCE(child_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
        )
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING_PROVIDER'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) IN ('WAITING_PROVIDER', 'RUNNABLE')
        AND (
          (
            upper(btrim(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE'
            AND (
              upper(btrim(COALESCE(operation_row.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
              OR upper(btrim(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
              OR upper(btrim(COALESCE(operation_row.resume_reason, operation_row.progress_json->>'resume_reason', operation_row.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
            )
          )
          OR (
            upper(btrim(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
            AND (
              upper(btrim(COALESCE(operation_row.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
              OR upper(btrim(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
            )
          )
        )
      )
    )
    AND COALESCE(operation_row.requires_user_action, false) IS FALSE
    AND COALESCE(operation_row.attempt_count, 0) < COALESCE(operation_row.max_attempts, 10)
    AND COALESCE(NULLIF(operation_row.progress_json->>'continuation_no_progress_count', '')::integer, 0) < 5
    AND (operation_row.lease_owner IS NULL OR operation_row.lease_expires_at_utc IS NULL OR operation_row.lease_expires_at_utc <= validation.checked_at_utc)
    AND (operation_row.run_after_utc IS NULL OR operation_row.run_after_utc <= validation.checked_at_utc)
    AND NOT (
      upper(btrim(operation_row.status)) = 'WAITING_PROVIDER'
      AND operation_row.run_after_utc IS NULL
    )
),
due AS (
  SELECT eligible.*
  FROM eligible
  CROSS JOIN validated AS validation
  WHERE eligible.last_meaningful_activity_utc
        <= eligible.checked_at_utc - make_interval(secs => eligible.effective_stale_seconds)
  ORDER BY eligible.last_meaningful_activity_utc, eligible.operation_id
  LIMIT (SELECT row_limit FROM validated)
),
descriptors AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_strip_nulls(jsonb_build_object(
        'required', true,
        'operation_id', due.operation_id,
        'operation_type', due.operation_type,
        'pay_batch_id', due.pay_batch_id,
        'root_operation_id', due.root_operation_id,
        'phase', due.phase,
        'run_after_utc', due.run_after_utc,
        'reason', 'STRANDED_OPERATION_RECOVERY',
        'successor_relation', 'SELF',
        'requires_user_action', false,
        'terminal', false,
        'effective_stale_seconds', due.effective_stale_seconds,
        'last_meaningful_activity_utc', due.last_meaningful_activity_utc
      ))
      ORDER BY due.last_meaningful_activity_utc, due.operation_id
    ),
    '[]'::jsonb
  ) AS rows
  FROM due
)
SELECT jsonb_build_object(
  'ok', true,
  'checked_at_utc', (SELECT checked_at_utc FROM validated),
  'count', jsonb_array_length(descriptors.rows),
  'continuations', descriptors.rows,
  'code', 'BANKING_PAY_CONTINUATION_RECOVERY_DUE_OK'
)
FROM descriptors;
$function$;

ALTER FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) FROM anon;
REVOKE ALL ON FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) FROM authenticated;
REVOKE ALL ON FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) FROM service_role;
GRANT EXECUTE ON FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[]) TO service_role;
