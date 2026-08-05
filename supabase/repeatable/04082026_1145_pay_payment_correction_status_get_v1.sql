-- CloudTMS Banking Pay cancellation — Stage 1.
-- Bounded correction request and operation status read.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_status_get_v1(
    p_correction_request_id uuid,
    p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '3000ms'
AS $function$
DECLARE
    v_request public.pay_payment_correction_requests%ROWTYPE;
    v_operation public.banking_pay_operations%ROWTYPE;
    v_batch_status text;
    v_operation_envelope jsonb := '{}'::jsonb;
    v_candidate_counts jsonb := '{}'::jsonb;
    v_workbench_refresh jsonb := '{}'::jsonb;
    v_progress jsonb := '{}'::jsonb;
    v_available_actions text[] := ARRAY[]::text[];
    v_user_title text;
    v_user_message text;
    v_request_expired boolean := false;
    v_workbench_session_id uuid;
    v_refresh_group_total integer := 0;
    v_refresh_group_complete integer := 0;
    v_refresh_candidate_count integer := 0;
    v_refresh_failed_count integer := 0;
    v_refresh_pending_count integer := 0;
    v_refresh_ready_count integer := 0;
    v_workbench_status text := 'NOT_STAGED';
    v_progress_stage text := 'PLANNING';
    v_poll_after_ms integer := 1000;
    v_financial_complete boolean := false;
    v_terminal boolean := false;
    v_blockers jsonb := '[]'::jsonb;
BEGIN
    SELECT request_row.*
    INTO v_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.id = p_correction_request_id;

    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'code', 'REQUEST_NOT_FOUND'
        );
    END IF;

    IF p_actor_user_id IS NOT NULL
       AND p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id
       AND NOT EXISTS (
           SELECT 1
           FROM public.pay_payment_correction_actions AS actor_action
           WHERE actor_action.correction_request_id = p_correction_request_id
             AND actor_action.actor_user_id = p_actor_user_id
             AND actor_action.action IN ('AUTHORISE', 'USE_GOLDEN_KEY')
       ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'code', 'PERMISSION_DENIED'
        );
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
      AND operation_row.input_json ->> 'correction_request_id' = p_correction_request_id::text
    ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
    LIMIT 1;

    IF v_operation.id IS NOT NULL THEN
        v_operation_envelope := public.banking_pay_operation_get(
            v_operation.id,
            p_actor_user_id,
            'PROGRESS_LIGHT'
        );
    END IF;

    SELECT batch_row.status
    INTO v_batch_status
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = v_request.pay_batch_id;

    SELECT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_actions AS expiry_action
        WHERE expiry_action.correction_request_id = p_correction_request_id
          AND expiry_action.action = 'CANCEL'
          AND expiry_action.metadata_json ->> 'audit_code' = 'UNAPPROVED_REQUEST_EXPIRED'
    ) INTO v_request_expired;

    SELECT pg_catalog.jsonb_build_object(
        'total', pg_catalog.count(*)::integer,
        'pending', pg_catalog.count(*) FILTER (WHERE work_item.status = 'PENDING')::integer,
        'processing', pg_catalog.count(*) FILTER (WHERE work_item.status = 'PROCESSING')::integer,
        'applied', pg_catalog.count(*) FILTER (WHERE work_item.status = 'APPLIED')::integer,
        'blocked', pg_catalog.count(*) FILTER (WHERE work_item.status = 'BLOCKED')::integer,
        'failed_retryable', pg_catalog.count(*) FILTER (WHERE work_item.status = 'FAILED_RETRYABLE')::integer,
        'failed_final', pg_catalog.count(*) FILTER (WHERE work_item.status = 'FAILED_FINAL')::integer,
        'cancelled', pg_catalog.count(*) FILTER (WHERE work_item.status = 'CANCELLED')::integer
    )
    INTO v_candidate_counts
    FROM public.pay_payment_correction_work_items AS work_item
    WHERE work_item.correction_request_id = p_correction_request_id;

    SELECT COALESCE(
        pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object('code', blocker_summary.code, 'count', blocker_summary.blocker_count)
            ORDER BY blocker_summary.blocker_count DESC, blocker_summary.code
        ),
        '[]'::jsonb
    )
    INTO v_blockers
    FROM (
        SELECT blocker_source.code, pg_catalog.count(*)::integer AS blocker_count
        FROM (
            SELECT COALESCE(
                NULLIF(work_item.result_json->>'result_code', ''),
                NULLIF(work_item.result_json->>'code', ''),
                CASE
                    WHEN work_item.status = 'BLOCKED' THEN 'PAYMENT_CORRECTION_BLOCKED'
                    WHEN work_item.status = 'FAILED_FINAL' THEN 'PAYMENT_CORRECTION_FAILED_FINAL'
                    ELSE 'PAYMENT_CORRECTION_REVIEW_REQUIRED'
                END
            ) AS code
            FROM public.pay_payment_correction_work_items AS work_item
            WHERE work_item.correction_request_id = p_correction_request_id
              AND work_item.status IN ('BLOCKED', 'FAILED_FINAL')
        ) AS blocker_source
        GROUP BY blocker_source.code
        ORDER BY pg_catalog.count(*) DESC, blocker_source.code
        LIMIT 20
    ) AS blocker_summary;

    IF v_operation.id IS NOT NULL THEN
        SELECT pg_catalog.count(*)::integer,
               pg_catalog.count(*) FILTER (WHERE chunk_row.status = 'COMPLETE')::integer
        INTO v_refresh_group_total, v_refresh_group_complete
        FROM public.banking_pay_operation_chunks AS chunk_row
        WHERE chunk_row.operation_id = v_operation.id
          AND chunk_row.phase = 'REFRESH_WORKBENCH'
          AND chunk_row.chunk_type = 'CANDIDATE_SCOPE';

        v_workbench_session_id := COALESCE(
            v_operation.workbench_session_id,
            (SELECT batch_row.source_workbench_session_id FROM public.pay_batches AS batch_row WHERE batch_row.id = v_request.pay_batch_id)
        );

        WITH refresh_candidates AS (
            SELECT DISTINCT (candidate_token.value #>> '{}')::uuid AS candidate_id
            FROM public.banking_pay_operation_chunks AS refresh_chunk
            CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
                COALESCE(refresh_chunk.payload_json->'candidate_ids', '[]'::jsonb)
            ) AS candidate_token(value)
            WHERE refresh_chunk.operation_id = v_operation.id
              AND refresh_chunk.phase = 'REFRESH_WORKBENCH'
              AND refresh_chunk.chunk_type = 'CANDIDATE_SCOPE'
              AND (candidate_token.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ), candidate_freshness AS (
            SELECT refresh_candidate.candidate_id,
                   pg_catalog.upper(COALESCE(candidate_state.status, 'MISSING')) AS candidate_state,
                   candidate_state.pending_job_id,
                   COALESCE(candidate_state.source_change_seq, 0) AS source_change_seq,
                   pg_catalog.upper(COALESCE(latest_job.status, 'NONE')) AS job_status,
                   COALESCE(latest_job.scope_change_generation, 0) AS job_generation
            FROM refresh_candidates AS refresh_candidate
            LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
              ON candidate_state.session_id = v_workbench_session_id
             AND candidate_state.candidate_id = refresh_candidate.candidate_id
            LEFT JOIN LATERAL (
                SELECT job_row.status, job_row.scope_change_generation
                FROM public.banking_pay_workbench_jobs AS job_row
                WHERE job_row.session_id = v_workbench_session_id
                  AND job_row.candidate_id = refresh_candidate.candidate_id
                ORDER BY job_row.updated_at_utc DESC, job_row.id DESC
                LIMIT 1
            ) AS latest_job ON true
        )
        SELECT pg_catalog.count(*)::integer,
               pg_catalog.count(*) FILTER (WHERE candidate_freshness.candidate_state = 'FAILED' OR candidate_freshness.job_status IN ('FAILED', 'DEAD'))::integer,
               pg_catalog.count(*) FILTER (WHERE candidate_freshness.candidate_state = 'READY' AND candidate_freshness.pending_job_id IS NULL AND candidate_freshness.job_status IN ('NONE', 'SUCCEEDED') AND candidate_freshness.source_change_seq >= candidate_freshness.job_generation)::integer,
               pg_catalog.count(*) FILTER (WHERE NOT (candidate_freshness.candidate_state = 'FAILED' OR candidate_freshness.job_status IN ('FAILED', 'DEAD')) AND NOT (candidate_freshness.candidate_state = 'READY' AND candidate_freshness.pending_job_id IS NULL AND candidate_freshness.job_status IN ('NONE', 'SUCCEEDED') AND candidate_freshness.source_change_seq >= candidate_freshness.job_generation))::integer
        INTO v_refresh_candidate_count, v_refresh_failed_count, v_refresh_ready_count, v_refresh_pending_count
        FROM candidate_freshness;

        v_workbench_status := CASE
            WHEN v_refresh_group_total = 0 THEN 'NOT_STAGED'
            WHEN EXISTS (
                SELECT 1 FROM public.banking_pay_operation_chunks AS failed_chunk
                WHERE failed_chunk.operation_id = v_operation.id
                  AND failed_chunk.phase = 'REFRESH_WORKBENCH'
                  AND failed_chunk.status IN ('FAILED', 'FAILED_FINAL', 'CANCELLED')
            ) OR v_refresh_failed_count > 0 THEN 'FAILED'
            WHEN v_refresh_group_complete < v_refresh_group_total THEN 'STAGED'
            WHEN v_refresh_candidate_count = 0
              OR NOT EXISTS (
                  SELECT 1 FROM public.banking_pay_operation_chunks AS required_chunk
                  WHERE required_chunk.operation_id = v_operation.id
                    AND required_chunk.phase = 'REFRESH_WORKBENCH'
                    AND COALESCE(required_chunk.result_json->>'status', '') NOT LIKE 'NOT_REQUIRED%'
              ) THEN 'CURRENT'
            WHEN v_refresh_ready_count = v_refresh_candidate_count THEN 'CURRENT'
            WHEN v_refresh_pending_count > 0 THEN 'PENDING'
            ELSE 'STAGED'
        END;
    END IF;

    v_workbench_refresh := pg_catalog.jsonb_build_object(
        'status', v_workbench_status,
        'group_total', v_refresh_group_total,
        'group_complete', v_refresh_group_complete,
        'candidate_total', v_refresh_candidate_count,
        'candidate_ready', v_refresh_ready_count,
        'candidate_pending', v_refresh_pending_count,
        'candidate_failed', v_refresh_failed_count
    );

    v_progress := pg_catalog.jsonb_strip_nulls(
        pg_catalog.jsonb_build_object(
            'total_units', v_operation.total_units,
            'completed_units', v_operation.completed_units,
            'failed_units', v_operation.failed_units,
            'phase_message', v_operation_envelope ->> 'status_text',
            'requires_user_action', COALESCE(v_operation.requires_user_action, false)
        )
    );

    CASE v_request.status
        WHEN 'PLANNING' THEN
            v_user_title := 'Preparing cancellation';
            v_user_message := 'CloudTMS is preparing the exact payments for review.';
            v_available_actions := ARRAY['CANCEL_REQUEST']::text[];
        WHEN 'PLANNED' THEN
            v_user_title := 'Ready to review';
            v_user_message := 'Review the exact payments, then confirm your identity.';
            v_available_actions := ARRAY['REAUTHENTICATE', 'CANCEL_REQUEST']::text[];
        WHEN 'REQUESTED' THEN
            v_user_title := 'Cancellation requested';
            v_user_message := 'The cancellation is awaiting authorisation.';
            v_available_actions := ARRAY['CANCEL_REQUEST']::text[];
        WHEN 'AWAITING_AUTHORISATION' THEN
            v_user_title := 'Awaiting authorisation';
            v_user_message := 'The cancellation is awaiting the configured financial approval.';
            v_available_actions := ARRAY['AUTHORISE', 'REJECT']::text[];
        WHEN 'AUTHORISED' THEN
            v_user_title := 'Cancellation approved and queued';
            v_user_message := 'CloudTMS will process the cancellation safely.';
        WHEN 'EXPANDED' THEN
            v_user_title := 'Cancellation ready to process';
            v_user_message := 'CloudTMS has prepared the cancellation work.';
        WHEN 'PROCESSING' THEN
            v_user_title := 'Cancelling payments';
            v_user_message := 'CloudTMS is processing the selected payments.';
        WHEN 'APPLIED' THEN
            v_user_title := 'Cancellation complete';
            v_user_message := CASE
                WHEN v_workbench_refresh ->> 'status' = 'CURRENT'
                    THEN 'Payment availability is up to date.'
                ELSE 'Payment availability is refreshing.'
            END;
            IF v_batch_status = 'AWAITING_AUTHORISATION' THEN
                v_available_actions := ARRAY['REAUTHORISE_REMAINING']::text[];
            END IF;
        WHEN 'APPLIED_WITH_BLOCKERS' THEN
            v_user_title := 'Cancellation complete with blockers';
            v_user_message := 'Some selected payments were cancelled. Review the payments that remain active.';
            v_available_actions := ARRAY['REAUTHORISE_REMAINING']::text[];
        WHEN 'BLOCKED' THEN
            v_user_title := 'No payment was cancelled';
            v_user_message := 'Review the payment status and reauthorise the intact batch where permitted.';
            v_available_actions := ARRAY['REAUTHORISE_REMAINING']::text[];
        WHEN 'FAILED' THEN
            v_user_title := 'CloudTMS safely stopped this request';
            v_user_message := 'No further automatic action will be taken. Review the remaining payment scope.';
        WHEN 'REJECTED' THEN
            v_user_title := 'Cancellation rejected';
            v_user_message := 'No payment was changed by this request.';
        WHEN 'CANCELLED' THEN
            v_user_title := 'Cancellation request ended';
            v_user_message := CASE
                WHEN v_request_expired
                    THEN 'This request expired. Refresh Current Payment Status and start again.'
                ELSE 'No further cancellation work will be performed.'
            END;
        ELSE
            v_user_title := 'Payment correction';
            v_user_message := 'Review the current payment status.';
    END CASE;

    v_financial_complete := v_request.status IN (
        'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
    );

    v_progress_stage := CASE
        WHEN v_request.status = 'PLANNING' THEN 'PLANNING'
        WHEN v_request.status = 'PLANNED' THEN 'REVIEW'
        WHEN v_request.status = 'REQUESTED' THEN 'AUTHORISATION'
        WHEN v_request.status = 'AWAITING_AUTHORISATION' THEN 'AUTHORISATION'
        WHEN v_request.status = 'AUTHORISED' THEN 'EXPANDING'
        WHEN v_request.status = 'EXPANDED' THEN 'PROCESSING'
        WHEN v_request.status = 'PROCESSING' AND v_operation.phase = 'FINALISE' THEN 'FINALISING'
        WHEN v_request.status = 'PROCESSING' AND v_operation.phase = 'REFRESH_WORKBENCH' THEN 'REFRESHING_AVAILABILITY'
        WHEN v_request.status = 'PROCESSING' THEN 'PROCESSING'
        WHEN v_request.status = 'APPLIED' AND v_workbench_status <> 'CURRENT' THEN 'REFRESHING_AVAILABILITY'
        WHEN v_request.status = 'APPLIED' THEN 'COMPLETE'
        WHEN v_request.status = 'APPLIED_WITH_BLOCKERS' THEN 'COMPLETE_WITH_BLOCKERS'
        WHEN v_request.status = 'BLOCKED' THEN 'BLOCKED'
        WHEN v_request.status = 'FAILED' THEN 'FAILED'
        WHEN v_request.status = 'REJECTED' THEN 'REJECTED'
        WHEN v_request.status = 'CANCELLED' THEN 'CANCELLED'
        ELSE 'PROCESSING'
    END;

    v_terminal := v_financial_complete
      AND (v_operation.id IS NULL OR v_operation.status IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'))
      AND v_workbench_status IN ('CURRENT', 'FAILED', 'NOT_STAGED');

    v_poll_after_ms := CASE
        WHEN v_terminal THEN NULL
        WHEN v_progress_stage IN ('REVIEW', 'AUTHORISATION', 'REFRESHING_AVAILABILITY') THEN 5000
        ELSE 1000
    END;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'correction_request_id', v_request.id,
        'operation_id', v_operation.id,
        'pay_batch_id', v_request.pay_batch_id,
        'request_status', v_request.status,
        'operation_status', v_operation.status,
        'phase', v_operation.phase,
        'progress_stage', v_progress_stage,
        'poll_after_ms', v_poll_after_ms,
        'financial_complete', v_financial_complete,
        'terminal', v_terminal,
        'progress', COALESCE(v_progress, '{}'::jsonb),
        'candidate_counts', COALESCE(v_candidate_counts, '{}'::jsonb),
        'blockers', COALESCE(v_blockers, '[]'::jsonb),
        'workbench_refresh', COALESCE(v_workbench_refresh, '{}'::jsonb),
        'available_actions', v_available_actions,
        'user_title', v_user_title,
        'user_message', v_user_message,
        'continuation', pg_catalog.jsonb_build_object(
            'required', false,
            'operation_id', v_operation.id,
            'operation_type', 'PAYMENT_CORRECTION',
            'pay_batch_id', v_request.pay_batch_id,
            'root_operation_id', v_operation.root_operation_id,
            'phase', v_operation.phase,
            'run_after_utc', v_operation.run_after_utc,
            'reason', 'STATUS_READ_ONLY',
            'successor_relation', 'NONE',
            'requires_user_action', COALESCE(v_operation.requires_user_action, false),
            'terminal', v_terminal
        ),
        'code', 'PAYMENT_CORRECTION_STATUS_OK'
    );
END
$function$;

ALTER FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_status_get_v1(uuid,uuid) TO service_role;
