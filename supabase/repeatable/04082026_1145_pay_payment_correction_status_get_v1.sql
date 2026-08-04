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

    IF v_operation.id IS NOT NULL THEN
        SELECT pg_catalog.jsonb_build_object(
            'status', CASE
                WHEN pg_catalog.count(*) = 0 THEN 'NOT_STAGED'
                WHEN pg_catalog.count(*) FILTER (WHERE chunk_row.status = 'COMPLETE') = pg_catalog.count(*) THEN 'CURRENT'
                WHEN pg_catalog.count(*) FILTER (WHERE chunk_row.status IN ('FAILED', 'CANCELLED')) > 0 THEN 'FAILED'
                ELSE 'PENDING'
            END,
            'group_total', pg_catalog.count(*)::integer,
            'group_complete', pg_catalog.count(*) FILTER (WHERE chunk_row.status = 'COMPLETE')::integer
        )
        INTO v_workbench_refresh
        FROM public.banking_pay_operation_chunks AS chunk_row
        WHERE chunk_row.operation_id = v_operation.id
          AND chunk_row.phase = 'REFRESH_WORKBENCH';
    ELSE
        v_workbench_refresh := pg_catalog.jsonb_build_object(
            'status', 'NOT_STAGED',
            'group_total', 0,
            'group_complete', 0
        );
    END IF;

    v_progress := pg_catalog.jsonb_strip_nulls(
        pg_catalog.jsonb_build_object(
            'total_units', v_operation.total_units,
            'completed_units', v_operation.completed_units,
            'failed_units', v_operation.failed_units,
            'phase_message', v_operation_envelope ->> 'status_text',
            'requires_user_action', pg_catalog.coalesce(v_operation.requires_user_action, false)
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
                WHEN v_request.requested_at_utc <= pg_catalog.clock_timestamp() - interval '24 hours'
                    THEN 'This request expired. Refresh Current Payment Status and start again.'
                ELSE 'No further cancellation work will be performed.'
            END;
        ELSE
            v_user_title := 'Payment correction';
            v_user_message := 'Review the current payment status.';
    END CASE;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'correction_request_id', v_request.id,
        'operation_id', v_operation.id,
        'pay_batch_id', v_request.pay_batch_id,
        'request_status', v_request.status,
        'operation_status', v_operation.status,
        'phase', v_operation.phase,
        'progress', pg_catalog.coalesce(v_progress, '{}'::jsonb),
        'candidate_counts', pg_catalog.coalesce(v_candidate_counts, '{}'::jsonb),
        'workbench_refresh', pg_catalog.coalesce(v_workbench_refresh, '{}'::jsonb),
        'available_actions', v_available_actions,
        'user_title', v_user_title,
        'user_message', v_user_message,
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
