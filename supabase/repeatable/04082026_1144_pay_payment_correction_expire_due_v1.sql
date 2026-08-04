-- CloudTMS Banking Pay cancellation — Stage 1.
-- Bounded expiry of unapproved correction requests.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_expire_due_v1(
    p_limit integer DEFAULT 50
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '6000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_claimed_count integer := 0;
    v_expired_count integer := 0;
    v_skipped_count integer := 0;
    v_request record;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 50 THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'checked_at_utc', v_now,
            'claimed_count', 0,
            'expired_count', 0,
            'skipped_count', 0,
            'code', 'EXPIRY_LIMIT_INVALID',
            'message', 'The expiry drain limit must be between 1 and 50.'
        );
    END IF;

    IF NOT pg_catalog.pg_try_advisory_xact_lock(
        pg_catalog.hashtextextended('BANKING_PAY_PAYMENT_CORRECTION_EXPIRY', 0)
    ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', true,
            'checked_at_utc', v_now,
            'claimed_count', 0,
            'expired_count', 0,
            'skipped_count', 0,
            'code', 'EXPIRY_OVERLAP_SKIPPED',
            'message', NULL
        );
    END IF;

    FOR v_request IN
        SELECT request_row.id,
               request_row.pay_batch_id,
               request_row.status,
               request_row.requested_by_user_id,
               request_row.requested_at_utc,
               request_row.authorised_at_utc
        FROM public.pay_payment_correction_requests AS request_row
        WHERE request_row.status IN (
                  'PLANNING',
                  'PLANNED',
                  'REQUESTED',
                  'AWAITING_AUTHORISATION'
              )
          AND request_row.requested_at_utc <= v_now - interval '24 hours'
          AND request_row.authorised_at_utc IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_work_items AS work_item
              WHERE work_item.correction_request_id = request_row.id
                AND work_item.status = 'APPLIED'
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_items AS correction_item
              WHERE correction_item.correction_request_id = request_row.id
                AND correction_item.status = 'APPLIED'
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_actions AS effect_action
              WHERE effect_action.correction_request_id = request_row.id
                AND (
                    pg_catalog.lower(COALESCE(effect_action.metadata_json->>'provider_outcome_changed', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on')
                    OR pg_catalog.lower(COALESCE(effect_action.metadata_json->>'settlement_outcome_changed', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on')
                    OR effect_action.metadata_json->>'effect_code' IN (
                        'PROVIDER_OUTCOME_CHANGED_BY_REQUEST',
                        'SETTLEMENT_OUTCOME_CHANGED_BY_REQUEST'
                    )
                )
          )
        ORDER BY request_row.requested_at_utc, request_row.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    LOOP
        v_claimed_count := v_claimed_count + 1;

        UPDATE public.pay_payment_correction_requests AS request_to_expire
        SET status = 'CANCELLED',
            cancelled_at_utc = v_now,
            reauth_proof_hash = NULL,
            reauth_expires_at_utc = NULL,
            reauth_consumed_at_utc = NULL,
            updated_at_utc = v_now
        WHERE request_to_expire.id = v_request.id
          AND request_to_expire.status IN (
              'PLANNING',
              'PLANNED',
              'REQUESTED',
              'AWAITING_AUTHORISATION'
          )
          AND request_to_expire.authorised_at_utc IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_work_items AS work_item
              WHERE work_item.correction_request_id = request_to_expire.id
                AND work_item.status = 'APPLIED'
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_items AS correction_item
              WHERE correction_item.correction_request_id = request_to_expire.id
                AND correction_item.status = 'APPLIED'
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_actions AS effect_action
              WHERE effect_action.correction_request_id = request_to_expire.id
                AND (
                    pg_catalog.lower(COALESCE(effect_action.metadata_json->>'provider_outcome_changed', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on')
                    OR pg_catalog.lower(COALESCE(effect_action.metadata_json->>'settlement_outcome_changed', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on')
                    OR effect_action.metadata_json->>'effect_code' IN (
                        'PROVIDER_OUTCOME_CHANGED_BY_REQUEST',
                        'SETTLEMENT_OUTCOME_CHANGED_BY_REQUEST'
                    )
                )
          );

        IF NOT FOUND THEN
            v_skipped_count := v_skipped_count + 1;
            CONTINUE;
        END IF;

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
        )
        SELECT
            v_request.id,
            v_request.pay_batch_id,
            'SYSTEM',
            NULL,
            'CANCEL',
            v_now,
            NULL,
            pg_catalog.jsonb_build_object('status', v_request.status),
            pg_catalog.jsonb_build_object('status', 'CANCELLED'),
            pg_catalog.jsonb_build_object(
                'audit_code', 'UNAPPROVED_REQUEST_EXPIRED',
                'expired_at_utc', v_now,
                'membership_retained', true
            )
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.pay_payment_correction_actions AS existing_action
            WHERE existing_action.correction_request_id = v_request.id
              AND existing_action.action = 'CANCEL'
              AND existing_action.metadata_json ->> 'audit_code' = 'UNAPPROVED_REQUEST_EXPIRED'
        );

        UPDATE public.banking_pay_operations AS operation_to_cancel
        SET status = 'CANCELLED',
            phase = 'COMPLETE',
            result_json = COALESCE(operation_to_cancel.result_json, '{}'::jsonb)
                || pg_catalog.jsonb_build_object(
                    'code', 'UNAPPROVED_REQUEST_EXPIRED',
                    'correction_request_id', v_request.id,
                    'expired_at_utc', v_now,
                    'membership_retained', true
                ),
            completed_at_utc = COALESCE(operation_to_cancel.completed_at_utc, v_now),
            requires_user_action = false,
            locked_by = NULL,
            lock_expires_at_utc = NULL,
            lease_owner = NULL,
            lease_expires_at_utc = NULL,
            run_after_utc = NULL
        WHERE operation_to_cancel.operation_type = 'PAYMENT_CORRECTION'
          AND operation_to_cancel.input_json ->> 'correction_request_id' = v_request.id::text
          AND operation_to_cancel.status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED');

        v_expired_count := v_expired_count + 1;
    END LOOP;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'checked_at_utc', v_now,
        'claimed_count', v_claimed_count,
        'expired_count', v_expired_count,
        'skipped_count', v_skipped_count,
        'code', 'EXPIRY_DRAIN_OK',
        'message', NULL
    );
END
$function$;

ALTER FUNCTION public.pay_payment_correction_expire_due_v1(integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expire_due_v1(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expire_due_v1(integer) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expire_due_v1(integer) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expire_due_v1(integer) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_expire_due_v1(integer) TO service_role;
