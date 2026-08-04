-- CloudTMS Banking Pay cancellation — Stage 1.
-- Session-bound correction reauthentication proof binding.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_reauth_bind_v1(
    p_correction_request_id uuid,
    p_actor_user_id uuid,
    p_session_hash text,
    p_proof_hash text,
    p_issued_at_utc timestamptz,
    p_expires_at_utc timestamptz
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
    v_request public.pay_payment_correction_requests%ROWTYPE;
    v_code text;
    v_previous_hash text;
    v_previous_expiry timestamptz;
BEGIN
    IF p_correction_request_id IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REQUEST_NOT_FOUND',
            'message', 'The payment-correction request was not found.'
        );
    END IF;

    SELECT request_row.*
    INTO v_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.id = p_correction_request_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REQUEST_NOT_FOUND',
            'message', 'The payment-correction request was not found.'
        );
    END IF;

    IF v_request.status <> 'PLANNED' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REQUEST_STATE_INVALID',
            'message', 'This payment-correction plan is no longer awaiting reauthentication.'
        );
    END IF;

    IF p_actor_user_id IS NULL
       OR p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'ACTOR_MISMATCH',
            'message', 'Only the requester can reauthenticate this payment correction.'
        );
    END IF;

    IF p_session_hash IS NULL
       OR p_session_hash !~ '^[0-9a-f]{64}$'
       OR p_proof_hash IS NULL
       OR p_proof_hash !~ '^[0-9a-f]{64}$' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'HASH_INVALID',
            'message', 'The reauthentication binding is invalid.'
        );
    END IF;

    IF p_issued_at_utc IS NULL
       OR p_issued_at_utc <> pg_catalog.date_trunc('second', p_issued_at_utc)
       OR p_issued_at_utc > v_now + interval '30 seconds'
       OR p_issued_at_utc < v_now - interval '2 minutes' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REAUTH_ISSUED_AT_INVALID',
            'message', 'The reauthentication proof issue time is invalid.'
        );
    END IF;

    IF p_expires_at_utc IS NULL
       OR p_expires_at_utc <> pg_catalog.date_trunc('second', p_expires_at_utc)
       OR p_expires_at_utc <= v_now THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REAUTH_EXPIRED',
            'message', 'The reauthentication proof has expired.'
        );
    END IF;

    IF p_expires_at_utc > v_now + interval '10 minutes' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REAUTH_EXPIRY_EXCEEDS_DATABASE_LIMIT',
            'message', 'The reauthentication proof expiry exceeds the permitted limit.'
        );
    END IF;

    IF p_expires_at_utc <= p_issued_at_utc
       OR p_expires_at_utc > p_issued_at_utc + interval '10 minutes' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REAUTH_EXPIRY_BINDING_INVALID',
            'message', 'The signed reauthentication interval is invalid.'
        );
    END IF;

    IF p_expires_at_utc > v_request.requested_at_utc + interval '24 hours' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'REAUTH_EXPIRY_EXCEEDS_REQUEST_EXPIRY',
            'message', 'The reauthentication proof exceeds the request expiry.'
        );
    END IF;

    IF v_request.reauth_consumed_at_utc IS NOT NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'reauth_consumed_at_utc', v_request.reauth_consumed_at_utc,
            'code', 'REAUTH_ALREADY_CONSUMED',
            'message', 'This reauthentication proof has already been consumed.'
        );
    END IF;

    IF v_request.selection_hash IS NULL OR v_request.plan_hash IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'code', 'PAYMENT_CORRECTION_PLAN_STALE',
            'message', 'The payment-correction plan is incomplete. Refresh and start again.'
        );
    END IF;

    IF v_request.reauth_proof_hash = p_proof_hash
       AND v_request.reauth_expires_at_utc = p_expires_at_utc THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', true,
            'correction_request_id', p_correction_request_id,
            'actor_user_id', p_actor_user_id,
            'reauth_proof_hash', v_request.reauth_proof_hash,
            'signed_issued_at_utc', p_issued_at_utc,
            'reauth_expires_at_utc', v_request.reauth_expires_at_utc,
            'reauth_consumed_at_utc', v_request.reauth_consumed_at_utc,
            'plan_hash', v_request.plan_hash,
            'selection_hash', v_request.selection_hash,
            'code', 'REAUTH_BOUND_EXISTING',
            'message', NULL
        );
    END IF;

    v_previous_hash := v_request.reauth_proof_hash;
    v_previous_expiry := v_request.reauth_expires_at_utc;
    v_code := CASE
        WHEN v_previous_hash IS NULL THEN 'REAUTH_BOUND'
        ELSE 'REAUTH_REPLACED'
    END;

    UPDATE public.pay_payment_correction_requests AS request_to_bind
    SET reauth_proof_hash = p_proof_hash,
        reauth_expires_at_utc = p_expires_at_utc,
        reauth_consumed_at_utc = NULL,
        updated_at_utc = v_now
    WHERE request_to_bind.id = p_correction_request_id;

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
    VALUES (
        p_correction_request_id,
        v_request.pay_batch_id,
        'USER',
        p_actor_user_id,
        'APPLY',
        v_now,
        NULL,
        pg_catalog.jsonb_build_object(
            'proof_bound', v_previous_hash IS NOT NULL,
            'expires_at_utc', v_previous_expiry
        ),
        pg_catalog.jsonb_build_object(
            'proof_bound', true,
            'expires_at_utc', p_expires_at_utc
        ),
        pg_catalog.jsonb_build_object(
            'audit_code', v_code,
            'session_bound', true,
            'signed_issued_at_utc', p_issued_at_utc,
            'signed_expires_at_utc', p_expires_at_utc
        )
    );

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'correction_request_id', p_correction_request_id,
        'actor_user_id', p_actor_user_id,
        'reauth_proof_hash', p_proof_hash,
        'signed_issued_at_utc', p_issued_at_utc,
        'reauth_expires_at_utc', p_expires_at_utc,
        'reauth_consumed_at_utc', NULL,
        'plan_hash', v_request.plan_hash,
        'selection_hash', v_request.selection_hash,
        'code', v_code,
        'message', NULL
    );
END
$function$;

ALTER FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz) TO service_role;
