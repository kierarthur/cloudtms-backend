-- CloudTMS Banking Pay cancellation — Stage 1.
-- Mode-aware Banking Pay mutation guard.

CREATE OR REPLACE FUNCTION private.pay_payment_mutation_guard_v1(
    p_pay_batch_id uuid,
    p_correction_request_id uuid DEFAULT NULL::uuid,
    p_mode text DEFAULT 'NEW_PAYMENT_ACTION'::text
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
    v_mode text := pg_catalog.upper(pg_catalog.btrim(coalesce(p_mode, '')));
    v_active_gate_request_id uuid;
    v_integrity_conflict_exists boolean := false;
    v_operation_continuity_exists boolean := false;
BEGIN
    IF p_pay_batch_id IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', NULL,
            'guard_acquired', false,
            'blocked', true,
            'code', 'PAYMENT_MUTATION_BATCH_REQUIRED',
            'message', 'A Banking Pay batch is required.'
        );
    END IF;

    IF v_mode NOT IN ('NEW_PAYMENT_ACTION', 'CORRECTION_APPLY', 'AUTHORITATIVE_EVENT') THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', NULL,
            'guard_acquired', false,
            'blocked', true,
            'code', 'PAYMENT_MUTATION_MODE_INVALID',
            'message', 'The Banking Pay mutation mode is invalid.'
        );
    END IF;

    IF v_mode = 'CORRECTION_APPLY' AND p_correction_request_id IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', NULL,
            'guard_acquired', false,
            'blocked', true,
            'code', 'PAYMENT_MUTATION_CORRECTION_REQUEST_REQUIRED',
            'message', 'The owning payment-correction request is required.'
        );
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'BANKING_PAY_BATCH_MUTATION:' || p_pay_batch_id::text,
            0
        )
    );

    IF NOT EXISTS (
        SELECT 1
        FROM public.pay_batches AS batch_row
        WHERE batch_row.id = p_pay_batch_id
    ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', NULL,
            'guard_acquired', true,
            'blocked', true,
            'code', 'PAYMENT_MUTATION_BATCH_REQUIRED',
            'message', 'The Banking Pay batch was not found.'
        );
    END IF;

    SELECT request_row.id
    INTO v_active_gate_request_id
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.pay_batch_id = p_pay_batch_id
      AND (
          request_row.status IN (
              'REQUESTED',
              'AWAITING_AUTHORISATION',
              'AUTHORISED',
              'EXPANDED',
              'PROCESSING'
          )
          OR EXISTS (
              SELECT 1
              FROM public.banking_pay_operations AS refresh_operation
              WHERE refresh_operation.operation_type = 'PAYMENT_CORRECTION'
                AND refresh_operation.pay_batch_id = request_row.pay_batch_id
                AND refresh_operation.input_json ->> 'correction_request_id' = request_row.id::text
                AND refresh_operation.phase = 'REFRESH_WORKBENCH'
                AND refresh_operation.status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED')
          )
      )
    ORDER BY request_row.requested_at_utc DESC, request_row.id DESC
    LIMIT 1;

    SELECT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS transfer_event
        WHERE transfer_event.normalised_state = 'COMPLETED'
          AND transfer_event.movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
          AND transfer_event.correction_disposition = 'BLOCKED'
          AND transfer_event.mapping_hints_json ->> 'business_code' = 'PAID_EVIDENCE_AFTER_RELEASE'
          AND (
              transfer_event.pay_batch_id = p_pay_batch_id
              OR EXISTS (
                  SELECT 1
                  FROM public.pay_batch_candidates AS guarded_candidate
                  WHERE guarded_candidate.pay_batch_id = p_pay_batch_id
                    AND guarded_candidate.candidate_id = transfer_event.candidate_id
              )
              OR EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS released_item
                  JOIN public.pay_batch_candidates AS released_candidate
                    ON released_candidate.id = released_item.pay_batch_candidate_id
                  JOIN public.pay_batch_candidates AS guarded_candidate
                    ON guarded_candidate.candidate_id = released_candidate.candidate_id
                   AND guarded_candidate.pay_batch_id = p_pay_batch_id
                  WHERE released_item.pay_bank_transfer_id = transfer_event.pay_bank_transfer_id
              )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM public.pay_payment_correction_actions AS review_action
              WHERE review_action.metadata_json ->> 'business_code' = 'PAID_EVIDENCE_AFTER_RELEASE'
                AND review_action.metadata_json ->> 'review_status' = 'ACKNOWLEDGED'
                AND (
                    review_action.metadata_json ->> 'bank_event_id'
                ) = transfer_event.id::text
          )
    )
    INTO v_integrity_conflict_exists;

    IF v_mode = 'AUTHORITATIVE_EVENT' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', true,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', v_active_gate_request_id,
            'guard_acquired', true,
            'blocked', false,
            'code', 'PAYMENT_MUTATION_GUARD_AUTHORITATIVE_EVENT',
            'message', NULL
        );
    END IF;

    IF v_integrity_conflict_exists THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', v_active_gate_request_id,
            'guard_acquired', true,
            'blocked', true,
            'code', 'PAID_EVIDENCE_AFTER_RELEASE_REVIEW_REQUIRED',
            'message', 'Finance review is required before another payment action.'
        );
    END IF;

    IF v_mode = 'NEW_PAYMENT_ACTION' AND v_active_gate_request_id IS NOT NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', v_active_gate_request_id,
            'guard_acquired', true,
            'blocked', true,
            'code', 'PAYMENT_CHANGE_IN_PROGRESS',
            'message', 'A payment correction is already in progress.'
        );
    END IF;

    IF v_mode = 'CORRECTION_APPLY' THEN
        IF v_active_gate_request_id IS DISTINCT FROM p_correction_request_id THEN
            RETURN pg_catalog.jsonb_build_object(
                'ok', false,
                'mode', v_mode,
                'pay_batch_id', p_pay_batch_id,
                'correction_request_id', p_correction_request_id,
                'active_gate_request_id', v_active_gate_request_id,
                'guard_acquired', true,
                'blocked', true,
                'code', 'PAYMENT_CORRECTION_GATE_OWNER_MISMATCH',
                'message', 'The payment correction does not own the active batch gate.'
            );
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM public.banking_pay_operations AS operation_row
            WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
              AND operation_row.pay_batch_id = p_pay_batch_id
              AND operation_row.input_json ->> 'correction_request_id' = p_correction_request_id::text
              AND operation_row.status NOT IN ('FAILED', 'CANCELLED')
        )
        INTO v_operation_continuity_exists;

        IF NOT v_operation_continuity_exists THEN
            RETURN pg_catalog.jsonb_build_object(
                'ok', false,
                'mode', v_mode,
                'pay_batch_id', p_pay_batch_id,
                'correction_request_id', p_correction_request_id,
                'active_gate_request_id', v_active_gate_request_id,
                'guard_acquired', true,
                'blocked', true,
                'code', 'PAYMENT_CORRECTION_GATE_OWNER_MISMATCH',
                'message', 'Durable payment-correction operation continuity was not found.'
            );
        END IF;

        RETURN pg_catalog.jsonb_build_object(
            'ok', true,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', v_active_gate_request_id,
            'guard_acquired', true,
            'blocked', false,
            'code', 'PAYMENT_MUTATION_GUARD_CORRECTION_OWNED',
            'message', NULL
        );
    END IF;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'mode', v_mode,
        'pay_batch_id', p_pay_batch_id,
        'correction_request_id', p_correction_request_id,
        'active_gate_request_id', v_active_gate_request_id,
        'guard_acquired', true,
        'blocked', false,
        'code', 'PAYMENT_MUTATION_GUARD_CLEAR',
        'message', NULL
    );
EXCEPTION
    WHEN lock_not_available OR query_canceled THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'mode', v_mode,
            'pay_batch_id', p_pay_batch_id,
            'correction_request_id', p_correction_request_id,
            'active_gate_request_id', v_active_gate_request_id,
            'guard_acquired', false,
            'blocked', true,
            'code', 'PAYMENT_MUTATION_LOCK_TIMEOUT',
            'message', 'The payment scope is busy. Try again.'
        );
END
$function$;

ALTER FUNCTION private.pay_payment_mutation_guard_v1(uuid,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_payment_mutation_guard_v1(uuid,uuid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_payment_mutation_guard_v1(uuid,uuid,text) FROM anon;
REVOKE ALL ON FUNCTION private.pay_payment_mutation_guard_v1(uuid,uuid,text) FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_payment_mutation_guard_v1(uuid,uuid,text) FROM service_role;
