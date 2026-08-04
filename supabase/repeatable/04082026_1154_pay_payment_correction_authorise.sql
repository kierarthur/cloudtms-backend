-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Final approval invalidates old authority and queues expansion atomically.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_authorise(
  p_correction_request_id uuid,
  p_actor_user_id uuid,
  p_action text,
  p_note text DEFAULT NULL::text
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
  v_action text := pg_catalog.upper(
    pg_catalog.btrim(coalesce(p_action, ''))
  );
  v_pay_batch_id uuid;
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_batch public.pay_batches%ROWTYPE;
  v_actor public.tms_users%ROWTYPE;
  v_guard jsonb;
  v_before_request jsonb;
  v_after_request jsonb;
  v_new_approved_count integer;
  v_selected_candidate_count integer;
  v_selected_active_item_count integer;
  v_selected_amount_pence bigint;
  v_old_auth_request_ids jsonb := '[]'::jsonb;
  v_old_schedule_kind text;
  v_old_scheduled_at_utc timestamptz;
  v_requested_action text;
  v_active_scope_hash text;
BEGIN
  IF p_correction_request_id IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTHORISE_IDENTIFIERS_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_AUTHORISE_IDENTIFIERS_REQUIRED'
      )::text;
  END IF;

  IF v_action NOT IN (
    'AUTHORISE',
    'USE_GOLDEN_KEY',
    'REJECT',
    'CANCEL'
  ) THEN
    RAISE EXCEPTION 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION',
        'action', p_action
      )::text;
  END IF;

  SELECT request_lookup.pay_batch_id
  INTO v_pay_batch_id
  FROM public.pay_payment_correction_requests AS request_lookup
  WHERE request_lookup.id = p_correction_request_id;

  IF v_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
        'correction_request_id', p_correction_request_id
      )::text;
  END IF;

  v_guard := private.pay_payment_mutation_guard_v1(
    v_pay_batch_id,
    p_correction_request_id,
    'CORRECTION_APPLY'
  );
  IF coalesce((v_guard->>'blocked')::boolean, true) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_GATE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', coalesce(
          v_guard->>'code',
          'PAYMENT_CORRECTION_GATE_OWNER_MISMATCH'
        ),
        'message', v_guard->>'message'
      )::text;
  END IF;

  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF v_request.status IN (
    'APPLIED',
    'APPLIED_WITH_BLOCKERS',
    'BLOCKED',
    'FAILED',
    'REJECTED',
    'CANCELLED'
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'request_id', v_request.id,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'request_status', v_request.status,
      'operation_id', (
        SELECT operation_row.id
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
          AND operation_row.input_json->>'correction_request_id'
              = v_request.id::text
        ORDER BY operation_row.created_at_utc DESC
        LIMIT 1
      ),
      'operation_status', (
        SELECT operation_row.status
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
          AND operation_row.input_json->>'correction_request_id'
              = v_request.id::text
        ORDER BY operation_row.created_at_utc DESC
        LIMIT 1
      ),
      'operation_phase', (
        SELECT operation_row.phase
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
          AND operation_row.input_json->>'correction_request_id'
              = v_request.id::text
        ORDER BY operation_row.created_at_utc DESC
        LIMIT 1
      ),
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'old_authorisation_invalidated',
        v_request.plan_json->>'old_authorisation_invalidated_at_utc'
          IS NOT NULL,
      'old_schedule_invalidated',
        v_request.plan_json->>'old_schedule_invalidated_at_utc'
          IS NOT NULL,
      'batch_status', (
        SELECT terminal_batch.status
        FROM public.pay_batches AS terminal_batch
        WHERE terminal_batch.id = v_request.pay_batch_id
      ),
      'gate_active', false,
      'display_message', 'This cancellation request is already complete.',
      'code', 'REQUEST_ALREADY_TERMINAL'
    );
  END IF;

  IF v_request.status NOT IN (
    'REQUESTED',
    'AWAITING_AUTHORISATION',
    'AUTHORISED'
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE',
        'status', v_request.status
      )::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_request.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_BATCH_NOT_FOUND'
      )::text;
  END IF;

  SELECT actor_row.*
  INTO v_actor
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id;

  IF NOT FOUND OR coalesce(v_actor.is_active, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTHORISER_NOT_ACTIVE'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PERMISSION_DENIED'
      )::text;
  END IF;

  IF v_action = 'AUTHORISE'
     AND coalesce(v_actor.payment_authoriser, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'ACTOR_IS_NOT_PAYMENT_AUTHORISER'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PERMISSION_DENIED'
      )::text;
  END IF;

  IF v_action = 'USE_GOLDEN_KEY'
     AND coalesce(v_actor.payment_golden_key, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'ACTOR_DOES_NOT_HAVE_PAYMENT_GOLDEN_KEY'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PERMISSION_DENIED'
      )::text;
  END IF;

  IF v_action = 'AUTHORISE'
     AND p_actor_user_id = v_request.requested_by_user_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_MAKER_CHECKER_VIOLATION'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'MAKER_CHECKER_VIOLATION'
      )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_actions AS existing_action
    WHERE existing_action.correction_request_id = v_request.id
      AND existing_action.actor_user_id = p_actor_user_id
      AND existing_action.action IN (
        'AUTHORISE',
        'USE_GOLDEN_KEY',
        'REJECT',
        'CANCEL'
      )
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_DUPLICATE_AUTHORISER'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DUPLICATE_AUTHORISER'
      )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.pay_batch_id = v_request.pay_batch_id
    AND operation_row.input_json->>'correction_request_id'
        = v_request.id::text
  ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND OR v_operation.status IN (
    'COMPLETE',
    'FAILED',
    'CANCELLED'
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'OPERATION_MISMATCH'
      )::text;
  END IF;

  v_before_request := pg_catalog.to_jsonb(v_request);

  IF v_action IN ('REJECT', 'CANCEL') THEN
    UPDATE public.pay_payment_correction_requests AS terminal_request
    SET status = CASE
          WHEN v_action = 'REJECT' THEN 'REJECTED'
          ELSE 'CANCELLED'
        END,
        cancelled_at_utc = CASE
          WHEN v_action = 'CANCEL' THEN
            coalesce(terminal_request.cancelled_at_utc, v_now)
          ELSE terminal_request.cancelled_at_utc
        END,
        reauth_proof_hash = NULL,
        reauth_expires_at_utc = NULL,
        reauth_consumed_at_utc =
          coalesce(terminal_request.reauth_consumed_at_utc, v_now),
        updated_at_utc = v_now
    WHERE terminal_request.id = v_request.id
    RETURNING terminal_request.* INTO v_request;

    UPDATE public.banking_pay_operations AS terminal_operation
    SET status = 'CANCELLED',
        phase = 'COMPLETE',
        runner_state = 'CANCELLED',
        requires_user_action = false,
        result_json = coalesce(
          terminal_operation.result_json,
          '{}'::jsonb
        ) || pg_catalog.jsonb_build_object(
          'code', CASE
            WHEN v_action = 'REJECT' THEN 'REQUEST_REJECTED'
            ELSE 'REQUEST_CANCELLED'
          END,
          'correction_request_id', v_request.id
        ),
        lease_owner = NULL,
        lease_expires_at_utc = NULL,
        locked_by = NULL,
        lock_expires_at_utc = NULL,
        run_after_utc = NULL,
        completed_at_utc = coalesce(
          terminal_operation.completed_at_utc,
          v_now
        ),
        updated_at_utc = v_now
    WHERE terminal_operation.id = v_operation.id;

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
    ) VALUES (
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      v_action,
      v_now,
      NULLIF(pg_catalog.btrim(coalesce(p_note, '')), ''),
      v_before_request,
      pg_catalog.to_jsonb(v_request),
      pg_catalog.jsonb_build_object(
        'code', CASE
          WHEN v_action = 'REJECT' THEN 'REQUEST_REJECTED'
          ELSE 'REQUEST_CANCELLED'
        END
      )
    );

    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'request_id', v_request.id,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'operation_id', v_operation.id,
      'action', v_action,
      'status', v_request.status,
      'request_status', v_request.status,
      'operation_status', 'CANCELLED',
      'operation_phase', 'COMPLETE',
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'old_authorisation_invalidated', false,
      'old_schedule_invalidated', false,
      'batch_status', v_batch.status,
      'gate_active', false,
      'display_message', CASE
        WHEN v_action = 'REJECT' THEN 'Cancellation request rejected.'
        ELSE 'Cancellation request cancelled.'
      END,
      'authorised', false,
      'queued', false,
      'code', CASE
        WHEN v_action = 'REJECT' THEN 'REQUEST_REJECTED'
        ELSE 'REQUEST_CANCELLED'
      END
    );
  END IF;

  v_active_scope_hash := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'version', 1,
      'pay_batch_id', v_batch.id,
      'batch_status', v_batch.status,
      'total_bank_out_pence', pg_catalog.round(coalesce(v_batch.total_bank_out, 0) * 100)::bigint,
      'source_scope_change_generation', v_batch.source_scope_change_generation,
      'execution_commit_state', v_batch.execution_commit_state,
      'execution_commit_ref', v_batch.execution_commit_ref,
      'execution_committed_at_utc', v_batch.execution_committed_at_utc,
      'execution_intent_json', v_batch.execution_intent_json,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot,
      'rail_env_snapshot', v_batch.rail_env_snapshot,
      'freshness_validation_status', v_batch.freshness_validation_status,
      'freshness_result_hash', v_batch.freshness_result_hash,
      'freshness_scope_hash', v_batch.freshness_scope_hash,
      'schedule_kind', v_batch.schedule_kind,
      'scheduled_at_utc', v_batch.scheduled_at_utc,
      'active_authorisation', (
        SELECT pg_catalog.jsonb_build_object(
          'auth_request_id', auth_row.id,
          'state', auth_row.state,
          'required_quantity', auth_row.required_quantity,
          'schedule_kind', auth_row.schedule_kind,
          'scheduled_at_utc', auth_row.scheduled_at_utc,
          'execution_intent_json', auth_row.execution_intent_json
        )
        FROM public.pay_batch_auth_requests AS auth_row
        WHERE auth_row.pay_batch_id = v_batch.id
          AND auth_row.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
        ORDER BY auth_row.created_at_utc DESC, auth_row.id DESC
        LIMIT 1
      ),
      'active_candidate_items', (
        SELECT coalesce(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'pay_batch_candidate_id', candidate_scope.id,
              'candidate_id', candidate_scope.candidate_id,
              'settlement_status', candidate_scope.settlement_status,
              'net_bank_amount_pence', pg_catalog.round(
                coalesce(candidate_scope.net_bank_amount, 0) * 100
              )::bigint,
              'pay_batch_item_id', item_scope.id,
              'item_type', item_scope.item_type,
              'amount_ex_vat_pence', pg_catalog.round(
                coalesce(item_scope.amount_ex_vat, 0) * 100
              )::bigint,
              'amount_vat_pence', pg_catalog.round(
                coalesce(item_scope.amount_vat, 0) * 100
              )::bigint,
              'amount_inc_vat_pence', pg_catalog.round(
                coalesce(item_scope.amount_inc_vat, 0) * 100
              )::bigint,
              'frozen_component_key_type', item_scope.frozen_component_key_type,
              'frozen_component_key_value', item_scope.frozen_component_key_value,
              'frozen_component_classification', item_scope.frozen_component_classification,
              'frozen_source_basis_json', item_scope.frozen_source_basis_json,
              'operation_source_key', item_scope.operation_source_key,
              'reservation_id', item_scope.reservation_id,
              'finance_component_id', item_scope.finance_component_id,
              'pay_bank_transfer_id', item_scope.pay_bank_transfer_id
            )
            ORDER BY candidate_scope.id, item_scope.id
          ),
          '[]'::jsonb
        )
        FROM public.pay_batch_candidates AS candidate_scope
        JOIN public.pay_batch_items AS item_scope
          ON item_scope.pay_batch_candidate_id = candidate_scope.id
         AND coalesce(item_scope.is_voided, false) IS NOT TRUE
        WHERE candidate_scope.pay_batch_id = v_batch.id
      ),
      'provider_scope', (
        SELECT coalesce(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'pay_bank_transfer_id', transfer_scope.id,
              'status', transfer_scope.status,
              'rail_state', transfer_scope.rail_state,
              'request_id', transfer_scope.request_id,
              'rail_tx_id', transfer_scope.rail_tx_id,
              'transfer_group_key', transfer_scope.transfer_group_key,
              'amount_pence', pg_catalog.round(
                coalesce(transfer_scope.amount, 0) * 100
              )::bigint
            )
            ORDER BY transfer_scope.id
          ),
          '[]'::jsonb
        )
        FROM public.pay_bank_transfers AS transfer_scope
        WHERE transfer_scope.pay_batch_id = v_batch.id
      ),
      'provider_events', (
        SELECT coalesce(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'bank_event_id', event_scope.id,
              'pay_bank_transfer_id', event_scope.pay_bank_transfer_id,
              'normalised_state', event_scope.normalised_state,
              'movement_classification', event_scope.movement_classification,
              'correction_disposition', event_scope.correction_disposition,
              'mapping_status', event_scope.mapping_status,
              'event_time_utc', event_scope.event_time_utc,
              'provider_request_id', event_scope.provider_request_id,
              'provider_transaction_id', event_scope.provider_transaction_id
            )
            ORDER BY event_scope.id
          ),
          '[]'::jsonb
        )
        FROM public.pay_bank_transfer_events AS event_scope
        WHERE event_scope.pay_batch_id = v_batch.id
      ),
      'candidate_count', (
        SELECT pg_catalog.count(*)
        FROM public.pay_batch_candidates AS candidate_row
        WHERE candidate_row.pay_batch_id = v_batch.id
      ),
      'active_item_count', (
        SELECT pg_catalog.count(*)
        FROM public.pay_batch_items AS item_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.id = item_row.pay_batch_candidate_id
        WHERE candidate_row.pay_batch_id = v_batch.id
          AND coalesce(item_row.is_voided, false) IS NOT TRUE
      ),
      'latest_request_update', (
        SELECT pg_catalog.max(other_request.updated_at_utc)
        FROM public.pay_payment_correction_requests AS other_request
        WHERE other_request.pay_batch_id = v_batch.id
          AND other_request.id <> v_request.id
          AND other_request.status NOT IN ('PLANNING', 'PLANNED')
      ),
      'latest_provider_event', (
        SELECT pg_catalog.max(event_row.received_at_utc)
        FROM public.pay_bank_transfer_events AS event_row
        WHERE event_row.pay_batch_id = v_batch.id
      )
    )
  );

  IF v_request.plan_json ->> 'active_batch_scope_hash'
       IS DISTINCT FROM v_active_scope_hash THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PROVIDER_OR_SCOPE_CHANGED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PROVIDER_STATE_CHANGED',
        'message', 'Payment status changed before final approval. Resolve the payment status or cancel this request.'
      )::text;
  END IF;

  SELECT
    pg_catalog.count(*)::integer,
    coalesce(pg_catalog.sum(member_row.active_item_count), 0)::integer,
    coalesce(
      pg_catalog.sum(
        pg_catalog.round(member_row.active_amount * 100)::bigint
      ),
      0
    )::bigint
  INTO
    v_selected_candidate_count,
    v_selected_active_item_count,
    v_selected_amount_pence
  FROM public.pay_payment_correction_request_candidates AS member_row
  WHERE member_row.correction_request_id = v_request.id;

  IF v_selected_candidate_count < 1
     OR v_selected_candidate_count IS DISTINCT FROM
          NULLIF(
            v_request.plan_json->>'selected_candidate_count',
            ''
          )::integer
     OR v_selected_active_item_count IS DISTINCT FROM
          NULLIF(
            v_request.plan_json->>'selected_active_item_count',
            ''
          )::integer
     OR v_selected_amount_pence IS DISTINCT FROM
          NULLIF(
            v_request.plan_json->>'selected_amount_pence',
            ''
          )::bigint
     OR EXISTS (
       SELECT 1
       FROM public.pay_payment_correction_request_candidates AS member_row
       LEFT JOIN public.pay_batch_items AS item_row
         ON item_row.id = ANY(member_row.pay_batch_item_ids)
        AND item_row.pay_batch_candidate_id =
            member_row.pay_batch_candidate_id
        AND coalesce(item_row.is_voided, false) IS NOT TRUE
       WHERE member_row.correction_request_id = v_request.id
       GROUP BY member_row.pay_batch_candidate_id,
                member_row.active_item_count
       HAVING pg_catalog.count(item_row.id)
              IS DISTINCT FROM member_row.active_item_count
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLAN_STALE'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PLAN_STALE'
      )::text;
  END IF;

  v_requested_action := coalesce(
    v_request.plan_json->>'requested_action',
    v_request.selection_json->>'requested_action',
    v_request.selection_json->>'action'
  );

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_request_candidates AS member_row
    JOIN public.pay_batch_items AS item_row
      ON item_row.id = ANY(member_row.pay_batch_item_ids)
    JOIN public.pay_bank_transfer_events AS event_row
      ON event_row.pay_batch_id = v_request.pay_batch_id
     AND (
       event_row.pay_bank_transfer_id = item_row.pay_bank_transfer_id
       OR event_row.candidate_id = (
         SELECT candidate_row.candidate_id
         FROM public.pay_batch_candidates AS candidate_row
         WHERE candidate_row.id = member_row.pay_batch_candidate_id
       )
     )
    WHERE member_row.correction_request_id = v_request.id
      AND event_row.normalised_state IN (
        'COMPLETED',
        'PAID',
        'SETTLED'
      )
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PROVIDER_STATE_CHANGED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PROVIDER_STATE_CHANGED'
      )::text;
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
  ) VALUES (
    v_request.id,
    v_request.pay_batch_id,
    'USER',
    p_actor_user_id,
    v_action,
    v_now,
    NULLIF(pg_catalog.btrim(coalesce(p_note, '')), ''),
    v_before_request,
    NULL,
    pg_catalog.jsonb_build_object(
      'approved_count_before',
        coalesce(v_request.approved_count, 0),
      'required_quantity',
        greatest(
          coalesce(v_request.required_quantity, 1),
          1
        )
    )
  );

  v_new_approved_count := CASE
    WHEN v_action = 'USE_GOLDEN_KEY' THEN
      greatest(
        coalesce(v_request.required_quantity, 1),
        1
      )
    ELSE
      least(
        coalesce(v_request.approved_count, 0) + 1,
        greatest(
          coalesce(v_request.required_quantity, 1),
          1
        )
      )
  END;

  IF v_new_approved_count <
       greatest(
         coalesce(v_request.required_quantity, 1),
         1
       ) THEN
    UPDATE public.pay_payment_correction_requests AS awaiting_request
    SET approved_count = v_new_approved_count,
        status = 'AWAITING_AUTHORISATION',
        updated_at_utc = v_now
    WHERE awaiting_request.id = v_request.id
    RETURNING awaiting_request.* INTO v_request;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'request_id', v_request.id,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'operation_id', v_operation.id,
      'action', v_action,
      'status', v_request.status,
      'request_status', v_request.status,
      'operation_status', v_operation.status,
      'operation_phase', v_operation.phase,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'old_authorisation_invalidated', false,
      'old_schedule_invalidated', false,
      'batch_status', v_batch.status,
      'gate_active', true,
      'display_message', 'Authorisation recorded. Further approval is required.',
      'authorised', false,
      'queued', false,
      'code', 'AUTHORISATION_RECORDED'
    );
  END IF;

  SELECT
    coalesce(
      pg_catalog.jsonb_agg(auth_request.id ORDER BY auth_request.created_at_utc),
      '[]'::jsonb
    )
  INTO v_old_auth_request_ids
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = v_request.pay_batch_id
    AND auth_request.state IN ('AWAITING', 'AUTHORISED');

  v_old_schedule_kind := v_batch.schedule_kind;
  v_old_scheduled_at_utc := v_batch.scheduled_at_utc;

  UPDATE public.pay_batch_auth_requests AS old_auth
  SET state = 'CANCELLED',
      finalised_at_utc = coalesce(
        old_auth.finalised_at_utc,
        v_now
      ),
      finalised_by_user_id = p_actor_user_id
  WHERE old_auth.pay_batch_id = v_request.pay_batch_id
    AND old_auth.state IN ('AWAITING', 'AUTHORISED');

  UPDATE public.pay_batch_auth_tokens AS old_token
  SET used_at_utc = coalesce(old_token.used_at_utc, v_now)
  WHERE old_token.auth_request_id IN (
    SELECT auth_request.id
    FROM public.pay_batch_auth_requests AS auth_request
    WHERE auth_request.pay_batch_id = v_request.pay_batch_id
      AND auth_request.state = 'CANCELLED'
  )
    AND old_token.used_at_utc IS NULL;

  UPDATE public.pay_batches AS correction_batch
  SET status = CASE
        WHEN v_requested_action = 'DRAFT_CANCEL' THEN
          correction_batch.status
        ELSE 'AWAITING_AUTHORISATION'
      END,
      schedule_kind = NULL,
      scheduled_at_utc = NULL,
      scheduled_by_user_id = NULL,
      funding_account_ref = NULL,
      funds_warning_hours_json = NULL,
      execution_intent_json = coalesce(
        correction_batch.execution_intent_json,
        '{}'::jsonb
      ) || pg_catalog.jsonb_build_object(
        'correction_request_id', v_request.id,
        'old_authorisation_invalidated_at_utc', v_now,
        'old_schedule_invalidated_at_utc', v_now,
        'reauthorisation_required',
          v_requested_action <> 'DRAFT_CANCEL'
      )
  WHERE correction_batch.id = v_request.pay_batch_id;

  UPDATE public.pay_payment_correction_requests AS authorised_request
  SET approved_count = v_new_approved_count,
      golden_key_used = v_action = 'USE_GOLDEN_KEY',
      golden_key_user_id = CASE
        WHEN v_action = 'USE_GOLDEN_KEY' THEN p_actor_user_id
        ELSE authorised_request.golden_key_user_id
      END,
      status = 'AUTHORISED',
      authorised_at_utc = coalesce(
        authorised_request.authorised_at_utc,
        v_now
      ),
      plan_json = coalesce(
        authorised_request.plan_json,
        '{}'::jsonb
      ) || pg_catalog.jsonb_build_object(
        'old_authorisation_request_ids', v_old_auth_request_ids,
        'old_schedule_kind', v_old_schedule_kind,
        'old_scheduled_at_utc', v_old_scheduled_at_utc,
        'old_authorisation_invalidated_at_utc', v_now,
        'old_schedule_invalidated_at_utc', v_now
      ),
      updated_at_utc = v_now
  WHERE authorised_request.id = v_request.id
  RETURNING authorised_request.* INTO v_request;

  UPDATE public.banking_pay_operations AS correction_operation
  SET status = 'RUNNING',
      phase = 'EXPAND_WORK',
      runner_state = 'RUNNABLE',
      requires_user_action = false,
      run_after_utc = v_now,
      lease_owner = NULL,
      lease_expires_at_utc = NULL,
      locked_by = NULL,
      lock_expires_at_utc = NULL,
      progress_json = coalesce(
        correction_operation.progress_json,
        '{}'::jsonb
      ) || pg_catalog.jsonb_build_object(
        'phase', 'EXPAND_WORK',
        'authorised_at_utc', v_now,
        'approved_count', v_new_approved_count
      ),
      updated_at_utc = v_now
  WHERE correction_operation.id = v_operation.id;

  UPDATE public.pay_payment_correction_actions AS final_action
  SET after_json = pg_catalog.to_jsonb(v_request),
      metadata_json = coalesce(
        final_action.metadata_json,
        '{}'::jsonb
      ) || pg_catalog.jsonb_build_object(
        'code', 'CORRECTION_AUTHORISED',
        'operation_id', v_operation.id,
        'old_authorisation_request_ids', v_old_auth_request_ids,
        'old_schedule_kind', v_old_schedule_kind,
        'old_scheduled_at_utc', v_old_scheduled_at_utc,
        'old_authorisation_invalidated_at_utc', v_now,
        'old_schedule_invalidated_at_utc', v_now
      )
  WHERE final_action.correction_request_id = v_request.id
    AND final_action.actor_user_id = p_actor_user_id
    AND final_action.action = v_action
    AND final_action.action_at_utc = v_now;

  PERFORM public.banking_pay_batch_signal_touch(
    p_pay_batch_id := v_request.pay_batch_id,
    p_change_reason := 'PAYMENT_CORRECTION_AUTHORISED',
    p_change_source := 'pay_payment_correction_authorise',
    p_change_scope_json := pg_catalog.jsonb_build_object(
      'correction_request_id', v_request.id,
      'operation_id', v_operation.id,
      'requested_action', v_requested_action
    ),
    p_touch_payment_status := true,
    p_touch_correction_progress := true,
    p_touch_alerts := false,
    p_touch_overview := true
  );

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'request_id', v_request.id,
    'correction_request_id', v_request.id,
    'pay_batch_id', v_request.pay_batch_id,
    'operation_id', v_operation.id,
    'action', v_action,
    'status', v_request.status,
    'request_status', v_request.status,
    'operation_status', 'RUNNING',
    'operation_phase', 'EXPAND_WORK',
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'old_authorisation_invalidated', true,
    'old_schedule_invalidated', true,
    'batch_status', CASE
      WHEN v_requested_action = 'DRAFT_CANCEL' THEN v_batch.status
      ELSE 'AWAITING_AUTHORISATION'
    END,
    'gate_active', true,
    'display_message', 'Cancellation approved and queued.',
    'authorised', true,
    'queued', true,
    'phase', 'EXPAND_WORK',
    'code', 'CORRECTION_AUTHORISED'
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_authorise(uuid,uuid,text,text) TO service_role;
