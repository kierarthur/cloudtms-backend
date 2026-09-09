-- Self-contained rollback evidence for payment-correction lost responses.
-- It requires only an existing DRAFT batch and active actor, creates its exact
-- request/operation/action fixture inside this transaction, and leaves no row
-- or settings change after rollback. It invokes no provider, settlement,
-- remittance or mail sender.

\set ON_ERROR_STOP on

BEGIN;

DO $runtime$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_original_input jsonb;
  v_prepare_result jsonb;
  v_start_result jsonb;
  v_before jsonb;
  v_after jsonb;
  v_error_detail jsonb;
  v_error_detail_text text;
  v_error_message text;
  v_probe_status text;
  v_probe_result jsonb;
  v_batch public.pay_batches%rowtype;
  v_live_active_scope_hash text;
  v_exact_before jsonb;
  v_exact_after jsonb;
  v_call_started_at timestamptz;
  v_call_elapsed_ms numeric;
  v_call_timings jsonb := '[]'::jsonb;
  v_fixture_request_id constant uuid := '80510000-0000-4510-8510-000000000001'::uuid;
  v_fixture_operation_id constant uuid := '80510000-0000-4510-8510-000000000002'::uuid;
  v_fixture_candidate_token constant uuid := '80510000-0000-4510-8510-000000000003'::uuid;
  v_fixture_actor_id uuid;
  v_fixture_explicit_hash text;
  v_fixture_selection jsonb;
  v_fixture_plan jsonb;
  v_fixture_descriptor_hash text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_requests AS existing_request
    WHERE existing_request.id = v_fixture_request_id
  ) OR EXISTS (
    SELECT 1
    FROM public.banking_pay_operations AS existing_operation
    WHERE existing_operation.id = v_fixture_operation_id
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_RUNTIME_FIXED_ID_COLLISION';
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.status = 'DRAFT'
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_requests AS active_request
      WHERE active_request.pay_batch_id = batch_row.id
        AND active_request.status IN (
          'PLANNING','PLANNED','REQUESTED','AWAITING_AUTHORISATION',
          'AUTHORISED','EXPANDED','PROCESSING'
        )
    )
  ORDER BY batch_row.id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_RUNTIME_DRAFT_BATCH_REQUIRED';
  END IF;

  SELECT actor_row.id
  INTO v_fixture_actor_id
  FROM public.tms_users AS actor_row
  WHERE coalesce(actor_row.is_active, false)
  ORDER BY actor_row.id
  LIMIT 1;

  IF v_fixture_actor_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_RUNTIME_ACTIVE_ACTOR_REQUIRED';
  END IF;

  -- Enable only the existing cancellation gate inside this outer rollback.
  -- The test neither changes nor bypasses any selection/payment policy.
  UPDATE public.settings_defaults AS fixture_settings
  SET banking_pay_candidate_cancellation_enabled = true
  WHERE fixture_settings.id = (
    SELECT settings_row.id
    FROM public.settings_defaults AS settings_row
    ORDER BY settings_row.id
    LIMIT 1
  );

  v_original_input := pg_catalog.jsonb_build_object(
    'idempotency_key', 'h12-payment-correction-replay-self-contained-v1',
    'mode', 'EXPLICIT',
    'requested_action', 'DRAFT_CANCEL',
    'filter_json', '{}'::jsonb,
    'sort_key', 'STATUS',
    'sort_direction', 'ASC',
    'snapshot_token', 'h12-payment-correction-replay-self-contained-snapshot-v1',
    'explicit_candidate_tokens', pg_catalog.jsonb_build_array(v_fixture_candidate_token::text)
  );

  v_fixture_explicit_hash := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'version', 1, 'pay_batch_id', v_batch.id, 'chain', 'EXPLICIT_IDS'
    )
  );
  v_fixture_explicit_hash := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'prior', v_fixture_explicit_hash,
      'pay_batch_candidate_id', v_fixture_candidate_token::text
    )
  );

  v_fixture_selection := v_original_input || pg_catalog.jsonb_build_object(
    'contract_version', 1,
    'mode', 'EXPLICIT',
    'requested_action', 'DRAFT_CANCEL',
    'filter_json', '{}'::jsonb,
    'sort_key', 'STATUS',
    'sort_direction', 'ASC',
    'snapshot_token', 'h12-payment-correction-replay-self-contained-snapshot-v1',
    'scope_fence_hash', pg_catalog.repeat('1', 64),
    'requested_explicit_count', 1,
    'requested_explicit_hash', v_fixture_explicit_hash,
    'draft_overlay_fast_pre_request_authorities', '{}'::jsonb,
    'cancellation_reversion_pre_request_authorities_v2', '{}'::jsonb,
    'cancellation_reversion_pre_request_authorities_v3', '{}'::jsonb,
    'canonical_explicit_candidate_tokens', pg_catalog.jsonb_build_array(v_fixture_candidate_token::text),
    'selection', pg_catalog.jsonb_build_object(
      'mode', 'EXPLICIT',
      'action', 'DRAFT_CANCEL',
      'filter_json', '{}'::jsonb,
      'sort_key', 'STATUS',
      'sort_direction', 'ASC',
      'snapshot_token', 'h12-payment-correction-replay-self-contained-snapshot-v1',
      'scope_fence_hash', pg_catalog.repeat('1', 64),
      'explicit_candidate_tokens', pg_catalog.jsonb_build_array(v_fixture_candidate_token::text),
      'requested_explicit_count', 1,
      'requested_explicit_hash', v_fixture_explicit_hash
    )
  );
  v_fixture_descriptor_hash := private.pay_payment_correction_sha256_v1(
    v_fixture_selection
      - 'command'
      - 'draft_overlay_fast_pre_request_authorities'
      - 'cancellation_reversion_pre_request_authorities_v2'
      - 'cancellation_reversion_pre_request_authorities_v3'
  );
  v_fixture_plan := pg_catalog.jsonb_build_object(
    'requested_action', 'DRAFT_CANCEL',
    'selected_candidate_count', 1,
    'selected_active_item_count', 1,
    'selected_amount_pence', 1,
    'reason_hash', private.pay_payment_correction_sha256_v1(
      pg_catalog.to_jsonb('DRAFT_PAYMENT_CANCELLED_BY_USER'::text)
    ),
    'evidence_hash', NULL::text,
    'outcome_hash', NULL::text
  );

  INSERT INTO public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, requested_by_user_id,
    required_quantity, approved_count, reason, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, authorised_at_utc, applied_at_utc,
    reauth_proof_hash, reauth_expires_at_utc, reauth_consumed_at_utc
  ) VALUES (
    v_fixture_request_id, v_batch.id, 'PRE_BANK_CANCEL', 'APPLIED', v_fixture_actor_id,
    1, 1, 'DRAFT_PAYMENT_CANCELLED_BY_USER', v_fixture_selection,
    private.pay_payment_correction_sha256_v1(v_fixture_selection),
    v_fixture_plan, private.pay_payment_correction_sha256_v1(v_fixture_plan), false,
    pg_catalog.clock_timestamp() - interval '2 minutes',
    pg_catalog.clock_timestamp() - interval '1 minute',
    pg_catalog.repeat('2', 64),
    pg_catalog.clock_timestamp() + interval '10 minutes',
    pg_catalog.clock_timestamp() - interval '2 minutes'
  );

  INSERT INTO public.banking_pay_operations (
    id, operation_type, status, phase, actor_user_id, pay_batch_id,
    idempotency_key, input_json, config_json, progress_json, result_json,
    requires_user_action, runner_state, run_after_utc, completed_at_utc
  ) VALUES (
    v_fixture_operation_id, 'PAYMENT_CORRECTION', 'COMPLETE', 'COMPLETE',
    v_fixture_actor_id, v_batch.id,
    'h12-payment-correction-replay-self-contained-v1',
    pg_catalog.jsonb_build_object(
      'correction_request_id', v_fixture_request_id::text,
      'requested_action', 'DRAFT_CANCEL',
      'auto_requested', false
    ),
    '{}'::jsonb, '{}'::jsonb,
    pg_catalog.jsonb_build_object('request_status', 'APPLIED'),
    false, 'COMPLETE', NULL::timestamptz, pg_catalog.clock_timestamp()
  );

  INSERT INTO public.pay_payment_correction_actions (
    correction_request_id, pay_batch_id, actor_kind, actor_user_id,
    action, note, metadata_json
  ) VALUES (
    v_fixture_request_id, v_batch.id, 'USER', v_fixture_actor_id,
    'REQUEST', 'Self-contained lost-response replay fixture.',
    pg_catalog.jsonb_build_object('descriptor_hash', v_fixture_descriptor_hash)
  );

  SELECT request_row.*
  INTO STRICT v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = v_fixture_request_id;

  SELECT operation_row.*
  INTO STRICT v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = v_fixture_operation_id;

  -- The H12 canonical fixture supplied no nested client selection object.  The
  -- projection below removes only fields added by the request owner itself.
  v_original_input := v_request.selection_json
    - 'selection'
    - 'scope_fence_hash'
    - 'requested_explicit_hash'
    - 'requested_explicit_count'
    - 'canonical_explicit_candidate_tokens'
    - 'draft_overlay_fast_pre_request_authorities'
    - 'cancellation_reversion_pre_request_authorities_v2'
    - 'cancellation_reversion_pre_request_authorities_v3';

  v_before := pg_catalog.jsonb_build_object(
    'requests', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_requests),
    'operations', (SELECT pg_catalog.count(*) FROM public.banking_pay_operations),
    'actions', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_actions),
    'work_items', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_work_items),
    'correction_items', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_items),
    'transfers', (SELECT pg_catalog.count(*) FROM public.pay_bank_transfers),
    'events', (SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events),
    'provider_attempts', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_provider_attempts),
    'settlement_scope', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_settlement_scope),
    'remittance_scope', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_remittance_scope),
    'mail', (SELECT pg_catalog.count(*) FROM public.mail_outbox)
  );

  SELECT pg_catalog.jsonb_build_object(
    'request', pg_catalog.to_jsonb(v_request),
    'operation', pg_catalog.to_jsonb(v_operation),
    'actions', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(action_row) ORDER BY action_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_actions AS action_row
      WHERE action_row.correction_request_id = v_request.id
    ),
    'work_items', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(work_row) ORDER BY work_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = v_request.id
    ),
    'correction_items', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(item_row) ORDER BY item_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_items AS item_row
      WHERE item_row.correction_request_id = v_request.id
    ),
    'transfers', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(transfer_row) ORDER BY transfer_row.id), '[]'::jsonb)
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = v_request.pay_batch_id
    ),
    'events', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(event_row) ORDER BY event_row.id), '[]'::jsonb)
      FROM public.pay_bank_transfer_events AS event_row
      WHERE event_row.pay_batch_id = v_request.pay_batch_id
    ),
    'provider_attempts', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(attempt_row) ORDER BY attempt_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_provider_attempts AS attempt_row
      WHERE attempt_row.pay_batch_id = v_request.pay_batch_id
    ),
    'settlement_scope', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(scope_row) ORDER BY scope_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.pay_batch_id = v_request.pay_batch_id
    ),
    'remittance_scope', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(scope_row) ORDER BY scope_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_remittance_scope AS scope_row
      WHERE scope_row.pay_batch_id = v_request.pay_batch_id
    ),
    'mail_outbox', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(mail_row) ORDER BY mail_row.id), '[]'::jsonb)
      FROM public.mail_outbox AS mail_row
    )
  )
  INTO v_exact_before;

  v_call_started_at := pg_catalog.clock_timestamp();
  v_prepare_result := public.pay_payment_correction_request_start(
    v_request.pay_batch_id,
    v_original_input,
    v_request.reason,
    v_request.requested_by_user_id,
    v_request.source_bank_event_id,
    v_request.auto_requested,
    v_request.accepted_resolution_json
  );
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_TERMINAL_REPLAY', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  IF COALESCE((v_prepare_result->>'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_prepare_result->>'is_existing')::boolean, false) IS NOT TRUE
     OR (v_prepare_result->>'correction_request_id')::uuid IS DISTINCT FROM v_request.id
     OR (v_prepare_result->>'operation_id')::uuid IS DISTINCT FROM v_operation.id
     OR v_prepare_result->>'code' IS DISTINCT FROM 'PAYMENT_CORRECTION_REQUEST_EXISTING' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXACT_PREPARE_REPLAY_FAILED: %', v_prepare_result;
  END IF;

  v_call_started_at := pg_catalog.clock_timestamp();
  v_start_result := public.pay_payment_correction_request_start(
    v_request.pay_batch_id,
    pg_catalog.jsonb_build_object(
      'command','START_PREPARED',
      'context','START_PREPARED',
      'correction_request_id',v_request.id,
      'proof_hash',v_request.reauth_proof_hash,
      'selection_hash',v_request.selection_hash,
      'plan_hash',v_request.plan_hash
    ),
    v_request.reason,
    v_request.requested_by_user_id,
    v_request.source_bank_event_id,
    false,
    v_request.accepted_resolution_json
  );
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'START_PREPARED_TERMINAL_REPLAY', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  IF COALESCE((v_start_result->>'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_start_result->>'is_existing')::boolean, false) IS NOT TRUE
     OR (v_start_result->>'correction_request_id')::uuid IS DISTINCT FROM v_request.id
     OR (v_start_result->>'operation_id')::uuid IS DISTINCT FROM v_operation.id
     OR v_start_result->>'code' IS DISTINCT FROM 'REQUEST_ALREADY_STARTED'
     OR COALESCE((v_start_result#>>'{continuation,terminal}')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXACT_START_REPLAY_FAILED: %', v_start_result;
  END IF;

  BEGIN
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id,
      pg_catalog.jsonb_set(v_original_input, '{snapshot_token}', pg_catalog.to_jsonb('changed-snapshot'::text), true),
      v_request.reason,
      v_request.requested_by_user_id,
      v_request.source_bank_event_id,
      v_request.auto_requested,
      v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_CHANGED_INTENT_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN
      v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN
      v_error_detail := '{}'::jsonb;
    END;
    IF v_error_detail->>'code' IS DISTINCT FROM 'IDEMPOTENCY_CONFLICT' THEN
      RAISE;
    END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_CHANGED_INTENT_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  -- A response lost after selection preparation must return the same PLANNED
  -- request.  The replay remains read-only and does not use mutable plan_json
  -- as its operation identity.
  BEGIN
    UPDATE public.pay_payment_correction_requests AS planned_request
    SET status = 'PLANNED', updated_at_utc = pg_catalog.clock_timestamp()
    WHERE planned_request.id = v_request.id;

    UPDATE public.banking_pay_operations AS planned_operation
    SET status = 'WAITING_AUTHORISATION',
        phase = 'AWAITING_REAUTHENTICATION',
        runner_state = 'WAITING_USER',
        requires_user_action = true,
        run_after_utc = NULL::timestamptz,
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE planned_operation.id = v_operation.id;

    v_call_started_at := pg_catalog.clock_timestamp();
    v_probe_result := public.pay_payment_correction_request_start(
      v_request.pay_batch_id,
      v_original_input,
      v_request.reason,
      v_request.requested_by_user_id,
      v_request.source_bank_event_id,
      v_request.auto_requested,
      v_request.accepted_resolution_json
    );
    v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
    IF v_call_elapsed_ms >= 6000 THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_CALL_EXCEEDED_6000MS: PREPARE_PLANNED_REPLAY %', v_call_elapsed_ms;
    END IF;
    v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'call', 'PREPARE_PLANNED_REPLAY', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
    ));

    IF coalesce((v_probe_result->>'ok')::boolean, false) IS NOT TRUE
       OR coalesce((v_probe_result->>'is_existing')::boolean, false) IS NOT TRUE
       OR v_probe_result->>'request_status' IS DISTINCT FROM 'PLANNED'
       OR v_probe_result->>'operation_status' IS DISTINCT FROM 'WAITING_AUTHORISATION'
       OR v_probe_result->>'phase' IS DISTINCT FROM 'AWAITING_REAUTHENTICATION'
       OR (v_probe_result->>'correction_request_id')::uuid IS DISTINCT FROM v_request.id
       OR (v_probe_result->>'operation_id')::uuid IS DISTINCT FROM v_operation.id THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_PLANNED_PREPARE_REPLAY_FAILED: %', v_probe_result;
    END IF;

    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLANNED_PREPARE_PROBE_ROLLBACK';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
    IF v_error_message IS DISTINCT FROM 'PAYMENT_CORRECTION_PLANNED_PREPARE_PROBE_ROLLBACK' THEN
      RAISE;
    END IF;
  END;
  -- Compute the exact current scope hash with the same fields and owners as
  -- request_start.  This lets the legacy-resume regression exercise only the
  -- historical status transition, not an unrelated stale-scope branch.
  v_live_active_scope_hash := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'version', 2,
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
      'change_signal', (
        SELECT pg_catalog.jsonb_build_object(
          'version', coalesce(signal_row.version, 0),
          'payment_status_version', coalesce(signal_row.payment_status_version, 0),
          'correction_progress_version', coalesce(signal_row.correction_progress_version, 0),
          'overview_version', coalesce(signal_row.overview_version, 0),
          'last_changed_at_utc', signal_row.last_changed_at_utc
        )
        FROM public.banking_pay_batch_change_signals AS signal_row
        WHERE signal_row.pay_batch_id = v_batch.id
      ),
      'scope_version_authority', 'banking_pay_batch_change_signals'
    )
  );

  -- Historical one-authoriser rows are still a real compatibility path.  A
  -- consumed proof in either REQUESTED state must perform the unchanged
  -- AUTHORISE transition; it must not be intercepted as a read-only replay.
  FOREACH v_probe_status IN ARRAY ARRAY['REQUESTED','AWAITING_AUTHORISATION']::text[] LOOP
    BEGIN
      UPDATE public.pay_payment_correction_requests AS legacy_request
      SET status = v_probe_status,
          required_quantity = 1,
          approved_count = 0,
          plan_json = pg_catalog.jsonb_set(
            coalesce(legacy_request.plan_json, '{}'::jsonb),
            '{active_batch_scope_hash}',
            pg_catalog.to_jsonb(v_live_active_scope_hash),
            true
          ),
          updated_at_utc = pg_catalog.clock_timestamp()
      WHERE legacy_request.id = v_request.id;

      UPDATE public.banking_pay_operations AS legacy_operation
      SET status = 'WAITING_AUTHORISATION',
          phase = 'AWAITING_REAUTHENTICATION',
          runner_state = 'WAITING_USER',
          requires_user_action = true,
          run_after_utc = NULL::timestamptz,
          updated_at_utc = pg_catalog.clock_timestamp()
      WHERE legacy_operation.id = v_operation.id;

      v_call_started_at := pg_catalog.clock_timestamp();
      v_probe_result := public.pay_payment_correction_request_start(
        v_request.pay_batch_id,
        pg_catalog.jsonb_build_object(
          'command','START_PREPARED',
          'context','START_PREPARED',
          'correction_request_id',v_request.id,
          'proof_hash',v_request.reauth_proof_hash,
          'selection_hash',v_request.selection_hash,
          'plan_hash',v_request.plan_hash
        ),
        v_request.reason,
        v_request.requested_by_user_id,
        v_request.source_bank_event_id,
        false,
        v_request.accepted_resolution_json
      );
      v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
      IF v_call_elapsed_ms >= 6000 THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_CALL_EXCEEDED_6000MS: START_PREPARED_LEGACY_RESUME_% %', v_probe_status, v_call_elapsed_ms;
      END IF;
      v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'call', 'START_PREPARED_LEGACY_RESUME_' || v_probe_status,
        'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
      ));

      IF coalesce((v_probe_result->>'ok')::boolean, false) IS NOT TRUE
         OR coalesce((v_probe_result->>'is_existing')::boolean, true) IS NOT FALSE
         OR v_probe_result->>'code' IS DISTINCT FROM 'PAYMENT_CORRECTION_AUTHORISED'
         OR v_probe_result->>'request_status' IS DISTINCT FROM 'AUTHORISED'
         OR v_probe_result->>'operation_status' IS DISTINCT FROM 'RUNNING'
         OR v_probe_result->>'phase' IS DISTINCT FROM 'EXPAND_WORK'
         OR coalesce((v_probe_result->>'approved_count')::integer, 0) <> 1
         OR NOT EXISTS (
           SELECT 1
           FROM public.pay_payment_correction_actions AS resume_action
           WHERE resume_action.correction_request_id = v_request.id
             AND resume_action.action = 'AUTHORISE'
             AND resume_action.metadata_json->>'code'
                   = 'LEGACY_REQUESTER_REAUTHORISED_CANCELLATION_RESUMED'
         ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_RESUME_FAILED: status %, result %', v_probe_status, v_probe_result;
      END IF;

      RAISE EXCEPTION 'PAYMENT_CORRECTION_LEGACY_RESUME_PROBE_ROLLBACK';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN
      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
      IF v_error_message IS DISTINCT FROM 'PAYMENT_CORRECTION_LEGACY_RESUME_PROBE_ROLLBACK' THEN
        RAISE;
      END IF;
    END;
  END LOOP;

  -- A unique link with an impossible request/operation phase pair is not an
  -- exact replay.  Both PREPARE and START_PREPARED must fail before mutation.
  BEGIN
    UPDATE public.banking_pay_operations AS mismatched_operation
    SET status = 'RUNNING', phase = 'EXPAND_WORK'
    WHERE mismatched_operation.id = v_operation.id;

    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id,
      v_original_input,
      v_request.reason,
      v_request.requested_by_user_id,
      v_request.source_bank_event_id,
      v_request.auto_requested,
      v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PREPARE_LIFECYCLE_MISMATCH_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN
      v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN
      v_error_detail := '{}'::jsonb;
    END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'REQUEST_OPERATION_LIFECYCLE_MISMATCH' THEN
      RAISE;
    END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_LIFECYCLE_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  BEGIN
    UPDATE public.banking_pay_operations AS mismatched_operation
    SET status = 'RUNNING', phase = 'EXPAND_WORK'
    WHERE mismatched_operation.id = v_operation.id;

    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id,
      pg_catalog.jsonb_build_object(
        'command','START_PREPARED', 'context','START_PREPARED',
        'correction_request_id',v_request.id,
        'proof_hash',v_request.reauth_proof_hash,
        'selection_hash',v_request.selection_hash,
        'plan_hash',v_request.plan_hash
      ),
      v_request.reason,
      v_request.requested_by_user_id,
      v_request.source_bank_event_id,
      false,
      v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_START_LIFECYCLE_MISMATCH_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN
      v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN
      v_error_detail := '{}'::jsonb;
    END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'REQUEST_OPERATION_LIFECYCLE_MISMATCH' THEN
      RAISE;
    END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'START_PREPARED_LIFECYCLE_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  -- Linkage failures are deterministic and fail closed.  Each hostile shape
  -- is contained by a PL/pgSQL subtransaction, preserving the fixture.
  BEGIN
    UPDATE public.banking_pay_operations AS invalid_link
    SET input_json = pg_catalog.jsonb_set(invalid_link.input_json, '{correction_request_id}', '"not-a-uuid"'::jsonb, true)
    WHERE invalid_link.id = v_operation.id;
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id, v_original_input, v_request.reason,
      v_request.requested_by_user_id, v_request.source_bank_event_id,
      v_request.auto_requested, v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_INVALID_OPERATION_LINK_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN v_error_detail := '{}'::jsonb; END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'OPERATION_REQUEST_LINK_INVALID' THEN RAISE; END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_INVALID_OPERATION_LINK_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  BEGIN
    UPDATE public.banking_pay_operations AS missing_request
    SET input_json = pg_catalog.jsonb_set(
      missing_request.input_json, '{correction_request_id}',
      pg_catalog.to_jsonb(extensions.gen_random_uuid()::text), true
    )
    WHERE missing_request.id = v_operation.id;
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id, v_original_input, v_request.reason,
      v_request.requested_by_user_id, v_request.source_bank_event_id,
      v_request.auto_requested, v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_MISSING_REQUEST_LINK_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN v_error_detail := '{}'::jsonb; END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'CORRECTION_REQUEST_MISSING' THEN RAISE; END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_MISSING_REQUEST_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  BEGIN
    UPDATE public.banking_pay_operations AS moved_operation
    SET idempotency_key = moved_operation.idempotency_key || ':moved'
    WHERE moved_operation.id = v_operation.id;
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id, v_original_input, v_request.reason,
      v_request.requested_by_user_id, v_request.source_bank_event_id,
      v_request.auto_requested, v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_MISSING_OPERATION_LINK_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN v_error_detail := '{}'::jsonb; END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'REQUEST_OPERATION_LINK_MISSING' THEN RAISE; END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_ORPHAN_REQUEST_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  BEGIN
    INSERT INTO public.banking_pay_operations
    SELECT (pg_catalog.jsonb_populate_record(
      NULL::public.banking_pay_operations,
      pg_catalog.to_jsonb(v_operation) || pg_catalog.jsonb_build_object(
        'id', extensions.gen_random_uuid(),
        'input_json', pg_catalog.jsonb_set(
          v_operation.input_json, '{correction_request_id}',
          pg_catalog.to_jsonb(extensions.gen_random_uuid()::text), true
        ),
        'status', 'COMPLETE',
        'phase', 'COMPLETE'
      )
    )).*;
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id, v_original_input, v_request.reason,
      v_request.requested_by_user_id, v_request.source_bank_event_id,
      v_request.auto_requested, v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_DUPLICATE_OPERATION_KEY_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN v_error_detail := '{}'::jsonb; END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'OPERATION_IDEMPOTENCY_KEY_AMBIGUOUS' THEN RAISE; END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_DUPLICATE_OPERATION_KEY_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  BEGIN
    DROP INDEX public.ux_banking_pay_operations_correction_request;
    INSERT INTO public.banking_pay_operations
    SELECT (pg_catalog.jsonb_populate_record(
      NULL::public.banking_pay_operations,
      pg_catalog.to_jsonb(v_operation) || pg_catalog.jsonb_build_object(
        'id', extensions.gen_random_uuid(),
        'idempotency_key', v_operation.idempotency_key || ':duplicate-link',
        'status', 'COMPLETE',
        'phase', 'COMPLETE'
      )
    )).*;
    v_call_started_at := pg_catalog.clock_timestamp();
    PERFORM public.pay_payment_correction_request_start(
      v_request.pay_batch_id, v_original_input, v_request.reason,
      v_request.requested_by_user_id, v_request.source_bank_event_id,
      v_request.auto_requested, v_request.accepted_resolution_json
    );
    RAISE EXCEPTION 'PAYMENT_CORRECTION_DUPLICATE_REQUEST_LINK_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error_detail_text = PG_EXCEPTION_DETAIL;
    BEGIN v_error_detail := coalesce(v_error_detail_text, '{}')::jsonb;
    EXCEPTION WHEN OTHERS THEN v_error_detail := '{}'::jsonb; END;
    IF v_error_detail->>'reason' IS DISTINCT FROM 'REQUEST_OPERATION_LINK_AMBIGUOUS' THEN RAISE; END IF;
  END;
  v_call_elapsed_ms := EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - v_call_started_at)) * 1000;
  v_call_timings := v_call_timings || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'call', 'PREPARE_DUPLICATE_REQUEST_LINK_NEGATIVE', 'elapsed_ms', pg_catalog.round(v_call_elapsed_ms, 3)
  ));

  v_after := pg_catalog.jsonb_build_object(
    'requests', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_requests),
    'operations', (SELECT pg_catalog.count(*) FROM public.banking_pay_operations),
    'actions', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_actions),
    'work_items', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_work_items),
    'correction_items', (SELECT pg_catalog.count(*) FROM public.pay_payment_correction_items),
    'transfers', (SELECT pg_catalog.count(*) FROM public.pay_bank_transfers),
    'events', (SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events),
    'provider_attempts', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_provider_attempts),
    'settlement_scope', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_settlement_scope),
    'remittance_scope', (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_remittance_scope),
    'mail', (SELECT pg_catalog.count(*) FROM public.mail_outbox)
  );

  SELECT pg_catalog.jsonb_build_object(
    'request', pg_catalog.to_jsonb(request_row),
    'operation', pg_catalog.to_jsonb(operation_row),
    'actions', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(action_row) ORDER BY action_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_actions AS action_row
      WHERE action_row.correction_request_id = v_request.id
    ),
    'work_items', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(work_row) ORDER BY work_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_work_items AS work_row
      WHERE work_row.correction_request_id = v_request.id
    ),
    'correction_items', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(item_row) ORDER BY item_row.id), '[]'::jsonb)
      FROM public.pay_payment_correction_items AS item_row
      WHERE item_row.correction_request_id = v_request.id
    ),
    'transfers', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(transfer_row) ORDER BY transfer_row.id), '[]'::jsonb)
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = v_request.pay_batch_id
    ),
    'events', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(event_row) ORDER BY event_row.id), '[]'::jsonb)
      FROM public.pay_bank_transfer_events AS event_row
      WHERE event_row.pay_batch_id = v_request.pay_batch_id
    ),
    'provider_attempts', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(attempt_row) ORDER BY attempt_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_provider_attempts AS attempt_row
      WHERE attempt_row.pay_batch_id = v_request.pay_batch_id
    ),
    'settlement_scope', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(scope_row) ORDER BY scope_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.pay_batch_id = v_request.pay_batch_id
    ),
    'remittance_scope', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(scope_row) ORDER BY scope_row.id), '[]'::jsonb)
      FROM public.banking_pay_operation_remittance_scope AS scope_row
      WHERE scope_row.pay_batch_id = v_request.pay_batch_id
    ),
    'mail_outbox', (
      SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(mail_row) ORDER BY mail_row.id), '[]'::jsonb)
      FROM public.mail_outbox AS mail_row
    )
  )
  INTO v_exact_after
  FROM public.pay_payment_correction_requests AS request_row
  JOIN public.banking_pay_operations AS operation_row
    ON operation_row.id = v_operation.id
  WHERE request_row.id = v_request.id;

  IF v_after IS DISTINCT FROM v_before THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_MUTATED_STATE: before %, after %', v_before, v_after;
  END IF;

  IF v_exact_after IS DISTINCT FROM v_exact_before THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_CHANGED_EXACT_ROWS';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(v_call_timings) AS measured(call)
    WHERE (measured.call->>'elapsed_ms')::numeric >= 6000
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REPLAY_CALL_EXCEEDED_6000MS: %', v_call_timings;
  END IF;

  RAISE NOTICE 'PAYMENT_CORRECTION_REPLAY_CALL_TIMINGS=%', v_call_timings;
END;
$runtime$;

ROLLBACK;
