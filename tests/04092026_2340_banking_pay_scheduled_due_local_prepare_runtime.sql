\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned

BEGIN;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30s';
SET LOCAL jit = off;
SELECT set_config('h2.batch_id', :'h2_batch_id', false);
SELECT set_config('h2.evidence_mode', :'h2_evidence_mode', false);

CREATE TEMP TABLE h2_execute_result(result_json jsonb) ON COMMIT DROP;

DO $h2_execute$
DECLARE
  v_actor uuid := '10000000-0000-4000-8000-000000000001'::uuid;
  v_batch uuid := current_setting('h2.batch_id')::uuid;
  v_evidence_mode text := upper(current_setting('h2.evidence_mode'));
  v_operation uuid;
  v_result jsonb;
  v_batch_prepare_result jsonb;
  v_auth_result jsonb;
  v_freshness_result_hash text;
  v_idempotency_key text;
  v_schedule_result jsonb;
  v_claim_result jsonb;
  v_claim_replay_result jsonb;
  v_classifier_result jsonb;
  v_provider_evidence_result jsonb;
  v_transfer_proof_hash text;
  v_scope record;
  v_cursor jsonb;
  v_worker_id text := 'h2-scheduled-local-prepare';
BEGIN
  UPDATE public.tms_users
  SET payment_authoriser = true
  WHERE id = v_actor;

  UPDATE public.settings_defaults
  SET rail_provider_default = 'REVOLUT',
      rail_env_default = 'SANDBOX',
      rail_supports_scheduling = true,
      rail_supports_name_check = false,
      rail_default_funding_account_ref = 'H2_ROLLBACK_FUNDING'
  WHERE id = 1;

  UPDATE public.pay_batches
  SET rail_provider_snapshot = 'REVOLUT',
      rail_env_snapshot = 'SANDBOX'
  WHERE id = v_batch;

  v_idempotency_key := 'h2-execute-preparation-diagnostic-standard-' || gen_random_uuid()::text;

  SELECT started.operation_id
  INTO v_operation
  FROM public.banking_pay_operation_start(
    'PAYMENT_EXECUTE',
    v_actor,
    v_idempotency_key,
    NULL::uuid,
    v_batch,
    NULL::uuid,
    jsonb_build_object(
      'execution_mode', 'STANDARD_BANK',
      'pay_channel_scope', 'ALL',
      'schedule_kind', 'SCHEDULED',
      'scheduled_at_utc', (now() + interval '1 day')::text,
      'payment_date', current_date::text,
      'funding_account_ref', 'H2_ROLLBACK_FUNDING',
      'source', 'H2_ROLLBACK_ONLY_EXECUTE_PREPARATION'
    ),
    '{}'::jsonb
  ) AS started;

  PERFORM 1
  FROM public.banking_pay_operation_claim_next(
    p_operation_id := v_operation,
    p_actor_user_id := v_actor,
    p_lock_owner := v_worker_id,
    p_lock_seconds := 60,
    p_allow_backend_runner_owned := true,
    p_operation_types := ARRAY['PAYMENT_EXECUTE']::text[]
  ) AS claimed
  WHERE claimed.claimed IS TRUE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'H2_SCHEDULED_LOCAL_PREPARE_OPERATION_CLAIM_FAILED';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_batches AS prepared_batch
    WHERE prepared_batch.id = v_batch
      AND prepared_batch.batch_kind_fixed = 'PAYE'
  ) THEN
    PERFORM public.pay_set_paye_net_manual(
      v_batch,
      (
        SELECT jsonb_agg(
          jsonb_build_object(
            'candidate_id', candidate.candidate_id::text,
            'net_amount', CASE WHEN right(candidate.candidate_id::text, 1) = '1' THEN '1.25' ELSE '1.50' END
          )
          ORDER BY candidate.candidate_id
        )
        FROM public.pay_batch_candidates AS candidate
        WHERE candidate.pay_batch_id = v_batch
      ),
      v_actor
    );
  END IF;

  UPDATE public.banking_pay_operations
  SET phase = 'FRESHNESS_SCOPE_SEED', updated_at_utc = now()
  WHERE id = v_operation;
  v_result := public.pay_batch_freshness_scope_seed(v_operation, v_batch, NULL::jsonb, 100);

  UPDATE public.banking_pay_operations
  SET phase = 'FRESHNESS_VALIDATE', updated_at_utc = now()
  WHERE id = v_operation;
  v_cursor := NULL::jsonb;
  LOOP
    v_result := public.pay_batch_validate_freshness_chunk(v_operation, v_cursor, 100);
    EXIT WHEN coalesce((v_result->>'has_more')::boolean, false) IS NOT TRUE;
    v_cursor := v_result->'next_cursor';
  END LOOP;

  UPDATE public.banking_pay_operations
  SET phase = 'FRESHNESS_RESULT', updated_at_utc = now()
  WHERE id = v_operation;
  v_result := public.pay_batch_freshness_result_get(v_operation, v_batch, v_actor);
  v_freshness_result_hash := coalesce(v_result->>'freshness_result_hash', v_result->>'result_hash');

  UPDATE public.banking_pay_operations
  SET phase = 'TRANSFER_SCOPE_SEED', updated_at_utc = now()
  WHERE id = v_operation;
  v_result := public.pay_execute_bank_transfer_scope_seed(v_operation, v_batch, 'ALL', v_actor, false);

  FOR v_scope IN
    SELECT id
    FROM public.banking_pay_operation_transfer_scope
    WHERE operation_id = v_operation
      AND pay_batch_id = v_batch
    ORDER BY id
  LOOP
    UPDATE public.banking_pay_operations
    SET phase = 'SEED_TRANSFER_SCOPE_ITEMS', updated_at_utc = now()
    WHERE id = v_operation;
    v_cursor := NULL::jsonb;
    LOOP
      v_result := public.pay_execute_bank_transfer_scope_items_seed_chunk(v_operation, v_scope.id, v_cursor, 100, v_actor);
      EXIT WHEN coalesce((v_result->>'has_more')::boolean, false) IS NOT TRUE;
      v_cursor := v_result->'next_cursor';
    END LOOP;

    UPDATE public.banking_pay_operations
    SET phase = 'ROLLUP_TRANSFER_SCOPE_ITEMS', updated_at_utc = now()
    WHERE id = v_operation;
    v_cursor := NULL::jsonb;
    LOOP
      v_result := public.pay_execute_bank_transfer_scope_rollup_chunk(v_operation, v_scope.id, v_cursor, 100, v_actor);
      EXIT WHEN coalesce((v_result->>'has_more')::boolean, false) IS NOT TRUE;
      v_cursor := v_result->'next_cursor';
    END LOOP;
  END LOOP;

  UPDATE public.banking_pay_operations
  SET phase = 'PREPARE_TRANSFER_CHUNKS', updated_at_utc = now()
  WHERE id = v_operation;
  v_result := public.pay_execute_bank_transfer_chunk_prepare(v_operation, v_batch, NULL::jsonb, v_actor);

  SELECT md5(coalesce(string_agg(
           scope_row.id::text || ':' ||
           coalesce(scope_row.pay_bank_transfer_id::text, '') || ':' ||
           coalesce(scope_row.status, '') || ':' ||
           round(coalesce(scope_row.amount, 0), 2)::text || ':' ||
           coalesce(scope_row.prepared_scope_hash, '') || ':' ||
           coalesce(scope_row.prepared_result_hash, ''),
           '|' ORDER BY scope_row.id
         ), 'NO_SCOPE'))
  INTO v_transfer_proof_hash
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = v_operation
    AND scope_row.pay_batch_id = v_batch;

  UPDATE public.banking_pay_operations
  SET progress_json = coalesce(progress_json, '{}'::jsonb) || jsonb_build_object(
        'prepared_transfer_proof', jsonb_build_object(
          'prepared_transfer_proof_hash', v_transfer_proof_hash
        )
      ),
      updated_at_utc = now()
  WHERE id = v_operation;

  INSERT INTO public.bank_payee_map(
    id,rail_provider,rail_env,entity_kind,entity_id,bank_details_hash,
    payee_id,payee_account_id,meta_json,created_at_utc,updated_at_utc
  )
  SELECT DISTINCT ON (
           transfer.rail_provider,
           transfer.rail_env,
           transfer.payee_entity_kind,
           transfer.payee_entity_id,
           transfer.bank_details_hash_snapshot
         )
         gen_random_uuid(),transfer.rail_provider,transfer.rail_env,
         transfer.payee_entity_kind,transfer.payee_entity_id,transfer.bank_details_hash_snapshot,
         'H2_PAYEE_' || left(md5(transfer.id::text),20),
         'H2_ACCOUNT_' || left(md5(transfer.id::text),18),
         jsonb_build_object('source','H2_ROLLBACK_ONLY'),now(),now()
  FROM public.pay_bank_transfers transfer
  WHERE transfer.pay_batch_id = v_batch
    AND NOT EXISTS (
      SELECT 1 FROM public.bank_payee_map existing
      WHERE existing.rail_provider=transfer.rail_provider
        AND existing.rail_env=transfer.rail_env
        AND existing.entity_kind=transfer.payee_entity_kind
        AND existing.entity_id=transfer.payee_entity_id
        AND existing.bank_details_hash=transfer.bank_details_hash_snapshot
    )
  ORDER BY transfer.rail_provider,transfer.rail_env,transfer.payee_entity_kind,
           transfer.payee_entity_id,transfer.bank_details_hash_snapshot,transfer.id;

  UPDATE public.banking_pay_operations
  SET phase = 'PREPARE_BATCH_PROOF', updated_at_utc = now()
  WHERE id = v_operation;
  v_batch_prepare_result := public.pay_batch_prepare(v_batch, v_actor, v_operation, v_freshness_result_hash);

  UPDATE public.banking_pay_operations
  SET phase = 'START_AUTHORISATION_PROOF', updated_at_utc = now()
  WHERE id = v_operation;
  v_auth_result := public.pay_batch_auth_start(
    v_batch,
    'SCHEDULED',
    now() + interval '1 day',
    'H2_ROLLBACK_FUNDING',
    '[]'::jsonb,
    v_actor,
    NULL::text,
    'STANDARD_BANK',
    current_date,
    'ALL',
    false,
    false,
    false,
    NULL::text,
    NULL::text,
    v_operation,
    v_idempotency_key,
    v_freshness_result_hash
  );

  UPDATE public.banking_pay_operations
  SET phase = 'SCHEDULE_PAYMENT', updated_at_utc = now()
  WHERE id = v_operation;
  v_schedule_result := public.pay_batch_schedule(
    v_batch,
    'SCHEDULED',
    now() + interval '1 day',
    'H2_ROLLBACK_FUNDING',
    '[]'::jsonb,
    v_actor,
    v_operation,
    v_freshness_result_hash
  );

  v_result := public.banking_pay_operation_release_lease(
    p_operation_id := v_operation,
    p_lease_owner := v_worker_id,
    p_release_state := 'COMPLETE',
    p_run_after_delay_seconds := 0,
    p_progress_patch_json := jsonb_build_object('phase', 'COMPLETE'),
    p_result_patch_json := jsonb_build_object(
      'ok', true,
      'status_text', 'Payment execution operation complete.',
      'pay_batch_schedule', v_schedule_result
    ),
    p_error_json := NULL::jsonb,
    p_resume_reason := 'OPERATION_COMPLETE',
    p_actor_user_id := v_actor
  );
  IF COALESCE((v_result->>'ok')::boolean, false) IS NOT TRUE
     OR COALESCE(v_result->>'status', '') <> 'COMPLETE' THEN
    RAISE EXCEPTION 'H2_SCHEDULED_LOCAL_PREPARE_OPERATION_RELEASE_FAILED:%', v_result;
  END IF;

  IF v_evidence_mode = 'RAIL_TRANSACTION' THEN
    UPDATE public.pay_bank_transfers
    SET rail_tx_id = 'H2-ROLLBACK-EXTERNAL-TRANSACTION',
        rail_state = 'PROCESSING'
    WHERE id = (
      SELECT transfer.id
      FROM public.pay_bank_transfers AS transfer
      WHERE transfer.pay_batch_id = v_batch
      ORDER BY transfer.id
      LIMIT 1
    );
  ELSIF v_evidence_mode = 'REQUEST_SENT_NO_EXTERNAL' THEN
    -- Mirror the canonical operation payload written by the existing
    -- pay_provider_submit_chunk_stage_record owner.  The standalone attempt
    -- ledger is audit evidence, but the current provider classifier reads the
    -- operation/chunk payload and the transfer-scope stage.
    UPDATE public.banking_pay_operations
    SET progress_json = coalesce(progress_json, '{}'::jsonb) || jsonb_build_object(
          'provider_request_sent', true,
          'provider_submission_attempted', true,
          'provider_submission_status', 'REQUEST_SENT_LOCAL'
        ),
        updated_at_utc = now()
    WHERE id = v_operation;

    UPDATE public.pay_bank_transfers
    SET rail_meta_json = coalesce(rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'provider_request_sent', true,
          'provider_submit_attempted', true
        )
    WHERE id = (
      SELECT transfer.id
      FROM public.pay_bank_transfers AS transfer
      WHERE transfer.pay_batch_id = v_batch
      ORDER BY transfer.id
      LIMIT 1
    );

    INSERT INTO public.banking_pay_operation_provider_attempts(
      operation_id,pay_batch_id,transfer_scope_id,provider_idempotency_key,
      previous_state,new_state,lease_owner,compact_request_hash
    )
    SELECT v_operation,v_batch,scope_row.id,scope_row.provider_idempotency_key,
           'REQUEST_PREPARING','REQUEST_SENDING','h2-rollback-request-sent',md5(scope_row.id::text)
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id=v_operation AND scope_row.pay_batch_id=v_batch
    ORDER BY scope_row.id
    LIMIT 1;
  ELSIF v_evidence_mode = 'PROVIDER_RESPONSE' THEN
    INSERT INTO public.pay_bank_transfer_events(
      pay_batch_id,
      pay_bank_transfer_id,
      provider_key,
      provider_event_id,
      provider_reference,
      provider_state,
      normalised_state,
      event_source,
      mapping_status,
      raw_payload,
      idempotency_key,
      provider_event_transport,
      provider_signature_valid,
      rail_env
    )
    SELECT v_batch,
           transfer.id,
           'REVOLUT',
           'H2-ROLLBACK-PROVIDER-EVENT',
           transfer.request_id,
           'PROCESSING',
           'PROCESSING',
           'PROVIDER_RESPONSE',
           'MATCHED',
           jsonb_build_object('source', 'H2_ROLLBACK_ONLY'),
           'h2-rollback-provider-event-' || transfer.id::text,
           'PROVIDER_RESPONSE',
           true,
           'SANDBOX'
    FROM public.pay_bank_transfers AS transfer
    WHERE transfer.id = (
      SELECT selected_transfer.id
      FROM public.pay_bank_transfers AS selected_transfer
      WHERE selected_transfer.pay_batch_id = v_batch
      ORDER BY selected_transfer.id
      LIMIT 1
    );
  ELSIF v_evidence_mode = 'OPERATION_ATTEMPT' THEN
    UPDATE public.banking_pay_operations
    SET progress_json = coalesce(progress_json, '{}'::jsonb) || jsonb_build_object(
          'provider_submit_attempted', true,
          'provider_submission_status', 'REQUEST_SENDING'
        ),
        updated_at_utc = now()
    WHERE id = v_operation;

    INSERT INTO public.banking_pay_operation_provider_attempts(
      operation_id,
      pay_batch_id,
      transfer_scope_id,
      provider_idempotency_key,
      previous_state,
      new_state,
      lease_owner,
      compact_request_hash
    )
    SELECT v_operation,
           v_batch,
           scope_row.id,
           scope_row.provider_idempotency_key,
           'READY',
           'REQUEST_SENDING',
           'h2-rollback-provider-attempt',
           md5(scope_row.id::text)
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = v_operation
      AND scope_row.pay_batch_id = v_batch
    ORDER BY scope_row.id
    LIMIT 1;
  ELSIF v_evidence_mode = 'AMBIGUOUS' THEN
    UPDATE public.banking_pay_operations
    SET error_json = coalesce(error_json, '{}'::jsonb) || jsonb_build_object(
          'provider_request_sent', true,
          'provider_outcome_unknown', true,
          'request_sent_no_response', true,
          'code', 'REQUEST_SENT_NO_RESPONSE'
        ),
        updated_at_utc = now()
    WHERE id = v_operation;

    UPDATE public.pay_bank_transfers
    SET rail_meta_json = coalesce(rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'provider_outcome_unknown', true,
          'request_sent_no_response', true
        )
    WHERE id = (
      SELECT transfer.id
      FROM public.pay_bank_transfers AS transfer
      WHERE transfer.pay_batch_id = v_batch
      ORDER BY transfer.id
      LIMIT 1
    );

    INSERT INTO public.banking_pay_operation_provider_attempts(
      operation_id,pay_batch_id,transfer_scope_id,provider_idempotency_key,
      previous_state,new_state,lease_owner,compact_request_hash,
      compact_error_summary_json
    )
    SELECT v_operation,v_batch,scope_row.id,scope_row.provider_idempotency_key,
           'REQUEST_SENDING','PROVIDER_UNKNOWN','h2-rollback-ambiguous',md5(scope_row.id::text),
           jsonb_build_object('provider_outcome_unknown',true,'request_sent_no_response',true)
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id=v_operation AND scope_row.pay_batch_id=v_batch
    ORDER BY scope_row.id
    LIMIT 1;
  ELSIF v_evidence_mode <> 'LOCAL_ONLY' THEN
    RAISE EXCEPTION 'H2_EVIDENCE_MODE_INVALID:%', v_evidence_mode;
  END IF;

  SELECT coalesce(jsonb_agg(to_jsonb(evidence)), '[]'::jsonb)
  INTO v_provider_evidence_result
  FROM public._pay_bank_transfer_provider_evidence_classify(
    v_batch,
    NULL::uuid,
    jsonb_build_object('scope_type', 'BATCH'),
    NULL::uuid
  ) AS evidence;

  SELECT coalesce(jsonb_agg(to_jsonb(classified) ORDER BY classified.pay_bank_transfer_id), '[]'::jsonb)
  INTO v_classifier_result
  FROM public.pay_bank_transfer_execution_classify(
    v_batch,
    'ALL',
    NULL::uuid,
    true,
    'SCHEDULE_ACTION'
  ) AS classified;

  v_claim_result := public.pay_batches_claim_due_scheduled(50, now() + interval '2 days');
  v_claim_replay_result := public.pay_batches_claim_due_scheduled(50, now() + interval '2 days');

  INSERT INTO h2_execute_result(result_json)
  SELECT jsonb_build_object(
    'evidence_mode', v_evidence_mode,
    'operation_id', v_operation::text,
    'prepare', jsonb_build_object(
      'ok', v_result->'ok',
      'code', v_result->'code',
      'message', v_result->'message',
      'review_required', v_result->'review_required',
      'next_required_phase', v_result->'next_required_phase',
      'execution_mode', v_result->'execution_mode',
      'prepared_count', v_result->'prepared_count',
      'reused_count', v_result->'reused_count',
      'failed_count', v_result->'failed_count',
      'remaining_count', v_result->'remaining_count',
      'has_more', v_result->'has_more',
      'item_transfer_linked_count', v_result->'item_transfer_linked_count',
      'item_transfer_reused_count', v_result->'item_transfer_reused_count',
      'item_transfer_conflict_count', v_result->'item_transfer_conflict_count'
    ),
    'batch_prepare', jsonb_build_object(
      'ok', v_batch_prepare_result->'ok',
      'code', v_batch_prepare_result->'code',
      'message', v_batch_prepare_result->'message',
      'ready', v_batch_prepare_result->'ready',
      'blocker_count', v_batch_prepare_result->'blocker_count',
      'blockers', v_batch_prepare_result->'blockers',
      'waived_blockers', v_batch_prepare_result->'waived_blockers',
      'next_required_phase', v_batch_prepare_result->'next_required_phase',
      'execution_mode', v_batch_prepare_result->'execution_mode'
    ),
    'auth', jsonb_build_object(
      'ok', v_auth_result->'ok',
      'code', v_auth_result->'code',
      'message', v_auth_result->'message',
      'state', coalesce(v_auth_result->'auth_state', v_auth_result->'status', v_auth_result->'state'),
      'requires_user_action', v_auth_result->'requires_user_action',
      'next_required_phase', v_auth_result->'next_required_phase',
      'execution_mode', v_auth_result->'execution_mode'
    ),
    'schedule', jsonb_build_object(
      'ok', v_schedule_result->'ok',
      'code', v_schedule_result->'code',
      'message', v_schedule_result->'message',
      'schedule_kind', v_schedule_result->'schedule_kind',
      'batch_status', coalesce(v_schedule_result->'batch_status', v_schedule_result->'status'),
      'next_required_phase', v_schedule_result->'next_required_phase'
    ),
    'due_claim', jsonb_build_object(
      'ok', v_claim_result->'ok',
      'code', v_claim_result->'code',
      'claimed_count', v_claim_result->'claimed_count',
      'skipped_count', v_claim_result->'skipped_count',
      'operations', v_claim_result->'operations'
    ),
    'due_claim_replay', jsonb_build_object(
      'ok', v_claim_replay_result->'ok',
      'code', v_claim_replay_result->'code',
      'claimed_count', v_claim_replay_result->'claimed_count',
      'skipped_count', v_claim_replay_result->'skipped_count',
      'operations', v_claim_replay_result->'operations'
    ),
    'canonical_transfer_classifier', v_classifier_result,
    'canonical_provider_evidence', v_provider_evidence_result,
    'raw_transfer_evidence', (
      SELECT jsonb_agg(jsonb_build_object(
        'rail_tx_id', transfer.rail_tx_id,
        'rail_state', transfer.rail_state,
        'rail_meta_json', transfer.rail_meta_json
      ) ORDER BY transfer.id)
      FROM public.pay_bank_transfers AS transfer
      WHERE transfer.pay_batch_id = v_batch
    ),
    'scope_count', (
      SELECT count(*) FROM public.banking_pay_operation_transfer_scope WHERE operation_id = v_operation
    ),
    'scope_statuses', (
      SELECT jsonb_object_agg(status, row_count ORDER BY status)
      FROM (
        SELECT status, count(*) AS row_count
        FROM public.banking_pay_operation_transfer_scope
        WHERE operation_id = v_operation
        GROUP BY status
      ) AS grouped
    ),
    'transfer_count', (
      SELECT count(*) FROM public.pay_bank_transfers WHERE pay_batch_id = v_batch
    ),
    'provider_attempt_count', (
      SELECT count(*) FROM public.banking_pay_operation_provider_attempts WHERE operation_id = v_operation
    ),
    'provider_event_count', (
      SELECT count(*) FROM public.pay_bank_transfer_events WHERE pay_batch_id = v_batch
    )
  );
END;
$h2_execute$;

SELECT '__H2_EXECUTE_PREP__' || result_json::text FROM h2_execute_result;
ROLLBACK;
