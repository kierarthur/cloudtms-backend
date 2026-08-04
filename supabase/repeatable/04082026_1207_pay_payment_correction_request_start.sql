-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Exact installed identity retained. PREPARE is non-gating; START_PREPARED consumes proof atomically.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_request_start(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_reason text,
  p_actor_user_id uuid,
  p_source_bank_event_id uuid DEFAULT NULL::uuid,
  p_auto_requested boolean DEFAULT false,
  p_accepted_resolution_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '6000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_command text;
  v_mode text;
  v_action text;
  v_filter jsonb;
  v_sort_key text;
  v_sort_direction text;
  v_snapshot_token text;
  v_active_scope_hash text;
  v_descriptor_hash text;
  v_plan_json jsonb;
  v_plan_hash text;
  v_selection jsonb;
  v_proof_hash text;
  v_correction_kind text;
  v_idempotency_key text;
  v_request_id uuid;
  v_operation_id uuid;
  v_request public.pay_payment_correction_requests%rowtype;
  v_batch public.pay_batches%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_mutation_guard jsonb;
  v_operation_result jsonb;
  v_enabled boolean := false;
  v_actor_active boolean := false;
  v_required_quantity integer := 1;
  v_expected_snapshot_token text;
  v_old_auth_request_ids jsonb := '[]'::jsonb;
  v_old_schedule_kind text;
  v_old_scheduled_at_utc timestamptz;
  v_reason_hash text;
  v_evidence_hash text;
  v_outcome_hash text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR pg_catalog.jsonb_typeof(p_selection_json) <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DESCRIPTOR_INVALID')::text;
  END IF;

  IF pg_catalog.octet_length(p_selection_json::text) > 524288 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_DESCRIPTOR_TOO_LARGE'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DESCRIPTOR_INVALID')::text;
  END IF;

  v_command := pg_catalog.upper(coalesce(p_selection_json->>'command', 'PREPARE'));
  IF v_command NOT IN ('PREPARE', 'START_PREPARED', 'START_AUTO') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_COMMAND_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DESCRIPTOR_INVALID')::text;
  END IF;

  SELECT settings_row.banking_pay_candidate_cancellation_enabled,
         greatest(coalesce(settings_row.payment_authoriser_quantity, 1), 1)
  INTO v_enabled, v_required_quantity
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  IF coalesce(v_enabled, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_FEATURE_DISABLED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_FEATURE_DISABLED')::text;
  END IF;

  IF v_command = 'PREPARE' THEN
    IF coalesce(p_auto_requested, false) IS NOT TRUE THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'ACTOR_USER_ID_REQUIRED')::text;
      END IF;

      SELECT coalesce(actor_row.is_active, false)
      INTO v_actor_active
      FROM public.tms_users AS actor_row
      WHERE actor_row.id = p_actor_user_id;

      IF coalesce(v_actor_active, false) IS NOT TRUE THEN
        RAISE EXCEPTION 'ACTOR_USER_INACTIVE'
          USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'PERMISSION_DENIED')::text;
      END IF;
    ELSIF p_source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION')::text;
    END IF;

    SELECT batch_row.*
    INTO v_batch
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = p_pay_batch_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND')::text;
    END IF;

    IF p_source_bank_event_id IS NOT NULL AND NOT EXISTS (
      SELECT 1
      FROM public.pay_bank_transfer_events AS event_row
      WHERE event_row.id = p_source_bank_event_id
        AND event_row.pay_batch_id = p_pay_batch_id
    ) THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH')::text;
    END IF;

    v_mode := pg_catalog.upper(coalesce(
      p_selection_json#>>'{selection,mode}', p_selection_json->>'mode', ''
    ));
    v_action := pg_catalog.upper(coalesce(
      p_selection_json#>>'{selection,action}', p_selection_json->>'requested_action', p_selection_json->>'action', ''
    ));
    v_filter := coalesce(
      p_selection_json#>'{selection,filter_json}', p_selection_json->'filter_json',
      p_selection_json#>'{selection,filter}', p_selection_json->'filter', '{}'::jsonb
    );
    v_sort_key := pg_catalog.upper(coalesce(
      p_selection_json#>>'{selection,sort_key}', p_selection_json->>'sort_key', 'STATUS'
    ));
    v_sort_direction := pg_catalog.upper(coalesce(
      p_selection_json#>>'{selection,sort_direction}', p_selection_json->>'sort_direction', 'ASC'
    ));

    IF v_mode NOT IN ('EXPLICIT', 'ALL_MATCHING')
       OR v_action NOT IN ('DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT', 'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND')
       OR pg_catalog.jsonb_typeof(v_filter) <> 'object'
       OR v_sort_key NOT IN ('STATUS', 'CANDIDATE', 'AMOUNT')
       OR v_sort_direction NOT IN ('ASC', 'DESC') THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_DESCRIPTOR_INVALID'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DESCRIPTOR_INVALID')::text;
    END IF;

    IF v_action = 'DRAFT_CANCEL' AND NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '')
         IS DISTINCT FROM 'DRAFT_PAYMENT_CANCELLED_BY_USER' THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_DRAFT_REASON_INVALID'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_REASON_INVALID')::text;
    ELSIF v_action <> 'DRAFT_CANCEL'
       AND coalesce(p_auto_requested, false) IS NOT TRUE
       AND NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '') IS NULL THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REASON_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_REASON_REQUIRED')::text;
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
          SELECT pg_catalog.count(*) FROM public.pay_batch_candidates AS candidate_row
          WHERE candidate_row.pay_batch_id = v_batch.id
        ),
        'active_item_count', (
          SELECT pg_catalog.count(*)
          FROM public.pay_batch_items AS item_row
          JOIN public.pay_batch_candidates AS candidate_row ON candidate_row.id = item_row.pay_batch_candidate_id
          WHERE candidate_row.pay_batch_id = v_batch.id AND coalesce(item_row.is_voided, false) IS NOT TRUE
        ),
        'latest_request_update', (
          SELECT pg_catalog.max(other_request.updated_at_utc)
          FROM public.pay_payment_correction_requests AS other_request
          WHERE other_request.pay_batch_id = v_batch.id
            AND other_request.status NOT IN ('PLANNING', 'PLANNED')
        ),
        'latest_provider_event', (
          SELECT pg_catalog.max(event_row.received_at_utc)
          FROM public.pay_bank_transfer_events AS event_row
          WHERE event_row.pay_batch_id = v_batch.id
        )
      )
    );

    v_expected_snapshot_token := private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'version', 1,
        'active_batch_scope_hash', v_active_scope_hash,
        'filter', v_filter,
        'sort_key', v_sort_key,
        'sort_direction', v_sort_direction
      )
    );
    v_snapshot_token := coalesce(
      p_selection_json#>>'{selection,snapshot_token}', p_selection_json->>'snapshot_token',
      CASE WHEN v_action = 'DRAFT_CANCEL' OR coalesce(p_auto_requested, false)
           THEN v_expected_snapshot_token ELSE NULL END
    );

    IF v_snapshot_token IS NULL OR v_snapshot_token IS DISTINCT FROM v_expected_snapshot_token THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_SNAPSHOT_STALE'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SELECTION_SNAPSHOT_STALE')::text;
    END IF;

    v_selection := p_selection_json || pg_catalog.jsonb_build_object(
      'contract_version', 1,
      'mode', v_mode,
      'requested_action', v_action,
      'filter_json', v_filter,
      'sort_key', v_sort_key,
      'sort_direction', v_sort_direction,
      'snapshot_token', v_snapshot_token,
      'selection', coalesce(p_selection_json->'selection', '{}'::jsonb) || pg_catalog.jsonb_build_object(
        'mode', v_mode,
        'action', v_action,
        'filter_json', v_filter,
        'sort_key', v_sort_key,
        'sort_direction', v_sort_direction,
        'snapshot_token', v_snapshot_token
      )
    );
    v_descriptor_hash := private.pay_payment_correction_sha256_v1(v_selection - 'command');
    v_correction_kind := CASE WHEN v_action IN ('NO_MONEY_RELEASE', 'NO_MONEY_UNWIND')
                              THEN 'NO_MONEY_UNWIND' ELSE 'PRE_BANK_CANCEL' END;
    v_idempotency_key := coalesce(NULLIF(v_selection->>'idempotency_key', ''),
      'payment-correction:' || p_pay_batch_id::text || ':' || v_descriptor_hash);
    v_reason_hash := private.pay_payment_correction_sha256_v1(
      coalesce(
        pg_catalog.to_jsonb(NULLIF(pg_catalog.btrim(p_reason), '')),
        'null'::jsonb
      )
    );
    v_evidence_hash := CASE
      WHEN p_source_bank_event_id IS NULL THEN NULL
      ELSE private.pay_payment_correction_sha256_v1(pg_catalog.to_jsonb(p_source_bank_event_id))
    END;
    v_outcome_hash := CASE
      WHEN p_accepted_resolution_json IS NULL THEN NULL
      ELSE private.pay_payment_correction_sha256_v1(p_accepted_resolution_json)
    END;

    SELECT existing_request.*
    INTO v_request
    FROM public.pay_payment_correction_requests AS existing_request
    WHERE existing_request.pay_batch_id = p_pay_batch_id
      AND existing_request.plan_json->>'idempotency_key' = v_idempotency_key
    ORDER BY existing_request.created_at_utc
    LIMIT 1;

    IF FOUND THEN
      IF v_request.plan_json ->> 'descriptor_hash' IS DISTINCT FROM v_descriptor_hash
         OR v_request.correction_kind IS DISTINCT FROM v_correction_kind
         OR v_request.plan_json ->> 'requested_action' IS DISTINCT FROM v_action
         OR v_request.plan_json ->> 'reason_hash' IS DISTINCT FROM v_reason_hash
         OR v_request.plan_json ->> 'evidence_hash' IS DISTINCT FROM v_evidence_hash
         OR v_request.plan_json ->> 'outcome_hash' IS DISTINCT FROM v_outcome_hash THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'IDEMPOTENCY_CONFLICT',
            'correction_request_id', v_request.id,
            'idempotency_key', v_idempotency_key
          )::text;
      END IF;

      SELECT operation_row.id INTO v_operation_id
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id' = v_request.id::text
      ORDER BY operation_row.created_at_utc LIMIT 1;
      RETURN pg_catalog.jsonb_build_object(
        'ok', true, 'existing_request', true, 'is_existing', true, 'correction_request_id', v_request.id,
        'operation_id', v_operation_id, 'request_status', v_request.status,
        'operation_status', (SELECT operation_row.status FROM public.banking_pay_operations AS operation_row WHERE operation_row.id = v_operation_id),
        'phase', (SELECT operation_row.phase FROM public.banking_pay_operations AS operation_row WHERE operation_row.id = v_operation_id),
        'selection_ready', v_request.status <> 'PLANNING',
        'gate_active', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING'),
        'approved_count', coalesce(v_request.approved_count, 0),
        'required_quantity', greatest(coalesce(v_request.required_quantity, 1), 1),
        'requires_reauthentication', v_request.status = 'PLANNED' AND coalesce(v_request.auto_requested, false) IS NOT TRUE,
        'requires_authorisation', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
        'display_status', CASE WHEN v_request.status = 'PLANNING' THEN 'Preparing payment selection' ELSE 'Cancellation request already exists' END,
        'display_message', 'CloudTMS returned the existing request for this exact idempotency contract.',
        'code', 'PAYMENT_CORRECTION_REQUEST_EXISTING'
      );
    END IF;

    v_request_id := extensions.gen_random_uuid();
    v_plan_json := pg_catalog.jsonb_build_object(
      'contract_version', 1, 'descriptor_hash', v_descriptor_hash,
      'active_batch_scope_hash', v_active_scope_hash, 'requested_action', v_action,
      'snapshot_token', v_snapshot_token, 'idempotency_key', v_idempotency_key,
      'reason_hash', v_reason_hash,
      'evidence_hash', v_evidence_hash,
      'outcome_hash', v_outcome_hash,
      'approval_expires_at_utc', v_now + interval '24 hours'
    );
    v_plan_hash := private.pay_payment_correction_sha256_v1(v_plan_json);

    INSERT INTO public.pay_payment_correction_requests (
      id, pay_batch_id, correction_kind, status, requested_by_user_id, requested_at_utc,
      required_quantity, approved_count, golden_key_used, reason, selection_json,
      selection_hash, plan_json, plan_hash, accepted_resolution_json,
      accepted_resolution_hash, source_bank_event_id, auto_requested, created_at_utc, updated_at_utc
    ) VALUES (
      v_request_id, p_pay_batch_id, v_correction_kind, 'PLANNING',
      CASE WHEN coalesce(p_auto_requested, false) THEN NULL ELSE p_actor_user_id END,
      v_now, v_required_quantity, 0, false, NULLIF(pg_catalog.btrim(p_reason), ''), v_selection,
      v_descriptor_hash, v_plan_json, v_plan_hash, p_accepted_resolution_json,
      CASE WHEN p_accepted_resolution_json IS NULL THEN NULL ELSE private.pay_payment_correction_sha256_v1(p_accepted_resolution_json) END,
      p_source_bank_event_id, coalesce(p_auto_requested, false), v_now, v_now
    ) RETURNING * INTO v_request;

    SELECT pg_catalog.to_jsonb(start_result), start_result.operation_id
    INTO v_operation_result, v_operation_id
    FROM public.banking_pay_operation_start(
      'PAYMENT_CORRECTION', p_actor_user_id, v_idempotency_key, NULL::uuid,
      p_pay_batch_id, NULL::uuid,
      pg_catalog.jsonb_build_object(
        'correction_request_id', v_request_id,
        'requested_action', v_action,
        'auto_requested', coalesce(p_auto_requested, false),
        'source_bank_event_id', p_source_bank_event_id
      ),
      pg_catalog.jsonb_build_object('contract_version', 1)
    ) AS start_result
    LIMIT 1;

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
      action_at_utc, note, before_json, after_json, metadata_json
    ) VALUES (
      v_request_id, p_pay_batch_id,
      CASE WHEN coalesce(p_auto_requested, false) THEN 'SYSTEM' ELSE 'USER' END,
      CASE WHEN coalesce(p_auto_requested, false) THEN NULL ELSE p_actor_user_id END,
      'REQUEST', v_now, NULLIF(pg_catalog.btrim(p_reason), ''), NULL, pg_catalog.to_jsonb(v_request),
      pg_catalog.jsonb_build_object('descriptor_hash', v_descriptor_hash, 'operation_id', v_operation_id, 'planning_only', true)
    );

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'existing_request', false, 'is_existing', false, 'correction_request_id', v_request_id,
      'operation_id', v_operation_id, 'request_status', 'PLANNING',
      'operation_status', coalesce(v_operation_result->>'status', 'RUNNING'),
      'phase', 'PREPARE_SELECTION',
      'selection_ready', false, 'gate_active', false, 'approved_count', 0,
      'required_quantity', v_required_quantity,
      'requires_reauthentication', false, 'requires_authorisation', false,
      'display_status', 'Preparing payment selection',
      'display_message', 'CloudTMS is preparing the exact payment selection for review.',
      'code', 'PAYMENT_CORRECTION_PLANNING_STARTED'
    );
  END IF;

  v_request_id := NULLIF(p_selection_json->>'correction_request_id', '')::uuid;
  IF v_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  SELECT request_row.* INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = v_request_id AND request_row.pay_batch_id = p_pay_batch_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  v_proof_hash := NULLIF(p_selection_json->>'proof_hash', '');
  IF v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING')
     AND (
       (
         v_command = 'START_AUTO'
         AND coalesce(v_request.auto_requested, false)
         AND v_request.source_bank_event_id IS NOT NULL
       )
       OR (
         v_request.reauth_consumed_at_utc IS NOT NULL
         AND v_request.reauth_proof_hash IS NOT DISTINCT FROM v_proof_hash
       )
     ) THEN
    SELECT operation_row.* INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
      AND operation_row.input_json->>'correction_request_id' = v_request_id::text
    ORDER BY operation_row.created_at_utc LIMIT 1;
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'is_existing', true, 'correction_request_id', v_request_id, 'operation_id', v_operation.id,
      'request_status', v_request.status, 'operation_status', v_operation.status,
      'phase', v_operation.phase,
      'selection_ready', true, 'gate_active', true,
      'approved_count', coalesce(v_request.approved_count, 0),
      'required_quantity', greatest(coalesce(v_request.required_quantity, 1), 1),
      'requires_reauthentication', false,
      'requires_authorisation', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
      'display_status', 'Cancellation request already started',
      'display_message', 'CloudTMS returned the already-committed cancellation request.',
      'code', 'REQUEST_ALREADY_STARTED'
    );
  END IF;

  v_mutation_guard := private.pay_payment_mutation_guard_v1(p_pay_batch_id, NULL::uuid, 'NEW_PAYMENT_ACTION');
  IF coalesce((v_mutation_guard->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION '%', coalesce(v_mutation_guard->>'code', 'PAYMENT_CHANGE_IN_PROGRESS')
      USING ERRCODE = 'P0001', DETAIL = v_mutation_guard::text;
  END IF;

  SELECT request_row.* INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = v_request_id AND request_row.pay_batch_id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  SELECT batch_row.* INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND')::text;
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.input_json->>'correction_request_id' = v_request_id::text
  ORDER BY operation_row.created_at_utc LIMIT 1
  FOR UPDATE;

  IF v_request.status IS DISTINCT FROM 'PLANNED' OR v_operation.id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_STATE_INVALID')::text;
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
    UPDATE public.pay_payment_correction_requests AS stale_request
    SET status = 'CANCELLED',
        cancelled_at_utc = v_now,
        reauth_proof_hash = NULL,
        reauth_expires_at_utc = NULL,
        reauth_consumed_at_utc = NULL,
        updated_at_utc = v_now,
        plan_json = coalesce(stale_request.plan_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'stale_code', 'SELECTION_SCOPE_CHANGED',
            'observed_active_batch_scope_hash', v_active_scope_hash
          )
    WHERE stale_request.id = v_request_id;

    UPDATE public.banking_pay_operations AS stale_operation
    SET status = 'CANCELLED',
        phase = 'COMPLETE',
        runner_state = 'CANCELLED',
        requires_user_action = false,
        result_json = coalesce(stale_operation.result_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'code', 'SELECTION_SCOPE_CHANGED',
            'active_batch_scope_hash', v_active_scope_hash
          ),
        completed_at_utc = v_now,
        run_after_utc = NULL,
        lease_owner = NULL,
        lease_expires_at_utc = NULL,
        locked_by = NULL,
        lock_expires_at_utc = NULL,
        updated_at_utc = v_now
    WHERE stale_operation.id = v_operation.id;

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
      action_at_utc, note, before_json, after_json, metadata_json
    ) VALUES (
      v_request_id,
      p_pay_batch_id,
      CASE WHEN v_command = 'START_AUTO' THEN 'SYSTEM' ELSE 'USER' END,
      CASE WHEN v_command = 'START_AUTO' THEN NULL ELSE p_actor_user_id END,
      'CANCEL',
      v_now,
      'The reviewed payment scope changed before the correction gate was installed.',
      pg_catalog.jsonb_build_object(
        'active_batch_scope_hash', v_request.plan_json ->> 'active_batch_scope_hash'
      ),
      pg_catalog.jsonb_build_object('active_batch_scope_hash', v_active_scope_hash),
      pg_catalog.jsonb_build_object('code', 'SELECTION_SCOPE_CHANGED')
    );

    RETURN pg_catalog.jsonb_build_object(
      'ok', false,
      'is_existing', true,
      'correction_request_id', v_request_id,
      'operation_id', v_operation.id,
      'request_status', 'CANCELLED',
      'operation_status', 'CANCELLED',
      'phase', 'COMPLETE',
      'selection_ready', false, 'gate_active', false,
      'approved_count', coalesce(v_request.approved_count, 0),
      'required_quantity', greatest(coalesce(v_request.required_quantity, 1), 1),
      'requires_reauthentication', false, 'requires_authorisation', false,
      'display_status', 'Payment status changed',
      'display_message', 'Payment status changed. Refresh and select the payments again.',
      'code', 'SELECTION_SCOPE_CHANGED',
      'message', 'Payment status changed. Refresh and select the payments again.'
    );
  END IF;

  IF v_command = 'START_AUTO' THEN
    IF coalesce(v_request.auto_requested, false) IS NOT TRUE
       OR v_request.source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTO_START_NOT_ALLOWED'
        USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'PERMISSION_DENIED')::text;
    END IF;
  ELSE
    IF p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id
       OR v_proof_hash IS NULL OR v_proof_hash !~ '^[0-9a-f]{64}$'
       OR v_request.reauth_proof_hash IS DISTINCT FROM v_proof_hash
       OR v_request.reauth_consumed_at_utc IS NOT NULL
       OR v_request.reauth_expires_at_utc IS NULL OR v_request.reauth_expires_at_utc <= v_now
       OR p_selection_json->>'selection_hash' IS DISTINCT FROM v_request.selection_hash
       OR p_selection_json->>'plan_hash' IS DISTINCT FROM v_request.plan_hash
       OR NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '') IS DISTINCT FROM v_request.reason THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTH_PROOF_INVALID'
        USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'REAUTH_PROOF_INVALID')::text;
    END IF;
  END IF;

  IF v_command = 'START_AUTO' THEN
    SELECT coalesce(
             pg_catalog.jsonb_agg(auth_request.id ORDER BY auth_request.created_at_utc),
             '[]'::jsonb
           )
    INTO v_old_auth_request_ids
    FROM public.pay_batch_auth_requests AS auth_request
    WHERE auth_request.pay_batch_id = v_request.pay_batch_id
      AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

    v_old_schedule_kind := v_batch.schedule_kind;
    v_old_scheduled_at_utc := v_batch.scheduled_at_utc;

    UPDATE public.pay_batch_auth_requests AS old_auth
    SET state = 'CANCELLED',
        finalised_at_utc = coalesce(old_auth.finalised_at_utc, v_now),
        finalised_by_user_id = p_actor_user_id
    WHERE old_auth.pay_batch_id = v_request.pay_batch_id
      AND old_auth.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

    UPDATE public.pay_batch_auth_tokens AS old_token
    SET used_at_utc = coalesce(old_token.used_at_utc, v_now)
    WHERE old_token.auth_request_id IN (
      SELECT auth_id.value::uuid
      FROM pg_catalog.jsonb_array_elements_text(v_old_auth_request_ids) AS auth_id(value)
    )
      AND old_token.used_at_utc IS NULL;

    UPDATE public.pay_batches AS auto_batch
    SET status = 'AWAITING_AUTHORISATION',
        schedule_kind = NULL,
        scheduled_at_utc = NULL,
        scheduled_by_user_id = NULL,
        funding_account_ref = NULL,
        funds_warning_hours_json = NULL,
        execution_intent_json = coalesce(auto_batch.execution_intent_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'correction_request_id', v_request.id,
            'old_authorisation_invalidated_at_utc', v_now,
            'old_schedule_invalidated_at_utc', v_now,
            'reauthorisation_required', true,
            'automatic_provider_no_money', true
          )
    WHERE auto_batch.id = v_request.pay_batch_id;
  END IF;

  UPDATE public.pay_payment_correction_requests AS started_request
  SET status = CASE WHEN v_command = 'START_AUTO' THEN 'AUTHORISED' ELSE 'REQUESTED' END,
      reauth_consumed_at_utc = CASE WHEN v_command = 'START_AUTO' THEN NULL ELSE v_now END,
      authorised_at_utc = CASE WHEN v_command = 'START_AUTO' THEN v_now ELSE started_request.authorised_at_utc END,
      approved_count = CASE WHEN v_command = 'START_AUTO' THEN started_request.required_quantity ELSE started_request.approved_count END,
      plan_json = CASE
        WHEN v_command = 'START_AUTO' THEN
          coalesce(started_request.plan_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'old_authorisation_request_ids', v_old_auth_request_ids,
            'old_schedule_kind', v_old_schedule_kind,
            'old_scheduled_at_utc', v_old_scheduled_at_utc,
            'old_authorisation_invalidated_at_utc', v_now,
            'old_schedule_invalidated_at_utc', v_now
          )
        ELSE started_request.plan_json
      END,
      updated_at_utc = v_now
  WHERE started_request.id = v_request_id
  RETURNING * INTO v_request;

  UPDATE public.banking_pay_operations AS started_operation
  SET status = CASE WHEN v_command = 'START_AUTO' THEN 'RUNNING' ELSE 'WAITING_AUTHORISATION' END,
      phase = CASE WHEN v_command = 'START_AUTO' THEN 'EXPAND_WORK' ELSE 'AWAITING_AUTHORISATION' END,
      runner_state = CASE WHEN v_command = 'START_AUTO' THEN 'RUNNABLE' ELSE 'WAITING_USER' END,
      requires_user_action = v_command <> 'START_AUTO',
      run_after_utc = CASE WHEN v_command = 'START_AUTO' THEN v_now ELSE NULL END,
      lease_owner = NULL, lease_expires_at_utc = NULL, locked_by = NULL, lock_expires_at_utc = NULL,
      updated_at_utc = v_now
  WHERE started_operation.id = v_operation.id
  RETURNING * INTO v_operation;

  INSERT INTO public.pay_payment_correction_actions (
    correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
    action_at_utc, note, before_json, after_json, metadata_json
  ) VALUES (
    v_request_id, p_pay_batch_id, CASE WHEN v_command = 'START_AUTO' THEN 'SYSTEM' ELSE 'USER' END,
    CASE WHEN v_command = 'START_AUTO' THEN NULL ELSE p_actor_user_id END,
    'REQUEST', v_now, 'Immutable correction request started.', NULL, pg_catalog.to_jsonb(v_request),
    pg_catalog.jsonb_build_object('code', CASE WHEN v_command = 'START_AUTO' THEN 'AUTO_REQUEST_STARTED' ELSE 'REAUTH_PROOF_CONSUMED' END)
  );

  IF v_command = 'START_AUTO' THEN
    PERFORM public.banking_pay_batch_signal_touch(
      p_pay_batch_id := v_request.pay_batch_id,
      p_change_reason := 'PAYMENT_CORRECTION_AUTO_AUTHORISED',
      p_change_source := 'pay_payment_correction_request_start',
      p_change_scope_json := pg_catalog.jsonb_build_object(
        'correction_request_id', v_request.id,
        'operation_id', v_operation.id,
        'source_bank_event_id', v_request.source_bank_event_id,
        'requested_action', v_request.plan_json ->> 'requested_action'
      )
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true, 'is_existing', false, 'correction_request_id', v_request_id, 'operation_id', v_operation.id,
    'request_status', v_request.status, 'operation_status', v_operation.status,
    'phase', v_operation.phase,
    'selection_ready', true,
    'gate_active', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING'),
    'approved_count', coalesce(v_request.approved_count, 0),
    'required_quantity', greatest(coalesce(v_request.required_quantity, 1), 1),
    'requires_reauthentication', false,
    'requires_authorisation', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
    'display_status', CASE WHEN v_command = 'START_AUTO' THEN 'Failed payment release authorised' ELSE 'Cancellation requested' END,
    'display_message', CASE WHEN v_command = 'START_AUTO' THEN 'CloudTMS is processing the provider-confirmed failed-payment release.' ELSE 'The cancellation request is awaiting the configured financial approval.' END,
    'code', CASE WHEN v_command = 'START_AUTO' THEN 'AUTO_REQUEST_STARTED' ELSE 'PAYMENT_CORRECTION_REQUESTED' END
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) TO service_role;
