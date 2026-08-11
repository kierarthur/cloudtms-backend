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
  v_explicit_tokens jsonb := '[]'::jsonb;
  v_canonical_explicit_tokens jsonb := '[]'::jsonb;
  v_requested_explicit_count integer := 0;
  v_unique_explicit_count integer := 0;
  v_requested_explicit_hash text;
  v_explicit_token text;
  v_max_candidates integer := 10000;
  v_auto_unwind_enabled boolean := false;
  v_source_event public.pay_bank_transfer_events%rowtype;
  v_auto_classification_result jsonb := '{}'::jsonb;
  v_existing_money_moved boolean := false;
  v_resume_reauthenticated_request boolean := false;
  v_candidate_id uuid;
  v_draft_overlay_pre_request_authorities jsonb := '{}'::jsonb;
  v_draft_overlay_start_admission jsonb := '{}'::jsonb;
  v_draft_overlay_start_authorities jsonb := '{}'::jsonb;
  v_cancel_reversion_pre_request_proof jsonb := '{}'::jsonb;
  v_cancel_reversion_pre_request_authorities_v2 jsonb := '{}'::jsonb;
  v_cancel_reversion_start_authorities_v2 jsonb := '{}'::jsonb;
  v_source_publication_identity_enforced boolean := false;
  v_scheduled_reversion_v2_observe_enabled boolean := false;
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
           least(coalesce(settings_row.banking_pay_correction_max_candidates, 10000), 10000),
           coalesce(settings_row.banking_pay_auto_unwind_terminal_no_money, false),
           coalesce(settings_row.banking_pay_source_publication_identity_enforce_v1_enabled, false),
           COALESCE((pg_catalog.to_jsonb(settings_row)
             ->>'banking_pay_scheduled_cancellation_reversion_v2_observe_enabled')::boolean,false)
    INTO v_enabled, v_max_candidates, v_auto_unwind_enabled,
         v_source_publication_identity_enforced,
         v_scheduled_reversion_v2_observe_enabled
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  -- Cancellation is authorised by exactly one authenticated user.  The
  -- organisation's payment_authoriser_quantity applies to creating/executing
  -- payments; it must never introduce a second-person cancellation approval.
  v_required_quantity := 1;

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

    IF pg_catalog.upper(pg_catalog.btrim(coalesce(v_batch.status, ''))) IN (
      'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_BATCH_TERMINAL'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_BATCH_TERMINAL',
          'batch_status', v_batch.status
        )::text;
    END IF;

    IF p_source_bank_event_id IS NOT NULL THEN
      SELECT event_row.*
      INTO v_source_event
      FROM public.pay_bank_transfer_events AS event_row
      WHERE event_row.id = p_source_bank_event_id
        AND event_row.pay_batch_id = p_pay_batch_id;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH')::text;
      END IF;
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

    IF coalesce(p_auto_requested, false) THEN
      v_auto_classification_result := public._pay_payment_movement_classify(
        p_pay_batch_id,
        p_selection_json
      );

      v_existing_money_moved := false;
      IF v_source_event.pay_bank_transfer_id IS NOT NULL THEN
        SELECT coalesce(movement_state.is_final_money_moved, false)
        INTO v_existing_money_moved
        FROM public.pay_bank_transfers AS transfer_row
        CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
          transfer_row.status,
          transfer_row.rail_state,
          coalesce(transfer_row.rail_meta_json, '{}'::jsonb),
          coalesce(transfer_row.rail_meta_json, '{}'::jsonb)
        ) AS movement_state
        WHERE transfer_row.id = v_source_event.pay_bank_transfer_id
        LIMIT 1;

        v_existing_money_moved := coalesce(v_existing_money_moved, false) OR EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS paid_event
          WHERE paid_event.pay_bank_transfer_id = v_source_event.pay_bank_transfer_id
            AND paid_event.id IS DISTINCT FROM v_source_event.id
            AND paid_event.normalised_state IN ('COMPLETED', 'PAID', 'SETTLED')
        );
      END IF;

      IF v_action IS DISTINCT FROM 'NO_MONEY_UNWIND'
         OR coalesce(v_auto_unwind_enabled, false) IS NOT TRUE
         OR v_source_event.id IS NULL
         OR v_source_event.pay_batch_id IS DISTINCT FROM p_pay_batch_id
         OR v_source_event.pay_bank_transfer_id IS NULL
         OR v_source_event.event_source NOT IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
         OR v_source_event.provider_event_transport NOT IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
         OR v_source_event.mapping_status IS DISTINCT FROM 'MATCHED'
         OR v_source_event.mapping_method NOT IN (
           'TRANSFER_ID', 'PROVIDER_EVENT_ID', 'PROVIDER_TRANSACTION_ID', 'REQUEST_ID',
           'PROVIDER_REFERENCE', 'RAIL_TX_ID', 'MATCHED_PROVIDER_EVENT', 'MANUAL_TRANSFER_SELECTION'
         )
         OR v_source_event.normalised_state NOT IN ('FAILED', 'REJECTED', 'CANCELLED')
         OR v_source_event.correction_disposition IS DISTINCT FROM 'AUTO_PROCESSING'
         OR coalesce((v_auto_classification_result->>'safe_to_auto_apply')::boolean, false) IS NOT TRUE
         OR (
           v_auto_classification_result->>'classification' NOT IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
           AND v_auto_classification_result->>'recommended_action' IS DISTINCT FROM 'NO_MONEY_UNWIND_AND_RECALCULATE'
         )
         OR coalesce(v_existing_money_moved, false)
         OR pg_catalog.jsonb_typeof(p_accepted_resolution_json) IS DISTINCT FROM 'object'
         OR p_accepted_resolution_json->>'contract_version' IS DISTINCT FROM '2'
         OR p_accepted_resolution_json->>'requested_action' IS DISTINCT FROM 'NO_MONEY_UNWIND'
         OR p_accepted_resolution_json->>'source_bank_event_id' IS DISTINCT FROM v_source_event.id::text
         OR p_accepted_resolution_json->>'source' IS DISTINCT FROM v_source_event.provider_event_transport
         OR p_accepted_resolution_json->>'event_source' IS DISTINCT FROM v_source_event.event_source
         OR p_accepted_resolution_json->>'provider_key' IS DISTINCT FROM v_source_event.provider_key
         OR p_accepted_resolution_json->>'provider_event_id' IS DISTINCT FROM v_source_event.provider_event_id
         OR p_accepted_resolution_json->>'provider_event_key' IS DISTINCT FROM v_source_event.provider_event_key
         OR p_accepted_resolution_json->>'provider_webhook_receipt_id'
              IS DISTINCT FROM (CASE WHEN v_source_event.provider_webhook_receipt_id IS NULL THEN NULL ELSE v_source_event.provider_webhook_receipt_id::text END)
         OR p_accepted_resolution_json->>'provider_transaction_id' IS DISTINCT FROM v_source_event.provider_transaction_id
         OR p_accepted_resolution_json->>'provider_request_id' IS DISTINCT FROM v_source_event.provider_request_id
         OR p_accepted_resolution_json->>'mapping_status' IS DISTINCT FROM v_source_event.mapping_status
         OR p_accepted_resolution_json->>'mapping_method' IS DISTINCT FROM v_source_event.mapping_method
         OR p_accepted_resolution_json->>'normalised_state' IS DISTINCT FROM v_source_event.normalised_state
         OR p_accepted_resolution_json->>'classification' IS DISTINCT FROM v_auto_classification_result->>'classification'
         OR p_accepted_resolution_json->>'recommended_action' IS DISTINCT FROM v_auto_classification_result->>'recommended_action'
         OR coalesce((p_accepted_resolution_json->>'safe_to_auto_apply')::boolean, false) IS NOT TRUE
         OR p_accepted_resolution_json->>'correction_disposition' IS DISTINCT FROM 'AUTO_PROCESSING'
         OR coalesce((p_accepted_resolution_json->>'terminal_no_money_evidence')::boolean, false) IS NOT TRUE
         OR coalesce((p_accepted_resolution_json->>'signature_valid')::boolean, false)
              IS DISTINCT FROM coalesce(v_source_event.provider_signature_valid, (
                SELECT receipt_row.signature_valid
                FROM public.bank_provider_webhook_receipts AS receipt_row
                WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
              ), false)
         OR (
           v_source_event.provider_event_transport IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
           AND (
             v_source_event.provider_webhook_receipt_id IS NULL
             OR coalesce(v_source_event.provider_signature_valid, (
               SELECT receipt_row.signature_valid
               FROM public.bank_provider_webhook_receipts AS receipt_row
               WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
             ), false) IS NOT TRUE
             OR NOT EXISTS (
               SELECT 1
               FROM public.bank_provider_webhook_receipts AS receipt_row
               WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
                 AND receipt_row.signature_valid IS TRUE
                 AND receipt_row.provider_key IS NOT DISTINCT FROM v_source_event.provider_key
                 AND receipt_row.rail_env IS NOT DISTINCT FROM v_source_event.rail_env
                 AND pg_catalog.upper(coalesce(receipt_row.status, '')) IN (
                   'VERIFIED', 'NORMALISED', 'NORMALIZED', 'INGESTED', 'FAILED_RETRYABLE', 'UNMATCHED_REVIEW_REQUIRED'
                 )
             )
           )
         ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTO_EVIDENCE_INVALID'
          USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'PAYMENT_CORRECTION_AUTO_EVIDENCE_INVALID',
            'source_bank_event_id', p_source_bank_event_id,
            'requested_action', v_action
          )::text;
      END IF;
    END IF;

    IF v_mode = 'EXPLICIT' THEN
      v_explicit_tokens := coalesce(
        p_selection_json#>'{selection,explicit_candidate_tokens}',
        p_selection_json->'explicit_candidate_tokens',
        p_selection_json->'pay_batch_candidate_ids',
        '[]'::jsonb
      );

      IF pg_catalog.jsonb_typeof(v_explicit_tokens) <> 'array' THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPLICIT_SELECTION_INVALID'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DESCRIPTOR_INVALID')::text;
      END IF;

      IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements_text(v_explicit_tokens) AS supplied_token(token_value)
        WHERE supplied_token.token_value
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPLICIT_SELECTION_INVALID'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'DESCRIPTOR_INVALID', 'reason', 'EXPLICIT_TOKEN_INVALID'
          )::text;
      END IF;

      SELECT pg_catalog.count(*)::integer,
             pg_catalog.count(DISTINCT token_value)::integer,
             coalesce(
               pg_catalog.jsonb_agg(token_value ORDER BY token_value),
               '[]'::jsonb
             )
      INTO v_requested_explicit_count, v_unique_explicit_count, v_canonical_explicit_tokens
      FROM (
        SELECT pg_catalog.lower(explicit_token.token_value) AS token_value
        FROM pg_catalog.jsonb_array_elements_text(v_explicit_tokens) AS explicit_token(token_value)
      ) AS canonical_tokens;

      IF v_requested_explicit_count <> pg_catalog.jsonb_array_length(v_explicit_tokens)
         OR v_requested_explicit_count < 1
         OR v_requested_explicit_count > v_max_candidates
         OR v_unique_explicit_count <> v_requested_explicit_count THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPLICIT_SELECTION_INVALID'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'DESCRIPTOR_INVALID',
            'requested_explicit_count', v_requested_explicit_count,
            'maximum_candidate_count', v_max_candidates
          )::text;
      END IF;

      -- Boundary-independent ordered chain: final selection reconciliation can
      -- compare page summaries without rebuilding a 10,000-ID JSON value.
      v_requested_explicit_hash := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
          'version', 1, 'pay_batch_id', p_pay_batch_id, 'chain', 'EXPLICIT_IDS'
        )
      );
      FOR v_explicit_token IN
        SELECT token.value
        FROM pg_catalog.jsonb_array_elements_text(v_canonical_explicit_tokens) AS token(value)
        ORDER BY token.value
      LOOP
        v_requested_explicit_hash := private.pay_payment_correction_sha256_v1(
          pg_catalog.jsonb_build_object(
            'prior', v_requested_explicit_hash,
            'pay_batch_candidate_id', v_explicit_token
          )
        );
      END LOOP;
    END IF;

    v_active_scope_hash := private.pay_payment_correction_sha256_v1(
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

    -- An untouched Draft cancellation is allowed to ignore only the dirtying
    -- caused by the cancellation request itself.  Capture the exact live
    -- candidate authority before the request/action/operation rows exist.
    -- Candidate advisory ownership prevents a genuine economic writer from
    -- racing this fence.  The later START_PREPARED call must prove this fence
    -- is still live before it performs any cancellation-control mutations.
    IF v_action = 'DRAFT_CANCEL' THEN
      FOR v_candidate_id IN
        SELECT DISTINCT batch_candidate.candidate_id
        FROM public.pay_batch_candidates AS batch_candidate
        WHERE batch_candidate.pay_batch_id = p_pay_batch_id
        ORDER BY batch_candidate.candidate_id
      LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock(
          pg_catalog.hashtextextended(
            public._pay_workbench_candidate_serial_key(v_candidate_id),
            24062027
          )
        );
      END LOOP;

      WITH authority_rows AS (
        SELECT
          batch_candidate.id AS pay_batch_candidate_id,
          batch_candidate.candidate_id,
          batch_row.source_workbench_session_id,
          batch_row.source_session_version,
          draft_operation.id AS draft_operation_id,
          draft_scope.allocation_basis_json->'post_draft_authority' AS post_draft_authority,
          COALESCE(candidate_counter.seq,0) AS live_source_change_seq,
          COALESCE(candidate_counter.scope_change_generation,0) AS live_dirty_generation,
          registry.current_source_change_seq AS registry_source_change_seq,
          registry.dirty_generation AS registry_dirty_generation,
          candidate_state.source_change_seq AS candidate_state_source_change_seq,
          candidate_state.session_version AS candidate_state_session_version
        FROM public.pay_batch_candidates AS batch_candidate
        JOIN public.pay_batches AS batch_row
          ON batch_row.id=batch_candidate.pay_batch_id
        LEFT JOIN LATERAL (
          SELECT operation_row.id
          FROM public.banking_pay_operations AS operation_row
          WHERE operation_row.operation_type='DRAFT_CREATE'
            AND operation_row.status='COMPLETE'
            AND (
              operation_row.pay_batch_id=batch_candidate.pay_batch_id
              OR EXISTS (
                SELECT 1
                FROM public.banking_pay_operation_candidate_scope AS operation_scope_link
                WHERE operation_scope_link.operation_id=operation_row.id
                  AND operation_scope_link.candidate_id=batch_candidate.candidate_id
                  AND operation_scope_link.pay_batch_id=batch_candidate.pay_batch_id
                  AND operation_scope_link.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
              )
            )
          ORDER BY operation_row.completed_at_utc DESC NULLS LAST,
                   operation_row.created_at_utc DESC
          LIMIT 1
        ) AS draft_operation ON true
        LEFT JOIN public.banking_pay_operation_candidate_scope AS draft_scope
          ON draft_scope.operation_id=draft_operation.id
         AND draft_scope.candidate_id=batch_candidate.candidate_id
         AND draft_scope.pay_batch_id=batch_candidate.pay_batch_id
         AND draft_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
        LEFT JOIN public.app_change_counters AS candidate_counter
          ON candidate_counter.entity_key='pay_candidate:'||batch_candidate.candidate_id::text
        LEFT JOIN private.banking_pay_workbench_candidate_scope_registry AS registry
          ON registry.candidate_id=batch_candidate.candidate_id
        LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
          ON candidate_state.session_id=batch_row.source_workbench_session_id
         AND candidate_state.candidate_id=batch_candidate.candidate_id
        WHERE batch_candidate.pay_batch_id=p_pay_batch_id
      ), normalised AS (
        SELECT authority_rows.*,
          CASE WHEN COALESCE(post_draft_authority->>'source_change_seq','') ~ '^[0-9]{1,18}$'
            THEN (post_draft_authority->>'source_change_seq')::bigint ELSE NULL::bigint END
            AS frozen_source_change_seq,
          CASE WHEN COALESCE(post_draft_authority->>'dirty_generation','') ~ '^[0-9]{1,18}$'
            THEN (post_draft_authority->>'dirty_generation')::bigint ELSE NULL::bigint END
            AS frozen_dirty_generation
        FROM authority_rows
      ), classified AS (
        SELECT normalised.*,
          (
            COALESCE(post_draft_authority->>'contract_version','')='POST_DRAFT_LIVE_AUTHORITY_V2'
            AND COALESCE(post_draft_authority->>'draft_operation_id','')=draft_operation_id::text
            AND COALESCE(post_draft_authority->>'pay_batch_id','')=p_pay_batch_id::text
            AND COALESCE(post_draft_authority->>'workbench_session_id','')=source_workbench_session_id::text
            AND COALESCE(post_draft_authority->>'candidate_id','')=candidate_id::text
            AND COALESCE((post_draft_authority->>'fast_reversion_eligible')::boolean,false)
            AND COALESCE(post_draft_authority->>'original_source_publication_id','')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND frozen_source_change_seq IS NOT NULL
            AND frozen_dirty_generation IS NOT NULL
            AND live_source_change_seq=frozen_source_change_seq
            AND live_dirty_generation=frozen_dirty_generation
            AND registry_source_change_seq=live_source_change_seq
            AND registry_dirty_generation=live_dirty_generation
            AND candidate_state_source_change_seq=live_source_change_seq
            AND candidate_state_session_version=source_session_version
          ) AS pre_request_exact
        FROM normalised
      )
      SELECT COALESCE(pg_catalog.jsonb_object_agg(
        classified.candidate_id::text,
        pg_catalog.jsonb_build_object(
          'contract_version','DRAFT_OVERLAY_FAST_PRE_REQUEST_AUTHORITY_V1',
          'candidate_id',classified.candidate_id,
          'pay_batch_candidate_id',classified.pay_batch_candidate_id,
          'source_change_seq',classified.live_source_change_seq,
          'dirty_generation',classified.live_dirty_generation,
          'post_draft_authority_digest',classified.post_draft_authority->>'authority_digest',
          'original_source_publication_id',
            classified.post_draft_authority->>'original_source_publication_id',
          'fast_reversion_eligible',classified.pre_request_exact,
          'pre_request_exact',classified.pre_request_exact,
          'rejection_reason',CASE WHEN classified.pre_request_exact THEN NULL
            WHEN COALESCE((classified.post_draft_authority->>'fast_reversion_eligible')::boolean,false) IS NOT TRUE
              OR COALESCE(classified.post_draft_authority->>'original_source_publication_id','')
                   !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN 'LEGACY_PHYSICAL_PUBLICATION_MISSING'
            ELSE 'PRE_REQUEST_ECONOMIC_AUTHORITY_NOT_CURRENT' END,
          'fence_digest',pg_catalog.md5(
            classified.candidate_id::text||'|'||classified.live_source_change_seq::text||'|'||
            classified.live_dirty_generation::text||'|'||
            COALESCE(classified.post_draft_authority->>'authority_digest','')||'|'||
            COALESCE(classified.post_draft_authority->>'original_source_publication_id','')||'|'||
            COALESCE(classified.registry_source_change_seq::text,'')||'|'||
            COALESCE(classified.registry_dirty_generation::text,'')||'|'||
            COALESCE(classified.candidate_state_source_change_seq::text,'')||'|'||
            COALESCE(classified.candidate_state_session_version::text,'')||'|'||
            classified.pre_request_exact::text||'|DRAFT_OVERLAY_FAST_PRE_REQUEST_AUTHORITY_V1'
          )
        ) ORDER BY classified.candidate_id
      ),'{}'::jsonb)
      INTO v_draft_overlay_pre_request_authorities
      FROM classified;
    END IF;

    -- Scheduled/executed-not-submitted cancellation has a different safe
    -- baseline from an untouched Draft.  Prove, before the correction request
    -- writes anything, that the current certified source differs from the
    -- immutable Draft only by the closed unsent-payment overlay.  Explicit
    -- pages are bounded to 100 candidates; larger/all-matching requests retain
    -- the safe fallback until the operation's bounded page owner captures the
    -- same proof per page.
    IF COALESCE(v_scheduled_reversion_v2_observe_enabled,false)
       AND v_action IN ('PRE_BANK_CANCEL','CANCEL_PAYMENT')
       AND v_mode='EXPLICIT'
       AND v_requested_explicit_count BETWEEN 1 AND 100 THEN
      v_cancel_reversion_pre_request_proof :=
        private.pay_workbench_cancel_reversion_proof_core_v1(
          NULL::uuid,NULL::uuid,NULL::uuid,ARRAY[]::uuid[],'PRE_REQUEST',
          pg_catalog.jsonb_build_object(
            'pay_batch_id',p_pay_batch_id,
            'candidate_ids',COALESCE((
              SELECT pg_catalog.jsonb_agg(batch_candidate.candidate_id ORDER BY batch_candidate.candidate_id)
              FROM public.pay_batch_candidates AS batch_candidate
              WHERE batch_candidate.pay_batch_id=p_pay_batch_id
                AND batch_candidate.id::text IN (
                  SELECT explicit_token.value
                  FROM pg_catalog.jsonb_array_elements_text(v_canonical_explicit_tokens)
                    AS explicit_token(value)
                )
            ),'[]'::jsonb)
          )
        );

      SELECT COALESCE(pg_catalog.jsonb_object_agg(
        result_row.value->>'candidate_id',
        pg_catalog.jsonb_build_object(
          'contract_version','CANCELLATION_REVERSION_PRE_REQUEST_AUTHORITY_V2',
          'candidate_id',result_row.value->>'candidate_id',
          'pay_batch_candidate_id',result_row.value->>'pay_batch_candidate_id',
          'source_change_seq',result_row.value->>'source_change_seq',
          'dirty_generation',result_row.value->>'dirty_generation',
          'original_economic_build_id',result_row.value->>'original_economic_build_id',
          'current_economic_build_id',result_row.value->>'current_economic_build_id',
          'original_source_publication_id',result_row.value->>'original_source_publication_id',
          'current_source_publication_id',result_row.value->>'current_source_publication_id',
          'invariant_economic_digest',result_row.value->>'invariant_economic_digest',
          'pre_request_exact',COALESCE((result_row.value->>'admitted')::boolean,false),
          'fast_reversion_eligible',COALESCE((result_row.value->>'admitted')::boolean,false),
          'rejection_reason',result_row.value->>'rejection_reason',
          'authority_digest',result_row.value->>'authority_digest'
        )
      ),'{}'::jsonb)
      INTO v_cancel_reversion_pre_request_authorities_v2
      FROM pg_catalog.jsonb_array_elements(
        COALESCE(v_cancel_reversion_pre_request_proof->'candidate_results','[]'::jsonb)
      ) AS result_row(value);
    END IF;

    v_selection := p_selection_json || pg_catalog.jsonb_build_object(
      'contract_version', 1,
      'mode', v_mode,
      'requested_action', v_action,
      'filter_json', v_filter,
      'sort_key', v_sort_key,
      'sort_direction', v_sort_direction,
      'snapshot_token', v_snapshot_token,
      'scope_fence_hash', v_active_scope_hash,
      'requested_explicit_count', v_requested_explicit_count,
      'requested_explicit_hash', v_requested_explicit_hash,
      'draft_overlay_fast_pre_request_authorities',
        CASE WHEN v_action='DRAFT_CANCEL'
          THEN v_draft_overlay_pre_request_authorities ELSE '{}'::jsonb END,
      'cancellation_reversion_pre_request_authorities_v2',
        CASE WHEN v_action IN ('PRE_BANK_CANCEL','CANCEL_PAYMENT')
          THEN v_cancel_reversion_pre_request_authorities_v2 ELSE '{}'::jsonb END,
      'canonical_explicit_candidate_tokens', CASE
        WHEN v_mode = 'EXPLICIT' THEN v_canonical_explicit_tokens ELSE '[]'::jsonb END,
      'selection', coalesce(p_selection_json->'selection', '{}'::jsonb) || pg_catalog.jsonb_build_object(
        'mode', v_mode,
        'action', v_action,
        'filter_json', v_filter,
        'sort_key', v_sort_key,
        'sort_direction', v_sort_direction,
        'snapshot_token', v_snapshot_token,
        'scope_fence_hash', v_active_scope_hash,
        'explicit_candidate_tokens', CASE WHEN v_mode = 'EXPLICIT' THEN v_canonical_explicit_tokens ELSE '[]'::jsonb END,
        'requested_explicit_count', v_requested_explicit_count,
        'requested_explicit_hash', v_requested_explicit_hash
      )
    );
    -- The pre-request authority is database-derived runtime evidence, not
    -- caller intent.  Keep it outside the descriptor/idempotency identity so
    -- a lost PREPARE response reuses the original request and its original
    -- fence instead of creating a second request after request-owned dirtying.
    v_descriptor_hash := private.pay_payment_correction_sha256_v1(
      v_selection - 'command' - 'draft_overlay_fast_pre_request_authorities'
        - 'cancellation_reversion_pre_request_authorities_v2'
    );
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
        'continuation', pg_catalog.jsonb_build_object(
          'required', v_request.status = 'PLANNING', 'operation_id', v_operation_id,
          'operation_type', 'PAYMENT_CORRECTION', 'pay_batch_id', v_request.pay_batch_id,
          'root_operation_id', NULL, 'phase', (SELECT operation_row.phase FROM public.banking_pay_operations AS operation_row WHERE operation_row.id = v_operation_id),
          'run_after_utc', (SELECT operation_row.run_after_utc FROM public.banking_pay_operations AS operation_row WHERE operation_row.id = v_operation_id),
          'reason', 'PAYMENT_CORRECTION_PREPARE', 'successor_relation', CASE WHEN v_request.status = 'PLANNING' THEN 'SELF' ELSE 'NONE' END,
          'requires_user_action', v_request.status <> 'PLANNING', 'terminal', v_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
        ),
        'code', 'PAYMENT_CORRECTION_REQUEST_EXISTING'
      );
    END IF;

    v_request_id := extensions.gen_random_uuid();
    v_plan_json := pg_catalog.jsonb_build_object(
      'contract_version', 1, 'descriptor_hash', v_descriptor_hash,
      'active_batch_scope_hash', v_active_scope_hash, 'requested_action', v_action,
      'scope_fence_hash', v_active_scope_hash,
      'requested_explicit_count', v_requested_explicit_count,
      'requested_explicit_hash', v_requested_explicit_hash,
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
      'continuation', pg_catalog.jsonb_build_object(
        'required', true, 'operation_id', v_operation_id, 'operation_type', 'PAYMENT_CORRECTION',
        'pay_batch_id', p_pay_batch_id, 'root_operation_id', NULL, 'phase', 'PREPARE_SELECTION',
        'run_after_utc', v_now, 'reason', 'PAYMENT_CORRECTION_PREPARE', 'successor_relation', 'SELF',
        'requires_user_action', false, 'terminal', false
      ),
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
  v_resume_reauthenticated_request :=
    v_command = 'START_PREPARED'
    AND coalesce(v_request.auto_requested, false) IS NOT TRUE
    AND v_request.status IN ('REQUESTED', 'AWAITING_AUTHORISATION')
    AND v_request.requested_by_user_id IS NOT DISTINCT FROM p_actor_user_id
    AND greatest(coalesce(v_request.required_quantity, 1), 1) = 1
    AND coalesce(v_request.approved_count, 0) = 0
    AND v_request.reauth_consumed_at_utc IS NOT NULL
    AND v_request.reauth_proof_hash IS NOT DISTINCT FROM v_proof_hash
    AND coalesce(v_request.plan_json->>'requested_action', '') IN (
      'DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT',
      'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND'
    );

  -- Recheck the pre-request fence before START_PREPARED performs any
  -- cancellation-owned updates.  This catches a genuine economic change
  -- during the user's reauthentication interval.  The result remains
  -- candidate-specific so mixed batches can safely fall back per candidate.
  IF v_command='START_PREPARED'
     AND COALESCE(v_request.plan_json->>'requested_action','') IN (
       'DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT'
     )
     AND (
       COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL'
       OR COALESCE(v_scheduled_reversion_v2_observe_enabled,false)
     ) THEN
    FOR v_candidate_id IN
      SELECT DISTINCT batch_candidate.candidate_id
      FROM public.pay_payment_correction_request_candidates AS request_candidate
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=request_candidate.pay_batch_candidate_id
      WHERE request_candidate.correction_request_id=v_request_id
      ORDER BY batch_candidate.candidate_id
    LOOP
      PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
          public._pay_workbench_candidate_serial_key(v_candidate_id),
          24062027
        )
      );
    END LOOP;

    WITH selected_candidates AS (
      SELECT batch_candidate.candidate_id,
             CASE
               WHEN COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL'
                 THEN v_request.selection_json->'draft_overlay_fast_pre_request_authorities'
                        ->batch_candidate.candidate_id::text
               ELSE v_request.selection_json->'cancellation_reversion_pre_request_authorities_v2'
                        ->batch_candidate.candidate_id::text
             END AS pre_request_authority,
             COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL' AS is_draft_overlay,
             COALESCE(candidate_counter.seq,0) AS live_source_change_seq,
             COALESCE(candidate_counter.scope_change_generation,0) AS live_dirty_generation,
             request_owned_dirty.job_id AS request_owned_dirty_job_id
      FROM public.pay_payment_correction_request_candidates AS request_candidate
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=request_candidate.pay_batch_candidate_id
      LEFT JOIN public.app_change_counters AS candidate_counter
        ON candidate_counter.entity_key='pay_candidate:'||batch_candidate.candidate_id::text
      LEFT JOIN LATERAL (
        SELECT dirty_job.id AS job_id
        FROM public.banking_pay_workbench_jobs AS dirty_job
        WHERE dirty_job.candidate_id=batch_candidate.candidate_id
          -- Finance/correction transition dirty jobs are canonical
          -- candidate-global owners and therefore legitimately carry a NULL
          -- session_id.  Session-scoped recovery jobs must still match the
          -- Draft's source Workbench session exactly.
          AND (
            dirty_job.session_id IS NULL
            OR dirty_job.session_id=v_batch.source_workbench_session_id
          )
          AND dirty_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND (
            dirty_job.status IN ('QUEUED','RUNNING')
            OR (
              dirty_job.status='SUCCEEDED'
              AND COALESCE(
                (dirty_job.payload_json->>'coalesced_to_current_refresh_authority')::boolean,
                false
              )
              AND COALESCE(dirty_job.payload_json->>'coalesced_owner_resolution','')
                    ='COMPLETE_CURRENT_AUTHORITY'
              AND COALESCE(dirty_job.payload_json->>'actual_refresh_scope_status','')
                    ='MATERIALISED'
            )
          )
          AND dirty_job.created_at_utc>=v_request.created_at_utc
          AND dirty_job.scope_change_generation=COALESCE(candidate_counter.scope_change_generation,0)
          AND COALESCE(dirty_job.payload_json->>'source_change_seq','')
                =COALESCE(candidate_counter.seq,0)::text
          AND COALESCE((dirty_job.payload_json->>'policy_x_dirtying_only')::boolean,false)
          AND COALESCE((dirty_job.payload_json->>'economic_truth_mutation_allowed')::boolean,true) IS FALSE
          AND (
            (
              pg_catalog.jsonb_typeof(COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb))='array'
              AND pg_catalog.jsonb_array_length(COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb))>0
              AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.jsonb_array_elements_text(
                  COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb)
                ) AS dirty_reason(value)
                WHERE pg_catalog.upper(pg_catalog.btrim(dirty_reason.value)) NOT IN (
                  'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
                  'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
                  'DIRTY_TRIGGER:PAY_BATCHES:UPDATE'
                )
              )
            )
            OR (
              COALESCE(dirty_job.payload_json->>'correction_dirty_causal_contract_version','')
                ='CORRECTION_OWNED_DIRTY_CAUSAL_V1'
              AND COALESCE(dirty_job.payload_json->'correction_dirty_contexts'
                    ->batch_candidate.candidate_id::text->>'correction_request_id','')
                    =v_request_id::text
              AND COALESCE(dirty_job.payload_json->'correction_dirty_contexts'
                    ->batch_candidate.candidate_id::text->>'pay_batch_id','')
                    =v_request.pay_batch_id::text
              AND COALESCE(dirty_job.payload_json->>'request_owned_scope_change_tx_token','')
                    =COALESCE(dirty_job.payload_json->>'scope_change_tx_token','')
            )
          )
        ORDER BY dirty_job.created_at_utc,dirty_job.id
        LIMIT 1
      ) AS request_owned_dirty ON true
      WHERE request_candidate.correction_request_id=v_request_id
    ), classified AS (
      SELECT selected_candidates.*,
        (
          COALESCE(pre_request_authority->>'contract_version','')=
            CASE WHEN is_draft_overlay THEN 'DRAFT_OVERLAY_FAST_PRE_REQUEST_AUTHORITY_V1'
                 ELSE 'CANCELLATION_REVERSION_PRE_REQUEST_AUTHORITY_V2' END
          AND COALESCE(pre_request_authority->>'candidate_id','')=candidate_id::text
          AND COALESCE((pre_request_authority->>'pre_request_exact')::boolean,false)
          AND COALESCE(pre_request_authority->>'source_change_seq','') ~ '^[0-9]{1,18}$'
          AND COALESCE(pre_request_authority->>'dirty_generation','') ~ '^[0-9]{1,18}$'
          AND (
            (
              live_source_change_seq=(pre_request_authority->>'source_change_seq')::bigint
              AND live_dirty_generation=(pre_request_authority->>'dirty_generation')::bigint
            )
            OR request_owned_dirty_job_id IS NOT NULL
          )
        ) AS start_exact
      FROM selected_candidates
    )
    SELECT COALESCE(pg_catalog.jsonb_object_agg(
      classified.candidate_id::text,
      pg_catalog.jsonb_build_object(
        'candidate_id',classified.candidate_id,
        'start_exact',classified.start_exact,
        'rejection_reason',CASE WHEN classified.start_exact THEN NULL
          ELSE 'ECONOMIC_AUTHORITY_CHANGED_BEFORE_CANCELLATION_START' END,
        'pre_request_fence_digest',COALESCE(
          classified.pre_request_authority->>'fence_digest',
          classified.pre_request_authority->>'authority_digest'
        ),
        'request_owned_dirty_job_id',classified.request_owned_dirty_job_id,
        'request_owned_dirty_proven',classified.request_owned_dirty_job_id IS NOT NULL
      ) ORDER BY classified.candidate_id
    ),'{}'::jsonb)
    INTO v_draft_overlay_start_admission
    FROM classified;
  END IF;

  IF v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING')
     AND coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE
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
      'continuation', pg_catalog.jsonb_build_object(
        'required', v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING'),
        'operation_id', v_operation.id, 'operation_type', 'PAYMENT_CORRECTION',
        'pay_batch_id', v_request.pay_batch_id, 'root_operation_id', v_operation.root_operation_id,
        'phase', v_operation.phase, 'run_after_utc', v_operation.run_after_utc,
        'reason', 'PAYMENT_CORRECTION_ALREADY_STARTED',
        'successor_relation', CASE WHEN v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING') THEN 'SELF' ELSE 'NONE' END,
        'requires_user_action', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
        'terminal', v_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
      ),
      'code', 'REQUEST_ALREADY_STARTED'
    );
  END IF;

  IF coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE THEN
    v_mutation_guard := private.pay_payment_mutation_guard_v1(p_pay_batch_id, NULL::uuid, 'NEW_PAYMENT_ACTION');
    IF coalesce((v_mutation_guard->>'ok')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION '%', coalesce(v_mutation_guard->>'code', 'PAYMENT_CHANGE_IN_PROGRESS')
        USING ERRCODE = 'P0001', DETAIL = v_mutation_guard::text;
    END IF;
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

  IF pg_catalog.upper(pg_catalog.btrim(coalesce(v_batch.status, ''))) IN (
    'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_BATCH_TERMINAL'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_BATCH_TERMINAL',
        'batch_status', v_batch.status
      )::text;
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.input_json->>'correction_request_id' = v_request_id::text
  ORDER BY operation_row.created_at_utc LIMIT 1
  FOR UPDATE;

  IF v_operation.id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_STATE_INVALID')::text;
  END IF;

  IF v_request.status IS DISTINCT FROM 'PLANNED'
     AND NOT coalesce(v_resume_reauthenticated_request, false) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_STATE_INVALID')::text;
  END IF;

  v_active_scope_hash := private.pay_payment_correction_sha256_v1(
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
      'continuation', pg_catalog.jsonb_build_object(
        'required', false, 'operation_id', v_operation.id, 'operation_type', 'PAYMENT_CORRECTION',
        'pay_batch_id', v_request.pay_batch_id, 'root_operation_id', v_operation.root_operation_id,
        'phase', 'COMPLETE', 'run_after_utc', NULL, 'reason', 'SELECTION_SCOPE_CHANGED',
        'successor_relation', 'NONE', 'requires_user_action', true, 'terminal', true
      ),
      'code', 'SELECTION_SCOPE_CHANGED',
      'message', 'Payment status changed. Refresh and select the payments again.'
    );
  END IF;

  IF v_command = 'START_AUTO' THEN
    SELECT coalesce(settings_row.banking_pay_auto_unwind_terminal_no_money, false)
    INTO v_auto_unwind_enabled
    FROM public.settings_defaults AS settings_row
    ORDER BY settings_row.id
    LIMIT 1;

    SELECT event_row.*
    INTO v_source_event
    FROM public.pay_bank_transfer_events AS event_row
    WHERE event_row.id = v_request.source_bank_event_id
      AND event_row.pay_batch_id = v_batch.id;

    v_auto_classification_result := public._pay_payment_movement_classify(
      v_batch.id,
      v_request.selection_json
    );

    v_existing_money_moved := false;
    IF v_source_event.pay_bank_transfer_id IS NOT NULL THEN
      SELECT coalesce(movement_state.is_final_money_moved, false)
      INTO v_existing_money_moved
      FROM public.pay_bank_transfers AS transfer_row
      CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
        transfer_row.status,
        transfer_row.rail_state,
        coalesce(transfer_row.rail_meta_json, '{}'::jsonb),
        coalesce(transfer_row.rail_meta_json, '{}'::jsonb)
      ) AS movement_state
      WHERE transfer_row.id = v_source_event.pay_bank_transfer_id
      LIMIT 1;

      v_existing_money_moved := coalesce(v_existing_money_moved, false) OR EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS paid_event
        WHERE paid_event.pay_bank_transfer_id = v_source_event.pay_bank_transfer_id
          AND paid_event.id IS DISTINCT FROM v_source_event.id
          AND paid_event.normalised_state IN ('COMPLETED', 'PAID', 'SETTLED')
      );
    END IF;

    IF coalesce(v_request.auto_requested, false) IS NOT TRUE
       OR v_request.source_bank_event_id IS NULL
       OR v_request.selection_json->>'requested_action' IS DISTINCT FROM 'NO_MONEY_UNWIND'
       OR coalesce(v_auto_unwind_enabled, false) IS NOT TRUE
       OR v_source_event.id IS NULL
       OR v_source_event.pay_bank_transfer_id IS NULL
       OR v_source_event.event_source NOT IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
       OR v_source_event.provider_event_transport NOT IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
       OR v_source_event.mapping_status IS DISTINCT FROM 'MATCHED'
       OR v_source_event.mapping_method NOT IN (
         'TRANSFER_ID', 'PROVIDER_EVENT_ID', 'PROVIDER_TRANSACTION_ID', 'REQUEST_ID',
         'PROVIDER_REFERENCE', 'RAIL_TX_ID', 'MATCHED_PROVIDER_EVENT', 'MANUAL_TRANSFER_SELECTION'
       )
       OR v_source_event.normalised_state NOT IN ('FAILED', 'REJECTED', 'CANCELLED')
       OR v_source_event.correction_disposition IS DISTINCT FROM 'AUTO_PROCESSING'
       OR coalesce((v_auto_classification_result->>'safe_to_auto_apply')::boolean, false) IS NOT TRUE
       OR (
         v_auto_classification_result->>'classification' NOT IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
         AND v_auto_classification_result->>'recommended_action' IS DISTINCT FROM 'NO_MONEY_UNWIND_AND_RECALCULATE'
       )
       OR coalesce(v_existing_money_moved, false)
       OR pg_catalog.jsonb_typeof(v_request.accepted_resolution_json) IS DISTINCT FROM 'object'
       OR v_request.accepted_resolution_json->>'contract_version' IS DISTINCT FROM '2'
       OR v_request.accepted_resolution_json->>'requested_action' IS DISTINCT FROM 'NO_MONEY_UNWIND'
       OR v_request.accepted_resolution_json->>'source_bank_event_id' IS DISTINCT FROM v_source_event.id::text
       OR v_request.accepted_resolution_json->>'source' IS DISTINCT FROM v_source_event.provider_event_transport
       OR v_request.accepted_resolution_json->>'event_source' IS DISTINCT FROM v_source_event.event_source
       OR v_request.accepted_resolution_json->>'provider_key' IS DISTINCT FROM v_source_event.provider_key
       OR v_request.accepted_resolution_json->>'provider_event_id' IS DISTINCT FROM v_source_event.provider_event_id
       OR v_request.accepted_resolution_json->>'provider_event_key' IS DISTINCT FROM v_source_event.provider_event_key
       OR v_request.accepted_resolution_json->>'provider_webhook_receipt_id'
            IS DISTINCT FROM (CASE WHEN v_source_event.provider_webhook_receipt_id IS NULL THEN NULL ELSE v_source_event.provider_webhook_receipt_id::text END)
       OR v_request.accepted_resolution_json->>'provider_transaction_id' IS DISTINCT FROM v_source_event.provider_transaction_id
       OR v_request.accepted_resolution_json->>'provider_request_id' IS DISTINCT FROM v_source_event.provider_request_id
       OR v_request.accepted_resolution_json->>'mapping_status' IS DISTINCT FROM v_source_event.mapping_status
       OR v_request.accepted_resolution_json->>'mapping_method' IS DISTINCT FROM v_source_event.mapping_method
       OR v_request.accepted_resolution_json->>'normalised_state' IS DISTINCT FROM v_source_event.normalised_state
       OR v_request.accepted_resolution_json->>'classification' IS DISTINCT FROM v_auto_classification_result->>'classification'
       OR v_request.accepted_resolution_json->>'recommended_action' IS DISTINCT FROM v_auto_classification_result->>'recommended_action'
       OR coalesce((v_request.accepted_resolution_json->>'safe_to_auto_apply')::boolean, false) IS NOT TRUE
       OR v_request.accepted_resolution_json->>'correction_disposition' IS DISTINCT FROM 'AUTO_PROCESSING'
       OR coalesce((v_request.accepted_resolution_json->>'terminal_no_money_evidence')::boolean, false) IS NOT TRUE
       OR coalesce((v_request.accepted_resolution_json->>'signature_valid')::boolean, false)
            IS DISTINCT FROM coalesce(v_source_event.provider_signature_valid, (
              SELECT receipt_row.signature_valid
              FROM public.bank_provider_webhook_receipts AS receipt_row
              WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
            ), false)
       OR (
         v_source_event.provider_event_transport IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
         AND (
           v_source_event.provider_webhook_receipt_id IS NULL
           OR coalesce(v_source_event.provider_signature_valid, (
             SELECT receipt_row.signature_valid
             FROM public.bank_provider_webhook_receipts AS receipt_row
             WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
           ), false) IS NOT TRUE
           OR NOT EXISTS (
             SELECT 1
             FROM public.bank_provider_webhook_receipts AS receipt_row
             WHERE receipt_row.id = v_source_event.provider_webhook_receipt_id
               AND receipt_row.signature_valid IS TRUE
               AND receipt_row.provider_key IS NOT DISTINCT FROM v_source_event.provider_key
               AND receipt_row.rail_env IS NOT DISTINCT FROM v_source_event.rail_env
               AND pg_catalog.upper(coalesce(receipt_row.status, '')) IN (
                 'VERIFIED', 'NORMALISED', 'NORMALIZED', 'INGESTED', 'FAILED_RETRYABLE', 'UNMATCHED_REVIEW_REQUIRED'
               )
           )
         )
       ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_AUTO_START_NOT_ALLOWED'
        USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_AUTO_EVIDENCE_INVALID',
          'source_bank_event_id', v_request.source_bank_event_id
        )::text;
    END IF;
  ELSE
    IF p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id
       OR v_proof_hash IS NULL OR v_proof_hash !~ '^[0-9a-f]{64}$'
       OR v_request.reauth_proof_hash IS DISTINCT FROM v_proof_hash
       OR (
         coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE
         AND v_request.reauth_consumed_at_utc IS NOT NULL
       )
       OR (
         coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE
         AND (
           v_request.reauth_expires_at_utc IS NULL
           OR v_request.reauth_expires_at_utc <= v_now
         )
       )
       OR p_selection_json->>'selection_hash' IS DISTINCT FROM v_request.selection_hash
       OR p_selection_json->>'plan_hash' IS DISTINCT FROM v_request.plan_hash
       OR NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '') IS DISTINCT FROM v_request.reason THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTH_PROOF_INVALID'
        USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'REAUTH_PROOF_INVALID')::text;
    END IF;
  END IF;

  IF v_command IN ('START_AUTO', 'START_PREPARED') THEN
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

    UPDATE public.pay_batches AS correction_batch
    SET status = CASE
          WHEN coalesce(v_request.plan_json->>'requested_action', '') = 'DRAFT_CANCEL'
            THEN correction_batch.status
          ELSE 'AWAITING_AUTHORISATION'
        END,
        schedule_kind = NULL,
        scheduled_at_utc = NULL,
        scheduled_by_user_id = NULL,
        funding_account_ref = NULL,
        funds_warning_hours_json = NULL,
        execution_intent_json = coalesce(correction_batch.execution_intent_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'correction_request_id', v_request.id,
            'old_authorisation_invalidated_at_utc', v_now,
            'old_schedule_invalidated_at_utc', v_now,
            'reauthorisation_required',
              coalesce(v_request.plan_json->>'requested_action', '') <> 'DRAFT_CANCEL',
            'automatic_provider_no_money', v_command = 'START_AUTO',
            'requester_reauthenticated_cancellation', v_command = 'START_PREPARED'
          )
    WHERE correction_batch.id = v_request.pay_batch_id;
  END IF;

  UPDATE public.pay_payment_correction_requests AS started_request
  SET status = 'AUTHORISED',
      required_quantity = 1,
      reauth_consumed_at_utc = CASE
        WHEN v_command = 'START_AUTO' THEN NULL
        WHEN coalesce(v_resume_reauthenticated_request, false) THEN started_request.reauth_consumed_at_utc
        ELSE v_now
      END,
      authorised_at_utc = coalesce(started_request.authorised_at_utc, v_now),
      approved_count = 1,
      plan_json = coalesce(started_request.plan_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'old_authorisation_request_ids', v_old_auth_request_ids,
          'old_schedule_kind', v_old_schedule_kind,
          'old_scheduled_at_utc', v_old_scheduled_at_utc,
          'old_authorisation_invalidated_at_utc', v_now,
          'old_schedule_invalidated_at_utc', v_now,
          'single_user_cancellation_authority', true
        ),
      updated_at_utc = v_now
  WHERE started_request.id = v_request_id
  RETURNING * INTO v_request;

  UPDATE public.banking_pay_operations AS started_operation
  SET status = 'RUNNING',
      phase = 'EXPAND_WORK',
      runner_state = 'RUNNABLE',
      requires_user_action = false,
      run_after_utc = v_now,
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
    CASE WHEN coalesce(v_resume_reauthenticated_request, false) THEN 'AUTHORISE' ELSE 'REQUEST' END,
    v_now,
    CASE
      WHEN v_command = 'START_AUTO' THEN 'Immutable automatic correction request started.'
      ELSE 'Requester reauthenticated and authorised the single-user cancellation.'
    END,
    NULL, pg_catalog.to_jsonb(v_request),
    pg_catalog.jsonb_build_object(
      'code', CASE
        WHEN v_command = 'START_AUTO' THEN 'AUTO_REQUEST_STARTED'
        WHEN coalesce(v_resume_reauthenticated_request, false) THEN 'LEGACY_REQUESTER_REAUTHORISED_CANCELLATION_RESUMED'
        ELSE 'REQUESTER_REAUTHORISED_CANCELLATION'
      END
    )
  );

  PERFORM public.banking_pay_batch_signal_touch(
    p_pay_batch_id := v_request.pay_batch_id,
    p_change_reason := CASE
      WHEN v_command = 'START_AUTO' THEN 'PAYMENT_CORRECTION_AUTO_AUTHORISED'
      ELSE 'PAYMENT_CORRECTION_REQUESTER_AUTHORISED'
    END,
    p_change_source := 'pay_payment_correction_request_start',
    p_change_scope_json := pg_catalog.jsonb_build_object(
      'correction_request_id', v_request.id,
      'operation_id', v_operation.id,
      'source_bank_event_id', v_request.source_bank_event_id,
      'requested_action', v_request.plan_json ->> 'requested_action',
      'single_user_cancellation_authority', v_command = 'START_PREPARED'
    )
  );

  IF v_command='START_PREPARED'
     AND COALESCE(v_request.plan_json->>'requested_action','') IN (
       'DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT'
     ) THEN
    WITH selected_candidates AS (
      SELECT batch_candidate.candidate_id,
             v_draft_overlay_start_admission->batch_candidate.candidate_id::text
               AS start_admission,
             COALESCE(candidate_counter.seq,0) AS live_source_change_seq,
             COALESCE(candidate_counter.scope_change_generation,0) AS live_dirty_generation
      FROM public.pay_payment_correction_request_candidates AS request_candidate
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=request_candidate.pay_batch_candidate_id
      LEFT JOIN public.app_change_counters AS candidate_counter
        ON candidate_counter.entity_key='pay_candidate:'||batch_candidate.candidate_id::text
      WHERE request_candidate.correction_request_id=v_request_id
    )
    SELECT COALESCE(pg_catalog.jsonb_object_agg(
      selected_candidates.candidate_id::text,
      pg_catalog.jsonb_build_object(
        'contract_version',CASE
          WHEN COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL'
            THEN 'DRAFT_OVERLAY_FAST_START_AUTHORITY_V1'
          ELSE 'CANCELLATION_REVERSION_START_AUTHORITY_V2' END,
        'correction_request_id',v_request_id,
        'operation_id',v_operation.id,
        'candidate_id',selected_candidates.candidate_id,
        'source_change_seq',selected_candidates.live_source_change_seq,
        'dirty_generation',selected_candidates.live_dirty_generation,
        'start_exact',COALESCE((selected_candidates.start_admission->>'start_exact')::boolean,false),
        'rejection_reason',selected_candidates.start_admission->>'rejection_reason',
        'pre_request_fence_digest',selected_candidates.start_admission->>'pre_request_fence_digest',
        'request_owned_dirty_job_id',selected_candidates.start_admission->>'request_owned_dirty_job_id',
        'request_owned_dirty_proven',COALESCE(
          (selected_candidates.start_admission->>'request_owned_dirty_proven')::boolean,false
        ),
        'fence_digest',pg_catalog.md5(
          v_request_id::text||'|'||v_operation.id::text||'|'||
          selected_candidates.candidate_id::text||'|'||
          selected_candidates.live_source_change_seq::text||'|'||
          selected_candidates.live_dirty_generation::text||'|'||
          COALESCE(selected_candidates.start_admission->>'pre_request_fence_digest','')||'|'||
          COALESCE(selected_candidates.start_admission->>'request_owned_dirty_job_id','')||'|'||
          COALESCE((selected_candidates.start_admission->>'request_owned_dirty_proven')::boolean,false)::text||'|'||
          COALESCE((selected_candidates.start_admission->>'start_exact')::boolean,false)::text||
          CASE WHEN COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL'
            THEN '|DRAFT_OVERLAY_FAST_START_AUTHORITY_V1'
            ELSE '|CANCELLATION_REVERSION_START_AUTHORITY_V2' END
        )
      ) ORDER BY selected_candidates.candidate_id
    ),'{}'::jsonb)
    INTO v_draft_overlay_start_authorities
    FROM selected_candidates;

    UPDATE public.banking_pay_operations AS authority_operation
    SET input_json=COALESCE(authority_operation.input_json,'{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'draft_overlay_fast_start_authorities',CASE
              WHEN COALESCE(v_request.plan_json->>'requested_action','')='DRAFT_CANCEL'
                THEN v_draft_overlay_start_authorities ELSE '{}'::jsonb END,
            'cancellation_reversion_start_authorities_v2',CASE
              WHEN COALESCE(v_request.plan_json->>'requested_action','') IN ('PRE_BANK_CANCEL','CANCEL_PAYMENT')
                THEN v_draft_overlay_start_authorities ELSE '{}'::jsonb END
          ),
        updated_at_utc=pg_catalog.clock_timestamp()
    WHERE authority_operation.id=v_operation.id
    RETURNING authority_operation.* INTO v_operation;
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
    'requires_authorisation', false,
    'display_status', CASE WHEN v_command = 'START_AUTO' THEN 'Failed payment release authorised' ELSE 'Cancellation authorised' END,
    'display_message', CASE WHEN v_command = 'START_AUTO' THEN 'CloudTMS is processing the provider-confirmed failed-payment release.' ELSE 'CloudTMS is processing the authorised cancellation.' END,
    'continuation', pg_catalog.jsonb_build_object(
      'required', v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING'),
      'operation_id', v_operation.id, 'operation_type', 'PAYMENT_CORRECTION',
      'pay_batch_id', v_request.pay_batch_id, 'root_operation_id', v_operation.root_operation_id,
      'phase', v_operation.phase, 'run_after_utc', v_operation.run_after_utc,
      'reason', CASE WHEN v_command = 'START_AUTO' THEN 'AUTO_NO_MONEY_START' ELSE 'PAYMENT_CORRECTION_START' END,
      'successor_relation', CASE WHEN v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING') THEN 'SELF' ELSE 'NONE' END,
      'requires_user_action', false,
      'terminal', false
    ),
    'code', CASE WHEN v_command = 'START_AUTO' THEN 'AUTO_REQUEST_STARTED' ELSE 'PAYMENT_CORRECTION_AUTHORISED' END
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) TO service_role;
