-- CloudTMS Banking Pay cancellation — Stage 1.
-- Materialise one bounded, immutable candidate-selection page.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(
    p_correction_request_id uuid,
    p_operation_id uuid,
    p_cursor_json jsonb DEFAULT NULL::jsonb,
    p_limit integer DEFAULT 50,
    p_worker_id text DEFAULT NULL::text,
    p_actor_user_id uuid DEFAULT NULL::uuid
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
    v_request public.pay_payment_correction_requests%ROWTYPE;
    v_operation public.banking_pay_operations%ROWTYPE;
    v_batch public.pay_batches%ROWTYPE;
    v_settings public.settings_defaults%ROWTYPE;
    v_selection jsonb;
    v_filter jsonb;
    v_mode text;
    v_requested_action text;
    v_snapshot_token text;
    v_expected_snapshot_token text;
    v_active_batch_scope_hash text;
    v_cursor_candidate_id uuid;
    v_next_cursor_json jsonb;
    v_page_candidate_count integer := 0;
    v_page_active_item_count integer := 0;
    v_page_source_row_count integer := 0;
    v_page_amount_pence bigint := 0;
    v_page_hash text;
    v_selection_hash text;
    v_plan_json jsonb;
    v_plan_hash text;
    v_complete boolean := false;
    v_sequence_no integer;
    v_start_ordinal bigint;
    v_last_candidate_id uuid;
    v_total_candidate_count integer;
    v_total_active_item_count integer;
    v_total_source_row_count integer;
    v_total_amount numeric(14,2);
    v_has_more boolean := false;
    v_candidate record;
    v_diagnostic jsonb;
    v_item_ids uuid[];
    v_item_count integer;
    v_source_row_count integer;
    v_amount numeric(14,2);
    v_candidate_scope_hash text;
    v_shared_instruction_scope_hash text;
    v_eligibility_code text;
    v_effective_display_state text;
    v_action_allowed boolean;
    v_max_candidates integer;
    v_max_active_items integer;
    v_max_items_per_candidate integer;
    v_max_source_rows_per_candidate integer;
    v_actor_user_id uuid;
    v_status_page jsonb := '{}'::jsonb;
    v_status_filter jsonb := '{}'::jsonb;
    v_status_snapshot_token text;
    v_status_read_limit integer;
    v_scan_candidate_tokens jsonb := '[]'::jsonb;
    v_scan_last_candidate_id uuid;
    v_scan_candidate_count integer := 0;
    v_candidate_selection_json jsonb := '{}'::jsonb;
    v_candidate_selected_scope_json jsonb := '{}'::jsonb;
    v_unselected_scope_hash_before text;
    v_page_sequence_no integer := 1;
    v_prior_page_hash text;
    v_first_scanned_candidate_id uuid;
    v_scanned_candidate_count integer := 0;
    v_selected_chain_hash text;
    v_unselected_chain_hash text;
    v_explicit_id_chain_hash text;
    v_candidate_audit_hash text;
    v_existing_chunk public.banking_pay_operation_chunks%ROWTYPE;
    v_requested_explicit_count integer := 0;
    v_requested_explicit_hash text;
    v_actual_explicit_count integer := 0;
    v_actual_explicit_hash text;
    v_explicit_missing_count integer := 0;
    v_explicit_extra_count integer := 0;
    v_page_chain_mismatch_count integer := 0;
BEGIN
    IF p_correction_request_id IS NULL OR p_operation_id IS NULL THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_IDENTIFIERS_REQUIRED'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'DESCRIPTOR_INVALID'
                  )::text;
    END IF;

    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 100 THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_LIMIT_INVALID'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'DESCRIPTOR_INVALID',
                      'limit', p_limit
                  )::text;
    END IF;

    IF p_cursor_json IS NOT NULL
       AND (
           pg_catalog.jsonb_typeof(p_cursor_json) <> 'object'
           OR p_cursor_json->>'page_sequence_no' !~ '^[0-9]+$'
           OR CASE WHEN p_cursor_json->>'page_sequence_no' ~ '^[0-9]+$'
                   THEN (p_cursor_json->>'page_sequence_no')::integer < 2 ELSE true END
           OR p_cursor_json ->> 'last_scanned_candidate_id'
                !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR p_cursor_json ->> 'prior_page_hash' !~ '^[0-9a-f]{64}$'
           OR p_cursor_json ->> 'scope_fence' !~ '^[0-9a-f]{64}$'
       ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_CURSOR_INVALID'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'DESCRIPTOR_INVALID'
                  )::text;
    END IF;

    IF p_worker_id IS NULL OR pg_catalog.btrim(p_worker_id) = '' THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_LEASE_REQUIRED'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'LEASE_REQUIRED',
                      'operation_id', p_operation_id
                  )::text;
    END IF;

    -- Non-gating planning order: request -> batch -> operation.
    SELECT request_row.*
    INTO v_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.id = p_correction_request_id
    FOR UPDATE;

    IF NOT FOUND OR v_request.status NOT IN ('PLANNING', 'PLANNED') THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_PLANNING'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'REQUEST_NOT_PLANNING',
                      'correction_request_id', p_correction_request_id
                  )::text;
    END IF;

    v_actor_user_id := COALESCE(p_actor_user_id, v_request.requested_by_user_id);
    IF p_actor_user_id IS NOT NULL
       AND p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_ACTOR_MISMATCH'
            USING ERRCODE = '42501',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'PERMISSION_DENIED'
                  )::text;
    END IF;

    SELECT batch_row.*
    INTO v_batch
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = v_request.pay_batch_id
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_BATCH_NOT_FOUND'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'SELECTION_STALE'
                  )::text;
    END IF;

    IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.status, ''))) IN (
        'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
    ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_BATCH_TERMINAL'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'PAYMENT_CORRECTION_BATCH_TERMINAL',
                      'batch_status', v_batch.status
                  )::text;
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_operation.operation_type IS DISTINCT FROM 'PAYMENT_CORRECTION'
       OR (v_request.status = 'PLANNING' AND v_operation.phase IS DISTINCT FROM 'PREPARE_SELECTION')
       OR (v_request.status = 'PLANNED' AND v_operation.phase IS DISTINCT FROM 'AWAITING_REAUTHENTICATION')
       OR v_operation.input_json ->> 'correction_request_id'
            IS DISTINCT FROM p_correction_request_id::text
       OR v_operation.pay_batch_id IS DISTINCT FROM v_request.pay_batch_id THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_OPERATION_MISMATCH'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'OPERATION_MISMATCH', 'operation_id', p_operation_id
            )::text;
    END IF;

    -- A committed final page may have moved the request and operation forward
    -- before the Worker received the response.  Return that exact durable page
    -- without requiring a now-released lease; a different cursor still fails.
    IF v_request.status = 'PLANNED' THEN
        v_page_sequence_no := COALESCE((p_cursor_json->>'page_sequence_no')::integer, 1);
        v_prior_page_hash := COALESCE(
          p_cursor_json->>'prior_page_hash',
          private.pay_payment_correction_sha256_v1(pg_catalog.jsonb_build_object(
            'version', 1, 'correction_request_id', v_request.id, 'chain', 'PREPARE_SELECTION'
          ))
        );
        SELECT chunk_row.* INTO v_existing_chunk
        FROM public.banking_pay_operation_chunks AS chunk_row
        WHERE chunk_row.operation_id = v_operation.id
          AND chunk_row.phase = 'PREPARE_SELECTION'
          AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
          AND chunk_row.sequence_no = v_page_sequence_no
          AND chunk_row.status = 'COMPLETE';
        IF NOT FOUND
           OR v_existing_chunk.payload_json->>'cursor_hash'
                IS DISTINCT FROM private.pay_payment_correction_sha256_v1(COALESCE(p_cursor_json, '{}'::jsonb))
           OR v_existing_chunk.result_json->>'prior_page_hash' IS DISTINCT FROM v_prior_page_hash
           OR COALESCE((v_existing_chunk.result_json->>'complete')::boolean, false) IS NOT TRUE THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAGE_DIGEST_MISMATCH', 'page_sequence_no', v_page_sequence_no
            )::text;
        END IF;
        RETURN v_existing_chunk.result_json || pg_catalog.jsonb_build_object(
          'ok', true, 'correction_request_id', v_request.id,
          'operation_id', v_operation.id, 'phase', 'AWAITING_REAUTHENTICATION',
          'selection_hash', v_request.selection_hash, 'complete', true,
          'replayed', true, 'code', 'SELECTION_READY'
        );
    END IF;

    IF COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NULL
       OR COALESCE(v_operation.lease_owner, v_operation.locked_by) IS DISTINCT FROM p_worker_id
       OR COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) IS NULL
       OR COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc)
            <= pg_catalog.clock_timestamp() THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_LEASE_MISMATCH'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'LEASE_MISMATCH', 'operation_id', p_operation_id
            )::text;
    END IF;

    SELECT settings_row.*
    INTO v_settings
    FROM public.settings_defaults AS settings_row
    ORDER BY settings_row.id
    LIMIT 1;

    v_max_candidates := LEAST(
        COALESCE(v_settings.banking_pay_correction_max_candidates, 10000),
        10000
    );
    v_max_active_items := LEAST(
        COALESCE(v_settings.banking_pay_correction_max_active_items, 250000),
        250000
    );
    v_max_items_per_candidate := LEAST(
        COALESCE(v_settings.banking_pay_correction_max_active_items_per_candidate, 128),
        128
    );
    v_max_source_rows_per_candidate := LEAST(
        COALESCE(v_settings.banking_pay_correction_max_source_rows_per_candidate, 512),
        512
    );

    v_selection := COALESCE(v_request.selection_json, '{}'::jsonb);
    IF pg_catalog.jsonb_typeof(v_selection) <> 'object'
       OR pg_catalog.octet_length(v_selection::text) > 524288 THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_DESCRIPTOR_INVALID'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'DESCRIPTOR_INVALID'
                  )::text;
    END IF;

    v_mode := pg_catalog.upper(COALESCE(
        v_selection #>> '{selection,mode}',
        v_selection ->> 'mode',
        ''
    ));
    v_requested_action := pg_catalog.upper(COALESCE(
        v_selection ->> 'requested_action',
        v_selection ->> 'action',
        v_selection #>> '{selection,action}',
        ''
    ));
    v_snapshot_token := COALESCE(
        v_selection #>> '{selection,snapshot_token}',
        v_selection ->> 'snapshot_token'
    );
    v_filter := COALESCE(
        v_selection -> 'filter_json',
        v_selection #> '{selection,filter_json}',
        '{}'::jsonb
    );

    IF v_mode NOT IN ('EXPLICIT', 'ALL_MATCHING')
       OR v_requested_action NOT IN (
           'DRAFT_CANCEL',
           'PRE_BANK_CANCEL',
           'CANCEL_PAYMENT',
           'NO_MONEY_RELEASE',
           'NO_MONEY_UNWIND'
       )
       OR pg_catalog.jsonb_typeof(v_filter) <> 'object' THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_DESCRIPTOR_INVALID'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'DESCRIPTOR_INVALID'
                  )::text;
    END IF;

    -- Constant-size scope fence.  Exact candidate/item identity is frozen by
    -- bounded pages and never materialised here as a whole-batch JSON value.
    v_active_batch_scope_hash := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
            'version', 2,
            'pay_batch_id', v_batch.id,
            'batch_status', v_batch.status,
            'total_bank_out_pence', pg_catalog.round(COALESCE(v_batch.total_bank_out, 0) * 100)::bigint,
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
                    'auth_request_id', auth_row.id, 'state', auth_row.state,
                    'required_quantity', auth_row.required_quantity,
                    'schedule_kind', auth_row.schedule_kind,
                    'scheduled_at_utc', auth_row.scheduled_at_utc,
                    'execution_intent_json', auth_row.execution_intent_json
                )
                FROM public.pay_batch_auth_requests AS auth_row
                WHERE auth_row.pay_batch_id = v_batch.id
                  AND auth_row.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
                ORDER BY auth_row.created_at_utc DESC, auth_row.id DESC LIMIT 1
            ),
            'change_signal', (
                SELECT pg_catalog.jsonb_build_object(
                    'version', COALESCE(signal_row.version, 0),
                    'payment_status_version', COALESCE(signal_row.payment_status_version, 0),
                    'correction_progress_version', COALESCE(signal_row.correction_progress_version, 0),
                    'overview_version', COALESCE(signal_row.overview_version, 0),
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
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'filter', v_filter,
            'sort_key', pg_catalog.upper(COALESCE(
                v_selection #>> '{selection,sort_key}',
                v_selection ->> 'sort_key',
                'STATUS'
            )),
            'sort_direction', pg_catalog.upper(COALESCE(
                v_selection #>> '{selection,sort_direction}',
                v_selection ->> 'sort_direction',
                'ASC'
            ))
        )
    );

    IF v_snapshot_token IS NULL
       OR v_snapshot_token IS DISTINCT FROM v_expected_snapshot_token
       OR (
           v_request.plan_json ->> 'active_batch_scope_hash' IS NOT NULL
           AND v_request.plan_json ->> 'active_batch_scope_hash'
               IS DISTINCT FROM v_active_batch_scope_hash
       ) THEN
        UPDATE public.pay_payment_correction_requests AS stale_request
        SET status = 'CANCELLED',
            cancelled_at_utc = pg_catalog.clock_timestamp(),
            reauth_proof_hash = NULL,
            reauth_expires_at_utc = NULL,
            reauth_consumed_at_utc = NULL,
            updated_at_utc = pg_catalog.clock_timestamp(),
            plan_json = COALESCE(stale_request.plan_json, '{}'::jsonb)
                || pg_catalog.jsonb_build_object(
                    'stale_code', 'SELECTION_STALE',
                    'observed_active_batch_scope_hash', v_active_batch_scope_hash
                )
        WHERE stale_request.id = v_request.id;

        UPDATE public.banking_pay_operations AS stale_operation
        SET status = 'CANCELLED',
            phase = 'COMPLETE',
            runner_state = 'CANCELLED',
            requires_user_action = false,
            result_json = COALESCE(stale_operation.result_json, '{}'::jsonb)
                || pg_catalog.jsonb_build_object(
                    'code', 'SELECTION_STALE',
                    'stale', true
                ),
            completed_at_utc = pg_catalog.clock_timestamp(),
            lease_owner = NULL,
            lease_expires_at_utc = NULL,
            locked_by = NULL,
            lock_expires_at_utc = NULL,
            run_after_utc = NULL,
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE stale_operation.id = v_operation.id;

        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'correction_request_id', v_request.id,
            'operation_id', v_operation.id,
            'phase', 'COMPLETE',
            'page_candidate_count', 0,
            'page_active_item_count', 0,
            'page_source_row_count', 0,
            'page_amount_pence', 0,
            'page_hash', NULL,
            'selection_hash', NULL,
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'next_cursor_json', NULL,
            'complete', true,
            'stale', true,
            'code', 'SELECTION_STALE',
            'message', 'Payment status changed. Refresh and select the payments again.'
        );
    END IF;

    v_page_sequence_no := COALESCE((p_cursor_json->>'page_sequence_no')::integer, 1);
    v_cursor_candidate_id := NULLIF(p_cursor_json->>'last_scanned_candidate_id', '')::uuid;
    v_prior_page_hash := COALESCE(
        p_cursor_json->>'prior_page_hash',
        private.pay_payment_correction_sha256_v1(pg_catalog.jsonb_build_object(
            'version', 1, 'correction_request_id', v_request.id, 'chain', 'PREPARE_SELECTION'
        ))
    );

    IF p_cursor_json IS NOT NULL
       AND p_cursor_json->>'scope_fence' IS DISTINCT FROM v_active_batch_scope_hash THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_SNAPSHOT_STALE'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SELECTION_STALE')::text;
    END IF;

    SELECT chunk_row.* INTO v_existing_chunk
    FROM public.banking_pay_operation_chunks AS chunk_row
    WHERE chunk_row.operation_id = v_operation.id
      AND chunk_row.phase = 'PREPARE_SELECTION'
      AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
      AND chunk_row.sequence_no = v_page_sequence_no;

    IF FOUND THEN
        IF v_existing_chunk.payload_json->>'cursor_hash'
             IS DISTINCT FROM private.pay_payment_correction_sha256_v1(COALESCE(p_cursor_json, '{}'::jsonb))
           OR v_existing_chunk.payload_json->>'scope_fence' IS DISTINCT FROM v_active_batch_scope_hash
           OR v_existing_chunk.result_json->>'prior_page_hash' IS DISTINCT FROM v_prior_page_hash THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
              USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'PAGE_DIGEST_MISMATCH', 'page_sequence_no', v_page_sequence_no
              )::text;
        END IF;
        RETURN v_existing_chunk.result_json || pg_catalog.jsonb_build_object(
          'ok', true, 'correction_request_id', v_request.id, 'operation_id', v_operation.id,
          'replayed', true, 'code', CASE WHEN COALESCE((v_existing_chunk.result_json->>'complete')::boolean, false)
            THEN 'SELECTION_READY' ELSE 'SELECTION_PAGE_PREPARED' END
        );
    END IF;

    IF v_page_sequence_no > 1 AND NOT EXISTS (
        SELECT 1 FROM public.banking_pay_operation_chunks AS prior_chunk
        WHERE prior_chunk.operation_id = v_operation.id
          AND prior_chunk.phase = 'PREPARE_SELECTION'
          AND prior_chunk.chunk_type = 'CANDIDATE_SCOPE'
          AND prior_chunk.sequence_no = v_page_sequence_no - 1
          AND prior_chunk.status = 'COMPLETE'
          AND prior_chunk.result_json->>'page_hash' = v_prior_page_hash
          AND prior_chunk.result_json->>'last_scanned_candidate_id' = v_cursor_candidate_id::text
    ) THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'PAGE_DIGEST_MISMATCH', 'page_sequence_no', v_page_sequence_no
          )::text;
    END IF;

    SELECT COALESCE(pg_catalog.sum((prior_chunk.result_json->>'page_candidate_count')::integer), 0)::bigint,
           COALESCE((pg_catalog.array_agg(prior_chunk.result_json->>'selected_chain_hash'
                    ORDER BY prior_chunk.sequence_no DESC))[1],
                    private.pay_payment_correction_sha256_v1(pg_catalog.jsonb_build_object(
                      'version', 1, 'correction_request_id', v_request.id, 'chain', 'SELECTED'
                    ))),
           COALESCE((pg_catalog.array_agg(prior_chunk.result_json->>'unselected_chain_hash'
                    ORDER BY prior_chunk.sequence_no DESC))[1],
                    private.pay_payment_correction_sha256_v1(pg_catalog.jsonb_build_object(
                      'version', 1, 'correction_request_id', v_request.id, 'chain', 'UNSELECTED'
                    ))),
           COALESCE((pg_catalog.array_agg(prior_chunk.result_json->>'explicit_id_chain_hash'
                    ORDER BY prior_chunk.sequence_no DESC))[1],
                    private.pay_payment_correction_sha256_v1(pg_catalog.jsonb_build_object(
                      'version', 1, 'pay_batch_id', v_batch.id, 'chain', 'EXPLICIT_IDS'
                    )))
    INTO v_start_ordinal, v_selected_chain_hash, v_unselected_chain_hash,
         v_explicit_id_chain_hash
    FROM public.banking_pay_operation_chunks AS prior_chunk
    WHERE prior_chunk.operation_id = v_operation.id
      AND prior_chunk.phase = 'PREPARE_SELECTION'
      AND prior_chunk.chunk_type = 'CANDIDATE_SCOPE'
      AND prior_chunk.status = 'COMPLETE'
      AND prior_chunk.sequence_no < v_page_sequence_no;

    SELECT COALESCE(pg_catalog.jsonb_agg(scan_row.id::text ORDER BY scan_row.id), '[]'::jsonb),
           pg_catalog.min(scan_row.id), pg_catalog.max(scan_row.id), pg_catalog.count(*)::integer
    INTO v_scan_candidate_tokens, v_first_scanned_candidate_id,
         v_scan_last_candidate_id, v_scan_candidate_count
    FROM (
        SELECT candidate_row.id
        FROM public.pay_batch_candidates AS candidate_row
        WHERE candidate_row.pay_batch_id = v_batch.id
          AND (v_cursor_candidate_id IS NULL OR candidate_row.id > v_cursor_candidate_id)
          AND (v_mode = 'ALL_MATCHING' OR candidate_row.id::text IN (
            SELECT explicit_token.value
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
          ))
        ORDER BY candidate_row.id
        LIMIT p_limit
    ) AS scan_row;
    v_scanned_candidate_count := v_scan_candidate_count;

    FOR v_candidate IN
        SELECT candidate_row.id AS pay_batch_candidate_id,
               candidate_row.candidate_id,
               candidate_row.candidate_display_name,
               candidate_row.net_bank_amount,
               candidate_row.settlement_status,
               (
                 SELECT work_row.status
                 FROM public.pay_payment_correction_work_items AS work_row
                 WHERE work_row.pay_batch_id = v_batch.id
                   AND work_row.pay_batch_candidate_id = candidate_row.id
                 ORDER BY work_row.created_at_utc DESC, work_row.id DESC
                 LIMIT 1
               ) AS latest_work_status
        FROM public.pay_batch_candidates AS candidate_row
        WHERE candidate_row.pay_batch_id = v_batch.id
          AND candidate_row.id::text IN (
            SELECT scan_token.value
            FROM pg_catalog.jsonb_array_elements_text(v_scan_candidate_tokens) AS scan_token(value)
          )
        ORDER BY candidate_row.id
    LOOP
        v_last_candidate_id := v_candidate.pay_batch_candidate_id;
        SELECT
            pg_catalog.array_agg(item_row.id ORDER BY item_row.id),
            pg_catalog.count(*)::integer,
            COALESCE(pg_catalog.sum(item_row.amount_inc_vat), 0)::numeric(14,2),
            private.pay_payment_correction_sha256_v1(
                pg_catalog.jsonb_build_object(
                    'transfer_ids',
                    COALESCE(
                        pg_catalog.jsonb_agg(
                            DISTINCT item_row.pay_bank_transfer_id
                        ) FILTER (WHERE item_row.pay_bank_transfer_id IS NOT NULL),
                        '[]'::jsonb
                    )
                )
            )
        INTO v_item_ids, v_item_count, v_amount, v_shared_instruction_scope_hash
        FROM public.pay_batch_items AS item_row
        WHERE item_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
          AND COALESCE(item_row.is_voided, false) IS NOT TRUE;

        -- The reviewed payment amount is the frozen candidate bank payment,
        -- not a gross sum of PAYE source items. The item identities and their
        -- frozen values remain independently bound into candidate_scope_hash.
        v_amount := pg_catalog.round(
            GREATEST(COALESCE(v_candidate.net_bank_amount, 0), 0),
            2
        )::numeric(14,2);

        IF v_item_count < 1 AND v_mode = 'EXPLICIT' THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_EMPTY_CANDIDATE'
                USING ERRCODE = 'P0001',
                      DETAIL = pg_catalog.jsonb_build_object(
                          'code', 'SELECTION_STALE',
                          'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id
                      )::text;
        END IF;

        IF v_item_count < 1 THEN
            v_candidate_audit_hash := private.pay_payment_correction_sha256_v1(
              pg_catalog.jsonb_build_object(
                'version', 1, 'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
                'candidate_id', v_candidate.candidate_id,
                'net_bank_amount_pence', pg_catalog.round(COALESCE(v_candidate.net_bank_amount, 0) * 100)::bigint,
                'settlement_status', v_candidate.settlement_status, 'pay_batch_item_ids', '[]'::jsonb
              )
            );
            v_unselected_chain_hash := private.pay_payment_correction_sha256_v1(
              pg_catalog.jsonb_build_object('prior', v_unselected_chain_hash,
                                             'candidate_hash', v_candidate_audit_hash)
            );
            CONTINUE;
        END IF;

        v_candidate_selection_json := pg_catalog.jsonb_build_object(
            'scope_type', 'CANDIDATES',
            'work_unit', 'CANDIDATE',
            'pay_batch_candidate_ids', pg_catalog.jsonb_build_array(v_candidate.pay_batch_candidate_id),
            'pay_batch_item_ids', pg_catalog.to_jsonb(v_item_ids)
        );

        v_candidate_selected_scope_json := pg_catalog.jsonb_build_object(
            'scope_type', 'CANDIDATES',
            'work_unit', 'CANDIDATE',
            'pay_batch_ids', pg_catalog.jsonb_build_array(v_batch.id),
            'pay_batch_candidate_ids', pg_catalog.jsonb_build_array(v_candidate.pay_batch_candidate_id),
            'candidate_ids', pg_catalog.jsonb_build_array(v_candidate.candidate_id),
            'pay_batch_item_ids', pg_catalog.to_jsonb(v_item_ids),
            'umbrella_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT item_scope.umbrella_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.umbrella_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'finance_case_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT item_scope.finance_case_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.finance_case_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'finance_component_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT item_scope.finance_component_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.finance_component_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'reservation_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT item_scope.reservation_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.reservation_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'pay_bank_transfer_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT item_scope.pay_bank_transfer_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.pay_bank_transfer_id IS NOT NULL
                    UNION
                    SELECT DISTINCT advance_scope.payout_transfer_id::text AS value_text
                    FROM public.pay_advances AS advance_scope
                    JOIN public.pay_batch_items AS item_scope ON item_scope.finance_case_id = advance_scope.id
                    WHERE item_scope.id = ANY(v_item_ids) AND advance_scope.payout_transfer_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'payout_transfer_ids', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT advance_scope.payout_transfer_id::text AS value_text
                    FROM public.pay_advances AS advance_scope
                    JOIN public.pay_batch_items AS item_scope ON item_scope.finance_case_id = advance_scope.id
                    WHERE item_scope.id = ANY(v_item_ids) AND advance_scope.payout_transfer_id IS NOT NULL
                    UNION
                    SELECT DISTINCT item_scope.pay_bank_transfer_id::text AS value_text
                    FROM public.pay_batch_items AS item_scope
                    WHERE item_scope.id = ANY(v_item_ids) AND item_scope.pay_bank_transfer_id IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb),
            'transfer_group_keys', COALESCE((
                SELECT pg_catalog.jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
                FROM (
                    SELECT DISTINCT transfer_scope.transfer_group_key AS value_text
                    FROM public.pay_bank_transfers AS transfer_scope
                    WHERE transfer_scope.id IN (
                        SELECT item_scope.pay_bank_transfer_id
                        FROM public.pay_batch_items AS item_scope
                        WHERE item_scope.id = ANY(v_item_ids) AND item_scope.pay_bank_transfer_id IS NOT NULL
                    )
                      AND NULLIF(pg_catalog.btrim(COALESCE(transfer_scope.transfer_group_key, '')), '') IS NOT NULL
                ) AS scope_value
            ), '[]'::jsonb)
        );

        WITH selected_items AS (
            SELECT item_scope.*
            FROM public.pay_batch_items AS item_scope
            WHERE item_scope.id = ANY(v_item_ids)
        ), transfer_ids AS (
            SELECT selected_item.pay_bank_transfer_id AS id
            FROM selected_items AS selected_item
            WHERE selected_item.pay_bank_transfer_id IS NOT NULL
            UNION
            SELECT advance_scope.payout_transfer_id
            FROM public.pay_advances AS advance_scope
            JOIN selected_items AS selected_item ON selected_item.finance_case_id = advance_scope.id
            WHERE advance_scope.payout_transfer_id IS NOT NULL
        ), transfer_groups AS (
            SELECT DISTINCT transfer_scope.transfer_group_key
            FROM public.pay_bank_transfers AS transfer_scope
            WHERE transfer_scope.id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id)
              AND NULLIF(pg_catalog.btrim(COALESCE(transfer_scope.transfer_group_key, '')), '') IS NOT NULL
        ), financial_scope_items AS (
            SELECT DISTINCT item_scope.id
            FROM public.pay_batch_items AS item_scope
            LEFT JOIN public.pay_bank_transfers AS item_transfer
              ON item_transfer.id = item_scope.pay_bank_transfer_id
            WHERE item_scope.id = ANY(v_item_ids)
               OR item_scope.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id)
               OR item_transfer.transfer_group_key IN (
                    SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group
               )
        ), source_counts AS (
            SELECT 1::bigint AS row_count
            UNION ALL SELECT pg_catalog.count(*) FROM financial_scope_items
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_batch_item_breakdowns AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_batch_timesheet_snapshots AS source_row WHERE source_row.pay_batch_id = v_batch.id AND source_row.candidate_id = v_candidate.candidate_id
            UNION ALL SELECT pg_catalog.count(*) FROM public.timesheet_pay_state_history AS source_row WHERE source_row.pay_batch_id = v_batch.id AND source_row.timesheet_id IN (SELECT selected_item.timesheet_id FROM selected_items AS selected_item WHERE selected_item.timesheet_id IS NOT NULL)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_advance_reservations AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_finance_case_components AS source_row WHERE source_row.id IN (SELECT selected_item.finance_component_id FROM selected_items AS selected_item WHERE selected_item.finance_component_id IS NOT NULL)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_manual_adjustment_carry_forwards AS source_row WHERE source_row.source_pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item) OR source_row.target_pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_advances AS source_row WHERE source_row.id IN (SELECT selected_item.finance_case_id FROM selected_items AS selected_item WHERE selected_item.finance_case_id IS NOT NULL)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_bank_transfers AS source_row WHERE source_row.id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.transfer_group_key IN (SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events AS source_row WHERE source_row.pay_batch_id = v_batch.id AND (source_row.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.candidate_id = v_candidate.candidate_id)
            UNION ALL SELECT pg_catalog.count(*) FROM public.banking_pay_operation_transfer_scope AS source_row WHERE source_row.pay_batch_id = v_batch.id AND (source_row.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.transfer_group_key IN (SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group))
            UNION ALL SELECT pg_catalog.count(*) FROM public.banking_pay_operation_transfer_scope_items AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
            UNION ALL SELECT pg_catalog.count(*) FROM public.pay_batch_paye_net_inputs AS source_row WHERE source_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
            UNION ALL SELECT pg_catalog.count(*)
              FROM public.mail_outbox AS source_row
              CROSS JOIN LATERAL (
                  SELECT public._pay_payment_correction_mail_scope_match(
                      source_row.id, v_batch.id, v_candidate_selection_json,
                      v_candidate_selected_scope_json, false
                  ) AS match_result
              ) AS mail_match
              WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status::text, ''))) = 'QUEUED'
                AND pg_catalog.lower(pg_catalog.concat_ws('|', source_row.type, source_row.email_type, source_row.context_kind, source_row.reference, COALESCE(source_row.payment_scope_json::text, '{}'))) LIKE ANY (ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%'])
                AND COALESCE(NULLIF(mail_match.match_result->>'matched', '')::boolean, false)
        )
        SELECT COALESCE(pg_catalog.sum(source_count.row_count), 0)::integer
        INTO v_source_row_count
        FROM source_counts AS source_count;

        IF v_item_count > v_max_items_per_candidate
           OR v_source_row_count > v_max_source_rows_per_candidate THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_CANDIDATE_SCOPE_TOO_LARGE'
                USING ERRCODE = 'P0001',
                      DETAIL = pg_catalog.jsonb_build_object(
                          'code', 'CANDIDATE_SCOPE_TOO_LARGE',
                          'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
                          'active_item_count', v_item_count,
                          'source_row_count', v_source_row_count
                      )::text;
        END IF;

        v_diagnostic := public.pay_payment_cancelability_diagnostic(
            v_batch.id,
            v_candidate_selection_json || pg_catalog.jsonb_build_object(
                'requested_action', v_requested_action
            ),
            v_actor_user_id,
            'PAYMENT_CORRECTION_PLAN'
        );

        v_eligibility_code := COALESCE(
            v_diagnostic ->> 'payment_lifecycle_state',
            'UNKNOWN'
        );
        v_action_allowed := CASE
            WHEN v_requested_action = 'DRAFT_CANCEL' THEN
                v_batch.status = 'DRAFT'
                AND COALESCE(
                    (v_diagnostic ->> 'can_pre_provider_cancel')::boolean,
                    false
                )
                AND v_candidate.latest_work_status IS DISTINCT FROM 'BLOCKED'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_FINAL'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_RETRYABLE'
            WHEN v_requested_action IN ('PRE_BANK_CANCEL', 'CANCEL_PAYMENT') THEN
                COALESCE(
                    (v_diagnostic ->> 'can_pre_provider_cancel')::boolean,
                    false
                )
                AND v_candidate.latest_work_status IS DISTINCT FROM 'BLOCKED'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_FINAL'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_RETRYABLE'
            WHEN v_requested_action IN ('NO_MONEY_RELEASE', 'NO_MONEY_UNWIND') THEN
                COALESCE(
                    (v_diagnostic ->> 'can_no_money_unwind')::boolean,
                    false
                )
                AND v_candidate.latest_work_status IS DISTINCT FROM 'BLOCKED'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_FINAL'
                AND v_candidate.latest_work_status IS DISTINCT FROM 'FAILED_RETRYABLE'
            ELSE false
        END;

        -- Current Payment Status and immutable preparation must interpret the
        -- reviewed status filter identically.  Provider/settlement states that
        -- are not actionable have already made v_action_allowed false.  For the
        -- remaining page-local candidate, preserve the Current Payment Status
        -- precedence for prior correction outcomes before no-money eligibility.
        v_effective_display_state := CASE
            WHEN v_candidate.latest_work_status = 'BLOCKED' THEN 'BLOCKED'
            WHEN v_candidate.latest_work_status IN ('FAILED_FINAL', 'FAILED_RETRYABLE') THEN 'FAILED'
            WHEN COALESCE((v_diagnostic ->> 'can_no_money_unwind')::boolean, false) THEN 'NOT_PAID'
            ELSE 'ACTIVE'
        END;

        IF v_action_allowed IS NOT TRUE AND v_mode = 'EXPLICIT' THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_ACTION_NOT_COMMON'
                USING ERRCODE = 'P0001',
                      DETAIL = pg_catalog.jsonb_build_object(
                          'code', 'ACTION_NOT_COMMON',
                          'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
                          'eligibility_code', v_eligibility_code
                      )::text;
        END IF;

        v_candidate_scope_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
                'version', 1,
                'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
                'candidate_id', v_candidate.candidate_id,
                'requested_action', v_requested_action,
                'pay_batch_item_ids', pg_catalog.to_jsonb(v_item_ids),
                'item_count', v_item_count,
                'source_row_count', v_source_row_count,
                'active_amount_pence', pg_catalog.round(v_amount * 100)::bigint,
                'item_contract', (
                    SELECT pg_catalog.jsonb_agg(
                        pg_catalog.jsonb_build_object(
                            'id', item_row.id,
                            'item_type', item_row.item_type,
                            'amount_ex_vat_pence', pg_catalog.round(
                                COALESCE(item_row.amount_ex_vat, 0) * 100
                            )::bigint,
                            'amount_vat_pence', pg_catalog.round(
                                COALESCE(item_row.amount_vat, 0) * 100
                            )::bigint,
                            'amount_inc_vat_pence', pg_catalog.round(
                                COALESCE(item_row.amount_inc_vat, 0) * 100
                            )::bigint,
                            'reservation_id', item_row.reservation_id,
                            'finance_component_id', item_row.finance_component_id,
                            'pay_bank_transfer_id', item_row.pay_bank_transfer_id,
                            'operation_source_key', item_row.operation_source_key,
                            'frozen_component_snapshot_json', item_row.frozen_component_snapshot_json,
                            'frozen_source_basis_json', item_row.frozen_source_basis_json
                        )
                        ORDER BY item_row.id
                    )
                    FROM public.pay_batch_items AS item_row
                    WHERE item_row.id = ANY(v_item_ids)
                ),
                'shared_instruction_scope_hash', v_shared_instruction_scope_hash,
                'eligibility_code', v_eligibility_code
            )
        );

        -- Boundary-independent audit chain.  Each candidate contributes one
        -- digest, so replay/finalisation may use different bounded page sizes
        -- without reconstructing a whole-request JSON value.
        v_candidate_audit_hash := private.pay_payment_correction_sha256_v1(
          pg_catalog.jsonb_build_object(
            'version', 1,
            'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
            'candidate_id', v_candidate.candidate_id,
            'net_bank_amount_pence', pg_catalog.round(COALESCE(v_candidate.net_bank_amount, 0) * 100)::bigint,
            'settlement_status', v_candidate.settlement_status,
            'item_state', COALESCE((
              SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                item_row.id, item_row.item_type, COALESCE(item_row.is_voided, false),
                pg_catalog.round(COALESCE(item_row.amount_ex_vat, 0) * 100)::bigint,
                pg_catalog.round(COALESCE(item_row.amount_vat, 0) * 100)::bigint,
                pg_catalog.round(COALESCE(item_row.amount_inc_vat, 0) * 100)::bigint,
                item_row.reservation_id, item_row.finance_component_id,
                item_row.pay_bank_transfer_id, item_row.operation_source_key
              ) ORDER BY item_row.id)
              FROM public.pay_batch_items AS item_row
              WHERE item_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
            ), '[]'::jsonb),
            'reservation_state', COALESCE((
              SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                reservation_row.id, reservation_row.pay_batch_item_id,
                reservation_row.status, reservation_row.committed_at_utc,
                reservation_row.settled_at_utc, reservation_row.released_at_utc
              ) ORDER BY reservation_row.id)
              FROM public.pay_advance_reservations AS reservation_row
              WHERE reservation_row.pay_batch_item_id = ANY(v_item_ids)
            ), '[]'::jsonb),
            'transfer_state', COALESCE((
              SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                transfer_row.id, transfer_row.status, transfer_row.rail_state,
                transfer_row.request_id, transfer_row.rail_tx_id,
                transfer_row.transfer_group_key
              ) ORDER BY transfer_row.id)
              FROM public.pay_bank_transfers AS transfer_row
              WHERE transfer_row.id IN (
                SELECT item_row.pay_bank_transfer_id
                FROM public.pay_batch_items AS item_row
                WHERE item_row.id = ANY(v_item_ids)
                  AND item_row.pay_bank_transfer_id IS NOT NULL
              )
            ), '[]'::jsonb)
          )
        );

        IF v_mode = 'ALL_MATCHING' AND (
             v_action_allowed IS NOT TRUE
             OR EXISTS (
               SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
                 COALESCE(v_selection#>'{selection,exclusions}', v_selection->'exclusions', '[]'::jsonb)
               ) AS excluded_token(value)
               WHERE excluded_token.value = v_candidate.pay_batch_candidate_id::text
             )
             OR (
               NULLIF(pg_catalog.btrim(v_filter->>'search'), '') IS NOT NULL
               AND pg_catalog.lower(v_candidate.candidate_display_name)
                   NOT LIKE '%' || pg_catalog.lower(pg_catalog.btrim(v_filter->>'search')) || '%'
             )
             OR (
               NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter->>'pay_channel')), '') = 'PAYE'
               AND NOT EXISTS (
                 SELECT 1 FROM public.pay_batch_items AS channel_item
                 WHERE channel_item.id = ANY(v_item_ids)
                   AND pg_catalog.upper(COALESCE(channel_item.pay_channel, '')) = 'PAYE'
               )
             )
              OR (
                NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter->>'status')), '') IS NOT NULL
                AND pg_catalog.upper(pg_catalog.btrim(v_filter->>'status'))
                    IS DISTINCT FROM v_effective_display_state
              )
           ) THEN
            v_unselected_chain_hash := private.pay_payment_correction_sha256_v1(
              pg_catalog.jsonb_build_object('prior', v_unselected_chain_hash,
                                             'candidate_hash', v_candidate_audit_hash)
            );
            CONTINUE;
        END IF;

        v_selected_chain_hash := private.pay_payment_correction_sha256_v1(
          pg_catalog.jsonb_build_object('prior', v_selected_chain_hash,
                                         'candidate_hash', v_candidate_scope_hash)
        );
        IF v_mode = 'EXPLICIT' THEN
          v_explicit_id_chain_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
              'prior', v_explicit_id_chain_hash,
              'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id::text
            )
          );
        END IF;

        INSERT INTO public.pay_payment_correction_request_candidates (
            correction_request_id,
            selection_ordinal,
            pay_batch_candidate_id,
            candidate_scope_hash,
            active_item_count,
            source_row_count,
            active_amount,
            pay_batch_item_ids,
            shared_instruction_scope_hash,
            eligibility_code_at_plan,
            created_at_utc
        )
        VALUES (
            v_request.id,
            v_start_ordinal + v_page_candidate_count + 1,
            v_candidate.pay_batch_candidate_id,
            v_candidate_scope_hash,
            v_item_count,
            v_source_row_count,
            v_amount,
            v_item_ids,
            v_shared_instruction_scope_hash,
            v_eligibility_code,
            pg_catalog.clock_timestamp()
        )
        ON CONFLICT (correction_request_id, pay_batch_candidate_id)
        DO NOTHING;

        IF NOT FOUND THEN
            IF NOT EXISTS (
                SELECT 1
                FROM public.pay_payment_correction_request_candidates AS existing_member
                WHERE existing_member.correction_request_id = v_request.id
                  AND existing_member.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
                  AND existing_member.candidate_scope_hash = v_candidate_scope_hash
                  AND existing_member.active_item_count = v_item_count
                  AND existing_member.source_row_count = v_source_row_count
                  AND existing_member.active_amount = v_amount
                  AND existing_member.pay_batch_item_ids = v_item_ids
            ) THEN
                RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
                    USING ERRCODE = 'P0001',
                          DETAIL = pg_catalog.jsonb_build_object(
                              'code', 'PAGE_DIGEST_MISMATCH',
                              'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id
                          )::text;
            END IF;
        ELSE
            v_page_candidate_count := v_page_candidate_count + 1;
            v_page_active_item_count := v_page_active_item_count + v_item_count;
            v_page_source_row_count := v_page_source_row_count + v_source_row_count;
            v_page_amount_pence := v_page_amount_pence
                + pg_catalog.round(v_amount * 100)::bigint;
        END IF;

        v_last_candidate_id := v_candidate.pay_batch_candidate_id;
    END LOOP;

    IF v_mode = 'ALL_MATCHING' THEN
        -- The durable cursor advances over the bounded identity scan, including
        -- filtered or excluded candidates. Membership still contains only rows
        -- returned by the canonical status/action classifier.
        v_last_candidate_id := COALESCE(
            v_scan_last_candidate_id,
            v_cursor_candidate_id
            );
        END IF;

    v_page_hash := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
            'version', 1,
            'correction_request_id', v_request.id,
            'operation_id', v_operation.id,
            'page_sequence_no', v_page_sequence_no,
            'prior_page_hash', v_prior_page_hash,
            'scope_fence', v_active_batch_scope_hash,
            'first_scanned_candidate_id', v_first_scanned_candidate_id,
            'last_scanned_candidate_id', v_scan_last_candidate_id,
            'scanned_candidate_count', v_scanned_candidate_count,
            'selected_chain_hash', v_selected_chain_hash,
            'unselected_chain_hash', v_unselected_chain_hash,
            'explicit_id_chain_hash', v_explicit_id_chain_hash,
            'page_members', (
                SELECT pg_catalog.jsonb_agg(
                    pg_catalog.jsonb_build_array(
                        member_row.selection_ordinal,
                        member_row.pay_batch_candidate_id,
                        member_row.candidate_scope_hash,
                        member_row.active_item_count,
                        member_row.source_row_count,
                        pg_catalog.round(member_row.active_amount * 100)::bigint,
                        member_row.shared_instruction_scope_hash,
                        member_row.eligibility_code_at_plan
                    )
                    ORDER BY member_row.selection_ordinal
                )
                FROM public.pay_payment_correction_request_candidates AS member_row
                WHERE member_row.correction_request_id = v_request.id
                  AND member_row.selection_ordinal > v_start_ordinal
            )
        )
    );
    v_sequence_no := v_page_sequence_no;

    INSERT INTO public.banking_pay_operation_chunks (
        operation_id,
        phase,
        chunk_type,
        sequence_no,
        status,
        payload_json,
        result_json,
        error_json,
        unit_count,
        completed_count,
        failed_count,
        locked_by,
        lock_expires_at_utc,
        started_at_utc,
        completed_at_utc
    )
    VALUES (
        v_operation.id,
        'PREPARE_SELECTION',
        'CANDIDATE_SCOPE',
        v_sequence_no,
        'COMPLETE',
        pg_catalog.jsonb_build_object(
            'cursor_json', p_cursor_json,
            'cursor_hash', private.pay_payment_correction_sha256_v1(COALESCE(p_cursor_json, '{}'::jsonb)),
            'scope_fence', v_active_batch_scope_hash,
            'prior_page_hash', v_prior_page_hash,
            'limit', p_limit
        ),
        pg_catalog.jsonb_build_object(
            'page_sequence_no', v_page_sequence_no,
            'prior_page_hash', v_prior_page_hash,
            'first_scanned_candidate_id', v_first_scanned_candidate_id,
            'last_scanned_candidate_id', v_scan_last_candidate_id,
            'page_candidate_count', v_page_candidate_count,
            'scanned_candidate_count', v_scanned_candidate_count,
            'page_active_item_count', v_page_active_item_count,
            'page_source_row_count', v_page_source_row_count,
            'page_amount_pence', v_page_amount_pence,
            'selected_chain_hash', v_selected_chain_hash,
            'unselected_chain_hash', v_unselected_chain_hash,
            'explicit_id_chain_hash', v_explicit_id_chain_hash,
            'page_hash', v_page_hash,
            'last_pay_batch_candidate_id', v_scan_last_candidate_id
        ),
        NULL,
        v_page_candidate_count,
        v_page_candidate_count,
        0,
        p_worker_id,
        NULL,
        pg_catalog.clock_timestamp(),
        pg_catalog.clock_timestamp()
    )
    ON CONFLICT (operation_id, phase, chunk_type, sequence_no) DO NOTHING;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAGE_DIGEST_MISMATCH', 'page_sequence_no', v_page_sequence_no
        )::text;
    END IF;

    IF v_mode = 'ALL_MATCHING' THEN
        SELECT EXISTS (
            SELECT 1
            FROM public.pay_batch_candidates AS remaining_candidate
            WHERE remaining_candidate.pay_batch_id = v_batch.id
              AND (
                  v_last_candidate_id IS NULL
                  OR remaining_candidate.id > v_last_candidate_id
              )
        )
        INTO v_has_more;
    ELSE
      SELECT EXISTS (
        SELECT 1 FROM public.pay_batch_candidates AS remaining_candidate
        WHERE remaining_candidate.pay_batch_id = v_batch.id
          AND (v_scan_last_candidate_id IS NULL OR remaining_candidate.id > v_scan_last_candidate_id)
          AND remaining_candidate.id::text IN (
            SELECT explicit_token.value
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
          )
      ) INTO v_has_more;
    END IF;

    v_complete := NOT v_has_more;
    v_next_cursor_json := CASE
        WHEN v_complete OR v_scan_last_candidate_id IS NULL THEN NULL
        ELSE pg_catalog.jsonb_build_object(
            'page_sequence_no', v_page_sequence_no + 1,
            'last_scanned_candidate_id', v_scan_last_candidate_id,
            'prior_page_hash', v_page_hash,
            'scope_fence', v_active_batch_scope_hash
        )
    END;

    UPDATE public.banking_pay_operation_chunks AS completed_page
    SET result_json = completed_page.result_json || pg_catalog.jsonb_build_object(
          'next_cursor_json', v_next_cursor_json,
          'complete', v_complete
        )
    WHERE completed_page.operation_id = v_operation.id
      AND completed_page.phase = 'PREPARE_SELECTION'
      AND completed_page.chunk_type = 'CANDIDATE_SCOPE'
      AND completed_page.sequence_no = v_page_sequence_no;

    -- Totals are reduced from at most 200 durable page summaries.  The final
    -- page does not scan or aggregate the complete membership identity set.
    SELECT
        COALESCE(pg_catalog.sum((page_chunk.result_json->>'page_candidate_count')::integer), 0)::integer,
        COALESCE(pg_catalog.sum((page_chunk.result_json->>'page_active_item_count')::integer), 0)::integer,
        COALESCE(pg_catalog.sum((page_chunk.result_json->>'page_source_row_count')::integer), 0)::integer,
        (COALESCE(pg_catalog.sum((page_chunk.result_json->>'page_amount_pence')::bigint), 0)::numeric / 100)::numeric(14,2)
    INTO
        v_total_candidate_count,
        v_total_active_item_count,
        v_total_source_row_count,
        v_total_amount
    FROM public.banking_pay_operation_chunks AS page_chunk
    WHERE page_chunk.operation_id = v_operation.id
      AND page_chunk.phase = 'PREPARE_SELECTION'
      AND page_chunk.chunk_type = 'CANDIDATE_SCOPE'
      AND page_chunk.status = 'COMPLETE';

    IF v_total_candidate_count > v_max_candidates
       OR v_total_active_item_count > v_max_active_items THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_CAPACITY_EXCEEDED'
            USING ERRCODE = 'P0001',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'SELECTION_CAPACITY_EXCEEDED',
                      'selected_candidate_count', v_total_candidate_count,
                      'selected_active_item_count', v_total_active_item_count
                  )::text;
    END IF;

    IF v_complete THEN
        IF v_total_candidate_count < 1 THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_EMPTY'
                USING ERRCODE = 'P0001',
                      DETAIL = pg_catalog.jsonb_build_object(
                          'code', 'DESCRIPTOR_INVALID'
                      )::text;
        END IF;

        -- The final reducer reads only the bounded page summaries.  It proves a
        -- contiguous page/hash chain and never rebuilds all item identities.
        WITH ordered_prepare_pages AS (
          SELECT chunk_row.sequence_no,
                 chunk_row.result_json,
                 pg_catalog.row_number() OVER (ORDER BY chunk_row.sequence_no)::integer
                   AS expected_sequence_no,
                 pg_catalog.lag(chunk_row.result_json->>'page_hash')
                   OVER (ORDER BY chunk_row.sequence_no) AS previous_page_hash
          FROM public.banking_pay_operation_chunks AS chunk_row
          WHERE chunk_row.operation_id = v_operation.id
            AND chunk_row.phase = 'PREPARE_SELECTION'
            AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
            AND chunk_row.status = 'COMPLETE'
        )
        SELECT pg_catalog.count(*) FILTER (
                 WHERE ordered_prepare_pages.sequence_no
                         <> ordered_prepare_pages.expected_sequence_no
                    OR (ordered_prepare_pages.sequence_no > 1
                        AND ordered_prepare_pages.result_json->>'prior_page_hash'
                          IS DISTINCT FROM ordered_prepare_pages.previous_page_hash)
               )::integer
        INTO v_page_chain_mismatch_count
        FROM ordered_prepare_pages;

        IF v_page_sequence_no > 200 OR v_page_chain_mismatch_count <> 0 OR (
          SELECT pg_catalog.count(*) FROM public.banking_pay_operation_chunks AS chunk_row
          WHERE chunk_row.operation_id = v_operation.id
            AND chunk_row.phase = 'PREPARE_SELECTION'
            AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
            AND chunk_row.status = 'COMPLETE'
        ) <> v_page_sequence_no THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_PAGE_DIGEST_MISMATCH'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAGE_DIGEST_MISMATCH', 'page_sequence_no', v_page_sequence_no
            )::text;
        END IF;

        v_unselected_scope_hash_before := v_unselected_chain_hash;

        IF v_mode = 'EXPLICIT' THEN
          v_requested_explicit_count := COALESCE((v_selection->>'requested_explicit_count')::integer, 0);
          v_requested_explicit_hash := v_selection->>'requested_explicit_hash';
          SELECT pg_catalog.count(*)::integer
          INTO v_actual_explicit_count
          FROM public.pay_payment_correction_request_candidates AS member_row
          WHERE member_row.correction_request_id = v_request.id;
          v_actual_explicit_hash := v_explicit_id_chain_hash;

          SELECT pg_catalog.count(*)::integer INTO v_explicit_missing_count
          FROM (
            SELECT explicit_token.value::uuid AS id
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
            EXCEPT
            SELECT member_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_request_candidates AS member_row
            WHERE member_row.correction_request_id = v_request.id
          ) AS missing_member;

          SELECT pg_catalog.count(*)::integer INTO v_explicit_extra_count
          FROM (
            SELECT member_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_request_candidates AS member_row
            WHERE member_row.correction_request_id = v_request.id
            EXCEPT
            SELECT explicit_token.value::uuid
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
          ) AS extra_member;

          IF v_requested_explicit_count <> v_actual_explicit_count
             OR v_requested_explicit_hash IS DISTINCT FROM v_actual_explicit_hash
             OR v_explicit_missing_count <> 0 OR v_explicit_extra_count <> 0 THEN
            UPDATE public.pay_payment_correction_requests AS invalid_request
            SET status = 'CANCELLED', cancelled_at_utc = pg_catalog.clock_timestamp(),
                plan_json = COALESCE(invalid_request.plan_json, '{}'::jsonb)
                  || pg_catalog.jsonb_build_object(
                    'stale_code', 'EXPLICIT_SELECTION_MISMATCH',
                    'requested_explicit_count', v_requested_explicit_count,
                    'materialised_explicit_count', v_actual_explicit_count
                  ),
                updated_at_utc = pg_catalog.clock_timestamp()
            WHERE invalid_request.id = v_request.id;
            UPDATE public.banking_pay_operations AS invalid_operation
            SET status = 'CANCELLED', phase = 'COMPLETE', runner_state = 'CANCELLED',
                requires_user_action = false, run_after_utc = NULL,
                result_json = COALESCE(invalid_operation.result_json, '{}'::jsonb)
                  || pg_catalog.jsonb_build_object('code', 'EXPLICIT_SELECTION_MISMATCH'),
                completed_at_utc = pg_catalog.clock_timestamp(),
                lease_owner = NULL, lease_expires_at_utc = NULL,
                locked_by = NULL, lock_expires_at_utc = NULL,
                updated_at_utc = pg_catalog.clock_timestamp()
            WHERE invalid_operation.id = v_operation.id;
            RETURN pg_catalog.jsonb_build_object(
              'ok', false, 'correction_request_id', v_request.id,
              'operation_id', v_operation.id, 'phase', 'COMPLETE',
              'complete', true, 'stale', true, 'code', 'EXPLICIT_SELECTION_MISMATCH',
              'message', 'The exact requested payments could not be frozen. Refresh and select them again.'
            );
          END IF;
        END IF;

        v_selection_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
                'version', 2, 'pay_batch_id', v_batch.id,
                'requested_action', v_requested_action,
                'active_batch_scope_hash', v_active_batch_scope_hash,
                'selected_candidate_count', v_total_candidate_count,
                'selected_active_item_count', v_total_active_item_count,
                'selected_source_row_count', v_total_source_row_count,
                'selected_amount_pence', pg_catalog.round(v_total_amount * 100)::bigint,
                'selected_chain_hash', v_selected_chain_hash,
                'unselected_chain_hash', v_unselected_chain_hash,
                'prepare_page_count', v_page_sequence_no
            )
        );

        v_plan_json := pg_catalog.jsonb_build_object(
            'contract_version', 2,
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'scope_fence_hash', v_active_batch_scope_hash,
            'requested_action', v_requested_action,
            'selected_candidate_count', v_total_candidate_count,
            'selected_active_item_count', v_total_active_item_count,
            'selected_source_row_count', v_total_source_row_count,
            'selected_amount_pence', pg_catalog.round(v_total_amount * 100)::bigint,
            'selection_hash', v_selection_hash,
            'selected_chain_hash', v_selected_chain_hash,
            'unselected_chain_hash', v_unselected_chain_hash,
            'prepare_page_count', v_page_sequence_no,
            'requested_explicit_count', v_requested_explicit_count,
            'requested_explicit_hash', v_requested_explicit_hash,
            'unselected_scope_hash_before', v_unselected_scope_hash_before,
            'selection_ready_at_utc', pg_catalog.clock_timestamp()
        );
        v_plan_hash := private.pay_payment_correction_sha256_v1(v_plan_json);

        UPDATE public.pay_payment_correction_requests AS ready_request
        SET status = 'PLANNED',
            selection_hash = v_selection_hash,
            plan_json = v_plan_json,
            plan_hash = v_plan_hash,
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE ready_request.id = v_request.id;

        UPDATE public.banking_pay_operations AS ready_operation
        SET status = 'WAITING_AUTHORISATION',
            phase = 'AWAITING_REAUTHENTICATION',
            progress_json = COALESCE(ready_operation.progress_json, '{}'::jsonb)
                || pg_catalog.jsonb_build_object(
                    'last_scanned_candidate_id', v_scan_last_candidate_id,
                    'selection_cursor_json', NULL,
                    'last_prepare_page_hash', v_page_hash,
                    'selected_candidate_count', v_total_candidate_count,
                    'selected_active_item_count', v_total_active_item_count,
                    'selected_source_row_count', v_total_source_row_count,
                    'selected_amount_pence', pg_catalog.round(v_total_amount * 100)::bigint,
                    'selection_hash', v_selection_hash,
                    'phase', 'AWAITING_REAUTHENTICATION'
                ),
            total_units = v_total_candidate_count,
            completed_units = 0,
            current_chunk_index = v_sequence_no,
            chunk_count = v_sequence_no,
            run_after_utc = NULL,
            requires_user_action = true,
            runner_state = 'WAITING_USER',
            resume_reason = 'AWAITING_REAUTHENTICATION',
            lease_owner = NULL,
            lease_expires_at_utc = NULL,
            locked_by = NULL,
            lock_expires_at_utc = NULL,
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE ready_operation.id = v_operation.id;
    ELSE
        UPDATE public.banking_pay_operations AS paging_operation
        SET progress_json = COALESCE(paging_operation.progress_json, '{}'::jsonb)
                || pg_catalog.jsonb_build_object(
                    'last_scanned_candidate_id', v_scan_last_candidate_id,
                    'selection_cursor_json', v_next_cursor_json,
                    'last_prepare_page_hash', v_page_hash,
                    'prepared_candidate_count', v_total_candidate_count,
                    'prepared_active_item_count', v_total_active_item_count,
                    'prepared_source_row_count', v_total_source_row_count,
                    'prepared_amount_pence', pg_catalog.round(v_total_amount * 100)::bigint,
                    'phase', 'PREPARE_SELECTION'
                ),
            current_chunk_index = v_sequence_no,
            chunk_count = v_sequence_no,
            run_after_utc = pg_catalog.clock_timestamp(),
            requires_user_action = false,
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE paging_operation.id = v_operation.id;
    END IF;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'correction_request_id', v_request.id,
        'operation_id', v_operation.id,
        'phase', CASE
            WHEN v_complete THEN 'AWAITING_REAUTHENTICATION'
            ELSE 'PREPARE_SELECTION'
        END,
        'page_candidate_count', v_page_candidate_count,
        'page_active_item_count', v_page_active_item_count,
        'page_source_row_count', v_page_source_row_count,
        'page_amount_pence', v_page_amount_pence,
        'page_hash', v_page_hash,
        'selection_hash', v_selection_hash,
        'active_batch_scope_hash', v_active_batch_scope_hash,
        'next_cursor_json', v_next_cursor_json,
        'complete', v_complete,
        'stale', false,
        'code', CASE
            WHEN v_complete THEN 'SELECTION_READY'
            ELSE 'SELECTION_PAGE_PREPARED'
        END,
        'message', CASE
            WHEN v_complete THEN 'The exact payment selection is ready to review.'
            ELSE 'The payment selection is still being prepared.'
        END
    );
EXCEPTION
    WHEN lock_not_available OR query_canceled THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_LOCK_TIMEOUT'
            USING ERRCODE = '55P03',
                  DETAIL = pg_catalog.jsonb_build_object(
                      'code', 'SELECTION_LOCK_TIMEOUT',
                      'correction_request_id', p_correction_request_id,
                      'operation_id', p_operation_id
                  )::text;
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) TO service_role;
