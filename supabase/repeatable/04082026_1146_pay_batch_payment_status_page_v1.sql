-- CloudTMS Banking Pay cancellation — Stage 1.
-- Bounded Current Payment Status page read.

CREATE OR REPLACE FUNCTION public.pay_batch_payment_status_page_v1(
    p_pay_batch_id uuid,
    p_actor_user_id uuid DEFAULT NULL::uuid,
    p_filter_json jsonb DEFAULT '{}'::jsonb,
    p_sort_key text DEFAULT 'STATUS'::text,
    p_sort_direction text DEFAULT 'ASC'::text,
    p_limit integer DEFAULT 25,
    p_cursor_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '5000ms'
AS $function$
DECLARE
    v_batch public.pay_batches%ROWTYPE;
    v_filter jsonb := coalesce(p_filter_json, '{}'::jsonb);
    v_sort_key text := pg_catalog.upper(pg_catalog.btrim(coalesce(p_sort_key, '')));
    v_sort_direction text := pg_catalog.upper(pg_catalog.btrim(coalesce(p_sort_direction, '')));
    v_snapshot_token text;
    v_explicit_snapshot_token text;
    v_active_batch_scope_hash text;
    v_rows jsonb := '[]'::jsonb;
    v_row_count integer := 0;
    v_total_matching_count integer := 0;
    v_eligible_matching_count integer := 0;
    v_selected_amount_pence_available bigint := 0;
    v_active_overview_candidate_count integer := 0;
    v_active_overview_amount_pence bigint := 0;
    v_original_overview_amount_pence bigint := 0;
    v_active_paye_schedule_line_count integer := 0;
    v_active_paye_schedule_amount_pence bigint := 0;
    v_has_more boolean := false;
    v_next_cursor_json jsonb := NULL;
    v_previous_cursor_json jsonb := NULL;
    v_last_row jsonb;
    v_latest_correction_request jsonb := NULL;
    v_batch_terminal boolean := false;
BEGIN
    IF p_pay_batch_id IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'code', 'PAY_BATCH_NOT_FOUND',
            'message', 'The Banking Pay batch was not found.'
        );
    END IF;

    IF pg_catalog.jsonb_typeof(v_filter) <> 'object' THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'FILTER_INVALID',
            'message', 'The payment-status filter is invalid.'
        );
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_object_keys(v_filter) AS filter_key(key_name)
        WHERE filter_key.key_name NOT IN (
            'status',
            'action',
            'search',
            'actionable_only',
            'pay_channel',
            'excluded_candidate_tokens',
            'included_candidate_tokens'
        )
    ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'FILTER_INVALID',
            'message', 'The payment-status filter contains an unsupported field.'
        );
    END IF;

    IF (v_filter ? 'excluded_candidate_tokens'
        AND pg_catalog.jsonb_typeof(v_filter -> 'excluded_candidate_tokens') <> 'array')
       OR (v_filter ? 'included_candidate_tokens'
        AND pg_catalog.jsonb_typeof(v_filter -> 'included_candidate_tokens') <> 'array') THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'FILTER_INVALID',
            'message', 'The payment-status candidate filter is invalid.'
        );
    END IF;

    IF v_sort_key NOT IN ('STATUS', 'CANDIDATE', 'AMOUNT')
       OR v_sort_direction NOT IN ('ASC', 'DESC') THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'SORT_INVALID',
            'message', 'The payment-status sort is invalid.'
        );
    END IF;

    IF p_limit IS NULL OR p_limit NOT IN (25, 50, 75, 100) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'PAGE_SIZE_INVALID',
            'message', 'The page size must be 25, 50, 75 or 100.'
        );
    END IF;

    IF p_cursor_json IS NOT NULL
       AND (
           pg_catalog.jsonb_typeof(p_cursor_json) <> 'object'
           OR p_cursor_json ->> 'snapshot_token' IS NULL
           OR p_cursor_json ->> 'sort_key' IS DISTINCT FROM v_sort_key
           OR p_cursor_json ->> 'sort_direction' IS DISTINCT FROM v_sort_direction
           OR p_cursor_json ->> 'last_pay_batch_candidate_id' IS NULL
           OR p_cursor_json ->> 'last_pay_batch_candidate_id'
                !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'CURSOR_INVALID',
            'message', 'The payment-status cursor is invalid.'
        );
    END IF;

    SELECT batch_row.*
    INTO v_batch
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = p_pay_batch_id;

    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'PAY_BATCH_NOT_FOUND',
            'message', 'The Banking Pay batch was not found.'
        );
    END IF;

    v_batch_terminal := pg_catalog.upper(pg_catalog.btrim(coalesce(v_batch.status, ''))) IN (
        'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
    );

    IF p_actor_user_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM public.tms_users AS actor_user
        WHERE actor_user.id = p_actor_user_id
          AND coalesce(actor_user.is_active, false)
    ) THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'code', 'PERMISSION_DENIED',
            'message', 'You do not have permission to view this payment status.'
        );
    END IF;

    v_active_batch_scope_hash := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
            'version', 2,
            'pay_batch_id', p_pay_batch_id,
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
                WHERE auth_row.pay_batch_id = p_pay_batch_id
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
                WHERE signal_row.pay_batch_id = p_pay_batch_id
            ),
            'scope_version_authority', 'banking_pay_batch_change_signals'
        )
    );

    v_snapshot_token := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
            'version', 1,
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'filter', v_filter,
            'sort_key', v_sort_key,
            'sort_direction', v_sort_direction
        )
    );

    -- EXPLICIT selection deliberately carries no filter authority. Bind a
    -- second token to that exact empty-filter contract so a filtered review
    -- can still submit an explicit UUID set without changing either scope.
    v_explicit_snapshot_token := private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
            'version', 1,
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'filter', '{}'::jsonb,
            'sort_key', v_sort_key,
            'sort_direction', v_sort_direction
        )
    );

    IF p_cursor_json IS NOT NULL
       AND p_cursor_json ->> 'snapshot_token' IS DISTINCT FROM v_snapshot_token THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'pay_batch_id', p_pay_batch_id,
            'snapshot_token', v_snapshot_token,
            'active_batch_scope_hash', v_active_batch_scope_hash,
            'code', 'PAYMENT_STATUS_SNAPSHOT_STALE',
            'message', 'Payment status changed. Refresh and select the payments again.'
        );
    END IF;

    WITH batch_provider_payloads AS MATERIALIZED (
        SELECT
            'OPERATION'::text AS evidence_source,
            operation_row.id AS operation_id,
            NULL::uuid AS chunk_id,
            operation_payload.payload_name,
            coalesce(operation_payload.payload_json, '{}'::jsonb) AS payload_json
        FROM public.banking_pay_operations AS operation_row
        CROSS JOIN LATERAL (VALUES
            ('progress_json'::text, operation_row.progress_json),
            ('result_json'::text, operation_row.result_json),
            ('error_json'::text, operation_row.error_json)
        ) AS operation_payload(payload_name, payload_json)
        WHERE operation_row.pay_batch_id = p_pay_batch_id
          AND pg_catalog.upper(coalesce(operation_row.operation_type, '')) LIKE '%PAY%'

        UNION ALL

        SELECT
            'CHUNK'::text AS evidence_source,
            operation_row.id AS operation_id,
            chunk_row.id AS chunk_id,
            chunk_payload.payload_name,
            coalesce(chunk_payload.payload_json, '{}'::jsonb) AS payload_json
        FROM public.banking_pay_operation_chunks AS chunk_row
        JOIN public.banking_pay_operations AS operation_row
          ON operation_row.id = chunk_row.operation_id
        CROSS JOIN LATERAL (VALUES
            ('payload_json'::text, chunk_row.payload_json),
            ('result_json'::text, chunk_row.result_json),
            ('error_json'::text, chunk_row.error_json)
        ) AS chunk_payload(payload_name, payload_json)
        WHERE operation_row.pay_batch_id = p_pay_batch_id
    ), batch_provider_payload_flags AS MATERIALIZED (
    SELECT
      batch_provider_payloads.evidence_source,
      batch_provider_payloads.operation_id,
      batch_provider_payloads.chunk_id,
      batch_provider_payloads.payload_name,
      batch_provider_payloads.payload_json,
      (
        lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerRequestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_request_sent_confirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerRequestSentConfirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'requestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'request_dispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'requestDispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_submit_attempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerSubmitAttempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_evidence,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerEvidence,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{diagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{outcome,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{outcome,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'requestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'requestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerRequestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerRequestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_request_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json->>'provider_request_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerRequestSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json->>'providerRequestSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_call_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json->>'provider_call_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerCallSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json->>'providerCallSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}')::numeric > 0
        )
      ) AS has_provider_request_sent,
      (
        lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_outcome_unknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerOutcomeUnknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'request_sent_no_response', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'requestSentNoResponse', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{diagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'error_code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'status', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'outcome', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{error,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
      ) AS has_provider_outcome_unknown,
      (
        (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'rail_tx_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'rail_tx_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'railTxId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'railTxId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_transaction_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_transaction_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerTransactionId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerTransactionId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_payment_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'provider_payment_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerPaymentId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json->>'providerPaymentId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,transactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,transactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,paymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider,paymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(batch_provider_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (batch_provider_payloads.payload_json ? 'provider_response' AND batch_provider_payloads.payload_json->'provider_response' IS NOT NULL AND batch_provider_payloads.payload_json->'provider_response' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'provider_response') = 'object' AND batch_provider_payloads.payload_json->'provider_response' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response') = 'array' AND batch_provider_payloads.payload_json->'provider_response' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response') = 'number')))
        OR (batch_provider_payloads.payload_json ? 'provider_response_json' AND batch_provider_payloads.payload_json->'provider_response_json' IS NOT NULL AND batch_provider_payloads.payload_json->'provider_response_json' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'provider_response_json') = 'object' AND batch_provider_payloads.payload_json->'provider_response_json' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response_json') = 'array' AND batch_provider_payloads.payload_json->'provider_response_json' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_response_json') = 'number')))
        OR (batch_provider_payloads.payload_json ? 'submit_response' AND batch_provider_payloads.payload_json->'submit_response' IS NOT NULL AND batch_provider_payloads.payload_json->'submit_response' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'submit_response') = 'object' AND batch_provider_payloads.payload_json->'submit_response' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'submit_response') = 'array' AND batch_provider_payloads.payload_json->'submit_response' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'submit_response') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'submit_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'submit_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'submit_response') = 'number')))
        OR (batch_provider_payloads.payload_json ? 'response_json' AND batch_provider_payloads.payload_json->'response_json' IS NOT NULL AND batch_provider_payloads.payload_json->'response_json' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'response_json') = 'object' AND batch_provider_payloads.payload_json->'response_json' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'response_json') = 'array' AND batch_provider_payloads.payload_json->'response_json' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'response_json') = 'number')))
        OR (batch_provider_payloads.payload_json ? 'provider_result' AND batch_provider_payloads.payload_json->'provider_result' IS NOT NULL AND batch_provider_payloads.payload_json->'provider_result' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'provider_result') = 'object' AND batch_provider_payloads.payload_json->'provider_result' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_result') = 'array' AND batch_provider_payloads.payload_json->'provider_result' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_result') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_result') = 'number')))
        OR (batch_provider_payloads.payload_json ? 'provider_payload' AND batch_provider_payloads.payload_json->'provider_payload' IS NOT NULL AND batch_provider_payloads.payload_json->'provider_payload' <> 'null'::jsonb AND ((jsonb_typeof(batch_provider_payloads.payload_json->'provider_payload') = 'object' AND batch_provider_payloads.payload_json->'provider_payload' <> '{}'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_payload') = 'array' AND batch_provider_payloads.payload_json->'provider_payload' <> '[]'::jsonb) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_payload') = 'string' AND NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_payload')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (batch_provider_payloads.payload_json->'provider_payload')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(batch_provider_payloads.payload_json->'provider_payload') = 'number')))
      ) AS has_provider_payload_evidence
    FROM batch_provider_payloads
    ), batch_provider_operation_facts AS MATERIALIZED (
        SELECT
            EXISTS (
                SELECT 1
                FROM public.banking_pay_operations AS provider_operation
                WHERE provider_operation.pay_batch_id = p_pay_batch_id
                  AND provider_operation.operation_type IN (
                      'PAYMENT_EXECUTE',
                      'PAYMENT_RETRY_BLOCKED_FUNDS'
                  )
                  AND provider_operation.status IN (
                      'QUEUED',
                      'RUNNING',
                      'PROCESSING',
                      'CLAIMED',
                      'IN_PROGRESS'
                  )
                  AND NOT (
                      pg_catalog.upper(pg_catalog.btrim(coalesce(provider_operation.phase, ''))) = 'SCHEDULE_PAYMENT'
                      AND pg_catalog.upper(pg_catalog.btrim(coalesce(provider_operation.resume_reason, ''))) IN (
                          'WAIT_FOR_SCHEDULED_NO_BANK_PAYMENT',
                          'WAIT_FOR_SCHEDULED_LOCAL_MANUAL_SETTLEMENT'
                      )
                  )
            ) AS provider_submission_in_progress,
            coalesce(pg_catalog.bool_or(has_provider_outcome_unknown), false)
                AS provider_outcome_unknown,
            coalesce(pg_catalog.bool_or(
                has_provider_request_sent OR has_provider_payload_evidence
            ), false) AS provider_request_sent,
            coalesce(pg_catalog.bool_or(has_provider_payload_evidence), false)
                AS provider_payload_evidence_present
        FROM batch_provider_payload_flags
    ), batch_unscoped_event_facts AS MATERIALIZED (
        SELECT
            coalesce(pg_catalog.bool_or(unscoped_movement.is_final_money_moved), false)
                AS final_money_moved,
            coalesce(pg_catalog.bool_or(unscoped_movement.is_terminal_no_money), false)
                AS terminal_no_money,
            coalesce(pg_catalog.bool_or(unscoped_movement.is_pending_non_final), false)
                AS pending_non_final,
            coalesce(pg_catalog.bool_or(
                (
                    unscoped_movement.cash_state = 'UNKNOWN'
                    OR pg_catalog.upper(coalesce(unscoped_event.normalised_state, ''))
                        IN ('UNKNOWN', 'OUTCOME_UNKNOWN', 'PROVIDER_UNKNOWN')
                    OR pg_catalog.upper(coalesce(unscoped_event.mapping_status, ''))
                        IN ('UNMATCHED', 'AMBIGUOUS', 'REVIEW_REQUIRED')
                    OR unscoped_event.movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
                    OR pg_catalog.upper(coalesce(unscoped_event.provider_failure_reason_group, ''))
                        IN ('WEBHOOK_UNMATCHED', 'PROVIDER_UNKNOWN')
                )
                AND pg_catalog.upper(coalesce(unscoped_event.provider_failure_reason_group, ''))
                    <> 'PROVIDER_OUTAGE'
            ), false) AS provider_outcome_unknown,
            coalesce(pg_catalog.bool_or(
                pg_catalog.upper(coalesce(unscoped_event.provider_failure_reason_group, '')) = 'PROVIDER_OUTAGE'
                AND pg_catalog.upper(coalesce(unscoped_event.mapping_status, '')) = 'MATCHED'
            ), false) AS provider_outage,
            coalesce(pg_catalog.bool_or(
                nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_transaction_id, '')), '') IS NOT NULL
                OR nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_event_key, '')), '') IS NOT NULL
            ), false) AS provider_external_id_present,
            coalesce(pg_catalog.bool_or(true), false) AS provider_event_present,
            coalesce(pg_catalog.bool_or(
                nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_transaction_id, '')), '') IS NOT NULL
                OR nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_event_key, '')), '') IS NOT NULL
                OR unscoped_event.provider_event_transport IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL')
                OR (
                    unscoped_event.provider_event_transport = 'PROVIDER_WEBHOOK'
                    AND unscoped_event.provider_signature_valid IS TRUE
                    AND unscoped_event.provider_webhook_receipt_id IS NOT NULL
                    AND unscoped_receipt.status IS NOT NULL
                    AND pg_catalog.upper(coalesce(unscoped_receipt.status, ''))
                        NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL', 'FAILED_RETRYABLE')
                    AND unscoped_receipt.signature_valid IS TRUE
                    AND unscoped_receipt.provider_key IS NOT DISTINCT FROM unscoped_event.provider_key
                    AND unscoped_receipt.rail_env IS NOT DISTINCT FROM unscoped_event.rail_env
                    AND nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_event_key, '')), '') IS NOT NULL
                    AND unscoped_receipt.provider_event_key = unscoped_event.provider_event_key
                )
                OR (
                    unscoped_event.provider_event_transport = 'FAILED_WEBHOOK_REPLAY'
                    AND unscoped_event.provider_webhook_receipt_id IS NOT NULL
                    AND unscoped_receipt.status IS NOT NULL
                    AND pg_catalog.upper(coalesce(unscoped_receipt.status, ''))
                        NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
                    AND unscoped_receipt.provider_key IS NOT DISTINCT FROM unscoped_event.provider_key
                    AND unscoped_receipt.rail_env IS NOT DISTINCT FROM unscoped_event.rail_env
                    AND nullif(pg_catalog.btrim(coalesce(unscoped_event.provider_event_key, '')), '') IS NOT NULL
                    AND unscoped_receipt.provider_event_key = unscoped_event.provider_event_key
                )
            ), false) AS provider_request_sent
        FROM public.pay_bank_transfer_events AS unscoped_event
        LEFT JOIN public.bank_provider_webhook_receipts AS unscoped_receipt
          ON unscoped_receipt.id = unscoped_event.provider_webhook_receipt_id
        CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
            unscoped_event.normalised_state,
            unscoped_event.provider_state,
            coalesce(unscoped_event.raw_payload, '{}'::jsonb),
            pg_catalog.jsonb_build_object(
                'provider_key', unscoped_event.provider_key,
                'provider_event_type', unscoped_event.provider_event_type,
                'provider_event_transport', unscoped_event.provider_event_transport,
                'provider_event_key', unscoped_event.provider_event_key
            )
        ) AS unscoped_movement
        WHERE unscoped_event.pay_batch_id = p_pay_batch_id
          AND unscoped_event.pay_bank_transfer_id IS NULL
    ), candidate_status_index AS MATERIALIZED (
        -- This is the key-selection index only.  It uses bounded indexed EXISTS
        -- probes and scalar latest-row lookups; all item/work/correction/provider
        -- rollups and JSON construction occur only after page_keys (<=100).
        SELECT candidate_row.id AS pay_batch_candidate_id,
               candidate_row.candidate_id,
               candidate_row.candidate_display_name,
               candidate_row.net_bank_amount,
               candidate_row.settlement_status,
               CASE WHEN EXISTS (
                 SELECT 1 FROM public.pay_batch_items AS active_item
                 WHERE active_item.pay_batch_candidate_id = candidate_row.id
                   AND COALESCE(active_item.is_voided, false) IS NOT TRUE
               ) THEN 1 ELSE 0 END::integer AS active_item_count,
               NOT EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS selected_scope_item
                 JOIN public.pay_batch_items AS shared_scope_item
                   ON shared_scope_item.pay_bank_transfer_id = selected_scope_item.pay_bank_transfer_id
                  AND shared_scope_item.id IS DISTINCT FROM selected_scope_item.id
                 JOIN public.pay_batch_candidates AS shared_scope_candidate
                   ON shared_scope_candidate.id = shared_scope_item.pay_batch_candidate_id
                 WHERE selected_scope_item.pay_batch_candidate_id = candidate_row.id
                   AND COALESCE(selected_scope_item.is_voided, false) IS NOT TRUE
                   AND selected_scope_item.pay_bank_transfer_id IS NOT NULL
                   AND shared_scope_candidate.pay_batch_id = p_pay_batch_id
                   AND NOT (
                     shared_scope_item.pay_batch_candidate_id = candidate_row.id
                     AND COALESCE(shared_scope_item.is_voided, false) IS NOT TRUE
                   )
               ) AS complete_candidate_instruction_scope,
               EXISTS (
                 SELECT 1
                 FROM public.banking_pay_operation_transfer_scope_items AS resolution_scope_item
                 JOIN public.banking_pay_operation_transfer_scope AS resolution_scope
                   ON resolution_scope.id = resolution_scope_item.transfer_scope_id
                  AND resolution_scope.operation_id = resolution_scope_item.operation_id
                  AND resolution_scope.pay_batch_id = resolution_scope_item.pay_batch_id
                 JOIN public.banking_pay_operations AS resolution_operation
                   ON resolution_operation.id = resolution_scope.operation_id
                  AND resolution_operation.pay_batch_id = resolution_scope.pay_batch_id
                 WHERE resolution_scope_item.pay_batch_id = p_pay_batch_id
                   AND resolution_scope_item.pay_batch_candidate_id = candidate_row.id
                   AND resolution_scope.pay_bank_transfer_id IS NOT NULL
                   AND resolution_operation.status <> 'CANCELLED'
               ) AS has_resolution_context,
               EXISTS (
                 SELECT 1 FROM public.pay_batch_items AS paye_item
                 WHERE paye_item.pay_batch_candidate_id = candidate_row.id
                   AND COALESCE(paye_item.is_voided, false) IS NOT TRUE
                   AND paye_item.pay_channel = 'PAYE'
               ) AS has_paye_item,
               EXISTS (
                 SELECT 1 FROM public.pay_batch_items AS non_paye_item
                 WHERE non_paye_item.pay_batch_candidate_id = candidate_row.id
                   AND COALESCE(non_paye_item.is_voided, false) IS NOT TRUE
                   AND non_paye_item.pay_channel <> 'PAYE'
               ) AS has_non_paye_item,
               (NOT EXISTS (
                  SELECT 1 FROM public.pay_batch_items AS active_item
                  WHERE active_item.pay_batch_candidate_id = candidate_row.id
                    AND COALESCE(active_item.is_voided, false) IS NOT TRUE
                ) OR EXISTS (
                  SELECT 1 FROM public.pay_payment_correction_items AS removed_item
                  WHERE removed_item.pay_batch_id = p_pay_batch_id
                    AND removed_item.pay_batch_candidate_id = candidate_row.id
                    AND removed_item.status = 'APPLIED'
                    AND removed_item.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND')
                )) AS removed,
               EXISTS (
                 SELECT 1 FROM public.pay_payment_correction_items AS released_item
                 WHERE released_item.pay_batch_id = p_pay_batch_id
                   AND released_item.pay_batch_candidate_id = candidate_row.id
                   AND released_item.status = 'APPLIED'
                   AND released_item.correction_item_kind = 'NO_MONEY_UNWIND'
               ) AS released,
               (candidate_row.settlement_status IN ('SETTLED','PAID') OR EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS event_item
                 JOIN public.pay_bank_transfer_events AS paid_event
                   ON paid_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                 CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   paid_event.normalised_state,
                   paid_event.provider_state,
                   coalesce(paid_event.raw_payload, '{}'::jsonb),
                   pg_catalog.jsonb_build_object(
                     'provider_key', paid_event.provider_key,
                     'provider_event_type', paid_event.provider_event_type,
                     'provider_event_transport', paid_event.provider_event_transport,
                     'provider_event_key', paid_event.provider_event_key
                   )
                 ) AS paid_movement
                 WHERE event_item.pay_batch_candidate_id = candidate_row.id
                   AND paid_event.pay_batch_id = p_pay_batch_id
                   AND paid_movement.is_final_money_moved
               ) OR EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS transfer_item
                 JOIN public.pay_bank_transfers AS paid_transfer
                   ON paid_transfer.id = transfer_item.pay_bank_transfer_id
                 CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   paid_transfer.status,
                   paid_transfer.rail_state,
                   coalesce(paid_transfer.rail_meta_json, '{}'::jsonb),
                   pg_catalog.jsonb_build_object(
                     'provider_key', paid_transfer.rail_provider,
                     'rail_env', paid_transfer.rail_env,
                     'request_id', paid_transfer.request_id,
                     'rail_tx_id', paid_transfer.rail_tx_id
                   )
                 ) AS paid_transfer_movement
                 WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                   AND paid_transfer_movement.is_final_money_moved
                ) OR unscoped_event_facts.final_money_moved) AS paid_or_settled,
               (EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS event_item
                 JOIN public.pay_bank_transfer_events AS no_money_event
                   ON no_money_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                 CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   no_money_event.normalised_state,
                   no_money_event.provider_state,
                   coalesce(no_money_event.raw_payload, '{}'::jsonb),
                   pg_catalog.jsonb_build_object(
                     'provider_key', no_money_event.provider_key,
                     'provider_event_type', no_money_event.provider_event_type,
                     'provider_event_transport', no_money_event.provider_event_transport,
                     'provider_event_key', no_money_event.provider_event_key
                   )
                 ) AS no_money_movement
                 WHERE event_item.pay_batch_candidate_id = candidate_row.id
                   AND no_money_event.pay_batch_id = p_pay_batch_id
                   AND no_money_movement.is_terminal_no_money
               ) OR EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS transfer_item
                 JOIN public.pay_bank_transfers AS failed_transfer
                   ON failed_transfer.id = transfer_item.pay_bank_transfer_id
                 CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   failed_transfer.status,
                   failed_transfer.rail_state,
                   coalesce(failed_transfer.rail_meta_json, '{}'::jsonb),
                   pg_catalog.jsonb_build_object(
                     'provider_key', failed_transfer.rail_provider,
                     'rail_env', failed_transfer.rail_env,
                     'request_id', failed_transfer.request_id,
                     'rail_tx_id', failed_transfer.rail_tx_id
                   )
                 ) AS failed_transfer_movement
                 WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                   AND failed_transfer_movement.is_terminal_no_money
                ) OR unscoped_event_facts.terminal_no_money) AS terminal_no_money,
               (EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS event_item
                  JOIN public.pay_bank_transfer_events AS ambiguous_event
                   ON ambiguous_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                 CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   ambiguous_event.normalised_state,
                   ambiguous_event.provider_state,
                   coalesce(ambiguous_event.raw_payload, '{}'::jsonb),
                   pg_catalog.jsonb_build_object(
                     'provider_key', ambiguous_event.provider_key,
                     'provider_event_type', ambiguous_event.provider_event_type,
                     'provider_event_transport', ambiguous_event.provider_event_transport,
                     'provider_event_key', ambiguous_event.provider_event_key
                   )
                 ) AS ambiguous_movement
                  WHERE event_item.pay_batch_candidate_id = candidate_row.id
                    AND ambiguous_event.pay_batch_id = p_pay_batch_id
                    AND (
                      ambiguous_movement.cash_state = 'UNKNOWN'
                      OR ambiguous_event.mapping_status IN ('UNMATCHED', 'AMBIGUOUS', 'REVIEW_REQUIRED')
                      OR ambiguous_event.movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
                      OR ambiguous_event.provider_failure_reason_group IN ('WEBHOOK_UNMATCHED', 'PROVIDER_UNKNOWN')
                    )
                    AND pg_catalog.upper(coalesce(ambiguous_event.provider_failure_reason_group, '')) <> 'PROVIDER_OUTAGE'
                ) OR EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS transfer_item
                  JOIN public.pay_bank_transfers AS unknown_transfer
                    ON unknown_transfer.id = transfer_item.pay_bank_transfer_id
                  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                    unknown_transfer.status,
                    unknown_transfer.rail_state,
                    coalesce(unknown_transfer.rail_meta_json, '{}'::jsonb),
                    pg_catalog.jsonb_build_object(
                      'provider_key', unknown_transfer.rail_provider,
                      'rail_env', unknown_transfer.rail_env,
                      'request_id', unknown_transfer.request_id,
                      'rail_tx_id', unknown_transfer.rail_tx_id
                    )
                  ) AS unknown_transfer_movement
                  WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                    AND unknown_transfer_movement.cash_state = 'UNKNOWN'
                ) OR unscoped_event_facts.provider_outcome_unknown) AS provider_outcome_unknown,
               (EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS event_item
                  JOIN public.pay_bank_transfer_events AS pending_event
                    ON pending_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                    pending_event.normalised_state,
                    pending_event.provider_state,
                    coalesce(pending_event.raw_payload, '{}'::jsonb),
                    pg_catalog.jsonb_build_object(
                      'provider_key', pending_event.provider_key,
                      'provider_event_type', pending_event.provider_event_type,
                      'provider_event_transport', pending_event.provider_event_transport,
                      'provider_event_key', pending_event.provider_event_key
                    )
                  ) AS pending_event_movement
                  WHERE event_item.pay_batch_candidate_id = candidate_row.id
                    AND pending_event.pay_batch_id = p_pay_batch_id
                    AND pending_event_movement.is_pending_non_final
                ) OR EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS transfer_item
                  JOIN public.pay_bank_transfers AS pending_transfer
                    ON pending_transfer.id = transfer_item.pay_bank_transfer_id
                  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
                    pending_transfer.status,
                    pending_transfer.rail_state,
                    coalesce(pending_transfer.rail_meta_json, '{}'::jsonb),
                    pg_catalog.jsonb_build_object(
                      'provider_key', pending_transfer.rail_provider,
                      'rail_env', pending_transfer.rail_env,
                      'request_id', pending_transfer.request_id,
                      'rail_tx_id', pending_transfer.rail_tx_id
                    )
                  ) AS pending_transfer_movement
                  WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                    AND pending_transfer_movement.is_pending_non_final
                ) OR unscoped_event_facts.pending_non_final) AS provider_pending_non_final,
               (EXISTS (
                  SELECT 1
                  FROM public.pay_batch_items AS outage_item
                  JOIN public.pay_bank_transfer_events AS outage_event
                    ON outage_event.pay_bank_transfer_id = outage_item.pay_bank_transfer_id
                  WHERE outage_item.pay_batch_candidate_id = candidate_row.id
                    AND outage_event.pay_batch_id = p_pay_batch_id
                    AND pg_catalog.upper(coalesce(outage_event.provider_failure_reason_group, '')) = 'PROVIDER_OUTAGE'
                    AND pg_catalog.upper(coalesce(outage_event.mapping_status, '')) = 'MATCHED'
                ) OR unscoped_event_facts.provider_outage) AS provider_outage,
               (
                 provider_facts.provider_request_sent
                 OR unscoped_event_facts.provider_request_sent
                 OR EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS request_item
                    JOIN public.pay_bank_transfers AS request_transfer
                      ON request_transfer.id = request_item.pay_bank_transfer_id
                    WHERE request_item.pay_batch_candidate_id = candidate_row.id
                      AND (
                        nullif(pg_catalog.btrim(coalesce(request_transfer.rail_tx_id, '')), '') IS NOT NULL
                        OR pg_catalog.upper(coalesce(request_transfer.status, '')) IN (
                          'REQUEST_SENT', 'PROVIDER_REQUEST_SENT', 'SUBMITTED', 'SENT',
                          'PROCESSING', 'ACCEPTED', 'PROVIDER_SUBMITTED',
                          'SUBMISSION_UNKNOWN', 'REQUEST_SENT_NO_RESPONSE'
                        )
                        OR pg_catalog.lower(nullif(pg_catalog.btrim(coalesce(
                             request_transfer.rail_meta_json ->> 'provider_request_sent', ''
                           )), '')) IN ('true', 't', 'yes', 'y', '1')
                        OR pg_catalog.lower(nullif(pg_catalog.btrim(coalesce(
                             request_transfer.rail_meta_json ->> 'request_sent', ''
                           )), '')) IN ('true', 't', 'yes', 'y', '1')
                        OR pg_catalog.lower(nullif(pg_catalog.btrim(coalesce(
                             request_transfer.rail_meta_json ->> 'provider_submit_attempted', ''
                           )), '')) IN ('true', 't', 'yes', 'y', '1')
                        OR nullif(pg_catalog.btrim(coalesce(
                             request_transfer.rail_meta_json ->> 'request_sent_at_utc', ''
                           )), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(
                             request_transfer.rail_meta_json ->> 'provider_request_sent_at_utc', ''
                           )), '') IS NOT NULL
                        OR (
                             request_transfer.rail_meta_json ? 'provider_response'
                             AND request_transfer.rail_meta_json -> 'provider_response' IS NOT NULL
                             AND request_transfer.rail_meta_json -> 'provider_response' <> 'null'::jsonb
                           )
                      )
                 )
                 OR EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS request_event_item
                    JOIN public.pay_bank_transfer_events AS request_event
                      ON request_event.pay_bank_transfer_id = request_event_item.pay_bank_transfer_id
                    LEFT JOIN public.bank_provider_webhook_receipts AS request_receipt
                      ON request_receipt.id = request_event.provider_webhook_receipt_id
                    WHERE request_event_item.pay_batch_candidate_id = candidate_row.id
                      AND request_event.pay_batch_id = p_pay_batch_id
                      AND (
                        nullif(pg_catalog.btrim(coalesce(request_event.provider_transaction_id, '')), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(request_event.provider_event_key, '')), '') IS NOT NULL
                        OR request_event.provider_event_transport IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL')
                        OR (
                          request_event.provider_event_transport = 'PROVIDER_WEBHOOK'
                          AND request_event.provider_signature_valid IS TRUE
                          AND request_event.provider_webhook_receipt_id IS NOT NULL
                          AND request_receipt.status IS NOT NULL
                          AND pg_catalog.upper(coalesce(request_receipt.status, ''))
                              NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL', 'FAILED_RETRYABLE')
                          AND request_receipt.signature_valid IS TRUE
                          AND request_receipt.provider_key IS NOT DISTINCT FROM request_event.provider_key
                          AND request_receipt.rail_env IS NOT DISTINCT FROM request_event.rail_env
                          AND nullif(pg_catalog.btrim(coalesce(request_event.provider_event_key, '')), '') IS NOT NULL
                          AND request_receipt.provider_event_key = request_event.provider_event_key
                        )
                        OR (
                          request_event.provider_event_transport = 'FAILED_WEBHOOK_REPLAY'
                          AND request_event.provider_webhook_receipt_id IS NOT NULL
                          AND request_receipt.status IS NOT NULL
                          AND pg_catalog.upper(coalesce(request_receipt.status, ''))
                              NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
                          AND request_receipt.provider_key IS NOT DISTINCT FROM request_event.provider_key
                          AND request_receipt.rail_env IS NOT DISTINCT FROM request_event.rail_env
                          AND nullif(pg_catalog.btrim(coalesce(request_event.provider_event_key, '')), '') IS NOT NULL
                          AND request_receipt.provider_event_key = request_event.provider_event_key
                        )
                      )
                 )
               ) AS provider_request_sent,
               (
                 unscoped_event_facts.provider_external_id_present
                 OR EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS external_item
                    JOIN public.pay_bank_transfers AS external_transfer
                      ON external_transfer.id = external_item.pay_bank_transfer_id
                    WHERE external_item.pay_batch_candidate_id = candidate_row.id
                      AND (
                        nullif(pg_catalog.btrim(coalesce(external_transfer.rail_tx_id, '')), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(
                             external_transfer.rail_meta_json ->> 'rail_tx_id', ''
                           )), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(
                             external_transfer.rail_meta_json ->> 'provider_transaction_id', ''
                           )), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(
                             external_transfer.rail_meta_json ->> 'provider_payment_id', ''
                           )), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(
                             external_transfer.rail_meta_json ->> 'provider_reference', ''
                           )), '') IS NOT NULL
                      )
                 )
                 OR EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS external_event_item
                    JOIN public.pay_bank_transfer_events AS external_event
                      ON external_event.pay_bank_transfer_id = external_event_item.pay_bank_transfer_id
                    WHERE external_event_item.pay_batch_candidate_id = candidate_row.id
                      AND external_event.pay_batch_id = p_pay_batch_id
                      AND (
                        nullif(pg_catalog.btrim(coalesce(external_event.provider_transaction_id, '')), '') IS NOT NULL
                        OR nullif(pg_catalog.btrim(coalesce(external_event.provider_event_key, '')), '') IS NOT NULL
                      )
                 )
               ) AS provider_external_id_present,
               (
                 unscoped_event_facts.provider_event_present
                 OR EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS provider_event_item
                    JOIN public.pay_bank_transfer_events AS provider_event
                      ON provider_event.pay_bank_transfer_id = provider_event_item.pay_bank_transfer_id
                    WHERE provider_event_item.pay_batch_candidate_id = candidate_row.id
                      AND provider_event.pay_batch_id = p_pay_batch_id
                 )
               ) AS provider_event_present,
               EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS manual_item
                 JOIN public.pay_batch_candidates AS manual_candidate
                   ON manual_candidate.id = manual_item.pay_batch_candidate_id
                 LEFT JOIN public.pay_bank_transfers AS manual_transfer
                   ON manual_transfer.id = manual_item.pay_bank_transfer_id
                 LEFT JOIN public.candidates AS manual_candidate_record
                   ON manual_candidate_record.id = manual_candidate.candidate_id
                 WHERE manual_candidate.pay_batch_id = p_pay_batch_id
                   AND manual_item.pay_batch_candidate_id = candidate_row.id
                   AND (
                     pg_catalog.upper(coalesce(manual_item.item_type, '')) LIKE '%MANUAL%'
                     OR pg_catalog.upper(coalesce(manual_item.item_type, '')) LIKE '%ADJUSTMENT%'
                     OR pg_catalog.upper(coalesce(manual_item.item_type, '')) LIKE '%DEBT%'
                     OR pg_catalog.upper(coalesce(manual_item.item_type, '')) LIKE '%CREDIT%'
                     OR pg_catalog.upper(coalesce(manual_item.item_type, '')) IN (
                       'ADJUSTMENT_DELTA', 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
                       'MANUAL_CREDIT_PAYOUT', 'MANUAL_DEBT_RECOVERY',
                       'FINANCE_ADJUSTMENT'
                     )
                     OR pg_catalog.upper(coalesce(manual_item.source_ref, '')) LIKE 'MANUAL%'
                     OR pg_catalog.upper(coalesce(manual_item.operation_source_key, '')) LIKE 'MANUAL%'
                   )
                   AND NOT EXISTS (
                     SELECT 1
                     FROM public.pay_manual_adjustment_carry_forwards AS existing_target
                     WHERE existing_target.target_pay_batch_item_id = manual_item.id
                   )
                   AND pg_catalog.upper(coalesce(manual_item.source_ref, '')) NOT LIKE 'CARRY_FORWARD:%'
                   AND pg_catalog.upper(coalesce(manual_item.operation_source_key, '')) NOT LIKE 'CARRY_FORWARD:%'
                   AND manual_item.finance_case_id IS NULL
                   AND manual_item.finance_component_id IS NULL
                   AND manual_item.reservation_id IS NULL
                   AND manual_item.timesheet_id IS NULL
                   AND pg_catalog.upper(coalesce(manual_item.operation_source_key, '')) NOT LIKE ALL (
                     ARRAY['TIMESHEET:%', 'TS:%', 'SEG:%', 'ADJ:%', 'EXPENSE:%',
                           'MILEAGE:%', 'ADDITIONAL%', 'FINANCE:%', 'FINANCE_CASE:%',
                           'RESERVATION:%', 'ADVANCE:%']
                   )
                   AND pg_catalog.upper(coalesce(manual_item.source_ref, '')) NOT LIKE ALL (
                     ARRAY['TS:%', 'SEG:%', 'ADJ:%', 'EXPENSE:%', 'MILEAGE:%', 'ADDITIONAL%']
                   )
                   AND NOT (
                     manual_item.amount_inc_vat IS NOT NULL
                     AND pg_catalog.round(manual_item.amount_inc_vat, 2) <> 0
                     AND manual_item.amount_ex_vat IS NOT NULL
                     AND manual_item.amount_vat IS NOT NULL
                     AND nullif(pg_catalog.btrim(coalesce(manual_item.description, '')), '') IS NOT NULL
                     AND manual_candidate.candidate_id IS NOT NULL
                     AND pg_catalog.upper(pg_catalog.btrim(coalesce(manual_item.pay_channel, ''))) IN ('PAYE', 'UMBRELLA')
                     AND (
                       pg_catalog.upper(pg_catalog.btrim(coalesce(manual_item.pay_channel, ''))) = 'PAYE'
                       OR coalesce(
                         manual_item.umbrella_id,
                         manual_transfer.umbrella_id,
                         manual_candidate_record.umbrella_id,
                         CASE
                           WHEN pg_catalog.upper(coalesce(manual_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
                           THEN manual_transfer.payee_entity_id
                           ELSE NULL::uuid
                         END
                       ) IS NOT NULL
                     )
                   )
               ) AS manual_carry_forward_blocked,
               EXISTS (
                 SELECT 1
                 FROM public.pay_manual_adjustment_carry_forwards AS carry_forward
                 LEFT JOIN public.pay_batch_items AS target_item
                   ON target_item.id = carry_forward.target_pay_batch_item_id
                 LEFT JOIN public.pay_batch_candidates AS target_candidate
                   ON target_candidate.id = target_item.pay_batch_candidate_id
                 LEFT JOIN public.pay_batch_items AS source_item
                   ON source_item.id = carry_forward.source_pay_batch_item_id
                 LEFT JOIN public.pay_batch_candidates AS source_candidate
                   ON source_candidate.id = source_item.pay_batch_candidate_id
                 LEFT JOIN public.pay_bank_transfers AS source_transfer
                   ON source_transfer.id = source_item.pay_bank_transfer_id
                 LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
                   source_transfer.status,
                   source_transfer.rail_state,
                   coalesce(source_transfer.rail_meta_json, '{}'::jsonb),
                   coalesce(source_transfer.rail_meta_json, '{}'::jsonb)
                 ) AS source_movement ON source_item.pay_bank_transfer_id IS NOT NULL
                 WHERE (
                   carry_forward.candidate_id = candidate_row.candidate_id
                   OR target_item.pay_batch_candidate_id = candidate_row.id
                   OR source_item.pay_batch_candidate_id = candidate_row.id
                 )
                   AND (
                     carry_forward.status IN ('CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW')
                     OR (
                       carry_forward.status = 'CONSUMED_IN_BATCH'
                       AND carry_forward.target_pay_batch_id IS DISTINCT FROM p_pay_batch_id
                     )
                     OR (
                       carry_forward.status = 'RESERVED_IN_DRAFT'
                       AND carry_forward.target_pay_batch_id IS NOT NULL
                       AND carry_forward.target_pay_batch_id IS DISTINCT FROM p_pay_batch_id
                     )
                     OR (
                       carry_forward.status = 'RESERVED_IN_DRAFT'
                       AND carry_forward.target_pay_batch_id = p_pay_batch_id
                       AND target_item.id IS NULL
                     )
                     OR (
                       carry_forward.status = 'RESERVED_IN_DRAFT'
                       AND carry_forward.target_pay_batch_id = p_pay_batch_id
                       AND coalesce(target_item.is_voided, false)
                     )
                     OR (
                       carry_forward.status = 'RESERVED_IN_DRAFT'
                       AND carry_forward.target_pay_batch_id = p_pay_batch_id
                       AND (
                         pg_catalog.round(coalesce(carry_forward.amount_ex_vat, 0), 2)
                           <> pg_catalog.round(coalesce(target_item.amount_ex_vat, 0), 2)
                         OR pg_catalog.round(coalesce(carry_forward.amount_vat, 0), 2)
                           <> pg_catalog.round(coalesce(target_item.amount_vat, 0), 2)
                         OR pg_catalog.round(coalesce(carry_forward.amount_inc_vat, 0), 2)
                           <> pg_catalog.round(coalesce(target_item.amount_inc_vat, 0), 2)
                       )
                     )
                     OR (
                       source_item.id IS NOT NULL
                       AND (
                         pg_catalog.round(coalesce(carry_forward.amount_ex_vat, 0), 2)
                           <> pg_catalog.round(coalesce(source_item.amount_ex_vat, 0), 2)
                         OR pg_catalog.round(coalesce(carry_forward.amount_vat, 0), 2)
                           <> pg_catalog.round(coalesce(source_item.amount_vat, 0), 2)
                         OR pg_catalog.round(coalesce(carry_forward.amount_inc_vat, 0), 2)
                           <> pg_catalog.round(coalesce(source_item.amount_inc_vat, 0), 2)
                       )
                     )
                     OR pg_catalog.upper(coalesce(source_candidate.settlement_status, '')) = 'SETTLED'
                     OR source_candidate.settled_at_utc IS NOT NULL
                     OR coalesce(source_movement.is_final_money_moved, false)
                     OR (
                       carry_forward.status = 'PENDING_CARRY_FORWARD'
                       AND carry_forward.candidate_id = candidate_row.candidate_id
                       AND NOT EXISTS (
                         SELECT 1
                         FROM public.pay_batch_items AS represented_item
                         JOIN public.pay_batch_candidates AS represented_candidate
                           ON represented_candidate.id = represented_item.pay_batch_candidate_id
                         WHERE represented_candidate.pay_batch_id = p_pay_batch_id
                           AND represented_candidate.candidate_id = carry_forward.candidate_id
                           AND pg_catalog.upper(pg_catalog.btrim(coalesce(represented_item.pay_channel, '')))
                             = pg_catalog.upper(pg_catalog.btrim(coalesce(carry_forward.pay_channel, '')))
                           AND coalesce(represented_item.is_voided, false) IS NOT TRUE
                           AND (
                             represented_item.id = carry_forward.target_pay_batch_item_id
                             OR pg_catalog.lower(coalesce(represented_item.operation_source_key, ''))
                               = 'carry_forward:' || carry_forward.id::text
                             OR pg_catalog.lower(coalesce(represented_item.source_ref, ''))
                               = 'carry_forward:' || carry_forward.id::text
                           )
                       )
                     )
                   )
               ) AS carry_forward_freshness_blocked,
               (
                 SELECT work_row.status
                 FROM public.pay_payment_correction_work_items AS work_row
                 WHERE work_row.pay_batch_id = p_pay_batch_id
                   AND work_row.pay_batch_candidate_id = candidate_row.id
                 ORDER BY work_row.created_at_utc DESC, work_row.id DESC LIMIT 1
               ) AS latest_work_status,
               COALESCE((
                 SELECT member_row.active_amount
                 FROM public.pay_payment_correction_request_candidates AS member_row
                 JOIN public.pay_payment_correction_requests AS request_row
                   ON request_row.id = member_row.correction_request_id
                 WHERE request_row.pay_batch_id = p_pay_batch_id
                   AND member_row.pay_batch_candidate_id = candidate_row.id
                 ORDER BY CASE WHEN request_row.status IN ('APPLIED','APPLIED_WITH_BLOCKERS') THEN 0 ELSE 1 END,
                          request_row.updated_at_utc DESC, request_row.id DESC LIMIT 1
               ), CASE
                    WHEN EXISTS (
                      SELECT 1
                      FROM public.pay_batch_items AS paye_scope_item
                      WHERE paye_scope_item.pay_batch_candidate_id = candidate_row.id
                        AND COALESCE(paye_scope_item.is_voided, false) IS NOT TRUE
                        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(paye_scope_item.pay_channel, ''))) = 'PAYE'
                    ) THEN
                      COALESCE((
                        SELECT paye_input.net_amount
                        FROM public.pay_batch_paye_net_inputs AS paye_input
                        WHERE paye_input.pay_batch_candidate_id = candidate_row.id
                        ORDER BY paye_input.imported_at_utc DESC, paye_input.id DESC
                        LIMIT 1
                      ), 0)
                      + COALESCE((
                        SELECT pg_catalog.sum(umbrella_scope_item.amount_inc_vat)
                        FROM public.pay_batch_items AS umbrella_scope_item
                        WHERE umbrella_scope_item.pay_batch_candidate_id = candidate_row.id
                          AND COALESCE(umbrella_scope_item.is_voided, false) IS NOT TRUE
                          AND pg_catalog.upper(pg_catalog.btrim(COALESCE(umbrella_scope_item.pay_channel, ''))) = 'UMBRELLA'
                      ), 0)
                    ELSE candidate_row.net_bank_amount
                  END, 0)::numeric(14,2) AS reviewed_payment_amount
        FROM public.pay_batch_candidates AS candidate_row
        CROSS JOIN batch_provider_operation_facts AS provider_facts
        CROSS JOIN batch_unscoped_event_facts AS unscoped_event_facts
        WHERE candidate_row.pay_batch_id = p_pay_batch_id
    ), candidate_provider_precedence_index AS MATERIALIZED (
        SELECT candidate_status_index.*,
               CASE
                 WHEN candidate_status_index.paid_or_settled THEN 'FINAL_PAID'
                 WHEN candidate_status_index.provider_outcome_unknown
                      OR provider_facts.provider_outcome_unknown THEN 'PROVIDER_OUTCOME_UNKNOWN'
                  WHEN (
                         candidate_status_index.provider_request_sent
                         OR candidate_status_index.provider_external_id_present
                         OR provider_facts.provider_submission_in_progress
                         OR (
                           candidate_status_index.provider_pending_non_final
                           AND candidate_status_index.provider_event_present
                         )
                       )
                      AND candidate_status_index.terminal_no_money IS NOT TRUE
                      AND candidate_status_index.paid_or_settled IS NOT TRUE
                   THEN 'PENDING_NON_FINAL'
                 WHEN candidate_status_index.provider_outage
                      AND candidate_status_index.provider_request_sent IS NOT TRUE
                      AND candidate_status_index.provider_external_id_present IS NOT TRUE
                      AND provider_facts.provider_submission_in_progress IS NOT TRUE
                   THEN 'PROVIDER_OUTAGE_RETRY_LATER'
                 WHEN candidate_status_index.terminal_no_money THEN 'TERMINAL_NO_MONEY'
                 ELSE 'NO_TRANSFER_EVIDENCE'
               END AS canonical_provider_state,
               (
                 candidate_status_index.provider_outcome_unknown
                 OR provider_facts.provider_outcome_unknown
                 OR (
                    (
                      candidate_status_index.provider_request_sent
                      OR candidate_status_index.provider_external_id_present
                      OR provider_facts.provider_submission_in_progress
                      OR (
                        candidate_status_index.provider_pending_non_final
                        AND candidate_status_index.provider_event_present
                      )
                    )
                   AND candidate_status_index.terminal_no_money IS NOT TRUE
                   AND candidate_status_index.paid_or_settled IS NOT TRUE
                 )
               ) AS ambiguous
        FROM candidate_status_index
        CROSS JOIN batch_provider_operation_facts AS provider_facts
    ), candidate_release_eligibility_index AS MATERIALIZED (
        SELECT candidate_provider_precedence_index.*,
               (
                  v_batch_terminal IS NOT TRUE
                  AND
                  candidate_provider_precedence_index.canonical_provider_state = 'TERMINAL_NO_MONEY'
                  AND candidate_provider_precedence_index.paid_or_settled IS NOT TRUE
                  AND candidate_provider_precedence_index.ambiguous IS NOT TRUE
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'BLOCKED'
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'FAILED_FINAL'
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'FAILED_RETRYABLE'
                  AND candidate_provider_precedence_index.manual_carry_forward_blocked IS NOT TRUE
                  AND candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE
                  AND candidate_provider_precedence_index.complete_candidate_instruction_scope
                  AND provider_facts.provider_submission_in_progress IS NOT TRUE
                  AND provider_facts.provider_outcome_unknown IS NOT TRUE
                ) AS release_failed_payment_eligible,
                (
                  v_batch_terminal IS NOT TRUE
                  AND
                  candidate_provider_precedence_index.removed IS NOT TRUE
                  AND candidate_provider_precedence_index.active_item_count > 0
                  AND candidate_provider_precedence_index.canonical_provider_state = 'NO_TRANSFER_EVIDENCE'
                  AND candidate_provider_precedence_index.paid_or_settled IS NOT TRUE
                  AND candidate_provider_precedence_index.ambiguous IS NOT TRUE
                  AND candidate_provider_precedence_index.provider_request_sent IS NOT TRUE
                  AND candidate_provider_precedence_index.provider_external_id_present IS NOT TRUE
                  AND candidate_provider_precedence_index.provider_outcome_unknown IS NOT TRUE
                  AND candidate_provider_precedence_index.provider_outage IS NOT TRUE
                  AND candidate_provider_precedence_index.terminal_no_money IS NOT TRUE
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'BLOCKED'
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'FAILED_FINAL'
                  AND candidate_provider_precedence_index.latest_work_status IS DISTINCT FROM 'FAILED_RETRYABLE'
                  AND candidate_provider_precedence_index.manual_carry_forward_blocked IS NOT TRUE
                  AND candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE
                  AND candidate_provider_precedence_index.complete_candidate_instruction_scope
                  AND provider_facts.provider_submission_in_progress IS NOT TRUE
                  AND provider_facts.provider_outcome_unknown IS NOT TRUE
                ) AS pre_provider_cancel_eligible
        FROM candidate_provider_precedence_index
        CROSS JOIN batch_provider_operation_facts AS provider_facts
    ), candidate_classified_index AS MATERIALIZED (
        SELECT candidate_release_eligibility_index.*,
               CASE
                 WHEN removed AND released THEN 'RELEASED'
                 WHEN removed THEN 'CANCELLED'
                 WHEN paid_or_settled THEN 'SETTLED'
                 WHEN latest_work_status = 'BLOCKED' THEN 'BLOCKED'
                 WHEN latest_work_status IN ('FAILED_FINAL','FAILED_RETRYABLE') THEN 'FAILED'
                 WHEN canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'BLOCKED'
                 WHEN release_failed_payment_eligible THEN 'NOT_PAID'
                 WHEN ambiguous THEN 'AMBIGUOUS'
                 WHEN terminal_no_money THEN 'BLOCKED'
                 ELSE 'ACTIVE'
               END AS payment_display_state,
               CASE
                 WHEN removed OR paid_or_settled THEN ARRAY[]::text[]
                 WHEN v_batch_terminal THEN ARRAY[]::text[]
                 WHEN latest_work_status IN ('BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE') THEN ARRAY[]::text[]
                 WHEN canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN ARRAY[]::text[]
                 WHEN v_batch.status = 'DRAFT' AND pre_provider_cancel_eligible
                   THEN ARRAY['DRAFT_CANCEL']::text[]
                 WHEN v_batch.status = 'DRAFT' THEN ARRAY[]::text[]
                 WHEN release_failed_payment_eligible THEN ARRAY['RELEASE_FAILED_PAYMENT']::text[]
                 WHEN ambiguous AND has_resolution_context THEN ARRAY['RESOLVE_PAYMENT_STATUS']::text[]
                 WHEN ambiguous THEN ARRAY[]::text[]
                 WHEN terminal_no_money THEN ARRAY[]::text[]
                 WHEN pre_provider_cancel_eligible THEN ARRAY['CANCEL_PAYMENT']::text[]
                 ELSE ARRAY[]::text[]
               END AS available_actions,
               pg_catalog.round(reviewed_payment_amount * 100)::bigint AS original_payment_amount_pence,
                CASE WHEN removed THEN 70 WHEN paid_or_settled THEN 60
                     WHEN latest_work_status = 'BLOCKED' THEN 50
                     WHEN canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 50
                     WHEN release_failed_payment_eligible THEN 40
                    WHEN ambiguous THEN 30
                    WHEN terminal_no_money THEN 50
                    ELSE 10 END AS status_rank
        FROM candidate_release_eligibility_index
    ), candidate_filtered_index AS MATERIALIZED (
        SELECT classified_index.*
        FROM candidate_classified_index AS classified_index
        WHERE (NULLIF(pg_catalog.btrim(v_filter->>'search'), '') IS NULL
               OR pg_catalog.lower(classified_index.candidate_display_name)
                  LIKE '%' || pg_catalog.lower(pg_catalog.btrim(v_filter->>'search')) || '%')
          AND (NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter->>'status')), '') IS NULL
               OR classified_index.payment_display_state = pg_catalog.upper(pg_catalog.btrim(v_filter->>'status')))
          AND (NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter->>'action')), '') IS NULL
               OR pg_catalog.upper(pg_catalog.btrim(v_filter->>'action')) = ANY(classified_index.available_actions))
          AND (COALESCE((v_filter->>'actionable_only')::boolean, false) IS NOT TRUE
               OR pg_catalog.cardinality(classified_index.available_actions) > 0)
          AND (NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter->>'pay_channel')), '') IS NULL
               OR (pg_catalog.upper(pg_catalog.btrim(v_filter->>'pay_channel')) = 'PAYE'
                   AND classified_index.has_paye_item))
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_filter->'excluded_candidate_tokens', '[]'::jsonb)
            ) AS excluded_token(value)
            WHERE excluded_token.value = classified_index.pay_batch_candidate_id::text
          )
          AND (NOT (v_filter ? 'included_candidate_tokens') OR EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
              v_filter->'included_candidate_tokens'
            ) AS included_token(value)
            WHERE included_token.value = classified_index.pay_batch_candidate_id::text
          ))
    ), candidate_after_cursor AS MATERIALIZED (
        SELECT filtered_index.*
        FROM candidate_filtered_index AS filtered_index
        WHERE p_cursor_json IS NULL OR
          (v_sort_key = 'STATUS' AND (
            (v_sort_direction = 'ASC' AND (filtered_index.status_rank,
              pg_catalog.lower(filtered_index.candidate_display_name), filtered_index.pay_batch_candidate_id) >
              ((p_cursor_json->>'last_status_rank')::integer,
               p_cursor_json->>'last_candidate_name',
               (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))
            OR (v_sort_direction = 'DESC' AND (filtered_index.status_rank,
              pg_catalog.lower(filtered_index.candidate_display_name), filtered_index.pay_batch_candidate_id) <
              ((p_cursor_json->>'last_status_rank')::integer,
               p_cursor_json->>'last_candidate_name',
               (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))))
          OR (p_cursor_json IS NOT NULL AND v_sort_key = 'CANDIDATE' AND (
            (v_sort_direction = 'ASC' AND (pg_catalog.lower(filtered_index.candidate_display_name),
              filtered_index.pay_batch_candidate_id) >
              (p_cursor_json->>'last_candidate_name', (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))
            OR (v_sort_direction = 'DESC' AND (pg_catalog.lower(filtered_index.candidate_display_name),
              filtered_index.pay_batch_candidate_id) <
              (p_cursor_json->>'last_candidate_name', (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))))
          OR (p_cursor_json IS NOT NULL AND v_sort_key = 'AMOUNT' AND (
            (v_sort_direction = 'ASC' AND (filtered_index.original_payment_amount_pence,
              filtered_index.pay_batch_candidate_id) >
              ((p_cursor_json->>'last_amount_pence')::bigint,
               (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))
            OR (v_sort_direction = 'DESC' AND (filtered_index.original_payment_amount_pence,
              filtered_index.pay_batch_candidate_id) <
              ((p_cursor_json->>'last_amount_pence')::bigint,
               (p_cursor_json->>'last_pay_batch_candidate_id')::uuid))))
    ), page_keys AS MATERIALIZED (
        SELECT candidate_after_cursor.pay_batch_candidate_id
        FROM candidate_after_cursor
        ORDER BY
          CASE WHEN v_sort_key = 'STATUS' AND v_sort_direction = 'ASC' THEN status_rank END ASC,
          CASE WHEN v_sort_key = 'STATUS' AND v_sort_direction = 'DESC' THEN status_rank END DESC,
          CASE WHEN v_sort_key IN ('STATUS','CANDIDATE') AND v_sort_direction = 'ASC'
            THEN pg_catalog.lower(candidate_display_name) END ASC,
          CASE WHEN v_sort_key IN ('STATUS','CANDIDATE') AND v_sort_direction = 'DESC'
            THEN pg_catalog.lower(candidate_display_name) END DESC,
          CASE WHEN v_sort_key = 'AMOUNT' AND v_sort_direction = 'ASC' THEN original_payment_amount_pence END ASC,
          CASE WHEN v_sort_key = 'AMOUNT' AND v_sort_direction = 'DESC' THEN original_payment_amount_pence END DESC,
          CASE WHEN v_sort_direction = 'ASC' THEN pay_batch_candidate_id END ASC,
          CASE WHEN v_sort_direction = 'DESC' THEN pay_batch_candidate_id END DESC
        LIMIT p_limit + 1
    ), item_rollup AS (
        SELECT item_candidate.id AS pay_batch_candidate_id,
               pg_catalog.count(*)::integer AS original_item_count,
               pg_catalog.count(*) FILTER (
                   WHERE coalesce(batch_item.is_voided, false) IS NOT TRUE
               )::integer AS active_item_count,
               pg_catalog.bool_or(batch_item.pay_channel = 'PAYE') AS has_paye_item,
               pg_catalog.bool_or(batch_item.pay_channel <> 'PAYE') AS has_non_paye_item,
               pg_catalog.array_agg(DISTINCT batch_item.pay_bank_transfer_id)
                   FILTER (WHERE batch_item.pay_bank_transfer_id IS NOT NULL) AS transfer_ids
        FROM public.pay_batch_candidates AS item_candidate
        JOIN page_keys ON page_keys.pay_batch_candidate_id = item_candidate.id
        JOIN public.pay_batch_items AS batch_item
          ON batch_item.pay_batch_candidate_id = item_candidate.id
        WHERE item_candidate.pay_batch_id = p_pay_batch_id
        GROUP BY item_candidate.id
    ), correction_rollup AS (
        SELECT correction_item.pay_batch_candidate_id,
               pg_catalog.bool_or(
                   correction_item.status = 'APPLIED'
                   AND correction_item.correction_item_kind IN (
                       'PRE_BANK_CANCEL',
                       'NO_MONEY_UNWIND'
                   )
                ) AS has_applied_removal,
               pg_catalog.bool_or(
                   correction_item.status = 'APPLIED'
                   AND correction_item.correction_item_kind = 'NO_MONEY_UNWIND'
               ) AS has_applied_release,
               pg_catalog.bool_or(
                   correction_item.status = 'APPLIED'
                   AND correction_item.correction_item_kind = 'PRE_BANK_CANCEL'
               ) AS has_applied_cancel,
               pg_catalog.max(correction_item.applied_at_utc) AS removed_at_utc
        FROM public.pay_payment_correction_items AS correction_item
        JOIN page_keys ON page_keys.pay_batch_candidate_id = correction_item.pay_batch_candidate_id
        WHERE correction_item.pay_batch_id = p_pay_batch_id
        GROUP BY correction_item.pay_batch_candidate_id
    ), latest_removal_request AS (
        SELECT DISTINCT ON (correction_item.pay_batch_candidate_id)
               correction_item.pay_batch_candidate_id,
               correction_item.correction_request_id
        FROM public.pay_payment_correction_items AS correction_item
        JOIN page_keys ON page_keys.pay_batch_candidate_id = correction_item.pay_batch_candidate_id
        JOIN public.pay_payment_correction_requests AS request_row
          ON request_row.id = correction_item.correction_request_id
         AND request_row.pay_batch_id = correction_item.pay_batch_id
        WHERE correction_item.pay_batch_id = p_pay_batch_id
          AND correction_item.status = 'APPLIED'
          AND correction_item.correction_item_kind IN (
              'PRE_BANK_CANCEL',
              'NO_MONEY_UNWIND'
          )
        ORDER BY correction_item.pay_batch_candidate_id,
                 correction_item.applied_at_utc DESC NULLS LAST,
                 request_row.updated_at_utc DESC,
                 correction_item.correction_request_id DESC
    ), removed_amount_rollup AS (
        SELECT latest_removal.pay_batch_candidate_id,
               pg_catalog.sum(correction_item.source_amount) AS removed_frozen_source_amount,
               pg_catalog.sum(correction_item.amount_inc_vat) AS removed_frozen_payable_amount,
               pg_catalog.max(member_row.active_amount) AS removed_reviewed_payment_amount
        FROM latest_removal_request AS latest_removal
        JOIN public.pay_payment_correction_items AS correction_item
          ON correction_item.correction_request_id = latest_removal.correction_request_id
         AND correction_item.pay_batch_candidate_id = latest_removal.pay_batch_candidate_id
         AND correction_item.status = 'APPLIED'
         AND correction_item.correction_item_kind IN (
             'PRE_BANK_CANCEL',
             'NO_MONEY_UNWIND'
         )
        LEFT JOIN public.pay_payment_correction_request_candidates AS member_row
          ON member_row.correction_request_id = latest_removal.correction_request_id
         AND member_row.pay_batch_candidate_id = latest_removal.pay_batch_candidate_id
        GROUP BY latest_removal.pay_batch_candidate_id
    ), work_rollup AS (
        SELECT work_item.pay_batch_candidate_id,
               (pg_catalog.array_agg(work_item.status ORDER BY work_item.created_at_utc DESC, work_item.id DESC))[1]
                   AS latest_work_status,
               pg_catalog.max(work_item.attempt_count)::integer AS attempt_count,
               (pg_catalog.array_agg(work_item.last_error ORDER BY work_item.created_at_utc DESC, work_item.id DESC))[1]
                   AS last_error,
               (pg_catalog.array_agg(request_row.status ORDER BY work_item.created_at_utc DESC, work_item.id DESC))[1]
                   AS latest_request_status
        FROM public.pay_payment_correction_work_items AS work_item
        JOIN page_keys ON page_keys.pay_batch_candidate_id = work_item.pay_batch_candidate_id
        LEFT JOIN public.pay_payment_correction_requests AS request_row
          ON request_row.id = work_item.correction_request_id
        WHERE work_item.pay_batch_id = p_pay_batch_id
        GROUP BY work_item.pay_batch_candidate_id
    ), membership_history AS (
        SELECT DISTINCT ON (member_row.pay_batch_candidate_id)
               member_row.pay_batch_candidate_id,
               member_row.active_amount AS reviewed_payment_amount,
               request_row.status AS membership_request_status
        FROM public.pay_payment_correction_request_candidates AS member_row
        JOIN page_keys ON page_keys.pay_batch_candidate_id = member_row.pay_batch_candidate_id
        JOIN public.pay_payment_correction_requests AS request_row
          ON request_row.id = member_row.correction_request_id
        WHERE request_row.pay_batch_id = p_pay_batch_id
        ORDER BY
            member_row.pay_batch_candidate_id,
            CASE
                WHEN request_row.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS') THEN 0
                WHEN request_row.status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED') THEN 1
                ELSE 2
            END,
            request_row.updated_at_utc DESC,
            request_row.id DESC
    ), provider_rollup AS (
        SELECT item_candidate.id AS pay_batch_candidate_id,
               pg_catalog.bool_or(
                   transfer_event.normalised_state IN ('COMPLETED', 'PAID', 'SETTLED')
               ) AS has_paid_evidence,
               pg_catalog.bool_or(
                   transfer_event.normalised_state IN (
                       'FAILED',
                       'REJECTED',
                       'CANCELLED',
                       'CONFIRMED_NOT_PAID'
                   )
               ) AS has_terminal_no_money,
               pg_catalog.bool_or(
                   transfer_event.normalised_state IN (
                       'UNKNOWN',
                       'PENDING',
                       'SUBMITTED',
                       'PROCESSING'
                   )
                   OR transfer_event.movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
               ) AS has_ambiguous_evidence,
               pg_catalog.max(transfer_event.event_time_utc) AS latest_event_time_utc
        FROM public.pay_batch_candidates AS item_candidate
        JOIN page_keys ON page_keys.pay_batch_candidate_id = item_candidate.id
        JOIN public.pay_batch_items AS provider_item
          ON provider_item.pay_batch_candidate_id = item_candidate.id
        LEFT JOIN public.pay_bank_transfer_events AS transfer_event
          ON transfer_event.pay_bank_transfer_id = provider_item.pay_bank_transfer_id
        WHERE item_candidate.pay_batch_id = p_pay_batch_id
        GROUP BY item_candidate.id
    ), transfer_rollup AS (
        SELECT item_candidate.id AS pay_batch_candidate_id,
               pg_catalog.bool_or(
                   bank_transfer.status IN ('FAILED', 'CANCELLED')
                   OR bank_transfer.rail_state IN ('FAILED', 'CANCELLED')
               ) AS transfer_failed,
               pg_catalog.bool_or(
                   bank_transfer.status IN ('SUBMITTED', 'PROCESSING', 'PENDING')
                   OR bank_transfer.rail_state IN ('SUBMITTED', 'PROCESSING', 'PENDING')
               ) AS transfer_pending,
               pg_catalog.count(DISTINCT bank_transfer.transfer_group_key)
                   FILTER (WHERE bank_transfer.transfer_group_key IS NOT NULL)::integer
                   AS shared_instruction_count
        FROM public.pay_batch_candidates AS item_candidate
        JOIN page_keys ON page_keys.pay_batch_candidate_id = item_candidate.id
        JOIN public.pay_batch_items AS transfer_item
          ON transfer_item.pay_batch_candidate_id = item_candidate.id
        LEFT JOIN public.pay_bank_transfers AS bank_transfer
          ON bank_transfer.id = transfer_item.pay_bank_transfer_id
        WHERE item_candidate.pay_batch_id = p_pay_batch_id
        GROUP BY item_candidate.id
    ), base AS (
        SELECT candidate_row.id AS pay_batch_candidate_id,
               candidate_row.candidate_id,
               candidate_row.candidate_display_name,
               candidate_row.net_bank_amount,
               pg_catalog.round(
                   coalesce(
                       CASE
                           WHEN coalesce(correction_rollup.has_applied_removal, false)
                             OR coalesce(item_rollup.active_item_count, 0) = 0
                           THEN membership_history.reviewed_payment_amount
                           ELSE NULL::numeric
                       END,
                       status_index.reviewed_payment_amount,
                       0
                   ) * 100
               )::bigint
                   AS original_payment_amount_pence,
               CASE
                   WHEN coalesce(correction_rollup.has_applied_removal, false)
                     OR coalesce(item_rollup.active_item_count, 0) = 0
                   THEN pg_catalog.round(
                       coalesce(removed_amount_rollup.removed_frozen_source_amount, 0) * 100
                   )::bigint
                   ELSE NULL::bigint
               END AS cancelled_gross_base_amount_pence,
               CASE
                   WHEN coalesce(correction_rollup.has_applied_removal, false)
                     OR coalesce(item_rollup.active_item_count, 0) = 0
                   THEN pg_catalog.round(
                       coalesce(
                           removed_amount_rollup.removed_frozen_payable_amount,
                           removed_amount_rollup.removed_reviewed_payment_amount,
                           0
                       ) * 100
                   )::bigint
                   ELSE NULL::bigint
               END AS cancelled_payable_amount_pence,
               CASE
                   WHEN coalesce(correction_rollup.has_applied_removal, false)
                     OR coalesce(item_rollup.active_item_count, 0) = 0
                   THEN pg_catalog.round(
                       coalesce(
                           removed_amount_rollup.removed_reviewed_payment_amount,
                           membership_history.reviewed_payment_amount,
                           candidate_row.net_bank_amount,
                           0
                       ) * 100
                   )::bigint
                   ELSE NULL::bigint
               END AS cancelled_bank_amount_pence,
               coalesce(item_rollup.active_item_count, 0) AS active_item_count,
               coalesce(item_rollup.has_paye_item, false) AS has_paye_item,
               coalesce(item_rollup.has_non_paye_item, false) AS has_non_paye_item,
               CASE
                   WHEN coalesce(item_rollup.has_paye_item, false)
                    AND coalesce(item_rollup.has_non_paye_item, false) THEN 'MIXED'
                   WHEN coalesce(item_rollup.has_paye_item, false) THEN 'PAYE'
                   ELSE 'UMBRELLA'
               END AS pay_channel,
               coalesce(correction_rollup.has_applied_removal, false)
                   OR coalesce(item_rollup.active_item_count, 0) = 0 AS removed,
               coalesce(correction_rollup.has_applied_release, false) AS released,
               coalesce(correction_rollup.has_applied_cancel, false) AS cancelled,
                status_index.paid_or_settled,
                status_index.terminal_no_money,
                status_index.ambiguous,
                status_index.has_resolution_context,
                status_index.canonical_provider_state,
                status_index.complete_candidate_instruction_scope,
                status_index.release_failed_payment_eligible,
                 status_index.pre_provider_cancel_eligible,
               status_index.latest_work_status,
               coalesce(work_rollup.attempt_count, 0) AS attempt_count,
               work_rollup.last_error,
               coalesce(
                   work_rollup.latest_request_status,
                   membership_history.membership_request_status
               ) AS latest_request_status,
               provider_rollup.latest_event_time_utc,
               coalesce(transfer_rollup.shared_instruction_count, 0) AS shared_instruction_count
         FROM public.pay_batch_candidates AS candidate_row
         JOIN page_keys ON page_keys.pay_batch_candidate_id = candidate_row.id
         JOIN candidate_classified_index AS status_index
           ON status_index.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN item_rollup ON item_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN correction_rollup ON correction_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN removed_amount_rollup ON removed_amount_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN work_rollup ON work_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN membership_history ON membership_history.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN provider_rollup ON provider_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN transfer_rollup ON transfer_rollup.pay_batch_candidate_id = candidate_row.id
        WHERE candidate_row.pay_batch_id = p_pay_batch_id
    ), classified AS (
        SELECT base.*,
               CASE
                   WHEN base.removed AND base.released THEN 'RELEASED'
                   WHEN base.removed THEN 'CANCELLED'
                    WHEN base.paid_or_settled THEN 'SETTLED'
                    WHEN base.latest_work_status = 'BLOCKED' THEN 'BLOCKED'
                    WHEN base.latest_work_status IN ('FAILED_FINAL', 'FAILED_RETRYABLE') THEN 'FAILED'
                    WHEN base.canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'BLOCKED'
                     WHEN base.release_failed_payment_eligible THEN 'NOT_PAID'
                    WHEN base.ambiguous THEN 'AMBIGUOUS'
                    WHEN base.terminal_no_money THEN 'BLOCKED'
                    ELSE 'ACTIVE'
                END AS payment_display_state,
                  CASE
                      WHEN base.removed OR base.paid_or_settled THEN ARRAY[]::text[]
                      WHEN v_batch_terminal THEN ARRAY[]::text[]
                      WHEN base.latest_work_status IN ('BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE') THEN ARRAY[]::text[]
                      WHEN base.canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN ARRAY[]::text[]
                      WHEN v_batch.status = 'DRAFT' AND base.pre_provider_cancel_eligible
                        THEN ARRAY['DRAFT_CANCEL']::text[]
                      WHEN v_batch.status = 'DRAFT' THEN ARRAY[]::text[]
                     WHEN base.release_failed_payment_eligible THEN ARRAY['RELEASE_FAILED_PAYMENT']::text[]
                    WHEN base.ambiguous AND base.has_resolution_context THEN ARRAY['RESOLVE_PAYMENT_STATUS']::text[]
                    WHEN base.ambiguous THEN ARRAY[]::text[]
                    WHEN base.terminal_no_money THEN ARRAY[]::text[]
                    WHEN base.pre_provider_cancel_eligible THEN ARRAY['CANCEL_PAYMENT']::text[]
                    ELSE ARRAY[]::text[]
               END AS available_actions,
               CASE
                   WHEN base.removed OR base.paid_or_settled THEN 0::bigint
                   ELSE base.original_payment_amount_pence
               END AS active_payment_amount_pence,
               CASE
                    WHEN base.removed THEN 70
                    WHEN base.paid_or_settled THEN 60
                    WHEN base.latest_work_status = 'BLOCKED' THEN 50
                    WHEN base.canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 50
                     WHEN base.release_failed_payment_eligible THEN 40
                    WHEN base.ambiguous THEN 30
                    WHEN base.terminal_no_money THEN 50
                    ELSE 10
               END AS status_rank
        FROM base
    ), filtered AS (
        SELECT classified.*
        FROM classified
        WHERE (
                NULLIF(pg_catalog.btrim(v_filter ->> 'search'), '') IS NULL
                OR pg_catalog.lower(classified.candidate_display_name)
                    LIKE '%' || pg_catalog.lower(pg_catalog.btrim(v_filter ->> 'search')) || '%'
              )
          AND (
                NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'status')), '') IS NULL
                OR classified.payment_display_state = pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'status'))
              )
          AND (
                NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'action')), '') IS NULL
                OR pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'action')) = ANY(classified.available_actions)
              )
          AND (
                coalesce((v_filter ->> 'actionable_only')::boolean, false) IS NOT TRUE
                OR pg_catalog.cardinality(classified.available_actions) > 0
              )
          AND (
                NULLIF(pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'pay_channel')), '') IS NULL
                OR (
                    pg_catalog.upper(pg_catalog.btrim(v_filter ->> 'pay_channel')) = 'PAYE'
                    AND classified.has_paye_item
                )
              )
          AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.jsonb_array_elements_text(
                    coalesce(v_filter -> 'excluded_candidate_tokens', '[]'::jsonb)
                ) AS excluded_token(token_value)
                WHERE excluded_token.token_value = classified.pay_batch_candidate_id::text
              )
          AND (
                NOT (v_filter ? 'included_candidate_tokens')
                OR EXISTS (
                    SELECT 1
                    FROM pg_catalog.jsonb_array_elements_text(
                        v_filter -> 'included_candidate_tokens'
                    ) AS included_token(token_value)
                    WHERE included_token.token_value = classified.pay_batch_candidate_id::text
                )
              )
    ), after_cursor AS (
        SELECT filtered.*
        FROM filtered
        WHERE p_cursor_json IS NULL
           OR (
               v_sort_key = 'STATUS'
               AND (
                   (v_sort_direction = 'ASC' AND (
                       filtered.status_rank,
                       pg_catalog.lower(filtered.candidate_display_name),
                       filtered.pay_batch_candidate_id
                   ) > (
                       (p_cursor_json ->> 'last_status_rank')::integer,
                       p_cursor_json ->> 'last_candidate_name',
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
                   OR (v_sort_direction = 'DESC' AND (
                       filtered.status_rank,
                       pg_catalog.lower(filtered.candidate_display_name),
                       filtered.pay_batch_candidate_id
                   ) < (
                       (p_cursor_json ->> 'last_status_rank')::integer,
                       p_cursor_json ->> 'last_candidate_name',
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
               )
           )
           OR (
               p_cursor_json IS NOT NULL
               AND v_sort_key = 'CANDIDATE'
               AND (
                   (v_sort_direction = 'ASC' AND (
                       pg_catalog.lower(filtered.candidate_display_name),
                       filtered.pay_batch_candidate_id
                   ) > (
                       p_cursor_json ->> 'last_candidate_name',
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
                   OR (v_sort_direction = 'DESC' AND (
                       pg_catalog.lower(filtered.candidate_display_name),
                       filtered.pay_batch_candidate_id
                   ) < (
                       p_cursor_json ->> 'last_candidate_name',
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
               )
           )
           OR (
               p_cursor_json IS NOT NULL
               AND v_sort_key = 'AMOUNT'
               AND (
                   (v_sort_direction = 'ASC' AND (
                       filtered.original_payment_amount_pence,
                       filtered.pay_batch_candidate_id
                   ) > (
                       (p_cursor_json ->> 'last_amount_pence')::bigint,
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
                   OR (v_sort_direction = 'DESC' AND (
                       filtered.original_payment_amount_pence,
                       filtered.pay_batch_candidate_id
                   ) < (
                       (p_cursor_json ->> 'last_amount_pence')::bigint,
                       (p_cursor_json ->> 'last_pay_batch_candidate_id')::uuid
                   ))
               )
           )
    ), paged AS (
        SELECT after_cursor.*
        FROM after_cursor
        ORDER BY
            CASE WHEN v_sort_key = 'STATUS' AND v_sort_direction = 'ASC' THEN after_cursor.status_rank END ASC,
            CASE WHEN v_sort_key = 'STATUS' AND v_sort_direction = 'DESC' THEN after_cursor.status_rank END DESC,
            CASE WHEN v_sort_key IN ('STATUS', 'CANDIDATE') AND v_sort_direction = 'ASC'
                THEN pg_catalog.lower(after_cursor.candidate_display_name) END ASC,
            CASE WHEN v_sort_key IN ('STATUS', 'CANDIDATE') AND v_sort_direction = 'DESC'
                THEN pg_catalog.lower(after_cursor.candidate_display_name) END DESC,
            CASE WHEN v_sort_key = 'AMOUNT' AND v_sort_direction = 'ASC'
                THEN after_cursor.original_payment_amount_pence END ASC,
            CASE WHEN v_sort_key = 'AMOUNT' AND v_sort_direction = 'DESC'
                THEN after_cursor.original_payment_amount_pence END DESC,
            CASE WHEN v_sort_direction = 'ASC' THEN after_cursor.pay_batch_candidate_id END ASC,
            CASE WHEN v_sort_direction = 'DESC' THEN after_cursor.pay_batch_candidate_id END DESC
        LIMIT p_limit + 1
    ), page_rows AS (
        SELECT paged.*,
               pg_catalog.row_number() OVER () AS page_ordinal
        FROM paged
        LIMIT p_limit
    )
    SELECT
        coalesce(
            pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object(
                    'row_key', page_rows.pay_batch_candidate_id::text,
                    'candidate_token', page_rows.pay_batch_candidate_id::text,
                    'candidate/payee_display', page_rows.candidate_display_name,
                    'pay_channel', page_rows.pay_channel,
                    'original_amount', (page_rows.original_payment_amount_pence::numeric / 100)::numeric(14,2),
                    'active_amount', (page_rows.active_payment_amount_pence::numeric / 100)::numeric(14,2),
                    'display_status', page_rows.payment_display_state,
                    'is_active', NOT page_rows.removed AND NOT page_rows.paid_or_settled,
                    'is_cancelled', page_rows.removed AND NOT page_rows.released,
                    'is_paid', page_rows.paid_or_settled,
                    'is_settled', page_rows.paid_or_settled,
                    'is_not_paid', page_rows.release_failed_payment_eligible AND NOT page_rows.removed,
                    'is_released', page_rows.removed AND page_rows.released,
                    'eligible_action_codes', page_rows.available_actions,
                    'plain_blocker', CASE
                        WHEN page_rows.paid_or_settled THEN 'Paid or settled payments cannot be cancelled.'
                        WHEN page_rows.payment_display_state = 'AMBIGUOUS' THEN 'The bank payment status must be resolved before continuing.'
                        WHEN page_rows.latest_work_status = 'BLOCKED' THEN 'This payment could not be changed because its status or source ownership changed.'
                        WHEN page_rows.latest_work_status IN ('FAILED_FINAL', 'FAILED_RETRYABLE') THEN 'CloudTMS could not complete this payment change.'
                        WHEN page_rows.canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER'
                            THEN 'The bank is temporarily unavailable. This payment cannot be changed until provider status is rechecked.'
                        WHEN page_rows.terminal_no_money AND page_rows.release_failed_payment_eligible IS NOT TRUE
                            THEN 'This failed payment cannot be released until its complete payment scope and evidence are safe.'
                        WHEN page_rows.payment_display_state = 'ACTIVE'
                             AND v_batch.status <> 'DRAFT'
                             AND page_rows.pre_provider_cancel_eligible IS NOT TRUE
                            THEN 'This payment cannot be cancelled until its complete payment scope and evidence are safe.'
                        ELSE NULL::text
                    END,
                    'correction_request_status', page_rows.latest_request_status,
                    'correction_work_status', page_rows.latest_work_status,
                    'progress_display', CASE
                        WHEN page_rows.latest_work_status IN ('PENDING', 'PROCESSING', 'FAILED_RETRYABLE') THEN 'Cancellation is in progress'
                        WHEN page_rows.payment_display_state = 'CANCELLED' THEN 'Cancellation complete'
                        WHEN page_rows.payment_display_state = 'RELEASED' THEN 'Release complete'
                        ELSE NULL::text
                    END,
                    'snapshot_token', v_snapshot_token,
                    'resolution_context', CASE
                        WHEN 'RESOLVE_PAYMENT_STATUS' = ANY(page_rows.available_actions) THEN
                            pg_catalog.jsonb_build_object(
                                'version', 1,
                                'candidate_token', page_rows.pay_batch_candidate_id::text,
                                'active_batch_scope_hash', v_active_batch_scope_hash,
                                'context_token', private.pay_payment_correction_sha256_v1(
                                    pg_catalog.jsonb_build_object(
                                        'version', 1,
                                        'pay_batch_id', p_pay_batch_id,
                                        'candidate_token', page_rows.pay_batch_candidate_id,
                                        'active_batch_scope_hash', v_active_batch_scope_hash
                                    )
                                )
                            )
                        ELSE NULL::jsonb
                    END,
                    'stable_sort_cursor', pg_catalog.jsonb_build_object(
                        'status_rank', page_rows.status_rank,
                        'candidate_name', pg_catalog.lower(page_rows.candidate_display_name),
                        'amount_pence', page_rows.original_payment_amount_pence,
                        'pay_batch_candidate_id', page_rows.pay_batch_candidate_id
                    ),
                    'pay_batch_candidate_id', page_rows.pay_batch_candidate_id,
                    'selection_token', page_rows.pay_batch_candidate_id::text,
                    'candidate_id', page_rows.candidate_id,
                    'candidate_display_name', page_rows.candidate_display_name,
                    'payment_display_state', page_rows.payment_display_state,
                    'release_failed_payment_eligible', page_rows.release_failed_payment_eligible,
                    'pre_provider_cancel_eligible', page_rows.pre_provider_cancel_eligible,
                    'draft_cancel_eligible', v_batch.status = 'DRAFT' AND page_rows.pre_provider_cancel_eligible,
                    'complete_candidate_instruction_scope', page_rows.complete_candidate_instruction_scope,
                    'available_actions', page_rows.available_actions,
                    'original_payment_amount_pence', page_rows.original_payment_amount_pence,
                    'active_payment_amount_pence', page_rows.active_payment_amount_pence,
                    'cancelled_gross_base_amount_pence', page_rows.cancelled_gross_base_amount_pence,
                    'cancelled_payable_amount_pence', page_rows.cancelled_payable_amount_pence,
                    'cancelled_bank_amount_pence', page_rows.cancelled_bank_amount_pence,
                    'include_in_active_overview', NOT page_rows.removed,
                    'include_in_active_paye_schedule', page_rows.has_paye_item AND NOT page_rows.removed,
                    'active_item_count', page_rows.active_item_count,
                    'work_status', page_rows.latest_work_status,
                    'attempt_count', page_rows.attempt_count,
                    'failure_reason', CASE
                        WHEN page_rows.latest_work_status IN ('BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE')
                            THEN CASE
                                WHEN page_rows.latest_work_status = 'BLOCKED' THEN 'This payment could not be changed because its status or source ownership changed.'
                                ELSE 'CloudTMS could not complete this payment change.'
                            END
                        ELSE NULL
                    END,
                    'shared_instruction', page_rows.shared_instruction_count > 0,
                    'latest_evidence_at_utc', page_rows.latest_event_time_utc
                )
                ORDER BY page_rows.page_ordinal
            ),
            '[]'::jsonb
        ),
        pg_catalog.count(*)::integer,
        (
            SELECT pg_catalog.count(*)::integer
            FROM candidate_filtered_index
        ),
        (
            SELECT pg_catalog.count(*)::integer
            FROM candidate_filtered_index
            WHERE pg_catalog.cardinality(candidate_filtered_index.available_actions) > 0
        ),
        (
            SELECT coalesce(pg_catalog.sum(candidate_filtered_index.original_payment_amount_pence), 0)::bigint
            FROM candidate_filtered_index
            WHERE pg_catalog.cardinality(candidate_filtered_index.available_actions) > 0
        ),
        (
            SELECT pg_catalog.count(*)::integer
            FROM candidate_classified_index
            WHERE NOT candidate_classified_index.removed
        ),
        (
            SELECT coalesce(pg_catalog.sum(
                CASE WHEN candidate_classified_index.removed
                     THEN 0::bigint
                     ELSE candidate_classified_index.original_payment_amount_pence END
            ), 0)::bigint
            FROM candidate_classified_index
        ),
        (
            SELECT coalesce(pg_catalog.sum(candidate_classified_index.original_payment_amount_pence), 0)::bigint
            FROM candidate_classified_index
        ),
        (
            SELECT pg_catalog.count(*)::integer
            FROM candidate_classified_index
            WHERE candidate_classified_index.has_paye_item
              AND NOT candidate_classified_index.removed
        ),
        (
            SELECT coalesce(pg_catalog.sum(candidate_classified_index.original_payment_amount_pence), 0)::bigint
            FROM candidate_classified_index
            WHERE candidate_classified_index.has_paye_item
              AND NOT candidate_classified_index.removed
        ),
        (
            SELECT pg_catalog.to_jsonb(last_page_row)
            FROM page_rows AS last_page_row
            ORDER BY last_page_row.page_ordinal DESC
            LIMIT 1
        ),
        (
            SELECT pg_catalog.count(*) > p_limit
            FROM paged
        )
    INTO
        v_rows,
        v_row_count,
        v_total_matching_count,
        v_eligible_matching_count,
        v_selected_amount_pence_available,
        v_active_overview_candidate_count,
        v_active_overview_amount_pence,
        v_original_overview_amount_pence,
        v_active_paye_schedule_line_count,
        v_active_paye_schedule_amount_pence,
        v_last_row,
        v_has_more
    FROM page_rows;

    IF v_last_row IS NOT NULL AND v_has_more THEN
        v_next_cursor_json := pg_catalog.jsonb_build_object(
            'snapshot_token', v_snapshot_token,
            'sort_key', v_sort_key,
            'sort_direction', v_sort_direction,
            'last_status_rank', (v_last_row ->> 'status_rank')::integer,
            'last_candidate_name', pg_catalog.lower(v_last_row ->> 'candidate_display_name'),
            'last_amount_pence', (v_last_row ->> 'original_payment_amount_pence')::bigint,
            'last_pay_batch_candidate_id', v_last_row ->> 'pay_batch_candidate_id',
            'previous_cursor_json', p_cursor_json
        );
    END IF;

    v_previous_cursor_json := CASE
        WHEN p_cursor_json IS NULL THEN NULL
        ELSE p_cursor_json -> 'previous_cursor_json'
    END;

    SELECT pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'id', request_row.id,
        'correction_request_id', request_row.id,
        'status', request_row.status,
        'request_status', request_row.status,
        'requested_at_utc', request_row.requested_at_utc,
        'updated_at_utc', request_row.updated_at_utc,
        'is_active', request_row.status IN (
            'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
            'AUTHORISED', 'EXPANDED', 'PROCESSING'
        ),
        'is_terminal', request_row.status IN (
            'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED',
            'REJECTED', 'CANCELLED'
        ),
        'user_title', CASE
            WHEN request_row.status IN (
                'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
                'AUTHORISED', 'EXPANDED', 'PROCESSING'
            ) THEN 'Payment cancellation in progress'
            ELSE 'Latest payment cancellation'
        END,
        'request_kind', COALESCE(
            NULLIF(request_row.selection_json->>'requested_action', ''),
            NULLIF(request_row.plan_json->>'requested_action', ''),
            request_row.correction_kind
        )
    ))
    INTO v_latest_correction_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.pay_batch_id = p_pay_batch_id
    ORDER BY
        CASE
            WHEN request_row.status IN (
                'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
                'AUTHORISED', 'EXPANDED', 'PROCESSING'
            ) THEN 0
            ELSE 1
        END,
        request_row.updated_at_utc DESC,
        request_row.id DESC
    LIMIT 1;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'pay_batch_id', p_pay_batch_id,
        'snapshot_token', v_snapshot_token,
        'explicit_snapshot_token', v_explicit_snapshot_token,
        'active_batch_scope_hash', v_active_batch_scope_hash,
        'sort_key', v_sort_key,
        'sort_direction', v_sort_direction,
        'page_size', p_limit,
        'row_count', v_row_count,
        'has_more', v_has_more,
        'total_matching_count', v_total_matching_count,
        'eligible_matching_count', v_eligible_matching_count,
        'selected_amount_pence_available', v_selected_amount_pence_available,
        'active_overview_candidate_count', v_active_overview_candidate_count,
        'active_overview_amount_pence', v_active_overview_amount_pence,
        'original_overview_amount_pence', v_original_overview_amount_pence,
        'active_paye_schedule_line_count', v_active_paye_schedule_line_count,
        'active_paye_schedule_amount_pence', v_active_paye_schedule_amount_pence,
        'latest_correction_request', v_latest_correction_request,
        'latest_correction_request_id', v_latest_correction_request->>'id',
        'rows', v_rows,
        'next_cursor_json', v_next_cursor_json,
        'previous_cursor_json', v_previous_cursor_json,
        'page_label', CASE
            WHEN v_total_matching_count = 0 THEN '0 of 0'
            ELSE 'Showing ' || v_row_count::text || ' of ' || v_total_matching_count::text
        END,
        'code', 'PAYMENT_STATUS_PAGE_OK',
        'message', NULL,
        'continuation', pg_catalog.jsonb_build_object(
            'required', false,
            'operation_id', NULL,
            'operation_type', NULL,
            'pay_batch_id', p_pay_batch_id,
            'root_operation_id', NULL,
            'phase', NULL,
            'run_after_utc', NULL,
            'reason', 'STATUS_READ_ONLY',
            'successor_relation', 'NONE',
            'requires_user_action', false,
            'terminal', true
        )
    );
END
$function$;

ALTER FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) TO service_role;
