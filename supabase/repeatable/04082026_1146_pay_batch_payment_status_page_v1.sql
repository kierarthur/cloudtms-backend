-- CloudTMS Banking Pay cancellation â€” Stage 1.
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
    v_next_cursor_json jsonb := NULL;
    v_previous_cursor_json jsonb := NULL;
    v_last_row jsonb;
    v_latest_correction_request jsonb := NULL;
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
ÛÍxòÚ$z{-®éÜj×FFUö–Br“£§WV–@¢’¢¢¢’ÂvVB2€¢4TÄT5BgFW%ö7W'6÷"â ¢e$ôÒgFW%ö7W'6÷ ¢õ$DU"%¢44Rt„Tâe÷6÷'Eö¶W’Òu5DEU2räBe÷6÷'EöF—&V7F–öâÒt42rD„TâgFW%ö7W'6÷"ç7FGW5÷&æ²TäB42À¢44Rt„Tâe÷6÷'Eö¶W’Òu5DEU2räBe÷6÷'EöF—&V7F–öâÒtDU42rD„TâgFW%ö7W'6÷"ç7FGW5÷&æ²TäBDU42À¢44Rt„Tâe÷6÷'Eö¶W’”â‚u5DEU2rÂt4äD”DDRr’äBe÷6÷'EöF—&V7F–öâÒt42p¢D„Tâuö6FÆöræÆ÷vW"†gFW%ö7W'6÷"æ6æF–FFUöF—7Æ•öæÖR’TäB42À¢44Rt„Tâe÷6÷'Eö¶W’”â‚u5DEU2rÂt4äD”DDRr’äBe÷6÷'EöF—&V7F–öâÒtDU42p¢D„Tâuö6FÆöræÆ÷vW"†gFW%ö7W'6÷"æ6æF–FFUöF—7Æ•öæÖR’TäBDU42À¢44Rt„Tâe÷6÷'Eö¶W’ÒtÔõTåBräBe÷6÷'EöF—&V7F–öâÒt42p¢D„TâgFW%ö7W'6÷"æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RTäB42À¢44Rt„Tâe÷6÷'Eö¶W’ÒtÔõTåBräBe÷6÷'EöF—&V7F–öâÒtDU42p¢D„TâgFW%ö7W'6÷"æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RTäBDU42À¢44Rt„Tâe÷6÷'EöF—&V7F–öâÒt42rD„TâgFW%ö7W'6÷"ç•ö&F6…ö6æF–FFUö–BTäB42À¢44Rt„Tâe÷6÷'EöF—&V7F–öâÒtDU42rD„TâgFW%ö7W'6÷"ç•ö&F6…ö6æF–FFUö–BTäBDU40¢Ä”Ô•BöÆ–Ö—B²¢’ÂvU÷&÷w22€¢4TÄT5BvVBâ¢À¢uö6FÆörç&÷uöçVÖ&W"‚’õdU"‚’2vUö÷&F–æÀ¢e$ôÒvV@¢Ä”Ô•BöÆ–Ö—@¢¢4TÄT5@¢6öÆW66R€¢uö6FÆöræ§6öæ%övr€¢uö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢w&÷uö¶W’rÂvU÷&÷w2ç•ö&F6…ö6æF–FFUö–C£§FW‡BÀ¢v6æF–FFU÷Fö¶VârÂvU÷&÷w2ç•ö&F6…ö6æF–FFUö–C£§FW‡BÀ¢v6æF–FFR÷–VUöF—7Æ’rÂvU÷&÷w2æ6æF–FFUöF—7Æ•öæÖRÀ¢w•ö6†ææVÂrÂvU÷&÷w2ç•ö6†ææVÂÀ¢v÷&–v–æÅöÖ÷VçBrÂ‡vU÷&÷w2æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6S£¦çVÖW&–2ò“£¦çVÖW&–2ƒBÃ"’À¢v7F—fUöÖ÷VçBrÂ‡vU÷&÷w2æ7F—fU÷–ÖVçEöÖ÷VçE÷Væ6S£¦çVÖW&–2ò“£¦çVÖW&–2ƒBÃ"’À¢vF—7Æ•÷7FGW2rÂvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÀ¢v—5ö7F—fRrÂäõBvU÷&÷w2ç&VÖ÷fVBäBäõBvU÷&÷w2ç–Eö÷%÷6WGFÆVBÀ¢v—5ö6æ6VÆÆVBrÂvU÷&÷w2ç&VÖ÷fVBäBäõBvU÷&÷w2ç&VÆV6VBÀ¢v—5÷–BrÂvU÷&÷w2ç–Eö÷%÷6WGFÆVBÀ¢v—5÷6WGFÆVBrÂvU÷&÷w2ç–Eö÷%÷6WGFÆVBÀ¢v—5öæ÷E÷–BrÂvU÷&÷w2ç&VÆV6Uöf–ÆVE÷–ÖVçEöVÆ–v–&ÆRäBäõBvU÷&÷w2ç&VÖ÷fVBÀ¢v—5÷&VÆV6VBrÂvU÷&÷w2ç&VÖ÷fVBäBvU÷&÷w2ç&VÆV6VBÀ¢vVÆ–v–&ÆUö7F–öåö6öFW2rÂvU÷&÷w2æf–Æ&ÆUö7F–öç2À¢wÆ–åö&Æö6¶W"rÂ44P¢t„TâvU÷&÷w2ç–Eö÷%÷6WGFÆVBD„Tâu–B÷"6WGFÆVB–ÖVçG26ææ÷B&R6æ6VÆÆVBâp¢t„TâvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÒtÔ$”uTõU2rD„TâuF†R&æ²–ÖVçB7FGW2×W7B&R&W6öÇfVB&Vf÷&R6öçF–çV–ærâp¢t„TâvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2Òt$Äô4´TBrD„TâuF†—2–ÖVçB6÷VÆBæ÷B&R6†ævVB&V6W6R—G27FGW2÷"6÷W&6R÷væW'6†—6†ævVBâp¢t„TâvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2”â‚td”ÄTEôd”äÂrÂtd”ÄTEõ$UE%”$ÄRr’D„Tât6Æ÷VEDÕ26÷VÆBæ÷B6ö×ÆWFRF†—2–ÖVçB6†ævRâp¢t„TâvU÷&÷w2æ6æöæ–6Å÷&÷f–FW%÷7FFRÒu$õd”DU%ôõUDtUõ$UE%•ôÄDU"p¢D„TâuF†R&æ²—2FV×÷&&–Ç’Væf–Æ&ÆRâF†—2–ÖVçB6ææ÷B&R6†ævVBVçF–Â&÷f–FW"7FGW2—2&V6†V6¶VBâp¢t„TâvU÷&÷w2çFW&Ö–æÅöæõöÖöæW’äBvU÷&÷w2ç&VÆV6Uöf–ÆVE÷–ÖVçEöVÆ–v–&ÆR•2äõBE%TP¢D„TâuF†—2f–ÆVB–ÖVçB6ææ÷B&R&VÆV6VBVçF–Â—G26ö×ÆWFR–ÖVçB66÷RæBWf–FVæ6R&R6fRâp¢t„TâvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÒt5D•dRp¢äBeö&F6‚ç7FGW2ÃâtE$eBp¢äBvU÷&÷w2ç&U÷&÷f–FW%ö6æ6VÅöVÆ–v–&ÆR•2äõBE%TP¢D„TâuF†—2–ÖVçB6ææ÷B&R6æ6VÆÆVBVçF–Â—G26ö×ÆWFR–ÖVçB66÷RæBWf–FVæ6R&R6fRâp¢TÅ4RåTÄÃ£§FW‡@¢TäBÀ¢v6÷'&V7F–öå÷&WVW7E÷7FGW2rÂvU÷&÷w2æÆFW7E÷&WVW7E÷7FGW2À¢v6÷'&V7F–öå÷v÷&µ÷7FGW2rÂvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2À¢w&öw&W75öF—7Æ’rÂ44P¢t„TâvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2”â‚uTäD”ärrÂu$ô4U54”ärrÂtd”ÄTEõ$UE%”$ÄRr’D„Tât6æ6VÆÆF–öâ—2–â&öw&W72p¢t„TâvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÒt4ä4TÄÄTBrD„Tât6æ6VÆÆF–öâ6ö×ÆWFRp¢t„TâvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÒu$TÄT4TBrD„Tâu&VÆV6R6ö×ÆWFRp¢TÅ4RåTÄÃ£§FW‡@¢TäBÀ¢w6æ6†÷E÷Fö¶VârÂe÷6æ6†÷E÷Fö¶VâÀ¢w7F&ÆU÷6÷'Eö7W'6÷"rÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢w7FGW5÷&æ²rÂvU÷&÷w2ç7FGW5÷&æ²À¢v6æF–FFUöæÖRrÂuö6FÆöræÆ÷vW"‡vU÷&÷w2æ6æF–FFUöF—7Æ•öæÖR’À¢vÖ÷VçE÷Væ6RrÂvU÷&÷w2æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RÀ¢w•ö&F6…ö6æF–FFUö–BrÂvU÷&÷w2ç•ö&F6…ö6æF–FFUö–@¢’À¢w•ö&F6…ö6æF–FFUö–BrÂvU÷&÷w2ç•ö&F6…ö6æF–FFUö–BÀ¢w6VÆV7F–öå÷Fö¶VârÂvU÷&÷w2ç•ö&F6…ö6æF–FFUö–C£§FW‡BÀ¢v6æF–FFUö–BrÂvU÷&÷w2æ6æF–FFUö–BÀ¢v6æF–FFUöF—7Æ•öæÖRrÂvU÷&÷w2æ6æF–FFUöF—7Æ•öæÖRÀ¢w–ÖVçEöF—7Æ•÷7FFRrÂvU÷&÷w2ç–ÖVçEöF—7Æ•÷7FFRÀ¢w&VÆV6Uöf–ÆVE÷–ÖVçEöVÆ–v–&ÆRrÂvU÷&÷w2ç&VÆV6Uöf–ÆVE÷–ÖVçEöVÆ–v–&ÆRÀ¢w&U÷&÷f–FW%ö6æ6VÅöVÆ–v–&ÆRrÂvU÷&÷w2ç&U÷&÷f–FW%ö6æ6VÅöVÆ–v–&ÆRÀ¢vf–Æ&ÆUö7F–öç2rÂvU÷&÷w2æf–Æ&ÆUö7F–öç2À¢v÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RrÂvU÷&÷w2æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RÀ¢v7F—fU÷–ÖVçEöÖ÷VçE÷Væ6RrÂvU÷&÷w2æ7F—fU÷–ÖVçEöÖ÷VçE÷Væ6RÀ¢v–æ6ÇVFUö–åö7F—fUö÷fW'f–WrrÂäõBvU÷&÷w2ç&VÖ÷fVBÀ¢v–æ6ÇVFUö–åö7F—fU÷–U÷66†VGVÆRrÂvU÷&÷w2æ†5÷–Uö—FVÒäBäõBvU÷&÷w2ç&VÖ÷fVBÀ¢v7F—fUö—FVÕö6÷VçBrÂvU÷&÷w2æ7F—fUö—FVÕö6÷VçBÀ¢wv÷&µ÷7FGW2rÂvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2À¢vGFV×Eö6÷VçBrÂvU÷&÷w2æGFV×Eö6÷VçBÀ¢vf–ÇW&U÷&V6öârÂ44P¢t„TâvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2”â‚t$Äô4´TBrÂtd”ÄTEôd”äÂrÂtd”ÄTEõ$UE%”$ÄRr¢D„Tâ44P¢t„TâvU÷&÷w2æÆFW7E÷v÷&µ÷7FGW2Òt$Äô4´TBrD„TâuF†—2–ÖVçB6÷VÆBæ÷B&R6†ævVB&V6W6R—G27FGW2÷"6÷W&6R÷væW'6†—6†ævVBâp¢TÅ4Rt6Æ÷VEDÕ26÷VÆBæ÷B6ö×ÆWFRF†—2–ÖVçB6†ævRâp¢Tä@¢TÅ4RåTÄÀ¢TäBÀ¢w6†&VEö–ç7G'V7F–öârÂvU÷&÷w2ç6†&VEö–ç7G'V7F–öåö6÷VçBâÀ¢vÆFW7EöWf–FVæ6UöE÷WF2rÂvU÷&÷w2æÆFW7EöWfVçE÷F–ÖU÷WF0¢¢õ$DU"%’vU÷&÷w2çvUö÷&F–æÀ¢’À¢uµÒs£¦§6öæ ¢’À¢uö6FÆöræ6÷VçB‚¢“£¦–çFVvW"À¢€¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW ¢e$ôÒ6æF–FFUöf–ÇFW&VEö–æFW€¢’À¢€¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW ¢e$ôÒ6æF–FFUöf–ÇFW&VEö–æFW€¢t„U$Ruö6FÆöræ6&F–æÆ—G’†6æF–FFUöf–ÇFW&VEö–æFW‚æf–Æ&ÆUö7F–öç2’â ¢’À¢€¢4TÄT5B6öÆW66R‡uö6FÆörç7VÒ†6æF–FFUöf–ÇFW&VEö–æFW‚æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6R’Â“£¦&–v–ç@¢e$ôÒ6æF–FFUöf–ÇFW&VEö–æFW€¢t„U$Ruö6FÆöræ6&F–æÆ—G’†6æF–FFUöf–ÇFW&VEö–æFW‚æf–Æ&ÆUö7F–öç2’â ¢’À¢€¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW ¢e$ôÒ6æF–FFUö6Æ76–f–VEö–æFW€¢t„U$RäõB6æF–FFUö6Æ76–f–VEö–æFW‚ç&VÖ÷fV@¢’À¢€¢4TÄT5B6öÆW66R‡uö6FÆörç7VÒ€¢44Rt„Tâ6æF–FFUö6Æ76–f–VEö–æFW‚ç&VÖ÷fV@¢D„Tâ£¦&–v–ç@¢TÅ4R6æF–FFUö6Æ76–f–VEö–æFW‚æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6RTä@¢’Â“£¦&–v–ç@¢e$ôÒ6æF–FFUö6Æ76–f–VEö–æFW€¢’À¢€¢4TÄT5B6öÆW66R‡uö6FÆörç7VÒ†6æF–FFUö6Æ76–f–VEö–æFW‚æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6R’Â“£¦&–v–ç@¢e$ôÒ6æF–FFUö6Æ76–f–VEö–æFW€¢’À¢€¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW ¢e$ôÒ6æF–FFUö6Æ76–f–VEö–æFW€¢t„U$R6æF–FFUö6Æ76–f–VEö–æFW‚æ†5÷–Uö—FVÐ¢äBäõB6æF–FFUö6Æ76–f–VEö–æFW‚ç&VÖ÷fV@¢’À¢€¢4TÄT5B6öÆW66R‡uö6FÆörç7VÒ†6æF–FFUö6Æ76–f–VEö–æFW‚æ÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6R’Â“£¦&–v–ç@¢e$ôÒ6æF–FFUö6Æ76–f–VEö–æFW€¢t„U$R6æF–FFUö6Æ76–f–VEö–æFW‚æ†5÷–Uö—FVÐ¢äBäõB6æF–FFUö6Æ76–f–VEö–æFW‚ç&VÖ÷fV@¢’À¢€¢4TÄT5Buö6FÆörçFõö§6öæ"†Æ7E÷vU÷&÷r¢e$ôÒvU÷&÷w22Æ7E÷vU÷&÷p¢õ$DU"%’Æ7E÷vU÷&÷rçvUö÷&F–æÂDU40¢Ä”Ô•B¢¢”åDð¢e÷&÷w2À¢e÷&÷uö6÷VçBÀ¢e÷F÷FÅöÖF6†–æuö6÷VçBÀ¢eöVÆ–v–&ÆUöÖF6†–æuö6÷VçBÀ¢e÷6VÆV7FVEöÖ÷VçE÷Væ6Uöf–Æ&ÆRÀ¢eö7F—fUö÷fW'f–Wuö6æF–FFUö6÷VçBÀ¢eö7F—fUö÷fW'f–WuöÖ÷VçE÷Væ6RÀ¢eö÷&–v–æÅö÷fW'f–WuöÖ÷VçE÷Væ6RÀ¢eö7F—fU÷–U÷66†VGVÆUöÆ–æUö6÷VçBÀ¢eö7F—fU÷–U÷66†VGVÆUöÖ÷VçE÷Væ6RÀ¢eöÆ7E÷&÷p¢e$ôÒvU÷&÷w3° ¢”beöÆ7E÷&÷r•2äõBåTÄÂäBe÷&÷uö6÷VçBÒöÆ–Ö—BD„Tà¢eöæW‡Eö7W'6÷%ö§6öâ£Òuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢w6æ6†÷E÷Fö¶VârÂe÷6æ6†÷E÷Fö¶VâÀ¢w6÷'Eö¶W’rÂe÷6÷'Eö¶W’À¢w6÷'EöF—&V7F–öârÂe÷6÷'EöF—&V7F–öâÀ¢vÆ7E÷7FGW5÷&æ²rÂ‡eöÆ7E÷&÷rÓãâw7FGW5÷&æ²r“£¦–çFVvW"À¢vÆ7Eö6æF–FFUöæÖRrÂuö6FÆöræÆ÷vW"‡eöÆ7E÷&÷rÓãâv6æF–FFUöF—7Æ•öæÖRr’À¢vÆ7EöÖ÷VçE÷Væ6RrÂ‡eöÆ7E÷&÷rÓãâv÷&–v–æÅ÷–ÖVçEöÖ÷VçE÷Væ6Rr“£¦&–v–çBÀ¢vÆ7E÷•ö&F6…ö6æF–FFUö–BrÂeöÆ7E÷&÷rÓãâw•ö&F6…ö6æF–FFUö–BrÀ¢w&Wf–÷W5ö7W'6÷%ö§6öârÂö7W'6÷%ö§6öà¢“°¢TäB”c° ¢e÷&Wf–÷W5ö7W'6÷%ö§6öâ£Ò44P¢t„Tâö7W'6÷%ö§6öâ•2åTÄÂD„TâåTÄÀ¢TÅ4Rö7W'6÷%ö§6öâÓâw&Wf–÷W5ö7W'6÷%ö§6öâp¢TäC° ¢4TÄT5Buö6FÆöræ§6öæ%÷7G&—öçVÆÇ2‡uö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v–BrÂ&WVW7E÷&÷ræ–BÀ¢v6÷'&V7F–öå÷&WVW7Eö–BrÂ&WVW7E÷&÷ræ–BÀ¢w7FGW2rÂ&WVW7E÷&÷rç7FGW2À¢w&WVW7E÷7FGW2rÂ&WVW7E÷&÷rç7FGW2À¢w&WVW7FVEöE÷WF2rÂ&WVW7E÷&÷rç&WVW7FVEöE÷WF2À¢wWFFVEöE÷WF2rÂ&WVW7E÷&÷rçWFFVEöE÷WF2À¢v—5ö7F—fRrÂ&WVW7E÷&÷rç7FGW2”â€¢uÄää”ärrÂuÄääTBrÂu$UTU5DTBrÂtt•D”äuôUD„õ$•4D”ôârÀ¢tUD„õ$•4TBrÂtU…äDTBrÂu$ô4U54”ärp¢’À¢v—5÷FW&Ö–æÂrÂ&WVW7E÷&÷rç7FGW2”â€¢tÄ”TBrÂtÄ”TEõt•D…ô$Äô4´U%2rÂt$Äô4´TBrÂtd”ÄTBrÀ¢u$T¤T5DTBrÂt4ä4TÄÄTBp¢’À¢wW6W%÷F—FÆRrÂ44P¢t„Tâ&WVW7E÷&÷rç7FGW2”â€¢uÄää”ärrÂuÄääTBrÂu$UTU5DTBrÂtt•D”äuôUD„õ$•4D”ôârÀ¢tUD„õ$•4TBrÂtU…äDTBrÂu$ô4U54”ärp¢’D„Tâu–ÖVçB6æ6VÆÆF–öâ–â&öw&W72p¢TÅ4RtÆFW7B–ÖVçB6æ6VÆÆF–öâp¢TäBÀ¢v÷W&F–öåö–BrÂ€¢4TÄT5B÷W&F–öå÷&÷ræ–@¢e$ôÒV&Æ–2æ&æ¶–æu÷•ö÷W&F–öç22÷W&F–öå÷&÷p¢t„U$R÷W&F–öå÷&÷ræ÷W&F–öå÷G—RÒu”ÔTåEô4õ%$T5D”ôâp¢äB÷W&F–öå÷&÷ræ–çWEö§6öâÓãâv6÷'&V7F–öå÷&WVW7Eö–BrÒ&WVW7E÷&÷ræ–C£§FW‡@¢õ$DU"%’÷W&F–öå÷&÷ræ7&VFVEöE÷WF2DU42Â÷W&F–öå÷&÷ræ–BDU40¢Ä”Ô•B¢¢’¢”åDòeöÆFW7Eö6÷'&V7F–öå÷&WVW7@¢e$ôÒV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7G22&WVW7E÷&÷p¢t„U$R&WVW7E÷&÷rç•ö&F6…ö–BÒ÷•ö&F6…ö–@¢õ$DU"%¢44P¢t„Tâ&WVW7E÷&÷rç7FGW2”â€¢uÄää”ärrÂuÄääTBrÂu$UTU5DTBrÂtt•D”äuôUD„õ$•4D”ôârÀ¢tUD„õ$•4TBrÂtU…äDTBrÂu$ô4U54”ärp¢’D„Tâ ¢TÅ4R¢TäBÀ¢&WVW7E÷&÷rçWFFVEöE÷WF2DU42À¢&WVW7E÷&÷ræ–BDU40¢Ä”Ô•B° ¢$UEU$âuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢vö²rÂG'VRÀ¢w•ö&F6…ö–BrÂ÷•ö&F6…ö–BÀ¢w6æ6†÷E÷Fö¶VârÂe÷6æ6†÷E÷Fö¶VâÀ¢v7F—fUö&F6…÷66÷Uö†6‚rÂeö7F—fUö&F6…÷66÷Uö†6‚À¢w6÷'Eö¶W’rÂe÷6÷'Eö¶W’À¢w6÷'EöF—&V7F–öârÂe÷6÷'EöF—&V7F–öâÀ¢wvU÷6—¦RrÂöÆ–Ö—BÀ¢w&÷uö6÷VçBrÂe÷&÷uö6÷VçBÀ¢wF÷FÅöÖF6†–æuö6÷VçBrÂe÷F÷FÅöÖF6†–æuö6÷VçBÀ¢vVÆ–v–&ÆUöÖF6†–æuö6÷VçBrÂeöVÆ–v–&ÆUöÖF6†–æuö6÷VçBÀ¢w6VÆV7FVEöÖ÷VçE÷Væ6Uöf–Æ&ÆRrÂe÷6VÆV7FVEöÖ÷VçE÷Væ6Uöf–Æ&ÆRÀ¢v7F—fUö÷fW'f–Wuö6æF–FFUö6÷VçBrÂeö7F—fUö÷fW'f–Wuö6æF–FFUö6÷VçBÀ¢v7F—fUö÷fW'f–WuöÖ÷VçE÷Væ6RrÂeö7F—fUö÷fW'f–WuöÖ÷VçE÷Væ6RÀ¢v÷&–v–æÅö÷fW'f–WuöÖ÷VçE÷Væ6RrÂeö÷&–v–æÅö÷fW'f–WuöÖ÷VçE÷Væ6RÀ¢v7F—fU÷–U÷66†VGVÆUöÆ–æUö6÷VçBrÂeö7F—fU÷–U÷66†VGVÆUöÆ–æUö6÷VçBÀ¢v7F—fU÷–U÷66†VGVÆUöÖ÷VçE÷Væ6RrÂeö7F—fU÷–U÷66†VGVÆUöÖ÷VçE÷Væ6RÀ¢vÆFW7Eö6÷'&V7F–öå÷&WVW7BrÂeöÆFW7Eö6÷'&V7F–öå÷&WVW7BÀ¢vÆFW7Eö6÷'&V7F–öå÷&WVW7Eö–BrÂeöÆFW7Eö6÷'&V7F–öå÷&WVW7BÓãâv–BrÀ¢w&÷w2rÂe÷&÷w2À¢væW‡Eö7W'6÷%ö§6öârÂeöæW‡Eö7W'6÷%ö§6öâÀ¢w&Wf–÷W5ö7W'6÷%ö§6öârÂe÷&Wf–÷W5ö7W'6÷%ö§6öâÀ¢wvUöÆ&VÂrÂ44P¢t„Tâe÷F÷FÅöÖF6†–æuö6÷VçBÒD„Tâsöbp¢TÅ4Ru6†÷v–ærrÇÂe÷&÷uö6÷VçC£§FW‡BÇÂröbrÇÂe÷F÷FÅöÖF6†–æuö6÷VçC£§FW‡@¢TäBÀ¢v6öFRrÂu”ÔTåEõ5DEU5õtUôô²rÀ¢vÖW76vRrÂåTÄÂÀ¢v6öçF–çVF–öârÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢w&WV—&VBrÂfÇ6RÀ¢v÷W&F–öåö–BrÂåTÄÂÀ¢v÷W&F–öå÷G—RrÂåTÄÂÀ¢w•ö&F6…ö–BrÂ÷•ö&F6…ö–BÀ¢w&ö÷Eö÷W&F–öåö–BrÂåTÄÂÀ¢w†6RrÂåTÄÂÀ¢w'VåögFW%÷WF2rÂåTÄÂÀ¢w&V6öârÂu5DEU5õ$TEôôäÅ’rÀ¢w7V66W76÷%÷&VÆF–öârÂtäôäRrÀ¢w&WV—&W5÷W6W%ö7F–öârÂfÇ6RÀ¢wFW&Ö–æÂrÂG'VP¢¢“°¤Tä@¢FgVæ7F–öâC° ¤ÅDU"eTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’õtäU"Dò÷7Fw&W3°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’e$ôÒT$Ä”3°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’e$ôÒæöã°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’e$ôÒWF†VçF–6FVC°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’e$ôÒ6W'f–6U÷&öÆS°¤u$åBU„T5UDRôâeTä5D”ôâV&Æ–2ç•ö&F6…÷–ÖVçE÷7FGW5÷vU÷c‡WV–BÇWV–BÆ§6öæ"ÇFW‡BÇFW‡BÆ–çFVvW"Æ§6öæ"’Dò6W'f–6U÷&öÆS°