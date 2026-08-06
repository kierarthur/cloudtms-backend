-- CloudTMS Banking Pay cancellation â€” Stage 1.
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
                    'correction_progr×÷îÚ$z{-®éÜj×õ4TÄT5D”ôâp¢äB6ö×ÆWFVE÷vRæ6‡Væµ÷G—RÒt4äD”DDUõ44õRp¢äB6ö×ÆWFVE÷vRç6WVVæ6UöæòÒe÷vU÷6WVVæ6Uöæó° ¢ÒÒF÷FÇ2&R&VGV6VBg&öÒBÖ÷7B#GW&&ÆRvR7VÖÖ&–W2âF†Rf–æÀ¢ÒÒvRFöW2æ÷B66â÷"vw&VvFRF†R6ö×ÆWFRÖVÖ&W'6†—–FVçF—G’6WBà¢4TÄT5@¢4ôÄU44R‡uö6FÆörç7VÒ‚‡vUö6‡Væ²ç&W7VÇEö§6öâÓãâwvUö6æF–FFUö6÷VçBr“£¦–çFVvW"’Â“£¦–çFVvW"À¢4ôÄU44R‡uö6FÆörç7VÒ‚‡vUö6‡Væ²ç&W7VÇEö§6öâÓãâwvUö7F—fUö—FVÕö6÷VçBr“£¦–çFVvW"’Â“£¦–çFVvW"À¢4ôÄU44R‡uö6FÆörç7VÒ‚‡vUö6‡Væ²ç&W7VÇEö§6öâÓãâwvU÷6÷W&6U÷&÷uö6÷VçBr“£¦–çFVvW"’Â“£¦–çFVvW"À¢„4ôÄU44R‡uö6FÆörç7VÒ‚‡vUö6‡Væ²ç&W7VÇEö§6öâÓãâwvUöÖ÷VçE÷Væ6Rr“£¦&–v–çB’Â“£¦çVÖW&–2ò“£¦çVÖW&–2ƒBÃ"¢”åDğ¢e÷F÷FÅö6æF–FFUö6÷VçBÀ¢e÷F÷FÅö7F—fUö—FVÕö6÷VçBÀ¢e÷F÷FÅ÷6÷W&6U÷&÷uö6÷VçBÀ¢e÷F÷FÅöÖ÷Vç@¢e$ôÒV&Æ–2æ&æ¶–æu÷•ö÷W&F–öåö6‡Væ·22vUö6‡Væ°¢t„U$RvUö6‡Væ²æ÷W&F–öåö–BÒeö÷W&F–öâæ–@¢äBvUö6‡Væ²ç†6RÒu$U$Uõ4TÄT5D”ôâp¢äBvUö6‡Væ²æ6‡Væµ÷G—RÒt4äD”DDUõ44õRp¢äBvUö6‡Væ²ç7FGW2Òt4ôÕÄUDRs° ¢”be÷F÷FÅö6æF–FFUö6÷VçBâeöÖ…ö6æF–FFW0¢õ"e÷F÷FÅö7F—fUö—FVÕö6÷VçBâeöÖ…ö7F—fUö—FV×2D„Tà¢$•4RU„4UD”ôâu”ÔTåEô4õ%$T5D”ôåõ4TÄT5D”ôåô44•E•ôU„4TTDTBp¢U4”ärU%$4ôDRÒurÀ¢DUD”ÂÒuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v6öFRrÂu4TÄT5D”ôåô44•E•ôU„4TTDTBrÀ¢w6VÆV7FVEö6æF–FFUö6÷VçBrÂe÷F÷FÅö6æF–FFUö6÷VçBÀ¢w6VÆV7FVEö7F—fUö—FVÕö6÷VçBrÂe÷F÷FÅö7F—fUö—FVÕö6÷Vç@¢“£§FW‡C°¢TäB”c° ¢”beö6ö×ÆWFRD„Tà¢”be÷F÷FÅö6æF–FFUö6÷VçBÂD„Tà¢$•4RU„4UD”ôâu”ÔTåEô4õ%$T5D”ôåõ4TÄT5D”ôåôTÕE’p¢U4”ärU%$4ôDRÒurÀ¢DUD”ÂÒuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v6öFRrÂtDU45$•Dõ%ô”ådÄ”Bp¢“£§FW‡C°¢TäB”c° ¢ÒÒF†Rf–æÂ&VGV6W"&VG2öæÇ’F†R&÷VæFVBvR7VÖÖ&–W2â—B&÷fW2¢ÒÒ6öçF–wV÷W2vRö†6‚6†–âæBæWfW"&V'V–ÆG2ÆÂ—FVÒ–FVçF—F–W2à¢t•D‚÷&FW&VE÷&W&U÷vW22€¢4TÄT5B6‡Væµ÷&÷rç6WVVæ6UöæòÀ¢6‡Væµ÷&÷rç&W7VÇEö§6öâÀ¢uö6FÆörç&÷uöçVÖ&W"‚’õdU"„õ$DU"%’6‡Væµ÷&÷rç6WVVæ6Uöæò“£¦–çFVvW ¢2W‡V7FVE÷6WVVæ6UöæòÀ¢uö6FÆöræÆr†6‡Væµ÷&÷rç&W7VÇEö§6öâÓãâwvUö†6‚r¢õdU"„õ$DU"%’6‡Væµ÷&÷rç6WVVæ6Uöæò’2&Wf–÷W5÷vUö†6€¢e$ôÒV&Æ–2æ&æ¶–æu÷•ö÷W&F–öåö6‡Væ·226‡Væµ÷&÷p¢t„U$R6‡Væµ÷&÷ræ÷W&F–öåö–BÒeö÷W&F–öâæ–@¢äB6‡Væµ÷&÷rç†6RÒu$U$Uõ4TÄT5D”ôâp¢äB6‡Væµ÷&÷ræ6‡Væµ÷G—RÒt4äD”DDUõ44õRp¢äB6‡Væµ÷&÷rç7FGW2Òt4ôÕÄUDRp¢¢4TÄT5Buö6FÆöræ6÷VçB‚¢’d”ÅDU"€¢t„U$R÷&FW&VE÷&W&U÷vW2ç6WVVæ6Uöæğ¢Ãâ÷&FW&VE÷&W&U÷vW2æW‡V7FVE÷6WVVæ6Uöæğ¢õ"†÷&FW&VE÷&W&U÷vW2ç6WVVæ6Uöæòâ¢äB÷&FW&VE÷&W&U÷vW2ç&W7VÇEö§6öâÓãâw&–÷%÷vUö†6‚p¢•2D•5D”ä5Be$ôÒ÷&FW&VE÷&W&U÷vW2ç&Wf–÷W5÷vUö†6‚¢“£¦–çFVvW ¢”åDòe÷vUö6†–åöÖ—6ÖF6…ö6÷Vç@¢e$ôÒ÷&FW&VE÷&W&U÷vW3° ¢”be÷vU÷6WVVæ6Uöæòâ#õ"e÷vUö6†–åöÖ—6ÖF6…ö6÷VçBÃâõ"€¢4TÄT5Buö6FÆöræ6÷VçB‚¢’e$ôÒV&Æ–2æ&æ¶–æu÷•ö÷W&F–öåö6‡Væ·226‡Væµ÷&÷p¢t„U$R6‡Væµ÷&÷ræ÷W&F–öåö–BÒeö÷W&F–öâæ–@¢äB6‡Væµ÷&÷rç†6RÒu$U$Uõ4TÄT5D”ôâp¢äB6‡Væµ÷&÷ræ6‡Væµ÷G—RÒt4äD”DDUõ44õRp¢äB6‡Væµ÷&÷rç7FGW2Òt4ôÕÄUDRp¢’Ãâe÷vU÷6WVVæ6UöæòD„Tà¢$•4RU„4UD”ôâu”ÔTåEô4õ%$T5D”ôåõ4TÄT5D”ôåõtUôD”tU5EôÔ•4ÔD4‚p¢U4”ärU%$4ôDRÒurÂDUD”ÂÒuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v6öFRrÂutUôD”tU5EôÔ•4ÔD4‚rÂwvU÷6WVVæ6UöæòrÂe÷vU÷6WVVæ6Uöæğ¢“£§FW‡C°¢TäB”c° ¢e÷Vç6VÆV7FVE÷66÷Uö†6…ö&Vf÷&R£Òe÷Vç6VÆV7FVEö6†–åö†6ƒ° ¢”beöÖöFRÒtU…Ä”4•BrD„Tà¢e÷&WVW7FVEöW‡Æ–6—Eö6÷VçB£Ò4ôÄU44R‚‡e÷6VÆV7F–öâÓãâw&WVW7FVEöW‡Æ–6—Eö6÷VçBr“£¦–çFVvW"Â“°¢e÷&WVW7FVEöW‡Æ–6—Eö†6‚£Òe÷6VÆV7F–öâÓãâw&WVW7FVEöW‡Æ–6—Eö†6‚s°¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW ¢”åDòeö7GVÅöW‡Æ–6—Eö6÷Vç@¢e$ôÒV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7Eö6æF–FFW22ÖVÖ&W%÷&÷p¢t„U$RÖVÖ&W%÷&÷ræ6÷'&V7F–öå÷&WVW7Eö–BÒe÷&WVW7Bæ–C°¢eö7GVÅöW‡Æ–6—Eö†6‚£ÒeöW‡Æ–6—Eö–Eö6†–åö†6ƒ° ¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW"”åDòeöW‡Æ–6—EöÖ—76–æuö6÷Vç@¢e$ôÒ€¢4TÄT5BW‡Æ–6—E÷Fö¶VâçfÇVS£§WV–B2–@¢e$ôÒuö6FÆöræ§6öæ%ö'&•öVÆVÖVçG5÷FW‡B€¢4ôÄU44R‡e÷6VÆV7F–öâÓâv6æöæ–6ÅöW‡Æ–6—Eö6æF–FFU÷Fö¶Vç2rÂuµÒs£¦§6öæ"¢’2W‡Æ–6—E÷Fö¶Vâ‡fÇVR¢U„4U@¢4TÄT5BÖVÖ&W%÷&÷rç•ö&F6…ö6æF–FFUö–@¢e$ôÒV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7Eö6æF–FFW22ÖVÖ&W%÷&÷p¢t„U$RÖVÖ&W%÷&÷ræ6÷'&V7F–öå÷&WVW7Eö–BÒe÷&WVW7Bæ–@¢’2Ö—76–æuöÖVÖ&W#° ¢4TÄT5Buö6FÆöræ6÷VçB‚¢“£¦–çFVvW"”åDòeöW‡Æ–6—EöW‡G&ö6÷Vç@¢e$ôÒ€¢4TÄT5BÖVÖ&W%÷&÷rç•ö&F6…ö6æF–FFUö–@¢e$ôÒV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7Eö6æF–FFW22ÖVÖ&W%÷&÷p¢t„U$RÖVÖ&W%÷&÷ræ6÷'&V7F–öå÷&WVW7Eö–BÒe÷&WVW7Bæ–@¢U„4U@¢4TÄT5BW‡Æ–6—E÷Fö¶VâçfÇVS£§WV–@¢e$ôÒuö6FÆöræ§6öæ%ö'&•öVÆVÖVçG5÷FW‡B€¢4ôÄU44R‡e÷6VÆV7F–öâÓâv6æöæ–6ÅöW‡Æ–6—Eö6æF–FFU÷Fö¶Vç2rÂuµÒs£¦§6öæ"¢’2W‡Æ–6—E÷Fö¶Vâ‡fÇVR¢’2W‡G&öÖVÖ&W#° ¢”be÷&WVW7FVEöW‡Æ–6—Eö6÷VçBÃâeö7GVÅöW‡Æ–6—Eö6÷Vç@¢õ"e÷&WVW7FVEöW‡Æ–6—Eö†6‚•2D•5D”ä5Be$ôÒeö7GVÅöW‡Æ–6—Eö†6€¢õ"eöW‡Æ–6—EöÖ—76–æuö6÷VçBÃâõ"eöW‡Æ–6—EöW‡G&ö6÷VçBÃâD„Tà¢UDDRV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7G22–çfÆ–E÷&WVW7@¢4UB7FGW2Òt4ä4TÄÄTBrÂ6æ6VÆÆVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚’À¢Æåö§6öâÒ4ôÄU44R†–çfÆ–E÷&WVW7BçÆåö§6öâÂw·Òs£¦§6öæ"¢ÇÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢w7FÆUö6öFRrÂtU…Ä”4•Eõ4TÄT5D”ôåôÔ•4ÔD4‚rÀ¢w&WVW7FVEöW‡Æ–6—Eö6÷VçBrÂe÷&WVW7FVEöW‡Æ–6—Eö6÷VçBÀ¢vÖFW&–Æ—6VEöW‡Æ–6—Eö6÷VçBrÂeö7GVÅöW‡Æ–6—Eö6÷Vç@¢’À¢WFFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢t„U$R–çfÆ–E÷&WVW7Bæ–BÒe÷&WVW7Bæ–C°¢UDDRV&Æ–2æ&æ¶–æu÷•ö÷W&F–öç22–çfÆ–Eö÷W&F–öà¢4UB7FGW2Òt4ä4TÄÄTBrÂ†6RÒt4ôÕÄUDRrÂ'VææW%÷7FFRÒt4ä4TÄÄTBrÀ¢&WV—&W5÷W6W%ö7F–öâÒfÇ6RÂ'VåögFW%÷WF2ÒåTÄÂÀ¢&W7VÇEö§6öâÒ4ôÄU44R†–çfÆ–Eö÷W&F–öâç&W7VÇEö§6öâÂw·Òs£¦§6öæ"¢ÇÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B‚v6öFRrÂtU…Ä”4•Eõ4TÄT5D”ôåôÔ•4ÔD4‚r’À¢6ö×ÆWFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚’À¢ÆV6Uö÷væW"ÒåTÄÂÂÆV6UöW‡—&W5öE÷WF2ÒåTÄÂÀ¢Æö6¶VEö'’ÒåTÄÂÂÆö6µöW‡—&W5öE÷WF2ÒåTÄÂÀ¢WFFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢t„U$R–çfÆ–Eö÷W&F–öâæ–BÒeö÷W&F–öâæ–C°¢$UEU$âuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢vö²rÂfÇ6RÂv6÷'&V7F–öå÷&WVW7Eö–BrÂe÷&WVW7Bæ–BÀ¢v÷W&F–öåö–BrÂeö÷W&F–öâæ–BÂw†6RrÂt4ôÕÄUDRrÀ¢v6ö×ÆWFRrÂG'VRÂw7FÆRrÂG'VRÂv6öFRrÂtU…Ä”4•Eõ4TÄT5D”ôåôÔ•4ÔD4‚rÀ¢vÖW76vRrÂuF†RW†7B&WVW7FVB–ÖVçG26÷VÆBæ÷B&Rg&÷¦Vââ&Vg&W6‚æB6VÆV7BF†VÒv–ââp¢“°¢TäB”c°¢TäB”c° ¢e÷6VÆV7F–öåö†6‚£Ò&—fFRç•÷–ÖVçEö6÷'&V7F–öå÷6†#Se÷c€¢uö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢wfW'6–öârÂ"Âw•ö&F6…ö–BrÂeö&F6‚æ–BÀ¢w&WVW7FVEö7F–öârÂe÷&WVW7FVEö7F–öâÀ¢v7F—fUö&F6…÷66÷Uö†6‚rÂeö7F—fUö&F6…÷66÷Uö†6‚À¢w6VÆV7FVEö6æF–FFUö6÷VçBrÂe÷F÷FÅö6æF–FFUö6÷VçBÀ¢w6VÆV7FVEö7F—fUö—FVÕö6÷VçBrÂe÷F÷FÅö7F—fUö—FVÕö6÷VçBÀ¢w6VÆV7FVE÷6÷W&6U÷&÷uö6÷VçBrÂe÷F÷FÅ÷6÷W&6U÷&÷uö6÷VçBÀ¢w6VÆV7FVEöÖ÷VçE÷Væ6RrÂuö6FÆörç&÷VæB‡e÷F÷FÅöÖ÷VçB¢“£¦&–v–çBÀ¢w6VÆV7FVEö6†–åö†6‚rÂe÷6VÆV7FVEö6†–åö†6‚À¢wVç6VÆV7FVEö6†–åö†6‚rÂe÷Vç6VÆV7FVEö6†–åö†6‚À¢w&W&U÷vUö6÷VçBrÂe÷vU÷6WVVæ6Uöæğ¢¢“° ¢e÷Æåö§6öâ£Òuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v6öçG&7E÷fW'6–öârÂ"À¢v7F—fUö&F6…÷66÷Uö†6‚rÂeö7F—fUö&F6…÷66÷Uö†6‚À¢w66÷UöfVæ6Uö†6‚rÂeö7F—fUö&F6…÷66÷Uö†6‚À¢w&WVW7FVEö7F–öârÂe÷&WVW7FVEö7F–öâÀ¢w6VÆV7FVEö6æF–FFUö6÷VçBrÂe÷F÷FÅö6æF–FFUö6÷VçBÀ¢w6VÆV7FVEö7F—fUö—FVÕö6÷VçBrÂe÷F÷FÅö7F—fUö—FVÕö6÷VçBÀ¢w6VÆV7FVE÷6÷W&6U÷&÷uö6÷VçBrÂe÷F÷FÅ÷6÷W&6U÷&÷uö6÷VçBÀ¢w6VÆV7FVEöÖ÷VçE÷Væ6RrÂuö6FÆörç&÷VæB‡e÷F÷FÅöÖ÷VçB¢“£¦&–v–çBÀ¢w6VÆV7F–öåö†6‚rÂe÷6VÆV7F–öåö†6‚À¢w6VÆV7FVEö6†–åö†6‚rÂe÷6VÆV7FVEö6†–åö†6‚À¢wVç6VÆV7FVEö6†–åö†6‚rÂe÷Vç6VÆV7FVEö6†–åö†6‚À¢w&W&U÷vUö6÷VçBrÂe÷vU÷6WVVæ6UöæòÀ¢w&WVW7FVEöW‡Æ–6—Eö6÷VçBrÂe÷&WVW7FVEöW‡Æ–6—Eö6÷VçBÀ¢w&WVW7FVEöW‡Æ–6—Eö†6‚rÂe÷&WVW7FVEöW‡Æ–6—Eö†6‚À¢wVç6VÆV7FVE÷66÷Uö†6…ö&Vf÷&RrÂe÷Vç6VÆV7FVE÷66÷Uö†6…ö&Vf÷&RÀ¢w6VÆV7F–öå÷&VG•öE÷WF2rÂuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢“°¢e÷Æåö†6‚£Ò&—fFRç•÷–ÖVçEö6÷'&V7F–öå÷6†#Se÷c‡e÷Æåö§6öâ“° ¢UDDRV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷&WVW7G22&VG•÷&WVW7@¢4UB7FGW2ÒuÄääTBrÀ¢6VÆV7F–öåö†6‚Òe÷6VÆV7F–öåö†6‚À¢Æåö§6öâÒe÷Æåö§6öâÀ¢Æåö†6‚Òe÷Æåö†6‚À¢WFFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢t„U$R&VG•÷&WVW7Bæ–BÒe÷&WVW7Bæ–C° ¢UDDRV&Æ–2æ&æ¶–æu÷•ö÷W&F–öç22&VG•ö÷W&F–öà¢4UB7FGW2Òut•D”äuôUD„õ$•4D”ôârÀ¢†6RÒtt•D”äuõ$TUD„TåD”4D”ôârÀ¢&öw&W75ö§6öâÒ4ôÄU44R‡&VG•ö÷W&F–öâç&öw&W75ö§6öâÂw·Òs£¦§6öæ"¢ÇÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢vÆ7E÷66ææVEö6æF–FFUö–BrÂe÷66åöÆ7Eö6æF–FFUö–BÀ¢w6VÆV7F–öåö7W'6÷%ö§6öârÂåTÄÂÀ¢vÆ7E÷&W&U÷vUö†6‚rÂe÷vUö†6‚À¢w6VÆV7FVEö6æF–FFUö6÷VçBrÂe÷F÷FÅö6æF–FFUö6÷VçBÀ¢w6VÆV7FVEö7F—fUö—FVÕö6÷VçBrÂe÷F÷FÅö7F—fUö—FVÕö6÷VçBÀ¢w6VÆV7FVE÷6÷W&6U÷&÷uö6÷VçBrÂe÷F÷FÅ÷6÷W&6U÷&÷uö6÷VçBÀ¢w6VÆV7FVEöÖ÷VçE÷Væ6RrÂuö6FÆörç&÷VæB‡e÷F÷FÅöÖ÷VçB¢“£¦&–v–çBÀ¢w6VÆV7F–öåö†6‚rÂe÷6VÆV7F–öåö†6‚À¢w†6RrÂtt•D”äuõ$TUD„TåD”4D”ôâp¢’À¢F÷FÅ÷Væ—G2Òe÷F÷FÅö6æF–FFUö6÷VçBÀ¢6ö×ÆWFVE÷Væ—G2ÒÀ¢7W'&VçEö6‡Væµö–æFW‚Òe÷6WVVæ6UöæòÀ¢6‡Væµö6÷VçBÒe÷6WVVæ6UöæòÀ¢'VåögFW%÷WF2ÒåTÄÂÀ¢&WV—&W5÷W6W%ö7F–öâÒG'VRÀ¢'VææW%÷7FFRÒut•D”äuõU4U"rÀ¢&W7VÖU÷&V6öâÒtt•D”äuõ$TUD„TåD”4D”ôârÀ¢ÆV6Uö÷væW"ÒåTÄÂÀ¢ÆV6UöW‡—&W5öE÷WF2ÒåTÄÂÀ¢Æö6¶VEö'’ÒåTÄÂÀ¢Æö6µöW‡—&W5öE÷WF2ÒåTÄÂÀ¢WFFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢t„U$R&VG•ö÷W&F–öâæ–BÒeö÷W&F–öâæ–C°¢TÅ4P¢UDDRV&Æ–2æ&æ¶–æu÷•ö÷W&F–öç22v–æuö÷W&F–öà¢4UB&öw&W75ö§6öâÒ4ôÄU44R‡v–æuö÷W&F–öâç&öw&W75ö§6öâÂw·Òs£¦§6öæ"¢ÇÂuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢vÆ7E÷66ææVEö6æF–FFUö–BrÂe÷66åöÆ7Eö6æF–FFUö–BÀ¢w6VÆV7F–öåö7W'6÷%ö§6öârÂeöæW‡Eö7W'6÷%ö§6öâÀ¢vÆ7E÷&W&U÷vUö†6‚rÂe÷vUö†6‚À¢w&W&VEö6æF–FFUö6÷VçBrÂe÷F÷FÅö6æF–FFUö6÷VçBÀ¢w&W&VEö7F—fUö—FVÕö6÷VçBrÂe÷F÷FÅö7F—fUö—FVÕö6÷VçBÀ¢w&W&VE÷6÷W&6U÷&÷uö6÷VçBrÂe÷F÷FÅ÷6÷W&6U÷&÷uö6÷VçBÀ¢w&W&VEöÖ÷VçE÷Væ6RrÂuö6FÆörç&÷VæB‡e÷F÷FÅöÖ÷VçB¢“£¦&–v–çBÀ¢w†6RrÂu$U$Uõ4TÄT5D”ôâp¢’À¢7W'&VçEö6‡Væµö–æFW‚Òe÷6WVVæ6UöæòÀ¢6‡Væµö6÷VçBÒe÷6WVVæ6UöæòÀ¢'VåögFW%÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚’À¢&WV—&W5÷W6W%ö7F–öâÒfÇ6RÀ¢WFFVEöE÷WF2Òuö6FÆöræ6Æö6µ÷F–ÖW7F×‚¢t„U$Rv–æuö÷W&F–öâæ–BÒeö÷W&F–öâæ–C°¢TäB”c° ¢$UEU$âuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢vö²rÂG'VRÀ¢v6÷'&V7F–öå÷&WVW7Eö–BrÂe÷&WVW7Bæ–BÀ¢v÷W&F–öåö–BrÂeö÷W&F–öâæ–BÀ¢w†6RrÂ44P¢t„Tâeö6ö×ÆWFRD„Tâtt•D”äuõ$TUD„TåD”4D”ôâp¢TÅ4Ru$U$Uõ4TÄT5D”ôâp¢TäBÀ¢wvUö6æF–FFUö6÷VçBrÂe÷vUö6æF–FFUö6÷VçBÀ¢wvUö7F—fUö—FVÕö6÷VçBrÂe÷vUö7F—fUö—FVÕö6÷VçBÀ¢wvU÷6÷W&6U÷&÷uö6÷VçBrÂe÷vU÷6÷W&6U÷&÷uö6÷VçBÀ¢wvUöÖ÷VçE÷Væ6RrÂe÷vUöÖ÷VçE÷Væ6RÀ¢wvUö†6‚rÂe÷vUö†6‚À¢w6VÆV7F–öåö†6‚rÂe÷6VÆV7F–öåö†6‚À¢v7F—fUö&F6…÷66÷Uö†6‚rÂeö7F—fUö&F6…÷66÷Uö†6‚À¢væW‡Eö7W'6÷%ö§6öârÂeöæW‡Eö7W'6÷%ö§6öâÀ¢v6ö×ÆWFRrÂeö6ö×ÆWFRÀ¢w7FÆRrÂfÇ6RÀ¢v6öFRrÂ44P¢t„Tâeö6ö×ÆWFRD„Tâu4TÄT5D”ôåõ$TE’p¢TÅ4Ru4TÄT5D”ôåõtUõ$U$TBp¢TäBÀ¢vÖW76vRrÂ44P¢t„Tâeö6ö×ÆWFRD„TâuF†RW†7B–ÖVçB6VÆV7F–öâ—2&VG’Fò&Wf–Wrâp¢TÅ4RuF†R–ÖVçB6VÆV7F–öâ—27F–ÆÂ&V–ær&W&VBâp¢Tä@¢“°¤U„4UD”ôà¢t„TâÆö6µöæ÷Eöf–Æ&ÆRõ"VW'•ö6æ6VÆVBD„Tà¢$•4RU„4UD”ôâu”ÔTåEô4õ%$T5D”ôåõ4TÄT5D”ôåôÄô4µõD”ÔTõUBp¢U4”ärU%$4ôDRÒsSU2rÀ¢DUD”ÂÒuö6FÆöræ§6öæ%ö'V–ÆEöö&¦V7B€¢v6öFRrÂu4TÄT5D”ôåôÄô4µõD”ÔTõUBrÀ¢v6÷'&V7F–öå÷&WVW7Eö–BrÂö6÷'&V7F–öå÷&WVW7Eö–BÀ¢v÷W&F–öåö–BrÂö÷W&F–öåö–@¢“£§FW‡C°¤TäC°¢FgVæ7F–öâC° ¤ÅDU"eTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’õtäU"Dò÷7Fw&W3°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’e$ôÒT$Ä”3°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’e$ôÒæöã°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’e$ôÒWF†VçF–6FVC°¥$Udô´RÄÂôâeTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’e$ôÒ6W'f–6U÷&öÆS°¤u$åBU„T5UDRôâeTä5D”ôâV&Æ–2ç•÷–ÖVçEö6÷'&V7F–öå÷6VÆV7F–öå÷&W&Uö6‡Væµ÷c‡WV–BÇWV–BÆ§6öæ"Æ–çFVvW"ÇFW‡BÇWV–B’Dò6W'f–6U÷&öÆS°