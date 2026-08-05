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

    WITH candidate_status_index AS MATERIALIZED (
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
                 WHERE event_item.pay_batch_candidate_id = candidate_row.id
                   AND paid_event.pay_batch_id = p_pay_batch_id
                   AND paid_event.normalised_state IN ('COMPLETED','PAID','SETTLED')
               )) AS paid_or_settled,
               (EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS event_item
                 JOIN public.pay_bank_transfer_events AS no_money_event
                   ON no_money_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                 WHERE event_item.pay_batch_candidate_id = candidate_row.id
                   AND no_money_event.pay_batch_id = p_pay_batch_id
                   AND no_money_event.normalised_state IN ('FAILED','REJECTED','CANCELLED','CONFIRMED_NOT_PAID')
               ) OR EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS transfer_item
                 JOIN public.pay_bank_transfers AS failed_transfer
                   ON failed_transfer.id = transfer_item.pay_bank_transfer_id
                 WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                   AND (failed_transfer.status IN ('FAILED','CANCELLED')
                     OR failed_transfer.rail_state IN ('FAILED','CANCELLED'))
               )) AS terminal_no_money,
               (EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS event_item
                 JOIN public.pay_bank_transfer_events AS ambiguous_event
                   ON ambiguous_event.pay_bank_transfer_id = event_item.pay_bank_transfer_id
                 WHERE event_item.pay_batch_candidate_id = candidate_row.id
                   AND ambiguous_event.pay_batch_id = p_pay_batch_id
                   AND (ambiguous_event.normalised_state IN ('UNKNOWN','PENDING','SUBMITTED','PROCESSING')
                     OR ambiguous_event.movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED')
               ) OR EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS transfer_item
                 JOIN public.pay_bank_transfers AS pending_transfer
                   ON pending_transfer.id = transfer_item.pay_bank_transfer_id
                 WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                   AND (pending_transfer.status IN ('SUBMITTED','PROCESSING','PENDING')
                     OR pending_transfer.rail_state IN ('SUBMITTED','PROCESSING','PENDING'))
               )) AS ambiguous,
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
               ), candidate_row.net_bank_amount, 0)::numeric(14,2) AS reviewed_payment_amount
        FROM public.pay_batch_candidates AS candidate_row
        WHERE candidate_row.pay_batch_id = p_pay_batch_id
    ), candidate_classified_index AS MATERIALIZED (
        SELECT candidate_status_index.*,
               CASE
                 WHEN removed AND released THEN 'RELEASED'
                 WHEN removed THEN 'CANCELLED'
                 WHEN paid_or_settled THEN 'SETTLED'
                 WHEN latest_work_status = 'BLOCKED' THEN 'BLOCKED'
                 WHEN latest_work_status IN ('FAILED_FINAL','FAILED_RETRYABLE') THEN 'FAILED'
                 WHEN terminal_no_money THEN 'NOT_PAID'
                 WHEN ambiguous THEN 'AMBIGUOUS'
                 ELSE 'ACTIVE'
               END AS payment_display_state,
               CASE
                 WHEN removed OR paid_or_settled THEN ARRAY[]::text[]
                 WHEN v_batch.status = 'DRAFT' THEN ARRAY['DRAFT_CANCEL']::text[]
                 WHEN terminal_no_money THEN ARRAY['RELEASE_FAILED_PAYMENT']::text[]
                 WHEN ambiguous THEN ARRAY['RESOLVE_PAYMENT_STATUS']::text[]
                 ELSE ARRAY['CANCEL_PAYMENT']::text[]
               END AS available_actions,
               pg_catalog.round(reviewed_payment_amount * 100)::bigint AS original_payment_amount_pence,
               CASE WHEN removed THEN 70 WHEN paid_or_settled THEN 60
                    WHEN latest_work_status = 'BLOCKED' THEN 50
                    WHEN terminal_no_money THEN 40 WHEN ambiguous THEN 30 ELSE 10 END AS status_rank
        FROM candidate_status_index
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
                       candidate_row.net_bank_amount,
                       0
                   ) * 100
               )::bigint
                   AS original_payment_amount_pence,
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
               coalesce(provider_rollup.has_paid_evidence, false)
                   OR candidate_row.settlement_status IN ('SETTLED', 'PAID') AS paid_or_settled,
               coalesce(provider_rollup.has_terminal_no_money, false)
                   OR coalesce(transfer_rollup.transfer_failed, false) AS terminal_no_money,
               coalesce(provider_rollup.has_ambiguous_evidence, false)
                   OR coalesce(transfer_rollup.transfer_pending, false) AS ambiguous,
               work_rollup.latest_work_status,
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
        LEFT JOIN item_rollup ON item_rollup.pay_batch_candidate_id = candidate_row.id
        LEFT JOIN correction_rollup ON correction_rollup.pay_batch_candidate_id = candidate_row.id
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
                   WHEN base.terminal_no_money THEN 'NOT_PAID'
                   WHEN base.ambiguous THEN 'AMBIGUOUS'
                   ELSE 'ACTIVE'
               END AS payment_display_state,
                CASE
                    WHEN base.removed OR base.paid_or_settled THEN ARRAY[]::text[]
                    WHEN v_batch.status = 'DRAFT' THEN ARRAY['DRAFT_CANCEL']::text[]
                    WHEN base.terminal_no_money THEN ARRAY['RELEASE_FAILED_PAYMENT']::text[]
                   WHEN base.ambiguous THEN ARRAY['RESOLVE_PAYMENT_STATUS']::text[]
                   ELSE ARRAY['CANCEL_PAYMENT']::text[]
               END AS available_actions,
               CASE
                   WHEN base.removed OR base.paid_or_settled THEN 0::bigint
                   ELSE base.original_payment_amount_pence
               END AS active_payment_amount_pence,
               CASE
                    WHEN base.removed THEN 70
                   WHEN base.paid_or_settled THEN 60
                   WHEN base.latest_work_status = 'BLOCKED' THEN 50
                   WHEN base.terminal_no_money THEN 40
                   WHEN base.ambiguous THEN 30
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
                    'is_not_paid', page_rows.terminal_no_money AND NOT page_rows.removed,
                    'is_released', page_rows.removed AND page_rows.released,
                    'eligible_action_codes', page_rows.available_actions,
                    'plain_blocker', CASE
                        WHEN page_rows.paid_or_settled THEN 'Paid or settled payments cannot be cancelled.'
                        WHEN page_rows.payment_display_state = 'AMBIGUOUS' THEN 'The bank payment status must be resolved before continuing.'
                        WHEN page_rows.latest_work_status = 'BLOCKED' THEN 'This payment could not be changed because its status or source ownership changed.'
                        WHEN page_rows.latest_work_status IN ('FAILED_FINAL', 'FAILED_RETRYABLE') THEN 'CloudTMS could not complete this payment change.'
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
                    'available_actions', page_rows.available_actions,
                    'original_payment_amount_pence', page_rows.original_payment_amount_pence,
                    'active_payment_amount_pence', page_rows.active_payment_amount_pence,
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
        v_last_row
    FROM page_rows;

    IF v_last_row IS NOT NULL AND v_row_count = p_limit THEN
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
        'status', request_row.status,
        'requested_at_utc', request_row.requested_at_utc,
        'updated_at_utc', request_row.updated_at_utc,
        'operation_id', (
            SELECT operation_row.id
            FROM public.banking_pay_operations AS operation_row
            WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
              AND operation_row.input_json->>'correction_request_id' = request_row.id::text
            ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
            LIMIT 1
        )
    ))
    INTO v_latest_correction_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.pay_batch_id = p_pay_batch_id
    ORDER BY request_row.created_at_utc DESC, request_row.id DESC
    LIMIT 1;

    RETURN pg_catalog.jsonb_build_object(
        'ok', true,
        'pay_batch_id', p_pay_batch_id,
        'snapshot_token', v_snapshot_token,
        'active_batch_scope_hash', v_active_batch_scope_hash,
        'sort_key', v_sort_key,
        'sort_direction', v_sort_direction,
        'page_size', p_limit,
        'row_count', v_row_count,
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
