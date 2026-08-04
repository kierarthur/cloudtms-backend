-- CloudTMS Banking Pay cancellation — Stage 1.
-- Optional bounded, read-only, non-repairing correction integrity checker.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_integrity_check_v1(
    p_correction_request_id uuid,
    p_operation_id uuid DEFAULT NULL::uuid,
    p_max_candidates integer DEFAULT 10000
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
    v_checked_at_utc timestamptz := pg_catalog.statement_timestamp();
    v_request public.pay_payment_correction_requests%ROWTYPE;
    v_operation public.banking_pay_operations%ROWTYPE;
    v_operation_id uuid;
    v_selected_candidate_count integer := 0;
    v_selected_active_item_count integer := 0;
    v_selected_source_row_count integer := 0;
    v_selected_amount numeric(14,2) := 0;
    v_materialised_work_count integer := 0;
    v_terminal_work_count integer := 0;
    v_applied_candidate_count integer := 0;
    v_blocked_candidate_count integer := 0;
    v_failed_candidate_count integer := 0;
    v_source_item_expected_count integer := 0;
    v_source_item_applied_count integer := 0;
    v_reservation_release_expected_count integer := 0;
    v_reservation_release_actual_count integer := 0;
    v_active_candidate_count integer := 0;
    v_expected_active_total numeric(14,2) := 0;
    v_actual_active_total numeric(14,2) := 0;
    v_active_total_difference numeric(14,2) := 0;
    v_old_authorisation_invalidated boolean := true;
    v_old_schedule_invalidated boolean := true;
    v_actionable_cancelled_provider_scope_count integer := 0;
    v_unselected_before_hash text;
    v_unselected_after_hash text;
    v_unselected_unchanged boolean := true;
    v_refresh_group_total integer := 0;
    v_refresh_group_complete integer := 0;
    v_workbench_refresh_status text := 'NOT_STAGED';
    v_failure_categories jsonb := '[]'::jsonb;
    v_warnings jsonb := '[]'::jsonb;
    v_recalculated_selection_hash text;
    v_expected_selection_hash text;
    v_work_membership_mismatch boolean := false;
    v_terminal_request boolean := false;
    v_nonterminal_work_count integer := 0;
    v_old_auth_request_id uuid;
    v_expected_old_schedule_kind text;
    v_expected_old_scheduled_at timestamptz;
BEGIN
    IF p_correction_request_id IS NULL THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'status', 'FAIL',
            'check_version', 1,
            'checked_at_utc', v_checked_at_utc,
            'correction_request_id', NULL,
            'operation_id', p_operation_id,
            'pay_batch_id', NULL,
            'failure_categories', pg_catalog.jsonb_build_array('REQUEST_NOT_FOUND'),
            'warnings', '[]'::jsonb
        );
    END IF;

    IF p_max_candidates IS NULL
       OR p_max_candidates < 1
       OR p_max_candidates > 10000 THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'status', 'FAIL',
            'check_version', 1,
            'checked_at_utc', v_checked_at_utc,
            'correction_request_id', p_correction_request_id,
            'operation_id', p_operation_id,
            'pay_batch_id', NULL,
            'failure_categories', pg_catalog.jsonb_build_array('CAPACITY_EXCEEDED'),
            'warnings', '[]'::jsonb
        );
    END IF;

    SELECT request_row.*
    INTO v_request
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.id = p_correction_request_id;

    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object(
            'ok', false,
            'status', 'FAIL',
            'check_version', 1,
            'checked_at_utc', v_checked_at_utc,
            'correction_request_id', p_correction_request_id,
            'operation_id', p_operation_id,
            'pay_batch_id', NULL,
            'failure_categories', pg_catalog.jsonb_build_array('REQUEST_NOT_FOUND'),
            'warnings', '[]'::jsonb
        );
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
      AND operation_row.pay_batch_id = v_request.pay_batch_id
      AND operation_row.input_json ->> 'correction_request_id'
          = v_request.id::text
      AND (p_operation_id IS NULL OR operation_row.id = p_operation_id)
    ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
    LIMIT 1;

    IF NOT FOUND THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('OPERATION_MISMATCH');
        v_operation_id := p_operation_id;
    ELSE
        v_operation_id := v_operation.id;
        IF p_operation_id IS NOT NULL
           AND v_operation.id IS DISTINCT FROM p_operation_id THEN
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('OPERATION_MISMATCH');
        END IF;
    END IF;

    SELECT
        pg_catalog.count(*)::integer,
        pg_catalog.coalesce(pg_catalog.sum(member_row.active_item_count), 0)::integer,
        pg_catalog.coalesce(pg_catalog.sum(member_row.source_row_count), 0)::integer,
        pg_catalog.coalesce(pg_catalog.sum(member_row.active_amount), 0)::numeric(14,2)
    INTO
        v_selected_candidate_count,
        v_selected_active_item_count,
        v_selected_source_row_count,
        v_selected_amount
    FROM public.pay_payment_correction_request_candidates AS member_row
    WHERE member_row.correction_request_id = v_request.id;

    IF v_selected_candidate_count > p_max_candidates
       OR v_selected_active_item_count > 250000 THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('CAPACITY_EXCEEDED');
    END IF;

    IF v_selected_candidate_count
           IS DISTINCT FROM pg_catalog.coalesce(
               NULLIF(v_request.plan_json ->> 'selected_candidate_count', '')::integer,
               v_selected_candidate_count
           )
       OR v_selected_active_item_count
           IS DISTINCT FROM pg_catalog.coalesce(
               NULLIF(v_request.plan_json ->> 'selected_active_item_count', '')::integer,
               v_selected_active_item_count
           )
       OR pg_catalog.round(v_selected_amount * 100)::bigint
           IS DISTINCT FROM pg_catalog.coalesce(
               NULLIF(v_request.plan_json ->> 'selected_amount_pence', '')::bigint,
               pg_catalog.round(v_selected_amount * 100)::bigint
           ) THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('SELECTION_COUNT_MISMATCH');
    END IF;

    IF v_selected_candidate_count > 0 THEN
        v_recalculated_selection_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
                'version', 1,
                'pay_batch_id', v_request.pay_batch_id,
                'requested_action', pg_catalog.coalesce(
                    v_request.plan_json ->> 'requested_action',
                    v_request.selection_json ->> 'requested_action',
                    v_request.selection_json ->> 'action'
                ),
                'active_batch_scope_hash',
                    v_request.plan_json ->> 'active_batch_scope_hash',
                'selected_candidate_count', v_selected_candidate_count,
                'selected_active_item_count', v_selected_active_item_count,
                'selected_source_row_count', v_selected_source_row_count,
                'selected_amount_pence',
                    pg_catalog.round(v_selected_amount * 100)::bigint,
                'members', (
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
                )
            )
        );
        v_expected_selection_hash := v_request.selection_hash;

        IF v_expected_selection_hash IS NULL
           OR v_recalculated_selection_hash IS DISTINCT FROM v_expected_selection_hash THEN
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('SELECTION_HASH_MISMATCH');
        END IF;
    ELSIF v_request.status NOT IN ('PLANNING', 'CANCELLED', 'REJECTED') THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('SELECTION_COUNT_MISMATCH');
    END IF;

    SELECT
        pg_catalog.count(*)::integer,
        pg_catalog.count(*) FILTER (
            WHERE work_row.status IN (
                'APPLIED',
                'SKIPPED',
                'BLOCKED',
                'FAILED_FINAL',
                'CANCELLED'
            )
        )::integer,
        pg_catalog.count(*) FILTER (
            WHERE work_row.status = 'APPLIED'
        )::integer,
        pg_catalog.count(*) FILTER (
            WHERE work_row.status = 'BLOCKED'
        )::integer,
        pg_catalog.count(*) FILTER (
            WHERE work_row.status = 'FAILED_FINAL'
        )::integer,
        pg_catalog.count(*) FILTER (
            WHERE work_row.status IN (
                'PENDING',
                'PROCESSING',
                'FAILED_RETRYABLE'
            )
        )::integer
    INTO
        v_materialised_work_count,
        v_terminal_work_count,
        v_applied_candidate_count,
        v_blocked_candidate_count,
        v_failed_candidate_count,
        v_nonterminal_work_count
    FROM public.pay_payment_correction_work_items AS work_row
    WHERE work_row.correction_request_id = v_request.id;

    SELECT EXISTS (
        (
            SELECT member_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_request_candidates AS member_row
            WHERE member_row.correction_request_id = v_request.id
            EXCEPT
            SELECT work_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_work_items AS work_row
            WHERE work_row.correction_request_id = v_request.id
        )
        UNION ALL
        (
            SELECT work_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_work_items AS work_row
            WHERE work_row.correction_request_id = v_request.id
            EXCEPT
            SELECT member_row.pay_batch_candidate_id
            FROM public.pay_payment_correction_request_candidates AS member_row
            WHERE member_row.correction_request_id = v_request.id
        )
    )
    INTO v_work_membership_mismatch;

    IF v_request.status IN (
        'EXPANDED',
        'PROCESSING',
        'APPLIED',
        'APPLIED_WITH_BLOCKERS',
        'BLOCKED',
        'FAILED'
    )
       AND (
           v_materialised_work_count IS DISTINCT FROM v_selected_candidate_count
           OR v_work_membership_mismatch
       ) THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('WORK_MEMBERSHIP_MISMATCH');
    END IF;

    v_terminal_request := v_request.status IN (
        'APPLIED',
        'APPLIED_WITH_BLOCKERS',
        'BLOCKED',
        'FAILED',
        'REJECTED',
        'CANCELLED'
    );

    IF v_terminal_request AND v_nonterminal_work_count > 0 THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('NONTERMINAL_WORK_REMAINS');
    END IF;

    SELECT pg_catalog.coalesce(
        pg_catalog.sum(member_row.active_item_count),
        0
    )::integer
    INTO v_source_item_expected_count
    FROM public.pay_payment_correction_request_candidates AS member_row
    JOIN public.pay_payment_correction_work_items AS work_row
      ON work_row.correction_request_id = member_row.correction_request_id
     AND work_row.pay_batch_candidate_id = member_row.pay_batch_candidate_id
    WHERE member_row.correction_request_id = v_request.id
      AND work_row.status = 'APPLIED';

    SELECT pg_catalog.count(
        DISTINCT correction_item.pay_batch_item_id
    )::integer
    INTO v_source_item_applied_count
    FROM public.pay_payment_correction_items AS correction_item
    WHERE correction_item.correction_request_id = v_request.id
      AND correction_item.status = 'APPLIED';

    IF v_source_item_expected_count IS DISTINCT FROM v_source_item_applied_count THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('SOURCE_EVIDENCE_MISSING');
    END IF;

    SELECT
        pg_catalog.count(DISTINCT reservation_row.id)::integer,
        pg_catalog.count(DISTINCT reservation_row.id) FILTER (
            WHERE reservation_row.status = 'RELEASED'
              AND reservation_row.released_at_utc IS NOT NULL
        )::integer
    INTO
        v_reservation_release_expected_count,
        v_reservation_release_actual_count
    FROM public.pay_advance_reservations AS reservation_row
    JOIN public.pay_payment_correction_request_candidates AS member_row
      ON reservation_row.pay_batch_item_id = ANY(member_row.pay_batch_item_ids)
    JOIN public.pay_payment_correction_work_items AS work_row
      ON work_row.correction_request_id = member_row.correction_request_id
     AND work_row.pay_batch_candidate_id = member_row.pay_batch_candidate_id
    WHERE member_row.correction_request_id = v_request.id
      AND work_row.status = 'APPLIED'
      AND reservation_row.status IN ('RESERVED', 'COMMITTED', 'RELEASED')
      AND (
          reservation_row.status <> 'COMMITTED'
          OR reservation_row.settled_at_utc IS NULL
      );

    IF v_reservation_release_expected_count
           IS DISTINCT FROM v_reservation_release_actual_count THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('RESERVATION_RELEASE_MISMATCH');
    END IF;

    SELECT
        pg_catalog.count(DISTINCT candidate_row.id) FILTER (
            WHERE active_item.pay_batch_candidate_id IS NOT NULL
        )::integer,
        pg_catalog.coalesce(
            pg_catalog.sum(active_item.amount_inc_vat),
            0
        )::numeric(14,2)
    INTO
        v_active_candidate_count,
        v_expected_active_total
    FROM public.pay_batch_candidates AS candidate_row
    LEFT JOIN public.pay_batch_items AS active_item
      ON active_item.pay_batch_candidate_id = candidate_row.id
     AND pg_catalog.coalesce(active_item.is_voided, false) IS NOT TRUE
    WHERE candidate_row.pay_batch_id = v_request.pay_batch_id;

    SELECT pg_catalog.coalesce(batch_row.total_bank_out, 0)::numeric(14,2)
    INTO v_actual_active_total
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = v_request.pay_batch_id;

    v_active_total_difference := pg_catalog.round(
        v_actual_active_total - v_expected_active_total,
        2
    );

    IF v_active_total_difference <> 0::numeric THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('ACTIVE_TOTAL_MISMATCH');
    END IF;

    IF v_operation_id IS NOT NULL THEN
        SELECT
            final_chunk.result_json ->> 'unselected_before_hash',
            final_chunk.result_json ->> 'unselected_after_hash'
        INTO
            v_unselected_before_hash,
            v_unselected_after_hash
        FROM public.banking_pay_operation_chunks AS final_chunk
        WHERE final_chunk.operation_id = v_operation_id
          AND final_chunk.phase = 'FINALISE'
          AND final_chunk.chunk_type = 'CANDIDATE_SCOPE'
          AND (
              final_chunk.result_json ? 'unselected_before_hash'
              OR final_chunk.result_json ? 'unselected_after_hash'
          )
        ORDER BY final_chunk.sequence_no DESC
        LIMIT 1;
    END IF;

    IF v_unselected_before_hash IS NULL
       AND v_unselected_after_hash IS NULL THEN
        v_warnings := v_warnings
            || pg_catalog.jsonb_build_array('UNSELECTED_SCOPE_HASH_NOT_YET_AVAILABLE');
        v_unselected_unchanged := NOT v_terminal_request;
    ELSE
        v_unselected_unchanged :=
            v_unselected_before_hash IS NOT DISTINCT FROM v_unselected_after_hash;
        IF NOT v_unselected_unchanged THEN
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('UNSELECTED_SCOPE_CHANGED');
        END IF;
    END IF;

    BEGIN
        v_old_auth_request_id := NULLIF(
            v_request.plan_json ->> 'old_authorisation_request_id',
            ''
        )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
        v_old_auth_request_id := NULL;
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('OLD_AUTHORISATION_STILL_ACTIVE');
    END;

    IF v_request.authorised_at_utc IS NOT NULL
       AND v_old_auth_request_id IS NOT NULL THEN
        SELECT NOT EXISTS (
            SELECT 1
            FROM public.pay_batch_auth_requests AS auth_request
            WHERE auth_request.id = v_old_auth_request_id
              AND auth_request.state IN (
                  'AWAITING',
                  'REQUESTED',
                  'AUTHORISED',
                  'AUTHORIZED'
              )
        )
        INTO v_old_authorisation_invalidated;

        IF NOT v_old_authorisation_invalidated THEN
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('OLD_AUTHORISATION_STILL_ACTIVE');
        END IF;
    END IF;

    v_expected_old_schedule_kind :=
        v_request.plan_json ->> 'old_schedule_kind';
    BEGIN
        v_expected_old_scheduled_at := NULLIF(
            v_request.plan_json ->> 'old_scheduled_at_utc',
            ''
        )::timestamptz;
    EXCEPTION WHEN invalid_datetime_format THEN
        v_expected_old_scheduled_at := NULL;
    END;

    IF v_request.authorised_at_utc IS NOT NULL
       AND (
           v_expected_old_schedule_kind IS NOT NULL
           OR v_expected_old_scheduled_at IS NOT NULL
       ) THEN
        SELECT
            batch_row.schedule_kind IS NULL
            AND batch_row.scheduled_at_utc IS NULL
        INTO v_old_schedule_invalidated
        FROM public.pay_batches AS batch_row
        WHERE batch_row.id = v_request.pay_batch_id;

        IF NOT v_old_schedule_invalidated THEN
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('OLD_SCHEDULE_STILL_ACTIVE');
        END IF;
    END IF;

    SELECT pg_catalog.count(*)::integer
    INTO v_actionable_cancelled_provider_scope_count
    FROM public.pay_payment_correction_items AS correction_item
    JOIN public.banking_pay_operation_transfer_scope_items AS scope_item
      ON scope_item.pay_batch_item_id = correction_item.pay_batch_item_id
    JOIN public.banking_pay_operation_transfer_scope AS transfer_scope
      ON transfer_scope.id = scope_item.transfer_scope_id
    WHERE correction_item.correction_request_id = v_request.id
      AND correction_item.status = 'APPLIED'
      AND (
          pg_catalog.coalesce(transfer_scope.provider_submit_ready, false)
          OR transfer_scope.provider_submit_state IN (
              'READY',
              'CLAIMED',
              'SENDING',
              'SUBMIT_READY'
          )
      );

    IF v_actionable_cancelled_provider_scope_count > 0 THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array(
                'CANCELLED_PROVIDER_SCOPE_ACTIONABLE'
            );
    END IF;

    IF v_operation_id IS NOT NULL THEN
        SELECT
            pg_catalog.count(*)::integer,
            pg_catalog.count(*) FILTER (
                WHERE refresh_chunk.status = 'COMPLETE'
            )::integer
        INTO
            v_refresh_group_total,
            v_refresh_group_complete
        FROM public.banking_pay_operation_chunks AS refresh_chunk
        WHERE refresh_chunk.operation_id = v_operation_id
          AND refresh_chunk.phase = 'REFRESH_WORKBENCH'
          AND refresh_chunk.chunk_type = 'CANDIDATE_SCOPE';
    END IF;

    v_workbench_refresh_status := CASE
        WHEN v_refresh_group_total = 0 THEN 'NOT_STAGED'
        WHEN v_refresh_group_complete = v_refresh_group_total THEN 'CURRENT'
        WHEN EXISTS (
            SELECT 1
            FROM public.banking_pay_operation_chunks AS failed_refresh_chunk
            WHERE failed_refresh_chunk.operation_id = v_operation_id
              AND failed_refresh_chunk.phase = 'REFRESH_WORKBENCH'
              AND failed_refresh_chunk.status IN ('FAILED', 'FAILED_FINAL')
        ) THEN 'FAILED'
        WHEN v_refresh_group_complete > 0 THEN 'PENDING'
        ELSE 'STAGED'
    END;

    IF v_terminal_request
       AND v_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED')
       AND v_workbench_refresh_status = 'NOT_STAGED' THEN
        v_failure_categories := v_failure_categories
            || pg_catalog.jsonb_build_array('WORKBENCH_REFRESH_NOT_STAGED');
    END IF;

    v_failure_categories := (
        SELECT pg_catalog.coalesce(
            pg_catalog.jsonb_agg(category_value ORDER BY first_ordinal),
            '[]'::jsonb
        )
        FROM (
            SELECT category_value,
                   pg_catalog.min(category_ordinal) AS first_ordinal
            FROM pg_catalog.jsonb_array_elements_text(
                v_failure_categories
            ) WITH ORDINALITY AS category_rows(
                category_value,
                category_ordinal
            )
            GROUP BY category_value
        ) AS unique_categories
    );

    RETURN pg_catalog.jsonb_build_object(
        'ok', pg_catalog.jsonb_array_length(v_failure_categories) = 0,
        'status', CASE
            WHEN pg_catalog.jsonb_array_length(v_failure_categories) > 0
                THEN 'FAIL'
            WHEN NOT v_terminal_request
                THEN 'IN_PROGRESS'
            ELSE 'PASS'
        END,
        'check_version', 1,
        'checked_at_utc', v_checked_at_utc,
        'correction_request_id', v_request.id,
        'operation_id', v_operation_id,
        'pay_batch_id', v_request.pay_batch_id,
        'selected_candidate_count', v_selected_candidate_count,
        'materialised_work_count', v_materialised_work_count,
        'terminal_work_count', v_terminal_work_count,
        'applied_candidate_count', v_applied_candidate_count,
        'blocked_candidate_count', v_blocked_candidate_count,
        'failed_candidate_count', v_failed_candidate_count,
        'source_item_expected_count', v_source_item_expected_count,
        'source_item_applied_count', v_source_item_applied_count,
        'reservation_release_expected_count',
            v_reservation_release_expected_count,
        'reservation_release_actual_count',
            v_reservation_release_actual_count,
        'active_candidate_count', v_active_candidate_count,
        'expected_active_total', v_expected_active_total,
        'actual_active_total', v_actual_active_total,
        'active_total_difference', v_active_total_difference,
        'old_authorisation_invalidated', v_old_authorisation_invalidated,
        'old_schedule_invalidated', v_old_schedule_invalidated,
        'actionable_cancelled_provider_scope_count',
            v_actionable_cancelled_provider_scope_count,
        'unselected_before_hash', v_unselected_before_hash,
        'unselected_after_hash', v_unselected_after_hash,
        'unselected_unchanged', v_unselected_unchanged,
        'refresh_group_total', v_refresh_group_total,
        'refresh_group_complete', v_refresh_group_complete,
        'workbench_refresh_status', v_workbench_refresh_status,
        'failure_categories', v_failure_categories,
        'warnings', v_warnings
    );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) TO service_role;
