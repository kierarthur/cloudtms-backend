-- Banking Pay preview-page concurrency contract.
-- Exposes the authoritative shared-session revision used by the frontend
-- immediately before draft creation. This changes no economic or frozen-batch logic.

\ir 20082026_1502_pay_workbench_preview_recovery_residual_current_v1.sql

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_preview_page(p_session_id uuid, p_section text, p_cursor_json jsonb DEFAULT '{}'::jsonb, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_requested_section text := lower(COALESCE(NULLIF(BTRIM(p_section), ''), 'canonical_preview_lines'));
  v_resolved_section text := 'canonical_preview_lines';
  v_cursor_section_raw text := NULL::text;
  v_cursor_resolved_section text := NULL::text;
  v_section_alias_applied boolean := false;
  v_cursor_json jsonb := CASE WHEN jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_cursor_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_last_row_ordinal bigint := NULL::bigint;
  v_last_id uuid := NULL::uuid;
  v_items jsonb := '[]'::jsonb;
  v_returned_count integer := 0;
  v_raw_count integer := 0;
  v_known_count integer := 0;
  v_selected_eligible_count integer := 0;
  v_eligible_row_ids uuid[] := ARRAY[]::uuid[];
  v_next_cursor jsonb := NULL::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_PAGE');

  v_resolved_section := CASE
    WHEN v_requested_section IN ('', 'canonical_preview_lines', 'ready_to_pay', 'ready_preview_lines', 'preview_rows') THEN 'canonical_preview_lines'
    WHEN v_requested_section IN ('cases_resolutions', 'case_resolution_states', 'case_resolutions', 'cases', 'resolutions') THEN 'cases_resolutions'
    WHEN v_requested_section IN ('blocked_for_pay', 'blocked_items', 'blocked_preview_lines', 'blocked_now', 'do_not_pay_items') THEN 'blocked_for_pay'
    WHEN v_requested_section IN ('drafted', 'drafted_rows') THEN 'drafted'
    ELSE v_requested_section
  END;
  v_section_alias_applied := v_requested_section IS DISTINCT FROM v_resolved_section;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'WORKBENCH_SESSION_NOT_FOUND',
      'code', 'WORKBENCH_SESSION_NOT_FOUND',
      'session_id', p_session_id::text,
      'requested_section', v_requested_section,
      'resolved_section', v_resolved_section,
      'section_alias_applied', v_section_alias_applied,
      'section', v_resolved_section,
      'items', '[]'::jsonb,
      'rows', '[]'::jsonb,
      'next_cursor', NULL::jsonb,
      'returned_count', 0,
      'ready', false,
      'ready_flag', false
    );
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL
     OR v_session_row.replacement_session_id IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'OBSOLETE_SESSION',
      'code', 'OBSOLETE_SESSION',
      'session_id', p_session_id::text,
      'session_obsolete', true,
      'replacement_required', true,
      'replacement_session_id', CASE WHEN v_session_row.replacement_session_id IS NULL THEN NULL ELSE v_session_row.replacement_session_id::text END,
      'replacement_available', v_session_row.replacement_session_id IS NOT NULL,
      'requested_section', v_requested_section,
      'resolved_section', v_resolved_section,
      'section_alias_applied', v_section_alias_applied,
      'section', v_resolved_section,
      'items', '[]'::jsonb,
      'rows', '[]'::jsonb,
      'next_cursor', NULL::jsonb,
      'returned_count', 0,
      'ready', false,
      'ready_flag', false
    );
  END IF;

  v_cursor_section_raw := lower(COALESCE(NULLIF(BTRIM(v_cursor_json->>'section'), ''), v_resolved_section));
  v_cursor_resolved_section := CASE
    WHEN v_cursor_section_raw IN ('', 'canonical_preview_lines', 'ready_to_pay', 'ready_preview_lines', 'preview_rows') THEN 'canonical_preview_lines'
    WHEN v_cursor_section_raw IN ('cases_resolutions', 'case_resolution_states', 'case_resolutions', 'cases', 'resolutions') THEN 'cases_resolutions'
    WHEN v_cursor_section_raw IN ('blocked_for_pay', 'blocked_items', 'blocked_preview_lines', 'blocked_now', 'do_not_pay_items') THEN 'blocked_for_pay'
    WHEN v_cursor_section_raw IN ('drafted', 'drafted_rows') THEN 'drafted'
    ELSE v_cursor_section_raw
  END;

  IF v_cursor_json ? 'section' AND v_cursor_resolved_section <> v_resolved_section THEN
    RAISE EXCEPTION 'cursor section % does not match requested section %', COALESCE(v_cursor_json->>'section', ''), v_requested_section;
  END IF;

  IF COALESCE(v_cursor_json->>'last_row_ordinal', '') ~ '^[0-9]+$' THEN
    v_last_row_ordinal := (v_cursor_json->>'last_row_ordinal')::bigint;
  END IF;

  IF COALESCE(v_cursor_json->>'last_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_last_id := (v_cursor_json->>'last_id')::uuid;
  END IF;

  -- Resolve the complete eligible identity set once.  The count and returned
  -- page are both derived from this array, so Cases/Ready/Blocked cannot drift
  -- through duplicated reader predicates.
  WITH session_preview_rows AS MATERIALIZED (
    SELECT
      preview_row.*,
      UPPER(BTRIM(COALESCE(
        preview_row.row_json->>'line_type',
        preview_row.row_json#>>'{preview_contract,line_type}',
        ''
      ))) AS authority_line_type,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'finance_case_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'finance_case_id')::uuid
        ELSE NULL::uuid
      END AS typed_finance_case_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'finance_component_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'finance_component_id')::uuid
        ELSE NULL::uuid
      END AS typed_finance_component_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'post_draft_overlay_pay_batch_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'post_draft_overlay_pay_batch_id')::uuid
        ELSE NULL::uuid
      END AS typed_overlay_pay_batch_id
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
  ), owned_recovery_cases AS MATERIALIZED (
    SELECT DISTINCT
      preview_row.candidate_id,
      preview_row.typed_finance_case_id AS finance_case_id
    FROM session_preview_rows AS preview_row
    JOIN public.pay_advances AS finance_case
      ON finance_case.id = preview_row.typed_finance_case_id
     AND finance_case.candidate_id = preview_row.candidate_id
    WHERE preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
      AND preview_row.status = 'READY'
  ), recovery_reservation_totals AS MATERIALIZED (
    SELECT
      recovery_case.candidate_id,
      recovery_case.finance_case_id,
      ROUND(COALESCE(SUM(reservation.reserved_amount), 0), 2)
        AS current_active_reserved_ex_vat,
      COUNT(reservation.id)::integer AS current_active_reservation_count
    FROM owned_recovery_cases AS recovery_case
    LEFT JOIN public.pay_advance_reservations AS reservation
      ON reservation.finance_case_id = recovery_case.finance_case_id
     AND UPPER(BTRIM(COALESCE(reservation.status, '')))
          IN ('RESERVED', 'COMMITTED')
    GROUP BY recovery_case.candidate_id, recovery_case.finance_case_id
  ), active_item_overlap_rows AS MATERIALIZED (
    SELECT DISTINCT preview_row.id
    FROM session_preview_rows AS preview_row
    JOIN public.pay_batch_candidates AS active_candidate
      ON active_candidate.candidate_id = preview_row.candidate_id
    JOIN public.pay_batches AS active_batch
      ON active_batch.id = active_candidate.pay_batch_id
     AND active_batch.cancelled_at_utc IS NULL
     AND UPPER(BTRIM(COALESCE(active_batch.status, ''))) IN (
       'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
       'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
       'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
     )
    JOIN public.pay_batch_items AS active_item
      ON active_item.pay_batch_candidate_id = active_candidate.id
     AND COALESCE(active_item.is_voided, false) = false
     AND (
       (
         COALESCE(
           active_item.timesheet_id,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_source_basis_json->>'timesheet_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(active_item.frozen_source_basis_json->>'timesheet_id')::uuid
             ELSE NULL::uuid
           END,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_source_basis_json->>'linked_timesheet_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(active_item.frozen_source_basis_json->>'linked_timesheet_id')::uuid
             ELSE NULL::uuid
           END,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(
                 active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
               )::uuid
             ELSE NULL::uuid
           END
         ) = preview_row.timesheet_id
         AND (
           (
             NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_type, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_value, '')), '') IS NOT NULL
             AND UPPER(BTRIM(active_item.frozen_component_key_type))
               = UPPER(BTRIM(COALESCE(
                 preview_row.key_type, preview_row.row_json->>'key_type', ''
               )))
             AND BTRIM(active_item.frozen_component_key_value)
               = BTRIM(COALESCE(
                 preview_row.key_value, preview_row.row_json->>'key_value', ''
               ))
           )
           OR (
             NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_type, '')), '') IS NULL
             AND NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_value, '')), '') IS NULL
           )
         )
       )
       OR active_item.finance_component_id = preview_row.typed_finance_component_id
       OR active_item.finance_case_id = preview_row.typed_finance_case_id
       OR (
         NULLIF(BTRIM(COALESCE(
           preview_row.row_json->>'canonical_correction_key', ''
         )), '') IS NOT NULL
         AND NULLIF(BTRIM(COALESCE(
           active_item.frozen_component_snapshot_json->>'canonical_correction_key',
           active_item.frozen_resolution_payload_json->>'canonical_correction_key',
           ''
         )), '') = NULLIF(BTRIM(COALESCE(
           preview_row.row_json->>'canonical_correction_key', ''
         )), '')
       )
     )
  ), strict_recovery_siblings AS MATERIALIZED (
    SELECT
      recovery_sibling.id,
      recovery_sibling.candidate_id,
      recovery_sibling.timesheet_id
    FROM session_preview_rows AS recovery_sibling
    WHERE recovery_sibling.status = 'READY'
      AND recovery_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'

    UNION

    SELECT DISTINCT
      recovery_sibling.id,
      recovery_sibling.candidate_id,
      recovery_sibling.timesheet_id
    FROM session_preview_rows AS recovery_sibling
    JOIN public.pay_batches AS frozen_recovery_batch
      ON frozen_recovery_batch.id = recovery_sibling.typed_overlay_pay_batch_id
     AND frozen_recovery_batch.cancelled_at_utc IS NULL
     AND UPPER(BTRIM(COALESCE(frozen_recovery_batch.status, ''))) IN (
       'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
       'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
       'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
     )
    JOIN public.pay_batch_candidates AS frozen_recovery_candidate
      ON frozen_recovery_candidate.pay_batch_id = frozen_recovery_batch.id
     AND frozen_recovery_candidate.candidate_id = recovery_sibling.candidate_id
    JOIN public.pay_batch_items AS frozen_recovery_item
      ON frozen_recovery_item.pay_batch_candidate_id = frozen_recovery_candidate.id
     AND COALESCE(frozen_recovery_item.is_voided, false) = false
     AND UPPER(BTRIM(COALESCE(frozen_recovery_item.item_type, '')))
          = 'OVERPAYMENT_RECOVERY'
     AND frozen_recovery_item.finance_case_id = recovery_sibling.typed_finance_case_id
     AND frozen_recovery_item.finance_component_id = recovery_sibling.typed_finance_component_id
     AND UPPER(BTRIM(COALESCE(
       frozen_recovery_item.frozen_component_key_type, ''
     ))) = UPPER(BTRIM(COALESCE(
       recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
     )))
     AND BTRIM(COALESCE(frozen_recovery_item.frozen_component_key_value, ''))
          = BTRIM(COALESCE(
              recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
            ))
     AND COALESCE(
       frozen_recovery_item.timesheet_id,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_source_basis_json->>'timesheet_id', ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_source_basis_json->>'timesheet_id'
           )::uuid
         ELSE NULL::uuid
       END,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id', ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id'
           )::uuid
         ELSE NULL::uuid
       END,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
           ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
           )::uuid
         ELSE NULL::uuid
       END
     ) = recovery_sibling.timesheet_id
    WHERE recovery_sibling.status = 'SUPERSEDED'
      AND recovery_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'
      AND recovery_sibling.timesheet_id IS NOT NULL
      AND COALESCE(LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_unavailable', ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
      AND COALESCE(LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_applied', ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
      AND LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_active', 'true'
      ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
      AND UPPER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_operation_type', ''
      ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
      AND recovery_sibling.typed_finance_case_id IS NOT NULL
      AND recovery_sibling.typed_finance_component_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(
        recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
      )), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(
        recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
      )), '') IS NOT NULL
  ), preview_authority AS MATERIALIZED (
    SELECT
      preview_row.*,
      COALESCE(reservation_total.current_active_reserved_ex_vat, 0)::numeric
        AS current_active_reserved_ex_vat,
      COALESCE(reservation_total.current_active_reservation_count, 0)::integer
        AS current_active_reservation_count,
      (active_overlap.id IS NOT NULL) AS exact_active_item_overlap,
      CASE
        WHEN preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY' THEN
          private.pay_workbench_preview_recovery_residual_is_current_v1(
            preview_row.status,
            preview_row.candidate_id,
            finance_case.candidate_id,
            finance_case.id,
            preview_row.row_json,
            COALESCE(reservation_total.current_active_reserved_ex_vat, 0),
            COALESCE(reservation_total.current_active_reservation_count, 0),
            active_overlap.id IS NOT NULL
          )
        ELSE true
      END AS recovery_residual_is_current
    FROM session_preview_rows AS preview_row
    LEFT JOIN public.pay_advances AS finance_case
      ON finance_case.id = preview_row.typed_finance_case_id
     AND finance_case.candidate_id = preview_row.candidate_id
    LEFT JOIN recovery_reservation_totals AS reservation_total
      ON reservation_total.candidate_id = preview_row.candidate_id
     AND reservation_total.finance_case_id = preview_row.typed_finance_case_id
    LEFT JOIN active_item_overlap_rows AS active_overlap
      ON active_overlap.id = preview_row.id
  ), eligible_rows AS (
    SELECT preview_row.id, preview_row.selected
    FROM preview_authority AS preview_row
    WHERE LOWER(private.pay_workbench_preview_effective_section_v1(
      preview_row.section, preview_row.row_json
    )) = v_resolved_section
      AND preview_row.status = 'READY'
      AND NOT (
        COALESCE(LOWER(BTRIM(COALESCE(
          preview_row.row_json->>'post_draft_unavailable', ''
        ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
        OR (
          COALESCE(LOWER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_applied', ''
          ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
          AND UPPER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_operation_type', ''
          ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND LOWER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_active', 'true'
          ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        )
      )
      AND preview_row.recovery_residual_is_current
      AND NOT (
        preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
        AND private.pay_workbench_preview_effective_section_v1(
          preview_row.section, preview_row.row_json
        ) <> 'cases_resolutions'
        AND preview_row.typed_finance_case_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM session_preview_rows AS actionable_sibling
          WHERE actionable_sibling.candidate_id = preview_row.candidate_id
            AND actionable_sibling.id <> preview_row.id
            AND actionable_sibling.status = 'READY'
            AND actionable_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'
            AND actionable_sibling.typed_finance_case_id = preview_row.typed_finance_case_id
            AND private.pay_workbench_preview_effective_section_v1(
              actionable_sibling.section, actionable_sibling.row_json
            ) = 'cases_resolutions'
        )
      )
      AND NOT (
        preview_row.authority_line_type = 'TIMESHEET_PAYMENT'
        AND UPPER(BTRIM(COALESCE(
          preview_row.row_json->>'presentation_reason', ''
        ))) = 'NEGATIVE_ORDINARY_PRESENTATION_ONLY'
        AND preview_row.timesheet_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM strict_recovery_siblings AS recovery_sibling
          WHERE recovery_sibling.candidate_id = preview_row.candidate_id
            AND recovery_sibling.id <> preview_row.id
            AND recovery_sibling.timesheet_id = preview_row.timesheet_id
        )
      )
      AND (
        preview_row.exact_active_item_overlap IS NOT TRUE
        OR (
          preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
          AND preview_row.recovery_residual_is_current
          AND CASE
            WHEN jsonb_typeof(
              preview_row.row_json->'recovery_active_reserved_ex_vat'
            ) = 'number'
              THEN (preview_row.row_json->>'recovery_active_reserved_ex_vat')::numeric > 0
            ELSE false
          END
        )
      )
  )
  SELECT
    COALESCE(ARRAY_AGG(eligible_row.id ORDER BY eligible_row.id), ARRAY[]::uuid[]),
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE eligible_row.selected IS TRUE)::integer
  INTO v_eligible_row_ids, v_known_count, v_selected_eligible_count
  FROM eligible_rows AS eligible_row;

  IF false THEN

  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE preview_count_row.selected IS TRUE)::integer
  INTO v_known_count, v_selected_eligible_count
  FROM public.banking_pay_workbench_preview_rows AS preview_count_row
  WHERE preview_count_row.session_id = p_session_id
    AND preview_count_row.session_version = v_session_row.version
    AND LOWER(private.pay_workbench_preview_effective_section_v1(
      preview_count_row.section, preview_count_row.row_json
    )) = v_resolved_section
    AND preview_count_row.status = 'READY'
    AND NOT (
      UPPER(BTRIM(COALESCE(
        preview_count_row.row_json->>'line_type',
        preview_count_row.row_json#>>'{preview_contract,line_type}',
        ''
      ))) = 'OVERPAYMENT_RECOVERY'
      AND private.pay_workbench_preview_effective_section_v1(
        preview_count_row.section, preview_count_row.row_json
      ) <> 'cases_resolutions'
      AND NULLIF(BTRIM(COALESCE(
        preview_count_row.row_json->>'finance_case_id', ''
      )), '') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS actionable_sibling
        WHERE actionable_sibling.session_id = preview_count_row.session_id
          AND actionable_sibling.session_version = preview_count_row.session_version
          AND actionable_sibling.candidate_id = preview_count_row.candidate_id
          AND actionable_sibling.id <> preview_count_row.id
          AND actionable_sibling.status = 'READY'
          AND UPPER(BTRIM(COALESCE(
            actionable_sibling.row_json->>'line_type',
            actionable_sibling.row_json#>>'{preview_contract,line_type}',
            ''
          ))) = 'OVERPAYMENT_RECOVERY'
          AND NULLIF(BTRIM(COALESCE(
            actionable_sibling.row_json->>'finance_case_id', ''
          )), '') = NULLIF(BTRIM(COALESCE(
            preview_count_row.row_json->>'finance_case_id', ''
          )), '')
          AND private.pay_workbench_preview_effective_section_v1(
            actionable_sibling.section, actionable_sibling.row_json
        ) = 'cases_resolutions'
      )
    )
    AND NOT (
      UPPER(BTRIM(COALESCE(
        preview_count_row.row_json->>'line_type',
        preview_count_row.row_json#>>'{preview_contract,line_type}',
        ''
      ))) = 'TIMESHEET_PAYMENT'
      AND UPPER(BTRIM(COALESCE(
        preview_count_row.row_json->>'presentation_reason', ''
      ))) = 'NEGATIVE_ORDINARY_PRESENTATION_ONLY'
      AND preview_count_row.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS recovery_sibling
        WHERE recovery_sibling.session_id = preview_count_row.session_id
          AND recovery_sibling.session_version = preview_count_row.session_version
          AND recovery_sibling.candidate_id = preview_count_row.candidate_id
          AND recovery_sibling.id <> preview_count_row.id
          AND recovery_sibling.timesheet_id = preview_count_row.timesheet_id
          AND UPPER(BTRIM(COALESCE(
            recovery_sibling.row_json->>'line_type',
            recovery_sibling.row_json#>>'{preview_contract,line_type}',
            ''
          ))) = 'OVERPAYMENT_RECOVERY'
          AND (
            recovery_sibling.status = 'READY'
            OR (
              recovery_sibling.status = 'SUPERSEDED'
              AND COALESCE(
                LOWER(BTRIM(COALESCE(
                  recovery_sibling.row_json->>'post_draft_unavailable', ''
                ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                false
              )
              AND COALESCE(
                LOWER(BTRIM(COALESCE(
                  recovery_sibling.row_json->>'post_draft_overlay_applied', ''
                ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                false
              )
              AND LOWER(BTRIM(COALESCE(
                recovery_sibling.row_json->>'post_draft_overlay_active', 'true'
              ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
              AND UPPER(BTRIM(COALESCE(
                recovery_sibling.row_json->>'post_draft_overlay_operation_type', ''
              ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
              AND NULLIF(BTRIM(COALESCE(
                recovery_sibling.row_json->>'post_draft_overlay_pay_batch_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              AND NULLIF(BTRIM(COALESCE(
                recovery_sibling.row_json->>'finance_case_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              AND NULLIF(BTRIM(COALESCE(
                recovery_sibling.row_json->>'finance_component_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              AND NULLIF(BTRIM(COALESCE(
                recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
              )), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(
                recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
              )), '') IS NOT NULL
              AND EXISTS (
                SELECT 1
                FROM public.pay_batch_items AS frozen_recovery_item
                JOIN public.pay_batch_candidates AS frozen_recovery_candidate
                  ON frozen_recovery_candidate.id = frozen_recovery_item.pay_batch_candidate_id
                JOIN public.pay_batches AS frozen_recovery_batch
                  ON frozen_recovery_batch.id = frozen_recovery_candidate.pay_batch_id
                WHERE frozen_recovery_batch.id = BTRIM(
                        recovery_sibling.row_json->>'post_draft_overlay_pay_batch_id'
                      )::uuid
                  AND frozen_recovery_candidate.candidate_id = recovery_sibling.candidate_id
                  AND COALESCE(frozen_recovery_item.is_voided, false) = false
                  AND UPPER(BTRIM(COALESCE(
                    frozen_recovery_item.item_type, ''
                  ))) = 'OVERPAYMENT_RECOVERY'
                  AND frozen_recovery_batch.cancelled_at_utc IS NULL
                  AND UPPER(BTRIM(COALESCE(
                    frozen_recovery_batch.status, ''
                  ))) IN (
                    'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
                    'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
                    'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
                  )
                  AND frozen_recovery_item.finance_case_id = BTRIM(
                        recovery_sibling.row_json->>'finance_case_id'
                      )::uuid
                  AND frozen_recovery_item.finance_component_id = BTRIM(
                        recovery_sibling.row_json->>'finance_component_id'
                      )::uuid
                  AND UPPER(BTRIM(COALESCE(
                    frozen_recovery_item.frozen_component_key_type, ''
                  ))) = UPPER(BTRIM(COALESCE(
                    recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
                  )))
                  AND BTRIM(COALESCE(
                    frozen_recovery_item.frozen_component_key_value, ''
                  )) = BTRIM(COALESCE(
                    recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
                  ))
                  AND COALESCE(
                    frozen_recovery_item.timesheet_id,
                    CASE
                      WHEN NULLIF(BTRIM(COALESCE(
                        frozen_recovery_item.frozen_source_basis_json->>'timesheet_id', ''
                      )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(
                          frozen_recovery_item.frozen_source_basis_json->>'timesheet_id'
                        )::uuid
                      ELSE NULL::uuid
                    END,
                    CASE
                      WHEN NULLIF(BTRIM(COALESCE(
                        frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id', ''
                      )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(
                          frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id'
                        )::uuid
                      ELSE NULL::uuid
                    END,
                    CASE
                      WHEN NULLIF(BTRIM(COALESCE(
                        frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                        ''
                      )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(
                          frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
                        )::uuid
                      ELSE NULL::uuid
                    END
                  ) = recovery_sibling.timesheet_id
              )
            )
          )
      )
    )
    AND (
      v_resolved_section <> 'canonical_preview_lines'
      OR NOT (
        COALESCE(
          lower(BTRIM(COALESCE(preview_count_row.row_json->>'post_draft_unavailable', '')))
            IN ('true', 't', '1', 'yes', 'y', 'on'),
          false
        )
        OR (
          COALESCE(
            lower(BTRIM(COALESCE(preview_count_row.row_json->>'post_draft_overlay_applied', '')))
              IN ('true', 't', '1', 'yes', 'y', 'on'),
            false
          )
          AND UPPER(BTRIM(COALESCE(preview_count_row.row_json->>'post_draft_overlay_operation_type', '')))
            IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND lower(BTRIM(COALESCE(preview_count_row.row_json->>'post_draft_overlay_active', 'true')))
            NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        )
      )
    )
    AND (
      v_resolved_section <> 'canonical_preview_lines'
      OR NOT EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS active_batch_item
        JOIN public.pay_batch_candidates AS active_batch_candidate
          ON active_batch_candidate.id = active_batch_item.pay_batch_candidate_id
        JOIN public.pay_batches AS active_batch
          ON active_batch.id = active_batch_candidate.pay_batch_id
        WHERE active_batch_candidate.candidate_id = preview_count_row.candidate_id
          AND COALESCE(active_batch_item.is_voided, false) = false
          AND active_batch.cancelled_at_utc IS NULL
          AND UPPER(BTRIM(COALESCE(active_batch.status, ''))) IN (
            'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
            'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
            'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
          )
          AND (
            (
              COALESCE(
                active_batch_item.timesheet_id,
                CASE
                  WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(BTRIM(COALESCE(active_batch_item.frozen_source_basis_json->>'timesheet_id', '')), '')
                     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN BTRIM(active_batch_item.frozen_source_basis_json->>'timesheet_id')::uuid
                  ELSE NULL::uuid
                END,
                CASE
                  WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(BTRIM(COALESCE(active_batch_item.frozen_source_basis_json->>'linked_timesheet_id', '')), '')
                     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN BTRIM(active_batch_item.frozen_source_basis_json->>'linked_timesheet_id')::uuid
                  ELSE NULL::uuid
                END,
                CASE
                  WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(BTRIM(COALESCE(
                     active_batch_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                     ''
                   )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN BTRIM(
                      active_batch_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
                    )::uuid
                  ELSE NULL::uuid
                END
              ) = preview_count_row.timesheet_id
              AND UPPER(BTRIM(COALESCE(active_batch_item.frozen_component_key_type, '')))
                = UPPER(BTRIM(COALESCE(preview_count_row.key_type, preview_count_row.row_json->>'key_type', '')))
              AND BTRIM(COALESCE(active_batch_item.frozen_component_key_value, ''))
                = BTRIM(COALESCE(preview_count_row.key_value, preview_count_row.row_json->>'key_value', ''))
            )
            OR active_batch_item.finance_component_id::text
              = NULLIF(BTRIM(COALESCE(preview_count_row.row_json->>'finance_component_id', '')), '')
            OR active_batch_item.finance_case_id::text
              = NULLIF(BTRIM(COALESCE(preview_count_row.row_json->>'finance_case_id', '')), '')
            OR NULLIF(BTRIM(COALESCE(
              active_batch_item.frozen_component_snapshot_json->>'canonical_correction_key',
              active_batch_item.frozen_resolution_payload_json->>'canonical_correction_key',
              ''
            )), '') = NULLIF(BTRIM(COALESCE(preview_count_row.row_json->>'canonical_correction_key', '')), '')
          )
      )
    );
  END IF;

  WITH page_rows AS (
    SELECT page_source.*,
           ROW_NUMBER() OVER (ORDER BY page_source.row_ordinal, page_source.id) AS page_ordinal
    FROM (
      SELECT
        preview_row.id,
        LOWER(private.pay_workbench_preview_effective_section_v1(
          preview_row.section, preview_row.row_json
        )) AS section,
        preview_row.section AS physical_section,
        preview_row.candidate_id,
        preview_row.row_key,
        preview_row.row_ordinal,
        preview_row.row_json,
        preview_row.timesheet_id,
        preview_row.key_type,
        preview_row.key_value,
        preview_row.selected,
        preview_row.selection_state,
        preview_row.status,
        preview_row.session_version
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.id = ANY(v_eligible_row_ids)
        AND (
          true
          OR (
            preview_row.session_id = p_session_id
        AND preview_row.session_version = v_session_row.version
        AND LOWER(private.pay_workbench_preview_effective_section_v1(
          preview_row.section, preview_row.row_json
        )) = v_resolved_section
        AND preview_row.status = 'READY'
        AND NOT (
          UPPER(BTRIM(COALESCE(
            preview_row.row_json->>'line_type',
            preview_row.row_json#>>'{preview_contract,line_type}',
            ''
          ))) = 'OVERPAYMENT_RECOVERY'
          AND private.pay_workbench_preview_effective_section_v1(
            preview_row.section, preview_row.row_json
          ) <> 'cases_resolutions'
          AND NULLIF(BTRIM(COALESCE(
            preview_row.row_json->>'finance_case_id', ''
          )), '') IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_preview_rows AS actionable_sibling
            WHERE actionable_sibling.session_id = preview_row.session_id
              AND actionable_sibling.session_version = preview_row.session_version
              AND actionable_sibling.candidate_id = preview_row.candidate_id
              AND actionable_sibling.id <> preview_row.id
              AND actionable_sibling.status = 'READY'
              AND UPPER(BTRIM(COALESCE(
                actionable_sibling.row_json->>'line_type',
                actionable_sibling.row_json#>>'{preview_contract,line_type}',
                ''
              ))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                actionable_sibling.row_json->>'finance_case_id', ''
              )), '') = NULLIF(BTRIM(COALESCE(
                preview_row.row_json->>'finance_case_id', ''
              )), '')
              AND private.pay_workbench_preview_effective_section_v1(
                actionable_sibling.section, actionable_sibling.row_json
            ) = 'cases_resolutions'
          )
        )
        AND NOT (
          UPPER(BTRIM(COALESCE(
            preview_row.row_json->>'line_type',
            preview_row.row_json#>>'{preview_contract,line_type}',
            ''
          ))) = 'TIMESHEET_PAYMENT'
          AND UPPER(BTRIM(COALESCE(
            preview_row.row_json->>'presentation_reason', ''
          ))) = 'NEGATIVE_ORDINARY_PRESENTATION_ONLY'
          AND preview_row.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_preview_rows AS recovery_sibling
            WHERE recovery_sibling.session_id = preview_row.session_id
              AND recovery_sibling.session_version = preview_row.session_version
              AND recovery_sibling.candidate_id = preview_row.candidate_id
              AND recovery_sibling.id <> preview_row.id
              AND recovery_sibling.timesheet_id = preview_row.timesheet_id
              AND UPPER(BTRIM(COALESCE(
                recovery_sibling.row_json->>'line_type',
                recovery_sibling.row_json#>>'{preview_contract,line_type}',
                ''
              ))) = 'OVERPAYMENT_RECOVERY'
              AND (
                recovery_sibling.status = 'READY'
                OR (
                  recovery_sibling.status = 'SUPERSEDED'
                  AND COALESCE(
                    LOWER(BTRIM(COALESCE(
                      recovery_sibling.row_json->>'post_draft_unavailable', ''
                    ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                    false
                  )
                  AND COALESCE(
                    LOWER(BTRIM(COALESCE(
                      recovery_sibling.row_json->>'post_draft_overlay_applied', ''
                    ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                    false
                  )
                  AND LOWER(BTRIM(COALESCE(
                    recovery_sibling.row_json->>'post_draft_overlay_active', 'true'
                  ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
                  AND UPPER(BTRIM(COALESCE(
                    recovery_sibling.row_json->>'post_draft_overlay_operation_type', ''
                  ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
                  AND NULLIF(BTRIM(COALESCE(
                    recovery_sibling.row_json->>'post_draft_overlay_pay_batch_id', ''
                  )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  AND NULLIF(BTRIM(COALESCE(
                    recovery_sibling.row_json->>'finance_case_id', ''
                  )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  AND NULLIF(BTRIM(COALESCE(
                    recovery_sibling.row_json->>'finance_component_id', ''
                  )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  AND NULLIF(BTRIM(COALESCE(
                    recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
                  )), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(
                    recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
                  )), '') IS NOT NULL
                  AND EXISTS (
                    SELECT 1
                    FROM public.pay_batch_items AS frozen_recovery_item
                    JOIN public.pay_batch_candidates AS frozen_recovery_candidate
                      ON frozen_recovery_candidate.id = frozen_recovery_item.pay_batch_candidate_id
                    JOIN public.pay_batches AS frozen_recovery_batch
                      ON frozen_recovery_batch.id = frozen_recovery_candidate.pay_batch_id
                    WHERE frozen_recovery_batch.id = BTRIM(
                            recovery_sibling.row_json->>'post_draft_overlay_pay_batch_id'
                          )::uuid
                      AND frozen_recovery_candidate.candidate_id = recovery_sibling.candidate_id
                      AND COALESCE(frozen_recovery_item.is_voided, false) = false
                      AND UPPER(BTRIM(COALESCE(
                        frozen_recovery_item.item_type, ''
                      ))) = 'OVERPAYMENT_RECOVERY'
                      AND frozen_recovery_batch.cancelled_at_utc IS NULL
                      AND UPPER(BTRIM(COALESCE(
                        frozen_recovery_batch.status, ''
                      ))) IN (
                        'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
                        'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
                        'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
                      )
                      AND frozen_recovery_item.finance_case_id = BTRIM(
                            recovery_sibling.row_json->>'finance_case_id'
                          )::uuid
                      AND frozen_recovery_item.finance_component_id = BTRIM(
                            recovery_sibling.row_json->>'finance_component_id'
                          )::uuid
                      AND UPPER(BTRIM(COALESCE(
                        frozen_recovery_item.frozen_component_key_type, ''
                      ))) = UPPER(BTRIM(COALESCE(
                        recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
                      )))
                      AND BTRIM(COALESCE(
                        frozen_recovery_item.frozen_component_key_value, ''
                      )) = BTRIM(COALESCE(
                        recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
                      ))
                      AND COALESCE(
                        frozen_recovery_item.timesheet_id,
                        CASE
                          WHEN NULLIF(BTRIM(COALESCE(
                            frozen_recovery_item.frozen_source_basis_json->>'timesheet_id', ''
                          )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                            THEN BTRIM(
                              frozen_recovery_item.frozen_source_basis_json->>'timesheet_id'
                            )::uuid
                          ELSE NULL::uuid
                        END,
                        CASE
                          WHEN NULLIF(BTRIM(COALESCE(
                            frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id', ''
                          )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                            THEN BTRIM(
                              frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id'
                            )::uuid
                          ELSE NULL::uuid
                        END,
                        CASE
                          WHEN NULLIF(BTRIM(COALESCE(
                            frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                            ''
                          )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                            THEN BTRIM(
                              frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
                            )::uuid
                          ELSE NULL::uuid
                        END
                      ) = recovery_sibling.timesheet_id
                  )
                )
              )
          )
        )
        AND (
          v_resolved_section <> 'canonical_preview_lines'
          OR NOT (
            COALESCE(
              lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_unavailable', '')))
                IN ('true', 't', '1', 'yes', 'y', 'on'),
              false
            )
            OR (
              COALESCE(
                lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_applied', '')))
                  IN ('true', 't', '1', 'yes', 'y', 'on'),
                false
              )
              AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_operation_type', '')))
                IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
              AND lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_active', 'true')))
                NOT IN ('false', 'f', '0', 'no', 'n', 'off')
            )
          )
        )
        AND (
          v_resolved_section <> 'canonical_preview_lines'
          OR NOT EXISTS (
            SELECT 1
            FROM public.pay_batch_items AS active_batch_item
            JOIN public.pay_batch_candidates AS active_batch_candidate
              ON active_batch_candidate.id = active_batch_item.pay_batch_candidate_id
            JOIN public.pay_batches AS active_batch
              ON active_batch.id = active_batch_candidate.pay_batch_id
            WHERE active_batch_candidate.candidate_id = preview_row.candidate_id
              AND COALESCE(active_batch_item.is_voided, false) = false
              AND active_batch.cancelled_at_utc IS NULL
              AND UPPER(BTRIM(COALESCE(active_batch.status, ''))) IN (
                'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
                'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
                'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
              )
              AND (
                (
                  COALESCE(
                    active_batch_item.timesheet_id,
                    CASE
                      WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                       AND NULLIF(BTRIM(COALESCE(active_batch_item.frozen_source_basis_json->>'timesheet_id', '')), '')
                         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(active_batch_item.frozen_source_basis_json->>'timesheet_id')::uuid
                      ELSE NULL::uuid
                    END,
                    CASE
                      WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                       AND NULLIF(BTRIM(COALESCE(active_batch_item.frozen_source_basis_json->>'linked_timesheet_id', '')), '')
                         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(active_batch_item.frozen_source_basis_json->>'linked_timesheet_id')::uuid
                      ELSE NULL::uuid
                    END,
                    CASE
                      WHEN UPPER(BTRIM(COALESCE(active_batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                       AND NULLIF(BTRIM(COALESCE(
                         active_batch_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                         ''
                       )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN BTRIM(
                          active_batch_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
                        )::uuid
                      ELSE NULL::uuid
                    END
                  ) = preview_row.timesheet_id
                  AND UPPER(BTRIM(COALESCE(active_batch_item.frozen_component_key_type, '')))
                    = UPPER(BTRIM(COALESCE(preview_row.key_type, preview_row.row_json->>'key_type', '')))
                  AND BTRIM(COALESCE(active_batch_item.frozen_component_key_value, ''))
                    = BTRIM(COALESCE(preview_row.key_value, preview_row.row_json->>'key_value', ''))
                )
                OR active_batch_item.finance_component_id::text
                  = NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_component_id', '')), '')
                OR active_batch_item.finance_case_id::text
                  = NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')
                OR NULLIF(BTRIM(COALESCE(
                  active_batch_item.frozen_component_snapshot_json->>'canonical_correction_key',
                  active_batch_item.frozen_resolution_payload_json->>'canonical_correction_key',
                  ''
                )), '') = NULLIF(BTRIM(COALESCE(preview_row.row_json->>'canonical_correction_key', '')), '')
              )
          )
        )
          )
        )
        AND (
          v_last_row_ordinal IS NULL
          OR preview_row.row_ordinal > v_last_row_ordinal
          OR (
            preview_row.row_ordinal = v_last_row_ordinal
            AND v_last_id IS NOT NULL
            AND preview_row.id > v_last_id
          )
        )
      ORDER BY preview_row.row_ordinal, preview_row.id
      LIMIT (v_limit + 1)
    ) AS page_source
  ), limited_rows AS (
    SELECT page_rows.*
    FROM page_rows
    WHERE page_rows.page_ordinal <= v_limit
  ), normalised_rows AS (
    SELECT
      limited_rows.*,
      base_values.base_json,
      display_values.amount_display_text,
      display_values.section_amount_display_text,
      post_draft_values.post_draft_unavailable_bool,
      bool_values.selection_allowed_bool,
      bool_values.draftable_bool,
      bool_values.ready_for_draft_bool,
      bool_values.excluded_bool,
      bool_values.deduction_bool,
      bool_values.materialisable_bool,
      name_values.candidate_display_name_text,
      name_values.candidate_name_text
    FROM limited_rows
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN jsonb_typeof(limited_rows.row_json) = 'object' THEN limited_rows.row_json
          ELSE '{}'::jsonb
        END AS stored_row_json
    ) AS stored_values
    CROSS JOIN LATERAL (
      SELECT
        stored_values.stored_row_json
        || jsonb_build_object(
          'preview_row_id', limited_rows.id::text,
          'row_id', limited_rows.id::text,
          'section', limited_rows.section,
          'effective_section', limited_rows.section,
          'physical_section', limited_rows.physical_section,
          'presentation_section', CASE limited_rows.section
            WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
            WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
            WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
            ELSE UPPER(limited_rows.section)
          END,
          'readiness_state', CASE limited_rows.section
            WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
            WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
            WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
            ELSE UPPER(limited_rows.section)
          END,
          'row_key', limited_rows.row_key,
          'row_ordinal', limited_rows.row_ordinal,
          'candidate_id', limited_rows.candidate_id::text,
          'timesheet_id', CASE WHEN limited_rows.timesheet_id IS NULL THEN NULL ELSE limited_rows.timesheet_id::text END,
          'key_type', limited_rows.key_type,
          'key_value', limited_rows.key_value,
          'selected', CASE
            WHEN limited_rows.section = 'canonical_preview_lines'
              THEN COALESCE(limited_rows.selected, false)
            ELSE false
          END,
          'selection_state', CASE
            WHEN limited_rows.section = 'canonical_preview_lines'
              THEN limited_rows.selection_state
            ELSE 'NOT_SELECTABLE'
          END,
          'status', limited_rows.status,
          'session_version', limited_rows.session_version,
          'amount_ex_vat', COALESCE(
            stored_values.stored_row_json->'amount_ex_vat',
            stored_values.stored_row_json->'preview_amount_ex_vat',
            stored_values.stored_row_json->'section_amount_ex_vat',
            'null'::jsonb
          )
        ) AS base_json
    ) AS base_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          NULLIF(base_values.base_json->>'amount_display', ''),
          NULLIF(base_values.base_json->>'section_amount_display', ''),
          NULLIF(base_values.base_json->>'amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'preview_amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'section_amount_ex_vat', '')
        ) AS amount_display_raw,
        COALESCE(
          NULLIF(base_values.base_json->>'section_amount_display', ''),
          NULLIF(base_values.base_json->>'amount_display', ''),
          NULLIF(base_values.base_json->>'section_amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'preview_amount_ex_vat', '')
        ) AS section_amount_display_raw
    ) AS display_raw_values
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN display_raw_values.amount_display_raw ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN to_char(display_raw_values.amount_display_raw::numeric, 'FM999999999999999990.00')
          ELSE display_raw_values.amount_display_raw
        END AS amount_display_text,
        CASE
          WHEN display_raw_values.section_amount_display_raw ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN to_char(display_raw_values.section_amount_display_raw::numeric, 'FM999999999999999990.00')
          ELSE display_raw_values.section_amount_display_raw
        END AS section_amount_display_text
    ) AS display_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_unavailable', '')))
            IN ('true', 't', '1', 'yes', 'y', 'on'),
          false
        )
        OR (
          COALESCE(
            lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_applied', '')))
              IN ('true', 't', '1', 'yes', 'y', 'on'),
            false
          )
          AND UPPER(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_operation_type', '')))
            IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_active', 'true')))
            NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        ) AS post_draft_unavailable_bool
    ) AS post_draft_values
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'selection_allowed'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'selection_allowed'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               THEN true
          ELSE false
        END AS selection_allowed_bool,
        CASE
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_excluded_from_allocation'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_excluded_from_allocation'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE false
        END AS excluded_bool,
        CASE
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_recognised_finance_deduction'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_recognised_finance_deduction'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE false
        END AS deduction_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'materialisable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'materialisable'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE limited_rows.status = 'READY'
        END AS materialisable_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               AND limited_rows.status = 'READY'
               THEN true
          ELSE false
        END AS draftable_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_ready_for_draft'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_ready_for_draft'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               AND limited_rows.status = 'READY'
               THEN true
          ELSE false
        END AS ready_for_draft_bool
    ) AS bool_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          NULLIF(BTRIM(base_values.base_json->>'candidate_display_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'candidate_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_display_name'), '')
        ) AS candidate_display_name_text,
        COALESCE(
          NULLIF(BTRIM(base_values.base_json->>'candidate_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'candidate_display_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_display_name'), '')
        ) AS candidate_name_text
    ) AS name_values
  )
  SELECT
    COALESCE(jsonb_agg(
      normalised_rows.base_json
      || jsonb_build_object(
        'amount_display', normalised_rows.amount_display_text,
        'section_amount_display', normalised_rows.section_amount_display_text
      )
      || jsonb_strip_nulls(jsonb_build_object(
        'candidate_display_name', normalised_rows.candidate_display_name_text,
        'candidate_name', normalised_rows.candidate_name_text
      ))
      || jsonb_build_object(
        'selection_allowed', normalised_rows.selection_allowed_bool,
        'draftable', normalised_rows.draftable_bool,
        'is_ready_for_draft', normalised_rows.ready_for_draft_bool,
        'is_excluded_from_allocation', normalised_rows.excluded_bool,
        'is_recognised_finance_deduction', normalised_rows.deduction_bool
      )
      || CASE
        WHEN normalised_rows.post_draft_unavailable_bool IS TRUE THEN jsonb_build_object(
          'post_draft_unavailable', true,
          'presentation_section', 'DRAFTED',
          'readiness_state', 'NOT_SELECTABLE',
          'preview_contract',
            CASE
              WHEN jsonb_typeof(normalised_rows.base_json->'preview_contract') = 'object'
                THEN normalised_rows.base_json->'preview_contract'
              ELSE '{}'::jsonb
            END
            || jsonb_build_object(
              'ok', false,
              'status', 'NOT_SELECTABLE',
              'materialisable', false,
              'selection_allowed', false,
              'draftable', false,
              'is_ready_for_draft', false
            )
        )
        WHEN jsonb_typeof(normalised_rows.base_json->'preview_contract') = 'object' THEN '{}'::jsonb
        ELSE jsonb_build_object(
          'preview_contract',
          jsonb_build_object(
            'ok', true,
            'status', 'OK',
            'reasons', '[]'::jsonb,
            'reason_count', 0,
            'key_type', COALESCE(NULLIF(normalised_rows.base_json->>'key_type', ''), normalised_rows.key_type),
            'key_value', COALESCE(NULLIF(normalised_rows.base_json->>'key_value', ''), normalised_rows.key_value),
            'line_key', COALESCE(NULLIF(normalised_rows.base_json->>'line_key', ''), NULLIF(normalised_rows.base_json->>'row_key', ''), normalised_rows.row_key),
            'case_type', COALESCE(NULLIF(normalised_rows.base_json->>'case_type', ''), 'TIMESHEET_PAYMENT'),
            'line_type', NULLIF(normalised_rows.base_json->>'line_type', ''),
            'source_kind', NULLIF(normalised_rows.base_json->>'source_kind', ''),
            'amount_ex_vat', COALESCE(
              normalised_rows.base_json->'amount_ex_vat',
              normalised_rows.base_json->'preview_amount_ex_vat',
              normalised_rows.base_json->'section_amount_ex_vat',
              'null'::jsonb
            ),
            'materialisable', normalised_rows.materialisable_bool,
            'target_section', COALESCE(NULLIF(normalised_rows.base_json->>'target_section', ''), NULLIF(normalised_rows.base_json->>'section', ''), normalised_rows.section),
            'presentation_section', NULLIF(normalised_rows.base_json->>'presentation_section', ''),
            'selection_allowed', normalised_rows.selection_allowed_bool,
            'draftable', normalised_rows.draftable_bool,
            'is_ready_for_draft', normalised_rows.ready_for_draft_bool,
            'is_excluded_from_allocation', normalised_rows.excluded_bool,
            'is_recognised_finance_deduction', normalised_rows.deduction_bool
          )
        )
      END
      ORDER BY normalised_rows.row_ordinal, normalised_rows.id
    ), '[]'::jsonb),
    COUNT(normalised_rows.id)::integer,
    (SELECT COUNT(*)::integer FROM page_rows),
    CASE
      WHEN (SELECT COUNT(*) FROM page_rows) > v_limit THEN (
        SELECT jsonb_build_object(
          'section', v_resolved_section,
          'last_row_ordinal', cursor_row.row_ordinal,
          'last_id', cursor_row.id::text
        )
        FROM limited_rows AS cursor_row
        ORDER BY cursor_row.row_ordinal DESC, cursor_row.id DESC
        LIMIT 1
      )
      ELSE NULL::jsonb
    END
  INTO v_items,
       v_returned_count,
       v_raw_count,
       v_next_cursor
  FROM normalised_rows;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'requested_section', v_requested_section,
    'resolved_section', v_resolved_section,
    'section_alias_applied', v_section_alias_applied,
    'section', v_resolved_section,
    'items', COALESCE(v_items, '[]'::jsonb),
    'rows', COALESCE(v_items, '[]'::jsonb),
    'known_count', COALESCE(v_known_count, 0),
    'total_count_estimate', COALESCE(v_known_count, 0),
    'returned_count', COALESCE(v_returned_count, 0),
    'next_cursor', v_next_cursor,
    'has_more', v_next_cursor IS NOT NULL
  )
  || jsonb_build_object(
    'limit', v_limit,
    'session_version', v_session_row.version,
    'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
    'selected_row_count', CASE
      WHEN v_resolved_section = 'canonical_preview_lines'
        THEN COALESCE(v_selected_eligible_count, 0)
      ELSE COALESCE(v_session_row.selected_row_count, 0)
    END,
    'session_signature', v_session_row.session_signature,
    'ready', true,
    'paging_mode', 'preview_rows_keyset_section_row_ordinal_id'
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_session_get_preview_page(uuid, text, jsonb, integer)
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_preview_page(uuid, text, jsonb, integer)
TO service_role;
