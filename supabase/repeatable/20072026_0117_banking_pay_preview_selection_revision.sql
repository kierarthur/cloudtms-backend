-- Banking Pay preview-page concurrency contract.
-- Exposes the authoritative shared-session revision used by the frontend
-- immediately before draft creation. This changes no economic or frozen-batch logic.

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

  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE preview_count_row.selected IS TRUE)::integer
  INTO v_known_count, v_selected_eligible_count
  FROM public.banking_pay_workbench_preview_rows AS preview_count_row
  WHERE preview_count_row.session_id = p_session_id
    AND preview_count_row.session_version = v_session_row.version
    AND LOWER(BTRIM(COALESCE(
      CASE
        WHEN preview_count_row.row_json#>>'{selection_recovery_headroom_v1,contract_version}' = '1'
          THEN preview_count_row.row_json#>>'{selection_recovery_headroom_v1,effective_section}'
        ELSE NULL::text
      END,
      preview_count_row.section,
      ''
    ))) = v_resolved_section
    AND preview_count_row.status = 'READY'
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
              active_batch_item.timesheet_id IS NOT NULL
              AND active_batch_item.timesheet_id = preview_count_row.timesheet_id
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

  WITH page_rows AS (
    SELECT page_source.*,
           ROW_NUMBER() OVER (ORDER BY page_source.row_ordinal, page_source.id) AS page_ordinal
    FROM (
      SELECT
        preview_row.id,
        LOWER(BTRIM(COALESCE(
          CASE
            WHEN preview_row.row_json#>>'{selection_recovery_headroom_v1,contract_version}' = '1'
              THEN preview_row.row_json#>>'{selection_recovery_headroom_v1,effective_section}'
            ELSE NULL::text
          END,
          preview_row.section,
          ''
        ))) AS section,
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
      WHERE preview_row.session_id = p_session_id
        AND preview_row.session_version = v_session_row.version
        AND LOWER(BTRIM(COALESCE(
          CASE
            WHEN preview_row.row_json#>>'{selection_recovery_headroom_v1,contract_version}' = '1'
              THEN preview_row.row_json#>>'{selection_recovery_headroom_v1,effective_section}'
            ELSE NULL::text
          END,
          preview_row.section,
          ''
        ))) = v_resolved_section
        AND preview_row.status = 'READY'
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
                  active_batch_item.timesheet_id IS NOT NULL
                  AND active_batch_item.timesheet_id = preview_row.timesheet_id
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
          'row_key', limited_rows.row_key,
          'row_ordinal', limited_rows.row_ordinal,
          'candidate_id', limited_rows.candidate_id::text,
          'timesheet_id', CASE WHEN limited_rows.timesheet_id IS NULL THEN NULL ELSE limited_rows.timesheet_id::text END,
          'key_type', limited_rows.key_type,
          'key_value', limited_rows.key_value,
          'selected', COALESCE(limited_rows.selected, false),
          'selection_state', limited_rows.selection_state,
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
