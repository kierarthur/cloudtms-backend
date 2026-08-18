-- Candidate preview paging must expose the pre-Draft semantic/effective
-- section while retaining the immutable physical section for parity proof.
-- No economic value, source row, selection identity or post-Draft authority
-- is changed by this read function.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_preview(
  p_session_id uuid,
  p_candidate_id uuid,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 100
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_cursor jsonb := CASE
    WHEN pg_catalog.jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_cursor_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_last_section text;
  v_last_row_ordinal bigint;
  v_last_id uuid;
  v_rows jsonb := '[]'::jsonb;
  v_returned_count integer := 0;
  v_raw_count integer := 0;
  v_next_cursor jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_candidate_state text := 'PENDING';
  v_summary_status text;
  v_effective_scope_status text;
  v_refreshing boolean := false;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CANDIDATE_PREVIEW_SCOPE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_PAGE');

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CANDIDATE_PREVIEW_SESSION_MISSING'
      USING ERRCODE = 'P0001';
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CANDIDATE_PREVIEW_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CANDIDATE_PREVIEW_CANDIDATE_NOT_IN_SCOPE'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT state_row.*
  INTO v_state
  FROM public.banking_pay_workbench_session_candidate_state AS state_row
  WHERE state_row.session_id = p_session_id
    AND state_row.candidate_id = p_candidate_id
    AND state_row.session_version = v_session.version
  ORDER BY state_row.updated_at_utc DESC, state_row.id DESC
  LIMIT 1;

  v_candidate_state := COALESCE(v_state.status, v_scope.status, 'PENDING');
  v_summary_status := pg_catalog.upper(pg_catalog.btrim(COALESCE(
    v_state.effective_summary_fragment_json->>'status', ''
  )));
  IF v_summary_status = 'READY_EMPTY' THEN
    v_candidate_state := 'READY_EMPTY';
  END IF;
  v_effective_scope_status := CASE
    WHEN v_summary_status = 'READY_EMPTY'
     AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status, ''))) = 'READY'
      THEN 'READY_EMPTY'
    ELSE COALESCE(v_scope.status, 'PENDING')
  END;
  v_refreshing := pg_catalog.upper(pg_catalog.btrim(COALESCE(v_effective_scope_status, '')))
    IN ('DELTA_REFRESH_PENDING', 'SOURCE_BUILD_PENDING', 'LINE_WORK_PENDING', 'PENDING');
  v_summary := COALESCE(v_state.effective_summary_fragment_json, '{}'::jsonb)
    || pg_catalog.jsonb_build_object(
      'candidate_count', 1,
      'scope_status', v_effective_scope_status,
      'candidate_state', v_candidate_state,
      'refreshing', v_refreshing,
      'requires_paging', true,
      'section_contract', 'EFFECTIVE_SECTION_WITH_PHYSICAL_DIAGNOSTIC_V1'
    );

  v_last_section := CASE
    WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(COALESCE(
      v_cursor->>'section', v_cursor->>'last_section', ''
    )), '')) IN ('', 'ready_to_pay', 'canonical_preview_lines')
      THEN 'canonical_preview_lines'
    WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(COALESCE(
      v_cursor->>'section', v_cursor->>'last_section', ''
    )), '')) IN ('cases_resolutions', 'case_resolution_states', 'case_resolutions', 'cases', 'resolutions')
      THEN 'cases_resolutions'
    WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(COALESCE(
      v_cursor->>'section', v_cursor->>'last_section', ''
    )), '')) IN ('blocked_for_pay', 'blocked_items', 'blocked_preview_lines', 'blocked_now', 'do_not_pay_items')
      THEN 'blocked_for_pay'
    ELSE NULLIF(pg_catalog.btrim(COALESCE(
      v_cursor->>'section', v_cursor->>'last_section', ''
    )), '')
  END;
  IF COALESCE(v_cursor->>'last_row_ordinal', '') ~ '^[0-9]+$' THEN
    v_last_row_ordinal := (v_cursor->>'last_row_ordinal')::bigint;
  END IF;
  IF COALESCE(v_cursor->>'last_id', '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_last_id := (v_cursor->>'last_id')::uuid;
  END IF;

  WITH eligible_rows AS (
    SELECT
      preview_row.id,
      private.pay_workbench_preview_effective_section_v1(
        preview_row.section, preview_row.row_json
      ) AS effective_section,
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
    WHERE preview_row.session_id = p_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.session_version = v_session.version
      AND preview_row.status = 'READY'
      AND NOT (
        COALESCE(pg_catalog.lower(pg_catalog.btrim(COALESCE(
          preview_row.row_json->>'post_draft_unavailable', ''
        ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
        OR (
          COALESCE(pg_catalog.lower(pg_catalog.btrim(COALESCE(
            preview_row.row_json->>'post_draft_overlay_applied', ''
          ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
          AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
            preview_row.row_json->>'post_draft_overlay_operation_type', ''
          ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
            preview_row.row_json->>'post_draft_overlay_active', 'true'
          ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        )
      )
      AND NOT (
        pg_catalog.upper(pg_catalog.btrim(COALESCE(
          preview_row.row_json->>'line_type',
          preview_row.row_json#>>'{preview_contract,line_type}',
          ''
        ))) = 'OVERPAYMENT_RECOVERY'
        AND private.pay_workbench_preview_effective_section_v1(
          preview_row.section, preview_row.row_json
        ) <> 'cases_resolutions'
        AND NULLIF(pg_catalog.btrim(COALESCE(
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
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
              actionable_sibling.row_json->>'line_type',
              actionable_sibling.row_json#>>'{preview_contract,line_type}',
              ''
            ))) = 'OVERPAYMENT_RECOVERY'
            AND NULLIF(pg_catalog.btrim(COALESCE(
              actionable_sibling.row_json->>'finance_case_id', ''
            )), '') = NULLIF(pg_catalog.btrim(COALESCE(
              preview_row.row_json->>'finance_case_id', ''
            )), '')
            AND private.pay_workbench_preview_effective_section_v1(
              actionable_sibling.section, actionable_sibling.row_json
          ) = 'cases_resolutions'
        )
      )
      -- A negative ordinary parent is audit/presentation evidence for the same
      -- timesheet debt represented by its OVERPAYMENT_RECOVERY sibling.  The
      -- recovery owns the single user-visible section (Cases, Ready or
      -- Blocked); exposing the parent as well duplicates one economic matter.
      AND NOT (
        pg_catalog.upper(pg_catalog.btrim(COALESCE(
          preview_row.row_json->>'line_type',
          preview_row.row_json#>>'{preview_contract,line_type}',
          ''
        ))) = 'TIMESHEET_PAYMENT'
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
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
            AND recovery_sibling.status = 'READY'
            AND recovery_sibling.timesheet_id = preview_row.timesheet_id
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
              recovery_sibling.row_json->>'line_type',
              recovery_sibling.row_json#>>'{preview_contract,line_type}',
              ''
            ))) = 'OVERPAYMENT_RECOVERY'
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS active_item
        JOIN public.pay_batch_candidates AS active_candidate
          ON active_candidate.id = active_item.pay_batch_candidate_id
        JOIN public.pay_batches AS active_batch
          ON active_batch.id = active_candidate.pay_batch_id
        WHERE active_candidate.candidate_id = preview_row.candidate_id
          AND COALESCE(active_item.is_voided, false) = false
          AND active_batch.cancelled_at_utc IS NULL
          AND pg_catalog.upper(pg_catalog.btrim(COALESCE(active_batch.status, ''))) IN (
            'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
            'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
            'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
          )
          AND (
            (
              COALESCE(
                active_item.timesheet_id,
                CASE
                  WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_source_basis_json->>'timesheet_id', '')), '')
                     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN pg_catalog.btrim(active_item.frozen_source_basis_json->>'timesheet_id')::uuid
                  ELSE NULL::uuid
                END,
                CASE
                  WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_source_basis_json->>'linked_timesheet_id', '')), '')
                     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN pg_catalog.btrim(active_item.frozen_source_basis_json->>'linked_timesheet_id')::uuid
                  ELSE NULL::uuid
                END,
                CASE
                  WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                   AND NULLIF(pg_catalog.btrim(COALESCE(
                     active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                     ''
                   )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN pg_catalog.btrim(
                      active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
                    )::uuid
                  ELSE NULL::uuid
                END
              ) = preview_row.timesheet_id
              AND (
                (
                  NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_component_key_type, '')), '') IS NOT NULL
                  AND NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_component_key_value, '')), '') IS NOT NULL
                  AND pg_catalog.upper(pg_catalog.btrim(active_item.frozen_component_key_type))
                    = pg_catalog.upper(pg_catalog.btrim(COALESCE(
                      preview_row.key_type, preview_row.row_json->>'key_type', ''
                    )))
                  AND pg_catalog.btrim(active_item.frozen_component_key_value)
                    = pg_catalog.btrim(COALESCE(
                      preview_row.key_value, preview_row.row_json->>'key_value', ''
                    ))
                )
                OR (
                  NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_component_key_type, '')), '') IS NULL
                  AND NULLIF(pg_catalog.btrim(COALESCE(active_item.frozen_component_key_value, '')), '') IS NULL
                )
              )
            )
            OR active_item.finance_component_id::text
              = NULLIF(pg_catalog.btrim(COALESCE(preview_row.row_json->>'finance_component_id', '')), '')
            OR active_item.finance_case_id::text
              = NULLIF(pg_catalog.btrim(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')
            OR (
              NULLIF(pg_catalog.btrim(COALESCE(
                preview_row.row_json->>'canonical_correction_key', ''
              )), '') IS NOT NULL
              AND NULLIF(pg_catalog.btrim(COALESCE(
                active_item.frozen_component_snapshot_json->>'canonical_correction_key',
                active_item.frozen_resolution_payload_json->>'canonical_correction_key', ''
              )), '') = NULLIF(pg_catalog.btrim(COALESCE(
                preview_row.row_json->>'canonical_correction_key', ''
              )), '')
            )
          )
      )
  ), page_rows AS (
    SELECT eligible_rows.*,
      pg_catalog.row_number() OVER (
        ORDER BY eligible_rows.effective_section, eligible_rows.row_ordinal, eligible_rows.id
      ) AS page_ordinal
    FROM eligible_rows
    WHERE v_last_section IS NULL
       OR eligible_rows.effective_section > v_last_section
       OR (
         eligible_rows.effective_section = v_last_section
         AND v_last_row_ordinal IS NOT NULL
         AND eligible_rows.row_ordinal > v_last_row_ordinal
       )
       OR (
         eligible_rows.effective_section = v_last_section
         AND v_last_row_ordinal IS NOT NULL
         AND eligible_rows.row_ordinal = v_last_row_ordinal
         AND v_last_id IS NOT NULL
         AND eligible_rows.id > v_last_id
       )
    ORDER BY eligible_rows.effective_section, eligible_rows.row_ordinal, eligible_rows.id
    LIMIT (v_limit + 1)
  ), limited_rows AS (
    SELECT page_rows.* FROM page_rows
    WHERE page_rows.page_ordinal <= v_limit
  )
  SELECT
    COALESCE(pg_catalog.jsonb_agg(
      limited_rows.row_json || pg_catalog.jsonb_build_object(
        'preview_row_id', limited_rows.id::text,
        'row_id', limited_rows.id::text,
        'section', limited_rows.effective_section,
        'effective_section', limited_rows.effective_section,
        'physical_section', limited_rows.physical_section,
        'presentation_section', CASE limited_rows.effective_section
          WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
          WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
          WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
          ELSE pg_catalog.upper(limited_rows.effective_section)
        END,
        'readiness_state', CASE limited_rows.effective_section
          WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
          WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
          WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
          ELSE pg_catalog.upper(limited_rows.effective_section)
        END,
        'row_key', limited_rows.row_key,
        'row_ordinal', limited_rows.row_ordinal,
        'selected', CASE
          WHEN limited_rows.effective_section = 'canonical_preview_lines'
            THEN COALESCE(limited_rows.selected, false)
          ELSE false
        END,
        'selection_state', CASE
          WHEN limited_rows.effective_section = 'canonical_preview_lines'
            THEN limited_rows.selection_state
          ELSE 'NOT_SELECTABLE'
        END,
        'selection_allowed', CASE
          WHEN limited_rows.effective_section = 'canonical_preview_lines'
            THEN CASE
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'selection_allowed'
              ), '')) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'selection_allowed'
              ), '')) IN ('false', 'f', '0', 'no', 'n', 'off') THEN false
              ELSE COALESCE(limited_rows.selected, false)
            END
          ELSE false
        END,
        'draftable', CASE
          WHEN limited_rows.effective_section = 'canonical_preview_lines'
            THEN CASE
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'draftable'
              ), '')) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'draftable'
              ), '')) IN ('false', 'f', '0', 'no', 'n', 'off') THEN false
              ELSE COALESCE(limited_rows.selected, false)
            END
          ELSE false
        END,
        'is_ready_for_draft', CASE
          WHEN limited_rows.effective_section = 'canonical_preview_lines'
            THEN CASE
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'is_ready_for_draft'
              ), '')) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
              WHEN pg_catalog.lower(NULLIF(pg_catalog.btrim(
                limited_rows.row_json->>'is_ready_for_draft'
              ), '')) IN ('false', 'f', '0', 'no', 'n', 'off') THEN false
              ELSE COALESCE(limited_rows.selected, false)
            END
          ELSE false
        END,
        'status', limited_rows.status,
        'session_version', limited_rows.session_version,
        'timesheet_id', CASE WHEN limited_rows.timesheet_id IS NULL
          THEN NULL ELSE limited_rows.timesheet_id::text END,
        'key_type', limited_rows.key_type,
        'key_value', limited_rows.key_value
      )
      ORDER BY limited_rows.effective_section, limited_rows.row_ordinal, limited_rows.id
    ), '[]'::jsonb),
    pg_catalog.count(limited_rows.id)::integer,
    (SELECT pg_catalog.count(*)::integer FROM page_rows),
    CASE WHEN (SELECT pg_catalog.count(*) FROM page_rows) > v_limit THEN (
      SELECT pg_catalog.jsonb_build_object(
        'section', cursor_row.effective_section,
        'last_section', cursor_row.effective_section,
        'last_row_ordinal', cursor_row.row_ordinal,
        'last_id', cursor_row.id::text
      )
      FROM limited_rows AS cursor_row
      ORDER BY cursor_row.effective_section DESC, cursor_row.row_ordinal DESC, cursor_row.id DESC
      LIMIT 1
    ) ELSE NULL::jsonb END
  INTO v_rows, v_returned_count, v_raw_count, v_next_cursor
  FROM limited_rows;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'session_version', v_session.version,
    'candidate_state', v_candidate_state,
    'scope_status', v_effective_scope_status,
    'refreshing', v_refreshing,
    'pending_refresh', v_refreshing,
    'summary', v_summary,
    'line_work_progress', pg_catalog.jsonb_build_object(
      'line_units_total', COALESCE(v_summary->>'line_units_total', '0'),
      'line_units_pending', COALESCE(v_summary->>'pending_line_work_count', '0'),
      'line_units_ready', COALESCE(v_summary->>'ready_line_work_count', '0'),
      'line_units_complete', COALESCE(v_summary->>'materialised_line_work_count', '0'),
      'line_units_error', COALESCE(v_summary->>'error_line_work_count', '0')
    ),
    'preview_rows', COALESCE(v_rows, '[]'::jsonb),
    'returned_count', COALESCE(v_returned_count, 0),
    'limit', v_limit,
    'next_cursor', v_next_cursor,
    'has_more', v_next_cursor IS NOT NULL,
    'cursor_scheme', 'effective_section_row_ordinal_id',
    'candidate_fragment_json', pg_catalog.jsonb_strip_nulls(
      COALESCE(v_state.effective_candidate_fragment_json, '{}'::jsonb)
      || pg_catalog.jsonb_build_object(
        'scope_status', v_effective_scope_status,
        'candidate_state', v_candidate_state,
        'summary_status', COALESCE(NULLIF(v_summary_status, ''), v_candidate_state),
        'refreshing', v_refreshing
      )
    )
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_get_candidate_preview(uuid,uuid,jsonb,integer)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_candidate_preview(uuid,uuid,jsonb,integer)
  FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_candidate_preview(uuid,uuid,jsonb,integer)
  TO service_role;
