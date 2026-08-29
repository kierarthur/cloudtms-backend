CREATE OR REPLACE FUNCTION private.pay_workbench_preview_effective_section_v1(
  p_physical_section text,
  p_row_json jsonb
) RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
  SELECT CASE
    WHEN COALESCE(
      pg_catalog.lower(pg_catalog.btrim(COALESCE(
        p_row_json->>'case_needs_resolution',
        p_row_json#>>'{case_resolution_summary,case_needs_resolution}',
        p_row_json#>>'{case_resolution_summary_json,case_needs_resolution}',
        ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      false
    )
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
      p_row_json->>'resolution_family',
      p_row_json#>>'{case_resolution_summary,resolution_family}',
      p_row_json#>>'{case_resolution_summary_json,resolution_family}',
      ''
    ))) = 'TAXABLE_CHANNEL_RESTRUCTURE'
    AND COALESCE(
      pg_catalog.lower(pg_catalog.btrim(COALESCE(
        p_row_json#>>'{taxable_channel_restructure,can_apply}',
        p_row_json#>>'{case_resolution_summary,taxable_channel_restructure,can_apply}',
        p_row_json#>>'{case_resolution_summary_json,taxable_channel_restructure,can_apply}',
        ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      false
    )
      THEN 'cases_resolutions'
    WHEN COALESCE(p_row_json#>>'{selection_recovery_headroom_v1,contract_version}', '') IN (
      '1', 'PAY_WORKBENCH_SELECTION_RECOVERY_HEADROOM_V1'
    )
      THEN COALESCE(
        NULLIF(pg_catalog.btrim(p_row_json#>>'{selection_recovery_headroom_v1,effective_section}'), ''),
        NULLIF(pg_catalog.btrim(p_physical_section), ''),
        'canonical_preview_lines'
      )
    ELSE COALESCE(NULLIF(pg_catalog.btrim(p_physical_section), ''), 'canonical_preview_lines')
  END;
$function$;

ALTER FUNCTION private.pay_workbench_preview_effective_section_v1(text, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_preview_effective_section_v1(text, jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_preview_effective_section_v1(text, jsonb) TO postgres;

-- identity_args: p_session_id uuid, p_selected_preview_row_ids jsonb, p_actor_user_id uuid
CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_selected_rows(p_session_id uuid, p_selected_preview_row_ids jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_input jsonb := COALESCE(p_selected_preview_row_ids, '[]'::jsonb);
  v_input_type text := COALESCE(jsonb_typeof(COALESCE(p_selected_preview_row_ids, '[]'::jsonb)), 'null');
  v_select_ids_source jsonb := '[]'::jsonb;
  v_deselect_ids_source jsonb := '[]'::jsonb;
  v_selected_ids jsonb := '[]'::jsonb;
  v_deselected_ids jsonb := '[]'::jsonb;
  v_missing_ids jsonb := '[]'::jsonb;
  v_stale_selection_ids jsonb := '[]'::jsonb;
  v_non_draftable_ids jsonb := '[]'::jsonb;
  v_conflicting_ids jsonb := '[]'::jsonb;
  v_server_selected_ids jsonb := '[]'::jsonb;
  v_current_selected_count integer := 0;
  v_selected_delta integer := 0;
  v_deselected_delta integer := 0;
  v_updated_count integer := 0;
  v_rejected_non_draftable_count integer := 0;
  v_forced_synthetic_cleanup_count integer := 0;
  v_replace_omitted_count integer := 0;
  v_replace_mode boolean := false;
  v_selection_mode text := 'ROW_PATCH_CAPPED_CONTRACT_GUARDED';
  v_global_selection_action text := '';
  v_requested_section text := 'canonical_preview_lines';
  v_resolved_selection_section text := 'canonical_preview_lines';
  v_audit_after_json jsonb := '{}'::jsonb;
  v_existing_selection_intent_mode text := '';
  v_next_selection_intent_mode text := '';
  v_next_server_selected_ids_provided boolean := false;
  v_expected_session_version bigint := NULL::bigint;
  v_expected_progress_counter_version bigint := NULL::bigint;
  v_expected_session_version_text text := '';
  v_expected_progress_counter_version_text text := '';
  v_next_progress_counter_version bigint := NULL::bigint;
  v_session_ready boolean := false;
  v_draft_blocker_codes jsonb := '[]'::jsonb;
  v_revalidation_candidate_id uuid := NULL::uuid;
  v_revalidation_result jsonb := '{}'::jsonb;
  v_selected_economic_identities jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  PERFORM 1
  FROM public.tms_users AS user_row
  WHERE user_row.id = p_actor_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not OPEN', p_session_id;
  END IF;

  IF v_input_type <> 'object' THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_CONTEXT_REQUIRED',
              'session_id', p_session_id::text,
              'message', 'The Banking Pay selection update must include the current workbench session and progress context.'
            )::text;
  END IF;

  IF v_input_type = 'object' THEN
    v_expected_session_version_text := NULLIF(BTRIM(COALESCE(
      v_input->>'expected_session_version',
      v_input->>'expectedSessionVersion',
      v_input->>'session_version',
      v_input->>'sessionVersion',
      ''
    )), '');
    v_expected_progress_counter_version_text := NULLIF(BTRIM(COALESCE(
      v_input->>'expected_progress_counter_version',
      v_input->>'expectedProgressCounterVersion',
      v_input->>'progress_counter_version',
      v_input->>'progressCounterVersion',
      ''
    )), '');

    IF v_expected_session_version_text IS NOT NULL THEN
      IF v_expected_session_version_text !~ '^[0-9]{1,18}$' THEN
        RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_SESSION_VERSION_CONTEXT_INVALID',
                  'session_id', p_session_id::text,
                  'provided_session_version', v_expected_session_version_text
                )::text;
      END IF;
      v_expected_session_version := v_expected_session_version_text::bigint;
    END IF;

    IF v_expected_progress_counter_version_text IS NOT NULL THEN
      IF v_expected_progress_counter_version_text !~ '^[0-9]{1,18}$' THEN
        RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CONTEXT_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_SESSION_PROGRESS_CONTEXT_INVALID',
                  'session_id', p_session_id::text,
                  'provided_progress_counter_version', v_expected_progress_counter_version_text
                )::text;
      END IF;
      v_expected_progress_counter_version := v_expected_progress_counter_version_text::bigint;
    END IF;
  END IF;

  IF v_expected_session_version IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_VERSION_CONTEXT_REQUIRED',
              'session_id', p_session_id::text,
              'current_session_version', COALESCE(v_session_row.version, 0),
              'message', 'The Banking Pay selection update was missing the current session version. Refresh the preview before changing the selection.'
            )::text;
  END IF;

  IF v_expected_progress_counter_version IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_PROGRESS_CONTEXT_REQUIRED',
              'session_id', p_session_id::text,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
              'message', 'The Banking Pay selection update was missing the current progress version. Refresh the preview before changing the selection.'
            )::text;
  END IF;

  IF COALESCE(v_session_row.version, 0) IS DISTINCT FROM v_expected_session_version THEN
    RAISE EXCEPTION 'STALE_SESSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'STALE_SESSION',
              'session_id', p_session_id::text,
              'expected_session_version', v_expected_session_version,
              'current_session_version', COALESCE(v_session_row.version, 0)
            )::text;
  END IF;

  IF COALESCE(v_session_row.progress_counter_version, 0) IS DISTINCT FROM v_expected_progress_counter_version THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SESSION_PROGRESS_CHANGED',
              'session_id', p_session_id::text,
              'expected_progress_counter_version', v_expected_progress_counter_version,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
              'message', 'The Banking Pay workbench changed in another window. Refresh the preview before changing the selection.'
            )::text;
  END IF;

  v_existing_selection_intent_mode := UPPER(BTRIM(COALESCE(
    v_session_row.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}',
    ''
  )));
  IF v_existing_selection_intent_mode NOT IN ('IMPLICIT_ALL', 'EXPLICIT_INCLUDE') THEN
    v_existing_selection_intent_mode := '';
  END IF;

  IF v_input_type = 'array' THEN
    v_replace_mode := true;
    v_select_ids_source := v_input;
    v_deselect_ids_source := '[]'::jsonb;
  ELSIF v_input_type = 'object' THEN
    v_select_ids_source := CASE
      WHEN jsonb_typeof(v_input->'select_preview_row_ids') = 'array' THEN COALESCE(v_input->'select_preview_row_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_input->'selected_preview_row_ids') = 'array' THEN COALESCE(v_input->'selected_preview_row_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_input->'select_row_ids') = 'array' THEN COALESCE(v_input->'select_row_ids', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;
    v_deselect_ids_source := CASE
      WHEN jsonb_typeof(v_input->'deselect_preview_row_ids') = 'array' THEN COALESCE(v_input->'deselect_preview_row_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_input->'deselected_preview_row_ids') = 'array' THEN COALESCE(v_input->'deselected_preview_row_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_input->'deselect_row_ids') = 'array' THEN COALESCE(v_input->'deselect_row_ids', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;
    v_replace_mode :=
      LOWER(BTRIM(COALESCE(
        v_input->>'replace_selected_preview_row_ids',
        v_input->>'replaceSelectedPreviewRowIds',
        v_input->>'replace_selected_rows',
        v_input->>'replaceSelectedRows',
        v_input->>'replace',
        'false'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR LOWER(BTRIM(COALESCE(
        v_input->>'selected_preview_row_ids_provided',
        v_input->>'selectedPreviewRowIdsProvided',
        v_input->>'server_selected_preview_row_ids_provided',
        v_input->>'serverSelectedPreviewRowIdsProvided',
        v_input->>'selected_rows_provided',
        v_input->>'selectedRowsProvided',
        'false'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(
        v_input->>'selection_mode',
        v_input->>'selectionMode',
        v_input->>'mode',
        ''
      ))) IN (
        'EXPLICIT_REPLACE',
        'AUTHORITATIVE_REPLACE',
        'REPLACE',
        'REPLACE_SELECTED_ROWS',
        'REPLACE_SELECTED_PREVIEW_ROW_IDS',
        'SET',
        'SET_SELECTED_ROWS',
        'SET_SELECTED_PREVIEW_ROW_IDS'
      );

    v_requested_section := LOWER(BTRIM(COALESCE(
      v_input->>'section',
      v_input->>'section_key',
      v_input->>'sectionKey',
      v_input->>'selection_section',
      v_input->>'selectionSection',
      'canonical_preview_lines'
    )));
    v_requested_section := REGEXP_REPLACE(v_requested_section, '[^a-z0-9_]+', '_', 'g');
    v_requested_section := BTRIM(v_requested_section, '_');
    v_resolved_selection_section := CASE
      WHEN v_requested_section IN ('', 'ready', 'ready_to_pay', 'ready_preview_lines', 'preview_rows', 'canonical_preview_lines') THEN 'canonical_preview_lines'
      WHEN v_requested_section IN ('cases', 'case_resolutions', 'case_resolution_states', 'cases_resolutions') THEN 'cases_resolutions'
      WHEN v_requested_section IN ('blocked', 'blocked_now', 'blocked_preview_lines', 'blocked_items', 'blocked_for_pay') THEN 'blocked_for_pay'
      ELSE v_requested_section
    END;

    v_global_selection_action := UPPER(BTRIM(COALESCE(
      v_input->>'selection_action',
      v_input->>'selectionAction',
      v_input->>'global_selection_action',
      v_input->>'globalSelectionAction',
      v_input->>'action',
      ''
    )));
    v_global_selection_action := REGEXP_REPLACE(v_global_selection_action, '[^A-Z0-9_]+', '_', 'g');
    v_global_selection_action := BTRIM(v_global_selection_action, '_');

    IF v_global_selection_action = '' THEN
      v_global_selection_action := CASE
        WHEN UPPER(BTRIM(COALESCE(v_input->>'selection_mode', v_input->>'selectionMode', v_input->>'mode', ''))) IN (
          'IMPLICIT_ALL', 'SELECT_ALL', 'SELECT_ALL_SECTION', 'SELECT_SECTION', 'SELECT_ALL_READY_TO_PAY'
        ) THEN 'SELECT_ALL_SECTION'
        WHEN UPPER(BTRIM(COALESCE(v_input->>'selection_mode', v_input->>'selectionMode', v_input->>'mode', ''))) IN (
          'EXPLICIT_NONE', 'CLEAR_ALL', 'CLEAR_SECTION', 'DESELECT_ALL', 'DESELECT_ALL_SECTION', 'CLEAR_SELECTED_ROWS'
        ) THEN 'CLEAR_SECTION'
        ELSE ''
      END;
    END IF;

    IF v_global_selection_action IN ('SELECT_ALL', 'SELECT_SECTION', 'SELECT_ALL_READY_TO_PAY') THEN
      v_global_selection_action := 'SELECT_ALL_SECTION';
    ELSIF v_global_selection_action IN ('CLEAR_ALL', 'DESELECT_ALL', 'DESELECT_ALL_SECTION', 'CLEAR_SELECTED_ROWS') THEN
      v_global_selection_action := 'CLEAR_SECTION';
    END IF;

    IF v_global_selection_action <> '' THEN
      v_replace_mode := false;
      v_select_ids_source := '[]'::jsonb;
      v_deselect_ids_source := '[]'::jsonb;
    END IF;
  ELSE
    RAISE EXCEPTION 'selected_preview_row_ids must be a JSON array or object';
  END IF;

  v_selection_mode := CASE
    WHEN v_replace_mode THEN 'REPLACE_CAPPED_CONTRACT_GUARDED'
    ELSE 'ROW_PATCH_CAPPED_CONTRACT_GUARDED'
  END;

  IF jsonb_array_length(v_select_ids_source) > 100 OR jsonb_array_length(v_deselect_ids_source) > 100 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_INPUT_TOO_LARGE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_INPUT_TOO_LARGE',
              'session_id', p_session_id::text,
              'select_count', jsonb_array_length(v_select_ids_source),
              'deselect_count', jsonb_array_length(v_deselect_ids_source),
              'max_per_call', 100
            )::text;
  END IF;


  IF v_global_selection_action <> '' THEN
    IF v_global_selection_action NOT IN ('SELECT_ALL_SECTION', 'CLEAR_SECTION') THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_UNSUPPORTED_GLOBAL_ACTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_UNSUPPORTED_GLOBAL_ACTION',
                'session_id', p_session_id::text,
                'selection_action', v_global_selection_action
              )::text;
    END IF;

    IF v_resolved_selection_section <> 'canonical_preview_lines' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_SECTION_NOT_SELECTABLE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_SECTION_NOT_SELECTABLE',
                'session_id', p_session_id::text,
                'requested_section', v_requested_section,
                'resolved_section', v_resolved_selection_section,
                'message', 'Only Ready to Pay canonical preview rows are draft-selectable.'
              )::text;
    END IF;

    v_selection_mode := CASE
      WHEN v_global_selection_action = 'SELECT_ALL_SECTION' THEN 'SECTION_SELECT_ALL_CONTRACT_GUARDED'
      ELSE 'SECTION_CLEAR_CONTRACT_GUARDED'
    END;
    v_next_selection_intent_mode := CASE
      WHEN v_global_selection_action IN ('SELECT_ALL_SECTION', 'CLEAR_SECTION') THEN 'IMPLICIT_ALL'
      ELSE 'EXPLICIT_INCLUDE'
    END;

    DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_global_selection_candidates;
    CREATE TEMP TABLE _tmp_pay_wb_global_selection_candidates ON COMMIT DROP AS
    SELECT DISTINCT preview_row.candidate_id
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
      AND preview_row.candidate_id IS NOT NULL;

    IF v_global_selection_action = 'SELECT_ALL_SECTION' THEN
      DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_global_selection_rows;
      CREATE TEMP TABLE _tmp_pay_wb_global_selection_rows ON COMMIT DROP AS
      SELECT preview_row.id,
             preview_row.row_ordinal,
             preview_row.row_key,
             COALESCE(preview_row.selected, false) AS was_selected,
             contract_check.contract_json,
             synthetic_check.is_synthetic_resolved_total,
             (
               LOWER(BTRIM(COALESCE(contract_check.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'materialisable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND COALESCE(contract_check.contract_json->>'target_section', '') = 'canonical_preview_lines'
               AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'presentation_section', ''))) = 'READY_TO_PAY'
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND COALESCE(contract_check.contract_json->>'key_type', '') <> ''
               AND COALESCE(contract_check.contract_json->>'key_value', '') <> ''
               AND (
                 UPPER(BTRIM(COALESCE(contract_check.contract_json->>'key_type', ''))) <> 'TS_DAY'
                 OR COALESCE(contract_check.contract_json->>'key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
               )
               AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'source_kind', ''))) NOT IN (
                 'TIMESHEET_SNAPSHOT',
                 'TIMESHEET_SNAPSHOT_EVIDENCE',
                 'RAW_TIMESHEET_SNAPSHOT',
                 'INTERNAL_ONLY',
                 'NO_DELTA',
                 'EXCLUDED'
               )
               AND preview_row.row_key NOT LIKE 'timesheet_snapshot:%'
               AND synthetic_check.is_synthetic_resolved_total IS NOT TRUE
             ) AS is_selectable
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      CROSS JOIN LATERAL (
        SELECT public.pay_workbench_preview_line_contract_ok(
          p_line_json => COALESCE(preview_row.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'line_key', preview_row.row_key,
              'row_key', preview_row.row_key,
              'section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
              'target_section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
              'key_type', preview_row.key_type,
              'key_value', preview_row.key_value
            ),
          p_economic_key_json => jsonb_build_object(
            'key_type', preview_row.key_type,
            'key_value', preview_row.key_value
          ),
          p_target_section => private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)
        ) AS contract_json
      ) AS contract_check
      CROSS JOIN LATERAL (
        SELECT (
          preview_row.timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(
            preview_row.key_type,
            preview_row.row_json#>>'{economic_key,key_type}',
            preview_row.row_json->>'component_key_type',
            preview_row.row_json->>'key_type',
            ''
          ))) = 'TS_TOTAL'
          AND UPPER(BTRIM(COALESCE(
            preview_row.key_value,
            preview_row.row_json#>>'{economic_key,key_value}',
            preview_row.row_json->>'component_key_value',
            preview_row.row_json->>'key_value',
            ''
          ))) = 'TOTAL'
          AND LOWER(BTRIM(COALESCE(
            preview_row.row_key,
            preview_row.row_json->>'row_key',
            preview_row.row_json->>'line_key',
            preview_row.row_json->>'source_ref',
            preview_row.row_json#>>'{source_basis,row_key}',
            preview_row.row_json#>>'{source_basis,line_key}',
            preview_row.row_json#>>'{source_basis,source_ref}',
            preview_row.row_json#>>'{source_basis_json,row_key}',
            preview_row.row_json#>>'{source_basis_json,line_key}',
            preview_row.row_json#>>'{source_basis_json,source_ref}',
            ''
          ))) LIKE '%:non_segment:total%'
          AND EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_preview_rows AS sibling_row
            WHERE sibling_row.session_id = preview_row.session_id
              AND sibling_row.session_version = preview_row.session_version
              AND sibling_row.id <> preview_row.id
              AND sibling_row.timesheet_id = preview_row.timesheet_id
              AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
              AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
              AND UPPER(BTRIM(COALESCE(
                sibling_row.key_type,
                sibling_row.row_json#>>'{economic_key,key_type}',
                sibling_row.row_json->>'component_key_type',
                sibling_row.row_json->>'key_type',
                ''
              ))) = 'TS_DAY'
              AND COALESCE(
                sibling_row.key_value,
                sibling_row.row_json#>>'{economic_key,key_value}',
                sibling_row.row_json->>'component_key_value',
                sibling_row.row_json->>'key_value',
                ''
              ) ~ '^\d{4}-\d{2}-\d{2}$'
          )
        ) AS is_synthetic_resolved_total
      ) AS synthetic_check
      WHERE preview_row.session_id = p_session_id
        AND preview_row.session_version = v_session_row.version
        AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY';

      WITH updated_rows AS (
        UPDATE public.banking_pay_workbench_preview_rows AS preview_row
        SET selected = true,
            selection_state = 'SELECTED',
            row_json = COALESCE(preview_row.row_json, '{}'::jsonb)
              || jsonb_build_object(
                'selected', true,
                'selection_state', 'SELECTED',
                'selection_user_override', 'SELECTED',
                'selection_origin', 'USER_GLOBAL_SELECT_ALL',
                'selection_user_override_at_utc', v_now::text
              ),
            updated_at_utc = v_now
        FROM pg_temp._tmp_pay_wb_global_selection_rows AS global_rows
        WHERE preview_row.id = global_rows.id
          AND global_rows.is_selectable IS TRUE
          AND (
            COALESCE(preview_row.selected, false) IS DISTINCT FROM true
            OR UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) <> 'SELECTED'
          )
        RETURNING preview_row.id,
                  global_rows.was_selected,
                  global_rows.row_ordinal
      )
      SELECT COALESCE(jsonb_agg(to_jsonb(updated_rows.id::text) ORDER BY updated_rows.row_ordinal, updated_rows.id), '[]'::jsonb),
             COUNT(*)::integer,
             COUNT(*)::integer
      INTO v_selected_ids, v_selected_delta, v_updated_count
      FROM updated_rows;

      WITH cleanup_rows AS (
        UPDATE public.banking_pay_workbench_preview_rows AS preview_row
        SET selected = false,
            selection_state = 'UNSELECTED',
            row_json = COALESCE(preview_row.row_json, '{}'::jsonb)
              || jsonb_build_object(
                'selected', false,
                'selection_state', 'UNSELECTED',
                'selection_user_override', 'UNSELECTED',
                'selection_origin', 'USER_GLOBAL_CLEAR',
                'selection_user_override_at_utc', v_now::text
              ),
            updated_at_utc = v_now
        FROM pg_temp._tmp_pay_wb_global_selection_rows AS global_rows
        WHERE preview_row.id = global_rows.id
          AND global_rows.is_synthetic_resolved_total IS TRUE
          AND COALESCE(preview_row.selected, false) = true
        RETURNING preview_row.id
      )
      SELECT COUNT(*)::integer
      INTO v_forced_synthetic_cleanup_count
      FROM cleanup_rows;

      v_deselected_delta := COALESCE(v_forced_synthetic_cleanup_count, 0);
      v_updated_count := COALESCE(v_updated_count, 0) + COALESCE(v_forced_synthetic_cleanup_count, 0);
    ELSE
      WITH updated_rows AS (
        UPDATE public.banking_pay_workbench_preview_rows AS preview_row
        SET selected = false,
            selection_state = 'UNSELECTED',
            row_json = COALESCE(preview_row.row_json, '{}'::jsonb)
              || jsonb_build_object(
                'selected', false,
                'selection_state', 'UNSELECTED'
              ),
            updated_at_utc = v_now
        WHERE preview_row.session_id = p_session_id
          AND preview_row.session_version = v_session_row.version
          AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
          AND COALESCE(preview_row.selected, false) = true
        RETURNING preview_row.id, preview_row.row_ordinal
      )
      SELECT COALESCE(jsonb_agg(to_jsonb(updated_rows.id::text) ORDER BY updated_rows.row_ordinal, updated_rows.id), '[]'::jsonb),
             COUNT(*)::integer,
             COUNT(*)::integer
      INTO v_deselected_ids, v_deselected_delta, v_updated_count
      FROM updated_rows;
    END IF;

    DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_current_selected_rows;
    CREATE TEMP TABLE _tmp_pay_wb_current_selected_rows ON COMMIT DROP AS
    SELECT selected_count_row.id,
           selected_count_row.candidate_id,
           selected_count_row.row_ordinal,
           selected_count_row.row_key,
           selected_count_row.row_json,
           selected_count_row.timesheet_id,
           selected_count_row.key_type,
           selected_count_row.key_value,
           selected_contract.contract_json,
           synthetic_check.is_synthetic_resolved_total
    FROM public.banking_pay_workbench_preview_rows AS selected_count_row
    CROSS JOIN LATERAL (
      SELECT public.pay_workbench_preview_line_contract_ok(
        p_line_json => COALESCE(selected_count_row.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'line_key', selected_count_row.row_key,
            'row_key', selected_count_row.row_key,
            'section', private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json),
            'target_section', private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json),
            'key_type', selected_count_row.key_type,
            'key_value', selected_count_row.key_value
          ),
        p_economic_key_json => jsonb_build_object(
          'key_type', selected_count_row.key_type,
          'key_value', selected_count_row.key_value
        ),
        p_target_section => private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json)
      ) AS contract_json
    ) AS selected_contract
    CROSS JOIN LATERAL (
      SELECT (
        selected_count_row.timesheet_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(
          selected_count_row.key_type,
          selected_count_row.row_json#>>'{economic_key,key_type}',
          selected_count_row.row_json->>'component_key_type',
          selected_count_row.row_json->>'key_type',
          ''
        ))) = 'TS_TOTAL'
        AND UPPER(BTRIM(COALESCE(
          selected_count_row.key_value,
          selected_count_row.row_json#>>'{economic_key,key_value}',
          selected_count_row.row_json->>'component_key_value',
          selected_count_row.row_json->>'key_value',
          ''
        ))) = 'TOTAL'
        AND LOWER(BTRIM(COALESCE(
          selected_count_row.row_key,
          selected_count_row.row_json->>'row_key',
          selected_count_row.row_json->>'line_key',
          selected_count_row.row_json->>'source_ref',
          selected_count_row.row_json#>>'{source_basis,row_key}',
          selected_count_row.row_json#>>'{source_basis,line_key}',
          selected_count_row.row_json#>>'{source_basis,source_ref}',
          selected_count_row.row_json#>>'{source_basis_json,row_key}',
          selected_count_row.row_json#>>'{source_basis_json,line_key}',
          selected_count_row.row_json#>>'{source_basis_json,source_ref}',
          ''
        ))) LIKE '%:non_segment:total%'
        AND EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_preview_rows AS sibling_row
          WHERE sibling_row.session_id = selected_count_row.session_id
            AND sibling_row.session_version = selected_count_row.session_version
            AND sibling_row.id <> selected_count_row.id
            AND sibling_row.timesheet_id = selected_count_row.timesheet_id
            AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
            AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
            AND UPPER(BTRIM(COALESCE(
              sibling_row.key_type,
              sibling_row.row_json#>>'{economic_key,key_type}',
              sibling_row.row_json->>'component_key_type',
              sibling_row.row_json->>'key_type',
              ''
            ))) = 'TS_DAY'
            AND COALESCE(
              sibling_row.key_value,
              sibling_row.row_json#>>'{economic_key,key_value}',
              sibling_row.row_json->>'component_key_value',
              sibling_row.row_json->>'key_value',
              ''
            ) ~ '^\d{4}-\d{2}-\d{2}$'
        )
      ) AS is_synthetic_resolved_total
    ) AS synthetic_check
    WHERE selected_count_row.session_id = p_session_id
      AND selected_count_row.session_version = v_session_row.version
      AND lower(private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json)) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(selected_count_row.status, ''))) = 'READY'
      AND COALESCE(selected_count_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(selected_count_row.selection_state, ''))) = 'SELECTED';

    SELECT COALESCE(jsonb_agg(to_jsonb(current_selected.id::text) ORDER BY current_selected.row_ordinal, current_selected.id), '[]'::jsonb),
           COUNT(*)::integer
    INTO v_selected_ids, v_current_selected_count
    FROM pg_temp._tmp_pay_wb_current_selected_rows AS current_selected
    WHERE LOWER(BTRIM(COALESCE(current_selected.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'materialisable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND COALESCE(current_selected.contract_json->>'target_section', '') = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(current_selected.contract_json->>'presentation_section', ''))) = 'READY_TO_PAY'
      AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND COALESCE(current_selected.contract_json->>'key_type', '') <> ''
      AND COALESCE(current_selected.contract_json->>'key_value', '') <> ''
      AND (
        UPPER(BTRIM(COALESCE(current_selected.contract_json->>'key_type', ''))) <> 'TS_DAY'
        OR COALESCE(current_selected.contract_json->>'key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
      )
      AND UPPER(BTRIM(COALESCE(current_selected.contract_json->>'source_kind', ''))) NOT IN (
        'TIMESHEET_SNAPSHOT',
        'TIMESHEET_SNAPSHOT_EVIDENCE',
        'RAW_TIMESHEET_SNAPSHOT',
        'INTERNAL_ONLY',
        'NO_DELTA',
        'EXCLUDED'
      )
      AND current_selected.row_key NOT LIKE 'timesheet_snapshot:%'
      AND current_selected.is_synthetic_resolved_total IS NOT TRUE;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'candidate_id',current_selected.candidate_id,
      'row_key',current_selected.row_key,
      'timesheet_id',current_selected.timesheet_id,
      'key_type',current_selected.key_type,
      'key_value',current_selected.key_value
    ) ORDER BY current_selected.candidate_id,current_selected.row_key,current_selected.id),'[]'::jsonb)
    INTO v_selected_economic_identities
    FROM pg_temp._tmp_pay_wb_current_selected_rows AS current_selected
    WHERE current_selected.id IN (
      SELECT selected_id.value::uuid
      FROM jsonb_array_elements_text(COALESCE(v_selected_ids,'[]'::jsonb)) AS selected_id(value)
    );

    v_server_selected_ids := COALESCE(v_selected_ids, '[]'::jsonb);
    v_session_ready := LOWER(BTRIM(COALESCE(
      v_session_row.progress_json->>'ready',
      v_session_row.progress_json->>'session_ready',
      v_session_row.progress_json->>'ready_flag',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');

    SELECT COALESCE(jsonb_agg(to_jsonb(blocker_code.value) ORDER BY blocker_code.ordinality), '[]'::jsonb)
    INTO v_draft_blocker_codes
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_session_row.progress_json->'draft_blocker_codes') = 'array'
          THEN v_session_row.progress_json->'draft_blocker_codes'
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS blocker_code(value, ordinality)
    WHERE UPPER(BTRIM(blocker_code.value)) <> 'NO_SELECTED_ROWS';

    IF v_session_ready AND COALESCE(v_current_selected_count, 0) = 0 THEN
      v_draft_blocker_codes := COALESCE(v_draft_blocker_codes, '[]'::jsonb)
        || jsonb_build_array('NO_SELECTED_ROWS');
    END IF;

    UPDATE public.banking_pay_workbench_sessions AS session_row
    SET selected_row_count = COALESCE(v_current_selected_count, 0),
        server_selected_preview_row_ids = COALESCE(v_server_selected_ids, '[]'::jsonb),
        server_selected_preview_row_ids_provided = true,
        progress_json = COALESCE(session_row.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_selection_update_at_utc', v_now::text,
            'last_selection_selected_delta', COALESCE(v_selected_delta, 0),
            'last_selection_deselected_delta', COALESCE(v_deselected_delta, 0),
            'last_selection_rejected_non_draftable_count', COALESCE(v_rejected_non_draftable_count, 0),
            'last_selection_mode', v_selection_mode,
            'last_selection_action', v_global_selection_action,
            'last_selection_section', v_resolved_selection_section,
            'last_selection_forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
            'last_selection_replace_omitted_count', COALESCE(v_replace_omitted_count, 0),
            'selected_row_count', COALESCE(v_current_selected_count, 0),
            'selected_eligible_ready_row_count', COALESCE(v_current_selected_count, 0),
            'selected_rows_available', COALESCE(v_current_selected_count, 0) > 0,
            'ready_for_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
            'can_create_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
            'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
            'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
            'selection_intent_v1', COALESCE(session_row.progress_json->'selection_intent_v1', '{}'::jsonb)
              || jsonb_build_object(
                'canonical_preview_lines', jsonb_build_object(
                  'mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL'),
                  'section', 'canonical_preview_lines',
                  'identity', 'preview_row_id_with_session_section_candidate_row_key_conflict_identity',
                  'updated_at_utc', v_now::text,
                  'updated_by_user_id', p_actor_user_id::text,
                  'source_selection_mode', v_selection_mode,
                  'source_selection_action', v_global_selection_action,
                  'server_selected_preview_row_ids_provided', true,
                  'selected_row_count', COALESCE(v_current_selected_count, 0),
                  'selected_economic_identities',COALESCE(v_selected_economic_identities,'[]'::jsonb),
                  'identity_contract_version',2
                )
              )
          ),
        progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_row.id = p_session_id
    RETURNING session_row.progress_counter_version
    INTO v_next_progress_counter_version;

    FOR v_revalidation_candidate_id IN
      SELECT changed_candidate.candidate_id
      FROM pg_temp._tmp_pay_wb_global_selection_candidates AS changed_candidate
    LOOP
      v_revalidation_result := public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
        p_session_id,
        v_revalidation_candidate_id
      );
    END LOOP;

    SELECT session_row.progress_counter_version
    INTO v_next_progress_counter_version
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = p_session_id;

    SELECT COALESCE(jsonb_agg(to_jsonb(selected_row.id::text) ORDER BY selected_row.row_ordinal, selected_row.id), '[]'::jsonb),
           COUNT(*)::integer
    INTO v_selected_ids,
         v_current_selected_count
    FROM public.banking_pay_workbench_preview_rows AS selected_row
    WHERE selected_row.session_id = p_session_id
      AND selected_row.session_version = v_session_row.version
      AND LOWER(private.pay_workbench_preview_effective_section_v1(selected_row.section, selected_row.row_json)) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(selected_row.status, ''))) = 'READY'
      AND COALESCE(selected_row.selected, false) IS TRUE
      AND UPPER(BTRIM(COALESCE(selected_row.selection_state, ''))) = 'SELECTED';

    v_server_selected_ids := COALESCE(v_selected_ids, '[]'::jsonb);

    v_audit_after_json := jsonb_build_object(
      'id', p_session_id::text,
      'selection_mode', v_selection_mode,
      'selection_action', v_global_selection_action,
      'selection_section', v_resolved_selection_section,
      'selection_intent_mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL'),
      'selected_preview_row_ids', COALESCE(v_selected_ids, '[]'::jsonb),
      'deselected_preview_row_ids', COALESCE(v_deselected_ids, '[]'::jsonb),
      'updated_preview_row_count', COALESCE(v_updated_count, 0),
      'selected_delta', COALESCE(v_selected_delta, 0),
      'deselected_delta', COALESCE(v_deselected_delta, 0),
      'forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
      'server_selected_preview_row_ids_provided', true,
      'updated_at_utc', v_now
    );

    PERFORM public._audit_insert(
      'banking_pay_workbench_session',
      p_session_id::text,
      CASE WHEN v_global_selection_action = 'SELECT_ALL_SECTION' THEN 'SESSION_SELECTED_ROWS_SECTION_SELECTED' ELSE 'SESSION_SELECTED_ROWS_SECTION_CLEARED' END,
      NULL::jsonb,
      v_audit_after_json,
      CASE WHEN v_global_selection_action = 'SELECT_ALL_SECTION' THEN 'SESSION_SELECTED_ROWS_SECTION_SELECTED' ELSE 'SESSION_SELECTED_ROWS_SECTION_CLEARED' END,
      p_actor_user_id
    );

    RETURN jsonb_build_object(
      'ok', true,
      'session_id', p_session_id::text,
      'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
      'session_version', v_session_row.version,
      'progress_counter_version', COALESCE(v_next_progress_counter_version, COALESCE(v_session_row.progress_counter_version, 0) + 1),
      'expected_session_version', v_expected_session_version,
      'expected_progress_counter_version', v_expected_progress_counter_version,
      'selection_action', v_global_selection_action,
      'selection_section', v_resolved_selection_section,
      'selection_intent_mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL'),
      'selected_preview_row_ids', COALESCE(v_selected_ids, '[]'::jsonb),
      'deselected_preview_row_ids', COALESCE(v_deselected_ids, '[]'::jsonb),
      'selected_preview_row_ids_provided', true,
      'server_selected_preview_row_ids', COALESCE(v_server_selected_ids, '[]'::jsonb),
      'server_selected_preview_row_ids_provided', true,
      'selected_delta', COALESCE(v_selected_delta, 0),
      'deselected_delta', COALESCE(v_deselected_delta, 0),
      'updated_preview_row_count', COALESCE(v_updated_count, 0),
      'dropped_non_draftable_preview_row_ids', COALESCE(v_non_draftable_ids, '[]'::jsonb),
      'rejected_non_draftable_preview_row_ids', COALESCE(v_non_draftable_ids, '[]'::jsonb),
      'rejected_non_draftable_preview_row_count', COALESCE(v_rejected_non_draftable_count, 0),
      'forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
      'replace_omitted_count', COALESCE(v_replace_omitted_count, 0),
      'selected_row_count', COALESCE(v_current_selected_count, 0),
      'selection_mode', v_selection_mode,
      'state_changed', true
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_selection_input_raw;
  CREATE TEMP TABLE _tmp_pay_wb_selection_input_raw ON COMMIT DROP AS
  SELECT BTRIM(select_id.value) AS supplied_id,
         'SELECT'::text AS action,
         select_id.ordinality::bigint AS first_ordinality
  FROM jsonb_array_elements_text(v_select_ids_source) WITH ORDINALITY AS select_id(value, ordinality)
  WHERE BTRIM(select_id.value) <> ''
  UNION ALL
  SELECT BTRIM(deselect_id.value) AS supplied_id,
         'DESELECT'::text AS action,
         deselect_id.ordinality::bigint AS first_ordinality
  FROM jsonb_array_elements_text(v_deselect_ids_source) WITH ORDINALITY AS deselect_id(value, ordinality)
  WHERE BTRIM(deselect_id.value) <> '';

  SELECT COALESCE(jsonb_agg(to_jsonb(conflicting_ids.supplied_id) ORDER BY conflicting_ids.supplied_id), '[]'::jsonb)
  INTO v_conflicting_ids
  FROM (
    SELECT input_raw.supplied_id
    FROM pg_temp._tmp_pay_wb_selection_input_raw AS input_raw
    GROUP BY input_raw.supplied_id
    HAVING COUNT(DISTINCT input_raw.action) > 1
  ) AS conflicting_ids;

  IF jsonb_array_length(COALESCE(v_conflicting_ids, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_CONFLICTING_ACTIONS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_SESSION_SET_SELECTED_ROWS_CONFLICTING_ACTIONS',
              'session_id', p_session_id::text,
              'conflicting_preview_row_ids', COALESCE(v_conflicting_ids, '[]'::jsonb)
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_selection_ids;
  CREATE TEMP TABLE _tmp_pay_wb_selection_ids ON COMMIT DROP AS
  SELECT input_raw.supplied_id,
         input_raw.action,
         MIN(input_raw.first_ordinality)::bigint AS first_ordinality
  FROM pg_temp._tmp_pay_wb_selection_input_raw AS input_raw
  GROUP BY input_raw.supplied_id, input_raw.action;

  SELECT COALESCE(jsonb_agg(to_jsonb(selection_ids.supplied_id) ORDER BY selection_ids.action, selection_ids.first_ordinality), '[]'::jsonb)
  INTO v_stale_selection_ids
  FROM pg_temp._tmp_pay_wb_selection_ids AS selection_ids
  WHERE EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_preview_rows AS any_preview_row
    WHERE any_preview_row.session_id = p_session_id
      AND (any_preview_row.id::text = selection_ids.supplied_id OR any_preview_row.row_key = selection_ids.supplied_id)
  )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS current_preview_row
      WHERE current_preview_row.session_id = p_session_id
        AND current_preview_row.session_version = v_session_row.version
        AND (current_preview_row.id::text = selection_ids.supplied_id OR current_preview_row.row_key = selection_ids.supplied_id)
        AND (
          selection_ids.action = 'DESELECT'
          OR UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
        )
    );

  IF jsonb_array_length(COALESCE(v_stale_selection_ids, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'WORKBENCH_STALE_SELECTION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_STALE_SELECTION',
              'session_id', p_session_id::text,
              'session_version', v_session_row.version,
              'rejected_preview_row_ids', COALESCE(v_stale_selection_ids, '[]'::jsonb),
              'message', 'Selected preview rows are not part of the current workbench session version. Refresh the preview page and try again.'
            )::text;
  END IF;

  SELECT COALESCE(jsonb_agg(to_jsonb(selection_ids.supplied_id) ORDER BY selection_ids.action, selection_ids.first_ordinality), '[]'::jsonb)
  INTO v_missing_ids
  FROM pg_temp._tmp_pay_wb_selection_ids AS selection_ids
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND (preview_row.id::text = selection_ids.supplied_id OR preview_row.row_key = selection_ids.supplied_id)
  );

  IF jsonb_array_length(COALESCE(v_missing_ids, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'selected preview row ids are not present in current session scope: %', v_missing_ids::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_selection_matched_rows;
  CREATE TEMP TABLE _tmp_pay_wb_selection_matched_rows ON COMMIT DROP AS
  SELECT DISTINCT ON (preview_row.id)
         preview_row.id,
         selection_ids.supplied_id,
         selection_ids.action,
         selection_ids.first_ordinality,
         preview_row.row_ordinal,
         COALESCE(preview_row.selected, false) AS was_selected,
         preview_row.selected AS previous_selected,
         preview_row.selection_state AS previous_selection_state,
         contract_check.contract_json,
         synthetic_check.is_synthetic_resolved_total,
         CASE
           WHEN selection_ids.action = 'DESELECT' THEN false
           ELSE LOWER(BTRIM(COALESCE(contract_check.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'materialisable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND COALESCE(contract_check.contract_json->>'target_section', '') = 'canonical_preview_lines'
             AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'presentation_section', ''))) = 'READY_TO_PAY'
             AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND LOWER(BTRIM(COALESCE(contract_check.contract_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND COALESCE(contract_check.contract_json->>'key_type', '') <> ''
             AND COALESCE(contract_check.contract_json->>'key_value', '') <> ''
             AND (
               UPPER(BTRIM(COALESCE(contract_check.contract_json->>'key_type', ''))) <> 'TS_DAY'
               OR COALESCE(contract_check.contract_json->>'key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
             )
             AND UPPER(BTRIM(COALESCE(contract_check.contract_json->>'source_kind', ''))) NOT IN (
               'TIMESHEET_SNAPSHOT',
               'TIMESHEET_SNAPSHOT_EVIDENCE',
               'RAW_TIMESHEET_SNAPSHOT',
               'INTERNAL_ONLY',
               'NO_DELTA',
               'EXCLUDED'
             )
             AND preview_row.row_key NOT LIKE 'timesheet_snapshot:%'
             AND synthetic_check.is_synthetic_resolved_total IS NOT TRUE
         END AS is_selectable
  FROM pg_temp._tmp_pay_wb_selection_ids AS selection_ids
  JOIN public.banking_pay_workbench_preview_rows AS preview_row
    ON preview_row.session_id = p_session_id
   AND preview_row.session_version = v_session_row.version
   AND (preview_row.id::text = selection_ids.supplied_id OR preview_row.row_key = selection_ids.supplied_id)
   AND (
     selection_ids.action = 'DESELECT'
     OR UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
   )
  CROSS JOIN LATERAL (
    SELECT public.pay_workbench_preview_line_contract_ok(
      p_line_json => COALESCE(preview_row.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'line_key', preview_row.row_key,
          'row_key', preview_row.row_key,
          'section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
          'target_section', private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json),
          'key_type', preview_row.key_type,
          'key_value', preview_row.key_value
        ),
      p_economic_key_json => jsonb_build_object(
        'key_type', preview_row.key_type,
        'key_value', preview_row.key_value
      ),
      p_target_section => private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)
    ) AS contract_json
  ) AS contract_check
  CROSS JOIN LATERAL (
    SELECT (
      preview_row.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(
        preview_row.key_type,
        preview_row.row_json#>>'{economic_key,key_type}',
        preview_row.row_json->>'component_key_type',
        preview_row.row_json->>'key_type',
        ''
      ))) = 'TS_TOTAL'
      AND UPPER(BTRIM(COALESCE(
        preview_row.key_value,
        preview_row.row_json#>>'{economic_key,key_value}',
        preview_row.row_json->>'component_key_value',
        preview_row.row_json->>'key_value',
        ''
      ))) = 'TOTAL'
      AND LOWER(BTRIM(COALESCE(
        preview_row.row_key,
        preview_row.row_json->>'row_key',
        preview_row.row_json->>'line_key',
        preview_row.row_json->>'source_ref',
        preview_row.row_json#>>'{source_basis,row_key}',
        preview_row.row_json#>>'{source_basis,line_key}',
        preview_row.row_json#>>'{source_basis,source_ref}',
        preview_row.row_json#>>'{source_basis_json,row_key}',
        preview_row.row_json#>>'{source_basis_json,line_key}',
        preview_row.row_json#>>'{source_basis_json,source_ref}',
        ''
      ))) LIKE '%:non_segment:total%'
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS sibling_row
        WHERE sibling_row.session_id = preview_row.session_id
          AND sibling_row.session_version = preview_row.session_version
          AND sibling_row.id <> preview_row.id
          AND sibling_row.timesheet_id = preview_row.timesheet_id
          AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
          AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
          AND UPPER(BTRIM(COALESCE(
            sibling_row.key_type,
            sibling_row.row_json#>>'{economic_key,key_type}',
            sibling_row.row_json->>'component_key_type',
            sibling_row.row_json->>'key_type',
            ''
          ))) = 'TS_DAY'
          AND COALESCE(
            sibling_row.key_value,
            sibling_row.row_json#>>'{economic_key,key_value}',
            sibling_row.row_json->>'component_key_value',
            sibling_row.row_json->>'key_value',
            ''
          ) ~ '^\d{4}-\d{2}-\d{2}$'
      )
    ) AS is_synthetic_resolved_total
  ) AS synthetic_check
  ORDER BY preview_row.id, selection_ids.first_ordinality;

  SELECT COALESCE(jsonb_agg(to_jsonb(non_draftable.preview_row_id) ORDER BY non_draftable.first_ordinality, non_draftable.preview_row_id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_non_draftable_ids, v_rejected_non_draftable_count
  FROM (
    SELECT DISTINCT matched_rows.id::text AS preview_row_id,
           matched_rows.first_ordinality
    FROM pg_temp._tmp_pay_wb_selection_matched_rows AS matched_rows
    WHERE matched_rows.action = 'SELECT'
      AND matched_rows.is_selectable IS NOT TRUE
  ) AS non_draftable;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_replace_omitted_rows;
  CREATE TEMP TABLE _tmp_pay_wb_replace_omitted_rows ON COMMIT DROP AS
  SELECT preview_row.id,
         COALESCE(preview_row.selected, false) AS was_selected,
         (1000000000::bigint + ROW_NUMBER() OVER (ORDER BY preview_row.row_ordinal, preview_row.id))::bigint AS first_ordinality
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE v_replace_mode
    AND preview_row.session_id = p_session_id
    AND preview_row.session_version = v_session_row.version
    AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
    AND COALESCE(preview_row.selected, false) = true
    AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._tmp_pay_wb_selection_matched_rows AS matched_rows
      WHERE matched_rows.id = preview_row.id
        AND matched_rows.action = 'SELECT'
    );

  SELECT COUNT(*)::integer
  INTO v_replace_omitted_count
  FROM pg_temp._tmp_pay_wb_replace_omitted_rows AS omitted_rows;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_synthetic_cleanup_rows;
  CREATE TEMP TABLE _tmp_pay_wb_synthetic_cleanup_rows ON COMMIT DROP AS
  SELECT preview_row.id,
         COALESCE(preview_row.selected, false) AS was_selected,
         (2000000000::bigint + ROW_NUMBER() OVER (ORDER BY preview_row.row_ordinal, preview_row.id))::bigint AS first_ordinality
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  CROSS JOIN LATERAL (
    SELECT (
      preview_row.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(
        preview_row.key_type,
        preview_row.row_json#>>'{economic_key,key_type}',
        preview_row.row_json->>'component_key_type',
        preview_row.row_json->>'key_type',
        ''
      ))) = 'TS_TOTAL'
      AND UPPER(BTRIM(COALESCE(
        preview_row.key_value,
        preview_row.row_json#>>'{economic_key,key_value}',
        preview_row.row_json->>'component_key_value',
        preview_row.row_json->>'key_value',
        ''
      ))) = 'TOTAL'
      AND LOWER(BTRIM(COALESCE(
        preview_row.row_key,
        preview_row.row_json->>'row_key',
        preview_row.row_json->>'line_key',
        preview_row.row_json->>'source_ref',
        preview_row.row_json#>>'{source_basis,row_key}',
        preview_row.row_json#>>'{source_basis,line_key}',
        preview_row.row_json#>>'{source_basis,source_ref}',
        preview_row.row_json#>>'{source_basis_json,row_key}',
        preview_row.row_json#>>'{source_basis_json,line_key}',
        preview_row.row_json#>>'{source_basis_json,source_ref}',
        ''
      ))) LIKE '%:non_segment:total%'
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS sibling_row
        WHERE sibling_row.session_id = preview_row.session_id
          AND sibling_row.session_version = preview_row.session_version
          AND sibling_row.id <> preview_row.id
          AND sibling_row.timesheet_id = preview_row.timesheet_id
          AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
          AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
          AND UPPER(BTRIM(COALESCE(
            sibling_row.key_type,
            sibling_row.row_json#>>'{economic_key,key_type}',
            sibling_row.row_json->>'component_key_type',
            sibling_row.row_json->>'key_type',
            ''
          ))) = 'TS_DAY'
          AND COALESCE(
            sibling_row.key_value,
            sibling_row.row_json#>>'{economic_key,key_value}',
            sibling_row.row_json->>'component_key_value',
            sibling_row.row_json->>'key_value',
            ''
          ) ~ '^\d{4}-\d{2}-\d{2}$'
      )
    ) AS is_synthetic_resolved_total
  ) AS synthetic_check
  WHERE preview_row.session_id = p_session_id
    AND preview_row.session_version = v_session_row.version
    AND lower(private.pay_workbench_preview_effective_section_v1(preview_row.section, preview_row.row_json)) = 'canonical_preview_lines'
    AND COALESCE(preview_row.selected, false) = true
    AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
    AND synthetic_check.is_synthetic_resolved_total;

  SELECT COUNT(*)::integer
  INTO v_forced_synthetic_cleanup_count
  FROM pg_temp._tmp_pay_wb_synthetic_cleanup_rows AS cleanup_rows;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_update_actions;
  CREATE TEMP TABLE _tmp_pay_wb_update_actions ON COMMIT DROP AS
  WITH raw_actions AS (
    SELECT matched_rows.id,
           CASE
             WHEN matched_rows.action = 'SELECT' AND matched_rows.is_selectable THEN 'SELECT'::text
             WHEN matched_rows.action = 'SELECT' THEN 'REJECT_SELECT'::text
             ELSE 'DESELECT'::text
           END AS action,
           matched_rows.was_selected,
           matched_rows.first_ordinality
    FROM pg_temp._tmp_pay_wb_selection_matched_rows AS matched_rows
    UNION ALL
    SELECT omitted_rows.id,
           'DESELECT'::text AS action,
           omitted_rows.was_selected,
           omitted_rows.first_ordinality
    FROM pg_temp._tmp_pay_wb_replace_omitted_rows AS omitted_rows
    UNION ALL
    SELECT cleanup_rows.id,
           'DESELECT'::text AS action,
           cleanup_rows.was_selected,
           cleanup_rows.first_ordinality
    FROM pg_temp._tmp_pay_wb_synthetic_cleanup_rows AS cleanup_rows
  )
  SELECT DISTINCT ON (raw_actions.id)
         raw_actions.id,
         raw_actions.action,
         raw_actions.was_selected,
         raw_actions.first_ordinality
  FROM raw_actions
  ORDER BY raw_actions.id,
           CASE raw_actions.action
             WHEN 'DESELECT' THEN 1
             WHEN 'REJECT_SELECT' THEN 2
             WHEN 'SELECT' THEN 3
             ELSE 4
           END,
           raw_actions.first_ordinality;

  WITH updated_rows AS (
    UPDATE public.banking_pay_workbench_preview_rows AS preview_row
    SET selected = CASE
          WHEN update_actions.action = 'SELECT' THEN true
          ELSE false
        END,
        selection_state = CASE
          WHEN update_actions.action = 'SELECT' THEN 'SELECTED'
          WHEN update_actions.action = 'REJECT_SELECT' THEN 'NOT_SELECTABLE'
          ELSE 'UNSELECTED'
        END,
        row_json = COALESCE(preview_row.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'selected', update_actions.action = 'SELECT',
            'selection_state', CASE
              WHEN update_actions.action = 'SELECT' THEN 'SELECTED'
              WHEN update_actions.action = 'REJECT_SELECT' THEN 'NOT_SELECTABLE'
              ELSE 'UNSELECTED'
            END,
            'selection_user_override', CASE
              WHEN update_actions.action = 'SELECT' THEN 'SELECTED'
              WHEN update_actions.action = 'DESELECT' THEN 'UNSELECTED'
              ELSE NULL
            END,
            'selection_origin', CASE
              WHEN update_actions.action = 'SELECT' THEN 'USER_EXPLICIT_SELECT'
              WHEN update_actions.action = 'DESELECT' THEN 'USER_EXPLICIT_DESELECT'
              ELSE NULL
            END,
            'selection_user_override_at_utc', CASE
              WHEN update_actions.action IN ('SELECT', 'DESELECT') THEN v_now::text
              ELSE NULL
            END
          ),
        updated_at_utc = v_now
    FROM pg_temp._tmp_pay_wb_update_actions AS update_actions
    WHERE preview_row.id = update_actions.id
    RETURNING preview_row.id,
              update_actions.action,
              update_actions.was_selected,
              preview_row.selected,
              update_actions.first_ordinality
  )
  SELECT
    COALESCE((SELECT jsonb_agg(to_jsonb(updated_rows.id::text) ORDER BY updated_rows.first_ordinality, updated_rows.id) FROM updated_rows WHERE updated_rows.action = 'DESELECT'), '[]'::jsonb),
    COALESCE((SELECT COUNT(*)::integer FROM updated_rows WHERE updated_rows.action = 'SELECT' AND updated_rows.was_selected IS NOT TRUE AND updated_rows.selected), 0),
    COALESCE((SELECT COUNT(*)::integer FROM updated_rows WHERE updated_rows.action = 'DESELECT' AND updated_rows.was_selected), 0),
    COALESCE((SELECT COUNT(*)::integer FROM updated_rows), 0)
  INTO v_deselected_ids, v_selected_delta, v_deselected_delta, v_updated_count;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_current_selected_rows;
  CREATE TEMP TABLE _tmp_pay_wb_current_selected_rows ON COMMIT DROP AS
  SELECT selected_count_row.id,
         selected_count_row.candidate_id,
         selected_count_row.row_ordinal,
         selected_count_row.row_key,
         selected_count_row.row_json,
         selected_count_row.timesheet_id,
         selected_count_row.key_type,
         selected_count_row.key_value,
         selected_contract.contract_json,
         synthetic_check.is_synthetic_resolved_total
  FROM public.banking_pay_workbench_preview_rows AS selected_count_row
  CROSS JOIN LATERAL (
    SELECT public.pay_workbench_preview_line_contract_ok(
      p_line_json => COALESCE(selected_count_row.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'line_key', selected_count_row.row_key,
          'row_key', selected_count_row.row_key,
          'section', private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json),
          'target_section', private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json),
          'key_type', selected_count_row.key_type,
          'key_value', selected_count_row.key_value
        ),
      p_economic_key_json => jsonb_build_object(
        'key_type', selected_count_row.key_type,
        'key_value', selected_count_row.key_value
      ),
      p_target_section => private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json)
    ) AS contract_json
  ) AS selected_contract
  CROSS JOIN LATERAL (
    SELECT (
      selected_count_row.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(
        selected_count_row.key_type,
        selected_count_row.row_json#>>'{economic_key,key_type}',
        selected_count_row.row_json->>'component_key_type',
        selected_count_row.row_json->>'key_type',
        ''
      ))) = 'TS_TOTAL'
      AND UPPER(BTRIM(COALESCE(
        selected_count_row.key_value,
        selected_count_row.row_json#>>'{economic_key,key_value}',
        selected_count_row.row_json->>'component_key_value',
        selected_count_row.row_json->>'key_value',
        ''
      ))) = 'TOTAL'
      AND LOWER(BTRIM(COALESCE(
        selected_count_row.row_key,
        selected_count_row.row_json->>'row_key',
        selected_count_row.row_json->>'line_key',
        selected_count_row.row_json->>'source_ref',
        selected_count_row.row_json#>>'{source_basis,row_key}',
        selected_count_row.row_json#>>'{source_basis,line_key}',
        selected_count_row.row_json#>>'{source_basis,source_ref}',
        selected_count_row.row_json#>>'{source_basis_json,row_key}',
        selected_count_row.row_json#>>'{source_basis_json,line_key}',
        selected_count_row.row_json#>>'{source_basis_json,source_ref}',
        ''
      ))) LIKE '%:non_segment:total%'
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS sibling_row
        WHERE sibling_row.session_id = selected_count_row.session_id
          AND sibling_row.session_version = selected_count_row.session_version
          AND sibling_row.id <> selected_count_row.id
          AND sibling_row.timesheet_id = selected_count_row.timesheet_id
          AND lower(private.pay_workbench_preview_effective_section_v1(sibling_row.section, sibling_row.row_json)) = 'canonical_preview_lines'
          AND UPPER(BTRIM(COALESCE(sibling_row.status, ''))) = 'READY'
          AND UPPER(BTRIM(COALESCE(
            sibling_row.key_type,
            sibling_row.row_json#>>'{economic_key,key_type}',
            sibling_row.row_json->>'component_key_type',
            sibling_row.row_json->>'key_type',
            ''
          ))) = 'TS_DAY'
          AND COALESCE(
            sibling_row.key_value,
            sibling_row.row_json#>>'{economic_key,key_value}',
            sibling_row.row_json->>'component_key_value',
            sibling_row.row_json->>'key_value',
            ''
          ) ~ '^\d{4}-\d{2}-\d{2}$'
      )
    ) AS is_synthetic_resolved_total
  ) AS synthetic_check
  WHERE selected_count_row.session_id = p_session_id
    AND selected_count_row.session_version = v_session_row.version
    AND lower(private.pay_workbench_preview_effective_section_v1(selected_count_row.section, selected_count_row.row_json)) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(selected_count_row.status, ''))) = 'READY'
    AND COALESCE(selected_count_row.selected, false) = true
    AND UPPER(BTRIM(COALESCE(selected_count_row.selection_state, ''))) = 'SELECTED';

  SELECT COALESCE(jsonb_agg(to_jsonb(current_selected.id::text) ORDER BY current_selected.row_ordinal, current_selected.id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_selected_ids, v_current_selected_count
  FROM pg_temp._tmp_pay_wb_current_selected_rows AS current_selected
  WHERE LOWER(BTRIM(COALESCE(current_selected.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'materialisable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND COALESCE(current_selected.contract_json->>'target_section', '') = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(current_selected.contract_json->>'presentation_section', ''))) = 'READY_TO_PAY'
    AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND LOWER(BTRIM(COALESCE(current_selected.contract_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND COALESCE(current_selected.contract_json->>'key_type', '') <> ''
    AND COALESCE(current_selected.contract_json->>'key_value', '') <> ''
    AND (
      UPPER(BTRIM(COALESCE(current_selected.contract_json->>'key_type', ''))) <> 'TS_DAY'
      OR COALESCE(current_selected.contract_json->>'key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
    )
    AND UPPER(BTRIM(COALESCE(current_selected.contract_json->>'source_kind', ''))) NOT IN (
      'TIMESHEET_SNAPSHOT',
      'TIMESHEET_SNAPSHOT_EVIDENCE',
      'RAW_TIMESHEET_SNAPSHOT',
      'INTERNAL_ONLY',
      'NO_DELTA',
      'EXCLUDED'
    )
    AND current_selected.row_key NOT LIKE 'timesheet_snapshot:%'
    AND current_selected.is_synthetic_resolved_total IS NOT TRUE;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'candidate_id',current_selected.candidate_id,
    'row_key',current_selected.row_key,
    'timesheet_id',current_selected.timesheet_id,
    'key_type',current_selected.key_type,
    'key_value',current_selected.key_value
  ) ORDER BY current_selected.candidate_id,current_selected.row_key,current_selected.id),'[]'::jsonb)
  INTO v_selected_economic_identities
  FROM pg_temp._tmp_pay_wb_current_selected_rows AS current_selected
  WHERE current_selected.id IN (
    SELECT selected_id.value::uuid
    FROM jsonb_array_elements_text(COALESCE(v_selected_ids,'[]'::jsonb)) AS selected_id(value)
  );

  v_next_selection_intent_mode := CASE
    WHEN v_replace_mode THEN 'EXPLICIT_INCLUDE'
    ELSE 'IMPLICIT_ALL'
  END;
  v_next_server_selected_ids_provided := (v_next_selection_intent_mode = 'EXPLICIT_INCLUDE');
  v_server_selected_ids := CASE
    WHEN v_next_server_selected_ids_provided THEN COALESCE(v_selected_ids, '[]'::jsonb)
    ELSE '[]'::jsonb
  END;
  v_session_ready := LOWER(BTRIM(COALESCE(
    v_session_row.progress_json->>'ready',
    v_session_row.progress_json->>'session_ready',
    v_session_row.progress_json->>'ready_flag',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COALESCE(jsonb_agg(to_jsonb(blocker_code.value) ORDER BY blocker_code.ordinality), '[]'::jsonb)
  INTO v_draft_blocker_codes
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_session_row.progress_json->'draft_blocker_codes') = 'array'
        THEN v_session_row.progress_json->'draft_blocker_codes'
      ELSE '[]'::jsonb
    END
  ) WITH ORDINALITY AS blocker_code(value, ordinality)
  WHERE UPPER(BTRIM(blocker_code.value)) <> 'NO_SELECTED_ROWS';

  IF v_session_ready AND COALESCE(v_current_selected_count, 0) = 0 THEN
    v_draft_blocker_codes := COALESCE(v_draft_blocker_codes, '[]'::jsonb)
      || jsonb_build_array('NO_SELECTED_ROWS');
  END IF;

  UPDATE public.banking_pay_workbench_sessions AS session_row
  SET selected_row_count = COALESCE(v_current_selected_count, 0),
      server_selected_preview_row_ids = COALESCE(v_server_selected_ids, '[]'::jsonb),
      server_selected_preview_row_ids_provided = COALESCE(v_next_server_selected_ids_provided, false),
      progress_json = COALESCE(session_row.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_selection_update_at_utc', v_now::text,
          'last_selection_selected_delta', COALESCE(v_selected_delta, 0),
          'last_selection_deselected_delta', COALESCE(v_deselected_delta, 0),
          'last_selection_rejected_non_draftable_count', COALESCE(v_rejected_non_draftable_count, 0),
          'last_selection_mode', v_selection_mode,
          'last_selection_forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
          'last_selection_replace_omitted_count', COALESCE(v_replace_omitted_count, 0),
          'selected_row_count', COALESCE(v_current_selected_count, 0),
          'selected_eligible_ready_row_count', COALESCE(v_current_selected_count, 0),
          'selected_rows_available', COALESCE(v_current_selected_count, 0) > 0,
          'ready_for_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
          'can_create_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
          'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
          'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
          'selection_intent_v1', COALESCE(session_row.progress_json->'selection_intent_v1', '{}'::jsonb)
            || jsonb_build_object(
              'canonical_preview_lines', jsonb_build_object(
                'mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL'),
                'section', 'canonical_preview_lines',
                'identity', 'preview_row_id_with_session_section_candidate_row_key_conflict_identity',
                'updated_at_utc', v_now::text,
                'updated_by_user_id', p_actor_user_id::text,
                'source_selection_mode', v_selection_mode,
                'source_selection_action', CASE WHEN v_replace_mode THEN 'REPLACE_SELECTED_ROWS' ELSE 'ROW_PATCH' END,
                'server_selected_preview_row_ids_provided', COALESCE(v_next_server_selected_ids_provided, false),
                'selected_row_count', COALESCE(v_current_selected_count, 0),
                'selected_economic_identities',COALESCE(v_selected_economic_identities,'[]'::jsonb),
                'identity_contract_version',2
              )
            )
        ),
      progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
      progress_updated_at_utc = v_now,
      updated_at_utc = v_now
  WHERE session_row.id = p_session_id
  RETURNING session_row.progress_counter_version
  INTO v_next_progress_counter_version;

  FOR v_revalidation_candidate_id IN
    SELECT DISTINCT preview_row.candidate_id
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    JOIN pg_temp._tmp_pay_wb_update_actions AS changed_row
      ON changed_row.id = preview_row.id
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND preview_row.candidate_id IS NOT NULL
  LOOP
    v_revalidation_result := public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
      p_session_id,
      v_revalidation_candidate_id
    );
  END LOOP;

  SELECT session_row.progress_counter_version
  INTO v_next_progress_counter_version
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_row.id::text) ORDER BY selected_row.row_ordinal, selected_row.id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_selected_ids,
       v_current_selected_count
  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = v_session_row.version
    AND LOWER(private.pay_workbench_preview_effective_section_v1(selected_row.section, selected_row.row_json)) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(selected_row.status, ''))) = 'READY'
    AND COALESCE(selected_row.selected, false) IS TRUE
    AND UPPER(BTRIM(COALESCE(selected_row.selection_state, ''))) = 'SELECTED';

  v_server_selected_ids := CASE
    WHEN v_next_server_selected_ids_provided THEN COALESCE(v_selected_ids, '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_audit_after_json := jsonb_build_object(
    'id', p_session_id::text,
    'selection_mode', v_selection_mode,
    'selected_preview_row_ids', COALESCE(v_selected_ids, '[]'::jsonb),
    'deselected_preview_row_ids', COALESCE(v_deselected_ids, '[]'::jsonb),
    'rejected_non_draftable_preview_row_ids', COALESCE(v_non_draftable_ids, '[]'::jsonb),
    'rejected_non_draftable_preview_row_count', COALESCE(v_rejected_non_draftable_count, 0),
    'updated_preview_row_count', COALESCE(v_updated_count, 0),
    'selected_delta', COALESCE(v_selected_delta, 0),
    'deselected_delta', COALESCE(v_deselected_delta, 0),
    'forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
    'replace_omitted_count', COALESCE(v_replace_omitted_count, 0),
    'selection_intent_mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL'),
    'server_selected_preview_row_ids_provided', COALESCE(v_next_server_selected_ids_provided, false),
    'updated_at_utc', v_now
  );

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',
    p_session_id::text,
    CASE WHEN v_replace_mode THEN 'SESSION_SELECTED_ROWS_REPLACED' ELSE 'SESSION_SELECTED_ROWS_PATCHED' END,
    NULL::jsonb,
    v_audit_after_json,
    CASE WHEN v_replace_mode THEN 'SESSION_SELECTED_ROWS_REPLACED' ELSE 'SESSION_SELECTED_ROWS_PATCHED' END,
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
    'session_version', v_session_row.version,
    'progress_counter_version', COALESCE(v_next_progress_counter_version, COALESCE(v_session_row.progress_counter_version, 0) + 1),
    'expected_session_version', v_expected_session_version,
    'expected_progress_counter_version', v_expected_progress_counter_version,
    'selected_preview_row_ids', COALESCE(v_selected_ids, '[]'::jsonb),
    'deselected_preview_row_ids', COALESCE(v_deselected_ids, '[]'::jsonb),
    'selected_preview_row_ids_provided', COALESCE(v_next_server_selected_ids_provided, false),
    'server_selected_preview_row_ids', COALESCE(v_server_selected_ids, '[]'::jsonb),
    'server_selected_preview_row_ids_provided', COALESCE(v_next_server_selected_ids_provided, false),
    'selected_delta', COALESCE(v_selected_delta, 0),
    'deselected_delta', COALESCE(v_deselected_delta, 0),
    'updated_preview_row_count', COALESCE(v_updated_count, 0),
    'dropped_non_draftable_preview_row_ids', COALESCE(v_non_draftable_ids, '[]'::jsonb),
    'rejected_non_draftable_preview_row_ids', COALESCE(v_non_draftable_ids, '[]'::jsonb),
    'rejected_non_draftable_preview_row_count', COALESCE(v_rejected_non_draftable_count, 0),
    'forced_synthetic_cleanup_count', COALESCE(v_forced_synthetic_cleanup_count, 0),
    'replace_omitted_count', COALESCE(v_replace_omitted_count, 0),
    'selected_row_count', COALESCE(v_current_selected_count, 0),
    'ready_for_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
    'can_create_draft', v_session_ready AND COALESCE(v_current_selected_count, 0) > 0,
    'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
    'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
    'selection_mode', v_selection_mode,
    'selection_intent_mode', COALESCE(v_next_selection_intent_mode, 'IMPLICIT_ALL')
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_set_selected_rows(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_set_selected_rows(uuid, jsonb, uuid)
  FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_set_selected_rows(uuid, jsonb, uuid)
  TO postgres, authenticated, service_role;
