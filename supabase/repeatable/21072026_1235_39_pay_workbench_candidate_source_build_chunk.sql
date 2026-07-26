-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 22e48950f854.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_source_build_chunk(p_session_id uuid, p_candidate_id uuid, p_cursor_json jsonb DEFAULT NULL::jsonb, p_payload_json jsonb DEFAULT '{}'::jsonb, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
 SET plpgsql_check.mode TO 'disabled'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_started_at_utc timestamptz := clock_timestamp();
  v_total_elapsed_ms numeric := 0;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_status text := NULL::text;
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_cursor_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_cursor_json, v_payload_json->'cursor', v_payload_json->'source_cursor', v_payload_json#>'{source_build,cursor}', '{}'::jsonb)) = 'object'
      THEN COALESCE(p_cursor_json, v_payload_json->'cursor', v_payload_json->'source_cursor', v_payload_json#>'{source_build,cursor}', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_payload_session_id_text text := NULL::text;
  v_payload_candidate_id_text text := NULL::text;
  v_source_build_run_id_text text := NULL::text;
  v_source_build_run_id uuid := NULL::uuid;
  v_source_change_seq_text text := NULL::text;
  v_source_change_seq bigint := 0;
  v_source_change_seq_was_supplied boolean := false;
  v_initial_source_change_seq bigint := 0;
  v_post_sync_source_change_seq bigint := 0;
  v_payload_session_version_text text := NULL::text;
  v_session_version bigint := NULL::bigint;
  v_payload_snapshot_run_id_text text := NULL::text;
  v_payload_snapshot_run_id uuid := NULL::uuid;
  v_payload_session_signature text := NULL::text;
  v_initial_session_signature text := NULL::text;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_payload_refresh_scope_kind text := NULL::text;
  v_cursor_requested_refresh_scope_kind text := NULL::text;
  v_source_build_cursor_continuation boolean := false;
  v_requested_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_actual_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_targeted_payload_received boolean := false;
  v_pay_channel_scope text := 'ALL';
  v_candidate_pay_channel_scope text := NULL::text;
  v_first_source_page boolean := true;
  v_cursor_last_timesheet_id uuid := NULL::uuid;
  v_cursor_source_ordinal_base bigint := 0;
  v_next_source_ordinal bigint := 0;
  v_next_cursor_json jsonb := NULL::jsonb;
  v_has_more boolean := false;
  v_cursor_advanced boolean := false;
  v_context_json jsonb := '{}'::jsonb;
  v_preview_decisions_json jsonb := '{}'::jsonb;
  v_collect_result jsonb := '{}'::jsonb;
  v_collect_diagnostics_json jsonb := '{}'::jsonb;
  v_canonical_result jsonb := '{}'::jsonb;
  v_canonical_diagnostics_json jsonb := '{}'::jsonb;
  v_collect_elapsed_ms numeric := 0;
  v_canonical_elapsed_ms numeric := 0;
  v_classifier_elapsed_ms numeric := 0;
  v_source_canonical_preview_line_count integer := 0;
  v_materialisable_source_line_count integer := 0;
  v_contract_rejected_count integer := 0;
  v_source_rows_written integer := 0;
  v_current_source_row_count integer := 0;
  v_source_rows_superseded integer := 0;
  v_source_reconcile_result jsonb := '{}'::jsonb;
  v_reconciled_source_rows_superseded integer := 0;
  v_reconciled_source_build_jobs_superseded integer := 0;
  v_timesheets_seen integer := 0;
  v_source_page_count integer := 0;
  v_source_remaining_after_cursor_count integer := 0;
  v_source_cursor_out_json jsonb := NULL::jsonb;
  v_source_cursor_in_json jsonb := NULL::jsonb;
  v_fallback_used boolean := false;
  v_fallback_reason text := NULL::text;
  v_fallback_from_delta boolean := false;
  v_delta_fallback_reason text := NULL::text;
  v_delta_projection_run_id_text text := NULL::text;
  v_delta_projection_class text := NULL::text;
  v_candidate_filter_applied_early boolean := false;
  v_timesheet_filter_applied_early boolean := false;
  v_large_aggregation_avoided boolean := false;
  v_collect_called_inside_canonical boolean := false;
  v_sync_result jsonb := '{}'::jsonb;
  v_sync_completed boolean := false;
  v_sync_attested boolean := false;
  v_sync_invoked boolean := false;
  v_sync_marker_reused boolean := false;
  v_sync_attestation jsonb := '{}'::jsonb;
  v_sync_result_code text := NULL::text;
  v_old_sync_marker jsonb := '{}'::jsonb;
  v_sync_scope_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_sync_scope_timesheet_ids_json jsonb := '[]'::jsonb;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_sync_scope_digest text := NULL::text;
  v_sync_negative_digest text := NULL::text;
  v_sync_baseline_digest text := NULL::text;
  v_sync_negative_component_count integer := 0;
  v_sync_durable_component_count integer := 0;
  v_sync_protected_component_count integer := 0;
  v_sync_resolution_pending_component_count integer := 0;
  v_sync_uncovered_component_count integer := 0;
  v_sync_candidate_covered boolean := false;
  v_sync_result_out_of_scope_count integer := 0;
  v_current_resolution_pending_member_ids uuid[] := ARRAY[]::uuid[];
  v_current_source_change_seq bigint := 0;
  v_final_source_change_seq bigint := 0;
  v_post_sync_candidate_pay_channel_scope text := NULL::text;
  v_post_sync_scope_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_post_sync_scope_timesheet_ids_json jsonb := '[]'::jsonb;
  v_post_sync_scope_digest text := NULL::text;
  v_post_sync_negative_component_count integer := 0;
  v_post_sync_negative_digest text := NULL::text;
  v_post_sync_negative_components_diagnostic jsonb := '[]'::jsonb;
  v_post_sync_baseline_digest text := NULL::text;
  v_sync_started_at_utc timestamptz := NULL::timestamptz;
  v_sync_authority_token text := NULL::text;
  v_existing_source_row_count integer := 0;
  v_client_filter_single uuid := NULL::uuid;
  v_filter_client_id_text text := NULL::text;
  v_force_include_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_exclude_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_mismatch_choices_json jsonb := '{}'::jsonb;
  v_session_progress_update_applied boolean := false;
  v_session_progress_lock_skipped boolean := false;
  v_session_progress_update_row_count integer := 0;
  v_diag_phase_started_at timestamptz := v_started_at_utc;
  v_budget_apply_elapsed_ms numeric := 0;
  v_initial_validation_elapsed_ms numeric := 0;
  v_overpayment_sync_elapsed_ms numeric := 0;
  v_preview_context_elapsed_ms numeric := 0;
  v_collect_call_wall_elapsed_ms numeric := 0;
  v_canonical_call_wall_elapsed_ms numeric := 0;
  v_temp_transform_elapsed_ms numeric := 0;
  v_temp_counts_elapsed_ms numeric := 0;
  v_scope_timesheet_temp_elapsed_ms numeric := 0;
  v_final_revalidation_elapsed_ms numeric := 0;
  v_source_retire_elapsed_ms numeric := 0;
  v_source_upsert_elapsed_ms numeric := 0;
  v_source_write_guard_elapsed_ms numeric := 0;
  v_current_source_count_elapsed_ms numeric := 0;
  v_reconciliation_defer_elapsed_ms numeric := 0;
  v_next_cursor_elapsed_ms numeric := 0;
  v_scope_update_elapsed_ms numeric := 0;
  v_session_progress_update_elapsed_ms numeric := 0;
  v_residual_elapsed_ms numeric := 0;
  v_residual_wall_elapsed_ms numeric := 0;
  v_residual_measured_elapsed_ms numeric := 0;
  v_residual_unattributed_elapsed_ms numeric := 0;
  v_source_build_timing_json jsonb := '{}'::jsonb;
BEGIN
  perform public._ctms_assert_payload_corrections_fresh_v1(coalesce(p_payload_json, '{}'::jsonb), 'PAY_WORKBENCH_SOURCE_BUILD');
  v_diag_phase_started_at := clock_timestamp();
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');
  v_budget_apply_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_REQUIRED')::text;
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_REQUIRED')::text;
  END IF;

  IF jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_PAYLOAD_NOT_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_PAYLOAD_NOT_OBJECT')::text;
  END IF;

  v_fallback_from_delta := LOWER(BTRIM(COALESCE(v_payload_json->>'fallback_from_delta', v_payload_json#>>'{source_build,fallback_from_delta}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_delta_fallback_reason := NULLIF(BTRIM(COALESCE(v_payload_json->>'fallback_reason', v_payload_json#>>'{source_build,fallback_reason}', '')), '');
  v_delta_projection_run_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'projection_run_id', v_payload_json#>>'{source_build,projection_run_id}', '')), '');
  v_delta_projection_class := NULLIF(UPPER(BTRIM(COALESCE(v_payload_json->>'projection_class', v_payload_json#>>'{source_build,projection_class}', ''))), '');

  IF to_regclass('public.banking_pay_workbench_candidate_source_lines') IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_TABLE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_TABLE_MISSING',
              'required_table', 'public.banking_pay_workbench_candidate_source_lines'
            )::text;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND',
              'session_id', p_session_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN',
              'session_id', p_session_id::text,
              'status', v_session_row.status
            )::text;
  END IF;

  v_initial_session_signature := v_session_row.session_signature;

  SELECT scope_row.status
  INTO v_scope_status
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_IN_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_IN_SCOPE',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  PERFORM 1
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_FOUND',
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  v_payload_session_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'session_id',
    v_payload_json->>'workbench_session_id',
    v_payload_json#>>'{workbench,session_id}',
    v_cursor_json->>'session_id',
    ''
  )), '');

  IF v_payload_session_id_text IS NOT NULL
     AND v_payload_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND v_payload_session_id_text::uuid IS DISTINCT FROM p_session_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_MISMATCH',
              'argument_session_id', p_session_id::text,
              'payload_session_id', v_payload_session_id_text
            )::text;
  ELSIF v_payload_session_id_text IS NOT NULL
        AND v_payload_session_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_ID_INVALID',
              'payload_session_id', v_payload_session_id_text
            )::text;
  END IF;

  v_payload_candidate_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'candidate_id',
    v_payload_json#>>'{candidate,id}',
    v_cursor_json->>'candidate_id',
    ''
  )), '');

  IF v_payload_candidate_id_text IS NOT NULL
     AND v_payload_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND v_payload_candidate_id_text::uuid IS DISTINCT FROM p_candidate_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_MISMATCH',
              'argument_candidate_id', p_candidate_id::text,
              'payload_candidate_id', v_payload_candidate_id_text
            )::text;
  ELSIF v_payload_candidate_id_text IS NOT NULL
        AND v_payload_candidate_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_ID_INVALID',
              'payload_candidate_id', v_payload_candidate_id_text
            )::text;
  END IF;

  v_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'source_build_run_id',
    v_payload_json#>>'{source_build,run_id}',
    v_payload_json#>>'{source_build,source_build_run_id}',
    v_cursor_json->>'source_build_run_id',
    ''
  )), '');

  IF v_source_build_run_id_text IS NULL
     OR v_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED',
              'message', 'WORKBENCH_CANDIDATE_SOURCE_BUILD requires a stable source_build_run_id in payload_json.'
            )::text;
  END IF;
  v_source_build_run_id := v_source_build_run_id_text::uuid;

  v_source_change_seq_text := NULLIF(BTRIM(COALESCE(
    v_cursor_json->>'source_change_seq',
    v_payload_json->>'source_change_seq',
    v_payload_json->>'source_change_sequence',
    v_payload_json#>>'{source_build,source_change_seq}',
    ''
  )), '');

  v_source_change_seq_was_supplied := v_source_change_seq_text IS NOT NULL;

  IF v_source_change_seq_text IS NULL THEN
    v_source_change_seq := 0;
  ELSIF v_source_change_seq_text ~ '^[0-9]{1,18}$' THEN
    v_source_change_seq := v_source_change_seq_text::bigint;
  ELSE
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_INVALID',
              'source_change_seq', v_source_change_seq_text
            )::text;
  END IF;

  v_payload_session_version_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'session_version',
    v_payload_json#>>'{source_build,session_version}',
    v_cursor_json->>'session_version',
    ''
  )), '');

  IF v_payload_session_version_text IS NULL THEN
    v_session_version := COALESCE(v_session_row.version, 1);
  ELSIF v_payload_session_version_text ~ '^[0-9]{1,18}$' THEN
    v_session_version := v_payload_session_version_text::bigint;
  ELSE
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_INVALID',
              'session_version', v_payload_session_version_text
            )::text;
  END IF;

  IF v_session_version IS DISTINCT FROM COALESCE(v_session_row.version, 1) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE',
              'payload_session_version', v_session_version,
              'current_session_version', COALESCE(v_session_row.version, 1)
            )::text;
  END IF;

  v_payload_snapshot_run_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'source_snapshot_run_id',
    v_payload_json->>'snapshot_run_id',
    v_payload_json#>>'{source_build,source_snapshot_run_id}',
    v_cursor_json->>'source_snapshot_run_id',
    ''
  )), '');

  IF v_payload_snapshot_run_id_text IS NULL THEN
    v_payload_snapshot_run_id := v_session_row.source_snapshot_run_id;
  ELSIF v_payload_snapshot_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_payload_snapshot_run_id := v_payload_snapshot_run_id_text::uuid;
  ELSE
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_INVALID',
              'source_snapshot_run_id', v_payload_snapshot_run_id_text
            )::text;
  END IF;

  IF v_payload_snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH',
              'payload_source_snapshot_run_id', v_payload_snapshot_run_id::text,
              'session_source_snapshot_run_id', v_session_row.source_snapshot_run_id::text
            )::text;
  END IF;

  v_payload_session_signature := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'session_signature',
    v_payload_json#>>'{source_build,session_signature}',
    v_cursor_json->>'session_signature',
    ''
  )), '');

  IF v_payload_session_signature IS NOT NULL
     AND v_payload_session_signature IS DISTINCT FROM v_session_row.session_signature THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_MISMATCH',
              'payload_session_signature', v_payload_session_signature,
              'current_session_signature', v_session_row.session_signature
            )::text;
  END IF;

  v_payload_refresh_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(
    v_payload_json->>'refresh_scope_kind',
    v_payload_json#>>'{source_build,refresh_scope_kind}',
    v_payload_json#>>'{preview_decisions_json,refresh_scope_kind}',
    ''
  ))), '');

  IF v_payload_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_payload_refresh_scope_kind := NULL::text;
  END IF;

  v_cursor_requested_refresh_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(
    v_cursor_json->>'requested_refresh_scope_kind',
    ''
  ))), '');

  IF v_cursor_requested_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_cursor_requested_refresh_scope_kind := NULL::text;
  END IF;

  v_source_build_cursor_continuation := (
    (
      LOWER(BTRIM(COALESCE(v_payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(
        v_payload_json->>'reason',
        v_payload_json->>'continuation_reason',
        v_payload_json#>>'{source_build,reason}',
        ''
      ))) = 'SOURCE_BUILD_CURSOR_CONTINUATION'
    )
    AND (
      NULLIF(BTRIM(COALESCE(v_cursor_json->>'last_timesheet_id', '')), '') IS NOT NULL
      OR NULLIF(BTRIM(COALESCE(v_cursor_json->>'last_source_ordinal', '')), '') IS NOT NULL
    )
  );

  v_refresh_scope_kind := COALESCE(
    CASE
      WHEN COALESCE(v_source_build_cursor_continuation, false)
        THEN v_cursor_requested_refresh_scope_kind
      ELSE NULL::text
    END,
    v_payload_refresh_scope_kind,
    v_cursor_requested_refresh_scope_kind,
    'CANDIDATE_FULL_LIVE'
  );

  IF v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
  END IF;
  v_requested_refresh_scope_kind := v_refresh_scope_kind;

  v_pay_channel_scope := NULLIF(UPPER(BTRIM(COALESCE(
    v_payload_json->>'pay_channel_scope',
    v_payload_json#>>'{source_build,pay_channel_scope}',
    v_payload_json#>>'{preview_decisions_json,pay_channel_scope}',
    v_session_row.filters_json->>'pay_channel_scope',
    v_session_row.filters_json#>>'{filters,pay_channel_scope}',
    'ALL'
  ))), '');

  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    v_pay_channel_scope := 'ALL';
  END IF;

  SELECT CASE
           WHEN UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))) IN ('PAYE', 'UMBRELLA')
             THEN UPPER(BTRIM(candidate_row.pay_method))
           ELSE 'ALL'
         END
  INTO v_candidate_pay_channel_scope
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  LIMIT 1;

  SELECT COALESCE(jsonb_agg(parsed_target_ids.timesheet_id_text ORDER BY parsed_target_ids.timesheet_id_text), '[]'::jsonb)
  INTO v_targeted_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(targeted_values.value), '') AS timesheet_id_text
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'array' THEN v_payload_json->'targeted_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'targeted_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,targeted_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,targeted_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS targeted_values(value)
    WHERE NULLIF(BTRIM(targeted_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_target_ids;

  SELECT COALESCE(jsonb_agg(parsed_linked_ids.timesheet_id_text ORDER BY parsed_linked_ids.timesheet_id_text), '[]'::jsonb)
  INTO v_linked_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(linked_values.value), '') AS timesheet_id_text
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'array' THEN v_payload_json->'linked_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'linked_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,linked_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,linked_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS linked_values(value)
    WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_linked_ids;

  v_targeted_payload_received := jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
    OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0;

  IF v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND COALESCE(v_targeted_payload_received, false) IS NOT TRUE
     AND LOWER(BTRIM(COALESCE(v_payload_json->>'source_build_allow_full_fallback', v_payload_json#>>'{source_build,allow_full_fallback}', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_TARGETED_IDS_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_TARGETED_IDS_REQUIRED',
              'refresh_scope_kind', v_refresh_scope_kind,
              'message', 'TARGETED_TIMESHEETS source-build requires targeted_timesheet_ids or linked_timesheet_ids unless explicit full fallback is allowed.'
            )::text;
  END IF;

  IF v_cursor_json->>'last_timesheet_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_cursor_last_timesheet_id := (v_cursor_json->>'last_timesheet_id')::uuid;
  END IF;

  IF COALESCE(v_cursor_json->>'last_source_ordinal', '') ~ '^[0-9]{1,18}$' THEN
    v_cursor_source_ordinal_base := (v_cursor_json->>'last_source_ordinal')::bigint;
  ELSE
    v_cursor_source_ordinal_base := 0;
  END IF;

  v_first_source_page := v_cursor_last_timesheet_id IS NULL AND COALESCE(v_cursor_source_ordinal_base, 0) <= 0;

  SELECT COUNT(*)::integer
  INTO v_existing_source_row_count
  FROM public.banking_pay_workbench_candidate_source_lines AS existing_source_rows
  WHERE existing_source_rows.session_id = p_session_id
    AND existing_source_rows.candidate_id = p_candidate_id
    AND existing_source_rows.status = 'CURRENT';

  v_old_sync_marker := CASE
    WHEN jsonb_typeof(v_session_row.progress_json #> ARRAY['overpayment_sync_completed_by_candidate', p_candidate_id::text]) = 'object'
      THEN COALESCE(v_session_row.progress_json #> ARRAY['overpayment_sync_completed_by_candidate', p_candidate_id::text], '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  /* A historic bare completion marker is diagnostic only.  Reconciliation authority
     is re-established from the current session/run/scope/economic state below. */
  v_sync_completed := false;
  v_sync_attested := false;
  v_sync_marker_reused := false;

  SELECT COALESCE(ARRAY_AGG(parsed_target_ids.timesheet_id ORDER BY parsed_target_ids.timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT DISTINCT targeted_id.value::uuid AS timesheet_id
    FROM jsonb_array_elements_text(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) AS targeted_id(value)
    WHERE targeted_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_target_ids;

  SELECT COALESCE(ARRAY_AGG(parsed_linked_ids.timesheet_id ORDER BY parsed_linked_ids.timesheet_id), ARRAY[]::uuid[])
  INTO v_linked_timesheet_ids
  FROM (
    SELECT DISTINCT linked_id.value::uuid AS timesheet_id
    FROM jsonb_array_elements_text(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) AS linked_id(value)
    WHERE linked_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_linked_ids;

  /* A targeted refresh of one import-authoritative correction leg must rebuild
     the whole correction chain.  The residual materialiser deliberately emits
     one net PRE_DRAFT_LIVE_TRUTH row per economic component and therefore needs
     a source carrier from the root or another member for every component.  A
     single-leg scope can never satisfy that invariant reliably. */
  IF v_requested_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND EXISTS (
       SELECT 1
       FROM unnest(
         COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])
         || COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])
       ) AS requested_correction_scope(timesheet_id)
       CROSS JOIN LATERAL (
         SELECT public.timesheet_correction_chain_scope_v1(
           requested_correction_scope.timesheet_id, false, 32, 100
         ) AS chain_json
       ) AS requested_chain
       WHERE COALESCE((requested_chain.chain_json->>'valid')::boolean, false)
         AND EXISTS (
           SELECT 1
           FROM jsonb_array_elements_text(
             COALESCE(requested_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
           ) AS chain_member(value)
           WHERE chain_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             AND COALESCE((
               public._ctms_import_correction_classify_v1(chain_member.value::uuid)
                 ->>'is_import_authoritative_correction'
             )::boolean, false)
         )
     ) THEN
    IF EXISTS (
      SELECT 1
      FROM unnest(
        COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])
        || COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])
      ) AS requested_correction_scope(timesheet_id)
      CROSS JOIN LATERAL (
        SELECT public.timesheet_correction_chain_scope_v1(
          requested_correction_scope.timesheet_id, false, 32, 100
        ) AS chain_json
      ) AS requested_chain
      WHERE COALESCE((public._ctms_import_correction_classify_v1(requested_correction_scope.timesheet_id)
          ->>'is_import_authoritative_correction')::boolean, false)
        AND COALESCE((requested_chain.chain_json->>'valid')::boolean, false) IS NOT TRUE
    ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CORRECTION_CHAIN_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CORRECTION_CHAIN_INVALID',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )::text;
    END IF;

    WITH requested_scope AS (
      SELECT DISTINCT requested_id AS timesheet_id
      FROM unnest(
        COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])
        || COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])
      ) AS requested_values(requested_id)
      WHERE requested_id IS NOT NULL
    ), correction_chains AS (
      SELECT requested_chain.chain_json
      FROM requested_scope
      CROSS JOIN LATERAL (
        SELECT public.timesheet_correction_chain_scope_v1(
          requested_scope.timesheet_id, false, 32, 100
        ) AS chain_json
      ) AS requested_chain
      WHERE COALESCE((requested_chain.chain_json->>'valid')::boolean, false)
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(
            COALESCE(requested_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
          ) AS chain_member(value)
          WHERE chain_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND COALESCE((
              public._ctms_import_correction_classify_v1(chain_member.value::uuid)
                ->>'is_import_authoritative_correction'
            )::boolean, false)
        )
    ), expanded_member_ids AS (
      SELECT DISTINCT member_id.value::uuid AS timesheet_id
      FROM correction_chains
      CROSS JOIN LATERAL jsonb_array_elements_text(
        COALESCE(correction_chains.chain_json->'member_timesheet_ids', '[]'::jsonb)
      ) AS member_id(value)
      WHERE member_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), expanded_target_scope AS (
      SELECT targeted_id AS timesheet_id
      FROM unnest(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])) AS targeted_values(targeted_id)
      UNION
      SELECT expanded_member_ids.timesheet_id
      FROM expanded_member_ids
    )
    SELECT COALESCE(ARRAY_AGG(expanded_target_scope.timesheet_id ORDER BY expanded_target_scope.timesheet_id), ARRAY[]::uuid[])
    INTO v_targeted_timesheet_ids
    FROM expanded_target_scope;

    -- Linked scope carries resolution context but does not guarantee that a
    -- payable source row is materialised. Promote every correction member to
    -- the actual targeted set and retain only genuinely additional linked
    -- identifiers here.
    SELECT COALESCE(ARRAY_AGG(linked_id ORDER BY linked_id), ARRAY[]::uuid[])
    INTO v_linked_timesheet_ids
    FROM unnest(COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])) AS linked_values(linked_id)
    WHERE NOT (linked_id = ANY(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])));

    v_targeted_timesheet_ids_json := to_jsonb(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[]));
    v_linked_timesheet_ids_json := to_jsonb(COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[]));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM unnest(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[]) || COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])) AS requested_scope(timesheet_id)
    WHERE requested_scope.timesheet_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS requested_tsfin
        WHERE requested_tsfin.timesheet_id = requested_scope.timesheet_id
          AND requested_tsfin.candidate_id = p_candidate_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_advances AS requested_case
        WHERE requested_case.linked_timesheet_id = requested_scope.timesheet_id
          AND requested_case.candidate_id = p_candidate_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_finance_case_components AS requested_component
        WHERE requested_component.linked_timesheet_id = requested_scope.timesheet_id
          AND requested_component.candidate_id = p_candidate_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.timesheet_payment_overrides AS requested_override
        WHERE requested_override.timesheet_id = requested_scope.timesheet_id
          AND requested_override.candidate_id = p_candidate_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.ts_pay_adjustments AS requested_adjustment
        WHERE requested_adjustment.timesheet_id = requested_scope.timesheet_id
          AND requested_adjustment.candidate_id = p_candidate_id
      )
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SCOPE_CANDIDATE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SCOPE_CANDIDATE_MISMATCH',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
              'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)
            )::text;
  END IF;

  v_filter_client_id_text := NULLIF(BTRIM(COALESCE(
    v_session_row.filters_json->>'client_id',
    v_session_row.filters_json->>'client_filter_single',
    v_session_row.filters_json#>>'{filters,client_id}',
    v_session_row.filters_json#>>'{client,id}',
    ''
  )), '');

  IF v_filter_client_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_client_filter_single := v_filter_client_id_text::uuid;
  ELSE
    v_client_filter_single := NULL::uuid;
  END IF;

  SELECT COALESCE(ARRAY_AGG(parsed_force_timesheet_id.timesheet_id ORDER BY parsed_force_timesheet_id.timesheet_id), ARRAY[]::uuid[])
  INTO v_force_include_timesheet_ids
  FROM (
    SELECT DISTINCT force_timesheet_id.value::uuid AS timesheet_id
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_session_row.filters_json->'force_include_timesheet_ids') = 'array' THEN v_session_row.filters_json->'force_include_timesheet_ids'
        WHEN jsonb_typeof(v_session_row.filters_json->'forced_timesheet_ids') = 'array' THEN v_session_row.filters_json->'forced_timesheet_ids'
        WHEN jsonb_typeof(v_session_row.filters_json#>'{scope,force_include_timesheet_ids}') = 'array' THEN v_session_row.filters_json#>'{scope,force_include_timesheet_ids}'
        ELSE '[]'::jsonb
      END
    ) AS force_timesheet_id(value)
    WHERE force_timesheet_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS force_tsfin
        WHERE force_tsfin.timesheet_id = force_timesheet_id.value::uuid
          AND force_tsfin.candidate_id = p_candidate_id
      )
  ) AS parsed_force_timesheet_id;

  SELECT COALESCE(ARRAY_AGG(parsed_exclude_timesheet_id.timesheet_id ORDER BY parsed_exclude_timesheet_id.timesheet_id), ARRAY[]::uuid[])
  INTO v_exclude_timesheet_ids
  FROM (
    SELECT DISTINCT exclude_timesheet_id.value::uuid AS timesheet_id
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_session_row.filters_json->'exclude_timesheet_ids') = 'array' THEN v_session_row.filters_json->'exclude_timesheet_ids'
        WHEN jsonb_typeof(v_session_row.filters_json#>'{scope,exclude_timesheet_ids}') = 'array' THEN v_session_row.filters_json#>'{scope,exclude_timesheet_ids}'
        ELSE '[]'::jsonb
      END
    ) AS exclude_timesheet_id(value)
    WHERE exclude_timesheet_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS exclude_tsfin
        WHERE exclude_tsfin.timesheet_id = exclude_timesheet_id.value::uuid
          AND exclude_tsfin.candidate_id = p_candidate_id
      )
  ) AS parsed_exclude_timesheet_id;

  v_mismatch_choices_json := CASE
    WHEN jsonb_typeof(v_session_row.filters_json->'mismatch_choices') = 'object' THEN COALESCE(v_session_row.filters_json->'mismatch_choices', '{}'::jsonb)
    WHEN jsonb_typeof(v_session_row.filters_json#>'{mismatch,choices}') = 'object' THEN COALESCE(v_session_row.filters_json#>'{mismatch,choices}', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_current_source_change_seq
  FROM (SELECT 1) AS anchor
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_initial_source_change_seq := COALESCE(v_current_source_change_seq, 0);

  IF COALESCE(v_source_change_seq_was_supplied, false) IS TRUE
     AND COALESCE(v_current_source_change_seq, 0) IS DISTINCT FROM COALESCE(v_source_change_seq, 0) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_STALE',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'payload_source_change_seq', COALESCE(v_source_change_seq, 0),
              'current_source_change_seq', COALESCE(v_current_source_change_seq, 0)
            )::text;
  ELSIF COALESCE(v_source_change_seq_was_supplied, false) IS NOT TRUE THEN
    /* Older valid callers may omit the sequence.  Bind the run to the current
       candidate authority before any economic work rather than defaulting to 0. */
    v_source_change_seq := COALESCE(v_current_source_change_seq, 0);
  END IF;

  WITH seed_timesheet_ids AS (
    SELECT targeted_id AS timesheet_id
    FROM unnest(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])) AS targeted_values(targeted_id)
    WHERE v_requested_refresh_scope_kind = 'TARGETED_TIMESHEETS'

    UNION

    SELECT linked_id AS timesheet_id
    FROM unnest(COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])) AS linked_values(linked_id)
    WHERE v_requested_refresh_scope_kind = 'TARGETED_TIMESHEETS'

    UNION

    SELECT force_id AS timesheet_id
    FROM unnest(COALESCE(v_force_include_timesheet_ids, ARRAY[]::uuid[])) AS force_values(force_id)

    UNION

    SELECT current_tsfin.timesheet_id
    FROM public.timesheets_financials AS current_tsfin
    JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = current_tsfin.timesheet_id
     AND current_timesheet.is_current = true
     AND current_timesheet.archived_at_utc IS NULL
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND current_tsfin.candidate_id = p_candidate_id
      AND current_tsfin.is_current = true

    UNION

    SELECT finance_case.linked_timesheet_id
    FROM public.pay_advances AS finance_case
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND finance_case.candidate_id = p_candidate_id
      AND finance_case.linked_timesheet_id IS NOT NULL

    UNION

    SELECT finance_component.linked_timesheet_id
    FROM public.pay_finance_case_components AS finance_component
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND finance_component.candidate_id = p_candidate_id
      AND finance_component.linked_timesheet_id IS NOT NULL

    UNION

    SELECT payment_override.timesheet_id
    FROM public.timesheet_payment_overrides AS payment_override
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND payment_override.candidate_id = p_candidate_id
      AND payment_override.consumed_at_utc IS NULL
      AND payment_override.cleared_at_utc IS NULL

    UNION

    SELECT unpaid_adjustment.timesheet_id
    FROM public.ts_pay_adjustments AS unpaid_adjustment
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND unpaid_adjustment.candidate_id = p_candidate_id
      AND unpaid_adjustment.paid_at_utc IS NULL

    UNION

    SELECT active_snooze.timesheet_id
    FROM public.pay_item_snoozes AS active_snooze
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND active_snooze.candidate_id = p_candidate_id
      AND active_snooze.timesheet_id IS NOT NULL
      AND active_snooze.cleared_at_utc IS NULL
      AND active_snooze.cancelled_at_utc IS NULL

    UNION

    SELECT retained_batch_item.timesheet_id
    FROM public.pay_batch_candidates AS retained_batch_candidate
    JOIN public.pay_batch_items AS retained_batch_item
      ON retained_batch_item.pay_batch_candidate_id = retained_batch_candidate.id
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND retained_batch_candidate.candidate_id = p_candidate_id
      AND retained_batch_item.timesheet_id IS NOT NULL
      AND COALESCE(retained_batch_item.is_voided, false) IS NOT TRUE

    UNION

    SELECT retained_correction.timesheet_id
    FROM public.pay_payment_correction_items AS retained_correction
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND retained_correction.candidate_id = p_candidate_id
      AND retained_correction.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(retained_correction.status, ''))) = 'APPLIED'
  ), seed_array AS (
    SELECT COALESCE(ARRAY_AGG(DISTINCT seed_timesheet_ids.timesheet_id ORDER BY seed_timesheet_ids.timesheet_id), ARRAY[]::uuid[]) AS timesheet_ids
    FROM seed_timesheet_ids
    WHERE seed_timesheet_ids.timesheet_id IS NOT NULL
  ), expanded_timesheet_ids AS (
    SELECT seed_timesheet_ids.timesheet_id
    FROM seed_timesheet_ids
    WHERE seed_timesheet_ids.timesheet_id IS NOT NULL

    UNION

    /* Full-live seeds normally contain only current/frozen carriers.  Import-
       authoritative correction residuals, however, are chain economic truth:
       an archived reversal or replacement member can still carry the positive
       side required to net a current negative recovery.  Promote every valid
       chain member before source collection so full-live and targeted refreshes
       materialise the same complete component family. */
    SELECT correction_member.value::uuid
    FROM seed_timesheet_ids AS correction_seed
    JOIN public.timesheets AS correction_seed_timesheet
      ON correction_seed_timesheet.timesheet_id = correction_seed.timesheet_id
    CROSS JOIN LATERAL (
      SELECT public.timesheet_correction_chain_scope_v1(
        correction_seed.timesheet_id, false, 32, 100
      ) AS chain_json
    ) AS correction_chain
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(correction_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
    ) AS correction_member(value)
    WHERE correction_seed.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS correction_seed_financials
        WHERE correction_seed_financials.timesheet_id = correction_seed.timesheet_id
          AND correction_seed_financials.candidate_id = p_candidate_id
      )
      AND COALESCE((correction_chain.chain_json->>'valid')::boolean, false)
      AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          COALESCE(correction_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
        ) AS classified_member(value)
        WHERE classified_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND COALESCE((public._ctms_import_correction_classify_v1(classified_member.value::uuid)
            ->>'is_import_authoritative_correction')::boolean, false)
      )
      AND correction_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION

    SELECT rotation_scope.canonical_timesheet_id
    FROM seed_array
    JOIN public._pay_timesheet_rotation_scope(seed_array.timesheet_ids) AS rotation_scope ON true
    WHERE rotation_scope.canonical_timesheet_id IS NOT NULL

    UNION

    SELECT rotation_scope.family_timesheet_id
    FROM seed_array
    JOIN public._pay_timesheet_rotation_scope(seed_array.timesheet_ids) AS rotation_scope ON true
    WHERE rotation_scope.family_timesheet_id IS NOT NULL
  )
  SELECT COALESCE(ARRAY_AGG(DISTINCT expanded_timesheet_ids.timesheet_id ORDER BY expanded_timesheet_ids.timesheet_id), ARRAY[]::uuid[])
  INTO v_sync_scope_timesheet_ids
  FROM expanded_timesheet_ids
  WHERE expanded_timesheet_ids.timesheet_id IS NOT NULL
    AND NOT (expanded_timesheet_ids.timesheet_id = ANY(COALESCE(v_exclude_timesheet_ids, ARRAY[]::uuid[])))
    AND (
      v_client_filter_single IS NULL
      OR EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS scoped_tsfin
        WHERE scoped_tsfin.timesheet_id = expanded_timesheet_ids.timesheet_id
          AND scoped_tsfin.candidate_id = p_candidate_id
          AND scoped_tsfin.client_id = v_client_filter_single
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_advances AS scoped_finance_case
        WHERE scoped_finance_case.linked_timesheet_id = expanded_timesheet_ids.timesheet_id
          AND scoped_finance_case.candidate_id = p_candidate_id
          AND scoped_finance_case.client_id = v_client_filter_single
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_finance_case_components AS scoped_finance_component
        WHERE scoped_finance_component.linked_timesheet_id = expanded_timesheet_ids.timesheet_id
          AND scoped_finance_component.candidate_id = p_candidate_id
          AND scoped_finance_component.client_id = v_client_filter_single
      )
    );

  SELECT COALESCE(jsonb_agg(scope_id::text ORDER BY scope_id), '[]'::jsonb)
  INTO v_sync_scope_timesheet_ids_json
  FROM unnest(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS sync_scope(scope_id);

  v_sync_scope_digest := md5(
    COALESCE(v_sync_scope_timesheet_ids_json::text, '[]') || '|' ||
    COALESCE(v_requested_refresh_scope_kind, 'CANDIDATE_FULL_LIVE') || '|' ||
    COALESCE(v_candidate_pay_channel_scope, '') || '|' ||
    COALESCE(v_client_filter_single::text, '')
  );

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_sync_rotation_scope;
  CREATE TEMPORARY TABLE pg_temp._tmp_pay_wb_sync_rotation_scope ON COMMIT DROP AS
  SELECT DISTINCT
    rotation_scope.requested_timesheet_id,
    rotation_scope.canonical_timesheet_id,
    rotation_scope.family_timesheet_id
  FROM public._pay_timesheet_rotation_scope(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS rotation_scope;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_sync_negative_components;
  CREATE TEMPORARY TABLE pg_temp._tmp_pay_wb_sync_negative_components ON COMMIT DROP AS
  WITH live_entitlement_components AS (
    SELECT
      entitlement_component.timesheet_id,
      UPPER(BTRIM(entitlement_component.key_type)) AS key_type,
      BTRIM(entitlement_component.key_value) AS key_value,
      ROUND(COALESCE(entitlement_component.truth_ex_vat, 0), 2) AS truth_ex_vat,
      ROUND(COALESCE(entitlement_component.baseline_ex_vat, 0), 2) AS baseline_ex_vat
    FROM public._pay_current_timesheet_entitlement_components(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS entitlement_component
    WHERE NULLIF(BTRIM(COALESCE(entitlement_component.key_type, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(entitlement_component.key_value, '')), '') IS NOT NULL
  ), active_entitlement_reservations AS (
    SELECT
      reserved_component.timesheet_id,
      UPPER(BTRIM(reserved_component.key_type)) AS key_type,
      BTRIM(reserved_component.key_value) AS key_value,
      ROUND(COALESCE(reserved_component.amount_ex_vat, 0), 2) AS reserved_ex_vat
    FROM public._pay_reserved_components(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[]), NULL::uuid) AS reserved_component
    WHERE NULLIF(BTRIM(COALESCE(reserved_component.key_type, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(reserved_component.key_value, '')), '') IS NOT NULL
  ), live_economic_keys AS (
    SELECT
      live_component.timesheet_id,
      live_component.key_type,
      live_component.key_value
    FROM live_entitlement_components AS live_component

    UNION

    SELECT
      reserved_component.timesheet_id,
      reserved_component.key_type,
      reserved_component.key_value
    FROM active_entitlement_reservations AS reserved_component
  ), raw_outstanding_components AS (
    SELECT
      live_key.timesheet_id,
      live_key.key_type,
      live_key.key_value,
      ROUND(COALESCE(live_component.truth_ex_vat, 0), 2) AS truth_ex_vat,
      ROUND(COALESCE(live_component.baseline_ex_vat, 0), 2) AS baseline_ex_vat,
      ROUND(COALESCE(reserved_component.reserved_ex_vat, 0), 2) AS reserved_ex_vat,
      ROUND(
        COALESCE(live_component.truth_ex_vat, 0)
        - COALESCE(live_component.baseline_ex_vat, 0)
        - COALESCE(reserved_component.reserved_ex_vat, 0),
        2
      ) AS outstanding_ex_vat
    FROM live_economic_keys AS live_key
    LEFT JOIN live_entitlement_components AS live_component
      ON live_component.timesheet_id = live_key.timesheet_id
     AND live_component.key_type = live_key.key_type
     AND live_component.key_value = live_key.key_value
    LEFT JOIN active_entitlement_reservations AS reserved_component
      ON reserved_component.timesheet_id = live_key.timesheet_id
     AND reserved_component.key_type = live_key.key_type
     AND reserved_component.key_value = live_key.key_value
  )
  SELECT
    raw_outstanding_component.timesheet_id,
    raw_outstanding_component.key_type,
    raw_outstanding_component.key_value,
    raw_outstanding_component.truth_ex_vat,
    raw_outstanding_component.baseline_ex_vat,
    raw_outstanding_component.reserved_ex_vat,
    raw_outstanding_component.outstanding_ex_vat,
    CASE
      WHEN COALESCE(active_settled_basis.active_settled_component_count, 0) > 0
        THEN active_settled_basis.active_settled_signature
      ELSE COALESCE(
        timesheet_pay_state.last_settled_signature,
        md5(COALESCE(timesheet_pay_state.last_settled_snapshot_json::text, '{}'))
      )
    END AS baseline_signature
  FROM raw_outstanding_components AS raw_outstanding_component
  LEFT JOIN public.timesheet_pay_state AS timesheet_pay_state
    ON timesheet_pay_state.timesheet_id = raw_outstanding_component.timesheet_id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*)::integer AS active_settled_component_count,
      md5(COALESCE(jsonb_agg(
        jsonb_build_object(
          'key_type', active_settled_component.key_type,
          'key_value', active_settled_component.key_value,
          'amount_ex_vat', ROUND(COALESCE(active_settled_component.amount_ex_vat, 0), 2),
          'amount_inc_vat', ROUND(COALESCE(active_settled_component.amount_inc_vat, 0), 2)
        ) ORDER BY active_settled_component.key_type, active_settled_component.key_value
      )::text, '[]')) AS active_settled_signature
    FROM public._pay_active_settled_components(ARRAY[raw_outstanding_component.timesheet_id]::uuid[]) AS active_settled_component
  ) AS active_settled_basis ON true
  WHERE raw_outstanding_component.outstanding_ex_vat < 0;

  /*
   * Capture durable correction-resolution membership before the source-row
   * rewrite below. The rewrite intentionally replaces the candidate's
   * correction source rows; asking the residual helper afterwards creates a
   * short-lived blind spot on continuation pages and can misclassify an
   * already-pending coupled component as unattested.
   */
  SELECT COALESCE(
    ARRAY_AGG(DISTINCT pending_member.value::uuid ORDER BY pending_member.value::uuid),
    ARRAY[]::uuid[]
  )
  INTO v_current_resolution_pending_member_ids
  FROM jsonb_array_elements(
    public._ctms_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      NULL::uuid,
      'PAY_WORKBENCH_SOURCE_BUILD_ATTESTATION'
    )
  ) AS pending_residual(value)
  CROSS JOIN LATERAL jsonb_array_elements_text(
    COALESCE(pending_residual.value->'member_timesheet_ids', '[]'::jsonb)
  ) AS pending_member(value)
  WHERE COALESCE((pending_residual.value->>'draftable')::boolean, false) IS NOT TRUE
    AND COALESCE(pending_residual.value->>'block_code', '')
          = 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
    AND COALESCE((pending_residual.value->>'unresolved_count')::integer, 0) > 0
    AND COALESCE((pending_residual.value->>'reservation_overrun_count')::integer, 0) = 0
    AND COALESCE((pending_residual.value->>'component_count')::integer, 0) > 0
    AND pending_member.value
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  PERFORM public._ctms_rewrite_source_build_correction_negative_components_v1(
    p_session_id,
    p_candidate_id,
    COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])
  );

  SELECT COUNT(*)::integer,
         md5(COALESCE(jsonb_agg(
           jsonb_build_object(
             'timesheet_id', negative_component.timesheet_id::text,
             'key_type', negative_component.key_type,
             'key_value', negative_component.key_value,
             'truth_ex_vat', ROUND(negative_component.truth_ex_vat, 2)::numeric(12,2),
             'baseline_ex_vat', ROUND(negative_component.baseline_ex_vat, 2)::numeric(12,2),
             'reserved_ex_vat', ROUND(negative_component.reserved_ex_vat, 2)::numeric(12,2),
             'outstanding_ex_vat', ROUND(negative_component.outstanding_ex_vat, 2)::numeric(12,2)
           ) ORDER BY negative_component.timesheet_id, negative_component.key_type, negative_component.key_value
         )::text, '[]'))
  INTO v_sync_negative_component_count,
       v_sync_negative_digest
  FROM pg_temp._tmp_pay_wb_sync_negative_components AS negative_component;

  SELECT md5(COALESCE(jsonb_agg(
           jsonb_build_object(
             'timesheet_id', settled_component.timesheet_id::text,
             'key_type', settled_component.key_type,
             'key_value', settled_component.key_value,
             'amount_ex_vat', ROUND(COALESCE(settled_component.amount_ex_vat, 0), 2),
             'amount_inc_vat', ROUND(COALESCE(settled_component.amount_inc_vat, 0), 2)
           ) ORDER BY settled_component.timesheet_id, settled_component.key_type, settled_component.key_value
         )::text, '[]'))
  INTO v_sync_baseline_digest
  FROM public._pay_active_settled_components(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS settled_component;

  v_initial_validation_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF v_candidate_pay_channel_scope IN ('PAYE', 'UMBRELLA')
     AND COALESCE(v_first_source_page, true)
     AND COALESCE(array_length(v_sync_scope_timesheet_ids, 1), 0) > 0 THEN
    v_sync_started_at_utc := clock_timestamp();
    v_sync_invoked := true;
    v_sync_authority_token := md5(
      txid_current()::text || '|' ||
      p_session_id::text || '|' ||
      p_candidate_id::text || '|' ||
      v_source_build_run_id::text || '|' ||
      clock_timestamp()::text || '|' ||
      random()::text
    );
    PERFORM set_config(
      'cloudtms.pay_workbench_overpayment_sync_token',
      v_sync_authority_token,
      true
    );

    v_sync_result := public.pay_sync_overpayments_from_preview(
      p_pay_date => v_session_row.pay_date,
      p_week_ending_cutoff => v_session_row.week_ending_cutoff,
      p_actor_user_id => v_session_row.actor_user_id,
      p_pay_channel_scope => v_candidate_pay_channel_scope,
      p_candidate_ids => ARRAY[p_candidate_id]::uuid[],
      p_mismatch_choices => COALESCE(v_mismatch_choices_json, '{}'::jsonb)
        || jsonb_build_object(
          'overpayment_sync_authoritative_timesheet_scope', true,
          'authoritative_timesheet_scope', true,
          'overpayment_sync_authority_token', v_sync_authority_token,
          'refresh_scope_kind', 'TARGETED_TIMESHEETS',
          'targeted_timesheet_ids', COALESCE(v_sync_scope_timesheet_ids_json, '[]'::jsonb),
          'targeted_timesheet_ids_requested', COALESCE(v_sync_scope_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', '[]'::jsonb,
          'linked_timesheet_ids_requested', '[]'::jsonb,
          'source_build_allow_full_fallback', false
        )
        || jsonb_build_object(
          'source_build_run_id', v_source_build_run_id::text,
          'source_change_seq', v_source_change_seq,
          'workbench_session_id', p_session_id::text,
          'session_version', v_session_version,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
        || jsonb_build_object(
          'overpayment_sync_scope_digest', v_sync_scope_digest,
          'overpayment_sync_negative_component_digest', v_sync_negative_digest,
          'overpayment_sync_settled_baseline_digest', v_sync_baseline_digest
        ),
      p_client_filter_single => v_client_filter_single,
      p_force_include_timesheet_ids => COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[]),
      p_exclude_timesheet_ids => COALESCE(v_exclude_timesheet_ids, ARRAY[]::uuid[])
    );

    PERFORM set_config(
      'cloudtms.pay_workbench_overpayment_sync_token',
      '',
      true
    );

    IF jsonb_typeof(COALESCE(v_sync_result, '{}'::jsonb)) <> 'object'
       OR LOWER(BTRIM(COALESCE(v_sync_result->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR LOWER(BTRIM(COALESCE(v_sync_result->>'preview_candidate_coverage_complete', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR LOWER(BTRIM(COALESCE(v_sync_result->>'authoritative_timesheet_scope', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR (
            CASE
              WHEN jsonb_typeof(v_sync_result->'scope_timesheet_ids') = 'array'
                THEN COALESCE(v_sync_result->'scope_timesheet_ids', '[]'::jsonb)
              ELSE '[]'::jsonb
            END
          ) IS DISTINCT FROM COALESCE(v_sync_scope_timesheet_ids_json, '[]'::jsonb) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_FAILED',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'source_build_run_id', v_source_build_run_id::text,
                'sync_result', COALESCE(v_sync_result, '{}'::jsonb)
              )::text;
    END IF;
  ELSIF v_candidate_pay_channel_scope IN ('PAYE', 'UMBRELLA')
        AND COALESCE(v_first_source_page, true) THEN
    v_sync_result := jsonb_build_object(
      'ok', true,
      'invoked', false,
      'reason', 'AUTHORITATIVE_EMPTY_TIMESHEET_SCOPE',
      'pay_channel_scope', v_candidate_pay_channel_scope,
      'authoritative_timesheet_scope', true,
      'explicit_empty_timesheet_scope', true,
      'scope_timesheet_ids', '[]'::jsonb,
      'preview_candidate_coverage_complete', true
    );
  ELSIF v_candidate_pay_channel_scope IN ('PAYE', 'UMBRELLA') THEN
    v_sync_result := jsonb_build_object(
      'ok', true,
      'invoked', false,
      'reason', 'SOURCE_BUILD_CONTINUATION_DURABLE_REVALIDATION',
      'pay_channel_scope', v_candidate_pay_channel_scope,
      'authoritative_timesheet_scope', true,
      'scope_timesheet_ids', COALESCE(v_sync_scope_timesheet_ids_json, '[]'::jsonb),
      'preview_candidate_coverage_complete', true
    );
  ELSE
    v_sync_result := jsonb_build_object(
      'ok', true,
      'invoked', false,
      'reason', 'PAY_CHANNEL_NOT_RECONCILABLE',
      'pay_channel_scope', v_candidate_pay_channel_scope
    );
  END IF;

  /* Reconciliation writes finance authority and its existing dirty triggers may
     legitimately advance the candidate sequence in this same transaction.  Rebuild
     the current scope/economic fingerprint after sync.  Adopt the new sequence only
     when route, scope, negative economics and settled baseline are unchanged. */
  SELECT CASE
           WHEN UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))) IN ('PAYE', 'UMBRELLA')
             THEN UPPER(BTRIM(candidate_row.pay_method))
           ELSE 'ALL'
         END
  INTO v_post_sync_candidate_pay_channel_scope
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_FOUND_POST_SYNC'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANDIDATE_NOT_FOUND_POST_SYNC',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  WITH post_seed_timesheet_ids AS (
    SELECT targeted_id AS timesheet_id
    FROM unnest(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])) AS targeted_values(targeted_id)
    WHERE v_requested_refresh_scope_kind = 'TARGETED_TIMESHEETS'

    UNION

    SELECT linked_id AS timesheet_id
    FROM unnest(COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])) AS linked_values(linked_id)
    WHERE v_requested_refresh_scope_kind = 'TARGETED_TIMESHEETS'

    UNION

    SELECT force_id AS timesheet_id
    FROM unnest(COALESCE(v_force_include_timesheet_ids, ARRAY[]::uuid[])) AS force_values(force_id)

    UNION

    SELECT current_tsfin.timesheet_id
    FROM public.timesheets_financials AS current_tsfin
    JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = current_tsfin.timesheet_id
     AND current_timesheet.is_current = true
     AND current_timesheet.archived_at_utc IS NULL
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND current_tsfin.candidate_id = p_candidate_id
      AND current_tsfin.is_current = true

    UNION

    SELECT finance_case.linked_timesheet_id
    FROM public.pay_advances AS finance_case
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND finance_case.candidate_id = p_candidate_id
      AND finance_case.linked_timesheet_id IS NOT NULL

    UNION

    SELECT finance_component.linked_timesheet_id
    FROM public.pay_finance_case_components AS finance_component
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND finance_component.candidate_id = p_candidate_id
      AND finance_component.linked_timesheet_id IS NOT NULL

    UNION

    SELECT payment_override.timesheet_id
    FROM public.timesheet_payment_overrides AS payment_override
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND payment_override.candidate_id = p_candidate_id
      AND payment_override.consumed_at_utc IS NULL
      AND payment_override.cleared_at_utc IS NULL

    UNION

    SELECT unpaid_adjustment.timesheet_id
    FROM public.ts_pay_adjustments AS unpaid_adjustment
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND unpaid_adjustment.candidate_id = p_candidate_id
      AND unpaid_adjustment.paid_at_utc IS NULL

    UNION

    SELECT active_snooze.timesheet_id
    FROM public.pay_item_snoozes AS active_snooze
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND active_snooze.candidate_id = p_candidate_id
      AND active_snooze.timesheet_id IS NOT NULL
      AND active_snooze.cleared_at_utc IS NULL
      AND active_snooze.cancelled_at_utc IS NULL

    UNION

    SELECT retained_batch_item.timesheet_id
    FROM public.pay_batch_candidates AS retained_batch_candidate
    JOIN public.pay_batch_items AS retained_batch_item
      ON retained_batch_item.pay_batch_candidate_id = retained_batch_candidate.id
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND retained_batch_candidate.candidate_id = p_candidate_id
      AND retained_batch_item.timesheet_id IS NOT NULL
      AND COALESCE(retained_batch_item.is_voided, false) IS NOT TRUE

    UNION

    SELECT retained_correction.timesheet_id
    FROM public.pay_payment_correction_items AS retained_correction
    WHERE v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
      AND retained_correction.candidate_id = p_candidate_id
      AND retained_correction.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(retained_correction.status, ''))) = 'APPLIED'
  ), post_seed_array AS (
    SELECT COALESCE(ARRAY_AGG(DISTINCT post_seed_timesheet_ids.timesheet_id ORDER BY post_seed_timesheet_ids.timesheet_id), ARRAY[]::uuid[]) AS timesheet_ids
    FROM post_seed_timesheet_ids
    WHERE post_seed_timesheet_ids.timesheet_id IS NOT NULL
  ), post_expanded_timesheet_ids AS (
    SELECT post_seed_timesheet_ids.timesheet_id
    FROM post_seed_timesheet_ids
    WHERE post_seed_timesheet_ids.timesheet_id IS NOT NULL

    UNION

    SELECT correction_member.value::uuid
    FROM post_seed_timesheet_ids AS correction_seed
    JOIN public.timesheets AS correction_seed_timesheet
      ON correction_seed_timesheet.timesheet_id = correction_seed.timesheet_id
    CROSS JOIN LATERAL (
      SELECT public.timesheet_correction_chain_scope_v1(
        correction_seed.timesheet_id, false, 32, 100
      ) AS chain_json
    ) AS correction_chain
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(correction_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
    ) AS correction_member(value)
    WHERE correction_seed.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS correction_seed_financials
        WHERE correction_seed_financials.timesheet_id = correction_seed.timesheet_id
          AND correction_seed_financials.candidate_id = p_candidate_id
      )
      AND COALESCE((correction_chain.chain_json->>'valid')::boolean, false)
      AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          COALESCE(correction_chain.chain_json->'member_timesheet_ids', '[]'::jsonb)
        ) AS classified_member(value)
        WHERE classified_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND COALESCE((public._ctms_import_correction_classify_v1(classified_member.value::uuid)
            ->>'is_import_authoritative_correction')::boolean, false)
      )
      AND correction_member.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION

    SELECT rotation_scope.canonical_timesheet_id
    FROM post_seed_array
    JOIN public._pay_timesheet_rotation_scope(post_seed_array.timesheet_ids) AS rotation_scope ON true
    WHERE rotation_scope.canonical_timesheet_id IS NOT NULL

    UNION

    SELECT rotation_scope.family_timesheet_id
    FROM post_seed_array
    JOIN public._pay_timesheet_rotation_scope(post_seed_array.timesheet_ids) AS rotation_scope ON true
    WHERE rotation_scope.family_timesheet_id IS NOT NULL
  )
  SELECT COALESCE(ARRAY_AGG(DISTINCT post_expanded_timesheet_ids.timesheet_id ORDER BY post_expanded_timesheet_ids.timesheet_id), ARRAY[]::uuid[])
  INTO v_post_sync_scope_timesheet_ids
  FROM post_expanded_timesheet_ids
  WHERE post_expanded_timesheet_ids.timesheet_id IS NOT NULL
    AND NOT (post_expanded_timesheet_ids.timesheet_id = ANY(COALESCE(v_exclude_timesheet_ids, ARRAY[]::uuid[])))
    AND (
      v_client_filter_single IS NULL
      OR EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS post_scoped_tsfin
        WHERE post_scoped_tsfin.timesheet_id = post_expanded_timesheet_ids.timesheet_id
          AND post_scoped_tsfin.candidate_id = p_candidate_id
          AND post_scoped_tsfin.client_id = v_client_filter_single
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_advances AS post_scoped_finance_case
        WHERE post_scoped_finance_case.linked_timesheet_id = post_expanded_timesheet_ids.timesheet_id
          AND post_scoped_finance_case.candidate_id = p_candidate_id
          AND post_scoped_finance_case.client_id = v_client_filter_single
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_finance_case_components AS post_scoped_finance_component
        WHERE post_scoped_finance_component.linked_timesheet_id = post_expanded_timesheet_ids.timesheet_id
          AND post_scoped_finance_component.candidate_id = p_candidate_id
          AND post_scoped_finance_component.client_id = v_client_filter_single
      )
    );

  SELECT COALESCE(jsonb_agg(scope_id::text ORDER BY scope_id), '[]'::jsonb)
  INTO v_post_sync_scope_timesheet_ids_json
  FROM unnest(COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS post_sync_scope(scope_id);

  v_post_sync_scope_digest := md5(
    COALESCE(v_post_sync_scope_timesheet_ids_json::text, '[]') || '|' ||
    COALESCE(v_requested_refresh_scope_kind, 'CANDIDATE_FULL_LIVE') || '|' ||
    COALESCE(v_post_sync_candidate_pay_channel_scope, '') || '|' ||
    COALESCE(v_client_filter_single::text, '')
  );

  /* The post-sync scope can legitimately expand when the reconciliation
     creates or rediscovers durable finance-case authority.  Rebuild the
     rotation map from that final scope before attesting the resulting
     components; retaining the pre-sync map makes newly discovered family
     members impossible to attest. */
  TRUNCATE TABLE pg_temp._tmp_pay_wb_sync_rotation_scope;
  INSERT INTO pg_temp._tmp_pay_wb_sync_rotation_scope (
    requested_timesheet_id,
    canonical_timesheet_id,
    family_timesheet_id
  )
  SELECT DISTINCT
    rotation_scope.requested_timesheet_id,
    rotation_scope.canonical_timesheet_id,
    rotation_scope.family_timesheet_id
  FROM public._pay_timesheet_rotation_scope(
    COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[])
  ) AS rotation_scope;

  TRUNCATE TABLE pg_temp._tmp_pay_wb_sync_negative_components;

  WITH post_live_entitlement_components AS (
    SELECT
      entitlement_component.timesheet_id,
      UPPER(BTRIM(entitlement_component.key_type)) AS key_type,
      BTRIM(entitlement_component.key_value) AS key_value,
      ROUND(COALESCE(entitlement_component.truth_ex_vat, 0), 2) AS truth_ex_vat,
      ROUND(COALESCE(entitlement_component.baseline_ex_vat, 0), 2) AS baseline_ex_vat
    FROM public._pay_current_timesheet_entitlement_components(COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS entitlement_component
    WHERE NULLIF(BTRIM(COALESCE(entitlement_component.key_type, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(entitlement_component.key_value, '')), '') IS NOT NULL
  ), post_active_entitlement_reservations AS (
    SELECT
      reserved_component.timesheet_id,
      UPPER(BTRIM(reserved_component.key_type)) AS key_type,
      BTRIM(reserved_component.key_value) AS key_value,
      ROUND(COALESCE(reserved_component.amount_ex_vat, 0), 2) AS reserved_ex_vat
    FROM public._pay_reserved_components(COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[]), NULL::uuid) AS reserved_component
    WHERE NULLIF(BTRIM(COALESCE(reserved_component.key_type, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(reserved_component.key_value, '')), '') IS NOT NULL
  ), post_live_economic_keys AS (
    SELECT
      live_component.timesheet_id,
      live_component.key_type,
      live_component.key_value
    FROM post_live_entitlement_components AS live_component

    UNION

    SELECT
      reserved_component.timesheet_id,
      reserved_component.key_type,
      reserved_component.key_value
    FROM post_active_entitlement_reservations AS reserved_component
  ), post_raw_outstanding_components AS (
    SELECT
      live_key.timesheet_id,
      live_key.key_type,
      live_key.key_value,
      ROUND(COALESCE(live_component.truth_ex_vat, 0), 2) AS truth_ex_vat,
      ROUND(COALESCE(live_component.baseline_ex_vat, 0), 2) AS baseline_ex_vat,
      ROUND(COALESCE(reserved_component.reserved_ex_vat, 0), 2) AS reserved_ex_vat,
      ROUND(
        COALESCE(live_component.truth_ex_vat, 0)
        - COALESCE(live_component.baseline_ex_vat, 0)
        - COALESCE(reserved_component.reserved_ex_vat, 0),
        2
      ) AS outstanding_ex_vat
    FROM post_live_economic_keys AS live_key
    LEFT JOIN post_live_entitlement_components AS live_component
      ON live_component.timesheet_id = live_key.timesheet_id
     AND live_component.key_type = live_key.key_type
     AND live_component.key_value = live_key.key_value
    LEFT JOIN post_active_entitlement_reservations AS reserved_component
      ON reserved_component.timesheet_id = live_key.timesheet_id
     AND reserved_component.key_type = live_key.key_type
     AND reserved_component.key_value = live_key.key_value
  )
  INSERT INTO pg_temp._tmp_pay_wb_sync_negative_components (
    timesheet_id,
    key_type,
    key_value,
    truth_ex_vat,
    baseline_ex_vat,
    reserved_ex_vat,
    outstanding_ex_vat,
    baseline_signature
  )
  SELECT
    post_negative.timesheet_id,
    post_negative.key_type,
    post_negative.key_value,
    post_negative.truth_ex_vat,
    post_negative.baseline_ex_vat,
    post_negative.reserved_ex_vat,
    post_negative.outstanding_ex_vat,
    CASE
      WHEN COALESCE(post_active_settled_basis.active_settled_component_count, 0) > 0
        THEN post_active_settled_basis.active_settled_signature
      ELSE COALESCE(
        post_timesheet_pay_state.last_settled_signature,
        md5(COALESCE(post_timesheet_pay_state.last_settled_snapshot_json::text, '{}'))
      )
    END
  FROM post_raw_outstanding_components AS post_negative
  LEFT JOIN public.timesheet_pay_state AS post_timesheet_pay_state
    ON post_timesheet_pay_state.timesheet_id = post_negative.timesheet_id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*)::integer AS active_settled_component_count,
      md5(COALESCE(jsonb_agg(
        jsonb_build_object(
          'key_type', post_active_settled_component.key_type,
          'key_value', post_active_settled_component.key_value,
          'amount_ex_vat', ROUND(COALESCE(post_active_settled_component.amount_ex_vat, 0), 2),
          'amount_inc_vat', ROUND(COALESCE(post_active_settled_component.amount_inc_vat, 0), 2)
        ) ORDER BY post_active_settled_component.key_type, post_active_settled_component.key_value
      )::text, '[]')) AS active_settled_signature
    FROM public._pay_active_settled_components(
      ARRAY[post_negative.timesheet_id]::uuid[]
    ) AS post_active_settled_component
  ) AS post_active_settled_basis ON true
  WHERE post_negative.outstanding_ex_vat < 0;

  PERFORM public._ctms_rewrite_source_build_correction_negative_components_v1(
    p_session_id,
    p_candidate_id,
    COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[])
  );

  SELECT COUNT(*)::integer,
         md5(COALESCE(jsonb_agg(
           jsonb_build_object(
             'timesheet_id', post_negative.timesheet_id::text,
             'key_type', post_negative.key_type,
             'key_value', post_negative.key_value,
             'truth_ex_vat', ROUND(post_negative.truth_ex_vat, 2)::numeric(12,2),
             'baseline_ex_vat', ROUND(post_negative.baseline_ex_vat, 2)::numeric(12,2),
             'reserved_ex_vat', ROUND(post_negative.reserved_ex_vat, 2)::numeric(12,2),
             'outstanding_ex_vat', ROUND(post_negative.outstanding_ex_vat, 2)::numeric(12,2)
           ) ORDER BY post_negative.timesheet_id, post_negative.key_type, post_negative.key_value
         )::text, '[]'))
  INTO v_post_sync_negative_component_count,
       v_post_sync_negative_digest
  FROM pg_temp._tmp_pay_wb_sync_negative_components AS post_negative;

  SELECT COALESCE(jsonb_agg(
           jsonb_build_object(
             'timesheet_id', bounded_negative.timesheet_id::text,
             'key_type', bounded_negative.key_type,
             'key_value', bounded_negative.key_value,
             'truth_ex_vat', ROUND(bounded_negative.truth_ex_vat, 2)::numeric(12,2),
             'baseline_ex_vat', ROUND(bounded_negative.baseline_ex_vat, 2)::numeric(12,2),
             'reserved_ex_vat', ROUND(bounded_negative.reserved_ex_vat, 2)::numeric(12,2),
             'outstanding_ex_vat', ROUND(bounded_negative.outstanding_ex_vat, 2)::numeric(12,2)
           )
           ORDER BY bounded_negative.timesheet_id,
                    bounded_negative.key_type,
                    bounded_negative.key_value
         ), '[]'::jsonb)
  INTO v_post_sync_negative_components_diagnostic
  FROM (
    SELECT *
    FROM pg_temp._tmp_pay_wb_sync_negative_components
    ORDER BY timesheet_id, key_type, key_value
    LIMIT 20
  ) AS bounded_negative;

  SELECT md5(COALESCE(jsonb_agg(
           jsonb_build_object(
             'timesheet_id', post_settled.timesheet_id::text,
             'key_type', post_settled.key_type,
             'key_value', post_settled.key_value,
             'amount_ex_vat', ROUND(COALESCE(post_settled.amount_ex_vat, 0), 2),
             'amount_inc_vat', ROUND(COALESCE(post_settled.amount_inc_vat, 0), 2)
           ) ORDER BY post_settled.timesheet_id, post_settled.key_type, post_settled.key_value
         )::text, '[]'))
  INTO v_post_sync_baseline_digest
  FROM public._pay_active_settled_components(COALESCE(v_post_sync_scope_timesheet_ids, ARRAY[]::uuid[])) AS post_settled;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_post_sync_source_change_seq
  FROM (SELECT 1) AS post_sync_anchor
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF v_post_sync_candidate_pay_channel_scope IS DISTINCT FROM v_candidate_pay_channel_scope
     OR v_post_sync_scope_digest IS DISTINCT FROM v_sync_scope_digest
     OR v_post_sync_negative_component_count IS DISTINCT FROM v_sync_negative_component_count
     OR v_post_sync_negative_digest IS DISTINCT FROM v_sync_negative_digest
     OR v_post_sync_baseline_digest IS DISTINCT FROM v_sync_baseline_digest THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_AUTHORITY_CHANGED_DURING_RECONCILIATION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_AUTHORITY_CHANGED_DURING_RECONCILIATION',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'initial_source_change_seq', COALESCE(v_initial_source_change_seq, 0),
              'post_sync_source_change_seq', COALESCE(v_post_sync_source_change_seq, 0),
              'initial_pay_channel_scope', v_candidate_pay_channel_scope,
              'post_sync_pay_channel_scope', v_post_sync_candidate_pay_channel_scope,
              'initial_scope_digest', v_sync_scope_digest,
              'post_sync_scope_digest', v_post_sync_scope_digest,
              'initial_negative_component_digest', v_sync_negative_digest,
              'post_sync_negative_component_digest', v_post_sync_negative_digest,
              'initial_negative_component_count', v_sync_negative_component_count,
              'post_sync_negative_component_count', v_post_sync_negative_component_count,
              'post_sync_negative_components', v_post_sync_negative_components_diagnostic,
              'initial_settled_baseline_digest', v_sync_baseline_digest,
              'post_sync_settled_baseline_digest', v_post_sync_baseline_digest
            )::text;
  END IF;

  /* The sequence advance is accepted only after the complete current economic
     fingerprint above remains identical.  Continuations and source rows use the
     adopted sequence returned by this function. */
  v_source_change_seq := COALESCE(v_post_sync_source_change_seq, 0);
  v_current_source_change_seq := COALESCE(v_post_sync_source_change_seq, 0);

  IF COALESCE(v_sync_negative_component_count, 0) > 0
     AND v_candidate_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_NEGATIVE_COMPONENT_UNSUPPORTED_PAY_CHANNEL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_NEGATIVE_COMPONENT_UNSUPPORTED_PAY_CHANNEL',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'pay_channel_scope', v_candidate_pay_channel_scope,
              'negative_component_count', COALESCE(v_sync_negative_component_count, 0)
            )::text;
  END IF;

  IF COALESCE(v_sync_invoked, false) THEN
    WITH raw_sync_rows AS (
      SELECT negative_sync_row.value AS sync_row_json
      FROM jsonb_array_elements(
        CASE WHEN jsonb_typeof(v_sync_result->'negative_preview_timesheets') = 'array' THEN v_sync_result->'negative_preview_timesheets' ELSE '[]'::jsonb END
      ) AS negative_sync_row(value)

      UNION ALL

      SELECT candidate_sync_row.value AS sync_row_json
      FROM jsonb_array_elements(
        CASE WHEN jsonb_typeof(v_sync_result->'timesheet_finance_case_candidates') = 'array' THEN v_sync_result->'timesheet_finance_case_candidates' ELSE '[]'::jsonb END
      ) AS candidate_sync_row(value)
    ), parsed_sync_rows AS (
      SELECT
        NULLIF(BTRIM(COALESCE(raw_sync_rows.sync_row_json->>'candidate_id', '')), '') AS candidate_id_text,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(raw_sync_rows.sync_row_json->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(raw_sync_rows.sync_row_json->>'timesheet_id', '')), '')::uuid
          ELSE NULL::uuid
        END AS timesheet_id
      FROM raw_sync_rows
    )
    SELECT COUNT(*)::integer
    INTO v_sync_result_out_of_scope_count
    FROM parsed_sync_rows
    WHERE parsed_sync_rows.candidate_id_text IS DISTINCT FROM p_candidate_id::text
       OR parsed_sync_rows.timesheet_id IS NULL
       OR NOT (parsed_sync_rows.timesheet_id = ANY(COALESCE(v_sync_scope_timesheet_ids, ARRAY[]::uuid[])));

    IF COALESCE(v_sync_result_out_of_scope_count, 0) > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_SCOPE_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_SCOPE_MISMATCH',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'source_build_run_id', v_source_build_run_id::text,
                'scope_digest', v_sync_scope_digest,
                'out_of_scope_result_count', COALESCE(v_sync_result_out_of_scope_count, 0)
              )::text;
    END IF;
  END IF;

  IF COALESCE(v_sync_negative_component_count, 0) = 0 THEN
    v_sync_candidate_covered := true;
  ELSIF COALESCE(v_sync_invoked, false) THEN
    SELECT NOT EXISTS (
      SELECT 1
      FROM pg_temp._tmp_pay_wb_sync_negative_components AS negative_component
      WHERE NOT EXISTS (
        SELECT 1
        FROM (
          SELECT negative_sync_row.value AS sync_row_json
          FROM jsonb_array_elements(
            CASE WHEN jsonb_typeof(v_sync_result->'negative_preview_timesheets') = 'array' THEN v_sync_result->'negative_preview_timesheets' ELSE '[]'::jsonb END
          ) AS negative_sync_row(value)

          UNION ALL

          SELECT candidate_sync_row.value AS sync_row_json
          FROM jsonb_array_elements(
            CASE WHEN jsonb_typeof(v_sync_result->'timesheet_finance_case_candidates') = 'array' THEN v_sync_result->'timesheet_finance_case_candidates' ELSE '[]'::jsonb END
          ) AS candidate_sync_row(value)
        ) AS sync_rows
        WHERE NULLIF(BTRIM(COALESCE(sync_rows.sync_row_json->>'candidate_id', '')), '') = p_candidate_id::text
          AND EXISTS (
            SELECT 1
            FROM pg_temp._tmp_pay_wb_sync_rotation_scope AS candidate_rotation_scope
            WHERE candidate_rotation_scope.requested_timesheet_id = negative_component.timesheet_id
              AND NULLIF(BTRIM(COALESCE(sync_rows.sync_row_json->>'timesheet_id', '')), '') IN (
                candidate_rotation_scope.requested_timesheet_id::text,
                candidate_rotation_scope.canonical_timesheet_id::text,
                candidate_rotation_scope.family_timesheet_id::text
              )
          )
      )
    )
    INTO v_sync_candidate_covered;
  ELSE
    v_sync_candidate_covered := true;
  END IF;

  WITH negative_timesheet_totals AS (
    SELECT
      negative_component.timesheet_id,
      ROUND(COALESCE(SUM(ABS(negative_component.outstanding_ex_vat)), 0), 2) AS overpayment_amount_ex
    FROM pg_temp._tmp_pay_wb_sync_negative_components AS negative_component
    GROUP BY negative_component.timesheet_id
  ), component_coverage AS (
    SELECT
      negative_component.timesheet_id,
      negative_component.key_type,
      negative_component.key_value,
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(
            coalesce(
              v_sync_result
                ->'correction_resolution_pending_member_timesheet_ids',
              '[]'::jsonb
            )
          ) pending_member(value)
          WHERE pending_member.value::uuid
                = negative_component.timesheet_id
        )
        OR negative_component.timesheet_id = ANY(
          COALESCE(v_current_resolution_pending_member_ids, ARRAY[]::uuid[])
        ) THEN 'RESOLUTION_PENDING'
        WHEN EXISTS (
          SELECT 1
          FROM pg_temp._tmp_pay_wb_sync_rotation_scope AS rotation_scope
          JOIN public.pay_advances AS finance_case
            ON finance_case.linked_timesheet_id = rotation_scope.family_timesheet_id
           AND finance_case.candidate_id = p_candidate_id
           AND finance_case.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
          JOIN public.pay_finance_case_components AS finance_component
            ON finance_component.finance_case_id = finance_case.id
           AND finance_component.candidate_id = p_candidate_id
           AND finance_component.linked_timesheet_id = finance_case.linked_timesheet_id
           AND UPPER(BTRIM(finance_component.component_key_type)) = negative_component.key_type
           AND BTRIM(finance_component.component_key_value) = negative_component.key_value
          WHERE rotation_scope.requested_timesheet_id = negative_component.timesheet_id
            AND (
              finance_case.baseline_signature IS NOT DISTINCT FROM negative_component.baseline_signature
              OR (
                LOWER(BTRIM(COALESCE(finance_component.source_family_key, '')))
                    LIKE 'correction-chain:%'
                AND NULLIF(
                      BTRIM(COALESCE(
                        finance_component.source_basis_json
                          ->>'correction_chain_residual_fingerprint',
                        ''
                      )),
                      ''
                    ) IS NOT NULL
                AND finance_case.baseline_signature IS NOT DISTINCT FROM NULLIF(
                      BTRIM(COALESCE(
                        finance_component.source_basis_json
                          ->>'correction_chain_residual_fingerprint',
                        ''
                      )),
                      ''
                    )
              )
            )
            AND finance_case.written_off_at_utc IS NULL
            AND finance_component.closed_at_utc IS NULL
            AND ROUND(COALESCE(finance_component.source_amount, 0), 2) >= ABS(negative_component.outstanding_ex_vat) - 0.01
            AND ABS(ROUND(
              COALESCE(finance_case.outstanding_amount, 0)
              - COALESCE((
                  SELECT SUM(COALESCE(case_component_remaining.remaining_source_amount, 0))
                  FROM public.pay_finance_case_components AS case_component_remaining
                  WHERE case_component_remaining.finance_case_id = finance_case.id
                    AND case_component_remaining.closed_at_utc IS NULL
                ), 0),
              2
            )) <= 0.01
            AND (
              (
                ROUND(COALESCE(finance_component.remaining_source_amount, 0), 2) > 0
                AND ROUND(COALESCE(finance_case.outstanding_amount, 0), 2) > 0
                AND finance_case.cleared_at_utc IS NULL
                AND UPPER(BTRIM(COALESCE(finance_case.status::text, ''))) = 'ACTIVE'
              )
              OR (
                ROUND(COALESCE(finance_component.remaining_source_amount, 0), 2) = 0
                AND ROUND(COALESCE(finance_case.outstanding_amount, 0), 2) = 0
                AND UPPER(BTRIM(COALESCE(finance_case.status::text, ''))) = 'PAID_OFF'
                AND (
                  EXISTS (
                    SELECT 1
                    FROM public.pay_finance_case_events AS component_settlement_event
                    WHERE component_settlement_event.finance_case_id = finance_case.id
                      AND component_settlement_event.finance_component_id = finance_component.id
                      AND UPPER(BTRIM(COALESCE(component_settlement_event.event_type, ''))) = 'COMPONENT_SETTLED'
                      AND BTRIM(COALESCE(component_settlement_event.after_json->>'remaining_source_amount', ''))
                          ~ '^-?[0-9]+([.][0-9]+)?$'
                      AND ROUND((component_settlement_event.after_json->>'remaining_source_amount')::numeric, 2) = 0
                  )
                  OR ROUND(COALESCE((
                    SELECT SUM(ABS(COALESCE(
                      settled_component_reservation.reserved_source_amount,
                      public._pay_batch_item_source_reservation_amount_ex_vat(settled_component_item.id),
                      settled_component_item.frozen_source_amount,
                      settled_component_item.amount_ex_vat,
                      settled_component_item.amount_inc_vat,
                      settled_component_reservation.reserved_amount,
                      0
                    )))
                    FROM public.pay_batch_items AS settled_component_item
                    JOIN public.pay_batch_candidates AS settled_component_candidate
                      ON settled_component_candidate.id = settled_component_item.pay_batch_candidate_id
                    LEFT JOIN public.pay_bank_transfers AS settled_component_transfer
                      ON settled_component_transfer.id = settled_component_item.pay_bank_transfer_id
                    LEFT JOIN public.pay_advance_reservations AS settled_component_reservation
                      ON settled_component_reservation.pay_batch_item_id = settled_component_item.id
                    WHERE COALESCE(settled_component_item.is_voided, false) IS NOT TRUE
                      AND UPPER(BTRIM(COALESCE(settled_component_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
                      AND (
                        COALESCE(
                          settled_component_reservation.finance_component_id,
                          settled_component_item.finance_component_id
                        ) = finance_component.id
                        OR (
                          COALESCE(
                            settled_component_reservation.finance_component_id,
                            settled_component_item.finance_component_id
                          ) IS NULL
                          AND COALESCE(
                            settled_component_reservation.finance_case_id,
                            settled_component_item.finance_case_id,
                            CASE
                              WHEN UPPER(BTRIM(SPLIT_PART(COALESCE(settled_component_item.source_ref, ''), ':', 1))) = 'ADVANCE'
                               AND NULLIF(BTRIM(SPLIT_PART(COALESCE(settled_component_item.source_ref, ''), ':', 2)), '')
                                   ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                              THEN NULLIF(BTRIM(SPLIT_PART(COALESCE(settled_component_item.source_ref, ''), ':', 2)), '')::uuid
                              ELSE NULL::uuid
                            END
                          ) = finance_case.id
                          AND UPPER(BTRIM(COALESCE(
                            settled_component_item.frozen_component_key_type,
                            settled_component_item.frozen_component_snapshot_json->>'component_key_type',
                            settled_component_item.frozen_source_basis_json->>'component_key_type',
                            ''
                          ))) = negative_component.key_type
                          AND BTRIM(COALESCE(
                            settled_component_item.frozen_component_key_value,
                            settled_component_item.frozen_component_snapshot_json->>'component_key_value',
                            settled_component_item.frozen_source_basis_json->>'component_key_value',
                            ''
                          )) = negative_component.key_value
                          AND (
                            settled_component_item.timesheet_id IS NULL
                            OR settled_component_item.timesheet_id = finance_component.linked_timesheet_id
                            OR EXISTS (
                              SELECT 1
                              FROM pg_temp._tmp_pay_wb_sync_rotation_scope AS settled_component_rotation_scope
                              WHERE settled_component_rotation_scope.requested_timesheet_id = negative_component.timesheet_id
                                AND settled_component_item.timesheet_id IN (
                                  settled_component_rotation_scope.requested_timesheet_id,
                                  settled_component_rotation_scope.canonical_timesheet_id,
                                  settled_component_rotation_scope.family_timesheet_id
                                )
                            )
                          )
                        )
                      )
                      AND NOT EXISTS (
                        SELECT 1
                        FROM public.pay_payment_correction_items AS settled_component_correction
                        WHERE settled_component_correction.pay_batch_item_id = settled_component_item.id
                          AND settled_component_correction.status = 'APPLIED'
                          AND settled_component_correction.correction_item_kind IN (
                            'PRE_BANK_CANCEL',
                            'NO_MONEY_UNWIND',
                            'SETTLED_REVERSAL'
                          )
                      )
                      AND (
                        UPPER(BTRIM(COALESCE(settled_component_candidate.settlement_status, ''))) = 'SETTLED'
                        OR settled_component_candidate.settled_at_utc IS NOT NULL
                        OR UPPER(BTRIM(COALESCE(settled_component_transfer.status, ''))) = 'COMPLETED'
                        OR settled_component_transfer.completed_at_utc IS NOT NULL
                        OR UPPER(BTRIM(COALESCE(settled_component_reservation.status, ''))) = 'SETTLED'
                        OR settled_component_reservation.settled_at_utc IS NOT NULL
                      )
                  ), 0), 2) >= ABS(negative_component.outstanding_ex_vat) - 0.01
                )
              )
            )
        ) THEN 'DURABLE_COMPONENT'
        WHEN EXISTS (
          SELECT 1
          FROM public.pay_advances AS protected_case
          WHERE protected_case.linked_timesheet_id = negative_component.timesheet_id
            AND protected_case.candidate_id = p_candidate_id
            AND protected_case.case_type IN (
              'OVERPAYMENT'::public.pay_finance_case_type_enum,
              'UNDERPAYMENT'::public.pay_finance_case_type_enum
            )
            AND (
              protected_case.written_off_at_utc IS NOT NULL
              OR protected_case.cleared_at_utc IS NOT NULL
              OR UPPER(BTRIM(COALESCE(protected_case.status::text, ''))) NOT IN ('ACTIVE', 'PAID_OFF')
              OR EXISTS (
                SELECT 1
                FROM public.pay_finance_case_events AS protected_lifecycle_event
                WHERE protected_lifecycle_event.finance_case_id = protected_case.id
                  AND (
                    UPPER(BTRIM(COALESCE(protected_lifecycle_event.event_type, ''))) LIKE '%WRITE%OFF%'
                    OR UPPER(BTRIM(COALESCE(protected_lifecycle_event.event_type, ''))) LIKE '%RESTRUCT%'
                    OR UPPER(BTRIM(COALESCE(protected_lifecycle_event.event_type, ''))) LIKE '%MANUAL%'
                  )
              )
            )
            AND EXISTS (
              SELECT 1
              FROM public.pay_finance_case_events AS protected_sync_event
              WHERE protected_sync_event.finance_case_id = protected_case.id
                AND UPPER(BTRIM(COALESCE(protected_sync_event.event_type, ''))) = 'SYNC_SKIPPED'
                AND UPPER(BTRIM(COALESCE(protected_sync_event.reason, ''))) = 'PREVIEW_FINANCE_SYNC_SKIPPED_PROTECTED_CASE'
                AND LOWER(BTRIM(COALESCE(protected_sync_event.after_json->>'sync_skipped', 'false'))) IN (
                  '1',
                  'true',
                  't',
                  'yes',
                  'y',
                  'on'
                )
                AND UPPER(BTRIM(COALESCE(protected_sync_event.after_json->>'target_case_type', ''))) = 'OVERPAYMENT'
                AND LOWER(BTRIM(COALESCE(protected_sync_event.after_json->>'component_sync_skipped', 'false'))) IN (
                  '1',
                  'true',
                  't',
                  'yes',
                  'y',
                  'on'
                )
                AND LOWER(BTRIM(COALESCE(protected_sync_event.after_json->>'component_authority_preserved', 'false'))) IN (
                  '1',
                  'true',
                  't',
                  'yes',
                  'y',
                  'on'
                )
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'workbench_session_id', '')), '') = p_session_id::text
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'source_build_run_id', '')), '') = v_source_build_run_id::text
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'session_version', '')), '') = v_session_version::text
                AND UPPER(BTRIM(COALESCE(protected_sync_event.after_json->>'target_pay_channel_scope', ''))) = v_candidate_pay_channel_scope
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'overpayment_sync_scope_digest', '')), '') IS NOT DISTINCT FROM v_sync_scope_digest
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'overpayment_sync_negative_component_digest', '')), '') IS NOT DISTINCT FROM v_sync_negative_digest
                AND NULLIF(BTRIM(COALESCE(protected_sync_event.after_json->>'overpayment_sync_settled_baseline_digest', '')), '') IS NOT DISTINCT FROM v_sync_baseline_digest
                AND UPPER(BTRIM(COALESCE(protected_sync_event.after_json->>'policy_x_authority_scope', ''))) = 'PRE_DRAFT_LIVE_TRUTH'
                AND NULLIF(
                      BTRIM(COALESCE(protected_sync_event.after_json->>'target_baseline_signature', '')),
                      ''
                    ) IS NOT DISTINCT FROM NULLIF(BTRIM(COALESCE(negative_component.baseline_signature, '')), '')
                AND CASE
                      WHEN BTRIM(COALESCE(protected_sync_event.after_json->>'target_original_amount', ''))
                           ~ '^-?[0-9]+([.][0-9]+)?$'
                      THEN ROUND(
                             ABS((protected_sync_event.after_json->>'target_original_amount')::numeric),
                             2
                           )
                      ELSE NULL::numeric
                    END = negative_timesheet_total.overpayment_amount_ex
            )
        ) THEN 'PROTECTED_CASE'
        ELSE 'UNCOVERED'
      END AS coverage_kind
    FROM pg_temp._tmp_pay_wb_sync_negative_components AS negative_component
    JOIN negative_timesheet_totals AS negative_timesheet_total
      ON negative_timesheet_total.timesheet_id = negative_component.timesheet_id
  )
  SELECT
    COUNT(*) FILTER (WHERE component_coverage.coverage_kind = 'DURABLE_COMPONENT')::integer,
    COUNT(*) FILTER (WHERE component_coverage.coverage_kind = 'PROTECTED_CASE')::integer,
    COUNT(*) FILTER (WHERE component_coverage.coverage_kind = 'RESOLUTION_PENDING')::integer,
    COUNT(*) FILTER (WHERE component_coverage.coverage_kind = 'UNCOVERED')::integer
  INTO v_sync_durable_component_count,
       v_sync_protected_component_count,
       v_sync_resolution_pending_component_count,
       v_sync_uncovered_component_count
  FROM component_coverage;

  /* A successful reconciliation need not echo a component that was already
     durably represented. A component awaiting the existing PAYE/umbrella
     resolution UI is intentionally not finance authority: the sync helper
     suppresses its mutation and draft creation remains fail-closed until the
     resolution is saved. Sync-result rows remain scope-checked above. */
  v_sync_candidate_covered := (
    COALESCE(v_sync_uncovered_component_count, 0) = 0
    AND COALESCE(v_sync_durable_component_count, 0)
        + COALESCE(v_sync_protected_component_count, 0)
        + COALESCE(v_sync_resolution_pending_component_count, 0)
        = COALESCE(v_sync_negative_component_count, 0)
  );

  IF COALESCE(v_sync_candidate_covered, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_UNATTESTED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_UNATTESTED',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'source_build_run_id', v_source_build_run_id::text,
              'source_change_seq', v_source_change_seq,
              'candidate_covered', COALESCE(v_sync_candidate_covered, false),
              'negative_component_count', COALESCE(v_sync_negative_component_count, 0),
              'durable_component_count', COALESCE(v_sync_durable_component_count, 0),
              'protected_component_count', COALESCE(v_sync_protected_component_count, 0),
              'resolution_pending_component_count',
                COALESCE(v_sync_resolution_pending_component_count, 0),
              'uncovered_component_count', COALESCE(v_sync_uncovered_component_count, 0),
              'sync_result', COALESCE(v_sync_result, '{}'::jsonb)
            )::text;
  END IF;

  v_sync_result_code := CASE
    WHEN COALESCE(v_sync_negative_component_count, 0) = 0 THEN 'NO_NEGATIVE_COMPONENTS'
    WHEN COALESCE(v_sync_resolution_pending_component_count, 0) > 0
      THEN 'PAY_METHOD_RESOLUTION_REQUIRED'
    WHEN COALESCE(v_sync_protected_component_count, 0) = COALESCE(v_sync_negative_component_count, 0) THEN 'PROTECTED_TERMINAL'
    WHEN COALESCE(v_sync_protected_component_count, 0) > 0 THEN 'RECONCILED_WITH_PROTECTED_COMPONENTS'
    ELSE 'RECONCILED_DURABLE_CASE_COMPONENTS'
  END;

  v_sync_attestation := jsonb_build_object(
    'completed', true,
    'attested', true,
    'ok', true,
    'result_code', v_sync_result_code,
    'attested_at_utc', clock_timestamp()::text,
    'session_id', p_session_id::text,
    'session_version', v_session_version,
    'candidate_id', p_candidate_id::text,
    'pay_channel_scope', v_candidate_pay_channel_scope,
    'source_change_seq', v_source_change_seq,
    'source_build_run_id', v_source_build_run_id::text
  )
  || jsonb_build_object(
    'refresh_scope_kind', v_requested_refresh_scope_kind,
    'scope_timesheet_ids', COALESCE(v_sync_scope_timesheet_ids_json, '[]'::jsonb),
    'scope_timesheet_count', COALESCE(array_length(v_sync_scope_timesheet_ids, 1), 0),
    'scope_digest', v_sync_scope_digest,
    'negative_component_digest', v_sync_negative_digest,
    'settled_baseline_digest', v_sync_baseline_digest,
    'negative_component_count', COALESCE(v_sync_negative_component_count, 0),
    'durable_component_count', COALESCE(v_sync_durable_component_count, 0),
    'protected_component_count', COALESCE(v_sync_protected_component_count, 0),
    'resolution_pending_component_count',
      COALESCE(v_sync_resolution_pending_component_count, 0),
    'uncovered_component_count', COALESCE(v_sync_uncovered_component_count, 0),
    'sync_invoked', COALESCE(v_sync_invoked, false),
    'sync_marker_reused', false
  )
  || jsonb_build_object(
    'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
    'session_signature', v_session_row.session_signature,
    'old_marker_present', COALESCE(v_old_sync_marker, '{}'::jsonb) <> '{}'::jsonb,
    'old_marker_accepted_as_authority', false,
    'source_change_seq_supplied', COALESCE(v_source_change_seq_was_supplied, false),
    'initial_source_change_seq', COALESCE(v_initial_source_change_seq, 0),
    'post_sync_source_change_seq', COALESCE(v_post_sync_source_change_seq, 0),
    'sequence_advanced_with_stable_economic_fingerprint', COALESCE(v_post_sync_source_change_seq, 0) > COALESCE(v_initial_source_change_seq, 0),
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );

  v_sync_completed := true;
  v_sync_attested := true;

  v_overpayment_sync_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF v_sync_authority_token IS NULL THEN
    v_sync_authority_token := md5(
      txid_current()::text || '|' ||
      p_session_id::text || '|' ||
      p_candidate_id::text || '|' ||
      v_source_build_run_id::text || '|' ||
      clock_timestamp()::text || '|' ||
      random()::text
    );
  END IF;

  BEGIN
    PERFORM set_config(
      'cloudtms.pay_workbench_overpayment_sync_token',
      v_sync_authority_token,
      true
    );

    v_preview_decisions_json := COALESCE(v_session_row.filters_json, '{}'::jsonb)
      || COALESCE(v_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'preview_context_mode', 'PAGE',
        'workbench_source_build_mode', true,
        'source_build_mode', true,
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'refresh_scope_kind', v_refresh_scope_kind,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'source_build_force_include_timesheet_ids', COALESCE(v_post_sync_scope_timesheet_ids_json, '[]'::jsonb),
        'pay_channel_scope', v_pay_channel_scope,
        'source_build_run_id', v_source_build_run_id::text,
        'source_change_seq', v_source_change_seq,
        'session_version', v_session_version,
        'overpayment_sync_authority_token', v_sync_authority_token
      )
      || jsonb_build_object(
        'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
        'session_signature', v_session_row.session_signature,
        'source_page_limit', v_limit,
        'source_cursor', COALESCE(v_cursor_json, '{}'::jsonb),
        'source_build_allow_full_fallback', LOWER(BTRIM(COALESCE(v_payload_json->>'source_build_allow_full_fallback', v_payload_json#>>'{source_build,allow_full_fallback}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'source_build_fallback_reason', NULLIF(BTRIM(COALESCE(v_payload_json->>'source_build_fallback_reason', v_payload_json#>>'{source_build,fallback_reason}', '')), ''),
        'overpayment_sync_completed', COALESCE(v_sync_attested, false),
        'overpayment_sync_attestation', COALESCE(v_sync_attestation, '{}'::jsonb),
        'overpayment_sync_pay_channel_scope', v_candidate_pay_channel_scope,
        'reason', NULLIF(BTRIM(COALESCE(v_payload_json->>'reason', v_payload_json#>>'{source_build,reason}', '')), ''),
        'trigger_table', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json#>>'{trigger,table}', '')), ''),
        'trigger_operation', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json#>>'{trigger,operation}', '')), '')
      );

    v_context_json := public.pay_preview_build_context(
      p_pay_date => v_session_row.pay_date,
      p_week_ending_cutoff => v_session_row.week_ending_cutoff,
      p_actor_user_id => v_session_row.actor_user_id,
      p_candidate_id => p_candidate_id,
      p_client_id => NULL::uuid,
      p_preview_decisions_json => v_preview_decisions_json
    );

    v_context_json := v_context_json
    || jsonb_build_object(
      'workbench_session_id', p_session_id::text,
      'session_id', p_session_id::text,
      'workbench_resolution_session_id', p_session_id::text,
      'workbench_source_build_mode', true,
      'source_build_mode', true,
      'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
      'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
      'source_build_force_include_timesheet_ids', COALESCE(v_post_sync_scope_timesheet_ids_json, '[]'::jsonb),
      'pay_channel_scope', v_pay_channel_scope,
      'source_build_run_id', v_source_build_run_id::text,
      'source_change_seq', v_source_change_seq,
      'session_version', v_session_version
    )
    || jsonb_build_object(
      'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
      'session_signature', v_session_row.session_signature,
      'source_page_limit', v_limit,
      'source_cursor', COALESCE(v_cursor_json, '{}'::jsonb),
      'overpayment_sync_completed', COALESCE(v_sync_attested, false),
      'overpayment_sync_attestation', COALESCE(v_sync_attestation, '{}'::jsonb),
      'overpayment_sync_pay_channel_scope', v_candidate_pay_channel_scope
    );

    v_preview_context_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
    v_diag_phase_started_at := clock_timestamp();

    v_collect_result := public.pay_preview_candidate_collect_scope(
      p_context_json => v_context_json,
      p_candidate_id => p_candidate_id,
      p_cursor_json => v_cursor_json,
      p_limit => v_limit
    );

    PERFORM set_config(
      'cloudtms.pay_workbench_overpayment_sync_token',
      '',
      true
    );

  EXCEPTION WHEN OTHERS THEN
    PERFORM set_config(
      'cloudtms.pay_workbench_overpayment_sync_token',
      '',
      true
    );
    RAISE;
  END;

  v_collect_call_wall_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  v_collect_diagnostics_json := CASE
    WHEN jsonb_typeof(v_collect_result->'source_build_collect_diagnostics') = 'object'
      THEN COALESCE(v_collect_result->'source_build_collect_diagnostics', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_actual_refresh_scope_kind := COALESCE(
    NULLIF(BTRIM(v_collect_diagnostics_json->>'actual_refresh_scope_kind'), ''),
    NULLIF(BTRIM(v_collect_result->>'refresh_scope_kind'), ''),
    v_refresh_scope_kind
  );
  v_has_more := LOWER(BTRIM(COALESCE(v_collect_diagnostics_json->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_source_cursor_out_json := CASE
    WHEN jsonb_typeof(v_collect_diagnostics_json->'cursor_out') = 'object' THEN v_collect_diagnostics_json->'cursor_out'
    ELSE NULL::jsonb
  END;
  v_source_cursor_in_json := CASE
    WHEN jsonb_typeof(v_collect_diagnostics_json->'cursor_in') = 'object' THEN v_collect_diagnostics_json->'cursor_in'
    ELSE COALESCE(v_cursor_json, '{}'::jsonb)
  END;
  v_collect_elapsed_ms := CASE WHEN COALESCE(v_collect_diagnostics_json->>'collect_elapsed_ms', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (v_collect_diagnostics_json->>'collect_elapsed_ms')::numeric ELSE 0 END;
  v_source_page_count := CASE WHEN COALESCE(v_collect_diagnostics_json->>'source_page_count', '') ~ '^-?[0-9]+$' THEN (v_collect_diagnostics_json->>'source_page_count')::integer ELSE 0 END;
  v_source_remaining_after_cursor_count := CASE WHEN COALESCE(v_collect_diagnostics_json->>'source_remaining_after_cursor_count', '') ~ '^-?[0-9]+$' THEN (v_collect_diagnostics_json->>'source_remaining_after_cursor_count')::integer ELSE 0 END;
  v_fallback_used := LOWER(BTRIM(COALESCE(v_collect_diagnostics_json->>'fallback_used', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_fallback_reason := NULLIF(BTRIM(COALESCE(v_collect_diagnostics_json->>'fallback_reason', '')), '');
  v_candidate_filter_applied_early := LOWER(BTRIM(COALESCE(v_collect_diagnostics_json->>'candidate_filter_applied_early', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_timesheet_filter_applied_early := LOWER(BTRIM(COALESCE(v_collect_diagnostics_json->>'timesheet_filter_applied_early', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_large_aggregation_avoided := LOWER(BTRIM(COALESCE(v_collect_diagnostics_json->>'large_aggregation_avoided', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_diag_phase_started_at := clock_timestamp();

  v_canonical_result := public.pay_preview_candidate_build_canonical_lines(
    p_context_json => v_context_json,
    p_candidate_id => p_candidate_id
  );

  v_canonical_call_wall_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  v_canonical_diagnostics_json := CASE
    WHEN jsonb_typeof(v_canonical_result->'source_build_canonical_diagnostics') = 'object'
      THEN COALESCE(v_canonical_result->'source_build_canonical_diagnostics', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_canonical_elapsed_ms := CASE WHEN COALESCE(v_canonical_diagnostics_json->>'canonical_elapsed_ms', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (v_canonical_diagnostics_json->>'canonical_elapsed_ms')::numeric ELSE 0 END;
  v_classifier_elapsed_ms := v_canonical_elapsed_ms;
  v_collect_called_inside_canonical := LOWER(BTRIM(COALESCE(v_canonical_diagnostics_json->>'collect_called_inside_canonical', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF COALESCE(v_collect_called_inside_canonical, false) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANONICAL_RECOLLECTED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANONICAL_RECOLLECTED',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  IF to_regclass('pg_temp.canonical_preview_lines') IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANONICAL_LINES_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CANONICAL_LINES_MISSING',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  v_diag_phase_started_at := clock_timestamp();

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_preview_line_seed_source;
  CREATE TEMPORARY TABLE pg_temp._tmp_pay_wb_preview_line_seed_source ON COMMIT DROP AS
  WITH base_rows_raw AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          public.pay_workbench_preview_section_from_line_json(canonical_rows.line_json),
          COALESCE(canonical_rows.line_json->>'preview_row_id', canonical_rows.line_json->>'line_id', canonical_rows.line_json->>'case_key', md5(canonical_rows.line_json::text))
      )::bigint AS base_ordinal,
      canonical_rows.candidate_id,
      canonical_rows.line_json,
      canonical_rows.pay_channel,
      canonical_rows.paye_treatment,
      canonical_rows.amount_ex_vat,
      canonical_rows.is_excluded_from_allocation,
      public.pay_workbench_preview_section_from_line_json(canonical_rows.line_json) AS target_section,
      UPPER(NULLIF(BTRIM(COALESCE(canonical_rows.line_json->>'line_type', canonical_rows.line_json->>'case_type', '')), '')) AS line_type,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(canonical_rows.line_json->>'real_business_timesheet_id', canonical_rows.line_json#>>'{economic_key,timesheet_id}', canonical_rows.line_json->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(canonical_rows.line_json->>'real_business_timesheet_id', canonical_rows.line_json#>>'{economic_key,timesheet_id}', canonical_rows.line_json->>'timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS requested_timesheet_id,
      NULLIF(BTRIM(COALESCE(canonical_rows.line_json->>'preview_row_id', canonical_rows.line_json->>'line_id', canonical_rows.line_json->>'case_key', md5(canonical_rows.line_json::text))), '') AS parent_line_key
    FROM pg_temp.canonical_preview_lines AS canonical_rows
    WHERE canonical_rows.candidate_id = p_candidate_id
  ), base_requested_timesheets AS (
    SELECT DISTINCT
      base_rows_raw.requested_timesheet_id
    FROM base_rows_raw
    WHERE base_rows_raw.requested_timesheet_id IS NOT NULL
  ), base_rotation_scope AS (
    SELECT
      rotation_scope_rows.requested_timesheet_id,
      rotation_scope_rows.canonical_timesheet_id,
      rotation_scope_rows.family_is_current,
      rotation_scope_rows.family_version,
      rotation_scope_rows.family_timesheet_id
    FROM public._pay_timesheet_rotation_scope(
      (
        SELECT COALESCE(
          array_agg(base_requested_timesheets.requested_timesheet_id ORDER BY base_requested_timesheets.requested_timesheet_id),
          ARRAY[]::uuid[]
        )
        FROM base_requested_timesheets
      )
    ) AS rotation_scope_rows
  ), base_requested_canonical AS (
    SELECT DISTINCT ON (base_rotation_scope.requested_timesheet_id)
      base_rotation_scope.requested_timesheet_id,
      COALESCE(base_rotation_scope.canonical_timesheet_id, base_rotation_scope.requested_timesheet_id) AS canonical_timesheet_id
    FROM base_rotation_scope
    WHERE base_rotation_scope.requested_timesheet_id IS NOT NULL
    ORDER BY
      base_rotation_scope.requested_timesheet_id,
      base_rotation_scope.family_is_current DESC NULLS LAST,
      base_rotation_scope.family_version DESC NULLS LAST,
      base_rotation_scope.family_timesheet_id
  ), base_rows AS (
    SELECT
      base_rows_raw.base_ordinal,
      base_rows_raw.candidate_id,
      jsonb_strip_nulls(
        base_rows_raw.line_json
        || jsonb_build_object(
          'timesheet_id', CASE WHEN COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id) IS NULL THEN NULL ELSE COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id)::text END,
          'real_business_timesheet_id', CASE WHEN COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id) IS NULL THEN NULL ELSE COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id)::text END
        )
        || CASE
          WHEN base_rows_raw.requested_timesheet_id IS NOT NULL
           AND COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id) IS DISTINCT FROM base_rows_raw.requested_timesheet_id
          THEN jsonb_build_object('rotation_requested_timesheet_id', base_rows_raw.requested_timesheet_id::text)
          ELSE '{}'::jsonb
        END
        || CASE
          WHEN LOWER(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,has_resolved_rate}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          THEN jsonb_build_object(
            'has_resolved_rate', true,
            'resolved_rate_family', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_family}', '')), ''),
            'resolved_rate_case_key', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_case_key}', '')), ''),
            'resolved_rate_timesheet_id', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_timesheet_id}', '')), ''),
            'resolved_rate_candidate_id', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_candidate_id}', '')), ''),
            'resolved_rate_source_anchor_case_key', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_source_anchor_case_key}', '')), ''),
            'resolved_rate_source_anchor_timesheet_id', NULLIF(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_source_anchor_timesheet_id}', '')), ''),
            'resolved_rate_applied_via_linked_scope', LOWER(BTRIM(COALESCE(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_applied_via_linked_scope}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
            'resolved_rate_clear_payload_json', CASE
              WHEN jsonb_typeof(base_rows_raw.line_json#>'{case_resolution_summary,resolved_rate_clear_payload_json}') = 'object'
                THEN base_rows_raw.line_json#>'{case_resolution_summary,resolved_rate_clear_payload_json}'
              ELSE NULL::jsonb
            END
          )
          ELSE '{}'::jsonb
        END
      ) AS line_json,
      base_rows_raw.pay_channel,
      base_rows_raw.paye_treatment,
      base_rows_raw.amount_ex_vat,
      base_rows_raw.is_excluded_from_allocation,
      base_rows_raw.target_section,
      base_rows_raw.line_type,
      (
        lower(btrim(coalesce(base_rows_raw.line_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
        or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
        or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
        or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
        or (
          coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
          and (base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_component_count}')::integer > 0
        )
      ) AS has_resolved_rate,
      jsonb_array_length(
        CASE
          WHEN jsonb_typeof(coalesce(base_rows_raw.line_json->'section_segment_rows', base_rows_raw.line_json->'segment_rows')) = 'array'
            THEN coalesce(base_rows_raw.line_json->'section_segment_rows', base_rows_raw.line_json->'segment_rows')
          ELSE '[]'::jsonb
        END
      ) > 0 AS has_timesheet_segment_rows,
      (
        lower(btrim(coalesce(base_rows_raw.line_json->>'resolved_segment_rows_replace_source_total', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
        and (
          lower(btrim(coalesce(base_rows_raw.line_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
          or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
          or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
          or lower(btrim(coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
          or (
            coalesce(base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
            and (base_rows_raw.line_json#>>'{case_resolution_summary,resolved_rate_component_count}')::integer > 0
          )
        )
        and jsonb_array_length(
          CASE
            WHEN jsonb_typeof(coalesce(base_rows_raw.line_json->'section_segment_rows', base_rows_raw.line_json->'segment_rows')) = 'array'
              THEN coalesce(base_rows_raw.line_json->'section_segment_rows', base_rows_raw.line_json->'segment_rows')
            ELSE '[]'::jsonb
          END
        ) > 0
      ) AS resolved_segment_rows_replace_source_total,
      COALESCE(base_requested_canonical.canonical_timesheet_id, base_rows_raw.requested_timesheet_id) AS timesheet_id,
      base_rows_raw.parent_line_key
    FROM base_rows_raw
    LEFT JOIN base_requested_canonical
      ON base_requested_canonical.requested_timesheet_id = base_rows_raw.requested_timesheet_id
  ), timesheet_segment_rows AS (
    SELECT
      base_rows.base_ordinal,
      base_rows.candidate_id,
      base_rows.timesheet_id,
      base_rows.target_section,
      base_rows.parent_line_key,
      ('segment:' || segment_rows.segment_ord::text) AS split_suffix,
      segment_rows.segment_ord::bigint AS split_ordinal,
      'SEGMENT_DELTA'::text AS item_type,
      NULL::text AS key_type_hint,
      NULL::text AS key_value_hint,
      segment_rows.segment_json,
      jsonb_strip_nulls(
        (
          base_rows.line_json
          - 'case_components'
          - 'component_key_type'
          - 'component_key_value'
          - 'frozen_component_key_type'
          - 'frozen_component_key_value'
          - 'frozen_component_classification'
          - 'frozen_component_snapshot_json'
          - 'source_basis_json'
          - 'frozen_source_basis_json'
        )
        || jsonb_build_object(
          'source_kind', 'VALID_PREVIEW_LINE',
          'preview_row_id', base_rows.parent_line_key || ':segment:' || segment_rows.segment_ord::text,
          'line_id', base_rows.parent_line_key || ':segment:' || segment_rows.segment_ord::text,
          'line_key', base_rows.parent_line_key || ':segment:' || segment_rows.segment_ord::text,
          'row_key', base_rows.parent_line_key || ':segment:' || segment_rows.segment_ord::text
        )
        || jsonb_build_object(
          'presentation_role', 'CHILD',
          'presentation_parent_line_id', base_rows.parent_line_key,
          'amount_ex_vat', segment_rows.segment_amount_ex_vat,
          'amount_display', segment_rows.segment_amount_ex_vat,
          'section_amount_ex_vat', segment_rows.segment_amount_ex_vat,
          'section_amount_display', segment_rows.segment_amount_ex_vat
        )
        || jsonb_build_object(
          'segment_rows', jsonb_build_array(segment_rows.segment_json),
          'section_segment_rows', jsonb_build_array(segment_rows.segment_json),
          'segment_count', 1,
          'section_segment_count', 1,
          'section_non_segment_amount_ex_vat', 0,
          'segment_id', NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_id', '')), '')
        )
        || jsonb_build_object(
          'segment_key', NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_key', '')), ''),
          'segment_stable_key', NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_stable_key', '')), ''),
          'work_date', NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'work_date', segment_rows.segment_json->>'date', '')), ''),
          'date', NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'date', segment_rows.segment_json->>'work_date', '')), ''),
          'source_basis_json', segment_rows.segment_json
        )
        || jsonb_build_object(
          'case_components', segment_case_components.segment_case_components_json
        )
      ) AS seed_line_json
    FROM base_rows
    CROSS JOIN LATERAL (
      SELECT
        segment_element.value AS segment_json,
        segment_element.ordinality::integer AS segment_ord,
        ROUND(COALESCE(
          CASE WHEN COALESCE(segment_element.value->>'pay_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (segment_element.value->>'pay_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(segment_element.value->>'effective_delta_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (segment_element.value->>'effective_delta_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(segment_element.value->>'delta_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (segment_element.value->>'delta_pay_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(segment_element.value->>'raw_delta_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (segment_element.value->>'raw_delta_ex_vat')::numeric ELSE NULL::numeric END,
          0::numeric
        ), 2) AS segment_amount_ex_vat
      FROM jsonb_array_elements(COALESCE(base_rows.line_json->'section_segment_rows', base_rows.line_json->'segment_rows', '[]'::jsonb)) WITH ORDINALITY AS segment_element(value, ordinality)
      WHERE segment_element.value IS NOT NULL
        AND jsonb_typeof(segment_element.value) = 'object'
    ) AS segment_rows
    CROSS JOIN LATERAL (
      WITH segment_identity AS (
        SELECT
          NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'work_date', segment_rows.segment_json->>'date', '')), '') AS work_date,
          NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_id', '')), '') AS segment_id,
          NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_key', '')), '') AS segment_key,
          NULLIF(BTRIM(COALESCE(segment_rows.segment_json->>'segment_stable_key', '')), '') AS segment_stable_key
      ), component_candidates AS (
        SELECT
          component_element.value,
          component_element.ordinality,
          UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', component_element.value->>'key_type', '')), '')) AS component_key_type,
          NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', component_element.value->>'key_value', '')), '') AS component_key_value,
          NULLIF(BTRIM(COALESCE(component_element.value->>'work_date', component_element.value->>'date', component_element.value->>'source_work_date', component_element.value#>>'{source_basis_json,work_date}', component_element.value#>>'{source_basis_json,date}', '')), '') AS component_work_date,
          NULLIF(BTRIM(COALESCE(component_element.value->>'segment_id', component_element.value#>>'{source_basis_json,segment_id}', '')), '') AS component_segment_id,
          NULLIF(BTRIM(COALESCE(component_element.value->>'segment_key', component_element.value#>>'{source_basis_json,segment_key}', '')), '') AS component_segment_key,
          NULLIF(BTRIM(COALESCE(component_element.value->>'segment_stable_key', component_element.value#>>'{source_basis_json,segment_stable_key}', '')), '') AS component_segment_stable_key
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(base_rows.line_json->'case_components') = 'array' THEN base_rows.line_json->'case_components'
            ELSE '[]'::jsonb
          END
        ) WITH ORDINALITY AS component_element(value, ordinality)
        WHERE component_element.value IS NOT NULL
          AND jsonb_typeof(component_element.value) = 'object'
          AND NOT (
            UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
            AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          )
      ), matched_components AS (
        SELECT component_candidates.value, component_candidates.ordinality
        FROM component_candidates
        CROSS JOIN segment_identity
        WHERE (
          segment_identity.work_date IS NOT NULL
          AND (
            component_candidates.component_work_date = segment_identity.work_date
            OR (component_candidates.component_key_type = 'TS_DAY' AND component_candidates.component_key_value = segment_identity.work_date)
          )
        )
        OR (segment_identity.segment_id IS NOT NULL AND component_candidates.component_segment_id = segment_identity.segment_id)
        OR (segment_identity.segment_key IS NOT NULL AND component_candidates.component_segment_key = segment_identity.segment_key)
        OR (segment_identity.segment_stable_key IS NOT NULL AND component_candidates.component_segment_stable_key = segment_identity.segment_stable_key)
      )
      SELECT COALESCE(
        (SELECT jsonb_agg(matched_components.value ORDER BY matched_components.ordinality) FROM matched_components),
        (SELECT jsonb_agg(component_candidates.value ORDER BY component_candidates.ordinality) FROM component_candidates),
        '[]'::jsonb
      ) AS segment_case_components_json
    ) AS segment_case_components
    WHERE base_rows.target_section = 'canonical_preview_lines'
      AND base_rows.line_type = 'TIMESHEET_PAYMENT'
      AND LOWER(BTRIM(COALESCE(base_rows.line_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND ROUND(COALESCE(segment_rows.segment_amount_ex_vat, 0), 2) <> 0
  ), timesheet_non_segment_rows AS (
    SELECT
      base_rows.base_ordinal,
      base_rows.candidate_id,
      base_rows.timesheet_id,
      base_rows.target_section,
      base_rows.parent_line_key,
      'non_segment:total'::text AS split_suffix,
      900000::bigint AS split_ordinal,
      'SEGMENT_DELTA'::text AS item_type,
      'TS_TOTAL'::text AS key_type_hint,
      'TOTAL'::text AS key_value_hint,
      '{}'::jsonb AS segment_json,
      jsonb_strip_nulls(
        (
          base_rows.line_json
          - 'case_components'
          - 'component_key_type'
          - 'component_key_value'
          - 'frozen_component_key_type'
          - 'frozen_component_key_value'
          - 'frozen_component_classification'
          - 'frozen_component_snapshot_json'
          - 'source_basis_json'
          - 'frozen_source_basis_json'
        )
        || jsonb_build_object(
          'source_kind', 'VALID_PREVIEW_LINE',
          'preview_row_id', base_rows.parent_line_key || ':non_segment:total',
          'line_id', base_rows.parent_line_key || ':non_segment:total',
          'line_key', base_rows.parent_line_key || ':non_segment:total',
          'row_key', base_rows.parent_line_key || ':non_segment:total'
        )
        || jsonb_build_object(
          'presentation_role', 'CHILD',
          'presentation_parent_line_id', base_rows.parent_line_key,
          'amount_ex_vat', non_segment_amounts.non_segment_amount_ex_vat,
          'amount_display', non_segment_amounts.non_segment_amount_ex_vat,
          'section_amount_ex_vat', non_segment_amounts.non_segment_amount_ex_vat,
          'section_amount_display', non_segment_amounts.non_segment_amount_ex_vat
        )
        || jsonb_build_object(
          'segment_rows', '[]'::jsonb,
          'section_segment_rows', '[]'::jsonb,
          'segment_count', 0,
          'section_segment_count', 0,
          'section_non_segment_amount_ex_vat', non_segment_amounts.non_segment_amount_ex_vat,
          'source_basis_json', jsonb_build_object('component_key_type', 'TS_TOTAL', 'component_key_value', 'TOTAL')
        )
        || jsonb_build_object(
          'case_components', non_segment_case_components.non_fixed_case_components_json
        )
      ) AS seed_line_json
    FROM base_rows
    CROSS JOIN LATERAL (
      SELECT
        ROUND(COALESCE(
          CASE WHEN COALESCE(base_rows.line_json->>'section_non_segment_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (base_rows.line_json->>'section_non_segment_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN jsonb_array_length(COALESCE(base_rows.line_json->'section_segment_rows', base_rows.line_json->'segment_rows', '[]'::jsonb)) = 0
                 AND COALESCE(base_rows.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
               THEN (base_rows.line_json->>'amount_ex_vat')::numeric ELSE NULL::numeric END,
          0::numeric
        ), 2) AS gross_non_segment_amount_ex_vat
    ) AS gross_non_segment_amounts
    CROSS JOIN LATERAL (
      SELECT
        ROUND(COALESCE(SUM(
          CASE
            WHEN COALESCE(base_rows.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
             AND (base_rows.line_json->>'amount_ex_vat')::numeric < 0
            THEN -ABS(ROUND(COALESCE(
              CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'preview_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'allocated_source_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'source_pay_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'remaining_source_amount')::numeric ELSE NULL::numeric END,
              0::numeric
            ), 2))
            ELSE ABS(ROUND(COALESCE(
              CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'preview_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'allocated_source_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'source_pay_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'remaining_source_amount')::numeric ELSE NULL::numeric END,
              0::numeric
            ), 2))
          END
        ), 0::numeric), 2) AS explicit_reimbursement_amount_ex_vat
      FROM jsonb_array_elements(COALESCE(base_rows.line_json->'case_components', '[]'::jsonb)) AS component_element(value)
      WHERE base_rows.line_type = 'TIMESHEET_PAYMENT'
        AND component_element.value IS NOT NULL
        AND jsonb_typeof(component_element.value) = 'object'
        AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
        AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
        AND (
          UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) <> 'EXPENSE_CODE'
          OR UPPER(BTRIM(COALESCE(component_element.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
        )
    ) AS explicit_component_amounts
    CROSS JOIN LATERAL (
      SELECT ROUND(
        COALESCE(gross_non_segment_amounts.gross_non_segment_amount_ex_vat, 0)
        - COALESCE(explicit_component_amounts.explicit_reimbursement_amount_ex_vat, 0),
        2
      ) AS non_segment_amount_ex_vat
    ) AS non_segment_amounts
    CROSS JOIN LATERAL (
      SELECT COALESCE(
        jsonb_agg(component_element.value ORDER BY component_element.ordinality),
        '[]'::jsonb
      ) AS non_fixed_case_components_json
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(base_rows.line_json->'case_components') = 'array' THEN base_rows.line_json->'case_components'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS component_element(value, ordinality)
      WHERE NOT (
        component_element.value IS NOT NULL
        AND jsonb_typeof(component_element.value) = 'object'
        AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
        AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
      )
    ) AS non_segment_case_components
    WHERE base_rows.target_section = 'canonical_preview_lines'
      AND base_rows.line_type = 'TIMESHEET_PAYMENT'
      AND LOWER(BTRIM(COALESCE(base_rows.line_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND NOT (
        COALESCE(base_rows.has_resolved_rate, false)
        AND COALESCE(base_rows.has_timesheet_segment_rows, false)
        AND COALESCE(base_rows.resolved_segment_rows_replace_source_total, false)
      )
      AND ROUND(COALESCE(non_segment_amounts.non_segment_amount_ex_vat, 0), 2) <> 0
  ), finance_component_rows AS (
    SELECT
      base_rows.base_ordinal,
      base_rows.candidate_id,
      component_timesheet.timesheet_id,
      component_identity.component_target_section as target_section,
      component_identity.component_parent_line_key as parent_line_key,
      component_identity.component_split_suffix as split_suffix,
      component_identity.component_split_ordinal as split_ordinal,
      CASE
        WHEN base_rows.line_type = 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT_RECOVERY'
        WHEN base_rows.line_type = 'UNDERPAYMENT_PAYMENT' THEN 'UNDERPAYMENT_PAYMENT'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) IN ('TS_DAY', 'TS_TOTAL') THEN 'SEGMENT_DELTA'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) = 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) = 'EXPENSE_CODE'
         AND UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_value', '')), '')) = 'MILEAGE' THEN 'MILEAGE_DELTA'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE') THEN 'EXPENSE_DELTA'
        ELSE base_rows.line_type
      END AS item_type,
      UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) AS key_type_hint,
      NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_value', '')), '') AS key_value_hint,
      '{}'::jsonb AS segment_json,
      jsonb_strip_nulls(
        (
          base_rows.line_json
          - 'case_components'
          - 'component_key_type'
          - 'component_key_value'
          - 'frozen_component_key_type'
          - 'frozen_component_key_value'
          - 'frozen_component_classification'
          - 'frozen_component_snapshot_json'
          - 'source_basis_json'
          - 'frozen_source_basis_json'
        )
        || jsonb_build_object(
          'source_kind', case when component_identity.component_target_section = 'internal_only' then 'INTERNAL_ONLY' else 'VALID_PREVIEW_LINE' end,
          'preview_row_id', component_identity.component_line_key,
          'line_id', component_identity.component_line_key,
          'line_key', component_identity.component_line_key,
          'row_key', component_identity.component_line_key,
          'target_section', component_identity.component_target_section,
          'section', component_identity.component_target_section
        )
        || jsonb_build_object(
          'presentation_role', 'CHILD',
          'presentation_parent_line_id', component_identity.component_parent_line_key,
          'presentation_section', case
            when component_identity.component_target_section = 'canonical_preview_lines' then 'READY_TO_PAY'
            when component_identity.component_target_section = 'blocked_for_pay' then 'BLOCKED_FOR_PAY'
            else 'INTERNAL_ONLY'
          end,
          'item_type', CASE
            WHEN base_rows.line_type = 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT_RECOVERY'
            WHEN base_rows.line_type = 'UNDERPAYMENT_PAYMENT' THEN 'UNDERPAYMENT_PAYMENT'
            WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) IN ('TS_DAY', 'TS_TOTAL') THEN 'SEGMENT_DELTA'
            WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) = 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
            WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) = 'EXPENSE_CODE'
             AND UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_value', '')), '')) = 'MILEAGE' THEN 'MILEAGE_DELTA'
            WHEN UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE') THEN 'EXPENSE_DELTA'
            ELSE base_rows.line_type
          END,
          'finance_component_id', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'finance_component_id', '')), ''),
          'component_key_type', component_identity.component_key_type,
          'component_key_value', component_identity.component_key_value,
          'source_ref', component_identity.expense_source_ref,
          'expense_code', case when component_identity.is_expense_component then component_identity.component_key_value else null end,
          'source_basis_fingerprint', case when component_identity.is_expense_component then component_identity.source_basis_fingerprint else null end,
          'expense_source_basis_fingerprint', case when component_identity.is_expense_component then component_identity.source_basis_fingerprint else null end,
          'expense_source_basis_json', case when component_identity.is_expense_component then component_identity.expense_source_basis_json else null end
        )
        || case
          when component_identity.is_expense_component then
            jsonb_build_object(
              'snooze_identity', component_rows.component_json->'snooze_identity',
              'snooze_state', component_rows.component_json->'snooze_state',
              'expense_presentation_state', component_rows.component_json->>'expense_presentation_state',
              'expense_identity_stale', lower(btrim(coalesce(component_rows.component_json->>'expense_identity_stale', 'false'))) in ('true','t','1','yes','y','on'),
              'draftable', lower(btrim(coalesce(component_rows.component_json->>'draftable', 'false'))) in ('true','t','1','yes','y','on'),
              'is_ready_for_draft', lower(btrim(coalesce(component_rows.component_json->>'is_ready_for_draft', 'false'))) in ('true','t','1','yes','y','on'),
              'is_excluded_from_allocation', lower(btrim(coalesce(component_rows.component_json->>'is_excluded_from_allocation', 'true'))) in ('true','t','1','yes','y','on'),
              'selection_allowed', lower(btrim(coalesce(component_rows.component_json->>'selection_allowed', 'false'))) in ('true','t','1','yes','y','on')
            )
            || jsonb_build_object(
              'presentation_reason', case
                when component_identity.expense_identity_valid is not true then 'EXPENSE_SOURCE_IDENTITY_INVALID'
                when component_identity.component_target_section = 'blocked_for_pay'
                 and lower(btrim(coalesce(component_rows.component_json#>>'{snooze_state,state}', ''))) = 'stale_source_identity'
                  then 'EXPENSE_SOURCE_IDENTITY_STALE'
                when component_identity.component_target_section = 'blocked_for_pay' then 'DATED_EXPENSE_SNOOZE'
                when component_identity.component_target_section = 'internal_only' then 'INDEFINITE_EXPENSE_SNOOZE'
                else 'READY_TO_PAY'
              end,
              'blocked_reason_codes', case
                when component_identity.expense_identity_valid is not true then jsonb_build_array('EXPENSE_SOURCE_IDENTITY_INVALID')
                when component_identity.component_target_section = 'blocked_for_pay'
                 and upper(btrim(coalesce(component_rows.component_json#>>'{snooze_state,state}', ''))) = 'STALE_SOURCE_IDENTITY'
                  then jsonb_build_array('EXPENSE_SOURCE_IDENTITY_STALE')
                when component_identity.component_target_section = 'blocked_for_pay'
                  then jsonb_build_array('BLOCKED_DATED_EXPENSE_SNOOZE')
                else '[]'::jsonb
              end
            )
          else '{}'::jsonb
        end
        || jsonb_build_object(
          'case_components', jsonb_build_array(
            CASE
              WHEN ROUND(ABS(COALESCE(component_rows.source_amount_ex_vat, 0)), 2) > 0 THEN
                component_rows.component_json
                || jsonb_build_object(
                  'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                  'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                  'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                  'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat,
                  'frozen_source_amount', component_rows.source_amount_ex_vat
                )
                || jsonb_build_object(
                  'source_basis_json',
                  COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
                  || jsonb_build_object(
                    'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                    'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                    'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                    'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat
                  )
                )
              ELSE component_rows.component_json
            END
          )
        )
        || jsonb_build_object(
          'amount_ex_vat', component_rows.signed_component_amount_ex_vat,
          'amount_display', component_rows.signed_component_amount_ex_vat,
          'preview_amount_ex_vat', component_rows.signed_component_amount_ex_vat,
          'source_basis_json', CASE
            WHEN ROUND(ABS(COALESCE(component_rows.source_amount_ex_vat, 0)), 2) > 0 THEN
              COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
              || jsonb_build_object(
                'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat
              )
            ELSE COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
          END,
          'frozen_component_snapshot_json', CASE
            WHEN ROUND(ABS(COALESCE(component_rows.source_amount_ex_vat, 0)), 2) > 0 THEN
              component_rows.component_json
              || jsonb_build_object(
                'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat,
                'frozen_source_amount', component_rows.source_amount_ex_vat
              )
              || jsonb_build_object(
                'source_basis_json',
                COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
                || jsonb_build_object(
                  'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                  'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                  'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                  'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat
                )
              )
            ELSE component_rows.component_json
          END
        )
        || jsonb_build_object(
          'frozen_component_key_type', UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')),
          'frozen_component_key_value', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_value', '')), ''),
          'frozen_component_classification', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'classification', '')), ''),
          'frozen_source_basis_json', CASE
            WHEN ROUND(ABS(COALESCE(component_rows.source_amount_ex_vat, 0)), 2) > 0 THEN
              COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
              || jsonb_build_object(
                'source_pay_ex_vat', component_rows.source_amount_ex_vat,
                'source_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
                'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat
              )
            ELSE COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
          END,
          'frozen_source_amount', component_rows.source_amount_ex_vat
        )
        || jsonb_build_object(
          'source_entitlement_amount_ex_vat', component_rows.source_amount_ex_vat,
          'source_reservation_amount_ex_vat', component_rows.source_amount_ex_vat,
          'source_amount_ex_vat', component_rows.source_amount_ex_vat,
          'remaining_source_amount', component_rows.source_amount_ex_vat,
          'target_pay_ex_vat', component_rows.signed_component_amount_ex_vat
        )
        || jsonb_build_object(
          'saved_target_pay_method', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'saved_target_pay_method', '')), ''),
          'saved_resolution_mode', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'saved_resolution_mode', '')), ''),
          'saved_resolution_payload_json', COALESCE(component_rows.component_json->'saved_resolution_payload_json', '{}'::jsonb),
          'saved_resolution_result_json', COALESCE(component_rows.component_json->'saved_resolution_result_json', '{}'::jsonb)
        )
        || jsonb_build_object(
          'resolution_mode', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'saved_resolution_mode', '')), ''),
          'resolution_payload_json', COALESCE(component_rows.component_json->'saved_resolution_payload_json', '{}'::jsonb),
          'resolution_result_json', COALESCE(component_rows.component_json->'saved_resolution_result_json', '{}'::jsonb),
          'frozen_resolution_mode', NULLIF(BTRIM(COALESCE(component_rows.component_json->>'saved_resolution_mode', '')), ''),
          'frozen_resolution_payload_json', COALESCE(component_rows.component_json->'saved_resolution_payload_json', '{}'::jsonb),
          'frozen_resolution_result_json', COALESCE(component_rows.component_json->'saved_resolution_result_json', '{}'::jsonb)
        )
      ) AS seed_line_json
    FROM base_rows
    CROSS JOIN LATERAL (
      SELECT
        component_element.value AS component_json,
        component_element.ordinality::integer AS component_ord,
        ROUND(COALESCE(
          CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'preview_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'allocated_source_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'source_pay_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'remaining_source_amount')::numeric ELSE NULL::numeric END,
          0::numeric
        ), 2) AS unsigned_component_amount_ex_vat,
        ROUND(COALESCE(
          CASE WHEN COALESCE(component_element.value->>'source_reservation_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_reservation_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_reservation_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_amount')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_amount')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_entitlement_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_entitlement_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_entitlement_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'pay_outstanding_clamped_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'pay_outstanding_clamped_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'pay_outstanding_clamped_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'pay_outstanding_available_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'pay_outstanding_available_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'pay_outstanding_available_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'remaining_source_amount')::numeric), 2) > 0 THEN ABS((component_element.value->>'remaining_source_amount')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value#>>'{source_basis_json,source_reservation_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value#>>'{source_basis_json,source_reservation_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((component_element.value#>>'{source_basis_json,source_reservation_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value#>>'{source_basis_json,source_entitlement_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value#>>'{source_basis_json,source_entitlement_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((component_element.value#>>'{source_basis_json,source_entitlement_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value#>>'{source_basis_json,source_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value#>>'{source_basis_json,source_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((component_element.value#>>'{source_basis_json,source_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value#>>'{source_basis_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value#>>'{source_basis_json,source_pay_ex_vat}')::numeric), 2) > 0 THEN ABS((component_element.value#>>'{source_basis_json,source_pay_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_pay_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_pay_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'source_pay_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'source_pay_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'source_pay_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED' AND COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED' AND COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((component_element.value->>'target_pay_ex_vat')::numeric), 2) > 0 THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
          0::numeric
        ), 2) AS source_amount_ex_vat,
        CASE
          WHEN COALESCE(base_rows.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
           AND (base_rows.line_json->>'amount_ex_vat')::numeric < 0
          THEN -ABS(ROUND(COALESCE(
            CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'preview_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'allocated_source_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'source_pay_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'remaining_source_amount')::numeric ELSE NULL::numeric END,
            0::numeric
          ), 2))
          ELSE ABS(ROUND(COALESCE(
            CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'preview_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'allocated_source_due_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'source_pay_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN COALESCE(component_element.value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (component_element.value->>'remaining_source_amount')::numeric ELSE NULL::numeric END,
            0::numeric
          ), 2))
        END AS signed_component_amount_ex_vat
      FROM jsonb_array_elements(COALESCE(base_rows.line_json->'case_components', '[]'::jsonb)) WITH ORDINALITY AS component_element(value, ordinality)
      WHERE component_element.value IS NOT NULL
        AND jsonb_typeof(component_element.value) = 'object'
    ) AS component_rows
    CROSS JOIN LATERAL (
      SELECT
        UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_type', '')), '')) AS component_key_type,
        UPPER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'component_key_value', component_rows.component_json->>'expense_code', '')), '')) AS component_key_value,
        CASE
          WHEN jsonb_typeof(component_rows.component_json->'expense_source_basis_json') = 'object'
            THEN COALESCE(component_rows.component_json->'expense_source_basis_json', '{}'::jsonb)
          WHEN jsonb_typeof(component_rows.component_json->'source_basis_json') = 'object'
            THEN COALESCE(component_rows.component_json->'source_basis_json', '{}'::jsonb)
          ELSE '{}'::jsonb
        END AS expense_source_basis_json,
        LOWER(NULLIF(BTRIM(COALESCE(
          component_rows.component_json->>'source_basis_fingerprint',
          component_rows.component_json->>'expense_source_basis_fingerprint',
          ''
        )), '')) AS source_basis_fingerprint,
        LOWER(NULLIF(BTRIM(COALESCE(component_rows.component_json->>'source_ref', '')), '')) AS expense_source_ref
    ) AS component_contract
    CROSS JOIN LATERAL (
      SELECT
        (component_contract.component_key_type = 'EXPENSE_CODE') AS is_expense_component,
        (
          component_contract.component_key_type = 'EXPENSE_CODE'
          AND component_contract.component_key_value IN ('EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE')
          AND component_contract.source_basis_fingerprint ~ '^[0-9a-f]{32}$'
          AND component_contract.expense_source_ref ~ (
            '^timesheet-expense:' || base_rows.timesheet_id::text ||
            ':' || lower(component_contract.component_key_value) ||
            ':' || component_contract.source_basis_fingerprint || '$'
          )
          AND md5(COALESCE(component_contract.expense_source_basis_json, '{}'::jsonb)::text)
              = component_contract.source_basis_fingerprint
        ) AS expense_identity_valid,
        component_contract.component_key_type,
        component_contract.component_key_value,
        component_contract.expense_source_basis_json,
        component_contract.source_basis_fingerprint,
        component_contract.expense_source_ref,
        CASE
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
            THEN base_rows.timesheet_id::text
          ELSE base_rows.parent_line_key
        END AS component_parent_line_key,
        CASE
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
            THEN 'component:expense:' || md5(component_contract.component_key_value)
          ELSE 'component:' || COALESCE(NULLIF(BTRIM(component_rows.component_json->>'finance_component_id'), ''), component_rows.component_ord::text)
        END AS component_split_suffix,
        CASE
          WHEN component_contract.component_key_type = 'EXPENSE_CODE' THEN
            800000::bigint + CASE component_contract.component_key_value
              WHEN 'EXPENSES' THEN 1
              WHEN 'TRAVEL' THEN 2
              WHEN 'ACCOMMODATION' THEN 3
              WHEN 'OTHER' THEN 4
              WHEN 'MILEAGE' THEN 5
              ELSE 99
            END
          ELSE component_rows.component_ord::bigint
        END AS component_split_ordinal,
        CASE
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
           AND NOT (
             component_contract.component_key_value IN ('EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE')
             AND component_contract.source_basis_fingerprint ~ '^[0-9a-f]{32}$'
             AND component_contract.expense_source_ref ~ (
               '^timesheet-expense:' || base_rows.timesheet_id::text ||
               ':' || lower(component_contract.component_key_value) ||
               ':' || component_contract.source_basis_fingerprint || '$'
             )
             AND md5(COALESCE(component_contract.expense_source_basis_json, '{}'::jsonb)::text)
                 = component_contract.source_basis_fingerprint
           ) THEN 'blocked_for_pay'
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
           AND UPPER(BTRIM(COALESCE(component_rows.component_json->>'presentation_section', ''))) = 'READY_TO_PAY'
            THEN 'canonical_preview_lines'
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
           AND UPPER(BTRIM(COALESCE(component_rows.component_json->>'presentation_section', ''))) = 'BLOCKED_FOR_PAY'
            THEN 'blocked_for_pay'
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
            THEN 'internal_only'
          ELSE base_rows.target_section
        END AS component_target_section,
        CASE
          WHEN component_contract.component_key_type = 'EXPENSE_CODE'
            THEN base_rows.timesheet_id::text || ':component:expense:' || md5(component_contract.component_key_value)
          ELSE base_rows.parent_line_key || ':component:' || COALESCE(NULLIF(BTRIM(component_rows.component_json->>'finance_component_id'), ''), component_rows.component_ord::text)
        END AS component_line_key
    ) AS component_identity
    CROSS JOIN LATERAL (
      SELECT base_rows.timesheet_id AS timesheet_id
    ) AS component_timesheet
    WHERE jsonb_array_length(COALESCE(base_rows.line_json->'case_components', '[]'::jsonb)) > 0
      AND ROUND(COALESCE(component_rows.signed_component_amount_ex_vat, 0), 2) <> 0
      AND (
        (
          component_identity.is_expense_component
          AND base_rows.line_type = 'TIMESHEET_PAYMENT'
          AND base_rows.target_section = component_identity.component_target_section
        )
        OR
        (
          NOT component_identity.is_expense_component
          AND base_rows.target_section = 'canonical_preview_lines'
          AND (
            base_rows.line_type <> 'TIMESHEET_PAYMENT'
            OR (
              base_rows.line_type = 'TIMESHEET_PAYMENT'
              AND component_identity.component_key_type = 'ADDITIONAL_CODE'
              AND component_identity.component_key_value IS NOT NULL
            )
          )
          AND LOWER(BTRIM(COALESCE(base_rows.line_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
      )
  ), hidden_expense_component_candidates AS (
    SELECT DISTINCT ON (hidden_base.timesheet_id, hidden_base.expense_code)
      hidden_base.candidate_id,
      hidden_base.timesheet_id,
      hidden_base.booking_id,
      hidden_base.ts_role,
      hidden_base.ts_band,
      hidden_base.client_id,
      hidden_base.client_name,
      hidden_base.week_ending_date,
      hidden_base.candidate_pay_method,
      hidden_base.cand_tms_ref,
      hidden_base.cand_display_name,
      hidden_base.expense_code,
      hidden_base.expense_label,
      hidden_base.expense_item_type,
      hidden_base.expense_source_ref,
      hidden_base.source_basis_fingerprint,
      hidden_base.expense_source_basis_json,
      hidden_base.component_amount_ex_vat,
      hidden_base.component_json,
      hidden_base.component_ordinal
    FROM (
      SELECT
        canonical_timesheet.candidate_id,
        canonical_timesheet.timesheet_id,
        canonical_timesheet.booking_id,
        canonical_timesheet.ts_role,
        canonical_timesheet.ts_band,
        canonical_timesheet.client_id,
        canonical_timesheet.client_name,
        canonical_timesheet.week_ending_date,
        canonical_timesheet.candidate_pay_method,
        canonical_timesheet.cand_tms_ref,
        canonical_timesheet.cand_display_name,
        UPPER(NULLIF(BTRIM(COALESCE(
          component_element.value->>'component_key_value',
          component_element.value->>'expense_code',
          ''
        )), '')) AS expense_code,
        COALESCE(
          NULLIF(BTRIM(COALESCE(component_element.value->>'expense_label', '')), ''),
          CASE UPPER(NULLIF(BTRIM(COALESCE(
            component_element.value->>'component_key_value',
            component_element.value->>'expense_code',
            ''
          )), ''))
            WHEN 'EXPENSES' THEN 'Expenses'
            WHEN 'TRAVEL' THEN 'Travel'
            WHEN 'ACCOMMODATION' THEN 'Accommodation'
            WHEN 'OTHER' THEN 'Other'
            WHEN 'MILEAGE' THEN 'Mileage'
            ELSE NULL::text
          END
        ) AS expense_label,
        CASE
          WHEN UPPER(NULLIF(BTRIM(COALESCE(
            component_element.value->>'component_key_value',
            component_element.value->>'expense_code',
            ''
          )), '')) = 'MILEAGE' THEN 'MILEAGE_DELTA'
          ELSE 'EXPENSE_DELTA'
        END AS expense_item_type,
        LOWER(NULLIF(BTRIM(COALESCE(component_element.value->>'source_ref', '')), '')) AS expense_source_ref,
        LOWER(NULLIF(BTRIM(COALESCE(
          component_element.value->>'source_basis_fingerprint',
          component_element.value->>'expense_source_basis_fingerprint',
          ''
        )), '')) AS source_basis_fingerprint,
        CASE
          WHEN jsonb_typeof(component_element.value->'expense_source_basis_json') = 'object'
            THEN COALESCE(component_element.value->'expense_source_basis_json', '{}'::jsonb)
          WHEN jsonb_typeof(component_element.value->'source_basis_json') = 'object'
            THEN COALESCE(component_element.value->'source_basis_json', '{}'::jsonb)
          ELSE '{}'::jsonb
        END AS expense_source_basis_json,
        ROUND(COALESCE(
          CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (component_element.value->>'ready_preview_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (component_element.value->>'preview_component_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (component_element.value->>'component_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (component_element.value->>'target_pay_ex_vat')::numeric ELSE NULL::numeric END,
          0::numeric
        ), 2) AS component_amount_ex_vat,
        component_element.value AS component_json,
        component_element.ordinality::integer AS component_ordinal
      FROM pg_temp.canonical_timesheet_lines AS canonical_timesheet
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(canonical_timesheet.case_components_json) = 'array'
            THEN COALESCE(canonical_timesheet.case_components_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS component_element(value, ordinality)
      WHERE component_element.value IS NOT NULL
        AND jsonb_typeof(component_element.value) = 'object'
        AND UPPER(BTRIM(COALESCE(component_element.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
        AND UPPER(BTRIM(COALESCE(component_element.value->>'presentation_section', ''))) = 'INTERNAL_ONLY'
        AND UPPER(BTRIM(COALESCE(component_element.value->>'expense_presentation_state', ''))) = 'HIDDEN_INDEFINITE'
    ) AS hidden_base
    WHERE hidden_base.expense_code IN ('EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE')
      AND hidden_base.source_basis_fingerprint ~ '^[0-9a-f]{32}$'
      AND hidden_base.expense_source_ref ~ (
        '^timesheet-expense:' || hidden_base.timesheet_id::text || ':' ||
        LOWER(hidden_base.expense_code) || ':' || hidden_base.source_basis_fingerprint || '$'
      )
      AND md5(COALESCE(hidden_base.expense_source_basis_json, '{}'::jsonb)::text)
          = hidden_base.source_basis_fingerprint
      AND ROUND(COALESCE(hidden_base.component_amount_ex_vat, 0), 2) <> 0
    ORDER BY
      hidden_base.timesheet_id,
      hidden_base.expense_code,
      hidden_base.component_ordinal
  ), hidden_expense_source_rows AS (
    SELECT
      (
        COALESCE((SELECT MAX(existing_base.base_ordinal) FROM base_rows AS existing_base), 0)
        + ROW_NUMBER() OVER (
          ORDER BY
            hidden_component.timesheet_id,
            CASE hidden_component.expense_code
              WHEN 'EXPENSES' THEN 1
              WHEN 'TRAVEL' THEN 2
              WHEN 'ACCOMMODATION' THEN 3
              WHEN 'OTHER' THEN 4
              WHEN 'MILEAGE' THEN 5
              ELSE 99
            END,
            hidden_component.expense_code
        )
      )::bigint AS base_ordinal,
      hidden_component.candidate_id,
      hidden_component.timesheet_id,
      'internal_only'::text AS target_section,
      hidden_component.timesheet_id::text AS parent_line_key,
      ('component:expense:' || md5(hidden_component.expense_code))::text AS split_suffix,
      (
        800000 + CASE hidden_component.expense_code
          WHEN 'EXPENSES' THEN 1
          WHEN 'TRAVEL' THEN 2
          WHEN 'ACCOMMODATION' THEN 3
          WHEN 'OTHER' THEN 4
          WHEN 'MILEAGE' THEN 5
          ELSE 99
        END
      )::bigint AS split_ordinal,
      hidden_component.expense_item_type AS item_type,
      'EXPENSE_CODE'::text AS key_type_hint,
      hidden_component.expense_code AS key_value_hint,
      '{}'::jsonb AS segment_json,
      jsonb_strip_nulls(
        jsonb_build_object(
          'source_kind', 'INTERNAL_ONLY',
          'preview_row_id', hidden_component.timesheet_id::text || ':component:expense:' || md5(hidden_component.expense_code),
          'line_id', hidden_component.timesheet_id::text || ':component:expense:' || md5(hidden_component.expense_code),
          'line_key', hidden_component.timesheet_id::text || ':component:expense:' || md5(hidden_component.expense_code),
          'row_key', hidden_component.timesheet_id::text || ':component:expense:' || md5(hidden_component.expense_code),
          'target_section', 'internal_only',
          'section', 'internal_only'
        )
        || jsonb_build_object(
          'candidate_id', hidden_component.candidate_id::text,
          'tms_ref', hidden_component.cand_tms_ref,
          'display_name', hidden_component.cand_display_name,
          'candidate_display_name', hidden_component.cand_display_name,
          'line_type', 'TIMESHEET_PAYMENT',
          'case_type', 'TIMESHEET_PAYMENT',
          'case_key', 'timesheet:' || hidden_component.timesheet_id::text,
          'timesheet_id', hidden_component.timesheet_id::text,
          'real_business_timesheet_id', hidden_component.timesheet_id::text,
          'booking_id', hidden_component.booking_id
        )
        || jsonb_build_object(
          'client_id', CASE WHEN hidden_component.client_id IS NULL THEN NULL ELSE hidden_component.client_id::text END,
          'client_name', hidden_component.client_name,
          'week_ending_date', CASE WHEN hidden_component.week_ending_date IS NULL THEN NULL ELSE hidden_component.week_ending_date::text END,
          'role', hidden_component.ts_role,
          'band', hidden_component.ts_band,
          'pay_channel', hidden_component.candidate_pay_method,
          'paye_treatment', CASE WHEN hidden_component.candidate_pay_method = 'PAYE' THEN 'GROSS_ADD' ELSE 'NONE' END,
          'route_type', 'NORMAL_PAYMENT',
          'item_type', hidden_component.expense_item_type,
          'component_key_type', 'EXPENSE_CODE',
          'component_key_value', hidden_component.expense_code
        )
        || jsonb_build_object(
          'economic_key', jsonb_build_object(
            'timesheet_id', hidden_component.timesheet_id::text,
            'key_type', 'EXPENSE_CODE',
            'key_value', hidden_component.expense_code
          ),
          'amount_ex_vat', hidden_component.component_amount_ex_vat,
          'amount_display', hidden_component.component_amount_ex_vat,
          'preview_amount_ex_vat', hidden_component.component_amount_ex_vat,
          'section_amount_ex_vat', hidden_component.component_amount_ex_vat,
          'section_amount_display', hidden_component.component_amount_ex_vat,
          'presentation_section', 'INTERNAL_ONLY',
          'presentation_role', 'CHILD',
          'presentation_parent_line_id', hidden_component.timesheet_id::text,
          'presentation_reason', 'INDEFINITE_EXPENSE_SNOOZE'
        )
        || jsonb_build_object(
          'draftable', false,
          'is_ready_for_draft', false,
          'is_excluded_from_allocation', true,
          'selection_allowed', false,
          'readiness_state', 'INTERNAL_ONLY',
          'source_ref', hidden_component.expense_source_ref,
          'expense_code', hidden_component.expense_code,
          'expense_label', hidden_component.expense_label,
          'source_basis_fingerprint', hidden_component.source_basis_fingerprint,
          'expense_source_basis_fingerprint', hidden_component.source_basis_fingerprint,
          'expense_source_basis_json', hidden_component.expense_source_basis_json,
          'source_basis_json', hidden_component.expense_source_basis_json
        )
        || jsonb_build_object(
          'snooze_identity', hidden_component.component_json->'snooze_identity',
          'snooze_state', hidden_component.component_json->'snooze_state',
          'expense_presentation_state', 'HIDDEN_INDEFINITE',
          'expense_identity_stale', false,
          'case_components', jsonb_build_array(hidden_component.component_json),
          'materialisation_suppressed', true,
          'materialisation_suppressed_reason', 'INDEFINITE_EXPENSE_SNOOZE',
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      ) AS seed_line_json
    FROM hidden_expense_component_candidates AS hidden_component
  ), parent_display_rows AS (
    SELECT
      base_rows.base_ordinal,
      base_rows.candidate_id,
      base_rows.timesheet_id,
      base_rows.target_section,
      base_rows.parent_line_key,
      'parent'::text AS split_suffix,
      0::bigint AS split_ordinal,
      CASE
        WHEN base_rows.line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD' THEN 'MANUAL_CARRY_FORWARD'
        ELSE base_rows.line_type
      END AS item_type,
      CASE
        WHEN base_rows.line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD' THEN 'MANUAL_CARRY_FORWARD'
        ELSE UPPER(NULLIF(BTRIM(COALESCE(base_rows.line_json->>'component_key_type', base_rows.line_json->>'frozen_component_key_type', '')), ''))
      END AS key_type_hint,
      CASE
        WHEN base_rows.line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD' THEN NULLIF(BTRIM(COALESCE(base_rows.line_json->>'manual_adjustment_carry_forward_id', REPLACE(base_rows.line_json->>'source_ref', 'carry_forward:', ''), base_rows.parent_line_key, '')), '')
        ELSE NULLIF(BTRIM(COALESCE(base_rows.line_json->>'component_key_value', base_rows.line_json->>'frozen_component_key_value', '')), '')
      END AS key_value_hint,
      '{}'::jsonb AS segment_json,
      jsonb_strip_nulls(
        base_rows.line_json
        || case
          when base_rows.line_type = 'TIMESHEET_PAYMENT'
           and base_rows.target_section = 'blocked_for_pay'
           and round(coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0), 2) <> 0
          then jsonb_build_object(
            'amount_ex_vat', round(
              coalesce(case when coalesce(base_rows.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (base_rows.line_json->>'amount_ex_vat')::numeric else null::numeric end, 0)
              - coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0),
              2
            ),
            'amount_display', round(
              coalesce(case when coalesce(base_rows.line_json->>'amount_display', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (base_rows.line_json->>'amount_display')::numeric else null::numeric end, 0)
              - coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0),
              2
            ),
            'section_amount_ex_vat', round(
              coalesce(case when coalesce(base_rows.line_json->>'section_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (base_rows.line_json->>'section_amount_ex_vat')::numeric else null::numeric end, 0)
              - coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0),
              2
            ),
            'section_amount_display', round(
              coalesce(case when coalesce(base_rows.line_json->>'section_amount_display', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (base_rows.line_json->>'section_amount_display')::numeric else null::numeric end, 0)
              - coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0),
              2
            ),
            'section_non_segment_amount_ex_vat', round(
              coalesce(case when coalesce(base_rows.line_json->>'section_non_segment_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (base_rows.line_json->>'section_non_segment_amount_ex_vat')::numeric else null::numeric end, 0)
              - coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0),
              2
            ),
            'split_expense_child_amount_ex_vat', round(coalesce(parent_expense_amounts.blocked_expense_amount_ex_vat, 0), 2)
          )
          else '{}'::jsonb
        end
        || jsonb_build_object(
          'source_kind', 'VALID_PREVIEW_LINE',
          'preview_row_id', base_rows.parent_line_key,
          'line_id', base_rows.parent_line_key,
          'line_key', base_rows.parent_line_key,
          'row_key', base_rows.parent_line_key,
          'target_section', base_rows.target_section
        )
        || CASE
          WHEN base_rows.target_section IN ('cases_resolutions', 'blocked_for_pay') THEN jsonb_build_object(
            'draftable', false,
            'is_ready_for_draft', false,
            'is_excluded_from_allocation', true,
            'selection_allowed', false
          )
          ELSE '{}'::jsonb
        END
      ) AS seed_line_json
    FROM base_rows
    LEFT JOIN LATERAL (
      SELECT round(coalesce(sum(
        coalesce(
          case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
          case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
          case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
          0::numeric
        )
      ), 0), 2) as blocked_expense_amount_ex_vat
      FROM jsonb_array_elements(
        CASE WHEN jsonb_typeof(base_rows.line_json->'case_components') = 'array'
          THEN COALESCE(base_rows.line_json->'case_components', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS expense_component(value)
      WHERE UPPER(BTRIM(COALESCE(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
        AND UPPER(BTRIM(COALESCE(expense_component.value->>'presentation_section', ''))) = 'BLOCKED_FOR_PAY'
    ) AS parent_expense_amounts ON true
    WHERE base_rows.target_section IN ('cases_resolutions', 'blocked_for_pay')
       OR (
          base_rows.target_section = 'canonical_preview_lines'
          AND base_rows.line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
       )
       OR (
          base_rows.target_section = 'canonical_preview_lines'
          AND base_rows.line_type <> 'TIMESHEET_PAYMENT'
          AND base_rows.line_type <> 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
          AND jsonb_array_length(COALESCE(base_rows.line_json->'case_components', '[]'::jsonb)) = 0
       )
  ), unioned_rows AS (
    SELECT * FROM timesheet_segment_rows
    UNION ALL
    SELECT * FROM timesheet_non_segment_rows
    UNION ALL
    SELECT * FROM finance_component_rows
    UNION ALL
    SELECT * FROM hidden_expense_source_rows
    UNION ALL
    SELECT * FROM parent_display_rows
  ), keyed_rows AS (
    SELECT
      unioned_rows.*,
      public.pay_workbench_preview_line_economic_key(
        p_line_json => unioned_rows.seed_line_json,
        p_timesheet_id => unioned_rows.timesheet_id,
        p_item_type => unioned_rows.item_type,
        p_segment_json => unioned_rows.segment_json,
        p_key_type_hint => unioned_rows.key_type_hint,
        p_key_value_hint => unioned_rows.key_value_hint
      ) AS economic_key_json
    FROM unioned_rows
  ), contracted_rows AS (
    SELECT
      keyed_rows.*,
      public.pay_workbench_preview_line_contract_ok(
        p_line_json => keyed_rows.seed_line_json,
        p_economic_key_json => keyed_rows.economic_key_json,
        p_target_section => keyed_rows.target_section
      ) AS contract_json
    FROM keyed_rows
  ), numbered_rows AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY contracted_rows.base_ordinal, contracted_rows.split_ordinal, contracted_rows.parent_line_key, contracted_rows.split_suffix
      )::bigint AS line_ordinal,
      contracted_rows.*
    FROM contracted_rows
    WHERE LOWER(BTRIM(COALESCE(contracted_rows.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       OR (
         contracted_rows.target_section = 'internal_only'
         AND UPPER(BTRIM(COALESCE(contracted_rows.seed_line_json->>'source_kind', ''))) = 'INTERNAL_ONLY'
         AND UPPER(BTRIM(COALESCE(contracted_rows.seed_line_json#>>'{snooze_identity,identity_type}', ''))) = 'TIMESHEET_EXPENSE'
         AND UPPER(BTRIM(COALESCE(contracted_rows.seed_line_json->>'expense_presentation_state', ''))) = 'HIDDEN_INDEFINITE'
         AND LOWER(BTRIM(COALESCE(contracted_rows.seed_line_json->>'source_ref', ''))) ~
             '^timesheet-expense:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:(expenses|travel|accommodation|other|mileage):[0-9a-f]{32}$'
       )
  )
  SELECT numbered_rows.*
  FROM numbered_rows;

  v_temp_transform_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  SELECT COUNT(*)::integer
  INTO v_source_canonical_preview_line_count
  FROM pg_temp.canonical_preview_lines AS source_count_rows
  WHERE source_count_rows.candidate_id = p_candidate_id;

  SELECT COUNT(*) FILTER (
           WHERE LOWER(BTRIM(COALESCE(materialisable_rows.contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
         )::integer
  INTO v_materialisable_source_line_count
  FROM pg_temp._tmp_pay_wb_preview_line_seed_source AS materialisable_rows;

  v_contract_rejected_count := GREATEST(COALESCE(v_source_canonical_preview_line_count, 0) - COALESCE(v_materialisable_source_line_count, 0), 0);

  SELECT COUNT(DISTINCT source_timesheet_rows.timesheet_id)::integer
  INTO v_timesheets_seen
  FROM pg_temp._tmp_pay_wb_preview_line_seed_source AS source_timesheet_rows
  WHERE source_timesheet_rows.timesheet_id IS NOT NULL;

  v_temp_counts_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_source_build_scope_timesheet_ids;
  CREATE TEMPORARY TABLE pg_temp._tmp_pay_wb_source_build_scope_timesheet_ids ON COMMIT DROP AS
    SELECT DISTINCT parsed_scope_ids.timesheet_id
    FROM (
      SELECT NULLIF(BTRIM(target_scope_values.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(v_collect_result->'targeted_timesheet_ids') = 'array' THEN v_collect_result->'targeted_timesheet_ids' ELSE '[]'::jsonb END
      ) AS target_scope_values(value)
      WHERE NULLIF(BTRIM(target_scope_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

      UNION

      SELECT NULLIF(BTRIM(linked_scope_values.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(v_collect_result->'linked_timesheet_ids') = 'array' THEN v_collect_result->'linked_timesheet_ids' ELSE '[]'::jsonb END
      ) AS linked_scope_values(value)
      WHERE NULLIF(BTRIM(linked_scope_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

      UNION

      SELECT source_rows.timesheet_id
      FROM pg_temp._tmp_pay_wb_preview_line_seed_source AS source_rows
      WHERE source_rows.timesheet_id IS NOT NULL
    ) AS parsed_scope_ids
    WHERE parsed_scope_ids.timesheet_id IS NOT NULL;

  v_scope_timesheet_temp_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  SELECT final_session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS final_session_row
  WHERE final_session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND_FINAL',
              'session_id', p_session_id::text,
              'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN_FINAL',
              'session_id', p_session_id::text,
              'status', v_session_row.status,
              'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
            )::text;
  END IF;

  IF v_session_version IS DISTINCT FROM COALESCE(v_session_row.version, 1) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE_FINAL',
              'payload_session_version', v_session_version,
              'current_session_version', COALESCE(v_session_row.version, 1),
              'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
            )::text;
  END IF;

  IF v_payload_snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH_FINAL',
              'payload_source_snapshot_run_id', CASE WHEN v_payload_snapshot_run_id IS NULL THEN NULL ELSE v_payload_snapshot_run_id::text END,
              'session_source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
              'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
            )::text;
  END IF;

  IF v_initial_session_signature IS DISTINCT FROM v_session_row.session_signature THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_STALE_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_STALE_FINAL',
              'initial_session_signature', v_initial_session_signature,
              'current_session_signature', v_session_row.session_signature,
              'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
            )::text;
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_final_source_change_seq
  FROM (SELECT 1) AS anchor
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF COALESCE(v_final_source_change_seq, 0) IS DISTINCT FROM COALESCE(v_source_change_seq, 0) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_STALE_FINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SOURCE_CHANGE_SEQ_STALE_FINAL',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'payload_source_change_seq', COALESCE(v_source_change_seq, 0),
              'current_source_change_seq', COALESCE(v_final_source_change_seq, 0)
            )::text;
  END IF;

  IF COALESCE(v_sync_attested, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RECONCILIATION_ATTESTATION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RECONCILIATION_ATTESTATION_REQUIRED',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text,
              'source_build_run_id', v_source_build_run_id::text
            )::text;
  END IF;

  v_final_revalidation_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF v_requested_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
     AND COALESCE(v_first_source_page, true) THEN
    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line_retire
    SET status = 'SUPERSEDED',
        updated_at_utc = v_now
    WHERE source_line_retire.session_id = p_session_id
      AND source_line_retire.candidate_id = p_candidate_id
      AND source_line_retire.status = 'CURRENT'
      AND NOT (
        source_line_retire.session_version = v_session_version
        AND source_line_retire.source_change_seq = v_source_change_seq
        AND source_line_retire.source_build_run_id = v_source_build_run_id
      );

    GET DIAGNOSTICS v_source_rows_superseded = ROW_COUNT;
  ELSIF v_actual_refresh_scope_kind = 'TARGETED_TIMESHEETS' THEN
    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line_retire
    SET status = 'SUPERSEDED',
        updated_at_utc = v_now
    WHERE source_line_retire.session_id = p_session_id
      AND source_line_retire.candidate_id = p_candidate_id
      AND source_line_retire.status = 'CURRENT'
      AND (
        source_line_retire.timesheet_id IN (
          SELECT scope_timesheet_ids.timesheet_id
          FROM pg_temp._tmp_pay_wb_source_build_scope_timesheet_ids AS scope_timesheet_ids
        )
        OR (
          source_line_retire.timesheet_id IS NULL
          AND EXISTS (
            SELECT 1
            FROM pg_temp._tmp_pay_wb_preview_line_seed_source AS null_scope_source_rows
            WHERE null_scope_source_rows.timesheet_id IS NULL
          )
        )
      )
      AND NOT (
        source_line_retire.session_version = v_session_version
        AND source_line_retire.source_change_seq = v_source_change_seq
        AND source_line_retire.source_build_run_id = v_source_build_run_id
      );

    GET DIAGNOSTICS v_source_rows_superseded = ROW_COUNT;
  END IF;

  v_source_retire_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  WITH source_rows_to_upsert AS (
    SELECT
      p_session_id AS session_id,
      source_rows.candidate_id AS candidate_id,
      v_session_version AS session_version,
      v_source_change_seq AS source_change_seq,
      v_source_build_run_id AS source_build_run_id,
      (COALESCE(v_cursor_source_ordinal_base, 0) + source_rows.line_ordinal)::bigint AS source_ordinal,
      source_rows.parent_line_key || ':' || source_rows.split_suffix AS line_key,
      source_rows.parent_line_key AS parent_line_key,
      source_rows.split_suffix AS split_suffix,
      source_rows.timesheet_id AS timesheet_id,
      source_rows.target_section AS section,
      jsonb_strip_nulls(
        COALESCE(source_rows.seed_line_json, '{}'::jsonb)
        || jsonb_build_object(
          'source_function', 'pay_workbench_candidate_source_build_chunk',
          'source_kind', CASE WHEN source_rows.target_section = 'internal_only' THEN 'INTERNAL_ONLY' ELSE 'VALID_PREVIEW_LINE' END,
          'session_id', p_session_id::text,
          'session_version', v_session_version,
          'candidate_id', source_rows.candidate_id::text,
          'source_build_run_id', v_source_build_run_id::text,
          'source_change_seq', v_source_change_seq,
          'source_ordinal', (COALESCE(v_cursor_source_ordinal_base, 0) + source_rows.line_ordinal),
          'line_key', source_rows.parent_line_key || ':' || source_rows.split_suffix,
          'line_ordinal', (COALESCE(v_cursor_source_ordinal_base, 0) + source_rows.line_ordinal)
        )
        || jsonb_build_object(
          'target_section', source_rows.target_section,
          'section', source_rows.target_section,
          'economic_key', source_rows.economic_key_json,
          'preview_contract', source_rows.contract_json,
          'refresh_scope_kind', v_actual_refresh_scope_kind,
          'requested_refresh_scope_kind', v_requested_refresh_scope_kind,
          'actual_refresh_scope_kind', v_actual_refresh_scope_kind,
          'pay_channel_scope', v_pay_channel_scope,
          'overpayment_sync_attestation', COALESCE(v_sync_attestation, '{}'::jsonb),
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      ) AS source_row_json,
      COALESCE(source_rows.economic_key_json, '{}'::jsonb) AS economic_key_json,
      jsonb_strip_nulls(
        COALESCE(source_rows.contract_json, '{}'::jsonb)
        || jsonb_build_object(
          'overpayment_sync_attested', COALESCE(v_sync_attested, false),
          'overpayment_sync_result_code', v_sync_result_code,
          'overpayment_sync_scope_digest', v_sync_scope_digest,
          'overpayment_sync_negative_component_digest', v_sync_negative_digest,
          'overpayment_sync_settled_baseline_digest', v_sync_baseline_digest,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      ) AS contract_json,
      v_pay_channel_scope AS pay_channel_scope,
      v_actual_refresh_scope_kind AS refresh_scope_kind
    FROM pg_temp._tmp_pay_wb_preview_line_seed_source AS source_rows
    WHERE source_rows.candidate_id = p_candidate_id
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS source_write_guard
        WHERE source_write_guard.id = p_session_id
          AND UPPER(BTRIM(COALESCE(source_write_guard.status, ''))) = 'OPEN'
          AND source_write_guard.discarded_at_utc IS NULL
          AND COALESCE(source_write_guard.version, 1) = v_session_version
          AND source_write_guard.source_snapshot_run_id IS NOT DISTINCT FROM v_payload_snapshot_run_id
          AND source_write_guard.session_signature IS NOT DISTINCT FROM v_initial_session_signature
      )
  ), upserted_source_rows AS (
    INSERT INTO public.banking_pay_workbench_candidate_source_lines (
      session_id,
      candidate_id,
      session_version,
      source_change_seq,
      source_build_run_id,
      source_ordinal,
      line_key,
      parent_line_key,
      split_suffix,
      timesheet_id,
      section,
      source_row_json,
      economic_key_json,
      contract_json,
      pay_channel_scope,
      refresh_scope_kind,
      status,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      source_rows_to_upsert.session_id,
      source_rows_to_upsert.candidate_id,
      source_rows_to_upsert.session_version,
      source_rows_to_upsert.source_change_seq,
      source_rows_to_upsert.source_build_run_id,
      source_rows_to_upsert.source_ordinal,
      source_rows_to_upsert.line_key,
      source_rows_to_upsert.parent_line_key,
      source_rows_to_upsert.split_suffix,
      source_rows_to_upsert.timesheet_id,
      source_rows_to_upsert.section,
      source_rows_to_upsert.source_row_json,
      source_rows_to_upsert.economic_key_json,
      source_rows_to_upsert.contract_json,
      source_rows_to_upsert.pay_channel_scope,
      source_rows_to_upsert.refresh_scope_kind,
      'CURRENT',
      v_now,
      v_now
    FROM source_rows_to_upsert
    ON CONFLICT (
      session_id,
      candidate_id,
      session_version,
      source_change_seq,
      source_build_run_id,
      (COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid)),
      line_key
    ) WHERE status = 'CURRENT'
    DO UPDATE
    SET source_ordinal = EXCLUDED.source_ordinal,
        parent_line_key = EXCLUDED.parent_line_key,
        split_suffix = EXCLUDED.split_suffix,
        section = EXCLUDED.section,
        source_row_json = EXCLUDED.source_row_json,
        economic_key_json = EXCLUDED.economic_key_json,
        contract_json = EXCLUDED.contract_json,
        pay_channel_scope = EXCLUDED.pay_channel_scope,
        refresh_scope_kind = EXCLUDED.refresh_scope_kind,
        updated_at_utc = v_now
    RETURNING public.banking_pay_workbench_candidate_source_lines.id,
              public.banking_pay_workbench_candidate_source_lines.source_ordinal
  )
  SELECT COUNT(*)::integer,
         COALESCE(MAX(upserted_source_rows.source_ordinal), COALESCE(v_cursor_source_ordinal_base, 0))::bigint
  INTO v_source_rows_written,
       v_next_source_ordinal
  FROM upserted_source_rows;

  v_source_upsert_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF COALESCE(v_materialisable_source_line_count, 0) > 0
     AND COALESCE(v_source_rows_written, 0) = 0 THEN
    SELECT final_session_row.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions AS final_session_row
    WHERE final_session_row.id = p_session_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND_BEFORE_SOURCE_WRITE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_FOUND_BEFORE_SOURCE_WRITE',
                'session_id', p_session_id::text,
                'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
                'source_rows_written', COALESCE(v_source_rows_written, 0),
                'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0)
              )::text;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
       OR v_session_row.discarded_at_utc IS NOT NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN_BEFORE_SOURCE_WRITE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_NOT_OPEN_BEFORE_SOURCE_WRITE',
                'session_id', p_session_id::text,
                'status', v_session_row.status,
                'discarded_at_utc', v_session_row.discarded_at_utc,
                'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
                'source_rows_written', COALESCE(v_source_rows_written, 0),
                'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0)
              )::text;
    END IF;

    IF v_session_version IS DISTINCT FROM COALESCE(v_session_row.version, 1) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE_BEFORE_SOURCE_WRITE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_VERSION_STALE_BEFORE_SOURCE_WRITE',
                'payload_session_version', v_session_version,
                'current_session_version', COALESCE(v_session_row.version, 1),
                'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
                'source_rows_written', COALESCE(v_source_rows_written, 0),
                'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0)
              )::text;
    END IF;

    IF v_payload_snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH_BEFORE_SOURCE_WRITE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SNAPSHOT_RUN_ID_MISMATCH_BEFORE_SOURCE_WRITE',
                'payload_source_snapshot_run_id', CASE WHEN v_payload_snapshot_run_id IS NULL THEN NULL ELSE v_payload_snapshot_run_id::text END,
                'session_source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
                'source_rows_written', COALESCE(v_source_rows_written, 0),
                'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0)
              )::text;
    END IF;

    IF v_initial_session_signature IS DISTINCT FROM v_session_row.session_signature THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_STALE_BEFORE_SOURCE_WRITE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_SESSION_SIGNATURE_STALE_BEFORE_SOURCE_WRITE',
                'initial_session_signature', v_initial_session_signature,
                'current_session_signature', v_session_row.session_signature,
                'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
                'source_rows_written', COALESCE(v_source_rows_written, 0),
                'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0)
              )::text;
    END IF;
  END IF;

  v_source_write_guard_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  SELECT COUNT(*)::integer
  INTO v_current_source_row_count
  FROM public.banking_pay_workbench_candidate_source_lines AS current_source_rows
  WHERE current_source_rows.session_id = p_session_id
    AND current_source_rows.candidate_id = p_candidate_id
    AND current_source_rows.session_version = v_session_version
    AND current_source_rows.source_change_seq = v_source_change_seq
    AND current_source_rows.source_build_run_id = v_source_build_run_id
    AND current_source_rows.status = 'CURRENT';

  v_current_source_count_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF COALESCE(v_current_source_row_count, 0) > 0 THEN
    v_source_reconcile_result := jsonb_build_object(
      'ok', true,
      'deferred', true,
      'deferred_to', 'pay_workbench_complete_job',
      'reason', 'SOURCE_BUILD_CHUNK_AVOIDS_SHARED_SESSION_ROW_FINALISATION_LOCK',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'source_build_run_id', v_source_build_run_id::text,
      'source_change_seq', v_source_change_seq,
      'session_version', v_session_version,
      'source_rows_superseded', 0,
      'source_build_jobs_superseded', 0,
      'counter_reconciled', false,
      'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS'
    );

    v_reconciled_source_rows_superseded := 0;
    v_reconciled_source_build_jobs_superseded := 0;
  END IF;

  v_reconciliation_defer_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  IF COALESCE(v_has_more, false) AND v_source_cursor_out_json IS NOT NULL THEN
    v_next_cursor_json := COALESCE(v_source_cursor_out_json, '{}'::jsonb)
      || jsonb_build_object(
        'last_source_ordinal', COALESCE(v_next_source_ordinal, v_cursor_source_ordinal_base, 0),
        'source_build_run_id', v_source_build_run_id::text,
        'source_change_seq', v_source_change_seq,
        'session_version', v_session_version,
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
        'session_signature', v_session_row.session_signature,
        'source_page_limit', v_limit,
        'requested_refresh_scope_kind', v_requested_refresh_scope_kind
      );
  ELSE
    v_next_cursor_json := NULL::jsonb;
  END IF;

  v_cursor_advanced := v_next_cursor_json IS NOT NULL
    AND v_next_cursor_json IS DISTINCT FROM COALESCE(v_cursor_json, '{}'::jsonb);

  v_next_cursor_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);
  v_diag_phase_started_at := clock_timestamp();

  UPDATE public.banking_pay_workbench_session_scope AS scope_update
  SET status = CASE
        WHEN COALESCE(v_sync_attested, false) IS NOT TRUE THEN 'SOURCE_BUILD_PENDING'
        WHEN COALESCE(v_has_more, false) THEN 'SOURCE_BUILD_PENDING'
        WHEN COALESCE(v_current_source_row_count, 0) > 0 THEN 'SOURCE_READY'
        ELSE 'SOURCE_EMPTY'
      END,
      dirty = COALESCE(v_has_more, false) OR COALESCE(v_sync_attested, false) IS NOT TRUE,
      error_json = NULL::jsonb,
      updated_at_utc = v_now
  WHERE scope_update.session_id = p_session_id
    AND scope_update.candidate_id = p_candidate_id;

  v_scope_update_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);

  v_source_build_timing_json := jsonb_build_object(
    'timing_version', 'source_build_residual_v1',
    'complete', false,
    'source_build_run_id', v_source_build_run_id::text,
    'source_change_seq', v_source_change_seq,
    'session_version', v_session_version,
    'source_page_count', COALESCE(v_source_page_count, 0),
    'source_rows_written', COALESCE(v_source_rows_written, 0),
    'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0),
    'source_rows_seen', COALESCE(v_source_canonical_preview_line_count, 0),
    'phase_elapsed_ms', jsonb_build_object(
      'budget_apply', COALESCE(v_budget_apply_elapsed_ms, 0),
      'initial_validation', COALESCE(v_initial_validation_elapsed_ms, 0),
      'overpayment_sync', COALESCE(v_overpayment_sync_elapsed_ms, 0),
      'preview_context_build', COALESCE(v_preview_context_elapsed_ms, 0),
      'collect_call_wall', COALESCE(v_collect_call_wall_elapsed_ms, 0),
      'collect_reported', COALESCE(v_collect_elapsed_ms, 0),
      'canonical_call_wall', COALESCE(v_canonical_call_wall_elapsed_ms, 0),
      'canonical_reported', COALESCE(v_canonical_elapsed_ms, 0)
    )
    || jsonb_build_object(
      'temp_transform', COALESCE(v_temp_transform_elapsed_ms, 0),
      'temp_counts', COALESCE(v_temp_counts_elapsed_ms, 0),
      'scope_timesheet_temp', COALESCE(v_scope_timesheet_temp_elapsed_ms, 0),
      'final_revalidation', COALESCE(v_final_revalidation_elapsed_ms, 0),
      'source_retire', COALESCE(v_source_retire_elapsed_ms, 0),
      'source_upsert', COALESCE(v_source_upsert_elapsed_ms, 0),
      'source_write_guard', COALESCE(v_source_write_guard_elapsed_ms, 0),
      'current_source_count', COALESCE(v_current_source_count_elapsed_ms, 0)
    )
    || jsonb_build_object(
      'reconciliation_defer', COALESCE(v_reconciliation_defer_elapsed_ms, 0),
      'next_cursor', COALESCE(v_next_cursor_elapsed_ms, 0),
      'scope_update', COALESCE(v_scope_update_elapsed_ms, 0),
      'session_progress_update', COALESCE(v_session_progress_update_elapsed_ms, 0)
    )
  );

  v_diag_phase_started_at := clock_timestamp();

  BEGIN
    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS session_progress_lock
    WHERE session_progress_lock.id = p_session_id
      AND UPPER(BTRIM(COALESCE(session_progress_lock.status, ''))) = 'OPEN'
      AND session_progress_lock.discarded_at_utc IS NULL
      AND COALESCE(session_progress_lock.version, 1) = v_session_version
      AND session_progress_lock.source_snapshot_run_id IS NOT DISTINCT FROM v_payload_snapshot_run_id
      AND session_progress_lock.session_signature IS NOT DISTINCT FROM v_initial_session_signature
    FOR UPDATE NOWAIT;

    IF FOUND THEN
      UPDATE public.banking_pay_workbench_sessions AS session_update
      SET progress_state = 'REFRESHING_CANDIDATES',
          progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
            || CASE
              WHEN COALESCE(v_sync_completed, false) THEN jsonb_build_object(
                'overpayment_sync_completed_by_candidate',
                CASE
                  WHEN jsonb_typeof(session_update.progress_json->'overpayment_sync_completed_by_candidate') = 'object'
                    THEN COALESCE(session_update.progress_json->'overpayment_sync_completed_by_candidate', '{}'::jsonb)
                  ELSE '{}'::jsonb
                END
                || jsonb_build_object(
                  p_candidate_id::text,
                  COALESCE(v_sync_attestation, '{}'::jsonb)
                  || jsonb_build_object(
                    'completed_at_utc', v_now::text,
                    'session_progress_marker_only', true
                  )
                )
              )
              ELSE '{}'::jsonb
            END
            || jsonb_build_object(
              'phase', 'SOURCE_BUILDING',
              'last_source_build_candidate_id', p_candidate_id::text,
              'last_source_build_run_id', v_source_build_run_id::text,
              'last_source_change_seq', v_source_change_seq,
              'last_source_build_session_version', v_session_version,
              'last_source_rows_written', COALESCE(v_source_rows_written, 0),
              'last_current_source_row_count', COALESCE(v_current_source_row_count, 0),
              'last_source_rows_superseded', COALESCE(v_source_rows_superseded, 0),
              'last_source_build_jobs_superseded', COALESCE(v_reconciled_source_build_jobs_superseded, 0),
              'last_source_build_reconciliation', COALESCE(v_source_reconcile_result, '{}'::jsonb),
              'last_source_build_has_more', COALESCE(v_has_more, false),
              'last_source_build_at_utc', v_now::text,
              'status_text', 'Preparing payment source rows.',
              'next_recommended_action', CASE WHEN COALESCE(v_has_more, false) THEN 'BUILD_SOURCE_CHUNK' ELSE 'SEED_LINE_WORK_CHUNK' END
            )
            || jsonb_build_object(
              'last_source_build_collect_diagnostics', jsonb_strip_nulls(jsonb_build_object(
                'has_more', COALESCE(v_has_more, false),
                'source_page_count', COALESCE(v_source_page_count, 0),
                'source_remaining_after_cursor_count', COALESCE(v_source_remaining_after_cursor_count, 0),
                'collect_elapsed_ms', COALESCE(v_collect_elapsed_ms, 0),
                'fallback_used', COALESCE(v_fallback_used, false),
                'fallback_reason', v_fallback_reason,
                'candidate_filter_applied_early', COALESCE(v_candidate_filter_applied_early, false),
                'timesheet_filter_applied_early', COALESCE(v_timesheet_filter_applied_early, false),
                'large_aggregation_avoided', COALESCE(v_large_aggregation_avoided, false)
              )),
              'last_source_build_canonical_diagnostics', jsonb_strip_nulls(jsonb_build_object(
                'canonical_elapsed_ms', COALESCE(v_canonical_elapsed_ms, 0),
                'collect_called_inside_canonical', COALESCE(v_collect_called_inside_canonical, false)
              )),
              'last_source_build_timing', COALESCE(v_source_build_timing_json, '{}'::jsonb),
              'terminal_readiness_deferred', true,
              'terminal_readiness_deferred_to', 'pay_workbench_complete_job',
              'session_progress_update_locking', 'NOWAIT',
              'session_progress_update_applied', true
            ),
          progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE session_update.id = p_session_id
        AND UPPER(BTRIM(COALESCE(session_update.status, ''))) = 'OPEN'
        AND session_update.discarded_at_utc IS NULL
        AND COALESCE(session_update.version, 1) = v_session_version
        AND session_update.source_snapshot_run_id IS NOT DISTINCT FROM v_payload_snapshot_run_id
        AND session_update.session_signature IS NOT DISTINCT FROM v_initial_session_signature;

      GET DIAGNOSTICS v_session_progress_update_row_count = ROW_COUNT;
      v_session_progress_update_applied := COALESCE(v_session_progress_update_row_count, 0) > 0;
      v_session_progress_lock_skipped := COALESCE(v_session_progress_update_row_count, 0) <= 0;
    ELSE
      v_session_progress_update_applied := false;
      v_session_progress_lock_skipped := true;
    END IF;
  EXCEPTION
    WHEN lock_not_available THEN
      v_session_progress_update_applied := false;
      v_session_progress_lock_skipped := true;
      v_session_progress_update_row_count := 0;
  END;

  v_session_progress_update_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_diag_phase_started_at)) * 1000.0)::numeric, 3);

  v_total_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at_utc)) * 1000.0)::numeric, 3);
  v_residual_elapsed_ms := round(GREATEST(COALESCE(v_total_elapsed_ms, 0) - COALESCE(v_collect_elapsed_ms, 0) - COALESCE(v_canonical_elapsed_ms, 0), 0), 3);
  v_residual_wall_elapsed_ms := round(GREATEST(COALESCE(v_total_elapsed_ms, 0) - COALESCE(v_collect_call_wall_elapsed_ms, 0) - COALESCE(v_canonical_call_wall_elapsed_ms, 0), 0), 3);
  v_residual_measured_elapsed_ms := round((
    COALESCE(v_budget_apply_elapsed_ms, 0)
    + COALESCE(v_initial_validation_elapsed_ms, 0)
    + COALESCE(v_overpayment_sync_elapsed_ms, 0)
    + COALESCE(v_preview_context_elapsed_ms, 0)
    + COALESCE(v_temp_transform_elapsed_ms, 0)
    + COALESCE(v_temp_counts_elapsed_ms, 0)
    + COALESCE(v_scope_timesheet_temp_elapsed_ms, 0)
    + COALESCE(v_final_revalidation_elapsed_ms, 0)
    + COALESCE(v_source_retire_elapsed_ms, 0)
    + COALESCE(v_source_upsert_elapsed_ms, 0)
    + COALESCE(v_source_write_guard_elapsed_ms, 0)
    + COALESCE(v_current_source_count_elapsed_ms, 0)
    + COALESCE(v_reconciliation_defer_elapsed_ms, 0)
    + COALESCE(v_next_cursor_elapsed_ms, 0)
    + COALESCE(v_scope_update_elapsed_ms, 0)
    + COALESCE(v_session_progress_update_elapsed_ms, 0)
  )::numeric, 3);
  v_residual_unattributed_elapsed_ms := round(GREATEST(COALESCE(v_residual_wall_elapsed_ms, 0) - COALESCE(v_residual_measured_elapsed_ms, 0), 0), 3);

  v_source_build_timing_json := jsonb_build_object(
    'timing_version', 'source_build_residual_v1',
    'complete', true,
    'source_build_run_id', v_source_build_run_id::text,
    'source_change_seq', v_source_change_seq,
    'session_version', v_session_version,
    'source_page_count', COALESCE(v_source_page_count, 0),
    'source_rows_written', COALESCE(v_source_rows_written, 0),
    'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0),
    'source_rows_seen', COALESCE(v_source_canonical_preview_line_count, 0),
    'elapsed_summary_ms', jsonb_build_object(
      'total', COALESCE(v_total_elapsed_ms, 0),
      'collect_reported', COALESCE(v_collect_elapsed_ms, 0),
      'collect_call_wall', COALESCE(v_collect_call_wall_elapsed_ms, 0),
      'canonical_reported', COALESCE(v_canonical_elapsed_ms, 0),
      'canonical_call_wall', COALESCE(v_canonical_call_wall_elapsed_ms, 0),
      'residual_reported_basis', COALESCE(v_residual_elapsed_ms, 0),
      'residual_wall_basis', COALESCE(v_residual_wall_elapsed_ms, 0),
      'residual_measured_components', COALESCE(v_residual_measured_elapsed_ms, 0),
      'residual_unattributed', COALESCE(v_residual_unattributed_elapsed_ms, 0)
    )
  )
  || jsonb_build_object(
    'phase_elapsed_ms', jsonb_build_object(
      'budget_apply', COALESCE(v_budget_apply_elapsed_ms, 0),
      'initial_validation', COALESCE(v_initial_validation_elapsed_ms, 0),
      'overpayment_sync', COALESCE(v_overpayment_sync_elapsed_ms, 0),
      'preview_context_build', COALESCE(v_preview_context_elapsed_ms, 0),
      'collect_call_wall', COALESCE(v_collect_call_wall_elapsed_ms, 0),
      'collect_reported', COALESCE(v_collect_elapsed_ms, 0),
      'canonical_call_wall', COALESCE(v_canonical_call_wall_elapsed_ms, 0),
      'canonical_reported', COALESCE(v_canonical_elapsed_ms, 0)
    )
    || jsonb_build_object(
      'temp_transform', COALESCE(v_temp_transform_elapsed_ms, 0),
      'temp_counts', COALESCE(v_temp_counts_elapsed_ms, 0),
      'scope_timesheet_temp', COALESCE(v_scope_timesheet_temp_elapsed_ms, 0),
      'final_revalidation', COALESCE(v_final_revalidation_elapsed_ms, 0),
      'source_retire', COALESCE(v_source_retire_elapsed_ms, 0),
      'source_upsert', COALESCE(v_source_upsert_elapsed_ms, 0),
      'source_write_guard', COALESCE(v_source_write_guard_elapsed_ms, 0),
      'current_source_count', COALESCE(v_current_source_count_elapsed_ms, 0)
    )
    || jsonb_build_object(
      'reconciliation_defer', COALESCE(v_reconciliation_defer_elapsed_ms, 0),
      'next_cursor', COALESCE(v_next_cursor_elapsed_ms, 0),
      'scope_update', COALESCE(v_scope_update_elapsed_ms, 0),
      'session_progress_update', COALESCE(v_session_progress_update_elapsed_ms, 0)
    )
  );

  -- A full-live candidate build is paged. Correction-chain members can fall on
  -- different pages, so validating/materialising the coupled residual before
  -- the terminal page would reject a temporarily incomplete (but valid) run.
  -- The terminal page still fails closed before line-work seeding if any
  -- required component is absent.
  IF COALESCE(v_has_more, false) IS NOT TRUE THEN
    PERFORM public._ctms_materialise_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      v_source_build_run_id,
      v_now
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'stage', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'session_version', v_session_version,
    'source_change_seq', v_source_change_seq,
    'source_build_run_id', v_source_build_run_id::text,
    'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
    'session_signature', v_session_row.session_signature,
    'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
    'candidate_lock_scope', 'SESSION_SCOPE_ROW_FOR_UPDATE',
    'session_progress_update_applied', COALESCE(v_session_progress_update_applied, false),
    'session_progress_lock_skipped', COALESCE(v_session_progress_lock_skipped, false),
    'session_progress_update_row_count', COALESCE(v_session_progress_update_row_count, 0),
    'limit', v_limit,
    'refresh_scope_kind', v_actual_refresh_scope_kind,
    'requested_refresh_scope_kind', v_requested_refresh_scope_kind,
    'targeted_payload_received', COALESCE(v_targeted_payload_received, false),
    'targeted_timesheet_count', jsonb_array_length(COALESCE(v_collect_result->'targeted_timesheet_ids', v_targeted_timesheet_ids_json, '[]'::jsonb)),
    'linked_timesheet_count', jsonb_array_length(COALESCE(v_collect_result->'linked_timesheet_ids', v_linked_timesheet_ids_json, '[]'::jsonb))
  )
  || jsonb_build_object(
    'source_rows_written', COALESCE(v_source_rows_written, 0),
    'current_source_row_count', COALESCE(v_current_source_row_count, 0),
    'source_rows_superseded', COALESCE(v_source_rows_superseded, 0),
    'source_rows_superseded_by_reconciliation', COALESCE(v_reconciled_source_rows_superseded, 0),
    'source_build_failed_jobs_superseded', COALESCE(v_reconciled_source_build_jobs_superseded, 0),
    'source_build_reconciliation', COALESCE(v_source_reconcile_result, '{}'::jsonb),
    'source_rows_seen', COALESCE(v_source_canonical_preview_line_count, 0),
    'source_canonical_preview_line_count', COALESCE(v_source_canonical_preview_line_count, 0),
    'materialisable_source_line_count', COALESCE(v_materialisable_source_line_count, 0),
    'contract_rejected_count', COALESCE(v_contract_rejected_count, 0),
    'timesheets_seen', COALESCE(v_timesheets_seen, 0),
    'source_page_count', COALESCE(v_source_page_count, 0),
    'source_remaining_after_cursor_count', COALESCE(v_source_remaining_after_cursor_count, 0),
    'has_more', COALESCE(v_has_more, false),
    'next_cursor', v_next_cursor_json,
    'next_cursor_json', v_next_cursor_json,
    'cursor_in', COALESCE(v_source_cursor_in_json, v_cursor_json, '{}'::jsonb),
    'cursor_out', COALESCE(v_next_cursor_json, 'null'::jsonb),
    'cursor_advanced', COALESCE(v_cursor_advanced, false)
  )
  || jsonb_build_object(
    'fallback_used', COALESCE(v_fallback_used, false),
    'legacy_path_used', true,
    'fallback_from_delta', COALESCE(v_fallback_from_delta, false),
    'fallback_reason', COALESCE(v_delta_fallback_reason, v_fallback_reason),
    'projection_run_id', v_delta_projection_run_id_text,
    'projection_class', v_delta_projection_class,
    'candidate_filter_applied_early', COALESCE(v_candidate_filter_applied_early, false),
    'timesheet_filter_applied_early', COALESCE(v_timesheet_filter_applied_early, false),
    'large_aggregation_avoided', COALESCE(v_large_aggregation_avoided, false),
    'collect_called_inside_canonical', COALESCE(v_collect_called_inside_canonical, false),
    'overpayment_sync_completed', COALESCE(v_sync_completed, false),
    'overpayment_sync_attested', COALESCE(v_sync_attested, false),
    'overpayment_sync_attestation', COALESCE(v_sync_attestation, '{}'::jsonb),
    'overpayment_sync_result', COALESCE(v_sync_result, '{}'::jsonb),
    'collect_elapsed_ms', COALESCE(v_collect_elapsed_ms, 0),
    'canonical_elapsed_ms', COALESCE(v_canonical_elapsed_ms, 0),
    'classifier_elapsed_ms', COALESCE(v_classifier_elapsed_ms, 0),
    'classifier_timing_source', 'canonical_elapsed_ms',
    'total_elapsed_ms', COALESCE(v_total_elapsed_ms, 0),
    'collect_call_wall_elapsed_ms', COALESCE(v_collect_call_wall_elapsed_ms, 0),
    'canonical_call_wall_elapsed_ms', COALESCE(v_canonical_call_wall_elapsed_ms, 0),
    'source_build_residual_elapsed_ms', COALESCE(v_residual_elapsed_ms, 0),
    'source_build_residual_wall_elapsed_ms', COALESCE(v_residual_wall_elapsed_ms, 0),
    'source_build_residual_measured_elapsed_ms', COALESCE(v_residual_measured_elapsed_ms, 0),
    'source_build_residual_unattributed_elapsed_ms', COALESCE(v_residual_unattributed_elapsed_ms, 0),
    'source_build_temp_transform_elapsed_ms', COALESCE(v_temp_transform_elapsed_ms, 0),
    'source_build_source_upsert_elapsed_ms', COALESCE(v_source_upsert_elapsed_ms, 0),
    'source_build_session_progress_update_elapsed_ms', COALESCE(v_session_progress_update_elapsed_ms, 0),
    'source_build_timing', COALESCE(v_source_build_timing_json, '{}'::jsonb),
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  )
  || jsonb_build_object(
    'source_build_complete', COALESCE(v_has_more, false) IS NOT TRUE AND COALESCE(v_sync_attested, false),
    'pending', COALESCE(v_has_more, false) OR COALESCE(v_sync_attested, false) IS NOT TRUE,
    'ready', COALESCE(v_has_more, false) IS NOT TRUE AND COALESCE(v_sync_attested, false),
    'collect_result', COALESCE(v_collect_result, '{}'::jsonb),
    'canonical_result', COALESCE(v_canonical_result, '{}'::jsonb),
    'source_build_diagnostics', jsonb_build_object(
      'collect', COALESCE(v_collect_diagnostics_json, '{}'::jsonb),
      'canonical', COALESCE(v_canonical_diagnostics_json, '{}'::jsonb),
      'legacy_path_used', true,
      'fallback_from_delta', COALESCE(v_fallback_from_delta, false),
      'fallback_reason', COALESCE(v_delta_fallback_reason, v_fallback_reason),
      'projection_run_id', v_delta_projection_run_id_text,
      'projection_class', v_delta_projection_class,
      'source_table', 'public.banking_pay_workbench_candidate_source_lines',
      'session_lock_scope', 'LOCKLESS_FINAL_REVALIDATION_OPTIONAL_NOWAIT_PROGRESS',
      'candidate_lock_scope', 'SESSION_SCOPE_ROW_FOR_UPDATE',
      'session_progress_update_applied', COALESCE(v_session_progress_update_applied, false),
      'session_progress_lock_skipped', COALESCE(v_session_progress_lock_skipped, false),
      'overpayment_sync_attestation', COALESCE(v_sync_attestation, '{}'::jsonb),
      'timing', COALESCE(v_source_build_timing_json, '{}'::jsonb),
      'source_build_reconciliation_deferred_to_complete_job', COALESCE(v_current_source_row_count, 0) > 0,
      'source_row_identity', jsonb_build_object(
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', v_session_version,
        'source_change_seq', v_source_change_seq,
        'source_build_run_id', v_source_build_run_id::text
      )
    )
  );
END;
$function$;
