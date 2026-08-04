-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer)
-- Installed pg_get_functiondef MD5: 803e138de6df05528ee35ece87357573
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_job_type text DEFAULT NULL::text, p_cursor_json jsonb DEFAULT NULL::jsonb, p_source_job_id uuid DEFAULT NULL::uuid, p_result_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_reason text DEFAULT NULL::text, p_priority integer DEFAULT NULL::integer, p_limit integer DEFAULT NULL::integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_row public.banking_pay_workbench_session_scope%ROWTYPE;
  v_source_job_row public.banking_pay_workbench_jobs%ROWTYPE;
  v_job_type text := CASE
    WHEN UPPER(BTRIM(COALESCE(p_job_type, ''))) IN (
      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
      'CANDIDATE_SOURCE_BUILD',
      'CANDIDATE_SOURCE_BUILD_CHUNK',
      'SOURCE_BUILD',
      'SOURCE_BUILD_PAGE',
      'WORKBENCH_SOURCE_BUILD'
    ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    WHEN UPPER(BTRIM(COALESCE(p_job_type, ''))) IN (
      'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'CANDIDATE_DELTA_REFRESH',
      'DELTA_REFRESH'
    ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    WHEN UPPER(BTRIM(COALESCE(p_job_type, ''))) IN (
      'WORKBENCH_SESSION_CLONE_REBASE',
      'SESSION_CLONE_REBASE',
      'CLONE_REBASE'
    ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
    ELSE UPPER(BTRIM(COALESCE(p_job_type, '')))
  END;
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'WORKBENCH_STAGE_CONTINUATION');
  v_result_json jsonb := CASE WHEN jsonb_typeof(COALESCE(p_result_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_result_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_actor_user_id uuid := p_actor_user_id;
  v_cursor_json jsonb := NULL::jsonb;
  v_cursor_token text := 'none';
  v_session_signature_token text := 'none';
  v_candidate_key text := 'ALL';
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_priority integer := 50;
  v_line_work_action text := NULL::text;
  v_next_recommended_action text := NULL::text;
  v_progress_state text := NULL::text;
  v_scope_status text := NULL::text;
  v_source_job_session_version bigint := NULL::bigint;
  v_payload_core_json jsonb := '{}'::jsonb;
  v_payload_stage_json jsonb := '{}'::jsonb;
  v_payload_cursor_json jsonb := '{}'::jsonb;
  v_payload_source_json jsonb := '{}'::jsonb;
  v_payload_result_summary_json jsonb := '{}'::jsonb;
  v_payload_json jsonb := '{}'::jsonb;
  v_reuse_patch_json jsonb := '{}'::jsonb;
  v_dedupe_key text := NULL::text;
  v_job_id uuid := NULL::uuid;
  v_job_status text := NULL::text;
  v_job_was_inserted boolean := false;
  v_stale_running_seconds integer := 180;
  v_stale_cutoff timestamptz := NULL::timestamptz;
  v_superseded_stale_count integer := 0;
  v_source_build_run_id_token text := 'none';
  v_source_change_seq_token text := 'none';
  v_projection_run_id_token text := 'none';
  v_projection_class text := 'UNKNOWN';
  v_delta_phase text := 'INIT_PREFLIGHT';
  v_delta_write_phase_token text := 'none';
  v_delta_next_phase text := NULL::text;
  v_refresh_scope_kind text := NULL::text;
  v_source_build_allow_full_fallback boolean := false;
  v_pay_channel_scope text := NULL::text;
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_source_seed_cursor_json jsonb := '{}'::jsonb;
  v_clone_source_session_id_text text := NULL::text;
  v_clone_target_session_id_text text := NULL::text;
  v_clone_mode text := NULL::text;
  v_candidate_cursor_json jsonb := '{}'::jsonb;
  v_feature_flags_json jsonb := '{}'::jsonb;
  v_session_progress_update_applied boolean := false;
  v_session_progress_lock_skipped boolean := false;
  v_session_progress_lock_skip_reason text := NULL::text;
  v_session_progress_update_row_count integer := 0;
  v_original_dedupe_key text := NULL::text;
  v_self_reuse_prevented boolean := false;
  v_self_reuse_retry_count integer := 0;
  v_insert_row_count integer := 0;
  v_live_candidate_source_change_seq bigint := 0;
  v_cursor_source_change_seq bigint := 0;
  v_projection_run_source_change_seq bigint := 0;
  v_effective_continuation_source_seq bigint := 0;
  v_normalised_delta_family_key text := NULL::text;
  v_economic_build_id uuid := NULL::uuid;
  v_private_stage text := NULL::text;
  v_private_cursor_kind text := NULL::text;
  v_private_stage_version integer := NULL::integer;
  v_private_cursor_start_hash text := NULL::text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SESSION_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_CONTINUATION_SESSION_ID_REQUIRED')::text;
  END IF;

  IF v_job_type = '' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_JOB_TYPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_CONTINUATION_JOB_TYPE_REQUIRED', 'session_id', p_session_id::text)::text;
  END IF;

  IF v_job_type NOT IN (
    'WORKBENCH_SESSION_SCOPE_SEED',
    'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'WORKBENCH_SESSION_CLONE_REBASE',
    'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
    'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
    'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_UNSUPPORTED_JOB_TYPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_UNSUPPORTED_JOB_TYPE',
              'session_id', p_session_id::text,
              'job_type', v_job_type
            )::text;
  END IF;

  IF p_cursor_json IS NOT NULL AND jsonb_typeof(p_cursor_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_CURSOR_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_CURSOR_MUST_BE_OBJECT',
              'session_id', p_session_id::text,
              'job_type', v_job_type,
              'cursor_type', jsonb_typeof(p_cursor_json)
            )::text;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_SESSION_NOT_FOUND',
              'session_id', p_session_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_SESSION_NOT_OPEN',
              'session_id', p_session_id::text,
              'status', v_session_row.status,
              'discarded_at_utc', v_session_row.discarded_at_utc
            )::text;
  END IF;

  v_actor_user_id := COALESCE(v_actor_user_id, v_session_row.actor_user_id);
  v_session_signature_token := md5(COALESCE(v_session_row.session_signature, ''));
  v_stale_running_seconds := 25;
  v_stale_cutoff := v_now - make_interval(secs => v_stale_running_seconds);

  IF p_source_job_id IS NOT NULL THEN
    SELECT source_job.*
    INTO v_source_job_row
    FROM public.banking_pay_workbench_jobs AS source_job
    WHERE source_job.id = p_source_job_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_NOT_FOUND',
                'session_id', p_session_id::text,
                'source_job_id', p_source_job_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    IF v_source_job_row.session_id IS DISTINCT FROM p_session_id THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SESSION_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SESSION_MISMATCH',
                'session_id', p_session_id::text,
                'source_job_id', p_source_job_id::text,
                'source_job_session_id', CASE WHEN v_source_job_row.session_id IS NULL THEN NULL ELSE v_source_job_row.session_id::text END,
                'job_type', v_job_type
              )::text;
    END IF;

    IF v_source_job_row.snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SNAPSHOT_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SNAPSHOT_MISMATCH',
                'session_id', p_session_id::text,
                'source_job_id', p_source_job_id::text,
                'source_job_snapshot_run_id', CASE WHEN v_source_job_row.snapshot_run_id IS NULL THEN NULL ELSE v_source_job_row.snapshot_run_id::text END,
                'session_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    IF COALESCE(v_source_job_row.payload_json->>'session_version', '') ~ '^[0-9]+$' THEN
      v_source_job_session_version := (v_source_job_row.payload_json->>'session_version')::bigint;

      IF COALESCE(v_source_job_session_version, 0) <> COALESCE(v_session_row.version, 0) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SESSION_VERSION_STALE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_SESSION_VERSION_STALE',
                  'session_id', p_session_id::text,
                  'source_job_id', p_source_job_id::text,
                  'source_job_session_version', v_source_job_session_version,
                  'current_session_version', COALESCE(v_session_row.version, 0),
                  'job_type', v_job_type
                )::text;
      END IF;
    END IF;
  END IF;

  v_source_build_run_id_token := COALESCE(
    NULLIF(BTRIM(COALESCE(v_result_json->>'source_build_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'source_build_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,source_build_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(p_cursor_json->>'source_build_run_id', '')), ''),
    'none'
  );
  v_source_change_seq_token := COALESCE(
    NULLIF(BTRIM(COALESCE(v_result_json->>'source_change_seq', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'source_change_seq', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'source_change_sequence', '')), ''),
    NULLIF(BTRIM(COALESCE(p_cursor_json->>'source_change_seq', '')), ''),
    'none'
  );

  v_projection_run_id_token := COALESCE(
    NULLIF(BTRIM(COALESCE(v_result_json->>'projection_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_result_json#>>'{cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_result_json#>>'{next_cursor_json,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_result_json#>>'{next_cursor_json,cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'projection_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{cursor_json,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{cursor_json,cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(p_cursor_json->>'projection_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(p_cursor_json#>>'{cursor,projection_run_id}', '')), ''),
    'none'
  );
  v_projection_class := COALESCE(
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'projection_class', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'projection_class', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'projection_class', ''))), ''),
    'UNKNOWN'
  );
  v_delta_phase := COALESCE(
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'next_phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'next_phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'phase', ''))), ''),
    'INIT_PREFLIGHT'
  );
  v_delta_next_phase := COALESCE(
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'next_phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'next_phase', ''))), ''),
    NULL::text
  );
  v_delta_write_phase_token := COALESCE(
    NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'write_phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'write_phase', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'write_phase', ''))), ''),
    'none'
  );
  /*
    A source-build result reports two related but different scopes:
      - requested_refresh_scope_kind: the caller's semantic scope for this run;
      - refresh_scope_kind / actual_refresh_scope_kind: the collection strategy used for this page.

    Full-live candidate builds intentionally page through internally targeted timesheet
    sets to avoid large aggregation.  Cursor continuations must preserve the requested
    scope, not the page's actual internal strategy; otherwise a CANDIDATE_FULL_LIVE
    run can be mis-enqueued as TARGETED_TIMESHEETS with no requested ids.
  */
  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND UPPER(BTRIM(COALESCE(v_reason, ''))) = 'SOURCE_BUILD_CURSOR_CONTINUATION' THEN
    v_refresh_scope_kind := COALESCE(
      NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'requested_refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'requested_refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json#>>'{source_build_diagnostics,collect,requested_refresh_scope_kind}', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,refresh_scope_kind}', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'refresh_scope_kind', ''))), ''),
      NULL::text
    );
  ELSIF v_job_type IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED') THEN
    v_refresh_scope_kind := COALESCE(
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'requested_refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json#>>'{source_build_diagnostics,collect,requested_refresh_scope_kind}', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,refresh_scope_kind}', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'requested_refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'refresh_scope_kind', ''))), ''),
      NULL::text
    );
  ELSE
    v_refresh_scope_kind := COALESCE(
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'refresh_scope_kind', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,refresh_scope_kind}', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'requested_refresh_scope_kind', ''))), ''),
      NULL::text
    );
  END IF;

  IF v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_refresh_scope_kind := CASE
      WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'CANDIDATE_FULL_LIVE'
      ELSE NULL::text
    END;
  END IF;

  v_pay_channel_scope := COALESCE(
    NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'pay_channel_scope', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'pay_channel_scope', ''))), ''),
    NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,pay_channel_scope}', ''))), ''),
    NULL::text
  );

  SELECT scope_arrays.array_json
  INTO v_targeted_timesheet_ids_json
  FROM (
    VALUES
      (10, v_source_job_row.payload_json->'targeted_timesheet_ids_requested'),
      (20, v_source_job_row.payload_json#>'{source_build,targeted_timesheet_ids_requested}'),
      (30, v_source_job_row.payload_json->'targeted_timesheet_ids'),
      (40, v_source_job_row.payload_json#>'{source_build,targeted_timesheet_ids}'),
      (50, v_result_json->'targeted_timesheet_ids_requested'),
      (60, v_result_json#>'{source_build,targeted_timesheet_ids_requested}'),
      (70, v_result_json->'targeted_timesheet_ids'),
      (80, v_result_json#>'{source_build,targeted_timesheet_ids}'),
      (90, p_cursor_json->'targeted_timesheet_ids')
  ) AS scope_arrays(priority, array_json)
  WHERE jsonb_typeof(scope_arrays.array_json) = 'array'
    AND jsonb_array_length(scope_arrays.array_json) > 0
  ORDER BY scope_arrays.priority
  LIMIT 1;

  v_targeted_timesheet_ids_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);

  SELECT scope_arrays.array_json
  INTO v_linked_timesheet_ids_json
  FROM (
    VALUES
      (10, v_source_job_row.payload_json->'linked_timesheet_ids_requested'),
      (20, v_source_job_row.payload_json#>'{source_build,linked_timesheet_ids_requested}'),
      (30, v_source_job_row.payload_json->'linked_timesheet_ids'),
      (40, v_source_job_row.payload_json#>'{source_build,linked_timesheet_ids}'),
      (50, v_result_json->'linked_timesheet_ids_requested'),
      (60, v_result_json#>'{source_build,linked_timesheet_ids_requested}'),
      (70, v_result_json->'linked_timesheet_ids'),
      (80, v_result_json#>'{source_build,linked_timesheet_ids}'),
      (90, p_cursor_json->'linked_timesheet_ids')
  ) AS scope_arrays(priority, array_json)
  WHERE jsonb_typeof(scope_arrays.array_json) = 'array'
    AND jsonb_array_length(scope_arrays.array_json) > 0
  ORDER BY scope_arrays.priority
  LIMIT 1;

  v_linked_timesheet_ids_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);

  IF v_job_type IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED')
     AND v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' THEN
    v_targeted_timesheet_ids_json := '[]'::jsonb;
    v_linked_timesheet_ids_json := '[]'::jsonb;
  END IF;


  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
     AND p_candidate_id IS NOT NULL THEN
    SELECT COALESCE(change_counter.seq, 0)
    INTO v_live_candidate_source_change_seq
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

    v_cursor_source_change_seq := GREATEST(
      COALESCE(CASE WHEN COALESCE(p_cursor_json->>'source_change_seq', '') ~ '^\d{1,18}$' THEN (p_cursor_json->>'source_change_seq')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(p_cursor_json->>'source_change_sequence', '') ~ '^\d{1,18}$' THEN (p_cursor_json->>'source_change_sequence')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(p_cursor_json#>>'{cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (p_cursor_json#>>'{cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(p_cursor_json#>>'{cursor_json,source_change_seq}', '') ~ '^\d{1,18}$' THEN (p_cursor_json#>>'{cursor_json,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(p_cursor_json#>>'{cursor_json,cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (p_cursor_json#>>'{cursor_json,cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_result_json#>>'{cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_result_json#>>'{cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_result_json#>>'{next_cursor_json,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_result_json#>>'{next_cursor_json,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_result_json#>>'{next_cursor_json,cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_result_json#>>'{next_cursor_json,cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_source_job_row.payload_json#>>'{cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_source_job_row.payload_json#>>'{cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_source_job_row.payload_json#>>'{cursor_json,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_source_job_row.payload_json#>>'{cursor_json,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_source_job_row.payload_json#>>'{cursor_json,cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_source_job_row.payload_json#>>'{cursor_json,cursor,source_change_seq}')::bigint END, 0),
      COALESCE(CASE WHEN v_source_change_seq_token ~ '^\d{1,18}$' THEN v_source_change_seq_token::bigint END, 0)
    );

    IF v_projection_run_id_token ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      SELECT COALESCE(projection_run.source_change_seq, 0)
      INTO v_projection_run_source_change_seq
      FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
      WHERE projection_run.id = v_projection_run_id_token::uuid;
    END IF;

    v_effective_continuation_source_seq := GREATEST(
      COALESCE(CASE WHEN v_source_change_seq_token ~ '^\d{1,18}$' THEN v_source_change_seq_token::bigint END, 0),
      COALESCE(v_cursor_source_change_seq, 0),
      COALESCE(v_projection_run_source_change_seq, 0)
    );

    v_normalised_delta_family_key := COALESCE(
      NULLIF(BTRIM(COALESCE(v_result_json->>'normalised_delta_family_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result_json->>'delta_family_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result_json->>'delta_coalescing_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'normalised_delta_family_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'delta_family_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'delta_coalescing_key', '')), ''),
      NULL::text
    );

    IF (
      (COALESCE(v_cursor_source_change_seq, 0) > 0 AND COALESCE(v_live_candidate_source_change_seq, 0) > COALESCE(v_cursor_source_change_seq, 0))
      OR (COALESCE(v_projection_run_source_change_seq, 0) > 0 AND COALESCE(v_live_candidate_source_change_seq, 0) > COALESCE(v_projection_run_source_change_seq, 0))
      OR (COALESCE(v_cursor_source_change_seq, 0) > 0 AND v_source_change_seq_token ~ '^\d{1,18}$' AND v_source_change_seq_token::bigint > COALESCE(v_cursor_source_change_seq, 0))
      OR (COALESCE(v_projection_run_source_change_seq, 0) > 0 AND v_source_change_seq_token ~ '^\d{1,18}$' AND v_source_change_seq_token::bigint > COALESCE(v_projection_run_source_change_seq, 0))
    ) THEN
      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        COALESCE(p_source_job_id::text, p_session_id::text || ':' || p_candidate_id::text),
        'CONTINUATION_SUPPRESSED_NEWER_SOURCE_SEQ',
        NULL::jsonb,
        jsonb_build_object(
          'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
          'session_id', p_session_id::text,
          'candidate_id', p_candidate_id::text,
          'job_type', v_job_type,
          'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
          'normalised_delta_family_key', v_normalised_delta_family_key,
          'cursor_source_change_seq', v_cursor_source_change_seq,
          'projection_run_source_change_seq', v_projection_run_source_change_seq,
          'payload_source_change_seq', CASE WHEN v_source_change_seq_token ~ '^\d{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'live_candidate_source_change_seq', v_live_candidate_source_change_seq,
          'targeted_timesheet_ids', v_targeted_timesheet_ids_json,
          'linked_timesheet_ids', v_linked_timesheet_ids_json,
          'continuation_not_enqueued', true,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        'CONTINUATION_SUPPRESSED_NEWER_SOURCE_SEQ',
        v_actor_user_id
      );

      RETURN jsonb_build_object(
        'ok', true,
        'job_id', NULL::text,
        'job_type', v_job_type,
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
        'continuation_not_enqueued', true,
        'stale_continuation_suppressed', true,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'reason', 'CONTINUATION_SUPPRESSED_NEWER_SOURCE_SEQ',
        'normalised_delta_family_key', v_normalised_delta_family_key,
        'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
        'cursor_source_change_seq', v_cursor_source_change_seq,
        'projection_run_source_change_seq', v_projection_run_source_change_seq,
        'live_candidate_source_change_seq', v_live_candidate_source_change_seq,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );
    END IF;
  END IF;

  v_source_build_allow_full_fallback := LOWER(BTRIM(COALESCE(
    NULLIF(BTRIM(COALESCE(v_result_json->>'source_build_allow_full_fallback', '')), ''),
    NULLIF(BTRIM(COALESCE(v_result_json#>>'{source_build,allow_full_fallback}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'source_build_allow_full_fallback', '')), ''),
    NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json#>>'{source_build,allow_full_fallback}', '')), ''),
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
    v_clone_source_session_id_text := COALESCE(
      NULLIF(BTRIM(COALESCE(v_result_json->>'source_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'source_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'clone_from_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(p_cursor_json->>'source_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(p_cursor_json->>'clone_from_session_id', '')), '')
    );
    v_clone_target_session_id_text := COALESCE(
      NULLIF(BTRIM(COALESCE(v_result_json->>'target_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'target_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'session_id', '')), ''),
      p_session_id::text
    );
    v_clone_mode := COALESCE(
      NULLIF(UPPER(BTRIM(COALESCE(v_result_json->>'clone_mode', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(v_source_job_row.payload_json->>'clone_mode', ''))), ''),
      NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'clone_mode', ''))), ''),
      'CERTIFIED_SIMPLE_ONLY'
    );
    v_candidate_cursor_json := CASE
      WHEN jsonb_typeof(p_cursor_json->'candidate_cursor') = 'object' THEN COALESCE(p_cursor_json->'candidate_cursor', '{}'::jsonb)
      WHEN jsonb_typeof(v_source_job_row.payload_json->'candidate_cursor') = 'object' THEN COALESCE(v_source_job_row.payload_json->'candidate_cursor', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;
    v_feature_flags_json := CASE
      WHEN jsonb_typeof(v_source_job_row.payload_json->'feature_flags') = 'object' THEN COALESCE(v_source_job_row.payload_json->'feature_flags', '{}'::jsonb)
      WHEN jsonb_typeof(p_cursor_json->'feature_flags') = 'object' THEN COALESCE(p_cursor_json->'feature_flags', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) = 0
     AND jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) = 0
     AND COALESCE(v_source_build_allow_full_fallback, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_TARGETED_IDS_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_TARGETED_IDS_REQUIRED',
              'session_id', p_session_id::text,
              'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
              'job_type', v_job_type,
              'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
              'reason', v_reason,
              'refresh_scope_kind', v_refresh_scope_kind,
              'cursor_requested_refresh_scope_kind', NULLIF(UPPER(BTRIM(COALESCE(p_cursor_json->>'requested_refresh_scope_kind', ''))), ''),
              'message', 'Refusing to enqueue TARGETED_TIMESHEETS source-build continuation without targeted_timesheet_ids or linked_timesheet_ids.'
            )::text;
  END IF;
  v_source_seed_cursor_json := jsonb_strip_nulls(jsonb_build_object(
    'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
    'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
    'projection_class', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_projection_class ELSE NULL::text END,
    'phase', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_phase ELSE NULL::text END,
    'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
    'session_version', COALESCE(v_session_row.version, 0),
    'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
    'session_signature', v_session_row.session_signature,
    'source_row_cursor', 'START'
  ));

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND COALESCE(v_source_build_run_id_token, 'none') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_BUILD_RUN_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_BUILD_RUN_ID_REQUIRED',
              'session_id', p_session_id::text,
              'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
              'job_type', v_job_type,
              'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
              'message', 'WORKBENCH_CANDIDATE_SOURCE_BUILD continuation requires source_build_run_id from result, source payload, or cursor.'
            )::text;
  END IF;

  IF v_job_type = 'WORKBENCH_SESSION_SCOPE_SEED' OR v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
    IF p_candidate_id IS NOT NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SESSION_JOB_CANDIDATE_NOT_ALLOWED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_SESSION_JOB_CANDIDATE_NOT_ALLOWED',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    IF v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
      v_line_work_action := 'CLONE_REBASE';
      v_next_recommended_action := 'CLONE_REBASE_CHUNK';
      v_progress_state := 'CLONE_REBASING';
      v_priority := COALESCE(p_priority, 42);
    ELSE
      v_line_work_action := 'SCOPE_SEED';
      v_next_recommended_action := 'SEED_SCOPE_CHUNK';
      v_progress_state := 'REFRESHING_CANDIDATES';
      v_priority := COALESCE(p_priority, 40);
    END IF;
  ELSE
    IF p_candidate_id IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_CANDIDATE_ID_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_CANDIDATE_ID_REQUIRED',
                'session_id', p_session_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    SELECT scope_row.*
    INTO v_scope_row
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND scope_row.candidate_id = p_candidate_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_CANDIDATE_NOT_IN_SCOPE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_CANDIDATE_NOT_IN_SCOPE',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    IF p_source_job_id IS NOT NULL
       AND v_source_job_row.candidate_id IS NOT NULL
       AND v_source_job_row.candidate_id IS DISTINCT FROM p_candidate_id THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_CANDIDATE_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_SOURCE_JOB_CANDIDATE_MISMATCH',
                'session_id', p_session_id::text,
                'candidate_id', p_candidate_id::text,
                'source_job_id', p_source_job_id::text,
                'source_job_candidate_id', v_source_job_row.candidate_id::text,
                'job_type', v_job_type
              )::text;
    END IF;

    v_candidate_key := p_candidate_id::text;

    IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
      v_line_work_action := 'SOURCE_BUILD';
      v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
      v_progress_state := 'REFRESHING_CANDIDATES';
      v_scope_status := 'SOURCE_BUILD_PENDING';
      v_priority := COALESCE(p_priority, 44);
    ELSIF v_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN
      v_line_work_action := 'SEED';
      v_next_recommended_action := 'SEED_LINE_WORK_CHUNK';
      v_progress_state := 'REFRESHING_CANDIDATES';
      v_scope_status := CASE
        WHEN UPPER(BTRIM(COALESCE(v_scope_row.status, ''))) = 'PENDING' THEN 'PENDING'
        ELSE 'LINE_WORK_PENDING'
      END;
      v_priority := COALESCE(p_priority, 45);
    ELSIF v_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN
      v_line_work_action := 'PROCESS';
      v_next_recommended_action := 'PROCESS_LINE_WORK_CHUNK';
      v_progress_state := 'PROCESSING_LINE_WORK';
      v_scope_status := 'LINE_WORK_PENDING';
      v_priority := COALESCE(p_priority, 42);
    ELSIF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
      v_line_work_action := 'DELTA_REFRESH';
      v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
      v_progress_state := 'DELTA_REFRESHING';
      v_scope_status := 'DELTA_REFRESH_PENDING';
      v_priority := COALESCE(p_priority, 43);
    ELSE
      v_line_work_action := 'MATERIALISE';
      v_next_recommended_action := 'MATERIALISE_PREVIEW_ROWS_CHUNK';
      v_progress_state := 'MATERIALISING_PREVIEW_ROWS';
      v_scope_status := 'LINE_WORK_READY';
      v_priority := COALESCE(p_priority, 41);
    END IF;
  END IF;

  v_priority := LEAST(GREATEST(COALESCE(v_priority, 50), 1), 999);

  IF p_cursor_json IS NOT NULL AND jsonb_typeof(p_cursor_json) = 'object' THEN
    v_cursor_json := p_cursor_json;
    v_cursor_token := md5(p_cursor_json::text);
  ELSE
    v_cursor_json := NULL::jsonb;
    v_cursor_token := 'none';
  END IF;

  IF v_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    v_economic_build_id:=v_source_job_row.economic_build_id;
    v_private_stage:=upper(COALESCE(NULLIF(v_result_json->>'next_action',''),
      NULLIF(v_result_json->>'private_stage',''),NULLIF(v_cursor_json->>'cursor_kind','')));
    v_private_cursor_kind:=upper(NULLIF(v_cursor_json->>'cursor_kind',''));
    v_private_stage_version:=COALESCE((v_cursor_json->>'cursor_version')::integer,1);
    v_private_cursor_start_hash:=md5(v_cursor_json::text);
    IF v_economic_build_id IS NULL OR v_cursor_json IS NULL
       OR v_private_stage NOT IN ('PREPARE_SCOPE','DEPENDENCY_CLOSURE','WORKSPACE_FACT',
         'RECONCILE_EXECUTE','SOURCE_PUBLISH','BOOTSTRAP_DISCOVERY','BUILD_CLEANUP')
       OR v_private_cursor_kind IS DISTINCT FROM (CASE v_private_stage
         WHEN 'PREPARE_SCOPE' THEN 'SCOPE_SELECT'
         ELSE v_private_stage END)
       OR NULLIF(v_cursor_json->>'build_id','')::uuid IS DISTINCT FROM v_economic_build_id
       OR NULLIF(v_cursor_json->>'candidate_id','')::uuid IS DISTINCT FROM p_candidate_id
       OR v_private_stage_version<1 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_CURSOR_STAGE_MISMATCH'
        USING ERRCODE='22023';
    END IF;
    PERFORM 1 FROM private.banking_pay_workbench_economic_builds build_row
    WHERE build_row.id=v_economic_build_id AND build_row.candidate_id=p_candidate_id
      AND build_row.private_stage=v_private_stage
      AND build_row.captured_candidate_generation=COALESCE(
        NULLIF(v_cursor_json->>'captured_candidate_generation','')::bigint,
        build_row.captured_candidate_generation)
      AND build_row.source_change_seq=COALESCE(
        NULLIF(v_cursor_json->>'captured_source_change_seq','')::bigint,build_row.source_change_seq);
    IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_BUILD_STALE' USING ERRCODE='40001'; END IF;
  END IF;

  IF v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
    v_dedupe_key := 'WORKBENCH_SESSION_CLONE_REBASE'
      || ':session:' || p_session_id::text
      || ':source:' || COALESCE(v_clone_source_session_id_text, 'none')
      || ':target:' || COALESCE(v_clone_target_session_id_text, p_session_id::text)
      || ':cursor_hash:' || v_cursor_token;
  ELSIF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_dedupe_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
      || ':session:' || p_session_id::text
      || ':candidate:' || COALESCE(v_candidate_key, 'ALL')
      || ':projection_run:' || COALESCE(v_projection_run_id_token, 'none')
      || ':phase:' || COALESCE(v_delta_phase, 'INIT_PREFLIGHT')
      || ':write_phase:' || COALESCE(v_delta_write_phase_token, 'none')
      || ':cursor_hash:' || v_cursor_token;
  ELSIF v_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    v_dedupe_key := 'source-build:'||p_session_id::text||':'||p_candidate_id::text||':'||
      v_economic_build_id::text||':'||v_private_stage||':'||v_private_cursor_start_hash;
  ELSE
    v_dedupe_key := 'WORKBENCH_STAGE_CONTINUATION'
      || ':' || v_job_type
      || ':session:' || p_session_id::text
      || ':version:' || COALESCE(v_session_row.version, 0)::text
      || ':snapshot:' || v_session_row.source_snapshot_run_id::text
      || ':signature:' || v_session_signature_token
      || ':candidate:' || COALESCE(v_candidate_key, 'ALL')
      || ':source_build:' || COALESCE(v_source_build_run_id_token, 'none')
      || ':source_change:' || COALESCE(v_source_change_seq_token, 'none')
      || ':cursor:' || v_cursor_token;
  END IF;

  v_payload_core_json := jsonb_build_object(
    'session_id', p_session_id::text,
    'session_version', COALESCE(v_session_row.version, 0),
    'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
    'session_signature', v_session_row.session_signature,
    'session_signature_token', v_session_signature_token,
    'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
    'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'candidate_serial_key', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE public._pay_workbench_candidate_serial_key(p_candidate_id) END,
    'candidate_serial_candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'candidate_serial_active_chain_id', CASE
      WHEN p_candidate_id IS NULL THEN NULL
      ELSE COALESCE(
        NULLIF(BTRIM(COALESCE(v_source_job_row.payload_json->>'candidate_serial_active_chain_id', '')), ''),
        CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END
      )
    END,
    'candidate_serial_source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
    'candidate_serial_reason', 'CANDIDATE_SERIAL_CONTINUATION_QUEUED',
    'candidate_serial_continuation', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE true END,
    'job_type', v_job_type,
    'dedupe_key', v_dedupe_key,
    'continuation', true
  );

  v_payload_stage_json := jsonb_build_object(
    'reason', v_reason,
    'continuation_reason', v_reason,
    'line_work_action', v_line_work_action,
    'next_recommended_action', v_next_recommended_action,
    'limit', v_limit,
    'line_limit', v_limit,
    'chunk_size', v_limit,
    'run_mode', 'BOUNDED_CONTINUATION',
    'source_build_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'line_work_required', v_job_type NOT IN ('WORKBENCH_SESSION_SCOPE_SEED', 'WORKBENCH_SESSION_CLONE_REBASE', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_DELTA_REFRESH'),
    'line_work_only', v_job_type NOT IN ('WORKBENCH_SESSION_SCOPE_SEED', 'WORKBENCH_SESSION_CLONE_REBASE', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_DELTA_REFRESH'),
    'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'clone_rebase_required', v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE'
  );

  v_payload_cursor_json := jsonb_build_object(
    'cursor_json', v_cursor_json,
    'cursor', v_cursor_json,
    'cursor_token', v_cursor_token,
    'has_cursor', v_cursor_json IS NOT NULL
  );

  v_payload_source_json := jsonb_build_object(
    'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
    'source_result_has_more', COALESCE(v_result_json->'has_more', 'false'::jsonb),
    'source_result_next_cursor_present', COALESCE((v_result_json ? 'next_cursor') OR (v_result_json ? 'next_cursor_json') OR (v_result_json ? 'nextCursor'), false),
    'created_by_helper', 'pay_workbench_enqueue_stage_continuation',
    'created_at_utc', v_now::text
  );

  v_payload_result_summary_json := jsonb_build_object(
    'source_result_summary', jsonb_strip_nulls(jsonb_build_object(
      'ok', COALESCE(v_result_json->'ok', 'true'::jsonb),
      'seeded_count', v_result_json->'seeded_count',
      'new_scope_count', v_result_json->'new_scope_count',
      'enqueued_count', v_result_json->'enqueued_count',
      'reused_count', v_result_json->'reused_count',
      'processed_count', v_result_json->'processed_count',
      'ready_count_delta', v_result_json->'ready_count_delta',
      'materialised_count', v_result_json->'materialised_count',
      'materialized_count', v_result_json->'materialized_count',
      'new_preview_row_count', v_result_json->'new_preview_row_count',
      'new_selected_row_count', v_result_json->'new_selected_row_count',
      'source_rows_written', v_result_json->'source_rows_written',
      'current_source_row_count', v_result_json->'current_source_row_count',
      'source_page_count', v_result_json->'source_page_count',
      'timesheets_seen', v_result_json->'timesheets_seen',
      'fallback_used', v_result_json->'fallback_used',
      'cursor_advanced', v_result_json->'cursor_advanced',
      'error_count', v_result_json->'error_count',
      'has_more', v_result_json->'has_more'
    ))
  );

  v_payload_json := jsonb_strip_nulls(
    CASE
      WHEN p_source_job_id IS NOT NULL THEN
        COALESCE(v_source_job_row.payload_json, '{}'::jsonb) - ARRAY[
          'worker_id',
          'worker_claimed_at_utc',
          'worker_lease_seconds',
          'worker_lease_expires_at_utc',
          'worker_function',
          'result_json',
          'completion_json',
          'cursor_json',
          'cursor',
          'next_cursor_json',
          'next_cursor'
        ]::text[]
      ELSE '{}'::jsonb
    END
    || v_payload_core_json
    || v_payload_stage_json
    || v_payload_cursor_json
    || v_payload_source_json
    || v_payload_result_summary_json
    || CASE
      WHEN v_job_type IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED') THEN
        jsonb_build_object(
          'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
          'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'source_change_sequence', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'session_version', COALESCE(v_session_row.version, 0),
          'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'session_signature', v_session_row.session_signature
        )
        || jsonb_build_object(
          'refresh_scope_kind', v_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'pay_channel_scope', v_pay_channel_scope,
          'source_build_allow_full_fallback', COALESCE(v_source_build_allow_full_fallback, false),
          'source_build_cursor', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN v_source_seed_cursor_json ELSE v_cursor_json END
        )
        || jsonb_build_object(
          'source_build', jsonb_strip_nulls(
            CASE
              WHEN jsonb_typeof(v_source_job_row.payload_json->'source_build') = 'object'
                THEN COALESCE(v_source_job_row.payload_json->'source_build', '{}'::jsonb)
              ELSE '{}'::jsonb
            END
            || jsonb_build_object(
              'required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
              'run_id', NULLIF(v_source_build_run_id_token, 'none'),
              'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
              'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
              'source_change_sequence', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
              'session_version', COALESCE(v_session_row.version, 0),
              'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
              'session_signature', v_session_row.session_signature,
              'refresh_scope_kind', v_refresh_scope_kind,
              'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
              'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
              'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
              'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
              'pay_channel_scope', v_pay_channel_scope,
              'allow_full_fallback', COALESCE(v_source_build_allow_full_fallback, false),
              'limit', v_limit,
              'cursor', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN v_source_seed_cursor_json ELSE v_cursor_json END,
              'reason', v_reason
            )
          )
        )
      ELSE '{}'::jsonb
    END
    || CASE
      WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
        jsonb_build_object(
          'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
          'projection_class', v_projection_class,
          'projection_mode', 'DELTA',
          'phase', v_delta_phase,
          'next_phase', v_delta_next_phase,
          'cursor_json', v_cursor_json,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'source_change_sequence', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'latest_source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'normalised_delta_family_key', v_normalised_delta_family_key,
          'delta_family_key', v_normalised_delta_family_key,
          'delta_coalescing_key', v_normalised_delta_family_key,
          'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'delta_refresh_required', true,
          'source_build_required', false,
          'line_work_required', false,
          'legacy_fallback_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        )
      ELSE '{}'::jsonb
    END
    || CASE
      WHEN v_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
        jsonb_build_object(
          'source_session_id', v_clone_source_session_id_text,
          'target_session_id', COALESCE(v_clone_target_session_id_text, p_session_id::text),
          'cursor_json', COALESCE(v_cursor_json, '{}'::jsonb),
          'candidate_cursor', COALESCE(v_candidate_cursor_json, '{}'::jsonb),
          'limit', v_limit,
          'line_limit', v_limit,
          'clone_mode', v_clone_mode,
          'feature_flags', COALESCE(v_feature_flags_json, '{}'::jsonb),
          'clone_rebase_required', true,
          'source_build_required', false,
          'line_work_required', false,
          'delta_refresh_required', false
        )
      ELSE '{}'::jsonb
    END
  );

  v_reuse_patch_json := jsonb_build_object(
    'last_reused_at_utc', v_now::text,
    'last_reuse_reason', v_reason,
    'last_source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
    'last_actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END
  );

  WITH superseded_stale_jobs AS (
    UPDATE public.banking_pay_workbench_jobs AS superseded_job
    SET status = 'FAILED',
        updated_at_utc = v_now,
        failed_at_utc = v_now,
        last_error_json = jsonb_build_object(
          'code', 'WORKBENCH_JOB_SUPERSEDED_BY_NEWER_SESSION_VERSION',
          'message', 'Superseded stale running workbench preview job while enqueueing newer session-version continuation.',
          'session_id', p_session_id::text,
          'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
          'job_type', v_job_type,
          'current_session_version', COALESCE(v_session_row.version, 0),
          'job_session_version', CASE
            WHEN COALESCE(superseded_job.payload_json->>'session_version', '') ~ '^[0-9]+$'
              THEN (superseded_job.payload_json->>'session_version')::bigint
            ELSE NULL::bigint
          END,
          'stale_running_seconds', v_stale_running_seconds,
          'last_activity_at_utc', COALESCE(superseded_job.updated_at_utc, superseded_job.started_at_utc, superseded_job.run_at_utc, superseded_job.created_at_utc),
          'superseded_at_utc', v_now
        ),
        payload_json = COALESCE(superseded_job.payload_json, '{}'::jsonb) || jsonb_build_object(
          'superseded_by_newer_session_version_at_utc', v_now::text,
          'superseded_by_session_version', COALESCE(v_session_row.version, 0),
          'superseded_by_dedupe_key', v_dedupe_key
        )
    WHERE superseded_job.session_id = p_session_id
      AND superseded_job.candidate_id IS NOT DISTINCT FROM p_candidate_id
      AND superseded_job.snapshot_run_id IS NOT DISTINCT FROM v_session_row.source_snapshot_run_id
      AND UPPER(BTRIM(COALESCE(superseded_job.job_type, ''))) = v_job_type
      AND UPPER(BTRIM(COALESCE(superseded_job.status, ''))) IN ('RUNNING', 'PROCESSING', 'CLAIMED', 'IN_PROGRESS')
      AND superseded_job.completed_at_utc IS NULL
      AND superseded_job.failed_at_utc IS NULL
      AND COALESCE(superseded_job.payload_json->>'cursor_token', 'none') = v_cursor_token
      AND COALESCE(superseded_job.payload_json->>'session_signature_token', 'none') = v_session_signature_token
      AND COALESCE(superseded_job.updated_at_utc, superseded_job.started_at_utc, superseded_job.run_at_utc, superseded_job.created_at_utc) <= v_stale_cutoff
      AND COALESCE(superseded_job.payload_json->>'session_version', '') ~ '^[0-9]+$'
      AND (superseded_job.payload_json->>'session_version')::bigint < COALESCE(v_session_row.version, 0)
    RETURNING superseded_job.id
  )
  SELECT COUNT(*)::integer
  INTO v_superseded_stale_count
  FROM superseded_stale_jobs;

  v_original_dedupe_key := v_dedupe_key;

  <<continuation_insert_attempt>>
  LOOP
    v_job_id := NULL::uuid;
    v_job_status := NULL::text;
    v_job_was_inserted := false;
    v_insert_row_count := 0;

  INSERT INTO public.banking_pay_workbench_jobs AS target_job (
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    candidate_id,
    payload_json,
    economic_build_id,
    private_stage,
    private_cursor_kind,
    private_cursor_json,
    private_stage_version,
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    v_job_type,
    'QUEUED',
    v_priority,
    v_now,
    0,
    8,
    v_dedupe_key,
    v_session_row.source_snapshot_run_id,
    p_session_id,
    p_candidate_id,
    v_payload_json,
    v_economic_build_id,
    v_private_stage,
    v_private_cursor_kind,
    COALESCE(v_cursor_json,'{}'::jsonb),
    v_private_stage_version,
    v_now,
    v_now,
    NULL::timestamptz,
    NULL::timestamptz,
    NULL::timestamptz,
    NULL::jsonb
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET economic_build_id=COALESCE(target_job.economic_build_id,EXCLUDED.economic_build_id),
      private_stage=COALESCE(target_job.private_stage,EXCLUDED.private_stage),
      private_cursor_kind=COALESCE(target_job.private_cursor_kind,EXCLUDED.private_cursor_kind),
      private_cursor_json=CASE WHEN target_job.status='RUNNING' THEN target_job.private_cursor_json ELSE EXCLUDED.private_cursor_json END,
      private_stage_version=COALESCE(target_job.private_stage_version,EXCLUDED.private_stage_version),
      status = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
             AND COALESCE(target_job.attempt_count, 0) >= GREATEST(COALESCE(target_job.max_attempts, 8), 1)
          THEN 'FAILED'
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
          THEN 'QUEUED'
        ELSE target_job.status
      END,
      priority = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) > v_stale_cutoff
          THEN target_job.priority
        ELSE LEAST(target_job.priority, EXCLUDED.priority)
      END,
      run_at_utc = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
             AND COALESCE(target_job.attempt_count, 0) >= GREATEST(COALESCE(target_job.max_attempts, 8), 1)
          THEN target_job.run_at_utc
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
          THEN v_now
        WHEN target_job.status = 'RUNNING' THEN target_job.run_at_utc
        ELSE LEAST(target_job.run_at_utc, EXCLUDED.run_at_utc)
      END,
      started_at_utc = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
             AND COALESCE(target_job.attempt_count, 0) >= GREATEST(COALESCE(target_job.max_attempts, 8), 1)
          THEN target_job.started_at_utc
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
          THEN NULL::timestamptz
        ELSE target_job.started_at_utc
      END,
      completed_at_utc = target_job.completed_at_utc,
      failed_at_utc = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
             AND COALESCE(target_job.attempt_count, 0) >= GREATEST(COALESCE(target_job.max_attempts, 8), 1)
          THEN v_now
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
          THEN NULL::timestamptz
        ELSE target_job.failed_at_utc
      END,
      last_error_json = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND target_job.completed_at_utc IS NULL
             AND target_job.failed_at_utc IS NULL
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
          THEN jsonb_build_object(
            'code', CASE
              WHEN COALESCE(target_job.attempt_count, 0) >= GREATEST(COALESCE(target_job.max_attempts, 8), 1)
                THEN 'STALE_RUNNING_WORKBENCH_JOB_MAX_ATTEMPTS'
              ELSE 'STALE_RUNNING_WORKBENCH_JOB_RECOVERED'
            END,
            'message', 'Recovered stale running workbench preview job during continuation enqueue.',
            'job_type', v_job_type,
            'session_id', p_session_id::text,
            'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
            'stale_running_seconds', v_stale_running_seconds,
            'recovered_at_utc', v_now
          )
        ELSE target_job.last_error_json
      END,
      payload_json = COALESCE(target_job.payload_json, '{}'::jsonb)
        || v_reuse_patch_json
        || CASE
          WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
               AND target_job.completed_at_utc IS NULL
               AND target_job.failed_at_utc IS NULL
               AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) <= v_stale_cutoff
            THEN jsonb_build_object(
              'stale_running_recovered_at_utc', v_now::text,
              'stale_running_recovered_by', 'pay_workbench_enqueue_stage_continuation'
            )
          ELSE '{}'::jsonb
        END,
      updated_at_utc = CASE
        WHEN UPPER(BTRIM(COALESCE(target_job.status, ''))) = 'RUNNING'
             AND COALESCE(target_job.updated_at_utc, target_job.started_at_utc, target_job.run_at_utc, target_job.created_at_utc) > v_stale_cutoff
          THEN target_job.updated_at_utc
        ELSE v_now
      END
  WHERE p_source_job_id IS NULL
     OR target_job.id IS DISTINCT FROM p_source_job_id
  RETURNING target_job.id,
            target_job.status,
            (xmax = 0)
  INTO v_job_id,
       v_job_status,
       v_job_was_inserted;

    GET DIAGNOSTICS v_insert_row_count = ROW_COUNT;

    IF COALESCE(v_insert_row_count, 0) = 0 THEN
      IF p_source_job_id IS NOT NULL
         AND EXISTS (
           SELECT 1
           FROM public.banking_pay_workbench_jobs AS self_reuse_source_job
           WHERE self_reuse_source_job.id = p_source_job_id
             AND self_reuse_source_job.dedupe_key = v_dedupe_key
             AND UPPER(BTRIM(COALESCE(self_reuse_source_job.status, ''))) IN ('QUEUED', 'RUNNING')
         ) THEN
        IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
          RETURN jsonb_build_object(
            'ok', true,
            'job_id', NULL::text,
            'job_type', v_job_type,
            'status', 'NOT_ENQUEUED',
            'session_id', p_session_id::text,
            'session_version', COALESCE(v_session_row.version, 0),
            'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
            'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
            'dedupe_key', v_dedupe_key,
            'cursor_token', v_cursor_token,
            'has_cursor', v_cursor_json IS NOT NULL,
            'limit', v_limit,
            'line_limit', v_limit,
            'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
            'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
            'refresh_scope_kind', v_refresh_scope_kind,
            'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
            'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
            'line_work_action', v_line_work_action,
            'next_recommended_action', v_next_recommended_action,
            'reused', false,
            'created', false,
            'superseded_stale_running_count', COALESCE(v_superseded_stale_count, 0),
            'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
            'projection_class', NULLIF(v_projection_class, ''),
            'phase', NULLIF(v_delta_phase, ''),
            'next_phase', NULLIF(v_delta_next_phase, ''),
            'delta_refresh_required', true,
            'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
            'continuation_reason', v_reason,
            'continuation_not_enqueued', true,
            'identical_cursor_self_reuse_suppressed', true,
            'self_reuse_prevented', true,
            'original_dedupe_key', COALESCE(v_original_dedupe_key, v_dedupe_key),
            'self_reuse_retry_count', COALESCE(v_self_reuse_retry_count, 0) + 1,
            'no_op', true,
            'reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED'
          );
        END IF;
        IF v_self_reuse_prevented IS TRUE THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED',
                    'session_id', p_session_id::text,
                    'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
                    'job_type', v_job_type,
                    'source_job_id', p_source_job_id::text,
                    'original_dedupe_key', v_original_dedupe_key,
                    'attempted_dedupe_key', v_dedupe_key,
                    'message', 'Continuation enqueue refused to return or update the currently running source job as its own continuation.'
                  )::text;
        END IF;

        v_self_reuse_prevented := true;
        v_self_reuse_retry_count := COALESCE(v_self_reuse_retry_count, 0) + 1;
        v_original_dedupe_key := COALESCE(v_original_dedupe_key, v_dedupe_key);
        v_dedupe_key := v_original_dedupe_key || ':after_source_job:' || p_source_job_id::text;

        v_payload_json := jsonb_strip_nulls(
          COALESCE(v_payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'dedupe_key', v_dedupe_key,
            'self_reuse_prevented', true,
            'continuation_self_reuse_prevented', true,
            'original_dedupe_key', v_original_dedupe_key,
            'self_reuse_safe_dedupe_key', v_dedupe_key,
            'self_reuse_source_job_id', p_source_job_id::text
          )
        );

        v_reuse_patch_json := COALESCE(v_reuse_patch_json, '{}'::jsonb)
          || jsonb_build_object(
            'self_reuse_prevented', true,
            'continuation_self_reuse_prevented', true,
            'original_dedupe_key', v_original_dedupe_key,
            'self_reuse_safe_dedupe_key', v_dedupe_key,
            'self_reuse_source_job_id', p_source_job_id::text
          );

        CONTINUE continuation_insert_attempt;
      END IF;

      RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_INSERT_RETURNED_NO_ROW'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_CONTINUATION_INSERT_RETURNED_NO_ROW',
                'session_id', p_session_id::text,
                'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
                'job_type', v_job_type,
                'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
                'dedupe_key', v_dedupe_key,
                'message', 'Continuation insert/upsert returned no row without a recognised source-job self-reuse conflict.'
              )::text;
    END IF;

    IF p_source_job_id IS NOT NULL
       AND v_job_id IS NOT NULL
       AND v_job_id = p_source_job_id THEN
      IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
        RETURN jsonb_build_object(
          'ok', true,
          'job_id', NULL::text,
          'job_type', v_job_type,
          'status', 'NOT_ENQUEUED',
          'session_id', p_session_id::text,
          'session_version', COALESCE(v_session_row.version, 0),
          'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
          'dedupe_key', v_dedupe_key,
          'cursor_token', v_cursor_token,
          'has_cursor', v_cursor_json IS NOT NULL,
          'limit', v_limit,
          'line_limit', v_limit,
          'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
          'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
          'refresh_scope_kind', v_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'line_work_action', v_line_work_action,
          'next_recommended_action', v_next_recommended_action,
          'reused', false,
          'created', false,
          'superseded_stale_running_count', COALESCE(v_superseded_stale_count, 0),
          'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
          'projection_class', NULLIF(v_projection_class, ''),
          'phase', NULLIF(v_delta_phase, ''),
          'next_phase', NULLIF(v_delta_next_phase, ''),
          'delta_refresh_required', true,
          'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
          'continuation_reason', v_reason,
          'continuation_not_enqueued', true,
          'identical_cursor_self_reuse_suppressed', true,
          'self_reuse_prevented', true,
          'original_dedupe_key', COALESCE(v_original_dedupe_key, v_dedupe_key),
          'self_reuse_retry_count', COALESCE(v_self_reuse_retry_count, 0) + 1,
          'no_op', true,
          'reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED'
        );
      END IF;
      IF v_self_reuse_prevented IS TRUE THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED',
                  'session_id', p_session_id::text,
                  'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
                  'job_type', v_job_type,
                  'source_job_id', p_source_job_id::text,
                  'original_dedupe_key', v_original_dedupe_key,
                  'attempted_dedupe_key', v_dedupe_key,
                  'message', 'Continuation helper still resolved to the current source job after self-reuse retry.'
                )::text;
      END IF;

      v_self_reuse_prevented := true;
      v_self_reuse_retry_count := COALESCE(v_self_reuse_retry_count, 0) + 1;
      v_original_dedupe_key := COALESCE(v_original_dedupe_key, v_dedupe_key);
      v_dedupe_key := v_original_dedupe_key || ':after_source_job:' || p_source_job_id::text;

      v_payload_json := jsonb_strip_nulls(
        COALESCE(v_payload_json, '{}'::jsonb)
        || jsonb_build_object(
          'dedupe_key', v_dedupe_key,
          'self_reuse_prevented', true,
          'continuation_self_reuse_prevented', true,
          'original_dedupe_key', v_original_dedupe_key,
          'self_reuse_safe_dedupe_key', v_dedupe_key,
          'self_reuse_source_job_id', p_source_job_id::text
        )
      );

      v_reuse_patch_json := COALESCE(v_reuse_patch_json, '{}'::jsonb)
        || jsonb_build_object(
          'self_reuse_prevented', true,
          'continuation_self_reuse_prevented', true,
          'original_dedupe_key', v_original_dedupe_key,
          'self_reuse_safe_dedupe_key', v_dedupe_key,
          'self_reuse_source_job_id', p_source_job_id::text
        );

      CONTINUE continuation_insert_attempt;
    END IF;

    EXIT continuation_insert_attempt;
  END LOOP;

  IF p_source_job_id IS NOT NULL
     AND v_job_id IS NOT NULL
     AND v_job_id = p_source_job_id
     AND v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_id', NULL::text,
      'job_type', v_job_type,
      'status', 'NOT_ENQUEUED',
      'session_id', p_session_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
      'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'dedupe_key', v_dedupe_key,
      'cursor_token', v_cursor_token,
      'has_cursor', v_cursor_json IS NOT NULL,
      'limit', v_limit,
      'line_limit', v_limit,
      'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
      'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
      'line_work_action', v_line_work_action,
      'next_recommended_action', v_next_recommended_action,
      'reused', false,
      'created', false,
      'superseded_stale_running_count', COALESCE(v_superseded_stale_count, 0),
      'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
      'projection_class', NULLIF(v_projection_class, ''),
      'phase', NULLIF(v_delta_phase, ''),
      'next_phase', NULLIF(v_delta_next_phase, ''),
      'delta_refresh_required', true,
      'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
      'continuation_reason', v_reason,
      'continuation_not_enqueued', true,
      'identical_cursor_self_reuse_suppressed', true,
      'self_reuse_prevented', true,
      'original_dedupe_key', COALESCE(v_original_dedupe_key, v_dedupe_key),
      'self_reuse_retry_count', COALESCE(v_self_reuse_retry_count, 0) + 1,
      'no_op', true,
      'reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED'
    );
  END IF;

  IF p_source_job_id IS NOT NULL
     AND v_job_id IS NOT NULL
     AND v_job_id = p_source_job_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_SELF_REUSE_BLOCKED',
              'session_id', p_session_id::text,
              'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
              'job_type', v_job_type,
              'source_job_id', p_source_job_id::text,
              'original_dedupe_key', v_original_dedupe_key,
              'dedupe_key', v_dedupe_key,
              'message', 'Continuation helper refused to return the current source job as its own continuation.'
            )::text;
  END IF;

  IF v_job_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_JOB_ID_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_CONTINUATION_JOB_ID_MISSING',
              'session_id', p_session_id::text,
              'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
              'job_type', v_job_type,
              'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
              'dedupe_key', v_dedupe_key
            )::text;
  END IF;

  IF p_candidate_id IS NOT NULL THEN
    UPDATE public.banking_pay_workbench_session_scope AS scope_update
    SET status = v_scope_status,
        pending_job_id = v_job_id,
        dirty = CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN true ELSE false END,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE scope_update.session_id = p_session_id
      AND scope_update.candidate_id = p_candidate_id;
  END IF;

  BEGIN
    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS session_progress_lock
    WHERE session_progress_lock.id = p_session_id
      AND UPPER(BTRIM(COALESCE(session_progress_lock.status, ''))) = 'OPEN'
      AND session_progress_lock.discarded_at_utc IS NULL
      AND COALESCE(session_progress_lock.version, 1) = COALESCE(v_session_row.version, 1)
      AND session_progress_lock.source_snapshot_run_id IS NOT DISTINCT FROM v_session_row.source_snapshot_run_id
      AND session_progress_lock.session_signature IS NOT DISTINCT FROM v_session_row.session_signature
    FOR UPDATE NOWAIT;

    IF FOUND THEN
      UPDATE public.banking_pay_workbench_sessions AS session_update
      SET progress_state = v_progress_state,
          progress_json = COALESCE(session_update.progress_json, '{}'::jsonb) || jsonb_build_object(
            'last_continuation_enqueue_at_utc', v_now::text,
            'last_continuation_job_id', v_job_id::text,
            'last_continuation_job_type', v_job_type,
            'last_continuation_candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
            'last_continuation_reused', NOT v_job_was_inserted,
            'next_recommended_action', v_next_recommended_action,
            'phase', v_progress_state,
            'continuation_session_progress_locking', 'NOWAIT',
            'continuation_session_progress_update_applied', true,
            'continuation_session_progress_lock_skipped', false
          ),
          progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE session_update.id = p_session_id;

      GET DIAGNOSTICS v_session_progress_update_row_count = ROW_COUNT;
      v_session_progress_update_applied := COALESCE(v_session_progress_update_row_count, 0) > 0;
      v_session_progress_lock_skipped := COALESCE(v_session_progress_update_row_count, 0) <= 0;
      IF v_session_progress_lock_skipped THEN
        v_session_progress_lock_skip_reason := 'SESSION_UPDATE_NOT_APPLIED';
      END IF;
    ELSE
      v_session_progress_update_applied := false;
      v_session_progress_lock_skipped := true;
      v_session_progress_lock_skip_reason := 'SESSION_NOT_OPEN_OR_CONTEXT_STALE';
    END IF;
  EXCEPTION
    WHEN lock_not_available THEN
      v_session_progress_update_applied := false;
      v_session_progress_lock_skipped := true;
      v_session_progress_lock_skip_reason := 'SESSION_LOCK_NOT_AVAILABLE';
  END;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    v_job_id::text,
    CASE WHEN v_job_was_inserted THEN 'QUEUED' ELSE 'REUSED' END,
    NULL::jsonb,
    jsonb_build_object(
      'id', v_job_id::text,
      'job_type', v_job_type,
      'status', v_job_status,
      'session_id', p_session_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
      'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'dedupe_key', v_dedupe_key,
      'cursor_token', v_cursor_token,
      'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
      'continuation_reason', v_reason,
      'reused', NOT v_job_was_inserted,
      'superseded_stale_running_count', COALESCE(v_superseded_stale_count, 0),
      'session_progress_update_applied', COALESCE(v_session_progress_update_applied, false),
      'session_progress_lock_skipped', COALESCE(v_session_progress_lock_skipped, false),
      'session_progress_lock_skip_reason', v_session_progress_lock_skip_reason,
      'self_reuse_prevented', COALESCE(v_self_reuse_prevented, false),
      'original_dedupe_key', CASE WHEN COALESCE(v_self_reuse_prevented, false) THEN v_original_dedupe_key ELSE NULL::text END,
      'self_reuse_retry_count', COALESCE(v_self_reuse_retry_count, 0)
    ),
    'WORKBENCH_STAGE_CONTINUATION_ENQUEUE',
    v_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', v_job_type,
    'status', v_job_status,
    'session_id', p_session_id::text,
    'session_version', COALESCE(v_session_row.version, 0),
    'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
    'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'dedupe_key', v_dedupe_key,
    'cursor_token', v_cursor_token,
    'has_cursor', v_cursor_json IS NOT NULL,
    'limit', v_limit,
    'line_limit', v_limit,
    'source_build_run_id', NULLIF(v_source_build_run_id_token, 'none'),
    'source_change_seq', CASE WHEN v_source_change_seq_token ~ '^[0-9]{1,18}$' THEN v_source_change_seq_token::bigint ELSE NULL::bigint END,
    'refresh_scope_kind', v_refresh_scope_kind,
    'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
    'line_work_action', v_line_work_action,
    'next_recommended_action', v_next_recommended_action,
    'reused', NOT v_job_was_inserted,
    'created', v_job_was_inserted,
    'superseded_stale_running_count', COALESCE(v_superseded_stale_count, 0),
    'session_progress_update_applied', COALESCE(v_session_progress_update_applied, false),
    'session_progress_lock_skipped', COALESCE(v_session_progress_lock_skipped, false),
    'session_progress_lock_skip_reason', v_session_progress_lock_skip_reason,
    'projection_run_id', NULLIF(v_projection_run_id_token, 'none'),
    'projection_class', NULLIF(v_projection_class, ''),
    'phase', NULLIF(v_delta_phase, ''),
    'next_phase', NULLIF(v_delta_next_phase, ''),
    'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'source_job_id', CASE WHEN p_source_job_id IS NULL THEN NULL ELSE p_source_job_id::text END,
    'continuation_reason', v_reason,
    'self_reuse_prevented', COALESCE(v_self_reuse_prevented, false),
    'original_dedupe_key', CASE WHEN COALESCE(v_self_reuse_prevented, false) THEN v_original_dedupe_key ELSE NULL::text END,
    'self_reuse_retry_count', COALESCE(v_self_reuse_retry_count, 0)
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer) TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_stage_continuation(p_session_id uuid, p_candidate_id uuid, p_job_type text, p_cursor_json jsonb, p_source_job_id uuid, p_result_json jsonb, p_actor_user_id uuid, p_reason text, p_priority integer, p_limit integer) TO service_role;
