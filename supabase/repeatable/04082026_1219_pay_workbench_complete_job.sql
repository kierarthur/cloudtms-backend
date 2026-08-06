-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb)
-- Installed pg_get_functiondef MD5: fd38d86186c17ec445a1d427281b5fbf
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_job_row public.banking_pay_workbench_jobs%ROWTYPE;
  v_result_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_result_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_result_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_job_type text := NULL::text;
  v_stage_job_type text := NULL::text;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_has_open_session boolean := false;
  v_has_more boolean := false;
  v_next_cursor jsonb := NULL::jsonb;
  v_seeded_count integer := 0;
  v_new_scope_count integer := 0;
  v_processed_count integer := 0;
  v_ready_count_delta integer := 0;
  v_materialised_count integer := 0;
  v_error_count integer := 0;
  v_source_rows_written integer := 0;
  v_current_source_row_count integer := 0;
  v_current_source_row_count_authoritative boolean := false;
  v_source_build_run_id_text text := NULL::text;
  v_source_change_seq bigint := NULL::bigint;
  v_result_session_version bigint := NULL::bigint;
  v_source_seed_cursor_json jsonb := '{}'::jsonb;
  v_pending_line_work_exists boolean := false;
  v_ready_line_work_exists boolean := false;
  v_continuation_result jsonb := '{}'::jsonb;
  v_continuation_job_id_text text := NULL::text;
  v_continuation_jobs jsonb := '[]'::jsonb;
  v_continuation_count integer := 0;
  v_continuation_enqueued boolean := false;
  v_continuation_reused_count integer := 0;
  v_next_recommended_action text := NULL::text;
  v_existing_completion_json jsonb := '{}'::jsonb;
  v_payload_limit integer := 100;
  v_duplicate_completion boolean := false;
  v_completed_at_utc timestamptz := NULL::timestamptz;
  v_scope_pending_job_cleared_count integer := 0;
  v_scope_status_before_continuation text := NULL::text;
  v_scope_status_after_continuation text := NULL::text;
  v_scope_bucket_before_continuation text := NULL::text;
  v_scope_bucket_after_continuation text := NULL::text;
  v_scope_continuation_pending_delta integer := 0;
  v_scope_continuation_ready_delta integer := 0;
  v_scope_continuation_failed_delta integer := 0;
  v_scope_continuation_counter_adjusted boolean := false;
  v_finalisation_should_evaluate boolean := false;
  v_finalisation_actual_precheck_required boolean := false;
  v_finalisation_actual_precheck_passed boolean := false;
  v_current_candidate_terminal_success boolean := false;
  v_finalisation_evaluated boolean := false;
  v_finalisation_progress_json jsonb := '{}'::jsonb;
  v_finalisation_session_ready boolean := false;
  v_finalisation_ready_for_draft boolean := false;
  v_finalisation_ready_empty boolean := false;
  v_finalisation_stored_ready_mismatch boolean := false;
  v_finalisation_counter_reconciliation_required boolean := false;
  v_finalisation_counter_reconciliation_applied boolean := false;
  v_finalisation_counter_reconciled boolean := false;
  v_finalisation_counter_reconciliation_json jsonb := '{}'::jsonb;
  v_finalisation_blocker_codes jsonb := '[]'::jsonb;
  v_finalisation_draft_blocker_codes jsonb := '[]'::jsonb;
  v_finalisation_blocker_counts jsonb := '{}'::jsonb;
  v_final_progress_state text := NULL::text;
  v_final_phase text := NULL::text;
  v_final_status_text text := NULL::text;
  v_final_next_recommended_action text := NULL::text;
  v_authoritative_scope_total integer := 0;
  v_authoritative_scope_seeded integer := 0;
  v_authoritative_scope_ready integer := 0;
  v_authoritative_scope_pending integer := 0;
  v_authoritative_scope_failed integer := 0;
  v_authoritative_line_total integer := 0;
  v_authoritative_line_pending integer := 0;
  v_authoritative_line_ready integer := 0;
  v_authoritative_line_failed integer := 0;
  v_authoritative_preview_row_count integer := 0;
  v_authoritative_selected_row_count integer := 0;
  v_authoritative_section_counts_json jsonb := '{}'::jsonb;
  v_authoritative_candidate_sample_rows_json jsonb := '[]'::jsonb;
  v_completion_finalisation_json jsonb := '{}'::jsonb;
  v_source_reconciliation_json jsonb := '{}'::jsonb;
  v_source_reconciliation_applied boolean := false;
  v_source_reconciliation_deferred boolean := false;
  v_source_reconciliation_deferred_reason text := NULL::text;
  v_source_empty_session_progress_deferred boolean := false;
  v_source_empty_session_progress_deferred_reason text := NULL::text;
  v_source_empty_cleanup_source_row_count integer := 0;
  v_source_empty_cleanup_line_work_count integer := 0;
  v_source_empty_cleanup_preview_row_count integer := 0;
  v_source_empty_targeted_timesheet_count integer := 0;
  v_continuation_scope_counter_deferred boolean := false;
  v_continuation_scope_counter_deferred_reason text := NULL::text;
  v_finalisation_deferred boolean := false;
  v_finalisation_deferred_reason text := NULL::text;
  v_delta_fallback_required boolean := false;
  v_delta_fallback_reason text := NULL::text;
  v_delta_refresh_complete boolean := false;
  v_delta_more_due boolean := false;
  v_delta_made_progress boolean := false;
  v_delta_projection_run_id_text text := NULL::text;
  v_delta_next_cursor_json jsonb := NULL::jsonb;
  v_delta_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_delta_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_delta_source_rows_written integer := 0;
  v_delta_line_rows_written integer := 0;
  v_delta_preview_rows_written integer := 0;
  v_delta_rows_superseded integer := 0;
  v_delta_shadow_compare_status text := NULL::text;
  v_delta_shadow_compare_required boolean := false;
  v_delta_shadow_compare_enforced boolean := false;
  v_delta_shadow_compare_failed boolean := false;
  v_clone_more_due boolean := false;
  v_clone_complete boolean := false;
  v_clone_copied_candidate_count integer := 0;
  v_clone_copied_preview_row_count integer := 0;
  v_clone_legacy_refresh_enqueued_count integer := 0;
  v_clone_continuation_job_id uuid := NULL::uuid;
  v_clone_source_session_id_text text := NULL::text;
  v_clone_source_session_id uuid := NULL::uuid;
  v_clone_source_session_retired_count integer := 0;
  v_delta_fallback_enqueue_result jsonb := '{}'::jsonb;
  v_delta_fallback_job_id_text text := NULL::text;
  v_delta_scope_total_count integer := 0;
  v_delta_scope_ready_count integer := 0;
  v_delta_scope_pending_count integer := 0;
  v_delta_scope_failed_count integer := 0;
  v_delta_line_units_total integer := 0;
  v_delta_line_units_ready integer := 0;
  v_delta_line_units_pending integer := 0;
  v_delta_line_units_failed integer := 0;
  v_delta_preview_row_count integer := 0;
  v_delta_selected_row_count integer := 0;
  v_delta_section_counts_json jsonb := '{}'::jsonb;
  v_delta_candidate_sample_rows_json jsonb := '[]'::jsonb;
  v_live_source_change_seq bigint := 0;
  v_delta_superseded_by_live_source_guard boolean := false;
  v_delta_superseded_projection_runs integer := 0;
  v_delta_projection_mode text := 'DELTA';
  v_delta_projection_class text := 'UNKNOWN';
  v_delta_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_delta_lifecycle_context text := NULL::text;
  v_delta_is_normal_targeted_lifecycle boolean := false;
  v_delta_recovery_enqueue_result jsonb := '{}'::jsonb;
  v_delta_recovery_job_id_text text := NULL::text;
  v_delta_recovery_job_id uuid := NULL::uuid;
  v_delta_recovery_source_change_seq bigint := 0;
  v_delta_recovery_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_delta_recovery_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_material_attempt_id uuid := NULL::uuid;
BEGIN
  IF p_job_id IS NULL THEN
    RAISE EXCEPTION 'job_id is required';
  END IF;

  SELECT workbench_job.*
  INTO v_job_row
  FROM public.banking_pay_workbench_jobs AS workbench_job
  WHERE workbench_job.id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_jobs row % not found', p_job_id;
  END IF;

  v_duplicate_completion := UPPER(BTRIM(COALESCE(v_job_row.status, ''))) = 'SUCCEEDED'
    AND v_job_row.completed_at_utc IS NOT NULL;

  IF v_duplicate_completion THEN
    v_existing_completion_json := CASE
      WHEN jsonb_typeof(COALESCE(v_job_row.payload_json->'completion_json', '{}'::jsonb)) = 'object'
        THEN COALESCE(v_job_row.payload_json->'completion_json', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;

    v_result_json := CASE
      WHEN jsonb_typeof(COALESCE(v_job_row.payload_json->'result_json', '{}'::jsonb)) = 'object'
        THEN COALESCE(v_job_row.payload_json->'result_json', '{}'::jsonb)
      ELSE v_result_json
    END;

    v_continuation_jobs := CASE
      WHEN jsonb_typeof(COALESCE(v_existing_completion_json->'continuation_jobs', '[]'::jsonb)) = 'array'
        THEN COALESCE(v_existing_completion_json->'continuation_jobs', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    v_continuation_count := CASE
      WHEN COALESCE(v_existing_completion_json->>'continuation_count', '') ~ '^[0-9]+$'
        THEN (v_existing_completion_json->>'continuation_count')::integer
      ELSE jsonb_array_length(v_continuation_jobs)
    END;

    v_continuation_reused_count := CASE
      WHEN COALESCE(v_existing_completion_json->>'continuation_reused_count', '') ~ '^[0-9]+$'
        THEN (v_existing_completion_json->>'continuation_reused_count')::integer
      ELSE 0
    END;

    v_continuation_enqueued := COALESCE(v_continuation_count, 0) > 0
      OR LOWER(BTRIM(COALESCE(v_existing_completion_json->>'continuation_enqueued', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_next_recommended_action := NULLIF(BTRIM(COALESCE(v_existing_completion_json->>'next_recommended_action', '')), '');
    v_completed_at_utc := v_job_row.completed_at_utc;
  END IF;

  v_job_type := UPPER(BTRIM(COALESCE(v_job_row.job_type, '')));
  v_stage_job_type := CASE
    WHEN v_job_type IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
    WHEN v_job_type IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    WHEN v_job_type IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    WHEN v_job_type IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
    WHEN v_job_type IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
    WHEN v_job_type IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS', 'LINE_WORK_PROCESS') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
    WHEN v_job_type IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
    WHEN v_job_type IN ('SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
      AND v_job_row.session_id IS NOT NULL
      AND COALESCE((v_job_row.payload_json->>'line_work_only')::boolean, false) IS TRUE THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
    ELSE v_job_type
  END;

  IF COALESCE(v_result_json->>'has_more', '') ~* '^(true|t|1|yes)$' THEN
    v_has_more := true;
  ELSE
    v_has_more := false;
  END IF;

  v_next_cursor := CASE
    WHEN jsonb_typeof(v_result_json->'next_cursor') = 'object' THEN v_result_json->'next_cursor'
    WHEN jsonb_typeof(v_result_json->'next_cursor_json') = 'object' THEN v_result_json->'next_cursor_json'
    WHEN jsonb_typeof(v_result_json->'nextCursor') = 'object' THEN v_result_json->'nextCursor'
    WHEN jsonb_typeof(v_result_json->'nextCursorJson') = 'object' THEN v_result_json->'nextCursorJson'
    ELSE NULL::jsonb
  END;

  -- Material bounded-source stages have their own exact completion path.  The
  -- immutable attempt must already be COMPLETED by RPC 2, and at most one
  -- typed continuation is committed with this job completion.
  IF v_stage_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND v_job_row.economic_build_id IS NOT NULL THEN
    IF v_duplicate_completion THEN
      RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','SUCCEEDED',
        'duplicate_completion',true,'continuation_enqueued',
        COALESCE((v_existing_completion_json->>'continuation_enqueued')::boolean,false),
        'continuation_jobs',COALESCE(v_existing_completion_json->'continuation_jobs','[]'::jsonb),
        'completed_at_utc',v_job_row.completed_at_utc);
    END IF;
    SELECT attempt.id INTO v_material_attempt_id
    FROM private.banking_pay_workbench_stage_attempts attempt
    WHERE attempt.job_id=p_job_id AND attempt.build_id=v_job_row.economic_build_id
      AND attempt.private_stage=v_job_row.private_stage
      AND attempt.attempt_status='COMPLETED'
      AND attempt.attempt_number=v_job_row.attempt_count
    ORDER BY attempt.id DESC LIMIT 1;
    IF v_material_attempt_id IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_COMPLETION_REQUIRED' USING ERRCODE='40001';
    END IF;
    IF v_has_more AND v_next_cursor IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_CONTINUATION_CURSOR_REQUIRED' USING ERRCODE='22023';
    END IF;
    IF v_has_more THEN
      v_continuation_result:=public.pay_workbench_enqueue_stage_continuation(
        p_session_id=>v_job_row.session_id,p_candidate_id=>v_job_row.candidate_id,
        p_job_type=>'WORKBENCH_CANDIDATE_SOURCE_BUILD',p_cursor_json=>v_next_cursor,
        p_source_job_id=>p_job_id,p_result_json=>v_result_json,
        p_actor_user_id=>NULLIF(v_job_row.payload_json->>'actor_user_id','')::uuid,
        p_reason=>COALESCE(v_result_json->>'next_action','BOUNDED_STAGE_CONTINUATION'),
        p_priority=>v_job_row.priority,p_limit=>COALESCE(
          NULLIF(v_job_row.payload_json->>'source_build_limit','')::integer,
          NULLIF(v_job_row.payload_json->>'limit','')::integer,25)
      );
      v_continuation_job_id_text:=NULLIF(v_continuation_result->>'job_id','');
      v_continuation_enqueued:=v_continuation_job_id_text IS NOT NULL;
      v_continuation_jobs:=CASE WHEN v_continuation_job_id_text IS NULL THEN '[]'::jsonb
        ELSE jsonb_build_array(jsonb_build_object('job_id',v_continuation_job_id_text,
          'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD','private_stage',v_result_json->>'next_action')) END;
      v_continuation_count:=CASE WHEN v_continuation_enqueued THEN 1 ELSE 0 END;
    END IF;
    -- A nonterminal material page owns only its exact continuation. Terminal
    -- material completion must continue through the common successful-source
    -- reconciliation below so the public session scope cannot remain bound to
    -- a job that has already succeeded.
    IF v_has_more THEN
      UPDATE public.banking_pay_workbench_jobs target_job SET status='SUCCEEDED',
        completed_at_utc=clock_timestamp(),failed_at_utc=NULL,last_error_json=NULL,
        payload_json=COALESCE(target_job.payload_json,'{}'::jsonb)
          ||jsonb_build_object('result_json',public.pay_workbench_compact_job_result_json(v_result_json))
          ||jsonb_build_object('completion_json',jsonb_build_object(
            'attempt_id',v_material_attempt_id,'continuation_enqueued',v_continuation_enqueued,
            'continuation_jobs',v_continuation_jobs,'continuation_count',v_continuation_count,
            'next_recommended_action',v_result_json->>'next_action',
            'completed_at_utc',clock_timestamp())),updated_at_utc=clock_timestamp()
      WHERE target_job.id=p_job_id AND target_job.status='RUNNING';
      IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_JOB_COMPLETION_STALE' USING ERRCODE='40001'; END IF;
      RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','SUCCEEDED',
        'duplicate_completion',false,'continuation_enqueued',v_continuation_enqueued,
        'continuation_jobs',v_continuation_jobs,'continuation_count',v_continuation_count,
        'next_recommended_action',v_result_json->>'next_action','completed_at_utc',clock_timestamp());
    END IF;
  END IF;

  v_seeded_count := CASE WHEN COALESCE(v_result_json->>'seeded_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'seeded_count')::integer ELSE 0 END;
  v_new_scope_count := CASE WHEN COALESCE(v_result_json->>'new_scope_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'new_scope_count')::integer ELSE 0 END;
  v_processed_count := CASE WHEN COALESCE(v_result_json->>'processed_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'processed_count')::integer ELSE 0 END;
  v_ready_count_delta := CASE WHEN COALESCE(v_result_json->>'ready_count_delta', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'ready_count_delta')::integer ELSE 0 END;
  v_materialised_count := CASE
    WHEN COALESCE(v_result_json->>'materialised_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'materialised_count')::integer
    WHEN COALESCE(v_result_json->>'materialized_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'materialized_count')::integer
    ELSE 0
  END;
  v_error_count := CASE
    WHEN COALESCE(v_result_json->>'error_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'error_count')::integer
    WHEN COALESCE(v_result_json->>'materialisation_error_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'materialisation_error_count')::integer
    ELSE 0
  END;
  v_source_rows_written := CASE
    WHEN COALESCE(v_result_json->>'source_rows_written', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'source_rows_written')::integer
    WHEN COALESCE(v_result_json->>'published_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'published_count')::integer
    ELSE 0
  END;
  v_current_source_row_count := CASE
    WHEN COALESCE(v_result_json->>'current_source_row_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'current_source_row_count')::integer
    WHEN COALESCE(v_result_json->>'source_rows_written', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'source_rows_written')::integer
    WHEN COALESCE(v_result_json->>'published_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'published_count')::integer
    ELSE 0
  END;
  v_current_source_row_count_authoritative := LOWER(BTRIM(COALESCE(
    v_result_json->>'current_source_row_count_authoritative',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR (
      v_stage_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND v_job_row.economic_build_id IS NOT NULL
      AND COALESCE(v_has_more, false) IS NOT TRUE
      AND UPPER(BTRIM(COALESCE(v_result_json->>'private_stage', ''))) = 'COMPLETE'
      AND UPPER(BTRIM(COALESCE(v_result_json->>'stage_status', ''))) = 'COMPLETE'
      AND COALESCE(v_result_json->>'published_count', '') ~ '^-?[0-9]+$'
    );
  v_source_build_run_id_text := COALESCE(
    NULLIF(BTRIM(COALESCE(v_result_json->>'source_build_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'source_build_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_job_row.payload_json#>>'{source_build,source_build_run_id}', '')), '')
  );
  v_source_change_seq := CASE
    WHEN COALESCE(v_result_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (v_result_json->>'source_change_seq')::bigint
    WHEN COALESCE(v_job_row.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (v_job_row.payload_json->>'source_change_seq')::bigint
    WHEN COALESCE(v_job_row.payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN (v_job_row.payload_json->>'source_change_sequence')::bigint
    ELSE NULL::bigint
  END;
  v_result_session_version := CASE
    WHEN COALESCE(v_result_json->>'session_version', '') ~ '^[0-9]{1,18}$' THEN (v_result_json->>'session_version')::bigint
    WHEN COALESCE(v_job_row.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$' THEN (v_job_row.payload_json->>'session_version')::bigint
    ELSE NULL::bigint
  END;
  v_source_seed_cursor_json := jsonb_strip_nulls(jsonb_build_object(
    'source_build_run_id', v_source_build_run_id_text,
    'source_change_seq', v_source_change_seq,
    'session_version', v_result_session_version,
    'source_snapshot_run_id', CASE WHEN v_job_row.snapshot_run_id IS NULL THEN NULL ELSE v_job_row.snapshot_run_id::text END,
    'source_row_cursor', 'START'
  ));
  v_payload_limit := LEAST(
    GREATEST(
      CASE
        WHEN COALESCE(NULLIF(BTRIM(v_job_row.payload_json->>'line_limit'), ''), NULLIF(BTRIM(v_job_row.payload_json->>'limit'), ''), '') ~ '^[0-9]+$'
          THEN COALESCE(NULLIF(BTRIM(v_job_row.payload_json->>'line_limit'), ''), NULLIF(BTRIM(v_job_row.payload_json->>'limit'), ''))::integer
        ELSE 100
      END,
      1
    ),
    100
  );

  IF v_job_row.session_id IS NOT NULL THEN
    SELECT session_row.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_job_row.session_id;

    v_has_open_session := FOUND
      AND UPPER(BTRIM(COALESCE(v_session_row.status, ''))) = 'OPEN'
      AND v_session_row.discarded_at_utc IS NULL;
  END IF;

  IF v_duplicate_completion IS NOT TRUE
     AND v_stage_job_type IN (
       'WORKBENCH_SESSION_SCOPE_SEED',
       'WORKBENCH_CANDIDATE_SOURCE_BUILD',
       'WORKBENCH_CANDIDATE_DELTA_REFRESH',
       'WORKBENCH_SESSION_CLONE_REBASE',
       'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
       'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
       'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
     )
     AND v_job_row.session_id IS NOT NULL
     AND v_has_open_session IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_COMPLETE_JOB_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_COMPLETE_JOB_SESSION_NOT_OPEN',
              'job_id', p_job_id::text,
              'session_id', v_job_row.session_id::text,
              'job_type', v_stage_job_type,
              'status', CASE WHEN v_session_row.id IS NULL THEN NULL ELSE v_session_row.status END,
              'discarded_at_utc', CASE WHEN v_session_row.id IS NULL THEN NULL ELSE v_session_row.discarded_at_utc END
            )::text;
  END IF;

  IF v_duplicate_completion IS NOT TRUE
     AND v_stage_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_delta_fallback_required := LOWER(BTRIM(COALESCE(v_result_json->>'fallback_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_fallback_reason := COALESCE(NULLIF(BTRIM(COALESCE(v_result_json->>'fallback_reason', '')), ''), 'DELTA_REFRESH_FALLBACK_REQUIRED');
    v_delta_refresh_complete := LOWER(BTRIM(COALESCE(v_result_json->>'delta_refresh_complete', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_more_due := LOWER(BTRIM(COALESCE(v_result_json->>'more_due', v_result_json->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_made_progress := LOWER(BTRIM(COALESCE(v_result_json->>'made_progress', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_projection_run_id_text := COALESCE(
      NULLIF(BTRIM(COALESCE(v_result_json->>'projection_run_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'projection_run_id', '')), '')
    );
    v_delta_next_cursor_json := CASE
      WHEN jsonb_typeof(v_result_json->'next_cursor_json') = 'object' THEN v_result_json->'next_cursor_json'
      WHEN jsonb_typeof(v_result_json->'next_cursor') = 'object' THEN v_result_json->'next_cursor'
      WHEN jsonb_typeof(v_job_row.payload_json->'cursor_json') = 'object' THEN v_job_row.payload_json->'cursor_json'
      ELSE '{}'::jsonb
    END;
    v_delta_targeted_timesheet_ids_json := CASE
      WHEN jsonb_typeof(v_result_json->'targeted_timesheet_ids') = 'array' THEN v_result_json->'targeted_timesheet_ids'
      WHEN jsonb_typeof(v_job_row.payload_json->'targeted_timesheet_ids') = 'array' THEN v_job_row.payload_json->'targeted_timesheet_ids'
      ELSE '[]'::jsonb
    END;
    v_delta_linked_timesheet_ids_json := CASE
      WHEN jsonb_typeof(v_result_json->'linked_timesheet_ids') = 'array' THEN v_result_json->'linked_timesheet_ids'
      WHEN jsonb_typeof(v_job_row.payload_json->'linked_timesheet_ids') = 'array' THEN v_job_row.payload_json->'linked_timesheet_ids'
      ELSE '[]'::jsonb
    END;
    v_delta_source_rows_written := CASE WHEN COALESCE(v_result_json->>'source_rows_written', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'source_rows_written')::integer ELSE 0 END;
    v_delta_line_rows_written := CASE WHEN COALESCE(v_result_json->>'line_rows_written', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'line_rows_written')::integer ELSE 0 END;
    v_delta_preview_rows_written := CASE WHEN COALESCE(v_result_json->>'preview_rows_written', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'preview_rows_written')::integer ELSE 0 END;
    v_delta_rows_superseded := CASE WHEN COALESCE(v_result_json->>'rows_superseded', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'rows_superseded')::integer ELSE 0 END;
    v_delta_shadow_compare_status := UPPER(BTRIM(COALESCE(v_result_json->>'shadow_compare_status', '')));
    v_delta_shadow_compare_required := LOWER(BTRIM(COALESCE(
      v_result_json->>'shadow_compare_required',
      v_job_row.payload_json->>'shadow_compare_required',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_shadow_compare_enforced := LOWER(BTRIM(COALESCE(
      v_result_json->>'shadow_compare_enforced',
      v_job_row.payload_json->>'shadow_compare_enforced',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_delta_shadow_compare_enforced := COALESCE(v_delta_shadow_compare_required, false)
      AND COALESCE(v_delta_shadow_compare_enforced, false);
    v_delta_shadow_compare_failed := LOWER(BTRIM(COALESCE(v_result_json->>'shadow_compare_failed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR v_delta_shadow_compare_status = 'MISMATCH'
      OR (
        COALESCE(v_delta_shadow_compare_enforced, false) IS TRUE
        AND v_delta_shadow_compare_status IN ('REFERENCE_UNAVAILABLE', 'REFERENCE_MISSING', 'UNAVAILABLE')
      );
    IF v_delta_shadow_compare_failed IS TRUE THEN
      v_delta_fallback_required := true;
      v_delta_fallback_reason := CASE
        WHEN COALESCE(v_delta_shadow_compare_enforced, false) IS TRUE
         AND v_delta_shadow_compare_status IN ('REFERENCE_UNAVAILABLE', 'REFERENCE_MISSING', 'UNAVAILABLE')
          THEN 'SHADOW_REFERENCE_UNAVAILABLE'
        ELSE 'SHADOW_COMPARE_MISMATCH'
      END;
    END IF;

    SELECT COALESCE(array_agg(DISTINCT parsed_target.timesheet_id ORDER BY parsed_target.timesheet_id), ARRAY[]::uuid[])
    INTO v_delta_recovery_targeted_timesheet_ids
    FROM (
      SELECT NULLIF(BTRIM(target_value.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb)) AS target_value(value)
      WHERE NULLIF(BTRIM(target_value.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) AS parsed_target;

    SELECT COALESCE(array_agg(DISTINCT parsed_linked.timesheet_id ORDER BY parsed_linked.timesheet_id), ARRAY[]::uuid[])
    INTO v_delta_recovery_linked_timesheet_ids
    FROM (
      SELECT NULLIF(BTRIM(linked_value.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb)) AS linked_value(value)
      WHERE NULLIF(BTRIM(linked_value.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) AS parsed_linked;

    v_delta_projection_mode := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
      v_result_json->>'projection_mode',
      v_job_row.payload_json->>'projection_mode',
      v_result_json->>'resolved_mode',
      v_job_row.payload_json->>'resolved_mode',
      'DELTA'
    ))), ''), 'DELTA');

    v_delta_projection_class := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
      v_result_json->>'projection_class',
      v_job_row.payload_json->>'projection_class',
      'UNKNOWN'
    ))), ''), 'UNKNOWN');

    v_delta_refresh_scope_kind := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
      v_result_json->>'refresh_scope_kind',
      v_job_row.payload_json->>'refresh_scope_kind',
      ''
    ))), ''), CASE
      WHEN COALESCE(array_length(v_delta_recovery_targeted_timesheet_ids, 1), 0) > 0
        OR COALESCE(array_length(v_delta_recovery_linked_timesheet_ids, 1), 0) > 0
      THEN 'TARGETED_TIMESHEETS'
      ELSE 'CANDIDATE_FULL_LIVE'
    END);

    v_delta_lifecycle_context := LOWER(BTRIM(COALESCE(
      v_result_json->>'lifecycle_mutation_context',
      v_job_row.payload_json->>'lifecycle_mutation_context',
      v_result_json->>'mutation_context',
      v_job_row.payload_json->>'mutation_context',
      v_result_json->>'lifecycle_context',
      v_job_row.payload_json->>'lifecycle_context',
      v_result_json->>'trigger_source',
      v_job_row.payload_json->>'trigger_source',
      v_result_json->>'reason',
      v_job_row.payload_json->>'reason_latest',
      v_job_row.payload_json->>'reason',
      ''
    )));

    v_delta_is_normal_targeted_lifecycle := v_delta_projection_mode = 'DELTA'
      AND v_delta_projection_class = 'NORMAL_TIMESHEET'
      AND v_delta_refresh_scope_kind = 'TARGETED_TIMESHEETS'
      AND COALESCE(array_length(v_delta_recovery_targeted_timesheet_ids, 1), 0) > 0
      AND COALESCE(v_delta_shadow_compare_failed, false) IS NOT TRUE
      AND (
        COALESCE(v_delta_fallback_required, false) IS NOT TRUE
        OR UPPER(BTRIM(COALESCE(v_delta_fallback_reason, ''))) = 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED'
        OR LOWER(BTRIM(COALESCE(v_result_json->>'identical_cursor_self_reuse_suppressed', v_result_json->>'continuation_not_enqueued', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
      AND LOWER(BTRIM(COALESCE(v_result_json->>'force_legacy', v_job_row.payload_json->>'force_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(v_result_json->>'force_broad_legacy', v_job_row.payload_json->>'force_broad_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(v_result_json->>'source_build_required', v_job_row.payload_json->>'source_build_required', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(v_result_json->>'ordinary_timesheet_edit_save_no_dirty', v_job_row.payload_json->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND (
        LOWER(BTRIM(COALESCE(v_result_json->>'authorise_boundary_changed', v_job_row.payload_json->>'authorise_boundary_changed', v_result_json->>'timesheet_authorise_boundary_changed', v_job_row.payload_json->>'timesheet_authorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR v_delta_lifecycle_context IN ('timesheet_authorise', 'authorise_timesheet', 'timesheet_unauthorise', 'unauthorise_timesheet')
        OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%authorise%'
        OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%unauthorise%'
      );

    v_delta_next_cursor_json := COALESCE(v_delta_next_cursor_json, '{}'::jsonb)
      || jsonb_strip_nulls(jsonb_build_object(
        'projection_run_id', v_delta_projection_run_id_text,
        'phase', COALESCE(NULLIF(BTRIM(COALESCE(v_result_json->>'next_phase', '')), ''), NULLIF(BTRIM(COALESCE(v_result_json->>'phase', '')), ''), NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'phase', '')), '')),
        'cursor', CASE
          WHEN jsonb_typeof(v_delta_next_cursor_json->'cursor') = 'object' THEN v_delta_next_cursor_json->'cursor'
          WHEN jsonb_typeof(v_job_row.payload_json->'cursor_json') = 'object' THEN v_job_row.payload_json->'cursor_json'
          ELSE '{}'::jsonb
        END,
        'write_phase', COALESCE(NULLIF(BTRIM(COALESCE(v_result_json->>'write_phase', '')), ''), NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'write_phase', '')), '')),
        'write_cursor_json', CASE
          WHEN jsonb_typeof(v_result_json->'write_cursor_json') = 'object' THEN v_result_json->'write_cursor_json'
          WHEN jsonb_typeof(v_delta_next_cursor_json->'write_cursor_json') = 'object' THEN v_delta_next_cursor_json->'write_cursor_json'
          WHEN jsonb_typeof(v_job_row.payload_json->'write_cursor_json') = 'object' THEN v_job_row.payload_json->'write_cursor_json'
          ELSE NULL::jsonb
        END,
        'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
        'source_change_seq', CASE WHEN COALESCE(v_result_json->>'source_change_seq', v_job_row.payload_json->>'source_change_seq', '') ~ '^-?[0-9]+$' THEN COALESCE(v_result_json->>'source_change_seq', v_job_row.payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END
      ));

    IF v_job_row.candidate_id IS NOT NULL
       AND v_source_change_seq IS NOT NULL THEN
      SELECT COALESCE(change_counter.seq, 0)
      INTO v_live_source_change_seq
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key = 'pay_candidate:' || v_job_row.candidate_id::text;

      v_live_source_change_seq := COALESCE(v_live_source_change_seq, 0);

      IF COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0) THEN
        v_delta_superseded_by_live_source_guard := true;
        v_delta_more_due := false;
        v_delta_fallback_required := false;
        v_delta_refresh_complete := true;
        v_delta_fallback_reason := 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ';

        v_result_json := jsonb_strip_nulls(
          COALESCE(v_result_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'complete_job_live_source_guard_applied', true,
            'source_change_seq', v_source_change_seq,
            'newer_source_change_seq', v_live_source_change_seq,
            'live_source_change_seq', v_live_source_change_seq,
            'fallback_required', false,
            'delta_refresh_complete', true,
            'more_due', false,
            'has_more', false,
            'stop_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ'
          )
        );

        IF v_delta_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
          UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
          SET status = 'FAILED',
              fallback_required = false,
              fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
              diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
                || jsonb_build_object(
                  'superseded_by_newer_source_change_seq', true,
                  'complete_job_live_source_guard_applied', true,
                  'source_change_seq', v_source_change_seq,
                  'newer_source_change_seq', v_live_source_change_seq,
                  'terminalised_by', 'pay_workbench_complete_job',
                  'terminalised_at_utc', v_now::text
                ),
              updated_at_utc = v_now,
              completed_at_utc = v_now
          WHERE projection_run_update.id = v_delta_projection_run_id_text::uuid
            AND projection_run_update.session_id = v_job_row.session_id
            AND projection_run_update.candidate_id = v_job_row.candidate_id
            AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';

          GET DIAGNOSTICS v_delta_superseded_projection_runs = ROW_COUNT;
        END IF;
      END IF;
    ELSE
      v_live_source_change_seq := 0;
    END IF;

    IF v_delta_superseded_by_live_source_guard IS TRUE THEN
      UPDATE public.banking_pay_workbench_session_scope AS delta_superseded_scope
      SET dirty = true,
          pending_job_id = CASE WHEN delta_superseded_scope.pending_job_id = p_job_id THEN NULL::uuid ELSE delta_superseded_scope.pending_job_id END,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE delta_superseded_scope.session_id = v_job_row.session_id
        AND delta_superseded_scope.candidate_id = v_job_row.candidate_id;

      v_continuation_jobs := '[]'::jsonb;
      v_continuation_count := 0;
      v_continuation_reused_count := 0;
      v_continuation_enqueued := false;
      v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
    ELSIF v_delta_more_due IS TRUE THEN
      v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
        p_session_id => v_job_row.session_id,
        p_candidate_id => v_job_row.candidate_id,
        p_job_type => 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        p_cursor_json => COALESCE(v_delta_next_cursor_json, '{}'::jsonb),
        p_source_job_id => p_job_id,
        p_result_json => v_result_json,
        p_actor_user_id => v_session_row.actor_user_id,
        p_reason => 'DELTA_REFRESH_MORE_DUE',
        p_priority => COALESCE(v_job_row.priority, 43),
        p_limit => v_payload_limit
      );

      v_continuation_job_id_text := NULLIF(BTRIM(COALESCE(v_continuation_result->>'job_id', '')), '');

      IF LOWER(BTRIM(COALESCE(
           v_continuation_result->>'identical_cursor_self_reuse_suppressed',
           v_continuation_result->>'continuation_not_enqueued',
           'false'
         ))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
        IF v_job_row.candidate_id IS NOT NULL
           AND v_source_change_seq IS NOT NULL THEN
          SELECT COALESCE(change_counter.seq, 0)
          INTO v_live_source_change_seq
          FROM public.app_change_counters AS change_counter
          WHERE change_counter.entity_key = 'pay_candidate:' || v_job_row.candidate_id::text;

          v_live_source_change_seq := COALESCE(v_live_source_change_seq, 0);
        END IF;

        IF v_delta_is_normal_targeted_lifecycle IS TRUE THEN
          v_delta_recovery_source_change_seq := GREATEST(COALESCE(v_live_source_change_seq, 0), COALESCE(v_source_change_seq, 0));
          v_delta_more_due := false;
          v_delta_fallback_required := false;
          v_delta_refresh_complete := true;
          v_delta_fallback_reason := 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED';
          v_delta_superseded_by_live_source_guard := COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0);

          v_result_json := jsonb_strip_nulls(
            COALESCE(v_result_json, '{}'::jsonb)
            || jsonb_build_object(
              'identical_cursor_self_reuse_suppressed', true,
              'delta_identical_cursor_self_reuse_no_source_build', true,
              'fallback_required', false,
              'fallback_reason', v_delta_fallback_reason,
              'delta_refresh_complete', true,
              'more_due', false,
              'has_more', false,
              'source_change_seq', COALESCE(v_source_change_seq, 0),
              'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
              'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
              'stop_reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_NO_SOURCE_BUILD_SUCCESSOR_REQUIRED',
              'next_recommended_action', 'DELTA_REFRESH_CHUNK'
            )
          );

          IF v_delta_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
            SET status = 'FAILED',
                fallback_required = false,
                fallback_reason = v_delta_fallback_reason,
                diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
                  || jsonb_build_object(
                    'identical_cursor_self_reuse_suppressed', true,
                    'delta_identical_cursor_self_reuse_no_source_build', true,
                    'successor_required', true,
                    'continuation_result', COALESCE(v_continuation_result, '{}'::jsonb),
                    'source_change_seq', COALESCE(v_source_change_seq, 0),
                    'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                    'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
                    'terminalised_by', 'pay_workbench_complete_job',
                    'terminalised_at_utc', v_now::text
                  ),
                updated_at_utc = v_now,
                completed_at_utc = v_now
            WHERE projection_run_update.id = v_delta_projection_run_id_text::uuid
              AND projection_run_update.session_id = v_job_row.session_id
              AND projection_run_update.candidate_id = v_job_row.candidate_id
              AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';
          END IF;

          v_delta_recovery_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
            p_snapshot_run_id => v_session_row.source_snapshot_run_id,
            p_candidate_id => v_job_row.candidate_id,
            p_reason => 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DELTA_SUCCESSOR',
            p_actor_user_id => v_session_row.actor_user_id,
            p_payload_json => jsonb_strip_nulls(
              (COALESCE(v_job_row.payload_json, '{}'::jsonb) - ARRAY[
                'cursor',
                'cursor_json',
                'next_cursor',
                'next_cursor_json',
                'source_cursor',
                'write_cursor_json',
                'candidate_cursor',
                'cursor_token',
                'has_cursor',
                'continuation_reason',
                'source_job_id',
                'continuation_source_job_id',
                'bounded_continuation_source_job_id',
                'parent_job_id',
                'next_phase',
                'write_phase',
                'source_result_summary',
                'source_result_has_more',
                'source_result_next_cursor_present',
                'projection_run_id'
              ]::text[])
              || jsonb_build_object(
                'session_id', v_job_row.session_id::text,
                'source_session_id', v_job_row.session_id::text,
                'workbench_session_id', v_job_row.session_id::text,
                'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                'session_version', COALESCE(v_session_row.version, 0),
                'session_signature', v_session_row.session_signature,
                'force_legacy', false,
                'force_broad_legacy', false,
                'fallback_from_delta', false,
                'fallback_reason', v_delta_fallback_reason,
                'projection_mode', 'DELTA',
                'projection_class', 'NORMAL_TIMESHEET',
                'refresh_scope_kind', 'TARGETED_TIMESHEETS',
                'phase', 'INIT_PREFLIGHT',
                'cursor_json', '{}'::jsonb,
                'source_build_required', false,
                'line_work_required', false,
                'delta_refresh_required', true,
                'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
                'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
                'source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                'source_change_sequence', COALESCE(v_delta_recovery_source_change_seq, 0),
                'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                'identical_cursor_self_reuse_suppressed', true,
                'delta_identical_cursor_self_reuse_no_source_build', true,
                'successor_for_job_id', p_job_id::text,
                'previous_projection_run_id', v_delta_projection_run_id_text,
                'authorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on'),
                'unauthorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                'lifecycle_mutation_context', CASE
                  WHEN LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                    OR v_delta_lifecycle_context IN ('timesheet_unauthorise', 'unauthorise_timesheet')
                    OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%unauthorise%'
                  THEN 'timesheet_unauthorise'
                  ELSE 'timesheet_authorise'
                END,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
                'policy_x_dirtying_only', true,
                'economic_truth_mutation_allowed', false
              )
            )
          );

          v_delta_recovery_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_id', '')), '');

          IF v_delta_recovery_job_id_text = p_job_id::text THEN
            v_delta_recovery_job_id_text := NULL;
          END IF;

          IF v_delta_recovery_job_id_text IS NULL THEN
            v_delta_recovery_enqueue_result := public.pay_workbench_dirty_event_enqueue(
              p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
              p_scope_kind => 'CANDIDATE',
              p_scope_id => v_job_row.candidate_id::text,
              p_candidate_id => v_job_row.candidate_id,
              p_targeted_timesheet_ids => v_delta_recovery_targeted_timesheet_ids,
              p_linked_timesheet_ids => v_delta_recovery_linked_timesheet_ids,
              p_payload_json => jsonb_strip_nulls(
                COALESCE(v_job_row.payload_json, '{}'::jsonb)
                || jsonb_build_object(
                  'session_id', v_job_row.session_id::text,
                  'source_session_id', v_job_row.session_id::text,
                  'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                  'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                  'session_version', COALESCE(v_session_row.version, 0),
                  'session_signature', v_session_row.session_signature,
                  'refresh_scope_kind', 'TARGETED_TIMESHEETS',
                  'projection_mode', 'DELTA',
                  'projection_class', 'NORMAL_TIMESHEET',
                  'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
                  'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
                  'source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                  'source_change_sequence', COALESCE(v_delta_recovery_source_change_seq, 0),
                  'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                  'reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DIRTY_RECOVERY',
                  'fallback_reason', v_delta_fallback_reason,
                  'source_build_required', false,
                  'line_work_required', false,
                  'delta_refresh_required', true,
                  'identical_cursor_self_reuse_suppressed', true,
                  'delta_identical_cursor_self_reuse_no_source_build', true,
                  'authorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on'),
                  'unauthorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
                  'lifecycle_mutation_context', CASE
                    WHEN LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                      OR v_delta_lifecycle_context IN ('timesheet_unauthorise', 'unauthorise_timesheet')
                      OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%unauthorise%'
                    THEN 'timesheet_unauthorise'
                    ELSE 'timesheet_authorise'
                  END,
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
                  'policy_x_dirtying_only', true,
                  'economic_truth_mutation_allowed', false
                )
              ),
              p_reason => 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DIRTY_RECOVERY',
              p_priority => -1000,
              p_run_at_utc => v_now
            );

            v_delta_recovery_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_id', '')), '');

            IF v_delta_recovery_job_id_text IS NULL
               AND jsonb_typeof(COALESCE(v_delta_recovery_enqueue_result->'coalesced_scope_results', '[]'::jsonb)) = 'array' THEN
              SELECT NULLIF(BTRIM(COALESCE(coalesced_scope_result.value->>'job_id', '')), '')
              INTO v_delta_recovery_job_id_text
              FROM jsonb_array_elements(COALESCE(v_delta_recovery_enqueue_result->'coalesced_scope_results', '[]'::jsonb)) AS coalesced_scope_result(value)
              WHERE COALESCE(coalesced_scope_result.value->>'session_id', '') = v_job_row.session_id::text
                AND COALESCE(coalesced_scope_result.value->>'job_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              LIMIT 1;
            END IF;

            IF v_delta_recovery_job_id_text = p_job_id::text THEN
              v_delta_recovery_job_id_text := NULL;
            END IF;
          END IF;

          IF v_delta_recovery_job_id_text IS NULL
             OR v_delta_recovery_job_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_DELTA_SELF_REUSE_RECOVERY_JOB_ID_MISSING'
              USING ERRCODE = 'P0001',
                    DETAIL = jsonb_build_object(
                      'code', 'PAY_WORKBENCH_DELTA_SELF_REUSE_RECOVERY_JOB_ID_MISSING',
                      'job_id', p_job_id::text,
                      'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
                      'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL ELSE v_job_row.candidate_id::text END,
                      'projection_run_id', v_delta_projection_run_id_text,
                      'source_change_seq', v_source_change_seq,
                      'recovery_enqueue_result', COALESCE(v_delta_recovery_enqueue_result, '{}'::jsonb),
                      'message', 'Normal targeted lifecycle DELTA self-reuse suppression could not create or reuse a cron-claimable recovery job.'
                    )::text;
          END IF;

          v_delta_recovery_job_id := v_delta_recovery_job_id_text::uuid;

          UPDATE public.banking_pay_workbench_session_scope AS delta_recovery_scope
          SET status = CASE
                WHEN UPPER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_type', ''))) = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
                 AND LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'dirty_enqueue_coalesced_all_relevant_scopes', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
                  THEN 'PENDING'
                ELSE 'DELTA_REFRESH_PENDING'
              END,
              dirty = true,
              pending_job_id = v_delta_recovery_job_id,
              error_json = NULL::jsonb,
              updated_at_utc = v_now
          WHERE delta_recovery_scope.session_id = v_job_row.session_id
            AND delta_recovery_scope.candidate_id = v_job_row.candidate_id;

          v_continuation_jobs := jsonb_build_array(v_continuation_result, v_delta_recovery_enqueue_result);
          v_continuation_count := 1;
          v_continuation_reused_count := CASE
            WHEN LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'dirty_enqueue_coalesced_all_relevant_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            THEN 1
            ELSE 0
          END;
          v_continuation_enqueued := true;
          v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
        ELSIF v_job_row.candidate_id IS NOT NULL
           AND v_source_change_seq IS NOT NULL
           AND COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0) THEN
          v_delta_more_due := false;
          v_delta_fallback_required := false;
          v_delta_refresh_complete := true;
          v_delta_fallback_reason := 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ';
          v_delta_superseded_by_live_source_guard := true;

          v_result_json := jsonb_strip_nulls(
            COALESCE(v_result_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'complete_job_live_source_guard_applied', true,
              'identical_cursor_self_reuse_suppressed', true,
              'source_change_seq', v_source_change_seq,
              'newer_source_change_seq', v_live_source_change_seq,
              'live_source_change_seq', v_live_source_change_seq,
              'fallback_required', false,
              'delta_refresh_complete', true,
              'more_due', false,
              'has_more', false,
              'stop_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ'
            )
          );

          IF v_delta_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
            SET status = 'FAILED',
                fallback_required = false,
                fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
                diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
                  || jsonb_build_object(
                    'superseded_by_newer_source_change_seq', true,
                    'complete_job_live_source_guard_applied', true,
                    'identical_cursor_self_reuse_suppressed', true,
                    'source_change_seq', v_source_change_seq,
                    'newer_source_change_seq', v_live_source_change_seq,
                    'terminalised_by', 'pay_workbench_complete_job',
                    'terminalised_at_utc', v_now::text
                  ),
                updated_at_utc = v_now,
                completed_at_utc = v_now
            WHERE projection_run_update.id = v_delta_projection_run_id_text::uuid
              AND projection_run_update.session_id = v_job_row.session_id
              AND projection_run_update.candidate_id = v_job_row.candidate_id
              AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';

            GET DIAGNOSTICS v_delta_superseded_projection_runs = ROW_COUNT;
          END IF;

          UPDATE public.banking_pay_workbench_session_scope AS delta_superseded_scope
          SET dirty = true,
              pending_job_id = CASE WHEN delta_superseded_scope.pending_job_id = p_job_id THEN NULL::uuid ELSE delta_superseded_scope.pending_job_id END,
              error_json = NULL::jsonb,
              updated_at_utc = v_now
          WHERE delta_superseded_scope.session_id = v_job_row.session_id
            AND delta_superseded_scope.candidate_id = v_job_row.candidate_id;

          v_continuation_jobs := jsonb_build_array(v_continuation_result);
          v_continuation_count := 0;
          v_continuation_reused_count := 0;
          v_continuation_enqueued := false;
          v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
        ELSE
          v_delta_more_due := false;
          v_delta_fallback_required := true;
          v_delta_fallback_reason := 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED';

          IF v_delta_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
            SET status = 'FALLBACK_REQUIRED',
                fallback_required = true,
                fallback_reason = v_delta_fallback_reason,
                diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
                  || jsonb_build_object(
                    'identical_cursor_self_reuse_suppressed', true,
                    'continuation_result', COALESCE(v_continuation_result, '{}'::jsonb),
                    'terminalised_by', 'pay_workbench_complete_job',
                    'terminalised_at_utc', v_now::text
                  ),
                updated_at_utc = v_now,
                completed_at_utc = v_now
            WHERE projection_run_update.id = v_delta_projection_run_id_text::uuid
              AND projection_run_update.session_id = v_job_row.session_id
              AND projection_run_update.candidate_id = v_job_row.candidate_id
              AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';
          END IF;

          v_delta_fallback_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
            p_snapshot_run_id => v_session_row.source_snapshot_run_id,
            p_candidate_id => v_job_row.candidate_id,
            p_reason => v_delta_fallback_reason,
            p_actor_user_id => v_session_row.actor_user_id,
            p_payload_json => jsonb_strip_nulls(
              jsonb_build_object(
                'session_id', v_job_row.session_id::text,
                'source_session_id', v_job_row.session_id::text,
                'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
                'session_version', COALESCE(v_session_row.version, 0),
                'session_signature', v_session_row.session_signature,
                'force_legacy', true,
                'fallback_from_delta', true,
                'fallback_reason', v_delta_fallback_reason,
                'projection_run_id', v_delta_projection_run_id_text,
                'projection_class', COALESCE(NULLIF(BTRIM(COALESCE(v_result_json->>'projection_class', '')), ''), NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'projection_class', '')), '')),
                'source_build_required', true,
                'line_work_required', true,
                'delta_refresh_required', false,
                'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
                'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
                'refresh_scope_kind', CASE WHEN jsonb_array_length(COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb)) > 0 OR jsonb_array_length(COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb)) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END
              )
            )
          );

          v_delta_fallback_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_fallback_enqueue_result->>'job_id', '')), '');

          UPDATE public.banking_pay_workbench_session_scope AS delta_fallback_scope
          SET status = 'SOURCE_BUILD_PENDING',
              dirty = true,
              pending_job_id = CASE WHEN v_delta_fallback_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN v_delta_fallback_job_id_text::uuid ELSE delta_fallback_scope.pending_job_id END,
              error_json = NULL::jsonb,
              updated_at_utc = v_now
          WHERE delta_fallback_scope.session_id = v_job_row.session_id
            AND delta_fallback_scope.candidate_id = v_job_row.candidate_id;

          v_continuation_jobs := jsonb_build_array(v_continuation_result, v_delta_fallback_enqueue_result);
          v_continuation_count := CASE WHEN v_delta_fallback_job_id_text IS NULL THEN 0 ELSE 1 END;
          v_continuation_reused_count := CASE WHEN LOWER(BTRIM(COALESCE(v_delta_fallback_enqueue_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
          v_continuation_enqueued := v_delta_fallback_job_id_text IS NOT NULL;
          v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
        END IF;
      ELSE
        IF v_continuation_job_id_text IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_DELTA_CONTINUATION_JOB_ID_MISSING'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_DELTA_CONTINUATION_JOB_ID_MISSING',
                    'job_id', p_job_id::text,
                    'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
                    'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL ELSE v_job_row.candidate_id::text END,
                    'projection_run_id', v_delta_projection_run_id_text,
                    'source_change_seq', v_source_change_seq,
                    'continuation_result', v_continuation_result,
                    'message', 'Delta more_due completion did not receive a concrete continuation job id.'
                  )::text;
        END IF;

        IF v_continuation_job_id_text = p_job_id::text THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_DELTA_CONTINUATION_SELF_REUSE_GUARD_FAILED'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_DELTA_CONTINUATION_SELF_REUSE_GUARD_FAILED',
                    'job_id', p_job_id::text,
                    'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
                    'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL ELSE v_job_row.candidate_id::text END,
                    'projection_run_id', v_delta_projection_run_id_text,
                    'source_change_seq', v_source_change_seq,
                    'continuation_result', v_continuation_result,
                    'message', 'Delta continuation helper returned the currently completing job as its own continuation.'
                  )::text;
        END IF;

        v_continuation_jobs := jsonb_build_array(v_continuation_result);
        v_continuation_count := 1;
        v_continuation_reused_count := CASE WHEN LOWER(BTRIM(COALESCE(v_continuation_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
        v_continuation_enqueued := true;
        v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
      END IF;
    ELSIF v_delta_fallback_required IS TRUE
          AND v_delta_is_normal_targeted_lifecycle IS TRUE
          AND UPPER(BTRIM(COALESCE(v_delta_fallback_reason, ''))) = 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED' THEN
      IF v_job_row.candidate_id IS NOT NULL
         AND v_source_change_seq IS NOT NULL THEN
        SELECT COALESCE(change_counter.seq, 0)
        INTO v_live_source_change_seq
        FROM public.app_change_counters AS change_counter
        WHERE change_counter.entity_key = 'pay_candidate:' || v_job_row.candidate_id::text;

        v_live_source_change_seq := COALESCE(v_live_source_change_seq, 0);
      END IF;

      v_delta_recovery_source_change_seq := GREATEST(COALESCE(v_live_source_change_seq, 0), COALESCE(v_source_change_seq, 0));
      v_delta_more_due := false;
      v_delta_fallback_required := false;
      v_delta_refresh_complete := true;
      v_delta_fallback_reason := 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED';
      v_delta_superseded_by_live_source_guard := COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0);

      v_continuation_result := jsonb_build_object(
        'ok', true,
        'action', 'DIRECT_FALLBACK_SELF_REUSE_SUPPRESSED_NO_SOURCE_BUILD',
        'continuation_not_enqueued', true,
        'identical_cursor_self_reuse_suppressed', true,
        'delta_identical_cursor_self_reuse_no_source_build', true,
        'normal_targeted_lifecycle_delta', true,
        'successor_required', true,
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
        'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
        'projection_run_id', v_delta_projection_run_id_text
      );

      v_result_json := jsonb_strip_nulls(
        COALESCE(v_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'identical_cursor_self_reuse_suppressed', true,
          'delta_identical_cursor_self_reuse_no_source_build', true,
          'direct_fallback_intercepted', true,
          'fallback_required', false,
          'fallback_reason', v_delta_fallback_reason,
          'delta_refresh_complete', true,
          'more_due', false,
          'has_more', false,
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
          'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
          'stop_reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DIRECT_FALLBACK_NO_SOURCE_BUILD_SUCCESSOR_REQUIRED',
          'next_recommended_action', 'DELTA_REFRESH_CHUNK'
        )
      );

      IF v_delta_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
        SET status = 'FAILED',
            fallback_required = false,
            fallback_reason = v_delta_fallback_reason,
            diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
              || jsonb_build_object(
                'identical_cursor_self_reuse_suppressed', true,
                'delta_identical_cursor_self_reuse_no_source_build', true,
                'direct_fallback_intercepted', true,
                'successor_required', true,
                'continuation_result', COALESCE(v_continuation_result, '{}'::jsonb),
                'source_change_seq', COALESCE(v_source_change_seq, 0),
                'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
                'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
                'terminalised_by', 'pay_workbench_complete_job',
                'terminalised_at_utc', v_now::text
              ),
            updated_at_utc = v_now,
            completed_at_utc = v_now
        WHERE projection_run_update.id = v_delta_projection_run_id_text::uuid
          AND projection_run_update.session_id = v_job_row.session_id
          AND projection_run_update.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) IN ('RUNNING', 'FALLBACK_REQUIRED');
      END IF;

      v_delta_recovery_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_session_row.source_snapshot_run_id,
        p_candidate_id => v_job_row.candidate_id,
        p_reason => 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DELTA_SUCCESSOR',
        p_actor_user_id => v_session_row.actor_user_id,
        p_payload_json => jsonb_strip_nulls(jsonb_build_object(
          'session_id', v_job_row.session_id::text,
          'source_session_id', v_job_row.session_id::text,
          'workbench_session_id', v_job_row.session_id::text,
          'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'session_version', COALESCE(v_session_row.version, 0),
          'session_signature', v_session_row.session_signature,
          'force_legacy', false,
          'force_broad_legacy', false,
          'fallback_from_delta', false,
          'fallback_reason', v_delta_fallback_reason,
          'projection_mode', 'DELTA',
          'projection_class', 'NORMAL_TIMESHEET',
          'refresh_scope_kind', 'TARGETED_TIMESHEETS',
          'phase', 'INIT_PREFLIGHT',
          'cursor_json', '{}'::jsonb,
          'source_build_required', false,
          'line_work_required', false,
          'delta_refresh_required', true,
          'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
          'source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
          'source_change_sequence', COALESCE(v_delta_recovery_source_change_seq, 0),
          'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
          'identical_cursor_self_reuse_suppressed', true,
          'delta_identical_cursor_self_reuse_no_source_build', true,
          'direct_fallback_intercepted', true,
          'successor_for_job_id', p_job_id::text,
          'previous_projection_run_id', v_delta_projection_run_id_text,
          'authorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on'),
          'unauthorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'lifecycle_mutation_context', CASE
            WHEN LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR v_delta_lifecycle_context IN ('timesheet_unauthorise', 'unauthorise_timesheet')
              OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%unauthorise%'
            THEN 'timesheet_unauthorise'
            ELSE 'timesheet_authorise'
          END,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'policy_x_dirtying_only', true,
          'economic_truth_mutation_allowed', false
        ))
      );

      v_delta_recovery_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_id', '')), '');

      IF v_delta_recovery_job_id_text = p_job_id::text THEN
        v_delta_recovery_job_id_text := NULL;
      END IF;

      IF v_delta_recovery_job_id_text IS NULL THEN
        v_delta_recovery_enqueue_result := public.pay_workbench_dirty_event_enqueue(
          p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
          p_scope_kind => 'CANDIDATE',
          p_scope_id => v_job_row.candidate_id::text,
          p_candidate_id => v_job_row.candidate_id,
          p_targeted_timesheet_ids => v_delta_recovery_targeted_timesheet_ids,
          p_linked_timesheet_ids => v_delta_recovery_linked_timesheet_ids,
          p_payload_json => jsonb_strip_nulls(jsonb_build_object(
            'session_id', v_job_row.session_id::text,
            'source_session_id', v_job_row.session_id::text,
            'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
            'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
            'session_version', COALESCE(v_session_row.version, 0),
            'session_signature', v_session_row.session_signature,
            'refresh_scope_kind', 'TARGETED_TIMESHEETS',
            'projection_mode', 'DELTA',
            'projection_class', 'NORMAL_TIMESHEET',
            'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
            'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
            'source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
            'source_change_sequence', COALESCE(v_delta_recovery_source_change_seq, 0),
            'latest_source_change_seq', COALESCE(v_delta_recovery_source_change_seq, 0),
            'reason', 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DIRTY_RECOVERY',
            'fallback_reason', v_delta_fallback_reason,
            'source_build_required', false,
            'line_work_required', false,
            'delta_refresh_required', true,
            'identical_cursor_self_reuse_suppressed', true,
            'delta_identical_cursor_self_reuse_no_source_build', true,
            'direct_fallback_intercepted', true,
            'authorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on'),
            'unauthorise_boundary_changed', LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
            'lifecycle_mutation_context', CASE
              WHEN LOWER(BTRIM(COALESCE(v_result_json->>'unauthorise_boundary_changed', v_job_row.payload_json->>'unauthorise_boundary_changed', v_result_json->>'timesheet_unauthorise_boundary_changed', v_job_row.payload_json->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                OR v_delta_lifecycle_context IN ('timesheet_unauthorise', 'unauthorise_timesheet')
                OR LOWER(COALESCE(v_result_json->>'reason', v_job_row.payload_json->>'reason_latest', v_job_row.payload_json->>'reason', '')) LIKE '%unauthorise%'
              THEN 'timesheet_unauthorise'
              ELSE 'timesheet_authorise'
            END,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
            'policy_x_dirtying_only', true,
            'economic_truth_mutation_allowed', false
          )),
          p_reason => 'DELTA_IDENTICAL_CURSOR_SELF_REUSE_SUPPRESSED_DIRTY_RECOVERY',
          p_priority => -1000,
          p_run_at_utc => v_now
        );

        v_delta_recovery_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_id', '')), '');

        IF v_delta_recovery_job_id_text IS NULL
           AND jsonb_typeof(COALESCE(v_delta_recovery_enqueue_result->'coalesced_scope_results', '[]'::jsonb)) = 'array' THEN
          SELECT NULLIF(BTRIM(COALESCE(coalesced_scope_result.value->>'job_id', '')), '')
          INTO v_delta_recovery_job_id_text
          FROM jsonb_array_elements(COALESCE(v_delta_recovery_enqueue_result->'coalesced_scope_results', '[]'::jsonb)) AS coalesced_scope_result(value)
          WHERE COALESCE(coalesced_scope_result.value->>'session_id', '') = v_job_row.session_id::text
            AND COALESCE(coalesced_scope_result.value->>'job_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          LIMIT 1;
        END IF;

        IF v_delta_recovery_job_id_text = p_job_id::text THEN
          v_delta_recovery_job_id_text := NULL;
        END IF;
      END IF;

      IF v_delta_recovery_job_id_text IS NULL
         OR v_delta_recovery_job_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DELTA_SELF_REUSE_RECOVERY_JOB_ID_MISSING'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_DELTA_SELF_REUSE_RECOVERY_JOB_ID_MISSING',
                  'job_id', p_job_id::text,
                  'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
                  'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL ELSE v_job_row.candidate_id::text END,
                  'projection_run_id', v_delta_projection_run_id_text,
                  'source_change_seq', v_source_change_seq,
                  'recovery_enqueue_result', COALESCE(v_delta_recovery_enqueue_result, '{}'::jsonb),
                  'message', 'Normal targeted lifecycle DELTA self-reuse fallback could not create or reuse a cron-claimable recovery job.'
                )::text;
      END IF;

      v_delta_recovery_job_id := v_delta_recovery_job_id_text::uuid;

      UPDATE public.banking_pay_workbench_session_scope AS delta_recovery_scope
      SET status = CASE
            WHEN UPPER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'job_type', ''))) = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
             AND LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'dirty_enqueue_coalesced_all_relevant_scopes', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
              THEN 'PENDING'
            ELSE 'DELTA_REFRESH_PENDING'
          END,
          dirty = true,
          pending_job_id = v_delta_recovery_job_id,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE delta_recovery_scope.session_id = v_job_row.session_id
        AND delta_recovery_scope.candidate_id = v_job_row.candidate_id;

      v_continuation_jobs := jsonb_build_array(v_continuation_result, v_delta_recovery_enqueue_result);
      v_continuation_count := 1;
      v_continuation_reused_count := CASE
        WHEN LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR LOWER(BTRIM(COALESCE(v_delta_recovery_enqueue_result->>'dirty_enqueue_coalesced_all_relevant_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        THEN 1
        ELSE 0
      END;
      v_continuation_enqueued := true;
      v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
    ELSIF v_delta_fallback_required IS TRUE THEN
      v_delta_fallback_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_session_row.source_snapshot_run_id,
        p_candidate_id => v_job_row.candidate_id,
        p_reason => COALESCE(v_delta_fallback_reason, 'DELTA_REFRESH_FALLBACK_REQUIRED'),
        p_actor_user_id => v_session_row.actor_user_id,
        p_payload_json => jsonb_strip_nulls(
          jsonb_build_object(
            'session_id', v_job_row.session_id::text,
            'source_session_id', v_job_row.session_id::text,
            'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
            'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
            'session_version', COALESCE(v_session_row.version, 0),
            'session_signature', v_session_row.session_signature,
            'force_legacy', true,
            'fallback_from_delta', true,
            'fallback_reason', v_delta_fallback_reason,
            'shadow_compare_failed', COALESCE(v_delta_shadow_compare_failed, false),
            'shadow_compare_status', NULLIF(v_delta_shadow_compare_status, ''),
            'shadow_compare_required', COALESCE(v_delta_shadow_compare_required, false),
            'shadow_compare_enforced', COALESCE(v_delta_shadow_compare_enforced, false),
            'projection_run_id', v_delta_projection_run_id_text,
            'projection_class', COALESCE(NULLIF(BTRIM(COALESCE(v_result_json->>'projection_class', '')), ''), NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'projection_class', '')), '')),
            'source_build_required', true,
            'line_work_required', true,
            'delta_refresh_required', false,
            'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
            'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb),
            'refresh_scope_kind', CASE WHEN jsonb_array_length(COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb)) > 0 OR jsonb_array_length(COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb)) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END
          )
        )
      );
      v_delta_fallback_job_id_text := NULLIF(BTRIM(COALESCE(v_delta_fallback_enqueue_result->>'job_id', '')), '');
      UPDATE public.banking_pay_workbench_session_scope AS delta_fallback_scope
      SET status = 'SOURCE_BUILD_PENDING',
          dirty = true,
          pending_job_id = CASE WHEN v_delta_fallback_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN v_delta_fallback_job_id_text::uuid ELSE delta_fallback_scope.pending_job_id END,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE delta_fallback_scope.session_id = v_job_row.session_id
        AND delta_fallback_scope.candidate_id = v_job_row.candidate_id;
      v_continuation_jobs := jsonb_build_array(v_delta_fallback_enqueue_result);
      v_continuation_count := CASE WHEN v_delta_fallback_job_id_text IS NULL THEN 0 ELSE 1 END;
      v_continuation_reused_count := CASE WHEN LOWER(BTRIM(COALESCE(v_delta_fallback_enqueue_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
      v_continuation_enqueued := v_delta_fallback_job_id_text IS NOT NULL;
      v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
    ELSIF v_delta_refresh_complete IS TRUE
          AND LOWER(BTRIM(COALESCE(v_result_json->>'superseded_by_newer_source_change_seq', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      UPDATE public.banking_pay_workbench_session_scope AS delta_superseded_scope
      SET dirty = true,
          pending_job_id = CASE WHEN delta_superseded_scope.pending_job_id = p_job_id THEN NULL::uuid ELSE delta_superseded_scope.pending_job_id END,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE delta_superseded_scope.session_id = v_job_row.session_id
        AND delta_superseded_scope.candidate_id = v_job_row.candidate_id;

      v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
    ELSIF v_delta_refresh_complete IS TRUE THEN
      UPDATE public.banking_pay_workbench_session_scope AS delta_ready_scope
      SET status = 'READY',
          seeded = true,
          dirty = false,
          pending_job_id = NULL::uuid,
          error_json = NULL::jsonb,
          updated_at_utc = v_now
      WHERE delta_ready_scope.session_id = v_job_row.session_id
        AND delta_ready_scope.candidate_id = v_job_row.candidate_id;

      -- Session counters/progress are recomputed authoritatively after the completing
      -- delta job is marked SUCCEEDED, so stored active_jobs never captures this job as stale.


      v_next_recommended_action := 'READ_PREVIEW_PAGE';
    ELSE
      v_delta_fallback_required := true;
      v_delta_fallback_reason := COALESCE(v_delta_fallback_reason, 'DELTA_RESULT_NOT_TERMINAL');
      v_delta_fallback_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_session_row.source_snapshot_run_id,
        p_candidate_id => v_job_row.candidate_id,
        p_reason => v_delta_fallback_reason,
        p_actor_user_id => v_session_row.actor_user_id,
        p_payload_json => jsonb_build_object(
          'session_id', v_job_row.session_id::text,
          'source_session_id', v_job_row.session_id::text,
          'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'session_version', COALESCE(v_session_row.version, 0),
          'session_signature', v_session_row.session_signature,
          'force_legacy', true,
          'fallback_from_delta', true,
          'fallback_reason', v_delta_fallback_reason,
          'shadow_compare_failed', COALESCE(v_delta_shadow_compare_failed, false),
          'shadow_compare_status', NULLIF(v_delta_shadow_compare_status, ''),
          'shadow_compare_required', COALESCE(v_delta_shadow_compare_required, false),
          'shadow_compare_enforced', COALESCE(v_delta_shadow_compare_enforced, false),
          'projection_run_id', v_delta_projection_run_id_text,
          'source_build_required', true,
          'line_work_required', true,
          'targeted_timesheet_ids', COALESCE(v_delta_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_delta_linked_timesheet_ids_json, '[]'::jsonb)
        )
      );
      v_continuation_jobs := jsonb_build_array(v_delta_fallback_enqueue_result);
      v_continuation_count := 1;
      v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
    END IF;

    UPDATE public.banking_pay_workbench_jobs AS update_delta_job
    SET status = 'SUCCEEDED',
        updated_at_utc = v_now,
        completed_at_utc = v_now,
        failed_at_utc = NULL,
        last_error_json = NULL,
        payload_json = COALESCE(update_delta_job.payload_json, '{}'::jsonb)
          || jsonb_build_object('result_json', public.pay_workbench_compact_job_result_json(v_result_json))
          || jsonb_build_object(
            'completion_json', jsonb_build_object(
              'delta_completion_handled', true,
              'delta_refresh_complete', COALESCE(v_delta_refresh_complete, false),
              'fallback_required', COALESCE(v_delta_fallback_required, false),
              'fallback_reason', v_delta_fallback_reason,
              'superseded_by_newer_source_change_seq', COALESCE(v_delta_superseded_by_live_source_guard, false),
              'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
              'superseded_projection_runs', COALESCE(v_delta_superseded_projection_runs, 0),
              'shadow_compare_status', NULLIF(v_delta_shadow_compare_status, ''),
              'shadow_compare_required', COALESCE(v_delta_shadow_compare_required, false),
              'shadow_compare_enforced', COALESCE(v_delta_shadow_compare_enforced, false),
              'shadow_compare_failed', COALESCE(v_delta_shadow_compare_failed, false),
              'projection_run_id', v_delta_projection_run_id_text,
              'continuation_enqueued', COALESCE(v_continuation_enqueued, false),
              'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
              'continuation_count', COALESCE(v_continuation_count, 0),
              'continuation_reused_count', COALESCE(v_continuation_reused_count, 0),
              'next_recommended_action', v_next_recommended_action,
              'completed_at_utc', v_now::text
            )
          )
    WHERE update_delta_job.id = p_job_id
    RETURNING update_delta_job.*
    INTO v_job_row;

    PERFORM public.pay_workbench_session_recompute_progress_counters(
      p_session_id => v_job_row.session_id,
      p_apply => true,
      p_reason => CASE
        WHEN COALESCE(v_delta_fallback_required, false) IS TRUE THEN 'DELTA_COMPLETE_JOB_FALLBACK_AUTHORITATIVE_RECOMPUTE'
        ELSE 'DELTA_COMPLETE_JOB_AUTHORITATIVE_RECOMPUTE'
      END,
      p_write_progress_json => true
    );

    PERFORM public.pay_workbench_projection_lifecycle_repair(
      p_session_id => v_job_row.session_id,
      p_candidate_id => v_job_row.candidate_id,
      p_safe_age_seconds => 300,
      p_limit => 50,
      p_reason => 'DELTA_COMPLETE_JOB_PROJECTION_LIFECYCLE_REPAIR'
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', p_job_id::text,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'stage_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'delta_completion_handled', true,
      'delta_refresh_complete', COALESCE(v_delta_refresh_complete, false),
      'fallback_required', COALESCE(v_delta_fallback_required, false),
      'fallback_reason', v_delta_fallback_reason,
      'superseded_by_newer_source_change_seq', COALESCE(v_delta_superseded_by_live_source_guard, false),
      'live_source_change_seq', COALESCE(v_live_source_change_seq, 0),
      'superseded_projection_runs', COALESCE(v_delta_superseded_projection_runs, 0),
      'shadow_compare_status', NULLIF(v_delta_shadow_compare_status, ''),
      'shadow_compare_required', COALESCE(v_delta_shadow_compare_required, false),
      'shadow_compare_enforced', COALESCE(v_delta_shadow_compare_enforced, false),
      'shadow_compare_failed', COALESCE(v_delta_shadow_compare_failed, false),
      'projection_run_id', v_delta_projection_run_id_text,
      'continuation_enqueued', COALESCE(v_continuation_enqueued, false),
      'continuation_count', COALESCE(v_continuation_count, 0),
      'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
      'next_recommended_action', v_next_recommended_action,
      'old_line_work_seed_enqueued', false,
      'old_line_work_process_enqueued', false,
      'old_preview_materialise_enqueued', false,
      'source_rows_written', COALESCE(v_delta_source_rows_written, 0),
      'line_rows_written', COALESCE(v_delta_line_rows_written, 0),
      'preview_rows_written', COALESCE(v_delta_preview_rows_written, 0),
      'rows_superseded', COALESCE(v_delta_rows_superseded, 0),
      'completed_at_utc', v_now::text
    );
  END IF;


  IF v_duplicate_completion IS NOT TRUE
     AND v_stage_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
    v_clone_more_due := LOWER(BTRIM(COALESCE(v_result_json->>'more_due', v_result_json->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_clone_complete := LOWER(BTRIM(COALESCE(v_result_json->>'clone_rebase_complete', v_result_json->>'complete', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') OR v_clone_more_due IS NOT TRUE;
    v_clone_copied_candidate_count := CASE WHEN COALESCE(v_result_json->>'copied_candidate_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'copied_candidate_count')::integer ELSE 0 END;
    v_clone_copied_preview_row_count := CASE WHEN COALESCE(v_result_json->>'copied_preview_row_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'copied_preview_row_count')::integer ELSE 0 END;
    v_clone_legacy_refresh_enqueued_count := CASE WHEN COALESCE(v_result_json->>'legacy_refresh_enqueued_count', '') ~ '^-?[0-9]+$' THEN (v_result_json->>'legacy_refresh_enqueued_count')::integer ELSE 0 END;
    v_clone_source_session_id_text := COALESCE(
      NULLIF(BTRIM(COALESCE(v_result_json->>'source_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result_json->>'clone_from_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'source_session_id', '')), ''),
      NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'source_session_id_text', '')), ''),
      NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'clone_from_session_id', '')), '')
    );
    IF v_clone_source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_clone_source_session_id := v_clone_source_session_id_text::uuid;
    ELSE
      v_clone_source_session_id := NULL::uuid;
    END IF;

    IF v_clone_more_due IS TRUE THEN
      INSERT INTO public.banking_pay_workbench_jobs AS clone_continuation (
        job_type,
        status,
        priority,
        run_at_utc,
        dedupe_key,
        snapshot_run_id,
        session_id,
        candidate_id,
        payload_json,
        created_at_utc,
        updated_at_utc
      )
      VALUES (
        'WORKBENCH_SESSION_CLONE_REBASE',
        'QUEUED',
        COALESCE(v_job_row.priority, 45),
        v_now,
        'WORKBENCH_SESSION_CLONE_REBASE:session:' || v_job_row.session_id::text || ':cursor:' || md5(COALESCE(v_next_cursor, '{}'::jsonb)::text),
        v_job_row.snapshot_run_id,
        v_job_row.session_id,
        v_job_row.candidate_id,
        jsonb_strip_nulls(
          COALESCE(v_job_row.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'job_type', 'WORKBENCH_SESSION_CLONE_REBASE',
            'cursor_json', COALESCE(v_next_cursor, '{}'::jsonb),
            'source_job_id', p_job_id::text,
            'continuation', true,
            'more_due_from_previous', true,
            'previous_result_summary', jsonb_build_object(
              'copied_candidate_count', COALESCE(v_clone_copied_candidate_count, 0),
              'copied_preview_row_count', COALESCE(v_clone_copied_preview_row_count, 0),
              'legacy_refresh_enqueued_count', COALESCE(v_clone_legacy_refresh_enqueued_count, 0)
            )
          )
        ),
        v_now,
        v_now
      )
      ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
      DO UPDATE SET
        run_at_utc = LEAST(clone_continuation.run_at_utc, EXCLUDED.run_at_utc),
        priority = LEAST(clone_continuation.priority, EXCLUDED.priority),
        payload_json = COALESCE(clone_continuation.payload_json, '{}'::jsonb) || EXCLUDED.payload_json,
        updated_at_utc = v_now
      RETURNING clone_continuation.id
      INTO v_clone_continuation_job_id;

      v_continuation_result := jsonb_build_object(
        'ok', true,
        'job_id', v_clone_continuation_job_id::text,
        'job_type', 'WORKBENCH_SESSION_CLONE_REBASE',
        'continuation_enqueued', true
      );
      v_continuation_jobs := jsonb_build_array(v_continuation_result);
      v_continuation_count := 1;
      v_continuation_enqueued := true;
      v_next_recommended_action := 'CLONE_REBASE_CHUNK';
    ELSE
      v_next_recommended_action := CASE WHEN COALESCE(v_clone_legacy_refresh_enqueued_count, 0) > 0 THEN 'WAIT_FOR_WORKER' ELSE 'READ_PREVIEW_PAGE' END;
    END IF;

    IF COALESCE(v_clone_more_due, false) IS NOT TRUE
       AND v_clone_source_session_id IS NOT NULL
       AND v_job_row.session_id IS NOT NULL
       AND v_clone_source_session_id IS DISTINCT FROM v_job_row.session_id THEN
      UPDATE public.banking_pay_workbench_sessions AS clone_source_session
      SET status = 'DISCARDED',
          discarded_at_utc = COALESCE(clone_source_session.discarded_at_utc, v_now),
          replacement_session_id = v_job_row.session_id,
          replacement_idempotency_key = COALESCE(
            clone_source_session.replacement_idempotency_key,
            'clone-rebase-complete:' || clone_source_session.id::text || ':replacement:' || v_job_row.session_id::text
          ),
          progress_state = 'DISCARDED',
          progress_json = jsonb_strip_nulls(
            COALESCE(clone_source_session.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'discard_reason', 'SUPERSEDED_BY_COMPLETED_CLONE_REBASE',
              'discarded_by_function', 'pay_workbench_complete_job',
              'replacement_session_id', v_job_row.session_id::text,
              'replacement_linked_at_utc', v_now::text,
              'clone_rebase_completed_by_job_id', p_job_id::text,
              'superseded_by_pay_date', CASE WHEN v_session_row.id IS NULL THEN NULL ELSE v_session_row.pay_date::text END,
              'superseded_by_week_ending_cutoff', CASE WHEN v_session_row.id IS NULL THEN NULL ELSE v_session_row.week_ending_cutoff::text END
            )
          ),
          progress_counter_version = COALESCE(clone_source_session.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE clone_source_session.id = v_clone_source_session_id
        AND clone_source_session.status = 'OPEN'
        AND clone_source_session.discarded_at_utc IS NULL
        AND (
          v_session_row.id IS NULL
          OR clone_source_session.actor_user_id = v_session_row.actor_user_id
        )
        AND (
          v_session_row.id IS NULL
          OR clone_source_session.pay_date IS DISTINCT FROM v_session_row.pay_date
          OR clone_source_session.week_ending_cutoff IS DISTINCT FROM v_session_row.week_ending_cutoff
        );

      GET DIAGNOSTICS v_clone_source_session_retired_count = ROW_COUNT;
    END IF;

    UPDATE public.banking_pay_workbench_sessions AS clone_session_update
    SET progress_state = CASE
          WHEN v_clone_more_due IS TRUE THEN 'CLONE_REBASING'
          WHEN COALESCE(v_clone_legacy_refresh_enqueued_count, 0) > 0 THEN 'REFRESHING_CANDIDATES'
          WHEN COALESCE(clone_session_update.scope_pending_count, 0) = 0
           AND COALESCE(clone_session_update.line_units_pending, 0) = 0
           AND COALESCE(clone_session_update.scope_failed_count, 0) = 0
           AND COALESCE(clone_session_update.line_units_failed, 0) = 0
          THEN 'READY'
          ELSE COALESCE(NULLIF(BTRIM(clone_session_update.progress_state), ''), 'REFRESHING_CANDIDATES')
        END,
        progress_json = jsonb_strip_nulls(
          public.pay_workbench_session_compact_progress_json(COALESCE(clone_session_update.progress_json, '{}'::jsonb), true)
          || jsonb_build_object(
            'last_clone_rebase_job_id', p_job_id::text,
            'last_clone_rebase_completed_at_utc', v_now::text,
            'last_clone_rebase_more_due', COALESCE(v_clone_more_due, false),
            'last_clone_rebase_copied_candidate_count', COALESCE(v_clone_copied_candidate_count, 0),
            'last_clone_rebase_copied_preview_row_count', COALESCE(v_clone_copied_preview_row_count, 0),
            'last_clone_rebase_legacy_refresh_enqueued_count', COALESCE(v_clone_legacy_refresh_enqueued_count, 0),
            'last_clone_rebase_source_session_id', CASE WHEN v_clone_source_session_id IS NULL THEN NULL ELSE v_clone_source_session_id::text END,
            'last_clone_rebase_source_session_retired_count', COALESCE(v_clone_source_session_retired_count, 0),
            'next_recommended_action', v_next_recommended_action
          )
        ),
        progress_counter_version = COALESCE(clone_session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE clone_session_update.id = v_job_row.session_id;

    UPDATE public.banking_pay_workbench_jobs AS update_clone_job
    SET status = 'SUCCEEDED',
        updated_at_utc = v_now,
        completed_at_utc = v_now,
        failed_at_utc = NULL,
        last_error_json = NULL,
        payload_json = COALESCE(update_clone_job.payload_json, '{}'::jsonb)
          || jsonb_build_object('result_json', public.pay_workbench_compact_job_result_json(v_result_json))
          || jsonb_build_object(
            'completion_json', jsonb_build_object(
              'clone_rebase_completion_handled', true,
              'clone_rebase_complete', COALESCE(v_clone_complete, false),
              'more_due', COALESCE(v_clone_more_due, false),
              'continuation_enqueued', COALESCE(v_continuation_enqueued, false),
              'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
              'source_session_id', CASE WHEN v_clone_source_session_id IS NULL THEN NULL ELSE v_clone_source_session_id::text END,
              'source_session_retired_count', COALESCE(v_clone_source_session_retired_count, 0),
              'source_session_retired', COALESCE(v_clone_source_session_retired_count, 0) > 0,
              'next_recommended_action', v_next_recommended_action,
              'completed_at_utc', v_now::text
            )
          )
    WHERE update_clone_job.id = p_job_id
    RETURNING update_clone_job.*
    INTO v_job_row;

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', p_job_id::text,
      'job_type', 'WORKBENCH_SESSION_CLONE_REBASE',
      'stage_job_type', 'WORKBENCH_SESSION_CLONE_REBASE',
      'clone_rebase_completion_handled', true,
      'clone_rebase_complete', COALESCE(v_clone_complete, false),
      'more_due', COALESCE(v_clone_more_due, false),
      'continuation_enqueued', COALESCE(v_continuation_enqueued, false),
      'continuation_count', COALESCE(v_continuation_count, 0),
      'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
      'copied_candidate_count', COALESCE(v_clone_copied_candidate_count, 0),
      'copied_preview_row_count', COALESCE(v_clone_copied_preview_row_count, 0),
      'legacy_refresh_enqueued_count', COALESCE(v_clone_legacy_refresh_enqueued_count, 0),
      'source_session_id', CASE WHEN v_clone_source_session_id IS NULL THEN NULL ELSE v_clone_source_session_id::text END,
      'source_session_retired_count', COALESCE(v_clone_source_session_retired_count, 0),
      'source_session_retired', COALESCE(v_clone_source_session_retired_count, 0) > 0,
      'next_recommended_action', v_next_recommended_action,
      'completed_at_utc', v_now::text
    );
  END IF;

  IF v_duplicate_completion IS NOT TRUE THEN
    IF v_stage_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
       AND v_job_row.session_id IS NOT NULL
       AND v_job_row.candidate_id IS NOT NULL
       AND v_has_more IS NOT TRUE THEN
      SELECT EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_candidate_line_work AS line_work_pending
        WHERE line_work_pending.session_id = v_job_row.session_id
          AND line_work_pending.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(line_work_pending.status, ''))) = 'PENDING'
      )
      INTO v_pending_line_work_exists;
    END IF;

    IF v_stage_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
       AND v_job_row.session_id IS NOT NULL
       AND v_job_row.candidate_id IS NOT NULL THEN
      SELECT EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_candidate_line_work AS line_work_pending
        WHERE line_work_pending.session_id = v_job_row.session_id
          AND line_work_pending.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(line_work_pending.status, ''))) = 'PENDING'
      )
      INTO v_pending_line_work_exists;

      SELECT EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_candidate_line_work AS line_work_ready
        WHERE line_work_ready.session_id = v_job_row.session_id
          AND line_work_ready.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(line_work_ready.status, ''))) = 'READY'
      )
      INTO v_ready_line_work_exists;
    END IF;

    IF v_stage_job_type IN (
         'WORKBENCH_CANDIDATE_SOURCE_BUILD',
         'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
         'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
         'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
       )
       AND v_job_row.session_id IS NOT NULL
       AND v_job_row.candidate_id IS NOT NULL THEN
      SELECT UPPER(BTRIM(COALESCE(scope_before_continuation.status, 'PENDING')))
      INTO v_scope_status_before_continuation
      FROM public.banking_pay_workbench_session_scope AS scope_before_continuation
      WHERE scope_before_continuation.session_id = v_job_row.session_id
        AND scope_before_continuation.candidate_id = v_job_row.candidate_id
      FOR UPDATE;
    END IF;

    IF v_stage_job_type = 'WORKBENCH_SESSION_SCOPE_SEED'
       AND v_has_more IS TRUE
       AND v_next_cursor IS NOT NULL THEN
      v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
        p_session_id => v_job_row.session_id,
        p_candidate_id => NULL::uuid,
        p_job_type => 'WORKBENCH_SESSION_SCOPE_SEED',
        p_cursor_json => v_next_cursor,
        p_source_job_id => p_job_id,
        p_result_json => v_result_json,
        p_actor_user_id => v_session_row.actor_user_id,
        p_reason => 'SCOPE_SEED_CURSOR_CONTINUATION',
        p_priority => 40,
        p_limit => v_payload_limit
      );
      v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
      v_next_recommended_action := 'SEED_SCOPE_CHUNK';
    ELSIF v_stage_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND v_job_row.candidate_id IS NOT NULL THEN
      IF v_has_more IS TRUE AND v_next_cursor IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_COMPLETE_JOB_SOURCE_BUILD_CURSOR_MISSING'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_COMPLETE_JOB_SOURCE_BUILD_CURSOR_MISSING',
                  'job_id', p_job_id::text,
                  'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
                  'candidate_id', v_job_row.candidate_id::text,
                  'job_type', v_stage_job_type,
                  'source_build_run_id', v_source_build_run_id_text,
                  'source_change_seq', v_source_change_seq,
                  'message', 'WORKBENCH_CANDIDATE_SOURCE_BUILD returned has_more=true without a next cursor; refusing to start line seed early.'
                )::text;
      ELSIF v_has_more IS TRUE THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          p_cursor_json => v_next_cursor,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'SOURCE_BUILD_CURSOR_CONTINUATION',
          p_priority => 44,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
      ELSIF v_is_material_source
            AND v_current_source_row_count_authoritative
            AND COALESCE(v_current_source_row_count, 0) > 0 THEN
        -- Bounded SOURCE_PUBLISH has already produced the complete canonical
        -- CURRENT set. The common terminal path below owns public scope and
        -- progress reconciliation; it must not enter the legacy line-work
        -- pipeline or carry private build identity into a legacy job.
        v_next_recommended_action := 'READ_PREVIEW_PAGE';
      ELSIF (
        (
          v_current_source_row_count_authoritative
          AND COALESCE(v_current_source_row_count, 0) > 0
        )
        OR (
          v_current_source_row_count_authoritative IS NOT TRUE
          AND (
            COALESCE(v_current_source_row_count, 0) > 0
            OR COALESCE(v_source_rows_written, 0) > 0
          )
        )
      ) THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          p_cursor_json => v_source_seed_cursor_json,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'SOURCE_BUILD_COMPLETE_SEED_LINE_WORK',
          p_priority => 45,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'SEED_LINE_WORK_CHUNK';
      ELSE
        DROP TABLE IF EXISTS pg_temp._bpay_complete_job_source_empty_timesheets;
        CREATE TEMP TABLE _bpay_complete_job_source_empty_timesheets ON COMMIT DROP AS
        WITH raw_timesheet_ids(timesheet_id_text) AS (
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result_json->'targeted_timesheet_ids') = 'array' THEN v_result_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result_json->'linked_timesheet_ids') = 'array' THEN v_result_json->'linked_timesheet_ids' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result_json #> '{source_build,targeted_timesheet_ids}') = 'array' THEN v_result_json #> '{source_build,targeted_timesheet_ids}' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result_json #> '{source_build,linked_timesheet_ids}') = 'array' THEN v_result_json #> '{source_build,linked_timesheet_ids}' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_job_row.payload_json->'targeted_timesheet_ids') = 'array' THEN v_job_row.payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_job_row.payload_json->'linked_timesheet_ids') = 'array' THEN v_job_row.payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_job_row.payload_json #> '{source_build,targeted_timesheet_ids}') = 'array' THEN v_job_row.payload_json #> '{source_build,targeted_timesheet_ids}' ELSE '[]'::jsonb END)
          UNION ALL
          SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_job_row.payload_json #> '{source_build,linked_timesheet_ids}') = 'array' THEN v_job_row.payload_json #> '{source_build,linked_timesheet_ids}' ELSE '[]'::jsonb END)
        )
        SELECT DISTINCT LOWER(BTRIM(timesheet_id_text))::uuid AS timesheet_id
        FROM raw_timesheet_ids
        WHERE NULLIF(BTRIM(timesheet_id_text), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

        SELECT COUNT(*)::integer
        INTO v_source_empty_targeted_timesheet_count
        FROM pg_temp._bpay_complete_job_source_empty_timesheets;

        v_delta_refresh_scope_kind := UPPER(BTRIM(COALESCE(
          NULLIF(v_result_json->>'refresh_scope_kind', ''),
          NULLIF(v_job_row.payload_json->>'refresh_scope_kind', ''),
          NULLIF(v_job_row.payload_json#>>'{source_build,refresh_scope_kind}', ''),
          'CANDIDATE_FULL_LIVE'
        )));

        IF v_delta_refresh_scope_kind = 'TARGETED_TIMESHEETS'
           AND COALESCE(v_source_empty_targeted_timesheet_count, 0) = 0 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_EMPTY_TARGET_SCOPE_REQUIRED'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_SOURCE_EMPTY_TARGET_SCOPE_REQUIRED',
                    'session_id', v_job_row.session_id::text,
                    'candidate_id', v_job_row.candidate_id::text,
                    'job_id', p_job_id::text,
                    'refresh_scope_kind', v_delta_refresh_scope_kind
                  )::text;
        END IF;

        UPDATE public.banking_pay_workbench_session_scope AS source_empty_scope
        SET status = CASE
              WHEN v_delta_refresh_scope_kind = 'TARGETED_TIMESHEETS' THEN 'READY'
              ELSE 'SOURCE_EMPTY'
            END,
            seeded = true,
            dirty = false,
            pending_job_id = NULL::uuid,
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE source_empty_scope.session_id = v_job_row.session_id
          AND source_empty_scope.candidate_id = v_job_row.candidate_id;

        UPDATE public.banking_pay_workbench_candidate_source_lines AS source_empty_source_line
        SET status = 'SUPERSEDED',
            source_row_json = jsonb_strip_nulls(
              COALESCE(source_empty_source_line.source_row_json, '{}'::jsonb)
              || jsonb_build_object(
                'source_empty_cleanup', true,
                'source_empty_cleanup_job_id', p_job_id::text,
                'source_empty_cleanup_at_utc', v_now::text,
                'source_empty_cleanup_reason', 'TARGETED_SOURCE_BUILD_RETURNED_ZERO_CURRENT_SOURCE_ROWS'
              )
            ),
            updated_at_utc = v_now
        WHERE source_empty_source_line.session_id = v_job_row.session_id
          AND source_empty_source_line.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(source_empty_source_line.status, ''))) IN ('CURRENT', 'READY', 'PENDING', 'DIRTY')
          AND (
            v_delta_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            OR source_empty_source_line.timesheet_id IN (
              SELECT empty_timesheets.timesheet_id
              FROM pg_temp._bpay_complete_job_source_empty_timesheets AS empty_timesheets
            )
          );
        GET DIAGNOSTICS v_source_empty_cleanup_source_row_count = ROW_COUNT;

        UPDATE public.banking_pay_workbench_candidate_line_work AS source_empty_line_work
        SET status = 'SKIPPED',
            work_payload_json = jsonb_strip_nulls(
              COALESCE(source_empty_line_work.work_payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'source_empty_cleanup', true,
                'source_empty_cleanup_job_id', p_job_id::text,
                'source_empty_cleanup_at_utc', v_now::text,
                'source_empty_cleanup_reason', 'TARGETED_SOURCE_BUILD_RETURNED_ZERO_CURRENT_SOURCE_ROWS',
                'source_empty_cleanup_targeted_timesheet_count', COALESCE(v_source_empty_targeted_timesheet_count, 0)
              )
            ),
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE source_empty_line_work.session_id = v_job_row.session_id
          AND source_empty_line_work.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(source_empty_line_work.status, ''))) IN (
            'PENDING',
            'PROCESSING',
            'RUNNING',
            'QUEUED',
            'DIRTY',
            'READY',
            'MATERIALISED',
            'MATERIALIZED'
          )
          AND (
            v_delta_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            OR source_empty_line_work.timesheet_id IN (
              SELECT empty_timesheets.timesheet_id
              FROM pg_temp._bpay_complete_job_source_empty_timesheets AS empty_timesheets
            )
          );
        GET DIAGNOSTICS v_source_empty_cleanup_line_work_count = ROW_COUNT;

        UPDATE public.banking_pay_workbench_preview_rows AS source_empty_preview_row
        SET status = 'SUPERSEDED',
            selected = false,
            selection_state = 'SUPERSEDED',
            row_json = jsonb_strip_nulls(
              COALESCE(source_empty_preview_row.row_json, '{}'::jsonb)
              || jsonb_build_object(
                'source_empty_cleanup', true,
                'source_empty_cleanup_job_id', p_job_id::text,
                'source_empty_cleanup_at_utc', v_now::text,
                'source_empty_cleanup_reason', 'TARGETED_SOURCE_BUILD_RETURNED_ZERO_CURRENT_SOURCE_ROWS',
                'not_payable_reason', 'SOURCE_EMPTY_AFTER_UNAUTHORISE_OR_SOURCE_CHANGE'
              )
            ),
            updated_at_utc = v_now
        WHERE source_empty_preview_row.session_id = v_job_row.session_id
          AND source_empty_preview_row.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(source_empty_preview_row.status, ''))) IN ('READY', 'DIRTY', 'PENDING')
          AND (
            v_delta_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            OR source_empty_preview_row.timesheet_id IN (
              SELECT empty_timesheets.timesheet_id
              FROM pg_temp._bpay_complete_job_source_empty_timesheets AS empty_timesheets
            )
          );
        GET DIAGNOSTICS v_source_empty_cleanup_preview_row_count = ROW_COUNT;

        BEGIN
          PERFORM 1
          FROM public.banking_pay_workbench_sessions AS source_empty_session_lock
          WHERE source_empty_session_lock.id = v_job_row.session_id
            AND UPPER(BTRIM(COALESCE(source_empty_session_lock.status, ''))) = 'OPEN'
            AND source_empty_session_lock.discarded_at_utc IS NULL
            AND COALESCE(source_empty_session_lock.version, 1) = COALESCE(v_session_row.version, 1)
            AND source_empty_session_lock.source_snapshot_run_id IS NOT DISTINCT FROM v_session_row.source_snapshot_run_id
            AND source_empty_session_lock.session_signature IS NOT DISTINCT FROM v_session_row.session_signature
          FOR UPDATE NOWAIT;

          IF FOUND THEN
            UPDATE public.banking_pay_workbench_sessions AS source_empty_session
            SET scope_ready_count = COALESCE(source_scope_counts.ready_count, 0),
                scope_pending_count = COALESCE(source_scope_counts.pending_count, 0),
                scope_failed_count = COALESCE(source_scope_counts.failed_count, 0),
                progress_state = CASE
                  WHEN COALESCE(source_scope_counts.pending_count, 0) > 0 THEN 'REFRESHING_CANDIDATES'
                  WHEN COALESCE(source_scope_counts.failed_count, 0) > 0 THEN 'ERROR'
                  ELSE source_empty_session.progress_state
                END,
                progress_json = public.pay_workbench_session_compact_progress_json(COALESCE(source_empty_session.progress_json, '{}'::jsonb), true)
                  || jsonb_build_object(
                    'last_source_build_empty_candidate_id', v_job_row.candidate_id::text,
                    'last_source_build_empty_at_utc', v_now::text,
                    'last_source_build_empty_job_id', p_job_id::text,
                    'next_recommended_action', 'READ_PREVIEW_PAGE',
                    'terminal_readiness_deferred', true,
                    'terminal_readiness_deferred_to', 'pay_workbench_complete_job',
                    'source_empty_session_progress_locking', 'NOWAIT',
                    'source_empty_session_progress_update_applied', true,
                    'source_empty_cleanup_source_row_count', COALESCE(v_source_empty_cleanup_source_row_count, 0),
                    'source_empty_cleanup_line_work_count', COALESCE(v_source_empty_cleanup_line_work_count, 0),
                    'source_empty_cleanup_preview_row_count', COALESCE(v_source_empty_cleanup_preview_row_count, 0),
                    'source_empty_cleanup_targeted_timesheet_count', COALESCE(v_source_empty_targeted_timesheet_count, 0)
                  ),
                progress_counter_version = COALESCE(source_empty_session.progress_counter_version, 0) + 1,
                progress_updated_at_utc = v_now,
                updated_at_utc = v_now
            FROM (
              SELECT
                COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_recount.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY'))::integer AS ready_count,
                COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_recount.status, ''))) IN ('ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR'))::integer AS failed_count,
                COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_recount.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY', 'ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR'))::integer AS pending_count
              FROM public.banking_pay_workbench_session_scope AS scope_recount
              WHERE scope_recount.session_id = v_job_row.session_id
            ) AS source_scope_counts
            WHERE source_empty_session.id = v_job_row.session_id
            RETURNING source_empty_session.*
            INTO v_session_row;
          ELSE
            v_source_empty_session_progress_deferred := true;
            v_source_empty_session_progress_deferred_reason := 'SESSION_NOT_OPEN_OR_CONTEXT_STALE';
          END IF;
        EXCEPTION
          WHEN lock_not_available THEN
            v_source_empty_session_progress_deferred := true;
            v_source_empty_session_progress_deferred_reason := 'SESSION_LOCK_NOT_AVAILABLE';
        END;

        v_next_recommended_action := 'READ_PREVIEW_PAGE';
      END IF;
    ELSIF v_stage_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
          AND v_job_row.candidate_id IS NOT NULL THEN
      IF v_has_more IS TRUE AND v_next_cursor IS NOT NULL THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          p_cursor_json => v_next_cursor,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'LINE_WORK_SEED_CURSOR_CONTINUATION',
          p_priority => 45,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'SEED_LINE_WORK_CHUNK';
      ELSIF COALESCE(v_seeded_count, 0) > 0 OR v_pending_line_work_exists IS TRUE THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          p_cursor_json => NULL::jsonb,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'LINE_WORK_SEED_COMPLETE_PROCESS_PENDING',
          p_priority => 42,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'PROCESS_LINE_WORK_CHUNK';
      END IF;
    ELSIF v_stage_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
          AND v_job_row.candidate_id IS NOT NULL THEN
      IF v_has_more IS TRUE AND v_next_cursor IS NOT NULL THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          p_cursor_json => v_next_cursor,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'LINE_WORK_PROCESS_CURSOR_CONTINUATION',
          p_priority => 42,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'PROCESS_LINE_WORK_CHUNK';
      END IF;

      IF v_has_more IS NOT TRUE
         AND v_pending_line_work_exists IS NOT TRUE
         AND (COALESCE(v_ready_count_delta, 0) > 0 OR v_ready_line_work_exists IS TRUE) THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          p_cursor_json => NULL::jsonb,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'LINE_WORK_READY_MATERIALISE_PREVIEW_ROWS',
          p_priority => 41,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := COALESCE(v_next_recommended_action, 'MATERIALISE_PREVIEW_ROWS_CHUNK');
      END IF;
    ELSIF v_stage_job_type = 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
          AND v_job_row.candidate_id IS NOT NULL THEN
      IF v_has_more IS TRUE AND v_next_cursor IS NOT NULL THEN
        v_continuation_result := public.pay_workbench_enqueue_stage_continuation(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_job_type => 'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          p_cursor_json => v_next_cursor,
          p_source_job_id => p_job_id,
          p_result_json => v_result_json,
          p_actor_user_id => v_session_row.actor_user_id,
          p_reason => 'PREVIEW_ROWS_MATERIALISE_CURSOR_CONTINUATION',
          p_priority => 41,
          p_limit => v_payload_limit
        );
        v_continuation_jobs := v_continuation_jobs || jsonb_build_array(v_continuation_result);
        v_next_recommended_action := 'MATERIALISE_PREVIEW_ROWS_CHUNK';
      ELSIF COALESCE(v_materialised_count, 0) > 0 THEN
        v_next_recommended_action := 'READ_PREVIEW_PAGE';
      END IF;
    END IF;

    v_continuation_count := jsonb_array_length(COALESCE(v_continuation_jobs, '[]'::jsonb));
    v_continuation_enqueued := v_continuation_count > 0;

    SELECT COUNT(*)::integer
    INTO v_continuation_reused_count
    FROM jsonb_array_elements(COALESCE(v_continuation_jobs, '[]'::jsonb)) AS continuation_items(continuation_item)
    WHERE LOWER(BTRIM(COALESCE(continuation_items.continuation_item->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

    IF COALESCE(v_continuation_count, 0) > 0
       AND v_scope_status_before_continuation IS NOT NULL
       AND v_job_row.session_id IS NOT NULL
       AND v_job_row.candidate_id IS NOT NULL THEN
      SELECT UPPER(BTRIM(COALESCE(scope_after_continuation.status, 'PENDING')))
      INTO v_scope_status_after_continuation
      FROM public.banking_pay_workbench_session_scope AS scope_after_continuation
      WHERE scope_after_continuation.session_id = v_job_row.session_id
        AND scope_after_continuation.candidate_id = v_job_row.candidate_id
      FOR UPDATE;

      v_scope_bucket_before_continuation := CASE
        WHEN v_scope_status_before_continuation IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY') THEN 'READY'
        WHEN v_scope_status_before_continuation IN ('ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR') THEN 'FAILED'
        ELSE 'PENDING'
      END;

      v_scope_bucket_after_continuation := CASE
        WHEN v_scope_status_after_continuation IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY') THEN 'READY'
        WHEN v_scope_status_after_continuation IN ('ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR') THEN 'FAILED'
        ELSE 'PENDING'
      END;

      v_scope_continuation_pending_delta := CASE WHEN v_scope_bucket_after_continuation = 'PENDING' THEN 1 ELSE 0 END
        - CASE WHEN v_scope_bucket_before_continuation = 'PENDING' THEN 1 ELSE 0 END;
      v_scope_continuation_ready_delta := CASE WHEN v_scope_bucket_after_continuation = 'READY' THEN 1 ELSE 0 END
        - CASE WHEN v_scope_bucket_before_continuation = 'READY' THEN 1 ELSE 0 END;
      v_scope_continuation_failed_delta := CASE WHEN v_scope_bucket_after_continuation = 'FAILED' THEN 1 ELSE 0 END
        - CASE WHEN v_scope_bucket_before_continuation = 'FAILED' THEN 1 ELSE 0 END;

      IF COALESCE(v_scope_continuation_pending_delta, 0) <> 0
         OR COALESCE(v_scope_continuation_ready_delta, 0) <> 0
         OR COALESCE(v_scope_continuation_failed_delta, 0) <> 0 THEN
        BEGIN
          PERFORM 1
          FROM public.banking_pay_workbench_sessions AS continuation_session_lock
          WHERE continuation_session_lock.id = v_job_row.session_id
            AND UPPER(BTRIM(COALESCE(continuation_session_lock.status, ''))) = 'OPEN'
            AND continuation_session_lock.discarded_at_utc IS NULL
            AND COALESCE(continuation_session_lock.version, 1) = COALESCE(v_session_row.version, 1)
            AND continuation_session_lock.source_snapshot_run_id IS NOT DISTINCT FROM v_session_row.source_snapshot_run_id
            AND continuation_session_lock.session_signature IS NOT DISTINCT FROM v_session_row.session_signature
          FOR UPDATE NOWAIT;

          IF FOUND THEN
            UPDATE public.banking_pay_workbench_sessions AS continuation_session_update
            SET scope_pending_count = GREATEST(
                  COALESCE(continuation_session_update.scope_pending_count, 0)
                    + COALESCE(v_scope_continuation_pending_delta, 0),
                  0
                ),
                scope_ready_count = GREATEST(
                  COALESCE(continuation_session_update.scope_ready_count, 0)
                    + COALESCE(v_scope_continuation_ready_delta, 0),
                  0
                ),
                scope_failed_count = GREATEST(
                  COALESCE(continuation_session_update.scope_failed_count, 0)
                    + COALESCE(v_scope_continuation_failed_delta, 0),
                  0
                ),
                progress_json = public.pay_workbench_session_compact_progress_json(COALESCE(continuation_session_update.progress_json, '{}'::jsonb), true)
                  || jsonb_build_object(
                    'last_continuation_scope_old_status', v_scope_status_before_continuation,
                    'last_continuation_scope_new_status', v_scope_status_after_continuation,
                    'last_continuation_scope_pending_delta', COALESCE(v_scope_continuation_pending_delta, 0),
                    'last_continuation_scope_ready_delta', COALESCE(v_scope_continuation_ready_delta, 0),
                    'last_continuation_scope_failed_delta', COALESCE(v_scope_continuation_failed_delta, 0),
                    'last_continuation_scope_counter_adjusted_at_utc', v_now::text,
                    'last_continuation_scope_counter_locking', 'NOWAIT',
                    'last_continuation_scope_counter_lock_skipped', false
                  ),
                progress_counter_version = COALESCE(continuation_session_update.progress_counter_version, 0) + 1,
                progress_updated_at_utc = v_now,
                updated_at_utc = v_now
            WHERE continuation_session_update.id = v_job_row.session_id;

            v_scope_continuation_counter_adjusted := true;
          ELSE
            v_continuation_scope_counter_deferred := true;
            v_continuation_scope_counter_deferred_reason := 'SESSION_NOT_OPEN_OR_CONTEXT_STALE';
          END IF;
        EXCEPTION
          WHEN lock_not_available THEN
            v_continuation_scope_counter_deferred := true;
            v_continuation_scope_counter_deferred_reason := 'SESSION_LOCK_NOT_AVAILABLE';
        END;
      END IF;
    END IF;

    UPDATE public.banking_pay_workbench_jobs AS update_job
    SET status = 'SUCCEEDED',
        updated_at_utc = v_now,
        completed_at_utc = v_now,
        failed_at_utc = NULL,
        last_error_json = NULL,
        payload_json = COALESCE(update_job.payload_json, '{}'::jsonb)
          || jsonb_build_object('result_json', public.pay_workbench_compact_job_result_json(v_result_json))
          || jsonb_build_object(
            'completion_json', jsonb_build_object(
              'continuation_enqueued', v_continuation_enqueued,
              'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
              'continuation_count', COALESCE(v_continuation_count, 0),
              'continuation_reused_count', COALESCE(v_continuation_reused_count, 0),
              'next_recommended_action', v_next_recommended_action,
              'completed_at_utc', v_now::text
            )
          )
    WHERE update_job.id = p_job_id
    RETURNING update_job.*
    INTO v_job_row;

    v_completed_at_utc := v_now;

    IF v_stage_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
       AND v_job_row.session_id IS NOT NULL
       AND v_job_row.candidate_id IS NOT NULL
       AND v_source_build_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       AND v_source_change_seq IS NOT NULL
       AND COALESCE(v_has_more, false) IS NOT TRUE
       AND (COALESCE(v_source_rows_written, 0) > 0 OR COALESCE(v_current_source_row_count, 0) > 0) THEN
      BEGIN
        v_source_reconciliation_json := public.pay_workbench_reconcile_successful_source_build(
          p_session_id => v_job_row.session_id,
          p_candidate_id => v_job_row.candidate_id,
          p_source_build_run_id => v_source_build_run_id_text::uuid,
          p_source_change_seq => v_source_change_seq,
          p_session_version => COALESCE(v_result_session_version, v_session_row.version),
          p_success_job_id => p_job_id,
          p_refresh_scope_kind => COALESCE(
            NULLIF(BTRIM(COALESCE(v_result_json->>'refresh_scope_kind', '')), ''),
            NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'refresh_scope_kind', '')), ''),
            NULLIF(BTRIM(COALESCE(v_job_row.payload_json#>>'{source_build,refresh_scope_kind}', '')), '')
          ),
          p_targeted_timesheet_ids => CASE
            WHEN jsonb_typeof(v_result_json->'targeted_timesheet_ids') = 'array' THEN v_result_json->'targeted_timesheet_ids'
            WHEN jsonb_typeof(v_job_row.payload_json->'targeted_timesheet_ids') = 'array' THEN v_job_row.payload_json->'targeted_timesheet_ids'
            WHEN jsonb_typeof(v_job_row.payload_json#>'{source_build,targeted_timesheet_ids}') = 'array' THEN v_job_row.payload_json#>'{source_build,targeted_timesheet_ids}'
            ELSE '[]'::jsonb
          END,
          p_linked_timesheet_ids => CASE
            WHEN jsonb_typeof(v_result_json->'linked_timesheet_ids') = 'array' THEN v_result_json->'linked_timesheet_ids'
            WHEN jsonb_typeof(v_job_row.payload_json->'linked_timesheet_ids') = 'array' THEN v_job_row.payload_json->'linked_timesheet_ids'
            WHEN jsonb_typeof(v_job_row.payload_json#>'{source_build,linked_timesheet_ids}') = 'array' THEN v_job_row.payload_json#>'{source_build,linked_timesheet_ids}'
            ELSE '[]'::jsonb
          END,
          p_recompute_session_progress => true
        );

        v_source_reconciliation_deferred := LOWER(BTRIM(COALESCE(v_source_reconciliation_json->>'deferred', 'false')))
          IN ('true', 't', '1', 'yes', 'y', 'on');
        v_source_reconciliation_deferred_reason := CASE
          WHEN v_source_reconciliation_deferred THEN NULLIF(BTRIM(COALESCE(v_source_reconciliation_json->>'reason', '')), '')
          ELSE NULL::text
        END;
        v_source_reconciliation_applied := v_source_reconciliation_deferred IS NOT TRUE
          AND LOWER(BTRIM(COALESCE(v_source_reconciliation_json->>'ok', 'true')))
            NOT IN ('false', 'f', '0', 'no', 'n', 'off');
      EXCEPTION
        WHEN lock_not_available THEN
          v_source_reconciliation_applied := false;
          v_source_reconciliation_deferred := true;
          v_source_reconciliation_deferred_reason := 'SESSION_LOCK_NOT_AVAILABLE';
          v_source_reconciliation_json := jsonb_build_object(
            'ok', true,
            'deferred', true,
            'reason', 'SESSION_LOCK_NOT_AVAILABLE',
            'session_id', v_job_row.session_id::text,
            'candidate_id', v_job_row.candidate_id::text,
            'source_build_run_id', v_source_build_run_id_text,
            'source_change_seq', v_source_change_seq,
            'progress_recomputed', false,
            'counter_reconciled', false,
            'retry_safe', true
          );
      END;
    END IF;
  END IF;

  IF v_stage_job_type IN (
       'WORKBENCH_CANDIDATE_SOURCE_BUILD',
       'WORKBENCH_CANDIDATE_DELTA_REFRESH',
       'WORKBENCH_SESSION_CLONE_REBASE',
       'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
       'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
       'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
     )
     AND v_job_row.session_id IS NOT NULL
     AND v_job_row.candidate_id IS NOT NULL
     AND COALESCE(v_continuation_count, 0) = 0 THEN
    UPDATE public.banking_pay_workbench_session_scope AS scope_update
    SET pending_job_id = NULL::uuid,
        updated_at_utc = v_now
    WHERE scope_update.session_id = v_job_row.session_id
      AND scope_update.candidate_id = v_job_row.candidate_id
      AND scope_update.pending_job_id = p_job_id;

    GET DIAGNOSTICS v_scope_pending_job_cleared_count = ROW_COUNT;
  END IF;

  IF v_stage_job_type IN (
       'WORKBENCH_SESSION_SCOPE_SEED',
       'WORKBENCH_CANDIDATE_SOURCE_BUILD',
       'WORKBENCH_CANDIDATE_DELTA_REFRESH',
       'WORKBENCH_SESSION_CLONE_REBASE',
       'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
       'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
       'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
     )
     AND v_job_row.session_id IS NOT NULL
     AND v_has_open_session THEN
    BEGIN
      SELECT session_row.*
      INTO v_session_row
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id = v_job_row.session_id
        AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL
        AND COALESCE(session_row.version, 1) = COALESCE(v_session_row.version, 1)
        AND session_row.source_snapshot_run_id IS NOT DISTINCT FROM v_session_row.source_snapshot_run_id
        AND session_row.session_signature IS NOT DISTINCT FROM v_session_row.session_signature
      FOR UPDATE NOWAIT;

      IF NOT FOUND THEN
        v_finalisation_deferred := true;
        v_finalisation_deferred_reason := 'SESSION_NOT_OPEN_OR_CONTEXT_STALE';
      END IF;
    EXCEPTION
      WHEN lock_not_available THEN
        v_finalisation_deferred := true;
        v_finalisation_deferred_reason := 'SESSION_LOCK_NOT_AVAILABLE';
    END;

    IF v_finalisation_deferred IS NOT TRUE THEN
      IF v_job_row.candidate_id IS NOT NULL THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS terminal_scope
        WHERE terminal_scope.session_id = v_job_row.session_id
          AND terminal_scope.candidate_id = v_job_row.candidate_id
          AND UPPER(BTRIM(COALESCE(terminal_scope.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
          AND COALESCE(terminal_scope.dirty, false) IS NOT TRUE
          AND (
            terminal_scope.error_json IS NULL
            OR terminal_scope.error_json = '{}'::jsonb
            OR terminal_scope.error_json = 'null'::jsonb
          )
      )
      INTO v_current_candidate_terminal_success;
    END IF;

    v_finalisation_should_evaluate := (
      COALESCE(v_continuation_count, 0) = 0
      AND COALESCE(v_session_row.scope_seed_complete, false)
      AND COALESCE(v_session_row.scope_pending_count, 0) = 0
      AND COALESCE(v_session_row.scope_failed_count, 0) = 0
      AND COALESCE(v_session_row.line_units_pending, 0) = 0
      AND COALESCE(v_session_row.line_units_failed, 0) = 0
    )
    OR UPPER(BTRIM(COALESCE(v_session_row.progress_state, ''))) IN ('READY', 'READY_EMPTY');

    v_finalisation_actual_precheck_required := v_finalisation_should_evaluate IS NOT TRUE
      AND COALESCE(v_continuation_count, 0) = 0
      AND COALESCE(v_session_row.scope_seed_complete, false)
      AND COALESCE(v_session_row.scope_next_cursor_json, '{}'::jsonb) = '{}'::jsonb
      AND (
        v_stage_job_type = 'WORKBENCH_SESSION_SCOPE_SEED'
        OR v_current_candidate_terminal_success
      );

    IF v_finalisation_actual_precheck_required THEN
      SELECT
        NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_session_scope AS unresolved_scope
          WHERE unresolved_scope.session_id = v_job_row.session_id
            AND (
              UPPER(BTRIM(COALESCE(unresolved_scope.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
              OR COALESCE(unresolved_scope.seeded, false) IS NOT TRUE
              OR COALESCE(unresolved_scope.dirty, false)
              OR NOT (
                unresolved_scope.error_json IS NULL
                OR unresolved_scope.error_json = '{}'::jsonb
                OR unresolved_scope.error_json = 'null'::jsonb
              )
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_line_work AS unresolved_line_work
          WHERE unresolved_line_work.session_id = v_job_row.session_id
            AND (
              UPPER(BTRIM(COALESCE(unresolved_line_work.status, ''))) NOT IN ('MATERIALISED', 'MATERIALIZED', 'SKIPPED')
              OR NOT (
                unresolved_line_work.error_json IS NULL
                OR unresolved_line_work.error_json = '{}'::jsonb
                OR unresolved_line_work.error_json = 'null'::jsonb
              )
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_jobs AS active_job
          CROSS JOIN LATERAL (
            SELECT CASE
              WHEN COALESCE(active_job.payload_json->>'session_version', '') ~ '^[0-9]+$'
                THEN (active_job.payload_json->>'session_version')::bigint
              ELSE NULL::bigint
            END AS job_session_version
          ) AS active_job_version
          WHERE active_job.session_id = v_job_row.session_id
            AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
            AND (
              active_job_version.job_session_version IS NULL
              OR active_job_version.job_session_version >= COALESCE(v_session_row.version, 0)
            )
        )
      INTO v_finalisation_actual_precheck_passed;

      v_finalisation_should_evaluate := COALESCE(v_finalisation_actual_precheck_passed, false);
    END IF;

    IF v_finalisation_should_evaluate THEN
      v_finalisation_progress_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id => v_job_row.session_id,
        p_apply => false,
        p_reason => 'COMPLETE_JOB_FINALISATION_AUTHORITATIVE_COUNTER_SNAPSHOT',
        p_write_progress_json => false
      );

      IF jsonb_typeof(COALESCE(v_finalisation_progress_json, '{}'::jsonb)) <> 'object'
         OR LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR NOT (v_finalisation_progress_json ? 'session_ready') THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_COMPLETE_JOB_FINALISATION_PROGRESS_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_COMPLETE_JOB_FINALISATION_PROGRESS_INVALID',
                  'job_id', p_job_id::text,
                  'session_id', v_job_row.session_id::text,
                  'progress_result', COALESCE(v_finalisation_progress_json, '{}'::jsonb)
                )::text;
      END IF;

      v_finalisation_evaluated := true;
      v_finalisation_session_ready := LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'session_ready', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_finalisation_ready_for_draft := LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_finalisation_ready_empty := LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'ready_empty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_finalisation_stored_ready_mismatch := LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'stored_ready_mismatch', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_finalisation_blocker_codes := CASE
        WHEN jsonb_typeof(COALESCE(v_finalisation_progress_json->'session_blocker_codes', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_finalisation_progress_json->'session_blocker_codes', '[]'::jsonb)
        ELSE '[]'::jsonb
      END;
      v_finalisation_draft_blocker_codes := CASE
        WHEN jsonb_typeof(COALESCE(v_finalisation_progress_json->'draft_blocker_codes', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_finalisation_progress_json->'draft_blocker_codes', '[]'::jsonb)
        ELSE '[]'::jsonb
      END;
      v_finalisation_blocker_counts := CASE
        WHEN jsonb_typeof(COALESCE(v_finalisation_progress_json->'blocker_counts', '{}'::jsonb)) = 'object'
          THEN COALESCE(v_finalisation_progress_json->'blocker_counts', '{}'::jsonb)
        ELSE '{}'::jsonb
      END;

      v_authoritative_scope_total := CASE WHEN COALESCE(v_finalisation_progress_json->>'scope_total_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'scope_total_count')::integer ELSE 0 END;
      v_authoritative_scope_seeded := CASE WHEN COALESCE(v_finalisation_progress_json->>'scope_seeded_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'scope_seeded_count')::integer ELSE 0 END;
      v_authoritative_scope_ready := CASE WHEN COALESCE(v_finalisation_progress_json->>'scope_ready_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'scope_ready_count')::integer ELSE 0 END;
      v_authoritative_scope_pending := CASE WHEN COALESCE(v_finalisation_progress_json->>'scope_pending_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'scope_pending_count')::integer ELSE 0 END;
      v_authoritative_scope_failed := CASE WHEN COALESCE(v_finalisation_progress_json->>'scope_failed_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'scope_failed_count')::integer ELSE 0 END;
      v_authoritative_line_total := CASE WHEN COALESCE(v_finalisation_progress_json->>'line_units_total', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'line_units_total')::integer ELSE 0 END;
      v_authoritative_line_pending := CASE WHEN COALESCE(v_finalisation_progress_json->>'line_units_pending', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'line_units_pending')::integer ELSE 0 END;
      v_authoritative_line_ready := CASE WHEN COALESCE(v_finalisation_progress_json->>'line_units_ready', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'line_units_ready')::integer ELSE 0 END;
      v_authoritative_line_failed := CASE WHEN COALESCE(v_finalisation_progress_json->>'line_units_failed', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'line_units_failed')::integer ELSE 0 END;
      v_authoritative_preview_row_count := CASE WHEN COALESCE(v_finalisation_progress_json->>'preview_row_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'preview_row_count')::integer ELSE 0 END;
      v_authoritative_selected_row_count := CASE WHEN COALESCE(v_finalisation_progress_json->>'selected_row_count', '') ~ '^[0-9]+$' THEN (v_finalisation_progress_json->>'selected_row_count')::integer ELSE 0 END;
      v_authoritative_section_counts_json := CASE
        WHEN jsonb_typeof(COALESCE(v_finalisation_progress_json->'section_counts_json', '{}'::jsonb)) = 'object'
          THEN COALESCE(v_finalisation_progress_json->'section_counts_json', '{}'::jsonb)
        ELSE '{}'::jsonb
      END;
      v_authoritative_candidate_sample_rows_json := CASE
        WHEN jsonb_typeof(COALESCE(v_finalisation_progress_json->'candidate_sample_rows_json', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_finalisation_progress_json->'candidate_sample_rows_json', '[]'::jsonb)
        ELSE '[]'::jsonb
      END;

      v_finalisation_counter_reconciliation_required := COALESCE(v_session_row.scope_total_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_scope_total, 0)
        OR COALESCE(v_session_row.scope_seeded_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_scope_seeded, 0)
        OR COALESCE(v_session_row.scope_ready_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_scope_ready, 0)
        OR COALESCE(v_session_row.scope_pending_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_scope_pending, 0)
        OR COALESCE(v_session_row.scope_failed_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_scope_failed, 0)
        OR COALESCE(v_session_row.line_units_total, 0) IS DISTINCT FROM COALESCE(v_authoritative_line_total, 0)
        OR COALESCE(v_session_row.line_units_pending, 0) IS DISTINCT FROM COALESCE(v_authoritative_line_pending, 0)
        OR COALESCE(v_session_row.line_units_ready, 0) IS DISTINCT FROM COALESCE(v_authoritative_line_ready, 0)
        OR COALESCE(v_session_row.line_units_failed, 0) IS DISTINCT FROM COALESCE(v_authoritative_line_failed, 0)
        OR COALESCE(v_session_row.preview_row_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_preview_row_count, 0)
        OR COALESCE(v_session_row.selected_row_count, 0) IS DISTINCT FROM COALESCE(v_authoritative_selected_row_count, 0)
        OR COALESCE(v_session_row.section_counts_json, '{}'::jsonb) IS DISTINCT FROM COALESCE(v_authoritative_section_counts_json, '{}'::jsonb);
      v_finalisation_counter_reconciliation_applied := v_duplicate_completion IS NOT TRUE;
      v_finalisation_counter_reconciled := v_finalisation_counter_reconciliation_applied;

      v_finalisation_counter_reconciliation_json := jsonb_build_object(
        'required', v_finalisation_counter_reconciliation_required,
        'applied', v_finalisation_counter_reconciliation_applied,
        'changed', v_finalisation_counter_reconciliation_required AND v_finalisation_counter_reconciliation_applied,
        'reconciled', v_finalisation_counter_reconciled,
        'scope_total_count', COALESCE(v_authoritative_scope_total, 0),
        'scope_seeded_count', COALESCE(v_authoritative_scope_seeded, 0),
        'scope_ready_count', COALESCE(v_authoritative_scope_ready, 0),
        'scope_pending_count', COALESCE(v_authoritative_scope_pending, 0),
        'scope_failed_count', COALESCE(v_authoritative_scope_failed, 0),
        'line_units_total', COALESCE(v_authoritative_line_total, 0),
        'line_units_pending', COALESCE(v_authoritative_line_pending, 0),
        'line_units_ready', COALESCE(v_authoritative_line_ready, 0),
        'line_units_failed', COALESCE(v_authoritative_line_failed, 0),
        'preview_row_count', COALESCE(v_authoritative_preview_row_count, 0),
        'selected_row_count', COALESCE(v_authoritative_selected_row_count, 0),
        'section_counts_json', COALESCE(v_authoritative_section_counts_json, '{}'::jsonb)
      );

      IF v_finalisation_session_ready THEN
        v_final_progress_state := CASE WHEN v_finalisation_ready_empty THEN 'READY_EMPTY' ELSE 'READY' END;
        v_final_phase := 'READY';
        v_final_status_text := 'Payment preview is ready.';
        v_final_next_recommended_action := 'READ_PREVIEW_PAGE';
      ELSE
        v_final_progress_state := UPPER(BTRIM(COALESCE(v_finalisation_progress_json->>'progress_state', '')));
        IF v_final_progress_state IN ('', 'READY', 'READY_EMPTY') THEN
          v_final_progress_state := CASE
            WHEN COALESCE(v_authoritative_scope_failed, 0) > 0
              OR COALESCE(v_authoritative_line_failed, 0) > 0
              OR (COALESCE(v_finalisation_blocker_codes, '[]'::jsonb) ? 'WORKBENCH_JOBS_FAILED') THEN 'ERROR'
            WHEN COALESCE(v_authoritative_line_ready, 0) > 0 THEN 'MATERIALISING_PREVIEW_ROWS'
            WHEN COALESCE(v_authoritative_line_pending, 0) > 0 THEN 'PROCESSING_LINE_WORK'
            ELSE 'REFRESHING_CANDIDATES'
          END;
        END IF;
        v_final_phase := COALESCE(NULLIF(BTRIM(v_finalisation_progress_json->>'phase'), ''), 'REFRESHING_CANDIDATES');
        v_final_status_text := COALESCE(NULLIF(BTRIM(v_finalisation_progress_json->>'status_text'), ''), 'Preparing payment preview.');
        v_final_next_recommended_action := COALESCE(NULLIF(BTRIM(v_finalisation_progress_json->>'next_recommended_action'), ''), 'WAIT_FOR_WORKER');
      END IF;

      IF v_duplicate_completion IS NOT TRUE THEN
        UPDATE public.banking_pay_workbench_sessions AS session_update
        SET scope_total_count = GREATEST(COALESCE(v_authoritative_scope_total, 0), 0),
          scope_seeded_count = GREATEST(COALESCE(v_authoritative_scope_seeded, 0), 0),
          scope_ready_count = GREATEST(COALESCE(v_authoritative_scope_ready, 0), 0),
          scope_pending_count = GREATEST(COALESCE(v_authoritative_scope_pending, 0), 0),
          scope_failed_count = GREATEST(COALESCE(v_authoritative_scope_failed, 0), 0),
          line_units_total = GREATEST(COALESCE(v_authoritative_line_total, 0), 0),
          line_units_pending = GREATEST(COALESCE(v_authoritative_line_pending, 0), 0),
          line_units_ready = GREATEST(COALESCE(v_authoritative_line_ready, 0), 0),
          line_units_failed = GREATEST(COALESCE(v_authoritative_line_failed, 0), 0),
          preview_row_count = GREATEST(COALESCE(v_authoritative_preview_row_count, 0), 0),
          selected_row_count = GREATEST(COALESCE(v_authoritative_selected_row_count, 0), 0),
          section_counts_json = COALESCE(v_authoritative_section_counts_json, '{}'::jsonb),
          candidate_sample_rows_json = COALESCE(v_authoritative_candidate_sample_rows_json, '[]'::jsonb),
          progress_state = v_final_progress_state,
          progress_json = public.pay_workbench_session_compact_progress_json(
            jsonb_strip_nulls(
              jsonb_build_object(
              'ok', true,
              'server_utc', v_now,
              'updated_at_utc', v_now,
              'phase', v_final_phase,
              'status_text', v_final_status_text,
              'next_recommended_action', v_final_next_recommended_action,
              'progress_state', v_final_progress_state,
              'ready', v_finalisation_session_ready,
              'ready_flag', v_finalisation_session_ready,
              'session_ready', v_finalisation_session_ready,
              'ready_for_draft', v_finalisation_ready_for_draft,
              'can_create_draft', v_finalisation_ready_for_draft,
              'ready_empty', v_finalisation_ready_empty,
              'preview_ready', v_finalisation_session_ready,
              'work_queued', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'work_queued', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'still_running', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'still_running', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'pending_refresh', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'pending_refresh', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'refresh_pending', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'refresh_pending', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'preview_refresh_pending', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'preview_refresh_pending', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'preview_deferred', CASE
                WHEN v_finalisation_session_ready THEN false
                ELSE LOWER(BTRIM(COALESCE(v_finalisation_progress_json->>'preview_deferred', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              END,
              'authoritative_session_ready', v_finalisation_session_ready,
              'authoritative_ready_for_draft', v_finalisation_ready_for_draft,
              'authoritative_ready_empty', v_finalisation_ready_empty,
              'authoritative_progress_state', v_final_progress_state,
              'session_blocker_codes', COALESCE(v_finalisation_blocker_codes, '[]'::jsonb),
              'draft_blocker_codes', COALESCE(v_finalisation_draft_blocker_codes, '[]'::jsonb),
              'blocker_codes', COALESCE(v_finalisation_draft_blocker_codes, '[]'::jsonb),
              'blocker_counts', COALESCE(v_finalisation_blocker_counts, '{}'::jsonb),
              'stored_ready_mismatch', v_finalisation_stored_ready_mismatch
            )
            || jsonb_build_object(
              'scope_total_count', COALESCE(v_authoritative_scope_total, 0),
              'scope_seeded_count', COALESCE(v_authoritative_scope_seeded, 0),
              'scope_ready_count', COALESCE(v_authoritative_scope_ready, 0),
              'scope_pending_count', COALESCE(v_authoritative_scope_pending, 0),
              'scope_failed_count', COALESCE(v_authoritative_scope_failed, 0),
              'line_units_total', COALESCE(v_authoritative_line_total, 0),
              'line_units_pending', COALESCE(v_authoritative_line_pending, 0),
              'line_units_ready', COALESCE(v_authoritative_line_ready, 0),
              'line_units_failed', COALESCE(v_authoritative_line_failed, 0),
              'preview_row_count', COALESCE(v_authoritative_preview_row_count, 0),
              'selected_row_count', COALESCE(v_authoritative_selected_row_count, 0),
              'selected_eligible_ready_row_count', CASE
                WHEN COALESCE(v_finalisation_progress_json->>'selected_eligible_ready_row_count', '') ~ '^[0-9]+$'
                  THEN (v_finalisation_progress_json->>'selected_eligible_ready_row_count')::integer
                ELSE COALESCE(v_authoritative_selected_row_count, 0)
              END,
              'section_counts_json', COALESCE(v_authoritative_section_counts_json, '{}'::jsonb),
              'candidate_sample_rows_json', COALESCE(v_authoritative_candidate_sample_rows_json, '[]'::jsonb),
              'candidate_counts', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'candidate_counts') = 'object'
                  THEN v_finalisation_progress_json->'candidate_counts'
                ELSE '{}'::jsonb
              END,
              'line_counts', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'line_counts') = 'object'
                  THEN v_finalisation_progress_json->'line_counts'
                ELSE '{}'::jsonb
              END,
              'job_counts', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'job_counts') = 'object'
                  THEN v_finalisation_progress_json->'job_counts'
                ELSE '{}'::jsonb
              END,
              'source_build_counts', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'source_build_counts') = 'object'
                  THEN v_finalisation_progress_json->'source_build_counts'
                ELSE '{}'::jsonb
              END,
              'pending_job_ids_json', CASE
                WHEN v_finalisation_session_ready THEN '[]'::jsonb
                WHEN jsonb_typeof(v_finalisation_progress_json->'pending_job_ids_json') = 'array'
                  THEN v_finalisation_progress_json->'pending_job_ids_json'
                ELSE '[]'::jsonb
              END,
              'session_blockers', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'session_blockers') = 'array'
                  THEN v_finalisation_progress_json->'session_blockers'
                ELSE '[]'::jsonb
              END,
              'draft_blockers', CASE
                WHEN jsonb_typeof(v_finalisation_progress_json->'draft_blockers') = 'array'
                  THEN v_finalisation_progress_json->'draft_blockers'
                ELSE '[]'::jsonb
              END
            )
            || jsonb_build_object(
              'finalisation_evaluated', true,
              'finalisation_evaluated_at_utc', v_now::text,
              'finalisation_source_job_id', p_job_id::text,
              'finalisation_source_job_type', v_stage_job_type,
              'terminal_readiness_deferred', v_finalisation_session_ready IS NOT TRUE,
              'finalisation_pending', v_finalisation_session_ready IS NOT TRUE,
              'finalised_at_utc', CASE WHEN v_finalisation_session_ready THEN v_now::text ELSE NULL::text END,
              'counter_reconciled', v_finalisation_counter_reconciled,
              'counter_reconciliation', COALESCE(v_finalisation_counter_reconciliation_json, '{}'::jsonb)
            )
            ),
            true
          ),
          progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
        WHERE session_update.id = v_job_row.session_id
        RETURNING session_update.*
        INTO v_session_row;
      END IF;
    END IF;
    END IF;
  END IF;

  v_completion_finalisation_json := jsonb_build_object(
    'finalisation_evaluated', v_finalisation_evaluated,
    'session_ready', CASE WHEN v_finalisation_evaluated THEN v_finalisation_session_ready ELSE NULL::boolean END,
    'ready_for_draft', CASE WHEN v_finalisation_evaluated THEN v_finalisation_ready_for_draft ELSE NULL::boolean END,
    'ready_empty', CASE WHEN v_finalisation_evaluated THEN v_finalisation_ready_empty ELSE NULL::boolean END,
    'final_progress_state', CASE WHEN v_finalisation_evaluated THEN v_final_progress_state ELSE v_session_row.progress_state END,
    'blocker_codes', COALESCE(v_finalisation_blocker_codes, '[]'::jsonb),
    'draft_blocker_codes', COALESCE(v_finalisation_draft_blocker_codes, '[]'::jsonb),
    'counter_reconciliation_required', v_finalisation_counter_reconciliation_required,
    'counter_reconciliation_applied', v_finalisation_counter_reconciliation_applied,
    'counter_reconciled', v_finalisation_counter_reconciled,
    'counter_reconciliation', COALESCE(v_finalisation_counter_reconciliation_json, '{}'::jsonb),
    'source_build_reconciliation_applied', v_source_reconciliation_applied,
    'source_build_reconciliation', COALESCE(v_source_reconciliation_json, '{}'::jsonb),
    'actual_precheck_required', v_finalisation_actual_precheck_required,
    'actual_precheck_passed', v_finalisation_actual_precheck_passed,
    'scope_pending_job_cleared_count', COALESCE(v_scope_pending_job_cleared_count, 0),
    'continuation_scope_counter_adjusted', v_scope_continuation_counter_adjusted,
    'continuation_scope_old_status', v_scope_status_before_continuation,
    'continuation_scope_new_status', v_scope_status_after_continuation,
    'continuation_scope_pending_delta', COALESCE(v_scope_continuation_pending_delta, 0),
    'continuation_scope_ready_delta', COALESCE(v_scope_continuation_ready_delta, 0),
    'continuation_scope_failed_delta', COALESCE(v_scope_continuation_failed_delta, 0),
    'source_build_reconciliation_deferred', COALESCE(v_source_reconciliation_deferred, false),
    'source_build_reconciliation_deferred_reason', v_source_reconciliation_deferred_reason,
    'source_empty_session_progress_deferred', COALESCE(v_source_empty_session_progress_deferred, false),
    'source_empty_session_progress_deferred_reason', v_source_empty_session_progress_deferred_reason,
    'source_empty_cleanup_source_row_count', COALESCE(v_source_empty_cleanup_source_row_count, 0),
    'source_empty_cleanup_line_work_count', COALESCE(v_source_empty_cleanup_line_work_count, 0),
    'source_empty_cleanup_preview_row_count', COALESCE(v_source_empty_cleanup_preview_row_count, 0),
    'source_empty_cleanup_targeted_timesheet_count', COALESCE(v_source_empty_targeted_timesheet_count, 0),
    'continuation_scope_counter_deferred', COALESCE(v_continuation_scope_counter_deferred, false),
    'continuation_scope_counter_deferred_reason', v_continuation_scope_counter_deferred_reason,
    'finalisation_deferred', COALESCE(v_finalisation_deferred, false),
    'finalisation_deferred_reason', v_finalisation_deferred_reason,
    'evaluated_at_utc', CASE WHEN v_finalisation_evaluated THEN v_now::text ELSE NULL::text END
  );

  IF v_duplicate_completion IS NOT TRUE THEN
    UPDATE public.banking_pay_workbench_jobs AS update_job
    SET payload_json = COALESCE(update_job.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'completion_json', COALESCE(update_job.payload_json->'completion_json', '{}'::jsonb)
              || v_completion_finalisation_json
          ),
        updated_at_utc = v_now
    WHERE update_job.id = p_job_id
    RETURNING update_job.*
    INTO v_job_row;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      p_job_id::text,
      'SUCCEEDED',
      NULL,
      jsonb_build_object(
        'id', p_job_id::text,
        'job_type', v_job_row.job_type,
        'status', v_job_row.status,
        'snapshot_run_id', CASE WHEN v_job_row.snapshot_run_id IS NULL THEN NULL ELSE v_job_row.snapshot_run_id::text END,
        'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL ELSE v_job_row.session_id::text END,
        'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL ELSE v_job_row.candidate_id::text END,
        'attempt_count', v_job_row.attempt_count,
        'completed_at_utc', v_job_row.completed_at_utc,
        'continuation_count', COALESCE(v_continuation_count, 0),
        'continuation_reused_count', COALESCE(v_continuation_reused_count, 0),
        'next_recommended_action', v_next_recommended_action
      )
      || jsonb_build_object(
        'has_more', v_has_more,
        'next_cursor_present', v_next_cursor IS NOT NULL,
        'seeded_count', COALESCE(v_seeded_count, 0),
        'source_rows_written', COALESCE(v_source_rows_written, 0),
        'current_source_row_count', COALESCE(v_current_source_row_count, 0),
        'source_build_run_id', v_source_build_run_id_text,
        'source_change_seq', v_source_change_seq,
        'new_scope_count', COALESCE(v_new_scope_count, 0),
        'processed_count', COALESCE(v_processed_count, 0),
        'ready_count_delta', COALESCE(v_ready_count_delta, 0),
        'materialised_count', COALESCE(v_materialised_count, 0),
        'error_count', COALESCE(v_error_count, 0),
        'finalisation', v_completion_finalisation_json,
        'source_build_reconciliation', COALESCE(v_source_reconciliation_json, '{}'::jsonb),
        'source_build_reconciliation_deferred', COALESCE(v_source_reconciliation_deferred, false),
        'source_build_reconciliation_deferred_reason', v_source_reconciliation_deferred_reason
      ),
      'WORKBENCH_JOB_COMPLETED',
      NULL
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'status', 'SUCCEEDED',
    'completed_at_utc', COALESCE(v_completed_at_utc, v_job_row.completed_at_utc, v_now),
    'result_json', v_result_json,
    'continuation_enqueued', v_continuation_enqueued,
    'continuation_jobs', COALESCE(v_continuation_jobs, '[]'::jsonb),
    'continuation_count', COALESCE(v_continuation_count, 0),
    'continuation_reused_count', COALESCE(v_continuation_reused_count, 0),
    'next_recommended_action', v_next_recommended_action,
    'has_more', v_has_more,
    'next_cursor_present', v_next_cursor IS NOT NULL,
    'line_work_pending_after_completion', CASE WHEN v_stage_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN v_pending_line_work_exists ELSE NULL END,
    'source_rows_written', COALESCE(v_source_rows_written, 0),
    'current_source_row_count', COALESCE(v_current_source_row_count, 0),
    'source_build_run_id', v_source_build_run_id_text,
    'source_change_seq', v_source_change_seq,
    'stage_job_type', v_stage_job_type,
    'duplicate_completion', v_duplicate_completion,
    'source_build_reconciliation_applied', v_source_reconciliation_applied,
    'source_build_reconciliation_deferred', COALESCE(v_source_reconciliation_deferred, false),
    'source_build_reconciliation_deferred_reason', v_source_reconciliation_deferred_reason,
    'source_build_reconciliation', COALESCE(v_source_reconciliation_json, '{}'::jsonb)
  )
  || jsonb_build_object(
    'finalisation_evaluated', v_finalisation_evaluated,
    'session_ready', CASE WHEN v_finalisation_evaluated THEN v_finalisation_session_ready ELSE NULL::boolean END,
    'ready_for_draft', CASE WHEN v_finalisation_evaluated THEN v_finalisation_ready_for_draft ELSE NULL::boolean END,
    'ready_empty', CASE WHEN v_finalisation_evaluated THEN v_finalisation_ready_empty ELSE NULL::boolean END,
    'final_progress_state', CASE WHEN v_finalisation_evaluated THEN v_final_progress_state ELSE v_session_row.progress_state END,
    'blocker_codes', COALESCE(v_finalisation_blocker_codes, '[]'::jsonb),
    'draft_blocker_codes', COALESCE(v_finalisation_draft_blocker_codes, '[]'::jsonb),
    'counter_reconciliation_required', v_finalisation_counter_reconciliation_required,
    'counter_reconciliation_applied', v_finalisation_counter_reconciliation_applied,
    'counter_reconciled', v_finalisation_counter_reconciled,
    'counter_reconciliation', COALESCE(v_finalisation_counter_reconciliation_json, '{}'::jsonb),
    'actual_precheck_required', v_finalisation_actual_precheck_required,
    'actual_precheck_passed', v_finalisation_actual_precheck_passed,
    'scope_pending_job_cleared_count', COALESCE(v_scope_pending_job_cleared_count, 0),
    'continuation_scope_counter_adjusted', v_scope_continuation_counter_adjusted,
    'continuation_scope_old_status', v_scope_status_before_continuation,
    'continuation_scope_new_status', v_scope_status_after_continuation,
    'continuation_scope_pending_delta', COALESCE(v_scope_continuation_pending_delta, 0),
    'continuation_scope_ready_delta', COALESCE(v_scope_continuation_ready_delta, 0),
    'continuation_scope_failed_delta', COALESCE(v_scope_continuation_failed_delta, 0),
    'source_empty_session_progress_deferred', COALESCE(v_source_empty_session_progress_deferred, false),
    'source_empty_session_progress_deferred_reason', v_source_empty_session_progress_deferred_reason,
    'source_empty_cleanup_source_row_count', COALESCE(v_source_empty_cleanup_source_row_count, 0),
    'source_empty_cleanup_line_work_count', COALESCE(v_source_empty_cleanup_line_work_count, 0),
    'source_empty_cleanup_preview_row_count', COALESCE(v_source_empty_cleanup_preview_row_count, 0),
    'source_empty_cleanup_targeted_timesheet_count', COALESCE(v_source_empty_targeted_timesheet_count, 0),
    'continuation_scope_counter_deferred', COALESCE(v_continuation_scope_counter_deferred, false),
    'continuation_scope_counter_deferred_reason', v_continuation_scope_counter_deferred_reason,
    'finalisation_deferred', COALESCE(v_finalisation_deferred, false),
    'finalisation_deferred_reason', v_finalisation_deferred_reason
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_complete_job(p_job_id uuid, p_result_json jsonb) TO service_role;
