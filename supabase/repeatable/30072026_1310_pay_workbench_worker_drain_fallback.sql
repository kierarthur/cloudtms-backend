CREATE OR REPLACE FUNCTION public.pay_workbench_worker_drain_chunk(p_limit integer DEFAULT 5, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_session_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_allowed_job_types text[] DEFAULT NULL::text[], p_worker_id text DEFAULT NULL::text, p_lease_seconds integer DEFAULT 180)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_cutoff timestamptz := COALESCE(p_now_utc, now());
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 5), 1), 100);
  v_worker_id text := LEFT(
    COALESCE(
      NULLIF(BTRIM(COALESCE(p_worker_id, '')), ''),
      'BANKING_PAY_WORKBENCH_DB_WORKER'
    ),
    200
  );
  v_lease_seconds integer := LEAST(
    GREATEST(COALESCE(p_lease_seconds, 180), 25),
    3600
  );
  v_supported_job_types text[] := ARRAY[
    'WORKBENCH_SESSION_SCOPE_SEED',
    'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'WORKBENCH_SESSION_CLONE_REBASE',
    'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
    'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
    'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
    'CONTRACT_CLIENT_DIRTY_FANOUT'
  ]::text[];
  v_allowed_job_types text[] := ARRAY[]::text[];
  v_claim_result jsonb := '{}'::jsonb;
  v_claimed_jobs_json jsonb := '[]'::jsonb;
  v_claimed_job_json jsonb := '{}'::jsonb;
  v_job_row public.banking_pay_workbench_jobs%ROWTYPE;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_job_id uuid := NULL::uuid;
  v_raw_job_type text := '';
  v_canonical_job_type text := '';
  v_original_job_type text := '';
  v_job_type_normalized boolean := false;
  v_payload_json jsonb := '{}'::jsonb;
  v_cursor_json jsonb := NULL::jsonb;
  v_job_limit_text text := NULL::text;
  v_job_limit integer := 100;
  v_payload_session_version bigint := NULL::bigint;
  v_source_change_seq bigint := NULL::bigint;
  v_live_change_seq bigint := 0;
  v_scope_exists boolean := false;
  v_is_obsolete boolean := false;
  v_obsolete_reason text := NULL::text;
  v_obsolete_result jsonb := '{}'::jsonb;
  v_stage_result jsonb := '{}'::jsonb;
  v_completion_result jsonb := '{}'::jsonb;
  v_fail_result jsonb := '{}'::jsonb;
  v_error_json jsonb := '{}'::jsonb;
  v_failure_sqlstate text := NULL::text;
  v_failure_message text := NULL::text;
  v_failure_detail text := NULL::text;
  v_failure_hint text := NULL::text;
  v_failure_context text := NULL::text;
  v_failure_handler_sqlstate text := NULL::text;
  v_failure_handler_message text := NULL::text;
  v_failure_handler_detail text := NULL::text;
  v_failure_handler_hint text := NULL::text;
  v_failure_handler_context text := NULL::text;
  v_failure_handler_error_json jsonb := '{}'::jsonb;
  v_failure_fallback_decision text := NULL::text;
  v_failure_fallback_updated_count integer := 0;
  v_failure_fallback_scope_updated_count integer := 0;
  v_failure_fallback_line_count integer := 0;
  v_failure_fallback_audit_failed boolean := false;
  v_retry_after_seconds integer := 30;
  v_final_failure_status text := 'FAILED';
  v_completion_continuation_count integer := 0;
  v_completion_continuation_reused_count integer := 0;
  v_fanout_continuation_job_id uuid := NULL::uuid;
  v_fanout_continuation_created boolean := false;
  v_fanout_continuation_dedupe_key text := NULL::text;
  v_fanout_cursor_token text := NULL::text;
  v_fanout_scope_kind text := NULL::text;
  v_fanout_scope_id text := NULL::text;
  v_next_cursor_json jsonb := NULL::jsonb;
  v_continuation_payload_json jsonb := '{}'::jsonb;
  v_claimed_count integer := 0;
  v_processed_claimed_job_ids uuid[] := ARRAY[]::uuid[];
  v_requeued_unprocessed_claimed_count integer := 0;
  v_processed_count integer := 0;
  v_delta_refresh_jobs_processed integer := 0;
  v_delta_source_rows_written integer := 0;
  v_delta_line_rows_written integer := 0;
  v_delta_preview_rows_written integer := 0;
  v_delta_rows_superseded integer := 0;
  v_delta_fallback_count integer := 0;
  v_delta_patch_count integer := 0;
  v_delta_more_due_count integer := 0;
  v_clone_rebase_jobs_processed integer := 0;
  v_clone_copied_candidate_count integer := 0;
  v_clone_copied_preview_row_count integer := 0;
  v_clone_legacy_refresh_enqueued_count integer := 0;
  v_clone_more_due_count integer := 0;
  v_succeeded_count integer := 0;
  v_failed_count integer := 0;
  v_dead_count integer := 0;
  v_obsolete_skipped_count integer := 0;
  v_continuations_created integer := 0;
  v_continuations_reused integer := 0;
  v_recovered_stale_count integer := 0;
  v_dead_stale_count integer := 0;
  v_supplemental_stale_row record;
  v_supplemental_stale_fail_result jsonb := '{}'::jsonb;
  v_supplemental_stale_status text := NULL::text;
  v_supplemental_stale_error_json jsonb := '{}'::jsonb;
  v_supplemental_stale_recovered_count integer := 0;
  v_supplemental_stale_terminal_count integer := 0;
  v_supplemental_stale_recovery_error_count integer := 0;
  v_more_due boolean := false;
  v_job_results_json jsonb := '[]'::jsonb;
  v_started_at_utc timestamptz := clock_timestamp();
  v_phase_started_at_utc timestamptz := clock_timestamp();
  v_elapsed_ms integer := 0;
  v_claim_elapsed_ms integer := 0;
  v_supplemental_stale_elapsed_ms integer := 0;
  v_final_more_due_elapsed_ms integer := 0;
  v_stop_reason text := NULL::text;
  v_max_runtime_ms integer := 8000;
  v_min_phase_budget_ms integer := 2500;
  v_stage_work_units_per_job integer := 25;
  v_job_retry_base_seconds integer := 30;
  v_job_retry_max_seconds integer := 900;
  v_settings_db_worker_lease_seconds integer := NULL::integer;
  v_settings_db_worker_max_runtime_ms integer := NULL::integer;
  v_settings_db_worker_min_phase_budget_ms integer := NULL::integer;
  v_settings_stage_work_units_per_job integer := NULL::integer;
  v_settings_defaults_json jsonb := '{}'::jsonb;
  v_worker_budget_profile text := 'GENERIC';
  v_scope_seed_work_units_per_job integer := 25;
  v_source_build_work_units_per_job integer := 10;
  v_line_seed_work_units_per_job integer := 50;
  v_line_process_work_units_per_job integer := 25;
  v_preview_materialise_work_units_per_job integer := 25;
  v_delta_refresh_work_units_per_job integer := 25;
  v_clone_rebase_work_units_per_job integer := 100;
  v_stage_limit_for_job integer := 25;
  v_settings_job_retry_base_seconds integer := NULL::integer;
  v_settings_job_retry_max_seconds integer := NULL::integer;
  v_due_queued_count integer := 0;
  v_claimable_count integer := 0;
  v_running_count integer := 0;
  v_stale_running_count integer := 0;
  v_claim_lock_contention_detected boolean := false;
  v_claim_lock_contention_count integer := 0;
  v_claim_lock_contention_sample jsonb := '[]'::jsonb;
  v_claim_mismatch_json jsonb := '{}'::jsonb;
  v_claim_mismatch_detected boolean := false;
  v_claim_mismatch_reason text := NULL::text;
  v_budget_claim_limit integer := 1;
  v_dirty_priority_result jsonb := '{}'::jsonb;
  v_dirty_priority_jobs_processed integer := 0;
  v_dirty_priority_jobs_remaining integer := 0;
  v_dirty_priority_cap_reached boolean := false;
  v_dirty_priority_made_progress boolean := false;
  v_dirty_priority_created_job_ids jsonb := '[]'::jsonb;
  v_dirty_priority_actual_refresh_job_ids jsonb := '[]'::jsonb;
  v_dirty_priority_claimed_job_id uuid := NULL::uuid;

  v_claim_phase_started_at timestamptz := NULL::timestamptz;
  v_claim_phase_completed_at timestamptz := NULL::timestamptz;
  v_pre_claim_due_count integer := 0;
  v_pre_claim_claimable_count integer := 0;
  v_pre_claim_job_sample jsonb := '[]'::jsonb;
  v_post_claim_due_job_ids jsonb := '[]'::jsonb;
  v_post_claim_due_job_types jsonb := '[]'::jsonb;
  v_post_claim_due_sample jsonb := '[]'::jsonb;
  v_created_after_claim_count integer := 0;
  v_post_claim_due_detected boolean := false;
  v_post_claim_due_reason text := NULL::text;

  v_made_progress_current_pass boolean := false;
  v_made_progress_cumulative boolean := false;
  v_normal_claim_made_progress boolean := false;
  v_terminalisation_count integer := 0;
  v_projection_terminalisation_count integer := 0;

  v_obsolete_projection_run_id uuid := NULL::uuid;
  v_projection_status_before text := NULL::text;
  v_projection_status_after text := NULL::text;
  v_active_continuation_count integer := 0;
  v_active_continuation_job_ids uuid[] := ARRAY[]::uuid[];
  v_active_continuation_job_ids_json jsonb := '[]'::jsonb;
  v_delta_projection_diag_json jsonb := '{}'::jsonb;
  v_obsolete_projection_update_count integer := 0;
BEGIN
  SELECT
    sd.banking_pay_workbench_db_worker_lease_seconds,
    sd.banking_pay_workbench_db_worker_max_runtime_ms,
    sd.banking_pay_workbench_db_worker_min_phase_budget_ms,
    sd.banking_pay_workbench_stage_work_units_per_job,
    to_jsonb(sd),
    sd.banking_pay_workbench_job_retry_base_seconds,
    sd.banking_pay_workbench_job_retry_max_seconds
  INTO
    v_settings_db_worker_lease_seconds,
    v_settings_db_worker_max_runtime_ms,
    v_settings_db_worker_min_phase_budget_ms,
    v_settings_stage_work_units_per_job,
    v_settings_defaults_json,
    v_settings_job_retry_base_seconds,
    v_settings_job_retry_max_seconds
  FROM public.settings_defaults AS sd
  WHERE sd.id = 1
  LIMIT 1;

  v_lease_seconds := LEAST(GREATEST(COALESCE(v_settings_db_worker_lease_seconds, p_lease_seconds, 180), 25), 3600);
  v_worker_budget_profile := CASE
    WHEN UPPER(BTRIM(COALESCE(v_worker_id, ''))) LIKE '%NUDGE%' THEN 'NUDGE'
    WHEN UPPER(BTRIM(COALESCE(v_worker_id, ''))) LIKE '%CRON%'
      OR UPPER(BTRIM(COALESCE(v_worker_id, ''))) LIKE '%SCHEDULED%'
      OR UPPER(BTRIM(COALESCE(v_worker_id, ''))) LIKE '%TICK%' THEN 'CRON'
    ELSE 'GENERIC'
  END;
  v_stage_work_units_per_job := LEAST(GREATEST(COALESCE(v_settings_stage_work_units_per_job, 25), 1), 100);
  v_scope_seed_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_scope_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_scope_seed_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_scope_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_scope_seed_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_scope_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_scope_seed_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job
  ), 1), 100);
  v_source_build_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_source_build_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_source_build_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_source_build_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_source_build_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_source_build_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_source_build_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job
  ), 1), 100);
  v_line_seed_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_line_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_line_seed_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_line_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_line_seed_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_line_seed_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_line_seed_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job
  ), 1), 100);
  v_line_process_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_line_process_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_line_process_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_line_process_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_line_process_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_line_process_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_line_process_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job
  ), 1), 100);
  v_preview_materialise_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_preview_mat_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_preview_mat_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_preview_mat_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_preview_mat_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_preview_mat_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_preview_mat_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job
  ), 1), 100);
  v_delta_refresh_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_delta_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_delta_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_delta_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_delta_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_delta_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_delta_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job,
    25
  ), 1), 100);
  v_clone_rebase_work_units_per_job := LEAST(GREATEST(COALESCE(
    CASE WHEN v_worker_budget_profile = 'NUDGE' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_nudge_clone_rebase_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_nudge_clone_rebase_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN v_worker_budget_profile = 'CRON' AND COALESCE(v_settings_defaults_json->>'banking_pay_workbench_cron_clone_rebase_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_cron_clone_rebase_units_per_job')::integer ELSE NULL::integer END,
    CASE WHEN COALESCE(v_settings_defaults_json->>'banking_pay_workbench_clone_rebase_units_per_job', '') ~ '^[0-9]{1,9}$' THEN (v_settings_defaults_json->>'banking_pay_workbench_clone_rebase_units_per_job')::integer ELSE NULL::integer END,
    v_stage_work_units_per_job,
    100
  ), 1), 250);
  v_max_runtime_ms := LEAST(GREATEST(COALESCE(v_settings_db_worker_max_runtime_ms, 8000), 1000), 8000);
  v_min_phase_budget_ms := LEAST(
    GREATEST(COALESCE(v_settings_db_worker_min_phase_budget_ms, 2500), 250),
    GREATEST(250, v_max_runtime_ms - 250)
  );
  v_job_retry_base_seconds := LEAST(GREATEST(COALESCE(v_settings_job_retry_base_seconds, 30), 5), 3600);
  v_job_retry_max_seconds := LEAST(
    GREATEST(COALESCE(v_settings_job_retry_max_seconds, 900), v_job_retry_base_seconds),
    86400
  );

  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_WORKER_CHUNK');

  PERFORM public._temp_diag_log(
    'WORKER_BUDGET_PROFILE_APPLIED',
    'TEMP_BANKING_PAY_WORKBENCH',
    COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id),
    jsonb_build_object(
      'function_name', 'pay_workbench_worker_drain_chunk',
      'worker_id', v_worker_id,
      'origin', 'pay_workbench_worker_drain_chunk',
      'budget_profile', v_worker_budget_profile,
      'db_worker_max_runtime_ms', v_max_runtime_ms,
      'db_statement_timeout_ms', 15000,
      'backend_rpc_timeout_ms', LEAST(v_max_runtime_ms + 1000, 14000),
      'computed_rpc_budget_ms', v_max_runtime_ms,
      'min_phase_budget_ms', v_min_phase_budget_ms,
      'policy', 'WORKER_BUDGET_BOUND_BELOW_STATEMENT_TIMEOUT'
    )
  );

  v_dirty_priority_result := public.pay_workbench_dirty_apply_jobs_chunk(
    p_limit => 1,
    p_now_utc => v_cutoff,
    p_session_id => p_session_id,
    p_candidate_id => p_candidate_id,
    p_worker_id => v_worker_id || ':DIRTY_PRIORITY',
    p_lease_seconds => v_lease_seconds
  );
  v_dirty_priority_jobs_processed := COALESCE((v_dirty_priority_result->>'dirty_priority_jobs_processed')::integer, 0);
  v_dirty_priority_jobs_remaining := COALESCE((v_dirty_priority_result->>'dirty_priority_jobs_remaining')::integer, 0);
  v_dirty_priority_cap_reached := lower(COALESCE(v_dirty_priority_result->>'cap_reached', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_dirty_priority_made_progress := COALESCE(v_dirty_priority_jobs_processed, 0) > 0
    OR CASE WHEN COALESCE(v_dirty_priority_result->>'recovered_stale_count', '') ~ '^[0-9]+$' THEN (v_dirty_priority_result->>'recovered_stale_count')::integer ELSE 0 END > 0
    OR CASE WHEN COALESCE(v_dirty_priority_result->>'requeued', '') ~ '^[0-9]+$' THEN (v_dirty_priority_result->>'requeued')::integer ELSE 0 END > 0;

  SELECT COALESCE(jsonb_agg(DISTINCT dirty_job.value->>'job_id') FILTER (
           WHERE NULLIF(BTRIM(COALESCE(dirty_job.value->>'job_id', '')), '') IS NOT NULL
         ), '[]'::jsonb)
  INTO v_dirty_priority_created_job_ids
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_dirty_priority_result->'job_results') = 'array' THEN v_dirty_priority_result->'job_results' ELSE '[]'::jsonb END) AS dirty_job(value);

  SELECT COALESCE(jsonb_agg(DISTINCT actual_refresh_id.refresh_job_id) FILTER (
           WHERE actual_refresh_id.refresh_job_id IS NOT NULL
         ), '[]'::jsonb)
  INTO v_dirty_priority_actual_refresh_job_ids
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_dirty_priority_result->'job_results') = 'array' THEN v_dirty_priority_result->'job_results' ELSE '[]'::jsonb END) AS dirty_job(value)
  CROSS JOIN LATERAL (
    SELECT COALESCE(
      NULLIF(BTRIM(COALESCE(dirty_job.value #>> '{stage_result,actual_refresh_job_id}', '')), ''),
      NULLIF(BTRIM(COALESCE(dirty_job.value #>> '{stage_result,refresh_enqueue_result,job_id}', '')), ''),
      NULLIF(BTRIM(COALESCE(dirty_job.value #>> '{job_payload_after,actual_refresh_job_id}', '')), ''),
      NULLIF(BTRIM(COALESCE(dirty_job.value #>> '{job_payload_after,refresh_enqueue_result,job_id}', '')), '')
    ) AS refresh_job_id
  ) AS actual_refresh_id;

  v_dirty_priority_claimed_job_id := CASE
    WHEN jsonb_typeof(v_dirty_priority_created_job_ids) = 'array'
         AND jsonb_array_length(v_dirty_priority_created_job_ids) > 0
         AND (v_dirty_priority_created_job_ids->>0) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_dirty_priority_created_job_ids->>0)::uuid
    ELSE NULL::uuid
  END;

  IF v_dirty_priority_made_progress
     AND jsonb_typeof(v_dirty_priority_actual_refresh_job_ids) = 'array'
     AND jsonb_array_length(v_dirty_priority_actual_refresh_job_ids) > 0 THEN
    PERFORM public._temp_diag_log(
      'DIRTY_PRIORITY_ENQUEUED_DELTA_AFTER_NORMAL_CLAIM',
      'TEMP_BANKING_PAY_WORKBENCH',
      COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id),
      jsonb_build_object(
        'function_name', 'pay_workbench_worker_drain_chunk',
        'worker_id', v_worker_id,
        'origin', 'pay_workbench_worker_drain_chunk',
        'route', 'worker_drain',
        'dirty_priority_job_id', CASE WHEN v_dirty_priority_claimed_job_id IS NULL THEN NULL::text ELSE v_dirty_priority_claimed_job_id::text END,
        'dirty_priority_created_job_ids', v_dirty_priority_created_job_ids,
        'dirty_priority_actual_refresh_job_ids', v_dirty_priority_actual_refresh_job_ids,
        'dirty_priority_jobs_processed', v_dirty_priority_jobs_processed
      )
    );
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_TRIGGER_DIRTY_STAGE',
    'TEMP_BANKING_PAY_DIRTY',
    NULL::text,
    jsonb_build_object(
      'function_name', 'pay_workbench_worker_drain_chunk',
      'stage', 'ordinary_worker_start_after_dirty_priority',
      'dirty_priority_jobs_processed', v_dirty_priority_jobs_processed,
      'dirty_priority_jobs_remaining', v_dirty_priority_jobs_remaining,
      'dirty_priority_cap_reached', v_dirty_priority_cap_reached
    )
  );
  -- Keep the original whole-RPC start time; dirty-priority work must count toward the drain wall-clock budget.
  v_phase_started_at_utc := clock_timestamp();

  IF p_allowed_job_types IS NULL THEN
    v_allowed_job_types := v_supported_job_types;
  ELSE
    SELECT COALESCE(
             array_agg(
               DISTINCT normalised_job_type.canonical_job_type
               ORDER BY normalised_job_type.canonical_job_type
             ),
             ARRAY[]::text[]
           )
    INTO v_allowed_job_types
    FROM (
      SELECT CASE
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_SESSION_SCOPE_SEED',
          'SESSION_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED_PAGE',
          'SCOPE_SEED_PAGE'
        ) THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'CANDIDATE_DELTA_REFRESH',
          'DELTA_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_SESSION_CLONE_REBASE',
          'SESSION_CLONE_REBASE',
          'CLONE_REBASE'
        ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
          'CANDIDATE_LINE_WORK_SEED',
          'CANDIDATE_LINE_WORK_SEED_PAGE',
          'LINE_WORK_SEED_PAGE',
          'SNAPSHOT_CANDIDATE_REFRESH',
          'CANDIDATE_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'CANDIDATE_LINE_WORK_PROCESS',
          'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'LINE_WORK_PROCESS',
          'LINE_WORK_PROCESS_CHUNK'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN raw_job_type.job_type IN (
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROWS_MATERIALISE',
          'PREVIEW_ROWS_MATERIALIZE',
          'PREVIEW_ROWS_MATERIALISE_CHUNK',
          'PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROW_MATERIALISE_CHUNK',
          'PREVIEW_ROW_MATERIALIZE_CHUNK'
        ) THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        WHEN raw_job_type.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
          THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
        ELSE NULL::text
      END AS canonical_job_type
      FROM (
        SELECT UPPER(BTRIM(COALESCE(job_type_input.raw_value, ''))) AS job_type
        FROM unnest(p_allowed_job_types) AS job_type_input(raw_value)
      ) AS raw_job_type
    ) AS normalised_job_type
    WHERE normalised_job_type.canonical_job_type = ANY(v_supported_job_types);
  END IF;

  /* pay_workbench_claim_due_jobs owns stale recovery for the five exact
     canonical session-stage job types. Recover only supported aliases and the
     global dirty-fanout type that its stale selector does not include. */
  FOR v_supplemental_stale_row IN
    SELECT
      stale_job.id AS job_id,
      stale_job.job_type,
      normalized_stale_job.canonical_job_type,
      COALESCE(
        stale_job.updated_at_utc,
        stale_job.started_at_utc,
        stale_job.run_at_utc,
        stale_job.created_at_utc
      ) AS last_activity_utc,
      CASE
        WHEN COALESCE(stale_job.payload_json->>'worker_lease_seconds', '')
               ~ '^[0-9]{1,9}$'
          THEN LEAST(
            GREATEST(
              (stale_job.payload_json->>'worker_lease_seconds')::integer,
              25
            ),
            3600
          )
        WHEN v_settings_db_worker_lease_seconds IS NOT NULL
          THEN v_lease_seconds
        WHEN p_session_id IS NOT NULL
             AND normalized_stale_job.canonical_job_type IN (
               'WORKBENCH_SESSION_SCOPE_SEED',
               'WORKBENCH_CANDIDATE_SOURCE_BUILD',
               'WORKBENCH_CANDIDATE_DELTA_REFRESH',
               'WORKBENCH_SESSION_CLONE_REBASE',
               'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
               'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
               'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
             )
          THEN 25
        ELSE 180
      END AS lease_seconds
    FROM public.banking_pay_workbench_jobs AS stale_job
    CROSS JOIN LATERAL (
      SELECT UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) AS raw_job_type
    ) AS raw_stale_job
    CROSS JOIN LATERAL (
      SELECT CASE
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_SESSION_SCOPE_SEED',
          'SESSION_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED_PAGE',
          'SCOPE_SEED_PAGE'
        ) THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'CANDIDATE_DELTA_REFRESH',
          'DELTA_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_SESSION_CLONE_REBASE',
          'SESSION_CLONE_REBASE',
          'CLONE_REBASE'
        ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
          'CANDIDATE_LINE_WORK_SEED',
          'CANDIDATE_LINE_WORK_SEED_PAGE',
          'LINE_WORK_SEED_PAGE',
          'SNAPSHOT_CANDIDATE_REFRESH',
          'CANDIDATE_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'CANDIDATE_LINE_WORK_PROCESS',
          'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'LINE_WORK_PROCESS',
          'LINE_WORK_PROCESS_CHUNK'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN raw_stale_job.raw_job_type IN (
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROWS_MATERIALISE',
          'PREVIEW_ROWS_MATERIALIZE',
          'PREVIEW_ROWS_MATERIALISE_CHUNK',
          'PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROW_MATERIALISE_CHUNK',
          'PREVIEW_ROW_MATERIALIZE_CHUNK'
        ) THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        WHEN raw_stale_job.raw_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
          THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
        ELSE raw_stale_job.raw_job_type
      END AS canonical_job_type
    ) AS normalized_stale_job
    WHERE stale_job.status = 'RUNNING'
      AND raw_stale_job.raw_job_type NOT IN (
        'WORKBENCH_SESSION_SCOPE_SEED',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'WORKBENCH_SESSION_CLONE_REBASE',
        'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
        'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
      )
      AND normalized_stale_job.canonical_job_type = ANY(v_allowed_job_types)
      AND (p_session_id IS NULL OR stale_job.session_id = p_session_id)
      AND (p_candidate_id IS NULL OR stale_job.candidate_id = p_candidate_id)
      AND stale_job.completed_at_utc IS NULL
      AND stale_job.failed_at_utc IS NULL
      AND COALESCE(
            stale_job.updated_at_utc,
            stale_job.started_at_utc,
            stale_job.run_at_utc,
            stale_job.created_at_utc
          )
          + make_interval(
              secs => CASE
                WHEN COALESCE(stale_job.payload_json->>'worker_lease_seconds', '')
                       ~ '^[0-9]{1,9}$'
                  THEN LEAST(
                    GREATEST(
                      (stale_job.payload_json->>'worker_lease_seconds')::integer,
                      25
                    ),
                    3600
                  )
                WHEN v_settings_db_worker_lease_seconds IS NOT NULL
                  THEN v_lease_seconds
                WHEN p_session_id IS NOT NULL
                     AND normalized_stale_job.canonical_job_type IN (
                       'WORKBENCH_SESSION_SCOPE_SEED',
                       'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                       'WORKBENCH_CANDIDATE_DELTA_REFRESH',
                       'WORKBENCH_SESSION_CLONE_REBASE',
                       'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
                       'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
                       'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
                     )
                  THEN 25
                ELSE 180
              END
            ) <= v_cutoff
    ORDER BY
      COALESCE(
        stale_job.updated_at_utc,
        stale_job.started_at_utc,
        stale_job.run_at_utc,
        stale_job.created_at_utc
      ) ASC,
      stale_job.priority ASC,
      stale_job.run_at_utc ASC,
      stale_job.created_at_utc ASC,
      stale_job.id ASC
    LIMIT 1
    FOR UPDATE OF stale_job SKIP LOCKED
  LOOP
    v_supplemental_stale_error_json := jsonb_build_object(
      'code', 'WORKBENCH_SUPPORTED_JOB_STALE_LEASE_EXPIRED',
      'message', 'A supported Banking Pay workbench job lease expired before completion.',
      'job_id', v_supplemental_stale_row.job_id::text,
      'job_type', v_supplemental_stale_row.job_type,
      'canonical_job_type', v_supplemental_stale_row.canonical_job_type,
      'worker_id', v_worker_id,
      'last_activity_utc', v_supplemental_stale_row.last_activity_utc,
      'lease_seconds', v_supplemental_stale_row.lease_seconds,
      'lease_expires_at_utc', (
        v_supplemental_stale_row.last_activity_utc
        + make_interval(secs => v_supplemental_stale_row.lease_seconds)
      ),
      'recovered_at_utc', v_now
    );

    BEGIN
      v_supplemental_stale_fail_result := public.pay_workbench_fail_job(
        p_job_id => v_supplemental_stale_row.job_id,
        p_error_json => v_supplemental_stale_error_json,
        p_retry_after_seconds => 5
      );
      v_supplemental_stale_status := UPPER(BTRIM(COALESCE(
        v_supplemental_stale_fail_result->>'status',
        ''
      )));

      IF v_supplemental_stale_status = 'QUEUED' THEN
        v_supplemental_stale_recovered_count := v_supplemental_stale_recovered_count + 1;
      ELSIF v_supplemental_stale_status IN ('FAILED', 'DEAD') THEN
        v_supplemental_stale_terminal_count := v_supplemental_stale_terminal_count + 1;
      ELSE
        v_supplemental_stale_recovery_error_count := v_supplemental_stale_recovery_error_count + 1;
      END IF;

      v_job_results_json := v_job_results_json || jsonb_build_array(
        jsonb_build_object(
          'job_id', v_supplemental_stale_row.job_id::text,
          'job_type', v_supplemental_stale_row.job_type,
          'canonical_job_type', v_supplemental_stale_row.canonical_job_type,
          'status', NULLIF(v_supplemental_stale_status, ''),
          'stale_recovery', true,
          'last_activity_utc', v_supplemental_stale_row.last_activity_utc,
          'lease_seconds', v_supplemental_stale_row.lease_seconds,
          'error_code', CASE
            WHEN v_supplemental_stale_status IN ('QUEUED', 'FAILED', 'DEAD') THEN NULL::text
            ELSE 'WORKBENCH_SUPPORTED_JOB_STALE_RECOVERY_STATUS_INVALID'
          END
        )
      );
    EXCEPTION WHEN OTHERS THEN
      v_supplemental_stale_recovery_error_count := v_supplemental_stale_recovery_error_count + 1;
      v_job_results_json := v_job_results_json || jsonb_build_array(
        jsonb_build_object(
          'job_id', v_supplemental_stale_row.job_id::text,
          'job_type', v_supplemental_stale_row.job_type,
          'canonical_job_type', v_supplemental_stale_row.canonical_job_type,
          'status', 'STALE_RECOVERY_FAILED',
          'stale_recovery', true,
          'error_code', SQLSTATE,
          'error_message', SQLERRM
        )
      );
    END;
  END LOOP;

  v_supplemental_stale_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_started_at_utc)) * 1000)::integer);
  v_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer);

  IF v_elapsed_ms >= (v_max_runtime_ms - v_min_phase_budget_ms) THEN
    v_claim_result := jsonb_build_object(
      'ok', true,
      'claimed', '[]'::jsonb,
      'claimed_count', 0,
      'recovered_stale_count', 0,
      'dead_stale_count', 0,
      'skipped_claim_due_to_budget', true
    );
    v_claimed_jobs_json := '[]'::jsonb;
    v_claimed_count := 0;
    v_recovered_stale_count := v_supplemental_stale_recovered_count;
    v_dead_stale_count := v_supplemental_stale_terminal_count;
    v_dead_count := v_dead_stale_count;
    v_more_due := true;
    v_stop_reason := 'WORKER_BUDGET_EARLY_STOP';
  ELSE
    v_phase_started_at_utc := clock_timestamp();
    v_claim_phase_started_at := v_phase_started_at_utc;
    -- One worker RPC may claim exactly one normal stage job.  Stage functions
    -- still process their own bounded, set-based p_limit units; concurrency is
    -- not used as the fix for queue safety.
    v_budget_claim_limit := 1;

    BEGIN
      WITH pre_claim_due AS MATERIALIZED (
        SELECT
          pre_job.id,
          pre_job.job_type,
          pre_job.created_at_utc,
          pre_job.run_at_utc
        FROM public.banking_pay_workbench_jobs AS pre_job
        CROSS JOIN LATERAL (
          SELECT CASE
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED','SESSION_SCOPE_SEED','WORKBENCH_SCOPE_SEED','WORKBENCH_SCOPE_SEED_PAGE','SCOPE_SEED_PAGE') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK','WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE','CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH','CANDIDATE_DELTA_REFRESH','DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE','SESSION_CLONE_REBASE','CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED','WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE','CANDIDATE_LINE_WORK_SEED','CANDIDATE_LINE_WORK_SEED_PAGE','LINE_WORK_SEED_PAGE','SNAPSHOT_CANDIDATE_REFRESH','CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK','CANDIDATE_LINE_WORK_PROCESS','CANDIDATE_LINE_WORK_PROCESS_CHUNK','LINE_WORK_PROCESS','LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_PREVIEW_ROWS_MATERIALIZE','WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK','WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROWS_MATERIALISE','PREVIEW_ROWS_MATERIALIZE','PREVIEW_ROWS_MATERIALISE_CHUNK','PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROW_MATERIALISE_CHUNK','PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
            WHEN UPPER(BTRIM(COALESCE(pre_job.job_type, ''))) = 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
            ELSE UPPER(BTRIM(COALESCE(pre_job.job_type, '')))
          END AS canonical_job_type
        ) AS pre_job_type
        WHERE pre_job.status = 'QUEUED'
          AND pre_job.run_at_utc <= GREATEST(v_cutoff, v_now)
          AND (p_session_id IS NULL OR pre_job.session_id = p_session_id)
          AND (p_candidate_id IS NULL OR pre_job.candidate_id = p_candidate_id)
          AND pre_job_type.canonical_job_type = ANY(v_allowed_job_types)
      )
      SELECT
        COUNT(*)::integer,
        COUNT(*)::integer,
        COALESCE(jsonb_agg(jsonb_build_object('job_id', pre_claim_due.id::text, 'job_type', pre_claim_due.job_type, 'created_at_utc', pre_claim_due.created_at_utc, 'run_at_utc', pre_claim_due.run_at_utc) ORDER BY pre_claim_due.created_at_utc, pre_claim_due.id) FILTER (WHERE pre_claim_due.id IN (SELECT id FROM pre_claim_due ORDER BY created_at_utc, id LIMIT 5)), '[]'::jsonb)
      INTO v_pre_claim_due_count,
           v_pre_claim_claimable_count,
           v_pre_claim_job_sample
      FROM pre_claim_due;
    EXCEPTION WHEN OTHERS THEN
      v_pre_claim_due_count := 0;
      v_pre_claim_claimable_count := 0;
      v_pre_claim_job_sample := '[]'::jsonb;
    END;

    v_claim_result := public.pay_workbench_claim_due_jobs(
      p_limit => v_budget_claim_limit,
      p_now_utc => v_cutoff,
      p_session_id => p_session_id,
      p_candidate_id => p_candidate_id,
      p_allowed_job_types => v_allowed_job_types
    );
    v_claim_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_started_at_utc)) * 1000)::integer);
    v_claim_phase_completed_at := clock_timestamp();

    v_claimed_jobs_json := CASE
      WHEN jsonb_typeof(v_claim_result->'claimed') = 'array'
        THEN COALESCE(v_claim_result->'claimed', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    v_claimed_count := COALESCE(jsonb_array_length(v_claimed_jobs_json), 0);
    v_recovered_stale_count := v_supplemental_stale_recovered_count + CASE
      WHEN COALESCE(v_claim_result->>'recovered_stale_count', '') ~ '^[0-9]+$'
        THEN (v_claim_result->>'recovered_stale_count')::integer
      ELSE 0
    END;
    v_dead_stale_count := v_supplemental_stale_terminal_count + CASE
      WHEN COALESCE(v_claim_result->>'dead_stale_count', '') ~ '^[0-9]+$'
        THEN (v_claim_result->>'dead_stale_count')::integer
      ELSE 0
    END;
    v_dead_count := v_dead_stale_count;

    v_claim_lock_contention_detected := LOWER(COALESCE(
      v_claim_result->>'claim_lock_contention_detected',
      v_claim_result->>'lock_contention_detected',
      'false'
    )) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_claim_lock_contention_count := CASE
      WHEN COALESCE(v_claim_result->>'claim_lock_contention_count', '') ~ '^[0-9]+$'
        THEN (v_claim_result->>'claim_lock_contention_count')::integer
      WHEN COALESCE(v_claim_result->>'lock_contention_count', '') ~ '^[0-9]+$'
        THEN (v_claim_result->>'lock_contention_count')::integer
      ELSE 0
    END;
    v_claim_lock_contention_sample := CASE
      WHEN jsonb_typeof(v_claim_result->'claim_lock_contention_sample') = 'array'
        THEN COALESCE(v_claim_result->'claim_lock_contention_sample', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;
    v_claim_mismatch_json := COALESCE(v_claim_result->'claim_mismatch_json', '{}'::jsonb);
    v_claim_mismatch_detected := LOWER(COALESCE(
      v_claim_result->>'claim_mismatch_detected',
      v_claim_mismatch_json->>'claim_mismatch_detected',
      'false'
    )) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_claim_mismatch_reason := NULLIF(BTRIM(COALESCE(
      v_claim_result->>'claim_mismatch_reason',
      v_claim_mismatch_json->>'claim_mismatch_reason',
      ''
    )), '');
  END IF;

  FOR v_claimed_job_json IN
    SELECT claimed_job.value
    FROM jsonb_array_elements(v_claimed_jobs_json) AS claimed_job(value)
  LOOP
    v_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer);
    IF v_elapsed_ms >= (v_max_runtime_ms - v_min_phase_budget_ms) THEN
      WITH claimed_unprocessed_jobs AS (
        SELECT (claimed_unprocessed_job.value->>'job_id')::uuid AS job_id
        FROM jsonb_array_elements(v_claimed_jobs_json) AS claimed_unprocessed_job(value)
        WHERE BTRIM(COALESCE(claimed_unprocessed_job.value->>'job_id', ''))
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND NOT (
            (claimed_unprocessed_job.value->>'job_id')::uuid
              = ANY(COALESCE(v_processed_claimed_job_ids, ARRAY[]::uuid[]))
          )
      ),
      requeued_unprocessed_jobs AS (
        UPDATE public.banking_pay_workbench_jobs AS unprocessed_job
        SET status = 'QUEUED',
            attempt_count = GREATEST(COALESCE(unprocessed_job.attempt_count, 1) - 1, 0),
            run_at_utc = LEAST(COALESCE(unprocessed_job.run_at_utc, v_cutoff), v_cutoff),
            started_at_utc = NULL::timestamptz,
            completed_at_utc = NULL::timestamptz,
            failed_at_utc = NULL::timestamptz,
            last_error_json = jsonb_build_object(
              'code', 'WORKBENCH_CLAIM_RELEASED_DUE_TO_WORKER_BUDGET',
              'message', 'Claimed Banking Pay workbench job was released without processing because the worker was near its runtime budget.',
              'worker_id', v_worker_id,
              'released_at_utc', v_now::text,
              'stop_reason', 'WORKER_BUDGET_EARLY_STOP'
            ),
            payload_json = jsonb_strip_nulls(
              (COALESCE(unprocessed_job.payload_json, '{}'::jsonb) - ARRAY[
                'worker_id',
                'worker_claimed_at_utc',
                'worker_lease_seconds',
                'worker_lease_expires_at_utc',
                'worker_function'
              ]::text[])
              || jsonb_build_object(
                'claim_released_due_to_worker_budget', true,
                'claim_released_at_utc', v_now::text,
                'claim_released_by_worker_id', v_worker_id,
                'claim_release_stop_reason', 'WORKER_BUDGET_EARLY_STOP',
                'claim_release_cleared_worker_lease_payload', true
              )
            ),
            updated_at_utc = v_now
        FROM claimed_unprocessed_jobs AS claimed_unprocessed_job
        WHERE unprocessed_job.id = claimed_unprocessed_job.job_id
          AND unprocessed_job.status = 'RUNNING'
          AND unprocessed_job.completed_at_utc IS NULL
          AND unprocessed_job.failed_at_utc IS NULL
        RETURNING unprocessed_job.id
      )
      SELECT COUNT(*)::integer
      INTO v_requeued_unprocessed_claimed_count
      FROM requeued_unprocessed_jobs;

      v_more_due := true;
      v_stop_reason := 'WORKER_BUDGET_EARLY_STOP';
      EXIT;
    END IF;

    v_job_row := NULL;
    v_session_row := NULL;
    v_job_id := NULL::uuid;
    v_raw_job_type := '';
    v_canonical_job_type := '';
    v_original_job_type := '';
    v_job_type_normalized := false;
    v_payload_json := '{}'::jsonb;
    v_cursor_json := NULL::jsonb;
    v_job_limit_text := NULL::text;
    v_job_limit := 100;
    v_payload_session_version := NULL::bigint;
    v_source_change_seq := NULL::bigint;
    v_live_change_seq := 0;
    v_scope_exists := false;
    v_is_obsolete := false;
    v_obsolete_reason := NULL::text;
    v_obsolete_result := '{}'::jsonb;
    v_stage_result := '{}'::jsonb;
    v_completion_result := '{}'::jsonb;
    v_fail_result := '{}'::jsonb;
    v_error_json := '{}'::jsonb;
    v_failure_sqlstate := NULL::text;
    v_failure_message := NULL::text;
    v_failure_detail := NULL::text;
    v_failure_hint := NULL::text;
    v_failure_context := NULL::text;
    v_retry_after_seconds := 30;
    v_final_failure_status := 'FAILED';
    v_completion_continuation_count := 0;
    v_completion_continuation_reused_count := 0;
    v_fanout_continuation_job_id := NULL::uuid;
    v_fanout_continuation_created := false;
    v_fanout_continuation_dedupe_key := NULL::text;
    v_fanout_cursor_token := NULL::text;
    v_fanout_scope_kind := NULL::text;
    v_fanout_scope_id := NULL::text;
    v_next_cursor_json := NULL::jsonb;
    v_continuation_payload_json := '{}'::jsonb;

    IF BTRIM(COALESCE(v_claimed_job_json->>'job_id', ''))
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_processed_count := v_processed_count + 1;
      v_failed_count := v_failed_count + 1;
      v_job_results_json := v_job_results_json || jsonb_build_array(
        jsonb_build_object(
          'job_id', NULL::text,
          'status', 'INVALID_CLAIM_RESULT',
          'error_code', 'PAY_WORKBENCH_WORKER_CLAIMED_JOB_ID_INVALID'
        )
      );
      CONTINUE;
    END IF;

    v_job_id := (v_claimed_job_json->>'job_id')::uuid;

    BEGIN
      SELECT current_job.*
      INTO v_job_row
      FROM public.banking_pay_workbench_jobs AS current_job
      WHERE current_job.id = v_job_id
      FOR UPDATE;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_CLAIMED_JOB_NOT_FOUND'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_WORKER_CLAIMED_JOB_NOT_FOUND',
                  'job_id', v_job_id::text
                )::text;
      END IF;

      IF UPPER(BTRIM(COALESCE(v_job_row.status, ''))) <> 'RUNNING' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_CLAIMED_JOB_NOT_RUNNING'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_WORKER_CLAIMED_JOB_NOT_RUNNING',
                  'job_id', v_job_row.id::text,
                  'status', v_job_row.status
                )::text;
      END IF;

      v_payload_json := CASE
        WHEN jsonb_typeof(COALESCE(v_job_row.payload_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(v_job_row.payload_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END;

      UPDATE public.banking_pay_workbench_jobs AS worker_claim_update
      SET payload_json = v_payload_json || jsonb_build_object(
            'worker_id', v_worker_id,
            'worker_claimed_at_utc', v_now::text,
            'worker_lease_seconds', v_lease_seconds,
            'worker_lease_expires_at_utc', (
              v_now + make_interval(secs => v_lease_seconds)
            )::text,
            'worker_function', 'pay_workbench_worker_drain_chunk'
          ),
          updated_at_utc = v_now
      WHERE worker_claim_update.id = v_job_row.id
      RETURNING worker_claim_update.*
      INTO v_job_row;

      v_payload_json := COALESCE(v_job_row.payload_json, '{}'::jsonb);
      v_original_job_type := COALESCE(v_job_row.job_type, '');
      v_raw_job_type := UPPER(BTRIM(v_original_job_type));
      v_canonical_job_type := CASE
        WHEN v_raw_job_type IN (
          'WORKBENCH_SESSION_SCOPE_SEED',
          'SESSION_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED_PAGE',
          'SCOPE_SEED_PAGE'
        ) THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN v_raw_job_type IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        WHEN v_raw_job_type IN (
          'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'CANDIDATE_DELTA_REFRESH',
          'DELTA_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN v_raw_job_type IN (
          'WORKBENCH_SESSION_CLONE_REBASE',
          'SESSION_CLONE_REBASE',
          'CLONE_REBASE'
        ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
        WHEN v_raw_job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
          'CANDIDATE_LINE_WORK_SEED',
          'CANDIDATE_LINE_WORK_SEED_PAGE',
          'LINE_WORK_SEED_PAGE'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN v_raw_job_type IN ('SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
          THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN v_raw_job_type IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'CANDIDATE_LINE_WORK_PROCESS',
          'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'LINE_WORK_PROCESS',
          'LINE_WORK_PROCESS_CHUNK'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN v_raw_job_type IN (
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROWS_MATERIALISE',
          'PREVIEW_ROWS_MATERIALIZE',
          'PREVIEW_ROWS_MATERIALISE_CHUNK',
          'PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROW_MATERIALISE_CHUNK',
          'PREVIEW_ROW_MATERIALIZE_CHUNK'
        ) THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        WHEN v_raw_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
          THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
        ELSE v_raw_job_type
      END;

      IF v_raw_job_type IN ('SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
         AND NOT (
           LOWER(BTRIM(COALESCE(v_payload_json->>'line_work_only', 'false')))
             IN ('true', 't', '1', 'yes', 'y', 'on')
           OR LOWER(BTRIM(COALESCE(v_payload_json->>'line_work_required', 'false')))
             IN ('true', 't', '1', 'yes', 'y', 'on')
           OR UPPER(BTRIM(COALESCE(v_payload_json->>'line_work_action', ''))) = 'SEED'
         ) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_LEGACY_REFRESH_NOT_LINE_WORK_ONLY'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'WORKBENCH_LEGACY_REFRESH_NOT_LINE_WORK_ONLY',
                  'job_id', v_job_row.id::text,
                  'job_type', v_job_row.job_type,
                  'session_id', CASE
                    WHEN v_job_row.session_id IS NULL THEN NULL::text
                    ELSE v_job_row.session_id::text
                  END,
                  'candidate_id', CASE
                    WHEN v_job_row.candidate_id IS NULL THEN NULL::text
                    ELSE v_job_row.candidate_id::text
                  END
                )::text;
      END IF;

      IF v_canonical_job_type = ANY(v_supported_job_types)
         AND v_raw_job_type IS DISTINCT FROM v_canonical_job_type THEN
        UPDATE public.banking_pay_workbench_jobs AS normalized_job
        SET job_type = v_canonical_job_type,
            payload_json = COALESCE(normalized_job.payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'original_job_type', COALESCE(
                  NULLIF(BTRIM(COALESCE(normalized_job.payload_json->>'original_job_type', '')), ''),
                  NULLIF(BTRIM(v_original_job_type), ''),
                  v_raw_job_type
                ),
                'job_type', v_canonical_job_type,
                'canonical_job_type', v_canonical_job_type,
                'job_type_normalized_by_worker', true,
                'job_type_normalized_at_utc', v_now::text
              ),
            updated_at_utc = v_now
        WHERE normalized_job.id = v_job_row.id
        RETURNING normalized_job.*
        INTO v_job_row;

        v_payload_json := COALESCE(v_job_row.payload_json, '{}'::jsonb);
        v_job_type_normalized := true;
      END IF;

      v_cursor_json := CASE
        WHEN jsonb_typeof(v_payload_json->'cursor_json') = 'object'
          THEN v_payload_json->'cursor_json'
        WHEN jsonb_typeof(v_payload_json->'cursor') = 'object'
          THEN v_payload_json->'cursor'
        WHEN jsonb_typeof(v_payload_json->'next_cursor_json') = 'object'
          THEN v_payload_json->'next_cursor_json'
        WHEN jsonb_typeof(v_payload_json->'next_cursor') = 'object'
          THEN v_payload_json->'next_cursor'
        ELSE NULL::jsonb
      END;

      v_job_limit_text := COALESCE(
        NULLIF(BTRIM(COALESCE(v_payload_json->>'limit', '')), ''),
        NULLIF(BTRIM(COALESCE(v_payload_json->>'p_limit', '')), ''),
        CASE
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN NULLIF(BTRIM(COALESCE(
            v_payload_json->>'source_build_limit',
            v_payload_json->>'source_page_limit',
            v_payload_json#>>'{source_build,limit}',
            v_payload_json#>>'{source_build,source_page_limit}',
            v_payload_json#>>'{stage_limits,source_build}',
            v_payload_json#>>'{limits,source_build}',
            ''
          )), '')
          ELSE NULL::text
        END,
        CASE
          WHEN v_canonical_job_type = 'WORKBENCH_SESSION_SCOPE_SEED' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,scope_seed}', v_payload_json#>>'{limits,scope_seed}', '')), '')
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,delta_refresh}', v_payload_json#>>'{limits,delta_refresh}', v_payload_json->>'delta_refresh_limit', '')), '')
          WHEN v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,clone_rebase}', v_payload_json#>>'{limits,clone_rebase}', v_payload_json->>'clone_rebase_limit', v_payload_json->>'line_limit', '')), '')
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,line_seed}', v_payload_json#>>'{limits,line_seed}', '')), '')
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,line_process}', v_payload_json#>>'{limits,line_process}', '')), '')
          WHEN v_canonical_job_type = 'WORKBENCH_PREVIEW_ROWS_MATERIALISE' THEN NULLIF(BTRIM(COALESCE(v_payload_json#>>'{stage_limits,preview_materialise}', v_payload_json#>>'{limits,preview_materialise}', '')), '')
          ELSE NULL::text
        END,
        NULLIF(BTRIM(COALESCE(v_payload_json->>'line_limit', '')), ''),
        NULLIF(BTRIM(COALESCE(v_payload_json->>'page_limit', '')), ''),
        NULLIF(BTRIM(COALESCE(v_payload_json->>'chunk_size', '')), '')
      );

      v_stage_limit_for_job := CASE
        WHEN v_canonical_job_type = 'WORKBENCH_SESSION_SCOPE_SEED' THEN v_scope_seed_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_source_build_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_refresh_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN v_clone_rebase_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN v_line_seed_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN v_line_process_work_units_per_job
        WHEN v_canonical_job_type = 'WORKBENCH_PREVIEW_ROWS_MATERIALISE' THEN v_preview_materialise_work_units_per_job
        ELSE v_stage_work_units_per_job
      END;

      v_job_limit := LEAST(
        GREATEST(
          CASE
            WHEN COALESCE(v_job_limit_text, '') ~ '^[0-9]{1,9}$'
              THEN v_job_limit_text::integer
            ELSE v_stage_limit_for_job
          END,
          1
        ),
        v_stage_limit_for_job
      );

      IF v_canonical_job_type = ANY(ARRAY[
           'WORKBENCH_SESSION_SCOPE_SEED',
           'WORKBENCH_CANDIDATE_SOURCE_BUILD',
           'WORKBENCH_CANDIDATE_DELTA_REFRESH',
           'WORKBENCH_SESSION_CLONE_REBASE',
           'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
           'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
           'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
         ]::text[]) THEN
        IF v_job_row.session_id IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_SESSION_CONTEXT_REQUIRED'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_WORKER_SESSION_CONTEXT_REQUIRED',
                    'job_id', v_job_row.id::text,
                    'job_type', v_job_row.job_type,
                    'canonical_job_type', v_canonical_job_type
                  )::text;
        END IF;

        SELECT workbench_session.*
        INTO v_session_row
        FROM public.banking_pay_workbench_sessions AS workbench_session
        WHERE workbench_session.id = v_job_row.session_id;

        IF NOT FOUND THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SESSION_NOT_FOUND';
        ELSIF v_session_row.status <> 'OPEN'
              OR v_session_row.discarded_at_utc IS NOT NULL THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SESSION_NOT_OPEN';
        ELSIF v_job_row.snapshot_run_id IS NOT NULL
              AND v_job_row.snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SESSION_SNAPSHOT_MISMATCH';
        END IF;

        IF v_is_obsolete IS NOT TRUE
           AND NULLIF(BTRIM(COALESCE(v_payload_json->>'session_version', '')), '') IS NOT NULL THEN
          IF COALESCE(v_payload_json->>'session_version', '') !~ '^[0-9]{1,18}$' THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_SESSION_VERSION_INVALID'
              USING ERRCODE = 'P0001',
                    DETAIL = jsonb_build_object(
                      'code', 'PAY_WORKBENCH_WORKER_SESSION_VERSION_INVALID',
                      'job_id', v_job_row.id::text,
                      'session_id', v_job_row.session_id::text,
                      'session_version', v_payload_json->>'session_version'
                    )::text;
          END IF;

          v_payload_session_version := (v_payload_json->>'session_version')::bigint;

          IF v_payload_session_version IS DISTINCT FROM v_session_row.version THEN
            v_is_obsolete := true;
            v_obsolete_reason := 'SESSION_VERSION_STALE';
          END IF;
        END IF;

        IF v_is_obsolete IS NOT TRUE
           AND v_job_row.candidate_id IS NOT NULL
           AND v_canonical_job_type <> 'WORKBENCH_SESSION_SCOPE_SEED' THEN
          SELECT EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_session_scope AS scope_row
            WHERE scope_row.session_id = v_job_row.session_id
              AND scope_row.candidate_id = v_job_row.candidate_id
          )
          INTO v_scope_exists;

          IF COALESCE(v_scope_exists, false) IS NOT TRUE THEN
            v_is_obsolete := true;
            v_obsolete_reason := 'CANDIDATE_NOT_IN_SESSION_SCOPE';
          END IF;
        END IF;

        IF v_is_obsolete IS NOT TRUE
           AND v_canonical_job_type IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_DELTA_REFRESH', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE')
           AND v_job_row.candidate_id IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_CANDIDATE_CONTEXT_REQUIRED'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_WORKER_CANDIDATE_CONTEXT_REQUIRED',
                    'job_id', v_job_row.id::text,
                    'session_id', v_job_row.session_id::text,
                    'job_type', v_job_row.job_type,
                    'canonical_job_type', v_canonical_job_type
                  )::text;
        END IF;

      END IF;

      IF v_is_obsolete IS NOT TRUE
         AND v_job_row.candidate_id IS NOT NULL
         AND v_canonical_job_type = ANY(ARRAY[
           'WORKBENCH_CANDIDATE_SOURCE_BUILD',
           'WORKBENCH_CANDIDATE_DELTA_REFRESH',
           'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
           'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
           'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
         ]::text[])
         AND NULLIF(BTRIM(COALESCE(v_payload_json->>'source_change_seq', '')), '') IS NOT NULL THEN
        IF COALESCE(v_payload_json->>'source_change_seq', '') !~ '^[0-9]{1,18}$' THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_SOURCE_CHANGE_SEQ_INVALID'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_WORKER_SOURCE_CHANGE_SEQ_INVALID',
                    'job_id', v_job_row.id::text,
                    'candidate_id', v_job_row.candidate_id::text,
                    'source_change_seq', v_payload_json->>'source_change_seq'
                  )::text;
        END IF;

        v_source_change_seq := (v_payload_json->>'source_change_seq')::bigint;

        SELECT COALESCE(candidate_counter.seq, 0)
        INTO v_live_change_seq
        FROM public.app_change_counters AS candidate_counter
        WHERE candidate_counter.entity_key = 'pay_candidate:' || v_job_row.candidate_id::text;

        IF COALESCE(v_live_change_seq, 0) > COALESCE(v_source_change_seq, 0) THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'CANDIDATE_SOURCE_CHANGE_SEQ_STALE';
        END IF;
      END IF;

      IF v_is_obsolete THEN
        v_obsolete_result := jsonb_build_object(
          'ok', true,
          'job_id', v_job_row.id::text,
          'job_type', v_job_row.job_type,
          'original_job_type', NULLIF(BTRIM(v_original_job_type), ''),
          'canonical_job_type', v_canonical_job_type,
          'job_type_normalized', v_job_type_normalized,
          'obsolete_skip', true,
          'obsolete_reason', v_obsolete_reason,
          'session_id', CASE
            WHEN v_job_row.session_id IS NULL THEN NULL::text
            ELSE v_job_row.session_id::text
          END,
          'candidate_id', CASE
            WHEN v_job_row.candidate_id IS NULL THEN NULL::text
            ELSE v_job_row.candidate_id::text
          END,
          'worker_id', v_worker_id,
          'skipped_at_utc', v_now
        );

        v_obsolete_projection_run_id := NULL::uuid;
        v_projection_status_before := NULL::text;
        v_projection_status_after := NULL::text;
        v_active_continuation_count := 0;
        v_active_continuation_job_ids := ARRAY[]::uuid[];
        v_active_continuation_job_ids_json := '[]'::jsonb;
        v_delta_projection_diag_json := '{}'::jsonb;
        v_obsolete_projection_update_count := 0;

        IF v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
          IF COALESCE(
               NULLIF(BTRIM(COALESCE(v_payload_json->>'projection_run_id', '')), ''),
               NULLIF(BTRIM(COALESCE(v_payload_json #>> '{cursor,projection_run_id}', '')), ''),
               NULLIF(BTRIM(COALESCE(v_payload_json #>> '{cursor_json,projection_run_id}', '')), ''),
               NULLIF(BTRIM(COALESCE(v_cursor_json->>'projection_run_id', '')), ''),
               NULLIF(BTRIM(COALESCE(v_payload_json #>> '{result_json,projection_run_id}', '')), ''),
               NULLIF(BTRIM(COALESCE(v_payload_json #>> '{result_json,next_cursor,projection_run_id}', '')), ''),
               ''
             ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            v_obsolete_projection_run_id := COALESCE(
              NULLIF(BTRIM(COALESCE(v_payload_json->>'projection_run_id', '')), ''),
              NULLIF(BTRIM(COALESCE(v_payload_json #>> '{cursor,projection_run_id}', '')), ''),
              NULLIF(BTRIM(COALESCE(v_payload_json #>> '{cursor_json,projection_run_id}', '')), ''),
              NULLIF(BTRIM(COALESCE(v_cursor_json->>'projection_run_id', '')), ''),
              NULLIF(BTRIM(COALESCE(v_payload_json #>> '{result_json,projection_run_id}', '')), ''),
              NULLIF(BTRIM(COALESCE(v_payload_json #>> '{result_json,next_cursor,projection_run_id}', '')), '')
            )::uuid;
          END IF;

          IF v_obsolete_projection_run_id IS NOT NULL THEN
            SELECT projection_run.status
            INTO v_projection_status_before
            FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
            WHERE projection_run.id = v_obsolete_projection_run_id
            FOR UPDATE;

            SELECT COALESCE(COUNT(*), 0)::integer,
                   COALESCE(array_agg(active_job.id ORDER BY active_job.created_at_utc, active_job.id), ARRAY[]::uuid[])
            INTO v_active_continuation_count,
                 v_active_continuation_job_ids
            FROM public.banking_pay_workbench_jobs AS active_job
            WHERE active_job.id <> v_job_row.id
              AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
              AND (v_job_row.session_id IS NULL OR active_job.session_id = v_job_row.session_id)
              AND (v_job_row.candidate_id IS NULL OR active_job.candidate_id = v_job_row.candidate_id)
              AND (
                   active_job.payload_json->>'projection_run_id' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{cursor,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{cursor_json,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{next_cursor,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{next_cursor_json,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{result_json,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json #>> '{result_json,next_cursor,projection_run_id}' = v_obsolete_projection_run_id::text
                OR active_job.payload_json::text ILIKE '%' || v_obsolete_projection_run_id::text || '%'
              );

            v_active_continuation_job_ids_json := COALESCE(to_jsonb(v_active_continuation_job_ids), '[]'::jsonb);
            v_delta_projection_diag_json := jsonb_strip_nulls(
              jsonb_build_object(
                'projection_run_id', v_obsolete_projection_run_id::text,
                'source_job_id', NULLIF(BTRIM(COALESCE(v_payload_json->>'source_job_id', '')), ''),
                'continuation_job_id', v_job_row.id::text,
                'obsolete_reason', v_obsolete_reason,
                'projection_status_before', v_projection_status_before,
                'active_continuation_count', COALESCE(v_active_continuation_count, 0),
                'active_continuation_job_ids', v_active_continuation_job_ids_json,
                'candidate_id', CASE WHEN v_job_row.candidate_id IS NULL THEN NULL::text ELSE v_job_row.candidate_id::text END,
                'session_id', CASE WHEN v_job_row.session_id IS NULL THEN NULL::text ELSE v_job_row.session_id::text END
              )
              || jsonb_build_object(
                'source_change_seq', v_source_change_seq,
                'latest_source_change_seq', v_live_change_seq,
                'terminalisation_reason', CASE WHEN COALESCE(v_active_continuation_count, 0) = 0 THEN 'NO_ACTIVE_CONTINUATION_REMAINING' ELSE 'ACTIVE_CONTINUATION_REMAINING' END,
                'diagnostic_reason', 'OBSOLETE_CONTINUATION_TERMINALISED_PROJECTION'
              )
            );

            PERFORM public._temp_diag_log(
              'DELTA_PROJECTION_OBSOLETE_SKIP_WITH_ACTIVE_STATE',
              'TEMP_BANKING_PAY_WORKBENCH',
              v_obsolete_projection_run_id::text,
              v_delta_projection_diag_json
            );

            IF UPPER(BTRIM(COALESCE(v_projection_status_before, ''))) IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS')
               AND COALESCE(v_active_continuation_count, 0) = 0 THEN
              UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
              SET status = 'FAILED',
                  fallback_required = false,
                  fallback_reason = 'OBSOLETE_CONTINUATION_TERMINALISED_PROJECTION',
                  diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
                    || jsonb_build_object(
                      'obsolete_continuation_terminalised_projection', true,
                      'obsolete_reason', v_obsolete_reason,
                      'continuation_job_id', v_job_row.id::text,
                      'source_job_id', NULLIF(BTRIM(COALESCE(v_payload_json->>'source_job_id', '')), ''),
                      'active_continuation_count', COALESCE(v_active_continuation_count, 0),
                      'terminalisation_reason', 'NO_ACTIVE_CONTINUATION_REMAINING',
                      'terminalised_by', 'pay_workbench_worker_drain_chunk',
                      'terminalised_at_utc', v_now::text
                    ),
                  updated_at_utc = v_now,
                  completed_at_utc = v_now
              WHERE projection_run_update.id = v_obsolete_projection_run_id
                AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS');

              GET DIAGNOSTICS v_obsolete_projection_update_count = ROW_COUNT;
              IF COALESCE(v_obsolete_projection_update_count, 0) > 0 THEN
                v_projection_terminalisation_count := v_projection_terminalisation_count + 1;
                v_terminalisation_count := v_terminalisation_count + 1;
              END IF;
              v_projection_status_after := CASE WHEN COALESCE(v_obsolete_projection_update_count, 0) > 0 THEN 'FAILED' ELSE v_projection_status_before END;
              v_delta_projection_diag_json := v_delta_projection_diag_json || jsonb_build_object(
                'projection_status_after', v_projection_status_after,
                'terminalised_count', COALESCE(v_obsolete_projection_update_count, 0)
              );

              PERFORM public._temp_diag_log(
                'DELTA_PROJECTION_TERMINALISED_ON_OBSOLETE_CONTINUATION',
                'TEMP_BANKING_PAY_WORKBENCH',
                v_obsolete_projection_run_id::text,
                v_delta_projection_diag_json
              );
            ELSE
              v_projection_status_after := v_projection_status_before;
              v_delta_projection_diag_json := v_delta_projection_diag_json || jsonb_build_object(
                'projection_status_after', v_projection_status_after,
                'terminalised_count', 0
              );
              PERFORM public._temp_diag_log(
                'DELTA_PROJECTION_OBSOLETE_SKIP_LEFT_ACTIVE_CONTINUATION',
                'TEMP_BANKING_PAY_WORKBENCH',
                v_obsolete_projection_run_id::text,
                v_delta_projection_diag_json
              );
            END IF;

            v_obsolete_result := v_obsolete_result || jsonb_build_object(
              'projection_run_id', v_obsolete_projection_run_id::text,
              'projection_status_before', v_projection_status_before,
              'projection_status_after', v_projection_status_after,
              'active_continuation_count', COALESCE(v_active_continuation_count, 0),
              'active_continuation_job_ids', v_active_continuation_job_ids_json,
              'projection_terminalisation_count', COALESCE(v_obsolete_projection_update_count, 0)
            );
          END IF;
        END IF;

        UPDATE public.banking_pay_workbench_jobs AS obsolete_job
        SET status = 'SUCCEEDED',
            completed_at_utc = v_now,
            failed_at_utc = NULL::timestamptz,
            last_error_json = NULL::jsonb,
            payload_json = COALESCE(obsolete_job.payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'result_json', v_obsolete_result,
                'completion_json', jsonb_build_object(
                  'obsolete_skip', true,
                  'obsolete_reason', v_obsolete_reason,
                  'continuation_enqueued', false,
                  'continuation_count', 0,
                  'completed_at_utc', v_now::text
                )
              ),
            updated_at_utc = v_now
        WHERE obsolete_job.id = v_job_row.id;

        PERFORM public._audit_insert(
          'banking_pay_workbench_job',
          v_job_row.id::text,
          'SUCCEEDED',
          NULL::jsonb,
          v_obsolete_result,
          'WORKBENCH_JOB_OBSOLETE_SKIPPED',
          NULL::uuid
        );

        IF v_job_row.id IS NOT NULL
           AND NOT (v_job_row.id = ANY(COALESCE(v_processed_claimed_job_ids, ARRAY[]::uuid[]))) THEN
          v_processed_claimed_job_ids := array_append(v_processed_claimed_job_ids, v_job_row.id);
        END IF;

        v_processed_count := v_processed_count + 1;
        v_obsolete_skipped_count := v_obsolete_skipped_count + 1;
        v_job_results_json := v_job_results_json || jsonb_build_array(
          jsonb_build_object(
            'job_id', v_job_row.id::text,
            'job_type', v_job_row.job_type,
            'original_job_type', NULLIF(BTRIM(v_original_job_type), ''),
            'canonical_job_type', v_canonical_job_type,
            'job_type_normalized', v_job_type_normalized,
            'status', 'SUCCEEDED',
            'obsolete_skip', true,
            'obsolete_reason', v_obsolete_reason,
            'projection_run_id', CASE WHEN v_obsolete_projection_run_id IS NULL THEN NULL::text ELSE v_obsolete_projection_run_id::text END,
            'projection_status_before', v_projection_status_before,
            'projection_status_after', v_projection_status_after,
            'projection_terminalisation_count', COALESCE(v_obsolete_projection_update_count, 0)
          )
        );
      ELSE
        v_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer);
        IF v_elapsed_ms >= (v_max_runtime_ms - v_min_phase_budget_ms) THEN
          WITH claimed_unprocessed_jobs AS (
            SELECT (claimed_unprocessed_job.value->>'job_id')::uuid AS job_id
            FROM jsonb_array_elements(v_claimed_jobs_json) AS claimed_unprocessed_job(value)
            WHERE BTRIM(COALESCE(claimed_unprocessed_job.value->>'job_id', ''))
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              AND NOT (
                (claimed_unprocessed_job.value->>'job_id')::uuid
                  = ANY(COALESCE(v_processed_claimed_job_ids, ARRAY[]::uuid[]))
              )
          ),
          requeued_unprocessed_jobs AS (
            UPDATE public.banking_pay_workbench_jobs AS unprocessed_job
            SET status = 'QUEUED',
                attempt_count = GREATEST(COALESCE(unprocessed_job.attempt_count, 1) - 1, 0),
                run_at_utc = LEAST(COALESCE(unprocessed_job.run_at_utc, v_cutoff), v_cutoff),
                started_at_utc = NULL::timestamptz,
                completed_at_utc = NULL::timestamptz,
                failed_at_utc = NULL::timestamptz,
                last_error_json = jsonb_build_object(
                  'code', 'WORKBENCH_CLAIM_RELEASED_BEFORE_STAGE_DUE_TO_WORKER_BUDGET',
                  'message', 'Claimed Banking Pay workbench job was released before starting a stage because the worker was near its runtime budget.',
                  'worker_id', v_worker_id,
                  'released_at_utc', v_now::text,
                  'stop_reason', 'WORKER_BUDGET_EARLY_STOP'
                ),
                payload_json = jsonb_strip_nulls(
                  (COALESCE(unprocessed_job.payload_json, '{}'::jsonb) - ARRAY[
                    'worker_id',
                    'worker_claimed_at_utc',
                    'worker_lease_seconds',
                    'worker_lease_expires_at_utc',
                    'worker_function'
                  ]::text[])
                  || jsonb_build_object(
                    'claim_released_before_stage_due_to_worker_budget', true,
                    'claim_released_at_utc', v_now::text,
                    'claim_released_by_worker_id', v_worker_id,
                    'claim_release_stop_reason', 'WORKER_BUDGET_EARLY_STOP',
                    'claim_release_cleared_worker_lease_payload', true
                  )
                ),
                updated_at_utc = v_now
            FROM claimed_unprocessed_jobs AS claimed_unprocessed_job
            WHERE unprocessed_job.id = claimed_unprocessed_job.job_id
              AND unprocessed_job.status = 'RUNNING'
              AND unprocessed_job.completed_at_utc IS NULL
              AND unprocessed_job.failed_at_utc IS NULL
            RETURNING unprocessed_job.id
          )
          SELECT COUNT(*)::integer
          INTO v_requeued_unprocessed_claimed_count
          FROM requeued_unprocessed_jobs;

          v_more_due := true;
          v_stop_reason := 'WORKER_BUDGET_EARLY_STOP';
          EXIT;
        END IF;

        IF v_canonical_job_type = 'WORKBENCH_SESSION_SCOPE_SEED' THEN
          v_stage_result := public.pay_workbench_session_seed_scope_chunk(
            p_session_id => v_job_row.session_id,
            p_cursor_json => v_cursor_json,
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
          v_stage_result := public.pay_workbench_candidate_source_build_chunk(
            p_session_id => v_job_row.session_id,
            p_candidate_id => v_job_row.candidate_id,
            p_cursor_json => v_cursor_json,
            p_payload_json => v_payload_json,
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
          v_stage_result := public.pay_workbench_candidate_delta_refresh_chunk(
            p_session_id => v_job_row.session_id,
            p_candidate_id => v_job_row.candidate_id,
            p_payload_json => v_payload_json,
            p_cursor_json => COALESCE(v_job_row.payload_json->'cursor_json', '{}'::jsonb),
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
          v_stage_result := public.pay_workbench_session_clone_eligible_rows_v1(
            p_target_session_id => v_job_row.session_id,
            p_source_session_id => CASE
              WHEN NULLIF(BTRIM(COALESCE(v_payload_json->>'source_session_id', v_payload_json->>'clone_from_session_id', '')), '')
                   ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'source_session_id', v_payload_json->>'clone_from_session_id', '')), '')::uuid
              ELSE NULL::uuid
            END,
            p_limit => v_job_limit,
            p_cursor_json => COALESCE(v_cursor_json, '{}'::jsonb),
            p_options_json => jsonb_strip_nulls(
              (COALESCE(v_payload_json, '{}'::jsonb) - ARRAY[
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
                'next_cursor',
                'source_rows',
                'line_rows',
                'preview_rows',
                'projection_rows',
                'row_payload_json',
                'source_row_json',
                'work_payload_json',
                'result_row_json',
                'preview_row_json'
              ]::text[])
              || jsonb_build_object(
                'source_job_id', v_job_row.id::text,
                'target_session_id', v_job_row.session_id::text,
                'session_version', COALESCE(v_session_row.version, 0),
                'clone_rebase_worker', true
              )
            )
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' THEN
          v_stage_result := public.pay_workbench_candidate_line_work_seed(
            p_session_id => v_job_row.session_id,
            p_candidate_id => v_job_row.candidate_id,
            p_cursor_json => v_cursor_json,
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN
          v_stage_result := public.pay_workbench_candidate_line_work_process_chunk(
            p_session_id => v_job_row.session_id,
            p_candidate_id => v_job_row.candidate_id,
            p_cursor_json => v_cursor_json,
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'WORKBENCH_PREVIEW_ROWS_MATERIALISE' THEN
          v_stage_result := public.pay_workbench_preview_rows_materialise_chunk(
            p_session_id => v_job_row.session_id,
            p_candidate_id => v_job_row.candidate_id,
            p_cursor_json => v_cursor_json,
            p_limit => v_job_limit
          );
        ELSIF v_canonical_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN
          v_stage_result := public.pay_workbench_contract_client_dirty_fanout_chunk(
            p_job_id => v_job_row.id,
            p_cursor_json => v_cursor_json,
            p_limit => v_job_limit
          );
        ELSE
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_UNSUPPORTED_JOB_TYPE'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_WORKER_UNSUPPORTED_JOB_TYPE',
                    'job_id', v_job_row.id::text,
                    'job_type', v_job_row.job_type,
                    'canonical_job_type', v_canonical_job_type
                  )::text;
        END IF;

        v_stage_result := CASE
          WHEN jsonb_typeof(COALESCE(v_stage_result, '{}'::jsonb)) = 'object'
            THEN COALESCE(v_stage_result, '{}'::jsonb)
          ELSE jsonb_build_object(
            'ok', true,
            'result', COALESCE(v_stage_result, 'null'::jsonb)
          )
        END;

        IF v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'ok', 'true'))) IN ('false', 'f', '0', 'no', 'n', 'off')
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'made_progress', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND UPPER(BTRIM(COALESCE(v_stage_result->>'stop_reason', ''))) IN (
             'DELTA_PHASE_BUDGET_EXHAUSTED',
             'DELTA_ROWS_REMAINING',
             'DELTA_MORE_DUE',
             'DELTA_PHASE_COMPLETE_MORE_DUE',
             'BUDGET_EXHAUSTED'
           ) THEN
          v_stage_result := v_stage_result || jsonb_build_object('ok', true, 'worker_safe_delta_bounded_stop', true);
        END IF;

        IF v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE'
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'ok', 'true'))) IN ('false', 'f', '0', 'no', 'n', 'off')
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'made_progress', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND UPPER(BTRIM(COALESCE(v_stage_result->>'stop_reason', ''))) IN (
             'CLONE_REBASE_MORE_DUE',
             'CLONE_REBASE_BUDGET_EXHAUSTED',
             'CLONE_REBASE_PHASE_BUDGET_EXHAUSTED',
             'CLONE_REBASE_ROWS_REMAINING',
             'WORKBENCH_STAGE_BUDGET_EXHAUSTED',
             'BUDGET_EXHAUSTED'
           ) THEN
          v_stage_result := v_stage_result || jsonb_build_object('ok', true, 'worker_safe_clone_bounded_stop', true);
        END IF;

        IF LOWER(BTRIM(COALESCE(v_stage_result->>'ok', 'true')))
           IN ('false', 'f', '0', 'no', 'n', 'off') THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_STAGE_RETURNED_NOT_OK'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', COALESCE(
                      NULLIF(BTRIM(COALESCE(v_stage_result->>'code', '')), ''),
                      NULLIF(BTRIM(COALESCE(v_stage_result->>'error_code', '')), ''),
                      'PAY_WORKBENCH_WORKER_STAGE_RETURNED_NOT_OK'
                    ),
                    'job_id', v_job_row.id::text,
                    'job_type', v_job_row.job_type,
                    'canonical_job_type', v_canonical_job_type,
                    'stage_result', v_stage_result
                  )::text;
        END IF;

        IF v_canonical_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
           AND LOWER(BTRIM(COALESCE(v_stage_result->>'has_more', 'false')))
             IN ('true', 't', '1', 'yes', 'y', 'on') THEN
          v_next_cursor_json := CASE
            WHEN jsonb_typeof(v_stage_result->'next_cursor') = 'object'
              THEN v_stage_result->'next_cursor'
            WHEN jsonb_typeof(v_stage_result->'next_cursor_json') = 'object'
              THEN v_stage_result->'next_cursor_json'
            ELSE NULL::jsonb
          END;

          IF v_next_cursor_json IS NULL THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CONTINUATION_CURSOR_MISSING'
              USING ERRCODE = 'P0001',
                    DETAIL = jsonb_build_object(
                      'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CONTINUATION_CURSOR_MISSING',
                      'job_id', v_job_row.id::text,
                      'stage_result', v_stage_result
                    )::text;
          END IF;

          v_fanout_scope_kind := UPPER(BTRIM(COALESCE(
            v_payload_json->>'scope_kind',
            v_stage_result->>'scope_kind',
            ''
          )));
          v_fanout_scope_id := COALESCE(
            NULLIF(BTRIM(COALESCE(v_payload_json->>'scope_id', '')), ''),
            NULLIF(BTRIM(COALESCE(v_stage_result->>'scope_id', '')), '')
          );

          IF v_fanout_scope_kind NOT IN ('CONTRACT', 'CLIENT', 'UMBRELLA')
             OR v_fanout_scope_id IS NULL THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CONTINUATION_SCOPE_INVALID'
              USING ERRCODE = 'P0001',
                    DETAIL = jsonb_build_object(
                      'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CONTINUATION_SCOPE_INVALID',
                      'job_id', v_job_row.id::text,
                      'scope_kind', v_fanout_scope_kind,
                      'scope_id', v_fanout_scope_id
                    )::text;
          END IF;

          v_fanout_cursor_token := md5(v_next_cursor_json::text);
          v_fanout_continuation_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:'
            || v_fanout_scope_kind
            || ':'
            || v_fanout_scope_id
            || ':cursor:'
            || v_fanout_cursor_token;

          v_continuation_payload_json := (
            v_payload_json - ARRAY[
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
          ) || jsonb_build_object(
            'job_type', 'CONTRACT_CLIENT_DIRTY_FANOUT',
            'scope_kind', v_fanout_scope_kind,
            'scope_id', v_fanout_scope_id,
            'cursor_json', v_next_cursor_json,
            'cursor', v_next_cursor_json,
            'continuation', true,
            'source_job_id', v_job_row.id::text,
            'page_limit', v_job_limit,
            'limit', v_job_limit,
            'cursor_token', v_fanout_cursor_token,
            'dedupe_key', v_fanout_continuation_dedupe_key,
            'created_by_helper', 'pay_workbench_worker_drain_chunk',
            'created_at_utc', v_now::text
          );

          INSERT INTO public.banking_pay_workbench_jobs AS fanout_continuation (
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
            created_at_utc,
            updated_at_utc,
            started_at_utc,
            completed_at_utc,
            failed_at_utc,
            last_error_json
          )
          VALUES (
            'CONTRACT_CLIENT_DIRTY_FANOUT',
            'QUEUED',
            COALESCE(v_job_row.priority, 200),
            v_now,
            0,
            COALESCE(v_job_row.max_attempts, 8),
            v_fanout_continuation_dedupe_key,
            NULL::uuid,
            NULL::uuid,
            NULL::uuid,
            v_continuation_payload_json,
            v_now,
            v_now,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::jsonb
          )
          ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
          DO UPDATE
          SET priority = LEAST(fanout_continuation.priority, EXCLUDED.priority),
              run_at_utc = LEAST(fanout_continuation.run_at_utc, EXCLUDED.run_at_utc),
              payload_json = COALESCE(fanout_continuation.payload_json, '{}'::jsonb)
                || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
              updated_at_utc = v_now
          RETURNING fanout_continuation.id,
                    (xmax = 0)
          INTO v_fanout_continuation_job_id,
               v_fanout_continuation_created;

          PERFORM public._audit_insert(
            'banking_pay_workbench_job',
            v_fanout_continuation_job_id::text,
            CASE
              WHEN v_fanout_continuation_created THEN 'QUEUED'
              ELSE 'REUSED'
            END,
            NULL::jsonb,
            jsonb_build_object(
              'id', v_fanout_continuation_job_id::text,
              'job_type', 'CONTRACT_CLIENT_DIRTY_FANOUT',
              'status', 'QUEUED',
              'scope_kind', v_fanout_scope_kind,
              'scope_id', v_fanout_scope_id,
              'source_job_id', v_job_row.id::text,
              'cursor_token', v_fanout_cursor_token,
              'created', v_fanout_continuation_created
            ),
            'WORKBENCH_DIRTY_FANOUT_CONTINUATION_ENQUEUE',
            NULL::uuid
          );
        END IF;

        v_completion_result := public.pay_workbench_complete_job(
          p_job_id => v_job_row.id,
          p_result_json => v_stage_result
        );

        IF LOWER(BTRIM(COALESCE(v_completion_result->>'ok', 'true')))
           IN ('false', 'f', '0', 'no', 'n', 'off') THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_WORKER_COMPLETION_RETURNED_NOT_OK'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', COALESCE(
                      NULLIF(BTRIM(COALESCE(v_completion_result->>'code', '')), ''),
                      NULLIF(BTRIM(COALESCE(v_completion_result->>'error_code', '')), ''),
                      'PAY_WORKBENCH_WORKER_COMPLETION_RETURNED_NOT_OK'
                    ),
                    'job_id', v_job_row.id::text,
                    'completion_result', v_completion_result
                  )::text;
        END IF;

        v_completion_continuation_count := CASE
          WHEN COALESCE(v_completion_result->>'continuation_count', '') ~ '^[0-9]+$'
            THEN (v_completion_result->>'continuation_count')::integer
          ELSE 0
        END;
        v_completion_continuation_reused_count := CASE
          WHEN COALESCE(v_completion_result->>'continuation_reused_count', '') ~ '^[0-9]+$'
            THEN (v_completion_result->>'continuation_reused_count')::integer
          ELSE 0
        END;

        v_continuations_created := v_continuations_created
          + GREATEST(
              v_completion_continuation_count
                - v_completion_continuation_reused_count,
              0
            )
          + CASE
              WHEN v_fanout_continuation_created THEN 1
              ELSE 0
            END;

        v_continuations_reused := v_continuations_reused
          + v_completion_continuation_reused_count
          + CASE
              WHEN v_fanout_continuation_job_id IS NOT NULL
                   AND v_fanout_continuation_created IS NOT TRUE THEN 1
              ELSE 0
            END;

        IF v_job_row.id IS NOT NULL
           AND NOT (v_job_row.id = ANY(COALESCE(v_processed_claimed_job_ids, ARRAY[]::uuid[]))) THEN
          v_processed_claimed_job_ids := array_append(v_processed_claimed_job_ids, v_job_row.id);
        END IF;

        v_processed_count := v_processed_count + 1;
        v_succeeded_count := v_succeeded_count + 1;
        IF v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
          v_delta_refresh_jobs_processed := v_delta_refresh_jobs_processed + 1;
          v_delta_source_rows_written := v_delta_source_rows_written + CASE WHEN COALESCE(v_stage_result->>'source_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'source_rows_written')::integer ELSE 0 END;
          v_delta_line_rows_written := v_delta_line_rows_written + CASE WHEN COALESCE(v_stage_result->>'line_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'line_rows_written')::integer ELSE 0 END;
          v_delta_preview_rows_written := v_delta_preview_rows_written + CASE WHEN COALESCE(v_stage_result->>'preview_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'preview_rows_written')::integer ELSE 0 END;
          v_delta_rows_superseded := v_delta_rows_superseded + CASE WHEN COALESCE(v_stage_result->>'rows_superseded', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'rows_superseded')::integer ELSE 0 END;
          v_delta_fallback_count := v_delta_fallback_count + CASE WHEN LOWER(BTRIM(COALESCE(v_stage_result->>'fallback_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
          v_delta_patch_count := v_delta_patch_count + CASE WHEN UPPER(BTRIM(COALESCE(v_stage_result->>'projection_mode', ''))) IN ('READINESS_PATCH', 'RESERVATION_PATCH', 'POST_DRAFT_OVERLAY') THEN 1 ELSE 0 END;
          v_delta_more_due_count := v_delta_more_due_count + CASE WHEN LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
        ELSIF v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN
          v_clone_rebase_jobs_processed := v_clone_rebase_jobs_processed + 1;
          v_clone_copied_candidate_count := v_clone_copied_candidate_count + CASE WHEN COALESCE(v_stage_result->>'copied_candidate_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'copied_candidate_count')::integer ELSE 0 END;
          v_clone_copied_preview_row_count := v_clone_copied_preview_row_count + CASE WHEN COALESCE(v_stage_result->>'copied_preview_row_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'copied_preview_row_count')::integer ELSE 0 END;
          v_clone_legacy_refresh_enqueued_count := v_clone_legacy_refresh_enqueued_count + CASE WHEN COALESCE(v_stage_result->>'legacy_refresh_enqueued_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'legacy_refresh_enqueued_count')::integer ELSE 0 END;
          v_clone_more_due_count := v_clone_more_due_count + CASE WHEN LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1 ELSE 0 END;
        END IF;

        v_job_results_json := v_job_results_json || jsonb_build_array(
          jsonb_build_object(
            'job_id', v_job_row.id::text,
            'job_type', v_job_row.job_type,
            'original_job_type', NULLIF(BTRIM(v_original_job_type), ''),
            'canonical_job_type', v_canonical_job_type,
            'job_type_normalized', v_job_type_normalized,
            'status', 'SUCCEEDED',
            'continuation_count', v_completion_continuation_count
              + CASE
                  WHEN v_fanout_continuation_job_id IS NULL THEN 0
                  ELSE 1
                END,
            'continuation_reused_count', v_completion_continuation_reused_count
              + CASE
                  WHEN v_fanout_continuation_job_id IS NOT NULL
                       AND v_fanout_continuation_created IS NOT TRUE THEN 1
                  ELSE 0
                END,
            'stage_result', v_stage_result,
            'source_rows_written', CASE WHEN COALESCE(v_stage_result->>'source_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'source_rows_written')::integer ELSE NULL::integer END,
            'source_page_count', CASE WHEN COALESCE(v_stage_result->>'source_page_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'source_page_count')::integer ELSE NULL::integer END,
            'targeted_payload_received', CASE WHEN v_stage_result ? 'targeted_payload_received' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'targeted_payload_received', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'fallback_used', CASE WHEN v_stage_result ? 'fallback_used' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'fallback_used', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'fallback_reason', NULLIF(BTRIM(COALESCE(v_stage_result->>'fallback_reason', '')), ''),
            'delta_refresh_complete', CASE WHEN v_stage_result ? 'delta_refresh_complete' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'delta_refresh_complete', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'delta_more_due', CASE WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'clone_more_due', CASE WHEN v_canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'more_due', v_stage_result->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'clone_copied_candidate_count', CASE WHEN COALESCE(v_stage_result->>'copied_candidate_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'copied_candidate_count')::integer ELSE NULL::integer END,
            'clone_copied_preview_row_count', CASE WHEN COALESCE(v_stage_result->>'copied_preview_row_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'copied_preview_row_count')::integer ELSE NULL::integer END,
            'clone_legacy_refresh_enqueued_count', CASE WHEN COALESCE(v_stage_result->>'legacy_refresh_enqueued_count', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'legacy_refresh_enqueued_count')::integer ELSE NULL::integer END,
            'delta_rows_superseded', CASE WHEN COALESCE(v_stage_result->>'rows_superseded', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'rows_superseded')::integer ELSE NULL::integer END,
            'delta_line_rows_written', CASE WHEN COALESCE(v_stage_result->>'line_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'line_rows_written')::integer ELSE NULL::integer END,
            'delta_preview_rows_written', CASE WHEN COALESCE(v_stage_result->>'preview_rows_written', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'preview_rows_written')::integer ELSE NULL::integer END,
            'cursor_advanced', CASE WHEN v_stage_result ? 'cursor_advanced' THEN LOWER(BTRIM(COALESCE(v_stage_result->>'cursor_advanced', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') ELSE NULL::boolean END,
            'collect_elapsed_ms', CASE WHEN COALESCE(v_stage_result->>'collect_elapsed_ms', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (v_stage_result->>'collect_elapsed_ms')::numeric ELSE NULL::numeric END,
            'canonical_elapsed_ms', CASE WHEN COALESCE(v_stage_result->>'canonical_elapsed_ms', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (v_stage_result->>'canonical_elapsed_ms')::numeric ELSE NULL::numeric END,
            'classifier_elapsed_ms', CASE WHEN COALESCE(v_stage_result->>'classifier_elapsed_ms', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (v_stage_result->>'classifier_elapsed_ms')::numeric ELSE NULL::numeric END,
            'timesheets_seen', CASE WHEN COALESCE(v_stage_result->>'timesheets_seen', '') ~ '^-?[0-9]+$' THEN (v_stage_result->>'timesheets_seen')::integer ELSE NULL::integer END
          )
        );
      END IF;

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_failure_sqlstate = RETURNED_SQLSTATE,
        v_failure_message = MESSAGE_TEXT,
        v_failure_detail = PG_EXCEPTION_DETAIL,
        v_failure_hint = PG_EXCEPTION_HINT,
        v_failure_context = PG_EXCEPTION_CONTEXT;

      v_job_type_normalized := false;

      SELECT failed_job.*
      INTO v_job_row
      FROM public.banking_pay_workbench_jobs AS failed_job
      WHERE failed_job.id = v_job_id
      FOR UPDATE;

      IF FOUND
         AND v_canonical_job_type = ANY(v_supported_job_types)
         AND UPPER(BTRIM(COALESCE(v_job_row.job_type, ''))) IS DISTINCT FROM v_canonical_job_type THEN
        UPDATE public.banking_pay_workbench_jobs AS normalized_failed_job
        SET job_type = v_canonical_job_type,
            payload_json = COALESCE(normalized_failed_job.payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'original_job_type', COALESCE(
                  NULLIF(BTRIM(COALESCE(normalized_failed_job.payload_json->>'original_job_type', '')), ''),
                  NULLIF(BTRIM(v_original_job_type), ''),
                  UPPER(BTRIM(COALESCE(v_job_row.job_type, '')))
                ),
                'job_type', v_canonical_job_type,
                'canonical_job_type', v_canonical_job_type,
                'job_type_normalized_by_worker', true,
                'job_type_normalized_at_utc', v_now::text
              ),
            updated_at_utc = v_now
        WHERE normalized_failed_job.id = v_job_row.id
        RETURNING normalized_failed_job.*
        INTO v_job_row;

        v_job_type_normalized := true;
      END IF;

      v_retry_after_seconds := LEAST(
        GREATEST(COALESCE(v_job_row.attempt_count, 1) * v_job_retry_base_seconds, v_job_retry_base_seconds),
        v_job_retry_max_seconds
      );

      v_error_json := jsonb_strip_nulls(
        jsonb_build_object(
          'code', COALESCE(
            NULLIF(BTRIM(COALESCE(v_failure_sqlstate, '')), ''),
            'PAY_WORKBENCH_WORKER_JOB_FAILED'
          ),
          'message', COALESCE(
            NULLIF(BTRIM(COALESCE(v_failure_message, '')), ''),
            'Banking Pay workbench job failed.'
          ),
          'sqlstate', v_failure_sqlstate,
          'detail', v_failure_detail,
          'hint', v_failure_hint,
          'context', v_failure_context,
          'job_id', v_job_id::text,
          'job_type', NULLIF(BTRIM(COALESCE(v_job_row.job_type, '')), ''),
          'original_job_type', NULLIF(BTRIM(COALESCE(v_original_job_type, '')), ''),
          'canonical_job_type', NULLIF(BTRIM(COALESCE(v_canonical_job_type, '')), ''),
          'job_type_normalized', v_job_type_normalized,
          'session_id', CASE
            WHEN v_job_row.session_id IS NULL THEN NULL::text
            ELSE v_job_row.session_id::text
          END,
          'candidate_id', CASE
            WHEN v_job_row.candidate_id IS NULL THEN NULL::text
            ELSE v_job_row.candidate_id::text
          END,
          'worker_id', v_worker_id,
          'retry_after_seconds', v_retry_after_seconds,
          'failed_at_utc', v_now::text
        )
      );

      v_failure_fallback_decision := NULL::text;
      v_failure_fallback_audit_failed := false;

      BEGIN
        v_fail_result := public.pay_workbench_fail_job(
          p_job_id => v_job_id,
          p_error_json => v_error_json,
          p_retry_after_seconds => v_retry_after_seconds
        );
        v_final_failure_status := UPPER(BTRIM(COALESCE(
          v_fail_result->>'status',
          'FAILED'
        )));
      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
          v_failure_handler_sqlstate = RETURNED_SQLSTATE,
          v_failure_handler_message = MESSAGE_TEXT,
          v_failure_handler_detail = PG_EXCEPTION_DETAIL,
          v_failure_handler_hint = PG_EXCEPTION_HINT,
          v_failure_handler_context = PG_EXCEPTION_CONTEXT;

        v_failure_handler_error_json := jsonb_strip_nulls(
          jsonb_build_object(
            'code', COALESCE(
              NULLIF(BTRIM(COALESCE(v_failure_handler_sqlstate, '')), ''),
              'PAY_WORKBENCH_FAILURE_HANDLER_FAILED'
            ),
            'message', COALESCE(
              NULLIF(BTRIM(COALESCE(v_failure_handler_message, '')), ''),
              'The normal workbench failure handler failed.'
            ),
            'sqlstate', v_failure_handler_sqlstate,
            'detail', v_failure_handler_detail,
            'hint', v_failure_handler_hint,
            'context', v_failure_handler_context,
            'failed_at_utc', v_now::text
          )
        );

        SELECT fallback_current.*
        INTO v_job_row
        FROM public.banking_pay_workbench_jobs AS fallback_current
        WHERE fallback_current.id = v_job_id
        FOR UPDATE;

        IF NOT FOUND THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_FAILURE_FALLBACK_JOB_MISSING'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_FAILURE_FALLBACK_JOB_MISSING',
                    'job_id', v_job_id::text
                  )::text;
        END IF;

        v_failure_fallback_updated_count := 0;
        v_failure_fallback_scope_updated_count := 0;
        v_failure_fallback_line_count := 0;
        v_failure_fallback_audit_failed := false;

        IF UPPER(BTRIM(COALESCE(v_job_row.status, ''))) = 'RUNNING'
           AND COALESCE(v_job_row.attempt_count, 0) < COALESCE(v_job_row.max_attempts, 8) THEN
          v_failure_fallback_decision := 'REQUEUED';

          UPDATE public.banking_pay_workbench_jobs AS fallback_retry
          SET status = 'QUEUED',
              run_at_utc = v_now + make_interval(secs => v_retry_after_seconds),
              started_at_utc = NULL::timestamptz,
              completed_at_utc = NULL::timestamptz,
              failed_at_utc = NULL::timestamptz,
              last_error_json = v_error_json,
              payload_json = jsonb_strip_nulls(
                COALESCE(fallback_retry.payload_json, '{}'::jsonb)
                || jsonb_build_object(
                  'last_failure_json', v_error_json,
                  'failure_handler_error_json', v_failure_handler_error_json,
                  'failure_fallback_applied', true,
                  'failure_fallback_decision', v_failure_fallback_decision,
                  'failure_fallback_at_utc', v_now::text,
                  'failure_fallback_retry_at_utc', (v_now + make_interval(secs => v_retry_after_seconds))::text,
                  'failure_fallback_policy', 'REQUEUE_WHILE_ATTEMPTS_REMAIN',
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                )
              ),
              updated_at_utc = v_now
          WHERE fallback_retry.id = v_job_id
            AND UPPER(BTRIM(COALESCE(fallback_retry.status, ''))) = 'RUNNING';

          GET DIAGNOSTICS v_failure_fallback_updated_count = ROW_COUNT;
          v_final_failure_status := 'QUEUED';
        ELSIF UPPER(BTRIM(COALESCE(v_job_row.status, ''))) = 'RUNNING' THEN
          v_failure_fallback_decision := 'FAILED_CLOSED_MAX_ATTEMPTS';

          UPDATE public.banking_pay_workbench_jobs AS fallback_terminal
          SET status = 'FAILED',
              completed_at_utc = NULL::timestamptz,
              failed_at_utc = v_now,
              last_error_json = v_error_json,
              payload_json = jsonb_strip_nulls(
                COALESCE(fallback_terminal.payload_json, '{}'::jsonb)
                || jsonb_build_object(
                  'last_failure_json', v_error_json,
                  'failure_handler_error_json', v_failure_handler_error_json,
                  'failure_fallback_applied', true,
                  'failure_fallback_decision', v_failure_fallback_decision,
                  'failure_fallback_at_utc', v_now::text,
                  'failure_fallback_policy', 'TERMINAL_ONLY_AT_MAX_ATTEMPTS',
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                )
              ),
              updated_at_utc = v_now
          WHERE fallback_terminal.id = v_job_id
            AND UPPER(BTRIM(COALESCE(fallback_terminal.status, ''))) = 'RUNNING';

          GET DIAGNOSTICS v_failure_fallback_updated_count = ROW_COUNT;
          v_final_failure_status := 'FAILED';

          IF v_failure_fallback_updated_count > 0
             AND v_job_row.session_id IS NOT NULL
             AND v_job_row.candidate_id IS NOT NULL
             AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
            UPDATE public.banking_pay_workbench_session_scope AS fallback_source_scope
            SET status = 'SOURCE_BUILD_ERROR',
                pending_job_id = NULL::uuid,
                dirty = true,
                error_json = jsonb_build_object(
                  'code', 'WORKBENCH_SOURCE_BUILD_JOB_FAILED',
                  'message', 'Candidate source build could not be completed after all retry attempts.',
                  'job_id', v_job_id::text,
                  'canonical_job_type', v_canonical_job_type,
                  'attempt_count', COALESCE(v_job_row.attempt_count, 0),
                  'max_attempts', COALESCE(v_job_row.max_attempts, 8),
                  'automatic_recovery_scheduled', false,
                  'failure_handler_fallback_applied', true,
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                ),
                updated_at_utc = v_now
            WHERE fallback_source_scope.session_id = v_job_row.session_id
              AND fallback_source_scope.candidate_id = v_job_row.candidate_id
              AND (
                fallback_source_scope.pending_job_id = v_job_id
                OR fallback_source_scope.pending_job_id IS NULL
              );

            GET DIAGNOSTICS v_failure_fallback_scope_updated_count = ROW_COUNT;
          ELSIF v_failure_fallback_updated_count > 0
                AND v_job_row.session_id IS NOT NULL
                AND v_job_row.candidate_id IS NOT NULL
                AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' THEN
            WITH fallback_failed_lines AS (
              UPDATE public.banking_pay_workbench_candidate_line_work AS fallback_line
              SET status = 'ERROR',
                  error_json = jsonb_build_object(
                    'code', 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED',
                    'message', 'Candidate line work could not be completed after all retry attempts.',
                    'job_id', v_job_id::text,
                    'attempt_count', COALESCE(v_job_row.attempt_count, 0),
                    'max_attempts', COALESCE(v_job_row.max_attempts, 8),
                    'automatic_recovery_scheduled', false,
                    'failure_handler_fallback_applied', true,
                    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                  ),
                  updated_at_utc = v_now
              WHERE fallback_line.session_id = v_job_row.session_id
                AND fallback_line.candidate_id = v_job_row.candidate_id
                AND UPPER(BTRIM(COALESCE(fallback_line.status, ''))) = 'PENDING'
                AND EXISTS (
                  SELECT 1
                  FROM public.banking_pay_workbench_session_scope AS fallback_owned_scope
                  WHERE fallback_owned_scope.session_id = v_job_row.session_id
                    AND fallback_owned_scope.candidate_id = v_job_row.candidate_id
                    AND (
                      fallback_owned_scope.pending_job_id = v_job_id
                      OR fallback_owned_scope.pending_job_id IS NULL
                    )
                )
              RETURNING fallback_line.id
            )
            SELECT COUNT(*)::integer
            INTO v_failure_fallback_line_count
            FROM fallback_failed_lines;

            UPDATE public.banking_pay_workbench_session_scope AS fallback_line_scope
            SET status = 'ERROR',
                pending_job_id = NULL::uuid,
                dirty = true,
                error_json = jsonb_build_object(
                  'code', 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED',
                  'message', 'Candidate line work could not be completed after all retry attempts.',
                  'job_id', v_job_id::text,
                  'line_work_failed_count', COALESCE(v_failure_fallback_line_count, 0),
                  'attempt_count', COALESCE(v_job_row.attempt_count, 0),
                  'max_attempts', COALESCE(v_job_row.max_attempts, 8),
                  'automatic_recovery_scheduled', false,
                  'failure_handler_fallback_applied', true,
                  'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                ),
                updated_at_utc = v_now
            WHERE fallback_line_scope.session_id = v_job_row.session_id
              AND fallback_line_scope.candidate_id = v_job_row.candidate_id
              AND (
                fallback_line_scope.pending_job_id = v_job_id
                OR fallback_line_scope.pending_job_id IS NULL
              );

            GET DIAGNOSTICS v_failure_fallback_scope_updated_count = ROW_COUNT;
          END IF;

          IF v_failure_fallback_scope_updated_count > 0
             AND v_job_row.session_id IS NOT NULL THEN
            UPDATE public.banking_pay_workbench_sessions AS fallback_session
            SET progress_state = 'ERROR',
                progress_json = COALESCE(fallback_session.progress_json, '{}'::jsonb)
                  || jsonb_build_object(
                    'last_terminal_failure_job_id', v_job_id::text,
                    'last_terminal_failure_code', CASE
                      WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
                        THEN 'WORKBENCH_SOURCE_BUILD_JOB_FAILED'
                      ELSE 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED'
                    END,
                    'failure_handler_fallback_applied', true,
                    'automatic_recovery_scheduled', false
                  ),
                progress_counter_version = COALESCE(fallback_session.progress_counter_version, 0) + 1,
                progress_updated_at_utc = v_now,
                updated_at_utc = v_now
            WHERE fallback_session.id = v_job_row.session_id;
          END IF;
        ELSE
          v_final_failure_status := UPPER(BTRIM(COALESCE(v_job_row.status, 'FAILED')));
          v_failure_fallback_decision := 'STATE_ALREADY_TRANSITIONED';
          v_failure_fallback_updated_count := 1;
        END IF;

        IF v_failure_fallback_updated_count = 0 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_FAILURE_FALLBACK_DID_NOT_TRANSITION_JOB'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_WORKBENCH_FAILURE_FALLBACK_DID_NOT_TRANSITION_JOB',
                    'job_id', v_job_id::text,
                    'observed_status', v_job_row.status,
                    'decision', v_failure_fallback_decision
                  )::text;
        END IF;

        BEGIN
          PERFORM public._audit_insert(
            'banking_pay_workbench_job',
            v_job_id::text,
            'FAILURE_HANDLER_FALLBACK_APPLIED',
            NULL,
            jsonb_build_object(
              'job_id', v_job_id::text,
              'status', v_final_failure_status,
              'decision', v_failure_fallback_decision,
              'attempt_count', COALESCE(v_job_row.attempt_count, 0),
              'max_attempts', COALESCE(v_job_row.max_attempts, 8),
              'scope_updated_count', v_failure_fallback_scope_updated_count,
              'line_work_failed_count', v_failure_fallback_line_count,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            'PAY_WORKBENCH_FAILURE_HANDLER_FALLBACK_APPLIED',
            NULL
          );
        EXCEPTION WHEN OTHERS THEN
          v_failure_fallback_audit_failed := true;
          UPDATE public.banking_pay_workbench_jobs AS fallback_audit_marker
          SET payload_json = COALESCE(fallback_audit_marker.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'failure_fallback_audit_failed', true,
              'failure_fallback_audit_failed_at_utc', v_now::text
            ),
              updated_at_utc = v_now
          WHERE fallback_audit_marker.id = v_job_id;
        END;
      END;

      IF v_job_id IS NOT NULL
         AND NOT (v_job_id = ANY(COALESCE(v_processed_claimed_job_ids, ARRAY[]::uuid[]))) THEN
        v_processed_claimed_job_ids := array_append(v_processed_claimed_job_ids, v_job_id);
      END IF;

      v_processed_count := v_processed_count + 1;
      v_failed_count := v_failed_count + 1;

      IF v_final_failure_status = 'DEAD' THEN
        v_dead_count := v_dead_count + 1;
      END IF;

      v_job_results_json := v_job_results_json || jsonb_build_array(
        jsonb_build_object(
          'job_id', v_job_id::text,
          'job_type', NULLIF(BTRIM(COALESCE(v_job_row.job_type, '')), ''),
          'original_job_type', NULLIF(BTRIM(COALESCE(v_original_job_type, '')), ''),
          'canonical_job_type', NULLIF(BTRIM(COALESCE(v_canonical_job_type, '')), ''),
          'job_type_normalized', v_job_type_normalized,
          'status', v_final_failure_status,
          'failure_fallback_decision', v_failure_fallback_decision,
          'failure_fallback_audit_failed', v_failure_fallback_audit_failed,
          'error_code', COALESCE(
            NULLIF(BTRIM(COALESCE(v_failure_sqlstate, '')), ''),
            'PAY_WORKBENCH_WORKER_JOB_FAILED'
          ),
          'retry_after_seconds', CASE
            WHEN v_final_failure_status = 'QUEUED' THEN v_retry_after_seconds
            ELSE NULL::integer
          END
        )
      );
    END;
  END LOOP;

  IF v_stop_reason IS NULL THEN
    v_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer);
    IF v_elapsed_ms >= (v_max_runtime_ms - v_min_phase_budget_ms) THEN
      v_more_due := true;
      v_stop_reason := 'WORKER_BUDGET_EARLY_STOP';
      v_final_more_due_elapsed_ms := 0;
    ELSE
      v_phase_started_at_utc := clock_timestamp();
        SELECT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_jobs AS due_job
          WHERE due_job.status = 'QUEUED'
            AND due_job.run_at_utc <= GREATEST(v_cutoff, v_now)
            AND (p_session_id IS NULL OR due_job.session_id = p_session_id)
            AND (p_candidate_id IS NULL OR due_job.candidate_id = p_candidate_id)
            AND (
              CASE
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_SESSION_SCOPE_SEED',
                  'SESSION_SCOPE_SEED',
                  'WORKBENCH_SCOPE_SEED',
                  'WORKBENCH_SCOPE_SEED_PAGE',
                  'SCOPE_SEED_PAGE'
                ) THEN 'WORKBENCH_SESSION_SCOPE_SEED'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                  'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
                  'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
                  'CANDIDATE_SOURCE_BUILD',
                  'CANDIDATE_SOURCE_BUILD_CHUNK',
                  'SOURCE_BUILD',
                  'SOURCE_BUILD_PAGE'
                ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_CANDIDATE_DELTA_REFRESH',
                  'CANDIDATE_DELTA_REFRESH',
                  'DELTA_REFRESH'
                ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_SESSION_CLONE_REBASE',
                  'SESSION_CLONE_REBASE',
                  'CLONE_REBASE'
                ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
                  'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
                  'CANDIDATE_LINE_WORK_SEED',
                  'CANDIDATE_LINE_WORK_SEED_PAGE',
                  'LINE_WORK_SEED_PAGE',
                  'SNAPSHOT_CANDIDATE_REFRESH',
                  'CANDIDATE_REFRESH'
                ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
                  'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
                  'CANDIDATE_LINE_WORK_PROCESS',
                  'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
                  'LINE_WORK_PROCESS',
                  'LINE_WORK_PROCESS_CHUNK'
                ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) IN (
                  'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
                  'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
                  'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
                  'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
                  'PREVIEW_ROWS_MATERIALISE',
                  'PREVIEW_ROWS_MATERIALIZE',
                  'PREVIEW_ROWS_MATERIALISE_CHUNK',
                  'PREVIEW_ROWS_MATERIALIZE_CHUNK',
                  'PREVIEW_ROW_MATERIALISE_CHUNK',
                  'PREVIEW_ROW_MATERIALIZE_CHUNK'
                ) THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
                WHEN UPPER(BTRIM(COALESCE(due_job.job_type, ''))) = 'CONTRACT_CLIENT_DIRTY_FANOUT'
                  THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
                ELSE UPPER(BTRIM(COALESCE(due_job.job_type, '')))
              END
            ) = ANY(v_allowed_job_types)
        )
        INTO v_more_due;
      v_final_more_due_elapsed_ms := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_started_at_utc)) * 1000)::integer);
      IF COALESCE(v_more_due, false) THEN
        v_stop_reason := 'MORE_DUE';
      ELSE
        v_stop_reason := 'NO_MORE_DUE';
      END IF;
    END IF;
  END IF;


  BEGIN
    SELECT
      COALESCE(COUNT(*) FILTER (
        WHERE diag_job.status = 'QUEUED'
          AND diag_job.run_at_utc <= GREATEST(v_cutoff, v_now)
      ), 0)::integer,
      COALESCE(COUNT(*) FILTER (
        WHERE diag_job.status = 'QUEUED'
          AND diag_job.run_at_utc <= GREATEST(v_cutoff, v_now)
      ), 0)::integer,
      COALESCE(COUNT(*) FILTER (
        WHERE diag_job.status = 'RUNNING'
      ), 0)::integer,
      COALESCE(COUNT(*) FILTER (
        WHERE diag_job.status = 'RUNNING'
          AND COALESCE(
            diag_job.updated_at_utc,
            diag_job.started_at_utc,
            diag_job.run_at_utc,
            diag_job.created_at_utc
          ) <= (GREATEST(v_cutoff, v_now) - make_interval(secs => v_lease_seconds))
      ), 0)::integer
    INTO
      v_due_queued_count,
      v_claimable_count,
      v_running_count,
      v_stale_running_count
    FROM public.banking_pay_workbench_jobs AS diag_job
    CROSS JOIN LATERAL (
      SELECT CASE
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_SESSION_SCOPE_SEED',
          'SESSION_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED',
          'WORKBENCH_SCOPE_SEED_PAGE',
          'SCOPE_SEED_PAGE'
        ) THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'CANDIDATE_DELTA_REFRESH',
          'DELTA_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_SESSION_CLONE_REBASE',
          'SESSION_CLONE_REBASE',
          'CLONE_REBASE'
        ) THEN 'WORKBENCH_SESSION_CLONE_REBASE'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
          'CANDIDATE_LINE_WORK_SEED',
          'CANDIDATE_LINE_WORK_SEED_PAGE',
          'LINE_WORK_SEED_PAGE',
          'SNAPSHOT_CANDIDATE_REFRESH',
          'CANDIDATE_REFRESH'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'CANDIDATE_LINE_WORK_PROCESS',
          'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
          'LINE_WORK_PROCESS',
          'LINE_WORK_PROCESS_CHUNK'
        ) THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) IN (
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
          'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROWS_MATERIALISE',
          'PREVIEW_ROWS_MATERIALIZE',
          'PREVIEW_ROWS_MATERIALISE_CHUNK',
          'PREVIEW_ROWS_MATERIALIZE_CHUNK',
          'PREVIEW_ROW_MATERIALISE_CHUNK',
          'PREVIEW_ROW_MATERIALIZE_CHUNK'
        ) THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        WHEN UPPER(BTRIM(COALESCE(diag_job.job_type, ''))) = 'CONTRACT_CLIENT_DIRTY_FANOUT'
          THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
        ELSE UPPER(BTRIM(COALESCE(diag_job.job_type, '')))
      END AS canonical_job_type
    ) AS diag_job_type
    WHERE diag_job.status IN ('QUEUED', 'RUNNING')
      AND (p_session_id IS NULL OR diag_job.session_id = p_session_id)
      AND (p_candidate_id IS NULL OR diag_job.candidate_id = p_candidate_id)
      AND diag_job_type.canonical_job_type = ANY(v_allowed_job_types);
  EXCEPTION WHEN OTHERS THEN
    v_due_queued_count := 0;
    v_claimable_count := 0;
    v_running_count := 0;
    v_stale_running_count := 0;
  END;

  BEGIN
    WITH post_claim_due AS MATERIALIZED (
      SELECT
        post_job.id,
        post_job.job_type,
        post_job.created_at_utc,
        post_job.run_at_utc
      FROM public.banking_pay_workbench_jobs AS post_job
      CROSS JOIN LATERAL (
        SELECT CASE
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED','SESSION_SCOPE_SEED','WORKBENCH_SCOPE_SEED','WORKBENCH_SCOPE_SEED_PAGE','SCOPE_SEED_PAGE') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK','WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE','CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH','CANDIDATE_DELTA_REFRESH','DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE','SESSION_CLONE_REBASE','CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED','WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE','CANDIDATE_LINE_WORK_SEED','CANDIDATE_LINE_WORK_SEED_PAGE','LINE_WORK_SEED_PAGE','SNAPSHOT_CANDIDATE_REFRESH','CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK','CANDIDATE_LINE_WORK_PROCESS','CANDIDATE_LINE_WORK_PROCESS_CHUNK','LINE_WORK_PROCESS','LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_PREVIEW_ROWS_MATERIALIZE','WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK','WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROWS_MATERIALISE','PREVIEW_ROWS_MATERIALIZE','PREVIEW_ROWS_MATERIALISE_CHUNK','PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROW_MATERIALISE_CHUNK','PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
          WHEN UPPER(BTRIM(COALESCE(post_job.job_type, ''))) = 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN 'CONTRACT_CLIENT_DIRTY_FANOUT'
          ELSE UPPER(BTRIM(COALESCE(post_job.job_type, '')))
        END AS canonical_job_type
      ) AS post_job_type
      WHERE post_job.status = 'QUEUED'
        AND post_job.run_at_utc <= GREATEST(v_cutoff, v_now)
        AND (p_session_id IS NULL OR post_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR post_job.candidate_id = p_candidate_id)
        AND post_job_type.canonical_job_type = ANY(v_allowed_job_types)
        AND (
             (v_claim_phase_completed_at IS NOT NULL AND post_job.created_at_utc >= v_claim_phase_completed_at)
          OR EXISTS (
               SELECT 1
               FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_dirty_priority_actual_refresh_job_ids) = 'array' THEN v_dirty_priority_actual_refresh_job_ids ELSE '[]'::jsonb END) AS dirty_refresh_id(value)
               WHERE dirty_refresh_id.value = post_job.id::text
             )
        )
      ORDER BY post_job.created_at_utc,
               post_job.id
      LIMIT 20
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(post_claim_due.id::text), '[]'::jsonb),
           COALESCE(jsonb_agg(DISTINCT post_claim_due.job_type), '[]'::jsonb),
           COALESCE(jsonb_agg(jsonb_build_object('job_id', post_claim_due.id::text, 'job_type', post_claim_due.job_type, 'created_at_utc', post_claim_due.created_at_utc, 'run_at_utc', post_claim_due.run_at_utc)), '[]'::jsonb)
    INTO v_created_after_claim_count,
         v_post_claim_due_job_ids,
         v_post_claim_due_job_types,
         v_post_claim_due_sample
    FROM post_claim_due;
  EXCEPTION WHEN OTHERS THEN
    v_created_after_claim_count := 0;
    v_post_claim_due_job_ids := '[]'::jsonb;
    v_post_claim_due_job_types := '[]'::jsonb;
    v_post_claim_due_sample := '[]'::jsonb;
  END;

  v_post_claim_due_detected := COALESCE(v_created_after_claim_count, 0) > 0;
  v_post_claim_due_reason := CASE
    WHEN v_post_claim_due_detected THEN 'DUE_CREATED_AFTER_CLAIM_PHASE'
    ELSE NULL::text
  END;

  IF v_post_claim_due_detected
     AND v_claimed_count = 0
     AND v_processed_count = 0
     AND v_failed_count = 0
     AND v_dead_count = 0
     AND v_supplemental_stale_recovery_error_count = 0
     AND COALESCE(v_more_due, false) THEN
    v_more_due := true;
    v_stop_reason := 'POST_CLAIM_DUE_WORK_REQUIRES_NEXT_PASS';
    v_claim_mismatch_detected := true;
    v_claim_mismatch_reason := 'DUE_CREATED_AFTER_CLAIM_PHASE';
    v_claim_mismatch_json := COALESCE(v_claim_mismatch_json, '{}'::jsonb)
      || jsonb_build_object(
        'claim_mismatch_detected', true,
        'claim_mismatch_reason', 'DUE_CREATED_AFTER_CLAIM_PHASE',
        'stop_reason', 'POST_CLAIM_DUE_WORK_REQUIRES_NEXT_PASS',
        'post_claim_due_job_ids', v_post_claim_due_job_ids,
        'post_claim_due_job_types', v_post_claim_due_job_types,
        'post_claim_due_sample', v_post_claim_due_sample,
        'created_after_claim_count', v_created_after_claim_count,
        'claim_phase_started_at', v_claim_phase_started_at,
        'claim_phase_completed_at', v_claim_phase_completed_at,
        'dirty_priority_created_job_ids', v_dirty_priority_created_job_ids,
        'dirty_priority_actual_refresh_job_ids', v_dirty_priority_actual_refresh_job_ids
      );

    PERFORM public._temp_diag_log(
      'DUE_CREATED_AFTER_CLAIM_PHASE',
      'TEMP_BANKING_PAY_WORKBENCH',
      COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id),
      jsonb_build_object(
        'function_name', 'pay_workbench_worker_drain_chunk',
        'worker_id', v_worker_id,
        'origin', 'pay_workbench_worker_drain_chunk',
        'route', 'worker_drain',
        'pass_number', 1,
        'claim_phase_started_at', v_claim_phase_started_at,
        'claim_phase_completed_at', v_claim_phase_completed_at,
        'dirty_priority_job_id', CASE WHEN v_dirty_priority_claimed_job_id IS NULL THEN NULL::text ELSE v_dirty_priority_claimed_job_id::text END,
        'dirty_priority_created_job_ids', v_dirty_priority_created_job_ids,
        'actual_refresh_job_id', CASE WHEN jsonb_typeof(v_dirty_priority_actual_refresh_job_ids) = 'array' AND jsonb_array_length(v_dirty_priority_actual_refresh_job_ids) > 0 THEN v_dirty_priority_actual_refresh_job_ids->>0 ELSE NULL::text END,
        'post_claim_due_job_ids', v_post_claim_due_job_ids,
        'post_claim_due_job_types', v_post_claim_due_job_types,
        'due_queued_count', v_due_queued_count,
        'claimable_count', v_claimable_count,
        'claimed', v_claimed_count,
        'processed', v_processed_count,
        'more_due', v_more_due,
        'stop_reason', v_stop_reason
      )
    );

    PERFORM public._temp_diag_log(
      'POST_CLAIM_DUE_WORK_REQUIRES_NEXT_PASS',
      'TEMP_BANKING_PAY_WORKBENCH',
      COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id),
      jsonb_build_object(
        'function_name', 'pay_workbench_worker_drain_chunk',
        'worker_id', v_worker_id,
        'origin', 'pay_workbench_worker_drain_chunk',
        'route', 'worker_drain',
        'pass_number', 1,
        'claim_mismatch_reason', 'DUE_CREATED_AFTER_CLAIM_PHASE',
        'post_claim_due_job_ids', v_post_claim_due_job_ids,
        'dirty_priority_created_job_ids', v_dirty_priority_created_job_ids,
        'dirty_priority_actual_refresh_job_ids', v_dirty_priority_actual_refresh_job_ids,
        'due_queued_count', v_due_queued_count,
        'claimable_count', v_claimable_count,
        'claimed', v_claimed_count,
        'processed', v_processed_count,
        'made_progress_current_pass', v_dirty_priority_made_progress,
        'made_progress_cumulative', v_dirty_priority_made_progress,
        'more_due', v_more_due,
        'stop_reason', v_stop_reason
      )
    );

  ELSIF (v_claim_lock_contention_detected OR v_claim_mismatch_detected)
     AND v_claimed_count = 0
     AND v_processed_count = 0
     AND v_failed_count = 0
     AND v_dead_count = 0
     AND v_supplemental_stale_recovery_error_count = 0
     AND COALESCE(v_more_due, false) THEN
    v_more_due := true;
    v_stop_reason := CASE
      WHEN v_claim_mismatch_detected THEN 'NO_PROGRESS_DUE_BUT_UNCLAIMED'
      ELSE 'LOCKED_BY_CONCURRENT_WORKER'
    END;
    IF v_claim_lock_contention_detected IS TRUE AND COALESCE(v_claim_lock_contention_count, 0) <= 0 THEN
      v_claim_lock_contention_count := GREATEST(COALESCE(v_due_queued_count, 0), 0);
    END IF;
    BEGIN
      PERFORM public._temp_diag_log(
        'TEMP_WORKBENCH_CLAIM_DIAG',
        'CLAIMABLE_BUT_UNCLAIMED_EXPLAIN',
        COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id, 'pay_workbench_worker_drain_chunk'),
        jsonb_strip_nulls(jsonb_build_object(
          'function_name', 'pay_workbench_worker_drain_chunk',
          'stage', 'CLAIMABLE_BUT_UNCLAIMED_EXPLAIN',
          'pass_number', 1,
          'route', 'worker_drain',
          'worker_id', v_worker_id,
          'origin', 'pay_workbench_worker_drain_chunk',
          'budget_profile', v_worker_budget_profile,
          'allowed_job_types', CASE WHEN p_allowed_job_types IS NULL THEN NULL ELSE to_jsonb(p_allowed_job_types) END,
          'claim_limit', v_budget_claim_limit,
          'effective_db_limit', v_budget_claim_limit,
          'computed_rpc_budget_ms', v_max_runtime_ms,
          'due_queued_count', v_due_queued_count,
          'claimable_count', v_claimable_count,
          'claimed', v_claimed_count,
          'processed', v_processed_count,
          'running_count', v_running_count,
          'stale_running_count', v_stale_running_count,
          'claim_lock_contention_detected', v_claim_lock_contention_detected,
          'lock_contention_detected', v_claim_lock_contention_detected,
          'job_ids_sample', COALESCE(v_claim_lock_contention_sample, '[]'::jsonb),
          'claim_mismatch_json', COALESCE(v_claim_mismatch_json, '{}'::jsonb),
          'claim_mismatch_reason', v_claim_mismatch_reason,
          'more_due', v_more_due,
          'db_stop_reason', v_stop_reason
        ))
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END IF;

  v_normal_claim_made_progress := COALESCE(v_claimed_count, 0) > 0
    OR COALESCE(v_processed_count, 0) > 0
    OR COALESCE(v_recovered_stale_count, 0) > 0
    OR COALESCE(v_dead_stale_count, 0) > 0
    OR COALESCE(v_continuations_created, 0) > 0
    OR COALESCE(v_continuations_reused, 0) > 0
    OR COALESCE(v_terminalisation_count, 0) > 0
    OR (
         COALESCE(v_delta_source_rows_written, 0)
       + COALESCE(v_delta_line_rows_written, 0)
       + COALESCE(v_delta_preview_rows_written, 0)
       + COALESCE(v_delta_rows_superseded, 0)
       + COALESCE(v_clone_copied_candidate_count, 0)
       + COALESCE(v_clone_copied_preview_row_count, 0)
       + COALESCE(v_clone_legacy_refresh_enqueued_count, 0)
      ) > 0;
  v_made_progress_current_pass := COALESCE(v_dirty_priority_made_progress, false)
    OR COALESCE(v_normal_claim_made_progress, false);
  v_made_progress_cumulative := v_made_progress_current_pass;

  IF COALESCE(v_stop_reason, '') = 'WORKER_BUDGET_EARLY_STOP' THEN
    PERFORM public._temp_diag_log(
      'WORKER_BUDGET_EARLY_STOP',
      'TEMP_BANKING_PAY_WORKBENCH',
      COALESCE(p_session_id::text, p_candidate_id::text, v_worker_id),
      jsonb_build_object(
        'function_name', 'pay_workbench_worker_drain_chunk',
        'worker_id', v_worker_id,
        'origin', 'pay_workbench_worker_drain_chunk',
        'db_worker_max_runtime_ms', v_max_runtime_ms,
        'db_statement_timeout_ms', 15000,
        'backend_rpc_timeout_ms', LEAST(v_max_runtime_ms + 1000, 14000),
        'computed_rpc_budget_ms', v_max_runtime_ms,
        'effective_max_runtime_ms', v_max_runtime_ms,
        'budget_remaining_ms', GREATEST(0, v_max_runtime_ms - GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer)),
        'stop_reason', v_stop_reason,
        'more_due', v_more_due,
        'claimed', v_claimed_count,
        'processed', v_processed_count,
        'row_units_processed', COALESCE(v_delta_source_rows_written, 0) + COALESCE(v_delta_line_rows_written, 0) + COALESCE(v_delta_preview_rows_written, 0) + COALESCE(v_delta_rows_superseded, 0)
      )
    );
  END IF;

  RETURN jsonb_build_object(
      'ok', v_failed_count = 0 AND v_supplemental_stale_recovery_error_count = 0,
      'dirty_priority_jobs_processed', v_dirty_priority_jobs_processed,
      'dirty_priority_jobs_remaining', v_dirty_priority_jobs_remaining,
      'dirty_priority_cap_reached', v_dirty_priority_cap_reached,
      'dirty_priority_result', v_dirty_priority_result,
      'dirty_priority_made_progress', COALESCE(v_dirty_priority_made_progress, false),
      'dirty_priority_created_job_ids', COALESCE(v_dirty_priority_created_job_ids, '[]'::jsonb),
      'dirty_priority_actual_refresh_job_ids', COALESCE(v_dirty_priority_actual_refresh_job_ids, '[]'::jsonb),
      'worker_id', v_worker_id,
      'worker_budget_profile', v_worker_budget_profile,
      'lease_seconds', v_lease_seconds,
      'server_utc', v_now,
      'cutoff_utc', v_cutoff,
      'limit', v_limit,
      'filtered_session_id', CASE
        WHEN p_session_id IS NULL THEN NULL::text
        ELSE p_session_id::text
      END,
      'filtered_candidate_id', CASE
        WHEN p_candidate_id IS NULL THEN NULL::text
        ELSE p_candidate_id::text
      END,
      'allowed_job_types', to_jsonb(v_allowed_job_types),
      'claimed', v_claimed_count,
      'processed', v_processed_count,
      'succeeded', v_succeeded_count,
      'failed', v_failed_count,
      'dead', v_dead_count,
      'obsolete_skipped', v_obsolete_skipped_count,
      'projection_terminalisation_count', COALESCE(v_projection_terminalisation_count, 0),
      'terminalisation_count', COALESCE(v_terminalisation_count, 0),
      'continuations_created', v_continuations_created,
      'continuations_reused', v_continuations_reused,
      'requeued_unprocessed_claimed', COALESCE(v_requeued_unprocessed_claimed_count, 0),
      'claim_lock_contention_detected', COALESCE(v_claim_lock_contention_detected, false),
      'lock_contention_detected', COALESCE(v_claim_lock_contention_detected, false),
      'claim_lock_contention_count', COALESCE(v_claim_lock_contention_count, 0),
      'lock_contention_count', COALESCE(v_claim_lock_contention_count, 0),
      'concurrent_worker_progress_expected', COALESCE(v_claim_lock_contention_detected, false),
      'more_due', COALESCE(v_more_due, false),
      'stop_reason', COALESCE(v_stop_reason, CASE WHEN COALESCE(v_more_due, false) THEN 'MORE_DUE' ELSE 'NO_MORE_DUE' END),
      'made_progress_current_pass', COALESCE(v_made_progress_current_pass, false),
      'made_progress_cumulative', COALESCE(v_made_progress_cumulative, false),
      'made_progress', COALESCE(v_made_progress_current_pass, false),
      'dirty_priority_made_progress', COALESCE(v_dirty_priority_made_progress, false),
      'normal_claim_made_progress', COALESCE(v_normal_claim_made_progress, false),
      'elapsed_ms', GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at_utc)) * 1000)::integer),
      'max_runtime_ms', v_max_runtime_ms,
      'min_phase_budget_ms', v_min_phase_budget_ms,
      'stage_work_units_per_job', v_stage_work_units_per_job,
      'stage_work_units_per_job_by_type', jsonb_build_object(
        'WORKBENCH_SESSION_SCOPE_SEED', v_scope_seed_work_units_per_job,
        'WORKBENCH_CANDIDATE_SOURCE_BUILD', v_source_build_work_units_per_job,
        'WORKBENCH_CANDIDATE_DELTA_REFRESH', v_delta_refresh_work_units_per_job,
        'WORKBENCH_SESSION_CLONE_REBASE', v_clone_rebase_work_units_per_job,
        'WORKBENCH_CANDIDATE_LINE_WORK_SEED', v_line_seed_work_units_per_job,
        'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', v_line_process_work_units_per_job,
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE', v_preview_materialise_work_units_per_job
      ),
      'job_retry_base_seconds', v_job_retry_base_seconds,
      'job_retry_max_seconds', v_job_retry_max_seconds,
      'near_deadline', COALESCE(v_stop_reason, '') = 'WORKER_BUDGET_EARLY_STOP'
    )
    || jsonb_build_object(
      'claimed_count', v_claimed_count,
      'processed_count', v_processed_count,
      'succeeded_count', v_succeeded_count,
      'failed_count', v_failed_count,
      'dead_count', v_dead_count,
      'obsolete_skipped_count', v_obsolete_skipped_count,
      'projection_terminalisation_count', COALESCE(v_projection_terminalisation_count, 0),
      'terminalisation_count', COALESCE(v_terminalisation_count, 0),
      'continuation_created_count', v_continuations_created,
      'continuation_reused_count', v_continuations_reused,
      'requeued_unprocessed_claimed_count', COALESCE(v_requeued_unprocessed_claimed_count, 0),
      'delta_refresh_jobs_processed', COALESCE(v_delta_refresh_jobs_processed, 0),
      'delta_source_rows_written', COALESCE(v_delta_source_rows_written, 0),
      'delta_line_rows_written', COALESCE(v_delta_line_rows_written, 0),
      'delta_preview_rows_written', COALESCE(v_delta_preview_rows_written, 0),
      'delta_rows_superseded', COALESCE(v_delta_rows_superseded, 0),
      'delta_fallback_count', COALESCE(v_delta_fallback_count, 0),
      'delta_patch_count', COALESCE(v_delta_patch_count, 0),
      'delta_more_due_count', COALESCE(v_delta_more_due_count, 0),
      'clone_rebase_jobs_processed', COALESCE(v_clone_rebase_jobs_processed, 0),
      'clone_jobs_processed', COALESCE(v_clone_rebase_jobs_processed, 0),
      'clone_copied_candidate_count', COALESCE(v_clone_copied_candidate_count, 0),
      'clone_copied_preview_row_count', COALESCE(v_clone_copied_preview_row_count, 0),
      'clone_legacy_refresh_enqueued_count', COALESCE(v_clone_legacy_refresh_enqueued_count, 0),
      'clone_more_due_count', COALESCE(v_clone_more_due_count, 0),
      'recovered_stale_count', v_recovered_stale_count,
      'dead_stale_count', v_dead_stale_count,
      'supplemental_recovered_stale_count', v_supplemental_stale_recovered_count,
      'supplemental_terminal_stale_count', v_supplemental_stale_terminal_count,
      'supplemental_stale_recovery_error_count', v_supplemental_stale_recovery_error_count,
      'claim_result', COALESCE(v_claim_result, '{}'::jsonb),
      'claim_lock_contention_sample', COALESCE(v_claim_lock_contention_sample, '[]'::jsonb),
      'claim_mismatch_detected', COALESCE(v_claim_mismatch_detected, false),
      'claim_mismatch_reason', v_claim_mismatch_reason,
      'claim_mismatch_json', COALESCE(v_claim_mismatch_json, '{}'::jsonb)
    )
    || jsonb_build_object(
      'post_claim_due_work_requires_next_pass', COALESCE(v_post_claim_due_detected, false),
      'created_after_claim_count', COALESCE(v_created_after_claim_count, 0),
      'claim_phase_started_at', v_claim_phase_started_at,
      'claim_phase_completed_at', v_claim_phase_completed_at,
      'pre_claim_due_count', COALESCE(v_pre_claim_due_count, 0),
      'pre_claim_claimable_count', COALESCE(v_pre_claim_claimable_count, 0),
      'pre_claim_job_sample', COALESCE(v_pre_claim_job_sample, '[]'::jsonb),
      'post_claim_due_job_ids', COALESCE(v_post_claim_due_job_ids, '[]'::jsonb),
      'post_claim_due_job_types', COALESCE(v_post_claim_due_job_types, '[]'::jsonb),
      'post_claim_due_sample', COALESCE(v_post_claim_due_sample, '[]'::jsonb),
      'lock_contention_detected', COALESCE(v_claim_lock_contention_detected, false),
      'stale_recovery_elapsed_ms', COALESCE(v_supplemental_stale_elapsed_ms, 0),
      'claim_elapsed_ms', COALESCE(v_claim_elapsed_ms, 0),
      'budget_claim_limit', COALESCE(v_budget_claim_limit, 1),
      'final_more_due_elapsed_ms', COALESCE(v_final_more_due_elapsed_ms, 0),
      'due_queued_count', COALESCE(v_due_queued_count, 0),
      'claimable_count', COALESCE(v_claimable_count, 0),
      'running_count', COALESCE(v_running_count, 0),
      'stale_running_count', COALESCE(v_stale_running_count, 0),
      'jobs', COALESCE(v_job_results_json, '[]'::jsonb)
    );
END;
$function$
