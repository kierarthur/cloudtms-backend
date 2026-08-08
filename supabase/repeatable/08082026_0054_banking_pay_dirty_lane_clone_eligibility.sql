-- Targeted Banking Pay dirty-lane ownership and historical clone eligibility authority.
-- Extracted from the current monolith so unchanged 9 MB authority is not redeployed.

CREATE OR REPLACE FUNCTION public.pay_workbench_dirty_apply_jobs_chunk(p_limit integer DEFAULT 5, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_session_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_worker_id text DEFAULT NULL::text, p_lease_seconds integer DEFAULT 180)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := COALESCE(p_now_utc, clock_timestamp());
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 5), 0), 50);
  v_processed integer := 0;
  v_succeeded integer := 0;
  v_failed integer := 0;
  v_requeued integer := 0;
  v_recovered_stale integer := 0;
  v_remaining integer := 0;
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_stage_result jsonb := '{}'::jsonb;
  v_processor_result jsonb := '{}'::jsonb;
  v_results jsonb := '[]'::jsonb;
  v_latest_source_change_seq bigint := 0;
  v_processed_source_change_seq bigint := 0;
  v_rerun_required boolean := false;
  v_lease_seconds integer := LEAST(GREATEST(COALESCE(p_lease_seconds, 180), 30), 3600);
  v_worker_id text := LEFT(COALESCE(NULLIF(BTRIM(p_worker_id), ''), 'dirty-priority-worker'), 200);
  v_processor_has_more boolean := false;
  v_next_cursor_json jsonb := '{}'::jsonb;
  v_page_limit integer := 100;
  v_error_message text;
  v_error_state text;
BEGIN
  UPDATE public.banking_pay_workbench_jobs AS stale_job
  SET status = 'QUEUED',
      run_at_utc = v_now,
      started_at_utc = NULL,
      updated_at_utc = v_now,
      payload_json = public._pay_workbench_dirty_payload_merge(
        COALESCE(stale_job.payload_json, '{}'::jsonb),
        jsonb_build_object(
          'rerun_required', true,
          'recovered_from_stale_running_at_utc', v_now::text,
          'recovered_by_worker_id', v_worker_id
        )
      )
  WHERE stale_job.status = 'RUNNING'
    AND stale_job.updated_at_utc <= v_now - make_interval(secs => v_lease_seconds)
    AND (
      stale_job.job_type IN ('WORKBENCH_CANDIDATE_DIRTY_APPLY', 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY')
      OR (stale_job.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND COALESCE(stale_job.payload_json->>'queue_class', '') = 'DIRTY_TRIGGER_PRIORITY')
    );
  GET DIAGNOSTICS v_recovered_stale = ROW_COUNT;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_priority_lane_start', 'limit', v_limit, 'worker_id', v_worker_id, 'recovered_stale_count', v_recovered_stale));

  WHILE v_processed < v_limit LOOP
    v_job := NULL;

    WITH candidate_pool AS (
      SELECT
        claim_job.id,
        claim_job.job_type,
        claim_job.priority,
        claim_job.run_at_utc,
        claim_job.created_at_utc,
        claim_job.candidate_id,
        claim_job.payload_json,
        public._pay_workbench_candidate_serial_candidate_id(claim_job.candidate_id, claim_job.payload_json) AS candidate_serial_candidate_id,
        public._pay_workbench_candidate_serial_key(public._pay_workbench_candidate_serial_candidate_id(claim_job.candidate_id, claim_job.payload_json)) AS candidate_serial_key,
        (
          claim_job.job_type <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
          AND public._pay_workbench_candidate_serial_is_candidate_job(claim_job.job_type, claim_job.candidate_id, claim_job.payload_json)
        ) AS candidate_serial_required,
        (
          lower(BTRIM(COALESCE(claim_job.payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(claim_job.payload_json->>'run_mode', ''))) IN ('BOUNDED_CONTINUATION', 'CONTINUATION', 'STAGE_CONTINUATION')
          OR lower(BTRIM(COALESCE(claim_job.payload_json->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR (
            lower(BTRIM(COALESCE(claim_job.payload_json->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            AND NULLIF(BTRIM(COALESCE(claim_job.payload_json->>'fallback_reason', '')), '') IS NOT NULL
          )
          OR (
            NULLIF(BTRIM(COALESCE(claim_job.payload_json->>'source_job_id', claim_job.payload_json->>'continuation_source_job_id', claim_job.payload_json->>'bounded_continuation_source_job_id', '')), '') IS NOT NULL
            AND UPPER(BTRIM(COALESCE(claim_job.payload_json->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')
          )
        ) AS candidate_serial_is_chain_continuation
      FROM public.banking_pay_workbench_jobs AS claim_job
      WHERE claim_job.status = 'QUEUED'
        AND claim_job.run_at_utc <= v_now
        AND (
          claim_job.job_type IN ('WORKBENCH_CANDIDATE_DIRTY_APPLY', 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY')
          OR (claim_job.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND COALESCE(claim_job.payload_json->>'queue_class', '') = 'DIRTY_TRIGGER_PRIORITY')
        )
        AND (p_candidate_id IS NULL OR claim_job.candidate_id = p_candidate_id OR claim_job.payload_json->>'candidate_id' = p_candidate_id::text OR claim_job.payload_json->'candidate_ids' ? p_candidate_id::text)
        AND (p_session_id IS NULL OR claim_job.session_id = p_session_id OR claim_job.payload_json->>'session_id' = p_session_id::text OR claim_job.payload_json->>'source_session_id' = p_session_id::text)
      ORDER BY claim_job.run_at_utc ASC, claim_job.priority ASC, claim_job.created_at_utc ASC, claim_job.id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT GREATEST(v_limit * 5, 10)
    ), ranked_pool AS (
      SELECT
        candidate_pool.*,
        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(candidate_pool.candidate_serial_key, candidate_pool.id::text)
          ORDER BY
            CASE WHEN candidate_pool.candidate_serial_required IS TRUE AND candidate_pool.candidate_serial_is_chain_continuation IS TRUE THEN 0 ELSE 1 END ASC,
            candidate_pool.run_at_utc ASC,
            candidate_pool.priority ASC,
            candidate_pool.created_at_utc ASC,
            candidate_pool.id ASC
        ) AS candidate_serial_rank
      FROM candidate_pool
    ), next_job AS (
      SELECT ranked_pool.id,
             ranked_pool.candidate_serial_required,
             ranked_pool.candidate_serial_key,
             ranked_pool.candidate_serial_candidate_id
      FROM ranked_pool
      CROSS JOIN LATERAL (
        SELECT public._pay_workbench_candidate_serial_active_state(
          ranked_pool.id,
          ranked_pool.candidate_serial_candidate_id,
          ranked_pool.job_type,
          ranked_pool.payload_json,
          v_now
        ) AS state_json
      ) AS candidate_serial_state
      WHERE (ranked_pool.candidate_serial_required IS NOT TRUE OR ranked_pool.candidate_serial_rank = 1)
        AND (ranked_pool.candidate_serial_required IS NOT TRUE OR pg_try_advisory_xact_lock(hashtextextended(ranked_pool.candidate_serial_key, 24062027)))
        AND (ranked_pool.candidate_serial_required IS NOT TRUE OR lower(BTRIM(COALESCE(candidate_serial_state.state_json->>'blocked', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on'))
      ORDER BY ranked_pool.run_at_utc ASC, ranked_pool.priority ASC, ranked_pool.created_at_utc ASC, ranked_pool.id ASC
      LIMIT 1
    ), claimed AS (
      UPDATE public.banking_pay_workbench_jobs AS update_job
      SET status = 'RUNNING',
          attempt_count = COALESCE(update_job.attempt_count, 0) + 1,
          started_at_utc = COALESCE(update_job.started_at_utc, v_now),
          updated_at_utc = v_now,
          last_error_json = NULL::jsonb,
          payload_json = public._pay_workbench_dirty_payload_merge(
            COALESCE(update_job.payload_json, '{}'::jsonb),
            jsonb_strip_nulls(jsonb_build_object(
              'claimed_at_utc', v_now::text,
              'claimed_by_worker_id', v_worker_id,
              'candidate_serial_key', CASE WHEN next_job.candidate_serial_required IS TRUE THEN next_job.candidate_serial_key ELSE NULL END,
              'candidate_serial_candidate_id', CASE WHEN next_job.candidate_serial_candidate_id IS NULL THEN NULL ELSE next_job.candidate_serial_candidate_id::text END,
              'candidate_serial_active_chain_id', CASE WHEN next_job.candidate_serial_required IS TRUE THEN COALESCE(NULLIF(BTRIM(COALESCE(update_job.payload_json->>'candidate_serial_active_chain_id', '')), ''), NULLIF(BTRIM(COALESCE(update_job.payload_json->>'source_job_id', '')), ''), update_job.id::text) ELSE NULL END,
              'candidate_serial_started_at_utc', CASE WHEN next_job.candidate_serial_required IS TRUE THEN v_now::text ELSE NULL END,
              'candidate_serial_reason', CASE WHEN next_job.candidate_serial_required IS TRUE THEN 'CANDIDATE_SERIAL_CLAIM_GRANTED' ELSE NULL END
            ))
          )
      FROM next_job
      WHERE update_job.id = next_job.id
      RETURNING update_job.*
    )
    SELECT claimed.* INTO v_job FROM claimed;

    IF v_job.id IS NULL THEN
      EXIT;
    END IF;

    v_processed := v_processed + 1;
    PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', v_job.id::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_priority_claimed', 'job_id', v_job.id::text, 'job_type', v_job.job_type, 'dedupe_key', v_job.dedupe_key, 'priority', v_job.priority, 'queue_class', COALESCE(v_job.payload_json->>'queue_class', '')));

    IF NULLIF(BTRIM(COALESCE(v_job.payload_json->>'candidate_serial_key', '')), '') IS NOT NULL THEN
      PERFORM public._pay_workbench_candidate_serial_audit(
        'CANDIDATE_SERIAL_CLAIM_GRANTED',
        v_job.id,
        public._pay_workbench_candidate_serial_candidate_id(v_job.candidate_id, v_job.payload_json),
        jsonb_build_object(
          'job_type', v_job.job_type,
          'candidate_serial_key', v_job.payload_json->>'candidate_serial_key',
          'candidate_serial_active_chain_id', v_job.payload_json->>'candidate_serial_active_chain_id',
          'claimed_by_worker_id', v_worker_id,
          'claimed_at_utc', v_now::text
        ),
        'CANDIDATE_SERIAL_CLAIM_GRANTED',
        NULL::uuid
      );
    END IF;

    BEGIN
      IF v_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY' THEN
        v_processor_result := public.pay_workbench_candidate_dirty_apply_job_process(v_job.id, 100);
      ELSIF v_job.job_type = 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY' THEN
        v_processor_result := public.pay_workbench_finance_case_dirty_apply_job_process(v_job.id, 100);
      ELSIF v_job.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN
        v_page_limit := COALESCE(
          CASE WHEN COALESCE(v_job.payload_json->>'page_limit', '') ~ '^\d+$' THEN (v_job.payload_json->>'page_limit')::integer END,
          100
        );
        v_processor_result := public.pay_workbench_contract_client_dirty_fanout_chunk(v_job.id, NULL::jsonb, v_page_limit);
      ELSE
        RAISE EXCEPTION 'Unsupported dirty priority job_type %', v_job.job_type
          USING ERRCODE = '22023';
      END IF;

      IF lower(COALESCE(v_processor_result->>'candidate_serial_delayed', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
        SELECT COALESCE(job_after.payload_json, '{}'::jsonb)
        INTO v_stage_result
        FROM public.banking_pay_workbench_jobs AS job_after
        WHERE job_after.id = v_job.id;

        v_requeued := v_requeued + 1;
        v_results := v_results || jsonb_build_array(jsonb_build_object(
          'job_id', v_job.id::text,
          'job_type', v_job.job_type,
          'status', 'DELAYED',
          'stage_result', v_processor_result,
          'job_payload_after', v_stage_result
        ));
        CONTINUE;
      END IF;

      v_processor_has_more := lower(COALESCE(v_processor_result->>'has_more', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_next_cursor_json := CASE
        WHEN jsonb_typeof(v_processor_result->'next_cursor_json') = 'object' THEN v_processor_result->'next_cursor_json'
        WHEN jsonb_typeof(v_processor_result->'next_cursor') = 'object' THEN v_processor_result->'next_cursor'
        ELSE '{}'::jsonb
      END;

      UPDATE public.banking_pay_workbench_jobs AS cursor_job
      SET payload_json = public._pay_workbench_dirty_payload_merge(
            COALESCE(cursor_job.payload_json, '{}'::jsonb),
            jsonb_build_object(
              'cursor_json', CASE WHEN v_processor_has_more THEN COALESCE(v_next_cursor_json, '{}'::jsonb) ELSE '{}'::jsonb END,
              'next_cursor_json', COALESCE(v_next_cursor_json, '{}'::jsonb),
              'has_more', COALESCE(v_processor_has_more, false)
            )
          ),
          updated_at_utc = v_now
      WHERE cursor_job.id = v_job.id;

      SELECT COALESCE(job_after.payload_json, '{}'::jsonb)
      INTO v_stage_result
      FROM public.banking_pay_workbench_jobs AS job_after
      WHERE job_after.id = v_job.id;

      v_latest_source_change_seq := COALESCE(CASE WHEN COALESCE(v_stage_result->>'latest_source_change_seq', '') ~ '^\d+$' THEN (v_stage_result->>'latest_source_change_seq')::bigint END, 0);
      v_processed_source_change_seq := COALESCE(CASE WHEN COALESCE(v_stage_result->>'processed_source_change_seq', '') ~ '^\d+$' THEN (v_stage_result->>'processed_source_change_seq')::bigint END, v_latest_source_change_seq);
      v_rerun_required := lower(COALESCE(v_stage_result->>'rerun_required', 'false')) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR v_latest_source_change_seq > v_processed_source_change_seq;

      IF v_processor_has_more IS TRUE THEN
        v_rerun_required := true;
      END IF;

      IF v_rerun_required THEN
        UPDATE public.banking_pay_workbench_jobs AS requeue_job
        SET status = 'QUEUED',
            run_at_utc = v_now,
            started_at_utc = NULL,
            completed_at_utc = NULL,
            failed_at_utc = NULL,
            last_error_json = NULL::jsonb,
            updated_at_utc = v_now,
            payload_json = public._pay_workbench_dirty_payload_merge(
              COALESCE(requeue_job.payload_json, '{}'::jsonb),
              jsonb_build_object(
                'rerun_required', false,
                'requeued_due_to_later_change_at_utc', v_now::text,
                'processed_source_change_seq', v_processed_source_change_seq,
                'latest_source_change_seq', GREATEST(v_latest_source_change_seq, v_processed_source_change_seq)
              )
            )
        WHERE requeue_job.id = v_job.id;
        v_requeued := v_requeued + 1;
        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', v_job.id::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_worker_requeued_due_to_later_change', 'job_id', v_job.id::text, 'job_type', v_job.job_type, 'latest_source_change_seq', v_latest_source_change_seq, 'processed_source_change_seq', v_processed_source_change_seq));
      ELSE
        PERFORM public.pay_workbench_complete_job(
          p_job_id => v_job.id,
          p_result_json => public._pay_workbench_dirty_payload_merge(
            v_stage_result,
            jsonb_build_object('completed_by_dirty_priority_lane', true, 'completed_at_utc', v_now::text)
          )
        );
        v_succeeded := v_succeeded + 1;
        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', v_job.id::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_worker_completed', 'job_id', v_job.id::text, 'job_type', v_job.job_type));
      END IF;

      v_results := v_results || jsonb_build_array(jsonb_build_object('job_id', v_job.id::text, 'job_type', v_job.job_type, 'status', CASE WHEN v_rerun_required THEN 'REQUEUED' ELSE 'SUCCEEDED' END, 'stage_result', v_processor_result, 'job_payload_after', v_stage_result));
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT, v_error_state = RETURNED_SQLSTATE;
      PERFORM public.pay_workbench_fail_job(
        p_job_id => v_job.id,
        p_error_json => jsonb_build_object(
          'code', COALESCE(v_error_state, 'DIRTY_PRIORITY_FAILED'),
          'message', COALESCE(v_error_message, 'Dirty priority job failed'),
          'job_type', v_job.job_type,
          'failed_at_utc', v_now::text,
          'worker_id', v_worker_id
        )
      );
      v_failed := v_failed + 1;
      v_results := v_results || jsonb_build_array(jsonb_build_object('job_id', v_job.id::text, 'job_type', v_job.job_type, 'status', 'FAILED', 'error', jsonb_build_object('code', v_error_state, 'message', v_error_message)));
      PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', v_job.id::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_worker_failed', 'job_id', v_job.id::text, 'job_type', v_job.job_type, 'error_code', v_error_state, 'error_message', v_error_message));
    END;
  END LOOP;

  SELECT COUNT(*)::integer
  INTO v_remaining
  FROM public.banking_pay_workbench_jobs AS remaining_job
  WHERE remaining_job.status = 'QUEUED'
    AND remaining_job.run_at_utc <= v_now
    AND (
      remaining_job.job_type IN ('WORKBENCH_CANDIDATE_DIRTY_APPLY', 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY')
      OR (remaining_job.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND COALESCE(remaining_job.payload_json->>'queue_class', '') = 'DIRTY_TRIGGER_PRIORITY')
    );

  IF v_processed >= v_limit AND v_remaining > 0 THEN
    PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_priority_cap_reached', 'dirty_priority_jobs_processed', v_processed, 'dirty_priority_jobs_remaining', v_remaining));
  ELSIF v_remaining = 0 THEN
    PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_dirty_apply_jobs_chunk', 'stage', 'dirty_priority_empty', 'dirty_priority_jobs_processed', v_processed));
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'dirty_priority_jobs_processed', v_processed,
    'processed', v_processed,
    'succeeded', v_succeeded,
    'failed', v_failed,
    'requeued', v_requeued,
    'recovered_stale_count', v_recovered_stale,
    'dirty_priority_jobs_remaining', v_remaining,
    'cap_reached', (v_processed >= v_limit AND v_remaining > 0),
    'more_due', (v_remaining > 0),
    'worker_id', v_worker_id,
    'job_results', v_results,
    'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
  );
END;
$function$;


ALTER FUNCTION public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer) TO postgres,authenticated,service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_clone_eligibility_v1(p_source_session_id uuid, p_target_session_id uuid, p_candidate_id uuid, p_options_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_today_uk date := (clock_timestamp() AT TIME ZONE 'Europe/London')::date;
  v_options_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_source_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_target_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_source_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_target_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_candidate_row public.candidates%ROWTYPE;
  v_settings_json jsonb := '{}'::jsonb;
  v_rail_provider text := 'CSV';
  v_rail_env text := 'PROD';
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_umbrella_bank_hash text := NULL::text;
  v_umbrella_enabled boolean := false;
  v_preview_count integer := 0;
  v_all_preview_count integer := 0;
  v_nonready_preview_count integer := 0;
  v_source_row_count integer := 0;
  v_line_work_count integer := 0;
  v_bad_preview_count integer := 0;
  v_bad_source_count integer := 0;
  v_bad_line_count integer := 0;
  v_noncurrent_source_risk_count integer := 0;
  v_unmaterialised_source_count integer := 0;
  v_orphan_ready_preview_count integer := 0;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_authority_timesheet_count integer := 0;
  v_authority_scope_digest text := md5('[]');
  v_current_source_change_seq bigint := 0;
  v_final_source_change_seq bigint := 0;
  v_source_change_seq_min bigint := NULL::bigint;
  v_source_change_seq_max bigint := NULL::bigint;
  v_outstanding_key_count integer := 0;
  v_nonzero_outstanding_count integer := 0;
  v_negative_outstanding_count integer := 0;
  v_unrepresented_negative_count integer := 0;
  v_unrepresented_positive_count integer := 0;
  v_active_settled_key_count integer := 0;
  v_active_reserved_key_count integer := 0;
  v_has_complexity boolean := false;
  v_has_pay_method_or_bank_mismatch boolean := false;
  v_has_readiness_mismatch boolean := false;
  v_has_entitlement_mismatch boolean := false;
  v_has_outstanding_mismatch boolean := false;
  v_has_extra_outstanding_key boolean := false;
  v_has_snapshot_scope_mismatch boolean := false;
  v_has_contract_mismatch boolean := false;
  v_has_snooze_target_date_risk boolean := false;
  v_has_active_snooze_risk boolean := false;
  v_reason text := NULL::text;
  v_controlled_clone_rebase boolean := false;
  v_option_source_session_id_text text := NULL::text;
  v_option_target_session_id_text text := NULL::text;
  v_option_source_session_id uuid := NULL::uuid;
  v_option_target_session_id uuid := NULL::uuid;
  v_target_snapshot_key_count integer := 0;
  v_target_scope_missing_allowed boolean := false;
  v_target_snapshot_empty_allowed boolean := false;
  v_certified_reuse_v2_probe boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_PROGRESS');

  IF p_source_session_id IS NULL OR p_target_session_id IS NULL OR p_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_TARGET_AND_CANDIDATE_REQUIRED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT target_session.*
  INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id = p_target_session_id
    AND UPPER(BTRIM(COALESCE(target_session.status, ''))) = 'OPEN'
    AND target_session.discarded_at_utc IS NULL
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'TARGET_SESSION_NOT_OPEN',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT source_session.*
  INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = p_source_session_id
    AND (
      (
        UPPER(BTRIM(COALESCE(source_session.status, ''))) = 'OPEN'
        AND source_session.discarded_at_utc IS NULL
      )
      OR (
        UPPER(BTRIM(COALESCE(source_session.status, ''))) = 'DISCARDED'
        AND source_session.discarded_at_utc IS NOT NULL
      )
    )
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_SESSION_NOT_OPEN',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  v_option_source_session_id_text := NULLIF(BTRIM(COALESCE(
    v_options_json->>'source_session_id',
    v_options_json->>'source_session_id_text',
    v_options_json->>'clone_from_session_id',
    v_options_json->>'replacement_source_session_id',
    ''
  )), '');

  IF v_option_source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_option_source_session_id := v_option_source_session_id_text::uuid;
  END IF;

  v_option_target_session_id_text := NULLIF(BTRIM(COALESCE(
    v_options_json->>'target_session_id',
    v_options_json->>'session_id',
    v_options_json->>'clone_to_session_id',
    ''
  )), '');

  IF v_option_target_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_option_target_session_id := v_option_target_session_id_text::uuid;
  END IF;

  v_controlled_clone_rebase := (
    (
      UPPER(BTRIM(COALESCE(
        v_options_json->>'job_type',
        v_options_json->>'operation_type',
        v_options_json->>'created_by_helper',
        ''
      ))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE', 'PAY_WORKBENCH_SESSION_OPEN_SHARED_V2')
      OR LOWER(BTRIM(COALESCE(
        v_options_json->>'allow_session_rebase',
        v_options_json->>'allowSessionRebase',
        'false'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(v_options_json->>'clone_mode', v_options_json->>'cloneMode', ''))) IN ('CERTIFIED_SIMPLE_ONLY', 'CERTIFIED_ONLY')
      OR LOWER(BTRIM(COALESCE(
        v_options_json->>'source_selection_authorised','false'
      ))) IN ('true','t','1','yes','y','on')
    )
    AND LOWER(BTRIM(COALESCE(
      v_options_json->>'rebase_simple_rows_only',
      v_options_json->>'rebaseSimpleRowsOnly',
      'true'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND (v_option_source_session_id IS NULL OR v_option_source_session_id = p_source_session_id)
    AND (v_option_target_session_id IS NULL OR v_option_target_session_id = p_target_session_id)
    AND (
      v_source_session.replacement_session_id = p_target_session_id
      OR UPPER(BTRIM(COALESCE(v_target_session.progress_state, ''))) = 'CLONE_REBASING'
      OR NULLIF(BTRIM(COALESCE(v_target_session.progress_json->>'clone_from_session_id', '')), '') = p_source_session_id::text
      OR LOWER(BTRIM(COALESCE(v_options_json->>'source_selection_authorised','false'))) IN ('true','t','1','yes','y','on')
    )
    AND v_source_session.pay_date IS NOT NULL
    AND v_target_session.pay_date IS NOT NULL
    AND v_source_session.pay_date IS DISTINCT FROM v_target_session.pay_date
  );

  /* This mode is a target-date proof only.  It is deliberately not clone
     authority: the postgres-only bounded certifier must still prove the
     immutable build, current publication parity and original lineage. */
  v_certified_reuse_v2_probe := COALESCE(v_controlled_clone_rebase, false) IS TRUE
    AND COALESCE(v_options_json->>'certification_version', '') = '2'
    AND LOWER(BTRIM(COALESCE(v_options_json->>'source_selection_authorised', 'false')))
          IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT target_scope.*
  INTO v_target_scope
  FROM public.banking_pay_workbench_session_scope AS target_scope
  WHERE target_scope.session_id = p_target_session_id
    AND target_scope.candidate_id = p_candidate_id
  FOR SHARE;

  SELECT source_scope.*
  INTO v_source_scope
  FROM public.banking_pay_workbench_session_scope AS source_scope
  WHERE source_scope.session_id = p_source_session_id
    AND source_scope.candidate_id = p_candidate_id
  FOR SHARE;

  IF v_source_scope.id IS NULL
     OR (v_target_scope.id IS NULL AND COALESCE(v_controlled_clone_rebase, false) IS NOT TRUE) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'CANDIDATE_NOT_IN_BOTH_SCOPES',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'controlled_clone_rebase', COALESCE(v_controlled_clone_rebase, false),
      'target_scope_missing_allowed', false
    );
  END IF;

  v_target_scope_missing_allowed := v_target_scope.id IS NULL
    AND COALESCE(v_controlled_clone_rebase, false) IS TRUE;

  IF UPPER(BTRIM(COALESCE(v_source_scope.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
     OR COALESCE(v_source_scope.dirty, false) IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_SCOPE_NOT_READY',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT candidate_row.*
  INTO v_candidate_row
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'CANDIDATE_NOT_FOUND',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT COALESCE(to_jsonb(settings_row), '{}'::jsonb)
  INTO v_settings_json
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  v_rail_provider := UPPER(BTRIM(COALESCE(v_settings_json->>'rail_provider_default', 'CSV')));
  v_rail_env := UPPER(BTRIM(COALESCE(v_settings_json->>'rail_env_default', 'PROD')));
  v_need_name_check := lower(BTRIM(COALESCE(v_settings_json->>'rail_supports_name_check', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                       AND v_rail_provider <> 'CSV';
  v_requires_payee_map := v_rail_provider <> 'CSV';

  SELECT umbrella_row.bank_details_hash, COALESCE(umbrella_row.enabled, false)
  INTO v_umbrella_bank_hash, v_umbrella_enabled
  FROM public.umbrellas AS umbrella_row
  WHERE umbrella_row.id = v_candidate_row.umbrella_id;

  SELECT COALESCE(app_counter.seq, 0)
  INTO v_current_source_change_seq
  FROM (SELECT 1) AS authority_anchor
  LEFT JOIN public.app_change_counters AS app_counter
    ON app_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  DROP TABLE IF EXISTS pg_temp._bpay_clone_all_preview;
  CREATE TEMP TABLE _bpay_clone_all_preview ON COMMIT DROP AS
  SELECT preview_row.*
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_source_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = v_source_session.version
    AND (
      v_certified_reuse_v2_probe IS NOT TRUE
      OR UPPER(BTRIM(COALESCE(preview_row.status, ''))) <> 'SUPERSEDED'
    );

  DROP TABLE IF EXISTS pg_temp._bpay_clone_source_preview;
  CREATE TEMP TABLE _bpay_clone_source_preview ON COMMIT DROP AS
  SELECT all_preview.*
  FROM pg_temp._bpay_clone_all_preview AS all_preview
  WHERE UPPER(BTRIM(COALESCE(all_preview.status, ''))) = 'READY';

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(all_preview.status, ''))) <> 'READY')::integer
  INTO v_all_preview_count,
       v_nonready_preview_count
  FROM pg_temp._bpay_clone_all_preview AS all_preview;

  SELECT COUNT(*)::integer
  INTO v_preview_count
  FROM pg_temp._bpay_clone_source_preview AS source_preview;

  /* Build candidate completeness from current authority, never from the READY
     projection that is being tested for cloning. */
  DROP TABLE IF EXISTS pg_temp._bpay_clone_authority_timesheets;
  CREATE TEMP TABLE _bpay_clone_authority_timesheets ON COMMIT DROP AS
  WITH authority_seed AS (
    SELECT current_tsfin.timesheet_id
    FROM public.timesheets_financials AS current_tsfin
    JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = current_tsfin.timesheet_id
     AND current_timesheet.is_current = true
    WHERE current_tsfin.candidate_id = p_candidate_id
      AND current_tsfin.is_current = true

    UNION

    SELECT all_preview.timesheet_id
    FROM pg_temp._bpay_clone_all_preview AS all_preview
    WHERE all_preview.timesheet_id IS NOT NULL

    UNION

    SELECT source_line.timesheet_id
    FROM public.banking_pay_workbench_candidate_source_lines AS source_line
    WHERE source_line.session_id = p_source_session_id
      AND source_line.candidate_id = p_candidate_id
      AND source_line.session_version = v_source_session.version
      AND source_line.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(source_line.status, ''))) IN ('CURRENT', 'DIRTY', 'ERROR')

    UNION

    SELECT snapshot_line.timesheet_id
    FROM public.banking_pay_snapshot_line_state AS snapshot_line
    WHERE snapshot_line.snapshot_run_id IN (v_source_session.source_snapshot_run_id, v_target_session.source_snapshot_run_id)
      AND snapshot_line.candidate_id = p_candidate_id
      AND snapshot_line.timesheet_id IS NOT NULL

    UNION

    SELECT advance_row.linked_timesheet_id
    FROM public.pay_advances AS advance_row
    WHERE advance_row.candidate_id = p_candidate_id
      AND advance_row.linked_timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(advance_row.status::text, ''))) IN ('ACTIVE', 'PAUSED')
      AND advance_row.cleared_at_utc IS NULL
      AND advance_row.written_off_at_utc IS NULL

    UNION

    SELECT component_row.linked_timesheet_id
    FROM public.pay_finance_case_components AS component_row
    WHERE component_row.candidate_id = p_candidate_id
      AND component_row.linked_timesheet_id IS NOT NULL
      AND component_row.closed_at_utc IS NULL

    UNION

    SELECT override_row.timesheet_id
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.candidate_id = p_candidate_id
      AND override_row.timesheet_id IS NOT NULL
      AND override_row.consumed_at_utc IS NULL
      AND override_row.cleared_at_utc IS NULL

    UNION

    SELECT adjustment_row.timesheet_id
    FROM public.ts_pay_adjustments AS adjustment_row
    WHERE adjustment_row.candidate_id = p_candidate_id
      AND adjustment_row.timesheet_id IS NOT NULL
      AND adjustment_row.paid_at_utc IS NULL

    UNION

    SELECT snooze_row.timesheet_id
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.candidate_id = p_candidate_id
      AND snooze_row.timesheet_id IS NOT NULL
      AND snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL

    UNION

    SELECT resolution_row.timesheet_id
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id IN (p_source_session_id, p_target_session_id)
      AND resolution_row.candidate_id = p_candidate_id
      AND resolution_row.timesheet_id IS NOT NULL

    UNION

    SELECT batch_item.timesheet_id
    FROM public.pay_batch_candidates AS batch_candidate
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = batch_candidate.id
    WHERE batch_candidate.candidate_id = p_candidate_id
      AND batch_item.timesheet_id IS NOT NULL
      AND COALESCE(batch_item.is_voided, false) IS NOT TRUE

    UNION

    SELECT correction_item.timesheet_id
    FROM public.pay_payment_correction_items AS correction_item
    WHERE correction_item.candidate_id = p_candidate_id
      AND correction_item.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(correction_item.status, ''))) = 'APPLIED'
  ),
  authority_seed_array AS (
    SELECT COALESCE(array_agg(authority_seed.timesheet_id ORDER BY authority_seed.timesheet_id), ARRAY[]::uuid[]) AS timesheet_ids
    FROM authority_seed
    WHERE authority_seed.timesheet_id IS NOT NULL
  ),
  authority_expanded AS (
    SELECT authority_seed.timesheet_id
    FROM authority_seed
    WHERE authority_seed.timesheet_id IS NOT NULL

    UNION

    SELECT rotation_scope.family_timesheet_id
    FROM authority_seed_array
    CROSS JOIN LATERAL public._pay_timesheet_rotation_scope(authority_seed_array.timesheet_ids) AS rotation_scope
    WHERE rotation_scope.family_timesheet_id IS NOT NULL

    UNION

    SELECT rotation_scope.canonical_timesheet_id
    FROM authority_seed_array
    CROSS JOIN LATERAL public._pay_timesheet_rotation_scope(authority_seed_array.timesheet_ids) AS rotation_scope
    WHERE rotation_scope.canonical_timesheet_id IS NOT NULL
  )
  SELECT DISTINCT authority_expanded.timesheet_id
  FROM authority_expanded
  WHERE authority_expanded.timesheet_id IS NOT NULL;

  SELECT COALESCE(array_agg(authority_timesheet.timesheet_id ORDER BY authority_timesheet.timesheet_id), ARRAY[]::uuid[]),
         COUNT(*)::integer,
         md5(COALESCE(jsonb_agg(authority_timesheet.timesheet_id::text ORDER BY authority_timesheet.timesheet_id), '[]'::jsonb)::text)
  INTO v_timesheet_ids,
       v_authority_timesheet_count,
       v_authority_scope_digest
  FROM pg_temp._bpay_clone_authority_timesheets AS authority_timesheet;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (
           WHERE source_preview.timesheet_id IS NULL
              OR source_preview.key_type IS NULL
              OR source_preview.key_value IS NULL
              OR UPPER(BTRIM(COALESCE(source_preview.section, ''))) <> 'CANONICAL_PREVIEW_LINES'
              OR lower(BTRIM(COALESCE(source_preview.row_json->>'policy_x_authority_scope', ''))) <> lower('PRE_DRAFT_LIVE_TRUTH')
              OR lower(BTRIM(COALESCE(source_preview.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         )::integer
  INTO v_preview_count,
       v_bad_preview_count
  FROM pg_temp._bpay_clone_source_preview AS source_preview;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (
           WHERE source_line.timesheet_id IS NULL
              OR source_line.line_key IS NULL
              OR jsonb_typeof(COALESCE(source_line.source_row_json, '{}'::jsonb)) <> 'object'
              OR jsonb_typeof(COALESCE(source_line.economic_key_json, '{}'::jsonb)) <> 'object'
              OR jsonb_typeof(COALESCE(source_line.contract_json, '{}'::jsonb)) <> 'object'
              OR lower(BTRIM(COALESCE(source_line.contract_json->>'policy_x_authority_scope', source_line.source_row_json->>'policy_x_authority_scope', ''))) <> lower('PRE_DRAFT_LIVE_TRUTH')
         )::integer,
         MIN(source_line.source_change_seq),
         MAX(source_line.source_change_seq)
  INTO v_source_row_count,
       v_bad_source_count,
       v_source_change_seq_min,
       v_source_change_seq_max
  FROM public.banking_pay_workbench_candidate_source_lines AS source_line
  WHERE source_line.session_id = p_source_session_id
    AND source_line.candidate_id = p_candidate_id
    AND source_line.session_version = v_source_session.version
    AND source_line.status = 'CURRENT';

  SELECT COUNT(*)::integer
  INTO v_noncurrent_source_risk_count
  FROM public.banking_pay_workbench_candidate_source_lines AS source_line
  WHERE source_line.session_id = p_source_session_id
    AND source_line.candidate_id = p_candidate_id
    AND source_line.session_version = v_source_session.version
    AND UPPER(BTRIM(COALESCE(source_line.status, ''))) IN ('DIRTY', 'ERROR');

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) NOT IN ('MATERIALISED', 'MATERIALIZED', 'SKIPPED'))::integer
  INTO v_line_work_count,
       v_bad_line_count
  FROM public.banking_pay_workbench_candidate_line_work AS line_work
  WHERE line_work.session_id = p_source_session_id
    AND line_work.candidate_id = p_candidate_id
    AND (
      v_certified_reuse_v2_probe IS NOT TRUE
      OR UPPER(BTRIM(COALESCE(line_work.status, '')))
           IN ('PENDING', 'READY', 'ERROR', 'FAILED')
    );

  SELECT COUNT(*)::integer
  INTO v_unmaterialised_source_count
  FROM public.banking_pay_workbench_candidate_source_lines AS source_line
  WHERE source_line.session_id = p_source_session_id
    AND source_line.candidate_id = p_candidate_id
    AND source_line.session_version = v_source_session.version
    AND source_line.status = 'CURRENT'
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_clone_source_preview AS source_preview
      WHERE source_preview.row_key = source_line.line_key
        AND source_preview.section IS NOT DISTINCT FROM source_line.section
    );

  SELECT COUNT(*)::integer
  INTO v_orphan_ready_preview_count
  FROM pg_temp._bpay_clone_source_preview AS source_preview
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_candidate_source_lines AS source_line
    WHERE source_line.session_id = p_source_session_id
      AND source_line.candidate_id = p_candidate_id
      AND source_line.session_version = v_source_session.version
      AND source_line.status = 'CURRENT'
      AND source_line.line_key = source_preview.row_key
      AND source_line.section IS NOT DISTINCT FROM source_preview.section
  );

  IF COALESCE(v_nonready_preview_count, 0) > 0
     OR COALESCE(v_noncurrent_source_risk_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_PROJECTION_HAS_NON_READY_OR_POISONED_STATE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'all_preview_row_count', COALESCE(v_all_preview_count, 0),
      'ready_preview_row_count', COALESCE(v_preview_count, 0),
      'nonready_preview_row_count', COALESCE(v_nonready_preview_count, 0),
      'noncurrent_source_risk_count', COALESCE(v_noncurrent_source_risk_count, 0),
      'authority_scope_digest', v_authority_scope_digest
    );
  END IF;

  IF v_certified_reuse_v2_probe IS NOT TRUE
     AND (
       COALESCE(v_bad_preview_count, 0) > 0
     OR COALESCE(v_bad_source_count, 0) > 0
     OR COALESCE(v_bad_line_count, 0) > 0
     OR COALESCE(v_unmaterialised_source_count, 0) > 0
     OR COALESCE(v_orphan_ready_preview_count, 0) > 0
     OR (COALESCE(v_preview_count, 0) > 0 AND COALESCE(v_source_row_count, 0) = 0)
     OR (COALESCE(v_preview_count, 0) = 0 AND COALESCE(v_source_row_count, 0) > 0)
     OR (COALESCE(v_preview_count, 0) = 0 AND COALESCE(v_line_work_count, 0) > 0)
     ) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_ROWS_NOT_CERTIFIED_SIMPLE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'bad_preview_count', COALESCE(v_bad_preview_count, 0),
      'bad_source_count', COALESCE(v_bad_source_count, 0),
      'bad_line_count', COALESCE(v_bad_line_count, 0),
      'unmaterialised_source_count', COALESCE(v_unmaterialised_source_count, 0),
      'orphan_ready_preview_count', COALESCE(v_orphan_ready_preview_count, 0),
      'source_preview_row_count', COALESCE(v_preview_count, 0),
      'source_row_count', COALESCE(v_source_row_count, 0),
      'line_work_count', COALESCE(v_line_work_count, 0),
      'authority_scope_digest', v_authority_scope_digest
    );
  END IF;

  IF COALESCE(v_source_row_count, 0) > 0
     AND (
       v_source_change_seq_min IS DISTINCT FROM v_current_source_change_seq
       OR v_source_change_seq_max IS DISTINCT FROM v_current_source_change_seq
     ) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_CHANGE_SEQUENCE_STALE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'current_source_change_seq', COALESCE(v_current_source_change_seq, 0),
      'source_change_seq_min', v_source_change_seq_min,
      'source_change_seq_max', v_source_change_seq_max,
      'authority_scope_digest', v_authority_scope_digest
    );
  END IF;

  IF v_source_session.source_snapshot_run_id IS NULL
     OR v_target_session.source_snapshot_run_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_OR_TARGET_SNAPSHOT_MISSING',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_clone_target_snapshot_keys;
  CREATE TEMP TABLE _bpay_clone_target_snapshot_keys ON COMMIT DROP AS
  SELECT DISTINCT
    snapshot_line.timesheet_id,
    UPPER(BTRIM(COALESCE(snapshot_line.component_key_type, ''))) AS key_type,
    NULLIF(BTRIM(COALESCE(snapshot_line.component_key_value, '')), '') AS key_value,
    UPPER(BTRIM(COALESCE(snapshot_line.pay_channel, ''))) AS pay_channel
  FROM public.banking_pay_snapshot_line_state AS snapshot_line
  WHERE snapshot_line.snapshot_run_id = v_target_session.source_snapshot_run_id
    AND snapshot_line.candidate_id = p_candidate_id
    AND snapshot_line.timesheet_id IS NOT NULL
    AND NULLIF(BTRIM(COALESCE(snapshot_line.component_key_type, '')), '') IS NOT NULL
    AND NULLIF(BTRIM(COALESCE(snapshot_line.component_key_value, '')), '') IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_target_snapshot_key_count
  FROM pg_temp._bpay_clone_target_snapshot_keys AS target_snapshot_count;

  v_target_snapshot_empty_allowed := COALESCE(v_controlled_clone_rebase, false) IS TRUE
    AND COALESCE(v_target_snapshot_key_count, 0) = 0;

  IF COALESCE(v_target_snapshot_empty_allowed, false) IS TRUE THEN
    v_has_snapshot_scope_mismatch := false;
  ELSE
    SELECT EXISTS (
      SELECT 1
      FROM (
        SELECT
          source_preview.timesheet_id,
          UPPER(BTRIM(COALESCE(source_preview.key_type, ''))) AS key_type,
          NULLIF(BTRIM(COALESCE(source_preview.key_value, '')), '') AS key_value
        FROM pg_temp._bpay_clone_source_preview AS source_preview
        WHERE source_preview.timesheet_id IS NOT NULL
          AND source_preview.key_type IS NOT NULL
          AND source_preview.key_value IS NOT NULL
        EXCEPT
        SELECT
          target_snapshot.timesheet_id,
          target_snapshot.key_type,
          target_snapshot.key_value
        FROM pg_temp._bpay_clone_target_snapshot_keys AS target_snapshot
      ) AS source_minus_target
    ) OR EXISTS (
      SELECT 1
      FROM (
        SELECT
          target_snapshot.timesheet_id,
          target_snapshot.key_type,
          target_snapshot.key_value
        FROM pg_temp._bpay_clone_target_snapshot_keys AS target_snapshot
        EXCEPT
        SELECT
          source_preview.timesheet_id,
          UPPER(BTRIM(COALESCE(source_preview.key_type, ''))) AS key_type,
          NULLIF(BTRIM(COALESCE(source_preview.key_value, '')), '') AS key_value
        FROM pg_temp._bpay_clone_source_preview AS source_preview
        WHERE source_preview.timesheet_id IS NOT NULL
          AND source_preview.key_type IS NOT NULL
          AND source_preview.key_value IS NOT NULL
      ) AS target_minus_source
    )
    INTO v_has_snapshot_scope_mismatch;
  END IF;

  IF v_has_snapshot_scope_mismatch IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'TARGET_SNAPSHOT_SCOPE_DIFFERS',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'controlled_clone_rebase', COALESCE(v_controlled_clone_rebase, false),
      'target_snapshot_key_count', COALESCE(v_target_snapshot_key_count, 0),
      'target_snapshot_empty_allowed', COALESCE(v_target_snapshot_empty_allowed, false)
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_finance_case_components AS component_row
    WHERE component_row.candidate_id = p_candidate_id
      AND component_row.closed_at_utc IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_advances AS advance_row
    WHERE advance_row.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(advance_row.status::text, ''))) IN ('ACTIVE', 'PAUSED')
      AND advance_row.cleared_at_utc IS NULL
      AND advance_row.written_off_at_utc IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.candidate_id = p_candidate_id
      AND override_row.consumed_at_utc IS NULL
      AND override_row.cleared_at_utc IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.ts_pay_adjustments AS adjustment_row
    WHERE adjustment_row.candidate_id = p_candidate_id
      AND adjustment_row.paid_at_utc IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id IN (p_source_session_id, p_target_session_id)
      AND resolution_row.candidate_id = p_candidate_id
  )
  INTO v_has_complexity;

  IF v_has_complexity IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'FINANCE_COMPLEXITY_REQUIRES_CURRENT_LIVE_REFRESH',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'authority_timesheet_count', COALESCE(v_authority_timesheet_count, 0),
      'authority_scope_digest', v_authority_scope_digest,
      'current_source_change_seq', COALESCE(v_current_source_change_seq, 0)
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_clone_source_preview AS source_preview
    WHERE UPPER(BTRIM(COALESCE(source_preview.row_json->>'pay_channel', ''))) NOT IN ('PAYE', 'UMBRELLA')
       OR (
         UPPER(BTRIM(COALESCE(source_preview.row_json->>'pay_channel', ''))) = 'PAYE'
         AND (
           UPPER(BTRIM(COALESCE(v_candidate_row.pay_method, ''))) <> 'PAYE'
           OR NULLIF(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,payee_bank_hash}', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_candidate_row.bank_details_hash, '')), '')
         )
       )
       OR (
         UPPER(BTRIM(COALESCE(source_preview.row_json->>'pay_channel', ''))) = 'UMBRELLA'
         AND (
           UPPER(BTRIM(COALESCE(v_candidate_row.pay_method, ''))) <> 'UMBRELLA'
           OR v_candidate_row.umbrella_id IS NULL
           OR v_umbrella_enabled IS NOT TRUE
           OR NULLIF(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,payee_bank_hash}', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_umbrella_bank_hash, '')), '')
         )
       )
  )
  INTO v_has_pay_method_or_bank_mismatch;

  IF v_has_pay_method_or_bank_mismatch IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'PAYE_UMBRELLA_SWITCH_OR_BANK_ROUTING_CHANGE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_clone_source_preview AS source_preview
    CROSS JOIN LATERAL (
      SELECT
        CASE WHEN UPPER(BTRIM(COALESCE(v_candidate_row.pay_method, ''))) = 'PAYE' THEN 'CANDIDATE' ELSE 'UMBRELLA' END AS payee_entity_kind,
        CASE WHEN UPPER(BTRIM(COALESCE(v_candidate_row.pay_method, ''))) = 'PAYE' THEN v_candidate_row.id ELSE v_candidate_row.umbrella_id END AS payee_entity_id,
        CASE WHEN UPPER(BTRIM(COALESCE(v_candidate_row.pay_method, ''))) = 'PAYE' THEN NULLIF(BTRIM(COALESCE(v_candidate_row.bank_details_hash, '')), '') ELSE NULLIF(BTRIM(COALESCE(v_umbrella_bank_hash, '')), '') END AS payee_bank_hash
    ) AS current_payee
    LEFT JOIN LATERAL (
      SELECT bank_name_check_row.status,
             (bank_name_check_row.override_reason IS NOT NULL
              AND bank_name_check_row.override_hash IS NOT DISTINCT FROM current_payee.payee_bank_hash) AS has_override
      FROM public.bank_name_checks AS bank_name_check_row
      WHERE bank_name_check_row.rail_provider = v_rail_provider
        AND bank_name_check_row.rail_env = v_rail_env
        AND bank_name_check_row.entity_kind = current_payee.payee_entity_kind
        AND bank_name_check_row.entity_id = current_payee.payee_entity_id
        AND bank_name_check_row.bank_details_hash IS NOT DISTINCT FROM current_payee.payee_bank_hash
      ORDER BY bank_name_check_row.checked_at_utc DESC NULLS LAST,
               bank_name_check_row.updated_at_utc DESC,
               bank_name_check_row.created_at_utc DESC,
               bank_name_check_row.id DESC
      LIMIT 1
    ) AS current_name_check ON true
    LEFT JOIN LATERAL (
      SELECT bank_payee_map_row.payee_id
      FROM public.bank_payee_map AS bank_payee_map_row
      WHERE bank_payee_map_row.rail_provider = v_rail_provider
        AND bank_payee_map_row.rail_env = v_rail_env
        AND bank_payee_map_row.entity_kind = current_payee.payee_entity_kind
        AND bank_payee_map_row.entity_id = current_payee.payee_entity_id
        AND bank_payee_map_row.bank_details_hash IS NOT DISTINCT FROM current_payee.payee_bank_hash
      ORDER BY bank_payee_map_row.updated_at_utc DESC,
               bank_payee_map_row.created_at_utc DESC,
               bank_payee_map_row.id DESC
      LIMIT 1
    ) AS current_payee_map ON true
    WHERE (
        v_need_name_check IS TRUE
        AND NULLIF(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,name_check_status}', '')), '') IS NOT NULL
        AND UPPER(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,name_check_status}', 'UNVERIFIED'))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(current_name_check.status, 'UNVERIFIED')))
      )
       OR (
        v_need_name_check IS TRUE
        AND NULLIF(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,name_check_has_override}', '')), '') IS NOT NULL
        AND (lower(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,name_check_has_override}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM COALESCE(current_name_check.has_override, false)
      )
       OR (
        (v_requires_payee_map IS TRUE OR NULLIF(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,payee_map_present}', '')), '') IS NOT NULL)
        AND (lower(BTRIM(COALESCE(source_preview.row_json#>>'{payee_context,payee_map_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM (current_payee_map.payee_id IS NOT NULL)
      )
  )
  INTO v_has_readiness_mismatch;

  IF v_has_readiness_mismatch IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'PAYEE_READINESS_FINGERPRINT_CHANGED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_clone_outstanding_now;
  CREATE TEMP TABLE _bpay_clone_outstanding_now ON COMMIT DROP AS
  SELECT outstanding_row.*
  FROM public._pay_outstanding_components(v_timesheet_ids, NULL::uuid) AS outstanding_row;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) <> 0)::integer,
         COUNT(*) FILTER (WHERE ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) < 0)::integer
  INTO v_outstanding_key_count,
       v_nonzero_outstanding_count,
       v_negative_outstanding_count
  FROM pg_temp._bpay_clone_outstanding_now AS outstanding_now;

  SELECT COUNT(*)::integer
  INTO v_active_settled_key_count
  FROM public._pay_active_settled_components(v_timesheet_ids) AS settled_component;

  SELECT COUNT(*)::integer
  INTO v_active_reserved_key_count
  FROM public._pay_reserved_components(v_timesheet_ids, NULL::uuid) AS reserved_component;

  SELECT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_clone_source_preview AS source_preview
    LEFT JOIN pg_temp._bpay_clone_outstanding_now AS outstanding_now
      ON outstanding_now.timesheet_id = source_preview.timesheet_id
     AND outstanding_now.key_type = source_preview.key_type
     AND outstanding_now.key_value = source_preview.key_value
    WHERE outstanding_now.timesheet_id IS NULL
       OR COALESCE(outstanding_now.reservation_overrun_detected, false) IS TRUE
       OR ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) IS DISTINCT FROM ROUND(COALESCE(CASE WHEN COALESCE(source_preview.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (source_preview.row_json->>'amount_ex_vat')::numeric ELSE NULL::numeric END, 0), 2)
       OR ROUND(COALESCE(outstanding_now.outstanding_inc_vat, 0), 2) IS DISTINCT FROM ROUND(COALESCE(CASE WHEN COALESCE(source_preview.row_json->>'amount_inc_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (source_preview.row_json->>'amount_inc_vat')::numeric ELSE NULL::numeric END, 0), 2)
  )
  INTO v_has_outstanding_mismatch;

  SELECT COUNT(*) FILTER (WHERE ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) < 0)::integer,
         COUNT(*) FILTER (WHERE ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) > 0)::integer
  INTO v_unrepresented_negative_count,
       v_unrepresented_positive_count
  FROM pg_temp._bpay_clone_outstanding_now AS outstanding_now
  LEFT JOIN pg_temp._bpay_clone_source_preview AS source_preview
    ON source_preview.timesheet_id = outstanding_now.timesheet_id
   AND source_preview.key_type = outstanding_now.key_type
   AND source_preview.key_value = outstanding_now.key_value
  WHERE source_preview.id IS NULL
    AND COALESCE(outstanding_now.reservation_overrun_detected, false) IS NOT TRUE
    AND ROUND(COALESCE(outstanding_now.outstanding_ex_vat, 0), 2) <> 0;

  v_has_extra_outstanding_key := COALESCE(v_unrepresented_negative_count, 0) > 0
    OR COALESCE(v_unrepresented_positive_count, 0) > 0;

  IF v_has_outstanding_mismatch IS TRUE OR v_has_extra_outstanding_key IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', CASE
        WHEN COALESCE(v_unrepresented_negative_count, 0) > 0 THEN 'HIDDEN_NEGATIVE_OUTSTANDING_REQUIRES_LIVE_REFRESH'
        WHEN COALESCE(v_unrepresented_positive_count, 0) > 0 THEN 'HIDDEN_POSITIVE_OUTSTANDING_REQUIRES_LIVE_REFRESH'
        ELSE 'OUTSTANDING_OR_RESERVATION_MISMATCH'
      END,
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'unrepresented_negative_count', COALESCE(v_unrepresented_negative_count, 0),
      'unrepresented_positive_count', COALESCE(v_unrepresented_positive_count, 0),
      'negative_outstanding_count', COALESCE(v_negative_outstanding_count, 0),
      'nonzero_outstanding_count', COALESCE(v_nonzero_outstanding_count, 0),
      'authority_scope_digest', v_authority_scope_digest
    );
  END IF;

  SELECT COALESCE(v_preview_count, 0) > 0 AND EXISTS (
    SELECT 1
    FROM public.timesheets AS timesheet_row
    LEFT JOIN public.timesheets_financials AS tsfin_row
      ON tsfin_row.timesheet_id = timesheet_row.timesheet_id
     AND tsfin_row.is_current = true
    WHERE EXISTS (
      SELECT 1
      FROM pg_temp._bpay_clone_source_preview AS source_preview
      WHERE source_preview.timesheet_id = timesheet_row.timesheet_id
    )
      AND (
        COALESCE(timesheet_row.is_current, false) IS NOT TRUE
        OR timesheet_row.updated_at > COALESCE(v_source_session.progress_updated_at_utc, v_source_session.updated_at_utc, v_source_session.created_at_utc)
        OR COALESCE(tsfin_row.updated_at, tsfin_row.created_at, '-infinity'::timestamptz) > COALESCE(v_source_session.progress_updated_at_utc, v_source_session.updated_at_utc, v_source_session.created_at_utc)
      )
  )
  INTO v_has_entitlement_mismatch;

  IF v_has_entitlement_mismatch IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'TIMESHEET_OR_FINANCE_FINGERPRINT_CHANGED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_clone_source_preview AS source_preview
    WHERE COALESCE((public.pay_workbench_preview_line_contract_ok(
      source_preview.row_json,
      jsonb_build_object(
        'timesheet_id', source_preview.timesheet_id::text,
        'key_type', source_preview.key_type,
        'key_value', source_preview.key_value
      ),
      source_preview.section
    )->>'ok')::boolean, false) IS NOT TRUE
  )
  INTO v_has_contract_mismatch;

  IF v_has_contract_mismatch IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'PREVIEW_CONTRACT_MISMATCH',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT
    EXISTS (
      SELECT 1
      FROM public.pay_item_snoozes AS snooze_row
      WHERE snooze_row.candidate_id = p_candidate_id
        AND snooze_row.cleared_at_utc IS NULL
        AND snooze_row.cancelled_at_utc IS NULL
    ),
    EXISTS (
      SELECT 1
      FROM public.pay_item_snoozes AS snooze_row
      WHERE snooze_row.candidate_id = p_candidate_id
        AND snooze_row.cleared_at_utc IS NULL
        AND snooze_row.cancelled_at_utc IS NULL
        AND (
          snooze_row.snooze_until_date IS NULL
          OR snooze_row.snooze_until_date >= v_today_uk
        )
    )
  INTO v_has_snooze_target_date_risk,
       v_has_active_snooze_risk;

  IF v_has_snooze_target_date_risk IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', CASE
        WHEN v_has_active_snooze_risk IS TRUE THEN 'ACTIVE_SNOOZE_REQUIRES_LIVE_REFRESH'
        ELSE 'SNOOZE_STATE_REQUIRES_LIVE_REFRESH'
      END,
      'london_current_date', v_today_uk::text,
      'active_snooze_present', COALESCE(v_has_active_snooze_risk, false),
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT COALESCE(app_counter.seq, 0)
  INTO v_final_source_change_seq
  FROM (SELECT 1) AS final_authority_anchor
  LEFT JOIN public.app_change_counters AS app_counter
    ON app_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF v_final_source_change_seq IS DISTINCT FROM v_current_source_change_seq THEN
    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'CANDIDATE_CHANGED_DURING_CLONE_CERTIFICATION',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'initial_source_change_seq', COALESCE(v_current_source_change_seq, 0),
      'final_source_change_seq', COALESCE(v_final_source_change_seq, 0),
      'authority_scope_digest', v_authority_scope_digest
    );
  END IF;

  IF COALESCE(v_preview_count, 0) = 0 THEN
    IF UPPER(BTRIM(COALESCE(v_source_scope.status, ''))) = 'SOURCE_EMPTY'
       AND COALESCE(v_source_row_count, 0) = 0
       AND COALESCE(v_line_work_count, 0) = 0
       AND COALESCE(v_all_preview_count, 0) = 0
       AND COALESCE(v_outstanding_key_count, 0) = 0
       AND COALESCE(v_nonzero_outstanding_count, 0) = 0
       AND COALESCE(v_active_settled_key_count, 0) = 0
       AND COALESCE(v_active_reserved_key_count, 0) = 0
       AND COALESCE(v_has_complexity, false) IS NOT TRUE
       AND COALESCE(v_has_snooze_target_date_risk, false) IS NOT TRUE THEN
      RETURN jsonb_build_object(
        'ok', true,
        'clone_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN false ELSE true END,
        'target_date_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN true ELSE NULL::boolean END,
        'reason', NULL::text,
        'ready_empty', true,
        'controlled_clone_rebase', COALESCE(v_controlled_clone_rebase, false),
        'target_scope_missing_allowed', COALESCE(v_target_scope_missing_allowed, false),
        'target_snapshot_key_count', COALESCE(v_target_snapshot_key_count, 0),
        'target_snapshot_empty_allowed', COALESCE(v_target_snapshot_empty_allowed, false),
        'authority_timesheet_count', COALESCE(v_authority_timesheet_count, 0),
        'authority_scope_digest', v_authority_scope_digest,
        'current_source_change_seq', COALESCE(v_current_source_change_seq, 0),
        'current_live_empty_proof', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'reason', 'SOURCE_EMPTY_NOT_PROVEN_BY_CURRENT_LIVE_AUTHORITY',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'source_scope_status', v_source_scope.status,
      'authority_timesheet_count', COALESCE(v_authority_timesheet_count, 0),
      'outstanding_key_count', COALESCE(v_outstanding_key_count, 0),
      'nonzero_outstanding_count', COALESCE(v_nonzero_outstanding_count, 0),
      'active_settled_key_count', COALESCE(v_active_settled_key_count, 0),
      'active_reserved_key_count', COALESCE(v_active_reserved_key_count, 0),
      'authority_scope_digest', v_authority_scope_digest,
      'current_source_change_seq', COALESCE(v_current_source_change_seq, 0)
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'clone_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN false ELSE true END,
    'target_date_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN true ELSE NULL::boolean END,
    'reason', NULL::text,
    'source_preview_row_count', COALESCE(v_preview_count, 0),
    'source_row_count', COALESCE(v_source_row_count, 0),
    'line_work_count', COALESCE(v_line_work_count, 0),
    'authority_timesheet_count', COALESCE(v_authority_timesheet_count, 0),
    'authority_scope_digest', v_authority_scope_digest,
    'current_source_change_seq', COALESCE(v_current_source_change_seq, 0),
    'outstanding_key_count', COALESCE(v_outstanding_key_count, 0),
    'nonzero_outstanding_count', COALESCE(v_nonzero_outstanding_count, 0),
    'negative_outstanding_count', COALESCE(v_negative_outstanding_count, 0),
    'controlled_clone_rebase', COALESCE(v_controlled_clone_rebase, false),
    'target_scope_missing_allowed', COALESCE(v_target_scope_missing_allowed, false),
    'target_snapshot_key_count', COALESCE(v_target_snapshot_key_count, 0),
    'target_snapshot_empty_allowed', COALESCE(v_target_snapshot_empty_allowed, false),
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;


ALTER FUNCTION public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb) TO postgres,authenticated,service_role;
