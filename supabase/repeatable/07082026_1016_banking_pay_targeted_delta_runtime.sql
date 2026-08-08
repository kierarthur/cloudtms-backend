-- Banking Pay targeted lifecycle delta runtime.
-- Exact current definitions extracted from the authoritative monolith before targeted amendments.

CREATE OR REPLACE FUNCTION public.pay_workbench_authorise_delta_hotkey_preflight(p_session_id uuid, p_candidate_id uuid, p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_linked_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_payload_json jsonb DEFAULT '{}'::jsonb, p_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_source_change_seq bigint DEFAULT NULL::bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_payload jsonb := CASE WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_family_scope jsonb := '{}'::jsonb;
  v_targeted_json jsonb := '[]'::jsonb;
  v_linked_json jsonb := '[]'::jsonb;
  v_queue_targeted_json jsonb := '[]'::jsonb;
  v_queue_linked_json jsonb := '[]'::jsonb;
  v_requested_json jsonb := '[]'::jsonb;
  v_family_key text := NULL::text;
  v_delta_hash text := NULL::text;
  v_latest_seq bigint := 0;
  v_payload_seq bigint := 0;
  v_live_seq bigint := 0;
  v_existing_queued public.banking_pay_workbench_jobs%ROWTYPE;
  v_running public.banking_pay_workbench_jobs%ROWTYPE;
  v_existing_seq bigint := 0;
  v_existing_event_count integer := 0;
  v_new_event_count integer := 1;
  v_new_projection_run_id uuid := gen_random_uuid();
  v_dedupe_key text := NULL::text;
  v_payload_new jsonb := '{}'::jsonb;
  v_job_id uuid := NULL::uuid;
  v_job_status text := NULL::text;
  v_inserted boolean := false;
  v_insert_count integer := 0;
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), NULLIF(BTRIM(COALESCE(v_payload->>'reason_latest', v_payload->>'reason', '')), ''), 'AUTHORISE_UNAUTHORISE_DELTA_PREFLIGHT');
  v_lifecycle_context text := LOWER(BTRIM(COALESCE(v_payload->>'lifecycle_mutation_context', v_payload->>'mutation_context', v_payload->>'lifecycle_context', '')));
  v_is_authorise_boundary boolean := false;
  v_is_archive_boundary boolean := false;
  v_is_lifecycle_boundary boolean := false;
  v_is_ordinary_save_no_dirty boolean := false;
  v_event_class text := NULL::text;
  v_admission jsonb := '{}'::jsonb;
  v_settings_json jsonb := '{}'::jsonb;
  v_lifecycle_fast_enabled boolean := false;
  v_projection_class text := 'NORMAL_TIMESHEET';
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL THEN
    RETURN jsonb_build_object('ok', true, 'action', 'PROCEED', 'match_found', false, 'reason', 'SESSION_OR_CANDIDATE_REQUIRED');
  END IF;

  v_is_authorise_boundary :=
    lower(BTRIM(COALESCE(v_payload->>'authorise_boundary_changed', v_payload->>'timesheet_authorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
    OR lower(BTRIM(COALESCE(v_payload->>'unauthorise_boundary_changed', v_payload->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
    OR v_lifecycle_context IN ('timesheet_authorise', 'authorise_timesheet', 'timesheet_unauthorise', 'unauthorise_timesheet')
    OR LOWER(COALESCE(v_reason, '')) LIKE '%authorise%'
    OR LOWER(COALESCE(v_reason, '')) LIKE '%unauthorise%';

  v_is_archive_boundary :=
    lower(BTRIM(COALESCE(v_payload->>'archive_boundary_changed', v_payload->>'timesheet_archive_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
    OR lower(BTRIM(COALESCE(v_payload->>'unarchive_boundary_changed', v_payload->>'timesheet_unarchive_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
    OR v_lifecycle_context IN ('timesheet_archive', 'archive_timesheet', 'timesheet_unarchive', 'unarchive_timesheet')
    OR LOWER(COALESCE(v_reason, '')) LIKE '%archive%'
    OR LOWER(COALESCE(v_reason, '')) LIKE '%unarchive%';

  v_is_lifecycle_boundary := COALESCE(v_is_authorise_boundary, false) OR COALESCE(v_is_archive_boundary, false);
  v_is_ordinary_save_no_dirty := lower(BTRIM(COALESCE(v_payload->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true','t','1','yes','y','on');

  IF v_is_archive_boundary IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'PROCEED',
      'match_found', false,
      'reason', 'TIMESHEET_ARCHIVE_LIFECYCLE_REQUIRES_SOURCE_BUILD',
      'authorise_boundary_changed', COALESCE(v_is_authorise_boundary, false),
      'archive_boundary_changed', COALESCE(v_is_archive_boundary, false),
      'source_build_required', true,
      'classifier_work_skipped', false,
      'dirty_marking_skipped', false,
      'session_progress_dirtying_skipped', false,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  IF v_is_ordinary_save_no_dirty IS TRUE
     OR COALESCE(array_length(COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[]), 1), 0) = 0 THEN
    RETURN jsonb_build_object('ok', true, 'action', 'PROCEED', 'match_found', false, 'reason', 'NOT_TARGETED_AUTHORISE_UNAUTHORISE_DELTA');
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
    AND session_row.status = 'OPEN'
    AND session_row.discarded_at_utc IS NULL;

  IF v_session_row.id IS NULL THEN
    RETURN jsonb_build_object('ok', true, 'action', 'PROCEED', 'match_found', false, 'reason', 'NO_OPEN_WORKBENCH_SESSION');
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_payload_seq := GREATEST(
    COALESCE(p_source_change_seq, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'latest_source_change_seq', '') ~ '^\d{1,18}$' THEN (v_payload->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_seq', '') ~ '^\d{1,18}$' THEN (v_payload->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_sequence', '') ~ '^\d{1,18}$' THEN (v_payload->>'source_change_sequence')::bigint END, 0)
  );
  v_latest_seq := GREATEST(COALESCE(v_payload_seq, 0), COALESCE(v_live_seq, 0));

  IF v_is_authorise_boundary THEN
    v_event_class := CASE
      WHEN lower(BTRIM(COALESCE(v_payload->>'unauthorise_boundary_changed',v_payload->>'timesheet_unauthorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
        OR v_lifecycle_context IN ('timesheet_unauthorise','unauthorise_timesheet')
        OR lower(COALESCE(v_reason,'')) LIKE '%unauthorise%'
      THEN 'UNAUTHORISE'
      ELSE 'AUTHORISE'
    END;
    v_admission := private.pay_workbench_targeted_delta_admission_v1(
      p_session_id,p_candidate_id,NULL,v_event_class,
      COALESCE(p_targeted_timesheet_ids,ARRAY[]::uuid[]),
      COALESCE(p_linked_timesheet_ids,ARRAY[]::uuid[]),v_latest_seq
    );
    SELECT to_jsonb(settings_row) INTO v_settings_json
    FROM public.settings_defaults AS settings_row WHERE settings_row.id=1;
    v_lifecycle_fast_enabled := CASE v_event_class
      WHEN 'AUTHORISE' THEN lower(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_simple_authorise','false')) IN ('true','t','1','yes','on')
      WHEN 'UNAUTHORISE' THEN lower(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_simple_unauthorise','false')) IN ('true','t','1','yes','on')
      ELSE false
    END;
    IF COALESCE((v_admission->>'admitted')::boolean,false) IS NOT TRUE
       OR v_lifecycle_fast_enabled IS NOT TRUE THEN
      RETURN jsonb_build_object(
        'ok',true,'action','PROCEED','match_found',false,
        'reason',CASE WHEN COALESCE((v_admission->>'admitted')::boolean,false)
          THEN 'TARGETED_LIFECYCLE_FAST_ROUTE_DISABLED'
          ELSE COALESCE(v_admission->>'reason','TARGETED_LIFECYCLE_REQUIRES_SOURCE_BUILD') END,
        'source_build_required',true,
        'would_fast_path_allowed',COALESCE((v_admission->>'admitted')::boolean,false),
        'fast_path_allowed',false,
        'lifecycle_event_class',v_event_class,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
      );
    END IF;
    v_projection_class := 'TIMESHEET_LIFECYCLE';
  END IF;

  v_family_scope := public._pay_workbench_normalise_timesheet_rotation_scope_payload(
    COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[]),
    COALESCE(p_linked_timesheet_ids, ARRAY[]::uuid[])
  );
  v_targeted_json := COALESCE(v_family_scope->'targeted_timesheet_ids', '[]'::jsonb);
  v_linked_json := COALESCE(v_family_scope->'linked_timesheet_ids', '[]'::jsonb);
  v_queue_targeted_json := COALESCE(v_family_scope->'queue_identity_targeted_timesheet_ids', v_family_scope->'family_timesheet_ids', v_targeted_json, '[]'::jsonb);
  v_queue_linked_json := COALESCE(v_family_scope->'queue_identity_linked_timesheet_ids', '[]'::jsonb);
  v_requested_json := COALESCE(v_family_scope->'requested_timesheet_ids', v_targeted_json);

  v_delta_hash := md5(COALESCE(v_queue_targeted_json, '[]'::jsonb)::text || ':' || COALESCE(v_queue_linked_json, '[]'::jsonb)::text);
  v_family_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
    || ':session:' || p_session_id::text
    || ':version:' || COALESCE(COALESCE(v_session_row.version, 0), 0)::text
    || ':projection_mode:DELTA'
    || ':projection_class:' || v_projection_class
    || ':refresh_scope:TARGETED_TIMESHEETS'
    || ':candidate:' || p_candidate_id::text
    || ':timesheets:' || v_delta_hash;

  PERFORM pg_advisory_xact_lock(hashtextextended(v_family_key, 24062026));

  SELECT queued_job.*
  INTO v_existing_queued
  FROM public.banking_pay_workbench_jobs AS queued_job
  WHERE queued_job.session_id = p_session_id
    AND queued_job.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
    AND UPPER(BTRIM(COALESCE(queued_job.status, ''))) = 'QUEUED'
    AND (
      queued_job.dedupe_key = v_family_key
      OR queued_job.dedupe_key LIKE v_family_key || ':waiting_after_running:%'
      OR COALESCE(queued_job.payload_json->>'normalised_delta_family_key', queued_job.payload_json->>'delta_family_key', queued_job.payload_json->>'delta_coalescing_key', '') = v_family_key
    )
  ORDER BY CASE WHEN queued_job.dedupe_key = v_family_key THEN 0 ELSE 1 END,
           queued_job.run_at_utc ASC,
           queued_job.created_at_utc ASC,
           queued_job.id ASC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_queued.id IS NOT NULL THEN
    v_existing_seq := GREATEST(
      COALESCE(CASE WHEN COALESCE(v_existing_queued.payload_json->>'latest_source_change_seq', '') ~ '^\d{1,18}$' THEN (v_existing_queued.payload_json->>'latest_source_change_seq')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_existing_queued.payload_json->>'source_change_seq', '') ~ '^\d{1,18}$' THEN (v_existing_queued.payload_json->>'source_change_seq')::bigint END, 0),
      COALESCE(CASE WHEN COALESCE(v_existing_queued.payload_json->>'source_change_sequence', '') ~ '^\d{1,18}$' THEN (v_existing_queued.payload_json->>'source_change_sequence')::bigint END, 0)
    );
    v_existing_event_count := CASE WHEN COALESCE(v_existing_queued.payload_json->>'coalesced_event_count', '') ~ '^\d{1,9}$' THEN GREATEST((v_existing_queued.payload_json->>'coalesced_event_count')::integer, 1) ELSE 1 END;
    v_new_event_count := v_existing_event_count + 1;
    v_latest_seq := GREATEST(v_latest_seq, v_existing_seq);
    v_dedupe_key := CASE WHEN v_existing_queued.dedupe_key LIKE v_family_key || ':waiting_after_running:%' THEN v_existing_queued.dedupe_key ELSE v_family_key END;

    v_payload_new := jsonb_strip_nulls(
      (
        public._pay_workbench_merge_targeted_scope_payload(COALESCE(v_existing_queued.payload_json, '{}'::jsonb), COALESCE(v_payload, '{}'::jsonb))
        - 'cursor' - 'cursor_json' - 'next_cursor' - 'next_cursor_json' - 'source_cursor' - 'write_cursor_json'
        - 'candidate_cursor' - 'cursor_token' - 'has_cursor' - 'continuation_reason' - 'source_job_id'
        - 'continuation_source_job_id' - 'bounded_continuation_source_job_id' - 'parent_job_id'
        - 'next_phase' - 'write_phase' - 'source_result_summary' - 'source_result_has_more' - 'source_result_next_cursor_present'
        - 'old_row_json' - 'new_row_json' - 'source_row_json' - 'work_payload_json' - 'result_row_json'
        - 'preview_row_json' - 'projection_rows' - 'projected_rows'
      )
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'projection_mode', 'DELTA',
        'projection_class', v_projection_class,
        'lifecycle_event_class', v_event_class,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'phase', 'INIT_PREFLIGHT',
        'run_mode', 'LATEST_STATE_HEAD',
        'continuation', false,
        'cursor_json', '{}'::jsonb,
        'projection_run_id', v_new_projection_run_id::text,
        'session_id', p_session_id::text,
        'source_session_id', p_session_id::text,
        'workbench_session_id', p_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'candidate_id', p_candidate_id::text,
        'targeted_timesheet_ids', v_targeted_json,
        'linked_timesheet_ids', v_linked_json,
        'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
        'queue_identity_linked_timesheet_ids', v_queue_linked_json,
        'family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
        'requested_timesheet_ids', v_requested_json,
        'targeted_timesheet_ids_requested', COALESCE(v_family_scope->'requested_targeted_timesheet_ids', v_requested_json),
        'linked_timesheet_ids_requested', COALESCE(v_family_scope->'requested_linked_timesheet_ids', '[]'::jsonb),
        'source_change_seq', v_latest_seq,
        'source_change_sequence', v_latest_seq,
        'latest_source_change_seq', v_latest_seq,
        'delta_coalescing_key', v_family_key,
        'delta_family_key', v_family_key,
        'normalised_delta_family_key', v_family_key,
        'delta_coalescing_hash', md5(v_family_key),
        'coalesced_event_count', v_new_event_count,
        'coalesced_source_change_seqs', jsonb_build_array(v_existing_seq, v_latest_seq),
        'latest_event_at_utc', v_now::text,
        'reason', v_reason,
        'early_preflight_action', 'REUSED_QUEUED_SAME_FAMILY_JOB',
        'dirty_marking_skipped', true,
        'session_progress_dirtying_skipped', true,
        'classifier_work_skipped_by_dirty_apply_preflight', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );

    UPDATE public.banking_pay_workbench_jobs AS job_update
    SET dedupe_key = v_dedupe_key,
        priority = LEAST(job_update.priority, 43),
        run_at_utc = LEAST(job_update.run_at_utc, v_now),
        payload_json = v_payload_new,
        updated_at_utc = v_now,
        started_at_utc = NULL,
        completed_at_utc = NULL,
        failed_at_utc = NULL,
        last_error_json = NULL::jsonb
    WHERE job_update.id = v_existing_queued.id
    RETURNING job_update.id, job_update.status
    INTO v_job_id, v_job_status;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_job_id::text,
      'EARLY_PREFLIGHT_REUSED_QUEUED_SAME_FAMILY_JOB',
      NULL::jsonb,
      jsonb_build_object(
        'id', v_job_id::text,
        'session_id', p_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'candidate_id', p_candidate_id::text,
        'targeted_timesheet_ids', v_targeted_json,
        'linked_timesheet_ids', v_linked_json,
        'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
        'queue_identity_linked_timesheet_ids', v_queue_linked_json,
        'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
        'requested_timesheet_ids', v_requested_json,
        'normalised_delta_family_key', v_family_key,
        'existing_job_id', v_existing_queued.id::text,
        'latest_source_change_seq', v_latest_seq,
        'previous_job_source_change_seq', v_existing_seq,
        'coalesced_event_count', v_new_event_count,
        'dirty_marking_skipped', true,
        'session_progress_dirtying_skipped', true,
        'classifier_work_skipped', true,
        'trigger_reason', v_reason,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      'EARLY_PREFLIGHT_REUSED_QUEUED_SAME_FAMILY_JOB',
      COALESCE(p_actor_user_id, v_session_row.actor_user_id)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'action', 'REUSED_QUEUED_SAME_FAMILY_JOB',
      'match_found', true,
      'job_id', v_job_id::text,
      'status', v_job_status,
      'candidate_id', p_candidate_id::text,
      'session_id', p_session_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'targeted_timesheet_ids', v_targeted_json,
      'linked_timesheet_ids', v_linked_json,
      'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
      'queue_identity_linked_timesheet_ids', v_queue_linked_json,
      'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
      'normalised_delta_family_key', v_family_key,
      'latest_source_change_seq', v_latest_seq,
      'coalesced_event_count', v_new_event_count,
      'dirty_marking_skipped', true,
      'session_progress_dirtying_skipped', true,
      'classifier_work_skipped', true,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'dirty_apply_can_complete_cleanly', true
    );
  END IF;

  SELECT running_job.*
  INTO v_running
  FROM public.banking_pay_workbench_jobs AS running_job
  WHERE running_job.session_id = p_session_id
    AND running_job.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(running_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
    AND UPPER(BTRIM(COALESCE(running_job.status, ''))) = 'RUNNING'
    AND (
      running_job.dedupe_key = v_family_key
      OR COALESCE(running_job.payload_json->>'normalised_delta_family_key', running_job.payload_json->>'delta_family_key', running_job.payload_json->>'delta_coalescing_key', '') = v_family_key
    )
  ORDER BY running_job.started_at_utc ASC NULLS LAST, running_job.created_at_utc ASC, running_job.id ASC
  LIMIT 1;

  IF v_running.id IS NOT NULL THEN
    v_dedupe_key := v_family_key || ':waiting_after_running:' || v_running.id::text;
    v_payload_new := jsonb_strip_nulls(
      (COALESCE(v_payload, '{}'::jsonb)
        - 'cursor' - 'cursor_json' - 'next_cursor' - 'next_cursor_json' - 'source_cursor' - 'write_cursor_json'
        - 'candidate_cursor' - 'cursor_token' - 'has_cursor' - 'continuation_reason' - 'source_job_id'
        - 'continuation_source_job_id' - 'bounded_continuation_source_job_id' - 'parent_job_id'
        - 'next_phase' - 'write_phase' - 'source_result_summary' - 'source_result_has_more' - 'source_result_next_cursor_present'
        - 'old_row_json' - 'new_row_json' - 'source_row_json' - 'work_payload_json' - 'result_row_json'
        - 'preview_row_json' - 'projection_rows' - 'projected_rows')
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'projection_mode', 'DELTA',
        'projection_class', v_projection_class,
        'lifecycle_event_class', v_event_class,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'phase', 'INIT_PREFLIGHT',
        'run_mode', 'LATEST_RERUN_AFTER_RUNNING',
        'continuation', false,
        'cursor_json', '{}'::jsonb,
        'projection_run_id', v_new_projection_run_id::text,
        'session_id', p_session_id::text,
        'source_session_id', p_session_id::text,
        'workbench_session_id', p_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'candidate_id', p_candidate_id::text,
        'targeted_timesheet_ids', v_targeted_json,
        'linked_timesheet_ids', v_linked_json,
        'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
        'queue_identity_linked_timesheet_ids', v_queue_linked_json,
        'family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
        'requested_timesheet_ids', v_requested_json,
        'targeted_timesheet_ids_requested', COALESCE(v_family_scope->'requested_targeted_timesheet_ids', v_requested_json),
        'linked_timesheet_ids_requested', COALESCE(v_family_scope->'requested_linked_timesheet_ids', '[]'::jsonb),
        'source_change_seq', v_latest_seq,
        'source_change_sequence', v_latest_seq,
        'latest_source_change_seq', v_latest_seq,
        'delta_coalescing_key', v_family_key,
        'delta_family_key', v_family_key,
        'normalised_delta_family_key', v_family_key,
        'delta_coalescing_hash', md5(v_family_key),
        'dedupe_key', v_dedupe_key,
        'delta_active_running_job_id', v_running.id::text,
        'waiting_after_running_delta_job', true,
        'coalesced_event_count', 1,
        'coalesced_source_change_seqs', jsonb_build_array(v_latest_seq),
        'latest_event_at_utc', v_now::text,
        'reason', v_reason,
        'early_preflight_action', 'UPDATED_WAITING_AFTER_RUNNING_JOB',
        'dirty_marking_skipped', true,
        'session_progress_dirtying_skipped', true,
        'classifier_work_skipped_by_dirty_apply_preflight', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );

    INSERT INTO public.banking_pay_workbench_jobs AS waiting_job (
      job_type, status, priority, run_at_utc, attempt_count, max_attempts, dedupe_key,
      snapshot_run_id, session_id, candidate_id, payload_json,
      created_at_utc, updated_at_utc, started_at_utc, completed_at_utc, failed_at_utc, last_error_json
    )
    VALUES (
      'WORKBENCH_CANDIDATE_DELTA_REFRESH', 'QUEUED', 43, v_now, 0, 8, v_dedupe_key,
      v_session_row.source_snapshot_run_id, p_session_id, p_candidate_id, v_payload_new,
      v_now, v_now, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::jsonb
    )
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(waiting_job.priority, EXCLUDED.priority),
        run_at_utc = LEAST(waiting_job.run_at_utc, EXCLUDED.run_at_utc),
        payload_json = jsonb_strip_nulls(
          (public._pay_workbench_merge_targeted_scope_payload(COALESCE(waiting_job.payload_json, '{}'::jsonb), EXCLUDED.payload_json)
            - 'cursor' - 'cursor_json' - 'next_cursor' - 'next_cursor_json' - 'source_cursor' - 'write_cursor_json'
            - 'candidate_cursor' - 'cursor_token' - 'has_cursor' - 'continuation_reason' - 'source_job_id'
            - 'continuation_source_job_id' - 'bounded_continuation_source_job_id' - 'parent_job_id'
            - 'next_phase' - 'write_phase' - 'source_result_summary' - 'source_result_has_more' - 'source_result_next_cursor_present')
          || jsonb_build_object(
            'cursor_json', '{}'::jsonb,
            'continuation', false,
            'projection_run_id', v_new_projection_run_id::text,
            'phase', 'INIT_PREFLIGHT',
            'run_mode', 'LATEST_RERUN_AFTER_RUNNING',
            'latest_source_change_seq', v_latest_seq,
            'source_change_seq', v_latest_seq,
            'source_change_sequence', v_latest_seq,
            'normalised_delta_family_key', v_family_key,
            'delta_family_key', v_family_key,
            'delta_coalescing_key', v_family_key,
            'coalesced_event_count', CASE WHEN COALESCE(waiting_job.payload_json->>'coalesced_event_count', '') ~ '^\d{1,9}$' THEN (waiting_job.payload_json->>'coalesced_event_count')::integer + 1 ELSE 2 END,
            'latest_event_at_utc', v_now::text,
            'dirty_marking_skipped', true,
            'session_progress_dirtying_skipped', true
          )
        ),
        updated_at_utc = v_now,
        started_at_utc = NULL,
        completed_at_utc = NULL,
        failed_at_utc = NULL,
        last_error_json = NULL::jsonb
    WHERE UPPER(BTRIM(COALESCE(waiting_job.status, ''))) = 'QUEUED'
    RETURNING waiting_job.id, waiting_job.status, (xmax = 0)
    INTO v_job_id, v_job_status, v_inserted;

    GET DIAGNOSTICS v_insert_count = ROW_COUNT;

    IF COALESCE(v_insert_count, 0) = 0 THEN
      SELECT queued_job.id, queued_job.status
      INTO v_job_id, v_job_status
      FROM public.banking_pay_workbench_jobs AS queued_job
      WHERE queued_job.dedupe_key = v_dedupe_key
        AND UPPER(BTRIM(COALESCE(queued_job.status, ''))) = 'QUEUED'
      LIMIT 1;
    END IF;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      COALESCE(v_job_id::text, v_running.id::text),
      'EARLY_PREFLIGHT_UPDATED_WAITING_AFTER_RUNNING_JOB',
      NULL::jsonb,
      jsonb_build_object(
        'id', CASE WHEN v_job_id IS NULL THEN NULL ELSE v_job_id::text END,
        'running_job_id', v_running.id::text,
        'session_id', p_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'candidate_id', p_candidate_id::text,
        'targeted_timesheet_ids', v_targeted_json,
        'linked_timesheet_ids', v_linked_json,
        'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
        'queue_identity_linked_timesheet_ids', v_queue_linked_json,
        'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
        'normalised_delta_family_key', v_family_key,
        'latest_source_change_seq', v_latest_seq,
        'dirty_marking_skipped', true,
        'session_progress_dirtying_skipped', true,
        'classifier_work_skipped', true,
        'trigger_reason', v_reason,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      'EARLY_PREFLIGHT_UPDATED_WAITING_AFTER_RUNNING_JOB',
      COALESCE(p_actor_user_id, v_session_row.actor_user_id)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'action', 'UPDATED_WAITING_AFTER_RUNNING_JOB',
      'match_found', true,
      'job_id', CASE WHEN v_job_id IS NULL THEN NULL ELSE v_job_id::text END,
      'running_job_id', v_running.id::text,
      'status', COALESCE(v_job_status, 'QUEUED'),
      'candidate_id', p_candidate_id::text,
      'session_id', p_session_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'targeted_timesheet_ids', v_targeted_json,
      'linked_timesheet_ids', v_linked_json,
      'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
      'queue_identity_linked_timesheet_ids', v_queue_linked_json,
      'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
      'normalised_delta_family_key', v_family_key,
      'latest_source_change_seq', v_latest_seq,
      'dirty_marking_skipped', true,
      'session_progress_dirtying_skipped', true,
      'classifier_work_skipped', true,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'dirty_apply_can_complete_cleanly', true
    );
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    p_session_id::text || ':' || p_candidate_id::text,
    'EARLY_PREFLIGHT_NO_MATCH_PROCEED',
    NULL::jsonb,
    jsonb_build_object(
      'session_id', p_session_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'candidate_id', p_candidate_id::text,
      'targeted_timesheet_ids', v_targeted_json,
      'linked_timesheet_ids', v_linked_json,
      'queue_identity_targeted_timesheet_ids', v_queue_targeted_json,
      'queue_identity_linked_timesheet_ids', v_queue_linked_json,
      'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
      'normalised_delta_family_key', v_family_key,
      'latest_source_change_seq', v_latest_seq,
      'dirty_marking_skipped', false,
      'session_progress_dirtying_skipped', false,
      'trigger_reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    'EARLY_PREFLIGHT_NO_MATCH_PROCEED',
    COALESCE(p_actor_user_id, v_session_row.actor_user_id)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'action', 'PROCEED',
    'match_found', false,
    'candidate_id', p_candidate_id::text,
    'session_id', p_session_id::text,
    'session_version', COALESCE(v_session_row.version, 0),
    'targeted_timesheet_ids', v_targeted_json,
    'linked_timesheet_ids', v_linked_json,
    'resolved_family_timesheet_ids', COALESCE(v_family_scope->'family_timesheet_ids', v_targeted_json),
    'normalised_delta_family_key', v_family_key,
    'latest_source_change_seq', v_latest_seq
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_delta_refresh_classify_v1(p_session_id uuid, p_candidate_id uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_row public.banking_pay_workbench_session_scope%ROWTYPE;
  v_settings_json jsonb := '{}'::jsonb;
  v_now timestamptz := now();
  v_trigger_table text := lower(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json->>'table_name', '')));
  v_trigger_operation text := upper(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json->>'trigger_op', v_payload_json->>'operation', '')));
  v_dirty_reason text := NULLIF(BTRIM(COALESCE(v_payload_json->>'dirty_reason', v_payload_json->>'refresh_reason', v_payload_json->>'reason', '')), '');
  v_refresh_scope_kind text := upper(BTRIM(COALESCE(v_payload_json->>'refresh_scope_kind', v_payload_json->>'scope_kind', '')));
  v_pay_channel_scope text := upper(BTRIM(COALESCE(v_payload_json->>'pay_channel_scope', 'ALL')));
  v_operation_type text := upper(BTRIM(COALESCE(v_payload_json->>'operation_type', v_payload_json->>'mutation_type', '')));
  v_requested_projection_mode text := upper(BTRIM(COALESCE(v_payload_json->>'projection_mode', '')));
  v_old_row_json jsonb := CASE WHEN jsonb_typeof(v_payload_json->'old_row_json') = 'object' THEN COALESCE(v_payload_json->'old_row_json', '{}'::jsonb) ELSE '{}'::jsonb END;
  v_new_row_json jsonb := CASE WHEN jsonb_typeof(v_payload_json->'new_row_json') = 'object' THEN COALESCE(v_payload_json->'new_row_json', '{}'::jsonb) ELSE '{}'::jsonb END;
  v_targeted_json jsonb := COALESCE(v_payload_json->'targeted_timesheet_ids', v_payload_json->'changed_timesheet_ids', '[]'::jsonb);
  v_linked_json jsonb := COALESCE(v_payload_json->'linked_timesheet_ids', '[]'::jsonb);
  v_finance_case_json jsonb := COALESCE(v_payload_json->'finance_case_ids', '[]'::jsonb);
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_payload_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_resolved_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_affected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_invalid_targeted_uuid_count integer := 0;
  v_invalid_linked_uuid_count integer := 0;
  v_invalid_finance_case_uuid_count integer := 0;
  v_targeted_count integer := 0;
  v_valid_targeted_owner_count integer := 0;
  v_valid_linked_owner_count integer := 0;
  v_linked_count integer := 0;
  v_source_change_seq bigint := 0;
  v_payload_session_version bigint := NULL::bigint;
  v_session_version_mismatch boolean := false;
  v_source_snapshot_run_id uuid := NULL::uuid;
  v_source_snapshot_run_id_text text := NULL::text;
  v_pay_batch_id uuid := NULL::uuid;
  v_pay_batch_id_text text := NULL::text;
  v_force_legacy boolean := false;
  v_force_broad_legacy boolean := false;
  v_payload_shadow_mode boolean := false;
  v_delta_refresh_enabled boolean := false;
  v_delta_shadow_mode boolean := true;
  v_enable_normal_timesheet boolean := false;
  v_enable_readiness_only boolean := false;
  v_enable_reservation_only boolean := false;
  v_fallback_on_mismatch boolean := true;
  v_clone_rebase_enabled boolean := false;
  v_has_finance_case boolean := false;
  v_has_pay_advance boolean := false;
  v_has_timesheet_advance boolean := false;
  v_has_loan boolean := false;
  v_has_overpayment boolean := false;
  v_has_underpayment boolean := false;
  v_has_manual_debt_adjustment boolean := false;
  v_has_manual_credit_adjustment boolean := false;
  v_has_manual_debit boolean := false;
  v_has_repayment boolean := false;
  v_has_case_resolution boolean := false;
  v_has_one_off_bank_account boolean := false;
  v_ts_adjustment_as_advance boolean := false;
  v_ts_adjustment_credit boolean := false;
  v_ts_adjustment_debit boolean := false;
  v_has_pay_method_switch boolean := false;
  v_has_umbrella_entity_change boolean := false;
  v_has_bank_routing_change boolean := false;
  v_has_contract_client_dirty boolean := false;
  v_has_unknown_trigger boolean := false;
  v_candidate_pay_method text := NULL::text;
  v_candidate_umbrella_id uuid := NULL::uuid;
  v_candidate_bank_details_hash text := NULL::text;
  v_old_pay_method text := NULL::text;
  v_new_pay_method text := NULL::text;
  v_old_umbrella_id_text text := NULL::text;
  v_new_umbrella_id_text text := NULL::text;
  v_old_bank_details_hash text := NULL::text;
  v_new_bank_details_hash text := NULL::text;
  v_bank_routing_unchanged boolean := false;
  v_payee_route_unchanged boolean := false;
  v_existing_preview_key_count integer := 0;
  v_existing_preview_key_missing_count integer := 0;
  v_would_fast_path_allowed boolean := false;
  v_fast_path_allowed boolean := false;
  v_resolved_job_type text := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  v_projection_mode text := 'LEGACY';
  v_projection_class text := 'UNKNOWN_TRIGGER';
  v_scope_status text := 'SOURCE_BUILD_PENDING';
  v_routing_decision text := 'LEGACY';
  v_fallback_required boolean := true;
  v_fallback_reason text := 'UNKNOWN_TRIGGER';
  v_complexity_flags jsonb := '{}'::jsonb;
  v_readiness_identity_checked boolean := false;
  v_resolved_mode text := 'LEGACY';
  v_no_dirty_event boolean := false;
  v_timesheet_authorise_boundary_changed boolean := false;
  v_timesheet_unauthorise_boundary_changed boolean := false;
  v_payload_authorise_boundary_changed boolean := false;
  v_payload_unauthorise_boundary_changed boolean := false;
  v_timesheet_archive_boundary_changed boolean := false;
  v_timesheet_unarchive_boundary_changed boolean := false;
  v_payload_archive_boundary_changed boolean := false;
  v_payload_unarchive_boundary_changed boolean := false;
  v_payload_ordinary_no_dirty boolean := false;
  v_payload_banking_pay_dirty_required boolean := false;
  v_explicit_banking_pay_action boolean := false;
  v_patch_after_batch_enabled boolean := true;
  v_timesheet_pay_state_settlement_changed boolean := false;
  v_timesheet_pay_state_summary_changed boolean := false;
  v_timesheet_pay_state_bookkeeping_ignored boolean := false;
  v_timesheet_pay_state_noop_ignored boolean := false;
  v_timesheet_pay_state_routing_reason text := NULL::text;
  v_lifecycle_event_class text := NULL::text;
  v_lifecycle_admission jsonb := '{}'::jsonb;
  v_enable_simple_authorise boolean := false;
  v_enable_simple_unauthorise boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  v_trigger_table := lower(BTRIM(REPLACE(COALESCE(v_trigger_table, ''), '"', '')));
  IF POSITION('.' IN v_trigger_table) > 0 THEN
    v_trigger_table := regexp_replace(v_trigger_table, '^.*\.', '');
  END IF;

  IF p_session_id IS NULL OR p_candidate_id IS NULL THEN
    v_complexity_flags := jsonb_build_object(
      'missing_session_or_candidate_input', true,
      'checked_at_utc', v_now::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'fast_path_allowed', false,
      'would_fast_path_allowed', false,
      'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'projection_mode', 'LEGACY',
      'projection_class', 'TARGET_SCOPE_MISSING',
      'routing_decision', 'LEGACY',
      'resolved_mode', 'LEGACY',
      'scope_status', 'SOURCE_BUILD_PENDING',
      'targeted_timesheet_ids', '[]'::jsonb,
      'linked_timesheet_ids', '[]'::jsonb,
      'affected_timesheet_ids', '[]'::jsonb,
      'fallback_required', true,
      'no_op', false,
      'banking_pay_dirty_required', true,
      'fallback_reason', 'SESSION_OR_SCOPE_NOT_OPEN',
      'complexity_flags', v_complexity_flags
    );
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
    AND session_row.status = 'OPEN'
    AND session_row.discarded_at_utc IS NULL;

  SELECT scope_row.*
  INTO v_scope_row
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id;

  IF v_session_row.id IS NULL OR v_scope_row.id IS NULL THEN
    v_complexity_flags := jsonb_build_object(
      'session_open', v_session_row.id IS NOT NULL,
      'candidate_in_scope', v_scope_row.id IS NOT NULL,
      'checked_at_utc', v_now::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'fast_path_allowed', false,
      'would_fast_path_allowed', false,
      'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'projection_mode', 'LEGACY',
      'projection_class', 'TARGET_SCOPE_MISSING',
      'routing_decision', 'LEGACY',
      'resolved_mode', 'LEGACY',
      'scope_status', 'SOURCE_BUILD_PENDING',
      'targeted_timesheet_ids', '[]'::jsonb,
      'linked_timesheet_ids', '[]'::jsonb,
      'affected_timesheet_ids', '[]'::jsonb,
      'fallback_required', true,
      'no_op', false,
      'banking_pay_dirty_required', true,
      'fallback_reason', 'SESSION_OR_SCOPE_NOT_OPEN',
      'complexity_flags', v_complexity_flags
    );
  END IF;

  SELECT COALESCE(to_jsonb(settings_row), '{}'::jsonb)
  INTO v_settings_json
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  v_delta_refresh_enabled := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_refresh_enabled', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_delta_shadow_mode := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_shadow_mode', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_enable_normal_timesheet := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_normal_timesheet', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_enable_readiness_only := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_readiness_only', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_enable_reservation_only := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_reservation_only', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_fallback_on_mismatch := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_fallback_on_mismatch', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_clone_rebase_enabled := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_clone_rebase_enabled', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_patch_after_batch_enabled := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_patch_after_batch_mutation_enabled', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_enable_simple_authorise := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_simple_authorise','false'))) IN ('true','t','1','yes','y','on');
  v_enable_simple_unauthorise := lower(BTRIM(COALESCE(v_settings_json->>'banking_pay_workbench_delta_enable_simple_unauthorise','false'))) IN ('true','t','1','yes','y','on');

  v_force_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_force_broad_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_broad_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_shadow_mode := lower(BTRIM(COALESCE(v_payload_json->>'shadow_mode', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_bank_routing_unchanged := lower(BTRIM(COALESCE(v_payload_json->>'bank_routing_unchanged', v_payload_json->>'bank_details_unchanged', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payee_route_unchanged := lower(BTRIM(COALESCE(v_payload_json->>'payee_route_unchanged', v_payload_json->>'payee_identity_unchanged', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_banking_pay_dirty_required := lower(BTRIM(COALESCE(v_payload_json->>'banking_pay_dirty_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_ordinary_no_dirty := lower(BTRIM(COALESCE(v_payload_json->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_trigger_table = 'timesheet_pay_state' THEN
    v_timesheet_pay_state_settlement_changed := lower(BTRIM(COALESCE(
      v_payload_json->>'timesheet_pay_state_settlement_changed',
      v_payload_json->>'pay_state_settlement_changed',
      v_payload_json->>'settled_baseline_changed',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');

    v_timesheet_pay_state_summary_changed := lower(BTRIM(COALESCE(
      v_payload_json->>'timesheet_pay_state_summary_changed',
      v_payload_json->>'pay_state_summary_changed',
      v_payload_json->>'summary_only_changed',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');

    v_timesheet_pay_state_bookkeeping_ignored := lower(BTRIM(COALESCE(
      v_payload_json->>'timesheet_pay_state_bookkeeping_ignored',
      v_payload_json->>'pay_state_bookkeeping_ignored',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'pay_state_dirty_routing_reason', ''))) = 'TIMESHEET_PAY_STATE_BOOKKEEPING_IGNORED';

    v_timesheet_pay_state_noop_ignored := lower(BTRIM(COALESCE(
      v_payload_json->>'timesheet_pay_state_noop_ignored',
      v_payload_json->>'pay_state_noop_ignored',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'pay_state_dirty_routing_reason', ''))) = 'TIMESHEET_PAY_STATE_NOOP_IGNORED';
  END IF;

  v_explicit_banking_pay_action := lower(BTRIM(COALESCE(
    v_payload_json->>'explicit_banking_pay_action',
    v_payload_json->>'banking_pay_dirty_allowed',
    v_payload_json->>'banking_pay_refresh_intended',
    v_payload_json->>'banking_pay_dirty_required',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on')
  OR UPPER(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', v_operation_type, v_dirty_reason, ''))) IN (
    'TIMESHEET_AUTHORISE',
    'TIMESHEET_UNAUTHORISE',
    'AUTHORISE_TIMESHEET',
    'UNAUTHORISE_TIMESHEET',
    'BANKING_PAY_EXPLICIT_ACTION',
    'BANKING_PAY_MANUAL_REFRESH',
    'BANKING_PAY_WORKBENCH_REFRESH',
    'BANKING_PAY_DECISION_OPERATION',
    'BANKING_PAY_CASE_RESOLUTION',
    'BANKING_PAY_TIMESHEET_ADVANCE',
    'BANKING_PAY_PATCH'
  );

  IF COALESCE(v_payload_json->>'source_change_seq', v_payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$' THEN
    v_source_change_seq := COALESCE(v_payload_json->>'source_change_seq', v_payload_json->>'source_change_sequence')::bigint;
  END IF;

  IF COALESCE(v_payload_json->>'session_version', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_session_version := (v_payload_json->>'session_version')::bigint;
    v_session_version_mismatch := v_payload_session_version IS DISTINCT FROM v_session_row.version;
  END IF;

  v_source_snapshot_run_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'source_snapshot_run_id', v_payload_json->>'snapshot_run_id', '')), '');
  IF v_source_snapshot_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_source_snapshot_run_id := v_source_snapshot_run_id_text::uuid;
  END IF;

  v_pay_batch_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'pay_batch_id', v_payload_json#>>'{batch,pay_batch_id}', '')), '');
  IF v_pay_batch_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_pay_batch_id := v_pay_batch_id_text::uuid;
  END IF;

  IF jsonb_typeof(v_targeted_json) = 'array' THEN
    SELECT COALESCE(array_agg(DISTINCT targeted_values.timesheet_id_value ORDER BY targeted_values.timesheet_id_value), ARRAY[]::uuid[]),
           COALESCE(count(*) FILTER (WHERE targeted_values.raw_value IS NOT NULL AND targeted_values.timesheet_id_value IS NULL), 0)::integer
    INTO v_targeted_timesheet_ids,
         v_invalid_targeted_uuid_count
    FROM (
      SELECT targeted_element.value #>> '{}' AS raw_value,
             CASE
               WHEN BTRIM(targeted_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN BTRIM(targeted_element.value #>> '{}')::uuid
               ELSE NULL::uuid
             END AS timesheet_id_value
      FROM jsonb_array_elements(v_targeted_json) AS targeted_element(value)
    ) AS targeted_values
    WHERE targeted_values.raw_value IS NOT NULL;
  ELSE
    v_invalid_targeted_uuid_count := CASE WHEN v_targeted_json IS NULL THEN 0 ELSE 1 END;
  END IF;

  IF jsonb_typeof(v_linked_json) = 'array' THEN
    SELECT COALESCE(array_agg(DISTINCT linked_values.timesheet_id_value ORDER BY linked_values.timesheet_id_value), ARRAY[]::uuid[]),
           COALESCE(count(*) FILTER (WHERE linked_values.raw_value IS NOT NULL AND linked_values.timesheet_id_value IS NULL), 0)::integer
    INTO v_payload_linked_timesheet_ids,
         v_invalid_linked_uuid_count
    FROM (
      SELECT linked_element.value #>> '{}' AS raw_value,
             CASE
               WHEN BTRIM(linked_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN BTRIM(linked_element.value #>> '{}')::uuid
               ELSE NULL::uuid
             END AS timesheet_id_value
      FROM jsonb_array_elements(v_linked_json) AS linked_element(value)
    ) AS linked_values
    WHERE linked_values.raw_value IS NOT NULL;
  ELSE
    v_invalid_linked_uuid_count := CASE WHEN v_linked_json IS NULL THEN 0 ELSE 1 END;
  END IF;

  IF jsonb_typeof(v_finance_case_json) = 'array' THEN
    SELECT COALESCE(array_agg(DISTINCT finance_case_values.finance_case_id_value ORDER BY finance_case_values.finance_case_id_value), ARRAY[]::uuid[]),
           COALESCE(count(*) FILTER (WHERE finance_case_values.raw_value IS NOT NULL AND finance_case_values.finance_case_id_value IS NULL), 0)::integer
    INTO v_finance_case_ids,
         v_invalid_finance_case_uuid_count
    FROM (
      SELECT finance_case_element.value #>> '{}' AS raw_value,
             CASE
               WHEN BTRIM(finance_case_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN BTRIM(finance_case_element.value #>> '{}')::uuid
               ELSE NULL::uuid
             END AS finance_case_id_value
      FROM jsonb_array_elements(v_finance_case_json) AS finance_case_element(value)
    ) AS finance_case_values
    WHERE finance_case_values.raw_value IS NOT NULL;
  ELSE
    v_invalid_finance_case_uuid_count := CASE WHEN v_finance_case_json IS NULL THEN 0 ELSE 1 END;
  END IF;

  v_targeted_count := COALESCE(array_length(v_targeted_timesheet_ids, 1), 0);

  IF v_targeted_count > 0 THEN
    SELECT COALESCE(array_agg(DISTINCT rotation_scope.family_timesheet_id ORDER BY rotation_scope.family_timesheet_id), ARRAY[]::uuid[])
    INTO v_resolved_family_timesheet_ids
    FROM public._pay_timesheet_rotation_scope(v_targeted_timesheet_ids) AS rotation_scope
    WHERE rotation_scope.family_timesheet_id IS NOT NULL;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT affected_timesheets.timesheet_id_value ORDER BY affected_timesheets.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_all_affected_timesheet_ids
  FROM (
    SELECT unnest(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id_value
    UNION ALL
    SELECT unnest(COALESCE(v_payload_linked_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id_value
    UNION ALL
    SELECT unnest(COALESCE(v_resolved_family_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id_value
  ) AS affected_timesheets
  WHERE affected_timesheets.timesheet_id_value IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT linked_timesheets.timesheet_id_value ORDER BY linked_timesheets.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_linked_timesheet_ids
  FROM unnest(COALESCE(v_all_affected_timesheet_ids, ARRAY[]::uuid[])) AS linked_timesheets(timesheet_id_value)
  WHERE linked_timesheets.timesheet_id_value IS NOT NULL
    AND NOT (linked_timesheets.timesheet_id_value = ANY(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])));

  v_linked_count := COALESCE(array_length(v_linked_timesheet_ids, 1), 0);

  IF v_targeted_count > 0 THEN
    SELECT count(DISTINCT targeted_owner.timesheet_id_value)::integer
    INTO v_valid_targeted_owner_count
    FROM unnest(v_targeted_timesheet_ids) AS targeted_owner(timesheet_id_value)
    JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = targeted_owner.timesheet_id_value
    LEFT JOIN public.contracts AS contract_row
      ON contract_row.id = timesheet_row.contract_id
    WHERE contract_row.candidate_id = p_candidate_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets_financials AS tsfin_owner
         WHERE tsfin_owner.timesheet_id = targeted_owner.timesheet_id_value
           AND tsfin_owner.candidate_id = p_candidate_id
       );
  END IF;

  IF v_linked_count > 0 THEN
    SELECT count(DISTINCT linked_owner.timesheet_id_value)::integer
    INTO v_valid_linked_owner_count
    FROM unnest(v_linked_timesheet_ids) AS linked_owner(timesheet_id_value)
    JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = linked_owner.timesheet_id_value
    LEFT JOIN public.contracts AS contract_row
      ON contract_row.id = timesheet_row.contract_id
    WHERE contract_row.candidate_id = p_candidate_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets_financials AS tsfin_owner
         WHERE tsfin_owner.timesheet_id = linked_owner.timesheet_id_value
           AND tsfin_owner.candidate_id = p_candidate_id
       );
  ELSE
    v_valid_linked_owner_count := 0;
  END IF;

  IF v_trigger_table = 'timesheets' AND v_trigger_operation = 'UPDATE' THEN
    v_timesheet_authorise_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'authorised_at_server', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'authorised_at_server', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'revoked_at', '')), '') IS NULL;

    v_timesheet_unauthorise_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'revoked_at', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'revoked_at', '')), '') IS NOT NULL;

    v_timesheet_archive_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'archived_at_utc', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'archived_at_utc', '')), '') IS NOT NULL;

    v_timesheet_unarchive_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'archived_at_utc', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'archived_at_utc', '')), '') IS NULL;
  ELSIF v_trigger_table = 'timesheets_financials' AND v_trigger_operation = 'UPDATE' THEN
    v_timesheet_authorise_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'authorised_at_utc', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'authorised_at_utc', '')), '') IS NOT NULL;

    v_timesheet_unauthorise_boundary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row_json->>'authorised_at_utc', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row_json->>'authorised_at_utc', '')), '') IS NULL;
  END IF;

  v_payload_authorise_boundary_changed := lower(BTRIM(COALESCE(
    v_payload_json->>'authorise_boundary_changed',
    v_payload_json->>'timesheet_authorise_boundary_changed',
    v_payload_json#>>'{complexity_flags,authorise_boundary_changed}',
    v_payload_json#>>'{classifier_result,complexity_flags,authorise_boundary_changed}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_unauthorise_boundary_changed := lower(BTRIM(COALESCE(
    v_payload_json->>'unauthorise_boundary_changed',
    v_payload_json->>'timesheet_unauthorise_boundary_changed',
    v_payload_json#>>'{complexity_flags,unauthorise_boundary_changed}',
    v_payload_json#>>'{classifier_result,complexity_flags,unauthorise_boundary_changed}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_archive_boundary_changed := lower(BTRIM(COALESCE(
    v_payload_json->>'archive_boundary_changed',
    v_payload_json->>'timesheet_archive_boundary_changed',
    v_payload_json#>>'{complexity_flags,archive_boundary_changed}',
    v_payload_json#>>'{classifier_result,complexity_flags,archive_boundary_changed}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_unarchive_boundary_changed := lower(BTRIM(COALESCE(
    v_payload_json->>'unarchive_boundary_changed',
    v_payload_json->>'timesheet_unarchive_boundary_changed',
    v_payload_json#>>'{complexity_flags,unarchive_boundary_changed}',
    v_payload_json#>>'{classifier_result,complexity_flags,unarchive_boundary_changed}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_timesheet_authorise_boundary_changed := v_timesheet_authorise_boundary_changed
    OR v_payload_authorise_boundary_changed
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', ''))) IN ('TIMESHEET_AUTHORISE', 'AUTHORISE_TIMESHEET');
  v_timesheet_unauthorise_boundary_changed := v_timesheet_unauthorise_boundary_changed
    OR v_payload_unauthorise_boundary_changed
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', ''))) IN ('TIMESHEET_UNAUTHORISE', 'UNAUTHORISE_TIMESHEET');
  v_timesheet_archive_boundary_changed := v_timesheet_archive_boundary_changed
    OR v_payload_archive_boundary_changed
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', ''))) IN ('TIMESHEET_ARCHIVE', 'ARCHIVE_TIMESHEET');
  v_timesheet_unarchive_boundary_changed := v_timesheet_unarchive_boundary_changed
    OR v_payload_unarchive_boundary_changed
    OR UPPER(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', ''))) IN ('TIMESHEET_UNARCHIVE', 'UNARCHIVE_TIMESHEET');

  v_explicit_banking_pay_action := v_explicit_banking_pay_action
    OR v_timesheet_authorise_boundary_changed
    OR v_timesheet_unauthorise_boundary_changed
    OR v_timesheet_archive_boundary_changed
    OR v_timesheet_unarchive_boundary_changed;

  v_no_dirty_event := v_trigger_operation = 'UPDATE'
    AND v_trigger_table IN ('timesheets', 'timesheets_financials')
    AND v_explicit_banking_pay_action IS NOT TRUE
    AND v_payload_banking_pay_dirty_required IS NOT TRUE
    AND v_timesheet_authorise_boundary_changed IS NOT TRUE
    AND v_timesheet_unauthorise_boundary_changed IS NOT TRUE
    AND v_timesheet_archive_boundary_changed IS NOT TRUE
    AND v_timesheet_unarchive_boundary_changed IS NOT TRUE;

  IF v_no_dirty_event IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'fast_path_allowed', false,
      'would_fast_path_allowed', false,
      'resolved_mode', 'BLOCKED',
      'resolved_job_type', 'NOOP',
      'projection_mode', 'BLOCKED',
      'projection_class', 'ORDINARY_TIMESHEET_EDIT_SAVE',
      'routing_decision', 'BLOCKED',
      'scope_status', 'NOOP',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'source_change_seq', COALESCE(v_source_change_seq, 0),
      'session_version', v_session_row.version,
      'payload_session_version', v_payload_session_version,
      'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
      'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb),
      'affected_timesheet_ids', COALESCE(to_jsonb(v_all_affected_timesheet_ids), '[]'::jsonb),
      'fallback_required', false,
      'fallback_reason', 'ORDINARY_TIMESHEET_EDIT_SAVE_NO_BANKING_PAY_DIRTY',
      'no_op', true,
      'banking_pay_dirty_required', false,
      'complexity_flags', jsonb_build_object(
        'ordinary_timesheet_edit_save_no_dirty', true,
        'trigger_table', v_trigger_table,
        'trigger_operation', v_trigger_operation,
        'explicit_banking_pay_action', COALESCE(v_explicit_banking_pay_action, false),
        'banking_pay_dirty_required', COALESCE(v_payload_banking_pay_dirty_required, false),
        'ordinary_timesheet_edit_save_no_dirty', COALESCE(v_payload_ordinary_no_dirty, true),
        'authorise_boundary_changed', COALESCE(v_timesheet_authorise_boundary_changed, false),
        'unauthorise_boundary_changed', COALESCE(v_timesheet_unauthorise_boundary_changed, false),
        'targeted_timesheet_count', COALESCE(v_targeted_count, 0),
        'linked_timesheet_count', COALESCE(v_linked_count, 0),
        'checked_at_utc', v_now::text
      )
    );
  END IF;

  SELECT candidate_row.pay_method,
         candidate_row.umbrella_id,
         candidate_row.bank_details_hash
  INTO v_candidate_pay_method,
       v_candidate_umbrella_id,
       v_candidate_bank_details_hash
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  v_old_pay_method := upper(BTRIM(COALESCE(v_payload_json->>'old_pay_method', v_old_row_json->>'pay_method', v_payload_json#>>'{old,pay_method}', '')));
  v_new_pay_method := upper(BTRIM(COALESCE(v_payload_json->>'new_pay_method', v_new_row_json->>'pay_method', v_payload_json#>>'{new,pay_method}', '')));
  v_old_umbrella_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'old_umbrella_id', v_old_row_json->>'umbrella_id', v_payload_json#>>'{old,umbrella_id}', '')), '');
  v_new_umbrella_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'new_umbrella_id', v_new_row_json->>'umbrella_id', v_payload_json#>>'{new,umbrella_id}', '')), '');
  v_old_bank_details_hash := NULLIF(BTRIM(COALESCE(v_payload_json->>'old_bank_details_hash', v_old_row_json->>'bank_details_hash', v_payload_json#>>'{old,bank_details_hash}', '')), '');
  v_new_bank_details_hash := NULLIF(BTRIM(COALESCE(v_payload_json->>'new_bank_details_hash', v_new_row_json->>'bank_details_hash', v_payload_json#>>'{new,bank_details_hash}', '')), '');

  v_has_pay_method_switch := v_old_pay_method IN ('PAYE', 'UMBRELLA')
                             AND v_new_pay_method IN ('PAYE', 'UMBRELLA')
                             AND v_old_pay_method IS DISTINCT FROM v_new_pay_method;
  v_has_umbrella_entity_change := v_old_umbrella_id_text IS NOT NULL
                                  AND v_new_umbrella_id_text IS NOT NULL
                                  AND v_old_umbrella_id_text IS DISTINCT FROM v_new_umbrella_id_text;
  v_has_bank_routing_change := v_old_bank_details_hash IS NOT NULL
                               AND v_new_bank_details_hash IS NOT NULL
                               AND v_old_bank_details_hash IS DISTINCT FROM v_new_bank_details_hash;

  IF lower(BTRIM(COALESCE(v_payload_json->>'pay_method_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    v_has_pay_method_switch := true;
  END IF;

  IF lower(BTRIM(COALESCE(v_payload_json->>'umbrella_entity_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    v_has_umbrella_entity_change := true;
  END IF;

  IF lower(BTRIM(COALESCE(v_payload_json->>'bank_routing_changed', v_payload_json->>'bank_details_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    v_has_bank_routing_change := true;
  END IF;

  v_has_contract_client_dirty := v_trigger_table IN ('contracts', 'clients', 'client_settings', 'contract_client_dirty')
                                 OR lower(BTRIM(COALESCE(v_payload_json->>'contract_client_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_trigger_table IN ('umbrellas', 'umbrella_companies') THEN
    v_has_umbrella_entity_change := true;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_finance_case_components AS component_row
    WHERE component_row.candidate_id = p_candidate_id
      AND component_row.closed_at_utc IS NULL
      AND (
        COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
        OR component_row.linked_timesheet_id IS NULL
        OR component_row.linked_timesheet_id = ANY(v_all_affected_timesheet_ids)
      )
  )
  INTO v_has_finance_case;

  SELECT COALESCE(bool_or(upper(COALESCE(advance_row.case_type::text, advance_row.reason::text, advance_row.advance_kind::text, '')) = 'PAYMENT_ADVANCE'
                          OR upper(COALESCE(advance_row.reason::text, '')) IN ('MANUAL_ADVANCE', 'MISSING_SHIFT')
                          OR upper(COALESCE(advance_row.advance_kind::text, '')) = 'LEGACY_ADVANCE'), false),
         COALESCE(bool_or(upper(COALESCE(advance_row.reason::text, advance_row.advance_kind::text, '')) = 'LOAN'
                          OR upper(COALESCE(advance_row.advance_kind::text, '')) = 'LOAN'), false),
         COALESCE(bool_or(upper(COALESCE(advance_row.reason::text, advance_row.advance_kind::text, advance_row.case_type::text, '')) = 'OVERPAYMENT'
                          OR upper(COALESCE(advance_row.advance_kind::text, '')) = 'OVERPAYMENT'), false),
         COALESCE(bool_or(upper(COALESCE(advance_row.reason::text, advance_row.advance_kind::text, advance_row.case_type::text, '')) = 'UNDERPAYMENT'
                          OR upper(COALESCE(advance_row.advance_kind::text, '')) = 'UNDERPAYMENT'), false),
         COALESCE(bool_or(upper(COALESCE(advance_row.case_type::text, '')) = 'MANUAL_DEBT_ADJUSTMENT'), false),
         COALESCE(bool_or(upper(COALESCE(advance_row.case_type::text, '')) = 'MANUAL_CREDIT_ADJUSTMENT'), false),
         COALESCE(bool_or(COALESCE(advance_row.outstanding_amount, 0) <> 0
                          AND upper(COALESCE(advance_row.status::text, '')) IN ('ACTIVE', 'PAUSED')), false),
         COALESCE(bool_or(COALESCE(advance_row.oneoff_bank_details_required, false) = true
                          OR upper(COALESCE(advance_row.routing_kind::text, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'), false)
  INTO v_has_pay_advance,
       v_has_loan,
       v_has_overpayment,
       v_has_underpayment,
       v_has_manual_debt_adjustment,
       v_has_manual_credit_adjustment,
       v_has_repayment,
       v_has_one_off_bank_account
  FROM public.pay_advances AS advance_row
  WHERE advance_row.candidate_id = p_candidate_id
    AND upper(COALESCE(advance_row.status::text, '')) IN ('ACTIVE', 'PAUSED')
    AND advance_row.cleared_at_utc IS NULL
    AND advance_row.written_off_at_utc IS NULL
    AND (
      COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
      OR advance_row.linked_timesheet_id IS NULL
      OR advance_row.linked_timesheet_id = ANY(v_all_affected_timesheet_ids)
    );

  SELECT EXISTS (
    SELECT 1
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.candidate_id = p_candidate_id
      AND override_row.override_type = 'ADVANCE_THIS_PAYMENT'
      AND override_row.consumed_at_utc IS NULL
      AND override_row.cleared_at_utc IS NULL
      AND (
        COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
        OR override_row.timesheet_id = ANY(v_all_affected_timesheet_ids)
      )
  )
  INTO v_has_timesheet_advance;

  SELECT COALESCE(bool_or(COALESCE(adjustment_row.as_advance, false) = true), false),
         COALESCE(bool_or(COALESCE(adjustment_row.delta_pay_ex_vat, 0) > 0), false),
         COALESCE(bool_or(COALESCE(adjustment_row.delta_pay_ex_vat, 0) < 0), false)
  INTO v_ts_adjustment_as_advance,
       v_ts_adjustment_credit,
       v_ts_adjustment_debit
  FROM public.ts_pay_adjustments AS adjustment_row
  WHERE adjustment_row.candidate_id = p_candidate_id
    AND adjustment_row.paid_at_utc IS NULL
    AND (
      COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
      OR adjustment_row.timesheet_id = ANY(v_all_affected_timesheet_ids)
    );

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id = p_session_id
      AND resolution_row.candidate_id = p_candidate_id
      AND (
        COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
        OR resolution_row.timesheet_id IS NULL
        OR resolution_row.timesheet_id = ANY(v_all_affected_timesheet_ids)
      )
  )
  INTO v_has_case_resolution;

  v_has_pay_advance := COALESCE(v_has_pay_advance, false) OR COALESCE(v_ts_adjustment_as_advance, false);
  v_has_manual_credit_adjustment := COALESCE(v_has_manual_credit_adjustment, false) OR COALESCE(v_ts_adjustment_credit, false);
  v_has_manual_debit := COALESCE(v_has_manual_debit, false) OR COALESCE(v_ts_adjustment_debit, false);

  IF v_trigger_table IN ('pay_finance_case_components', 'pay_finance_cases') OR COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    v_has_finance_case := true;
  END IF;

  IF v_trigger_table = 'pay_advances' THEN
    v_has_pay_advance := true;
  END IF;

  IF v_trigger_table = 'timesheet_payment_overrides' THEN
    v_has_timesheet_advance := true;
  END IF;

  IF v_trigger_table = 'ts_pay_adjustments' THEN
    v_has_manual_credit_adjustment := true;
  END IF;

  IF v_trigger_table IN ('banking_pay_workbench_session_case_resolutions', 'case_resolution', 'case_resolutions')
     OR v_operation_type IN ('CASE_RESOLUTION', 'APPLY_CASE_RESOLUTION', 'CLEAR_CASE_RESOLUTION') THEN
    v_has_case_resolution := true;
  END IF;

  v_complexity_flags := jsonb_build_object(
    'has_finance_case', COALESCE(v_has_finance_case, false),
    'has_pay_advance', COALESCE(v_has_pay_advance, false),
    'has_timesheet_advance', COALESCE(v_has_timesheet_advance, false),
    'has_loan', COALESCE(v_has_loan, false),
    'has_overpayment', COALESCE(v_has_overpayment, false),
    'has_underpayment', COALESCE(v_has_underpayment, false),
    'has_manual_debt_adjustment', COALESCE(v_has_manual_debt_adjustment, false),
    'has_manual_credit_adjustment', COALESCE(v_has_manual_credit_adjustment, false),
    'has_manual_debit', COALESCE(v_has_manual_debit, false),
    'has_repayment', COALESCE(v_has_repayment, false),
    'has_case_resolution', COALESCE(v_has_case_resolution, false),
    'has_paye_umbrella_switch', COALESCE(v_has_pay_method_switch, false),
    'has_umbrella_entity_change', COALESCE(v_has_umbrella_entity_change, false),
    'has_bank_routing_change', COALESCE(v_has_bank_routing_change, false),
    'has_one_off_bank_account', COALESCE(v_has_one_off_bank_account, false),
    'has_timesheet_pay_adjustment_as_advance', COALESCE(v_ts_adjustment_as_advance, false),
    'has_timesheet_pay_adjustment_credit', COALESCE(v_ts_adjustment_credit, false),
    'has_timesheet_pay_adjustment_debit', COALESCE(v_ts_adjustment_debit, false),
    'has_contract_client_dirty', COALESCE(v_has_contract_client_dirty, false),
    'timesheet_pay_state_event', v_trigger_table = 'timesheet_pay_state',
    'timesheet_pay_state_settlement_changed', COALESCE(v_timesheet_pay_state_settlement_changed, false),
    'timesheet_pay_state_summary_changed', COALESCE(v_timesheet_pay_state_summary_changed, false),
    'timesheet_pay_state_bookkeeping_ignored', COALESCE(v_timesheet_pay_state_bookkeeping_ignored, false),
    'timesheet_pay_state_noop_ignored', COALESCE(v_timesheet_pay_state_noop_ignored, false),
    'timesheet_pay_state_routing_reason', v_timesheet_pay_state_routing_reason,
    'authorise_boundary_changed', COALESCE(v_timesheet_authorise_boundary_changed, false),
    'unauthorise_boundary_changed', COALESCE(v_timesheet_unauthorise_boundary_changed, false),
    'archive_boundary_changed', COALESCE(v_timesheet_archive_boundary_changed, false),
    'unarchive_boundary_changed', COALESCE(v_timesheet_unarchive_boundary_changed, false),
    'invalid_targeted_uuid_count', COALESCE(v_invalid_targeted_uuid_count, 0),
    'invalid_linked_uuid_count', COALESCE(v_invalid_linked_uuid_count, 0),
    'invalid_finance_case_uuid_count', COALESCE(v_invalid_finance_case_uuid_count, 0),
    'targeted_timesheet_count', COALESCE(v_targeted_count, 0),
    'linked_timesheet_count', COALESCE(v_linked_count, 0),
    'session_version_mismatch', COALESCE(v_session_version_mismatch, false),
    'payload_session_version', v_payload_session_version,
    'current_session_version', v_session_row.version,
    'checked_at_utc', v_now::text
  );

  IF v_session_version_mismatch IS TRUE THEN
    v_projection_class := 'BROAD_SESSION_REFRESH';
    v_fallback_reason := 'SESSION_VERSION_MISMATCH';
  ELSIF v_source_snapshot_run_id IS NOT NULL AND v_source_snapshot_run_id IS DISTINCT FROM v_session_row.source_snapshot_run_id THEN
    v_projection_class := 'BROAD_SESSION_REFRESH';
    v_fallback_reason := 'SOURCE_SNAPSHOT_RUN_MISMATCH';
  ELSIF v_force_legacy IS TRUE THEN
    v_projection_class := 'UNKNOWN_TRIGGER';
    v_fallback_reason := 'FORCE_LEGACY';
  ELSIF v_force_broad_legacy IS TRUE THEN
    v_projection_class := 'BROAD_SESSION_REFRESH';
    v_fallback_reason := 'FORCE_BROAD_LEGACY';
  ELSIF v_invalid_targeted_uuid_count > 0 OR v_invalid_linked_uuid_count > 0 OR v_invalid_finance_case_uuid_count > 0 THEN
    v_projection_class := 'TARGET_SCOPE_MISSING';
    v_fallback_reason := 'INVALID_UUID_IN_TARGET_PAYLOAD';
  ELSIF lower(BTRIM(COALESCE(v_payload_json->>'new_scope_baseline_required','false'))) IN ('true','t','1','yes','y','on') THEN
    v_projection_class := 'NEW_SESSION_SCOPE_BASELINE';
    v_fallback_reason := 'BASELINE_NOT_ESTABLISHED_FOR_NEW_SCOPE';
  ELSIF v_timesheet_authorise_boundary_changed IS TRUE
        OR v_timesheet_unauthorise_boundary_changed IS TRUE
        OR v_timesheet_archive_boundary_changed IS TRUE
        OR v_timesheet_unarchive_boundary_changed IS TRUE THEN
    v_projection_class := 'TIMESHEET_LIFECYCLE';
    IF v_timesheet_archive_boundary_changed IS TRUE OR v_timesheet_unarchive_boundary_changed IS TRUE THEN
      v_fallback_reason := CASE WHEN v_timesheet_archive_boundary_changed
        THEN 'SOURCE_BUILD_REQUIRED_TIMESHEET_ARCHIVE'
        ELSE 'SOURCE_BUILD_REQUIRED_TIMESHEET_UNARCHIVE' END;
    ELSE
      v_lifecycle_event_class := CASE WHEN v_timesheet_unauthorise_boundary_changed
        THEN 'UNAUTHORISE' ELSE 'AUTHORISE' END;
      v_lifecycle_admission := private.pay_workbench_targeted_delta_admission_v1(
        p_session_id,p_candidate_id,NULL,v_lifecycle_event_class,
        v_targeted_timesheet_ids,v_payload_linked_timesheet_ids,v_source_change_seq
      );
      v_would_fast_path_allowed := COALESCE((v_lifecycle_admission->>'admitted')::boolean,false);
      IF v_would_fast_path_allowed THEN
        v_projection_mode := 'DELTA';
        v_routing_decision := 'DELTA';
        v_scope_status := 'DELTA_REFRESH_PENDING';
        v_fallback_reason := NULL;
      ELSE
        v_fallback_reason := COALESCE(v_lifecycle_admission->>'reason',
          CASE WHEN v_lifecycle_event_class='UNAUTHORISE'
            THEN 'SOURCE_BUILD_REQUIRED_TIMESHEET_UNAUTHORISE'
            ELSE 'SOURCE_BUILD_REQUIRED_TIMESHEET_AUTHORISE' END);
      END IF;
    END IF;
  ELSIF v_has_pay_method_switch IS TRUE THEN
    v_projection_class := 'PAYE_UMBRELLA_SWITCH';
    v_fallback_reason := 'PAYE_UMBRELLA_SWITCH';
  ELSIF v_has_umbrella_entity_change IS TRUE THEN
    v_projection_class := 'UMBRELLA_ENTITY_CHANGE';
    v_fallback_reason := 'UMBRELLA_ENTITY_CHANGE';
  ELSIF v_has_bank_routing_change IS TRUE THEN
    v_projection_class := 'BANK_ROUTING_CHANGE';
    v_fallback_reason := 'BANK_ROUTING_CHANGE';
  ELSIF v_has_one_off_bank_account IS TRUE THEN
    v_projection_class := 'ONE_OFF_BANK_ACCOUNT';
    v_fallback_reason := 'ONE_OFF_BANK_ACCOUNT';
  ELSIF v_has_contract_client_dirty IS TRUE THEN
    v_projection_class := 'CONTRACT_CLIENT_DIRTY';
    v_fallback_reason := 'CONTRACT_CLIENT_OR_CLIENT_SETTINGS_DIRTY';
  ELSIF v_has_case_resolution IS TRUE THEN
    v_projection_class := 'CASE_RESOLUTION';
    v_fallback_reason := 'CASE_RESOLUTION_PRESENT_OR_CHANGED';
  ELSIF v_has_timesheet_advance IS TRUE THEN
    v_projection_class := 'TIMESHEET_ADVANCE';
    v_fallback_reason := 'TIMESHEET_ADVANCE_PRESENT';
  ELSIF v_has_loan IS TRUE THEN
    v_projection_class := 'LOAN';
    v_fallback_reason := 'LOAN_OR_REPAYMENT_PRESENT';
  ELSIF v_has_overpayment IS TRUE THEN
    v_projection_class := 'OVERPAYMENT';
    v_fallback_reason := 'OVERPAYMENT_PRESENT';
  ELSIF v_has_underpayment IS TRUE THEN
    v_projection_class := 'UNDERPAYMENT';
    v_fallback_reason := 'UNDERPAYMENT_PRESENT';
  ELSIF v_has_manual_debt_adjustment IS TRUE THEN
    v_projection_class := 'MANUAL_DEBT_ADJUSTMENT';
    v_fallback_reason := 'MANUAL_DEBT_ADJUSTMENT_PRESENT';
  ELSIF v_has_manual_credit_adjustment IS TRUE THEN
    v_projection_class := 'MANUAL_CREDIT_ADJUSTMENT';
    v_fallback_reason := 'MANUAL_CREDIT_ADJUSTMENT_PRESENT';
  ELSIF v_has_manual_debit IS TRUE THEN
    v_projection_class := 'MANUAL_DEBIT';
    v_fallback_reason := 'MANUAL_DEBIT_PRESENT';
  ELSIF v_has_repayment IS TRUE THEN
    v_projection_class := 'REPAYMENT';
    v_fallback_reason := 'REPAYMENT_PRESENT';
  ELSIF v_has_pay_advance IS TRUE THEN
    v_projection_class := 'PAY_ADVANCE';
    v_fallback_reason := 'PAY_ADVANCE_PRESENT';
  ELSIF v_has_finance_case IS TRUE THEN
    v_projection_class := 'FINANCE_CASE';
    v_fallback_reason := 'FINANCE_CASE_COMPONENTS_PRESENT';
  ELSIF v_operation_type = 'CLONE_REBASE' OR v_requested_projection_mode = 'CLONE_REBASE' THEN
    v_projection_class := 'CLONE_REBASE';
    v_projection_mode := 'CLONE_REBASE';
    v_routing_decision := 'CLONE_REBASE';
    v_scope_status := 'CLONE_REBASE_PENDING';
    v_would_fast_path_allowed := true;
    v_fallback_reason := NULL::text;
  ELSIF v_operation_type IN ('DRAFT_CREATE', 'DRAFT_DELETE', 'DRAFT_CANCEL', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
        OR v_requested_projection_mode IN ('RESERVATION_PATCH', 'POST_DRAFT_OVERLAY') THEN
    SELECT count(*)::integer
    INTO v_existing_preview_key_count
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.status = 'READY'
      AND (
        COALESCE(array_length(v_all_affected_timesheet_ids, 1), 0) = 0
        OR preview_row.timesheet_id = ANY(v_all_affected_timesheet_ids)
      );

    v_projection_class := 'RESERVATION_ONLY';
    v_projection_mode := 'RESERVATION_PATCH';
    v_routing_decision := 'PATCH_ONLY';
    v_scope_status := 'DELTA_REFRESH_PENDING';
    v_would_fast_path_allowed := v_existing_preview_key_count > 0 OR v_pay_batch_id IS NOT NULL;
    v_fallback_reason := CASE WHEN v_would_fast_path_allowed THEN NULL::text ELSE 'RESERVATION_PATCH_TARGET_NOT_FOUND' END;
  ELSIF v_trigger_table IN ('bank_name_checks', 'bank_payee_map') THEN
    IF v_bank_routing_unchanged IS NOT TRUE AND v_old_bank_details_hash IS NOT NULL AND v_new_bank_details_hash IS NOT NULL AND v_old_bank_details_hash = v_new_bank_details_hash THEN
      v_bank_routing_unchanged := true;
    END IF;

    v_readiness_identity_checked := v_bank_routing_unchanged IS TRUE OR v_payee_route_unchanged IS TRUE;

    IF v_readiness_identity_checked IS TRUE THEN
      v_projection_class := 'READINESS_ONLY';
      v_projection_mode := 'READINESS_PATCH';
      v_routing_decision := 'PATCH_ONLY';
      v_scope_status := 'DELTA_REFRESH_PENDING';
      v_would_fast_path_allowed := true;
      v_fallback_reason := NULL::text;
    ELSE
      v_projection_class := 'BANK_ROUTING_CHANGE';
      v_fallback_reason := 'READINESS_CHANGE_WITH_UNVERIFIED_BANK_ROUTING_IDENTITY';
    END IF;
  ELSIF v_trigger_table = 'timesheet_pay_state' THEN
    IF v_timesheet_pay_state_bookkeeping_ignored IS TRUE OR v_timesheet_pay_state_noop_ignored IS TRUE THEN
      v_projection_class := 'TIMESHEET_PAY_STATE_BOOKKEEPING';
      v_projection_mode := 'BLOCKED';
      v_routing_decision := 'BLOCKED';
      v_scope_status := 'NOOP';
      v_resolved_mode := 'BLOCKED';
      v_would_fast_path_allowed := false;
      v_fallback_reason := CASE
        WHEN v_timesheet_pay_state_bookkeeping_ignored IS TRUE THEN 'TIMESHEET_PAY_STATE_BOOKKEEPING_IGNORED'
        ELSE 'TIMESHEET_PAY_STATE_NOOP_IGNORED'
      END;
      v_timesheet_pay_state_routing_reason := v_fallback_reason;
    ELSIF v_targeted_count = 0 THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'TARGETED_TIMESHEET_IDS_REQUIRED_FOR_PAY_STATE';
      v_timesheet_pay_state_routing_reason := v_fallback_reason;
    ELSIF v_valid_targeted_owner_count <> v_targeted_count THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'PAY_STATE_TARGETED_TIMESHEET_NOT_IN_CANDIDATE_SCOPE';
      v_timesheet_pay_state_routing_reason := v_fallback_reason;
    ELSIF v_linked_count > 0 AND v_valid_linked_owner_count <> v_linked_count THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'PAY_STATE_LINKED_TIMESHEET_NOT_IN_CANDIDATE_SCOPE';
      v_timesheet_pay_state_routing_reason := v_fallback_reason;
    ELSE
      v_projection_class := 'TIMESHEET_PAY_STATE';
      v_fallback_reason := CASE
        WHEN v_timesheet_pay_state_settlement_changed IS TRUE THEN 'SOURCE_BUILD_REQUIRED_PAY_STATE_SETTLED_BASELINE_CHANGE'
        ELSE 'SOURCE_BUILD_REQUIRED_PAY_STATE_UNCLASSIFIED_CHANGE'
      END;
      v_timesheet_pay_state_routing_reason := v_fallback_reason;
    END IF;
  ELSIF v_trigger_table IN ('timesheets', 'timesheets_financials') THEN
    IF v_targeted_count = 0 THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'TARGETED_TIMESHEET_IDS_REQUIRED';
    ELSIF v_valid_targeted_owner_count <> v_targeted_count THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'TARGETED_TIMESHEET_NOT_IN_CANDIDATE_SCOPE';
    ELSIF v_linked_count > 0 AND v_valid_linked_owner_count <> v_linked_count THEN
      v_projection_class := 'TARGET_SCOPE_MISSING';
      v_fallback_reason := 'LINKED_TIMESHEET_NOT_IN_CANDIDATE_SCOPE';
    ELSE
      v_projection_class := 'NORMAL_TIMESHEET';
      v_projection_mode := 'DELTA';
      v_routing_decision := 'DELTA';
      v_scope_status := 'DELTA_REFRESH_PENDING';
      v_would_fast_path_allowed := true;
      v_fallback_reason := NULL::text;
    END IF;
  ELSIF v_refresh_scope_kind IN ('CANDIDATE_FULL_LIVE', 'SESSION_FULL_LIVE', 'BROAD_SESSION_REFRESH') THEN
    v_projection_class := 'BROAD_SESSION_REFRESH';
    v_fallback_reason := 'BROAD_SESSION_REFRESH';
  ELSE
    v_has_unknown_trigger := true;
    v_projection_class := 'UNKNOWN_TRIGGER';
    v_fallback_reason := 'UNKNOWN_TRIGGER';
  END IF;

  v_complexity_flags := v_complexity_flags || jsonb_build_object(
    'has_unknown_trigger', COALESCE(v_has_unknown_trigger, false),
    'delta_refresh_enabled', COALESCE(v_delta_refresh_enabled, false),
    'delta_shadow_mode', COALESCE(v_delta_shadow_mode, true),
    'payload_shadow_mode', COALESCE(v_payload_shadow_mode, false),
    'enable_normal_timesheet', COALESCE(v_enable_normal_timesheet, false),
    'enable_readiness_only', COALESCE(v_enable_readiness_only, false),
    'enable_reservation_only', COALESCE(v_enable_reservation_only, false),
    'fallback_on_mismatch', COALESCE(v_fallback_on_mismatch, true),
    'clone_rebase_enabled', COALESCE(v_clone_rebase_enabled, false),
    'bank_routing_unchanged', COALESCE(v_bank_routing_unchanged, false),
    'payee_route_unchanged', COALESCE(v_payee_route_unchanged, false),
    'readiness_identity_checked', COALESCE(v_readiness_identity_checked, false),
    'explicit_banking_pay_action', COALESCE(v_explicit_banking_pay_action, false),
    'banking_pay_dirty_required', COALESCE(v_payload_banking_pay_dirty_required, v_explicit_banking_pay_action, false),
    'ordinary_timesheet_edit_save_no_dirty', COALESCE(v_payload_ordinary_no_dirty, false),
    'authorise_boundary_changed', COALESCE(v_timesheet_authorise_boundary_changed, false),
    'unauthorise_boundary_changed', COALESCE(v_timesheet_unauthorise_boundary_changed, false),
    'archive_boundary_changed', COALESCE(v_timesheet_archive_boundary_changed, false),
    'unarchive_boundary_changed', COALESCE(v_timesheet_unarchive_boundary_changed, false),
    'timesheet_pay_state_event', v_trigger_table = 'timesheet_pay_state',
    'timesheet_pay_state_settlement_changed', COALESCE(v_timesheet_pay_state_settlement_changed, false),
    'timesheet_pay_state_summary_changed', COALESCE(v_timesheet_pay_state_summary_changed, false),
    'timesheet_pay_state_bookkeeping_ignored', COALESCE(v_timesheet_pay_state_bookkeeping_ignored, false),
    'timesheet_pay_state_noop_ignored', COALESCE(v_timesheet_pay_state_noop_ignored, false),
    'timesheet_pay_state_routing_reason', v_timesheet_pay_state_routing_reason,
    'lifecycle_event_class', v_lifecycle_event_class,
    'lifecycle_admission_reason', v_lifecycle_admission->>'reason',
    'enable_simple_authorise', v_enable_simple_authorise,
    'enable_simple_unauthorise', v_enable_simple_unauthorise,
    'trigger_table', v_trigger_table,
    'trigger_operation', v_trigger_operation
  );

  IF v_would_fast_path_allowed IS TRUE THEN
    IF v_projection_class = 'NORMAL_TIMESHEET' THEN
      IF v_delta_refresh_enabled IS NOT TRUE THEN
        v_fallback_reason := 'DELTA_REFRESH_DISABLED';
      ELSIF v_delta_shadow_mode IS TRUE OR v_payload_shadow_mode IS TRUE THEN
        v_fallback_reason := CASE WHEN v_payload_shadow_mode IS TRUE THEN 'DELTA_PAYLOAD_SHADOW_MODE' ELSE 'DELTA_SHADOW_MODE' END;
      ELSIF v_enable_normal_timesheet IS NOT TRUE THEN
        v_fallback_reason := 'NORMAL_TIMESHEET_DELTA_DISABLED';
      ELSE
        v_resolved_mode := 'DELTA';
        v_fast_path_allowed := true;
      END IF;
    ELSIF v_projection_class = 'TIMESHEET_LIFECYCLE' THEN
      IF v_delta_refresh_enabled IS NOT TRUE THEN
        v_fallback_reason := 'DELTA_REFRESH_DISABLED';
      ELSIF v_delta_shadow_mode IS TRUE OR v_payload_shadow_mode IS TRUE THEN
        v_fallback_reason := CASE WHEN v_payload_shadow_mode THEN 'DELTA_PAYLOAD_SHADOW_MODE' ELSE 'DELTA_SHADOW_MODE' END;
      ELSIF v_lifecycle_event_class='AUTHORISE' AND v_enable_simple_authorise IS NOT TRUE THEN
        v_fallback_reason := 'SIMPLE_AUTHORISE_DELTA_DISABLED';
      ELSIF v_lifecycle_event_class='UNAUTHORISE' AND v_enable_simple_unauthorise IS NOT TRUE THEN
        v_fallback_reason := 'SIMPLE_UNAUTHORISE_DELTA_DISABLED';
      ELSE
        v_resolved_mode := 'DELTA';
        v_fast_path_allowed := true;
      END IF;
    ELSIF v_projection_class = 'READINESS_ONLY' THEN
      IF v_patch_after_batch_enabled IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'PATCH_AFTER_BATCH_MUTATION_DISABLED';
      ELSIF v_delta_refresh_enabled IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'DELTA_REFRESH_DISABLED_FOR_READINESS_PATCH';
      ELSIF v_delta_shadow_mode IS TRUE OR v_payload_shadow_mode IS TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := CASE WHEN v_payload_shadow_mode IS TRUE THEN 'READINESS_PATCH_PAYLOAD_SHADOW_MODE' ELSE 'READINESS_PATCH_SHADOW_MODE' END;
      ELSIF v_enable_readiness_only IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'READINESS_ONLY_PATCH_DISABLED';
      ELSE
        v_resolved_mode := 'PATCH_ONLY';
        v_fast_path_allowed := true;
      END IF;
    ELSIF v_projection_class = 'RESERVATION_ONLY' THEN
      IF v_patch_after_batch_enabled IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'PATCH_AFTER_BATCH_MUTATION_DISABLED';
      ELSIF v_delta_refresh_enabled IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'DELTA_REFRESH_DISABLED_FOR_RESERVATION_PATCH';
      ELSIF v_delta_shadow_mode IS TRUE OR v_payload_shadow_mode IS TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := CASE WHEN v_payload_shadow_mode IS TRUE THEN 'RESERVATION_PATCH_PAYLOAD_SHADOW_MODE' ELSE 'RESERVATION_PATCH_SHADOW_MODE' END;
      ELSIF v_enable_reservation_only IS NOT TRUE THEN
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'RESERVATION_ONLY_PATCH_DISABLED';
      ELSE
        v_resolved_mode := 'PATCH_ONLY';
        v_fast_path_allowed := true;
      END IF;
    ELSIF v_projection_class = 'CLONE_REBASE' THEN
      IF v_clone_rebase_enabled IS TRUE THEN
        v_resolved_mode := 'CLONE_REBASE';
        v_fast_path_allowed := true;
        v_fallback_reason := NULL::text;
      ELSE
        v_resolved_mode := 'BLOCKED';
        v_fallback_reason := 'CLONE_REBASE_DISABLED';
      END IF;
    ELSE
      v_fallback_reason := COALESCE(v_fallback_reason, 'LEGACY_REQUIRED');
    END IF;
  END IF;

  IF v_fast_path_allowed IS TRUE AND v_resolved_mode = 'DELTA' THEN
    v_resolved_job_type := 'WORKBENCH_CANDIDATE_DELTA_REFRESH';
    v_projection_mode := 'DELTA';
    v_routing_decision := 'DELTA';
    v_scope_status := 'DELTA_REFRESH_PENDING';
    v_fallback_required := false;
    v_fallback_reason := NULL::text;
  ELSIF v_fast_path_allowed IS TRUE AND v_resolved_mode = 'PATCH_ONLY' THEN
    v_resolved_job_type := 'PATCH_ONLY';
    v_routing_decision := 'PATCH_ONLY';
    v_scope_status := 'READY';
    v_fallback_required := false;
    v_fallback_reason := NULL::text;
  ELSIF v_fast_path_allowed IS TRUE AND v_resolved_mode = 'CLONE_REBASE' THEN
    v_resolved_job_type := 'WORKBENCH_SESSION_CLONE_REBASE';
    v_projection_mode := 'CLONE_REBASE';
    v_routing_decision := 'CLONE_REBASE';
    v_scope_status := 'CLONE_REBASING';
    v_fallback_required := false;
    v_fallback_reason := NULL::text;
  ELSIF v_resolved_mode = 'BLOCKED' THEN
    v_resolved_job_type := 'NOOP';
    v_projection_mode := 'BLOCKED';
    v_routing_decision := 'BLOCKED';
    v_scope_status := 'NOOP';
    v_fallback_required := false;
    IF v_fallback_reason IS NULL THEN
      v_fallback_reason := 'BLOCKED_BY_CLASSIFIER';
    END IF;
  ELSE
    v_resolved_mode := 'LEGACY';
    v_resolved_job_type := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
    v_projection_mode := 'LEGACY';
    v_scope_status := 'SOURCE_BUILD_PENDING';
    v_routing_decision := 'LEGACY';
    v_fallback_required := true;
    IF v_fallback_reason IS NULL THEN
      v_fallback_reason := 'LEGACY_REQUIRED';
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'fast_path_allowed', COALESCE(v_fast_path_allowed, false),
    'would_fast_path_allowed', COALESCE(v_would_fast_path_allowed, false),
    'resolved_mode', v_resolved_mode,
    'resolved_job_type', v_resolved_job_type,
    'projection_mode', v_projection_mode,
    'projection_class', v_projection_class,
    'routing_decision', v_routing_decision,
    'scope_status', v_scope_status,
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'source_change_seq', COALESCE(v_source_change_seq, 0),
    'session_version', v_session_row.version,
    'payload_session_version', v_payload_session_version,
    'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
    'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE(to_jsonb(v_all_affected_timesheet_ids), '[]'::jsonb),
    'fallback_required', COALESCE(v_fallback_required, true),
    'no_op', v_resolved_mode = 'BLOCKED',
    'banking_pay_dirty_required', v_resolved_mode IN ('DELTA', 'PATCH_ONLY', 'LEGACY'),
    'fallback_reason', v_fallback_reason,
    'complexity_flags', COALESCE(v_complexity_flags, '{}'::jsonb)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(p_job_id uuid, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_payload jsonb := '{}'::jsonb;
  v_candidate_id uuid;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_dependency_closure_json jsonb := '{}'::jsonb;
  v_dependency_closure_requires_full boolean := false;
  v_dependency_closure_reason text := NULL::text;
  v_family_scope_json jsonb := '{}'::jsonb;
  v_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_canonical_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_canonical_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_reason text := 'DIRTY_TRIGGER:CANDIDATE';
  v_payload_seq bigint := 0;
  v_live_seq bigint := 0;
  v_processed_source_change_seq bigint := 0;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_updated integer := 0;
  v_scope_count integer := 0;
  v_source_dirty_count integer := 0;
  v_source_dirty_total integer := 0;
  v_line_work_count integer := 0;
  v_line_work_total integer := 0;
  v_preview_count integer := 0;
  v_preview_total integer := 0;
  v_session_count integer := 0;
  v_jobs_queued integer := 0;
  v_refresh_result jsonb := '{}'::jsonb;
  v_preflight_result jsonb := '{}'::jsonb;
  v_preflight_action text := 'PROCEED';
  v_preflight_match_count integer := 0;
  v_dirty_marking_skipped boolean := false;
  v_is_authorise_delta_targeted boolean := false;
  v_lifecycle_context text := NULL::text;
  v_candidate_serial_state jsonb := '{}'::jsonb;
  v_candidate_serial_blocked boolean := false;
  v_scope_ensure_result jsonb := '{}'::jsonb;
  v_new_scope_baseline_required boolean := false;
  v_session_scan_cutoff_created_at timestamptz;
  v_session_scan_cutoff_id uuid;
  v_session_scan_last_created_at timestamptz;
  v_session_scan_last_id uuid;
  v_session_scan_has_more boolean := false;
  v_session_scan_started_at timestamptz;
  v_session_scan_sessions_examined bigint := 0;
  v_session_page_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_lock_acquired boolean := false;
  v_private_cursor jsonb := '{}'::jsonb;
  v_cursor_sequence bigint := NULL::bigint;
  v_reuse_selection jsonb := '{}'::jsonb;
  v_reuse_job_id uuid := NULL::uuid;
  v_reuse_source_session_id uuid := NULL::uuid;
  v_reuse_dedupe_key text := NULL::text;
BEGIN
  SELECT job_row.*
  INTO v_job
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.id = p_job_id
  FOR UPDATE;

  IF v_job.id IS NULL THEN
    RAISE EXCEPTION 'Banking Pay candidate dirty apply job not found: %', p_job_id
      USING ERRCODE = 'P0002';
  END IF;
  IF UPPER(BTRIM(COALESCE(v_job.job_type, ''))) <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY' THEN
    RAISE EXCEPTION 'Job % is %, not WORKBENCH_CANDIDATE_DIRTY_APPLY', p_job_id, v_job.job_type
      USING ERRCODE = '22023';
  END IF;
  IF UPPER(BTRIM(COALESCE(v_job.status, ''))) <> 'RUNNING' THEN
    RAISE EXCEPTION 'Job % must be RUNNING before processing; status=%', p_job_id, v_job.status
      USING ERRCODE = '55000';
  END IF;

  v_payload := COALESCE(v_job.payload_json, '{}'::jsonb);
  v_candidate_id := COALESCE(
    v_job.candidate_id,
    CASE WHEN COALESCE(v_payload->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (v_payload->>'candidate_id')::uuid END
  );
  IF v_candidate_id IS NULL THEN
    RAISE EXCEPTION 'Candidate dirty apply job % has no candidate_id', p_job_id
      USING ERRCODE = '22023';
  END IF;

  v_candidate_lock_acquired := pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_candidate_id),
      24062027
    )
  );

  IF v_candidate_lock_acquired THEN
    v_candidate_serial_state := public._pay_workbench_candidate_serial_active_state(
      p_job_id,
      v_candidate_id,
      v_job.job_type,
      v_payload,
      v_now
    );
  END IF;
  v_candidate_serial_blocked := NOT v_candidate_lock_acquired
    OR lower(BTRIM(COALESCE(v_candidate_serial_state->>'blocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_candidate_serial_blocked IS TRUE THEN
    UPDATE public.banking_pay_workbench_jobs AS delayed_job
    SET status = 'QUEUED',
        attempt_count = GREATEST(COALESCE(delayed_job.attempt_count, 0) - 1, 0),
        run_at_utc = GREATEST(COALESCE(delayed_job.run_at_utc, v_now), v_now + interval '5 seconds'),
        started_at_utc = NULL,
        updated_at_utc = v_now,
        payload_json = public._pay_workbench_dirty_payload_merge(
          COALESCE(delayed_job.payload_json, '{}'::jsonb),
          jsonb_strip_nulls(jsonb_build_object(
            'candidate_serial_key', public._pay_workbench_candidate_serial_key(v_candidate_id),
            'candidate_serial_candidate_id', v_candidate_id::text,
            'candidate_serial_blocked_by_job_id', v_candidate_serial_state->>'blocked_job_id',
            'candidate_serial_blocked_by_chain_job_id', v_candidate_serial_state->>'blocked_chain_job_id',
            'candidate_serial_blocked_by_projection_run_id', v_candidate_serial_state->>'projection_run_id',
            'candidate_serial_wait_reason', COALESCE(v_candidate_serial_state->>'reason', CASE WHEN NOT v_candidate_lock_acquired THEN 'CANDIDATE_SERIAL_LOCK_BUSY' ELSE 'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED' END),
            'candidate_serial_delayed_at_utc', v_now::text,
            'dirty_apply_row_marking_applied', false,
            'dirty_marking_skipped', true,
            'session_progress_dirtying_skipped', true,
            'classifier_work_skipped_by_candidate_serial', true,
            'source_build_enqueue_skipped_by_candidate_serial', true,
            'line_work_enqueue_skipped_by_candidate_serial', true,
            'preview_materialise_enqueue_skipped_by_candidate_serial', true,
            'rerun_required', true,
            'has_more', true,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ))
        )
    WHERE delayed_job.id = p_job_id;

    PERFORM public._pay_workbench_candidate_serial_audit(
      'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED',
      p_job_id,
      v_candidate_id,
      COALESCE(v_candidate_serial_state, '{}'::jsonb) || jsonb_build_object(
        'job_type', v_job.job_type,
        'dirty_apply_row_marking_applied', false,
        'dirty_marking_skipped', true,
        'delayed_until_utc', (v_now + interval '5 seconds')::text
      ),
      'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED',
      NULL::uuid
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', p_job_id::text,
      'candidate_id', v_candidate_id::text,
      'candidate_serial_delayed', true,
      'has_more', true,
      'rerun_required', true,
      'next_cursor_json', COALESCE(v_job.private_cursor_json, '{}'::jsonb),
      'reason', COALESCE(v_candidate_serial_state->>'reason', CASE WHEN NOT v_candidate_lock_acquired THEN 'CANDIDATE_SERIAL_LOCK_BUSY' ELSE 'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED' END),
      'dirty_apply_row_marking_applied', false,
      'dirty_marking_skipped', true,
      'session_progress_dirtying_skipped', true,
      'classifier_work_skipped_by_candidate_serial', true,
      'source_build_enqueue_skipped_by_candidate_serial', true,
      'line_work_enqueue_skipped_by_candidate_serial', true,
      'preview_materialise_enqueue_skipped_by_candidate_serial', true,
      'elapsed_ms', EXTRACT(MILLISECONDS FROM clock_timestamp() - v_started_at)
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_payload->'targeted_timesheet_ids') = 'array' THEN v_payload->'targeted_timesheet_ids' ELSE '[]'::jsonb END) AS raw_value(value_text)
  WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_linked_timesheet_ids
  FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_payload->'linked_timesheet_ids') = 'array' THEN v_payload->'linked_timesheet_ids' ELSE '[]'::jsonb END) AS raw_value(value_text)
  WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_payload->'finance_case_ids') = 'array'
        THEN v_payload->'finance_case_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_value(value_text)
  WHERE raw_value.value_text
    ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_linked_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    IF to_regprocedure(
         'public._pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)'
       ) IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_REFRESH_DEPENDENCY_CLOSURE_UNAVAILABLE'
        USING ERRCODE = 'P0001';
    END IF;

    EXECUTE
      'SELECT public._pay_workbench_refresh_dependency_closure_v1($1,$2,$3,$4,$5,$6)'
    INTO v_dependency_closure_json
    USING
      v_candidate_id,
      v_targeted_timesheet_ids,
      v_linked_timesheet_ids,
      v_finance_case_ids,
      250,
      100;

    v_dependency_closure_requires_full :=
      LOWER(BTRIM(COALESCE(
        v_dependency_closure_json->>'requires_full_candidate',
        'true'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR LOWER(BTRIM(COALESCE(
        v_dependency_closure_json->>'coverage_complete',
        'false'
      ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on');
    v_dependency_closure_reason := NULLIF(BTRIM(COALESCE(
      v_dependency_closure_json->>'fallback_reason',
      ''
    )), '');

    IF v_dependency_closure_requires_full THEN
      v_targeted_timesheet_ids := ARRAY[]::uuid[];
      v_linked_timesheet_ids := ARRAY[]::uuid[];
      v_finance_case_ids := ARRAY[]::uuid[];
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    ELSE
      SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
      INTO v_targeted_timesheet_ids
      FROM jsonb_array_elements_text(
        COALESCE(v_dependency_closure_json->'effective_targeted_timesheet_ids', '[]'::jsonb)
      ) AS effective_timesheet(value)
      WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

      v_linked_timesheet_ids := ARRAY[]::uuid[];

      SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
      INTO v_finance_case_ids
      FROM jsonb_array_elements_text(
        COALESCE(v_dependency_closure_json->'effective_finance_case_ids', '[]'::jsonb)
      ) AS effective_finance_case(value)
      WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
    END IF;
  END IF;

  IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    v_family_scope_json := public._pay_workbench_normalise_timesheet_rotation_scope_payload(v_targeted_timesheet_ids, v_linked_timesheet_ids);

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_family_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'family_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_canonical_targeted_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'targeted_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_canonical_linked_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'linked_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT all_ids.timesheet_id ORDER BY all_ids.timesheet_id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
    FROM (
      SELECT unnest(v_targeted_timesheet_ids) AS timesheet_id
      UNION ALL
      SELECT unnest(v_linked_timesheet_ids) AS timesheet_id
      UNION ALL
      SELECT unnest(v_family_timesheet_ids) AS timesheet_id
    ) AS all_ids
    WHERE all_ids.timesheet_id IS NOT NULL;
    v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
  ELSE
    v_linked_timesheet_ids := ARRAY[]::uuid[];
    v_family_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_targeted_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_linked_timesheet_ids := ARRAY[]::uuid[];
    v_all_timesheet_ids := ARRAY[]::uuid[];
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
  END IF;

  v_reason := COALESCE(NULLIF(BTRIM(COALESCE(v_payload->>'reason_latest', v_payload->>'reason', '')), ''), 'DIRTY_TRIGGER:CANDIDATE');
  v_payload_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_payload->>'latest_source_change_seq', '') ~ '^\d+$' THEN (v_payload->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_seq', '') ~ '^\d+$' THEN (v_payload->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_sequence', '') ~ '^\d+$' THEN (v_payload->>'source_change_sequence')::bigint END, 0)
  );

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || v_candidate_id::text;

  v_processed_source_change_seq := GREATEST(COALESCE(v_payload_seq, 0), COALESCE(v_live_seq, 0));
  IF v_job.private_cursor_kind IS NOT NULL
     AND v_job.private_cursor_kind <> 'DIRTY_SESSION_SCAN_V1' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_SESSION_CURSOR_KIND_INVALID'
      USING ERRCODE='P0001';
  END IF;

  v_private_cursor := COALESCE(v_job.private_cursor_json, '{}'::jsonb);
  BEGIN
    v_cursor_sequence := CASE
      WHEN COALESCE(v_private_cursor->>'scan_source_change_seq','') ~ '^\d+$'
        THEN (v_private_cursor->>'scan_source_change_seq')::bigint
      ELSE NULL::bigint
    END;
    v_session_scan_cutoff_created_at := NULLIF(BTRIM(COALESCE(v_private_cursor->>'upper_created_at_utc','')),'')::timestamptz;
    v_session_scan_cutoff_id := NULLIF(BTRIM(COALESCE(v_private_cursor->>'upper_session_id','')),'')::uuid;
    v_session_scan_last_created_at := NULLIF(BTRIM(COALESCE(v_private_cursor->>'last_created_at_utc','')),'')::timestamptz;
    v_session_scan_last_id := NULLIF(BTRIM(COALESCE(v_private_cursor->>'last_session_id','')),'')::uuid;
    v_session_scan_started_at := COALESCE(
      NULLIF(BTRIM(COALESCE(v_private_cursor->>'scan_started_at_utc','')),'')::timestamptz,
      v_now
    );
    v_session_scan_sessions_examined := COALESCE(
      CASE WHEN COALESCE(v_private_cursor->>'sessions_examined','') ~ '^\d+$'
        THEN (v_private_cursor->>'sessions_examined')::bigint END,
      0
    );
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_SESSION_CURSOR_INVALID' USING ERRCODE='P0001';
  END;

  IF v_job.private_cursor_kind IS NULL
     OR v_job.private_stage_version IS DISTINCT FROM 1
     OR v_cursor_sequence IS DISTINCT FROM v_processed_source_change_seq THEN
    SELECT session_candidate.created_at_utc, session_candidate.id
    INTO v_session_scan_cutoff_created_at, v_session_scan_cutoff_id
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.status='OPEN'
      AND session_candidate.discarded_at_utc IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=session_candidate.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (session_candidate.pay_date,session_candidate.created_at_utc,session_candidate.id)
      )
    ORDER BY session_candidate.created_at_utc DESC,session_candidate.id DESC
    LIMIT 1;

    v_session_scan_last_created_at := NULL::timestamptz;
    v_session_scan_last_id := NULL::uuid;
    v_session_scan_started_at := v_now;
    v_session_scan_sessions_examined := 0;

    UPDATE public.banking_pay_workbench_jobs AS cursor_job
    SET private_cursor_kind='DIRTY_SESSION_SCAN_V1',
        private_stage_version=1,
        private_cursor_json=jsonb_strip_nulls(jsonb_build_object(
          'scan_source_change_seq',v_processed_source_change_seq,
          'upper_created_at_utc',v_session_scan_cutoff_created_at,
          'upper_session_id',v_session_scan_cutoff_id,
          'last_created_at_utc',NULL,
          'last_session_id',NULL,
          'scan_started_at_utc',v_session_scan_started_at,
          'sessions_examined',0
        )),
        updated_at_utc=v_now
    WHERE cursor_job.id=p_job_id;
  END IF;
  v_lifecycle_context := LOWER(BTRIM(COALESCE(v_payload->>'lifecycle_mutation_context', v_payload->>'mutation_context', v_payload->>'lifecycle_context', '')));
  v_is_authorise_delta_targeted := v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
    AND COALESCE(array_length(v_canonical_targeted_timesheet_ids, 1), 0) > 0
    AND COALESCE(array_length(v_finance_case_ids, 1), 0) = 0
    AND lower(BTRIM(COALESCE(v_payload->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) NOT IN ('true','t','1','yes','y','on')
    AND (
      lower(BTRIM(COALESCE(v_payload->>'authorise_boundary_changed', v_payload->>'timesheet_authorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
      OR lower(BTRIM(COALESCE(v_payload->>'unauthorise_boundary_changed', v_payload->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
      OR v_lifecycle_context IN ('timesheet_authorise', 'authorise_timesheet', 'timesheet_unauthorise', 'unauthorise_timesheet')
      OR LOWER(COALESCE(v_reason, '')) LIKE '%authorise%'
      OR LOWER(COALESCE(v_reason, '')) LIKE '%unauthorise%'
    );

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'dirty_worker_apply_start', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0), 'family_timesheet_count', COALESCE(array_length(v_family_timesheet_ids, 1), 0), 'latest_source_change_seq', v_payload_seq, 'processed_source_change_seq', v_processed_source_change_seq));

  SELECT COALESCE(array_agg(page_row.id ORDER BY page_row.created_at_utc,page_row.id),ARRAY[]::uuid[])
  INTO v_session_page_ids
  FROM (
    SELECT session_candidate.id,session_candidate.created_at_utc
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.status='OPEN'
      AND session_candidate.discarded_at_utc IS NULL
      AND v_session_scan_cutoff_created_at IS NOT NULL
      AND (session_candidate.created_at_utc,session_candidate.id)<=
          (v_session_scan_cutoff_created_at,v_session_scan_cutoff_id)
      AND (
        v_session_scan_last_created_at IS NULL
        OR (session_candidate.created_at_utc,session_candidate.id)>
           (v_session_scan_last_created_at,v_session_scan_last_id)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=session_candidate.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (session_candidate.pay_date,session_candidate.created_at_utc,session_candidate.id)
      )
    ORDER BY session_candidate.created_at_utc,session_candidate.id
    LIMIT GREATEST(1, COALESCE(p_limit, 100))
  ) AS page_row;

  PERFORM session_lock.id
  FROM public.banking_pay_workbench_sessions AS session_lock
  WHERE session_lock.id=ANY(v_session_page_ids)
  ORDER BY session_lock.id
  FOR UPDATE;

  FOR v_session_row IN
    SELECT session_candidate.*
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.id=ANY(v_session_page_ids)
    ORDER BY session_candidate.created_at_utc,session_candidate.id
  LOOP
    v_session_scan_last_created_at:=v_session_row.created_at_utc;
    v_session_scan_last_id:=v_session_row.id;
    v_session_scan_sessions_examined:=v_session_scan_sessions_examined+1;
    IF NOT EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_session_scope AS existing_scope
      WHERE existing_scope.session_id=v_session_row.id
        AND existing_scope.candidate_id=v_candidate_id
    ) THEN
      v_scope_ensure_result:=private.pay_workbench_session_candidate_scope_ensure_v1(
        v_session_row.id,v_candidate_id,p_job_id,v_processed_source_change_seq,v_reason
      );
      IF COALESCE((v_scope_ensure_result->>'eligible')::boolean,false) IS NOT TRUE THEN
        CONTINUE;
      END IF;
      v_new_scope_baseline_required:=COALESCE((v_scope_ensure_result->>'inserted')::boolean,false);
    ELSE
      v_new_scope_baseline_required:=false;
    END IF;
    IF v_new_scope_baseline_required IS TRUE THEN
      v_reuse_selection := private.pay_workbench_candidate_reuse_source_select_v1(
        v_session_row.id,
        v_candidate_id,
        v_processed_source_change_seq,
        jsonb_build_object(
          'source_job_id',p_job_id::text,
          'direct_candidate_id',v_candidate_id::text,
          'target_session_id',v_session_row.id::text
        )
      );

      IF coalesce((v_reuse_selection->>'reuse_available')::boolean,false) IS TRUE
         AND coalesce(v_reuse_selection->>'selected_source_session_id','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_reuse_source_session_id := (v_reuse_selection->>'selected_source_session_id')::uuid;
        v_reuse_dedupe_key := 'workbench:certified-reuse-v2:'
          ||v_session_row.id::text||':'||v_candidate_id::text||':'||v_processed_source_change_seq::text;

        INSERT INTO public.banking_pay_workbench_jobs AS reuse_job(
          id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,
          dedupe_key,snapshot_run_id,session_id,candidate_id,payload_json,
          created_at_utc,updated_at_utc,started_at_utc,completed_at_utc,
          failed_at_utc,last_error_json
        ) VALUES (
          gen_random_uuid(),'WORKBENCH_SESSION_CLONE_REBASE','QUEUED',42,v_now,0,8,
          v_reuse_dedupe_key,v_session_row.source_snapshot_run_id,v_session_row.id,v_candidate_id,
          jsonb_strip_nulls(jsonb_build_object(
            'job_type','WORKBENCH_SESSION_CLONE_REBASE',
            'source_session_id',v_reuse_source_session_id::text,
            'target_session_id',v_session_row.id::text,
            'direct_candidate_id',v_candidate_id::text,
            'source_change_seq',v_processed_source_change_seq,
            'source_job_id',p_job_id::text,
            'source_selection_authorised',true,
            'allow_session_rebase',true,
            'rebase_simple_rows_only',true,
            'clone_mode','CERTIFIED_ONLY',
            'cursor_json','{}'::jsonb,
            'limit',1,
            'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
          )),
          v_now,v_now,NULL,NULL,NULL,NULL
        )
        ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED','RUNNING')
        DO UPDATE SET
          run_at_utc=LEAST(reuse_job.run_at_utc,EXCLUDED.run_at_utc),
          payload_json=coalesce(reuse_job.payload_json,'{}'::jsonb)||EXCLUDED.payload_json,
          updated_at_utc=v_now
        RETURNING reuse_job.id INTO v_reuse_job_id;

        UPDATE public.banking_pay_workbench_session_scope AS reuse_scope
        SET status='PENDING',dirty=true,pending_job_id=v_reuse_job_id,
            error_json=NULL::jsonb,
            certified_preview_publication_required=true,
            certified_preview_publication_parity_ok=false,
            certified_preview_publication_session_version=NULL,
            certified_preview_publication_source_change_seq=NULL,
            certified_preview_publication_source_build_run_id=NULL,
            certified_preview_publication_attestation_json='{}'::jsonb,
            certified_preview_publication_attested_at_utc=NULL,
            updated_at_utc=v_now
        WHERE reuse_scope.session_id=v_session_row.id
          AND reuse_scope.candidate_id=v_candidate_id;

        v_jobs_queued:=v_jobs_queued+1;
        v_session_count:=v_session_count+1;
        v_refresh_result:=jsonb_build_object(
          'ok',true,
          'job_id',v_reuse_job_id::text,
          'job_type','WORKBENCH_SESSION_CLONE_REBASE',
          'scope_status','PENDING',
          'source_build_required',false,
          'certified_reuse_pending',true
        );
        CONTINUE;
      END IF;
    END IF;


    IF v_is_authorise_delta_targeted AND NOT v_new_scope_baseline_required THEN
      SELECT public.pay_workbench_authorise_delta_hotkey_preflight(
        p_session_id => v_session_row.id,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_canonical_targeted_timesheet_ids,
        p_linked_timesheet_ids => v_canonical_linked_timesheet_ids,
        p_payload_json => public._pay_workbench_merge_targeted_scope_payload(
          v_payload,
          jsonb_build_object(
            'session_id', v_session_row.id::text,
            'source_session_id', v_session_row.id::text,
            'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
            'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
            'session_version', COALESCE(v_session_row.version, 0),
            'session_signature', v_session_row.session_signature,
            'refresh_scope_kind', 'TARGETED_TIMESHEETS',
            'projection_mode', 'DELTA',
            'projection_class', 'TIMESHEET_LIFECYCLE',
            'targeted_timesheet_ids', COALESCE(v_family_scope_json->'targeted_timesheet_ids', to_jsonb(v_canonical_targeted_timesheet_ids)),
            'linked_timesheet_ids', COALESCE(v_family_scope_json->'linked_timesheet_ids', to_jsonb(v_canonical_linked_timesheet_ids)),
            'queue_identity_targeted_timesheet_ids', COALESCE(v_family_scope_json->'queue_identity_targeted_timesheet_ids', v_family_scope_json->'family_timesheet_ids', to_jsonb(v_canonical_targeted_timesheet_ids)),
            'queue_identity_linked_timesheet_ids', COALESCE(v_family_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
            'requested_timesheet_ids', COALESCE(v_family_scope_json->'requested_timesheet_ids', to_jsonb(v_targeted_timesheet_ids)),
            'targeted_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids)),
            'linked_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_linked_timesheet_ids', to_jsonb(v_linked_timesheet_ids)),
            'source_change_seq', v_processed_source_change_seq,
            'latest_source_change_seq', v_processed_source_change_seq,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        p_reason => v_reason,
        p_actor_user_id => v_session_row.actor_user_id,
        p_source_change_seq => v_processed_source_change_seq
      ) INTO v_preflight_result;

      v_preflight_action := COALESCE(v_preflight_result->>'action', 'PROCEED');

      IF v_preflight_action IN ('REUSED_QUEUED_SAME_FAMILY_JOB', 'UPDATED_WAITING_AFTER_RUNNING_JOB') THEN
        v_preflight_match_count := v_preflight_match_count + 1;
        v_dirty_marking_skipped := true;
        v_jobs_queued := v_jobs_queued + CASE WHEN NULLIF(BTRIM(COALESCE(v_preflight_result->>'job_id', '')), '') IS NOT NULL THEN 1 ELSE 0 END;
        v_session_count := v_session_count + 1;

        UPDATE public.banking_pay_workbench_jobs AS job_update
        SET payload_json = public._pay_workbench_dirty_payload_merge(
              COALESCE(job_update.payload_json, '{}'::jsonb),
              jsonb_build_object(
                'processed_source_change_seq', v_processed_source_change_seq,
                'processed_at_utc', v_now::text,
                'processed_candidate_id', v_candidate_id::text,
                'source_change_seq', v_processed_source_change_seq,
                'source_change_sequence', v_processed_source_change_seq,
                'latest_source_change_seq', v_processed_source_change_seq,
                'rerun_required', false,
                'has_more', false,
                'cursor_json', '{}'::jsonb,
                'next_cursor_json', '{}'::jsonb,
                'dirty_apply_row_marking_applied', false,
                'dirty_marking_skipped', true,
                'session_progress_dirtying_skipped', true,
                'classifier_work_skipped', true,
                'source_rows_marking_skipped', true,
                'line_work_marking_skipped', true,
                'preview_marking_skipped', true,
                'early_preflight_action', v_preflight_action,
                'early_preflight_result', COALESCE(v_preflight_result, '{}'::jsonb),
                'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_preflight_result->>'job_id', '')), ''),
                'actual_refresh_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
                'actual_refresh_scope_status', 'DELTA_REFRESH_PENDING',
                'source_build_required', false,
                'delta_refresh_required', true,
                'line_work_action', 'EARLY_PREFLIGHT_SKIPPED_DIRTY_MARKING',
                'early_preflight_completed_cleanly', true,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            updated_at_utc = v_now
        WHERE job_update.id = p_job_id;

        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'early_preflight_return_before_dirty_marking', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'session_id', v_session_row.id::text, 'action', v_preflight_action, 'normalised_delta_family_key', v_preflight_result->>'normalised_delta_family_key', 'dirty_marking_skipped', true));

        CONTINUE;
      END IF;
    END IF;

    UPDATE public.banking_pay_workbench_session_scope AS scope_row
    SET status = 'SOURCE_BUILD_PENDING',
        dirty = true,
        error_json = NULL::jsonb,
        certified_preview_publication_required = true,
        certified_preview_publication_parity_ok = false,
        certified_preview_publication_session_version = NULL,
        certified_preview_publication_source_change_seq = NULL,
        certified_preview_publication_source_build_run_id = NULL,
        certified_preview_publication_attestation_json = '{}'::jsonb,
        certified_preview_publication_attested_at_utc = NULL,
        updated_at_utc = v_now
    WHERE scope_row.session_id = v_session_row.id
      AND scope_row.candidate_id = v_candidate_id;
    GET DIAGNOSTICS v_scope_updated = ROW_COUNT;
    v_scope_count := v_scope_count + COALESCE(v_scope_updated, 0);

    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line
    SET status = 'DIRTY',
        source_row_json = jsonb_strip_nulls(
          COALESCE(source_line.source_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_trigger_table', COALESCE(v_payload->>'trigger_table', 'dirty_apply_worker'),
            'dirty_trigger_operation', COALESCE(v_payload->>'trigger_op', 'APPLY'),
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE source_line.session_id = v_session_row.id
      AND source_line.candidate_id = v_candidate_id
      AND source_line.status = 'CURRENT'
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR source_line.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(source_line.source_row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (source_line.source_row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_source_dirty_count = ROW_COUNT;
    v_source_dirty_total := v_source_dirty_total + COALESCE(v_source_dirty_count, 0);

    UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
    SET status = 'PENDING',
        result_row_json = NULL::jsonb,
        error_json = NULL::jsonb,
        work_payload_json = jsonb_strip_nulls(
          COALESCE(line_work.work_payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE line_work.session_id = v_session_row.id
      AND line_work.candidate_id = v_candidate_id
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR line_work.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(line_work.result_row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (line_work.result_row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_line_work_count = ROW_COUNT;
    v_line_work_total := v_line_work_total + COALESCE(v_line_work_count, 0);

    UPDATE public.banking_pay_workbench_preview_rows AS preview_row
    SET status = 'DIRTY',
        selected = false,
        selection_state = 'DIRTY',
        row_json = jsonb_strip_nulls(
          COALESCE(preview_row.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE preview_row.session_id = v_session_row.id
      AND preview_row.candidate_id = v_candidate_id
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR preview_row.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (preview_row.row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_preview_count = ROW_COUNT;
    v_preview_total := v_preview_total + COALESCE(v_preview_count, 0);

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET progress_state = 'DIRTY',
        scope_pending_count = GREATEST(COALESCE(session_update.scope_pending_count, 0), 1),
        selected_row_count = GREATEST(COALESCE(session_update.selected_row_count, 0) - COALESCE(v_preview_count, 0), 0),
        candidate_sample_rows_json = jsonb_build_array(jsonb_build_object('candidate_id', v_candidate_id::text, 'status', 'SOURCE_BUILD_PENDING', 'reason', v_reason)),
        progress_json = jsonb_strip_nulls(
          COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_dirty_candidate_id', v_candidate_id::text,
            'last_dirty_reason', v_reason,
            'last_dirty_at_utc', v_now::text,
            'last_dirty_job_id', p_job_id::text,
            'last_dirty_source_change_seq', v_processed_source_change_seq,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = v_session_row.id;
    v_session_count := v_session_count + 1;

    SELECT public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id => v_session_row.source_snapshot_run_id,
      p_candidate_id => v_candidate_id,
      p_reason => v_reason,
      p_actor_user_id => v_session_row.actor_user_id,
      p_payload_json => public._pay_workbench_merge_targeted_scope_payload(
        v_payload,
        jsonb_build_object(
          'session_id', v_session_row.id::text,
          'source_session_id', v_session_row.id::text,
          'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
          'session_version', COALESCE(v_session_row.version, 0),
          'session_signature', v_session_row.session_signature,
          'pay_channel_scope', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_session_row.filters_json->>'pay_channel_scope', v_session_row.filters_json#>>'{filters,pay_channel_scope}', ''))), ''), 'ALL'),
          'refresh_scope_kind', v_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(to_jsonb(v_canonical_targeted_timesheet_ids), '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(to_jsonb(v_canonical_linked_timesheet_ids), '[]'::jsonb),
          'finance_case_ids', COALESCE(to_jsonb(v_finance_case_ids), '[]'::jsonb),
          'dependency_closure', COALESCE(v_dependency_closure_json, '{}'::jsonb),
          'dependency_closure_fallback_reason', v_dependency_closure_reason,
          'requested_timesheet_ids', COALESCE(v_family_scope_json->'requested_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb)),
          'targeted_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb)),
          'linked_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb)),
          'family_timesheet_ids', COALESCE(v_family_scope_json->'family_timesheet_ids', COALESCE(to_jsonb(v_family_timesheet_ids), '[]'::jsonb)),
          'source_rows_marked_dirty_count', COALESCE(v_source_dirty_count, 0),
          'line_work_marked_pending_count', COALESCE(v_line_work_count, 0),
          'preview_rows_marked_dirty_count', COALESCE(v_preview_count, 0),
          'source_change_seq', v_processed_source_change_seq,
          'latest_source_change_seq', v_processed_source_change_seq,
          'force_legacy', false,
          'force_broad_legacy', false,
          'new_scope_baseline_required',v_new_scope_baseline_required,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      )
    )
    INTO v_refresh_result;

    v_jobs_queued := v_jobs_queued
      + CASE
          WHEN NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), '') IS NOT NULL THEN 1
          WHEN COALESCE(v_refresh_result->>'jobs_queued', '') ~ '^-?[0-9]+$' THEN (v_refresh_result->>'jobs_queued')::integer
          ELSE 0
        END;
  END LOOP;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS remaining_session
    WHERE remaining_session.status='OPEN' AND remaining_session.discarded_at_utc IS NULL
      AND v_session_scan_cutoff_created_at IS NOT NULL
      AND (remaining_session.created_at_utc,remaining_session.id)<=
          (v_session_scan_cutoff_created_at,v_session_scan_cutoff_id)
      AND (v_session_scan_last_created_at IS NULL OR
        (remaining_session.created_at_utc,remaining_session.id)>
          (v_session_scan_last_created_at,v_session_scan_last_id))
      AND NOT EXISTS (
        SELECT 1 FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=remaining_session.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (remaining_session.pay_date,remaining_session.created_at_utc,remaining_session.id)
      )
  ) INTO v_session_scan_has_more;

  UPDATE public.banking_pay_workbench_jobs AS job_update
  SET payload_json = public._pay_workbench_dirty_payload_merge(
        COALESCE(job_update.payload_json, '{}'::jsonb),
        jsonb_build_object(
          'processed_source_change_seq', v_processed_source_change_seq,
          'processed_at_utc', v_now::text,
          'processed_candidate_id', v_candidate_id::text,
          'dirty_scope_count', v_scope_count,
          'dirty_line_count', v_line_work_total,
          'dirty_preview_count', v_preview_total,
          'dirty_apply_row_marking_applied', true,
          'dirty_marking_skipped', false,
          'early_preflight_checked', v_is_authorise_delta_targeted,
          'early_preflight_action', COALESCE(v_preflight_action, 'PROCEED'),
          'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), ''),
          'actual_refresh_job_type', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_type', v_refresh_result->>'canonical_job_type', '')), ''),
          'actual_refresh_scope_status', NULLIF(BTRIM(COALESCE(v_refresh_result->>'scope_status', '')), ''),
          'source_build_required', lower(BTRIM(COALESCE(v_refresh_result->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'delta_refresh_required', lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'line_work_action', CASE
            WHEN lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              THEN 'DIRTY_ROW_MARKING_ONLY_DELTA_REFRESH'
            ELSE 'SOURCE_BUILD_OR_LEGACY_REFRESH'
          END,
          'refresh_enqueue_result', COALESCE(v_refresh_result, '{}'::jsonb)
          ,'session_scan_cutoff_created_at_utc',v_session_scan_cutoff_created_at::text
          ,'session_scan_cutoff_id',v_session_scan_cutoff_id::text
          ,'session_scan_last_created_at_utc',CASE WHEN v_session_scan_has_more THEN v_session_scan_last_created_at::text ELSE NULL END
          ,'session_scan_last_id',CASE WHEN v_session_scan_has_more THEN v_session_scan_last_id::text ELSE NULL END
          ,'rerun_required',v_session_scan_has_more
          ,'has_more',v_session_scan_has_more
        )
      ),
      private_cursor_kind=CASE WHEN v_session_scan_has_more THEN 'DIRTY_SESSION_SCAN_V1' ELSE NULL::text END,
      private_stage_version=CASE WHEN v_session_scan_has_more THEN 1 ELSE NULL::integer END,
      private_cursor_json=CASE
        WHEN v_session_scan_has_more THEN jsonb_strip_nulls(jsonb_build_object(
          'scan_source_change_seq',v_processed_source_change_seq,
          'upper_created_at_utc',v_session_scan_cutoff_created_at,
          'upper_session_id',v_session_scan_cutoff_id,
          'last_created_at_utc',v_session_scan_last_created_at,
          'last_session_id',v_session_scan_last_id,
          'scan_started_at_utc',v_session_scan_started_at,
          'sessions_examined',v_session_scan_sessions_examined
        ))
        ELSE '{}'::jsonb
      END,
      updated_at_utc = v_now
  WHERE job_update.id = p_job_id;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'dirty_worker_apply_done', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'dirty_scope_count', v_scope_count, 'dirty_source_line_count', v_source_dirty_total, 'dirty_line_count', v_line_work_total, 'dirty_preview_count', v_preview_total, 'jobs_queued', v_jobs_queued, 'processed_source_change_seq', v_processed_source_change_seq, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    'candidate_id', v_candidate_id::text,
    'refresh_scope_kind', v_refresh_scope_kind,
    'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
    'family_timesheet_count', COALESCE(array_length(v_family_timesheet_ids, 1), 0),
    'finance_case_count', COALESCE(array_length(v_finance_case_ids, 1), 0),
    'dependency_closure', COALESCE(v_dependency_closure_json, '{}'::jsonb),
    'dirty_scope_count', v_scope_count,
    'dirty_source_line_count', v_source_dirty_total,
    'dirty_line_count', v_line_work_total,
    'dirty_preview_count', v_preview_total,
    'sessions_touched', v_session_count,
    'jobs_queued', v_jobs_queued,
    'processed_source_change_seq', v_processed_source_change_seq,
    'dirty_apply_row_marking_applied', true,
    'dirty_marking_skipped', false,
    'early_preflight_checked', v_is_authorise_delta_targeted,
    'early_preflight_action', COALESCE(v_preflight_action, 'PROCEED'),
    'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), ''),
    'actual_refresh_job_type', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_type', v_refresh_result->>'canonical_job_type', '')), ''),
    'actual_refresh_scope_status', NULLIF(BTRIM(COALESCE(v_refresh_result->>'scope_status', '')), ''),
    'source_build_required', lower(BTRIM(COALESCE(v_refresh_result->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'delta_refresh_required', lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'line_work_action', CASE
      WHEN lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        THEN 'DIRTY_ROW_MARKING_ONLY_DELTA_REFRESH'
      ELSE 'SOURCE_BUILD_OR_LEGACY_REFRESH'
    END,
    'refresh_enqueue_result', COALESCE(v_refresh_result, '{}'::jsonb),
    'more_due', v_session_scan_has_more,
    'has_more', v_session_scan_has_more,
    'made_progress', true,
    'dirty_apply_complete', NOT v_session_scan_has_more,
    'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_delta_refresh_chunk(p_session_id uuid, p_candidate_id uuid, p_payload_json jsonb DEFAULT '{}'::jsonb, p_cursor_json jsonb DEFAULT '{}'::jsonb, p_limit integer DEFAULT 25)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms integer := 0;
  v_budget_ms integer := 3000;
  v_budget_cutoff_ms integer := 2400;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 25), 1), 100);
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_cursor_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_cursor_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_settings_json jsonb := '{}'::jsonb;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_row public.banking_pay_workbench_session_scope%ROWTYPE;
  v_projection_run_id uuid := NULL::uuid;
  v_projection_run_id_text text := NULL::text;
  v_projection_run_status text := NULL::text;
  v_requested_phase text := 'INIT_PREFLIGHT';
  v_persisted_phase text := NULL::text;
  v_requested_phase_order integer := 0;
  v_persisted_phase_order integer := 0;
  v_effective_phase_order integer := 0;
  v_write_resume_recreate_limit integer := 0;
  v_source_change_seq bigint := 0;
  v_source_snapshot_run_id uuid := NULL::uuid;
  v_source_snapshot_run_id_text text := NULL::text;
  v_session_version bigint := NULL::bigint;
  v_projection_mode text := 'DELTA';
  v_projection_class text := 'UNKNOWN';
  v_phase text := 'INIT_PREFLIGHT';
  v_cursor_phase text := NULL::text;
  v_phase_cursor_json jsonb := '{}'::jsonb;
  v_write_phase text := 'NOT_STARTED';
  v_write_cursor_json jsonb := '{}'::jsonb;
  v_classifier_result jsonb := '{}'::jsonb;
  v_stored_classifier_result jsonb := '{}'::jsonb;
  v_effective_classifier_payload_json jsonb := '{}'::jsonb;
  v_stored_classifier_safe_delta boolean := false;
  v_project_result jsonb := '{}'::jsonb;
  v_write_result jsonb := '{}'::jsonb;
  v_candidate_state_result jsonb := '{}'::jsonb;
  v_shadow_result jsonb := '{}'::jsonb;
  v_payload_shadow_mode boolean := false;
  v_payload_shadow_compare_required boolean := false;
  v_payload_shadow_compare_enforced boolean := false;
  v_settings_delta_shadow_mode boolean := false;
  v_classifier_delta_shadow_mode boolean := false;
  v_classifier_payload_shadow_mode boolean := false;
  v_classifier_shadow_compare_required boolean := false;
  v_classifier_shadow_compare_enforced boolean := false;
  v_shadow_flags_reconciled_to_not_required boolean := false;
  v_shadow_compare_required boolean := false;
  v_shadow_compare_enforced boolean := false;
  v_shadow_compare_status text := 'NOT_REQUIRED';
  v_shadow_reference_unavailable boolean := false;
  v_fallback_allowed boolean := true;
  v_fallback_reason text := NULL::text;
  v_dependency_missing_reason text := NULL::text;
  v_targeted_json jsonb := '[]'::jsonb;
  v_linked_json jsonb := '[]'::jsonb;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_resolved_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_invalid_targeted_uuid_count integer := 0;
  v_invalid_linked_uuid_count integer := 0;
  v_projected_row_count integer := 0;
  v_written_source_count integer := 0;
  v_written_line_work_count integer := 0;
  v_written_preview_count integer := 0;
  v_source_rows_written integer := 0;
  v_line_rows_written integer := 0;
  v_preview_rows_written integer := 0;
  v_rows_superseded integer := 0;
  v_selected_preserved_count integer := 0;
  v_selected_cleared_count integer := 0;
  v_projection_more_due boolean := false;
  v_projection_cursor_json jsonb := '{}'::jsonb;
  v_current_projection_cursor_json jsonb := '{}'::jsonb;
  v_candidate_state_updated boolean := false;
  v_policy_x_invalid_count integer := 0;
  v_economic_key_invalid_count integer := 0;
  v_projection_cert_invalid_count integer := 0;
  v_key_parity_invalid_count integer := 0;
  v_scope_total_count integer := 0;
  v_scope_ready_count integer := 0;
  v_scope_pending_count integer := 0;
  v_scope_failed_count integer := 0;
  v_line_units_total integer := 0;
  v_line_units_ready integer := 0;
  v_line_units_pending integer := 0;
  v_line_units_failed integer := 0;
  v_preview_row_count integer := 0;
  v_selected_row_count integer := 0;
  v_section_counts_json jsonb := '{}'::jsonb;
  v_candidate_sample_rows_json jsonb := '[]'::jsonb;
  v_pre_candidate_counts_json jsonb := '{}'::jsonb;
  v_post_candidate_counts_json jsonb := '{}'::jsonb;
  v_section_delta_json jsonb := '{}'::jsonb;
  v_preview_row_count_delta integer := 0;
  v_selected_row_count_delta integer := 0;
  v_line_units_total_delta integer := 0;
  v_line_units_ready_delta integer := 0;
  v_line_units_pending_delta integer := 0;
  v_line_units_failed_delta integer := 0;
  v_scope_ready_delta integer := 0;
  v_scope_seeded_delta integer := 0;
  v_scope_pending_delta integer := 0;
  v_scope_failed_delta integer := 0;
  v_return_json jsonb := '{}'::jsonb;
  v_live_source_change_seq bigint := 0;
  v_superseded_projection_runs integer := 0;
  v_prelock_payload_source_change_seq bigint := 0;
  v_prelock_cursor_source_change_seq bigint := 0;
  v_prelock_projection_source_change_seq bigint := 0;
  v_prelock_effective_latest_source_seq bigint := 0;
  v_prelock_projection_run_id uuid := NULL::uuid;
  v_prelock_projection_run_id_text text := NULL::text;
  v_prelock_family_key text := NULL::text;
  v_admission_result jsonb := '{}'::jsonb;
  v_admission_seal_json jsonb := '{}'::jsonb;
  v_admission_seal_digest text := NULL::text;
  v_admission_seal_version integer := 0;
  v_sealed_lifecycle_delta boolean := false;
  v_lifecycle_event_class text := NULL::text;
  v_finalize_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_session_id IS NULL OR p_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'fallback_required', true,
      'fallback_reason', 'SESSION_ID_AND_CANDIDATE_ID_REQUIRED',
      'more_due', false,
      'has_more', false,
      'made_progress', false,
      'delta_refresh_complete', false,
      'stop_reason', 'DELTA_INPUT_INVALID'
    );
  END IF;

  SELECT COALESCE(to_jsonb(settings_row), '{}'::jsonb)
  INTO v_settings_json
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  v_settings_delta_shadow_mode := lower(BTRIM(COALESCE(
    v_settings_json->>'banking_pay_workbench_delta_shadow_mode',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF COALESCE(v_settings_json->>'banking_pay_workbench_delta_budget_ms', '') ~ '^[0-9]{1,9}$' THEN
    v_budget_ms := LEAST(GREATEST((v_settings_json->>'banking_pay_workbench_delta_budget_ms')::integer, 500), 30000);
  ELSE
    v_budget_ms := 3000;
  END IF;
  v_budget_cutoff_ms := GREATEST(250, FLOOR(v_budget_ms * 0.80)::integer);

  v_prelock_projection_run_id_text := NULLIF(BTRIM(COALESCE(
    v_cursor_json->>'projection_run_id',
    v_cursor_json#>>'{cursor,projection_run_id}',
    v_payload_json->>'projection_run_id',
    v_payload_json#>>'{cursor,projection_run_id}',
    v_payload_json#>>'{cursor_json,projection_run_id}',
    v_payload_json#>>'{cursor_json,cursor,projection_run_id}',
    ''
  )), '');
  IF v_prelock_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_prelock_projection_run_id := v_prelock_projection_run_id_text::uuid;
  END IF;

  v_prelock_payload_source_change_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_payload_json->>'latest_source_change_seq', '') ~ '^\d{1,18}$' THEN (v_payload_json->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json->>'source_change_seq', '') ~ '^\d{1,18}$' THEN (v_payload_json->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json->>'source_change_sequence', '') ~ '^\d{1,18}$' THEN (v_payload_json->>'source_change_sequence')::bigint END, 0)
  );
  v_prelock_cursor_source_change_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_cursor_json->>'source_change_seq', '') ~ '^\d{1,18}$' THEN (v_cursor_json->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_cursor_json->>'source_change_sequence', '') ~ '^\d{1,18}$' THEN (v_cursor_json->>'source_change_sequence')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_cursor_json#>>'{cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_cursor_json#>>'{cursor,source_change_seq}')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json#>>'{cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_payload_json#>>'{cursor,source_change_seq}')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json#>>'{cursor,cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_payload_json#>>'{cursor,cursor,source_change_seq}')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json#>>'{cursor_json,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_payload_json#>>'{cursor_json,source_change_seq}')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload_json#>>'{cursor_json,cursor,source_change_seq}', '') ~ '^\d{1,18}$' THEN (v_payload_json#>>'{cursor_json,cursor,source_change_seq}')::bigint END, 0)
  );
  v_prelock_family_key := COALESCE(
    NULLIF(BTRIM(COALESCE(v_payload_json->>'normalised_delta_family_key', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload_json->>'delta_family_key', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload_json->>'delta_coalescing_key', '')), ''),
    NULL::text
  );

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_source_change_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF v_prelock_projection_run_id IS NOT NULL THEN
    SELECT COALESCE(projection_run.source_change_seq, 0)
    INTO v_prelock_projection_source_change_seq
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.id = v_prelock_projection_run_id;
  END IF;

  v_prelock_effective_latest_source_seq := GREATEST(
    COALESCE(v_live_source_change_seq, 0),
    COALESCE(v_prelock_payload_source_change_seq, 0)
  );

  IF (
    (COALESCE(v_prelock_cursor_source_change_seq, 0) > 0 AND v_prelock_effective_latest_source_seq > COALESCE(v_prelock_cursor_source_change_seq, 0))
    OR (COALESCE(v_prelock_projection_source_change_seq, 0) > 0 AND v_prelock_effective_latest_source_seq > COALESCE(v_prelock_projection_source_change_seq, 0))
  ) THEN
    IF v_prelock_projection_run_id IS NOT NULL THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
      SET status = CASE WHEN UPPER(BTRIM(COALESCE(projection_run.status, ''))) = 'COMPLETED' THEN projection_run.status ELSE 'FAILED' END,
          fallback_required = false,
          fallback_reason = CASE WHEN UPPER(BTRIM(COALESCE(projection_run.status, ''))) = 'COMPLETED' THEN projection_run.fallback_reason ELSE 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ' END,
          diagnostics_json = jsonb_strip_nulls(
            COALESCE(projection_run.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'stale_continuation_superseded_before_session_lock', true,
              'stale_continuation_superseded_at_utc', v_now::text,
              'payload_source_change_seq', v_prelock_payload_source_change_seq,
              'cursor_source_change_seq', v_prelock_cursor_source_change_seq,
              'projection_run_source_change_seq', v_prelock_projection_source_change_seq,
              'live_candidate_source_change_seq', v_live_source_change_seq,
              'normalised_delta_family_key', v_prelock_family_key
            )
          ),
          updated_at_utc = v_now,
          completed_at_utc = COALESCE(projection_run.completed_at_utc, v_now)
      WHERE projection_run.id = v_prelock_projection_run_id;
    END IF;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      COALESCE(v_payload_json->>'job_id', v_payload_json->>'source_job_id', p_session_id::text || ':' || p_candidate_id::text),
      'STALE_CONTINUATION_SUPERSEDED_BEFORE_SESSION_LOCK',
      NULL::jsonb,
      jsonb_build_object(
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'projection_run_id', CASE WHEN v_prelock_projection_run_id IS NULL THEN NULL ELSE v_prelock_projection_run_id::text END,
        'normalised_delta_family_key', v_prelock_family_key,
        'payload_source_change_seq', v_prelock_payload_source_change_seq,
        'cursor_source_change_seq', v_prelock_cursor_source_change_seq,
        'projection_run_source_change_seq', v_prelock_projection_source_change_seq,
        'live_candidate_source_change_seq', v_live_source_change_seq,
        'session_progress_lock_skipped', true,
        'session_lock_skipped', true,
        'write_compatible_outputs_blocked', true,
        'more_due', false,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      'STALE_CONTINUATION_SUPERSEDED_BEFORE_SESSION_LOCK',
      CASE WHEN COALESCE(v_payload_json->>'actor_user_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (v_payload_json->>'actor_user_id')::uuid ELSE NULL::uuid END
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'projection_run_id', CASE WHEN v_prelock_projection_run_id IS NULL THEN NULL ELSE v_prelock_projection_run_id::text END,
      'normalised_delta_family_key', v_prelock_family_key,
      'source_change_seq', v_prelock_effective_latest_source_seq,
      'cursor_source_change_seq', v_prelock_cursor_source_change_seq,
      'projection_run_source_change_seq', v_prelock_projection_source_change_seq,
      'live_candidate_source_change_seq', v_live_source_change_seq,
      'fallback_required', false,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', true,
      'superseded_by_newer_source_change_seq', true,
      'stop_reason', 'STALE_CONTINUATION_SUPERSEDED_BEFORE_SESSION_LOCK',
      'session_lock_skipped', true,
      'write_compatible_outputs_blocked', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
    AND session_row.status = 'OPEN'
    AND session_row.discarded_at_utc IS NULL
  FOR UPDATE;

  SELECT scope_row.*
  INTO v_scope_row
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF v_session_row.id IS NULL OR v_scope_row.id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'fallback_required', true,
      'fallback_reason', 'SESSION_OR_SCOPE_NOT_OPEN',
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', false,
      'stop_reason', 'DELTA_FALLBACK_REQUIRED'
    );
  END IF;

  IF to_regclass('public.banking_pay_workbench_candidate_delta_projection_runs') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'fallback_required', true,
      'fallback_reason', 'DELTA_PROJECTION_RUN_TABLE_MISSING',
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', false,
      'stop_reason', 'DELTA_FALLBACK_REQUIRED'
    );
  END IF;

  v_projection_run_id_text := NULLIF(BTRIM(COALESCE(
    v_cursor_json->>'projection_run_id',
    v_payload_json->>'projection_run_id',
    ''
  )), '');

  IF v_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_projection_run_id := v_projection_run_id_text::uuid;
  ELSE
    v_projection_run_id := gen_random_uuid();
  END IF;

  v_cursor_phase := upper(BTRIM(COALESCE(v_cursor_json->>'phase', '')));
  v_phase := upper(BTRIM(COALESCE(v_cursor_json->>'phase', v_payload_json->>'phase', 'INIT_PREFLIGHT')));
  IF v_phase = 'SHADOW_COMPARE' THEN
    v_phase := 'VERIFY_PARITY_OR_SHADOW';
  END IF;
  IF v_cursor_phase = 'SHADOW_COMPARE' THEN
    v_cursor_phase := 'VERIFY_PARITY_OR_SHADOW';
  END IF;

  IF v_phase NOT IN (
    'INIT_PREFLIGHT',
    'RESOLVE_SCOPE',
    'PROJECT_ROWS',
    'WRITE_COMPATIBLE_OUTPUTS',
    'VERIFY_PARITY_OR_SHADOW',
    'UPDATE_CANDIDATE_STATE',
    'FINALISE'
  ) THEN
    v_phase := 'INIT_PREFLIGHT';
  END IF;
  v_requested_phase := v_phase;

  v_phase_cursor_json := CASE
    WHEN jsonb_typeof(v_cursor_json->'cursor') = 'object' THEN COALESCE(v_cursor_json->'cursor', '{}'::jsonb)
    WHEN jsonb_typeof(v_payload_json->'cursor_json') = 'object' THEN COALESCE(v_payload_json->'cursor_json', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  IF COALESCE(v_payload_json->>'session_version', '') ~ '^[0-9]{1,18}$' THEN
    v_session_version := (v_payload_json->>'session_version')::bigint;
  ELSE
    v_session_version := COALESCE(v_session_row.version, 1);
  END IF;

  IF COALESCE(v_payload_json->>'source_change_seq', v_payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$' THEN
    v_source_change_seq := COALESCE(v_payload_json->>'source_change_seq', v_payload_json->>'source_change_sequence')::bigint;
  ELSE
    v_source_change_seq := 0;
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_source_change_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_live_source_change_seq := COALESCE(v_live_source_change_seq, 0);

  IF COALESCE(v_source_change_seq, 0) > 0
     AND COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0) THEN
    UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
    SET selected = false,
        selection_state = 'DIRTY',
        status = 'DIRTY',
        row_json = jsonb_strip_nulls(
          COALESCE(preview_row_update.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'source_change_seq', v_source_change_seq,
            'newer_source_change_seq', v_live_source_change_seq,
            'superseded_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE preview_row_update.session_id = p_session_id
      AND preview_row_update.candidate_id = p_candidate_id
      AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
    SET status = 'SUPERSEDED',
        source_row_json = jsonb_strip_nulls(
          COALESCE(source_row_update.source_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'source_change_seq', v_source_change_seq,
            'newer_source_change_seq', v_live_source_change_seq,
            'superseded_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE source_row_update.session_id = p_session_id
      AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

    UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
    SET status = 'SKIPPED',
        result_row_json = jsonb_strip_nulls(
          COALESCE(line_work_update.result_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'source_change_seq', v_source_change_seq,
            'newer_source_change_seq', v_live_source_change_seq,
            'superseded_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE line_work_update.session_id = p_session_id
      AND line_work_update.candidate_id = p_candidate_id
      AND (
        COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
        OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
      )
      AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET status = 'FAILED',
        fallback_required = false,
        fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'source_change_seq', v_source_change_seq,
            'newer_source_change_seq', v_live_source_change_seq,
            'terminalised_by', 'pay_workbench_candidate_delta_refresh_chunk',
            'terminalised_at_utc', v_now::text
          ),
        updated_at_utc = v_now,
        completed_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';

    GET DIAGNOSTICS v_superseded_projection_runs = ROW_COUNT;

    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'projection_run_id', v_projection_run_id::text,
      'source_change_seq', v_source_change_seq,
      'newer_source_change_seq', v_live_source_change_seq,
      'fallback_required', false,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', true,
      'superseded_by_newer_source_change_seq', true,
      'superseded_projection_runs', COALESCE(v_superseded_projection_runs, 0),
      'stop_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ'
    );
  END IF;

  v_source_snapshot_run_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'source_snapshot_run_id', v_payload_json->>'snapshot_run_id', '')), '');
  IF v_source_snapshot_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_source_snapshot_run_id := v_source_snapshot_run_id_text::uuid;
  ELSE
    v_source_snapshot_run_id := v_session_row.source_snapshot_run_id;
  END IF;

  v_payload_shadow_mode := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_mode',
    v_payload_json->>'payload_shadow_mode',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_shadow_compare_required := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_required',
    v_payload_json->>'shadow_compare',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_payload_shadow_compare_enforced := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_enforced',
    v_payload_json->>'enforce_shadow_compare',
    v_payload_json->>'shadow_enforced',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_shadow_compare_required := COALESCE(v_payload_shadow_compare_required, false);
  v_shadow_compare_enforced := COALESCE(v_payload_shadow_compare_enforced, false);
  v_fallback_allowed := lower(BTRIM(COALESCE(v_payload_json->>'fallback_allowed', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_stored_classifier_result := CASE
    WHEN jsonb_typeof(COALESCE(v_payload_json->'classifier_result', '{}'::jsonb)) = 'object'
      THEN COALESCE(v_payload_json->'classifier_result', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_stored_classifier_safe_delta :=
    UPPER(BTRIM(COALESCE(v_stored_classifier_result->>'resolved_mode', ''))) = 'DELTA'
    AND UPPER(BTRIM(COALESCE(v_stored_classifier_result->>'projection_mode', v_stored_classifier_result->>'resolved_mode', ''))) = 'DELTA'
    AND UPPER(BTRIM(COALESCE(v_stored_classifier_result->>'resolved_job_type', v_stored_classifier_result->>'job_type', ''))) = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    AND UPPER(BTRIM(COALESCE(v_stored_classifier_result->>'projection_class', ''))) = 'NORMAL_TIMESHEET'
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->>'fast_path_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->>'fallback_required', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->>'no_op', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->>'banking_pay_dirty_required', v_stored_classifier_result->'complexity_flags'->>'banking_pay_dirty_required', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'force_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'force_source_build', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND jsonb_typeof(COALESCE(v_stored_classifier_result->'targeted_timesheet_ids', '[]'::jsonb)) = 'array'
    AND jsonb_array_length(COALESCE(v_stored_classifier_result->'targeted_timesheet_ids', '[]'::jsonb)) > 0;

  IF v_phase = 'INIT_PREFLIGHT' THEN
    v_effective_classifier_payload_json := v_payload_json;

    IF v_stored_classifier_safe_delta IS TRUE
       AND lower(BTRIM(COALESCE(v_effective_classifier_payload_json->>'banking_pay_dirty_required', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       AND lower(BTRIM(COALESCE(v_effective_classifier_payload_json->>'explicit_banking_pay_action', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       AND NULLIF(BTRIM(COALESCE(v_effective_classifier_payload_json->>'mutation_context', v_effective_classifier_payload_json->>'lifecycle_mutation_context', '')), '') IS NULL THEN
      v_effective_classifier_payload_json := jsonb_strip_nulls(
        v_effective_classifier_payload_json
        || jsonb_build_object(
          'mutation_context', COALESCE(
            NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'mutation_context'), ''),
            NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'lifecycle_mutation_context'), ''),
            'timesheet_unauthorise'
          ),
          'lifecycle_mutation_context', COALESCE(
            NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'lifecycle_mutation_context'), ''),
            NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'mutation_context'), ''),
            'timesheet_unauthorise'
          ),
          'trigger_table', COALESCE(NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'trigger_table'), ''), 'timesheets'),
          'trigger_op', COALESCE(NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'trigger_operation'), ''), 'UPDATE'),
          'trigger_operation', COALESCE(NULLIF(BTRIM(v_stored_classifier_result->'complexity_flags'->>'trigger_operation'), ''), 'UPDATE'),
          'authorise_boundary_changed', lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'authorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'unauthorise_boundary_changed', CASE
            WHEN lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'unauthorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN true
            WHEN lower(BTRIM(COALESCE(v_stored_classifier_result->'complexity_flags'->>'authorise_boundary_changed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN false
            ELSE true
          END,
          'explicit_banking_pay_action', true,
          'banking_pay_dirty_required', true,
          'ordinary_timesheet_edit_save_no_dirty', false,
          'projection_class', 'NORMAL_TIMESHEET',
          'refresh_scope_kind', COALESCE(NULLIF(BTRIM(v_stored_classifier_result->>'refresh_scope_kind'), ''), 'TARGETED_TIMESHEETS'),
          'targeted_timesheet_ids', CASE
            WHEN jsonb_typeof(COALESCE(v_effective_classifier_payload_json->'targeted_timesheet_ids', '[]'::jsonb)) = 'array'
             AND jsonb_array_length(COALESCE(v_effective_classifier_payload_json->'targeted_timesheet_ids', '[]'::jsonb)) > 0
              THEN v_effective_classifier_payload_json->'targeted_timesheet_ids'
            ELSE COALESCE(v_stored_classifier_result->'targeted_timesheet_ids', '[]'::jsonb)
          END,
          'linked_timesheet_ids', CASE
            WHEN jsonb_typeof(COALESCE(v_effective_classifier_payload_json->'linked_timesheet_ids', '[]'::jsonb)) = 'array'
             AND jsonb_array_length(COALESCE(v_effective_classifier_payload_json->'linked_timesheet_ids', '[]'::jsonb)) > 0
              THEN v_effective_classifier_payload_json->'linked_timesheet_ids'
            ELSE COALESCE(v_stored_classifier_result->'linked_timesheet_ids', '[]'::jsonb)
          END,
          'stored_classifier_context_rehydrated', true
        )
      );
    END IF;

    v_classifier_result := public.pay_workbench_delta_refresh_classify_v1(
      p_session_id,
      p_candidate_id,
      v_effective_classifier_payload_json || jsonb_build_object(
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'projection_run_id', v_projection_run_id::text,
        'session_version', COALESCE(v_session_version, v_session_row.version),
        'source_snapshot_run_id', CASE WHEN v_source_snapshot_run_id IS NULL THEN NULL ELSE v_source_snapshot_run_id::text END
      )
    );

    IF v_stored_classifier_safe_delta IS TRUE
       AND (
         UPPER(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) <> 'DELTA'
         OR COALESCE((v_classifier_result->>'fast_path_allowed')::boolean, false) IS NOT TRUE
         OR UPPER(BTRIM(COALESCE(v_classifier_result->>'projection_class', 'UNKNOWN'))) NOT IN ('NORMAL_TIMESHEET','TIMESHEET_LIFECYCLE')
       ) THEN
      v_classifier_result := jsonb_strip_nulls(
        v_stored_classifier_result
        || jsonb_build_object(
          'resolved_mode', 'DELTA',
          'projection_mode', 'DELTA',
          'resolved_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_class', COALESCE(NULLIF(UPPER(BTRIM(v_stored_classifier_result->>'projection_class')),''),'NORMAL_TIMESHEET'),
          'fast_path_allowed', true,
          'delta_refresh_required', true,
          'source_build_required', false,
          'fallback_required', false,
          'fallback_reason', NULL::text,
          'no_op', false,
          'blocked', false,
          'banking_pay_dirty_required', true,
          'ordinary_timesheet_edit_save_no_dirty', false,
          'classifier_recovered_from_stored_result', true,
          'classifier_recovery_reason', 'PRESERVED_TARGETED_DELTA_CLASSIFIER_RESULT'
        )
      );
    END IF;

    v_projection_mode := upper(BTRIM(COALESCE(v_classifier_result->>'projection_mode', v_payload_json->>'projection_mode', 'DELTA')));
    v_projection_class := upper(BTRIM(COALESCE(v_classifier_result->>'projection_class', v_payload_json->>'projection_class', 'UNKNOWN')));
    v_classifier_delta_shadow_mode := lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,delta_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_classifier_payload_shadow_mode := lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,payload_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_classifier_shadow_compare_required := lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_classifier_shadow_compare_enforced := lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_enforced', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_shadow_compare_required := COALESCE(v_shadow_compare_required, false)
      OR COALESCE(v_classifier_delta_shadow_mode, false)
      OR COALESCE(v_classifier_payload_shadow_mode, false)
      OR COALESCE(v_classifier_shadow_compare_required, false);
    v_shadow_compare_enforced := COALESCE(v_shadow_compare_required, false)
      AND (COALESCE(v_shadow_compare_enforced, false) OR COALESCE(v_classifier_shadow_compare_enforced, false));

    v_targeted_json := COALESCE(v_classifier_result->'targeted_timesheet_ids', v_payload_json->'targeted_timesheet_ids', '[]'::jsonb);
    v_linked_json := COALESCE(v_classifier_result->'linked_timesheet_ids', v_payload_json->'linked_timesheet_ids', '[]'::jsonb);

    IF v_projection_class='TIMESHEET_LIFECYCLE' THEN
      v_lifecycle_event_class:=CASE
        WHEN lower(BTRIM(COALESCE(v_payload_json->>'unauthorise_boundary_changed',v_payload_json->>'timesheet_unauthorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
          OR lower(BTRIM(COALESCE(v_payload_json->>'lifecycle_mutation_context',v_payload_json->>'mutation_context',''))) IN ('timesheet_unauthorise','unauthorise_timesheet')
        THEN 'UNAUTHORISE' ELSE 'AUTHORISE' END;
      v_source_change_seq:=GREATEST(COALESCE(v_source_change_seq,0),COALESCE(v_live_source_change_seq,0));
      v_admission_result:=private.pay_workbench_targeted_delta_admission_v1(
        p_session_id,p_candidate_id,v_projection_run_id,v_lifecycle_event_class,
        v_targeted_timesheet_ids,v_linked_timesheet_ids,v_source_change_seq
      );
      IF COALESCE((v_admission_result->>'admitted')::boolean,false) IS NOT TRUE THEN
        RETURN jsonb_build_object(
          'ok',true,'job_type','WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id',v_projection_run_id,'fallback_required',true,
          'fallback_reason',COALESCE(v_admission_result->>'reason','TARGETED_DELTA_ADMISSION_FAILED'),
          'more_due',false,'has_more',false,'made_progress',true,
          'delta_refresh_complete',false,'stop_reason','DELTA_FALLBACK_REQUIRED'
        );
      END IF;
      v_admission_seal_json:=v_admission_result->'admission_seal_json';
      v_admission_seal_digest:=v_admission_result->>'admission_seal_digest';
      v_sealed_lifecycle_delta:=true;
    END IF;

    INSERT INTO public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run (
      id,
      session_id,
      candidate_id,
      session_version,
      source_change_seq,
      source_snapshot_run_id,
      projection_mode,
      projection_class,
      phase,
      cursor_json,
      targeted_timesheet_ids,
      linked_timesheet_ids,
      status,
      fallback_required,
      fallback_reason,
      diagnostics_json,
      write_phase,
      write_cursor_json,
      projected_row_count,
      written_source_count,
      written_line_work_count,
      written_preview_count,
      candidate_state_updated,
      projection_fingerprint,
      shadow_compare_required,
      shadow_compare_status,
      shadow_compare_enforced,
      admission_seal_version,
      admission_seal_json,
      admission_seal_digest,
      admission_sealed_at_utc,
      started_at_utc,
      updated_at_utc
    )
    VALUES (
      v_projection_run_id,
      p_session_id,
      p_candidate_id,
      COALESCE(v_session_version, v_session_row.version, 1),
      COALESCE(v_source_change_seq, 0),
      v_source_snapshot_run_id,
      CASE WHEN v_projection_mode IN ('DELTA', 'READINESS_PATCH', 'RESERVATION_PATCH', 'POST_DRAFT_OVERLAY', 'CLONE_REBASE', 'SHADOW_COMPARE') THEN v_projection_mode ELSE 'DELTA' END,
      v_projection_class,
      'INIT_PREFLIGHT',
      jsonb_build_object('phase', 'INIT_PREFLIGHT', 'cursor', '{}'::jsonb),
      CASE WHEN jsonb_typeof(v_targeted_json) = 'array' THEN v_targeted_json ELSE '[]'::jsonb END,
      CASE WHEN jsonb_typeof(v_linked_json) = 'array' THEN v_linked_json ELSE '[]'::jsonb END,
      'RUNNING',
      false,
      NULL::text,
      jsonb_build_object('classifier_result', v_classifier_result, 'created_by', 'pay_workbench_candidate_delta_refresh_chunk'),
      'NOT_STARTED',
      '{}'::jsonb,
      0,
      0,
      0,
      0,
      false,
      NULL::text,
      v_shadow_compare_required,
      CASE WHEN v_shadow_compare_required OR v_shadow_compare_enforced THEN 'PENDING' ELSE 'NOT_REQUIRED' END,
      v_shadow_compare_enforced,
      CASE WHEN v_sealed_lifecycle_delta THEN 1 ELSE NULL END,
      CASE WHEN v_sealed_lifecycle_delta THEN v_admission_seal_json ELSE '{}'::jsonb END,
      CASE WHEN v_sealed_lifecycle_delta THEN v_admission_seal_digest ELSE NULL END,
      CASE WHEN v_sealed_lifecycle_delta THEN v_now ELSE NULL END,
      v_now,
      v_now
    )
    ON CONFLICT (id) DO UPDATE SET
      session_version = EXCLUDED.session_version,
      source_change_seq = EXCLUDED.source_change_seq,
      source_snapshot_run_id = EXCLUDED.source_snapshot_run_id,
      projection_mode = EXCLUDED.projection_mode,
      projection_class = EXCLUDED.projection_class,
      phase = EXCLUDED.phase,
      cursor_json = EXCLUDED.cursor_json,
      targeted_timesheet_ids = EXCLUDED.targeted_timesheet_ids,
      linked_timesheet_ids = EXCLUDED.linked_timesheet_ids,
      status = 'RUNNING',
      fallback_required = false,
      fallback_reason = NULL::text,
      diagnostics_json = COALESCE(projection_run.diagnostics_json, '{}'::jsonb) || EXCLUDED.diagnostics_json,
      write_phase = 'NOT_STARTED',
      write_cursor_json = '{}'::jsonb,
      projected_row_count = 0,
      written_source_count = 0,
      written_line_work_count = 0,
      written_preview_count = 0,
      candidate_state_updated = false,
      shadow_compare_required = EXCLUDED.shadow_compare_required,
      shadow_compare_status = EXCLUDED.shadow_compare_status,
      shadow_compare_enforced = EXCLUDED.shadow_compare_enforced,
      admission_seal_version = CASE
        WHEN projection_run.admission_seal_version IS NULL THEN EXCLUDED.admission_seal_version
        ELSE projection_run.admission_seal_version END,
      admission_seal_json = CASE
        WHEN projection_run.admission_seal_version IS NULL THEN EXCLUDED.admission_seal_json
        ELSE projection_run.admission_seal_json END,
      admission_seal_digest = CASE
        WHEN projection_run.admission_seal_version IS NULL THEN EXCLUDED.admission_seal_digest
        ELSE projection_run.admission_seal_digest END,
      admission_sealed_at_utc = CASE
        WHEN projection_run.admission_seal_version IS NULL THEN EXCLUDED.admission_sealed_at_utc
        ELSE projection_run.admission_sealed_at_utc END,
      completed_at_utc = NULL::timestamptz,
      updated_at_utc = v_now;

    IF v_sealed_lifecycle_delta AND EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_candidate_delta_projection_runs AS sealed_run
      WHERE sealed_run.id=v_projection_run_id
        AND (sealed_run.admission_seal_version<>1
          OR sealed_run.admission_seal_digest IS DISTINCT FROM v_admission_seal_digest
          OR sealed_run.admission_seal_json IS DISTINCT FROM v_admission_seal_json)
    ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_SEAL_REPLAY_CONFLICT' USING ERRCODE='P0001';
    END IF;

    WITH supersedable_older_projection_runs AS MATERIALIZED (
      SELECT older_projection_run.id
      FROM public.banking_pay_workbench_candidate_delta_projection_runs AS older_projection_run
      WHERE older_projection_run.session_id = p_session_id
        AND older_projection_run.candidate_id = p_candidate_id
        AND older_projection_run.id <> v_projection_run_id
        AND UPPER(BTRIM(COALESCE(older_projection_run.status, ''))) = 'RUNNING'
        AND COALESCE(older_projection_run.source_change_seq, 0) < COALESCE(v_source_change_seq, 0)
        AND UPPER(BTRIM(COALESCE(older_projection_run.projection_class, ''))) = v_projection_class
      FOR UPDATE SKIP LOCKED
    )
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS older_projection_run
    SET status = 'FAILED',
        fallback_required = false,
        fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
        diagnostics_json = COALESCE(older_projection_run.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_newer_source_change_seq', true,
            'source_change_seq', COALESCE(older_projection_run.source_change_seq, 0),
            'newer_source_change_seq', COALESCE(v_source_change_seq, 0),
            'newer_projection_run_id', v_projection_run_id::text,
            'terminalised_by', 'pay_workbench_candidate_delta_refresh_chunk_init_preflight',
            'terminalised_at_utc', v_now::text
          ),
        completed_at_utc = v_now,
        updated_at_utc = v_now
    FROM supersedable_older_projection_runs
    WHERE older_projection_run.id = supersedable_older_projection_runs.id;

    GET DIAGNOSTICS v_superseded_projection_runs = ROW_COUNT;

    IF COALESCE(v_superseded_projection_runs, 0) > 0 THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'older_running_projection_runs_superseded', v_superseded_projection_runs,
              'older_runs_superseded_at_utc', v_now::text
            ),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
    END IF;

    IF COALESCE((v_classifier_result->>'fast_path_allowed')::boolean, false) IS NOT TRUE
       OR upper(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) <> 'DELTA' THEN
      v_fallback_reason := COALESCE(v_classifier_result->>'fallback_reason', 'DELTA_CLASSIFIER_REQUIRES_NON_DELTA_MODE');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = CASE WHEN upper(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) = 'BLOCKED' THEN 'BLOCKED' ELSE 'FALLBACK_REQUIRED' END,
          phase = 'INIT_PREFLIGHT',
          cursor_json = jsonb_build_object('phase', 'INIT_PREFLIGHT', 'cursor', '{}'::jsonb),
          fallback_required = upper(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) NOT IN ('BLOCKED', 'PATCH_ONLY', 'CLONE_REBASE'),
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb) || jsonb_build_object('classifier_result', v_classifier_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', upper(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) NOT IN ('BLOCKED', 'PATCH_ONLY', 'CLONE_REBASE'),
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', CASE WHEN upper(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', 'LEGACY'))) = 'BLOCKED' THEN 'DELTA_BLOCKED' ELSE 'DELTA_FALLBACK_REQUIRED' END,
        'classifier_result', v_classifier_result
      );
    END IF;

    IF v_projection_mode <> 'DELTA' THEN
      v_fallback_reason := 'DELTA_CHUNK_UNSUPPORTED_PROJECTION_MODE';

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    v_phase := 'RESOLVE_SCOPE';
    v_projection_run_status := 'RUNNING';
    v_persisted_phase := v_phase;
    v_requested_phase := v_phase;
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = v_phase,
        cursor_json = jsonb_build_object('phase', v_phase, 'cursor', '{}'::jsonb),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;
  ELSE
    SELECT projection_run.projection_mode,
           projection_run.projection_class,
           UPPER(BTRIM(COALESCE(projection_run.status, 'RUNNING'))),
           UPPER(BTRIM(COALESCE(projection_run.phase, 'INIT_PREFLIGHT'))),
           projection_run.cursor_json,
           projection_run.write_phase,
           projection_run.write_cursor_json,
           projection_run.targeted_timesheet_ids,
           projection_run.linked_timesheet_ids,
           projection_run.session_version,
           projection_run.source_change_seq,
           projection_run.source_snapshot_run_id,
           COALESCE(projection_run.projected_row_count, 0),
           COALESCE(projection_run.written_source_count, 0),
           COALESCE(projection_run.written_line_work_count, 0),
           COALESCE(projection_run.written_preview_count, 0),
           COALESCE(projection_run.candidate_state_updated, false),
           COALESCE(projection_run.shadow_compare_required, false),
           COALESCE(projection_run.shadow_compare_status, 'NOT_REQUIRED'),
           COALESCE(projection_run.shadow_compare_enforced, false),
           COALESCE(projection_run.admission_seal_version,0),
           COALESCE(projection_run.admission_seal_json,'{}'::jsonb),
           projection_run.admission_seal_digest
    INTO v_projection_mode,
         v_projection_class,
         v_projection_run_status,
         v_persisted_phase,
         v_phase_cursor_json,
         v_write_phase,
         v_write_cursor_json,
         v_targeted_json,
         v_linked_json,
         v_session_version,
         v_source_change_seq,
         v_source_snapshot_run_id,
         v_projected_row_count,
         v_written_source_count,
         v_written_line_work_count,
         v_written_preview_count,
         v_candidate_state_updated,
         v_shadow_compare_required,
         v_shadow_compare_status,
         v_shadow_compare_enforced,
         v_admission_seal_version,
         v_admission_seal_json,
         v_admission_seal_digest
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.id = v_projection_run_id
      AND projection_run.session_id = p_session_id
      AND projection_run.candidate_id = p_candidate_id
      AND projection_run.status IN ('RUNNING', 'COMPLETED')
    FOR UPDATE;

    v_sealed_lifecycle_delta:=v_admission_seal_version=1
      AND v_projection_class='TIMESHEET_LIFECYCLE';

    IF NOT FOUND THEN
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', 'DELTA_PROJECTION_RUN_NOT_RUNNING',
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    IF v_projection_run_status = 'COMPLETED' THEN
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', COALESCE(NULLIF(v_persisted_phase, ''), 'FINALISE'),
        'write_phase', COALESCE(NULLIF(v_write_phase, ''), 'WRITE_COMPLETE'),
        'fallback_required', false,
        'more_due', false,
        'has_more', false,
        'made_progress', false,
        'delta_refresh_complete', true,
        'stop_reason', 'DELTA_PROJECTION_RUN_ALREADY_COMPLETED'
      );
    END IF;

    IF COALESCE(v_source_change_seq, 0) > 0
       AND COALESCE(v_live_source_change_seq, 0) > COALESCE(v_source_change_seq, 0) THEN
      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'source_change_seq', v_source_change_seq,
              'newer_source_change_seq', v_live_source_change_seq,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'source_change_seq', v_source_change_seq,
              'newer_source_change_seq', v_live_source_change_seq,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'source_change_seq', v_source_change_seq,
              'newer_source_change_seq', v_live_source_change_seq,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FAILED',
          fallback_required = false,
          fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'source_change_seq', v_source_change_seq,
              'newer_source_change_seq', v_live_source_change_seq,
              'terminalised_by', 'pay_workbench_candidate_delta_refresh_chunk',
              'terminalised_at_utc', v_now::text
            ),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id
        AND projection_run_update.session_id = p_session_id
        AND projection_run_update.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(projection_run_update.status, ''))) = 'RUNNING';

      GET DIAGNOSTICS v_superseded_projection_runs = ROW_COUNT;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'source_change_seq', v_source_change_seq,
        'newer_source_change_seq', v_live_source_change_seq,
        'phase', COALESCE(NULLIF(v_persisted_phase, ''), 'INIT_PREFLIGHT'),
        'write_phase', COALESCE(NULLIF(v_write_phase, ''), 'NOT_STARTED'),
        'fallback_required', false,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', true,
        'superseded_by_newer_source_change_seq', true,
        'superseded_projection_runs', COALESCE(v_superseded_projection_runs, 0),
        'stop_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ'
      );
    END IF;

    IF v_cursor_phase IN ('WRITE_COMPATIBLE_OUTPUTS', 'VERIFY_PARITY_OR_SHADOW', 'UPDATE_CANDIDATE_STATE', 'FINALISE') THEN
      v_requested_phase := v_cursor_phase;
    END IF;

    v_requested_phase_order := CASE v_requested_phase
      WHEN 'INIT_PREFLIGHT' THEN 0
      WHEN 'RESOLVE_SCOPE' THEN 1
      WHEN 'PROJECT_ROWS' THEN 2
      WHEN 'WRITE_COMPATIBLE_OUTPUTS' THEN 3
      WHEN 'VERIFY_PARITY_OR_SHADOW' THEN 4
      WHEN 'UPDATE_CANDIDATE_STATE' THEN 5
      WHEN 'FINALISE' THEN 6
      ELSE 0
    END;
    v_persisted_phase_order := CASE v_persisted_phase
      WHEN 'INIT_PREFLIGHT' THEN 0
      WHEN 'RESOLVE_SCOPE' THEN 1
      WHEN 'PROJECT_ROWS' THEN 2
      WHEN 'WRITE_COMPATIBLE_OUTPUTS' THEN 3
      WHEN 'VERIFY_PARITY_OR_SHADOW' THEN 4
      WHEN 'UPDATE_CANDIDATE_STATE' THEN 5
      WHEN 'FINALISE' THEN 6
      ELSE 0
    END;
    IF NULLIF(UPPER(BTRIM(COALESCE(v_write_phase, ''))), '') IS NOT NULL
       AND UPPER(BTRIM(COALESCE(v_write_phase, ''))) <> 'NOT_STARTED' THEN
      v_persisted_phase_order := GREATEST(v_persisted_phase_order, 3);
      IF UPPER(BTRIM(COALESCE(v_write_phase, ''))) = 'WRITE_COMPLETE' THEN
        v_persisted_phase_order := GREATEST(v_persisted_phase_order, 4);
      END IF;
    END IF;

    v_effective_phase_order := GREATEST(v_requested_phase_order, v_persisted_phase_order);
    v_phase := CASE v_effective_phase_order
      WHEN 0 THEN 'INIT_PREFLIGHT'
      WHEN 1 THEN 'RESOLVE_SCOPE'
      WHEN 2 THEN 'PROJECT_ROWS'
      WHEN 3 THEN 'WRITE_COMPATIBLE_OUTPUTS'
      WHEN 4 THEN 'VERIFY_PARITY_OR_SHADOW'
      WHEN 5 THEN 'UPDATE_CANDIDATE_STATE'
      ELSE 'FINALISE'
    END;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'phase_resolver_requested_phase', v_requested_phase,
            'phase_resolver_persisted_phase', v_persisted_phase,
            'phase_resolver_effective_phase', v_phase,
            'phase_resolver_write_phase', COALESCE(v_write_phase, 'NOT_STARTED')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;
  END IF;

  v_classifier_delta_shadow_mode := lower(BTRIM(COALESCE(
    NULLIF(v_classifier_result#>>'{complexity_flags,delta_shadow_mode}', ''),
    NULLIF(v_stored_classifier_result#>>'{complexity_flags,delta_shadow_mode}', ''),
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_classifier_payload_shadow_mode := lower(BTRIM(COALESCE(
    NULLIF(v_classifier_result#>>'{complexity_flags,payload_shadow_mode}', ''),
    NULLIF(v_stored_classifier_result#>>'{complexity_flags,payload_shadow_mode}', ''),
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_classifier_shadow_compare_required := lower(BTRIM(COALESCE(
    NULLIF(v_classifier_result->>'shadow_compare_required', ''),
    NULLIF(v_stored_classifier_result->>'shadow_compare_required', ''),
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_classifier_shadow_compare_enforced := lower(BTRIM(COALESCE(
    NULLIF(v_classifier_result->>'shadow_compare_enforced', ''),
    NULLIF(v_stored_classifier_result->>'shadow_compare_enforced', ''),
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_shadow_compare_required := COALESCE(v_shadow_compare_required, false)
    OR COALESCE(v_classifier_delta_shadow_mode, false)
    OR COALESCE(v_classifier_payload_shadow_mode, false)
    OR COALESCE(v_classifier_shadow_compare_required, false);
  v_shadow_compare_enforced := COALESCE(v_shadow_compare_required, false)
    AND (COALESCE(v_shadow_compare_enforced, false) OR COALESCE(v_classifier_shadow_compare_enforced, false));

  IF v_projection_mode = 'DELTA'
     AND v_projection_class = 'NORMAL_TIMESHEET'
     AND COALESCE(v_settings_delta_shadow_mode, false) IS NOT TRUE
     AND COALESCE(v_classifier_delta_shadow_mode, false) IS NOT TRUE
     AND COALESCE(v_classifier_payload_shadow_mode, false) IS NOT TRUE
     AND COALESCE(v_classifier_shadow_compare_required, false) IS NOT TRUE
     AND COALESCE(v_payload_shadow_mode, false) IS NOT TRUE
     AND COALESCE(v_payload_shadow_compare_required, false) IS NOT TRUE THEN
    v_shadow_compare_required := false;
    v_shadow_compare_enforced := false;
    v_shadow_compare_status := 'NOT_REQUIRED';
    v_shadow_flags_reconciled_to_not_required := true;

    IF v_projection_run_id IS NOT NULL THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET shadow_compare_required = false,
          shadow_compare_enforced = false,
          shadow_compare_status = 'NOT_REQUIRED',
          legacy_compare_status = 'NOT_REQUIRED',
          legacy_compare_json = jsonb_build_object(
            'compare_status', 'NOT_REQUIRED',
            'shadow_compare_not_required', true,
            'shadow_flags_reconciled_to_current_settings', true
          ),
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'shadow_flags_reconciled_to_not_required', true,
              'settings_delta_shadow_mode', COALESCE(v_settings_delta_shadow_mode, false),
              'classifier_delta_shadow_mode', COALESCE(v_classifier_delta_shadow_mode, false),
              'classifier_payload_shadow_mode', COALESCE(v_classifier_payload_shadow_mode, false),
              'payload_shadow_mode', COALESCE(v_payload_shadow_mode, false)
            ),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
    END IF;
  ELSIF v_shadow_compare_enforced IS TRUE AND v_shadow_compare_required IS NOT TRUE THEN
    v_shadow_compare_required := true;
  END IF;

  IF jsonb_typeof(v_targeted_json) = 'array' THEN
    SELECT COALESCE(array_agg(DISTINCT parsed_targeted.timesheet_id_value ORDER BY parsed_targeted.timesheet_id_value), ARRAY[]::uuid[]),
           COALESCE(count(*) FILTER (WHERE parsed_targeted.raw_value IS NOT NULL AND parsed_targeted.timesheet_id_value IS NULL), 0)::integer
    INTO v_targeted_timesheet_ids,
         v_invalid_targeted_uuid_count
    FROM (
      SELECT targeted_element.value #>> '{}' AS raw_value,
             CASE
               WHEN BTRIM(targeted_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN BTRIM(targeted_element.value #>> '{}')::uuid
               ELSE NULL::uuid
             END AS timesheet_id_value
      FROM jsonb_array_elements(v_targeted_json) AS targeted_element(value)
    ) AS parsed_targeted
    WHERE parsed_targeted.raw_value IS NOT NULL;
  END IF;

  IF jsonb_typeof(v_linked_json) = 'array' THEN
    SELECT COALESCE(array_agg(DISTINCT parsed_linked.timesheet_id_value ORDER BY parsed_linked.timesheet_id_value), ARRAY[]::uuid[]),
           COALESCE(count(*) FILTER (WHERE parsed_linked.raw_value IS NOT NULL AND parsed_linked.timesheet_id_value IS NULL), 0)::integer
    INTO v_linked_timesheet_ids,
         v_invalid_linked_uuid_count
    FROM (
      SELECT linked_element.value #>> '{}' AS raw_value,
             CASE
               WHEN BTRIM(linked_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN BTRIM(linked_element.value #>> '{}')::uuid
               ELSE NULL::uuid
             END AS timesheet_id_value
      FROM jsonb_array_elements(v_linked_json) AS linked_element(value)
    ) AS parsed_linked
    WHERE parsed_linked.raw_value IS NOT NULL;
  END IF;

  IF v_invalid_targeted_uuid_count > 0 OR v_invalid_linked_uuid_count > 0 THEN
    v_fallback_reason := 'INVALID_UUID_IN_DELTA_CURSOR_OR_PAYLOAD';
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET status = 'FALLBACK_REQUIRED',
        fallback_required = true,
        fallback_reason = v_fallback_reason,
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object('invalid_targeted_uuid_count', v_invalid_targeted_uuid_count, 'invalid_linked_uuid_count', v_invalid_linked_uuid_count),
        updated_at_utc = v_now,
        completed_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;
    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'projection_run_id', v_projection_run_id::text,
      'fallback_required', true,
      'fallback_reason', v_fallback_reason,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', false,
      'stop_reason', 'DELTA_FALLBACK_REQUIRED'
    );
  END IF;

  IF v_phase = 'RESOLVE_SCOPE' THEN
    IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN
      SELECT COALESCE(array_agg(DISTINCT rotation_scope.family_timesheet_id ORDER BY rotation_scope.family_timesheet_id), ARRAY[]::uuid[])
      INTO v_resolved_family_timesheet_ids
      FROM public._pay_timesheet_rotation_scope(v_targeted_timesheet_ids) AS rotation_scope
      WHERE rotation_scope.family_timesheet_id IS NOT NULL;

      SELECT COALESCE(array_agg(DISTINCT linked_scope.timesheet_id_value ORDER BY linked_scope.timesheet_id_value), ARRAY[]::uuid[])
      INTO v_linked_timesheet_ids
      FROM (
        SELECT unnest(COALESCE(v_linked_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id_value
        UNION ALL
        SELECT unnest(COALESCE(v_resolved_family_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id_value
      ) AS linked_scope
      WHERE linked_scope.timesheet_id_value IS NOT NULL
        AND NOT (linked_scope.timesheet_id_value = ANY(COALESCE(v_targeted_timesheet_ids, ARRAY[]::uuid[])));
    END IF;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = 'PROJECT_ROWS',
        cursor_json = jsonb_build_object('phase', 'PROJECT_ROWS', 'cursor', '{}'::jsonb),
        targeted_timesheet_ids = COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
        linked_timesheet_ids = COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb),
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'resolved_targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
            'resolved_linked_timesheet_count', COALESCE(array_length(v_linked_timesheet_ids, 1), 0),
            'resolve_scope_completed_at_utc', v_now::text
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_phase := 'PROJECT_ROWS';
    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_elapsed_ms >= v_budget_cutoff_ms THEN
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'RESOLVE_SCOPE',
        'next_phase', 'PROJECT_ROWS',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', 'PROJECT_ROWS', 'cursor', '{}'::jsonb)
      );
    END IF;
  END IF;

  IF to_regprocedure('public.pay_workbench_project_changed_timesheets_v1(uuid,uuid,uuid,uuid[],uuid[],jsonb)') IS NULL THEN
    v_dependency_missing_reason := 'PAY_WORKBENCH_PROJECT_CHANGED_TIMESHEETS_V1_MISSING';
  ELSIF to_regprocedure('public.pay_workbench_delta_write_compatible_rows_v1(uuid,uuid,uuid,jsonb)') IS NULL THEN
    v_dependency_missing_reason := 'PAY_WORKBENCH_DELTA_WRITE_COMPATIBLE_ROWS_V1_MISSING';
  ELSIF to_regprocedure('public.pay_workbench_delta_update_candidate_state_v1(uuid,uuid,uuid,jsonb)') IS NULL THEN
    v_dependency_missing_reason := 'PAY_WORKBENCH_DELTA_UPDATE_CANDIDATE_STATE_V1_MISSING';
  END IF;

  IF v_dependency_missing_reason IS NOT NULL THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET status = 'FALLBACK_REQUIRED',
        fallback_required = true,
        fallback_reason = v_dependency_missing_reason,
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object('dependency_missing_reason', v_dependency_missing_reason),
        updated_at_utc = v_now,
        completed_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;
    RETURN jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'projection_run_id', v_projection_run_id::text,
      'fallback_required', true,
      'fallback_reason', v_dependency_missing_reason,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', false,
      'stop_reason', 'DELTA_FALLBACK_REQUIRED'
    );
  END IF;

  IF v_phase IN ('PROJECT_ROWS', 'WRITE_COMPATIBLE_OUTPUTS') THEN
    CREATE TEMP TABLE IF NOT EXISTS _bpay_delta_projection_rows (
      projection_run_id uuid NOT NULL,
      session_id uuid NOT NULL,
      candidate_id uuid NOT NULL,
      session_version bigint NOT NULL,
      source_change_seq bigint,
      timesheet_id uuid,
      canonical_timesheet_id uuid,
      section text NOT NULL,
      line_key text NOT NULL,
      row_key text NOT NULL,
      parent_line_key text,
      split_suffix text,
      source_ordinal bigint NOT NULL,
      row_ordinal bigint NOT NULL,
      pay_channel_scope text,
      refresh_scope_kind text NOT NULL,
      key_type text,
      key_value text,
      amount_ex_vat numeric,
      amount_inc_vat numeric,
      vat_amount numeric,
      draftable boolean NOT NULL DEFAULT false,
      is_ready_for_draft boolean NOT NULL DEFAULT false,
      selection_allowed boolean NOT NULL DEFAULT false,
      readiness_state text,
      blocked_reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
      outstanding_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      source_row_json jsonb NOT NULL,
      economic_key_json jsonb NOT NULL,
      contract_json jsonb NOT NULL,
      work_payload_json jsonb NOT NULL,
      result_row_json jsonb NOT NULL,
      preview_row_json jsonb NOT NULL,
      contract_ok boolean NOT NULL DEFAULT false,
      materialisable boolean NOT NULL DEFAULT false
    ) ON COMMIT DROP;

    TRUNCATE TABLE _bpay_delta_projection_rows;

    v_current_projection_cursor_json := CASE
      WHEN v_phase = 'WRITE_COMPATIBLE_OUTPUTS' THEN '{}'::jsonb
      WHEN jsonb_typeof(v_phase_cursor_json->'cursor') = 'object' THEN COALESCE(v_phase_cursor_json->'cursor', '{}'::jsonb)
      ELSE COALESCE(v_phase_cursor_json, '{}'::jsonb)
    END;
    v_write_resume_recreate_limit := CASE
      WHEN v_phase = 'WRITE_COMPATIBLE_OUTPUTS'
        THEN LEAST(GREATEST(COALESCE(v_projected_row_count, 0), COALESCE(v_limit, 25), 25), 1000)
      ELSE v_limit
    END;

    v_project_result := public.pay_workbench_project_changed_timesheets_v1(
      p_session_id,
      p_candidate_id,
      v_projection_run_id,
      v_targeted_timesheet_ids,
      v_linked_timesheet_ids,
      v_payload_json
        || jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'projection_mode', v_projection_mode,
          'projection_class', v_projection_class,
          'session_version', COALESCE(v_session_version, v_session_row.version),
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'source_snapshot_run_id', CASE WHEN v_source_snapshot_run_id IS NULL THEN NULL ELSE v_source_snapshot_run_id::text END,
          'limit', v_write_resume_recreate_limit,
          'cursor', v_current_projection_cursor_json,
          'shadow_compare_required', v_shadow_compare_required,
          'shadow_compare_enforced', v_shadow_compare_enforced
        )
    );

    IF COALESCE((v_project_result->>'fallback_required')::boolean, false) IS TRUE THEN
      v_fallback_reason := COALESCE(v_project_result->>'fallback_reason', 'DELTA_PROJECT_ROWS_FALLBACK_REQUIRED');

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          phase = 'PROJECT_ROWS',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('project_result', v_project_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED',
        'project_result', jsonb_build_object('fallback_reason', v_fallback_reason)
      );
    END IF;

    IF v_phase = 'PROJECT_ROWS' THEN
      v_projected_row_count := COALESCE(v_projected_row_count, 0) + COALESCE((v_project_result->>'projected_row_count')::integer, 0);
    ELSE
      v_projected_row_count := GREATEST(
        COALESCE(v_projected_row_count, 0),
        COALESCE((v_project_result->>'projected_row_count')::integer, 0)
      );
    END IF;
    v_projection_more_due := COALESCE((v_project_result->>'more_due')::boolean, false);
    v_projection_cursor_json := CASE
      WHEN jsonb_typeof(v_project_result->'next_cursor_json') = 'object' THEN v_project_result->'next_cursor_json'
      ELSE '{}'::jsonb
    END;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = CASE
          WHEN v_phase = 'WRITE_COMPATIBLE_OUTPUTS' THEN 'WRITE_COMPATIBLE_OUTPUTS'
          WHEN v_projection_more_due THEN 'PROJECT_ROWS'
          ELSE 'WRITE_COMPATIBLE_OUTPUTS'
        END,
        cursor_json = jsonb_build_object(
          'phase', CASE
            WHEN v_phase = 'WRITE_COMPATIBLE_OUTPUTS' THEN 'WRITE_COMPATIBLE_OUTPUTS'
            WHEN v_projection_more_due THEN 'PROJECT_ROWS'
            ELSE 'WRITE_COMPATIBLE_OUTPUTS'
          END,
          'cursor', CASE
            WHEN v_phase = 'WRITE_COMPATIBLE_OUTPUTS' THEN '{}'::jsonb
            ELSE COALESCE(CASE WHEN v_projection_more_due THEN v_projection_cursor_json ELSE v_current_projection_cursor_json END, '{}'::jsonb)
          END
        ),
        projected_row_count = v_projected_row_count,
        projection_fingerprint = md5(jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'candidate_id', p_candidate_id::text,
          'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb),
          'session_version', COALESCE(v_session_version, v_session_row.version),
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'projected_row_count', v_projected_row_count
        )::text),
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_projection_row_count', COALESCE((v_project_result->>'projected_row_count')::integer, 0),
            'projection_more_due', v_projection_more_due,
            'write_resume_recreate', v_phase = 'WRITE_COMPATIBLE_OUTPUTS',
            'write_resume_recreate_limit', v_write_resume_recreate_limit,
            'row_key_line_key_parity_proven', LOWER(BTRIM(COALESCE(v_project_result->>'row_key_line_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
            'economic_key_parity_proven', LOWER(BTRIM(COALESCE(v_project_result->>'economic_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    IF v_phase = 'WRITE_COMPATIBLE_OUTPUTS' AND v_projection_more_due THEN
      v_fallback_reason := 'DELTA_WRITE_RECREATE_EXCEEDED_SINGLE_CHUNK';
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'write_resume_recreate_more_due', true,
              'write_resume_recreate_limit', v_write_resume_recreate_limit,
              'project_result', v_project_result
            ),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_elapsed_ms >= v_budget_cutoff_ms AND v_phase = 'PROJECT_ROWS' THEN
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'PROJECT_ROWS',
        'next_phase', CASE WHEN v_projection_more_due THEN 'PROJECT_ROWS' ELSE 'WRITE_COMPATIBLE_OUTPUTS' END,
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'next_cursor_json', jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'phase', CASE WHEN v_projection_more_due THEN 'PROJECT_ROWS' ELSE 'WRITE_COMPATIBLE_OUTPUTS' END,
          'cursor', COALESCE(CASE WHEN v_projection_more_due THEN v_projection_cursor_json ELSE v_current_projection_cursor_json END, '{}'::jsonb),
          'write_phase', COALESCE(v_write_result->>'write_phase', v_write_phase, 'SUPERSEDE_PREVIEW_ROWS'),
          'write_cursor_json', CASE
            WHEN jsonb_typeof(v_write_result->'write_cursor_json') = 'object' THEN v_write_result->'write_cursor_json'
            ELSE COALESCE(v_write_cursor_json, '{}'::jsonb)
          END
        )
      );
    ELSIF v_elapsed_ms >= v_budget_cutoff_ms AND v_phase = 'WRITE_COMPATIBLE_OUTPUTS' THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'write_resume_project_budget_cutoff_reached', true,
              'write_resume_project_elapsed_ms', v_elapsed_ms,
              'write_resume_project_budget_cutoff_ms', v_budget_cutoff_ms,
              'write_resume_project_continued_to_write', true
            ),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
    END IF;

    SELECT COALESCE(projection_run.diagnostics_json->'pre_delta_candidate_counts', '{}'::jsonb)
    INTO v_pre_candidate_counts_json
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.id = v_projection_run_id
      AND projection_run.session_id = p_session_id
      AND projection_run.candidate_id = p_candidate_id
    FOR UPDATE;

    IF COALESCE(v_pre_candidate_counts_json, '{}'::jsonb) = '{}'::jsonb THEN
      WITH pre_line_counts AS (
        SELECT COUNT(*)::integer AS line_units_total,
               COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SKIPPED'))::integer AS line_units_ready,
               COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('PENDING', 'PROCESSING', 'DIRTY'))::integer AS line_units_pending,
               COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('ERROR', 'FAILED'))::integer AS line_units_failed
        FROM public.banking_pay_workbench_candidate_line_work AS line_work
        WHERE line_work.session_id = p_session_id
          AND line_work.candidate_id = p_candidate_id
      ), pre_preview_counts AS (
        SELECT COUNT(*) FILTER (WHERE preview_row.status = 'READY')::integer AS preview_row_count,
               COUNT(*) FILTER (WHERE preview_row.status = 'READY' AND preview_row.selected IS TRUE AND preview_row.selection_state = 'SELECTED')::integer AS selected_row_count
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.session_id = p_session_id
          AND preview_row.candidate_id = p_candidate_id
          AND preview_row.session_version = COALESCE(v_session_version, v_session_row.version)
      ), pre_section_counts AS (
        SELECT COALESCE(jsonb_object_agg(section_group.section, section_group.row_count ORDER BY section_group.section), '{}'::jsonb) AS section_counts_json
        FROM (
          SELECT preview_section.section, COUNT(*)::integer AS row_count
          FROM public.banking_pay_workbench_preview_rows AS preview_section
          WHERE preview_section.session_id = p_session_id
            AND preview_section.candidate_id = p_candidate_id
            AND preview_section.session_version = COALESCE(v_session_version, v_session_row.version)
            AND preview_section.status = 'READY'
          GROUP BY preview_section.section
        ) AS section_group
      )
      SELECT jsonb_build_object(
               'line_units_total', pre_line_counts.line_units_total,
               'line_units_ready', pre_line_counts.line_units_ready,
               'line_units_pending', pre_line_counts.line_units_pending,
               'line_units_failed', pre_line_counts.line_units_failed,
               'preview_row_count', pre_preview_counts.preview_row_count,
               'selected_row_count', pre_preview_counts.selected_row_count,
               'section_counts_json', pre_section_counts.section_counts_json
             )
      INTO v_pre_candidate_counts_json
      FROM pre_line_counts, pre_preview_counts, pre_section_counts;

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('pre_delta_candidate_counts', COALESCE(v_pre_candidate_counts_json, '{}'::jsonb)),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id
        AND projection_run_update.session_id = p_session_id
        AND projection_run_update.candidate_id = p_candidate_id;
    END IF;

    v_write_result := public.pay_workbench_delta_write_compatible_rows_v1(
      p_session_id,
      p_candidate_id,
      v_projection_run_id,
      v_payload_json
        || jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'projection_class', v_projection_class,
          'reset_write_phase', (
            v_phase = 'PROJECT_ROWS'
            OR (
              v_phase = 'WRITE_COMPATIBLE_OUTPUTS'
              AND COALESCE(v_write_phase, 'SUPERSEDE_PREVIEW_ROWS') = 'SUPERSEDE_PREVIEW_ROWS'
              AND COALESCE(v_written_source_count, 0) = 0
              AND COALESCE(v_written_line_work_count, 0) = 0
              AND COALESCE(v_written_preview_count, 0) = 0
            )
          ),
          'shadow_compare_required', v_shadow_compare_required,
          'shadow_compare_enforced', v_shadow_compare_enforced
        )
    );

    IF COALESCE((v_write_result->>'fallback_required')::boolean, false) IS TRUE THEN
      v_fallback_reason := COALESCE(v_write_result->>'fallback_reason', 'DELTA_WRITE_COMPATIBLE_ROWS_FALLBACK_REQUIRED');

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          phase = 'WRITE_COMPATIBLE_OUTPUTS',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('write_result', v_write_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    IF COALESCE((v_write_result->>'more_due')::boolean, false) IS TRUE THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET phase = 'WRITE_COMPATIBLE_OUTPUTS',
          cursor_json = jsonb_build_object('phase', 'WRITE_COMPATIBLE_OUTPUTS', 'cursor', COALESCE(v_current_projection_cursor_json, '{}'::jsonb)),
          write_phase = COALESCE(v_write_result->>'write_phase', projection_run_update.write_phase, 'SUPERSEDE_PREVIEW_ROWS'),
          write_cursor_json = CASE WHEN jsonb_typeof(v_write_result->'write_cursor_json') = 'object' THEN v_write_result->'write_cursor_json' ELSE COALESCE(projection_run_update.write_cursor_json, '{}'::jsonb) END,
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'WRITE_COMPATIBLE_OUTPUTS',
        'next_phase', 'WRITE_COMPATIBLE_OUTPUTS',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', COALESCE(v_write_result->>'stop_reason', 'DELTA_WRITE_PHASE_BUDGET_EXHAUSTED'),
        'elapsed_ms', FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer,
        'source_rows_written', COALESCE((v_write_result->>'source_rows_written')::integer, 0),
        'line_rows_written', COALESCE((v_write_result->>'line_rows_written')::integer, 0),
        'preview_rows_written', COALESCE((v_write_result->>'preview_rows_written')::integer, 0),
        'next_cursor_json', jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'phase', 'WRITE_COMPATIBLE_OUTPUTS',
          'cursor', COALESCE(v_current_projection_cursor_json, '{}'::jsonb),
          'write_phase', COALESCE(v_write_result->>'write_phase', v_write_phase, 'SUPERSEDE_PREVIEW_ROWS'),
          'write_cursor_json', CASE
            WHEN jsonb_typeof(v_write_result->'write_cursor_json') = 'object' THEN v_write_result->'write_cursor_json'
            ELSE COALESCE(v_write_cursor_json, '{}'::jsonb)
          END
        )
      );
    END IF;

    v_source_rows_written := COALESCE((v_write_result->>'source_rows_written')::integer, 0);
    v_line_rows_written := COALESCE((v_write_result->>'line_rows_written')::integer, 0);
    v_preview_rows_written := COALESCE((v_write_result->>'preview_rows_written')::integer, 0);
    v_rows_superseded := COALESCE((v_write_result->>'rows_superseded')::integer, 0);
    v_selected_preserved_count := COALESCE((v_write_result->>'selected_preserved_count')::integer, 0);
    v_selected_cleared_count := COALESCE((v_write_result->>'selected_cleared_count')::integer, 0);
    v_written_source_count := COALESCE(v_written_source_count, 0) + v_source_rows_written;
    v_written_line_work_count := COALESCE(v_written_line_work_count, 0) + v_line_rows_written;
    v_written_preview_count := COALESCE(v_written_preview_count, 0) + v_preview_rows_written;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = CASE WHEN v_projection_more_due THEN 'PROJECT_ROWS' ELSE 'VERIFY_PARITY_OR_SHADOW' END,
        cursor_json = jsonb_build_object(
          'phase', CASE WHEN v_projection_more_due THEN 'PROJECT_ROWS' ELSE 'VERIFY_PARITY_OR_SHADOW' END,
          'cursor', COALESCE(v_projection_cursor_json, '{}'::jsonb)
        ),
        write_phase = 'WRITE_COMPLETE',
        write_cursor_json = jsonb_build_object('phase', 'WRITE_COMPLETE'),
        written_source_count = GREATEST(COALESCE(projection_run_update.written_source_count, 0), v_written_source_count),
        written_line_work_count = GREATEST(COALESCE(projection_run_update.written_line_work_count, 0), v_written_line_work_count),
        written_preview_count = GREATEST(COALESCE(projection_run_update.written_preview_count, 0), v_written_preview_count),
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_write_source_rows', v_source_rows_written,
            'last_write_line_rows', v_line_rows_written,
            'last_write_preview_rows', v_preview_rows_written,
            'last_rows_superseded', v_rows_superseded
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_projection_more_due THEN
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'WRITE_COMPATIBLE_OUTPUTS',
        'next_phase', 'PROJECT_ROWS',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_ROWS_REMAINING',
        'elapsed_ms', v_elapsed_ms,
        'source_rows_written', v_source_rows_written,
        'line_rows_written', v_line_rows_written,
        'preview_rows_written', v_preview_rows_written,
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', 'PROJECT_ROWS', 'cursor', COALESCE(v_projection_cursor_json, '{}'::jsonb))
      );
    END IF;

    IF v_elapsed_ms >= v_budget_cutoff_ms THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'budget_checkpoint_phase', 'WRITE_COMPATIBLE_OUTPUTS',
              'budget_checkpoint_elapsed_ms', v_elapsed_ms,
              'budget_checkpoint_at_utc', v_now::text
            ),
          phase = 'VERIFY_PARITY_OR_SHADOW',
          cursor_json = jsonb_build_object('phase', 'VERIFY_PARITY_OR_SHADOW', 'cursor', '{}'::jsonb),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'WRITE_COMPATIBLE_OUTPUTS',
        'next_phase', 'VERIFY_PARITY_OR_SHADOW',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'source_rows_written', v_source_rows_written,
        'line_rows_written', v_line_rows_written,
        'preview_rows_written', v_preview_rows_written,
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', 'VERIFY_PARITY_OR_SHADOW', 'cursor', '{}'::jsonb)
      );
    END IF;

    v_phase := 'VERIFY_PARITY_OR_SHADOW';
  END IF;


  IF v_phase IN ('VERIFY_PARITY_OR_SHADOW', 'UPDATE_CANDIDATE_STATE', 'FINALISE') THEN
    CREATE TEMP TABLE IF NOT EXISTS _bpay_delta_projection_rows (
      projection_run_id uuid NOT NULL,
      session_id uuid NOT NULL,
      candidate_id uuid NOT NULL,
      session_version bigint NOT NULL,
      source_change_seq bigint,
      timesheet_id uuid,
      canonical_timesheet_id uuid,
      section text NOT NULL,
      line_key text NOT NULL,
      row_key text NOT NULL,
      parent_line_key text,
      split_suffix text,
      source_ordinal bigint NOT NULL,
      row_ordinal bigint NOT NULL,
      pay_channel_scope text,
      refresh_scope_kind text NOT NULL,
      key_type text,
      key_value text,
      amount_ex_vat numeric,
      amount_inc_vat numeric,
      vat_amount numeric,
      draftable boolean NOT NULL DEFAULT false,
      is_ready_for_draft boolean NOT NULL DEFAULT false,
      selection_allowed boolean NOT NULL DEFAULT false,
      readiness_state text,
      blocked_reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
      outstanding_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      source_row_json jsonb NOT NULL,
      economic_key_json jsonb NOT NULL,
      contract_json jsonb NOT NULL,
      work_payload_json jsonb NOT NULL,
      result_row_json jsonb NOT NULL,
      preview_row_json jsonb NOT NULL,
      contract_ok boolean NOT NULL DEFAULT false,
      materialisable boolean NOT NULL DEFAULT false
    ) ON COMMIT DROP;


    TRUNCATE TABLE _bpay_delta_projection_rows;

    v_current_projection_cursor_json := '{}'::jsonb;
    v_project_result := public.pay_workbench_project_changed_timesheets_v1(
      p_session_id,
      p_candidate_id,
      v_projection_run_id,
      v_targeted_timesheet_ids,
      v_linked_timesheet_ids,
      v_payload_json
        || jsonb_build_object(
          'projection_run_id', v_projection_run_id::text,
          'projection_mode', v_projection_mode,
          'projection_class', v_projection_class,
          'session_version', COALESCE(v_session_version, v_session_row.version),
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'source_snapshot_run_id', CASE WHEN v_source_snapshot_run_id IS NULL THEN NULL ELSE v_source_snapshot_run_id::text END,
          'limit', LEAST(GREATEST(COALESCE(v_projected_row_count, 0), COALESCE(v_limit, 25), 25), 1000),
          'cursor', v_current_projection_cursor_json,
          'shadow_compare_required', v_shadow_compare_required,
          'shadow_compare_enforced', v_shadow_compare_enforced,
          'deterministic_recreate', true
        )
    );

    IF COALESCE((v_project_result->>'fallback_required')::boolean, false) IS TRUE THEN
      v_fallback_reason := COALESCE(v_project_result->>'fallback_reason', 'DELTA_RECREATE_PROJECTION_FALLBACK_REQUIRED');

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'deterministic_recreate_failed', true,
              'delta_fallback_reason', v_fallback_reason,
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status IN ('DELTA_PENDING', 'READY')
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('deterministic_recreate_result', v_project_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    IF COALESCE((v_project_result->>'more_due')::boolean, false) IS TRUE THEN
      v_fallback_reason := 'DELTA_DETERMINISTIC_RECREATE_EXCEEDED_SINGLE_CHUNK';

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'deterministic_recreate_incomplete', true,
              'delta_fallback_reason', v_fallback_reason,
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status IN ('DELTA_PENDING', 'READY')
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('deterministic_recreate_result', v_project_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'deterministic_recreate_completed_at_utc', v_now::text,
            'deterministic_recreate_phase', v_phase,
            'deterministic_recreate_row_count', COALESCE((v_project_result->>'projected_row_count')::integer, 0),
            'deterministic_recreate_row_key_line_key_parity_proven', LOWER(BTRIM(COALESCE(v_project_result->>'row_key_line_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
            'deterministic_recreate_economic_key_parity_proven', LOWER(BTRIM(COALESCE(v_project_result->>'economic_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_elapsed_ms >= v_budget_cutoff_ms THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'budget_checkpoint_phase', 'DETERMINISTIC_RECREATE',
              'budget_checkpoint_elapsed_ms', v_elapsed_ms,
              'budget_checkpoint_at_utc', v_now::text
            ),
          cursor_json = jsonb_build_object('phase', v_phase, 'cursor', '{}'::jsonb),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'DETERMINISTIC_RECREATE',
        'next_phase', v_phase,
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', v_phase, 'cursor', '{}'::jsonb)
      );
    END IF;
  END IF;

  IF v_phase = 'VERIFY_PARITY_OR_SHADOW' THEN
    IF v_shadow_compare_required OR v_shadow_compare_enforced THEN
      IF to_regprocedure('public.pay_workbench_delta_shadow_compare_v1(uuid,uuid,uuid,jsonb)') IS NULL THEN
        v_shadow_result := jsonb_build_object(
          'ok', true,
          'compare_status', 'REFERENCE_UNAVAILABLE',
          'fallback_required', v_shadow_compare_enforced,
          'fallback_reason', 'DELTA_SHADOW_COMPARE_FUNCTION_MISSING'
        );
      ELSE
        v_shadow_result := public.pay_workbench_delta_shadow_compare_v1(
          p_session_id,
          p_candidate_id,
          v_projection_run_id,
          v_payload_json || jsonb_build_object(
            'shadow_compare_required', v_shadow_compare_required,
            'shadow_compare_enforced', v_shadow_compare_enforced
          )
        );
      END IF;

      v_shadow_compare_status := upper(BTRIM(COALESCE(v_shadow_result->>'compare_status', 'UNKNOWN')));
      v_shadow_reference_unavailable := v_shadow_compare_status IN ('REFERENCE_UNAVAILABLE', 'REFERENCE_MISSING', 'UNAVAILABLE');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET shadow_compare_required = v_shadow_compare_required,
          shadow_compare_enforced = v_shadow_compare_enforced,
          shadow_compare_status = v_shadow_compare_status,
          legacy_compare_status = v_shadow_compare_status,
          legacy_compare_json = jsonb_build_object(
            'compare_status', v_shadow_compare_status,
            'source_mismatches', COALESCE((v_shadow_result->>'source_mismatches')::integer, 0),
            'line_work_mismatches', COALESCE((v_shadow_result->>'line_work_mismatches')::integer, 0),
            'preview_mismatches', COALESCE((v_shadow_result->>'preview_mismatches')::integer, 0),
            'reference_unavailable', v_shadow_reference_unavailable
          ),
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('shadow_compare_status', v_shadow_compare_status),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      IF v_shadow_compare_status = 'MISMATCH'
         OR (v_shadow_reference_unavailable AND v_shadow_compare_enforced)
         OR COALESCE((v_shadow_result->>'fallback_required')::boolean, false) IS TRUE THEN
        v_fallback_reason := COALESCE(v_shadow_result->>'fallback_reason', CASE WHEN v_shadow_reference_unavailable THEN 'SHADOW_COMPARE_REFERENCE_UNAVAILABLE' ELSE 'SHADOW_COMPARE_MISMATCH' END);

        UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
        SET selected = false,
            selection_state = 'DIRTY',
            status = 'DIRTY',
            row_json = jsonb_strip_nulls(COALESCE(preview_row_update.row_json, '{}'::jsonb)
              || jsonb_build_object('delta_fallback_reason', v_fallback_reason, 'shadow_compare_failed', true)),
            updated_at_utc = v_now
        WHERE preview_row_update.session_id = p_session_id
          AND preview_row_update.candidate_id = p_candidate_id
          AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text
          AND preview_row_update.status = 'DELTA_PENDING';

        UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
        SET status = 'SUPERSEDED',
            source_row_json = jsonb_strip_nulls(
              COALESCE(source_row_update.source_row_json, '{}'::jsonb)
              || jsonb_build_object(
                'delta_fallback_cleanup_applied', true,
                'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
                'shadow_compare_failed', true,
                'delta_fallback_at_utc', v_now::text
              )
            ),
            updated_at_utc = v_now
        WHERE source_row_update.session_id = p_session_id
          AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

        UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
        SET status = 'SKIPPED',
            result_row_json = jsonb_strip_nulls(
              COALESCE(line_work_update.result_row_json, '{}'::jsonb)
              || jsonb_build_object(
                'delta_fallback_cleanup_applied', true,
                'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
                'shadow_compare_failed', true,
                'delta_fallback_at_utc', v_now::text
              )
            ),
            updated_at_utc = v_now
        WHERE line_work_update.session_id = p_session_id
          AND line_work_update.candidate_id = p_candidate_id
          AND (
            COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
            OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
          )
          AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

        UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
        SET status = 'FALLBACK_REQUIRED',
            fallback_required = true,
            fallback_reason = v_fallback_reason,
            updated_at_utc = v_now,
            completed_at_utc = v_now
        WHERE projection_run_update.id = v_projection_run_id;

        RETURN jsonb_build_object(
          'ok', true,
          'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id', v_projection_run_id::text,
          'fallback_required', true,
          'fallback_reason', v_fallback_reason,
          'shadow_compare_status', v_shadow_compare_status,
          'shadow_compare_failed', true,
          'more_due', false,
          'has_more', false,
          'made_progress', true,
          'delta_refresh_complete', false,
          'stop_reason', 'DELTA_FALLBACK_REQUIRED'
        );
      END IF;
    ELSE
      v_shadow_compare_status := 'NOT_REQUIRED';
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET shadow_compare_required = false,
          shadow_compare_enforced = false,
          shadow_compare_status = 'NOT_REQUIRED',
          legacy_compare_status = 'NOT_REQUIRED',
          legacy_compare_json = jsonb_build_object(
            'compare_status', 'NOT_REQUIRED',
            'shadow_compare_not_required', true
          ),
          phase = 'UPDATE_CANDIDATE_STATE',
          cursor_json = jsonb_build_object('phase', 'UPDATE_CANDIDATE_STATE', 'cursor', '{}'::jsonb),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
    END IF;

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = 'UPDATE_CANDIDATE_STATE',
        cursor_json = jsonb_build_object('phase', 'UPDATE_CANDIDATE_STATE', 'cursor', '{}'::jsonb),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_elapsed_ms >= v_budget_cutoff_ms THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'budget_checkpoint_phase', 'VERIFY_PARITY_OR_SHADOW',
              'budget_checkpoint_elapsed_ms', v_elapsed_ms,
              'budget_checkpoint_at_utc', v_now::text
            ),
          phase = 'UPDATE_CANDIDATE_STATE',
          cursor_json = jsonb_build_object('phase', 'UPDATE_CANDIDATE_STATE', 'cursor', '{}'::jsonb),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'VERIFY_PARITY_OR_SHADOW',
        'next_phase', 'UPDATE_CANDIDATE_STATE',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'shadow_compare_status', COALESCE(NULLIF(v_shadow_compare_status, ''), 'NOT_REQUIRED'),
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', 'UPDATE_CANDIDATE_STATE', 'cursor', '{}'::jsonb)
      );
    END IF;

    v_phase := 'UPDATE_CANDIDATE_STATE';
  END IF;

  IF v_phase='UPDATE_CANDIDATE_STATE' AND v_sealed_lifecycle_delta THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS sealed_run
    SET phase='FINALISE',cursor_json=jsonb_build_object('phase','FINALISE','cursor','{}'::jsonb),
        candidate_state_updated=false,updated_at_utc=v_now
    WHERE sealed_run.id=v_projection_run_id;
    v_phase:='FINALISE';
  END IF;

  IF v_phase = 'UPDATE_CANDIDATE_STATE' THEN
    SELECT COUNT(*) FILTER (
             WHERE COALESCE(preview_row.row_json->>'policy_x_authority_scope', '') <> 'PRE_DRAFT_LIVE_TRUTH'
           )::integer,
           COUNT(*) FILTER (
             WHERE (preview_row.selected IS TRUE OR COALESCE(preview_row.row_json->>'target_selected', 'false') IN ('true', 't', '1', 'yes', 'y', 'on'))
               AND (preview_row.timesheet_id IS NULL OR NULLIF(BTRIM(COALESCE(preview_row.key_type, '')), '') IS NULL OR NULLIF(BTRIM(COALESCE(preview_row.key_value, '')), '') IS NULL)
           )::integer,
           COUNT(*) FILTER (
             WHERE COALESCE(preview_row.row_json->>'projection_certified', 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
           )::integer,
           COUNT(*) FILTER (
             WHERE (preview_row.selected IS TRUE OR COALESCE(preview_row.row_json->>'target_selected', 'false') IN ('true', 't', '1', 'yes', 'y', 'on'))
               AND COALESCE(preview_row.row_json->>'row_key_line_key_parity_proven', 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
               AND NOT (
                 (v_shadow_compare_required OR v_shadow_compare_enforced)
                 AND COALESCE(v_shadow_compare_status, '') = 'MATCH'
               )
           )::integer
    INTO v_policy_x_invalid_count,
         v_economic_key_invalid_count,
         v_projection_cert_invalid_count,
         v_key_parity_invalid_count
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.status IN ('DELTA_PENDING', 'READY')
      AND COALESCE(preview_row.row_json->>'projection_run_id', '') = v_projection_run_id::text;

    IF COALESCE(v_policy_x_invalid_count, 0) > 0
       OR COALESCE(v_economic_key_invalid_count, 0) > 0
       OR COALESCE(v_projection_cert_invalid_count, 0) > 0
       OR COALESCE(v_key_parity_invalid_count, 0) > 0 THEN
      v_fallback_reason := 'DELTA_FINAL_SAFETY_GUARD_FAILED';
      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object('delta_final_safety_guard_failed', true, 'delta_fallback_reason', v_fallback_reason)),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status = 'DELTA_PENDING'
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'policy_x_invalid_count', COALESCE(v_policy_x_invalid_count, 0),
              'economic_key_invalid_count', COALESCE(v_economic_key_invalid_count, 0),
              'projection_cert_invalid_count', COALESCE(v_projection_cert_invalid_count, 0),
              'key_parity_invalid_count', COALESCE(v_key_parity_invalid_count, 0)
            ),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
    SET status = 'READY',
        selected = CASE
          WHEN (v_shadow_compare_required OR v_shadow_compare_enforced)
           AND COALESCE(v_shadow_compare_status, '') = 'MATCH'
           AND COALESCE(preview_row_update.row_json->>'target_selected', 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
           AND COALESCE(preview_row_update.row_json->>'target_selection_state', '') = 'SELECTED'
          THEN true
          ELSE false
        END,
        selection_state = CASE
          WHEN (v_shadow_compare_required OR v_shadow_compare_enforced)
           AND COALESCE(v_shadow_compare_status, '') = 'MATCH'
           AND COALESCE(preview_row_update.row_json->>'target_selected', 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
           AND COALESCE(preview_row_update.row_json->>'target_selection_state', '') = 'SELECTED'
          THEN 'SELECTED'
          ELSE 'NOT_SELECTABLE'
        END,
        row_json = jsonb_strip_nulls(
          COALESCE(preview_row_update.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'shadow_compare_status', COALESCE(NULLIF(v_shadow_compare_status, ''), 'NOT_REQUIRED')
          )
          || CASE
            WHEN COALESCE(v_shadow_compare_status, '') = 'MATCH' THEN jsonb_build_object(
              'shadow_compare_passed', true,
              'projection_certified', true,
              'shadow_promoted_at_utc', v_now::text
            )
            ELSE jsonb_build_object(
              'shadow_compare_passed', false
            )
          END
        ),
        updated_at_utc = v_now
    WHERE preview_row_update.session_id = p_session_id
      AND preview_row_update.candidate_id = p_candidate_id
      AND preview_row_update.status = 'DELTA_PENDING'
      AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

    v_candidate_state_result := public.pay_workbench_delta_update_candidate_state_v1(
      p_session_id,
      p_candidate_id,
      v_projection_run_id,
      v_payload_json || jsonb_build_object('context', 'DELTA_REFRESH')
    );

    IF LOWER(BTRIM(COALESCE(v_candidate_state_result->>'stale_candidate_state_update_skipped', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'candidate_state_update_skipped', true,
              'source_change_seq', COALESCE(v_source_change_seq, 0),
              'existing_source_change_seq', CASE WHEN COALESCE(v_candidate_state_result->>'existing_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (v_candidate_state_result->>'existing_source_change_seq')::bigint ELSE 0::bigint END,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'candidate_state_update_skipped', true,
              'source_change_seq', COALESCE(v_source_change_seq, 0),
              'existing_source_change_seq', CASE WHEN COALESCE(v_candidate_state_result->>'existing_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (v_candidate_state_result->>'existing_source_change_seq')::bigint ELSE 0::bigint END,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_newer_source_change_seq', true,
              'candidate_state_update_skipped', true,
              'source_change_seq', COALESCE(v_source_change_seq, 0),
              'existing_source_change_seq', CASE WHEN COALESCE(v_candidate_state_result->>'existing_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (v_candidate_state_result->>'existing_source_change_seq')::bigint ELSE 0::bigint END,
              'superseded_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FAILED',
          fallback_required = false,
          fallback_reason = 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ',
          candidate_state_updated = false,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'candidate_state_result', v_candidate_state_result,
              'superseded_by_newer_source_change_seq', true,
              'terminalised_by', 'pay_workbench_candidate_delta_refresh_chunk_candidate_state_guard',
              'terminalised_at_utc', v_now::text
            ),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id
        AND projection_run_update.session_id = p_session_id
        AND projection_run_update.candidate_id = p_candidate_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', false,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', true,
        'candidate_state_updated', false,
        'superseded_by_newer_source_change_seq', true,
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'newer_source_change_seq', CASE WHEN COALESCE(v_candidate_state_result->>'existing_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (v_candidate_state_result->>'existing_source_change_seq')::bigint ELSE COALESCE(v_live_source_change_seq, 0) END,
        'candidate_state_result', v_candidate_state_result,
        'stop_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ'
      );
    END IF;

    IF COALESCE((v_candidate_state_result->>'ok')::boolean, false) IS NOT TRUE THEN
      v_fallback_reason := COALESCE(v_candidate_state_result->>'fallback_reason', 'DELTA_CANDIDATE_STATE_UPDATE_FAILED');

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'candidate_state_update_failed', true,
              'delta_fallback_reason', v_fallback_reason,
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status IN ('DELTA_PENDING', 'READY')
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object('candidate_state_result', v_candidate_state_result),
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    v_candidate_state_updated := true;
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET phase = 'FINALISE',
        cursor_json = jsonb_build_object('phase', 'FINALISE', 'cursor', '{}'::jsonb),
        candidate_state_updated = true,
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object('candidate_state_result', v_candidate_state_result),
        updated_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    IF v_elapsed_ms >= v_budget_cutoff_ms THEN
      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
            || jsonb_build_object(
              'budget_checkpoint_phase', 'UPDATE_CANDIDATE_STATE',
              'budget_checkpoint_elapsed_ms', v_elapsed_ms,
              'budget_checkpoint_at_utc', v_now::text
            ),
          phase = 'FINALISE',
          cursor_json = jsonb_build_object('phase', 'FINALISE', 'cursor', '{}'::jsonb),
          updated_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'phase', 'UPDATE_CANDIDATE_STATE',
        'next_phase', 'FINALISE',
        'more_due', true,
        'has_more', true,
        'made_progress', true,
        'fallback_required', false,
        'delta_refresh_complete', false,
        'candidate_state_updated', true,
        'stop_reason', 'DELTA_PHASE_BUDGET_EXHAUSTED',
        'elapsed_ms', v_elapsed_ms,
        'next_cursor_json', jsonb_build_object('projection_run_id', v_projection_run_id::text, 'phase', 'FINALISE', 'cursor', '{}'::jsonb)
      );
    END IF;

    v_phase := 'FINALISE';
  END IF;

  IF v_phase = 'FINALISE' THEN
    IF v_sealed_lifecycle_delta THEN
      BEGIN
        v_finalize_result:=private.pay_workbench_targeted_delta_scope_finalize_v1(
          p_session_id,p_candidate_id,v_projection_run_id
        );
      EXCEPTION WHEN OTHERS THEN
        v_fallback_reason:='TARGETED_DELTA_ATOMIC_FINALISE_FAILED';
        UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS failed_run
        SET status='FALLBACK_REQUIRED',fallback_required=true,
            fallback_reason=v_fallback_reason,completed_at_utc=v_now,updated_at_utc=v_now,
            diagnostics_json=COALESCE(failed_run.diagnostics_json,'{}'::jsonb)
              ||jsonb_build_object('finalise_sqlstate',SQLSTATE,'finalise_failed',true)
        WHERE failed_run.id=v_projection_run_id;
        RETURN jsonb_build_object(
          'ok',true,'job_type','WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id',v_projection_run_id,'fallback_required',true,
          'fallback_reason',v_fallback_reason,'more_due',false,'has_more',false,
          'made_progress',true,'delta_refresh_complete',false,
          'stop_reason','DELTA_FALLBACK_REQUIRED'
        );
      END;
      RETURN COALESCE(v_finalize_result,'{}'::jsonb)||jsonb_build_object(
        'job_type','WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id',v_projection_run_id,'fallback_required',false,
        'more_due',false,'has_more',false,'made_progress',true,
        'delta_refresh_complete',true,'stop_reason','DELTA_COMPLETE'
      );
    END IF;

    SELECT COALESCE(projection_run.candidate_state_updated, false),
           COALESCE(projection_run.fallback_required, false),
           projection_run.fallback_reason,
           COALESCE(projection_run.shadow_compare_status, 'NOT_REQUIRED'),
           COALESCE(projection_run.shadow_compare_required, false),
           COALESCE(projection_run.shadow_compare_enforced, false),
           COALESCE(projection_run.projected_row_count, 0),
           COALESCE(projection_run.written_source_count, 0),
           COALESCE(projection_run.written_line_work_count, 0),
           COALESCE(projection_run.written_preview_count, 0)
    INTO v_candidate_state_updated,
         v_fallback_allowed,
         v_fallback_reason,
         v_shadow_compare_status,
         v_shadow_compare_required,
         v_shadow_compare_enforced,
         v_projected_row_count,
         v_written_source_count,
         v_written_line_work_count,
         v_written_preview_count
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.id = v_projection_run_id
      AND projection_run.session_id = p_session_id
      AND projection_run.candidate_id = p_candidate_id
    FOR UPDATE;

    IF v_fallback_allowed IS TRUE OR v_candidate_state_updated IS NOT TRUE THEN
      v_fallback_reason := COALESCE(v_fallback_reason, 'DELTA_FINALISE_GUARD_NOT_SATISFIED');

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'finalise_guard_failed', true,
              'delta_fallback_reason', v_fallback_reason,
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status IN ('DELTA_PENDING', 'READY')
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    IF COALESCE(v_shadow_compare_enforced, false) IS TRUE
       AND COALESCE(v_shadow_compare_status, '') NOT IN ('MATCH', 'NOT_REQUIRED') THEN
      v_fallback_reason := 'DELTA_SHADOW_COMPARE_NOT_PASSED_AT_FINALISE';

      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'shadow_compare_not_passed_at_finalise', true,
              'shadow_compare_status', COALESCE(v_shadow_compare_status, 'UNKNOWN'),
              'delta_fallback_reason', v_fallback_reason,
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND preview_row_update.status IN ('DELTA_PENDING', 'READY')
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;


      UPDATE public.banking_pay_workbench_preview_rows AS preview_row_update
      SET selected = false,
          selection_state = 'DIRTY',
          status = 'DIRTY',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_row_update.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE preview_row_update.session_id = p_session_id
        AND preview_row_update.candidate_id = p_candidate_id
        AND COALESCE(preview_row_update.row_json->>'projection_run_id', '') = v_projection_run_id::text;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_row_update.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE source_row_update.session_id = p_session_id
        AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = v_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_update.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_fallback_cleanup_applied', true,
              'delta_fallback_reason', COALESCE(v_fallback_reason, 'DELTA_FALLBACK_REQUIRED'),
              'delta_fallback_at_utc', v_now::text
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_update.session_id = p_session_id
        AND line_work_update.candidate_id = p_candidate_id
        AND (
          COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = v_projection_run_id::text
          OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = v_projection_run_id::text
        )
        AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

      UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
      SET status = 'FALLBACK_REQUIRED',
          fallback_required = true,
          fallback_reason = v_fallback_reason,
          updated_at_utc = v_now,
          completed_at_utc = v_now
      WHERE projection_run_update.id = v_projection_run_id;
      RETURN jsonb_build_object(
        'ok', true,
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'fallback_required', true,
        'fallback_reason', v_fallback_reason,
        'more_due', false,
        'has_more', false,
        'made_progress', true,
        'delta_refresh_complete', false,
        'stop_reason', 'DELTA_FALLBACK_REQUIRED'
      );
    END IF;

    SELECT COALESCE(projection_run.diagnostics_json->'pre_delta_candidate_counts', '{}'::jsonb)
    INTO v_pre_candidate_counts_json
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.id = v_projection_run_id
      AND projection_run.session_id = p_session_id
      AND projection_run.candidate_id = p_candidate_id;

    WITH post_line_counts AS (
      SELECT COUNT(*)::integer AS line_units_total,
             COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SKIPPED'))::integer AS line_units_ready,
             COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('PENDING', 'PROCESSING', 'DIRTY'))::integer AS line_units_pending,
             COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('ERROR', 'FAILED'))::integer AS line_units_failed
      FROM public.banking_pay_workbench_candidate_line_work AS line_work
      WHERE line_work.session_id = p_session_id
        AND line_work.candidate_id = p_candidate_id
    ), post_preview_counts AS (
      SELECT COUNT(*) FILTER (WHERE preview_row.status = 'READY')::integer AS preview_row_count,
             COUNT(*) FILTER (WHERE preview_row.status = 'READY' AND preview_row.selected IS TRUE AND preview_row.selection_state = 'SELECTED')::integer AS selected_row_count
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = p_candidate_id
        AND preview_row.session_version = COALESCE(v_session_version, v_session_row.version)
    ), post_section_counts AS (
      SELECT COALESCE(jsonb_object_agg(section_group.section, section_group.row_count ORDER BY section_group.section), '{}'::jsonb) AS section_counts_json
      FROM (
        SELECT preview_section.section, COUNT(*)::integer AS row_count
        FROM public.banking_pay_workbench_preview_rows AS preview_section
        WHERE preview_section.session_id = p_session_id
          AND preview_section.candidate_id = p_candidate_id
          AND preview_section.session_version = COALESCE(v_session_version, v_session_row.version)
          AND preview_section.status = 'READY'
        GROUP BY preview_section.section
      ) AS section_group
    ), post_sample_rows AS (
      SELECT COALESCE(jsonb_agg(sample_row.sample_json ORDER BY sample_row.row_ordinal, sample_row.id), '[]'::jsonb) AS candidate_sample_rows_json
      FROM (
        SELECT preview_sample.id,
               preview_sample.row_ordinal,
               jsonb_build_object(
                 'id', preview_sample.id::text,
                 'candidate_id', preview_sample.candidate_id::text,
                 'section', preview_sample.section,
                 'row_key', preview_sample.row_key,
                 'selected', preview_sample.selected,
                 'selection_state', preview_sample.selection_state
               ) AS sample_json
        FROM public.banking_pay_workbench_preview_rows AS preview_sample
        WHERE preview_sample.session_id = p_session_id
          AND preview_sample.candidate_id = p_candidate_id
          AND preview_sample.session_version = COALESCE(v_session_version, v_session_row.version)
          AND preview_sample.status = 'READY'
        ORDER BY preview_sample.row_ordinal, preview_sample.id
        LIMIT 50
      ) AS sample_row
    )
    SELECT jsonb_build_object(
             'line_units_total', post_line_counts.line_units_total,
             'line_units_ready', post_line_counts.line_units_ready,
             'line_units_pending', post_line_counts.line_units_pending,
             'line_units_failed', post_line_counts.line_units_failed,
             'preview_row_count', post_preview_counts.preview_row_count,
             'selected_row_count', post_preview_counts.selected_row_count,
             'section_counts_json', post_section_counts.section_counts_json
           ),
           post_line_counts.line_units_total - COALESCE((v_pre_candidate_counts_json->>'line_units_total')::integer, 0),
           post_line_counts.line_units_ready - COALESCE((v_pre_candidate_counts_json->>'line_units_ready')::integer, 0),
           post_line_counts.line_units_pending - COALESCE((v_pre_candidate_counts_json->>'line_units_pending')::integer, 0),
           post_line_counts.line_units_failed - COALESCE((v_pre_candidate_counts_json->>'line_units_failed')::integer, 0),
           post_preview_counts.preview_row_count - COALESCE((v_pre_candidate_counts_json->>'preview_row_count')::integer, 0),
           post_preview_counts.selected_row_count - COALESCE((v_pre_candidate_counts_json->>'selected_row_count')::integer, 0),
           post_section_counts.section_counts_json,
           post_sample_rows.candidate_sample_rows_json
    INTO v_post_candidate_counts_json,
         v_line_units_total_delta,
         v_line_units_ready_delta,
         v_line_units_pending_delta,
         v_line_units_failed_delta,
         v_preview_row_count_delta,
         v_selected_row_count_delta,
         v_section_counts_json,
         v_candidate_sample_rows_json
    FROM post_line_counts, post_preview_counts, post_section_counts, post_sample_rows;

    SELECT COALESCE(jsonb_object_agg(section_keys.section_key, to_jsonb(section_keys.section_delta) ORDER BY section_keys.section_key), '{}'::jsonb)
    INTO v_section_delta_json
    FROM (
      SELECT all_sections.section_key,
             COALESCE((v_section_counts_json->>all_sections.section_key)::integer, 0)
             - COALESCE((COALESCE(v_pre_candidate_counts_json->'section_counts_json', '{}'::jsonb)->>all_sections.section_key)::integer, 0) AS section_delta
      FROM (
        SELECT current_sections.key AS section_key
        FROM jsonb_each(COALESCE(v_section_counts_json, '{}'::jsonb)) AS current_sections(key, value)
        UNION
        SELECT previous_sections.key AS section_key
        FROM jsonb_each(COALESCE(v_pre_candidate_counts_json->'section_counts_json', '{}'::jsonb)) AS previous_sections(key, value)
      ) AS all_sections
    ) AS section_keys;

    v_scope_ready_delta := CASE WHEN UPPER(BTRIM(COALESCE(v_scope_row.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY') THEN 0 ELSE 1 END;
    v_scope_seeded_delta := CASE WHEN COALESCE(v_scope_row.seeded, false) IS TRUE THEN 0 ELSE 1 END;
    v_scope_pending_delta := CASE WHEN UPPER(BTRIM(COALESCE(v_scope_row.status, ''))) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY', 'FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR') THEN -1 ELSE 0 END;
    v_scope_failed_delta := CASE WHEN UPPER(BTRIM(COALESCE(v_scope_row.status, ''))) IN ('FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR') THEN -1 ELSE 0 END;


    UPDATE public.banking_pay_workbench_session_scope AS scope_row_update
    SET status = 'READY',
        seeded = true,
        dirty = false,
        pending_job_id = NULL::uuid,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE scope_row_update.session_id = p_session_id
      AND scope_row_update.candidate_id = p_candidate_id;

    PERFORM public.pay_workbench_session_recompute_progress_counters(
      p_session_id => p_session_id,
      p_apply => true,
      p_reason => 'DELTA_REFRESH_COMPLETE_AUTHORITATIVE_RECOMPUTE',
      p_write_progress_json => true
    );

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET status = 'COMPLETED',
        phase = 'FINALISE',
        cursor_json = jsonb_build_object('phase', 'FINALISE', 'cursor', '{}'::jsonb),
        fallback_required = false,
        fallback_reason = NULL::text,
        candidate_state_updated = true,
        shadow_compare_required = COALESCE(v_shadow_compare_required, false),
        shadow_compare_enforced = COALESCE(v_shadow_compare_enforced, false),
        shadow_compare_status = COALESCE(NULLIF(v_shadow_compare_status, ''), 'NOT_REQUIRED'),
        legacy_compare_status = CASE
          WHEN COALESCE(v_shadow_compare_required, false) OR COALESCE(v_shadow_compare_enforced, false)
            THEN projection_run_update.legacy_compare_status
          ELSE 'NOT_REQUIRED'
        END,
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'finalise_candidate_state_updated', true,
            'finalise_shadow_compare_status', COALESCE(NULLIF(v_shadow_compare_status, ''), 'NOT_REQUIRED'),
            'finalise_policy_x_invalid_count', COALESCE(v_policy_x_invalid_count, 0),
            'finalise_economic_key_invalid_count', COALESCE(v_economic_key_invalid_count, 0),
            'finalise_projection_cert_invalid_count', COALESCE(v_projection_cert_invalid_count, 0),
            'finalise_key_parity_invalid_count', COALESCE(v_key_parity_invalid_count, 0),
            'finalise_completed_at_utc', v_now::text
          ),
        updated_at_utc = v_now,
        completed_at_utc = v_now
    WHERE projection_run_update.id = v_projection_run_id;

    v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
    v_return_json := jsonb_build_object(
      'ok', true,
      'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'projection_run_id', v_projection_run_id::text,
      'candidate_id', p_candidate_id::text,
      'session_id', p_session_id::text,
      'fallback_required', false,
      'fallback_reason', NULL,
      'more_due', false,
      'has_more', false,
      'made_progress', true,
      'delta_refresh_complete', true,
      'candidate_state_updated', true,
      'shadow_compare_required', COALESCE(v_shadow_compare_required, false),
      'shadow_compare_enforced', COALESCE(v_shadow_compare_enforced, false),
      'shadow_compare_status', COALESCE(NULLIF(v_shadow_compare_status, ''), 'NOT_REQUIRED'),
      'source_rows_written', COALESCE(v_written_source_count, 0),
      'line_rows_written', COALESCE(v_written_line_work_count, 0),
      'preview_rows_written', COALESCE(v_written_preview_count, 0),
      'rows_superseded', COALESCE(v_rows_superseded, 0),
      'selected_preserved_count', COALESCE(v_selected_preserved_count, 0),
      'selected_cleared_count', COALESCE(v_selected_cleared_count, 0),
      'elapsed_ms', v_elapsed_ms,
      'stop_reason', 'DELTA_REFRESH_COMPLETE'
    );

    RETURN v_return_json;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'projection_run_id', v_projection_run_id::text,
    'fallback_required', true,
    'fallback_reason', 'DELTA_UNKNOWN_PHASE_STATE',
    'more_due', false,
    'has_more', false,
    'made_progress', true,
    'delta_refresh_complete', false,
    'stop_reason', 'DELTA_FALLBACK_REQUIRED'
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_delta_write_compatible_rows_v1(p_session_id uuid, p_candidate_id uuid, p_projection_run_id uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms integer := 0;
  v_budget_ms integer := 3000;
  v_budget_cutoff_ms integer := 2400;
  v_settings_json jsonb := '{}'::jsonb;
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_projection_row_count integer := 0;
  v_preview_rows_superseded integer := 0;
  v_source_rows_superseded integer := 0;
  v_line_rows_superseded integer := 0;
  v_source_rows_written integer := 0;
  v_line_rows_written integer := 0;
  v_preview_rows_written integer := 0;
  v_selected_preserved_count integer := 0;
  v_selected_cleared_count integer := 0;
  v_affected_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_cleanup_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_current_source_change_seq bigint := 0;
  v_current_session_version bigint := NULL::bigint;
  v_affected_key_count integer := 0;
  v_write_phase text := 'SUPERSEDE_PREVIEW';
  v_write_cursor_json jsonb := '{}'::jsonb;
  v_write_phase_order integer := 1;
  v_selection_guard_violation_count integer := 0;
  v_invalid_direct_preview_contract_count integer := 0;
  v_shadow_compare_required boolean := false;
  v_shadow_compare_enforced boolean := false;
  v_shadow_compare_status text := NULL::text;
  v_shadow_compare_passed boolean := false;
  v_run_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_run_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_selection_intent_mode text := '';
  v_explicit_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_reconciled_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_reconciled_selected_row_count integer := 0;
  v_sealed_targeted_delta boolean := false;
  v_sealed_event_class text := '';
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  SELECT COALESCE(to_jsonb(settings_row), '{}'::jsonb)
  INTO v_settings_json
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  IF COALESCE(v_settings_json->>'banking_pay_workbench_delta_budget_ms', '') ~ '^[0-9]{1,9}$' THEN
    v_budget_ms := LEAST(GREATEST((v_settings_json->>'banking_pay_workbench_delta_budget_ms')::integer, 500), 30000);
  ELSE
    v_budget_ms := 3000;
  END IF;
  v_budget_cutoff_ms := GREATEST(250, FLOOR(v_budget_ms * 0.80)::integer);

  SELECT COALESCE(projection_run.write_phase, 'SUPERSEDE_PREVIEW'),
         COALESCE(projection_run.write_cursor_json, '{}'::jsonb),
         COALESCE(projection_run.shadow_compare_required, false),
         COALESCE(projection_run.shadow_compare_enforced, false),
         NULLIF(UPPER(BTRIM(COALESCE(projection_run.shadow_compare_status, ''))), ''),
         COALESCE(projection_run.targeted_timesheet_ids, '[]'::jsonb),
         COALESCE(projection_run.linked_timesheet_ids, '[]'::jsonb),
         (
           projection_run.admission_seal_version = 1
           AND projection_run.admission_seal_digest IS NOT NULL
           AND projection_run.admission_sealed_at_utc IS NOT NULL
           AND UPPER(BTRIM(COALESCE(projection_run.projection_class, ''))) = 'TIMESHEET_LIFECYCLE'
         ),
         UPPER(BTRIM(COALESCE(projection_run.admission_seal_json->>'event_class','')))
  INTO v_write_phase,
       v_write_cursor_json,
       v_shadow_compare_required,
       v_shadow_compare_enforced,
       v_shadow_compare_status,
       v_run_targeted_timesheet_ids_json,
       v_run_linked_timesheet_ids_json,
       v_sealed_targeted_delta,
       v_sealed_event_class
  FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
  WHERE projection_run.id = p_projection_run_id
    AND projection_run.session_id = p_session_id
    AND projection_run.candidate_id = p_candidate_id
  LIMIT 1;

  IF COALESCE(v_sealed_targeted_delta,false) IS TRUE THEN
    UPDATE public.banking_pay_workbench_session_scope AS staged_scope
    SET certified_preview_publication_required=true,
        certified_preview_publication_parity_ok=false,
        certified_preview_publication_session_version=NULL,
        certified_preview_publication_source_change_seq=NULL,
        certified_preview_publication_source_build_run_id=NULL,
        certified_preview_publication_attestation_json='{}'::jsonb,
        certified_preview_publication_attested_at_utc=NULL,
        status='DELTA_REFRESH_PENDING',
        dirty=true,
        updated_at_utc=v_now
    WHERE staged_scope.session_id=p_session_id
      AND staged_scope.candidate_id=p_candidate_id;
  END IF;

  IF lower(BTRIM(COALESCE(v_payload_json->>'reset_write_phase', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    v_write_phase := 'SUPERSEDE_PREVIEW';
    v_write_cursor_json := '{}'::jsonb;
  END IF;

  v_write_phase := upper(BTRIM(COALESCE(v_write_phase, 'SUPERSEDE_PREVIEW')));
  v_write_phase := CASE v_write_phase
    WHEN 'SUPERSEDE_PREVIEW_ROWS' THEN 'SUPERSEDE_PREVIEW'
    WHEN 'SUPERSEDE_SOURCE_ROWS' THEN 'SUPERSEDE_SOURCE'
    WHEN 'SUPERSEDE_LINE_WORK_ROWS' THEN 'SUPERSEDE_LINE_WORK'
    WHEN 'UPSERT_SOURCE_ROWS' THEN 'UPSERT_SOURCE'
    WHEN 'UPSERT_LINE_WORK_ROWS' THEN 'UPSERT_LINE_WORK'
    WHEN 'UPSERT_PREVIEW_ROWS' THEN 'UPSERT_PREVIEW'
    WHEN 'SUPERSEDE_PREVIEW' THEN 'SUPERSEDE_PREVIEW'
    WHEN 'SUPERSEDE_SOURCE' THEN 'SUPERSEDE_SOURCE'
    WHEN 'SUPERSEDE_LINE_WORK' THEN 'SUPERSEDE_LINE_WORK'
    WHEN 'UPSERT_SOURCE' THEN 'UPSERT_SOURCE'
    WHEN 'UPSERT_LINE_WORK' THEN 'UPSERT_LINE_WORK'
    WHEN 'UPSERT_PREVIEW' THEN 'UPSERT_PREVIEW'
    ELSE v_write_phase
  END;
  v_shadow_compare_status := UPPER(BTRIM(COALESCE(v_shadow_compare_status, '')));
  v_shadow_compare_passed := v_shadow_compare_status IN ('MATCH', 'PASSED');
  v_write_phase_order := CASE v_write_phase
    WHEN 'SUPERSEDE_PREVIEW' THEN 1
    WHEN 'SUPERSEDE_SOURCE' THEN 2
    WHEN 'SUPERSEDE_LINE_WORK' THEN 3
    WHEN 'UPSERT_SOURCE' THEN 4
    WHEN 'UPSERT_LINE_WORK' THEN 5
    WHEN 'UPSERT_PREVIEW' THEN 6
    WHEN 'WRITE_COUNTS' THEN 7
    WHEN 'WRITE_COMPLETE' THEN 8
    ELSE 1
  END;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = COALESCE(v_write_phase, 'SUPERSEDE_PREVIEW'),
      write_cursor_json = COALESCE(v_write_cursor_json, '{}'::jsonb),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_projection_run_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'fallback_required', true,
      'fallback_reason', 'DELTA_WRITE_INPUT_REQUIRED',
      'source_rows_written', 0,
      'line_rows_written', 0,
      'preview_rows_written', 0,
      'rows_superseded', 0
    );
  END IF;

  SELECT UPPER(BTRIM(COALESCE(
           session_row.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}',
           ''
         ))),
         CASE
           WHEN jsonb_typeof(COALESCE(session_row.server_selected_preview_row_ids, '[]'::jsonb)) = 'array'
             THEN COALESCE(session_row.server_selected_preview_row_ids, '[]'::jsonb)
           ELSE '[]'::jsonb
         END
  INTO v_selection_intent_mode,
       v_explicit_selected_preview_row_ids
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  LIMIT 1;

  IF v_selection_intent_mode NOT IN ('IMPLICIT_ALL', 'EXPLICIT_INCLUDE') THEN
    SELECT CASE
             WHEN COALESCE(session_row.server_selected_preview_row_ids_provided, false) THEN 'EXPLICIT_INCLUDE'
             ELSE ''
           END
    INTO v_selection_intent_mode
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = p_session_id
    LIMIT 1;
  END IF;
  v_explicit_selected_preview_row_ids := COALESCE(v_explicit_selected_preview_row_ids, '[]'::jsonb);

  IF to_regclass('pg_temp._bpay_delta_projection_rows') IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'fallback_required', true,
      'fallback_reason', 'DELTA_PROJECTION_TEMP_ROWS_MISSING',
      'source_rows_written', 0,
      'line_rows_written', 0,
      'preview_rows_written', 0,
      'rows_superseded', 0
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT parsed_timesheet_id ORDER BY parsed_timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_cleanup_timesheet_ids
  FROM (
    SELECT CASE WHEN value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN value_text::uuid ELSE NULL::uuid END AS parsed_timesheet_id
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(COALESCE(v_payload_json->'targeted_timesheet_ids', '[]'::jsonb)) = 'array' THEN COALESCE(v_payload_json->'targeted_timesheet_ids', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) AS parsed(value_text)
    UNION ALL
    SELECT CASE WHEN value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN value_text::uuid ELSE NULL::uuid END
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(COALESCE(v_payload_json->'linked_timesheet_ids', '[]'::jsonb)) = 'array' THEN COALESCE(v_payload_json->'linked_timesheet_ids', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) AS parsed(value_text)
    UNION ALL
    SELECT CASE WHEN value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN value_text::uuid ELSE NULL::uuid END
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(COALESCE(v_run_targeted_timesheet_ids_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_run_targeted_timesheet_ids_json, '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) AS parsed(value_text)
    UNION ALL
    SELECT CASE WHEN value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN value_text::uuid ELSE NULL::uuid END
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(COALESCE(v_run_linked_timesheet_ids_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_run_linked_timesheet_ids_json, '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) AS parsed(value_text)
  ) AS parsed_ids
  WHERE parsed_timesheet_id IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_projection_row_count
  FROM pg_temp._bpay_delta_projection_rows AS projection_rows
  WHERE projection_rows.projection_run_id = p_projection_run_id
    AND projection_rows.session_id = p_session_id
    AND projection_rows.candidate_id = p_candidate_id;

  IF COALESCE(v_projection_row_count, 0) = 0 THEN
    IF COALESCE(v_sealed_targeted_delta,false) IS TRUE
       AND v_sealed_event_class <> 'UNAUTHORISE' THEN
      RETURN jsonb_build_object(
        'ok',false,'fallback_required',true,
        'fallback_reason','TARGETED_DELTA_ZERO_AUTHORISE_UNSUPPORTED',
        'source_rows_written',0,'line_rows_written',0,'preview_rows_written',0,'rows_superseded',0
      );
    END IF;
    IF COALESCE(v_sealed_targeted_delta, false) IS NOT TRUE
       AND COALESCE(array_length(v_targeted_cleanup_timesheet_ids, 1), 0) > 0 THEN
      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line_cleanup
      SET status = 'SUPERSEDED',
          source_row_json = jsonb_strip_nulls(
            COALESCE(source_line_cleanup.source_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_projection_run_id', p_projection_run_id::text,
              'superseded_reason', 'DELTA_ZERO_ROWS_TARGETED_TIMESHEET_NON_PAYABLE',
              'superseded_at_utc', v_now::text,
              'policy_x_boundary', 'PRE_DRAFT_WORKBENCH_ONLY_NO_ECONOMIC_CHANGE'
            )
          ),
          updated_at_utc = v_now
      WHERE source_line_cleanup.session_id = p_session_id
        AND source_line_cleanup.candidate_id = p_candidate_id
        AND source_line_cleanup.timesheet_id = ANY(v_targeted_cleanup_timesheet_ids)
        AND UPPER(BTRIM(COALESCE(source_line_cleanup.status, ''))) IN ('CURRENT', 'DIRTY');
      GET DIAGNOSTICS v_source_rows_superseded = ROW_COUNT;

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_cleanup
      SET status = 'SKIPPED',
          result_row_json = jsonb_strip_nulls(
            COALESCE(line_work_cleanup.result_row_json, '{}'::jsonb)
            || jsonb_build_object(
              'skipped_by_projection_run_id', p_projection_run_id::text,
              'skipped_reason', 'DELTA_ZERO_ROWS_TARGETED_TIMESHEET_NON_PAYABLE',
              'skipped_at_utc', v_now::text,
              'policy_x_boundary', 'PRE_DRAFT_WORKBENCH_ONLY_NO_ECONOMIC_CHANGE'
            )
          ),
          updated_at_utc = v_now
      WHERE line_work_cleanup.session_id = p_session_id
        AND line_work_cleanup.candidate_id = p_candidate_id
        AND line_work_cleanup.timesheet_id = ANY(v_targeted_cleanup_timesheet_ids)
        AND UPPER(BTRIM(COALESCE(line_work_cleanup.status, ''))) IN ('PENDING', 'PROCESSING', 'RUNNING', 'QUEUED', 'DIRTY', 'READY', 'MATERIALISED');
      GET DIAGNOSTICS v_line_rows_superseded = ROW_COUNT;

      UPDATE public.banking_pay_workbench_preview_rows AS preview_cleanup
      SET status = 'SUPERSEDED',
          selected = false,
          selection_state = 'SUPERSEDED',
          row_json = jsonb_strip_nulls(
            COALESCE(preview_cleanup.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_projection_run_id', p_projection_run_id::text,
              'superseded_reason', 'DELTA_ZERO_ROWS_TARGETED_TIMESHEET_NON_PAYABLE',
              'superseded_at_utc', v_now::text,
              'policy_x_boundary', 'PRE_DRAFT_WORKBENCH_ONLY_NO_ECONOMIC_CHANGE'
            )
          ),
          updated_at_utc = v_now
      WHERE preview_cleanup.session_id = p_session_id
        AND preview_cleanup.candidate_id = p_candidate_id
        AND preview_cleanup.timesheet_id = ANY(v_targeted_cleanup_timesheet_ids)
        AND (
          UPPER(BTRIM(COALESCE(preview_cleanup.status, ''))) IN ('READY', 'DIRTY', 'DELTA_PENDING')
          OR UPPER(BTRIM(COALESCE(preview_cleanup.selection_state, ''))) IN ('SELECTED', 'DIRTY')
          OR preview_cleanup.selected IS TRUE
        );
      GET DIAGNOSTICS v_preview_rows_superseded = ROW_COUNT;
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'fallback_required', false,
      'source_rows_written', 0,
      'line_rows_written', 0,
      'preview_rows_written', 0,
      'rows_superseded', COALESCE(v_source_rows_superseded, 0) + COALESCE(v_line_rows_superseded, 0) + COALESCE(v_preview_rows_superseded, 0),
      'source_rows_superseded', COALESCE(v_source_rows_superseded, 0),
      'line_rows_superseded', COALESCE(v_line_rows_superseded, 0),
      'preview_rows_superseded', COALESCE(v_preview_rows_superseded, 0),
      'zero_projection_cleanup_applied', COALESCE(array_length(v_targeted_cleanup_timesheet_ids, 1), 0) > 0,
      'zero_projection_cleanup_timesheet_count', COALESCE(array_length(v_targeted_cleanup_timesheet_ids, 1), 0),
      'selected_preserved_count', 0,
      'selected_cleared_count', COALESCE(v_preview_rows_superseded, 0)
    );
  END IF;

  SELECT COUNT(*)::integer
  INTO v_invalid_direct_preview_contract_count
  FROM pg_temp._bpay_delta_projection_rows AS projection_rows
  WHERE projection_rows.projection_run_id = p_projection_run_id
    AND projection_rows.session_id = p_session_id
    AND projection_rows.candidate_id = p_candidate_id
    AND (
      jsonb_typeof(projection_rows.result_row_json->'preview_contract') IS DISTINCT FROM 'object'
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'ok')
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'materialisable')
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'selection_allowed')
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'draftable')
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'is_ready_for_draft')
      OR NOT ((projection_rows.result_row_json->'preview_contract') ? 'is_excluded_from_allocation')
      OR (projection_rows.result_row_json->'preview_contract') ? 'preview_contract'
      OR jsonb_typeof(projection_rows.contract_json->'preview_contract') IS DISTINCT FROM 'object'
      OR projection_rows.contract_json->'preview_contract'
           IS DISTINCT FROM projection_rows.result_row_json->'preview_contract'
      OR (
        LOWER(BTRIM(COALESCE(
          projection_rows.result_row_json#>>'{preview_contract,ok}',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      ) IS DISTINCT FROM COALESCE(projection_rows.contract_ok, false)
      OR (
        LOWER(BTRIM(COALESCE(
          projection_rows.result_row_json#>>'{preview_contract,materialisable}',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      ) IS DISTINCT FROM COALESCE(projection_rows.materialisable, false)
    );

  IF COALESCE(v_invalid_direct_preview_contract_count, 0) > 0 THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET write_phase = 'WRITE_FALLBACK_REQUIRED',
        write_cursor_json = jsonb_build_object(
          'phase', 'WRITE_FALLBACK_REQUIRED',
          'fallback_reason', 'DELTA_DIRECT_PREVIEW_CONTRACT_INVALID'
        ),
        fallback_required = true,
        fallback_reason = 'DELTA_DIRECT_PREVIEW_CONTRACT_INVALID',
        diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'invalid_direct_preview_contract_count', COALESCE(v_invalid_direct_preview_contract_count, 0),
            'direct_preview_contract_required', true,
            'nested_preview_contract_rejected', true,
            'delta_write_contract_validation_at_utc', v_now::text
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;

    RETURN jsonb_build_object(
      'ok', false,
      'fallback_required', true,
      'fallback_reason', 'DELTA_DIRECT_PREVIEW_CONTRACT_INVALID',
      'invalid_direct_preview_contract_count', COALESCE(v_invalid_direct_preview_contract_count, 0),
      'source_rows_written', 0,
      'line_rows_written', 0,
      'preview_rows_written', 0,
      'rows_superseded', 0
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT projection_rows.timesheet_id ORDER BY projection_rows.timesheet_id), ARRAY[]::uuid[])
  INTO v_affected_timesheet_ids
  FROM pg_temp._bpay_delta_projection_rows AS projection_rows
  WHERE projection_rows.projection_run_id = p_projection_run_id
    AND projection_rows.session_id = p_session_id
    AND projection_rows.candidate_id = p_candidate_id
    AND projection_rows.timesheet_id IS NOT NULL;

  SELECT COALESCE(MAX(projection_rows.source_change_seq), 0)::bigint,
         COALESCE(MAX(projection_rows.session_version), NULL)::bigint
  INTO v_current_source_change_seq,
       v_current_session_version
  FROM pg_temp._bpay_delta_projection_rows AS projection_rows
  WHERE projection_rows.projection_run_id = p_projection_run_id
    AND projection_rows.session_id = p_session_id
    AND projection_rows.candidate_id = p_candidate_id;

  SELECT COALESCE(array_agg(DISTINCT affected_scope.timesheet_id ORDER BY affected_scope.timesheet_id), ARRAY[]::uuid[])
  INTO v_affected_timesheet_ids
  FROM (
    SELECT unnest(COALESCE(v_affected_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id
    UNION ALL
    SELECT unnest(COALESCE(v_targeted_cleanup_timesheet_ids, ARRAY[]::uuid[])) AS timesheet_id
  ) AS affected_scope
  WHERE affected_scope.timesheet_id IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_affected_key_count
  FROM (
    SELECT DISTINCT projection_rows.timesheet_id, projection_rows.key_type, projection_rows.key_value
    FROM pg_temp._bpay_delta_projection_rows AS projection_rows
    WHERE projection_rows.projection_run_id = p_projection_run_id
      AND projection_rows.session_id = p_session_id
      AND projection_rows.candidate_id = p_candidate_id
      AND projection_rows.timesheet_id IS NOT NULL
      AND projection_rows.key_type IS NOT NULL
      AND projection_rows.key_value IS NOT NULL
  ) AS affected_keys;

  DROP TABLE IF EXISTS pg_temp._bpay_delta_existing_preview_rows;
  CREATE TEMP TABLE _bpay_delta_existing_preview_rows ON COMMIT DROP AS
  SELECT preview_row.*
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND (v_current_session_version IS NULL OR preview_row.session_version IS NOT DISTINCT FROM v_current_session_version)
    AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
    AND (
      preview_row.timesheet_id = ANY(COALESCE(v_affected_timesheet_ids, ARRAY[]::uuid[]))
      OR EXISTS (
        SELECT 1
        FROM pg_temp._bpay_delta_projection_rows AS projection_rows
        WHERE projection_rows.projection_run_id = p_projection_run_id
          AND projection_rows.session_id = p_session_id
          AND projection_rows.candidate_id = p_candidate_id
          AND projection_rows.timesheet_id IS NOT DISTINCT FROM preview_row.timesheet_id
          AND projection_rows.key_type IS NOT DISTINCT FROM preview_row.key_type
          AND projection_rows.key_value IS NOT DISTINCT FROM preview_row.key_value
      )
    );

  IF v_write_phase_order <= 1 THEN
  IF COALESCE(v_sealed_targeted_delta, false) IS NOT TRUE THEN
  UPDATE public.banking_pay_workbench_preview_rows AS preview_update
  SET status = 'SUPERSEDED',
      selected = false,
      selection_state = 'SUPERSEDED',
      row_json = jsonb_strip_nulls(
        COALESCE(preview_update.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'superseded_by_projection_run_id', p_projection_run_id::text,
          'superseded_by_source_change_seq', NULLIF(v_current_source_change_seq, 0),
          'superseded_by_session_version', v_current_session_version,
          'superseded_reason', 'DELTA_REPROJECTED_AFFECTED_SCOPE',
          'superseded_at_utc', v_now::text,
          'policy_x_boundary', 'PRE_DRAFT_WORKBENCH_ONLY_NO_ECONOMIC_CHANGE'
        )
      ),
      updated_at_utc = v_now
  WHERE preview_update.session_id = p_session_id
    AND preview_update.candidate_id = p_candidate_id
    AND (v_current_session_version IS NULL OR preview_update.session_version IS NOT DISTINCT FROM v_current_session_version)
    AND UPPER(BTRIM(COALESCE(preview_update.status, ''))) IN ('READY', 'DIRTY', 'DELTA_PENDING')
    AND (
      preview_update.timesheet_id = ANY(COALESCE(v_affected_timesheet_ids, ARRAY[]::uuid[]))
      OR EXISTS (
        SELECT 1
        FROM pg_temp._bpay_delta_projection_rows AS projection_rows
        WHERE projection_rows.projection_run_id = p_projection_run_id
          AND projection_rows.session_id = p_session_id
          AND projection_rows.candidate_id = p_candidate_id
          AND projection_rows.timesheet_id IS NOT DISTINCT FROM preview_update.timesheet_id
          AND projection_rows.key_type IS NOT DISTINCT FROM preview_update.key_type
          AND projection_rows.key_value IS NOT DISTINCT FROM preview_update.key_value
      )
    )
    AND COALESCE(preview_update.row_json->>'projection_run_id', '') IS DISTINCT FROM p_projection_run_id::text
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_delta_projection_rows AS projection_rows
      WHERE projection_rows.projection_run_id = p_projection_run_id
        AND projection_rows.session_id = preview_update.session_id
        AND projection_rows.candidate_id = preview_update.candidate_id
        AND projection_rows.section = preview_update.section
        AND projection_rows.row_key = preview_update.row_key
    );

  GET DIAGNOSTICS v_preview_rows_superseded = ROW_COUNT;
  END IF;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'SUPERSEDE_SOURCE',
      write_cursor_json = jsonb_build_object('phase', 'SUPERSEDE_SOURCE', 'preview_rows_superseded', COALESCE(v_preview_rows_superseded, 0)),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
  IF v_elapsed_ms >= v_budget_cutoff_ms THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'write_budget_checkpoint_elapsed_ms', v_elapsed_ms,
            'write_budget_checkpoint_at_utc', v_now::text,
            'write_budget_checkpoint_phase', COALESCE(v_write_phase, 'UNKNOWN')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;
  END IF;

  END IF;

  IF v_write_phase_order <= 2 THEN
  IF COALESCE(v_sealed_targeted_delta, false) IS NOT TRUE THEN
  UPDATE public.banking_pay_workbench_candidate_source_lines AS source_update
  SET status = 'SUPERSEDED',
      source_row_json = jsonb_strip_nulls(
        COALESCE(source_update.source_row_json, '{}'::jsonb)
        || jsonb_build_object(
          'superseded_by_projection_run_id', p_projection_run_id::text,
          'superseded_by_source_change_seq', NULLIF(v_current_source_change_seq, 0),
          'superseded_by_session_version', v_current_session_version,
          'superseded_reason', 'DELTA_REPROJECTED_AFFECTED_SCOPE',
          'superseded_at_utc', v_now::text,
          'policy_x_boundary', 'PRE_DRAFT_WORKBENCH_ONLY_NO_ECONOMIC_CHANGE'
        )
      ),
      updated_at_utc = v_now
  WHERE source_update.session_id = p_session_id
    AND source_update.candidate_id = p_candidate_id
    AND (v_current_session_version IS NULL OR source_update.session_version IS NOT DISTINCT FROM v_current_session_version)
    AND UPPER(BTRIM(COALESCE(source_update.status, ''))) IN ('CURRENT', 'DIRTY')
    AND source_update.timesheet_id = ANY(COALESCE(v_affected_timesheet_ids, ARRAY[]::uuid[]))
    AND COALESCE(source_update.source_change_seq, 0) <= COALESCE(NULLIF(v_current_source_change_seq, 0), COALESCE(source_update.source_change_seq, 0))
    AND source_update.source_build_run_id IS DISTINCT FROM p_projection_run_id;

  GET DIAGNOSTICS v_source_rows_superseded = ROW_COUNT;
  END IF;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'SUPERSEDE_LINE_WORK',
      write_cursor_json = jsonb_build_object('phase', 'SUPERSEDE_LINE_WORK'),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
  IF v_elapsed_ms >= v_budget_cutoff_ms THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'write_budget_checkpoint_elapsed_ms', v_elapsed_ms,
            'write_budget_checkpoint_at_utc', v_now::text,
            'write_budget_checkpoint_phase', COALESCE(v_write_phase, 'UNKNOWN')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;
  END IF;

  END IF;

  IF v_write_phase_order <= 3 THEN
  IF COALESCE(v_sealed_targeted_delta, false) IS NOT TRUE THEN
  UPDATE public.banking_pay_workbench_candidate_line_work AS line_update
  SET status = 'SKIPPED',
      result_row_json = jsonb_strip_nulls(
        COALESCE(line_update.result_row_json, '{}'::jsonb)
        || jsonb_build_object(
          'superseded_by_projection_run_id', p_projection_run_id::text,
          'superseded_reason', 'DELTA_REPROJECTED_AFFECTED_SCOPE',
          'superseded_at_utc', v_now::text
        )
      ),
      updated_at_utc = v_now
  WHERE line_update.session_id = p_session_id
    AND line_update.candidate_id = p_candidate_id
    AND line_update.timesheet_id = ANY(COALESCE(v_affected_timesheet_ids, ARRAY[]::uuid[]))
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_delta_projection_rows AS projection_rows
      WHERE projection_rows.projection_run_id = p_projection_run_id
        AND projection_rows.session_id = line_update.session_id
        AND projection_rows.candidate_id = line_update.candidate_id
        AND projection_rows.timesheet_id IS NOT DISTINCT FROM line_update.timesheet_id
        AND projection_rows.line_key = line_update.line_key
    )
    AND COALESCE(line_update.work_payload_json->>'projection_run_id', line_update.result_row_json->>'projection_run_id', '') IS DISTINCT FROM p_projection_run_id::text
    AND UPPER(BTRIM(COALESCE(line_update.status, ''))) NOT IN ('SKIPPED');

  GET DIAGNOSTICS v_line_rows_superseded = ROW_COUNT;
  END IF;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'UPSERT_SOURCE',
      write_cursor_json = jsonb_build_object('phase', 'UPSERT_SOURCE'),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
  IF v_elapsed_ms >= v_budget_cutoff_ms THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'write_budget_checkpoint_elapsed_ms', v_elapsed_ms,
            'write_budget_checkpoint_at_utc', v_now::text,
            'write_budget_checkpoint_phase', COALESCE(v_write_phase, 'UNKNOWN')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;
  END IF;

  END IF;

  IF v_write_phase_order <= 4 THEN
  WITH upserted_source_rows AS (
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
      projection_rows.session_id,
      projection_rows.candidate_id,
      projection_rows.session_version,
      COALESCE(projection_rows.source_change_seq, 0),
      p_projection_run_id,
      projection_rows.source_ordinal,
      projection_rows.line_key,
      projection_rows.parent_line_key,
      projection_rows.split_suffix,
      projection_rows.timesheet_id,
      projection_rows.section,
      jsonb_strip_nulls(
        COALESCE(projection_rows.source_row_json, '{}'::jsonb)
        || jsonb_build_object(
          'source_function', 'pay_workbench_project_changed_timesheets_v1',
          'source_kind', 'VALID_PREVIEW_LINE',
          'projection_path', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id', p_projection_run_id::text,
          'source_ordinal', projection_rows.source_ordinal,
          'line_ordinal', projection_rows.source_ordinal,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      ),
      projection_rows.economic_key_json,
      projection_rows.contract_json,
      COALESCE(NULLIF(BTRIM(projection_rows.pay_channel_scope), ''), 'ALL'),
      COALESCE(NULLIF(BTRIM(projection_rows.refresh_scope_kind), ''), 'TARGETED_TIMESHEETS'),
      CASE WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN 'DIRTY' ELSE 'CURRENT' END,
      v_now,
      v_now
    FROM pg_temp._bpay_delta_projection_rows AS projection_rows
    WHERE projection_rows.projection_run_id = p_projection_run_id
      AND projection_rows.session_id = p_session_id
      AND projection_rows.candidate_id = p_candidate_id
      AND LOWER(BTRIM(COALESCE(
        projection_rows.preview_row_json->>'live_pay_eligibility_proven',
        projection_rows.preview_row_json#>>'{live_pay_eligibility,proven}',
        projection_rows.source_row_json->>'live_pay_eligibility_proven',
        projection_rows.source_row_json#>>'{live_pay_eligibility,proven}',
        projection_rows.contract_json->>'live_pay_eligibility_proven',
        projection_rows.contract_json#>>'{live_pay_eligibility,proven}',
        'false'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
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
    RETURNING public.banking_pay_workbench_candidate_source_lines.id
  )
  SELECT COUNT(*)::integer
  INTO v_source_rows_written
  FROM upserted_source_rows;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'UPSERT_LINE_WORK',
      write_cursor_json = jsonb_build_object('phase', 'UPSERT_LINE_WORK'),
      written_source_count = COALESCE(projection_run_update.written_source_count, 0) + COALESCE(v_source_rows_written, 0),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
  IF v_elapsed_ms >= v_budget_cutoff_ms THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'write_budget_checkpoint_elapsed_ms', v_elapsed_ms,
            'write_budget_checkpoint_at_utc', v_now::text,
            'write_budget_checkpoint_phase', COALESCE(v_write_phase, 'UNKNOWN')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;
  END IF;

  END IF;

  IF v_write_phase_order <= 5 THEN
  WITH upserted_line_rows AS (
    INSERT INTO public.banking_pay_workbench_candidate_line_work (
      session_id,
      candidate_id,
      timesheet_id,
      line_key,
      line_ordinal,
      status,
      work_payload_json,
      result_row_json,
      error_json,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      projection_rows.session_id,
      projection_rows.candidate_id,
      projection_rows.timesheet_id,
      projection_rows.line_key,
      projection_rows.row_ordinal,
      CASE
        WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN 'PENDING'
        WHEN projection_rows.contract_ok IS TRUE
         AND projection_rows.materialisable IS TRUE
         AND LOWER(BTRIM(COALESCE(
           projection_rows.preview_row_json->>'live_pay_eligibility_proven',
           projection_rows.preview_row_json#>>'{live_pay_eligibility,proven}',
           projection_rows.source_row_json->>'live_pay_eligibility_proven',
           projection_rows.source_row_json#>>'{live_pay_eligibility,proven}',
           projection_rows.contract_json->>'live_pay_eligibility_proven',
           projection_rows.contract_json#>>'{live_pay_eligibility,proven}',
           'false'
         ))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'MATERIALISED'
        WHEN projection_rows.contract_ok IS TRUE THEN 'SKIPPED'
        ELSE 'ERROR'
      END,
      jsonb_strip_nulls(
        COALESCE(projection_rows.work_payload_json, '{}'::jsonb)
        || jsonb_build_object(
          'projection_path', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id', p_projection_run_id::text,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'targeted_delta_stage', CASE
            WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN jsonb_build_object(
              'projection_run_id', p_projection_run_id::text,
              'final_status', CASE
                WHEN projection_rows.contract_ok IS TRUE
                 AND projection_rows.materialisable IS TRUE
                 AND LOWER(BTRIM(COALESCE(
                   projection_rows.preview_row_json->>'live_pay_eligibility_proven',
                   projection_rows.preview_row_json#>>'{live_pay_eligibility,proven}',
                   projection_rows.source_row_json->>'live_pay_eligibility_proven',
                   projection_rows.source_row_json#>>'{live_pay_eligibility,proven}',
                   projection_rows.contract_json->>'live_pay_eligibility_proven',
                   projection_rows.contract_json#>>'{live_pay_eligibility,proven}',
                   'false'
                 ))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'MATERIALISED'
                WHEN projection_rows.contract_ok IS TRUE THEN 'SKIPPED'
                ELSE 'ERROR'
              END
            )
            ELSE NULL::jsonb
          END
        )
      ),
      jsonb_strip_nulls(
        (COALESCE(projection_rows.result_row_json, '{}'::jsonb) - 'preview_contract')
        || jsonb_build_object(
          'projection_path', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'projection_run_id', p_projection_run_id::text,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'live_pay_eligibility_proven', LOWER(BTRIM(COALESCE(
            projection_rows.preview_row_json->>'live_pay_eligibility_proven',
            projection_rows.preview_row_json#>>'{live_pay_eligibility,proven}',
            projection_rows.source_row_json->>'live_pay_eligibility_proven',
            projection_rows.source_row_json#>>'{live_pay_eligibility,proven}',
            projection_rows.contract_json->>'live_pay_eligibility_proven',
            projection_rows.contract_json#>>'{live_pay_eligibility,proven}',
            'false'
          ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'economic_key', projection_rows.economic_key_json,
          'preview_contract', projection_rows.result_row_json->'preview_contract'
        )
      ),
      CASE
        WHEN projection_rows.contract_ok IS TRUE THEN NULL::jsonb
        ELSE jsonb_build_object(
          'code', 'DELTA_CONTRACT_NOT_OK',
          'projection_run_id', p_projection_run_id::text
        )
      END,
      v_now,
      v_now
    FROM pg_temp._bpay_delta_projection_rows AS projection_rows
    WHERE projection_rows.projection_run_id = p_projection_run_id
      AND projection_rows.session_id = p_session_id
      AND projection_rows.candidate_id = p_candidate_id
    ON CONFLICT (session_id, candidate_id, (COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid)), line_key)
    DO UPDATE
    SET line_ordinal = EXCLUDED.line_ordinal,
        status = EXCLUDED.status,
        work_payload_json = EXCLUDED.work_payload_json,
        result_row_json = EXCLUDED.result_row_json,
        error_json = EXCLUDED.error_json,
        updated_at_utc = v_now
    RETURNING public.banking_pay_workbench_candidate_line_work.id
  )
  SELECT COUNT(*)::integer
  INTO v_line_rows_written
  FROM upserted_line_rows;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'UPSERT_PREVIEW',
      write_cursor_json = jsonb_build_object('phase', 'UPSERT_PREVIEW'),
      written_line_work_count = COALESCE(projection_run_update.written_line_work_count, 0) + COALESCE(v_line_rows_written, 0),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  v_elapsed_ms := FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::integer;
  IF v_elapsed_ms >= v_budget_cutoff_ms THEN
    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET diagnostics_json = COALESCE(projection_run_update.diagnostics_json, '{}'::jsonb)
          || jsonb_build_object(
            'write_budget_checkpoint_elapsed_ms', v_elapsed_ms,
            'write_budget_checkpoint_at_utc', v_now::text,
            'write_budget_checkpoint_phase', COALESCE(v_write_phase, 'UNKNOWN')
          ),
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;
  END IF;

  END IF;

  IF v_write_phase_order <= 6 THEN
  DROP TABLE IF EXISTS pg_temp._bpay_delta_preview_upsert_rows;
  CREATE TEMP TABLE _bpay_delta_preview_upsert_rows ON COMMIT DROP AS
  SELECT
    projection_rows.*,
    existing_preview.id AS existing_preview_row_id,
    COALESCE(existing_preview.selected, false) AS existing_preview_selected,
    UPPER(BTRIM(COALESCE(existing_preview.selection_state, ''))) AS existing_preview_selection_state,
    CASE
      WHEN existing_preview.id IS NULL THEN false
      WHEN existing_preview.selected IS TRUE
       AND existing_preview.selection_state = 'SELECTED'
       AND existing_preview.row_key = projection_rows.row_key
       AND existing_preview.timesheet_id IS NOT DISTINCT FROM projection_rows.timesheet_id
       AND existing_preview.key_type IS NOT DISTINCT FROM projection_rows.key_type
       AND existing_preview.key_value IS NOT DISTINCT FROM projection_rows.key_value
       AND ROUND(COALESCE(CASE WHEN COALESCE(existing_preview.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (existing_preview.row_json->>'amount_ex_vat')::numeric ELSE NULL::numeric END, 0), 2) = ROUND(COALESCE(projection_rows.amount_ex_vat, 0), 2)
       AND ROUND(COALESCE(CASE WHEN COALESCE(existing_preview.row_json->>'amount_inc_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (existing_preview.row_json->>'amount_inc_vat')::numeric ELSE NULL::numeric END, 0), 2) = ROUND(COALESCE(projection_rows.amount_inc_vat, 0), 2)
       AND LOWER(BTRIM(COALESCE(existing_preview.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = projection_rows.draftable
       AND LOWER(BTRIM(COALESCE(existing_preview.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = projection_rows.is_ready_for_draft
       AND LOWER(BTRIM(COALESCE(existing_preview.row_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = projection_rows.selection_allowed
       AND NULLIF(BTRIM(COALESCE(existing_preview.row_json->>'readiness_state', '')), '') IS NOT DISTINCT FROM NULLIF(BTRIM(COALESCE(projection_rows.readiness_state, '')), '')
       AND COALESCE(existing_preview.row_json->'outstanding_state_json', '{}'::jsonb) = COALESCE(projection_rows.outstanding_state_json, '{}'::jsonb)
      THEN true
      ELSE false
    END AS preserve_selected
  FROM pg_temp._bpay_delta_projection_rows AS projection_rows
  LEFT JOIN pg_temp._bpay_delta_existing_preview_rows AS existing_preview
    ON existing_preview.session_id = projection_rows.session_id
   AND existing_preview.candidate_id = projection_rows.candidate_id
   AND existing_preview.section = projection_rows.section
   AND existing_preview.row_key = projection_rows.row_key
  WHERE projection_rows.projection_run_id = p_projection_run_id
    AND projection_rows.session_id = p_session_id
    AND projection_rows.candidate_id = p_candidate_id;

  ALTER TABLE pg_temp._bpay_delta_preview_upsert_rows
    ADD COLUMN can_publish_ready boolean NOT NULL DEFAULT false,
    ADD COLUMN can_serve_selected boolean NOT NULL DEFAULT false,
    ADD COLUMN effective_selected boolean NOT NULL DEFAULT false,
    ADD COLUMN effective_selection_state text NOT NULL DEFAULT 'NOT_SELECTABLE';

  UPDATE pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_update
  SET can_publish_ready = (
        preview_upsert_update.contract_ok IS TRUE
        AND preview_upsert_update.materialisable IS TRUE
        AND preview_upsert_update.timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(preview_upsert_update.key_type, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(preview_upsert_update.key_value, '')), '') IS NOT NULL
        AND COALESCE(preview_upsert_update.contract_json->>'policy_x_authority_scope', preview_upsert_update.preview_row_json->>'policy_x_authority_scope', '') = 'PRE_DRAFT_LIVE_TRUTH'
        AND LOWER(BTRIM(COALESCE(
          preview_upsert_update.preview_row_json->>'live_pay_eligibility_proven',
          preview_upsert_update.preview_row_json#>>'{live_pay_eligibility,proven}',
          preview_upsert_update.source_row_json->>'live_pay_eligibility_proven',
          preview_upsert_update.source_row_json#>>'{live_pay_eligibility,proven}',
          preview_upsert_update.contract_json->>'live_pay_eligibility_proven',
          preview_upsert_update.contract_json#>>'{live_pay_eligibility,proven}',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(preview_upsert_update.preview_row_json->>'projection_certified', preview_upsert_update.contract_json->>'projection_certified', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          LOWER(BTRIM(COALESCE(preview_upsert_update.preview_row_json->>'row_key_line_key_parity_proven', preview_upsert_update.contract_json->>'row_key_line_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR COALESCE(v_shadow_compare_passed, false) IS TRUE
        )
      ),
      can_serve_selected = (
        preview_upsert_update.contract_ok IS TRUE
        AND preview_upsert_update.materialisable IS TRUE
        AND preview_upsert_update.timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(preview_upsert_update.key_type, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(preview_upsert_update.key_value, '')), '') IS NOT NULL
        AND COALESCE(preview_upsert_update.contract_json->>'policy_x_authority_scope', preview_upsert_update.preview_row_json->>'policy_x_authority_scope', '') = 'PRE_DRAFT_LIVE_TRUTH'
        AND LOWER(BTRIM(COALESCE(
          preview_upsert_update.preview_row_json->>'live_pay_eligibility_proven',
          preview_upsert_update.preview_row_json#>>'{live_pay_eligibility,proven}',
          preview_upsert_update.source_row_json->>'live_pay_eligibility_proven',
          preview_upsert_update.source_row_json#>>'{live_pay_eligibility,proven}',
          preview_upsert_update.contract_json->>'live_pay_eligibility_proven',
          preview_upsert_update.contract_json#>>'{live_pay_eligibility,proven}',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(preview_upsert_update.preview_row_json->>'projection_certified', preview_upsert_update.contract_json->>'projection_certified', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          LOWER(BTRIM(COALESCE(preview_upsert_update.preview_row_json->>'row_key_line_key_parity_proven', preview_upsert_update.contract_json->>'row_key_line_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR COALESCE(v_shadow_compare_passed, false) IS TRUE
        )
        AND LOWER(BTRIM(COALESCE(preview_upsert_update.preview_row_json->>'target_selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND COALESCE(NULLIF(BTRIM(preview_upsert_update.preview_row_json->>'target_selection_state'), ''), 'NOT_SELECTABLE') = 'SELECTED'
        AND preview_upsert_update.draftable IS TRUE
        AND preview_upsert_update.is_ready_for_draft IS TRUE
        AND preview_upsert_update.selection_allowed IS TRUE
        AND LOWER(BTRIM(COALESCE(preview_upsert_update.outstanding_state_json->>'reservation_overrun_detected', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      )
  WHERE preview_upsert_update.projection_run_id = p_projection_run_id
    AND preview_upsert_update.session_id = p_session_id
    AND preview_upsert_update.candidate_id = p_candidate_id;

  UPDATE pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_update
  SET effective_selected = CASE
        WHEN preview_upsert_update.can_serve_selected IS NOT TRUE THEN false
        WHEN preview_upsert_update.existing_preview_row_id IS NOT NULL
         AND preview_upsert_update.existing_preview_selection_state IN ('SELECTED', 'UNSELECTED')
          THEN COALESCE(preview_upsert_update.existing_preview_selected, false)
        WHEN COALESCE(v_selection_intent_mode, '') = 'EXPLICIT_INCLUDE' THEN EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(COALESCE(v_explicit_selected_preview_row_ids, '[]'::jsonb)) AS explicit_id(value)
          WHERE BTRIM(explicit_id.value) = preview_upsert_update.existing_preview_row_id::text
        )
        ELSE true
      END
  WHERE preview_upsert_update.projection_run_id = p_projection_run_id
    AND preview_upsert_update.session_id = p_session_id
    AND preview_upsert_update.candidate_id = p_candidate_id;

  UPDATE pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_update
  SET effective_selection_state = CASE
        WHEN preview_upsert_update.can_serve_selected IS NOT TRUE THEN 'NOT_SELECTABLE'
        WHEN preview_upsert_update.effective_selected IS TRUE THEN 'SELECTED'
        ELSE 'UNSELECTED'
      END
  WHERE preview_upsert_update.projection_run_id = p_projection_run_id
    AND preview_upsert_update.session_id = p_session_id
    AND preview_upsert_update.candidate_id = p_candidate_id;

  SELECT COUNT(*) FILTER (WHERE preview_upsert_rows.existing_preview_row_id IS NOT NULL AND preview_upsert_rows.effective_selected IS TRUE)::integer,
         COUNT(*) FILTER (WHERE preview_upsert_rows.existing_preview_row_id IS NOT NULL AND preview_upsert_rows.effective_selected IS NOT TRUE)::integer
  INTO v_selected_preserved_count,
       v_selected_cleared_count
  FROM pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_rows;

  SELECT COUNT(*)::integer
  INTO v_selection_guard_violation_count
  FROM pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_rows
  WHERE (
      preview_upsert_rows.can_serve_selected IS TRUE
      AND preview_upsert_rows.can_publish_ready IS NOT TRUE
    )
    OR (
      COALESCE(v_sealed_targeted_delta,false) IS TRUE
      AND preview_upsert_rows.can_publish_ready IS NOT TRUE
    );

  IF COALESCE(v_selection_guard_violation_count, 0) > 0 THEN
    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row_update
    SET status = 'SUPERSEDED',
        source_row_json = jsonb_strip_nulls(
          COALESCE(source_row_update.source_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'delta_write_selection_guard_failed', true,
            'delta_fallback_reason', 'DELTA_SELECTED_PREVIEW_ROW_SAFETY_GUARD_FAILED',
            'delta_fallback_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE source_row_update.session_id = p_session_id
      AND source_row_update.candidate_id = p_candidate_id
      AND source_row_update.source_build_run_id = p_projection_run_id
      AND source_row_update.status IN ('CURRENT', 'DIRTY');

    UPDATE public.banking_pay_workbench_candidate_line_work AS line_work_update
    SET status = 'SKIPPED',
        result_row_json = jsonb_strip_nulls(
          COALESCE(line_work_update.result_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'delta_write_selection_guard_failed', true,
            'delta_fallback_reason', 'DELTA_SELECTED_PREVIEW_ROW_SAFETY_GUARD_FAILED',
            'delta_fallback_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE line_work_update.session_id = p_session_id
      AND line_work_update.candidate_id = p_candidate_id
      AND (
        COALESCE(line_work_update.work_payload_json->>'projection_run_id', '') = p_projection_run_id::text
        OR COALESCE(line_work_update.result_row_json->>'projection_run_id', '') = p_projection_run_id::text
      )
      AND UPPER(BTRIM(COALESCE(line_work_update.status, ''))) NOT IN ('ERROR', 'FAILED', 'SKIPPED');

    UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
    SET write_phase = 'WRITE_FALLBACK_REQUIRED',
        fallback_required = true,
        fallback_reason = CASE
          WHEN COALESCE(v_sealed_targeted_delta,false) IS TRUE THEN 'TARGETED_DELTA_OUTPUT_NOT_READY'
          ELSE 'DELTA_SELECTED_PREVIEW_ROW_SAFETY_GUARD_FAILED'
        END,
        updated_at_utc = v_now
    WHERE projection_run_update.id = p_projection_run_id
      AND projection_run_update.session_id = p_session_id
      AND projection_run_update.candidate_id = p_candidate_id;

    RETURN jsonb_build_object(
      'ok', false,
      'fallback_required', true,
      'fallback_reason', CASE
        WHEN COALESCE(v_sealed_targeted_delta,false) IS TRUE THEN 'TARGETED_DELTA_OUTPUT_NOT_READY'
        ELSE 'DELTA_SELECTED_PREVIEW_ROW_SAFETY_GUARD_FAILED'
      END,
      'selection_guard_violation_count', COALESCE(v_selection_guard_violation_count, 0),
      'source_rows_written', 0,
      'line_rows_written', 0,
      'preview_rows_written', 0,
      'rows_superseded', 0
    );
  END IF;

  WITH upserted_preview_rows AS (
    INSERT INTO public.banking_pay_workbench_preview_rows (
      session_id,
      candidate_id,
      section,
      row_key,
      row_ordinal,
      row_json,
      timesheet_id,
      key_type,
      key_value,
      selected,
      selection_state,
      status,
      session_version,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      preview_upsert_rows.session_id,
      preview_upsert_rows.candidate_id,
      preview_upsert_rows.section,
      preview_upsert_rows.row_key,
      preview_upsert_rows.row_ordinal,
      jsonb_strip_nulls(
        (COALESCE(preview_upsert_rows.preview_row_json, '{}'::jsonb) - 'preview_contract')
        || jsonb_build_object(
          'projection_path', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'preview_contract', preview_upsert_rows.result_row_json->'preview_contract',
          'projection_run_id', p_projection_run_id::text,
          'projection_certified', preview_upsert_rows.can_publish_ready,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'selected', COALESCE(preview_upsert_rows.effective_selected, false),
          'selection_state', COALESCE(NULLIF(BTRIM(preview_upsert_rows.effective_selection_state), ''), 'NOT_SELECTABLE'),
          'outstanding_state_json', COALESCE(preview_upsert_rows.outstanding_state_json, '{}'::jsonb),
          'target_selected', LOWER(BTRIM(COALESCE(preview_upsert_rows.preview_row_json->>'target_selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'target_selection_state', COALESCE(NULLIF(BTRIM(preview_upsert_rows.preview_row_json->>'target_selection_state'), ''), CASE WHEN preview_upsert_rows.can_serve_selected IS TRUE THEN 'SELECTED' ELSE 'NOT_SELECTABLE' END),
          'row_key_line_key_parity_proven', LOWER(BTRIM(COALESCE(preview_upsert_rows.preview_row_json->>'row_key_line_key_parity_proven', preview_upsert_rows.contract_json->>'row_key_line_key_parity_proven', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'shadow_compare_status', CASE WHEN COALESCE(v_shadow_compare_status, '') = '' THEN NULL ELSE v_shadow_compare_status END,
          'targeted_delta_stage', CASE
            WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN jsonb_build_object(
              'projection_run_id', p_projection_run_id::text,
              'final_selected', COALESCE(preview_upsert_rows.effective_selected, false),
              'final_selection_state', COALESCE(NULLIF(BTRIM(preview_upsert_rows.effective_selection_state), ''), 'NOT_SELECTABLE'),
              'final_status', CASE WHEN preview_upsert_rows.can_publish_ready IS TRUE THEN 'READY' ELSE 'DELTA_PENDING' END
            )
            ELSE NULL::jsonb
          END
        )
      ),
      preview_upsert_rows.timesheet_id,
      preview_upsert_rows.key_type,
      preview_upsert_rows.key_value,
      CASE WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN false ELSE COALESCE(preview_upsert_rows.effective_selected, false) END,
      CASE WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN 'NOT_SELECTABLE' ELSE COALESCE(NULLIF(BTRIM(preview_upsert_rows.effective_selection_state), ''), 'NOT_SELECTABLE') END,
      CASE WHEN COALESCE(v_sealed_targeted_delta, false) IS TRUE THEN 'DELTA_PENDING' WHEN preview_upsert_rows.can_publish_ready IS TRUE THEN 'READY' ELSE 'DELTA_PENDING' END,
      preview_upsert_rows.session_version,
      v_now,
      v_now
    FROM pg_temp._bpay_delta_preview_upsert_rows AS preview_upsert_rows
    WHERE preview_upsert_rows.materialisable IS TRUE
      AND preview_upsert_rows.contract_ok IS TRUE
    ON CONFLICT (session_id, section, candidate_id, row_key)
    DO UPDATE
    SET row_ordinal = EXCLUDED.row_ordinal,
        row_json = EXCLUDED.row_json,
        timesheet_id = EXCLUDED.timesheet_id,
        key_type = EXCLUDED.key_type,
        key_value = EXCLUDED.key_value,
        selected = COALESCE(EXCLUDED.selected, false),
        selection_state = COALESCE(NULLIF(BTRIM(EXCLUDED.selection_state), ''), 'NOT_SELECTABLE'),
        status = EXCLUDED.status,
        session_version = EXCLUDED.session_version,
        updated_at_utc = v_now
    RETURNING public.banking_pay_workbench_preview_rows.id
  )
  SELECT COUNT(*)::integer
  INTO v_preview_rows_written
  FROM upserted_preview_rows;

  IF COALESCE(v_preview_rows_written, 0) > 0
     AND COALESCE(v_sealed_targeted_delta, false) IS NOT TRUE THEN
    PERFORM public.pay_workbench_session_recompute_progress_counters(
      p_session_id,
      true,
      'DELTA_WRITE_SELECTION_RECONCILE',
      false
    );

    SELECT COALESCE(jsonb_agg(to_jsonb(current_selected_preview_row.id::text) ORDER BY current_selected_preview_row.row_ordinal, current_selected_preview_row.id), '[]'::jsonb),
           COUNT(*)::integer
    INTO v_reconciled_selected_preview_row_ids,
         v_reconciled_selected_row_count
    FROM public.banking_pay_workbench_preview_rows AS current_selected_preview_row
    WHERE current_selected_preview_row.session_id = p_session_id
      AND current_selected_preview_row.session_version = COALESCE(v_current_session_version, 1)
      AND lower(COALESCE(NULLIF(BTRIM(current_selected_preview_row.section), ''), 'canonical_preview_lines')) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(current_selected_preview_row.status, ''))) = 'READY'
      AND COALESCE(current_selected_preview_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(current_selected_preview_row.selection_state, ''))) = 'SELECTED';

    UPDATE public.banking_pay_workbench_sessions AS selection_summary_session
    SET selected_row_count = COALESCE(v_reconciled_selected_row_count, 0),
        server_selected_preview_row_ids = CASE
          WHEN COALESCE(v_selection_intent_mode, '') = 'EXPLICIT_INCLUDE'
            OR COALESCE(selection_summary_session.server_selected_preview_row_ids_provided, false) IS TRUE
            THEN COALESCE(v_reconciled_selected_preview_row_ids, '[]'::jsonb)
          ELSE selection_summary_session.server_selected_preview_row_ids
        END,
        server_selected_preview_row_ids_provided = CASE
          WHEN COALESCE(v_selection_intent_mode, '') = 'EXPLICIT_INCLUDE' THEN true
          ELSE COALESCE(selection_summary_session.server_selected_preview_row_ids_provided, false)
        END,
        updated_at_utc = v_now
    WHERE selection_summary_session.id = p_session_id;
  END IF;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'WRITE_COUNTS',
      write_cursor_json = jsonb_build_object('phase', 'WRITE_COUNTS'),
      written_preview_count = COALESCE(projection_run_update.written_preview_count, 0) + COALESCE(v_preview_rows_written, 0),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run_update
  SET write_phase = 'WRITE_COMPLETE',
      write_cursor_json = jsonb_build_object('phase', 'WRITE_COMPLETE'),
      updated_at_utc = v_now
  WHERE projection_run_update.id = p_projection_run_id
    AND projection_run_update.session_id = p_session_id
    AND projection_run_update.candidate_id = p_candidate_id;

  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'fallback_required', false,
    'source_rows_written', COALESCE(v_source_rows_written, 0),
    'line_rows_written', COALESCE(v_line_rows_written, 0),
    'preview_rows_written', COALESCE(v_preview_rows_written, 0),
    'rows_superseded', COALESCE(v_preview_rows_superseded, 0) + COALESCE(v_source_rows_superseded, 0) + COALESCE(v_line_rows_superseded, 0),
    'preview_rows_superseded', COALESCE(v_preview_rows_superseded, 0),
    'source_rows_superseded', COALESCE(v_source_rows_superseded, 0),
    'line_rows_superseded', COALESCE(v_line_rows_superseded, 0),
    'selected_preserved_count', COALESCE(v_selected_preserved_count, 0),
    'selected_cleared_count', COALESCE(v_selected_cleared_count, 0),
    'affected_timesheet_count', COALESCE(array_length(v_affected_timesheet_ids, 1), 0),
    'affected_economic_key_count', COALESCE(v_affected_key_count, 0),
    'live_pay_eligibility_publication_guard', true,
    'write_phase', 'WRITE_COMPLETE',
    'write_cursor_json', jsonb_build_object('phase', 'WRITE_COMPLETE')
  );
END;
$function$;
-- Reasserted after the final removal of the legacy monolith's prepare drop.
