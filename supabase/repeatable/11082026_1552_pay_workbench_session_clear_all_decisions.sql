-- Clear user decisions without invalidating every physically current candidate.
-- Existing case/override economics are recomputed only for candidates whose
-- saved decisions were actually removed. Selection returns to IMPLICIT_ALL so
-- all current and subsequently rebuilt eligible Ready-to-Pay rows default on.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_clear_all_decisions(
  p_session_id uuid,
  p_actor_user_id uuid
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_affected_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid := NULL::uuid;
  v_deleted_case_resolution_count integer := 0;
  v_deleted_override_count integer := 0;
  v_previous_selected_count integer := 0;
  v_job_json jsonb := '{}'::jsonb;
  v_job_id uuid := NULL::uuid;
  v_job_ids jsonb := '[]'::jsonb;
  v_selection_json jsonb := '{}'::jsonb;
  v_selected_ids jsonb := '[]'::jsonb;
  v_selected_count integer := 0;
  v_progress_counter_version bigint := 0;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  PERFORM 1 FROM public.tms_users AS actor_row WHERE actor_row.id = p_actor_user_id;
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
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not OPEN', p_session_id;
  END IF;

  v_previous_selected_count := COALESCE(v_session_row.selected_row_count, 0);

  SELECT COALESCE(pg_catalog.array_agg(DISTINCT affected.candidate_id ORDER BY affected.candidate_id), ARRAY[]::uuid[])
  INTO v_affected_candidate_ids
  FROM (
    SELECT case_row.candidate_id
    FROM public.banking_pay_workbench_session_case_resolutions AS case_row
    WHERE case_row.session_id = p_session_id
      AND case_row.candidate_id IS NOT NULL
    UNION
    SELECT override_row.candidate_id
    FROM public.banking_pay_workbench_session_overrides AS override_row
    WHERE override_row.session_id = p_session_id
      AND override_row.candidate_id IS NOT NULL
  ) AS affected;

  DELETE FROM public.banking_pay_workbench_session_case_resolutions AS case_delete
  WHERE case_delete.session_id = p_session_id;
  GET DIAGNOSTICS v_deleted_case_resolution_count = ROW_COUNT;

  DELETE FROM public.banking_pay_workbench_session_overrides AS override_delete
  WHERE override_delete.session_id = p_session_id;
  GET DIAGNOSTICS v_deleted_override_count = ROW_COUNT;

  -- The canonical selection owner validates every current row, enforces
  -- same-candidate recovery headroom and advances only the selection revision.
  v_selection_json := public.pay_workbench_session_set_selected_rows(
    p_session_id,
    pg_catalog.jsonb_build_object(
      'expected_session_version', COALESCE(v_session_row.version, 0),
      'expected_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
      'section', 'canonical_preview_lines',
      'global_selection_action', 'SELECT_ALL_SECTION',
      'selection_intent_mode', 'IMPLICIT_ALL',
      'clear_all_decisions', true
    ),
    p_actor_user_id
  );

  v_selected_ids := CASE
    WHEN pg_catalog.jsonb_typeof(v_selection_json->'server_selected_preview_row_ids') = 'array'
      THEN v_selection_json->'server_selected_preview_row_ids'
    ELSE '[]'::jsonb
  END;
  v_selected_count := COALESCE((v_selection_json->>'selected_row_count')::integer, pg_catalog.jsonb_array_length(v_selected_ids), 0);
  v_progress_counter_version := COALESCE((v_selection_json->>'progress_counter_version')::bigint, v_session_row.progress_counter_version, 0);

  -- Only candidates whose saved economic decision was removed become pending.
  IF pg_catalog.cardinality(v_affected_candidate_ids) > 0 THEN
    UPDATE public.banking_pay_workbench_session_candidate_state AS candidate_state
    SET status = 'PENDING',
        effective_candidate_fragment_json = '{}'::jsonb,
        effective_summary_fragment_json = '{}'::jsonb,
        effective_paye_candidate_json = NULL,
        effective_non_paye_payee_json = NULL,
        effective_payees_json = '[]'::jsonb,
        effective_case_resolution_states_json = '[]'::jsonb,
        effective_canonical_preview_lines_json = '[]'::jsonb,
        pending_job_id = NULL::uuid,
        updated_at_utc = v_now,
        last_recomputed_at_utc = NULL,
        last_error_json = NULL
    WHERE candidate_state.session_id = p_session_id
      AND candidate_state.candidate_id = ANY(v_affected_candidate_ids);

    FOREACH v_candidate_id IN ARRAY v_affected_candidate_ids LOOP
      v_job_json := public.pay_workbench_enqueue_session_candidate_refresh(
        p_session_id => p_session_id,
        p_candidate_id => v_candidate_id,
        p_reason => 'SESSION_DECISIONS_CLEARED',
        p_actor_user_id => p_actor_user_id,
        p_payload_json => pg_catalog.jsonb_build_object(
          'clear_all_decisions', true,
          'decision_changed_candidate_only', true,
          'default_select_eligible_rows', true,
          'force_refresh', true,
          'user_requested_refresh', false,
          'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'
        )
      );
      IF COALESCE(v_job_json->>'job_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_job_id := (v_job_json->>'job_id')::uuid;
        v_job_ids := v_job_ids || pg_catalog.jsonb_build_array(v_job_id::text);
      END IF;
    END LOOP;
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',
    p_session_id::text,
    'WORKBENCH_SESSION_DECISIONS_CLEARED',
    pg_catalog.jsonb_build_object(
      'selected_row_count', v_previous_selected_count,
      'case_resolution_count', v_deleted_case_resolution_count,
      'override_count', v_deleted_override_count
    ),
    pg_catalog.jsonb_build_object(
      'selected_row_count', v_selected_count,
      'selection_intent_mode', 'IMPLICIT_ALL',
      'affected_candidate_ids', pg_catalog.to_jsonb(v_affected_candidate_ids),
      'job_ids', v_job_ids,
      'session_version_unchanged', true
    ),
    'SESSION_DECISIONS_CLEARED',
    p_actor_user_id
  );

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'status', v_session_row.status,
    'session_version', v_session_row.version,
    'progress_counter_version', v_progress_counter_version,
    'server_selected_preview_row_ids', v_selected_ids,
    'server_selected_preview_row_ids_provided', true,
    'selected_row_count', v_selected_count,
    'selected_preview_row_mode', 'IMPLICIT_ALL',
    'selection_intent_mode', 'IMPLICIT_ALL',
    'cleared_case_resolution_count', v_deleted_case_resolution_count,
    'cleared_override_count', v_deleted_override_count,
    'cleared_selected_preview_row_count', v_previous_selected_count,
    'affected_candidate_count', pg_catalog.cardinality(v_affected_candidate_ids),
    'requeue_candidate_count', pg_catalog.cardinality(v_affected_candidate_ids),
    'requeue_candidate_ids', pg_catalog.to_jsonb(v_affected_candidate_ids),
    'requeue_job_count', pg_catalog.jsonb_array_length(v_job_ids),
    'requeue_job_ids', v_job_ids,
    'physically_current_candidate_count', pg_catalog.cardinality(COALESCE(v_session_row.scope_candidate_ids, ARRAY[]::uuid[])) - pg_catalog.cardinality(v_affected_candidate_ids),
    'no_change_candidate_rebuild_count', 0,
    'state_changed', true
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_clear_all_decisions(uuid, uuid) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_session_clear_all_decisions(uuid, uuid) SET plpgsql_check.mode TO 'disabled';
REVOKE ALL ON FUNCTION public.pay_workbench_session_clear_all_decisions(uuid, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_clear_all_decisions(uuid, uuid) TO service_role;
