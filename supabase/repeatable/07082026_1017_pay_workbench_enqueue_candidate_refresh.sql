-- Banking Pay bounded-scope Version 1.2.5
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb)
-- Prior installed pg_get_functiondef MD5: 13e76063b68e27483e09c637379b04ac
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_session_id uuid := NULL::uuid;
  v_session_id_text text := NULL::text;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_actor_user_id uuid := p_actor_user_id;
  v_live_change_seq bigint := 0;
  v_payload_source_change_seq bigint := NULL::bigint;
  v_source_change_seq bigint := 0;
  v_job_type text := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_pay_channel_scope text := 'ALL';
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_queue_identity_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_queue_identity_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_source_build_run_id_text text := NULL::text;
  v_source_build_run_id uuid := NULL::uuid;
  v_source_build_hash text := NULL::text;
  v_source_build_seed_text text := NULL::text;
  v_initial_cursor_json jsonb := '{}'::jsonb;
  v_cursor_token text := 'none';
  v_session_signature_token text := 'none';
  v_stage_limit integer := 100;
  v_dedupe_key text;
  v_job_id uuid;
  v_job_status text;
  v_job_was_inserted boolean := false;
  v_insert_row_count integer := 0;
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'WORKBENCH_CANDIDATE_SOURCE_BUILD');
  v_payload_out_json jsonb := '{}'::jsonb;
  v_classifier_result jsonb := '{}'::jsonb;
  v_force_legacy boolean := false;
  v_force_broad_legacy boolean := false;
  v_resolved_job_type text := NULL::text;
  v_projection_run_id uuid := NULL::uuid;
  v_projection_run_id_text text := NULL::text;
  v_projection_mode text := 'LEGACY';
  v_projection_class text := 'UNKNOWN';
  v_resolved_mode text := 'LEGACY';
  v_no_job_reason text := NULL::text;
  v_delta_jobs_superseded integer := 0;
  v_delta_ids_hash text := NULL::text;
  v_delta_coalescing_key text := NULL::text;
  v_delta_coalescing_hash text := NULL::text;
  v_delta_active_running_job_id uuid := NULL::uuid;
  v_existing_delta_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_existing_delta_source_change_seq bigint := 0;
  v_existing_delta_event_count integer := 0;
  v_merged_delta_event_count integer := 1;
  v_existing_delta_projection_run_id_text text := NULL::text;
  v_delta_merge_reused_existing boolean := false;
  v_payload_shadow_compare_required boolean := false;
  v_payload_shadow_compare_enforced boolean := false;
  v_rotation_scope_json jsonb := '{}'::jsonb;
  v_early_preflight_result jsonb := '{}'::jsonb;
  v_early_preflight_action text := 'PROCEED';
  v_early_preflight_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_early_preflight_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_shadow_compare_required boolean := false;
  v_shadow_compare_enforced boolean := false;
  v_bounded_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_scope_change_tx_token uuid := NULL::uuid;
  v_scope_invalidation_result jsonb := '{}'::jsonb;
  v_scope_state_precedes_job boolean := false;
  v_payload_scope_change_generation bigint := NULL::bigint;
  v_finalized_scope_tx_state text := NULL::text;
  v_finalized_scope_tx_generation bigint := NULL::bigint;
  v_live_scope_change_generation bigint := 0;
  v_registry_dirty_generation bigint := NULL::bigint;
  v_registry_source_change_seq_before bigint := 0;
  v_registry_source_change_seq_after bigint := 0;
  v_registry_sequence_synchronised boolean := false;
  v_scope_state_generation_match_count integer := 0;
  v_stale_preinvalidated_absorb_only boolean := false;
  v_requested_source_build_run_id uuid := NULL::uuid;
  v_authority_fingerprint_text text := NULL::text;
  v_authority_fingerprint text := NULL::text;
  v_owner_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_owner_root_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_owner_active_job_id uuid := NULL::uuid;
  v_owner_refresh_scope_kind text := NULL::text;
  v_owner_pay_channel_scope text := NULL::text;
  v_owner_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_owner_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_owner_covers_request boolean := false;
  v_owner_resolution text := 'NO_CURRENT_OWNER';
  v_owner_reasons_json jsonb := '[]'::jsonb;
  v_owner_trigger_sources_json jsonb := '[]'::jsonb;
  v_owner_provenance_json jsonb := '{}'::jsonb;
  v_owner_request_count bigint := 0;
  v_owner_scope_status text := NULL::text;
  v_reversion_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_reversion_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_reversion_attestation jsonb := '{}'::jsonb;
  v_reversion_source_count integer := 0;
  v_reversion_state_exact boolean := false;
  v_semantic_ready_publication_enabled boolean := false;
  v_source_publication_identity_enforced boolean := false;
  v_source_publication_baseline_required boolean := false;
  v_required_physical_publication_contract_version smallint := 0;
  v_authority_fingerprint_version smallint := 2;
  v_physical_currentness jsonb := '{}'::jsonb;
  v_candidate_currentness jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.banking_pay_snapshot_runs AS snapshot_run
  WHERE snapshot_run.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  PERFORM 1
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  SELECT COALESCE(
           settings_row.banking_pay_workbench_semantic_ready_publication_v3_enabled,
           false
         ),
         COALESCE(
           settings_row.banking_pay_source_publication_identity_enforce_v1_enabled,
           false
         )
  INTO v_semantic_ready_publication_enabled,
       v_source_publication_identity_enforced
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1;

  v_source_publication_baseline_required :=
    v_source_publication_identity_enforced
    OR lower(BTRIM(COALESCE(
      v_payload_json->>'source_publication_baseline_required',
      v_payload_json#>>'{source_publication,baseline_required}',
      'false'
    ))) IN ('true','t','1','yes','y','on');
  v_required_physical_publication_contract_version :=
    CASE WHEN v_source_publication_baseline_required THEN 1 ELSE 0 END;
  -- Fingerprint V3 distinguishes the semantic publication contract from the
  -- legacy structural owner even while physical-publication enforcement is
  -- still being rolled out.  The physical contract remains an independent
  -- 0/1 capability inside V3, so enabling semantic V3 does not silently make
  -- legacy Drafts fast-reversion eligible.
  v_authority_fingerprint_version := CASE
    WHEN v_semantic_ready_publication_enabled
      OR v_source_publication_baseline_required THEN 3
    ELSE 2
  END;

  -- One candidate may be requested through several independent refresh routes
  -- in the same lifecycle. Elect/reuse its economic owner under the common
  -- candidate serial authority before taking any session or scope row lock.
  -- This is re-entrant for callers which already own the transaction lock.
  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(p_candidate_id),
      24062027
    )
  );

  v_session_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'session_id',
    v_payload_json->>'source_session_id',
    v_payload_json->>'workbench_session_id',
    v_payload_json#>>'{workbench,session_id}',
    ''
  )), '');

  IF v_session_id_text IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_id', NULL::text,
      'job_type', NULL::text,
      'canonical_job_type', NULL::text,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', NULL::text,
      'candidate_id', p_candidate_id::text,
      'source_change_seq', 0,
      'source_build_required', true,
      'line_work_required', true,
      'delta_refresh_required', false,
      'full_snapshot_job', false,
      'no_op', true,
      'deferred', true,
      'requires_workbench_session', true,
      'reason', v_reason,
      'message', 'Banking Pay candidate refresh now requires a row-backed workbench session.'
    );
  END IF;

  IF v_session_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SESSION_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SESSION_ID_INVALID',
              'session_id', v_session_id_text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  v_session_id := v_session_id_text::uuid;

  SELECT COALESCE(array_agg(DISTINCT parsed_target_ids.timesheet_id_value ORDER BY parsed_target_ids.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_early_preflight_targeted_timesheet_ids
  FROM (
    SELECT NULLIF(BTRIM(targeted_values.value), '')::uuid AS timesheet_id_value
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

  SELECT COALESCE(array_agg(DISTINCT parsed_linked_ids.timesheet_id_value ORDER BY parsed_linked_ids.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_early_preflight_linked_timesheet_ids
  FROM (
    SELECT NULLIF(BTRIM(linked_values.value), '')::uuid AS timesheet_id_value
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

  -- The hotkey is only advisory after the canonical session and scope
  -- authorities have been locked.  It must never create/reuse work for a
  -- candidate which has not first entered the session through the scope owner.
  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=v_session_id
  FOR UPDATE;
  IF NOT FOUND OR UPPER(BTRIM(COALESCE(v_session_row.status,'')))<>'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not open',v_session_id;
  END IF;
  IF v_session_row.source_snapshot_run_id IS DISTINCT FROM p_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH' USING ERRCODE='P0001';
  END IF;
  PERFORM 1 FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id=v_session_id AND scope_row.candidate_id=p_candidate_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %',p_candidate_id,v_session_id;
  END IF;

  IF COALESCE(array_length(v_early_preflight_targeted_timesheet_ids, 1), 0) > 0 THEN
    SELECT public.pay_workbench_authorise_delta_hotkey_preflight(
      p_session_id => v_session_id,
      p_candidate_id => p_candidate_id,
      p_targeted_timesheet_ids => v_early_preflight_targeted_timesheet_ids,
      p_linked_timesheet_ids => v_early_preflight_linked_timesheet_ids,
      p_payload_json => v_payload_json || jsonb_build_object(
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'projection_mode', 'DELTA',
        'projection_class', CASE
          WHEN lower(BTRIM(COALESCE(v_payload_json->>'authorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
            OR lower(BTRIM(COALESCE(v_payload_json->>'unauthorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
            OR lower(BTRIM(COALESCE(v_payload_json->>'lifecycle_mutation_context',v_payload_json->>'mutation_context',''))) IN ('timesheet_authorise','authorise_timesheet','timesheet_unauthorise','unauthorise_timesheet')
          THEN 'TIMESHEET_LIFECYCLE' ELSE 'NORMAL_TIMESHEET' END,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      p_reason => v_reason,
      p_actor_user_id => v_actor_user_id,
      p_source_change_seq => NULL::bigint
    ) INTO v_early_preflight_result;

    v_early_preflight_action := COALESCE(v_early_preflight_result->>'action', 'PROCEED');

    IF v_early_preflight_action IN ('REUSED_QUEUED_SAME_FAMILY_JOB', 'UPDATED_WAITING_AFTER_RUNNING_JOB') THEN
      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok', true,
        'job_id', NULLIF(BTRIM(COALESCE(v_early_preflight_result->>'job_id', '')), ''),
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', CASE WHEN COALESCE(v_early_preflight_result->>'latest_source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (v_early_preflight_result->>'latest_source_change_seq')::bigint ELSE NULL::bigint END,
        'dedupe_key', NULLIF(BTRIM(COALESCE(v_early_preflight_result->>'normalised_delta_family_key', '')), ''),
        'reason', v_reason,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'targeted_timesheet_ids', COALESCE(v_early_preflight_result->'targeted_timesheet_ids', '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_early_preflight_result->'linked_timesheet_ids', '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_early_preflight_result->'queue_identity_targeted_timesheet_ids', v_early_preflight_result->'resolved_family_timesheet_ids', v_early_preflight_result->'targeted_timesheet_ids', '[]'::jsonb),
        'queue_identity_linked_timesheet_ids', COALESCE(v_early_preflight_result->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
        'source_build_required', false,
        'line_work_required', false,
        'line_work_only', false,
        'delta_refresh_required', true,
        'scope_status', 'DELTA_REFRESH_PENDING',
        'projection_mode', 'DELTA',
        'projection_class', 'NORMAL_TIMESHEET',
        'full_snapshot_job', false,
        'reused', true,
        'early_preflight_action', v_early_preflight_action,
        'early_preflight_result', COALESCE(v_early_preflight_result, '{}'::jsonb),
        'early_preflight_returned_before_session_lock', true,
        'session_lock_skipped', true,
        'scope_lock_skipped', true,
        'classifier_work_skipped', true,
        'dirty_marking_skipped', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = v_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', v_session_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not open', v_session_id;
  END IF;

  IF v_session_row.source_snapshot_run_id IS DISTINCT FROM p_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH',
              'session_id', v_session_id::text,
              'payload_snapshot_run_id', p_snapshot_run_id::text,
              'session_snapshot_run_id', v_session_row.source_snapshot_run_id::text
            )::text;
  END IF;

  PERFORM 1
  FROM public.banking_pay_workbench_session_scope AS session_scope
  WHERE session_scope.session_id = v_session_id
    AND session_scope.candidate_id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %', p_candidate_id, v_session_id;
  END IF;

  v_actor_user_id := COALESCE(v_actor_user_id, v_session_row.actor_user_id);

  SELECT COALESCE(change_counter.seq, 0),
         COALESCE(change_counter.scope_change_generation, 0)
  INTO v_live_change_seq,
       v_live_scope_change_generation
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text
  FOR UPDATE;

  IF COALESCE(v_payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'source_change_seq')::bigint;
  ELSIF COALESCE(v_payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'source_change_sequence')::bigint;
  ELSIF COALESCE(v_payload_json->>'latest_source_change_seq', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'latest_source_change_seq')::bigint;
  ELSIF COALESCE(v_payload_json#>>'{source_build,source_change_seq}', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json#>>'{source_build,source_change_seq}')::bigint;
  END IF;

  v_source_change_seq := GREATEST(COALESCE(v_payload_source_change_seq, 0), COALESCE(v_live_change_seq, 0));

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

  v_refresh_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(
    v_payload_json->>'refresh_scope_kind',
    v_payload_json#>>'{source_build,refresh_scope_kind}',
    v_payload_json#>>'{preview_decisions_json,refresh_scope_kind}',
    ''
  ))), '');

  IF v_refresh_scope_kind IS NULL OR v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_refresh_scope_kind := CASE
      WHEN jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
        OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0
      THEN 'TARGETED_TIMESHEETS'
      ELSE 'CANDIDATE_FULL_LIVE'
    END;
  END IF;


  v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
  v_queue_identity_linked_timesheet_ids_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);

  IF v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
     AND to_regprocedure('public._pay_workbench_normalise_timesheet_rotation_scope_payload(uuid[],uuid[])') IS NOT NULL THEN
    SELECT public._pay_workbench_normalise_timesheet_rotation_scope_payload(
      COALESCE((
        SELECT array_agg(DISTINCT targeted_value.value::uuid ORDER BY targeted_value.value::uuid)
        FROM jsonb_array_elements_text(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) AS targeted_value(value)
        WHERE targeted_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ), ARRAY[]::uuid[]),
      COALESCE((
        SELECT array_agg(DISTINCT linked_value.value::uuid ORDER BY linked_value.value::uuid)
        FROM jsonb_array_elements_text(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) AS linked_value(value)
        WHERE linked_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ), ARRAY[]::uuid[])
    )
    INTO v_rotation_scope_json;

    v_payload_json := public._pay_workbench_merge_targeted_scope_payload(
      v_payload_json,
      jsonb_build_object(
        'targeted_timesheet_ids_requested', COALESCE(v_rotation_scope_json->'requested_targeted_timesheet_ids', v_targeted_timesheet_ids_json),
        'linked_timesheet_ids_requested', COALESCE(v_rotation_scope_json->'requested_linked_timesheet_ids', v_linked_timesheet_ids_json),
        'requested_timesheet_ids', COALESCE(v_rotation_scope_json->'requested_timesheet_ids', v_targeted_timesheet_ids_json),
        'family_timesheet_ids', COALESCE(v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'targeted_timesheet_ids', COALESCE(v_rotation_scope_json->'targeted_timesheet_ids', v_targeted_timesheet_ids_json),
        'linked_timesheet_ids', COALESCE(v_rotation_scope_json->'linked_timesheet_ids', v_linked_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_targeted_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'queue_identity_linked_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
        'queue_identity_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'rotation_family_scope_for_queue_identity', true,
        'queue_identity_preserves_targeted_linked_semantics', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );
    v_targeted_timesheet_ids_json := COALESCE(v_rotation_scope_json->'targeted_timesheet_ids', v_targeted_timesheet_ids_json);
    v_linked_timesheet_ids_json := COALESCE(v_rotation_scope_json->'linked_timesheet_ids', v_linked_timesheet_ids_json, '[]'::jsonb);
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_rotation_scope_json->'queue_identity_targeted_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json, '[]'::jsonb);
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_rotation_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb);
  END IF;

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

  v_force_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_force_broad_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_broad_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF jsonb_typeof(v_payload_json->'classifier_result') = 'object' THEN
    v_classifier_result := v_payload_json->'classifier_result';
  ELSIF v_payload_json ? 'fast_path_allowed' OR v_payload_json ? 'resolved_job_type' THEN
    v_classifier_result := v_payload_json;
  ELSIF to_regprocedure('public.pay_workbench_delta_refresh_classify_v1(uuid,uuid,jsonb)') IS NOT NULL THEN
    v_classifier_result := public.pay_workbench_delta_refresh_classify_v1(
      v_session_id,
      p_candidate_id,
      v_payload_json
      || jsonb_build_object(
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'pay_channel_scope', v_pay_channel_scope,
        'force_legacy', v_force_legacy,
        'force_broad_legacy', v_force_broad_legacy
      )
    );
  ELSE
    v_classifier_result := jsonb_build_object(
      'ok', true,
      'fast_path_allowed', false,
      'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'projection_mode', 'LEGACY',
      'projection_class', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_payload_json->>'projection_class', ''))), ''), 'UNKNOWN_TRIGGER'),
      'scope_status', 'SOURCE_BUILD_PENDING',
      'fallback_required', true,
      'fallback_reason', 'DELTA_CLASSIFIER_NOT_INSTALLED'
    );
  END IF;

  v_classifier_result := COALESCE(v_classifier_result, '{}'::jsonb)
    - 'old_row_json'
    - 'new_row_json'
    - 'old_row'
    - 'new_row'
    - 'source_row_json'
    - 'work_payload_json'
    - 'result_row_json'
    - 'preview_row_json'
    - 'row_payload_json'
    - 'line_payload_json'
    - 'projection_rows'
    - 'projected_rows';

  v_resolved_mode := UPPER(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', v_classifier_result->>'routing_decision', v_classifier_result->>'projection_mode', 'LEGACY')));
  IF v_resolved_mode NOT IN ('DELTA', 'PATCH_ONLY', 'CLONE_REBASE', 'LEGACY', 'BLOCKED') THEN
    v_resolved_mode := 'LEGACY';
  END IF;

  v_resolved_job_type := UPPER(BTRIM(COALESCE(v_classifier_result->>'resolved_job_type', CASE WHEN v_resolved_mode = 'DELTA' THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH' WHEN v_resolved_mode = 'LEGACY' THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD' ELSE 'NOOP' END)));
  v_projection_mode := UPPER(BTRIM(COALESCE(v_classifier_result->>'projection_mode', v_payload_json->>'projection_mode', 'LEGACY')));
  v_projection_class := UPPER(BTRIM(COALESCE(v_classifier_result->>'projection_class', v_payload_json->>'projection_class', 'UNKNOWN')));

  IF jsonb_typeof(v_classifier_result->'targeted_timesheet_ids') = 'array' THEN
    v_targeted_timesheet_ids_json := v_classifier_result->'targeted_timesheet_ids';
  END IF;
  IF jsonb_typeof(v_classifier_result->'linked_timesheet_ids') = 'array' THEN
    v_linked_timesheet_ids_json := v_classifier_result->'linked_timesheet_ids';
  END IF;

  IF jsonb_typeof(v_payload_json->'queue_identity_targeted_timesheet_ids') = 'array' THEN
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_payload_json->'queue_identity_targeted_timesheet_ids', v_queue_identity_targeted_timesheet_ids_json, '[]'::jsonb);
  ELSIF jsonb_array_length(COALESCE(v_queue_identity_targeted_timesheet_ids_json, '[]'::jsonb)) = 0 THEN
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
  END IF;

  IF jsonb_typeof(v_payload_json->'queue_identity_linked_timesheet_ids') = 'array' THEN
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_payload_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb);
  ELSIF jsonb_array_length(COALESCE(v_queue_identity_linked_timesheet_ids_json, '[]'::jsonb)) = 0 THEN
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);
  END IF;

  IF v_force_legacy IS TRUE OR v_force_broad_legacy IS TRUE THEN
    v_resolved_mode := 'LEGACY';
    v_resolved_job_type := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
  INTO v_bounded_timesheet_ids
  FROM jsonb_array_elements_text(
    COALESCE(v_targeted_timesheet_ids_json,'[]'::jsonb)
    || COALESCE(v_linked_timesheet_ids_json,'[]'::jsonb)
  ) value
  WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  IF COALESCE(v_payload_json->>'scope_change_tx_token','')
     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_scope_change_tx_token:=(v_payload_json->>'scope_change_tx_token')::uuid;
  END IF;
  v_scope_state_precedes_job := lower(BTRIM(COALESCE(
    v_payload_json->>'bounded_scope_state_precedes_job','false'
  ))) IN ('true','t','1','yes','y','on');
  IF COALESCE(v_payload_json->>'scope_change_generation','') ~ '^\d+$' THEN
    v_payload_scope_change_generation :=
      (v_payload_json->>'scope_change_generation')::bigint;
  END IF;

  -- Elect an already-current economic owner before scope invalidation.  A
  -- reason such as USER_REQUESTED_FULL_REFRESH or force_legacy is provenance,
  -- not proof that current certified output is stale.  This early fence is
  -- what prevents a same-sequence refresh from first destroying the evidence
  -- needed by COMPLETE_CURRENT_AUTHORITY.
  IF NOT v_scope_state_precedes_job THEN
    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_before
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;
  END IF;

  v_session_signature_token:=md5(COALESCE(v_session_row.session_signature,''));

  v_authority_fingerprint_text := concat_ws('|',
    CASE WHEN v_authority_fingerprint_version=3
      THEN 'WORKBENCH_SOURCE_OWNER_V3' ELSE 'WORKBENCH_SOURCE_OWNER_V2' END,
    v_session_id::text,
    COALESCE(v_session_row.version,0)::text,
    v_session_row.source_snapshot_run_id::text,
    v_session_signature_token,
    p_candidate_id::text,
    COALESCE(v_source_change_seq,0)::text,
    COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)::text,
    UPPER(BTRIM(COALESCE(v_pay_channel_scope,'ALL'))),
    'FULL_CANDIDATE',
    CASE WHEN v_authority_fingerprint_version=3
      THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
    CASE WHEN v_authority_fingerprint_version=3
      THEN v_required_physical_publication_contract_version::text ELSE NULL END
  );
  v_authority_fingerprint := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_authority_fingerprint_text,'UTF8'),'sha256'),
    'hex'
  );

  SELECT build_row.*
  INTO v_owner_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  JOIN public.banking_pay_workbench_session_scope AS current_scope
    ON current_scope.session_id=build_row.session_id
   AND current_scope.candidate_id=build_row.candidate_id
  JOIN public.banking_pay_workbench_session_candidate_state AS current_state
    ON current_state.session_id=build_row.session_id
   AND current_state.candidate_id=build_row.candidate_id
  WHERE build_row.candidate_id=p_candidate_id
    AND build_row.session_id=v_session_id
    AND build_row.session_version=COALESCE(v_session_row.version,0)
    AND build_row.source_snapshot_run_id=v_session_row.source_snapshot_run_id
    AND build_row.source_change_seq=COALESCE(v_source_change_seq,0)
    AND build_row.captured_candidate_generation=
          COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)
    AND UPPER(BTRIM(COALESCE(build_row.status,'')))='COMPLETE'
    AND UPPER(BTRIM(COALESCE(build_row.private_stage,'')))='COMPLETE'
    AND current_scope.dirty IS FALSE
    AND current_scope.pending_job_id IS NULL
    AND current_scope.certified_preview_publication_required IS TRUE
    AND current_scope.certified_preview_publication_parity_ok IS TRUE
    AND current_scope.certified_preview_publication_session_version=build_row.session_version
    AND current_scope.certified_preview_publication_source_change_seq=build_row.source_change_seq
    AND current_scope.certified_preview_publication_source_build_run_id=build_row.source_build_run_id
    AND (
      NOT v_source_publication_baseline_required
      OR (
        current_scope.certified_preview_publication_source_publication_id IS NOT NULL
        AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
              =current_scope.certified_preview_publication_source_publication_id::text
      )
    )
    AND (
      NOT v_semantic_ready_publication_enabled
      OR (
        current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              ='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
        AND current_scope.certified_preview_publication_attestation_json->>'contract_version'='3'
        AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              ='READY_TO_PAY_SEMANTIC_V2'
        AND COALESCE(
          (current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,
          false
        )
        AND COALESCE(
          (current_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,
          false
        )
      )
    )
    AND UPPER(BTRIM(COALESCE(current_state.status,'')))='READY'
    AND current_state.pending_job_id IS NULL
    AND current_state.session_version=build_row.session_version
    AND current_state.source_change_seq=build_row.source_change_seq
  ORDER BY build_row.completed_at_utc DESC NULLS LAST,build_row.id DESC
  LIMIT 1
  FOR UPDATE OF build_row,current_scope,current_state;

  IF FOUND THEN
    SELECT source_job.*
    INTO v_owner_root_job
    FROM public.banking_pay_workbench_jobs AS source_job
    WHERE source_job.economic_build_id=v_owner_build.id
      AND UPPER(BTRIM(COALESCE(source_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND COALESCE(
        source_job.payload_json->>'source_build_run_id',
        source_job.payload_json#>>'{source_build,source_build_run_id}',
        ''
      )=v_owner_build.source_build_run_id::text
    ORDER BY source_job.created_at_utc,source_job.id
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      v_owner_pay_channel_scope:=UPPER(BTRIM(COALESCE(
        v_owner_root_job.payload_json->>'pay_channel_scope',
        v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
        'ALL'
      )));
      IF v_owner_pay_channel_scope=UPPER(BTRIM(COALESCE(v_pay_channel_scope,'ALL'))) THEN
        EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
          INTO v_physical_currentness
          USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
            jsonb_build_object('contract_version',1,'allow_active_owner',false);
        v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
        IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) IS NOT TRUE THEN
          v_owner_build:=NULL;
          v_owner_root_job:=NULL;
        ELSE
        UPDATE public.banking_pay_workbench_jobs AS owner_job
        SET payload_json=jsonb_strip_nulls(
              COALESCE(owner_job.payload_json,'{}'::jsonb)
              || jsonb_build_object('reason_latest',v_reason)
            ),
            updated_at_utc=v_now
        WHERE owner_job.id=v_owner_root_job.id;

        RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok',true,
        'job_id',v_owner_root_job.id::text,
        'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id',v_session_id::text,
        'candidate_id',p_candidate_id::text,
        'session_version',COALESCE(v_session_row.version,0),
        'source_change_seq',COALESCE(v_source_change_seq,0),
        'registry_source_change_seq',COALESCE(v_registry_source_change_seq_before,v_source_change_seq,0),
        'source_build_run_id',v_owner_build.source_build_run_id::text,
        'source_publication_id',(
          SELECT current_scope.certified_preview_publication_source_publication_id::text
          FROM public.banking_pay_workbench_session_scope AS current_scope
          WHERE current_scope.session_id=v_session_id
            AND current_scope.candidate_id=p_candidate_id
        ),
        'authority_fingerprint',COALESCE(v_owner_build.authority_fingerprint,v_authority_fingerprint),
        'authority_fingerprint_version',COALESCE(v_owner_build.authority_fingerprint_version,v_authority_fingerprint_version),
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution','COMPLETE_CURRENT_AUTHORITY',
        'owner_build_id',v_owner_build.id::text,
        'owner_root_job_id',v_owner_root_job.id::text,
        'owner_source_build_run_id',v_owner_build.source_build_run_id::text,
        'requested_coverage','FULL_CANDIDATE',
        'owner_coverage','FULL_CANDIDATE',
        'scope_status','MATERIALISED',
        'source_build_required',false,
        'delta_refresh_required',false,
        'coalesced',true,
        'reused',true,
        'new_owner_created',false,
        'no_op',true,
        'pre_invalidation_owner_election',true,
        'diagnostic_provenance_merged',true,
        'reason',v_reason,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
        ));
        END IF;
      END IF;
    END IF;
  END IF;

  v_owner_build:=NULL;
  v_owner_root_job:=NULL;
  v_owner_active_job_id:=NULL::uuid;
  SELECT build_row.*
  INTO v_owner_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.candidate_id=p_candidate_id
    AND build_row.session_id=v_session_id
    AND build_row.session_version=COALESCE(v_session_row.version,0)
    AND build_row.source_snapshot_run_id=v_session_row.source_snapshot_run_id
    AND build_row.source_change_seq=COALESCE(v_source_change_seq,0)
    AND build_row.captured_candidate_generation=
          COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)
    AND build_row.authority_fingerprint_version=v_authority_fingerprint_version
    AND build_row.authority_fingerprint=v_authority_fingerprint
    AND UPPER(BTRIM(COALESCE(build_row.status,''))) IN (
      'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
      'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
    )
    AND EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.economic_build_id=build_row.id
        AND active_job.status IN ('QUEUED','RUNNING')
    )
  ORDER BY build_row.created_at_utc DESC,build_row.id DESC
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    SELECT source_job.*
    INTO v_owner_root_job
    FROM public.banking_pay_workbench_jobs AS source_job
    WHERE source_job.economic_build_id=v_owner_build.id
      AND UPPER(BTRIM(COALESCE(source_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
    ORDER BY source_job.created_at_utc,source_job.id
    LIMIT 1
    FOR UPDATE;
    SELECT active_job.id
    INTO v_owner_active_job_id
    FROM public.banking_pay_workbench_jobs AS active_job
    WHERE active_job.economic_build_id=v_owner_build.id
      AND active_job.status IN ('QUEUED','RUNNING')
    ORDER BY CASE WHEN active_job.status='RUNNING' THEN 0 ELSE 1 END,
             active_job.run_at_utc,active_job.created_at_utc,active_job.id
    LIMIT 1;

    IF v_owner_root_job.id IS NOT NULL AND v_owner_active_job_id IS NOT NULL THEN
      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok',true,
        'job_id',v_owner_root_job.id::text,
        'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id',v_session_id::text,
        'candidate_id',p_candidate_id::text,
        'session_version',COALESCE(v_session_row.version,0),
        'source_change_seq',COALESCE(v_source_change_seq,0),
        'source_build_run_id',v_owner_build.source_build_run_id::text,
        'authority_fingerprint',v_authority_fingerprint,
        'authority_fingerprint_version',v_authority_fingerprint_version,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution','ACTIVE_CURRENT_OWNER_COVERS_REQUEST',
        'owner_build_id',v_owner_build.id::text,
        'owner_root_job_id',v_owner_root_job.id::text,
        'owner_active_job_id',v_owner_active_job_id::text,
        'owner_source_build_run_id',v_owner_build.source_build_run_id::text,
        'requested_coverage','FULL_CANDIDATE',
        'owner_coverage','FULL_CANDIDATE',
        'scope_status','SOURCE_BUILD_PENDING',
        'source_build_required',true,
        'delta_refresh_required',false,
        'coalesced',true,
        'reused',true,
        'new_owner_created',false,
        'no_op',false,
        'pre_invalidation_owner_election',true,
        'reason',v_reason,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  v_owner_build:=NULL;
  v_owner_root_job:=NULL;
  v_owner_active_job_id:=NULL::uuid;

  IF v_scope_state_precedes_job THEN
    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_before
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;

    -- A delayed pre-invalidated job may be older than a newer authority which
    -- has already adopted the live sequence and generation.  Such a request
    -- may only be absorbed by an active/current owner below; it must never
    -- create work from its stale transaction proof.
    v_stale_preinvalidated_absorb_only :=
      v_payload_scope_change_generation IS NOT NULL
      AND v_payload_scope_change_generation < COALESCE(v_live_scope_change_generation,0)
      AND COALESCE(v_registry_dirty_generation,0)=COALESCE(v_live_scope_change_generation,0)
      AND COALESCE(v_registry_source_change_seq_before,0)=COALESCE(v_source_change_seq,0);

    IF v_stale_preinvalidated_absorb_only THEN
      v_registry_source_change_seq_after:=v_registry_source_change_seq_before;
      v_scope_invalidation_result:=jsonb_build_object(
        'ok',true,
        'stale_preinvalidated_absorb_only',true,
        'payload_scope_change_generation',v_payload_scope_change_generation,
        'current_scope_change_generation',v_live_scope_change_generation,
        'accepted_source_change_seq',v_source_change_seq,
        'reason',COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED')
      );
    ELSE
      SELECT scope_tx.state,
             scope_tx.allocated_generation
      INTO v_finalized_scope_tx_state,
           v_finalized_scope_tx_generation
      FROM public.banking_pay_scope_change_transactions AS scope_tx
      WHERE scope_tx.tx_token=v_scope_change_tx_token
      FOR UPDATE;

      SELECT count(*)::integer
      INTO v_scope_state_generation_match_count
      FROM unnest(v_bounded_timesheet_ids) AS requested(timesheet_id)
       JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
        ON scope_state.timesheet_id=requested.timesheet_id
       AND scope_state.candidate_id=p_candidate_id
       AND scope_state.dirty_generation=v_payload_scope_change_generation;

      IF v_scope_change_tx_token IS NULL
         OR v_payload_scope_change_generation IS NULL
         OR v_payload_scope_change_generation < 1
         OR v_finalized_scope_tx_state IS DISTINCT FROM 'FINALIZED'
         OR v_finalized_scope_tx_generation IS DISTINCT FROM
            v_payload_scope_change_generation
         OR v_live_scope_change_generation IS DISTINCT FROM
            v_payload_scope_change_generation
         OR COALESCE(v_registry_dirty_generation,0) IS DISTINCT FROM
            v_payload_scope_change_generation
         OR v_scope_state_generation_match_count IS DISTINCT FROM
            cardinality(v_bounded_timesheet_ids) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED'
          USING ERRCODE='22023', DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
            'candidate_id',p_candidate_id,
            'scope_change_tx_token',v_scope_change_tx_token,
            'payload_scope_change_generation',v_payload_scope_change_generation,
            'transaction_state',v_finalized_scope_tx_state,
            'transaction_generation',v_finalized_scope_tx_generation,
            'live_scope_change_generation',v_live_scope_change_generation,
            'registry_dirty_generation',v_registry_dirty_generation,
            'requested_timesheet_count',cardinality(v_bounded_timesheet_ids),
            'matched_scope_state_count',v_scope_state_generation_match_count
          )::text;
      END IF;

      UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry
      SET current_source_change_seq=GREATEST(
            COALESCE(registry.current_source_change_seq,0),
            COALESCE(v_source_change_seq,0)
          ),
          updated_at_utc=CASE
            WHEN COALESCE(registry.current_source_change_seq,0)<COALESCE(v_source_change_seq,0)
              THEN clock_timestamp()
            ELSE registry.updated_at_utc
          END
      WHERE registry.candidate_id=p_candidate_id
        AND registry.dirty_generation=v_payload_scope_change_generation
      RETURNING registry.current_source_change_seq
      INTO v_registry_source_change_seq_after;

      IF NOT FOUND
         OR COALESCE(v_registry_source_change_seq_after,0)<COALESCE(v_source_change_seq,0) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED'
          USING ERRCODE='40001', DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED',
            'candidate_id',p_candidate_id,
            'accepted_source_change_seq',v_source_change_seq,
            'registry_source_change_seq_before',v_registry_source_change_seq_before,
            'registry_source_change_seq_after',v_registry_source_change_seq_after,
            'scope_change_generation',v_payload_scope_change_generation
          )::text;
      END IF;
      v_registry_sequence_synchronised :=
        COALESCE(v_registry_source_change_seq_after,0)>
        COALESCE(v_registry_source_change_seq_before,0);

      v_scope_invalidation_result := jsonb_build_object(
        'ok',true,
        'already_finalized',true,
        'scope_change_tx_token',v_scope_change_tx_token,
        'scope_change_generation',v_finalized_scope_tx_generation,
        'candidate_count',1,
        'timesheet_count',cardinality(v_bounded_timesheet_ids),
        'accepted_source_change_seq',v_source_change_seq,
        'registry_source_change_seq_before',v_registry_source_change_seq_before,
        'registry_source_change_seq_after',v_registry_source_change_seq_after,
        'registry_sequence_synchronised',v_registry_sequence_synchronised,
        'reason',COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED')
      );
    END IF;
  ELSE
    v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
      CASE WHEN cardinality(v_bounded_timesheet_ids)=0 THEN ARRAY[p_candidate_id]
        ELSE array_fill(p_candidate_id,ARRAY[cardinality(v_bounded_timesheet_ids)]) END,
      CASE WHEN cardinality(v_bounded_timesheet_ids)=0 THEN ARRAY[NULL::uuid]
        ELSE v_bounded_timesheet_ids END,
      COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED'),
      v_scope_change_tx_token,
      v_payload_json||jsonb_build_object(
        'skip_candidate_job_enqueue',true,
        'source_change_seq',v_source_change_seq,
        'source_change_sequence',v_source_change_seq,
        'latest_source_change_seq',v_source_change_seq
      )
    );

    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_after
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;

    IF NOT FOUND
       OR COALESCE(v_registry_source_change_seq_after,0)<COALESCE(v_source_change_seq,0) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED'
        USING ERRCODE='40001', DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED',
          'candidate_id',p_candidate_id,
          'accepted_source_change_seq',v_source_change_seq,
          'registry_source_change_seq_after',v_registry_source_change_seq_after,
          'registry_dirty_generation',v_registry_dirty_generation
        )::text;
    END IF;
    v_registry_sequence_synchronised := true;
  END IF;

  v_payload_shadow_compare_required := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_required',
    v_payload_json->>'shadow_compare',
    v_payload_json->>'shadow_mode',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_payload_shadow_compare_enforced := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_enforced',
    v_payload_json->>'enforce_shadow_compare',
    v_payload_json->>'shadow_enforced',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_shadow_compare_required := (
    lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,delta_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,payload_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_payload_shadow_compare_required
  );

  v_shadow_compare_enforced := COALESCE(v_shadow_compare_required, false) AND (
    COALESCE(v_payload_shadow_compare_enforced, false)
    OR lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_enforced', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
  );

  IF v_resolved_mode IN ('PATCH_ONLY', 'CLONE_REBASE', 'BLOCKED') THEN
    v_no_job_reason := CASE
      WHEN v_resolved_mode = 'PATCH_ONLY' THEN COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'PATCH_ONLY_NO_SOURCE_BUILD_JOB')
      WHEN v_resolved_mode = 'CLONE_REBASE' THEN COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'CLONE_REBASE_NOT_A_CANDIDATE_SOURCE_BUILD_JOB')
      ELSE COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'CLASSIFIER_BLOCKED_REFRESH')
    END;

    UPDATE public.banking_pay_workbench_session_scope AS no_job_scope
    SET status = CASE
          WHEN v_resolved_mode = 'PATCH_ONLY' THEN COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), 'PATCH_ONLY_PENDING')
          WHEN v_resolved_mode = 'CLONE_REBASE' THEN COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), 'CLONE_REBASE_PENDING')
          ELSE COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), no_job_scope.status)
        END,
        pending_job_id = NULL,
        dirty = false,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE no_job_scope.session_id = v_session_id
      AND no_job_scope.candidate_id = p_candidate_id
      AND v_resolved_mode IN ('PATCH_ONLY', 'CLONE_REBASE');

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'job_id', NULL::text,
      'job_type', NULL::text,
      'canonical_job_type', NULL::text,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', v_session_id::text,
      'candidate_id', p_candidate_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'source_change_seq', COALESCE(v_source_change_seq, 0),
      'reason', v_reason,
      'resolved_mode', v_resolved_mode,
      'projection_mode', v_projection_mode,
      'projection_class', v_projection_class,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
      'pay_channel_scope', v_pay_channel_scope,
      'source_build_required', false,
      'line_work_required', false,
      'line_work_only', false,
      'delta_refresh_required', false,
      'patch_only', v_resolved_mode = 'PATCH_ONLY',
      'clone_rebase_required', v_resolved_mode = 'CLONE_REBASE',
      'blocked', v_resolved_mode = 'BLOCKED',
      'no_op', v_resolved_mode = 'BLOCKED',
      'deferred', v_resolved_mode = 'CLONE_REBASE',
      'fallback_required', lower(BTRIM(COALESCE(v_classifier_result->>'fallback_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'fallback_reason', v_no_job_reason,
      'classifier_result', v_classifier_result
    ));
  END IF;

  IF v_resolved_mode = 'DELTA'
     AND v_resolved_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
     AND lower(BTRIM(COALESCE(v_classifier_result->>'fast_path_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
     AND v_force_legacy IS NOT TRUE
     AND v_force_broad_legacy IS NOT TRUE THEN
    v_job_type := 'WORKBENCH_CANDIDATE_DELTA_REFRESH';
  ELSE
    v_resolved_mode := 'LEGACY';
    v_job_type := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  END IF;

  IF COALESCE(v_payload_json->>'source_build_limit', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json->>'source_build_limit')::integer, 1), 100);
  ELSIF COALESCE(v_payload_json#>>'{stage_limits,source_build}', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json#>>'{stage_limits,source_build}')::integer, 1), 100);
  ELSIF COALESCE(v_payload_json->>'limit', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json->>'limit')::integer, 1), 100);
  ELSE
    SELECT LEAST(GREATEST(COALESCE(
      CASE
        WHEN COALESCE(to_jsonb(settings_row)->>'banking_pay_workbench_source_build_units_per_job', '') ~ '^[0-9]{1,9}$'
          THEN (to_jsonb(settings_row)->>'banking_pay_workbench_source_build_units_per_job')::integer
        ELSE NULL::integer
      END,
      CASE
        WHEN COALESCE(to_jsonb(settings_row)->>'banking_pay_workbench_stage_work_units_per_job', '') ~ '^[0-9]{1,9}$'
          THEN (to_jsonb(settings_row)->>'banking_pay_workbench_stage_work_units_per_job')::integer
        ELSE NULL::integer
      END,
      25
    ), 1), 100)
    INTO v_stage_limit
    FROM public.settings_defaults AS settings_row
    WHERE settings_row.id = 1
    LIMIT 1;

    v_stage_limit := COALESCE(v_stage_limit, 25);
  END IF;

  v_initial_cursor_json := CASE
    WHEN jsonb_typeof(v_payload_json->'cursor_json') = 'object' THEN v_payload_json->'cursor_json'
    WHEN jsonb_typeof(v_payload_json->'cursor') = 'object' THEN v_payload_json->'cursor'
    WHEN jsonb_typeof(v_payload_json->'source_cursor') = 'object' THEN v_payload_json->'source_cursor'
    WHEN jsonb_typeof(v_payload_json#>'{source_build,cursor}') = 'object' THEN v_payload_json#>'{source_build,cursor}'
    ELSE '{}'::jsonb
  END;

  v_cursor_token := md5(COALESCE(v_initial_cursor_json, '{}'::jsonb)::text);
  v_session_signature_token := md5(COALESCE(v_session_row.session_signature, ''));

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_projection_run_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'projection_run_id', v_classifier_result->>'projection_run_id', '')), '');
    IF v_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_projection_run_id := v_projection_run_id_text::uuid;
    ELSE
      v_projection_run_id := gen_random_uuid();
    END IF;

    v_delta_ids_hash := md5(
      COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb)::text
      || ':'
      || COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb)::text
    );

    v_delta_coalescing_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
    || ':session:' || COALESCE(v_session_id::text, 'none')
    || ':version:' || COALESCE(COALESCE(v_session_row.version, 0), 0)::text
    || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''), 'DELTA'))), ''), 'DELTA')
    || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(v_projection_class, ''), 'UNKNOWN'))), ''), 'UNKNOWN')
    || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''), 'TARGETED_TIMESHEETS'))), ''), 'TARGETED_TIMESHEETS')
    || ':candidate:' || COALESCE(p_candidate_id::text, 'none')
    || ':timesheets:' || COALESCE(v_delta_ids_hash, md5('[]:[]'));
    v_delta_coalescing_hash := md5(v_delta_coalescing_key);

    SELECT running_delta_job.id
    INTO v_delta_active_running_job_id
    FROM public.banking_pay_workbench_jobs AS running_delta_job
    WHERE running_delta_job.session_id = v_session_id
      AND running_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
      AND (
        COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
        OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.session_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.candidate_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
      )
    ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
    LIMIT 1;

    v_dedupe_key := CASE
      WHEN v_delta_active_running_job_id IS NOT NULL THEN
        v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text
      ELSE
        v_delta_coalescing_key
    END;

    v_payload_out_json := jsonb_strip_nulls(
      v_payload_json
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'projection_run_id', v_projection_run_id::text,
        'projection_mode', COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'),
        'projection_class', v_projection_class,
        'phase', 'INIT_PREFLIGHT',
        'cursor_json', '{}'::jsonb,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_linked_timesheet_ids', COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_change_sequence', COALESCE(v_source_change_seq, 0),
        'latest_source_change_seq', COALESCE(v_source_change_seq, 0),
        'delta_coalescing_key', v_delta_coalescing_key,
        'delta_family_key', v_delta_coalescing_key,
        'normalised_delta_family_key', v_delta_coalescing_key,
        'delta_coalescing_hash', v_delta_coalescing_hash,
        'coalesced_source_change_seqs', jsonb_build_array(COALESCE(v_source_change_seq, 0)),
        'coalesced_event_count', 1,
        'latest_event_at_utc', v_now::text,
        'source_build_required', false,
        'line_work_required', false,
        'delta_refresh_required', true,
        'legacy_fallback_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      )
      || jsonb_build_object(
        'reason', v_reason,
        'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'workbench_session_id', v_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'pay_channel_scope', v_pay_channel_scope,
        'refresh_scope_kind', COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'),
        'candidate_id', p_candidate_id::text,
        'dedupe_key', v_dedupe_key,
        'shadow_compare_required', COALESCE(v_shadow_compare_required, false),
        'shadow_compare_enforced', COALESCE(v_shadow_compare_enforced, false),
        'classifier_result', v_classifier_result,
        'fallback_required', false,
        'fallback_reason', NULL::text,
        'force_legacy', false,
        'force_broad_legacy', false
      )
      || jsonb_build_object(
        'mutation_context', NULLIF(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', '')), ''),
        'lifecycle_mutation_context', NULLIF(BTRIM(COALESCE(v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_context', '')), ''),
        'trigger_table', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json#>>'{trigger,table}', '')), ''),
        'trigger_operation', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json->>'trigger_op', v_payload_json#>>'{trigger,operation}', '')), ''),
        'trigger_op', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_op', v_payload_json->>'trigger_operation', v_payload_json#>>'{trigger,operation}', '')), ''),
        'authorise_boundary_changed', lower(BTRIM(COALESCE(v_payload_json->>'authorise_boundary_changed', v_classifier_result#>>'{complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'unauthorise_boundary_changed', lower(BTRIM(COALESCE(v_payload_json->>'unauthorise_boundary_changed', v_classifier_result#>>'{complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'explicit_banking_pay_action', lower(BTRIM(COALESCE(v_payload_json->>'explicit_banking_pay_action', v_classifier_result#>>'{complexity_flags,explicit_banking_pay_action}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'banking_pay_dirty_required', lower(BTRIM(COALESCE(v_payload_json->>'banking_pay_dirty_required', v_classifier_result->>'banking_pay_dirty_required', v_classifier_result#>>'{complexity_flags,banking_pay_dirty_required}', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'ordinary_timesheet_edit_save_no_dirty', lower(BTRIM(COALESCE(v_payload_json->>'ordinary_timesheet_edit_save_no_dirty', v_classifier_result#>>'{complexity_flags,ordinary_timesheet_edit_save_no_dirty}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
    );
  ELSE
    v_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
      v_payload_json->>'source_build_run_id',
      v_payload_json#>>'{source_build,source_build_run_id}',
      v_payload_json#>>'{source_build,run_id}',
      v_initial_cursor_json->>'source_build_run_id',
      ''
    )), '');

    IF v_source_build_run_id_text IS NOT NULL THEN
      IF v_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SOURCE_BUILD_RUN_ID_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SOURCE_BUILD_RUN_ID_INVALID',
                  'source_build_run_id', v_source_build_run_id_text,
                  'session_id', v_session_id::text,
                  'candidate_id', p_candidate_id::text
                )::text;
      END IF;
      v_requested_source_build_run_id := v_source_build_run_id_text::uuid;
    END IF;

    v_authority_fingerprint_text := concat_ws('|',
      CASE WHEN v_authority_fingerprint_version=3
        THEN 'WORKBENCH_SOURCE_OWNER_V3' ELSE 'WORKBENCH_SOURCE_OWNER_V2' END,
      v_session_id::text,
      COALESCE(v_session_row.version, 0)::text,
      v_session_row.source_snapshot_run_id::text,
      v_session_signature_token,
      p_candidate_id::text,
      COALESCE(v_source_change_seq, 0)::text,
      COALESCE(v_registry_dirty_generation, 0)::text,
      UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL'))),
      'FULL_CANDIDATE',
      CASE WHEN v_authority_fingerprint_version=3
        THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
      CASE WHEN v_authority_fingerprint_version=3
        THEN v_required_physical_publication_contract_version::text ELSE NULL END
    );
    v_authority_fingerprint := pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_authority_fingerprint_text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_source_build_hash := substr(v_authority_fingerprint, 1, 32);
    v_source_build_run_id := (
      substr(v_source_build_hash, 1, 8) || '-' ||
      substr(v_source_build_hash, 9, 4) || '-' ||
      substr(v_source_build_hash, 13, 4) || '-' ||
      substr(v_source_build_hash, 17, 4) || '-' ||
      substr(v_source_build_hash, 21, 12)
    )::uuid;

    v_dedupe_key := CASE WHEN v_authority_fingerprint_version=3
        THEN 'WORKBENCH_SOURCE_OWNER_V3:' ELSE 'WORKBENCH_SOURCE_OWNER_V2:' END
      || v_authority_fingerprint
      || ':cursor:' || v_cursor_token;

    v_payload_out_json := jsonb_strip_nulls(
      v_payload_json
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'resolved_mode', 'LEGACY',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'reason', v_reason,
        'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'workbench_session_id', v_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'session_signature_token', v_session_signature_token
      )
      || jsonb_build_object(
        'candidate_id', p_candidate_id::text,
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_change_sequence', COALESCE(v_source_change_seq, 0),
        'source_build_run_id', v_source_build_run_id::text,
        'refresh_scope_kind', v_refresh_scope_kind,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)
      )
      || jsonb_build_object(
        'pay_channel_scope', v_pay_channel_scope,
        'source_build_required', true,
        'line_work_required', true,
        'line_work_only', false,
        'delta_refresh_required', false,
        'source_build_action', 'BUILD_SOURCE',
        'line_work_action', 'SOURCE_BUILD_THEN_SEED',
        'source_build_limit', v_stage_limit,
        'limit', v_stage_limit,
        'source_cursor', COALESCE(v_initial_cursor_json, '{}'::jsonb),
        'cursor_json', COALESCE(v_initial_cursor_json, '{}'::jsonb),
        'cursor_token', v_cursor_token
      )
      || jsonb_build_object(
        'trigger_table', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json#>>'{trigger,table}', '')), ''),
        'trigger_operation', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json->>'trigger_op', v_payload_json#>>'{trigger,operation}', '')), ''),
        'trigger_op', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_op', v_payload_json->>'trigger_operation', v_payload_json#>>'{trigger,operation}', '')), ''),
        'dedupe_key', v_dedupe_key,
        'authority_fingerprint_version', v_authority_fingerprint_version,
        'authority_fingerprint', v_authority_fingerprint,
        'source_publication_baseline_required',v_source_publication_baseline_required,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'requested_source_build_run_id', CASE WHEN v_requested_source_build_run_id IS NULL THEN NULL ELSE v_requested_source_build_run_id::text END,
        'created_by_helper', 'pay_workbench_enqueue_candidate_refresh',
        'created_at_utc', v_now::text,
        'classifier_result', v_classifier_result
      )
      || jsonb_build_object(
        'source_build', jsonb_strip_nulls(jsonb_build_object(
          'required', true,
          'run_id', v_source_build_run_id::text,
          'source_build_run_id', v_source_build_run_id::text,
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'session_version', COALESCE(v_session_row.version, 0),
          'refresh_scope_kind', v_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'pay_channel_scope', v_pay_channel_scope,
          'limit', v_stage_limit,
          'cursor', COALESCE(v_initial_cursor_json, '{}'::jsonb),
          'reason', v_reason
          ,'source_publication_baseline_required',v_source_publication_baseline_required
          ,'required_physical_publication_contract_version',v_required_physical_publication_contract_version
        ))
      )
    );
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    -- An active economic build, rather than the lifecycle reason or the root
    -- job's current status, is the durable refresh owner.  A succeeded root
    -- with queued/running stage continuations therefore remains active.
    SELECT build_row.*
    INTO v_owner_build
    FROM private.banking_pay_workbench_economic_builds AS build_row
    WHERE build_row.candidate_id = p_candidate_id
      AND build_row.session_id = v_session_id
      AND build_row.session_version = COALESCE(v_session_row.version, 0)
      AND build_row.source_snapshot_run_id = v_session_row.source_snapshot_run_id
      AND build_row.source_change_seq = COALESCE(v_source_change_seq, 0)
      AND build_row.captured_candidate_generation = COALESCE(v_registry_dirty_generation, 0)
      AND (
        v_source_publication_baseline_required IS NOT TRUE
        OR (
          build_row.authority_fingerprint_version=3
          AND COALESCE((build_row.attestation_json->>'required_physical_publication_contract_version')::integer,0)>=1
        )
      )
      AND UPPER(BTRIM(COALESCE(build_row.status, ''))) IN (
        'COLLECTING',
        'READY_FOR_RECONCILIATION',
        'RECONCILING',
        'RECONCILED',
        'PUBLISHING',
        'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
      )
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS owner_active_job
        WHERE owner_active_job.economic_build_id = build_row.id
          AND UPPER(BTRIM(COALESCE(owner_active_job.status, ''))) IN ('QUEUED', 'RUNNING')
      )
    ORDER BY
      CASE
        WHEN build_row.id = (
          SELECT registry.current_build_id
          FROM private.banking_pay_workbench_candidate_scope_registry AS registry
          WHERE registry.candidate_id = p_candidate_id
        ) THEN 0
        ELSE 1
      END,
      build_row.created_at_utc DESC,
      build_row.id DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      SELECT source_job.*
      INTO v_owner_root_job
      FROM public.banking_pay_workbench_jobs AS source_job
      WHERE source_job.economic_build_id = v_owner_build.id
        AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND COALESCE(
          source_job.payload_json->>'source_build_run_id',
          source_job.payload_json#>>'{source_build,source_build_run_id}',
          ''
        ) = v_owner_build.source_build_run_id::text
      ORDER BY source_job.created_at_utc, source_job.id
      LIMIT 1
      FOR UPDATE;

      IF FOUND THEN
        v_owner_refresh_scope_kind := UPPER(BTRIM(COALESCE(
          v_owner_root_job.payload_json->>'refresh_scope_kind',
          v_owner_root_job.payload_json#>>'{source_build,refresh_scope_kind}',
          ''
        )));
        v_owner_pay_channel_scope := UPPER(BTRIM(COALESCE(
          v_owner_root_job.payload_json->>'pay_channel_scope',
          v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
          'ALL'
        )));
        v_owner_targeted_timesheet_ids_json := CASE
          WHEN jsonb_typeof(v_owner_root_job.payload_json->'targeted_timesheet_ids') = 'array'
            THEN v_owner_root_job.payload_json->'targeted_timesheet_ids'
          WHEN jsonb_typeof(v_owner_root_job.payload_json#>'{source_build,targeted_timesheet_ids}') = 'array'
            THEN v_owner_root_job.payload_json#>'{source_build,targeted_timesheet_ids}'
          ELSE '[]'::jsonb
        END;
        v_owner_linked_timesheet_ids_json := CASE
          WHEN jsonb_typeof(v_owner_root_job.payload_json->'linked_timesheet_ids') = 'array'
            THEN v_owner_root_job.payload_json->'linked_timesheet_ids'
          WHEN jsonb_typeof(v_owner_root_job.payload_json#>'{source_build,linked_timesheet_ids}') = 'array'
            THEN v_owner_root_job.payload_json#>'{source_build,linked_timesheet_ids}'
          ELSE '[]'::jsonb
        END;
        -- Every WORKBENCH_CANDIDATE_SOURCE_BUILD owns complete candidate truth.
        -- Diagnostic refresh scope is retained as provenance, not identity.
        v_owner_covers_request :=
          v_owner_pay_channel_scope = UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL')));
        IF v_owner_covers_request THEN
          v_owner_resolution := 'ACTIVE_CURRENT_OWNER_COVERS_REQUEST';
        END IF;
      END IF;
    END IF;

    -- A cancellation reversion deliberately retains the immutable original
    -- economic-build lineage while publishing a new current source run at the
    -- cancellation sequence/generation.  It is therefore a complete current
    -- authority even though the historical build row itself has an older
    -- source sequence.  Recognise only the exact V3 terminal attestation; an
    -- ordinary V1/V2 or structurally incomplete scope cannot enter this path.
    IF v_owner_resolution='NO_CURRENT_OWNER'
       AND v_scope_state_precedes_job IS TRUE THEN
      SELECT current_scope.*
      INTO v_reversion_scope
      FROM public.banking_pay_workbench_session_scope AS current_scope
      WHERE current_scope.session_id=v_session_id
        AND current_scope.candidate_id=p_candidate_id
        AND current_scope.dirty IS FALSE
        AND current_scope.pending_job_id IS NULL
        AND current_scope.certified_preview_publication_required IS TRUE
        AND current_scope.certified_preview_publication_parity_ok IS TRUE
        AND current_scope.certified_preview_publication_session_version=COALESCE(v_session_row.version,0)
        AND current_scope.certified_preview_publication_source_change_seq=COALESCE(v_source_change_seq,0)
        AND current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              ='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
        AND current_scope.certified_preview_publication_attestation_json->>'contract_version'='3'
        AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              ='READY_TO_PAY_SEMANTIC_V2'
        AND current_scope.certified_preview_publication_attestation_json->>'authority_kind'
              ='CERTIFIED_CANCELLATION_REVERSION'
        AND COALESCE((current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,false)
        AND COALESCE((current_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,false)
        AND (
          NOT v_source_publication_baseline_required
          OR (
            current_scope.certified_preview_publication_source_publication_id IS NOT NULL
            AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
                  =current_scope.certified_preview_publication_source_publication_id::text
          )
        )
      FOR UPDATE;

      IF FOUND THEN
        v_reversion_attestation:=COALESCE(
          v_reversion_scope.certified_preview_publication_attestation_json,'{}'::jsonb
        );

        SELECT current_state.*
        INTO v_reversion_state
        FROM public.banking_pay_workbench_session_candidate_state AS current_state
        WHERE current_state.session_id=v_session_id
          AND current_state.candidate_id=p_candidate_id
          AND UPPER(BTRIM(COALESCE(current_state.status,'')))='READY'
          AND current_state.pending_job_id IS NULL
          AND current_state.session_version=COALESCE(v_session_row.version,0)
          AND current_state.source_change_seq=COALESCE(v_source_change_seq,0)
        FOR UPDATE;
        v_reversion_state_exact:=FOUND;

        SELECT COUNT(*)::integer
        INTO v_reversion_source_count
        FROM public.banking_pay_workbench_candidate_source_lines AS reversion_source
        WHERE reversion_source.session_id=v_session_id
          AND reversion_source.candidate_id=p_candidate_id
          AND reversion_source.session_version=COALESCE(v_session_row.version,0)
          AND reversion_source.source_change_seq=COALESCE(v_source_change_seq,0)
          AND reversion_source.source_publication_id
                =v_reversion_scope.certified_preview_publication_source_publication_id
          AND reversion_source.status='CURRENT';

        IF v_reversion_state_exact
           AND COALESCE(v_registry_source_change_seq_after,v_source_change_seq,0)
                =COALESCE(v_source_change_seq,0)
           AND COALESCE(v_registry_dirty_generation,0)=COALESCE(v_live_scope_change_generation,0)
           AND COALESCE(v_reversion_attestation->>'source_row_count','') ~ '^[0-9]{1,9}$'
           AND v_reversion_source_count=(v_reversion_attestation->>'source_row_count')::integer
           AND NOT EXISTS (
             SELECT 1
             FROM public.banking_pay_workbench_candidate_source_lines AS foreign_current_source
             WHERE foreign_current_source.session_id=v_session_id
               AND foreign_current_source.candidate_id=p_candidate_id
               AND foreign_current_source.status='CURRENT'
               AND (
                 foreign_current_source.session_version IS DISTINCT FROM COALESCE(v_session_row.version,0)
                 OR foreign_current_source.source_change_seq IS DISTINCT FROM COALESCE(v_source_change_seq,0)
                 OR foreign_current_source.source_publication_id IS DISTINCT FROM
                      v_reversion_scope.certified_preview_publication_source_publication_id
               )
            ) THEN
          EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
            INTO v_physical_currentness
            USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
              jsonb_build_object('contract_version',1,'allow_active_owner',false);
          v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
          IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) THEN
          RETURN jsonb_strip_nulls(jsonb_build_object(
            'ok',true,
            'job_id',NULL,
            'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'session_id',v_session_id::text,
            'candidate_id',p_candidate_id::text,
            'session_version',COALESCE(v_session_row.version,0),
            'source_change_seq',COALESCE(v_source_change_seq,0),
            'registry_source_change_seq',COALESCE(v_registry_source_change_seq_after,v_source_change_seq,0),
            'source_build_run_id',v_reversion_scope.certified_preview_publication_source_build_run_id::text,
            'source_publication_id',v_reversion_scope.certified_preview_publication_source_publication_id::text,
            'authority_fingerprint',v_authority_fingerprint,
            'authority_fingerprint_version',v_authority_fingerprint_version,
            'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
            'owner_resolution','COMPLETE_CURRENT_AUTHORITY',
            'owner_build_id',v_reversion_attestation->>'economic_build_id',
            'owner_root_job_id',NULL,
            'owner_source_build_run_id',v_reversion_scope.certified_preview_publication_source_build_run_id::text,
            'requested_coverage','FULL_CANDIDATE',
            'owner_coverage','FULL_CANDIDATE',
            'scope_status',v_reversion_scope.status,
            'source_build_required',false,
            'delta_refresh_required',false,
            'coalesced',true,
            'reused',true,
            'new_owner_created',false,
            'no_op',true,
            'stale_preinvalidated_absorb_only',v_stale_preinvalidated_absorb_only,
            'diagnostic_provenance_merged',false,
            'reason',v_reason,
            'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
          ));
          END IF;
        END IF;
      END IF;
    END IF;

    -- A delayed dirty event which revalidates to an already published current
    -- authority is a true no-op.  This branch is deliberately limited to the
    -- caller's proven, already-finalised scope generation.
    IF v_owner_resolution = 'NO_CURRENT_OWNER'
       AND v_scope_state_precedes_job IS TRUE THEN
      SELECT build_row.*
      INTO v_owner_build
      FROM private.banking_pay_workbench_economic_builds AS build_row
      JOIN public.banking_pay_workbench_session_scope AS current_scope
        ON current_scope.session_id = build_row.session_id
       AND current_scope.candidate_id = build_row.candidate_id
      JOIN public.banking_pay_workbench_session_candidate_state AS current_state
        ON current_state.session_id = build_row.session_id
       AND current_state.candidate_id = build_row.candidate_id
      WHERE build_row.candidate_id = p_candidate_id
        AND build_row.session_id = v_session_id
        AND build_row.session_version = COALESCE(v_session_row.version, 0)
        AND build_row.source_snapshot_run_id = v_session_row.source_snapshot_run_id
        AND build_row.source_change_seq = COALESCE(v_source_change_seq, 0)
        AND build_row.captured_candidate_generation = COALESCE(v_registry_dirty_generation, 0)
        AND UPPER(BTRIM(COALESCE(build_row.status, ''))) = 'COMPLETE'
        AND UPPER(BTRIM(COALESCE(build_row.private_stage, ''))) = 'COMPLETE'
        AND current_scope.dirty IS FALSE
        AND current_scope.pending_job_id IS NULL
        AND current_scope.certified_preview_publication_required IS TRUE
        AND current_scope.certified_preview_publication_parity_ok IS TRUE
        AND current_scope.certified_preview_publication_session_version = build_row.session_version
        AND current_scope.certified_preview_publication_source_change_seq = build_row.source_change_seq
        AND current_scope.certified_preview_publication_source_build_run_id = build_row.source_build_run_id
        AND (
          NOT v_source_publication_baseline_required
          OR (
            current_scope.certified_preview_publication_source_publication_id IS NOT NULL
            AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
                  =current_scope.certified_preview_publication_source_publication_id::text
          )
        )
        AND (
          v_semantic_ready_publication_enabled IS NOT TRUE
          OR (
            current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
            AND current_scope.certified_preview_publication_attestation_json->>'contract_version' = '3'
            AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              = 'READY_TO_PAY_SEMANTIC_V2'
            AND COALESCE(
              (current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,
              false
            )
          )
        )
        AND UPPER(BTRIM(COALESCE(current_state.status, ''))) = 'READY'
        AND current_state.pending_job_id IS NULL
        AND current_state.session_version = build_row.session_version
        AND current_state.source_change_seq = build_row.source_change_seq
      ORDER BY build_row.completed_at_utc DESC NULLS LAST, build_row.id DESC
      LIMIT 1
      FOR UPDATE OF build_row, current_scope, current_state;

      IF FOUND THEN
        SELECT source_job.*
        INTO v_owner_root_job
        FROM public.banking_pay_workbench_jobs AS source_job
        WHERE source_job.economic_build_id = v_owner_build.id
          AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND COALESCE(
            source_job.payload_json->>'source_build_run_id',
            source_job.payload_json#>>'{source_build,source_build_run_id}',
            ''
          ) = v_owner_build.source_build_run_id::text
        ORDER BY source_job.created_at_utc, source_job.id
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
          v_owner_refresh_scope_kind := UPPER(BTRIM(COALESCE(
            v_owner_root_job.payload_json->>'refresh_scope_kind',
            v_owner_root_job.payload_json#>>'{source_build,refresh_scope_kind}',
            ''
          )));
          v_owner_pay_channel_scope := UPPER(BTRIM(COALESCE(
            v_owner_root_job.payload_json->>'pay_channel_scope',
            v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
            'ALL'
          )));
          v_owner_covers_request :=
            v_owner_pay_channel_scope = UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL')));
          IF v_owner_covers_request THEN
            EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
              INTO v_physical_currentness
              USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
                jsonb_build_object('contract_version',1,'allow_active_owner',false);
            v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
            IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) THEN
              v_owner_resolution := 'COMPLETE_CURRENT_AUTHORITY';
            END IF;
          END IF;
        END IF;
      END IF;
    END IF;

    IF v_owner_resolution IN (
      'ACTIVE_CURRENT_OWNER_COVERS_REQUEST',
      'COMPLETE_CURRENT_AUTHORITY'
    ) THEN
      SELECT COALESCE(jsonb_agg(bounded_reason.reason ORDER BY bounded_reason.reason), '[]'::jsonb)
      INTO v_owner_reasons_json
      FROM (
        SELECT DISTINCT reason_value.reason
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_owner_root_job.payload_json->'reasons') = 'array'
              THEN v_owner_root_job.payload_json->'reasons'
            WHEN NULLIF(BTRIM(COALESCE(v_owner_root_job.payload_json->>'reason', '')), '') IS NOT NULL
              THEN jsonb_build_array(v_owner_root_job.payload_json->>'reason')
            ELSE '[]'::jsonb
          END || jsonb_build_array(v_reason)
        ) AS reason_value(reason)
        WHERE NULLIF(BTRIM(reason_value.reason), '') IS NOT NULL
        ORDER BY reason_value.reason
        LIMIT 16
      ) AS bounded_reason;

      SELECT COALESCE(jsonb_agg(bounded_source.source ORDER BY bounded_source.source), '[]'::jsonb)
      INTO v_owner_trigger_sources_json
      FROM (
        SELECT DISTINCT source_value.source
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_owner_root_job.payload_json->'trigger_sources') = 'array'
              THEN v_owner_root_job.payload_json->'trigger_sources'
            ELSE '[]'::jsonb
          END || jsonb_build_array(NULLIF(BTRIM(COALESCE(
            v_payload_json->>'trigger_source',
            v_payload_json->>'trigger_table',
            v_payload_json->>'enqueue_origin',
            ''
          )), ''))
        ) AS source_value(source)
        WHERE NULLIF(BTRIM(source_value.source), '') IS NOT NULL
        ORDER BY source_value.source
        LIMIT 16
      ) AS bounded_source;

      v_owner_request_count := GREATEST(
        CASE WHEN COALESCE(v_owner_root_job.payload_json->>'reason_count', '') ~ '^\d+$'
          THEN (v_owner_root_job.payload_json->>'reason_count')::bigint ELSE 1 END,
        CASE WHEN COALESCE(v_owner_root_job.payload_json#>>'{orchestration_provenance,coalesced_request_count}', '') ~ '^\d+$'
          THEN (v_owner_root_job.payload_json#>>'{orchestration_provenance,coalesced_request_count}')::bigint ELSE 1 END
      ) + 1;
      v_owner_provenance_json := jsonb_strip_nulls(
        CASE WHEN jsonb_typeof(v_owner_root_job.payload_json->'orchestration_provenance') = 'object'
          THEN v_owner_root_job.payload_json->'orchestration_provenance' ELSE '{}'::jsonb END
        || jsonb_build_object(
          'primary_reason', COALESCE(v_owner_root_job.payload_json#>>'{orchestration_provenance,primary_reason}', v_owner_root_job.payload_json->>'reason', v_reason),
          'reason_latest', v_reason,
          'reasons', v_owner_reasons_json,
          'trigger_sources', v_owner_trigger_sources_json,
          'coalesced_request_count', v_owner_request_count,
          'last_requested_at_utc', v_now::text,
          'authority_fingerprint_version', v_authority_fingerprint_version,
          'authority_fingerprint', v_authority_fingerprint
        )
      );

      UPDATE public.banking_pay_workbench_jobs AS owner_job
      SET payload_json = jsonb_strip_nulls(
            COALESCE(owner_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'reason_latest', v_reason,
              'reason_count', v_owner_request_count,
              'reasons', v_owner_reasons_json,
              'trigger_sources', v_owner_trigger_sources_json,
              'orchestration_provenance', v_owner_provenance_json
            )
          ),
          updated_at_utc = v_now
      WHERE owner_job.id = v_owner_root_job.id;

      SELECT active_job.id
      INTO v_owner_active_job_id
      FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.economic_build_id = v_owner_build.id
        AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
      ORDER BY CASE WHEN UPPER(BTRIM(active_job.status)) = 'RUNNING' THEN 0 ELSE 1 END,
               active_job.run_at_utc,
               active_job.created_at_utc,
               active_job.id
      LIMIT 1;

      SELECT scope_row.status
      INTO v_owner_scope_status
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = v_session_id
        AND scope_row.candidate_id = p_candidate_id;

      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok', true,
        'job_id', v_owner_root_job.id::text,
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'registry_source_change_seq', COALESCE(v_registry_source_change_seq_after, v_source_change_seq, 0),
        'source_build_run_id', v_owner_build.source_build_run_id::text,
        'requested_source_build_run_id', CASE WHEN v_requested_source_build_run_id IS NULL THEN NULL ELSE v_requested_source_build_run_id::text END,
        'authority_fingerprint', v_authority_fingerprint,
        'authority_fingerprint_version', v_authority_fingerprint_version,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution', v_owner_resolution,
        'owner_build_id', v_owner_build.id::text,
        'owner_root_job_id', v_owner_root_job.id::text,
        'owner_active_job_id', CASE WHEN v_owner_active_job_id IS NULL THEN NULL ELSE v_owner_active_job_id::text END,
        'owner_source_build_run_id', v_owner_build.source_build_run_id::text,
        'requested_coverage', 'FULL_CANDIDATE',
        'owner_coverage', 'FULL_CANDIDATE',
        'scope_status', COALESCE(v_owner_scope_status, CASE WHEN v_owner_resolution = 'COMPLETE_CURRENT_AUTHORITY' THEN 'MATERIALISED' ELSE 'SOURCE_BUILD_PENDING' END),
        'source_build_required', v_owner_resolution <> 'COMPLETE_CURRENT_AUTHORITY',
        'delta_refresh_required', false,
        'coalesced', true,
        'reused', true,
        'new_owner_created', false,
        'no_op', v_owner_resolution = 'COMPLETE_CURRENT_AUTHORITY',
        'stale_preinvalidated_absorb_only', v_stale_preinvalidated_absorb_only,
        'diagnostic_provenance_merged', true,
        'reason', v_reason,
        'reason_count', v_owner_request_count,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  IF v_stale_preinvalidated_absorb_only THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT'
      USING ERRCODE='40001', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT',
        'candidate_id',p_candidate_id,
        'payload_scope_change_generation',v_payload_scope_change_generation,
        'live_scope_change_generation',v_live_scope_change_generation,
        'registry_dirty_generation',v_registry_dirty_generation,
        'source_change_seq',v_source_change_seq,
        'registry_source_change_seq',v_registry_source_change_seq_after,
        'owner_resolution',v_owner_resolution
      )::text;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    SELECT existing_delta_job.*
    INTO v_existing_delta_job
    FROM public.banking_pay_workbench_jobs AS existing_delta_job
    WHERE existing_delta_job.session_id = v_session_id
      AND existing_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(existing_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(existing_delta_job.status, ''))) = 'QUEUED'
      AND (
        COALESCE(existing_delta_job.payload_json->>'normalised_delta_family_key', existing_delta_job.payload_json->>'delta_family_key', existing_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
        OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                existing_delta_job.session_id::text,
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                existing_delta_job.candidate_id::text,
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
      )
      AND lower(BTRIM(COALESCE(existing_delta_job.payload_json->>'force_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND lower(BTRIM(COALESCE(existing_delta_job.payload_json->>'force_broad_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    ORDER BY
      CASE
        WHEN COALESCE(existing_delta_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (existing_delta_job.payload_json->>'source_change_seq')::bigint
        ELSE 0::bigint
      END DESC,
      existing_delta_job.priority ASC,
      existing_delta_job.run_at_utc ASC,
      existing_delta_job.created_at_utc ASC,
      existing_delta_job.id ASC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      -- Reused queued latest-state heads must not inherit an old projection/cursor identity.
      v_existing_delta_projection_run_id_text := NULLIF(BTRIM(COALESCE(v_existing_delta_job.payload_json->>'projection_run_id', '')), '');
      v_projection_run_id := gen_random_uuid();

      v_existing_delta_source_change_seq := GREATEST(
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
      );
      v_existing_delta_event_count := CASE
        WHEN COALESCE(v_existing_delta_job.payload_json->>'coalesced_event_count', '') ~ '^[0-9]{1,9}$'
          THEN GREATEST((v_existing_delta_job.payload_json->>'coalesced_event_count')::integer, 1)
        ELSE 1
      END;
      v_merged_delta_event_count := v_existing_delta_event_count + 1;

      v_payload_out_json := public._pay_workbench_merge_targeted_scope_payload(
        COALESCE(v_existing_delta_job.payload_json, '{}'::jsonb),
        COALESCE(v_payload_out_json, '{}'::jsonb)
      ) || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'projection_mode', COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'),
        'projection_class', v_projection_class,
        'phase', 'INIT_PREFLIGHT',
        'source_build_required', false,
        'line_work_required', false,
        'delta_refresh_required', true,
        'legacy_fallback_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      );

      v_source_change_seq := GREATEST(
        COALESCE(v_live_change_seq, 0),
        COALESCE(v_source_change_seq, 0),
        COALESCE(v_existing_delta_source_change_seq, 0),
        CASE WHEN COALESCE(v_payload_out_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_payload_out_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_payload_out_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'source_change_sequence')::bigint ELSE 0::bigint END
      );

      SELECT COALESCE(jsonb_agg(merged_target_ids.timesheet_id_text ORDER BY merged_target_ids.timesheet_id_text), '[]'::jsonb)
      INTO v_targeted_timesheet_ids_json
      FROM (
        SELECT DISTINCT NULLIF(BTRIM(targeted_values.value), '') AS timesheet_id_text
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_payload_out_json->'targeted_timesheet_ids') = 'array' THEN v_payload_out_json->'targeted_timesheet_ids'
            WHEN jsonb_typeof(v_payload_out_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_out_json->>'targeted_timesheet_ids')
            ELSE '[]'::jsonb
          END
        ) AS targeted_values(value)
        WHERE NULLIF(BTRIM(targeted_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS merged_target_ids;

      SELECT COALESCE(jsonb_agg(merged_linked_ids.timesheet_id_text ORDER BY merged_linked_ids.timesheet_id_text), '[]'::jsonb)
      INTO v_linked_timesheet_ids_json
      FROM (
        SELECT DISTINCT NULLIF(BTRIM(linked_values.value), '') AS timesheet_id_text
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_payload_out_json->'linked_timesheet_ids') = 'array' THEN v_payload_out_json->'linked_timesheet_ids'
            WHEN jsonb_typeof(v_payload_out_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_out_json->>'linked_timesheet_ids')
            ELSE '[]'::jsonb
          END
        ) AS linked_values(value)
        WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS merged_linked_ids;

      v_delta_ids_hash := md5(
        COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb)::text
        || ':'
        || COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb)::text
      );

      v_delta_coalescing_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
    || ':session:' || COALESCE(v_session_id::text, 'none')
    || ':version:' || COALESCE(COALESCE(v_session_row.version, 0), 0)::text
    || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''), 'DELTA'))), ''), 'DELTA')
    || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(v_projection_class, ''), 'UNKNOWN'))), ''), 'UNKNOWN')
    || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''), 'TARGETED_TIMESHEETS'))), ''), 'TARGETED_TIMESHEETS')
    || ':candidate:' || COALESCE(p_candidate_id::text, 'none')
    || ':timesheets:' || COALESCE(v_delta_ids_hash, md5('[]:[]'));
      v_delta_coalescing_hash := md5(v_delta_coalescing_key);

      v_payload_out_json := jsonb_strip_nulls(
        (COALESCE(v_payload_out_json, '{}'::jsonb) - ARRAY[
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
          'source_result_next_cursor_present'
        ]::text[])
        || jsonb_build_object(
          'cursor_json', '{}'::jsonb,
          'continuation', false,
          'phase', 'INIT_PREFLIGHT',
          'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END,
          'normalised_delta_family_key', v_delta_coalescing_key,
          'delta_family_key', v_delta_coalescing_key,
          'delta_coalescing_key', v_delta_coalescing_key,
          'delta_coalescing_hash', v_delta_coalescing_hash
        )
      );

      SELECT running_delta_job.id
      INTO v_delta_active_running_job_id
      FROM public.banking_pay_workbench_jobs AS running_delta_job
      WHERE running_delta_job.session_id = v_session_id
        AND running_delta_job.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
        AND running_delta_job.id IS DISTINCT FROM v_existing_delta_job.id
        AND (
          COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
          OR (
              SELECT
                'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
                || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  running_delta_job.session_id::text,
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                  ''
                )), ''), 'none')
                || ':version:' || COALESCE(
                  CASE
                    WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                      THEN delta_family_values.session_version_text::bigint
                    ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                  END,
                  0
                )::text
                || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_mode_text, ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))), ''), 'DELTA')
                || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_class_text, ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))), ''), 'UNKNOWN')
                || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                  CASE
                    WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                      OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                      THEN 'TARGETED_TIMESHEETS'
                    ELSE 'CANDIDATE_FULL_LIVE'
                  END
                ))), ''), 'CANDIDATE_FULL_LIVE')
                || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  running_delta_job.candidate_id::text,
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                  ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                  ''
                )), ''), 'none')
                || ':timesheets:' || md5(
                  COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                  || ':'
                  || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
                )
              FROM (
                SELECT
                  NULLIF(BTRIM(COALESCE(
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                    CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                    '0'
                  )), '') AS session_version_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                    NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                    'DELTA'
                  ))) AS projection_mode_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                    NULLIF(v_projection_class, ''),
                    'UNKNOWN'
                  ))) AS projection_class_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                    NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                  ))) AS refresh_scope_kind_text
              ) AS delta_family_values
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                        THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                      WHEN NULLIF(BTRIM(COALESCE(
                             (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                             (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                             ''
                           )), '') IS NOT NULL
                        THEN jsonb_build_array(COALESCE(
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                        ))
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_targeted_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_targeted_sorted
              ) AS delta_family_targeted
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                        THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_linked_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_linked_sorted
              ) AS delta_family_linked
            ) = v_delta_coalescing_key
        )
      ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
      LIMIT 1;

      v_dedupe_key := CASE
        WHEN v_delta_active_running_job_id IS NOT NULL THEN
          v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text
        ELSE
          v_delta_coalescing_key
      END;

      v_payload_out_json := jsonb_strip_nulls(
        v_payload_out_json
        || jsonb_build_object(
          'dedupe_key', v_dedupe_key,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'queue_identity_targeted_timesheet_ids', COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb),
          'queue_identity_linked_timesheet_ids', COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb),
          'projection_run_id', v_projection_run_id::text,
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'source_change_sequence', COALESCE(v_source_change_seq, 0),
          'latest_source_change_seq', COALESCE(v_source_change_seq, 0),
          'delta_coalescing_key', v_delta_coalescing_key,
          'delta_family_key', v_delta_coalescing_key,
          'normalised_delta_family_key', v_delta_coalescing_key,
          'delta_coalescing_hash', v_delta_coalescing_hash,
          'coalesced_event_count', GREATEST(COALESCE(v_merged_delta_event_count, 1), 1),
          'coalesced_source_change_seqs', jsonb_build_array(COALESCE(v_existing_delta_source_change_seq, 0), COALESCE(v_source_change_seq, 0)),
          'latest_event_at_utc', v_now::text,
          'scope_merge_applied', true,
          'scope_merge_at_utc', v_now::text,
          'cursor_json', '{}'::jsonb,
          'continuation', false,
          'phase', 'INIT_PREFLIGHT',
          'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END
        )
      );

      UPDATE public.banking_pay_workbench_jobs AS existing_delta_update
      SET dedupe_key = v_dedupe_key,
          priority = LEAST(existing_delta_update.priority, 43),
          run_at_utc = LEAST(existing_delta_update.run_at_utc, v_now),
          payload_json = v_payload_out_json,
          updated_at_utc = v_now
      WHERE existing_delta_update.id = v_existing_delta_job.id
      RETURNING existing_delta_update.id,
                existing_delta_update.status,
                false
      INTO v_job_id,
           v_job_status,
           v_job_was_inserted;

      v_delta_merge_reused_existing := true;
    END IF;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_payload_out_json := jsonb_strip_nulls(
      (COALESCE(v_payload_out_json, '{}'::jsonb) - ARRAY[
        'old_row_json',
        'new_row_json',
        'old_row',
        'new_row',
        'source_row_json',
        'work_payload_json',
        'result_row_json',
        'preview_row_json',
        'row_payload_json',
        'line_payload_json',
        'projection_rows',
        'projected_rows',
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
        'source_result_next_cursor_present'
      ]::text[])
      || jsonb_build_object(
        'continuation', false,
        'cursor_json', '{}'::jsonb,
        'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END,
        'phase', 'INIT_PREFLIGHT'
      )
    );
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    UPDATE public.banking_pay_workbench_jobs AS queued_delta_job
    SET status = 'SUCCEEDED',
        completed_at_utc = v_now,
        updated_at_utc = v_now,
        payload_json = jsonb_strip_nulls(
          COALESCE(queued_delta_job.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_legacy', true,
            'superseded_by_legacy_at_utc', v_now::text,
            'superseded_reason', COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'LEGACY_REFRESH_WINS'),
            'legacy_source_change_seq', COALESCE(v_source_change_seq, 0)
          )
        )
    WHERE queued_delta_job.session_id = v_session_id
      AND queued_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(queued_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(queued_delta_job.status, ''))) = 'QUEUED';
    GET DIAGNOSTICS v_delta_jobs_superseded = ROW_COUNT;
  END IF;

  IF v_job_id IS NULL THEN
    INSERT INTO public.banking_pay_workbench_jobs AS enqueue_job (
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
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 43 ELSE 44 END,
      v_now,
      0,
      8,
      v_dedupe_key,
      p_snapshot_run_id,
      v_session_id,
      p_candidate_id,
      v_payload_out_json,
      NULL::uuid,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
      '{}'::jsonb,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 1 ELSE NULL::integer END,
      v_now,
      v_now,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::jsonb
    )
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(enqueue_job.priority, EXCLUDED.priority),
        run_at_utc = LEAST(enqueue_job.run_at_utc, EXCLUDED.run_at_utc),
        payload_json = CASE
          WHEN EXCLUDED.job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
            jsonb_strip_nulls(
              (public._pay_workbench_merge_targeted_scope_payload(
                COALESCE(enqueue_job.payload_json, '{}'::jsonb),
                COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
              ) - ARRAY[
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
                'source_result_next_cursor_present'
              ]::text[])
              || jsonb_build_object(
                'cursor_json', '{}'::jsonb,
                'continuation', false,
                'phase', 'INIT_PREFLIGHT',
                'run_mode', CASE
                  WHEN lower(BTRIM(COALESCE(EXCLUDED.payload_json->>'waiting_after_running_delta_job', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'LATEST_RERUN_AFTER_RUNNING'
                  ELSE 'LATEST_STATE_HEAD'
                END,
                'latest_source_change_seq', GREATEST(
                  CASE WHEN COALESCE(enqueue_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (enqueue_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(enqueue_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (enqueue_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END
                )
              )
            )
          ELSE public._pay_workbench_merge_targeted_scope_payload(
            COALESCE(enqueue_job.payload_json, '{}'::jsonb),
            COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
          )
        END,
        updated_at_utc = v_now
    WHERE NOT (
      UPPER(BTRIM(COALESCE(enqueue_job.status, ''))) = 'RUNNING'
      AND EXCLUDED.job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    )
    RETURNING enqueue_job.id,
              enqueue_job.status,
              (xmax = 0)
    INTO v_job_id, v_job_status, v_job_was_inserted;

    GET DIAGNOSTICS v_insert_row_count = ROW_COUNT;

    IF COALESCE(v_insert_row_count, 0) = 0
       AND v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
      SELECT running_delta_job.id
      INTO v_delta_active_running_job_id
      FROM public.banking_pay_workbench_jobs AS running_delta_job
      WHERE running_delta_job.session_id = v_session_id
        AND running_delta_job.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
        AND (
          COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
          OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.session_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.candidate_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
          OR running_delta_job.dedupe_key = v_dedupe_key
        )
      ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
      LIMIT 1;

      IF v_delta_active_running_job_id IS NOT NULL THEN
        v_dedupe_key := v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text;

        v_payload_out_json := jsonb_strip_nulls(
          COALESCE(v_payload_out_json, '{}'::jsonb)
          || jsonb_build_object(
            'dedupe_key', v_dedupe_key,
            'delta_active_running_job_id', v_delta_active_running_job_id::text,
            'waiting_after_running_delta_job', true,
            'waiting_after_running_enqueued_at_utc', v_now::text,
            'delta_running_conflict_fail_closed', true
          )
        );

        INSERT INTO public.banking_pay_workbench_jobs AS waiting_enqueue_job (
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
          43,
          v_now,
          0,
          8,
          v_dedupe_key,
          p_snapshot_run_id,
          v_session_id,
          p_candidate_id,
          v_payload_out_json,
          NULL::uuid,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
          '{}'::jsonb,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 1 ELSE NULL::integer END,
          v_now,
          v_now,
          NULL::timestamptz,
          NULL::timestamptz,
          NULL::timestamptz,
          NULL::jsonb
        )
        ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
        DO UPDATE
        SET priority = LEAST(waiting_enqueue_job.priority, EXCLUDED.priority),
            run_at_utc = LEAST(waiting_enqueue_job.run_at_utc, EXCLUDED.run_at_utc),
            payload_json = jsonb_strip_nulls(
              (public._pay_workbench_merge_targeted_scope_payload(
                COALESCE(waiting_enqueue_job.payload_json, '{}'::jsonb),
                COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
              ) - ARRAY[
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
                'source_result_next_cursor_present'
              ]::text[])
              || jsonb_build_object(
                'cursor_json', '{}'::jsonb,
                'continuation', false,
                'phase', 'INIT_PREFLIGHT',
                'run_mode', 'LATEST_RERUN_AFTER_RUNNING',
                'waiting_after_running_delta_job', true,
                'latest_source_change_seq', GREATEST(
                  CASE WHEN COALESCE(waiting_enqueue_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (waiting_enqueue_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(waiting_enqueue_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (waiting_enqueue_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END
                )
              )
            ),
            updated_at_utc = v_now
        WHERE UPPER(BTRIM(COALESCE(waiting_enqueue_job.status, ''))) <> 'RUNNING'
        RETURNING waiting_enqueue_job.id,
                  waiting_enqueue_job.status,
                  (xmax = 0)
        INTO v_job_id, v_job_status, v_job_was_inserted;

        GET DIAGNOSTICS v_insert_row_count = ROW_COUNT;
      END IF;
    END IF;

    IF v_job_id IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_ACTIVE_DELTA_HEAD_CONFLICT'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_ACTIVE_DELTA_HEAD_CONFLICT',
                'session_id', v_session_id::text,
                'candidate_id', p_candidate_id::text,
                'job_type', v_job_type,
                'delta_coalescing_key', v_delta_coalescing_key,
                'dedupe_key', v_dedupe_key,
                'active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
                'message', 'A running delta refresh head was detected during enqueue conflict handling and a safe waiting-head could not be created.'
              )::text;
    END IF;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    v_owner_resolution := CASE
      WHEN v_job_was_inserted THEN 'NEW_OWNER_CREATED'
      ELSE 'ACTIVE_ROOT_JOB_REUSED'
    END;
  END IF;

  UPDATE public.banking_pay_workbench_session_scope AS session_scope
  SET status = CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 'DELTA_REFRESH_PENDING' ELSE 'SOURCE_BUILD_PENDING' END,
      pending_job_id = v_job_id,
      dirty = true,
      error_json = NULL::jsonb,
      updated_at_utc = v_now
  WHERE session_scope.session_id = v_session_id
    AND session_scope.candidate_id = p_candidate_id;

  /*
    The certified publisher requires one session-candidate state row carrying
    the exact session version and source sequence owned by this refresh.  A
    freshly seeded or force-reseeded scope can legitimately have no state row
    yet.  Establish that pending owner here, alongside the canonical job/scope
    enqueue, rather than allowing the completed build to reach publication
    with no state authority to adopt.

    Existing fragments are deliberately retained on conflict for display
    continuity, but they are no longer READY authority while this job owns the
    candidate.  A state row from a newer source sequence or session version is
    never overwritten by an older enqueue.
  */
  INSERT INTO public.banking_pay_workbench_session_candidate_state AS candidate_state (
    session_id,
    candidate_id,
    status,
    source_change_seq,
    session_version,
    pending_job_id,
    created_at_utc,
    updated_at_utc,
    last_error_json
  )
  VALUES (
    v_session_id,
    p_candidate_id,
    'PENDING',
    COALESCE(v_source_change_seq, 0),
    COALESCE(v_session_row.version, 1),
    v_job_id,
    v_now,
    v_now,
    NULL::jsonb
  )
  ON CONFLICT (session_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = EXCLUDED.source_change_seq,
      session_version = EXCLUDED.session_version,
      pending_job_id = EXCLUDED.pending_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL::jsonb
  WHERE candidate_state.source_change_seq <= EXCLUDED.source_change_seq
    AND candidate_state.session_version <= EXCLUDED.session_version;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    v_job_id::text,
    CASE WHEN v_job_was_inserted THEN 'QUEUED' ELSE 'REUSED' END,
    NULL::jsonb,
    jsonb_build_object(
      'id', v_job_id::text,
      'job_type', v_job_type,
    'resolved_mode', v_resolved_mode,
      'status', v_job_status,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', v_session_id::text,
      'candidate_id', p_candidate_id::text,
      'dedupe_key', v_dedupe_key,
      'source_change_seq', COALESCE(v_source_change_seq, 0),
      'source_build_run_id', CASE WHEN v_source_build_run_id IS NULL THEN NULL ELSE v_source_build_run_id::text END,
      'projection_run_id', CASE WHEN v_projection_run_id IS NULL THEN NULL ELSE v_projection_run_id::text END,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_count', jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)),
      'linked_timesheet_count', jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)),
      'queue_identity_targeted_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
      'queue_identity_linked_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
      'source_build_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'line_work_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'delta_scope_merge_reused_existing', COALESCE(v_delta_merge_reused_existing, false),
      'delta_coalescing_key', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_key ELSE NULL::text END,
      'delta_coalescing_hash', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_hash ELSE NULL::text END,
      'delta_active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
      'delta_jobs_superseded_by_legacy', COALESCE(v_delta_jobs_superseded, 0)
    ),
    'WORKBENCH_CANDIDATE_REFRESH_JOB_ENQUEUE',
    v_actor_user_id
  );

  RETURN jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', v_job_type,
    'canonical_job_type', v_job_type,
    'resolved_mode', v_resolved_mode,
    'snapshot_run_id', p_snapshot_run_id::text,
    'session_id', v_session_id::text,
    'candidate_id', p_candidate_id::text,
    'session_version', COALESCE(v_session_row.version, 0),
    'source_change_seq', COALESCE(v_source_change_seq, 0),
    'registry_source_change_seq', COALESCE(v_registry_source_change_seq_after, v_source_change_seq, 0),
    'registry_sequence_synchronised', COALESCE(v_registry_sequence_synchronised, false),
    'source_build_run_id', CASE WHEN v_source_build_run_id IS NULL THEN NULL ELSE v_source_build_run_id::text END,
    'projection_run_id', CASE WHEN v_projection_run_id IS NULL THEN NULL ELSE v_projection_run_id::text END,
    'dedupe_key', v_dedupe_key,
    'reason', v_reason,
    'refresh_scope_kind', v_refresh_scope_kind,
    'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
    'queue_identity_targeted_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
    'queue_identity_linked_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
    'pay_channel_scope', v_pay_channel_scope,
    'source_build_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'line_work_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'line_work_only', false,
    'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'scope_status', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 'DELTA_REFRESH_PENDING' ELSE 'SOURCE_BUILD_PENDING' END,
    'projection_mode', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA') ELSE NULL::text END,
    'projection_class', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_projection_class ELSE NULL::text END,
    'full_snapshot_job', false,
    'reused', NOT v_job_was_inserted,
    'coalesced', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND NOT v_job_was_inserted,
    'new_owner_created', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND v_job_was_inserted,
    'owner_resolution', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_owner_resolution ELSE NULL::text END,
    'authority_fingerprint_version', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_authority_fingerprint_version ELSE NULL::integer END,
    'required_physical_publication_contract_version', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_required_physical_publication_contract_version ELSE NULL::integer END,
    'authority_fingerprint', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_authority_fingerprint ELSE NULL::text END,
    'owner_root_job_id', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_job_id::text ELSE NULL::text END,
    'requested_coverage', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'FULL_CANDIDATE' ELSE NULL::text END,
    'owner_coverage', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'FULL_CANDIDATE' ELSE NULL::text END,
    'owner_source_build_run_id', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_source_build_run_id::text ELSE NULL::text END,
    'delta_scope_merge_reused_existing', COALESCE(v_delta_merge_reused_existing, false),
    'delta_coalescing_key', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_key ELSE NULL::text END,
    'delta_coalescing_hash', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_hash ELSE NULL::text END,
    'delta_active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
    'delta_jobs_superseded_by_legacy', COALESCE(v_delta_jobs_superseded, 0),
    'classifier_result', v_classifier_result
  ));
END;
$function$;

ALTER FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb) TO service_role;
