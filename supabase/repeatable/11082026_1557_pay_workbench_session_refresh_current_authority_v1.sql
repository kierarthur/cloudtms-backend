-- Same-session version-only rebase for a clean, already-certified candidate.
--
-- This helper never re-derives economics.  It preserves the exact source JSON,
-- physical preview section/identity, selection and row ids, and changes only
-- the session-version/publication envelope required after an unrelated
-- candidate advanced the open session.  Any ambiguity returns a typed
-- non-rebase result so the caller can use the existing full-build route.
CREATE OR REPLACE FUNCTION private.pay_workbench_candidate_session_version_rebase_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_from_session_version bigint,
  p_to_session_version bigint,
  p_actor_user_id uuid
) RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_from_version bigint;
  v_source_change_seq bigint;
  v_source_build_run_id uuid;
  v_source_publication_id uuid;
  v_source_count integer := 0;
  v_preview_count integer := 0;
  v_source_run_count integer := 0;
  v_source_publication_count integer := 0;
  v_source_minus_preview_count integer := 0;
  v_preview_minus_source_count integer := 0;
  v_active_job_count integer := 0;
  v_existing_target_source_count integer := 0;
  v_existing_target_preview_count integer := 0;
  v_source_identity_digest text;
  v_preview_identity_digest text;
  v_source_digest text;
  v_attestation jsonb := '{}'::jsonb;
  v_currentness jsonb := '{}'::jsonb;
  v_currentness_result jsonb := '{}'::jsonb;
  v_candidate_state_result jsonb := '{}'::jsonb;
  v_invalid_selectable_count integer := 0;
  v_selected_preview_count integer := 0;
  v_selected_session_count integer := 0;
  v_selection_consistent boolean := false;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL
     OR p_from_session_version IS NULL OR p_to_session_version IS NULL
     OR p_actor_user_id IS NULL
     OR p_from_session_version < 1
     OR p_to_session_version <> p_from_session_version + 1 THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_REBASE_ARGUMENT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_REBASE_ACTOR_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.version IS DISTINCT FROM p_to_session_version THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'rebased', false, 'reason', 'SESSION_NOT_REBASEABLE'
    );
  END IF;
  v_from_version := p_from_session_version;

  SELECT registry_row.*
  INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
  WHERE registry_row.candidate_id = p_candidate_id
  FOR UPDATE;

  SELECT scope_row.*
  INTO v_scope
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  SELECT state_row.*
  INTO v_state
  FROM public.banking_pay_workbench_session_candidate_state AS state_row
  WHERE state_row.session_id = p_session_id
    AND state_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF v_registry.candidate_id IS NULL
     OR v_scope.session_id IS NULL
     OR v_state.session_id IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_registry.initialisation_status, ''))) <> 'READY'
     OR v_registry.evaluated_generation IS DISTINCT FROM v_registry.dirty_generation
     OR v_scope.dirty IS NOT FALSE
     OR v_scope.pending_job_id IS NOT NULL
     OR v_state.pending_job_id IS NOT NULL
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_scope.status, '')))
          NOT IN ('MATERIALISED', 'READY', 'COMPLETE', 'SOURCE_EMPTY')
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_state.status, '')))
          NOT IN ('READY', 'COMPLETE')
     OR v_scope.certified_preview_publication_required IS NOT TRUE
     OR v_scope.certified_preview_publication_parity_ok IS NOT TRUE
     OR v_scope.certified_preview_publication_session_version IS DISTINCT FROM v_from_version
     OR v_state.session_version IS DISTINCT FROM v_from_version
     OR v_scope.certified_preview_publication_source_change_seq
          IS DISTINCT FROM v_registry.current_source_change_seq
     OR v_state.source_change_seq IS DISTINCT FROM v_registry.current_source_change_seq
     OR v_scope.certified_preview_publication_attestation_json->>'attestation_version'
          IS DISTINCT FROM 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
     OR v_scope.certified_preview_publication_attestation_json->>'contract_version'
          IS DISTINCT FROM '3'
     OR v_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
          IS DISTINCT FROM 'READY_TO_PAY_SEMANTIC_V2'
     OR COALESCE((v_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean, false) IS NOT TRUE
     OR COALESCE((v_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean, false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'rebased', false, 'reason', 'CANDIDATE_AUTHORITY_NOT_CLEAN_VERSION_ONLY'
    );
  END IF;

  v_source_change_seq := v_registry.current_source_change_seq;

  PERFORM 1
  FROM public.banking_pay_workbench_candidate_source_lines AS source_lock
  WHERE source_lock.session_id = p_session_id
    AND source_lock.candidate_id = p_candidate_id
    AND source_lock.session_version IN (v_from_version, v_session.version)
  ORDER BY source_lock.session_version, source_lock.source_ordinal, source_lock.id
  FOR UPDATE;

  PERFORM 1
  FROM public.banking_pay_workbench_preview_rows AS preview_lock
  WHERE preview_lock.session_id = p_session_id
    AND preview_lock.candidate_id = p_candidate_id
    AND preview_lock.session_version IN (v_from_version, v_session.version)
  ORDER BY preview_lock.session_version, preview_lock.row_ordinal, preview_lock.id
  FOR UPDATE;

  PERFORM 1
  FROM public.banking_pay_workbench_candidate_line_work AS line_lock
  WHERE line_lock.session_id = p_session_id
    AND line_lock.candidate_id = p_candidate_id
  ORDER BY line_lock.line_ordinal, line_lock.id
  FOR UPDATE;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_job_count
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.session_id = p_session_id
    AND job_row.candidate_id = p_candidate_id
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(job_row.status, '')))
          IN ('QUEUED', 'PENDING', 'CLAIMED', 'RUNNING', 'PROCESSING');

  SELECT pg_catalog.count(*)::integer
  INTO v_existing_target_source_count
  FROM public.banking_pay_workbench_candidate_source_lines AS target_source
  WHERE target_source.session_id = p_session_id
    AND target_source.candidate_id = p_candidate_id
    AND target_source.session_version = v_session.version
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(target_source.status, ''))) = 'CURRENT';

  SELECT pg_catalog.count(*)::integer
  INTO v_existing_target_preview_count
  FROM public.banking_pay_workbench_preview_rows AS target_preview
  WHERE target_preview.session_id = p_session_id
    AND target_preview.candidate_id = p_candidate_id
    AND target_preview.session_version = v_session.version
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(target_preview.status, ''))) = 'READY';

  WITH source_rows AS (
    SELECT
      public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS section,
      source_row.line_key,
      source_row.source_ordinal,
      source_row.source_build_run_id,
      source_row.source_publication_id,
      source_row.source_row_json
    FROM public.banking_pay_workbench_candidate_source_lines AS source_row
    WHERE source_row.session_id = p_session_id
      AND source_row.candidate_id = p_candidate_id
      AND source_row.session_version = v_from_version
      AND source_row.source_change_seq = v_source_change_seq
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status, ''))) = 'CURRENT'
  ), preview_rows AS (
    SELECT
      preview_row.section,
      preview_row.row_key,
      (preview_row.row_ordinal - (v_scope.scope_ordinal * 1000000))::bigint AS source_ordinal
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.session_version = v_from_version
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
  )
  SELECT
    (SELECT pg_catalog.count(*)::integer FROM source_rows),
    (SELECT pg_catalog.count(*)::integer FROM preview_rows),
    (SELECT pg_catalog.count(DISTINCT source_build_run_id)::integer FROM source_rows),
    (SELECT pg_catalog.count(DISTINCT source_publication_id)::integer FROM source_rows),
    (SELECT pg_catalog.min(source_build_run_id::text)::uuid FROM source_rows),
    (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
      section || E'\x1f' || line_key || E'\x1f' || source_ordinal::text,
      E'\x1e' ORDER BY source_ordinal, section, line_key
    ), '')) FROM source_rows),
    (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
      section || E'\x1f' || row_key || E'\x1f' || source_ordinal::text,
      E'\x1e' ORDER BY source_ordinal, section, row_key
    ), '')) FROM preview_rows),
    (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
      pg_catalog.md5(source_row_json::text), '' ORDER BY source_ordinal
    ), '')) FROM source_rows),
    (SELECT pg_catalog.count(*)::integer FROM (
      SELECT section, line_key, source_ordinal FROM source_rows
      EXCEPT ALL
      SELECT section, row_key, source_ordinal FROM preview_rows
    ) AS source_diff),
    (SELECT pg_catalog.count(*)::integer FROM (
      SELECT section, row_key, source_ordinal FROM preview_rows
      EXCEPT ALL
      SELECT section, line_key, source_ordinal FROM source_rows
    ) AS preview_diff)
  INTO v_source_count, v_preview_count, v_source_run_count,
       v_source_publication_count, v_source_build_run_id,
       v_source_identity_digest, v_preview_identity_digest, v_source_digest,
       v_source_minus_preview_count, v_preview_minus_source_count;

  IF v_active_job_count <> 0
     OR v_existing_target_source_count <> 0
     OR v_existing_target_preview_count <> 0
     OR v_source_count = 0
     OR v_source_count IS DISTINCT FROM v_preview_count
     OR v_source_run_count <> 1
     OR v_source_publication_count <> 1
     OR v_source_minus_preview_count <> 0
     OR v_preview_minus_source_count <> 0
     OR v_scope.certified_preview_publication_source_build_run_id
          IS DISTINCT FROM v_source_build_run_id
     OR v_scope.certified_preview_publication_attestation_json->>'source_identity_digest'
          IS DISTINCT FROM v_source_identity_digest
     OR v_scope.certified_preview_publication_attestation_json->>'preview_identity_digest'
          IS DISTINCT FROM v_preview_identity_digest
     OR v_scope.certified_preview_publication_attestation_json->>'source_digest'
          IS DISTINCT FROM v_source_digest THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'rebased', false, 'reason', 'VERSION_REBASE_PARITY_NOT_EXACT'
    );
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_invalid_selectable_count
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = v_from_version
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
    AND (
      preview_row.selected IS TRUE
      OR pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
    )
    AND NOT (
      pg_catalog.lower(pg_catalog.btrim(COALESCE(preview_row.row_json->>'draftable', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(preview_row.row_json->>'is_ready_for_draft', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(preview_row.row_json->>'selection_allowed', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
    );

  SELECT pg_catalog.count(*)::integer
  INTO v_selected_preview_count
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = v_from_version
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
    AND preview_row.selected IS TRUE
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state, ''))) = 'SELECTED';

  SELECT pg_catalog.count(*)::integer
  INTO v_selected_session_count
  FROM pg_catalog.jsonb_array_elements_text(
    CASE
      WHEN pg_catalog.jsonb_typeof(COALESCE(v_session.server_selected_preview_row_ids, '[]'::jsonb)) = 'array'
        THEN COALESCE(v_session.server_selected_preview_row_ids, '[]'::jsonb)
      ELSE '[]'::jsonb
    END
  ) AS selected_id(value)
  JOIN public.banking_pay_workbench_preview_rows AS preview_row
    ON selected_id.value = preview_row.id::text
   AND preview_row.session_id = p_session_id
   AND preview_row.candidate_id = p_candidate_id
   AND preview_row.session_version = v_from_version
   AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY';

  v_selection_consistent := v_selected_preview_count = v_selected_session_count
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = p_candidate_id
        AND preview_row.session_version = v_from_version
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
        AND (
          preview_row.selected IS DISTINCT FROM (
            pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
          )
          OR COALESCE((preview_row.row_json->>'selected')::boolean, false)
               IS DISTINCT FROM COALESCE(preview_row.selected, false)
          OR pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.row_json->>'selection_state', '')))
               IS DISTINCT FROM pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.selection_state, '')))
        )
    );

  IF v_invalid_selectable_count <> 0
     OR v_selection_consistent IS NOT TRUE
     OR COALESCE((v_scope.certified_preview_publication_attestation_json->>'invalid_selectable_row_count')::integer, 0) <> 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'rebased', false, 'reason', 'VERSION_REBASE_SELECTION_NOT_EXACT'
    );
  END IF;

  v_source_publication_id := private.pay_workbench_source_publication_identity_v1(
    p_session_id, p_candidate_id, v_session.version,
    v_source_change_seq, v_source_build_run_id
  );

  BEGIN
    DROP TABLE IF EXISTS pg_temp._bpay_same_session_rebase_source;
    CREATE TEMPORARY TABLE pg_temp._bpay_same_session_rebase_source ON COMMIT DROP AS
    SELECT source_row.*
    FROM public.banking_pay_workbench_candidate_source_lines AS source_row
    WHERE source_row.session_id = p_session_id
      AND source_row.candidate_id = p_candidate_id
      AND source_row.session_version = v_from_version
      AND source_row.source_change_seq = v_source_change_seq
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status, ''))) = 'CURRENT';

    UPDATE public.banking_pay_workbench_candidate_source_lines AS old_source
    SET status = 'SUPERSEDED', updated_at_utc = v_now
    WHERE old_source.session_id = p_session_id
      AND old_source.candidate_id = p_candidate_id
      AND old_source.session_version = v_from_version
      AND old_source.source_change_seq = v_source_change_seq
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(old_source.status, ''))) = 'CURRENT';

    INSERT INTO public.banking_pay_workbench_candidate_source_lines (
      session_id, candidate_id, session_version, source_change_seq,
      source_build_run_id, source_publication_id, source_ordinal, line_key,
      parent_line_key, split_suffix, timesheet_id, section, source_row_json,
      economic_key_json, contract_json, pay_channel_scope,
      refresh_scope_kind, status, created_at_utc, updated_at_utc
    )
    SELECT
      old_source.session_id, old_source.candidate_id, v_session.version,
      old_source.source_change_seq, old_source.source_build_run_id,
      v_source_publication_id, old_source.source_ordinal, old_source.line_key,
      old_source.parent_line_key, old_source.split_suffix, old_source.timesheet_id,
      old_source.section, old_source.source_row_json, old_source.economic_key_json,
      COALESCE(old_source.contract_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'authority_kind', 'CERTIFIED_CLONE',
          'invocation_kind', 'SESSION_VERSION_REBASE',
          'same_session_version_rebase_v1', pg_catalog.jsonb_build_object(
            'contract_version', 1,
            'authority_kind', 'CERTIFIED_CLONE',
            'invocation_kind', 'SESSION_VERSION_REBASE',
            'from_session_version', v_from_version,
            'to_session_version', v_session.version,
            'actor_user_id', p_actor_user_id::text,
            'rebased_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
      old_source.pay_channel_scope, 'CLONE_REBASE', 'CURRENT', v_now, v_now
    FROM pg_temp._bpay_same_session_rebase_source AS old_source;

    UPDATE public.banking_pay_workbench_preview_rows AS preview_update
    SET session_version = v_session.version,
        row_json = COALESCE(preview_update.row_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'session_id', p_session_id::text,
            'session_version', v_session.version,
            'same_session_version_rebase_v1', pg_catalog.jsonb_build_object(
              'contract_version', 1,
              'authority_kind', 'CERTIFIED_CLONE',
              'invocation_kind', 'SESSION_VERSION_REBASE',
              'from_session_version', v_from_version,
              'to_session_version', v_session.version,
              'actor_user_id', p_actor_user_id::text,
              'rebased_at_utc', v_now::text,
              'physical_section_preserved', true,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ),
        updated_at_utc = v_now
    WHERE preview_update.session_id = p_session_id
      AND preview_update.candidate_id = p_candidate_id
      AND preview_update.session_version = v_from_version
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_update.status, ''))) = 'READY';

    UPDATE public.banking_pay_workbench_candidate_line_work AS line_update
    SET work_payload_json = COALESCE(line_update.work_payload_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'same_session_version_rebase_v1', pg_catalog.jsonb_build_object(
              'contract_version', 1,
              'authority_kind', 'CERTIFIED_CLONE',
              'invocation_kind', 'SESSION_VERSION_REBASE',
              'from_session_version', v_from_version,
              'to_session_version', v_session.version,
              'actor_user_id', p_actor_user_id::text,
              'rebased_at_utc', v_now::text
            )
          ),
        result_row_json = COALESCE(line_update.result_row_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'same_session_version_rebase_v1', pg_catalog.jsonb_build_object(
              'contract_version', 1,
              'authority_kind', 'CERTIFIED_CLONE',
              'invocation_kind', 'SESSION_VERSION_REBASE',
              'from_session_version', v_from_version,
              'to_session_version', v_session.version,
              'actor_user_id', p_actor_user_id::text,
              'rebased_at_utc', v_now::text
            )
          ),
        updated_at_utc = v_now
    WHERE line_update.session_id = p_session_id
      AND line_update.candidate_id = p_candidate_id;

    v_candidate_state_result := public.pay_workbench_delta_update_candidate_state_v1(
      p_session_id,
      p_candidate_id,
      v_source_build_run_id,
      pg_catalog.jsonb_build_object(
        'context', 'CLONE_REBASE',
        'session_version', v_session.version,
        'source_change_seq', v_source_change_seq,
        'authority_kind', 'CERTIFIED_CLONE',
        'invocation_kind', 'SESSION_VERSION_REBASE',
        'actor_user_id', p_actor_user_id::text
      )
    );
    IF COALESCE((v_candidate_state_result->>'ok')::boolean, false) IS NOT TRUE
       OR COALESCE((v_candidate_state_result->>'fallback_required')::boolean, false) IS TRUE
       OR COALESCE((v_candidate_state_result->>'candidate_state_updated')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_REBASE_CANDIDATE_STATE_FAILED'
        USING ERRCODE = 'P0001';
    END IF;

    WITH current_source AS (
      SELECT
        public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS section,
        source_row.line_key,
        source_row.source_ordinal,
        source_row.source_row_json
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      WHERE source_row.session_id = p_session_id
        AND source_row.candidate_id = p_candidate_id
        AND source_row.session_version = v_session.version
        AND source_row.source_change_seq = v_source_change_seq
        AND source_row.source_publication_id = v_source_publication_id
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(source_row.status, ''))) = 'CURRENT'
    ), ready_preview AS (
      SELECT preview_row.section, preview_row.row_key,
        (preview_row.row_ordinal - (v_scope.scope_ordinal * 1000000))::bigint AS source_ordinal
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = p_candidate_id
        AND preview_row.session_version = v_session.version
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(preview_row.status, ''))) = 'READY'
    )
    SELECT
      pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        section || E'\x1f' || line_key || E'\x1f' || source_ordinal::text,
        E'\x1e' ORDER BY source_ordinal, section, line_key
      ), '')),
      (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        section || E'\x1f' || row_key || E'\x1f' || source_ordinal::text,
        E'\x1e' ORDER BY source_ordinal, section, row_key
      ), '')) FROM ready_preview),
      pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        pg_catalog.md5(source_row_json::text), '' ORDER BY source_ordinal
      ), ''))
    INTO v_source_identity_digest, v_preview_identity_digest, v_source_digest
    FROM current_source;

    v_attestation := COALESCE(v_scope.certified_preview_publication_attestation_json, '{}'::jsonb)
      || pg_catalog.jsonb_build_object(
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', v_session.version,
        'source_change_seq', v_source_change_seq,
        'source_build_run_id', v_source_build_run_id::text,
        'source_publication_id', v_source_publication_id::text,
        'source_row_count', v_source_count,
        'preview_row_count', v_preview_count,
        'invalid_selectable_row_count', 0,
        'authority_kind', 'CERTIFIED_CLONE',
        'invocation_kind', 'SESSION_VERSION_REBASE',
        'source_identity_digest', v_source_identity_digest,
        'preview_identity_digest', v_preview_identity_digest,
        'source_digest', v_source_digest,
        'same_session_version_rebase_v1', pg_catalog.jsonb_build_object(
          'contract_version', 1,
          'authority_kind', 'CERTIFIED_CLONE',
          'invocation_kind', 'SESSION_VERSION_REBASE',
          'from_session_version', v_from_version,
          'to_session_version', v_session.version,
          'actor_user_id', p_actor_user_id::text,
          'rebased_at_utc', v_now::text,
          'source_rows_rederived', false,
          'preview_economics_changed', false,
          'physical_sections_changed', false,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      );

    UPDATE public.banking_pay_workbench_session_scope AS scope_update
    SET status = 'READY',
        dirty = false,
        pending_job_id = NULL,
        error_json = NULL::jsonb,
        certified_preview_publication_required = true,
        certified_preview_publication_parity_ok = true,
        certified_preview_publication_session_version = v_session.version,
        certified_preview_publication_source_change_seq = v_source_change_seq,
        certified_preview_publication_source_build_run_id = v_source_build_run_id,
        certified_preview_publication_source_publication_id = v_source_publication_id,
        certified_preview_publication_attestation_json = v_attestation,
        updated_at_utc = v_now
    WHERE scope_update.session_id = p_session_id
      AND scope_update.candidate_id = p_candidate_id;

    v_currentness := private.pay_workbench_candidate_physical_currentness_page_v1(
      p_session_id, ARRAY[p_candidate_id]::uuid[], 'TERMINAL_CURRENT',
      pg_catalog.jsonb_build_object('contract_version', '1', 'allow_active_owner', false)
    );
    v_currentness_result := COALESCE(v_currentness->'candidate_results'->0, '{}'::jsonb);

    IF COALESCE((v_currentness_result->>'terminal_current')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'WORKBENCH_SESSION_VERSION_REBASE_POST_FENCE_FAILED'
        USING ERRCODE = 'P0001';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'rebased', false,
      'reason', 'VERSION_REBASE_TRANSACTION_ROLLED_BACK',
      'failure_code', SQLSTATE
    );
  END;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'rebased', true,
    'reason', 'CLEAN_SAME_SESSION_VERSION_REBASE',
    'session_id', p_session_id,
    'candidate_id', p_candidate_id,
    'from_session_version', v_from_version,
    'to_session_version', v_session.version,
    'source_change_seq', v_source_change_seq,
    'source_build_run_id', v_source_build_run_id,
    'source_publication_id', v_source_publication_id,
    'source_row_count', v_source_count,
    'preview_row_count', v_preview_count,
    'physical_sections_changed', false,
    'source_rows_rederived', false,
    'preview_economics_changed', false,
    'post_draft_artifacts_touched', false,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_candidate_session_version_rebase_v1(uuid,uuid,bigint,bigint,uuid)
  OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_candidate_session_version_rebase_v1(uuid,uuid,bigint,bigint,uuid)
  SET plpgsql_check.mode TO 'disabled';
REVOKE ALL ON FUNCTION private.pay_workbench_candidate_session_version_rebase_v1(uuid,uuid,bigint,bigint,uuid)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_candidate_session_version_rebase_v1(uuid,uuid,bigint,bigint,uuid)
  TO postgres;


-- User Refresh is a currentness check, not authority to rebuild candidates
-- whose certified source/preview publication is already physically current.
-- Only non-current candidates without an exact active owner enter the existing
-- canonical enqueue ladder. Policy X remains PRE_DRAFT_LIVE_TRUTH.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_refresh_current_authority_v1(
  p_session_id uuid,
  p_actor_user_id uuid,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 100
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_after_scope_ordinal bigint := COALESCE(NULLIF(pg_catalog.btrim(COALESCE(p_cursor_json->>'last_scope_ordinal', '')), '')::bigint, -1);
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_last_scope_ordinal bigint := NULL::bigint;
  v_has_more boolean := false;
  v_currentness jsonb := '{}'::jsonb;
  v_candidate_result jsonb := '{}'::jsonb;
  v_candidate_id uuid := NULL::uuid;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_rebase_result jsonb := '{}'::jsonb;
  v_terminal_current_count integer := 0;
  v_active_owner_count integer := 0;
  v_version_rebased_count integer := 0;
  v_enqueued_candidate_count integer := 0;
  v_no_job_count integer := 0;
  v_route_results jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_ARGUMENT_INVALID'
      USING ERRCODE = 'P0001';
  END IF;
  IF pg_catalog.jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_CURSOR_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  PERFORM 1 FROM public.tms_users AS actor_row WHERE actor_row.id = p_actor_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_ACTOR_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_SESSION_MISSING'
      USING ERRCODE = 'P0001';
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001';
  END IF;

  WITH candidate_page AS (
    SELECT scope_row.candidate_id, scope_row.scope_ordinal
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND scope_row.scope_ordinal > v_after_scope_ordinal
    ORDER BY scope_row.scope_ordinal, scope_row.candidate_id
    LIMIT (v_limit + 1)
  ), bounded_page AS (
    SELECT candidate_page.candidate_id, candidate_page.scope_ordinal
    FROM candidate_page
    ORDER BY candidate_page.scope_ordinal, candidate_page.candidate_id
    LIMIT v_limit
  )
  SELECT
    COALESCE(pg_catalog.array_agg(bounded_page.candidate_id ORDER BY bounded_page.scope_ordinal, bounded_page.candidate_id), ARRAY[]::uuid[]),
    pg_catalog.max(bounded_page.scope_ordinal),
    (SELECT pg_catalog.count(*) > v_limit FROM candidate_page)
  INTO v_candidate_ids, v_last_scope_ordinal, v_has_more
  FROM bounded_page;

  IF pg_catalog.cardinality(v_candidate_ids) = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'contract_version', 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_V1',
      'session_id', p_session_id,
      'candidate_count', 0,
      'terminal_current_count', 0,
      'active_owner_count', 0,
      'version_rebased_count', 0,
      'enqueued_candidate_count', 0,
      'work_candidate_count', 0,
      'has_more', false,
      'next_cursor', NULL::jsonb,
      'no_change', true,
      'policy_x_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  v_currentness := private.pay_workbench_candidate_physical_currentness_page_v1(
    p_session_id,
    v_candidate_ids,
    'OBSERVE_ONLY',
    pg_catalog.jsonb_build_object('contract_version', '1', 'allow_active_owner', true)
  );

  FOR v_candidate_result IN
    SELECT result_row.value
    FROM pg_catalog.jsonb_array_elements(COALESCE(v_currentness->'candidate_results', '[]'::jsonb)) AS result_row(value)
    ORDER BY result_row.value->>'candidate_id'
  LOOP
    IF COALESCE(v_candidate_result->>'candidate_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_RESULT_INVALID'
        USING ERRCODE = 'P0001';
    END IF;
    v_candidate_id := (v_candidate_result->>'candidate_id')::uuid;

    IF COALESCE((v_candidate_result->>'terminal_current')::boolean, false) THEN
      v_terminal_current_count := v_terminal_current_count + 1;
      v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id', v_candidate_id,
        'route', 'CURRENT_NO_CHANGE',
        'currentness_reason', v_candidate_result->>'currentness_reason',
        'proof_digest', v_candidate_result->>'proof_digest'
      ));
    ELSIF COALESCE((v_candidate_result->>'current_or_active_owner')::boolean, false)
          AND COALESCE(v_candidate_result->>'currentness_reason', '') = 'ACTIVE_OWNER' THEN
      v_active_owner_count := v_active_owner_count + 1;
      v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id', v_candidate_id,
        'route', 'ACTIVE_OWNER',
        'owner_job_id', v_candidate_result->>'active_owner_job_id',
        'proof_digest', v_candidate_result->>'proof_digest'
      ));
    ELSE
      v_rebase_result := private.pay_workbench_candidate_session_version_rebase_v1(
        p_session_id,
        v_candidate_id,
        v_session.version - 1,
        v_session.version,
        p_actor_user_id
      );

      IF COALESCE((v_rebase_result->>'rebased')::boolean, false) THEN
        v_version_rebased_count := v_version_rebased_count + 1;
        v_terminal_current_count := v_terminal_current_count + 1;
        v_route_results := v_route_results || pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'candidate_id', v_candidate_id,
            'route', 'SESSION_VERSION_REBASE',
            'currentness_reason', v_candidate_result->>'currentness_reason',
            'rebase_result', v_rebase_result
          )
        );
      ELSE
        v_enqueue_result := public.pay_workbench_enqueue_session_candidate_refresh(
        p_session_id => p_session_id,
        p_candidate_id => v_candidate_id,
        p_reason => 'USER_REQUESTED_CURRENT_AUTHORITY_REFRESH',
        p_actor_user_id => p_actor_user_id,
        p_payload_json => pg_catalog.jsonb_build_object(
          'force_refresh', false,
          'user_requested_refresh', false,
          'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
          'physical_currentness_reason', v_candidate_result->>'currentness_reason',
          'physical_currentness_proof_digest', v_candidate_result->>'proof_digest'
        )
        );
        IF COALESCE((v_enqueue_result->>'enqueued_candidate_count')::integer, 0) > 0
           OR COALESCE(v_enqueue_result->>'job_type', '') <> '' THEN
          v_enqueued_candidate_count := v_enqueued_candidate_count + 1;
        ELSE
          v_no_job_count := v_no_job_count + 1;
        END IF;
        v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'candidate_id', v_candidate_id,
          'route', COALESCE(NULLIF(v_enqueue_result->>'job_type', ''), 'CANONICAL_ENQUEUE_NO_JOB'),
          'currentness_reason', v_candidate_result->>'currentness_reason',
          'version_rebase_result', v_rebase_result,
          'enqueue_result', v_enqueue_result
        ));
      END IF;
    END IF;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'contract_version', 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_V1',
    'session_id', p_session_id,
    'candidate_count', pg_catalog.cardinality(v_candidate_ids),
    'terminal_current_count', v_terminal_current_count,
    'active_owner_count', v_active_owner_count,
    'version_rebased_count', v_version_rebased_count,
    'enqueued_candidate_count', v_enqueued_candidate_count,
    'no_job_count', v_no_job_count,
    'work_candidate_count', v_active_owner_count + v_enqueued_candidate_count,
    'route_results', v_route_results,
    'has_more', v_has_more,
    'next_cursor', CASE WHEN v_has_more THEN pg_catalog.jsonb_build_object('last_scope_ordinal', v_last_scope_ordinal) ELSE NULL::jsonb END,
    'no_change', v_terminal_current_count = pg_catalog.cardinality(v_candidate_ids)
      AND v_version_rebased_count = 0,
    'policy_x_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) SET plpgsql_check.mode TO 'disabled';
REVOKE ALL ON FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) TO service_role;
