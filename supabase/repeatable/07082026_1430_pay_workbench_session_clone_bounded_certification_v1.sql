-- Banking Pay clone/rebase certification for the bounded-source authority.
--
-- This helper does not calculate or alter payment economics.  It permits reuse
-- only when the source candidate is the exact published output of one complete
-- immutable economic build and every durable freshness fence is still current.
-- Any missing, ambiguous, time-sensitive or session-specific authority fails
-- closed to the existing full source-build route.

CREATE OR REPLACE FUNCTION private.pay_workbench_session_clone_bounded_certification_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_candidate_id uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_source_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_target_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_source_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_candidate public.candidates%ROWTYPE;
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_current_source_change_seq bigint := 0;
  v_source_count integer := 0;
  v_source_run_count integer := 0;
  v_source_build_run_id uuid := NULL::uuid;
  v_source_seq_min bigint := NULL::bigint;
  v_source_seq_max bigint := NULL::bigint;
  v_source_digest text := NULL::text;
  v_ready_preview_count integer := 0;
  v_active_preview_poison_count integer := 0;
  v_bad_ready_preview_count integer := 0;
  v_active_line_work_count integer := 0;
  v_active_job_count integer := 0;
  v_bad_timesheet_scope_count integer := 0;
  v_matching_build_count integer := 0;
  v_matching_build_digest_count integer := 0;
  v_noncloneable_complexity_count integer := 0;
  v_payee_mismatch_count integer := 0;
  v_rail_provider text := 'CSV';
  v_rail_env text := 'PROD';
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_readiness_mismatch boolean := false;
  v_umbrella_bank_hash text := NULL::text;
  v_umbrella_enabled boolean := false;
  v_proof_digest text := NULL::text;
  v_options_json jsonb := CASE
    WHEN pg_catalog.jsonb_typeof(coalesce(p_options_json, '{}'::jsonb)) = 'object'
      THEN coalesce(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_option_source_session_id_text text := NULL::text;
  v_option_target_session_id_text text := NULL::text;
  v_option_source_session_id uuid := NULL::uuid;
  v_option_target_session_id uuid := NULL::uuid;
  v_controlled_clone_rebase boolean := false;
  v_final_fence_mode boolean := false;
  v_clone_job_id uuid := NULL::uuid;
  v_source_mode text := 'NONEMPTY';
  v_certification_digest text := NULL::text;
  v_live_eligibility jsonb := '{}'::jsonb;
BEGIN
  IF p_source_session_id IS NULL
     OR p_target_session_id IS NULL
     OR p_candidate_id IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SOURCE_TARGET_AND_CANDIDATE_REQUIRED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT source_session.*
  INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = p_source_session_id
    AND (
      (
        pg_catalog.upper(pg_catalog.btrim(coalesce(source_session.status, ''))) = 'OPEN'
        AND source_session.discarded_at_utc IS NULL
      )
      OR (
        pg_catalog.upper(pg_catalog.btrim(coalesce(source_session.status, ''))) = 'DISCARDED'
        AND source_session.discarded_at_utc IS NOT NULL
      )
    )
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SOURCE_SESSION_NOT_AVAILABLE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT target_session.*
  INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id = p_target_session_id
    AND pg_catalog.upper(pg_catalog.btrim(coalesce(target_session.status, ''))) = 'OPEN'
    AND target_session.discarded_at_utc IS NULL
  FOR SHARE;

  IF NOT FOUND
     OR v_source_session.pay_date IS NULL
     OR v_target_session.pay_date IS NULL
     OR v_source_session.pay_date >= v_target_session.pay_date THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SOURCE_TARGET_SESSION_SCOPE_MISMATCH',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  v_option_source_session_id_text := NULLIF(pg_catalog.btrim(coalesce(
    v_options_json->>'source_session_id',
    v_options_json->>'source_session_id_text',
    v_options_json->>'clone_from_session_id',
    v_options_json->>'replacement_source_session_id',
    ''
  )), '');

  IF v_option_source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_option_source_session_id := v_option_source_session_id_text::uuid;
  END IF;

  v_option_target_session_id_text := NULLIF(pg_catalog.btrim(coalesce(
    v_options_json->>'target_session_id',
    v_options_json->>'session_id',
    v_options_json->>'clone_to_session_id',
    ''
  )), '');

  IF v_option_target_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_option_target_session_id := v_option_target_session_id_text::uuid;
  END IF;
  v_final_fence_mode := pg_catalog.lower(pg_catalog.btrim(coalesce(
    v_options_json->>'final_fence_mode','false'
  ))) IN ('true','t','1','yes','y','on');

  IF NULLIF(pg_catalog.btrim(coalesce(
       v_options_json->>'clone_job_id',
       v_options_json->>'source_job_id',
       ''
     )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_clone_job_id := NULLIF(pg_catalog.btrim(coalesce(
      v_options_json->>'clone_job_id',v_options_json->>'source_job_id',''
    )), '')::uuid;
  END IF;

  v_controlled_clone_rebase := (
    (
      pg_catalog.upper(pg_catalog.btrim(coalesce(
        v_options_json->>'job_type',
        v_options_json->>'operation_type',
        v_options_json->>'created_by_helper',
        ''
      ))) IN (
        'WORKBENCH_SESSION_CLONE_REBASE',
        'SESSION_CLONE_REBASE',
        'CLONE_REBASE',
        'PAY_WORKBENCH_SESSION_OPEN_SHARED_V2'
      )
      OR pg_catalog.lower(pg_catalog.btrim(coalesce(
        v_options_json->>'allow_session_rebase',
        v_options_json->>'allowSessionRebase',
        'false'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR pg_catalog.upper(pg_catalog.btrim(coalesce(
        v_options_json->>'clone_mode',
        v_options_json->>'cloneMode',
        ''
      ))) IN ('CERTIFIED_SIMPLE_ONLY', 'CERTIFIED_ONLY')
    )
    AND pg_catalog.lower(pg_catalog.btrim(coalesce(
      v_options_json->>'rebase_simple_rows_only',
      v_options_json->>'rebaseSimpleRowsOnly',
      'true'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND v_option_source_session_id_text IS NOT NULL
    AND v_option_source_session_id = p_source_session_id
    AND v_option_target_session_id_text IS NOT NULL
    AND v_option_target_session_id = p_target_session_id
    AND (
      v_final_fence_mode IS TRUE
      OR pg_catalog.upper(pg_catalog.btrim(coalesce(v_target_session.progress_state, ''))) = 'CLONE_REBASING'
      OR NULLIF(pg_catalog.btrim(coalesce(
        v_target_session.progress_json->>'clone_from_session_id',
        ''
      )), '') = p_source_session_id::text
      OR pg_catalog.lower(pg_catalog.btrim(coalesce(v_options_json->>'source_selection_authorised','false'))) IN ('true','t','1','yes','y','on')
    )
  );

  IF coalesce(v_controlled_clone_rebase, false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'CONTROLLED_CLONE_REBASE_NOT_AUTHORISED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  v_live_eligibility := public.pay_workbench_session_clone_eligibility_v1(
    p_source_session_id,
    p_target_session_id,
    p_candidate_id,
    v_options_json || pg_catalog.jsonb_build_object(
      'source_session_id',p_source_session_id::text,
      'target_session_id',p_target_session_id::text,
      'source_selection_authorised',true,
      'allow_session_rebase',true,
      'rebase_simple_rows_only',true
    )
  );

  IF coalesce((v_live_eligibility->>'clone_eligible')::boolean,false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,
      'clone_eligible',false,
      'bounded_build_certified',false,
      'reason',coalesce(
        nullif(v_live_eligibility->>'reason',''),
        'TARGET_DATE_REUSE_NOT_CERTIFIED'
      ),
      'required_refresh_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT source_scope.*
  INTO v_source_scope
  FROM public.banking_pay_workbench_session_scope AS source_scope
  WHERE source_scope.session_id = p_source_session_id
    AND source_scope.candidate_id = p_candidate_id
    AND pg_catalog.upper(pg_catalog.btrim(coalesce(source_scope.status, '')))
          IN ('READY', 'MATERIALISED', 'MATERIALIZED')
    AND coalesce(source_scope.certified_preview_publication_required,false) IS TRUE
    AND coalesce(source_scope.certified_preview_publication_parity_ok,false) IS TRUE
    AND coalesce(source_scope.dirty, false) IS NOT TRUE
    AND source_scope.pending_job_id IS NULL
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SOURCE_SCOPE_NOT_CURRENT',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT registry.*
  INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id = p_candidate_id
    AND registry.initialisation_status = 'READY'
    AND registry.evaluated_generation = registry.dirty_generation
    AND registry.failure_json = '{}'::jsonb
  FOR SHARE;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'CANDIDATE_REGISTRY_NOT_CURRENT',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT coalesce(change_counter.seq, 0)
  INTO v_current_source_change_seq
  FROM (SELECT 1) AS authority_anchor
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF v_registry.current_source_change_seq IS DISTINCT FROM v_current_source_change_seq THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SOURCE_CHANGE_SEQUENCE_STALE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT
    pg_catalog.count(*)::integer,
    pg_catalog.count(DISTINCT source_line.source_build_run_id)::integer,
    (pg_catalog.array_agg(source_line.source_build_run_id ORDER BY source_line.source_build_run_id))[1],
    pg_catalog.min(source_line.source_change_seq),
    pg_catalog.max(source_line.source_change_seq),
    pg_catalog.md5(coalesce(
      pg_catalog.string_agg(
        pg_catalog.md5(source_line.source_row_json::text),
        '' ORDER BY source_line.source_ordinal
      ),
      ''
    ))
  INTO
    v_source_count,
    v_source_run_count,
    v_source_build_run_id,
    v_source_seq_min,
    v_source_seq_max,
    v_source_digest
  FROM public.banking_pay_workbench_candidate_source_lines AS source_line
  WHERE source_line.session_id = p_source_session_id
    AND source_line.candidate_id = p_candidate_id
    AND source_line.session_version = v_source_session.version
    AND source_line.status = 'CURRENT';

  IF coalesce(v_source_count,0)=0 THEN
    v_source_mode := 'SOURCE_EMPTY';
    v_source_build_run_id := v_source_scope.certified_preview_publication_source_build_run_id;
    v_source_run_count := CASE WHEN v_source_build_run_id IS NULL THEN 0 ELSE 1 END;
    v_source_seq_min := v_current_source_change_seq;
    v_source_seq_max := v_current_source_change_seq;
    v_source_digest := pg_catalog.md5('');
  END IF;
  IF v_source_run_count <> 1
     OR v_source_build_run_id IS NULL
     OR v_source_seq_min IS DISTINCT FROM v_current_source_change_seq
     OR v_source_seq_max IS DISTINCT FROM v_current_source_change_seq THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'CURRENT_SOURCE_BUILD_AUTHORITY_INCOMPLETE',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT
    pg_catalog.count(*)::integer,
    pg_catalog.count(DISTINCT candidate_build.canonical_digest)::integer
  INTO v_matching_build_count, v_matching_build_digest_count
  FROM private.banking_pay_workbench_economic_builds AS candidate_build
  WHERE candidate_build.candidate_id = p_candidate_id
    AND candidate_build.source_build_run_id = v_source_build_run_id
    AND candidate_build.status = 'COMPLETE'
    AND candidate_build.private_stage = 'COMPLETE'
    AND candidate_build.completed_at_utc IS NOT NULL
    AND candidate_build.failure_json = '{}'::jsonb
    AND candidate_build.source_change_seq = v_current_source_change_seq
    AND candidate_build.captured_candidate_generation = v_registry.evaluated_generation
    AND candidate_build.canonical_count = v_source_count
    AND candidate_build.canonical_digest = v_source_digest
    AND candidate_build.sealed_fingerprint_digest IS NOT NULL
    AND coalesce((candidate_build.attestation_json->>'effect_plan_sealed')::boolean,false) IS TRUE
    AND NULLIF(candidate_build.attestation_json->>'effect_plan_digest','') IS NOT NULL
    AND NULLIF(candidate_build.attestation_json->>'observed_finance_effect_digest','') IS NOT NULL
    AND candidate_build.attestation_json->>'observed_finance_effect_count' ~ '^[0-9]+$'
    AND candidate_build.pre_sync_digest IS NOT NULL
    AND candidate_build.post_sync_digest IS NOT NULL
    AND candidate_build.canonical_digest IS NOT NULL
    AND candidate_build.dependency_closure_sealed_at_utc IS NOT NULL
    AND candidate_build.dependency_edge_stream_complete IS TRUE
    AND candidate_build.edge_tag_stream_complete IS TRUE
    AND pg_catalog.lower(coalesce(candidate_build.publication_cursor_json->>'terminal', 'false'))
          IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_matching_build_count < 1 OR v_matching_build_digest_count <> 1 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'COMPLETE_BUILD_PUBLICATION_NOT_PROVEN',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT candidate_build.*
  INTO v_build
  FROM private.banking_pay_workbench_economic_builds AS candidate_build
  WHERE candidate_build.candidate_id = p_candidate_id
    AND candidate_build.source_build_run_id = v_source_build_run_id
    AND candidate_build.status = 'COMPLETE'
    AND candidate_build.private_stage = 'COMPLETE'
    AND candidate_build.source_change_seq = v_current_source_change_seq
    AND candidate_build.captured_candidate_generation = v_registry.evaluated_generation
    AND candidate_build.canonical_count = v_source_count
    AND candidate_build.canonical_digest = v_source_digest
  ORDER BY candidate_build.completed_at_utc DESC, candidate_build.id
  LIMIT 1
  FOR SHARE;

  SELECT pg_catalog.count(*)::integer
  INTO v_bad_timesheet_scope_count
  FROM private.banking_pay_workbench_economic_build_scope AS build_scope
  LEFT JOIN private.banking_pay_workbench_timesheet_scope_state AS timesheet_state
    ON timesheet_state.timesheet_id = build_scope.timesheet_id
   AND timesheet_state.candidate_id = p_candidate_id
  WHERE build_scope.build_id = v_build.id
    AND (
      timesheet_state.timesheet_id IS NULL
      OR timesheet_state.evaluated_generation IS DISTINCT FROM timesheet_state.dirty_generation
      OR timesheet_state.current_input_fingerprint IS NULL
      OR timesheet_state.evaluated_input_fingerprint IS DISTINCT FROM timesheet_state.current_input_fingerprint
      OR build_scope.captured_input_fingerprint IS DISTINCT FROM timesheet_state.current_input_fingerprint
    );

  IF v_bad_timesheet_scope_count > 0
     OR (SELECT pg_catalog.count(*) FROM private.banking_pay_workbench_economic_build_scope AS build_scope WHERE build_scope.build_id = v_build.id)
          IS DISTINCT FROM v_build.scope_count::bigint THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'SEALED_TIMESHEET_SCOPE_NOT_CURRENT',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT
    pg_catalog.count(*) FILTER (WHERE preview_row.status = 'READY')::integer,
    pg_catalog.count(*) FILTER (
      WHERE pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.status, '')))
            IN ('DIRTY', 'ERROR', 'FAILED', 'PENDING')
    )::integer,
    pg_catalog.count(*) FILTER (
      WHERE preview_row.status = 'READY'
        AND (
          pg_catalog.jsonb_typeof(coalesce(preview_row.row_json, '{}'::jsonb)) <> 'object'
          OR
          pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.row_json->>'policy_x_authority_scope', ''))) <> 'PRE_DRAFT_LIVE_TRUTH'
        )
    )::integer
  INTO v_ready_preview_count, v_active_preview_poison_count, v_bad_ready_preview_count
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_source_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = v_source_session.version;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_line_work_count
  FROM public.banking_pay_workbench_candidate_line_work AS line_work
  WHERE line_work.session_id = p_source_session_id
    AND line_work.candidate_id = p_candidate_id
    AND pg_catalog.upper(pg_catalog.btrim(coalesce(line_work.status, '')))
          IN ('PENDING', 'READY', 'ERROR', 'FAILED');

  SELECT pg_catalog.count(*)::integer
  INTO v_active_job_count
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.candidate_id = p_candidate_id
    AND pg_catalog.upper(pg_catalog.btrim(coalesce(job_row.status, '')))
          IN ('QUEUED', 'PENDING', 'CLAIMED', 'RUNNING', 'PROCESSING')
    AND (
      job_row.session_id IN (p_source_session_id, p_target_session_id)
      OR job_row.session_id IS NULL
    )
    AND (v_clone_job_id IS NULL OR job_row.id <> v_clone_job_id);

  IF (v_source_mode='NONEMPTY' AND v_ready_preview_count = 0)
     OR v_active_preview_poison_count > 0
     OR v_bad_ready_preview_count > 0
     OR v_active_line_work_count > 0
     OR v_active_job_count > 0
     OR NOT EXISTS (
       SELECT 1
       FROM public.banking_pay_workbench_session_candidate_state AS candidate_state
       WHERE candidate_state.session_id = p_source_session_id
         AND candidate_state.candidate_id = p_candidate_id
         AND candidate_state.status = 'READY'
         AND candidate_state.session_version = v_source_session.version
         AND candidate_state.source_change_seq = v_current_source_change_seq
         AND candidate_state.pending_job_id IS NULL
     ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'PUBLISHED_PREVIEW_NOT_CURRENT',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  -- These authorities can change merely because a date/session boundary is
  -- crossed.  They remain full-build-only even where the last economic build
  -- itself is complete.
  SELECT (
    SELECT pg_catalog.count(*)
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.candidate_id = p_candidate_id
      AND snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL
  ) + (
    SELECT pg_catalog.count(*)
    FROM public.pay_advances AS advance_row
    WHERE advance_row.candidate_id = p_candidate_id
      AND pg_catalog.upper(pg_catalog.btrim(coalesce(advance_row.status::text, ''))) IN ('ACTIVE', 'PAUSED')
      AND advance_row.cleared_at_utc IS NULL
      AND advance_row.written_off_at_utc IS NULL
  ) + (
    SELECT pg_catalog.count(*)
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.candidate_id = p_candidate_id
      AND override_row.consumed_at_utc IS NULL
      AND override_row.cleared_at_utc IS NULL
  ) + (
    SELECT pg_catalog.count(*)
    FROM public.ts_pay_adjustments AS adjustment_row
    WHERE adjustment_row.candidate_id = p_candidate_id
      AND adjustment_row.paid_at_utc IS NULL
  ) + (
    SELECT pg_catalog.count(*)
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id IN (p_source_session_id, p_target_session_id)
      AND resolution_row.candidate_id = p_candidate_id
  )
  INTO v_noncloneable_complexity_count;

  IF v_noncloneable_complexity_count > 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'TIME_OR_SESSION_SENSITIVE_AUTHORITY_REQUIRES_FULL_BUILD',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT candidate_row.*
  INTO v_candidate
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  FOR SHARE;

  SELECT umbrella_row.bank_details_hash, coalesce(umbrella_row.enabled, false)
  INTO v_umbrella_bank_hash, v_umbrella_enabled
  FROM public.umbrellas AS umbrella_row
  WHERE umbrella_row.id = v_candidate.umbrella_id;

  SELECT
    pg_catalog.upper(pg_catalog.btrim(coalesce(settings_row.rail_provider_default, 'CSV'))),
    pg_catalog.upper(pg_catalog.btrim(coalesce(settings_row.rail_env_default, 'PROD'))),
    coalesce(settings_row.rail_supports_name_check, false)
      AND pg_catalog.upper(pg_catalog.btrim(coalesce(settings_row.rail_provider_default, 'CSV'))) <> 'CSV',
    pg_catalog.upper(pg_catalog.btrim(coalesce(settings_row.rail_provider_default, 'CSV'))) <> 'CSV'
  INTO v_rail_provider, v_rail_env, v_need_name_check, v_requires_payee_map
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1;

  SELECT pg_catalog.count(*)::integer
  INTO v_payee_mismatch_count
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_source_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = v_source_session.version
    AND preview_row.status = 'READY'
    AND (
      pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.row_json->>'pay_channel', ''))) NOT IN ('PAYE', 'UMBRELLA')
      OR (
        pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.row_json->>'pay_channel', ''))) = 'PAYE'
        AND (
          pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.pay_method, ''))) <> 'PAYE'
          OR nullif(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,payee_bank_hash}', '')), '')
               IS DISTINCT FROM nullif(pg_catalog.btrim(coalesce(v_candidate.bank_details_hash, '')), '')
        )
      )
      OR (
        pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.row_json->>'pay_channel', ''))) = 'UMBRELLA'
        AND (
          pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.pay_method, ''))) <> 'UMBRELLA'
          OR v_candidate.umbrella_id IS NULL
          OR v_umbrella_enabled IS NOT TRUE
          OR nullif(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,payee_bank_hash}', '')), '')
               IS DISTINCT FROM nullif(pg_catalog.btrim(coalesce(v_umbrella_bank_hash, '')), '')
        )
      )
    );

  IF v_payee_mismatch_count > 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'PAYE_UMBRELLA_OR_BANK_ROUTING_CHANGED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.pay_method, ''))) = 'PAYE'
            THEN 'CANDIDATE'
          ELSE 'UMBRELLA'
        END AS payee_entity_kind,
        CASE
          WHEN pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.pay_method, ''))) = 'PAYE'
            THEN v_candidate.id
          ELSE v_candidate.umbrella_id
        END AS payee_entity_id,
        CASE
          WHEN pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.pay_method, ''))) = 'PAYE'
            THEN nullif(pg_catalog.btrim(coalesce(v_candidate.bank_details_hash, '')), '')
          ELSE nullif(pg_catalog.btrim(coalesce(v_umbrella_bank_hash, '')), '')
        END AS payee_bank_hash
    ) AS current_payee
    LEFT JOIN LATERAL (
      SELECT
        bank_name_check_row.status,
        (
          bank_name_check_row.override_reason IS NOT NULL
          AND bank_name_check_row.override_hash IS NOT DISTINCT FROM current_payee.payee_bank_hash
        ) AS has_override
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
    WHERE preview_row.session_id = p_source_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.session_version = v_source_session.version
      AND preview_row.status = 'READY'
      AND (
        (
          v_need_name_check IS TRUE
          AND nullif(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,name_check_status}', '')), '') IS NOT NULL
          AND pg_catalog.upper(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,name_check_status}', 'UNVERIFIED')))
                IS DISTINCT FROM pg_catalog.upper(pg_catalog.btrim(coalesce(current_name_check.status, 'UNVERIFIED')))
        )
        OR (
          v_need_name_check IS TRUE
          AND nullif(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,name_check_has_override}', '')), '') IS NOT NULL
          AND (
            pg_catalog.lower(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,name_check_has_override}', 'false')))
              IN ('true', 't', '1', 'yes', 'y', 'on')
          ) IS DISTINCT FROM coalesce(current_name_check.has_override, false)
        )
        OR (
          (
            v_requires_payee_map IS TRUE
            OR nullif(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,payee_map_present}', '')), '') IS NOT NULL
          )
          AND (
            pg_catalog.lower(pg_catalog.btrim(coalesce(preview_row.row_json#>>'{payee_context,payee_map_present}', 'false')))
              IN ('true', 't', '1', 'yes', 'y', 'on')
          ) IS DISTINCT FROM (current_payee_map.payee_id IS NOT NULL)
        )
      )
  )
  INTO v_readiness_mismatch;

  IF v_readiness_mismatch IS TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'clone_eligible', false,
      'bounded_build_certified', false,
      'reason', 'PAYEE_READINESS_FINGERPRINT_CHANGED',
      'required_refresh_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    );
  END IF;

  v_proof_digest := pg_catalog.md5(pg_catalog.jsonb_build_object(
    'certification_version', 2,
    'source_session_id',p_source_session_id::text,
    'target_session_id',p_target_session_id::text,
    'original_economic_build_id',v_build.id::text,
    'source_mode',v_source_mode,
    'candidate_generation', v_registry.evaluated_generation,
    'source_change_seq', v_current_source_change_seq,
    'source_build_run_id', v_source_build_run_id::text,
    'source_count', v_source_count,
    'source_digest', v_source_digest,
    'canonical_digest', v_build.canonical_digest,
    'sealed_fingerprint_digest', v_build.sealed_fingerprint_digest,
    'pre_sync_digest', v_build.pre_sync_digest,
    'post_sync_digest', v_build.post_sync_digest,
    'ready_preview_count', v_ready_preview_count,
    'target_authority_scope_digest',v_live_eligibility->>'authority_scope_digest',
    'target_authority_timesheet_count',v_live_eligibility->>'authority_timesheet_count',
    'target_snapshot_key_count',v_live_eligibility->>'target_snapshot_key_count'
  )::text);
  v_certification_digest := v_proof_digest;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'clone_eligible', true,
    'bounded_build_certified', true,
    'bounded_build_proof_version', 2,
    'certification_version',2,
    'certification_digest',v_certification_digest,
    'source_mode',v_source_mode,
    'ready_empty',(v_source_mode='SOURCE_EMPTY'),
    'post_clone_action','NONE',
    'reason', NULL::text,
    'required_refresh_job_type', NULL::text,
    'current_source_change_seq', v_current_source_change_seq,
    'source_build_run_id', v_source_build_run_id::text,
    'source_row_count', v_source_count,
    'original_economic_build_id',v_build.id::text,
    'economic_build_id',v_build.id::text,
    'original_source_build_run_id',v_source_build_run_id::text,
    'source_session_id',p_source_session_id::text,
    'source_digest', v_source_digest,
    'ready_preview_row_count', v_ready_preview_count,
    'proof_digest', v_proof_digest,
    'target_authority_scope_digest',v_live_eligibility->>'authority_scope_digest',
    'target_authority_timesheet_count',v_live_eligibility->>'authority_timesheet_count',
    'target_snapshot_key_count',v_live_eligibility->>'target_snapshot_key_count',
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_session_clone_bounded_certification_v1(uuid, uuid, uuid, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_session_clone_bounded_certification_v1(uuid, uuid, uuid, jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_session_clone_bounded_certification_v1(uuid, uuid, uuid, jsonb) TO postgres;
