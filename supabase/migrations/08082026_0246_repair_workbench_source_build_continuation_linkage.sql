-- Repair only continuation jobs created by the obsolete continuation owner.
-- These rows already carry the exact build cursor in their durable payload but
-- are missing the top-level linkage required by RPC 1. This migration fails
-- closed unless every target is an unattempted continuation for the current,
-- nonterminal candidate build with matching source, generation and sequence.

DO $repair$
DECLARE
  v_target_count integer := 0;
  v_valid_count integer := 0;
  v_updated_count integer := 0;
BEGIN
  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      'BANKING_PAY_WORKBENCH:CONTINUATION_LINKAGE_REPAIR:V1',
      0
    )
  );

  PERFORM 1
  FROM public.banking_pay_workbench_jobs job
  WHERE job.status = 'QUEUED'
    AND job.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND job.economic_build_id IS NULL
    AND COALESCE((job.payload_json ->> 'continuation')::boolean, false)
  FOR UPDATE;

  SELECT count(*)::integer
  INTO v_target_count
  FROM public.banking_pay_workbench_jobs job
  WHERE job.status = 'QUEUED'
    AND job.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND job.economic_build_id IS NULL
    AND COALESCE((job.payload_json ->> 'continuation')::boolean, false);

  IF v_target_count = 0 THEN
    RETURN;
  END IF;

  IF v_target_count > 20 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_TARGET_LIMIT_EXCEEDED'
      USING ERRCODE = '22023';
  END IF;

  CREATE TEMP TABLE pg_temp._bpay_wb_continuation_linkage_repair_v1
  ON COMMIT DROP
  AS
  WITH target AS (
    SELECT
      job.id AS job_id,
      job.session_id,
      job.candidate_id,
      job.payload_json,
      job.payload_json -> 'cursor_json' AS cursor_json,
      CASE
        WHEN pg_catalog.pg_input_is_valid(job.payload_json ->> 'source_job_id', 'uuid')
        THEN (job.payload_json ->> 'source_job_id')::uuid
        ELSE NULL::uuid
      END AS source_job_id,
      CASE
        WHEN pg_catalog.pg_input_is_valid(job.payload_json ->> 'source_build_run_id', 'uuid')
        THEN (job.payload_json ->> 'source_build_run_id')::uuid
        ELSE NULL::uuid
      END AS payload_source_build_run_id,
      CASE
        WHEN pg_catalog.pg_input_is_valid(job.payload_json -> 'cursor_json' ->> 'build_id', 'uuid')
        THEN (job.payload_json -> 'cursor_json' ->> 'build_id')::uuid
        ELSE NULL::uuid
      END AS cursor_build_id,
      CASE
        WHEN pg_catalog.pg_input_is_valid(job.payload_json -> 'cursor_json' ->> 'candidate_id', 'uuid')
        THEN (job.payload_json -> 'cursor_json' ->> 'candidate_id')::uuid
        ELSE NULL::uuid
      END AS cursor_candidate_id,
      CASE
        WHEN COALESCE(job.payload_json -> 'cursor_json' ->> 'captured_candidate_generation', '') ~ '^[0-9]+$'
        THEN (job.payload_json -> 'cursor_json' ->> 'captured_candidate_generation')::bigint
        ELSE NULL::bigint
      END AS cursor_generation,
      CASE
        WHEN COALESCE(job.payload_json -> 'cursor_json' ->> 'captured_source_change_seq', '') ~ '^[0-9]+$'
        THEN (job.payload_json -> 'cursor_json' ->> 'captured_source_change_seq')::bigint
        ELSE NULL::bigint
      END AS cursor_source_change_seq,
      upper(NULLIF(job.payload_json -> 'cursor_json' ->> 'cursor_kind', '')) AS cursor_kind,
      CASE
        WHEN COALESCE(job.payload_json -> 'cursor_json' ->> 'cursor_version', '') ~ '^[0-9]+$'
        THEN (job.payload_json -> 'cursor_json' ->> 'cursor_version')::integer
        ELSE 1
      END AS cursor_version
    FROM public.banking_pay_workbench_jobs job
    WHERE job.status = 'QUEUED'
      AND job.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND job.economic_build_id IS NULL
      AND job.private_stage IS NULL
      AND job.private_cursor_kind IS NULL
      AND job.attempt_count = 0
      AND job.started_at_utc IS NULL
      AND job.completed_at_utc IS NULL
      AND job.failed_at_utc IS NULL
      AND COALESCE((job.payload_json ->> 'continuation')::boolean, false)
      AND jsonb_typeof(job.payload_json) = 'object'
      AND jsonb_typeof(job.payload_json -> 'cursor_json') = 'object'
  )
  SELECT
    target.job_id,
    build.id AS build_id,
    build.private_stage,
    target.cursor_kind,
    target.cursor_json,
    target.cursor_version,
    'source-build:' || target.session_id::text || ':' || target.candidate_id::text
      || ':' || build.id::text || ':' || build.private_stage
      || ':' || md5(target.cursor_json::text) AS repaired_dedupe_key
  FROM target
  JOIN public.banking_pay_workbench_jobs source_job
    ON source_job.id = target.source_job_id
  JOIN private.banking_pay_workbench_candidate_scope_registry registry
    ON registry.candidate_id = target.candidate_id
  JOIN private.banking_pay_workbench_economic_builds build
    ON build.id = registry.current_build_id
  WHERE source_job.status = 'SUCCEEDED'
    AND source_job.session_id = target.session_id
    AND source_job.candidate_id = target.candidate_id
    AND source_job.economic_build_id = build.id
    AND build.source_job_id = source_job.id
    AND build.session_id = target.session_id
    AND build.candidate_id = target.candidate_id
    AND build.status = 'COLLECTING'
    AND build.private_stage IN (
      'PREPARE_SCOPE',
      'DEPENDENCY_CLOSURE',
      'WORKSPACE_FACT',
      'RECONCILE_EXECUTE',
      'SOURCE_PUBLISH',
      'BOOTSTRAP_DISCOVERY',
      'BUILD_CLEANUP'
    )
    AND build.id = target.cursor_build_id
    AND build.source_build_run_id = target.payload_source_build_run_id
    AND build.captured_candidate_generation = registry.dirty_generation
    AND build.captured_candidate_generation = target.cursor_generation
    AND build.source_change_seq = registry.current_source_change_seq
    AND build.source_change_seq = target.cursor_source_change_seq
    AND target.cursor_candidate_id = target.candidate_id
    AND target.cursor_version >= 1
    AND CASE build.private_stage
      WHEN 'PREPARE_SCOPE' THEN target.cursor_kind IN ('SCOPE_SELECT', 'SEED_SCOPE_SEAL')
      WHEN 'DEPENDENCY_CLOSURE' THEN target.cursor_kind IN ('DEPENDENCY_CLOSURE', 'DEPENDENCY_SCOPE_SEAL')
      WHEN 'WORKSPACE_FACT' THEN target.cursor_kind = 'WORKSPACE_FACT'
      WHEN 'RECONCILE_EXECUTE' THEN target.cursor_kind = 'RECONCILE_EXECUTE'
      WHEN 'SOURCE_PUBLISH' THEN target.cursor_kind = 'SOURCE_PUBLISH'
      WHEN 'BOOTSTRAP_DISCOVERY' THEN target.cursor_kind = 'BOOTSTRAP_DISCOVERY'
      WHEN 'BUILD_CLEANUP' THEN target.cursor_kind = 'BUILD_CLEANUP'
      ELSE false
    END
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_jobs competing_job
      WHERE competing_job.id <> target.job_id
        AND competing_job.economic_build_id = build.id
        AND competing_job.status IN ('QUEUED', 'RUNNING')
    )
    AND NOT EXISTS (
      SELECT 1
      FROM private.banking_pay_workbench_stage_attempts attempt
      WHERE attempt.build_id = build.id
        AND attempt.attempt_status = 'STARTED'
    );

  SELECT count(*)::integer
  INTO v_valid_count
  FROM pg_temp._bpay_wb_continuation_linkage_repair_v1;

  IF v_valid_count IS DISTINCT FROM v_target_count THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_PROOF_FAILED: target %, valid %',
      v_target_count,
      v_valid_count
      USING ERRCODE = '40001';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_wb_continuation_linkage_repair_v1 repair_row
    JOIN public.banking_pay_workbench_jobs existing_job
      ON existing_job.dedupe_key = repair_row.repaired_dedupe_key
     AND existing_job.status IN ('QUEUED', 'RUNNING')
     AND existing_job.id <> repair_row.job_id
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_DEDUPE_CONFLICT'
      USING ERRCODE = '23505';
  END IF;

  UPDATE public.banking_pay_workbench_jobs job
  SET economic_build_id = repair_row.build_id,
      private_stage = repair_row.private_stage,
      private_cursor_kind = repair_row.cursor_kind,
      private_cursor_json = repair_row.cursor_json,
      private_stage_version = repair_row.cursor_version,
      dedupe_key = repair_row.repaired_dedupe_key,
      run_at_utc = now(),
      updated_at_utc = now(),
      last_error_json = NULL::jsonb
  FROM pg_temp._bpay_wb_continuation_linkage_repair_v1 repair_row
  WHERE job.id = repair_row.job_id
    AND job.status = 'QUEUED'
    AND job.economic_build_id IS NULL
    AND job.attempt_count = 0;

  GET DIAGNOSTICS v_updated_count = ROW_COUNT;

  IF v_updated_count IS DISTINCT FROM v_target_count THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_UPDATE_MISMATCH: target %, updated %',
      v_target_count,
      v_updated_count
      USING ERRCODE = '40001';
  END IF;
END;
$repair$;

DO $verify$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs job
    WHERE job.status = 'QUEUED'
      AND job.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND job.economic_build_id IS NULL
      AND COALESCE((job.payload_json ->> 'continuation')::boolean, false)
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_INCOMPLETE'
      USING ERRCODE = '40001';
  END IF;
END;
$verify$;
