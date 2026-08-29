\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for replacement-session queue replay.
-- A queued session-scope root is safe to replay after its session authority is
-- replaced. Candidate work has a session-bound build identity and is covered
-- separately by the canonical-candidate-owner verifier. This proof retains the
-- established session replay, terminalisation and idempotency boundaries. No
-- Draft, provider, payment or settlement owner is invoked.
BEGIN;

DO $verification$
DECLARE
  v_snapshot_id uuid := gen_random_uuid();
  v_actor_id uuid := gen_random_uuid();
  v_source_session_id uuid := gen_random_uuid();
  v_target_session_id uuid := gen_random_uuid();
  v_source_job_id uuid := gen_random_uuid();
  v_second_source_job_id uuid := gen_random_uuid();
  v_replay_job_id uuid;
  v_result jsonb;
  v_row record;
  v_prefix text := 'BANKING_PAY_QUEUE_REPLAY_VERIFY:' || gen_random_uuid()::text;
BEGIN
  INSERT INTO public.banking_pay_snapshot_runs (
    id,
    pay_date,
    week_ending_cutoff,
    pay_week_start,
    eligibility_from_date,
    eligibility_to_date,
    status,
    is_active
  ) VALUES (
    v_snapshot_id,
    DATE '2099-01-09',
    DATE '2099-01-04',
    DATE '2098-12-29',
    DATE '2098-12-01',
    DATE '2099-01-04',
    'OPEN',
    false
  );

  INSERT INTO public.banking_pay_workbench_sessions (
    id,
    actor_user_id,
    pay_date,
    week_ending_cutoff,
    session_signature,
    source_snapshot_run_id,
    status,
    version
  ) VALUES
    (
      v_source_session_id,
      v_actor_id,
      DATE '2099-01-09',
      DATE '2099-01-04',
      v_prefix || ':SOURCE',
      v_snapshot_id,
      'DISCARDED',
      1
    ),
    (
      v_target_session_id,
      v_actor_id,
      DATE '2099-01-09',
      DATE '2099-01-04',
      v_prefix || ':TARGET',
      v_snapshot_id,
      'OPEN',
      2
    );

  INSERT INTO public.banking_pay_workbench_jobs (
    id,
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    payload_json,
    economic_build_id,
    private_stage,
    private_cursor_kind,
    private_cursor_json,
    private_stage_version
  ) VALUES (
    v_source_job_id,
    'WORKBENCH_SESSION_SCOPE_SEED',
    'QUEUED',
    40,
    clock_timestamp(),
    0,
    8,
    v_prefix || ':SOURCE_JOB',
    v_snapshot_id,
    v_source_session_id,
    jsonb_build_object('verification_marker', v_prefix),
    NULL,
    NULL,
    NULL,
    '{}'::jsonb,
    NULL
  );

  v_result := public.pay_workbench_session_replay_replaced_queue_v1(
    v_source_session_id,
    v_target_session_id,
    'VERIFICATION_REPLAY',
    '{}'::jsonb
  );

  IF COALESCE((v_result->>'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_result->>'replayed_job_count')::integer, -1) <> 1
     OR COALESCE((v_result->>'source_queued_terminalised_count')::integer, -1) <> 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_REPLAY_RESULT_INVALID',
      DETAIL = v_result::text;
  END IF;

  SELECT
    job.status,
    job.economic_build_id,
    job.private_stage,
    job.private_cursor_kind,
    job.private_cursor_json,
    job.private_stage_version,
    job.last_error_json->>'code' AS error_code
  INTO v_row
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.id = v_source_job_id;

  IF v_row.status IS DISTINCT FROM 'DEAD'
     OR v_row.economic_build_id IS NOT NULL
     OR v_row.private_stage IS NOT NULL
     OR v_row.private_cursor_kind IS NOT NULL
     OR v_row.private_cursor_json IS DISTINCT FROM '{}'::jsonb
     OR v_row.private_stage_version IS NOT NULL
     OR v_row.error_code IS DISTINCT FROM 'REPLACED_SESSION_QUEUE_REPLAYED' THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_SOURCE_TERMINAL_SHAPE_INVALID',
      DETAIL = pg_catalog.to_jsonb(v_row)::text;
  END IF;

  SELECT job.id
  INTO v_replay_job_id
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.dedupe_key = (
    'REPLAY_REPLACED_SESSION:'
    || v_target_session_id::text
    || ':source_job:'
    || v_source_job_id::text
  );

  IF v_replay_job_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_REPLACED_QUEUE_TARGET_JOB_MISSING';
  END IF;

  SELECT
    job.job_type,
    job.status,
    job.attempt_count,
    job.snapshot_run_id,
    job.session_id,
    job.economic_build_id,
    job.private_stage,
    job.private_cursor_kind,
    job.private_cursor_json,
    job.private_stage_version,
    job.payload_json->>'replayed_from_session_id' AS replayed_from_session_id,
    job.payload_json->>'replayed_from_job_id' AS replayed_from_job_id
  INTO v_row
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.id = v_replay_job_id;

  IF v_row.job_type IS DISTINCT FROM 'WORKBENCH_SESSION_SCOPE_SEED'
     OR v_row.status IS DISTINCT FROM 'QUEUED'
     OR v_row.attempt_count IS DISTINCT FROM 0
     OR v_row.snapshot_run_id IS DISTINCT FROM v_snapshot_id
     OR v_row.session_id IS DISTINCT FROM v_target_session_id
     OR v_row.economic_build_id IS NOT NULL
     OR v_row.private_stage IS NOT NULL
     OR v_row.private_cursor_kind IS NOT NULL
     OR v_row.private_cursor_json IS DISTINCT FROM '{}'::jsonb
     OR v_row.private_stage_version IS NOT NULL
     OR v_row.replayed_from_session_id IS DISTINCT FROM v_source_session_id::text
     OR v_row.replayed_from_job_id IS DISTINCT FROM v_source_job_id::text THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_TARGET_INITIAL_SHAPE_INVALID',
      DETAIL = pg_catalog.to_jsonb(v_row)::text;
  END IF;

  v_result := public.pay_workbench_session_replay_replaced_queue_v1(
    v_source_session_id,
    v_target_session_id,
    'VERIFICATION_REPLAY',
    '{}'::jsonb
  );

  IF COALESCE((v_result->>'source_active_job_count')::integer, -1) <> 0
     OR (
       SELECT COUNT(*)
       FROM public.banking_pay_workbench_jobs AS job
       WHERE job.dedupe_key = (
         'REPLAY_REPLACED_SESSION:'
         || v_target_session_id::text
         || ':source_job:'
         || v_source_job_id::text
       )
     ) <> 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_REPLAY_NOT_IDEMPOTENT',
      DETAIL = v_result::text;
  END IF;

  INSERT INTO public.banking_pay_workbench_jobs (
    id,
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    payload_json,
    economic_build_id,
    private_stage,
    private_cursor_kind,
    private_cursor_json,
    private_stage_version
  ) VALUES (
    v_second_source_job_id,
    'WORKBENCH_SESSION_SCOPE_SEED',
    'QUEUED',
    41,
    clock_timestamp(),
    0,
    8,
    v_prefix || ':SECOND_SOURCE_JOB',
    v_snapshot_id,
    v_source_session_id,
    '{}'::jsonb,
    NULL,
    NULL,
    NULL,
    '{}'::jsonb,
    NULL
  );

  v_result := public.pay_workbench_session_replay_replaced_queue_v1(
    v_source_session_id,
    v_target_session_id,
    'VERIFICATION_NO_TERMINALISE',
    jsonb_build_object('terminalise_source_queued', false)
  );

  SELECT job.status, job.private_stage, job.private_cursor_kind, job.private_stage_version
  INTO v_row
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.id = v_second_source_job_id;

  IF v_row.status IS DISTINCT FROM 'QUEUED'
     OR v_row.private_stage IS NOT NULL
     OR v_row.private_cursor_kind IS NOT NULL
     OR v_row.private_stage_version IS NOT NULL
     OR COALESCE((v_result->>'source_queued_terminalised_count')::integer, -1) <> 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_NO_TERMINALISE_OPTION_CHANGED_SOURCE',
      DETAIL = pg_catalog.to_jsonb(v_row)::text;
  END IF;

  v_result := public.pay_workbench_session_replay_replaced_queue_v1(
    v_target_session_id,
    v_target_session_id,
    'VERIFICATION_IDENTICAL',
    '{}'::jsonb
  );

  IF COALESCE((v_result->>'skipped')::boolean, false) IS NOT TRUE
     OR v_result->>'skip_reason' IS DISTINCT FROM 'SOURCE_AND_TARGET_IDENTICAL' THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_REPLACED_QUEUE_IDENTICAL_SESSION_GUARD_FAILED',
      DETAIL = v_result::text;
  END IF;
END;
$verification$;

ROLLBACK;
