-- Banking Pay bounded-scope Version 1.2.4: TEST-only legacy bootstrap start.
--
-- DO NOT auto-run. This is a state-changing TEST operation and requires a
-- separate explicit approval after the migration, repeatables and blocking
-- verification suite have passed. It never executes finance, payment,
-- settlement, remittance, provider or post-draft work.

BEGIN;

DO $guard$
BEGIN
  IF current_setting('server_version_num')::integer < 170000 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_POSTGRES_17_REQUIRED';
  END IF;
  IF to_regclass('private.banking_pay_workbench_candidate_scope_registry') IS NULL
     OR to_regclass('private.banking_pay_workbench_timesheet_scope_state') IS NULL
     OR to_regclass('private.banking_pay_workbench_economic_builds') IS NULL
     OR to_regprocedure(
       'public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamp with time zone,uuid,uuid)'
     ) IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_RUNTIME_NOT_INSTALLED';
  END IF;
END
$guard$;

CREATE TEMP TABLE pg_temp._bpay_wb_bootstrap_targets_v1 ON COMMIT DROP AS
WITH ranked_scope AS (
  SELECT
    scope_row.candidate_id,
    session_row.id AS session_id,
    session_row.version AS session_version,
    session_row.source_snapshot_run_id,
    row_number() OVER (
      PARTITION BY scope_row.candidate_id
      ORDER BY session_row.updated_at_utc DESC NULLS LAST,
        session_row.created_at_utc DESC NULLS LAST,session_row.id DESC
    ) AS session_rank
  FROM public.banking_pay_workbench_session_scope scope_row
  JOIN public.banking_pay_workbench_sessions session_row
    ON session_row.id=scope_row.session_id
   AND session_row.status='OPEN'
   AND session_row.discarded_at_utc IS NULL
  WHERE scope_row.candidate_id IS NOT NULL
), eligible AS (
  SELECT ranked_scope.*
  FROM ranked_scope
  WHERE session_rank=1
)
SELECT eligible.candidate_id,eligible.session_id,eligible.session_version,
  eligible.source_snapshot_run_id,gen_random_uuid() AS bootstrap_id,
  gen_random_uuid() AS source_build_run_id
FROM eligible
LEFT JOIN private.banking_pay_workbench_candidate_scope_registry registry
  ON registry.candidate_id=eligible.candidate_id
WHERE COALESCE(registry.initialisation_status,'UNINITIALISED')
        IN ('UNINITIALISED','FAILED')
  AND registry.current_build_id IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.banking_pay_workbench_jobs active_job
    WHERE active_job.candidate_id=eligible.candidate_id
      AND active_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND active_job.status IN ('QUEUED','RUNNING')
  );

INSERT INTO private.banking_pay_workbench_candidate_scope_registry(
  candidate_id,initialisation_status,last_dirty_reason,last_dirtied_at_utc,
  created_at_utc,updated_at_utc
)
SELECT target.candidate_id,'UNINITIALISED','LEGACY_BOOTSTRAP_PENDING',
  statement_timestamp(),statement_timestamp(),statement_timestamp()
FROM pg_temp._bpay_wb_bootstrap_targets_v1 target
ON CONFLICT(candidate_id) DO UPDATE
SET initialisation_status='UNINITIALISED',bootstrap_id=NULL,
    bootstrap_stream=NULL,bootstrap_cursor_json='{}'::jsonb,
    bootstrap_rows_seen=0,bootstrap_timesheets_registered=0,
    bootstrap_captured_generation=NULL,
    bootstrap_captured_source_change_seq=NULL,
    failure_json='{}'::jsonb,last_dirty_reason='LEGACY_BOOTSTRAP_PENDING',
    updated_at_utc=statement_timestamp()
WHERE private.banking_pay_workbench_candidate_scope_registry.current_build_id IS NULL
  AND private.banking_pay_workbench_candidate_scope_registry.initialisation_status
        IN ('UNINITIALISED','FAILED');

INSERT INTO public.banking_pay_workbench_jobs(
  job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
  snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
  private_stage,private_cursor_kind,private_cursor_json,private_stage_version,
  created_at_utc,updated_at_utc,started_at_utc,completed_at_utc,failed_at_utc,
  last_error_json
)
SELECT
  'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',44,clock_timestamp(),0,8,
  'WORKBENCH_CANDIDATE_SOURCE_BUILD:'||target.session_id::text||':'||
    target.candidate_id::text||':v'||target.session_version::text||':s'||
    registry.current_source_change_seq::text||':bootstrap:'||target.bootstrap_id::text,
  target.source_snapshot_run_id,target.session_id,target.candidate_id,
  jsonb_build_object(
    'bootstrap_id',target.bootstrap_id,
    'source_build_run_id',target.source_build_run_id,
    'session_id',target.session_id,
    'candidate_id',target.candidate_id,
    'session_version',target.session_version,
    'snapshot_run_id',target.source_snapshot_run_id,
    'source_snapshot_run_id',target.source_snapshot_run_id,
    'source_change_seq',registry.current_source_change_seq,
    'refresh_scope_kind','CANDIDATE_FULL_LIVE',
    'targeted_timesheet_ids','[]'::jsonb,
    'linked_timesheet_ids','[]'::jsonb,
    'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
    'legacy_bootstrap',true
  ),
  NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
  clock_timestamp(),clock_timestamp(),NULL,NULL,NULL,NULL
FROM pg_temp._bpay_wb_bootstrap_targets_v1 target
JOIN private.banking_pay_workbench_candidate_scope_registry registry
  ON registry.candidate_id=target.candidate_id
ON CONFLICT(dedupe_key) WHERE status IN ('QUEUED','RUNNING') DO NOTHING;

-- Deliberately return aggregate operational evidence only.
SELECT
  count(*)::integer AS bootstrap_candidate_count,
  count(*) FILTER (WHERE registry.initialisation_status='UNINITIALISED')::integer
    AS uninitialised_candidate_count,
  count(*) FILTER (WHERE queued_job.id IS NOT NULL)::integer AS queued_job_count
FROM pg_temp._bpay_wb_bootstrap_targets_v1 target
JOIN private.banking_pay_workbench_candidate_scope_registry registry
  ON registry.candidate_id=target.candidate_id
LEFT JOIN public.banking_pay_workbench_jobs queued_job
  ON queued_job.candidate_id=target.candidate_id
 AND queued_job.session_id=target.session_id
 AND queued_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
 AND queued_job.status='QUEUED'
 AND queued_job.payload_json->>'bootstrap_id'=target.bootstrap_id::text;

COMMIT;
