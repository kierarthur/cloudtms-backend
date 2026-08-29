/*
 * Banking Pay dirty-session cursor authority.
 *
 * This extends the existing private job cursor columns to the candidate dirty
 * processor. It does not add a queue, change payment economics, or alter the
 * bounded source-build cursor contract.
 */

ALTER TABLE public.banking_pay_workbench_jobs
  DROP CONSTRAINT bpay_wb_jobs_private_stage_cursor_chk,
  DROP CONSTRAINT bpay_wb_jobs_build_identity_chk;

ALTER TABLE public.banking_pay_workbench_jobs
  ADD CONSTRAINT bpay_wb_jobs_private_stage_cursor_chk CHECK (
    (private_stage IS NULL AND private_cursor_kind IS NULL AND private_stage_version IS NULL)
    OR (
      job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND private_stage IS NULL
      AND private_cursor_kind = 'DIRTY_SESSION_SCAN_V1'
      AND private_stage_version = 1
    )
    OR (private_stage = 'BUILD_INITIALISE' AND private_cursor_kind = 'BUILD_INITIALISE')
    OR (private_stage = 'PREPARE_SCOPE' AND private_cursor_kind IN ('SCOPE_SELECT','SEED_SCOPE_SEAL'))
    OR (private_stage = 'DEPENDENCY_CLOSURE' AND private_cursor_kind IN ('DEPENDENCY_CLOSURE','DEPENDENCY_SCOPE_SEAL'))
    OR (private_stage = 'WORKSPACE_FACT' AND private_cursor_kind = 'WORKSPACE_FACT')
    OR (private_stage = 'RECONCILE_EXECUTE' AND private_cursor_kind = 'RECONCILE_EXECUTE')
    OR (private_stage = 'SOURCE_PUBLISH' AND private_cursor_kind = 'SOURCE_PUBLISH')
    OR (private_stage = 'BOOTSTRAP_DISCOVERY' AND private_cursor_kind = 'BOOTSTRAP_DISCOVERY')
    OR (private_stage = 'BUILD_CLEANUP' AND private_cursor_kind = 'BUILD_CLEANUP')
    OR (private_stage = 'COMPLETE' AND private_cursor_kind = 'COMPLETE')
  ),
  ADD CONSTRAINT bpay_wb_jobs_build_identity_chk CHECK (
    (
      private_stage IS NULL
      AND economic_build_id IS NULL
      AND private_cursor_kind IS NULL
      AND private_stage_version IS NULL
      AND private_cursor_json = '{}'::jsonb
    )
    OR (
      economic_build_id IS NULL
      AND job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND private_stage IS NULL
      AND private_cursor_kind = 'DIRTY_SESSION_SCAN_V1'
      AND private_stage_version = 1
    )
    OR (
      economic_build_id IS NULL AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND status = 'QUEUED' AND private_stage = 'BUILD_INITIALISE'
      AND private_cursor_kind = 'BUILD_INITIALISE' AND attempt_count = 0
      AND private_stage_version = 1
    )
    OR (
      economic_build_id IS NOT NULL AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND private_stage IS NOT NULL AND private_stage <> 'BUILD_INITIALISE'
      AND private_cursor_kind IS NOT NULL AND private_stage_version IS NOT NULL
    )
  );
