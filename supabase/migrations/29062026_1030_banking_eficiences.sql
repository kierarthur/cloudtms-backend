-- Banking Pay Workbench delta projection run state
-- Safe to rerun.

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_candidate_delta_projection_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,

  session_version bigint NOT NULL,
  source_change_seq bigint,
  source_snapshot_run_id uuid,

  projection_mode text NOT NULL DEFAULT 'DELTA',
  projection_class text NOT NULL DEFAULT 'UNKNOWN',

  phase text NOT NULL DEFAULT 'INIT_PREFLIGHT',
  cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,

  targeted_timesheet_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  linked_timesheet_ids jsonb NOT NULL DEFAULT '[]'::jsonb,

  status text NOT NULL DEFAULT 'RUNNING',
  fallback_required boolean NOT NULL DEFAULT false,
  fallback_reason text,

  legacy_compare_status text,
  legacy_compare_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  diagnostics_json jsonb NOT NULL DEFAULT '{}'::jsonb,

  started_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  completed_at_utc timestamptz
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_bpay_delta_projection_runs_status'
      AND conrelid = 'public.banking_pay_workbench_candidate_delta_projection_runs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_delta_projection_runs
      ADD CONSTRAINT chk_bpay_delta_projection_runs_status
      CHECK (status IN (
        'RUNNING',
        'COMPLETED',
        'FAILED',
        'FALLBACK_REQUIRED',
        'BLOCKED'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_bpay_delta_projection_runs_mode'
      AND conrelid = 'public.banking_pay_workbench_candidate_delta_projection_runs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_delta_projection_runs
      ADD CONSTRAINT chk_bpay_delta_projection_runs_mode
      CHECK (projection_mode IN (
        'DELTA',
        'READINESS_PATCH',
        'RESERVATION_PATCH',
        'POST_DRAFT_OVERLAY',
        'CLONE_REBASE',
        'SHADOW_COMPARE'
      ));
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_bpay_delta_runs_session_candidate_status
ON public.banking_pay_workbench_candidate_delta_projection_runs
(session_id, candidate_id, status, phase);

CREATE INDEX IF NOT EXISTS idx_bpay_delta_runs_status_updated
ON public.banking_pay_workbench_candidate_delta_projection_runs
(status, updated_at_utc);

CREATE INDEX IF NOT EXISTS idx_bpay_delta_runs_session_seq
ON public.banking_pay_workbench_candidate_delta_projection_runs
(session_id, candidate_id, session_version, source_change_seq);

COMMENT ON TABLE public.banking_pay_workbench_candidate_delta_projection_runs IS
'Persistent cursor/fallback/audit state for bounded Banking Pay Workbench candidate delta refresh chunks. This table does not itself serve source, line-work, or preview rows.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_delta_projection_runs.targeted_timesheet_ids IS
'JSON array for RPC payload compatibility. SQL functions must convert this to uuid[] before joins.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_delta_projection_runs.linked_timesheet_ids IS
'JSON array for RPC payload compatibility. SQL functions must convert this to uuid[] before joins.';