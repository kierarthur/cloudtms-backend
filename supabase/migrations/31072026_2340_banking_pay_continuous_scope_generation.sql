BEGIN;

-- Banking Pay continuous candidate-scope maintenance.
-- This migration is deliberately additive. Runtime behaviour is installed by
-- repeatable functions only after this schema has reached TEST.

CREATE TABLE IF NOT EXISTS public.banking_pay_scope_change_transactions (
  tx_token uuid PRIMARY KEY,
  state text NOT NULL DEFAULT 'PENDING',
  allocated_generation bigint,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  finalized_at_utc timestamptz,
  CONSTRAINT banking_pay_scope_change_transactions_state_chk
    CHECK (state IN ('PENDING', 'FINALIZED', 'NOOP')),
  CONSTRAINT banking_pay_scope_change_transactions_finalized_chk
    CHECK (
      (state = 'PENDING' AND allocated_generation IS NULL AND finalized_at_utc IS NULL)
      OR (state = 'NOOP' AND allocated_generation IS NULL AND finalized_at_utc IS NOT NULL)
      OR (state = 'FINALIZED' AND allocated_generation IS NOT NULL AND finalized_at_utc IS NOT NULL)
    )
);

ALTER TABLE public.banking_pay_scope_change_transactions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.banking_pay_scope_change_transactions FROM PUBLIC;
REVOKE ALL ON TABLE public.banking_pay_scope_change_transactions FROM anon;
REVOKE ALL ON TABLE public.banking_pay_scope_change_transactions FROM authenticated;
REVOKE ALL ON TABLE public.banking_pay_scope_change_transactions FROM service_role;

ALTER TABLE public.app_change_counters
  ADD COLUMN IF NOT EXISTS scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS scope_change_tx_token uuid;

ALTER TABLE public.banking_pay_workbench_jobs
  ADD COLUMN IF NOT EXISTS scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS scope_change_tx_token uuid;

ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS scope_change_generation_target bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS scope_change_generation_applied bigint NOT NULL DEFAULT 0;

ALTER TABLE public.banking_pay_operations
  ADD COLUMN IF NOT EXISTS scope_freeze_status text NOT NULL DEFAULT 'NONE',
  ADD COLUMN IF NOT EXISTS frozen_scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS scope_frozen_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS source_scope_seed_complete boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS frozen_candidate_scope_count integer,
  ADD COLUMN IF NOT EXISTS frozen_selected_row_count integer,
  ADD COLUMN IF NOT EXISTS frozen_operation_scope_hash text,
  ADD COLUMN IF NOT EXISTS frozen_source_session_version bigint,
  ADD COLUMN IF NOT EXISTS frozen_source_snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS post_freeze_scope_status text NOT NULL DEFAULT 'NONE',
  ADD COLUMN IF NOT EXISTS post_freeze_observed_generation bigint,
  ADD COLUMN IF NOT EXISTS post_freeze_relevant_generation bigint,
  ADD COLUMN IF NOT EXISTS post_freeze_scope_checked_at_utc timestamptz;

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS source_scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS scope_generation_observed_at_shell bigint;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_scope_reconcile_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_scope_reconcile_shadow_mode boolean NOT NULL DEFAULT false;

INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
VALUES ('pay_candidate_scope_generation', 0, now())
ON CONFLICT (entity_key) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.app_change_counters'::regclass
      AND conname = 'app_change_counters_scope_change_tx_token_fkey'
  ) THEN
    ALTER TABLE public.app_change_counters
      ADD CONSTRAINT app_change_counters_scope_change_tx_token_fkey
      FOREIGN KEY (scope_change_tx_token)
      REFERENCES public.banking_pay_scope_change_transactions(tx_token)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_jobs'::regclass
      AND conname = 'banking_pay_workbench_jobs_scope_change_tx_token_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_scope_change_tx_token_fkey
      FOREIGN KEY (scope_change_tx_token)
      REFERENCES public.banking_pay_scope_change_transactions(tx_token)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_jobs'::regclass
      AND conname = 'banking_pay_workbench_jobs_coordinator_token_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_coordinator_token_chk
      CHECK (
        UPPER(BTRIM(COALESCE(job_type, ''))) <> 'WORKBENCH_SCOPE_RECONCILE'
        OR scope_change_tx_token IS NULL
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_sessions'::regclass
      AND conname = 'banking_pay_workbench_sessions_scope_generation_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT banking_pay_workbench_sessions_scope_generation_chk
      CHECK (
        scope_change_generation_target >= 0
        AND scope_change_generation_applied >= 0
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_operations'::regclass
      AND conname = 'banking_pay_operations_scope_freeze_status_chk'
  ) THEN
    ALTER TABLE public.banking_pay_operations
      ADD CONSTRAINT banking_pay_operations_scope_freeze_status_chk
      CHECK (scope_freeze_status IN ('NONE', 'SEEDING', 'FROZEN'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_operations'::regclass
      AND conname = 'banking_pay_operations_post_freeze_scope_status_chk'
  ) THEN
    ALTER TABLE public.banking_pay_operations
      ADD CONSTRAINT banking_pay_operations_post_freeze_scope_status_chk
      CHECK (post_freeze_scope_status IN ('NONE', 'PENDING_RELEVANCE', 'RELEVANT', 'IRRELEVANT'));
  END IF;
END
$$;

-- Existing freshness authority remains central. Extend its vocabulary only to
-- carry the freeze-to-shell generation evidence described by Policy X.
DO $$
DECLARE
  v_exists boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.pay_batches'::regclass
      AND conname = 'pay_batches_freshness_validation_status_check'
  ) INTO v_exists;

  IF v_exists THEN
    ALTER TABLE public.pay_batches
      DROP CONSTRAINT pay_batches_freshness_validation_status_check;
  END IF;

  ALTER TABLE public.pay_batches
    ADD CONSTRAINT pay_batches_freshness_validation_status_check
    CHECK (
      freshness_validation_status IS NULL
      OR freshness_validation_status IN (
        'PENDING', 'PASSED', 'STALE', 'FAILED', 'NOT_REQUIRED',
        'VALID_AT_SCOPE_FREEZE', 'STALE_POST_SCOPE_FREEZE',
        'PENDING_SCOPE_CHANGE_RELEVANCE'
      )
    );
END
$$;

CREATE INDEX IF NOT EXISTS idx_app_change_counters_pay_candidate_scope_generation
  ON public.app_change_counters(scope_change_generation, entity_key)
  WHERE scope_change_generation IS NOT NULL
    AND entity_key LIKE 'pay_candidate:%';

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_scope_generation_barrier
  ON public.banking_pay_workbench_jobs(job_type, status, scope_change_generation)
  WHERE scope_change_generation IS NOT NULL
    AND status IN ('QUEUED', 'RUNNING', 'FAILED');

CREATE INDEX IF NOT EXISTS idx_bpay_scope_change_transactions_cleanup
  ON public.banking_pay_scope_change_transactions(state, finalized_at_utc, tx_token)
  WHERE state IN ('FINALIZED', 'NOOP');

COMMENT ON TABLE public.banking_pay_scope_change_transactions IS
'Internal transaction finalisation staging for one Banking Pay scope generation per source transaction. It is not a queue and is inaccessible to browser roles.';

COMMENT ON COLUMN public.app_change_counters.scope_change_generation IS
'Latest global Banking Pay candidate-scope generation affecting this pay_candidate:<uuid> counter row.';

COMMENT ON COLUMN public.banking_pay_workbench_jobs.scope_change_generation IS
'Authoritative source scope generation inherited by Banking Pay dirty/fan-out work; null for unrelated jobs.';

COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_change_generation_applied IS
'Highest global scope generation completely reconciled for candidate admission into this current session.';

COMMENT ON COLUMN public.banking_pay_operations.frozen_scope_change_generation IS
'Policy X boundary generation atomically sealed with the immutable DRAFT_CREATE operation candidate scope.';

COMMENT ON COLUMN public.pay_batches.source_scope_change_generation IS
'Immutable operation scope generation copied to the draft shell; later freshness checks compare relevant candidate/root generations against it.';

COMMIT;
