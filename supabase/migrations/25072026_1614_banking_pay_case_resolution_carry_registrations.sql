-- Canonical Banking Pay correction-resolution carry registrations.
-- This relation is the durable authority for decisions that must survive
-- workbench session replacement while target candidate evidence is rebuilt.

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_case_resolution_carry_registrations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  target_session_id uuid NOT NULL,
  source_session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  source_resolution_id uuid NOT NULL,
  source_resolution_identity_key text NOT NULL,
  canonical_resolution_key text NOT NULL,
  resolution_scope_kind text NOT NULL,
  source_economic_fingerprint text NOT NULL,
  source_resolution_snapshot_json jsonb NOT NULL,
  source_priority integer NOT NULL,
  carry_reason text NOT NULL,
  status text NOT NULL,
  state_reason_code text,
  target_source_build_run_id uuid,
  target_resolution_id uuid,
  target_economic_fingerprint text,
  attempt_count integer NOT NULL DEFAULT 0,
  last_attempt_at_utc timestamptz,
  last_error_json jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  completed_at_utc timestamptz
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_target_session_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_target_session_fkey
      FOREIGN KEY (target_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_source_session_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_source_session_fkey
      FOREIGN KEY (source_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_candidate_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_candidate_fkey
      FOREIGN KEY (candidate_id)
      REFERENCES public.candidates(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_source_resolution_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_source_resolution_fkey
      FOREIGN KEY (source_resolution_id)
      REFERENCES public.banking_pay_workbench_session_case_resolutions(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_target_resolution_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_target_resolution_fkey
      FOREIGN KEY (target_resolution_id)
      REFERENCES public.banking_pay_workbench_session_case_resolutions(id)
      ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_scope_kind_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_scope_kind_chk
      CHECK (resolution_scope_kind IN ('CORRECTION_COMPONENT', 'NON_CORRECTION'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_status_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_status_chk
      CHECK (status IN ('PENDING', 'CARRIED', 'STALE', 'INCOMPATIBLE', 'SUPERSEDED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid =
      'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass
      AND conname =
        'banking_pay_case_resolution_carry_terminal_shape_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
      ADD CONSTRAINT banking_pay_case_resolution_carry_terminal_shape_chk
      CHECK (
        (
          status = 'PENDING'
          AND completed_at_utc IS NULL
          AND target_resolution_id IS NULL
        )
        OR (
          status = 'CARRIED'
          AND completed_at_utc IS NOT NULL
          AND target_resolution_id IS NOT NULL
          AND target_economic_fingerprint IS NOT NULL
        )
        OR (
          status IN ('STALE', 'INCOMPATIBLE', 'SUPERSEDED')
          AND completed_at_utc IS NOT NULL
          AND state_reason_code IS NOT NULL
        )
      );
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS
  ux_bpay_case_resolution_carry_target_source_resolution
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (target_session_id, source_resolution_id);

CREATE UNIQUE INDEX IF NOT EXISTS
  ux_bpay_case_resolution_carry_target_candidate_canonical_carried
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (target_session_id, candidate_id, canonical_resolution_key)
WHERE status = 'CARRIED';

CREATE INDEX IF NOT EXISTS
  idx_bpay_case_resolution_carry_processing
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (
    target_session_id,
    candidate_id,
    status,
    source_priority,
    created_at_utc
  );

CREATE INDEX IF NOT EXISTS
  idx_bpay_case_resolution_carry_source_session
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (source_session_id, status, created_at_utc);

CREATE INDEX IF NOT EXISTS
  idx_bpay_case_resolution_carry_candidate
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (candidate_id, status, target_session_id);

CREATE INDEX IF NOT EXISTS
  idx_bpay_case_resolution_carry_source_resolution
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (source_resolution_id);

CREATE INDEX IF NOT EXISTS
  idx_bpay_case_resolution_carry_target_resolution
ON public.banking_pay_workbench_case_resolution_carry_registrations
  (target_resolution_id)
WHERE target_resolution_id IS NOT NULL;

ALTER TABLE public.banking_pay_workbench_case_resolution_carry_registrations
  ENABLE ROW LEVEL SECURITY;

REVOKE ALL
ON TABLE public.banking_pay_workbench_case_resolution_carry_registrations
FROM PUBLIC, anon, authenticated, service_role;

COMMENT ON TABLE
  public.banking_pay_workbench_case_resolution_carry_registrations
IS
  'Durable, server-owned registrations for carrying saved Banking Pay decisions '
  'across workbench session replacement. PENDING rows are never age-cleaned.';
