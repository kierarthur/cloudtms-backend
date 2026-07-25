-- Durable Banking Pay preview-selection carry across shared workbench replacement.
-- This stores intent only. It does not calculate or alter payment economics.

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_selection_carry_registrations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  target_session_id uuid NOT NULL,
  source_session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  source_preview_row_id uuid,
  stable_selection_key text NOT NULL,
  selected boolean NOT NULL,
  selection_state text NOT NULL,
  source_priority integer NOT NULL DEFAULT 0,
  carry_reason text NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  state_reason_code text,
  source_row_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  target_preview_row_id uuid,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  completed_at_utc timestamptz
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_target_session_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_target_session_fkey
      FOREIGN KEY (target_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_source_session_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_source_session_fkey
      FOREIGN KEY (source_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_candidate_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_candidate_fkey
      FOREIGN KEY (candidate_id)
      REFERENCES public.candidates(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_source_preview_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_source_preview_fkey
      FOREIGN KEY (source_preview_row_id)
      REFERENCES public.banking_pay_workbench_preview_rows(id)
      ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_target_preview_fkey'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_target_preview_fkey
      FOREIGN KEY (target_preview_row_id)
      REFERENCES public.banking_pay_workbench_preview_rows(id)
      ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_state_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_state_chk
      CHECK (selection_state IN ('SELECTED', 'UNSELECTED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_status_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_status_chk
      CHECK (status IN ('PENDING', 'APPLIED', 'SUPERSEDED', 'AMBIGUOUS'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_selection_carry_registrations'::regclass
      AND conname = 'banking_pay_selection_carry_terminal_shape_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
      ADD CONSTRAINT banking_pay_selection_carry_terminal_shape_chk
      CHECK (
        (
          status = 'PENDING'
          AND completed_at_utc IS NULL
          AND target_preview_row_id IS NULL
          AND source_preview_row_id IS NOT NULL
        )
        OR
        (status = 'APPLIED' AND completed_at_utc IS NOT NULL)
        OR
        (status IN ('SUPERSEDED', 'AMBIGUOUS') AND completed_at_utc IS NOT NULL AND state_reason_code IS NOT NULL)
      );
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS
  ux_bpay_selection_carry_target_source_preview
ON public.banking_pay_workbench_selection_carry_registrations
  (target_session_id, source_preview_row_id);

CREATE INDEX IF NOT EXISTS
  idx_bpay_selection_carry_pending_target_candidate_key
ON public.banking_pay_workbench_selection_carry_registrations
  (target_session_id, candidate_id, stable_selection_key, status, source_priority DESC);

CREATE INDEX IF NOT EXISTS
  idx_bpay_selection_carry_source_session
ON public.banking_pay_workbench_selection_carry_registrations
  (source_session_id, status, created_at_utc);

CREATE UNIQUE INDEX IF NOT EXISTS
  ux_bpay_selection_carry_target_key_applied
ON public.banking_pay_workbench_selection_carry_registrations
  (target_session_id, candidate_id, stable_selection_key)
WHERE status = 'APPLIED';

ALTER TABLE public.banking_pay_workbench_selection_carry_registrations
  ENABLE ROW LEVEL SECURITY;

REVOKE ALL
ON TABLE public.banking_pay_workbench_selection_carry_registrations
FROM PUBLIC, anon, authenticated, service_role;

COMMENT ON TABLE public.banking_pay_workbench_selection_carry_registrations
IS 'Durable server-owned intent for carrying explicit Banking Pay preview selections across shared workbench replacement. It stores no financial authority.';

DROP TRIGGER IF EXISTS trg_banking_pay_preview_selection_carry_apply
ON public.banking_pay_workbench_preview_rows;

CREATE TRIGGER trg_banking_pay_preview_selection_carry_apply
AFTER INSERT OR UPDATE OF
  status,
  selected,
  selection_state,
  row_json,
  key_type,
  key_value
ON public.banking_pay_workbench_preview_rows
FOR EACH ROW
EXECUTE FUNCTION public.trg_banking_pay_preview_selection_carry_apply();
