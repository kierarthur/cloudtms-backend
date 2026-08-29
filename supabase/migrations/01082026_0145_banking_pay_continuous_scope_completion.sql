-- Banking Pay continuous candidate-scope completion: additive schema only.
--
-- This migration intentionally runs before the repeatable function changes.
-- The later repeatable installs the snooze metadata-only dirty guard before
-- performing the bounded source-fingerprint backfill.

ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS scope_change_generation_shadow_checked bigint NOT NULL DEFAULT 0;

ALTER TABLE public.pay_item_snoozes
  ADD COLUMN IF NOT EXISTS natural_expiry_source_fingerprint text NULL,
  ADD COLUMN IF NOT EXISTS natural_expiry_checked_fingerprint text NULL,
  ADD COLUMN IF NOT EXISTS natural_expiry_checked_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS natural_expiry_state_changed boolean NULL,
  ADD COLUMN IF NOT EXISTS natural_expiry_result_code text NULL;

COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_change_generation_shadow_checked IS
  'Highest global Banking Pay scope generation evaluated by non-mutating shadow reconciliation.';

COMMENT ON COLUMN public.pay_item_snoozes.natural_expiry_source_fingerprint IS
  'SHA-256 of the canonical snooze identity/date used to deduplicate natural-expiry evaluation.';
COMMENT ON COLUMN public.pay_item_snoozes.natural_expiry_checked_fingerprint IS
  'Source fingerprint most recently evaluated by the natural-expiry transition.';
COMMENT ON COLUMN public.pay_item_snoozes.natural_expiry_checked_at_utc IS
  'Server timestamp of the latest completed natural-expiry evaluation.';
COMMENT ON COLUMN public.pay_item_snoozes.natural_expiry_state_changed IS
  'Whether the latest completed natural-expiry evaluation changed authoritative snooze state.';
COMMENT ON COLUMN public.pay_item_snoozes.natural_expiry_result_code IS
  'Bounded result code from the latest completed natural-expiry evaluation.';

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_generated_work_authority
  ON public.banking_pay_workbench_jobs (
    dedupe_key,
    scope_change_generation DESC,
    created_at_utc DESC,
    id DESC
  )
  INCLUDE (job_type, status, candidate_id, session_id)
  WHERE scope_change_generation IS NOT NULL
    AND job_type IN (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
      'CONTRACT_CLIENT_DIRTY_FANOUT'
    );

DO $verification$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_sessions'
      AND column_name = 'scope_change_generation_shadow_checked'
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_SHADOW_WATERMARK_COLUMN_MISSING';
  END IF;

  IF (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'pay_item_snoozes'
      AND column_name IN (
        'natural_expiry_source_fingerprint',
        'natural_expiry_checked_fingerprint',
        'natural_expiry_checked_at_utc',
        'natural_expiry_state_changed',
        'natural_expiry_result_code'
      )
  ) <> 5 THEN
    RAISE EXCEPTION 'BANKING_PAY_SNOOZE_EXPIRY_METADATA_COLUMNS_MISSING';
  END IF;
END
$verification$;
