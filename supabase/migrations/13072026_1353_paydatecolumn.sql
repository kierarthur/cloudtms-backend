-- CloudTMS safe prerequisite migration
-- Purpose:
--   1. Add the organisation-configurable Banking Pay official weekday.
--   2. Add nullable immutable case-resolution origin columns and their NOT VALID FK.
--   3. Add the supporting case-resolution and active-Snooze indexes.
--
-- IMPORTANT:
--   This migration deliberately does NOT:
--     - validate bpay_wb_case_resolution_origin_session_fk;
--     - set resolution_origin_session_id / resolution_origin_pay_date NOT NULL;
--     - revoke updates to the origin columns.
--
--   Those finalisation steps must run only after
--   public.pay_workbench_case_resolution_origin_backfill_v1 has completed
--   and all origin validation queries return zero unresolved/invalid rows.
--
-- Indexes use ordinary CREATE INDEX rather than CREATE INDEX CONCURRENTLY so
-- this file is safe for normal transactional migration runners.

BEGIN;

-- ---------------------------------------------------------------------------
-- Banking Pay official weekday
-- PostgreSQL DOW numbering: Sunday=0 through Saturday=6.
-- ---------------------------------------------------------------------------

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_official_pay_weekday smallint;

UPDATE public.settings_defaults AS settings_row
SET banking_pay_official_pay_weekday = 5
WHERE settings_row.banking_pay_official_pay_weekday IS NULL;

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_official_pay_weekday SET DEFAULT 5,
  ALTER COLUMN banking_pay_official_pay_weekday SET NOT NULL;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'public.settings_defaults'::regclass
      AND constraint_row.conname = 'settings_defaults_banking_pay_official_pay_weekday_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_banking_pay_official_pay_weekday_chk
      CHECK (banking_pay_official_pay_weekday BETWEEN 0 AND 6);
  END IF;
END;
$migration$;

COMMENT ON COLUMN public.settings_defaults.banking_pay_official_pay_weekday IS
  'Authoritative Banking Pay official weekday using PostgreSQL DOW numbering: Sunday=0 through Saturday=6. Official pay-date, rollover, expiry and warning decisions use this value with timezone_id.';

-- ---------------------------------------------------------------------------
-- Immutable case-resolution origin prerequisites
-- Columns remain nullable until the bounded origin backfill has completed.
-- ---------------------------------------------------------------------------

ALTER TABLE public.banking_pay_workbench_session_case_resolutions
  ADD COLUMN IF NOT EXISTS resolution_origin_session_id uuid,
  ADD COLUMN IF NOT EXISTS resolution_origin_pay_date date,
  ADD COLUMN IF NOT EXISTS resolution_origin_source_basis_fingerprint text;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
      AND constraint_row.conname = 'bpay_wb_case_resolution_origin_session_fk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ADD CONSTRAINT bpay_wb_case_resolution_origin_session_fk
      FOREIGN KEY (resolution_origin_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT
      NOT VALID;
  END IF;
END;
$migration$;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_case_resolution_origin_session
  ON public.banking_pay_workbench_session_case_resolutions
  USING btree (resolution_origin_session_id, resolution_origin_pay_date)
  WHERE resolution_origin_session_id IS NOT NULL;

COMMENT ON COLUMN public.banking_pay_workbench_session_case_resolutions.resolution_origin_session_id IS
  'Immutable Workbench session in which this resolution authority was first established.';

COMMENT ON COLUMN public.banking_pay_workbench_session_case_resolutions.resolution_origin_pay_date IS
  'Immutable official pay date of resolution_origin_session_id.';

COMMENT ON COLUMN public.banking_pay_workbench_session_case_resolutions.resolution_origin_source_basis_fingerprint IS
  'Immutable source-basis fingerprint captured when resolution authority was first established.';

-- ---------------------------------------------------------------------------
-- Active-Snooze indexes
-- Ordinary CREATE INDEX is used for compatibility with transactional migration
-- runners. Apply during an appropriate maintenance window if this table is large.
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_pay_item_snoozes_active_expiry
  ON public.pay_item_snoozes
  USING btree (snooze_until_date, candidate_id, id)
  WHERE snooze_until_date IS NOT NULL
    AND cleared_at_utc IS NULL
    AND cancelled_at_utc IS NULL;

CREATE INDEX IF NOT EXISTS idx_pay_item_snoozes_active_timesheet_segment_stable
  ON public.pay_item_snoozes
  USING btree (candidate_id, timesheet_id, segment_stable_key, snooze_kind)
  WHERE cleared_at_utc IS NULL
    AND cancelled_at_utc IS NULL
    AND timesheet_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pay_item_snoozes_active_timesheet_scope
  ON public.pay_item_snoozes
  USING btree (candidate_id, timesheet_id, snooze_kind, snooze_until_date)
  WHERE cleared_at_utc IS NULL
    AND cancelled_at_utc IS NULL
    AND timesheet_id IS NOT NULL;

COMMIT;
