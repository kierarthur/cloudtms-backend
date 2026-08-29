BEGIN;

ALTER TABLE public.timesheets
  ADD COLUMN IF NOT EXISTS archived_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS archived_by_user_id uuid,
  ADD COLUMN IF NOT EXISTS archived_reason_code text;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.timesheets'::regclass
      AND conname = 'timesheets_archived_by_user_id_fkey'
  ) THEN
    ALTER TABLE public.timesheets
      ADD CONSTRAINT timesheets_archived_by_user_id_fkey
      FOREIGN KEY (archived_by_user_id)
      REFERENCES public.tms_users(id)
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.timesheets'::regclass
      AND conname = 'timesheets_archive_metadata_coherent_chk'
  ) THEN
    ALTER TABLE public.timesheets
      ADD CONSTRAINT timesheets_archive_metadata_coherent_chk
      CHECK (
        (
          archived_at_utc IS NULL
          AND archived_by_user_id IS NULL
          AND archived_reason_code IS NULL
        )
        OR
        (
          archived_at_utc IS NOT NULL
          AND archived_by_user_id IS NOT NULL
          AND NULLIF(BTRIM(archived_reason_code), '') IS NOT NULL
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.timesheets'::regclass
      AND conname = 'timesheets_archived_not_authorised_chk'
  ) THEN
    ALTER TABLE public.timesheets
      ADD CONSTRAINT timesheets_archived_not_authorised_chk
      CHECK (archived_at_utc IS NULL OR authorised_at_server IS NULL);
  END IF;
END;
$migration$;

CREATE INDEX IF NOT EXISTS idx_timesheets_active_tools
  ON public.timesheets (is_current, week_ending_date DESC, timesheet_id)
  WHERE archived_at_utc IS NULL;

CREATE INDEX IF NOT EXISTS idx_timesheets_archived_tools
  ON public.timesheets (archived_at_utc DESC, week_ending_date DESC, timesheet_id)
  WHERE archived_at_utc IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_timesheets_archive_actor
  ON public.timesheets (archived_by_user_id, archived_at_utc DESC)
  WHERE archived_at_utc IS NOT NULL;

COMMENT ON COLUMN public.timesheets.archived_at_utc IS
  'Operational Archive timestamp. Archive is reversible metadata and must not mutate financial history.';
COMMENT ON COLUMN public.timesheets.archived_by_user_id IS
  'Authenticated CloudTMS user who archived the current authoritative timesheet.';
COMMENT ON COLUMN public.timesheets.archived_reason_code IS
  'Machine-stable Archive reason code. Delete-converted Archive uses FINANCIAL_HISTORY_PREVENTED_DELETE.';

COMMIT;
