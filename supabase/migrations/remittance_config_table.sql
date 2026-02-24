-- Add remittance inclusion config (safe to rerun)
BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS remittance_includes_json jsonb;

-- Optional: enforce JSON is either NULL or an object containing weekly + daily keys
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.conname = 'settings_defaults_remittance_includes_json_chk'
      AND n.nspname = 'public'
      AND t.relname = 'settings_defaults'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_remittance_includes_json_chk
      CHECK (
        remittance_includes_json IS NULL
        OR (
          jsonb_typeof(remittance_includes_json) = 'object'
          AND (remittance_includes_json ? 'weekly')
          AND (remittance_includes_json ? 'daily')
        )
      );
  END IF;
END $$;

COMMIT;
