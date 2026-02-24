-- Add PAYE remittance toggle (safe to rerun)
BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS paye_remittances_enabled boolean;

-- Ensure deterministic default + non-null (handles cases where column already exists)
ALTER TABLE public.settings_defaults
  ALTER COLUMN paye_remittances_enabled SET DEFAULT false;

UPDATE public.settings_defaults
  SET paye_remittances_enabled = false
  WHERE paye_remittances_enabled IS NULL;

ALTER TABLE public.settings_defaults
  ALTER COLUMN paye_remittances_enabled SET NOT NULL;

COMMIT;
