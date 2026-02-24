-- Add remittance header/footer message settings (safe to rerun)
BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS remittance_header_message text;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS remittance_footer_message text;

COMMIT;
