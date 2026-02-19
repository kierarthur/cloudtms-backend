-- 2026-02-19: Add settings_defaults.system_emails (+ optional system_email) for PowerAutomate “system” emails (2FA etc.)

BEGIN;

-- 1) Add the JSONB settings blob
ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS system_emails jsonb;

-- 2) (Optional but useful) Add a dedicated “from/primary” email like finance_email
ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS system_email text;

-- 3) Seed values (assumes your canonical settings_defaults row is id=1)
UPDATE public.settings_defaults
SET
  system_emails = jsonb_build_object(
    'headers', jsonb_build_object(
      'Content-Type', 'application/json'
    ),
    'webhook_url', 'https://defaultba10833977be4042a72b025b785d1f.21.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/cf7a7dd6463e4f27a376990ae2ad7e6e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=8zNea8mfsj9QlNIeeLmHoWbm1tqHbhDVlMEzCGa1KG4'
  ),
  system_email = 'enquiries@arthur-rai.co.uk'
WHERE id = 1;

-- 4) Defaults (safe)
ALTER TABLE public.settings_defaults
  ALTER COLUMN system_emails SET DEFAULT '{}'::jsonb;

COMMIT;
