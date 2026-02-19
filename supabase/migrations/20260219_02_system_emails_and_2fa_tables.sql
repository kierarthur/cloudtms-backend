-- 2026-02-19: 2FA DB + SYSTEM email settings
-- Adds:
--   - settings_defaults.system_emails (jsonb) + settings_defaults.system_email (text)
--   - public.tms_user_2fa_trust
--   - public.tms_login_2fa_challenges
--   - seeds auth policy defaults under settings_defaults.import_config_json->auth (non-destructive)

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1) settings_defaults: SYSTEM email settings
-- ─────────────────────────────────────────────────────────────
ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS system_emails jsonb;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS system_email text;

ALTER TABLE public.settings_defaults
  ALTER COLUMN system_emails SET DEFAULT '{}'::jsonb;

UPDATE public.settings_defaults AS sd
SET system_emails = '{}'::jsonb
WHERE sd.system_emails IS NULL;

ALTER TABLE public.settings_defaults
  ALTER COLUMN system_emails SET NOT NULL;

-- Seed SYSTEM webhook/settings (only if empty/null so we don't overwrite later edits)
UPDATE public.settings_defaults AS sd
SET system_emails = jsonb_build_object(
      'headers', jsonb_build_object('Content-Type', 'application/json'),
      'webhook_url', 'https://defaultba10833977be4042a72b025b785d1f.21.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/cf7a7dd6463e4f27a376990ae2ad7e6e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=8zNea8mfsj9QlNIeeLmHoWbm1tqHbhDVlMEzCGa1KG4'
    )
WHERE sd.id = 1
  AND (sd.system_emails IS NULL OR sd.system_emails = '{}'::jsonb);

UPDATE public.settings_defaults AS sd
SET system_email = 'enquiries@arthur-rai.co.uk'
WHERE sd.id = 1
  AND (sd.system_email IS NULL OR btrim(sd.system_email) = '');

-- ─────────────────────────────────────────────────────────────
-- 2) Auth policy defaults in settings_defaults.import_config_json->auth
--    (fills missing keys but does NOT override existing ones)
-- ─────────────────────────────────────────────────────────────
UPDATE public.settings_defaults AS sd
SET import_config_json = jsonb_set(
  sd.import_config_json,
  '{auth}',
  (
    jsonb_build_object(
      'tfa_enabled', true,
      'tfa_code_ttl_seconds', 300,
      'tfa_trust_window_seconds', 28800,
      'tfa_max_attempts', 10,
      'tfa_resend_cooldown_seconds', 30,
      'tfa_max_resends', 5,
      'idle_logout_seconds', 7200,
      'idle_warning_seconds', 300
    )
    || COALESCE(sd.import_config_json->'auth', '{}'::jsonb)
  ),
  true
)
WHERE sd.id = 1;

-- ─────────────────────────────────────────────────────────────
-- 3) 2FA TRUST TABLE (same IP within window)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.tms_user_2fa_trust (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES public.tms_users(id) ON DELETE CASCADE,
  ip_address text NOT NULL,
  verified_at_utc timestamptz NOT NULL DEFAULT now(),
  last_used_at_utc timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tms_user_2fa_trust_user_ip_key UNIQUE (user_id, ip_address)
);

CREATE INDEX IF NOT EXISTS idx_tms_user_2fa_trust_user_ip
  ON public.tms_user_2fa_trust (user_id, ip_address);

CREATE INDEX IF NOT EXISTS idx_tms_user_2fa_trust_verified_at_utc
  ON public.tms_user_2fa_trust (verified_at_utc DESC);

-- ─────────────────────────────────────────────────────────────
-- 4) 2FA CHALLENGES TABLE (one-time 6-digit code, salted+hashed)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.tms_login_2fa_challenges (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES public.tms_users(id) ON DELETE CASCADE,
  ip_address text NOT NULL,

  code_salt text NOT NULL,
  code_hash text NOT NULL,

  expires_at_utc timestamptz NOT NULL,
  used_at_utc timestamptz NULL,

  attempt_count integer NOT NULL DEFAULT 0,
  resend_count integer NOT NULL DEFAULT 0,
  last_sent_at_utc timestamptz NOT NULL DEFAULT now(),

  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tms_login_2fa_challenges_user_ip
  ON public.tms_login_2fa_challenges (user_id, ip_address);

CREATE INDEX IF NOT EXISTS idx_tms_login_2fa_challenges_expires_at_utc
  ON public.tms_login_2fa_challenges (expires_at_utc);

CREATE INDEX IF NOT EXISTS idx_tms_login_2fa_challenges_used_at_utc
  ON public.tms_login_2fa_challenges (used_at_utc);

COMMIT;
