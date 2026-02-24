BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS rail_default_funding_account_ref text NULL;

INSERT INTO public.settings_defaults (
  id,
  rail_provider_default,
  rail_env_default,
  rail_supports_scheduling,
  rail_supports_name_check,
  rail_supports_auto_execute,
  funds_warning_hours_json,
  rail_default_funding_account_ref,
  updated_at
)
VALUES (
  1,
  'REVOLUT',
  'PROD',
  true,
  true,
  true,
  '[24, 12]'::jsonb,
  NULL,
  now()
)
ON CONFLICT (id) DO UPDATE
SET
  rail_provider_default              = EXCLUDED.rail_provider_default,
  rail_env_default                   = EXCLUDED.rail_env_default,
  rail_supports_scheduling           = EXCLUDED.rail_supports_scheduling,
  rail_supports_name_check           = EXCLUDED.rail_supports_name_check,
  rail_supports_auto_execute         = EXCLUDED.rail_supports_auto_execute,
  funds_warning_hours_json           = EXCLUDED.funds_warning_hours_json,
  rail_default_funding_account_ref   = EXCLUDED.rail_default_funding_account_ref,
  updated_at                         = EXCLUDED.updated_at;

COMMIT;
