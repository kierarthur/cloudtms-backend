-- CloudTMS TEST additive/idempotent hierarchy guard.
--
-- Contract override -> current client setting -> global default is the single
-- policy hierarchy for both HealthRoster and NHSP import auto-authorisation.
-- Every column is added independently so this migration is safe when a
-- partially upgraded database already has some or all of the hierarchy.

alter table public.settings_defaults
  add column if not exists healthroster_import_auto_authorise_default boolean not null default true,
  add column if not exists nhsp_import_auto_authorise_default boolean not null default false,
  add column if not exists auto_authorise_on_validation boolean not null default false;

alter table public.client_settings
  add column if not exists healthroster_import_auto_authorise boolean not null default true,
  add column if not exists nhsp_import_auto_authorise boolean not null default false;

alter table public.contracts
  add column if not exists healthroster_import_auto_authorise_override boolean,
  add column if not exists nhsp_import_auto_authorise_override boolean;

comment on column public.settings_defaults.healthroster_import_auto_authorise_default
  is 'Global HealthRoster auto-authorisation fallback used only when no contract override or current client value applies.';
comment on column public.settings_defaults.nhsp_import_auto_authorise_default
  is 'Global NHSP auto-authorisation fallback used only when no contract override or current client value applies.';
comment on column public.client_settings.healthroster_import_auto_authorise
  is 'Current effective client-level HealthRoster auto-authorisation policy.';
comment on column public.client_settings.nhsp_import_auto_authorise
  is 'Current effective client-level NHSP auto-authorisation policy.';
comment on column public.contracts.healthroster_import_auto_authorise_override
  is 'Nullable contract override for HealthRoster auto-authorisation; null inherits the current client setting.';
comment on column public.contracts.nhsp_import_auto_authorise_override
  is 'Nullable contract override for NHSP auto-authorisation; null inherits the current client setting.';
