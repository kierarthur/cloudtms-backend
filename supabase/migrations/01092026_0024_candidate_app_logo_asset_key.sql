-- One-time CloudTMS schema migration: dedicated MyTMS Candidate app logo pointer.
--
-- `settings_defaults.agency_logo` remains the established CloudTMS document
-- branding authority.  The Candidate app logo is deliberately separate so an
-- Office app-branding change cannot alter invoice or Timesheet presentation.

\set ON_ERROR_STOP on

begin;

alter table public.settings_defaults
  add column if not exists candidate_app_logo_asset_key text;

do $$
begin
  if not exists (
    select 1
    from pg_catalog.pg_constraint c
    join pg_catalog.pg_class t on t.oid = c.conrelid
    join pg_catalog.pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'settings_defaults'
      and c.conname = 'settings_defaults_candidate_app_logo_asset_key_ck'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_app_logo_asset_key_ck
      check (
        candidate_app_logo_asset_key is null
        or candidate_app_logo_asset_key
          ~ '^candidate-app/branding/[0-9a-f]{64}\.png$'
      );
  end if;
end
$$;

comment on column public.settings_defaults.candidate_app_logo_asset_key is
  'Private content-addressed R2 pointer for the MyTMS Candidate app agency logo; independent of document branding.';

commit;
