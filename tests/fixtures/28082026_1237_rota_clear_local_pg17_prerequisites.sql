-- Disposable local PostgreSQL 17 only. Provider prerequisites, not app stubs.
-- The complete unchanged repository baseline/release is installed after this.
-- The disposable PG17 image must start with BOTH pgsodium,supabase_vault in
-- shared_preload_libraries and BOTH pgsodium.getkey_script and
-- vault.getkey_script=/usr/lib/postgresql/bin/pgsodium_getkey.sh. Preloading
-- pgsodium alone does not initialise Vault's independent bootstrap key.
-- Use the disposable local postgres owner and PGOPTIONS=-c search_path=public,extensions
-- for canonical export. The image's supabase_admin/auth search path otherwise
-- changes display-only policy-role ordering and auth.users FK qualification.
-- Never alter a hosted role or hosted search_path to reproduce this fixture.
\set ON_ERROR_STOP on
DO $local_only$
BEGIN
  IF current_database() NOT IN ('rota_clear_proof','rota_calendar_proof','rota_continuous_proof','rota_removal_proof','rota_daily_first_proof','rota_daily_recovery_proof','rota_daily_display_proof','rota_daily_window_proof')
     OR current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999
     OR inet_server_addr() IS NULL THEN
    RAISE EXCEPTION 'ROTA_CLEAR_LOCAL_FIXTURE_TARGET_INVALID';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('public', 'private') AND c.relkind IN ('r','p','v')) THEN
    RAISE EXCEPTION 'ROTA_CLEAR_LOCAL_FIXTURE_REQUIRES_EMPTY_DATABASE';
  END IF;
END
$local_only$;
CREATE SCHEMA IF NOT EXISTS extensions;
CREATE SCHEMA IF NOT EXISTS auth;
CREATE EXTENSION IF NOT EXISTS supabase_vault CASCADE;
DO $provider_roles$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN CREATE ROLE anon NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN CREATE ROLE authenticated NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN CREATE ROLE service_role NOLOGIN BYPASSRLS; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticator') THEN CREATE ROLE authenticator NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin') THEN CREATE ROLE supabase_admin NOLOGIN; END IF;
END
$provider_roles$;
CREATE TABLE IF NOT EXISTS auth.users (id uuid PRIMARY KEY);
CREATE OR REPLACE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid
$$;
CREATE OR REPLACE FUNCTION auth.role() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT NULLIF(current_setting('request.jwt.claim.role', true), '')
$$;
CREATE OR REPLACE FUNCTION auth.jwt() RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT COALESCE(NULLIF(current_setting('request.jwt.claims', true), '')::jsonb, '{}'::jsonb)
$$;
