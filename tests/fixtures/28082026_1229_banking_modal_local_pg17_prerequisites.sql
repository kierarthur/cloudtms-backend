-- Disposable local PostgreSQL 17 only. Provider prerequisites, not app stubs.
-- The complete unchanged repository baseline/release is installed after this.
\set ON_ERROR_STOP on
DO $local_only$
BEGIN
  IF current_database() !~ '^banking_modal_v2_(test|contract_[0-9]{8}|release[0-9]*_[0-9]{8})$'
     OR current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999
     OR inet_server_addr() IS NULL THEN
    RAISE EXCEPTION 'BANKING_MODAL_LOCAL_FIXTURE_TARGET_INVALID';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('public', 'private') AND c.relkind IN ('r','p','v')) THEN
    RAISE EXCEPTION 'BANKING_MODAL_LOCAL_FIXTURE_REQUIRES_EMPTY_DATABASE';
  END IF;
END
$local_only$;
CREATE SCHEMA extensions;
CREATE SCHEMA auth;
CREATE EXTENSION supabase_vault CASCADE;
DO $provider_roles$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN CREATE ROLE anon NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN CREATE ROLE authenticated NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN CREATE ROLE service_role NOLOGIN BYPASSRLS; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticator') THEN CREATE ROLE authenticator NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin') THEN CREATE ROLE supabase_admin NOLOGIN; END IF;
END
$provider_roles$;
CREATE TABLE auth.users (id uuid PRIMARY KEY);
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid
$$;
CREATE FUNCTION auth.role() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT NULLIF(current_setting('request.jwt.claim.role', true), '')
$$;
CREATE FUNCTION auth.jwt() RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT COALESCE(NULLIF(current_setting('request.jwt.claims', true), '')::jsonb, '{}'::jsonb)
$$;
