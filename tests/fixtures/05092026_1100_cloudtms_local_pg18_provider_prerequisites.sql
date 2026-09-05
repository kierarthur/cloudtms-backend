-- Disposable local PostgreSQL 18 compatibility fixture only.
--
-- The stock PostgreSQL image does not ship the hosting provider's auth/vault
-- extensions. This file supplies only the exact provider API surface consumed
-- by the unchanged CloudTMS NEW baseline. It is not a production vault,
-- contains no customer data or durable credential, and is excluded from the
-- exported CloudTMS application contract.
\set ON_ERROR_STOP on

DO $local_only$
BEGIN
  IF current_database() !~ '^banking_modal_v2_(test|contract_[0-9]{8}|release[0-9]*_[0-9]{8})$'
     OR current_setting('server_version_num')::integer NOT BETWEEN 180000 AND 189999
     OR inet_server_addr() IS NULL THEN
    RAISE EXCEPTION 'CLOUDTMS_LOCAL_PG18_PROVIDER_FIXTURE_TARGET_INVALID';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('public', 'private', 'auth', 'vault')
      AND c.relkind IN ('r', 'p', 'v')
  ) THEN
    RAISE EXCEPTION 'CLOUDTMS_LOCAL_PG18_PROVIDER_FIXTURE_REQUIRES_EMPTY_DATABASE';
  END IF;
END
$local_only$;

CREATE SCHEMA extensions;
CREATE SCHEMA auth;
CREATE SCHEMA vault;
CREATE EXTENSION pgcrypto WITH SCHEMA extensions;

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

CREATE TABLE vault.secrets (
  id uuid PRIMARY KEY DEFAULT extensions.gen_random_uuid(),
  name text,
  description text NOT NULL DEFAULT '',
  secret text NOT NULL,
  key_id uuid,
  nonce bytea,
  created_at timestamptz NOT NULL DEFAULT current_timestamp,
  updated_at timestamptz NOT NULL DEFAULT current_timestamp
);

CREATE VIEW vault.decrypted_secrets AS
SELECT
  id,
  name,
  description,
  secret,
  secret AS decrypted_secret,
  key_id,
  nonce,
  created_at,
  updated_at
FROM vault.secrets;

CREATE FUNCTION vault.create_secret(
  new_secret text,
  new_name text DEFAULT NULL,
  new_description text DEFAULT '',
  new_key_id uuid DEFAULT NULL
)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path TO ''
AS $$
  INSERT INTO vault.secrets (secret, name, description, key_id)
  VALUES (new_secret, new_name, new_description, new_key_id)
  RETURNING id
$$;
