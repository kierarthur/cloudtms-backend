-- Provider-neutral Vault compatibility for Miget PostgreSQL. Native Supabase
-- Vault is preserved unchanged when its complete API is present. On providers
-- without that extension, generated values are encrypted with pgcrypto and all
-- storage/decryption authority remains database-owner-only in schema vault.

\set ON_ERROR_STOP on

begin;

do $vault_compatibility$
declare
  v_schema_present boolean := to_regnamespace('vault') is not null;
  v_secrets_present boolean := to_regclass('vault.secrets') is not null;
  v_decrypted_present boolean := to_regclass('vault.decrypted_secrets') is not null;
  v_create_present boolean :=
    to_regprocedure('vault.create_secret(text,text,text)') is not null;
begin
  if v_secrets_present and v_decrypted_present and v_create_present then
    return;
  end if;

  if v_schema_present or v_secrets_present or v_decrypted_present or v_create_present then
    raise exception 'VAULT_COMPATIBILITY_PARTIAL_AUTHORITY_REFUSED';
  end if;

  execute 'create schema vault';
  execute 'revoke all on schema vault from public';

  execute $ddl$
    create table vault._cloudtms_key_material (
      singleton boolean primary key default true check (singleton),
      key_material text not null,
      created_at timestamptz not null default clock_timestamp()
    )
  $ddl$;

  execute $ddl$
    create table vault.secrets (
      id uuid primary key default extensions.gen_random_uuid(),
      name text not null unique,
      description text,
      encrypted_secret bytea not null,
      created_at timestamptz not null default clock_timestamp(),
      updated_at timestamptz not null default clock_timestamp()
    )
  $ddl$;

  execute $ddl$
    insert into vault._cloudtms_key_material(singleton, key_material)
    values (
      true,
      pg_catalog.encode(extensions.gen_random_bytes(48), 'base64')
    )
  $ddl$;

  execute $ddl$
    create function vault.create_secret(
      new_secret text,
      new_name text default null::text,
      new_description text default null::text
    ) returns uuid
    language plpgsql
    security definer
    set search_path to 'pg_catalog', 'vault', 'extensions', 'pg_temp'
    as $function$
    declare
      v_key text;
      v_id uuid;
    begin
      if new_secret is null or new_secret = '' then
        raise exception 'VAULT_SECRET_VALUE_REQUIRED';
      end if;
      if nullif(pg_catalog.btrim(coalesce(new_name, '')), '') is null then
        raise exception 'VAULT_SECRET_NAME_REQUIRED';
      end if;

      select key_material
      into v_key
      from vault._cloudtms_key_material
      where singleton;

      if v_key is null then
        raise exception 'VAULT_COMPATIBILITY_KEY_MISSING';
      end if;

      insert into vault.secrets(name, description, encrypted_secret)
      values (
        pg_catalog.btrim(new_name),
        new_description,
        extensions.pgp_sym_encrypt(
          new_secret,
          v_key,
          'cipher-algo=aes256,compress-algo=1'
        )
      )
      on conflict (name) do update
      set description = coalesce(
            excluded.description,
            vault.secrets.description
          ),
          updated_at = pg_catalog.clock_timestamp()
      returning id into v_id;

      return v_id;
    end
    $function$
  $ddl$;

  execute $ddl$
    create view vault.decrypted_secrets
    with (security_barrier = true)
    as
    select
      secret_row.id,
      secret_row.name,
      secret_row.description,
      secret_row.created_at,
      secret_row.updated_at,
      extensions.pgp_sym_decrypt(
        secret_row.encrypted_secret,
        key_row.key_material
      ) as decrypted_secret
    from vault.secrets as secret_row
    cross join vault._cloudtms_key_material as key_row
    where key_row.singleton
  $ddl$;

  execute 'alter table vault._cloudtms_key_material enable row level security';
  execute 'alter table vault.secrets enable row level security';
  execute 'revoke all on table vault._cloudtms_key_material from public';
  execute 'revoke all on table vault.secrets from public';
  execute 'revoke all on table vault.decrypted_secrets from public';
  execute 'revoke all on function vault.create_secret(text,text,text) from public';
end
$vault_compatibility$;

do $vault_compatibility_acl$
declare
  v_role text;
begin
  if to_regclass('vault._cloudtms_key_material') is null then
    return;
  end if;

  foreach v_role in array array[
    'anon', 'authenticated', 'service_role', 'authenticator', 'supabase_admin'
  ]::text[]
  loop
    if exists (select 1 from pg_catalog.pg_roles where rolname = v_role) then
      execute format('revoke all on schema vault from %I', v_role);
      execute format(
        'revoke all on table vault._cloudtms_key_material, vault.secrets, vault.decrypted_secrets from %I',
        v_role
      );
      execute format(
        'revoke all on function vault.create_secret(text,text,text) from %I',
        v_role
      );
    end if;
  end loop;
end
$vault_compatibility_acl$;

commit;
