-- Verifies either native Vault or the restricted Miget compatibility authority.
-- No secret value, ciphertext or key material is returned.

\set ON_ERROR_STOP on

do $verify_vault$
declare
  v_role text;
  v_unreadable integer;
begin
  if to_regclass('vault.secrets') is null
     or to_regclass('vault.decrypted_secrets') is null
     or to_regprocedure('vault.create_secret(text,text,text)') is null then
    raise exception 'VAULT_AUTHORITY_INCOMPLETE';
  end if;

  if to_regclass('vault._cloudtms_key_material') is null then
    return;
  end if;

  foreach v_role in array array[
    'anon', 'authenticated', 'service_role', 'authenticator', 'supabase_admin'
  ]::text[]
  loop
    if exists (select 1 from pg_catalog.pg_roles where rolname = v_role) then
      if pg_catalog.has_schema_privilege(v_role, 'vault', 'USAGE')
         or pg_catalog.has_table_privilege(v_role, 'vault.secrets', 'SELECT')
         or pg_catalog.has_table_privilege(v_role, 'vault.decrypted_secrets', 'SELECT')
         or pg_catalog.has_table_privilege(v_role, 'vault._cloudtms_key_material', 'SELECT')
         or pg_catalog.has_function_privilege(
           v_role,
           'vault.create_secret(text,text,text)',
           'EXECUTE'
         ) then
        raise exception 'VAULT_BROWSER_OR_SERVICE_ROLE_EXPOSURE: %', v_role;
      end if;
    end if;
  end loop;

  if not exists (
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'vault'
      and c.relname in ('secrets', '_cloudtms_key_material')
      and c.relrowsecurity
    group by n.nspname
    having count(*) = 2
  ) then
    raise exception 'VAULT_COMPATIBILITY_RLS_MISSING';
  end if;

  select count(*)::integer
  into v_unreadable
  from private.invoice_async_snapshot_hmac_keys as key_row
  left join vault.decrypted_secrets as secret_row
    on secret_row.id = key_row.vault_secret_id
  where key_row.is_current
    and nullif(secret_row.decrypted_secret, '') is null;

  if v_unreadable <> 0 then
    raise exception 'VAULT_ACTIVE_HMAC_SECRET_UNREADABLE';
  end if;

  if exists (
    select 1
    from vault.secrets
    where encrypted_secret is null
       or pg_catalog.octet_length(encrypted_secret) = 0
  ) then
    raise exception 'VAULT_COMPATIBILITY_CIPHERTEXT_MISSING';
  end if;
end
$verify_vault$;
