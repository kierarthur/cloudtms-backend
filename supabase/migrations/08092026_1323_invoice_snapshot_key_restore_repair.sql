-- Repair an Invoice V8 snapshot-signing key whose metadata survived a
-- provider restore but whose protected Vault secret did not.
--
-- A healthy current key is left completely unchanged. When the current key
-- cannot be resolved to a non-empty secret, retire only that unusable key
-- identity and create a new key and secret inside PostgreSQL. Supabase uses
-- Vault when it is installed; provider-neutral PostgreSQL uses a locked-down
-- private fallback table. Reusing the old key identity with new secret
-- material would make historical signatures ambiguous, so this migration
-- deliberately rotates the identity as well.

create table if not exists private.invoice_async_snapshot_hmac_secret_material (
  secret_id uuid primary key,
  secret_material text not null,
  created_at_utc timestamptz not null default pg_catalog.now(),
  constraint invoice_async_snapshot_hmac_secret_material_nonempty_ck
    check (pg_catalog.length(secret_material) >= 43)
);

revoke all on table private.invoice_async_snapshot_hmac_secret_material
  from public, anon, authenticated, service_role;

comment on table private.invoice_async_snapshot_hmac_secret_material is
  'Provider-neutral private fallback for Invoice snapshot signing material when Supabase Vault is unavailable.';

do $migration$
declare
  v_now constant timestamptz := pg_catalog.statement_timestamp();
  v_key_id text;
  v_secret_name text;
  v_secret_id uuid;
  v_secret_material text;
begin
  lock table private.invoice_async_snapshot_hmac_keys
    in share row exclusive mode;

  if not exists (
    select 1
    from private.invoice_async_snapshot_hmac_keys k
    left join vault.decrypted_secrets s on s.id = k.vault_secret_id
    left join private.invoice_async_snapshot_hmac_secret_material m
      on m.secret_id = k.vault_secret_id
    where k.is_current
      and k.active_from_utc <= v_now
      and (k.active_to_utc is null or k.active_to_utc > v_now)
      and coalesce(
        nullif(s.decrypted_secret, ''),
        nullif(m.secret_material, '')
      ) is not null
  ) then
    update private.invoice_async_snapshot_hmac_keys
    set
      is_current = false,
      active_to_utc = greatest(
        coalesce(active_to_utc, v_now),
        active_from_utc + interval '1 millisecond'
      ),
      verify_until_utc = greatest(
        coalesce(verify_until_utc, '-infinity'::timestamptz),
        greatest(v_now, active_from_utc + interval '1 millisecond')
          + interval '30 minutes'
      )
    where is_current;

    v_key_id := 'restore-' || pg_catalog.substr(
      extensions.gen_random_uuid()::text,
      1,
      16
    );
    v_secret_name := 'cloudtms_invoice_snapshot_' || pg_catalog.replace(
      extensions.gen_random_uuid()::text,
      '-',
      ''
    );
    v_secret_material := pg_catalog.encode(
      extensions.gen_random_bytes(48),
      'base64'
    );

    if pg_catalog.to_regprocedure(
      'vault.create_secret(text,text,text,uuid)'
    ) is not null then
      execute 'select vault.create_secret($1,$2,$3,$4)'
      into v_secret_id
      using
        v_secret_material,
        v_secret_name,
        'CloudTMS invoice snapshot HMAC repaired after provider restore',
        null::uuid;
    else
      v_secret_id := extensions.gen_random_uuid();
      insert into private.invoice_async_snapshot_hmac_secret_material (
        secret_id,
        secret_material
      ) values (
        v_secret_id,
        v_secret_material
      );
    end if;

    insert into private.invoice_async_snapshot_hmac_keys (
      key_id,
      vault_secret_id,
      active_from_utc,
      is_current
    ) values (
      v_key_id,
      v_secret_id,
      v_now,
      true
    );

    v_secret_material := null;
  end if;
end
$migration$;
