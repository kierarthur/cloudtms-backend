-- CloudTMS TEST Invoice Async V8/V2 contract corrections.
-- Adds only private snapshot-key metadata and the manifest-carrier uniqueness
-- authority. The HMAC secret is generated inside PostgreSQL and stored by
-- Supabase Vault; no secret value is present in this file or returned.

create table if not exists private.invoice_async_snapshot_hmac_keys (
  key_id text primary key,
  vault_secret_id uuid not null unique,
  active_from_utc timestamptz not null,
  active_to_utc timestamptz,
  verify_until_utc timestamptz,
  is_current boolean not null default false,
  created_at_utc timestamptz not null default now(),
  constraint invoice_async_snapshot_hmac_keys_key_id_ck
    check (key_id ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  constraint invoice_async_snapshot_hmac_keys_window_ck
    check (
      (active_to_utc is null or active_to_utc > active_from_utc)
      and (
        verify_until_utc is null
        or verify_until_utc >= coalesce(active_to_utc, active_from_utc)
      )
    )
);

revoke all on table private.invoice_async_snapshot_hmac_keys
  from public, anon, authenticated, service_role;

create unique index if not exists
  idx_invoice_async_snapshot_hmac_keys_one_current
on private.invoice_async_snapshot_hmac_keys (is_current)
where is_current;

do $migration$
declare
  v_key_id constant text := 'test-v8-20260727-1';
  v_secret_name constant text :=
    'cloudtms_invoice_async_snapshot_hmac_v2_test_20260727_1';
  v_secret_id uuid;
begin
  if not exists (
    select 1
    from private.invoice_async_snapshot_hmac_keys k
    where k.is_current
      and k.active_from_utc <= statement_timestamp()
      and (k.active_to_utc is null or k.active_to_utc > statement_timestamp())
  ) then
    select s.id
    into v_secret_id
    from vault.secrets s
    where s.name = v_secret_name
    order by s.created_at desc
    limit 1;

    if v_secret_id is null then
      v_secret_id := vault.create_secret(
        encode(extensions.gen_random_bytes(48), 'base64'),
        v_secret_name,
        'CloudTMS TEST Invoice Async V8/V2 candidate snapshot HMAC'
      );
    end if;

    update private.invoice_async_snapshot_hmac_keys
    set
      is_current = false,
      active_to_utc = coalesce(active_to_utc, statement_timestamp()),
      verify_until_utc = greatest(
        coalesce(verify_until_utc, '-infinity'::timestamptz),
        statement_timestamp() + interval '30 minutes'
      )
    where is_current;

    insert into private.invoice_async_snapshot_hmac_keys (
      key_id,
      vault_secret_id,
      active_from_utc,
      is_current
    )
    values (
      v_key_id,
      v_secret_id,
      statement_timestamp(),
      true
    )
    on conflict (key_id) do update
    set
      vault_secret_id = excluded.vault_secret_id,
      active_from_utc = least(
        private.invoice_async_snapshot_hmac_keys.active_from_utc,
        excluded.active_from_utc
      ),
      active_to_utc = null,
      verify_until_utc = null,
      is_current = true;
  end if;
end;
$migration$;

create unique index if not exists
  idx_invoice_manifest_carrier_identity_v8
on public.invoice_operation_chunks (
  operation_id,
  manifest_generation,
  selection_key
)
where is_manifest_member
  and selection_key is not null
  and replaced_by_chunk_id is null;

comment on table private.invoice_async_snapshot_hmac_keys is
  'Private metadata linking rotating Invoice V8 snapshot keys to Supabase Vault.';
