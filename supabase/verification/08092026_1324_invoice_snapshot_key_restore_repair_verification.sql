do $verification$
declare
  v_current_count integer;
  v_healthy_count integer;
begin
  select
    count(*) filter (where k.is_current),
    count(*) filter (
      where k.is_current
        and k.active_from_utc <= pg_catalog.statement_timestamp()
        and (k.active_to_utc is null
          or k.active_to_utc > pg_catalog.statement_timestamp())
        and coalesce(
          nullif(s.decrypted_secret, ''),
          nullif(m.secret_material, '')
        ) is not null
    )
  into v_current_count, v_healthy_count
  from private.invoice_async_snapshot_hmac_keys k
  left join vault.decrypted_secrets s on s.id = k.vault_secret_id
  left join private.invoice_async_snapshot_hmac_secret_material m
    on m.secret_id = k.vault_secret_id;

  if v_current_count <> 1 or v_healthy_count <> 1 then
    raise exception 'INVOICE_SNAPSHOT_SIGNING_KEY_RESTORE_REPAIR_FAILED';
  end if;

  if not coalesce(
    (public.invoice_async_contract_get_v2()->>'snapshot_signing_ready')::boolean,
    false
  ) then
    raise exception 'INVOICE_SNAPSHOT_SIGNING_CONTRACT_NOT_READY';
  end if;
end
$verification$;
