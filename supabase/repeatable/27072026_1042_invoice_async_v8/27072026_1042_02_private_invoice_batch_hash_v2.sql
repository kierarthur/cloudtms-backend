create or replace function private._invoice_batch_hash_v2(
  p_value jsonb
) returns text
language sql
immutable
security invoker
set search_path to 'public','private','extensions','pg_temp'
as $function$
  select encode(
    extensions.digest(
      convert_to(private._invoice_batch_canonical_text_v2(p_value), 'UTF8'),
      'sha256'
    ),
    'hex'
  );
$function$;

alter function private._invoice_batch_hash_v2(jsonb) owner to postgres;
revoke all on function private._invoice_batch_hash_v2(jsonb)
  from public, anon, authenticated;
grant execute on function private._invoice_batch_hash_v2(jsonb)
  to service_role;
