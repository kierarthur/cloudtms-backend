-- Rollback-contained verification support only; never a runtime definition.
-- Compare complete rows, including multiplicity, without constructing one JSON
-- array containing the populated database's complete financial payloads. That
-- array can exceed PostgreSQL's 268435455-byte JSONB element limit. Aggregate
-- only fixed-size full-row SHA256 digests; no data is omitted or returned.
create or replace function pg_temp.ws_verify_full_relation_fingerprint(p_relation regclass)
returns text language plpgsql set search_path='' as $fingerprint$
declare v_fingerprint text;
begin
  execute pg_catalog.format($query$
    select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
      pg_catalog.encode(extensions.digest(coalesce(
        pg_catalog.string_agg(row_digest,'' order by row_digest),''),'sha256'),'hex'))::text
    from (select pg_catalog.encode(extensions.digest(
      pg_catalog.to_jsonb(row_value)::text,'sha256'),'hex') as row_digest
      from %s row_value) complete_rows
  $query$,p_relation) into v_fingerprint;
  return v_fingerprint;
end;
$fingerprint$;

