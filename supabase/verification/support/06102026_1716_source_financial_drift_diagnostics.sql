-- Rollback-only failure diagnostics. No runtime owner or financial policy.
-- Retain only primary-key, complete-row and per-field SHA256 digests. Never
-- retain/return customer values, identifiers, monetary values or payloads.
create temporary table ws_verify_financial_drift_rows(
  phase text not null check(phase in ('BEFORE','AFTER')),
  relation_oid oid not null,
  key_sha256 text not null,
  row_sha256 text not null,
  field_sha256 jsonb not null,
  primary key(phase,relation_oid,key_sha256)
) on commit drop;

create function pg_temp.ws_verify_financial_drift_capture(p_relation regclass,p_phase text)
returns text language plpgsql set search_path='' as $capture$
declare v_keys text;v_fingerprint text;
begin
  if p_phase not in ('BEFORE','AFTER') or p_phase is null then
    raise exception 'WS_VERIFY_DRIFT_PHASE_INVALID';
  end if;
  select pg_catalog.string_agg(pg_catalog.format('j->%L',a.attname),',' order by k.ord)
    into v_keys
    from pg_catalog.pg_index i
    cross join lateral pg_catalog.unnest(i.indkey) with ordinality k(attnum,ord)
    join pg_catalog.pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.attnum
   where i.indrelid=p_relation and i.indisprimary and k.ord<=i.indnkeyatts;
  if v_keys is null then raise exception 'WS_VERIFY_DRIFT_PRIMARY_KEY_REQUIRED'; end if;
  delete from pg_temp.ws_verify_financial_drift_rows
   where relation_oid=p_relation::oid and phase=p_phase;
  execute pg_catalog.format($query$
    insert into pg_temp.ws_verify_financial_drift_rows
      (phase,relation_oid,key_sha256,row_sha256,field_sha256)
    select $1,$2,
      pg_catalog.encode(extensions.digest(pg_catalog.jsonb_build_array(%s)::text,'sha256'),'hex'),
      pg_catalog.encode(extensions.digest(j::text,'sha256'),'hex'),
      (select pg_catalog.jsonb_object_agg(f.key,
        pg_catalog.encode(extensions.digest(f.value::text,'sha256'),'hex'))
         from pg_catalog.jsonb_each(j) f)
    from (select pg_catalog.to_jsonb(t) as j from %s t) complete_rows
  $query$,v_keys,p_relation) using p_phase,p_relation::oid;
  -- Identical count/multiplicity/full-row digest algorithm to the original
  -- fingerprint guard. This additional capture does not replace that guard.
  select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
    pg_catalog.encode(extensions.digest(coalesce(
      pg_catalog.string_agg(row_sha256,'' order by row_sha256),''),'sha256'),'hex'))::text
    into v_fingerprint
    from pg_temp.ws_verify_financial_drift_rows
   where relation_oid=p_relation::oid and phase=p_phase;
  return v_fingerprint;
end $capture$;

create function pg_temp.ws_verify_financial_drift_detail(p_before jsonb,p_after jsonb,p_worker_call integer)
returns text language plpgsql set search_path='' as $detail$
declare v_relation text;v_oid oid;v_before_hash text;v_after_hash text;
  v_added bigint;v_removed bigint;v_changed bigint;v_fields jsonb;
  v_relations jsonb:='[]'::jsonb;
begin
  for v_relation in select k from pg_catalog.jsonb_object_keys(p_before||p_after) k
    where p_before->k is distinct from p_after->k order by k
  loop
    v_oid:=v_relation::regclass::oid;
    v_after_hash:=pg_temp.ws_verify_financial_drift_capture(v_relation::regclass,'AFTER');
    select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
      pg_catalog.encode(extensions.digest(coalesce(
        pg_catalog.string_agg(row_sha256,'' order by row_sha256),''),'sha256'),'hex'))::text
      into v_before_hash
      from pg_temp.ws_verify_financial_drift_rows
     where relation_oid=v_oid and phase='BEFORE';
    select pg_catalog.count(*) filter(where b.key_sha256 is null),
           pg_catalog.count(*) filter(where a.key_sha256 is null),
           pg_catalog.count(*) filter(where b.key_sha256 is not null
             and a.key_sha256 is not null and b.row_sha256 is distinct from a.row_sha256)
      into v_added,v_removed,v_changed
      from (select * from pg_temp.ws_verify_financial_drift_rows
        where relation_oid=v_oid and phase='BEFORE') b
      full join (select * from pg_temp.ws_verify_financial_drift_rows
        where relation_oid=v_oid and phase='AFTER') a using(key_sha256);
    select coalesce(pg_catalog.jsonb_object_agg(changed.field,changed.rows),'{}'::jsonb)
      into v_fields from (
      select f.key as field,pg_catalog.count(*) as rows
        from pg_temp.ws_verify_financial_drift_rows b
        join pg_temp.ws_verify_financial_drift_rows a
          on a.phase='AFTER' and a.relation_oid=b.relation_oid and a.key_sha256=b.key_sha256
        cross join lateral pg_catalog.jsonb_each(b.field_sha256) f
       where b.phase='BEFORE' and b.relation_oid=v_oid
         and f.value is distinct from a.field_sha256->f.key
       group by f.key
    ) changed;
    v_relations:=v_relations||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'relation',v_relation,'added_rows',v_added,'removed_rows',v_removed,
      'changed_rows',v_changed,'changed_fields_row_counts',v_fields,
      'before_matches_guard',v_before_hash is not distinct from p_before->>v_relation,
      'after_matches_guard',v_after_hash is not distinct from p_after->>v_relation));
  end loop;
  return pg_catalog.jsonb_build_object('worker_call',p_worker_call,'relations',v_relations)::text;
end $detail$;
