-- Include only inside a rollback-contained verifier, before fixture INSERTs.
-- Records keys and counters for this transaction, never reads business history.
-- Any UPDATE/DELETE of a row not inserted by this fixture is rejected outright.
create temporary table if not exists ws_verify_relations(
  rel oid primary key, key_expression text not null,
  inserts bigint not null default 0, updates bigint not null default 0,
  deletes bigint not null default 0
) on commit drop;
create temporary table if not exists ws_verify_keys(
  rel oid not null, key jsonb not null, primary key(rel,key)
) on commit drop;

create or replace function pg_temp.ws_verify_capture() returns trigger language plpgsql
set search_path='' as $f$
declare expr text; old_key jsonb; new_key jsonb;
begin
  if tg_op='TRUNCATE' then raise exception 'WS_VERIFY_TRUNCATE_FORBIDDEN'; end if;
  select key_expression into strict expr from pg_temp.ws_verify_relations where rel=tg_relid;
  if tg_op in ('UPDATE','DELETE') then
    execute 'select pg_catalog.jsonb_build_array('||expr||')' into old_key using old;
    if not exists(select 1 from pg_temp.ws_verify_keys where rel=tg_relid and key=old_key) then
      raise exception 'WS_VERIFY_EXISTING_ROW_MUTATION: %.% %',tg_table_schema,tg_table_name,tg_op;
    end if;
  end if;
  if tg_op in ('INSERT','UPDATE') then
    execute 'select pg_catalog.jsonb_build_array('||expr||')' into new_key using new;
    if pg_catalog.octet_length(new_key::text)>2048 then raise exception 'WS_VERIFY_KEY_TOO_LARGE'; end if;
  end if;
  if tg_op in ('UPDATE','DELETE') then
    delete from pg_temp.ws_verify_keys where rel=tg_relid and key=old_key;
  end if;
  if tg_op in ('INSERT','UPDATE') then
    insert into pg_temp.ws_verify_keys values(tg_relid,new_key);
  end if;
  update pg_temp.ws_verify_relations set
    inserts=inserts+case when tg_op='INSERT' then 1 else 0 end,
    updates=updates+case when tg_op='UPDATE' then 1 else 0 end,
    deletes=deletes+case when tg_op='DELETE' then 1 else 0 end
  where rel=tg_relid;
  if (select inserts+updates+deletes from pg_temp.ws_verify_relations where rel=tg_relid)>100000 then
    raise exception 'WS_VERIFY_FIXTURE_WORK_LIMIT';
  end if;
  return null;
end $f$;

create or replace function pg_temp.ws_verify_watch(p_rel regclass) returns void language plpgsql
set search_path='' as $f$
declare expr text; target text;
begin
  if exists(select 1 from pg_temp.ws_verify_relations where rel=p_rel) then return; end if;
  select pg_catalog.string_agg(pg_catalog.format('($1).%I',a.attname),',' order by k.ord)
  into expr from pg_catalog.pg_index i
  cross join lateral pg_catalog.unnest(i.indkey) with ordinality k(attnum,ord)
  join pg_catalog.pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.attnum
  where i.indrelid=p_rel and i.indisprimary and k.ord<=i.indnkeyatts;
  if expr is null then raise exception 'WS_VERIFY_PRIMARY_KEY_REQUIRED: %',p_rel; end if;
  select pg_catalog.format('%I.%I',n.nspname,c.relname) into strict target
  from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where c.oid=p_rel;
  insert into pg_temp.ws_verify_relations(rel,key_expression) values(p_rel,expr);
  execute pg_catalog.format('create trigger ws_verify_capture after insert or update or delete on %s for each row execute function pg_temp.ws_verify_capture()',target);
  execute pg_catalog.format('create trigger ws_verify_no_truncate before truncate on %s for each statement execute function pg_temp.ws_verify_capture()',target);
end $f$;

create or replace function pg_temp.ws_verify_count(p_rel regclass) returns bigint language plpgsql
set search_path='' as $f$
declare n bigint;
begin
  select inserts-deletes into strict n from pg_temp.ws_verify_relations where rel=p_rel;
  return n;
end $f$;
create or replace function pg_temp.ws_verify_writes(p_rel regclass) returns bigint language plpgsql
set search_path='' as $f$
declare n bigint;
begin
  select inserts+updates+deletes into strict n from pg_temp.ws_verify_relations where rel=p_rel;
  return n;
end $f$;
