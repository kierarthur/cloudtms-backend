\set ON_ERROR_STOP on
begin;
set local statement_timeout='15s';
do $inventory_release$
declare v_role text; v_function text; v_relation text;
begin
  if not exists(select 1 from private.weekly_source_workbench_inventory_install_v1 where singleton and complete) then
    raise exception 'SOURCE_READER_INSTALLATION_INCOMPLETE';
  end if;
  foreach v_relation in array array['weekly_source_workbench_inventory_v1','weekly_source_workbench_inventory_install_v1'] loop
    if not exists(select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
      where n.nspname='private' and c.relname=v_relation and c.relrowsecurity and c.relforcerowsecurity
      and c.relowner=(select oid from pg_catalog.pg_roles where rolname=current_user)) then
      raise exception 'SOURCE_READER_METADATA_OWNER_OR_RLS_INVALID: %',v_relation;
    end if;
    foreach v_role in array array['anon','authenticated','service_role'] loop
      if has_table_privilege(v_role,'private.'||v_relation,'SELECT,INSERT,UPDATE,DELETE') then
        raise exception 'SOURCE_READER_METADATA_DIRECT_GRANT: % %',v_relation,v_role;
      end if;
    end loop;
  end loop;
  foreach v_function in array array[
    'private.weekly_source_workbench_inventory_track_v1()',
    'private.weekly_source_workbench_inventory_seed_page_v1(uuid)',
    'private.weekly_source_workbench_inventory_install_page_v1()'] loop
    if not exists(select 1 from pg_catalog.pg_proc where oid=v_function::regprocedure and prosecdef
      and proowner=(select oid from pg_catalog.pg_roles where rolname=current_user)
      and proconfig @> array['search_path=""']) then
      raise exception 'SOURCE_READER_HELPER_OWNER_OR_PATH_INVALID: %',v_function;
    end if;
    foreach v_role in array array['anon','authenticated','service_role'] loop
      if has_function_privilege(v_role,v_function,'EXECUTE') then
        raise exception 'SOURCE_READER_HELPER_DIRECT_GRANT: % %',v_function,v_role;
      end if;
    end loop;
  end loop;
  if not exists(select 1 from pg_catalog.pg_index where indexrelid='public.weekly_source_head_components_emittable_ordinal_idx'::regclass
    and indrelid='public.weekly_source_entitlement_head_components'::regclass and indisvalid and indisready and indpred is not null) then
    raise exception 'SOURCE_READER_EMISSION_INDEX_NOT_READY';
  end if;
  if not exists(select 1 from pg_catalog.pg_trigger where tgrelid='public.weekly_source_entitlement_head_components'::regclass
    and tgname='weekly_source_workbench_inventory_component' and tgenabled in ('O','A')
    and tgfoid='private.weekly_source_workbench_inventory_track_v1()'::regprocedure) then
    raise exception 'SOURCE_READER_COMPONENT_TRACKER_NOT_READY';
  end if;
end;
$inventory_release$;
rollback;
