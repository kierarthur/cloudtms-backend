-- Read-only installed-definition/ACL check. This does not replace native journeys.
\set ON_ERROR_STOP on
begin read only;
do $check$
declare r record; role_name text; actual integer;
begin
  for r in select p.oid,p.proname,p.prosecdef,p.proconfig,n.nspname
    from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in (
      'weekly_source_observation_storage_v1','weekly_source_observation_begin_v1',
      'weekly_source_observation_seal_v1','weekly_source_observation_end_v1',
      'weekly_source_observe_effect_v1','weekly_source_inventory_changed_v1')
  loop
    if not r.prosecdef or not ('search_path=pg_catalog, pg_temp'=any(r.proconfig)) then
      raise exception 'SB02_HELPER_SECURITY_INVALID: %',r.proname;
    end if;
    foreach role_name in array array['anon','authenticated','service_role'] loop
      if pg_catalog.has_function_privilege(role_name,r.oid,'EXECUTE') then
        raise exception 'SB02_HELPER_CALLABLE: %, %',role_name,r.proname;
      end if;
    end loop;
    if exists(select 1 from pg_catalog.pg_proc p,
      lateral pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
      where p.oid=r.oid and a.grantee=0 and a.privilege_type='EXECUTE') then
      raise exception 'SB02_HELPER_PUBLIC_EXECUTE: %',r.proname;
    end if;
  end loop;
  select count(*) into actual from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in (
      'weekly_source_observation_storage_v1','weekly_source_observation_begin_v1',
      'weekly_source_observation_seal_v1','weekly_source_observation_end_v1',
      'weekly_source_observe_effect_v1','weekly_source_inventory_changed_v1');
  if actual<>6 then raise exception 'SB02_HELPER_COUNT: %',actual; end if;
  select count(*) into actual from pg_catalog.pg_trigger t
   where (t.tgname='ws_operation_effect_capture_v1' and t.tgrelid in (
     'public.banking_pay_workbench_jobs'::regclass,'public.banking_pay_scope_change_transactions'::regclass,
     'private.banking_pay_workbench_timesheet_scope_state'::regclass,
     'private.banking_pay_workbench_candidate_scope_registry'::regclass))
     or (t.tgname='ws_inventory_changed_v1' and t.tgrelid in (
       'public.weekly_source_entitlement_heads'::regclass,'public.weekly_source_entitlement_head_components'::regclass));
  if actual<>6 then raise exception 'SB02_TRIGGER_COUNT: %',actual; end if;
  -- A name count cannot prove execution order or an enabled capture boundary.
  for r in select * from (values
    ('public.banking_pay_workbench_jobs','ws_operation_effect_capture_v1',29,'private.weekly_source_observe_effect_v1()'),
    ('public.banking_pay_scope_change_transactions','ws_operation_effect_capture_v1',29,'private.weekly_source_observe_effect_v1()'),
    ('private.banking_pay_workbench_timesheet_scope_state','ws_operation_effect_capture_v1',29,'private.weekly_source_observe_effect_v1()'),
    ('private.banking_pay_workbench_candidate_scope_registry','ws_operation_effect_capture_v1',29,'private.weekly_source_observe_effect_v1()'),
    ('public.weekly_source_entitlement_heads','ws_inventory_changed_v1',31,'private.weekly_source_inventory_changed_v1()'),
    ('public.weekly_source_entitlement_head_components','ws_inventory_changed_v1',31,'private.weekly_source_inventory_changed_v1()')
  ) expected(relation_name,trigger_name,event_bits,function_identity) loop
    if not exists(select 1 from pg_catalog.pg_trigger t
      where t.tgrelid=pg_catalog.to_regclass(r.relation_name) and t.tgname=r.trigger_name
        and t.tgfoid=pg_catalog.to_regprocedure(r.function_identity)
        and t.tgtype=r.event_bits and t.tgenabled='O'
        and not t.tgisinternal and not t.tgdeferrable and not t.tginitdeferred
        and t.tgnargs=0 and t.tgqual is null) then
      raise exception 'SB02_TRIGGER_BINDING_INVALID: %.%',r.relation_name,r.trigger_name;
    end if;
  end loop;
end;
$check$;
rollback;
