\set ON_ERROR_STOP on
begin;
-- Independent literal read-closure inventory; never copied from classifiers
-- while verifying. Broader pre-existing Source coverage remains a separate duty.
do $verify$
declare expected record; actual record; role_name text; fn oid;
begin
 for expected in select * from (values
  ('private.weekly_source_pay_decision_native_normal_gate_v1(uuid,uuid,uuid,text)',false,'s'),
  ('private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid)',false,'i'),
  ('private.weekly_source_pay_decision_read_access_v1(uuid,uuid,uuid)',false,'s'),
  ('private.weekly_source_pay_decision_read_source_v1(uuid,uuid,uuid)',false,'s'),
  ('public.weekly_source_pay_decision_accept_v1(jsonb)',true,'v'),
  ('public.weekly_source_pay_decision_read_source_v1(jsonb)',true,'s')
 ) names(signature,service_execute,volatility) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_SOURCE_READ_ROUTINE_MISSING: %',expected.signature;end if;
  select p.* into strict actual from pg_catalog.pg_proc p where p.oid=fn;
  if not actual.prosecdef or actual.proowner<>(current_user::regrole)::oid
   or actual.provolatile::text<>expected.volatility
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path=pg_catalog,private,public')
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) acl
    where acl.privilege_type='EXECUTE' and acl.grantee<>actual.proowner
     and (not expected.service_execute or acl.grantee<>('service_role'::regrole)::oid)) then
   raise exception 'HEALTH_SOURCE_READ_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_SOURCE_READ_BROWSER_EXECUTE: %',expected.signature;end if;
  end loop;
  if pg_catalog.has_function_privilege('service_role',fn,'EXECUTE') is distinct from expected.service_execute then
   raise exception 'HEALTH_SOURCE_READ_SERVICE_EXECUTE_INVALID: %',expected.signature;end if;
 end loop;
end;
$verify$;
rollback;
