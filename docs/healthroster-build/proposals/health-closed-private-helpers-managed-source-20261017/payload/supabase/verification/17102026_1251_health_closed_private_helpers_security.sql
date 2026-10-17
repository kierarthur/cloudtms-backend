\set ON_ERROR_STOP on
begin;
-- Independent literal helper-only inventory. No business/helper invocation.
do $verify$
declare expected record; actual record; fn oid; role_name text;
begin
 for expected in select * from (values
  ('private.daily_rate_correction_health_financial_projection_assert_v1(jsonb,boolean)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_invoice_effect_v1(jsonb)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_actions_assert_v1(jsonb)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_review_enroll_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_revision_apply_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_current_placement_v1(uuid,uuid)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.timesheet_contract_move_public_assert_v1(jsonb,text)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.timesheet_contract_move_actions_assert_v1(jsonb)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.timesheet_contract_move_invoice_effect_v1(jsonb,uuid)','jsonb','s','pg_catalog,public,private,pg_temp')
 ) contract(signature,result_type,volatility,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_CLOSED_HELPER_MISSING: %',expected.signature;end if;
  select routine.*,namespace.nspname,language.lanname into strict actual
   from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace on namespace.oid=routine.pronamespace
   join pg_catalog.pg_language language on language.oid=routine.prolang where routine.oid=fn;
  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef
   or actual.proretset or actual.prorettype<>pg_catalog.to_regtype(expected.result_type)
   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) permission
    where permission.privilege_type='EXECUTE' and permission.grantee<>actual.proowner) then
   raise exception 'HEALTH_CLOSED_HELPER_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_CLOSED_HELPER_NONOWNER_EXECUTE: %',expected.signature;end if;
  end loop;
 end loop;
end;
$verify$;
rollback;
