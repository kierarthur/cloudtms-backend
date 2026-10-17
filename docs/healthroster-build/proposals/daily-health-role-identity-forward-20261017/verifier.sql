\set ON_ERROR_STOP on
begin;
-- Catalogue-only, independent explicit inventory. No business/helper execution.
do $verify$
declare expected record; actual record; fn oid; role_name text;
begin
 for expected in select * from (values
  ('private.daily_rate_correction_context_v1(uuid,uuid,boolean)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_reason_note_v1(text)','text','i','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_actor_require_v1(uuid,boolean)','void','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_basis_v1(jsonb,boolean)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_review_prepare_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_accept_basis_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_result_basis_v1(uuid,uuid)','jsonb','v','pg_catalog,public,private,pg_temp')
 ) contract(signature,result_type,volatility,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'DAILY_ROLE_ROUTINE_MISSING: %',expected.signature;end if;
  select p.*,n.nspname,l.lanname into strict actual from pg_catalog.pg_proc p
   join pg_catalog.pg_namespace n on n.oid=p.pronamespace
   join pg_catalog.pg_language l on l.oid=p.prolang where p.oid=fn;
  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef
   or actual.proretset or actual.prorettype<>pg_catalog.to_regtype(expected.result_type)
   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) permission
    where permission.privilege_type='EXECUTE' and permission.grantee<>actual.proowner) then
   raise exception 'DAILY_ROLE_ROUTINE_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'DAILY_ROLE_NONOWNER_EXECUTE: %',expected.signature;end if;
  end loop;
  if expected.signature='private.daily_rate_correction_context_v1(uuid,uuid,boolean)' and
   (position('f.role is distinct from upper(t.job_title_norm)' in actual.prosrc)=0
    or position('f.role is distinct from t.job_title_norm' in actual.prosrc)>0
    or position('f.band is distinct from t.band' in actual.prosrc)=0) then
   raise exception 'DAILY_ROLE_COMPARISON_OWNER_MISSING';end if;
 end loop;
end;
$verify$;
rollback;
