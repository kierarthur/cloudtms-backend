-- Future independent mandatory verifier body; catalogue checks only.
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
  ('private.daily_rate_correction_health_result_basis_v1(uuid,uuid)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_financial_projection_assert_v1(jsonb,boolean)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_invoice_effect_v1(jsonb)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_actions_assert_v1(jsonb)','jsonb','s','pg_catalog,private,pg_temp'),
  ('private.daily_rate_correction_health_review_enroll_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_health_revision_apply_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_review_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_accept_v1(jsonb)','jsonb','v','pg_catalog,public,private,pg_temp'),
  ('private.daily_rate_correction_result_v1(uuid,uuid)','jsonb','v','pg_catalog,public,private,pg_temp')
 ) contract(signature,result_type,volatility,search_path) loop
  fn:=to_regprocedure(expected.signature);
  if fn is null then raise exception 'DAILY_COMPOSER_ROUTINE_MISSING: %',expected.signature;end if;
  select p.*,n.nspname,l.lanname into strict actual from pg_proc p
   join pg_namespace n on n.oid=p.pronamespace join pg_language l on l.oid=p.prolang where p.oid=fn;
  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef
   or actual.proretset or actual.prorettype<>to_regtype(expected.result_type)
   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from unnest(actual.proconfig) setting where replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from aclexplode(coalesce(actual.proacl,acldefault('f',actual.proowner))) a
    where a.privilege_type='EXECUTE' and a.grantee<>actual.proowner) then
   raise exception 'DAILY_COMPOSER_ROUTINE_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,fn,'EXECUTE') then raise exception 'DAILY_COMPOSER_NONOWNER_EXECUTE: %',expected.signature;end if;
  end loop;
 end loop;
end; $verify$;
