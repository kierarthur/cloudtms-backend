-- Catalogue-only body for the eventual independent mandatory full verifier.
do $verify$
declare fn oid; p record; role_name text;
begin
 fn:=to_regprocedure('private.daily_rate_correction_health_review_enroll_v1(jsonb)');
 if fn is null then raise exception 'DAILY_HEALTH_REVIEW_ENROLL_MISSING';end if;
 select x.*,n.nspname,l.lanname into strict p from pg_proc x
  join pg_namespace n on n.oid=x.pronamespace join pg_language l on l.oid=x.prolang where x.oid=fn;
 if p.nspname<>'private' or p.lanname<>'plpgsql' or p.prokind<>'f' or not p.prosecdef or p.proretset
  or p.prorettype<>'jsonb'::regtype or p.provolatile<>'v' or p.proowner<>(current_user::regrole)::oid
  or not exists(select 1 from unnest(p.proconfig) setting where replace(setting,' ','')='search_path=pg_catalog,public,private,pg_temp')
  or exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   where a.privilege_type='EXECUTE' and a.grantee<>p.proowner) then
  raise exception 'DAILY_HEALTH_REVIEW_ENROLL_SECURITY_INVALID';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_function_privilege(role_name,fn,'EXECUTE') then raise exception 'DAILY_HEALTH_REVIEW_ENROLL_NONOWNER_EXECUTE';end if;
 end loop;
end; $verify$;
