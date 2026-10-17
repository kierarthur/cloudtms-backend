\set ON_ERROR_STOP on
begin;
do $verify$
declare expected record; actual record; fn oid; role_name text;
begin
 for expected in select * from (values
  ('private.weekly_source_actual_timesheet_submission_v1(uuid)','s'),
  ('private.weekly_source_signed_submission_read_context_v1(uuid,uuid)','s'),
  ('private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)','s'),
  ('private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb)','i'),
  ('private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)','s'),
  ('private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid)','s'),
  ('private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)','s')
 ) contract(signature,volatility) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_D10_READER_MISSING: %',expected.signature;end if;
  select routine.*,namespace.nspname,language.lanname into strict actual
   from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace on namespace.oid=routine.pronamespace
   join pg_catalog.pg_language language on language.oid=routine.prolang where routine.oid=fn;
  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef
   or actual.proretset or actual.prorettype<>pg_catalog.to_regtype('jsonb')
   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path=pg_catalog,public,private,pg_temp')
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) permission
    where permission.privilege_type='EXECUTE' and permission.grantee<>actual.proowner) then
   raise exception 'HEALTH_D10_READER_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_D10_READER_NONOWNER_EXECUTE: %',expected.signature;end if;
  end loop;
 end loop;
end;
$verify$;
rollback;
