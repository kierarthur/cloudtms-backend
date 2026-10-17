-- Catalogue-only admission inside actual B. Never install or replace the existing key helper.
do $source_pure_preflight$
declare p pg_catalog.pg_proc%rowtype; n integer;
begin
 if current_database()<>'cloudtms_test_clone' or current_user<>session_user or current_user<>'dee50tht'
  or current_setting('transaction_read_only')<>'off'
  or current_setting('server_version_num')::integer not between 170000 and 179999 then
  raise exception 'SOURCE_PURE_NATIVE_CONTEXT_REFUSED' using errcode='55000';end if;
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_source_pay_decision_native_status_child_v1';
 if n<>0 then raise exception 'SOURCE_PURE_NEW_NAME_ALREADY_PRESENT' using errcode='55000';end if;
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_exceptional_json_keys_exact_v1';
 if n<>1 then raise exception 'SOURCE_PURE_DEPENDENCY_OVERLOAD_REFUSED' using errcode='55000';end if;
 select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure('private.weekly_exceptional_json_keys_exact_v1(jsonb,text[])');
 if not found then raise exception 'SOURCE_PURE_DEPENDENCY_MISSING' using errcode='55000';end if;
 if p.prokind<>'f' or p.pronargs<>2 or p.prorettype<>'boolean'::regtype or p.proretset
  or p.proargtypes<>'3802 1009'::oidvector or p.proargnames is distinct from array['p_value','p_allowed_keys']::text[]
  or (select lanname from pg_catalog.pg_language where oid=p.prolang)<>'sql'
  or p.provolatile<>'i' or p.prosecdef or pg_catalog.pg_get_userbyid(p.proowner)<>'dee50tht'
  or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
  or octet_length(pg_catalog.pg_get_functiondef(p.oid))<>429
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')<>'bb36d7e2d71abd7b9c0dfba29936d85edca54c8101591fd113e11d3ab7cf9189'
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>'c1e23ad8f8102e5b7b996bb448838441cb8920e23b64c69d46007d515b618063'
  or p.proacl is null or cardinality(p.proacl)<>1
  or (select count(*) from pg_catalog.aclexplode(p.proacl) a
   where a.grantee=p.proowner and a.grantor=p.proowner and a.privilege_type='EXECUTE' and not a.is_grantable)<>1
  or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
  or pg_catalog.has_schema_privilege('anon',p.pronamespace,'USAGE')
  or pg_catalog.has_schema_privilege('authenticated',p.pronamespace,'USAGE')
  or not pg_catalog.has_schema_privilege('service_role',p.pronamespace,'USAGE') then
  raise exception 'SOURCE_PURE_GENUINE_DEPENDENCY_CHANGED' using errcode='55000';end if;
end;$source_pure_preflight$;
