-- Closed owner-only admission before the first call of the newly created pure child.
do $source_pure_postflight$
declare p pg_catalog.pg_proc%rowtype; n integer;
begin
 select count(*) into n from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace
  where s.nspname='private' and p.proname='weekly_source_pay_decision_native_status_child_v1';
 if n<>1 then raise exception 'SOURCE_PURE_NEW_NAME_COUNT_REFUSED' using errcode='55000';end if;
 select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure('private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid)');
 if not found then raise exception 'SOURCE_PURE_NEW_SIGNATURE_MISSING' using errcode='55000';end if;
 if p.prokind<>'f' or p.pronargs<>5 or p.prorettype<>'jsonb'::regtype or p.proretset
  or p.proargtypes<>'3802 2950 2950 2950 2950'::oidvector
  or p.proargnames is distinct from array['p_source','p_family','p_run','p_decision','p_request']::text[]
  or (select lanname from pg_catalog.pg_language where oid=p.prolang)<>'plpgsql'
  or p.provolatile<>'i' or not p.prosecdef or pg_catalog.pg_get_userbyid(p.proowner)<>'dee50tht'
  or p.proconfig is distinct from array['search_path=pg_catalog, private, public']::text[]
  or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>'6798fc172f368d87abcfb950a60623b682df3cbce6667d54813da32ac77bb70e'
  or p.proacl is null or cardinality(p.proacl)<>1
  or (select count(*) from pg_catalog.aclexplode(p.proacl) a
   where a.grantee=p.proowner and a.grantor=p.proowner and a.privilege_type='EXECUTE' and not a.is_grantable)<>1
  or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
  or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') then
  raise exception 'SOURCE_PURE_NEW_DEFINITION_OR_ACL_REFUSED' using errcode='55000';end if;
end;$source_pure_postflight$;
