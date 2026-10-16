with expected(signature)as(values ('private.weekly_source_query_require_service_v1()'),('public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'),('public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'),('private.weekly_source_protected_qualification_owner_v1()')),functions as(
 select e.signature,case when p.oid is null then null else md5(to_jsonb(p)::text)end fingerprint
 from expected e left join pg_proc p on p.oid=to_regprocedure(e.signature))
 select jsonb_build_object('database',current_database(),'postmaster_epoch',extract(epoch from pg_postmaster_start_time())::text,'server_version_num',current_setting('server_version_num'),
 'functions',(select jsonb_object_agg(signature,fingerprint)from functions),
 'schemas',(select jsonb_object_agg(nspname,md5(jsonb_build_array(nspowner,nspacl)::text))from pg_namespace where nspname in('public','private')),
 'default_acl',(select md5(coalesce(jsonb_agg(jsonb_build_array(defaclrole,defaclnamespace,defaclobjtype,defaclacl)order by defaclrole,defaclnamespace,defaclobjtype),'[]'::jsonb)::text)from pg_default_acl),
 'migration_ledger',(select jsonb_build_object('count',count(*),'md5',md5(coalesce(jsonb_agg(to_jsonb(l)order by to_jsonb(l)::text),'[]'::jsonb)::text))from private.cloudtms_migration_ledger l),
 'repeatable_ledger',(select jsonb_build_object('count',count(*),'md5',md5(coalesce(jsonb_agg(to_jsonb(l)order by to_jsonb(l)::text),'[]'::jsonb)::text))from private.cloudtms_repeatable_ledger l),
 'roles',(select jsonb_object_agg(rolname,jsonb_build_array(rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls))from pg_roles where rolname in('anon','authenticated','service_role')),
 'identity_relation_present',to_regclass('private.cloudtms_database_identity')is not null,
 'marker_absent',to_regprocedure('private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc()')is null)as contact_component_snapshot;

do $contact_preflight$
 declare actual_owner name; p pg_proc%rowtype; begin
 if to_regclass('private.cloudtms_database_identity')is null then raise exception 'CONTACT_COMPONENT_IDENTITY_RELATION_MISSING' using errcode='55000';end if;
 if current_database()<>'cloudtms_test_clone' or (select count(*)from private.cloudtms_database_identity where singleton and environment='TEST')<>1 then raise exception 'CONTACT_COMPONENT_TEST_IDENTITY_REQUIRED' using errcode='42501';end if;
 select * into p from pg_proc where oid=to_regprocedure('private.weekly_source_protected_qualification_owner_v1()');
 if not found then raise exception 'CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_MISSING' using errcode='55000';end if;
 if not(coalesce((p.oid is not null and md5(p.prosrc)='7dcc1e303ab436298d7a3fa1ebc61807' and p.prosecdef=false and p.provolatile='s' and (select lanname from pg_language where oid=p.prolang)='plpgsql' and cardinality(p.proconfig)=1 and replace(replace(p.proconfig[1],' ',''),'"','')='search_path=pg_catalog,pg_temp'),false))then raise exception 'CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_CHANGED' using errcode='55000';end if;
 actual_owner:=pg_get_userbyid(p.proowner);
 if session_user is distinct from actual_owner or current_user is distinct from actual_owner then raise exception 'CONTACT_COMPONENT_ACTUAL_LOGIN_OWNER_REQUIRED' using errcode='42501';end if;
 select * into p from pg_proc where oid=to_regprocedure('private.weekly_source_query_require_service_v1()');
 if not found then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_MISSING' using errcode='55000';end if;
 if not(coalesce((p.oid is not null and md5(p.prosrc)='237c30e8fdce7bf84d2002d0e669cdd3' and p.prosecdef=true and p.provolatile='s' and (select lanname from pg_language where oid=p.prolang)='plpgsql' and cardinality(p.proconfig)=1 and replace(replace(p.proconfig[1],' ',''),'"','')='search_path=pg_catalog,pg_temp'),false))then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_CHANGED' using errcode='55000';end if;
 if pg_get_userbyid(p.proowner)is distinct from actual_owner then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_OWNER_REQUIRED' using errcode='42501';end if;
 if exists(select 1 from pg_proc q where q.oid in(to_regprocedure('public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'),to_regprocedure('public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'))and pg_get_userbyid(q.proowner)is distinct from actual_owner)then raise exception 'CONTACT_COMPONENT_EXISTING_COMMAND_OWNER_REQUIRED' using errcode='42501';end if;
 if (select count(*)from pg_roles where rolname in('anon','authenticated','service_role'))<>3 then raise exception 'CONTACT_COMPONENT_REAL_ROLES_REQUIRED' using errcode='55000';end if;
 if not has_schema_privilege(current_user,'private','USAGE')or not has_schema_privilege(current_user,'private','CREATE')or not has_schema_privilege(current_user,'public','USAGE')or not has_schema_privilege(current_user,'public','CREATE')or not has_schema_privilege('service_role','public','USAGE')then raise exception 'CONTACT_COMPONENT_SCHEMA_PRIVILEGES_REQUIRED' using errcode='42501';end if;

 if to_regprocedure('private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc()')is not null then raise exception 'CONTACT_COMPONENT_MARKER_ALREADY_PRESENT' using errcode='55000';end if;
 end $contact_preflight$;

do $contact_owner_request$ begin
 -- Existing reviewed owner request dialect, transaction-local component test only.
 -- The preceding guard proved actual session_user/current_user equals the unchanged
 -- installed protected owner. No role, actor, writer or capability is manufactured.
 perform set_config('request.jwt.claim.role','service_role',true);
 perform private.weekly_source_protected_qualification_owner_v1();
 end $contact_owner_request$;

create or replace function private.weekly_source_query_require_service_v1()
returns void
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     ) <> 'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
end;
$function$;

alter function private.weekly_source_query_require_service_v1() owner to current_user;

revoke all on function private.weekly_source_query_require_service_v1() from public,anon,authenticated,service_role;

create or replace function public.weekly_source_message_dispatch_submission_start_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  perform private.weekly_source_query_require_service_v1();
  raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';
end;
$function$;

alter function public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb) OWNER TO CURRENT_USER;

revoke all on function public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb) from public,anon,authenticated,service_role;

grant execute on function public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb) to service_role;

create or replace function public.weekly_source_message_dispatch_result_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  perform private.weekly_source_query_require_service_v1();
  raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';
end;
$function$;

alter function public.weekly_source_message_dispatch_result_atomic_v1(jsonb) owner to current_user;

revoke all on function public.weekly_source_message_dispatch_result_atomic_v1(jsonb) from public,anon,authenticated,service_role;

grant execute on function public.weekly_source_message_dispatch_result_atomic_v1(jsonb) to service_role;

do $contact_catalogue$ declare p pg_proc%rowtype; begin
 select * into strict p from pg_proc where oid='private.weekly_source_query_require_service_v1()'::regprocedure;
 if not(coalesce((p.oid is not null and md5(p.prosrc)='237c30e8fdce7bf84d2002d0e669cdd3' and p.prosecdef=true and p.provolatile='s' and (select lanname from pg_language where oid=p.prolang)='plpgsql' and cardinality(p.proconfig)=1 and replace(replace(p.proconfig[1],' ',''),'"','')='search_path=pg_catalog,pg_temp'),false))or pg_get_userbyid(p.proowner)is distinct from current_user then raise exception 'CONTACT_COMPONENT_EXACT_NATIVE_DECLARATION_REQUIRED' using errcode='55000';end if;
 if has_function_privilege('anon',p.oid,'EXECUTE')or has_function_privilege('authenticated',p.oid,'EXECUTE')or has_function_privilege('service_role',p.oid,'EXECUTE')is distinct from false then raise exception 'CONTACT_COMPONENT_NATIVE_ACL_REQUIRED' using errcode='42501';end if;
select * into strict p from pg_proc where oid='public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'::regprocedure;
 if not(coalesce((p.oid is not null and md5(p.prosrc)='4c0326a4000e71a3fab3cdabd0e691c9' and p.prosecdef=true and p.provolatile='v' and (select lanname from pg_language where oid=p.prolang)='plpgsql' and cardinality(p.proconfig)=1 and replace(replace(p.proconfig[1],' ',''),'"','')='search_path=public,private,pg_catalog,pg_temp'),false))or pg_get_userbyid(p.proowner)is distinct from current_user then raise exception 'CONTACT_COMPONENT_EXACT_NATIVE_DECLARATION_REQUIRED' using errcode='55000';end if;
 if has_function_privilege('anon',p.oid,'EXECUTE')or has_function_privilege('authenticated',p.oid,'EXECUTE')or has_function_privilege('service_role',p.oid,'EXECUTE')is distinct from true then raise exception 'CONTACT_COMPONENT_NATIVE_ACL_REQUIRED' using errcode='42501';end if;
select * into strict p from pg_proc where oid='public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'::regprocedure;
 if not(coalesce((p.oid is not null and md5(p.prosrc)='4c0326a4000e71a3fab3cdabd0e691c9' and p.prosecdef=true and p.provolatile='v' and (select lanname from pg_language where oid=p.prolang)='plpgsql' and cardinality(p.proconfig)=1 and replace(replace(p.proconfig[1],' ',''),'"','')='search_path=public,private,pg_catalog,pg_temp'),false))or pg_get_userbyid(p.proowner)is distinct from current_user then raise exception 'CONTACT_COMPONENT_EXACT_NATIVE_DECLARATION_REQUIRED' using errcode='55000';end if;
 if has_function_privilege('anon',p.oid,'EXECUTE')or has_function_privilege('authenticated',p.oid,'EXECUTE')or has_function_privilege('service_role',p.oid,'EXECUTE')is distinct from true then raise exception 'CONTACT_COMPONENT_NATIVE_ACL_REQUIRED' using errcode='42501';end if;
 end $contact_catalogue$;

create function private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc()returns jsonb language plpgsql security invoker set search_path='pg_catalog','pg_temp'
 as $contact_native_assertions$ declare message text;closed_count integer:=0; begin
 perform private.weekly_source_protected_qualification_owner_v1();
 perform private.weekly_source_query_require_service_v1();
 begin perform public.weekly_source_message_dispatch_submission_start_atomic_v1('{}'::jsonb);
 raise exception 'CONTACT_COMPONENT_COMMAND_START_REOPENED';
 exception when object_not_in_prerequisite_state then get stacked diagnostics message=message_text;
 if message<>'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'then raise;end if;closed_count:=closed_count+1;end;
 begin perform public.weekly_source_message_dispatch_result_atomic_v1('{}'::jsonb);
 raise exception 'CONTACT_COMPONENT_COMMAND_RESULT_REOPENED';
 exception when object_not_in_prerequisite_state then get stacked diagnostics message=message_text;
 if message<>'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'then raise;end if;closed_count:=closed_count+1;end;
 if closed_count<>2 then raise exception 'CONTACT_COMPONENT_NATIVE_ASSERTION_COUNT_REQUIRED';end if;
 return jsonb_build_object('closed_count',closed_count,'service_helper_first_use',true,'protected_owner_first_use',true,'native_acl_checked',true,'marker','hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc','mode','protected-owner-request','scope','CLOSED_COMMAND_COMPONENT_ONLY','api_role_proof','NOT_RUN','provider_called',false);
 end $contact_native_assertions$;

revoke all on function private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc()from public,anon,authenticated,service_role;

select jsonb_build_object('status','CLOSED_COMMAND_COMPONENT_ASSERTIONS_REACHED','database',current_database(),'marker','hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc','mode','protected-owner-request','proof',private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc())as contact_component_result;
