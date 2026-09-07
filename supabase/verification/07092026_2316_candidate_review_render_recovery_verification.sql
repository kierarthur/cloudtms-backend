do $verify$
declare
  v_oid oid;
  v_owner text;
  v_security_definer boolean;
  v_volatility "char";
  v_search_path text;
begin
  select routine.oid,owner_role.rolname,routine.prosecdef,routine.provolatile,
         pg_catalog.array_to_string(routine.proconfig,',')
  into v_oid,v_owner,v_security_definer,v_volatility,v_search_path
  from pg_catalog.pg_proc routine
  join pg_catalog.pg_namespace namespace on namespace.oid=routine.pronamespace
  join pg_catalog.pg_roles owner_role on owner_role.oid=routine.proowner
  where namespace.nspname='public'
    and routine.proname='candidate_review_render_recovery_list_v1'
    and pg_catalog.pg_get_function_identity_arguments(routine.oid)=
      'p_environment text, p_limit integer, p_workflow_id uuid, p_workflow_generation integer, p_now_utc timestamp with time zone';

  if v_oid is null then
    raise exception 'candidate review render recovery function is missing';
  end if;
  if v_owner<>current_user or not v_security_definer or v_volatility<>'s'
     or v_search_path<>'search_path=pg_catalog, public, private, pg_temp' then
    raise exception 'candidate review render recovery authority is invalid';
  end if;
  if not pg_catalog.has_function_privilege(
       'service_role',v_oid,'EXECUTE'
     ) or pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
       or pg_catalog.has_function_privilege('public',v_oid,'EXECUTE') then
    raise exception 'candidate review render recovery grants are invalid';
  end if;
end;
$verify$;
