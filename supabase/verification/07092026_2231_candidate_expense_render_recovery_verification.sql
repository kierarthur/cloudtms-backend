do $verification$
declare
  v_identity regprocedure:=to_regprocedure(
    'public.candidate_expense_update_render_recovery_list_v1(text,integer,uuid,uuid,timestamp with time zone)'
  );
begin
  if v_identity is null then
    raise exception 'candidate expense render recovery function is missing';
  end if;
  if has_function_privilege('anon',v_identity,'EXECUTE')
     or has_function_privilege('authenticated',v_identity,'EXECUTE')
     or not has_function_privilege('service_role',v_identity,'EXECUTE') then
    raise exception 'candidate expense render recovery grants are invalid';
  end if;
  if not exists (
    select 1
    from pg_proc procedure
    join pg_namespace namespace on namespace.oid=procedure.pronamespace
    where procedure.oid=v_identity
      and namespace.nspname='public'
      and procedure.prosecdef
      and procedure.provolatile='s'
      and procedure.proconfig @> array['search_path=pg_catalog, public, private, pg_temp']
  ) then
    raise exception 'candidate expense render recovery security boundary is invalid';
  end if;
end;
$verification$;
