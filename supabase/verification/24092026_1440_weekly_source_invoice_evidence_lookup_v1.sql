\set ON_ERROR_STOP on

do $verify$
declare
  v_oid oid := pg_catalog.to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)');
  v_definition text;
begin
  if v_oid is null then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_ROUTINE_MISSING';
  end if;
  select pg_catalog.pg_get_functiondef(v_oid) into v_definition;
  if not exists(select 1 from pg_catalog.pg_proc where oid=v_oid
      and prosecdef and proowner=(select oid from pg_catalog.pg_roles where rolname=current_user))
     or not pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
     or v_definition not like '%weekly_source_query_require_service_v1%'
     or v_definition not like '%weekly_source_office_authority_v1%'
     or v_definition not like '%weekly_source_invoice_line_bindings%' then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_CONTRACT_INVALID';
  end if;
end;
$verify$;
