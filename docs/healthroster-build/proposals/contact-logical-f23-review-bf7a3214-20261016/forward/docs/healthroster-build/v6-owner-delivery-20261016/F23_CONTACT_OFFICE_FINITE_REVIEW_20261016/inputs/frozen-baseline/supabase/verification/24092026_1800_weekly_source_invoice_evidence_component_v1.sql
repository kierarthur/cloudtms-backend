-- Fixed-scope component verification for the independent Weekly Source TEST release.
-- It proves presence, owner/ACL isolation and executable source for only the
-- seven public entry points changed or consumed by this component.

\set ON_ERROR_STOP on

do $verify$
declare
  v_identity text;
  v_oid oid;
  v_definition text;
begin
  foreach v_identity in array array[
    'public.weekly_source_invoice_batch_candidates_v1(jsonb)',
    'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_edit_context_v1(jsonb)',
    'public.weekly_source_office_workspace_v1(jsonb)',
    'public.weekly_source_office_timesheet_presentation_v1(jsonb)',
    'public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)',
    'public.weekly_source_no_shifts_attest_atomic_v1(jsonb)'
  ] loop
    v_oid := pg_catalog.to_regprocedure(v_identity);
    if v_oid is null then
      raise exception 'WEEKLY_SOURCE_COMPONENT_ROUTINE_MISSING: %', v_identity;
    end if;
    select pg_catalog.pg_get_functiondef(v_oid) into v_definition;
    if v_definition is null or pg_catalog.length(v_definition) < 100 then
      raise exception 'WEEKLY_SOURCE_COMPONENT_DEFINITION_EMPTY: %', v_identity;
    end if;
    if pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE') then
      raise exception 'WEEKLY_SOURCE_COMPONENT_BROWSER_EXECUTE_EXPOSED: %', v_identity;
    end if;
    if not pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE') then
      raise exception 'WEEKLY_SOURCE_COMPONENT_SERVICE_EXECUTE_MISSING: %', v_identity;
    end if;
  end loop;
end
$verify$;
