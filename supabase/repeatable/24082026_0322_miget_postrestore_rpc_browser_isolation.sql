-- Reassert the provider-neutral browser boundary after the late invoice
-- repeatables. Those source authorities intentionally retain service-role
-- execution but must not expose SECURITY DEFINER entry points to PostgREST
-- browser roles after a pg_dump/pg_restore owner transition.

\set ON_ERROR_STOP on

begin;

do $browser_isolation$
declare
  v_signature regprocedure;
begin
  for v_signature in
    select pg_catalog.to_regprocedure(signature)
    from (values
      ('public.invoice_batch_generate_candidates(jsonb)'),
      ('public.invoice_batch_issue_candidates(jsonb)'),
      ('public.invoice_operation_get(uuid[],uuid,text,jsonb)'),
      ('public.invoice_operation_start_batch(jsonb,uuid,timestamp with time zone)'),
      ('public.invoice_unissue_one(uuid,uuid,boolean)'),
      ('public.manual_timesheet_queue_attach_process_atomic(uuid,uuid,uuid,text,text,uuid,jsonb,timestamp with time zone)'),
      ('public.rpc_changes_ping(jsonb)')
    ) target(signature)
  loop
    if v_signature is null then
      raise exception using errcode='ZX999',message='POSTRESTORE_BROWSER_ISOLATION_SIGNATURE_MISSING';
    end if;
    if not pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE') then
      raise exception using errcode='ZX999',message='POSTRESTORE_BROWSER_ISOLATION_SERVICE_ROLE_MISSING';
    end if;
    execute pg_catalog.format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated',
      v_signature
    );
  end loop;

  if exists(
    select 1
    from (values
      ('public.invoice_batch_generate_candidates(jsonb)'),
      ('public.invoice_batch_issue_candidates(jsonb)'),
      ('public.invoice_operation_get(uuid[],uuid,text,jsonb)'),
      ('public.invoice_operation_start_batch(jsonb,uuid,timestamp with time zone)'),
      ('public.invoice_unissue_one(uuid,uuid,boolean)'),
      ('public.manual_timesheet_queue_attach_process_atomic(uuid,uuid,uuid,text,text,uuid,jsonb,timestamp with time zone)'),
      ('public.rpc_changes_ping(jsonb)')
    ) target(signature)
    cross join lateral (select pg_catalog.to_regprocedure(target.signature) as oid) resolved
    where pg_catalog.has_function_privilege('anon',resolved.oid,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',resolved.oid,'EXECUTE')
       or not pg_catalog.has_function_privilege('service_role',resolved.oid,'EXECUTE')
  ) then
    raise exception using errcode='ZX999',message='POSTRESTORE_BROWSER_ISOLATION_VERIFICATION_FAILED';
  end if;
end
$browser_isolation$;

commit;
