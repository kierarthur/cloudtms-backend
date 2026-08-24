-- Restore the exact service-role EXECUTE grant required by the scheduled email
-- outbox claim route after a provider-owner ACL baseline is applied.
--
-- This changes no application rows and sends no communication. The guarded
-- no-op keeps NEW databases and historical LIVE schemas safe when the helper is
-- not installed yet; its owning repeatable installs the same grant later.

\set ON_ERROR_STOP on

begin;

do $grant$
begin
  if pg_catalog.to_regprocedure(
    'private._candidate_manager_email_claim_route_current_v1(uuid,uuid,bigint,text,uuid,uuid,integer)'
  ) is not null then
    grant execute on function private._candidate_manager_email_claim_route_current_v1(
      uuid,uuid,bigint,text,uuid,uuid,integer
    ) to service_role;
  end if;
end
$grant$;

do $verification$
declare
  v_signature regprocedure := pg_catalog.to_regprocedure(
    'private._candidate_manager_email_claim_route_current_v1(uuid,uuid,bigint,text,uuid,uuid,integer)'
  );
begin
  if v_signature is not null
     and not pg_catalog.has_function_privilege(
       'service_role',v_signature,'EXECUTE'
     ) then
    raise exception using
      errcode='ZX999',
      message='MIGET_PRIVATE_HELPER_SERVICE_EXECUTE_MISSING';
  end if;
end
$verification$;

commit;
