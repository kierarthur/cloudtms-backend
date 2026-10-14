-- D10 compilation/read fallback/ACL verifier. No fixture, enqueue or email.
\set ON_ERROR_STOP on
begin;
do $d10$
declare v_oid oid; v_observed jsonb; v_expected jsonb;
begin
  v_expected:=public._import_review_query_evidence_core_v1(null::uuid);
  v_observed:=private.weekly_source_issued_signed_query_evidence_v1(null::uuid,null::uuid);
  if v_observed is distinct from v_expected then
    raise exception 'D10_MISSING_RECORD_LEGACY_EVIDENCE_CHANGED';
  end if;
  v_oid:=to_regprocedure('private.weekly_source_issued_signed_query_evidence_v1(uuid,uuid)');
  if v_oid is null or has_function_privilege('anon',v_oid,'EXECUTE')
    or has_function_privilege('authenticated',v_oid,'EXECUTE')
    or has_function_privilege('service_role',v_oid,'EXECUTE')
    or exists(select 1 from pg_proc p,lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
      where p.oid=v_oid and a.grantee=0 and a.privilege_type='EXECUTE')
    or not exists(select 1 from pg_proc p where p.oid=v_oid and p.prosecdef and p.proconfig is not null) then
    raise exception 'D10_OWNER_PRIVATE_ACL_FAILED';
  end if;
  v_oid:=to_regprocedure('public.timesheet_query_email_enqueue_v1(uuid,uuid,jsonb,uuid,integer,integer)');
  if v_oid is null or has_function_privilege('anon',v_oid,'EXECUTE')
    or has_function_privilege('authenticated',v_oid,'EXECUTE')
    or not has_function_privilege('service_role',v_oid,'EXECUTE') then
    raise exception 'D10_EXISTING_ENQUEUE_SERVICE_ACL_FAILED';
  end if;
end;
$d10$;
rollback;
