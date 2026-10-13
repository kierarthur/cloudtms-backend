-- Data-free declaration/security and missing-authority refusal only.
-- Does not certify native Report financial readiness, A/B or feature enablement.
\set ON_ERROR_STOP on
begin;
do $report_native_security$
declare
  signatures text[]:=array[
    'private.weekly_source_report_projection_receipt_assert_v1(public.weekly_source_ordinary_pay_projection_receipts)',
    'private.weekly_source_self_bill_report_pay_authority_v1(uuid)',
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)'
  ];
  signature text;
begin
  foreach signature in array signatures loop
    if to_regprocedure(signature) is null
      or has_function_privilege('anon',signature,'EXECUTE')
      or has_function_privilege('authenticated',signature,'EXECUTE')
      or has_function_privilege('service_role',signature,'EXECUTE')
      or exists(select 1 from pg_catalog.pg_proc p
        cross join lateral pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
        where p.oid=to_regprocedure(signature) and a.grantee=0 and a.privilege_type='EXECUTE')
      or not exists(select 1 from pg_catalog.pg_proc p where p.oid=to_regprocedure(signature)
        and p.proowner=current_user::regrole::oid and p.pronargdefaults=0
        and p.proconfig @> array[case when signature=signatures[3]
          then 'search_path=public, private, pg_catalog, pg_temp'
          else 'search_path=pg_catalog, pg_temp' end]) then
      raise exception 'REPORT_NATIVE_OWNER_ONLY_DECLARATION_FAILED';
    end if;
  end loop;
  if not exists(select 1 from pg_catalog.pg_proc p
    where p.oid=to_regprocedure(signatures[1]) and not p.prosecdef and p.provolatile='s')
    or not exists(select 1 from pg_catalog.pg_proc p
    where p.oid=to_regprocedure(signatures[2]) and p.prosecdef and p.provolatile='s')
    or not exists(select 1 from pg_catalog.pg_proc p
    where p.oid=to_regprocedure(signatures[3]) and p.prosecdef and p.provolatile='v') then
    raise exception 'REPORT_NATIVE_ROUTINE_SECURITY_KIND_FAILED';
  end if;
  if private.weekly_source_self_bill_report_pay_authority_v1(null) is not null
    or private.weekly_source_self_bill_report_pay_witness_v1(null) is not null then
    raise exception 'REPORT_NATIVE_MISSING_BINDING_NOT_REFUSED';
  end if;
end;
$report_native_security$;
rollback;

