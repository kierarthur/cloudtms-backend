-- WP-F3 legacy default/upload precedence and owner-private ACL. No fixtures.
\set ON_ERROR_STOP on
begin;
do $f3$
declare v_oid oid;
begin
  if private.weekly_source_configured_upload_profile_v1(null,null,'NHSP',null) is distinct from 'NHSP_FINAL_BACKING_V1'
    or private.weekly_source_configured_upload_profile_v1(null,null,'ROSTER',null) is distinct from 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'
    or private.weekly_source_configured_upload_profile_v1(null,null,'ROSTER','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1') is distinct from 'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
    or private.weekly_source_configured_upload_profile_v1(null,null,'ROSTER','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') is distinct from 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
    or private.weekly_source_configured_upload_profile_v1(null,null,'NHSP','NHSP_PREFINAL_RELEASED_V1') is distinct from 'NHSP_PREFINAL_RELEASED_V1' then
    raise exception 'WP_F3_EXISTING_PROFILE_PRECEDENCE_CHANGED';
  end if;
  v_oid:=to_regprocedure('private.weekly_source_configured_upload_profile_v1(uuid,uuid,text,text)');
  if v_oid is null or has_function_privilege('anon',v_oid,'EXECUTE')
    or has_function_privilege('authenticated',v_oid,'EXECUTE')
    or has_function_privilege('service_role',v_oid,'EXECUTE')
    or exists(select 1 from pg_proc p,lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
      where p.oid=v_oid and a.grantee=0 and a.privilege_type='EXECUTE')
    or not exists(select 1 from pg_proc p where p.oid=v_oid and p.prosecdef and p.proconfig is not null) then
    raise exception 'WP_F3_OWNER_PRIVATE_ACL_FAILED';
  end if;
end;
$f3$;
rollback;
