-- Portable NEW and UPGRADE security/ownership checks. No business rows changed.
do $verify$
declare
  routine record;
  receipt_table oid:=to_regclass('private.weekly_source_local_protected_decision_receipts');
  definition text;
begin
  if receipt_table is null or exists(select 1 from pg_class where oid=receipt_table
      and relowner<>(select oid from pg_roles where rolname=current_user))
    or exists(select 1 from (values('anon'),('authenticated'),('service_role')) role(name)
      where has_table_privilege(role.name,receipt_table,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RECEIPT_ACL_INVALID';
  end if;
  for routine in select expected.signature,expected.service_execute,p.* from (values
    ('public.weekly_exceptional_pay_complete_local_v1(jsonb)',true),
    ('private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)',true),
    ('private.weekly_source_local_protected_pending_released_v1()',false)
  ) expected(signature,service_execute) left join pg_proc p on p.oid=to_regprocedure(expected.signature)
  loop
    if routine.oid is null or not routine.prosecdef
      or routine.proowner<>(select oid from pg_roles where rolname=current_user)
      or not exists(select 1 from unnest(routine.proconfig) config where config like 'search_path=%')
      or has_function_privilege('anon',routine.oid,'EXECUTE')
      or has_function_privilege('authenticated',routine.oid,'EXECUTE')
      or has_function_privilege('service_role',routine.oid,'EXECUTE') is distinct from routine.service_execute then
      raise exception 'WEEKLY_PROTECTED_LOCAL_ROUTINE_ACL_INVALID:%',routine.signature;
    end if;
  end loop;
  if not exists(select 1 from pg_trigger where tgrelid='public.weekly_source_pending_entitlement_bundles'::regclass
    and tgname='weekly_source_local_protected_pending_released' and tgenabled='O'
    and tgfoid='private.weekly_source_local_protected_pending_released_v1()'::regprocedure) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RELEASE_HOOK_MISSING';
  end if;
  definition:=pg_get_functiondef('public.weekly_exceptional_pay_complete_local_v1(jsonb)'::regprocedure);
  if position('private.weekly_source_entitlement_publish_immediate_v1' in definition)=0
    or position('public.tsfin_write_current_snapshot_single_bounded' in definition)=0
    or position('prior_effective_inventory' in definition)=0
    or position('public.weekly_source_manual_review_resolve_v1' in definition)=0
    or definition ~ '(weekly_source_start_c1|weekly_source_publish_c1|weekly_source_status_c1)' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORITY_BOUNDARY_INVALID';
  end if;
  perform set_config('request.jwt.claim.role','authenticated',true);
  begin
    perform public.weekly_exceptional_pay_complete_local_v1('{}'::jsonb);
    raise exception 'WEEKLY_PROTECTED_LOCAL_BROWSER_CALL_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' then raise; end if;
  end;
  perform set_config('request.jwt.claim.role','service_role',true);
  begin
    perform public.weekly_exceptional_pay_complete_local_v1('{}'::jsonb);
    raise exception 'WEEKLY_PROTECTED_LOCAL_INVALID_REQUEST_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_PROTECTED_LOCAL_COMPLETE_INVALID' then raise; end if;
  end;
end
$verify$;
