-- Portable NEW/UPGRADE check, read-only except transaction-local request role.
-- This proves relation/routine isolation, not native APP/publication/J13.
do $verify$
declare
  v_table oid:=to_regclass('private.weekly_source_accepted_publication_actions_v1');
  v_routine record;
  v_signature text;
  v_result jsonb;
begin
  if v_table is null or exists(select 1 from pg_class c where c.oid=v_table
      and c.relowner<>(select oid from pg_roles where rolname=current_user))
     or exists(select 1 from (values('anon'),('authenticated'),('service_role')) r(name)
      where has_table_privilege(r.name,v_table,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_TABLE_ACL_INVALID'; end if;
  foreach v_signature in array array[
    'private.weekly_source_accepted_publication_context_v1(jsonb)',
    'private.weekly_source_accepted_publication_action_record_v1(jsonb,jsonb,jsonb)',
    'private.weekly_source_accepted_publication_action_verify_v1(uuid,uuid,bigint,bytea,uuid)'] loop
    select * into v_routine from pg_proc where oid=to_regprocedure(v_signature);
    if not found or not v_routine.prosecdef
       or v_routine.proowner<>(select oid from pg_roles where rolname=current_user)
       or not ('search_path=public, private, pg_catalog, pg_temp'=any(v_routine.proconfig))
       or has_function_privilege('anon',v_routine.oid,'EXECUTE')
       or has_function_privilege('authenticated',v_routine.oid,'EXECUTE')
       or has_function_privilege('service_role',v_routine.oid,'EXECUTE') then
      raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_ROUTINE_ACL_INVALID:%',v_signature; end if;
  end loop;
  if (select count(*) from pg_trigger t where t.tgrelid=v_table and not t.tgisinternal
       and t.tgenabled='O' and t.tgfoid=
         'private.weekly_source_accepted_publication_action_immutable_v1()'::regprocedure)<>2
     or exists(select 1 from (values('anon'),('authenticated'),('service_role')) r(name)
       where has_function_privilege(r.name,
         'private.weekly_source_accepted_publication_action_immutable_v1()','EXECUTE')) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_IMMUTABILITY_INVALID'; end if;
  if not exists(select 1 from pg_constraint c where c.conrelid=v_table and c.contype='f'
      and c.confrelid='public.weekly_source_entitlement_decision_bundles'::regclass
      and array_length(c.conkey,1)=2 and c.confdeltype='r') then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_BUNDLE_REVISION_FK_INVALID'; end if;
  perform set_config('request.jwt.claim.role','authenticated',true);
  begin
    perform private.weekly_source_accepted_publication_action_record_v1('{}','{}','{}');
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_UNTRUSTED_ROLE_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm<>'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' then raise; end if;
  end;
  perform set_config('request.jwt.claim.role','service_role',true);
  begin
    perform private.weekly_source_accepted_publication_action_verify_v1(null,null,null,null,null);
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_NULL_CERTIFICATE_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' then raise; end if;
  end;
end; $verify$;
