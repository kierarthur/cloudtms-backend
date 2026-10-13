\set ON_ERROR_STOP on
begin;
do $verify$
declare v_policy jsonb;
begin
  if has_function_privilege('service_role','private.weekly_source_office_contact_policy_v1(uuid,jsonb)','EXECUTE')
    or has_function_privilege('anon','private.weekly_source_office_contact_policy_v1(uuid,jsonb)','EXECUTE')
    or has_function_privilege('authenticated','private.weekly_source_office_contact_policy_v1(uuid,jsonb)','EXECUTE') then
    raise exception 'CONTACT_POLICY_OWNER_ONLY_BOUNDARY_INVALID';
  end if;
  perform set_config('request.jwt.claim.role','service_role',true);
  v_policy:=private.weekly_source_office_contact_policy_v1(null,'{}'::jsonb);
  if v_policy->>'contract'<>'WEEKLY_SOURCE_CONTACT_POLICY_V1'
    or (v_policy#>>'{candidate,eligible}')::boolean
    or (v_policy#>>'{manager,eligible}')::boolean then
    raise exception 'CONTACT_POLICY_EMPTY_FACTS_MUST_FAIL_CLOSED';
  end if;
  perform set_config('request.jwt.claim.role','authenticated',true);
  begin
    perform private.weekly_source_office_contact_policy_v1(null,'{}'::jsonb);
    raise exception 'CONTACT_POLICY_BROWSER_ROLE_ACCEPTED';
  exception when insufficient_privilege then null;
  end;
end;
$verify$;
rollback;
