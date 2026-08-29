\set ON_ERROR_STOP on

begin;

do $verify$
declare
  v_policy jsonb;
  v_allowed jsonb;
begin
  v_policy:=private._candidate_manager_authoriser_effective_v2(
    '{"approved_emails":["client@trust.test"],"approved_domains":["@client.test"],"allow_free_business_email":true}'::jsonb,
    '{"mode":"EXTEND","approved_emails":["contract@trust.test","CLIENT@TRUST.TEST"],"approved_domains":["contract.test","@client.test"]}'::jsonb
  );
  if v_policy->'approved_emails'<>'["client@trust.test","contract@trust.test"]'::jsonb
     or v_policy->'approved_domains'<>'["client.test","contract.test"]'::jsonb
     or (v_policy->>'allow_free_business_email')::boolean is not true then
    raise exception 'manager authoriser additive policy verification failed';
  end if;

  v_policy:=private._candidate_manager_authoriser_effective_v2(
    '{"approved_emails":["client@trust.test"],"approved_domains":["client.test"],"allow_free_business_email":true}'::jsonb,
    '{"mode":"CONTRACT_ONLY","approved_emails":[],"approved_domains":["contract.test"]}'::jsonb
  );
  if v_policy->'approved_emails'<>'[]'::jsonb
     or v_policy->'approved_domains'<>'["contract.test"]'::jsonb
     or (v_policy->>'allow_free_business_email')::boolean is not false then
    raise exception 'manager authoriser contract-only policy verification failed';
  end if;

  v_allowed:=private._candidate_manager_email_allowed_v1(
    '{"approved_emails":[],"approved_domains":["trust.test"],"allow_free_business_email":false}'::jsonb,
    'manager@trust.test','["gmail.com"]'::jsonb
  );
  if (v_allowed->>'allowed')::boolean is not true then
    raise exception 'manager authoriser exact-domain allow verification failed';
  end if;
  v_allowed:=private._candidate_manager_email_allowed_v1(
    '{"approved_emails":[],"approved_domains":["trust.test"],"allow_free_business_email":false}'::jsonb,
    'manager@sub.trust.test','["gmail.com"]'::jsonb
  );
  if (v_allowed->>'allowed')::boolean is not false then
    raise exception 'manager authoriser subdomain boundary verification failed';
  end if;

  if exists (
    select 1
    from information_schema.routine_privileges rp
    where rp.routine_schema='public'
      and rp.routine_name in (
        'client_manager_authoriser_policy_get_v1','contract_manager_authoriser_policy_get_v1',
        'client_manager_authoriser_policy_update_v1','contract_manager_authoriser_policy_update_v1'
      )
      and rp.grantee in ('PUBLIC','anon','authenticated')
      and rp.privilege_type='EXECUTE'
  ) then
    raise exception 'manager authoriser Office RPC browser grant verification failed';
  end if;

  if not exists (
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relname='candidate_manager_authoriser_policy_receipts'
      and c.relkind='r'
      and c.relrowsecurity
      and c.relforcerowsecurity
  ) or not exists (
    select 1
    from pg_catalog.pg_policies p
    where p.schemaname='public'
      and p.tablename='candidate_manager_authoriser_policy_receipts'
      and p.policyname='cloudtms_miget_service_owner_all'
  ) then
    raise exception 'manager authoriser receipt RLS verification failed';
  end if;

  if pg_catalog.has_table_privilege('anon','public.candidate_manager_authoriser_policy_receipts','SELECT,INSERT,UPDATE,DELETE')
     or pg_catalog.has_table_privilege('authenticated','public.candidate_manager_authoriser_policy_receipts','SELECT,INSERT,UPDATE,DELETE')
     or pg_catalog.has_table_privilege('service_role','public.candidate_manager_authoriser_policy_receipts','SELECT,INSERT,UPDATE,DELETE') then
    raise exception 'manager authoriser receipt direct privilege verification failed';
  end if;
end;
$verify$;

do $first_use$
declare
  v_client_id uuid:=extensions.gen_random_uuid();
  v_contract_id uuid:=extensions.gen_random_uuid();
  v_actor_id uuid:=extensions.gen_random_uuid();
  v_client_updated_at timestamptz;
  v_contract_updated_at timestamptz;
  v_before jsonb;
  v_after jsonb;
  v_result jsonb;
  v_error text;
begin
  insert into public.clients(id,name)
  values(v_client_id,'Manager authoriser verification client');
  insert into public.client_settings(client_id)
  values(v_client_id)
  returning updated_at into v_client_updated_at;
  insert into public.contracts(id,client_id,start_date,end_date,pay_method_snapshot)
  values(v_contract_id,v_client_id,current_date,current_date+6,'PAYE')
  returning updated_at into v_contract_updated_at;

  select to_jsonb(cs)-'candidate_manager_approval_policy_json'-'updated_at'
  into v_before
  from public.client_settings cs
  where cs.client_id=v_client_id;

  v_result:=public.client_manager_authoriser_policy_update_v1(
    v_client_id,v_client_updated_at,
    '{"approved_emails":[" Manager@Trust.Test ","manager@trust.test"],"approved_domains":["@Trust.Test","trust.test"],"allow_free_business_email":false}'::jsonb,
    v_actor_id,'manager-authoriser-client-first-use',clock_timestamp()
  );
  if v_result#>'{policy,approved_emails}'<>'["manager@trust.test"]'::jsonb
     or v_result#>'{policy,approved_domains}'<>'["trust.test"]'::jsonb
     or (v_result#>>'{policy,allow_free_business_email}')::boolean is not false
     or coalesce((v_result->>'idempotent_replay')::boolean,true) then
    raise exception 'manager authoriser client first-use normalization failed';
  end if;

  select to_jsonb(cs)-'candidate_manager_approval_policy_json'-'updated_at'
  into v_after
  from public.client_settings cs
  where cs.client_id=v_client_id;
  if v_before is distinct from v_after then
    raise exception 'manager authoriser client narrow update changed unrelated data';
  end if;

  v_result:=public.client_manager_authoriser_policy_update_v1(
    v_client_id,v_client_updated_at,
    '{"approved_emails":["manager@trust.test"],"approved_domains":["trust.test"],"allow_free_business_email":false}'::jsonb,
    v_actor_id,'manager-authoriser-client-first-use',clock_timestamp()
  );
  if coalesce((v_result->>'idempotent_replay')::boolean,false) is not true then
    raise exception 'manager authoriser client idempotent replay failed';
  end if;

  begin
    perform public.client_manager_authoriser_policy_update_v1(
      v_client_id,v_client_updated_at,
      '{"approved_emails":["changed@trust.test"],"approved_domains":[],"allow_free_business_email":false}'::jsonb,
      v_actor_id,'manager-authoriser-client-first-use',clock_timestamp()
    );
    raise exception 'manager authoriser changed-body replay was accepted';
  exception when sqlstate '40001' then
    get stacked diagnostics v_error=message_text;
    if v_error<>'CANDIDATE_MANAGER_AUTHORISER_IDEMPOTENCY_CONFLICT' then raise; end if;
  end;

  select updated_at into v_contract_updated_at from public.contracts where id=v_contract_id;
  v_result:=public.contract_manager_authoriser_policy_update_v1(
    v_contract_id,v_contract_updated_at,
    '{"mode":"EXTEND","approved_emails":["contract@trust.test"],"approved_domains":["contract.test"]}'::jsonb,
    v_actor_id,'manager-authoriser-contract-extend',clock_timestamp()
  );
  if v_result#>'{effective_policy,approved_emails}'<>'["contract@trust.test","manager@trust.test"]'::jsonb
     or v_result#>'{effective_policy,approved_domains}'<>'["contract.test","trust.test"]'::jsonb
     or (v_result#>>'{effective_policy,allow_free_business_email}')::boolean is not false then
    raise exception 'manager authoriser contract additive first-use failed';
  end if;

  select updated_at into v_contract_updated_at from public.contracts where id=v_contract_id;
  v_result:=public.contract_manager_authoriser_policy_update_v1(
    v_contract_id,v_contract_updated_at,
    '{"mode":"CONTRACT_ONLY","approved_emails":[],"approved_domains":["only.test"]}'::jsonb,
    v_actor_id,'manager-authoriser-contract-only',clock_timestamp()+interval '1 millisecond'
  );
  if v_result#>'{effective_policy,approved_emails}'<>'[]'::jsonb
     or v_result#>'{effective_policy,approved_domains}'<>'["only.test"]'::jsonb
     or (v_result#>>'{effective_policy,allow_free_business_email}')::boolean is not false then
    raise exception 'manager authoriser contract-only first-use failed';
  end if;

  begin
    perform public.client_manager_authoriser_policy_update_v1(
      v_client_id,(select updated_at from public.client_settings where client_id=v_client_id),
      '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":false}'::jsonb,
      v_actor_id,'manager-authoriser-empty-restricted',clock_timestamp()
    );
    raise exception 'manager authoriser empty restricted policy was accepted';
  exception when sqlstate '22023' then
    get stacked diagnostics v_error=message_text;
    if v_error<>'CANDIDATE_MANAGER_RESTRICTED_POLICY_EMPTY' then raise; end if;
  end;

  if (private._candidate_manager_email_allowed_v1(
        '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":true}'::jsonb,
        'person@gmail.com','["gmail.com"]'::jsonb
      )->>'allowed')::boolean is not false then
    raise exception 'manager authoriser barred-domain authority was bypassed';
  end if;
end
$first_use$;

rollback;
