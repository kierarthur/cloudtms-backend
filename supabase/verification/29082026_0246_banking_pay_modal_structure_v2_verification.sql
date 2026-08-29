-- Mandatory NEW/UPGRADE verification for Banking Pay Modal Structure v2.
-- All first-use rows are synthetic, unusable, transaction-local and rolled back.

\set ON_ERROR_STOP on

begin;
set local statement_timeout = '45s';
set local client_min_messages = 'warning';

do $catalog$
declare
  v_contract jsonb;
  v_identity regprocedure;
  v_definition text;
  v_public_identities regprocedure[] := array[
    'public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer)'::regprocedure,
    'public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)'::regprocedure,
    'public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)'::regprocedure,
    'public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer)'::regprocedure,
    'public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text)'::regprocedure,
    'public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer)'::regprocedure,
    'public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text)'::regprocedure,
    'public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)'::regprocedure,
    'public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)'::regprocedure,
    'public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)'::regprocedure,
    'public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb)'::regprocedure
  ];
begin
  v_contract := public.pay_workbench_contract_version_get_v1();
  if v_contract->>'contract_version' is distinct from 'BANKING_PAY_WORKBENCH_DB_V1'
     or v_contract#>>'{banking_pay_workbench_v2,available}' is distinct from 'true'
     or v_contract#>>'{banking_pay_workbench_v2,contract_version}' is distinct from '1'
     or v_contract#>>'{banking_pay_workbench_v2,surface_contract}' is distinct from 'BANKING_PAY_MODAL_STRUCTURE_V2' then
    raise exception 'BANKING_PAY_V2_VERIFY: capability contract is incomplete';
  end if;

  foreach v_identity in array v_public_identities loop
    select pg_get_functiondef(v_identity) into strict v_definition;
    if not has_function_privilege('service_role', v_identity, 'EXECUTE')
       or has_function_privilege('anon', v_identity, 'EXECUTE')
       or has_function_privilege('authenticated', v_identity, 'EXECUTE')
       or has_function_privilege('public', v_identity, 'EXECUTE') then
      raise exception 'BANKING_PAY_V2_VERIFY: public RPC ACL mismatch for %', v_identity;
    end if;
    if not (select p.prosecdef from pg_catalog.pg_proc p where p.oid=v_identity::oid)
       or not exists (
         select 1 from pg_catalog.pg_proc p
         where p.oid=v_identity::oid and p.proconfig @> array['search_path=""']::text[]
       ) then
      raise exception 'BANKING_PAY_V2_VERIFY: public RPC security/search_path mismatch for %', v_identity;
    end if;
    if lower(v_definition) like '%pg_catalog.coalesce(%'
       or lower(v_definition) like '%pg_catalog.nullif(%'
       or lower(v_definition) like '%pg_catalog.least(%'
       or lower(v_definition) like '%pg_catalog.greatest(%' then
      raise exception 'BANKING_PAY_V2_VERIFY: illegal conditional qualification in %', v_identity;
    end if;
  end loop;

  if exists (
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname like 'pay_workbench_modal%'
      and (has_function_privilege('service_role',p.oid,'EXECUTE')
        or has_function_privilege('anon',p.oid,'EXECUTE')
        or has_function_privilege('authenticated',p.oid,'EXECUTE')
        or has_function_privilege('public',p.oid,'EXECUTE')
        or p.prosecdef)
  ) then
    raise exception 'BANKING_PAY_V2_VERIFY: private helper execution boundary widened';
  end if;
end;
$catalog$;

do $first_use$
declare
  v_actor uuid := gen_random_uuid();
  v_session uuid := gen_random_uuid();
  v_candidate uuid := gen_random_uuid();
  v_request uuid := gen_random_uuid();
  v_row uuid := gen_random_uuid();
  v_options jsonb := jsonb_build_object(
    'expected_session_version', 1,
    'expected_progress_counter_version', 1,
    'scope_hash', repeat('a',64),
    'pay_channel_scope', 'ALL'
  );
  v_summary_options jsonb := jsonb_build_object(
    'expected_session_version', 1,
    'expected_progress_counter_version', 1,
    'pay_channel_scope', 'ALL'
  );
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'banking-v2-verifier-'||v_actor::text||'@example.invalid','UNUSABLE_VERIFICATION_ONLY','admin',true);

  begin
    perform public.pay_workbench_session_get_candidate_summary_page_v1(
      v_session,v_summary_options,v_actor,'CANDIDATE','ASC',null,100);
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session summary was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_get_candidate_ready_page_v1(
      v_session,v_candidate,v_options,v_actor,null,100);
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session Ready detail was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_get_action_required_page_v1(
      v_session,v_options,v_actor,'TITLE','ASC',null,100,'','ACTION_REQUIRED');
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session Action list was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_get_blocked_page_v1(
      v_session,v_options,v_actor,'CANDIDATE','ASC',null,100,'');
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session Blocked list was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_set_candidate_ready_selection_v1(
      v_session,v_candidate,v_options,v_actor,'SELECT_ALL_READY',v_request,repeat('b',64),null);
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session candidate mutation was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_set_filtered_ready_selection_v1(
      v_session,v_options,v_actor,'SELECT_ALL_READY',v_request,repeat('b',64));
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session all-page mutation was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_set_ready_rows_v1(
      v_session,v_candidate,v_options,v_actor,jsonb_build_array(v_row),true,v_request,repeat('b',64),null);
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session row mutation was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
  begin
    perform public.pay_workbench_session_set_ready_group_v1(
      v_session,v_candidate,v_options,v_actor,'TIMESHEET','verification-group',true,v_request,repeat('b',64),null);
    raise exception 'BANKING_PAY_V2_VERIFY: missing-session group mutation was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise; end if;
  end;
end;
$first_use$;

rollback;
