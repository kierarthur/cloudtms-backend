-- Mandatory NEW/UPGRADE catalogue and first-use proof for Candidate Banking
-- group-first pagination. Synthetic calls are transaction-local and rolled back.
\set ON_ERROR_STOP on

begin;
set local statement_timeout='45s';
set local client_min_messages='warning';

do $catalog$
declare
  v_identity regprocedure := 'public.pay_workbench_session_get_candidate_ready_group_page_v1(uuid,uuid,jsonb,uuid,text,text,text,integer)'::regprocedure;
  v_definition text;
begin
  select pg_get_functiondef(v_identity) into strict v_definition;
  if not has_function_privilege('service_role',v_identity,'EXECUTE')
     or has_function_privilege('anon',v_identity,'EXECUTE')
     or has_function_privilege('authenticated',v_identity,'EXECUTE')
     or has_function_privilege('public',v_identity,'EXECUTE') then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: RPC ACL mismatch';
  end if;
  if not (select p.prosecdef from pg_catalog.pg_proc p where p.oid=v_identity::oid)
     or not exists(select 1 from pg_catalog.pg_proc p where p.oid=v_identity::oid
       and p.proconfig @> array['search_path=""']::text[])
     or not exists(select 1 from pg_catalog.pg_proc p where p.oid=v_identity::oid
       and p.proconfig @> array['statement_timeout=3s']::text[])
     or not exists(select 1 from pg_catalog.pg_proc p where p.oid=v_identity::oid
       and p.proconfig @> array['lock_timeout=1s']::text[]) then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: RPC security or timeout mismatch';
  end if;
  if lower(v_definition) like '%pg_catalog.coalesce(%'
     or lower(v_definition) like '%pg_catalog.nullif(%'
     or lower(v_definition) like '%pg_catalog.least(%'
     or lower(v_definition) like '%pg_catalog.greatest(%' then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: illegal conditional qualification';
  end if;
end;
$catalog$;

do $first_use$
declare
  v_actor uuid:=gen_random_uuid();v_session uuid:=gen_random_uuid();v_candidate uuid:=gen_random_uuid();
  v_options jsonb:=jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',1,
    'scope_hash',repeat('a',64),'pay_channel_scope','ALL');
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'banking-group-page-'||v_actor::text||'@example.invalid','UNUSABLE_VERIFICATION_ONLY','admin',true);
  begin
    perform public.pay_workbench_session_get_candidate_ready_group_page_v1(v_session,v_candidate,v_options,v_actor,
      'ROW',v_candidate::text,null,10);
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: missing session was accepted';
  exception when sqlstate 'P0001' then
    if sqlerrm is distinct from 'WORKBENCH_SESSION_NOT_FOUND' then raise;end if;
  end;
end;
$first_use$;

rollback;
