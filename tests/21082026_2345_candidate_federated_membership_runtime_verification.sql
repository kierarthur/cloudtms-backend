-- Rollback-only real PostgreSQL proof for the agency-local central-membership
-- link and short-lived compatibility session.

begin;

do $verification$
declare
  v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_membership uuid:=pg_catalog.gen_random_uuid();
  v_account_hmac bytea:=pg_catalog.decode(repeat('1',64),'hex');
  v_session_hmac bytea:=pg_catalog.decode(repeat('2',64),'hex');
  v_context jsonb:=pg_catalog.jsonb_build_object(
    'route_context_verified',true,'audience','FEDERATED_MEMBERSHIP_LINK'
  );
  v_result jsonb;
  v_session uuid;
begin
  insert into public.candidates(id,email,active,key_norm)
    values(v_candidate,'federated-runtime@example.invalid',true,'CID1-ABCDE');

  v_result:=public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,1,v_candidate,
    'CID1-ABCDE','ACTIVE',v_now
  );
  if v_result->>'status'<>'LINKED'
     or (select state from public.candidate_app_global_membership_links where membership_id=v_membership)<>'ACTIVE'
     or exists(
       select 1 from public.candidate_app_accounts a
       join public.candidate_app_global_membership_links l on l.account_id=a.id
       where l.membership_id=v_membership
         and (a.password_scheme is not null or a.password_salt is not null or a.password_digest is not null)
     ) then
    raise exception using errcode='ZX999',message='FEDERATED_LINK_CREATION_FAILED';
  end if;

  v_result:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_session_hmac,v_membership,1,v_candidate,1,1,
    v_now,v_now+interval '4 minutes'
  );
  v_session:=(v_result->>'session_id')::uuid;
  if v_result->>'idempotent_reuse'<>'false'
     or (select auth_source from public.candidate_app_sessions where id=v_session)<>'CONTROL_PLANE' then
    raise exception using errcode='ZX999',message='FEDERATED_SESSION_CREATION_FAILED';
  end if;
  v_result:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_session_hmac,v_membership,1,v_candidate,1,1,
    v_now+interval '1 second',v_now+interval '4 minutes'
  );
  if v_result->>'idempotent_reuse'<>'true' or (v_result->>'session_id')::uuid<>v_session then
    raise exception using errcode='ZX999',message='FEDERATED_SESSION_REUSE_FAILED';
  end if;

  perform public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,2,v_candidate,
    'CID1-ABCDE','ACTIVE',v_now+interval '2 seconds'
  );
  begin
    perform public.candidate_app_federated_session_project_v1(
      'TEST',v_account_hmac,v_session_hmac,v_membership,1,v_candidate,1,1,
      v_now+interval '3 seconds',v_now+interval '4 minutes'
    );
    raise exception using errcode='ZX999',message='STALE_MEMBERSHIP_GENERATION_ACCEPTED';
  exception when sqlstate '28000' then null;
  end;

  perform public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,3,v_candidate,
    'CID1-ABCDE','DISABLED',v_now+interval '4 seconds'
  );
  if exists(
    select 1 from public.candidate_app_sessions
    where membership_id=v_membership and status='ACTIVE'
  ) then
    raise exception using errcode='ZX999',message='DISABLED_MEMBERSHIP_SESSION_REMAINED_ACTIVE';
  end if;
  begin
    perform public.candidate_app_federated_session_project_v1(
      'TEST',v_account_hmac,v_session_hmac,v_membership,3,v_candidate,1,2,
      v_now+interval '5 seconds',v_now+interval '4 minutes'
    );
    raise exception using errcode='ZX999',message='DISABLED_MEMBERSHIP_PROJECTED';
  exception when sqlstate '28000' then null;
  end;
end;
$verification$;

rollback;

select pg_catalog.jsonb_build_object(
  'verification','PASS','suite','candidate_federated_membership_runtime',
  'persistent_rows',0
) as candidate_federated_membership_runtime_proof;
