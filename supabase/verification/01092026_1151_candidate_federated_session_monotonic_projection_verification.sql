-- Rollback-contained first-use proof that a delayed older Candidate request
-- cannot replace or revoke the newer projected control-plane session.

\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_now timestamptz:='2026-09-01 10:00:00+00'::timestamptz;
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_membership uuid:=pg_catalog.gen_random_uuid();
  v_account_hmac bytea:=pg_catalog.decode(repeat('5',64),'hex');
  v_old_session_hmac bytea:=pg_catalog.decode(repeat('6',64),'hex');
  v_new_session_hmac bytea:=pg_catalog.decode(repeat('7',64),'hex');
  v_conflicting_session_hmac bytea:=pg_catalog.decode(repeat('8',64),'hex');
  v_context jsonb:=pg_catalog.jsonb_build_object(
    'route_context_verified',true,
    'audience','FEDERATED_MEMBERSHIP_LINK'
  );
  v_old jsonb;
  v_new jsonb;
  v_reused jsonb;
  v_new_session_id uuid;
  v_session_context jsonb;
  v_rejected boolean:=false;
begin
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'federated-monotonic-session@example.invalid',true,'CID1-ABCDF');

  perform public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,1,v_candidate,
    'CID1-ABCDF','ACTIVE',v_now
  );

  v_old:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_old_session_hmac,v_membership,1,v_candidate,2,40,
    v_now,v_now+interval '5 minutes'
  );
  v_new:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_new_session_hmac,v_membership,1,v_candidate,2,41,
    v_now+interval '5 seconds',v_now+interval '5 minutes 5 seconds'
  );
  v_new_session_id:=(v_new->>'session_id')::uuid;

  if (v_old->>'session_id')::uuid=v_new_session_id
     or (select status from public.candidate_app_sessions
         where id=(v_old->>'session_id')::uuid)<>'REVOKED'
     or (select status from public.candidate_app_sessions
         where id=v_new_session_id)<>'ACTIVE' then
    raise exception using errcode='ZX999',message='FEDERATED_NEW_SESSION_DID_NOT_WIN';
  end if;

  begin
    perform public.candidate_app_federated_session_project_v1(
      'TEST',v_account_hmac,v_old_session_hmac,v_membership,1,v_candidate,2,40,
      v_now+interval '10 seconds',v_now+interval '5 minutes 10 seconds'
    );
  exception when sqlstate '28000' then
    if sqlerrm<>'CANDIDATE_FEDERATED_SESSION_STALE' then raise; end if;
    v_rejected:=true;
  end;
  if not v_rejected then
    raise exception using errcode='ZX999',message='FEDERATED_OLD_SESSION_WAS_NOT_REJECTED';
  end if;

  if (select count(*) from public.candidate_app_sessions
      where membership_id=v_membership and status='ACTIVE')<>1
     or (select session_epoch from public.candidate_app_sessions
         where membership_id=v_membership and status='ACTIVE')<>41
     or (select id from public.candidate_app_sessions
         where membership_id=v_membership and status='ACTIVE')<>v_new_session_id then
    raise exception using errcode='ZX999',message='FEDERATED_STALE_REQUEST_REPLACED_NEW_SESSION';
  end if;

  v_rejected:=false;
  begin
    perform public.candidate_app_federated_session_project_v1(
      'TEST',v_account_hmac,v_conflicting_session_hmac,v_membership,1,v_candidate,2,41,
      v_now+interval '15 seconds',v_now+interval '5 minutes 15 seconds'
    );
  exception when sqlstate '28000' then
    if sqlerrm<>'CANDIDATE_FEDERATED_SESSION_STALE' then raise; end if;
    v_rejected:=true;
  end;
  if not v_rejected then
    raise exception using errcode='ZX999',message='FEDERATED_SAME_EPOCH_CONFLICT_NOT_REJECTED';
  end if;

  v_reused:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_new_session_hmac,v_membership,1,v_candidate,2,41,
    v_now+interval '20 seconds',v_now+interval '5 minutes 20 seconds'
  );
  if v_reused->>'idempotent_reuse' is distinct from 'true'
     or (v_reused->>'session_id')::uuid<>v_new_session_id
     or (v_reused->>'expires_at_utc')::timestamptz<>v_now+interval '5 minutes 20 seconds' then
    raise exception using errcode='ZX999',message='FEDERATED_CURRENT_SESSION_REUSE_INVALID';
  end if;

  -- The newest session remains usable after both delayed/conflicting arrivals.
  v_session_context:=private._candidate_session_context_v1(
    v_new_session_id,'TEST',0,v_now+interval '4 minutes',true
  );
  if (v_session_context->>'session_id')::uuid<>v_new_session_id
     or (v_session_context->>'selected_candidate_id')::uuid<>v_candidate then
    raise exception using errcode='ZX999',message='FEDERATED_CURRENT_SESSION_FIRST_USE_FAILED';
  end if;

  if not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_app_federated_session_project_v1(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_app_federated_session_project_v1(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_app_federated_session_project_v1(text,bytea,bytea,uuid,integer,uuid,integer,bigint,timestamptz,timestamptz)',
       'EXECUTE'
     ) then
    raise exception using errcode='ZX999',message='FEDERATED_SESSION_PROJECTION_ACL_INVALID';
  end if;
end;
$verification$;

rollback;

select pg_catalog.jsonb_build_object(
  'verification','PASS',
  'suite','candidate_federated_session_monotonic_projection',
  'persistent_rows',0
) as candidate_federated_session_monotonic_projection_proof;
