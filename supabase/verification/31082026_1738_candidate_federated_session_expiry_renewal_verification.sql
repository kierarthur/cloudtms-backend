-- Rollback-contained first-use proof that a freshly verified federated route
-- renews its exact agency-local session beyond the previous expiry without
-- changing the session identity or weakening its private ACL.

\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_now timestamptz:='2026-08-31 12:00:00+00'::timestamptz;
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_membership uuid:=pg_catalog.gen_random_uuid();
  v_account_hmac bytea:=pg_catalog.decode(repeat('3',64),'hex');
  v_session_hmac bytea:=pg_catalog.decode(repeat('4',64),'hex');
  v_context jsonb:=pg_catalog.jsonb_build_object(
    'route_context_verified',true,
    'audience','FEDERATED_MEMBERSHIP_LINK'
  );
  v_first jsonb;
  v_renewed jsonb;
  v_session_context jsonb;
  v_session_id uuid;
  v_old_expiry timestamptz:=v_now+interval '2 minutes';
  v_new_now timestamptz:=v_now+interval '1 minute';
  v_new_expiry timestamptz:=v_now+interval '6 minutes';
begin
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'federated-expiry-renewal@example.invalid',true,'CID1-ABCDE');

  perform public.candidate_app_federated_membership_link_set_v1(
    v_context,'TEST',v_account_hmac,v_membership,1,v_candidate,
    'CID1-ABCDE','ACTIVE',v_now
  );

  v_first:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_session_hmac,v_membership,1,v_candidate,1,1,
    v_now,v_old_expiry
  );
  v_session_id:=(v_first->>'session_id')::uuid;

  if v_first->>'idempotent_reuse' is distinct from 'false'
     or (v_first->>'expires_at_utc')::timestamptz is distinct from v_old_expiry
     or (v_first->>'absolute_expires_at_utc')::timestamptz is distinct from v_old_expiry then
    raise exception using errcode='ZX999',message='FEDERATED_INITIAL_EXPIRY_INVALID';
  end if;

  v_renewed:=public.candidate_app_federated_session_project_v1(
    'TEST',v_account_hmac,v_session_hmac,v_membership,1,v_candidate,1,1,
    v_new_now,v_new_expiry
  );

  if v_renewed->>'idempotent_reuse' is distinct from 'true'
     or (v_renewed->>'session_id')::uuid is distinct from v_session_id
     or (v_renewed->>'expires_at_utc')::timestamptz is distinct from v_new_expiry
     or (v_renewed->>'absolute_expires_at_utc')::timestamptz is distinct from v_new_expiry then
    raise exception using errcode='ZX999',message='FEDERATED_RENEWED_PROJECTION_INVALID';
  end if;

  if (select expires_at_utc from public.candidate_app_sessions where id=v_session_id)
       is distinct from v_new_expiry
     or (select absolute_expires_at_utc from public.candidate_app_sessions where id=v_session_id)
       is distinct from v_new_expiry
     or (select last_used_at_utc from public.candidate_app_sessions where id=v_session_id)
       is distinct from v_new_now
     or (select count(*) from public.candidate_app_sessions
         where membership_id=v_membership and status='ACTIVE')<>1 then
    raise exception using errcode='ZX999',message='FEDERATED_RENEWED_SESSION_STATE_INVALID';
  end if;

  -- This is deliberately after the original two-minute expiry. The renewed
  -- session must still pass the same locked Candidate session authority.
  v_session_context:=private._candidate_session_context_v1(
    v_session_id,'TEST',0,v_now+interval '3 minutes',true
  );
  if (v_session_context->>'session_id')::uuid is distinct from v_session_id
     or (v_session_context->>'selected_candidate_id')::uuid is distinct from v_candidate
     or (v_session_context->>'expires_at_utc')::timestamptz is distinct from v_new_expiry then
    raise exception using errcode='ZX999',message='FEDERATED_RENEWED_SESSION_FIRST_USE_FAILED';
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
  'suite','candidate_federated_session_expiry_renewal',
  'persistent_rows',0
) as candidate_federated_session_expiry_renewal_proof;
