-- Candidate public-auth final correction: durable resend throttles, frozen
-- challenge-token issuing versions and unknown-account login receipts.
-- Disposable PostgreSQL only; all fixture writes are rolled back.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;

do $candidate_public_auth_final_correction$
declare
  v_now timestamptz:=date_trunc('second',clock_timestamp());
  v_candidate uuid:='ca120812-2146-4000-8000-000000000001';
  v_challenge uuid;
  v_later_challenge uuid;
  v_first jsonb;
  v_replay jsonb;
  v_probe jsonb;
  v_failed boolean:=false;
  v_challenge_count integer;
  v_unknown_accounts integer;
begin
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'auth-final-correction@example.test',true,'AUTH-FINAL-CORRECTION');

  v_first:=public.candidate_auth_challenge_transition_v1(
    'START','TEST','auth-final-correction@example.test','ACTIVATE',null,
    decode(repeat('71',32),'hex'),'auth-final-start-v1',v_now,2
  );
  v_challenge:=(v_first->>'challenge_id')::uuid;

  select count(*) into v_challenge_count
  from public.candidate_auth_challenges
  where email_normalized='auth-final-correction@example.test';

  v_first:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_challenge,
    decode(repeat('72',32),'hex'),'auth-final-too-soon-v1',v_now+interval '10 seconds',2
  );
  if v_first->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_TOO_SOON'
     or coalesce((v_first->>'retry_after_seconds')::integer,0)<>50
     or coalesce((v_first->>'terminal')::boolean,true) then
    raise exception 'resend-too-soon did not return its durable throttle: %',v_first;
  end if;

  v_probe:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_challenge,
    null,'auth-final-too-soon-v1',v_now+interval '70 seconds',null
  );
  if not coalesce((v_probe->>'replay_receipt_found')::boolean,false)
     or v_probe->>'token_hash_hex'<>repeat('72',32)
     or (v_probe->>'token_key_version')::integer<>2 then
    raise exception 'challenge receipt did not freeze token hash/version: %',v_probe;
  end if;

  v_replay:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_challenge,
    decode(repeat('73',32),'hex'),'auth-final-too-soon-v1',v_now+interval '70 seconds',1
  );
  if v_replay->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_TOO_SOON'
     or coalesce((v_replay->>'retry_after_seconds')::integer,0)<>50
     or not coalesce((v_replay->>'idempotent_replay')::boolean,false) then
    raise exception 'same resend key changed effect after throttle elapsed: %',v_replay;
  end if;
  if (select count(*) from public.candidate_auth_challenges
      where email_normalized='auth-final-correction@example.test')<>v_challenge_count then
    raise exception 'throttled resend created a replacement challenge';
  end if;

  v_first:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_challenge,
    decode(repeat('74',32),'hex'),'auth-final-after-delay-v1',v_now+interval '70 seconds',1
  );
  if not coalesce((v_first->>'ok')::boolean,false)
     or not coalesce((v_first->>'deliver_email')::boolean,false) then
    raise exception 'new resend key did not run after delay: %',v_first;
  end if;
  v_later_challenge:=(v_first->>'challenge_id')::uuid;

  update public.candidate_auth_challenges
  set resend_count=5,last_sent_at_utc=v_now
  where id=v_later_challenge;
  v_first:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_later_challenge,
    decode(repeat('75',32),'hex'),'auth-final-limit-v1',v_now+interval '90 seconds',1
  );
  v_replay:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','auth-final-correction@example.test','ACTIVATE',v_later_challenge,
    decode(repeat('76',32),'hex'),'auth-final-limit-v1',v_now+interval '190 seconds',2
  );
  if v_first->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_LIMIT'
     or not coalesce((v_first->>'terminal')::boolean,false)
     or v_replay->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_LIMIT'
     or not coalesce((v_replay->>'idempotent_replay')::boolean,false) then
    raise exception 'resend-limit result was not durable: first %, replay %',v_first,v_replay;
  end if;

  select count(*) into v_unknown_accounts
  from public.candidate_app_accounts
  where email_normalized='unknown-auth-final@example.test';
  v_first:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',null,'unknown-auth-final@example.test',null,null,
    jsonb_build_object(
      'login_failed',true,'idempotency_request_sha256',repeat('81',32),
      'idempotency_key_version',1
    ),'unknown-auth-final-login-v1',v_now
  );
  v_replay:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',null,'unknown-auth-final@example.test',null,null,
    jsonb_build_object(
      'login_failed',true,'idempotency_request_sha256',repeat('81',32),
      'idempotency_key_version',1,'replay_probe_only',true
    ),'unknown-auth-final-login-v1',v_now+interval '1 second'
  );
  if coalesce((v_first->>'ok')::boolean,true)
     or coalesce((v_first->>'failed_login_recorded')::boolean,true)
     or v_first->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or not coalesce((v_replay->>'idempotent_replay')::boolean,false) then
    raise exception 'unknown-account failure was not durably replayed: first %, replay %',v_first,v_replay;
  end if;
  if (select count(*) from public.candidate_app_accounts
      where email_normalized='unknown-auth-final@example.test')<>v_unknown_accounts then
    raise exception 'unknown-account login created or mutated an account';
  end if;

  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',null,'changed-unknown-auth-final@example.test',null,null,
      jsonb_build_object(
        'login_failed',true,'idempotency_request_sha256',repeat('82',32),
        'idempotency_key_version',1,'replay_probe_only',true
      ),'unknown-auth-final-login-v1',v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'unknown-account login key accepted changed factual input';
  end if;

  if to_regprocedure(
       'public.candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamptz,integer)'
     ) is null
     or to_regprocedure(
       'public.candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamptz)'
     ) is not null then
    raise exception 'Candidate challenge RPC overload boundary changed unexpectedly';
  end if;
end;
$candidate_public_auth_final_correction$;

rollback;
