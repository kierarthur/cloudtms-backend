-- Candidate authentication/account idempotency must be receipt-first across
-- lost responses while preserving refresh-token theft detection for a new key.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;

do $candidate_auth_receipts$
declare
  v_now timestamptz:=clock_timestamp();
  v_candidate_one uuid:='ca120812-0000-4000-8000-000000000001';
  v_candidate_two uuid:='ca120812-0000-4000-8000-000000000002';
  v_account uuid:='ca120812-0000-4000-8000-000000000003';
  v_session uuid:='ca120812-0000-4000-8000-000000000004';
  v_login_session uuid:='ca120812-0000-4000-8000-000000000005';
  v_family uuid:='ca120812-0000-4000-8000-000000000006';
  v_notification_one uuid:='ca120812-0000-4000-8000-000000000007';
  v_notification_two uuid:='ca120812-0000-4000-8000-000000000008';
  v_challenge uuid;
  v_resend uuid;
  v_result jsonb;
  v_failed boolean;
  v_receipt_text text;
begin
  insert into public.candidates(id,email,active,key_norm) values
    (v_candidate_one,'candidate-auth-receipt@example.test',true,'AUTH-RECEIPT-1'),
    (v_candidate_two,'candidate-auth-receipt@example.test',true,'AUTH-RECEIPT-2');
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_params_json,password_changed_at_utc
  ) values (
    v_account,'TEST','candidate-auth-receipt@example.test','ACTIVE',
    'PBKDF2-HMAC-SHA256',1,decode(repeat('51',16),'hex'),
    decode(repeat('52',32),'hex'),jsonb_build_object('iterations',100000),v_now
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    token_family_id,rotation,issued_at_utc,expires_at_utc,absolute_expires_at_utc,
    last_used_at_utc,created_at_utc,updated_at_utc
  ) values (
    v_session,v_account,'TEST',v_candidate_one,'ACTIVE',decode(repeat('53',32),'hex'),
    v_family,0,v_now,v_now+interval '30 days',v_now+interval '90 days',
    v_now,v_now,v_now
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,event_type,preference_category,template_key,
    dedupe_key,created_at_utc
  ) values
    (v_notification_one,v_account,v_candidate_one,'AUTH_RECEIPT_TEST','SYSTEM',
      'AUTH_RECEIPT_TEST','candidate-auth-receipt-notification-1',v_now),
    (v_notification_two,v_account,v_candidate_one,'AUTH_RECEIPT_TEST','SYSTEM',
      'AUTH_RECEIPT_TEST','candidate-auth-receipt-notification-2',v_now);

  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'SET_NOTIFICATION_PREFERENCES','TEST',v_account,null,v_session,null,
      jsonb_build_object('notification_preferences',jsonb_build_object('email',true)),
      null,v_now
    );
  exception when sqlstate '22023' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'account mutation accepted a missing idempotency key'; end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-auth-receipt@example.test',
    null,null,jsonb_build_object(
      'login_failed',true,'idempotency_request_sha256',repeat('a0',32),
      'idempotency_key_version',1,
      'presented_password_digest_hex',repeat('99',32),
      'expected_password_authority_sha256',private._candidate_password_authority_sha256_v1(
        v_account,'PBKDF2-HMAC-SHA256',1::smallint,decode(repeat('51',16),'hex'),
        decode(repeat('52',32),'hex'),jsonb_build_object('iterations',100000)
      )
    ),'auth-login-failure-receipt-v1',v_now-interval '2 seconds'
  );
  if coalesce((v_result->>'ok')::boolean,true)
     or v_result->>'error_code'<>'CANDIDATE_LOGIN_INVALID' then
    raise exception 'failed login did not record its canonical result: %',v_result;
  end if;
  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-auth-receipt@example.test',
    null,null,jsonb_build_object(
      'login_failed',true,'idempotency_request_sha256',repeat('a0',32),
      'idempotency_key_version',1,'replay_probe_only',true
    ),'auth-login-failure-receipt-v1',v_now-interval '1 second'
  );
  if coalesce((v_result->>'ok')::boolean,true)
     or not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or (select failed_login_count from public.candidate_app_accounts where id=v_account)<>1 then
    raise exception 'failed login replay advanced the lockout counter: %',v_result;
  end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-auth-receipt@example.test',
    v_login_session,v_candidate_one,jsonb_build_object(
      'refresh_token_hash_hex',repeat('54',32),
      'presented_password_digest_hex',repeat('52',32),
      'expected_password_authority_sha256',private._candidate_password_authority_sha256_v1(
        v_account,'PBKDF2-HMAC-SHA256',1::smallint,decode(repeat('51',16),'hex'),
        decode(repeat('52',32),'hex'),jsonb_build_object('iterations',100000)
      ),
      'expires_at_utc',v_now+interval '30 days',
      'absolute_expires_at_utc',v_now+interval '90 days',
      'idempotency_request_sha256',repeat('a1',32),'idempotency_key_version',1
    ),'auth-login-receipt-v1',v_now
  );
  if (v_result->>'session_id')::uuid<>v_login_session then
    raise exception 'login receipt setup failed: %',v_result;
  end if;
  v_result:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',v_account,'candidate-auth-receipt@example.test',
    'ca120812-0000-4000-8000-000000000099',v_candidate_one,jsonb_build_object(
      'refresh_token_hash_hex',repeat('99',32),
      'idempotency_request_sha256',repeat('a1',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-login-receipt-v1',v_now+interval '1 second'
  );
  if (v_result->>'session_id')::uuid<>v_login_session
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'login lost-response replay did not return the original session: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',v_account,'candidate-auth-receipt@example.test',
      v_login_session,v_candidate_one,jsonb_build_object(
        'idempotency_request_sha256',repeat('a2',32),'idempotency_key_version',1,
        'replay_probe_only',true
      ),'auth-login-receipt-v1',v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'login accepted a changed request under the same key'; end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'SELECT_TEST_CANDIDATE','TEST',v_account,null,v_session,v_candidate_two,
    jsonb_build_object('idempotency_request_sha256',repeat('b1',32),'idempotency_key_version',1),
    'auth-select-receipt-v1',v_now+interval '3 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'SELECT_TEST_CANDIDATE','TEST',v_account,null,v_session,v_candidate_one,
    jsonb_build_object(
      'idempotency_request_sha256',repeat('b1',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-select-receipt-v1',v_now+interval '4 seconds'
  );
  if (v_result->>'selected_candidate_id')::uuid<>v_candidate_two
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'candidate selection replay was not receipt-owned: %',v_result;
  end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'SET_NOTIFICATION_PREFERENCES','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'notification_preferences',jsonb_build_object('email',true,'push',false),
      'idempotency_request_sha256',repeat('b2',32),'idempotency_key_version',1
    ),'auth-preferences-receipt-v1',v_now+interval '5 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'SET_NOTIFICATION_PREFERENCES','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'notification_preferences',jsonb_build_object('email',false),
      'idempotency_request_sha256',repeat('b2',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-preferences-receipt-v1',v_now+interval '6 seconds'
  );
  if coalesce((v_result#>>'{notification_preferences,email}')::boolean,false)=false
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'notification preference replay was not exact: %',v_result;
  end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'MARK_NOTIFICATION_READ','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'notification_id',v_notification_one,
      'idempotency_request_sha256',repeat('b6',32),'idempotency_key_version',1
    ),'auth-notification-read-v1',v_now+interval '6 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'MARK_NOTIFICATION_READ','TEST',v_account,null,v_session,null,
    jsonb_build_object(
      'notification_id',v_notification_one,
      'idempotency_request_sha256',repeat('b6',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-notification-read-v1',v_now+interval '7 seconds'
  );
  if v_result->>'state'<>'READ'
     or not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or (v_result->>'read_at_utc')::timestamptz<>v_now+interval '6 seconds' then
    raise exception 'notification read replay was not exact: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'MARK_NOTIFICATION_READ','TEST',v_account,null,v_session,null,
      jsonb_build_object(
        'notification_id',v_notification_two,
        'idempotency_request_sha256',repeat('b7',32),'idempotency_key_version',1,
        'replay_probe_only',true
      ),'auth-notification-read-v1',v_now+interval '8 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'notification read key accepted a different notification'; end if;

  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'REGISTER_PUSH_TOKEN','TEST',v_account,null,v_session,null,jsonb_build_object(
        'push_provider','WEB_PUSH','push_token_ciphertext_hex',repeat('55',32),
        'push_key_version',32768,'push_token_identity_hmac',repeat('54',32),
        'push_token_identity_key_version',1,'idempotency_request_sha256',repeat('b8',32),
        'idempotency_key_version',1
      ),'auth-push-version-overflow-v1',v_now+interval '9 seconds'
    );
  exception when sqlstate '22023' then
    v_failed:=position('CANDIDATE_PUSH_TOKEN_INVALID' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'push encryption key version exceeded database-safe range'; end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'REGISTER_PUSH_TOKEN','TEST',v_account,null,v_session,null,jsonb_build_object(
      'push_provider','WEB_PUSH','push_token_ciphertext_hex',repeat('55',32),
      'push_key_version',1,'push_token_identity_hmac',repeat('54',32),
      'push_token_identity_key_version',1,'idempotency_request_sha256',repeat('b3',32),
      'idempotency_key_version',1
    ),'auth-push-receipt-v1',v_now+interval '10 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'REGISTER_PUSH_TOKEN','TEST',v_account,null,v_session,null,jsonb_build_object(
      'replay_probe_only',true
    ),'auth-push-receipt-v1',v_now+interval '10.5 seconds'
  );
  if not coalesce((v_result->>'replay_receipt_found')::boolean,false)
     or v_result->>'push_token_identity_key_version'<>'1' then
    raise exception 'push semantic identity version was not frozen: %',v_result;
  end if;
  v_result:=public.candidate_auth_account_transition_v1(
    'REGISTER_PUSH_TOKEN','TEST',v_account,null,v_session,null,jsonb_build_object(
      'idempotency_request_sha256',repeat('b3',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-push-receipt-v1',v_now+interval '11 seconds'
  );
  if not coalesce((v_result->>'push_registered')::boolean,false)
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'push registration replay was not exact: %',v_result;
  end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'CHANGE_PASSWORD','TEST',v_account,null,v_session,null,jsonb_build_object(
      'password_scheme','PBKDF2-HMAC-SHA256','password_scheme_version',1,
      'password_salt_hex',repeat('56',16),'password_digest_hex',repeat('57',32),
      'password_params',jsonb_build_object('iterations',100000),
      'presented_password_digest_hex',repeat('52',32),
      'expected_password_authority_sha256',private._candidate_password_authority_sha256_v1(
        v_account,'PBKDF2-HMAC-SHA256',1::smallint,decode(repeat('51',16),'hex'),
        decode(repeat('52',32),'hex'),jsonb_build_object('iterations',100000)
      ),
      'idempotency_request_sha256',repeat('b4',32),'idempotency_key_version',1
    ),'auth-password-receipt-v1',v_now+interval '12 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'CHANGE_PASSWORD','TEST',v_account,null,v_session,null,jsonb_build_object(
      'idempotency_request_sha256',repeat('b4',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-password-receipt-v1',v_now+interval '13 seconds'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'password change did not replay before verifier validation: %',v_result;
  end if;

  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGOUT','TEST',v_account,null,v_session,null,jsonb_build_object(
        'idempotency_request_sha256',repeat('b1',32),'idempotency_key_version',1
      ),'auth-select-receipt-v1',v_now+interval '14 seconds'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'same auth key was accepted for a different action'; end if;

  v_result:=public.candidate_auth_account_transition_v1(
    'LOGOUT','TEST',v_account,null,v_session,null,jsonb_build_object(
      'idempotency_request_sha256',repeat('b5',32),'idempotency_key_version',1
    ),'auth-logout-receipt-v1',v_now+interval '15 seconds'
  );
  v_result:=public.candidate_auth_account_transition_v1(
    'LOGOUT','TEST',v_account,null,v_session,null,jsonb_build_object(
      'idempotency_request_sha256',repeat('b5',32),'idempotency_key_version',1,
      'replay_probe_only',true
    ),'auth-logout-receipt-v1',v_now+interval '16 seconds'
  );
  if v_result->>'status'<>'REVOKED'
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'logout did not replay after the session became inactive: %',v_result;
  end if;

  v_result:=public.candidate_auth_challenge_transition_v1(
    'START','TEST','candidate-auth-receipt@example.test','RESET',null,
    decode(repeat('61',32),'hex'),'auth-challenge-start-v1',v_now,1
  );
  v_challenge:=(v_result->>'challenge_id')::uuid;
  v_failed:=false;
  begin
    perform public.candidate_auth_challenge_transition_v1(
      'START','TEST','different-auth-receipt@example.test','RESET',null,
      decode(repeat('61',32),'hex'),'auth-challenge-start-v1',v_now+interval '1 second',1
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'challenge key accepted a changed email'; end if;
  v_result:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','candidate-auth-receipt@example.test','RESET',v_challenge,
    decode(repeat('62',32),'hex'),'auth-challenge-resend-v1',v_now+interval '61 seconds',1
  );
  v_resend:=(v_result->>'challenge_id')::uuid;
  v_result:=public.candidate_auth_challenge_transition_v1(
    'RESEND','TEST','candidate-auth-receipt@example.test','RESET',v_challenge,
    decode(repeat('62',32),'hex'),'auth-challenge-resend-v1',v_now+interval '62 seconds',1
  );
  if (v_result->>'challenge_id')::uuid<>v_resend
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'challenge resend replay was not exact: %',v_result;
  end if;
  v_result:=public.candidate_auth_challenge_transition_v1(
    'VERIFY','TEST','candidate-auth-receipt@example.test','RESET',v_resend,
    decode(repeat('62',32),'hex'),'auth-challenge-verify-v1',v_now+interval '63 seconds'
  );
  v_result:=public.candidate_auth_challenge_transition_v1(
    'VERIFY','TEST','candidate-auth-receipt@example.test','RESET',v_resend,
    decode(repeat('62',32),'hex'),'auth-challenge-verify-v1',v_now+interval '64 seconds'
  );
  if v_result->>'state'<>'VERIFIED'
     or not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'challenge verification replay was not exact: %',v_result;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_auth_challenge_transition_v1(
      'VERIFY','TEST','candidate-auth-receipt@example.test','RESET',v_resend,
      decode(repeat('62',32),'hex'),null,v_now+interval '65 seconds'
    );
  exception when sqlstate '22023' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' in sqlerrm)>0;
  end;
  if not v_failed then raise exception 'challenge verification accepted a missing key'; end if;

  select string_agg(coalesce(ae.before_json::text,'')||coalesce(ae.after_json::text,''),'')
  into v_receipt_text
  from public.audit_events ae
  where ae.object_type='candidate_auth_mutation_receipt';
  if position(repeat('53',32) in coalesce(v_receipt_text,''))>0
     or position(repeat('55',32) in coalesce(v_receipt_text,''))>0
     or position(repeat('57',32) in coalesce(v_receipt_text,''))>0 then
    raise exception 'auth receipt retained a refresh/push/password verifier secret';
  end if;
end;
$candidate_auth_receipts$;

rollback;
