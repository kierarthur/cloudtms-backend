-- Candidate App clean-registration authentication RPCs.
-- No Google password hash/salt migration and no plaintext password/token input.

create or replace function public.candidate_auth_account_transition_v1(
  p_action text,
  p_environment text,
  p_account_id uuid default null,
  p_email_normalized text default null,
  p_session_id uuid default null,
  p_selected_candidate_id uuid default null,
  p_payload jsonb default '{}'::jsonb,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_environment text;
  v_email text:=private._candidate_normalize_email(p_email_normalized);
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_account public.candidate_app_accounts%rowtype;
  v_session public.candidate_app_sessions%rowtype;
  v_new_session public.candidate_app_sessions%rowtype;
  v_challenge public.candidate_auth_challenges%rowtype;
  v_eligibility jsonb;
  v_context jsonb;
  v_candidate_ids jsonb;
  v_match_count integer;
  v_salt bytea;
  v_digest bytea;
  v_refresh_hash bytea;
  v_presented_refresh_hash bytea;
  v_device_hash bytea;
  v_expires timestamptz;
  v_absolute timestamptz;
  v_new_session_id uuid;
  v_preferences jsonb;
  v_audit_action text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_account_registration');
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'CANDIDATE_AUTH_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if v_payload ?| array['password','plaintext_password','refresh_token','token'] then
    raise exception 'CANDIDATE_AUTH_PLAINTEXT_SECRET_FORBIDDEN' using errcode='22023';
  end if;

  if v_action in ('ACTIVATE_PASSWORD','CHANGE_PASSWORD') then
    if coalesce(v_payload->>'password_salt_hex','') !~ '^[0-9a-fA-F]{32,128}$'
       or length(v_payload->>'password_salt_hex')%2<>0
       or coalesce(v_payload->>'password_digest_hex','') !~ '^[0-9a-fA-F]+$'
       or length(v_payload->>'password_digest_hex') not between 64 and 256
       or length(v_payload->>'password_digest_hex')%2<>0
       or nullif(btrim(v_payload->>'password_scheme'),'') is null
       or coalesce(v_payload->>'password_scheme_version','') !~ '^[1-9][0-9]{0,4}$' then
      raise exception 'CANDIDATE_PASSWORD_VERIFIER_INVALID' using errcode='22023';
    end if;
    v_salt:=decode(v_payload->>'password_salt_hex','hex');
    v_digest:=decode(v_payload->>'password_digest_hex','hex');
  end if;

  if v_action='ACTIVATE_PASSWORD' then
    if coalesce(v_payload->>'challenge_id','') !~ '^[0-9a-fA-F-]{36}$' then
      raise exception 'CANDIDATE_VERIFIED_CHALLENGE_REQUIRED' using errcode='22023';
    end if;
    select * into v_challenge from public.candidate_auth_challenges
    where id=(v_payload->>'challenge_id')::uuid for update;
    if not found or v_challenge.environment<>v_environment
       or v_challenge.state<>'VERIFIED'
       or v_challenge.expires_at_utc<=p_now_utc
       or v_challenge.purpose not in ('ACTIVATE','RESET','RECOVERY') then
      raise exception 'CANDIDATE_VERIFIED_CHALLENGE_INVALID' using errcode='28000';
    end if;
    v_email:=v_challenge.email_normalized;
    v_eligibility:=private._candidate_email_eligibility_v1(v_environment,v_email);
    if coalesce((v_eligibility->>'eligible')::boolean,false)=false then
      raise exception 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' using errcode='28000';
    end if;
    if p_session_id is null
       or coalesce(v_payload->>'refresh_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_ACTIVATION_SESSION_REQUIRED' using errcode='22023';
    end if;
    v_refresh_hash:=decode(v_payload->>'refresh_token_hash_hex','hex');
    v_expires:=coalesce(nullif(v_payload->>'expires_at_utc','')::timestamptz,p_now_utc+interval '30 days');
    v_absolute:=coalesce(nullif(v_payload->>'absolute_expires_at_utc','')::timestamptz,p_now_utc+interval '90 days');
    if v_expires>v_absolute or v_expires<=p_now_utc or v_absolute<=p_now_utc then
      raise exception 'CANDIDATE_SESSION_EXPIRY_INVALID' using errcode='22023';
    end if;
    v_candidate_ids:=v_eligibility->'candidate_ids';
    v_match_count:=coalesce((v_eligibility->>'match_count')::integer,0);
    if p_selected_candidate_id is null and v_match_count=1 then
      p_selected_candidate_id:=(v_candidate_ids->>0)::uuid;
    end if;
    if p_selected_candidate_id is not null and not exists(
      select 1 from public.candidates c where c.id=p_selected_candidate_id and c.active=true
        and lower(btrim(coalesce(c.email,'')))=v_email
    ) then raise exception 'CANDIDATE_SELECTION_NOT_ALLOWED' using errcode='28000'; end if;
    if v_environment='TEST' and v_match_count>1 and p_selected_candidate_id is null then
      raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
    end if;
    if coalesce(v_payload->>'device_id_hash_hex','')~'^[0-9a-fA-F]{64}$' then
      v_device_hash:=decode(v_payload->>'device_id_hash_hex','hex');
    end if;

    select * into v_account from public.candidate_app_accounts
    where environment=v_environment and email_normalized=v_email for update;

    if not found then
      if v_challenge.purpose<>'ACTIVATE' then
        raise exception 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' using errcode='28000';
      end if;
      insert into public.candidate_app_accounts(
        environment,email_normalized,status,password_scheme,password_scheme_version,
        password_salt,password_digest,password_params_json,password_changed_at_utc,
        failed_login_count,locked_until_utc,created_at_utc,updated_at_utc
      ) values (
        v_environment,v_email,'ACTIVE',btrim(v_payload->>'password_scheme'),
        (v_payload->>'password_scheme_version')::smallint,v_salt,v_digest,
        coalesce(v_payload->'password_params','{}'::jsonb),p_now_utc,0,null,p_now_utc,p_now_utc
      ) returning * into v_account;
      v_audit_action:='CANDIDATE_ACCOUNT_ACTIVATED';
    else
      if v_account.status='DISABLED' then raise exception 'CANDIDATE_ACCOUNT_DISABLED' using errcode='28000'; end if;
      update public.candidate_app_accounts set
        status='ACTIVE',password_scheme=btrim(v_payload->>'password_scheme'),
        password_scheme_version=(v_payload->>'password_scheme_version')::smallint,
        password_salt=v_salt,password_digest=v_digest,
        password_params_json=coalesce(v_payload->'password_params','{}'::jsonb),
        password_changed_at_utc=p_now_utc,failed_login_count=0,locked_until_utc=null,
        session_version=session_version+1,updated_at_utc=p_now_utc
      where id=v_account.id returning * into v_account;
      update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
        revoke_reason='PASSWORD_REPLACED',updated_at_utc=p_now_utc
      where account_id=v_account.id and status='ACTIVE';
      v_audit_action:=case when v_challenge.purpose='ACTIVATE' then 'CANDIDATE_ACCOUNT_ACTIVATED' else 'CANDIDATE_PASSWORD_RESET' end;
    end if;

    update public.candidate_auth_challenges set state='CONSUMED',consumed_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_challenge.id;
    insert into public.candidate_app_sessions(
      id,account_id,environment,selected_candidate_id,status,refresh_token_hash,token_family_id,
      rotation,issued_at_utc,expires_at_utc,absolute_expires_at_utc,last_used_at_utc,
      device_id_hash,platform,created_at_utc,updated_at_utc
    ) values (
      p_session_id,v_account.id,v_environment,p_selected_candidate_id,'ACTIVE',v_refresh_hash,gen_random_uuid(),
      0,p_now_utc,v_expires,v_absolute,p_now_utc,v_device_hash,
      nullif(btrim(v_payload->>'platform'),''),p_now_utc,p_now_utc
    ) returning * into v_new_session;
    perform private._candidate_audit_v1('candidate_app_account',v_account.id::text,v_audit_action,null,
      jsonb_build_object('status',v_account.status,'session_version',v_account.session_version,
        'session_id',v_new_session.id,'selected_candidate_id',v_new_session.selected_candidate_id),
      v_challenge.purpose,null,p_idempotency_key,p_now_utc);
    return jsonb_build_object(
      'ok',true,'account_id',v_account.id,'status',v_account.status,
      'session_version',v_account.session_version,'selection_required',v_eligibility->'selection_required',
      'candidate_ids',v_eligibility->'candidate_ids','session_id',v_new_session.id,
      'rotation',v_new_session.rotation,'selected_candidate_id',v_new_session.selected_candidate_id,
      'expires_at_utc',v_new_session.expires_at_utc,
      'absolute_expires_at_utc',v_new_session.absolute_expires_at_utc
    );
  end if;

  if p_account_id is not null then
    select * into v_account from public.candidate_app_accounts where id=p_account_id for update;
  elsif v_email<>'' then
    select * into v_account from public.candidate_app_accounts
    where environment=v_environment and email_normalized=v_email for update;
  end if;

  if v_action='LOGIN_FAILURE' then
    if v_account.id is not null and v_account.environment=v_environment and v_account.status<>'DISABLED' then
      update public.candidate_app_accounts set
        failed_login_count=least(failed_login_count+1,1000),
        status=case when failed_login_count+1>=5 then 'LOCKED' else status end,
        locked_until_utc=case when failed_login_count+1>=5 then p_now_utc+interval '15 minutes' else locked_until_utc end,
        updated_at_utc=p_now_utc
      where id=v_account.id returning * into v_account;
    end if;
    return jsonb_build_object('ok',true,'accepted',true);
  end if;

  if v_action='LOGIN_SUCCESS' then
    if v_account.id is null or v_account.environment<>v_environment or v_account.status='DISABLED' then
      raise exception 'CANDIDATE_LOGIN_INVALID' using errcode='28000';
    end if;
    if v_account.status='LOCKED' and coalesce(v_account.locked_until_utc,'infinity'::timestamptz)>p_now_utc then
      raise exception 'CANDIDATE_ACCOUNT_LOCKED' using errcode='28000';
    end if;
    if v_account.password_digest is null then raise exception 'CANDIDATE_PASSWORD_SETUP_REQUIRED' using errcode='28000'; end if;
    if coalesce(v_payload->>'refresh_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_REFRESH_HASH_INVALID' using errcode='22023';
    end if;
    v_refresh_hash:=decode(v_payload->>'refresh_token_hash_hex','hex');
    v_expires:=coalesce(nullif(v_payload->>'expires_at_utc','')::timestamptz,p_now_utc+interval '30 days');
    v_absolute:=coalesce(nullif(v_payload->>'absolute_expires_at_utc','')::timestamptz,p_now_utc+interval '90 days');
    if v_expires>v_absolute or v_expires<=p_now_utc then raise exception 'CANDIDATE_SESSION_EXPIRY_INVALID' using errcode='22023'; end if;
    v_eligibility:=private._candidate_email_eligibility_v1(v_environment,v_account.email_normalized);
    v_candidate_ids:=v_eligibility->'candidate_ids';
    v_match_count:=coalesce((v_eligibility->>'match_count')::integer,0);
    if coalesce((v_eligibility->>'eligible')::boolean,false)=false then raise exception 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' using errcode='28000'; end if;
    if p_selected_candidate_id is null and v_match_count=1 then
      p_selected_candidate_id:=(v_candidate_ids->>0)::uuid;
    end if;
    if p_selected_candidate_id is not null and not exists(
      select 1 from public.candidates c where c.id=p_selected_candidate_id and c.active=true
        and lower(btrim(coalesce(c.email,'')))=v_account.email_normalized
    ) then raise exception 'CANDIDATE_SELECTION_NOT_ALLOWED' using errcode='28000'; end if;
    if v_environment='TEST' and v_match_count>1 and p_selected_candidate_id is null then
      raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
    end if;

    if coalesce(v_payload->>'device_id_hash_hex','')~'^[0-9a-fA-F]{64}$' then
      v_device_hash:=decode(v_payload->>'device_id_hash_hex','hex');
    end if;
    v_new_session_id:=coalesce(p_session_id,gen_random_uuid());
    insert into public.candidate_app_sessions(
      id,account_id,environment,selected_candidate_id,status,refresh_token_hash,token_family_id,
      rotation,issued_at_utc,expires_at_utc,absolute_expires_at_utc,last_used_at_utc,
      device_id_hash,platform,created_at_utc,updated_at_utc
    ) values (
      v_new_session_id,v_account.id,v_environment,p_selected_candidate_id,'ACTIVE',v_refresh_hash,gen_random_uuid(),
      0,p_now_utc,v_expires,v_absolute,p_now_utc,v_device_hash,nullif(btrim(v_payload->>'platform'),''),p_now_utc,p_now_utc
    ) returning * into v_new_session;

    with ranked as (
      select id,row_number() over(order by last_used_at_utc desc,issued_at_utc desc,id desc) rn
      from public.candidate_app_sessions where account_id=v_account.id and status='ACTIVE'
    )
    update public.candidate_app_sessions s set status='REVOKED',revoked_at_utc=p_now_utc,
      revoke_reason='CONCURRENT_SESSION_LIMIT',updated_at_utc=p_now_utc
    from ranked r where s.id=r.id and r.rn>5;

    update public.candidate_app_accounts set status='ACTIVE',failed_login_count=0,locked_until_utc=null,
      last_login_at_utc=p_now_utc,updated_at_utc=p_now_utc where id=v_account.id returning * into v_account;
    perform private._candidate_audit_v1('candidate_app_session',v_new_session.id::text,'CANDIDATE_LOGIN_SUCCESS',null,
      jsonb_build_object('account_id',v_account.id,'rotation',0,'selected_candidate_id',p_selected_candidate_id),
      null,null,p_idempotency_key,p_now_utc);
    return jsonb_build_object(
      'ok',true,'account_id',v_account.id,'session_id',v_new_session.id,'rotation',0,
      'selected_candidate_id',p_selected_candidate_id,'expires_at_utc',v_expires,
      'absolute_expires_at_utc',v_absolute,'session_version',v_account.session_version
    );
  end if;

  if v_action='REFRESH_SESSION' then
    select * into v_session from public.candidate_app_sessions where id=p_session_id for update;
    if not found or v_session.environment<>v_environment then raise exception 'CANDIDATE_SESSION_INVALID' using errcode='28000'; end if;
    if coalesce(v_payload->>'presented_refresh_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'new_refresh_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'new_session_id','') !~ '^[0-9a-fA-F-]{36}$' then
      raise exception 'CANDIDATE_REFRESH_HASH_INVALID' using errcode='22023';
    end if;
    v_presented_refresh_hash:=decode(v_payload->>'presented_refresh_token_hash_hex','hex');
    if v_session.status='ROTATED' then
      if v_presented_refresh_hash is distinct from v_session.refresh_token_hash then
        raise exception 'CANDIDATE_SESSION_INVALID' using errcode='28000';
      end if;
      update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
        revoke_reason='REFRESH_TOKEN_REUSE',updated_at_utc=p_now_utc
      where token_family_id=v_session.token_family_id and status in ('ACTIVE','ROTATED');
      return jsonb_build_object('ok',false,'error_code','CANDIDATE_REFRESH_TOKEN_REUSE','family_revoked',true);
    end if;
    if v_session.status<>'ACTIVE' or v_session.expires_at_utc<=p_now_utc or v_session.absolute_expires_at_utc<=p_now_utc then
      raise exception 'CANDIDATE_SESSION_EXPIRED' using errcode='28000';
    end if;
    if v_presented_refresh_hash is distinct from v_session.refresh_token_hash then
      raise exception 'CANDIDATE_SESSION_INVALID' using errcode='28000';
    end if;
    v_refresh_hash:=decode(v_payload->>'new_refresh_token_hash_hex','hex');
    if v_refresh_hash=v_presented_refresh_hash then
      raise exception 'CANDIDATE_REFRESH_HASH_REUSE_FORBIDDEN' using errcode='22023';
    end if;
    v_new_session_id:=(v_payload->>'new_session_id')::uuid;
    v_expires:=least(p_now_utc+interval '30 days',v_session.absolute_expires_at_utc);
    insert into public.candidate_app_sessions(
      id,account_id,environment,selected_candidate_id,status,refresh_token_hash,token_family_id,rotation,
      issued_at_utc,expires_at_utc,absolute_expires_at_utc,last_used_at_utc,device_id_hash,platform,
      push_provider,push_token_ciphertext,push_key_version,created_at_utc,updated_at_utc
    ) values (
      v_new_session_id,v_session.account_id,v_environment,v_session.selected_candidate_id,'ACTIVE',v_refresh_hash,
      v_session.token_family_id,v_session.rotation+1,p_now_utc,v_expires,v_session.absolute_expires_at_utc,p_now_utc,
      v_session.device_id_hash,v_session.platform,v_session.push_provider,v_session.push_token_ciphertext,
      v_session.push_key_version,p_now_utc,p_now_utc
    ) returning * into v_new_session;
    update public.candidate_app_sessions set status='ROTATED',replaced_by_session_id=v_new_session_id,
      last_used_at_utc=p_now_utc,updated_at_utc=p_now_utc where id=v_session.id;
    return jsonb_build_object('ok',true,'session_id',v_new_session.id,'rotation',v_new_session.rotation,
      'expires_at_utc',v_new_session.expires_at_utc,'absolute_expires_at_utc',v_new_session.absolute_expires_at_utc,
      'selected_candidate_id',v_new_session.selected_candidate_id);
  end if;

  if v_action in ('LOGOUT','SELECT_TEST_CANDIDATE','SET_NOTIFICATION_PREFERENCES','REGISTER_PUSH_TOKEN','CHANGE_PASSWORD') then
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    select * into v_session from public.candidate_app_sessions where id=p_session_id;
    select * into v_account from public.candidate_app_accounts where id=v_session.account_id for update;
  end if;

  if v_action='LOGOUT' then
    update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
      revoke_reason='LOGOUT',updated_at_utc=p_now_utc where id=v_session.id;
    return jsonb_build_object('ok',true,'session_id',v_session.id,'status','REVOKED');
  elsif v_action='SELECT_TEST_CANDIDATE' then
    if v_environment<>'TEST' then raise exception 'CANDIDATE_SELECTION_TEST_ONLY' using errcode='28000'; end if;
    if p_selected_candidate_id is null or not exists(
      select 1 from public.candidates c where c.id=p_selected_candidate_id and c.active=true
        and lower(btrim(coalesce(c.email,'')))=v_account.email_normalized
    ) then raise exception 'CANDIDATE_SELECTION_NOT_ALLOWED' using errcode='28000'; end if;
    update public.candidate_app_sessions set selected_candidate_id=p_selected_candidate_id,
      updated_at_utc=p_now_utc where id=v_session.id;
    return jsonb_build_object('ok',true,'session_id',v_session.id,'selected_candidate_id',p_selected_candidate_id);
  elsif v_action='SET_NOTIFICATION_PREFERENCES' then
    v_preferences:=coalesce(v_payload->'notification_preferences','{}'::jsonb);
    if jsonb_typeof(v_preferences)<>'object' then raise exception 'CANDIDATE_NOTIFICATION_PREFERENCES_INVALID' using errcode='22023'; end if;
    update public.candidate_app_accounts set notification_preferences_json=v_preferences,updated_at_utc=p_now_utc
    where id=v_account.id returning * into v_account;
    return jsonb_build_object('ok',true,'notification_preferences',v_account.notification_preferences_json);
  elsif v_action='REGISTER_PUSH_TOKEN' then
    if coalesce(v_payload->>'push_token_ciphertext_hex','') !~ '^[0-9a-fA-F]+$'
       or length(v_payload->>'push_token_ciphertext_hex')%2<>0
       or coalesce(v_payload->>'push_key_version','') !~ '^[1-9][0-9]{0,4}$' then
      raise exception 'CANDIDATE_PUSH_TOKEN_INVALID' using errcode='22023';
    end if;
    update public.candidate_app_sessions set
      push_provider=nullif(btrim(v_payload->>'push_provider'),''),
      push_token_ciphertext=decode(v_payload->>'push_token_ciphertext_hex','hex'),
      push_key_version=(v_payload->>'push_key_version')::smallint,updated_at_utc=p_now_utc
    where id=v_session.id;
    return jsonb_build_object('ok',true,'session_id',v_session.id,'push_registered',true);
  elsif v_action='CHANGE_PASSWORD' then
    update public.candidate_app_accounts set
      password_scheme=btrim(v_payload->>'password_scheme'),
      password_scheme_version=(v_payload->>'password_scheme_version')::smallint,
      password_salt=v_salt,password_digest=v_digest,
      password_params_json=coalesce(v_payload->'password_params','{}'::jsonb),
      password_changed_at_utc=p_now_utc,failed_login_count=0,locked_until_utc=null,
      session_version=session_version+1,updated_at_utc=p_now_utc
    where id=v_account.id returning * into v_account;
    update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
      revoke_reason='PASSWORD_CHANGED',updated_at_utc=p_now_utc
    where account_id=v_account.id and status='ACTIVE' and id<>v_session.id;
    return jsonb_build_object('ok',true,'account_id',v_account.id,'session_version',v_account.session_version);
  end if;

  if v_action='REVOKE_SESSIONS' then
    if v_account.id is null or v_account.environment<>v_environment then raise exception 'CANDIDATE_ACCOUNT_NOT_FOUND' using errcode='P0002'; end if;
    update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
      revoke_reason=coalesce(nullif(btrim(v_payload->>'reason'),''),'ACCOUNT_SESSION_REVOKE'),updated_at_utc=p_now_utc
    where account_id=v_account.id and status='ACTIVE';
    update public.candidate_app_accounts set session_version=session_version+1,updated_at_utc=p_now_utc
    where id=v_account.id returning * into v_account;
    return jsonb_build_object('ok',true,'account_id',v_account.id,'session_version',v_account.session_version);
  elsif v_action in ('LOCK','DISABLE') then
    if v_account.id is null or v_account.environment<>v_environment then raise exception 'CANDIDATE_ACCOUNT_NOT_FOUND' using errcode='P0002'; end if;
    update public.candidate_app_accounts set status=case when v_action='LOCK' then 'LOCKED' else 'DISABLED' end,
      locked_until_utc=case when v_action='LOCK' then coalesce(nullif(v_payload->>'locked_until_utc','')::timestamptz,p_now_utc+interval '15 minutes') else null end,
      session_version=session_version+1,updated_at_utc=p_now_utc where id=v_account.id returning * into v_account;
    update public.candidate_app_sessions set status='REVOKED',revoked_at_utc=p_now_utc,
      revoke_reason=v_action,updated_at_utc=p_now_utc where account_id=v_account.id and status='ACTIVE';
    return jsonb_build_object('ok',true,'account_id',v_account.id,'status',v_account.status);
  end if;

  raise exception 'CANDIDATE_AUTH_ACTION_INVALID'
    using errcode='22023',detail=jsonb_build_object('code','CANDIDATE_AUTH_ACTION_INVALID','action',v_action)::text;
end;
$function$;

create or replace function public.candidate_auth_challenge_transition_v1(
  p_action text,
  p_environment text,
  p_email_normalized text,
  p_purpose text,
  p_challenge_id uuid default null,
  p_token_hash bytea default null,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_environment text;
  v_email text:=private._candidate_normalize_email(p_email_normalized);
  v_purpose text:=upper(btrim(coalesce(p_purpose,'')));
  v_eligibility jsonb;
  v_account public.candidate_app_accounts%rowtype;
  v_challenge public.candidate_auth_challenges%rowtype;
  v_new public.candidate_auth_challenges%rowtype;
  v_allowed boolean:=false;
  v_expiry timestamptz;
  v_outbox_key text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_account_registration');
  if v_purpose not in ('ACTIVATE','RESET','RECOVERY') then raise exception 'CANDIDATE_CHALLENGE_PURPOSE_INVALID' using errcode='22023'; end if;
  if v_action in ('START','RESEND') and nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
  end if;
  if v_action in ('START','RESEND') then
    select * into v_new from public.candidate_auth_challenges
    where deterministic_outbox_key=btrim(p_idempotency_key);
    if found then
      if v_new.environment<>v_environment or v_new.email_normalized<>v_email or v_new.purpose<>v_purpose
         or (p_token_hash is not null and v_new.token_hash is distinct from p_token_hash) then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return jsonb_build_object(
        'ok',true,'accepted',true,'deliver_email',false,'idempotent_replay',true,
        'challenge_id',v_new.id,'expires_at_utc',v_new.expires_at_utc,
        'resend_count',v_new.resend_count,'deterministic_outbox_key',v_new.deterministic_outbox_key
      );
    end if;
  end if;

  if v_action='START' then
    perform pg_advisory_xact_lock(hashtext(v_environment||'|'||v_email||'|'||v_purpose));
    v_eligibility:=private._candidate_email_eligibility_v1(v_environment,v_email);
    select * into v_account from public.candidate_app_accounts
    where environment=v_environment and email_normalized=v_email;
    v_allowed:=coalesce((v_eligibility->>'eligible')::boolean,false)
      and ((v_purpose='ACTIVATE' and (v_account.id is null or v_account.status='SETUP_REQUIRED'))
        or (v_purpose in ('RESET','RECOVERY') and v_account.id is not null and v_account.status<>'DISABLED'));
    if not v_allowed then
      return jsonb_build_object('ok',true,'accepted',true,'deliver_email',false);
    end if;
    if p_token_hash is null or octet_length(p_token_hash)<>32 then raise exception 'CANDIDATE_CHALLENGE_TOKEN_HASH_INVALID' using errcode='22023'; end if;
    update public.candidate_auth_challenges set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where environment=v_environment and email_normalized=v_email and purpose=v_purpose and state in ('PENDING','VERIFIED');
    v_expiry:=p_now_utc+case when v_purpose='ACTIVATE' then interval '60 minutes' else interval '30 minutes' end;
    v_outbox_key:=btrim(p_idempotency_key);
    insert into public.candidate_auth_challenges(
      account_id,environment,email_normalized,purpose,state,token_hash,expires_at_utc,
      last_sent_at_utc,deterministic_outbox_key,created_at_utc,updated_at_utc
    ) values (
      v_account.id,v_environment,v_email,v_purpose,'PENDING',p_token_hash,v_expiry,
      p_now_utc,v_outbox_key,p_now_utc,p_now_utc
    ) returning * into v_new;
    return jsonb_build_object('ok',true,'accepted',true,'deliver_email',true,'idempotent_replay',false,
      'challenge_id',v_new.id,'expires_at_utc',v_new.expires_at_utc,
      'deterministic_outbox_key',v_new.deterministic_outbox_key);
  end if;

  if p_challenge_id is not null then
    select * into v_challenge from public.candidate_auth_challenges where id=p_challenge_id for update;
  elsif p_token_hash is not null then
    select * into v_challenge from public.candidate_auth_challenges where token_hash=p_token_hash for update;
  end if;
  if not found or v_challenge.environment<>v_environment or v_challenge.purpose<>v_purpose then
    raise exception 'CANDIDATE_CHALLENGE_INVALID' using errcode='28000';
  end if;

  if v_action='VERIFY' then
    if p_token_hash is null or p_token_hash is distinct from v_challenge.token_hash then
      update public.candidate_auth_challenges set
        attempt_count=least(attempt_count+1,5),
        state=case when attempt_count+1>=5 then 'SUPERSEDED' else state end,
        superseded_at_utc=case when attempt_count+1>=5 then p_now_utc else superseded_at_utc end,
        updated_at_utc=p_now_utc
      where id=v_challenge.id returning * into v_challenge;
      return jsonb_build_object(
        'ok',false,'error_code','CANDIDATE_CHALLENGE_INVALID',
        'terminal',v_challenge.state='SUPERSEDED','attempt_count',v_challenge.attempt_count
      );
    end if;
    if v_challenge.state<>'PENDING' then raise exception 'CANDIDATE_CHALLENGE_ALREADY_USED' using errcode='28000'; end if;
    if v_challenge.expires_at_utc<=p_now_utc then
      update public.candidate_auth_challenges set state='EXPIRED',updated_at_utc=p_now_utc where id=v_challenge.id;
      return jsonb_build_object('ok',false,'error_code','CANDIDATE_CHALLENGE_EXPIRED','terminal',true);
    end if;
    update public.candidate_auth_challenges set state='VERIFIED',verified_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_challenge.id returning * into v_challenge;
    return jsonb_build_object('ok',true,'challenge_id',v_challenge.id,'state',v_challenge.state,
      'purpose',v_challenge.purpose,'expires_at_utc',v_challenge.expires_at_utc);
  elsif v_action='CONSUME' then
    if v_challenge.state<>'VERIFIED' or v_challenge.expires_at_utc<=p_now_utc then
      raise exception 'CANDIDATE_VERIFIED_CHALLENGE_INVALID' using errcode='28000';
    end if;
    return jsonb_build_object('ok',true,'challenge_id',v_challenge.id,'state','VERIFIED',
      'consume_via_account_transition',true);
  elsif v_action='RESEND' then
    if v_challenge.state not in ('PENDING','VERIFIED') or v_challenge.expires_at_utc<=p_now_utc then
      raise exception 'CANDIDATE_CHALLENGE_NOT_RESENDABLE' using errcode='55000';
    end if;
    if v_challenge.resend_count>=5 then raise exception 'CANDIDATE_CHALLENGE_RESEND_LIMIT' using errcode='55000'; end if;
    if v_challenge.last_sent_at_utc is not null and v_challenge.last_sent_at_utc+interval '60 seconds'>p_now_utc then
      raise exception 'CANDIDATE_CHALLENGE_RESEND_TOO_SOON' using errcode='55000';
    end if;
    if p_token_hash is null or octet_length(p_token_hash)<>32 then raise exception 'CANDIDATE_CHALLENGE_TOKEN_HASH_INVALID' using errcode='22023'; end if;
    update public.candidate_auth_challenges set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_challenge.id;
    v_expiry:=p_now_utc+case when v_purpose='ACTIVATE' then interval '60 minutes' else interval '30 minutes' end;
    v_outbox_key:=btrim(p_idempotency_key);
    insert into public.candidate_auth_challenges(
      account_id,environment,email_normalized,purpose,state,token_hash,expires_at_utc,
      resend_count,last_sent_at_utc,superseded_by_id,deterministic_outbox_key,created_at_utc,updated_at_utc
    ) values (
      v_challenge.account_id,v_environment,v_challenge.email_normalized,v_purpose,'PENDING',p_token_hash,v_expiry,
      v_challenge.resend_count+1,p_now_utc,null,v_outbox_key,p_now_utc,p_now_utc
    ) returning * into v_new;
    update public.candidate_auth_challenges set superseded_by_id=v_new.id where id=v_challenge.id;
    return jsonb_build_object('ok',true,'accepted',true,'deliver_email',true,'idempotent_replay',false,
      'challenge_id',v_new.id,'expires_at_utc',v_new.expires_at_utc,'resend_count',v_new.resend_count,
      'deterministic_outbox_key',v_new.deterministic_outbox_key);
  elsif v_action in ('EXPIRE','SUPERSEDE') then
    update public.candidate_auth_challenges set
      state=case when v_action='EXPIRE' then 'EXPIRED' else 'SUPERSEDED' end,
      superseded_at_utc=case when v_action='SUPERSEDE' then p_now_utc else superseded_at_utc end,
      updated_at_utc=p_now_utc where id=v_challenge.id returning * into v_challenge;
    return jsonb_build_object('ok',true,'challenge_id',v_challenge.id,'state',v_challenge.state);
  end if;
  raise exception 'CANDIDATE_CHALLENGE_ACTION_INVALID' using errcode='22023';
end;
$function$;

comment on function public.candidate_auth_account_transition_v1(text,text,uuid,text,uuid,uuid,jsonb,text,timestamptz) is
  'Service-role-only clean Candidate App account/session transition authority. Accepts hashes/verifiers only; no Google or plaintext password path.';
comment on function public.candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamptz) is
  'Service-role-only single-use activation/reset/recovery challenge state authority. Public enumeration masking remains a backend responsibility.';

revoke all on function public.candidate_auth_account_transition_v1(text,text,uuid,text,uuid,uuid,jsonb,text,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_auth_account_transition_v1(text,text,uuid,text,uuid,uuid,jsonb,text,timestamptz) to service_role;
grant execute on function public.candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamptz) to service_role;
