-- Candidate account/session invalidation concurrency authority.
--
-- Every race below executes the real Candidate account-transition RPC in two
-- independent PostgreSQL transactions.  The first operation remains open for
-- 750 ms after its mutation so that the second request is forced to overlap
-- and wait on the account-scoped transaction advisory lock.  Both operation
-- orders are covered for refresh-token reuse, password change/reset,
-- REVOKE_SESSIONS, LOCK and DISABLE.

create extension if not exists dblink;

drop table if exists public._candidate_session_lock_restore_13082026;
create table public._candidate_session_lock_restore_13082026(
  id integer primary key,
  feature_flags jsonb not null
);

begin;
insert into public._candidate_session_lock_restore_13082026(id,feature_flags)
select id,candidate_app_feature_flags_json
from public.settings_defaults
where id=1;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;
commit;

create or replace function private._candidate_session_lock_seed_13082026(
  p_account_id uuid,
  p_candidate_id uuid,
  p_email text,
  p_session_ids uuid[]
)
returns void
language plpgsql
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_session_id uuid;
  v_result jsonb;
begin
  insert into public.candidates(id,email,active,key_norm)
  values(p_candidate_id,p_email,true,upper(replace(p_account_id::text,'-','')));

  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,
    password_scheme_version,password_salt,password_digest,
    password_params_json,password_changed_at_utc,created_at_utc,updated_at_utc
  ) values (
    p_account_id,'TEST',p_email,'ACTIVE','ARGON2ID',1,
    decode(repeat('11',16),'hex'),decode(repeat('22',32),'hex'),
    '{}'::jsonb,clock_timestamp()-interval '1 day',clock_timestamp(),clock_timestamp()
  );

  foreach v_session_id in array p_session_ids loop
    v_result:=public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',p_account_id,p_email,v_session_id,p_candidate_id,
      jsonb_build_object(
        'refresh_token_hash_hex',encode(extensions.digest(
          convert_to('refresh|'||v_session_id::text,'UTF8'),'sha256'
        ),'hex'),
        'expires_at_utc',clock_timestamp()+interval '30 days',
        'absolute_expires_at_utc',clock_timestamp()+interval '90 days',
        'idempotency_request_sha256',encode(extensions.digest(
          convert_to('login|'||v_session_id::text,'UTF8'),'sha256'
        ),'hex'),
        'idempotency_key_version',1
      ),
      'candidate-session-lock-seed-'||v_session_id::text,clock_timestamp()
    );
    if not coalesce((v_result->>'ok')::boolean,false)
       or v_result->>'session_id'<>v_session_id::text then
      raise exception 'Candidate concurrency seed login failed: %',v_result;
    end if;
  end loop;
end;
$function$;

create or replace function private._candidate_session_lock_call_13082026(
  p_action text,
  p_account_id uuid,
  p_session_id uuid,
  p_new_session_id uuid,
  p_idempotency_key text
)
returns jsonb
language plpgsql
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_presented_hash text;
begin
  if p_action='REFRESH_SESSION' then
    select encode(s.refresh_token_hash,'hex') into v_presented_hash
    from public.candidate_app_sessions s where s.id=p_session_id;
    return public.candidate_auth_account_transition_v1(
      'REFRESH_SESSION','TEST',null,null,p_session_id,null,
      jsonb_build_object(
        'presented_refresh_token_hash_hex',v_presented_hash,
        'new_refresh_token_hash_hex',encode(extensions.digest(
          convert_to('refresh|'||p_new_session_id::text,'UTF8'),'sha256'
        ),'hex'),
        'new_session_id',p_new_session_id,
        'idempotency_request_sha256',encode(extensions.digest(
          convert_to('refresh-request|'||p_idempotency_key,'UTF8'),'sha256'
        ),'hex'),
        'idempotency_key_version',1
      ),p_idempotency_key,clock_timestamp()
    );
  elsif p_action='LOGIN_SUCCESS' then
    return public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',p_account_id,
      (select a.email_normalized from public.candidate_app_accounts a where a.id=p_account_id),
      p_new_session_id,
      (select c.id from public.candidates c
       join public.candidate_app_accounts a
         on lower(btrim(c.email))=a.email_normalized
       where a.id=p_account_id and c.active=true order by c.id limit 1),
      jsonb_build_object(
        'refresh_token_hash_hex',encode(extensions.digest(
          convert_to('refresh|'||p_new_session_id::text,'UTF8'),'sha256'
        ),'hex'),
        'expires_at_utc',clock_timestamp()+interval '30 days',
        'absolute_expires_at_utc',clock_timestamp()+interval '90 days',
        'idempotency_request_sha256',encode(extensions.digest(
          convert_to('login-request|'||p_idempotency_key,'UTF8'),'sha256'
        ),'hex'),
        'idempotency_key_version',1
      ),p_idempotency_key,clock_timestamp()
    );
  elsif p_action='CHANGE_PASSWORD' then
    return public.candidate_auth_account_transition_v1(
      'CHANGE_PASSWORD','TEST',null,null,p_session_id,null,
      jsonb_build_object(
        'password_scheme','ARGON2ID','password_scheme_version',1,
        'password_salt_hex',repeat('33',16),
        'password_digest_hex',repeat('44',32),
        'password_params',jsonb_build_object('memory_kib',65536),
        'idempotency_request_sha256',encode(extensions.digest(
          convert_to('password-request|'||p_idempotency_key,'UTF8'),'sha256'
        ),'hex'),
        'idempotency_key_version',1
      ),p_idempotency_key,clock_timestamp()
    );
  elsif p_action in ('REVOKE_SESSIONS','LOCK','DISABLE') then
    return public.candidate_auth_account_transition_v1(
      p_action,'TEST',p_account_id,null,null,null,
      case when p_action='REVOKE_SESSIONS'
        then jsonb_build_object('reason','CONCURRENCY_VERIFICATION')
        when p_action='LOCK'
        then jsonb_build_object('locked_until_utc',clock_timestamp()+interval '15 minutes')
        else '{}'::jsonb end,
      null,clock_timestamp()
    );
  end if;
  raise exception 'unsupported Candidate concurrency action %',p_action;
exception when others then
  return jsonb_build_object(
    'ok',false,'caught',true,'sqlstate',sqlstate,'error_code',sqlerrm
  );
end;
$function$;

create or replace function private._candidate_session_lock_reset_13082026(
  p_account_id uuid,
  p_candidate_id uuid,
  p_challenge_id uuid,
  p_new_session_id uuid,
  p_idempotency_key text
)
returns jsonb
language plpgsql
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_email text;
begin
  select email_normalized into v_email
  from public.candidate_app_accounts where id=p_account_id;
  return public.candidate_auth_account_transition_v1(
    'ACTIVATE_PASSWORD','TEST',p_account_id,v_email,p_new_session_id,p_candidate_id,
    jsonb_build_object(
      'challenge_id',p_challenge_id,
      'password_scheme','ARGON2ID','password_scheme_version',1,
      'password_salt_hex',repeat('55',16),
      'password_digest_hex',repeat('66',32),
      'password_params',jsonb_build_object('memory_kib',65536),
      'refresh_token_hash_hex',encode(extensions.digest(
        convert_to('refresh|'||p_new_session_id::text,'UTF8'),'sha256'
      ),'hex'),
      'expires_at_utc',clock_timestamp()+interval '30 days',
      'absolute_expires_at_utc',clock_timestamp()+interval '90 days',
      'idempotency_request_sha256',encode(extensions.digest(
        convert_to('reset-request|'||p_idempotency_key,'UTF8'),'sha256'
      ),'hex'),
      'idempotency_key_version',1
    ),p_idempotency_key,clock_timestamp()
  );
exception when others then
  return jsonb_build_object(
    'ok',false,'caught',true,'sqlstate',sqlstate,'error_code',sqlerrm
  );
end;
$function$;

create or replace function private._candidate_session_lock_race_13082026(
  p_first_query text,
  p_second_query text
)
returns jsonb
language plpgsql
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_connection text:='host=127.0.0.1 port='||coalesce(inet_server_port(),5432)::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
begin
  perform dblink_connect('candidate_session_lock_first',v_connection);
  perform dblink_connect('candidate_session_lock_second',v_connection);
  perform dblink_send_query('candidate_session_lock_first',
    'with operated as materialized ('||p_first_query||'), '
    ||'held as materialized ('
    ||'select pg_sleep(0.75),operated.result from operated) '
    ||'select held.result::text from held'
  );
  perform pg_sleep(0.10);
  perform dblink_send_query('candidate_session_lock_second',p_second_query);
  select result::jsonb into v_first
  from dblink_get_result('candidate_session_lock_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_session_lock_second') as received(result text);
  perform dblink_disconnect('candidate_session_lock_first');
  perform dblink_disconnect('candidate_session_lock_second');
  return jsonb_build_object('first',v_first,'second',v_second);
end;
$function$;

-- Commit every race precondition before the dblink transactions begin.  The
-- seed path uses the real LOGIN_SUCCESS/REFRESH_SESSION authorities; only the
-- verified reset challenge itself is fixture data.
do $seed_all_races$
declare
  v_order integer;
  v_action_no integer;
  v_action text;
  v_account uuid;
  v_candidate uuid;
  v_old uuid;
  v_next uuid;
  v_challenge uuid;
begin
  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0001-4000-8000-000000000001'::uuid
      else 'aa130813-0001-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0001-4000-8000-000000000001'::uuid
      else 'ca130813-0001-4000-8000-000000000002'::uuid end;
    v_old:=case v_order when 1 then '5a130813-0001-4000-8000-000000000001'::uuid
      else '5a130813-0001-4000-8000-000000000004'::uuid end;
    v_next:=case v_order when 1 then '5a130813-0001-4000-8000-000000000002'::uuid
      else '5a130813-0001-4000-8000-000000000005'::uuid end;
    perform private._candidate_session_lock_seed_13082026(
      v_account,v_candidate,'session-reuse-'||v_order||'@example.test',array[v_old]
    );
    perform private._candidate_session_lock_call_13082026(
      'REFRESH_SESSION',v_account,v_old,v_next,'session-lock-seed-refresh-'||v_order
    );

    v_account:=case v_order when 1 then 'aa130813-0002-4000-8000-000000000001'::uuid
      else 'aa130813-0002-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0002-4000-8000-000000000001'::uuid
      else 'ca130813-0002-4000-8000-000000000002'::uuid end;
    perform private._candidate_session_lock_seed_13082026(
      v_account,v_candidate,'session-password-'||v_order||'@example.test',
      case v_order when 1 then array[
        '5a130813-0002-4000-8000-000000000001'::uuid,
        '5a130813-0002-4000-8000-000000000002'::uuid
      ] else array[
        '5a130813-0002-4000-8000-000000000004'::uuid,
        '5a130813-0002-4000-8000-000000000005'::uuid
      ] end
    );

    v_account:=case v_order when 1 then 'aa130813-0003-4000-8000-000000000001'::uuid
      else 'aa130813-0003-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0003-4000-8000-000000000001'::uuid
      else 'ca130813-0003-4000-8000-000000000002'::uuid end;
    v_old:=case v_order when 1 then '5a130813-0003-4000-8000-000000000001'::uuid
      else '5a130813-0003-4000-8000-000000000004'::uuid end;
    v_challenge:=case v_order when 1 then 'cc130813-0003-4000-8000-000000000001'::uuid
      else 'cc130813-0003-4000-8000-000000000002'::uuid end;
    perform private._candidate_session_lock_seed_13082026(
      v_account,v_candidate,'session-reset-'||v_order||'@example.test',array[v_old]
    );
    insert into public.candidate_auth_challenges(
      id,account_id,environment,email_normalized,purpose,state,token_hash,
      expires_at_utc,verified_at_utc,deterministic_outbox_key,created_at_utc,updated_at_utc
    ) select v_challenge,v_account,'TEST',a.email_normalized,'RESET','VERIFIED',
      extensions.digest(convert_to('challenge|'||v_challenge::text,'UTF8'),'sha256'),
      clock_timestamp()+interval '30 minutes',clock_timestamp(),
      'candidate-session-lock-reset-'||v_order,clock_timestamp(),clock_timestamp()
    from public.candidate_app_accounts a where a.id=v_account;
  end loop;

  v_action_no:=0;
  foreach v_action in array array['REVOKE_SESSIONS','LOCK','DISABLE'] loop
    v_action_no:=v_action_no+1;
    for v_order in 1..2 loop
      v_account:=format('aa130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      v_candidate:=format('ca130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      v_old:=format('5a130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      perform private._candidate_session_lock_seed_13082026(
        v_account,v_candidate,
        'session-'||lower(v_action)||'-'||v_order||'@example.test',array[v_old]
      );
    end loop;
  end loop;

  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0005-4000-8000-000000000001'::uuid
      else 'aa130813-0005-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0005-4000-8000-000000000001'::uuid
      else 'ca130813-0005-4000-8000-000000000002'::uuid end;
    v_old:=case v_order when 1 then '5a130813-0005-4000-8000-000000000001'::uuid
      else '5a130813-0005-4000-8000-000000000003'::uuid end;
    perform private._candidate_session_lock_seed_13082026(
      v_account,v_candidate,'session-login-disable-'||v_order||'@example.test',array[v_old]
    );
  end loop;
end;
$seed_all_races$;

-- Refresh-token reuse versus a legitimate current-session refresh.  Cover
-- both winners.  In either serial order no ACTIVE/ROTATED row may remain in
-- the compromised token family.
do $reuse_refresh_both_orders$
declare
  v_account uuid;
  v_candidate uuid;
  v_s1 uuid;
  v_s2 uuid;
  v_s3 uuid;
  v_result jsonb;
  v_order integer;
begin
  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0001-4000-8000-000000000001'::uuid
      else 'aa130813-0001-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0001-4000-8000-000000000001'::uuid
      else 'ca130813-0001-4000-8000-000000000002'::uuid end;
    v_s1:=case v_order when 1 then '5a130813-0001-4000-8000-000000000001'::uuid
      else '5a130813-0001-4000-8000-000000000004'::uuid end;
    v_s2:=case v_order when 1 then '5a130813-0001-4000-8000-000000000002'::uuid
      else '5a130813-0001-4000-8000-000000000005'::uuid end;
    v_s3:=case v_order when 1 then '5a130813-0001-4000-8000-000000000003'::uuid
      else '5a130813-0001-4000-8000-000000000006'::uuid end;
    if v_order=1 then
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L) result',
          'REFRESH_SESSION',v_account,v_s2,v_s3,'session-lock-current-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L)::text',
          'REFRESH_SESSION',v_account,v_s1,'00000000-0000-4000-8000-000000000001'::uuid,'session-lock-reuse-second')
      );
    else
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L) result',
          'REFRESH_SESSION',v_account,v_s1,'00000000-0000-4000-8000-000000000002'::uuid,'session-lock-reuse-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L)::text',
          'REFRESH_SESSION',v_account,v_s2,v_s3,'session-lock-current-second')
      );
    end if;

    if exists(
      select 1 from public.candidate_app_sessions s
      where s.account_id=v_account and s.status in ('ACTIVE','ROTATED')
    ) or not (
      v_result#>>'{first,error_code}'='CANDIDATE_REFRESH_TOKEN_REUSE'
      or v_result#>>'{second,error_code}'='CANDIDATE_REFRESH_TOKEN_REUSE'
    ) then
      raise exception 'refresh reuse/current race left a live family in order %: %',v_order,v_result;
    end if;
  end loop;
end;
$reuse_refresh_both_orders$;

-- Authenticated password change on S1 versus refresh on independent S2.
-- Regardless of ordering, S1 is the sole permitted active session.
do $password_change_refresh_both_orders$
declare
  v_account uuid;
  v_candidate uuid;
  v_s1 uuid;
  v_s2 uuid;
  v_s3 uuid;
  v_result jsonb;
  v_order integer;
begin
  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0002-4000-8000-000000000001'::uuid
      else 'aa130813-0002-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0002-4000-8000-000000000001'::uuid
      else 'ca130813-0002-4000-8000-000000000002'::uuid end;
    v_s1:=case v_order when 1 then '5a130813-0002-4000-8000-000000000001'::uuid
      else '5a130813-0002-4000-8000-000000000004'::uuid end;
    v_s2:=case v_order when 1 then '5a130813-0002-4000-8000-000000000002'::uuid
      else '5a130813-0002-4000-8000-000000000005'::uuid end;
    v_s3:=case v_order when 1 then '5a130813-0002-4000-8000-000000000003'::uuid
      else '5a130813-0002-4000-8000-000000000006'::uuid end;
    if v_order=1 then
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L) result',
          'REFRESH_SESSION',v_account,v_s2,v_s3,'session-lock-password-refresh-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,null,%L)::text',
          'CHANGE_PASSWORD',v_account,v_s1,'session-lock-password-change-second')
      );
    else
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,null,%L) result',
          'CHANGE_PASSWORD',v_account,v_s1,'session-lock-password-change-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L)::text',
          'REFRESH_SESSION',v_account,v_s2,v_s3,'session-lock-password-refresh-second')
      );
    end if;
    if (select count(*) from public.candidate_app_sessions
        where account_id=v_account and status='ACTIVE')<>1
       or not exists(select 1 from public.candidate_app_sessions where id=v_s1 and status='ACTIVE')
       or exists(select 1 from public.candidate_app_sessions where id in (v_s2,v_s3) and status='ACTIVE') then
      raise exception 'password-change/refresh race did not preserve only S1 in order %: %',v_order,v_result;
    end if;
  end loop;
end;
$password_change_refresh_both_orders$;

-- Password reset versus an old-session refresh.  The reset-created session is
-- the sole permitted active session in either serial order.
do $password_reset_refresh_both_orders$
declare
  v_account uuid;
  v_candidate uuid;
  v_old uuid;
  v_refresh_new uuid;
  v_reset_new uuid;
  v_challenge uuid;
  v_result jsonb;
  v_order integer;
begin
  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0003-4000-8000-000000000001'::uuid
      else 'aa130813-0003-4000-8000-000000000002'::uuid end;
    v_candidate:=case v_order when 1 then 'ca130813-0003-4000-8000-000000000001'::uuid
      else 'ca130813-0003-4000-8000-000000000002'::uuid end;
    v_old:=case v_order when 1 then '5a130813-0003-4000-8000-000000000001'::uuid
      else '5a130813-0003-4000-8000-000000000004'::uuid end;
    v_refresh_new:=case v_order when 1 then '5a130813-0003-4000-8000-000000000002'::uuid
      else '5a130813-0003-4000-8000-000000000005'::uuid end;
    v_reset_new:=case v_order when 1 then '5a130813-0003-4000-8000-000000000003'::uuid
      else '5a130813-0003-4000-8000-000000000006'::uuid end;
    v_challenge:=case v_order when 1 then 'cc130813-0003-4000-8000-000000000001'::uuid
      else 'cc130813-0003-4000-8000-000000000002'::uuid end;
    if v_order=1 then
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L) result',
          'REFRESH_SESSION',v_account,v_old,v_refresh_new,'session-lock-reset-refresh-first'),
        format('select private._candidate_session_lock_reset_13082026(%L::uuid,%L::uuid,%L::uuid,%L::uuid,%L)::text',
          v_account,v_candidate,v_challenge,v_reset_new,'session-lock-reset-second')
      );
    else
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_reset_13082026(%L::uuid,%L::uuid,%L::uuid,%L::uuid,%L) result',
          v_account,v_candidate,v_challenge,v_reset_new,'session-lock-reset-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L)::text',
          'REFRESH_SESSION',v_account,v_old,v_refresh_new,'session-lock-reset-refresh-second')
      );
    end if;
    if (select count(*) from public.candidate_app_sessions
        where account_id=v_account and status='ACTIVE')<>1
       or not exists(select 1 from public.candidate_app_sessions where id=v_reset_new and status='ACTIVE')
       or exists(select 1 from public.candidate_app_sessions where id in (v_old,v_refresh_new) and status='ACTIVE') then
      raise exception 'password-reset/refresh race did not preserve only reset session in order %: %',v_order,v_result;
    end if;
  end loop;
end;
$password_reset_refresh_both_orders$;

-- Account-wide invalidations versus refresh.  Each action is exercised with
-- the invalidation first and the refresh first.  No ACTIVE session may remain.
do $administrative_invalidation_refresh_both_orders$
declare
  v_action text;
  v_action_no integer;
  v_order integer;
  v_account uuid;
  v_candidate uuid;
  v_old uuid;
  v_new uuid;
  v_result jsonb;
begin
  v_action_no:=0;
  foreach v_action in array array['REVOKE_SESSIONS','LOCK','DISABLE'] loop
    v_action_no:=v_action_no+1;
    for v_order in 1..2 loop
      v_account:=format('aa130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      v_candidate:=format('ca130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      v_old:=format('5a130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      v_new:=format('5b130813-0004-4000-8%s-%s',
        lpad(v_action_no::text,3,'0'),lpad(v_order::text,12,'0'))::uuid;
      if v_order=1 then
        v_result:=private._candidate_session_lock_race_13082026(
          format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L) result',
            'REFRESH_SESSION',v_account,v_old,v_new,'session-lock-'||lower(v_action)||'-refresh-first'),
          format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,null,null)::text',
            v_action,v_account)
        );
      else
        v_result:=private._candidate_session_lock_race_13082026(
          format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,null,null) result',
            v_action,v_account),
          format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,%L::uuid,%L::uuid,%L)::text',
            'REFRESH_SESSION',v_account,v_old,v_new,'session-lock-'||lower(v_action)||'-refresh-second')
        );
      end if;
      if exists(
        select 1 from public.candidate_app_sessions
        where account_id=v_account and status='ACTIVE'
      ) then
        raise exception '%/refresh race left an active session in order %: %',v_action,v_order,v_result;
      end if;
      if v_action in ('LOCK','DISABLE') and not exists(
        select 1 from public.candidate_app_accounts
        where id=v_account
          and status=case when v_action='LOCK' then 'LOCKED' else 'DISABLED' end
      ) then
        raise exception '%/refresh race did not retain account state in order %: %',v_action,v_order,v_result;
      end if;
    end loop;
  end loop;
end;
$administrative_invalidation_refresh_both_orders$;

-- LOGIN_SUCCESS is the other session-creation owner.  During a disable race,
-- either the new login commits first and is then revoked, or disable commits
-- first and the waiting login is refused.  It must never revive the account.
do $login_disable_both_orders$
declare
  v_order integer;
  v_account uuid;
  v_new uuid;
  v_result jsonb;
begin
  for v_order in 1..2 loop
    v_account:=case v_order when 1 then 'aa130813-0005-4000-8000-000000000001'::uuid
      else 'aa130813-0005-4000-8000-000000000002'::uuid end;
    v_new:=case v_order when 1 then '5a130813-0005-4000-8000-000000000002'::uuid
      else '5a130813-0005-4000-8000-000000000004'::uuid end;
    if v_order=1 then
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,%L::uuid,%L) result',
          'LOGIN_SUCCESS',v_account,v_new,'session-lock-login-first'),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,null,null)::text',
          'DISABLE',v_account)
      );
    else
      v_result:=private._candidate_session_lock_race_13082026(
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,null,null) result',
          'DISABLE',v_account),
        format('select private._candidate_session_lock_call_13082026(%L,%L::uuid,null,%L::uuid,%L)::text',
          'LOGIN_SUCCESS',v_account,v_new,'session-lock-login-second')
      );
    end if;
    if exists(select 1 from public.candidate_app_sessions where account_id=v_account and status='ACTIVE')
       or not exists(select 1 from public.candidate_app_accounts where id=v_account and status='DISABLED') then
      raise exception 'login/disable race revived a disabled account in order %: %',v_order,v_result;
    end if;
  end loop;
end;
$login_disable_both_orders$;

-- Contract/ACL checks for the new private boundary and the unchanged public
-- Candidate RPC inventory.
do $lock_contract$
declare
  v_proc oid;
begin
  select p.oid into v_proc
  from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private'
    and p.proname='_candidate_auth_account_session_lock_v1'
    and pg_get_function_identity_arguments(p.oid)='p_environment text, p_account_id uuid';
  if v_proc is null
     or has_function_privilege('anon',v_proc,'EXECUTE')
     or has_function_privilege('authenticated',v_proc,'EXECUTE')
     or has_function_privilege('service_role',v_proc,'EXECUTE') then
    raise exception 'Candidate account/session lock helper ACL/signature is invalid';
  end if;
end;
$lock_contract$;

begin;
delete from public.audit_events
where object_type in ('candidate_auth_mutation_receipt','candidate_app_session','candidate_app_account')
  and (
    correlation_id like 'candidate-session-lock-%'
    or correlation_id like 'session-lock-%'
  );
delete from public.candidate_notifications
where account_id in (select id from public.candidate_app_accounts where email_normalized like 'session-%@example.test');
delete from public.candidate_auth_challenges
where account_id in (select id from public.candidate_app_accounts where email_normalized like 'session-%@example.test');
delete from public.candidate_app_sessions
where account_id in (select id from public.candidate_app_accounts where email_normalized like 'session-%@example.test');
delete from public.candidate_app_accounts where email_normalized like 'session-%@example.test';
delete from public.candidates where email like 'session-%@example.test';
update public.settings_defaults sd
set candidate_app_feature_flags_json=restore.feature_flags
from public._candidate_session_lock_restore_13082026 restore
where sd.id=restore.id;
drop function private._candidate_session_lock_race_13082026(text,text);
drop function private._candidate_session_lock_reset_13082026(uuid,uuid,uuid,uuid,text);
drop function private._candidate_session_lock_call_13082026(text,uuid,uuid,uuid,text);
drop function private._candidate_session_lock_seed_13082026(uuid,uuid,text,uuid[]);
drop table public._candidate_session_lock_restore_13082026;
commit;
