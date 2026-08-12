-- Candidate public-auth final correction: two-session proof for exact resend
-- throttles, unknown-account login failures and conflicting action reuse.
-- Fixtures are committed only so dblink sessions can exercise the real
-- advisory-lock/receipt ordering, then are removed before the suite ends.

create extension if not exists dblink;

drop table if exists public._candidate_auth_concurrency_restore_12082026;
create table public._candidate_auth_concurrency_restore_12082026(
  id integer primary key,
  feature_flags jsonb not null
);

begin;
insert into public._candidate_auth_concurrency_restore_12082026(id,feature_flags)
select id,candidate_app_feature_flags_json
from public.settings_defaults
where id=1;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;

insert into public.candidates(id,email,active,key_norm)
values(
  'ca120812-2223-4000-8000-000000000001',
  'auth-concurrent@example.test',true,'AUTH-CONCURRENT'
);

do $seed_challenge$
declare
  v_result jsonb;
begin
  v_result:=public.candidate_auth_challenge_transition_v1(
    'START','TEST','auth-concurrent@example.test','ACTIVATE',null,
    decode(repeat('91',32),'hex'),'auth-concurrent-start-v1',
    date_trunc('second',clock_timestamp()),2
  );
  if not coalesce((v_result->>'deliver_email')::boolean,false) then
    raise exception 'concurrency challenge seed was not deliverable: %',v_result;
  end if;
end;
$seed_challenge$;
commit;

do $same_key_throttle_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
begin
  perform dblink_connect('candidate_auth_throttle_first',v_connection);
  perform dblink_connect('candidate_auth_throttle_second',v_connection);
  perform dblink_send_query('candidate_auth_throttle_first',$query$
    with throttled as materialized (
      select public.candidate_auth_challenge_transition_v1(
        'RESEND','TEST','auth-concurrent@example.test','ACTIVATE',
        (select id from public.candidate_auth_challenges
         where deterministic_outbox_key='auth-concurrent-start-v1'),
        decode(repeat('92',32),'hex'),'auth-concurrent-throttle-v1',
        (select last_sent_at_utc+interval '10 seconds'
         from public.candidate_auth_challenges
         where deterministic_outbox_key='auth-concurrent-start-v1'),2
      ) result
    ), held as materialized (select pg_sleep(1))
    select throttled.result::text from throttled cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_auth_throttle_second',$query$
    select public.candidate_auth_challenge_transition_v1(
      'RESEND','TEST','auth-concurrent@example.test','ACTIVATE',
      (select id from public.candidate_auth_challenges
       where deterministic_outbox_key='auth-concurrent-start-v1'),
      decode(repeat('92',32),'hex'),'auth-concurrent-throttle-v1',
      (select last_sent_at_utc+interval '10 seconds'
       from public.candidate_auth_challenges
       where deterministic_outbox_key='auth-concurrent-start-v1'),2
    )::text
  $query$);
  select result::jsonb into v_first
  from dblink_get_result('candidate_auth_throttle_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_auth_throttle_second') as received(result text);
  perform dblink_disconnect('candidate_auth_throttle_first');
  perform dblink_disconnect('candidate_auth_throttle_second');

  if v_first->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_TOO_SOON'
     or v_second->>'error_code'<>'CANDIDATE_CHALLENGE_RESEND_TOO_SOON'
     or (v_first->>'retry_after_seconds')::integer<>50
     or (v_second->>'retry_after_seconds')::integer<>50
     or not coalesce((v_second->>'idempotent_replay')::boolean,false)
     or (select count(*) from public.audit_events
         where object_type='candidate_auth_mutation_receipt'
           and object_id_text='TEST'
           and correlation_id='auth-concurrent-throttle-v1')<>1
     or (select count(*) from public.candidate_auth_challenges
         where email_normalized='auth-concurrent@example.test')<>1 then
    raise exception 'concurrent throttle did not return one durable result: %, %',v_first,v_second;
  end if;
end;
$same_key_throttle_race$;

do $same_key_unknown_login_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
begin
  perform dblink_connect('candidate_auth_unknown_first',v_connection);
  perform dblink_connect('candidate_auth_unknown_second',v_connection);
  perform dblink_send_query('candidate_auth_unknown_first',$query$
    with failed as materialized (
      select public.candidate_auth_account_transition_v1(
        'LOGIN_SUCCESS','TEST',null,'unknown-auth-concurrent@example.test',null,null,
        jsonb_build_object(
          'login_failed',true,
          'idempotency_request_sha256',repeat('93',32),
          'idempotency_key_version',1
        ),'auth-concurrent-unknown-login-v1',clock_timestamp()
      ) result
    ), held as materialized (select pg_sleep(1))
    select failed.result::text from failed cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_auth_unknown_second',$query$
    select public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',null,'unknown-auth-concurrent@example.test',null,null,
      jsonb_build_object(
        'login_failed',true,
        'idempotency_request_sha256',repeat('93',32),
        'idempotency_key_version',1
      ),'auth-concurrent-unknown-login-v1',clock_timestamp()
    )::text
  $query$);
  select result::jsonb into v_first
  from dblink_get_result('candidate_auth_unknown_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_auth_unknown_second') as received(result text);
  perform dblink_disconnect('candidate_auth_unknown_first');
  perform dblink_disconnect('candidate_auth_unknown_second');

  if v_first->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or v_second->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or coalesce((v_first->>'failed_login_recorded')::boolean,true)
     or coalesce((v_second->>'failed_login_recorded')::boolean,true)
     or not coalesce((v_second->>'idempotent_replay')::boolean,false)
     or (select count(*) from public.audit_events
         where object_type='candidate_auth_mutation_receipt'
           and object_id_text='TEST'
           and correlation_id='auth-concurrent-unknown-login-v1')<>1
     or exists (
       select 1 from public.candidate_app_accounts
       where email_normalized='unknown-auth-concurrent@example.test'
     ) then
    raise exception 'concurrent unknown login did not return one generic receipt: %, %',v_first,v_second;
  end if;
end;
$same_key_unknown_login_race$;

do $different_action_same_key_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second_conflict boolean:=false;
begin
  perform dblink_connect('candidate_auth_action_first',v_connection);
  perform dblink_connect('candidate_auth_action_second',v_connection);
  perform dblink_send_query('candidate_auth_action_first',$query$
    with failed as materialized (
      select public.candidate_auth_account_transition_v1(
        'LOGIN_SUCCESS','TEST',null,'unknown-action-race@example.test',null,null,
        jsonb_build_object(
          'login_failed',true,
          'idempotency_request_sha256',repeat('94',32),
          'idempotency_key_version',1
        ),'auth-concurrent-different-action-v1',clock_timestamp()
      ) result
    ), held as materialized (select pg_sleep(1))
    select failed.result::text from failed cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_auth_action_second',$query$
    select public.candidate_auth_challenge_transition_v1(
      'START','TEST','different-action-race@example.test','ACTIVATE',null,
      decode(repeat('95',32),'hex'),'auth-concurrent-different-action-v1',
      clock_timestamp(),1
    )::text
  $query$);
  select result::jsonb into v_first
  from dblink_get_result('candidate_auth_action_first') as received(result text);
  begin
    perform result
    from dblink_get_result('candidate_auth_action_second') as received(result text);
  exception when sqlstate '40001' then
    v_second_conflict:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  perform dblink_disconnect('candidate_auth_action_first');
  perform dblink_disconnect('candidate_auth_action_second');

  if v_first->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or not v_second_conflict
     or (select count(*) from public.audit_events
         where object_type='candidate_auth_mutation_receipt'
           and object_id_text='TEST'
           and correlation_id='auth-concurrent-different-action-v1')<>1 then
    raise exception 'concurrent different-action key reuse was not rejected';
  end if;
end;
$different_action_same_key_race$;

begin;
delete from public.audit_events
where object_type='candidate_auth_mutation_receipt'
  and object_id_text='TEST'
  and correlation_id like 'auth-concurrent-%';
delete from public.candidate_auth_challenges
where email_normalized='auth-concurrent@example.test';
delete from public.candidates
where id='ca120812-2223-4000-8000-000000000001';
update public.settings_defaults sd
set candidate_app_feature_flags_json=restore.feature_flags
from public._candidate_auth_concurrency_restore_12082026 restore
where sd.id=restore.id;
drop table public._candidate_auth_concurrency_restore_12082026;
commit;
