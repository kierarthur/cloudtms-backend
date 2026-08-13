-- Candidate PHONE winner and refresh security-receipt closure proof.
-- This suite exercises the durable refresh-token-reuse result concurrently.

create extension if not exists dblink;

drop table if exists public._candidate_refresh_security_restore_13082026;
create table public._candidate_refresh_security_restore_13082026(
  id integer primary key,
  feature_flags jsonb not null
);

begin;
insert into public._candidate_refresh_security_restore_13082026(id,feature_flags)
select id,candidate_app_feature_flags_json
from public.settings_defaults
where id=1;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;

insert into public.candidate_app_accounts(
  id,environment,email_normalized,status,created_at_utc,updated_at_utc
) values (
  '13082026-0617-4000-8000-000000000001','TEST',
  'refresh-security@example.test','ACTIVE',now(),now()
);

insert into public.candidate_app_sessions(
  id,account_id,environment,status,refresh_token_hash,token_family_id,rotation,
  issued_at_utc,expires_at_utc,absolute_expires_at_utc,last_used_at_utc,
  created_at_utc,updated_at_utc
) values (
  '13082026-0617-4000-8000-000000000003',
  '13082026-0617-4000-8000-000000000001','TEST','ACTIVE',decode(repeat('62',32),'hex'),
  '13082026-0617-4000-8000-000000000004',1,now(),now()+interval '30 days',
  now()+interval '90 days',now(),now(),now()
);

insert into public.candidate_app_sessions(
  id,account_id,environment,status,refresh_token_hash,token_family_id,rotation,
  issued_at_utc,expires_at_utc,absolute_expires_at_utc,last_used_at_utc,
  replaced_by_session_id,created_at_utc,updated_at_utc
) values (
  '13082026-0617-4000-8000-000000000002',
  '13082026-0617-4000-8000-000000000001','TEST','ROTATED',decode(repeat('61',32),'hex'),
  '13082026-0617-4000-8000-000000000004',0,now()-interval '1 hour',
  now()+interval '29 days',now()+interval '90 days',now(),
  '13082026-0617-4000-8000-000000000003',now(),now()
);
commit;

do $refresh_security_concurrency$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
  v_receipt jsonb;
  v_revoked_count integer;
begin
  perform dblink_connect('candidate_refresh_security_first',v_connection);
  perform dblink_connect('candidate_refresh_security_second',v_connection);

  perform dblink_send_query('candidate_refresh_security_first',$query$
    with security_event as materialized (
      select public.candidate_auth_account_transition_v1(
        'REFRESH_SESSION','TEST','13082026-0617-4000-8000-000000000001',null,
        '13082026-0617-4000-8000-000000000002',null,
        jsonb_build_object(
          'presented_refresh_token_hash_hex',repeat('61',32),
          'new_refresh_token_hash_hex',repeat('63',32),
          'new_session_id','13082026-0617-4000-8000-000000000005',
          'idempotency_request_sha256',repeat('64',32),
          'idempotency_key_version',1
        ),'candidate-refresh-security-concurrent-v1',clock_timestamp()
      ) result
    ), held as materialized (select pg_sleep(1))
    select security_event.result::text from security_event cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_refresh_security_second',$query$
    select public.candidate_auth_account_transition_v1(
      'REFRESH_SESSION','TEST','13082026-0617-4000-8000-000000000001',null,
      '13082026-0617-4000-8000-000000000002',null,
      jsonb_build_object(
        'presented_refresh_token_hash_hex',repeat('61',32),
        'new_refresh_token_hash_hex',repeat('65',32),
        'new_session_id','13082026-0617-4000-8000-000000000006',
        'idempotency_request_sha256',repeat('64',32),
        'idempotency_key_version',1
      ),'candidate-refresh-security-concurrent-v1',clock_timestamp()
    )::text
  $query$);

  select result::jsonb into v_first
  from dblink_get_result('candidate_refresh_security_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_refresh_security_second') as received(result text);
  perform dblink_disconnect('candidate_refresh_security_first');
  perform dblink_disconnect('candidate_refresh_security_second');

  select after_json into v_receipt
  from public.audit_events
  where object_type='candidate_auth_mutation_receipt'
    and object_id_text='TEST'
    and correlation_id='candidate-refresh-security-concurrent-v1';

  select count(*) into v_revoked_count
  from public.candidate_app_sessions
  where token_family_id='13082026-0617-4000-8000-000000000004'
    and status='REVOKED'
    and revoke_reason='REFRESH_TOKEN_REUSE';

  if v_first->>'error_code'<>'CANDIDATE_REFRESH_TOKEN_REUSE'
     or v_second->>'error_code'<>'CANDIDATE_REFRESH_TOKEN_REUSE'
     or not coalesce((v_first->>'family_revoked')::boolean,false)
     or not coalesce((v_second->>'family_revoked')::boolean,false)
     or (
       coalesce((v_first->>'idempotent_replay')::boolean,false)::integer
       + coalesce((v_second->>'idempotent_replay')::boolean,false)::integer
     )<>1
     or v_receipt->>'error_code'<>'CANDIDATE_REFRESH_TOKEN_REUSE'
     or not coalesce((v_receipt->>'family_revoked')::boolean,false)
     or v_revoked_count<>2 then
    raise exception 'concurrent refresh security receipt failed: first %, second %, receipt %, revoked %',
      v_first,v_second,v_receipt,v_revoked_count;
  end if;
end;
$refresh_security_concurrency$;

begin;
delete from public.audit_events
where object_type='candidate_auth_mutation_receipt'
  and object_id_text='TEST'
  and correlation_id='candidate-refresh-security-concurrent-v1';
delete from public.candidate_app_sessions
where account_id='13082026-0617-4000-8000-000000000001';
delete from public.candidate_app_accounts
where id='13082026-0617-4000-8000-000000000001';
update public.settings_defaults sd
set candidate_app_feature_flags_json=restore.feature_flags
from public._candidate_refresh_security_restore_13082026 restore
where sd.id=restore.id;
drop table public._candidate_refresh_security_restore_13082026;
commit;
