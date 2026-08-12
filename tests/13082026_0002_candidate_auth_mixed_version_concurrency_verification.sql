-- Candidate authentication mixed-writer concurrency proof.
-- Two database sessions race the same operation key while proposing different
-- request-HMAC writer versions.  The first reservation is authoritative and
-- the later factual request must use that exact frozen version.

create extension if not exists dblink;

drop table if exists public._candidate_auth_mixed_restore_13082026;
create table public._candidate_auth_mixed_restore_13082026(
  id integer primary key,
  feature_flags jsonb not null
);

begin;
insert into public._candidate_auth_mixed_restore_13082026(id,feature_flags)
select id,candidate_app_feature_flags_json
from public.settings_defaults
where id=1;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_account_registration',true
)
where id=1;
commit;

do $mixed_version_reservation_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
  v_receipt jsonb;
begin
  perform dblink_connect('candidate_auth_reserve_first',v_connection);
  perform dblink_connect('candidate_auth_reserve_second',v_connection);
  perform dblink_send_query('candidate_auth_reserve_first',$query$
    with reserved as materialized (
      select public.candidate_auth_account_transition_v1(
        'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
        jsonb_build_object(
          'replay_probe_only',true,
          'reserve_request_key_version',true,
          'idempotency_key_version',2
        ),'candidate-auth-mixed-version-v1',clock_timestamp()
      ) result
    ), held as materialized (select pg_sleep(1))
    select reserved.result::text from reserved cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_auth_reserve_second',$query$
    select public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
      jsonb_build_object(
        'replay_probe_only',true,
        'reserve_request_key_version',true,
        'idempotency_key_version',1
      ),'candidate-auth-mixed-version-v1',clock_timestamp()
    )::text
  $query$);

  select result::jsonb into v_first
  from dblink_get_result('candidate_auth_reserve_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_auth_reserve_second') as received(result text);
  perform dblink_disconnect('candidate_auth_reserve_first');
  perform dblink_disconnect('candidate_auth_reserve_second');

  select jsonb_build_object('before',before_json,'after',after_json)
  into v_receipt
  from public.audit_events
  where object_type='candidate_auth_mutation_receipt'
    and object_id_text='TEST'
    and correlation_id='candidate-auth-mixed-version-v1';

  if coalesce((v_first->>'replay_receipt_found')::boolean,true)
     or coalesce((v_second->>'replay_receipt_found')::boolean,true)
     or not coalesce((v_first->>'request_version_reserved')::boolean,false)
     or not coalesce((v_second->>'request_version_reserved')::boolean,false)
     or (v_first->>'request_key_version')::integer<>2
     or (v_second->>'request_key_version')::integer<>2
     or v_receipt#>>'{before,metadata,request_key_version}'<>'2'
     or v_receipt#>>'{before,request_sha256}' is not null
     or v_receipt->'after'<>'null'::jsonb
     or (select count(*) from public.audit_events
         where object_type='candidate_auth_mutation_receipt'
           and object_id_text='TEST'
           and correlation_id='candidate-auth-mixed-version-v1')<>1 then
    raise exception 'mixed writer reservation was not first-writer stable: first %, second %, receipt %',
      v_first,v_second,v_receipt;
  end if;

  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',null,'changed-mixed-version@example.test',null,null,
      jsonb_build_object(
        'replay_probe_only',true,
        'reserve_request_key_version',true,
        'idempotency_key_version',2
      ),'candidate-auth-mixed-version-v1',clock_timestamp()
    );
    raise exception 'changed reservation identity reused the same key';
  exception when sqlstate '40001' then
    if position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)=0 then raise; end if;
  end;
end;
$mixed_version_reservation_race$;

do $reserved_version_execution_and_replay$
declare
  v_first jsonb;
  v_probe jsonb;
  v_replay jsonb;
  v_failed boolean:=false;
begin
  v_first:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
    jsonb_build_object(
      'login_failed',true,
      'idempotency_request_sha256',repeat('a1',32),
      'idempotency_key_version',2
    ),'candidate-auth-mixed-version-v1',clock_timestamp()
  );
  v_probe:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
    jsonb_build_object(
      'replay_probe_only',true,
      'reserve_request_key_version',true,
      'idempotency_key_version',1
    ),'candidate-auth-mixed-version-v1',clock_timestamp()
  );
  v_replay:=public.candidate_auth_account_transition_v1(
    'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
    jsonb_build_object(
      'login_failed',true,
      'idempotency_request_sha256',repeat('a1',32),
      'idempotency_key_version',2
    ),'candidate-auth-mixed-version-v1',clock_timestamp()
  );

  if v_first->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or coalesce((v_first->>'failed_login_recorded')::boolean,true)
     or not coalesce((v_probe->>'replay_receipt_found')::boolean,false)
     or (v_probe->>'request_key_version')::integer<>2
     or v_replay->>'error_code'<>'CANDIDATE_LOGIN_INVALID'
     or not coalesce((v_replay->>'idempotent_replay')::boolean,false) then
    raise exception 'reserved version did not own execution/replay: first %, probe %, replay %',
      v_first,v_probe,v_replay;
  end if;

  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGIN_SUCCESS','TEST',null,'mixed-version@example.test',null,null,
      jsonb_build_object(
        'login_failed',true,
        'idempotency_request_sha256',repeat('a1',32),
        'idempotency_key_version',1
      ),'candidate-auth-mixed-version-v1',clock_timestamp()
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'main execution could overwrite its reserved key version';
  end if;

  v_failed:=false;
  begin
    perform public.candidate_auth_account_transition_v1(
      'LOGOUT','TEST',null,null,null,null,
      jsonb_build_object(
        'replay_probe_only',true,
        'reserve_request_key_version',true,
        'idempotency_key_version',2
      ),'candidate-auth-mixed-version-v1',clock_timestamp()
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_IDEMPOTENCY_CONFLICT' in sqlerrm)>0;
  end;
  if not v_failed then
    raise exception 'different action reused a reserved authentication key';
  end if;
end;
$reserved_version_execution_and_replay$;

begin;
delete from public.audit_events
where object_type='candidate_auth_mutation_receipt'
  and object_id_text='TEST'
  and correlation_id='candidate-auth-mixed-version-v1';
update public.settings_defaults sd
set candidate_app_feature_flags_json=restore.feature_flags
from public._candidate_auth_mixed_restore_13082026 restore
where sd.id=restore.id;
drop table public._candidate_auth_mixed_restore_13082026;
commit;
