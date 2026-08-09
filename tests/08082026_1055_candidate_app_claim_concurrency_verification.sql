-- Disposable PostgreSQL-only concurrency proof for one expense claim until authorised.
-- This test commits only synthetic fixtures so that two independent database sessions can see them,
-- then removes every exact fixture before returning. Never run this file against TEST or LIVE.

create extension if not exists dblink;

begin;
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":true,"candidate_manager_approval":true}'::jsonb
where id=1;

insert into public.clients(id,name)
values('a4000000-0000-0000-0000-000000000001','Concurrency runtime client');
insert into public.candidates(id,email,active,key_norm)
values('a4000000-0000-0000-0000-000000000002','concurrency@example.test',true,'GCK-CONCURRENCY');
insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
values(gen_random_uuid(),'a4000000-0000-0000-0000-000000000001',current_date-1,'ELECTRONIC');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode
) values(
  'a4000000-0000-0000-0000-000000000003',
  'a4000000-0000-0000-0000-000000000002',
  'a4000000-0000-0000-0000-000000000001',
  current_date-30,current_date+30,extract(dow from current_date)::integer,'ELECTRONIC'
);
insert into public.timesheets(
  timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
  r2_nurse_key,r2_auth_key
) values(
  'a4000000-0000-0000-0000-000000000009',
  'a4000000-0000-0000-0000-000000000003',current_date,'HOURS','ELECTRONIC',
  'existing/candidate-signature','existing/manager-signature'
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
) values(
  'a4000000-0000-0000-0000-000000000004',
  'a4000000-0000-0000-0000-000000000003',current_date,'OPEN','ELECTRONIC',
  'a4000000-0000-0000-0000-000000000009'
);
insert into public.timesheets_financials(
  timesheet_id,candidate_id,client_id,total_hours,processing_status
) values(
  'a4000000-0000-0000-0000-000000000009',
  'a4000000-0000-0000-0000-000000000002',
  'a4000000-0000-0000-0000-000000000001',8,'UNPROCESSED'
);
insert into public.candidate_app_accounts(id,environment,email_normalized,status)
values(
  'a4000000-0000-0000-0000-000000000005','TEST','concurrency@example.test','ACTIVE'
);
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values(
  'a4000000-0000-0000-0000-000000000006',
  'a4000000-0000-0000-0000-000000000005','TEST',
  'a4000000-0000-0000-0000-000000000002','ACTIVE',decode(repeat('a4',32),'hex'),
  now()+interval '30 days',now()+interval '90 days'
);

create function public._candidate_claim_concurrency_probe_v1(p_workflow_id uuid,p_key text)
returns jsonb language sql as $function$
  select public.candidate_workflow_transition_atomic_v1(
    'a4000000-0000-0000-0000-000000000006','TEST',p_workflow_id,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','a4000000-0000-0000-0000-000000000003',
      'contract_week_id','a4000000-0000-0000-0000-000000000004',
      'week_ending_date',current_date
    ),p_key,now()
  );
$function$;
commit;

do $concurrent_create$
declare
  -- A libpq database/user string works for both local-socket disposable
  -- PostgreSQL and TCP-hosted verification environments.
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first text;
  v_second_failed boolean:=false;
begin
  perform dblink_connect('candidate_claim_first',v_connection);
  perform dblink_connect('candidate_claim_second',v_connection);
  perform dblink_send_query('candidate_claim_first',$query$
    with created as (
      select public._candidate_claim_concurrency_probe_v1(
        'a4000000-0000-0000-0000-000000000007','concurrent-first'
      ) as result
    ), held as (select pg_sleep(1))
    select created.result::text from created cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_claim_second',$query$
    select public._candidate_claim_concurrency_probe_v1(
      'a4000000-0000-0000-0000-000000000008','concurrent-second'
    )::text
  $query$);
  select result into v_first
  from dblink_get_result('candidate_claim_first') as first_result(result text);
  if (v_first::jsonb)->>'state'<>'WORKER_DRAFT' then
    raise exception 'first concurrent expense claim did not succeed: %',v_first;
  end if;
  begin
    perform result
    from dblink_get_result('candidate_claim_second') as second_result(result text);
  exception when others then
    v_second_failed:=sqlerrm like '%CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE%';
  end;
  if not v_second_failed then
    raise exception 'second concurrent expense claim did not fail with the canonical active-claim error';
  end if;
  perform dblink_disconnect('candidate_claim_first');
  perform dblink_disconnect('candidate_claim_second');
  if (select count(*) from public.candidate_submission_workflows
      where id in (
        'a4000000-0000-0000-0000-000000000007'::uuid,
        'a4000000-0000-0000-0000-000000000008'::uuid
      ))<>1 then
    raise exception 'concurrent expense claim gate did not leave exactly one workflow';
  end if;
end;
$concurrent_create$;

begin;
drop function public._candidate_claim_concurrency_probe_v1(uuid,text);
delete from public.candidate_submission_workflows
where id in (
  'a4000000-0000-0000-0000-000000000007',
  'a4000000-0000-0000-0000-000000000008'
);
delete from public.candidate_app_sessions where id='a4000000-0000-0000-0000-000000000006';
delete from public.candidate_app_accounts where id='a4000000-0000-0000-0000-000000000005';
delete from public.contract_weeks where id='a4000000-0000-0000-0000-000000000004';
delete from public.timesheets_financials where timesheet_id='a4000000-0000-0000-0000-000000000009';
delete from public.timesheets where timesheet_id='a4000000-0000-0000-0000-000000000009';
delete from public.contracts where id='a4000000-0000-0000-0000-000000000003';
delete from public.client_settings where client_id='a4000000-0000-0000-0000-000000000001';
delete from public.candidates where id='a4000000-0000-0000-0000-000000000002';
delete from public.clients where id='a4000000-0000-0000-0000-000000000001';
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":false,"candidate_manager_approval":false}'::jsonb
where id=1;
commit;
