-- Disposable two-session proof that an exact concurrent Office rejection retry
-- returns the first durable receipt after the shared family lock. Synthetic rows
-- are committed only for dblink visibility and are removed before this file ends.

create extension if not exists dblink;

begin;
update public.settings_defaults
set candidate_app_environment='TEST',
    candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
      'candidate_app_reads',false,
      'candidate_app_writes',false,
      'candidate_notifications',false,
      'candidate_record_role_capabilities',false
    )
where id=1;

insert into public.clients(id,name)
values('ad520000-0000-4000-8000-000000000001','Office rejection replay client');
insert into public.candidates(id,email,active,key_norm)
values('ad520000-0000-4000-8000-000000000002','office-reject-replay@example.test',true,'OFFICE-REJECT-REPLAY');
insert into public.client_settings(
  id,client_id,effective_from,default_submission_mode,week_ending_weekday
) values(
  'ad520000-0000-4000-8000-000000000003',
  'ad520000-0000-4000-8000-000000000001',current_date-30,'ELECTRONIC',
  extract(dow from current_date)::integer
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
  default_submission_mode
) values(
  'ad520000-0000-4000-8000-000000000004',
  'ad520000-0000-4000-8000-000000000002',
  'ad520000-0000-4000-8000-000000000001',
  current_date-30,current_date+30,extract(dow from current_date)::integer,'ELECTRONIC'
);
insert into public.timesheets(
  timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
  submission_mode,r2_nurse_key,r2_auth_key
) values(
  'ad520000-0000-4000-8000-000000000005',
  'ad520000-0000-4000-8000-000000000004','OFFICE-REJECTION-REPLAY',
  current_date,'HOURS','WEEKLY','ELECTRONIC',
  'candidate/office-replay','manager/office-replay'
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
) values(
  'ad520000-0000-4000-8000-000000000006',
  'ad520000-0000-4000-8000-000000000004',current_date,'SUBMITTED','ELECTRONIC',
  'ad520000-0000-4000-8000-000000000005'
);
insert into public.timesheets_financials(
  timesheet_id,candidate_id,client_id,total_hours,processing_status
) values(
  'ad520000-0000-4000-8000-000000000005',
  'ad520000-0000-4000-8000-000000000002',
  'ad520000-0000-4000-8000-000000000001',8,'UNPROCESSED'
);
insert into public.candidate_app_accounts(id,environment,email_normalized,status)
values(
  'ad520000-0000-4000-8000-000000000007','TEST',
  'office-reject-replay@example.test','ACTIVE'
);
insert into public.candidate_submission_workflows(
  id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
  contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
  idempotency_key
) values(
  'ad520000-0000-4000-8000-000000000008','TEST',
  'ad520000-0000-4000-8000-000000000007',
  'ad520000-0000-4000-8000-000000000002','CONTRACT_HOURS','WEEKLY','ELECTRONIC',
  'WORKER_DRAFT',1,'ad520000-0000-4000-8000-000000000004',
  'ad520000-0000-4000-8000-000000000006',
  'ad520000-0000-4000-8000-000000000005',
  'ad520000-0000-4000-8000-000000000005',current_date,'office-reject-replay-source'
);

create function public._candidate_office_reject_replay_probe_v1(p_key text)
returns jsonb
language plpgsql
as $function$
declare
  v_preview jsonb;
begin
  v_preview:=public.cloudtms_office_candidate_adapter_v1(
    'REJECT_PREVIEW','ad520000-0000-4000-8000-000000000009','TEST',
    jsonb_build_object('timesheet_id','ad520000-0000-4000-8000-000000000005'),
    '2026-08-11 21:45:00+00'
  );
  if not coalesce((v_preview->>'permitted')::boolean,false) then
    raise exception 'rejection replay fixture not permitted: %',v_preview;
  end if;
  return public.cloudtms_office_candidate_adapter_v1(
    'REJECT_CONFIRM','ad520000-0000-4000-8000-000000000009','TEST',
    jsonb_build_object(
      'timesheet_id','ad520000-0000-4000-8000-000000000005',
      'expected_timesheet_id',v_preview->>'expected_timesheet_id',
      'expected_row_signature',v_preview->>'expected_row_signature',
      'context_sha256',v_preview->>'context_sha256',
      'reason','Candidate must correct the submitted hours.',
      'idempotency_key',p_key
    ),'2026-08-11 21:45:00+00'
  );
end;
$function$;
commit;

do $same_key_rejection$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
  v_key text:='ad520000-0000-4000-8000-000000000010';
begin
  perform dblink_connect('candidate_office_reject_first',v_connection);
  perform dblink_connect('candidate_office_reject_second',v_connection);
  perform dblink_send_query('candidate_office_reject_first',format($query$
    with rejected as (
      select public._candidate_office_reject_replay_probe_v1('%s') result
    ), held as (select pg_sleep(1))
    select rejected.result::text from rejected cross join held
  $query$,v_key));
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_office_reject_second',format($query$
    select public._candidate_office_reject_replay_probe_v1('%s')::text
  $query$,v_key));
  select result::jsonb into v_first
  from dblink_get_result('candidate_office_reject_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_office_reject_second') as received(result text);
  perform dblink_disconnect('candidate_office_reject_first');
  perform dblink_disconnect('candidate_office_reject_second');

  if v_first->>'timesheet_id' is null
     or v_second->>'timesheet_id' is distinct from v_first->>'timesheet_id'
     or not coalesce((v_second->>'idempotent_replay')::boolean,false)
     or (select count(*) from public.audit_events
         where object_type='cloudtms_office_candidate_rejection'
           and actor_user_id='ad520000-0000-4000-8000-000000000009'
           and correlation_id=v_key)<>1 then
    raise exception 'concurrent exact Office rejection did not return one durable receipt: %, %',v_first,v_second;
  end if;
end;
$same_key_rejection$;

begin;
drop function public._candidate_office_reject_replay_probe_v1(text);
delete from public.candidate_notifications
where account_id='ad520000-0000-4000-8000-000000000007';
delete from public.audit_events
where object_id_text like 'ad520000-0000-4000-8000-%'
   or actor_user_id='ad520000-0000-4000-8000-000000000009';
delete from public.candidate_submission_workflows
where account_id='ad520000-0000-4000-8000-000000000007';
delete from public.candidate_app_accounts
where id='ad520000-0000-4000-8000-000000000007';
delete from public.contract_weeks
where contract_id='ad520000-0000-4000-8000-000000000004';
delete from public.ts_financials_outbox
where timesheet_id in (
  select timesheet_id
  from public.timesheets
  where contract_id='ad520000-0000-4000-8000-000000000004'
);
delete from public.timesheets_financials
where candidate_id='ad520000-0000-4000-8000-000000000002';
delete from public.timesheets
where contract_id='ad520000-0000-4000-8000-000000000004';
delete from public.contracts where id='ad520000-0000-4000-8000-000000000004';
delete from public.client_settings where client_id='ad520000-0000-4000-8000-000000000001';
delete from public.candidates where id='ad520000-0000-4000-8000-000000000002';
delete from public.clients where id='ad520000-0000-4000-8000-000000000001';
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_app_reads',false,
  'candidate_app_writes',false,
  'candidate_notifications',false,
  'candidate_record_role_capabilities',false
)
where id=1;
commit;
