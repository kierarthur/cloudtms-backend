-- Disposable PostgreSQL proof for source-bound rejected-workflow resubmission.
-- Synthetic fixtures are committed only so dblink sessions can exercise races,
-- then every exact fixture and helper is removed before the suite returns.

create extension if not exists dblink;

begin;

update public.settings_defaults
set candidate_app_environment='TEST',
    candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||'{"candidate_app_writes":true,"candidate_app_reads":true}'::jsonb
where id=1;

insert into public.clients(id,name)
values('b7080000-0000-4000-8000-000000000001','Candidate resubmission client');
insert into public.candidates(id,email,active,key_norm)
values('b7080000-0000-4000-8000-000000000002','candidate-resubmit@example.test',true,'RESUBMIT-GCK');
insert into public.client_settings(
  id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  candidate_expenses_require_separate_timesheet,allow_daily_manager_authorise_on_phone
) values(
  'b7080000-0000-4000-8000-000000000003',
  'b7080000-0000-4000-8000-000000000001',current_date-365,'ELECTRONIC',
  extract(dow from current_date)::integer,true,true
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
  default_submission_mode
) values(
  'b7080000-0000-4000-8000-000000000004',
  'b7080000-0000-4000-8000-000000000002',
  'b7080000-0000-4000-8000-000000000001',
  current_date-400,current_date+30,extract(dow from current_date)::integer,
  'ELECTRONIC'
);
insert into public.candidate_app_accounts(id,environment,email_normalized,status)
values('b7080000-0000-4000-8000-000000000005','TEST','candidate-resubmit@example.test','ACTIVE');
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values(
  'b7080000-0000-4000-8000-000000000006',
  'b7080000-0000-4000-8000-000000000005','TEST',
  'b7080000-0000-4000-8000-000000000002','ACTIVE',decode(repeat('b7',32),'hex'),
  now()+interval '30 days',now()+interval '90 days'
);

do $fixtures$
declare
  v_no integer;
  v_timesheet uuid;
  v_week uuid;
  v_source uuid;
  v_kind text;
  v_week_date date;
begin
  for v_no in 1..7 loop
    v_timesheet:=format('b7080000-0000-4000-8000-%s',lpad((100+v_no)::text,12,'0'))::uuid;
    v_week:=format('b7080000-0000-4000-8000-%s',lpad((200+v_no)::text,12,'0'))::uuid;
    v_week_date:=current_date-((v_no-1)*7);
    insert into public.timesheets(
      timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
      submission_mode,r2_nurse_key,r2_auth_key
    ) values(
      v_timesheet,'b7080000-0000-4000-8000-000000000004',
      'RESUBMIT-WEEK-'||v_no::text,v_week_date,'HOURS','WEEKLY','ELECTRONIC',
      'candidate/resubmit/'||v_no::text,'manager/resubmit/'||v_no::text
    );
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
    ) values(
      v_week,'b7080000-0000-4000-8000-000000000004',v_week_date,
      'OPEN','ELECTRONIC',v_timesheet
    );
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,total_hours,processing_status
    ) values(
      v_timesheet,'b7080000-0000-4000-8000-000000000002',
      'b7080000-0000-4000-8000-000000000001',8,'UNPROCESSED'
    );
    if v_no<=5 then
      v_source:=format('b7080000-0000-4000-8000-%s',lpad((300+v_no)::text,12,'0'))::uuid;
      v_kind:=case when v_no=4 then 'CONTRACT_EXPENSE'
                   when v_no=5 then 'CONTRACT_COMBINED'
                   else 'CONTRACT_HOURS' end;
      insert into public.candidate_submission_workflows(
        id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
        contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
        week_ending_date,idempotency_key,created_at_utc,updated_at_utc
      ) values(
        v_source,'TEST','b7080000-0000-4000-8000-000000000005',
        'b7080000-0000-4000-8000-000000000002',v_kind,'WEEKLY','ELECTRONIC',
        'REJECTED',2,'b7080000-0000-4000-8000-000000000004',v_week,v_timesheet,
        case when v_kind='CONTRACT_EXPENSE' then null else v_timesheet end,
        v_week_date,
        case when v_no=1 then 'b7080000-0000-4000-8000-000000009001'
             else format('resubmit-source-%s',v_no) end,
        now()-interval '1 day',now()-interval '1 day'
      );
    end if;
  end loop;
end;
$fixtures$;

insert into public.timesheets(
  timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
  submission_mode,scheduled_start_iso,scheduled_end_iso,worked_start_iso,worked_end_iso,
  break_minutes,worked_minutes,job_title_norm,band,candidate_submission_route_intent
) values(
  'b7080000-0000-4000-8000-000000000701',
  'b7080000-0000-4000-8000-000000000004','RESUBMIT-DAILY-1',current_date-42,
  'HOURS','DAILY','MANUAL',current_date-42+time '08:00',current_date-42+time '18:00',
  current_date-42+time '08:00',current_date-42+time '17:00',60,480,'NURSE','5','ELECTRONIC'
);
insert into public.timesheets_financials(
  timesheet_id,candidate_id,client_id,total_hours,processing_status,
  worked_start_iso,worked_end_iso,break_minutes,role,band,pay_method
) values(
  'b7080000-0000-4000-8000-000000000701',
  'b7080000-0000-4000-8000-000000000002',
  'b7080000-0000-4000-8000-000000000001',8,'UNPROCESSED',
  current_date-42+time '08:00',current_date-42+time '17:00',60,'NURSE','Band 5','PAYE'
);
insert into public.candidate_submission_workflows(
  id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
  contract_id,anchor_timesheet_id,target_timesheet_id,work_date,idempotency_key,
  created_at_utc,updated_at_utc
) values(
  'b7080000-0000-4000-8000-000000000306','TEST',
  'b7080000-0000-4000-8000-000000000005','b7080000-0000-4000-8000-000000000002',
  'DAILY','DAILY','PHONE','REJECTED',2,'b7080000-0000-4000-8000-000000000004',
  'b7080000-0000-4000-8000-000000000701','b7080000-0000-4000-8000-000000000701',
  current_date-42,'resubmit-source-daily',now()-interval '1 day',now()-interval '1 day'
);

create function public._candidate_resubmit_probe_v1(p_source uuid,p_key text)
returns jsonb
language sql
as $function$
  select public.candidate_workflow_transition_atomic_v1(
    'b7080000-0000-4000-8000-000000000006','TEST',p_source,
    'RESUBMIT_REJECTED',2,'{}'::jsonb,p_key,now()
  );
$function$;

create function public._candidate_generic_create_probe_v1(p_workflow uuid,p_key text)
returns jsonb
language sql
as $function$
  select public.candidate_workflow_transition_atomic_v1(
    'b7080000-0000-4000-8000-000000000006','TEST',p_workflow,'CREATE',null,
    jsonb_build_object(
      'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
      'contract_id','b7080000-0000-4000-8000-000000000004',
      'contract_week_id','b7080000-0000-4000-8000-000000000207'
    ),p_key,now()
  );
$function$;

commit;

do $sequential_contract$
declare
  v_result jsonb;
  v_replay jsonb;
  v_replacement uuid;
  v_failed boolean;
begin
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      'b7080000-0000-4000-8000-000000000006','TEST',
      'b7080000-0000-4000-8000-000000000301','RESUBMIT_REJECTED',1,
      '{}'::jsonb,'b7080000-0000-4000-8000-000000009000',now()
    );
  exception when sqlstate '40001' then
    v_failed:=sqlerrm='WORKFLOW_GENERATION_CONFLICT';
  end;
  if not coalesce(v_failed,false) then
    raise exception 'resubmission accepted the wrong rejected-source generation';
  end if;
  v_failed:=false;

  begin
    perform public._candidate_resubmit_probe_v1(
      'b7080000-0000-4000-8000-000000000301',
      'b7080000-0000-4000-8000-000000009001'
    );
  exception when sqlstate '40001' then
    v_failed:=sqlerrm='CANDIDATE_IDEMPOTENCY_CONFLICT';
  end;
  if not coalesce(v_failed,false) then
    raise exception 'source idempotency key was accepted as its own replacement';
  end if;

  v_result:=public._candidate_resubmit_probe_v1(
    'b7080000-0000-4000-8000-000000000301',
    'b7080000-0000-4000-8000-000000009002'
  );
  v_replay:=public._candidate_resubmit_probe_v1(
    'b7080000-0000-4000-8000-000000000301',
    'b7080000-0000-4000-8000-000000009002'
  );
  v_replacement:=(v_result->>'replacement_workflow_id')::uuid;
  if v_replacement is null
     or v_replay->>'replacement_workflow_id'<>v_replacement::text
     or coalesce((v_replay->>'idempotent_replay')::boolean,false)=false
     or (select replacement_of_workflow_id from public.candidate_submission_workflows
         where id=v_replacement)<>'b7080000-0000-4000-8000-000000000301'::uuid
     or (select state from public.candidate_submission_workflows
         where id='b7080000-0000-4000-8000-000000000301')<>'REJECTED' then
    raise exception 'sequential source-bound replay or lineage failed: %, %',v_result,v_replay;
  end if;

  v_failed:=false;
  begin
    perform public._candidate_resubmit_probe_v1(
      'b7080000-0000-4000-8000-000000000302',
      'b7080000-0000-4000-8000-000000009002'
    );
  exception when sqlstate '40001' then
    v_failed:=sqlerrm='CANDIDATE_IDEMPOTENCY_CONFLICT';
  end;
  if not v_failed then raise exception 'cross-source key reuse did not fail closed'; end if;

  v_result:=public._candidate_generic_create_probe_v1(
    'b7080000-0000-4000-8000-000000000801',
    'b7080000-0000-4000-8000-000000009010'
  );
  v_replay:=public._candidate_generic_create_probe_v1(
    'b7080000-0000-4000-8000-000000000802',
    'b7080000-0000-4000-8000-000000009010'
  );
  if v_result->>'workflow_id'<>v_replay->>'workflow_id'
     or coalesce((v_replay->>'idempotent_replay')::boolean,false)=false then
    raise exception 'generic CREATE exact replay failed: %, %',v_result,v_replay;
  end if;
  v_failed:=false;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      'b7080000-0000-4000-8000-000000000006','TEST',
      'b7080000-0000-4000-8000-000000000803','CREATE',null,
      jsonb_build_object(
        'workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','PAPER',
        'contract_id','b7080000-0000-4000-8000-000000000004',
        'contract_week_id','b7080000-0000-4000-8000-000000000207',
        'week_ending_date',current_date-42
      ),'b7080000-0000-4000-8000-000000009010',now()
    );
  exception when sqlstate '40001' then
    v_failed:=sqlerrm='CANDIDATE_IDEMPOTENCY_CONFLICT';
  end;
  if not v_failed then raise exception 'generic CREATE conflicting key reuse did not fail'; end if;
end;
$sequential_contract$;

do $same_key_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first jsonb;
  v_second jsonb;
begin
  perform dblink_connect('candidate_resubmit_same_first',v_connection);
  perform dblink_connect('candidate_resubmit_same_second',v_connection);
  perform dblink_send_query('candidate_resubmit_same_first',$query$
    with created as (
      select public._candidate_resubmit_probe_v1(
        'b7080000-0000-4000-8000-000000000302',
        'b7080000-0000-4000-8000-000000009020'
      ) result
    ), held as (select pg_sleep(1))
    select created.result::text from created cross join held
  $query$);
  perform pg_sleep(0.1);
  perform dblink_send_query('candidate_resubmit_same_second',$query$
    select public._candidate_resubmit_probe_v1(
      'b7080000-0000-4000-8000-000000000302',
      'b7080000-0000-4000-8000-000000009020'
    )::text
  $query$);
  select result::jsonb into v_first
  from dblink_get_result('candidate_resubmit_same_first') as received(result text);
  select result::jsonb into v_second
  from dblink_get_result('candidate_resubmit_same_second') as received(result text);
  perform dblink_disconnect('candidate_resubmit_same_first');
  perform dblink_disconnect('candidate_resubmit_same_second');
  if v_first->>'replacement_workflow_id' is distinct from v_second->>'replacement_workflow_id'
     or (select count(*) from public.candidate_submission_workflows
         where replacement_of_workflow_id='b7080000-0000-4000-8000-000000000302')<>1 then
    raise exception 'same-key concurrent resubmission did not return one replacement: %, %',v_first,v_second;
  end if;
end;
$same_key_race$;

do $different_key_races$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_source uuid;
  v_no integer;
  v_first text;
  v_second_controlled boolean;
begin
  foreach v_source in array array[
    'b7080000-0000-4000-8000-000000000303'::uuid,
    'b7080000-0000-4000-8000-000000000304'::uuid,
    'b7080000-0000-4000-8000-000000000305'::uuid,
    'b7080000-0000-4000-8000-000000000306'::uuid
  ] loop
    v_no:=coalesce(v_no,0)+1;
    perform dblink_connect('candidate_resubmit_diff_first',v_connection);
    perform dblink_connect('candidate_resubmit_diff_second',v_connection);
    perform dblink_send_query('candidate_resubmit_diff_first',format($query$
      with created as (
        select public._candidate_resubmit_probe_v1('%s','b7080000-0000-4000-8000-%s') result
      ), held as (select pg_sleep(1))
      select created.result::text from created cross join held
    $query$,v_source,lpad((9030+v_no)::text,12,'0')));
    perform pg_sleep(0.1);
    perform dblink_send_query('candidate_resubmit_diff_second',format($query$
      select public._candidate_resubmit_probe_v1('%s','b7080000-0000-4000-8000-%s')::text
    $query$,v_source,lpad((9040+v_no)::text,12,'0')));
    select result into v_first
    from dblink_get_result('candidate_resubmit_diff_first') as received(result text);
    v_second_controlled:=false;
    begin
      perform result
      from dblink_get_result('candidate_resubmit_diff_second') as received(result text);
    exception when sqlstate '40001' then
      v_second_controlled:=sqlerrm='CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED';
    end;
    perform dblink_disconnect('candidate_resubmit_diff_first');
    perform dblink_disconnect('candidate_resubmit_diff_second');
    if (v_first::jsonb)->>'replacement_workflow_id' is null
       or not v_second_controlled
       or (select count(*) from public.candidate_submission_workflows
           where replacement_of_workflow_id=v_source)<>1 then
      raise exception 'different-key source race failed for %',v_source;
    end if;
  end loop;
end;
$different_key_races$;

do $no_work_invocation$
declare
  v_invocation jsonb;
begin
  v_invocation:=private._candidate_action_invocation_v1(jsonb_build_object(
    'code','NO_WORK_THIS_WEEK','method','POST',
    'contract_week_id','b7080000-0000-4000-8000-000000000207',
    'path','/candidate-app/v1/contract-weeks/b7080000-0000-4000-8000-000000000207/no-work'
  ));
  if v_invocation#>'{invocation,required_user_inputs}'
       <> '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb then
    raise exception 'NO_WORK_THIS_WEEK still requires a server-owned row signature: %',v_invocation;
  end if;
end;
$no_work_invocation$;

begin;
drop function public._candidate_generic_create_probe_v1(uuid,text);
drop function public._candidate_resubmit_probe_v1(uuid,text);
delete from public.candidate_submission_workflows
where account_id='b7080000-0000-4000-8000-000000000005';
delete from public.candidate_app_sessions where id='b7080000-0000-4000-8000-000000000006';
delete from public.candidate_app_accounts where id='b7080000-0000-4000-8000-000000000005';
delete from public.contract_weeks where contract_id='b7080000-0000-4000-8000-000000000004';
delete from public.timesheets_financials where candidate_id='b7080000-0000-4000-8000-000000000002';
delete from public.timesheets where contract_id='b7080000-0000-4000-8000-000000000004';
delete from public.contracts where id='b7080000-0000-4000-8000-000000000004';
delete from public.client_settings where client_id='b7080000-0000-4000-8000-000000000001';
delete from public.candidates where id='b7080000-0000-4000-8000-000000000002';
delete from public.clients where id='b7080000-0000-4000-8000-000000000001';
delete from public.audit_events where object_id_text like 'b7080000-0000-4000-8000-%';
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":false,"candidate_app_reads":false}'::jsonb
where id=1;
commit;
