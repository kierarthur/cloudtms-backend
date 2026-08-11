\set ON_ERROR_STOP on

-- Disposable PostgreSQL-only two-session proof. Synthetic fixtures are
-- committed so dblink sessions can see them, then removed before return.
-- Never run this file against TEST or LIVE.
create extension if not exists dblink;

begin;
alter table public.timesheets_financials
  add column if not exists paid_by_user_id uuid,
  add column if not exists payment_reference text;
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":true,"candidate_paper_qr":true,"candidate_notifications":true,"candidate_route_confirmation":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

create or replace function public._candidate_rejection_lock_fixture_v1()
returns void language plpgsql as $function$
declare
  v_hours_manifest jsonb:=jsonb_build_object(
    'workflow_id','b5300000-0000-4000-8000-000000000010'::uuid,
    'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_expense_manifest jsonb:=jsonb_build_object(
    'workflow_id','b5300000-0000-4000-8000-000000000011'::uuid,
    'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','EXPENSE_SUMMARY','component_kind','EXPENSE_SUMMARY'))
  );
  v_old_token text:='candidate-paper-lock-old-token';
  v_current_token text:='candidate-paper-lock-current-token';
begin
  insert into public.tms_users(id)
  values('b5300000-0000-4000-8000-000000000001');
  update public.settings_defaults
  set candidate_app_system_actor_user_id='b5300000-0000-4000-8000-000000000001'
  where id=1;
  insert into public.candidates(id,email,active)
  values('b5300000-0000-4000-8000-000000000002','paper-lock@example.test',true);
  insert into public.clients(id,name)
  values('b5300000-0000-4000-8000-000000000003','Candidate PAPER lock-order client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(
    'b5300000-0000-4000-8000-000000000004',
    'b5300000-0000-4000-8000-000000000003',current_date-1,'MANUAL',true
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    'b5300000-0000-4000-8000-000000000005',
    'b5300000-0000-4000-8000-000000000002',
    'b5300000-0000-4000-8000-000000000003',
    current_date-30,current_date+30,extract(dow from current_date)::integer,'MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at,
    qr_signed_hash,qr_signed_at_utc
  ) values(
    'b5300000-0000-4000-8000-000000000006',
    'b5300000-0000-4000-8000-000000000005','PAPER-LOCK-HOURS',current_date,
    'HOURS','WEEKLY','MANUAL','PENDING',v_current_token,
    jsonb_build_object('workflow_id','b5300000-0000-4000-8000-000000000011'::uuid),now(),
    'paper-lock-signed-return',now()
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    'b5300000-0000-4000-8000-000000000007',
    'b5300000-0000-4000-8000-000000000005',current_date,0,'SUBMITTED','MANUAL',
    'b5300000-0000-4000-8000-000000000006'
  );
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours
  ) values(
    'b5300000-0000-4000-8000-000000000006',
    'b5300000-0000-4000-8000-000000000002',
    'b5300000-0000-4000-8000-000000000003',true,'UNPROCESSED',8
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,is_adjustment,parent_timesheet_id
  ) values(
    'b5300000-0000-4000-8000-000000000008',
    'b5300000-0000-4000-8000-000000000005','PAPER-LOCK-EXPENSE',current_date,
    'EXPENSES','WEEKLY','MANUAL',true,'b5300000-0000-4000-8000-000000000006'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id,is_adjustment
  ) values(
    'b5300000-0000-4000-8000-000000000009',
    'b5300000-0000-4000-8000-000000000005',current_date,1,'SUBMITTED','MANUAL',
    'b5300000-0000-4000-8000-000000000008',true
  );
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
    expenses_pay_ex_vat,expenses_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat
  ) values(
    'b5300000-0000-4000-8000-000000000008',
    'b5300000-0000-4000-8000-000000000002',
    'b5300000-0000-4000-8000-000000000003',true,'UNPROCESSED',0,25,30,25,30
  );
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    'b5300000-0000-4000-8000-000000000012','TEST','paper-lock@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values
    (
      'b5300000-0000-4000-8000-000000000010','TEST',
      'b5300000-0000-4000-8000-000000000012','b5300000-0000-4000-8000-000000000002',
      'CONTRACT_HOURS','WEEKLY','PAPER','FINALISED',2,
      'b5300000-0000-4000-8000-000000000005','b5300000-0000-4000-8000-000000000007',
      'b5300000-0000-4000-8000-000000000006','b5300000-0000-4000-8000-000000000006',
      current_date,'paper-lock-hours',now()-interval '2 minutes',v_hours_manifest,
      private._candidate_sha256_jsonb_v1(v_hours_manifest),'CANDIDATE_REVIEW_DOCUMENTS_V1'
    ),
    (
      'b5300000-0000-4000-8000-000000000011','TEST',
      'b5300000-0000-4000-8000-000000000012','b5300000-0000-4000-8000-000000000002',
      'CONTRACT_EXPENSE','WEEKLY','PAPER','FINALISED',2,
      'b5300000-0000-4000-8000-000000000005','b5300000-0000-4000-8000-000000000009',
      'b5300000-0000-4000-8000-000000000006','b5300000-0000-4000-8000-000000000008',
      current_date,'paper-lock-expense',now()-interval '1 minute',v_expense_manifest,
      private._candidate_sha256_jsonb_v1(v_expense_manifest),'CANDIDATE_REVIEW_DOCUMENTS_V1'
    );
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json
  ) values
    (
      'b5300000-0000-4000-8000-000000000013','TIMESHEET_QR','paper-lock@example.test',
      'Earlier hours pack','Pack ready',
      jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/paper-lock/hours.pdf')),
      'QUEUED',now()-interval '2 minutes','timesheets',
      'b5300000-0000-4000-8000-000000000006',now(),now(),
      'paper-lock-hours-mail',jsonb_build_object(
        'candidate_workflow_id','b5300000-0000-4000-8000-000000000010'::uuid,
        'candidate_workflow_generation',1,'candidate_paper_pack_ready',true,
        'mail_held_until_pdf_rendered',false,
        'qr_token_hash',encode(extensions.digest(convert_to(v_old_token,'UTF8'),'sha256'),'hex')
      )
    ),
    (
      'b5300000-0000-4000-8000-000000000014','TIMESHEET_QR','paper-lock@example.test',
      'Current expense pack','Pack ready',
      jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/paper-lock/expense.pdf')),
      'QUEUED',now()-interval '1 minute','timesheets',
      'b5300000-0000-4000-8000-000000000006',now(),now(),
      'paper-lock-expense-mail',jsonb_build_object(
        'candidate_workflow_id','b5300000-0000-4000-8000-000000000011'::uuid,
        'candidate_workflow_generation',1,'candidate_paper_pack_ready',true,
        'mail_held_until_pdf_rendered',false,
        'qr_token_hash',encode(extensions.digest(convert_to(v_current_token,'UTF8'),'sha256'),'hex')
      )
    );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values
    (
      'b5300000-0000-4000-8000-000000000015',
      'b5300000-0000-4000-8000-000000000012','b5300000-0000-4000-8000-000000000002',
      'b5300000-0000-4000-8000-000000000010','b5300000-0000-4000-8000-000000000006',
      'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1','{}','{}',
      'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:b5300000-0000-4000-8000-000000000010:1:lock',now()
    ),
    (
      'b5300000-0000-4000-8000-000000000016',
      'b5300000-0000-4000-8000-000000000012','b5300000-0000-4000-8000-000000000002',
      'b5300000-0000-4000-8000-000000000011','b5300000-0000-4000-8000-000000000006',
      'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1','{}','{}',
      'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:b5300000-0000-4000-8000-000000000011:1:lock',now()
    );
end;
$function$;

create or replace function public._candidate_route_hours_lock_probe_v1(
  p_key text,p_hold_seconds numeric default 0
)
returns jsonb language plpgsql as $function$
declare v_context jsonb; v_result jsonb;
begin
  v_context:=public.timesheet_route_version_preview_v1(
    'b5300000-0000-4000-8000-000000000006','CONVERT_QR_TO_MANUAL'
  );
  if not coalesce((v_context->>'permitted_action')::boolean,false) then
    return jsonb_build_object(
      'outcome','CONTROLLED_CONFLICT','code',coalesce(v_context->>'block_reason','ROUTE_CHANGE_NOT_PERMITTED')
    );
  end if;
  v_result:=public.timesheet_route_version_confirmed_v1(
    'b5300000-0000-4000-8000-000000000006',
    'b5300000-0000-4000-8000-000000000006',
    v_context->>'row_signature',v_context->>'context_sha256',
    'CONVERT_QR_TO_MANUAL','b5300000-0000-4000-8000-000000000001',
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,p_key,false,clock_timestamp()
  );
  perform pg_sleep(greatest(coalesce(p_hold_seconds,0),0));
  return jsonb_build_object('outcome','SUCCESS','result',v_result);
exception
  when sqlstate '40001' then
    return jsonb_build_object('outcome','CONTROLLED_CONFLICT','code',sqlerrm);
  when sqlstate '55000' then
    if sqlerrm not in ('ROUTE_CHANGE_NOT_PERMITTED','TIMESHEET_MOVED') then raise; end if;
    return jsonb_build_object('outcome','CONTROLLED_CONFLICT','code',sqlerrm);
end;
$function$;

create or replace function public._candidate_rejection_lock_cleanup_v1()
returns void language plpgsql as $function$
begin
  delete from public.candidate_notifications
  where workflow_id in (
    'b5300000-0000-4000-8000-000000000010',
    'b5300000-0000-4000-8000-000000000011'
  );
  delete from public.audit_events
  where actor_user_id='b5300000-0000-4000-8000-000000000001'
    or object_id_text in (
      'b5300000-0000-4000-8000-000000000010',
      'b5300000-0000-4000-8000-000000000011'
    );
  delete from public.mail_outbox
  where id in (
    'b5300000-0000-4000-8000-000000000013',
    'b5300000-0000-4000-8000-000000000014'
  );
  delete from public.candidate_submission_components
  where workflow_id in (
    'b5300000-0000-4000-8000-000000000010',
    'b5300000-0000-4000-8000-000000000011'
  );
  delete from public.candidate_approval_requests
  where workflow_id in (
    'b5300000-0000-4000-8000-000000000010',
    'b5300000-0000-4000-8000-000000000011'
  );
  delete from public.candidate_submission_workflows
  where id in (
    'b5300000-0000-4000-8000-000000000010',
    'b5300000-0000-4000-8000-000000000011'
  );
  delete from public.candidate_app_accounts
  where id='b5300000-0000-4000-8000-000000000012';
  delete from public.ts_financials_outbox
  where timesheet_id in (
    select timesheet_id from public.timesheets
    where booking_id in ('PAPER-LOCK-HOURS','PAPER-LOCK-EXPENSE')
  );
  delete from public.timesheets_financials
  where timesheet_id in (
    select timesheet_id from public.timesheets
    where booking_id in ('PAPER-LOCK-HOURS','PAPER-LOCK-EXPENSE')
  );
  delete from public.contract_weeks
  where contract_id='b5300000-0000-4000-8000-000000000005';
  delete from public.timesheets
  where booking_id in ('PAPER-LOCK-HOURS','PAPER-LOCK-EXPENSE');
  delete from public.client_settings
  where id='b5300000-0000-4000-8000-000000000004';
  delete from public.contracts
  where id='b5300000-0000-4000-8000-000000000005';
  delete from public.clients
  where id='b5300000-0000-4000-8000-000000000003';
  delete from public.candidates
  where id='b5300000-0000-4000-8000-000000000002';
  update public.settings_defaults
  set candidate_app_system_actor_user_id=null
  where id=1;
  delete from public.tms_users
  where id='b5300000-0000-4000-8000-000000000001';
end;
$function$;

create or replace function public._candidate_reject_hours_lock_probe_v1(
  p_key text,p_hold_seconds numeric default 0
)
returns jsonb language plpgsql as $function$
declare v_signature text; v_result jsonb;
begin
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(
      'b5300000-0000-4000-8000-000000000006',
      'b5300000-0000-4000-8000-000000000007',false
    )->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(
      'b5300000-0000-4000-8000-000000000006',
      'b5300000-0000-4000-8000-000000000007',false
    )->>'backend_row_signature'
  );
  if nullif(btrim(coalesce(v_signature,'')),'') is null then
    raise exception 'Hours lifecycle signature missing: %',
      public.timesheet_lifecycle_guard_signature_v1(
        'b5300000-0000-4000-8000-000000000006',
        'b5300000-0000-4000-8000-000000000007',false
      );
  end if;
  v_result:=public.candidate_submission_reject_atomic_v1(
    'b5300000-0000-4000-8000-000000000001','TEST',
    'b5300000-0000-4000-8000-000000000006','b5300000-0000-4000-8000-000000000006',
    v_signature,'Concurrent hours rejection',p_key,clock_timestamp()
  );
  perform pg_sleep(greatest(coalesce(p_hold_seconds,0),0));
  return jsonb_build_object('outcome','SUCCESS','result',v_result);
exception when sqlstate '40001' then
  return jsonb_build_object('outcome','CONTROLLED_CONFLICT','code',sqlerrm);
end;
$function$;

create or replace function public._candidate_reject_expense_lock_probe_v1(
  p_key text,p_hold_seconds numeric default 0
)
returns jsonb language plpgsql as $function$
declare v_signature text; v_result jsonb;
begin
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(
      'b5300000-0000-4000-8000-000000000008',
      'b5300000-0000-4000-8000-000000000009',false
    )->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(
      'b5300000-0000-4000-8000-000000000008',
      'b5300000-0000-4000-8000-000000000009',false
    )->>'backend_row_signature'
  );
  if nullif(btrim(coalesce(v_signature,'')),'') is null then
    raise exception 'Expense lifecycle signature missing: %',
      public.timesheet_lifecycle_guard_signature_v1(
        'b5300000-0000-4000-8000-000000000008',
        'b5300000-0000-4000-8000-000000000009',false
      );
  end if;
  v_result:=public.candidate_submission_reject_atomic_v1(
    'b5300000-0000-4000-8000-000000000001','TEST',
    'b5300000-0000-4000-8000-000000000008','b5300000-0000-4000-8000-000000000008',
    v_signature,'Concurrent expense rejection',p_key,clock_timestamp()
  );
  perform pg_sleep(greatest(coalesce(p_hold_seconds,0),0));
  return jsonb_build_object('outcome','SUCCESS','result',v_result);
exception when sqlstate '40001' then
  return jsonb_build_object('outcome','CONTROLLED_CONFLICT','code',sqlerrm);
end;
$function$;
commit;

create or replace function public._run_candidate_rejection_lock_race_v1(p_reverse boolean)
returns void language plpgsql as $function$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first text;
  v_second text;
begin
  perform dblink_connect('candidate_reject_first',v_connection);
  perform dblink_connect('candidate_reject_second',v_connection);
  if not p_reverse then
    perform dblink_send_query('candidate_reject_first',
      $$select public._candidate_reject_hours_lock_probe_v1('lock-order-hours-a',1)::text$$);
    perform pg_sleep(0.1);
    perform dblink_send_query('candidate_reject_second',
      $$select public._candidate_reject_expense_lock_probe_v1('lock-order-expense-b',0)::text$$);
  else
    perform dblink_send_query('candidate_reject_first',
      $$select public._candidate_reject_expense_lock_probe_v1('lock-order-expense-a',1)::text$$);
    perform pg_sleep(0.1);
    perform dblink_send_query('candidate_reject_second',
      $$select public._candidate_reject_hours_lock_probe_v1('lock-order-hours-b',0)::text$$);
  end if;
  select result into v_first
  from dblink_get_result('candidate_reject_first') as first_result(result text);
  select result into v_second
  from dblink_get_result('candidate_reject_second') as second_result(result text);
  if coalesce(v_first::jsonb->>'outcome','') not in ('SUCCESS','CONTROLLED_CONFLICT')
     or coalesce(v_second::jsonb->>'outcome','') not in ('SUCCESS','CONTROLLED_CONFLICT') then
    raise exception 'Concurrent Candidate rejection returned an uncontrolled result: %, %',
      v_first,v_second;
  end if;
  perform dblink_disconnect('candidate_reject_first');
  perform dblink_disconnect('candidate_reject_second');
end;
$function$;

create or replace function public._run_candidate_route_rejection_lock_race_v1(p_reverse boolean)
returns void language plpgsql as $function$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_first text;
  v_second text;
begin
  perform dblink_connect('candidate_route_first',v_connection);
  perform dblink_connect('candidate_route_second',v_connection);
  if not p_reverse then
    perform dblink_send_query('candidate_route_first',
      $$select public._candidate_route_hours_lock_probe_v1('route-reject-route-a',1)::text$$);
    perform pg_sleep(0.1);
    perform dblink_send_query('candidate_route_second',
      $$select public._candidate_reject_expense_lock_probe_v1('route-reject-expense-b',0)::text$$);
  else
    perform dblink_send_query('candidate_route_first',
      $$select public._candidate_reject_expense_lock_probe_v1('route-reject-expense-a',1)::text$$);
    perform pg_sleep(0.1);
    perform dblink_send_query('candidate_route_second',
      $$select public._candidate_route_hours_lock_probe_v1('route-reject-route-b',0)::text$$);
  end if;
  select result into v_first
  from dblink_get_result('candidate_route_first') as first_result(result text);
  select result into v_second
  from dblink_get_result('candidate_route_second') as second_result(result text);
  if coalesce(v_first::jsonb->>'outcome','') not in ('SUCCESS','CONTROLLED_CONFLICT')
     or coalesce(v_second::jsonb->>'outcome','') not in ('SUCCESS','CONTROLLED_CONFLICT')
     or (v_first::jsonb->>'outcome')<>'SUCCESS' and (v_second::jsonb->>'outcome')<>'SUCCESS' then
    raise exception 'Route/rejection race returned an uncontrolled result: %, %',v_first,v_second;
  end if;
  perform dblink_disconnect('candidate_route_first');
  perform dblink_disconnect('candidate_route_second');
end;
$function$;

select public._candidate_rejection_lock_fixture_v1();
select public._run_candidate_rejection_lock_race_v1(false);
select public._candidate_rejection_lock_cleanup_v1();

select public._candidate_rejection_lock_fixture_v1();
select public._run_candidate_rejection_lock_race_v1(true);
select public._candidate_rejection_lock_cleanup_v1();

select public._candidate_rejection_lock_fixture_v1();
select public._run_candidate_route_rejection_lock_race_v1(false);
select public._candidate_rejection_lock_cleanup_v1();

select public._candidate_rejection_lock_fixture_v1();
select public._run_candidate_route_rejection_lock_race_v1(true);
select public._candidate_rejection_lock_cleanup_v1();

begin;
drop function public._run_candidate_route_rejection_lock_race_v1(boolean);
drop function public._run_candidate_rejection_lock_race_v1(boolean);
drop function public._candidate_route_hours_lock_probe_v1(text,numeric);
drop function public._candidate_reject_expense_lock_probe_v1(text,numeric);
drop function public._candidate_reject_hours_lock_probe_v1(text,numeric);
drop function public._candidate_rejection_lock_cleanup_v1();
drop function public._candidate_rejection_lock_fixture_v1();
commit;
