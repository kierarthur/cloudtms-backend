\set ON_ERROR_STOP on

-- Candidate ELECTRONIC source-rotation workflow guard. Disposable PostgreSQL
-- only; every synthetic row and feature change is rolled back.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||jsonb_build_object('candidate_route_confirmation',true),
    candidate_app_environment='TEST'
where id=1;

create or replace function pg_temp.verify_electronic_route_workflow_guard(
  p_expense_route text,
  p_expense_state text
)
returns void
language plpgsql
as $function$
declare
  v_now timestamptz:=clock_timestamp();
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_request uuid:=gen_random_uuid();
  v_component uuid:=gen_random_uuid();
  v_booking_id text:='ELECTRONIC-ROUTE-GUARD-'||replace(gen_random_uuid()::text,'-','');
  v_context jsonb;
  v_result jsonb;
  v_manual uuid;
begin
  if p_expense_route not in ('EMAIL','PAPER') then
    raise exception 'Invalid expense route fixture';
  end if;
  if p_expense_state not in (
      'WORKER_DRAFT','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','REFUSED'
    ) then
    raise exception 'Invalid expense state fixture';
  end if;
  if p_expense_route='PAPER' and p_expense_state='AWAITING_MANAGER_APPROVAL' then
    raise exception 'Invalid PAPER manager-state fixture';
  end if;

  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'electronic-route-guard-'||replace(v_candidate::text,'-','')
    ||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Electronic route guard client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode,overrideclientsettings
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC',true);
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,status,is_current,r2_nurse_key,r2_auth_key
  ) values(v_timesheet,v_contract,v_booking_id,current_date,'HOURS','WEEKLY',
    'ELECTRONIC','RECEIVED',true,
    'candidate/electronic-route-guard/candidate-signature.png',
    'candidate/electronic-route-guard/manager-signature.png');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','electronic-route-guard-'||replace(v_candidate::text,'-','')
    ||'@example.test','ACTIVE');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'FINALISED',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'electronic-route-hours:'||v_hours_workflow::text,v_now
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    p_expense_route,p_expense_state,1,v_contract,v_week,v_timesheet,null,current_date,
    'electronic-route-expense:'||v_expense_workflow::text,null
  );
  update public.timesheets set
    candidate_workflow_id=v_hours_workflow,candidate_workflow_generation=2
  where timesheet_id=v_timesheet;
  if p_expense_state='REFUSED' then
    update public.candidate_submission_workflows set
      rejection_reason='A receipt needs correcting',updated_at_utc=v_now
    where id=v_expense_workflow;
  end if;

  if p_expense_state in ('AWAITING_MANAGER_APPROVAL','REFUSED') then
    insert into public.candidate_approval_requests(
      id,workflow_id,workflow_generation,request_generation,method,state,
      manager_email_normalized,token_hash,expires_at_utc,initial_sent_at_utc,
      last_sent_at_utc,review_manifest_sha256,required_component_ids,
      required_component_manifest_json,idempotency_key,refusal_reason,refused_at_utc
    ) values(
      v_request,v_expense_workflow,1,1,'EMAIL',
      case when p_expense_state='REFUSED' then 'REFUSED' else 'PENDING' end,
      'electronic-route-manager@example.test',
      extensions.digest(
        'electronic-route-manager-token:'||v_expense_workflow::text,'sha256'
      ),
      v_now+interval '7 days',v_now,v_now,
      extensions.digest(
        'electronic-route-manager-manifest:'||v_expense_workflow::text,'sha256'
      ),
      array[v_component],
      jsonb_build_array(jsonb_build_object('component_id',v_component)),
      'electronic-route-manager:'||v_expense_workflow::text,
      case when p_expense_state='REFUSED' then 'A receipt needs correcting' else null end,
      case when p_expense_state='REFUSED' then v_now else null end
    );
  end if;

  v_context:=public.timesheet_route_version_preview_v1(v_timesheet,'SWITCH_TO_MANUAL');
  if v_context->>'linked_workflow_id'<>v_hours_workflow::text
     or v_context->>'active_workflow_id'<>v_expense_workflow::text
     or (v_context->>'active_workflow_count')::integer<>1
     or v_context->>'warning_code'<>
       'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM'
     or not coalesce(
       (v_context->>'incomplete_expense_claim_removal_required')::boolean,false
     )
     or v_context->>'incomplete_expense_workflow_id'<>v_expense_workflow::text
     or v_context->>'incomplete_expense_confirmation_message'<>
       'The candidate has started an expense claim but has not completed it. Do you want to remove the incomplete claim and continue?'
     or not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'Electronic route preview did not request removal for % / %: %',
      p_expense_route,p_expense_state,v_context;
  end if;

  -- The preview represents the office choosing No: no state changes merely by
  -- opening or dismissing the CloudTMS confirmation modal.
  if not (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>p_expense_state then
    raise exception 'Warning preview mutated workflow/source truth';
  end if;

  -- Calling the confirmed authority represents the office choosing Yes.
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_timesheet,v_timesheet,v_context->>'row_signature',
    v_context->>'context_sha256','SWITCH_TO_MANUAL',v_actor,
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,
    'electronic-route-confirm:'||v_expense_workflow::text,false,v_now
  );
  v_manual:=nullif(v_result->>'new_timesheet_id','')::uuid;

  if v_manual is null
     or (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or not (select is_current from public.timesheets where timesheet_id=v_manual)
     or (select count(*) from public.timesheets where booking_id=v_booking_id)<>2
     or (select state from public.candidate_submission_workflows
         where id=v_hours_workflow)<>'FINALISED'
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'SUPERSEDED'
     or (select anchor_timesheet_id from public.candidate_submission_workflows
         where id=v_expense_workflow)<>v_timesheet
     or (p_expense_state='AWAITING_MANAGER_APPROVAL' and
         (select state from public.candidate_approval_requests where id=v_request)<>'CANCELLED')
     or not coalesce(
       (v_result#>>'{workflow_retirement,incomplete_expense_claim_removed}')::boolean,false
     )
     or v_result#>>'{workflow_retirement,incomplete_expense_workflow_id}'<>
       v_expense_workflow::text
     or (p_expense_state='REFUSED' and
         (select rejection_reason from public.candidate_submission_workflows
          where id=v_expense_workflow)<>'A receipt needs correcting')
     or (p_expense_state='REFUSED' and
         (select state from public.candidate_approval_requests where id=v_request)<>'REFUSED')
     or (p_expense_state='REFUSED' and
         (select refusal_reason from public.candidate_approval_requests
          where id=v_request)<>'A receipt needs correcting')
     or (p_expense_state='REFUSED' and
         (select refused_at_utc from public.candidate_approval_requests
          where id=v_request)<>v_now)
     or (p_expense_state='AWAITING_MANAGER_APPROVAL' and not exists(
       select 1 from public.mail_outbox
       where deterministic_outbox_key=
         'CANDIDATE_MANAGER_APPROVAL_WITHDRAWN_V1:'||v_request::text
     )) then
    raise exception 'Confirmed incomplete-claim removal was incomplete: %',v_result;
  end if;
end;
$function$;

select pg_temp.verify_electronic_route_workflow_guard('EMAIL','WORKER_DRAFT');
select pg_temp.verify_electronic_route_workflow_guard('EMAIL','WORKER_SUBMITTED');
select pg_temp.verify_electronic_route_workflow_guard('EMAIL','AWAITING_MANAGER_APPROVAL');
select pg_temp.verify_electronic_route_workflow_guard('EMAIL','REFUSED');
select pg_temp.verify_electronic_route_workflow_guard('PAPER','WORKER_DRAFT');
select pg_temp.verify_electronic_route_workflow_guard('PAPER','WORKER_SUBMITTED');
select pg_temp.verify_electronic_route_workflow_guard('PAPER','REFUSED');

-- A workflow introduced after preview must invalidate the frozen context when
-- confirmation recomputes the complete booking family under its locks.
do $preview_confirm_race$
declare
  v_now timestamptz:=clock_timestamp();
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_context jsonb;
  v_blocked boolean:=false;
  v_booking_id text:='ELECTRONIC-ROUTE-RACE-'||replace(gen_random_uuid()::text,'-','');
begin
  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'electronic-route-race-'||replace(v_candidate::text,'-','')
    ||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Electronic route race client '||left(v_client::text,8));
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
  values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,overrideclientsettings
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC',true);
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,status,is_current,r2_nurse_key,r2_auth_key
  ) values(v_timesheet,v_contract,v_booking_id,current_date,'HOURS','WEEKLY',
    'ELECTRONIC','RECEIVED',true,
    'candidate/electronic-route-race/candidate-signature.png',
    'candidate/electronic-route-race/manager-signature.png');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','electronic-route-race-'||replace(v_candidate::text,'-','')
    ||'@example.test','ACTIVE');

  v_context:=public.timesheet_route_version_preview_v1(v_timesheet,'SWITCH_TO_MANUAL');
  if not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'Race fixture preview was unexpectedly blocked: %',v_context;
  end if;

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','EMAIL',
    'WORKER_DRAFT',1,v_contract,v_week,v_timesheet,null,current_date,
    'electronic-route-race:'||v_workflow::text
  );

  begin
    perform public.timesheet_route_version_confirmed_v1(
      v_timesheet,v_timesheet,v_context->>'row_signature',
      v_context->>'context_sha256','SWITCH_TO_MANUAL',v_actor,
      'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,
      'electronic-route-race-confirm:'||v_workflow::text,false,v_now
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'ROUTE_CHANGE_CONTEXT_CHANGED' then raise; end if;
    v_blocked:=true;
  end;

  if not v_blocked
     or not (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or (select count(*) from public.timesheets where booking_id=v_booking_id)<>1
     or (select state from public.candidate_submission_workflows
         where id=v_workflow)<>'WORKER_DRAFT' then
    raise exception 'Preview/confirm race changed route or workflow truth';
  end if;
end;
$preview_confirm_race$;

rollback;
