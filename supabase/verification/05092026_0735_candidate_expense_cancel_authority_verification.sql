\set ON_ERROR_STOP on

-- Rollback-contained proof that an expense-only Candidate workflow uses its
-- own carrier lifecycle for cancellation.  Protected imported hours must not
-- hide cancellation for a new claim, while an authorised carrier remains
-- immutable even if a caller bypasses the read-model action list.

begin;

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_anchor uuid:=gen_random_uuid();
  v_carrier uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_workflows jsonb;
  v_contract_result jsonb;
  v_authority jsonb;
  v_rejected boolean:=false;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate Expense Cancellation Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'expense-cancel-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,r2_nurse_key,r2_auth_key,booking_id,occupant_key_norm,
    hospital_norm,ward_norm,job_title_norm,authorised_at_server
  ) values(
    v_anchor,v_contract,current_date,'HOURS','MANUAL','WEEKLY',
    'verification/expense-cancel-anchor','verification/expense-cancel-manager',
    'EXPENSE-CANCEL-ANCHOR-'||v_anchor::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',now()
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    total_hours,processing_status,authorised_at_utc
  ) values(
    v_anchor,1,v_candidate,v_client,true,'CONTRACT_WEEKLY',8,'READY_FOR_INVOICE',now()
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'AUTHORISED','MANUAL',v_anchor);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST','expense-cancel-'||v_candidate::text||'@example.test','SETUP_REQUIRED'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,
    input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_week,v_anchor,
    null,current_date,'{}','{}','expense-cancel-authority-verification'
  );

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_EXPENSE',
    'state','READY_FOR_MANAGER_APPROVAL','generation',1,
    'detail_action_owner',true,'updated_at_utc',now()
  ));
  v_authority:=private._candidate_workflow_cancel_authority_v1(v_workflow);
  v_contract_result:=private._candidate_timesheet_action_contract_v1(
    'AUTHORISED',v_workflows,'{}'::jsonb,v_anchor,v_week,now()
  );
  if not coalesce((v_authority->>'eligible')::boolean,false)
     or not exists(
       select 1
       from jsonb_array_elements(v_contract_result->'available_actions') action_row
       where action_row->>'code'='DISCARD_EXPENSE_CLAIM'
         and (action_row->>'workflow_id')::uuid=v_workflow
     ) then
    raise exception 'Protected imported hours hid a cancellable new expense claim: %, %',
      v_authority,v_contract_result;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,r2_nurse_key,r2_auth_key,booking_id,occupant_key_norm,
    hospital_norm,ward_norm,job_title_norm
  ) values(
    v_carrier,v_contract,current_date,'EXPENSES','ELECTRONIC','WEEKLY',
    'verification/expense-cancel-carrier','verification/expense-cancel-carrier-manager',
    'EXPENSE-CANCEL-CARRIER-'||v_carrier::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    total_hours,processing_status
  ) values(
    v_carrier,1,v_candidate,v_client,true,'CONTRACT_WEEKLY',0,'PENDING_AUTH'
  );
  update public.candidate_submission_workflows
  set target_timesheet_id=v_carrier,state='FINALISED',generation=2
  where id=v_workflow;
  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_EXPENSE',
    'state','FINALISED','generation',2,
    'detail_action_owner',true,'updated_at_utc',now()
  ));
  v_authority:=private._candidate_workflow_cancel_authority_v1(v_workflow);
  v_contract_result:=private._candidate_timesheet_action_contract_v1(
    'AUTHORISED',v_workflows,'{}'::jsonb,v_anchor,v_week,now()
  );
  if not coalesce((v_authority->>'eligible')::boolean,false)
     or not exists(
       select 1
       from jsonb_array_elements(v_contract_result->'available_actions') action_row
       where action_row->>'code'='DISCARD_EXPENSE_CLAIM'
     ) then
    raise exception 'Unauthorised finalised expense carrier was not cancellable: %, %',
      v_authority,v_contract_result;
  end if;

  update public.timesheets
  set authorised_at_server=now()
  where timesheet_id=v_carrier;
  update public.timesheets_financials
  set authorised_at_utc=now()
  where timesheet_id=v_carrier and is_current=true;
  v_authority:=private._candidate_workflow_cancel_authority_v1(v_workflow);
  v_contract_result:=private._candidate_timesheet_action_contract_v1(
    'AUTHORISED',v_workflows,'{}'::jsonb,v_anchor,v_week,now()
  );
  if coalesce((v_authority->>'eligible')::boolean,true)
     or v_authority->>'reason_code'<>'CANDIDATE_WORKFLOW_OFFICE_AUTHORISED'
     or exists(
       select 1
       from jsonb_array_elements(v_contract_result->'available_actions') action_row
       where action_row->>'code'='DISCARD_EXPENSE_CLAIM'
     ) then
    raise exception 'Authorised expense carrier remained cancellable: %, %',
      v_authority,v_contract_result;
  end if;

  begin
    perform public.candidate_workflow_cancel_atomic_v2(
      null,'TEST',v_workflow,2,
      jsonb_build_object('reason_note','Protected carrier verification'),
      gen_random_uuid()::text,now()
    );
  exception when sqlstate '55000' then
    if sqlerrm='CANDIDATE_WORKFLOW_NOT_CANCELLABLE' then
      v_rejected:=true;
    else
      raise;
    end if;
  end;
  if not v_rejected then
    raise exception 'Direct cancellation did not fail closed for authorised carrier';
  end if;
end;
$verification$;

rollback;
