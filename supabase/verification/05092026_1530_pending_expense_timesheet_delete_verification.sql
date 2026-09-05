\set ON_ERROR_STOP on

-- Rollback-contained proof for the Office Timesheet deletion rule:
-- an unfinished standalone expense claim has no Timesheet of its own, follows
-- a safe Timesheet ID rotation, and is cancelled atomically with the worked
-- Timesheet.  A manager-approved or already materialised expense is excluded.
begin;

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_carrier_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_anchor uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_context jsonb;
  v_signature text;
  v_result jsonb;
  v_operation uuid:=gen_random_uuid();
  v_notification uuid;
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(
    v_actor,'pending-expense-delete-actor-'||v_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true
  );

  insert into public.clients(id,name)
  values(v_client,'Pending Expense Timesheet Delete Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'pending-expense-delete-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_anchor,v_contract,current_date,'HOURS','MANUAL','DAILY',
    'PENDING-EXPENSE-DELETE-'||v_anchor::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_carrier_week,v_contract,current_date,1,'PLANNED','ELECTRONIC',null);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values(
    v_account,'TEST','pending-expense-delete-'||v_candidate::text||'@example.test',
    'ACTIVE','{}'::jsonb
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,
    input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_carrier_week,v_anchor,
    null,current_date,'{}','{}','pending-expense-timesheet-delete-verification'
  );

  v_context:=public.timesheet_pending_expense_delete_preview_v1(
    'TEST',array[v_anchor]
  );
  if v_context->>'contract_version'<>'TIMESHEET_PENDING_EXPENSE_DELETE_CONTEXT_V1'
     or coalesce((v_context->>'pending_expense_claim_count')::integer,0)<>1
     or coalesce((v_context->>'cancellation_required')::boolean,false) is not true
     or coalesce((v_context->>'delete_blocked')::boolean,true) is not false
     or v_context#>>'{pending_expense_claims,0,workflow_id}'<>v_workflow::text then
    raise exception 'Unapproved target-less expense was not selected exactly once: %',v_context;
  end if;

  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_anchor,null,false
  )->>'backend_row_signature';
  if nullif(v_signature,'') is null then
    raise exception 'Delete verification signature was not available';
  end if;

  v_result:=public.timesheet_delete_with_pending_expense_apply_v1(
    'TEST','STANDARD_DELETE',v_anchor,v_actor,v_anchor,v_signature,
    array[v_anchor],array[]::uuid[],array[]::uuid[],array[]::uuid[],array[]::uuid[],
    v_context->>'context_sha256',v_operation,now()
  );
  if coalesce((v_result->>'apply_performed')::boolean,false) is not true
     or v_result->>'decision'<>'PERMANENT_DELETE'
     or coalesce((v_result->>'cancelled_pending_expense_claim_count')::integer,0)<>1
     or v_result#>>'{cancelled_pending_expense_claims,0,workflow_id}'<>v_workflow::text
     or v_result#>>'{cancelled_pending_expense_claims,0,state}'<>'CANCELLED'
     or coalesce((v_result#>>'{banking_pay_candidate_refresh,ok}')::boolean,false) is not true
     or coalesce((v_result#>>'{banking_pay_candidate_refresh,targeted_timesheet_count}')::integer,-1)<>0 then
    raise exception 'Atomic Timesheet/expense cancellation did not return exact proof: %',v_result;
  end if;
  if exists(select 1 from public.timesheets where timesheet_id=v_anchor) then
    raise exception 'Worked Timesheet still existed after committed delete';
  end if;
  if not exists(
    select 1
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=v_workflow
      and workflow_row.state='CANCELLED'
      and workflow_row.generation=2
      and workflow_row.anchor_timesheet_id is null
      and workflow_row.target_timesheet_id is null
      and workflow_row.issue_codes @> '["OFFICE_TIMESHEET_DELETED_EXPENSE_CANCELLED"]'::jsonb
  ) then
    raise exception 'Pending expense was not cancelled and detached exactly';
  end if;
  select notification.id into strict v_notification
  from public.candidate_notifications notification
  where notification.account_id=v_account
    and notification.candidate_id=v_candidate
    and notification.event_type='EXPENSE_CLAIM_CANCELLED'
    and notification.state='UNREAD'
    and notification.push_state='PENDING'
    and notification.timesheet_id is null;
  if v_result#>>'{candidate_notification_ids,0}'<>v_notification::text then
    raise exception 'Returned Candidate notification identity did not match the queued row';
  end if;
  if (select count(*) from public.candidate_notifications
      where dedupe_key='CANDIDATE_EXPENSE_CLAIM_CANCELLED_TIMESHEET_DELETE_V1:'
        ||v_workflow::text||':1')<>1 then
    raise exception 'Cancellation notification was not one-only';
  end if;
end;
$verification$;

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_carrier_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_old_anchor uuid:=gen_random_uuid();
  v_new_anchor uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_expense_timesheet uuid:=gen_random_uuid();
  v_context_before jsonb;
  v_context_after jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Pending Expense Rotation Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'pending-expense-rotation-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    version,is_current
  ) values(
    v_old_anchor,v_contract,current_date,'HOURS','MANUAL','DAILY',
    'PENDING-EXPENSE-ROTATION-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',1,true
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_carrier_week,v_contract,current_date,1,'PLANNED','ELECTRONIC',null);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    v_account,'TEST','pending-expense-rotation-'||v_candidate::text||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,
    input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_carrier_week,v_old_anchor,
    null,current_date,'{}','{}','pending-expense-rotation-verification'
  );
  v_context_before:=public.timesheet_pending_expense_delete_preview_v1(
    'TEST',array[v_old_anchor]
  );

  update public.timesheets set is_current=false where timesheet_id=v_old_anchor;
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    version,is_current
  )
  select
    v_new_anchor,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    2,true
  from public.timesheets
  where timesheet_id=v_old_anchor;

  v_context_after:=public.timesheet_pending_expense_delete_preview_v1(
    'TEST',array[v_new_anchor]
  );
  if (select anchor_timesheet_id from public.candidate_submission_workflows where id=v_workflow)
       is distinct from v_new_anchor
     or v_context_after#>>'{pending_expense_claims,0,workflow_id}'<>v_workflow::text
     or v_context_before->>'context_sha256'=v_context_after->>'context_sha256' then
    raise exception 'Pending expense did not follow the exact current Timesheet rotation safely: %, %',
      v_context_before,v_context_after;
  end if;

  update public.candidate_submission_workflows
  set state='MANAGER_APPROVED'
  where id=v_workflow;
  v_context_after:=public.timesheet_pending_expense_delete_preview_v1(
    'TEST',array[v_new_anchor]
  );
  if coalesce((v_context_after->>'pending_expense_claim_count')::integer,-1)<>0
     or coalesce((v_context_after->>'blocking_claim_count')::integer,0)<>1
     or coalesce((v_context_after->>'delete_blocked')::boolean,false) is not true then
    raise exception 'Manager-approved expense did not block Timesheet deletion: %',v_context_after;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_expense_timesheet,v_contract,current_date,'EXPENSES','MANUAL','WEEKLY',
    'PENDING-EXPENSE-TARGET-'||v_expense_timesheet::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  update public.candidate_submission_workflows
  set state='FINALISED',target_timesheet_id=v_expense_timesheet
  where id=v_workflow;
  v_context_after:=public.timesheet_pending_expense_delete_preview_v1(
    'TEST',array[v_new_anchor]
  );
  if coalesce((v_context_after->>'pending_expense_claim_count')::integer,-1)<>0
     or coalesce((v_context_after->>'blocking_claim_count')::integer,-1)<>0 then
    raise exception 'Expense with its own Timesheet was still attached to the worked Timesheet delete: %',
      v_context_after;
  end if;
end;
$verification$;

do $verification$
begin
  if has_function_privilege('anon',
       'public.timesheet_pending_expense_delete_preview_v1(text,uuid[])','EXECUTE')
     or has_function_privilege('authenticated',
       'public.timesheet_pending_expense_delete_preview_v1(text,uuid[])','EXECUTE')
     or has_function_privilege('anon',
       'public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE')
     or has_function_privilege('authenticated',
       'public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE') then
    raise exception 'Browser roles can execute the pending-expense Timesheet delete authority';
  end if;
  if not has_function_privilege('service_role',
       'public.timesheet_pending_expense_delete_preview_v1(text,uuid[])','EXECUTE')
     or not has_function_privilege('service_role',
       'public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE') then
    raise exception 'Service role cannot execute the pending-expense Timesheet delete authority';
  end if;
end;
$verification$;

rollback;
