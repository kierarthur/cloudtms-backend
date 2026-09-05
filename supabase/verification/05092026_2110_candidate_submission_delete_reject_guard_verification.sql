\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the Office reject-before-delete rule.
-- A submitted/manager-approved Electronic or QR Candidate workflow prevents
-- deletion.  A target-less pending expense follows the same worked Timesheet
-- across Timesheet ID rotation and is rejected in the same transaction.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_reads":true,"candidate_app_writes":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_old_timesheet uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_guard jsonb;
  v_preview jsonb;
  v_signature text;
  v_result jsonb;
  v_new_timesheet uuid;
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(
    v_actor,'candidate-delete-reject-'||v_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true
  );
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_actor
  where id=1;

  insert into public.clients(id,name)
  values(v_client,'Candidate Delete Reject Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(
    v_candidate,'candidate-delete-reject-'||v_candidate::text||'@example.test',true
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-14,current_date+14,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );

  -- The expense deliberately points to the previous Timesheet ID.  The
  -- current row retains the same stable work identity.
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,r2_nurse_key,r2_auth_key,version,is_current
  ) values(
    v_old_timesheet,v_contract,current_date,'HOURS','ELECTRONIC','WEEKLY',
    'CANDIDATE-DELETE-REJECT-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',
    'verification/old/candidate-signature','verification/old/manager-signature',1,false
  ),(
    v_timesheet,v_contract,current_date,'HOURS','ELECTRONIC','WEEKLY',
    'CANDIDATE-DELETE-REJECT-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',
    'verification/current/candidate-signature','verification/current/manager-signature',2,true
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,
    processing_status,total_hours
  ) values(v_timesheet,2,v_candidate,v_client,true,'UNPROCESSED',8);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values(
    v_account,'TEST','candidate-delete-reject-'||v_candidate::text||'@example.test',
    'ACTIVE','{}'::jsonb
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,input_snapshot_json,
    idempotency_key
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'ELECTRONIC','MANAGER_APPROVED',1,v_contract,v_week,v_timesheet,
    v_timesheet,current_date,'{}','{}','candidate-delete-reject-hours'
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_week,v_old_timesheet,
    null,current_date,'{}','{}','candidate-delete-reject-expense'
  );

  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_timesheet]
  );
  if v_guard->>'contract_version'<>'TIMESHEET_CANDIDATE_SUBMISSION_DELETE_GUARD_V1'
     or coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or v_guard->>'candidate_submission_stage'<>'MANAGER_APPROVED'
     or coalesce((v_guard->>'guarded_workflow_count')::integer,0)<>2
     or coalesce((v_guard->>'linked_pending_expense_claim_count')::integer,0)<>1
     or not exists(
       select 1 from jsonb_array_elements(v_guard->'guarded_workflows') item
       where item->>'workflow_id'=v_expense_workflow::text
         and item->>'link_kind'='ROTATED_ANCHOR'
         and coalesce((item->>'linked_pending_expense')::boolean,false)
     ) then
    raise exception 'Candidate delete guard did not include the rotated linked expense: %',v_guard;
  end if;

  -- Weekly parent deletion supplies every Timesheet version in the booking
  -- chain.  The historical version has no Contract Week of its own; it must
  -- inherit the current Electronic route without losing or duplicating either
  -- protected workflow.
  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_old_timesheet,v_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or v_guard->>'candidate_submission_stage'<>'MANAGER_APPROVED'
     or coalesce((v_guard->>'guarded_workflow_count')::integer,0)<>2
     or coalesce((v_guard->>'linked_pending_expense_claim_count')::integer,0)<>1
     or (select count(*) from jsonb_array_elements(v_guard->'guarded_workflows') item
         where item->>'workflow_id'=v_expense_workflow::text)<>1 then
    raise exception 'Candidate delete guard did not resolve the full rotated Timesheet chain: %',v_guard;
  end if;

  update public.candidate_submission_workflows
  set state='WORKER_DRAFT'
  where id=v_expense_workflow;
  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or coalesce((v_guard->>'guarded_workflow_count')::integer,0)<>1
     or coalesce((v_guard->>'linked_pending_expense_claim_count')::integer,0)<>1 then
    raise exception 'Draft linked expense was omitted from the delete warning: %',v_guard;
  end if;
  update public.candidate_submission_workflows
  set state='READY_FOR_MANAGER_APPROVAL'
  where id=v_expense_workflow;

  begin
    perform public.timesheet_delete_with_candidate_submission_guard_apply_v1(
      'TEST','STANDARD_DELETE',v_timesheet,v_actor,v_timesheet,repeat('0',64),
      array[v_timesheet],array[v_week],array[]::uuid[],array[]::uuid[],
      array[]::uuid[],repeat('0',64),gen_random_uuid(),now()
    );
    raise exception 'Delete unexpectedly passed a Candidate submission';
  exception
    when sqlstate '55000' then
      if sqlerrm not like '%CANDIDATE_SUBMISSION_REJECTION_REQUIRED%' then
        raise;
      end if;
  end;
  if not exists(select 1 from public.timesheets where timesheet_id=v_timesheet)
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'READY_FOR_MANAGER_APPROVAL' then
    raise exception 'Delete refusal changed the Timesheet or pending expense';
  end if;

  v_preview:=private._candidate_office_reject_preview_v1(
    'TEST',v_timesheet,v_actor,now()
  );
  if coalesce((v_preview->>'permitted')::boolean,false) is not true
     or coalesce((v_preview->>'linked_pending_expense_claim_count')::integer,0)<>1
     or jsonb_array_length(v_preview->'target_workflows')<>2 then
    raise exception 'Reject preview did not warn about the linked expense: %',v_preview;
  end if;

  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_timesheet,v_week,false
  )->>'row_signature';
  v_result:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_timesheet,v_timesheet,v_signature,
    'Verification rejection with linked pending expense',
    'candidate-delete-reject:reject',now()
  );
  v_new_timesheet:=nullif(v_result->>'timesheet_id','')::uuid;
  if v_new_timesheet is null or v_new_timesheet=v_timesheet
     or coalesce((v_result->>'linked_pending_expense_claim_count')::integer,0)<>1
     or (select state from public.candidate_submission_workflows
         where id=v_hours_workflow)<>'REJECTED'
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'REJECTED'
     or (select rejection_scope from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'COMPLETE_EXPENSE_CLAIM'
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_hours_workflow
           and event_type='OFFICE_REJECTED'
           and template_key='candidate-office-rejected-v1'
           and timesheet_id=v_new_timesheet)<>1
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_expense_workflow
           and event_type='EXPENSE_CLAIM_CANCELLED'
           and preference_category='timesheet_expense_attention'
           and template_key='candidate-expense-claim-cancelled-timesheet-rejection-v1'
           and template_params->>'reason_code'='LINKED_TIMESHEET_REJECTED_FOR_DELETE'
           and deep_link_json->>'type'='workflow'
           and deep_link_json->>'workflow_id'=v_expense_workflow::text
           and timesheet_id is null)<>1 then
    raise exception 'Atomic rejection did not reject both workflows exactly: %',v_result;
  end if;

  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_new_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,true)
     or coalesce((v_guard->>'guarded_workflow_count')::integer,-1)<>0 then
    raise exception 'Rejected workflows still prevented deletion: %',v_guard;
  end if;
end;
$verification$;

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_manual_timesheet uuid:=gen_random_uuid();
  v_expense_timesheet uuid:=gen_random_uuid();
  v_qr_timesheet uuid:=gen_random_uuid();
  v_manual_week uuid:=gen_random_uuid();
  v_expense_week uuid:=gen_random_uuid();
  v_qr_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_qr_workflow uuid:=gen_random_uuid();
  v_guard jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate Delete Exclusion Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'MANUAL',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'candidate-delete-exclusion-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_manual_timesheet,v_contract,current_date,'HOURS','MANUAL','WEEKLY',
    'MANUAL-DELETE-EXCLUSION-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  ),(
    v_expense_timesheet,v_contract,current_date,'EXPENSES','MANUAL','WEEKLY',
    'CANDIDATE-EXPENSE-CARRIER-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  ),(
    v_qr_timesheet,v_contract,current_date,'HOURS','MANUAL','WEEKLY',
    'CANDIDATE-QR-DELETE-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  update public.timesheets set qr_status='PENDING' where timesheet_id=v_qr_timesheet;
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_manual_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_manual_timesheet
  ),(
    v_expense_week,v_contract,current_date,1,'SUBMITTED','MANUAL',v_expense_timesheet
  ),(
    v_qr_week,v_contract,current_date,2,'SUBMITTED','MANUAL',v_qr_timesheet
  );
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    v_account,'TEST','candidate-delete-exclusion-'||v_candidate::text||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_manual_week,v_manual_timesheet,
    v_manual_timesheet,current_date,'manual-delete-exclusion-hours'
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_expense_week,v_expense_timesheet,
    v_expense_timesheet,current_date,'manual-shaped-candidate-expense'
  ),(
    v_qr_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'PAPER','AWAITING_PAPER_RETURN',1,v_contract,v_qr_week,v_qr_timesheet,
    v_qr_timesheet,current_date,'candidate-qr-delete-hours'
  );

  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_manual_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,true)
     or coalesce((v_guard->>'guarded_workflow_count')::integer,-1)<>0 then
    raise exception 'Manual non-QR hours were incorrectly put behind Candidate rejection: %',v_guard;
  end if;

  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_expense_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or v_guard#>>'{guarded_workflows,0,workflow_id}'<>v_expense_workflow::text then
    raise exception 'Candidate-created expense carrier was not protected: %',v_guard;
  end if;

  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_qr_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or v_guard#>>'{guarded_workflows,0,workflow_id}'<>v_qr_workflow::text
     or v_guard#>>'{guarded_workflows,0,candidate_submission_stage}'<>'CANDIDATE_SUBMITTED' then
    raise exception 'QR Candidate submission was not protected: %',v_guard;
  end if;
end;
$verification$;

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_guard jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate Delete NHSP Exclusion Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,is_nhsp
  ) values(
    gen_random_uuid(),v_client,current_date-7,'MANUAL',
    extract(dow from current_date)::integer,true
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'candidate-delete-nhsp-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode,weekly_timesheet_source
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'MANUAL','NHSP'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_timesheet,v_contract,current_date,'HOURS','MANUAL','WEEKLY',
    'NHSP-DELETE-EXCLUSION-'||v_contract::text,v_candidate::text,
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    v_account,'TEST','candidate-delete-nhsp-'||v_candidate::text||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,
    v_timesheet,current_date,'nhsp-delete-exclusion-hours'
  );
  v_guard:=public.timesheet_candidate_submission_delete_guard_preview_v1(
    'TEST',array[v_timesheet]
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,true)
     or coalesce((v_guard->>'guarded_workflow_count')::integer,-1)<>0 then
    raise exception 'Import-authoritative NHSP hours were incorrectly gated: %',v_guard;
  end if;
end;
$verification$;

do $verification$
begin
  if has_function_privilege(
       'anon','public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[])','EXECUTE'
     ) or has_function_privilege(
       'authenticated','public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[])','EXECUTE'
     ) or has_function_privilege(
       'anon','public.timesheet_delete_with_candidate_submission_guard_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE'
     ) or has_function_privilege(
       'authenticated','public.timesheet_delete_with_candidate_submission_guard_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Browser roles can execute Candidate delete guard authority';
  end if;
  if not has_function_privilege(
       'service_role','public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[])','EXECUTE'
     ) or not has_function_privilege(
       'service_role','public.timesheet_delete_with_candidate_submission_guard_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Service role cannot execute Candidate delete guard authority';
  end if;
end;
$verification$;

rollback;
