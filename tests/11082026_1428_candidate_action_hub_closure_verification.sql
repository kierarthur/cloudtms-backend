-- Rejected replacement actions, exact card identity and durable PAPER readiness.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_paper_qr',true,
    'candidate_record_role_capabilities',true
  )
where id=1;

do $candidate_action_hub$
declare
  v_now timestamptz:='2026-08-11 14:28:00+00';
  v_client uuid:='ac170000-0000-0000-0000-000000000001';
  v_candidate uuid:='ac170000-0000-0000-0000-000000000002';
  v_contract uuid:='ac170000-0000-0000-0000-000000000003';
  v_account uuid:='ac170000-0000-0000-0000-000000000004';
  v_session uuid:='ac170000-0000-0000-0000-000000000005';
  v_timesheet_1 uuid:='ac170000-0000-0000-0000-000000000011';
  v_timesheet_2 uuid:='ac170000-0000-0000-0000-000000000012';
  v_timesheet_3 uuid:='ac170000-0000-0000-0000-000000000013';
  v_week_1 uuid:='ac170000-0000-0000-0000-000000000021';
  v_week_2 uuid:='ac170000-0000-0000-0000-000000000022';
  v_week_3 uuid:='ac170000-0000-0000-0000-000000000023';
  v_hours_rejected uuid:='ac170000-0000-0000-0000-000000000031';
  v_expense_rejected uuid:='ac170000-0000-0000-0000-000000000032';
  v_other_workflow uuid:='ac170000-0000-0000-0000-000000000033';
  v_paper_workflow uuid:='ac170000-0000-0000-0000-000000000034';
  v_mail uuid:='ac170000-0000-0000-0000-000000000041';
  v_manifest text:=encode(extensions.digest('action-hub-paper-manifest','sha256'),'hex');
  v_pack_sha text:=encode(extensions.digest('action-hub-paper-pack','sha256'),'hex');
  v_detail jsonb;
  v_action_count integer;
begin
  insert into public.clients(id,name) values(v_client,'Candidate action hub client');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode,week_ending_weekday)
  values(gen_random_uuid(),v_client,current_date-60,'ELECTRONIC',2);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'action-hub@example.test',true,'ACTION-HUB-CANDIDATE');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-90,current_date+30,2,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,booking_id,
    qr_status,qr_token
  ) values
    (v_timesheet_1,v_contract,current_date,'HOURS','MANUAL','ACTION-HUB-BOOKING-1',null,null),
    (v_timesheet_2,v_contract,current_date,'HOURS','MANUAL','ACTION-HUB-BOOKING-2',null,null),
    (v_timesheet_3,v_contract,current_date-7,'HOURS','MANUAL','ACTION-HUB-BOOKING-3','PENDING','action-hub-token');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_week_1,v_contract,current_date,0,'OPEN','ELECTRONIC',v_timesheet_1),
    (v_week_2,v_contract,current_date,1,'OPEN','ELECTRONIC',v_timesheet_2),
    (v_week_3,v_contract,current_date-7,0,'SUBMITTED','MANUAL',v_timesheet_3);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values
    (v_timesheet_1,v_candidate,v_client,8,'UNPROCESSED'),
    (v_timesheet_2,v_candidate,v_client,8,'UNPROCESSED'),
    (v_timesheet_3,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','action-hub@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('17',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,rejection_reason,rejection_scope,updated_at_utc
  ) values
    (v_hours_rejected,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
      'REJECTED',2,v_contract,v_week_1,v_timesheet_1,v_timesheet_1,current_date,
      'action-hub-hours-rejected','Hours need correcting','COMPLETE_TIMESHEET_RECORD',v_now-interval '2 hours'),
    (v_expense_rejected,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
      'REJECTED',2,v_contract,v_week_1,v_timesheet_1,null,current_date,
      'action-hub-expense-rejected','Receipts are incomplete','COMPLETE_EXPENSE_CLAIM',v_now-interval '1 hour'),
    (v_other_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
      'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week_2,v_timesheet_2,v_timesheet_2,current_date,
      'action-hub-other-workflow',null,null,v_now),
    (v_paper_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
      'AWAITING_PAPER_RETURN',1,v_contract,v_week_3,v_timesheet_3,v_timesheet_3,current_date-7,
      'action-hub-paper-workflow',null,null,v_now);
  update public.candidate_submission_workflows
  set paper_return_manifest_sha256=decode(v_manifest,'hex')
  where id=v_paper_workflow;

  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',null,null,v_hours_rejected,v_now
  );
  if jsonb_array_length(v_detail->'workflows')<>2
     or (v_detail->'workflows') @> jsonb_build_array(jsonb_build_object('workflow_id',v_other_workflow))
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','RESUBMIT_TIMESHEET','workflow_id',v_hours_rejected,
         'path','/candidate-app/v1/workflows/'||v_hours_rejected::text||'/resubmit')))
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','RESUBMIT_EXPENSE_CLAIM','workflow_id',v_expense_rejected,
         'path','/candidate-app/v1/workflows/'||v_expense_rejected::text||'/resubmit'))) then
    raise exception 'exact card/rejection action contract failed: %',v_detail;
  end if;
  select count(*) into v_action_count
  from jsonb_array_elements(v_detail->'available_actions') action
  where action->>'code' like 'RESUBMIT_%'
    and action#>>'{invocation,kind}'='HTTP'
    and action#>>'{invocation,idempotency}'='REQUIRED';
  if v_action_count<>2 then
    raise exception 'independent rejection actions were collapsed: %',v_detail->'available_actions';
  end if;

  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,attachments
  ) values(
    v_mail,'TIMESHEET_QR','action-hub@example.test','Paper pack','Documents','QUEUED',
    'timesheets',v_timesheet_3,'ACTION-HUB-PAPER-MAIL',jsonb_build_object(
      'candidate_workflow_id',v_paper_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest,'candidate_paper_pack_ready',false,
      'mail_held_until_pdf_rendered',true,'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
      'candidate_paper_generation_retired',false
    ),'[]'::jsonb
  );
  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',null,null,v_paper_workflow,v_now
  );
  if v_detail#>>'{paper_pack,state}'<>'PREPARING'
     or v_detail->>'candidate_status_code'<>'PREPARING_DOCUMENTS'
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','DOWNLOAD_PAPER_DOCUMENTS','enabled',false,
         'disabled_reason','CANDIDATE_PAPER_PACK_PREPARING')))
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','UPLOAD_SIGNED_RETURN','enabled',false,
         'disabled_reason','CANDIDATE_PAPER_PACK_PREPARING'))) then
    raise exception 'preparing paper actions were enabled: %',v_detail;
  end if;

  update public.mail_outbox
  set attachments=jsonb_build_array(jsonb_build_object(
        'r2_key','candidate-app/action-hub/complete.pdf','sha256',v_pack_sha,
        'size_bytes',512,'page_count',3,'content_type','application/pdf',
        'candidate_workflow_id',v_paper_workflow,'candidate_workflow_generation',1,
        'paper_return_manifest_sha256',v_manifest
      )),
      payment_scope_json=payment_scope_json||jsonb_build_object(
        'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
        'mail_hold_reason',null,'candidate_complete_pack_storage_key','candidate-app/action-hub/complete.pdf',
        'candidate_complete_pack_sha256',v_pack_sha,'candidate_complete_pack_size_bytes',512,
        'candidate_complete_pack_page_count',3,'candidate_complete_pack_media_type','application/pdf'
      )
  where id=v_mail;
  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',null,null,v_paper_workflow,v_now
  );
  if v_detail#>>'{paper_pack,state}'<>'READY'
     or v_detail->>'candidate_status_code'<>'AWAITING_SIGNED_DOCUMENTS'
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','DOWNLOAD_PAPER_DOCUMENTS','enabled',true)))
     or not ((v_detail->'available_actions') @> jsonb_build_array(
       jsonb_build_object('code','UPLOAD_SIGNED_RETURN','enabled',true))) then
    raise exception 'ready paper actions remained disabled: %',v_detail;
  end if;
end;
$candidate_action_hub$;

rollback;
