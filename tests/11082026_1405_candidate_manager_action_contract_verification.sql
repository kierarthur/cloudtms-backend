-- Provider-accepted reminder timing, renewal separation, reasoned cancellation and detail actions.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_manager_approval',true,
    'candidate_record_role_capabilities',true
  )
where id=1;

do $manager_action_contract$
declare
  v_now timestamptz:='2026-08-11 12:00:00+00';
  v_client uuid:='ac160000-0000-0000-0000-000000000001';
  v_candidate uuid:='ac160000-0000-0000-0000-000000000002';
  v_contract uuid:='ac160000-0000-0000-0000-000000000003';
  v_timesheet uuid:='ac160000-0000-0000-0000-000000000004';
  v_week uuid:='ac160000-0000-0000-0000-000000000005';
  v_account uuid:='ac160000-0000-0000-0000-000000000006';
  v_session uuid:='ac160000-0000-0000-0000-000000000007';
  v_workflow uuid:='ac160000-0000-0000-0000-000000000008';
  v_request uuid:='ac160000-0000-0000-0000-000000000009';
  v_component uuid:='ac160000-0000-0000-0000-000000000010';
  v_result jsonb;
  v_detail jsonb;
  v_reminder_mail uuid;
  v_initial_token_hash bytea:=extensions.digest('initial-manager-token','sha256');
  v_reminder_token_hash bytea:=extensions.digest('reminder-manager-token','sha256');
  v_count integer;
  v_mail_count integer;
  v_cancel_payload jsonb:=jsonb_build_object(
    'reason_note','I entered the wrong week.',
    'manager_terminal_mail',jsonb_build_object(
      'subject','Timesheet approval request cancelled',
      'body_text','The approval request for this timesheet has been cancelled. No further action is required.',
      'body_html','<p>The approval request for this timesheet has been cancelled. No further action is required.</p>',
      'manager_template_version',1,
      'manager_template_sha256',repeat('e1',32),
      'manager_submission_type','TIMESHEET'
    )
  );
begin
  insert into public.clients(id,name) values(v_client,'Manager action contract client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(gen_random_uuid(),v_client,current_date-30,'ELECTRONIC',2);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'manager-action@example.test',true,'MANAGER-ACTION-CANDIDATE');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode
  ) values(v_timesheet,v_contract,current_date,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','manager-action@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('16',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,review_manifest_sha256
  ) values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'manager-action-workflow',extensions.digest('manager-action-manifest','sha256'));
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,review_render_state,final_signed_render_state
  ) values(v_component,v_workflow,1,1,v_timesheet,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
    'IMMUTABLE','candidate/manager-action/signature.png','image/png',100,
    extensions.digest('manager-action-signature','sha256'),v_now,false,'NOT_REQUIRED','NOT_REQUIRED');
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(v_request,v_workflow,1,1,'EMAIL','PENDING','manager@example.test',
    v_initial_token_hash,v_now+interval '7 days',extensions.digest('manager-action-manifest','sha256'),
    array[v_component],jsonb_build_array(jsonb_build_object('component_id',v_component)),
    'manager-action-request');
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,sent_at,provider_status,provider_message_id
  ) values(
    gen_random_uuid(),'TIMESHEET_GENERAL','manager@example.test','Approval required','Review.',
    'SENT','CANDIDATE_WORKFLOW',v_workflow,'MANAGER-ACTION-INITIAL',jsonb_build_object(
      'candidate_mail_authority','MANAGER_APPROVAL_V1','candidate_manager_mail_kind','INITIAL',
      'candidate_manager_workflow_id',v_workflow,'candidate_manager_workflow_generation',1,
      'candidate_approval_request_id',v_request,'candidate_approval_request_generation',1,
      'candidate_manager_mail_retired',false
    ),v_now-interval '24 hours','ACCEPTED','manager-action-initial'
  );

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'REMIND',1,
    jsonb_build_object(
      'approval_request_id',v_request,
      'approval_request_generation',1,
      'approval_token_hash_hex',encode(v_reminder_token_hash,'hex'),
      'mail',jsonb_build_object('subject','Reminder','body_text','Please review.')
    ),'manager-action-remind',v_now
  );
  v_reminder_mail:=(v_result->>'mail_outbox_id')::uuid;
  if v_result->>'approval_request_id'<>v_request::text
     or (v_result->>'resend_count')::integer<>1
     or (select token_hash from public.candidate_approval_requests where id=v_request)
          is distinct from v_reminder_token_hash
     or (select payment_scope_json->>'candidate_manager_mail_kind'
         from public.mail_outbox where id=v_reminder_mail)<>'REMINDER' then
    raise exception 'REMIND did not preserve and rotate the current request: %',v_result;
  end if;
  select count(*) into v_mail_count
  from public.mail_outbox mail
  where mail.context_kind='CANDIDATE_WORKFLOW' and mail.context_id=v_workflow;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'REMIND',1,
    jsonb_build_object(
      'approval_request_id',v_request,
      'approval_request_generation',1,
      'approval_token_hash_hex',repeat('fe',32),
      'mail',jsonb_build_object(
        'subject','A newly generated subject for a lost-response retry',
        'body_text','A newly generated body must not change the semantic request.'
      )
    ),'manager-action-remind',v_now+interval '1 second'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or (v_result->>'mail_outbox_id')::uuid<>v_reminder_mail
     or (v_result->>'resend_count')::integer<>1
     or (select token_hash from public.candidate_approval_requests where id=v_request)
          is distinct from v_reminder_token_hash
     or (select count(*) from public.mail_outbox mail
         where mail.context_kind='CANDIDATE_WORKFLOW' and mail.context_id=v_workflow)<>v_mail_count then
    raise exception 'REMIND semantic replay depended on generated token or mail content: %',v_result;
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'REMIND',1,
      jsonb_build_object(
        'approval_request_id',v_request,
        'approval_request_generation',1,
        'approval_token_hash_hex',repeat('17',32),
        'mail',jsonb_build_object('subject','Duplicate reminder','body_text','Do not queue.')
      ),'manager-action-remind-too-soon',v_now
    );
    raise exception 'pending reminder did not fence a second reminder';
  exception when sqlstate '55000' then
    if sqlerrm<>'MANAGER_REMINDER_NOT_ELIGIBLE' then raise; end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'RENEW',1,
      jsonb_build_object(
        'approval_request_id',v_request,
        'approval_request_generation',1,
        'approval_token_hash_hex',repeat('18',32),
        'mail',jsonb_build_object('subject','Renewal','body_text','Do not queue.')
      ),'manager-action-renew-live',v_now
    );
    raise exception 'live PENDING request was renewed';
  exception when sqlstate '55000' then
    if sqlerrm<>'MANAGER_APPROVAL_NOT_RENEWABLE' then raise; end if;
  end;

  update public.mail_outbox set
    status='SENT',sent_at=v_now,provider_status='ACCEPTED',provider_message_id='manager-action-reminder'
  where id=v_reminder_mail;
  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',v_timesheet,null,null,v_now
  );
  if v_detail#>>'{manager_approval,request_id}'<>v_request::text
     or v_detail#>>'{manager_approval,provider_accepted_at_utc}' is null
     or (v_detail#>>'{manager_approval,resends_remaining}')::integer<>4
     or coalesce((v_detail#>>'{manager_approval,reminder_eligible}')::boolean,true)
     or coalesce((v_detail#>>'{manager_approval,renewal_eligible}')::boolean,true)
     or not (v_detail->'available_actions' @> jsonb_build_array(
       jsonb_build_object('code','SEND_MANAGER_REMINDER','enabled',false)
     ))
     or not (v_detail->'available_actions' @> jsonb_build_array(
       jsonb_build_object('code','CANCEL_ENTIRE_CLAIM_AND_START_AGAIN','requires_reason',true)
     )) then
    raise exception 'detail manager/action contract is incomplete: %',v_detail;
  end if;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'CANCEL',1,'{}'::jsonb,
      'manager-action-cancel-missing-reason',v_now+interval '1 minute'
    );
    raise exception 'reasonless cancellation succeeded';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_CANCELLATION_REASON_REQUIRED' then raise; end if;
  end;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CANCEL',1,
    v_cancel_payload,
    'manager-action-cancel',v_now+interval '1 minute'
  );
  if v_result->>'state'<>'CANCELLED'
     or v_result->>'cancellation_reason'<>'I entered the wrong week.'
     or (v_result->>'manager_withdrawal_count')::integer<>1 then
    raise exception 'reasoned cancellation result incomplete: %',v_result;
  end if;
  select count(*) into v_count from public.mail_outbox
  where deterministic_outbox_key='CANDIDATE_MANAGER_CANCELLATION_V1:'||v_request::text||':1'
    and payment_scope_json->>'candidate_manager_mail_kind'='CANCELLATION';
  if v_count<>1 then raise exception 'accepted manager mail did not create one cancellation notice'; end if;
  if not exists(
    select 1 from public.audit_events
    where object_type='candidate_submission_workflow'
      and object_id_text=v_workflow::text
      and action='CANDIDATE_WORKFLOW_CANCEL'
      and reason='I entered the wrong week.'
  ) then raise exception 'cancellation reason was not audited'; end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CANCEL',1,
    v_cancel_payload,
    'manager-action-cancel',v_now+interval '2 minutes'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false) then
    raise exception 'cancellation replay was not idempotent: %',v_result;
  end if;
  select count(*) into v_count from public.mail_outbox
  where deterministic_outbox_key='CANDIDATE_MANAGER_CANCELLATION_V1:'||v_request::text||':1';
  if v_count<>1 then raise exception 'cancellation replay duplicated the cancellation notice'; end if;
end;
$manager_action_contract$;

rollback;
