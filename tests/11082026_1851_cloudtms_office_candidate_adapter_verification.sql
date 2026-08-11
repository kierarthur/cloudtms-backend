-- Normal CloudTMS office Candidate adapter: ACL, snapshot projection and batch reminder authority.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
  'candidate_app_reads',true,
  'candidate_app_writes',true,
  'candidate_manager_approval',true,
  'candidate_paper_qr',true,
  'candidate_route_confirmation',true,
  'candidate_record_role_capabilities',true
)
where id=1;

do $office_adapter$
declare
  v_now timestamptz:='2026-08-11 12:00:00+00';
  v_actor uuid:='ad510000-0000-4000-8000-000000000001';
  v_client uuid:='ad510000-0000-4000-8000-000000000002';
  v_candidate uuid:='ad510000-0000-4000-8000-000000000003';
  v_contract uuid:='ad510000-0000-4000-8000-000000000004';
  v_timesheet uuid:='ad510000-0000-4000-8000-000000000005';
  v_week uuid:='ad510000-0000-4000-8000-000000000006';
  v_other_timesheet uuid:='ad510000-0000-4000-8000-000000000012';
  v_other_week uuid:='ad510000-0000-4000-8000-000000000013';
  v_account uuid:='ad510000-0000-4000-8000-000000000007';
  v_workflow uuid:='ad510000-0000-4000-8000-000000000008';
  v_request uuid:='ad510000-0000-4000-8000-000000000009';
  v_batch uuid:='ad510000-0000-4000-8000-000000000010';
  v_component uuid:='ad510000-0000-4000-8000-000000000011';
  v_capabilities jsonb;
  v_projection jsonb;
  v_preview jsonb;
  v_result jsonb;
  v_identity jsonb;
  v_reminders jsonb;
  v_execute_payload jsonb;
  v_signature text;
  v_definition text;
  v_count integer;
  v_failed boolean;
begin
  if has_function_privilege('anon','public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz)','execute')
     or has_function_privilege('authenticated','public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz)','execute')
     or not has_function_privilege('service_role','public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz)','execute') then
    raise exception 'office adapter ACL is not service-role-only';
  end if;
  if exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    cross join (values('anon'),('authenticated'),('service_role')) role_name(name)
    where n.nspname='private'
      and p.proname in (
        '_candidate_office_capabilities_v1','_candidate_office_action_v1',
        '_candidate_office_reject_preview_v1','_candidate_office_projection_identity_v1',
        '_candidate_office_timesheet_projection_v1'
      )
      and has_function_privilege(role_name.name,p.oid,'execute')
  ) or exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
    where n.nspname='private'
      and p.proname in (
        '_candidate_office_capabilities_v1','_candidate_office_action_v1',
        '_candidate_office_reject_preview_v1','_candidate_office_projection_identity_v1',
        '_candidate_office_timesheet_projection_v1'
      )
      and acl.grantee=0 and acl.privilege_type='EXECUTE'
  ) then
    raise exception 'office private helper ACL is not closed';
  end if;
  select pg_get_functiondef('private._candidate_office_timesheet_projection_v1(text,uuid,uuid,text,text,uuid,timestamptz)'::regprocedure)
  into v_definition;
  if position('v_workflow.generation-1' in v_definition)=0
     or position('candidate_workflow_generation''=v_paper_delivery_generation::text' in v_definition)=0
     or position('candidate_paper_generation_retired' in v_definition)=0
     or position('''delivery_generation'',v_paper_delivery_generation' in v_definition)=0 then
    raise exception 'office PAPER projection is not bound to the immutable delivery generation';
  end if;

  insert into public.clients(id,name) values(v_client,'Office adapter client');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode,week_ending_weekday)
  values(gen_random_uuid(),v_client,current_date-30,'ELECTRONIC',2);
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'office-adapter@example.test',true,'OFFICE-ADAPTER-CANDIDATE');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC');
  insert into public.timesheets(timesheet_id,contract_id,week_ending_date,line_type,submission_mode)
  values(v_timesheet,v_contract,current_date,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,total_hours,processing_status)
  values(v_timesheet,v_candidate,v_client,8,'UNPROCESSED');
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,submission_mode
  ) values(v_other_timesheet,v_contract,'OFFICE-ADAPTER-ADDITIONAL',current_date,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,additional_seq
  ) values(v_other_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_other_timesheet,1);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,total_hours,processing_status)
  values(v_other_timesheet,v_candidate,v_client,4,'UNPROCESSED');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','office-adapter@example.test','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,review_manifest_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'office-adapter-workflow',extensions.digest('office-adapter-manifest','sha256')
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,review_ordinal,review_render_state,review_storage_key,
    review_content_sha256,review_media_type,review_byte_size,review_page_count,
    review_generated_at_utc,review_render_input_sha256,review_renderer_contract_version,
    review_renderer_receipt_json,final_signed_render_state
  ) values(
    v_component,v_workflow,1,1,v_timesheet,'HOURS_TIMESHEET','ELECTRONIC_TIMESHEET_MANAGER_REVIEW',
    'IMMUTABLE','candidate/office-adapter/source.pdf','application/pdf',100,
    extensions.digest('office-adapter-source','sha256'),v_now,true,1,'READY',
    'candidate/office-adapter/review.pdf',extensions.digest('office-adapter-review','sha256'),
    'application/pdf',100,1,v_now,extensions.digest('office-adapter-render','sha256'),
    'CANDIDATE_REVIEW_DOCUMENTS_V1',jsonb_build_object('page_count',1),'PENDING'
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(
    v_request,v_workflow,1,1,'EMAIL','PENDING','manager@example.test',
    extensions.digest('office-adapter-token','sha256'),v_now+interval '7 days',
    extensions.digest('office-adapter-manifest','sha256'),array[v_component],
    jsonb_build_array(jsonb_build_object('component_id',v_component)),
    'office-adapter-request'
  );
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,sent_at,provider_status,provider_message_id
  ) values(
    gen_random_uuid(),'TIMESHEET_GENERAL','manager@example.test','Approval required','Review.',
    'SENT','CANDIDATE_WORKFLOW',v_workflow,'OFFICE-ADAPTER-INITIAL',jsonb_build_object(
      'candidate_mail_authority','MANAGER_APPROVAL_V1','candidate_manager_mail_kind','INITIAL',
      'candidate_manager_workflow_id',v_workflow,'candidate_manager_workflow_generation',1,
      'candidate_approval_request_id',v_request,'candidate_approval_request_generation',1,
      'candidate_manager_mail_retired',false
    ),v_now-interval '24 hours','ACCEPTED','office-adapter-provider-id'
  );

  v_capabilities:=public.cloudtms_office_candidate_adapter_v1(
    'CAPABILITIES',v_actor,'TEST','{}'::jsonb,v_now
  );
  if v_capabilities->>'mode'<>'ENABLED'
     or v_capabilities->>'required_office_role'<>'admin'
     or v_capabilities->>'permission_source'<>'OFFICE_ADMIN_ROLE_V1'
     or not coalesce((v_capabilities#>>'{permissions,send_manager_reminder_batch}')::boolean,false) then
    raise exception 'office capabilities incorrect: %',v_capabilities;
  end if;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'PROJECT_BATCH',v_actor,'TEST',jsonb_build_object(
        'surface','UNDECLARED_SURFACE',
        'identities',jsonb_build_array(jsonb_build_object('timesheet_id',v_timesheet))
      ),v_now
    );
    raise exception 'undeclared office projection surface unexpectedly succeeded';
  exception when others then
    if position('CANDIDATE_OFFICE_PROJECTION_SURFACE_INVALID' in sqlerrm)=0 then raise; end if;
  end;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'PROJECT_ONE',v_actor,'TEST',jsonb_build_object(
        'timesheet_id',v_timesheet,'contract_week_id',v_other_week
      ),v_now
    );
    raise exception 'mixed additional-record projection identity unexpectedly succeeded';
  exception when others then
    if position('CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' in sqlerrm)=0 then raise; end if;
  end;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'REJECT_CONFIRM',v_actor,'TEST',jsonb_build_object(
        'timesheet_id',v_timesheet,'expected_timesheet_id',v_timesheet,
        'expected_row_signature','not-used','context_sha256',repeat('a',64),
        'reason',repeat('x',1001),'idempotency_key',gen_random_uuid()::text
      ),v_now
    );
    raise exception 'oversized office rejection reason unexpectedly succeeded';
  exception when others then
    if position('CANDIDATE_REJECT_PAYLOAD_INVALID' in sqlerrm)=0 then raise; end if;
  end;

  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  v_signature:=v_projection#>>'{current_identity,row_signature}';
  if v_projection->>'contract_version'<>'OFFICE_CANDIDATE_TIMESHEET_V1'
     or v_projection#>>'{workflow,workflow_id}'<>v_workflow::text
     or v_projection#>>'{manager_approval,request_id}'<>v_request::text
     or not (v_projection->'available_actions' @> jsonb_build_array(
       jsonb_build_object('code','SEND_MANAGER_REMINDER','enabled',true)
     ))
     or jsonb_typeof(v_projection->'diagnostics')<>'array'
     or nullif(v_signature,'') is null then
    raise exception 'office projection incorrect: %',v_projection;
  end if;
  if exists(
    select 1 from jsonb_array_elements(v_projection->'available_actions') action_item
    where action_item->>'contract_version'<>'OFFICE_CANDIDATE_ACTION_V1'
       or jsonb_typeof(action_item->'invocation')<>'object'
       or action_item#>>'{invocation,version}'<>'1'
       or action_item#>>'{invocation,kind}' not in ('HTTP','CLIENT_DESTINATION')
       or (coalesce((action_item->>'enabled')::boolean,false)
         and nullif(action_item#>>'{invocation,path}','') is null)
       or (not coalesce((action_item->>'enabled')::boolean,false)
         and (nullif(action_item->>'disabled_reason_code','') is null
           or nullif(action_item->>'disabled_reason','') is null))
  ) then
    raise exception 'office typed action invocation/disabled contract incomplete: %',v_projection->'available_actions';
  end if;

  v_identity:=jsonb_build_object(
    'row_key',v_week,'timesheet_id',v_timesheet,'contract_week_id',v_week,
    'expected_row_signature',v_signature
  );
  v_preview:=public.cloudtms_office_candidate_adapter_v1(
    'REMINDER_BATCH_PREVIEW',v_actor,'TEST',
    jsonb_build_object('identities',jsonb_build_array(v_identity)),v_now
  );
  if (v_preview->>'eligible_count')::integer<>1
     or v_preview->>'preview_context_hash'<>v_preview->>'selection_fingerprint' then
    raise exception 'batch preview incorrect: %',v_preview;
  end if;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'REMINDER_BATCH_PREVIEW',v_actor,'TEST',
      jsonb_build_object('identities',jsonb_build_array(v_identity,v_identity)),v_now
    );
    raise exception 'duplicate batch identity unexpectedly succeeded';
  exception when others then
    if position('CANDIDATE_REMINDER_BATCH_DUPLICATE_IDENTITY' in sqlerrm)=0 then raise; end if;
  end;
  v_reminders:=jsonb_build_array((v_preview->'items'->0)||jsonb_build_object(
    'payload',jsonb_build_object(
      'approval_token_hash_hex',repeat('51',32),
      'manager_email','manager@example.test',
      'mail',jsonb_build_object('to','manager@example.test','subject','Reminder','body_text','Review.')
    )
  ));
  v_execute_payload:=jsonb_build_object(
      'identities',jsonb_build_array(v_identity),'batch_id',v_batch,
      'idempotency_key',v_batch::text,
      'preview_context_hash',v_preview->>'preview_context_hash',
      'selection_fingerprint',v_preview->>'selection_fingerprint',
      'reminders',v_reminders
    );
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'REMINDER_BATCH_EXECUTE',v_actor,'TEST',v_execute_payload,v_now
  );
  if v_result->>'status'<>'COMPLETED' or (v_result->>'success_count')::integer<>1 then
    raise exception 'batch execution incorrect: %',v_result;
  end if;
  select count(*) into v_count from public.mail_outbox
  where payment_scope_json->>'candidate_manager_mail_kind'='REMINDER'
    and payment_scope_json->>'candidate_approval_request_id'=v_request::text;
  if v_count<>1 then raise exception 'batch did not queue exactly one reminder'; end if;
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'REMINDER_BATCH_REPLAY',v_actor,'TEST',v_execute_payload-'reminders',v_now+interval '30 seconds'
  );
  if not coalesce((v_result->>'found')::boolean,false)
     or not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or v_result->>'batch_id'<>v_batch::text then
    raise exception 'batch replay probe did not return the durable receipt: %',v_result;
  end if;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'REMINDER_BATCH_REPLAY',v_actor,'TEST',
      (v_execute_payload-'reminders')||jsonb_build_object('selection_fingerprint',repeat('f',64)),
      v_now+interval '40 seconds'
    );
    raise exception 'altered batch replay probe unexpectedly succeeded';
  exception when others then
    if position('IDEMPOTENCY_CONFLICT' in sqlerrm)=0 then raise; end if;
  end;
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'REMINDER_BATCH_EXECUTE',v_actor,'TEST',v_execute_payload,v_now+interval '1 minute'
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or v_result->>'batch_id'<>v_batch::text then
    raise exception 'batch exact replay did not return durable receipt: %',v_result;
  end if;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'REMINDER_BATCH_EXECUTE',v_actor,'TEST',
      v_execute_payload||jsonb_build_object('selection_fingerprint',repeat('f',64)),v_now+interval '2 minutes'
    );
    raise exception 'altered batch replay unexpectedly succeeded';
  exception when others then
    if position('IDEMPOTENCY_CONFLICT' in sqlerrm)=0 then raise; end if;
  end;
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'REMINDER_BATCH_STATUS',v_actor,'TEST',jsonb_build_object('batch_id',v_batch),v_now
  );
  if v_result->>'batch_id'<>v_batch::text or v_result->>'status'<>'COMPLETED' then
    raise exception 'batch status receipt incorrect: %',v_result;
  end if;

  update public.candidate_submission_workflows set route='PHONE' where id=v_workflow;
  update public.candidate_approval_requests set method='PHONE' where id=v_request;
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if not (v_projection->'available_actions' @> jsonb_build_array(
      jsonb_build_object('code','BEGIN_PHONE_REVIEW','enabled',true)
    ))
     or not (v_projection->'available_actions' @> jsonb_build_array(
      jsonb_build_object('code','PREPARE_PHONE_SIGNATURE','enabled',true)
    ))
     or not (v_projection->'available_actions' @> jsonb_build_array(
      jsonb_build_object('code','RECORD_PHONE_REVIEW_PROGRESS','enabled',true)
    ))
     or not (v_projection->'available_actions' @> jsonb_build_array(
      jsonb_build_object('code','APPROVE_BY_PHONE','enabled',true)
    ))
     or not (v_projection->'available_actions' @> jsonb_build_array(
      jsonb_build_object('code','REFUSE_BY_PHONE','enabled',true)
    ))
     or v_projection#>>'{primary_action,code}'<>'BEGIN_PHONE_REVIEW' then
    raise exception 'typed phone action contract incomplete: %',v_projection->'available_actions';
  end if;

  update public.timesheets_financials set expenses_pay_ex_vat=10 where timesheet_id=v_timesheet;
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if not (v_projection->'diagnostics' @> jsonb_build_array(jsonb_build_object(
      'code','EXPENSE_EMAIL_MISSING','severity','WARNING','message','Expense Email missing',
      'calculation_effect','NONE','authority_effect','PRESENTATION_ONLY'
    ))) then
    raise exception 'expense email semantic diagnostic incorrect: %',v_projection->'diagnostics';
  end if;

  update public.timesheets_financials
  set locked_by_invoice_id='ad510000-0000-4000-8000-000000000099'
  where timesheet_id=v_timesheet;
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if v_projection#>>'{candidate_status,code}'<>'INVOICED_NOT_PAID'
     or exists(
       select 1 from jsonb_array_elements(v_projection->'available_actions') action_item
       where coalesce((action_item->>'enabled')::boolean,false)
         and action_item->>'group' in ('MANAGER_APPROVAL','REJECTION','FINALISATION')
     ) then
    raise exception 'invoice-locked office status/action precedence incorrect: %',v_projection;
  end if;
  update public.timesheets_financials set locked_by_invoice_id=null where timesheet_id=v_timesheet;

  -- Cancel the exact approval request/link, not the Candidate claim. Sent mail
  -- remains immutable history and only mutable manager delivery is retired.
  update public.candidate_submission_workflows
  set route='EMAIL',state='AWAITING_MANAGER_APPROVAL'
  where id=v_workflow;
  update public.candidate_approval_requests
  set method='EMAIL',state='PENDING',cancelled_at_utc=null
  where id=v_request;
  update public.mail_outbox set
    attempt_lease_token='ad510000-0000-4000-8000-000000000016',
    attempt_leased_at_utc=v_now,
    attempt_lease_expires_at_utc=v_now+interval '10 minutes'
  where payment_scope_json->>'candidate_manager_mail_kind'='REMINDER'
    and payment_scope_json->>'candidate_approval_request_id'=v_request::text;
  v_failed:=false;
  begin
    perform public.cloudtms_office_candidate_adapter_v1(
      'WORKFLOW_ACTION_EXECUTE',v_actor,'TEST',jsonb_build_object(
        'workflow_id',v_workflow,'generation',1,
        'workflow_action','MANAGER_REQUEST_CANCEL',
        'approval_request_id',v_request,'approval_request_generation',1,
        'idempotency_key','ad510000-0000-4000-8000-000000000013',
        'payload',jsonb_build_object('reason_note','Office is selecting a different approval method.')
      ),v_now+interval '2 minutes'
    );
  exception when sqlstate '40001' then
    v_failed:=position('CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS' in sqlerrm)>0;
  end;
  if not v_failed
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'AWAITING_MANAGER_APPROVAL'
     or (select state from public.candidate_approval_requests where id=v_request)<>'PENDING' then
    raise exception 'active manager provider lease did not block request cancellation atomically';
  end if;
  update public.mail_outbox set
    attempt_lease_token=null,attempt_leased_at_utc=null,attempt_lease_expires_at_utc=null
  where payment_scope_json->>'candidate_manager_mail_kind'='REMINDER'
    and payment_scope_json->>'candidate_approval_request_id'=v_request::text;
  v_result:=public.cloudtms_office_candidate_adapter_v1(
    'WORKFLOW_ACTION_EXECUTE',v_actor,'TEST',jsonb_build_object(
      'workflow_id',v_workflow,'generation',1,
      'workflow_action','MANAGER_REQUEST_CANCEL',
      'approval_request_id',v_request,'approval_request_generation',1,
      'idempotency_key','ad510000-0000-4000-8000-000000000014',
      'payload',jsonb_build_object('reason_note','Office is selecting a different approval method.')
    ),v_now+interval '3 minutes'
  );
  if v_result->>'state'<>'READY_FOR_MANAGER_APPROVAL'
     or coalesce((v_result->>'claim_cancelled')::boolean,true)
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'READY_FOR_MANAGER_APPROVAL'
     or (select state from public.candidate_approval_requests where id=v_request)<>'CANCELLED'
     or (select count(*) from public.candidate_submission_components
         where workflow_id=v_workflow and state='IMMUTABLE')=0
     or (select status from public.mail_outbox where deterministic_outbox_key='OFFICE-ADAPTER-INITIAL')<>'SENT'
     or (select count(*) from public.mail_outbox
         where payment_scope_json->>'candidate_manager_mail_kind'='WITHDRAWAL'
           and payment_scope_json->>'candidate_approval_request_id'=v_request::text)<>1 then
    raise exception 'manager request cancellation changed the claim or immutable history: %',v_result;
  end if;

  -- PREPARING is ordinary asynchronous work, not a retry signal. Only the
  -- explicit durable retryable receipt enables retry.
  update public.candidate_submission_workflows
  set route='PAPER',state='AWAITING_PAPER_RETURN'
  where id=v_workflow;
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json
  ) values(
    'ad510000-0000-4000-8000-000000000015','TIMESHEET_GENERAL',
    'candidate@example.test','Paper pack','Preparing.','QUEUED','CANDIDATE_WORKFLOW',v_workflow,
    'OFFICE-ADAPTER-PAPER',jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1',
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'candidate_paper_pack_ready',false,'candidate_paper_pack_retryable',false
    )
  );
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if v_projection#>>'{paper_pack,state}'<>'PREPARING'
     or v_projection#>>'{paper_pack,retryable}'<>'false'
     or v_projection->'available_actions' @> jsonb_build_array(
       jsonb_build_object('code','RETRY_PAPER_PREPARATION','enabled',true)
     ) then
    raise exception 'ordinary PAPER preparation was incorrectly retryable: %',v_projection->'paper_pack';
  end if;
  update public.mail_outbox set status='FAILED' where id='ad510000-0000-4000-8000-000000000015';
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if v_projection#>>'{paper_pack,state}'<>'FAILED_TERMINAL'
     or v_projection#>>'{paper_pack,retryable}'<>'false' then
    raise exception 'terminal PAPER failure was incorrectly retryable: %',v_projection->'paper_pack';
  end if;
  update public.mail_outbox set payment_scope_json=payment_scope_json
    ||'{"candidate_paper_pack_retryable":true}'::jsonb
  where id='ad510000-0000-4000-8000-000000000015';
  v_projection:=public.cloudtms_office_candidate_adapter_v1(
    'PROJECT_ONE',v_actor,'TEST',jsonb_build_object('timesheet_id',v_timesheet),v_now
  );
  if v_projection#>>'{paper_pack,state}'<>'FAILED_RETRYABLE'
     or v_projection#>>'{paper_pack,retryable}'<>'true'
     or not (v_projection->'available_actions' @> jsonb_build_array(
       jsonb_build_object('code','RETRY_PAPER_PREPARATION','enabled',true)
     )) then
    raise exception 'durable PAPER retry receipt did not enable retry: %',v_projection;
  end if;
end;
$office_adapter$;

rollback;
