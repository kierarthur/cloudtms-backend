-- Manager approval mail is request/generation bound, claim-gated and provider-lease fenced.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||jsonb_build_object(
    'candidate_app_writes',true,
    'candidate_manager_approval',true
  )
where id=1;

do $manager_mail$
declare
  v_now timestamptz:=date_trunc('second',now());
  v_client uuid:='ac140000-0000-0000-0000-000000000001';
  v_candidate uuid:='ac140000-0000-0000-0000-000000000002';
  v_contract uuid:='ac140000-0000-0000-0000-000000000003';
  v_timesheet uuid:='ac140000-0000-0000-0000-000000000004';
  v_week uuid:='ac140000-0000-0000-0000-000000000005';
  v_account uuid:='ac140000-0000-0000-0000-000000000006';
  v_workflow uuid:='ac140000-0000-0000-0000-000000000007';
  v_component uuid:='ac140000-0000-0000-0000-000000000012';
  v_request uuid:='ac140000-0000-0000-0000-000000000008';
  v_request_sent uuid:='ac140000-0000-0000-0000-000000000009';
  v_request_failed uuid:='ac140000-0000-0000-0000-000000000010';
  v_request_permit uuid:='ac140000-0000-0000-0000-000000000011';
  v_mail uuid;
  v_result jsonb;
  v_claim public.mail_outbox%rowtype;
  v_count integer;
begin
  insert into public.clients(id,name) values(v_client,'Manager mail client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'manager-mail@example.test',true,'MANAGER-MAIL-CANDIDATE');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(timesheet_id,contract_id,week_ending_date,submission_mode)
  values(v_timesheet,v_contract,current_date,'MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','manager-mail@example.test','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,review_manifest_sha256
  ) values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'manager-mail-workflow',extensions.digest('manager-mail-manifest','sha256'));
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,review_render_state,final_signed_render_state
  ) values(v_component,v_workflow,1,1,v_timesheet,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
    'IMMUTABLE','candidate/manager-mail/signature.png','image/png',100,
    extensions.digest('manager-mail-signature','sha256'),v_now,false,'NOT_REQUIRED','NOT_REQUIRED');
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(v_request,v_workflow,1,1,'EMAIL','PENDING','manager@example.test',
    extensions.digest('request-one','sha256'),v_now+interval '7 days',
    extensions.digest('manager-mail-manifest','sha256'),array[v_component],
    jsonb_build_array(jsonb_build_object('component_id',v_component)),'request-one');

  v_mail:=private._candidate_queue_mail_v1(
    jsonb_build_object(
      'subject','Please approve','body_text','Review this timesheet.',
      'payment_scope_json',jsonb_build_object(
        'candidate_mail_authority','MANAGER_APPROVAL_V1',
        'candidate_manager_mail_kind','INITIAL',
        'candidate_manager_workflow_id',v_workflow,
        'candidate_manager_workflow_generation',1,
        'candidate_approval_request_id',v_request,
        'candidate_approval_request_generation',1,
        'candidate_manager_mail_retired',false
      )
    ),'manager@example.test','MANAGER-MAIL-INITIAL-1','manager-mail-initial',
    v_workflow,v_now
  );
  select * into v_claim from public.email_outbox_claim_ready_batch(10,'manager-live-lease',5)
  where id=v_mail;
  if not found then raise exception 'current manager invitation was not claimable'; end if;
  begin
    update public.candidate_approval_requests
    set state='CANCELLED',cancelled_at_utc=v_now,updated_at_utc=v_now
    where id=v_request;
    raise exception 'live manager provider lease did not block request cancellation';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
  end;
  if (select state from public.candidate_approval_requests where id=v_request)<>'PENDING' then
    raise exception 'request changed despite live provider lease';
  end if;
  update public.mail_outbox set attempt_lease_token=null,attempt_leased_at_utc=null,
    attempt_lease_expires_at_utc=null where id=v_mail;
  update public.candidate_approval_requests
  set state='CANCELLED',cancelled_at_utc=v_now,updated_at_utc=v_now
  where id=v_request;
  if not coalesce((select (payment_scope_json->>'candidate_manager_mail_retired')::boolean
      from public.mail_outbox where id=v_mail),false)
     or (select scheduled_for_utc from public.mail_outbox where id=v_mail)<>'infinity'::timestamptz then
    raise exception 'queued obsolete manager invitation was not retired';
  end if;
  select count(*) into v_count
  from public.email_outbox_claim_ready_batch(10,'manager-stale-claim',5)
  where id=v_mail;
  if v_count<>0 then raise exception 'retired manager invitation was claimed'; end if;

  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(
    v_request_sent,v_workflow,1,2,'EMAIL','PENDING','manager@example.test',
      extensions.digest('request-sent','sha256'),v_now+interval '7 days',
      extensions.digest('manager-mail-manifest','sha256'),array[v_component],
      jsonb_build_array(jsonb_build_object('component_id',v_component)),'request-sent'
  );
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,sent_at,provider_status,provider_message_id
  ) values(
    gen_random_uuid(),'TIMESHEET_GENERAL','manager@example.test','Sent invitation','Sent.',
    'SENT','CANDIDATE_WORKFLOW',v_workflow,'MANAGER-MAIL-SENT',jsonb_build_object(
      'candidate_mail_authority','MANAGER_APPROVAL_V1','candidate_manager_mail_kind','REMINDER',
      'candidate_manager_workflow_id',v_workflow,'candidate_manager_workflow_generation',1,
      'candidate_approval_request_id',v_request_sent,'candidate_approval_request_generation',2,
      'candidate_manager_mail_retired',false
    ),v_now,'ACCEPTED','provider-sent'
  );
  v_result:=private._candidate_manager_mail_retire_v1(
    v_workflow,1,array[v_request_sent],'TEST_SENT_RETIREMENT',v_now
  );
  if not coalesce((v_result->>'withdrawal_required')::boolean,false)
     or (select status from public.mail_outbox where deterministic_outbox_key='MANAGER-MAIL-SENT')<>'SENT' then
    raise exception 'provider-accepted mail was not preserved as withdrawal truth: %',v_result;
  end if;
  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=v_now,updated_at_utc=v_now
  where id=v_request_sent;
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(v_request_failed,v_workflow,1,3,'EMAIL','PENDING','manager@example.test',
    extensions.digest('request-failed','sha256'),v_now+interval '7 days',
    extensions.digest('manager-mail-manifest','sha256'),array[v_component],
    jsonb_build_array(jsonb_build_object('component_id',v_component)),'request-failed');
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json
  ) values(
    gen_random_uuid(),'TIMESHEET_GENERAL','manager@example.test','Failed invitation','Failed.',
    'FAILED','CANDIDATE_WORKFLOW',v_workflow,'MANAGER-MAIL-FAILED',jsonb_build_object(
      'candidate_mail_authority','MANAGER_APPROVAL_V1','candidate_manager_mail_kind','INITIAL',
      'candidate_manager_workflow_id',v_workflow,'candidate_manager_workflow_generation',1,
      'candidate_approval_request_id',v_request_failed,'candidate_approval_request_generation',3,
      'candidate_manager_mail_retired',false
    )
  );
  update public.candidate_approval_requests
  set state='SUPERSEDED',superseded_at_utc=v_now,updated_at_utc=v_now
  where id=v_request_failed;
  if (select status from public.mail_outbox where deterministic_outbox_key='MANAGER-MAIL-FAILED')<>'FAILED'
     or not coalesce((select (payment_scope_json->>'candidate_manager_mail_retired')::boolean
       from public.mail_outbox where deterministic_outbox_key='MANAGER-MAIL-FAILED'),false) then
    raise exception 'failed manager mail was requeued or not retired';
  end if;

  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,review_manifest_sha256,
    required_component_ids,required_component_manifest_json,idempotency_key
  ) values(v_request_permit,v_workflow,1,4,'EMAIL','PENDING','manager@example.test',
    extensions.digest('request-permit','sha256'),v_now+interval '7 days',
    extensions.digest('manager-mail-manifest','sha256'),array[v_component],
    jsonb_build_array(jsonb_build_object('component_id',v_component)),'request-permit');
  v_mail:=private._candidate_queue_mail_v1(
    jsonb_build_object(
      'subject','Renewed approval','body_text','Review this renewed request.',
      'payment_scope_json',jsonb_build_object(
        'candidate_mail_authority','MANAGER_APPROVAL_V1','candidate_manager_mail_kind','RENEWAL',
        'candidate_manager_workflow_id',v_workflow,'candidate_manager_workflow_generation',1,
        'candidate_approval_request_id',v_request_permit,'candidate_approval_request_generation',4,
        'candidate_manager_mail_retired',false
      )
    ),'manager@example.test','MANAGER-MAIL-PERMIT','manager-mail-permit',v_workflow,v_now
  );
  select * into v_claim from public.email_outbox_claim_ready_batch(10,'manager-permit-lease',5)
  where id=v_mail;
  if not found then raise exception 'provider-permit manager mail was not claimable'; end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'MANAGER_PROVIDER_SUBMIT_PERMIT',1,
    jsonb_build_object(
      'service_manager_provider_submit_permit',true,
      'mail_outbox_id',v_mail,'attempt_lease_token','manager-permit-lease'
    ),null,v_now
  );
  if not coalesce((v_result->>'provider_submit_permit')::boolean,false)
     or v_result->>'approval_request_id'<>v_request_permit::text then
    raise exception 'manager provider permit was not exact: %',v_result;
  end if;
  begin
    update public.candidate_approval_requests
    set state='CANCELLED',cancelled_at_utc=v_now,updated_at_utc=v_now
    where id=v_request_permit;
    raise exception 'provider permit did not fence request invalidation';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
  end;
end;
$manager_mail$;

rollback;
