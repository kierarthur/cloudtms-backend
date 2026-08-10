\set ON_ERROR_STOP on
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":true,"candidate_paper_qr":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

do $verify_runtime_digest_authority$
begin
  if to_regprocedure('public.digest(text,text)') is not null then
    raise exception 'Disposable verification must not provide a misleading public.digest shim';
  end if;
  if to_regprocedure('extensions.digest(text,text)') is null then
    raise exception 'Expected extensions.digest(text,text) is unavailable';
  end if;
  if encode(extensions.digest(convert_to('candidate-runtime-digest','UTF8'),'sha256'),'hex')
     <>encode(extensions.digest(convert_to('candidate-runtime-digest','UTF8'),'sha256'),'hex') then
    raise exception 'Explicit pgcrypto digest authority is unstable';
  end if;
end;
$verify_runtime_digest_authority$;

do $verify_anchor_only_expense_rejection$
declare
  v_actor constant uuid:='b6510000-0000-4000-8000-000000000001';
  v_candidate constant uuid:='b6510000-0000-4000-8000-000000000002';
  v_client constant uuid:='b6510000-0000-4000-8000-000000000003';
  v_contract constant uuid:='b6510000-0000-4000-8000-000000000004';
  v_timesheet constant uuid:='b6510000-0000-4000-8000-000000000005';
  v_week constant uuid:='b6510000-0000-4000-8000-000000000006';
  v_account constant uuid:='b6510000-0000-4000-8000-000000000007';
  v_session constant uuid:='b6510000-0000-4000-8000-000000000008';
  v_workflow constant uuid:='b6510000-0000-4000-8000-000000000009';
  v_component constant uuid:='b6510000-0000-4000-8000-000000000010';
  v_request constant uuid:='b6510000-0000-4000-8000-000000000011';
  v_mail constant uuid:='b6510000-0000-4000-8000-000000000012';
  v_notification constant uuid:='b6510000-0000-4000-8000-000000000013';
  v_replacement_workflow constant uuid:='b6510000-0000-4000-8000-000000000014';
  v_signature text;
  v_result jsonb;
  v_new_timesheet uuid;
  v_state text;
  v_generation integer;
  v_scope jsonb;
  v_attachments jsonb;
begin
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.candidates(id,email,active)
  values(v_candidate,'anchor-rejection@example.test',true);
  insert into public.clients(id,name) values(v_client,'Anchor rejection client');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
  values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,v_contract,'ANCHOR-REJECTION-1',current_date,'HOURS','WEEKLY',
    'ELECTRONIC','anchor/candidate-signature','anchor/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','anchor-rejection@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('b6',32),'hex'),
    now()+interval '1 day',now()+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,null,current_date,
    'anchor-rejection:original-claim'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,component_kind,expense_category,
    document_role,state,storage_key,media_type,byte_size,upload_idempotency_key
  ) values(
    v_component,v_workflow,1,1,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE','PENDING',
    'candidate-app/test/anchor-rejection/receipt.png','image/png',1024,
    'anchor-rejection:component'
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,method,state,manager_email_normalized,
    token_hash,expires_at_utc,idempotency_key,review_manifest_sha256,
    required_component_ids,required_component_manifest_json
  ) values(
    v_request,v_workflow,1,'EMAIL','PENDING','manager@example.test',
    extensions.digest(convert_to('anchor-rejection-manager-token','UTF8'),'sha256'),
    now()+interval '1 day','anchor-rejection:approval',
    extensions.digest(convert_to('anchor-rejection-review-manifest','UTF8'),'sha256'),
    array[v_component],jsonb_build_array(jsonb_build_object(
      'component_id',v_component,'component_kind','EXPENSE_EVIDENCE'
    ))
  );
  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,reference,payment_scope_json
  ) values(
    v_mail,'TIMESHEET_QR','anchor-rejection@example.test','Paper expense claim',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/anchor-rejection/pack.pdf')),
    'QUEUED',now(),'timesheets',v_timesheet,'infinity','infinity',
    'anchor-rejection:mail',jsonb_build_object(
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'candidate_complete_pack_storage_key','candidate-app/test/anchor-rejection/pack.pdf'
    )
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,
    preference_category,template_key,template_params,deep_link_json,state,push_state,dedupe_key
  ) values(
    v_notification,v_account,v_candidate,v_workflow,v_timesheet,'PAPER_PACK_READY',
    'resubmission_required','candidate-paper-pack-ready-v1','{}'::jsonb,
    jsonb_build_object('type','timesheet','timesheet_id',v_timesheet),
    'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow::text||':1:anchor-rejection'
  );

  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet,v_week,false)
    ->>'row_signature';
  v_result:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_timesheet,v_timesheet,v_signature,
    'Complete Candidate submission rejected','anchor-rejection:reject',now()
  );
  v_new_timesheet:=(v_result->>'timesheet_id')::uuid;
  if v_new_timesheet is null or v_new_timesheet=v_timesheet then
    raise exception 'Candidate rejection did not create the replacement current timesheet: %',v_result;
  end if;
  select state,generation into v_state,v_generation
  from public.candidate_submission_workflows where id=v_workflow;
  if v_state<>'REJECTED' or v_generation<>2 then
    raise exception 'Anchor-only expense workflow remained active: %, %',v_state,v_generation;
  end if;
  if (select state from public.candidate_submission_components where id=v_component)<>'REJECTED'
     or (select state from public.candidate_approval_requests where id=v_request)<>'SUPERSEDED' then
    raise exception 'Anchor-only workflow component/request lineage remained active';
  end if;
  select payment_scope_json,attachments into v_scope,v_attachments
  from public.mail_outbox where id=v_mail;
  if lower(coalesce(v_scope->>'candidate_paper_generation_retired','false'))<>'true'
     or jsonb_array_length(v_attachments)<>0 then
    raise exception 'Anchor-only PAPER delivery was not retired';
  end if;
  if (select state from public.candidate_notifications where id=v_notification)<>'DISMISSED'
     or (select push_state from public.candidate_notifications where id=v_notification)<>'SKIPPED' then
    raise exception 'Anchor-only PAPER readiness notification remained actionable';
  end if;
  if exists(
    select 1 from public.candidate_submission_workflows active
    where (active.target_timesheet_id=v_timesheet or active.anchor_timesheet_id=v_timesheet)
      and active.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
  ) then
    raise exception 'Rejected timesheet still has an active target-or-anchor Candidate workflow';
  end if;

  -- Simulate the candidate completing the replacement hours before submitting
  -- a new expense claim. The rejected claim must no longer trip the one-active-
  -- expense gate.
  update public.timesheets_financials set total_hours=8 where timesheet_id=v_new_timesheet;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_replacement_workflow,'CREATE',1,jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_week,
      'anchor_timesheet_id',v_new_timesheet,'week_ending_date',current_date
    ),'anchor-rejection:replacement-claim',now()
  );
  if v_result->>'state'<>'WORKER_DRAFT'
     or v_result->>'workflow_id'<>v_replacement_workflow::text then
    raise exception 'Replacement expense claim was not created: %',v_result;
  end if;
end;
$verify_anchor_only_expense_rejection$;

rollback;
