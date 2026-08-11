\set ON_ERROR_STOP on

-- Candidate PAPER source-owner and claim-isolation closure. Disposable
-- PostgreSQL only; every synthetic row and feature change is rolled back.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
      'candidate_app_writes',true,
      'candidate_paper_qr',true,
      'candidate_notifications',true,
      'candidate_route_confirmation',true
    ),
    candidate_app_environment='TEST'
where id=1;

create or replace function pg_temp.verify_current_token_owner_route_selection(
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
  v_hours_mail uuid:=gen_random_uuid();
  v_expense_mail uuid:=gen_random_uuid();
  v_expense_notification uuid:=gen_random_uuid();
  v_old_token text:='historical-hours-token-'||gen_random_uuid()::text;
  v_current_token text:='current-expense-token-'||gen_random_uuid()::text;
  v_hours_manifest jsonb;
  v_expense_manifest jsonb;
  v_hours_manifest_hash text;
  v_expense_manifest_hash text;
  v_source_context jsonb;
  v_context jsonb;
  v_result jsonb;
  v_new_timesheet_id uuid;
begin
  if p_expense_state not in ('AWAITING_PAPER_RETURN','RECEIVED') then
    raise exception 'Invalid source-owner route fixture state';
  end if;
  v_hours_manifest:=jsonb_build_object(
    'workflow_id',v_hours_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_expense_manifest:=jsonb_build_object(
    'workflow_id',v_expense_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','EXPENSE_SUMMARY','component_kind','EXPENSE_SUMMARY'))
  );
  v_hours_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_hours_manifest),'hex');
  v_expense_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_expense_manifest),'hex');

  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'source-owner-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'PAPER source owner client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode,overrideclientsettings
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'MANUAL',true);
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at,
    qr_last_sent_hash,qr_last_sent_at_utc,qr_signed_hash,qr_signed_at_utc
  ) values(v_timesheet,v_contract,
    'PAPER-SOURCE-OWNER-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_current_token,'{}',v_now,
    'current-expense-pack-sent',v_now,
    case when p_expense_state='RECEIVED' then 'current-expense-signed-return' else null end,
    case when p_expense_state='RECEIVED' then v_now else null end);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST',
    'source-owner-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'FINALISED',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'paper-source-owner-hours:'||v_hours_workflow::text,v_now,
    v_hours_manifest,decode(v_hours_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER',
    p_expense_state,1,v_contract,v_week,v_timesheet,null,current_date,
    'paper-source-owner-expense:'||v_expense_workflow::text,null,
    v_expense_manifest,decode(v_expense_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  update public.timesheets set
    candidate_workflow_id=v_hours_workflow,candidate_workflow_generation=2
  where timesheet_id=v_timesheet;

  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json
  ) values(
    v_hours_mail,'TIMESHEET_QR','hours-history@example.test','Historical hours pack',
    'Historical pack',jsonb_build_array(jsonb_build_object(
      'r2_key','candidate-app/test/source-owner/hours.pdf')),
    'SENT',v_now,'timesheets',v_timesheet,v_now,v_now,
    'paper-source-owner-hours-mail:'||v_hours_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_hours_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_hours_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_old_token,'UTF8'),'sha256'),'hex')
    )
  ),(
    v_expense_mail,'TIMESHEET_QR','expense-current@example.test','Current expense pack',
    'Current pack',jsonb_build_array(jsonb_build_object(
      'r2_key','candidate-app/test/source-owner/expense.pdf')),
    'QUEUED',v_now,'timesheets',v_timesheet,v_now,v_now,
    'paper-source-owner-expense-mail:'||v_expense_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_expense_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_expense_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(
        convert_to(v_current_token,'UTF8'),'sha256'),'hex')
    )
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values(v_expense_notification,v_account,v_candidate,v_expense_workflow,v_timesheet,
    'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
    jsonb_build_object('workflow_generation',1),
    jsonb_build_object('type','paper_pack','workflow_id',v_expense_workflow,'generation',1),
    'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:'||v_expense_workflow::text
      ||':1:source-owner',v_now);

  v_source_context:=private._candidate_paper_source_workflow_context_v1(v_timesheet);
  if v_source_context->>'current_token_owner_workflow_id'<>v_expense_workflow::text
     or v_source_context->>'selected_workflow_id'<>v_expense_workflow::text
     or (v_source_context->>'nonterminal_workflow_count')::integer<>1
     or coalesce((v_source_context->>'identity_conflict')::boolean,false) then
    raise exception 'Current token owner context selected historical principal workflow: %',
      v_source_context;
  end if;

  v_context:=public.timesheet_route_version_preview_v1(
    v_timesheet,'CONVERT_QR_TO_MANUAL'
  );
  if v_context->>'warning_code'<>
       'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM'
     or v_context->>'paper_workflow_id'<>v_expense_workflow::text
     or v_context->>'paper_source_current_token_owner_workflow_id'<>
       v_expense_workflow::text
     or not coalesce(
       (v_context->>'incomplete_expense_claim_removal_required')::boolean,false
     )
     or v_context->>'incomplete_expense_workflow_id'<>v_expense_workflow::text
     or not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'Route preview did not select the live expense PAPER owner: %',v_context;
  end if;

  v_result:=public.timesheet_route_version_confirmed_v1(
    v_timesheet,v_timesheet,v_context->>'row_signature',v_context->>'context_sha256',
    'CONVERT_QR_TO_MANUAL',v_actor,'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,
    'paper-source-owner-confirm:'||v_expense_workflow::text,false,v_now
  );
  v_new_timesheet_id:=nullif(v_result->>'new_timesheet_id','')::uuid;
  if v_new_timesheet_id is null
     or (select state from public.candidate_submission_workflows
         where id=v_hours_workflow)<>'FINALISED'
     or (select generation from public.candidate_submission_workflows
         where id=v_hours_workflow)<>2
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'SUPERSEDED'
     or not coalesce(
       (v_result#>>'{workflow_retirement,incomplete_expense_claim_removed}')::boolean,false
     )
     or exists(
       select 1 from public.candidate_submission_workflows workflow
       where workflow.id=v_expense_workflow
         and workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED')
     )
     or (select state from public.candidate_notifications
         where id=v_expense_notification)<>'DISMISSED'
     or not coalesce((select
       (payment_scope_json->>'candidate_paper_generation_retired')::boolean
       from public.mail_outbox where id=v_expense_mail),false)
     or (select status from public.mail_outbox where id=v_hours_mail)<>'SENT'
     or not exists(
       select 1 from public.timesheets current_row
       where current_row.timesheet_id=v_new_timesheet_id
         and current_row.is_current=true
         and current_row.submission_mode='MANUAL'
         and current_row.qr_token is null
     ) then
    raise exception 'Route conversion stranded the later expense PAPER workflow: result=%',
      v_result;
  end if;
end;
$function$;

select pg_temp.verify_current_token_owner_route_selection('AWAITING_PAPER_RETURN');
select pg_temp.verify_current_token_owner_route_selection('RECEIVED');

create or replace function pg_temp.verify_claim_level_cancellation_isolation()
returns void
language plpgsql
as $function$
declare
  v_now timestamptz:=clock_timestamp();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_expense_workflow uuid:=gen_random_uuid();
  v_hours_mail uuid:=gen_random_uuid();
  v_expense_mail uuid:=gen_random_uuid();
  v_expense_notification uuid:=gen_random_uuid();
  v_old_token text:='cancel-hours-token-'||gen_random_uuid()::text;
  v_current_token text:='cancel-expense-token-'||gen_random_uuid()::text;
  v_hours_manifest jsonb;
  v_expense_manifest jsonb;
  v_hours_manifest_hash text;
  v_expense_manifest_hash text;
  v_context jsonb;
  v_blocked boolean:=false;
begin
  v_hours_manifest:=jsonb_build_object(
    'workflow_id',v_hours_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object('page_key','HOURS_TIMESHEET'))
  );
  v_expense_manifest:=jsonb_build_object(
    'workflow_id',v_expense_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object('page_key','EXPENSE_SUMMARY'))
  );
  v_hours_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_hours_manifest),'hex');
  v_expense_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_expense_manifest),'hex');

  insert into public.candidates(id,email,active)
  values(v_candidate,'cancel-isolation-'||replace(v_candidate::text,'-','')
    ||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Cancellation isolation client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'MANUAL');
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at
  ) values(v_timesheet,v_contract,
    'PAPER-CANCEL-ISOLATION-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_current_token,'{}',v_now);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','cancel-isolation-'||replace(v_candidate::text,'-','')
    ||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '1 day',v_now+interval '7 days');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,paper_return_manifest_json,
    paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'cancel-isolation-hours:'||v_hours_workflow::text,v_hours_manifest,
    decode(v_hours_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  ),(
    v_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,null,current_date,
    'cancel-isolation-expense:'||v_expense_workflow::text,v_expense_manifest,
    decode(v_expense_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  update public.timesheets set
    candidate_workflow_id=v_hours_workflow,candidate_workflow_generation=1
  where timesheet_id=v_timesheet;

  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json
  ) values(
    v_hours_mail,'TIMESHEET_QR','cancel-hours@example.test','Hours pack','Hours pack',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/cancel/hours.pdf')),
    'QUEUED',v_now,'timesheets',v_timesheet,v_now,v_now,
    'cancel-isolation-hours-mail:'||v_hours_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_hours_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_hours_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_old_token,'UTF8'),'sha256'),'hex')
    )
  ),(
    v_expense_mail,'TIMESHEET_QR','cancel-expense@example.test','Expense pack','Expense pack',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/cancel/expense.pdf')),
    'QUEUED',v_now,'timesheets',v_timesheet,v_now,v_now,
    'cancel-isolation-expense-mail:'||v_expense_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_expense_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_expense_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(
        convert_to(v_current_token,'UTF8'),'sha256'),'hex')
    )
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values(v_expense_notification,v_account,v_candidate,v_expense_workflow,v_timesheet,
    'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
    jsonb_build_object('workflow_generation',1),
    jsonb_build_object('type','paper_pack','workflow_id',v_expense_workflow,'generation',1),
    'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:'||v_expense_workflow::text
      ||':1:cancel-isolation',v_now);

  v_context:=public.timesheet_route_version_preview_v1(
    v_timesheet,'CONVERT_QR_TO_MANUAL'
  );
  if v_context->>'warning_code'<>'ROUTE_CHANGE_WORKFLOW_CONFLICT'
     or v_context->>'block_reason'<>'MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS'
     or coalesce((v_context->>'permitted_action')::boolean,false)
     or nullif(v_context->>'paper_workflow_id','') is not null then
    raise exception 'Multiple live PAPER claims did not fail route preview closed: %',
      v_context;
  end if;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_hours_workflow,'CANCEL',1,
      jsonb_build_object('reason_note','Test shared-source cancellation.'),
      'cancel-isolation-attempt:'||v_hours_workflow::text,v_now
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT' then raise; end if;
    v_blocked:=true;
  end;

  if not v_blocked
     or (select state from public.candidate_submission_workflows
         where id=v_hours_workflow)<>'AWAITING_PAPER_RETURN'
     or (select state from public.candidate_submission_workflows
         where id=v_expense_workflow)<>'AWAITING_PAPER_RETURN'
     or (select qr_token from public.timesheets where timesheet_id=v_timesheet)<>
       v_current_token
     or coalesce((select
       (payment_scope_json->>'candidate_paper_generation_retired')::boolean
       from public.mail_outbox where id=v_hours_mail),false)
     or coalesce((select
       (payment_scope_json->>'candidate_paper_generation_retired')::boolean
       from public.mail_outbox where id=v_expense_mail),false)
     or jsonb_array_length((select attachments from public.mail_outbox
       where id=v_expense_mail))<>1
     or (select state from public.candidate_notifications
       where id=v_expense_notification)<>'UNREAD' then
    raise exception 'Claim-level cancellation invalidated another live PAPER claim';
  end if;
end;
$function$;

select pg_temp.verify_claim_level_cancellation_isolation();

rollback;
