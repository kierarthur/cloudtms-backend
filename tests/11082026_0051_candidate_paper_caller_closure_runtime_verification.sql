\set ON_ERROR_STOP on

-- Candidate PAPER caller/submit-permit closure. Disposable PostgreSQL only;
-- every synthetic row and feature change is rolled back.
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

create or replace function pg_temp.verify_received_cancel_or_supersede(
  p_action text,
  p_mail_status text,
  p_active_lease boolean
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
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_mail uuid:=gen_random_uuid();
  v_notification uuid:=gen_random_uuid();
  v_manifest jsonb;
  v_manifest_hash text;
  v_token text:='received-caller-token-'||gen_random_uuid()::text;
  v_result jsonb;
  v_blocked boolean:=false;
  v_expected_state text;
begin
  if upper(p_action) not in ('CANCEL','SUPERSEDE')
     or p_mail_status not in ('QUEUED','SENT')
     or (p_mail_status='SENT' and p_active_lease) then
    raise exception 'Invalid RECEIVED caller fixture';
  end if;
  v_expected_state:=case when upper(p_action)='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end;
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');

  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_actor
  where id=1;
  insert into public.candidates(id,email,active)
  values(v_candidate,'received-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'RECEIVED caller client '||left(v_client::text,8));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,current_date-1,'MANUAL',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,qr_status,qr_token,qr_payload_json,qr_generated_at,qr_signed_hash
  ) values(
    v_timesheet,v_contract,'RECEIVED-CALLER-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_token,'{}',v_now,'signed-return'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','received-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '1 day',v_now+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,paper_return_manifest_json,
    paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER','RECEIVED',1,
    v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'received-caller:'||v_workflow::text,v_manifest,
    decode(v_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1'
  );
  update public.timesheets
  set candidate_workflow_id=v_workflow,candidate_workflow_generation=1
  where timesheet_id=v_timesheet;
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json,
    attempt_lease_token,attempt_leased_at_utc,attempt_lease_expires_at_utc
  ) values(
    v_mail,'TIMESHEET_QR','received@example.test','RECEIVED pack','Pack ready',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/received/pack.pdf')),
    p_mail_status::public.mail_status_enum,v_now,'timesheets',v_timesheet,v_now,v_now,
    'received-caller-mail:'||v_workflow::text,
    jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1',
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_token,'UTF8'),'sha256'),'hex')
    ),
    case when p_active_lease then 'received-active-provider-permit' else null end,
    case when p_active_lease then v_now else null end,
    case when p_active_lease then v_now+interval '15 minutes' else null end
  );
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values(
    v_notification,v_account,v_candidate,v_workflow,v_timesheet,
    'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
    jsonb_build_object('workflow_generation',1),
    jsonb_build_object('type','paper_pack','workflow_id',v_workflow,'generation',1),
    'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow::text||':1:received-caller',v_now
  );

  begin
    v_result:=public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,upper(p_action),1,
      case when upper(p_action)='CANCEL'
        then jsonb_build_object('reason_note','Test RECEIVED cancellation.')
        else '{}'::jsonb end,
      'received-caller-action:'||v_workflow::text,v_now
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
    v_blocked:=true;
  end;

  if p_active_lease then
    if not v_blocked
       or (select state from public.candidate_submission_workflows where id=v_workflow)<>'RECEIVED'
       or (select qr_token from public.timesheets where timesheet_id=v_timesheet)<>v_token
       or (select state from public.candidate_notifications where id=v_notification)<>'UNREAD' then
      raise exception 'Active provider permit did not atomically block RECEIVED %',p_action;
    end if;
    return;
  end if;
  if v_blocked or v_result->>'state'<>v_expected_state
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>v_expected_state
     or (select qr_token from public.timesheets where timesheet_id=v_timesheet) is not null
     or (select state from public.candidate_notifications where id=v_notification)<>'DISMISSED'
     or not coalesce((select (deep_link_json->>'obsolete')::boolean
                      from public.candidate_notifications where id=v_notification),false) then
    raise exception 'RECEIVED % did not close delivery authority: result=%, workflow=%, qr=%, notification=%, obsolete=%',
      p_action,v_result,
      (select state from public.candidate_submission_workflows where id=v_workflow),
      (select qr_token from public.timesheets where timesheet_id=v_timesheet),
      (select state from public.candidate_notifications where id=v_notification),
      (select deep_link_json->>'obsolete' from public.candidate_notifications where id=v_notification);
  end if;
  if p_mail_status='SENT' then
    if (select status from public.mail_outbox where id=v_mail)<>'SENT'
       or coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                    from public.mail_outbox where id=v_mail),false) then
      raise exception 'RECEIVED % changed immutable SENT mail',p_action;
    end if;
  elsif not coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                       from public.mail_outbox where id=v_mail),false)
     or (select attachments from public.mail_outbox where id=v_mail)<>'[]'::jsonb then
    raise exception 'RECEIVED % did not make queued mail inert',p_action;
  end if;
end;
$function$;

select pg_temp.verify_received_cancel_or_supersede('CANCEL','QUEUED',false);
select pg_temp.verify_received_cancel_or_supersede('CANCEL','QUEUED',true);
select pg_temp.verify_received_cancel_or_supersede('CANCEL','SENT',false);
select pg_temp.verify_received_cancel_or_supersede('SUPERSEDE','QUEUED',false);
select pg_temp.verify_received_cancel_or_supersede('SUPERSEDE','QUEUED',true);
select pg_temp.verify_received_cancel_or_supersede('SUPERSEDE','SENT',false);

create or replace function pg_temp.verify_provider_submit_permit_barrier(p_mail_authority text)
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
  v_workflow uuid:=gen_random_uuid();
  v_component uuid:=gen_random_uuid();
  v_mail uuid:=gen_random_uuid();
  v_manifest jsonb;
  v_manifest_hash text;
  v_token text:='provider-permit-token-'||gen_random_uuid()::text;
  v_lease text:='provider-permit-lease-'||gen_random_uuid()::text;
  v_result jsonb;
  v_blocked boolean:=false;
  v_stale boolean:=false;
begin
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,'workflow_generation',1,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');
  insert into public.candidates(id,email,active)
  values(v_candidate,'permit-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name) values(v_client,'Provider permit client');
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
  ) values(v_timesheet,v_contract,'PROVIDER-PERMIT-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_token,'{}',v_now);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','permit-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');
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
  ) values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'provider-permit:'||v_workflow::text,v_manifest,decode(v_manifest_hash,'hex'),
    'CANDIDATE_REVIEW_DOCUMENTS_V1');
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,paper_return_page_key,
    review_render_state,final_signed_render_state
  ) values(v_component,v_workflow,1,1,v_timesheet,'SIGNED_RETURN','SIGNED_RETURN',
    'IMMUTABLE','candidate-app/test/provider/return.pdf','application/pdf',100,
    extensions.digest(convert_to('provider-return','UTF8'),'sha256'),v_now,false,
    'HOURS_TIMESHEET','NOT_REQUIRED','NOT_REQUIRED');
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json,
    attempt_lease_token,attempt_leased_at_utc,attempt_lease_expires_at_utc
  ) values(v_mail,'TIMESHEET_QR','permit@example.test','Provider permit pack','Pack ready',
    jsonb_build_array(jsonb_build_object(
      'r2_key','candidate-app/test/provider/pack.pdf',
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest_hash)),
    'QUEUED',v_now,'timesheets',v_timesheet,v_now,v_now,
    'provider-permit-mail:'||v_workflow::text,
    jsonb_build_object(
      'candidate_mail_authority',p_mail_authority,
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_token,'UTF8'),'sha256'),'hex')
    ),v_lease,v_now,v_now+interval '5 minutes');

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PROVIDER_SUBMIT_PERMIT',1,
    jsonb_build_object(
      'service_paper_provider_submit_permit',true,
      'mail_outbox_id',v_mail,'attempt_lease_token',v_lease,
      'paper_return_manifest_sha256',v_manifest_hash
    ),null,v_now
  );
  if not coalesce((v_result->>'provider_submit_permit')::boolean,false)
     or (select attempt_lease_expires_at_utc from public.mail_outbox where id=v_mail)
          <v_now+interval '15 minutes' then
    raise exception 'Atomic provider submit permit was not issued: %',v_result;
  end if;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'PAPER_RETURN',1,'{}'::jsonb,
      'provider-permit-return-blocked:'||v_workflow::text,v_now+interval '1 second'
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
    v_blocked:=true;
  end;
  if not v_blocked
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'AWAITING_PAPER_RETURN' then
    raise exception 'PAPER_RETURN crossed an active provider submit permit';
  end if;

  update public.mail_outbox
  set attempt_lease_token=null,attempt_leased_at_utc=null,attempt_lease_expires_at_utc=null
  where id=v_mail;
  perform public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_RETURN',1,'{}'::jsonb,
    'provider-permit-return-success:'||v_workflow::text,v_now+interval '2 seconds'
  );
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'PAPER_PROVIDER_SUBMIT_PERMIT',1,
      jsonb_build_object(
        'service_paper_provider_submit_permit',true,
        'mail_outbox_id',v_mail,'attempt_lease_token',v_lease,
        'paper_return_manifest_sha256',v_manifest_hash
      ),null,v_now+interval '3 seconds'
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE' then raise; end if;
    v_stale:=true;
  end;
  if not v_stale then
    raise exception 'Provider submit permit remained issuable after PAPER_RETURN';
  end if;
end;
$function$;

select pg_temp.verify_provider_submit_permit_barrier('CANDIDATE_PAPER_V1');
select pg_temp.verify_provider_submit_permit_barrier('CANDIDATE_PAPER_PACK_EMAIL_V1');

create or replace function pg_temp.verify_signed_paper_route_conversion(
  p_workflow_state text,
  p_mail_status text,
  p_active_lease boolean
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
  v_workflow uuid:=gen_random_uuid();
  v_mail uuid:=gen_random_uuid();
  v_notification uuid:=gen_random_uuid();
  v_generation integer;
  v_delivery_generation integer;
  v_manifest jsonb;
  v_manifest_hash text;
  v_token text:='signed-route-token-'||gen_random_uuid()::text;
  v_context jsonb;
  v_result jsonb;
  v_blocked boolean:=false;
begin
  if p_workflow_state not in ('RECEIVED','FINALISED')
     or p_mail_status not in ('QUEUED','SENT')
     or (p_mail_status='SENT' and p_active_lease) then
    raise exception 'Invalid signed PAPER route fixture';
  end if;
  v_generation:=case when p_workflow_state='FINALISED' then 2 else 1 end;
  v_delivery_generation:=case when p_workflow_state='FINALISED' then 1 else v_generation end;
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,'workflow_generation',v_delivery_generation,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_manifest_hash:=encode(private._candidate_sha256_jsonb_v1(v_manifest),'hex');

  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'route-paper-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name) values(v_client,'Signed PAPER route client');
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
    qr_signed_hash,qr_signed_at_utc
  ) values(v_timesheet,v_contract,'SIGNED-PAPER-ROUTE-'||replace(v_timesheet::text,'-',''),
    current_date,'HOURS','WEEKLY','MANUAL','PENDING',v_token,'{}',v_now,
    'signed-return-hash',v_now);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,0,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,basis,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','route-paper-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    p_workflow_state,v_generation,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'signed-paper-route:'||v_workflow::text,
    case when p_workflow_state='FINALISED' then v_now else null end,
    v_manifest,decode(v_manifest_hash,'hex'),'CANDIDATE_REVIEW_DOCUMENTS_V1');
  update public.timesheets
  set candidate_workflow_id=v_workflow,candidate_workflow_generation=v_generation
  where timesheet_id=v_timesheet;
  insert into public.mail_outbox(
    id,type,"to",subject,body_text,attachments,status,created_at_utc,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,
    deterministic_outbox_key,payment_scope_json,
    attempt_lease_token,attempt_leased_at_utc,attempt_lease_expires_at_utc
  ) values(v_mail,'TIMESHEET_QR','route@example.test','Signed PAPER pack','Pack ready',
    jsonb_build_array(jsonb_build_object('r2_key','candidate-app/test/route/pack.pdf')),
    p_mail_status::public.mail_status_enum,v_now,'timesheets',v_timesheet,v_now,v_now,
    'signed-paper-route-mail:'||v_workflow::text,
    jsonb_build_object(
      'candidate_workflow_id',v_workflow,
      'candidate_workflow_generation',v_delivery_generation,
      'paper_return_manifest_sha256',v_manifest_hash,
      'candidate_paper_pack_ready',true,'mail_held_until_pdf_rendered',false,
      'qr_token_hash',encode(extensions.digest(convert_to(v_token,'UTF8'),'sha256'),'hex')
    ),
    case when p_active_lease then 'route-active-provider-permit' else null end,
    case when p_active_lease then v_now else null end,
    case when p_active_lease then v_now+interval '15 minutes' else null end);
  insert into public.candidate_notifications(
    id,account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
  ) values(v_notification,v_account,v_candidate,v_workflow,v_timesheet,
    'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
    jsonb_build_object('workflow_generation',v_delivery_generation),
    jsonb_build_object('type','paper_pack','workflow_id',v_workflow,
      'generation',v_delivery_generation),
    'UNREAD','PENDING','CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow::text||':'
      ||v_delivery_generation::text||':signed-paper-route',v_now);

  v_context:=public.timesheet_route_version_preview_v1(v_timesheet,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_SIGNED_TO_MANUAL'
     or v_context->>'paper_workflow_id'<>v_workflow::text
     or not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'Signed PAPER route preview lost exact delivery owner: %',v_context;
  end if;
  begin
    v_result:=public.timesheet_route_version_confirmed_v1(
      v_timesheet,v_timesheet,v_context->>'row_signature',v_context->>'context_sha256',
      'CONVERT_QR_TO_MANUAL',v_actor,'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',null,
      'signed-paper-route-confirm:'||v_workflow::text,false,v_now
    );
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' then raise; end if;
    v_blocked:=true;
  end;

  if p_active_lease then
    if not v_blocked
       or (select count(*) from public.timesheets
           where booking_id='SIGNED-PAPER-ROUTE-'||replace(v_timesheet::text,'-',''))<>1
       or (select state from public.candidate_submission_workflows where id=v_workflow)<>p_workflow_state
       or (select qr_token from public.timesheets where timesheet_id=v_timesheet)<>v_token then
      raise exception 'Active permit did not block signed PAPER route conversion';
    end if;
    return;
  end if;
  if v_blocked
     or nullif(v_result->>'new_timesheet_id','') is null
     or (select state from public.candidate_notifications where id=v_notification)<>'DISMISSED'
     or exists(select 1 from public.timesheets
       where booking_id='SIGNED-PAPER-ROUTE-'||replace(v_timesheet::text,'-','')
         and is_current=true and qr_token is not null) then
    raise exception 'Signed PAPER route conversion did not retire delivery first: %',v_result;
  end if;
  if p_workflow_state='FINALISED' then
    if (select state from public.candidate_submission_workflows where id=v_workflow)<>'FINALISED'
       or (select generation from public.candidate_submission_workflows where id=v_workflow)<>2 then
      raise exception 'Finalised signed PAPER history was mutated by route conversion';
    end if;
  elsif (select state from public.candidate_submission_workflows where id=v_workflow)<>'SUPERSEDED' then
    raise exception 'RECEIVED PAPER workflow was not superseded after retirement';
  end if;
  if p_mail_status='SENT' then
    if (select status from public.mail_outbox where id=v_mail)<>'SENT' then
      raise exception 'Signed PAPER route conversion changed SENT mail history';
    end if;
  elsif not coalesce((select (payment_scope_json->>'candidate_paper_generation_retired')::boolean
                       from public.mail_outbox where id=v_mail),false) then
    raise exception 'Signed PAPER route conversion did not retire queued mail';
  end if;
end;
$function$;

select pg_temp.verify_signed_paper_route_conversion('RECEIVED','QUEUED',false);
select pg_temp.verify_signed_paper_route_conversion('RECEIVED','QUEUED',true);
select pg_temp.verify_signed_paper_route_conversion('RECEIVED','SENT',false);
select pg_temp.verify_signed_paper_route_conversion('FINALISED','QUEUED',false);
select pg_temp.verify_signed_paper_route_conversion('FINALISED','QUEUED',true);
select pg_temp.verify_signed_paper_route_conversion('FINALISED','SENT',false);

rollback;
