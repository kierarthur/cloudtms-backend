-- Final signed-state route-conversion, notification and retention verification.
-- Disposable PostgreSQL only. Every fixture row is rolled back.

begin;

do $candidate_route_confirmation_runtime$
declare
  v_client uuid:='95400000-0000-0000-0000-000000000001';
  v_candidate uuid:='95400000-0000-0000-0000-000000000002';
  v_contract uuid:='95400000-0000-0000-0000-000000000003';
  v_actor uuid:='95400000-0000-0000-0000-000000000004';
  v_account uuid:='95400000-0000-0000-0000-000000000005';
  v_electronic uuid:='95400000-0000-0000-0000-000000000010';
  v_week uuid:='95400000-0000-0000-0000-000000000011';
  v_fin uuid:='95400000-0000-0000-0000-000000000012';
  v_workflow uuid:='95400000-0000-0000-0000-000000000013';
  v_signature uuid:='95400000-0000-0000-0000-000000000014';
  v_request uuid:='95400000-0000-0000-0000-000000000015';
  v_context jsonb;
  v_changed_context jsonb;
  v_result jsonb;
  v_manual uuid;
  v_resubmitted uuid;
begin
  update public.settings_defaults set
    candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||jsonb_build_object('candidate_route_confirmation',true)
  where id=1;
  insert into public.clients(id,name) values(v_client,'Route Confirmation Client');
  insert into public.candidates(id,email,active,key_norm,pay_method)
  values(v_candidate,'route-confirm@example.test',true,'ROUTE-CONFIRM-GCK','PAYE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,is_nhsp,requires_hr,
    no_timesheet_required,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,'2026-01-01','MANUAL',false,false,false,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,overrideclientsettings,weekly_timesheet_source,role,band
  ) values(
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,
    'ELECTRONIC',true,'NONE','NURSE','Band 5'
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values(
    v_account,'TEST','route-confirm@example.test','ACTIVE',
    jsonb_build_object('resubmission_required',true)
  );
  insert into public.timesheets(
    timesheet_id,version,status,submission_mode,line_type,sheet_scope,is_current,
    contract_id,booking_id,week_ending_date,occupant_key_norm,worked_start_iso,
    worked_end_iso,break_minutes,worked_minutes,r2_nurse_key,r2_auth_key
  ) values(
    v_electronic,1,'RECEIVED','MANUAL','HOURS','WEEKLY',true,v_contract,
    'ROUTE-CONFIRM-ELECTRONIC','2026-08-08','ROUTE-CONFIRM-GCK',
    '2026-08-03 08:00:00+01','2026-08-03 18:00:00+01',60,540,
    'candidate/route-confirm/current',null
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,'2026-08-08',0,'SUBMITTED','ELECTRONIC',v_electronic);
  insert into public.timesheets_financials(
    id,timesheet_id,candidate_id,client_id,is_current,timesheet_version,basis,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat
  ) values(v_fin,v_electronic,v_candidate,v_client,true,1,'CONTRACT_WEEKLY',
    'UNPROCESSED',9,90,180,90);
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_electronic,v_electronic,
    '2026-08-08','route-confirm-workflow'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,storage_key,media_type,byte_size,source_content_sha256,
    immutable_at_utc,required,review_render_state,final_signed_render_state
  ) values(
    v_signature,v_workflow,1,1,v_electronic,'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE',
    'IMMUTABLE','candidate/route-confirm/signature.png','image/png',100,
    extensions.digest('route-confirm-signature','sha256'),now(),false,'NOT_REQUIRED','NOT_REQUIRED'
  );
  update public.candidate_submission_workflows set
    candidate_signature_component_id=v_signature,
    candidate_signature_sha256=extensions.digest('route-confirm-signature','sha256'),
    candidate_signed_at_utc=now()
  where id=v_workflow;
  update public.timesheets set
    candidate_workflow_id=v_workflow,candidate_workflow_generation=1
  where timesheet_id=v_electronic;
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,initial_sent_at_utc,
    last_sent_at_utc,review_manifest_sha256,required_component_ids,
    required_component_manifest_json,idempotency_key
  ) values(
    v_request,v_workflow,1,1,'EMAIL','PENDING','manager@example.test',
    extensions.digest('route-confirm-token','sha256'),now()+interval '7 days',now(),now(),
    extensions.digest('route-confirm-manifest','sha256'),array[v_signature],
    jsonb_build_array(jsonb_build_object('component_id',v_signature)),
    'route-confirm-request'
  );
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,document_role,processing_state
  ) values(
    v_electronic,'TIMESHEET','Candidate signed electronic submission',
    'candidate/route-confirm/signed.pdf','SIGNED_TIMESHEET','READY'
  );

  v_context:=public.timesheet_route_version_preview_v1(v_electronic,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL'
     or not coalesce((v_context->>'reason_required')::boolean,false)
     or nullif(v_context->>'context_sha256','') is null then
    raise exception 'signed/pending route context is incomplete: %',v_context;
  end if;
  begin
    perform public.timesheet_route_version_rotate(
      v_electronic,v_electronic,'SWITCH_TO_MANUAL',v_actor,false
    );
    raise exception 'legacy route function bypassed confirmed transition';
  exception when others then
    if sqlerrm<>'ROUTE_CHANGE_CONFIRMATION_REQUIRED' then raise; end if;
  end;
  -- A manager state change after preview invalidates the context before any
  -- route, workflow, evidence or TSFIN mutation can occur.
  update public.candidate_approval_requests set
    state='APPROVED',approved_at_utc=now(),updated_at_utc=now()
  where id=v_request;
  v_changed_context:=public.timesheet_route_version_preview_v1(v_electronic,'SWITCH_TO_MANUAL');
  if v_changed_context->>'warning_code'<>'MANAGER_APPROVED_TO_MANUAL' then
    raise exception 'manager-approved warning classification failed: %',v_changed_context;
  end if;
  begin
    perform public.timesheet_route_version_confirmed_v1(
      v_electronic,v_electronic,v_context->>'current_row_signature',
      v_context->>'context_sha256','SWITCH_TO_MANUAL',v_actor,
      'CANDIDATE_REPORTED_HOURS_INCORRECT',null,'route-confirm-race',false,now()
    );
    raise exception 'stale manager-state context was accepted';
  exception when others then
    if sqlerrm<>'ROUTE_CHANGE_CONTEXT_CHANGED' then raise; end if;
  end;
  update public.candidate_approval_requests set
    state='PENDING',approved_at_utc=null,updated_at_utc=now()
  where id=v_request;
  insert into public.mail_outbox(
    type,"to",subject,body_text,status,context_kind,context_id,
    deterministic_outbox_key,payment_scope_json,sent_at,provider_status,provider_message_id
  ) values(
    'TIMESHEET_GENERAL','manager@example.test','Approval requested','Please review.',
    'SENT','CANDIDATE_WORKFLOW',v_workflow,
    'CANDIDATE_MANAGER_APPROVAL_V1:'||v_request::text||':0',
    jsonb_build_object(
      'candidate_mail_authority','MANAGER_APPROVAL_V1',
      'candidate_manager_mail_kind','INITIAL',
      'candidate_manager_workflow_id',v_workflow,
      'candidate_manager_workflow_generation',1,
      'candidate_approval_request_id',v_request,
      'candidate_approval_request_generation',1,
      'candidate_manager_mail_retired',false
    ),now(),'ACCEPTED','provider-route-confirm'
  );
  v_context:=public.timesheet_route_version_preview_v1(v_electronic,'SWITCH_TO_MANUAL');
  begin
    perform public.timesheet_route_version_confirmed_v1(
      v_electronic,v_electronic,v_context->>'current_row_signature',
      v_context->>'context_sha256','SWITCH_TO_MANUAL',v_actor,
      null,null,'route-confirm-1',false,now()
    );
    raise exception 'missing intervention reason was accepted';
  exception when others then
    if sqlerrm<>'ROUTE_INTERVENTION_REASON_REQUIRED' then raise; end if;
  end;

  v_result:=public.timesheet_route_version_confirmed_v1(
    v_electronic,v_electronic,v_context->>'current_row_signature',
    v_context->>'context_sha256','SWITCH_TO_MANUAL',v_actor,
    'CANDIDATE_REPORTED_HOURS_INCORRECT','Candidate asked the office to intervene.',
    'route-confirm-2',false,now()
  );
  v_manual:=(v_result->>'new_timesheet_id')::uuid;
  if v_manual is null
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'SUPERSEDED'
     or (select state from public.candidate_approval_requests where id=v_request)<>'CANCELLED'
     or (select state from public.candidate_submission_components where id=v_signature)<>'SUPERSEDED'
     or not coalesce((v_result#>>'{workflow_retirement,manager_cancellation_email_queued}')::boolean,false)
     or not coalesce((v_result->>'retain_historical_evidence')::boolean,false) then
    raise exception 'confirmed electronic-to-manual retirement incomplete: %',v_result;
  end if;
  if not exists(
    select 1 from public.mail_outbox
    where deterministic_outbox_key='CANDIDATE_MANAGER_APPROVAL_WITHDRAWN_V1:'||v_request::text
      and body_text='The approval request for this timesheet has been withdrawn by CloudTMS. No further action is required.'
  ) then raise exception 'manager cancellation email was not queued exactly once'; end if;
  if not exists(
    select 1 from public.timesheet_evidence
    where timesheet_id=v_electronic and storage_key='candidate/route-confirm/signed.pdf'
  ) then raise exception 'historical signed evidence was deleted'; end if;
  if not exists(
    select 1 from public.timesheets
    where timesheet_id=v_electronic and revoked_reason=
      'SWITCHED_TO_MANUAL:CANDIDATE_REPORTED_HOURS_INCORRECT'
  ) then raise exception 'intervention reason missing from revoked generation'; end if;

  -- Contract override wins over the MANUAL client default. A fresh electronic
  -- generation is created and one idempotent in-app/push notification is queued.
  v_context:=public.timesheet_route_version_preview_v1(v_manual,'ALLOW_ELECTRONIC_AGAIN');
  if v_context->>'warning_code'<>'FRESH_ELECTRONIC_RESUBMISSION_REQUIRED'
     or v_context->>'effective_submission_mode'<>'ELECTRONIC' then
    raise exception 'effective contract-over-client electronic policy failed: %',v_context;
  end if;
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_manual,v_manual,v_context->>'current_row_signature',v_context->>'context_sha256',
    'ALLOW_ELECTRONIC_AGAIN',v_actor,null,null,'route-confirm-3',false,now()
  );
  v_resubmitted:=(v_result->>'new_timesheet_id')::uuid;
  if v_resubmitted is null
     or not coalesce((v_result->>'fresh_submission_required')::boolean,false)
     or not coalesce((v_result->>'notification_created')::boolean,false)
     or coalesce((v_result->>'notification_recipient_unavailable')::boolean,true)
     or (select submission_mode from public.timesheets where timesheet_id=v_resubmitted)<>'MANUAL'
     or (select submission_mode_snapshot from public.contract_weeks where id=v_week)<>'ELECTRONIC'
     or (select r2_nurse_key from public.timesheets where timesheet_id=v_resubmitted) is not null
     or (select r2_auth_key from public.timesheets where timesheet_id=v_resubmitted) is not null then
    raise exception 'fresh electronic resubmission/notification failed: %',v_result;
  end if;
  if (select count(*) from public.candidate_notifications
      where timesheet_id=v_resubmitted and event_type='RESUBMISSION_REQUIRED')<>1 then
    raise exception 'resubmission notification was not idempotently unique';
  end if;

  -- Ordinary historical QR restore is unavailable after coordinated cutover.
  begin
    perform public.timesheet_qr_restore_version(v_electronic,v_electronic,'SIGNED',v_actor);
    raise exception 'retired QR restore remained reachable';
  exception when others then
    if sqlerrm<>'QR_RESTORE_RETIRED_USE_FRESH_GENERATION' then raise; end if;
  end;
end;
$candidate_route_confirmation_runtime$;

rollback;
