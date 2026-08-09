-- Final compatibility, DAILY-intent, workflow-retirement and notification timing proof.
-- Disposable PostgreSQL only. Every fixture row is rolled back.

begin;

do $final_route_closure$
declare
  v_client uuid:='95500000-0000-0000-0000-000000000001';
  v_candidate uuid:='95500000-0000-0000-0000-000000000002';
  v_contract uuid:='95500000-0000-0000-0000-000000000003';
  v_actor uuid:='95500000-0000-0000-0000-000000000004';
  v_account uuid:='95500000-0000-0000-0000-000000000005';
  v_manual uuid:='95500000-0000-0000-0000-000000000010';
  v_manual_week uuid:='95500000-0000-0000-0000-000000000011';
  v_qr uuid:='95500000-0000-0000-0000-000000000020';
  v_qr_week uuid:='95500000-0000-0000-0000-000000000021';
  v_qr_workflow uuid:='95500000-0000-0000-0000-000000000022';
  v_daily uuid:='95500000-0000-0000-0000-000000000030';
  v_conflict uuid:='95500000-0000-0000-0000-000000000040';
  v_conflict_week uuid:='95500000-0000-0000-0000-000000000041';
  v_expired uuid:='95500000-0000-0000-0000-000000000050';
  v_expired_week uuid:='95500000-0000-0000-0000-000000000051';
  v_expired_workflow uuid:='95500000-0000-0000-0000-000000000052';
  v_expired_request uuid:='95500000-0000-0000-0000-000000000053';
  v_context jsonb;
  v_result jsonb;
  v_new uuid;
  v_document uuid:=gen_random_uuid();
begin
  -- Dormant installation must preserve the exact legacy public behaviour and
  -- the currently installed authenticated/service-only QR restore ACL.
  v_result:=public.timesheet_route_version_rotate(
    v_manual,v_manual,'ALLOW_QR_AGAIN',v_actor,false
  );
  if not coalesce((v_result->>'legacy_route_rotate')::boolean,false) then
    raise exception 'feature-off route wrapper did not delegate to exact legacy owner';
  end if;
  if has_function_privilege('anon','public.timesheet_qr_restore_version(uuid,uuid,text,uuid)','EXECUTE')
     or not has_function_privilege('authenticated','public.timesheet_qr_restore_version(uuid,uuid,text,uuid)','EXECUTE')
     or not has_function_privilege('service_role','public.timesheet_qr_restore_version(uuid,uuid,text,uuid)','EXECUTE') then
    raise exception 'QR restore ACL does not match the installed compatibility contract';
  end if;
  if (select not prosecdef or proconfig is distinct from array['search_path=public']::text[]
      from pg_proc where oid='public.timesheet_qr_restore_version(uuid,uuid,text,uuid)'::regprocedure) then
    raise exception 'QR restore security/search_path compatibility drifted';
  end if;

  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set
    candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||'{"candidate_route_confirmation":true,"candidate_app_writes":true}'::jsonb,
    candidate_app_system_actor_user_id=v_actor
  where id=1;
  insert into public.clients(id,name) values(v_client,'Final Route Closure Client');
  -- Deliberately no GCK: CONTRACT notification resolution must use contract
  -- ownership, not DAILY entitlement identity.
  insert into public.candidates(id,email,active,key_norm,pay_method)
  values(v_candidate,'final-route@example.test',true,null,'PAYE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,'2026-01-01','ELECTRONIC',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,overrideclientsettings,weekly_timesheet_source,role,band
  ) values(v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,
    'ELECTRONIC',true,'NONE','NURSE','Band 5');
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values(v_account,'TEST','final-route@example.test','ACTIVE',
    '{"resubmission_required":true}'::jsonb);

  -- CONTRACT candidate with no GCK receives the fresh ELECTRONIC notification.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status
  ) values(v_manual,v_contract,'FINAL-MANUAL','2026-08-08','WEEKLY','MANUAL','HOURS','RECEIVED');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_manual_week,v_contract,'2026-08-08','OPEN','MANUAL',v_manual);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_manual,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_manual,'ALLOW_ELECTRONIC_AGAIN');
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_manual,v_manual,v_context->>'row_signature',v_context->>'context_sha256',
    'ALLOW_ELECTRONIC_AGAIN',v_actor,null,null,'final-contract-notification',false,now()
  );
  v_new:=(v_result->>'new_timesheet_id')::uuid;
  if not coalesce((v_result->>'notification_created')::boolean,false)
     or coalesce((v_result->>'notification_recipient_unavailable')::boolean,true)
     or (select count(*) from public.candidate_notifications
         where account_id=v_account and timesheet_id=v_new)<>1 then
    raise exception 'CONTRACT candidate without GCK did not receive one notification: %',v_result;
  end if;

  -- An unsigned DAILY row remains physically MANUAL but is logically
  -- ELECTRONIC through one constrained server-owned pending intent.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,candidate_submission_route_intent,
    scheduled_start_iso,scheduled_end_iso
  ) values(v_daily,v_contract,'FINAL-DAILY','2026-08-09','DAILY','MANUAL','HOURS',
    'RECEIVED','ELECTRONIC','2026-08-09 08:00:00+01','2026-08-09 18:00:00+01');
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_daily,v_candidate,v_client,'SELF_REPORTED','UNPROCESSED',9);
  if private._candidate_route_family_v1(v_daily,null)->>'route_family'<>'ELECTRONIC'
     or private._candidate_route_family_v1(v_daily,null)->>'pending_route_intent'<>'ELECTRONIC' then
    raise exception 'DAILY pending ELECTRONIC intent was not consumed by route authority';
  end if;

  -- Unexpected active-workflow multiplicity fails closed before any warning
  -- can claim a transition is safe.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,r2_nurse_key,r2_auth_key
  ) values(v_conflict,v_contract,'FINAL-CONFLICT','2026-08-15','WEEKLY',
    'ELECTRONIC','HOURS','RECEIVED','candidate/conflict','manager/conflict');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_conflict_week,v_contract,'2026-08-15','OPEN','ELECTRONIC',v_conflict);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_conflict,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_submission_workflows(
    environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key
  ) values
    ('TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL','CREATED',1,
      v_contract,v_conflict_week,v_conflict,v_conflict,'2026-08-15','conflict-one'),
    ('TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL','CREATED',1,
      v_contract,v_conflict_week,v_conflict,v_conflict,'2026-08-15','conflict-two');
  v_context:=public.timesheet_route_version_preview_v1(v_conflict,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'ROUTE_CHANGE_WORKFLOW_CONFLICT'
     or coalesce((v_context->>'active_workflow_count')::integer,0)<>2
     or coalesce((v_context->>'permitted_action')::boolean,true) then
    raise exception 'multiple active workflows did not fail closed: %',v_context;
  end if;
  if (public.timesheet_route_version_preview_v1(v_conflict,'SWITCH_DAILY_TO_MANUAL')->>'permitted_action')::boolean then
    raise exception 'preview accepted a DAILY action against a WEEKLY record';
  end if;

  -- Expired manager links are not live, are normalised to EXPIRED during the
  -- route transaction, and do not receive a withdrawal email.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,r2_nurse_key,r2_auth_key
  ) values(v_expired,v_contract,'FINAL-EXPIRED','2026-08-22','WEEKLY',
    'ELECTRONIC','HOURS','RECEIVED','candidate/expired','manager/expired');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_expired_week,v_contract,'2026-08-22','OPEN','ELECTRONIC',v_expired);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_expired,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key
  ) values(v_expired_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'EMAIL','AWAITING_MANAGER_APPROVAL',1,v_contract,v_expired_week,v_expired,v_expired,
    '2026-08-22','expired-workflow');
  update public.timesheets set candidate_workflow_id=v_expired_workflow,
    candidate_workflow_generation=1 where timesheet_id=v_expired;
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    manager_email_normalized,token_hash,expires_at_utc,initial_sent_at_utc,idempotency_key,
    review_manifest_sha256,required_component_ids,required_component_manifest_json
  ) values(v_expired_request,v_expired_workflow,1,1,'EMAIL','PENDING',
    'expired-manager@example.test',extensions.digest('expired-token','sha256'),
    now()-interval '1 minute',now()-interval '1 day','expired-request',
    extensions.digest('expired-manifest','sha256'),array[v_expired_workflow],
    jsonb_build_array(jsonb_build_object('component_id',v_expired_workflow)));
  v_context:=public.timesheet_route_version_preview_v1(v_expired,'SWITCH_TO_MANUAL');
  if coalesce((v_context->>'manager_approval_pending')::boolean,true) then
    raise exception 'expired manager request was classified as live: %',v_context;
  end if;
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_expired,v_expired,v_context->>'row_signature',v_context->>'context_sha256',
    'SWITCH_TO_MANUAL',v_actor,'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',null,
    'expired-route-change',false,now()
  );
  if (select state from public.candidate_approval_requests where id=v_expired_request)<>'EXPIRED'
     or exists(select 1 from public.mail_outbox
       where deterministic_outbox_key='CANDIDATE_MANAGER_APPROVAL_WITHDRAWN_V1:'||v_expired_request::text) then
    raise exception 'expired request was withdrawn/emailed instead of normalised';
  end if;

  -- QR replacement retires the old paper workflow immediately, but the
  -- worker push is released only when the replacement document is READY.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at,
    manual_pdf_r2_key,qr_last_sent_at_utc,qr_last_sent_hash
  ) values(v_qr,v_contract,'FINAL-QR','2026-08-29','WEEKLY','MANUAL','HOURS',
    'RECEIVED','PENDING','old-qr-token',now(),'final-qr/issued-pack.pdf',now(),'old-issued-hash');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_qr_week,v_contract,'2026-08-29','OPEN','MANUAL',v_qr);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_qr,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key
  ) values(v_qr_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_qr_week,v_qr,v_qr,'2026-08-29','qr-old-workflow');
  update public.timesheets set candidate_workflow_id=v_qr_workflow,
    candidate_workflow_generation=1 where timesheet_id=v_qr;
  v_context:=public.timesheet_route_version_preview_v1(v_qr,'REISSUE_QR');
  v_result:=public.timesheet_route_version_confirmed_v1(
    v_qr,v_qr,v_context->>'row_signature',v_context->>'context_sha256',
    'REISSUE_QR',v_actor,null,null,'qr-reissue-final',false,now()
  );
  v_new:=(v_result->>'new_timesheet_id')::uuid;
  if (select state from public.candidate_submission_workflows where id=v_qr_workflow)<>'SUPERSEDED'
     or not coalesce((v_result->>'notification_deferred_until_pack_ready')::boolean,false)
     or exists(select 1 from public.candidate_notifications where timesheet_id=v_new)
     or (select candidate_submission_route_intent from public.timesheets where timesheet_id=v_new)<>'PAPER' then
    raise exception 'QR replacement retirement/deferred notification failed: %',v_result;
  end if;
  insert into public.invoice_document_versions(
    id,entity_type,entity_id,purpose,source_revision,template_version,status
  ) values(v_document,'TIMESHEET',v_new,'TIMESHEET_QR','2','TIMESHEET_OFFICIAL_PDF_V1','READY');
  update public.timesheets set document_state='READY',current_document_version_id=v_document
  where timesheet_id=v_new;
  if (select candidate_submission_route_intent from public.timesheets where timesheet_id=v_new) is not null
     or (select count(*) from public.candidate_notifications
         where account_id=v_account and timesheet_id=v_new and event_type='RESUBMISSION_REQUIRED')<>1 then
    raise exception 'QR pack-ready transaction did not release one worker notification';
  end if;
end;
$final_route_closure$;

rollback;
