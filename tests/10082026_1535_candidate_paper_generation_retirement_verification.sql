\set ON_ERROR_STOP on
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":true,"candidate_paper_qr":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

do $verify_release_and_amend$
declare
  v_candidate constant uuid:='b5100000-0000-4000-8000-000000000001';
  v_client constant uuid:='b5100000-0000-4000-8000-000000000002';
  v_contract constant uuid:='b5100000-0000-4000-8000-000000000003';
  v_timesheet constant uuid:='b5100000-0000-4000-8000-000000000004';
  v_week constant uuid:='b5100000-0000-4000-8000-000000000005';
  v_account constant uuid:='b5100000-0000-4000-8000-000000000006';
  v_session constant uuid:='b5100000-0000-4000-8000-000000000007';
  v_workflow constant uuid:='b5100000-0000-4000-8000-000000000008';
  v_mail constant uuid:='b5100000-0000-4000-8000-000000000009';
  v_sent_mail constant uuid:='b5100000-0000-4000-8000-000000000010';
  v_manifest jsonb;
  v_manifest_sha bytea;
  v_manifest_hex text;
  v_base_sha constant text:=repeat('b',64);
  v_branding_sha constant text:=repeat('c',64);
  v_pack_sha constant text:=repeat('d',64);
  v_renderer constant text:='CANDIDATE_REVIEW_DOCUMENTS_V1';
  v_pack_key text;
  v_result jsonb;
  v_claimed uuid[];
  v_scope jsonb;
  v_attachments jsonb;
  v_state text;
  v_push_state text;
  v_qr_token text;
  v_generation integer;
begin
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,'workflow_generation',1,
    'immutable_submission_sha256',repeat('a',64),
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_manifest_sha:=private._candidate_sha256_jsonb_v1(v_manifest);
  v_manifest_hex:=encode(v_manifest_sha,'hex');
  v_pack_key:='candidate-app/test/'||v_workflow::text||'/1/paper-pack/'
    ||v_manifest_hex||'-'||v_base_sha||'-'||v_branding_sha||'-'||v_renderer||'.pdf';

  insert into public.candidates(id,email,active)
  values(v_candidate,'paper-retirement@example.test',true);
  insert into public.clients(id,name) values(v_client,'Paper retirement client');
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date)
  values(v_contract,v_candidate,v_client,'2026-01-01','2026-12-31');
  insert into public.timesheets(
    timesheet_id,submission_mode,sheet_scope,contract_id,booking_id,week_ending_date,
    qr_status,qr_token,qr_payload_json,qr_generated_at,document_state
  ) values(
    v_timesheet,'MANUAL','WEEKLY',v_contract,'PAPER-RETIRE-1','2026-08-16',
    'PENDING','paper-retirement-token',jsonb_build_object('v',1,'tok','paper-retirement-token'),
    now(),'READY'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,'2026-08-16','SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours
  ) values(v_timesheet,v_candidate,v_client,true,'AWAITING_MANUAL_SIGNATURE',8);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','paper-retirement@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('ef',32),'hex'),
    now()+interval '1 day',now()+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,immutable_submission_json,immutable_submission_sha256,
    paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',1,v_contract,v_week,v_timesheet,v_timesheet,'2026-08-16',
    'paper-retirement-workflow',
    jsonb_build_object('official_presentation',jsonb_build_object(
      'renderer_contract_version',v_renderer,
      'branding',jsonb_build_object('branding_contract_sha256',v_branding_sha))),
    decode(repeat('a',64),'hex'),v_manifest,v_manifest_sha,v_renderer
  );
  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,reference,payment_scope_json
  ) values(
    v_mail,'TIMESHEET_QR','paper-retirement@example.test','Paper pack','[]'::jsonb,
    'QUEUED',now(),'timesheets',v_timesheet,'infinity','infinity',
    'candidate-paper-retirement:1',jsonb_build_object(
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',v_manifest_hex,
      'candidate_paper_pack_ready',false,'mail_held_until_pdf_rendered',true,
      'mail_delayed_for_pdf_render',true,'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
      'qr_token_hash',encode(digest('paper-retirement-token','sha256'),'hex')
    )
  );

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PACK_RELEASE',1,
    jsonb_build_object(
      'service_paper_pack_release',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hex,
      'complete_pack_storage_key',v_pack_key,'complete_pack_sha256',v_pack_sha,
      'complete_pack_byte_size',500,'complete_pack_page_count',1,
      'complete_pack_media_type','application/pdf','base_document_sha256',v_base_sha,
      'branding_contract_sha256',v_branding_sha,'renderer_contract_version',v_renderer
    ),'paper-release-once',now()
  );
  if not coalesce((v_result->>'ok')::boolean,false)
     or v_result->>'mail_outbox_id'<>v_mail::text then
    raise exception 'Atomic PAPER release did not return its durable receipt: %',v_result;
  end if;

  select payment_scope_json,attachments into v_scope,v_attachments
  from public.mail_outbox where id=v_mail;
  if lower(v_scope->>'candidate_paper_pack_ready')<>'true'
     or v_scope->>'mail_hold_reason' is not null
     or jsonb_array_length(v_attachments)<>1
     or v_attachments->0->>'r2_key'<>v_pack_key then
    raise exception 'Atomic PAPER release did not install the exact complete pack';
  end if;
  if (select count(*) from public.candidate_notifications
      where workflow_id=v_workflow and event_type='PAPER_PACK_READY')<>1 then
    raise exception 'Atomic PAPER release did not insert exactly one readiness notification';
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PACK_RELEASE',1,
    jsonb_build_object(
      'service_paper_pack_release',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hex,
      'complete_pack_storage_key',v_pack_key,'complete_pack_sha256',v_pack_sha,
      'complete_pack_byte_size',500,'complete_pack_page_count',1,
      'complete_pack_media_type','application/pdf','base_document_sha256',v_base_sha,
      'branding_contract_sha256',v_branding_sha,'renderer_contract_version',v_renderer
    ),'paper-release-retry',now()
  );
  if not coalesce((v_result->>'idempotent_replay')::boolean,false)
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_workflow and event_type='PAPER_PACK_READY')<>1 then
    raise exception 'Atomic PAPER release replay was not idempotent: %',v_result;
  end if;

  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,sent_at,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,reference,payment_scope_json
  ) values(
    v_sent_mail,'TIMESHEET_QR','paper-retirement@example.test','Historical sent pack',
    v_attachments,'SENT',now(),now(),'timesheets',v_timesheet,now(),now(),
    'candidate-paper-retirement:sent-history',v_scope
  );

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'AMEND',1,
    jsonb_build_object('input_snapshot',jsonb_build_object('amended',true)),
    'paper-amend-after-ready',now()
  );
  if v_result->>'state'<>'WORKER_DRAFT' or (v_result->>'generation')::integer<>2 then
    raise exception 'PAPER amendment did not create the next workflow generation: %',v_result;
  end if;
  select generation into v_generation from public.candidate_submission_workflows where id=v_workflow;
  if v_generation<>2 then raise exception 'PAPER workflow generation did not advance'; end if;

  select payment_scope_json,attachments into v_scope,v_attachments
  from public.mail_outbox where id=v_mail;
  if lower(v_scope->>'candidate_paper_generation_retired')<>'true'
     or v_scope->>'mail_hold_reason'<>'CANDIDATE_PAPER_GENERATION_RETIRED'
     or jsonb_array_length(v_attachments)<>0
     or v_scope#>>'{candidate_retired_delivery_receipt,candidate_complete_pack_storage_key}'<>v_pack_key
     or (select status::text from public.mail_outbox where id=v_sent_mail)<>'SENT' then
    raise exception 'PAPER amendment did not retire delivery while preserving sent history';
  end if;
  select state,push_state into v_state,v_push_state
  from public.candidate_notifications
  where workflow_id=v_workflow and event_type='PAPER_PACK_READY';
  if v_state<>'DISMISSED' or v_push_state<>'SKIPPED' then
    raise exception 'Retired PAPER readiness notification remained actionable';
  end if;
  select qr_token into v_qr_token from public.timesheets where timesheet_id=v_timesheet;
  if v_qr_token is not null then raise exception 'Retired PAPER QR token remained active'; end if;

  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'retired-paper-claim',5) claimed;
  if v_claimed @> array[v_mail] or v_claimed @> array[v_sent_mail] then
    raise exception 'Retired PAPER delivery became claimable: %',v_claimed;
  end if;
end;
$verify_release_and_amend$;

do $verify_provider_lease_blocks_retirement$
declare
  v_workflow constant uuid:='b5100000-0000-4000-8000-000000000008';
  v_session constant uuid:='b5100000-0000-4000-8000-000000000007';
  v_timesheet constant uuid:='b5100000-0000-4000-8000-000000000004';
  v_mail constant uuid:='b5100000-0000-4000-8000-000000000011';
  v_manifest jsonb;
  v_manifest_sha bytea;
  v_failed boolean:=false;
begin
  v_manifest:=jsonb_build_object(
    'workflow_id',v_workflow,'workflow_generation',2,
    'immutable_submission_sha256',repeat('a',64),
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  );
  v_manifest_sha:=private._candidate_sha256_jsonb_v1(v_manifest);
  update public.candidate_submission_workflows
  set route='PAPER',state='AWAITING_PAPER_RETURN',
      paper_return_manifest_json=v_manifest,paper_return_manifest_sha256=v_manifest_sha
  where id=v_workflow;
  update public.timesheets
  set qr_token='leased-paper-token',qr_payload_json=jsonb_build_object('v',1,'tok','leased-paper-token'),
      qr_generated_at=now()
  where timesheet_id=v_timesheet;
  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,reference,attempt_lease_token,
    attempt_leased_at_utc,attempt_lease_expires_at_utc,payment_scope_json
  ) values(
    v_mail,'TIMESHEET_QR','paper-retirement@example.test','Leased paper pack','[]'::jsonb,
    'QUEUED',now(),'timesheets',v_timesheet,'infinity','infinity','candidate-paper-lease',
    'provider-lease',now(),now()+interval '5 minutes',jsonb_build_object(
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',2,
      'paper_return_manifest_sha256',encode(v_manifest_sha,'hex'),
      'candidate_paper_pack_ready',false,'mail_held_until_pdf_rendered',true,
      'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
      'qr_token_hash',encode(digest('leased-paper-token','sha256'),'hex')
    )
  );
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_workflow,'CANCEL',2,'{}'::jsonb,'paper-cancel-during-lease',now()
    );
  exception when sqlstate '40001' then
    v_failed:=sqlerrm='CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS';
  end;
  if not v_failed then raise exception 'Active provider lease did not block PAPER retirement'; end if;
  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'AWAITING_PAPER_RETURN'
     or (select generation from public.candidate_submission_workflows where id=v_workflow)<>2
     or (select attempt_lease_token from public.mail_outbox where id=v_mail)<>'provider-lease' then
    raise exception 'Lease conflict partially mutated PAPER workflow or delivery';
  end if;
end;
$verify_provider_lease_blocks_retirement$;

rollback;
