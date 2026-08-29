-- Canonical QR/version action-matrix runtime verification.
-- Run only in a disposable database after the complete Candidate App bundle.
-- Every row created by this file is rolled back.

begin;

do $candidate_qr_version_runtime$
declare
  v_client uuid := '95300000-0000-0000-0000-000000000001';
  v_candidate uuid := '95300000-0000-0000-0000-000000000002';
  v_contract uuid := '95300000-0000-0000-0000-000000000003';
  v_actor uuid := '95300000-0000-0000-0000-000000000004';
  v_manual_ts uuid := '95300000-0000-0000-0000-000000000010';
  v_manual_week uuid := '95300000-0000-0000-0000-000000000011';
  v_manual_fin uuid := '95300000-0000-0000-0000-000000000012';
  v_qr_new uuid;
  v_revert_electronic uuid := '95300000-0000-0000-0000-000000000020';
  v_revert_manual uuid := '95300000-0000-0000-0000-000000000021';
  v_revert_week uuid := '95300000-0000-0000-0000-000000000022';
  v_revert_fin uuid := '95300000-0000-0000-0000-000000000023';
  v_revert_account uuid := '95300000-0000-0000-0000-000000000024';
  v_revert_workflow uuid := '95300000-0000-0000-0000-000000000025';
  v_revert_candidate_signature uuid := '95300000-0000-0000-0000-000000000026';
  v_revert_manager_signature uuid := '95300000-0000-0000-0000-000000000027';
  v_revert_hours_component uuid := '95300000-0000-0000-0000-000000000028';
  v_revert_approval_request uuid := '95300000-0000-0000-0000-000000000029';
  v_revert_expense_component uuid := '95300000-0000-0000-0000-00000000002a';
  v_mismatch_electronic uuid := '95300000-0000-0000-0000-000000000030';
  v_mismatch_manual uuid := '95300000-0000-0000-0000-000000000031';
  v_mismatch_week uuid := '95300000-0000-0000-0000-000000000032';
  v_mismatch_fin uuid := '95300000-0000-0000-0000-000000000033';
  v_result jsonb;
begin
  select to_jsonb(legacy_restore) into v_result
  from public.timesheet_qr_restore_version(
    v_manual_ts,v_manual_ts,'PENDING',v_actor
  ) legacy_restore;
  if (v_result->>'timesheet_id')::uuid<>v_manual_ts
     or v_result->>'qr_status'<>'PENDING' then
    raise exception 'feature-off QR restore compatibility wrapper did not delegate';
  end if;
  update public.settings_defaults set
    candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||'{"candidate_route_confirmation":true}'::jsonb
  where id=1;
  perform set_config('cloudtms.route_transition_confirmed','on',true);
  insert into public.clients(id,name) values(v_client,'QR Runtime Client');
  insert into public.candidates(id,email,active,key_norm,pay_method)
  values(v_candidate,'qr-runtime@example.test',true,'QR-RUNTIME-GCK','PAYE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,is_nhsp,requires_hr,
    no_timesheet_required,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,'2026-01-01','ELECTRONIC',false,false,false,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,weekly_timesheet_source,role,band
  ) values(
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,
    'ELECTRONIC','NONE','NURSE','Band 5'
  );

  -- A manual non-QR row rotated to QR creates a clean generation. The old
  -- generation becomes fully revoked, and the exact current TSFIN/week links
  -- move to the new row without copying any signature/document/QR bytes.
  insert into public.timesheets(
    timesheet_id,version,status,submission_mode,line_type,sheet_scope,is_current,
    contract_id,booking_id,week_ending_date,worked_start_iso,worked_end_iso,
    break_minutes,worked_minutes,manual_pdf_r2_key,manual_document_asset_id,
    generated_pdf_at_utc,generated_pdf_refs_sig,generated_pdf_refs_snapshot_json,
    generated_pdf_refs_captured_at_utc,qr_sent_refs_sig,qr_sent_refs_snapshot_json,
    qr_sent_refs_captured_at_utc,day_references_json,r2_nurse_key,r2_auth_key,
    img_sha256_nurse,img_sha256_auth,authorised_at_server,auth_name,auth_job_title,
    document_state,document_revision
  ) values(
    v_manual_ts,1,'RECEIVED','MANUAL','HOURS','WEEKLY',true,
    v_contract,'QR-RUNTIME-ALLOW','2026-08-08','2026-08-03 08:00:00+01',
    '2026-08-03 18:00:00+01',60,540,'manual/old.pdf',gen_random_uuid(),
    '2026-08-08 09:00:00+00','old-generated',jsonb_build_object('old',true),
    '2026-08-08 09:00:00+00','old-sent',jsonb_build_object('old',true),
    '2026-08-08 09:00:00+00',jsonb_build_object('old',true),
    'candidate/old','manager/old','candidate-hash','manager-hash',
    '2026-08-08 09:00:00+00','Manager','Position','READY',7
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(v_manual_week,v_contract,'2026-08-08',0,'SUBMITTED','MANUAL',v_manual_ts);
  insert into public.timesheets_financials(
    id,timesheet_id,candidate_id,client_id,is_current,timesheet_version,basis,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
    authorised_at_utc,authorised_by_user_id
  ) values(v_manual_fin,v_manual_ts,v_candidate,v_client,true,1,'SELF_REPORTED',
    'UNPROCESSED',9,90,180,90,null,null);

  -- Route rotation is never a hidden unauthorise implementation. Prove both
  -- timesheet-side and TSFIN-side authority independently before emulating the
  -- separately tested existing Unauthorise lifecycle.
  begin
    perform public.timesheet_route_version_rotate(
      v_manual_ts,v_manual_ts,'ALLOW_QR_AGAIN',v_actor,false
    );
    raise exception 'timesheet-authorised ALLOW_QR_AGAIN was accepted';
  exception when others then
    if sqlerrm not like 'TIMESHEET_AUTHORISED_EDIT_BLOCKED:%' then raise; end if;
  end;
  if not exists(select 1 from public.timesheets where timesheet_id=v_manual_ts and is_current and version=1)
     or (select timesheet_id from public.timesheets_financials where id=v_manual_fin)<>v_manual_ts then
    raise exception 'failed timesheet-authorised rotation mutated canonical ownership';
  end if;

  update public.timesheets set authorised_at_server=null where timesheet_id=v_manual_ts;
  update public.timesheets_financials set authorised_at_utc='2026-08-08 09:00:00+00',
    authorised_by_user_id=v_actor where id=v_manual_fin;
  begin
    perform public.timesheet_route_version_rotate(
      v_manual_ts,v_manual_ts,'ALLOW_QR_AGAIN',v_actor,false
    );
    raise exception 'TSFIN-authorised ALLOW_QR_AGAIN was accepted';
  exception when others then
    if sqlerrm not like 'TIMESHEET_AUTHORISED_EDIT_BLOCKED:%' then raise; end if;
  end;
  update public.timesheets_financials set authorised_at_utc=null,authorised_by_user_id=null
  where id=v_manual_fin;

  v_result := public.timesheet_route_version_rotate(
    v_manual_ts,v_manual_ts,'ALLOW_QR_AGAIN',v_actor,false
  );
  v_qr_new := (v_result->>'new_timesheet_id')::uuid;
  if v_qr_new is null or v_qr_new=v_manual_ts then
    raise exception 'ALLOW_QR_AGAIN did not create a new current generation: %',v_result;
  end if;
  if not exists(
    select 1 from public.timesheets t
    where t.timesheet_id=v_manual_ts and not t.is_current and t.status='REVOKED'
      and t.revoked_at is not null and t.revoked_reason='ALLOW_QR_AGAIN'
      and t.revoked_by=v_actor::text
  ) then
    raise exception 'old manual generation does not have complete revocation state';
  end if;
  if not exists(
    select 1 from public.timesheets t
    where t.timesheet_id=v_qr_new and t.is_current and t.version=2
      and t.submission_mode='MANUAL' and t.qr_status='PENDING'
      and t.qr_token is null and t.qr_r2_key is null
      and t.r2_nurse_key is null and t.r2_auth_key is null
      and t.img_sha256_nurse is null and t.img_sha256_auth is null
      and t.authorised_at_server is null and t.auth_name is null and t.auth_job_title is null
      and t.manual_pdf_r2_key is null and t.manual_document_asset_id is null
      and t.generated_pdf_at_utc is null and t.generated_pdf_refs_sig is null
      and t.generated_pdf_refs_snapshot_json is null
      and t.generated_pdf_refs_captured_at_utc is null
      and t.qr_sent_refs_sig is null and t.qr_sent_refs_snapshot_json is null
      and t.qr_sent_refs_captured_at_utc is null and t.day_references_json is null
      and t.document_state='NOT_REQUESTED' and t.document_revision=1
  ) then
    raise exception 'new QR generation inherited protected route/document/signature state';
  end if;
  if (select timesheet_id from public.contract_weeks where id=v_manual_week)<>v_qr_new
     or (select submission_mode_snapshot from public.contract_weeks where id=v_manual_week)<>'MANUAL'
     or (select timesheet_id from public.timesheets_financials where id=v_manual_fin)<>v_qr_new
     or (select timesheet_version from public.timesheets_financials where id=v_manual_fin)<>2 then
    raise exception 'ALLOW_QR_AGAIN did not move exact week/TSFIN authority to the new generation';
  end if;
  v_result:=public.timesheet_route_version_preview_v1(v_qr_new,'CONVERT_QR_TO_MANUAL');
  if v_result->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_result->>'block_reason'<>'QR_PACK_PREPARATION_IN_PROGRESS' then
    raise exception 'pre-ready QR pack was represented as issued: %',v_result;
  end if;
  update public.timesheets set qr_token='route-runtime-issued',qr_generated_at=now()
  where timesheet_id=v_qr_new;
  v_result:=public.timesheet_route_version_preview_v1(v_qr_new,'CONVERT_QR_TO_MANUAL');
  if v_result->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_result->>'block_reason'<>'QR_PACK_PREPARATION_IN_PROGRESS'
     or not coalesce((v_result->>'qr_code_generated')::boolean,false)
     or coalesce((v_result->>'qr_pack_ready')::boolean,true) then
    raise exception 'QR token generation was incorrectly represented as a ready pack: %',v_result;
  end if;
  update public.timesheets set manual_pdf_r2_key='route-runtime/unsigned-pack.pdf'
  where timesheet_id=v_qr_new;
  v_result:=public.timesheet_route_version_preview_v1(v_qr_new,'CONVERT_QR_TO_MANUAL');
  if v_result->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_result->>'block_reason'<>'QR_PACK_READY_NOT_ISSUED'
     or not coalesce((v_result->>'qr_pack_ready')::boolean,false)
     or coalesce((v_result->>'qr_pack_issued_or_sent')::boolean,true)
     or coalesce((v_result->>'qr_signed_returned')::boolean,true) then
    raise exception 'unsigned ready QR pack was represented as issued or signed: %',v_result;
  end if;
  update public.timesheets set
    qr_last_sent_at_utc=now(),qr_last_sent_hash='route-runtime-issued-hash'
  where timesheet_id=v_qr_new;
  v_result:=public.timesheet_route_version_preview_v1(v_qr_new,'CONVERT_QR_TO_MANUAL');
  if v_result->>'warning_code'<>'QR_ISSUED_TO_MANUAL'
     or not coalesce((v_result->>'qr_pack_issued_or_sent')::boolean,false)
     or coalesce((v_result->>'qr_signed_returned')::boolean,true) then
    raise exception 'issued unsigned QR warning classification failed: %',v_result;
  end if;
  update public.timesheets set qr_signed_hash='route-runtime-signed',qr_signed_at_utc=now()
  where timesheet_id=v_qr_new;
  v_result:=public.timesheet_route_version_preview_v1(v_qr_new,'CONVERT_QR_TO_MANUAL');
  if v_result->>'warning_code'<>'QR_SIGNED_TO_MANUAL' then
    raise exception 'signed QR warning classification failed: %',v_result;
  end if;
  update public.timesheets set qr_signed_hash=null,qr_signed_at_utc=null
  where timesheet_id=v_qr_new;

  -- Exact-content signed electronic lineage may be promoted back to current.
  insert into public.timesheets(
    timesheet_id,version,status,submission_mode,line_type,sheet_scope,is_current,
    contract_id,booking_id,week_ending_date,worked_start_iso,worked_end_iso,
    break_minutes,worked_minutes,r2_nurse_key,r2_auth_key,img_sha256_nurse,img_sha256_auth,
    day_references_json,candidate_workflow_id,candidate_workflow_generation,
    revoked_at,revoked_reason,revoked_by
  ) values
    (v_revert_electronic,1,'REVOKED','ELECTRONIC','HOURS','WEEKLY',false,
      v_contract,'QR-RUNTIME-REVERT','2026-08-15','2026-08-10 08:00:00+01',
      '2026-08-10 18:00:00+01',60,540,'candidate/revert','manager/revert',
      repeat('11',32),repeat('22',32),jsonb_build_object('2026-08-10','REF-1'),
      null,null,
      '2026-08-15 09:00:00+00','SWITCHED_TO_MANUAL',v_actor::text),
    (v_revert_manual,2,'RECEIVED','MANUAL','HOURS','WEEKLY',true,
      v_contract,'QR-RUNTIME-REVERT','2026-08-15','2026-08-10 08:00:00+01',
      '2026-08-10 18:00:00+01',60,540,null,null,null,null,
      jsonb_build_object('2026-08-10','REF-1'),null,null,null,null,null);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(v_revert_week,v_contract,'2026-08-15',0,'SUBMITTED','MANUAL',v_revert_manual);
  insert into public.timesheets_financials(
    id,timesheet_id,candidate_id,client_id,is_current,timesheet_version,basis,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat
  ) values(v_revert_fin,v_revert_manual,v_candidate,v_client,true,2,'SELF_REPORTED',
    'UNPROCESSED',9,90,180,90);

  insert into public.candidate_app_accounts(id,environment,email_normalized)
  values(v_revert_account,'TEST','qr-revert@example.test');
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,manager_name,manager_position,manager_approved_at_utc,finalised_at_utc,
    review_manifest_sha256
  ) values(
    v_revert_workflow,'TEST',v_revert_account,v_candidate,'CONTRACT_COMBINED','WEEKLY','EMAIL',
    'FINALISED',1,v_contract,v_revert_week,v_revert_electronic,v_revert_electronic,
    '2026-08-15','qr-runtime-revert','Manager','Position','2026-08-15 08:30:00+00',
    '2026-08-15 09:00:00+00',decode(repeat('aa',32),'hex')
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    review_manifest_sha256,required_component_ids,required_component_manifest_json,
    manager_review_timesheet_component_id,manager_review_timesheet_sha256,approved_at_utc
  ) values(
    v_revert_approval_request,v_revert_workflow,1,1,'PHONE','SUPERSEDED',
    decode(repeat('aa',32),'hex'),array[v_revert_hours_component,v_revert_expense_component],
    jsonb_build_array(
      jsonb_build_object('component_id',v_revert_hours_component),
      jsonb_build_object('component_id',v_revert_expense_component)
    ),null,decode(repeat('33',32),'hex'),'2026-08-15 08:30:00+00'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,component_kind,document_role,state,
    storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,required,
    review_render_state,final_signed_render_state
  ) values
    (v_revert_candidate_signature,v_revert_workflow,1,1,'CANDIDATE_SIGNATURE',
      'CANDIDATE_SIGNATURE','IMMUTABLE','candidate/revert','image/png',100,
      decode(repeat('11',32),'hex'),now(),false,'NOT_REQUIRED','NOT_REQUIRED');
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,component_kind,document_role,state,
    approval_request_id,storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
    required,review_render_state,final_signed_render_state
  ) values(
    v_revert_manager_signature,v_revert_workflow,1,2,'MANAGER_SIGNATURE',
    'MANAGER_SIGNATURE','IMMUTABLE',v_revert_approval_request,'manager/revert','image/png',100,
    decode(repeat('22',32),'hex'),now(),false,'NOT_REQUIRED','NOT_REQUIRED'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
    immutable_at_utc,required,review_ordinal,review_storage_key,review_content_sha256,review_media_type,
    review_byte_size,review_page_count,review_render_input_sha256,
    review_renderer_contract_version,review_renderer_receipt_json,review_generated_at_utc,
    review_render_state,final_signed_storage_key,final_signed_content_sha256,
    final_signed_media_type,final_signed_byte_size,final_signed_page_count,
    final_signed_render_input_sha256,final_signed_renderer_contract_version,
    final_signed_renderer_receipt_json,final_signed_generated_at_utc,final_signed_render_state
  ) values(
    v_revert_hours_component,v_revert_workflow,1,3,v_revert_electronic,'HOURS_TIMESHEET',
    'ELECTRONIC_TIMESHEET_MANAGER_REVIEW','IMMUTABLE',now(),true,1,
    'workflow/revert/review/timesheet.pdf',decode(repeat('33',32),'hex'),'application/pdf',1000,1,
    decode(repeat('44',32),'hex'),'TIMESHEET_OFFICIAL_PDF_V1',jsonb_build_object('page_count',1),now(),'READY',
    'workflow/revert/final/timesheet.pdf',decode(repeat('55',32),'hex'),'application/pdf',1100,1,
    decode(repeat('44',32),'hex'),'TIMESHEET_OFFICIAL_PDF_V1',jsonb_build_object('page_count',1),now(),'READY'
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
    immutable_at_utc,required,review_ordinal,review_storage_key,review_content_sha256,review_media_type,
    review_byte_size,review_page_count,review_render_input_sha256,
    review_renderer_contract_version,review_renderer_receipt_json,review_generated_at_utc,
    review_render_state,final_signed_storage_key,final_signed_content_sha256,
    final_signed_media_type,final_signed_byte_size,final_signed_page_count,
    final_signed_render_input_sha256,final_signed_renderer_contract_version,
    final_signed_renderer_receipt_json,final_signed_generated_at_utc,final_signed_render_state
  ) values(
    v_revert_expense_component,v_revert_workflow,1,4,v_revert_electronic,'EXPENSE_SUMMARY',
    'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',now(),true,2,
    'workflow/revert/review/expense-summary.pdf',decode(repeat('66',32),'hex'),'application/pdf',900,1,
    decode(repeat('77',32),'hex'),'EXPENSE_REVIEW_PDF_V1',jsonb_build_object('page_count',1),now(),'READY',
    'workflow/revert/final/expense-summary.pdf',decode(repeat('88',32),'hex'),'application/pdf',950,1,
    decode(repeat('77',32),'hex'),'EXPENSE_REVIEW_PDF_V1',jsonb_build_object('page_count',1),now(),'READY'
  );
  update public.candidate_approval_requests set
    manager_review_timesheet_component_id=v_revert_hours_component
  where id=v_revert_approval_request;
  update public.candidate_submission_workflows set
    candidate_signature_component_id=v_revert_candidate_signature,
    candidate_signature_sha256=decode(repeat('11',32),'hex'),candidate_signed_at_utc=now(),
    manager_signature_component_id=v_revert_manager_signature,
    manager_signature_sha256=decode(repeat('22',32),'hex'),
    canonical_financial_sha256=private._candidate_financial_content_sha256_v1(v_revert_fin)
  where id=v_revert_workflow;
  update public.timesheets set
    candidate_workflow_id=v_revert_workflow,candidate_workflow_generation=1,
    candidate_manager_approved_at_utc='2026-08-15 08:30:00+00'
  where timesheet_id=v_revert_electronic;
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,document_role,candidate_component_id,processing_state
  ) values
  (
    v_revert_electronic,'TIMESHEET','Official electronically signed timesheet',
    'workflow/revert/final/timesheet.pdf','SIGNED_TIMESHEET',v_revert_hours_component,'READY'
  ),(
    v_revert_electronic,'OTHER','Final signed expense approval summary',
    'workflow/revert/final/expense-summary.pdf','EXPENSE_MILEAGE_APPROVAL_SUMMARY',
    v_revert_expense_component,'READY'
  );
  v_result:=private._timesheet_exact_electronic_restore_proof_v1(v_revert_manual);
  if coalesce((v_result->>'proven')::boolean,false)<>true then
    raise exception 'complete combined exact-restore proof was rejected: %',v_result;
  end if;
  update public.timesheet_evidence
  set processing_state='SUPERSEDED'
  where candidate_component_id=v_revert_expense_component;
  v_result:=private._timesheet_exact_electronic_restore_proof_v1(v_revert_manual);
  if coalesce((v_result->>'proven')::boolean,false)<>false
     or v_result->>'reason'<>'REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE' then
    raise exception 'missing expense-page lineage did not fail exact restore proof: %',v_result;
  end if;
  update public.timesheet_evidence
  set processing_state='READY'
  where candidate_component_id=v_revert_expense_component;
  update public.candidate_submission_components
  set state='SUPERSEDED'
  where id=v_revert_expense_component;
  v_result:=private._timesheet_exact_electronic_restore_proof_v1(v_revert_manual);
  if coalesce((v_result->>'proven')::boolean,false)<>false
     or v_result->>'reason'<>'REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE' then
    raise exception 'superseded required component disappeared from approved-manifest proof: %',v_result;
  end if;
  begin
    perform private._timesheet_route_version_core_v1(
      v_revert_manual,v_revert_manual,'REVERT_TO_ELECTRONIC',v_actor,true
    );
    raise exception 'mutation-side exact restore accepted a superseded approved component';
  exception when others then
    if sqlerrm not like 'TIMESHEET_REVERT_REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE:%' then raise; end if;
  end;
  update public.candidate_submission_components
  set state='IMMUTABLE'
  where id=v_revert_expense_component;
  v_result := public.timesheet_route_version_rotate(
    v_revert_manual,v_revert_manual,'REVERT_TO_ELECTRONIC',v_actor,true
  );
  if (v_result->>'current_timesheet_id')::uuid<>v_revert_electronic
     or not exists(
       select 1 from public.timesheets t
       where t.timesheet_id=v_revert_electronic and t.is_current
         and t.status='RECEIVED' and t.revoked_at is null
         and t.revoked_reason is null and t.revoked_by is null
         and t.submission_mode='ELECTRONIC'
         and t.r2_nurse_key='candidate/revert' and t.r2_auth_key='manager/revert'
     )
     or not exists(
       select 1 from public.timesheets t
       where t.timesheet_id=v_revert_manual and not t.is_current
         and t.status='REVOKED' and t.revoked_at is not null
         and t.revoked_reason='REVERTED_TO_EXACT_ELECTRONIC_VERSION'
     ) then
    raise exception 'exact-content signed electronic revert did not restore safe lineage: %',v_result;
  end if;
  if (select timesheet_id from public.contract_weeks where id=v_revert_week)<>v_revert_electronic
     or (select submission_mode_snapshot from public.contract_weeks where id=v_revert_week)<>'ELECTRONIC'
     or (select timesheet_id from public.timesheets_financials where id=v_revert_fin)<>v_revert_electronic
     or (select timesheet_version from public.timesheets_financials where id=v_revert_fin)<>1 then
    raise exception 'exact electronic revert did not move the week/TSFIN authority';
  end if;

  -- Changed factual content must fail before either generation or TSFIN moves.
  insert into public.timesheets(
    timesheet_id,version,status,submission_mode,line_type,sheet_scope,is_current,
    contract_id,booking_id,week_ending_date,worked_start_iso,worked_end_iso,
    break_minutes,worked_minutes,r2_nurse_key,r2_auth_key,revoked_at,revoked_reason,revoked_by
  ) values
    (v_mismatch_electronic,1,'REVOKED','ELECTRONIC','HOURS','WEEKLY',false,
      v_contract,'QR-RUNTIME-MISMATCH','2026-08-22','2026-08-17 08:00:00+01',
      '2026-08-17 18:00:00+01',60,540,'candidate/mismatch','manager/mismatch',
      '2026-08-22 09:00:00+00','SWITCHED_TO_MANUAL',v_actor::text),
    (v_mismatch_manual,2,'RECEIVED','MANUAL','HOURS','WEEKLY',true,
      v_contract,'QR-RUNTIME-MISMATCH','2026-08-22','2026-08-17 08:00:00+01',
      '2026-08-17 19:00:00+01',60,600,null,null,null,null,null);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(v_mismatch_week,v_contract,'2026-08-22',0,'SUBMITTED','MANUAL',v_mismatch_manual);
  insert into public.timesheets_financials(
    id,timesheet_id,candidate_id,client_id,is_current,timesheet_version,basis,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat
  ) values(v_mismatch_fin,v_mismatch_manual,v_candidate,v_client,true,2,'SELF_REPORTED',
    'UNPROCESSED',10,100,200,100);
  begin
    perform public.timesheet_route_version_rotate(
      v_mismatch_manual,v_mismatch_manual,'REVERT_TO_ELECTRONIC',v_actor,true
    );
    raise exception 'changed-content electronic revert was accepted';
  exception when others then
    if sqlerrm not like 'TIMESHEET_REVERT_CONTENT_MISMATCH:%' then raise; end if;
  end;
  if not exists(
       select 1 from public.timesheets where timesheet_id=v_mismatch_manual and is_current
     )
     or (select timesheet_id from public.contract_weeks where id=v_mismatch_week)<>v_mismatch_manual
     or (select timesheet_id from public.timesheets_financials where id=v_mismatch_fin)<>v_mismatch_manual then
    raise exception 'failed changed-content revert mutated canonical ownership';
  end if;
end;
$candidate_qr_version_runtime$;

rollback;
