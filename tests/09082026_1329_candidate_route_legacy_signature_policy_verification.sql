-- Legacy signed-state, QR-readiness and WEEKLY-only paper-route verification.
-- Disposable PostgreSQL only. Every fixture row is rolled back.

begin;

do $candidate_route_legacy_signature_policy$
declare
  v_client uuid:='95600000-0000-0000-0000-000000000001';
  v_candidate uuid:='95600000-0000-0000-0000-000000000002';
  v_contract uuid:='95600000-0000-0000-0000-000000000003';
  v_actor uuid:='95600000-0000-0000-0000-000000000004';
  v_manager_signed uuid:='95600000-0000-0000-0000-000000000010';
  v_candidate_signed uuid:='95600000-0000-0000-0000-000000000020';
  v_unsigned uuid:='95600000-0000-0000-0000-000000000030';
  v_qr_scanned uuid:='95600000-0000-0000-0000-000000000040';
  v_qr_manual_pdf uuid:='95600000-0000-0000-0000-000000000050';
  v_qr_evidence uuid:='95600000-0000-0000-0000-000000000060';
  v_qr_issued uuid:='95600000-0000-0000-0000-000000000070';
  v_qr_preparing uuid:='95600000-0000-0000-0000-000000000080';
  v_daily uuid:='95600000-0000-0000-0000-000000000090';
  v_daily_qr uuid:='95600000-0000-0000-0000-0000000000a0';
  v_qr_generic_evidence uuid:='95600000-0000-0000-0000-0000000000b0';
  v_context jsonb;
  v_before_version integer;
begin
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set
    candidate_app_feature_flags_json=candidate_app_feature_flags_json
      ||'{"candidate_route_confirmation":true,"candidate_app_writes":true}'::jsonb,
    candidate_app_system_actor_user_id=v_actor
  where id=1;
  insert into public.clients(id,name) values(v_client,'Legacy route state client');
  insert into public.candidates(id,email,active,key_norm,pay_method)
  values(v_candidate,'legacy-route-state@example.test',true,'GCK-LEGACY-ROUTE','PAYE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,candidate_paper_submission_enabled
  ) values(gen_random_uuid(),v_client,'2026-01-01','ELECTRONIC',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,overrideclientsettings,weekly_timesheet_source,role,band
  ) values(v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,
    'ELECTRONIC',true,'NONE','NURSE','Band 5');

  -- Legacy fully signed ELECTRONIC rows have no Candidate workflow but their
  -- canonical nurse/authoriser fields must still select W03 (or W04 while
  -- authorised), never the unsigned W01 warning.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,r2_nurse_key,img_sha256_nurse,
    r2_auth_key,img_sha256_auth,candidate_manager_approved_at_utc
  ) values(v_manager_signed,v_contract,'LEGACY-MANAGER-SIGNED','2026-08-08','WEEKLY',
    'ELECTRONIC','HOURS','RECEIVED','legacy/candidate-sig',repeat('11',32),
    'legacy/manager-sig',repeat('22',32),now());
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(gen_random_uuid(),v_contract,'2026-08-08','OPEN','ELECTRONIC',v_manager_signed);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_manager_signed,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_manager_signed,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'MANAGER_APPROVED_TO_MANUAL'
     or not coalesce((v_context->>'candidate_signed')::boolean,false)
     or not coalesce((v_context->>'manager_approved')::boolean,false) then
    raise exception 'legacy manager-approved ELECTRONIC state was misclassified: %',v_context;
  end if;
  update public.timesheets set authorised_at_server=now()
  where timesheet_id=v_manager_signed;
  v_context:=public.timesheet_route_version_preview_v1(v_manager_signed,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'ROUTE_CHANGE_REQUIRES_UNAUTHORISE'
     or coalesce((v_context->>'permitted_action')::boolean,true) then
    raise exception 'authorised legacy ELECTRONIC row did not select W04: %',v_context;
  end if;
  update public.timesheets set authorised_at_server=null
  where timesheet_id=v_manager_signed;

  -- A logical ELECTRONIC WEEKLY row with only the canonical candidate
  -- signature selects W02; an unsigned equivalent selects W01.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,r2_nurse_key,img_sha256_nurse
  ) values(v_candidate_signed,v_contract,'LEGACY-CANDIDATE-SIGNED','2026-08-15','WEEKLY',
    'MANUAL','HOURS','RECEIVED','legacy/candidate-only',repeat('33',32));
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(gen_random_uuid(),v_contract,'2026-08-15','OPEN','ELECTRONIC',v_candidate_signed);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_candidate_signed,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_candidate_signed,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL' then
    raise exception 'legacy candidate-only signed state did not select W02: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status
  ) values(v_unsigned,v_contract,'LEGACY-UNSIGNED','2026-08-22','WEEKLY',
    'MANUAL','HOURS','RECEIVED');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(gen_random_uuid(),v_contract,'2026-08-22','OPEN','ELECTRONIC',v_unsigned);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,basis,processing_status,total_hours
  ) values(v_unsigned,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_unsigned,'SWITCH_TO_MANUAL');
  if v_context->>'warning_code'<>'ELECTRONIC_UNSIGNED_TO_MANUAL' then
    raise exception 'unsigned logical ELECTRONIC state did not select W01: %',v_context;
  end if;

  -- Legacy QR signed-return proof is recognised only from explicit return
  -- lineage.  The generated unsigned printable PDF and a generic document
  -- asset are pack facts, not returned-signature facts.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at,qr_scanned_at
  ) values(v_qr_scanned,v_contract,'LEGACY-QR-SCANNED','2026-08-29','WEEKLY',
    'MANUAL','HOURS','RECEIVED','USED','legacy-qr-scanned',now(),now());
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-08-29','OPEN','MANUAL',v_qr_scanned);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_scanned,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_qr_scanned,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_SIGNED_TO_MANUAL' then
    raise exception 'legacy qr_scanned_at state did not select W09: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at,
    manual_pdf_r2_key,manual_document_asset_id,qr_last_sent_at_utc,qr_last_sent_hash
  ) values(v_qr_manual_pdf,v_contract,'LEGACY-QR-MANUAL-PDF','2026-09-05','WEEKLY',
    'MANUAL','HOURS','RECEIVED','PENDING','legacy-qr-manual-pdf',now(),
    'legacy/generated-unsigned-pack.pdf',gen_random_uuid(),now(),'legacy-sent-hash');
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-09-05','OPEN','MANUAL',v_qr_manual_pdf);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_manual_pdf,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_qr_manual_pdf,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_ISSUED_TO_MANUAL'
     or not coalesce((v_context->>'qr_pack_ready')::boolean,false)
     or not coalesce((v_context->>'qr_pack_issued_or_sent')::boolean,false)
     or coalesce((v_context->>'qr_signed_returned')::boolean,true) then
    raise exception 'generated unsigned QR PDF/document asset was classified as signed: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at
  ) values(v_qr_evidence,v_contract,'LEGACY-QR-EVIDENCE','2026-09-12','WEEKLY',
    'MANUAL','HOURS','RECEIVED','PENDING','legacy-qr-evidence',now());
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-09-12','OPEN','MANUAL',v_qr_evidence);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_evidence,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,document_role,processing_state
  ) values(v_qr_evidence,'TIMESHEET','Legacy signed returned pack',
    'legacy/returned-pack.pdf','SIGNED_TIMESHEET','READY');
  v_context:=public.timesheet_route_version_preview_v1(v_qr_evidence,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_SIGNED_TO_MANUAL' then
    raise exception 'active signed TIMESHEET evidence did not select W09: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at,
    manual_pdf_r2_key,qr_last_sent_at_utc,qr_last_sent_hash
  ) values(v_qr_generic_evidence,v_contract,'LEGACY-QR-GENERIC-EVIDENCE','2026-09-13','WEEKLY',
    'MANUAL','HOURS','RECEIVED','PENDING','legacy-qr-generic-evidence',now(),
    'legacy/generic-unsigned-pack.pdf',now(),'legacy-generic-sent-hash');
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-09-13','OPEN','MANUAL',v_qr_generic_evidence);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_generic_evidence,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,document_role,processing_state
  ) values(v_qr_generic_evidence,'TIMESHEET','Generated unsigned pack evidence',
    'legacy/generic-unsigned-pack.pdf','SOURCE_EVIDENCE','READY');
  v_context:=public.timesheet_route_version_preview_v1(v_qr_generic_evidence,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_ISSUED_TO_MANUAL'
     or coalesce((v_context->>'qr_signed_returned')::boolean,true) then
    raise exception 'generic TIMESHEET evidence was classified as signed return: %',v_context;
  end if;

  -- Ready/issued and merely preparing QR states are distinct.  Preparing
  -- state fails closed rather than making the approved W08 claim too early.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at
  ) values(v_qr_issued,v_contract,'LEGACY-QR-ISSUED','2026-09-19','WEEKLY',
    'MANUAL','HOURS','RECEIVED','PENDING','legacy-qr-issued',now());
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-09-19','OPEN','MANUAL',v_qr_issued);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_issued,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  update public.timesheets set document_state='FAILED',
    last_document_error_json=jsonb_build_object('code','PDF_GENERATION_FAILED')
  where timesheet_id=v_qr_issued;
  v_context:=public.timesheet_route_version_preview_v1(v_qr_issued,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_context->>'block_reason'<>'QR_PACK_PREPARATION_IN_PROGRESS'
     or not coalesce((v_context->>'qr_code_generated')::boolean,false)
     or coalesce((v_context->>'qr_pack_ready')::boolean,true)
     or coalesce((v_context->>'qr_pack_issued')::boolean,true) then
    raise exception 'token plus failed PDF generation was represented as issued: %',v_context;
  end if;
  update public.timesheets set document_state='NOT_REQUESTED',
    last_document_error_json=null,manual_pdf_r2_key='legacy/ready-unsent-pack.pdf'
  where timesheet_id=v_qr_issued;
  v_context:=public.timesheet_route_version_preview_v1(v_qr_issued,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_context->>'block_reason'<>'QR_PACK_READY_NOT_ISSUED'
     or not coalesce((v_context->>'qr_pack_ready')::boolean,false)
     or coalesce((v_context->>'qr_pack_issued')::boolean,true) then
    raise exception 'ready unsent QR pack was represented as issued: %',v_context;
  end if;
  update public.timesheets set qr_last_sent_at_utc=now(),qr_last_sent_hash='legacy-issued-hash'
  where timesheet_id=v_qr_issued;
  v_context:=public.timesheet_route_version_preview_v1(v_qr_issued,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'QR_ISSUED_TO_MANUAL'
     or not coalesce((v_context->>'qr_pack_issued')::boolean,false)
     or coalesce((v_context->>'qr_signed_returned')::boolean,true) then
    raise exception 'actually issued unsigned QR pack did not select W08: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status
  ) values(v_qr_preparing,v_contract,'LEGACY-QR-PREPARING','2026-09-26','WEEKLY',
    'MANUAL','HOURS','RECEIVED','PENDING');
  insert into public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id)
  values(gen_random_uuid(),v_contract,'2026-09-26','OPEN','MANUAL',v_qr_preparing);
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_qr_preparing,v_candidate,v_client,'CONTRACT_WEEKLY','UNPROCESSED',8);
  v_context:=public.timesheet_route_version_preview_v1(v_qr_preparing,'CONVERT_QR_TO_MANUAL');
  if v_context->>'warning_code'<>'ROUTE_CHANGE_NOT_PERMITTED'
     or v_context->>'block_reason'<>'QR_PACK_PREPARATION_IN_PROGRESS'
     or coalesce((v_context->>'permitted_action')::boolean,true)
     or coalesce((v_context->>'qr_pack_issued')::boolean,true)
     or not coalesce((v_context->>'qr_route_active')::boolean,false) then
    raise exception 'QR preparation state was represented as an issued pack: %',v_context;
  end if;

  -- Candidate paper/QR is WEEKLY-only.  Preview and the mutation core both
  -- reject feature-on DAILY QR creation/reissue without touching the row.
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,scheduled_start_iso,scheduled_end_iso
  ) values(v_daily,v_contract,'DAILY-PAPER-BLOCK','2026-08-09','DAILY',
    'MANUAL','HOURS','RECEIVED','2026-08-09 08:00:00+01','2026-08-09 18:00:00+01');
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_daily,v_candidate,v_client,'SELF_REPORTED','UNPROCESSED',9);
  v_context:=public.timesheet_route_version_preview_v1(v_daily,'ALLOW_QR_AGAIN');
  if v_context->>'warning_code'<>'DAILY_PAPER_ROUTE_NOT_ALLOWED'
     or coalesce((v_context->>'permitted_action')::boolean,true)
     or coalesce((private._candidate_route_family_v1(v_daily,null)
       ->>'candidate_paper_submission_allowed')::boolean,true) then
    raise exception 'DAILY fresh paper route was not blocked: %',v_context;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,sheet_scope,
    submission_mode,line_type,status,qr_status,qr_token,qr_generated_at,
    scheduled_start_iso,scheduled_end_iso
  ) values(v_daily_qr,v_contract,'DAILY-QR-REISSUE-BLOCK','2026-08-10','DAILY',
    'MANUAL','HOURS','RECEIVED','PENDING','legacy-daily-qr',now(),
    '2026-08-10 08:00:00+01','2026-08-10 18:00:00+01');
  insert into public.timesheets_financials(timesheet_id,candidate_id,client_id,basis,processing_status,total_hours)
  values(v_daily_qr,v_candidate,v_client,'SELF_REPORTED','UNPROCESSED',9);
  v_context:=public.timesheet_route_version_preview_v1(v_daily_qr,'REISSUE_QR');
  if v_context->>'warning_code'<>'DAILY_PAPER_ROUTE_NOT_ALLOWED'
     or coalesce((v_context->>'permitted_action')::boolean,true) then
    raise exception 'DAILY QR reissue was not blocked by preview: %',v_context;
  end if;
  select version into v_before_version from public.timesheets where timesheet_id=v_daily_qr;
  begin
    perform private._timesheet_route_version_core_v1(
      v_daily_qr,v_daily_qr,'REISSUE_QR',v_actor,false
    );
    raise exception 'DAILY QR reissue unexpectedly reached mutation';
  exception when sqlstate '55000' then
    if position('DAILY_PAPER_ROUTE_NOT_ALLOWED' in sqlerrm)=0 then raise; end if;
  end;
  if not exists(select 1 from public.timesheets
      where timesheet_id=v_daily_qr and is_current and version=v_before_version) then
    raise exception 'blocked DAILY QR reissue changed the current generation';
  end if;
end;
$candidate_route_legacy_signature_policy$;

rollback;
