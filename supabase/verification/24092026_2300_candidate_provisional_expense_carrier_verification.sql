\set ON_ERROR_STOP on
begin;
do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_root uuid:=gen_random_uuid();
  v_carrier uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_hours_workflow uuid:=gen_random_uuid();
  v_result jsonb;
  v_contract_before jsonb;
  v_actor uuid;
  v_office_actor uuid;
  v_anchor_timesheet uuid:=gen_random_uuid();
  v_evidence uuid:=gen_random_uuid();
  v_approval uuid;
  v_signature uuid:=gen_random_uuid();
  v_policy jsonb;
  v_submission jsonb;
  v_expense_timesheet uuid;
begin
  insert into public.settings_defaults(id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256)
  values(1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')) on conflict(id) do nothing;
  update public.settings_defaults set candidate_app_feature_flags_json=
    coalesce(candidate_app_feature_flags_json,'{}'::jsonb)||
    '{"candidate_app_reads":true,"candidate_app_writes":true,"candidate_expense_atomic_placement":true,"candidate_record_role_capabilities":true}'::jsonb where id=1;
  if (select candidate_app_system_actor_user_id from public.settings_defaults where id=1) is null then
    insert into public.tms_users(email,role,is_active,password_hash,display_name,payment_authoriser,payment_golden_key)
    values('carrier-regression-'||v_workflow::text||'@example.test','user',false,
      '!no-login!','Carrier regression system',false,false) returning id into v_actor;
    update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  end if;
  insert into public.clients(id,name) values(v_client,'Expense carrier regression');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode,week_ending_weekday)
  values(gen_random_uuid(),v_client,'1900-01-01','ELECTRONIC',extract(dow from current_date)::integer);
  insert into public.candidates(id,email,active)
  values(v_candidate,'carrier-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode)
  values(v_contract,v_candidate,v_client,current_date-7,current_date+7,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC');
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,day_entries_json,planned_schedule_json,totals_json,is_adjustment)
  values(v_root,v_contract,current_date,0,'OPEN','ELECTRONIC','[]','[]','{}',false),
    (v_carrier,v_contract,current_date,1,'OPEN','MANUAL','[]','[]','{}',true);
  if private._candidate_provisional_expense_carrier_v1(v_carrier) then
    raise exception 'Unproven additional row classified as expense';
  end if;
  perform private._candidate_audit_v1('contract_week',v_carrier::text,'CANDIDATE_EXPENSE_CARRIER_CREATED',null,
    jsonb_build_object('contract_id',v_contract,'week_ending_date',current_date,'additional_seq',1));
  update public.client_settings set is_nhsp=true,
    send_manual_invoices_to_different_email=true,
    candidate_expense_invoice_email='expenses@example.test',
    manual_invoices_alt_email_address='expenses@example.test'
  where client_id=v_client;
  v_result:=private._candidate_record_capabilities_v1(null,v_carrier,'{}');
  if v_result->>'record_role' is distinct from 'FLEXIBLE'
     or v_result->>'import_authoritative' is distinct from 'false'
     or v_result->>'can_edit_hours' is distinct from 'false'
     or v_result->>'can_edit_expenses' is distinct from 'true' then
    raise exception 'Expense reservation capabilities incorrect: %',v_result;
  end if;
  v_result:=private._candidate_record_capabilities_v1(null,v_root,'{}');
  if v_result->>'record_role' is distinct from 'IMPORT_HOURS'
     or v_result->>'import_authoritative' is distinct from 'true' then
    raise exception 'Source root lost import authority: %',v_result;
  end if;
  if private._candidate_provisional_expense_carrier_v1(v_carrier) is distinct from true
     or private._candidate_provisional_expense_carrier_v1(v_root) is distinct from false then
    raise exception 'Provisional carrier/root classification failed';
  end if;
  update public.contract_weeks set totals_json='{"hours":{"day":1,"night":-1}}' where id=v_carrier;
  if private._candidate_provisional_expense_carrier_v1(v_carrier) then
    raise exception 'Offsetting hours bypassed carrier classification';
  end if;
  update public.contract_weeks set totals_json='{}' where id=v_carrier;
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','carrier-'||v_candidate::text||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(id,account_id,environment,selected_candidate_id,status,
    refresh_token_hash,expires_at_utc,absolute_expires_at_utc)
  values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('d7',32),'hex'),
    now()+interval '1 day',now()+interval '2 days');
  insert into public.candidate_submission_workflows(id,environment,account_id,candidate_id,
    workflow_kind,scope,route,state,generation,contract_id,contract_week_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key)
  values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','EMAIL',
    'WORKER_DRAFT',1,v_contract,v_carrier,current_date,'{}','{}','carrier-regression-'||v_workflow::text);
  v_result:=public.expense_placement_resolve_v1(v_candidate,'TEST',null,v_root);
  if v_result->>'placement' is distinct from 'BLOCKED'
     or v_result->>'reason_code' is distinct from 'CANDIDATE_RECORD_VIEW_ONLY' then
    raise exception 'A week without worked evidence was admitted: %',v_result;
  end if;
  insert into public.candidate_submission_workflows(id,environment,account_id,candidate_id,
    workflow_kind,scope,route,state,generation,contract_id,contract_week_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key)
  values(v_hours_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'WORKER_SUBMITTED',1,v_contract,v_root,current_date,'{}',
    '{"hours_submission":{"timesheet_patch_json":{"actual_schedule_json":[{"start_time":"09:00","end_time":"17:00"}]}}}',
    'carrier-hours-regression-'||v_hours_workflow::text);
  -- Match the actual pre-final source state: a manual-storage hours root holds
  -- candidate clock evidence but has no final source financial snapshot yet.
  insert into public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,submission_mode,actual_schedule_json)
  values(v_anchor_timesheet,'SOURCE_EXPENSE_'||v_anchor_timesheet::text,'SOURCE_EXPENSE_CANDIDATE',
    'REGRESSION HOSPITAL','REGRESSION WARD','NURSE',v_contract,current_date,'WEEKLY','HOURS','MANUAL',
    '[{"start_time":"09:00","end_time":"17:00"}]');
  update public.contract_weeks set timesheet_id=v_anchor_timesheet where id=v_root;
  update public.candidate_submission_workflows set anchor_timesheet_id=v_anchor_timesheet where id=v_workflow;
  v_result:=public.expense_placement_resolve_v1(v_candidate,'TEST',null,v_root);
  if v_result->>'placement' is distinct from 'REUSE_CARRIER'
     or v_result->>'target_contract_week_id' is distinct from v_carrier::text then
    raise exception 'Submitted source-week evidence did not reuse its expense reservation: %',v_result;
  end if;
  v_result:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,100,now());
  if not exists(select 1 from jsonb_array_elements(v_result->'items') card
      where card->>'contract_week_id'=v_root::text)
     or exists(select 1 from jsonb_array_elements(v_result->'items') card
      where card->>'contract_week_id'=v_carrier::text) then
    raise exception 'MyTMS did not retain root and hide its expense reservation: %',v_result;
  end if;
  v_result:=private._candidate_empty_provisional_expense_cleanup_v1('TEST',v_carrier);
  if v_result->>'deleted' is distinct from 'false' then
    raise exception 'Active draft was deleted';
  end if;
  insert into public.tms_users(email,role,is_active,password_hash,display_name,payment_authoriser,payment_golden_key)
  values('carrier-office-'||v_workflow::text||'@example.test','user',true,
    '!no-login!','Carrier regression Office',false,false) returning id into v_office_actor;
  v_result:=public.candidate_office_expense_reservations_v1('TEST',v_office_actor,array[v_root,v_carrier]);
  if jsonb_array_length(v_result->'rows') is distinct from 1
     or v_result#>>'{rows,0,contract_week_id}' is distinct from v_carrier::text
     or v_result#>>'{rows,0,state}' is distinct from 'WORKER_DRAFT' then
    raise exception 'Office reservation projection lost exact claim/root scope: %',v_result;
  end if;
  begin
    perform public.candidate_office_expense_reservations_v1('TEST',gen_random_uuid(),array[v_carrier]);
    raise exception 'Unknown Office actor accepted';
  exception when sqlstate '28000' then null;
  end;
  if has_function_privilege('anon','public.candidate_office_expense_reservations_v1(text,uuid,uuid[])','EXECUTE')
     or has_function_privilege('authenticated','public.candidate_office_expense_reservations_v1(text,uuid,uuid[])','EXECUTE')
     or not has_function_privilege('service_role','public.candidate_office_expense_reservations_v1(text,uuid,uuid[])','EXECUTE') then
    raise exception 'Office reservation RPC role boundary incorrect';
  end if;
  -- Exercise the real finaliser's source-work gate, while deliberately missing
  -- signed documents: eligibility must not bypass the next approval/evidence gate.
  begin
    update public.candidate_submission_workflows set anchor_timesheet_id=v_anchor_timesheet,
      state='READY_TO_FINALISE',review_manifest_sha256=decode(repeat('a1',32),'hex') where id=v_workflow;
    insert into public.candidate_submission_components(id,workflow_id,workflow_generation,component_no,
      component_kind,expense_category,document_role,state,storage_key,media_type,byte_size,
      source_content_sha256,immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state)
    values(v_evidence,v_workflow,1,1,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE','IMMUTABLE',
      'regression/expense-receipt.png','image/png',100,decode(repeat('c1',32),'hex'),now(),true,1,'PENDING','PENDING');
    insert into public.candidate_approval_requests(workflow_id,workflow_generation,request_generation,
      method,state,token_hash,expires_at_utc,review_manifest_sha256,required_component_ids,
      required_component_manifest_json,approved_at_utc,manager_email_normalized)
    values(v_workflow,1,1,'EMAIL','APPROVED',decode(repeat('b1',32),'hex'),now()+interval '1 hour',
      decode(repeat('a1',32),'hex'),array[v_evidence],jsonb_build_array(jsonb_build_object('component_id',v_evidence)),now(),'manager@example.test') returning id into v_approval;
    begin
      perform public.candidate_submission_finalize_single_flight_v1(
        v_session,'TEST',v_workflow,1,null,'carrier-finalise-proof',now(),null);
      raise exception 'Finaliser accepted missing signed evidence';
    exception when sqlstate '55000' then
      if sqlerrm<>'FINAL_SIGNED_DOCUMENT_NOT_READY' then raise; end if;
    end;
    update public.candidate_submission_workflows set state='CANCELLED' where id=v_hours_workflow;
    begin
      perform public.candidate_submission_finalize_single_flight_v1(
        v_session,'TEST',v_workflow,1,null,'carrier-finalise-no-work-proof',now(),null);
      raise exception 'Finaliser accepted a source week without submitted work';
    exception when sqlstate '55000' then
      if sqlerrm<>'CANDIDATE_EXPENSE_FINALISATION_ADMISSION_BLOCKED' then raise; end if;
    end;
    if exists(select 1 from public.timesheets_financials where timesheet_id=v_anchor_timesheet)
       or exists(select 1 from public.contract_weeks where id=v_carrier and timesheet_id is not null) then
      raise exception 'Failed finalisation wrote source finance or materialised an expense';
    end if;
    -- Approved-document fixture exercises the canonical financial write, not
    -- external rendering/email delivery. No function or trigger is replaced.
    update public.candidate_submission_workflows set state='WORKER_SUBMITTED' where id=v_hours_workflow;
    v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,current_date);
    v_submission:=jsonb_build_object('canonical_tsfin_snapshot',jsonb_build_object(
      'candidate_id',v_candidate,'client_id',v_client,'total_hours',0,
      'expenses_pay_ex_vat',14.56,'expenses_charge_ex_vat',14.56,
      'other_pay_ex_vat',14.56,'other_charge_ex_vat',14.56,
      'total_pay_ex_vat',14.56,'total_charge_ex_vat',14.56));
    insert into public.candidate_submission_components(id,workflow_id,workflow_generation,component_no,
      component_kind,document_role,state,storage_key,media_type,byte_size,source_content_sha256,
      immutable_at_utc,required,approval_request_id)
    values(v_signature,v_workflow,1,2,'MANAGER_SIGNATURE','MANAGER_SIGNATURE','IMMUTABLE',
      'regression/manager.png','image/png',100,decode(repeat('d1',32),'hex'),now(),false,v_approval);
    update public.candidate_submission_components set review_render_state='READY',final_signed_render_state='READY',
      review_render_input_sha256=decode(repeat('e1',32),'hex'),final_signed_render_input_sha256=decode(repeat('e1',32),'hex'),
      final_signed_content_sha256=decode(repeat('f1',32),'hex'),final_signed_storage_key='regression/expense-signed.pdf',
      review_storage_key='regression/expense-review.pdf',review_content_sha256=decode(repeat('c1',32),'hex'),
      review_media_type='application/pdf',review_byte_size=100,review_page_count=1,
      review_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',review_renderer_receipt_json='{}',review_generated_at_utc=now(),
      final_signed_media_type='application/pdf',final_signed_byte_size=100,final_signed_page_count=1,
      final_signed_renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',final_signed_renderer_receipt_json='{}',final_signed_generated_at_utc=now()
    where id=v_evidence;
    update public.candidate_submission_workflows set policy_snapshot_json=v_policy,
      policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
      immutable_submission_json=v_submission,immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_submission),
      manager_signature_component_id=v_signature,manager_signature_sha256=decode(repeat('d1',32),'hex'),
      manager_approved_at_utc=now(),manager_name='Regression manager',manager_position='Manager'
    where id=v_workflow;
    v_result:=public.candidate_submission_finalize_single_flight_v1(
      null,'TEST',v_workflow,1,null,'carrier-finalise-success-proof',now(),null);
    select timesheet_id into v_expense_timesheet from public.contract_weeks where id=v_carrier;
    if v_result->>'ok' is distinct from 'true' or v_expense_timesheet is null
       or v_expense_timesheet=v_anchor_timesheet
       or not exists(select 1 from public.timesheets_financials where timesheet_id=v_expense_timesheet
         and is_current and total_hours=0 and expenses_pay_ex_vat=14.56)
       or exists(select 1 from public.timesheets_financials where timesheet_id=v_anchor_timesheet) then
      raise exception 'Source expense did not finalise separately: %',v_result;
    end if;
    raise exception using errcode='ZC002',message='Rollback finalisation gate subcase';
  exception when sqlstate 'ZC002' then null;
  end;
  begin
    v_result:=public.candidate_workflow_cancel_atomic_v2(
      v_session,'TEST',v_workflow,1,'{"reason_note":"No expenses to claim"}','carrier-cancel-'||v_workflow::text,now());
    if exists(select 1 from public.contract_weeks where id=v_carrier)
       or not exists(select 1 from public.contract_weeks where id=v_root) then
      raise exception 'Ordinary cancellation failed to remove only the provisional carrier';
    end if;
    v_result:=public.candidate_workflow_cancel_atomic_v2(
      v_session,'TEST',v_workflow,1,'{"reason_note":"No expenses to claim"}','carrier-cancel-'||v_workflow::text,now());
    if v_result->>'idempotent_replay' is distinct from 'true' then
      raise exception 'Ordinary cancellation replay failed: %',v_result;
    end if;
    raise exception using errcode='ZC001',message='Rollback cancellation subcase';
  exception when sqlstate 'ZC001' then null;
  end;
  update public.candidate_submission_workflows set state='CANCELLED' where id=v_workflow;
  select to_jsonb(contract_row) into v_contract_before from public.contracts contract_row where id=v_contract;
  v_result:=private._candidate_empty_provisional_expense_cleanup_v1('TEST',v_carrier);
  if v_result->>'deleted' is distinct from 'true'
     or exists(select 1 from public.contract_weeks where id=v_carrier)
     or not exists(select 1 from public.contract_weeks where id=v_root)
     or not exists(select 1 from public.candidate_submission_workflows
       where id=v_workflow and state='CANCELLED' and contract_week_id is null
         and input_snapshot_json#>>'{office_permanent_delete_tombstone,previous_contract_week_id}'=v_carrier::text)
     or v_contract_before is distinct from (select to_jsonb(contract_row) from public.contracts contract_row where id=v_contract) then
    raise exception 'Empty carrier deletion/audit/root preservation failed: %',v_result;
  end if;
  v_result:=private._candidate_empty_provisional_expense_cleanup_v1('TEST',v_carrier);
  if v_result->>'deleted' is distinct from 'false' then
    raise exception 'Repeat cleanup failed';
  end if;
end;
$verification$;
rollback;
