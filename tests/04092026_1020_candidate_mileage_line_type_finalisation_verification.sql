\set ON_ERROR_STOP on

-- A reusable empty expense carrier can still be typed EXPENSES. A pure-mileage
-- final claim must atomically retype it to MILEAGE. All fixture work rolls back.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||jsonb_build_object(
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_expense_atomic_placement',true
  ),
  candidate_app_environment='TEST';

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_new_carrier_week uuid:=gen_random_uuid();
  v_import_week uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_import_timesheet uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_new_carrier_workflow uuid:=gen_random_uuid();
  v_component uuid:=gen_random_uuid();
  v_new_carrier_component uuid:=gen_random_uuid();
  v_claim jsonb;
  v_response jsonb;
  v_signature text;
  v_line_type text;
  v_new_target uuid;
begin
  insert into public.tms_users(id,email,is_active)
  values(v_actor,'mileage-line-type-actor-'||replace(v_actor::text,'-','')||'@example.test',true);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;

  insert into public.clients(id,name)
  values(v_client,'Mileage line type verification client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    candidate_expenses_require_separate_timesheet
  ) values(
    gen_random_uuid(),v_client,current_date-30,'ELECTRONIC',
    extract(dow from current_date)::integer,true
  );
  insert into public.candidates(id,email,active)
  values(v_candidate,'mileage-line-type-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode,pay_method_snapshot
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    extract(dow from current_date)::integer,'ELECTRONIC','PAYE'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,contract_id,week_ending_date,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    line_type,submission_mode,sheet_scope
  ) values(
    v_timesheet,'MILEAGE-LINE-TYPE-'||replace(v_timesheet::text,'-',''),
    v_contract,current_date,'candidate','test hospital','test ward','test role',
    'EXPENSES','MANUAL','WEEKLY'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,1,'OPEN','MANUAL',v_timesheet);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST','mileage-line-type-'||replace(v_candidate::text,'-','')||'@example.test','SETUP_REQUIRED'
  );

  v_claim:=jsonb_build_object(
    'contract_week_id',v_week,
    'canonical_tsfin_snapshot',jsonb_build_object(
      'candidate_id',v_candidate,'client_id',v_client,
      'total_hours',0,'mileage_units',25,
      'mileage_pay_ex_vat',0,'mileage_charge_ex_vat',0,
      'expenses_pay_ex_vat',0,'expenses_charge_ex_vat',0,
      'travel_pay_ex_vat',0,'travel_charge_ex_vat',0,
      'accommodation_pay_ex_vat',0,'accommodation_charge_ex_vat',0,
      'other_pay_ex_vat',0,'other_charge_ex_vat',0
    ),
    'timesheet_patch_json',jsonb_build_object('line_type','EXPENSES'),
    'contract_week_patch_json','{}'::jsonb,
    'evidence_display_name','Mileage Claim Form'
  );

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key,manager_approved_at_utc,
    immutable_submission_json,immutable_submission_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PHONE',
    'READY_TO_FINALISE',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    '{}'::jsonb,'{}'::jsonb,'mileage-line-type-workflow-'||v_workflow::text,now(),
    v_claim,private._candidate_sha256_jsonb_v1(v_claim)
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,review_ordinal,timesheet_id,component_kind,
    expense_category,document_role,required,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc,
    review_render_state,review_storage_key,review_content_sha256,review_media_type,
    review_byte_size,review_page_count,review_render_input_sha256,
    review_renderer_contract_version,review_renderer_receipt_json,review_generated_at_utc,
    final_signed_render_state,final_signed_storage_key,final_signed_content_sha256,
    final_signed_media_type,final_signed_byte_size,final_signed_page_count,
    final_signed_render_input_sha256,final_signed_renderer_contract_version,
    final_signed_renderer_receipt_json,final_signed_generated_at_utc
  ) values(
    v_component,v_workflow,1,1,1,v_timesheet,'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',
    true,'IMMUTABLE','expense/mileage-source.jpg','image/jpeg',100,
    decode(repeat('cc',32),'hex'),now(),
    'READY','expense/mileage-review.pdf',decode(repeat('ab',32),'hex'),'application/pdf',
    200,1,decode(repeat('ef',32),'hex'),'MILEAGE_REVIEW_TEST_V1','{}'::jsonb,now(),
    'READY','expense/mileage-final.pdf',decode(repeat('dd',32),'hex'),'application/pdf',
    220,1,decode(repeat('ef',32),'hex'),'MILEAGE_FINAL_TEST_V1','{}'::jsonb,now()
  );

  perform set_config('cloudtms.candidate_finalize_workflow',v_workflow::text||':1',true);
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet,v_week,false)->>'row_signature';
  v_response:=public.timesheet_expense_apply_atomic_v1(
    v_candidate,'TEST',v_timesheet,v_workflow,1,v_signature,
    v_claim,array[v_component],'mileage-line-type-apply-'||v_workflow::text,now()
  );
  select line_type::text into v_line_type
  from public.timesheets where timesheet_id=v_timesheet and is_current=true;
  if not coalesce((v_response->>'ok')::boolean,false) or v_line_type<>'MILEAGE' then
    raise exception 'pure mileage carrier line type verification failed';
  end if;

  -- Close the first fixture workflow before proving a later claim for the same
  -- Candidate, Contract and week. Production permits one active claim at a
  -- time; the later carrier is valid only after the earlier claim is terminal.
  update public.candidate_submission_workflows
  set state='FINALISED',finalised_at_utc=now(),updated_at_utc=now()
  where id=v_workflow;

  -- The second carrier deliberately inherits an NHSP/import-authoritative
  -- Client. The separate zero-hour Mileage row must still materialise, while
  -- the source imported-hours row remains protected by the exact workflow
  -- and line-type boundary in the final-state guard.
  update public.client_settings
  set is_nhsp=true,
      candidate_expense_invoice_email='expenses@example.test'
  where client_id=v_client;

  -- The real Candidate route begins with an authoritative worked Timesheet and
  -- a reserved additional Contract week that has no Timesheet yet. Exercise
  -- that first-use materialisation so an invalid Contract-week status cannot
  -- be passed into the Timesheet enum, and ensure an inherited import route
  -- cannot misclassify the explicit zero-hour Mileage carrier as mixed data.
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot
  ) values(v_new_carrier_week,v_contract,current_date,2,'OPEN','MANUAL');

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key,manager_approved_at_utc,
    immutable_submission_json,immutable_submission_sha256
  ) values(
    v_new_carrier_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','PHONE',
    'READY_TO_FINALISE',1,v_contract,v_new_carrier_week,v_timesheet,null,current_date,
    '{}'::jsonb,'{}'::jsonb,'mileage-new-carrier-workflow-'||v_new_carrier_workflow::text,now(),
    v_claim,private._candidate_sha256_jsonb_v1(v_claim)
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,review_ordinal,timesheet_id,component_kind,
    expense_category,document_role,required,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc,
    review_render_state,review_storage_key,review_content_sha256,review_media_type,
    review_byte_size,review_page_count,review_render_input_sha256,
    review_renderer_contract_version,review_renderer_receipt_json,review_generated_at_utc,
    final_signed_render_state,final_signed_storage_key,final_signed_content_sha256,
    final_signed_media_type,final_signed_byte_size,final_signed_page_count,
    final_signed_render_input_sha256,final_signed_renderer_contract_version,
    final_signed_renderer_receipt_json,final_signed_generated_at_utc
  ) values(
    v_new_carrier_component,v_new_carrier_workflow,1,1,1,v_timesheet,
    'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',true,'IMMUTABLE',
    'expense/new-mileage-source.jpg','image/jpeg',100,decode(repeat('ac',32),'hex'),now(),
    'READY','expense/new-mileage-review.pdf',decode(repeat('ad',32),'hex'),'application/pdf',
    200,1,decode(repeat('ae',32),'hex'),'MILEAGE_REVIEW_TEST_V1','{}'::jsonb,now(),
    'READY','expense/new-mileage-final.pdf',decode(repeat('af',32),'hex'),'application/pdf',
    220,1,decode(repeat('ae',32),'hex'),'MILEAGE_FINAL_TEST_V1','{}'::jsonb,now()
  );

  perform set_config('cloudtms.candidate_finalize_workflow',v_new_carrier_workflow::text||':1',true);
  if coalesce((private._candidate_import_authoritative_v1(
       v_client,v_contract,null,v_claim->'canonical_tsfin_snapshot',current_date
     )->>'is_import_authoritative')::boolean,false)=false then
    raise exception 'Mileage carrier fixture is not import-authoritative';
  end if;
  insert into public.timesheets(
    timesheet_id,booking_id,contract_id,week_ending_date,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    line_type,submission_mode,sheet_scope
  ) values(
    v_import_timesheet,'MILEAGE-IMPORT-HOURS-'||replace(v_import_timesheet::text,'-',''),
    v_contract,current_date,'candidate','test hospital','test ward','test role',
    'HOURS','MANUAL','WEEKLY'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_import_week,v_contract,current_date,3,'AUTHORISED','MANUAL',v_import_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,basis,total_hours,processing_status
  ) values(v_import_timesheet,1,v_candidate,v_client,'NHSP',8,'READY_FOR_INVOICE');
  begin
    v_response:=private._candidate_weekly_final_state_guard_v1(
      v_import_week,v_import_timesheet,null,jsonb_build_object('line_type','MILEAGE'),
      v_claim->'canonical_tsfin_snapshot'
    );
    raise exception 'Import hours accepted expense economics: %',v_response;
  exception when sqlstate '22023' then
    if sqlerrm<>'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' then
      raise;
    end if;
  end;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(null,v_new_carrier_week,false)->>'row_signature';
  v_response:=public.timesheet_expense_apply_atomic_v1(
    v_candidate,'TEST',null,v_new_carrier_workflow,1,v_signature,
    v_claim,array[v_new_carrier_component],
    'mileage-new-carrier-apply-'||v_new_carrier_workflow::text,now()
  );
  v_new_target:=nullif(v_response->>'target_timesheet_id','')::uuid;
  if not coalesce((v_response->>'ok')::boolean,false)
     or v_new_target is null or v_new_target=v_timesheet
     or v_response#>>'{capabilities,record_role}'<>'EXPENSE_ONLY'
     or not exists(
       select 1 from public.timesheets target
       where target.timesheet_id=v_new_target
         and target.line_type='MILEAGE' and target.status='RECEIVED'
         and target.is_current=true
     )
     or (select timesheet_id from public.contract_weeks where id=v_new_carrier_week)
       is distinct from v_new_target then
    raise exception 'new Mileage carrier materialisation verification failed: %',v_response;
  end if;
end;
$verification$;

rollback;
