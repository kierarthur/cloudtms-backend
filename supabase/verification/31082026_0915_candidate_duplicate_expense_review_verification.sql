\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for Candidate duplicate-expense review.
-- The official expense summary never counts as a real Other expense.  An
-- individual Office user may authorise only after explicit review; bulk and
-- automatic routes remain blocked.
begin;

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_prior_contract uuid:=gen_random_uuid();
  v_current_contract uuid:=gen_random_uuid();
  v_prior_week uuid:=gen_random_uuid();
  v_current_week uuid:=gen_random_uuid();
  v_prior_timesheet uuid:=gen_random_uuid();
  v_current_timesheet uuid:=gen_random_uuid();
  v_prior_workflow uuid:=gen_random_uuid();
  v_current_workflow uuid:=gen_random_uuid();
  v_week date:=date '2026-08-30';
  v_review jsonb;
  v_bulk jsonb;
  v_authorised jsonb;
  v_error text;
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'duplicate-expense-actor-'||v_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true);
  insert into public.clients(id,name) values(v_client,'Duplicate Expense Verification Client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'duplicate-expense-'||v_candidate::text||'@example.test',true,
    'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''));
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','duplicate-expense-'||v_candidate::text||'@example.test','ACTIVE');

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values
    (v_prior_contract,v_candidate,v_client,v_week-30,v_week+30,'PAYE',0,'ELECTRONIC'),
    (v_current_contract,v_candidate,v_client,v_week-30,v_week+30,'PAYE',0,'ELECTRONIC');

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,sheet_scope,line_type,submission_mode
  ) values
    (v_prior_timesheet,'DUPLICATE-PRIOR-'||v_prior_timesheet::text,
      'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''),'HOSPITAL','WARD','NURSE',
      v_prior_contract,v_week,'WEEKLY','HOURS','MANUAL'),
    (v_current_timesheet,'DUPLICATE-CURRENT-'||v_current_timesheet::text,
      'GCK-DUPLICATE-'||replace(v_candidate::text,'-',''),'HOSPITAL','WARD','NURSE',
      v_current_contract,v_week,'WEEKLY','HOURS','MANUAL');

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_prior_week,v_prior_contract,v_week,'OPEN','MANUAL',v_prior_timesheet),
    (v_current_week,v_current_contract,v_week,'OPEN','MANUAL',v_current_timesheet);

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status,
    mileage_units,mileage_pay_ex_vat,mileage_charge_ex_vat
  ) values
    (v_prior_timesheet,1,v_candidate,v_client,8,'READY_FOR_INVOICE',12,6,6),
    (v_current_timesheet,1,v_candidate,v_client,8,'PENDING_AUTH',4,2,2);

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,issue_codes,idempotency_key,worker_submitted_at_utc
  ) values
    (v_prior_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
      'FINALISED',1,v_prior_contract,v_prior_week,v_prior_timesheet,v_prior_timesheet,
      v_week,'[]','duplicate-expense-prior',now()-interval '1 day'),
    (v_current_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
      'MANAGER_APPROVED',1,v_current_contract,v_current_week,v_current_timesheet,v_current_timesheet,
      v_week,'[]','duplicate-expense-current',now());

  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,upload_idempotency_key,immutable_at_utc
  ) values
    (v_prior_workflow,1,1,v_prior_timesheet,'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',
      'IMMUTABLE','duplicate-expense/mileage.jpg','image/jpeg',1024,
      decode(repeat('a1',32),'hex'),'duplicate-expense-mileage',now()),
    (v_prior_workflow,1,2,v_prior_timesheet,'EXPENSE_SUMMARY',null,
      'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',null,null,null,null,
      'duplicate-expense-summary',now());

  v_review:=private._expense_duplicate_review_v1(
    v_current_workflow,array['MILEAGE','OTHER']::text[]
  );
  if coalesce((v_review->>'required')::boolean,false) is distinct from true
     or v_review->'categories' is distinct from '["MILEAGE"]'::jsonb
     or nullif(v_review->>'confirmation_digest','') is null then
    raise exception 'Duplicate review did not isolate Mileage from the official summary: %',v_review;
  end if;

  update public.candidate_submission_workflows
  set issue_codes=jsonb_build_array('DUPLICATE_EXPENSE_REVIEW','DUPLICATE_EXPENSE_MILEAGE')
  where id=v_current_workflow;

  begin
    perform public.timesheet_authorise_generic_atomic(
      v_current_timesheet,v_current_timesheet,v_actor,now(),null
    );
    raise exception 'Generic authorisation unexpectedly bypassed duplicate review';
  exception when sqlstate 'PT409' then
    get stacked diagnostics v_error=message_text;
    if v_error<>'DUPLICATE_EXPENSE_REVIEW_REQUIRED' then raise; end if;
  end;

  v_bulk:=public.timesheet_authorise_bulk_atomic(
    jsonb_build_array(jsonb_build_object(
      'timesheet_id',v_current_timesheet,
      'expected_timesheet_id',v_current_timesheet
    )),v_actor,now()
  );
  if v_bulk#>>'{results,0,error_code}' is distinct from 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
     or coalesce((v_bulk->>'success_count')::integer,-1)<>0 then
    raise exception 'Bulk authorisation did not exclude duplicate expense review: %',v_bulk;
  end if;

  v_authorised:=public.timesheet_authorise_reviewed_atomic(
    v_current_timesheet,v_current_timesheet,v_actor,now(),null,true
  );
  if coalesce((v_authorised->>'ok')::boolean,false) is distinct from true
     or (select authorised_at_server is null from public.timesheets
         where timesheet_id=v_current_timesheet)
     or (select authorised_at_utc is null from public.timesheets_financials
         where timesheet_id=v_current_timesheet and is_current=true) then
    raise exception 'Reviewed individual authorisation did not complete: %',v_authorised;
  end if;
end;
$verification$;

rollback;
