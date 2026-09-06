\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for a later standalone PAPER expense.
-- The worked Timesheet is already manager-approved and signed. It is only the
-- Candidate ownership/display anchor: PAPER preparation must not change it or
-- create the separate expense Timesheet before the returned pack is accepted.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_expense_atomic_placement',true,
    'candidate_manager_approval',true,
    'candidate_paper_qr',true
  )
where id=1;

do $targetless_expense_paper$
declare
  v_now timestamptz:='2026-09-06 06:00:00+00';
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_anchor_week uuid:=gen_random_uuid();
  v_anchor_timesheet uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_carrier_week uuid;
  v_workflow uuid:=gen_random_uuid();
  v_component_one uuid:=gen_random_uuid();
  v_component_two uuid:=gen_random_uuid();
  v_email text:='targetless-paper-'||gen_random_uuid()::text||'@example.test';
  v_submission jsonb;
  v_placement jsonb;
  v_create jsonb;
  v_prepare jsonb;
  v_replay jsonb;
  v_promote jsonb;
  v_old_manifest_sha256 text;
  v_new_manifest_sha256 text;
  v_anchor_before jsonb;
  v_anchor_after jsonb;
  v_financial_before jsonb;
  v_financial_after jsonb;
  v_anchor_week_before jsonb;
  v_anchor_week_after jsonb;
  v_mail_id uuid;
  v_mail_count integer;
begin
  insert into public.clients(id,name)
  values(v_client,'Targetless PAPER expense verification client');

  insert into public.candidates(id,email,display_name,active,key_norm,opt_in_email)
  values(
    v_candidate,v_email,'Targetless PAPER Candidate',true,
    'TARGETLESS-PAPER-'||replace(v_candidate::text,'-',''),true
  );

  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet,
    candidate_paper_submission_enabled,candidate_expense_invoice_email
  ) values(
    gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true,true,
    'expenses@example.test'
  );

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    pay_method_snapshot,week_ending_weekday_snapshot,default_submission_mode,
    candidate_paper_submission_enabled_override
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    'PAYE',extract(dow from current_date)::integer,'ELECTRONIC',true
  );

  -- Deliberately make the worked anchor an already-used, signed, approved row.
  -- The pre-fix PAPER adapter rejected this exact authority state.
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,qr_status,qr_token,qr_generated_at,qr_scanned_at,
    qr_signed_hash,qr_signed_at_utc,candidate_manager_approved_at_utc,
    auth_name,auth_job_title,r2_nurse_key,r2_auth_key
  ) values(
    v_anchor_timesheet,
    'TARGETLESS_PAPER_ANCHOR_'||replace(v_anchor_timesheet::text,'-',''),
    'TARGETLESS-PAPER-'||replace(v_candidate::text,'-',''),
    'TARGETLESS PAPER HOSPITAL','TARGETLESS PAPER WARD','NURSE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'USED','used-anchor-token',v_now-interval '2 days',v_now-interval '1 day',
    repeat('a',64),v_now-interval '1 day',v_now-interval '1 day',
    'Approved Manager','Ward Manager',
    'targetless-paper/anchor/candidate','targetless-paper/anchor/manager'
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_anchor_week,v_contract,current_date,0,'SUBMITTED','ELECTRONIC',
    v_anchor_timesheet
  );

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    processing_status
  ) values(
    v_anchor_timesheet,1,v_candidate,v_client,8,'PENDING_AUTH'
  );

  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(v_account,'TEST',lower(v_email),'ACTIVE');

  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(v_session::text,'sha256'),
    v_now+interval '1 day',v_now+interval '7 days'
  );

  v_placement:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_anchor_timesheet,null,
    'targetless-paper:carrier',v_now
  );
  if v_placement->>'placement'<>'CREATE_CARRIER'
     or nullif(v_placement->>'target_timesheet_id','') is not null then
    raise exception 'TARGETLESS_PAPER_CARRIER_NOT_RESERVED: %',v_placement;
  end if;
  v_carrier_week:=(v_placement->>'target_contract_week_id')::uuid;

  v_create:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_carrier_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),
    'targetless-paper:create',v_now
  );
  if not coalesce((v_create->>'ok')::boolean,false) then
    raise exception 'TARGETLESS_PAPER_WORKFLOW_NOT_CREATED: %',v_create;
  end if;

  v_submission:=jsonb_build_object(
    'contract_week_id',v_carrier_week,
    'canonical_tsfin_snapshot',jsonb_build_object(
      'candidate_id',v_candidate,'client_id',v_client,
      'total_hours',0,'mileage_units',25,
      'mileage_pay_ex_vat',11.25,'mileage_charge_ex_vat',11.25,
      'expenses_pay_ex_vat',0,'expenses_charge_ex_vat',0,
      'travel_pay_ex_vat',0,'travel_charge_ex_vat',0,
      'accommodation_pay_ex_vat',0,'accommodation_charge_ex_vat',0,
      'other_pay_ex_vat',0,'other_charge_ex_vat',0
    ),
    'timesheet_patch_json',jsonb_build_object('line_type','MILEAGE'),
    'contract_week_patch_json','{}'::jsonb,
    'official_presentation',jsonb_build_object(
      'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
      'branding',jsonb_build_object(
        'branding_contract_sha256',repeat('b',64)
      )
    )
  );

  update public.candidate_submission_workflows
  set state='READY_FOR_MANAGER_APPROVAL',
      immutable_submission_json=v_submission,
      immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_submission),
      review_manifest_sha256=extensions.digest('targetless-paper-review','sha256'),
      renderer_contract_version='CANDIDATE_REVIEW_DOCUMENTS_V1',
      updated_at_utc=v_now
  where id=v_workflow;

  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,review_ordinal,timesheet_id,
    component_kind,expense_category,document_role,required,state,storage_key,
    media_type,byte_size,source_content_sha256,immutable_at_utc,
    review_render_state,final_signed_render_state
  ) values
  (
    v_component_one,v_workflow,1,1,1,v_anchor_timesheet,
    'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',true,'IMMUTABLE',
    'targetless-paper/mileage-page-1.jpg','image/jpeg',100,
    decode(repeat('c1',32),'hex'),v_now,'PENDING','PENDING'
  ),
  (
    v_component_two,v_workflow,1,2,2,v_anchor_timesheet,
    'MILEAGE_FORM','MILEAGE','MILEAGE_CLAIM_FORM',true,'IMMUTABLE',
    'targetless-paper/mileage-page-2.jpg','image/jpeg',100,
    decode(repeat('c2',32),'hex'),v_now,'PENDING','PENDING'
  );

  select to_jsonb(anchor_row.*) into v_anchor_before
  from public.timesheets anchor_row
  where anchor_row.timesheet_id=v_anchor_timesheet and anchor_row.is_current;
  select to_jsonb(fin_row.*) into v_financial_before
  from public.timesheets_financials fin_row
  where fin_row.timesheet_id=v_anchor_timesheet and fin_row.is_current;
  select to_jsonb(week_row.*) into v_anchor_week_before
  from public.contract_weeks week_row where week_row.id=v_anchor_week;

  v_prepare:=public.candidate_weekly_paper_prepare_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_PREPARE',1,'{}'::jsonb,
    'targetless-paper:prepare',v_now+interval '1 second'
  );
  v_replay:=public.candidate_weekly_paper_prepare_atomic_v1(
    v_session,'TEST',v_workflow,'PAPER_PREPARE',1,'{}'::jsonb,
    'targetless-paper:prepare',v_now+interval '2 seconds'
  );

  select to_jsonb(anchor_row.*) into v_anchor_after
  from public.timesheets anchor_row
  where anchor_row.timesheet_id=v_anchor_timesheet and anchor_row.is_current;
  select to_jsonb(fin_row.*) into v_financial_after
  from public.timesheets_financials fin_row
  where fin_row.timesheet_id=v_anchor_timesheet and fin_row.is_current;
  select to_jsonb(week_row.*) into v_anchor_week_after
  from public.contract_weeks week_row where week_row.id=v_anchor_week;

  if v_prepare->>'state' is distinct from 'AWAITING_PAPER_RETURN'
     or v_prepare->>'paper_return_page_count' is distinct from '3'
     or v_replay->>'state' is distinct from 'AWAITING_PAPER_RETURN'
     or v_anchor_after is distinct from v_anchor_before
     or v_financial_after is distinct from v_financial_before
     or v_anchor_week_after is distinct from v_anchor_week_before
     or (select target_timesheet_id from public.candidate_submission_workflows
         where id=v_workflow) is not null
     or (select timesheet_id from public.contract_weeks where id=v_carrier_week)
          is not null
     or (select count(*) from public.timesheets
         where contract_id=v_contract and week_ending_date=current_date
           and is_current)<>1 then
    raise exception 'TARGETLESS_PAPER_PREPARE_CHANGED_APPROVED_AUTHORITY: %, %',
      v_prepare,v_replay;
  end if;

  v_mail_id:=(v_prepare#>>'{paper_pack,mail_outbox_id}')::uuid;
  select count(*)::integer into v_mail_count
  from public.mail_outbox mail_row
  where mail_row.id=v_mail_id
    and mail_row.type='TIMESHEET_QR'
    and mail_row.context_kind='timesheets'
    and mail_row.context_id=v_anchor_timesheet
    and mail_row.status='QUEUED'
    and mail_row.attempt_lease_token is null
    and mail_row.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
    and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow::text
    and mail_row.payment_scope_json->>'source_authority'='WORKFLOW_IMMUTABLE_SUBMISSION'
    and mail_row.payment_scope_json->>'source_authority_sha256'=
      encode(private._candidate_sha256_jsonb_v1(v_submission),'hex')
    and lower(coalesce(mail_row.payment_scope_json->>'candidate_paper_pack_ready','false'))
      in ('false','f','0','no')
    and lower(coalesce(mail_row.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
      in ('true','t','1','yes')
    and jsonb_typeof(mail_row.attachments)='array'
    and jsonb_array_length(mail_row.attachments)=0;
  if v_mail_count<>1 then
    raise exception 'TARGETLESS_PAPER_HELD_MAIL_AUTHORITY_INVALID: %',v_prepare;
  end if;

  v_old_manifest_sha256:=v_prepare->>'paper_return_manifest_sha256';
  v_promote:=public.candidate_paper_manifest_v2_promote_v1(
    v_session,'TEST',v_workflow,1,v_old_manifest_sha256,
    v_now+interval '3 seconds'
  );
  v_new_manifest_sha256:=v_promote->>'paper_return_manifest_sha256';
  if v_promote->>'manifest_version' is distinct from '2'
     or v_promote->>'qr_contract_version'
          is distinct from 'CANDIDATE_PAPER_PAGE_QR_V2'
     or v_promote->>'paper_return_page_count' is distinct from '3'
     or v_new_manifest_sha256 is null
     or v_new_manifest_sha256=v_old_manifest_sha256
     or (select payment_scope_json->>'paper_return_manifest_sha256'
         from public.mail_outbox where id=v_mail_id)
          is distinct from v_new_manifest_sha256
     or (select payment_scope_json->>'candidate_paper_manifest_version'
         from public.mail_outbox where id=v_mail_id)
          is distinct from '2'
     or (select target_timesheet_id from public.candidate_submission_workflows
         where id=v_workflow) is not null
     or (select timesheet_id from public.contract_weeks where id=v_carrier_week)
          is not null then
    raise exception 'TARGETLESS_PAPER_MANIFEST_PROMOTION_INVALID: %',v_promote;
  end if;

  if pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_targetless_expense_paper_pack_enqueue_v1(text,uuid,integer,text,timestamp with time zone)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_targetless_expense_paper_pack_enqueue_v1(text,uuid,integer,text,timestamp with time zone)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_targetless_expense_paper_pack_enqueue_v1(text,uuid,integer,text,timestamp with time zone)',
       'EXECUTE'
     ) then
    raise exception 'TARGETLESS_PAPER_HELPER_ACL_INVALID';
  end if;
end;
$targetless_expense_paper$;

rollback;
