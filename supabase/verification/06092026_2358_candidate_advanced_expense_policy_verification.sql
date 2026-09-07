\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the Advanced Expense and Refusal
-- authority.  This verifies the new private ledger boundary, exact Candidate
-- category replay, category-exact financial/evidence removal, unsigned summary
-- freshness, protection denial and generic Office-delete retirement without
-- changing retained application data.

begin;

update public.settings_defaults
set candidate_app_environment='TEST',
    candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
      ||'{"candidate_app_reads":true,"candidate_app_writes":true,"candidate_notifications":true,"candidate_expense_atomic_placement":true,"candidate_manager_approval":true,"candidate_paper_qr":true}'::jsonb
where id=1;

do $catalog$
declare
  v_table text;
  v_function regprocedure;
begin
  foreach v_table in array array[
    'candidate_expense_components','candidate_expense_component_events',
    'candidate_pending_expense_updates','candidate_expense_summary_refreshes',
    'candidate_expense_operations'
  ] loop
    if not exists(
      select 1 from pg_catalog.pg_class class
      join pg_catalog.pg_namespace namespace on namespace.oid=class.relnamespace
      where namespace.nspname='public' and class.relname=v_table
        and class.relkind='r' and class.relrowsecurity and class.relforcerowsecurity
    ) then
      raise exception 'Advanced expense table is not FORCE RLS: %',v_table;
    end if;
    if not exists(
      select 1 from pg_catalog.pg_policies policy
      where policy.schemaname='public' and policy.tablename=v_table
        and policy.policyname='cloudtms_miget_service_owner_all'
        and 'service_role'=any(policy.roles)
    ) then
      raise exception 'Advanced expense service-owner policy is missing: %',v_table;
    end if;
    if pg_catalog.has_table_privilege('anon','public.'||v_table,'SELECT')
       or pg_catalog.has_table_privilege('authenticated','public.'||v_table,'SELECT')
       or pg_catalog.has_table_privilege('service_role','public.'||v_table,'SELECT') then
      raise exception 'Advanced expense table leaked direct browser/service grants: %',v_table;
    end if;
  end loop;

  foreach v_function in array array[
    'public.candidate_expense_component_projection_v1(text,uuid[],uuid[])'::regprocedure,
    'public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz)'::regprocedure,
    'public.candidate_whole_claim_action_atomic_v1(uuid,text,uuid,integer,uuid,text,text,text,text,timestamptz)'::regprocedure,
    'public.candidate_expense_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)'::regprocedure,
    'public.candidate_expense_update_abort_atomic_v1(uuid,text,uuid,uuid,boolean,text,text,timestamptz)'::regprocedure,
    'public.candidate_expense_summary_claim_v1(integer,integer,timestamptz)'::regprocedure,
    'public.candidate_expense_summary_render_begin_v1(uuid,uuid,text,text,timestamptz)'::regprocedure,
    'public.candidate_expense_summary_complete_v1(uuid,uuid,text,text,text,timestamptz)'::regprocedure,
    'public.candidate_office_expense_category_reject_commit_v1(text,uuid,uuid,timestamptz)'::regprocedure,
    'public.candidate_office_expense_category_adapter_v1(uuid,text,jsonb,timestamptz)'::regprocedure
  ] loop
    if not pg_catalog.has_function_privilege('service_role',v_function,'EXECUTE')
       or pg_catalog.has_function_privilege('anon',v_function,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_function,'EXECUTE')
       or pg_catalog.has_function_privilege('public',v_function,'EXECUTE') then
      raise exception 'Advanced expense function ACL is not service-only: %',v_function;
    end if;
  end loop;

  if pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_office_expense_category_reject_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,text,timestamptz)'::regprocedure,
       'EXECUTE'
     ) then
    raise exception 'Unvalidated Office expense-category mutation leaked service execution';
  end if;
  if pg_catalog.to_regprocedure(
       'public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid)'
     ) is not null then
    raise exception 'Obsolete unguarded two-argument Timesheet delete apply was recreated';
  end if;

  if private._candidate_expense_component_status_v1(
       'SUBMITTED','PENDING','NOT_AUTHORISED'
     )<>'SUBMITTED'
     or private._candidate_expense_component_status_v1(
       'SUBMITTED','NOT_REQUESTED','NOT_AUTHORISED'
     )<>'MANAGER_APPROVAL_REQUIRED' then
    raise exception 'Pending-manager status mapping is not truthful';
  end if;
end;
$catalog$;

do $runtime$
declare
  v_actor uuid:=pg_catalog.gen_random_uuid();
  v_client uuid:=pg_catalog.gen_random_uuid();
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_contract uuid:=pg_catalog.gen_random_uuid();
  v_week uuid:=pg_catalog.gen_random_uuid();
  v_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_source_week uuid:=pg_catalog.gen_random_uuid();
  v_source_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_draft_week uuid:=pg_catalog.gen_random_uuid();
  v_draft_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_zero_week uuid:=pg_catalog.gen_random_uuid();
  v_zero_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_archive_week uuid:=pg_catalog.gen_random_uuid();
  v_archive_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_daily_week uuid:=pg_catalog.gen_random_uuid();
  v_daily_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_nonadjustment_week uuid:=pg_catalog.gen_random_uuid();
  v_nonadjustment_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_tombstone_week uuid:=pg_catalog.gen_random_uuid();
  v_tombstone_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_account uuid:=pg_catalog.gen_random_uuid();
  v_session uuid:=pg_catalog.gen_random_uuid();
  v_workflow uuid:=pg_catalog.gen_random_uuid();
  v_draft_workflow uuid:=pg_catalog.gen_random_uuid();
  v_zero_workflow uuid:=pg_catalog.gen_random_uuid();
  v_archive_workflow uuid:=pg_catalog.gen_random_uuid();
  v_daily_workflow uuid:=pg_catalog.gen_random_uuid();
  v_nonadjustment_workflow uuid:=pg_catalog.gen_random_uuid();
  v_tombstone_workflow uuid:=pg_catalog.gen_random_uuid();
  v_tombstone_history_workflow uuid:=pg_catalog.gen_random_uuid();
  v_accommodation uuid:=pg_catalog.gen_random_uuid();
  v_travel uuid:=pg_catalog.gen_random_uuid();
  v_other uuid:=pg_catalog.gen_random_uuid();
  v_draft_component uuid:=pg_catalog.gen_random_uuid();
  v_zero_component uuid:=pg_catalog.gen_random_uuid();
  v_archive_component uuid:=pg_catalog.gen_random_uuid();
  v_daily_component uuid:=pg_catalog.gen_random_uuid();
  v_nonadjustment_component uuid:=pg_catalog.gen_random_uuid();
  v_tombstone_component uuid:=pg_catalog.gen_random_uuid();
  v_accommodation_source uuid:=pg_catalog.gen_random_uuid();
  v_travel_source uuid:=pg_catalog.gen_random_uuid();
  v_travel_source_clone uuid:=pg_catalog.gen_random_uuid();
  v_result jsonb;
  v_replay jsonb;
  v_job jsonb;
  v_claim jsonb;
  v_refresh_id uuid;
  v_claim_token uuid;
  v_digest text;
  v_key text;
  v_archive_confirmation jsonb;
  v_archive_action jsonb;
  v_archive_idempotency uuid:=pg_catalog.gen_random_uuid();
  v_summary_sha text:=repeat('ab',32);
  v_event_count integer;
  v_scope jsonb;
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'advanced-expense-'||v_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;

  insert into public.clients(id,name)
  values(v_client,'Advanced expense verification client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    week_ending_weekday,candidate_expenses_require_separate_timesheet,
    candidate_paper_submission_enabled,candidate_expense_invoice_email
  ) values(
    pg_catalog.gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer,true,true,
    'advanced-expense-verification@example.test'
  );
  insert into public.candidates(id,email,active,key_norm,display_name)
  values(v_candidate,'advanced-expense-'||v_candidate::text||'@example.test',true,
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'Verification Candidate');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,is_adjustment,adjustment_origin,r2_nurse_key,r2_auth_key
  ) values(
    v_source_timesheet,'ADVANCED_SOURCE_'||replace(v_source_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','HOURS',
    'ELECTRONIC',false,null,
    'verification/source/candidate','verification/source/manager'
  ),(
    v_timesheet,'ADVANCED_EXPENSE_'||replace(v_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/advanced/candidate','verification/advanced/manager'
  ),(
    v_draft_timesheet,'ADVANCED_DRAFT_'||replace(v_draft_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/draft/candidate','verification/draft/manager'
  ),(
    v_zero_timesheet,'ADVANCED_ZERO_'||replace(v_zero_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/zero/candidate','verification/zero/manager'
  ),(
    v_archive_timesheet,'ADVANCED_ARCHIVE_'||replace(v_archive_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/archive/candidate','verification/archive/manager'
  ),(
    v_daily_timesheet,'ADVANCED_DAILY_'||replace(v_daily_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'DAILY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/daily/candidate','verification/daily/manager'
  ),(
    v_nonadjustment_timesheet,
    'ADVANCED_NONADJUST_'||replace(v_nonadjustment_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',false,null,
    'verification/nonadjustment/candidate','verification/nonadjustment/manager'
  ),(
    v_tombstone_timesheet,
    'ADVANCED_TOMBSTONE_'||replace(v_tombstone_timesheet::text,'-',''),
    'GCK-ADVANCED-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date-14,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/tombstone/candidate','verification/tombstone/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,is_adjustment,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_source_week,v_contract,current_date,0,false,'SUBMITTED','ELECTRONIC',
    v_source_timesheet
  ),(
    v_week,v_contract,current_date,801,true,'SUBMITTED','ELECTRONIC',v_timesheet
  ),(
    v_draft_week,v_contract,current_date,802,true,'OPEN','ELECTRONIC',v_draft_timesheet
  ),(
    v_zero_week,v_contract,current_date,803,true,'SUBMITTED','ELECTRONIC',v_zero_timesheet
  ),(
    v_archive_week,v_contract,current_date,807,true,'SUBMITTED','ELECTRONIC',
    v_archive_timesheet
  ),(
    v_daily_week,v_contract,current_date,804,true,'SUBMITTED','ELECTRONIC',v_daily_timesheet
  ),(
    v_nonadjustment_week,v_contract,current_date,805,false,'SUBMITTED','ELECTRONIC',
    v_nonadjustment_timesheet
  ),(
    v_tombstone_week,v_contract,current_date-14,806,true,'SUBMITTED','ELECTRONIC',
    v_tombstone_timesheet
  );

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    accommodation_pay_ex_vat,accommodation_charge_ex_vat,
    travel_pay_ex_vat,travel_charge_ex_vat,
    other_pay_ex_vat,other_charge_ex_vat,
    expenses_pay_ex_vat,expenses_charge_ex_vat,total_pay_ex_vat,total_charge_ex_vat,
    expenses_description,processing_status
  ) values(
    v_source_timesheet,1,v_candidate,v_client,7.5,
    0,0,0,0,0,0,0,0,0,0,'Worked hours source','PENDING_AUTH'
  ),(
    v_timesheet,1,v_candidate,v_client,0,
    25,25,12.50,12.50,10,-10,47.50,27.50,47.50,27.50,
    'Accommodation, Travel and Other expense','PENDING_AUTH'
  ),(
    v_draft_timesheet,1,v_candidate,v_client,0,
    5,5,0,0,0,0,5,5,5,5,'Draft accommodation','UNPROCESSED'
  ),(
    v_zero_timesheet,1,v_candidate,v_client,0,
    6,6,0,0,0,0,6,6,6,6,'Only accommodation','UNPROCESSED'
  ),(
    v_archive_timesheet,1,v_candidate,v_client,0,
    6.50,6.50,0,0,0,0,6.50,6.50,6.50,6.50,
    'Retained accommodation','UNPROCESSED'
  ),(
    v_daily_timesheet,1,v_candidate,v_client,0,
    7,7,0,0,0,0,7,7,7,7,'Daily accommodation','UNPROCESSED'
  ),(
    v_nonadjustment_timesheet,1,v_candidate,v_client,0,
    8,8,0,0,0,0,8,8,8,8,'Non-adjustment accommodation','UNPROCESSED'
  ),(
    v_tombstone_timesheet,1,v_candidate,v_client,0,
    9,9,0,0,0,0,9,9,9,9,'Tombstone accommodation','UNPROCESSED'
  );

  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','advanced-expense-'||v_candidate::text||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('d7',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );

  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,policy_snapshot_json,input_snapshot_json,idempotency_key,
    manager_approved_at_utc,finalised_at_utc
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'FINALISED',1,v_contract,v_week,v_timesheet,v_timesheet,current_date,'{}','{}',
    'advanced-expense:approved',now(),now()
  ),(
    v_draft_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'WORKER_DRAFT',1,v_contract,v_draft_week,v_draft_timesheet,v_draft_timesheet,
    current_date,'{}','{}','advanced-expense:draft',null,null
  ),(
    v_zero_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'FINALISED',1,v_contract,v_zero_week,v_zero_timesheet,v_zero_timesheet,
    current_date,'{}','{}','advanced-expense:zero',now(),now()
  ),(
    v_archive_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','FINALISED',1,v_contract,v_archive_week,
    v_archive_timesheet,v_archive_timesheet,current_date,'{}','{}',
    'advanced-expense:archive',now(),now()
  ),(
    v_daily_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'FINALISED',1,v_contract,v_daily_week,v_daily_timesheet,v_daily_timesheet,
    current_date,'{}','{}','advanced-expense:daily',now(),now()
  ),(
    v_nonadjustment_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','FINALISED',1,v_contract,v_nonadjustment_week,
    v_nonadjustment_timesheet,v_nonadjustment_timesheet,current_date,'{}','{}',
    'advanced-expense:nonadjustment',now(),now()
  ),(
    v_tombstone_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','FINALISED',1,v_contract,v_tombstone_week,
    v_tombstone_timesheet,v_tombstone_timesheet,current_date-14,'{}','{}',
    'advanced-expense:tombstone-active',now(),now()
  ),(
    v_tombstone_history_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY',
    'ELECTRONIC','REFUSED',7,v_contract,v_tombstone_week,
    v_tombstone_timesheet,v_tombstone_timesheet,current_date-14,'{}','{}',
    'advanced-expense:tombstone-history',null,null
  );

  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc,final_signed_storage_key,
    final_signed_content_sha256,final_signed_media_type,final_signed_byte_size,
    final_signed_page_count,final_signed_render_input_sha256,
    final_signed_renderer_contract_version,final_signed_renderer_receipt_json,
    final_signed_generated_at_utc,final_signed_render_state
  ) values(
    v_accommodation_source,v_workflow,1,1,v_timesheet,'EXPENSE_EVIDENCE',
    'ACCOMMODATION','SOURCE_EVIDENCE','IMMUTABLE','verification/accommodation.jpg',
    'image/jpeg',100,decode(repeat('21',32),'hex'),now(),
    'verification/accommodation.pdf',decode(repeat('31',32),'hex'),'application/pdf',100,
    1,decode(repeat('41',32),'hex'),'EXPENSE_EVIDENCE_PDF_V1','{}',now(),'READY'
  ),(
    v_travel_source,v_workflow,1,2,v_timesheet,'EXPENSE_EVIDENCE',
    'TRAVEL','SOURCE_EVIDENCE','IMMUTABLE','verification/travel.jpg',
    'image/jpeg',100,decode(repeat('22',32),'hex'),now(),
    'verification/travel.pdf',decode(repeat('32',32),'hex'),'application/pdf',100,
    1,decode(repeat('42',32),'hex'),'EXPENSE_EVIDENCE_PDF_V1','{}',now(),'READY'
  );
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,processing_state,document_role,
    candidate_component_id
  ) values(
    v_timesheet,'ACCOMMODATION','Accommodation receipt','verification/accommodation.pdf',
    'READY','SOURCE_EVIDENCE',v_accommodation_source
  ),(
    v_timesheet,'TRAVEL','Travel receipt','verification/travel.pdf',
    'READY','SOURCE_EVIDENCE',v_travel_source
  );

  insert into public.candidate_expense_components(
    expense_component_id,workflow_id,workflow_generation,expense_category,
    owning_timesheet_id,amount,mileage_units,lifecycle_state,
    manager_approval_state,agency_authorisation_state,submitted_at_utc,
    manager_approved_at_utc
  ) values(
    v_accommodation,v_workflow,1,'ACCOMMODATION',v_timesheet,25,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_travel,v_workflow,1,'TRAVEL',v_timesheet,12.50,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_other,v_workflow,1,'OTHER',v_timesheet,10,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_draft_component,v_draft_workflow,1,'ACCOMMODATION',v_draft_timesheet,5,0,
    'DRAFT','NOT_REQUESTED','NOT_AUTHORISED',null,null
  ),(
    v_zero_component,v_zero_workflow,1,'ACCOMMODATION',v_zero_timesheet,6,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_archive_component,v_archive_workflow,1,'ACCOMMODATION',v_archive_timesheet,6.50,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_daily_component,v_daily_workflow,1,'ACCOMMODATION',v_daily_timesheet,7,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_nonadjustment_component,v_nonadjustment_workflow,1,'ACCOMMODATION',
    v_nonadjustment_timesheet,8,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  ),(
    v_tombstone_component,v_tombstone_workflow,1,'ACCOMMODATION',
    v_tombstone_timesheet,9,0,
    'MANAGER_APPROVED','APPROVED','NOT_AUTHORISED',now(),now()
  );
  insert into public.timesheet_financial_retention(timesheet_id)
  values(v_archive_timesheet);

  -- Cancelling one approved category changes only that category.  Surviving
  -- Travel and an offset-pay/charge Other category prove the exact absolute
  -- emptiness predicate cannot misclassify this carrier as zero.
  v_result:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_workflow,1,v_accommodation,1,'CANCEL_EXPENSE',
    'advanced-expense:cancel-accommodation',now()
  );
  if coalesce(v_result->>'contract_version','')<>'CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1'
     or coalesce(v_result->>'action_code','')<>'CANCEL_EXPENSE'
     or coalesce(v_result->>'state','')<>'CANCELLED'
     or coalesce((v_result->>'zero_expense_carrier')::boolean,true)
     or coalesce(v_result->>'empty_timesheet_consequence','')<>'NONE'
     or jsonb_array_length(coalesce(
       v_result->'removed_from_current_timesheet_ids','[]'::jsonb
     ))<>0
     or coalesce((v_result->>'owning_timesheet_deleted')::boolean,true)
     or (select accommodation_pay_ex_vat from public.timesheets_financials
         where timesheet_id=v_timesheet and is_current) is distinct from 0::numeric
     or (select travel_pay_ex_vat from public.timesheets_financials
         where timesheet_id=v_timesheet and is_current) is distinct from 12.50::numeric
     or (select other_pay_ex_vat from public.timesheets_financials
         where timesheet_id=v_timesheet and is_current) is distinct from 10::numeric
     or (select other_charge_ex_vat from public.timesheets_financials
         where timesheet_id=v_timesheet and is_current) is distinct from (-10)::numeric
     or (select processing_state from public.timesheet_evidence
         where candidate_component_id=v_accommodation_source) is distinct from 'SUPERSEDED'
     or (select processing_state from public.timesheet_evidence
         where candidate_component_id=v_travel_source) is distinct from 'READY' then
    raise exception 'Category-exact cancellation did not preserve surviving values/evidence: %',v_result;
  end if;
  if (select manager_approval_state from public.candidate_expense_components
      where expense_component_id=v_travel) is distinct from 'APPROVED' then
    raise exception 'Unchanged Travel manager approval was invalidated';
  end if;
  v_replay:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_workflow,1,v_accommodation,1,'CANCEL_EXPENSE',
    'advanced-expense:cancel-accommodation',now()+interval '1 second'
  );
  if not coalesce((v_replay->>'idempotent_replay')::boolean,false)
     or v_replay-'idempotent_replay'<>v_result-'idempotent_replay' then
    raise exception 'Category cancellation lost-response replay changed: %, %',v_result,v_replay;
  end if;

  update public.candidate_expense_components
  set agency_authorisation_state='AUTHORISED',owning_timesheet_id=null,
    updated_at_utc=now()
  where expense_component_id=v_travel;
  begin
    perform public.candidate_expense_component_action_atomic_v1(
      v_session,'TEST',v_workflow,1,v_travel,1,'CANCEL_EXPENSE',
      'advanced-expense:protected-negative',now()
    );
    raise exception 'Protected expense category was cancellable';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' then raise; end if;
  end;
  if private._candidate_whole_claim_action_v1('TEST',v_timesheet) is not null then
    raise exception 'Whole-claim action was projected with a target-less protected component';
  end if;
  update public.candidate_expense_components
  set owning_timesheet_id=v_timesheet,updated_at_utc=now()
  where expense_component_id=v_travel;

  -- An unchanged carried source uses the original source as its canonical
  -- lineage while materialised Timesheet evidence still points at that
  -- original.  Projection and the internal summary must count it once, not
  -- once per immutable generation plus once per materialisation.
  update public.candidate_submission_components
  set state='SUPERSEDED',superseded_at_utc=now()
  where id=v_travel_source;
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,source_component_id,storage_key,media_type,
    byte_size,source_content_sha256,immutable_at_utc,required,review_ordinal,
    review_render_state,final_signed_render_state
  ) select
    v_travel_source_clone,workflow_id,workflow_generation,99,timesheet_id,component_kind,
    expense_category,document_role,'IMMUTABLE',v_travel_source,storage_key,media_type,
    byte_size,source_content_sha256,now(),false,null,'NOT_REQUIRED','NOT_REQUIRED'
  from public.candidate_submission_components where id=v_travel_source;
  select private._candidate_expense_component_json_v1(component,false)
  into v_result
  from public.candidate_expense_components component
  where component.expense_component_id=v_travel;
  if coalesce((v_result->>'supporting_evidence_count')::integer,-1)<>1 then
    raise exception 'Carried evidence lineage was double-counted in projection: %',v_result;
  end if;
  v_scope:=private._candidate_whole_claim_scope_v1(
    'TEST',v_candidate,v_contract,current_date
  );
  begin
    perform public.candidate_whole_claim_action_atomic_v1(
      v_session,'TEST',v_workflow,1,v_timesheet,
      v_scope->>'scope_sha256','CANCEL_ENTIRE_CLAIM',
      'Protected component negative','advanced-expense:whole-protected-negative',now()
    );
    raise exception 'Whole-claim mutation ignored a protected component';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' then raise; end if;
  end;

  -- The summary queue is content-addressed and includes retained identity,
  -- only non-zero categories and exact evidence counts.  Registering the R2
  -- attempt before PUT makes completion exactly replayable.
  select refresh_id into v_refresh_id
  from public.candidate_expense_summary_refreshes
  where timesheet_id_snapshot=v_timesheet and state='PENDING'
  order by summary_generation desc limit 1;
  if v_refresh_id is null then
    v_result:=private._candidate_expense_summary_queue_v1(v_timesheet,now());
    v_refresh_id:=(v_result->>'refresh_id')::uuid;
  end if;
  update public.candidate_expense_summary_refreshes
  set requested_at_utc='2000-01-01 00:00:00+00'
  where refresh_id=v_refresh_id;
  v_claim:=public.candidate_expense_summary_claim_v1(25,300,now());
  select value into v_job from jsonb_array_elements(v_claim->'jobs') job(value)
  where value->>'refresh_id'=v_refresh_id::text;
  if v_job is null
     or v_job#>>'{totals,identity,candidate_name}'<>'Verification Candidate'
     or jsonb_typeof(v_job#>'{totals,categories}') is distinct from 'array'
     or jsonb_array_length(v_job#>'{totals,categories}')<>2
     or not exists(
       select 1 from jsonb_array_elements(v_job#>'{totals,categories}') category
       where category->>'expense_category'='TRAVEL'
         and (category->>'amount')::numeric=12.50
         and (category->>'mileage_units')::numeric=0
         and (category->>'supporting_evidence_count')::integer=1
     )
     or not exists(
       select 1 from jsonb_array_elements(v_job#>'{totals,categories}') category
       where category->>'expense_category'='OTHER'
         and (category->>'amount')::numeric=10
         and (category->>'mileage_units')::numeric=0
         and (category->>'supporting_evidence_count')::integer=0
     )
     or exists(select 1 from jsonb_array_elements(v_job#>'{totals,categories}') category
       where coalesce((category->>'amount')::numeric,0)=0
         and coalesce((category->>'mileage_units')::numeric,0)=0) then
    raise exception 'Expense summary claim omitted identity or retained a zero category: %',v_job;
  end if;
  v_claim_token:=(v_job->>'claim_token')::uuid;
  v_digest:=v_job->>'totals_sha256';
  v_key:='verification/expense-summaries/'||v_timesheet::text||'/'
    ||lpad((v_job->>'summary_generation'),6,'0')||'-'||v_digest||'-'
    ||v_claim_token::text||'.pdf';
  perform public.candidate_expense_summary_render_begin_v1(
    v_refresh_id,v_claim_token,v_digest,v_key,now()
  );
  v_result:=public.candidate_expense_summary_complete_v1(
    v_refresh_id,v_claim_token,v_digest,v_key,v_summary_sha,now()
  );
  v_replay:=public.candidate_expense_summary_complete_v1(
    v_refresh_id,v_claim_token,v_digest,v_key,v_summary_sha,now()+interval '1 second'
  );
  if coalesce(v_result->>'state','')<>'READY'
     or not coalesce((v_replay->>'idempotent_replay')::boolean,false) then
    raise exception 'Expense summary completion/replay is not exact: %, %',v_result,v_replay;
  end if;

  -- The exact last category on a proved WEEKLY Candidate expense-only
  -- adjustment deletes that empty carrier and leaves a workflow/component
  -- tombstone plus durable cleanup keys. DAILY and non-adjustment owners are
  -- deliberately retained after the same category cancellation.
  v_result:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_zero_workflow,1,v_zero_component,1,'CANCEL_EXPENSE',
    'advanced-expense:zero-carrier-delete',now()
  );
  if coalesce((v_result->>'zero_expense_carrier')::boolean,false) is not true
     or coalesce((v_result->>'owning_timesheet_deleted')::boolean,false) is not true
     or coalesce(v_result->>'empty_timesheet_consequence','')<>'PERMANENT_REMOVE'
     or not (coalesce(v_result->'deleted_timesheet_ids','[]'::jsonb)
       @> jsonb_build_array(v_zero_timesheet))
     or not (coalesce(v_result->'removed_from_current_timesheet_ids','[]'::jsonb)
       @> jsonb_build_array(v_zero_timesheet))
     or jsonb_typeof(v_result->'r2_cleanup_keys') is distinct from 'array'
     or not (v_result->'r2_cleanup_keys' @> jsonb_build_array(
       'verification/zero/candidate','verification/zero/manager'
     ))
     or exists(select 1 from public.timesheets row
       where row.timesheet_id=v_zero_timesheet)
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_source_timesheet and row.is_current)
     or not exists(select 1 from public.contract_weeks week
       where week.id=v_source_week and week.timesheet_id=v_source_timesheet)
     or (select owning_timesheet_id from public.candidate_expense_components
         where expense_component_id=v_zero_component) is not null
     or not coalesce((select input_snapshot_json ? 'expense_carrier_delete_tombstone'
         from public.candidate_submission_workflows where id=v_zero_workflow),false) then
    raise exception 'Eligible zero Candidate expense carrier was not safely deleted: %',v_result;
  end if;

  -- A retained-source carrier that crossed the sticky financial-history
  -- boundary cannot be physically deleted. Office must confirm the exact
  -- removal-from-current consequence and scope without calling it deletion.
  v_scope:=public.candidate_office_expense_category_projection_v1(
    'TEST',v_archive_timesheet,now()
  );
  select category->'office_rejection_confirmation',category->'office_rejection_action'
  into v_archive_confirmation,v_archive_action
  from jsonb_array_elements(v_scope->'expense_claims') claim
  cross join lateral jsonb_array_elements(claim->'categories') category
  where category->>'expense_component_id'=v_archive_component::text;
  if coalesce(v_archive_confirmation->>'empty_timesheet_consequence','')
       <>'REMOVE_FROM_CURRENT_KEEP_HISTORY'
     or coalesce((v_archive_confirmation->>'will_delete_timesheet')::boolean,true)
     or coalesce(v_archive_action->>'code','')<>'REJECT_EXPENSE_CATEGORY' then
    raise exception 'Office archive consequence was not presented truthfully: %, %',
      v_archive_confirmation,v_archive_action;
  end if;

  -- Changing one confirmation-bound identity fact must invalidate the old
  -- digest before an operation, component, financial or archive mutation.
  update public.timesheets set version=version+1,updated_at=now()
  where timesheet_id=v_archive_timesheet;
  begin
    perform public.candidate_office_expense_category_adapter_v1(
      v_actor,'TEST',
      (v_archive_action#>'{invocation,fixed_body}')||jsonb_build_object(
        'workflow_id',v_archive_workflow,
        'reason_note','History-retained Office rejection',
        'idempotency_key',v_archive_idempotency
      ),now()
    );
    raise exception 'Stale Office archive confirmation was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED' then raise; end if;
  end;
  if (select lifecycle_state from public.candidate_expense_components
      where expense_component_id=v_archive_component) is distinct from 'MANAGER_APPROVED'
     or exists(select 1 from public.timesheets row
       where row.timesheet_id=v_archive_timesheet and row.archived_at_utc is not null) then
    raise exception 'Stale Office archive confirmation mutated current state';
  end if;

  v_scope:=public.candidate_office_expense_category_projection_v1(
    'TEST',v_archive_timesheet,now()
  );
  select category->'office_rejection_confirmation',category->'office_rejection_action'
  into v_archive_confirmation,v_archive_action
  from jsonb_array_elements(v_scope->'expense_claims') claim
  cross join lateral jsonb_array_elements(claim->'categories') category
  where category->>'expense_component_id'=v_archive_component::text;
  v_result:=public.candidate_office_expense_category_adapter_v1(
    v_actor,'TEST',
    (v_archive_action#>'{invocation,fixed_body}')||jsonb_build_object(
      'workflow_id',v_archive_workflow,
      'reason_note','History-retained Office rejection',
      'idempotency_key',v_archive_idempotency
    ),now()
  );
  if coalesce(v_result->>'contract_version','')<>
       'OFFICE_EXPENSE_CATEGORY_REJECTION_RESULT_V2'
     or coalesce(v_result->>'empty_timesheet_consequence','')<>
       'REMOVE_FROM_CURRENT_KEEP_HISTORY'
     or coalesce((v_result->>'owning_timesheet_deleted')::boolean,true)
     or jsonb_array_length(coalesce(v_result->'deleted_timesheet_ids','[]'::jsonb))<>0
     or not (coalesce(v_result->'retained_timesheet_ids','[]'::jsonb)
       @> jsonb_build_array(v_archive_timesheet))
     or not (coalesce(v_result->'affected_timesheet_ids','[]'::jsonb)
       @> jsonb_build_array(v_archive_timesheet))
     or not (coalesce(v_result->'removed_from_current_timesheet_ids','[]'::jsonb)
       @> jsonb_build_array(v_archive_timesheet))
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_archive_timesheet and row.is_current
         and row.archived_at_utc is not null
         and row.archived_reason_code='FINANCIAL_HISTORY_PREVENTED_DELETE')
     or exists(select 1 from public.timesheets row
       where row.timesheet_id=v_archive_timesheet and row.is_current
         and row.archived_at_utc is null)
     or not exists(select 1 from public.contract_weeks week
       where week.id=v_archive_week and week.timesheet_id=v_archive_timesheet)
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_source_timesheet and row.is_current
         and row.archived_at_utc is null)
     or not exists(select 1 from public.contract_weeks week
       where week.id=v_source_week and week.timesheet_id=v_source_timesheet)
     or (select lifecycle_state from public.candidate_expense_components
         where expense_component_id=v_archive_component) is distinct from 'OFFICE_REJECTED'
     or coalesce((select progress_json->>'expected_delete_target_scope_sha256'
         from public.candidate_expense_operations
         where operation_id=(v_result->>'operation_id')::uuid),'') !~ '^[0-9a-f]{64}$'
     or not exists(select 1 from public.audit_events event
       where event.object_type='timesheets'
         and event.object_id_text=v_archive_timesheet::text
         and event.action='TIMESHEET_ARCHIVED') then
    raise exception 'Financial-history zero carrier was not safely archived: %',v_result;
  end if;
  begin
    perform private._candidate_office_projection_identity_v1(
      v_archive_timesheet,v_archive_week
    );
    raise exception 'Archived empty carrier remained addressable as a current Office row';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' then raise; end if;
  end;

  -- A legacy expense-only carrier with no proved worked/base source is
  -- retained rather than broadened into a parent-chain delete.  Both its live
  -- workflow and terminal history keep their exact state/generation.
  v_result:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_tombstone_workflow,1,v_tombstone_component,1,
    'CANCEL_EXPENSE','advanced-expense:terminal-history-preserved',now()
  );
  if coalesce((v_result->>'owning_timesheet_deleted')::boolean,true)
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_tombstone_timesheet and row.is_current)
     or (select state from public.candidate_submission_workflows
         where id=v_tombstone_workflow) is distinct from 'FINALISED'
     or (select generation from public.candidate_submission_workflows
         where id=v_tombstone_workflow) is distinct from 1
     or (select state from public.candidate_submission_workflows
         where id=v_tombstone_history_workflow) is distinct from 'REFUSED'
     or (select generation from public.candidate_submission_workflows
         where id=v_tombstone_history_workflow) is distinct from 7
     or (select target_timesheet_id from public.candidate_submission_workflows
         where id=v_tombstone_history_workflow) is distinct from v_tombstone_timesheet then
    raise exception 'Zero-carrier deletion rewrote terminal workflow history: %',v_result;
  end if;

  v_result:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_daily_workflow,1,v_daily_component,1,'CANCEL_EXPENSE',
    'advanced-expense:daily-retained',now()
  );
  if coalesce((v_result->>'zero_expense_carrier')::boolean,true)
     or coalesce((v_result->>'owning_timesheet_deleted')::boolean,true)
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_daily_timesheet and row.is_current)
     or (select accommodation_pay_ex_vat from public.timesheets_financials
         where timesheet_id=v_daily_timesheet and is_current) is distinct from 0::numeric then
    raise exception 'DAILY expense carrier was deleted or cancellation failed: %',v_result;
  end if;

  v_result:=public.candidate_expense_component_action_atomic_v1(
    v_session,'TEST',v_nonadjustment_workflow,1,v_nonadjustment_component,1,
    'CANCEL_EXPENSE','advanced-expense:nonadjustment-retained',now()
  );
  if coalesce((v_result->>'zero_expense_carrier')::boolean,true)
     or coalesce((v_result->>'owning_timesheet_deleted')::boolean,true)
     or not exists(select 1 from public.timesheets row
       where row.timesheet_id=v_nonadjustment_timesheet and row.is_current)
     or (select accommodation_pay_ex_vat from public.timesheets_financials
         where timesheet_id=v_nonadjustment_timesheet and is_current) is distinct from 0::numeric then
    raise exception 'Non-adjustment expense carrier was deleted or cancellation failed: %',v_result;
  end if;

  -- Generic Office deletion retires the exact live draft category and records
  -- an auditable terminal event before its owner can be detached/deleted.
  update public.tms_users set is_active=false where id=v_actor;
  perform public.timesheet_weekly_manual_adjustment_delete_preview(
    v_draft_timesheet,v_actor
  );
  perform private._candidate_timesheet_delete_retire_workflows_v1(
    'TEST',array[v_draft_timesheet],array[v_draft_week],array[]::uuid[],
    v_actor,pg_catalog.gen_random_uuid(),now()
  );
  if (select lifecycle_state from public.candidate_expense_components
      where expense_component_id=v_draft_component) is distinct from 'CANCELLED'
     or (select owning_timesheet_id from public.candidate_expense_components
         where expense_component_id=v_draft_component) is not null
     or not exists(
       select 1 from public.candidate_expense_component_events event
       where event.expense_component_id=v_draft_component
         and event.event_type='CANCELLED' and event.actor_kind='OFFICE'
     )
     or private._candidate_expense_component_action_v1(
       (select component from public.candidate_expense_components component
        where component.expense_component_id=v_draft_component)
     ) is not null then
    raise exception 'Generic Office delete left a live/actionable expense component';
  end if;

  select count(*)::integer into v_event_count
  from public.candidate_expense_component_events event
  where event.expense_component_id=v_accommodation and event.event_type='CANCELLED';
  if v_event_count<>1
     or (select count(*) from public.candidate_notifications notification
         where notification.workflow_id=v_workflow
           and notification.event_type='EXPENSE_CANCELLED')<>1 then
    raise exception 'Category cancellation audit/notification was not exactly once';
  end if;
end;
$runtime$;

do $paper_replacement_hold$
declare
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_client uuid:=pg_catalog.gen_random_uuid();
  v_contract uuid:=pg_catalog.gen_random_uuid();
  v_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_week uuid:=pg_catalog.gen_random_uuid();
  v_account uuid:=pg_catalog.gen_random_uuid();
  v_session uuid:=pg_catalog.gen_random_uuid();
  v_workflow uuid:=pg_catalog.gen_random_uuid();
  v_mail uuid:=pg_catalog.gen_random_uuid();
  v_old_mail uuid:=pg_catalog.gen_random_uuid();
  v_update uuid:=pg_catalog.gen_random_uuid();
  v_manifest jsonb;
  v_manifest_sha bytea;
  v_manifest_hex text;
  v_attempt_token text:=repeat('7c',32);
  v_operation_key text:='advanced-expense-paper-pack-operation';
  v_base_sha text:=repeat('8d',32);
  v_branding_sha text:=repeat('9e',32);
  v_pack_sha text:=repeat('af',32);
  v_renderer text:='CANDIDATE_REVIEW_DOCUMENTS_V1';
  v_pack_key text;
  v_result jsonb;
  v_scope jsonb;
  v_attachments jsonb;
  v_scheduled timestamptz;
  v_next_attempt timestamptz;
  v_claimed uuid[];
  v_retire_blocked boolean:=false;
  v_forbidden_manifest jsonb;
  v_forbidden_manifest_blocked boolean:=false;
  v_summary_only_manifest jsonb;
  v_summary_only_blocked boolean:=false;
  v_component uuid;
  v_component_generation integer;
  v_office_actor uuid:=pg_catalog.gen_random_uuid();
  v_office_operation uuid:=pg_catalog.gen_random_uuid();
  v_candidate_operation uuid:=pg_catalog.gen_random_uuid();
  v_office_confirmation text:=repeat('b1',32);
begin
  v_manifest:=jsonb_build_object(
    'manifest_version',2,
    'qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
    'workflow_id',v_workflow,'workflow_generation',2,
    'immutable_submission_sha256',repeat('31',32),
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','EXPENSE_EVIDENCE:TRAVEL:1',
      'component_kind','EXPENSE_EVIDENCE',
      'expense_category','TRAVEL'
    ))
  );
  v_manifest_sha:=private._candidate_sha256_jsonb_v1(v_manifest);
  v_manifest_hex:=encode(v_manifest_sha,'hex');
  v_pack_key:='candidate-app/test/'||v_workflow::text||'/2/paper-pack/'
    ||v_manifest_hex||'-'||v_base_sha||'-'||v_branding_sha||'-'
    ||v_renderer||'.pdf';

  insert into public.candidates(id,email,active,key_norm,display_name)
  values(v_candidate,'paper-hold-'||v_candidate::text||'@example.test',true,
    'GCK-PAPER-HOLD-'||replace(v_candidate::text,'-',''),'Paper Hold Candidate');
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_office_actor,'paper-hold-office-'||v_office_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true);
  insert into public.clients(id,name) values(v_client,'Paper hold verification client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    week_ending_weekday,candidate_expenses_require_separate_timesheet,
    candidate_paper_submission_enabled,candidate_expense_invoice_email
  ) values(
    pg_catalog.gen_random_uuid(),v_client,current_date-7,'MANUAL',
    extract(dow from current_date)::integer,true,true,
    'paper-hold-verification@example.test'
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,'PAYE',
    extract(dow from current_date)::integer,'MANUAL'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,is_adjustment,adjustment_origin,qr_status,qr_token,
    qr_payload_json,qr_generated_at,document_state,r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,'ADVANCED_PAPER_'||replace(v_timesheet::text,'-',''),
    'GCK-PAPER-HOLD-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','HOURS',
    'MANUAL',true,'CANDIDATE_EXPENSE_ONLY','PENDING','paper-hold-token',
    jsonb_build_object('v',2,'tok','paper-hold-token'),now(),'READY',
    'verification/paper/candidate','verification/paper/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,803,'SUBMITTED','MANUAL',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    travel_pay_ex_vat,travel_charge_ex_vat,expenses_pay_ex_vat,
    expenses_charge_ex_vat,total_pay_ex_vat,total_charge_ex_vat,
    processing_status
  ) values(v_timesheet,1,v_candidate,v_client,0,15,15,15,15,15,15,'PENDING_AUTH');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','paper-hold-'||v_candidate::text||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('ce',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,policy_snapshot_json,input_snapshot_json,idempotency_key,
    immutable_submission_json,immutable_submission_sha256,
    review_manifest_json,review_manifest_sha256,
    paper_return_manifest_json,paper_return_manifest_sha256,
    renderer_contract_version
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_COMBINED','WEEKLY','PAPER',
    'AWAITING_PAPER_RETURN',2,v_contract,v_week,v_timesheet,v_timesheet,
    current_date,'{}','{}','advanced-expense:paper-hold',
    jsonb_build_object(
      'expense_submission',jsonb_build_object('canonical_tsfin_snapshot',
        jsonb_build_object(
          'travel_pay_ex_vat',15,'travel_charge_ex_vat',15,
          'expenses_pay_ex_vat',15,'expenses_charge_ex_vat',15
        )),
      'official_presentation',jsonb_build_object(
        'renderer_contract_version',v_renderer,
        'branding',jsonb_build_object('branding_contract_sha256',v_branding_sha))
    ),
    decode(repeat('31',32),'hex'),'{}',private._candidate_sha256_jsonb_v1('{}'::jsonb),
    v_manifest,v_manifest_sha,v_renderer
  );

  -- The shared BEGIN RPC is service-callable for the ordinary EMAIL/PHONE
  -- update path, but a public Candidate may not use it to bypass the durable
  -- CREATE_UPDATED_DOCUMENTS operation that owns a PAPER replacement.
  perform set_config('cloudtms.candidate_paper_update_begin_context','',true);
  begin
    perform public.candidate_expense_update_begin_atomic_v1(
      v_session,'TEST',v_workflow,2,
      '[{"update_kind":"ADD_CATEGORY","expense_category":"OTHER"}]'::jsonb,
      'advanced-expense:paper-direct-begin-denied',now()
    );
    raise exception 'Public BEGIN_EXPENSE_UPDATE bypassed PAPER replacement authority';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_PAPER_DOCUMENT_UPDATE_REQUIRED' then raise; end if;
  end;
  if (select state from public.candidate_submission_workflows where id=v_workflow)
       is distinct from 'AWAITING_PAPER_RETURN'
     or (select generation from public.candidate_submission_workflows where id=v_workflow)
       is distinct from 2
     or exists(
       select 1 from public.candidate_pending_expense_updates update_row
       where update_row.workflow_id=v_workflow
     ) then
    raise exception 'Rejected public PAPER BEGIN mutated workflow/update authority';
  end if;

  insert into public.candidate_pending_expense_updates(
    update_id,workflow_id,from_workflow_generation,current_workflow_generation,
    update_plan_json,state,update_mode,actor_kind,prior_workflow_state,
    prior_workflow_snapshot_json,prior_immutable_submission_json,
    prior_immutable_submission_sha256,prior_review_manifest_json,
    prior_review_manifest_sha256,prior_paper_source_timesheet_id,
    prior_paper_source_timesheet_id_snapshot,prior_paper_source_snapshot_json,
    prior_paper_source_snapshot_sha256,idempotency_key,started_at_utc,
    expires_at_utc,updated_at_utc
  ) values(
    v_update,v_workflow,1,2,
    '[{"update_kind":"ADD_CATEGORY","expense_category":"TRAVEL"}]'::jsonb,
    'RENDERING','PAPER_REPLACEMENT','CANDIDATE','AWAITING_PAPER_RETURN',
    (select to_jsonb(workflow) from public.candidate_submission_workflows workflow
      where workflow.id=v_workflow),
    '{}'::jsonb,private._candidate_sha256_jsonb_v1('{}'::jsonb),
    '{}'::jsonb,private._candidate_sha256_jsonb_v1('{}'::jsonb),
    v_timesheet,v_timesheet,'{}'::jsonb,
    private._candidate_sha256_jsonb_v1('{}'::jsonb),
    'advanced-expense:paper-hold-update',now(),now()+interval '30 minutes',now()
  );
  select component.expense_component_id,component.component_generation
  into v_component,v_component_generation
  from public.candidate_expense_components component
  where component.workflow_id=v_workflow and component.expense_category='TRAVEL';
  if v_component is null then
    raise exception 'PAPER replacement fixture did not create its Travel authority';
  end if;
  if coalesce((select private._candidate_expense_component_action_v1(component)->>'code'
      from public.candidate_expense_components component
      where component.expense_component_id=v_component),'')<>'WITHDRAW_EXPENSE' then
    raise exception 'PAPER category did not project its exact withdrawal action';
  end if;
  insert into public.candidate_expense_operations(
    operation_id,environment,account_id,candidate_id,actor_kind,actor_id,
    action_code,workflow_id,timesheet_id,expense_component_id,request_sha256,
    idempotency_key,state,progress_json,created_at_utc,updated_at_utc
  ) values(
    v_candidate_operation,'TEST',v_account,v_candidate,'CANDIDATE',v_candidate,
    'RESUBMIT_EXPENSE_CATEGORY',v_workflow,v_timesheet,v_component,
    private._candidate_sha256_jsonb_v1(jsonb_build_object(
      'contract_version','CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_REQUEST_V1',
      'source_workflow_id',v_workflow,'source_workflow_generation',2,
      'source_expense_component_id',v_component,
      'source_component_generation',v_component_generation
    )),'advanced-expense-paper-resubmit','PREPARING',jsonb_build_object(
      'contract_version','CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1',
      'ok',true,'operation_id',v_candidate_operation,
      'action_code','RESUBMIT_EXPENSE_CATEGORY',
      'source_workflow_id',v_workflow,
      'source_expense_component_id',v_component,
      'source_component_generation',v_component_generation,
      'expense_category','TRAVEL',
      'category_changes',jsonb_build_array(jsonb_build_object(
        'update_kind','ADD_CATEGORY','expense_category','TRAVEL'
      )),
      'editor_mode','PAPER_REPLACEMENT',
      'workflow_id',v_workflow,'generation',2,'state','WORKER_DRAFT',
      'update_state','UPDATING','update_id',v_update,'blank_claim',true,
      'route_selection_required',false,'route','PAPER',
      'paper_pack_replacement',true,'old_pack_recoverable',true,
      'manager_link_preserved',false,'idempotent_replay',false
    ),now(),now()
  );
  update public.candidate_pending_expense_updates
  set operation_id=v_candidate_operation
  where update_id=v_update;
  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,sent_at,
    context_kind,context_id,scheduled_for_utc,next_attempt_at_utc,reference,
    payment_scope_json
  ) values(
    v_old_mail,'TIMESHEET_QR','paper-hold@example.test','Original paper pack',
    jsonb_build_array(jsonb_build_object(
      'filename','original-paper-pack.pdf','storage_key','verification/paper/original.pdf'
    )),'SENT',now()-interval '1 hour',now()-interval '1 hour',
    'timesheets',v_timesheet,now()-interval '1 hour',now()-interval '1 hour',
    'advanced-expense-paper-original',jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1',
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',1,
      'paper_return_manifest_sha256',repeat('42',32),
      'candidate_paper_pack_ready',true,
      'qr_token_hash',encode(extensions.digest(
        convert_to('old-paper-token','UTF8'),'sha256'),'hex')
    )
  );
  insert into public.mail_outbox(
    id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
    scheduled_for_utc,next_attempt_at_utc,reference,payment_scope_json
  ) values(
    v_mail,'TIMESHEET_QR','paper-hold@example.test','Updated paper pack','[]'::jsonb,
    'QUEUED',now(),'timesheets',v_timesheet,'infinity','infinity',
    'advanced-expense-paper-hold',jsonb_build_object(
      'candidate_mail_authority','CANDIDATE_PAPER_V1',
      'candidate_workflow_id',v_workflow,'candidate_workflow_generation',2,
      'paper_return_manifest_sha256',v_manifest_hex,
      'candidate_paper_pack_ready',false,'mail_held_until_pdf_rendered',true,
      'mail_delayed_for_pdf_render',true,
      'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
      'qr_token_hash',encode(extensions.digest(
        convert_to('paper-hold-token','UTF8'),'sha256'),'hex')
    )
  );

  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PACK_ATTEMPT_CLAIM',2,
    jsonb_build_object(
      'service_paper_pack_attempt',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hex,
      'paper_pack_attempt_token',v_attempt_token,
      'paper_pack_operation_id',v_operation_key
    ),v_operation_key,now()
  );
  if not coalesce((v_result->>'claim_acquired_new')::boolean,false) then
    raise exception 'PAPER replacement render attempt was not acquired: %',v_result;
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow,'PAPER_PACK_RELEASE',2,
    jsonb_build_object(
      'service_paper_pack_release',true,'mail_outbox_id',v_mail,
      'paper_return_manifest_sha256',v_manifest_hex,
      'complete_pack_storage_key',v_pack_key,'complete_pack_sha256',v_pack_sha,
      'complete_pack_byte_size',500,'complete_pack_page_count',1,
      'complete_pack_media_type','application/pdf','base_document_sha256',v_base_sha,
      'branding_contract_sha256',v_branding_sha,
      'renderer_contract_version',v_renderer,
      'paper_pack_attempt_token',v_attempt_token,
      'paper_pack_operation_id',v_operation_key
    ),'advanced-expense-paper-release',now()
  );
  select payment_scope_json,attachments,scheduled_for_utc,next_attempt_at_utc
  into v_scope,v_attachments,v_scheduled,v_next_attempt
  from public.mail_outbox where id=v_mail;
  if coalesce((v_result->>'paper_pack_held_for_expense_update')::boolean,false) is not true
     or lower(coalesce(v_scope->>'candidate_paper_pack_ready','false'))<>'true'
     or lower(coalesce(v_scope->>'candidate_expense_update_hold','false'))<>'true'
     or coalesce(v_scope->>'mail_hold_reason','')<>'CANDIDATE_EXPENSE_UPDATE_PENDING'
     or v_scheduled is distinct from 'infinity'::timestamptz
     or v_next_attempt is distinct from 'infinity'::timestamptz
     or jsonb_typeof(v_attachments) is distinct from 'array'
     or jsonb_array_length(v_attachments)<>1
     or exists(select 1 from public.candidate_notifications notification
       where notification.workflow_id=v_workflow and notification.event_type='PAPER_PACK_READY')
     or exists(select 1 from jsonb_array_elements(v_manifest->'pages') page
       where page->>'page_key'='EXPENSE_SUMMARY') then
    raise exception 'PAPER replacement became visible/sendable before rebind: %, %',v_result,v_scope;
  end if;
  select coalesce(array_agg(claimed.id),array[]::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(100,'advanced-expense-paper-hold',5) claimed;
  if v_mail=any(v_claimed) then
    raise exception 'Held PAPER replacement mail was claimable before rebind';
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'PAPER_PROVIDER_SUBMIT_PERMIT',2,
      jsonb_build_object(
        'service_paper_provider_submit_permit',true,'mail_outbox_id',v_mail,
        'attempt_lease_token','not-granted-while-held',
        'paper_return_manifest_sha256',v_manifest_hex
      ),'advanced-expense-paper-provider-negative',now()
    );
    raise exception 'PAPER provider permit ignored the active replacement hold';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_EXPENSE_UPDATE_IN_PROGRESS' then raise; end if;
  end;

  -- The same transaction that retires the old immutable generation must
  -- release exactly one fully rendered new pack and create exactly one
  -- Candidate-ready notice.  Before this point no provider could claim it.
  v_result:=public.candidate_expense_update_rebind_atomic_v1(
    'TEST',v_workflow,v_update,'advanced-expense-paper-rebind',now()
  );
  if coalesce(v_result->>'state','')<>'AWAITING_PAPER_RETURN'
     or coalesce(v_result->>'update_state','')<>'NONE'
     or coalesce((v_result->>'paper_pack_replacement')::boolean,false) is not true
     or coalesce((v_result->>'old_pack_retired')::boolean,false) is not true
     or (select state from public.candidate_pending_expense_updates
         where update_id=v_update) is distinct from 'COMMITTED'
     or (select lower(coalesce(payment_scope_json->>'candidate_expense_update_hold','false'))
         from public.mail_outbox where id=v_mail) not in ('false','f','0','no')
     or (select scheduled_for_utc from public.mail_outbox where id=v_mail) is distinct from
       (select updated_at_utc from public.candidate_pending_expense_updates where update_id=v_update)
     or (select lower(coalesce(payment_scope_json->>'candidate_paper_generation_retired','false'))
         from public.mail_outbox where id=v_old_mail) not in ('true','t','1','yes')
     or (select count(*) from public.candidate_notifications notification
         where notification.workflow_id=v_workflow
           and notification.event_type='PAPER_PACK_READY')<>1 then
    raise exception 'PAPER replacement did not atomically retire/release/notify: %',v_result;
  end if;
  if (select result_json->>'contract_version'
      from public.candidate_expense_operations
      where operation_id=v_candidate_operation)
       is distinct from 'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1' then
    raise exception 'PAPER completion overwrote the original RESUBMIT receipt';
  end if;
  v_result:=public.candidate_expense_paper_update_receipt_commit_v1(
    'TEST',v_candidate_operation,v_update,jsonb_build_object(
      'contract_version','CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1',
      'ok',true,'operation_id',v_candidate_operation,
      'action_code','CREATE_UPDATED_DOCUMENTS','stage','COMPLETE',
      'workflow_id',v_workflow,'generation',2,
      'state','AWAITING_PAPER_RETURN','update_state','NONE','update_id',v_update,
      'category_changes',jsonb_build_array(jsonb_build_object(
        'update_kind','ADD_CATEGORY','expense_category','TRAVEL'
      )),'route','PAPER','route_selection_required',false,
      'upload_mode','EXISTING_WORKFLOW_DELTA','submission_requires_update_id',true,
      'approval_request_id',null,'approval_request_generation',null,
      'paper_pack_replacement',true,'old_pack_recoverable',false,
      'old_pack_retired',true,'manager_link_preserved',false,
      'previous_owning_timesheet_id',v_timesheet,'owning_timesheet_deleted',false,
      'deleted_timesheet_ids','[]'::jsonb,
      'retained_timesheet_ids',jsonb_build_array(v_timesheet),
      'affected_timesheet_ids',jsonb_build_array(v_timesheet),
      'expense_component_id',null,
      'paper_return_manifest_sha256',v_manifest_hex,
      'paper_return_page_count',1,'paper_return_manifest_version',2,
      'paper_return_qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
      'paper_pack_queued',true,'paper_pack',jsonb_build_object(
        'queued',true,'send_state','QUEUED','document_state','READY',
        'document_operation_id',null,'current_timesheet_id',v_timesheet,
        'timesheet_version',1,'recipient_available',true
      ),'paper_pack_email_bound',true,'paper_return_pages',v_manifest->'pages',
      'idempotent_replay',false
    ),now()
  );
  if coalesce(v_result->>'contract_version','')<>
       'CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1'
     or coalesce(v_result->>'stage','')<>'COMPLETE'
     or coalesce((v_result->>'idempotent_replay')::boolean,true) is not false
     or (select result_json->>'contract_version'
         from public.candidate_expense_operations
         where operation_id=v_candidate_operation)
          is distinct from 'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1'
     or (select submit_result_json->>'contract_version'
         from public.candidate_pending_expense_updates
         where update_id=v_update)
          is distinct from 'CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1' then
    raise exception 'PAPER RESUBMIT completion receipts were not separated: %',v_result;
  end if;
  v_result:=public.candidate_expense_paper_update_receipt_commit_v1(
    'TEST',v_candidate_operation,v_update,v_result,now()
  );
  if coalesce((v_result->>'idempotent_replay')::boolean,false) is not true then
    raise exception 'PAPER RESUBMIT COMPLETE receipt did not replay exactly: %',v_result;
  end if;
  v_result:=public.candidate_expense_category_resubmit_atomic_v1(
    v_session,'TEST',v_workflow,2,v_component,v_component_generation,
    'advanced-expense-paper-resubmit',now()
  );
  if coalesce(v_result->>'contract_version','')<>
       'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1'
     or coalesce(v_result->>'editor_mode','')<>'PAPER_REPLACEMENT'
     or v_result->'category_changes' is distinct from
       '[{"update_kind":"ADD_CATEGORY","expense_category":"TRAVEL"}]'::jsonb
     or coalesce((v_result->>'idempotent_replay')::boolean,false) is not true then
    raise exception 'Original PAPER RESUBMIT receipt was not replayable: %',v_result;
  end if;
  select coalesce(array_agg(claimed.id),array[]::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(100,'advanced-expense-paper-after-rebind',5) claimed;
  if not (v_mail=any(v_claimed)) then
    raise exception 'Committed PAPER replacement mail was not claimable after rebind';
  end if;

  -- A lost Office response while its PAPER category rejection is rendering
  -- must replay the complete server-bound preparation receipt.  It cannot
  -- collapse to a generic pending body that omits update_id/prepare intent.
  select component_generation into v_component_generation
  from public.candidate_expense_components
  where expense_component_id=v_component;
  insert into public.candidate_expense_operations(
    operation_id,environment,account_id,candidate_id,actor_kind,actor_id,
    action_code,workflow_id,timesheet_id,expense_component_id,request_sha256,
    idempotency_key,state,progress_json,created_at_utc,updated_at_utc
  ) values(
    v_office_operation,'TEST',v_account,v_candidate,'OFFICE',v_office_actor,
    'REJECT_EXPENSE_CATEGORY',v_workflow,v_timesheet,v_component,
    private._candidate_sha256_jsonb_v1(jsonb_build_object(
      'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_REQUEST_V1',
      'actor_user_id',v_office_actor,'environment','TEST',
      'workflow_id',v_workflow,'generation',2,
      'expense_component_id',v_component,
      'component_generation',v_component_generation,
      'confirmation_sha256',v_office_confirmation,
      'reason_note','Replay PAPER rejection'
    )),'advanced-expense-paper-office-replay','RENDERING',jsonb_build_object(
      'workflow_id',v_workflow,'generation',2,'update_id',v_update,
      'paper_prepare_required',true,'paper_pack_replacement',true
    ),now(),now()
  );
  perform private._candidate_office_service_context_open_v1(
    'TEST',v_office_actor,'reject_submission','REJECT_EXPENSE_CATEGORY',now()
  );
  v_result:=public.candidate_office_expense_category_reject_atomic_v1(
    v_office_actor,'TEST',v_workflow,2,v_component,v_component_generation,
    v_office_confirmation,'Replay PAPER rejection',
    'advanced-expense-paper-office-replay',now()
  );
  perform private._candidate_office_service_context_close_v1();
  if coalesce(v_result->>'contract_version','')<>
       'OFFICE_EXPENSE_CATEGORY_REJECTION_PENDING_V1'
     or coalesce(v_result->>'state','')<>'RENDERING'
     or nullif(v_result->>'update_id','')::uuid is distinct from v_update
     or coalesce((v_result->>'paper_prepare_required')::boolean,false) is not true
     or coalesce((v_result->>'idempotent_replay')::boolean,false) is not true then
    raise exception 'Office PAPER rejection replay lost its render authority: %',v_result;
  end if;

  -- Even if an external fault marks the new generation sent, retirement must
  -- fail closed; it may never claim that the recoverable old pack was restored.
  update public.mail_outbox set status='SENT',sent_at=now(),
    attempt_lease_token=null,attempt_leased_at_utc=null,
    attempt_lease_expires_at_utc=null where id=v_mail;
  begin
    perform private._candidate_paper_delivery_retire_v1(
      v_workflow,2,'PAPER_REPLACEMENT_ABORTED',now()
    );
    raise exception 'Retirement falsely accepted an already-sent replacement pack';
  exception when sqlstate '55000' then
    v_retire_blocked:=sqlerrm='CANDIDATE_PAPER_OUTBOX_ALREADY_SENT';
  end;
  if not v_retire_blocked then
    raise exception 'Already-sent replacement pack was not fenced from abort';
  end if;

  -- A persisted legacy/current manifest can never make the internal unsigned
  -- Expense Summary a return, signature, QR or emailed pack page.  The failed
  -- subtransaction restores the valid manifest and outbox fixture above.
  begin
    v_forbidden_manifest:=jsonb_set(
      v_manifest,'{pages}',v_manifest->'pages'||jsonb_build_array(jsonb_build_object(
        'page_key','EXPENSE_SUMMARY','component_kind','EXPENSE_SUMMARY',
        'expense_category','OTHER','ordinal',2,'display_name','Expense summary',
        'category_occurrence',1,'page_kind_code','S','category_code','O',
        'page_key_sha256_16',substring(encode(extensions.digest(
          'EXPENSE_SUMMARY','sha256'),'hex') from 1 for 16),'qr_required',true
      )),true
    );
    update public.candidate_submission_workflows set
      paper_return_manifest_json=v_forbidden_manifest,
      paper_return_manifest_sha256=private._candidate_sha256_jsonb_v1(v_forbidden_manifest)
    where id=v_workflow;
    perform public.candidate_workflow_transition_atomic_v1(
      null,'TEST',v_workflow,'PAPER_PACK_RELEASE',2,
      jsonb_build_object('service_paper_pack_release',true),
      'advanced-expense-paper-summary-forbidden',now()
    );
    raise exception 'Persisted PAPER manifest exposed the internal Expense Summary';
  exception when sqlstate '55000' then
    v_forbidden_manifest_blocked:=sqlerrm='CANDIDATE_PAPER_RETURN_MANIFEST_STALE';
  end;
  if not v_forbidden_manifest_blocked then
    raise exception 'Persisted PAPER Expense Summary was not rejected';
  end if;

  -- A legacy pack made only from the internal summary cannot be promoted to
  -- an empty/null V2 pack.  The failed subtransaction restores the committed
  -- replacement manifest.
  v_summary_only_manifest:=jsonb_build_object(
    'manifest_version',1,'workflow_id',v_workflow,'workflow_generation',2,
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','EXPENSE_SUMMARY','component_kind','EXPENSE_SUMMARY',
      'expense_category','OTHER'
    ))
  );
  begin
    update public.candidate_submission_workflows set
      paper_return_manifest_json=v_summary_only_manifest,
      paper_return_manifest_sha256=private._candidate_sha256_jsonb_v1(v_summary_only_manifest)
    where id=v_workflow;
    perform public.candidate_paper_manifest_v2_promote_v1(
      v_session,'TEST',v_workflow,2,
      encode(private._candidate_sha256_jsonb_v1(v_summary_only_manifest),'hex'),now()
    );
    raise exception 'Summary-only legacy PAPER manifest promoted to V2';
  exception when sqlstate '40001' then
    v_summary_only_blocked:=sqlerrm='CANDIDATE_PAPER_RETURN_MANIFEST_STALE';
  end;
  if not v_summary_only_blocked then
    raise exception 'Summary-only legacy PAPER manifest was not rejected';
  end if;

  -- The workflow-to-component synchroniser must preserve the same
  -- authorised_at_server-only protection truth as the migration backfill and
  -- financial/Timesheet triggers.  This deliberately resets only the derived
  -- fixture value before invoking the canonical workflow synchroniser.
  update public.timesheets set authorised_at_server=now()
  where timesheet_id=v_timesheet;
  update public.candidate_expense_components set
    agency_authorisation_state='NOT_AUTHORISED'
  where expense_component_id=v_component;
  perform private._candidate_expense_components_sync_v1(v_workflow,now());
  if (select agency_authorisation_state
      from public.candidate_expense_components
      where expense_component_id=v_component) is distinct from 'AUTHORISED' then
    raise exception 'authorised_at_server-only protection was not synchronised';
  end if;
end;
$paper_replacement_hold$;

do $manager_update_hold$
declare
  v_candidate uuid:=pg_catalog.gen_random_uuid();
  v_client uuid:=pg_catalog.gen_random_uuid();
  v_contract uuid:=pg_catalog.gen_random_uuid();
  v_timesheet uuid:=pg_catalog.gen_random_uuid();
  v_week uuid:=pg_catalog.gen_random_uuid();
  v_account uuid:=pg_catalog.gen_random_uuid();
  v_session uuid:=pg_catalog.gen_random_uuid();
  v_workflow uuid:=pg_catalog.gen_random_uuid();
  v_component uuid:=pg_catalog.gen_random_uuid();
  v_approval uuid:=pg_catalog.gen_random_uuid();
  v_token_sha text:=repeat('c7',32);
  v_manifest jsonb;
  v_manifest_sha bytea;
  v_begin jsonb;
  v_hold jsonb;
  v_update uuid;
  v_now timestamptz:=clock_timestamp();
begin
  v_manifest:=jsonb_build_object(
    'required_components',jsonb_build_array(jsonb_build_object(
      'component_id',v_component,'component_kind','EXPENSE_EVIDENCE',
      'expense_category','TRAVEL'
    ))
  );
  v_manifest_sha:=private._candidate_sha256_jsonb_v1(v_manifest);
  insert into public.candidates(id,email,active,key_norm,display_name)
  values(
    v_candidate,'manager-hold-'||v_candidate::text||'@example.test',true,
    'GCK-MANAGER-HOLD-'||replace(v_candidate::text,'-',''),'Manager Hold Candidate'
  );
  insert into public.clients(id,name) values(v_client,'Manager hold verification client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    week_ending_weekday,candidate_expenses_require_separate_timesheet
  ) values(
    pg_catalog.gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer,true
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,is_adjustment,adjustment_origin,r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,'ADVANCED_MANAGER_HOLD_'||replace(v_timesheet::text,'-',''),
    'GCK-MANAGER-HOLD-'||replace(v_candidate::text,'-',''),'VERIFICATION HOSPITAL',
    'VERIFICATION WARD','NURSE',v_contract,current_date,'WEEKLY','EXPENSES',
    'ELECTRONIC',true,'CANDIDATE_EXPENSE_ONLY',
    'verification/manager-hold/candidate','verification/manager-hold/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,is_adjustment,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,901,true,'SUBMITTED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    travel_pay_ex_vat,travel_charge_ex_vat,expenses_pay_ex_vat,
    expenses_charge_ex_vat,total_pay_ex_vat,total_charge_ex_vat,processing_status
  ) values(v_timesheet,1,v_candidate,v_client,0,10,10,10,10,10,10,'PENDING_AUTH');
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','manager-hold-'||v_candidate::text||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('c8',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,policy_snapshot_json,input_snapshot_json,idempotency_key,
    immutable_submission_json,immutable_submission_sha256,
    review_manifest_json,review_manifest_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'AWAITING_MANAGER_APPROVAL',1,v_contract,v_week,v_timesheet,v_timesheet,
    current_date,'{}','{}','advanced-expense:manager-hold',
    jsonb_build_object('expense_submission',jsonb_build_object(
      'canonical_tsfin_snapshot',jsonb_build_object(
        'travel_pay_ex_vat',10,'travel_charge_ex_vat',10,
        'expenses_pay_ex_vat',10,'expenses_charge_ex_vat',10
      )
    )),private._candidate_sha256_jsonb_v1(jsonb_build_object(
      'expense_submission',jsonb_build_object(
        'canonical_tsfin_snapshot',jsonb_build_object(
          'travel_pay_ex_vat',10,'travel_charge_ex_vat',10,
          'expenses_pay_ex_vat',10,'expenses_charge_ex_vat',10
        )
      )
    )),v_manifest,v_manifest_sha
  );
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    expense_category,document_role,state,storage_key,media_type,byte_size,
    source_content_sha256,immutable_at_utc,required,review_render_state,
    final_signed_render_state
  ) values(
    v_component,v_workflow,1,1,v_timesheet,'EXPENSE_EVIDENCE','TRAVEL',
    'SOURCE_EVIDENCE','IMMUTABLE','verification/manager-hold/travel.jpg',
    'image/jpeg',100,decode(repeat('c9',32),'hex'),v_now,false,
    'NOT_REQUIRED','NOT_REQUIRED'
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,token_hash,
    expires_at_utc,review_manifest_sha256,required_component_ids,
    required_component_manifest_json
  ) values(
    v_approval,v_workflow,1,1,'PHONE','PENDING',decode(v_token_sha,'hex'),
    v_now+interval '1 hour',v_manifest_sha,array[v_component],
    v_manifest->'required_components'
  );

  v_begin:=public.candidate_expense_update_begin_atomic_v1(
    v_session,'TEST',v_workflow,1,
    '[{"update_kind":"ADD_CATEGORY","expense_category":"OTHER"}]'::jsonb,
    'advanced-expense:manager-hold-begin',v_now
  );
  v_update:=nullif(v_begin->>'update_id','')::uuid;
  v_hold:=public.candidate_expense_update_manager_hold_v1(
    'TEST',v_workflow,v_token_sha,v_now+interval '1 second'
  );
  if coalesce(v_hold->>'state','')<>'UPDATING'
     or coalesce(v_hold->>'status_code','')<>'MANAGER_APPROVAL_REQUEST_UPDATING'
     or coalesce((v_hold->>'retry_after_seconds')::integer,0)<>2
     or nullif(v_hold->>'workflow_id','')::uuid is distinct from v_workflow
     or nullif(v_hold->>'approval_request_id','')::uuid is distinct from v_approval
     or coalesce((v_hold->>'approval_request_generation')::integer,0)<>1 then
    raise exception 'Manager link did not receive its exact active update hold: %',v_hold;
  end if;

  v_hold:=public.candidate_expense_update_manager_hold_v1(
    'TEST',v_workflow,v_token_sha,v_now+interval '31 minutes'
  );
  if v_hold is not null
     or (select state from public.candidate_pending_expense_updates
         where update_id=v_update) is distinct from 'FAILED'
     or (select failure_code from public.candidate_pending_expense_updates
         where update_id=v_update) is distinct from 'CANDIDATE_EXPENSE_UPDATE_EXPIRED'
     or (select state from public.candidate_submission_workflows
         where id=v_workflow) is distinct from 'AWAITING_MANAGER_APPROVAL'
     or (select generation from public.candidate_submission_workflows
         where id=v_workflow) is distinct from 1
     or not exists(
       select 1 from public.candidate_approval_requests request
       where request.id=v_approval and request.workflow_id=v_workflow
         and request.workflow_generation=1 and request.state='PENDING'
         and request.token_hash=decode(v_token_sha,'hex')
     ) then
    raise exception 'Expired manager hold did not atomically restore the same link';
  end if;
  if public.candidate_expense_update_manager_hold_v1(
       'TEST',v_workflow,v_token_sha,v_now+interval '32 minutes'
     ) is not null then
    raise exception 'Terminal update continued to hold the restored manager link';
  end if;
end;
$manager_update_hold$;

rollback;
