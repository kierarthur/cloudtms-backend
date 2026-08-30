\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the weekly PAPER/QR adapter.  A
-- submitted Candidate workflow may have no Timesheet after withdrawal.  The
-- adapter must create exactly one unapproved current Timesheet/TSFIN through
-- the existing weekly canonical writer, link the workflow, replay safely, and
-- remain inaccessible to browser roles.
begin;

do $candidate_weekly_paper_target_prepare_verification$
declare
  v_now timestamptz := '2026-08-30 06:05:00+00';
  v_client uuid := gen_random_uuid();
  v_candidate uuid := gen_random_uuid();
  v_contract uuid := gen_random_uuid();
  v_week uuid := gen_random_uuid();
  v_account uuid := gen_random_uuid();
  v_session uuid := gen_random_uuid();
  v_workflow uuid := gen_random_uuid();
  v_timesheet uuid := gen_random_uuid();
  v_email text;
  v_schedule jsonb;
  v_create jsonb;
  v_patch jsonb;
  v_week_patch jsonb;
  v_snapshot jsonb;
  v_submission jsonb;
  v_first jsonb;
  v_replay jsonb;
  v_stale_failed boolean := false;
begin
  v_email := 'paper-target-'||replace(v_candidate::text,'-','')||'@example.test';
  v_schedule := jsonb_build_array(jsonb_build_object(
    'day','MON','start','09:00','end','17:00','break_minutes',0,'hours',8
  ));
  v_create := jsonb_build_object(
    'timesheet_id',v_timesheet,
    'booking_id','CANDIDATE-WEEKLY-'||replace(v_week::text,'-',''),
    'contract_id',v_contract,
    'week_ending_date',current_date,
    'occupant_key_norm','PAPER-TARGET-'||replace(v_candidate::text,'-',''),
    'hospital_norm','Paper target verification client',
    'ward_norm','',
    'job_title_norm','RMN',
    'shift_label_norm','weekly',
    'status','RECEIVED',
    'submission_mode','MANUAL',
    'line_type','HOURS',
    'sheet_scope','WEEKLY',
    'actual_schedule_json',v_schedule,
    'additional_units_week','{}'::jsonb,
    'additional_units_per_day','{}'::jsonb,
    'candidate_hint_text',jsonb_build_object('display_name','Paper Target Candidate')
  );
  v_patch := jsonb_build_object(
    'actual_schedule_json',v_schedule,
    'additional_units_week','{}'::jsonb,
    'additional_units_per_day','{}'::jsonb
  );
  v_week_patch := jsonb_build_object(
    'totals_json',jsonb_build_object('total_hours',8,'actual_schedule_json',v_schedule),
    'planned_schedule_json',v_schedule
  );
  v_snapshot := jsonb_build_object(
    'timesheet_id',v_timesheet,
    'timesheet_version',1,
    'candidate_id',v_candidate,
    'client_id',v_client,
    'basis','SELF_REPORTED',
    'candidate_assignment','ASSIGNED',
    'processing_status','PENDING_AUTH',
    'total_hours',8,
    'hours_day',8,
    'hours_night',0,
    'hours_sat',0,
    'hours_sun',0,
    'hours_bh',0,
    'invoice_breakdown_json','{}'::jsonb,
    'additional_units_json','{}'::jsonb,
    'mileage_units',0,
    'mileage_pay_ex_vat',0,
    'mileage_charge_ex_vat',0,
    'expenses_pay_ex_vat',0,
    'expenses_charge_ex_vat',0,
    'travel_pay_ex_vat',0,
    'travel_charge_ex_vat',0,
    'accommodation_pay_ex_vat',0,
    'accommodation_charge_ex_vat',0,
    'other_pay_ex_vat',0,
    'other_charge_ex_vat',0
  );
  v_submission := jsonb_build_object(
    'contract_version','CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1',
    'timesheet_create_json',v_create,
    'timesheet_patch_json',v_patch,
    'contract_week_patch_json',v_week_patch,
    'canonical_tsfin_snapshot',v_snapshot
  );

  update public.settings_defaults
  set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_paper_qr',true
  )
  where id=1;

  insert into public.clients(id,name)
  values(v_client,'Paper target verification client');
  insert into public.candidates(id,email,display_name,active,key_norm)
  values(v_candidate,v_email,'Paper Target Candidate',true,
    'PAPER-TARGET-'||replace(v_candidate::text,'-',''));
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role,
    candidate_paper_submission_enabled_override
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,0,
    'ELECTRONIC','PAYE','RMN',true
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
    day_entries_json,totals_json
  ) values(
    v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',null,
    v_schedule,jsonb_build_object('total_hours',8,'actual_schedule_json',v_schedule)
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,password_scheme,password_scheme_version,
    password_salt,password_digest,password_changed_at_utc
  ) values(
    v_account,'TEST',v_email,'ACTIVE','PBKDF2-HMAC-SHA256',1,
    decode(repeat('41',16),'hex'),decode(repeat('42',32),'hex'),v_now
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(v_session::text,'sha256'),v_now+interval '1 hour',v_now+interval '2 hours'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,review_manifest_sha256,
    immutable_submission_json,immutable_submission_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PAPER',
    'WORKER_SUBMITTED',1,v_contract,v_week,null,null,current_date,
    'paper-target-verification-'||v_workflow::text,
    extensions.digest('paper-target-verification-'||v_workflow::text,'sha256'),
    v_submission,private._candidate_sha256_jsonb_v1(v_submission)
  );
  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,component_kind,document_role,state,
    required,review_ordinal,review_render_state,final_signed_render_state
  ) values(
    v_workflow,1,1,'HOURS_TIMESHEET','ELECTRONIC_TIMESHEET_MANAGER_REVIEW','PENDING',
    true,1,'PENDING','PENDING'
  );

  v_first:=public.candidate_weekly_paper_target_prepare_v1(
    v_session,'TEST',v_workflow,1,v_now
  );
  v_replay:=public.candidate_weekly_paper_target_prepare_v1(
    v_session,'TEST',v_workflow,1,v_now+interval '1 second'
  );

  if v_first->>'timesheet_id' is distinct from v_timesheet::text
     or v_first->>'created' is distinct from 'true'
     or v_replay->>'timesheet_id' is distinct from v_timesheet::text
     or v_replay->>'idempotent_replay' is distinct from 'true'
     or (select timesheet_id from public.contract_weeks where id=v_week) is distinct from v_timesheet
     or (select target_timesheet_id from public.candidate_submission_workflows where id=v_workflow) is distinct from v_timesheet
     or (select timesheet_id from public.candidate_submission_components where workflow_id=v_workflow and component_no=1) is distinct from v_timesheet
     or (select count(*) from public.timesheets where contract_id=v_contract and week_ending_date=current_date and is_current)<>1
     or not exists(
       select 1 from public.timesheets
       where timesheet_id=v_timesheet and is_current and archived_at_utc is null
         and sheet_scope='WEEKLY' and submission_mode='MANUAL'
         and authorised_at_server is null
     )
     or (select count(*) from public.timesheets_financials where timesheet_id=v_timesheet and is_current)<>1
     or not exists(
       select 1 from public.timesheets_financials
       where timesheet_id=v_timesheet and is_current
         and authorised_at_utc is null and paid_at_utc is null
         and locked_by_invoice_id is null
     ) then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_FIRST_USE_FAILED: %, %',v_first,v_replay;
  end if;

  begin
    perform public.candidate_weekly_paper_target_prepare_v1(
      v_session,'TEST',v_workflow,2,v_now+interval '2 seconds'
    );
  exception when sqlstate '40001' then
    v_stale_failed:=true;
  end;
  if not v_stale_failed then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_STALE_GENERATION_ACCEPTED';
  end if;

  if pg_catalog.has_function_privilege('anon',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE')
     or not pg_catalog.has_function_privilege('service_role',
       'public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_WEEKLY_PAPER_TARGET_ACL_INVALID';
  end if;
end;
$candidate_weekly_paper_target_prepare_verification$;

rollback;
