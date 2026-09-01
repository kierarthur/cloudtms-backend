\set ON_ERROR_STOP on

-- A Candidate may declare a genuinely empty planned week as not worked.
-- Once hours, evidence or an active submission exist, the same action must
-- fail without changing the Timesheet or Contract Week. All fixture work is
-- rollback-contained.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json||jsonb_build_object(
      'candidate_app_reads',true,
      'candidate_app_writes',true,
      'candidate_record_role_capabilities',true
    ),
    candidate_app_environment='TEST'
where id=1;

do $verification$
declare
  v_now timestamptz:=clock_timestamp();
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_worked_timesheet uuid:=gen_random_uuid();
  v_worked_week uuid:=gen_random_uuid();
  v_empty_week uuid:=gen_random_uuid();
  v_capabilities jsonb;
  v_actions jsonb;
  v_result jsonb;
begin
  if exists(
    select 1 from information_schema.columns
    where table_schema='public' and table_name='tms_users' and column_name='password_hash'
  ) then
    execute $sql$
      insert into public.tms_users(id,email,password_hash,role,is_active)
      values($1,$2,'UNUSABLE_VERIFICATION_ONLY','admin',true)
    $sql$ using v_actor,'no-work-actor-'||replace(v_actor::text,'-','')||'@example.test';
  else
    insert into public.tms_users(id,email,is_active)
    values(v_actor,'no-work-actor-'||replace(v_actor::text,'-','')||'@example.test',true);
  end if;
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;

  insert into public.candidates(id,email,active)
  values(v_candidate,'no-work-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Candidate no-work verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-30,'ELECTRONIC',extract(dow from current_date)::integer
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    pay_method_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'PAYE','ELECTRONIC'
  );
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','no-work-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    v_now+interval '1 day',v_now+interval '7 days'
  );

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,occupant_key_norm,hospital_norm,
    ward_norm,job_title_norm,week_ending_date,line_type,sheet_scope,
    submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_worked_timesheet,v_contract,
    'NO-WORK-WORKED-'||replace(v_worked_timesheet::text,'-',''),
    'GCK-NO-WORK-'||replace(v_candidate::text,'-',''),
    'VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE',
    current_date,'HOURS','WEEKLY','ELECTRONIC',
    'verification/no-work/candidate','verification/no-work/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_worked_week,v_contract,current_date,0,'OPEN','ELECTRONIC',v_worked_timesheet
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    processing_status,total_hours
  ) values(
    v_worked_timesheet,1,v_candidate,v_client,true,'CONTRACT_WEEKLY','UNPROCESSED',8
  );

  v_capabilities:=private._candidate_record_capabilities_v1(
    v_worked_timesheet,v_worked_week,'{}'::jsonb
  );
  if coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,true)
     or coalesce((v_capabilities->>'hours_value')::numeric,0)<>8 then
    raise exception 'CANDIDATE_WORKED_WEEK_NO_WORK_CAPABILITY_INVALID: %',v_capabilities;
  end if;
  v_actions:=private._candidate_timesheet_action_contract_v1(
    'OPEN','[]'::jsonb,v_capabilities,v_worked_timesheet,v_worked_week,v_now
  );
  if exists(
    select 1 from jsonb_array_elements(coalesce(v_actions->'available_actions','[]'::jsonb)) action
    where action->>'code'='NO_WORK_THIS_WEEK'
  ) then
    raise exception 'CANDIDATE_WORKED_WEEK_NO_WORK_ACTION_EXPOSED: %',v_actions;
  end if;
  begin
    perform public.candidate_no_work_atomic_v1(
      v_session,'TEST',v_worked_week,null,
      'no-work-worked:'||v_worked_week::text,v_now
    );
    raise exception 'CANDIDATE_WORKED_WEEK_NO_WORK_ACCEPTED';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_NO_WORK_NOT_ALLOWED' then raise; end if;
  end;
  if not exists(select 1 from public.contract_weeks where id=v_worked_week)
     or not exists(select 1 from public.timesheets where timesheet_id=v_worked_timesheet)
     or coalesce((select total_hours from public.timesheets_financials
        where timesheet_id=v_worked_timesheet and is_current=true),0)<>8 then
    raise exception 'CANDIDATE_WORKED_WEEK_CHANGED_AFTER_REJECTION';
  end if;

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot
  ) values(v_empty_week,v_contract,current_date-7,0,'PLANNED','ELECTRONIC');
  v_capabilities:=private._candidate_record_capabilities_v1(null,v_empty_week,'{}'::jsonb);
  if not coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    raise exception 'CANDIDATE_EMPTY_WEEK_NO_WORK_CAPABILITY_MISSING: %',v_capabilities;
  end if;
  v_result:=public.candidate_no_work_atomic_v1(
    v_session,'TEST',v_empty_week,null,
    'no-work-empty:'||v_empty_week::text,v_now
  );
  if not coalesce((v_result->>'ok')::boolean,false)
     or exists(select 1 from public.contract_weeks where id=v_empty_week) then
    raise exception 'CANDIDATE_EMPTY_WEEK_NO_WORK_FAILED: %',v_result;
  end if;
end;
$verification$;

rollback;
