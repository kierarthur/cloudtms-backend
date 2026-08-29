-- Rollback-contained first-use proof for Candidate weekly withdrawal reset.
begin;

do $candidate_weekly_withdrawal_reset_verification$
declare
  v_now timestamptz:='2026-08-27 13:15:00+00';
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_result jsonb;
  v_system_actor uuid;
begin
  select candidate_app_system_actor_user_id into v_system_actor
  from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_WEEKLY_WITHDRAWAL_SYSTEM_ACTOR_MISSING';
  end if;
  insert into public.clients(id,name)
  values(v_client,'Candidate withdrawal verification client');
  insert into public.candidates(id,email,active,key_norm)
  values(
    v_candidate,
    'withdrawal-'||replace(v_candidate::text,'-','')||'@example.test',
    true,
    'WITHDRAWAL-'||replace(v_candidate::text,'-','')
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC','PAYE'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    version,is_current,status,contract_id,week_ending_date,line_type,sheet_scope,
    submission_mode
  ) values(
    v_timesheet,'WEEKLY-WITHDRAWAL-'||v_timesheet::text,
    'WITHDRAWAL-VERIFICATION','VERIFICATION-HOSPITAL','VERIFICATION-WARD',
    'VERIFICATION-JOB',1,true,'RECEIVED',v_contract,current_date,
    'HOURS','WEEKLY','MANUAL'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
    day_entries_json,totals_json
  ) values(
    v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',v_timesheet,
    jsonb_build_array(jsonb_build_object('day','MON','hours',8)),
    jsonb_build_object('total_hours',8)
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    hours_day,processing_status
  ) values(v_timesheet,1,v_candidate,v_client,8,8,'UNPROCESSED');
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST',
    'withdrawal-'||replace(v_account::text,'-','')||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,idempotency_key,review_manifest_sha256
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL',
    'CANCELLED',2,v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'withdrawal-verification-'||v_workflow::text,
    extensions.digest('withdrawal-verification-'||v_workflow::text,'sha256')
  );

  v_result:=private._candidate_weekly_withdrawal_reset_v1(
    v_workflow,'I entered the wrong week.',v_now
  );
  if not coalesce((v_result->>'reset')::boolean,false)
     or v_result->>'current_timesheet_id' is not null
     or v_result->>'draft_submission_mode' is not null
     or v_result->>'effective_submission_mode'<>'ELECTRONIC'
     or (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or (select status from public.timesheets where timesheet_id=v_timesheet)<>'REVOKED'
     or (select timesheet_id from public.contract_weeks where id=v_week) is not null
     or (select status from public.contract_weeks where id=v_week)<>'OPEN'
     or (select submission_mode_snapshot from public.contract_weeks where id=v_week)<>'ELECTRONIC'
     or (select day_entries_json from public.contract_weeks where id=v_week)<>'[]'::jsonb
     or (select totals_json from public.contract_weeks where id=v_week)<>'{}'::jsonb
     or (select total_hours from public.timesheets_financials where timesheet_id=v_timesheet)<>8
     or not exists(
       select 1 from public.audit_events
       where object_type='timesheet'
         and object_id_text=v_timesheet::text
         and action='CANDIDATE_SUBMISSION_WITHDRAWN_TO_CONTRACT_WEEK'
         and actor_user_id=v_system_actor
     ) then
    raise exception 'CANDIDATE_WEEKLY_WITHDRAWAL_RESET_VERIFICATION_FAILED: %',v_result;
  end if;
end;
$candidate_weekly_withdrawal_reset_verification$;

rollback;
