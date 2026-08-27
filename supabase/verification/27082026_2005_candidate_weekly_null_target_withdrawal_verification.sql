-- Rollback-contained first-use proof for a refused planned weekly workflow
-- which has no canonical Timesheet row yet.
begin;

do $candidate_weekly_null_target_withdrawal_verification$
declare
  v_now timestamptz:='2026-08-27 20:05:00+00';
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_result jsonb;
  v_system_actor uuid;
begin
  select candidate_app_system_actor_user_id into v_system_actor
  from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_WEEKLY_NULL_TARGET_SYSTEM_ACTOR_MISSING';
  end if;

  insert into public.clients(id,name)
  values(v_client,'Candidate null-target withdrawal verification client');
  insert into public.candidates(id,email,active,key_norm)
  values(
    v_candidate,
    'null-target-'||replace(v_candidate::text,'-','')||'@example.test',
    true,
    'NULL-TARGET-'||replace(v_candidate::text,'-','')
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC','PAYE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
    day_entries_json,totals_json
  ) values(
    v_week,v_contract,current_date,'SUBMITTED','ELECTRONIC',null,
    jsonb_build_array(jsonb_build_object('day','MON','hours',8)),
    jsonb_build_object('total_hours',8)
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST',
    'null-target-'||replace(v_account::text,'-','')||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,idempotency_key,review_manifest_sha256,
    rejection_reason,rejection_scope
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','PHONE',
    'REFUSED',2,v_contract,v_week,null,null,current_date,
    'null-target-withdrawal-verification-'||v_workflow::text,
    extensions.digest('null-target-withdrawal-verification-'||v_workflow::text,'sha256'),
    'Hours need correction.','COMPLETE_ELECTRONIC_TRANSACTION'
  );

  v_result:=private._candidate_weekly_withdrawal_reset_v1(
    v_workflow,'Reset the refused submission.',v_now
  );
  if not coalesce((v_result->>'reset')::boolean,false)
     or v_result->>'old_timesheet_id' is not null
     or v_result->>'current_timesheet_id' is not null
     or v_result->>'contract_week_id'<>v_week::text
     or v_result->>'effective_submission_mode'<>'ELECTRONIC'
     or (select timesheet_id from public.contract_weeks where id=v_week) is not null
     or (select status from public.contract_weeks where id=v_week)<>'OPEN'
     or (select day_entries_json from public.contract_weeks where id=v_week)<>'[]'::jsonb
     or (select totals_json from public.contract_weeks where id=v_week)<>'{}'::jsonb
     or not exists(
       select 1 from public.audit_events
       where object_type='contract_week'
         and object_id_text=v_week::text
         and action='CANDIDATE_SUBMISSION_WITHDRAWN_TO_CONTRACT_WEEK'
         and actor_user_id=v_system_actor
     ) then
    raise exception 'CANDIDATE_WEEKLY_NULL_TARGET_WITHDRAWAL_VERIFICATION_FAILED: %',v_result;
  end if;
end;
$candidate_weekly_null_target_withdrawal_verification$;

rollback;
