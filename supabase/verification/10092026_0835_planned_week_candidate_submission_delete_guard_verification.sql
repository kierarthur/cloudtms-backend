\set ON_ERROR_STOP on

-- A Candidate submission can exist before its Timesheet is materialised. Office
-- must reject that exact submission before the planned Contract Week can be
-- permanently deleted. Every change below is rollback-contained.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_reads":true,"candidate_app_writes":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

do $verification$
declare
  v_actor uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_guard jsonb;
  v_result jsonb;
  v_delete_operation uuid:=gen_random_uuid();
begin
  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(
    v_actor,'planned-week-delete-'||v_actor::text||'@example.test',
    'UNUSABLE_VERIFICATION_ONLY','admin',true
  );
  update public.settings_defaults
  set candidate_app_system_actor_user_id=v_actor
  where id=1;

  insert into public.clients(id,name)
  values(v_client,'Planned Week Candidate Delete Verification Client');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(
    gen_random_uuid(),v_client,current_date-7,'ELECTRONIC',
    extract(dow from current_date)::integer
  );
  insert into public.candidates(id,email,active)
  values(
    v_candidate,'planned-week-delete-'||v_candidate::text||'@example.test',true
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-14,current_date+14,'PAYE',
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,
    day_entries_json,totals_json,timesheet_id
  ) values(
    v_week,v_contract,current_date,'OPEN','ELECTRONIC',
    '[{"date":"2026-09-10","hours":8}]'::jsonb,'{"total_hours":8}'::jsonb,null
  );
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status,notification_preferences_json
  ) values(
    v_account,'TEST','planned-week-delete-'||v_candidate::text||'@example.test',
    'ACTIVE','{}'::jsonb
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,
    generation,contract_id,contract_week_id,anchor_timesheet_id,
    target_timesheet_id,week_ending_date,policy_snapshot_json,input_snapshot_json,
    idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY',
    'ELECTRONIC','READY_FOR_MANAGER_APPROVAL',1,v_contract,v_week,null,
    null,current_date,'{}','{}','planned-week-delete-guard'
  );

  v_guard:=public.contract_week_submission_delete_guard_preview_v1('TEST',v_week);
  if v_guard->>'contract_version'<>'CONTRACT_WEEK_SUBMISSION_DELETE_GUARD_V1'
     or coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) is not true
     or v_guard->>'candidate_submission_stage'<>'CANDIDATE_SUBMITTED'
     or coalesce((v_guard->>'guarded_workflow_count')::integer,0)<>1
     or v_guard#>>'{guarded_workflows,0,workflow_id}'<>v_workflow::text
     or length(v_guard->>'context_sha256')<>64 then
    raise exception 'Planned-week Candidate delete guard was incomplete: %',v_guard;
  end if;

  begin
    perform public.contract_week_delete_planned_guarded_v1(
      'TEST',v_week,v_actor,v_guard->>'context_sha256',v_delete_operation,now()
    );
    raise exception 'Submitted planned week was deleted without rejection';
  exception
    when sqlstate '55000' then
      if sqlerrm not like '%CANDIDATE_SUBMISSION_REJECTION_REQUIRED%' then raise; end if;
  end;
  if not exists(select 1 from public.contract_weeks where id=v_week)
     or (select state from public.candidate_submission_workflows where id=v_workflow)
        <>'READY_FOR_MANAGER_APPROVAL' then
    raise exception 'Rejected delete attempt changed the planned week or workflow';
  end if;

  v_result:=public.contract_week_submission_reject_atomic_v1(
    v_actor,'TEST',v_week,v_guard->>'context_sha256',
    'Verification correction required','planned-week-delete:reject',now()
  );
  if coalesce((v_result->>'candidate_submission_rejected')::boolean,false) is not true
     or coalesce((v_result->>'candidate_must_start_new_claim')::boolean,false) is not true
     or (select state from public.candidate_submission_workflows where id=v_workflow)<>'REJECTED'
     or (select status from public.contract_weeks where id=v_week)<>'OPEN'
     or (select day_entries_json from public.contract_weeks where id=v_week)<>'[]'::jsonb
     or (select totals_json from public.contract_weeks where id=v_week)<>'{}'::jsonb
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_workflow
           and event_type='OFFICE_REJECTED'
           and template_key='candidate-office-rejected-v1'
           and deep_link_json->>'type'='workflow')<>1 then
    raise exception 'Planned-week Candidate rejection did not reset and notify exactly: %',v_result;
  end if;

  v_guard:=public.contract_week_submission_delete_guard_preview_v1('TEST',v_week);
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,true)
     or coalesce((v_guard->>'guarded_workflow_count')::integer,-1)<>0 then
    raise exception 'Rejected planned-week workflow still blocked deletion: %',v_guard;
  end if;

  v_result:=public.contract_week_delete_planned_guarded_v1(
    'TEST',v_week,v_actor,v_guard->>'context_sha256',v_delete_operation,now()
  );
  if coalesce((v_result->>'deleted')::boolean,false) is not true
     or exists(select 1 from public.contract_weeks where id=v_week)
     or (select count(*) from public.candidate_submission_workflows
         where id=v_workflow and state='REJECTED'
           and contract_week_id is null and anchor_timesheet_id is null
           and target_timesheet_id is null
           and issue_codes @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb)<>1
     or (select count(*) from public.candidate_notifications
         where workflow_id=v_workflow and timesheet_id is null
           and deep_link_json->>'type'='workflow'
           and deep_link_json->>'workflow_id'=v_workflow::text)<>1 then
    raise exception 'Rejected planned week did not delete cleanly: %',v_result;
  end if;
end;
$verification$;

do $verification$
begin
  if has_function_privilege(
       'anon','public.contract_week_submission_delete_guard_preview_v1(text,uuid)','EXECUTE'
     ) or has_function_privilege(
       'authenticated','public.contract_week_submission_delete_guard_preview_v1(text,uuid)','EXECUTE'
     ) or has_function_privilege(
       'anon','public.contract_week_submission_reject_atomic_v1(uuid,text,uuid,text,text,text,timestamptz)','EXECUTE'
     ) or has_function_privilege(
       'authenticated','public.contract_week_submission_reject_atomic_v1(uuid,text,uuid,text,text,text,timestamptz)','EXECUTE'
     ) or has_function_privilege(
       'anon','public.contract_week_delete_planned_guarded_v1(text,uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     ) or has_function_privilege(
       'authenticated','public.contract_week_delete_planned_guarded_v1(text,uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Browser roles can execute planned-week Candidate delete authority';
  end if;
  if not has_function_privilege(
       'service_role','public.contract_week_submission_delete_guard_preview_v1(text,uuid)','EXECUTE'
     ) or not has_function_privilege(
       'service_role','public.contract_week_submission_reject_atomic_v1(uuid,text,uuid,text,text,text,timestamptz)','EXECUTE'
     ) or not has_function_privilege(
       'service_role','public.contract_week_delete_planned_guarded_v1(text,uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Service role cannot execute planned-week Candidate delete authority';
  end if;
end;
$verification$;

rollback;
