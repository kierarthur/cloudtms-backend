\set ON_ERROR_STOP on

-- Rollback-contained proof for a reused expense-only carrier. Two worked rows
-- exist for the same contract/week, so the generic fallback is ambiguous. The
-- carrier's one immutable historical anchor must be reused; contradictory
-- historical anchors must remain blocked.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  || jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_expense_atomic_placement',true,
    'candidate_manager_approval',true,
    'candidate_paper_qr',true
  )
where id=1;

do $history_anchor$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_anchor_one_week uuid:=gen_random_uuid();
  v_anchor_two_week uuid:=gen_random_uuid();
  v_anchor_one_timesheet uuid:=gen_random_uuid();
  v_anchor_two_timesheet uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_first_workflow uuid:=gen_random_uuid();
  v_recovered_workflow uuid:=gen_random_uuid();
  v_conflicting_workflow uuid:=gen_random_uuid();
  v_refused_workflow uuid:=gen_random_uuid();
  v_carrier_week uuid;
  v_email text:='history-anchor-'||gen_random_uuid()::text||'@example.test';
  v_carrier jsonb;
  v_result jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate history anchor runtime client');

  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,v_email,true,'GCK-HISTORY-'||replace(v_candidate::text,'-',''));

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
    pay_method_snapshot,week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    'PAYE',extract(dow from current_date)::integer,'ELECTRONIC'
  );

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,sheet_scope,line_type,submission_mode,
    r2_nurse_key,r2_auth_key
  ) values
  (
    v_anchor_one_timesheet,'HISTORY_ANCHOR_ONE_'||replace(v_anchor_one_timesheet::text,'-',''),
    'GCK-HISTORY-'||replace(v_candidate::text,'-',''),
    'HISTORY HOSPITAL','HISTORY WARD ONE','NURSE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'history-anchor/one/candidate','history-anchor/one/manager'
  ),
  (
    v_anchor_two_timesheet,'HISTORY_ANCHOR_TWO_'||replace(v_anchor_two_timesheet::text,'-',''),
    'GCK-HISTORY-'||replace(v_candidate::text,'-',''),
    'HISTORY HOSPITAL','HISTORY WARD TWO','NURSE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'history-anchor/two/candidate','history-anchor/two/manager'
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values
    (v_anchor_one_week,v_contract,current_date,0,'OPEN','ELECTRONIC',v_anchor_one_timesheet),
    (v_anchor_two_week,v_contract,current_date,1,'OPEN','ELECTRONIC',v_anchor_two_timesheet);

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status
  ) values
    (v_anchor_one_timesheet,1,v_candidate,v_client,8,'UNPROCESSED'),
    (v_anchor_two_timesheet,1,v_candidate,v_client,4,'UNPROCESSED');

  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST',lower(v_email),'ACTIVE');

  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('d4',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );

  v_carrier:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_anchor_one_timesheet,null,
    'history-anchor:create-carrier',now()
  );
  if v_carrier->>'placement'<>'CREATE_CARRIER'
     or nullif(v_carrier->>'target_contract_week_id','') is null then
    raise exception 'history-anchor carrier was not created: %',v_carrier;
  end if;
  v_carrier_week:=(v_carrier->>'target_contract_week_id')::uuid;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_first_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_carrier_week,
      'anchor_timesheet_id',v_anchor_one_timesheet,
      'week_ending_date',current_date
    ),
    'history-anchor:first-workflow',now()
  );
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'first history-anchor workflow was not created: %',v_result;
  end if;
  update public.candidate_submission_workflows
  set state='CANCELLED',updated_at_utc=now()
  where id=v_first_workflow;

  -- No anchor is supplied. Although two worked Timesheets exist, this exact
  -- carrier has one durable historical anchor and must recover it.
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_recovered_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_carrier_week,
      'week_ending_date',current_date
    ),
    'history-anchor:recovered-workflow',now()+interval '1 second'
  );
  if coalesce((v_result->>'ok')::boolean,false)=false
     or not exists(
       select 1 from public.candidate_submission_workflows
       where id=v_recovered_workflow
         and contract_week_id=v_carrier_week
         and anchor_timesheet_id=v_anchor_one_timesheet
         and target_timesheet_id is null
     ) then
    raise exception 'unique historical carrier anchor was not recovered: %',v_result;
  end if;
  update public.candidate_submission_workflows
  set state='CANCELLED',updated_at_utc=now()
  where id=v_recovered_workflow;

  -- An explicit, valid but different anchor creates contradictory carrier
  -- history. A later anchorless request must fail closed instead of guessing.
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_conflicting_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_carrier_week,
      'anchor_timesheet_id',v_anchor_two_timesheet,
      'week_ending_date',current_date
    ),
    'history-anchor:conflicting-workflow',now()+interval '2 seconds'
  );
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'conflicting history fixture workflow was not created: %',v_result;
  end if;
  update public.candidate_submission_workflows
  set state='CANCELLED',updated_at_utc=now()
  where id=v_conflicting_workflow;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_refused_workflow,'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract,'contract_week_id',v_carrier_week,
        'week_ending_date',current_date
      ),
      'history-anchor:must-refuse',now()+interval '3 seconds'
    );
    raise exception 'contradictory historical carrier anchors were accepted';
  exception
    when sqlstate '55000' then
      if sqlerrm<>'EXPENSE_WORKED_ANCHOR_HISTORY_AMBIGUOUS' then
        raise exception 'wrong contradictory-history failure: %',sqlerrm;
      end if;
  end;

  if exists(select 1 from public.candidate_submission_workflows where id=v_refused_workflow) then
    raise exception 'refused contradictory-history request created a workflow';
  end if;
end;
$history_anchor$;

rollback;
