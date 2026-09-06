\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for sequential Candidate expenses. A
-- finalised manager-approved expense remains awaiting agency authorisation,
-- but it must no longer block the next claim. The newly opened claim must then
-- block a third parallel claim until it is completed or cancelled.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  || jsonb_build_object(
    'candidate_app_reads',true,
    'candidate_app_writes',true,
    'candidate_record_role_capabilities',true,
    'candidate_expense_atomic_placement',true,
    'candidate_manager_approval',true
  )
where id=1;

do $sequential_expense$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_anchor_week uuid:=gen_random_uuid();
  v_anchor_timesheet uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_first_carrier_week uuid;
  v_first_expense_timesheet uuid:=gen_random_uuid();
  v_first_workflow uuid:=gen_random_uuid();
  v_second_carrier_week uuid;
  v_second_workflow uuid:=gen_random_uuid();
  v_third_workflow uuid:=gen_random_uuid();
  v_email text:='sequential-expense-'||gen_random_uuid()::text||'@example.test';
  v_result jsonb;
  v_action jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate sequential expense verification client');

  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,v_email,true,
    'GCK-SEQUENTIAL-'||replace(v_candidate::text,'-',''));

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
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_anchor_timesheet,
    'SEQUENTIAL_ANCHOR_'||replace(v_anchor_timesheet::text,'-',''),
    'GCK-SEQUENTIAL-'||replace(v_candidate::text,'-',''),
    'SEQUENTIAL HOSPITAL','SEQUENTIAL WARD','NURSE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'sequential/anchor/candidate','sequential/anchor/manager'
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(
    v_anchor_week,v_contract,current_date,0,'OPEN','ELECTRONIC',
    v_anchor_timesheet
  );

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    processing_status
  ) values(
    v_anchor_timesheet,1,v_candidate,v_client,8,'UNPROCESSED'
  );

  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(v_account,'TEST',lower(v_email),'ACTIVE');

  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    decode(repeat('e4',32),'hex'),now()+interval '30 days',
    now()+interval '90 days'
  );

  v_result:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_anchor_timesheet,null,
    'sequential-expense:first-carrier',now()
  );
  if v_result->>'placement'<>'CREATE_CARRIER' then
    raise exception 'First sequential expense carrier was not created: %',v_result;
  end if;
  v_first_carrier_week:=(v_result->>'target_contract_week_id')::uuid;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_first_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_first_carrier_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),
    'sequential-expense:first-workflow',now()
  );
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'First sequential expense workflow was not created: %',v_result;
  end if;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,sheet_scope,line_type,
    submission_mode,candidate_workflow_id,candidate_workflow_generation,
    candidate_manager_approved_at_utc,r2_nurse_key,r2_auth_key
  ) values(
    v_first_expense_timesheet,
    'SEQUENTIAL_EXPENSE_'||replace(v_first_expense_timesheet::text,'-',''),
    'GCK-SEQUENTIAL-'||replace(v_candidate::text,'-',''),
    'SEQUENTIAL HOSPITAL','SEQUENTIAL WARD','NURSE',
    v_contract,current_date,'WEEKLY','EXPENSES','ELECTRONIC',
    v_first_workflow,2,now(),
    'sequential/expense/candidate','sequential/expense/manager'
  );

  update public.contract_weeks
  set timesheet_id=v_first_expense_timesheet,updated_at=now()
  where id=v_first_carrier_week;

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    expenses_pay_ex_vat,processing_status
  ) values(
    v_first_expense_timesheet,1,v_candidate,v_client,0,12.50,'PENDING_AUTH'
  );

  update public.candidate_submission_workflows
  set state='FINALISED',generation=2,
    target_timesheet_id=v_first_expense_timesheet,
    manager_approved_at_utc=now(),finalised_at_utc=now(),updated_at_utc=now()
  where id=v_first_workflow;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'PENDING_AUTH',
    jsonb_build_array(jsonb_build_object(
      'workflow_id',v_first_workflow,'workflow_kind','CONTRACT_EXPENSE',
      'state','FINALISED','generation',2,'detail_action_owner',true,
      'target_timesheet_id',v_first_expense_timesheet,
      'updated_at_utc',now()
    )),
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    v_anchor_timesheet,v_anchor_week
  );
  if v_action->>'code'<>'ADD_EXPENSES' then
    raise exception 'Finalised expense did not offer Add Expenses: %',v_action;
  end if;

  v_result:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_anchor_timesheet,null,
    'sequential-expense:second-carrier',now()+interval '1 second'
  );
  if v_result->>'placement'<>'CREATE_CARRIER' then
    raise exception 'Second sequential expense carrier was not created: %',v_result;
  end if;
  v_second_carrier_week:=(v_result->>'target_contract_week_id')::uuid;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_second_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_second_carrier_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),
    'sequential-expense:second-workflow',now()+interval '1 second'
  );
  if coalesce((v_result->>'ok')::boolean,false)=false
     or not exists(
       select 1
       from public.candidate_submission_workflows
       where id=v_second_workflow and state='WORKER_DRAFT'
     ) then
    raise exception 'Second sequential expense workflow was not opened: %',v_result;
  end if;

  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_third_workflow,'CREATE',1,
      jsonb_build_object(
        'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_contract,'contract_week_id',v_second_carrier_week,
        'anchor_timesheet_id',v_anchor_timesheet,
        'week_ending_date',current_date
      ),
      'sequential-expense:parallel-workflow',now()+interval '2 seconds'
    );
    raise exception 'Parallel unfinished expense claim was accepted';
  exception
    when sqlstate '55000' then
      if sqlerrm<>'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' then
        raise exception 'Wrong parallel-claim failure: %',sqlerrm;
      end if;
  end;

  if exists(
    select 1 from public.candidate_submission_workflows
    where id=v_third_workflow
  ) then
    raise exception 'Rejected parallel expense claim created a workflow';
  end if;
end;
$sequential_expense$;

rollback;
