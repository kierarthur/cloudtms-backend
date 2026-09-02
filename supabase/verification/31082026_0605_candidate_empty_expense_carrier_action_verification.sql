\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for an Office-created empty expense-only
-- carrier.  The Candidate must add the first expense to the existing workflow;
-- the read path must not call it a saved claim or create a second workflow.

begin;

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_workflows jsonb;
  v_action jsonb;
  v_invocation jsonb;
  v_definition text;
begin
  select lower(pg_get_functiondef(to_regprocedure(
    'private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)'
  ))) into v_definition;
  if position('and not v_draft_has_content then ''add_expenses''' in v_definition)=0
     or position('v_workflow:=null' in replace(v_definition,' ',''))=0
     or v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Empty expense-carrier primary-action authority is not installed safely';
  end if;

  select lower(pg_get_functiondef(to_regprocedure(
    'private._candidate_action_invocation_v1(jsonb)'
  ))) into v_definition;
  if position(
       'v_code=''add_expenses'' and nullif(p_action->>''workflow_id'','''') is not null'
       in v_definition
     )=0
     or position('''expense_claim_editor''' in v_definition)=0
     or v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Existing-workflow expense editor invocation is not installed safely';
  end if;

  insert into public.clients(id,name)
  values(v_client,'Empty Expense Carrier Verification Client');
  insert into public.candidates(id,email,active)
  values(v_candidate,'empty-expense-'||v_candidate::text||'@example.test',true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    pay_method_snapshot,week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-7,current_date+7,
    'PAYE',extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,1,'OPEN','MANUAL',null);
  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values(
    v_account,'TEST','empty-expense-'||v_candidate::text||'@example.test','SETUP_REQUIRED'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,week_ending_date,
    policy_snapshot_json,input_snapshot_json,idempotency_key
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC',
    'WORKER_DRAFT',1,v_contract,v_week,current_date,'{}','{}',
    'empty-expense-carrier-verification'
  );

  v_workflows:=jsonb_build_array(jsonb_build_object(
    'workflow_id',v_workflow,'workflow_kind','CONTRACT_EXPENSE',
    'state','WORKER_DRAFT','generation',1,'detail_action_owner',true,
    'updated_at_utc',now()
  ));
  v_action:=private._candidate_timesheet_primary_action_v1(
    'WORKER_DRAFT',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    null,v_week
  );
  if v_action->>'code'<>'ADD_EXPENSES'
     or v_action->>'label'<>'Add Expenses'
     or (v_action->>'workflow_id')::uuid is distinct from v_workflow
     or (v_action->>'workflow_generation')::integer<>1 then
    raise exception 'Empty expense carrier was not presented as Add Expenses: %',v_action;
  end if;
  v_invocation:=private._candidate_action_invocation_v1(v_action);
  if v_invocation#>>'{invocation,kind}'<>'CLIENT_DESTINATION'
     or v_invocation#>>'{invocation,destination}'<>'EXPENSE_CLAIM_EDITOR'
     or (v_invocation#>>'{invocation,context,workflow_id}')::uuid is distinct from v_workflow
     or v_invocation ? 'method' and v_invocation->>'method' is not null
     or v_invocation ? 'path' and v_invocation->>'path' is not null then
    raise exception 'Empty expense carrier did not reuse its existing editor workflow: %',v_invocation;
  end if;

  v_action:=private._candidate_timesheet_primary_action_v1(
    'WORKER_DRAFT',v_workflows,
    jsonb_build_object('can_edit_hours',true,'can_edit_expenses',true),
    null,v_week
  );
  if v_action->>'code'<>'ENTER_TIMESHEET'
     or v_action->>'label'<>'Enter Timesheet'
     or (v_action->>'contract_week_id')::uuid is distinct from v_week
     or v_action ? 'workflow_id' then
    raise exception 'Empty expense shell hid the FLEXIBLE hours journey: %',v_action;
  end if;

  insert into public.candidate_submission_components(
    workflow_id,workflow_generation,component_no,component_kind,expense_category,
    document_role,state,created_at_utc
  ) values(
    v_workflow,1,1,'EXPENSE_EVIDENCE','OTHER','SOURCE_EVIDENCE','PENDING',now()
  );
  v_action:=private._candidate_timesheet_primary_action_v1(
    'WORKER_DRAFT',v_workflows,
    jsonb_build_object('can_edit_hours',false,'can_edit_expenses',true),
    null,v_week
  );
  if v_action->>'code'<>'CONTINUE_EXPENSE_CLAIM'
     or v_action->>'label'<>'Continue Expense Claim'
     or (v_action->>'workflow_id')::uuid is distinct from v_workflow then
    raise exception 'A genuine expense draft no longer continues normally: %',v_action;
  end if;

  if (select count(*) from public.candidate_submission_workflows where id=v_workflow)<>1
     or (select count(*) from public.contract_weeks where id=v_week)<>1 then
    raise exception 'Read-only action projection duplicated the expense claim';
  end if;
end;
$verification$;

rollback;
