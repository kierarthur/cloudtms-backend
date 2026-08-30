\set ON_ERROR_STOP on

-- First-use proof for the mobile sequence that allocates a separate expense
-- carrier before it creates the Candidate workflow.  The carrier is MANUAL
-- storage, while approval-route authority remains the original worked
-- ELECTRONIC Timesheet.  Every fixture write is rolled back.

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

do $verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_anchor_week uuid:=gen_random_uuid();
  v_anchor_timesheet uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_carrier_week uuid;
  v_email text:='carrier-route-'||gen_random_uuid()::text||'@example.test';
  v_carrier jsonb;
  v_created jsonb;
  v_replayed jsonb;
  v_before_timesheet jsonb;
  v_after_timesheet jsonb;
  v_before_financial jsonb;
  v_after_financial jsonb;
  v_definition text;
begin
  select lower(pg_get_functiondef(to_regprocedure(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'
  ))) into v_definition;
  if position(
       'case when v_workflow_kind=''contract_expense'' then v_anchor_week.timesheet_id else v_week.timesheet_id end'
       in v_definition
     )=0
     or position(
       'case when v_workflow_kind=''contract_expense'' then v_anchor_week.id else v_week.id end'
       in v_definition
     )=0 then
    raise exception 'Candidate expense creation does not derive route authority from its worked anchor';
  end if;
  if v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Candidate workflow transition contains illegal qualified conditional syntax';
  end if;

  insert into public.clients(id,name) values(v_client,'Carrier Route Verification Client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,v_email,true,'GCK-CARRIER-'||replace(v_candidate::text,'-',''));
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet,
    candidate_paper_submission_enabled
  ) values(
    gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true,true
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
  ) values(
    v_anchor_timesheet,'CARRIER_ROUTE_'||replace(v_anchor_timesheet::text,'-',''),
    'GCK-CARRIER-'||replace(v_candidate::text,'-',''),
    'CARRIER HOSPITAL','CARRIER WARD','NURSE',
    v_contract,current_date,'WEEKLY','HOURS','ELECTRONIC',
    'carrier-route/candidate-signature','carrier-route/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(
    v_anchor_week,v_contract,current_date,'OPEN','ELECTRONIC',v_anchor_timesheet
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status
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
    v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('a7',32),'hex'),
    now()+interval '30 days',now()+interval '90 days'
  );

  select to_jsonb(t) into v_before_timesheet
  from public.timesheets t where t.timesheet_id=v_anchor_timesheet;
  select to_jsonb(tf) into v_before_financial
  from public.timesheets_financials tf
  where tf.timesheet_id=v_anchor_timesheet and tf.is_current=true;

  v_carrier:=public.expense_carrier_resolve_or_create_atomic_v1(
    v_candidate,'TEST',v_anchor_timesheet,null,
    'carrier-route:create-carrier',now()
  );
  if v_carrier->>'placement'<>'CREATE_CARRIER'
     or nullif(v_carrier->>'target_contract_week_id','') is null
     or nullif(v_carrier->>'target_timesheet_id','') is not null then
    raise exception 'Separate expense carrier was not created safely: %',v_carrier;
  end if;
  v_carrier_week:=(v_carrier->>'target_contract_week_id')::uuid;
  if not exists(
    select 1 from public.contract_weeks
    where id=v_carrier_week
      and contract_id=v_contract
      and week_ending_date=current_date
      and submission_mode_snapshot='MANUAL'
      and timesheet_id is null
  ) then
    raise exception 'Expense carrier no longer has the expected empty MANUAL storage shape';
  end if;

  v_created:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE',
      'scope','WEEKLY',
      'route','ELECTRONIC',
      'contract_id',v_contract,
      'contract_week_id',v_carrier_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),
    'carrier-route:create-workflow',now()
  );
  if coalesce((v_created->>'ok')::boolean,false)=false
     or v_created->>'state'<>'WORKER_DRAFT'
     or (v_created->>'workflow_id')::uuid is distinct from v_workflow then
    raise exception 'First expense-workflow creation was not accepted: %',v_created;
  end if;
  if not exists(
    select 1 from public.candidate_submission_workflows
    where id=v_workflow
      and workflow_kind='CONTRACT_EXPENSE'
      and route='ELECTRONIC'
      and contract_week_id=v_carrier_week
      and anchor_timesheet_id=v_anchor_timesheet
      and target_timesheet_id is null
  ) then
    raise exception 'Created expense workflow lost its carrier/anchor boundary';
  end if;

  v_replayed:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_workflow,'CREATE',1,
    jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE',
      'scope','WEEKLY',
      'route','ELECTRONIC',
      'contract_id',v_contract,
      'contract_week_id',v_carrier_week,
      'anchor_timesheet_id',v_anchor_timesheet,
      'week_ending_date',current_date
    ),
    'carrier-route:create-workflow',now()+interval '1 second'
  );
  if coalesce((v_replayed->>'idempotent_replay')::boolean,false)=false
     or (v_replayed->>'workflow_id')::uuid is distinct from v_workflow
     or (select count(*) from public.candidate_submission_workflows
         where candidate_id=v_candidate)<>1
     or (select count(*) from public.contract_weeks
         where contract_id=v_contract and week_ending_date=current_date)<>2 then
    raise exception 'Expense workflow replay created duplicate state: %',v_replayed;
  end if;

  select to_jsonb(t) into v_after_timesheet
  from public.timesheets t where t.timesheet_id=v_anchor_timesheet;
  select to_jsonb(tf) into v_after_financial
  from public.timesheets_financials tf
  where tf.timesheet_id=v_anchor_timesheet and tf.is_current=true;
  if v_after_timesheet is distinct from v_before_timesheet
     or v_after_financial is distinct from v_before_financial
     or (select count(*) from public.timesheets_financials
         where candidate_id=v_candidate)<>1 then
    raise exception 'Expense workflow creation changed Timesheet financial authority';
  end if;
end;
$verification$;

rollback;
