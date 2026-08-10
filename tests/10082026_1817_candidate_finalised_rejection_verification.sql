\set ON_ERROR_STOP on
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_reads":true,"candidate_app_writes":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

create temporary table candidate_finalised_rejection_probe(id integer);

create or replace function pg_temp.verify_finalised_rejection(
  p_workflow_kind text,
  p_separate_expense boolean,
  p_anchor_isolation boolean
)
returns void
language plpgsql
as $function$
declare
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_base_timesheet uuid:=gen_random_uuid();
  v_base_week uuid:=gen_random_uuid();
  v_target_timesheet uuid;
  v_target_week uuid;
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
  v_component uuid:=gen_random_uuid();
  v_request uuid:=gen_random_uuid();
  v_replacement_workflow uuid:=gen_random_uuid();
  v_isolated_expense_timesheet uuid:=gen_random_uuid();
  v_isolated_expense_week uuid:=gen_random_uuid();
  v_isolated_expense_workflow uuid:=gen_random_uuid();
  v_secondary_rejection_workflow uuid:=gen_random_uuid();
  v_target_booking text:='FINALISED-REJECT-'||replace(gen_random_uuid()::text,'-','');
  v_signature text;
  v_result jsonb;
  v_replay jsonb;
  v_page jsonb;
  v_detail jsonb;
  v_card jsonb;
  v_new_timesheet uuid;
  v_expected_display_timesheet uuid;
  v_expected_scope text;
  v_expected_action text;
  v_state text;
  v_generation integer;
  v_notification_count integer;
  v_current_count integer;
  v_version_count integer;
  v_component_kind text;
  v_document_role text;
begin
  if p_workflow_kind not in ('CONTRACT_HOURS','CONTRACT_COMBINED','CONTRACT_EXPENSE') then
    raise exception 'Unsupported test workflow kind: %',p_workflow_kind;
  end if;
  if p_separate_expense is distinct from (p_workflow_kind='CONTRACT_EXPENSE') then
    raise exception 'Separate-expense fixture shape does not match workflow kind';
  end if;
  if p_anchor_isolation and p_workflow_kind<>'CONTRACT_HOURS' then
    raise exception 'Anchor-isolation fixture is hours-only';
  end if;

  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.candidates(id,email,active)
  values(v_candidate,'finalised-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name)
  values(v_client,'Finalised rejection client '||left(v_client::text,8));
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
  values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );

  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_base_timesheet,v_contract,
    case when p_separate_expense then v_target_booking||'-HOURS' else v_target_booking end,
    current_date,'HOURS','WEEKLY','ELECTRONIC',
    'candidate-app/test/finalised/candidate-signature',
    'candidate-app/test/finalised/manager-signature'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id
  ) values(v_base_week,v_contract,current_date,0,'SUBMITTED','ELECTRONIC',v_base_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
    expenses_pay_ex_vat,expenses_charge_ex_vat
  ) values(
    v_base_timesheet,v_candidate,v_client,true,'UNPROCESSED',8,
    case when p_workflow_kind='CONTRACT_COMBINED' then 25 else 0 end,
    case when p_workflow_kind='CONTRACT_COMBINED' then 30 else 0 end
  );

  if p_separate_expense then
    v_target_timesheet:=gen_random_uuid();
    v_target_week:=gen_random_uuid();
    insert into public.timesheets(
      timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
      submission_mode,is_adjustment,parent_timesheet_id
    ) values(
      v_target_timesheet,v_contract,v_target_booking,current_date,'EXPENSES','WEEKLY',
      'MANUAL',true,v_base_timesheet
    );
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,additional_seq,status,
      submission_mode_snapshot,timesheet_id,is_adjustment
    ) values(v_target_week,v_contract,current_date,1,'SUBMITTED','MANUAL',v_target_timesheet,true);
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
      expenses_pay_ex_vat,expenses_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat
    ) values(
      v_target_timesheet,v_candidate,v_client,true,'UNPROCESSED',0,25,30,25,30
    );
  else
    v_target_timesheet:=v_base_timesheet;
    v_target_week:=v_base_week;
  end if;

  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    v_account,'TEST','finalised-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE'
  );
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(
    v_session,v_account,'TEST',v_candidate,'ACTIVE',
    extensions.digest(convert_to(v_session::text,'UTF8'),'sha256'),
    now()+interval '1 day',now()+interval '7 days'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc
  ) values(
    v_workflow,'TEST',v_account,v_candidate,p_workflow_kind,'WEEKLY','EMAIL','FINALISED',2,
    v_contract,case when p_separate_expense then v_target_week else v_base_week end,
    v_base_timesheet,v_target_timesheet,
    current_date,'finalised-original:'||v_workflow::text,now()
  );

  v_component_kind:=case when p_workflow_kind='CONTRACT_HOURS'
    then 'HOURS_TIMESHEET' else 'EXPENSE_SUMMARY' end;
  v_document_role:=case when p_workflow_kind='CONTRACT_HOURS'
    then 'ELECTRONIC_TIMESHEET_MANAGER_REVIEW'
    else 'EXPENSE_MILEAGE_APPROVAL_SUMMARY' end;
  insert into public.candidate_submission_components(
    id,workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
    document_role,state,immutable_at_utc,required,review_ordinal,
    review_render_state,final_signed_render_state
  ) values(
    v_component,v_workflow,1,1,v_target_timesheet,v_component_kind,
    v_document_role,'IMMUTABLE',now(),
    p_workflow_kind='CONTRACT_HOURS',
    case when p_workflow_kind='CONTRACT_HOURS' then 1 else null end,
    case when p_workflow_kind='CONTRACT_HOURS' then 'PENDING' else 'NOT_REQUIRED' end,
    case when p_workflow_kind='CONTRACT_HOURS' then 'PENDING' else 'NOT_REQUIRED' end
  );
  insert into public.candidate_approval_requests(
    id,workflow_id,workflow_generation,request_generation,method,state,
    approved_at_utc,idempotency_key,review_manifest_sha256,
    required_component_ids,required_component_manifest_json
  ) values(
    v_request,v_workflow,1,1,'PHONE','APPROVED',now(),
    'finalised-approval:'||v_request::text,
    extensions.digest(convert_to(v_request::text,'UTF8'),'sha256'),
    array[v_component],jsonb_build_array(jsonb_build_object(
      'component_id',v_component,'component_kind',v_component_kind
    ))
  );

  if p_anchor_isolation then
    insert into public.timesheets(
      timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
      submission_mode,is_adjustment,parent_timesheet_id
    ) values(
      v_isolated_expense_timesheet,v_contract,
      'ISOLATED-EXPENSE-'||replace(v_isolated_expense_timesheet::text,'-',''),
      current_date,'EXPENSES','WEEKLY','MANUAL',true,v_base_timesheet
    );
    insert into public.contract_weeks(
      id,contract_id,week_ending_date,additional_seq,status,
      submission_mode_snapshot,timesheet_id,is_adjustment
    ) values(
      v_isolated_expense_week,v_contract,current_date,1,'SUBMITTED','MANUAL',
      v_isolated_expense_timesheet,true
    );
    insert into public.timesheets_financials(
      timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
      expenses_pay_ex_vat,expenses_charge_ex_vat,other_pay_ex_vat,other_charge_ex_vat
    ) values(
      v_isolated_expense_timesheet,v_candidate,v_client,true,'UNPROCESSED',0,40,48,40,48
    );
    insert into public.candidate_submission_workflows(
      id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
      contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
      week_ending_date,idempotency_key,finalised_at_utc
    ) values(
      v_isolated_expense_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE',
      'WEEKLY','EMAIL','FINALISED',2,v_contract,v_isolated_expense_week,v_base_timesheet,
      v_isolated_expense_timesheet,current_date,
      'isolated-expense:'||v_isolated_expense_workflow::text,now()
    );
  end if;

  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_target_timesheet,v_target_week,false
  )->>'row_signature';
  v_result:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_target_timesheet,v_target_timesheet,v_signature,
    'Finalised Candidate submission rejected',
    'finalised-reject:'||v_workflow::text,now()
  );
  v_new_timesheet:=(v_result->>'timesheet_id')::uuid;
  if v_new_timesheet is null or v_new_timesheet=v_target_timesheet then
    raise exception 'Finalised rejection did not create a replacement target: %',v_result;
  end if;
  select state,generation into v_state,v_generation
  from public.candidate_submission_workflows where id=v_workflow;
  if v_state<>'REJECTED' or v_generation<>3 then
    raise exception 'Finalised workflow was not rejected exactly once: %, %',v_state,v_generation;
  end if;
  if (select rejection_reason from public.candidate_submission_workflows where id=v_workflow)
       <>'Finalised Candidate submission rejected' then
    raise exception 'Finalised workflow rejection reason was not recorded';
  end if;
  if (select state from public.candidate_submission_components where id=v_component)<>'REJECTED' then
    raise exception 'Finalised workflow component lineage remained active';
  end if;
  if (select state from public.candidate_approval_requests where id=v_request)<>'SUPERSEDED' then
    raise exception 'Finalised workflow approval lineage remained active';
  end if;
  if (select timesheet_id from public.contract_weeks where id=v_target_week)<>v_new_timesheet then
    raise exception 'Rejected contract week did not point to the replacement target';
  end if;

  v_expected_display_timesheet:=case when p_separate_expense
    then v_base_timesheet else v_new_timesheet end;
  v_expected_scope:=case when p_separate_expense
    then 'COMPLETE_EXPENSE_CLAIM' else 'COMPLETE_TIMESHEET_RECORD' end;
  v_expected_action:=case p_workflow_kind
    when 'CONTRACT_EXPENSE' then 'RESUBMIT_EXPENSE_CLAIM'
    when 'CONTRACT_COMBINED' then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
    else 'RESUBMIT_TIMESHEET' end;

  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,now());
  select item.value into v_card
  from jsonb_array_elements(v_page->'items') item(value)
  where item.value->>'timesheet_id'=v_expected_display_timesheet::text
  limit 1;
  if v_card is null then
    raise exception 'Replacement current Candidate card did not retain rejected workflow: %',v_page;
  end if;
  if v_card->>'candidate_status_code'<>'REJECTED'
     or v_card->>'rejection_reason'<>'Finalised Candidate submission rejected'
     or v_card->>'rejection_scope'<>v_expected_scope
     or v_card#>>'{rejection,workflow_id}'<>v_workflow::text
     or v_card#>>'{rejection,scope}'<>v_expected_scope
     or v_card#>>'{rejection,required_action}'<>v_expected_action then
    raise exception 'Replacement Candidate rejection contract was incorrect: %',v_card;
  end if;
  if not exists(
    select 1 from jsonb_array_elements(v_card->'workflows') workflow_item(value)
    where workflow_item.value->>'workflow_id'=v_workflow::text
      and workflow_item.value->>'state'='REJECTED'
      and workflow_item.value->>'rejection_scope'=v_expected_scope
      and workflow_item.value->>'required_resubmission_action'=v_expected_action
  ) then
    raise exception 'Rejected workflow overlay omitted server-owned recovery authority: %',v_card;
  end if;

  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',null,null,v_workflow,now()
  );
  if not exists(
    select 1 from jsonb_array_elements(v_detail->'workflows') workflow_item(value)
    where workflow_item.value->>'workflow_id'=v_workflow::text
      and workflow_item.value->>'state'='REJECTED'
      and workflow_item.value->>'rejection_scope'=v_expected_scope
      and workflow_item.value->>'required_resubmission_action'=v_expected_action
  ) then
    raise exception 'Rejected workflow detail omitted version-aware recovery authority: %',v_detail;
  end if;
  if p_anchor_isolation then
    select state,generation into v_state,v_generation
    from public.candidate_submission_workflows where id=v_isolated_expense_workflow;
    if v_state<>'FINALISED' or v_generation<>2 then
      raise exception 'Anchor-only finalised separate expense workflow was wrongly rejected: %, %',
        v_state,v_generation;
    end if;
    if not exists(
      select 1 from public.timesheets_financials
      where timesheet_id=v_isolated_expense_timesheet and is_current=true
        and other_pay_ex_vat=40 and other_charge_ex_vat=48
    ) then
      raise exception 'Anchor-only finalised separate expense economics were altered';
    end if;
    if exists(
      select 1 from public.candidate_notifications
      where workflow_id=v_isolated_expense_workflow and event_type='OFFICE_REJECTED'
    ) then
      raise exception 'Anchor-only finalised separate expense received a rejection notification';
    end if;
    if v_card#>>'{expenses,other_pay_ex_vat}'<>'40'
       or not exists(
         select 1 from jsonb_array_elements(v_card->'workflows') workflow_item(value)
         where workflow_item.value->>'workflow_id'=v_isolated_expense_workflow::text
           and workflow_item.value->>'state'='FINALISED'
           and workflow_item.value->>'claim_family'='EXPENSES'
       )
       or exists(
         select 1 from jsonb_array_elements(v_page->'readiness_conflicts') conflict(value)
         where conflict.value->>'code' in ('INVALID_WORKFLOW_ANCHOR','INVALID_PARENT_ANCHOR')
       ) then
      raise exception 'Version-family anchor did not preserve the separate expense overlay: %',v_page;
    end if;

    insert into public.candidate_submission_workflows(
      id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
      contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
      week_ending_date,rejection_reason,rejection_scope,idempotency_key,
      created_at_utc,updated_at_utc
    ) values(
      v_secondary_rejection_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE',
      'WEEKLY','EMAIL','REJECTED',2,v_contract,v_isolated_expense_week,
      v_base_timesheet,v_isolated_expense_timesheet,current_date,
      'Separate expense also needs correction','COMPLETE_EXPENSE_CLAIM',
      'secondary-rejection:'||v_secondary_rejection_workflow::text,
      clock_timestamp()+interval '1 millisecond',clock_timestamp()+interval '1 millisecond'
    );
    v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,clock_timestamp());
    select item.value into v_card
    from jsonb_array_elements(v_page->'items') item(value)
    where item.value->>'timesheet_id'=v_new_timesheet::text
    limit 1;
    if jsonb_array_length(v_card->'rejections')<>2
       or not exists(
         select 1 from jsonb_array_elements(v_card->'rejections') rejection_item(value)
         where rejection_item.value->>'claim_family'='HOURS'
           and rejection_item.value->>'required_resubmission_action'='RESUBMIT_TIMESHEET'
       )
       or not exists(
         select 1 from jsonb_array_elements(v_card->'rejections') rejection_item(value)
         where rejection_item.value->>'claim_family'='EXPENSES'
           and rejection_item.value->>'required_resubmission_action'='RESUBMIT_EXPENSE_CLAIM'
       ) then
      raise exception 'Independent hours and expense rejections were not both represented: %',v_card;
    end if;
    update public.candidate_submission_workflows
    set state='SUPERSEDED',updated_at_utc=clock_timestamp()
    where id=v_secondary_rejection_workflow;
  end if;

  select count(*) into v_notification_count
  from public.candidate_notifications
  where workflow_id=v_workflow and event_type='OFFICE_REJECTED';
  if v_notification_count<>1 then
    raise exception 'Expected one office-rejection notification, found %',v_notification_count;
  end if;
  select count(*),count(*) filter(where is_current)
  into v_version_count,v_current_count
  from public.timesheets where booking_id=v_target_booking;
  if v_version_count<>2 or v_current_count<>1 then
    raise exception 'Unexpected rejection version family: versions %, current %',
      v_version_count,v_current_count;
  end if;

  v_replay:=public.candidate_submission_reject_atomic_v1(
    v_actor,'TEST',v_target_timesheet,v_target_timesheet,v_signature,
    'Finalised Candidate submission rejected',
    'finalised-reject:'||v_workflow::text,now()
  );
  if coalesce((v_replay->>'idempotent_replay')::boolean,false)=false
     or (v_replay->>'timesheet_id')::uuid<>v_new_timesheet then
    raise exception 'Finalised rejection replay was not durable: %',v_replay;
  end if;
  if (select generation from public.candidate_submission_workflows where id=v_workflow)<>3 then
    raise exception 'Finalised rejection replay incremented generation twice';
  end if;
  if (select count(*) from public.timesheets where booking_id=v_target_booking)<>2 then
    raise exception 'Finalised rejection replay rotated the timesheet twice';
  end if;
  if (select count(*) from public.candidate_notifications
      where workflow_id=v_workflow and event_type='OFFICE_REJECTED')<>1 then
    raise exception 'Finalised rejection replay duplicated the notification';
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_replacement_workflow,'CREATE',1,
    case when p_separate_expense then jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_base_week,
      'anchor_timesheet_id',v_base_timesheet,'week_ending_date',current_date
    ) else jsonb_build_object(
      'workflow_kind',p_workflow_kind,'scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_contract,'contract_week_id',v_base_week,
      'target_timesheet_id',v_new_timesheet,'week_ending_date',current_date
    ) end,
    'replacement:'||v_replacement_workflow::text,now()
  );
  if v_result->>'state'<>'WORKER_DRAFT'
     or v_result->>'workflow_id'<>v_replacement_workflow::text then
    raise exception 'Replacement workflow was not created: %',v_result;
  end if;
  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,now());
  select item.value into v_card
  from jsonb_array_elements(v_page->'items') item(value)
  where item.value->>'timesheet_id'=v_expected_display_timesheet::text
  limit 1;
  if v_card->>'candidate_status_code'<>'WORKER_DRAFT' then
    raise exception 'Active replacement did not take precedence over rejection history: %',v_card;
  end if;
  if v_card->>'rejection' is not null or v_card->>'rejection_reason' is not null
     or jsonb_array_length(v_card->'rejections')<>0 then
    raise exception 'Historical rejection remained actionable after replacement began: %',v_card;
  end if;
  if not exists(
    select 1 from jsonb_array_elements(v_card->'workflows') workflow_item(value)
    where workflow_item.value->>'workflow_id'=v_workflow::text
      and workflow_item.value->>'state'='REJECTED'
  ) then
    raise exception 'Active replacement removed immutable rejection history: %',v_card;
  end if;

  -- A successfully received/finalised replacement must keep the immutable old
  -- rejection in history without allowing it to become actionable again.
  update public.candidate_submission_workflows
  set state='FINALISED',target_timesheet_id=v_new_timesheet,
      contract_week_id=v_target_week,finalised_at_utc=clock_timestamp(),
      updated_at_utc=clock_timestamp()
  where id=v_replacement_workflow;
  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,clock_timestamp());
  select item.value into v_card
  from jsonb_array_elements(v_page->'items') item(value)
  where item.value->>'timesheet_id'=v_expected_display_timesheet::text
  limit 1;
  if v_card->>'candidate_status_code'='REJECTED'
     or v_card->>'rejection' is not null
     or v_card->>'rejection_reason' is not null
     or jsonb_array_length(v_card->'rejections')<>0 then
    raise exception 'Historical rejection resurfaced after replacement finalisation: %',v_card;
  end if;
  if not exists(
    select 1 from jsonb_array_elements(v_card->'workflows') workflow_item(value)
    where workflow_item.value->>'workflow_id'=v_workflow::text
      and workflow_item.value->>'state'='REJECTED'
      and coalesce((workflow_item.value->>'rejection_actionable')::boolean,false)=false
  ) then
    raise exception 'Finalised replacement lost immutable resolved rejection history: %',v_card;
  end if;

  -- A current manager refusal is its own recovery lifecycle and must take
  -- precedence over the older office rejection.
  update public.candidate_submission_workflows
  set state='REFUSED',finalised_at_utc=null,updated_at_utc=clock_timestamp()
  where id=v_replacement_workflow;
  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST',null,50,clock_timestamp());
  select item.value into v_card
  from jsonb_array_elements(v_page->'items') item(value)
  where item.value->>'timesheet_id'=v_expected_display_timesheet::text
  limit 1;
  if v_card->>'candidate_status_code'<>'REFUSED'
     or v_card->>'rejection' is not null
     or v_card->>'rejection_reason' is not null
     or jsonb_array_length(v_card->'rejections')<>0 then
    raise exception 'Current refusal did not supersede historical office rejection: %',v_card;
  end if;
end;
$function$;

select pg_temp.verify_finalised_rejection('CONTRACT_HOURS',false,true);
select pg_temp.verify_finalised_rejection('CONTRACT_COMBINED',false,false);
select pg_temp.verify_finalised_rejection('CONTRACT_EXPENSE',true,false);

do $verify_protected_history$
declare
  v_actor uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_workflow uuid:=gen_random_uuid();
begin
  insert into public.tms_users(id) values(v_actor);
  insert into public.candidates(id,email,active)
  values(v_candidate,'protected-'||replace(v_candidate::text,'-','')||'@example.test',true);
  insert into public.clients(id,name) values(v_client,'Protected rejection client');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
  values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC');
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,
    week_ending_weekday_snapshot,default_submission_mode
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,
    submission_mode,r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet,v_contract,'PROTECTED-'||replace(v_timesheet::text,'-',''),current_date,
    'HOURS','WEEKLY','ELECTRONIC','protected/candidate','protected/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'AUTHORISED','ELECTRONIC',v_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours,
    authorised_at_utc
  ) values(v_timesheet,v_candidate,v_client,true,'UNPROCESSED',8,now());
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(
    v_account,'TEST','protected-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE'
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,
    week_ending_date,idempotency_key,finalised_at_utc
  ) values(
    v_workflow,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','EMAIL','FINALISED',2,
    v_contract,v_week,v_timesheet,v_timesheet,current_date,
    'protected-workflow:'||v_workflow::text,now()
  );

  begin
    perform public.candidate_submission_reject_atomic_v1(
      v_actor,'TEST',v_timesheet,v_timesheet,'not-evaluated',
      'Authorised rejection must be blocked','protected-authorised:'||v_workflow::text,now()
    );
    raise exception 'Authorised finalised rejection was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_REJECT_REQUIRES_UNAUTHORISE' then raise; end if;
  end;

  update public.timesheets_financials
  set authorised_at_utc=null,paid_at_utc=now()
  where timesheet_id=v_timesheet and is_current=true;
  begin
    perform public.candidate_submission_reject_atomic_v1(
      v_actor,'TEST',v_timesheet,v_timesheet,'not-evaluated',
      'Paid rejection must be blocked','protected-paid:'||v_workflow::text,now()
    );
    raise exception 'Paid finalised rejection was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_REJECT_PROTECTED_HISTORY' then raise; end if;
  end;

  if (select state from public.candidate_submission_workflows where id=v_workflow)<>'FINALISED'
     or (select count(*) from public.timesheets
         where booking_id='PROTECTED-'||replace(v_timesheet::text,'-',''))<>1 then
    raise exception 'Protected rejection mutated workflow or version state';
  end if;
end;
$verify_protected_history$;

rollback;
