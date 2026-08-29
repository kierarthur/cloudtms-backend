-- Candidate App entitlement and one-anchor overlay executable verification.
-- Run only in a disposable database after the complete install bundle.
-- Every fixture write is rolled back.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=jsonb_build_object(
  'candidate_app_reads',true,
  'candidate_record_role_capabilities',true
)
where id=1;

do $read_contract$
declare
  v_now timestamptz:=date_trunc('second',now());
  v_client uuid:='93000000-0000-0000-0000-000000000001';
  v_candidate uuid:='93000000-0000-0000-0000-000000000002';
  v_contract uuid:='93000000-0000-0000-0000-000000000003';
  v_base_week uuid:='93000000-0000-0000-0000-000000000004';
  v_base_timesheet uuid:='93000000-0000-0000-0000-000000000005';
  v_additional_week uuid:='93000000-0000-0000-0000-000000000006';
  v_additional_timesheet uuid:='93000000-0000-0000-0000-000000000007';
  v_expense_week uuid:='93000000-0000-0000-0000-000000000008';
  v_expense_timesheet uuid:='93000000-0000-0000-0000-000000000009';
  v_account uuid:='93000000-0000-0000-0000-000000000010';
  v_session uuid:='93000000-0000-0000-0000-000000000011';
  v_old_client uuid:='93000000-0000-0000-0000-000000000012';
  v_old_candidate uuid:='93000000-0000-0000-0000-000000000013';
  v_old_contract uuid:='93000000-0000-0000-0000-000000000014';
  v_old_week uuid:='93000000-0000-0000-0000-000000000015';
  v_old_timesheet uuid:='93000000-0000-0000-0000-000000000016';
  v_old_account uuid:='93000000-0000-0000-0000-000000000017';
  v_old_session uuid:='93000000-0000-0000-0000-000000000018';
  v_bootstrap jsonb;
  v_page jsonb;
  v_items jsonb;
begin
  insert into public.clients(id) values(v_client),(v_old_client);
  insert into public.candidates(id,email,active,key_norm) values
    (v_candidate,'read-contract@example.test',true,'REAL-GLOBAL-CANDIDATE-KEY'),
    (v_old_candidate,'read-old@example.test',true,null);
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    candidate_expenses_require_separate_timesheet
  ) values
    (gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',extract(dow from current_date)::integer,true),
    (gen_random_uuid(),v_old_client,current_date-300,'ELECTRONIC',extract(dow from current_date)::integer,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values
    (v_contract,v_candidate,v_client,current_date-60,current_date+30,
      extract(dow from current_date)::integer,'ELECTRONIC'),
    (v_old_contract,v_old_candidate,v_old_client,current_date-400,current_date-180,
      extract(dow from current_date)::integer,'ELECTRONIC');

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,parent_timesheet_id
  ) values
    (v_base_timesheet,v_contract,current_date,'HOURS','MANUAL',null),
    (v_additional_timesheet,v_contract,current_date,'HOURS','MANUAL',v_base_timesheet),
    (v_expense_timesheet,v_contract,current_date,'EXPENSES','MANUAL',v_base_timesheet),
    (v_old_timesheet,v_old_contract,(current_date-interval '7 months')::date,'HOURS','MANUAL',null);
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_base_week,v_contract,current_date,0,'OPEN','ELECTRONIC',v_base_timesheet),
    (v_additional_week,v_contract,current_date,1,'OPEN','ELECTRONIC',v_additional_timesheet),
    (v_expense_week,v_contract,current_date,2,'OPEN','ELECTRONIC',v_expense_timesheet),
    (v_old_week,v_old_contract,(current_date-interval '7 months')::date,0,'INVOICED','ELECTRONIC',v_old_timesheet);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,other_pay_ex_vat,processing_status
  ) values
    (v_base_timesheet,v_candidate,v_client,8,0,'UNPROCESSED'),
    (v_additional_timesheet,v_candidate,v_client,4,0,'UNPROCESSED'),
    (v_expense_timesheet,v_candidate,v_client,0,10,'UNPROCESSED'),
    (v_old_timesheet,v_old_candidate,v_old_client,8,0,'READY_FOR_INVOICE');

  insert into public.candidate_app_accounts(id,environment,email_normalized,status) values
    (v_account,'TEST','read-contract@example.test','ACTIVE'),
    (v_old_account,'TEST','read-old@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values
    (v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('97',32),'hex'),
      v_now+interval '30 days',v_now+interval '90 days'),
    (v_old_session,v_old_account,'TEST',v_old_candidate,'ACTIVE',decode(repeat('98',32),'hex'),
      v_now+interval '30 days',v_now+interval '90 days');

  v_bootstrap:=public.candidate_app_bootstrap_v1(v_session,'TEST',0,v_now);
  if coalesce((v_bootstrap#>>'{entitlements,daily}')::boolean,false)=false
     or coalesce((v_bootstrap#>>'{entitlements,contract}')::boolean,false)=false then
    raise exception 'nonliteral GCK/current-week entitlement failed: %',v_bootstrap;
  end if;
  v_bootstrap:=public.candidate_app_bootstrap_v1(v_old_session,'TEST',0,v_now);
  if coalesce((v_bootstrap#>>'{entitlements,daily}')::boolean,false)=true
     or coalesce((v_bootstrap#>>'{entitlements,contract}')::boolean,false)=true then
    raise exception 'empty GCK or older-than-six-month history granted entitlement: %',v_bootstrap;
  end if;

  v_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,50,v_now);
  v_items:=v_page->'items';
  if jsonb_array_length(v_items)<>2
     or (select count(*) from jsonb_array_elements(v_items) item
         where item->>'timesheet_id'=v_base_timesheet::text
           and (item#>>'{expenses,other_pay_ex_vat}')::numeric=10)<>1
     or (select count(*) from jsonb_array_elements(v_items) item
         where item->>'timesheet_id'=v_additional_timesheet::text
           and (item#>>'{expenses,other_pay_ex_vat}')::numeric=0)<>1
     or jsonb_array_length(v_page->'readiness_conflicts')<>0 then
    raise exception 'parent-linked expense carrier was not hidden and overlaid on its exact worked row: %',v_page;
  end if;
end;
$read_contract$;

rollback;
