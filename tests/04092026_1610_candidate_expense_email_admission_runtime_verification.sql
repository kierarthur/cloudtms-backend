-- Rollback-contained runtime proof that a separated/import-authoritative expense
-- claim is blocked before entry when its dedicated Expense Invoice Email is
-- missing, while a correctly configured claim remains available.

begin;

update public.settings_defaults
set candidate_app_feature_flags_json=jsonb_build_object(
  'candidate_app_reads',true,
  'candidate_app_writes',true,
  'candidate_record_role_capabilities',true,
  'candidate_expense_atomic_placement',true,
  'candidate_expense_invoice_routing_v1',true
)
where id=1;

do $candidate_expense_email_admission$
declare
  v_client uuid:='b4000000-0000-0000-0000-000000000001';
  v_candidate uuid:='b4000000-0000-0000-0000-000000000002';
  v_contract uuid:='b4000000-0000-0000-0000-000000000003';
  v_week uuid:='b4000000-0000-0000-0000-000000000004';
  v_timesheet uuid:='b4000000-0000-0000-0000-000000000005';
  v_capabilities jsonb;
  v_placement jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate expense admission runtime client');

  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'expense-admission-runtime@example.test',true,'GCK-EXPENSE-ADMISSION');

  -- Direct fixture insertion deliberately reproduces a historical Client row
  -- that predates the Office create/update validation.
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,is_nhsp,
    candidate_expenses_require_separate_timesheet,candidate_expense_invoice_email
  ) values(
    gen_random_uuid(),v_client,current_date-1,'ELECTRONIC',true,true,null
  );

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,weekly_timesheet_source,
    candidate_expenses_require_separate_timesheet_override
  ) values(
    v_contract,v_candidate,v_client,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'ELECTRONIC','PAYE','NHSP',null
  );

  insert into public.timesheets(
    timesheet_id,booking_id,contract_id,week_ending_date,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    line_type,submission_mode,sheet_scope
  ) values(
    v_timesheet,'EXPENSE-ADMISSION-'||replace(v_timesheet::text,'-',''),v_contract,current_date,
    'candidate','test hospital','test ward','test role',
    'HOURS','MANUAL','WEEKLY'
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
  ) values(v_week,v_contract,current_date,'OPEN','ELECTRONIC',v_timesheet);

  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status
  ) values(v_timesheet,1,v_candidate,v_client,8,'UNPROCESSED');

  v_capabilities:=private._candidate_record_capabilities_v1(
    v_timesheet,v_week,'{}'::jsonb
  );

  if not (coalesce(v_capabilities->'reason_codes','[]'::jsonb) ? 'EXPENSE_INVOICE_EMAIL_REQUIRED')
     or coalesce((v_capabilities->>'candidate_expenses_allowed')::boolean,true)
     or coalesce((v_capabilities->>'can_edit_expenses')::boolean,true) then
    raise exception 'missing Expense Invoice Email was not blocked at capability admission: %',v_capabilities;
  end if;

  v_placement:=public.expense_placement_resolve_v1(
    v_candidate,'TEST',v_timesheet,v_week,'{}'::jsonb,now()
  );
  if v_placement->>'placement'<>'BLOCKED'
     or v_placement->>'reason_code'<>'EXPENSE_INVOICE_EMAIL_REQUIRED' then
    raise exception 'missing Expense Invoice Email did not preserve its exact placement reason: %',v_placement;
  end if;

  update public.client_settings
  set candidate_expense_invoice_email='expenses@example.test'
  where client_id=v_client;

  v_capabilities:=private._candidate_record_capabilities_v1(
    v_timesheet,v_week,'{}'::jsonb
  );
  if coalesce(v_capabilities->'reason_codes','[]'::jsonb) ? 'EXPENSE_INVOICE_EMAIL_REQUIRED'
     or not coalesce((v_capabilities->>'candidate_expenses_allowed')::boolean,false)
     or not coalesce((v_capabilities->>'can_edit_expenses')::boolean,false) then
    raise exception 'configured separated expense claim did not remain available: %',v_capabilities;
  end if;

  v_placement:=public.expense_placement_resolve_v1(
    v_candidate,'TEST',v_timesheet,v_week,'{}'::jsonb,now()
  );
  if v_placement->>'placement'<>'CREATE_CARRIER'
     or v_placement->>'reason_code'<>'NO_SAFE_CARRIER' then
    raise exception 'configured separated expense claim did not retain carrier placement: %',v_placement;
  end if;
end;
$candidate_expense_email_admission$;

rollback;
