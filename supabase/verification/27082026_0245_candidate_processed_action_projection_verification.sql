-- Rollback-contained proof that a submitted/processed Candidate timesheet cannot be re-entered.
-- The same immutable hours may still anchor a separately allocated expense claim when policy permits.
\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_client_id uuid:='00000000-0000-4000-8000-00000000f101';
  v_candidate_id uuid:='00000000-0000-4000-8000-00000000f102';
  v_contract_id uuid:='00000000-0000-4000-8000-00000000f103';
  v_week_id uuid:='00000000-0000-4000-8000-00000000f104';
  v_timesheet_id uuid:='00000000-0000-4000-8000-00000000f105';
  v_planned_week_id uuid:='00000000-0000-4000-8000-00000000f106';
  v_worked_week_id uuid:='00000000-0000-4000-8000-00000000f107';
  v_worked_timesheet_id uuid:='00000000-0000-4000-8000-00000000f108';
  v_capabilities jsonb;
  v_actions jsonb;
  v_definition text;
begin
  insert into public.clients(id,name) values(v_client_id,'Candidate no-work verification Client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate_id,'processed-action-proof@example.invalid',true,'CID1-PROCESSED-PROOF');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,
    candidate_expenses_require_separate_timesheet
  ) values(
    gen_random_uuid(),v_client_id,current_date-1,'ELECTRONIC',false
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot
  ) values(
    v_contract_id,v_candidate_id,v_client_id,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'ELECTRONIC','PAYE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(
    v_planned_week_id,v_contract_id,current_date-7,0,'PLANNED','ELECTRONIC',null
  );

  v_capabilities:=private._candidate_record_capabilities_v1(
    null,v_planned_week_id,'{}'::jsonb
  );
  if v_capabilities->'candidate_mutation_locked'<>'false'::jsonb
     or v_capabilities->'candidate_hours_submission_allowed'<>'true'::jsonb
     or v_capabilities->'can_edit_hours'<>'true'::jsonb then
    raise exception 'CANDIDATE_PLANNED_CAPABILITY_PROJECTION_INVALID: %',v_capabilities;
  end if;
  v_actions:=private._candidate_timesheet_action_contract_v1(
    'PLANNED','[]'::jsonb,v_capabilities,null,v_planned_week_id,clock_timestamp()
  );
  if v_actions#>>'{primary_action,code}'<>'ENTER_TIMESHEET'
     or not exists(
       select 1 from jsonb_array_elements(coalesce(v_actions->'available_actions','[]'::jsonb)) item
       where item->>'code'='ENTER_TIMESHEET' and coalesce((item->>'enabled')::boolean,false)
     ) then
    raise exception 'CANDIDATE_PLANNED_ACTION_PROJECTION_INVALID: %',v_actions;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,
    r2_nurse_key,r2_auth_key,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_worked_timesheet_id,v_contract_id,current_date-14,'HOURS','ELECTRONIC','WEEKLY',
    'verification/worked-candidate','verification/worked-manager','CANDIDATE-NO-WORK-WORKED',
    'GCK-NO-WORK-WORKED','VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(
    v_worked_week_id,v_contract_id,current_date-14,0,'OPEN','ELECTRONIC',v_worked_timesheet_id
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    total_hours,processing_status,authorised_at_utc
  ) values(
    v_worked_timesheet_id,1,v_candidate_id,v_client_id,true,'CONTRACT_WEEKLY',
    8,'UNPROCESSED',null
  );
  v_capabilities:=private._candidate_record_capabilities_v1(
    v_worked_timesheet_id,v_worked_week_id,'{}'::jsonb
  );
  if coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,true) then
    raise exception 'CANDIDATE_WORKED_WEEK_NO_WORK_CAPABILITY_INVALID: %',v_capabilities;
  end if;
  v_actions:=private._candidate_timesheet_action_contract_v1(
    'OPEN','[]'::jsonb,v_capabilities,v_worked_timesheet_id,v_worked_week_id,clock_timestamp()
  );
  if exists(
    select 1 from jsonb_array_elements(coalesce(v_actions->'available_actions','[]'::jsonb)) item
    where item->>'code'='NO_WORK_THIS_WEEK'
  ) then
    raise exception 'CANDIDATE_WORKED_WEEK_NO_WORK_ACTION_INVALID: %',v_actions;
  end if;

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,
    r2_nurse_key,r2_auth_key,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm
  ) values(
    v_timesheet_id,v_contract_id,current_date,'HOURS','ELECTRONIC','WEEKLY',
    'verification/candidate','verification/manager','CANDIDATE-NO-WORK-PROCESSED',
    'GCK-NO-WORK-PROCESSED','VERIFICATION HOSPITAL','VERIFICATION WARD','VERIFICATION ROLE'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(
    v_week_id,v_contract_id,current_date,0,'SUBMITTED','ELECTRONIC',v_timesheet_id
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,is_current,basis,
    total_hours,processing_status,authorised_at_utc
  ) values(
    v_timesheet_id,1,v_candidate_id,v_client_id,true,'CONTRACT_WEEKLY',
    8,'PENDING_AUTH',null
  );

  v_capabilities:=private._candidate_record_capabilities_v1(
    v_timesheet_id,v_week_id,'{}'::jsonb
  );

  if not coalesce((v_capabilities->>'candidate_mutation_locked')::boolean,false)
     or coalesce((v_capabilities->>'can_edit_hours')::boolean,true)
     or coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,true)
     or not coalesce((v_capabilities->>'can_edit_expenses')::boolean,false) then
    raise exception 'CANDIDATE_PROCESSED_CAPABILITY_PROJECTION_INVALID: %',v_capabilities;
  end if;

  v_actions:=private._candidate_timesheet_action_contract_v1(
    'PENDING_AUTH','[]'::jsonb,v_capabilities,v_timesheet_id,v_week_id,clock_timestamp()
  );

  if v_actions#>>'{primary_action,code}'<>'ADD_EXPENSES'
     or exists(
       select 1 from jsonb_array_elements(coalesce(v_actions->'available_actions','[]'::jsonb)) item
       where item->>'code' in ('ENTER_TIMESHEET','NO_WORK_THIS_WEEK')
     )
     or not exists(
       select 1 from jsonb_array_elements(coalesce(v_actions->'available_actions','[]'::jsonb)) item
       where item->>'code'='ADD_EXPENSES' and coalesce((item->>'enabled')::boolean,false)
     ) then
    raise exception 'CANDIDATE_PROCESSED_ACTION_PROJECTION_INVALID: %',v_actions;
  end if;

  select pg_catalog.pg_get_functiondef(proc.oid)
  into strict v_definition
  from pg_catalog.pg_proc proc
  join pg_catalog.pg_namespace namespace on namespace.oid=proc.pronamespace
  where namespace.nspname='private'
    and proc.proname='_candidate_record_capabilities_v1'
    and pg_catalog.pg_get_function_identity_arguments(proc.oid)=
      'p_timesheet_id uuid, p_contract_week_id uuid, p_proposed_claim jsonb';

  if pg_catalog.strpos(v_definition,'PENDING_AUTH')=0
     or pg_catalog.strpos(v_definition,'READY_FOR_HR')=0
     or pg_catalog.strpos(v_definition,'READY_FOR_INVOICE')=0
     or pg_catalog.strpos(lower(v_definition),'pg_catalog.coalesce')>0
     or pg_catalog.strpos(lower(v_definition),'pg_catalog.nullif')>0
     or pg_catalog.strpos(lower(v_definition),'pg_catalog.least')>0
     or pg_catalog.strpos(lower(v_definition),'pg_catalog.greatest')>0 then
    raise exception 'CANDIDATE_PROCESSED_CAPABILITY_DEFINITION_INVALID';
  end if;

  if pg_catalog.has_function_privilege(
       'service_role','private._candidate_record_capabilities_v1(uuid,uuid,jsonb)','EXECUTE')
     or pg_catalog.has_function_privilege(
       'anon','private._candidate_record_capabilities_v1(uuid,uuid,jsonb)','EXECUTE')
     or pg_catalog.has_function_privilege(
       'authenticated','private._candidate_record_capabilities_v1(uuid,uuid,jsonb)','EXECUTE') then
    raise exception 'CANDIDATE_PROCESSED_CAPABILITY_HELPER_EXPOSED';
  end if;
end;
$verification$;

rollback;
