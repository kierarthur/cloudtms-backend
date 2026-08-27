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
  v_capabilities jsonb;
  v_actions jsonb;
  v_definition text;
begin
  insert into public.clients(id) values(v_client_id);
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
    default_submission_mode
  ) values(
    v_contract_id,v_candidate_id,v_client_id,current_date-60,current_date+60,
    extract(dow from current_date)::integer,'ELECTRONIC'
  );
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,
    r2_nurse_key,r2_auth_key
  ) values(
    v_timesheet_id,v_contract_id,current_date,'HOURS','ELECTRONIC','WEEKLY',
    'verification/candidate','verification/manager'
  );
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values(
    v_week_id,v_contract_id,current_date,0,'SUBMITTED','ELECTRONIC',v_timesheet_id
  );
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status,authorised_at_utc
  ) values(
    v_timesheet_id,v_candidate_id,v_client_id,8,'PENDING_AUTH',null
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

  if pg_catalog.position('PENDING_AUTH' in v_definition)=0
     or pg_catalog.position('READY_FOR_HR' in v_definition)=0
     or pg_catalog.position('READY_FOR_INVOICE' in v_definition)=0
     or pg_catalog.position('pg_catalog.coalesce' in lower(v_definition))>0
     or pg_catalog.position('pg_catalog.nullif' in lower(v_definition))>0
     or pg_catalog.position('pg_catalog.least' in lower(v_definition))>0
     or pg_catalog.position('pg_catalog.greatest' in lower(v_definition))>0 then
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
