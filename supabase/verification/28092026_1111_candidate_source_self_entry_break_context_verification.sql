\set ON_ERROR_STOP on

-- Portable, rollback-contained first-use proof. Candidate-reported breaks are
-- evidence on source-authoritative weeks; they do not alter source authority.
begin;

do $candidate_source_break_verification$
declare
  v_date date := '2026-09-27';
  v_candidate uuid := gen_random_uuid();
  v_client uuid := gen_random_uuid();
  v_contract uuid := gen_random_uuid();
  v_week uuid := gen_random_uuid();
  v_result jsonb;
  v_capabilities jsonb := jsonb_build_object(
    'candidate_source_self_entry_allowed',true,
    'import_authoritative',true,
    'route_family','IMPORT_AUTHORITATIVE',
    'protected',false,
    'candidate_mutation_locked',false,
    'can_edit_hours',false
  );
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate source break verification Client');

  insert into public.candidates(id,email,display_name,active,key_norm)
  values(v_candidate,
    'source-break-'||replace(v_candidate::text,'-','')||'@example.test',
    'Candidate Source Break Verification',true,
    'SOURCE-BREAK-'||replace(v_candidate::text,'-',''));

  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    is_nhsp,timesheet_break_entry_mode
  ) values(gen_random_uuid(),v_client,v_date-30,'ELECTRONIC',0,
    true,'START_END_TIMES');

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role
  ) values(v_contract,v_candidate,v_client,v_date-30,v_date+30,0,
    'ELECTRONIC','PAYE','RMN');

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot
  ) values(v_week,v_contract,v_date,'OPEN','ELECTRONIC');

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities);
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'START_END_TIMES'
     or v_result#>>'{source}' is distinct from 'CLIENT_SETTINGS'
     or v_result#>>'{reason}' is distinct from 'CANDIDATE_SOURCE_SELF_ENTRY'
     or v_result#>>'{context_token}' !~ '^[a-f0-9]{64}$' then
    raise exception 'SOURCE_BREAK_SELF_ENTRY_NOT_EXPOSED:%',v_result;
  end if;

  update public.client_settings set timesheet_break_entry_mode='DURATION_MINUTES'
  where client_id=v_client;
  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities);
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'DURATION_MINUTES'
     or v_result#>>'{source}' is distinct from 'CLIENT_SETTINGS' then
    raise exception 'SOURCE_BREAK_DURATION_MODE_LOST:%',v_result;
  end if;

  update public.contracts
  set overrideclientsettings=true,timesheet_break_entry_mode='START_END_TIMES'
  where id=v_contract;
  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities);
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'START_END_TIMES'
     or v_result#>>'{source}' is distinct from 'CONTRACT_OVERRIDE' then
    raise exception 'SOURCE_BREAK_CONTRACT_OVERRIDE_LOST:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities||'{"candidate_source_self_entry_allowed":false}'::jsonb);
  if v_result#>>'{applicable}' is distinct from 'false'
     or v_result#>'{mode}' is distinct from 'null'::jsonb then
    raise exception 'SOURCE_BREAK_UNADMITTED_EXPOSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities||'{"protected":true}'::jsonb);
  if v_result#>>'{applicable}' is distinct from 'false' then
    raise exception 'SOURCE_BREAK_PROTECTED_EXPOSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities||'{"candidate_mutation_locked":true}'::jsonb);
  if v_result#>>'{applicable}' is distinct from 'false' then
    raise exception 'SOURCE_BREAK_LOCKED_EXPOSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_date,v_capabilities||'{"route_family":"ELECTRONIC"}'::jsonb);
  if v_result#>>'{applicable}' is distinct from 'false' then
    raise exception 'SOURCE_BREAK_OTHER_ROUTE_EXPOSED:%',v_result;
  end if;
end
$candidate_source_break_verification$;

rollback;

select 'PASS'::text as candidate_source_self_entry_break_context_verification;
