\set ON_ERROR_STOP on

-- Rollback-contained first-use proof that an editable Candidate printed-document
-- Timesheet receives the same server-owned break-entry mode as an editable
-- electronic Timesheet. Protected/import-authoritative and unrelated routes
-- remain inapplicable.
begin;

do $candidate_paper_break_entry_verification$
declare
  v_as_of date:='2026-08-30';
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_result jsonb;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate printed break verification Client');

  insert into public.candidates(id,email,display_name,active,key_norm)
  values(
    v_candidate,
    'paper-break-'||replace(v_candidate::text,'-','')||'@example.test',
    'Candidate Printed Break Verification',
    true,
    'PAPER-BREAK-'||replace(v_candidate::text,'-','')
  );

  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    timesheet_break_entry_mode
  ) values(
    gen_random_uuid(),v_client,v_as_of-30,'ELECTRONIC',0,'DURATION_MINUTES'
  );

  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role
  ) values(
    v_contract,v_candidate,v_client,v_as_of-30,v_as_of+30,0,'ELECTRONIC','PAYE','RMN'
  );

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot
  ) values(v_week,v_contract,v_as_of,'OPEN','ELECTRONIC');

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','PAPER'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'DURATION_MINUTES'
     or v_result#>>'{source}' is distinct from 'CLIENT_SETTINGS'
     or v_result#>>'{context_version}' is distinct from 'CANDIDATE_BREAK_ENTRY_V1'
     or v_result#>>'{context_token}' !~ '^[a-f0-9]{64}$' then
    raise exception 'CANDIDATE_PAPER_BREAK_ENTRY_NOT_EXPOSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','QR'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'DURATION_MINUTES'
     or v_result#>>'{source}' is distinct from 'CLIENT_SETTINGS' then
    raise exception 'CANDIDATE_QR_BREAK_ENTRY_NOT_EXPOSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','ELECTRONIC'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'true'
     or v_result#>>'{mode}' is distinct from 'DURATION_MINUTES' then
    raise exception 'CANDIDATE_ELECTRONIC_BREAK_ENTRY_REGRESSED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',true,
      'route_family','PAPER'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'false'
     or v_result#>'{mode}' is distinct from 'null'::jsonb
     or v_result#>>'{reason}' is distinct from 'IMPORT_AUTHORITATIVE' then
    raise exception 'CANDIDATE_IMPORT_BREAK_ENTRY_BROADENED:%',v_result;
  end if;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','PHONE'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'false'
     or v_result#>'{mode}' is distinct from 'null'::jsonb
     or v_result#>>'{reason}' is distinct from 'NON_ELECTRONIC_ROUTE' then
    raise exception 'CANDIDATE_UNRELATED_ROUTE_BREAK_ENTRY_BROADENED:%',v_result;
  end if;
end
$candidate_paper_break_entry_verification$;

rollback;

select 'PASS'::text as candidate_paper_break_entry_verification;
