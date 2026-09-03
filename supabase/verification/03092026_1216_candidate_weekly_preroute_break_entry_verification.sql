\set ON_ERROR_STOP on

-- Rollback-contained proof that Weekly break entry is visible for both
-- pre-route capability shapes (omitted and MANUAL_NON_QR), retains the
-- identical context after Electronic/Paper/QR selection, and remains closed
-- for import-authoritative or unrelated routes. It also reproduces the live
-- stale editable NHSP Weekly shape and proves the resolver still suppresses
-- the hours/break editor because Dedicated NHSP Weekly is authoritative.
begin;

do $candidate_weekly_preroute_break_entry_verification$
declare
  v_as_of date:='2026-09-03';
  v_candidate uuid:=gen_random_uuid();
  v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week uuid:=gen_random_uuid();
  v_pre_route jsonb;
  v_projected_pre_route jsonb;
  v_selected jsonb;
  v_result jsonb;
  v_route text;
begin
  insert into public.clients(id,name)
  values(v_client,'Candidate Weekly pre-route break verification Client');

  insert into public.candidates(id,email,display_name,active,key_norm)
  values(
    v_candidate,
    'weekly-preroute-break-'||replace(v_candidate::text,'-','')||'@example.test',
    'Candidate Weekly Pre-route Break Verification',
    true,
    'WEEKLY-PREROUTE-BREAK-'||replace(v_candidate::text,'-','')
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

  v_pre_route:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object('can_edit_hours',true,'import_authoritative',false)
  );
  if v_pre_route#>>'{applicable}' is distinct from 'true'
     or v_pre_route#>>'{mode}' is distinct from 'DURATION_MINUTES'
     or v_pre_route#>>'{source}' is distinct from 'CLIENT_SETTINGS'
     or v_pre_route#>>'{reason}' is distinct from 'CANDIDATE_EDITABLE_ELECTRONIC'
     or v_pre_route#>>'{context_token}' !~ '^[a-f0-9]{64}$' then
    raise exception 'CANDIDATE_PRE_ROUTE_BREAK_ENTRY_NOT_EXPOSED:%',v_pre_route;
  end if;

  -- candidate_app_timesheet_detail_v1 currently projects the editable,
  -- not-yet-selected Weekly route as MANUAL_NON_QR. This exact shape must be
  -- covered because it is what the physical app receives.
  v_projected_pre_route:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','MANUAL_NON_QR'
    )
  );
  if v_projected_pre_route#>>'{applicable}' is distinct from 'true'
     or v_projected_pre_route#>>'{mode}' is distinct from 'DURATION_MINUTES'
     or v_projected_pre_route#>>'{source}' is distinct from 'CLIENT_SETTINGS'
     or v_projected_pre_route#>>'{reason}' is distinct from 'CANDIDATE_EDITABLE_ELECTRONIC'
     or v_projected_pre_route#>>'{context_token}' is distinct from v_pre_route#>>'{context_token}' then
    raise exception 'CANDIDATE_PROJECTED_PRE_ROUTE_BREAK_ENTRY_NOT_EXPOSED:%',
      v_projected_pre_route;
  end if;

  foreach v_route in array array['ELECTRONIC','PAPER','QR']
  loop
    v_selected:=private._candidate_break_entry_context_core_v1(
      null,v_week,v_as_of,
      jsonb_build_object(
        'can_edit_hours',true,
        'import_authoritative',false,
        'route_family',v_route
      )
    );
    if v_selected#>>'{applicable}' is distinct from 'true'
       or v_selected#>>'{mode}' is distinct from 'DURATION_MINUTES'
       or v_selected#>>'{context_token}' is distinct from v_pre_route#>>'{context_token}' then
      raise exception 'CANDIDATE_SELECTED_ROUTE_BREAK_ENTRY_DRIFT:%:%',v_route,v_selected;
    end if;
  end loop;

  update public.client_settings
  set is_nhsp=true
  where client_id=v_client;
  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','ELECTRONIC'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'false'
     or v_result#>'{mode}' is distinct from 'null'::jsonb
     or v_result#>>'{source}' is distinct from 'NOT_APPLICABLE'
     or v_result#>>'{reason}' is distinct from 'IMPORT_AUTHORITATIVE' then
    raise exception 'CANDIDATE_DEDICATED_NHSP_WEEKLY_BREAK_ENTRY_EXPOSED:%',v_result;
  end if;

  update public.client_settings
  set is_nhsp=false
  where client_id=v_client;

  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',true,
      'route_family','ELECTRONIC'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'false'
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
     or v_result#>>'{reason}' is distinct from 'NON_ELECTRONIC_ROUTE' then
    raise exception 'CANDIDATE_UNRELATED_ROUTE_BREAK_ENTRY_BROADENED:%',v_result;
  end if;

  update public.client_settings
  set autoprocess_hr=true,requires_hr=false,no_timesheet_required=true
  where client_id=v_client;
  v_result:=private._candidate_break_entry_context_core_v1(
    null,v_week,v_as_of,
    jsonb_build_object(
      'can_edit_hours',true,
      'import_authoritative',false,
      'route_family','ELECTRONIC'
    )
  );
  if v_result#>>'{applicable}' is distinct from 'false'
     or v_result#>>'{reason}' is distinct from 'IMPORT_AUTHORITATIVE' then
    raise exception 'CANDIDATE_AUTHORITATIVE_ROSTER_BREAK_ENTRY_BROADENED:%',v_result;
  end if;
end
$candidate_weekly_preroute_break_entry_verification$;

rollback;

select 'PASS'::text as candidate_weekly_preroute_break_entry_verification;
