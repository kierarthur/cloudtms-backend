-- Rollback-contained first-use proof for Candidate Daily withdrawal/rejection reset.
do $candidate_submission_withdrawal_boundary_verification$
declare
  v_read_definition text;
  v_transition_definition text;
  v_bootstrap_config text[];
  v_page_definition text;
  v_detail_definition text;
begin
  select pg_get_functiondef(
    'private._candidate_timesheet_action_contract_v1(text,jsonb,jsonb,uuid,uuid,timestamptz)'::regprocedure
  ) into v_read_definition;
  select pg_get_functiondef(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_transition_definition;
  select proconfig into v_bootstrap_config
  from pg_proc
  where oid='public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz)'::regprocedure;
  select pg_get_functiondef(
    'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)'::regprocedure
  ) into v_page_definition;
  select pg_get_functiondef(
    'public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz)'::regprocedure
  ) into v_detail_definition;

  if position('''FINALISED''' in v_read_definition)=0
     or position('not in (''PAID'',''AUTHORISED'',''INVOICED_NOT_PAID'')' in v_read_definition)=0
     or position('v_action=''SUPERSEDE'' and v_workflow.state=''FINALISED''' in v_transition_definition)=0
     or position('v_action=''CANCEL''' in v_transition_definition)=0
     or position('_candidate_weekly_withdrawal_reset_v1' in v_transition_definition)=0
     or position('_candidate_daily_submission_reset_v1' in v_transition_definition)=0
     or v_bootstrap_config is distinct from array['search_path=""']::text[]
     or position('draft_week.id' in v_page_definition)=0
     or position('final_signed_document_ready' in v_detail_definition)=0 then
    raise exception 'CANDIDATE_SUBMISSION_WITHDRAWAL_BOUNDARY_INCOMPLETE';
  end if;
end;
$candidate_submission_withdrawal_boundary_verification$;

begin;

do $candidate_daily_withdrawal_reset_verification$
declare
  v_now timestamptz:='2026-08-27 12:27:00+00';
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_timesheet uuid:=gen_random_uuid();
  v_result jsonb;
  v_new_timesheet uuid;
  v_system_actor uuid;
begin
  select candidate_app_system_actor_user_id into v_system_actor
  from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_DAILY_WITHDRAWAL_SYSTEM_ACTOR_MISSING';
  end if;
  insert into public.clients(id,name)
  values(v_client,'Candidate Daily withdrawal verification client');
  insert into public.candidates(id,email,active,key_norm)
  values(
    v_candidate,
    'daily-withdrawal-'||replace(v_candidate::text,'-','')||'@example.test',
    true,
    'DAILY-WITHDRAWAL-'||replace(v_candidate::text,'-','')
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot
  ) values(
    v_contract,v_candidate,v_client,current_date-30,current_date+30,2,'ELECTRONIC','PAYE'
  );
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    version,is_current,status,contract_id,week_ending_date,line_type,sheet_scope,
    submission_mode,scheduled_start_iso,scheduled_end_iso,worked_start_iso,
    worked_end_iso,break_start_iso,break_end_iso,break_minutes,worked_minutes,
    candidate_submission_route_intent
  ) values(
    v_timesheet,'DAILY-WITHDRAWAL-'||v_timesheet::text,
    'DAILY-WITHDRAWAL-VERIFICATION','VERIFICATION-HOSPITAL','VERIFICATION-WARD',
    'VERIFICATION-JOB',1,true,'RECEIVED',v_contract,current_date,
    'HOURS','DAILY','MANUAL',
    '2026-08-27 08:00:00+01','2026-08-27 18:00:00+01',
    '2026-08-27 08:00:00+01','2026-08-27 17:00:00+01',
    '2026-08-27 12:00:00+01','2026-08-27 12:30:00+01',30,510,
    'ELECTRONIC'
  );
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    hours_day,processing_status,worked_start_iso,worked_end_iso,
    break_start_iso,break_end_iso,break_minutes
  ) values(
    v_timesheet,1,v_candidate,v_client,8.5,8.5,'UNPROCESSED',
    '2026-08-27 08:00:00+01','2026-08-27 17:00:00+01',
    '2026-08-27 12:00:00+01','2026-08-27 12:30:00+01',30
  );

  v_result:=private._candidate_daily_submission_reset_v1(
    v_timesheet,v_timesheet,'I entered the wrong Daily shift.',v_candidate,
    'CANDIDATE_WITHDRAWN',v_now
  );
  v_new_timesheet:=(v_result->>'current_timesheet_id')::uuid;

  if not coalesce((v_result->>'reset')::boolean,false)
     or v_result->>'scope'<>'DAILY'
     or v_result->>'event'<>'CANDIDATE_WITHDRAWN'
     or v_new_timesheet is null
     or v_new_timesheet=v_timesheet
     or v_result->>'draft_submission_mode'<>'MANUAL'
     or v_result->>'effective_submission_mode'<>'ELECTRONIC'
     or (select is_current from public.timesheets where timesheet_id=v_timesheet)
     or (select status from public.timesheets where timesheet_id=v_timesheet)<>'REVOKED'
     or not coalesce((select is_current from public.timesheets where timesheet_id=v_new_timesheet),false)
     or (select status from public.timesheets where timesheet_id=v_new_timesheet)<>'RECEIVED'
     or (select sheet_scope from public.timesheets where timesheet_id=v_new_timesheet)<>'DAILY'
     or (select submission_mode from public.timesheets where timesheet_id=v_new_timesheet)<>'MANUAL'
     or (select candidate_submission_route_intent from public.timesheets where timesheet_id=v_new_timesheet)<>'ELECTRONIC'
     or (select scheduled_start_iso from public.timesheets where timesheet_id=v_new_timesheet)
        is distinct from '2026-08-27 08:00:00+01'::timestamptz
     or (select scheduled_end_iso from public.timesheets where timesheet_id=v_new_timesheet)
        is distinct from '2026-08-27 18:00:00+01'::timestamptz
     or (select worked_start_iso from public.timesheets where timesheet_id=v_new_timesheet) is not null
     or (select worked_end_iso from public.timesheets where timesheet_id=v_new_timesheet) is not null
     or (select break_minutes from public.timesheets where timesheet_id=v_new_timesheet) is not null
     or (select worked_minutes from public.timesheets where timesheet_id=v_new_timesheet) is not null
     or (select processing_status from public.timesheets_financials where timesheet_id=v_new_timesheet)<>'UNASSIGNED'
     or (select total_hours from public.timesheets_financials where timesheet_id=v_new_timesheet)<>0
     or (select total_hours from public.timesheets_financials where timesheet_id=v_timesheet)<>8.5
     or exists(select 1 from public.contract_weeks where timesheet_id=v_new_timesheet)
     or exists(
       select 1 from public.ts_financials_outbox
       where timesheet_id=v_new_timesheet
     )
     or not exists(
       select 1 from public.audit_events
       where object_type='timesheet'
         and object_id_text=v_new_timesheet::text
         and action='CANDIDATE_DAILY_SUBMISSION_WITHDRAWN_VERSION_ROTATED'
         and actor_user_id=v_system_actor
     ) then
    raise exception 'CANDIDATE_DAILY_WITHDRAWAL_RESET_VERIFICATION_FAILED: %',v_result;
  end if;

  update public.timesheets_financials
  set authorised_at_utc=v_now+interval '30 seconds'
  where timesheet_id=v_new_timesheet and is_current=true;
  begin
    perform private._candidate_daily_submission_reset_v1(
      v_new_timesheet,v_new_timesheet,'Protected Daily history.',v_candidate,
      'CANDIDATE_WITHDRAWN',v_now+interval '1 minute'
    );
    raise exception 'CANDIDATE_DAILY_PROTECTED_HISTORY_WAS_RESET';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_DAILY_RESET_PROTECTED_HISTORY' then
      raise;
    end if;
  end;
end;
$candidate_daily_withdrawal_reset_verification$;

rollback;
