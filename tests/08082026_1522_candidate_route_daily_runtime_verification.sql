-- Candidate route-family, DAILY entitlement/date, issue and save/recalculate verification.
-- Run only in a disposable database after the complete Candidate App install bundle.
-- Every schema/data change in this file is rolled back.

begin;

create table if not exists public.ts_financials_outbox (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid not null,
  reason public.ts_fin_reason_enum not null,
  created_at timestamptz not null default now()
);

create or replace function public.enqueue_ts_financials_priority(
  _timesheet_ids uuid[],
  _reason public.ts_fin_reason_enum
)
returns integer
language plpgsql
as $function$
declare
  v_id uuid;
  v_count integer:=0;
begin
  foreach v_id in array coalesce(_timesheet_ids,array[]::uuid[]) loop
    insert into public.ts_financials_outbox(timesheet_id,reason) values(v_id,_reason);
    v_count:=v_count+1;
  end loop;
  return v_count;
end;
$function$;

create or replace function public.tsfin_load_context_batch(p_timesheet_ids uuid[])
returns table(
  effective_timesheet_id uuid,
  out_timesheet jsonb,
  out_cur_fin jsonb,
  out_candidate jsonb,
  out_umbrella jsonb,
  out_client_id uuid,
  out_effective_flags jsonb,
  out_policy jsonb
)
language sql
stable
as $function$
  select t.timesheet_id,
    to_jsonb(t),
    to_jsonb(f),
    jsonb_build_object(
      'id',f.candidate_id,
      'pay_method','PAYE',
      'roles',jsonb_build_array(jsonb_build_object('code',t.job_title_norm)),
      'sort_code','000000',
      'account_number','00000000'
    ),
    '{}'::jsonb,
    f.client_id,
    jsonb_build_object(
      'hr_validation_required_for_invoice',(t.booking_id like 'HR-%'),
      'validation_status',case when t.booking_id like 'HR-%' then 'PENDING' else '' end
    ),
    jsonb_build_object(
      'timezone_id','Europe/London',
      'day_start','06:00','day_end','20:00',
      'sat_start','00:00','sat_end','00:00',
      'sun_start','00:00','sun_end','00:00',
      'bh_start','00:00','bh_end','00:00','bh_list','[]'::jsonb,
      'erni_pct',13.8,'apply_erni_to','PAYE_ONLY'
    )
  from public.timesheets t
  join public.timesheets_financials f on f.timesheet_id=t.timesheet_id and f.is_current=true
  where t.timesheet_id=any(p_timesheet_ids) and t.is_current=true;
$function$;

create or replace function public.tsfin_resolve_rates_batch(p_items jsonb)
returns table(
  k text,candidate_id uuid,client_id uuid,role text,band text,date_ymd date,rate_type text,
  source_kind text,override_id uuid,default_id uuid,
  pay_day numeric,pay_night numeric,pay_sat numeric,pay_sun numeric,pay_bh numeric,
  charge_day numeric,charge_night numeric,charge_sat numeric,charge_sun numeric,charge_bh numeric
)
language sql
stable
as $function$
  select item->>'k',(item->>'candidate_id')::uuid,(item->>'client_id')::uuid,
    item->>'role',item->>'band',(item->>'date')::date,item->>'rate_type',
    'CLIENT_DEFAULT',null::uuid,null::uuid,
    10::numeric,10::numeric,10::numeric,10::numeric,10::numeric,
    20::numeric,20::numeric,20::numeric,20::numeric,20::numeric
  from jsonb_array_elements(coalesce(p_items,'[]'::jsonb)) item;
$function$;

create or replace function public.tsfin_write_snapshots_and_complete(p_rows jsonb)
returns table(ok_count integer,fail_count integer,errors jsonb)
language plpgsql
as $function$
declare
  v_row jsonb;
  v_snapshot jsonb;
begin
  ok_count:=0;
  fail_count:=0;
  errors:='[]'::jsonb;
  for v_row in select value from jsonb_array_elements(coalesce(p_rows,'[]'::jsonb)) loop
    v_snapshot:=v_row->'snapshot';
    update public.timesheets_financials set
      processing_status=coalesce((v_snapshot->>'processing_status')::public.ts_fin_processing_status_enum,processing_status),
      worked_start_iso=nullif(v_snapshot->>'worked_start_iso','')::timestamptz,
      worked_end_iso=nullif(v_snapshot->>'worked_end_iso','')::timestamptz,
      break_start_iso=nullif(v_snapshot->>'break_start_iso','')::timestamptz,
      break_end_iso=nullif(v_snapshot->>'break_end_iso','')::timestamptz,
      break_minutes=nullif(v_snapshot->>'break_minutes','')::integer,
      role=v_snapshot->>'role',band=v_snapshot->>'band',pay_method=v_snapshot->>'pay_method',
      has_rate_issue=coalesce((v_snapshot->>'has_rate_issue')::boolean,false),
      has_pay_channel_issue=coalesce((v_snapshot->>'has_pay_channel_issue')::boolean,false),
      hours_day=coalesce((v_snapshot->>'hours_day')::numeric,0),
      hours_night=coalesce((v_snapshot->>'hours_night')::numeric,0),
      hours_sat=coalesce((v_snapshot->>'hours_sat')::numeric,0),
      hours_sun=coalesce((v_snapshot->>'hours_sun')::numeric,0),
      hours_bh=coalesce((v_snapshot->>'hours_bh')::numeric,0),
      total_hours=coalesce((v_snapshot->>'total_hours')::numeric,0),
      total_pay_ex_vat=coalesce((v_snapshot->>'total_pay_ex_vat')::numeric,0),
      total_charge_ex_vat=coalesce((v_snapshot->>'total_charge_ex_vat')::numeric,0),
      margin_ex_vat=coalesce((v_snapshot->>'margin_ex_vat')::numeric,0),
      policy_snapshot_json=coalesce(v_snapshot->'policy_snapshot_json','{}'::jsonb),
      rate_source_refs_json=coalesce(v_snapshot->'rate_source_refs_json','{}'::jsonb),
      invoice_breakdown_json=coalesce(v_snapshot->'invoice_breakdown_json','{}'::jsonb),
      actual_schedule_json=v_snapshot->'actual_schedule_json',
      actual_minutes_by_day_json=v_snapshot->'actual_minutes_by_day_json',
      computed_at_utc=now(),updated_at=now()
    where timesheet_id=(v_row->>'timesheet_id')::uuid and is_current=true;
    if found then ok_count:=ok_count+1; else fail_count:=fail_count+1; end if;
    delete from public.ts_financials_outbox where id=(v_row->>'outbox_id')::uuid;
  end loop;
  return next;
end;
$function$;

create or replace view public.v_timesheets_summary as
select t.timesheet_id,
  (t.booking_id like 'HR-%')::boolean as client_hr_validation_required,
  false::boolean as client_no_timesheet_required,
  case
    when t.booking_id like 'IMPORT-%' then 'IMPORT'
    when t.qr_token is not null or t.qr_status is not null then 'QR'
    when t.submission_mode = 'ELECTRONIC'::public.submission_mode_enum then 'ELECTRONIC'
    else 'MANUAL_NON_QR'
  end::text as route_type,
  t.contract_id,
  cw.id as contract_week_id,
  null::text as validation_status
from public.timesheets t
left join lateral (
  select cw0.id
  from public.contract_weeks cw0
  where cw0.timesheet_id = t.timesheet_id
  order by cw0.updated_at desc, cw0.created_at desc
  limit 1
) cw on true;

update public.settings_defaults
set candidate_app_feature_flags_json=jsonb_build_object(
  'candidate_app_reads',true,
  'candidate_app_writes',true,
  'candidate_record_role_capabilities',true,
  'candidate_daily_finalisation',true,
  'candidate_manager_approval',true
)
where id=1;

do $route_and_daily_authority$
declare
  v_now timestamptz:='2026-08-08 12:00:00+00';
  v_client uuid:='95200000-0000-0000-0000-000000000001';
  v_candidate uuid:='95200000-0000-0000-0000-000000000002';
  v_contract uuid:='95200000-0000-0000-0000-000000000003';
  v_account uuid:='95200000-0000-0000-0000-000000000004';
  v_session uuid:='95200000-0000-0000-0000-000000000005';
  v_actor uuid:='95200000-0000-0000-0000-000000000006';
  v_electronic_week uuid:='95200000-0000-0000-0000-000000000010';
  v_electronic_ts uuid:='95200000-0000-0000-0000-000000000011';
  v_manual_week uuid:='95200000-0000-0000-0000-000000000012';
  v_manual_ts uuid:='95200000-0000-0000-0000-000000000013';
  v_qr_week uuid:='95200000-0000-0000-0000-000000000014';
  v_qr_ts uuid:='95200000-0000-0000-0000-000000000015';
  v_import_contract uuid:='95200000-0000-0000-0000-000000000016';
  v_import_week uuid:='95200000-0000-0000-0000-000000000017';
  v_import_ts uuid:='95200000-0000-0000-0000-000000000018';
  v_daily_ts uuid:='95200000-0000-0000-0000-000000000019';
  v_daily_workflow uuid:='95200000-0000-0000-0000-000000000020';
  v_result jsonb;
  v_issues jsonb;
  v_daily_input jsonb;
  v_daily_submission jsonb;
  v_before_daily_updated_at timestamptz;
begin
  insert into public.clients(id,name) values(v_client,'Runtime Client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'route-daily@example.test',true,null);
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    candidate_expenses_require_separate_timesheet,allow_daily_manager_authorise_on_phone
  ) values(gen_random_uuid(),v_client,'2026-01-01','ELECTRONIC',6,true,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,weekly_timesheet_source,role,band
  ) values
    (v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,'ELECTRONIC','NONE','NURSE','Band 5'),
    (v_import_contract,v_candidate,v_client,'2026-01-01','2026-12-31',6,'ELECTRONIC','NHSP','NURSE','Band 5');
  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode,sheet_scope,
    r2_nurse_key,r2_auth_key,qr_status,qr_token,scheduled_start_iso,scheduled_end_iso,
    worked_start_iso,worked_end_iso,break_start_iso,break_end_iso,break_minutes,worked_minutes,
    job_title_norm,band,booking_id,candidate_submission_route_intent
  ) values
    (v_electronic_ts,v_contract,'2026-08-08','HOURS','ELECTRONIC','WEEKLY','candidate/e','manager/e',null,null,null,null,null,null,null,null,null,null,'NURSE','Band 5',null,null),
    (v_manual_ts,v_contract,'2026-08-01','HOURS','MANUAL','WEEKLY',null,null,null,null,null,null,null,null,null,null,null,null,'NURSE','Band 5',null,null),
    (v_qr_ts,v_contract,'2026-07-25','HOURS','MANUAL','WEEKLY',null,null,'PENDING','fixture-qr',null,null,null,null,null,null,null,null,'NURSE','Band 5',null,null),
    (v_import_ts,v_import_contract,'2026-08-08','HOURS','ELECTRONIC','WEEKLY','candidate/i','manager/i',null,null,null,null,null,null,null,null,null,null,'NURSE','Band 5',null,null),
    (v_daily_ts,v_contract,'2026-08-08','HOURS','MANUAL','DAILY',null,null,null,null,
      '2026-08-08 08:00:00+01','2026-08-08 18:00:00+01',
      '2026-08-08 08:00:00+01','2026-08-08 17:00:00+01',
      '2026-08-08 12:00:00+01','2026-08-08 13:00:00+01',60,480,
      'NURSE','5','HR-RUNTIME-SHIFT','ELECTRONIC');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values
    (v_electronic_week,v_contract,'2026-08-08',0,'OPEN','ELECTRONIC',v_electronic_ts),
    (v_manual_week,v_contract,'2026-08-01',0,'OPEN','MANUAL',v_manual_ts),
    (v_qr_week,v_contract,'2026-07-25',0,'OPEN','MANUAL',v_qr_ts),
    (v_import_week,v_import_contract,'2026-08-08',0,'OPEN','ELECTRONIC',v_import_ts);
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status,worked_start_iso,worked_end_iso,
    break_start_iso,break_end_iso,break_minutes,role,band,pay_method
  ) values
    (v_electronic_ts,v_candidate,v_client,8,'UNPROCESSED',null,null,null,null,null,'NURSE','Band 5','PAYE'),
    (v_manual_ts,v_candidate,v_client,8,'UNPROCESSED',null,null,null,null,null,'NURSE','Band 5','PAYE'),
    (v_qr_ts,v_candidate,v_client,8,'UNPROCESSED',null,null,null,null,null,'NURSE','Band 5','PAYE'),
    (v_import_ts,v_candidate,v_client,8,'UNPROCESSED',null,null,null,null,null,'NURSE','Band 5','PAYE'),
    (v_daily_ts,v_candidate,v_client,8,'UNPROCESSED','2026-08-08 08:00:00+01','2026-08-08 17:00:00+01',
      '2026-08-08 12:00:00+01','2026-08-08 13:00:00+01',60,'NURSE','Band 5','PAYE');
  insert into public.tms_users(id) values(v_actor);
  update public.settings_defaults set candidate_app_system_actor_user_id=v_actor where id=1;
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','route-daily@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('d1',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days');

  if private._candidate_route_family_v1(v_electronic_ts,v_electronic_week)->>'route_family'<>'ELECTRONIC'
     or private._candidate_route_family_v1(v_manual_ts,v_manual_week)->>'route_family'<>'MANUAL_NON_QR'
     or private._candidate_route_family_v1(v_qr_ts,v_qr_week)->>'route_family'<>'QR'
     or private._candidate_route_family_v1(v_import_ts,v_import_week)->>'route_family'<>'IMPORT_AUTHORITATIVE' then
    raise exception 'candidate route-family resolution failed';
  end if;
  if coalesce((private._candidate_route_family_v1(v_manual_ts,v_manual_week)->>'candidate_no_work_allowed')::boolean,true)
     or coalesce((private._candidate_route_family_v1(v_import_ts,v_import_week)->>'candidate_hours_submission_allowed')::boolean,true) then
    raise exception 'view-only route exposed a candidate mutation';
  end if;
  begin
    perform public.candidate_no_work_atomic_v1(
      v_session,'TEST',v_manual_week,null,'route-daily:manual-no-work',v_now
    );
    raise exception 'manual non-QR no-work mutation was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_NO_WORK_NOT_ALLOWED' then raise; end if;
  end;
  begin
    perform public.candidate_no_work_atomic_v1(
      v_session,'TEST',v_import_week,null,'route-daily:import-no-work',v_now
    );
    raise exception 'import-authoritative no-work mutation was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_NO_WORK_NOT_ALLOWED' then raise; end if;
  end;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST','95200000-0000-0000-0000-000000000021','CREATE',1,
      jsonb_build_object('workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC',
        'contract_id',v_import_contract,'contract_week_id',v_import_week,
        'target_timesheet_id',v_import_ts,'week_ending_date','2026-08-08'),
      'route-daily:import-hours',v_now
    );
    raise exception 'import-authoritative candidate hours workflow was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_RECORD_VIEW_ONLY' then raise; end if;
  end;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST','95200000-0000-0000-0000-000000000022','CREATE',1,
    jsonb_build_object('workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','PAPER',
      'contract_id',v_contract,'contract_week_id',v_qr_week,
      'target_timesheet_id',v_qr_ts,'week_ending_date','2026-07-25'),
    'route-daily:qr-paper',v_now
  );
  if v_result->>'state'<>'WORKER_DRAFT' then
    raise exception 'QR-backed candidate paper workflow was not accepted: %',v_result;
  end if;

  if private._candidate_daily_work_date_v1('2026-06-30 23:30:00+00',null,null)<>'2026-07-01'
     or private._candidate_daily_work_date_v1('2026-12-31 23:30:00+00',null,null)<>'2026-12-31'
     or private._candidate_daily_work_date_v1('2026-03-29 00:30:00+00',null,null)<>'2026-03-29'
     or private._candidate_daily_work_date_v1('2026-10-25 01:30:00+00',null,null)<>'2026-10-25' then
    raise exception 'Europe/London DAILY work-date derivation failed';
  end if;
  if private._candidate_daily_entitled_v1(v_candidate) then
    raise exception 'candidate without GCK received DAILY write entitlement';
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_daily_workflow,'CREATE',1,
      jsonb_build_object('workflow_kind','DAILY','scope','DAILY','route','PHONE',
        'target_timesheet_id',v_daily_ts,'work_date','2026-08-08'),
      'route-daily:no-gck',v_now
    );
    raise exception 'DAILY workflow was created without GCK';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' then raise; end if;
  end;
  update public.candidates set key_norm='RUNTIME-GCK' where id=v_candidate;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    v_session,'TEST',v_daily_workflow,'CREATE',1,
    jsonb_build_object('workflow_kind','DAILY','scope','DAILY','route','PHONE',
      'target_timesheet_id',v_daily_ts,'work_date','2026-08-08'),
    'route-daily:create',v_now
  );
  if v_result->>'state'<>'WORKER_DRAFT' then raise exception 'eligible DAILY workflow creation failed: %',v_result; end if;
  update public.candidates set key_norm=null where id=v_candidate;
  begin
    perform public.candidate_workflow_transition_atomic_v1(
      v_session,'TEST',v_daily_workflow,'WORKER_SUBMIT',1,
      jsonb_build_object('immutable_submission',jsonb_build_object(
        'worked_start_iso','2026-08-08 08:00:00+01','worked_end_iso','2026-08-08 18:00:00+01',
        'break_start_iso','2026-08-08 12:00:00+01','break_end_iso','2026-08-08 13:00:00+01'
      )),'route-daily:removed-gck',v_now
    );
    raise exception 'DAILY submission continued after GCK removal';
  exception when sqlstate '55000' then
    if sqlerrm<>'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' then raise; end if;
  end;
  update public.candidates set key_norm='RUNTIME-GCK' where id=v_candidate;

  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object(
        'worked_start_iso','2026-08-08 09:00:00+01','worked_end_iso','2026-08-08 18:00:00+01',
        'actual_schedule_json',jsonb_build_array(
          jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 09:00:00+01','end_iso','2026-08-08 13:00:00+01',
            'break_start_iso','2026-08-08 10:00:00+01','break_end_iso','2026-08-08 11:00:00+01'),
          jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 14:00:00+01','end_iso','2026-08-08 18:00:00+01')
        ),
        'additional_units_week',jsonb_build_object('ON_CALL',1)
      ),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 09:00:00+01','end_iso','2026-08-08 13:00:00+01',
          'break_start_iso','2026-08-08 10:00:00+01','break_end_iso','2026-08-08 11:00:00+01'),
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 14:00:00+01','end_iso','2026-08-08 17:00:00+01')
      )
    ),
    jsonb_build_object('hours_deviation_pct',10)
  );
  if not (v_issues ? 'UNEXPECTED_HOURS')
     or not (v_issues ? 'ADDITIONAL_UNITS_NEEDS_CHECKING')
     or not (v_issues ? 'HEALTHROSTER_VALIDATION_REQUIRED')
     or (v_issues ? 'DAILY_BREAK_UNEXPECTED') then
    raise exception 'DAILY issue derivation did not use daily totals/current policy/break/HR truth: %',v_issues;
  end if;

  -- Fewer-than-planned work is not the one-sided Unexpected Hours issue.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 09:00:00+01','end_iso','2026-08-08 17:00:00+01',
          'break_start_iso','2026-08-08 12:00:00+01','break_end_iso','2026-08-08 13:00:00+01'))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 09:00:00+01','end_iso','2026-08-08 18:00:00+01',
          'break_start_iso','2026-08-08 12:00:00+01','break_end_iso','2026-08-08 13:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if v_issues ? 'UNEXPECTED_HOURS' or v_issues ? 'DAILY_BREAK_UNEXPECTED' then
    raise exception 'fewer-than-planned/standard-break DAILY submission was misclassified: %',v_issues;
  end if;

  -- With the authoritative 60-minute planned fallback, planned net is nine
  -- hours. Exactly 9h54 actual net is the 10% boundary and does not exceed it.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 17:54:00+01'))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if v_issues ? 'UNEXPECTED_HOURS' then
    raise exception 'exact-threshold DAILY submission was incorrectly flagged: %',v_issues;
  end if;

  -- The default planned break is applied once to the DAILY day, not once per
  -- planned segment. Two five-hour segments still have nine planned net hours.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 17:54:00+01'))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 13:00:00+01'),
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 14:00:00+01','end_iso','2026-08-08 19:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if v_issues ? 'UNEXPECTED_HOURS' then
    raise exception 'planned-break fallback was incorrectly applied per segment: %',v_issues;
  end if;

  -- One minute beyond the same planned-net threshold is unexpected.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 17:55:00+01'))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if not (v_issues ? 'UNEXPECTED_HOURS') then
    raise exception 'above-threshold DAILY submission was not flagged: %',v_issues;
  end if;

  -- A break window is authoritative even without a break_minutes field.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01',
          'break_start_iso','2026-08-08 12:00:00+01','break_end_iso','2026-08-08 12:30:00+01'))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if not (v_issues ? 'DAILY_BREAK_UNEXPECTED') then
    raise exception 'non-standard break window was not flagged: %',v_issues;
  end if;

  -- Explicit No break is encoded as zero minutes with no start/end. DAILY
  -- records the non-60-minute issue; WEEKLY accepts the same representation
  -- without inventing a DAILY break issue.
  v_issues:=private._candidate_submission_issue_codes_v1(
    v_daily_workflow,
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01',
          'break_minutes',0))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-08-08','start_iso','2026-08-08 08:00:00+01','end_iso','2026-08-08 18:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if not (v_issues ? 'DAILY_BREAK_UNEXPECTED') then
    raise exception 'explicit DAILY No break was not documented as the non-60-minute issue: %',v_issues;
  end if;
  v_issues:=private._candidate_submission_issue_codes_v1(
    '95200000-0000-0000-0000-000000000022',
    jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-07-25','start_iso','2026-07-25 08:00:00+01','end_iso','2026-07-25 18:00:00+01',
          'break_minutes',0))),
      'planned_schedule_json',jsonb_build_array(
        jsonb_build_object('date','2026-07-25','start_iso','2026-07-25 08:00:00+01','end_iso','2026-07-25 18:00:00+01'))
    ),jsonb_build_object('hours_deviation_pct',10)
  );
  if v_issues ? 'DAILY_BREAK_UNEXPECTED' then
    raise exception 'explicit WEEKLY No break incorrectly created a DAILY break issue: %',v_issues;
  end if;

  -- The private helper is an atomic materialisation adapter, not a financial
  -- calculator. It must reject a stale BEGIN signature before factual data is
  -- changed; the shared backend module remains the sole calculation body.
  if to_regprocedure(
       'private._candidate_daily_save_recalculate_atomic_v1(uuid,integer,jsonb,uuid,timestamp with time zone)'
     ) is null then
    raise exception 'atomic Candidate DAILY materialisation helper is missing';
  end if;

  -- The DB hand-off preserves an explicit No break as zero minutes with no
  -- fabricated start/finish. The shared backend authority owns the economic
  -- regression matrix for this factual input.
  v_daily_submission:=jsonb_build_object(
    'hours_submission',jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object(
        'worked_start_iso','2026-08-08 08:00:00+01',
        'worked_end_iso','2026-08-08 18:00:00+01',
        'break_start_iso',null,
        'break_end_iso',null,
        'break_minutes',0
      )
    )
  );
  update public.candidate_submission_workflows
  set immutable_submission_json=v_daily_submission,
      immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_daily_submission),
      work_date='2026-08-08'
  where id=v_daily_workflow;
  v_daily_input:=private._candidate_daily_canonical_save_input_v1(v_daily_workflow,1);
  if v_daily_input->>'contract_version'<>'CANDIDATE_DAILY_CANONICAL_SAVE_V1'
     or (v_daily_input->'timesheet_patch_json'->>'break_minutes')::integer<>0
     or (v_daily_input->'timesheet_patch_json'->'break_start_iso') is distinct from 'null'::jsonb
     or (v_daily_input->'timesheet_patch_json'->'break_end_iso') is distinct from 'null'::jsonb then
    raise exception 'explicit DAILY No break was not preserved in canonical save input: %',v_daily_input;
  end if;
  select updated_at into v_before_daily_updated_at
  from public.timesheets where timesheet_id=v_daily_ts;
  begin
    perform private._candidate_daily_save_recalculate_atomic_v1(
      v_daily_workflow,1,
      jsonb_build_object(
        'contract_version','CANDIDATE_DAILY_ATOMIC_FINALISATION_V2',
        'workflow_id',v_daily_workflow,
        'workflow_generation',1,
        'timesheet_id',v_daily_ts,
        'canonical_save_input_sha256_hex',encode(private._candidate_sha256_jsonb_v1(v_daily_input),'hex'),
        'expected_pre_save_row_signature','deliberately-stale-runtime-signature',
        'timesheet_patch_json',(v_daily_input->'timesheet_patch_json')||jsonb_build_object('worked_minutes',600),
        'canonical_snapshot','{}'::jsonb,
        'rate_request','{}'::jsonb,
        'resolved_rate_row','{}'::jsonb
      ),v_actor,v_now
    );
    raise exception 'stale Candidate DAILY materialisation was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'CANDIDATE_DAILY_CANONICAL_SAVE_STALE' then raise; end if;
  end;
  if (select updated_at from public.timesheets where timesheet_id=v_daily_ts)
       is distinct from v_before_daily_updated_at then
    raise exception 'stale Candidate DAILY materialisation changed the timesheet before rejection';
  end if;
end;
$route_and_daily_authority$;

rollback;
