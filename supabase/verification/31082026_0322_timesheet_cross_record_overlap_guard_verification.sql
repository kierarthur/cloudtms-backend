\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the shared Daily/Weekly overlap guard.
-- Real table triggers remain enabled.  The proof exercises Candidate workflow
-- admission and Office authorisation without retaining any fixture data.
begin;

do $timesheet_cross_record_overlap_guard_verification$
declare
  v_now timestamptz:='2026-08-31 03:22:00+00';
  v_week date:='2026-08-30';
  v_candidate uuid:=gen_random_uuid();
  v_client_one uuid:=gen_random_uuid();
  v_client_two uuid:=gen_random_uuid();
  v_contract_one uuid:=gen_random_uuid();
  v_contract_two uuid:=gen_random_uuid();
  v_week_one uuid:=gen_random_uuid();
  v_week_two uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid();
  v_timesheet_one uuid:=gen_random_uuid();
  v_timesheet_two uuid:=gen_random_uuid();
  v_timesheet_same_family uuid:=gen_random_uuid();
  v_timesheet_office uuid:=gen_random_uuid();
  v_daily_one uuid:=gen_random_uuid();
  v_daily_two uuid:=gen_random_uuid();
  v_workflow_one uuid:=gen_random_uuid();
  v_workflow_two uuid:=gen_random_uuid();
  v_workflow_same_family uuid:=gen_random_uuid();
  v_daily_workflow_one uuid:=gen_random_uuid();
  v_daily_workflow_two uuid:=gen_random_uuid();
  v_schedule_morning jsonb;
  v_schedule_overlap jsonb;
  v_schedule_touching jsonb;
  v_schedule_late jsonb;
  v_payload_morning jsonb;
  v_payload_overlap jsonb;
  v_payload_touching jsonb;
  v_failed boolean:=false;
  v_family text;
begin
  v_family:='OVERLAP-'||replace(v_candidate::text,'-','');
  v_schedule_morning:=jsonb_build_array(jsonb_build_object(
    'date','2026-08-24','start','09:00','end','17:00','break_minutes',30));
  v_schedule_overlap:=jsonb_build_array(jsonb_build_object(
    'date','2026-08-24','start','16:00','end','18:00','break_minutes',0));
  v_schedule_touching:=jsonb_build_array(jsonb_build_object(
    'date','2026-08-24','start','17:00','end','20:00','break_minutes',0));
  v_schedule_late:=jsonb_build_array(jsonb_build_object(
    'date','2026-08-24','start','20:00','end','22:00','break_minutes',0));
  v_payload_morning:=jsonb_build_object('hours_submission',jsonb_build_object(
    'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_morning)));
  v_payload_overlap:=jsonb_build_object('hours_submission',jsonb_build_object(
    'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_overlap)));
  v_payload_touching:=jsonb_build_object('hours_submission',jsonb_build_object(
    'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_touching)));

  insert into public.clients(id,name) values
    (v_client_one,'Overlap guard client one'),
    (v_client_two,'Overlap guard client two');
  insert into public.candidates(id,email,display_name,active,key_norm,opt_in_email)
  values(v_candidate,'overlap-'||replace(v_candidate::text,'-','')||'@example.test',
    'Overlap Guard Candidate',true,v_family,true);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role
  ) values
    (v_contract_one,v_candidate,v_client_one,v_week-30,v_week+30,0,'ELECTRONIC','PAYE','RMN'),
    (v_contract_two,v_candidate,v_client_two,v_week-30,v_week+30,0,'ELECTRONIC','PAYE','RMN');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,status,submission_mode_snapshot,day_entries_json,totals_json
  ) values
    (v_week_one,v_contract_one,v_week,'SUBMITTED','ELECTRONIC',v_schedule_morning,
      jsonb_build_object('total_hours',7.5,'actual_schedule_json',v_schedule_morning)),
    (v_week_two,v_contract_two,v_week,'SUBMITTED','ELECTRONIC',v_schedule_touching,
      jsonb_build_object('total_hours',3,'actual_schedule_json',v_schedule_touching));
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','overlap-'||replace(v_candidate::text,'-','')||'@example.test','ACTIVE');

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,actual_schedule_json
  ) values
    (v_timesheet_one,v_family||'-ONE',v_family,'HOSPITAL-ONE','WARD-ONE','RMN',v_contract_one,v_week,'HOURS','MANUAL','WEEKLY',v_schedule_morning),
    (v_timesheet_two,v_family||'-TWO',v_family,'HOSPITAL-TWO','WARD-TWO','RMN',v_contract_two,v_week,'HOURS','MANUAL','WEEKLY',v_schedule_touching);
  update public.contract_weeks set timesheet_id=v_timesheet_one where id=v_week_one;
  update public.contract_weeks set timesheet_id=v_timesheet_two where id=v_week_two;

  -- First accepted Weekly submission establishes 09:00-17:00.
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,worker_submitted_at_utc,immutable_submission_json,immutable_submission_sha256
  ) values(
    v_workflow_one,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',2,v_contract_one,v_week_one,
    v_timesheet_one,v_timesheet_one,v_week,'overlap-workflow-one-'||v_workflow_one::text,
    v_now,v_payload_morning,private._candidate_sha256_jsonb_v1(v_payload_morning)
  );

  -- A separate Weekly Timesheet for another Client overlaps 16:00-18:00.
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key
  ) values(
    v_workflow_two,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'CREATED',1,v_contract_two,v_week_two,v_timesheet_two,v_timesheet_two,v_week,
    'overlap-workflow-two-'||v_workflow_two::text
  );
  begin
    update public.candidate_submission_workflows set
      state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=2,
      worker_submitted_at_utc=v_now,immutable_submission_json=v_payload_overlap,
      immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_payload_overlap)
    where id=v_workflow_two;
    raise exception 'OVERLAPPING_WEEKLY_SUBMISSION_WAS_ACCEPTED';
  exception when sqlstate 'PT409' then
    if sqlerrm<>'TIMESHEET_WORK_INTERVAL_OVERLAP' then raise; end if;
    v_failed:=true;
  end;
  if not v_failed or (select state from public.candidate_submission_workflows where id=v_workflow_two)<>'CREATED' then
    raise exception 'OVERLAPPING_WEEKLY_ROLLBACK_NOT_PROVEN';
  end if;

  -- Boundary-touching 17:00 start is valid.
  update public.candidate_submission_workflows set
    state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=2,
    worker_submitted_at_utc=v_now,immutable_submission_json=v_payload_touching,
    immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_payload_touching)
  where id=v_workflow_two;

  -- A replacement version in the same booking family is not a second logical
  -- Timesheet and therefore must not conflict with itself.
  update public.timesheets set is_current=false where timesheet_id=v_timesheet_one;
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,actual_schedule_json,version,is_current
  ) values(
    v_timesheet_same_family,v_family||'-ONE',v_family,'HOSPITAL-ONE','WARD-ONE','RMN',v_contract_one,v_week,'HOURS','MANUAL',
    'WEEKLY',v_schedule_morning,2,true
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
    idempotency_key,worker_submitted_at_utc,immutable_submission_json,immutable_submission_sha256,
    replacement_of_workflow_id
  ) values(
    v_workflow_same_family,'TEST',v_account,v_candidate,'CONTRACT_HOURS','WEEKLY','ELECTRONIC',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',3,v_contract_one,v_week_one,
    v_timesheet_same_family,v_timesheet_same_family,v_week,
    'overlap-same-family-'||v_workflow_same_family::text,v_now,v_payload_morning,
    private._candidate_sha256_jsonb_v1(v_payload_morning),v_workflow_one
  );

  -- Office authorisation is independently guarded.  A 16:00-18:00 row is
  -- rejected; a 20:00-22:00 boundary-touching row is accepted.
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,actual_schedule_json
  ) values(v_timesheet_office,v_family||'-OFFICE',v_family,'HOSPITAL-TWO','WARD-TWO','RMN',v_contract_two,v_week,
    'HOURS','MANUAL','WEEKLY',v_schedule_overlap);
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,processing_status,actual_schedule_json
  ) values(v_timesheet_office,1,v_candidate,v_client_two,2,'UNPROCESSED',v_schedule_overlap);
  v_failed:=false;
  begin
    update public.timesheets set authorised_at_server=v_now where timesheet_id=v_timesheet_office;
    raise exception 'OVERLAPPING_OFFICE_AUTHORISATION_WAS_ACCEPTED';
  exception when sqlstate 'PT409' then
    if sqlerrm<>'TIMESHEET_WORK_INTERVAL_OVERLAP' then raise; end if;
    v_failed:=true;
  end;
  if not v_failed or (select authorised_at_server from public.timesheets where timesheet_id=v_timesheet_office) is not null then
    raise exception 'OVERLAPPING_OFFICE_AUTHORISATION_ROLLBACK_NOT_PROVEN';
  end if;
  update public.timesheets set actual_schedule_json=v_schedule_late where timesheet_id=v_timesheet_office;
  update public.timesheets_financials set actual_schedule_json=v_schedule_late where timesheet_id=v_timesheet_office;
  update public.timesheets set authorised_at_server=v_now where timesheet_id=v_timesheet_office;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,line_type,submission_mode,sheet_scope,
    actual_schedule_json
  ) values(
    v_daily_one,v_family||'-DAILY-ONE',v_family,'HOSPITAL-DAILY','WARD-DAILY','RMN',v_week,'HOURS','MANUAL','DAILY',v_schedule_morning
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    anchor_timesheet_id,target_timesheet_id,work_date,idempotency_key
  ) values(
    v_daily_workflow_one,'TEST',v_account,v_candidate,'DAILY','DAILY','PHONE',
    'CREATED',1,v_daily_one,v_daily_one,'2026-08-24',
    'overlap-daily-one-'||v_daily_workflow_one::text
  );
  update public.timesheets set candidate_workflow_id=v_daily_workflow_one,
    candidate_workflow_generation=1 where timesheet_id=v_daily_one;

  -- The same check spans record types: a Daily shift cannot overlap a Weekly
  -- submission for the same Candidate.
  v_failed:=false;
  begin
    update public.candidate_submission_workflows set
      state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=2,
      worker_submitted_at_utc=v_now,
      immutable_submission_json=jsonb_build_object(
        'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_morning)),
      immutable_submission_sha256=private._candidate_sha256_jsonb_v1(jsonb_build_object(
        'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_morning)))
    where id=v_daily_workflow_one;
    raise exception 'OVERLAPPING_WEEKLY_DAILY_SUBMISSION_WAS_ACCEPTED';
  exception when sqlstate 'PT409' then
    if sqlerrm<>'TIMESHEET_WORK_INTERVAL_OVERLAP' then raise; end if;
    v_failed:=true;
  end;
  if not v_failed or (select state from public.candidate_submission_workflows where id=v_daily_workflow_one)<>'CREATED' then
    raise exception 'OVERLAPPING_WEEKLY_DAILY_ROLLBACK_NOT_PROVEN';
  end if;

  -- Retire the Weekly submissions.  Refused history must not block the same
  -- valid Daily submission, which now becomes the active 09:00-17:00 record.
  update public.candidate_submission_workflows set state='REFUSED',updated_at_utc=v_now
  where id in (v_workflow_one,v_workflow_two,v_workflow_same_family);
  update public.candidate_submission_workflows set
    state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=2,
    worker_submitted_at_utc=v_now,
    immutable_submission_json=jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_morning)),
    immutable_submission_sha256=private._candidate_sha256_jsonb_v1(jsonb_build_object(
      'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_morning)))
  where id=v_daily_workflow_one;
  update public.timesheets set candidate_workflow_generation=2 where timesheet_id=v_daily_one;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,line_type,submission_mode,sheet_scope,
    actual_schedule_json
  ) values(
    v_daily_two,v_family||'-DAILY-TWO',v_family,'HOSPITAL-DAILY','WARD-DAILY','RMN',v_week,'HOURS','MANUAL','DAILY',v_schedule_overlap
  );
  insert into public.candidate_submission_workflows(
    id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
    anchor_timesheet_id,target_timesheet_id,work_date,idempotency_key
  ) values(
    v_daily_workflow_two,'TEST',v_account,v_candidate,'DAILY','DAILY','PHONE','CREATED',1,
    v_daily_two,v_daily_two,'2026-08-24','overlap-daily-two-'||v_daily_workflow_two::text
  );
  update public.timesheets set candidate_workflow_id=v_daily_workflow_two,
    candidate_workflow_generation=1 where timesheet_id=v_daily_two;
  v_failed:=false;
  begin
    update public.candidate_submission_workflows set
      state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=2,
      worker_submitted_at_utc=v_now,
      immutable_submission_json=jsonb_build_object(
        'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_overlap)),
      immutable_submission_sha256=private._candidate_sha256_jsonb_v1(jsonb_build_object(
        'timesheet_patch_json',jsonb_build_object('actual_schedule_json',v_schedule_overlap)))
    where id=v_daily_workflow_two;
    raise exception 'OVERLAPPING_DAILY_SUBMISSION_WAS_ACCEPTED';
  exception when sqlstate 'PT409' then
    if sqlerrm<>'TIMESHEET_WORK_INTERVAL_OVERLAP' then raise; end if;
    v_failed:=true;
  end;
  if not v_failed or (select state from public.candidate_submission_workflows where id=v_daily_workflow_two)<>'CREATED' then
    raise exception 'OVERLAPPING_DAILY_ROLLBACK_NOT_PROVEN';
  end if;

  -- Browser roles cannot invoke the private parser/assertion/trigger helpers.
  if exists(
    select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in (
      '_timesheet_work_intervals_v1','_timesheet_cross_record_overlap_assert_v1',
      '_timesheet_submission_workflow_overlap_trg_v1','_timesheet_authorisation_overlap_trg_v1'
    ) and (
      pg_catalog.has_function_privilege('public',p.oid,'EXECUTE')
      or (pg_catalog.to_regrole('anon') is not null and pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE'))
      or (pg_catalog.to_regrole('authenticated') is not null and pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE'))
    )
  ) then raise exception 'TIMESHEET_OVERLAP_PRIVATE_HELPER_BROWSER_EXECUTABLE'; end if;

  if not exists(select 1 from pg_catalog.pg_trigger where tgname='trg_timesheet_submission_workflow_overlap_biu' and tgenabled='O')
     or not exists(select 1 from pg_catalog.pg_trigger where tgname='trg_timesheet_authorisation_overlap_biu' and tgenabled='O') then
    raise exception 'TIMESHEET_OVERLAP_TRIGGER_NOT_ENABLED';
  end if;
end;$timesheet_cross_record_overlap_guard_verification$;

rollback;
