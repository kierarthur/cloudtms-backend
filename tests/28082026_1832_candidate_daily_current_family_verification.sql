-- Disposable/rollback-contained proof. No Contract or contract_week is created.
-- This proves the existing reset owner; first-submission integration is separate.
begin;

do $daily_current_family$
declare
  v_candidate uuid:=gen_random_uuid();
  v_first uuid;
  v_current uuid;
  v_next uuid;
  v_booking text;
  v_result jsonb;
  v_case integer;
  v_now timestamptz:='2026-08-28 17:32:00+00';
  v_date date;
  v_constraint text;
  v_summary_count integer;
  v_summary_id uuid;
  v_protected_case integer;
  v_protected_before jsonb;
begin
  if not exists(select 1 from public.settings_defaults
    where id=1 and candidate_app_system_actor_user_id is not null) then
    raise exception 'DAILY_CURRENT_FAMILY_SYSTEM_ACTOR_REQUIRED';
  end if;

  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'daily-family-'||v_candidate::text||'@example.test',true,
    'DAILY-FAMILY-'||v_candidate::text);

  -- Multiple dates in the same week, with no Client/role/rate mapping.
  for v_case in 1..2 loop
    v_date:=date '2026-08-27'+v_case;
    v_booking:='DAILY-FAMILY-'||v_candidate::text||'-'||v_date::text;
    v_first:=gen_random_uuid();
    insert into public.timesheets(
      timesheet_id,booking_id,version,is_current,status,contract_id,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      week_ending_date,line_type,sheet_scope,submission_mode,
      scheduled_start_iso,scheduled_end_iso,worked_start_iso,worked_end_iso,
      break_minutes,worked_minutes,candidate_submission_route_intent
    ) values(
      v_first,v_booking,1,true,'RECEIVED',null,
      'DAILY-FAMILY-'||v_candidate::text,'UNRESOLVED-HOSPITAL','','RMN',
      date '2026-08-30','HOURS','DAILY','MANUAL',
      (v_date::text||' 09:00:00+01')::timestamptz,
      (v_date::text||' 17:00:00+01')::timestamptz,
      (v_date::text||' 09:00:00+01')::timestamptz,
      (v_date::text||' 17:00:00+01')::timestamptz,
      0,480,'ELECTRONIC'
    );
    insert into public.timesheets_financials(
      timesheet_id,timesheet_version,candidate_id,client_id,role,band,
      processing_status,total_hours,hours_day
    ) values(v_first,1,v_candidate,null,null,null,'CLIENT_UNRESOLVED',8,8);

    -- Office authorisation must defeat a stale Candidate screen. Exercise both
    -- established authorisation markers directly against the reset owner and
    -- prove the rejection leaves the current record and evidence untouched.
    for v_protected_case in 1..2 loop
      update public.timesheets set authorised_at_server=
        case when v_protected_case=1 then v_now else null end where timesheet_id=v_first;
      update public.timesheets_financials set authorised_at_utc=
        case when v_protected_case=2 then v_now else null end where timesheet_id=v_first;
      select jsonb_build_object('timesheet',to_jsonb(t),'financials',to_jsonb(f),
        'evidence',(select coalesce(jsonb_agg(to_jsonb(e) order by e.id),'[]'::jsonb)
          from public.timesheet_evidence e where e.timesheet_id=v_first))
        into v_protected_before from public.timesheets t
        join public.timesheets_financials f using(timesheet_id)
        where t.timesheet_id=v_first and f.is_current;
      begin
        perform private._candidate_daily_submission_reset_v1(
          v_first,v_first,'Stale app attempted withdrawal.',v_candidate,'CANDIDATE_WITHDRAWN',v_now);
        raise exception 'DAILY_AUTHORISED_WITHDRAWAL_ACCEPTED';
      exception when sqlstate '55000' then
        if sqlerrm<>'CANDIDATE_DAILY_RESET_PROTECTED_HISTORY' then raise; end if;
      end;
      if not exists(select 1 from public.timesheets t
        join public.timesheets_financials f using(timesheet_id)
        where t.timesheet_id=v_first and f.is_current
          and jsonb_build_object('timesheet',to_jsonb(t),'financials',to_jsonb(f),
            'evidence',(select coalesce(jsonb_agg(to_jsonb(e) order by e.id),'[]'::jsonb)
              from public.timesheet_evidence e where e.timesheet_id=v_first))=v_protected_before)
        or (select count(*) from public.timesheets where booking_id=v_booking)<>1 then
        raise exception 'DAILY_AUTHORISED_WITHDRAWAL_CHANGED_RECORD';
      end if;
    end loop;
    update public.timesheets set authorised_at_server=null where timesheet_id=v_first;
    update public.timesheets_financials set authorised_at_utc=null where timesheet_id=v_first;

    v_result:=private._candidate_daily_submission_reset_v1(
      v_first,v_first,'Correct the hours entered.',v_candidate,
      'CANDIDATE_WITHDRAWN',v_now
    );
    v_current:=(v_result->>'current_timesheet_id')::uuid;
    if v_current is null or v_current=v_first
       or (select count(*) from public.timesheets where booking_id=v_booking)<>2
       or (select count(*) from public.timesheets where booking_id=v_booking and is_current)<>1
       or not exists(select 1 from public.timesheets
         where timesheet_id=v_first and not is_current and status='REVOKED'
           and revoked_reason='Correct the hours entered.')
       or not exists(select 1 from public.timesheets
         where timesheet_id=v_current and contract_id is null and version=2)
       or exists(select 1 from public.contract_weeks where timesheet_id in(v_first,v_current)) then
      raise exception 'DAILY_CURRENT_FAMILY_RESET_INVALID';
    end if;

    -- A late/retried old withdrawal must not rotate the replacement a second time.
    begin
      perform private._candidate_daily_submission_reset_v1(
        v_first,v_first,'Correct the hours entered.',v_candidate,
        'CANDIDATE_WITHDRAWN',v_now+interval '1 second');
      raise exception 'DAILY_STALE_WITHDRAWAL_ACCEPTED';
    exception when serialization_failure then
      if sqlerrm<>'TIMESHEET_MOVED' then raise; end if;
    end;

    -- Demonstrate the exact storage contract resubmission must use: reuse version 2.
    -- This is not a claim of API/PHONE end-to-end proof.
    update public.timesheets
    set worked_start_iso=scheduled_start_iso,worked_end_iso=scheduled_end_iso,
      break_minutes=30,worked_minutes=450
    where timesheet_id=v_current and is_current;
    update public.timesheets_financials
    set total_hours=7.5,hours_day=7.5,processing_status='CLIENT_UNRESOLVED'
    where timesheet_id=v_current and is_current;

    -- A faulty INSERT cannot create another active record for this same shift.
    begin
      insert into public.timesheets(
        booking_id,version,is_current,status,sheet_scope,submission_mode,
        occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date,line_type
      ) values(v_booking,3,true,'RECEIVED','DAILY','MANUAL',
        'DAILY-FAMILY-'||v_candidate::text,'UNRESOLVED-HOSPITAL','','RMN',
        date '2026-08-30','HOURS');
      raise exception 'DAILY_DUPLICATE_CURRENT_ACCEPTED';
    exception when unique_violation then
      get stacked diagnostics v_constraint=CONSTRAINT_NAME;
      if v_constraint<>'timesheets_booking_id_current_uidx' then raise; end if;
    end;

    if (select count(*) from public.timesheets where booking_id=v_booking)<>2
       or (select count(*) from public.timesheets where booking_id=v_booking and is_current)<>1
       or (select worked_minutes from public.timesheets where timesheet_id=v_current)<>450
       or (select total_hours from public.timesheets_financials where timesheet_id=v_first and is_current)<>8
       or exists(select 1 from public.timesheets_financials
         where timesheet_id=v_current and (authorised_at_utc is not null
           or paid_at_utc is not null or locked_by_invoice_id is not null)) then
      raise exception 'DAILY_CURRENT_FAMILY_RESUBMISSION_STORAGE_INVALID';
    end if;

    select count(*)::integer,min(summary_row.timesheet_id::text)::uuid
    into v_summary_count,v_summary_id
    from public.timesheet_summary_lightweight_rows_v1(
      jsonb_build_object('candidate_id',v_candidate)
    ) summary_row
    where summary_row.booking_id=v_booking;
    if v_summary_count<>1 or v_summary_id is distinct from v_current then
      raise exception 'DAILY_OFFICE_SUMMARY_CURRENT_FAMILY_INVALID: count %, current %',
        v_summary_count,v_summary_id;
    end if;

    -- Office rejection uses that same per-shift reset owner, including unresolved rows.
    select candidate_app_system_actor_user_id into v_next
    from public.settings_defaults where id=1;
    v_result:=private._candidate_daily_submission_reset_v1(
      v_current,v_current,'Office requested a correction.',v_next,
      'OFFICE_REJECTED',v_now+interval '2 seconds');
    v_next:=(v_result->>'current_timesheet_id')::uuid;
    if (select count(*) from public.timesheets where booking_id=v_booking and is_current)<>1
       or (select version from public.timesheets where timesheet_id=v_next)<>3
       or (select contract_id from public.timesheets where timesheet_id=v_next) is not null then
      raise exception 'DAILY_CURRENT_FAMILY_OFFICE_RESET_INVALID';
    end if;
    select count(*)::integer,min(summary_row.timesheet_id::text)::uuid
    into v_summary_count,v_summary_id
    from public.timesheet_summary_lightweight_rows_v1(
      jsonb_build_object('candidate_id',v_candidate)
    ) summary_row
    where summary_row.booking_id=v_booking;
    if v_summary_count<>1 or v_summary_id is distinct from v_next then
      raise exception 'DAILY_OFFICE_SUMMARY_AFTER_REJECTION_INVALID';
    end if;
  end loop;

  if (select count(*) from public.timesheets
      where occupant_key_norm='DAILY-FAMILY-'||v_candidate::text and is_current)<>2 then
    raise exception 'DAILY_DIFFERENT_WORK_DATES_COLLAPSED';
  end if;
  raise notice 'PASS: unresolved no-Contract Daily withdrawal, old retry rejection, one current shift record, retained revoked history, Office rejection, current-only Office summary and separate dates.';
end;
$daily_current_family$;

rollback;
