-- Cross-record worked-time overlap guard for CloudTMS Office and MyTMS.
--
-- One Candidate cannot submit or authorise two separate logical Timesheets
-- whose worked intervals overlap.  The guard is deliberately installed at
-- both durable admission boundaries:
--   * Candidate workflow submission, before a signed schedule is accepted;
--   * Timesheet authorisation, before Office or a finaliser authorises it.
--
-- A Candidate-scoped transaction lock serialises simultaneous submissions.
-- The current workflow/Timesheet booking family is excluded so withdrawal,
-- refusal, version rotation and resubmission remain one logical Timesheet.
-- Adjacent intervals are valid: one interval may begin exactly when another
-- ends.  Breaks do not make a Candidate available elsewhere; the whole shift
-- span is occupied.
\set ON_ERROR_STOP on
begin;

create or replace function private._timesheet_work_intervals_v1(
  p_submission_json jsonb,
  p_actual_schedule_json jsonb,
  p_worked_start_iso timestamptz,
  p_worked_end_iso timestamptz
) returns table(work_start_utc timestamptz,work_end_utc timestamptz,work_date date)
language plpgsql immutable security invoker
set search_path='pg_catalog','pg_temp' as $function$
declare
  v_schedule jsonb;
  v_row jsonb;
  v_start_text text;
  v_end_text text;
  v_date_text text;
  v_start_clock text;
  v_end_clock text;
  v_date date;
  v_end_date date;
  v_start timestamptz;
  v_end timestamptz;
begin
  v_schedule:=case
    when jsonb_typeof(p_submission_json#>'{hours_submission,timesheet_patch_json,actual_schedule_json}')='array'
      then p_submission_json#>'{hours_submission,timesheet_patch_json,actual_schedule_json}'
    when jsonb_typeof(p_submission_json#>'{timesheet_patch_json,actual_schedule_json}')='array'
      then p_submission_json#>'{timesheet_patch_json,actual_schedule_json}'
    when jsonb_typeof(p_submission_json->'actual_schedule_json')='array'
      then p_submission_json->'actual_schedule_json'
    when jsonb_typeof(p_actual_schedule_json)='array'
      then p_actual_schedule_json
    else '[]'::jsonb
  end;

  for v_row in select value from jsonb_array_elements(v_schedule)
  loop
    if jsonb_typeof(v_row)<>'object' then
      raise exception using errcode='22023',message='TIMESHEET_WORK_INTERVAL_INVALID';
    end if;

    v_start_text:=nullif(btrim(COALESCE(
      v_row->>'start_iso',v_row->>'worked_start_iso',v_row->>'start_utc',v_row->>'worked_start',''
    )), '');
    v_end_text:=nullif(btrim(COALESCE(
      v_row->>'end_iso',v_row->>'worked_end_iso',v_row->>'end_utc',v_row->>'worked_end',''
    )), '');

    if v_start_text is not null or v_end_text is not null then
      if v_start_text is null or v_end_text is null then
        raise exception using errcode='22023',message='TIMESHEET_WORK_INTERVAL_INVALID';
      end if;
      v_start:=v_start_text::timestamptz;
      v_end:=v_end_text::timestamptz;
    else
      v_date_text:=nullif(btrim(COALESCE(v_row->>'date',v_row->>'work_date','')), '');
      v_start_clock:=nullif(btrim(COALESCE(v_row->>'start',v_row->>'start_time','')), '');
      v_end_clock:=nullif(btrim(COALESCE(v_row->>'end',v_row->>'end_time','')), '');
      if v_date_text is null or v_start_clock is null or v_end_clock is null then
        raise exception using errcode='22023',message='TIMESHEET_WORK_INTERVAL_INVALID';
      end if;
      v_date:=v_date_text::date;
      v_end_date:=v_date+case when v_end_clock::time<=v_start_clock::time then 1 else 0 end;
      v_start:=(v_date::text||' '||v_start_clock)::timestamp without time zone at time zone 'Europe/London';
      v_end:=(v_end_date::text||' '||v_end_clock)::timestamp without time zone at time zone 'Europe/London';
    end if;

    if v_end<=v_start then
      raise exception using errcode='22023',message='TIMESHEET_WORK_INTERVAL_INVALID';
    end if;
    work_start_utc:=v_start;
    work_end_utc:=v_end;
    work_date:=(v_start at time zone 'Europe/London')::date;
    return next;
  end loop;

  if jsonb_array_length(v_schedule)=0
     and (p_worked_start_iso is not null or p_worked_end_iso is not null) then
    if p_worked_start_iso is null or p_worked_end_iso is null
       or p_worked_end_iso<=p_worked_start_iso then
      raise exception using errcode='22023',message='TIMESHEET_WORK_INTERVAL_INVALID';
    end if;
    work_start_utc:=p_worked_start_iso;
    work_end_utc:=p_worked_end_iso;
    work_date:=(p_worked_start_iso at time zone 'Europe/London')::date;
    return next;
  end if;
end;$function$;

alter function private._timesheet_work_intervals_v1(jsonb,jsonb,timestamptz,timestamptz) owner to postgres;
revoke all on function private._timesheet_work_intervals_v1(jsonb,jsonb,timestamptz,timestamptz) from public;

create or replace function private._timesheet_cross_record_overlap_assert_v1(
  p_candidate_id uuid,
  p_submission_json jsonb,
  p_actual_schedule_json jsonb,
  p_worked_start_iso timestamptz,
  p_worked_end_iso timestamptz,
  p_exclude_workflow_id uuid,
  p_exclude_timesheet_id uuid,
  p_exclude_booking_id text
) returns void language plpgsql volatile security definer
set search_path='public','private','pg_temp' as $function$
declare
  v_new_count integer:=0;
  v_conflict_kind text;
  v_conflict_id uuid;
  v_conflict_start timestamptz;
  v_conflict_end timestamptz;
begin
  if p_candidate_id is null then
    raise exception using errcode='PT409',message='TIMESHEET_CANDIDATE_IDENTITY_REQUIRED_FOR_OVERLAP_CHECK';
  end if;

  select count(*)::integer into v_new_count
  from private._timesheet_work_intervals_v1(
    COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
    p_worked_start_iso,p_worked_end_iso
  );
  if v_new_count=0 then return; end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'timesheet-cross-record-overlap:'||p_candidate_id::text,31082026
  ));

  with new_intervals as materialized (
    select * from private._timesheet_work_intervals_v1(
      COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
      p_worked_start_iso,p_worked_end_iso
    )
  )
  select 'WORKFLOW',w.id,existing.work_start_utc,existing.work_end_utc
  into v_conflict_kind,v_conflict_id,v_conflict_start,v_conflict_end
  from public.candidate_submission_workflows w
  cross join lateral private._timesheet_work_intervals_v1(
    COALESCE(w.immutable_submission_json,'{}'::jsonb),null,null,null
  ) existing
  join new_intervals proposed
    on proposed.work_start_utc<existing.work_end_utc
   and existing.work_start_utc<proposed.work_end_utc
  where w.candidate_id=p_candidate_id
    and w.id is distinct from p_exclude_workflow_id
    and w.worker_submitted_at_utc is not null
    and w.state in (
      'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
      'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
      'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
      'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED'
    )
    and not (
      p_exclude_booking_id is not null and exists(
        select 1 from public.timesheets linked
        where linked.timesheet_id in (w.target_timesheet_id,w.anchor_timesheet_id)
          and linked.booking_id=p_exclude_booking_id
      )
    )
  order by existing.work_start_utc,w.id
  limit 1;

  if v_conflict_id is null then
    with new_intervals as materialized (
      select * from private._timesheet_work_intervals_v1(
        COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
        p_worked_start_iso,p_worked_end_iso
      )
    ), accepted_timesheets as materialized (
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.timesheets_financials f
      join public.timesheets t on t.timesheet_id=f.timesheet_id
      where f.candidate_id=p_candidate_id and f.is_current=true
        and t.is_current=true and t.revoked_at is null
        and (t.authorised_at_server is not null or f.authorised_at_utc is not null)
      union
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.contracts c
      join public.timesheets t on t.contract_id=c.id
      where c.candidate_id=p_candidate_id and t.is_current=true
        and t.revoked_at is null and t.authorised_at_server is not null
      union
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.candidates c
      join public.timesheets t on t.occupant_key_norm=c.key_norm
      where c.id=p_candidate_id and c.key_norm is not null
        and t.is_current=true and t.revoked_at is null
        and t.authorised_at_server is not null
    )
    select 'TIMESHEET',t.timesheet_id,existing.work_start_utc,existing.work_end_utc
    into v_conflict_kind,v_conflict_id,v_conflict_start,v_conflict_end
    from accepted_timesheets t
    cross join lateral private._timesheet_work_intervals_v1(
      '{}'::jsonb,t.actual_schedule_json,t.worked_start_iso,t.worked_end_iso
    ) existing
    join new_intervals proposed
      on proposed.work_start_utc<existing.work_end_utc
     and existing.work_start_utc<proposed.work_end_utc
    where t.timesheet_id is distinct from p_exclude_timesheet_id
      and (p_exclude_booking_id is null or t.booking_id<>p_exclude_booking_id)
    order by existing.work_start_utc,t.timesheet_id
    limit 1;
  end if;

  if v_conflict_id is not null then
    raise exception using
      errcode='PT409',
      message='TIMESHEET_WORK_INTERVAL_OVERLAP',
      detail=jsonb_build_object(
        'error_code','TIMESHEET_WORK_INTERVAL_OVERLAP',
        'conflicting_record_kind',v_conflict_kind,
        'conflicting_record_id',v_conflict_id,
        'conflicting_start_utc',v_conflict_start,
        'conflicting_end_utc',v_conflict_end
      )::text;
  end if;
end;$function$;

alter function private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text) owner to postgres;
revoke all on function private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text) from public;

create or replace function private._timesheet_submission_workflow_overlap_trg_v1()
returns trigger language plpgsql volatile security definer
set search_path='public','private','pg_temp' as $function$
declare
  v_booking_id text;
begin
  if new.worker_submitted_at_utc is null
     or new.immutable_submission_json is null
     or new.state not in (
       'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
       'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
       'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
       'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED'
     ) then return new; end if;

  if tg_op='UPDATE'
     and old.worker_submitted_at_utc is not distinct from new.worker_submitted_at_utc
     and old.immutable_submission_json is not distinct from new.immutable_submission_json
     and old.state in (
       'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
       'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
       'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
       'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED'
     ) then return new; end if;

  select t.booking_id into v_booking_id
  from public.timesheets t
  where t.timesheet_id=COALESCE(new.target_timesheet_id,new.anchor_timesheet_id)
  limit 1;

  perform private._timesheet_cross_record_overlap_assert_v1(
    new.candidate_id,new.immutable_submission_json,null,null,null,
    new.id,COALESCE(new.target_timesheet_id,new.anchor_timesheet_id),v_booking_id
  );
  return new;
end;$function$;

alter function private._timesheet_submission_workflow_overlap_trg_v1() owner to postgres;
revoke all on function private._timesheet_submission_workflow_overlap_trg_v1() from public;

drop trigger if exists trg_timesheet_submission_workflow_overlap_biu on public.candidate_submission_workflows;
create trigger trg_timesheet_submission_workflow_overlap_biu
before insert or update on public.candidate_submission_workflows
for each row execute function private._timesheet_submission_workflow_overlap_trg_v1();

create or replace function private._timesheet_authorisation_overlap_trg_v1()
returns trigger language plpgsql volatile security definer
set search_path='public','private','pg_temp' as $function$
declare
  v_candidate_id uuid;
  v_candidate_count integer:=0;
  v_fin_schedule jsonb;
  v_fin_start timestamptz;
  v_fin_end timestamptz;
  v_interval_count integer:=0;
begin
  if new.authorised_at_server is null then return new; end if;
  if tg_op='UPDATE' and old.authorised_at_server is not distinct from new.authorised_at_server then
    return new;
  end if;

  select f.actual_schedule_json,f.worked_start_iso,f.worked_end_iso
  into v_fin_schedule,v_fin_start,v_fin_end
  from public.timesheets_financials f
  where f.timesheet_id=new.timesheet_id and f.is_current=true
  order by f.updated_at desc nulls last,f.created_at desc nulls last,f.id desc
  limit 1;

  select count(*)::integer into v_interval_count
  from private._timesheet_work_intervals_v1(
    '{}'::jsonb,COALESCE(new.actual_schedule_json,v_fin_schedule),
    COALESCE(new.worked_start_iso,v_fin_start),COALESCE(new.worked_end_iso,v_fin_end)
  );
  if v_interval_count=0 then return new; end if;

  with candidate_ids as (
    select w.candidate_id
    from public.candidate_submission_workflows w
    where w.id=new.candidate_workflow_id
    union
    select f.candidate_id
    from public.timesheets_financials f
    where f.timesheet_id=new.timesheet_id and f.is_current=true and f.candidate_id is not null
    union
    select c.candidate_id from public.contracts c where c.id=new.contract_id and c.candidate_id is not null
    union
    select c.id from public.candidates c
    where c.key_norm is not null and c.key_norm=new.occupant_key_norm
  )
  select count(*)::integer,min(candidate_id::text)::uuid
  into v_candidate_count,v_candidate_id from candidate_ids;

  if v_candidate_count<>1 or v_candidate_id is null then
    raise exception using
      errcode='PT409',
      message='TIMESHEET_CANDIDATE_IDENTITY_REQUIRED_FOR_OVERLAP_CHECK';
  end if;

  perform private._timesheet_cross_record_overlap_assert_v1(
    v_candidate_id,'{}'::jsonb,COALESCE(new.actual_schedule_json,v_fin_schedule),
    COALESCE(new.worked_start_iso,v_fin_start),COALESCE(new.worked_end_iso,v_fin_end),
    new.candidate_workflow_id,new.timesheet_id,new.booking_id
  );
  return new;
end;$function$;

alter function private._timesheet_authorisation_overlap_trg_v1() owner to postgres;
revoke all on function private._timesheet_authorisation_overlap_trg_v1() from public;

drop trigger if exists trg_timesheet_authorisation_overlap_biu on public.timesheets;
create trigger trg_timesheet_authorisation_overlap_biu
before insert or update of authorised_at_server on public.timesheets
for each row execute function private._timesheet_authorisation_overlap_trg_v1();

do $acl$ begin
  if exists(select 1 from pg_catalog.pg_roles where rolname='anon') then
    revoke all on function private._timesheet_work_intervals_v1(jsonb,jsonb,timestamptz,timestamptz) from anon;
    revoke all on function private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text) from anon;
    revoke all on function private._timesheet_submission_workflow_overlap_trg_v1() from anon;
    revoke all on function private._timesheet_authorisation_overlap_trg_v1() from anon;
  end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='authenticated') then
    revoke all on function private._timesheet_work_intervals_v1(jsonb,jsonb,timestamptz,timestamptz) from authenticated;
    revoke all on function private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text) from authenticated;
    revoke all on function private._timesheet_submission_workflow_overlap_trg_v1() from authenticated;
    revoke all on function private._timesheet_authorisation_overlap_trg_v1() from authenticated;
  end if;
end;$acl$;

notify pgrst,'reload schema';
commit;
