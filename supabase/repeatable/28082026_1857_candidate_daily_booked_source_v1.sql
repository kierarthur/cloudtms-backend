-- Candidate-only admission input. No financial definitions or values are changed.
-- Called only by the authenticated Candidate workflow owner, never by a browser.
begin;

create or replace function private._candidate_daily_booked_source_v1(
  p_environment text,
  p_candidate_id uuid,
  p_source jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_environment text:=upper(p_environment);
  v_generation_id uuid;
  v_work_date date;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_day public.candidate_daily_rota_days%rowtype;
  v_candidate public.candidates%rowtype;
  v_count integer;
  v_first date;
  v_last date;
  v_week_ending date;
begin
  if v_environment is null or v_environment not in ('TEST','LIVE')
     or p_candidate_id is null or p_now_utc is null
     or jsonb_typeof(p_source) is distinct from 'object'
     or not(p_source ?& array['generation_id','work_date','source_row_hash','booking_id'])
     or (p_source-array['generation_id','work_date','source_row_hash','booking_id'])<>'{}'::jsonb
     or exists(select 1 from jsonb_each(p_source) part where jsonb_typeof(part.value)<>'string')
     or coalesce(p_source->>'generation_id','') !~ '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$'
     or coalesce(p_source->>'work_date','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
     or coalesce(p_source->>'source_row_hash','') !~ '^[a-f0-9]{64}$'
     or nullif(btrim(p_source->>'booking_id'),'') is null
     or length(p_source->>'booking_id')>128 then
    raise exception 'CANDIDATE_DAILY_SOURCE_INVALID' using errcode='22023';
  end if;
  v_generation_id:=(p_source->>'generation_id')::uuid;
  v_work_date:=(p_source->>'work_date')::date;

  -- Same order as source enrolment/removal: Candidate, then authority scope.
  select * into v_candidate from public.candidates c
  where c.id=p_candidate_id and c.active is true for update;
  if not found or nullif(btrim(v_candidate.key_norm),'') is null then
    raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
  end if;
  select * into v_scope from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=p_candidate_id for update;
  if not found or v_scope.transition_in_progress
     or v_scope.active_generation_id is distinct from v_generation_id
     or not private._candidate_daily_entitled_v1(p_candidate_id)
     or not exists(select 1 from private.candidate_daily_entitlements e
       where e.environment=v_environment and e.candidate_id=p_candidate_id and e.enabled)
     or not exists(select 1 from private.candidate_daily_source_links l
       where l.environment=v_environment and l.candidate_id=p_candidate_id
         and l.state in ('PRIMARY','OVERLAP') and l.valid_from_utc<=p_now_utc
         and (l.valid_to_utc is null or l.valid_to_utc>p_now_utc)) then
    raise exception 'CANDIDATE_DAILY_SOURCE_CHANGED' using errcode='40001';
  end if;
  select * into v_generation from public.candidate_daily_rota_generations g
  where g.generation_id=v_generation_id and g.environment=v_environment
    and g.candidate_id=p_candidate_id and g.state='ACTIVE' for share;
  if not found or v_generation.expected_day_count<>14 or v_generation.actual_day_count<>14
     or v_generation.window_end<>v_generation.window_start+13 then
    raise exception 'CANDIDATE_DAILY_SOURCE_INCOMPLETE' using errcode='55000';
  end if;
  select count(*),min(d.rota_date),max(d.rota_date) into v_count,v_first,v_last
  from public.candidate_daily_rota_days d
  where d.generation_id=v_generation_id and d.environment=v_environment
    and d.candidate_id=p_candidate_id;
  if v_count<>14 or v_first<>v_generation.window_start or v_last<>v_generation.window_end then
    raise exception 'CANDIDATE_DAILY_SOURCE_INCOMPLETE' using errcode='55000';
  end if;
  select * into v_day from public.candidate_daily_rota_days d
  where d.generation_id=v_generation_id and d.environment=v_environment
    and d.candidate_id=p_candidate_id and d.rota_date=v_work_date for share;
  if not found or v_day.source_row_hash is distinct from p_source->>'source_row_hash'
     or v_day.booking_id is distinct from p_source->>'booking_id' then
    raise exception 'CANDIDATE_DAILY_SOURCE_CHANGED' using errcode='40001';
  end if;
  -- Blank availability is legitimate, but it is not a booked, worked shift.
  -- Do not borrow emergency windows or credentials for ordinary submission.
  if not v_day.booked or coalesce(v_day.timesheet_authorised,false)
     or v_day.timesheet_eligible is false
     or v_day.shift_starts_at is null or v_day.shift_ends_at is null
     or v_day.shift_ends_at<=v_day.shift_starts_at
     or (v_day.shift_starts_at at time zone 'Europe/London')::date<>v_work_date
     or v_work_date>(p_now_utc at time zone 'Europe/London')::date then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_ELIGIBLE' using errcode='55000';
  end if;
  v_week_ending:=v_work_date+(7-extract(isodow from v_work_date)::integer);
  return jsonb_build_object(
    'contract_version','CANDIDATE_DAILY_BOOKED_SOURCE_V1',
    'environment',v_environment,'candidate_id',p_candidate_id,
    'source',p_source,'booking_id',v_day.booking_id,
    'work_date',v_work_date,'week_ending_date',v_week_ending,
    'candidate_global_key',v_candidate.key_norm,
    'hospital',coalesce(v_day.hospital,''),'ward',coalesce(v_day.ward,''),
    'job_title',coalesce(v_day.job_title,''),'shift_type',coalesce(v_day.shift_type,''),
    'booking_reference',coalesce(v_day.booking_ref,''),
    'scheduled_start_iso',v_day.shift_starts_at,'scheduled_end_iso',v_day.shift_ends_at
  );
end;
$function$;

alter function private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz)
  from public,anon,authenticated,service_role;

create or replace function private._candidate_daily_first_receipt_v1(
  p_environment text,
  p_candidate_id uuid,
  p_source jsonb,
  p_workflow_id uuid,
  p_submission_requested boolean,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_source jsonb;
  v_identity text;
  v_current public.timesheets%rowtype;
  v_timesheet_id uuid;
begin
  if p_submission_requested is distinct from true or p_workflow_id is null then
    raise exception 'CANDIDATE_DAILY_SUBMISSION_INTENT_REQUIRED' using errcode='22023';
  end if;
  v_source:=private._candidate_daily_booked_source_v1(
    p_environment,p_candidate_id,p_source,p_now_utc);
  v_identity:='candidate-daily-first:'||p_workflow_id::text||':'||encode(
    extensions.digest(convert_to(jsonb_build_object(
      'environment',upper(p_environment),'candidate_id',p_candidate_id,'source',p_source
    )::text,'UTF8'),'sha256'),'hex');
  -- The publisher/removal scope lock is already held; serialize this booking
  -- family as well. Never use max(version)+merge-duplicates as admission.
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-daily-receipt|'||upper(p_environment)||'|'||p_candidate_id::text||'|'||
    (v_source->>'booking_id'),0));
  select * into v_current from public.timesheets t
  where t.booking_id=v_source->>'booking_id' and t.is_current for update;
  if found then
    if v_current.idempotency_key is distinct from v_identity
       or v_current.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_current.archived_at_utc is not null
       or upper(btrim(v_current.occupant_key_norm)) is distinct from upper(btrim(v_source->>'candidate_global_key'))
       or v_current.scheduled_start_iso is distinct from (v_source->>'scheduled_start_iso')::timestamptz
       or v_current.scheduled_end_iso is distinct from (v_source->>'scheduled_end_iso')::timestamptz then
      raise exception 'CANDIDATE_DAILY_CURRENT_TIMESHEET_REQUIRED' using errcode='40001';
    end if;
    return v_current.timesheet_id;
  end if;
  -- A different legacy booking identifier for the same source shift is not
  -- permission to create another summary entry. Route through its current row.
  if exists(select 1 from public.timesheets t where
      t.booking_id=v_source->>'booking_id' or t.idempotency_key like 'candidate-daily-first:'||p_workflow_id::text||':%'
      or (t.sheet_scope='DAILY'::public.timesheet_scope_enum and t.is_current
        and upper(btrim(t.occupant_key_norm))=upper(btrim(v_source->>'candidate_global_key'))
        and t.scheduled_start_iso=(v_source->>'scheduled_start_iso')::timestamptz
        and upper(btrim(t.hospital_norm))=upper(btrim(v_source->>'hospital'))
        and upper(btrim(t.ward_norm))=upper(btrim(v_source->>'ward'))
        and upper(btrim(t.job_title_norm))=upper(btrim(v_source->>'job_title')))) then
    raise exception 'CANDIDATE_DAILY_CURRENT_TIMESHEET_REQUIRED' using errcode='40001';
  end if;

  -- Factual receipt only. Both signatures are required before ELECTRONIC
  -- storage; keep the established MANUAL + ELECTRONIC-intent representation.
  -- Normal insert triggers are retained. No financial row, rate, total,
  -- processing/authorisation state or Contract Week is written here.
  insert into public.timesheets(
    booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    contract_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    scheduled_start_iso,scheduled_end_iso,week_ending_date,
    candidate_submission_route_intent,candidate_hint_text,idempotency_key,created_at,updated_at
  ) values(
    v_source->>'booking_id',1,true,'RECEIVED','DAILY','MANUAL','HOURS',
    null,v_source->>'candidate_global_key',v_source->>'hospital',v_source->>'ward',
    v_source->>'job_title',v_source->>'shift_type',
    (v_source->>'scheduled_start_iso')::timestamptz,
    (v_source->>'scheduled_end_iso')::timestamptz,(v_source->>'week_ending_date')::date,
    'ELECTRONIC',jsonb_build_object('candidate_id',p_candidate_id),v_identity,p_now_utc,p_now_utc
  ) returning timesheet_id into v_timesheet_id;
  return v_timesheet_id;
end;
$function$;

alter function private._candidate_daily_first_receipt_v1(text,uuid,jsonb,uuid,boolean,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_first_receipt_v1(text,uuid,jsonb,uuid,boolean,timestamptz)
  from public,anon,authenticated,service_role;

commit;
