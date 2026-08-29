-- Ordinary MyTMS Daily entry follows the active booking window, not legacy time limits.
-- No finance, legacy or emergency owner is changed.
\set ON_ERROR_STOP on
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
  -- Ordinary MyTMS admission lasts while the exact started booking remains in
  -- the active published window. The stored legacy four-hour eligibility flag
  -- is not Candidate authority. Emergency/legacy windows remain unchanged.
  if not v_day.booked or coalesce(v_day.timesheet_authorised,false)
     or v_day.shift_starts_at is null or v_day.shift_ends_at is null
     or v_day.shift_ends_at<=v_day.shift_starts_at
     or v_day.shift_starts_at>p_now_utc
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

create or replace function public.candidate_daily_tiles_get_v1(
  p_internal_context jsonb,
  p_from date default null,
  p_days integer default 14
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_policy text;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_tiles jsonb;
  v_from date;
  v_freshness jsonb;
  v_today date:=(pg_catalog.now() at time zone 'Europe/London')::date;
begin
  v_policy:=pg_catalog.upper(coalesce(
    nullif(p_internal_context->>'policy',''),'CANDIDATE_SURFACE'
  ));
  if v_policy='LEGACY_COMPAT' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
    v_environment:=v_context->>'environment';
    v_candidate_id:=private._candidate_daily_source_candidate_v1(
      v_environment,p_internal_context->>'candidate_source_hmac'
    );
  elsif v_policy='CANDIDATE_SURFACE' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
    v_environment:=v_context->>'environment';
    v_candidate_id:=(v_context->>'candidate_id')::uuid;
  else
    raise exception using errcode='22023',message='FORBIDDEN';
  end if;
  if p_days<>14 then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_scope from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=v_candidate_id;
  select * into v_generation from public.candidate_daily_rota_generations g
  where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE';
  if v_generation.generation_id is null then
    raise exception using errcode='55000',message='DAILY_GENERATION_UNAVAILABLE';
  end if;
  v_from:=coalesce(p_from,v_generation.window_start);
  -- A request for today may retain the last complete published window.
  -- Never shift its date labels or invent an unpublished fourteenth day.
  if v_from<>v_generation.window_start
     and not(v_policy='CANDIDATE_SURFACE' and v_from=v_today
       and v_generation.window_start<v_today) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'date',d.rota_date,'display_day',pg_catalog.to_char(d.rota_date,'Dy'),
    'display_date',pg_catalog.to_char(d.rota_date,'DD/MM/YYYY'),
    'booked',d.booked,'system_blocked',d.system_blocked,
    'editable',not(d.booked or d.system_blocked)
      and (v_policy<>'CANDIDATE_SURFACE' or d.rota_date>=v_today),
    'status',case when d.booked then 'BOOKED' when d.system_blocked then 'BLOCKED'
      else coalesce(a.preference,'PENDING') end,
    'availability',coalesce(a.preference,'PENDING'),
    'shift_starts_at',d.shift_starts_at,'shift_ends_at',d.shift_ends_at,
    'shift_info',d.shift_info,'hospital',d.hospital,'ward',d.ward,
    'job_title',d.job_title,'booking_ref',d.booking_ref,'shift_type',d.shift_type,
    'booking_id',d.booking_id,'timesheet_authorised',d.timesheet_authorised,
    'timesheet_eligible',case when v_policy='CANDIDATE_SURFACE'
      then candidate_entry.eligible else d.timesheet_eligible end,
    'week_ending_date',case when d.booked then
      d.rota_date+mod(7-extract(dow from d.rota_date)::integer,7) else null end,
    'break_entry',case when v_policy='CANDIDATE_SURFACE' and d.booked
      and nullif(btrim(d.booking_id),'') is not null then
      case when current_daily.timesheet_id is not null then
        private._candidate_daily_read_projection_v1(v_environment,v_candidate_id,
          current_daily.timesheet_id,now())->'break_entry'
      else private._candidate_daily_break_entry_v1(d.booking_id,d.rota_date,null,null,
        candidate_entry.eligible) end else null end,
    'action_target',case
      when v_policy='CANDIDATE_SURFACE' and d.booked then
        case when current_daily.timesheet_id is not null then
          pg_catalog.jsonb_build_object('target_kind','TIMESHEET_DETAIL',
            'timesheet_id',current_daily.timesheet_id,
            'row_signature',public.timesheet_lifecycle_signature_v1(
              current_daily.timesheet_id,null,false)->>'row_signature')
        when candidate_entry.eligible
          and not exists(select 1 from public.timesheets previous where previous.booking_id=d.booking_id)
        then pg_catalog.jsonb_build_object('target_kind','BOOKED_DAILY_SHIFT',
          'source',pg_catalog.jsonb_build_object('generation_id',d.generation_id,
            'work_date',d.rota_date,'source_row_hash',d.source_row_hash,'booking_id',d.booking_id))
        else null end
      else case d.action_target_kind
      when 'TIMESHEET_DETAIL' then pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'timesheet_id',d.action_timesheet_id,
        'workflow_id',d.action_workflow_id,'row_signature',d.action_row_signature))
      when 'CONTRACT_WEEK_DETAIL' then pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'contract_week_id',d.action_contract_week_id,
        'timesheet_id',d.action_timesheet_id,'workflow_id',d.action_workflow_id,
        'row_signature',d.action_row_signature))
      when 'WORKFLOW_DETAIL' then pg_catalog.jsonb_build_object(
        'target_kind',d.action_target_kind,'workflow_id',d.action_workflow_id,
        'workflow_generation',d.action_workflow_generation,'row_signature',d.action_row_signature)
      else null end end
  )) order by d.rota_date) into v_tiles
  from public.candidate_daily_rota_days d
  cross join lateral (
    -- Do not inherit the legacy four-hour flag. A source-only entry is usable
    -- once started, until this exact complete generation is replaced.
    select coalesce(d.booked and not coalesce(d.timesheet_authorised,false)
      and d.shift_starts_at is not null and d.shift_ends_at>d.shift_starts_at
      and d.shift_starts_at<=pg_catalog.now()
      and (d.shift_starts_at at time zone 'Europe/London')::date=d.rota_date
      and nullif(btrim(d.booking_id),'') is not null
      and d.source_row_hash~'^[a-f0-9]{64}$',false) as eligible
  ) candidate_entry
  left join public.candidate_daily_availability_days a
    on a.environment=d.environment and a.candidate_id=d.candidate_id
    and a.availability_date=d.rota_date
  left join lateral (
    select t.timesheet_id from public.timesheets t
    join public.candidates c on c.id=v_candidate_id
      and upper(btrim(c.key_norm))=upper(btrim(t.occupant_key_norm))
    where v_policy='CANDIDATE_SURFACE' and d.booked and t.booking_id=d.booking_id
      and t.sheet_scope='DAILY' and t.is_current and t.archived_at_utc is null
      and not exists(select 1 from public.timesheets_financials f
        where f.timesheet_id=t.timesheet_id and f.is_current
          and f.candidate_id is not null and f.candidate_id<>v_candidate_id)
    limit 1
  ) current_daily on true
  where d.generation_id=v_generation.generation_id;
  if pg_catalog.jsonb_array_length(coalesce(v_tiles,'[]'::jsonb))<>14 then
    raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
  end if;
  v_freshness:=private._candidate_daily_freshness_v1(v_environment,v_candidate_id,pg_catalog.now());
  if v_policy='CANDIDATE_SURFACE'
     and coalesce((v_freshness->>'ready')::boolean,false) is not true then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  return pg_catalog.jsonb_build_object(
    'candidate_id',v_candidate_id,'window_start',v_generation.window_start,
    'window_end',v_generation.window_end,'generation_id',v_generation.generation_id,
    'generation_version',v_generation.generation_version,
    'availability_version',v_scope.canonical_version,'freshness',v_freshness,
    'cohorts','[]'::jsonb,'tiles',v_tiles
  );
end;
$function$;

alter function private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz)
  from public,anon,authenticated,service_role;
alter function public.candidate_daily_tiles_get_v1(jsonb,date,integer) owner to postgres;
revoke all on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) from public,anon,authenticated;
grant execute on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) to service_role;
commit;
