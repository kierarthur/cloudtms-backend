create or replace function private._candidate_daily_specialist_shift_v1(
  p_internal_context jsonb,
  p_emergency_shift_token text,
  p_now_utc timestamptz default now()
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
  v_day public.candidate_daily_rota_days%rowtype;
  v_generation_id uuid;
  v_groups jsonb;
  v_allowed jsonb:='[]'::jsonb;
  v_today date;
  v_earliest_future timestamptz;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment';
  v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if p_emergency_shift_token !~ '^[a-f0-9]{64}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  select d.* into v_day
  from private.candidate_daily_authority_scopes s
  join public.candidate_daily_rota_generations g
    on g.generation_id=s.active_generation_id and g.environment=s.environment
    and g.candidate_id=s.candidate_id and g.state='ACTIVE'
  join public.candidate_daily_rota_days d
    on d.generation_id=g.generation_id and d.environment=g.environment and d.candidate_id=g.candidate_id
  where s.environment=v_environment and s.candidate_id=v_candidate_id
    and not s.transition_in_progress and d.booked
    and private._candidate_daily_emergency_token_v1(v_environment,v_candidate_id,g.generation_id,
      d.rota_date,d.source_row_hash)=p_emergency_shift_token;
  if v_day.generation_id is null then raise exception using errcode='02000',message='NOT_FOUND'; end if;
  v_generation_id:=v_day.generation_id;

  select min(d.shift_starts_at) into v_earliest_future
  from public.candidate_daily_rota_days d
  where d.generation_id=v_generation_id and d.environment=v_environment and d.candidate_id=v_candidate_id
    and d.booked and d.shift_starts_at>p_now_utc
    and d.rota_date between timezone('Europe/London',p_now_utc)::date
      and timezone('Europe/London',p_now_utc)::date+1;
  v_today:=timezone('Europe/London',p_now_utc)::date;

  v_groups:=private._candidate_daily_emergency_contacts_v1(v_environment,v_candidate_id,
    p_emergency_shift_token,v_day.hospital,v_day.ward,v_day.rota_date,v_day.shift_type,
    v_day.shift_starts_at,v_day.shift_ends_at);
  if p_now_utc>=v_day.shift_starts_at-interval '4 hours'
     and p_now_utc<=v_day.shift_starts_at+interval '600 minutes' then
    v_allowed:=v_allowed||jsonb_build_array('RUNNING_LATE');
  end if;
  if v_earliest_future is not null and v_day.shift_starts_at=v_earliest_future then
    v_allowed:=v_allowed||jsonb_build_array('CANNOT_ATTEND');
  end if;
  if p_now_utc>=v_day.shift_starts_at and p_now_utc<v_day.shift_ends_at then
    v_allowed:=v_allowed||jsonb_build_array('LEAVE_EARLY');
    if v_day.rota_date=v_today and jsonb_array_length(v_groups->'current')>0 then
      v_allowed:=v_allowed||jsonb_build_array('DNA');
    end if;
  end if;
  if jsonb_array_length(v_allowed)=0 then
    raise exception using errcode='22023',message='SEMANTIC_REJECTION';
  end if;

  return jsonb_build_object(
    'emergency_shift_token',p_emergency_shift_token,
    'date',v_day.rota_date,
    'starts_at',v_day.shift_starts_at,
    'ends_at',v_day.shift_ends_at,
    'display_label',to_char(v_day.rota_date,'DD/MM/YYYY') || ' · ' ||
      to_char(v_day.shift_starts_at at time zone 'Europe/London','HH24:MI') || '–' ||
      to_char(v_day.shift_ends_at at time zone 'Europe/London','HH24:MI') || ' · ' ||
      coalesce(nullif(btrim(v_day.hospital),''),'Agency site') ||
      case when nullif(btrim(v_day.ward),'') is null then '' else ' · '||btrim(v_day.ward) end,
    'allowed_issues',v_allowed,
    'colleague_groups',v_groups,
    'dna_subjects',case when v_allowed ? 'DNA' then v_groups->'current' else '[]'::jsonb end,
    '_agency_payload',jsonb_build_object(
      'date',v_day.rota_date,'starts_at',v_day.shift_starts_at,'ends_at',v_day.shift_ends_at,
      'hospital',v_day.hospital,'ward',v_day.ward,'job_title',v_day.job_title,
      'booking_reference',v_day.booking_ref,'shift_type',v_day.shift_type,'shift_info',v_day.shift_info)
  );
end;
$function$;

revoke all on function private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz)
  from public,anon,authenticated,service_role;
alter function private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz) owner to postgres;
