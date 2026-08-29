-- Candidate DAILY specialist authority.
-- The agency database owns exact shift/contact discovery. Google remains only
-- the retained, narrowly invoked effect executor after a durable receipt claim.

create or replace function private._candidate_daily_emergency_token_v1(
  p_environment text,
  p_candidate_id uuid,
  p_generation_id uuid,
  p_rota_date date,
  p_source_row_hash text
)
returns text
language sql
immutable
security definer
set search_path=''
as $function$
  select encode(extensions.digest(convert_to(
    'CANDIDATE_DAILY_EMERGENCY_SHIFT_V1|' || upper(coalesce(p_environment,'')) || '|' ||
    coalesce(p_candidate_id::text,'') || '|' || coalesce(p_generation_id::text,'') || '|' ||
    coalesce(p_rota_date::text,'') || '|' || coalesce(p_source_row_hash,''), 'UTF8'), 'sha256'), 'hex');
$function$;

create or replace function private._candidate_daily_emergency_subject_token_v1(
  p_environment text,
  p_emergency_shift_token text,
  p_subject_candidate_id uuid
)
returns text
language sql
immutable
security definer
set search_path=''
as $function$
  select encode(extensions.digest(convert_to(
    'CANDIDATE_DAILY_EMERGENCY_SUBJECT_V1|' || upper(coalesce(p_environment,'')) || '|' ||
    coalesce(p_emergency_shift_token,'') || '|' || coalesce(p_subject_candidate_id::text,''), 'UTF8'), 'sha256'), 'hex');
$function$;

create or replace function private._candidate_daily_emergency_contacts_v1(
  p_environment text,
  p_reporting_candidate_id uuid,
  p_emergency_shift_token text,
  p_hospital text,
  p_ward text,
  p_rota_date date,
  p_shift_type text,
  p_shift_starts_at timestamptz,
  p_shift_ends_at timestamptz
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_current jsonb:='[]'::jsonb;
  v_previous jsonb:='[]'::jsonb;
  v_next jsonb:='[]'::jsonb;
begin
  if p_environment not in ('TEST','LIVE') or p_reporting_candidate_id is null
     or p_emergency_shift_token !~ '^[a-f0-9]{64}$'
     or nullif(btrim(p_hospital),'') is null or p_rota_date is null or p_shift_starts_at is null
     or p_shift_ends_at is null or p_shift_ends_at<=p_shift_starts_at then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  -- Preserve the retained legacy handover relationship exactly. A rota date has
  -- a LONG DAY and NIGHT cohort. For a LONG DAY, both adjacent handover views
  -- resolve the NIGHT cohort on that rota date. For a NIGHT, previous/next
  -- resolve LONG DAY on the preceding/following rota dates respectively.
  with selected as (
    select case when upper(coalesce(p_shift_type,'')) like '%NIGHT%'
                       or upper(btrim(coalesce(p_shift_type,'')))='N'
      then 'NIGHT' else 'LONG DAY' end as canonical_shift
  ), target_groups as (
    select 'CURRENT'::text as group_kind,p_rota_date as target_date,canonical_shift as target_shift from selected
    union all
    select 'PREVIOUS',case when canonical_shift='NIGHT' then p_rota_date-1 else p_rota_date end,
      case when canonical_shift='NIGHT' then 'LONG DAY' else 'NIGHT' end from selected
    union all
    select 'NEXT',case when canonical_shift='NIGHT' then p_rota_date+1 else p_rota_date end,
      case when canonical_shift='NIGHT' then 'LONG DAY' else 'NIGHT' end from selected
  ), matching as (
    select t.group_kind,d.candidate_id,
      coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),'')) as display_name,
      coalesce(nullif(btrim(d.job_title),''),'Worker') as role,
      btrim(c.phone) as callable_mobile,
      g.generation_version,d.updated_at_utc
    from target_groups t
    join private.candidate_daily_authority_scopes s on s.environment=p_environment and not s.transition_in_progress
    join public.candidate_daily_rota_generations g
      on g.generation_id=s.active_generation_id and g.environment=s.environment
      and g.candidate_id=s.candidate_id and g.state='ACTIVE'
    join public.candidate_daily_rota_days d
      on d.generation_id=g.generation_id and d.environment=g.environment
      and d.candidate_id=g.candidate_id and d.booked
    join public.candidates c on c.id=d.candidate_id and c.active
    where s.environment=p_environment and not s.transition_in_progress
      and d.candidate_id<>p_reporting_candidate_id
      and nullif(btrim(c.phone),'') is not null
      and d.rota_date=t.target_date
      and (case when upper(coalesce(d.shift_type,'')) like '%NIGHT%'
                    or upper(btrim(coalesce(d.shift_type,'')))='N'
            then 'NIGHT' else 'LONG DAY' end)=t.target_shift
      and lower(regexp_replace(btrim(coalesce(d.hospital,'')),'\s+',' ','g'))=
          lower(regexp_replace(btrim(p_hospital),'\s+',' ','g'))
      and lower(regexp_replace(btrim(coalesce(d.ward,'')),'\s+',' ','g'))=
          lower(regexp_replace(btrim(coalesce(p_ward,'')),'\s+',' ','g'))
  ), grouped as (
    select * from (
      select matching.*,row_number() over(
        partition by group_kind,candidate_id order by generation_version desc,updated_at_utc desc) as rn
      from matching
    ) ranked where rn=1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'display_name',display_name,'role',role,'callable_mobile',callable_mobile,
      'subject_token',private._candidate_daily_emergency_subject_token_v1(
        p_environment,p_emergency_shift_token,candidate_id)) order by display_name,candidate_id)
      filter(where group_kind='CURRENT'),'[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'display_name',display_name,'role',role,'callable_mobile',callable_mobile)
      order by display_name,candidate_id) filter(where group_kind='PREVIOUS'),'[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'display_name',display_name,'role',role,'callable_mobile',callable_mobile)
      order by display_name,candidate_id) filter(where group_kind='NEXT'),'[]'::jsonb)
  into v_current,v_previous,v_next from grouped;

  return jsonb_build_object('current',v_current,'previous',v_previous,'next',v_next);
end;
$function$;

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

  select d into v_day
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

create or replace function public.candidate_daily_specialist_read_v1(
  p_internal_context jsonb,
  p_operation text,
  p_input jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now(),
  p_correlation_id text default null
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
  v_items jsonb:='[]'::jsonb;
  v_shifts jsonb:='[]'::jsonb;
  v_shift jsonb;
  v_limit integer:=50;
  v_offset integer:=0;
  v_count integer:=0;
  v_minutes integer;
  v_option_token text;
  v_arrival timestamptz;
  v_candidate public.candidates%rowtype;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment';
  v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if jsonb_typeof(p_input)<>'object' or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  if p_operation='MESSAGE_CONTEXT' then
    if p_input<>'{}'::jsonb then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
    select * into v_candidate from public.candidates where id=v_candidate_id and active;
    if v_candidate.id is null or nullif(btrim(v_candidate.phone),'') is null then
      raise exception using errcode='55000',message='SOURCE_IDENTITY_NOT_READY';
    end if;
    return jsonb_build_object('candidate',jsonb_build_object(
      'display_name',coalesce(nullif(btrim(v_candidate.display_name),''),
        nullif(btrim(concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),'')),
      'callable_mobile',btrim(v_candidate.phone)));
  end if;

  if p_operation='PAST_SHIFTS' then
    v_limit:=coalesce((p_input->>'limit')::integer,50);
    if v_limit not between 1 and 100 then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;
    if nullif(p_input->>'cursor','') is not null then
      if p_input->>'cursor' !~ '^PASTSHIFT-[0-9]{16}$' then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      if substring(p_input->>'cursor' from 11 for 16)::bigint>1000 then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      v_offset:=substring(p_input->>'cursor' from 11 for 16)::integer;
    end if;
    with ranked as (
      select distinct on (d.rota_date) d.*
      from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g on g.generation_id=s.active_generation_id
        and g.environment=s.environment and g.candidate_id=s.candidate_id and g.state='ACTIVE'
      join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
      where s.environment=v_environment and s.candidate_id=v_candidate_id and not s.transition_in_progress
        and d.environment=g.environment and d.candidate_id=g.candidate_id and d.booked
        and d.rota_date between timezone('Europe/London',p_now_utc)::date-14
          and timezone('Europe/London',p_now_utc)::date-1
      order by d.rota_date,g.generation_version desc,d.updated_at_utc desc
    ), page as (
      select * from ranked order by rota_date desc limit v_limit+1 offset v_offset
    )
    select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'date',rota_date,'display_date',to_char(rota_date,'DD/MM/YYYY'),
      'shift_type',coalesce(nullif(btrim(shift_type),''),'Booked shift'),
      'starts_at',shift_starts_at,'ends_at',shift_ends_at,'notes',nullif(btrim(shift_info),''),
      'hospital',hospital,'ward',nullif(btrim(ward),''),'booking_reference',nullif(btrim(booking_ref),''),
      'job_title',nullif(btrim(job_title),''),'status',case when timesheet_authorised then 'AUTHORISED'
        when timesheet_eligible then 'TIMESHEET AVAILABLE' else 'BOOKED' end,
      'action_target',case
        when action_target_kind='TIMESHEET_DETAIL' then jsonb_build_object('target_kind','TIMESHEET_DETAIL','timesheet_id',action_timesheet_id)
        when action_target_kind='CONTRACT_WEEK_DETAIL' then jsonb_build_object('target_kind','CONTRACT_WEEK_DETAIL','contract_week_id',action_contract_week_id)
        when action_target_kind='WORKFLOW_DETAIL' then jsonb_build_object('target_kind','WORKFLOW_DETAIL','workflow_id',action_workflow_id,
          'workflow_generation',action_workflow_generation,'row_signature',action_row_signature)
        else null end)) order by rota_date desc) filter(where rn<=v_limit),'[]'::jsonb),count(*)
    into v_items,v_count from (select page.*,row_number() over(order by rota_date desc) rn from page) q;
    return jsonb_build_object('items',v_items,'limit',v_limit,'next_cursor',case when v_count>v_limit
      then 'PASTSHIFT-'||lpad((v_offset+v_limit)::text,16,'0') else null end);
  end if;

  if p_operation='EMERGENCY_WINDOW' then
    for v_shift in
      select private._candidate_daily_specialist_shift_v1(p_internal_context,
        private._candidate_daily_emergency_token_v1(v_environment,v_candidate_id,g.generation_id,d.rota_date,d.source_row_hash),p_now_utc)
      from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g on g.generation_id=s.active_generation_id
        and g.environment=s.environment and g.candidate_id=s.candidate_id and g.state='ACTIVE'
      join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
        and d.environment=g.environment and d.candidate_id=g.candidate_id and d.booked
      where s.environment=v_environment and s.candidate_id=v_candidate_id and not s.transition_in_progress
        and (p_now_utc between d.shift_starts_at-interval '4 hours' and d.shift_starts_at+interval '600 minutes'
          or p_now_utc between d.shift_starts_at and d.shift_ends_at
          or d.shift_starts_at=(select min(fd.shift_starts_at)
            from public.candidate_daily_rota_days fd
            where fd.generation_id=g.generation_id and fd.environment=g.environment
              and fd.candidate_id=g.candidate_id and fd.booked and fd.shift_starts_at>p_now_utc
              and fd.rota_date between timezone('Europe/London',p_now_utc)::date
                and timezone('Europe/London',p_now_utc)::date+1))
      order by d.shift_starts_at
    loop
      v_shifts:=v_shifts||jsonb_build_array(v_shift-'_agency_payload');
    end loop;
    return jsonb_build_object('eligible',jsonb_array_length(v_shifts)>0,
      'grace_minutes_after_start',600,'shifts',v_shifts);
  end if;

  if p_operation in ('RUNNING_LATE_OPTIONS','RUNNING_LATE_PREVIEW') then
    v_shift:=private._candidate_daily_specialist_shift_v1(p_internal_context,p_input->>'emergency_shift_token',p_now_utc);
    if not (v_shift->'allowed_issues' ? 'RUNNING_LATE') then
      raise exception using errcode='22023',message='SEMANTIC_REJECTION';
    end if;
    if p_operation='RUNNING_LATE_OPTIONS' then
      return jsonb_build_object('options',(
        select jsonb_agg(jsonb_build_object(
          'running_late_option_token',encode(extensions.digest(convert_to(
            'CANDIDATE_DAILY_RUNNING_LATE_V1|'||(p_input->>'emergency_shift_token')||'|'||x.minutes,'UTF8'),'sha256'),'hex'),
          'minutes',x.minutes,'label',x.label,
          'arrival_at',(v_shift->>'starts_at')::timestamptz+make_interval(mins=>x.minutes)) order by x.minutes)
        from (values (15,'Less than 15 minutes'),(30,'Less than 30 minutes'),
          (60,'Less than 1 hour'),(120,'Less than 2 hours')) x(minutes,label)));
    end if;
    for v_minutes in select unnest(array[15,30,60,120]) loop
      v_option_token:=encode(extensions.digest(convert_to(
        'CANDIDATE_DAILY_RUNNING_LATE_V1|'||(p_input->>'emergency_shift_token')||'|'||v_minutes,'UTF8'),'sha256'),'hex');
      exit when v_option_token=p_input->>'running_late_option_token';
      v_minutes:=null;
    end loop;
    if v_minutes is null then raise exception using errcode='22023',message='SEMANTIC_REJECTION'; end if;
    v_arrival:=(v_shift->>'starts_at')::timestamptz+make_interval(mins=>v_minutes);
    return jsonb_build_object('arrival_at',v_arrival,'preview_text',
      'We will inform all your colleagues on shift that you are running late and will arrive no later than '||
      to_char(v_arrival at time zone 'Europe/London','HH24:MI')||
      'hrs and provide them with your contact mobile number.');
  end if;
  raise exception using errcode='22023',message='VALIDATION_FAILED';
end;
$function$;

create or replace function public.candidate_daily_effect_claim_candidate_v1(
  p_internal_context jsonb,
  p_operation text,
  p_input jsonb,
  p_executor_id text,
  p_idempotency_key text,
  p_lease_seconds integer default 120,
  p_now_utc timestamptz default now(),
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_candidate public.candidates%rowtype;
  v_shift jsonb;
  v_subject jsonb;
  v_request_hash text;
  v_effect_key text;
  v_receipt private.candidate_daily_external_effect_receipts%rowtype;
  v_lease_token text;
  v_payload jsonb;
  v_safe_evidence jsonb;
  v_running_late jsonb;
  v_inserted boolean:=false;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment';
  v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if p_operation not in ('RUNNING_LATE_SEND','CANNOT_ATTEND','LEAVE_EARLY','DNA','MESSAGE_SEEN')
     or jsonb_typeof(p_input)<>'object' or length(btrim(p_executor_id)) not between 8 and 128
     or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$' or p_lease_seconds not between 30 and 600
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_candidate from public.candidates where id=v_candidate_id and active;
  if v_candidate.id is null or nullif(btrim(v_candidate.phone),'') is null then
    raise exception using errcode='55000',message='SOURCE_IDENTITY_NOT_READY';
  end if;
  if p_operation<>'MESSAGE_SEEN' then
    v_shift:=private._candidate_daily_specialist_shift_v1(p_internal_context,p_input->>'emergency_shift_token',p_now_utc);
    if not (v_shift->'allowed_issues' ? p_operation) and not
      (p_operation='RUNNING_LATE_SEND' and v_shift->'allowed_issues' ? 'RUNNING_LATE') then
      raise exception using errcode='22023',message='SEMANTIC_REJECTION';
    end if;
    if p_operation='DNA' then
      select value into v_subject from jsonb_array_elements(v_shift->'dna_subjects') value
      where value->>'subject_token'=p_input->>'subject_token';
      if v_subject is null or coalesce((p_input->>'tried_calling')::boolean,false) is not true then
        raise exception using errcode='22023',message='SEMANTIC_REJECTION';
      end if;
    end if;
    if p_operation='RUNNING_LATE_SEND' then
      v_running_late:=public.candidate_daily_specialist_read_v1(p_internal_context,'RUNNING_LATE_PREVIEW',
        jsonb_build_object('emergency_shift_token',p_input->>'emergency_shift_token',
          'running_late_option_token',p_input->>'running_late_option_token'),p_now_utc,p_correlation_id);
    end if;
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation',p_operation,'candidate_id',v_candidate_id,'input',p_input));
  v_effect_key:=encode(extensions.digest(convert_to(
    'CANDIDATE_DAILY_EFFECT_V1|'||v_environment||'|'||v_candidate_id::text||'|'||p_operation||'|'||
    p_idempotency_key||'|'||v_request_hash,'UTF8'),'sha256'),'hex');
  v_lease_token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
  v_safe_evidence:=jsonb_strip_nulls(jsonb_build_object(
    'authority','CANDIDATE_SURFACE','emergency_shift_token',p_input->>'emergency_shift_token',
    'subject_token',p_input->>'subject_token'));
  insert into private.candidate_daily_external_effect_receipts(
    environment,candidate_id,effect_key,operation,shift_identity,source_event_identity,request_hash,
    idempotency_key,state,first_claimed_at_utc,lease_owner,lease_token,lease_expires_at_utc,
    stable_provider_request_id,attempt_count,safe_evidence_json,correlation_id,retain_until_utc)
  values(v_environment,v_candidate_id,v_effect_key,p_operation,p_input->>'emergency_shift_token',
    case when v_shift is null then null else private._candidate_daily_json_sha256_v1(v_shift->'_agency_payload') end,
    v_request_hash,p_idempotency_key,'IN_PROGRESS',p_now_utc,p_executor_id,v_lease_token,
    p_now_utc+make_interval(secs=>p_lease_seconds),v_effect_key,1,v_safe_evidence,p_correlation_id,
    p_now_utc+interval '7 years') on conflict do nothing
  returning * into v_receipt;
  v_inserted:=v_receipt.effect_receipt_id is not null;
  if not v_inserted then
    select * into v_receipt from private.candidate_daily_external_effect_receipts where
      (environment=v_environment and effect_key=v_effect_key)
      or (environment=v_environment and candidate_id=v_candidate_id and operation=p_operation
        and idempotency_key=p_idempotency_key)
      order by (effect_key=v_effect_key) desc limit 1 for update;
  end if;
  if v_receipt.effect_receipt_id is null or v_receipt.candidate_id<>v_candidate_id
     or v_receipt.operation<>p_operation or v_receipt.request_hash<>v_request_hash
     or v_receipt.idempotency_key<>p_idempotency_key or v_receipt.effect_key<>v_effect_key then
    raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
  end if;
  if v_receipt.state<>'IN_PROGRESS' then
    return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'effect_key',v_receipt.effect_key,
      'state',v_receipt.state,'safe_result',v_receipt.terminal_result_json,'_idempotent_replay',true);
  end if;
  if not v_inserted then
    raise exception using errcode='55000',message=case when v_receipt.lease_expires_at_utc<=p_now_utc
      then 'EFFECT_STATUS_UNKNOWN' else 'EFFECT_IN_PROGRESS' end;
  end if;
  if v_receipt.lease_expires_at_utc<=p_now_utc then
    raise exception using errcode='55000',message='EFFECT_STATUS_UNKNOWN';
  end if;
  v_payload:=jsonb_build_object(
    'effect_key',v_effect_key,'operation',p_operation,
    'candidate',jsonb_build_object(
      'display_name',coalesce(nullif(btrim(v_candidate.display_name),''),btrim(concat_ws(' ',v_candidate.first_name,v_candidate.last_name))),
      'callable_mobile',btrim(v_candidate.phone)),
    'input',p_input,
    'running_late',v_running_late,
    'shift',case when v_shift is null then null else v_shift->'_agency_payload' end,
    'current_contacts',case when v_shift is null then '[]'::jsonb else v_shift->'colleague_groups'->'current' end,
    'previous_contacts',case when v_shift is null then '[]'::jsonb else v_shift->'colleague_groups'->'previous' end,
    'dna_subject',v_subject);
  return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'effect_key',v_effect_key,
    'state','CLAIMED','lease_token',v_receipt.lease_token,'lease_expires_at',v_receipt.lease_expires_at_utc,
    'effect_payload',v_payload);
end;
$function$;

create or replace function public.candidate_daily_effect_complete_candidate_v1(
  p_internal_context jsonb,
  p_effect_receipt_id uuid,
  p_lease_token text,
  p_outcome text,
  p_provider_reference_hash text default null,
  p_safe_message text default null,
  p_now_utc timestamptz default now(),
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_receipt private.candidate_daily_external_effect_receipts%rowtype;
  v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment'; v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if p_effect_receipt_id is null or length(p_lease_token) not between 16 and 256
     or p_outcome not in ('COMPLETED','FAILED_FINAL','UNKNOWN')
     or (p_provider_reference_hash is not null and p_provider_reference_hash !~ '^[a-f0-9]{64}$')
     or (p_safe_message is not null and length(p_safe_message)>320)
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_receipt from private.candidate_daily_external_effect_receipts
  where effect_receipt_id=p_effect_receipt_id and environment=v_environment and candidate_id=v_candidate_id for update;
  if v_receipt.effect_receipt_id is null then raise exception using errcode='02000',message='EFFECT_NOT_FOUND'; end if;
  if v_receipt.state<>'IN_PROGRESS' then
    if v_receipt.lease_token<>p_lease_token or v_receipt.state<>p_outcome then
      raise exception using errcode='55000',message='LEASE_CONFLICT';
    end if;
    return v_receipt.terminal_result_json||jsonb_build_object('_idempotent_replay',true);
  end if;
  if v_receipt.lease_token<>p_lease_token then raise exception using errcode='55000',message='LEASE_CONFLICT'; end if;
  if v_receipt.lease_expires_at_utc<=p_now_utc then
    raise exception using errcode='55000',message='LEASE_EXPIRED_STATUS_REQUIRED';
  end if;
  v_terminal:=jsonb_strip_nulls(jsonb_build_object(
    'effect_key',v_receipt.effect_key,'operation',v_receipt.operation,'status',p_outcome,
    'created_at',v_receipt.created_at_utc,'updated_at',p_now_utc,'safe_message',p_safe_message));
  update private.candidate_daily_external_effect_receipts set state=p_outcome,
    provider_reference_hash=p_provider_reference_hash,terminal_result_json=v_terminal,
    terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc where effect_receipt_id=v_receipt.effect_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_effect_status_candidate_v1(
  p_internal_context jsonb,
  p_effect_key text,
  p_now_utc timestamptz default now(),
  p_correlation_id text default null
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
  v_receipt private.candidate_daily_external_effect_receipts%rowtype;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment'; v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if p_effect_key !~ '^[a-f0-9]{64}$' or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_receipt from private.candidate_daily_external_effect_receipts
  where environment=v_environment and candidate_id=v_candidate_id and effect_key=p_effect_key;
  if v_receipt.effect_receipt_id is null then raise exception using errcode='02000',message='EFFECT_NOT_FOUND'; end if;
  if v_receipt.state='IN_PROGRESS' then
    return jsonb_build_object('effect_key',v_receipt.effect_key,'operation',v_receipt.operation,
      'status','IN_PROGRESS','created_at',v_receipt.created_at_utc,'updated_at',v_receipt.updated_at_utc,
      'safe_message','The agency communication is being completed. Do not submit it again.');
  end if;
  return v_receipt.terminal_result_json;
end;
$function$;

revoke all on function private._candidate_daily_emergency_token_v1(text,uuid,uuid,date,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_emergency_subject_token_v1(text,text,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_emergency_contacts_v1(text,uuid,text,text,text,date,text,timestamptz,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) from public,anon,authenticated;
revoke all on function public.candidate_daily_effect_claim_candidate_v1(jsonb,text,jsonb,text,text,integer,timestamptz,text) from public,anon,authenticated;
revoke all on function public.candidate_daily_effect_complete_candidate_v1(jsonb,uuid,text,text,text,text,timestamptz,text) from public,anon,authenticated;
revoke all on function public.candidate_daily_effect_status_candidate_v1(jsonb,text,timestamptz,text) from public,anon,authenticated;
grant execute on function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) to service_role;
grant execute on function public.candidate_daily_effect_claim_candidate_v1(jsonb,text,jsonb,text,text,integer,timestamptz,text) to service_role;
grant execute on function public.candidate_daily_effect_complete_candidate_v1(jsonb,uuid,text,text,text,text,timestamptz,text) to service_role;
grant execute on function public.candidate_daily_effect_status_candidate_v1(jsonb,text,timestamptz,text) to service_role;

alter function private._candidate_daily_emergency_token_v1(text,uuid,uuid,date,text) owner to postgres;
alter function private._candidate_daily_emergency_subject_token_v1(text,text,uuid) owner to postgres;
alter function private._candidate_daily_emergency_contacts_v1(text,uuid,text,text,text,date,text,timestamptz,timestamptz) owner to postgres;
alter function private._candidate_daily_specialist_shift_v1(jsonb,text,timestamptz) owner to postgres;
alter function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) owner to postgres;
alter function public.candidate_daily_effect_claim_candidate_v1(jsonb,text,jsonb,text,text,integer,timestamptz,text) owner to postgres;
alter function public.candidate_daily_effect_complete_candidate_v1(jsonb,uuid,text,text,text,text,timestamptz,text) owner to postgres;
alter function public.candidate_daily_effect_status_candidate_v1(jsonb,text,timestamptz,text) owner to postgres;
