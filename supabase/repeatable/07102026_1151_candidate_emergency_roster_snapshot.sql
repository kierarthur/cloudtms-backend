-- Full Master emergency roster independent of MyTMS enrolment. No public app
-- operation is added; the existing service-only RPC carries narrow transport.
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
  v_groups jsonb;
  v_current jsonb;
begin
  if p_environment is null or p_environment not in ('TEST','LIVE') or p_reporting_candidate_id is null
     or p_emergency_shift_token is null or p_emergency_shift_token !~ '^[a-f0-9]{64}$'
     or nullif(btrim(p_hospital),'') is null or p_rota_date is null or p_shift_starts_at is null
     or p_shift_ends_at is null or p_shift_ends_at<=p_shift_starts_at then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select groups_json into v_groups from private.candidate_daily_emergency_roster_snapshots
    where environment=p_environment and candidate_id=p_reporting_candidate_id
      and emergency_shift_token=p_emergency_shift_token
      and observed_at_utc<=clock_timestamp()+interval '30 seconds' and expires_at_utc>clock_timestamp();
  if v_groups is null then raise exception using errcode='55000',message='DEPENDENCY_UNAVAILABLE'; end if;
  select coalesce(jsonb_agg(value||jsonb_build_object('subject_token',encode(extensions.digest(convert_to(
    'CANDIDATE_DAILY_MASTER_SUBJECT_V1|'||p_environment||'|'||p_reporting_candidate_id::text||'|'||
    p_emergency_shift_token||'|'||(value->>'callable_mobile')||'|'||(value->>'display_name'),'UTF8'),'sha256'),'hex'))
    order by value->>'display_name',value->>'callable_mobile'),'[]'::jsonb)
  into v_current from jsonb_array_elements(v_groups->'current');
  return jsonb_build_object('current',v_current,'previous',v_groups->'previous','next',v_groups->'next');
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
volatile
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
  v_group text;
  v_contact jsonb;
  v_groups jsonb;
  v_observed timestamptz;
  v_receipt private.candidate_daily_external_effect_receipts%rowtype;
  v_request_hash text;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment';
  v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if jsonb_typeof(p_input)<>'object' or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  -- Only the private authenticated Worker can invoke these internal operations.
  if p_operation='EMERGENCY_ROSTER_CONTEXT' then
    if p_input - 'emergency_shift_token' <> '{}'::jsonb then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;
    select * into v_candidate from public.candidates where id=v_candidate_id and active;
    if v_candidate.id is null or nullif(btrim(v_candidate.phone),'') is null then
      raise exception using errcode='55000',message='SOURCE_IDENTITY_NOT_READY';
    end if;
    for v_shift in
      select jsonb_build_object(
        'emergency_shift_token',private._candidate_daily_emergency_token_v1(v_environment,v_candidate_id,g.generation_id,d.rota_date,d.source_row_hash),
        'candidate',jsonb_build_object('display_name',coalesce(nullif(btrim(v_candidate.display_name),''),btrim(concat_ws(' ',v_candidate.first_name,v_candidate.last_name))),'callable_mobile',btrim(v_candidate.phone)),
        'shift',jsonb_build_object('date',d.rota_date,'starts_at',d.shift_starts_at,'ends_at',d.shift_ends_at,
          'hospital',d.hospital,'ward',d.ward,'job_title',d.job_title,'booking_reference',d.booking_ref,
          'shift_type',d.shift_type,'shift_info',d.shift_info))
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

        and (not (p_input ? 'emergency_shift_token') or private._candidate_daily_emergency_token_v1(
          v_environment,v_candidate_id,g.generation_id,d.rota_date,d.source_row_hash)=p_input->>'emergency_shift_token')
      order by d.shift_starts_at limit 6
    loop v_shifts:=v_shifts||jsonb_build_array(v_shift); end loop;
    if jsonb_array_length(v_shifts)>5 then raise exception using errcode='55000',message='DEPENDENCY_UNAVAILABLE'; end if;
    if p_input ? 'emergency_shift_token' and jsonb_array_length(v_shifts)<>1 then raise exception using errcode='02000',message='NOT_FOUND'; end if;
    return jsonb_build_object('anchors',v_shifts);
  end if;

  if p_operation='EMERGENCY_ROSTER_PUBLISH' then
    if p_input - array['emergency_shift_token','roster'] <> '{}'::jsonb
       or jsonb_typeof(p_input->'roster') is distinct from 'object'
       or (p_input->'roster') - array['emergency_shift_token','groups','captured_at'] <> '{}'::jsonb
       or p_input#>>'{roster,emergency_shift_token}' is distinct from p_input->>'emergency_shift_token'
       or octet_length(p_input::text)>65536 then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;
    -- Revalidate the CURRENT generation and booking after Google has responded.
    v_shift:=public.candidate_daily_specialist_read_v1(p_internal_context,'EMERGENCY_ROSTER_CONTEXT',
      jsonb_build_object('emergency_shift_token',p_input->>'emergency_shift_token'),p_now_utc,p_correlation_id);
    v_groups:=p_input#>'{roster,groups}';
    if jsonb_typeof(v_groups) is distinct from 'object' or v_groups - array['current','previous','next'] <> '{}'::jsonb then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
    select * into v_candidate from public.candidates where id=v_candidate_id and active;
    foreach v_group in array array['current','previous','next'] loop
      if jsonb_typeof(v_groups->v_group) is distinct from 'array' or jsonb_array_length(v_groups->v_group)>100 then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      for v_contact in select value from jsonb_array_elements(v_groups->v_group) loop
        if jsonb_typeof(v_contact) is distinct from 'object'
          or v_contact - array['display_name','role','callable_mobile'] <> '{}'::jsonb
          or jsonb_typeof(v_contact->'display_name') is distinct from 'string'
          or length(btrim(v_contact->>'display_name')) not between 1 and 200
          or jsonb_typeof(v_contact->'role') is distinct from 'string' or length(v_contact->>'role')>200
          or coalesce(v_contact->>'callable_mobile','') !~ '^447[0-9]{9}$'
          or v_contact->>'callable_mobile'=(case when regexp_replace(v_candidate.phone,'[^0-9]','','g') ~ '^07[0-9]{9}$'
            then '44'||substring(regexp_replace(v_candidate.phone,'[^0-9]','','g') from 2)
            else regexp_replace(v_candidate.phone,'[^0-9]','','g') end) then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
      end loop;
      if (select count(*)<>count(distinct value->>'callable_mobile') from jsonb_array_elements(v_groups->v_group)) then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
    end loop;
    v_observed:=(p_input#>>'{roster,captured_at}')::timestamptz;
    if v_observed is null or v_observed<clock_timestamp()-interval '60 seconds' or v_observed>clock_timestamp()+interval '30 seconds' then
      raise exception using errcode='55000',message='DEPENDENCY_UNAVAILABLE';
    end if;
    insert into private.candidate_daily_emergency_roster_snapshots as snapshot
      (environment,candidate_id,emergency_shift_token,groups_json,observed_at_utc,expires_at_utc,source_sha256)
    values(v_environment,v_candidate_id,p_input->>'emergency_shift_token',v_groups,v_observed,v_observed+interval '5 minutes',
      private._candidate_daily_json_sha256_v1(v_groups))
    on conflict(environment,candidate_id,emergency_shift_token) do update set
      groups_json=excluded.groups_json,observed_at_utc=excluded.observed_at_utc,
      expires_at_utc=excluded.expires_at_utc,source_sha256=excluded.source_sha256
    where snapshot.observed_at_utc<=excluded.observed_at_utc;
    delete from private.candidate_daily_emergency_roster_snapshots where environment=v_environment
      and candidate_id=v_candidate_id and expires_at_utc<clock_timestamp()-interval '7 days';
    return jsonb_build_object('accepted',true);
  end if;

  if p_operation='EFFECT_REPLAY' then
    if p_input - array['operation','input','idempotency_key'] <> '{}'::jsonb
      or coalesce(p_input->>'operation','') not in ('RUNNING_LATE_SEND','CANNOT_ATTEND','LEAVE_EARLY','DNA','MESSAGE_SEEN')
      or jsonb_typeof(p_input->'input') is distinct from 'object'
      or coalesce(p_input->>'idempotency_key','') !~ '^[A-Za-z0-9._~:+/-]{16,128}$' then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
    v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
      'operation',p_input->>'operation','candidate_id',v_candidate_id,'input',p_input->'input'));
    select * into v_receipt from private.candidate_daily_external_effect_receipts
      where environment=v_environment and candidate_id=v_candidate_id and operation=p_input->>'operation'
        and idempotency_key=p_input->>'idempotency_key';
    if v_receipt.effect_receipt_id is null then return jsonb_build_object('state','ABSENT'); end if;
    if v_receipt.request_hash is distinct from v_request_hash then raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT'; end if;
    -- Exact candidate, request hash and idempotency ownership were checked above.
    -- A delayed executor is a status read, never authority to claim/send again.
    if v_receipt.state='IN_PROGRESS' then
      return jsonb_build_object('state','IN_PROGRESS','safe_result',
        public.candidate_daily_effect_status_candidate_v1(p_internal_context,
          v_receipt.effect_key,p_now_utc,p_correlation_id));
    end if;
    return jsonb_build_object('state',v_receipt.state,'safe_result',v_receipt.terminal_result_json);
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


revoke all on function private._candidate_daily_emergency_contacts_v1(text,uuid,text,text,text,date,text,timestamptz,timestamptz) from public,anon,authenticated,service_role;
alter function private._candidate_daily_emergency_contacts_v1(text,uuid,text,text,text,date,text,timestamptz,timestamptz) owner to postgres;
revoke all on function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) from public,anon,authenticated;
grant execute on function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) to service_role;
alter function public.candidate_daily_specialist_read_v1(jsonb,text,jsonb,timestamptz,text) owner to postgres;
