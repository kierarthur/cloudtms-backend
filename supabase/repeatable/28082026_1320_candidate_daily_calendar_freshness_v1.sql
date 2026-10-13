-- A complete published Rota has no read TTL, including at London midnight.
-- The separate strict 120-second controlled-cutover proof is unchanged.
begin;

create or replace function private._candidate_daily_freshness_v1(
  p_environment text,p_candidate_id uuid,p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_oldest integer:=0; v_reasons jsonb:='[]'::jsonb; v_age integer:=0;
  v_today date:=(p_now_utc at time zone 'Europe/London')::date;
  v_deadline timestamptz; v_max_age integer:=1; v_days integer;
  v_first date; v_last date;
begin
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=p_environment and candidate_id=p_candidate_id;
  if v_scope.active_generation_id is not null then
    select * into v_generation from public.candidate_daily_rota_generations
      where generation_id=v_scope.active_generation_id
        and environment=p_environment and candidate_id=p_candidate_id;
  end if;
  select * into v_sync from private.candidate_daily_sync_state
    where environment=p_environment and candidate_id=p_candidate_id and target='MASTER_AVAILABILITY_SHEET';
  select coalesce(greatest(0,extract(epoch from p_now_utc-min(o.created_at_utc))::integer),0)
    into v_oldest from public.candidate_daily_sheet_projection_outbox o
    where o.environment=p_environment and o.candidate_id=p_candidate_id
      and o.state in ('PENDING','RETRY','CLAIMED');
  if v_generation.generation_id is null then
    v_reasons:=v_reasons||'"GENERATION_MISSING"'::jsonb;
  else
    v_age:=greatest(0,extract(epoch from p_now_utc-v_generation.published_at_utc)::integer);
    select count(*),min(rota_date),max(rota_date) into v_days,v_first,v_last
      from public.candidate_daily_rota_days
      where generation_id=v_generation.generation_id
        and environment=p_environment and candidate_id=p_candidate_id;
    if v_generation.state<>'ACTIVE' or v_generation.expected_day_count<>14
       or v_generation.actual_day_count<>14 or v_days<>14
       or v_first is distinct from v_generation.window_start
       or v_last is distinct from v_generation.window_end
       or v_generation.window_end is distinct from v_generation.window_start+13 then
      v_reasons:=v_reasons||'"GENERATION_INCOMPLETE"'::jsonb;
    end if;
    -- No artificial re-dating or two-minute republishing of unchanged shifts.
    -- The expected refresh age remains diagnostic, not a visibility deadline.
    -- Retain the last complete window until its replacement is atomically published.
    v_deadline:=(v_generation.window_start+1)::timestamp at time zone 'Europe/London';
    v_max_age:=greatest(1,ceil(extract(epoch from v_deadline-v_generation.published_at_utc))::integer);
    if p_now_utc is null or v_generation.published_at_utc is null
       or v_generation.window_start>v_today then
      v_reasons:=v_reasons||'"GENERATION_STALE"'::jsonb;
    end if;
  end if;
  if coalesce(v_sync.effective_visible_cursor,0)<coalesce(v_sync.required_visible_cursor,0) then
    v_reasons:=v_reasons||'"PROJECTION_LAG"'::jsonb;
  end if;
  if coalesce(v_sync.terminal_count,0)>0 then v_reasons:=v_reasons||'"TERMINAL_OUTBOX"'::jsonb; end if;
  if not exists(select 1 from private.candidate_daily_source_links l where l.environment=p_environment
    and l.candidate_id=p_candidate_id and l.state in ('PRIMARY','OVERLAP')) then
    v_reasons:=v_reasons||'"IDENTITY_NOT_READY"'::jsonb;
  end if;
  return jsonb_build_object(
    'generation_version',coalesce(v_generation.generation_version,1),
    'generation_published_at',coalesce(v_generation.published_at_utc,p_now_utc),
    'generation_age_seconds',v_age,
    'canonical_version',coalesce(v_scope.canonical_version,0),
    'accepted_canonical_cursor',coalesce(v_sync.accepted_canonical_cursor,0),
    'required_visible_cursor',coalesce(v_sync.required_visible_cursor,0),
    'delivered_visible_cursor',coalesce(v_sync.delivered_visible_cursor,0),
    'overlay_proof_cursor',coalesce(v_sync.overlay_proof_cursor,0),
    'effective_visible_cursor',coalesce(v_sync.effective_visible_cursor,0),
    'projection_oldest_pending_seconds',v_oldest,
    'generation_max_age_seconds',v_max_age,'projection_warning_seconds',120,
    'ready',jsonb_array_length(v_reasons)=0,'reasons',v_reasons
  );
end;
$function$;

alter function private._candidate_daily_freshness_v1(text,uuid,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_freshness_v1(text,uuid,timestamptz)
  from public,anon,authenticated,service_role;

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
    'timesheet_eligible',d.timesheet_eligible,
    'action_target',case d.action_target_kind
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
      else null end
  )) order by d.rota_date) into v_tiles
  from public.candidate_daily_rota_days d
  left join public.candidate_daily_availability_days a
    on a.environment=d.environment and a.candidate_id=d.candidate_id
    and a.availability_date=d.rota_date
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

create or replace function public.candidate_daily_availability_apply_atomic_v1(
  p_internal_context jsonb,
  p_idempotency_key text,
  p_expected_availability_version bigint,
  p_changes jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype; v_generation_id uuid;
  v_generation public.candidate_daily_rota_generations%rowtype; v_freshness jsonb;
  v_request_hash text; v_receipt public.candidate_daily_command_receipts%rowtype;
  v_command_id uuid:=gen_random_uuid(); v_new_version bigint; v_change jsonb; v_dates jsonb;
  v_row public.candidate_daily_rota_days%rowtype; v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
  v_environment:=v_context->>'environment'; v_candidate_id:=(v_context->>'candidate_id')::uuid;
  if p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or p_expected_availability_version<0 or jsonb_typeof(p_changes)<>'array'
     or jsonb_array_length(p_changes) not between 1 and 14
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  if exists(select 1 from jsonb_array_elements(p_changes) c
    where (select count(*) from jsonb_object_keys(c))<>2
      or not(c ? 'date' and c ? 'availability')
      or c->>'availability' not in ('PENDING','NOT_AVAILABLE','LONG_DAY','NIGHT','LONG_DAY_OR_NIGHT')
      or c->>'date' !~ '^\d{4}-\d{2}-\d{2}$')
     or (select count(*)<>count(distinct c->>'date') from jsonb_array_elements(p_changes)c) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','AVAILABILITY_APPLY','candidate_id',v_candidate_id,
    'expected_availability_version',p_expected_availability_version,'changes',p_changes));
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
    command_class,idempotency_key,request_sha256,canonical_version_before,state,correlation_id)
  values(v_command_id,v_environment,v_candidate_id,'CANDIDATE','AVAILABILITY_APPLY',p_idempotency_key,
    v_request_hash,p_expected_availability_version,'IN_PROGRESS',p_correlation_id)
  on conflict(environment,candidate_id,actor_class,idempotency_key) do nothing;
  select * into v_receipt from public.candidate_daily_command_receipts
    where environment=v_environment and candidate_id=v_candidate_id and actor_class='CANDIDATE'
      and idempotency_key=p_idempotency_key for update;
  if v_receipt.request_sha256<>v_request_hash then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_receipt.state<>'IN_PROGRESS' then
    return v_receipt.terminal_body_json||jsonb_build_object('_idempotent_replay',true);
  end if;
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=v_environment and candidate_id=v_candidate_id for update;
  if v_scope.canonical_version<>p_expected_availability_version then
    v_terminal:=jsonb_build_object('error_code','AVAILABILITY_VERSION_CONFLICT',
      'current_availability_version',v_scope.canonical_version);
    update public.candidate_daily_command_receipts set canonical_version_after=v_scope.canonical_version,
      state='FAILED_FINAL',terminal_http_status=409,terminal_body_json=v_terminal,
      terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
      completed_at_utc=now(),updated_at_utc=now() where command_id=v_receipt.command_id;
    return v_terminal;
  end if;
  v_generation_id:=v_scope.active_generation_id;
  select * into v_generation from public.candidate_daily_rota_generations
    where generation_id=v_generation_id and environment=v_environment and candidate_id=v_candidate_id;
  if v_generation.generation_id is null or v_generation.state<>'ACTIVE'
     or v_generation.actual_day_count<>14 or v_generation.expected_day_count<>14 then
    v_terminal:=jsonb_build_object('error_code','GENERATION_INCOMPLETE');
    update public.candidate_daily_command_receipts set canonical_version_after=v_scope.canonical_version,
      state='FAILED_FINAL',terminal_http_status=422,terminal_body_json=v_terminal,
      terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
      completed_at_utc=now(),updated_at_utc=now() where command_id=v_receipt.command_id;
    return v_terminal;
  end if;
  v_freshness:=private._candidate_daily_freshness_v1(v_environment,v_candidate_id,now());
  if coalesce((v_freshness->>'ready')::boolean,false) is not true then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  for v_change in select value from jsonb_array_elements(p_changes) loop
    select * into v_row from public.candidate_daily_rota_days
      where generation_id=v_generation_id and rota_date=(v_change->>'date')::date;
    if v_row.generation_id is null or v_row.booked or v_row.system_blocked
       or v_row.rota_date<(now() at time zone 'Europe/London')::date then
      v_terminal:=jsonb_build_object('error_code','AVAILABILITY_DATE_NOT_EDITABLE',
        'date',v_change->>'date');
      update public.candidate_daily_command_receipts set canonical_version_after=v_scope.canonical_version,
        state='FAILED_FINAL',terminal_http_status=422,terminal_body_json=v_terminal,
        terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
        completed_at_utc=now(),updated_at_utc=now() where command_id=v_receipt.command_id;
      return v_terminal;
    end if;
  end loop;
  v_new_version:=v_scope.canonical_version+1;
  update private.candidate_daily_authority_scopes set canonical_version=v_new_version,updated_at_utc=now()
    where environment=v_environment and candidate_id=v_candidate_id;
  for v_change in select value from jsonb_array_elements(p_changes) loop
    insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,
      preference,availability_version,source_class,source_command_id,changed_by_class,row_hash)
    values(v_environment,v_candidate_id,(v_change->>'date')::date,v_change->>'availability',v_new_version,
      'CANDIDATE',v_receipt.command_id,'CANDIDATE',private._candidate_daily_json_sha256_v1(v_change))
    on conflict(environment,candidate_id,availability_date) do update set
      preference=excluded.preference,availability_version=excluded.availability_version,
      source_class=excluded.source_class,source_command_id=excluded.source_command_id,
      changed_at_utc=now(),changed_by_class=excluded.changed_by_class,row_hash=excluded.row_hash;
    insert into public.candidate_daily_sheet_projection_outbox(environment,candidate_id,availability_date,
      availability_version,preference,command_id,correlation_id)
    values(v_environment,v_candidate_id,(v_change->>'date')::date,v_new_version,
      v_change->>'availability',v_receipt.command_id,p_correlation_id);
  end loop;
  insert into private.candidate_daily_sync_state(environment,candidate_id,target,accepted_canonical_cursor,
    required_visible_cursor,effective_visible_cursor,state)
  values(v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_new_version,v_new_version,
    greatest(0,v_new_version-1),'LAGGING')
  on conflict(environment,candidate_id,target) do update set
    accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,v_new_version),
    required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,v_new_version),
    state='LAGGING',updated_at_utc=now();
  perform private._candidate_daily_refresh_sync_state_v1(
    v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',now());
  select jsonb_agg(c->>'date' order by ord) into v_dates
    from jsonb_array_elements(p_changes) with ordinality x(c,ord);
  v_terminal:=jsonb_build_object('command_id',v_receipt.command_id,'availability_version',v_new_version,
    'changed_dates',v_dates);
  update public.candidate_daily_command_receipts set canonical_version_after=v_new_version,state='COMPLETED',
    terminal_http_status=200,terminal_body_json=v_terminal,
    terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),completed_at_utc=now(),updated_at_utc=now()
  where command_id=v_receipt.command_id;
  return v_terminal;
end;
$function$;

alter function public.candidate_daily_tiles_get_v1(jsonb,date,integer) owner to postgres;
alter function public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text) owner to postgres;
revoke all on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) from public,anon,authenticated;
revoke all on function public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text) from public,anon,authenticated;
grant execute on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) to service_role;
grant execute on function public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text) to service_role;
notify pgrst, 'reload schema';

commit;
