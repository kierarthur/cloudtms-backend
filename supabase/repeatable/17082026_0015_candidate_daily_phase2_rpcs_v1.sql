create or replace function private._candidate_daily_json_sha256_v1(p_value jsonb)
returns text
language sql
immutable
set search_path=''
as $function$
  select encode(extensions.digest(convert_to(coalesce(p_value,'null'::jsonb)::text,'UTF8'),'sha256'),'hex');
$function$;

create or replace function private._candidate_daily_refresh_sync_state_v1(
  p_environment text,
  p_candidate_id uuid,
  p_target text default 'MASTER_AVAILABILITY_SHEET',
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_delivered bigint:=0;
  v_overlay bigint:=0;
  v_effective bigint:=0;
  v_pending integer:=0;
  v_retry integer:=0;
  v_deferred integer:=0;
  v_terminal integer:=0;
  v_state text:='LAGGING';
begin
  select * into v_scope
  from private.candidate_daily_authority_scopes s
  where s.environment=p_environment and s.candidate_id=p_candidate_id;
  if v_scope.candidate_id is null then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;

  select
    coalesce(min(o.availability_version) filter(where o.state<>'DELIVERED')-1,v_scope.canonical_version),
    coalesce(max(o.availability_version) filter(where o.state='DEFERRED_OVERLAY'
      and o.overlay_generation_id=v_scope.active_generation_id
      and exists(
        select 1
        from public.candidate_daily_rota_generations g
        join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
          and d.environment=o.environment and d.candidate_id=o.candidate_id
          and d.rota_date=o.availability_date
        where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE'
          and g.generation_version=o.overlay_generation_version
          and (d.booked or d.system_blocked)
          and d.source_row_hash=o.overlay_source_row_hash
      )),0),
    coalesce(min(o.availability_version) filter(where not(
      o.state='DELIVERED'
      or (o.state='DEFERRED_OVERLAY'
        and o.overlay_generation_id=v_scope.active_generation_id
        and exists(
          select 1
          from public.candidate_daily_rota_generations g
          join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
            and d.environment=o.environment and d.candidate_id=o.candidate_id
            and d.rota_date=o.availability_date
          where g.generation_id=v_scope.active_generation_id and g.state='ACTIVE'
            and g.generation_version=o.overlay_generation_version
            and (d.booked or d.system_blocked)
            and d.source_row_hash=o.overlay_source_row_hash
        ))) )-1,v_scope.canonical_version),
    count(*) filter(where o.state in ('PENDING','CLAIMED')),
    count(*) filter(where o.state='RETRY'),
    count(*) filter(where o.state='DEFERRED_OVERLAY'),
    count(*) filter(where o.state='TERMINAL')
  into v_delivered,v_overlay,v_effective,v_pending,v_retry,v_deferred,v_terminal
  from public.candidate_daily_sheet_projection_outbox o
  where o.environment=p_environment and o.candidate_id=p_candidate_id and o.target=p_target;

  v_delivered:=greatest(0,least(v_scope.canonical_version,coalesce(v_delivered,0)));
  v_overlay:=greatest(0,least(v_scope.canonical_version,coalesce(v_overlay,0)));
  v_effective:=greatest(0,least(v_scope.canonical_version,coalesce(v_effective,0)));
  if v_terminal>0 then v_state:='ERROR';
  elsif v_effective>=v_scope.canonical_version then v_state:='READY';
  else v_state:='LAGGING'; end if;

  insert into private.candidate_daily_sync_state(environment,candidate_id,target,
    accepted_canonical_cursor,required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,
    effective_visible_cursor,pending_count,retry_count,deferred_count,terminal_count,state,
    last_acknowledged_at_utc,updated_at_utc)
  values(p_environment,p_candidate_id,p_target,v_scope.canonical_version,v_scope.canonical_version,
    v_delivered,v_overlay,v_effective,v_pending,v_retry,v_deferred,v_terminal,v_state,p_now_utc,p_now_utc)
  on conflict(environment,candidate_id,target) do update set
    accepted_canonical_cursor=excluded.accepted_canonical_cursor,
    required_visible_cursor=excluded.required_visible_cursor,
    delivered_visible_cursor=excluded.delivered_visible_cursor,
    overlay_proof_cursor=excluded.overlay_proof_cursor,
    effective_visible_cursor=excluded.effective_visible_cursor,
    pending_count=excluded.pending_count,retry_count=excluded.retry_count,
    deferred_count=excluded.deferred_count,terminal_count=excluded.terminal_count,
    state=excluded.state,last_acknowledged_at_utc=excluded.last_acknowledged_at_utc,
    updated_at_utc=excluded.updated_at_utc;

  return jsonb_build_object('delivered_visible_cursor',v_delivered,
    'overlay_proof_cursor',v_overlay,'effective_visible_cursor',v_effective,
    'pending_count',v_pending,'retry_count',v_retry,'deferred_count',v_deferred,
    'terminal_count',v_terminal,'state',v_state);
end;
$function$;

create or replace function public.candidate_daily_legacy_availability_apply_atomic_v1(
  p_internal_context jsonb,
  p_candidate_source_hmac text,
  p_request_id uuid,
  p_idempotency_key text,
  p_changes jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype; v_receipt public.candidate_daily_command_receipts%rowtype;
  v_request_hash text; v_change jsonb; v_preference text; v_outcomes jsonb:='[]'::jsonb;
  v_accepted jsonb:='[]'::jsonb; v_seen text[]:='{}'; v_reason text; v_row public.candidate_daily_rota_days%rowtype;
  v_new_version bigint; v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
  v_environment:=v_context->>'environment';
  v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,p_candidate_source_hmac);
  if p_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_changes)<>'array' or jsonb_array_length(p_changes) not between 1 and 14
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','LEGACY_AVAILABILITY_APPLY','candidate_source_hmac',p_candidate_source_hmac,
    'request_id',p_request_id,'changes',p_changes));
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
    command_class,idempotency_key,request_sha256,canonical_version_before,state,correlation_id)
  select p_request_id,v_environment,v_candidate_id,'LEGACY_ADAPTER','LEGACY_AVAILABILITY_APPLY',
    p_idempotency_key,v_request_hash,s.canonical_version,'IN_PROGRESS',p_correlation_id
  from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=v_candidate_id
  on conflict do nothing;
  select * into v_receipt from public.candidate_daily_command_receipts
    where command_id=p_request_id or (environment=v_environment and candidate_id=v_candidate_id
      and actor_class='LEGACY_ADAPTER' and idempotency_key=p_idempotency_key)
    order by (command_id=p_request_id) desc limit 1 for update;
  if v_receipt.command_id is null then raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY'; end if;
  if v_receipt.candidate_id<>v_candidate_id or v_receipt.request_sha256<>v_request_hash then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_receipt.state<>'IN_PROGRESS' then
    return v_receipt.terminal_body_json||jsonb_build_object('_idempotent_replay',true);
  end if;
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=v_environment and candidate_id=v_candidate_id for update;
  for v_change in select value from jsonb_array_elements(p_changes) loop
    v_reason:=null; v_preference:=private._candidate_daily_preference_v1(v_change->>'availability');
    if (select count(*) from jsonb_object_keys(v_change))<>2
       or not(v_change ? 'date' and v_change ? 'availability') or v_preference is null
       or v_change->>'date' !~ '^\d{4}-\d{2}-\d{2}$' then v_reason:='INVALID_VALUE';
    elsif (v_change->>'date')=any(v_seen) then v_reason:='DUPLICATE_DATE';
    else
      v_seen:=array_append(v_seen,v_change->>'date');
      select * into v_row from public.candidate_daily_rota_days
        where generation_id=v_scope.active_generation_id and rota_date=(v_change->>'date')::date;
      if v_row.generation_id is null then v_reason:='OUTSIDE_WINDOW';
      elsif v_row.booked then v_reason:='BOOKED';
      elsif v_row.system_blocked then v_reason:='BLOCKED';
      elsif v_scope.authority_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
        or v_scope.transition_in_progress then v_reason:='NOT_EDITABLE'; end if;
    end if;
    if v_reason is null then
      v_accepted:=v_accepted||jsonb_build_array(jsonb_build_object('date',v_change->>'date','preference',v_preference));
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('date',v_change->>'date','applied',true));
    else
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('date',coalesce(v_change->>'date','1970-01-01'),
        'applied',false,'reason',v_reason));
    end if;
  end loop;
  if jsonb_array_length(v_accepted)>0 then
    v_new_version:=v_scope.canonical_version+1;
    update private.candidate_daily_authority_scopes set canonical_version=v_new_version,updated_at_utc=now()
      where environment=v_environment and candidate_id=v_candidate_id;
    for v_change in select value from jsonb_array_elements(v_accepted) loop
      insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,
        preference,availability_version,source_class,source_command_id,changed_by_class,row_hash)
      values(v_environment,v_candidate_id,(v_change->>'date')::date,v_change->>'preference',v_new_version,
        'LEGACY_ADAPTER',v_receipt.command_id,'LEGACY_ADAPTER',private._candidate_daily_json_sha256_v1(v_change))
      on conflict(environment,candidate_id,availability_date) do update set
        preference=excluded.preference,availability_version=excluded.availability_version,
        source_class=excluded.source_class,source_command_id=excluded.source_command_id,
        changed_at_utc=now(),changed_by_class=excluded.changed_by_class,row_hash=excluded.row_hash;
    end loop;
    insert into private.candidate_daily_sync_state(environment,candidate_id,target,accepted_canonical_cursor,
      required_visible_cursor,delivered_visible_cursor,effective_visible_cursor,state,last_acknowledged_at_utc)
    values(v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_new_version,v_new_version,
      v_new_version,v_new_version,'READY',now())
    on conflict(environment,candidate_id,target) do update set
      accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,v_new_version),
      required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,v_new_version),
      delivered_visible_cursor=greatest(private.candidate_daily_sync_state.delivered_visible_cursor,v_new_version),
      effective_visible_cursor=greatest(private.candidate_daily_sync_state.effective_visible_cursor,v_new_version),
      state='READY',last_acknowledged_at_utc=now(),updated_at_utc=now();
  else v_new_version:=null; end if;
  v_terminal:=jsonb_strip_nulls(jsonb_build_object('request_receipt_id',v_receipt.command_id,
    'committed_version',v_new_version,'outcomes',v_outcomes));
  update public.candidate_daily_command_receipts set canonical_version_after=coalesce(v_new_version,canonical_version_before),
    state='COMPLETED',terminal_http_status=200,terminal_body_json=v_terminal,
    terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),completed_at_utc=now(),updated_at_utc=now()
  where command_id=v_receipt.command_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_legacy_availability_status_get_v1(
  p_internal_context jsonb,
  p_candidate_source_hmac text,
  p_request_id uuid,
  p_correlation_id text
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_candidate_id uuid;
  v_receipt public.candidate_daily_command_receipts%rowtype;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
  v_environment:=v_context->>'environment';
  v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,p_candidate_source_hmac);
  select * into v_receipt from public.candidate_daily_command_receipts
  where command_id=p_request_id and environment=v_environment and candidate_id=v_candidate_id
    and actor_class='LEGACY_ADAPTER' and command_class='LEGACY_AVAILABILITY_APPLY';
  if v_receipt.command_id is null then raise exception using errcode='02000',message='NOT_FOUND'; end if;
  return jsonb_build_object('request_id',p_request_id,'state',v_receipt.state,
    'terminal_response',case when v_receipt.state='IN_PROGRESS' then null else
      jsonb_build_object('ok',true,'correlation_id',v_receipt.correlation_id,'result',v_receipt.terminal_body_json) end);
end;
$function$;

create or replace function public.candidate_daily_rota_generation_publish_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_items jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_item jsonb; v_day jsonb; v_index integer:=0; v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype; v_existing public.candidate_daily_rota_generations%rowtype;
  v_generation_id uuid; v_generation_version bigint; v_window_start date; v_outcomes jsonb:='[]'::jsonb;
  v_item_keys jsonb; v_terminal jsonb; v_dates date[]; v_error text;
  v_clear_count integer; v_new_version bigint; v_command_id uuid; v_clear_key text;
  v_clear_dates jsonb; v_command_body jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if p_batch_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_items)<>'array' or jsonb_array_length(p_items) not between 1 and 50
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'item_key' order by ord) into v_item_keys
    from jsonb_array_elements(p_items) with ordinality x(i,ord);
  if exists(select 1 from jsonb_array_elements(p_items)i where i->>'item_key' !~ '^[A-Za-z0-9._~-]{8,160}$')
     or jsonb_array_length(v_item_keys)<>(select count(distinct x) from jsonb_array_elements_text(v_item_keys)x) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','ROTA_GENERATION_PUBLISH','batch_request_id',p_batch_request_id,'items',p_items));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH',p_idempotency_key,
    v_request_hash,v_item_keys,jsonb_array_length(p_items),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts
    where environment=v_environment and actor_class='SIGNED_SYSTEM'
      and operation_class='ROTA_GENERATION_PUBLISH' and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash or v_batch.item_keys_json<>v_item_keys then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  perform 1 from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id in (
    select distinct private._candidate_daily_source_candidate_v1(v_environment,i->>'candidate_source_hmac')
    from jsonb_array_elements(p_items)i
  ) order by s.candidate_id for update;
  for v_item in select value from jsonb_array_elements(p_items) loop
    v_error:=null; v_generation_id:=null; v_generation_version:=null;
    begin
      if (select count(*) from jsonb_object_keys(v_item))<>8
         or not(v_item ?& array['candidate_source_hmac','source_event_id','source_revision','source_hash',
           'window_start','days','source_event_time','item_key'])
         or v_item->>'source_hash' !~ '^[a-f0-9]{64}$'
         or length(v_item->>'source_event_id') not between 8 and 160
         or length(v_item->>'source_revision') not between 1 and 160
         or jsonb_typeof(v_item->'days')<>'array' or jsonb_array_length(v_item->'days')<>14 then
        raise exception using errcode='22023',message='GENERATION_INCOMPLETE';
      end if;
      v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,v_item->>'candidate_source_hmac');
      v_window_start:=(v_item->>'window_start')::date;
      select array_agg((d->>'date')::date order by (d->>'date')::date) into v_dates
        from jsonb_array_elements(v_item->'days')d;
      if cardinality(v_dates)<>14 or (select count(distinct x) from unnest(v_dates)x)<>14
         or v_dates[1]<>v_window_start or v_dates[14]<>v_window_start+13
         or exists(select 1 from generate_series(0,13)n where v_dates[n+1]<>v_window_start+n) then
        raise exception using errcode='22023',message='GENERATION_INCOMPLETE';
      end if;
      if exists(select 1 from jsonb_array_elements(v_item->'days')d where
        (select count(*) from jsonb_object_keys(d))<4 or not(d ?& array['date','booked','system_blocked','source_row_hash'])
        or d->>'source_row_hash' !~ '^[a-f0-9]{64}$'
        or (coalesce((d->>'booked')::boolean,false) and (
          nullif(btrim(d->>'booking_id'),'') is null or nullif(d->>'shift_starts_at','') is null
          or nullif(d->>'shift_ends_at','') is null or (d->>'shift_ends_at')::timestamptz<=(d->>'shift_starts_at')::timestamptz))
        or (not coalesce((d->>'booked')::boolean,false) and (
          coalesce(d->'booking_id','null'::jsonb)<>'null'::jsonb
          or coalesce(d->'shift_starts_at','null'::jsonb)<>'null'::jsonb
          or coalesce(d->'shift_ends_at','null'::jsonb)<>'null'::jsonb))) then
        raise exception using errcode='22023',message='GENERATION_INCOMPLETE';
      end if;
      select * into v_existing from public.candidate_daily_rota_generations
      where environment=v_environment and source_system='MASTER_ROTA'
        and source_event_id=v_item->>'source_event_id' and item_key=v_item->>'item_key';
      if v_existing.generation_id is not null then
        if v_existing.source_hash<>v_item->>'source_hash' then
          raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
        end if;
        v_generation_id:=v_existing.generation_id; v_generation_version:=v_existing.generation_version;
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REPLAYED',
          'generation_id',v_generation_id,'generation_version',v_generation_version));
      else
        select * into v_scope from private.candidate_daily_authority_scopes
          where environment=v_environment and candidate_id=v_candidate_id for update;
        if v_scope.candidate_id is null or v_scope.transition_in_progress then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        select coalesce(max(g.generation_version),0)+1 into v_generation_version
          from public.candidate_daily_rota_generations g
          where g.environment=v_environment and g.candidate_id=v_candidate_id;
        v_generation_id:=gen_random_uuid();
        insert into public.candidate_daily_rota_generations(generation_id,environment,candidate_id,
          generation_version,window_start,window_end,state,expected_day_count,actual_day_count,source_system,
          source_event_id,source_revision,source_event_time,item_key,source_hash,generation_row_hash,
          batch_receipt_id,correlation_id)
        values(v_generation_id,v_environment,v_candidate_id,v_generation_version,v_window_start,v_window_start+13,
          'BUILDING',14,0,'MASTER_ROTA',v_item->>'source_event_id',v_item->>'source_revision',
          (v_item->>'source_event_time')::timestamptz,v_item->>'item_key',v_item->>'source_hash',
          private._candidate_daily_json_sha256_v1(v_item),v_batch.batch_receipt_id,p_correlation_id);
        for v_day in select value from jsonb_array_elements(v_item->'days') loop
          insert into public.candidate_daily_rota_days(generation_id,environment,candidate_id,rota_date,
            booked,system_blocked,booking_id,shift_starts_at,shift_ends_at,shift_info,hospital,ward,job_title,
            booking_ref,source_row_hash)
          values(v_generation_id,v_environment,v_candidate_id,(v_day->>'date')::date,
            (v_day->>'booked')::boolean,(v_day->>'system_blocked')::boolean,
            nullif(v_day->>'booking_id',''),nullif(v_day->>'shift_starts_at','')::timestamptz,
            nullif(v_day->>'shift_ends_at','')::timestamptz,nullif(v_day->>'shift_info',''),
            nullif(v_day->>'hospital',''),nullif(v_day->>'ward',''),nullif(v_day->>'job_title',''),
            null,v_day->>'source_row_hash');
        end loop;
        select count(*),jsonb_agg(a.availability_date order by a.availability_date)
          into v_clear_count,v_clear_dates
        from public.candidate_daily_availability_days a
        join public.candidate_daily_rota_days d on d.generation_id=v_generation_id
          and d.environment=a.environment and d.candidate_id=a.candidate_id
          and d.rota_date=a.availability_date
        where a.environment=v_environment and a.candidate_id=v_candidate_id
          and (d.booked or d.system_blocked) and a.preference<>'PENDING';
        if v_clear_count>0 then
          v_command_id:=gen_random_uuid();
          v_clear_key:=left('rota-clear:'||p_batch_request_id::text||':'||v_item->>'item_key',160);
          insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
            command_class,idempotency_key,request_sha256,source_system,source_event_id,source_revision,
            source_event_time,item_key,canonical_version_before,state,correlation_id)
          values(v_command_id,v_environment,v_candidate_id,'SIGNED_SYSTEM','ROTA_GENERATION_CLEAR',v_clear_key,
            private._candidate_daily_json_sha256_v1(jsonb_build_object('source_hash',v_item->>'source_hash',
              'clear_dates',v_clear_dates)),'MASTER_ROTA_GENERATION',v_item->>'source_event_id',
            v_item->>'source_revision',(v_item->>'source_event_time')::timestamptz,
            left('clear:'||v_item->>'item_key',160),v_scope.canonical_version,'IN_PROGRESS',p_correlation_id);
          update private.candidate_daily_authority_scopes
          set canonical_version=canonical_version+1,updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id
          returning canonical_version into v_new_version;
          update public.candidate_daily_availability_days a
          set preference='PENDING',availability_version=v_new_version,source_class='SIGNED_SYSTEM',
            source_command_id=v_command_id,changed_at_utc=now(),changed_by_class='SIGNED_SYSTEM',
            row_hash=private._candidate_daily_json_sha256_v1(jsonb_build_object(
              'date',a.availability_date,'preference','PENDING','availability_version',v_new_version,
              'source_event_id',v_item->>'source_event_id'))
          from public.candidate_daily_rota_days d
          where d.generation_id=v_generation_id and d.environment=a.environment
            and d.candidate_id=a.candidate_id and d.rota_date=a.availability_date
            and a.environment=v_environment and a.candidate_id=v_candidate_id
            and (d.booked or d.system_blocked) and a.preference<>'PENDING';
          insert into public.candidate_daily_sheet_projection_outbox(environment,candidate_id,availability_date,
            availability_version,preference,command_id,correlation_id)
          select v_environment,v_candidate_id,(value#>>'{}')::date,v_new_version,'PENDING',v_command_id,p_correlation_id
          from jsonb_array_elements(v_clear_dates);
          v_command_body:=jsonb_build_object('availability_version',v_new_version,'changed_dates',v_clear_dates);
          update public.candidate_daily_command_receipts set canonical_version_after=v_new_version,state='COMPLETED',
            terminal_http_status=200,terminal_body_json=v_command_body,
            terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_command_body),
            completed_at_utc=now(),updated_at_utc=now() where command_id=v_command_id;
          v_scope.canonical_version:=v_new_version;
        end if;
        update public.candidate_daily_rota_generations set state='SUPERSEDED',updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id and state='ACTIVE';
        update public.candidate_daily_rota_generations set state='ACTIVE',actual_day_count=14,
          activated_at_utc=now(),published_at_utc=now(),updated_at_utc=now()
          where generation_id=v_generation_id;
        update private.candidate_daily_authority_scopes set active_generation_id=v_generation_id,updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id;
        insert into private.candidate_daily_sync_state(environment,candidate_id,target,accepted_canonical_cursor,
          required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,effective_visible_cursor,
          observed_source_revision,state,last_pulled_at_utc)
        values(v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_scope.canonical_version,
          v_scope.canonical_version,0,0,0,v_item->>'source_revision','LAGGING',now())
        on conflict(environment,candidate_id,target) do update set
          accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,
            v_scope.canonical_version),
          required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,
            v_scope.canonical_version),
          observed_source_revision=excluded.observed_source_revision,last_pulled_at_utc=now(),updated_at_utc=now();

        update public.candidate_daily_sheet_projection_outbox o
        set state='PENDING',next_available_at_utc=now(),overlay_generation_id=null,
          overlay_generation_version=null,overlay_source_row_hash=null,completed_at_utc=null,updated_at_utc=now()
        where o.environment=v_environment and o.candidate_id=v_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET' and o.state='DEFERRED_OVERLAY'
          and not exists(
            select 1 from public.candidate_daily_rota_days d
            where d.generation_id=v_generation_id and d.environment=o.environment
              and d.candidate_id=o.candidate_id and d.rota_date=o.availability_date
              and (d.booked or d.system_blocked) and d.source_row_hash=o.overlay_source_row_hash
          );
        update public.candidate_daily_sheet_projection_outbox o
        set overlay_generation_id=v_generation_id,overlay_generation_version=v_generation_version,
          updated_at_utc=now()
        where o.environment=v_environment and o.candidate_id=v_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET' and o.state='DEFERRED_OVERLAY'
          and exists(
            select 1 from public.candidate_daily_rota_days d
            where d.generation_id=v_generation_id and d.environment=o.environment
              and d.candidate_id=o.candidate_id and d.rota_date=o.availability_date
              and (d.booked or d.system_blocked) and d.source_row_hash=o.overlay_source_row_hash
          );
        perform private._candidate_daily_refresh_sync_state_v1(
          v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',now());
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','COMMITTED',
          'generation_id',v_generation_id,'generation_version',v_generation_version));
      end if;
    exception when others then
      v_error:=sqlerrm;
      if v_error not in ('GENERATION_INCOMPLETE','SOURCE_EVENT_CONFLICT','IDENTITY_LINK_MISSING',
        'IDENTITY_LINK_AMBIGUOUS','CANDIDATE_DAILY_NOT_READY') then raise; end if;
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REJECTED','error_code',v_error));
    end;
    v_index:=v_index+1;
  end loop;
  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_projection_claim_v1(
  p_internal_context jsonb,
  p_claim_request_id uuid,
  p_idempotency_key text,
  p_target text,
  p_claimant text,
  p_max_items integer default 50,
  p_lease_seconds integer default 120,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_items jsonb:='[]'::jsonb; v_row public.candidate_daily_sheet_projection_outbox%rowtype;
  v_link text; v_token text; v_expiry timestamptz; v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if p_claim_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or p_target<>'MASTER_AVAILABILITY_SHEET' or length(btrim(p_claimant)) not between 8 and 128
     or p_max_items not between 1 and 100 or p_lease_seconds not between 30 and 600
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','PROJECTION_CLAIM','claim_request_id',p_claim_request_id,'target',p_target,
    'claimant',p_claimant,'max_items',p_max_items,'lease_seconds',p_lease_seconds));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,claim_request_id,claim_target,claim_limit,
    correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM','PROJECTION_CLAIM',p_idempotency_key,v_request_hash,
    jsonb_build_array('claim:'||p_claim_request_id::text),1,'IN_PROGRESS',p_claim_request_id,p_target,p_max_items,
    p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts
    where environment=v_environment and actor_class='SIGNED_SYSTEM' and operation_class='PROJECTION_CLAIM'
      and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash or v_batch.claim_request_id<>p_claim_request_id then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_batch.state<>'IN_PROGRESS' then
    if exists(select 1 from jsonb_array_elements(coalesce(v_batch.claimed_items_json,'[]'::jsonb))i
      where (i->>'lease_expires_at')::timestamptz<=now()) then
      raise exception using errcode='55000',message='LEASE_EXPIRED_STATUS_REQUIRED';
    end if;
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  if exists(select 1 from public.candidate_daily_sheet_projection_outbox o
    where o.environment=v_environment and o.target=p_target and o.state='CLAIMED'
      and o.lease_owner=p_claimant and o.lease_expires_at_utc>now()) then
    raise exception using errcode='55000',message='LEASE_CONFLICT';
  end if;
  v_expiry:=now()+make_interval(secs=>p_lease_seconds);
  for v_row in
    select o.* from public.candidate_daily_sheet_projection_outbox o
    where o.environment=v_environment and o.target=p_target
      and o.state in ('PENDING','RETRY') and o.next_available_at_utc<=now()
    order by o.created_at_utc,o.outbox_id
    for update skip locked limit p_max_items
  loop
    select l.identifier_hmac into v_link from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_row.candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID' and l.state='PRIMARY'
        and l.valid_from_utc<=now() and (l.valid_to_utc is null or l.valid_to_utc>now())
      order by l.hmac_key_version desc limit 1;
    if v_link is null then continue; end if;
    v_token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
    update public.candidate_daily_sheet_projection_outbox set state='CLAIMED',lease_owner=p_claimant,
      lease_token=v_token,lease_expires_at_utc=v_expiry,updated_at_utc=now() where outbox_id=v_row.outbox_id;
    v_items:=v_items||jsonb_build_array(jsonb_build_object('outbox_id',v_row.outbox_id,
      'lease_token',v_token,'lease_expires_at',v_expiry,'candidate_source_hmac',v_link,
      'date',v_row.availability_date,'availability_version',v_row.availability_version,
      'availability',private._candidate_daily_legacy_value_v1(v_row.preference)));
  end loop;
  v_terminal:=jsonb_build_object('claim_request_id',p_claim_request_id,'batch_receipt_id',v_batch.batch_receipt_id,
    'lease_set_expires_at',v_expiry,'items',v_items);
  update private.candidate_daily_batch_receipts set state='COMPLETED',lease_owner=p_claimant,
    lease_expires_at_utc=v_expiry,claimed_items_json=v_items,terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_projection_complete_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_items jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_item jsonb; v_row public.candidate_daily_sheet_projection_outbox%rowtype;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_day public.candidate_daily_rota_days%rowtype;
  v_item_keys jsonb; v_index integer:=0; v_state text; v_outcomes jsonb:='[]'::jsonb; v_terminal jsonb;
  v_sync jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if p_batch_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_items)<>'array' or jsonb_array_length(p_items) not between 1 and 100
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'outbox_id' order by ord) into v_item_keys
    from jsonb_array_elements(p_items) with ordinality x(i,ord);
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','PROJECTION_COMPLETE','batch_request_id',p_batch_request_id,'items',p_items));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM','PROJECTION_COMPLETE',p_idempotency_key,v_request_hash,
    v_item_keys,jsonb_array_length(p_items),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts
    where environment=v_environment and actor_class='SIGNED_SYSTEM' and operation_class='PROJECTION_COMPLETE'
      and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash then raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED'; end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  for v_item in select value from jsonb_array_elements(p_items) loop
    if not(v_item ?& array['outbox_id','lease_token','outcome'])
       or v_item->>'outcome' not in ('DELIVERED','RETRY','DEFERRED_OVERLAY','TERMINAL') then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
    select * into v_row from public.candidate_daily_sheet_projection_outbox
      where outbox_id=(v_item->>'outbox_id')::uuid and environment=v_environment for update;
    if v_row.outbox_id is null or v_row.state<>'CLAIMED' or v_row.lease_token<>v_item->>'lease_token' then
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'accepted',false,'state','LEASE_CONFLICT'));
    elsif v_row.lease_expires_at_utc<=now() then
      raise exception using errcode='55000',message='LEASE_EXPIRED_STATUS_REQUIRED';
    else
      v_state:=v_item->>'outcome';
      if v_state='DELIVERED' then
        if nullif(btrim(v_item->>'observed_sheet_revision'),'') is null
           or length(v_item->>'observed_sheet_revision')>160 then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        update public.candidate_daily_sheet_projection_outbox set state='DELIVERED',
          observed_sheet_revision=nullif(v_item->>'observed_sheet_revision',''),completed_at_utc=now(),
          overlay_generation_id=null,overlay_generation_version=null,overlay_source_row_hash=null,
          lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=now()
        where outbox_id=v_row.outbox_id;
      elsif v_state='RETRY' then
        update public.candidate_daily_sheet_projection_outbox set
          state=case when delivery_attempt_count+1>=12 then 'TERMINAL' else 'RETRY' end,
          delivery_attempt_count=delivery_attempt_count+1,
          next_available_at_utc=now()+least(interval '24 hours',make_interval(secs=>power(2,least(delivery_attempt_count+1,16))::integer)),
          safe_error_code=left(nullif(v_item->>'error_code',''),80),
          completed_at_utc=case when delivery_attempt_count+1>=12 then now() else null end,
          lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=now()
        where outbox_id=v_row.outbox_id returning state into v_state;
      elsif v_state='DEFERRED_OVERLAY' then
        if nullif(btrim(v_item->>'observed_sheet_revision'),'') is null
           or length(v_item->>'observed_sheet_revision')>160 then
          raise exception using errcode='22023',message='PROJECTION_STALE_COMPLETION';
        end if;
        select * into v_scope from private.candidate_daily_authority_scopes
          where environment=v_environment and candidate_id=v_row.candidate_id for update;
        select * into v_generation from public.candidate_daily_rota_generations
          where generation_id=v_scope.active_generation_id and environment=v_environment
            and candidate_id=v_row.candidate_id and state='ACTIVE';
        select * into v_day from public.candidate_daily_rota_days
          where generation_id=v_generation.generation_id and environment=v_environment
            and candidate_id=v_row.candidate_id and rota_date=v_row.availability_date;
        if v_generation.generation_id is null or v_day.generation_id is null
           or not(v_day.booked or v_day.system_blocked) then
          raise exception using errcode='55000',message='PROJECTION_STALE_COMPLETION';
        end if;
        update public.candidate_daily_sheet_projection_outbox set state='DEFERRED_OVERLAY',
          deferral_count=deferral_count+1,observed_sheet_revision=v_item->>'observed_sheet_revision',
          overlay_generation_id=v_generation.generation_id,
          overlay_generation_version=v_generation.generation_version,
          overlay_source_row_hash=v_day.source_row_hash,completed_at_utc=now(),
          lease_owner=null,lease_token=null,lease_expires_at_utc=null,
          updated_at_utc=now() where outbox_id=v_row.outbox_id;
      else
        update public.candidate_daily_sheet_projection_outbox set state='TERMINAL',
          safe_error_code=left(nullif(v_item->>'error_code',''),80),completed_at_utc=now(),
          lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=now()
        where outbox_id=v_row.outbox_id;
      end if;
      v_sync:=private._candidate_daily_refresh_sync_state_v1(
        v_environment,v_row.candidate_id,v_row.target,now());
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'accepted',true,
        'state',v_state,'delivered_visible_cursor',coalesce((v_sync->>'delivered_visible_cursor')::bigint,0)));
    end if;
    v_index:=v_index+1;
  end loop;
  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_sync_status_get_v1(
  p_internal_context jsonb,
  p_candidate_source_hmacs jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_hmac text; v_candidate_id uuid; v_items jsonb:='[]'::jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if jsonb_typeof(p_candidate_source_hmacs)<>'array'
     or jsonb_array_length(p_candidate_source_hmacs) not between 1 and 100 then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  for v_hmac in select value#>>'{}' from jsonb_array_elements(p_candidate_source_hmacs) loop
    v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,v_hmac);
    v_items:=v_items||jsonb_build_array(jsonb_build_object('candidate_source_hmac',v_hmac,
      'freshness',private._candidate_daily_freshness_v1(v_environment,v_candidate_id,now())));
  end loop;
  return jsonb_build_object('items',v_items);
end;
$function$;

create or replace function public.candidate_daily_reconciliation_apply_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_observations jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_route_operation text; v_operation_class text;
  v_request_hash text; v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_item_keys jsonb; v_item jsonb; v_index integer:=0; v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype; v_day public.candidate_daily_rota_days%rowtype;
  v_availability public.candidate_daily_availability_days%rowtype; v_preference text; v_command_id uuid;
  v_command public.candidate_daily_command_receipts%rowtype; v_item_hash text; v_item_key text;
  v_outcomes jsonb:='[]'::jsonb; v_terminal jsonb; v_classification text; v_error text;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  v_route_operation:=upper(coalesce(nullif(p_internal_context->>'route_operation',''),'RECONCILIATION'));
  if v_route_operation not in ('RECONCILIATION','SHEET_EDIT_INGEST') then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_operation_class:=case when v_route_operation='SHEET_EDIT_INGEST' then 'SHEET_EDIT_INGEST' else 'RECONCILIATION' end;
  if p_batch_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_observations)<>'array' or jsonb_array_length(p_observations) not between 1 and 100
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'item_key' order by ord) into v_item_keys
    from jsonb_array_elements(p_observations) with ordinality x(i,ord);
  if exists(select 1 from jsonb_array_elements(p_observations)i where i->>'item_key' !~ '^[A-Za-z0-9._~-]{8,160}$')
     or jsonb_array_length(v_item_keys)<>(select count(distinct x) from jsonb_array_elements_text(v_item_keys)x) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation',v_route_operation,
    'batch_request_id',p_batch_request_id,'items',p_observations));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM',v_operation_class,p_idempotency_key,v_request_hash,
    v_item_keys,jsonb_array_length(p_observations),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts where environment=v_environment
    and actor_class='SIGNED_SYSTEM' and operation_class=v_operation_class and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash then raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED'; end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  perform 1 from private.candidate_daily_authority_scopes s where s.environment=v_environment and s.candidate_id in (
    select distinct private._candidate_daily_source_candidate_v1(v_environment,i->>'candidate_source_hmac')
    from jsonb_array_elements(p_observations)i
  ) order by s.candidate_id for update;
  for v_item in select value from jsonb_array_elements(p_observations) loop
    v_error:=null; v_classification:=null;
    begin
      v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,v_item->>'candidate_source_hmac');
      if v_route_operation='SHEET_EDIT_INGEST' then
        if not(v_item ?& array['source_event_id','source_revision','editor_hmac','sheet','cell','date',
          'availability','source_event_time','source_hash','item_key'])
          or v_item->>'sheet'<>'Availability' or v_item->>'cell' !~ '^[A-Z]{1,3}[1-9][0-9]{0,5}$'
          or v_item->>'editor_hmac' !~ '^[a-f0-9]{64}$' or v_item->>'source_hash' !~ '^[a-f0-9]{64}$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_preference:=private._candidate_daily_preference_v1(v_item->>'availability');
        if v_preference is null then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;
        v_item_hash:=private._candidate_daily_json_sha256_v1(v_item);
        v_item_key:=left(p_idempotency_key||':item:'||v_item->>'item_key',160);
        v_command_id:=gen_random_uuid();
        select * into v_scope from private.candidate_daily_authority_scopes
          where environment=v_environment and candidate_id=v_candidate_id for update;
        insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
          command_class,idempotency_key,request_sha256,source_system,source_event_id,source_revision,
          source_event_time,item_key,canonical_version_before,state,correlation_id)
        values(v_command_id,v_environment,v_candidate_id,'SIGNED_SYSTEM','SHEET_EDIT_INGEST',v_item_key,
          v_item_hash,'MASTER_AVAILABILITY_SHEET',v_item->>'source_event_id',v_item->>'source_revision',
          (v_item->>'source_event_time')::timestamptz,v_item->>'item_key',v_scope.canonical_version,
          'IN_PROGRESS',p_correlation_id)
        on conflict do nothing;
        select * into v_command from public.candidate_daily_command_receipts where
          (environment=v_environment and source_system='MASTER_AVAILABILITY_SHEET'
            and source_event_id=v_item->>'source_event_id' and item_key=v_item->>'item_key')
          or (environment=v_environment and candidate_id=v_candidate_id and actor_class='SIGNED_SYSTEM'
            and idempotency_key=v_item_key)
          order by (source_event_id=v_item->>'source_event_id') desc limit 1 for update;
        if v_command.request_sha256<>v_item_hash then
          raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
        end if;
        if v_command.state='COMPLETED' then
          v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REPLAYED',
            'availability_version',v_command.canonical_version_after));
        else
          select * into v_day from public.candidate_daily_rota_days
            where generation_id=v_scope.active_generation_id and rota_date=(v_item->>'date')::date;
          if v_day.generation_id is null or v_day.booked or v_day.system_blocked then
            raise exception using errcode='22023',message='AVAILABILITY_DATE_NOT_EDITABLE';
          end if;
          update private.candidate_daily_authority_scopes set canonical_version=canonical_version+1,updated_at_utc=now()
            where environment=v_environment and candidate_id=v_candidate_id returning canonical_version into v_scope.canonical_version;
          insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,preference,
            availability_version,source_class,source_command_id,changed_by_class,row_hash)
          values(v_environment,v_candidate_id,(v_item->>'date')::date,v_preference,v_scope.canonical_version,
            'SIGNED_SYSTEM',v_command.command_id,'SIGNED_SYSTEM',v_item_hash)
          on conflict(environment,candidate_id,availability_date) do update set preference=excluded.preference,
            availability_version=excluded.availability_version,source_class=excluded.source_class,
            source_command_id=excluded.source_command_id,changed_by_class=excluded.changed_by_class,
            row_hash=excluded.row_hash,changed_at_utc=now();
          insert into private.candidate_daily_sync_state(environment,candidate_id,target,accepted_canonical_cursor,
            required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,effective_visible_cursor,
            observed_source_revision,state,last_acknowledged_at_utc)
          values(v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_scope.canonical_version,
            v_scope.canonical_version,v_scope.canonical_version,v_scope.canonical_version,v_scope.canonical_version,
            v_item->>'source_revision','READY',now())
          on conflict(environment,candidate_id,target) do update set
            accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,v_scope.canonical_version),
            required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,v_scope.canonical_version),
            delivered_visible_cursor=greatest(private.candidate_daily_sync_state.delivered_visible_cursor,v_scope.canonical_version),
            effective_visible_cursor=greatest(private.candidate_daily_sync_state.effective_visible_cursor,v_scope.canonical_version),
            observed_source_revision=excluded.observed_source_revision,state='READY',last_acknowledged_at_utc=now(),updated_at_utc=now();
          update public.candidate_daily_command_receipts set canonical_version_after=v_scope.canonical_version,
            state='COMPLETED',terminal_http_status=200,
            terminal_body_json=jsonb_build_object('availability_version',v_scope.canonical_version),
            terminal_body_sha256=private._candidate_daily_json_sha256_v1(jsonb_build_object('availability_version',v_scope.canonical_version)),
            completed_at_utc=now(),updated_at_utc=now() where command_id=v_command.command_id;
          v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','COMMITTED',
            'availability_version',v_scope.canonical_version));
        end if;
      else
        if not(v_item ?& array['date','observed_value','observed_sheet_revision','source_event_id',
          'source_revision','source_event_time','source_hash','item_key'])
          or v_item->>'source_hash' !~ '^[a-f0-9]{64}$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_preference:=private._candidate_daily_preference_v1(v_item->>'observed_value');
        select * into v_availability from public.candidate_daily_availability_days
          where environment=v_environment and candidate_id=v_candidate_id and availability_date=(v_item->>'date')::date;
        if v_preference is null then v_classification:='AMBIGUOUS';
        elsif v_availability.candidate_id is null then v_classification:='CANONICAL_COMMAND_REQUIRED';
        elsif v_availability.preference=v_preference then
          v_classification:='MATCH';
          update public.candidate_daily_sheet_projection_outbox
          set state='DELIVERED',observed_sheet_revision=v_item->>'observed_sheet_revision',
            overlay_generation_id=null,overlay_generation_version=null,overlay_source_row_hash=null,
            completed_at_utc=now(),lease_owner=null,lease_token=null,lease_expires_at_utc=null,
            updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id
            and target='MASTER_AVAILABILITY_SHEET'
            and availability_date=v_availability.availability_date
            and availability_version=v_availability.availability_version
            and preference=v_availability.preference and state<>'TERMINAL';
        else
          insert into public.candidate_daily_sheet_projection_outbox(environment,candidate_id,availability_date,
            availability_version,preference,command_id,correlation_id)
          values(v_environment,v_candidate_id,v_availability.availability_date,v_availability.availability_version,
            v_availability.preference,v_availability.source_command_id,p_correlation_id)
          on conflict(environment,target,candidate_id,availability_date,availability_version,operation)
          do update set state=case when public.candidate_daily_sheet_projection_outbox.state='TERMINAL'
            then 'TERMINAL' else 'PENDING' end,next_available_at_utc=now(),
            overlay_generation_id=null,overlay_generation_version=null,overlay_source_row_hash=null,
            completed_at_utc=null,updated_at_utc=now();
          v_classification:='REPAIR_PROJECTION';
        end if;
        update private.candidate_daily_sync_state set last_reconciled_at_utc=now(),
          observed_source_revision=v_item->>'observed_sheet_revision',updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id and target='MASTER_AVAILABILITY_SHEET';
        perform private._candidate_daily_refresh_sync_state_v1(
          v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',now());
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'classification',v_classification));
      end if;
    exception when others then
      v_error:=sqlerrm;
      if v_route_operation='SHEET_EDIT_INGEST' and v_error in ('SOURCE_EVENT_CONFLICT','AVAILABILITY_DATE_NOT_EDITABLE',
        'IDENTITY_LINK_MISSING','IDENTITY_LINK_AMBIGUOUS','VALIDATION_FAILED') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REJECTED','error_code',v_error));
      elsif v_route_operation='RECONCILIATION' and v_error in ('IDENTITY_LINK_MISSING','IDENTITY_LINK_AMBIGUOUS','VALIDATION_FAILED') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'classification','TERMINAL_CONFLICT','error_code',v_error));
      else raise; end if;
    end;
    v_index:=v_index+1;
  end loop;
  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_authority_transition_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_transition_items jsonb,
  p_independent_approver uuid,
  p_reason text,
  p_evidence_sha256 text,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_item_keys jsonb; v_item jsonb; v_candidate_id uuid; v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_transition_id uuid; v_index integer:=0; v_outcomes jsonb:='[]'::jsonb; v_terminal jsonb;
  v_prior_mode text; v_new_mode text; v_entitlement_before boolean:=false; v_entitlement_after boolean;
  v_actor uuid; v_error text; v_link jsonb; v_link_group uuid; v_link_state text;
  v_now timestamptz:=clock_timestamp(); v_global_enabled boolean:=false;
  v_expected_mode text; v_expected_version bigint; v_expected_entitlement boolean;
  v_expected_generation_id uuid; v_expected_generation_version bigint;
  v_expected_accepted_cursor bigint; v_expected_required_cursor bigint; v_expected_effective_cursor bigint;
  v_requested_disposition text; v_actual_disposition text; v_strict_barrier boolean:=false;
  v_source_primary_count integer:=0; v_source_group_count integer:=0; v_generation_day_count integer:=0;
  v_pending_count integer:=0; v_claimed_count integer:=0; v_retry_count integer:=0;
  v_deferred_count integer:=0; v_terminal_count integer:=0; v_invalid_overlay_count integer:=0;
  v_command_count integer:=0; v_other_batch_count integer:=0; v_effect_count integer:=0; v_unknown_effect_count integer:=0;
  v_latest_fact_at timestamptz; v_source_link_changed boolean:=false;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  begin v_actor:=nullif(p_internal_context->>'actor_user_id','')::uuid; exception when others then v_actor:=null; end;
  if p_batch_request_id is null or v_actor is null or p_independent_approver is null or p_independent_approver=v_actor
     or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_transition_items)<>'array' or jsonb_array_length(p_transition_items) not between 1 and 100
     or length(btrim(p_reason)) not between 1 and 500 or p_evidence_sha256 !~ '^[a-f0-9]{64}$'
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'candidate_id' order by ord) into v_item_keys
    from jsonb_array_elements(p_transition_items) with ordinality x(i,ord);
  if exists(select 1 from jsonb_array_elements(p_transition_items)i where
    i->>'candidate_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    or jsonb_array_length(v_item_keys)<>(select count(distinct x) from jsonb_array_elements_text(v_item_keys)x) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation','AUTHORITY_TRANSITION',
    'batch_request_id',p_batch_request_id,'items',p_transition_items,'independent_approver',p_independent_approver,
    'reason',p_reason,'evidence_sha256',p_evidence_sha256));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'OFFICE_ADMIN','AUTHORITY_TRANSITION',p_idempotency_key,v_request_hash,
    v_item_keys,jsonb_array_length(p_transition_items),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts where environment=v_environment
    and actor_class='OFFICE_ADMIN' and operation_class='AUTHORITY_TRANSITION' and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash then raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED'; end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  select coalesce(sd.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb,false)
    into v_global_enabled
  from public.settings_defaults sd where sd.id=1 for update;
  perform 1 from private.candidate_daily_authority_scopes s where s.environment=v_environment
    and s.candidate_id in (select (i->>'candidate_id')::uuid from jsonb_array_elements(p_transition_items)i)
    order by s.candidate_id for update;
  for v_item in select value from jsonb_array_elements(p_transition_items) loop
    v_error:=null; v_scope:=null; v_generation:=null; v_sync:=null; v_link:=null;
    v_link_group:=null; v_link_state:=null; v_source_link_changed:=false;
    v_prior_mode:=null; v_new_mode:=null; v_entitlement_before:=false; v_entitlement_after:=null;
    v_expected_mode:=null; v_expected_version:=null; v_expected_entitlement:=null;
    v_expected_generation_id:=null; v_expected_generation_version:=null;
    v_expected_accepted_cursor:=null; v_expected_required_cursor:=null; v_expected_effective_cursor:=null;
    v_requested_disposition:=null; v_actual_disposition:=null; v_strict_barrier:=false;
    v_latest_fact_at:=null;
    v_source_primary_count:=0; v_source_group_count:=0; v_generation_day_count:=0;
    v_pending_count:=0; v_claimed_count:=0; v_retry_count:=0; v_deferred_count:=0;
    v_terminal_count:=0; v_invalid_overlay_count:=0; v_command_count:=0;
    v_other_batch_count:=0; v_effect_count:=0; v_unknown_effect_count:=0;
    begin
      v_now:=clock_timestamp();
      v_candidate_id:=(v_item->>'candidate_id')::uuid;
      if not(v_item ?& array['candidate_id','expected_authority_mode','expected_canonical_version',
        'expected_entitlement_enabled','new_authority_mode','entitlement_enabled','in_flight_disposition'])
         or v_item->>'expected_canonical_version' !~ '^\d+$'
         or v_item->'expected_entitlement_enabled' not in ('true'::jsonb,'false'::jsonb)
         or v_item->'entitlement_enabled' not in ('true'::jsonb,'false'::jsonb) then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      v_expected_mode:=upper(v_item->>'expected_authority_mode');
      v_expected_version:=(v_item->>'expected_canonical_version')::bigint;
      v_expected_entitlement:=(v_item->>'expected_entitlement_enabled')::boolean;
      v_new_mode:=upper(v_item->>'new_authority_mode');
      v_entitlement_after:=(v_item->>'entitlement_enabled')::boolean;
      v_requested_disposition:=upper(v_item->>'in_flight_disposition');
      if v_expected_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
         or v_new_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
         or v_requested_disposition not in ('DRAINED','CANCELLED','RECONCILED','NONE') then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      select * into v_scope from private.candidate_daily_authority_scopes
        where environment=v_environment and candidate_id=v_candidate_id for update;
      if v_scope.candidate_id is null then
        raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
      end if;
      if v_scope.transition_in_progress then
        raise exception using errcode='55000',message='BATCH_IN_PROGRESS';
      end if;
      v_prior_mode:=v_scope.authority_mode;
      if v_expected_mode<>v_prior_mode or v_expected_version<>v_scope.canonical_version then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      select coalesce(e.enabled,false) into v_entitlement_before
      from private.candidate_daily_entitlements e
      where e.environment=v_environment and e.candidate_id=v_candidate_id for update;
      v_entitlement_before:=coalesce(v_entitlement_before,false);
      if v_expected_entitlement<>v_entitlement_before then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if not ((v_prior_mode='GOOGLE_PRIMARY' and v_new_mode in ('GOOGLE_PRIMARY','SUPABASE_PRIMARY'))
        or (v_prior_mode='SUPABASE_PRIMARY' and v_new_mode in ('SUPABASE_PRIMARY','ROLLBACK_PENDING'))
        or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode in ('ROLLBACK_PENDING','GOOGLE_PRIMARY'))) then
        raise exception using errcode='22023',message='SEMANTIC_REJECTION';
      end if;
      v_link:=v_item->'source_link';
      if v_prior_mode=v_new_mode and v_entitlement_before=v_entitlement_after and v_link is null then
        if v_requested_disposition<>'NONE' then
          raise exception using errcode='40001',message='SEMANTIC_REJECTION';
        end if;
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','NO_CHANGE',
          'candidate_id',v_candidate_id,'authority_mode',v_prior_mode,
          'entitlement_enabled',v_entitlement_before));
        v_index:=v_index+1;
        continue;
      end if;
      if v_new_mode in ('GOOGLE_PRIMARY','ROLLBACK_PENDING') and v_entitlement_after then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_entitlement_after and not v_global_enabled then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_new_mode in ('ROLLBACK_PENDING','GOOGLE_PRIMARY')
         and v_prior_mode in ('SUPABASE_PRIMARY','ROLLBACK_PENDING') and v_global_enabled then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      update private.candidate_daily_authority_scopes set transition_in_progress=true,updated_at_utc=v_now
      where environment=v_environment and candidate_id=v_candidate_id;
      if v_link is not null then
        if jsonb_typeof(v_link)<>'object' or not(v_link ?& array['identifier_hmac','hmac_key_version'])
           or v_link->>'identifier_hmac' !~ '^[a-f0-9]{64}$'
           or v_link->>'hmac_key_version' !~ '^[1-9]\d*$'
           or (nullif(v_link->>'link_group_id','') is not null and
             v_link->>'link_group_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_link_state:=upper(coalesce(nullif(v_link->>'state',''),'PRIMARY'));
        if v_link_state not in ('PRIMARY','OVERLAP') then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_link_group:=coalesce(nullif(v_link->>'link_group_id','')::uuid,gen_random_uuid());
        insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
          canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,rotation_receipt_id,
          actor_user_id,independent_approver_user_id,evidence_sha256)
        values(v_environment,v_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',v_link_group,
          v_link->>'identifier_hmac',(v_link->>'hmac_key_version')::integer,
          v_link_state,v_batch.batch_receipt_id,v_actor,
          p_independent_approver,p_evidence_sha256)
        on conflict(environment,source_system,hmac_key_version,identifier_hmac) where state in ('PRIMARY','OVERLAP')
        do nothing;
        if not exists(select 1 from private.candidate_daily_source_links l where l.environment=v_environment
          and l.candidate_id=v_candidate_id and l.identifier_hmac=v_link->>'identifier_hmac'
          and l.hmac_key_version=(v_link->>'hmac_key_version')::integer and l.state in ('PRIMARY','OVERLAP')) then
          raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
        end if;
        v_source_link_changed:=true;
      end if;

      perform 1 from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
      order by l.link_id for update;
      select count(*) filter(where l.state='PRIMARY' and l.valid_from_utc<=v_now
          and (l.valid_to_utc is null or l.valid_to_utc>v_now)),
        count(distinct l.link_group_id) filter(where l.state in ('PRIMARY','OVERLAP')
          and l.valid_from_utc<=v_now and (l.valid_to_utc is null or l.valid_to_utc>v_now))
      into v_source_primary_count,v_source_group_count
      from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID';
      if (v_source_link_changed or v_new_mode='SUPABASE_PRIMARY'
          or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY'))
         and v_source_primary_count=0 then
        raise exception using errcode='55000',message='IDENTITY_LINK_MISSING';
      end if;
      if (v_source_link_changed or v_new_mode='SUPABASE_PRIMARY'
          or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY'))
         and (v_source_primary_count<>1 or v_source_group_count<>1) then
        raise exception using errcode='55000',message='IDENTITY_LINK_AMBIGUOUS';
      end if;

      perform 1 from public.candidate_daily_command_receipts c
      where c.environment=v_environment and c.candidate_id=v_candidate_id and c.state='IN_PROGRESS'
      order by c.command_id for update;
      select count(*) into v_command_count from public.candidate_daily_command_receipts c
      where c.environment=v_environment and c.candidate_id=v_candidate_id and c.state='IN_PROGRESS';
      perform 1 from private.candidate_daily_batch_receipts b
      where b.environment=v_environment and b.state='IN_PROGRESS'
        and b.batch_receipt_id<>v_batch.batch_receipt_id
        and b.item_keys_json @> jsonb_build_array(v_candidate_id::text)
      order by b.batch_receipt_id for update;
      select count(*) into v_other_batch_count from private.candidate_daily_batch_receipts b
      where b.environment=v_environment and b.state='IN_PROGRESS'
        and b.batch_receipt_id<>v_batch.batch_receipt_id
        and b.item_keys_json @> jsonb_build_array(v_candidate_id::text);
      perform 1 from private.candidate_daily_external_effect_receipts e
      where e.environment=v_environment and e.candidate_id=v_candidate_id and e.state in ('IN_PROGRESS','UNKNOWN')
      order by e.effect_receipt_id for update;
      select count(*),count(*) filter(where e.state='UNKNOWN')
      into v_effect_count,v_unknown_effect_count
      from private.candidate_daily_external_effect_receipts e
      where e.environment=v_environment and e.candidate_id=v_candidate_id and e.state in ('IN_PROGRESS','UNKNOWN');
      perform 1 from public.candidate_daily_sheet_projection_outbox o
      where o.environment=v_environment and o.candidate_id=v_candidate_id
        and o.target='MASTER_AVAILABILITY_SHEET'
      order by o.availability_version,o.outbox_id for update;
      select count(*) filter(where o.state='PENDING'),count(*) filter(where o.state='CLAIMED'),
        count(*) filter(where o.state='RETRY'),count(*) filter(where o.state='DEFERRED_OVERLAY'),
        count(*) filter(where o.state='TERMINAL')
      into v_pending_count,v_claimed_count,v_retry_count,v_deferred_count,v_terminal_count
      from public.candidate_daily_sheet_projection_outbox o
      where o.environment=v_environment and o.candidate_id=v_candidate_id
        and o.target='MASTER_AVAILABILITY_SHEET';
      v_actual_disposition:=case
        when v_pending_count+v_claimed_count+v_retry_count+v_terminal_count+v_command_count
          +v_other_batch_count+v_effect_count>0 then 'NONE'
        when v_deferred_count>0 then 'RECONCILED'
        else 'DRAINED' end;
      if v_requested_disposition<>v_actual_disposition then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_prior_mode<>v_new_mode and v_actual_disposition='NONE' then
        raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
      end if;

      v_strict_barrier:=v_new_mode='SUPABASE_PRIMARY'
        or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY');
      if v_strict_barrier then
        if not(v_item ?& array['expected_generation_id','expected_generation_version',
          'expected_accepted_canonical_cursor','expected_required_visible_cursor','expected_effective_visible_cursor'])
           or v_item->>'expected_generation_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
           or v_item->>'expected_generation_version' !~ '^[1-9]\d*$'
           or v_item->>'expected_accepted_canonical_cursor' !~ '^\d+$'
           or v_item->>'expected_required_visible_cursor' !~ '^\d+$'
           or v_item->>'expected_effective_visible_cursor' !~ '^\d+$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_expected_generation_id:=(v_item->>'expected_generation_id')::uuid;
        v_expected_generation_version:=(v_item->>'expected_generation_version')::bigint;
        v_expected_accepted_cursor:=(v_item->>'expected_accepted_canonical_cursor')::bigint;
        v_expected_required_cursor:=(v_item->>'expected_required_visible_cursor')::bigint;
        v_expected_effective_cursor:=(v_item->>'expected_effective_visible_cursor')::bigint;
        if v_scope.active_generation_id is null or v_scope.active_generation_id<>v_expected_generation_id then
          raise exception using errcode='40001',message='SEMANTIC_REJECTION';
        end if;
        select * into v_generation from public.candidate_daily_rota_generations g
        where g.generation_id=v_scope.active_generation_id and g.environment=v_environment
          and g.candidate_id=v_candidate_id for update;
        select count(*) into v_generation_day_count from public.candidate_daily_rota_days d
        where d.generation_id=v_scope.active_generation_id and d.environment=v_environment
          and d.candidate_id=v_candidate_id;
        v_now:=clock_timestamp();
        if v_generation.generation_id is null or v_generation.state<>'ACTIVE'
           or v_generation.generation_version<>v_expected_generation_version
           or v_generation.expected_day_count<>14 or v_generation.actual_day_count<>14
           or v_generation_day_count<>14 or v_generation.activated_at_utc is null
           or v_generation.published_at_utc is null then
          raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
        end if;
        if v_generation.published_at_utc<v_now-interval '120 seconds' then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET' for update;
        if v_sync.candidate_id is null then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        perform private._candidate_daily_refresh_sync_state_v1(
          v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_now);
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET' for update;
        select count(*) into v_invalid_overlay_count
        from public.candidate_daily_sheet_projection_outbox o
        where o.environment=v_environment and o.candidate_id=v_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET' and o.state='DEFERRED_OVERLAY'
          and not(o.overlay_generation_id=v_generation.generation_id
            and o.overlay_generation_version=v_generation.generation_version
            and exists(select 1 from public.candidate_daily_rota_days d
              where d.generation_id=v_generation.generation_id and d.environment=v_environment
                and d.candidate_id=v_candidate_id and d.rota_date=o.availability_date
                and (d.booked or d.system_blocked) and d.source_row_hash=o.overlay_source_row_hash));
        if v_invalid_overlay_count>0 then
          raise exception using errcode='55000',message='PROJECTION_STALE_COMPLETION';
        end if;
        if v_pending_count+v_claimed_count+v_retry_count+v_terminal_count+v_command_count
             +v_other_batch_count+v_effect_count>0 or v_unknown_effect_count>0 then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        if v_sync.accepted_canonical_cursor<>v_expected_accepted_cursor
           or v_sync.required_visible_cursor<>v_expected_required_cursor
           or v_sync.effective_visible_cursor<>v_expected_effective_cursor
           or v_expected_accepted_cursor<>v_scope.canonical_version
           or v_expected_required_cursor<>v_scope.canonical_version
           or v_expected_effective_cursor<>v_scope.canonical_version
           or v_sync.state<>'READY' or v_sync.pending_count<>0 or v_sync.retry_count<>0
           or v_sync.terminal_count<>0 or nullif(v_sync.observed_source_revision,'') is null
           or v_sync.last_reconciled_at_utc is null then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        select greatest(v_generation.published_at_utc,
          coalesce(max(a.changed_at_utc),v_generation.published_at_utc),
          coalesce((select max(o.updated_at_utc) from public.candidate_daily_sheet_projection_outbox o
            where o.environment=v_environment and o.candidate_id=v_candidate_id
              and o.target='MASTER_AVAILABILITY_SHEET'),v_generation.published_at_utc))
        into v_latest_fact_at
        from public.candidate_daily_availability_days a
        where a.environment=v_environment and a.candidate_id=v_candidate_id;
        if v_sync.last_reconciled_at_utc<v_latest_fact_at then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
      elsif v_scope.active_generation_id is not null then
        select * into v_generation from public.candidate_daily_rota_generations g
        where g.generation_id=v_scope.active_generation_id and g.environment=v_environment
          and g.candidate_id=v_candidate_id;
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET';
      end if;
      v_transition_id:=gen_random_uuid();
      insert into private.candidate_daily_authority_transitions(transition_id,batch_receipt_id,environment,
        candidate_id,prior_authority_mode,new_authority_mode,effective_at_utc,canonical_version_snapshot,
        generation_id_snapshot,generation_version_snapshot,sync_snapshot_json,in_flight_disposition,
        entitlement_before,entitlement_after,actor_user_id,independent_approver_user_id,reason,
        evidence_sha256,outcome,correlation_id)
      select v_transition_id,v_batch.batch_receipt_id,v_environment,v_candidate_id,v_prior_mode,v_new_mode,now(),
        v_scope.canonical_version,v_scope.active_generation_id,v_generation.generation_version,
        coalesce(to_jsonb(v_sync),'{}'::jsonb),
        v_actual_disposition,v_entitlement_before,v_entitlement_after,v_actor,
        p_independent_approver,p_reason,p_evidence_sha256,'COMMITTED',p_correlation_id
      from (values(1))x(n);
      insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,valid_from_utc,valid_to_utc,
        actor_user_id,reason,evidence_sha256)
      values(v_environment,v_candidate_id,v_entitlement_after,case when v_entitlement_after then now() else null end,
        case when v_entitlement_after then null else now() end,v_actor,p_reason,p_evidence_sha256)
      on conflict(environment,candidate_id) do update set enabled=excluded.enabled,
        valid_from_utc=excluded.valid_from_utc,valid_to_utc=excluded.valid_to_utc,actor_user_id=excluded.actor_user_id,
        reason=excluded.reason,evidence_sha256=excluded.evidence_sha256,updated_at_utc=now();
      update private.candidate_daily_authority_scopes set authority_mode=v_new_mode,
        last_transition_id=v_transition_id,transition_in_progress=false,updated_at_utc=now()
        where environment=v_environment and candidate_id=v_candidate_id;
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','COMMITTED',
        'candidate_id',v_candidate_id,'transition_id',v_transition_id,'authority_mode',v_new_mode,
        'entitlement_enabled',v_entitlement_after));
    exception when others then
      v_error:=sqlerrm;
      if v_error in ('VALIDATION_FAILED','SEMANTIC_REJECTION','SOURCE_EVENT_CONFLICT',
        'CANDIDATE_DAILY_NOT_READY','GENERATION_INCOMPLETE','IDENTITY_LINK_MISSING',
        'IDENTITY_LINK_AMBIGUOUS','BATCH_IN_PROGRESS','COMMAND_IN_PROGRESS',
        'EFFECT_STATUS_UNKNOWN','PROJECTION_STALE_COMPLETION') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REJECTED','error_code',v_error));
      else raise; end if;
    end;
    v_index:=v_index+1;
  end loop;
  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

create or replace function public.candidate_daily_external_effect_claim_v1(
  p_internal_context jsonb,
  p_effect_key text,
  p_operation text,
  p_candidate_source_hmac text,
  p_request_hash text,
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
declare v_context jsonb; v_environment text; v_candidate_id uuid;
  v_receipt private.candidate_daily_external_effect_receipts%rowtype;
  v_receipt_id uuid:=gen_random_uuid(); v_lease_token text; v_lease_expiry timestamptz;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,p_candidate_source_hmac);
  if length(p_effect_key) not between 16 and 256
     or p_operation not in ('RUNNING_LATE_SEND','CANNOT_ATTEND','LEAVE_EARLY','DNA','MESSAGE_SEEN','ESCALATION_STEP','ACKNOWLEDGEMENT')
     or p_request_hash !~ '^[a-f0-9]{64}$' or length(btrim(p_executor_id)) not between 8 and 128
     or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$' or p_lease_seconds not between 30 and 600
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_lease_token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
  v_lease_expiry:=p_now_utc+make_interval(secs=>p_lease_seconds);
  insert into private.candidate_daily_external_effect_receipts(effect_receipt_id,environment,candidate_id,
    effect_key,operation,request_hash,idempotency_key,state,first_claimed_at_utc,lease_owner,lease_token,
    lease_expires_at_utc,stable_provider_request_id,attempt_count,correlation_id,retain_until_utc)
  values(v_receipt_id,v_environment,v_candidate_id,p_effect_key,p_operation,p_request_hash,p_idempotency_key,
    'IN_PROGRESS',p_now_utc,p_executor_id,v_lease_token,v_lease_expiry,p_effect_key,1,p_correlation_id,
    p_now_utc+interval '7 years')
  on conflict do nothing;
  select * into v_receipt from private.candidate_daily_external_effect_receipts where
    (environment=v_environment and effect_key=p_effect_key)
    or (environment=v_environment and candidate_id=v_candidate_id and operation=p_operation
      and idempotency_key=p_idempotency_key)
    order by (effect_key=p_effect_key) desc limit 1 for update;
  if v_receipt.candidate_id<>v_candidate_id or v_receipt.operation<>p_operation
     or v_receipt.request_hash<>p_request_hash or v_receipt.idempotency_key<>p_idempotency_key then
    raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
  end if;
  if v_receipt.state='IN_PROGRESS' and v_receipt.lease_expires_at_utc<=p_now_utc then
    raise exception using errcode='55000',message='EFFECT_STATUS_UNKNOWN';
  end if;
  if v_receipt.state='IN_PROGRESS' then
    return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'state','CLAIMED',
      'lease_token',v_receipt.lease_token,'lease_expires_at',v_receipt.lease_expires_at_utc,'safe_result',null);
  end if;
  return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'state',v_receipt.state,
    'lease_token',null,'lease_expires_at',null,'safe_result',v_receipt.terminal_result_json,
    '_idempotent_replay',true);
end;
$function$;

create or replace function public.candidate_daily_external_effect_complete_v1(
  p_internal_context jsonb,
  p_effect_receipt_id uuid,
  p_lease_token text,
  p_outcome text,
  p_provider_reference_hash text default null,
  p_safe_result jsonb default null,
  p_now_utc timestamptz default now(),
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_receipt private.candidate_daily_external_effect_receipts%rowtype;
  v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if p_effect_receipt_id is null or length(p_lease_token) not between 16 and 256
     or p_outcome not in ('COMPLETED','FAILED_FINAL','UNKNOWN')
     or (p_provider_reference_hash is not null and p_provider_reference_hash !~ '^[a-f0-9]{64}$')
     or (p_safe_result is not null and (jsonb_typeof(p_safe_result)<>'object' or p_safe_result<>'{}'::jsonb))
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select * into v_receipt from private.candidate_daily_external_effect_receipts
    where effect_receipt_id=p_effect_receipt_id and environment=v_environment for update;
  if v_receipt.effect_receipt_id is null then raise exception using errcode='02000',message='EFFECT_NOT_FOUND'; end if;
  if v_receipt.state<>'IN_PROGRESS' then
    if v_receipt.lease_token<>p_lease_token or v_receipt.state<>p_outcome then
      raise exception using errcode='55000',message='LEASE_CONFLICT';
    end if;
    return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'state',v_receipt.state,
      '_idempotent_replay',true);
  end if;
  if v_receipt.lease_token<>p_lease_token then raise exception using errcode='55000',message='LEASE_CONFLICT'; end if;
  if v_receipt.lease_expires_at_utc<=p_now_utc then
    raise exception using errcode='55000',message='LEASE_EXPIRED_STATUS_REQUIRED';
  end if;
  v_terminal:=coalesce(p_safe_result,'{}'::jsonb);
  update private.candidate_daily_external_effect_receipts set state=p_outcome,
    provider_reference_hash=p_provider_reference_hash,terminal_result_json=v_terminal,
    terminal_body_sha256=private._candidate_daily_json_sha256_v1(v_terminal),completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc where effect_receipt_id=v_receipt.effect_receipt_id;
  return jsonb_build_object('effect_receipt_id',v_receipt.effect_receipt_id,'state',p_outcome);
end;
$function$;

create or replace function public.candidate_daily_external_effect_status_get_v1(
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
declare v_context jsonb; v_environment text; v_receipt private.candidate_daily_external_effect_receipts%rowtype;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  select * into v_receipt from private.candidate_daily_external_effect_receipts
    where environment=v_environment and effect_key=p_effect_key;
  if v_receipt.effect_receipt_id is null then raise exception using errcode='02000',message='EFFECT_NOT_FOUND'; end if;
  return jsonb_build_object('effect_key',v_receipt.effect_key,'operation',v_receipt.operation,
    'status',v_receipt.state,'created_at',v_receipt.created_at_utc,'updated_at',v_receipt.updated_at_utc);
end;
$function$;

create or replace function private._candidate_daily_context_v1(
  p_context jsonb,
  p_policy text,
  p_require_candidate boolean default false
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_environment text;
  v_candidate_id uuid;
  v_enabled boolean:=false;
  v_entitled boolean:=false;
  v_source_ready boolean:=false;
  v_scope_ready boolean:=false;
begin
  if jsonb_typeof(p_context)<>'object'
     or p_context ?| array['requested_environment','browser_candidate_id','mobile','email','legacy_reference'] then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_environment:=upper(nullif(btrim(p_context->>'environment'),''));
  if v_environment not in ('TEST','LIVE') then
    raise exception using errcode='22023',message='FORBIDDEN';
  end if;
  if nullif(p_context->>'candidate_id','') is not null then
    begin v_candidate_id:=(p_context->>'candidate_id')::uuid;
    exception when others then raise exception using errcode='22023',message='VALIDATION_FAILED'; end;
  end if;
  if p_require_candidate and v_candidate_id is null then
    raise exception using errcode='22023',message='SOURCE_IDENTITY_NOT_READY';
  end if;
  if p_policy='CANDIDATE_SURFACE' then
    select sd.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb
      into v_enabled from public.settings_defaults sd where sd.id=1;
    if coalesce(v_enabled,false) is not true then
      raise exception using errcode='42501',message='CANDIDATE_DAILY_DISABLED';
    end if;
    select e.enabled
      and (e.valid_from_utc is null or e.valid_from_utc<=now())
      and (e.valid_to_utc is null or e.valid_to_utc>now())
      into v_entitled
    from private.candidate_daily_entitlements e
    where e.environment=v_environment and e.candidate_id=v_candidate_id;
    select exists(
      select 1 from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
        and l.state in ('PRIMARY','OVERLAP')
        and l.valid_from_utc<=now() and (l.valid_to_utc is null or l.valid_to_utc>now())
    ) into v_source_ready;
    select exists(
      select 1 from private.candidate_daily_authority_scopes s
      where s.environment=v_environment and s.candidate_id=v_candidate_id
        and s.authority_mode='SUPABASE_PRIMARY' and not s.transition_in_progress
    ) into v_scope_ready;
    if not coalesce(v_entitled,false) then
      raise exception using errcode='42501',message='CANDIDATE_DAILY_DISABLED';
    end if;
    if not v_source_ready then
      raise exception using errcode='55000',message='SOURCE_IDENTITY_NOT_READY';
    end if;
    if not v_scope_ready then
      raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
    end if;
  elsif p_policy in ('LEGACY_COMPAT','SIGNED_SYSTEM_SYNC') then
    if coalesce((p_context->>'system_auth_verified')::boolean,false) is not true
       or coalesce((p_context->>'nonce_consumed')::boolean,false) is not true
       or coalesce((p_context->>'environment_trusted')::boolean,false) is not true
       or coalesce((p_context->>'transition_ready')::boolean,false) is not true
       or coalesce((p_context->>'authority_mode_compatible')::boolean,false) is not true then
      raise exception using errcode='42501',message='FORBIDDEN';
    end if;
    if p_policy='LEGACY_COMPAT' and (
      coalesce((p_context->>'stable_operation_identity')::boolean,false) is not true
      or coalesce((p_context->>'approved_source_mapping')::boolean,false) is not true
    ) then raise exception using errcode='42501',message='FORBIDDEN'; end if;
    if p_policy='SIGNED_SYSTEM_SYNC'
       and coalesce((p_context->>'source_scope_ready')::boolean,false) is not true then
      raise exception using errcode='42501',message='FORBIDDEN';
    end if;
  else
    raise exception using errcode='22023',message='FORBIDDEN';
  end if;
  return jsonb_build_object('environment',v_environment,'candidate_id',v_candidate_id,'policy',p_policy);
end;
$function$;

create or replace function private._candidate_daily_source_candidate_v1(
  p_environment text,
  p_candidate_source_hmac text
)
returns uuid
language plpgsql
stable
security definer
set search_path=''
as $function$
declare v_candidate_id uuid; v_count integer;
begin
  if p_candidate_source_hmac !~ '^[a-f0-9]{64}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select count(distinct l.candidate_id),min(l.candidate_id::text)::uuid
    into v_count,v_candidate_id
  from private.candidate_daily_source_links l
  where l.environment=upper(p_environment)
    and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
    and l.identifier_hmac=p_candidate_source_hmac
    and l.state in ('PRIMARY','OVERLAP')
    and l.valid_from_utc<=now() and (l.valid_to_utc is null or l.valid_to_utc>now());
  if v_count=0 then raise exception using errcode='55000',message='IDENTITY_LINK_MISSING'; end if;
  if v_count<>1 then raise exception using errcode='55000',message='IDENTITY_LINK_AMBIGUOUS'; end if;
  return v_candidate_id;
end;
$function$;

create or replace function private._candidate_daily_legacy_value_v1(p_preference text)
returns text language sql immutable set search_path=''
as $function$
  select case p_preference
    when 'PENDING' then '' when 'NOT_AVAILABLE' then 'N/A' when 'LONG_DAY' then 'LD'
    when 'NIGHT' then 'N' when 'LONG_DAY_OR_NIGHT' then 'LD/N' else null end;
$function$;

create or replace function private._candidate_daily_preference_v1(p_legacy_value text)
returns text language sql immutable set search_path=''
as $function$
  select case p_legacy_value
    when '' then 'PENDING' when 'N/A' then 'NOT_AVAILABLE' when 'LD' then 'LONG_DAY'
    when 'N' then 'NIGHT' when 'LD/N' then 'LONG_DAY_OR_NIGHT'
    when 'LD+N' then 'LONG_DAY_OR_NIGHT' else null end;
$function$;

create or replace function private._candidate_daily_freshness_v1(
  p_environment text,p_candidate_id uuid,p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_oldest integer:=0; v_reasons jsonb:='[]'::jsonb; v_age integer:=0;
begin
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=p_environment and candidate_id=p_candidate_id;
  if v_scope.active_generation_id is not null then
    select * into v_generation from public.candidate_daily_rota_generations
      where generation_id=v_scope.active_generation_id;
  end if;
  select * into v_sync from private.candidate_daily_sync_state
    where environment=p_environment and candidate_id=p_candidate_id and target='MASTER_AVAILABILITY_SHEET';
  select coalesce(greatest(0,extract(epoch from p_now_utc-min(o.created_at_utc))::integer),0)
    into v_oldest from public.candidate_daily_sheet_projection_outbox o
    where o.environment=p_environment and o.candidate_id=p_candidate_id
      and o.state in ('PENDING','RETRY','CLAIMED');
  if v_generation.generation_id is null then v_reasons:=v_reasons||'"GENERATION_MISSING"'::jsonb;
  else
    v_age:=greatest(0,extract(epoch from p_now_utc-v_generation.published_at_utc)::integer);
    if v_generation.actual_day_count<>14 then v_reasons:=v_reasons||'"GENERATION_INCOMPLETE"'::jsonb; end if;
    if v_age>120 then v_reasons:=v_reasons||'"GENERATION_STALE"'::jsonb; end if;
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
    'generation_max_age_seconds',120,'projection_warning_seconds',120,
    'ready',jsonb_array_length(v_reasons)=0,'reasons',v_reasons
  );
end;
$function$;

create or replace function private._candidate_daily_capability_v1(
  p_environment text,
  p_candidate_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_environment text:=upper(nullif(btrim(p_environment),''));
  v_enabled boolean:=false;
  v_entitled boolean:=false;
  v_source_ready boolean:=false;
  v_authority_ready boolean:=false;
  v_freshness jsonb;
begin
  if v_environment not in ('TEST','LIVE') then
    return jsonb_build_object('enabled',false,'unavailable_reason','AUTHORITY_UNREADABLE');
  end if;
  begin
    select sd.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb
      into v_enabled from public.settings_defaults sd where sd.id=1;
    if coalesce(v_enabled,false) is not true then
      return jsonb_build_object('enabled',false,'unavailable_reason','GLOBAL_DISABLED');
    end if;
    if p_candidate_id is null then
      return jsonb_build_object('enabled',false,'unavailable_reason','NOT_ENTITLED');
    end if;
    select e.enabled
      and (e.valid_from_utc is null or e.valid_from_utc<=p_now_utc)
      and (e.valid_to_utc is null or e.valid_to_utc>p_now_utc)
      into v_entitled
    from private.candidate_daily_entitlements e
    where e.environment=v_environment and e.candidate_id=p_candidate_id;
    if coalesce(v_entitled,false) is not true then
      return jsonb_build_object('enabled',false,'unavailable_reason','NOT_ENTITLED');
    end if;
    select exists(
      select 1 from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=p_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
        and l.state in ('PRIMARY','OVERLAP')
        and l.valid_from_utc<=p_now_utc and (l.valid_to_utc is null or l.valid_to_utc>p_now_utc)
    ) into v_source_ready;
    if not v_source_ready then
      return jsonb_build_object('enabled',false,'unavailable_reason','SOURCE_IDENTITY_NOT_READY');
    end if;
    select exists(
      select 1 from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g on g.generation_id=s.active_generation_id
      where s.environment=v_environment and s.candidate_id=p_candidate_id
        and s.authority_mode='SUPABASE_PRIMARY' and not s.transition_in_progress
        and g.state='ACTIVE' and g.actual_day_count=14
    ) into v_authority_ready;
    if not v_authority_ready then
      return jsonb_build_object('enabled',false,'unavailable_reason','AUTHORITY_NOT_READY');
    end if;
    v_freshness:=private._candidate_daily_freshness_v1(v_environment,p_candidate_id,p_now_utc);
    if coalesce((v_freshness->>'ready')::boolean,false) is not true then
      return jsonb_build_object('enabled',false,'unavailable_reason','AUTHORITY_NOT_READY');
    end if;
    return jsonb_build_object('enabled',true);
  exception when others then
    return jsonb_build_object('enabled',false,'unavailable_reason','AUTHORITY_UNREADABLE');
  end;
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
declare v_context jsonb; v_environment text; v_candidate_id uuid; v_policy text;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_tiles jsonb; v_from date; v_freshness jsonb;
begin
  v_policy:=upper(coalesce(nullif(p_internal_context->>'policy',''),'CANDIDATE_SURFACE'));
  if v_policy='LEGACY_COMPAT' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'LEGACY_COMPAT',false);
    v_environment:=v_context->>'environment';
    v_candidate_id:=private._candidate_daily_source_candidate_v1(
      v_environment,p_internal_context->>'candidate_source_hmac');
  elsif v_policy='CANDIDATE_SURFACE' then
    v_context:=private._candidate_daily_context_v1(p_internal_context,'CANDIDATE_SURFACE',true);
    v_environment:=v_context->>'environment'; v_candidate_id:=(v_context->>'candidate_id')::uuid;
  else
    raise exception using errcode='22023',message='FORBIDDEN';
  end if;
  if p_days<>14 then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;
  select * into v_scope from private.candidate_daily_authority_scopes
    where environment=v_environment and candidate_id=v_candidate_id;
  select * into v_generation from public.candidate_daily_rota_generations
    where generation_id=v_scope.active_generation_id and state='ACTIVE';
  if v_generation.generation_id is null then
    raise exception using errcode='55000',message='DAILY_GENERATION_UNAVAILABLE';
  end if;
  v_from:=coalesce(p_from,v_generation.window_start);
  if v_from<>v_generation.window_start then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
    'date',d.rota_date,'display_day',to_char(d.rota_date,'Dy'),'display_date',to_char(d.rota_date,'DD/MM/YYYY'),
    'booked',d.booked,'system_blocked',d.system_blocked,'editable',not(d.booked or d.system_blocked),
    'status',case when d.booked then 'BOOKED' when d.system_blocked then 'BLOCKED'
      else coalesce(a.preference,'PENDING') end,
    'availability',coalesce(a.preference,'PENDING'),'shift_info',d.shift_info,'hospital',d.hospital,
    'ward',d.ward,'job_title',d.job_title,'booking_ref',d.booking_ref,'shift_type',d.shift_type,
    'booking_id',d.booking_id,'timesheet_authorised',d.timesheet_authorised,
    'timesheet_eligible',d.timesheet_eligible,
    'action_target',case d.action_target_kind
      when 'TIMESHEET_DETAIL' then jsonb_strip_nulls(jsonb_build_object('target_kind',d.action_target_kind,
        'timesheet_id',d.action_timesheet_id,'workflow_id',d.action_workflow_id,'row_signature',d.action_row_signature))
      when 'CONTRACT_WEEK_DETAIL' then jsonb_strip_nulls(jsonb_build_object('target_kind',d.action_target_kind,
        'contract_week_id',d.action_contract_week_id,'timesheet_id',d.action_timesheet_id,
        'workflow_id',d.action_workflow_id,'row_signature',d.action_row_signature))
      when 'WORKFLOW_DETAIL' then jsonb_build_object('target_kind',d.action_target_kind,
        'workflow_id',d.action_workflow_id,'workflow_generation',d.action_workflow_generation,
        'row_signature',d.action_row_signature) else null end
  )) order by d.rota_date) into v_tiles
  from public.candidate_daily_rota_days d
  left join public.candidate_daily_availability_days a on a.environment=d.environment
    and a.candidate_id=d.candidate_id and a.availability_date=d.rota_date
  where d.generation_id=v_generation.generation_id;
  if jsonb_array_length(coalesce(v_tiles,'[]'::jsonb))<>14 then
    raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
  end if;
  v_freshness:=private._candidate_daily_freshness_v1(v_environment,v_candidate_id,now());
  if v_policy='CANDIDATE_SURFACE' and coalesce((v_freshness->>'ready')::boolean,false) is not true then
    raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
  end if;
  return jsonb_build_object('candidate_id',v_candidate_id,'window_start',v_generation.window_start,
    'window_end',v_generation.window_end,'generation_id',v_generation.generation_id,
    'generation_version',v_generation.generation_version,'availability_version',v_scope.canonical_version,
    'freshness',v_freshness,
    'cohorts','[]'::jsonb,'tiles',v_tiles);
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
    if v_row.generation_id is null or v_row.booked or v_row.system_blocked then
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

revoke all on function private._candidate_daily_json_sha256_v1(jsonb) from public;
revoke all on function private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz) from public;
revoke all on function private._candidate_daily_context_v1(jsonb,text,boolean) from public;
revoke all on function private._candidate_daily_source_candidate_v1(text,text) from public;
revoke all on function private._candidate_daily_legacy_value_v1(text) from public;
revoke all on function private._candidate_daily_preference_v1(text) from public;
revoke all on function private._candidate_daily_freshness_v1(text,uuid,timestamptz) from public;
revoke all on function private._candidate_daily_capability_v1(text,uuid,timestamptz) from public;

revoke all on function public.candidate_daily_tiles_get_v1(jsonb,date,integer) from public;
revoke all on function public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text) from public;
revoke all on function public.candidate_daily_legacy_availability_apply_atomic_v1(jsonb,text,uuid,text,jsonb,text) from public;
revoke all on function public.candidate_daily_legacy_availability_status_get_v1(jsonb,text,uuid,text) from public;
revoke all on function public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text) from public;
revoke all on function public.candidate_daily_projection_claim_v1(jsonb,uuid,text,text,text,integer,integer,text) from public;
revoke all on function public.candidate_daily_projection_complete_atomic_v1(jsonb,uuid,text,jsonb,text) from public;
revoke all on function public.candidate_daily_sync_status_get_v1(jsonb,jsonb,text) from public;
revoke all on function public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text) from public;
revoke all on function public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text) from public;
revoke all on function public.candidate_daily_external_effect_claim_v1(jsonb,text,text,text,text,text,text,integer,timestamptz,text) from public;
revoke all on function public.candidate_daily_external_effect_complete_v1(jsonb,uuid,text,text,text,jsonb,timestamptz,text) from public;
revoke all on function public.candidate_daily_external_effect_status_get_v1(jsonb,text,timestamptz,text) from public;

do $grants$
declare
  v_function regprocedure;
begin
  foreach v_function in array array[
    'public.candidate_daily_tiles_get_v1(jsonb,date,integer)'::regprocedure,
    'public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text)'::regprocedure,
    'public.candidate_daily_legacy_availability_apply_atomic_v1(jsonb,text,uuid,text,jsonb,text)'::regprocedure,
    'public.candidate_daily_legacy_availability_status_get_v1(jsonb,text,uuid,text)'::regprocedure,
    'public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text)'::regprocedure,
    'public.candidate_daily_projection_claim_v1(jsonb,uuid,text,text,text,integer,integer,text)'::regprocedure,
    'public.candidate_daily_projection_complete_atomic_v1(jsonb,uuid,text,jsonb,text)'::regprocedure,
    'public.candidate_daily_sync_status_get_v1(jsonb,jsonb,text)'::regprocedure,
    'public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text)'::regprocedure,
    'public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)'::regprocedure,
    'public.candidate_daily_external_effect_claim_v1(jsonb,text,text,text,text,text,text,integer,timestamptz,text)'::regprocedure,
    'public.candidate_daily_external_effect_complete_v1(jsonb,uuid,text,text,text,jsonb,timestamptz,text)'::regprocedure,
    'public.candidate_daily_external_effect_status_get_v1(jsonb,text,timestamptz,text)'::regprocedure
  ] loop
    if exists(select 1 from pg_roles where rolname='anon') then
      execute format('revoke all on function %s from anon',v_function);
    end if;
    if exists(select 1 from pg_roles where rolname='authenticated') then
      execute format('revoke all on function %s from authenticated',v_function);
    end if;
    if exists(select 1 from pg_roles where rolname='service_role') then
      execute format('grant execute on function %s to service_role',v_function);
    end if;
  end loop;
  if exists(select 1 from pg_roles where rolname='service_role') then
    revoke all on function private._candidate_daily_json_sha256_v1(jsonb) from service_role;
    revoke all on function private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz) from service_role;
    revoke all on function private._candidate_daily_context_v1(jsonb,text,boolean) from service_role;
    revoke all on function private._candidate_daily_source_candidate_v1(text,text) from service_role;
    revoke all on function private._candidate_daily_legacy_value_v1(text) from service_role;
    revoke all on function private._candidate_daily_preference_v1(text) from service_role;
    revoke all on function private._candidate_daily_freshness_v1(text,uuid,timestamptz) from service_role;
    revoke all on function private._candidate_daily_capability_v1(text,uuid,timestamptz) from service_role;
  end if;
end;
$grants$;
