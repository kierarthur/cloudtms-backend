-- Explicit same-identity re-enrolment; no historical link/generation deletion.
\set ON_ERROR_STOP on
begin;

create or replace function public.candidate_google_provisioning_attach_v1(
  p_internal_context jsonb,
  p_local_candidate_id uuid,
  p_candidate_code text,
  p_surname text,
  p_email text,
  p_mobile text,
  p_google_source_identity_hmac text,
  p_source_hmac_key_version integer,
  p_correlation_id text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_code text:=pg_catalog.upper(pg_catalog.btrim(p_candidate_code));
  v_surname text:=pg_catalog.lower(pg_catalog.btrim(p_surname));
  v_email text:=private._candidate_normalize_email(p_email);
  v_mobile text:=private._candidate_google_mobile_normalize_v1(p_mobile);
  v_candidate public.candidates%rowtype;
  v_existing text;
  v_environment text:=p_internal_context->>'environment';
  v_receipt private.candidate_google_rota_removal_receipts%rowtype;
  v_link private.candidate_daily_source_links%rowtype;
  v_operation uuid;
  v_created timestamptz;
  v_lock text;
begin
  if p_internal_context is null
     or pg_catalog.jsonb_typeof(p_internal_context) is distinct from 'object'
     or p_internal_context->>'route_context_verified' is distinct from 'true'
     or p_internal_context->>'audience' is distinct from 'GOOGLE_PROVISIONING_ATTACH'
     or coalesce(p_internal_context->>'environment','') not in ('TEST','LIVE')
     or p_local_candidate_id is null
     or coalesce(v_code,'') !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
     or coalesce(pg_catalog.char_length(v_surname),0) not between 1 and 200
     or coalesce(pg_catalog.char_length(v_email),0) not between 3 and 320
     or coalesce(pg_catalog.char_length(v_mobile),0) not between 7 and 32
     or coalesce(p_google_source_identity_hmac,'') !~ '^[a-f0-9]{64}$'
     or coalesce(p_source_hmac_key_version,0) not between 1 and 2147483647
     or coalesce(pg_catalog.char_length(pg_catalog.btrim(p_correlation_id)),0) not between 1 and 200
     or p_now_utc is null or not isfinite(p_now_utc)
     or abs(extract(epoch from p_now_utc-clock_timestamp()))>120 then
    raise exception using errcode='22023',message='GOOGLE_PROVISIONING_ATTACH_INVALID';
  end if;

  for v_lock in select x from unnest(array['GLOBAL:'||v_code,
      'SOURCE:'||p_source_hmac_key_version::text||':'||p_google_source_identity_hmac])x order by x loop
    perform pg_advisory_xact_lock(hashtextextended(v_environment||':'||v_lock,0));
  end loop;
  select * into strict v_candidate
  from public.candidates c where c.id=p_local_candidate_id for update;
  if not v_candidate.active
     or pg_catalog.lower(pg_catalog.btrim(coalesce(v_candidate.last_name,''))) is distinct from v_surname
     or private._candidate_normalize_email(v_candidate.email) is distinct from v_email
     or private._candidate_google_mobile_normalize_v1(v_candidate.phone) is distinct from v_mobile then
    raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
  end if;
  select * into v_receipt from private.candidate_google_rota_removal_receipts
    where environment=v_environment and source_hmac_key_version=p_source_hmac_key_version
      and candidate_source_hmac=p_google_source_identity_hmac
    order by removed_at_utc desc,operation_id desc limit 1 for update;
  if found then
    begin
      v_operation:=(p_internal_context->>'operation_id')::uuid;
      v_created:=(p_internal_context->>'operation_created_at_utc')::timestamptz;
    exception when others then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_REENROLMENT_REQUIRED';
    end;
    if p_internal_context->>'project_role' is distinct from 'MASTER'
      or v_operation is null or v_created is null or not isfinite(v_created)
      or v_created<=v_receipt.removed_at_utc or v_created>p_now_utc
      or (v_receipt.candidate_id is not null and v_receipt.candidate_id<>p_local_candidate_id)
      or v_receipt.candidate_code_sha256<>encode(extensions.digest(convert_to(v_code,'UTF8'),'sha256'),'hex') then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_REENROLMENT_REQUIRED';
    end if;
    select * into v_link from private.candidate_daily_source_links
      where environment=v_environment and source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
        and hmac_key_version=p_source_hmac_key_version and identifier_hmac=p_google_source_identity_hmac for update;
    if v_link.link_id is not null and (v_link.candidate_id<>p_local_candidate_id
      or v_link.state='REJECTED') then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
    end if;
    if v_receipt.reenrolled_at_utc is null then
      if v_link.link_id is not null and v_link.state<>'RETIRED' then
        raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
      end if;
      -- Keep the exact historical row and association. Only this fresh explicit
      -- attach can reactivate it; ordinary publication remains fail-closed.
      -- Preserve its original identity-verification time. The immutable removal
      -- receipt records the new enrolment boundary used to reject old snapshots.
      update private.candidate_daily_source_links set state='PRIMARY',
        valid_to_utc=null,updated_at_utc=p_now_utc
        where link_id=v_link.link_id and state='RETIRED';
      update private.candidate_google_rota_removal_receipts
        set reenrolled_at_utc=p_now_utc,reenrolment_operation_id=v_operation
        where environment=v_receipt.environment and integration_id=v_receipt.integration_id
          and operation_id=v_receipt.operation_id;
    elsif v_link.link_id is not null and v_link.state not in ('PRIMARY','OVERLAP') then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
    end if;
  end if;
  v_existing:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_candidate.key_norm,'')));
  if nullif(v_existing,'') is null then
    update public.candidates c
       set key_norm=v_code,updated_at=p_now_utc
     where c.id=p_local_candidate_id
       and nullif(pg_catalog.btrim(coalesce(c.key_norm,'')),'') is null;
    if not found then
      raise exception using errcode='40001',message='GOOGLE_PROVISIONING_CID_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'state','ATTACHED','local_candidate_id',p_local_candidate_id,
      'candidate_code',v_code,'idempotent_replay',false
    );
  elsif v_existing=v_code then
    return pg_catalog.jsonb_build_object(
      'ok',true,'state','UNCHANGED','local_candidate_id',p_local_candidate_id,
      'candidate_code',v_code,'idempotent_replay',true
    );
  end if;
  raise exception using errcode='40001',message='GOOGLE_PROVISIONING_CID_CONFLICT';
exception when no_data_found then
  raise exception using errcode='40001',message='GOOGLE_PROVISIONING_IDENTITY_CHANGED';
end;
$function$;

alter function public.candidate_google_provisioning_attach_v1(jsonb,uuid,text,text,text,text,text,integer,text,timestamptz) owner to postgres;
revoke all on function public.candidate_google_provisioning_attach_v1(jsonb,uuid,text,text,text,text,text,integer,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_google_provisioning_attach_v1(jsonb,uuid,text,text,text,text,text,integer,text,timestamptz) to service_role;

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
  v_clear_dates jsonb; v_command_body jsonb; v_lock_key text;
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

  for v_lock_key in
    select distinct q.lock_key
    from (
      select 'GLOBAL:'||upper(btrim(i->>'candidate_global_key')) as lock_key
      from jsonb_array_elements(p_items)i
      where i->>'candidate_global_key' ~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
      union all
      select 'SOURCE:'||(i->>'source_hmac_key_version')||':'||(i->>'candidate_source_hmac')
      from jsonb_array_elements(p_items)i
      where i->>'candidate_source_hmac' ~ '^[a-f0-9]{64}$'
        and i->>'source_hmac_key_version' ~ '^[1-9][0-9]*$'
    )q
    order by q.lock_key
  loop
    perform pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(v_environment||':'||v_lock_key,0)
    );
  end loop;

  for v_item in select value from jsonb_array_elements(p_items) loop
    v_error:=null; v_generation_id:=null; v_generation_version:=null;
    begin
      if (select count(*) from jsonb_object_keys(v_item))<>10
         or not(v_item ?& array['candidate_global_key','candidate_source_hmac','source_hmac_key_version',
           'source_event_id','source_revision','source_hash','window_start','days','source_event_time','item_key'])
         or v_item->>'candidate_global_key' !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
         or v_item->>'candidate_source_hmac' !~ '^[a-f0-9]{64}$'
         or v_item->>'source_hmac_key_version' !~ '^[1-9][0-9]*$'
         or (v_item->>'source_hmac_key_version')::integer<>1
         or v_item->>'source_hash' !~ '^[a-f0-9]{64}$'
         or length(v_item->>'source_event_id') not between 8 and 160
         or length(v_item->>'source_revision') not between 1 and 160
         or jsonb_typeof(v_item->'days')<>'array' or jsonb_array_length(v_item->'days')<>14 then
        raise exception using errcode='22023',message='GENERATION_INCOMPLETE';
      end if;
      -- A frozen pre-removal snapshot must not reinstate an old Rota after a
      -- fresh explicit re-enrolment. Retained history is not publication authority.
      if exists(select 1 from (
        select r.reenrolled_at_utc from private.candidate_google_rota_removal_receipts r
        where r.environment=v_environment and r.source_hmac_key_version=(v_item->>'source_hmac_key_version')::integer
          and r.candidate_source_hmac=v_item->>'candidate_source_hmac'
        order by r.removed_at_utc desc,r.operation_id desc limit 1
      ) latest where latest.reenrolled_at_utc is null
        or (v_item->>'source_event_time')::timestamptz is null
        or not isfinite((v_item->>'source_event_time')::timestamptz)
        or (v_item->>'source_event_time')::timestamptz<latest.reenrolled_at_utc) then
        raise exception using errcode='55000',message='SOURCE_EVENT_CONFLICT';
      end if;
      v_candidate_id:=private._candidate_daily_source_candidate_bind_on_generation_v1(
        v_environment,v_item->>'candidate_global_key',v_item->>'candidate_source_hmac',
        (v_item->>'source_hmac_key_version')::integer
      );
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
        if v_existing.candidate_id<>v_candidate_id
           or v_existing.source_hash<>v_item->>'source_hash' then
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
          v_clear_key:=left('rota-clear:'||p_batch_request_id::text||':'||(v_item->>'item_key'),160);
          insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
            command_class,idempotency_key,request_sha256,source_system,source_event_id,source_revision,
            source_event_time,item_key,canonical_version_before,state,correlation_id)
          values(v_command_id,v_environment,v_candidate_id,'SIGNED_SYSTEM','ROTA_GENERATION_CLEAR',v_clear_key,
            private._candidate_daily_json_sha256_v1(jsonb_build_object('source_hash',v_item->>'source_hash',
              'clear_dates',v_clear_dates)),'MASTER_ROTA_GENERATION',v_item->>'source_event_id',
            v_item->>'source_revision',(v_item->>'source_event_time')::timestamptz,
            left('clear:'||(v_item->>'item_key'),160),v_scope.canonical_version,'IN_PROGRESS',p_correlation_id);
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
        'IDENTITY_LINK_AMBIGUOUS','IDENTITY_LINK_CONFLICT','CANDIDATE_DAILY_NOT_READY') then raise; end if;
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

-- CREATE OR REPLACE preserves the existing owner and service-only ACL.

notify pgrst,'reload schema';
commit;
