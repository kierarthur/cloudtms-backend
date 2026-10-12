\set ON_ERROR_STOP on
begin;

-- Synthetic fixtures only. No staff, Sheet, network or communications effects.
do $test$
declare
  c uuid:=gen_random_uuid(); g uuid:=gen_random_uuid(); b uuid:=gen_random_uuid();
  h text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  ctx jsonb; r jsonb; replay jsonb; req uuid; changes jsonb; mode text;
  old_version bigint; idem text; d date:=date '2026-10-12';
  cid text:='01M4T000000000000000000001';
begin
  if not has_function_privilege('service_role',
    'public.candidate_daily_legacy_availability_apply_atomic_v1(jsonb,text,uuid,text,jsonb,text)','EXECUTE')
    or has_function_privilege('anon',
    'public.candidate_daily_legacy_availability_apply_atomic_v1(jsonb,text,uuid,text,jsonb,text)','EXECUTE')
    or has_function_privilege('authenticated',
    'public.candidate_daily_legacy_availability_apply_atomic_v1(jsonb,text,uuid,text,jsonb,text)','EXECUTE') then
    raise exception 'Legacy availability service-only ACL regressed';
  end if;
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(c,'two-way-'||c::text||'@example.invalid','Two Way Fixture','Two','Way',true,
    'CID1-'||upper(replace(c::text,'-','')));
  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values('TEST',c,'SUPABASE_PRIMARY');
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values('TEST',c,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',gen_random_uuid(),h,1,'PRIMARY',repeat('e',64));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,
    operation_class,idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(b,'TEST','SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH','two-way-generation-'||b::text,
    repeat('a',64),jsonb_build_array(c::text),1,'IN_PROGRESS',cid);
  insert into public.candidate_daily_rota_generations(generation_id,environment,candidate_id,
    generation_version,window_start,window_end,state,actual_day_count,source_system,source_event_id,
    source_revision,source_event_time,item_key,source_hash,generation_row_hash,batch_receipt_id,
    correlation_id,activated_at_utc,published_at_utc)
  values(g,'TEST',c,1,d,d+13,'ACTIVE',14,'MASTER_ROTA','two-way-'||g::text,
    'two-way-fixture',now(),'two-way-'||c::text,repeat('b',64),repeat('c',64),b,cid,now(),now());
  insert into public.candidate_daily_rota_days(generation_id,environment,candidate_id,rota_date,
    booked,system_blocked,source_row_hash)
  select g,'TEST',c,d+n,false,false,repeat('d',64) from generate_series(0,13)n;
  update private.candidate_daily_authority_scopes set active_generation_id=g
  where environment='TEST' and candidate_id=c;
  ctx:=jsonb_build_object('policy','LEGACY_COMPAT','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true,'candidate_source_hmac',h);

  foreach mode in array array['SUPABASE_PRIMARY','GOOGLE_PRIMARY','ROLLBACK_PENDING'] loop
    update private.candidate_daily_authority_scopes set authority_mode=mode
    where environment='TEST' and candidate_id=c;
    select canonical_version into old_version from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id=c;
    req:=gen_random_uuid(); idem:='two-way-command-'||req::text;
    changes:=jsonb_build_array(jsonb_build_object('date',d::text,'availability','N'));
    r:=public.candidate_daily_legacy_availability_apply_atomic_v1(ctx,h,req,idem,changes,cid);
    if r#>>'{outcomes,0,applied}' is distinct from 'true'
      or (r->>'committed_version')::bigint is distinct from old_version+1
      or not exists(select 1 from public.candidate_daily_availability_days
        where environment='TEST' and candidate_id=c and availability_date=d
        and preference='NIGHT' and availability_version=old_version+1) then
      raise exception 'Two-way canonical admission failed for %',mode;
    end if;
    if mode='SUPABASE_PRIMARY' then
      if (select count(*) from public.candidate_daily_sheet_projection_outbox where command_id=req)<>1
        or (select state from private.candidate_daily_sync_state where environment='TEST'
          and candidate_id=c and target='MASTER_AVAILABILITY_SHEET') not in ('LAGGING','ERROR') then
        raise exception 'New canonical projection was missing or falsely acknowledged';
      end if;
    elsif exists(select 1 from public.candidate_daily_sheet_projection_outbox where command_id=req)
      or (select state from private.candidate_daily_sync_state where environment='TEST'
        and candidate_id=c and target='MASTER_AVAILABILITY_SHEET')<>'READY' then
      raise exception 'Legacy Google-primary acknowledgement changed';
    end if;
    replay:=public.candidate_daily_legacy_availability_apply_atomic_v1(ctx,h,req,idem,changes,cid);
    if replay->>'_idempotent_replay' is distinct from 'true'
      or (replay-'_idempotent_replay') is distinct from r
      or (select canonical_version from private.candidate_daily_authority_scopes
        where environment='TEST' and candidate_id=c)<>old_version+1 then
      raise exception 'Replay mutated canonical state';
    end if;
    begin
      perform public.candidate_daily_legacy_availability_apply_atomic_v1(ctx,h,req,idem,
        jsonb_build_array(jsonb_build_object('date',d::text,'availability','LD')),cid);
      raise exception 'Conflicting replay was accepted';
    exception when unique_violation then null;
    end;
  end loop;

  update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
  where environment='TEST' and candidate_id=c;
  update public.candidate_daily_rota_days set booked=true,booking_id='synthetic-two-way-booking',
    shift_starts_at=(d+1)::timestamp at time zone 'Europe/London',
    shift_ends_at=((d+1)::timestamp+interval '8 hours') at time zone 'Europe/London'
  where generation_id=g and rota_date=d+1;
  update public.candidate_daily_rota_days set system_blocked=true where generation_id=g and rota_date=d+2;
  req:=gen_random_uuid();
  r:=public.candidate_daily_legacy_availability_apply_atomic_v1(ctx,h,req,'two-way-mixed-'||req::text,
    jsonb_build_array(
      jsonb_build_object('date',(d+1)::text,'availability','LD'),
      jsonb_build_object('date',(d+2)::text,'availability','LD'),
      jsonb_build_object('date',(d+14)::text,'availability','LD'),
      jsonb_build_object('date',(d+3)::text,'availability','wrong'),
      jsonb_build_object('date',(d+4)::text,'availability','LD'),
      jsonb_build_object('date',(d+4)::text,'availability','N')),cid);
  if r#>>'{outcomes,0,reason}' is distinct from 'BOOKED'
    or r#>>'{outcomes,1,reason}' is distinct from 'BLOCKED'
    or r#>>'{outcomes,2,reason}' is distinct from 'OUTSIDE_WINDOW'
    or r#>>'{outcomes,3,reason}' is distinct from 'INVALID_VALUE'
    or r#>>'{outcomes,4,applied}' is distinct from 'true'
    or r#>>'{outcomes,5,reason}' is distinct from 'DUPLICATE_DATE'
    or (select count(*) from public.candidate_daily_sheet_projection_outbox where command_id=req)<>1 then
    raise exception 'Per-date protections or projection scope regressed';
  end if;
  update private.candidate_daily_authority_scopes set transition_in_progress=true
  where environment='TEST' and candidate_id=c;
  req:=gen_random_uuid();
  r:=public.candidate_daily_legacy_availability_apply_atomic_v1(ctx,h,req,'two-way-transition-'||req::text,
    jsonb_build_array(jsonb_build_object('date',d::text,'availability','LD')),cid);
  if r#>>'{outcomes,0,reason}' is distinct from 'NOT_EDITABLE'
    or r ? 'committed_version'
    or exists(select 1 from public.candidate_daily_sheet_projection_outbox where command_id=req) then
    raise exception 'Transition fence failed';
  end if;
  begin
    perform public.candidate_daily_legacy_availability_apply_atomic_v1('{}'::jsonb,h,gen_random_uuid(),
      'two-way-invalid-context-0001',jsonb_build_array(jsonb_build_object('date',d::text,'availability','LD')),cid);
    raise exception 'Unsigned context was accepted';
  exception when insufficient_privilege then null;
  end;
end;
$test$;
rollback;
