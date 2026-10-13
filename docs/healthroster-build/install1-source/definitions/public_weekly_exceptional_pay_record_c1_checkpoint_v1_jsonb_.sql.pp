create or replace function public.weekly_exceptional_pay_record_c1_checkpoint_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id','phase',
    'record_offset','next_record_offset','result','idempotency_key'
  ];
  v_control_result_keys constant text[]:=array[
    'contract','ok','status','code','operation_id','scope_id','owner_epoch',
    'sequence','next_sequence','phase','source_cursor','component_cursor',
    'verify_cursor','rows_read','rows_written','work_used','processed_bytes',
    'has_more','replayed','retry_after_ms','request_sha256',
    'checkpoint_sha256','receipt_sha256','publication_id','head_revision',
    'source_identity_sha256','operation_created','input_records_consumed',
    'part_cursor','verify_part_cursor','verify_byte_offset'
  ];
  v_status_result_keys constant text[]:=array[
    'contract','ok','status','code','operation_id','scope_id','owner_epoch',
    'sequence','next_sequence','phase','source_cursor','component_cursor',
    'verify_cursor','rows_read','rows_written','work_used','processed_bytes',
    'has_more','replayed','retry_after_ms','request_sha256',
    'checkpoint_sha256','receipt_sha256','publication_id','head_revision',
    'source_identity_sha256','operation_created','state',
    'terminal_receipt_sha256','retained_sequence_low','retained_sequence_high'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_phase text;
  v_record_offset integer;
  v_next_record_offset integer;
  v_result jsonb;
  v_status text;
  v_idempotency_key text;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_existing public.weekly_exceptional_c1_publication_checkpoints%rowtype;
  v_prior public.weekly_exceptional_c1_publication_checkpoints%rowtype;
  v_total_records integer;
  v_sequence bigint;
  v_phase_rank integer;
  v_prior_phase_rank integer;
  v_result_hash bytea;
  v_checkpoint_hash bytea;
  v_result_operation_id uuid;
  v_result_request_hash bytea;
  v_result_receipt_hash bytea;
  v_terminal_failure boolean:=false;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_CHECKPOINT_V1'
     or pg_catalog.jsonb_typeof(p_request->'result')<>'object' then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_INVALID' using errcode='22023';
  end if;
  if pg_catalog.octet_length(pg_catalog.convert_to(p_request::text,'UTF8'))>131072 then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_TOO_LARGE' using errcode='54000';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_record_offset:=(p_request->>'record_offset')::integer;
    v_next_record_offset:=(p_request->>'next_record_offset')::integer;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_INVALID' using errcode='22023';
  end;
  v_phase:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'phase','')));
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_result:=p_request->'result';
  if v_phase not in ('START','STAGE','VALIDATE','CERTIFY','PUBLISH')
     or v_record_offset<0 or v_next_record_offset<v_record_offset
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 240
     or not (
       private.weekly_exceptional_json_keys_exact_v1(v_result,v_control_result_keys)
       or private.weekly_exceptional_json_keys_exact_v1(v_result,v_status_result_keys)
     )
     or not (v_result ?& array[
       'contract','ok','status','code','operation_id','next_sequence','phase',
       'request_sha256','receipt_sha256','has_more'
     ])
     or v_result->>'contract'<>'WEEKLY_SOURCE_C1_V1'
     or pg_catalog.jsonb_typeof(v_result->'ok')<>'boolean'
     or pg_catalog.jsonb_typeof(v_result->'has_more')<>'boolean' then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_INVALID' using errcode='22023';
  end if;
  v_status:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_result->>'status','')));
  if v_status not in (
    'PROGRESS','READY','PUBLISHED','PENDING','ABORTED','RETIRED','BUSY',
    'STALE','REPLAYED','REFUSED','COMPACTED','SEALED','ADOPTED','TERMINAL'
  ) then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_STATUS_INVALID' using errcode='22023';
  end if;
  begin
    v_result_operation_id:=(v_result->>'operation_id')::uuid;
    perform (v_result->>'next_sequence')::bigint;
    v_result_request_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'request_sha256');
    v_result_receipt_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'receipt_sha256');
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_RESULT_INVALID' using errcode='22023';
  end;
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_RESULT_V1',v_result
  );

  select checkpoint.* into v_existing
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.idempotency_key=v_idempotency_key
  for share;
  if found then
    if v_existing.publication_request_id is distinct from v_request_id
       or v_existing.phase is distinct from v_phase
       or v_existing.record_offset is distinct from v_record_offset
       or v_existing.next_record_offset is distinct from v_next_record_offset
       or v_existing.result_sha256 is distinct from v_result_hash then
      raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_KEY_COLLISION' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','CHECKPOINT_RECORDED','idempotent_replay',true,
      'publication_request_id',v_request_id,
      'checkpoint_id',v_existing.id,
      'checkpoint_sequence',v_existing.checkpoint_sequence,
      'state',(select request.state from public.weekly_exceptional_c1_publication_requests request where request.id=v_request_id)
    );
  end if;

  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id
  for update;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_publication.family_id
  for update;
  if v_run.requested_by_user_id is distinct from v_actor_user_id
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id)
     or v_publication.state in ('RECOVERY_REQUIRED','PUBLISHED','REFUSED','FAILED','RETIRED')
     or v_family.id is distinct from v_publication.family_id
     or v_publication.generation_id is distinct from (
       select generation.id from public.weekly_exceptional_pay_generations generation
       where generation.id=v_publication.generation_id and generation.family_id=v_family.id
     )
     or v_result_request_hash is distinct from v_publication.request_sha256
     or (v_publication.c1_operation_id is not null
         and v_publication.c1_operation_id is distinct from v_result_operation_id) then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_SCOPE_INVALID' using errcode='55000';
  end if;

  select count(*)::integer into v_total_records
  from (
    select id from public.weekly_exceptional_c1_source_records
      where publication_request_id=v_request_id
    union all
    select source_record_id from public.weekly_exceptional_c1_source_parts
      where publication_request_id=v_request_id
    union all
    select id from public.weekly_exceptional_c1_component_records
      where publication_request_id=v_request_id
  ) records;
  if v_total_records<1 or v_next_record_offset>v_total_records then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_OFFSET_INVALID' using errcode='55000';
  end if;

  select checkpoint.* into v_prior
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.publication_request_id=v_request_id
  order by checkpoint.checkpoint_sequence desc
  limit 1
  for share;
  v_sequence:=coalesce(v_prior.checkpoint_sequence,0)+1;
  v_phase_rank:=case v_phase when 'START' then 1 when 'STAGE' then 2
    when 'VALIDATE' then 3 when 'CERTIFY' then 4 when 'PUBLISH' then 5 end;
  v_prior_phase_rank:=case v_prior.phase when 'START' then 1 when 'STAGE' then 2
    when 'VALIDATE' then 3 when 'CERTIFY' then 4 when 'PUBLISH' then 5 else 0 end;
  if v_phase_rank<v_prior_phase_rank
     or (v_prior.id is not null and v_record_offset<>v_prior.next_record_offset)
     or (v_phase<>'STAGE' and v_next_record_offset<>v_record_offset)
     or (v_phase in ('VALIDATE','CERTIFY','PUBLISH') and v_record_offset<>v_total_records)
     or (v_phase='START' and (v_record_offset<>0 or v_next_record_offset<>0))
     or (v_phase='STAGE' and (
       nullif(v_result->>'input_records_consumed','') is null
       or (v_result->>'input_records_consumed')::integer
          <>v_next_record_offset-v_record_offset
     ))
     or (v_status='PUBLISHED' and (
       v_phase<>'PUBLISH'
       or nullif(v_result->>'publication_id','') is null
       or nullif(v_result->>'head_revision','') is null
     )) then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_SEQUENCE_INVALID' using errcode='55000';
  end if;

  v_checkpoint_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_CHECKPOINT_V1',
    pg_catalog.jsonb_build_object(
      'publication_request_id',v_request_id,'checkpoint_sequence',v_sequence,
      'phase',v_phase,'record_offset',v_record_offset,
      'next_record_offset',v_next_record_offset,
      'result_hash',pg_catalog.encode(v_result_hash,'hex'),
      'prior_checkpoint_hash',case when v_prior.checkpoint_sha256 is null then null
        else pg_catalog.encode(v_prior.checkpoint_sha256,'hex') end
    )
  );
  insert into public.weekly_exceptional_c1_publication_checkpoints(
    publication_request_id,checkpoint_sequence,phase,record_offset,
    next_record_offset,result_json,result_sha256,prior_checkpoint_sha256,
    checkpoint_sha256,idempotency_key
  ) values (
    v_request_id,v_sequence,v_phase,v_record_offset,v_next_record_offset,
    v_result,v_result_hash,v_prior.checkpoint_sha256,v_checkpoint_hash,
    v_idempotency_key
  ) returning * into v_existing;

  v_terminal_failure:=v_status in ('ABORTED','RETIRED','STALE','REFUSED','COMPACTED');
  update public.weekly_exceptional_c1_publication_requests
  set c1_operation_id=coalesce(c1_operation_id,v_result_operation_id),
      typed_result_json=v_result,
      state=case
        when v_status='PENDING' or v_status='BUSY' then 'PENDING'
        when v_status='RETIRED' then 'RETIRED'
        when v_terminal_failure then 'REFUSED'
        else 'SUBMITTED' end,
      completed_at_utc=case when v_terminal_failure then pg_catalog.statement_timestamp()
        else completed_at_utc end
  where id=v_request_id
  returning * into v_publication;

  if v_terminal_failure then
    update public.weekly_exceptional_pay_generations
    set lifecycle_state='FAILED',result_hash=v_result_receipt_hash
    where id=v_publication.generation_id and lifecycle_state='PENDING_C1';
    update public.weekly_exceptional_pay_target_families
    set c1_publication_state=case when current_generation_id is null then 'FAILED' else 'LIVE' end,
        bound_version=bound_version+1
    where id=v_family.id;
    update public.weekly_exceptional_orchestration_runs
    set state='REFUSED',after_state_fingerprint=v_result_hash,
        completed_at_utc=pg_catalog.statement_timestamp()
    where id=v_run.id;
  elsif v_status in ('PENDING','BUSY') then
    update public.weekly_exceptional_pay_target_families
    set c1_publication_state='PENDING'
    where id=v_family.id;
    update public.weekly_exceptional_orchestration_runs
    set state='PENDING' where id=v_run.id;
  else
    update public.weekly_exceptional_pay_target_families
    set c1_publication_state='PUBLISHING'
    where id=v_family.id;
    update public.weekly_exceptional_orchestration_runs
    set state='RUNNING' where id=v_run.id;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','CHECKPOINT_RECORDED','idempotent_replay',false,
    'publication_request_id',v_request_id,'checkpoint_id',v_existing.id,
    'checkpoint_sequence',v_sequence,'state',v_publication.state,
    'checkpoint_sha256',pg_catalog.encode(v_checkpoint_hash,'hex')
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_CHECKPOINT_SCOPE_INVALID' using errcode='55000';
end;
$function$;
